// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Translation of `org.apache.kafka.common.network.KafkaChannel`.
//!
//! Phase 5b-3 ships the producer-relevant subset:
//!
//! * `setSend` / `write` / `maybeCompleteSend` — write path.
//! * `read` / `currentReceive` / `maybeCompleteReceive` — read path.
//! * `prepare` / `ready` / `finishConnect` / `disconnect` / `close` —
//!   lifecycle.
//! * `mute` / `maybeUnmute` / `handleChannelMuteEvent` / `muteState` —
//!   mute state machine. The server-only `MUTED_AND_*` transitions are
//!   carried verbatim so the state machine matches Java byte-for-byte;
//!   the producer never originates the events that drive those
//!   transitions.
//!
//! Server-only and re-authentication paths are deferred:
//! `maybeBeginServerReauthentication`, `maybeBeginClientReauthentication`,
//! `serverAuthenticationSessionExpired`, `reauthenticationLatencyMs`,
//! `pollResponseReceivedDuringReauthentication`,
//! `connectedClientSupportsReauthentication`,
//! `swapAuthenticatorsAndBeginReauthentication`,
//! `maybeAddWriteInterestAfterReauth`. They land with SASL in Phase 9.
//!
//! `MemoryPool` is also deferred — Phase 5a's `NetworkReceive` allocates
//! its payload buffer eagerly, sized to the parsed length-prefix. The
//! [`KafkaChannel::is_in_mutable_state`] check below collapses to "is
//! the receive in-progress and the transport ready"; it never reports
//! "out of memory" because there is no pool.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use crate::common::errors::KafkaError;
use crate::common::network::authenticator::Authenticator;
use crate::common::network::transport_layer::{OP_READ, OP_WRITE};
use crate::common::network::{
    ChannelMetadataRegistry, ChannelState, ChannelStateName, NetworkReceive, NetworkSend, Receive, Send as KafkaSend,
    TransportLayer,
};
use crate::common::security::auth::KafkaPrincipal;

/// Mute states for [`KafkaChannel`]. Mirrors the Java
/// `KafkaChannel.ChannelMuteState` enum exactly.
///
/// * `NotMuted` — channel is not muted (default state).
/// * `Muted` — channel is muted; only this state can transition out via
///   [`KafkaChannel::maybe_unmute`].
/// * `MutedAndResponsePending` — server-only: channel is muted and
///   `SocketServer` has not sent a response back to the client yet.
/// * `MutedAndThrottled` — server-only: channel is muted and throttling
///   is in progress due to quota violation.
/// * `MutedAndThrottledAndResponsePending` — server-only: channel is
///   muted, throttling is in progress, and a response is currently
///   pending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelMuteState {
    NotMuted,
    Muted,
    MutedAndResponsePending,
    MutedAndThrottled,
    MutedAndThrottledAndResponsePending,
}

/// Events that drive the [`ChannelMuteState`] transitions. Mirrors the
/// Java `KafkaChannel.ChannelMuteEvent` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelMuteEvent {
    RequestReceived,
    ResponseSent,
    ThrottleStarted,
    ThrottleEnded,
}

impl ChannelMuteEvent {
    /// Mirrors Java's `Enum.name()` on the `ChannelMuteEvent`. Used for
    /// the `IllegalStateException` message in
    /// [`KafkaChannel::handle_channel_mute_event`].
    pub fn name(self) -> &'static str {
        match self {
            ChannelMuteEvent::RequestReceived => "REQUEST_RECEIVED",
            ChannelMuteEvent::ResponseSent => "RESPONSE_SENT",
            ChannelMuteEvent::ThrottleStarted => "THROTTLE_STARTED",
            ChannelMuteEvent::ThrottleEnded => "THROTTLE_ENDED",
        }
    }
}

impl ChannelMuteState {
    /// Mirrors Java's `Enum.name()` on the `ChannelMuteState`. Used for
    /// the `IllegalStateException` message in
    /// [`KafkaChannel::handle_channel_mute_event`].
    pub fn name(self) -> &'static str {
        match self {
            ChannelMuteState::NotMuted => "NOT_MUTED",
            ChannelMuteState::Muted => "MUTED",
            ChannelMuteState::MutedAndResponsePending => "MUTED_AND_RESPONSE_PENDING",
            ChannelMuteState::MutedAndThrottled => "MUTED_AND_THROTTLED",
            ChannelMuteState::MutedAndThrottledAndResponsePending => "MUTED_AND_THROTTLED_AND_RESPONSE_PENDING",
        }
    }
}

/// Owned trait object alias for the transport layer the channel uses.
/// `Send` is required so a `KafkaChannel` can be moved between Tokio
/// tasks (Phase 5c will park each channel on its own read/write task).
/// `Sync` is required so [`Selector::poll`] can hold
/// `&(dyn TransportLayer + Sync)` references across an `.await` point
/// (Phase 8a.0 readiness-notification arm). Both production
/// transports (`PlaintextTransportLayer`, `SslTransportLayer`) are
/// already `Sync` — adding the bound here just makes the constraint
/// explicit at the type-alias level.
pub type BoxedTransport = Box<dyn TransportLayer + std::marker::Send + std::marker::Sync>;

/// Owned trait object alias for the channel's authenticator.
pub type BoxedAuthenticator = Box<dyn Authenticator>;

/// Owned trait object alias for the metadata registry. `Send` so the
/// channel can move tasks; the registry mutates on the same task that
/// drives the channel so no `Sync` is required.
pub type BoxedMetadataRegistry = Box<dyn ChannelMetadataRegistry + std::marker::Send>;

/// A Kafka connection bridging the [`TransportLayer`] (raw bytes) to the
/// [`Receive`]/[`KafkaSend`] traits (request/response framing). Mirrors
/// the Java [`KafkaChannel`].
///
/// The channel owns the transport, an authenticator, an in-progress
/// [`NetworkReceive`] and [`NetworkSend`], the mute-state machine, and
/// the [`ChannelState`] used by upper-layer disconnect handling.
///
/// Re-authentication state is deferred to Phase 9 — see the module
/// docstring for the per-method deferral matrix.
pub struct KafkaChannel {
    /// Connection id. Java uses `String`; Rust uses `Arc<str>` so per-
    /// message clones (e.g. when constructing a [`NetworkSend`] tagged
    /// with this id) are cheap. CLAUDE.md rule 11.
    id: Arc<str>,
    transport_layer: BoxedTransport,
    authenticator: BoxedAuthenticator,
    /// Maximum size of a single receive buffer to allocate. Mirrors
    /// Java's `maxReceiveSize`. Used when constructing the in-progress
    /// [`NetworkReceive`] on the first call to [`Self::read`].
    max_receive_size: i32,
    metadata_registry: BoxedMetadataRegistry,
    /// In-progress receive (None when no receive is being built).
    receive: Option<NetworkReceive>,
    /// In-progress send (None when no send is queued).
    send: Option<NetworkSend>,
    /// Track connection and mute state of channels to enable outstanding
    /// requests on channels to be processed after the channel is
    /// disconnected.
    disconnected: bool,
    mute_state: ChannelMuteState,
    state: ChannelState,
    /// Last-known peer address — captured by [`Self::finish_connect`]
    /// before the underlying socket can become disconnected (refused
    /// connections, half-closes). Used by [`Self::disconnect`] to enrich
    /// the [`ChannelState`] returned to the upper layer.
    remote_address: Option<SocketAddr>,
    /// True iff a [`Self::write`] call has happened since the last
    /// [`Self::set_send`]. Mirrors Java's `midWrite` flag, used by the
    /// (Phase-9-deferred) client-side reauthentication path to refuse
    /// re-authentication mid-write.
    mid_write: bool,
}

impl KafkaChannel {
    /// Construct a `KafkaChannel`. Mirrors Java's
    /// `new KafkaChannel(id, transportLayer, authenticatorCreator,
    /// maxReceiveSize, memoryPool, metadataRegistry)` minus the
    /// `Supplier<Authenticator>` (the producer side never re-creates an
    /// authenticator since re-authentication is deferred to Phase 9)
    /// and the `MemoryPool` (deferred — see the module docstring).
    pub fn new(
        id: Arc<str>,
        transport_layer: BoxedTransport,
        authenticator: BoxedAuthenticator,
        max_receive_size: i32,
        metadata_registry: BoxedMetadataRegistry,
    ) -> Self {
        KafkaChannel {
            id,
            transport_layer,
            authenticator,
            max_receive_size,
            metadata_registry,
            receive: None,
            send: None,
            disconnected: false,
            mute_state: ChannelMuteState::NotMuted,
            state: ChannelState::not_connected(),
            remote_address: None,
            mid_write: false,
        }
    }

    /// Close the channel, releasing all owned resources. Mirrors Java's
    /// `close()` (the `AutoCloseable` impl). Java calls `Utils.closeAll`
    /// which best-effort-closes every resource even on intermediate
    /// errors; we mirror by capturing the first error and continuing.
    pub fn close(&mut self) -> io::Result<()> {
        self.disconnected = true;
        let mut first_err: Option<io::Error> = None;
        if let Err(e) = self.transport_layer.close() {
            first_err.get_or_insert(e);
        }
        if let Err(e) = self.authenticator.close() {
            first_err.get_or_insert(e);
        }
        if let Some(receive) = self.receive.as_mut()
            && let Err(e) = receive.close()
        {
            first_err.get_or_insert(e);
        }
        // metadata_registry.close() is infallible (Java returns void).
        self.metadata_registry.close();
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Returns the principal returned by `authenticator.principal()`.
    /// Mirrors Java's `principal()` — the lookup is lazy, mirroring
    /// Java's `SslAuthenticator` re-reading `transportLayer.sslSession()`
    /// on every call. The owning channel forwards its transport
    /// reference into the authenticator so SSL impls can read the
    /// post-handshake peer certificate without holding the transport
    /// themselves.
    pub fn principal(&self) -> KafkaPrincipal {
        self.authenticator.principal(self.transport_layer.as_ref())
    }

    /// Drives the transport handshake and authentication. Mirrors Java's
    /// `prepare()`. Returns `Err(KafkaError::Authentication)` when the
    /// handshake or authentication fails.
    pub fn prepare(&mut self) -> Result<(), KafkaError> {
        let mut authenticating = false;
        let result: io::Result<()> = (|| {
            if !self.transport_layer.ready() {
                self.transport_layer.handshake()?;
            }
            if self.transport_layer.ready() && !self.authenticator.complete() {
                authenticating = true;
                self.authenticator.authenticate()?;
            }
            Ok(())
        })();

        if let Err(e) = result {
            // Mirror Java: an `AuthenticationException` is captured into
            // the channel state so the upper layer can surface the
            // failure without a retry. Other I/O errors are re-thrown
            // unchanged so the Selector can disconnect the channel.
            let remote_desc = self.remote_address.as_ref().map(|a| a.to_string());
            // We classify errors by content because the Phase 5b SSL
            // handshake surfaces `SslAuthenticationException` as
            // `io::Error::other(KafkaError::Authentication(...))`; the
            // plaintext and connect-time errors come through as the
            // raw `io::Error`. Java distinguishes via Java exception
            // hierarchy.
            let is_auth = matches!(
                e.get_ref().and_then(|inner| inner.downcast_ref::<KafkaError>()),
                Some(KafkaError::Authentication(_))
            );
            let msg = e.to_string();
            if is_auth {
                self.state = ChannelState::with_exception(
                    ChannelStateName::AuthenticationFailed,
                    KafkaError::Authentication(msg.clone()),
                    remote_desc,
                );
                if authenticating {
                    self.delay_close_on_authentication_failure();
                }
                return Err(KafkaError::Authentication(msg));
            }
            // Non-authentication error: surface as a Network error
            // (Java's `IOException` subtypes that are not
            // `AuthenticationException`).
            return Err(KafkaError::Network(msg));
        }
        if self.ready() {
            self.state = ChannelState::ready();
        }
        Ok(())
    }

    /// Mark the channel as disconnected. Mirrors Java's `disconnect()`:
    /// flips the `disconnected` flag, enriches the channel state with
    /// the captured remote address (if any), and disconnects the
    /// underlying transport.
    pub fn disconnect(&mut self) {
        self.disconnected = true;
        if self.state.state() == ChannelStateName::NotConnected
            && let Some(addr) = self.remote_address.as_ref()
        {
            self.state = ChannelState::with_remote_address(ChannelStateName::NotConnected, addr.to_string());
        }
        self.transport_layer.disconnect();
    }

    /// Override the channel state. Mirrors Java's `state(ChannelState)`.
    pub fn set_state(&mut self, state: ChannelState) {
        self.state = state;
    }

    /// Borrow the channel's current [`ChannelState`]. Mirrors Java's
    /// `state()` getter.
    pub fn state(&self) -> &ChannelState {
        &self.state
    }

    /// Finish the connect process on the underlying transport. Mirrors
    /// Java's `finishConnect()`. Returns `true` once the underlying
    /// transport reports the connect is complete.
    pub fn finish_connect(&mut self) -> io::Result<bool> {
        // Capture the remote address before `finishConnect` runs — Java
        // calls `socketChannel.getRemoteAddress()` before the connect
        // completes so a refused connection still records who we tried
        // to reach. The Tokio transport surfaces `peer_addr()` only
        // after the connect is complete, so we capture lazily and
        // ignore failures (the address is best-effort metadata).
        if self.remote_address.is_none()
            && let Ok(addr) = self.transport_layer.peer_addr()
        {
            self.remote_address = Some(addr);
        }
        let connected = self.transport_layer.finish_connect()?;
        if connected {
            if self.ready() {
                self.state = ChannelState::ready();
            } else if let Some(addr) = self.remote_address.as_ref() {
                self.state = ChannelState::with_remote_address(ChannelStateName::Authenticate, addr.to_string());
            } else {
                self.state = ChannelState::authenticate();
            }
        }
        Ok(connected)
    }

    /// Returns `true` if the underlying transport reports a connected
    /// socket. Mirrors Java's `isConnected()`.
    pub fn is_connected(&self) -> bool {
        self.transport_layer.is_connected()
    }

    /// Channel id. Mirrors Java's `id()`.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Channel id as a clone of the underlying [`Arc<str>`]. Cheap
    /// (atomic refcount bump) — used when the caller needs to outlive
    /// the borrow.
    pub fn id_arc(&self) -> Arc<str> {
        Arc::clone(&self.id)
    }

    /// Externally muting a channel should be done via the Selector to
    /// ensure proper state handling. Mirrors Java's package-private
    /// `mute()`. `pub(crate)` matches Java's package-private boundary —
    /// the Phase 5c `Selector` (sibling module in the same crate) drives
    /// this on the muted-channel re-tick path; downstream consumers
    /// outside the crate must not call it. `dead_code` is allowed
    /// because no in-crate caller exists until Phase 5c lands.
    #[allow(dead_code)]
    pub(crate) fn mute(&mut self) {
        if self.mute_state == ChannelMuteState::NotMuted {
            if !self.disconnected {
                self.transport_layer.remove_interest_ops(OP_READ);
            }
            self.mute_state = ChannelMuteState::Muted;
        }
    }

    /// Unmute the channel. The channel can be unmuted only if it is in
    /// the [`ChannelMuteState::Muted`] state. For other muted states
    /// (`MutedAnd*`), this is a no-op. Returns whether the channel is
    /// in the [`ChannelMuteState::NotMuted`] state after the call.
    /// Mirrors Java's package-private `maybeUnmute()` — see [`Self::mute`]
    /// for why `pub(crate)` is the right Rust visibility. `dead_code`
    /// allowed until the Phase 5c Selector wires it up.
    #[allow(dead_code)]
    pub(crate) fn maybe_unmute(&mut self) -> bool {
        if self.mute_state == ChannelMuteState::Muted {
            if !self.disconnected {
                self.transport_layer.add_interest_ops(OP_READ);
            }
            self.mute_state = ChannelMuteState::NotMuted;
        }
        self.mute_state == ChannelMuteState::NotMuted
    }

    /// Handle the specified mute-related event and transition the mute
    /// state according to the state machine. Mirrors Java's
    /// `handleChannelMuteEvent(ChannelMuteEvent)`.
    pub fn handle_channel_mute_event(&mut self, event: ChannelMuteEvent) -> Result<(), KafkaError> {
        let mut state_changed = false;
        match event {
            ChannelMuteEvent::RequestReceived => {
                if self.mute_state == ChannelMuteState::Muted {
                    self.mute_state = ChannelMuteState::MutedAndResponsePending;
                    state_changed = true;
                }
            },
            ChannelMuteEvent::ResponseSent => {
                if self.mute_state == ChannelMuteState::MutedAndResponsePending {
                    self.mute_state = ChannelMuteState::Muted;
                    state_changed = true;
                }
                if self.mute_state == ChannelMuteState::MutedAndThrottledAndResponsePending {
                    self.mute_state = ChannelMuteState::MutedAndThrottled;
                    state_changed = true;
                }
            },
            ChannelMuteEvent::ThrottleStarted => {
                if self.mute_state == ChannelMuteState::MutedAndResponsePending {
                    self.mute_state = ChannelMuteState::MutedAndThrottledAndResponsePending;
                    state_changed = true;
                }
            },
            ChannelMuteEvent::ThrottleEnded => {
                if self.mute_state == ChannelMuteState::MutedAndThrottled {
                    self.mute_state = ChannelMuteState::Muted;
                    state_changed = true;
                }
                if self.mute_state == ChannelMuteState::MutedAndThrottledAndResponsePending {
                    self.mute_state = ChannelMuteState::MutedAndResponsePending;
                    state_changed = true;
                }
            },
        }
        if !state_changed {
            return Err(KafkaError::IllegalState(format!(
                "Cannot transition from {} for {}",
                self.mute_state.name(),
                event.name()
            )));
        }
        Ok(())
    }

    /// Current mute state. Mirrors Java's `muteState()`.
    pub fn mute_state(&self) -> ChannelMuteState {
        self.mute_state
    }

    /// Delay channel close on authentication failure. Mirrors Java's
    /// private `delayCloseOnAuthenticationFailure`. Removes
    /// [`OP_WRITE`] so the upper layer's pending sends do not race
    /// with the close.
    fn delay_close_on_authentication_failure(&mut self) {
        self.transport_layer.remove_interest_ops(OP_WRITE);
    }

    /// Re-arm the OP_WRITE interest after an authentication-failure
    /// delay. Mirrors Java's package-private
    /// `completeCloseOnAuthenticationFailure` — `pub(crate)` matches
    /// Java's package-private boundary; the Phase 5c `Selector`
    /// (sibling module) calls this during its disconnect-with-delay
    /// path. `dead_code` allowed until the Phase 5c Selector wires
    /// it up.
    #[allow(dead_code)]
    pub(crate) fn complete_close_on_authentication_failure(&mut self) -> io::Result<()> {
        self.transport_layer.add_interest_ops(OP_WRITE);
        // Java calls `authenticator.handleAuthenticationFailure()`; the
        // Phase 5b-3 non-SASL authenticators do not have that hook.
        Ok(())
    }

    /// Returns true iff this channel has been explicitly muted via
    /// [`Self::mute`]. Mirrors Java's `isMuted()`.
    pub fn is_muted(&self) -> bool {
        self.mute_state != ChannelMuteState::NotMuted
    }

    /// Returns true iff the channel can be muted by the upper layer.
    /// Mirrors Java's `isInMutableState()`. The Java semantics gate on
    /// memory-pool allocation status; our [`NetworkReceive`] allocates
    /// eagerly, so the gate collapses to "is the receive in progress
    /// and the transport ready".
    pub fn is_in_mutable_state(&self) -> bool {
        // Mirror Java: if there's no in-progress receive (or the
        // memory has already been allocated), there's no reason to
        // mute. Our `NetworkReceive::memory_allocated` becomes true
        // once the size header is parsed and the payload buffer is
        // allocated, which always happens within a single `read_from`
        // — so this returns false in practice and the producer never
        // mutes itself for memory pressure.
        match self.receive.as_ref() {
            None => false,
            Some(r) if r.memory_allocated() => false,
            _ => self.transport_layer.ready(),
        }
    }

    /// Returns true when the transport handshake and the authenticator
    /// are both done. Mirrors Java's `ready()`.
    pub fn ready(&self) -> bool {
        self.transport_layer.ready() && self.authenticator.complete()
    }

    /// Borrow the underlying transport layer. Phase 8a.0 — needed by
    /// the `Selector` poll loop to register socket-readiness wakeups
    /// without owning the transport. Mirrors Java's package-private
    /// `transportLayer()` getter the Selector uses to dispatch reads.
    pub fn transport_layer_ref(&self) -> &dyn TransportLayer {
        self.transport_layer.as_ref()
    }

    /// Variant of [`Self::transport_layer_ref`] that preserves the
    /// `+ Sync` bound, so the borrow can cross `.await` points
    /// (`&T: Send` iff `T: Sync`). Used by the [`Selector`]'s
    /// readiness-notification select arm — see
    /// [`crate::common::network::selector::wait_any_transport_readable`].
    pub fn transport_layer_sync_ref(&self) -> &(dyn TransportLayer + Sync) {
        self.transport_layer.as_ref()
    }

    /// Returns true iff there is an in-progress send. Mirrors Java's
    /// `hasSend()`.
    pub fn has_send(&self) -> bool {
        self.send.is_some()
    }

    /// Returns the peer host (IP address only). Mirrors Java's
    /// `socketAddress()` which returns
    /// `transportLayer.socketChannel().socket().getInetAddress()` — the
    /// remote `InetAddress`, with no port. Falls back to the captured
    /// `remote_address` if the underlying socket is no longer accessible
    /// (post-disconnect).
    pub fn socket_address(&self) -> io::Result<IpAddr> {
        let addr = match self.transport_layer.peer_addr() {
            Ok(addr) => addr,
            Err(_) => self
                .remote_address
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "peer address unknown — never connected"))?,
        };
        Ok(addr.ip())
    }

    /// Returns the peer port, or `0` if the socket has never been
    /// connected. Mirrors Java's `socketPort()`. The Java doc states
    /// "If the socket was connected prior to being closed, then this
    /// method will continue to return the connected port number after
    /// the socket is closed", which we mirror through the captured
    /// `remote_address` fallback.
    pub fn socket_port(&self) -> u16 {
        match self.transport_layer.peer_addr() {
            Ok(addr) => addr.port(),
            Err(_) => self.remote_address.map(|a| a.port()).unwrap_or(0),
        }
    }

    /// Returns a stable string suitable for log lines. Mirrors Java's
    /// `socketDescription()` which falls back to the local socket
    /// address when the peer address is unknown.
    ///
    /// Phase 5c's `Selector` uses this in disconnect log lines.
    pub fn socket_description(&self) -> String {
        if let Ok(addr) = self.transport_layer.peer_addr() {
            return addr.ip().to_string();
        }
        // No peer — fall back to captured remote, then local.
        if let Some(addr) = self.remote_address {
            return addr.ip().to_string();
        }
        match self.transport_layer.local_addr() {
            Ok(local) => local.ip().to_string(),
            Err(_) => String::from("<unknown>"),
        }
    }

    /// Borrow the channel's metadata registry. Mirrors Java's
    /// `channelMetadataRegistry()`.
    pub fn channel_metadata_registry(&self) -> &dyn ChannelMetadataRegistry {
        self.metadata_registry.as_ref()
    }

    /// Mutably borrow the channel's metadata registry. Mirrors Java's
    /// `channelMetadataRegistry()` returning a reference whose
    /// register* methods mutate the registry in place.
    pub fn channel_metadata_registry_mut(&mut self) -> &mut dyn ChannelMetadataRegistry {
        self.metadata_registry.as_mut()
    }

    /// Queue a [`NetworkSend`] on this channel. Mirrors Java's
    /// `setSend(NetworkSend)`. Returns `Err(IllegalState)` when there
    /// is already a send in progress — mirroring Java's
    /// `IllegalStateException`.
    ///
    /// The actual write happens lazily on the next call to
    /// [`Self::write`] (which is invoked on the next `Selector::poll`
    /// tick). Mirrors Java's lazy-send pattern — see PLAN.md line
    /// 263–264.
    pub fn set_send(&mut self, send: NetworkSend) -> Result<(), KafkaError> {
        if self.send.is_some() {
            return Err(KafkaError::IllegalState(format!(
                "Attempt to begin a send operation with prior send operation still in progress, connection id is {}",
                self.id
            )));
        }
        self.send = Some(send);
        self.transport_layer.add_interest_ops(OP_WRITE);
        Ok(())
    }

    /// If the in-progress send has completed, take it and return it,
    /// clearing OP_WRITE. Returns `None` if there is no send in
    /// progress, or the send is not yet complete. Mirrors Java's
    /// `maybeCompleteSend()`.
    pub fn maybe_complete_send(&mut self) -> Option<NetworkSend> {
        if self.send.as_ref().is_some_and(KafkaSend::completed) {
            self.mid_write = false;
            self.transport_layer.remove_interest_ops(OP_WRITE);
            return self.send.take();
        }
        None
    }

    /// Drive the read path: lazily construct a [`NetworkReceive`] on
    /// first call, then ask it to read from the transport. Returns the
    /// number of bytes read in this call (`0` for "no progress"). Mirrors
    /// Java's `read()`.
    ///
    /// The return type is `i64` to mirror Java's `long` return — Phase
    /// 5c's metric path accumulates total bytes read in a `long`.
    pub fn read(&mut self) -> io::Result<i64> {
        if self.receive.is_none() {
            self.receive = Some(NetworkReceive::with_max_size(self.max_receive_size, self.id.as_ref()));
        }
        // SAFETY: `receive` is Some immediately above.
        let receive = self.receive.as_mut().expect("receive constructed above");
        // Bridge `&mut dyn TransportLayer` to `&mut dyn io::Read` —
        // both `PlaintextTransportLayer` and `SslTransportLayer`
        // implement `io::Read` (the Phase 5b-1/5b-2 forwarder).
        let bytes_received = read_with_transport(receive, self.transport_layer.as_mut())?;
        // No mute-on-OOM logic — see `is_in_mutable_state`'s docstring.
        Ok(bytes_received as i64)
    }

    /// Borrow the in-progress [`NetworkReceive`]. Mirrors Java's
    /// `currentReceive()`. Returns `None` when there is no receive in
    /// flight (e.g. immediately after a complete-receive was taken).
    pub fn current_receive(&self) -> Option<&NetworkReceive> {
        self.receive.as_ref()
    }

    /// If the in-progress receive has completed, take it and return it.
    /// Mirrors Java's `maybeCompleteReceive()`. The Java implementation
    /// rewinds the payload buffer's `position` to zero before returning;
    /// our `BytesMut` payload buffer has no separate read cursor, so
    /// `take_payload()` already returns a buffer ready to be consumed.
    pub fn maybe_complete_receive(&mut self) -> Option<NetworkReceive> {
        if self.receive.as_ref().is_some_and(Receive::complete) {
            return self.receive.take();
        }
        None
    }

    /// Drive the write path: ask the in-progress send to write what it
    /// can. Returns the number of bytes written (`0` for "no progress",
    /// `0` when there is no send in progress). Mirrors Java's `write()`.
    pub fn write(&mut self) -> io::Result<i64> {
        let Some(send) = self.send.as_mut() else {
            return Ok(0);
        };
        self.mid_write = true;
        let written = send.write_to(self.transport_layer.as_mut())?;
        Ok(written as i64)
    }

    /// `true` iff the underlying transport has bytes buffered internally
    /// that may be processed without further reads from the network.
    /// Mirrors Java's `hasBytesBuffered()`. SSL surfaces `true` when
    /// rustls has decrypted plaintext queued; PLAINTEXT always returns
    /// `false`.
    pub fn has_bytes_buffered(&self) -> bool {
        self.transport_layer.has_bytes_buffered()
    }

    /// `true` iff [`Self::disconnect`] has been called or
    /// [`Self::close`] has been called. Mirrors Java's `disconnected`
    /// field, which is package-private but read by the Selector via
    /// `KafkaChannel.state()`.
    pub fn is_disconnected(&self) -> bool {
        self.disconnected
    }
}

impl PartialEq for KafkaChannel {
    /// Mirrors Java's `equals` — equality by id only.
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for KafkaChannel {}

impl std::hash::Hash for KafkaChannel {
    /// Mirrors Java's `hashCode` — hash by id only.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl std::fmt::Debug for KafkaChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaChannel")
            .field("id", &&*self.id)
            .field("state", &self.state.state())
            .field("mute_state", &self.mute_state)
            .field("disconnected", &self.disconnected)
            .finish()
    }
}

impl std::fmt::Display for KafkaChannel {
    /// Mirrors Java's `toString()` — `<typename> id=<id>`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KafkaChannel id={}", self.id)
    }
}

/// Adapter: hand a `&mut dyn TransportLayer` to a `Receive` that wants
/// `&mut dyn io::Read`. We know both the plaintext and SSL transports
/// implement `io::Read`; this helper unifies the call site so
/// `KafkaChannel::read` is generic-free.
fn read_with_transport(receive: &mut NetworkReceive, transport: &mut dyn TransportLayer) -> io::Result<u64> {
    struct TransportReader<'a> {
        inner: &'a mut dyn TransportLayer,
    }
    impl io::Read for TransportReader<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.read(buf)
        }
    }
    let mut adapter = TransportReader { inner: transport };
    receive.read_from(&mut adapter)
}

#[cfg(test)]
mod tests {
    use std::io::IoSlice;

    use bytes::Bytes;

    use super::*;
    use crate::common::network::byte_buffer_send::ByteBufferSend;
    use crate::common::network::channel_metadata_registry::DefaultChannelMetadataRegistry;
    use crate::common::network::transport_layer::{OP_CONNECT, OP_READ};

    /// Shared backing state for [`MockTransport`]. Keeping the read
    /// queue (and the connect/open/interest-ops bookkeeping) behind an
    /// `Arc<Mutex<_>>` lets the test enqueue canned reads through a
    /// handle the test owns, even after the [`MockTransport`] itself
    /// has been moved into the [`KafkaChannel`]. This mirrors the
    /// Mockito pattern (`Mockito.when(transport.read(...))`) where the
    /// stub is configured through a reference held by the test.
    #[derive(Default)]
    struct MockState {
        writes: Vec<u8>,
        read_queue: std::collections::VecDeque<Vec<u8>>,
        ready: bool,
        connected: bool,
        is_open: bool,
        interest_ops: i32,
        /// Optional cap on bytes accepted by a single `write_vectored`
        /// call. `None` writes everything at once (the default — fastest
        /// path); `Some(n)` truncates the call to at most `n` bytes,
        /// mirroring Java's mocked `transport.write(...)` returning
        /// partial counts in `KafkaChannelTest.testSending`.
        max_bytes_per_write: Option<usize>,
    }

    /// Mock transport that records writes into a `Vec<u8>` and serves
    /// reads from a queue of canned responses. Modelled on Mockito's
    /// `transport.read(...).thenAnswer(...)` chains used by the Java
    /// `KafkaChannelTest`.
    struct MockTransport {
        state: Arc<std::sync::Mutex<MockState>>,
    }

    impl MockTransport {
        fn new() -> (Self, Arc<std::sync::Mutex<MockState>>) {
            let state = Arc::new(std::sync::Mutex::new(MockState {
                writes: Vec::new(),
                read_queue: std::collections::VecDeque::new(),
                ready: true,
                connected: true,
                is_open: true,
                interest_ops: OP_READ,
                max_bytes_per_write: None,
            }));
            (MockTransport { state: Arc::clone(&state) }, state)
        }
    }

    /// Configure the per-call write cap. Mirrors Mockito stubbing
    /// `when(transport.write(any())).thenReturn(4, 64, 64)` in
    /// `KafkaChannelTest.testSending` — letting a single test drive the
    /// write loop through multiple ticks of partial progress.
    fn set_max_bytes_per_write(state: &Arc<std::sync::Mutex<MockState>>, cap: usize) {
        state.lock().expect("mock state").max_bytes_per_write = Some(cap);
    }

    /// Convenience: enqueue a canned-read chunk on a shared mock state.
    fn enqueue_read(state: &Arc<std::sync::Mutex<MockState>>, bytes: Vec<u8>) {
        state.lock().expect("mock state").read_queue.push_back(bytes);
    }

    impl crate::common::network::TransferableChannel for MockTransport {
        fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            let mut s = self.state.lock().expect("mock state");
            // When the cap is set, accept up to `cap` bytes of the
            // vectored input. This faithfully mirrors a kernel-buffer-
            // exhausted write in Java's `SocketChannel.write` returning a
            // partial byte count.
            let cap = s.max_bytes_per_write;
            let mut total: usize = 0;
            for buf in bufs {
                let remaining_cap = match cap {
                    Some(c) => c.saturating_sub(total),
                    None => buf.len(),
                };
                if remaining_cap == 0 {
                    break;
                }
                let take = buf.len().min(remaining_cap);
                s.writes.extend_from_slice(&buf[..take]);
                total += take;
                if take < buf.len() {
                    // Hit the cap mid-buffer — stop iterating.
                    break;
                }
            }
            Ok(total)
        }
        fn has_pending_writes(&self) -> bool {
            false
        }
    }

    impl TransportLayer for MockTransport {
        fn ready(&self) -> bool {
            self.state.lock().expect("mock state").ready
        }
        fn finish_connect(&mut self) -> io::Result<bool> {
            let mut s = self.state.lock().expect("mock state");
            s.connected = true;
            s.interest_ops = (s.interest_ops & !OP_CONNECT) | OP_READ;
            Ok(true)
        }
        fn disconnect(&mut self) {
            let mut s = self.state.lock().expect("mock state");
            s.is_open = false;
            s.connected = false;
        }
        fn is_connected(&self) -> bool {
            self.state.lock().expect("mock state").connected
        }
        fn is_open(&self) -> bool {
            self.state.lock().expect("mock state").is_open
        }
        fn close(&mut self) -> io::Result<()> {
            self.state.lock().expect("mock state").is_open = false;
            Ok(())
        }
        fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
            let mut s = self.state.lock().expect("mock state");
            match s.read_queue.pop_front() {
                Some(chunk) => {
                    let n = chunk.len().min(dst.len());
                    dst[..n].copy_from_slice(&chunk[..n]);
                    Ok(n)
                },
                None => Ok(0),
            }
        }
        fn handshake(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn peer_principal(&self) -> io::Result<KafkaPrincipal> {
            Ok(KafkaPrincipal::anonymous())
        }
        fn add_interest_ops(&mut self, ops: i32) {
            self.state.lock().expect("mock state").interest_ops |= ops;
        }
        fn remove_interest_ops(&mut self, ops: i32) {
            self.state.lock().expect("mock state").interest_ops &= !ops;
        }
        fn interest_ops(&self) -> i32 {
            self.state.lock().expect("mock state").interest_ops
        }
        fn is_mute(&self) -> bool {
            let s = self.state.lock().expect("mock state");
            s.is_open && (s.interest_ops & OP_READ) == 0
        }
        fn has_bytes_buffered(&self) -> bool {
            false
        }
        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 0)))
        }
        fn peer_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from(([127, 0, 0, 1], 9092)))
        }
    }

    fn build_channel() -> (KafkaChannel, Arc<std::sync::Mutex<MockState>>) {
        use crate::common::network::authenticator::PlaintextAuthenticator;
        let (transport, state) = MockTransport::new();
        let channel = KafkaChannel::new(
            Arc::from("0"),
            Box::new(transport),
            Box::new(PlaintextAuthenticator::new()),
            1024,
            Box::new(DefaultChannelMetadataRegistry::new()),
        );
        (channel, state)
    }

    /// Translation of `KafkaChannelTest.testSending`. The Java version
    /// uses `transport.write(ByteBuffer[])` returning a partial-byte
    /// count; the default `MockTransport::write_vectored` writes
    /// everything at once, so this single-tick test exercises the
    /// lifecycle (`setSend`, `write`, `maybeCompleteSend`,
    /// `IllegalStateException` on double-set). The multi-tick partial-
    /// write contract is exercised by
    /// [`sending_partial_writes_progress_across_multiple_ticks`].
    #[test]
    fn sending_lifecycle() {
        let (mut channel, _state) = build_channel();
        let payload = (0..128u8).collect::<Vec<u8>>();
        let send = ByteBufferSend::size_prefixed(Bytes::from(payload));
        let network_send = NetworkSend::new(channel.id_arc(), Box::new(send));

        channel.set_send(network_send).expect("set send");
        assert!(channel.has_send());

        // Setting again while a send is in progress is illegal.
        let payload2 = (0..128u8).collect::<Vec<u8>>();
        let send2 = ByteBufferSend::size_prefixed(Bytes::from(payload2));
        let dup = NetworkSend::new(channel.id_arc(), Box::new(send2));
        let err = channel.set_send(dup).expect_err("double-set");
        assert!(matches!(err, KafkaError::IllegalState(_)));

        // First write completes the entire send (mock writes everything).
        let written = channel.write().expect("write");
        assert_eq!(written, 4 + 128);
        let completed = channel.maybe_complete_send().expect("complete");
        assert_eq!(completed.size(), 4 + 128);
        assert!(!channel.has_send());
    }

    /// Mirrors the Java `KafkaChannelTest.testSending` partial-write
    /// progression: with `transport.write` capped to return 4, then 64,
    /// then 64 bytes, drive three ticks asserting `maybe_complete_send`
    /// returns `None` until the third tick. This locks in the
    /// `bytes_remaining > 0 → maybe_complete_send() == None` invariant
    /// the Selector relies on to schedule another write tick when the
    /// kernel TCP buffer is full.
    #[test]
    fn sending_partial_writes_progress_across_multiple_ticks() {
        let (mut channel, state) = build_channel();
        let payload = (0..128u8).collect::<Vec<u8>>();
        let send = ByteBufferSend::size_prefixed(Bytes::from(payload));
        let network_send = NetworkSend::new(channel.id_arc(), Box::new(send));

        channel.set_send(network_send).expect("set send");
        assert!(channel.has_send());

        // Tick 1: cap at 4 bytes — only the size header lands.
        set_max_bytes_per_write(&state, 4);
        let written = channel.write().expect("write 1");
        assert_eq!(written, 4);
        // Send is not complete yet — must return None so the Selector
        // re-schedules OP_WRITE on the next tick.
        assert!(
            channel.maybe_complete_send().is_none(),
            "partial send (4/132 bytes written) must not return a completed send"
        );
        assert!(channel.has_send());

        // Tick 2: cap at 64 bytes — half the payload.
        set_max_bytes_per_write(&state, 64);
        let written = channel.write().expect("write 2");
        assert_eq!(written, 64);
        assert!(
            channel.maybe_complete_send().is_none(),
            "partial send (68/132 bytes written) must not return a completed send"
        );
        assert!(channel.has_send());

        // Tick 3: cap at 64 bytes — the remaining payload finishes the send.
        set_max_bytes_per_write(&state, 64);
        let written = channel.write().expect("write 3");
        assert_eq!(written, 64);
        let completed = channel.maybe_complete_send().expect("send completes on tick 3");
        assert_eq!(completed.size(), 4 + 128);
        assert!(!channel.has_send());
    }

    /// Translation of `KafkaChannelTest.testReceiving`. Drives the read
    /// path through three ticks. Java's mock returns the size header
    /// then `0` from subsequent internal `read` calls in tick 1, then
    /// `Mockito.reset(transport)` replaces the answer for tick 2 / 3.
    /// We mirror by enqueuing exactly the bytes each tick is supposed
    /// to consume — `MockTransport::read` returns `Ok(0)` when the
    /// queue is empty, matching Java's `thenReturn(0)`.
    #[test]
    fn receiving_lifecycle() {
        let (mut channel, state) = build_channel();
        // Tick 1: enqueue only the size header. The internal second
        // read inside `NetworkReceive::read_from` (which fills the
        // payload buffer) will see an empty queue and return Ok(0),
        // matching Java's `thenReturn(0)` chain.
        enqueue_read(&state, 128i32.to_be_bytes().to_vec());

        let n = channel.read().expect("read 1");
        assert_eq!(n, 4);
        // Java: assertEquals(4, channel.currentReceive().bytesRead());
        assert_eq!(channel.current_receive().expect("receive").bytes_read(), 4);
        assert!(channel.maybe_complete_receive().is_none());

        // Tick 2: enqueue 64 payload bytes. The size header is already
        // parsed so `read_from` only fills the payload region.
        enqueue_read(&state, vec![0xAB; 64]);
        let n = channel.read().expect("read 2");
        assert_eq!(n, 64);
        assert_eq!(channel.current_receive().expect("receive").bytes_read(), 68);
        assert!(channel.maybe_complete_receive().is_none());

        // Tick 3: enqueue final 64 payload bytes — completes.
        enqueue_read(&state, vec![0xCD; 64]);
        let n = channel.read().expect("read 3");
        assert_eq!(n, 64);
        assert_eq!(channel.current_receive().expect("receive").bytes_read(), 132);
        let received = channel.maybe_complete_receive().expect("complete");
        assert_eq!(received.payload().expect("payload").len(), 128);
        assert!(channel.current_receive().is_none());
    }

    #[test]
    fn ready_requires_transport_and_authenticator() {
        let (channel, _state) = build_channel();
        // PlaintextAuthenticator is always complete; MockTransport is
        // ready by default.
        assert!(channel.ready());
    }

    #[test]
    fn channel_id_used_for_equality_and_hash() {
        use std::collections::HashSet;
        let (a, _sa) = build_channel();
        let (b, _sb) = build_channel();
        assert_eq!(a, b, "two channels with the same id must compare equal");
        let mut set = HashSet::new();
        set.insert(a.id_arc());
        assert!(set.contains(&b.id_arc()));
    }

    #[test]
    fn mute_unmute_lifecycle() {
        let (mut channel, _state) = build_channel();
        assert!(!channel.is_muted());
        assert_eq!(channel.mute_state(), ChannelMuteState::NotMuted);
        channel.mute();
        assert!(channel.is_muted());
        assert_eq!(channel.mute_state(), ChannelMuteState::Muted);
        // Re-mute is a no-op.
        channel.mute();
        assert_eq!(channel.mute_state(), ChannelMuteState::Muted);
        // Unmute returns true (back to NotMuted).
        assert!(channel.maybe_unmute());
        assert!(!channel.is_muted());
    }

    /// Mute-state transitions for the server-only states. Even though
    /// the producer never originates these events, the state-machine
    /// table must match Java byte-for-byte (CLAUDE.md rule on
    /// preserving original architecture).
    #[test]
    fn mute_event_state_machine() {
        let (mut channel, _state) = build_channel();
        channel.mute();
        // Muted -> RequestReceived -> MutedAndResponsePending
        channel
            .handle_channel_mute_event(ChannelMuteEvent::RequestReceived)
            .expect("transition");
        assert_eq!(channel.mute_state(), ChannelMuteState::MutedAndResponsePending);
        // ThrottleStarted -> MutedAndThrottledAndResponsePending
        channel
            .handle_channel_mute_event(ChannelMuteEvent::ThrottleStarted)
            .expect("transition");
        assert_eq!(channel.mute_state(), ChannelMuteState::MutedAndThrottledAndResponsePending);
        // ThrottleEnded from MutedAndThrottledAndResponsePending ->
        // MutedAndResponsePending
        channel
            .handle_channel_mute_event(ChannelMuteEvent::ThrottleEnded)
            .expect("transition");
        assert_eq!(channel.mute_state(), ChannelMuteState::MutedAndResponsePending);
        // ResponseSent from MutedAndResponsePending -> Muted
        channel
            .handle_channel_mute_event(ChannelMuteEvent::ResponseSent)
            .expect("transition");
        assert_eq!(channel.mute_state(), ChannelMuteState::Muted);
    }

    #[test]
    fn invalid_mute_transition_returns_illegal_state() {
        let (mut channel, _state) = build_channel();
        // From NotMuted, RequestReceived has no transition (only Muted
        // accepts it) — must return IllegalState.
        let err = channel
            .handle_channel_mute_event(ChannelMuteEvent::RequestReceived)
            .expect_err("illegal");
        assert!(matches!(err, KafkaError::IllegalState(_)));
    }

    #[test]
    fn disconnect_marks_channel_disconnected() {
        let (mut channel, _state) = build_channel();
        assert!(!channel.is_disconnected());
        channel.disconnect();
        assert!(channel.is_disconnected());
    }

    #[test]
    fn close_releases_resources() {
        let (mut channel, _state) = build_channel();
        channel.close().expect("close");
        assert!(channel.is_disconnected());
        // Idempotent — calling close again should not error.
        channel.close().expect("close idempotent");
    }

    #[test]
    fn finish_connect_captures_remote_address_and_advances_state() {
        use crate::common::network::authenticator::PlaintextAuthenticator;
        let (transport, state) = MockTransport::new();
        // Force the connect-pending pattern: pre-set `connected = false`,
        // then call finish_connect through the channel.
        {
            let mut s = state.lock().expect("mock state");
            s.connected = false;
            s.interest_ops = OP_CONNECT;
        }
        let mut channel = KafkaChannel::new(
            Arc::from("0"),
            Box::new(transport),
            Box::new(PlaintextAuthenticator::new()),
            1024,
            Box::new(DefaultChannelMetadataRegistry::new()),
        );
        let connected = channel.finish_connect().expect("finish_connect");
        assert!(connected);
        // After finish_connect, the channel is in Ready (transport is
        // ready & authenticator is complete).
        assert_eq!(channel.state().state(), ChannelStateName::Ready);
    }
}
