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

//! A Kafka connection channel with transport, authentication, and I/O.
//!
//! Translated from `org.apache.kafka.common.network.KafkaChannel`.
//!
//! Each instance has:
//! - A unique ID identifying it in the `Selector`
//! - A reference to the underlying [`TransportLayer`] for reading and writing
//! - An [`Authenticator`] that performs authentication
//! - A [`NetworkReceive`] representing the current in-progress receive, if any
//! - A [`NetworkSend`] representing the current in-progress send, if any
//! - A [`ChannelMuteState`] to document if the channel has been muted

use super::Authenticator;
use super::ChannelMetadataRegistry;
use super::KafkaSend;
use super::NetworkReceive;
use super::NetworkSend;
use super::Receive;
use super::authentication_error::is_authentication_error;
use super::channel_state::State;
use super::{ChannelState, channel_state};
use super::{InterestOps, TransportLayer};

use std::io;
use std::net::SocketAddr;

/// Minimum interval between re-authentication attempts: 1 second in nanoseconds.
const MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS: u64 = 1_000_000_000;

/// Mute states for KafkaChannel.
///
/// - `NotMuted`: Channel is not muted. This is the default state.
/// - `Muted`: Channel is muted. Channel must be in this state to be unmuted.
/// - `MutedAndResponsePending`: (SocketServer only) Channel is muted and a response
///   has not been sent back to the client yet.
/// - `MutedAndThrottled`: (SocketServer only) Channel is muted and throttling is in
///   progress due to quota violation.
/// - `MutedAndThrottledAndResponsePending`: (SocketServer only) Channel is muted,
///   throttling is in progress, and a response is pending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelMuteState {
    /// Channel is not muted.
    NotMuted,
    /// Channel is muted.
    Muted,
    /// Muted with response pending.
    MutedAndResponsePending,
    /// Muted and throttled.
    MutedAndThrottled,
    /// Muted, throttled, and response pending.
    MutedAndThrottledAndResponsePending,
}

/// Socket server events that change the mute state.
///
/// Valid transitions:
/// - `RequestReceived`: `Muted` => `MutedAndResponsePending`
/// - `ResponseSent`: `MutedAndResponsePending` => `Muted`,
///   `MutedAndThrottledAndResponsePending` => `MutedAndThrottled`
/// - `ThrottleStarted`: `MutedAndResponsePending` => `MutedAndThrottledAndResponsePending`
/// - `ThrottleEnded`: `MutedAndThrottled` => `Muted`,
///   `MutedAndThrottledAndResponsePending` => `MutedAndResponsePending`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelMuteEvent {
    /// A request has been received from the client.
    RequestReceived,
    /// A response has been sent out to the client.
    ResponseSent,
    /// Throttling started due to quota violation.
    ThrottleStarted,
    /// Throttling ended.
    ThrottleEnded,
}

/// A Kafka connection channel with transport, authentication, and I/O.
///
/// Translated from `org.apache.kafka.common.network.KafkaChannel`.
///
/// Key differences from Java:
/// - `selectionKey()` eliminated — Selector uses `HashMap<String, KafkaChannel>` keyed by ID
/// - `read()`, `write()`, `prepare()`, `finish_connect()`, `close()` are `async fn`
/// - No `MemoryPool` parameter on `NetworkReceive` creation (pooling deferred)
pub struct KafkaChannel {
    /// Unique channel identifier.
    id: String,
    /// The underlying transport layer.
    transport_layer: Box<dyn TransportLayer>,
    /// The authenticator for this channel.
    authenticator: Box<dyn Authenticator>,
    /// Maximum receive size in bytes.
    max_receive_size: i32,
    /// Channel metadata registry.
    metadata_registry: Box<dyn ChannelMetadataRegistry>,
    /// Current in-progress receive, if any.
    receive: Option<NetworkReceive>,
    /// Current in-progress send, if any.
    send: Option<NetworkSend>,
    /// Whether the channel has been disconnected.
    disconnected: bool,
    /// Current mute state.
    mute_state: ChannelMuteState,
    /// Current channel state.
    state: ChannelState,
    /// Remote address, captured before finishConnect.
    remote_address: Option<SocketAddr>,
    /// Number of successful authentications.
    successful_authentications: u32,
    /// Whether a write is mid-progress.
    mid_write: bool,
    /// Accumulated network thread time in nanoseconds.
    network_thread_time_nanos: u64,
    /// Time of last re-authentication start in nanoseconds.
    last_reauthentication_start_nanos: u64,
}

impl KafkaChannel {
    /// Creates a new `KafkaChannel` with the given ID, transport layer, authenticator,
    /// maximum receive size, and metadata registry.
    pub fn new(
        id: &str,
        transport_layer: Box<dyn TransportLayer>,
        authenticator: Box<dyn Authenticator>,
        max_receive_size: i32,
        metadata_registry: Box<dyn ChannelMetadataRegistry>,
    ) -> Self {
        Self {
            id: id.to_string(),
            transport_layer,
            authenticator,
            max_receive_size,
            metadata_registry,
            receive: None,
            send: None,
            disconnected: false,
            mute_state: ChannelMuteState::NotMuted,
            state: channel_state::NOT_CONNECTED.clone(),
            remote_address: None,
            successful_authentications: 0,
            mid_write: false,
            network_thread_time_nanos: 0,
            last_reauthentication_start_nanos: 0,
        }
    }

    /// Closes the channel.
    pub async fn close(&mut self) -> io::Result<()> {
        self.disconnected = true;
        // Close transport layer
        let transport_result = self.transport_layer.close().await;
        // Close authenticator
        self.authenticator.close();
        // Close metadata registry
        self.metadata_registry.close();
        transport_result
    }

    /// Does handshake of transport layer and authentication using configured authenticator.
    ///
    /// For SSL with client authentication enabled, `TransportLayer::handshake()` performs
    /// authentication. For SASL, authentication is performed by `Authenticator::authenticate()`.
    ///
    /// # Errors
    ///
    /// Returns an error if the handshake or authentication fails.
    pub async fn prepare(&mut self) -> io::Result<()> {
        let mut authenticating = false;
        let result: io::Result<()> = async {
            if !self.transport_layer.ready() {
                self.transport_layer.handshake().await?;
            }
            if self.transport_layer.ready() && !self.authenticator.complete() {
                authenticating = true;
                let auth = &mut *self.authenticator;
                let transport = &mut *self.transport_layer;
                auth.authenticate(transport).await?;
            }
            Ok(())
        }
        .await;

        if let Err(e) = result {
            // Mirror Java's `catch (AuthenticationException)` in
            // KafkaChannel.prepare(): only genuine authentication failures
            // (typed `AuthenticationError`) move the channel to
            // AUTHENTICATION_FAILED — "Clients are notified of authentication
            // exceptions to enable operations to be terminated without retries".
            // Any other error (e.g. a TCP connection-reset during the TLS
            // handshake) is "handled as a network exception in Selector": the
            // channel state is left as-is (Authenticate) and the error is
            // returned unchanged, so the selector/network client treat it as a
            // retriable network disconnect and reconnect with backoff.
            if is_authentication_error(&e) {
                let remote_desc = self.remote_address.map(|a| a.to_string());
                self.state =
                    ChannelState::with_error(State::AuthenticationFailed, &e.to_string(), remote_desc.as_deref());
                if authenticating {
                    self.delay_close_on_authentication_failure();
                }
            }
            return Err(e);
        }

        if self.ready() {
            self.successful_authentications += 1;
            self.state = channel_state::READY.clone();
        }
        Ok(())
    }

    /// Disconnects the channel.
    pub fn disconnect(&mut self) {
        self.disconnected = true;
        if self.state == channel_state::NOT_CONNECTED
            && let Some(addr) = &self.remote_address
        {
            // If we captured the remote address we can provide more information
            self.state = ChannelState::with_remote_address(State::NotConnected, &addr.to_string());
        }
        self.transport_layer.disconnect();
    }

    /// Sets the channel state.
    pub fn set_state(&mut self, state: ChannelState) {
        self.state = state;
    }

    /// Returns the channel state.
    pub fn state(&self) -> &ChannelState {
        &self.state
    }

    /// Finishes the connection process.
    ///
    /// Captures the remote address before `finish_connect()` is called, since it
    /// becomes inaccessible if the connection was refused.
    pub async fn finish_connect(&mut self) -> io::Result<bool> {
        // Grab remote address before finishConnect() — it becomes
        // inaccessible if the connection was refused.
        if let Ok(addr) = self.transport_layer.peer_addr() {
            self.remote_address = Some(addr);
        }

        let connected = self.transport_layer.finish_connect().await?;
        if connected {
            if self.ready() {
                self.state = channel_state::READY.clone();
            } else if let Some(addr) = self.remote_address {
                self.state = ChannelState::with_remote_address(State::Authenticate, &addr.to_string());
            } else {
                self.state = channel_state::AUTHENTICATE.clone();
            }
        }
        Ok(connected)
    }

    /// Returns `true` if the underlying transport is connected.
    pub fn is_connected(&self) -> bool {
        self.transport_layer.is_connected()
    }

    /// Whether the transport supports non-blocking `try_read` (plaintext does;
    /// SSL/mocks do not). Lets the selector drain it in a tight loop.
    pub fn supports_try_read(&self) -> bool {
        self.transport_layer.supports_try_read()
    }

    /// Bytes already read into the in-progress receive (0 if none / fresh).
    /// Used by the selector to avoid yielding mid-message on a wakeup.
    pub fn current_receive_bytes_read(&self) -> usize {
        self.receive.as_ref().map_or(0, |r| r.bytes_read())
    }

    /// Returns the channel ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Externally muting a channel should be done via selector to ensure proper
    /// state handling.
    pub(crate) fn mute(&mut self) {
        if self.mute_state == ChannelMuteState::NotMuted {
            if !self.disconnected {
                self.transport_layer.remove_interest_ops(InterestOps::OP_READ);
            }
            self.mute_state = ChannelMuteState::Muted;
        }
    }

    /// Unmute the channel. The channel can be unmuted only if it is in the `Muted` state.
    /// For other muted states (`MutedAnd*`), this is a no-op.
    ///
    /// Returns `true` if the channel is in the `NotMuted` state after the call.
    #[allow(dead_code)]
    pub(crate) fn maybe_unmute(&mut self) -> bool {
        if self.mute_state == ChannelMuteState::Muted {
            if !self.disconnected {
                self.transport_layer.add_interest_ops(InterestOps::OP_READ);
            }
            self.mute_state = ChannelMuteState::NotMuted;
        }
        self.mute_state == ChannelMuteState::NotMuted
    }

    /// Handle the specified channel mute-related event and transition the mute state
    /// according to the state machine.
    ///
    /// # Panics
    ///
    /// Panics if the event is not valid for the current state.
    pub fn handle_channel_mute_event(&mut self, event: ChannelMuteEvent) {
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
            panic!("Cannot transition from {:?} for {:?}", self.mute_state, event);
        }
    }

    /// Returns the current mute state.
    pub fn mute_state(&self) -> ChannelMuteState {
        self.mute_state
    }

    /// Delay channel close on authentication failure.
    ///
    /// This removes the write interest from the channel until
    /// `complete_close_on_authentication_failure()` is called.
    fn delay_close_on_authentication_failure(&mut self) {
        self.transport_layer.remove_interest_ops(InterestOps::OP_WRITE);
    }

    /// Finish up any processing on `prepare()` failure.
    #[allow(dead_code)]
    pub(crate) fn complete_close_on_authentication_failure(&mut self) -> io::Result<()> {
        self.transport_layer.add_interest_ops(InterestOps::OP_WRITE);
        self.authenticator.handle_authentication_failure()
    }

    /// Returns `true` if this channel has been explicitly muted.
    pub fn is_muted(&self) -> bool {
        self.mute_state != ChannelMuteState::NotMuted
    }

    /// Returns `true` if the channel is in a state where it could be muted
    /// due to memory pressure.
    pub fn is_in_mutable_state(&self) -> bool {
        // Some requests do not require memory, so if we do not know what the
        // current (or future) request is (receive == None) we don't mute.
        // We also don't mute if whatever memory required has already been
        // successfully allocated.
        match &self.receive {
            None => false,
            Some(recv) => {
                if recv.memory_allocated() {
                    return false;
                }
                // Also cannot mute if underlying transport is not in the ready state
                self.transport_layer.ready()
            },
        }
    }

    /// Returns `true` if the channel is ready (transport ready and authentication complete).
    pub fn ready(&self) -> bool {
        self.transport_layer.ready() && self.authenticator.complete()
    }

    /// Returns `true` if there is an in-progress send.
    pub fn has_send(&self) -> bool {
        self.send.is_some()
    }

    /// Returns `true` if the transport layer has buffered ciphertext or other
    /// pending bytes that need to be flushed to the socket. Used by the
    /// selector to decide whether to register write-interest for a handshaking
    /// channel.
    pub(crate) fn has_pending_writes(&self) -> bool {
        self.transport_layer.has_pending_writes()
    }

    /// Sets the send for this channel.
    ///
    /// # Errors
    ///
    /// Returns an error if there is already an in-progress send.
    pub fn set_send(&mut self, send: NetworkSend) -> Result<(), String> {
        if self.send.is_some() {
            return Err(format!(
                "Attempt to begin a send operation with prior send operation still in progress, connection id is {}",
                self.id
            ));
        }
        self.send = Some(send);
        self.transport_layer.add_interest_ops(InterestOps::OP_WRITE);
        Ok(())
    }

    /// If the current send is complete, returns it and clears the send state.
    pub fn maybe_complete_send(&mut self) -> Option<NetworkSend> {
        if self.send.as_ref().is_some_and(|s| s.completed()) {
            self.mid_write = false;
            self.transport_layer.remove_interest_ops(InterestOps::OP_WRITE);
            self.send.take()
        } else {
            None
        }
    }

    /// Poll-style read-readiness pass-through to the underlying transport
    /// layer. Side-effect-free (registers the waker only); used by the
    /// selector's single non-allocating readiness future (Phase 23).
    pub(crate) fn poll_transport_readable(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
        self.transport_layer.poll_readable(cx)
    }

    /// Poll-style write-readiness pass-through to the underlying transport layer.
    pub(crate) fn poll_transport_writable(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
        self.transport_layer.poll_writable(cx)
    }

    /// Reads data from the transport layer into the current receive buffer.
    ///
    /// Creates a new `NetworkReceive` if there is no current receive.
    pub async fn read(&mut self) -> io::Result<usize> {
        if self.receive.is_none() {
            self.receive = Some(NetworkReceive::with_max_size(self.max_receive_size, &self.id));
        }

        let bytes_received = {
            let receive = self.receive.as_mut().unwrap();
            let transport = &mut *self.transport_layer;
            receive.read_from(transport).await?
        };

        // Check if we should mute due to memory pressure
        if let Some(ref recv) = self.receive
            && recv.required_memory_amount_known()
            && !recv.memory_allocated()
            && self.is_in_mutable_state()
        {
            // Pool must be out of memory, mute ourselves.
            self.mute();
        }

        Ok(bytes_received)
    }

    /// Synchronous, non-blocking mirror of [`read`](Self::read).
    ///
    /// Calls [`NetworkReceive::try_read_from`] instead of awaiting
    /// [`Receive::read_from`], which in turn calls
    /// [`TransportLayer::try_read`] directly without `Box::pin(async {…})`,
    /// without `tokio::time::timeout(Duration::ZERO, …)` and without any
    /// timer-driver registration. Used by `Selector::attempt_read`.
    ///
    /// On `WouldBlock` from the transport, returns `Ok(0)` (handled inside
    /// `try_read_from`); the caller treats that as "no progress this tick"
    /// and retries on the next selector iteration.
    pub fn try_read(&mut self) -> io::Result<usize> {
        if self.receive.is_none() {
            self.receive = Some(NetworkReceive::with_max_size(self.max_receive_size, &self.id));
        }

        let bytes_received = {
            let receive = self.receive.as_mut().unwrap();
            let transport = &mut *self.transport_layer;
            receive.try_read_from(transport)?
        };

        // Check if we should mute due to memory pressure
        if let Some(ref recv) = self.receive
            && recv.required_memory_amount_known()
            && !recv.memory_allocated()
            && self.is_in_mutable_state()
        {
            self.mute();
        }

        Ok(bytes_received)
    }

    /// Returns the current in-progress receive, if any.
    pub fn current_receive(&self) -> Option<&NetworkReceive> {
        self.receive.as_ref()
    }

    /// If the current receive is complete, returns it and clears the receive state.
    pub fn maybe_complete_receive(&mut self) -> Option<NetworkReceive> {
        if self.receive.as_ref().is_some_and(|r| r.complete()) {
            self.receive.take()
        } else {
            None
        }
    }

    /// Writes data from the current send to the transport layer.
    ///
    /// Returns the number of bytes written.
    pub async fn write(&mut self) -> io::Result<usize> {
        if self.send.is_none() {
            return Ok(0);
        }

        self.mid_write = true;
        let transport = &mut *self.transport_layer;
        let send = self.send.as_mut().unwrap();
        match send.try_write_to(transport) {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Socket buffer is full. Fall back to async with zero timeout
                // to avoid blocking.
                match tokio::time::timeout(std::time::Duration::ZERO, send.write_to(transport)).await {
                    Ok(Ok(n)) => Ok(n),
                    Ok(Err(e)) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
                    Ok(Err(e)) => Err(e),
                    Err(_elapsed) => Ok(0),
                }
            },
            result => result,
        }
    }

    /// Writes data with true async yielding for concurrent I/O across channels.
    ///
    /// Unlike `write()` which uses `timeout(Duration::ZERO)` to prevent blocking,
    /// this method lets the underlying TLS write suspend at TCP wait points.
    /// Safe when called from a concurrent context (e.g. `join_all`) because
    /// suspending one channel's write allows other channels' writes to proceed.
    pub async fn write_concurrent(&mut self) -> io::Result<usize> {
        if self.send.is_none() {
            return Ok(0);
        }

        self.mid_write = true;
        let transport = &mut *self.transport_layer;
        let send = self.send.as_mut().unwrap();
        match send.try_write_to(transport) {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Socket buffer is full. Bound the await so the caller (and
                // the read pass on the next poll iteration) can make progress.
                // With a single channel, an unbounded await deadlocks the
                // test echo server because the client never re-enters its
                // read pass to drain the response data, which back-pressures
                // the server's reads. With multiple channels, a small budget
                // still permits useful overlap of one channel's TCP wait with
                // another channel's encrypt+write.
                match tokio::time::timeout(std::time::Duration::from_millis(1), send.write_to(transport)).await {
                    Ok(Ok(n)) => Ok(n),
                    Ok(Err(e)) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
                    Ok(Err(e)) => Err(e),
                    Err(_elapsed) => Ok(0),
                }
            },
            result => result,
        }
    }

    /// Accumulates network thread time for this channel.
    pub fn add_network_thread_time_nanos(&mut self, nanos: u64) {
        self.network_thread_time_nanos += nanos;
    }

    /// Returns accumulated network thread time for this channel and resets
    /// the value to zero.
    pub fn get_and_reset_network_thread_time_nanos(&mut self) -> u64 {
        let current = self.network_thread_time_nanos;
        self.network_thread_time_nanos = 0;
        current
    }

    /// Returns `true` if the underlying transport has bytes remaining to be read
    /// from any intermediate buffers.
    pub fn has_bytes_buffered(&self) -> bool {
        self.transport_layer.has_bytes_buffered()
    }

    /// Returns the number of successful authentications.
    pub fn successful_authentications(&self) -> u32 {
        self.successful_authentications
    }

    /// Returns the re-authentication latency in milliseconds, if applicable.
    pub fn reauthentication_latency_ms(&self) -> Option<u64> {
        self.authenticator.reauthentication_latency_ms()
    }

    /// Returns `true` if this is a server-side channel and the given time is past
    /// the session expiration time.
    pub fn server_authentication_session_expired(&self, now_nanos: u64) -> bool {
        if let Some(expiration) = self.authenticator.server_session_expiration_time_nanos() {
            now_nanos > expiration
        } else {
            false
        }
    }

    /// Returns the client-side `NetworkReceive` response that arrived during
    /// re-authentication that is unrelated to re-authentication, if any.
    pub fn poll_response_received_during_reauthentication(&mut self) -> Option<NetworkReceive> {
        self.authenticator.poll_response_received_during_reauthentication()
    }

    /// Returns `true` if this is a server-side channel and the connected client
    /// has indicated that it supports re-authentication.
    pub fn connected_client_supports_reauthentication(&self) -> bool {
        self.authenticator.connected_client_supports_reauthentication()
    }

    /// Returns a reference to the channel metadata registry.
    pub fn channel_metadata_registry(&mut self) -> &mut dyn ChannelMetadataRegistry {
        &mut *self.metadata_registry
    }

    /// Maybe add write interest after re-authentication. This ensures that any
    /// pending write operation is resumed.
    pub fn maybe_add_write_interest_after_reauth(&mut self) {
        if self.send.is_some() {
            self.transport_layer.add_interest_ops(InterestOps::OP_WRITE);
        }
    }

    /// Returns a description of the socket for logging.
    pub fn socket_description(&self) -> String {
        match self.transport_layer.peer_addr() {
            Ok(addr) => addr.to_string(),
            Err(_) => "unknown".to_string(),
        }
    }

    /// If this is a server-side connection that has an expiration time and at least
    /// 1 second has passed since the prior re-authentication (if any) started then
    /// begin the process of re-authenticating the connection and return true,
    /// otherwise return false.
    ///
    /// For PLAINTEXT, this always returns `false` since re-authentication does not
    /// apply.
    pub fn maybe_begin_server_reauthentication(
        &mut self,
        _sasl_handshake_network_receive: &NetworkReceive,
        now_nanos_supplier: impl FnOnce() -> u64,
    ) -> io::Result<bool> {
        if !self.ready() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "KafkaChannel should be \"ready\" when processing SASL Handshake for potential re-authentication",
            ));
        }
        if self.authenticator.server_session_expiration_time_nanos().is_none() {
            return Ok(false);
        }
        let now_nanos = now_nanos_supplier();
        if self.last_reauthentication_start_nanos != 0
            && now_nanos - self.last_reauthentication_start_nanos < MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS
        {
            return Ok(false);
        }
        self.last_reauthentication_start_nanos = now_nanos;
        self.authenticator.reauthenticate()?;
        Ok(true)
    }

    /// If this is a client-side connection that is not muted, there is no
    /// in-progress write, and there is a session expiration time defined that has
    /// passed, then begin the process of re-authenticating and return true,
    /// otherwise return false.
    ///
    /// For PLAINTEXT, this always returns `false`.
    pub fn maybe_begin_client_reauthentication(
        &mut self,
        now_nanos_supplier: impl FnOnce() -> u64,
    ) -> io::Result<bool> {
        if !self.ready() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "KafkaChannel should always be \"ready\" when it is checked for possible re-authentication",
            ));
        }
        if self.mute_state != ChannelMuteState::NotMuted
            || self.mid_write
            || self.authenticator.client_session_reauthentication_time_nanos().is_none()
        {
            return Ok(false);
        }
        let now_nanos = now_nanos_supplier();
        if now_nanos < self.authenticator.client_session_reauthentication_time_nanos().unwrap() {
            return Ok(false);
        }
        self.receive = None;
        self.authenticator.reauthenticate()?;
        Ok(true)
    }

    /// Returns a mutable reference to the transport layer.
    ///
    /// This is used by the Selector for non-blocking I/O operations.
    #[allow(dead_code)]
    pub(crate) fn transport_layer(&mut self) -> &mut dyn TransportLayer {
        &mut *self.transport_layer
    }

    /// Returns `true` if the transport layer is open.
    #[allow(dead_code)]
    pub(crate) fn is_open(&self) -> bool {
        self.transport_layer.is_open()
    }

    /// Returns the remote address, if known.
    pub fn remote_address(&self) -> Option<SocketAddr> {
        self.remote_address
    }
}

impl PartialEq for KafkaChannel {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for KafkaChannel {}

impl std::hash::Hash for KafkaChannel {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl std::fmt::Display for KafkaChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KafkaChannel id={}", self.id)
    }
}

impl std::fmt::Debug for KafkaChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaChannel")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("mute_state", &self.mute_state)
            .field("disconnected", &self.disconnected)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::network::ByteBufferSend;
    use crate::common::network::DefaultChannelMetadataRegistry;
    use crate::common::network::InterestOps;
    use crate::common::network::authentication_error::auth_io_error;

    use std::future::Future;
    use std::io;
    use std::net::SocketAddr;
    use std::pin::Pin;

    /// Mock transport layer for testing KafkaChannel.
    ///
    /// Supports configurable read/write behavior through closures.
    struct MockTransportLayer {
        ready: bool,
        connected: bool,
        open: bool,
        read_data: Vec<u8>,
        read_pos: usize,
        write_results: Vec<io::Result<usize>>,
        interest_ops: InterestOps,
        /// When set, `handshake()` returns this error and `ready()` stays false,
        /// simulating a failed TLS handshake. The kind/payload of the error
        /// controls auth-vs-disconnect classification in `prepare()`.
        handshake_error: Option<Box<dyn Fn() -> io::Error + Send + Sync>>,
    }

    impl MockTransportLayer {
        fn new() -> Self {
            Self {
                ready: true,
                connected: true,
                open: true,
                read_data: Vec::new(),
                read_pos: 0,
                write_results: Vec::new(),
                interest_ops: InterestOps::OP_READ,
                handshake_error: None,
            }
        }

        fn with_read_data(mut self, data: Vec<u8>) -> Self {
            self.read_data = data;
            self
        }

        fn with_write_results(mut self, results: Vec<io::Result<usize>>) -> Self {
            self.write_results = results;
            self
        }

        /// Configures the mock to fail `handshake()` with the error produced by
        /// `make_err`, leaving the transport not-ready (mirrors a failed TLS
        /// handshake before the data path opens).
        fn with_handshake_error(mut self, make_err: impl Fn() -> io::Error + Send + Sync + 'static) -> Self {
            self.ready = false;
            self.handshake_error = Some(Box::new(make_err));
            self
        }
    }

    impl TransportLayer for MockTransportLayer {
        fn peer_addr(&self) -> io::Result<SocketAddr> {
            Ok("127.0.0.1:9092".parse().unwrap())
        }

        fn ready(&self) -> bool {
            self.ready
        }

        fn finish_connect(&mut self) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + '_>> {
            Box::pin(async { Ok(true) })
        }

        fn disconnect(&mut self) {
            self.connected = false;
        }

        fn is_connected(&self) -> bool {
            self.connected
        }

        fn handshake(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            if let Some(make_err) = &self.handshake_error {
                let err = make_err();
                return Box::pin(async move { Err(err) });
            }
            Box::pin(async { Ok(()) })
        }

        fn add_interest_ops(&mut self, ops: InterestOps) {
            self.interest_ops |= ops;
        }

        fn remove_interest_ops(&mut self, ops: InterestOps) {
            self.interest_ops = self.interest_ops.remove(ops);
        }

        fn is_mute(&self) -> bool {
            !self.interest_ops.contains(InterestOps::OP_READ)
        }

        fn has_bytes_buffered(&self) -> bool {
            false
        }

        fn has_pending_writes(&self) -> bool {
            false
        }

        fn is_open(&self) -> bool {
            self.open
        }

        fn close(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            self.open = false;
            Box::pin(async { Ok(()) })
        }

        fn readable(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn writable(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn poll_readable(&self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_writable(&self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn read<'a>(&'a mut self, dst: &'a mut [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            let remaining = self.read_data.len() - self.read_pos;
            let to_read = remaining.min(dst.len());
            if to_read == 0 {
                return Box::pin(async { Err(io::Error::from(io::ErrorKind::WouldBlock)) });
            }
            dst[..to_read].copy_from_slice(&self.read_data[self.read_pos..self.read_pos + to_read]);
            self.read_pos += to_read;
            Box::pin(async move { Ok(to_read) })
        }

        fn write<'a>(&'a mut self, _src: &'a [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            Box::pin(async { Ok(0) })
        }

        fn write_vectored<'a>(
            &'a mut self,
            srcs: &'a [io::IoSlice<'a>],
        ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            if !self.write_results.is_empty() {
                let first = self.write_results.remove(0);
                let result = match first {
                    Ok(n) => Ok(n),
                    Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
                };
                Box::pin(async { result })
            } else {
                let total: usize = srcs.iter().map(|s| s.len()).sum();
                Box::pin(async move { Ok(total) })
            }
        }
    }

    /// Mock authenticator for testing.
    struct MockAuthenticator {
        complete: bool,
        /// When set (and not yet complete), `authenticate()` fails with this
        /// error, simulating a SASL authentication failure.
        auth_error: Option<Box<dyn Fn() -> io::Error + Send + Sync>>,
    }

    impl MockAuthenticator {
        fn new(complete: bool) -> Self {
            Self { complete, auth_error: None }
        }

        /// Configures the authenticator to fail `authenticate()` with the error
        /// produced by `make_err` (the transport handshake is assumed ready).
        fn with_auth_error(mut self, make_err: impl Fn() -> io::Error + Send + Sync + 'static) -> Self {
            self.auth_error = Some(Box::new(make_err));
            self
        }
    }

    impl Authenticator for MockAuthenticator {
        fn authenticate<'a>(
            &'a mut self,
            _transport: &'a mut (dyn TransportLayer + Send),
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
            if let Some(make_err) = &self.auth_error {
                let err = make_err();
                return Box::pin(async move { Err(err) });
            }
            Box::pin(async { Ok(()) })
        }

        fn complete(&self) -> bool {
            self.complete
        }

        fn close(&mut self) {}
    }

    /// Translated from `KafkaChannelTest.testSending` in
    /// `org.apache.kafka.common.network.KafkaChannelTest`.
    #[tokio::test]
    async fn test_sending() {
        let transport = MockTransportLayer::new().with_write_results(vec![
            Ok(4),  // First write: 4 bytes
            Ok(64), // Second write: 64 bytes
            Ok(64), // Third write: 64 bytes
        ]);
        let authenticator = MockAuthenticator::new(true);
        let metadata = DefaultChannelMetadataRegistry::new();

        let mut channel =
            KafkaChannel::new("0", Box::new(transport), Box::new(authenticator), 1024, Box::new(metadata));

        let send = ByteBufferSend::size_prefixed(bytes::Bytes::from(vec![0xABu8; 128]));
        let network_send = NetworkSend::new("0", Box::new(send));

        channel.set_send(network_send).unwrap();
        assert!(channel.has_send());

        // Duplicate send should fail
        let send2 = ByteBufferSend::size_prefixed(bytes::Bytes::from(vec![0xCDu8; 32]));
        let network_send2 = NetworkSend::new("0", Box::new(send2));
        assert!(channel.set_send(network_send2).is_err());

        // First write: 4 bytes
        let written = channel.write().await.unwrap();
        assert_eq!(4, written);
        assert!(channel.maybe_complete_send().is_none());

        // Second write: 64 bytes
        let written = channel.write().await.unwrap();
        assert_eq!(64, written);
        assert!(channel.maybe_complete_send().is_none());

        // Third write: 64 bytes (completes the send)
        let written = channel.write().await.unwrap();
        assert_eq!(64, written);
        assert!(channel.maybe_complete_send().is_some());
    }

    /// Translated from `KafkaChannelTest.testReceiving` in
    /// `org.apache.kafka.common.network.KafkaChannelTest`.
    #[tokio::test]
    async fn test_receiving() {
        // Build read data: 4-byte size header (128) + 128 bytes of payload
        let mut read_data = Vec::new();
        read_data.extend_from_slice(&128_i32.to_be_bytes());
        read_data.extend_from_slice(&[0xABu8; 128]);

        let transport = MockTransportLayer::new().with_read_data(read_data);
        let authenticator = MockAuthenticator::new(true);
        let metadata = DefaultChannelMetadataRegistry::new();

        let mut channel =
            KafkaChannel::new("0", Box::new(transport), Box::new(authenticator), 1024, Box::new(metadata));

        // First read: should read the 4-byte size header + 128 bytes payload
        // (all available in the mock)
        let bytes_read = channel.read().await.unwrap();
        assert!(bytes_read > 0);
        // The total bytes read should be 4 (header) + 128 (payload) = 132
        assert_eq!(132, channel.current_receive().unwrap().bytes_read());
        // The receive should be complete since we have all 128 bytes
        let completed = channel.maybe_complete_receive();
        assert!(completed.is_some());
        assert!(channel.current_receive().is_none());
    }

    #[test]
    fn test_mute_state_machine() {
        let transport = MockTransportLayer::new();
        let authenticator = MockAuthenticator::new(true);
        let metadata = DefaultChannelMetadataRegistry::new();

        let mut channel =
            KafkaChannel::new("0", Box::new(transport), Box::new(authenticator), 1024, Box::new(metadata));

        assert_eq!(ChannelMuteState::NotMuted, channel.mute_state());
        assert!(!channel.is_muted());

        channel.mute();
        assert_eq!(ChannelMuteState::Muted, channel.mute_state());
        assert!(channel.is_muted());

        channel.handle_channel_mute_event(ChannelMuteEvent::RequestReceived);
        assert_eq!(ChannelMuteState::MutedAndResponsePending, channel.mute_state());

        channel.handle_channel_mute_event(ChannelMuteEvent::ThrottleStarted);
        assert_eq!(ChannelMuteState::MutedAndThrottledAndResponsePending, channel.mute_state());

        channel.handle_channel_mute_event(ChannelMuteEvent::ResponseSent);
        assert_eq!(ChannelMuteState::MutedAndThrottled, channel.mute_state());

        channel.handle_channel_mute_event(ChannelMuteEvent::ThrottleEnded);
        assert_eq!(ChannelMuteState::Muted, channel.mute_state());

        assert!(channel.maybe_unmute());
        assert_eq!(ChannelMuteState::NotMuted, channel.mute_state());
    }

    /// Regression: a transient transport-level I/O error during the TLS
    /// handshake (e.g. "Connection reset by peer", os error 104) must NOT move
    /// the channel to AUTHENTICATION_FAILED. Java's KafkaChannel.prepare() only
    /// catches `AuthenticationException`; a plain `IOException` is "handled as a
    /// network exception in Selector" — a retriable disconnect. The channel
    /// state is left as-is (Authenticate) so the network client reconnects.
    #[tokio::test]
    async fn test_prepare_handshake_reset_is_not_authentication_failed() {
        let transport = MockTransportLayer::new().with_handshake_error(|| {
            io::Error::new(io::ErrorKind::ConnectionReset, "Connection reset by peer (os error 104)")
        });
        // Authenticator never reached because the handshake fails first.
        let authenticator = MockAuthenticator::new(false);
        let metadata = DefaultChannelMetadataRegistry::new();
        let mut channel =
            KafkaChannel::new("0", Box::new(transport), Box::new(authenticator), 1024, Box::new(metadata));

        let err = channel.prepare().await.expect_err("handshake reset must surface an error");
        // The error is a transport disconnect, not an auth failure.
        assert!(!is_authentication_error(&err));
        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
        // The channel state must NOT be AuthenticationFailed (it remains in the
        // pre-prepare state, NotConnected here, since finish_connect was not
        // called in this unit test).
        assert_ne!(channel.state().state(), State::AuthenticationFailed);
    }

    /// Regression: a transient EOF during the TLS handshake (peer closed mid-
    /// handshake) is likewise a retriable disconnect, not an auth failure.
    #[tokio::test]
    async fn test_prepare_handshake_eof_is_not_authentication_failed() {
        let transport = MockTransportLayer::new()
            .with_handshake_error(|| io::Error::new(io::ErrorKind::UnexpectedEof, "TLS handshake EOF"));
        let authenticator = MockAuthenticator::new(false);
        let metadata = DefaultChannelMetadataRegistry::new();
        let mut channel =
            KafkaChannel::new("0", Box::new(transport), Box::new(authenticator), 1024, Box::new(metadata));

        let err = channel.prepare().await.expect_err("handshake EOF must surface an error");
        assert!(!is_authentication_error(&err));
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_ne!(channel.state().state(), State::AuthenticationFailed);
    }

    /// Regression: a genuine TLS negotiation/certificate failure (the rustls
    /// analogue of Java's `SSLException`, surfaced as a typed authentication
    /// error) MUST move the channel to AUTHENTICATION_FAILED (fatal), mirroring
    /// Java's `maybeProcessHandshakeFailure` -> `SslAuthenticationException`.
    #[tokio::test]
    async fn test_prepare_tls_negotiation_failure_is_authentication_failed() {
        let transport = MockTransportLayer::new()
            .with_handshake_error(|| auth_io_error("TLS handshake failed: invalid peer certificate"));
        let authenticator = MockAuthenticator::new(false);
        let metadata = DefaultChannelMetadataRegistry::new();
        let mut channel =
            KafkaChannel::new("0", Box::new(transport), Box::new(authenticator), 1024, Box::new(metadata));

        let err = channel
            .prepare()
            .await
            .expect_err("TLS negotiation failure must surface an error");
        assert!(is_authentication_error(&err));
        // Display is the payload's, i.e. Java's `toString()` form. The bare message
        // is still available on the channel state, asserted below.
        assert_eq!(
            err.to_string(),
            "AuthenticationError: TLS handshake failed: invalid peer certificate"
        );
        assert_eq!(channel.state().state(), State::AuthenticationFailed);
        // The channel state records the rendered error, so it carries the same
        // class-prefixed form.
        assert_eq!(
            channel.state().error(),
            Some("AuthenticationError: TLS handshake failed: invalid peer certificate")
        );
    }

    /// Regression: a genuine SASL credential rejection (the broker returns an
    /// error during authentication; Java throws `SaslAuthenticationException`,
    /// an `AuthenticationException`) MUST move the channel to
    /// AUTHENTICATION_FAILED (fatal), not be silently treated as a disconnect.
    #[tokio::test]
    async fn test_prepare_sasl_auth_failure_is_authentication_failed() {
        // Handshake succeeds (transport ready), authenticator fails with a typed
        // auth error.
        let transport = MockTransportLayer::new();
        let authenticator = MockAuthenticator::new(false)
            .with_auth_error(|| auth_io_error("Authentication failed: Invalid username or password"));
        let metadata = DefaultChannelMetadataRegistry::new();
        let mut channel =
            KafkaChannel::new("0", Box::new(transport), Box::new(authenticator), 1024, Box::new(metadata));

        let err = channel.prepare().await.expect_err("SASL auth failure must surface an error");
        assert!(is_authentication_error(&err));
        assert_eq!(
            err.to_string(),
            "AuthenticationError: Authentication failed: Invalid username or password"
        );
        assert_eq!(channel.state().state(), State::AuthenticationFailed);
    }
}
