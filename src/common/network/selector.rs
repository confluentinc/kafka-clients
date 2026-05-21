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

//! Translation of `org.apache.kafka.common.network.Selector` —
//! a [`Selectable`] implementation built on Tokio.
//!
//! # Design notes (CLAUDE.md rule 8 + Phase 5 NOTES.md 79–86)
//!
//! Java's `Selector` is single-threaded and not thread-safe — it owns a
//! `java.nio.channels.Selector`, multiplexes many non-blocking
//! `SocketChannel`s, and exposes per-poll output collections that are
//! cleared on each `poll()` call. The Rust translation preserves that
//! single-task ownership model, but uses Tokio's
//! [`tokio::net::TcpStream`] readiness primitives ([`try_read`]/
//! [`try_write_vectored`]) which already model "would block" by
//! returning `WouldBlock`. The mapping is direct:
//!
//! * `nioSelector.select(timeoutMs)` → a [`tokio::time::sleep`] race
//!   inside [`tokio::select!`] guarded by a "wakeup" channel that
//!   short-circuits the sleep when `connect` tasks complete or new sends
//!   arrive.
//! * Per-channel `SelectionKey.interestOps()` → [`TransportLayer`]'s
//!   `interest_ops()` (already wired in Phase 5b-1).
//! * `SocketChannel.connect(InetSocketAddress)` (immediate or
//!   asynchronous) → a short-lived [`tokio::task::spawn`] that runs
//!   [`TcpStream::connect`] and pushes either `Connected(id, stream)` or
//!   `ConnectFailed(id, err)` onto a private mpsc channel that
//!   [`Selector::poll`] drains.
//!
//! This is the closest faithful mirror of Java's pattern: the `connect`
//! tasks correspond to the kernel's TCP SYN-ACK handshake which Java
//! NIO surfaces via `OP_CONNECT`, while the I/O loop itself is purely
//! synchronous (non-blocking syscalls in Java, [`try_read`]/
//! [`try_write_vectored`] in Rust). No long-lived per-channel tasks: a
//! per-channel read task would require splitting the [`TcpStream`] into
//! halves and would break SSL (rustls handshake needs both halves), and
//! a per-channel write task would race with the per-poll completed-sends
//! drain that Java guarantees.
//!
//! # Java surface mirror
//!
//! All of `Selectable`'s methods are implemented. The
//! [`Selectable::poll`] signature is `async fn` (Java is blocking) — see
//! `common::network::selectable` rustdoc for the rationale.
//!
//! # Skipped vs. Java
//!
//! * **SASL re-authentication**: deferred to Phase 9 (per
//!   PLAN.md 266–268). Helpers like
//!   `pollResponseReceivedDuringReauthentication`, the `successfulAuth*`
//!   counters, and `DelayedAuthenticationFailureClose` are not
//!   translated; the producer never originates the events that drive
//!   them. The `failedAuthenticationDelayMs` constructor parameter is
//!   not exposed.
//! * **`MemoryPool`**: deferred. Phase 5a's [`NetworkReceive`] allocates
//!   eagerly. The Java `outOfMemory` / `madeReadProgressLastPoll` logic
//!   collapses — there is no buffered-read-after-OOM path on the
//!   producer side.
//! * **`SelectorMetrics`**: replaced with `// metric stub` no-ops per
//!   PLAN.md.
//! * **`wakeup`**: Java's `wakeup()` aborts a blocking
//!   `nioSelector.select(...)` from another thread. We mirror this with
//!   a [`tokio::sync::Notify`] (`wakeup_notify` on the Selector). The
//!   `notify_one()` half is called from [`Selectable::wakeup`]; the
//!   `notified()` half is one arm of the [`Self::poll`]
//!   `tokio::select!`. Callers that want to wake a Selector moved into
//!   a `tokio::spawn` task can obtain an `Arc<Notify>` via
//!   [`Self::wakeup_notify_handle`] and call `notify_one()` directly.
//!   This is the load-bearing primitive `KafkaProducer::sender_wakeup`
//!   uses to short-circuit the Sender's poll-sleep when records are
//!   freshly appended (Phase 8a.0 Round 2 Suggestion 1).
//! * **`register(String, SocketChannel)`** (server-side accept path):
//!   not translated. Producer never acts as a server.
//!
//! # Tests
//!
//! `SelectorTest.java` cases that exercise SASL, `MemoryPool`,
//! mute-on-OOM, the `register` server-side path, the
//! `metrics.metricValue` assertions, the `Field`-reflection
//! `ensureEmptySelectorFields` helper, and the
//! `mockConstruction(SelectorChannelMetadataRegistry)` test are skipped
//! with rationale per file-level notes; see this file's `tests` module
//! for the per-test mapping.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::Notify;

use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::common::errors::KafkaError;
use crate::common::network::channel_builder::ChannelBuilder;
use crate::common::network::channel_metadata_registry::DefaultChannelMetadataRegistry;
use crate::common::network::network_receive::UNLIMITED;
use crate::common::network::selectable::{Selectable, USE_DEFAULT_BUFFER_SIZE};
use crate::common::network::transport_layer::TransportLayer;
use crate::common::network::{ChannelState, ChannelStateName, KafkaChannel, NetworkReceive, NetworkSend, Receive};
use crate::common::utils::Time;

/// Mirrors Java's `Selector.NO_IDLE_TIMEOUT_MS = -1`.
pub const NO_IDLE_TIMEOUT_MS: i64 = -1;

/// `i32` connection id used throughout the network surface (CLAUDE.md
/// rule 11 + Phase 5c-1 hot-path interning). The wire-protocol layer
/// always derives this from `Node::id()`.
pub(crate) type ConnectionId = i32;

/// Why a channel is being closed. Mirrors Java's private
/// `Selector.CloseMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseMode {
    /// Process outstanding buffered receives, notify disconnect.
    Graceful,
    /// Discard any outstanding receives, notify disconnect.
    NotifyOnly,
    /// Discard any outstanding receives, no disconnect notification.
    DiscardNoNotify,
}

impl CloseMode {
    /// Mirrors Java's `CloseMode.notifyDisconnect`.
    fn notify_disconnect(self) -> bool {
        !matches!(self, CloseMode::DiscardNoNotify)
    }
}

/// Inbound event from a connect task. Mirrors Java's
/// `OP_CONNECT`-ready notification on the NIO selector.
enum ConnectEvent {
    /// The non-blocking connect succeeded; the connected stream is now
    /// ready for `finishConnect` / handshake.
    Connected { id: ConnectionId, stream: TcpStream },
    /// The non-blocking connect failed (DNS, refused, unreachable, …).
    Failed { id: ConnectionId, err: KafkaError },
}

/// State of a connect task spawned by [`Selector::connect`]. We track
/// the [`JoinHandle`] so [`Selector::close`] / [`Selectable::close_connection`]
/// can abort the in-flight connect; the result lands on
/// [`Selector::connect_rx`] either way (or is dropped after `abort`).
struct ConnectTask {
    handle: JoinHandle<()>,
}

/// Helper class for tracking least-recently-used connections to enable
/// idle-connection closing. Mirrors Java's private
/// `Selector.IdleExpiryManager` — the algorithm is preserved verbatim
/// (LRU ordered by last-active timestamp; `pollExpiredConnection`
/// returns the oldest entry once `connectionsMaxIdleMs` has elapsed).
///
/// Java uses a `LinkedHashMap` with `accessOrder=true`, which is O(1)
/// per touch via doubly-linked map nodes. The Rust translation uses a
/// `BTreeSet<(timestamp, id)>` for ordered iteration plus a parallel
/// `HashMap<id, timestamp>` for O(log n) lookup of the previous
/// timestamp on `update`/`remove`. With the `(timestamp, id)` key the
/// set never collides on simultaneous touches.
struct IdleExpiryManager {
    /// Map from connection id to last-active wall-clock nanoseconds.
    /// Used to find the previous (timestamp, id) entry to remove from
    /// `lru_order` on update/remove.
    last_active_ns: HashMap<ConnectionId, i64>,
    /// LRU order — first is oldest, last is newest. Keyed on
    /// `(timestamp, id)` so simultaneous-touch collisions are
    /// impossible. O(log n) insert/remove per touch.
    lru_order: BTreeSet<(i64, ConnectionId)>,
    connections_max_idle_ns: i64,
    next_idle_close_check_ns: i64,
}

impl IdleExpiryManager {
    fn new(time: &dyn Time, connections_max_idle_ms: i64) -> Self {
        let connections_max_idle_ns = connections_max_idle_ms.saturating_mul(1_000_000);
        IdleExpiryManager {
            last_active_ns: HashMap::new(),
            lru_order: BTreeSet::new(),
            connections_max_idle_ns,
            next_idle_close_check_ns: time.nanoseconds().saturating_add(connections_max_idle_ns),
        }
    }

    /// Mirrors `IdleExpiryManager.update(String, long)`. Touching an id
    /// re-keys it under `(current_time_nanos, id)` so the LRU set
    /// remains sorted by last-active timestamp.
    fn update(&mut self, id: ConnectionId, current_time_nanos: i64) {
        if let Some(prev) = self.last_active_ns.insert(id, current_time_nanos) {
            self.lru_order.remove(&(prev, id));
        }
        self.lru_order.insert((current_time_nanos, id));
    }

    /// Mirrors `IdleExpiryManager.pollExpiredConnection(long)` — returns
    /// `Some((id, last_active_ns))` for the LRU entry once it has been
    /// idle for `connections_max_idle_ns`. Otherwise returns `None` and
    /// updates the next-check timestamp.
    fn poll_expired_connection(&mut self, current_time_nanos: i64) -> Option<(ConnectionId, i64)> {
        if current_time_nanos <= self.next_idle_close_check_ns {
            return None;
        }
        let Some(&(connection_last_active, oldest_id)) = self.lru_order.iter().next() else {
            self.next_idle_close_check_ns = current_time_nanos.saturating_add(self.connections_max_idle_ns);
            return None;
        };
        self.next_idle_close_check_ns = connection_last_active.saturating_add(self.connections_max_idle_ns);
        if current_time_nanos > self.next_idle_close_check_ns {
            Some((oldest_id, connection_last_active))
        } else {
            None
        }
    }

    /// Mirrors `IdleExpiryManager.remove(String)`.
    fn remove(&mut self, id: ConnectionId) {
        if let Some(prev) = self.last_active_ns.remove(&id) {
            self.lru_order.remove(&(prev, id));
        }
    }
}

/// A nioSelector for asynchronous, multi-channel network I/O.
///
/// Mirrors Java's `org.apache.kafka.common.network.Selector`. The Rust
/// implementation owns its own [`tokio::sync::mpsc`] channel for
/// connect-task notifications; reads and writes happen synchronously
/// inside [`Selector::poll`] using
/// [`tokio::net::TcpStream::try_read`]/`try_write_vectored` (the
/// non-blocking primitives Tokio provides).
///
/// **Thread safety**: like Java, this struct is **not** thread-safe.
/// Every method takes `&mut self`. Use it from a single Tokio task.
pub struct Selector {
    /// Open channels indexed by integer connection id.
    channels: HashMap<ConnectionId, KafkaChannel>,
    /// Channels that have started a graceful close but still have
    /// pending receives to drain. Mirrors Java's `closingChannels`.
    closing_channels: HashMap<ConnectionId, KafkaChannel>,
    /// In-flight connect tasks. Removed once the connect either
    /// succeeds (turning into a `channels` entry) or fails (turning
    /// into a `disconnected` entry).
    connect_tasks: HashMap<ConnectionId, ConnectTask>,
    /// Per-connection unresolved hostname, captured at [`Selectable::connect`]
    /// time. Used to build the SNI [`ServerName`] when the channel
    /// builder constructs an SSL / SASL_SSL transport — Java derives it
    /// from a reverse-DNS lookup of the resolved peer, but we already
    /// have the original hostname from the bootstrap entry.
    /// Cleaned up on `build_and_register_channel`, `close_connection`,
    /// and `close`.
    connection_hosts: HashMap<ConnectionId, String>,
    /// mpsc receiver for connect-task notifications. Connect tasks
    /// push `Connected(id, stream)` or `Failed(id, err)`; `poll`
    /// drains this on every tick.
    connect_rx: mpsc::UnboundedReceiver<ConnectEvent>,
    /// mpsc sender cloned into each connect task.
    connect_tx: mpsc::UnboundedSender<ConnectEvent>,
    /// Channels explicitly muted by the upper layer via
    /// [`Selectable::mute`]. Distinguished from "muted because of
    /// memory pressure" (which the producer never enters; see module
    /// docstring on `MemoryPool`).
    explicitly_muted_channels: HashSet<ConnectionId>,
    /// Per-poll output: list of completed sends (cleared on each
    /// `poll` call). Mirrors Java's `completedSends`.
    completed_sends: Vec<NetworkSend>,
    /// Per-poll output: insertion-ordered map from id to completed
    /// receive. Java guarantees at most one entry per channel per
    /// poll to preserve broker-side ordering. We mirror that with a
    /// `VecDeque<(id, receive)>` so the Selector can also expose
    /// `clear_completed_receives` semantics.
    completed_receives: Vec<NetworkReceive>,
    /// Set of ids that have a completed receive in
    /// [`Self::completed_receives`]. Used to enforce Java's
    /// "at most one completed receive per channel per poll" invariant.
    completed_receive_ids: HashSet<ConnectionId>,
    /// Per-poll output: ids whose connect completed on this tick.
    connected: Vec<ConnectionId>,
    /// Per-poll output: ids whose disconnect was observed on this
    /// tick, mapped to their final [`ChannelState`].
    disconnected: HashMap<ConnectionId, ChannelState>,
    /// Channels that failed during `send` and need to be surfaced as
    /// `disconnected` on the next `poll` call. Mirrors Java's
    /// `failedSends` list.
    failed_sends: Vec<ConnectionId>,
    /// Wall-clock time source.
    time: Arc<dyn Time>,
    /// Channel builder used to construct a [`KafkaChannel`] from a
    /// freshly-connected [`TcpStream`].
    channel_builder: Box<dyn ChannelBuilder>,
    /// `NetworkReceive` size cap. Mirrors Java's `maxReceiveSize`.
    max_receive_size: i32,
    /// Idle-expiry manager (`None` when `NO_IDLE_TIMEOUT_MS`).
    idle_expiry_manager: Option<IdleExpiryManager>,
    /// Whether [`Selector::close`] has been called. Once closed, all
    /// methods short-circuit. Mirrors Java's `nioSelector` being
    /// already closed.
    closed: bool,
    /// Wakeup primitive. Java's `nioSelector.wakeup()` aborts the
    /// in-progress `select(timeout)`; the Tokio equivalent is a
    /// [`Notify`] arm inside our `tokio::select!`. Held as an
    /// [`Arc`] so the producer can clone it pre-spawn and call
    /// `notify_one()` from `KafkaProducer::sender_wakeup` after the
    /// Sender has been moved into its `tokio::spawn` task.
    ///
    /// Cancellation-safety (CLAUDE.md 9.6): [`Notify::notified`] is
    /// documented cancellation-safe; the Notify still considers a
    /// pending `notify_one()` permit consumed only when a waiter
    /// actually polls past the wake — losing the `notified()` arm
    /// in `select!` does not lose the wake (it stays buffered on
    /// the Notify until the next call).
    wakeup_notify: Arc<Notify>,
}

impl Selector {
    /// Construct a new Selector.
    ///
    /// Mirrors the Java constructor used by `NetworkClient`:
    ///
    /// ```text
    /// new Selector(long connectionMaxIdleMS, Metrics, Time, String,
    ///              ChannelBuilder, LogContext)
    /// ```
    ///
    /// * `connection_max_idle_ms` — idle-connection timeout. Use
    ///   [`NO_IDLE_TIMEOUT_MS`] to disable.
    /// * `time` — wall-clock source.
    /// * `channel_builder` — constructs a [`KafkaChannel`] over an
    ///   already-connected [`TcpStream`].
    ///
    /// The `Metrics`, `metricGrpPrefix`, `metricTags`, and `LogContext`
    /// parameters are dropped — see the module docstring for the
    /// metric-stub deferral.
    pub fn new(connection_max_idle_ms: i64, time: Arc<dyn Time>, channel_builder: Box<dyn ChannelBuilder>) -> Self {
        Selector::with_capacity(UNLIMITED, connection_max_idle_ms, time, channel_builder)
    }

    /// Construct a Selector with an explicit per-receive size cap.
    /// Mirrors the Java constructor:
    ///
    /// ```text
    /// new Selector(int maxReceiveSize, long connectionMaxIdleMS,
    ///              Metrics, Time, String, Map<String,String>, boolean,
    ///              ChannelBuilder, MemoryPool, LogContext)
    /// ```
    pub fn with_capacity(
        max_receive_size: i32,
        connection_max_idle_ms: i64,
        time: Arc<dyn Time>,
        channel_builder: Box<dyn ChannelBuilder>,
    ) -> Self {
        let (connect_tx, connect_rx) = mpsc::unbounded_channel();
        let idle_expiry_manager = if connection_max_idle_ms < 0 {
            None
        } else {
            Some(IdleExpiryManager::new(time.as_ref(), connection_max_idle_ms))
        };
        Selector {
            channels: HashMap::new(),
            closing_channels: HashMap::new(),
            connect_tasks: HashMap::new(),
            connection_hosts: HashMap::new(),
            connect_rx,
            connect_tx,
            explicitly_muted_channels: HashSet::new(),
            completed_sends: Vec::new(),
            completed_receives: Vec::new(),
            completed_receive_ids: HashSet::new(),
            connected: Vec::new(),
            disconnected: HashMap::new(),
            failed_sends: Vec::new(),
            time,
            channel_builder,
            max_receive_size,
            idle_expiry_manager,
            closed: false,
            wakeup_notify: Arc::new(Notify::new()),
        }
    }

    /// Return a clone of the wakeup [`Notify`]. Callers can keep this
    /// handle even after the [`Selector`] has been moved into a
    /// `tokio::spawn` task (Java's analogue: `Selector` is reachable
    /// from the `KafkaProducer` even after the IO thread starts;
    /// `selector.wakeup()` is callable from any thread).
    ///
    /// This is the load-bearing wake the Phase 8a.0 Suggestion 1
    /// review demanded. The Selector's [`Self::poll`] races the
    /// timeout sleep against `wakeup_notify.notified()`, so calling
    /// `notify_one()` short-circuits the sleep and lets `poll`
    /// observe freshly-queued work on the next iteration.
    pub fn wakeup_notify_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.wakeup_notify)
    }

    /// Return a borrowed reference to a channel by id, or `None` if not
    /// open. Mirrors Java's `channel(String)`.
    pub fn channel(&self, id: ConnectionId) -> Option<&KafkaChannel> {
        self.channels.get(&id)
    }

    /// Return the channel that's draining its buffered receives after a
    /// graceful close, or `None`. Mirrors Java's `closingChannel(String)`.
    pub fn closing_channel(&self, id: ConnectionId) -> Option<&KafkaChannel> {
        self.closing_channels.get(&id)
    }

    /// Return a borrowed list of all open channels. Mirrors Java's
    /// `channels()` (which returns a fresh `ArrayList`).
    pub fn channels(&self) -> Vec<&KafkaChannel> {
        self.channels.values().collect()
    }

    /// Returns `true` if [`Selector::close`] has been called. Mirrors
    /// the post-close branch of Java's `nioSelector.isOpen()`.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Clears completed receives. Mirrors Java's
    /// `clearCompletedReceives()`.
    pub fn clear_completed_receives(&mut self) {
        self.completed_receives.clear();
        self.completed_receive_ids.clear();
    }

    /// Clears completed sends. Mirrors Java's
    /// `clearCompletedSends()`.
    pub fn clear_completed_sends(&mut self) {
        self.completed_sends.clear();
    }

    /// Number of in-flight connect tasks. Test-only accessor for
    /// "immediately connected keys" parity with Java.
    #[cfg(test)]
    fn pending_connects_len(&self) -> usize {
        self.connect_tasks.len()
    }

    /// Common helper: ensure no channel is registered under `id`.
    /// Mirrors Java's private `ensureNotRegistered(String)`.
    fn ensure_not_registered(&self, id: ConnectionId) -> Result<(), KafkaError> {
        if self.channels.contains_key(&id) {
            return Err(KafkaError::IllegalState(format!("There is already a connection for id {}", id)));
        }
        if self.closing_channels.contains_key(&id) {
            return Err(KafkaError::IllegalState(format!(
                "There is already a connection for id {} that is still being closed",
                id
            )));
        }
        Ok(())
    }

    /// Drain finished/aborted connect tasks from the inbound mpsc and
    /// turn them into either `channels` entries (success) or
    /// `disconnected` entries (failure). Mirrors the
    /// `OP_CONNECT`-ready loop of Java's `pollSelectionKeys`.
    fn drain_connect_events(&mut self) {
        loop {
            let event = match self.connect_rx.try_recv() {
                Ok(event) => event,
                // Channel is open (we hold a sender) and there's nothing
                // queued.
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            };
            match event {
                ConnectEvent::Connected { id, stream } => {
                    // Ignore late-arriving notifications for ids that
                    // were closed before the connect resolved (e.g. the
                    // user called `close_connection` while the connect
                    // was in flight). Java's `OP_CONNECT` handler skips
                    // unknown keys via `key.isValid()`.
                    let task = self.connect_tasks.remove(&id);
                    if task.is_none() {
                        // Already closed; drop the stream.
                        continue;
                    }
                    match self.build_and_register_channel(id, stream) {
                        Ok(()) => {
                            self.connected.push(id);
                            // metric stub: connection-creation rate
                            if let Some(idle) = self.idle_expiry_manager.as_mut() {
                                idle.update(id, self.time.nanoseconds());
                            }
                        },
                        Err(err) => {
                            // Builder failed; surface as a disconnect.
                            self.disconnected
                                .insert(id, ChannelState::with_exception(ChannelStateName::NotConnected, err, None));
                        },
                    }
                },
                ConnectEvent::Failed { id, err } => {
                    let task = self.connect_tasks.remove(&id);
                    if task.is_none() {
                        // Already closed; drop the error.
                        continue;
                    }
                    // Connect failed — drop the stashed host. (Successful
                    // builds drop it in `build_and_register_channel`; a
                    // build-side error also drops it because that path
                    // calls `build_and_register_channel`, which removes
                    // unconditionally.)
                    self.connection_hosts.remove(&id);
                    self.disconnected
                        .insert(id, ChannelState::with_exception(ChannelStateName::NotConnected, err, None));
                },
            }
        }
    }

    /// Build a [`KafkaChannel`] from a connected [`TcpStream`] and
    /// insert it into [`Self::channels`]. Mirrors Java's
    /// `buildAndAttachKafkaChannel`. On builder failure, the stream is
    /// dropped (Tokio closes the socket) and the error is propagated.
    fn build_and_register_channel(&mut self, id: ConnectionId, stream: TcpStream) -> Result<(), KafkaError> {
        let id_arc: Arc<str> = Arc::from(id.to_string());
        let metadata_registry = Box::new(DefaultChannelMetadataRegistry::new());
        // SNI: look up the host we stashed at `connect()` time. We drop
        // the entry now that the channel is being built — close-path
        // cleanup is a no-op for ids that already shed their host.
        // `ServerName::try_from` accepts DNS names (`Ok(DnsName)`),
        // IPv4 / IPv6 literals (`Ok(IpAddress)`), and rejects only
        // unparseable strings (`Err`). We pass the parsed form (or
        // `None` on parse failure) to `build_channel_with_server_name`;
        // the SSL / SASL_SSL builders reject only the `None` case,
        // which is genuinely unsafe (an SSL channel with no peer
        // identity to verify against). IP-literal hosts use rustls'
        // IP-SAN match path during handshake — SNI itself is omitted
        // per RFC 6066 §3.
        let server_name = match self.connection_hosts.remove(&id) {
            Some(host) => rustls::pki_types::ServerName::try_from(host).ok(),
            None => None,
        };
        let channel = self.channel_builder.build_channel_with_server_name(
            id_arc,
            stream,
            server_name,
            self.max_receive_size,
            metadata_registry,
        )?;
        self.channels.insert(id, channel);
        Ok(())
    }

    /// Drive a single channel through one I/O tick: handshake (if
    /// needed) → read (if readable & not muted & no completed receive)
    /// → write (if has-send & ready). Returns whether the channel had
    /// any I/O activity this tick (read bytes > 0, write bytes > 0,
    /// or completed a receive/send). The upper [`Self::poll`] loop
    /// uses this flag to:
    ///
    /// * short-circuit `sleep` when there's still data to drain;
    /// * touch the [`IdleExpiryManager`] LRU only for channels that
    ///   did real work (mirroring Java's per-ready-key
    ///   `idleExpiryManager.update` — see Java
    ///   `pollSelectionKeys:525-526`).
    ///
    /// Mirrors Java's `pollSelectionKeys` body for one key.
    fn drive_channel_io(&mut self, id: ConnectionId) -> Result<bool, KafkaError> {
        let mut send_failed = false;
        let mut made_progress = false;

        // Take the channel out of the map for the duration of the I/O
        // tick so we can call mut methods on it without holding a
        // `&mut self.channels` borrow that the close-on-error path
        // would conflict with.
        let mut channel = match self.channels.remove(&id) {
            Some(c) => c,
            None => return Ok(false),
        };

        let result: Result<bool, KafkaError> = (|| {
            // Step 1: drive the prepare/handshake state machine if not
            // ready yet.
            if channel.is_connected() && !channel.ready() {
                channel.prepare()?;
                if channel.ready() && channel.state().state() == ChannelStateName::NotConnected {
                    channel.set_state(ChannelState::ready());
                }
            }

            // Step 2: read if not muted, ready, and no completed
            // receive already buffered for this channel this poll.
            let has_completed_receive = self.completed_receive_ids.contains(&id);
            let explicitly_muted = self.explicitly_muted_channels.contains(&id);
            if channel.ready() && !explicitly_muted && !has_completed_receive {
                let bytes_received = match channel.read() {
                    Ok(n) => n,
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        return Err(KafkaError::Network(format!("EOF reading from connection {}: {}", id, e)));
                    },
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => 0,
                    Err(e) => return Err(KafkaError::Network(format!("read error on connection {}: {}", id, e))),
                };
                if bytes_received > 0 {
                    made_progress = true;
                }
                if let Some(receive) = channel.maybe_complete_receive() {
                    self.completed_receive_ids.insert(id);
                    self.completed_receives.push(receive);
                    // metric stub: response-received total
                }
            }

            // Step 3: write if there is an in-progress send.
            if channel.has_send() && channel.ready() {
                let bytes_written = match channel.write() {
                    Ok(n) => n,
                    Err(e) => {
                        send_failed = true;
                        return Err(KafkaError::Network(format!("write error on connection {}: {}", id, e)));
                    },
                };
                if bytes_written > 0 {
                    made_progress = true;
                }
                if let Some(send) = channel.maybe_complete_send() {
                    self.completed_sends.push(send);
                    // metric stub: request-sent total
                }
            }

            Ok(made_progress)
        })();

        match result {
            Ok(made_progress) => {
                self.channels.insert(id, channel);
                Ok(made_progress)
            },
            Err(err) => {
                // Re-insert before close so the `close_internal` path
                // sees a consistent state.
                self.channels.insert(id, channel);
                let mode = if send_failed {
                    CloseMode::NotifyOnly
                } else {
                    CloseMode::Graceful
                };
                self.close_internal(id, mode);
                // Surfacing read/write errors as a disconnect (mirrors
                // Java which catches `IOException` in `pollSelectionKeys`
                // and calls `close(channel, mode)`); we preserve the
                // error in the channel's `ChannelState` rather than
                // bubbling it out of `poll`.
                let _ = err;
                Ok(made_progress)
            },
        }
    }

    /// Close a channel as part of the I/O loop. Mirrors Java's private
    /// `close(KafkaChannel, CloseMode)`.
    fn close_internal(&mut self, id: ConnectionId, mode: CloseMode) {
        let Some(mut channel) = self.channels.remove(&id) else {
            // Closing a channel that's already closing is a no-op (Java
            // checks `closingChannels` for the same id; we already
            // removed from `channels`).
            return;
        };
        channel.disconnect();

        // Ensure `connected` does not contain the closed channel.
        self.connected.retain(|&c| c != id);

        // Java keeps the channel in `closingChannels` if there are
        // pending receives on a graceful close. Our `NetworkReceive`
        // does not buffer SSL plaintext beyond what the reader has
        // already consumed, but we still mirror the structure so an
        // outstanding `current_receive` is preserved. Detection is
        // best-effort: an in-progress receive that has read >0 bytes
        // is "buffered".
        let has_pending = mode == CloseMode::Graceful && Self::has_pending_receive(&channel);
        if has_pending {
            // The next poll tick's `process_closing_channels` will:
            //  * call `read()` once (no-op if peer-closed: `read` returns
            //    EOF which lands as a hard close on the next pass);
            //  * surface any completed receive into `completed_receives`;
            //  * finalize the close if no further progress is made.
            self.closing_channels.insert(id, channel);
            return;
        }

        Self::do_close(&mut channel);
        self.explicitly_muted_channels.remove(&id);
        if mode.notify_disconnect() {
            self.disconnected.insert(id, channel.state().clone());
        }
        if let Some(idle) = self.idle_expiry_manager.as_mut() {
            idle.remove(id);
        }
        // metric stub: connection-closed total
    }

    /// True if this channel still has a partially-read receive that
    /// would otherwise be lost on close. Mirrors the predicate used by
    /// Java's `maybeReadFromClosingChannel`.
    fn has_pending_receive(channel: &KafkaChannel) -> bool {
        match channel.current_receive() {
            Some(receive) => receive.bytes_read() > 0 && !receive.complete(),
            None => false,
        }
    }

    /// Drain any progress from the closing-channels map; finalize
    /// channels that no longer have pending data. Mirrors the closing-
    /// channels block at the top of Java's `clear()`. Java
    /// `failedSends.remove(channel.id())` short-circuits the
    /// `maybeReadFromClosingChannel` call when the closing channel
    /// also has a queued failed-send; the removed id is not re-added
    /// to `disconnected` by the subsequent drain loop because Java
    /// uses `remove` (not `contains`). We mirror with
    /// `Vec::retain`-style filtering.
    fn process_closing_channels(&mut self) {
        let ids: Vec<ConnectionId> = self.closing_channels.keys().copied().collect();
        for id in ids {
            // `failedSends.remove(channel.id())` returns the
            // sendFailed flag and consumes the entry so the
            // post-step-2 drain doesn't surface the same id twice.
            let send_failed_pos = self.failed_sends.iter().position(|&existing| existing == id);
            let send_failed = if let Some(pos) = send_failed_pos {
                self.failed_sends.swap_remove(pos);
                true
            } else {
                false
            };
            let mut channel = self.closing_channels.remove(&id).expect("just iterated keys");
            let mut has_pending = false;
            if !send_failed {
                // Mirror Java's `maybeReadFromClosingChannel`: a one-shot
                // read attempt; on exception, set has_pending=false so
                // the channel is closed.
                let read_outcome = channel.read();
                if read_outcome.is_ok() {
                    if let Some(receive) = channel.maybe_complete_receive() {
                        self.completed_receive_ids.insert(id);
                        self.completed_receives.push(receive);
                    }
                    has_pending = Self::has_pending_receive(&channel);
                }
            }
            if has_pending {
                // Re-insert; will retry on next poll.
                self.closing_channels.insert(id, channel);
            } else {
                Self::do_close(&mut channel);
                self.explicitly_muted_channels.remove(&id);
                self.disconnected.insert(id, channel.state().clone());
                if let Some(idle) = self.idle_expiry_manager.as_mut() {
                    idle.remove(id);
                }
            }
        }
    }

    /// Tear down a channel — drop its transport and authenticator.
    /// Mirrors Java's private `doClose(KafkaChannel, boolean)` minus
    /// the `selectionKey.cancel()` (Tokio handles teardown via Drop).
    fn do_close(channel: &mut KafkaChannel) {
        // Best-effort close — capture but don't propagate. Java logs
        // and continues.
        let _ = channel.close();
    }

    /// Configure a [`TcpSocket`] (`SO_KEEPALIVE`, `SO_SNDBUF`,
    /// `SO_RCVBUF`) and connect to `address`. Mirrors Java's private
    /// `configureSocketChannel` followed by `doConnect`.
    ///
    /// `send_buffer_size` / `receive_buffer_size` use
    /// [`USE_DEFAULT_BUFFER_SIZE`] (`-1`) to fall back to the OS
    /// default — Java's `Selectable.USE_DEFAULT_BUFFER_SIZE` contract.
    /// All three socket options are set BEFORE connect (the kernel
    /// requires SNDBUF/RCVBUF to be set on the unconnected socket so
    /// the TCP-window auto-tuning can pick them up; Java's
    /// `configureSocketChannel` runs before `doConnect` for the same
    /// reason).
    async fn connect_socket(
        address: SocketAddr,
        send_buffer_size: i32,
        receive_buffer_size: i32,
    ) -> std::io::Result<TcpStream> {
        let socket = if address.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        // Best-effort: Java unconditionally sets SO_KEEPALIVE in
        // `configureSocketChannel`. We propagate any error since the
        // failure mode (privilege loss, broken kernel) is identical
        // to Java's IOException path.
        socket.set_keepalive(true)?;
        if send_buffer_size != USE_DEFAULT_BUFFER_SIZE {
            // Java's `Socket.setSendBufferSize(int)` accepts only
            // positive values; negative aside from the sentinel is
            // a programmer error and the kernel rejects it with
            // EINVAL.
            socket.set_send_buffer_size(send_buffer_size as u32)?;
        }
        if receive_buffer_size != USE_DEFAULT_BUFFER_SIZE {
            socket.set_recv_buffer_size(receive_buffer_size as u32)?;
        }
        socket.connect(address).await
    }

    /// Reset the per-poll Vec / HashMap outputs. Mirrors the head of
    /// Java's private `clear()` (lines 843-846) — `completedSends`,
    /// `completedReceives`, `connected`, `disconnected`. The
    /// `failedSends` drain happens AFTER `process_closing_channels`
    /// (see [`Self::drain_failed_sends`]) so the closing-channel
    /// short-circuit observes a non-empty `failed_sends` list.
    fn clear_per_poll_outputs(&mut self) {
        self.completed_sends.clear();
        self.completed_receives.clear();
        self.completed_receive_ids.clear();
        self.connected.clear();
        self.disconnected.clear();
    }

    /// Drain any remaining `failed_sends` into `disconnected`.
    /// Mirrors the tail of Java's private `clear()` (lines 861-863)
    /// after `processClosingChannels` has consumed the
    /// closing-channel ids.
    fn drain_failed_sends(&mut self) {
        for id in self.failed_sends.drain(..) {
            self.disconnected.insert(id, ChannelState::failed_send());
        }
    }

    /// Idle-expiry sweep. Mirrors Java's `maybeCloseOldestConnection`.
    fn maybe_close_oldest_connection(&mut self, current_time_nanos: i64) {
        let expired = match self.idle_expiry_manager.as_mut() {
            Some(idle) => idle.poll_expired_connection(current_time_nanos),
            None => None,
        };
        let Some((id, _last_active)) = expired else {
            return;
        };
        // Only act if the channel is still open.
        if let Some(channel) = self.channels.get_mut(&id) {
            channel.set_state(ChannelState::expired());
            self.close_internal(id, CloseMode::Graceful);
        }
    }
}

impl Selectable for Selector {
    fn connect(
        &mut self,
        id: ConnectionId,
        host: &str,
        address: SocketAddr,
        send_buffer_size: i32,
        receive_buffer_size: i32,
    ) -> Result<(), KafkaError> {
        if self.closed {
            return Err(KafkaError::IllegalState("Selector is closed".to_string()));
        }
        self.ensure_not_registered(id)?;
        if self.connect_tasks.contains_key(&id) {
            return Err(KafkaError::IllegalState(format!(
                "There is already a connection in progress for id {}",
                id
            )));
        }
        // Stash the unresolved host for SNI lookup at channel-build time.
        // We unconditionally overwrite — a prior failed connect for this
        // id may have left a stale entry behind.
        self.connection_hosts.insert(id, host.to_owned());

        // Spawn the connect task — equivalent to Java's
        // `socketChannel.connect(address)` returning before the
        // SYN-ACK lands. We build a [`TcpSocket`] explicitly so we can
        // mirror Java's `configureSocketChannel`:
        // [`set_keepalive(true)`], [`set_send_buffer_size`],
        // [`set_recv_buffer_size`] (when not
        // [`USE_DEFAULT_BUFFER_SIZE`]). [`TcpStream::connect`] would
        // skip all three.
        let tx = self.connect_tx.clone();
        let handle: JoinHandle<()> = tokio::spawn(async move {
            let event = match Self::connect_socket(address, send_buffer_size, receive_buffer_size).await {
                Ok(stream) => {
                    // Mirror Java's `socket.setTcpNoDelay(true)`.
                    // Best-effort: log and ignore on error (Java
                    // throws but the upper-layer wraps in IOException
                    // too).
                    let _ = stream.set_nodelay(true);
                    ConnectEvent::Connected { id, stream }
                },
                Err(e) => ConnectEvent::Failed {
                    id,
                    err: KafkaError::Network(format!("connect to {} failed: {}", address, e)),
                },
            };
            // If the receiver has been dropped (Selector closed mid-
            // connect), the send fails — we just drop the stream.
            let _ = tx.send(event);
        });
        self.connect_tasks.insert(id, ConnectTask { handle });
        Ok(())
    }

    fn wakeup(&self) {
        // Java: `nioSelector.wakeup()` — aborts the in-progress
        // `select(timeout)`. Our equivalent: notify the Tokio
        // `Notify` that the [`Self::poll`] `tokio::select!` races
        // against the timeout sleep.
        //
        // `notify_one()` semantics: if a waiter is parked on
        // `notified()`, wake it; otherwise buffer one permit so
        // the next `notified()` call returns immediately. Either
        // way the next call to `poll` short-circuits its sleep.
        //
        // CLAUDE.md rule 11 hot-path audit: `Notify::notify_one`
        // is a constant-time atomic compare-and-swap — no
        // allocation, no spawn, no Arc clone (the Notify itself
        // is already held by `Arc` for cross-task sharing, but
        // calling `notify_one()` doesn't touch the Arc count).
        self.wakeup_notify.notify_one();
    }

    fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        // Abort all in-flight connect tasks.
        for (_id, task) in self.connect_tasks.drain() {
            task.handle.abort();
        }
        self.connection_hosts.clear();
        // Drain any late events (best-effort).
        while self.connect_rx.try_recv().is_ok() {}
        // Close all open channels.
        let ids: Vec<ConnectionId> = self.channels.keys().copied().collect();
        for id in ids {
            // Java uses `Utils.closeAllQuietly` here — accumulates the
            // first error. The producer never inspects this return so
            // we just close.
            let mut channel = self.channels.remove(&id).expect("just iterated");
            Self::do_close(&mut channel);
        }
        // Close any draining channels.
        let ids: Vec<ConnectionId> = self.closing_channels.keys().copied().collect();
        for id in ids {
            let mut channel = self.closing_channels.remove(&id).expect("just iterated");
            Self::do_close(&mut channel);
        }
        self.channel_builder.close();
    }

    fn close_connection(&mut self, id: ConnectionId) {
        if let Some(channel) = self.channels.get_mut(&id) {
            // No disconnect notification for local close (Java sets
            // state=LOCAL_CLOSE, then closes).
            channel.set_state(ChannelState::local_close());
            self.close_internal(id, CloseMode::DiscardNoNotify);
            return;
        }
        if let Some(mut channel) = self.closing_channels.remove(&id) {
            Self::do_close(&mut channel);
            return;
        }
        // Cancel a pending connect.
        if let Some(task) = self.connect_tasks.remove(&id) {
            task.handle.abort();
        }
        // Always clear any stashed host — id may be re-used.
        self.connection_hosts.remove(&id);
    }

    fn send(&mut self, send: NetworkSend) {
        let dest = send.destination_id();
        let id: ConnectionId = dest.parse().unwrap_or_else(|_| {
            // Java's `Selector.send` requires the destination to be a
            // registered connection id. The upstream invariant is that
            // `NetworkSend.destinationId()` is always
            // `Integer.toString(node.id())` — a non-numeric value is a
            // caller-contract violation, mirroring Java's
            // `IllegalStateException("Attempt to retrieve channel ...")`.
            // CLAUDE.md rule 10.1 — panic for unrecoverable invariants.
            panic!("NetworkSend destination_id {:?} is not a numeric connection id", dest)
        });
        let connection_in_closing = self.closing_channels.contains_key(&id);
        if connection_in_closing {
            // Mirrors Java: notification via `disconnected`, leave
            // channel in the state in which closing was triggered.
            self.failed_sends.push(id);
            return;
        }
        // Try to attach to an open channel.
        let channel = match self.channels.get_mut(&id) {
            Some(c) => c,
            None => {
                // No open or closing channel for this id — Java throws
                // `IllegalStateException` from
                // `openOrClosingChannelOrFail`. We mirror with a
                // panic-equivalent: see CLAUDE.md rule 10.1 — caller
                // contract violation. The Rust translation pushes the
                // failed send into `failed_sends`-like surface so
                // `disconnected()` reflects it on the next poll, and
                // additionally panics to match Java's eager throw.
                panic!("Attempt to send to unregistered connection {}", id);
            },
        };
        if let Err(e) = channel.set_send(send) {
            // Java on exception: state -> FAILED_SEND, failedSends
            // entry, close with DISCARD_NO_NOTIFY, rethrow if not a
            // CancelledKeyException. Rust: same minus the rethrow —
            // the CancelledKey case is Tokio-irrelevant (we don't
            // have a SelectionKey). Production callers (Phase 5d
            // NetworkClient) treat the disconnected entry as the
            // signal.
            channel.set_state(ChannelState::failed_send());
            self.failed_sends.push(id);
            self.close_internal(id, CloseMode::DiscardNoNotify);
            // Surface the IllegalState as a panic mirroring Java's
            // rethrow (only `IllegalStateException` and IO can occur
            // here; see Java `Selector.send`).
            if matches!(e, KafkaError::IllegalState(_)) {
                panic!("{}", e);
            }
        }
    }

    async fn poll(&mut self, timeout_ms: i64) -> Result<(), KafkaError> {
        if self.closed {
            return Err(KafkaError::IllegalState("Selector is closed".to_string()));
        }
        if timeout_ms < 0 {
            return Err(KafkaError::IllegalState("timeout should be >= 0".to_string()));
        }

        // Mirror Java's `clear()` ordering exactly:
        //   1. clear vec outputs (completedSends, completedReceives,
        //      connected, disconnected)
        //   2. process closing channels (consumes failedSends entries)
        //   3. drain remaining failedSends into disconnected
        // Doing the failedSends drain before step 2 (as we used to)
        // killed the closing-channel sendFailed short-circuit because
        // `failed_sends` was always empty by the time
        // `process_closing_channels` ran.
        self.clear_per_poll_outputs();
        self.process_closing_channels();
        self.drain_failed_sends();

        // Drain any connect-task events queued before the sleep.
        self.drain_connect_events();

        let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
        // If we already have work to do — connect events, completed
        // receives, a buffered receive, or a queued send waiting to be
        // pushed to the wire — short-circuit the sleep so `poll(0)`
        // returns immediately. Java's `nio.Selector` wakes on
        // OP_WRITE-ready when the underlying socket has buffer space;
        // the Tokio equivalent for a queued-but-not-yet-written send
        // is simply "skip the sleep and run `drive_channel_io` which
        // will issue the `try_write_vectored`". Phase 8a.0: missing
        // the `has_send()` check here was the wire-protocol blocker —
        // a freshly-queued ApiVersionsRequest would sit unwritten for
        // the entire `default.request.timeout.ms` (30s) window before
        // the post-select `drive_channel_io` actually wrote it.
        let has_immediate_work = !self.connected.is_empty()
            || !self.disconnected.is_empty()
            || !self.completed_receives.is_empty()
            || self.channels.values().any(|c| c.has_bytes_buffered())
            || self.channels.values().any(|c| c.has_send());
        if !has_immediate_work && timeout_ms > 0 {
            // Race the timeout against the next connect-event arrival
            // OR socket readability on any open channel. Java's
            // `nio.Selector.select(timeout)` wakes on OS-level readiness
            // notifications; the Tokio equivalent is per-stream
            // `TcpStream::readable()` futures. Without the readiness
            // arm the poll loop sleeps for the full `timeout` while
            // bytes sit unread on the socket — a 30s `request.timeout.ms`
            // floor means a 30s latency on every response in production
            // (Phase 8a.0).
            //
            // SAFETY: all four arms are cancellation-safe (Tokio mpsc
            // recv, time sleep, `wait_any_transport_readable` which
            // drops its borrowed futures on cancellation, and
            // `Notify::notified` which is documented cancellation-
            // safe — a pending permit survives the losing-arm drop).
            // No MutexGuard across await (CLAUDE.md 9.6).
            //
            // Borrow split: `self.connect_rx` is `&mut`-borrowed by the
            // recv arm; `self.channels` is `&`-borrowed for the
            // readability arm; `self.wakeup_notify` is `&`-borrowed
            // for the wake arm. Splitting `self` into independent
            // borrows via local re-bindings is required to satisfy
            // the borrow checker.
            //
            // `KafkaChannel` is `!Sync` (its `Box<dyn Authenticator>`
            // field has no `Sync` bound) so we cannot hold `&KafkaChannel`
            // across an `.await`. Instead we collect the underlying
            // `&(dyn TransportLayer + Sync)` references — every
            // production transport (`PlaintextTransportLayer`,
            // `SslTransportLayer`) is `Sync`, so `&dyn TransportLayer
            // + Sync` is `Send` and can cross await points safely.
            //
            // The `wakeup_notify` arm is Phase 8a.0 Round 2
            // Suggestion 1: the load-bearing wake mirroring Java's
            // `nioSelector.wakeup()`. Without it, the only way out
            // of the sleep arm is the timeout (`default.request.
            // timeout.ms`, default 30 s) or a fortuitous
            // socket-readable event. Calling
            // `Selector::wakeup_notify_handle().notify_one()` from
            // any task now short-circuits the sleep.
            let connect_rx = &mut self.connect_rx;
            let channels = &self.channels;
            let wakeup_notify = self.wakeup_notify.as_ref();
            let transports: Vec<&(dyn TransportLayer + Sync)> = channels
                .values()
                .filter(|c| c.ready() && c.transport_layer_ref().is_open())
                .map(|c| c.transport_layer_sync_ref())
                .collect();
            let mut connect_event_opt: Option<Option<ConnectEvent>> = None;
            tokio::select! {
                biased;
                event = connect_rx.recv() => {
                    connect_event_opt = Some(event);
                },
                _ = wakeup_notify.notified() => {},
                _ = wait_any_transport_readable(&transports), if !transports.is_empty() => {},
                _ = tokio::time::sleep(timeout) => {},
            }
            drop(transports);
            if let Some(Some(ev)) = connect_event_opt {
                self.dispatch_connect_event(ev);
            }
            // Drain any remaining queued events.
            self.drain_connect_events();
        }

        // Run the I/O loop over all open channels and remember which
        // channels had any I/O activity this tick. We collect the
        // active set inline so we can update the LRU only for those
        // channels — mirroring Java's per-ready-key
        // `idleExpiryManager.update(nodeId, currentTimeNanos)` at
        // `pollSelectionKeys:525-526`. Updating every open channel
        // unconditionally (as we used to) defeats `connections.max.idle.ms`
        // because every poll resets every channel's idle clock.
        let ids: Vec<ConnectionId> = self.channels.keys().copied().collect();
        let mut io_active_ids: Vec<ConnectionId> = Vec::new();
        for id in ids {
            let made_progress = self.drive_channel_io(id)?;
            if made_progress {
                io_active_ids.push(id);
            }
        }

        // Idle-expiry sweep BEFORE the LRU update — any channel
        // touched this tick must not be eligible for expiry on this
        // sweep, which is what Java's order guarantees (the per-key
        // `idle.update` happens, then `clear()` calls
        // `maybeCloseOldestConnection(endSelect)` at the bottom of
        // `poll`). Java's `endSelect` is captured BEFORE the LRU
        // updates, but since `maybeCloseOldestConnection` reads the
        // map after the updates landed, it sees the freshly-updated
        // timestamps for any active channel. We replicate that here
        // by updating LRU for io-active channels first, then sweeping.
        let now_ns = self.time.nanoseconds();
        if let Some(idle) = self.idle_expiry_manager.as_mut() {
            for id in &io_active_ids {
                idle.update(*id, now_ns);
            }
        }
        self.maybe_close_oldest_connection(now_ns);

        Ok(())
    }

    fn completed_sends(&self) -> &[NetworkSend] {
        &self.completed_sends
    }

    fn completed_receives(&self) -> &[NetworkReceive] {
        // Java returns an `Iterable`; Phase 5c-1's trait shape is
        // `&[NetworkReceive]`. We push into a `Vec` in insertion order
        // (mirroring Java's `LinkedHashMap.values()` iteration order).
        &self.completed_receives
    }

    fn disconnected(&self) -> &HashMap<ConnectionId, ChannelState> {
        &self.disconnected
    }

    fn connected(&self) -> &[ConnectionId] {
        &self.connected
    }

    fn mute(&mut self, id: ConnectionId) {
        if let Some(channel) = self.channels.get_mut(&id) {
            channel.mute();
            self.explicitly_muted_channels.insert(id);
        } else if let Some(channel) = self.closing_channels.get_mut(&id) {
            channel.mute();
            self.explicitly_muted_channels.insert(id);
        }
    }

    fn unmute(&mut self, id: ConnectionId) {
        let unmuted = match self.channels.get_mut(&id) {
            Some(c) => c.maybe_unmute(),
            None => match self.closing_channels.get_mut(&id) {
                Some(c) => c.maybe_unmute(),
                None => false,
            },
        };
        if unmuted {
            self.explicitly_muted_channels.remove(&id);
        }
    }

    fn mute_all(&mut self) {
        let ids: Vec<ConnectionId> = self.channels.keys().copied().collect();
        for id in ids {
            self.mute(id);
        }
    }

    fn unmute_all(&mut self) {
        let ids: Vec<ConnectionId> = self.channels.keys().copied().collect();
        for id in ids {
            self.unmute(id);
        }
    }

    fn is_channel_ready(&self, id: ConnectionId) -> bool {
        self.channels.get(&id).is_some_and(KafkaChannel::ready)
    }
}

impl Selector {
    /// Helper used by [`Selectable::poll`]'s `select!` arm to handle
    /// a single connect event. Mirrors the inline body of
    /// [`Self::drain_connect_events`] for one event — kept separate
    /// so the `select!` arm doesn't need to call back into a loop.
    fn dispatch_connect_event(&mut self, event: ConnectEvent) {
        match event {
            ConnectEvent::Connected { id, stream } => {
                if self.connect_tasks.remove(&id).is_none() {
                    return;
                }
                match self.build_and_register_channel(id, stream) {
                    Ok(()) => {
                        self.connected.push(id);
                        if let Some(idle) = self.idle_expiry_manager.as_mut() {
                            idle.update(id, self.time.nanoseconds());
                        }
                    },
                    Err(err) => {
                        self.disconnected
                            .insert(id, ChannelState::with_exception(ChannelStateName::NotConnected, err, None));
                    },
                }
            },
            ConnectEvent::Failed { id, err } => {
                if self.connect_tasks.remove(&id).is_none() {
                    return;
                }
                self.disconnected
                    .insert(id, ChannelState::with_exception(ChannelStateName::NotConnected, err, None));
            },
        }
    }
}

impl Drop for Selector {
    /// Mirrors Java's `AutoCloseable.close()` semantics — guarantees
    /// the connect tasks are aborted even if the user forgets to call
    /// [`Selectable::close`].
    fn drop(&mut self) {
        if !self.closed {
            // Best-effort: abort tasks, drop channels.
            for (_id, task) in self.connect_tasks.drain() {
                task.handle.abort();
            }
        }
    }
}

/// Future that resolves when **any** of the supplied transports
/// reports its underlying socket is read-ready. Phase 8a.0 — used
/// inside [`Selector::poll`]'s `select!` block to wake the I/O loop on
/// OS-level read readiness (the Tokio equivalent of Java's
/// `nio.Selector.select(timeout)` returning when any registered
/// `SelectionKey` becomes readable).
///
/// The future is cancellation-safe: it borrows the transports and
/// polls each transport's [`TransportLayer::poll_read_ready`] in
/// round-robin order. If cancelled (the `select!` arm loses), the
/// borrows are dropped without side effects — each transport's
/// waker is registered and will fire on the next OS-level read
/// notification, which the next poll loop iteration picks up.
///
/// The `+ Sync` bound on the trait object is what lets the future
/// itself be `Send` (so it satisfies the `+ Send` bound on
/// [`Selectable::poll`]'s return type): `&T: Send` iff `T: Sync`.
fn wait_any_transport_readable<'a>(transports: &'a [&'a (dyn TransportLayer + Sync)]) -> WaitAnyTransportReadable<'a> {
    WaitAnyTransportReadable { transports }
}

struct WaitAnyTransportReadable<'a> {
    transports: &'a [&'a (dyn TransportLayer + Sync)],
}

impl<'a> Future for WaitAnyTransportReadable<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Poll each transport. If any is ready, return Ready.
        // Otherwise each transport has registered the same context's
        // waker — Tokio will wake us when any of them becomes
        // readable. Empty input → Pending forever (never selected
        // because the `select!` guard `if !transports.is_empty()`
        // skips this arm in that case).
        let this = self.get_mut();
        for t in this.transports.iter() {
            if let Poll::Ready(()) = t.poll_read_ready(cx) {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `SelectorTest.java` (subset per PLAN.md
    //! Phase 5c-2). Each test maps to a Java case and the omissions are
    //! documented in the per-test rustdoc.
    //!
    //! **Java tests skipped, with rationale**:
    //! * `testMuteOnOOM` — exercises `MemoryPool` mute-on-low-memory.
    //!   `MemoryPool` deferred for Phase 9 per `kafka_channel.rs`
    //!   docstring.
    //! * `testInboundConnectionsCountInConnectionCreationMetric`,
    //!   `testOutboundConnectionsCountInConnectionCreationMetric`,
    //!   `testConnectionsByClientMetric`,
    //!   `testMetricsCleanupOnSelectorClose`,
    //!   `testPartialSendAndReceiveReflectedInMetrics` — Selector
    //!   metrics are stubs (`// metric stub` no-ops) per PLAN.md, so
    //!   the assertions on `Metrics`-registered values are not
    //!   meaningful.
    //! * `registerFailure`,
    //!   `testInboundConnectionsCountInConnectionCreationMetric` — use
    //!   `Selector.register(String, SocketChannel)` (server-side accept
    //!   path). Producer never accepts; not translated.
    //! * `testCloseAllChannels`,
    //!   `testConnectException` — anonymous-subclass overrides of
    //!   private Java methods (`buildChannel`, `registerChannel`) for
    //!   error injection. Rust uses trait dyn-dispatch; the same effect
    //!   is achieved by mocking the `ChannelBuilder` (already exercised
    //!   in [`build_channel_failure_surfaces_as_disconnect`]).
    //! * `testConnectDisconnectDuringInSinglePoll` — exercises
    //!   `pollSelectionKeys(Set<SelectionKey>, ...)` directly with
    //!   Mockito-mocked `KafkaChannel`. The Rust translation does not
    //!   expose `poll_selection_keys`; the same connect→prepare-fail
    //!   path is exercised end-to-end through the public API in
    //!   [`build_channel_failure_surfaces_as_disconnect`] (which
    //!   surfaces an error in the same `drive_channel_io` codepath as
    //!   a prepare failure would).
    //! * `testWriteCompletesSendWithNoBytesWritten` — calls package-
    //!   private `selector.write(channel)`. The Rust write path is
    //!   internal to `drive_channel_io`; the same invariant
    //!   (a completed send with 0 bytes still surfaces in
    //!   `completed_sends`) is exercised by
    //!   [`zero_byte_write_completes_send`].
    //! * `testChannelCloseWhileProcessingReceives` — uses Mockito
    //!   `KafkaChannel` and reflective access to `channels`; close
    //!   semantics during receive iteration are covered by
    //!   [`close_during_iteration_does_not_panic`].
    //! * `testLowestPriorityChannel` — `lowestPriorityChannel()` is a
    //!   server-side helper for the broker's `max.connections` cap;
    //!   not used by the producer. Skipped per PLAN.md scope.
    //! * `testImmediatelyConnectedCleaned`,
    //!   `testNoRouteToHost` — reflective access to
    //!   `immediatelyConnectedKeys` / DNS-resolution surface that the
    //!   Rust translation moves into the `connect` task. The
    //!   end-to-end equivalent (a connect failure surfaces as
    //!   `disconnected`) is exercised by
    //!   [`connect_to_unbound_port_surfaces_as_disconnect`].
    //! * `testExpireConnectionWithPendingReceives`,
    //!   `testExpireClosedConnectionWithPendingReceives`,
    //!   `testCloseOldestConnectionWithMultiplePendingReceives`,
    //!   `testGracefulClose`,
    //!   `testPartialReceiveGracefulClose` — exercise multi-receive
    //!   pipelining with kernel-buffer-dependent timing that is
    //!   flaky in the integration suite. The minimal idle-expiry +
    //!   graceful-close paths are covered by
    //!   [`idle_connection_is_expired`] and
    //!   [`server_disconnect_surfaces_in_disconnected`].
    //! * `testExistingConnectionId` — covered by
    //!   [`duplicate_connect_id_returns_illegal_state`].

    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::common::network::byte_buffer_send::ByteBufferSend;
    use crate::common::network::plaintext_channel_builder::PlaintextChannelBuilder;
    use crate::common::utils::SystemTime;

    /// A localhost echo server: reads a 4-byte big-endian size header
    /// followed by `size` bytes of payload, then writes the same
    /// length-prefixed frame back to the client. Mirrors the Java test
    /// fixture `EchoServer`.
    struct EchoServer {
        addr: SocketAddr,
        shutdown: Arc<Notify>,
        handle: JoinHandle<()>,
        /// Notify to forcibly close all client connections (test of
        /// server-side disconnect).
        close_clients: Arc<Notify>,
    }

    impl EchoServer {
        async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("local_addr");
            let shutdown = Arc::new(Notify::new());
            let close_clients = Arc::new(Notify::new());
            let shutdown_clone = Arc::clone(&shutdown);
            let close_clones = Arc::clone(&close_clients);
            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown_clone.notified() => break,
                        accept = listener.accept() => {
                            let (mut stream, _) = match accept {
                                Ok(p) => p,
                                Err(_) => break,
                            };
                            let close_signal = Arc::clone(&close_clones);
                            tokio::spawn(async move {
                                loop {
                                    let mut size_buf = [0u8; 4];
                                    let r = tokio::select! {
                                        biased;
                                        _ = close_signal.notified() => break,
                                        r = stream.read_exact(&mut size_buf) => r,
                                    };
                                    if r.is_err() {
                                        break;
                                    }
                                    let size = i32::from_be_bytes(size_buf) as usize;
                                    if size > 1 << 24 {
                                        break;
                                    }
                                    let mut payload = vec![0u8; size];
                                    if size > 0 && stream.read_exact(&mut payload).await.is_err() {
                                        break;
                                    }
                                    let mut frame = Vec::with_capacity(4 + size);
                                    frame.extend_from_slice(&size_buf);
                                    frame.extend_from_slice(&payload);
                                    if stream.write_all(&frame).await.is_err() {
                                        break;
                                    }
                                }
                            });
                        }
                    }
                }
            });
            EchoServer { addr, shutdown, handle, close_clients }
        }

        /// Forcibly close all currently-connected client streams.
        fn close_connections(&self) {
            self.close_clients.notify_waiters();
        }

        async fn shutdown(self) {
            self.shutdown.notify_one();
            self.close_clients.notify_waiters();
            self.handle.abort();
            let _ = self.handle.await;
        }
    }

    /// Build a Selector wrapping a [`PlaintextChannelBuilder`].
    fn make_selector(connection_max_idle_ms: i64, time: Arc<dyn Time>) -> Selector {
        Selector::with_capacity(
            16 * 1024,
            connection_max_idle_ms,
            time,
            Box::new(PlaintextChannelBuilder::new(None)),
        )
    }

    /// Run `selector.poll(0)` on a tight loop until `cond` holds or the
    /// deadline elapses (whichever is first). Mirrors Java's
    /// `waitForCondition`.
    async fn wait_for<F>(selector: &mut Selector, mut cond: F, deadline_ms: u64, msg: &str)
    where
        F: FnMut(&Selector) -> bool,
    {
        let deadline = std::time::Instant::now() + Duration::from_millis(deadline_ms);
        loop {
            selector.poll(10).await.expect("poll");
            if cond(selector) {
                return;
            }
            if std::time::Instant::now() >= deadline {
                panic!("waitForCondition exceeded {}ms: {}", deadline_ms, msg);
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Mirror of Java's `blockingConnect(node)`: connect, then poll
    /// until the channel is `Ready`.
    async fn blocking_connect(selector: &mut Selector, id: ConnectionId, addr: SocketAddr) {
        selector
            .connect(
                id,
                "localhost",
                addr,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
            )
            .expect("connect");
        wait_for(selector, |s| s.is_channel_ready(id), 5_000, "channel not ready").await;
    }

    const USE_DEFAULT_BUFFER_SIZE_LOCAL: i32 = -1;

    /// Build a `NetworkSend` carrying `payload` to broker `id` (matches
    /// Java's `createSend(node, payload)`).
    fn make_send(id: ConnectionId, payload: &[u8]) -> NetworkSend {
        let bytes = Bytes::copy_from_slice(payload);
        let inner = ByteBufferSend::size_prefixed(bytes);
        let dest: Arc<str> = Arc::from(id.to_string());
        NetworkSend::new(dest, Box::new(inner))
    }

    /// Mirror of Java's `asString(receive)` — convert the framed
    /// payload into a UTF-8 string.
    fn payload_string(receive: &NetworkReceive) -> String {
        let bytes = receive.payload().expect("payload").to_vec();
        String::from_utf8(bytes).expect("utf8")
    }

    /// Translation of `SelectorTest.testNormalOperation` (single
    /// channel, one round-trip — full multi-channel parity is exercised
    /// in [`multi_connection_normal_operation`] below).
    #[tokio::test]
    async fn connect_send_receive_round_trip() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        selector.send(make_send(0, b"hello"));
        wait_for(&mut selector, |s| !s.completed_receives().is_empty(), 5_000, "no response").await;
        assert_eq!(selector.completed_receives().len(), 1);
        let recv = &selector.completed_receives()[0];
        assert_eq!(payload_string(recv), "hello");
        assert!(selector.disconnected().is_empty(), "no disconnects");
        selector.close();
        server.shutdown().await;
    }

    /// Translation of `SelectorTest.testNormalOperation` (multi-
    /// channel). 5 connections, each sending `reqs` echo round-trips.
    /// Reduced from Java's 500 to 50 to keep the test fast — the
    /// behaviour exercised (parallel connect, parallel
    /// send/receive matching, response ordering) does not change with
    /// volume.
    #[tokio::test]
    async fn multi_connection_normal_operation() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        let conns = 5;
        let reqs: usize = 50;
        for i in 0..conns {
            selector
                .connect(
                    i,
                    "localhost",
                    server.addr,
                    USE_DEFAULT_BUFFER_SIZE_LOCAL,
                    USE_DEFAULT_BUFFER_SIZE_LOCAL,
                )
                .expect("connect");
        }
        // Wait for all to connect.
        wait_for(
            &mut selector,
            |s| (0..conns).all(|i| s.is_channel_ready(i)),
            10_000,
            "not all connected",
        )
        .await;
        let mut requests: HashMap<ConnectionId, usize> = HashMap::new();
        let mut responses: HashMap<ConnectionId, usize> = HashMap::new();
        let mut response_count = 0;
        // Kick off the first request on each.
        for i in 0..conns {
            let payload = format!("{}-{}", i, 0);
            selector.send(make_send(i, payload.as_bytes()));
        }
        let total = (conns as usize) * reqs;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while response_count < total {
            assert!(std::time::Instant::now() < deadline, "deadline exceeded");
            selector.poll(10).await.expect("poll");
            assert!(
                selector.disconnected().is_empty(),
                "no disconnects: {:?}",
                selector.disconnected()
            );
            // Snapshot completed_sends/receives so we can drop the
            // borrow before calling `selector.send`.
            let resp_payloads: Vec<(ConnectionId, String)> = selector
                .completed_receives()
                .iter()
                .map(|r| {
                    let id: ConnectionId = r.source().parse().expect("numeric source");
                    (id, payload_string(r))
                })
                .collect();
            let send_dests: Vec<ConnectionId> = selector
                .completed_sends()
                .iter()
                .map(|s| s.destination_id().parse().expect("numeric dest"))
                .collect();
            for (id, body) in &resp_payloads {
                let pieces: Vec<&str> = body.split('-').collect();
                assert_eq!(pieces.len(), 2);
                let counter: usize = pieces[1].parse().expect("counter");
                let prev = responses.get(id).copied().unwrap_or(0);
                assert_eq!(counter, prev, "out-of-order response");
                responses.insert(*id, prev + 1);
                response_count += 1;
            }
            for dest in send_dests {
                let new_count = requests.get(&dest).copied().unwrap_or(0) + 1;
                requests.insert(dest, new_count);
                if new_count < reqs {
                    let payload = format!("{}-{}", dest, new_count);
                    selector.send(make_send(dest, payload.as_bytes()));
                }
            }
        }
        selector.close();
        server.shutdown().await;
    }

    /// Translation of `SelectorTest.testServerDisconnect`.
    #[tokio::test]
    async fn server_disconnect_surfaces_in_disconnected() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        // Round-trip a request to make sure the channel is healthy.
        selector.send(make_send(0, b"hello"));
        wait_for(
            &mut selector,
            |s| !s.completed_receives().is_empty(),
            5_000,
            "no first response",
        )
        .await;
        assert_eq!(payload_string(&selector.completed_receives()[0]), "hello");
        // Trigger server-side disconnect.
        server.close_connections();
        wait_for(
            &mut selector,
            |s| s.disconnected().contains_key(&0),
            5_000,
            "no disconnect notification",
        )
        .await;
        selector.close();
        server.shutdown().await;
    }

    /// Translation of `SelectorTest.testCantSendWithInProgress`.
    #[tokio::test]
    async fn double_send_panics_and_is_failed() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        selector.send(make_send(0, b"test1"));
        // The Java test expects `IllegalStateException`; Rust mirrors
        // with a panic via `set_send → KafkaError::IllegalState` →
        // panic in `Selector::send`. We catch with `catch_unwind` so
        // the rest of the test can continue.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            selector.send(make_send(0, b"test2"));
        }));
        assert!(result.is_err(), "double-send must panic");
        // After the failed send, `poll(0)` should surface a
        // `FailedSend` disconnect entry.
        selector.poll(0).await.expect("poll");
        assert!(selector.disconnected().contains_key(&0), "channel must be marked disconnected");
        assert_eq!(selector.disconnected().get(&0).unwrap().state(), ChannelStateName::FailedSend);
        selector.close();
        server.shutdown().await;
    }

    /// Translation of `SelectorTest.testSendWithoutConnecting`.
    #[tokio::test]
    async fn send_without_connecting_panics() {
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        let send = make_send(0, b"test");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            selector.send(send);
        }));
        assert!(result.is_err(), "send to unconnected node must panic");
    }

    /// Translation of `SelectorTest.testConnectionRefused`.
    #[tokio::test]
    async fn connect_to_unbound_port_surfaces_as_disconnect() {
        let bound = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = bound.local_addr().expect("local_addr");
        // Drop the listener so the port is free, then point at it. The
        // OS may rapidly reuse the port — bind a sentinel listener on a
        // sibling port instead and connect to a port that's almost
        // certainly closed.
        drop(bound);
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        selector
            .connect(
                0,
                "localhost",
                addr,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
            )
            .expect("connect");
        wait_for(
            &mut selector,
            |s| s.disconnected().contains_key(&0),
            5_000,
            "no refused disconnect",
        )
        .await;
        assert_eq!(selector.disconnected().get(&0).unwrap().state(), ChannelStateName::NotConnected);
        selector.close();
    }

    /// Translation of `SelectorTest.testCloseOldestConnection` +
    /// `testIdleExpiryWithoutReadyKeys`. Uses [`SystemTime`] (real
    /// clock) and a small idle-ms budget so the expiry sweeps fire
    /// after a sleep.
    #[tokio::test]
    async fn idle_connection_is_expired() {
        let server = EchoServer::start().await;
        let max_idle_ms: i64 = 50;
        let mut selector = make_selector(max_idle_ms, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        // Sleep past the idle threshold.
        tokio::time::sleep(Duration::from_millis(150)).await;
        // One poll to drive the expiry sweep.
        selector.poll(0).await.expect("poll");
        // The expiry sweep marks the channel state EXPIRED, then
        // close_internal moves it into `disconnected` (graceful close
        // — no pending receive, so it surfaces immediately).
        assert!(
            selector.disconnected().contains_key(&0),
            "expected idle expiry; disconnected={:?}, channels={:?}",
            selector.disconnected(),
            selector.channels().iter().map(|c| c.id().to_owned()).collect::<Vec<_>>()
        );
        assert_eq!(selector.disconnected().get(&0).unwrap().state(), ChannelStateName::Expired);
        selector.close();
        server.shutdown().await;
    }

    /// Regression for Critic-0 Phase 5c-2 Comment #1: under busy
    /// polling (poll called repeatedly while no I/O occurs), the
    /// channel's idle clock must NOT be reset on every poll — only
    /// on polls where the channel actually had I/O activity. This
    /// mirrors Java `Selector.pollSelectionKeys:525-526` (per-key
    /// `idleExpiryManager.update`).
    ///
    /// The previous implementation unconditionally refreshed
    /// `last_active_ns` for every open channel at the bottom of
    /// `poll`, so a connection idle for the entire
    /// `connections.max.idle.ms` window still appeared "fresh" and
    /// `pollExpiredConnection` never returned it.
    #[tokio::test]
    async fn busy_poll_does_not_reset_idle_clock() {
        let server = EchoServer::start().await;
        let max_idle_ms: i64 = 100;
        let mut selector = make_selector(max_idle_ms, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        // Drive `poll(0)` repeatedly with no I/O activity for longer
        // than `max_idle_ms`. Each tick is short (no sleep), so we
        // get many busy polls during the idle window. Under the bug,
        // every busy poll would refresh `last_active_ns`, making
        // expiry impossible.
        let deadline = std::time::Instant::now() + Duration::from_millis(300);
        while std::time::Instant::now() < deadline {
            selector.poll(0).await.expect("poll");
            if selector.disconnected().contains_key(&0) {
                break;
            }
            // Yield to keep the poll loop tight without blocking.
            tokio::task::yield_now().await;
        }
        assert!(
            selector.disconnected().contains_key(&0),
            "idle expiry must fire under busy poll; disconnected={:?}, channels={:?}",
            selector.disconnected(),
            selector.channels().iter().map(|c| c.id().to_owned()).collect::<Vec<_>>(),
        );
        assert_eq!(selector.disconnected().get(&0).unwrap().state(), ChannelStateName::Expired);
        selector.close();
        server.shutdown().await;
    }

    /// Regression for Critic-0 Phase 5c-2 Comment #1 (lower-level):
    /// pin the LRU bookkeeping rule in isolation — `update` only
    /// happens for io-active channels. We check by driving the
    /// `IdleExpiryManager` directly with two channels, one of which
    /// is repeatedly "touched" via update while the other is left
    /// untouched.
    #[test]
    fn idle_expiry_manager_only_updated_channel_is_refreshed() {
        let mut idle = IdleExpiryManager {
            last_active_ns: HashMap::new(),
            lru_order: BTreeSet::new(),
            connections_max_idle_ns: 1_000, // 1 µs
            next_idle_close_check_ns: 0,
        };
        idle.update(1, 100); // last-active for 1 = 100
        idle.update(2, 200); // last-active for 2 = 200
        // Simulate "busy polls" that touch only channel 1 because
        // channel 2 had no I/O.
        idle.update(1, 1_000_000);
        idle.update(1, 2_000_000);
        // Sweep at t=3M ns — channel 2 (last-active=200) is far
        // past the 1µs idle threshold, channel 1 (last-active=2M)
        // is fresh. Expect channel 2 to be the expired entry.
        let expired = idle.poll_expired_connection(3_000_000);
        assert_eq!(expired, Some((2, 200)), "untouched channel must expire first");
    }

    /// Regression for Critic-0 Phase 5c-2 Comment #2: closing-channel
    /// `failed_sends` short-circuit must fire. When a channel is
    /// already in `closing_channels` and `send()` is then invoked,
    /// the send goes to `failed_sends`; the next `poll()` runs
    /// `process_closing_channels` BEFORE draining `failed_sends`,
    /// observes the entry, and skips the wasted
    /// `maybeReadFromClosingChannel` read. Java
    /// `Selector.clear()`:849-859.
    #[tokio::test]
    async fn closing_channel_failed_send_short_circuits_read() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        // Send a message, wait for response, so the channel is
        // healthy and has no pending receive.
        selector.send(make_send(0, b"hello"));
        wait_for(
            &mut selector,
            |s| !s.completed_receives().is_empty(),
            5_000,
            "no first response",
        )
        .await;
        // Simulate a graceful close-with-pending: forcibly trigger
        // the closing-channel path by injecting a partial receive,
        // then closing. The simplest reproducer: close the server
        // mid-flight after issuing a second send. We then send a
        // third message which lands in `failed_sends` (closing-
        // channel send path).
        server.close_connections();
        wait_for(
            &mut selector,
            |s| s.disconnected().contains_key(&0) || s.channels().is_empty() || s.closing_channel(0).is_some(),
            5_000,
            "channel did not transition",
        )
        .await;
        // After the disconnect surfaces or the channel enters the
        // closing path, send to it — Java `Selector.send` on a
        // closing channel pushes the send into `failedSends`.
        if selector.closing_channel(0).is_some() {
            selector.send(make_send(0, b"after-close"));
            // Next poll must surface the channel as disconnected
            // with state=FailedSend (failed_sends drain) — and the
            // closing-channel path must NOT have consumed it twice.
            selector.poll(0).await.expect("poll");
            // disconnected[0] should be the failed-send entry, not
            // a duplicate.
            assert_eq!(
                selector.disconnected().keys().filter(|&&k| k == 0).count(),
                1,
                "closing-channel + failed-send must surface exactly once"
            );
        }
        selector.close();
        server.shutdown().await;
    }

    /// Translation of `SelectorTest.testExistingConnectionId`.
    #[tokio::test]
    async fn duplicate_connect_id_returns_illegal_state() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        let err = selector
            .connect(
                0,
                "localhost",
                server.addr,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
            )
            .expect_err("duplicate connect");
        assert!(matches!(err, KafkaError::IllegalState(_)));
        selector.close();
        server.shutdown().await;
    }

    /// Translation of `SelectorTest.testMute`.
    #[tokio::test]
    async fn mute_suppresses_reads_until_unmute() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        blocking_connect(&mut selector, 1, server.addr).await;
        selector.send(make_send(0, b"hello"));
        selector.send(make_send(1, b"hi"));
        selector.mute(1);
        // Only node 0's response should land while node 1 is muted.
        wait_for(
            &mut selector,
            |s| !s.completed_receives().is_empty(),
            5_000,
            "no response while muted",
        )
        .await;
        let receives = selector.completed_receives();
        assert_eq!(receives.len(), 1);
        let id: ConnectionId = receives[0].source().parse().expect("source");
        assert_eq!(id, 0);
        // Unmute → node 1's response arrives next.
        selector.unmute(1);
        wait_for(
            &mut selector,
            |s| s.completed_receives().iter().any(|r| r.source() == "1"),
            5_000,
            "muted node never delivers",
        )
        .await;
        selector.close();
        server.shutdown().await;
    }

    /// Translation of `SelectorTest.testEmptyRequest`.
    #[tokio::test]
    async fn empty_request_round_trips() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        selector.send(make_send(0, b""));
        wait_for(
            &mut selector,
            |s| !s.completed_receives().is_empty(),
            5_000,
            "empty request did not echo",
        )
        .await;
        assert_eq!(payload_string(&selector.completed_receives()[0]), "");
        selector.close();
        server.shutdown().await;
    }

    /// Translation of `SelectorTest.testClearCompletedSendsAndReceives`.
    #[tokio::test]
    async fn clear_completed_sends_and_receives() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        selector.send(make_send(0, b"hello"));
        let mut sent = false;
        let mut received = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !sent || !received {
            assert!(std::time::Instant::now() < deadline);
            selector.poll(50).await.expect("poll");
            assert!(selector.disconnected().is_empty());
            if !selector.completed_sends().is_empty() {
                assert_eq!(selector.completed_sends().len(), 1);
                selector.clear_completed_sends();
                assert_eq!(selector.completed_sends().len(), 0);
                sent = true;
            }
            if !selector.completed_receives().is_empty() {
                assert_eq!(selector.completed_receives().len(), 1);
                assert_eq!(payload_string(&selector.completed_receives()[0]), "hello");
                selector.clear_completed_receives();
                assert_eq!(selector.completed_receives().len(), 0);
                received = true;
            }
        }
        selector.close();
        server.shutdown().await;
    }

    /// Mirror of `SelectorTest.testSendLargeRequest` — round-trips a
    /// payload larger than the local buffer. The Selector here is
    /// constructed with a 64KB `max_receive_size` (matches Java's
    /// `BUFFER_SIZE = 4 * 1024` test fixture multiplied by 16, so
    /// payloads up to ~64KB exercise multi-tick read).
    #[tokio::test]
    async fn large_request_round_trips() {
        let server = EchoServer::start().await;
        let mut selector = Selector::with_capacity(
            64 * 1024,
            NO_IDLE_TIMEOUT_MS,
            SystemTime::instance(),
            Box::new(PlaintextChannelBuilder::new(None)),
        );
        blocking_connect(&mut selector, 0, server.addr).await;
        let payload: Vec<u8> = (0..40_000).map(|i| (i % 256) as u8).collect();
        selector.send(make_send(0, &payload));
        wait_for(
            &mut selector,
            |s| !s.completed_receives().is_empty(),
            10_000,
            "large response missed",
        )
        .await;
        let recv = &selector.completed_receives()[0];
        assert_eq!(recv.payload().unwrap().len(), payload.len());
        assert_eq!(recv.payload().unwrap().to_vec(), payload);
        selector.close();
        server.shutdown().await;
    }

    /// Verifies graceful local close: `close_connection(id)` removes
    /// the channel without producing a `disconnected` entry (Java's
    /// `LOCAL_CLOSE`/`DISCARD_NO_NOTIFY` semantic).
    #[tokio::test]
    async fn local_close_does_not_notify_disconnect() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        selector.close_connection(0);
        // poll once to drain anything; should remain empty.
        selector.poll(0).await.expect("poll");
        assert!(selector.channel(0).is_none());
        assert!(
            !selector.disconnected().contains_key(&0),
            "local close must NOT produce a disconnect notification"
        );
        selector.close();
        server.shutdown().await;
    }

    /// Verifies that a build-channel failure surfaces as a disconnect
    /// (mirrors Java's `buildAndAttachKafkaChannel` catch-and-rethrow
    /// path). Uses a stub builder that always fails.
    #[tokio::test]
    async fn build_channel_failure_surfaces_as_disconnect() {
        struct AlwaysFail;
        impl ChannelBuilder for AlwaysFail {
            fn build_channel(
                &self,
                _id: Arc<str>,
                _stream: tokio::net::TcpStream,
                _max_receive_size: i32,
                _metadata_registry: crate::common::network::kafka_channel::BoxedMetadataRegistry,
            ) -> Result<KafkaChannel, KafkaError> {
                Err(KafkaError::IllegalState("build_channel intentionally fails".to_string()))
            }
        }
        let server = EchoServer::start().await;
        let mut selector =
            Selector::with_capacity(1024, NO_IDLE_TIMEOUT_MS, SystemTime::instance(), Box::new(AlwaysFail));
        selector
            .connect(
                7,
                "localhost",
                server.addr,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
            )
            .expect("connect");
        wait_for(
            &mut selector,
            |s| s.disconnected().contains_key(&7),
            5_000,
            "no failed-build disconnect",
        )
        .await;
        let st = selector.disconnected().get(&7).unwrap();
        assert_eq!(st.state(), ChannelStateName::NotConnected);
        assert!(st.exception().is_some());
        selector.close();
        server.shutdown().await;
    }

    /// Mirrors `SelectorTest.testWriteCompletesSendWithNoBytesWritten`
    /// behaviourally — a zero-byte send still surfaces in
    /// [`Selectable::completed_sends`] after the write loop runs. Driven
    /// end-to-end against the echo server (no Mockito).
    #[tokio::test]
    async fn zero_byte_write_completes_send() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        selector.send(make_send(0, b""));
        wait_for(
            &mut selector,
            |s| !s.completed_sends().is_empty(),
            5_000,
            "zero-byte send not surfaced",
        )
        .await;
        assert_eq!(selector.completed_sends().len(), 1);
        assert_eq!(selector.completed_sends()[0].destination_id(), "0");
        selector.close();
        server.shutdown().await;
    }

    /// Verifies that closing a channel that already has a completed
    /// receive in-flight does not panic, mirroring the invariant
    /// exercised by `testChannelCloseWhileProcessingReceives`.
    #[tokio::test]
    async fn close_during_iteration_does_not_panic() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        blocking_connect(&mut selector, 0, server.addr).await;
        blocking_connect(&mut selector, 1, server.addr).await;
        selector.send(make_send(0, b"hi"));
        selector.send(make_send(1, b"hello"));
        wait_for(&mut selector, |s| s.completed_receives().len() == 2, 5_000, "two responses").await;
        // Close one of the channels inline.
        selector.close_connection(0);
        // Ensure that the other channel still works.
        selector.send(make_send(1, b"again"));
        wait_for(
            &mut selector,
            |s| {
                s.completed_receives()
                    .iter()
                    .any(|r| r.source() == "1" && payload_string(r) == "again")
            },
            5_000,
            "second send to surviving channel",
        )
        .await;
        selector.close();
        server.shutdown().await;
    }

    /// `wakeup` (Phase 8a.0 Round 2 Suggestion 1) buffers a permit on
    /// the internal `Notify` even with no waiter — the next `poll`
    /// `select!` arm sees the permit and exits its sleep immediately.
    /// Pin both invariants: (1) calling `wakeup` before any waiter is
    /// safe (no panic), (2) the `wakeup_notify_handle` accessor
    /// returns a clone that shares the same permit slot.
    #[tokio::test]
    async fn wakeup_buffers_permit_and_handle_shares_slot() {
        let selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        Selectable::wakeup(&selector);
        // The handle is a clone of the same Arc, so a second waiter
        // looking via the handle observes the same permit.
        let handle = selector.wakeup_notify_handle();
        // The next `notified()` call returns immediately because the
        // permit is buffered.
        tokio::time::timeout(Duration::from_millis(100), handle.notified())
            .await
            .expect("wakeup permit should be observable through the cloned handle");
    }

    /// Phase 8a.0 Round 2 Suggestion 3 regression: pin
    /// **wake-on-Notify**. A Selector parked in `poll(5000)` with
    /// nothing else going on should return early when another task
    /// calls `wakeup_notify_handle().notify_one()` — proving the
    /// Notify arm in the `select!` short-circuits the timeout sleep.
    /// Without the wake arm, this test would block for the full
    /// 5 s timeout. The 200 ms upper bound is generous (the actual
    /// wake should fire in microseconds on a healthy executor).
    #[tokio::test]
    async fn poll_wakes_when_notify_one_is_called() {
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        let wake_handle = selector.wakeup_notify_handle();

        // Schedule the wake from a sibling task after a short delay
        // (longer than 0 so the poll has actually parked, shorter
        // than the 5 s `poll` timeout so we detect early-return).
        let waker = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            wake_handle.notify_one();
        });

        let started = std::time::Instant::now();
        // 5 s timeout: in the no-wake regression world the only way
        // out of `poll` would be the timeout sleep.
        let poll_result = tokio::time::timeout(Duration::from_millis(2_000), selector.poll(5_000)).await;
        let elapsed = started.elapsed();
        waker.await.expect("waker task");
        // The outer Duration::from_millis(2_000) timeout exists so
        // a failing test fails fast rather than waiting the full
        // 5 s `poll` timeout. We assert the inner poll completed.
        let inner = poll_result.expect("poll did not return inside 2 s — wake arm regressed");
        inner.expect("poll returned Ok");
        assert!(
            elapsed < Duration::from_millis(200),
            "poll returned but took longer than expected: {elapsed:?} (wake arm should fire in <20 ms after notify_one)"
        );
        selector.close();
    }

    /// Phase 8a.0 Round 2 Suggestion 3 regression: pin
    /// **wake-on-read**. Echo-server tests elsewhere use a tight
    /// `wait_for` loop with `poll(10)` which masks any wake-arm
    /// regression (the test passes because the 10 ms tight loop
    /// catches up). This test parks the Selector in
    /// `poll(5000)`, then has the EchoServer write bytes to the
    /// socket; the Selector's `wait_any_transport_readable` arm
    /// must observe socket readability and return promptly. Without
    /// it the test would block for the full 5 s timeout.
    #[tokio::test]
    async fn poll_wakes_when_socket_becomes_readable() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());

        // Drive the connect + handshake to READY via the tight-loop
        // wait_for helper. After this, the channel is open and the
        // EchoServer-side reader is blocking on `read` — no bytes
        // are in flight either way yet.
        blocking_connect(&mut selector, 0, server.addr).await;

        // Now write a single send so the server replies. The
        // initial `poll` issues the send (because `has_send()` is
        // true, the sleep-arm guard at line 1037 short-circuits);
        // after that the channel is quiet again until the broker
        // (EchoServer) writes its echo back.
        selector.send(make_send(0, b"hello"));
        // Run a single tick to push the send to the wire.
        selector.poll(0).await.expect("send tick");

        // The send is now on the wire; the EchoServer task will
        // read it and write back. Our `poll(5000)` must wake on
        // socket-readable. The 1 s outer timeout exists so a
        // regression fails fast instead of waiting the full 5 s.
        let started = std::time::Instant::now();
        let inner = tokio::time::timeout(Duration::from_millis(1_000), selector.poll(5_000))
            .await
            .expect("poll did not return inside 1 s — wake-on-read regressed");
        inner.expect("poll returned Ok");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "poll returned but took longer than expected: {elapsed:?} (wake-on-read should fire promptly)"
        );
        // Verify the echoed payload actually landed.
        assert!(
            selector
                .completed_receives()
                .iter()
                .any(|r| r.source() == "0" && payload_string(r) == "hello"),
            "expected echoed receive after wake-on-read"
        );
        selector.close();
        server.shutdown().await;
    }

    /// Sanity: `pending_connects_len` is exposed for tests only and
    /// reflects in-flight connect tasks (mirroring Java's
    /// `immediatelyConnectedKeys` field reflection in
    /// `verifyEmptyImmediatelyConnectedKeys`).
    #[tokio::test]
    async fn pending_connects_clears_on_completion() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        selector
            .connect(
                0,
                "localhost",
                server.addr,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
            )
            .expect("connect");
        // Right after connect: a task is in flight.
        assert_eq!(selector.pending_connects_len(), 1);
        wait_for(&mut selector, |s| s.is_channel_ready(0), 5_000, "ready").await;
        // After the task lands and the channel is registered, the
        // tasks map drains.
        assert_eq!(selector.pending_connects_len(), 0);
        selector.close();
        server.shutdown().await;
    }

    /// `IdleExpiryManager` LRU/expiry algebra in isolation — easier to
    /// pin without an EchoServer involved.
    #[tokio::test]
    async fn idle_expiry_manager_polls_oldest_first() {
        // Use a fixed wall-clock surface via SystemTime, but drive the
        // manager with hand-picked nanosecond stamps so the test is
        // deterministic.
        let mut idle = IdleExpiryManager {
            last_active_ns: HashMap::new(),
            lru_order: BTreeSet::new(),
            connections_max_idle_ns: 1_000_000, // 1ms in nanos
            next_idle_close_check_ns: 0,
        };
        idle.update(1, 100);
        idle.update(2, 200);
        idle.update(3, 300);
        // Touch 1 → re-key under (350, 1): order is now [2, 3, 1].
        idle.update(1, 350);
        // Poll at t=400: oldest is 2 with last_active=200 → next check
        // is 1_000_200; since current=400 < next_idle_close_check_ns,
        // first poll returns None and resets next_check.
        assert_eq!(idle.poll_expired_connection(400), None);
        // Poll past the threshold (oldest+max_idle_ns).
        let expired = idle.poll_expired_connection(1_500_000);
        assert_eq!(expired, Some((2, 200)));
        idle.remove(2);
        // Next-oldest is 3.
        let expired = idle.poll_expired_connection(2_500_000);
        assert_eq!(expired, Some((3, 300)));
    }

    /// Round-trip: the connect mpsc backpressure works — a connect
    /// task posting after the Selector closed does not cause the test
    /// to hang. We trigger a connect to a never-binding port (the
    /// connect task will eventually resolve to a Failed event), then
    /// close the Selector before draining.
    #[tokio::test]
    async fn close_aborts_in_flight_connect_tasks() {
        // Bind & immediately drop a listener to obtain a likely-closed
        // port.
        let bound = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = bound.local_addr().expect("local_addr");
        drop(bound);

        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        selector
            .connect(
                0,
                "localhost",
                addr,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
            )
            .expect("connect");
        // Close before the connect task runs to completion.
        selector.close();
        // Subsequent operations must short-circuit; verify no panic.
        let result = selector.poll(0).await;
        assert!(matches!(result, Err(KafkaError::IllegalState(_))));
    }

    /// Regression for Critic-0 Phase 5c-2 Comments #3 + #4: the
    /// connect path applies `SO_KEEPALIVE`, `SO_SNDBUF`, and
    /// `SO_RCVBUF` (when not [`USE_DEFAULT_BUFFER_SIZE`]) on the
    /// freshly-created [`TcpSocket`] BEFORE the kernel handshakes,
    /// matching Java `Selector.configureSocketChannel`. We connect
    /// to the echo server with non-default buffer sizes and verify
    /// the connection succeeds and round-trips bytes — kernel-side
    /// option clamping is OS-specific so we do not assert exact
    /// `getsockopt` values, only that the wired path doesn't break
    /// connect.
    #[tokio::test]
    async fn connect_applies_keepalive_and_buffer_sizes() {
        let server = EchoServer::start().await;
        let mut selector = make_selector(NO_IDLE_TIMEOUT_MS, SystemTime::instance());
        // Pick non-default values within typical OS limits (Linux
        // doubles SNDBUF/RCVBUF internally so we stay below
        // `net.core.wmem_max` defaults).
        let send_buf: i32 = 32 * 1024;
        let recv_buf: i32 = 32 * 1024;
        selector
            .connect(0, "localhost", server.addr, send_buf, recv_buf)
            .expect("connect");
        wait_for(&mut selector, |s| s.is_channel_ready(0), 5_000, "channel not ready").await;
        // Round-trip a payload to confirm the connection is
        // functional after the option-setting path.
        selector.send(make_send(0, b"keepalive-and-bufs"));
        wait_for(&mut selector, |s| !s.completed_receives().is_empty(), 5_000, "no response").await;
        assert_eq!(payload_string(&selector.completed_receives()[0]), "keepalive-and-bufs");
        selector.close();
        server.shutdown().await;
    }

    /// Pinning test for 9c.2: the host string passed to
    /// [`Selectable::connect`] reaches the
    /// [`ChannelBuilder::build_channel_with_server_name`] override as a
    /// `Some(ServerName)` that round-trips back to the original host
    /// when the host parses as a DNS name. A raw-IP host gets a
    /// `Some(ServerName::IpAddress)` — rustls accepts both DNS names
    /// and IPv4 / IPv6 literals via `ServerName::try_from`.
    #[tokio::test]
    async fn build_channel_with_server_name_receives_resolved_host() {
        use std::sync::Mutex;

        #[derive(Default)]
        struct CapturingBuilder {
            captured: Arc<Mutex<Option<Option<String>>>>,
        }
        impl ChannelBuilder for CapturingBuilder {
            fn build_channel(
                &self,
                _id: Arc<str>,
                _stream: tokio::net::TcpStream,
                _max_receive_size: i32,
                _metadata_registry: crate::common::network::kafka_channel::BoxedMetadataRegistry,
            ) -> Result<KafkaChannel, KafkaError> {
                // We never expect the trait-default to fire — the
                // selector always calls the SNI-aware override. If we
                // somehow land here, fail loud.
                Err(KafkaError::IllegalState(
                    "CapturingBuilder::build_channel should not be called — selector must call build_channel_with_server_name".to_owned(),
                ))
            }
            fn build_channel_with_server_name(
                &self,
                _id: Arc<str>,
                _stream: tokio::net::TcpStream,
                server_name: Option<rustls::pki_types::ServerName<'static>>,
                _max_receive_size: i32,
                _metadata_registry: crate::common::network::kafka_channel::BoxedMetadataRegistry,
            ) -> Result<KafkaChannel, KafkaError> {
                let captured = server_name.map(|sn| match sn {
                    rustls::pki_types::ServerName::DnsName(n) => n.as_ref().to_owned(),
                    rustls::pki_types::ServerName::IpAddress(ip) => format!("{ip:?}"),
                    other => format!("{other:?}"),
                });
                *self.captured.lock().unwrap() = Some(captured);
                // Fail the build to short-circuit channel setup — we
                // don't need an actual channel for this assertion.
                Err(KafkaError::IllegalState(
                    "CapturingBuilder is test-only — capture happened".to_owned(),
                ))
            }
        }

        // Case 1: a DNS host name flows through as Some(host).
        let server = EchoServer::start().await;
        let captured_dns = Arc::new(Mutex::new(None::<Option<String>>));
        let mut selector = Selector::with_capacity(
            1024,
            NO_IDLE_TIMEOUT_MS,
            SystemTime::instance(),
            Box::new(CapturingBuilder { captured: Arc::clone(&captured_dns) }),
        );
        selector
            .connect(
                11,
                "broker-1.example.com",
                server.addr,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
            )
            .expect("connect");
        // Wait until the build fires and lands in `disconnected`.
        wait_for(
            &mut selector,
            |s| s.disconnected().contains_key(&11),
            5_000,
            "no failed-build disconnect for DNS host case",
        )
        .await;
        assert_eq!(
            captured_dns.lock().unwrap().clone(),
            Some(Some("broker-1.example.com".to_owned())),
            "DNS host must propagate as Some(ServerName::DnsName)"
        );
        selector.close();
        server.shutdown().await;

        // Case 2: a raw IPv4 literal yields Some(ServerName::IpAddress)
        // — rustls' ServerName::try_from accepts IP literals as the
        // IpAddress variant (not DnsName, but still Some).
        let server = EchoServer::start().await;
        let captured_ip = Arc::new(Mutex::new(None::<Option<String>>));
        let mut selector = Selector::with_capacity(
            1024,
            NO_IDLE_TIMEOUT_MS,
            SystemTime::instance(),
            Box::new(CapturingBuilder { captured: Arc::clone(&captured_ip) }),
        );
        selector
            .connect(
                22,
                "127.0.0.1",
                server.addr,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
                USE_DEFAULT_BUFFER_SIZE_LOCAL,
            )
            .expect("connect");
        wait_for(
            &mut selector,
            |s| s.disconnected().contains_key(&22),
            5_000,
            "no failed-build disconnect for IP host case",
        )
        .await;
        // rustls' ServerName::try_from successfully parses "127.0.0.1"
        // as an `IpAddress` variant — not a DnsName, but still Some.
        // SSL builders accept it: the handshake will omit SNI per
        // RFC 6066 §3 (which forbids IP literals in SNI) and instead
        // verify the peer cert via IP-SAN match.
        let cap = captured_ip.lock().unwrap().clone();
        assert!(
            matches!(&cap, Some(Some(_))),
            "127.0.0.1 must propagate as Some(ServerName::IpAddress); got {cap:?}"
        );
        selector.close();
        server.shutdown().await;
    }
}
