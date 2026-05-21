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

//! Translation of `org.apache.kafka.common.network.Selectable`.
//!
//! Java's `Selectable` is the trait through which `NetworkClient` drives
//! the `Selector`. It exposes connect/poll/send/disconnect plus the
//! per-poll output collections (completed sends/receives, disconnected
//! nodes, newly connected nodes).
//!
//! The Rust translation keeps the same shape but with two adjustments:
//!
//! 1. **Async `poll`.** Java's `poll(long)` blocks the calling thread on
//!    NIO selection. Rust uses Tokio; `poll` is `async fn` (CLAUDE.md
//!    rule 9.1).
//!
//! 2. **Numeric connection ids.** Java keys connections by `String`
//!    (see `Selectable.connect(String id, ...)`). The Rust translation
//!    uses `i32` everywhere — it is the broker node id (`Node::id()`)
//!    that the producer code path always converts from, and using the
//!    integer avoids a per-message `String` clone on the hot path
//!    (CLAUDE.md rule 11; see `design/history/Milestone-1/Phase-5/NOTES.md`
//!    "Hot-path identifier interning"). The Selector implementation in
//!    Phase 5c-2 will accept `i32` ids directly; downstream
//!    `KafkaChannel`s already accept `Arc<str>` for the human-readable
//!    label, so no string is materialised on the request path.

use std::net::SocketAddr;

use crate::common::errors::KafkaError;
use crate::common::network::{ChannelState, NetworkReceive, NetworkSend};

/// Mirrors `Selectable.USE_DEFAULT_BUFFER_SIZE`.
pub const USE_DEFAULT_BUFFER_SIZE: i32 = -1;

/// An interface for asynchronous, multi-channel network I/O.
///
/// Mirrors Java's `org.apache.kafka.common.network.Selectable`. See the
/// module-level rustdoc for the design notes on `i32` ids and the
/// `async fn poll` signature.
///
/// All methods are documented in lock-step with the Java original. The
/// poll-output accessors (`completed_sends`, `completed_receives`,
/// `disconnected`, `connected`) reset on each call to `poll` — same
/// semantics as Java.
pub trait Selectable: Send {
    /// Begin establishing a socket connection to the given address.
    ///
    /// * `id` — the connection id (broker node id).
    /// * `host` — the unresolved hostname (used for SNI on SSL / SASL_SSL
    ///   channels; ignored by plaintext channels).
    /// * `address` — the resolved peer address.
    /// * `send_buffer_size` — SO_SNDBUF (use [`USE_DEFAULT_BUFFER_SIZE`]).
    /// * `receive_buffer_size` — SO_RCVBUF (use [`USE_DEFAULT_BUFFER_SIZE`]).
    ///
    /// Mirrors `Selectable.connect(String, InetSocketAddress, int, int)`.
    /// Java derives the SNI hostname from
    /// `SocketChannel.socket().getInetAddress()` (reverse-DNS lookup on
    /// the resolved peer); the Rust translation passes the unresolved
    /// hostname explicitly because the producer's connect path already
    /// knows it from the original bootstrap entry — this avoids a
    /// reverse-DNS roundtrip that could disagree with the cert's SAN
    /// (see [`crate::common::network::SslChannelBuilder::build_ssl_channel`]).
    ///
    /// Java throws `IOException`; we surface failures as
    /// [`KafkaError::Network`] (retriable).
    fn connect(
        &mut self,
        id: i32,
        host: &str,
        address: SocketAddr,
        send_buffer_size: i32,
        receive_buffer_size: i32,
    ) -> Result<(), KafkaError>;

    /// Wakeup this selector if it is blocked on I/O. Mirrors
    /// `Selectable.wakeup()`.
    fn wakeup(&self);

    /// Close this selector. Mirrors `Selectable.close()`.
    fn close(&mut self);

    /// Close the connection identified by the given id. Mirrors
    /// `Selectable.close(String)`.
    fn close_connection(&mut self, id: i32);

    /// Queue the given request for sending in the subsequent
    /// [`Self::poll`] calls. Mirrors `Selectable.send(NetworkSend)`.
    fn send(&mut self, send: NetworkSend);

    /// Do I/O. Reads, writes, connection establishment, etc.
    ///
    /// `timeout_ms` is the maximum amount of time to block when there is
    /// nothing to do. Mirrors `Selectable.poll(long)`. Java blocks the
    /// thread on `Selector.select(timeout)`; the Rust translation is
    /// async because Tokio uses cooperative scheduling.
    ///
    /// Java throws `IOException`; we surface failures as
    /// [`KafkaError::Network`].
    ///
    /// Returns a `Send` future so [`crate::KafkaClient::poll`] (which
    /// `await`s this) can in turn return a `Send` future — required so
    /// the producer's [`crate::producer::internals::sender::Sender`]
    /// run loop can be moved into a `tokio::spawn` task that is itself
    /// `Send`.
    fn poll(&mut self, timeout_ms: i64) -> impl std::future::Future<Output = Result<(), KafkaError>> + Send;

    /// The list of sends that completed on the last [`Self::poll`] call.
    /// Mirrors `Selectable.completedSends()`.
    fn completed_sends(&self) -> &[NetworkSend];

    /// The collection of receives that completed on the last
    /// [`Self::poll`] call.
    ///
    /// Mirrors `Selectable.completedReceives()`. Note: Java's contract
    /// says callers are responsible for closing the returned receives if
    /// they were backed by a `MemoryPool`. Our Phase 5a `NetworkReceive`
    /// allocates eagerly (no `MemoryPool`), so dropping the slice is
    /// sufficient.
    fn completed_receives(&self) -> &[NetworkReceive];

    /// The connections that finished disconnecting on the last
    /// [`Self::poll`] call. The map value indicates the local channel
    /// state at the time of disconnection. Mirrors
    /// `Selectable.disconnected()`.
    fn disconnected(&self) -> &std::collections::HashMap<i32, ChannelState>;

    /// The list of connections that completed their connection on the
    /// last [`Self::poll`] call. Mirrors `Selectable.connected()`.
    fn connected(&self) -> &[i32];

    /// Disable reads from the given connection. Mirrors
    /// `Selectable.mute(String)`.
    fn mute(&mut self, id: i32);

    /// Re-enable reads from the given connection. Mirrors
    /// `Selectable.unmute(String)`.
    fn unmute(&mut self, id: i32);

    /// Disable reads from all connections. Mirrors
    /// `Selectable.muteAll()`.
    fn mute_all(&mut self);

    /// Re-enable reads from all connections. Mirrors
    /// `Selectable.unmuteAll()`.
    fn unmute_all(&mut self);

    /// Returns true if a channel is ready. Mirrors
    /// `Selectable.isChannelReady(String)`.
    fn is_channel_ready(&self, id: i32) -> bool;
}
