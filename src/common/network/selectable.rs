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

//! An interface for asynchronous, multi-channel network I/O.
//!
//! Translated from `org.apache.kafka.common.network.Selectable`.

use super::ChannelState;
use super::NetworkReceive;
use super::NetworkSend;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::Notify;

/// See [`Selectable::connect`] — use the platform default buffer size.
pub const USE_DEFAULT_BUFFER_SIZE: i32 = -1;

/// An interface for asynchronous, multi-channel network I/O.
///
/// Translated from the Java `Selectable` interface.
///
/// All I/O methods are `async` per CLAUDE.md rule 8.
pub trait Selectable: Send {
    /// Begin establishing a socket connection to the given address identified by
    /// the given id.
    ///
    /// # Arguments
    ///
    /// * `id` - The id for this connection
    /// * `address` - The address to connect to
    /// * `peer_host` - The hostname of the remote peer (used for TLS SNI and hostname verification)
    /// * `send_buffer_size` - The send buffer for the socket
    ///   (use [`USE_DEFAULT_BUFFER_SIZE`] for platform default)
    /// * `receive_buffer_size` - The receive buffer for the socket
    ///   (use [`USE_DEFAULT_BUFFER_SIZE`] for platform default)
    ///
    /// # Errors
    ///
    /// Returns an error if we cannot begin connecting.
    fn connect(
        &mut self,
        id: &str,
        address: SocketAddr,
        peer_host: &str,
        send_buffer_size: i32,
        receive_buffer_size: i32,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send;

    /// Wakeup this selector if it is blocked on I/O.
    fn wakeup(&self);

    /// Returns the [`Notify`] handle used by the selector's poll loop.
    ///
    /// Callers can use this to share the selector's wakeup mechanism,
    /// ensuring that `notify_one()` on the returned handle causes the
    /// selector's `poll()` to return promptly.
    fn wakeup_notify(&self) -> Arc<Notify> {
        Arc::new(Notify::new())
    }

    /// Close this selector.
    fn close(&mut self) -> impl std::future::Future<Output = ()> + Send;

    /// Close the connection identified by the given id.
    fn close_channel(&mut self, id: &str) -> impl std::future::Future<Output = ()> + Send;

    /// Queue the given request for sending in the subsequent `poll()` calls.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel does not exist.
    fn send(&mut self, send: NetworkSend) -> Result<(), String>;

    /// Do I/O. Reads, writes, connection establishment, etc.
    ///
    /// # Arguments
    ///
    /// * `timeout_ms` - The amount of time to block if there is nothing to do
    ///
    /// # Errors
    ///
    /// Returns an error if I/O fails.
    fn poll(&mut self, timeout_ms: i64) -> impl std::future::Future<Output = io::Result<()>> + Send;

    /// The list of sends that completed on the last `poll()` call.
    fn completed_sends(&self) -> &[NetworkSend];

    /// The collection of receives that completed on the last `poll()` call.
    fn completed_receives(&self) -> Vec<&NetworkReceive>;

    /// The connections that finished disconnecting on the last `poll()` call.
    /// Channel state indicates the local channel state at the time of disconnection.
    fn disconnected(&self) -> &HashMap<String, ChannelState>;

    /// The list of connections that completed their connection on the last `poll()` call.
    fn connected(&self) -> &[String];

    /// Disable reads from the given connection.
    fn mute(&mut self, id: &str);

    /// Re-enable reads from the given connection.
    fn unmute(&mut self, id: &str);

    /// Disable reads from all connections.
    fn mute_all(&mut self);

    /// Re-enable reads from all connections.
    fn unmute_all(&mut self);

    /// Returns `true` if a channel is ready.
    fn is_channel_ready(&self, id: &str) -> bool;
}
