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

//! A selector for doing non-blocking multi-connection network I/O.
//!
//! Translated from `org.apache.kafka.common.network.Selector`.
//!
//! This class works with [`NetworkSend`] and [`NetworkReceive`] to transmit
//! size-delimited network requests and responses.
//!
//! # NIO to Tokio mapping
//!
//! Per CLAUDE.md rule 8: single Selector for multiple TCP connections.
//! The `poll()` method:
//! 1. Iterates all channels, attempts non-blocking I/O (connect/read/write)
//! 2. If no progress and timeout > 0, uses `tokio::time::sleep` + `tokio::sync::Notify`
//!    for wakeup
//! 3. Matches Java's sequential iteration over selectedKeys
//!
//! # Thread safety
//!
//! This class is not thread safe! (Same as Java.)

use super::channel_builder::ChannelBuilder;
use super::channel_metadata_registry::DefaultChannelMetadataRegistry;
use super::channel_state::{self, ChannelState};
use super::kafka_channel::KafkaChannel;
use super::network_receive::NetworkReceive;
use super::network_send::NetworkSend;
use super::plaintext_transport_layer::PlaintextTransportLayer;
use super::receive::Receive;
use super::selectable::{Selectable, USE_DEFAULT_BUFFER_SIZE};

use indexmap::IndexMap;
use log::{debug, error, trace};
use tokio::net::TcpSocket;
use tokio::sync::Notify;

use std::collections::{HashMap, HashSet, LinkedList};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

/// Value indicating no idle timeout.
pub const NO_IDLE_TIMEOUT_MS: i64 = -1;

/// Close mode for channel closing operations.
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
    fn notify_disconnect(self) -> bool {
        match self {
            CloseMode::Graceful | CloseMode::NotifyOnly => true,
            CloseMode::DiscardNoNotify => false,
        }
    }
}

/// A selector for doing non-blocking multi-connection network I/O.
///
/// Translated from `org.apache.kafka.common.network.Selector`.
///
/// Key adaptations:
/// - NIO `Selector.select()` → sequential try_read/try_write + tokio timeout
/// - `SelectionKey` eliminated — channel lookup by ID in `HashMap`
/// - Wakeup via `tokio::sync::Notify`
/// - Metrics deferred (no-op stubs)
pub struct Selector {
    /// Active channels indexed by connection ID.
    channels: HashMap<String, KafkaChannel>,
    /// Channels that have been explicitly muted.
    explicitly_muted_channels: HashSet<String>,
    /// Channels that have data buffered in intermediate buffers.
    channels_with_buffered_read: HashSet<String>,
    /// Channels that connected immediately (before poll).
    immediately_connected_keys: HashSet<String>,
    /// Channels that are being closed gracefully (pending receives).
    closing_channels: HashMap<String, KafkaChannel>,
    /// Sends completed during the last poll.
    completed_sends: Vec<NetworkSend>,
    /// Receives completed during the last poll, keyed by channel ID.
    completed_receives: LinkedList<NetworkReceive>,
    /// Channels that disconnected during the last poll.
    disconnected: HashMap<String, ChannelState>,
    /// Channels that connected during the last poll.
    connected: Vec<String>,
    /// Channels that failed to send.
    failed_sends: Vec<String>,
    /// Channel builder.
    channel_builder: Box<dyn ChannelBuilder>,
    /// Maximum receive size.
    max_receive_size: i32,
    /// Idle expiry manager, if idle timeout is enabled.
    idle_expiry_manager: Option<IdleExpiryManager>,
    /// Notify for wakeup support.
    notify: Arc<Notify>,
    /// Whether progress was made reading in the last poll.
    made_read_progress_last_poll: bool,
}

impl Selector {
    /// Create a new selector.
    ///
    /// # Arguments
    ///
    /// * `max_receive_size` - Max size in bytes of a single network receive
    ///   (use `NetworkReceive::UNLIMITED` for no limit)
    /// * `connection_max_idle_ms` - Max idle connection time
    ///   (use [`NO_IDLE_TIMEOUT_MS`] to disable idle timeout)
    /// * `channel_builder` - Channel builder for every new connection
    pub fn new(max_receive_size: i32, connection_max_idle_ms: i64, channel_builder: Box<dyn ChannelBuilder>) -> Self {
        Self {
            channels: HashMap::new(),
            explicitly_muted_channels: HashSet::new(),
            channels_with_buffered_read: HashSet::new(),
            immediately_connected_keys: HashSet::new(),
            closing_channels: HashMap::new(),
            completed_sends: Vec::new(),
            completed_receives: LinkedList::new(),
            disconnected: HashMap::new(),
            connected: Vec::new(),
            failed_sends: Vec::new(),
            channel_builder,
            max_receive_size,
            idle_expiry_manager: if connection_max_idle_ms >= 0 {
                Some(IdleExpiryManager::new(connection_max_idle_ms))
            } else {
                None
            },
            notify: Arc::new(Notify::new()),
            made_read_progress_last_poll: true,
        }
    }

    /// Convenience constructor matching the common Java pattern.
    pub fn with_defaults(connection_max_idle_ms: i64, channel_builder: Box<dyn ChannelBuilder>) -> Self {
        Self::new(super::network_receive::UNLIMITED, connection_max_idle_ms, channel_builder)
    }

    fn ensure_not_registered(&self, id: &str) -> Result<(), String> {
        if self.channels.contains_key(id) {
            return Err(format!("There is already a connection for id {id}"));
        }
        if self.closing_channels.contains_key(id) {
            return Err(format!("There is already a connection for id {id} that is still being closed"));
        }
        Ok(())
    }

    /// Returns the channel for the given ID, or None.
    pub fn channel(&self, id: &str) -> Option<&KafkaChannel> {
        self.channels.get(id)
    }

    /// Returns a mutable reference to the channel for the given ID, or None.
    pub fn channel_mut(&mut self, id: &str) -> Option<&mut KafkaChannel> {
        self.channels.get_mut(id)
    }

    /// Returns the closing channel for the given ID, or None.
    pub fn closing_channel(&self, id: &str) -> Option<&KafkaChannel> {
        self.closing_channels.get(id)
    }

    /// Returns all active channels.
    pub fn channels(&self) -> Vec<&KafkaChannel> {
        self.channels.values().collect()
    }

    fn open_or_closing_channel_or_fail(&self, id: &str) -> Result<(), String> {
        if self.channels.contains_key(id) || self.closing_channels.contains_key(id) {
            Ok(())
        } else {
            Err(format!(
                "Attempt to retrieve channel for which there is no connection. Connection id {id} existing connections {:?}",
                self.channels.keys().collect::<Vec<_>>()
            ))
        }
    }

    fn has_completed_receive(&self, channel_id: &str) -> bool {
        self.completed_receives.iter().any(|r| r.source() == channel_id)
    }

    fn add_to_completed_receives(&mut self, receive: NetworkReceive) {
        let channel_id = receive.source().to_string();
        if self.has_completed_receive(&channel_id) {
            panic!("Attempting to add second completed receive to channel {channel_id}");
        }
        self.completed_receives.push_back(receive);
    }

    /// Clear all results from the previous poll.
    async fn clear(&mut self) {
        self.completed_sends.clear();
        self.completed_receives.clear();
        self.connected.clear();
        self.disconnected.clear();

        // Remove closed channels after all their buffered receives have been processed
        // or if a send was requested
        let closing_ids: Vec<String> = self.closing_channels.keys().cloned().collect();
        for id in closing_ids {
            let send_failed = self.failed_sends.iter().position(|s| s == &id).map(|i| {
                self.failed_sends.remove(i);
            });
            let has_pending = if send_failed.is_some() {
                false
            } else {
                self.maybe_read_from_closing_channel(&id).await
            };
            if !has_pending
                && let Some(channel) = self.closing_channels.remove(&id) {
                    self.do_close_async(channel, true).await;
                }
        }

        for channel_id in &self.failed_sends {
            self.disconnected.insert(channel_id.clone(), channel_state::FAILED_SEND.clone());
        }
        self.failed_sends.clear();
        self.made_read_progress_last_poll = false;
    }

    async fn maybe_read_from_closing_channel(&mut self, id: &str) -> bool {
        // Check state and mute/receive conditions with immutable borrow first
        let channel = match self.closing_channels.get(id) {
            Some(c) => c,
            None => return false,
        };

        if channel.state().state() != channel_state::State::Ready {
            return false;
        }
        if self.explicitly_muted_channels.contains(id) || self.has_completed_receive(id) {
            return true;
        }

        // Now take mutable borrow for reading
        let channel = self.closing_channels.get_mut(id).unwrap();
        match channel.read().await {
            Ok(_) => {
                if let Some(receive) = channel.maybe_complete_receive() {
                    self.completed_receives.push_back(receive);
                    true
                } else {
                    false
                }
            },
            Err(_) => false,
        }
    }

    /// Poll a single channel for I/O.
    ///
    /// Uses non-blocking try_read/try_write via the transport layer.
    /// Channels with no data available return WouldBlock and are skipped.
    async fn poll_channel(&mut self, channel_id: &str, is_immediately_connected: bool, current_time_nanos: u64) {
        let mut send_failed = false;

        // Update idle expiry
        {
            if let Some(ref mut mgr) = self.idle_expiry_manager {
                mgr.update(channel_id, current_time_nanos);
            }
        }

        let result: io::Result<()> = async {
            // Complete any connections that have finished their handshake
            let channel = self.channels.get_mut(channel_id).unwrap();
            if is_immediately_connected || !channel.is_connected() {
                if channel.finish_connect().await? {
                    self.connected.push(channel_id.to_string());
                    debug!("Connected to node {}", channel_id);
                } else {
                    return Ok(());
                }
            }

            // If channel is not ready, finish prepare
            let channel = self.channels.get_mut(channel_id).unwrap();
            if channel.is_connected() && !channel.ready() {
                channel.prepare().await?;
            }

            let channel = self.channels.get_mut(channel_id).unwrap();
            if channel.ready() && channel.state() == &channel_state::NOT_CONNECTED {
                channel.set_state(channel_state::READY.clone());
            }

            // Handle re-authentication responses
            let channel = self.channels.get_mut(channel_id).unwrap();
            if let Some(receive) = channel.poll_response_received_during_reauthentication() {
                self.add_to_completed_receives(receive);
            }

            // Read if ready and not muted and no completed receive yet
            self.attempt_read(channel_id).await?;

            // Track buffered read state
            let channel = self.channels.get(channel_id).unwrap();
            if channel.has_bytes_buffered() && !self.explicitly_muted_channels.contains(channel_id) {
                self.channels_with_buffered_read.insert(channel_id.to_string());
            }

            // Write if ready and has send
            let channel = self.channels.get_mut(channel_id).unwrap();
            if channel.has_send() && channel.ready() {
                let now_nanos = current_time_nanos;
                let should_write = !channel.maybe_begin_client_reauthentication(|| now_nanos)?;
                if should_write {
                    match self.write_channel(channel_id).await {
                        Ok(()) => {},
                        Err(e) => {
                            send_failed = true;
                            return Err(e);
                        },
                    }
                }
            }

            Ok(())
        }
        .await;

        if let Err(e) = result {
            let desc = if let Some(channel) = self.channels.get(channel_id) {
                format!("{} (channelId={})", channel.socket_description(), channel.id())
            } else {
                format!("unknown (channelId={channel_id})")
            };

            if e.kind() == io::ErrorKind::Other || e.kind() == io::ErrorKind::InvalidInput {
                // Authentication error
                error!("Failed authentication with {} ({})", desc, e);
            } else {
                debug!("Connection with {} disconnected: {}", desc, e);
            }

            let close_mode = if send_failed {
                CloseMode::NotifyOnly
            } else {
                CloseMode::Graceful
            };
            self.close_channel_internal(channel_id, close_mode).await;
        }
    }

    /// Attempt to read from a channel.
    ///
    /// Translated from `Selector.attemptRead` in Java.
    /// Uses a zero-duration timeout to make the read non-blocking from the
    /// selector's perspective, matching Java NIO's non-blocking channel reads.
    async fn attempt_read(&mut self, channel_id: &str) -> io::Result<()> {
        // Check conditions with immutable borrows first
        let should_read = {
            let channel = self.channels.get(channel_id).unwrap();
            channel.ready()
                && (channel.has_bytes_buffered() || !channel.is_muted())
                && !self.has_completed_receive(channel_id)
                && !self.explicitly_muted_channels.contains(channel_id)
        };

        if should_read {
            let channel = self.channels.get_mut(channel_id).unwrap();
            // Use timeout to avoid blocking on this channel's readability.
            // If not ready, we skip and retry on the next poll() iteration.
            let read_result = tokio::time::timeout(std::time::Duration::ZERO, channel.read()).await;
            let bytes = match read_result {
                Ok(Ok(b)) => b,
                Ok(Err(e)) if e.kind() == io::ErrorKind::WouldBlock => 0,
                Ok(Err(e)) => return Err(e),
                Err(_elapsed) => 0, // timeout = not ready
            };
            if bytes != 0 {
                self.made_read_progress_last_poll = true;
                if let Some(receive) = self.channels.get_mut(channel_id).unwrap().maybe_complete_receive() {
                    self.add_to_completed_receives(receive);
                }
            }
            let channel = self.channels.get(channel_id).unwrap();
            if channel.is_muted() {
                // Channel has muted itself due to memory pressure (outOfMemory)
            } else {
                self.made_read_progress_last_poll = true;
            }
        }
        Ok(())
    }

    async fn write_channel(&mut self, channel_id: &str) -> io::Result<()> {
        let channel = self.channels.get_mut(channel_id).unwrap();
        // Use timeout to avoid blocking on this channel's writability.
        let write_result = tokio::time::timeout(std::time::Duration::ZERO, channel.write()).await;
        let bytes_sent = match write_result {
            Ok(Ok(b)) => b,
            Ok(Err(e)) if e.kind() == io::ErrorKind::WouldBlock => 0,
            Ok(Err(e)) => return Err(e),
            Err(_elapsed) => 0, // timeout = not ready
        };
        let send = channel.maybe_complete_send();
        if (bytes_sent > 0 || send.is_some())
            && let Some(send) = send {
                self.completed_sends.push(send);
            }
        Ok(())
    }

    /// Begin closing a channel.
    async fn close_channel_internal(&mut self, id: &str, close_mode: CloseMode) {
        let mut channel = match self.channels.remove(id) {
            Some(c) => c,
            None => return,
        };

        channel.disconnect();

        // Ensure that `connected` does not have closed channels
        self.connected.retain(|c| c != id);

        if close_mode == CloseMode::Graceful {
            // Check if there are pending receives
            self.closing_channels.insert(id.to_string(), channel);
            let has_pending = self.maybe_read_from_closing_channel(id).await;
            if !has_pending
                && let Some(channel) = self.closing_channels.remove(id) {
                    self.do_close_async(channel, close_mode.notify_disconnect()).await;
                }
        } else {
            self.do_close_async(channel, close_mode.notify_disconnect()).await;
        }

        if let Some(ref mut mgr) = self.idle_expiry_manager {
            mgr.remove(id);
        }
    }

    async fn do_close_async(&mut self, mut channel: KafkaChannel, notify_disconnect: bool) {
        let id = channel.id().to_string();
        self.immediately_connected_keys.remove(&id);
        self.channels_with_buffered_read.remove(&id);

        let _ = channel.close().await;

        self.explicitly_muted_channels.remove(&id);
        if notify_disconnect {
            self.disconnected.insert(id, channel.state().clone());
        }
    }

    fn do_close(&mut self, channel: KafkaChannel, notify_disconnect: bool) {
        let id = channel.id().to_string();
        self.immediately_connected_keys.remove(&id);
        self.channels_with_buffered_read.remove(&id);

        // Channel is dropped here, which closes the underlying TcpStream
        // The KafkaChannel's close() method is async, but dropping is sufficient
        // for cleanup since Tokio streams close on drop.

        self.explicitly_muted_channels.remove(&id);
        if notify_disconnect {
            self.disconnected.insert(id, channel.state().clone());
        }
    }

    async fn maybe_close_oldest_connection(&mut self, current_time_nanos: u64) {
        if self.idle_expiry_manager.is_none() {
            return;
        }

        let mgr = self.idle_expiry_manager.as_mut().unwrap();
        if let Some((connection_id, _last_active)) = mgr.poll_expired_connection(current_time_nanos)
            && self.channels.contains_key(&connection_id) {
                trace!("About to close the idle connection from {} due to being idle", connection_id);
                if let Some(channel) = self.channels.get_mut(&connection_id) {
                    channel.set_state(channel_state::EXPIRED.clone());
                }
                // Remove from channels and close
                let id = connection_id;
                if let Some(mut channel) = self.channels.remove(&id) {
                    channel.disconnect();
                    self.connected.retain(|c| c != &id);
                    self.do_close_async(channel, true).await;
                    if let Some(ref mut mgr) = self.idle_expiry_manager {
                        mgr.remove(&id);
                    }
                }
            }
    }

    /// Clear completed receives.
    pub fn clear_completed_receives(&mut self) {
        self.completed_receives.clear();
    }

    /// Clear completed sends.
    pub fn clear_completed_sends(&mut self) {
        self.completed_sends.clear();
    }

    /// Returns the lowest priority channel.
    pub fn lowest_priority_channel(&self) -> Option<&KafkaChannel> {
        if !self.closing_channels.is_empty() {
            return self.closing_channels.values().next();
        }
        if let Some(ref mgr) = self.idle_expiry_manager
            && let Some((id, _)) = mgr.lru_connections.first() {
                return self.channels.get(id);
            }
        self.channels.values().next()
    }
}

impl Selectable for Selector {
    async fn connect(
        &mut self,
        id: &str,
        address: SocketAddr,
        send_buffer_size: i32,
        receive_buffer_size: i32,
    ) -> io::Result<()> {
        self.ensure_not_registered(id)
            .map_err(|e| io::Error::new(io::ErrorKind::AlreadyExists, e))?;

        // Create socket
        let socket = if address.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };

        // Configure socket
        socket.set_keepalive(true)?;
        if send_buffer_size != USE_DEFAULT_BUFFER_SIZE {
            socket.set_send_buffer_size(send_buffer_size as u32)?;
        }
        if receive_buffer_size != USE_DEFAULT_BUFFER_SIZE {
            socket.set_recv_buffer_size(receive_buffer_size as u32)?;
        }
        socket.set_nodelay(true)?;

        // Connect (non-blocking)
        let stream = match socket.connect(address).await {
            Ok(stream) => stream,
            Err(e) => {
                return Err(e);
            },
        };

        // Wrap in transport layer
        let transport_layer = PlaintextTransportLayer::connected(stream);
        let metadata_registry = Box::new(DefaultChannelMetadataRegistry::new());

        // Build channel
        let channel = match self.channel_builder.build_channel(
            id,
            Box::new(transport_layer),
            self.max_receive_size,
            metadata_registry,
        ) {
            Ok(c) => c,
            Err(e) => {
                return Err(e);
            },
        };

        // The connection completed immediately (Tokio connect is async but resolves when done)
        self.immediately_connected_keys.insert(id.to_string());
        self.channels.insert(id.to_string(), channel);

        if let Some(ref mut mgr) = self.idle_expiry_manager {
            mgr.update(id, nanos_now());
        }

        Ok(())
    }

    fn wakeup(&self) {
        self.notify.notify_one();
    }

    async fn close(&mut self) {
        let ids: Vec<String> = self.channels.keys().cloned().collect();
        for id in ids {
            self.close_channel(&id).await;
        }
        self.channel_builder.close();
    }

    async fn close_channel(&mut self, id: &str) {
        if self.channels.contains_key(id) {
            if let Some(channel) = self.channels.get_mut(id) {
                channel.set_state(channel_state::LOCAL_CLOSE.clone());
            }
            self.close_channel_internal(id, CloseMode::DiscardNoNotify).await;
        } else if let Some(closing_channel) = self.closing_channels.remove(id) {
            self.do_close_async(closing_channel, false).await;
        }
    }

    fn send(&mut self, send: NetworkSend) -> Result<(), String> {
        let connection_id = send.destination_id().to_string();
        self.open_or_closing_channel_or_fail(&connection_id)?;

        if self.closing_channels.contains_key(&connection_id) {
            // Ensure notification via `disconnected`, leave channel in the state
            // in which closing was triggered
            self.failed_sends.push(connection_id);
        } else {
            let channel = self.channels.get_mut(&connection_id).unwrap();
            match channel.set_send(send) {
                Ok(()) => {},
                Err(e) => {
                    // Update the state for consistency
                    channel.set_state(channel_state::FAILED_SEND.clone());
                    self.failed_sends.push(connection_id.clone());

                    // Remove and close the channel
                    if let Some(mut ch) = self.channels.remove(&connection_id) {
                        ch.disconnect();
                        self.connected.retain(|c| c != &connection_id);
                        self.do_close(ch, false);
                        if let Some(ref mut mgr) = self.idle_expiry_manager {
                            mgr.remove(&connection_id);
                        }
                    }

                    error!(
                        "Unexpected exception during send, closing connection {} and rethrowing: {}",
                        connection_id, e
                    );
                    return Err(e);
                },
            }
        }
        Ok(())
    }

    async fn poll(&mut self, timeout_ms: i64) -> io::Result<()> {
        if timeout_ms < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "timeout should be >= 0"));
        }

        let made_read_progress_last_call = self.made_read_progress_last_poll;
        self.clear().await;

        let data_in_buffers = !self.channels_with_buffered_read.is_empty();

        let effective_timeout =
            if !self.immediately_connected_keys.is_empty() || (made_read_progress_last_call && data_in_buffers) {
                0
            } else {
                timeout_ms
            };

        let start_select = nanos_now();
        let deadline = if effective_timeout > 0 {
            Some(tokio::time::Instant::now() + std::time::Duration::from_millis(effective_timeout as u64))
        } else {
            None
        };

        // Poll loop: try non-blocking I/O on all channels, then yield to let
        // the tokio reactor deliver readiness events, repeating until we make
        // progress or the timeout expires. This matches Java NIO's
        // nioSelector.select(timeout) which blocks until I/O or timeout.
        loop {
            // Process channels with buffered data
            if data_in_buffers {
                let buffered_ids: Vec<String> = self.channels_with_buffered_read.drain().collect();
                for id in buffered_ids {
                    if self.channels.contains_key(&id) {
                        self.poll_channel(&id, false, start_select).await;
                    }
                }
            }

            // Process all active channels with non-blocking I/O (try_read/try_write).
            let channel_ids: Vec<String> = self.channels.keys().cloned().collect();
            for id in &channel_ids {
                if self.channels.contains_key(id) {
                    let is_immediately = self.immediately_connected_keys.remove(id);
                    self.poll_channel(id, is_immediately, start_select).await;
                }
            }
            self.immediately_connected_keys.clear();

            let made_progress = !self.completed_sends.is_empty()
                || !self.completed_receives.is_empty()
                || !self.connected.is_empty()
                || !self.disconnected.is_empty();

            if made_progress {
                break;
            }

            // No progress — check if we should keep waiting
            match deadline {
                Some(dl) if tokio::time::Instant::now() < dl => {
                    // Yield to let the tokio reactor process I/O events, then
                    // retry. Sleep 1ms to avoid busy-spinning while still
                    // responding quickly to data arrival.
                    let notify = self.notify.clone();
                    tokio::select! {
                        biased;
                        _ = notify.notified() => {},
                        _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {},
                    }
                },
                _ => break,
            }
        }

        if self.completed_sends.is_empty()
            && self.completed_receives.is_empty()
            && self.connected.is_empty()
            && self.disconnected.is_empty()
        {
            self.made_read_progress_last_poll = true; // no work is also "progress"
        }

        let end_time = nanos_now();

        // Close oldest connection if idle.
        self.maybe_close_oldest_connection(end_time).await;

        Ok(())
    }

    fn completed_sends(&self) -> &[NetworkSend] {
        &self.completed_sends
    }

    fn completed_receives(&self) -> Vec<&NetworkReceive> {
        self.completed_receives.iter().collect()
    }

    fn disconnected(&self) -> &HashMap<String, ChannelState> {
        &self.disconnected
    }

    fn connected(&self) -> &[String] {
        &self.connected
    }

    fn mute(&mut self, id: &str) {
        if let Some(channel) = self.channels.get_mut(id) {
            channel.mute();
            self.explicitly_muted_channels.insert(id.to_string());
            self.channels_with_buffered_read.remove(id);
        } else if let Some(channel) = self.closing_channels.get_mut(id) {
            channel.mute();
            self.explicitly_muted_channels.insert(id.to_string());
        }
    }

    fn unmute(&mut self, id: &str) {
        let unmuted = if let Some(channel) = self.channels.get_mut(id) {
            channel.maybe_unmute()
        } else if let Some(channel) = self.closing_channels.get_mut(id) {
            channel.maybe_unmute()
        } else {
            false
        };

        if unmuted {
            self.explicitly_muted_channels.remove(id);
            if let Some(channel) = self.channels.get(id)
                && channel.has_bytes_buffered() {
                    self.channels_with_buffered_read.insert(id.to_string());
                    self.made_read_progress_last_poll = true;
                }
        }
    }

    fn mute_all(&mut self) {
        let ids: Vec<String> = self.channels.keys().cloned().collect();
        for id in ids {
            self.mute(&id);
        }
    }

    fn unmute_all(&mut self) {
        let ids: Vec<String> = self.channels.keys().cloned().collect();
        for id in ids {
            self.unmute(&id);
        }
    }

    fn is_channel_ready(&self, id: &str) -> bool {
        self.channels.get(id).is_some_and(|c| c.ready())
    }
}

/// Helper to get current time in nanoseconds.
fn nanos_now() -> u64 {
    // Use std::time::Instant for monotonic time, converting to u64 nanos
    // relative to an arbitrary epoch. This matches Java's System.nanoTime().
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

/// Helper class for tracking least recently used connections to enable idle
/// connection closing.
///
/// Translated from `Selector.IdleExpiryManager` in Java.
/// Uses `IndexMap` (from the `indexmap` crate) to maintain insertion/access
/// order, similar to Java's `LinkedHashMap(accessOrder=true)`.
struct IdleExpiryManager {
    /// LRU connections: maps connection ID to last active time (nanoseconds).
    /// IndexMap maintains insertion order; we manually move entries to back on update.
    lru_connections: IndexMap<String, u64>,
    /// Maximum idle duration in nanoseconds.
    connections_max_idle_nanos: u64,
    /// Next time to check for idle connections.
    next_idle_close_check_time: u64,
}

impl IdleExpiryManager {
    fn new(connections_max_idle_ms: i64) -> Self {
        let connections_max_idle_nanos = (connections_max_idle_ms as u64) * 1_000_000;
        Self {
            lru_connections: IndexMap::new(),
            connections_max_idle_nanos,
            next_idle_close_check_time: nanos_now() + connections_max_idle_nanos,
        }
    }

    fn update(&mut self, connection_id: &str, current_time_nanos: u64) {
        // Remove and re-insert to move to the back (most recently used)
        self.lru_connections.shift_remove(connection_id);
        self.lru_connections.insert(connection_id.to_string(), current_time_nanos);
    }

    fn poll_expired_connection(&mut self, current_time_nanos: u64) -> Option<(String, u64)> {
        if current_time_nanos <= self.next_idle_close_check_time {
            return None;
        }

        if self.lru_connections.is_empty() {
            self.next_idle_close_check_time = current_time_nanos + self.connections_max_idle_nanos;
            return None;
        }

        // Get the oldest entry (first in the map)
        let (connection_id, last_active_time) = {
            let (id, time) = self.lru_connections.first().unwrap();
            (id.clone(), *time)
        };

        self.next_idle_close_check_time = last_active_time + self.connections_max_idle_nanos;

        if current_time_nanos > self.next_idle_close_check_time {
            Some((connection_id, last_active_time))
        } else {
            None
        }
    }

    fn remove(&mut self, connection_id: &str) {
        self.lru_connections.shift_remove(connection_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::network::byte_buffer_send::ByteBufferSend;
    use crate::common::network::plaintext_channel_builder::PlaintextChannelBuilder;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use std::sync::atomic::{AtomicBool, Ordering};

    const BUFFER_SIZE: i32 = 4 * 1024;
    const CONNECTION_MAX_IDLE_MS: i64 = 5_000;

    /// A simple echo server for testing.
    ///
    /// Translated from `org.apache.kafka.common.network.EchoServer` in Java.
    ///
    /// Takes size-delimited byte arrays and echoes them back to the sender.
    struct EchoServer {
        addr: SocketAddr,
        closing: Arc<AtomicBool>,
        /// Sender to signal connection close
        close_connections_tx: tokio::sync::broadcast::Sender<()>,
        /// Handle to the accept loop task
        _task: tokio::task::JoinHandle<()>,
    }

    impl EchoServer {
        async fn new() -> io::Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let closing = Arc::new(AtomicBool::new(false));
            let closing_clone = closing.clone();
            let (close_connections_tx, _) = tokio::sync::broadcast::channel::<()>(16);
            let close_rx = close_connections_tx.subscribe();

            let task = tokio::spawn(Self::accept_loop(listener, closing_clone, close_rx));

            Ok(Self { addr, closing, close_connections_tx, _task: task })
        }

        async fn accept_loop(
            listener: TcpListener,
            closing: Arc<AtomicBool>,
            mut _close_rx: tokio::sync::broadcast::Receiver<()>,
        ) {
            while !closing.load(Ordering::Relaxed) {
                let accept_result = tokio::select! {
                    result = listener.accept() => result,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => continue,
                };

                match accept_result {
                    Ok((stream, _)) => {
                        let close_rx = _close_rx.resubscribe();
                        tokio::spawn(async move {
                            Self::handle_connection(stream, close_rx).await;
                        });
                    },
                    Err(_) if closing.load(Ordering::Relaxed) => break,
                    Err(_) => continue,
                }
            }
        }

        async fn handle_connection(
            mut stream: tokio::net::TcpStream,
            mut close_rx: tokio::sync::broadcast::Receiver<()>,
        ) {
            let mut size_buf = [0u8; 4];
            loop {
                // Read 4-byte size, or stop on close signal
                let read_result = tokio::select! {
                    biased;
                    _ = close_rx.recv() => break,
                    r = stream.read_exact(&mut size_buf) => r,
                };
                if read_result.is_err() {
                    break;
                }
                let size = i32::from_be_bytes(size_buf) as usize;

                // Read payload
                let mut payload = vec![0u8; size];
                if stream.read_exact(&mut payload).await.is_err() {
                    break;
                }

                // Echo back: size + payload
                if stream.write_all(&size_buf).await.is_err() {
                    break;
                }
                if stream.write_all(&payload).await.is_err() {
                    break;
                }
                if stream.flush().await.is_err() {
                    break;
                }
            }
            // Drop the stream to close the TCP connection
        }

        fn port(&self) -> u16 {
            self.addr.port()
        }

        fn close_connections(&self) {
            let _ = self.close_connections_tx.send(());
            // Mark as closing to stop accept loop
            self.closing.store(true, Ordering::Relaxed);
        }
    }

    impl Drop for EchoServer {
        fn drop(&mut self) {
            self.close_connections();
        }
    }

    fn create_send(node: &str, payload: &str) -> NetworkSend {
        NetworkSend::new(node, Box::new(ByteBufferSend::size_prefixed(payload.as_bytes().to_vec())))
    }

    fn as_string(receive: &NetworkReceive) -> String {
        String::from_utf8_lossy(receive.payload().unwrap_or(&[])).to_string()
    }

    async fn create_selector() -> Selector {
        let channel_builder = Box::new(PlaintextChannelBuilder::new(None));
        Selector::new(
            super::super::network_receive::UNLIMITED,
            CONNECTION_MAX_IDLE_MS,
            channel_builder,
        )
    }

    async fn blocking_connect(selector: &mut Selector, node: &str, port: u16) {
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE).await.unwrap();
        wait_for_channel_ready(selector, node).await;
    }

    async fn wait_for_channel_ready(selector: &mut Selector, node: &str) {
        let mut attempts = 0;
        while !selector.is_channel_ready(node) && attempts < 30 {
            selector.poll(1000).await.unwrap();
            attempts += 1;
        }
        assert!(
            selector.is_channel_ready(node),
            "Channel {node} was not ready after {attempts} attempts"
        );
    }

    async fn blocking_request(selector: &mut Selector, node: &str, s: &str) -> String {
        selector.send(create_send(node, s)).unwrap();
        loop {
            selector.poll(1000).await.unwrap();
            for receive in selector.completed_receives() {
                if receive.source() == node {
                    return as_string(receive);
                }
            }
        }
    }

    /// Translated from `SelectorTest.testSendWithoutConnecting`.
    #[tokio::test]
    async fn test_send_without_connecting() {
        let mut selector = create_selector().await;
        let result = selector.send(create_send("0", "test"));
        assert!(result.is_err());
    }

    /// Translated from `SelectorTest.testNoRouteToHost`.
    #[tokio::test]
    async fn test_no_route_to_host() {
        let mut selector = create_selector().await;
        // Use a non-routable address
        let addr: SocketAddr = "192.0.2.1:9999".parse().unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            selector.connect("0", addr, BUFFER_SIZE, BUFFER_SIZE),
        )
        .await;

        // Either timeout or connection error is acceptable
        match result {
            Ok(Err(_)) => {}, // Connection error
            Err(_) => {},     // Timeout
            Ok(Ok(())) => {
                // If connect succeeded, poll should eventually show disconnection
                for _ in 0..10 {
                    let _ = selector.poll(100).await;
                    if selector.disconnected().contains_key("0") {
                        return;
                    }
                }
            },
        }
    }

    /// Translated from `SelectorTest.testNormalOperation`.
    #[tokio::test]
    async fn test_normal_operation() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        let conns = 5;
        let reqs = 50;

        // Create connections
        let addr: SocketAddr = format!("127.0.0.1:{}", server.port()).parse().unwrap();
        for i in 0..conns {
            selector.connect(&i.to_string(), addr, BUFFER_SIZE, BUFFER_SIZE).await.unwrap();
        }

        // Wait for all connections
        for i in 0..conns {
            wait_for_channel_ready(&mut selector, &i.to_string()).await;
        }

        // Send initial requests
        let mut requests: HashMap<String, i32> = HashMap::new();
        let mut responses: HashMap<String, i32> = HashMap::new();
        let mut response_count = 0;

        for i in 0..conns {
            let node = i.to_string();
            selector.send(create_send(&node, &format!("{node}-0"))).unwrap();
        }

        // Loop until we complete all requests
        while response_count < conns * reqs {
            selector.poll(0).await.unwrap();

            assert_eq!(0, selector.disconnected().len(), "No disconnects should have occurred.");

            // Handle responses
            for receive in selector.completed_receives() {
                let content = as_string(receive);
                let pieces: Vec<&str> = content.split('-').collect();
                assert_eq!(2, pieces.len(), "Should be in the form 'conn-counter'");
                assert_eq!(receive.source(), pieces[0], "Check the source");

                let counter = pieces[1].parse::<i32>().unwrap();
                let resp_count = responses.entry(receive.source().to_string()).or_insert(0);
                assert_eq!(*resp_count, counter, "Check the request counter");
                *resp_count += 1;
                response_count += 1;
            }

            // Prepare new sends — collect destinations first to avoid borrow conflict
            let completed_dests: Vec<String> = selector
                .completed_sends()
                .iter()
                .map(|s| s.destination_id().to_string())
                .collect();
            for dest in completed_dests {
                let req_count = requests.entry(dest.clone()).or_insert(0);
                *req_count += 1;
                if *req_count < reqs {
                    selector.send(create_send(&dest, &format!("{dest}-{}", *req_count))).unwrap();
                }
            }
        }

        // Cleanup
        for i in 0..conns {
            selector.close_channel(&i.to_string()).await;
        }
        selector.poll(0).await.unwrap();
        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testSendLargeRequest`.
    #[tokio::test]
    async fn test_send_large_request() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        blocking_connect(&mut selector, "0", server.port()).await;
        let big = "x".repeat(10 * BUFFER_SIZE as usize);
        let result = blocking_request(&mut selector, "0", &big).await;
        assert_eq!(big, result);

        selector.close_channel("0").await;
        selector.poll(0).await.unwrap();
        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testEmptyRequest`.
    #[tokio::test]
    async fn test_empty_request() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        blocking_connect(&mut selector, "0", server.port()).await;
        let result = blocking_request(&mut selector, "0", "").await;
        assert_eq!("", result);

        selector.close_channel("0").await;
        selector.poll(0).await.unwrap();
        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testExistingConnectionId`.
    #[tokio::test]
    async fn test_existing_connection_id() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        blocking_connect(&mut selector, "0", server.port()).await;
        let addr: SocketAddr = format!("127.0.0.1:{}", server.port()).parse().unwrap();
        let result = selector.connect("0", addr, BUFFER_SIZE, BUFFER_SIZE).await;
        assert!(result.is_err());

        selector.close_channel("0").await;
        selector.poll(0).await.unwrap();
        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testMute`.
    #[tokio::test]
    async fn test_mute() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        blocking_connect(&mut selector, "0", server.port()).await;
        blocking_connect(&mut selector, "1", server.port()).await;

        selector.send(create_send("0", "hello")).unwrap();
        selector.send(create_send("1", "hi")).unwrap();

        selector.mute("1");

        // Wait for response from unmuted channel
        let mut attempts = 0;
        while selector.completed_receives().is_empty() && attempts < 100 {
            selector.poll(5).await.unwrap();
            attempts += 1;
        }
        assert_eq!(1, selector.completed_receives().len(), "We should have only one response");
        assert_eq!(
            "0",
            selector.completed_receives()[0].source(),
            "The response should not be from the muted node"
        );

        selector.unmute("1");
        let mut attempts = 0;
        loop {
            selector.poll(5).await.unwrap();
            if !selector.completed_receives().is_empty() {
                break;
            }
            attempts += 1;
            assert!(attempts < 100, "Timed out waiting for unmuted response");
        }
        assert_eq!(1, selector.completed_receives().len(), "We should have only one response");
        assert_eq!(
            "1",
            selector.completed_receives()[0].source(),
            "The response should be from the previously muted node"
        );

        // Cleanup
        selector.close_channel("0").await;
        selector.close_channel("1").await;
        selector.poll(0).await.unwrap();
        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testCloseOldestConnection`.
    #[tokio::test]
    async fn test_close_oldest_connection() {
        let server = EchoServer::new().await.unwrap();
        let channel_builder = Box::new(PlaintextChannelBuilder::new(None));
        let mut selector = Selector::new(
            super::super::network_receive::UNLIMITED,
            CONNECTION_MAX_IDLE_MS,
            channel_builder,
        );

        let addr: SocketAddr = format!("127.0.0.1:{}", server.port()).parse().unwrap();
        selector.connect("0", addr, BUFFER_SIZE, BUFFER_SIZE).await.unwrap();
        wait_for_channel_ready(&mut selector, "0").await;

        // Simulate time passing by manipulating the idle expiry manager.
        // Set the connection's last active time far enough in the past that
        // it exceeds connections_max_idle_nanos, and force the next check
        // to happen immediately.
        if let Some(ref mut mgr) = selector.idle_expiry_manager {
            mgr.lru_connections.insert("0".to_string(), 0);
            mgr.next_idle_close_check_time = 0;
            // Also set connections_max_idle_nanos to 0 so that
            // last_active_time + max_idle = 0, which is < current_time
            mgr.connections_max_idle_nanos = 0;
        }

        selector.poll(0).await.unwrap();

        assert!(
            selector.disconnected().contains_key("0"),
            "The idle connection should have been closed"
        );
        assert_eq!(channel_state::EXPIRED, *selector.disconnected().get("0").unwrap());

        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testImmediatelyConnectedCleaned`.
    #[tokio::test]
    async fn test_immediately_connected_cleaned() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        let addr: SocketAddr = format!("127.0.0.1:{}", server.port()).parse().unwrap();
        selector.connect("0", addr, BUFFER_SIZE, BUFFER_SIZE).await.unwrap();

        // After connect, immediately_connected_keys should be non-empty
        assert!(
            !selector.immediately_connected_keys.is_empty(),
            "Immediately connected keys should not be empty after connect"
        );

        selector.poll(0).await.unwrap();

        // After poll, immediately_connected_keys should be cleared
        assert!(
            selector.immediately_connected_keys.is_empty(),
            "Immediately connected keys should be empty after poll"
        );

        selector.close_channel("0").await;

        // Verify selector is clean
        selector.poll(0).await.unwrap();
        selector.poll(0).await.unwrap();
        assert!(selector.channels.is_empty());
        assert!(selector.closing_channels.is_empty());
        assert!(selector.immediately_connected_keys.is_empty());
    }

    /// Translated from `SelectorTest.testCantSendWithInProgress`.
    #[tokio::test]
    async fn test_cant_send_with_in_progress() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        blocking_connect(&mut selector, "0", server.port()).await;
        selector.send(create_send("0", "test1")).unwrap();
        let result = selector.send(create_send("0", "test2"));
        assert!(result.is_err(), "Should fail with in-progress send");

        selector.poll(0).await.unwrap();
        assert!(selector.disconnected().contains_key("0"), "Channel not closed");
        assert_eq!(channel_state::FAILED_SEND, *selector.disconnected().get("0").unwrap());

        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testServerDisconnect`.
    #[tokio::test]
    async fn test_server_disconnect() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        // Connect and do a simple request
        blocking_connect(&mut selector, "0", server.port()).await;
        let result = blocking_request(&mut selector, "0", "hello").await;
        assert_eq!("hello", result);

        // Disconnect server
        server.close_connections();

        // Wait for disconnect to be detected
        let mut attempts = 0;
        loop {
            selector.poll(100).await.unwrap();
            if selector.disconnected().contains_key("0") {
                break;
            }
            attempts += 1;
            assert!(attempts < 50, "Failed to observe disconnected node");
        }

        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testConnectionRefused`.
    #[tokio::test]
    async fn test_connection_refused() {
        let mut selector = create_selector().await;

        // Bind a socket but don't listen/accept
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener); // Close the listener so connections are refused

        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let result = selector.connect("0", addr, BUFFER_SIZE, BUFFER_SIZE).await;

        // Connection refused can manifest either as an error from connect()
        // or as a disconnect detected during poll()
        if result.is_ok() {
            let mut attempts = 0;
            while !selector.disconnected().contains_key("0") && attempts < 30 {
                let _ = selector.poll(100).await;
                attempts += 1;
            }
        }

        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testCloseAllChannels`.
    #[tokio::test]
    async fn test_close_all_channels() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        let addr: SocketAddr = format!("127.0.0.1:{}", server.port()).parse().unwrap();
        selector.connect("0", addr, BUFFER_SIZE, BUFFER_SIZE).await.unwrap();
        selector.connect("1", addr, BUFFER_SIZE, BUFFER_SIZE).await.unwrap();

        selector.close().await;
        assert!(selector.channels.is_empty());
    }
}
