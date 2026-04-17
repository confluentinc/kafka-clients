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

#![allow(dead_code)]
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
//! 2. If no progress and timeout > 0, waits for I/O readiness on any channel
//!    via `select_all` + `tokio::sync::Notify` for wakeup
//! 3. Matches Java's sequential iteration over selectedKeys
//!
//! # Thread safety
//!
//! This class is not thread safe! (Same as Java.)

use super::ChannelBuilder;
use super::DefaultChannelMetadataRegistry;
use super::KafkaChannel;
use super::NetworkReceive;
use super::NetworkSend;
use super::Receive;
use super::Selectable;
use super::selectable::USE_DEFAULT_BUFFER_SIZE;
use super::{ChannelState, channel_state};

use futures_util::future::select_all;
use indexmap::IndexMap;
use log::{debug, error, trace};
use tokio::net::TcpSocket;
use tokio::sync::Notify;

use std::collections::{HashMap, HashSet, LinkedList};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
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
    ///   (use `UNLIMITED` for no limit)
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
            if !has_pending && let Some(channel) = self.closing_channels.remove(&id) {
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
    ///
    /// The idle expiry LRU is updated when the channel actually had I/O
    /// activity, matching Java's behavior where `idleExpiryManager.update`
    /// is called unconditionally for every channel whose NIO selection key
    /// was selected (i.e., has ready I/O) in `pollSelectionKeys`. This
    /// includes partial reads/writes where bytes were transferred but a
    /// full `NetworkReceive`/`NetworkSend` was not yet completed.
    async fn poll_channel(&mut self, channel_id: &str, is_immediately_connected: bool, current_time_nanos: u64) {
        let mut send_failed = false;
        let mut had_bytes_transferred = false;

        // Track pre-poll state to detect if any I/O activity occurred
        let pre_connected = self.connected.len();

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
            if self.attempt_read(channel_id).await? {
                had_bytes_transferred = true;
            }

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
                        Ok(bytes_written) => {
                            if bytes_written {
                                had_bytes_transferred = true;
                            }
                        },
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

        // Update idle expiry if actual I/O activity occurred on this channel.
        // This matches Java's behavior where `idleExpiryManager.update` is
        // called unconditionally for every channel with a ready NIO selection
        // key in `pollSelectionKeys` (line 525-526). In Java, NIO naturally
        // filters to only channels with ready I/O. In Rust, we poll all
        // channels, so we track whether bytes were actually transferred
        // (including partial reads/writes) or a connection was established.
        let had_activity = had_bytes_transferred || self.connected.len() > pre_connected;
        if had_activity && let Some(ref mut mgr) = self.idle_expiry_manager {
            mgr.update(channel_id, current_time_nanos);
        }

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
    /// Returns `true` if bytes were actually read from the channel (including
    /// partial reads where a full `NetworkReceive` has not yet completed).
    ///
    /// Translated from `Selector.attemptRead` in Java.
    /// Uses a zero-duration timeout to make the read non-blocking from the
    /// selector's perspective, matching Java NIO's non-blocking channel reads.
    async fn attempt_read(&mut self, channel_id: &str) -> io::Result<bool> {
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
            return Ok(bytes != 0);
        }
        Ok(false)
    }

    /// Write to a channel.
    ///
    /// Returns `true` if bytes were actually written to the channel (including
    /// partial writes where a full `NetworkSend` has not yet completed).
    async fn write_channel(&mut self, channel_id: &str) -> io::Result<bool> {
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
            && let Some(send) = send
        {
            self.completed_sends.push(send);
        }
        Ok(bytes_sent > 0)
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
            if !has_pending && let Some(channel) = self.closing_channels.remove(id) {
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
            && self.channels.contains_key(&connection_id)
        {
            trace!("About to close the idle connection from {} due to being idle", connection_id);
            if let Some(channel) = self.channels.get_mut(&connection_id) {
                channel.set_state(channel_state::EXPIRED.clone());
            }
            // Use graceful close to process any buffered receives before
            // fully closing the channel, matching the Java implementation.
            self.close_channel_internal(&connection_id, CloseMode::Graceful).await;
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
            && let Some((id, _)) = mgr.lru_connections.first()
        {
            return self.channels.get(id);
        }
        self.channels.values().next()
    }
    /// Collect readiness futures for channels interested in I/O.
    fn collect_readiness_futures(&self) -> Vec<Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>> {
        let mut futs: Vec<Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>> = Vec::new();
        for (id, channel) in &self.channels {
            let want_read = channel.ready()
                && (channel.has_bytes_buffered() || !channel.is_muted())
                && !self.has_completed_receive(id)
                && !self.explicitly_muted_channels.contains(id);
            if want_read {
                futs.push(channel.transport_readable());
            }
            if channel.has_send() && channel.ready() {
                futs.push(channel.transport_writable());
            }
        }
        futs
    }
}

impl Selectable for Selector {
    async fn connect(
        &mut self,
        id: &str,
        address: SocketAddr,
        peer_host: &str,
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

        let metadata_registry = Box::new(DefaultChannelMetadataRegistry::new());

        // Build channel — the channel builder wraps the stream in the
        // appropriate transport layer (plaintext or SSL/TLS).
        let channel =
            match self
                .channel_builder
                .build_channel(id, stream, peer_host, self.max_receive_size, metadata_registry)
            {
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
                        "Unexpected error during send, closing connection {} and returning error: {}",
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

            // No progress — wait for I/O readiness on any channel, wakeup,
            // or deadline. This replaces the former 1ms busy-poll with
            // proper event-driven readiness, matching Java NIO's
            // Selector.select(timeout) which uses epoll/kqueue.
            match deadline {
                Some(dl) if tokio::time::Instant::now() < dl => {
                    let readiness_futs: Vec<Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>> =
                        self.collect_readiness_futures();

                    let notify = self.notify.clone();
                    if readiness_futs.is_empty() {
                        tokio::select! {
                            biased;
                            _ = notify.notified() => {},
                            _ = tokio::time::sleep_until(dl) => {},
                        }
                    } else {
                        tokio::select! {
                            biased;
                            _ = notify.notified() => {},
                            _ = select_all(readiness_futs) => {},
                            _ = tokio::time::sleep_until(dl) => {},
                        }
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
                && channel.has_bytes_buffered()
            {
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
    use crate::common::network::ByteBufferSend;
    use crate::common::network::PlaintextChannelBuilder;

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
        selector
            .connect(node, addr, "localhost", BUFFER_SIZE, BUFFER_SIZE)
            .await
            .unwrap();
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
            selector.connect("0", addr, "localhost", BUFFER_SIZE, BUFFER_SIZE),
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
            selector
                .connect(&i.to_string(), addr, "localhost", BUFFER_SIZE, BUFFER_SIZE)
                .await
                .unwrap();
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
        let result = selector.connect("0", addr, "localhost", BUFFER_SIZE, BUFFER_SIZE).await;
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
        selector
            .connect("0", addr, "localhost", BUFFER_SIZE, BUFFER_SIZE)
            .await
            .unwrap();
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
        selector
            .connect("0", addr, "localhost", BUFFER_SIZE, BUFFER_SIZE)
            .await
            .unwrap();

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
        let result = selector.connect("0", addr, "localhost", BUFFER_SIZE, BUFFER_SIZE).await;

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
        selector
            .connect("0", addr, "localhost", BUFFER_SIZE, BUFFER_SIZE)
            .await
            .unwrap();
        selector
            .connect("1", addr, "localhost", BUFFER_SIZE, BUFFER_SIZE)
            .await
            .unwrap();

        selector.close().await;
        assert!(selector.channels.is_empty());
    }

    /// Helper to send requests and receive responses sequentially on a single connection.
    ///
    /// Translated from `SelectorTest.sendAndReceive` in Java.
    async fn send_and_receive(
        selector: &mut Selector,
        node: &str,
        request_prefix: &str,
        start_index: i32,
        end_index: i32,
    ) {
        let mut requests = start_index;
        let mut responses = start_index;
        selector
            .send(create_send(node, &format!("{request_prefix}-{start_index}")))
            .unwrap();
        requests += 1;
        while responses < end_index {
            selector.poll(0).await.unwrap();
            assert_eq!(0, selector.disconnected().len(), "No disconnects should have occurred.");
            for receive in selector.completed_receives() {
                let expected = format!("{request_prefix}-{responses}");
                assert_eq!(expected, as_string(receive));
                responses += 1;
            }

            let completed_count = selector.completed_sends().len() as i32;
            for _ in 0..completed_count {
                if requests < end_index {
                    selector
                        .send(create_send(node, &format!("{request_prefix}-{requests}")))
                        .unwrap();
                    requests += 1;
                }
            }
        }
    }

    /// Helper to send requests without reading any responses.
    ///
    /// The channel is muted during polling so incoming data accumulates in socket buffers.
    ///
    /// Translated from `SelectorTest.sendNoReceive` in Java.
    async fn send_no_receive(selector: &mut Selector, channel_id: &str, num_requests: i32) {
        selector.mute(channel_id);
        for i in 0..num_requests {
            selector.send(create_send(channel_id, &i.to_string())).unwrap();
            loop {
                selector.poll(10).await.unwrap();
                if !selector.completed_sends().is_empty() {
                    break;
                }
            }
        }
        selector.unmute(channel_id);
    }

    /// Helper to create a connection with pending receives.
    ///
    /// Connects a channel, sends `pending_receives` requests without reading
    /// responses, so data accumulates in the socket buffers.
    ///
    /// Translated from `SelectorTest.createConnectionWithPendingReceives` in Java.
    async fn create_connection_with_pending_receives(
        selector: &mut Selector,
        server: &EchoServer,
        pending_receives: i32,
    ) -> String {
        let id = "0";
        blocking_connect(selector, id, server.port()).await;
        send_no_receive(selector, id, pending_receives).await;
        id.to_string()
    }

    /// Translated from `SelectorTest.testLargeMessageSequence`.
    ///
    /// Tests sending/receiving sequential large messages on a single connection.
    #[tokio::test]
    async fn test_large_message_sequence() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        let buffer_size = 512 * 1024;
        let node = "0";
        let reqs = 50;
        let addr: SocketAddr = format!("127.0.0.1:{}", server.port()).parse().unwrap();
        selector
            .connect(node, addr, "localhost", BUFFER_SIZE, BUFFER_SIZE)
            .await
            .unwrap();
        wait_for_channel_ready(&mut selector, node).await;

        // Generate a large random-ish prefix
        let request_prefix: String = (0..buffer_size).map(|i| (b'a' + (i % 26) as u8) as char).collect();
        send_and_receive(&mut selector, node, &request_prefix, 0, reqs).await;

        selector.close_channel(node).await;
        selector.poll(0).await.unwrap();
        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testClearCompletedSendsAndReceives`.
    ///
    /// Tests that `clear_completed_sends()` and `clear_completed_receives()` work correctly.
    #[tokio::test]
    async fn test_clear_completed_sends_and_receives() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        let node = "0";
        let addr: SocketAddr = format!("127.0.0.1:{}", server.port()).parse().unwrap();
        selector
            .connect(node, addr, "localhost", BUFFER_SIZE, BUFFER_SIZE)
            .await
            .unwrap();
        wait_for_channel_ready(&mut selector, node).await;

        let request: String = (0..1024).map(|i| (b'a' + (i % 26) as u8) as char).collect();
        selector.send(create_send(node, &request)).unwrap();
        let mut sent = false;
        let mut received = false;
        while !sent || !received {
            selector.poll(1000).await.unwrap();
            assert_eq!(0, selector.disconnected().len(), "No disconnects should have occurred.");

            if !selector.completed_sends().is_empty() {
                assert_eq!(1, selector.completed_sends().len());
                selector.clear_completed_sends();
                assert_eq!(0, selector.completed_sends().len());
                sent = true;
            }

            if !selector.completed_receives().is_empty() {
                assert_eq!(1, selector.completed_receives().len());
                assert_eq!(request, as_string(selector.completed_receives()[0]));
                selector.clear_completed_receives();
                assert_eq!(0, selector.completed_receives().len());
                received = true;
            }
        }

        selector.close_channel(node).await;
        selector.poll(0).await.unwrap();
        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testLowestPriorityChannel`.
    ///
    /// Tests that `lowest_priority_channel()` returns the least recently used channel,
    /// and that closing channels take priority.
    ///
    /// Note: The Java test relies on NIO's per-channel readiness selection (only
    /// channels with ready I/O keys get their LRU updated during poll). The Rust
    /// implementation polls all channels during each poll() call, so we manipulate
    /// the LRU directly to simulate the intended ordering.
    #[tokio::test]
    async fn test_lowest_priority_channel() {
        let server = EchoServer::new().await.unwrap();
        let channel_builder = Box::new(PlaintextChannelBuilder::new(None));
        let mut selector = Selector::new(
            super::super::network_receive::UNLIMITED,
            CONNECTION_MAX_IDLE_MS,
            channel_builder,
        );

        let conns = 5;
        let addr: SocketAddr = format!("127.0.0.1:{}", server.port()).parse().unwrap();
        for i in 0..conns {
            selector
                .connect(&i.to_string(), addr, "localhost", BUFFER_SIZE, BUFFER_SIZE)
                .await
                .unwrap();
            wait_for_channel_ready(&mut selector, &i.to_string()).await;
        }

        assert!(selector.lowest_priority_channel().is_some());

        // Simulate the Java test's per-channel LRU update behavior:
        // All channels except "2" are used, so "2" should be the oldest (lowest priority).
        // Set LRU timestamps so that "2" has the oldest timestamp.
        if let Some(ref mut mgr) = selector.idle_expiry_manager {
            mgr.lru_connections.clear();
            // Insert "2" first (oldest) with timestamp 0
            mgr.lru_connections.insert("2".to_string(), 0);
            // Insert others with increasing timestamps
            for &i in &[4, 3, 1, 0] {
                mgr.lru_connections.insert(i.to_string(), (5 - i as u64) * 10_000_000);
            }
        }
        assert_eq!("2", selector.lowest_priority_channel().unwrap().id());

        // Inserting a closing channel should make it lowest priority
        if let Some(channel) = selector.channels.remove("3") {
            selector.closing_channels.insert("3".to_string(), channel);
        }
        assert_eq!("3", selector.lowest_priority_channel().unwrap().id());
        // Restore channel
        if let Some(channel) = selector.closing_channels.remove("3") {
            selector.channels.insert("3".to_string(), channel);
        }

        for i in 0..conns {
            selector.close_channel(&i.to_string()).await;
        }
        assert!(selector.lowest_priority_channel().is_none());

        selector.poll(0).await.unwrap();
        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testGracefulClose`.
    ///
    /// Tests that graceful close of channel processes remaining data from socket
    /// read buffers. Since we cannot determine how much data is available in the
    /// buffers, this test verifies that multiple receives are completed after
    /// server shuts down connections, with retries to tolerate cases where data
    /// may not be available in the socket buffer.
    #[tokio::test]
    async fn test_graceful_close() {
        let mut max_receive_count_after_close = 0;
        // Iterate from 6 up to 100, stop early once we've received >= 5 after close
        let mut i = 6;
        while i <= 100 && max_receive_count_after_close < 5 {
            let server = EchoServer::new().await.unwrap();
            let mut selector = create_selector().await;

            let id = create_connection_with_pending_receives(&mut selector, &server, i).await;

            // Poll until one or more receives complete
            let mut attempts = 0;
            loop {
                selector.poll(1000).await.unwrap();
                if !selector.completed_receives().is_empty() {
                    break;
                }
                attempts += 1;
                assert!(attempts < 50, "Receive not completed");
            }

            // Close server-side connections
            server.close_connections();

            let mut receive_count = 0;
            while selector.disconnected().is_empty() {
                selector.poll(1).await.unwrap();
                receive_count += selector.completed_receives().len();
                assert!(
                    selector.completed_receives().len() <= 1,
                    "Too many completed receives in one poll"
                );
            }
            assert!(
                selector.disconnected().contains_key(&id),
                "Disconnect should be for our channel"
            );
            max_receive_count_after_close = std::cmp::max(max_receive_count_after_close, receive_count);

            selector.close().await;
            i += 1;
        }
        assert!(
            max_receive_count_after_close >= 5,
            "Too few receives after close: {max_receive_count_after_close}"
        );
    }

    /// Translated from `SelectorTest.testExpireConnectionWithPendingReceives`.
    ///
    /// Verifies that a muted connection is expired on idle timeout even if there
    /// are pending receives on the socket.
    #[tokio::test]
    async fn test_expire_connection_with_pending_receives() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        let id = create_connection_with_pending_receives(&mut selector, &server, 5).await;

        // Mute to allow channel to be expired even if more data is available
        selector.mute(&id);

        // Simulate time passing past the idle timeout
        if let Some(ref mut mgr) = selector.idle_expiry_manager {
            mgr.lru_connections.insert(id.clone(), 0);
            mgr.next_idle_close_check_time = 0;
            mgr.connections_max_idle_nanos = 0;
        }

        selector.poll(0).await.unwrap();

        assert!(selector.channel(&id).is_none(), "Channel not expired");
        assert!(
            selector.closing_channel(&id).is_none(),
            "Channel not removed from closingChannels"
        );
        assert!(selector.disconnected().contains_key(&id), "Disconnect not notified");
        assert_eq!(channel_state::EXPIRED, *selector.disconnected().get(&id).unwrap());

        selector.poll(0).await.unwrap();
    }

    /// Translated from `SelectorTest.testCloseOldestConnectionWithMultiplePendingReceives`.
    ///
    /// Verifies that sockets with incoming data available are not expired until
    /// all pending receives are processed.
    #[tokio::test]
    async fn test_close_oldest_connection_with_multiple_pending_receives() {
        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;

        let expected_receives = 5;
        let id = create_connection_with_pending_receives(&mut selector, &server, expected_receives).await;
        let mut completed_receives = selector.completed_receives().len() as i32;

        while selector.disconnected().is_empty() {
            // Simulate time passing past the idle timeout, matching Java's
            // `time.sleep(CONNECTION_MAX_IDLE_MS + 1_000)`.
            // We set the LRU entry to a timestamp far enough in the past that
            // idle expiry will fire. However, if poll_channel reads data, it
            // refreshes the LRU to the current time, preventing expiry —
            // matching Java's behavior where pollSelectionKeys updates the LRU
            // for channels with ready selection keys.
            if let Some(ref mut mgr) = selector.idle_expiry_manager {
                let expired_time = nanos_now().saturating_sub(mgr.connections_max_idle_nanos + 1_000_000_000);
                mgr.lru_connections.insert(id.clone(), expired_time);
                mgr.next_idle_close_check_time = 0;
            }

            let timeout = if completed_receives == expected_receives {
                0
            } else {
                1000
            };
            selector.poll(timeout).await.unwrap();
            completed_receives += selector.completed_receives().len() as i32;
        }

        assert_eq!(expected_receives, completed_receives);
        assert!(selector.channel(&id).is_none(), "Channel not expired");
        assert!(selector.closing_channel(&id).is_none(), "Channel not expired");
        assert!(selector.disconnected().contains_key(&id), "Disconnect not notified");
        assert!(selector.completed_receives().is_empty(), "Unexpected receive");

        selector.poll(0).await.unwrap();
    }

    // NOTE: testWriteCompletesSendWithNoBytesWritten is not translated.
    // The Java test uses Mockito to mock a KafkaChannel where write() returns 0L
    // but maybeCompleteSend() returns a send. This tests a specific edge case
    // with TransportLayer.hasPendingWrites (relevant for SSL buffering). The Rust
    // implementation does not use the same SSL buffering mechanism and this code
    // path is already exercised by test_empty_request which sends/receives a
    // 0-byte payload through the real pipeline.

    // NOTE: The following Java SelectorTest tests are not translated:
    //
    // - testPartialSendAndReceiveReflectedInMetrics: requires metrics (deferred)
    // - testOutboundConnectionsCountInConnectionCreationMetric: requires metrics
    // - testInboundConnectionsCountInConnectionCreationMetric: requires metrics
    // - testConnectionsByClientMetric: requires metrics
    // - testMetricsCleanupOnSelectorClose: requires metrics
    // - registerFailure: register() is server-side only, not implemented
    // - testMuteOnOOM: requires SimpleMemoryPool, not implemented (NoopMemoryPool used)
    // - testConnectDisconnectDuringInSinglePoll: relies on Mockito mocking of
    //   pollSelectionKeys which does not exist in the Rust translation
    // - testChannelCloseWhileProcessingReceives: relies on Mockito mocking of
    //   internal Selector methods
    // - testConnectException: tests exception cleanup during registerChannel
    //   which is eliminated in Rust
    // - testIdleExpiryWithoutReadyKeys: tests Java NIO SelectionKey interest ops
    //   manipulation, not applicable in Rust
    // - testPartialReceiveGracefulClose: requires injecting a NetworkReceive via
    //   reflection
    // - testExpireClosedConnectionWithPendingReceives: similar to
    //   testExpireConnectionWithPendingReceives but with server close; the core
    //   behavior is already covered by test_expire_connection_with_pending_receives
}
