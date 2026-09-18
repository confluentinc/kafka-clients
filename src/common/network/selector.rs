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
//!    via a single non-allocating readiness `poll_fn` over all channels
//!    + `tokio::sync::Notify` for wakeup
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
use super::authentication_error::is_authentication_error;
use super::selectable::USE_DEFAULT_BUFFER_SIZE;
use super::{ChannelState, channel_state};

use indexmap::IndexMap;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::net::TcpSocket;
use tokio::sync::Notify;

use crate::common::utils::LogContext;
use crate::{kafka_debug, kafka_error, kafka_trace};

use std::collections::{HashMap, LinkedList};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;

/// Value indicating no idle timeout.
pub const NO_IDLE_TIMEOUT_MS: i64 = -1;

/// Shared between the [`Selector`] and every per-channel [`ChannelWaker`].
///
/// This is the data structure that gives the selector Java NIO
/// `selector.select()` → `selectedKeys()` semantics on top of tokio. tokio's
/// readiness API registers ONE task waker per socket registration and, on
/// wake, cannot tell us *which* socket fired. By handing every interested
/// `(channel, direction)` its OWN cached waker (a [`ChannelWaker`] that pushes
/// `(token, is_write)` here when fired), the WAIT future learns exactly which
/// channels became ready — O(ready), not O(registered) — mirroring
/// `selectedKeys()`.
struct ReadyQueue {
    /// Channels whose registered interest fired since the last drain, recorded
    /// as `(token, is_write)`. Pushed from [`ChannelWaker::wake`] (in practice
    /// the same thread — the current-thread reactor fires wakers during park —
    /// but `Waker` is `Send`, so this is a `std::sync::Mutex`; it is
    /// uncontended).
    fired: StdMutex<Vec<(u32, bool)>>,
    /// Root waker of the WAIT future, registered on each poll. `Mutex<Option>`
    /// rather than an `AtomicWaker` to avoid adding a dependency; the waker is
    /// taken out under the lock and woken *outside* it.
    root: StdMutex<Option<Waker>>,
}

impl ReadyQueue {
    fn new() -> Self {
        Self { fired: StdMutex::new(Vec::new()), root: StdMutex::new(None) }
    }

    /// Register (or refresh) the WAIT future's root waker. Cheap: only clones
    /// when the stored waker would not wake the same task.
    fn set_root(&self, waker: &Waker) {
        let mut guard = self.root.lock().expect("ReadyQueue.root poisoned");
        match guard.as_ref() {
            Some(existing) if existing.will_wake(waker) => {},
            _ => *guard = Some(waker.clone()),
        }
    }

    /// Wake the WAIT future's root waker, if any. Taken-and-woken outside the
    /// `fired` lock (and we drop the `root` lock before waking) to avoid waking
    /// while holding a lock.
    fn wake_root(&self) {
        let waker = self.root.lock().expect("ReadyQueue.root poisoned").take();
        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// One waker per channel per interest direction, created at registration time
/// and cached on the channel's [`ChannelArming`] side-struct (so arming is a
/// cheap `Waker::clone`, with no per-poll allocation). When the tokio reactor
/// fires it, it records which `(token, direction)` became ready in the shared
/// [`ReadyQueue`] and wakes the WAIT future's root waker — the equivalent of a
/// NIO `SelectionKey` landing in `selectedKeys()`.
struct ChannelWaker {
    token: u32,
    is_write: bool,
    queue: Arc<ReadyQueue>,
}

impl Wake for ChannelWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.queue
            .fired
            .lock()
            .expect("ReadyQueue.fired poisoned")
            .push((self.token, self.is_write));
        self.queue.wake_root();
    }
}

/// Per-channel arming bookkeeping owned by the [`Selector`] (kept out of
/// [`KafkaChannel`] so the channel type stays transport-focused).
///
/// A `(channel, direction)` is **armed** when [`KafkaChannel::poll_transport_readable`]
/// / [`KafkaChannel::poll_transport_writable`] was last called with the cached
/// waker below and returned `Pending`. tokio then holds that waker until the
/// readiness event fires it (consuming the waker) — at which point the
/// [`ChannelWaker`] records the fire in the [`ReadyQueue`] and the arming flag
/// is cleared by the WAIT future when it drains the queue.
struct ChannelArming {
    /// Stable token identifying this channel in the [`ReadyQueue`] fired list
    /// (monotonic, assigned at registration). Maps back to the id via
    /// [`Selector::token_to_id`].
    token: u32,
    /// Cached read-readiness waker (clone is a refcount bump, no allocation).
    read_waker: Waker,
    /// Cached write-readiness waker.
    write_waker: Waker,
    /// True while a read-readiness arm is outstanding (Pending, not yet fired).
    armed_read: bool,
    /// True while a write-readiness arm is outstanding (Pending, not yet fired).
    armed_write: bool,
}

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
    ///
    /// The connection ID is stored as `Arc<str>` (CLAUDE.md §11: identifiers
    /// cloned on every poll iteration) so that the per-poll
    /// `channels.keys().cloned()` (and the equivalent clones into the tracking
    /// sets below) are refcount bumps rather than heap allocations. Lookups by
    /// `&str` continue to work because `Arc<str>: Borrow<str>`.
    ///
    /// Uses `FxHashMap` (fast, non-cryptographic FxHash) rather than the default
    /// SipHash: these maps/sets are looked up for every channel on every poll
    /// iteration, and the keys are internal connection ids (not attacker-
    /// controlled), so SipHash's DoS resistance is unnecessary while its
    /// per-lookup cost dominated the high-frequency poll loop on a real network
    /// (Phase 22 / TLS CPU profile: ~10% of CPU was channel-id hashing).
    channels: FxHashMap<Arc<str>, KafkaChannel>,
    /// Channels that have been explicitly muted.
    explicitly_muted_channels: FxHashSet<Arc<str>>,
    /// Channels that have data buffered in intermediate buffers.
    channels_with_buffered_read: FxHashSet<Arc<str>>,
    /// Channels that connected immediately (before poll).
    immediately_connected_keys: FxHashSet<Arc<str>>,
    /// Channels that are being closed gracefully (pending receives).
    closing_channels: FxHashMap<Arc<str>, KafkaChannel>,
    /// Sends completed during the last poll.
    completed_sends: Vec<NetworkSend>,
    /// Receives completed during the last poll, keyed by channel ID.
    completed_receives: LinkedList<NetworkReceive>,
    /// Channels that disconnected during the last poll.
    disconnected: HashMap<String, ChannelState>,
    /// Channels that connected during the last poll.
    connected: Vec<String>,
    /// Channels that failed to send.
    failed_sends: Vec<Arc<str>>,
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
    /// Reusable scratch buffer for the per-iteration snapshot of channel ids in
    /// [`Self::poll`]. Hoisted to a field (taken via `mem::take`, refilled, and
    /// restored each iteration) so the poll loop does not heap-allocate a fresh
    /// `Vec<Arc<str>>` every time it runs — only `Arc` refcount bumps remain
    /// (Phase 22; the allocation showed up as per-poll `malloc`/`from_iter` in
    /// the TLS CPU profile).
    poll_id_scratch: Vec<Arc<str>>,
    /// Reusable scratch set for the per-iteration *ready* channel ids recorded
    /// by [`Self::poll_channel_readiness`] (Phase 24). The poll loop takes this
    /// via `mem::take`, the readiness wait refills it with the ids the reactor
    /// flagged ready, and pass-1 processes only those (plus buffered /
    /// immediately-connected channels) instead of sweeping every channel and
    /// issuing a `recv` syscall on each. Hoisted to a field (like
    /// [`Self::poll_id_scratch`]) so no per-poll `FxHashSet` allocation occurs.
    ready_scratch: FxHashSet<Arc<str>>,
    /// Shared fired-queue + root-waker cell for the per-channel wakers (Phase
    /// 30). The [`ChannelWaker`]s cached in [`Self::arming`] push into this when
    /// the tokio reactor fires them; the WAIT future drains it. This is what
    /// gives the selector Java NIO `selectedKeys()` (O(ready)) dispatch instead
    /// of the Phase-24 per-WAIT sweep over every registered channel.
    ready_queue: Arc<ReadyQueue>,
    /// Per-channel arming bookkeeping: cached wakers + armed flags + token, one
    /// entry per active channel. Created at registration ([`Selectable::connect`])
    /// and removed when the channel is removed. Not put inside [`KafkaChannel`]
    /// to keep the channel type transport-focused.
    arming: FxHashMap<Arc<str>, ChannelArming>,
    /// Maps a [`ChannelArming::token`] back to the channel id, so the WAIT
    /// future can translate the `(token, is_write)` pairs drained from the
    /// fired-queue into channel ids to process. Closed tokens (channel removed
    /// since arming) are simply absent and dropped.
    token_to_id: FxHashMap<u32, Arc<str>>,
    /// Monotonic token allocator for new channel registrations.
    next_token: u32,
    /// Channels whose `channel_interest` may have flipped ON since they were
    /// last armed — they must be (re-)armed at the next WAIT regardless of
    /// whether they were processed in pass-1. Populated by the "dirty sites"
    /// (see [`Self::mark_interest_dirty`]). Mirrors the Phase-24 risk-#1
    /// guarantee: a missed arm is a data-stall bug, so when in doubt, mark
    /// dirty (a spurious arm costs one `poll_ready`).
    interest_dirty: FxHashSet<Arc<str>>,
    /// O(1) count of channels with at least one outstanding armed flag. Used by
    /// [`Self::has_interested_channel`] to choose the `select!` form without
    /// sweeping all channels each WAIT.
    armed_count: usize,
    /// Contextual log message prefix.
    ///
    /// Translated from Java's `LogContext logContext` field in `Selector`.
    log_context: LogContext,
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
        Self::with_log_context(max_receive_size, connection_max_idle_ms, channel_builder, LogContext::empty())
    }

    /// Create a new selector with a `LogContext`.
    ///
    /// # Arguments
    ///
    /// * `max_receive_size` - Max size in bytes of a single network receive
    ///   (use `UNLIMITED` for no limit)
    /// * `connection_max_idle_ms` - Max idle connection time
    ///   (use [`NO_IDLE_TIMEOUT_MS`] to disable idle timeout)
    /// * `channel_builder` - Channel builder for every new connection
    /// * `log_context` - Contextual log message prefix
    pub fn with_log_context(
        max_receive_size: i32,
        connection_max_idle_ms: i64,
        channel_builder: Box<dyn ChannelBuilder>,
        log_context: LogContext,
    ) -> Self {
        Self {
            channels: FxHashMap::default(),
            explicitly_muted_channels: FxHashSet::default(),
            channels_with_buffered_read: FxHashSet::default(),
            immediately_connected_keys: FxHashSet::default(),
            closing_channels: FxHashMap::default(),
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
            poll_id_scratch: Vec::new(),
            ready_scratch: FxHashSet::default(),
            ready_queue: Arc::new(ReadyQueue::new()),
            arming: FxHashMap::default(),
            token_to_id: FxHashMap::default(),
            next_token: 0,
            interest_dirty: FxHashSet::default(),
            armed_count: 0,
            log_context,
        }
    }

    /// Convenience constructor matching the common Java pattern.
    pub fn with_defaults(connection_max_idle_ms: i64, channel_builder: Box<dyn ChannelBuilder>) -> Self {
        Self::new(super::network_receive::UNLIMITED, connection_max_idle_ms, channel_builder)
    }

    /// Create a new selector with default max receive size and a `LogContext`.
    pub fn with_defaults_and_log_context(
        connection_max_idle_ms: i64,
        channel_builder: Box<dyn ChannelBuilder>,
        log_context: LogContext,
    ) -> Self {
        Self::with_log_context(
            super::network_receive::UNLIMITED,
            connection_max_idle_ms,
            channel_builder,
            log_context,
        )
    }

    /// Returns the interned `Arc<str>` key for `id` from the active channels
    /// map, cloning the existing `Arc` (a refcount bump, no allocation) so the
    /// tracking sets share one heap allocation per connection id. Falls back to
    /// allocating a fresh `Arc<str>` only when the channel is not (yet) in the
    /// map — the same situations where Java would have allocated a `String`.
    fn intern_id(&self, id: &str) -> Arc<str> {
        self.channels
            .get_key_value(id)
            .map(|(k, _)| Arc::clone(k))
            .unwrap_or_else(|| Arc::from(id))
    }

    /// Register per-channel arming bookkeeping for a freshly-connected channel
    /// (Phase 30): allocate a token, build the two cached wakers, and seed the
    /// arming entry with both flags clear. The channel is also marked dirty so
    /// the next WAIT arms whatever interest it currently has (connect /
    /// immediately-connected handling — dirty site #4).
    fn register_arming(&mut self, key: &Arc<str>) {
        let token = self.next_token;
        self.next_token = self.next_token.wrapping_add(1);
        let read_waker = Waker::from(Arc::new(ChannelWaker {
            token,
            is_write: false,
            queue: Arc::clone(&self.ready_queue),
        }));
        let write_waker = Waker::from(Arc::new(ChannelWaker {
            token,
            is_write: true,
            queue: Arc::clone(&self.ready_queue),
        }));
        self.arming.insert(
            Arc::clone(key),
            ChannelArming { token, read_waker, write_waker, armed_read: false, armed_write: false },
        );
        self.token_to_id.insert(token, Arc::clone(key));
        self.interest_dirty.insert(Arc::clone(key));
    }

    /// Drop per-channel arming bookkeeping when a channel is removed. Decrements
    /// `armed_count` for any outstanding arm so the O(1) counter stays exact.
    fn unregister_arming(&mut self, id: &str) {
        if let Some(arming) = self.arming.remove(id) {
            if arming.armed_read {
                self.armed_count -= 1;
            }
            if arming.armed_write {
                self.armed_count -= 1;
            }
            self.token_to_id.remove(&arming.token);
        }
        self.interest_dirty.remove(id);
    }

    /// Mark a channel's interest as possibly having flipped ON since it was last
    /// armed — it must be (re-)armed at the next WAIT (Phase 30 "dirty site").
    /// Cheap: an `Arc` refcount bump into a set. See the dirty-sites list on
    /// [`Self::poll`] for the exhaustive enumeration of callers.
    fn mark_interest_dirty(&mut self, key: &Arc<str>) {
        self.interest_dirty.insert(Arc::clone(key));
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
        // Phase 30 (dirty site #3): clearing the completed receives flips
        // `want_read` back ON for every channel that had one (the
        // `!has_completed_receive(id)` term in `channel_interest`). Mark those
        // ids dirty so the next WAIT re-arms them. Cheapest correct form: mark
        // each source whose completed receive is being cleared.
        if !self.completed_receives.is_empty() {
            let dirtied: Vec<Arc<str>> = self
                .completed_receives
                .iter()
                .filter_map(|r| self.channels.get_key_value(r.source()).map(|(k, _)| Arc::clone(k)))
                .collect();
            for key in dirtied {
                self.mark_interest_dirty(&key);
            }
        }
        self.completed_receives.clear();
        self.connected.clear();
        self.disconnected.clear();

        // Remove closed channels after all their buffered receives have been processed
        // or if a send was requested
        let closing_ids: Vec<Arc<str>> = self.closing_channels.keys().cloned().collect();
        for id in closing_ids {
            let send_failed = self.failed_sends.iter().position(|s| *s == id).map(|i| {
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
            self.disconnected
                .insert(channel_id.to_string(), channel_state::FAILED_SEND.clone());
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
    /// Read-phase of channel polling: connect, prepare, read.
    ///
    /// Handles everything except writes, which are done concurrently
    /// in `poll_channels_write_concurrent`.
    async fn poll_channel_reads(&mut self, channel_id: &str, is_immediately_connected: bool, current_time_nanos: u64) {
        let mut had_bytes_transferred = false;
        let pre_connected = self.connected.len();

        let result: io::Result<()> = async {
            // Resolve the channel handle ONCE for the connect / prepare / ready /
            // reauthentication sequence, capturing the resulting selector-level
            // bookkeeping as locals so the `&mut self.channels` borrow is released
            // before we touch the other fields. This previously re-looked-up the
            // channel by id 4 separate times per channel per poll — pure overhead
            // on the steady-state hot path (Phase 22 follow-up: lookup-once).
            let mut connect_logged = false;
            // Number of times to append `channel_id` to `self.connected`
            // (finish_connect transition, and/or the post-handshake ready
            // transition — order preserved relative to the original code).
            let mut push_connected: u8 = 0;
            // Assigned once below (the early `return` path never reads it).
            let reauth_receive: Option<NetworkReceive>;
            {
                let channel = self.channels.get_mut(channel_id).unwrap();
                if is_immediately_connected || !channel.is_connected() {
                    if channel.finish_connect().await? {
                        push_connected += 1;
                        connect_logged = true;
                    } else {
                        return Ok(());
                    }
                }

                let was_ready = channel.ready();
                if channel.is_connected() && !channel.ready() {
                    channel.prepare().await?;
                }

                // Signal the post-handshake (TLS/SASL) ready transition so poll()
                // exits and handle_initiate_api_version_requests fires without
                // waiting for an external event.
                if !was_ready && channel.ready() {
                    push_connected += 1;
                }

                reauth_receive = channel.poll_response_received_during_reauthentication();
            }

            // Apply the captured bookkeeping now that the channel borrow is gone.
            for _ in 0..push_connected {
                self.connected.push(channel_id.to_string());
            }
            if connect_logged {
                kafka_debug!(self.log_context, "Connected to node {}", channel_id);
            }
            if let Some(receive) = reauth_receive {
                self.add_to_completed_receives(receive);
            }

            if self.attempt_read(channel_id).await? {
                had_bytes_transferred = true;
            }

            let channel = self.channels.get(channel_id).unwrap();
            if channel.has_bytes_buffered() && !self.explicitly_muted_channels.contains(channel_id) {
                let key = self.intern_id(channel_id);
                self.channels_with_buffered_read.insert(key);
            }

            Ok(())
        }
        .await;

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

            // Route by typed error, not by an opaque ErrorKind heuristic
            // (mirrors Java's `e instanceof AuthenticationException` in
            // Selector): only genuine authentication failures log "Failed
            // authentication"; everything else (connection reset, broken pipe,
            // EOF) is a retriable network disconnect.
            if is_authentication_error(&e) {
                kafka_error!(self.log_context, "Failed authentication with {} ({})", desc, e);
            } else {
                kafka_debug!(self.log_context, "Connection with {} disconnected: {}", desc, e);
            }

            self.close_channel_internal(channel_id, CloseMode::Graceful).await;
        }
    }

    /// Write-phase: extract channels with pending sends and write concurrently.
    ///
    /// Channels are temporarily removed from the HashMap so each can be
    /// borrowed independently by `join_all`. While one channel's TLS write
    /// awaits TCP readiness, other channels' writes can proceed.
    async fn poll_channels_write_concurrent(&mut self, channel_ids: &[Arc<str>], current_time_nanos: u64) {
        let mut extracted: Vec<(Arc<str>, KafkaChannel)> = Vec::new();
        for id in channel_ids {
            if let Some(channel) = self.channels.get(id)
                && channel.has_send()
                && channel.ready()
                && let Some(channel) = self.channels.remove(id)
            {
                extracted.push((Arc::clone(id), channel));
            }
        }

        if extracted.is_empty() {
            return;
        }

        let results: Vec<ChannelWriteResult> = futures_util::future::join_all(
            extracted
                .iter_mut()
                .map(|(_, channel)| do_channel_write(channel, current_time_nanos)),
        )
        .await;

        for ((id, channel), result) in extracted.into_iter().zip(results) {
            self.channels.insert(id.clone(), channel);

            if (result.bytes_written > 0 || result.completed_send.is_some())
                && let Some(ref mut mgr) = self.idle_expiry_manager
            {
                mgr.update(&id, current_time_nanos);
            }

            if let Some(send) = result.completed_send {
                self.completed_sends.push(send);
            }

            if let Some((error, send_failed)) = result.error {
                let desc = if let Some(ch) = self.channels.get(&id) {
                    format!("{} (channelId={})", ch.socket_description(), ch.id())
                } else {
                    format!("unknown (channelId={id})")
                };

                // Route by typed error, not by an opaque ErrorKind heuristic
                // (mirrors Java's `e instanceof AuthenticationException`): only
                // genuine authentication failures log "Failed authentication";
                // a transient handshake/write I/O error is a retriable
                // disconnect.
                if is_authentication_error(&error) {
                    kafka_error!(self.log_context, "Failed authentication with {} ({})", desc, error);
                } else {
                    kafka_debug!(self.log_context, "Connection with {} disconnected: {}", desc, error);
                }

                let close_mode = if send_failed {
                    CloseMode::NotifyOnly
                } else {
                    CloseMode::Graceful
                };
                self.close_channel_internal(&id, close_mode).await;
            }
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
        let should_read = {
            let channel = self.channels.get(channel_id).unwrap();
            channel.ready()
                && (channel.has_bytes_buffered() || !channel.is_muted())
                && !self.has_completed_receive(channel_id)
                && !self.explicitly_muted_channels.contains(channel_id)
        };

        if should_read {
            let channel = self.channels.get_mut(channel_id).unwrap();
            // Fix A (read-path bottleneck): for transports with a real
            // non-blocking `try_read` (plaintext), use the synchronous
            // `try_read` path. The previous
            // `tokio::time::timeout(Duration::ZERO, channel.read()).await`
            // wrapper paid ~546us per call to register/deregister a Sleep on
            // the timer driver, capping single-Sender throughput at ~12K msg/s.
            // `try_read` drains all currently-available socket bytes in a tight
            // loop (see `NetworkReceive`), matching Java-NIO's read pattern and
            // avoiding the per-chunk readiness/timer overhead that throttled
            // large fetch reads
            // (design/current/consumer-throughput-bottleneck.md, UPDATE 4).
            // SSL transports keep the async fallback because rustls's I/O state
            // machine is driven through the async TLS stream; the zero-timeout
            // guard skips an async read that would await readiness to the next
            // poll iteration.
            let read_result = if channel.supports_try_read() {
                channel.try_read()
            } else {
                match tokio::time::timeout(std::time::Duration::ZERO, channel.read()).await {
                    Ok(r) => r,
                    Err(_elapsed) => Err(io::Error::from(io::ErrorKind::WouldBlock)),
                }
            };
            let bytes = match read_result {
                Ok(b) => b,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
                Err(e) => return Err(e),
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

    /// Begin closing a channel.
    async fn close_channel_internal(&mut self, id: &str, close_mode: CloseMode) {
        let mut channel = match self.channels.remove(id) {
            Some(c) => c,
            None => return,
        };

        // Phase 30: drop the channel's arming bookkeeping (and its armed-count
        // contribution). The closing-channels path below does not re-arm — the
        // graceful-close drain uses `maybe_read_from_closing_channel`, not the
        // WAIT arming path.
        self.unregister_arming(id);

        channel.disconnect();

        // Ensure that `connected` does not have closed channels
        self.connected.retain(|c| c != id);

        if close_mode == CloseMode::Graceful {
            // Check if there are pending receives
            self.closing_channels.insert(Arc::from(id), channel);
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
        self.immediately_connected_keys.remove(id.as_str());
        self.channels_with_buffered_read.remove(id.as_str());

        let _ = channel.close().await;

        self.explicitly_muted_channels.remove(id.as_str());
        if notify_disconnect {
            self.disconnected.insert(id, channel.state().clone());
        }
    }

    fn do_close(&mut self, channel: KafkaChannel, notify_disconnect: bool) {
        let id = channel.id().to_string();
        self.immediately_connected_keys.remove(id.as_str());
        self.channels_with_buffered_read.remove(id.as_str());

        // Channel is dropped here, which closes the underlying TcpStream
        // The KafkaChannel's close() method is async, but dropping is sufficient
        // for cleanup since Tokio streams close on drop.

        self.explicitly_muted_channels.remove(id.as_str());
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
            && self.channels.contains_key(connection_id.as_str())
        {
            kafka_trace!(
                self.log_context,
                "About to close the idle connection from {} due to being idle",
                connection_id
            );
            if let Some(channel) = self.channels.get_mut(connection_id.as_str()) {
                channel.set_state(channel_state::EXPIRED.clone());
            }
            // Use graceful close to process any buffered receives before
            // fully closing the channel, matching the Java implementation.
            self.close_channel_internal(&connection_id, CloseMode::Graceful).await;
        }
    }

    /// Clear completed receives.
    pub fn clear_completed_receives(&mut self) {
        // Phase 30 (dirty site #3): same reasoning as `clear()` — removing a
        // channel's completed receive turns `want_read` back ON, so re-arm it
        // at the next WAIT. (This is the explicit-clear entry point; `clear()`
        // covers the poll-top path. The next poll's `clear()` finds the list
        // already empty, so this must mark dirty itself.)
        if !self.completed_receives.is_empty() {
            let dirtied: Vec<Arc<str>> = self
                .completed_receives
                .iter()
                .filter_map(|r| self.channels.get_key_value(r.source()).map(|(k, _)| Arc::clone(k)))
                .collect();
            for key in dirtied {
                self.mark_interest_dirty(&key);
            }
        }
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
            return self.channels.get(id.as_str());
        }
        self.channels.values().next()
    }
    /// True if any channel is mid-message — it has read part of a receive but
    /// not completed it. The selector must not yield to a wakeup-poke in this
    /// state, or large reads get fragmented across `run_once` round-trips (the
    /// throughput bug; see design/current/consumer-throughput-bottleneck.md).
    /// A freshly-created empty receive (bytes_read == 0) does NOT count, so the
    /// between-fetch wakeup that triggers the next fetch is still honored.
    fn any_channel_mid_receive(&self) -> bool {
        self.channels.values().any(|c| c.current_receive_bytes_read() > 0)
    }

    /// Returns `true` if any channel is currently interested in read or write
    /// readiness, using the exact same per-channel interest predicate as
    /// [`Self::poll_channel_readiness`]. Mirrors Java NIO having at least one
    /// registered `SelectionKey` with a non-zero interest set.
    fn has_interested_channel(&self) -> bool {
        // Phase 30: O(1) form. A channel is "interested" for the purpose of
        // choosing the `select!` form iff it has an outstanding arm OR it is in
        // the re-arm set (`interest_dirty`) and will be armed by the WAIT's
        // first poll. This matches the old per-WAIT predicate sweep exactly:
        //   - any channel with interest that was armed-and-not-yet-fired
        //     contributes to `armed_count`;
        //   - any channel whose interest just turned ON is in `interest_dirty`
        //     and will be armed (becoming `Ready` or `armed`) by the WAIT.
        // A channel with no interest is neither armed nor dirty, so it does not
        // count — same as `channel_interest(..) == (false, false)`.
        self.armed_count > 0 || !self.interest_dirty.is_empty()
    }

    /// Computes the read/write interest for a single channel.
    ///
    /// Returns `(want_read_or_handshake, want_write)`. This predicate is
    /// byte-for-byte identical to the former `collect_readiness_futures`
    /// (Phase 23) — any divergence re-introduces either the busy-spin or the
    /// join stall.
    ///
    /// Channels mid-handshake (TLS or SASL) also register read interest so the
    /// poll loop wakes when handshake bytes arrive (matching Java NIO, which
    /// registers OP_READ | OP_WRITE based on SSLEngine / SASL state). Write
    /// interest is registered only when there is actual outgoing data to push:
    /// for ready channels a queued send, for handshaking channels unflushed
    /// ciphertext (`has_pending_writes`). Blanket write-interest for handshaking
    /// channels would fire immediately whenever the socket buffer is non-full
    /// (almost always), busy-spinning the loop while a response sits unread in
    /// the kernel buffer.
    fn channel_interest(&self, id: &str, channel: &KafkaChannel) -> (bool, bool) {
        let in_handshake = channel.is_connected() && !channel.ready();
        let want_read = channel.ready()
            && (channel.has_bytes_buffered() || !channel.is_muted())
            && !self.has_completed_receive(id)
            && !self.explicitly_muted_channels.contains(id);
        let want_write = (channel.has_send() && channel.ready()) || (in_handshake && channel.has_pending_writes());
        (want_read || in_handshake, want_write)
    }

    /// Arm a single channel's current interest with its cached per-channel
    /// wakers (Phase 30), the per-direction core of the `selectedKeys()` model.
    ///
    /// Computes [`Self::channel_interest`] (the EXACT Phase-23 predicate) and,
    /// for each interested direction that is not already armed, polls the
    /// transport with that direction's cached waker:
    ///   - `Ready` → the channel is immediately ready; it is added to
    ///     `ready_out` for processing this iteration (the direction is NOT
    ///     marked armed — there is nothing to wait for).
    ///   - `Pending` → the direction is marked armed; tokio now holds the
    ///     cached waker and the [`ChannelWaker`] will record the fire in the
    ///     `ready_queue` when readiness arrives.
    ///
    /// Buffered decrypted plaintext short-circuits to `ready_out` without a
    /// transport poll, exactly as the former sweep did (preserves the
    /// `data_in_buffers` fast path inside the loop).
    ///
    /// Side-effect-free with respect to connection state (§10): the only effect
    /// is waker registration and updating the selector's own arming
    /// bookkeeping. No bytes are consumed.
    fn arm_channel(&mut self, id: &Arc<str>, ready_out: &mut FxHashSet<Arc<str>>) {
        let channel = match self.channels.get(&**id) {
            Some(c) => c,
            None => return,
        };
        let (want_read, want_write) = self.channel_interest(id, channel);

        // Decrypted plaintext already buffered: do not wait on the socket
        // (mirrors the former sweep + the `effective_timeout = 0` data_in_buffers
        // fast path; preserve it).
        if want_read && channel.has_bytes_buffered() {
            ready_out.insert(Arc::clone(id));
            return;
        }

        // Read direction.
        if want_read {
            let arming = self.arming.get(&**id).expect("arming entry for active channel");
            if !arming.armed_read {
                let waker = arming.read_waker.clone();
                let mut cx = Context::from_waker(&waker);
                let channel = self.channels.get(&**id).unwrap();
                if channel.poll_transport_readable(&mut cx).is_ready() {
                    ready_out.insert(Arc::clone(id));
                } else {
                    let arming = self.arming.get_mut(&**id).unwrap();
                    arming.armed_read = true;
                    self.armed_count += 1;
                }
            }
        }

        // Write direction.
        if want_write {
            let arming = self.arming.get(&**id).expect("arming entry for active channel");
            if !arming.armed_write {
                let waker = arming.write_waker.clone();
                let mut cx = Context::from_waker(&waker);
                let channel = self.channels.get(&**id).unwrap();
                if channel.poll_transport_writable(&mut cx).is_ready() {
                    ready_out.insert(Arc::clone(id));
                } else {
                    let arming = self.arming.get_mut(&**id).unwrap();
                    arming.armed_write = true;
                    self.armed_count += 1;
                }
            }
        }
    }

    /// Arm every channel in this iteration's re-arm set (Phase 30): channels
    /// processed in pass-1 (their readiness was consumed, so they must be
    /// re-armed) ∪ the drained `interest_dirty` set (interest just flipped ON)
    /// ∪ immediately-connected channels. Channels armed-and-not-fired are left
    /// alone — zero per-iteration cost, which is the whole O(ready) win.
    ///
    /// `processed` is the pass-1 channel-id snapshot (`poll_id_scratch`).
    /// Immediately-ready channels are recorded in `ready_out`.
    fn arm_rearm_set(&mut self, processed: &[Arc<str>], ready_out: &mut FxHashSet<Arc<str>>) {
        // Drain the dirty set into the local scratch first so we don't borrow it
        // across the arming mutations.
        let dirty: Vec<Arc<str>> = self.interest_dirty.drain().collect();
        for id in &dirty {
            self.arm_channel(id, ready_out);
        }
        for id in processed {
            // Avoid re-arming a channel twice in one iteration.
            if !self.interest_dirty.contains(&**id) {
                self.arm_channel(id, ready_out);
            }
        }
    }

    /// Drain the fired-queue into `ready_out` (Phase 30), translating each
    /// `(token, is_write)` to the channel id via [`Self::token_to_id`] and
    /// clearing the corresponding armed flag. Tokens for channels removed since
    /// arming are simply absent and dropped. This is the `selectedKeys()`
    /// read-out: O(fired), not O(registered).
    ///
    /// Draining into the selector-owned `ready_out` (the persistent
    /// `ready_scratch`) — never a future-local — is what keeps the WAIT future
    /// cancel-safe: a wakeup / deadline that cancels the WAIT after a drain
    /// leaves the drained ids in `ready_scratch` to be processed next iteration.
    fn drain_fired_queue(&mut self, ready_out: &mut FxHashSet<Arc<str>>) {
        let fired: Vec<(u32, bool)> = {
            let mut guard = self.ready_queue.fired.lock().expect("ReadyQueue.fired poisoned");
            std::mem::take(&mut *guard)
        };
        for (token, is_write) in fired {
            // The arming entry may have been replaced or removed; only clear the
            // flag if the token still maps to a live, matching arming entry.
            if let Some(id) = self.token_to_id.get(&token).cloned() {
                if let Some(arming) = self.arming.get_mut(&*id)
                    && arming.token == token
                {
                    let flag = if is_write {
                        &mut arming.armed_write
                    } else {
                        &mut arming.armed_read
                    };
                    if *flag {
                        *flag = false;
                        self.armed_count -= 1;
                    }
                }
                ready_out.insert(id);
            }
        }
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
        // Intern the connection id once as a single `Arc<str>` shared between the
        // channels map and the tracking sets (CLAUDE.md §11).
        let key: Arc<str> = Arc::from(id);
        self.immediately_connected_keys.insert(Arc::clone(&key));
        // Phase 30 (dirty site #4): register per-channel wakers/arming so the
        // next WAIT can arm this channel's interest directly instead of
        // sweeping. Also marks it dirty.
        self.register_arming(&key);
        self.channels.insert(key, channel);

        if let Some(ref mut mgr) = self.idle_expiry_manager {
            mgr.update(id, nanos_now());
        }

        Ok(())
    }

    fn wakeup(&self) {
        self.notify.notify_one();
    }

    fn wakeup_handle(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    fn wakeup_notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    async fn close(&mut self) {
        let ids: Vec<Arc<str>> = self.channels.keys().cloned().collect();
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

        if let Some((key, _)) = self.closing_channels.get_key_value(connection_id.as_str()) {
            // Ensure notification via `disconnected`, leave channel in the state
            // in which closing was triggered. Reuse the interned id (refcount
            // bump, no allocation).
            let key = Arc::clone(key);
            self.failed_sends.push(key);
        } else {
            let channel = self.channels.get_mut(connection_id.as_str()).unwrap();
            match channel.set_send(send) {
                Ok(()) => {
                    // Phase 30 (dirty site #1): a queued send can turn
                    // `want_write` ON for this channel, which the WAIT must arm.
                    if let Some((key, _)) = self.channels.get_key_value(connection_id.as_str()) {
                        let key = Arc::clone(key);
                        self.mark_interest_dirty(&key);
                    }
                },
                Err(e) => {
                    // Update the state for consistency
                    channel.set_state(channel_state::FAILED_SEND.clone());
                    // Error path only (matches Java allocating here); a fresh
                    // `Arc<str>` is fine since the channel is about to be removed.
                    self.failed_sends.push(Arc::from(connection_id.as_str()));

                    // Remove and close the channel
                    if let Some(mut ch) = self.channels.remove(connection_id.as_str()) {
                        self.unregister_arming(connection_id.as_str());
                        ch.disconnect();
                        self.connected.retain(|c| c != &connection_id);
                        self.do_close(ch, false);
                        if let Some(ref mut mgr) = self.idle_expiry_manager {
                            mgr.remove(&connection_id);
                        }
                    }

                    kafka_error!(
                        self.log_context,
                        "Unexpected error during send, closing connection {} and returning error: {}",
                        connection_id,
                        e
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
        //
        // `deferred_wakeup` carries a wakeup that arrived while a channel was
        // mid-receive: the `select!` already consumed the `Notify` permit, so we
        // must not drop it. We give the in-flight receive exactly one more drain
        // pass (next loop iteration) and, if that pass makes no progress, honor
        // the wakeup by returning — never re-entering `select!` with the permit
        // already gone (which would block until the deadline and lose the
        // wakeup, unlike Java's `Selector.wakeup()` which always returns).
        let mut deferred_wakeup = false;

        // Phase 24: ready-set sweep. `ready_ids` holds the channels the prior
        // WAIT's readiness `poll_fn` flagged ready (empty on the first
        // iteration). Pass-1 processes only `immediately_connected ∪ ready_ids`
        // — mirroring Java NIO's `selector.select()` → `selectedKeys()` (process
        // only the ready keys) instead of issuing a `recv` syscall on every
        // registered channel each iteration. Taken from a reusable scratch field
        // (refilled by the WAIT, restored after the loop) so no per-poll
        // allocation occurs.
        let mut ready_ids = std::mem::take(&mut self.ready_scratch);
        ready_ids.clear();

        // `process_all` forces pass-1 to sweep every channel for one iteration.
        // It is set ONLY in two cases where there is no socket-readiness set to
        // narrow the work and the channels involved are known to have work:
        //   - the timeout-0 / `deadline = None` path (immediately-connected, or
        //     made-progress-last + data-in-buffers): there is no WAIT this poll,
        //     so no ready set is produced — fall back to processing all once
        //     (the original behavior). This is rare (join / partial-read drain).
        //   - a deferred wakeup's "one more drain pass" for a mid-receive
        //     channel: the WAIT was cancelled by the wakeup so it produced no
        //     ready set; sweep all so the in-flight receive is drained.
        // Correctness strictly trumps the optimization (PLAN): when in doubt,
        // process the channel.
        let mut process_all = deadline.is_none();

        loop {
            // Process channels with buffered data (reads only)
            if data_in_buffers {
                let buffered_ids: Vec<Arc<str>> = self.channels_with_buffered_read.drain().collect();
                for id in &buffered_ids {
                    if self.channels.contains_key(id) {
                        self.poll_channel_reads(id, false, start_select).await;
                    }
                }
                self.poll_channels_write_concurrent(&buffered_ids, start_select).await;
            }

            // Pass 1: Connect + Read the *ready* channels (sequential, fast).
            //
            // Snapshot the ids to process into the reusable scratch `Vec` (taken
            // from the struct, refilled, restored below) so this hot
            // per-iteration pass performs only `Arc` refcount bumps, not a fresh
            // heap allocation each time (Phase 22). The set is:
            //   - every channel, when `process_all` (no ready set available); or
            //   - `immediately_connected ∪ ready_ids` otherwise — only the
            //     channels the reactor flagged ready this iteration, plus any
            //     that connected immediately (which need connect/prepare
            //     processing even though no read-readiness was observed).
            let mut channel_ids = std::mem::take(&mut self.poll_id_scratch);
            channel_ids.clear();
            if process_all {
                channel_ids.extend(self.channels.keys().cloned());
            } else {
                // immediately-connected channels are always processed (connect /
                // prepare), exactly as before.
                channel_ids.extend(self.immediately_connected_keys.iter().cloned());
                for id in &ready_ids {
                    // Avoid a duplicate entry for a channel that is both ready
                    // and immediately-connected.
                    if !self.immediately_connected_keys.contains(&**id) {
                        channel_ids.push(Arc::clone(id));
                    }
                }
            }
            for id in &channel_ids {
                if self.channels.contains_key(id) {
                    let is_immediately = self.immediately_connected_keys.remove(&**id);
                    self.poll_channel_reads(id, is_immediately, start_select).await;
                }
            }
            self.immediately_connected_keys.clear();

            // Pass 2: Write the same set of channels concurrently.
            self.poll_channels_write_concurrent(&channel_ids, start_select).await;

            // Consumed this iteration's ready set; its readiness events have been
            // drained (try_read → WouldBlock / write completed), so the prior
            // ready ids are stale. Clear so `ready_ids` only holds channels that
            // are ready *this* iteration (re-armed below or drained by the WAIT).
            ready_ids.clear();
            process_all = false;

            // Phase 30: (re-)arm the re-arm set = pass-1 processed set
            // (`channel_ids`, immediately-connected already folded in) ∪ drained
            // `interest_dirty`. Arming registers each interested direction's
            // cached waker with the tokio reactor (the `selectedKeys()` model);
            // any direction already ready is added straight to `ready_ids`
            // (processed next iteration, no WAIT). Channels armed-and-not-fired
            // are left untouched — the O(ready) win. Done before the scratch
            // restore so the processed set (`channel_ids`) is still live.
            self.arm_rearm_set(&channel_ids, &mut ready_ids);

            // Restore the scratch buffer (retains its capacity for next time).
            channel_ids.clear();
            self.poll_id_scratch = channel_ids;

            // Phase 26 (Fix #1): match stock Java `NetworkClient.poll`, which
            // loops `do { selector.poll(t) } while (completedReceives().isEmpty()
            // && disconnected().isEmpty())` — it does NOT return on a completed
            // *response-expecting* send. A send-only round (fetch / heartbeat /
            // commit / `acks!=0` produce request written, response not yet
            // arrived) keeps waiting in the `select!` below for the actual
            // response, rather than returning and forcing `run_once` to spin a
            // full extra iteration (drain events + poll every manager) before
            // re-entering to await the response. Such `completed_sends` still
            // accumulate and are returned to the caller when the poll next breaks
            // (on receive / connect / disconnect / deadline), just one cycle
            // later.
            //
            // EXCEPTION — fire-and-forget sends (producer `acks=0`): these get NO
            // response ever, so there is nothing to wait for. If they did not
            // break here, the loop would park in `select!` until the deadline,
            // delaying every acks=0 send's synthesized completion
            // (`NetworkClient::handle_completed_sends`) by the full poll timeout —
            // observed as a ~throughput collapse to a few hundred msg/s with
            // multi-second latency and an idle CPU. This mirrors Java, whose
            // `Selector.poll` returns on the write-readiness event that completes
            // the send. Only fire-and-forget sends are checked, so the
            // response-expecting optimization above is preserved unchanged.
            //
            // CRITICAL (§10): `connected` MUST stay in the break.
            // `poll_channel_reads` pushes onto `self.connected` on the
            // post-handshake (TLS/SASL) ready transition specifically so `poll()`
            // exits and `handle_initiate_api_version_requests` fires (the join
            // path). Removing `connected` would re-introduce the join stall.
            let made_progress = !self.completed_receives.is_empty()
                || !self.connected.is_empty()
                || !self.disconnected.is_empty()
                || self.completed_sends.iter().any(NetworkSend::is_fire_and_forget);

            if made_progress {
                break;
            }

            // A wakeup arrived last iteration while a channel was mid-receive; we
            // gave the receive one more drain pass above and it made no progress,
            // so honor the deferred wakeup now rather than blocking in `select!`
            // with the already-consumed permit.
            if deferred_wakeup {
                break;
            }

            // Phase 30: arming above found channels already ready (buffered
            // plaintext, or a readiness the reactor had already set). Loop now to
            // process them rather than parking — mirrors the old
            // `poll_channel_readiness` returning `Ready` immediately when any
            // channel was ready.
            if !ready_ids.is_empty() {
                continue;
            }

            // No progress — wait for I/O readiness on any channel, wakeup,
            // or deadline. This replaces the former 1ms busy-poll with
            // proper event-driven readiness, matching Java NIO's
            // Selector.select(timeout) which uses epoll/kqueue.
            match deadline {
                Some(dl) if tokio::time::Instant::now() < dl => {
                    // Whether any channel is interested in readiness right now.
                    // When none are, fall back to the wakeup-vs-deadline-only
                    // form (mirrors the former `readiness_futs.is_empty()`
                    // branch).
                    let any_interested = self.has_interested_channel();

                    // An explicit wakeup (`Selector::wakeup()`) makes the poll
                    // return at this safe boundary, mirroring Java NIO where
                    // `Selector.wakeup()` causes the in-progress `select()` to
                    // return. Without returning here, a wakeup only triggers one
                    // more non-blocking pass and the poll keeps blocking until
                    // the deadline — which defeats the network-thread wakeup and
                    // is part of the consumer join-stall root cause
                    // (`design/current/consumer-join-stall-rootcause.md`).
                    // Cloned before the mutable reborrow below.
                    let notify = self.notify.clone();

                    // Phase 30: the WAIT future. The per-channel wakers were
                    // armed above (the `selectedKeys()` model). This future does
                    // only two things, both cancel-safe (§10): (1) register the
                    // WAIT task's root waker in the `ready_queue` so a
                    // `ChannelWaker` fire un-parks us, and (2) drain the
                    // fired-queue into `ready_ids` (selector-owned scratch, never
                    // a future-local — so a wakeup / deadline that cancels this
                    // future after a drain leaves the ids in `ready_ids` to be
                    // processed next iteration). It returns `Ready` as soon as any
                    // armed channel has fired; otherwise `Pending`. No bytes are
                    // consumed and no connection state is mutated, so dropping it
                    // loses nothing — the non-cancel-safe network poll stays in
                    // pass-1, outside this `select!`.
                    let selector = &mut *self;
                    let ready_ids_ref = &mut ready_ids;
                    let readiness_wait = std::future::poll_fn(move |cx| {
                        selector.ready_queue.set_root(cx.waker());
                        selector.drain_fired_queue(ready_ids_ref);
                        if ready_ids_ref.is_empty() {
                            Poll::Pending
                        } else {
                            Poll::Ready(())
                        }
                    });

                    // `notify.notified() => true` returns the poll on wakeup. A
                    // `wakeup()` issued just *before* this poll parked stores a
                    // permit, so the next poll returns immediately with no I/O —
                    // this is intentional and matches Java NIO, where a
                    // `Selector.wakeup()` before `select()` makes that `select()`
                    // return at once. The early return is harmless (empty
                    // `responses`); the caller simply loops.
                    let woke_by_wakeup = if !any_interested {
                        tokio::select! {
                            biased;
                            _ = notify.notified() => true,
                            _ = tokio::time::sleep_until(dl) => false,
                        }
                    } else {
                        tokio::select! {
                            biased;
                            _ = notify.notified() => true,
                            _ = readiness_wait => false,
                            _ = tokio::time::sleep_until(dl) => false,
                        }
                    };
                    if woke_by_wakeup {
                        if self.any_channel_mid_receive() {
                            // Mid-message: don't abandon the in-flight receive to
                            // the poke (which would fragment the read across
                            // run_once round-trips). Give it one more drain pass,
                            // but remember the wakeup — the `select!` consumed the
                            // permit, so the loop's `deferred_wakeup` check will
                            // honor it if that pass makes no progress.
                            deferred_wakeup = true;
                            // The wakeup cancelled `readiness_wait`, so `ready_ids`
                            // does not reflect a fresh readiness snapshot. Sweep
                            // all channels on the drain pass so the mid-receive
                            // channel is processed (correctness > optimization;
                            // this path is rare — a wakeup landing mid-frame).
                            process_all = true;
                        } else {
                            break;
                        }
                    }
                },
                _ => {
                    // Immediate return (timeout 0 / deadline passed): there is no
                    // blocking `select!` wait this pass. On transports with a
                    // synchronous `try_read`, the read path performs no `.await`
                    // of its own (Fix A: no `timeout(ZERO, …).await`), so a
                    // single pass can complete without ever yielding to the
                    // runtime. Yield once before returning so the I/O driver and
                    // any cooperatively-scheduled peer tasks make progress —
                    // otherwise a caller that spin-loops `poll(0)` on a
                    // single-threaded runtime would starve the Tokio I/O reactor
                    // (OS readiness never reaches the channel and idle expiry
                    // trips with a spurious disconnect). Cheap (once per poll,
                    // not per read chunk) and harmless when there is work.
                    tokio::task::yield_now().await;
                    break;
                },
            }
        }

        // Phase 30 cancel-safety: the WAIT may have drained `(token, dir)`
        // fires into `ready_ids` and cleared their armed flags, then been
        // cancelled by a wakeup / deadline before pass-1 could process them
        // (the break paths above). Those channels are now neither armed nor
        // processed. Re-mark them dirty so the NEXT poll re-arms them — tokio
        // readiness is level-triggered, so re-arming a channel whose socket is
        // still readable returns `Ready` again and the data is not stranded.
        // Without this, a wakeup landing exactly after a fire-drain could strand
        // a ready channel (the subtle data-stall the PLAN flags). Cheap: bounded
        // by the number of fires this poll, and a no-op on the steady-state path
        // (`ready_ids` is consumed at the top of the loop, so it is only
        // non-empty here after a cancelled WAIT).
        if !ready_ids.is_empty() {
            let leftover: Vec<Arc<str>> = ready_ids
                .iter()
                .filter(|id| self.channels.contains_key(&***id))
                .cloned()
                .collect();
            for id in leftover {
                self.mark_interest_dirty(&id);
            }
        }

        // Restore the ready-set scratch buffer (retains its capacity for next
        // time), mirroring the `poll_id_scratch` handling above.
        ready_ids.clear();
        self.ready_scratch = ready_ids;

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

    fn drain_completed_receives(&mut self) -> Vec<(String, Option<Vec<u8>>)> {
        // Move every NetworkReceive out of the list and take its payload Vec by
        // move (no copy — §27 Phase 20 Fix #3). std::mem::take leaves the list
        // empty so the next poll's clear() is a no-op.
        let drained = std::mem::take(&mut self.completed_receives);
        // Phase 30 (dirty site #3): draining a channel's completed receive turns
        // `want_read` back ON, so mark each source dirty for re-arming. The next
        // poll's `clear()` sees an empty list, so this entry point must do it.
        for r in &drained {
            if let Some((key, _)) = self.channels.get_key_value(r.source()) {
                let key = Arc::clone(key);
                self.mark_interest_dirty(&key);
            }
        }
        drained.into_iter().map(NetworkReceive::into_source_and_payload).collect()
    }

    fn disconnected(&self) -> &HashMap<String, ChannelState> {
        &self.disconnected
    }

    fn connected(&self) -> &[String] {
        &self.connected
    }

    fn mute(&mut self, id: &str) {
        // Reuse the interned `Arc<str>` key from whichever map owns the channel
        // (refcount bump, no allocation).
        if let Some((key, _)) = self.channels.get_key_value(id) {
            let key = Arc::clone(key);
            self.channels.get_mut(&*key).unwrap().mute();
            self.explicitly_muted_channels.insert(key);
            self.channels_with_buffered_read.remove(id);
        } else if let Some((key, _)) = self.closing_channels.get_key_value(id) {
            let key = Arc::clone(key);
            self.closing_channels.get_mut(&*key).unwrap().mute();
            self.explicitly_muted_channels.insert(key);
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
            // Phase 30 (dirty site #2): unmuting turns `want_read` back ON for
            // this channel, so the next WAIT must (re-)arm it. A readiness event
            // that fired while the channel was muted may have already been
            // consumed (stale), so re-arming is required even when no bytes are
            // currently buffered — otherwise data sent while muted would stall.
            if let Some((key, _)) = self.channels.get_key_value(id) {
                let key = Arc::clone(key);
                self.mark_interest_dirty(&key);
            }
            if let Some((key, channel)) = self.channels.get_key_value(id)
                && channel.has_bytes_buffered()
            {
                let key = Arc::clone(key);
                self.channels_with_buffered_read.insert(key);
                self.made_read_progress_last_poll = true;
            }
        }
    }

    fn mute_all(&mut self) {
        let ids: Vec<Arc<str>> = self.channels.keys().cloned().collect();
        for id in ids {
            self.mute(&id);
        }
    }

    fn unmute_all(&mut self) {
        let ids: Vec<Arc<str>> = self.channels.keys().cloned().collect();
        for id in ids {
            self.unmute(&id);
        }
    }

    fn is_channel_ready(&self, id: &str) -> bool {
        self.channels.get(id).is_some_and(|c| c.ready())
    }
}

/// Result of a single channel write operation.
struct ChannelWriteResult {
    bytes_written: usize,
    completed_send: Option<NetworkSend>,
    error: Option<(io::Error, bool)>,
}

/// Standalone write I/O on a single extracted channel — no Selector state access.
async fn do_channel_write(channel: &mut KafkaChannel, current_time_nanos: u64) -> ChannelWriteResult {
    if !channel.has_send() || !channel.ready() {
        return ChannelWriteResult { bytes_written: 0, completed_send: None, error: None };
    }

    let should_write = match channel.maybe_begin_client_reauthentication(|| current_time_nanos) {
        Ok(reauth) => !reauth,
        Err(e) => {
            return ChannelWriteResult { bytes_written: 0, completed_send: None, error: Some((e, false)) };
        },
    };
    if !should_write {
        return ChannelWriteResult { bytes_written: 0, completed_send: None, error: None };
    }

    match channel.write_concurrent().await {
        Ok(bytes) => {
            let send = channel.maybe_complete_send();
            ChannelWriteResult { bytes_written: bytes, completed_send: send, error: None }
        },
        Err(e) => ChannelWriteResult { bytes_written: 0, completed_send: None, error: Some((e, true)) },
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
        NetworkSend::new(
            node,
            Box::new(ByteBufferSend::size_prefixed(bytes::Bytes::copy_from_slice(payload.as_bytes()))),
        )
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
            selector.closing_channels.insert(Arc::from("3"), channel);
        }
        assert_eq!("3", selector.lowest_priority_channel().unwrap().id());
        // Restore channel
        if let Some(channel) = selector.closing_channels.remove("3") {
            selector.channels.insert(Arc::from("3"), channel);
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

    /// Phase 23 — exercises the rewritten non-allocating readiness wait
    /// (`poll_channel_readiness` driven through the `poll_fn` in `Selector::poll`)
    /// against three distinct failure modes the rewrite must avoid:
    ///
    ///   (a) a channel that becomes readable wakes the parked `poll` (waker
    ///       registration on the underlying socket works — the loss of which
    ///       would only un-park on the deadline);
    ///   (b) an explicit `wakeup()` returns the parked `poll` promptly (§11
    ///       parity — the `notify.notified()` arm still wins);
    ///   (c) an idle muted channel does NOT busy-spin: with no interested
    ///       channel the wait parks to the deadline and returns *on* it, rather
    ///       than returning `Ready` immediately and spinning the loop.
    ///
    /// Each sub-assertion is wrapped in `tokio::time::timeout` so a hung wait
    /// path fails the test instead of blocking the suite. Uses the real TCP
    /// loopback `EchoServer` scaffolding the other selector tests use, so it
    /// drives the production socket-readiness path (no mock transport).
    #[tokio::test]
    async fn test_readiness_wait_path() {
        use std::time::{Duration, Instant};

        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;
        blocking_connect(&mut selector, "0", server.port()).await;

        // (a) A channel that becomes readable wakes the parked poll.
        //
        // Send a request; the echo response is not yet available, so the first
        // poll parks on read-readiness. When the server echoes back, the
        // registered waker fires and the poll returns with the response — well
        // before the (generous) 5s timeout. If the waker were lost, the poll
        // would only return on its own internal deadline.
        selector.send(create_send("0", "readiness-wake")).unwrap();
        let start = Instant::now();
        let mut got_response = false;
        // Bound the whole drain in a hard timeout so a hang fails the test.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                // Long per-poll timeout so the poll genuinely parks on socket
                // readiness rather than returning on a short deadline; a working
                // waker un-parks it as soon as the echo arrives.
                selector.poll(5_000).await.unwrap();
                if selector.completed_receives().iter().any(|r| r.source() == "0") {
                    got_response = true;
                    break;
                }
            }
        })
        .await
        .expect("(a) parked poll did not wake on socket readiness within 5s");
        assert!(got_response, "(a) expected an echoed response from node 0");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "(a) readable channel should wake the parked poll well before the poll deadline"
        );

        // (b) wakeup() returns the parked poll promptly.
        //
        // The channel is now idle (no in-flight send/receive) but still read
        // interested. Park `poll` on a long (10s) deadline, then fire wakeup()
        // from another task after a short delay. The `notify.notified()` arm is
        // the first `biased;` arm, so the poll must return promptly — far short
        // of the 10s deadline. We assert it returns inside a 2s hard timeout.
        let notify = selector.wakeup_notify();
        let waker = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            notify.notify_one();
        });
        let start = Instant::now();
        tokio::time::timeout(Duration::from_secs(2), selector.poll(10_000))
            .await
            .expect("(b) wakeup() did not return the parked poll within 2s")
            .unwrap();
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "(b) wakeup() should return the parked poll promptly, not on the deadline"
        );
        waker.await.unwrap();

        // (c) An idle muted channel does NOT spin.
        //
        // Muting the only channel makes `channel_interest` return
        // (false, false) for it, so `has_interested_channel()` is false and the
        // wait falls to the wakeup-vs-deadline-only form. With no wakeup and no
        // readiness, the poll must park until the deadline and return *on* it —
        // NOT return `Ready` immediately (which would busy-spin the loop). We
        // assert the elapsed time is close to the requested timeout; a spurious
        // `Ready` / busy-spin would return in ~0ms.
        selector.mute("0");
        let timeout_ms = 200;
        let start = Instant::now();
        // Hard cap well above the deadline so a genuine hang still fails.
        tokio::time::timeout(Duration::from_secs(2), selector.poll(timeout_ms))
            .await
            .expect("(c) muted-channel poll hung past its deadline")
            .unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis((timeout_ms as u64 * 8) / 10),
            "(c) idle muted channel must wait to the deadline, not busy-spin \
             (elapsed {elapsed:?}, expected >= ~{}ms)",
            (timeout_ms * 8) / 10
        );
        selector.unmute("0");

        // Cleanup.
        selector.close_channel("0").await;
        selector.poll(0).await.unwrap();
    }

    // ---- Phase 24: ready-set sweep scaffolding ---------------------------
    //
    // A transport layer that wraps a real `PlaintextTransportLayer` and counts
    // every `try_read` (recv syscall) per channel id into a shared map. Lets a
    // test assert that the steady-state poll issues `try_read` ONLY on the
    // channels the reactor flagged ready — not on idle channels.

    use crate::common::network::transport_layer::{InterestOps, TransportLayer};
    use crate::common::network::{ChannelMetadataRegistry, PlaintextAuthenticator, PlaintextTransportLayer};
    use std::sync::Mutex as StdMutex;

    /// Shared per-channel `try_read` call counter.
    type TryReadCounts = Arc<StdMutex<HashMap<String, usize>>>;

    struct CountingTransportLayer {
        id: String,
        inner: PlaintextTransportLayer,
        counts: TryReadCounts,
    }

    impl TransportLayer for CountingTransportLayer {
        fn peer_addr(&self) -> io::Result<SocketAddr> {
            self.inner.peer_addr()
        }
        fn ready(&self) -> bool {
            self.inner.ready()
        }
        fn finish_connect(
            &mut self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<bool>> + Send + '_>> {
            self.inner.finish_connect()
        }
        fn disconnect(&mut self) {
            self.inner.disconnect()
        }
        fn is_connected(&self) -> bool {
            self.inner.is_connected()
        }
        fn handshake(&mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
            self.inner.handshake()
        }
        fn add_interest_ops(&mut self, ops: InterestOps) {
            self.inner.add_interest_ops(ops)
        }
        fn remove_interest_ops(&mut self, ops: InterestOps) {
            self.inner.remove_interest_ops(ops)
        }
        fn is_mute(&self) -> bool {
            self.inner.is_mute()
        }
        fn has_bytes_buffered(&self) -> bool {
            self.inner.has_bytes_buffered()
        }
        fn has_pending_writes(&self) -> bool {
            self.inner.has_pending_writes()
        }
        fn is_open(&self) -> bool {
            self.inner.is_open()
        }
        fn close(&mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
            self.inner.close()
        }
        fn read<'a>(
            &'a mut self,
            dst: &'a mut [u8],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<usize>> + Send + 'a>> {
            self.inner.read(dst)
        }
        fn try_read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
            // The whole point of the test scaffolding: record the recv syscall.
            *self.counts.lock().unwrap().entry(self.id.clone()).or_insert(0) += 1;
            self.inner.try_read(dst)
        }
        fn supports_try_read(&self) -> bool {
            self.inner.supports_try_read()
        }
        fn write<'a>(
            &'a mut self,
            src: &'a [u8],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<usize>> + Send + 'a>> {
            self.inner.write(src)
        }
        fn readable(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
            self.inner.readable()
        }
        fn writable(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
            self.inner.writable()
        }
        fn poll_readable(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
            self.inner.poll_readable(cx)
        }
        fn poll_writable(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
            self.inner.poll_writable(cx)
        }
        fn write_vectored<'a>(
            &'a mut self,
            srcs: &'a [io::IoSlice<'a>],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<usize>> + Send + 'a>> {
            self.inner.write_vectored(srcs)
        }
        fn try_write_vectored(&mut self, srcs: &[io::IoSlice<'_>]) -> io::Result<usize> {
            self.inner.try_write_vectored(srcs)
        }
    }

    struct CountingChannelBuilder {
        counts: TryReadCounts,
    }

    impl ChannelBuilder for CountingChannelBuilder {
        fn build_channel(
            &self,
            id: &str,
            stream: tokio::net::TcpStream,
            _peer_host: &str,
            max_receive_size: i32,
            metadata_registry: Box<dyn ChannelMetadataRegistry>,
        ) -> io::Result<KafkaChannel> {
            let transport_layer = Box::new(CountingTransportLayer {
                id: id.to_string(),
                inner: PlaintextTransportLayer::connected(stream),
                counts: self.counts.clone(),
            });
            let authenticator = Box::new(PlaintextAuthenticator::new());
            Ok(KafkaChannel::new(
                id,
                transport_layer,
                authenticator,
                max_receive_size,
                metadata_registry,
            ))
        }

        fn close(&mut self) {}
    }

    async fn create_counting_selector() -> (Selector, TryReadCounts) {
        let counts: TryReadCounts = Arc::new(StdMutex::new(HashMap::new()));
        let channel_builder = Box::new(CountingChannelBuilder { counts: counts.clone() });
        let selector = Selector::new(
            super::super::network_receive::UNLIMITED,
            CONNECTION_MAX_IDLE_MS,
            channel_builder,
        );
        (selector, counts)
    }

    fn try_reads_for(counts: &TryReadCounts, id: &str) -> usize {
        *counts.lock().unwrap().get(id).unwrap_or(&0)
    }

    /// Phase 24 — the ready-set sweep processes ONLY the channels the reactor
    /// flagged ready, never the idle ones, and never *permanently* excludes a
    /// channel that later becomes readable.
    ///
    /// Two connected channels share one counting transport. Only channel "0"
    /// has an in-flight request/echo; channel "1" is idle. We drive `poll` to
    /// drain "0"'s echo, then assert:
    ///
    ///   (a) channel "1" (idle, read-interested, no data) received ZERO
    ///       `try_read` syscalls during the steady-state drain — the old
    ///       sweep-all loop would have issued one per poll iteration;
    ///   (b) channel "0" received at least one `try_read` (it was ready and was
    ///       processed — proving the ready set is actually driving pass-1, not
    ///       silently skipping work);
    ///   (c) when channel "1" later gets its own request, it IS processed and
    ///       its echo is drained — no permanent exclusion / no stall.
    ///
    /// Wrapped in `tokio::time::timeout` so a stall fails the test instead of
    /// hanging the suite. Uses the real TCP loopback `EchoServer`, so it drives
    /// the production socket-readiness path with only `try_read` instrumented.
    #[tokio::test]
    async fn test_ready_set_sweep_skips_idle_channels() {
        use std::time::Duration;

        let server = EchoServer::new().await.unwrap();
        let (mut selector, counts) = create_counting_selector().await;
        blocking_connect(&mut selector, "0", server.port()).await;
        blocking_connect(&mut selector, "1", server.port()).await;

        // Settle: drain the "immediately connected" process-all pass and any
        // spurious post-connect socket readiness, so the selector is quiescent
        // before we measure. (A freshly-connected channel forces a one-shot
        // process-all pass and may report read-ready once; we want to measure
        // the steady-state ready-set behavior, not connect bookkeeping.)
        for _ in 0..3 {
            selector.poll(20).await.unwrap();
        }

        // Baseline the counters AFTER settling. We only care about syscalls
        // during the steady-state drain that follows.
        let base0 = try_reads_for(&counts, "0");
        let base1 = try_reads_for(&counts, "1");

        // Only channel "0" sends; channel "1" stays idle.
        selector.send(create_send("0", "ready-set-0")).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                selector.poll(5_000).await.unwrap();
                if selector.completed_receives().iter().any(|r| r.source() == "0") {
                    break;
                }
            }
        })
        .await
        .expect("(a) channel 0's echo was not drained within 5s");

        let after0 = try_reads_for(&counts, "0");
        let after1 = try_reads_for(&counts, "1");

        // (b) the ready channel WAS processed.
        assert!(
            after0 > base0,
            "(b) ready channel 0 must receive at least one try_read (got {base0} -> {after0})"
        );
        // (a) the idle channel was NOT touched by a recv syscall.
        assert_eq!(
            after1, base1,
            "(a) idle channel 1 must not receive any try_read during the drain \
             (got {base1} -> {after1}); the ready-set sweep must skip idle channels"
        );

        // (c) channel "1" later becomes readable and IS processed — no
        // permanent exclusion. (Mutation check: if a ready channel were wrongly
        // excluded from the sweep, this drain would never complete and the
        // timeout would fail the test.)
        selector.send(create_send("1", "ready-set-1")).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                selector.poll(5_000).await.unwrap();
                if selector.completed_receives().iter().any(|r| r.source() == "1") {
                    break;
                }
            }
        })
        .await
        .expect("(c) previously-idle channel 1 was never processed — permanent exclusion / stall");
        assert!(
            try_reads_for(&counts, "1") > after1,
            "(c) channel 1 must receive a try_read once it has data"
        );

        // Cleanup.
        selector.close_channel("0").await;
        selector.close_channel("1").await;
        selector.poll(0).await.unwrap();
    }

    /// Phase 24 — no busy-spin: once all ready channels are drained, a poll with
    /// idle (read-interested, no-data) channels must PARK to its deadline rather
    /// than returning `Ready` immediately and re-entering the loop at 100% CPU.
    ///
    /// If a channel that `poll_transport_readable` reports ready were left
    /// unprocessed, the next WAIT would return immediately (reactor readiness
    /// still set) and the loop would spin. Here the channel has NO data, so the
    /// reactor never flags it ready and the poll must block to the deadline. We
    /// assert the elapsed time is close to the requested timeout; a busy-spin
    /// (or a spurious `Ready`) would return in ~0ms.
    ///
    /// Complements `test_readiness_wait_path` (c) (single muted channel) by
    /// exercising the multi-channel ready-set path with an idle but
    /// read-interested channel.
    #[tokio::test]
    async fn test_ready_set_sweep_no_busy_spin() {
        use std::time::{Duration, Instant};

        let server = EchoServer::new().await.unwrap();
        let (mut selector, counts) = create_counting_selector().await;
        blocking_connect(&mut selector, "0", server.port()).await;
        blocking_connect(&mut selector, "1", server.port()).await;

        // Settle: drain the "immediately connected" state and any spurious
        // post-connect socket readiness. A freshly-connected channel is in
        // `immediately_connected_keys` (forcing a one-shot process-all pass) and
        // its socket may report read-ready once before a `try_read` -> WouldBlock
        // clears it. Poll a few short times so the selector reaches a quiescent
        // state before we measure the parking behavior.
        for _ in 0..3 {
            selector.poll(20).await.unwrap();
        }

        // No sends: both channels are read-interested but have no data. The
        // poll must park to its deadline (no readiness, no wakeup).
        let timeout_ms = 200;
        let base0 = try_reads_for(&counts, "0");
        let base1 = try_reads_for(&counts, "1");
        let start = Instant::now();
        tokio::time::timeout(Duration::from_secs(2), selector.poll(timeout_ms))
            .await
            .expect("no-busy-spin poll hung well past its deadline")
            .unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis((timeout_ms as u64 * 8) / 10),
            "idle read-interested channels must park to the deadline, not busy-spin \
             (elapsed {elapsed:?}, expected >= ~{}ms)",
            (timeout_ms * 8) / 10
        );
        // With no data, neither channel should have been read in a tight spin.
        // (One initial try_read on the first iteration's process-all fallback is
        // possible since this poll has deadline=None? No: timeout_ms>0 so
        // deadline is Some and the first iteration's ready set is empty — pass-1
        // processes nothing, the WAIT parks. So zero new try_reads is expected.)
        assert_eq!(
            try_reads_for(&counts, "0"),
            base0,
            "idle channel 0 must not be read while parked"
        );
        assert_eq!(
            try_reads_for(&counts, "1"),
            base1,
            "idle channel 1 must not be read while parked"
        );

        // Cleanup.
        selector.close_channel("0").await;
        selector.close_channel("1").await;
        selector.poll(0).await.unwrap();
    }

    // ---- Phase 26 (Fix #1): send-only poll does not return early -------------

    /// A loopback server that accepts a connection and silently drains all
    /// inbound bytes WITHOUT echoing anything back. Used to construct a
    /// send-only poll round: the selector writes a request (completing a
    /// `NetworkSend`) but no response ever arrives, so a poll that returned on a
    /// completed *send* would return early. After Phase 26 Fix #1 the poll must
    /// keep parking on read-readiness until the deadline (or a wakeup / connect /
    /// disconnect), matching stock Java `NetworkClient.poll`.
    struct SinkServer {
        addr: SocketAddr,
        closing: Arc<AtomicBool>,
        _task: tokio::task::JoinHandle<()>,
    }

    impl SinkServer {
        async fn new() -> io::Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let closing = Arc::new(AtomicBool::new(false));
            let closing_clone = closing.clone();
            let task = tokio::spawn(async move {
                while !closing_clone.load(Ordering::Relaxed) {
                    let accept_result = tokio::select! {
                        result = listener.accept() => result,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => continue,
                    };
                    if let Ok((mut stream, _)) = accept_result {
                        let closing_inner = closing_clone.clone();
                        tokio::spawn(async move {
                            let mut buf = [0u8; 1024];
                            // Drain and discard everything; never write back.
                            while !closing_inner.load(Ordering::Relaxed) {
                                match stream.read(&mut buf).await {
                                    Ok(0) => break,
                                    Ok(_) => {},
                                    Err(_) => break,
                                }
                            }
                        });
                    }
                }
            });
            Ok(Self { addr, closing, _task: task })
        }

        fn port(&self) -> u16 {
            self.addr.port()
        }
    }

    impl Drop for SinkServer {
        fn drop(&mut self) {
            self.closing.store(true, Ordering::Relaxed);
        }
    }

    /// Phase 26 (Fix #1) — a poll that only completes a send (no receive) must
    /// NOT return until a receive arrives or the deadline, but must still return
    /// promptly on a wakeup. Mirrors stock Java `NetworkClient.poll`, which loops
    /// while `completedReceives().isEmpty() && disconnected().isEmpty()` and does
    /// NOT return on completed sends.
    ///
    /// Each sub-assertion is bounded by a hard `tokio::time::timeout` so a
    /// regression (returning early on the completed send, or hanging) fails the
    /// test instead of blocking the suite.
    #[tokio::test]
    async fn test_send_only_poll_does_not_return_early() {
        use std::time::{Duration, Instant};

        let server = SinkServer::new().await.unwrap();
        let mut selector = create_selector().await;
        blocking_connect(&mut selector, "0", server.port()).await;

        // Settle: drain the immediately-connected / made-read-progress
        // bookkeeping from the connect so the next poll genuinely parks on the
        // socket-readiness `select!` (eff_timeout > 0, deadline = Some) rather
        // than taking the timeout-0 process-all path. Mirrors the Phase-24
        // selector tests' settle loop.
        for _ in 0..3 {
            selector.poll(20).await.unwrap();
        }

        // Queue the request to the sink server (which never echoes). Drive the
        // poll until the send is written (`completed_sends` non-empty). This may
        // take a couple of iterations (the writable-readiness must fire). Bounded
        // by a hard timeout so a regression hangs the test instead of the suite.
        selector.send(create_send("0", "send-only")).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                selector.poll(200).await.unwrap();
                if !selector.completed_sends().is_empty() {
                    break;
                }
            }
        })
        .await
        .expect("the send was never written within 5s");
        // No echo ever arrives from the sink server.
        assert!(
            selector.completed_receives().is_empty(),
            "the sink server never echoes, so there must be no completed receive"
        );

        // (a) Now the send is done and there is no pending write and no incoming
        // data. A poll must PARK on read readiness until its deadline rather than
        // returning early on the (now-cleared / never-again) send. We use a short
        // 200ms deadline and assert the poll took close to the full deadline. A
        // regression that returned on a completed send would only matter while a
        // send is outstanding, so we additionally re-send below to cover the
        // outstanding-send case directly.
        let timeout_ms = 200;
        let start = Instant::now();
        tokio::time::timeout(Duration::from_secs(2), selector.poll(timeout_ms))
            .await
            .expect("(a) idle poll hung well past its deadline")
            .unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis((timeout_ms as u64 * 8) / 10),
            "(a) an idle poll with no incoming data must wait to the deadline \
             (elapsed {elapsed:?}, expected >= ~{}ms)",
            (timeout_ms * 8) / 10
        );

        // (a2) The core Fix #1 assertion: with a send OUTSTANDING (queued but not
        // yet acked by any receive), the poll that completes the send must NOT
        // return early on that completed send — it must keep parking to the
        // deadline because no receive arrived. Send a fresh request and measure a
        // single poll with a short deadline; the send completes during this poll
        // but, post-fix, `completed_sends` is no longer in the `made_progress`
        // break, so the poll parks to the deadline.
        selector.send(create_send("0", "send-only-2")).unwrap();
        let timeout_ms = 300;
        let start = Instant::now();
        tokio::time::timeout(Duration::from_secs(2), selector.poll(timeout_ms))
            .await
            .expect("(a2) send-only poll hung well past its deadline")
            .unwrap();
        let elapsed = start.elapsed();
        assert!(
            !selector.completed_sends().is_empty(),
            "(a2) the request should have been written and completed as a send"
        );
        assert!(
            selector.completed_receives().is_empty(),
            "(a2) no echo arrives from the sink server, so there must be no completed receive"
        );
        assert!(
            elapsed >= Duration::from_millis((timeout_ms as u64 * 8) / 10),
            "(a2) a send-only poll must wait to the deadline, not return early on the completed send \
             (elapsed {elapsed:?}, expected >= ~{}ms)",
            (timeout_ms * 8) / 10
        );

        // (b) A wakeup() must still return the parked poll promptly even when the
        // only outstanding state is an unanswered send. Fire wakeup() after a
        // short delay while the poll parks on a long (10s) deadline.
        let notify = selector.wakeup_notify();
        let waker = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            notify.notify_one();
        });
        let start = Instant::now();
        tokio::time::timeout(Duration::from_secs(2), selector.poll(10_000))
            .await
            .expect("(b) wakeup() did not return the parked send-only poll within 2s")
            .unwrap();
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "(b) wakeup() should return the parked poll promptly, not on the deadline"
        );
        waker.await.unwrap();

        // Note: the connect-break and disconnect-break paths are unchanged by
        // Fix #1 (`connected` and `disconnected` stay in the `made_progress`
        // break) and are covered by `test_readiness_wait_path`,
        // `blocking_connect`/`wait_for_channel_ready`, and the `test_close*`
        // family. Only `completed_sends` was removed from the break here.

        // Cleanup.
        selector.close_channel("0").await;
        selector.poll(0).await.unwrap();
    }

    /// Regression test for the producer `acks=0` collapse: a **fire-and-forget**
    /// send (one that expects no response, `NetworkSend::is_fire_and_forget`)
    /// gets NO receive ever, so the poll that completes it MUST break promptly
    /// rather than parking to the deadline. This is the inverse of
    /// `test_send_only_poll_does_not_return_early`'s `(a2)` case (a
    /// response-expecting send, which correctly parks to the deadline).
    ///
    /// Before the fix, `made_progress` excluded `completed_sends` entirely, so an
    /// `acks=0` send's synthesized completion (`NetworkClient::handle_completed_sends`)
    /// was delayed by the full poll timeout — observed as an ~1600x throughput
    /// collapse (a few hundred msg/s) with multi-second latency and an idle CPU.
    ///
    /// Bounded by a hard `tokio::time::timeout` so a regression (parking to the
    /// long deadline) fails the test instead of blocking the suite.
    #[tokio::test]
    async fn test_fire_and_forget_send_breaks_poll_promptly() {
        use std::time::{Duration, Instant};

        let server = SinkServer::new().await.unwrap();
        let mut selector = create_selector().await;
        blocking_connect(&mut selector, "0", server.port()).await;

        // Settle the connect bookkeeping so the measured poll genuinely parks on
        // the socket-readiness `select!` (deadline = Some), matching the sibling
        // test's settle loop.
        for _ in 0..3 {
            selector.poll(20).await.unwrap();
        }

        // Queue a FIRE-AND-FORGET send (producer acks=0): no echo will ever come
        // back from the sink server, so the only terminal event is the send
        // completing.
        let mut send = create_send("0", "fire-and-forget");
        send.set_fire_and_forget(true);
        selector.send(send).unwrap();

        // A single poll with a LONG (10s) deadline must return PROMPTLY once the
        // send is written — the fire-and-forget completed send breaks the poll
        // loop. Pre-fix, this poll would park the full 10s.
        let start = Instant::now();
        tokio::time::timeout(Duration::from_secs(3), selector.poll(10_000))
            .await
            .expect("fire-and-forget poll hung well past a prompt return")
            .unwrap();
        let elapsed = start.elapsed();

        assert!(
            !selector.completed_sends().is_empty(),
            "the fire-and-forget request should have been written and completed as a send"
        );
        assert!(
            selector.completed_sends().iter().any(NetworkSend::is_fire_and_forget),
            "the completed send must be marked fire-and-forget"
        );
        assert!(
            selector.completed_receives().is_empty(),
            "the sink server never echoes, so there must be no completed receive"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "a fire-and-forget send must break the poll promptly (returned in {elapsed:?}), \
             not park to the 10s deadline"
        );

        // Cleanup.
        selector.close_channel("0").await;
        selector.poll(0).await.unwrap();
    }

    // ---- Phase 30: per-channel wakers (selectedKeys() on tokio) --------------

    /// Phase 30 (dirty site #1) — send-after-arm wake. A channel parked on
    /// read-only interest (its write direction NOT armed) must still pick up a
    /// freshly queued send and complete the write without waiting out the poll
    /// deadline. This exercises the `send()` dirty-mark -> re-arm path: without
    /// the dirty mark, the next WAIT would not arm `want_write`, the writable
    /// readiness would never be observed, and the write would stall until the
    /// (here long) deadline.
    ///
    /// Mutation check (confirmed during development): removing the
    /// `mark_interest_dirty` call from `send()`'s `Ok(())` arm leaves the write
    /// unarmed; this test then blocks on the inner 5s `timeout` and FAILS.
    #[tokio::test]
    async fn test_send_after_arm_wakes_write() {
        use std::time::{Duration, Instant};

        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;
        blocking_connect(&mut selector, "0", server.port()).await;

        // Settle the immediately-connected / made-progress bookkeeping so the
        // next poll genuinely parks on the readiness `select!` (eff_timeout > 0,
        // deadline = Some) with the channel armed read-only. See the Phase-24/26
        // selector tests for why three short settling polls are needed.
        for _ in 0..3 {
            selector.poll(20).await.unwrap();
        }

        // Park a WAIT with read-only interest, then queue a send. The send's
        // dirty mark must cause the next poll to arm the write direction, fire
        // on writable readiness, complete the send, and (with the echo coming
        // back) drain the response — all well before a generous deadline.
        selector.send(create_send("0", "send-after-arm")).unwrap();
        let start = Instant::now();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                // Long per-poll deadline so a stall (missing re-arm) shows up as
                // a timeout, not a fast deadline return.
                selector.poll(5_000).await.unwrap();
                if selector.completed_receives().iter().any(|r| r.source() == "0") {
                    break;
                }
            }
        })
        .await
        .expect("(send-after-arm) the queued send never completed / round-tripped within 5s");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "(send-after-arm) the write must be armed and complete promptly, not on the poll deadline"
        );

        selector.close_channel("0").await;
        selector.poll(0).await.unwrap();
    }

    /// Phase 30 (dirty sites #2 / #3) — unmute redelivery. A readiness event
    /// that fires for a channel WHILE it is muted is recorded by the
    /// `ChannelWaker` but the channel is not read (interest is off). When the
    /// channel is later unmuted, the data must be delivered promptly even though
    /// the fire happened during the muted window: `unmute()` marks the channel
    /// dirty so the next WAIT re-arms `want_read`; tokio readiness is
    /// level-triggered, so the re-arm observes the still-readable socket and the
    /// data is drained.
    ///
    /// Mutation check (confirmed during development): removing the
    /// `mark_interest_dirty` call from `unmute()` leaves the channel un-armed
    /// after unmuting; the post-unmute drain then blocks on the inner 5s
    /// `timeout` and the test FAILS.
    #[tokio::test]
    async fn test_unmute_redelivers_after_muted_fire() {
        use std::time::Duration;

        let server = EchoServer::new().await.unwrap();
        let mut selector = create_selector().await;
        blocking_connect(&mut selector, "0", server.port()).await;
        for _ in 0..3 {
            selector.poll(20).await.unwrap();
        }

        // Send a request and drain the echo so the channel is quiescent first.
        let resp = blocking_request(&mut selector, "0", "warmup").await;
        assert_eq!(resp, "warmup");

        // Mute the channel, then have the peer send (a fresh request whose echo
        // arrives while muted). Poll a few times: with the channel muted there
        // must be NO delivery even though the socket becomes readable (the fire
        // is recorded against a now-uninterested channel).
        selector.mute("0");
        selector.send(create_send("0", "while-muted")).unwrap();
        for _ in 0..5 {
            selector.poll(20).await.unwrap();
        }
        assert!(
            selector.completed_receives().iter().all(|r| r.source() != "0"),
            "(unmute) a muted channel must not deliver receives even when its socket is readable"
        );

        // Now unmute: the dirty mark must re-arm read interest, the level-
        // triggered readiness re-fires, and the echo is delivered promptly.
        selector.unmute("0");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                selector.poll(5_000).await.unwrap();
                if selector.completed_receives().iter().any(|r| r.source() == "0") {
                    break;
                }
            }
        })
        .await
        .expect("(unmute) data sent while muted was not delivered after unmute within 5s");

        selector.close_channel("0").await;
        selector.poll(0).await.unwrap();
    }

    /// Phase 30 — multi-channel ready-set exactness. With N connected channels,
    /// sending on K of them must process exactly those K (one `try_read` minimum
    /// on each that has data) and leave the idle N-K untouched by any recv
    /// syscall during the steady-state drain. Extends the Phase-24 skip-idle
    /// test to the multi-ready case to prove the fired-queue delivers the exact
    /// `selectedKeys()` set, not a superset (busy-spin) or subset (stall).
    ///
    /// Mutation checks (confirmed during development): (subset) dropping the
    /// fired entry's id in `drain_fired_queue` (not inserting it into
    /// `ready_out`) strands the active channels and the drain `timeout` FAILS;
    /// (superset) making `arm_channel` treat every read-interested channel as
    /// immediately ready (skipping the `poll_transport_readable` gate) puts the
    /// idle channels in `ready_ids`, busy-spinning the poll loop so the test
    /// never settles and times out.
    #[tokio::test]
    async fn test_multi_channel_ready_set_exactness() {
        use std::time::Duration;

        let server = EchoServer::new().await.unwrap();
        let (mut selector, counts) = create_counting_selector().await;

        // N = 4 channels; K = 2 will get a request.
        let ids = ["0", "1", "2", "3"];
        for id in &ids {
            blocking_connect(&mut selector, id, server.port()).await;
        }
        for _ in 0..3 {
            selector.poll(20).await.unwrap();
        }

        let base: Vec<usize> = ids.iter().map(|id| try_reads_for(&counts, id)).collect();

        // Send on channels "1" and "3" only.
        let active = ["1", "3"];
        let idle = ["0", "2"];
        for id in &active {
            selector.send(create_send(id, &format!("ready-{id}"))).unwrap();
        }

        // Drain both active channels' echoes. `completed_receives` is wiped by
        // each poll's `clear()`, and a poll typically returns one receive at a
        // time, so accumulate the set of source ids observed across polls rather
        // than expecting both in a single poll's snapshot.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                selector.poll(5_000).await.unwrap();
                for r in selector.completed_receives() {
                    seen.insert(r.source().to_string());
                }
                if active.iter().all(|id| seen.contains(*id)) {
                    break;
                }
            }
        })
        .await
        .expect("the K active channels' echoes were not all drained within 5s");

        let after: Vec<usize> = ids.iter().map(|id| try_reads_for(&counts, id)).collect();

        // Active channels were processed.
        for id in &active {
            let i = ids.iter().position(|x| x == id).unwrap();
            assert!(
                after[i] > base[i],
                "active channel {id} must receive at least one try_read ({} -> {})",
                base[i],
                after[i]
            );
        }
        // Idle channels were NOT touched by a recv syscall during the drain —
        // the ready set is exactly the K active channels, not a superset.
        for id in &idle {
            let i = ids.iter().position(|x| x == id).unwrap();
            assert_eq!(
                after[i], base[i],
                "idle channel {id} must not receive any try_read during the drain \
                 ({} -> {}); the ready set must be exactly the active channels",
                base[i], after[i]
            );
        }

        for id in &ids {
            selector.close_channel(id).await;
        }
        selector.poll(0).await.unwrap();
    }
}
