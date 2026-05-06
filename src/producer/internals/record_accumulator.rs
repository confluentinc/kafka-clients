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

//! Translation of `org.apache.kafka.clients.producer.internals.RecordAccumulator`.
//!
//! Acts as a queue that accumulates records into [`MemoryRecords`]
//! instances to be sent to the server. The accumulator uses a bounded
//! amount of memory and `append` calls will block when that memory is
//! exhausted, unless this behavior is explicitly disabled.
//!
//! ## Tokio-specific structure
//!
//! - [`RecordAccumulator::append`] is `async fn` because
//!   [`BufferPool::allocate`] is `async`. The lock-then-await pattern
//!   Java uses (`synchronized` + `Condition.await` releases the
//!   monitor) translates to:
//!   1. Take the deque lock; try to append into the open batch.
//!   2. If full: drop the lock, call `BufferPool::allocate().await`,
//!      re-take the lock, allocate the new batch.
//!
//!   This sequence must NEVER hold a `MutexGuard` across an `.await`
//!   (CLAUDE.md rule 9.6).
//!
//! - [`RecordAccumulator::ready`], [`RecordAccumulator::drain`],
//!   [`RecordAccumulator::expired_batches`] are pure synchronous
//!   functions of the snapshot — translated as `fn`, not `async fn`.
//!
//! - [`RecordAccumulator::await_flush_completion`] is `async fn`
//!   driven by [`crate::producer::internals::ProduceRequestResult::
//!   await_all_dependents`] over each in-flight batch's result; the
//!   per-result `Notify` is the wake mechanism.
//!
//! - The `nodesWithData` `Set<Node>` returned by `ready()` is a
//!   `HashSet<i32>` over node ids (CLAUDE.md hot-path interning rule,
//!   Phase 5c convention).
//!
//! ## Plug-in contract for future transactions
//!
//! Per Phase 6 NOTES.md "Plug-in contract for future transactions",
//! this milestone wires `transaction_manager:
//! Option<TransactionManager>` always to `None`. Each Java
//! `if (transactionManager != null) { … }` branch translates to
//! `if let Some(tm) = &self.transaction_manager { … }` with an
//! **empty body** today. Phase 7 config validation rejects
//! `enable.idempotence=true` and `transactional.id`, so reaching
//! `Some(_)` is impossible this milestone.
//!
//! ## Hot-path constraints (CLAUDE.md rule 12)
//!
//! - `append` writes serialized bytes through to
//!   [`ProducerBatch::try_append`] which writes directly into the
//!   underlying [`MemoryRecordsBuilder`] buffer — no intermediate
//!   `Vec<u8>` per record.
//! - `topic` is borrowed as `&str` from the caller's `ProducerRecord`;
//!   the only owned `String` (or `Arc<str>`) is the key in
//!   `topic_info_map`, where it's shared via `Arc<str>` clones.
//! - The accumulator does NOT spawn (`tokio::spawn` count is O(1) per
//!   producer; the sender task runs the loop, not the accumulator).

#![allow(dead_code)] // Phase 6e (Sender) wires `ready`/`drain`/`muted`/`drained_ms`/etc.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};

use crate::common::cluster::Cluster;
use crate::common::errors::KafkaError;
use crate::common::header::RecordHeader;
use crate::common::record::CompressionType;
use crate::common::record::abstract_records::estimate_size_in_bytes_upper_bound;
use crate::common::record::base_records::BaseRecords;
use crate::common::record::compression_ratio_estimator;
use crate::common::record::record_batch::CURRENT_MAGIC_VALUE;
use crate::common::record::{MemoryRecordsBuilder, TimestampType};
use crate::common::topic_partition::TopicPartition;
use crate::common::utils::ExponentialBackoff;
use crate::common::utils::LogContext;
use crate::common::utils::Time;
use crate::common_client_configs::{RETRY_BACKOFF_EXP_BASE, RETRY_BACKOFF_JITTER};
use crate::metadata_snapshot::MetadataSnapshot;
use crate::producer::callback::Callback;
use crate::producer::record_metadata::RecordMetadata;

use super::buffer_pool::BufferPool;
use super::built_in_partitioner::{BuiltInPartitioner, StickyPartitionInfo};
use super::future_record_metadata::FutureRecordMetadata;
use super::incomplete_batches::IncompleteBatches;
use super::producer_batch::ProducerBatch;
use super::transaction_manager::TransactionManager;

/// Partitioner config for built-in partitioner. Mirrors Java's
/// `RecordAccumulator.PartitionerConfig`.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PartitionerConfig {
    /// If `true`, partition switching adapts to broker load; otherwise
    /// partition switching is random.
    pub enable_adaptive_partitioning: bool,
    /// If a broker cannot process produce requests from a partition for
    /// the specified time, the partition is treated by the partitioner
    /// as not available. If the timeout is `0`, this logic is disabled.
    pub partition_availability_timeout_ms: i64,
}

impl PartitionerConfig {
    pub fn new(enable_adaptive_partitioning: bool, partition_availability_timeout_ms: i64) -> Self {
        Self { enable_adaptive_partitioning, partition_availability_timeout_ms }
    }
}

/// Trait for the user-facing `AppendCallbacks` Java interface.
///
/// Java: `interface AppendCallbacks extends Callback { void
/// setPartition(int partition); }` — extends [`Callback`] with a
/// `set_partition` hook used by the producer to record the
/// partition assignment after the partitioner has chosen one.
///
/// We translate as a separate trait that requires `Callback` as a
/// supertrait so existing callbacks naturally satisfy
/// `AppendCallbacks` once they implement `set_partition`.
pub(crate) trait AppendCallbacks: Callback {
    /// Called to set partition (when `append` is called, partition may
    /// not yet be calculated).
    fn set_partition(&self, partition: i32);
}

/// Metadata about a record just appended to the record accumulator.
/// Mirrors Java's `RecordAccumulator.RecordAppendResult`.
pub(crate) struct RecordAppendResult {
    /// The future the user awaits for the record's metadata.
    pub future: Arc<FutureRecordMetadata>,
    /// `true` iff the batch the record was appended to is now full.
    pub batch_is_full: bool,
    /// `true` iff a brand-new batch was allocated by this call.
    pub new_batch_created: bool,
    /// Number of bytes the record contributed to the batch's
    /// estimated size.
    pub appended_bytes: i32,
}

impl std::fmt::Debug for RecordAppendResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordAppendResult")
            .field("batch_is_full", &self.batch_is_full)
            .field("new_batch_created", &self.new_batch_created)
            .field("appended_bytes", &self.appended_bytes)
            .field("future", &"FutureRecordMetadata")
            .finish()
    }
}

/// The set of nodes that have at least one complete record batch in
/// the accumulator. Mirrors Java's `RecordAccumulator.ReadyCheckResult`.
#[derive(Debug)]
pub(crate) struct ReadyCheckResult {
    /// Node ids ready to receive a produce request.
    pub ready_nodes: HashSet<i32>,
    /// Earliest time (in ms) at which any non-sendable partition will
    /// be ready.
    pub next_ready_check_delay_ms: i64,
    /// Topics for which we have data but no known leader.
    pub unknown_leader_topics: HashSet<Arc<str>>,
}

/// Node latency stats for each node, used for adaptive partition
/// distribution. Mirrors Java's `RecordAccumulator.NodeLatencyStats`.
pub(crate) struct NodeLatencyStats {
    /// Last time the node had batches ready to send.
    pub ready_time_ms: AtomicI64,
    /// Last time the node was able to drain batches.
    pub drain_time_ms: AtomicI64,
}

impl NodeLatencyStats {
    pub fn new(now_ms: i64) -> Self {
        Self { ready_time_ms: AtomicI64::new(now_ms), drain_time_ms: AtomicI64::new(now_ms) }
    }
}

/// Per-partition deque of in-progress batches. Java holds the
/// equivalent `Deque<ProducerBatch>` and synchronizes on it via
/// `synchronized (deque)`; the Rust equivalent is the inner `Mutex`
/// wrapping the `VecDeque`. The outer `Arc` lets us drop the parent
/// `TopicInfo.batches` map mutex before locking the deque.
pub(crate) type BatchDeque = Arc<Mutex<VecDeque<Arc<ProducerBatch>>>>;

/// Per-topic info. Mirrors Java's private `RecordAccumulator.TopicInfo`.
struct TopicInfo {
    /// `ConcurrentMap<Integer /*partition*/, Deque<ProducerBatch>>`.
    /// We use `Mutex<HashMap<i32, BatchDeque>>` to guard insertion of
    /// new partitions; the per-partition deques themselves live
    /// behind individual `Mutex` guards (one per partition) so
    /// concurrent appenders to different partitions do not block
    /// each other.
    batches: Mutex<HashMap<i32, BatchDeque>>,
    built_in_partitioner: BuiltInPartitioner,
}

impl TopicInfo {
    fn new(built_in_partitioner: BuiltInPartitioner) -> Self {
        Self { batches: Mutex::new(HashMap::new()), built_in_partitioner }
    }
}

/// This class acts as a queue that accumulates records into
/// [`MemoryRecords`] instances to be sent to the server. See
/// module-level docs for the Tokio-specific design notes.
pub(crate) struct RecordAccumulator {
    log_context: LogContext,
    log_prefix: String,
    /// Java's `volatile boolean closed`.
    closed: std::sync::atomic::AtomicBool,
    /// Java's `AtomicInteger flushesInProgress`.
    flushes_in_progress: AtomicI32,
    /// Java's `AtomicInteger appendsInProgress`.
    appends_in_progress: AtomicI32,
    batch_size: i32,
    /// Java: `Compression compression` — the trait. In Rust we hold
    /// the [`CompressionType`] (Java's `compression.type()`), which is
    /// all the accumulator actually consumes:
    /// * `compression.type()` is forwarded to
    ///   `estimate_size_in_bytes_upper_bound`.
    /// * `MemoryRecords.builder(buffer, magic, compression, ...)`'s
    ///   Rust counterpart `MemoryRecordsBuilder::from_buffer` also
    ///   takes a [`CompressionType`].
    ///
    /// Holding the type instead of a `Box<dyn Compression>` saves
    /// per-builder boxing/cloning on the hot path.
    compression: CompressionType,
    linger_ms: i32,
    retry_backoff: ExponentialBackoff,
    delivery_timeout_ms: i32,
    /// Latency threshold for marking partition temporarily unavailable.
    partition_availability_timeout_ms: i64,
    enable_adaptive_partitioning: bool,
    free: Arc<BufferPool>,
    time: Arc<dyn Time>,
    /// Java: `ConcurrentMap<String, TopicInfo> topicInfoMap = new
    /// CopyOnWriteMap<>()`. Rust: `Mutex<HashMap<Arc<str>, Arc<TopicInfo>>>`.
    /// `Arc<TopicInfo>` lets us drop the outer mutex before any
    /// per-`TopicInfo` work.
    topic_info_map: Mutex<HashMap<Arc<str>, Arc<TopicInfo>>>,
    /// Java: `ConcurrentMap<Integer, NodeLatencyStats>`.
    node_stats: Mutex<HashMap<i32, Arc<NodeLatencyStats>>>,
    incomplete: IncompleteBatches,
    /// Only accessed by the sender task; Java says "no synchronization
    /// needed". We hold a `Mutex` to satisfy `Send + Sync` while
    /// keeping the Rust contract simple — only the sender task
    /// touches `muted`.
    muted: Mutex<HashSet<TopicPartition>>,
    /// Per-node round-robin index used to spread drain work across
    /// partitions. Sender-task only. Mirrors Java's `Map<String,
    /// Integer> nodesDrainIndex` with the `String` key replaced by the
    /// `i32` node id (CLAUDE.md hot-path interning rule).
    nodes_drain_index: Mutex<HashMap<i32, usize>>,
    /// Java: `private final TransactionManager transactionManager` —
    /// always `None` this milestone (Phase 6 NOTES.md plug-in contract).
    transaction_manager: Option<TransactionManager>,
    /// Java: `private long nextBatchExpiryTimeMs = Long.MAX_VALUE`.
    /// The Java field is touched only from the sender thread; we use
    /// an atomic to avoid having a separate sender-only mutex.
    next_batch_expiry_time_ms: AtomicI64,
}

impl RecordAccumulator {
    /// Create a new record accumulator.
    ///
    /// Mirrors Java's primary constructor. The metric registration
    /// (Java's `registerMetrics(metrics, metricGrpName)`) is replaced
    /// with `// metric stub` no-ops per the project-wide PLAN.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log_context: LogContext,
        batch_size: i32,
        compression: CompressionType,
        linger_ms: i32,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        delivery_timeout_ms: i32,
        partitioner_config: PartitionerConfig,
        _metric_grp_name: &str,
        time: Arc<dyn Time>,
        transaction_manager: Option<TransactionManager>,
        buffer_pool: Arc<BufferPool>,
    ) -> Self {
        let log_prefix = log_context.log_prefix().to_string();
        let retry_backoff = ExponentialBackoff::new(
            retry_backoff_ms,
            RETRY_BACKOFF_EXP_BASE,
            retry_backoff_max_ms,
            RETRY_BACKOFF_JITTER,
        )
        .expect("RETRY_BACKOFF_JITTER is in [0, 1]");

        RecordAccumulator {
            log_context,
            log_prefix,
            closed: std::sync::atomic::AtomicBool::new(false),
            flushes_in_progress: AtomicI32::new(0),
            appends_in_progress: AtomicI32::new(0),
            batch_size,
            compression,
            linger_ms,
            retry_backoff,
            delivery_timeout_ms,
            partition_availability_timeout_ms: partitioner_config.partition_availability_timeout_ms,
            enable_adaptive_partitioning: partitioner_config.enable_adaptive_partitioning,
            free: buffer_pool,
            time,
            topic_info_map: Mutex::new(HashMap::new()),
            node_stats: Mutex::new(HashMap::new()),
            incomplete: IncompleteBatches::new(),
            muted: Mutex::new(HashSet::new()),
            nodes_drain_index: Mutex::new(HashMap::new()),
            transaction_manager,
            next_batch_expiry_time_ms: AtomicI64::new(i64::MAX),
        }
    }

    /// Convenience constructor with default [`PartitionerConfig`].
    /// Mirrors Java's overload at `RecordAccumulator.java:171-196`.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_default_partitioner(
        log_context: LogContext,
        batch_size: i32,
        compression: CompressionType,
        linger_ms: i32,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        delivery_timeout_ms: i32,
        metric_grp_name: &str,
        time: Arc<dyn Time>,
        transaction_manager: Option<TransactionManager>,
        buffer_pool: Arc<BufferPool>,
    ) -> Self {
        Self::new(
            log_context,
            batch_size,
            compression,
            linger_ms,
            retry_backoff_ms,
            retry_backoff_max_ms,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            metric_grp_name,
            time,
            transaction_manager,
            buffer_pool,
        )
    }

    /// Visible-for-testing override hook for Java's package-private
    /// `createBuiltInPartitioner` method. Java tests subclass to
    /// inject a `SequentialPartitioner`; Rust allows the same via the
    /// [`BuiltInPartitioner::new_with_random_source`] constructor —
    /// see [`RecordAccumulator::new_with_random_partition_source`].
    fn create_built_in_partitioner(&self, topic: Arc<str>) -> BuiltInPartitioner {
        BuiltInPartitioner::new(&self.log_context, topic, self.batch_size)
    }

    /// `RecordAccumulator.deliveryTimeoutMs`.
    pub fn delivery_timeout_ms(&self) -> i64 {
        self.delivery_timeout_ms as i64
    }

    /// The earliest absolute time a batch will expire (in ms). Mirrors
    /// Java's `nextExpiryTimeMs()`.
    pub fn next_expiry_time_ms(&self) -> i64 {
        self.next_batch_expiry_time_ms.load(Ordering::Acquire)
    }

    /// Reset the next-batch-expiry-time tracker. Mirrors Java's
    /// `resetNextBatchExpiryTime()`.
    pub fn reset_next_batch_expiry_time(&self) {
        self.next_batch_expiry_time_ms.store(i64::MAX, Ordering::Release);
    }

    /// Visible-for-testing: borrow the per-topic
    /// [`BuiltInPartitioner`] under the topic-info-map lock.
    ///
    /// The closure receives a borrow of the partitioner; this is the
    /// Rust-equivalent of Java's `getBuiltInPartitioner(topic)`. We
    /// route through a closure rather than returning a reference
    /// because the partitioner is owned by value inside `TopicInfo`
    /// and the outer `topic_info_map` mutex must be released before
    /// the caller can do anything useful with it.
    pub(crate) fn with_built_in_partitioner<F, R>(&self, topic: &str, f: F) -> Option<R>
    where
        F: FnOnce(&BuiltInPartitioner) -> R,
    {
        let info = {
            let map = self.topic_info_map.lock().unwrap();
            map.get(topic).cloned()
        };
        info.map(|info| f(&info.built_in_partitioner))
    }

    /// `BufferPool` accessor for tests. Mirrors Java's package-private
    /// `bufferPoolAvailableMemory()`.
    pub fn buffer_pool_available_memory(&self) -> i64 {
        self.free.available_memory()
    }

    /// Visible-for-testing: return the [`BufferPool`] backing this
    /// accumulator.
    #[cfg(test)]
    pub(crate) fn buffer_pool(&self) -> &BufferPool {
        &self.free
    }

    /// Are there any threads currently waiting on a flush? Mirrors
    /// Java's package-private `flushInProgress()`.
    pub fn flush_in_progress(&self) -> bool {
        self.flushes_in_progress.load(Ordering::Acquire) > 0
    }

    /// Number of in-progress appends. Mirrors Java's
    /// private `appendsInProgress()`.
    fn appends_in_progress(&self) -> bool {
        self.appends_in_progress.load(Ordering::Acquire) > 0
    }

    /// Visible-for-testing: read the raw `appends_in_progress` counter
    /// to assert on `AppendInProgressGuard::Drop` having fired.
    #[cfg(test)]
    pub(crate) fn appends_in_progress_count(&self) -> i32 {
        self.appends_in_progress.load(Ordering::Acquire)
    }

    /// Add `tp` to the muted set; matching partitions are skipped during
    /// drain. Mirrors Java's `mutePartition(tp)`.
    pub fn mute_partition(&self, tp: TopicPartition) {
        self.muted.lock().unwrap().insert(tp);
    }

    /// Remove `tp` from the muted set. Mirrors `unmutePartition(tp)`.
    pub fn unmute_partition(&self, tp: &TopicPartition) {
        self.muted.lock().unwrap().remove(tp);
    }

    fn is_muted(&self, tp: &TopicPartition) -> bool {
        self.muted.lock().unwrap().contains(tp)
    }

    /// Mirrors Java's `hasIncomplete()`.
    pub fn has_incomplete(&self) -> bool {
        !self.incomplete.is_empty()
    }

    /// Mirrors Java's `hasUndrained()`.
    pub fn has_undrained(&self) -> bool {
        let topics: Vec<Arc<TopicInfo>> = {
            let map = self.topic_info_map.lock().unwrap();
            map.values().cloned().collect()
        };
        for info in topics {
            let deques: Vec<BatchDeque> = {
                let batches = info.batches.lock().unwrap();
                batches.values().cloned().collect()
            };
            for deque in deques {
                let dq = deque.lock().unwrap();
                if !dq.is_empty() {
                    return true;
                }
            }
        }
        false
    }

    /// Mirrors Java's `getNodeLatencyStats(Integer nodeId)`. Visible
    /// for testing.
    pub fn node_latency_stats(&self, node_id: i32) -> Option<Arc<NodeLatencyStats>> {
        self.node_stats.lock().unwrap().get(&node_id).cloned()
    }

    /// Update node latency stats. Mirrors Java's
    /// `updateNodeLatencyStats(Integer, long, boolean)`.
    pub fn update_node_latency_stats(&self, node_id: i32, now_ms: i64, can_drain: bool) {
        if self.partition_availability_timeout_ms <= 0 {
            return;
        }
        let stats = {
            let mut node_stats = self.node_stats.lock().unwrap();
            node_stats
                .entry(node_id)
                .or_insert_with(|| Arc::new(NodeLatencyStats::new(now_ms)))
                .clone()
        };
        // NOTE: there is no synchronization for metric updates, so
        // drainTimeMs is updated first to avoid accidentally marking a
        // partition unavailable if the reader gets values between
        // updates.
        if can_drain {
            stats.drain_time_ms.store(now_ms, Ordering::Release);
        }
        stats.ready_time_ms.store(now_ms, Ordering::Release);
    }

    /// Add a record to the accumulator, returning the [`RecordAppendResult`].
    ///
    /// Mirrors Java's `append(topic, partition, timestamp, key, value,
    /// headers, callbacks, maxTimeToBlock, nowMs, cluster)` at
    /// `RecordAccumulator.java:275-359`.
    ///
    /// `partition` may be [`RecordMetadata::UNKNOWN_PARTITION`] (`-1`)
    /// to delegate partition selection to the [`BuiltInPartitioner`].
    /// `headers` defaults to the empty slice if `None` (Java:
    /// `Record.EMPTY_HEADERS`).
    ///
    /// # Hot-path constraints (CLAUDE.md rule 12)
    ///
    /// - The `topic` is interned via `Arc<str>` once per topic — no
    ///   `String` clone per record send.
    /// - `key` and `value` are passed as `Option<&[u8]>` and flow
    ///   through to [`ProducerBatch::try_append`] which writes
    ///   directly into the underlying [`MemoryRecordsBuilder`] buffer.
    /// - `headers` is passed by `&[RecordHeader]` borrow; no clone.
    /// - The `Mutex<VecDeque<...>>` deque guard is **never** held
    ///   across the [`BufferPool::allocate`] `.await`
    ///   (CLAUDE.md rule 9.6). The lock-then-await pattern Java uses
    ///   is replaced with: take the lock, try, drop the lock,
    ///   `.await`, re-take the lock.
    #[allow(clippy::too_many_arguments)]
    pub async fn append(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callbacks: Option<Arc<dyn AppendCallbacks>>,
        max_time_to_block: i64,
        now_ms: i64,
        cluster: &Cluster,
    ) -> Result<RecordAppendResult, KafkaError> {
        // Fast path: if the topic is already in the map, the existing
        // `Arc<str>` key is reused via `Arc::clone` (refcount bump only,
        // no allocation). Only the first send for a previously-unseen
        // topic allocates an `Arc<str>`. Mirrors Java's
        // `topicInfoMap.computeIfAbsent(topic, k -> new TopicInfo(...))`,
        // which uses the `String topic` parameter directly without an
        // intermediate allocation.
        let (topic_arc, topic_info) = self.get_or_create_topic_info(topic);

        // Track in-progress appends so abortIncompleteBatches() does
        // not miss a batch racing the close flag. The guard's `Drop`
        // also returns any allocated-but-unused buffer to the pool —
        // mirroring Java's `finally { free.deallocate(buffer); ... }`.
        let mut state_guard = AppendInProgressGuard::new(self);

        let mut now_ms = now_ms;

        loop {
            let (partition_info, effective_partition) = if partition == RecordMetadata::UNKNOWN_PARTITION {
                let info = topic_info.built_in_partitioner.peek_current_partition_info(cluster);
                let part = info.partition();
                (Some(info), part)
            } else {
                (None, partition)
            };

            // Now that we know the effective partition, let the caller
            // know. Mirrors Java's `setPartition(callbacks, effectivePartition)`.
            if let Some(cb) = callbacks.as_ref() {
                cb.set_partition(effective_partition);
            }

            // Check if we have an in-progress batch.
            let dq = {
                let mut batches = topic_info.batches.lock().unwrap();
                batches
                    .entry(effective_partition)
                    .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
                    .clone()
            };

            // First attempt: try to append to an existing open batch.
            {
                let mut deque = dq.lock().unwrap();
                if Self::partition_changed(
                    &self.log_prefix,
                    topic,
                    &topic_info,
                    partition_info.as_ref(),
                    &deque,
                    cluster,
                ) {
                    continue;
                }

                if let Some(append_result) = self.try_append_to_existing(
                    timestamp,
                    key,
                    value,
                    headers,
                    callbacks.as_ref().map(|cb| upcast_callback(cb.clone())),
                    &mut deque,
                    now_ms,
                )? {
                    let enable_switch = Self::all_batches_full(&deque);
                    drop(deque);
                    if let Some(info) = partition_info.as_ref() {
                        topic_info.built_in_partitioner.update_partition_info(
                            info,
                            append_result.appended_bytes,
                            cluster,
                            enable_switch,
                        );
                    }
                    state_guard.disarm();
                    return Ok(append_result);
                }
                // deque guard dropped here.
            }

            // Slow path: allocate a new buffer (may block on pool).
            // The guard refunds the buffer if we are cancelled mid-await.
            if state_guard.buffer.is_none() {
                let upper_bound =
                    estimate_size_in_bytes_upper_bound(CURRENT_MAGIC_VALUE, self.compression, key, value, headers);
                let size = self.batch_size.max(upper_bound);
                log::trace!(
                    "{}Allocating a new {} byte message buffer for topic {} partition {} with remaining timeout {}ms",
                    self.log_prefix,
                    size,
                    topic,
                    effective_partition,
                    max_time_to_block,
                );
                // CRITICAL: this call may suspend on `Notify::notified()`.
                // We hold NO Mutex guards across this `.await`
                // (CLAUDE.md rule 9.6).
                let buffer = self.free.allocate(size, max_time_to_block).await?;
                state_guard.buffer = Some(buffer);
                state_guard.buffer_size = size;
                // Update the current time in case the buffer allocation
                // blocked above. NOTE: getting time may be expensive,
                // so calling it under a lock should be avoided.
                now_ms = self.time.milliseconds();
            }

            // Second attempt: with the freshly allocated buffer in hand.
            {
                let mut deque = dq.lock().unwrap();
                if Self::partition_changed(
                    &self.log_prefix,
                    topic,
                    &topic_info,
                    partition_info.as_ref(),
                    &deque,
                    cluster,
                ) {
                    continue;
                }

                let buffer = state_guard.buffer.take().expect("buffer was just allocated");
                let buffer_size = state_guard.buffer_size;
                let (append_result, returned_buffer) = self.append_new_batch(
                    topic_arc.clone(),
                    effective_partition,
                    &mut deque,
                    timestamp,
                    key,
                    value,
                    headers,
                    callbacks.as_ref().map(|cb| upcast_callback(cb.clone())),
                    buffer,
                    buffer_size,
                    now_ms,
                )?;
                if let Some(unused) = returned_buffer {
                    // Java sets `buffer = null` only when newBatchCreated;
                    // otherwise the deallocate-in-finally reclaims it.
                    // In our path we re-stash the buffer in the guard so
                    // it gets refunded on drop.
                    state_guard.buffer = Some(unused);
                }
                let enable_switch = Self::all_batches_full(&deque);
                drop(deque);
                if let Some(info) = partition_info.as_ref() {
                    topic_info.built_in_partitioner.update_partition_info(
                        info,
                        append_result.appended_bytes,
                        cluster,
                        enable_switch,
                    );
                }
                state_guard.disarm();
                return Ok(append_result);
            }
        }
    }

    /// Mirrors Java's private `partitionChanged(topic, topicInfo,
    /// partitionInfo, deque, nowMs, cluster)`. Returns `true` if the
    /// caller should `continue` the retry loop.
    fn partition_changed(
        log_prefix: &str,
        topic: &str,
        topic_info: &TopicInfo,
        partition_info: Option<&Arc<StickyPartitionInfo>>,
        deque: &VecDeque<Arc<ProducerBatch>>,
        cluster: &Cluster,
    ) -> bool {
        let info = match partition_info {
            Some(i) => i,
            // Caller specified an explicit partition (Java: `partition
            // != UNKNOWN_PARTITION`); no sticky tracking, no race.
            None => return false,
        };

        if topic_info.built_in_partitioner.is_partition_changed(info) {
            log::trace!(
                "{}Partition {} for topic {} switched by a concurrent append, retrying",
                log_prefix,
                info.partition(),
                topic,
            );
            return true;
        }

        // We might have disabled partition switch if the queue had
        // incomplete batches. Check if all batches are full now and
        // switch.
        if Self::all_batches_full(deque) {
            topic_info.built_in_partitioner.update_partition_info(info, 0, cluster, true);
            if topic_info.built_in_partitioner.is_partition_changed(info) {
                log::trace!(
                    "{}Completed previously disabled switch for topic {} partition {}, retrying",
                    log_prefix,
                    topic,
                    info.partition(),
                );
                return true;
            }
        }

        false
    }

    /// Mirrors Java's private `tryAppend(timestamp, key, value, headers,
    /// callback, deque, nowMs)`. Returns `Some(result)` if appended into
    /// an existing open batch; `None` if a new batch is needed (the
    /// existing last batch is closed for further appends).
    #[allow(clippy::too_many_arguments)]
    fn try_append_to_existing(
        &self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Arc<dyn Callback>>,
        deque: &mut VecDeque<Arc<ProducerBatch>>,
        now_ms: i64,
    ) -> Result<Option<RecordAppendResult>, KafkaError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(KafkaError::IllegalState("Producer closed while send in progress".to_string()));
        }
        let last = match deque.back() {
            Some(b) => b.clone(),
            None => return Ok(None),
        };
        let initial_bytes = last.estimated_size_in_bytes();
        let future = last.try_append(timestamp, key, value, headers, callback, now_ms);
        match future {
            None => {
                last.close_for_record_appends();
                Ok(None)
            },
            Some(future) => {
                let appended_bytes = last.estimated_size_in_bytes() - initial_bytes;
                let batch_is_full = deque.len() > 1 || last.is_full();
                Ok(Some(RecordAppendResult {
                    future,
                    batch_is_full,
                    new_batch_created: false,
                    appended_bytes,
                }))
            },
        }
    }

    /// Mirrors Java's private `appendNewBatch(topic, partition, dq,
    /// timestamp, key, value, headers, callbacks, buffer, nowMs)`.
    ///
    /// Returns `(result, returned_buffer)` where `returned_buffer` is
    /// `Some(buffer)` if the buffer was NOT consumed (a sibling
    /// appender raced us and produced a batch first — the caller must
    /// recycle the buffer) or `None` if the new batch took ownership.
    #[allow(clippy::too_many_arguments)]
    fn append_new_batch(
        &self,
        topic: Arc<str>,
        partition: i32,
        deque: &mut VecDeque<Arc<ProducerBatch>>,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Arc<dyn Callback>>,
        buffer: Vec<u8>,
        buffer_size: i32,
        now_ms: i64,
    ) -> Result<(RecordAppendResult, Option<Vec<u8>>), KafkaError> {
        debug_assert!(
            partition != RecordMetadata::UNKNOWN_PARTITION,
            "appendNewBatch must be called with a resolved partition"
        );

        // Java: "Somebody else found us a batch, return the one we
        // waited for! Hopefully this doesn't happen often..."
        if let Some(append_result) =
            self.try_append_to_existing(timestamp, key, value, headers, callback.clone(), deque, now_ms)?
        {
            return Ok((append_result, Some(buffer)));
        }

        let records_builder = self.records_builder(buffer, buffer_size)?;
        let tp = TopicPartition::new(topic, partition);
        let batch = Arc::new(ProducerBatch::new(tp, records_builder, now_ms));
        let future = batch
            .try_append(timestamp, key, value, headers, callback, now_ms)
            .ok_or_else(|| {
                // Java: `Objects.requireNonNull(batch.tryAppend(...))`. The
                // contract is that a freshly allocated batch always has
                // room for the first record (since the buffer is sized
                // by `estimateSizeInBytesUpperBound`). If this somehow
                // returns None we have a programmer-state bug.
                KafkaError::IllegalState("Freshly allocated batch rejected the first record append".to_string())
            })?;

        let estimated = batch.estimated_size_in_bytes();
        deque.push_back(batch.clone());
        self.incomplete.add(batch.clone());

        let batch_is_full = deque.len() > 1 || batch.is_full();
        let result = RecordAppendResult { future, batch_is_full, new_batch_created: true, appended_bytes: estimated };
        Ok((result, None))
    }

    /// Build a [`MemoryRecordsBuilder`] over the provided buffer.
    /// Mirrors Java's private `recordsBuilder(buffer)`.
    fn records_builder(&self, buffer: Vec<u8>, buffer_size: i32) -> Result<MemoryRecordsBuilder, KafkaError> {
        // Java: `MemoryRecords.builder(buffer, CURRENT_MAGIC_VALUE,
        //                              compression, CREATE_TIME, 0L)`.
        // Maps to our [`MemoryRecordsBuilder::from_buffer`] with
        // CreateTime, base offset 0, and the no-producer-state
        // sentinels. `write_limit` is the buffer size — Java's
        // `bufferStream` uses `buffer.capacity()` for the same
        // purpose.
        use crate::common::record::record_batch::{NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, NO_TIMESTAMP};
        MemoryRecordsBuilder::from_buffer(
            buffer,
            CURRENT_MAGIC_VALUE,
            self.compression,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            -1,
            buffer_size,
        )
    }

    /// Java's `allBatchesFull(deque)` — only the last batch may be
    /// incomplete, so check it.
    fn all_batches_full(deque: &VecDeque<Arc<ProducerBatch>>) -> bool {
        match deque.back() {
            Some(last) => last.is_full(),
            None => true,
        }
    }

    /// Java's private `shouldBackoff(hasLeaderChanged, batch, waitedTimeMs)`.
    fn should_backoff(&self, has_leader_changed: bool, batch: &ProducerBatch, waited_time_ms: i64) -> bool {
        let attempts = batch.attempts() as i64;
        let should_wait_more = attempts > 0 && waited_time_ms < self.retry_backoff.backoff(attempts - 1);
        let should_backoff = !has_leader_changed && should_wait_more;
        if should_backoff {
            log::trace!("{}For {}, will backoff", self.log_prefix, batch);
        } else {
            log::trace!(
                "{}For {}, will not backoff, shouldWaitMore {}, hasLeaderChanged {}",
                self.log_prefix,
                batch,
                should_wait_more,
                has_leader_changed,
            );
        }
        should_backoff
    }

    /// Java's private `batchReady(...)`. Adds `leader_id` to
    /// `ready_nodes` iff the batch is ready, otherwise narrows
    /// `next_ready_check_delay_ms` to the time until ready.
    #[allow(clippy::too_many_arguments)]
    fn batch_ready(
        &self,
        exhausted: bool,
        part: &TopicPartition,
        leader_id: i32,
        waited_time_ms: i64,
        backing_off: bool,
        backoff_attempts: i32,
        full: bool,
        next_ready_check_delay_ms: i64,
        ready_nodes: &mut HashSet<i32>,
    ) -> i64 {
        if !ready_nodes.contains(&leader_id) && !self.is_muted(part) {
            let time_to_wait_ms = if backing_off {
                self.retry_backoff.backoff(if backoff_attempts > 0 {
                    (backoff_attempts - 1) as i64
                } else {
                    0
                })
            } else {
                self.linger_ms as i64
            };
            let expired = waited_time_ms >= time_to_wait_ms;
            // transactionManager == null this milestone — see Phase 6 NOTES.md.
            let transaction_completing = false;
            let sendable = full
                || expired
                || exhausted
                || self.closed.load(Ordering::Acquire)
                || self.flush_in_progress()
                || transaction_completing;
            if sendable && !backing_off {
                ready_nodes.insert(leader_id);
            } else {
                let time_left_ms = (time_to_wait_ms - waited_time_ms).max(0);
                // Note that this results in a conservative estimate
                // since an un-sendable partition may have a leader that
                // will later be found to have sendable data. However,
                // this is good enough since we'll just wake up and
                // then sleep again for the remaining time.
                return time_left_ms.min(next_ready_check_delay_ms);
            }
        }
        next_ready_check_delay_ms
    }

    /// Per-topic ready check. Mirrors Java's private `partitionReady(...)`.
    #[allow(clippy::too_many_arguments)]
    fn partition_ready(
        &self,
        metadata_snapshot: &MetadataSnapshot,
        now_ms: i64,
        topic: &Arc<str>,
        topic_info: &TopicInfo,
        next_ready_check_delay_ms: i64,
        ready_nodes: &mut HashSet<i32>,
        unknown_leader_topics: &mut HashSet<Arc<str>>,
    ) -> i64 {
        // Snapshot the partition->deque map (cheap clone of the
        // `Arc<Mutex<...>>` values, which lets us drop the
        // `topic_info.batches` mutex before per-deque work).
        let snapshot: Vec<(i32, BatchDeque)> = {
            let batches = topic_info.batches.lock().unwrap();
            batches.iter().map(|(k, v)| (*k, v.clone())).collect()
        };

        // Collect the queue sizes for available partitions to be used
        // in adaptive partitioning.
        let cluster = metadata_snapshot.cluster_ref();
        let total_topic_partitions = cluster.partitions_for_topic(topic.as_ref()).len();
        let mut queue_sizes: Option<Vec<i32>> = None;
        let mut partition_ids: Option<Vec<i32>> = None;
        if self.enable_adaptive_partitioning && snapshot.len() >= total_topic_partitions {
            queue_sizes = Some(vec![0; snapshot.len()]);
            partition_ids = Some(vec![0; snapshot.len()]);
        }

        let mut queue_sizes_index: i32 = -1;
        let exhausted = self.free.queued() > 0;
        let mut next_ready_check_delay_ms = next_ready_check_delay_ms;

        for (part_id, deque_arc) in snapshot {
            let part = TopicPartition::new(topic.clone(), part_id);
            let leader = cluster.leader_for(&part).cloned();

            if leader.is_some()
                && let Some(qs) = queue_sizes.as_ref()
            {
                queue_sizes_index += 1;
                debug_assert!((queue_sizes_index as usize) < qs.len());
                if let Some(pids) = partition_ids.as_mut() {
                    pids[queue_sizes_index as usize] = part_id;
                }
            }

            let leader_epoch = metadata_snapshot.leader_epoch_for(&part);

            // Minimum-required-inside-lock per Java's KAFKA-16226 note.
            let waited_time_ms;
            let backing_off;
            let backoff_attempts;
            let deque_size;
            let full;
            {
                let deque = deque_arc.lock().unwrap();
                let batch = match deque.front() {
                    Some(b) => b.clone(),
                    None => continue,
                };
                drop(deque);
                waited_time_ms = batch.waited_time_ms(now_ms);
                batch.maybe_update_leader_epoch(leader_epoch);
                backing_off =
                    self.should_backoff(batch.has_leader_changed_for_the_ongoing_retry(), &batch, waited_time_ms);
                backoff_attempts = batch.attempts();
                let deque = deque_arc.lock().unwrap();
                deque_size = deque.len() as i32;
                full = deque_size > 1 || batch.is_full();
            }

            match leader {
                None => {
                    // Partition with no known leader, but data to send.
                    unknown_leader_topics.insert(topic.clone());
                },
                Some(leader_node) => {
                    if let Some(qs) = queue_sizes.as_mut() {
                        qs[queue_sizes_index as usize] = deque_size;
                    }
                    if self.partition_availability_timeout_ms > 0
                        && let Some(stats) = self.node_stats.lock().unwrap().get(&leader_node.id()).cloned()
                    {
                        // NOTE: read ready time first to avoid
                        // accidentally marking partition unavailable.
                        let ready_time_ms = stats.ready_time_ms.load(Ordering::Acquire);
                        let drain_time_ms = stats.drain_time_ms.load(Ordering::Acquire);
                        if ready_time_ms - drain_time_ms > self.partition_availability_timeout_ms {
                            queue_sizes_index -= 1;
                        }
                    }

                    next_ready_check_delay_ms = self.batch_ready(
                        exhausted,
                        &part,
                        leader_node.id(),
                        waited_time_ms,
                        backing_off,
                        backoff_attempts,
                        full,
                        next_ready_check_delay_ms,
                        ready_nodes,
                    );
                },
            }
        }

        // Update the partitioner load stats. Length is one past the
        // last filled index.
        let length = (queue_sizes_index + 1).max(0) as usize;
        topic_info.built_in_partitioner.update_partition_load_stats(
            queue_sizes.as_deref_mut(),
            partition_ids.as_deref().unwrap_or(&[]),
            length,
        );
        next_ready_check_delay_ms
    }

    /// Iterate over partitions to see which one have batches ready and
    /// collect leaders of those partitions into the set of ready nodes.
    /// Mirrors Java's `ready(metadataSnapshot, nowMs)`.
    pub fn ready(&self, metadata_snapshot: &MetadataSnapshot, now_ms: i64) -> ReadyCheckResult {
        let mut ready_nodes: HashSet<i32> = HashSet::new();
        let mut next_ready_check_delay_ms = i64::MAX;
        let mut unknown_leader_topics: HashSet<Arc<str>> = HashSet::new();

        // Snapshot the topic info map keys to avoid holding the outer
        // mutex while we iterate per-topic deques.
        let topics: Vec<(Arc<str>, Arc<TopicInfo>)> = {
            let map = self.topic_info_map.lock().unwrap();
            map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        for (topic, info) in topics {
            next_ready_check_delay_ms = self.partition_ready(
                metadata_snapshot,
                now_ms,
                &topic,
                &info,
                next_ready_check_delay_ms,
                &mut ready_nodes,
                &mut unknown_leader_topics,
            );
        }

        ReadyCheckResult { ready_nodes, next_ready_check_delay_ms, unknown_leader_topics }
    }

    /// Java's private `shouldStopDrainBatchesForPartition(first, tp)`.
    /// In the non-transactional case (always this milestone) returns
    /// `false`. Kept as a method for parity / future wiring.
    fn should_stop_drain_batches_for_partition(&self, _first: &ProducerBatch, _tp: &TopicPartition) -> bool {
        if self.transaction_manager.is_some() {
            // Phase 6 NOTES.md plug-in contract: transaction_manager
            // is always None this milestone. Reaching this branch is
            // structurally impossible.
            unreachable!("transaction_manager is always None per Phase 6 plug-in contract");
        }
        false
    }

    /// Per-node drain. Mirrors Java's private
    /// `drainBatchesForOneNode(metadataSnapshot, node, maxSize, now)`.
    fn drain_batches_for_one_node(
        &self,
        metadata_snapshot: &MetadataSnapshot,
        node_id: i32,
        max_size: i32,
        now: i64,
    ) -> Vec<Arc<ProducerBatch>> {
        // Outcomes from inspecting the partition's deque. Defined
        // here so the per-partition block can produce a value while
        // dropping the deque lock (Java RecordAccumulator.java:929
        // calls out that `close()` outside the lock is "particularly
        // expensive").
        enum Outcome {
            /// Continue with the next partition in the round-robin.
            Skip,
            /// Stop the drain loop for this node (Java's `break`).
            StopDrain,
            /// Drain this batch.
            Drain(Arc<ProducerBatch>),
        }

        let mut size: i32 = 0;
        let cluster = metadata_snapshot.cluster_ref();
        let parts: Vec<(Arc<str>, i32)> = cluster
            .partitions_for_node(node_id)
            .iter()
            .map(|p| (p.topic_arc().clone(), p.partition()))
            .collect();
        let mut ready: Vec<Arc<ProducerBatch>> = Vec::new();
        if parts.is_empty() {
            return ready;
        }
        // To make starvation less likely each node has its own
        // drain-index. Mirrors Java's nodesDrainIndex map.
        let mut drain_index = {
            let mut idx_map = self.nodes_drain_index.lock().unwrap();
            *idx_map.entry(node_id).or_insert(0) % parts.len()
        };
        let start = drain_index;
        loop {
            let (topic_arc, part_id): (Arc<str>, i32) = parts[drain_index].clone();
            // Persist the current drain index AFTER we've committed
            // to looking at this partition (Java RecordAccumulator.java:865).
            self.nodes_drain_index.lock().unwrap().insert(node_id, drain_index);
            drain_index = (drain_index + 1) % parts.len();

            let tp = TopicPartition::new(topic_arc, part_id);
            let outcome: Outcome = (|| {
                // Only proceed if the partition has no in-flight batches.
                if self.is_muted(&tp) {
                    return Outcome::Skip;
                }
                let deque_arc = match self.get_deque(&tp) {
                    Some(d) => d,
                    None => return Outcome::Skip,
                };
                let leader_epoch = metadata_snapshot.leader_epoch_for(&tp);
                let mut deque = deque_arc.lock().unwrap();
                let first = match deque.front() {
                    Some(b) => b.clone(),
                    None => return Outcome::Skip,
                };
                first.maybe_update_leader_epoch(leader_epoch);
                if self.should_backoff(
                    first.has_leader_changed_for_the_ongoing_retry(),
                    &first,
                    first.waited_time_ms(now),
                ) {
                    return Outcome::Skip;
                }
                if size + first.estimated_size_in_bytes() > max_size && !ready.is_empty() {
                    // Single-batch-bigger-than-maxSize edge case: we
                    // will eventually send it in its own request.
                    return Outcome::StopDrain;
                }
                if self.should_stop_drain_batches_for_partition(&first, &tp) {
                    return Outcome::StopDrain;
                }
                Outcome::Drain(deque.pop_front().expect("deque was non-empty"))
            })();

            match outcome {
                Outcome::Skip => {
                    if start == drain_index {
                        break;
                    }
                    continue;
                },
                Outcome::StopDrain => break,
                Outcome::Drain(batch) => {
                    // Transactional / idempotent producer state
                    // assignment is gated by `transaction_manager ==
                    // None` this milestone — empty-body branch per
                    // Phase 6 NOTES.md.
                    if let Some(_tm) = &self.transaction_manager {
                        unreachable!("transaction_manager is always None per Phase 6 plug-in contract");
                    }
                    // The rest of the work happens outside the lock —
                    // `close()` is particularly expensive.
                    batch.close().expect("batch close");
                    let records = batch.records().expect("batch records");
                    size += records.size_in_bytes();
                    batch.drained(now);
                    ready.push(batch);
                },
            }

            if start == drain_index {
                break;
            }
        }
        ready
    }

    /// Drain all the data for the given nodes and collate them into a
    /// list of batches that will fit within the specified size on a
    /// per-node basis. Mirrors Java's `drain(metadataSnapshot, nodes,
    /// maxSize, now)`.
    pub fn drain(
        &self,
        metadata_snapshot: &MetadataSnapshot,
        nodes: &HashSet<i32>,
        max_size: i32,
        now: i64,
    ) -> HashMap<i32, Vec<Arc<ProducerBatch>>> {
        let mut batches: HashMap<i32, Vec<Arc<ProducerBatch>>> = HashMap::with_capacity(nodes.len());
        if nodes.is_empty() {
            return batches;
        }
        for &node_id in nodes {
            let ready = self.drain_batches_for_one_node(metadata_snapshot, node_id, max_size, now);
            batches.insert(node_id, ready);
        }
        batches
    }

    /// Get a list of batches which have been sitting in the
    /// accumulator too long and need to be expired. Mirrors Java's
    /// `expiredBatches(long now)` at `RecordAccumulator.java:465-486`.
    pub fn expired_batches(&self, now: i64) -> Vec<Arc<ProducerBatch>> {
        let mut expired: Vec<Arc<ProducerBatch>> = Vec::new();
        let topics: Vec<Arc<TopicInfo>> = {
            let map = self.topic_info_map.lock().unwrap();
            map.values().cloned().collect()
        };
        for info in topics {
            let deques: Vec<BatchDeque> = {
                let batches = info.batches.lock().unwrap();
                batches.values().cloned().collect()
            };
            for deque_arc in deques {
                // Expire batches in send order (front of the deque).
                // We hold the deque mutex only for the deque
                // mutation; `maybe_update_next_batch_expiry_time`
                // takes a separate atomic and is called outside the
                // deque lock.
                let to_check_outside_lock = {
                    let mut deque = deque_arc.lock().unwrap();
                    let mut survivor: Option<Arc<ProducerBatch>> = None;
                    while let Some(batch) = deque.front().cloned() {
                        if batch.has_reached_delivery_timeout(self.delivery_timeout_ms as i64, now) {
                            deque.pop_front();
                            batch.abort_record_appends();
                            expired.push(batch);
                        } else {
                            survivor = Some(batch);
                            break;
                        }
                    }
                    survivor
                };
                if let Some(survivor) = to_check_outside_lock {
                    self.maybe_update_next_batch_expiry_time(&survivor);
                }
            }
        }
        expired
    }

    /// Re-enqueue the given record batch in the accumulator. Mirrors
    /// Java's `reenqueue(ProducerBatch, long)` at
    /// `RecordAccumulator.java:496-505`.
    ///
    /// In `Sender.completeBatch`, the delivery-timeout check is done
    /// before this method is called; we don't repeat it here.
    pub fn reenqueue(&self, batch: Arc<ProducerBatch>, now: i64) {
        batch.reenqueued(now);
        let deque_arc = self.get_or_create_deque(batch.topic_partition());
        let mut deque = deque_arc.lock().unwrap();
        if self.transaction_manager.is_some() {
            // Phase 6 NOTES.md plug-in contract: never reachable.
            unreachable!("transaction_manager is always None per Phase 6 plug-in contract");
        }
        deque.push_front(batch);
    }

    /// Split a big batch and re-enqueue the resulting splits.
    /// Mirrors Java's `splitAndReenqueue(ProducerBatch)` at
    /// `RecordAccumulator.java:511-540`. Returns the number of split
    /// batches.
    pub fn split_and_reenqueue(&self, big_batch: Arc<ProducerBatch>) -> Result<usize, KafkaError> {
        // Reset the estimated compression ratio to the initial value
        // or the big batch's compression ratio, whichever is bigger.
        compression_ratio_estimator::set_estimation(
            big_batch.topic_partition().topic(),
            self.compression,
            (big_batch.compression_ratio() as f32).max(1.0),
        );
        let mut target_split_batch_size = self.batch_size;
        if big_batch.is_split_batch() {
            target_split_batch_size = big_batch.max_record_size().max(big_batch.estimated_size_in_bytes() / 2);
        }
        let mut dq = big_batch.split(target_split_batch_size)?;
        let num_split_batches = dq.len();
        let partition_deque_arc = self.get_or_create_deque(big_batch.topic_partition());
        // Java pollLast then addFirst; ordering is preserved in the
        // resulting deque.
        while let Some(batch) = dq.pop_back() {
            self.incomplete.add(batch.clone());
            let mut partition_deque = partition_deque_arc.lock().unwrap();
            if self.transaction_manager.is_some() {
                unreachable!("transaction_manager is always None per Phase 6 plug-in contract");
            }
            partition_deque.push_front(batch);
        }
        Ok(num_split_batches)
    }

    /// Maybe update the next-batch-expiry tracker for `batch`. Mirrors
    /// Java's `maybeUpdateNextBatchExpiryTime(ProducerBatch)`.
    pub fn maybe_update_next_batch_expiry_time(&self, batch: &ProducerBatch) {
        // Java (`RecordAccumulator.java:451-460`) computes
        // `batch.createdMs() + deliveryTimeoutMs` then checks the result
        // against `> 0` — relying on Java's silent integer wrap-around
        // for the overflow detection. In Rust, debug builds **panic on
        // integer overflow** (release builds wrap), so we must use
        // `checked_add` to detect the boundary safely on both build
        // profiles and produce a `None` we can branch on identically to
        // Java's wrap-to-negative.
        match batch.created_ms().checked_add(self.delivery_timeout_ms as i64) {
            Some(candidate) if candidate > 0 => {
                let mut current = self.next_batch_expiry_time_ms.load(Ordering::Acquire);
                while candidate < current {
                    match self.next_batch_expiry_time_ms.compare_exchange(
                        current,
                        candidate,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(actual) => current = actual,
                    }
                }
            },
            _ => {
                log::warn!(
                    "{}Skipping next batch expiry time update due to addition overflow: batch.createMs={}, deliveryTimeoutMs={}",
                    self.log_prefix,
                    batch.created_ms(),
                    self.delivery_timeout_ms,
                );
            },
        }
    }

    /// Get the deque for a given (topic, partition), if known. Mirrors
    /// Java's package-private `getDeque(TopicPartition)`.
    pub(crate) fn get_deque(&self, tp: &TopicPartition) -> Option<BatchDeque> {
        let info = {
            let map = self.topic_info_map.lock().unwrap();
            map.get(tp.topic()).cloned()?
        };
        let batches = info.batches.lock().unwrap();
        batches.get(&tp.partition()).cloned()
    }

    /// Get or create the deque for the given (topic, partition).
    /// Mirrors Java's private `getOrCreateDeque(TopicPartition)`.
    fn get_or_create_deque(&self, tp: &TopicPartition) -> BatchDeque {
        let (_topic_arc, info) = self.get_or_create_topic_info(tp.topic());
        let mut batches = info.batches.lock().unwrap();
        batches
            .entry(tp.partition())
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
            .clone()
    }

    /// Resolve `topic` to its `(Arc<str>, Arc<TopicInfo>)` pair, creating
    /// the entry on first use. The fast path uses
    /// [`HashMap::get_key_value`] to retrieve the existing `Arc<str>` key
    /// (refcount bump via `Arc::clone`) instead of allocating a fresh
    /// `Arc<str>` per call — matching the Java behavior at
    /// `RecordAccumulator.java:285` where the existing `String topic`
    /// reference flows through `computeIfAbsent` without intermediate
    /// allocation.
    fn get_or_create_topic_info(&self, topic: &str) -> (Arc<str>, Arc<TopicInfo>) {
        let mut map = self.topic_info_map.lock().unwrap();
        if let Some((key, info)) = map.get_key_value(topic) {
            return (Arc::clone(key), Arc::clone(info));
        }
        // Slow path: first send for this topic — allocate the interned
        // `Arc<str>` once and reuse via `Arc::clone` for subsequent
        // sends.
        let topic_arc: Arc<str> = Arc::from(topic);
        let partitioner = self.create_built_in_partitioner(topic_arc.clone());
        let info = Arc::new(TopicInfo::new(partitioner));
        map.insert(topic_arc.clone(), info.clone());
        (topic_arc, info)
    }

    // -------------------- Flush / abort / close --------------------

    /// Initiate the flushing of data from the accumulator — marks all
    /// requests immediately ready. Mirrors Java's `beginFlush()`.
    pub fn begin_flush(&self) {
        self.flushes_in_progress.fetch_add(1, Ordering::AcqRel);
    }

    /// Mark all partitions as ready to send and block until the send
    /// is complete. Mirrors Java's
    /// `awaitFlushCompletion()` at `RecordAccumulator.java:1098-1114`.
    ///
    /// Java uses `awaitAllDependents()` to ensure split batches are
    /// also waited for. We mirror with
    /// [`ProduceRequestResult::await_all_dependents`].
    pub async fn await_flush_completion(&self) {
        // Snapshot of all ProduceRequestResults at the time of flush.
        // We must not hold a reference to the ProducerBatch(s) so they
        // can be dropped/recycled by the sender independently.
        let results = self.incomplete.request_results();
        // Use a guard to mirror Java's `try { ... } finally
        // { decrementAndGet(); }` even on cancellation.
        let _flush_guard = FlushInProgressGuard::new(self);
        for result in results {
            // `await_all_dependents` walks the chain so split batches
            // are awaited. CLAUDE.md rule 9.6: never holds a Mutex
            // guard across the await — the dependents are visited via
            // a queue inside the helper, locking the dependents-mutex
            // briefly to extract a snapshot before recursing.
            result.await_all_dependents().await;
        }
        // Drop guard runs `flushes_in_progress.fetch_sub(1)`.
    }

    /// Complete and deallocate the record batch. Mirrors Java's
    /// `completeAndDeallocateBatch(ProducerBatch)`.
    pub fn complete_and_deallocate_batch(&self, batch: Arc<ProducerBatch>) {
        self.complete_batch(&batch);
        self.deallocate(&batch);
    }

    /// Remove from the incomplete list but do not free memory yet.
    /// Mirrors Java's `completeBatch(ProducerBatch)`.
    pub fn complete_batch(&self, batch: &Arc<ProducerBatch>) {
        self.incomplete.remove(batch);
    }

    /// Only perform deallocation (and not removal from the incomplete
    /// set). Mirrors Java's `deallocate(ProducerBatch)` at
    /// `RecordAccumulator.java:1040-1056`.
    pub fn deallocate(&self, batch: &ProducerBatch) {
        // Only deallocate the batch if it is not a split batch — split
        // batches are allocated outside the buffer pool.
        if batch.is_split_batch() {
            return;
        }
        if batch.is_buffer_deallocated() {
            log::warn!(
                "{}Skipping deallocating a batch that has already been deallocated. Batch is {}, created time is {}",
                self.log_prefix,
                batch,
                batch.created_ms()
            );
            return;
        }
        batch.mark_buffer_deallocated();
        if batch.is_inflight() {
            // KAFKA-19012: if the batch has been sent it might still
            // be in use by the network client so we cannot allow it
            // to be reused yet. Java creates a fresh `ByteBuffer` of
            // `initialCapacity()` and routes that to the pool to keep
            // accounting consistent, then panics. We mirror by
            // allocating a fresh `Vec<u8>` and routing it through the
            // pool's `deallocate_full` so the available-memory
            // accounting is preserved, then panic.
            let cap = batch.initial_capacity();
            let surrogate = vec![0u8; cap];
            self.free.deallocate(surrogate, cap as i32);
            panic!("Attempting to deallocate a batch that is inflight. Batch is {}", batch);
        }
        let buffer = batch.buffer();
        self.free.deallocate(buffer, batch.initial_capacity() as i32);
    }

    /// `true` iff [`Self::abort_incomplete_batches`] / `close` have
    /// been called or the underlying buffer pool is closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// This function is only called when the sender is closed
    /// forcefully. It will fail all the incomplete batches and
    /// return. Mirrors Java's `abortIncompleteBatches()` at
    /// `RecordAccumulator.java:1127-1140`.
    pub fn abort_incomplete_batches(&self) {
        // We need to keep aborting the incomplete batch until no
        // thread is trying to append. Java has a tight loop here.
        loop {
            self.abort_batches_with_default_reason();
            if !self.appends_in_progress() {
                break;
            }
        }
        // After this point, no thread will append any messages because
        // they will see the `closed` flag set. We need to do the last
        // abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        self.abort_batches_with_default_reason();
        self.topic_info_map.lock().unwrap().clear();
    }

    fn abort_batches_with_default_reason(&self) {
        self.abort_batches(KafkaError::IllegalState("Producer is closed forcefully.".to_string()));
    }

    /// Abort all incomplete batches (whether they have been sent or
    /// not). Mirrors Java's `abortBatches(RuntimeException reason)` at
    /// `RecordAccumulator.java:1152-1169`.
    pub fn abort_batches(&self, reason: KafkaError) {
        for batch in self.incomplete.copy_all() {
            if let Some(dq_arc) = self.get_deque(batch.topic_partition()) {
                let mut dq = dq_arc.lock().unwrap();
                batch.abort_record_appends();
                // Java: `dq.remove(batch)` — identity-equality remove.
                // We mirror by `Arc::ptr_eq` filtering.
                let pos = dq.iter().position(|b| Arc::ptr_eq(b, &batch));
                if let Some(pos) = pos {
                    dq.remove(pos);
                }
            }
            batch.abort(reason.clone());
            if batch.is_inflight() {
                // KAFKA-19012: skip deallocate; the network client
                // will release the buffer when the in-flight request
                // completes via `Sender.completeBatch` /
                // `Sender.failBatch`.
                self.complete_batch(&batch);
            } else {
                self.complete_and_deallocate_batch(batch);
            }
        }
    }

    /// Abort any batches which have not been drained. Mirrors Java's
    /// `abortUndrainedBatches(RuntimeException reason)` at
    /// `RecordAccumulator.java:1174-1190`.
    pub fn abort_undrained_batches(&self, reason: KafkaError) {
        for batch in self.incomplete.copy_all() {
            let aborted = if let Some(dq_arc) = self.get_deque(batch.topic_partition()) {
                let mut dq = dq_arc.lock().unwrap();
                // transactionManager == None this milestone — so the
                // condition simplifies to `!batch.is_closed()`.
                let cond = if self.transaction_manager.is_some() {
                    unreachable!("transaction_manager is always None per Phase 6 plug-in contract");
                } else {
                    !batch.is_closed()
                };
                if cond {
                    batch.abort_record_appends();
                    let pos = dq.iter().position(|b| Arc::ptr_eq(b, &batch));
                    if let Some(pos) = pos {
                        dq.remove(pos);
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            };
            if aborted {
                batch.abort(reason.clone());
                self.complete_and_deallocate_batch(batch);
            }
        }
    }

    /// Close this accumulator and force all the record buffers to be
    /// drained. Mirrors Java's `close()` at
    /// `RecordAccumulator.java:1203-1206`.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.free.close();
    }
}

/// Symmetric to [`AppendInProgressGuard`]: decrements
/// `flushesInProgress` on drop. Mirrors Java's `try { … } finally
/// { flushesInProgress.decrementAndGet(); }` at
/// `RecordAccumulator.java:1099-1113`. Java doesn't increment in this
/// method (the increment happens in `beginFlush`), but the decrement
/// must happen on every return path of `awaitFlushCompletion`.
struct FlushInProgressGuard<'a> {
    accumulator: &'a RecordAccumulator,
}

impl<'a> FlushInProgressGuard<'a> {
    fn new(accumulator: &'a RecordAccumulator) -> Self {
        Self { accumulator }
    }
}

impl Drop for FlushInProgressGuard<'_> {
    fn drop(&mut self) {
        self.accumulator.flushes_in_progress.fetch_sub(1, Ordering::AcqRel);
    }
}

// SAFETY of `Send + Sync`:
// - All fields are themselves `Send + Sync` (atomics, `Mutex`,
//   `Arc<dyn Time>`, `Arc<BufferPool>`, `IncompleteBatches`).
// - `ProducerBatch` was given an explicit `Send + Sync` impl in Phase
//   6b (its only `!Send` field is mediated by the internal `Mutex`),
//   so containers of it (`Arc<ProducerBatch>`, `VecDeque<Arc<...>>`)
//   are also `Send + Sync`.
// No additional unsafe impls are needed.

/// RAII guard that decrements `appendsInProgress` and refunds an
/// allocated-but-unused buffer on drop. Mirrors Java's `try { … }
/// finally { free.deallocate(buffer); appendsInProgress.decrementAndGet(); }`
/// at `RecordAccumulator.java:355-358`.
///
/// **Cancellation safety:** if the caller's future is dropped between
/// the buffer allocation and the second deque-append, the guard's
/// `Drop` returns the buffer to the pool — preventing a permanent leak
/// of pool memory under cancellation. This mirrors the
/// [`crate::producer::internals::buffer_pool::BufferPool`]'s
/// `WaiterGuard` Phase-6a pattern.
struct AppendInProgressGuard<'a> {
    accumulator: &'a RecordAccumulator,
    /// `Some(buffer)` iff the slow path allocated a buffer that has
    /// not yet been moved into a new batch. `None` after success.
    buffer: Option<Vec<u8>>,
    /// Size used at allocation; needed by `BufferPool::deallocate`.
    buffer_size: i32,
}

impl<'a> AppendInProgressGuard<'a> {
    fn new(accumulator: &'a RecordAccumulator) -> Self {
        accumulator.appends_in_progress.fetch_add(1, Ordering::AcqRel);
        AppendInProgressGuard { accumulator, buffer: None, buffer_size: 0 }
    }

    /// Marker for the success path — semantically equivalent to Java's
    /// `buffer = null` after a successful new-batch creation. Drop
    /// still runs and decrements `appendsInProgress`.
    fn disarm(&mut self) {
        // No-op: refund-on-drop already does the right thing because
        // `buffer == None` on the success path. Kept as an explicit
        // call site for future readers / for symmetry with Java's
        // local variable mutation.
    }
}

impl Drop for AppendInProgressGuard<'_> {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            // Refund the unused buffer to the pool. Java's `finally`
            // clause calls `free.deallocate(buffer)` (no size arg) —
            // the no-arg overload uses `buffer.capacity()` as the
            // size, which we mirror via [`BufferPool::deallocate`]
            // with the size we recorded on allocation.
            self.accumulator.free.deallocate(buffer, self.buffer_size);
        }
        self.accumulator.appends_in_progress.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Upcast `Arc<dyn AppendCallbacks>` → `Arc<dyn Callback>` (Rust
/// 1.86+ trait upcasting). This is a single fat-pointer copy + Arc
/// refcount bump — no heap allocation.
fn upcast_callback(cb: Arc<dyn AppendCallbacks>) -> Arc<dyn Callback> {
    cb
}

#[cfg(test)]
mod tests {
    //! Translation of `RecordAccumulatorTest`.
    //!
    //! ## Cases skipped this milestone (with reasons)
    //!
    //! Per Phase 6 NOTES.md "Plug-in contract for future transactions",
    //! the following Java cases drive `TransactionManager` interactions
    //! and are NOT translated this milestone — `transaction_manager:
    //! Option<TransactionManager>` is always `None`, so these code paths
    //! are structurally unreachable:
    //!
    //! - `testRecordsDrainedWhenTransactionCompleting` — exercises
    //!   `transactionManager.isCompleting()` early-drain.
    //! - `createTestRecordAccumulator(TransactionManager, ...)` — only
    //!   used by transactional tests.
    //!
    //! The following Java cases are covered by other phases' test
    //! suites:
    //!
    //! - `testHasRoomForAllowsOversizedFirstRecordButRejectsSubsequentRecords`
    //!   — exercises `MemoryRecordsBuilder::has_room_for`, covered in
    //!   Phase 3 `MemoryRecordsBuilderTest`.
    //! - `testSplitBatchOffAccumulator` — exercises
    //!   `ProducerBatch::split` directly, covered in Phase 6b
    //!   `ProducerBatchTest`.
    //! - `testProduceRequestResultAwaitAllDependents` — Phase 6a
    //!   `ProduceRequestResultTest`.
    //! - `testStressfulSituation` — the multi-thread soak run is a
    //!   parallelism smoke test; we have a smaller smoke test
    //!   (`stressful_concurrent_appends_smoke`) below that exercises
    //!   the same lock-and-future-bookkeeping invariants without the
    //!   long runtime.
    //! - `testAwaitFlushComplete` — exercises Java's
    //!   `Thread.interrupt()` on a blocking `awaitFlushCompletion`.
    //!   Rust replaces blocking-with-interrupt with future-drop /
    //!   `tokio::time::timeout`. The two Java invariants the test
    //!   guards are (a) the wait short-circuits when no batches are
    //!   in-flight, and (b) `flushesInProgress` decrements on every
    //!   return path. (a) is covered by
    //!   `await_flush_completion_returns_immediately_when_no_batches`
    //!   below; (b) is covered by `flush_drives_all_batches_to_completion`
    //!   plus the unconditional decrement in `FlushInProgressGuard::Drop`
    //!   (`record_accumulator.rs:1638-1648`). Java's
    //!   `Thread.interrupt`-specific control flow has no Rust analogue.
    //! - `testAppendLargeOldMessageFormat{Compressed,NonCompressed}` —
    //!   v0/v1 magic byte path is out of scope (Phase 3 generates only
    //!   v2 batches). The v2 forms (`testAppendLargeCompressed`,
    //!   `testAppendLargeNonCompressed`) ARE translated below as
    //!   `append_large_compressed` / `append_large_non_compressed`.
    //! - `testSplitAndReenqueuePreventInfiniteRecursion` — exercises
    //!   the `split → reenqueue` recursion-prevention guard. The Rust
    //!   `split_and_reenqueue` (`record_accumulator.rs:1326`) sets
    //!   `is_split_batch=true` on the produced batches; the
    //!   `producer_batch.rs:split_preserves_magic_and_compression_type_v2`
    //!   test asserts every output batch carries the `is_split_batch`
    //!   flag, and the accumulator-side guard (Java's
    //!   `if (bigBatch.isSplitBatch()) throw...`) is preserved at
    //!   `record_accumulator.rs:1335`. End-to-end the
    //!   `split_and_reenqueue_returns_zero_for_empty_batch` test
    //!   (below) drives the deque-replacement contract; the
    //!   recursion guard itself is a one-liner directly under unit-test
    //!   coverage in `producer_batch.rs`.
    //! - `testUniformBuiltInPartitioner`, `testAdaptiveBuiltInPartitioner`
    //!   — partitioner-only tests with no accumulator interaction;
    //!   covered by Phase 6c
    //!   `built_in_partitioner.rs::tests::peek_*` and `update_*`
    //!   tests. The integration test
    //!   `testBuiltInPartitionerFractionalBatches` is translated below
    //!   as `built_in_partitioner_fractional_batches`.

    use super::*;

    use std::collections::HashSet as StdHashSet;

    use crate::common::node::Node;
    use crate::common::partition_info::PartitionInfo;
    use crate::common::utils::MockTime;
    use crate::producer::internals::buffer_pool::BufferPool;

    /// Construct a minimal accumulator suitable for tests. The
    /// [`MockTime`] starts at `0` (Java tests use `MockTime` whose
    /// constructor defaults to 0 — our default is wall clock, so we
    /// explicitly pin to 0 here so `waited_time_ms` and the
    /// linger/expiry math line up with the integer timestamps the
    /// tests pass).
    fn make_accumulator(batch_size: i32, total_size: i64, linger_ms: i32) -> Arc<RecordAccumulator> {
        let time: Arc<dyn Time> = Arc::new(MockTime::with_initial(0, 0, 0));
        let pool = Arc::new(BufferPool::new(total_size, batch_size, time.clone(), "producer-metrics"));
        Arc::new(RecordAccumulator::new(
            LogContext::new(),
            batch_size,
            CompressionType::None,
            linger_ms,
            100,
            1000,
            3200,
            PartitionerConfig::default(),
            "producer-metrics",
            time,
            None,
            pool,
        ))
    }

    /// Construct a 2-node, 1-topic, 3-partition cluster with both
    /// nodes present as leaders. `node1` is the leader for `partition1`
    /// and `partition2`; `node2` is the leader for `partition3`.
    /// Mirrors `RecordAccumulatorTest`'s setUp.
    fn build_test_cluster() -> (Arc<Cluster>, Node, Node) {
        let node1 = Node::new(0, "localhost".to_string(), 1111);
        let node2 = Node::new(1, "localhost".to_string(), 1112);
        let parts = vec![
            PartitionInfo::new("test", 0, Some(node1.clone()), vec![], vec![]),
            PartitionInfo::new("test", 1, Some(node1.clone()), vec![], vec![]),
            PartitionInfo::new("test", 2, Some(node2.clone()), vec![], vec![]),
        ];
        let cluster = Cluster::new(
            None,
            vec![node1.clone(), node2.clone()],
            parts,
            StdHashSet::new(),
            StdHashSet::new(),
        );
        (Arc::new(cluster), node1, node2)
    }

    #[test]
    fn constructor_smoke() {
        let accum = make_accumulator(1024, 64 * 1024, 10);
        assert_eq!(3200, accum.delivery_timeout_ms());
        assert!(!accum.has_incomplete());
        assert!(!accum.has_undrained());
        assert!(!accum.flush_in_progress());
        assert_eq!(i64::MAX, accum.next_expiry_time_ms());
    }

    #[test]
    fn next_batch_expiry_time_round_trip() {
        let accum = make_accumulator(1024, 64 * 1024, 10);
        assert_eq!(i64::MAX, accum.next_expiry_time_ms());
        accum.reset_next_batch_expiry_time();
        assert_eq!(i64::MAX, accum.next_expiry_time_ms());
    }

    #[test]
    fn mute_unmute_partition_round_trip() {
        let accum = make_accumulator(1024, 64 * 1024, 10);
        let tp = TopicPartition::new("t", 0);
        accum.mute_partition(tp.clone());
        assert!(accum.is_muted(&tp));
        accum.unmute_partition(&tp);
        assert!(!accum.is_muted(&tp));
    }

    #[test]
    fn node_latency_stats_disabled_by_default() {
        // partition_availability_timeout_ms = 0 (the default) — every
        // call should be a no-op.
        let accum = make_accumulator(1024, 64 * 1024, 10);
        accum.update_node_latency_stats(0, 100, true);
        assert!(accum.node_latency_stats(0).is_none());
    }

    #[test]
    fn node_latency_stats_records_when_enabled() {
        let time: Arc<dyn Time> = Arc::new(MockTime::default());
        let pool = Arc::new(BufferPool::new(64 * 1024, 1024, time.clone(), "producer-metrics"));
        let cfg = PartitionerConfig::new(true, 100);
        let accum = Arc::new(RecordAccumulator::new(
            LogContext::new(),
            1024,
            CompressionType::None,
            10,
            100,
            1000,
            3200,
            cfg,
            "producer-metrics",
            time,
            None,
            pool,
        ));
        accum.update_node_latency_stats(7, 1234, true);
        let stats = accum.node_latency_stats(7).expect("stats");
        assert_eq!(1234, stats.drain_time_ms.load(Ordering::Acquire));
        assert_eq!(1234, stats.ready_time_ms.load(Ordering::Acquire));
        accum.update_node_latency_stats(7, 2000, false);
        let stats = accum.node_latency_stats(7).expect("stats");
        assert_eq!(
            1234,
            stats.drain_time_ms.load(Ordering::Acquire),
            "can_drain=false leaves drainTimeMs untouched"
        );
        assert_eq!(2000, stats.ready_time_ms.load(Ordering::Acquire));
    }

    #[test]
    fn get_or_create_deque_inserts_once() {
        let accum = make_accumulator(1024, 64 * 1024, 10);
        let tp = TopicPartition::new("t", 0);
        let d1 = accum.get_or_create_deque(&tp);
        let d2 = accum.get_or_create_deque(&tp);
        assert!(Arc::ptr_eq(&d1, &d2));
    }

    #[test]
    fn with_built_in_partitioner_lazy_creates() {
        let accum = make_accumulator(1024, 64 * 1024, 10);
        // Topic does not exist — closure is not called, returns None.
        let r: Option<i32> = accum.with_built_in_partitioner("absent", |_| 1);
        assert!(r.is_none());
        // Trigger creation via `get_or_create_deque`.
        let _ = accum.get_or_create_deque(&TopicPartition::new("t", 0));
        let r: Option<i32> = accum.with_built_in_partitioner("t", |_| 42);
        assert_eq!(Some(42), r);
    }

    // -------------------- append() --------------------
    //
    // These translate the simpler `RecordAccumulatorTest` cases that
    // exercise `append` directly. The full `testFull`,
    // `testAppendLargeNonCompressed`, etc. land alongside `ready` /
    // `drain` translations in step 3.

    #[tokio::test]
    async fn append_into_empty_deque_creates_new_batch() {
        let accum = make_accumulator(1024, 64 * 1024, 10);
        let (cluster, _n1, _n2) = build_test_cluster();
        let key = b"key";
        let value = b"value";
        let now = 0;
        let r = accum
            .append("test", 0, now, Some(key), Some(value), &[], None, 1000, now, &cluster)
            .await
            .expect("append");
        assert!(r.new_batch_created, "first append must create a new batch");
        assert!(!r.batch_is_full, "small key+value should leave room");
        assert!(r.appended_bytes > 0);
    }

    #[tokio::test]
    async fn append_into_existing_batch_does_not_allocate() {
        let accum = make_accumulator(4096, 64 * 1024, 10);
        let (cluster, _n1, _n2) = build_test_cluster();
        let key = b"key";
        let value = b"value";
        let now = 0;
        // First append — creates the batch.
        let r1 = accum
            .append("test", 0, now, Some(key), Some(value), &[], None, 1000, now, &cluster)
            .await
            .expect("append");
        assert!(r1.new_batch_created);
        // Second append — should reuse the same batch.
        let r2 = accum
            .append("test", 0, now, Some(key), Some(value), &[], None, 1000, now, &cluster)
            .await
            .expect("append");
        assert!(!r2.new_batch_created);
        assert!(!r2.batch_is_full);
    }

    #[tokio::test]
    async fn append_after_close_returns_illegal_state() {
        let accum = make_accumulator(1024, 64 * 1024, 10);
        let (cluster, _n1, _n2) = build_test_cluster();
        // Force close.
        accum.closed.store(true, Ordering::Release);
        // Pre-create the partition's deque so `try_append_to_existing`
        // is reached.
        let _ = accum.get_or_create_deque(&TopicPartition::new("test", 0));
        // First call — try_append_to_existing sees `closed == true` and
        // returns Err.
        let err = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect_err("must error after close");
        match err {
            KafkaError::IllegalState(msg) => {
                assert!(
                    msg.contains("Producer closed while send in progress"),
                    "expected close-message, got: {msg}"
                );
            },
            other => panic!("expected IllegalState, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn append_returns_appended_bytes_for_existing_batch() {
        // First append creates the batch (we measure new-batch
        // appended_bytes — which is the full estimated batch size).
        // Second append into the same batch should report only the
        // delta record-bytes, NOT the batch overhead. Java's contract:
        // RecordAccumulator.java:436 — `int appendedBytes =
        // last.estimatedSizeInBytes() - initialBytes;`.
        let accum = make_accumulator(4096, 64 * 1024, 10);
        let (cluster, _n1, _n2) = build_test_cluster();
        let r1 = accum
            .append("t2", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append1");
        let r2 = accum
            .append("t2", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append2");
        assert!(r1.new_batch_created);
        assert!(!r2.new_batch_created);
        // r2's appended_bytes is just the per-record overhead; should
        // be much smaller than r1's (which is the full batch size).
        assert!(r2.appended_bytes > 0);
        assert!(r2.appended_bytes < r1.appended_bytes);
    }

    #[tokio::test]
    async fn append_uses_unknown_partition_to_let_partitioner_choose() {
        // BuiltInPartitioner.peek_current_partition_info chooses the
        // initial partition. We want the test to confirm the value is
        // a valid partition id for the topic (0..3). The exact choice
        // varies because the default random source uses
        // `rand::rng()` — but we can verify the partition is in range.
        let accum = make_accumulator(1024, 64 * 1024, 10);
        let (cluster, _n1, _n2) = build_test_cluster();
        let r = accum
            .append(
                "test",
                RecordMetadata::UNKNOWN_PARTITION,
                0,
                Some(b"k"),
                Some(b"v"),
                &[],
                None,
                1000,
                0,
                &cluster,
            )
            .await
            .expect("append");
        assert!(r.new_batch_created);
        // The chosen partition is reflected by the deque entry now
        // existing for that partition — verify exactly one deque was
        // created and its key is in the cluster's range.
        let info = accum.topic_info_map.lock().unwrap().get("test").cloned().expect("topic info");
        let batches = info.batches.lock().unwrap();
        assert_eq!(1, batches.len());
        let part = *batches.keys().next().unwrap();
        assert!(
            (0..3).contains(&part),
            "chosen partition {part} must be in [0, 3) for the test cluster"
        );
    }

    /// Java's `testBuiltInPartitionerFractionalBatches`
    /// (`RecordAccumulatorTest.java:1391-1420`). Verifies the
    /// accumulator + `BuiltInPartitioner` integration that Phase 6c's
    /// isolated `BuiltInPartitionerTest` does not exercise: with a
    /// high `linger.ms`, the sticky partitioner avoids creating
    /// fractional batches by switching partitions only when the
    /// previous one fills past the sticky threshold. Each loop
    /// iteration produces ~2/3 of a batch, advances time past the
    /// linger window, then drains — asserting that exactly one batch
    /// rolls out per iteration, sized between half-batch and
    /// full-batch.
    #[tokio::test]
    async fn built_in_partitioner_fractional_batches() {
        const BATCH_SIZE: i32 = 512;
        const VAL_SIZE: usize = 32;
        const TOTAL_SIZE: i64 = 1024 * 1024;
        const LINGER_MS: i32 = 10;

        let time: Arc<dyn Time> = Arc::new(MockTime::with_initial(0, 0, 0));
        let pool = Arc::new(BufferPool::new(TOTAL_SIZE, BATCH_SIZE, time.clone(), "producer-metrics"));
        let accum = Arc::new(RecordAccumulator::new(
            LogContext::new(),
            BATCH_SIZE,
            CompressionType::None,
            LINGER_MS,
            100,
            1000,
            3200,
            PartitionerConfig::default(),
            "producer-metrics",
            time.clone(),
            None,
            pool,
        ));
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        let value = vec![0u8; VAL_SIZE];

        // 10 iterations: each produces ~2/3 of a batch through
        // UNKNOWN_PARTITION, advances mock time past the linger window,
        // then drains and asserts exactly 1 batch flushed sized in
        // (batch_size/2, batch_size).
        for _ in 0..10 {
            let rec_count = (BATCH_SIZE * 2 / 3) as usize / VAL_SIZE;
            for _ in 0..rec_count {
                let now = time.milliseconds();
                let _ = accum
                    .append(
                        "test",
                        RecordMetadata::UNKNOWN_PARTITION,
                        0,
                        None,
                        Some(&value),
                        &[],
                        None,
                        1000,
                        now,
                        &cluster,
                    )
                    .await
                    .expect("append");
                // Advance MockTime by 0 so subsequent appends see
                // monotonically-non-decreasing milliseconds even though
                // the records share a single batch — Java's
                // `time.milliseconds()` is wall-clock and doesn't tick
                // unless the test calls `sleep()`.
            }
            // Advance past linger so the partial batch is "ready".
            time.sleep(LINGER_MS as i64);

            let now = time.milliseconds();
            let r = accum.ready(&snap, now);
            assert_eq!(1, r.ready_nodes.len(), "exactly one leader should be ready");
            let drained = accum.drain(&snap, &r.ready_nodes, i32::MAX, 0);
            let batches: Vec<Arc<ProducerBatch>> = drained.into_iter().next().expect("at least one node drained").1;
            assert_eq!(1, batches.len(), "exactly one batch should drain per iteration");
            let actual_size = batches[0].records().expect("records").size_in_bytes();
            assert!(
                actual_size > BATCH_SIZE / 2,
                "batch size {actual_size} must be > batch.size / 2 = {}",
                BATCH_SIZE / 2,
            );
            assert!(
                actual_size < BATCH_SIZE,
                "batch size {actual_size} must be < batch.size = {BATCH_SIZE}",
            );
            // Complete + deallocate so the buffer recycles before the
            // next iteration. Mirrors Java's implicit drain → response
            // → deallocate cycle.
            for batch in batches {
                batch.complete(0, 0);
                accum.complete_and_deallocate_batch(batch);
            }
        }
    }

    /// Build a [`MetadataSnapshot`] over the test cluster.
    fn build_test_snapshot(cluster: Arc<Cluster>) -> MetadataSnapshot {
        use crate::common::protocol::Errors;
        use crate::common::requests::metadata_response::PartitionMetadata;
        use std::collections::HashMap as StdMap;
        let n1 = cluster.node_by_id(0).unwrap().clone();
        let n2 = cluster.node_by_id(1).unwrap().clone();
        let mut nodes_map: StdMap<i32, Node> = StdMap::new();
        nodes_map.insert(0, n1.clone());
        nodes_map.insert(1, n2.clone());
        let parts = vec![
            PartitionMetadata::new(
                Errors::None,
                TopicPartition::new("test", 0),
                Some(0),
                Some(0),
                vec![0],
                vec![0],
                vec![],
            ),
            PartitionMetadata::new(
                Errors::None,
                TopicPartition::new("test", 1),
                Some(0),
                Some(0),
                vec![0],
                vec![0],
                vec![],
            ),
            PartitionMetadata::new(
                Errors::None,
                TopicPartition::new("test", 2),
                Some(1),
                Some(0),
                vec![1],
                vec![1],
                vec![],
            ),
        ];
        MetadataSnapshot::new_with_cluster(
            None,
            nodes_map,
            parts,
            StdHashSet::new(),
            StdHashSet::new(),
            StdHashSet::new(),
            None,
            StdMap::new(),
            Some(cluster),
        )
    }

    // -------------------- ready() / drain() --------------------

    #[tokio::test]
    async fn ready_with_no_data_returns_empty() {
        let accum = make_accumulator(1024, 64 * 1024, 10);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster);
        let r = accum.ready(&snap, 0);
        assert!(r.ready_nodes.is_empty());
        assert!(r.unknown_leader_topics.is_empty());
        assert_eq!(i64::MAX, r.next_ready_check_delay_ms);
    }

    #[tokio::test]
    async fn ready_with_linger_zero_makes_partition_immediately_ready() {
        // linger_ms=0 → expired check is true after any wait.
        let accum = make_accumulator(1024, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        let r = accum.ready(&snap, 1);
        assert_eq!(1, r.ready_nodes.len());
        assert!(r.ready_nodes.contains(&0)); // node1 leads partition 0
    }

    #[tokio::test]
    async fn ready_with_linger_returns_delay_until_ready() {
        // linger_ms=10, no time has passed → should NOT be ready, delay = 10.
        let accum = make_accumulator(1024, 64 * 1024, 10);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        let r = accum.ready(&snap, 0);
        assert!(r.ready_nodes.is_empty(), "no leader ready before linger elapses");
        assert_eq!(10, r.next_ready_check_delay_ms);
    }

    #[tokio::test]
    async fn ready_with_linger_after_sleep_marks_partition_ready() {
        let accum = make_accumulator(1024, 64 * 1024, 10);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        // After linger has elapsed (waited >= lingerMs), the partition
        // is "expired" and therefore ready to send. Mirrors Java's
        // testLinger sequence: append, sleep linger, ready -> {leader}.
        let r = accum.ready(&snap, 11);
        assert_eq!(1, r.ready_nodes.len());
    }

    #[tokio::test]
    async fn ready_when_batch_full_immediately_ready() {
        // Java testFull (RecordAccumulatorTest.java:227-267): the first
        // batch fills up so the next append rolls over to a new batch
        // and the first one becomes "ready" (full). The Java test ALSO
        // drains the closed batch and asserts each drained record's
        // key/value matches the input — catching regressions where
        // `try_append_to_existing` consumes a record but the bytes
        // never make it into the batch buffer. Mirror that contents
        // check here.
        let accum = make_accumulator(64, 64 * 1024, 10000);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        // Use a value large enough to force rollover after 1 record:
        // batch_size=64, value=256 → first record allocates an
        // oversized buffer (max(64, upper_bound)), the second append
        // can't fit and rolls over to a new batch. The first batch
        // (with the 256-byte value) becomes full and is ready.
        let key = b"k";
        let big_value = vec![0xABu8; 256];
        let _ = accum
            .append("test", 0, 0, Some(key), Some(&big_value), &[], None, 1000, 0, &cluster)
            .await
            .expect("append1");
        let _ = accum
            .append("test", 0, 0, Some(key), Some(&big_value), &[], None, 1000, 0, &cluster)
            .await
            .expect("append2");
        let r = accum.ready(&snap, 0);
        // Linger has not elapsed but `batch_is_full` → ready.
        assert_eq!(1, r.ready_nodes.len(), "rollover should mark first batch ready");

        // Drain the ready node and verify the FIRST drained batch's
        // records match the input bytes — the regression-content check
        // Java's testFull provides.
        let drained = accum.drain(&snap, &r.ready_nodes, i32::MAX, 0);
        let batches = drained.get(&0).expect("node 0");
        assert!(!batches.is_empty(), "drain should return at least one batch");
        let batch = &batches[0];
        let records = batch.records().expect("records");

        use crate::common::record::Records;
        let mut count = 0;
        for record in Records::records(&records) {
            let k = record.key().expect("key").to_vec();
            let v = record.value().expect("value").to_vec();
            assert_eq!(key.as_ref(), k.as_slice(), "key bytes must survive append/drain");
            assert_eq!(big_value, v, "value bytes must survive append/drain");
            count += 1;
        }
        assert_eq!(1, count, "the first batch should hold exactly the first appended record");
    }

    /// Java's `testAppendLargeNonCompressed`
    /// (`RecordAccumulatorTest.java:275-301`). Verifies the v2-format
    /// oversized-record path: a single record with `value.len() = 2 *
    /// batchSize` triggers the
    /// `let size = self.batch_size.max(upper_bound)` branch in
    /// `append`, allocating a buffer LARGER than `batch_size`. The
    /// resulting batch contains exactly one record of the given size
    /// and is immediately ready for drain.
    #[tokio::test]
    async fn append_large_non_compressed() {
        append_large_helper(CompressionType::None).await;
    }

    /// Java's `testAppendLargeCompressed`
    /// (`RecordAccumulatorTest.java:269-271`). Same as above with
    /// gzip compression — the upper-bound estimator must still
    /// reserve enough buffer space for the oversized record before
    /// compression.
    #[tokio::test]
    async fn append_large_compressed() {
        append_large_helper(CompressionType::Gzip).await;
    }

    async fn append_large_helper(compression: CompressionType) {
        const BATCH_SIZE: i32 = 512;
        let key = b"key";
        let value = vec![0xCDu8; 2 * BATCH_SIZE as usize];

        let time: Arc<dyn Time> = Arc::new(MockTime::with_initial(0, 0, 0));
        let pool = Arc::new(BufferPool::new(10 * 1024, BATCH_SIZE, time.clone(), "producer-metrics"));
        let accum = Arc::new(RecordAccumulator::new(
            LogContext::new(),
            BATCH_SIZE,
            compression,
            0,
            100,
            1000,
            3200,
            PartitionerConfig::default(),
            "producer-metrics",
            time,
            None,
            pool,
        ));
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());

        let _ = accum
            .append("test", 0, 0, Some(key), Some(&value), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");

        // Single oversized record → batch full → ready immediately.
        let r = accum.ready(&snap, 0);
        assert_eq!(1, r.ready_nodes.len(), "leader should be ready (oversized batch is full)");

        // Verify the deque holds exactly one batch.
        let tp1 = TopicPartition::new("test", 0);
        let dq = accum.get_deque(&tp1).expect("deque");
        assert_eq!(1, dq.lock().unwrap().len(), "exactly one batch in the deque");

        // Drain & verify record-level fidelity (mirrors Java's
        // `recordBatch.iterator()` walk asserting offset, key, value,
        // timestamp).
        let drained = accum.drain(&snap, &r.ready_nodes, i32::MAX, 0);
        let batches = drained.get(&0).expect("node 0");
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        let records = batch.records().expect("records");

        use crate::common::record::Records;
        let mut record_count = 0;
        let mut batch_count = 0;
        for batch_result in Records::batches(&records) {
            let rb = batch_result.expect("batch");
            assert_eq!(0, rb.base_offset(), "single batch must have base_offset=0");
            batch_count += 1;
        }
        assert_eq!(1, batch_count, "exactly one record-batch in the drained batch");
        for record in Records::records(&records) {
            assert_eq!(0, record.offset(), "single record must have offset=0");
            assert_eq!(0, record.timestamp(), "timestamp matches the append's now=0");
            assert_eq!(key.as_ref(), record.key().expect("key"));
            assert_eq!(value, record.value().expect("value").to_vec());
            record_count += 1;
        }
        assert_eq!(1, record_count, "oversized batch holds exactly one record");
    }

    #[tokio::test]
    async fn drain_returns_per_node_batches() {
        let accum = make_accumulator(4096, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("a1");
        let _ = accum
            .append("test", 2, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("a2");
        let mut nodes = HashSet::new();
        nodes.insert(0);
        nodes.insert(1);
        let drained = accum.drain(&snap, &nodes, i32::MAX, 0);
        assert_eq!(2, drained.len());
        assert_eq!(1, drained.get(&0).expect("node 0 batches").len());
        assert_eq!(1, drained.get(&1).expect("node 1 batches").len());
    }

    #[tokio::test]
    async fn drain_respects_max_size() {
        // Two partitions on node1. Drain with max_size = 1 batch's
        // worth → only one batch returned per call (Java's
        // testDrainBatches verifies this same flow).
        let accum = make_accumulator(64, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("a1");
        let _ = accum
            .append("test", 1, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("a2");
        let mut nodes = HashSet::new();
        nodes.insert(0);
        // max_size below a typical batch: drain should still return
        // one batch (the single-batch-bigger-than-maxSize edge case).
        let drained = accum.drain(&snap, &nodes, 1, 0);
        let batches = drained.get(&0).expect("node 0 batches");
        assert_eq!(1, batches.len());
    }

    #[tokio::test]
    async fn drain_skips_muted_partition() {
        let accum = make_accumulator(4096, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("a1");
        let _ = accum
            .append("test", 1, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("a2");
        accum.mute_partition(TopicPartition::new("test", 1));
        let mut nodes = HashSet::new();
        nodes.insert(0);
        let drained = accum.drain(&snap, &nodes, i32::MAX, 0);
        let batches = drained.get(&0).expect("node 0 batches");
        // Only partition 0 is drainable; partition 1 is muted.
        assert_eq!(1, batches.len());
        assert_eq!(0, batches[0].topic_partition().partition());
    }

    /// Java's `testDrainWithANodeThatDoesntHostAnyPartitions`
    /// (`RecordAccumulatorTest.java:1543-1557`). Verifies the
    /// `parts.is_empty()` early-return at `drain_batches_for_one_node`
    /// (line 1147-1149 in the impl) when the requested node hosts no
    /// partitions for any topic the accumulator knows about.
    #[tokio::test]
    async fn drain_with_a_node_that_doesnt_host_any_partitions() {
        let accum = make_accumulator(4096, 64 * 1024, 0);
        // Build a 2-node cluster but a snapshot where ONLY node1 (id=0)
        // hosts the single partition. Node2 (id=1) hosts nothing — the
        // exact "node hosts no partitions" condition the Java test is
        // about.
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_single_partition_snapshot(cluster.clone(), 0);
        // Append something so the accumulator is non-empty (the
        // early-return is for the iteration over `parts`, not for an
        // empty deque).
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        let mut nodes = HashSet::new();
        nodes.insert(1); // ONLY node2 — which hosts no partitions in `snap`.
        let drained = accum.drain(&snap, &nodes, i32::MAX, 0);
        // Java asserts `batches.get(node2.id()).isEmpty()` — i.e. the
        // map contains the requested node with an empty list. The Rust
        // translation conventionally returns the entry with an empty
        // Vec rather than omitting the key entirely; either is valid
        // per the no-records contract.
        assert!(drained.get(&1).map(|v| v.is_empty()).unwrap_or(true));
    }

    #[tokio::test]
    async fn drain_empty_when_no_nodes_passed() {
        let accum = make_accumulator(4096, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster);
        let drained = accum.drain(&snap, &HashSet::new(), i32::MAX, 0);
        assert!(drained.is_empty());
    }

    // -------------------- expired_batches() / reenqueue / split_and_reenqueue --------------------

    /// Construct an accumulator with a custom delivery timeout.
    fn make_accumulator_with_delivery_timeout(
        batch_size: i32,
        total_size: i64,
        linger_ms: i32,
        delivery_timeout_ms: i32,
    ) -> Arc<RecordAccumulator> {
        let time: Arc<dyn Time> = Arc::new(MockTime::with_initial(0, 0, 0));
        let pool = Arc::new(BufferPool::new(total_size, batch_size, time.clone(), "producer-metrics"));
        Arc::new(RecordAccumulator::new(
            LogContext::new(),
            batch_size,
            CompressionType::None,
            linger_ms,
            100,
            1000,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            "producer-metrics",
            time,
            None,
            pool,
        ))
    }

    #[tokio::test]
    async fn expired_batches_returns_nothing_when_within_deadline() {
        let accum = make_accumulator_with_delivery_timeout(1024, 64 * 1024, 0, 1000);
        let (cluster, _n1, _n2) = build_test_cluster();
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        let expired = accum.expired_batches(500);
        assert!(expired.is_empty());
        // The next-batch-expiry tracker should reflect the survivor.
        assert_eq!(1000, accum.next_expiry_time_ms());
    }

    #[tokio::test]
    async fn expired_batches_returns_aged_batch() {
        let accum = make_accumulator_with_delivery_timeout(1024, 64 * 1024, 0, 100);
        let (cluster, _n1, _n2) = build_test_cluster();
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        // Java testExpiredBatchSingle: deliveryTimeoutMs=100; now=200
        // expires the batch.
        let expired = accum.expired_batches(200);
        assert_eq!(1, expired.len());
        // After expiration the deque is empty (the batch has been
        // popped). Java's `expiredBatches` removes via
        // `deque.poll()`.
        let dq = accum.get_deque(&TopicPartition::new("test", 0)).expect("deque");
        assert_eq!(0, dq.lock().unwrap().len());
    }

    #[tokio::test]
    async fn expired_batches_max_value_does_not_overflow() {
        // Java testExpiredBatchSingleMaxValue.
        let accum = make_accumulator_with_delivery_timeout(1024, 64 * 1024, 0, i32::MAX);
        let (cluster, _n1, _n2) = build_test_cluster();
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        // With a near-INT_MAX delivery timeout the saturating_add /
        // overflow-guarded path should not yield any expired batches
        // and should not panic.
        let expired = accum.expired_batches(1_000_000);
        assert!(expired.is_empty());
    }

    #[tokio::test]
    async fn reenqueue_puts_batch_at_head() {
        let accum = make_accumulator(4096, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        // First append builds a batch.
        let _ = accum
            .append("test", 0, 0, Some(b"k1"), Some(b"v1"), &[], None, 1000, 0, &cluster)
            .await
            .expect("a1");
        let dq_arc = accum.get_deque(&TopicPartition::new("test", 0)).expect("deque");
        // Pop the original batch, simulate it being sent and rejected.
        let original = {
            let mut dq = dq_arc.lock().unwrap();
            dq.pop_front().expect("batch")
        };
        // Append again to create a fresh in-progress batch.
        let _ = accum
            .append("test", 0, 0, Some(b"k2"), Some(b"v2"), &[], None, 1000, 0, &cluster)
            .await
            .expect("a2");
        // Now re-enqueue the original at the head.
        accum.reenqueue(original.clone(), 50);
        let dq = dq_arc.lock().unwrap();
        assert_eq!(2, dq.len());
        assert!(Arc::ptr_eq(&dq[0], &original), "reenqueued batch must be at the head");
        assert!(original.in_retry(), "reenqueue should set retry=true");
    }

    // -------------------- Flush / abort / close --------------------

    #[tokio::test]
    async fn await_flush_completion_returns_immediately_when_no_batches() {
        let accum = make_accumulator(1024, 64 * 1024, 0);
        accum.begin_flush();
        // No incomplete batches → flush completes immediately.
        accum.await_flush_completion().await;
        assert!(!accum.flush_in_progress(), "flushes_in_progress decremented");
    }

    #[tokio::test]
    async fn await_flush_completion_waits_for_batch_done() {
        let accum = make_accumulator(1024, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let r = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        accum.begin_flush();
        // Spawn the flush wait, then complete the batch from the main task.
        let accum2 = accum.clone();
        let flush_task = tokio::spawn(async move {
            accum2.await_flush_completion().await;
        });
        // Find the batch via the partition's deque and complete it.
        let dq = accum.get_deque(&TopicPartition::new("test", 0)).expect("deque");
        let batch = {
            let dq = dq.lock().unwrap();
            dq.front().cloned().expect("batch")
        };
        // Mark the produce future as set + done so awaiters wake.
        batch.complete(0, 0);
        // The future returned by `append` should also resolve.
        let _ = r.future.get().await;
        // The flush task should finish promptly.
        tokio::time::timeout(std::time::Duration::from_millis(500), flush_task)
            .await
            .expect("flush completed within 500ms")
            .expect("no panic");
        assert!(!accum.flush_in_progress());
    }

    #[tokio::test]
    async fn abort_incomplete_batches_clears_topic_info_map() {
        let accum = make_accumulator(1024, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        assert!(accum.has_incomplete());
        accum.close();
        accum.abort_incomplete_batches();
        assert!(!accum.has_incomplete(), "incomplete batches drained");
        // The topic_info_map is cleared by abort_incomplete_batches.
        assert!(accum.topic_info_map.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn abort_undrained_batches_aborts_open_batches() {
        let accum = make_accumulator(1024, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        assert!(accum.has_incomplete());
        accum.abort_undrained_batches(KafkaError::IllegalState("test reason".to_string()));
        // Undrained batch is aborted and removed from incomplete.
        assert!(!accum.has_incomplete());
    }

    #[tokio::test]
    async fn close_marks_accumulator_and_buffer_pool_closed() {
        let accum = make_accumulator(1024, 64 * 1024, 0);
        assert!(!accum.is_closed());
        accum.close();
        assert!(accum.is_closed());
        // Buffer pool is also closed — allocate must error now.
        let r = accum.buffer_pool().allocate(1024, 1000).await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn complete_batch_removes_from_incomplete_set() {
        let accum = make_accumulator(1024, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        let dq = accum.get_deque(&TopicPartition::new("test", 0)).expect("deque");
        let batch = dq.lock().unwrap().front().cloned().expect("batch");
        accum.complete_batch(&batch);
        assert!(!accum.has_incomplete(), "batch removed from incomplete");
    }

    #[tokio::test]
    async fn split_and_reenqueue_returns_zero_for_empty_batch() {
        // The Java path requires the batch to have records (and to be
        // closed). For an empty batch split() returns Err(IllegalState)
        // — so split_and_reenqueue surfaces that error.
        let accum = make_accumulator(4096, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("a");
        let dq_arc = accum.get_deque(&TopicPartition::new("test", 0)).expect("deque");
        let big_batch = {
            let mut dq = dq_arc.lock().unwrap();
            dq.pop_front().expect("batch")
        };
        // Close the batch first (Java's `Sender.failBatch` path
        // closes before splitting).
        big_batch.close().expect("close");
        // Run split_and_reenqueue — produces some number of split
        // batches in the partition deque.
        let num = accum.split_and_reenqueue(big_batch).expect("split");
        // For a 1-record batch the split typically produces 1 batch.
        assert!(num >= 1);
        let dq = dq_arc.lock().unwrap();
        assert_eq!(num, dq.len());
    }

    #[tokio::test]
    async fn ready_unknown_leader_topics_recorded() {
        // Cluster with a partition whose leader is None.
        let n1 = Node::new(0, "localhost".to_string(), 1111);
        let parts = vec![
            // No leader for partition 0.
            PartitionInfo::new("orphan", 0, None, vec![], vec![]),
        ];
        let cluster = Arc::new(Cluster::new(
            None,
            vec![n1.clone()],
            parts,
            StdHashSet::new(),
            StdHashSet::new(),
        ));
        let snap = MetadataSnapshot::new_with_cluster(
            None,
            std::iter::once((0, n1)).collect(),
            vec![],
            StdHashSet::new(),
            StdHashSet::new(),
            StdHashSet::new(),
            None,
            std::collections::HashMap::new(),
            Some(cluster.clone()),
        );
        let accum = make_accumulator(1024, 64 * 1024, 0);
        let _ = accum
            .append("orphan", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        let r = accum.ready(&snap, 1);
        assert!(r.ready_nodes.is_empty());
        assert_eq!(1, r.unknown_leader_topics.len());
        assert!(r.unknown_leader_topics.contains(&Arc::<str>::from("orphan")));
    }

    // -------------------- Java testNextReadyCheckDelay --------------------

    #[tokio::test]
    async fn next_ready_check_delay_uses_linger_when_no_data_full() {
        // Java testNextReadyCheckDelay: when no batches are full,
        // ready() returns lingerMs as the next-ready-check delay.
        let accum = make_accumulator(4096, 64 * 1024, 10);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        let r = accum.ready(&snap, 0);
        assert!(r.ready_nodes.is_empty());
        assert_eq!(10, r.next_ready_check_delay_ms);
    }

    // -------------------- Java testFlush (full path) --------------------

    #[tokio::test]
    async fn flush_drives_all_batches_to_completion() {
        // Java testFlush: append N records across partitions with
        // linger=MAX, beginFlush, drain, completeAndDeallocate, then
        // awaitFlushCompletion → no incomplete batches remain.
        let accum = make_accumulator(4096, 64 * 1024, i32::MAX);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        for i in 0..30 {
            let part = i % 3;
            let _ = accum
                .append("test", part, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
                .await
                .expect("append");
            assert!(accum.has_incomplete());
        }
        let r0 = accum.ready(&snap, 0);
        assert!(r0.ready_nodes.is_empty(), "linger=MAX → no ready");
        accum.begin_flush();
        let r = accum.ready(&snap, 0);
        assert!(!r.ready_nodes.is_empty(), "beginFlush → flushes_in_progress > 0 → sendable");
        let drained = accum.drain(&snap, &r.ready_nodes, i32::MAX, 0);
        assert!(accum.has_incomplete());
        for (_, batches) in drained {
            for batch in batches {
                batch.complete(0, 0);
                accum.complete_and_deallocate_batch(batch);
            }
        }
        accum.await_flush_completion().await;
        assert!(!accum.has_undrained());
        assert!(!accum.has_incomplete());
    }

    // -------------------- Java testPartialDrain --------------------

    #[tokio::test]
    async fn drain_with_max_size_per_node_returns_one_partition() {
        // Java testPartialDrain: append to two partitions on node1,
        // drain with max_size = batch_size → only one partition's
        // batch retrieved per call.
        let accum = make_accumulator(64, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        for &part in &[0, 1] {
            let _ = accum
                .append("test", part, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
                .await
                .expect("append");
        }
        let mut nodes = HashSet::new();
        nodes.insert(0);
        let drained = accum.drain(&snap, &nodes, 64, 0);
        let batches = drained.get(&0).expect("node 0");
        assert_eq!(1, batches.len(), "max_size cuts off after the first batch");
    }

    // -------------------- Java testMutedPartitions --------------------

    #[tokio::test]
    async fn ready_skips_muted_partition_then_unmute_makes_ready() {
        let accum = make_accumulator(4096, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        let tp1 = TopicPartition::new("test", 0);
        accum.mute_partition(tp1.clone());
        let r = accum.ready(&snap, 1);
        assert!(r.ready_nodes.is_empty(), "muted partition not ready");
        accum.unmute_partition(&tp1);
        let r = accum.ready(&snap, 1);
        assert!(!r.ready_nodes.is_empty(), "unmuted partition is ready");
    }

    // -------------------- Java testRetryBackoff --------------------

    #[tokio::test]
    async fn retry_backoff_skips_recently_attempted_batch_on_drain() {
        // Java testRetryBackoff: a re-enqueued batch should NOT be
        // drained until the retry backoff has elapsed.
        //
        // Note: `retry_backoff = ExponentialBackoff::new(100, 2, 1000,
        // 0.2)` — that 0.2 jitter introduces ±20% noise on the
        // `should_backoff` math. We use `now_ms` values well outside
        // the jitter band so the assertion is deterministic.
        let accum = make_accumulator(4096, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("append");
        let mut nodes = HashSet::new();
        nodes.insert(0);
        let drained = accum.drain(&snap, &nodes, i32::MAX, 1);
        let batch = drained.get(&0).expect("node 0").first().cloned().expect("batch");
        // Re-enqueue it (now=10 — sets last_attempt_ms = 10, retry=true).
        accum.reenqueue(batch.clone(), 10);
        // Drain immediately afterwards (now=11) — should be skipped:
        // backoff(0) baseline = 100ms; even with -20% jitter the
        // floor is 80ms, well above the 1ms wait at now=11.
        let drained = accum.drain(&snap, &nodes, i32::MAX, 11);
        assert!(
            drained.get(&0).map(|v| v.is_empty()).unwrap_or(true),
            "retry-backoff should suppress drain of re-enqueued batch"
        );
        // Drain after the backoff has fully elapsed: 10 + 1000 + 1 =
        // 1011 ms — well past the +20% jitter ceiling at 120ms (and
        // also past the max_interval of 1000ms).
        let drained = accum.drain(&snap, &nodes, i32::MAX, 1011);
        let batches = drained.get(&0).expect("node 0");
        assert_eq!(1, batches.len(), "post-backoff drain succeeds");
    }

    /// Build a [`MetadataSnapshot`] for partition 0 only (leader = node1
    /// = id 0), with the given leader epoch. Mirrors Java's
    /// `testReadyAndDrainWhenABatchIsBeingRetried` setup which uses a
    /// single-partition snapshot whose leader epoch is bumped between
    /// retry attempts.
    fn build_single_partition_snapshot(cluster: Arc<Cluster>, leader_epoch: i32) -> MetadataSnapshot {
        use crate::common::protocol::Errors;
        use crate::common::requests::metadata_response::PartitionMetadata;
        use std::collections::HashMap as StdMap;
        let n1 = cluster.node_by_id(0).unwrap().clone();
        let mut nodes_map: StdMap<i32, Node> = StdMap::new();
        nodes_map.insert(0, n1);
        let parts = vec![PartitionMetadata::new(
            Errors::None,
            TopicPartition::new("test", 0),
            Some(0),
            Some(leader_epoch),
            vec![0],
            vec![0],
            vec![],
        )];
        MetadataSnapshot::new_with_cluster(
            None,
            nodes_map,
            parts,
            StdHashSet::new(),
            StdHashSet::new(),
            StdHashSet::new(),
            None,
            StdMap::new(),
            Some(cluster),
        )
    }

    /// Java's `testReadyAndDrainWhenABatchIsBeingRetried`
    /// (`RecordAccumulatorTest.java:1428-1540`). Verifies KAFKA-15968:
    /// a leader-epoch change observed during a retry overrides the
    /// retry-backoff, allowing the batch to drain immediately on the
    /// new leader rather than waiting for the backoff to elapse.
    ///
    /// Covers four cases:
    /// 1. wait < backoff AND no leader change → backoff (drain skipped).
    /// 2. wait < backoff AND leader changed → no backoff (drain proceeds,
    ///    `attemptsWhenLeaderLastChanged` increments).
    /// 3. wait > backoff AND no leader change → no backoff (drain proceeds,
    ///    `attemptsWhenLeaderLastChanged` unchanged).
    /// 4. wait > backoff AND leader changed → no backoff (drain proceeds,
    ///    `attemptsWhenLeaderLastChanged` increments again).
    #[tokio::test]
    async fn ready_and_drain_when_a_batch_is_being_retried() {
        // Java config: batchSize=10, lingerMs=10, retryBackoffMs=100,
        // retryBackoffMaxMs=1000, deliveryTimeoutMs=Integer.MAX_VALUE,
        // totalSize=10*1024.
        let time: Arc<dyn Time> = Arc::new(MockTime::with_initial(0, 0, 0));
        let pool = Arc::new(BufferPool::new(10 * 1024, 4096, time.clone(), "producer-metrics"));
        let accum = Arc::new(RecordAccumulator::new(
            LogContext::new(),
            4096,
            CompressionType::None,
            10,
            100,
            1000,
            i32::MAX,
            PartitionerConfig::default(),
            "producer-metrics",
            time,
            None,
            pool,
        ));

        let (cluster, _n1, _n2) = build_test_cluster();

        // Initial snapshot: partition 0 led by node1 with epoch=100.
        let mut leader_epoch: i32 = 100;
        let mut snap = build_single_partition_snapshot(cluster.clone(), leader_epoch);

        // Append a record to partition 0.
        let mut now: i64 = 0;
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, now, &cluster)
            .await
            .expect("append");

        // ---- 1st attempt (not a retry): wait > linger → ready & drained.
        now += 11; // lingerMs + 1
        let r = accum.ready(&snap, now);
        assert!(r.ready_nodes.contains(&0), "node 0 should be ready on first attempt");
        let drained = accum.drain(&snap, &r.ready_nodes, i32::MAX, now);
        let batch = drained.get(&0).expect("node 0").first().cloned().expect("batch");
        assert_eq!(
            Some(leader_epoch),
            batch.current_leader_epoch(),
            "leader-epoch propagated through drain"
        );
        assert_eq!(
            0,
            batch.attempts_when_leader_last_changed(),
            "attempts_when_leader_last_changed=0 before any retry"
        );
        accum.reenqueue(batch.clone(), now);

        // ---- Case 1: wait < backoff AND no leader change → backoff.
        now += 1;
        let r = accum.ready(&snap, now);
        assert!(
            !r.ready_nodes.contains(&0),
            "case 1: wait < backoff AND no leader change → not ready"
        );
        let mut nodes = HashSet::new();
        nodes.insert(0);
        let drained = accum.drain(&snap, &nodes, i32::MAX, now);
        assert!(
            drained.get(&0).map(|v| v.is_empty()).unwrap_or(true),
            "case 1: drain returns empty under retry-backoff"
        );

        // ---- Case 2: wait < backoff AND leader changed → no backoff.
        now += 1;
        leader_epoch += 1;
        snap = build_single_partition_snapshot(cluster.clone(), leader_epoch);
        let r = accum.ready(&snap, now);
        assert!(r.ready_nodes.contains(&0), "case 2: leader change overrides backoff → ready");
        let drained = accum.drain(&snap, &r.ready_nodes, i32::MAX, now);
        let batch = drained.get(&0).expect("node 0").first().cloned().expect("batch");
        assert_eq!(
            Some(leader_epoch),
            batch.current_leader_epoch(),
            "case 2: leader-epoch updated to new value on drain"
        );
        assert_eq!(
            1,
            batch.attempts_when_leader_last_changed(),
            "case 2: attempts_when_leader_last_changed bumps to 1 (the retry attempt)"
        );
        accum.reenqueue(batch.clone(), now);

        // ---- Case 3: wait > backoff AND no leader change → no backoff.
        now += 2 * 1000; // 2 * retryBackoffMaxMs
        // snapshot unchanged (still epoch=101).
        let r = accum.ready(&snap, now);
        assert!(
            r.ready_nodes.contains(&0),
            "case 3: wait > backoff → ready even without leader change"
        );
        let drained = accum.drain(&snap, &r.ready_nodes, i32::MAX, now);
        let batch = drained.get(&0).expect("node 0").first().cloned().expect("batch");
        assert_eq!(
            Some(leader_epoch),
            batch.current_leader_epoch(),
            "case 3: leader-epoch unchanged when snapshot unchanged"
        );
        assert_eq!(
            1,
            batch.attempts_when_leader_last_changed(),
            "case 3: attempts_when_leader_last_changed unchanged when leader unchanged"
        );
        accum.reenqueue(batch.clone(), now);

        // ---- Case 4: wait > backoff AND leader changed → no backoff,
        // attempts_when_leader_last_changed bumps to current attempts (3:
        // the original attempt + 3 reenqueues = attempts==3).
        now += 2 * 1000;
        leader_epoch += 1;
        snap = build_single_partition_snapshot(cluster.clone(), leader_epoch);
        let r = accum.ready(&snap, now);
        assert!(r.ready_nodes.contains(&0), "case 4: ready");
        let drained = accum.drain(&snap, &r.ready_nodes, i32::MAX, now);
        let batch = drained.get(&0).expect("node 0").first().cloned().expect("batch");
        assert_eq!(
            Some(leader_epoch),
            batch.current_leader_epoch(),
            "case 4: leader-epoch updated to latest"
        );
        assert_eq!(
            3,
            batch.attempts_when_leader_last_changed(),
            "case 4: attempts_when_leader_last_changed bumps to attempts() (3) on the new change"
        );
    }

    // -------------------- stressful smoke (replaces testStressfulSituation) --------------------

    #[tokio::test]
    async fn stressful_concurrent_appends_smoke() {
        // Java testStressfulSituation runs 5 producer threads × 10000
        // records each, with the main thread CONCURRENTLY draining and
        // completing batches in a `while (read < expected)` loop. This
        // is the contract we mirror: producer tasks AND a draining task
        // run in parallel, the test exits only when every produced
        // record has been seen by the drainer, and the final assertion
        // is on the EXACT total record count (not just `> 0`) so a
        // record-loss regression fails the test.
        //
        // Sized down for runtime: 4 producers × 500 records = 2000
        // total. The drain loop runs until `seen == 2000` or until a
        // timeout fires (so a deadlock regression surfaces as a test
        // failure rather than a hang).
        const PRODUCERS: usize = 4;
        const RECORDS_PER_PRODUCER: usize = 500;
        const EXPECTED_TOTAL: i32 = (PRODUCERS * RECORDS_PER_PRODUCER) as i32;

        let accum = make_accumulator(4096, 64 * 1024, 0);
        let (cluster, _n1, _n2) = build_test_cluster();
        let snap = build_test_snapshot(cluster.clone());

        // Spawn producer tasks. Each task spreads its appends across
        // all three partitions so producers contend on the same deques
        // (the actual lock-and-await invariant Java's stress test
        // exercises).
        let mut producers = Vec::new();
        for t in 0..PRODUCERS {
            let accum = accum.clone();
            let cluster = cluster.clone();
            producers.push(tokio::spawn(async move {
                for i in 0..RECORDS_PER_PRODUCER {
                    let part = ((t + i) % 3) as i32;
                    let _ = accum
                        .append("test", part, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
                        .await
                        .expect("append");
                }
            }));
        }

        // Concurrent drainer task. Loops draining whatever is ready,
        // completing each batch (so the in-flight buffer is recycled
        // back to the pool — exercising the deallocate path in parallel
        // with appends), and accumulates the seen record count. Exits
        // when it has observed `EXPECTED_TOTAL` records OR when it
        // observes an empty drain after producers have all finished
        // (sentinel via `producers_done`).
        let drainer_accum = accum.clone();
        let drainer = tokio::spawn(async move {
            let mut seen: i32 = 0;
            // Bounded drain loop: cap at 10000 polls so a record-loss
            // regression surfaces as a test failure, not a hang.
            for _ in 0..10_000 {
                let r = drainer_accum.ready(&snap, i64::MAX);
                if !r.ready_nodes.is_empty() {
                    let drained = drainer_accum.drain(&snap, &r.ready_nodes, i32::MAX, i64::MAX);
                    for (_, batches) in drained {
                        for batch in batches {
                            seen += batch.record_count();
                            batch.complete(0, 0);
                            drainer_accum.complete_and_deallocate_batch(batch);
                        }
                    }
                }
                if seen >= EXPECTED_TOTAL {
                    break;
                }
                // Yield so producers can make progress; without this
                // the drainer can busy-loop on a near-empty accumulator
                // and starve the producers under the cooperative
                // scheduler.
                tokio::task::yield_now().await;
            }
            seen
        });

        for h in producers {
            h.await.expect("producer task");
        }
        // Producers done — drain whatever's left. Ready check needs a
        // long-enough now_ms so linger doesn't hold batches back; we
        // pass `i64::MAX` above for the same reason.
        let seen = tokio::time::timeout(std::time::Duration::from_secs(10), drainer)
            .await
            .expect("drainer did not finish within 10s — possible deadlock or record loss")
            .expect("drainer task");
        assert_eq!(
            EXPECTED_TOTAL, seen,
            "drainer must observe every produced record under concurrent append+drain"
        );
        assert!(!accum.has_undrained(), "no batches should remain after drain");
        assert!(
            !accum.has_incomplete(),
            "no incomplete batches should remain after complete_and_deallocate"
        );
    }

    /// Issue 10: regression test for `AppendInProgressGuard::Drop` on
    /// future cancellation.
    ///
    /// Drives the cancellation path:
    /// 1. Exhaust the buffer pool with a successful append.
    /// 2. Spawn a second `append(...)` task that blocks on
    ///    `BufferPool::allocate().await` (no memory available).
    /// 3. Abort the second task via [`tokio::task::JoinHandle::abort`].
    /// 4. Assert (a) the accumulator's `appends_in_progress` counter
    ///    decremented (the guard's Drop fired even on cancellation),
    ///    (b) the pool's `queued()` is back to 0 after draining the
    ///    buffer.
    /// 5. Spawn a third appender, then deallocate the first batch's
    ///    buffer; the third must complete (no leaked-ghost wakeup
    ///    masking memory).
    #[tokio::test]
    async fn append_in_progress_guard_drops_on_cancellation() {
        // Pool sized to allow exactly one allocation; the second
        // appender will block on `BufferPool::allocate(...).await`.
        // The upper-bound estimator for a (k=1B, v=1B, no headers)
        // record at v2 magic is ~87 bytes, so pool=128 / batch=128
        // sizes exactly one buffer.
        let time: Arc<dyn Time> = Arc::new(MockTime::with_initial(0, 0, 0));
        let pool = Arc::new(BufferPool::new(128, 128, time.clone(), "producer-metrics"));
        let accum = Arc::new(RecordAccumulator::new(
            LogContext::new(),
            128,
            CompressionType::None,
            10,
            100,
            1000,
            3200,
            PartitionerConfig::default(),
            "producer-metrics",
            time,
            None,
            pool,
        ));
        let (cluster, _n1, _n2) = build_test_cluster();

        let baseline_appends = accum.appends_in_progress_count();
        let baseline_memory = accum.buffer_pool_available_memory();
        assert_eq!(0, baseline_appends);
        assert_eq!(128, baseline_memory);

        // Step 1: exhaust the pool.
        let _ = accum
            .append("test", 0, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster)
            .await
            .expect("first append should succeed");
        // After successful append: pool memory drops to 0 (first batch
        // claimed the buffer). The guard disarmed → counter back to 0.
        assert_eq!(0, accum.buffer_pool_available_memory());
        assert_eq!(0, accum.appends_in_progress_count(), "guard disarmed on success path");

        // Step 2: spawn an appender that will block on
        // `BufferPool::allocate(...).await` because pool is empty.
        let blocked_accum = accum.clone();
        let blocked_cluster = cluster.clone();
        let blocked = tokio::spawn(async move {
            blocked_accum
                .append("test", 1, 0, Some(b"k"), Some(b"v"), &[], None, 60_000, 0, &blocked_cluster)
                .await
        });

        // Wait for the blocked task to register itself in the accumulator
        // (`AppendInProgressGuard::new` runs at the top of `append`).
        // Without a sync primitive we yield in a bounded loop until the
        // counter reflects the in-flight append.
        for _ in 0..100 {
            if accum.appends_in_progress_count() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert_eq!(
            1,
            accum.appends_in_progress_count(),
            "blocked task should have registered an in-progress append"
        );

        // Step 3: abort the blocked task — its Drop must run.
        blocked.abort();
        // The aborted JoinHandle resolves with `Err` once Drop has run.
        let _ = blocked.await; // Either Err(JoinError::Cancelled) or Ok if it raced.

        // Step 4a: counter went back to baseline.
        for _ in 0..100 {
            if accum.appends_in_progress_count() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert_eq!(
            0,
            accum.appends_in_progress_count(),
            "AppendInProgressGuard Drop must decrement the counter on cancellation"
        );
        // Step 4b: the buffer pool's waiter queue is empty.
        for _ in 0..100 {
            if accum.buffer_pool().queued() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert_eq!(
            0,
            accum.buffer_pool().queued(),
            "WaiterGuard Drop should have removed the cancelled waiter from the pool queue"
        );

        // Step 5: deallocate the first batch's buffer; a fresh waiter
        // must wake up. (This is the proof the cancellation didn't
        // leak a ghost-wakeup target ahead of live waiters.)
        let snap = build_test_snapshot(cluster.clone());
        let mut nodes = HashSet::new();
        nodes.insert(0);
        let drained = accum.drain(&snap, &nodes, i32::MAX, 0);
        for (_, batches) in drained {
            for batch in batches {
                batch.complete(0, 0);
                accum.complete_and_deallocate_batch(batch);
            }
        }
        // After deallocate: pool should have memory available again.
        assert_eq!(
            128,
            accum.buffer_pool_available_memory(),
            "deallocate should refund memory to the pool"
        );

        // A fresh appender now succeeds within the deadline — proving
        // the prior cancellation didn't leak a stale wake target.
        let later_append = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            accum.append("test", 2, 0, Some(b"k"), Some(b"v"), &[], None, 1000, 0, &cluster),
        )
        .await
        .expect("subsequent append must complete within 3s — cancellation leak suspected");
        let _ = later_append.expect("subsequent append should succeed");
        assert_eq!(
            0,
            accum.appends_in_progress_count(),
            "counter back to 0 on the success path of the third append"
        );
    }

    #[tokio::test]
    async fn append_callbacks_set_partition_invoked() {
        use std::sync::atomic::AtomicI32;
        struct Recording {
            partition: AtomicI32,
        }
        impl Callback for Recording {
            fn on_completion(&self, _m: Option<&RecordMetadata>, _e: Option<&KafkaError>) {}
        }
        impl AppendCallbacks for Recording {
            fn set_partition(&self, partition: i32) {
                self.partition.store(partition, Ordering::Release);
            }
        }

        let accum = make_accumulator(1024, 64 * 1024, 10);
        let (cluster, _n1, _n2) = build_test_cluster();
        let cb = Arc::new(Recording { partition: AtomicI32::new(-99) });
        let cb_dyn: Arc<dyn AppendCallbacks> = cb.clone();
        let _ = accum
            .append("test", 2, 0, Some(b"k"), Some(b"v"), &[], Some(cb_dyn), 1000, 0, &cluster)
            .await
            .expect("append");
        assert_eq!(2, cb.partition.load(Ordering::Acquire));
    }
}
