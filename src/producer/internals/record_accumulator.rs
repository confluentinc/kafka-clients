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
//!   driven by [`tokio::sync::Notify`] (one shared notifier, woken by
//!   `done()` on each batch).
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

use crate::common::record::CompressionType;
use crate::common::topic_partition::TopicPartition;
use crate::common::utils::ExponentialBackoff;
use crate::common::utils::LogContext;
use crate::common::utils::Time;
use crate::common_client_configs::{RETRY_BACKOFF_EXP_BASE, RETRY_BACKOFF_JITTER};
use crate::producer::callback::Callback;

use super::buffer_pool::BufferPool;
use super::built_in_partitioner::BuiltInPartitioner;
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
    /// Notifier for `await_flush_completion` — woken on every
    /// per-batch `done()` so flush can wake. We use a single shared
    /// `Notify` (Phase 6a pattern, mirrors `ProduceRequestResult`'s
    /// `Notify::notify_waiters` to fan-out).
    flush_notify: Arc<tokio::sync::Notify>,
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
            flush_notify: Arc::new(tokio::sync::Notify::new()),
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
            let deques: Vec<Arc<Mutex<VecDeque<Arc<ProducerBatch>>>>> = {
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

    /// Maybe update the next-batch-expiry tracker for `batch`. Mirrors
    /// Java's `maybeUpdateNextBatchExpiryTime(ProducerBatch)`.
    pub fn maybe_update_next_batch_expiry_time(&self, batch: &ProducerBatch) {
        let candidate = batch.created_ms().saturating_add(self.delivery_timeout_ms as i64);
        if batch.created_ms() + self.delivery_timeout_ms as i64 > 0 {
            // non-negative check guards against overflow
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
        } else {
            log::warn!(
                "{}Skipping next batch expiry time update due to addition overflow: batch.createMs={}, deliveryTimeoutMs={}",
                self.log_prefix,
                batch.created_ms(),
                self.delivery_timeout_ms,
            );
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
        let info = self.get_or_create_topic_info(tp.topic_arc().clone());
        let mut batches = info.batches.lock().unwrap();
        batches
            .entry(tp.partition())
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
            .clone()
    }

    fn get_or_create_topic_info(&self, topic: Arc<str>) -> Arc<TopicInfo> {
        let mut map = self.topic_info_map.lock().unwrap();
        if let Some(info) = map.get(&topic) {
            return info.clone();
        }
        let partitioner = self.create_built_in_partitioner(topic.clone());
        let info = Arc::new(TopicInfo::new(partitioner));
        map.insert(topic, info.clone());
        info
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::common::utils::MockTime;
    use crate::producer::internals::buffer_pool::BufferPool;

    /// Construct a minimal accumulator suitable for type-level smoke
    /// tests (no append calls yet — those land in Phase 6d step 2).
    fn make_accumulator(batch_size: i32, total_size: i64, linger_ms: i32) -> Arc<RecordAccumulator> {
        let time: Arc<dyn Time> = Arc::new(MockTime::default());
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
}
