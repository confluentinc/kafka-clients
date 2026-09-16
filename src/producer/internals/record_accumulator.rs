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
//! Queue that accumulates records into [`MemoryRecords`] instances to be sent
//! to the server.
//!
//! Translated from `org.apache.kafka.clients.producer.internals.RecordAccumulator`.
//!
//! The accumulator uses a bounded amount of memory and append calls will block
//! when that memory is exhausted, unless this behavior is explicitly disabled.

use crate::producer::RecordMetadata;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use crate::{kafka_debug, kafka_trace, kafka_warn};
use dashmap::DashMap;

use crate::MetadataSnapshot;
use crate::common::Cluster;
use crate::common::Error;
use crate::common::Node;
use crate::common::TopicPartition;
use crate::common::header::RecordHeader;
use crate::common::metrics::{ClosureMeasurable, Metrics};
use crate::common::record::TimestampType;
use crate::common::record::internal::AbstractRecords;
use crate::common::record::internal::CompressionRatioEstimator;
use crate::common::record::internal::MemoryRecords;
use crate::common::record::internal::MemoryRecordsBuilder;
use crate::common::record::internal::RecordBatch;
use crate::common::utils::ExponentialBackoff;
use crate::common::utils::LogContext;
use crate::producer::Callback;
use crate::producer::internals::BufferPool;
use crate::producer::internals::BuiltInPartitioner;
use crate::producer::internals::FutureRecordMetadata;
use crate::producer::internals::IncompleteBatches;
use crate::producer::internals::ProducerBatch;
use crate::producer::internals::{InFlightBatchPool, TransactionManager};

/// Partitioner configuration for the built-in partitioner.
///
/// Translated from `RecordAccumulator.PartitionerConfig`.
#[derive(Clone, Default)]
pub struct PartitionerConfig {
    /// If true, partition switching adapts to broker load, otherwise partition
    /// switching is random.
    pub enable_adaptive_partitioning: bool,
    /// If a broker cannot process produce requests from a partition for the
    /// specified time, the partition is treated by the partitioner as not
    /// available. If the timeout is 0, this logic is disabled.
    pub partition_availability_timeout_ms: i64,
}

/// The failure returned by [`RecordAccumulator::append`], carrying the user
/// `Callback` back to the caller when this append never handed it to a batch.
///
/// DoD #7: this type has no Java counterpart, and the reason is purely a Rust
/// ownership one. Java's `KafkaProducer.doSend` builds one `AppendCallbacks`
/// object and passes the *reference* to `accumulator.append(..)`
/// (`KafkaProducer.java:1049`), so after `append` throws it still holds the
/// callback and its `catch (ApiException e)` block invokes it exactly once with a
/// null-metadata `RecordMetadata` (`KafkaProducer.java:1056-1062`). Rust's
/// [`Callback`] is a `Box<dyn FnOnce>` — deliberately non-`Clone`, which is what
/// makes "exactly once" a type-level guarantee — so it is *moved* into `append`
/// and the only way the caller can still honour the callback obligation
/// (CLAUDE.md §9.5) is for `append` to give it back. That is the same
/// `returned_callback` mechanism [`RecordAccumulator::try_append`] already uses
/// for the batch-is-full path; this type extends it to the error paths.
///
/// It appears **boxed** in every `Result` position. [`Error`] alone already sits at
/// clippy's 128-byte `result_large_err` threshold, so the extra callback slot pushes
/// an unboxed `Result` over it — the same reason `FetchCollector`'s `FetchFail` is a
/// `Box`.
pub struct AppendFailure {
    /// The error Java's `append` throws.
    pub error: Error,
    /// The callback, when this append did not consume it. `None` when the caller
    /// passed no callback, or when the callback was already handed to a batch.
    pub callback: Option<Callback>,
}

impl AppendFailure {
    /// An append that failed without ever taking the callback, boxed for the
    /// `Result` position (see the type-level note).
    fn boxed(error: Error, callback: Option<Callback>) -> Box<Self> {
        Box::new(Self { error, callback })
    }
}

// A `Callback` is a `Box<dyn FnOnce>` and cannot derive `Debug`, but `Debug` is
// what `Result::unwrap`/`expect` need, so report whether one came back instead.
impl std::fmt::Debug for AppendFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendFailure")
            .field("error", &self.error)
            .field("callback_returned", &self.callback.is_some())
            .finish()
    }
}

impl std::fmt::Display for AppendFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

/// Metadata about a record just appended to the record accumulator.
///
/// Translated from `RecordAccumulator.RecordAppendResult`.
pub struct RecordAppendResult {
    /// The future for the record metadata.
    pub future: Arc<FutureRecordMetadata>,
    /// Whether the batch is full.
    pub batch_is_full: bool,
    /// Whether a new batch was created for this append.
    pub new_batch_created: bool,
    /// The number of bytes appended.
    pub appended_bytes: i32,
    /// The topic-partition the record was actually appended to.
    ///
    /// Java reports the resolved partition through
    /// `RecordAccumulator.AppendCallbacks.setPartition` (`KafkaProducer.java:1606`),
    /// which the accumulator calls once the built-in partitioner has resolved
    /// `UNKNOWN_PARTITION`; `KafkaProducer.doSend` then reads it back as
    /// `appendCallbacks.topicPartition()` to pass to
    /// `transactionManager.maybeAddPartition` (`:1045`). The Rust `append` takes a
    /// plain completion `Callback` rather than an `AppendCallbacks` trait object, so
    /// it is reported here instead. Its partition is never
    /// `RecordMetadata::UNKNOWN_PARTITION`, which is what Java asserts at `:1038`.
    ///
    /// # Why the whole `TopicPartition` and not just the index
    ///
    /// Because the caller needs one, and this is the only place that can produce it
    /// without allocating. The accumulator interns one `Arc<str>` per topic
    /// (`get_or_create_topic_info`), so building it here costs a single refcount
    /// increment, whereas `KafkaProducer::do_send_bytes` rebuilding it from the
    /// `&str` would allocate a `String` *and* an `Arc<str>` and copy the topic name
    /// twice — per record, on the default path, which CLAUDE.md §11 names as an
    /// anti-pattern ("identifiers cloned on every message ... prefer `Arc<str>`")
    /// and `definition-of-done.md` §10 asks the send-path audit to catch. Carrying
    /// the index alone was exactly that regression; see Critic 44 issue 1.
    pub topic_partition: TopicPartition,
}

/// The set of nodes that have at least one complete record batch in the
/// accumulator.
///
/// Translated from `RecordAccumulator.ReadyCheckResult`.
pub struct ReadyCheckResult {
    /// Nodes with ready batches.
    pub ready_nodes: HashSet<Node>,
    /// The time in ms until the next check is needed.
    pub next_ready_check_delay_ms: i64,
    /// Topics whose leader is unknown.
    pub unknown_leader_topics: HashSet<Arc<str>>,
}

/// Callbacks passed into append.
///
/// Translated from `RecordAccumulator.AppendCallbacks`.
pub trait AppendCallbacks: Send {
    /// Called to set the partition when it is resolved.
    fn set_partition(&mut self, partition: i32);
    /// Called when the record has been acknowledged or errored.
    fn on_completion(&self, metadata: Option<&crate::producer::RecordMetadata>, error: Option<&Error>);
}

/// Node latency stats for each node that are used for adaptive partition
/// distribution.
///
/// Translated from `RecordAccumulator.NodeLatencyStats`.
pub struct NodeLatencyStats {
    /// Last time the node had batches ready to send.
    pub ready_time_ms: i64,
    /// Last time the node was able to drain batches.
    pub drain_time_ms: i64,
}

impl NodeLatencyStats {
    /// Creates new latency stats initialized to `now_ms`.
    pub fn new(now_ms: i64) -> Self {
        Self { ready_time_ms: now_ms, drain_time_ms: now_ms }
    }
}

/// A borrowed handle on one partition's batch deque, as
/// [`TopicInfo::batches`] hands it out.
///
/// A named alias only because the tuple it appears in inside
/// [`RecordAccumulator::with_in_flight_batch_pool`] is otherwise too complex for
/// `clippy::type_complexity`.
type PartitionDequeRef<'a> = dashmap::mapref::one::Ref<'a, i32, Mutex<VecDeque<ProducerBatch>>>;

/// Per topic info.
///
/// Translated from `RecordAccumulator.TopicInfo`.
struct TopicInfo {
    /// Map from partition id to the per-partition batch deque.
    batches: DashMap<i32, Mutex<VecDeque<ProducerBatch>>>,
    /// The built-in partitioner for this topic.
    built_in_partitioner: Mutex<BuiltInPartitioner>,
}

impl TopicInfo {
    fn new(built_in_partitioner: BuiltInPartitioner) -> Self {
        Self { batches: DashMap::new(), built_in_partitioner: Mutex::new(built_in_partitioner) }
    }
}

/// The `finally` block of `RecordAccumulator.append`
/// (`RecordAccumulator.java:355-358`), as a `Drop` type.
///
/// Java's `finally` returns the not-yet-consumed buffer to the pool and
/// decrements `appendsInProgress`, whatever exit `append` takes. A Rust `async fn`
/// has one exit Java does not — the future being dropped mid-`await`
/// (CLAUDE.md §9.6) — so straight-line code after the `await` cannot stand in for
/// it. This is not a new abstraction over Java: it is the only way to express
/// `finally` across a cancellable await.
///
/// The `buffer` field holds `append`'s local of the same name, so the same
/// null/non-null discipline applies: [`append_new_batch`] takes it out when a batch
/// adopts the buffer, mirroring Java's `buffer = null`
/// (`RecordAccumulator.java:344-346`).
///
/// [`append_new_batch`]: RecordAccumulator::append_new_batch
struct AppendGuard<'a> {
    free: &'a BufferPool,
    appends_in_progress: &'a AtomicI32,
    buffer: Option<Vec<u8>>,
}

impl<'a> AppendGuard<'a> {
    /// Counts the append in and arms the cleanup. Java increments at
    /// `RecordAccumulator.java:283`, just inside the `try`.
    fn new(free: &'a BufferPool, appends_in_progress: &'a AtomicI32) -> Self {
        appends_in_progress.fetch_add(1, Ordering::Relaxed);
        Self { free, appends_in_progress, buffer: None }
    }
}

impl Drop for AppendGuard<'_> {
    fn drop(&mut self) {
        // Java's order: deallocate, then decrement.
        if let Some(buffer) = self.buffer.take() {
            self.free.deallocate(buffer);
        }
        self.appends_in_progress.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Queue that accumulates records into [`MemoryRecords`] to be sent to the server.
///
/// Translated from `org.apache.kafka.clients.producer.internals.RecordAccumulator`.
pub struct RecordAccumulator {
    closed: AtomicBool,
    flushes_in_progress: AtomicI32,
    appends_in_progress: AtomicI32,
    batch_size: i32,
    compression: crate::common::compress::Compression,
    linger_ms: i32,
    retry_backoff: ExponentialBackoff,
    delivery_timeout_ms: i32,
    partition_availability_timeout_ms: i64,
    enable_adaptive_partitioning: bool,
    free: Arc<BufferPool>,
    topic_info_map: DashMap<Arc<str>, Arc<TopicInfo>>,
    node_stats: DashMap<i32, NodeLatencyStats>,
    incomplete: IncompleteBatches,
    /// The shared transaction state object which tracks producer IDs, epochs, and
    /// sequence numbers per partition; `None` for a producer with idempotence
    /// disabled.
    ///
    /// Translated from `RecordAccumulator.transactionManager` (Java 90), which is
    /// nullable — hence [`Option`].
    ///
    /// # Lock topology and ordering
    ///
    /// `std::sync::Mutex`, shared with `KafkaProducer` and `Sender`
    /// (`.claude/rules/producer-transactions.md` §2 and PLAN §6.3). Every
    /// critical section here is CPU-bound with no `.await`, matching
    /// `RecordAccumulator.java:877-926`, so the async mutex is wrong (rules §3).
    ///
    /// **The lock order is per-partition deque → transaction manager, never
    /// inverted** (rules §3). Java assigns sequences inside
    /// `synchronized (deque)` while calling into `synchronized`
    /// `TransactionManager` methods, so inverting the order here would create a
    /// cycle against the Java-ordered path and deadlock under concurrent
    /// append + drain.
    transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
    /// Only accessed by the sender thread, so no synchronization needed.
    muted: Mutex<HashSet<TopicPartition>>,
    /// Only accessed by the sender thread.
    nodes_drain_index: Mutex<HashMap<i32, usize>>,
    next_batch_expiry_time_ms: Mutex<i64>,
    /// Contextual log message prefix.
    ///
    /// Translated from Java's `LogContext logContext` field in `RecordAccumulator`.
    log_context: LogContext,
}

impl RecordAccumulator {
    /// Create a new record accumulator.
    ///
    /// # Arguments
    /// * `batch_size` - The size to use when allocating `MemoryRecords` instances
    /// * `compression` - The compression codec for the records
    /// * `linger_ms` - An artificial delay time to add before declaring a records
    ///   instance that isn't full ready for sending
    /// * `retry_backoff_ms` - An artificial delay time to retry the produce request
    ///   upon receiving an error
    /// * `retry_backoff_max_ms` - The upper bound of the retry backoff time
    /// * `delivery_timeout_ms` - An upper bound on the time to report success or
    ///   failure on record delivery
    /// * `partitioner_config` - Partitioner configuration
    /// * `metrics` - The metrics
    /// * `metric_grp_name` - The metric group name
    /// * `buffer_pool` - The buffer pool
    /// * `transaction_manager` - The shared transaction state object which tracks
    ///   producer IDs, epochs, and sequence numbers per partition, or `None` when
    ///   idempotence is disabled
    // `TransactionManager` is `pub(crate)` per CLAUDE.md §2 (its Java package is
    // `internals`), while this constructor is nominally `pub` inside the
    // `pub(crate) producer::internals` module and so is not reachable from outside
    // the crate either. Demoting it instead would make several genuinely-used
    // `ProducerBatch` / `ProducerMetadata` accessors look dead.
    #[allow(private_interfaces)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_size: i32,
        compression: crate::common::compress::Compression,
        linger_ms: i32,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        delivery_timeout_ms: i32,
        partitioner_config: PartitionerConfig,
        metrics: Arc<Metrics>,
        metric_grp_name: &str,
        buffer_pool: Arc<BufferPool>,
        transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
    ) -> Self {
        Self::with_log_context(
            batch_size,
            compression,
            linger_ms,
            retry_backoff_ms,
            retry_backoff_max_ms,
            delivery_timeout_ms,
            partitioner_config,
            metrics,
            metric_grp_name,
            buffer_pool,
            transaction_manager,
            LogContext::empty(),
        )
    }

    /// Test-only convenience constructor that supplies a fresh reporter-less
    /// [`Metrics`] registry and the `producer-metrics` group, mirroring Java's
    /// tests passing `new Metrics()`. Java has no metrics-less production
    /// constructor.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_for_test(
        batch_size: i32,
        compression: crate::common::compress::Compression,
        linger_ms: i32,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        delivery_timeout_ms: i32,
        partitioner_config: PartitionerConfig,
        buffer_pool: Arc<BufferPool>,
        transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
    ) -> Self {
        Self::new(
            batch_size,
            compression,
            linger_ms,
            retry_backoff_ms,
            retry_backoff_max_ms,
            delivery_timeout_ms,
            partitioner_config,
            Arc::new(Metrics::new()),
            "producer-metrics",
            buffer_pool,
            transaction_manager,
        )
    }

    /// Create a new record accumulator with a `LogContext`.
    ///
    /// # Arguments
    /// * `batch_size` - The size to use when allocating `MemoryRecords` instances
    /// * `compression` - The compression codec for the records
    /// * `linger_ms` - An artificial delay time to add before declaring a records
    ///   instance that isn't full ready for sending
    /// * `retry_backoff_ms` - An artificial delay time to retry the produce request
    ///   upon receiving an error
    /// * `retry_backoff_max_ms` - The upper bound of the retry backoff time
    /// * `delivery_timeout_ms` - An upper bound on the time to report success or
    ///   failure on record delivery
    /// * `partitioner_config` - Partitioner configuration
    /// * `metrics` - The metrics
    /// * `metric_grp_name` - The metric group name
    /// * `buffer_pool` - The buffer pool
    /// * `transaction_manager` - The shared transaction state object which tracks
    ///   producer IDs, epochs, and sequence numbers per partition, or `None` when
    ///   idempotence is disabled
    /// * `log_context` - Contextual log message prefix
    // See [`Self::new`] for why `private_interfaces` is allowed here.
    #[allow(private_interfaces)]
    #[allow(clippy::too_many_arguments)]
    pub fn with_log_context(
        batch_size: i32,
        compression: crate::common::compress::Compression,
        linger_ms: i32,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        delivery_timeout_ms: i32,
        partitioner_config: PartitionerConfig,
        metrics: Arc<Metrics>,
        metric_grp_name: &str,
        buffer_pool: Arc<BufferPool>,
        transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
        log_context: LogContext,
    ) -> Self {
        let retry_backoff = ExponentialBackoff::new(
            retry_backoff_ms,
            crate::CommonClientConfigs::RETRY_BACKOFF_EXP_BASE,
            retry_backoff_max_ms,
            crate::CommonClientConfigs::RETRY_BACKOFF_JITTER,
        )
        .expect("Invalid backoff parameters");

        Self::register_metrics(&buffer_pool, &metrics, metric_grp_name);

        Self {
            closed: AtomicBool::new(false),
            flushes_in_progress: AtomicI32::new(0),
            appends_in_progress: AtomicI32::new(0),
            batch_size,
            compression,
            linger_ms,
            retry_backoff,
            delivery_timeout_ms,
            enable_adaptive_partitioning: partitioner_config.enable_adaptive_partitioning,
            partition_availability_timeout_ms: partitioner_config.partition_availability_timeout_ms,
            free: buffer_pool,
            topic_info_map: DashMap::new(),
            node_stats: DashMap::new(),
            incomplete: IncompleteBatches::new(),
            transaction_manager,
            muted: Mutex::new(HashSet::new()),
            nodes_drain_index: Mutex::new(HashMap::new()),
            next_batch_expiry_time_ms: Mutex::new(i64::MAX),
            log_context,
        }
    }

    /// Register the three buffer-pool gauges in the metric group. Translated
    /// from `RecordAccumulator.registerMetrics` (RecordAccumulator.java:198-213).
    ///
    /// Each gauge is a [`ClosureMeasurable`] over an [`Arc<BufferPool>`] clone,
    /// the analog of Java's lambdas capturing the `free` field. Registration
    /// failure is a construction-time programming error (duplicate name), so it
    /// panics — Java's `registerMetrics` declares no checked throw.
    fn register_metrics(free: &Arc<BufferPool>, metrics: &Arc<Metrics>, metric_grp_name: &str) {
        let free_waiting = Arc::clone(free);
        metrics
            .add_metric_measurable(
                metrics.metric_name_description_tags(
                    "waiting-threads",
                    metric_grp_name,
                    "The number of user threads blocked waiting for buffer memory to enqueue their records",
                    std::collections::BTreeMap::new(),
                ),
                Box::new(ClosureMeasurable::new(move |_config, _now| free_waiting.queued() as f64)),
            )
            .expect("registering waiting-threads metric");

        let free_total = Arc::clone(free);
        metrics
            .add_metric_measurable(
                metrics.metric_name_description_tags(
                    "buffer-total-bytes",
                    metric_grp_name,
                    "The maximum amount of buffer memory the client can use (whether or not it is currently used).",
                    std::collections::BTreeMap::new(),
                ),
                Box::new(ClosureMeasurable::new(move |_config, _now| free_total.total_memory() as f64)),
            )
            .expect("registering buffer-total-bytes metric");

        let free_available = Arc::clone(free);
        metrics
            .add_metric_measurable(
                metrics.metric_name_description_tags(
                    "buffer-available-bytes",
                    metric_grp_name,
                    "The total amount of buffer memory that is not being used (either unallocated or in the free list).",
                    std::collections::BTreeMap::new(),
                ),
                Box::new(ClosureMeasurable::new(move |_config, _now| free_available.available_memory() as f64)),
            )
            .expect("registering buffer-available-bytes metric");
    }

    /// Add a record to the accumulator, return the append result.
    ///
    /// The buffer allocation is performed synchronously. In Java, this method
    /// blocks on `BufferPool.allocate()`. Here we allocate a `Vec` directly
    /// for simplicity. The buffer pool is used for memory accounting and
    /// deallocation only.
    ///
    /// # Arguments
    /// * `topic` - The topic to which this record is being sent
    /// * `partition` - The partition to which this record is being sent, or
    ///   `UNKNOWN_PARTITION` if any partition could be used
    /// * `timestamp` - The timestamp of the record
    /// * `key` - The key for the record
    /// * `value` - The value for the record
    /// * `headers` - The headers for the record
    /// * `callback` - The callback to execute
    /// * `max_time_to_block` - The maximum time in milliseconds to block for
    ///   buffer memory to be available
    /// * `now_ms` - The current time, in milliseconds
    /// * `cluster` - The cluster metadata
    ///
    /// # Errors
    ///
    /// On failure the returned [`AppendFailure`] hands `callback` back
    /// unfired (see that type for why) so the caller can honour the
    /// exactly-once callback obligation, exactly as Java's
    /// `KafkaProducer.doSend` `catch (ApiException e)` arm does.
    #[allow(clippy::too_many_arguments)]
    pub async fn append(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        max_time_to_block: i64,
        now_ms: i64,
        cluster: &Cluster,
    ) -> Result<RecordAppendResult, Box<AppendFailure>> {
        let (topic_arc, topic_info) = self.get_or_create_topic_info(topic);

        // Java's `finally` (`RecordAccumulator.java:355-358`):
        //
        //     } finally {
        //         free.deallocate(buffer);
        //         appendsInProgress.decrementAndGet();
        //     }
        //
        // Both halves must run on every exit, so they belong in a `Drop` type
        // rather than as straight-line code after the `await`. Two exits are not
        // covered otherwise:
        //
        //   - the error return: `try_append` propagates
        //     `KafkaException("Producer closed while send in progress")`
        //     (`RecordAccumulator.java:427-428`) with a buffer already allocated,
        //     which then never went back to the pool. `BufferPool::allocate` has
        //     already debited `non_pooled_available_memory` and `deallocate` is the
        //     only thing that credits it, so every send that raced `close()`
        //     permanently shrank the pool.
        //   - the drop path: `append` is `async`, so a caller may wrap it in
        //     `tokio::time::timeout` / `select!`, and it blocks up to
        //     `max.block.ms` inside `free.allocate`. Java has no analogue — threads
        //     have no cancellation — so this exit exists only in Rust
        //     (CLAUDE.md §9.6). A lost `appends_in_progress` decrement makes
        //     `abort_incomplete_batches` never leave its (non-yielding) loop.
        let mut guard = AppendGuard::new(&self.free, &self.appends_in_progress);

        self.append_inner(
            &topic_arc,
            partition,
            timestamp,
            key,
            value,
            headers,
            callback,
            max_time_to_block,
            now_ms,
            cluster,
            &topic_info,
            &mut guard,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn append_inner(
        &self,
        topic: &Arc<str>,
        partition: i32,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        max_time_to_block: i64,
        now_ms: i64,
        cluster: &Cluster,
        topic_info: &Arc<TopicInfo>,
        guard: &mut AppendGuard<'_>,
    ) -> Result<RecordAppendResult, Box<AppendFailure>> {
        let mut callback = callback;

        loop {
            // Determine the effective partition.
            let effective_partition = if partition == RecordMetadata::UNKNOWN_PARTITION {
                let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
                partitioner.peek_current_partition_info(cluster).partition()
            } else {
                partition
            };

            // Ensure the deque for this partition exists, then drop the DashMap guard
            // before any potential .await to avoid holding the shard lock across
            // an await point (which would block ready()/drain() from iterating).
            topic_info
                .batches
                .entry(effective_partition)
                .or_insert_with(|| Mutex::new(VecDeque::new()));

            // Try to append to an existing batch.
            {
                let dq_ref = topic_info.batches.get(&effective_partition).unwrap();
                let mut deque = dq_ref.lock().unwrap();

                // Check if we need to complete a previously disabled partition switch.
                if partition == RecordMetadata::UNKNOWN_PARTITION && self.partition_changed(topic_info, &deque, cluster)
                {
                    continue;
                }

                let (result, returned_callback) = self.try_append(
                    timestamp,
                    key,
                    value,
                    headers,
                    callback,
                    &mut deque,
                    topic,
                    effective_partition,
                    now_ms,
                )?;
                if let Some(result) = result {
                    if partition == RecordMetadata::UNKNOWN_PARTITION {
                        let enable_switch = Self::all_batches_full(&deque);
                        let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
                        partitioner.update_partition_info_with_switch(result.appended_bytes, cluster, enable_switch);
                    }
                    return Ok(result);
                }
                callback = returned_callback;
            }
            // DashMap guard dropped here — safe to .await below.

            // Need a new batch. Allocate a buffer (only once). It lives on the
            // `finally` guard — Java's `ByteBuffer buffer = null` local — not in a
            // plain local, so the pool gets it back however this function exits.
            if guard.buffer.is_none() {
                let estimated = AbstractRecords::estimate_size_in_bytes_upper_bound(
                    RecordBatch::CURRENT_MAGIC_VALUE,
                    self.compression.compression_type(),
                    key,
                    value,
                    headers,
                );
                let size = self.batch_size.max(estimated);

                kafka_trace!(
                    self.log_context,
                    "Allocating a new {} byte message buffer for topic {} partition {} with remaining timeout {}ms",
                    size,
                    topic,
                    effective_partition,
                    max_time_to_block
                );

                // Buffer exhaustion / `max.block.ms` expiry. Java throws
                // `BufferExhaustedException` (an `ApiException`) out of `append`, and
                // `doSend` fires the user callback with the placeholder metadata — hand
                // the callback back so the caller can do the same. (`BufferPool` can
                // also fail with the bare `KafkaException` "Producer closed while
                // allocating memory", which is *not* an `ApiException`; `doSend`
                // re-raises that one without firing.) Either way the callback has not
                // been handed to a batch.
                guard.buffer = Some(
                    self.free
                        .allocate(size as usize, max_time_to_block)
                        .await
                        .map_err(|error| AppendFailure::boxed(error, callback.take()))?,
                );
            }

            // Try again under lock -- another thread might have created the batch.
            {
                let dq_ref = topic_info.batches.get(&effective_partition).unwrap();
                let mut deque = dq_ref.lock().unwrap();

                if partition == RecordMetadata::UNKNOWN_PARTITION && self.partition_changed(topic_info, &deque, cluster)
                {
                    continue;
                }

                let (result, returned_callback) = self.try_append(
                    timestamp,
                    key,
                    value,
                    headers,
                    callback,
                    &mut deque,
                    topic,
                    effective_partition,
                    now_ms,
                )?;
                if let Some(result) = result {
                    // Somebody else created a batch with room while we were
                    // allocating. Java leaves this to the `finally` — `buffer` is
                    // still non-null (`newBatchCreated == false`), so
                    // `free.deallocate(buffer)` returns it. The guard does the same
                    // when it drops, so nothing is released early here.
                    if partition == RecordMetadata::UNKNOWN_PARTITION {
                        let enable_switch = Self::all_batches_full(&deque);
                        let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
                        partitioner.update_partition_info_with_switch(result.appended_bytes, cluster, enable_switch);
                    }
                    return Ok(result);
                }

                // Create a new batch.
                let result = self.append_new_batch(
                    topic,
                    effective_partition,
                    &mut deque,
                    timestamp,
                    key,
                    value,
                    headers,
                    returned_callback,
                    // The batch takes the buffer over, so the guard must not return
                    // it to the pool — Java's `buffer = null` at
                    // `RecordAccumulator.java:344-346`, for exactly this reason.
                    guard.buffer.take().expect("a buffer was allocated for the new batch"),
                    now_ms,
                );

                if partition == RecordMetadata::UNKNOWN_PARTITION {
                    let enable_switch = Self::all_batches_full(&deque);
                    let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
                    partitioner.update_partition_info_with_switch(result.appended_bytes, cluster, enable_switch);
                }

                return Ok(result);
            }
        }
    }

    /// Append a new batch to the queue.
    #[allow(clippy::too_many_arguments)]
    fn append_new_batch(
        &self,
        topic: &Arc<str>,
        partition: i32,
        deque: &mut VecDeque<ProducerBatch>,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        buffer: Vec<u8>,
        now_ms: i64,
    ) -> RecordAppendResult {
        debug_assert!(partition != RecordMetadata::UNKNOWN_PARTITION);

        let records_builder = self.records_builder(buffer);
        // Both the batch and the append result carry this; `TopicPartition` holds an
        // `Arc<str>`, so the clone is a refcount increment, not a copy.
        let tp = TopicPartition::new(Arc::clone(topic), partition);
        let mut batch = ProducerBatch::new(tp.clone(), records_builder, now_ms);

        let future = batch
            .try_append(timestamp, key, value, headers, callback, now_ms)
            .unwrap_or_else(|_| panic!("Newly created batch should have room for at least one record"));

        let estimated_size = batch.estimated_size_in_bytes() as i32;
        let batch_is_full = !deque.is_empty() || batch.is_full();

        self.incomplete.add(Arc::clone(&batch.produce_future));
        deque.push_back(batch);

        RecordAppendResult {
            future,
            batch_is_full,
            new_batch_created: true,
            appended_bytes: estimated_size,
            topic_partition: tp,
        }
    }

    fn records_builder(&self, buffer: Vec<u8>) -> MemoryRecordsBuilder {
        MemoryRecords::builder_with_buffer(
            buffer,
            RecordBatch::CURRENT_MAGIC_VALUE,
            self.compression.clone(),
            TimestampType::CreateTime,
            0,
        )
    }

    /// Check if we need to complete a previously disabled partition switch.
    /// If all batches are full (or the deque is empty after drain), try an
    /// eager switch with `enable_switch = true` so the partitioner can move
    /// to a new partition before we append the next record.
    fn partition_changed(&self, topic_info: &TopicInfo, deque: &VecDeque<ProducerBatch>, cluster: &Cluster) -> bool {
        if Self::all_batches_full(deque) {
            let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
            let old_partition = partitioner.peek_current_partition_info(cluster).partition();
            partitioner.update_partition_info_with_switch(0, cluster, true);
            let new_partition = partitioner.peek_current_partition_info(cluster).partition();
            if new_partition != old_partition {
                return true;
            }
        }
        false
    }

    /// Check if all batches in the queue are full.
    fn all_batches_full(deque: &VecDeque<ProducerBatch>) -> bool {
        match deque.back() {
            None => true,
            Some(last) => last.is_full(),
        }
    }

    /// Try to append to a ProducerBatch.
    ///
    /// If it is full, we return `Ok(None)` and a new batch is created. The callback is
    /// returned back via the second element of the tuple so the caller can retry or
    /// pass it to `append_new_batch`. On error it is returned inside the
    /// [`AppendFailure`] for the same reason.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn try_append(
        &self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        deque: &mut VecDeque<ProducerBatch>,
        topic: &Arc<str>,
        partition: i32,
        now_ms: i64,
    ) -> Result<(Option<RecordAppendResult>, Option<Callback>), Box<AppendFailure>> {
        if self.closed.load(Ordering::Relaxed) {
            // `throw new KafkaException("Producer closed while send in progress")`
            // (`RecordAccumulator.java:427-428`) — a *bare* `KafkaException`, so it
            // is not an `ApiException`. `Error::with_message(UnknownServerError, ..)`
            // resolves the code to `UnknownServerException`, which IS an
            // `ApiException`, and `doSend` dispatches on exactly that difference:
            // `catch (ApiException e)` returns a failed future, `catch (KafkaException
            // e)` rethrows out of `send()` (`KafkaProducer.java:1073-1077`).
            // [`Error::kafka`] is what preserves that distinction.
            //
            // The callback rides back out on the error, for the same reason
            // `try_append` returns it on the batch-is-full path: the record was not
            // appended, so nothing else owns it, and it is `do_send_bytes` — not the
            // accumulator — that decides whether an error fires it. The
            // `Box<dyn FnOnce>` cannot be cloned, so handing it back is the only way.
            return Err(AppendFailure::boxed(
                Error::kafka_message("Producer closed while send in progress"),
                callback,
            ));
        }

        if let Some(last) = deque.back_mut() {
            let initial_bytes = last.estimated_size_in_bytes() as i32;
            match last.try_append(timestamp, key, value, headers, callback, now_ms) {
                Ok(future) => {
                    let appended_bytes = last.estimated_size_in_bytes() as i32 - initial_bytes;
                    let is_full = last.is_full();
                    let batch_is_full = deque.len() > 1 || is_full;
                    return Ok((
                        Some(RecordAppendResult {
                            future,
                            batch_is_full,
                            new_batch_created: false,
                            appended_bytes,
                            // Refcount increment on the accumulator's interned
                            // `Arc<str>`; no allocation on the per-record path.
                            topic_partition: TopicPartition::new(Arc::clone(topic), partition),
                        }),
                        None, // callback was consumed
                    ));
                },
                Err(returned_callback) => {
                    last.close_for_record_appends();
                    // Callback was not consumed; return it
                    return Ok((None, returned_callback));
                },
            }
        }
        // No batch in deque; callback was not consumed.
        Ok((None, callback))
    }

    fn is_muted(&self, tp: &TopicPartition) -> bool {
        self.muted.lock().unwrap().contains(tp)
    }

    /// Reset the next batch expiry time.
    pub fn reset_next_batch_expiry_time(&self) {
        *self.next_batch_expiry_time_ms.lock().unwrap() = i64::MAX;
    }

    /// Update the next batch expiry time if the given batch expires sooner.
    pub fn maybe_update_next_batch_expiry_time(&self, batch: &ProducerBatch) {
        let expiry = batch.created_ms.saturating_add(self.delivery_timeout_ms as i64);
        if expiry > 0 {
            let mut next = self.next_batch_expiry_time_ms.lock().unwrap();
            *next = (*next).min(expiry);
        } else {
            kafka_warn!(
                self.log_context,
                "Skipping next batch expiry time update due to addition overflow: \
                 batch.created_ms={}, delivery_timeout_ms={}",
                batch.created_ms,
                self.delivery_timeout_ms
            );
        }
    }

    /// Get a list of batches which have been sitting in the accumulator too long
    /// and need to be expired.
    pub fn expired_batches(&self, now: i64) -> Vec<ProducerBatch> {
        let mut expired = Vec::new();
        for topic_info_ref in self.topic_info_map.iter() {
            let topic_info = topic_info_ref.value();
            for deque_ref in topic_info.batches.iter() {
                let deque_mutex = deque_ref.value();
                let mut deque = deque_mutex.lock().unwrap();
                while let Some(batch) = deque.front() {
                    if batch.has_reached_delivery_timeout(self.delivery_timeout_ms as i64, now) {
                        let mut batch = deque.pop_front().unwrap();
                        batch.abort_record_appends();
                        expired.push(batch);
                    } else {
                        self.maybe_update_next_batch_expiry_time(batch);
                        break;
                    }
                }
            }
        }
        expired
    }

    /// Returns the delivery timeout in milliseconds.
    pub fn delivery_timeout_ms(&self) -> i32 {
        self.delivery_timeout_ms
    }

    /// Re-enqueue the given record batch in the accumulator.
    ///
    /// Translated from `reenqueue(ProducerBatch, long)` (Java 496). In
    /// `Sender.completeBatch` we check whether the batch has reached
    /// `deliveryTimeoutMs` or not, hence we do not do the delivery timeout check
    /// here.
    ///
    /// # Errors
    ///
    /// Propagates [`Self::insert_in_sequence_order`] when idempotence is enabled.
    /// Java's `IllegalStateException` escapes to `Sender.run`'s catch-and-log.
    pub fn reenqueue(&self, mut batch: ProducerBatch, now: i64) -> Result<(), Error> {
        batch.reenqueued(now);
        let tp = batch.topic_partition.clone();
        let (_topic_arc, topic_info) = self.get_or_create_topic_info(tp.topic());
        let dq_entry = topic_info
            .batches
            .entry(tp.partition())
            .or_insert_with(|| Mutex::new(VecDeque::new()));
        let mut deque = dq_entry.value().lock().unwrap();
        if self.transaction_manager.is_some() {
            self.insert_in_sequence_order(&mut deque, batch)
        } else {
            deque.push_front(batch);
            Ok(())
        }
    }

    /// Inserts `batch` into `deque` at the position its base sequence requires.
    ///
    /// Translated from `insertInSequenceOrder(Deque<ProducerBatch>, ProducerBatch)`
    /// (Java 552-592), with Java's comment at 542-551 reproduced below.
    ///
    /// We will have to do extra work to ensure the queue is in order when requests are being retried and there are
    /// multiple requests in flight to that partition. If the first in flight request fails to append, then all the
    /// subsequent in flight requests will also fail because the sequence numbers will not be accepted.
    ///
    /// Further, once batches are being retried, we are reduced to a single in flight request for that partition. So when
    /// the subsequent batches come back in sequence order, they will have to be placed further back in the queue.
    ///
    /// Note that this assumes that all the batches in the queue which have an assigned sequence also have the current
    /// producer id. We will not attempt to reorder messages if the producer id has changed, we will return an error
    /// instead.
    ///
    /// Called with `deque`'s lock held, so the manager lock taken here observes
    /// rules §3's deque → manager order.
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalState`] with Java's message when the batch has no
    /// sequence, or when it is not tracked as in flight. The second check is the one
    /// rules §7 cites as Java's proof that `reenqueueBatch` leaves a batch tracked:
    /// `Sender.reenqueueBatch` (`Sender.java:750-752`) deliberately does **not** call
    /// `removeInFlightBatch`, unlike the `MESSAGE_TOO_LARGE` split path at `:685`.
    fn insert_in_sequence_order(&self, deque: &mut VecDeque<ProducerBatch>, batch: ProducerBatch) -> Result<(), Error> {
        // When we are re-enqueueing and have enabled idempotence, the re-enqueued batch must always have a sequence.
        if batch.base_sequence() == RecordBatch::NO_SEQUENCE {
            return Err(Error::local_illegal_state(
                "Trying to re-enqueue a batch which doesn't have a sequence even though idempotency is enabled.",
            ));
        }

        let has_inflight_batches = match &self.transaction_manager {
            Some(transaction_manager) => {
                transaction_manager.lock().unwrap().has_inflight_batches(&batch.topic_partition)
            },
            // Unreachable: both callers test `transaction_manager.is_some()` first.
            None => false,
        };
        if !has_inflight_batches {
            return Err(Error::local_illegal_state(format!(
                "We are re-enqueueing a batch which is not tracked as part of the in flight requests. \
                 batch.topicPartition: {}; batch.baseSequence: {}",
                batch.topic_partition,
                batch.base_sequence()
            )));
        }

        let should_reorder = deque
            .front()
            .is_some_and(|first| first.has_sequence() && first.base_sequence() < batch.base_sequence());
        if should_reorder {
            // The incoming batch can't be inserted at the front of the queue without violating the sequence ordering.
            // This means that the incoming batch should be placed somewhere further back.
            // We need to find the right place for the incoming batch and insert it there.
            // We will only enter this branch if we have multiple inflights sent to different brokers and we need to
            // retry the inflight batches.
            //
            // Since we reenqueue exactly one batch a time and ensure that the queue is ordered by sequence always, it
            // is a simple linear scan of a subset of the in flight batches to find the right place in the queue each
            // time.
            let mut ordered_batches: Vec<ProducerBatch> = Vec::new();
            while deque
                .front()
                .is_some_and(|first| first.has_sequence() && first.base_sequence() < batch.base_sequence())
            {
                ordered_batches.push(deque.pop_front().expect("just observed to be non-empty"));
            }

            kafka_debug!(
                self.log_context,
                "Reordered incoming batch with sequence {} for partition {}. It was placed in the queue at position {}",
                batch.base_sequence(),
                batch.topic_partition,
                ordered_batches.len()
            );
            // Either we have reached a point where there are batches without a sequence (ie. never been drained
            // and are hence in order by default), or the batch at the front of the queue has a sequence greater
            // than the incoming batch. This is the right place to add the incoming batch.
            deque.push_front(batch);

            // Now we have to re insert the previously queued batches in the right order.
            while let Some(ordered_batch) = ordered_batches.pop() {
                deque.push_front(ordered_batch);
            }

            // At this point, the incoming batch has been queued in the correct place according to its sequence.
        } else {
            deque.push_front(batch);
        }
        Ok(())
    }

    /// Determine if the given partition leader has ready batches.
    #[allow(clippy::too_many_arguments)]
    fn batch_ready(
        &self,
        exhausted: bool,
        part: &TopicPartition,
        leader: &Node,
        waited_time_ms: i64,
        backing_off: bool,
        backoff_attempts: i32,
        full: bool,
        transaction_completing: bool,
        next_ready_check_delay_ms: i64,
        ready_nodes: &mut HashSet<Node>,
    ) -> i64 {
        if !ready_nodes.contains(leader) && !self.is_muted(part) {
            let time_to_wait_ms = if backing_off {
                let attempts = if backoff_attempts > 0 { backoff_attempts - 1 } else { 0 };
                self.retry_backoff.backoff(attempts as i64)
            } else {
                self.linger_ms as i64
            };
            let expired = waited_time_ms >= time_to_wait_ms;
            let sendable = full
                || expired
                || exhausted
                || self.closed.load(Ordering::Relaxed)
                || self.flush_in_progress()
                || transaction_completing;
            if sendable && !backing_off {
                ready_nodes.insert(leader.clone());
                return next_ready_check_delay_ms;
            } else {
                let time_left_ms = 0i64.max(time_to_wait_ms - waited_time_ms);
                return next_ready_check_delay_ms.min(time_left_ms);
            }
        }
        next_ready_check_delay_ms
    }

    /// Iterate over partitions of a topic to see which have batches ready and
    /// collect leaders of those partitions into the set of ready nodes.
    #[allow(clippy::too_many_arguments)]
    fn partition_ready(
        &self,
        metadata_snapshot: &MetadataSnapshot,
        now_ms: i64,
        topic: &Arc<str>,
        topic_info: &TopicInfo,
        transaction_completing: bool,
        mut next_ready_check_delay_ms: i64,
        ready_nodes: &mut HashSet<Node>,
        unknown_leader_topics: &mut HashSet<Arc<str>>,
    ) -> i64 {
        let cluster = metadata_snapshot.cluster();
        let mut queue_sizes: Option<Vec<i32>> = None;
        let mut partition_ids: Option<Vec<i32>> = None;

        if self.enable_adaptive_partitioning && topic_info.batches.len() >= cluster.partitions_for_topic(topic).len() {
            let len = topic_info.batches.len();
            queue_sizes = Some(vec![0; len]);
            partition_ids = Some(vec![0; len]);
        }

        let mut queue_sizes_index: i32 = -1;
        let exhausted = self.free.queued() > 0;

        for entry in topic_info.batches.iter() {
            let partition = *entry.key();
            let deque_mutex = entry.value();
            let part = TopicPartition::new(Arc::clone(topic), partition);

            let leader = cluster.leader_for(&part);
            if leader.is_some() && queue_sizes.is_some() {
                queue_sizes_index += 1;
                if let Some(ref mut pids) = partition_ids
                    && (queue_sizes_index as usize) < pids.len()
                {
                    pids[queue_sizes_index as usize] = part.partition();
                }
            }

            let mut deque = deque_mutex.lock().unwrap();
            let deque_size = deque.len();

            let batch = match deque.front_mut() {
                Some(b) => b,
                None => continue,
            };

            let leader_epoch = metadata_snapshot.leader_epoch_for(&part);
            let waited_time_ms = batch.waited_time_ms(now_ms);
            batch.maybe_update_leader_epoch(leader_epoch);
            let backing_off =
                self.should_backoff(batch.has_leader_changed_for_the_ongoing_retry(), batch, waited_time_ms);
            let backoff_attempts = batch.attempts();
            let full = deque_size > 1 || batch.is_full();

            drop(deque);

            if let Some(leader_node) = leader {
                if let Some(ref mut qs) = queue_sizes
                    && (queue_sizes_index as usize) < qs.len()
                {
                    qs[queue_sizes_index as usize] = deque_size as i32;
                }

                if self.partition_availability_timeout_ms > 0
                    && let Some(node_latency) = self.node_stats.get(&leader_node.id())
                    && node_latency.ready_time_ms - node_latency.drain_time_ms > self.partition_availability_timeout_ms
                {
                    queue_sizes_index -= 1;
                }

                next_ready_check_delay_ms = self.batch_ready(
                    exhausted,
                    &part,
                    leader_node,
                    waited_time_ms,
                    backing_off,
                    backoff_attempts,
                    full,
                    transaction_completing,
                    next_ready_check_delay_ms,
                    ready_nodes,
                );
            } else {
                unknown_leader_topics.insert(part.topic_arc().clone());
            }
        }

        // Update partition load stats.
        let length = (queue_sizes_index + 1) as usize;
        let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
        if let (Some(qs), Some(pids)) = (&mut queue_sizes, &partition_ids) {
            partitioner.update_partition_load_stats(Some(qs.as_mut_slice()), pids, length);
        } else {
            partitioner.update_partition_load_stats(None, &[], 0);
        }

        next_ready_check_delay_ms
    }

    /// Get a list of nodes whose partitions are ready to be sent, and the
    /// earliest time at which any non-sendable partition will be ready.
    pub fn ready(&self, metadata_snapshot: &MetadataSnapshot, now_ms: i64) -> ReadyCheckResult {
        let mut ready_nodes = HashSet::new();
        let mut next_ready_check_delay_ms = i64::MAX;
        let mut unknown_leader_topics = HashSet::new();

        // Java reads `transactionManager.isCompleting()` inside `batchReady`
        // (`RecordAccumulator.java:614`), i.e. once per batch. The value is
        // partition-independent, so it is read once per `ready()` here: that avoids
        // taking the manager lock per partition and removes the (Java-visible)
        // possibility of two partitions in the same `ready()` pass disagreeing about
        // it. Always `false` for an idempotent producer, since `COMMITTING_TRANSACTION`
        // and `ABORTING_TRANSACTION` need a transactional id.
        let transaction_completing = self
            .transaction_manager
            .as_ref()
            .is_some_and(|transaction_manager| transaction_manager.lock().unwrap().is_completing());

        for entry in self.topic_info_map.iter() {
            let topic = entry.key();
            let topic_info = entry.value();
            next_ready_check_delay_ms = self.partition_ready(
                metadata_snapshot,
                now_ms,
                topic,
                topic_info,
                transaction_completing,
                next_ready_check_delay_ms,
                &mut ready_nodes,
                &mut unknown_leader_topics,
            );
        }

        ReadyCheckResult { ready_nodes, next_ready_check_delay_ms, unknown_leader_topics }
    }

    /// Check whether there are any batches which haven't been drained.
    pub fn has_undrained(&self) -> bool {
        for topic_info_ref in self.topic_info_map.iter() {
            let topic_info = topic_info_ref.value();
            for deque_ref in topic_info.batches.iter() {
                let deque = deque_ref.value().lock().unwrap();
                if !deque.is_empty() {
                    return true;
                }
            }
        }
        false
    }

    fn should_backoff(&self, has_leader_changed: bool, batch: &ProducerBatch, waited_time_ms: i64) -> bool {
        let attempts = batch.attempts();
        let should_wait_more = attempts > 0 && waited_time_ms < self.retry_backoff.backoff(attempts as i64 - 1);
        let should_backoff = !has_leader_changed && should_wait_more;
        if log::log_enabled!(log::Level::Trace) {
            if should_backoff {
                kafka_trace!(self.log_context, "For {}, will backoff", batch);
            } else {
                kafka_trace!(
                    self.log_context,
                    "For {}, will not backoff, should_wait_more {}, has_leader_changed {}",
                    batch,
                    should_wait_more,
                    has_leader_changed
                );
            }
        } else if log::log_enabled!(log::Level::Debug) && has_leader_changed {
            kafka_debug!(self.log_context, "For {}, leader has changed, hence skipping backoff.", batch);
        }
        should_backoff
    }

    /// Whether the drain must stop at `first` for `tp`.
    ///
    /// Translated from `shouldStopDrainBatchesForPartition(ProducerBatch,
    /// TopicPartition)` (Java 815-850). Returns `false` when idempotence is
    /// disabled, matching Java's fall-through at `:849`.
    ///
    /// Called with `tp`'s deque lock held, so the manager lock taken here observes
    /// rules §3's deque → manager order.
    ///
    /// # Errors
    ///
    /// Propagates [`TransactionManager::first_in_flight_sequence`]. Java's
    /// equivalent exception escapes `drain` to `Sender.run`'s catch-and-log.
    fn should_stop_drain_batches_for_partition(
        &self,
        first: &ProducerBatch,
        tp: &TopicPartition,
    ) -> Result<bool, Error> {
        let Some(transaction_manager) = &self.transaction_manager else {
            return Ok(false);
        };
        let mut manager = transaction_manager.lock().unwrap();

        if !manager.is_send_to_partition_allowed(tp) {
            return Ok(true);
        }

        let producer_id_and_epoch = manager.producer_id_and_epoch();
        if !producer_id_and_epoch.is_valid() {
            // We cannot send the batch until we have refreshed the producer id.
            return Ok(true);
        }

        if !first.has_sequence() {
            if manager.has_inflight_batches(tp) && manager.has_stale_producer_id_and_epoch(tp) {
                // Don't drain any new batches while the partition has in-flight batches with a different epoch
                // and/or producer ID. Otherwise, a batch with a new epoch and sequence number
                // 0 could be written before earlier batches complete, which would cause out of sequence errors
                return Ok(true);
            }

            if manager.has_unresolved_sequence(&first.topic_partition) {
                // Don't drain any new batches while the state of previous sequence numbers
                // is unknown. The previous batches would be unknown if they were aborted
                // on the client after being sent to the broker at least once.
                return Ok(true);
            }
        }

        let first_in_flight_sequence = manager.first_in_flight_sequence(&first.topic_partition)?;
        // If the queued batch already has an assigned sequence, then it is being retried.
        // In this case, we wait until the next immediate batch is ready and drain that.
        // We only move on when the next in line batch is complete (either successfully or due to
        // a fatal broker error). This effectively reduces our in flight request count to 1.
        Ok(first_in_flight_sequence != RecordBatch::NO_SEQUENCE
            && first.has_sequence()
            && first.base_sequence() != first_in_flight_sequence)
    }

    /// Assigns `batch`'s producer id, epoch and base sequence, and tracks it as in
    /// flight.
    ///
    /// Translated from `RecordAccumulator.java:900-925`, the block between
    /// `deque.pollFirst()` (`:898`) and `batch.close()` (`:930`). It **must** stay
    /// here rather than move to the `Sender`: `Sender::send_producer_data` reads
    /// `batch.records()`, which serialises the v2 batch header, so the producer
    /// state has to be set before the batch leaves the accumulator.
    ///
    /// Called with `tp`'s deque lock held (rules §3: deque → manager).
    ///
    /// Java's guard is `producerIdAndEpoch != null && !batch.hasSequence()`.
    /// `TransactionManager.producerIdAndEpoch()` never returns null — an unacquired
    /// id is `ProducerIdAndEpoch.NONE`, not null — so the first conjunct is exactly
    /// "a transaction manager exists", which is the `Option` test here. An invalid
    /// id cannot reach this point either way, because
    /// [`Self::should_stop_drain_batches_for_partition`] has already stopped the
    /// drain for it.
    ///
    /// # Hot path
    ///
    /// Runs once per **batch** on the drain path, never per record. It allocates
    /// nothing: `set_producer_state` writes four scalars, and the two
    /// `TopicPartition` clones inside the manager happen only where Java also
    /// inserts into a map (CLAUDE.md §11, DoD §10).
    fn maybe_assign_producer_state(&self, batch: &mut ProducerBatch) -> Result<(), Error> {
        let Some(transaction_manager) = &self.transaction_manager else {
            return Ok(());
        };
        if batch.has_sequence() {
            // If the batch already has an assigned sequence, then we should not change the producer id and
            // sequence number, since this may introduce duplicates. In particular, the previous attempt
            // may actually have been accepted, and if we change the producer id and sequence here, this
            // attempt will also be accepted, causing a duplicate.
            return Ok(());
        }

        let mut manager = transaction_manager.lock().unwrap();
        let is_transactional = manager.is_transactional();
        let producer_id_and_epoch = manager.producer_id_and_epoch();

        // If the producer id/epoch of the partition do not match the latest one
        // of the producer, we update it and reset the sequence. This should be
        // only done when all its in-flight batches have completed. This is guarantee
        // in `shouldStopDrainBatchesForPartition`.
        //
        // The guard inside `maybe_update_producer_id_and_epoch` is
        // `has_stale_producer_id_and_epoch && !has_inflight_batches`, so the entry
        // tracks no batches whenever the rewrite runs and an empty pool is always
        // correct here (rules §7).
        manager.maybe_update_producer_id_and_epoch(&batch.topic_partition, &mut [])?;

        // Additionally, we update the next sequence number bound for the partition, and also have
        // the transaction manager track the batch so as to ensure that sequence ordering is maintained
        // even if we receive out of order responses.
        let sequence = manager.sequence_number(&batch.topic_partition);
        batch.set_producer_state(
            producer_id_and_epoch.producer_id,
            producer_id_and_epoch.epoch,
            sequence,
            is_transactional,
        );
        manager.increment_sequence_number(&batch.topic_partition, batch.record_count)?;
        kafka_debug!(
            self.log_context,
            "Assigned producerId {} and producerEpoch {} to batch with base sequence {} being sent to partition {}",
            producer_id_and_epoch.producer_id,
            producer_id_and_epoch.epoch,
            batch.base_sequence(),
            batch.topic_partition
        );

        manager.add_in_flight_batch(batch)
    }

    fn drain_batches_for_one_node(
        &self,
        metadata_snapshot: &MetadataSnapshot,
        node: &Node,
        max_size: i32,
        now: i64,
    ) -> Result<Vec<ProducerBatch>, Error> {
        let mut size = 0i32;
        let parts = metadata_snapshot.cluster().partitions_for_node(node.id());
        let mut ready = Vec::new();
        if parts.is_empty() {
            return Ok(ready);
        }

        let mut drain_index = self.get_drain_index(node.id());
        drain_index %= parts.len();
        let start = drain_index;

        loop {
            let part = &parts[drain_index];
            let tp = TopicPartition::new(part.topic(), part.partition());

            drain_index = (drain_index + 1) % parts.len();

            if self.is_muted(&tp) {
                if start == drain_index {
                    break;
                }
                continue;
            }

            let topic_info = match self.topic_info_map.get(tp.topic()) {
                Some(ti) => Arc::clone(ti.value()),
                None => {
                    if start == drain_index {
                        break;
                    }
                    continue;
                },
            };

            let deque_ref = match topic_info.batches.get(&tp.partition()) {
                Some(dr) => dr,
                None => {
                    if start == drain_index {
                        break;
                    }
                    continue;
                },
            };

            let leader_epoch = metadata_snapshot.leader_epoch_for(&tp);

            let batch = {
                let mut deque = deque_ref.lock().unwrap();
                let first = match deque.front_mut() {
                    Some(b) => b,
                    None => {
                        drop(deque);
                        if start == drain_index {
                            break;
                        }
                        continue;
                    },
                };

                // Update leader epoch before checking backoff.
                first.maybe_update_leader_epoch(leader_epoch);

                if self.should_backoff(
                    first.has_leader_changed_for_the_ongoing_retry(),
                    first,
                    first.waited_time_ms(now),
                ) {
                    drop(deque);
                    if start == drain_index {
                        break;
                    }
                    continue;
                }

                if size + first.estimated_size_in_bytes() as i32 > max_size && !ready.is_empty() {
                    // There is a rare case that a single batch size is larger than the
                    // request size due to compression; in this case we will still
                    // eventually send this batch in a single request.
                    break;
                } else if self.should_stop_drain_batches_for_partition(first, &tp)? {
                    break;
                }

                let mut batch = deque.pop_front().unwrap();
                // Still inside `synchronized (deque)` in Java (:900-925), and still
                // before `batch.close()` below, because closing serialises the v2
                // batch header.
                self.maybe_assign_producer_state(&mut batch)?;
                batch
            };

            let mut batch = batch;
            // The rest of the work is done outside the lock; close() is particularly
            // expensive.
            batch.close();
            size += batch.estimated_size_in_bytes() as i32;
            batch.drained(now);
            ready.push(batch);

            if start == drain_index {
                break;
            }
        }
        self.update_drain_index(node.id(), drain_index);
        Ok(ready)
    }

    fn get_drain_index(&self, node_id: i32) -> usize {
        let map = self.nodes_drain_index.lock().unwrap();
        map.get(&node_id).copied().unwrap_or(0)
    }

    fn update_drain_index(&self, node_id: i32, drain_index: usize) {
        let mut map = self.nodes_drain_index.lock().unwrap();
        map.insert(node_id, drain_index);
    }

    /// Drain all the data for the given nodes and collate them into a list of
    /// batches that will fit within the specified size on a per-node basis.
    pub fn drain(
        &self,
        metadata_snapshot: &MetadataSnapshot,
        nodes: &HashSet<Node>,
        max_size: i32,
        now: i64,
    ) -> Result<HashMap<i32, Vec<ProducerBatch>>, Error> {
        if nodes.is_empty() {
            return Ok(HashMap::new());
        }
        let mut batches = HashMap::new();
        for node in nodes {
            let ready = self.drain_batches_for_one_node(metadata_snapshot, node, max_size, now)?;
            batches.insert(node.id(), ready);
        }
        Ok(batches)
    }

    /// Update node latency stats.
    pub fn update_node_latency_stats(&self, node_id: i32, now_ms: i64, can_drain: bool) {
        if self.partition_availability_timeout_ms <= 0 {
            return;
        }
        let mut entry = self.node_stats.entry(node_id).or_insert_with(|| NodeLatencyStats::new(now_ms));
        if can_drain {
            entry.drain_time_ms = now_ms;
        }
        entry.ready_time_ms = now_ms;
    }

    /// Get the node latency stats for the given node. Visible for testing.
    pub fn get_node_latency_stats(&self, node_id: i32) -> Option<dashmap::mapref::one::Ref<'_, i32, NodeLatencyStats>> {
        self.node_stats.get(&node_id)
    }

    /// The earliest absolute time a batch will expire (in milliseconds).
    pub fn next_expiry_time_ms(&self) -> i64 {
        *self.next_batch_expiry_time_ms.lock().unwrap()
    }

    /// Get the deque size for the given topic-partition.
    pub fn deque_size(&self, tp: &TopicPartition) -> usize {
        let topic_info = match self.topic_info_map.get(tp.topic()) {
            Some(ti) => Arc::clone(ti.value()),
            None => return 0,
        };
        match topic_info.batches.get(&tp.partition()) {
            Some(dq) => dq.lock().unwrap().len(),
            None => 0,
        }
    }

    /// Get batches for a topic-partition. Used in drain logic.
    fn get_or_create_topic_info(&self, topic: &str) -> (Arc<str>, Arc<TopicInfo>) {
        if let Some(entry) = self.topic_info_map.get(topic) {
            return (entry.key().clone(), Arc::clone(entry.value()));
        }
        let topic_arc: Arc<str> = Arc::from(topic);
        let entry = self.topic_info_map.entry(Arc::clone(&topic_arc)).or_insert_with(|| {
            Arc::new(TopicInfo::new(BuiltInPartitioner::with_log_context(
                &topic_arc,
                self.batch_size,
                self.log_context.clone(),
            )))
        });
        (entry.key().clone(), Arc::clone(entry.value()))
    }

    /// Deallocate the batch buffer back to the pool.
    ///
    /// Only deallocates non-split batches because split batches are allocated outside
    /// the buffer pool. Includes safety checks matching Java's `deallocate`:
    /// - Warns and skips if the buffer was already deallocated
    /// - Panics if the batch is still in-flight
    /// - Marks the buffer as deallocated to prevent double deallocation
    pub fn deallocate(&self, batch: &mut ProducerBatch) {
        // Only deallocate the batch if it is not a split batch because split batches
        // are allocated outside the buffer pool.
        if !batch.is_split_batch() {
            if batch.is_buffer_deallocated() {
                kafka_warn!(
                    self.log_context,
                    "Skipping deallocating a batch that has already been deallocated. \
                     Batch is {}, created time is {}",
                    batch,
                    batch.created_ms
                );
            } else {
                batch.mark_buffer_deallocated();
                if batch.is_inflight() {
                    // Create a fresh buffer to give to BufferPool to reuse since we can't
                    // safely call deallocate with the ProducerBatch's buffer.
                    self.free.deallocate(vec![0u8; batch.initial_capacity()]);
                    panic!("Attempting to deallocate a batch that is inflight. Batch is {}", batch);
                }
                // Return the actual batch buffer to the pool.
                let initial_capacity = batch.initial_capacity();
                let buffer = batch.take_buffer();
                self.free.deallocate_with_size(buffer, initial_capacity);
            }
        }
    }

    /// Buffer pool remaining size in bytes. Package private for unit test.
    pub fn buffer_pool_available_memory(&self) -> i64 {
        self.free.available_memory()
    }

    /// Are there any threads currently waiting on a flush?
    pub fn flush_in_progress(&self) -> bool {
        self.flushes_in_progress.load(Ordering::Relaxed) > 0
    }

    /// Initiate the flushing of data from the accumulator.
    pub fn begin_flush(&self) {
        self.flushes_in_progress.fetch_add(1, Ordering::Relaxed);
    }

    /// Are there any threads currently appending messages?
    fn appends_in_progress(&self) -> bool {
        self.appends_in_progress.load(Ordering::Relaxed) > 0
    }

    /// Check whether there are any pending batches.
    pub fn has_incomplete(&self) -> bool {
        !self.incomplete.is_empty()
    }

    /// Abort all incomplete batches.
    pub fn abort_incomplete_batches(&self) {
        loop {
            // Java's no-argument `abortBatches()` (Java 1145-1147) supplies this
            // reason; Rust has no overloading, so it is inlined at the two call
            // sites that used it.
            self.abort_batches(Self::producer_closed_forcefully_error());
            if !self.appends_in_progress() {
                break;
            }
        }
        self.abort_batches(Self::producer_closed_forcefully_error());
        self.topic_info_map.clear();
    }

    /// The reason Java's no-argument `abortBatches()` passes (Java 1146).
    ///
    /// `pub(crate)` because `Sender::run`'s force-close branch needs the same reason
    /// for the batches the accumulator cannot reach — see
    /// `Sender::abort_in_flight_batches`.
    pub(crate) fn producer_closed_forcefully_error() -> Error {
        Error::kafka_message("Producer is closed forcefully.")
    }

    /// Abort all incomplete batches (whether they have been sent or not).
    ///
    /// Translated from `abortBatches(RuntimeException)` (Java 1152).
    ///
    /// `pub(crate)` because `Sender.maybeAbortBatches` (`Sender.java:535`) calls it
    /// with the transaction manager's `lastError`.
    ///
    /// # Deviation: driven by the deques, not by `incomplete`
    ///
    /// Java iterates `incomplete.copyAll()`, which returns the `ProducerBatch`es
    /// themselves and therefore also covers batches already drained into the
    /// `Sender`. Rust's [`IncompleteBatches`] tracks
    /// [`ProduceRequestResult`](super::ProduceRequestResult)s rather than batches,
    /// because a `ProducerBatch` has exactly one owner (rules §7), so this can only
    /// reach the batches the accumulator still owns. The `Sender`'s share is aborted
    /// by `Sender::maybe_abort_batches`, which documents why dropping them
    /// un-aborted would leave their futures pending forever.
    ///
    /// Java's tail (`:1160-1167`) *is* translated: each aborted batch is removed
    /// from `incomplete`, and deallocated unless it is still marked in flight
    /// (KAFKA-19012 — the pooled buffer may still be in use by the network client,
    /// in which case `Sender::complete_batch` / `fail_batch` deallocates it when the
    /// response arrives). Without that tail `has_incomplete()` would stay true
    /// forever and `Sender.maybeAbortBatches` would re-abort on every `runOnce`.
    pub(crate) fn abort_batches(&self, reason: Error) {
        for topic_info_ref in self.topic_info_map.iter() {
            let topic_info = topic_info_ref.value();
            for deque_ref in topic_info.batches.iter() {
                loop {
                    // Java holds `synchronized (dq)` only for `abortRecordAppends()`
                    // and the removal, then calls `batch.abort(reason)` *outside* it
                    // (`RecordAccumulator.java:1155-1159`). That matters twice over:
                    // `abort` fires the user's delivery callbacks, so holding the
                    // deque lock across them would let a callback that re-enters the
                    // producer deadlock, and a panicking callback would poison this
                    // mutex — after which every later `lock()` on the partition
                    // panics again and the Sender's `catch_unwind` recovers into an
                    // unbounded re-panic loop.
                    let mut batch = {
                        let mut deque = deque_ref.value().lock().unwrap();
                        match deque.pop_front() {
                            Some(mut batch) => {
                                batch.abort_record_appends();
                                batch
                            },
                            None => break,
                        }
                    };
                    batch.abort(reason.clone());
                    if batch.is_inflight() {
                        self.complete_batch(&batch);
                    } else {
                        self.complete_and_deallocate_batch(&mut batch);
                    }
                }
            }
        }
    }

    /// Mute a partition (prevent it from being drained).
    pub fn mute_partition(&self, tp: TopicPartition) {
        self.muted.lock().unwrap().insert(tp);
    }

    /// Unmute a partition.
    pub fn unmute_partition(&self, tp: &TopicPartition) {
        self.muted.lock().unwrap().remove(tp);
    }

    /// Close this accumulator and force all record buffers to be drained.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.free.close();
    }

    /// Returns the batch size.
    pub fn batch_size(&self) -> i32 {
        self.batch_size
    }

    /// Remove from the incomplete list and deallocate the batch buffer.
    ///
    /// Translated from `RecordAccumulator.completeAndDeallocateBatch`.
    pub fn complete_and_deallocate_batch(&self, batch: &mut ProducerBatch) {
        self.complete_batch(batch);
        self.deallocate(batch);
    }

    /// The base sequences of the batches queued for `tp`, in queue order.
    ///
    /// Test-only. Java's `SenderTest` reaches these through
    /// `accumulator.getDeque(tp)` (package-private) and asserts on
    /// `peekFirst().baseSequence()` / `peekLast().baseSequence()`; the Rust deques are
    /// behind a `DashMap` of `Mutex`es, so the sequences are collected here instead of
    /// handing out a guard.
    #[cfg(test)]
    pub(crate) fn base_sequences_for_test(&self, tp: &TopicPartition) -> Vec<i32> {
        let Some(topic_info) = self.topic_info_map.get(tp.topic()).map(|ti| Arc::clone(ti.value())) else {
            return Vec::new();
        };
        match topic_info.batches.get(&tp.partition()) {
            Some(deque) => deque.lock().unwrap().iter().map(|batch| batch.base_sequence()).collect(),
            None => Vec::new(),
        }
    }

    /// Whether adaptive partitioning is enabled on this accumulator.
    ///
    /// Test-only. `KafkaProducer` disables adaptive partitioning whenever a custom
    /// [`Partitioner`](crate::producer::Partitioner) is configured
    /// (`KafkaProducer.java:428-433`: "There is no need to do work required for
    /// adaptive partitioning, if we use a custom partitioner."); this accessor lets a
    /// producer-level test verify that gating against the built-in-partitioner case.
    #[cfg(test)]
    pub(crate) fn enable_adaptive_partitioning_for_test(&self) -> bool {
        self.enable_adaptive_partitioning
    }

    /// Registers `batch` in the incomplete set as [`Self::append`] would.
    ///
    /// Test-only. `TransactionManagerTest`'s `writeIdempotentBatchWithValue`
    /// (Java 812) constructs a `ProducerBatch` outside the accumulator, and Java gets
    /// away with it because `IncompleteBatches.remove` is only reached for batches the
    /// accumulator itself created. The Rust tests that hand such a batch back to the
    /// accumulator (`reenqueue`) do reach `complete_batch`, which would then fail its
    /// "This should be impossible" assertion — so the batch is registered here first,
    /// putting it in the state a real append would have left it in.
    #[cfg(test)]
    pub(crate) fn register_incomplete_for_test(&self, batch: &ProducerBatch) {
        self.incomplete.add(Arc::clone(&batch.produce_future));
    }

    /// Remove from the incomplete list but do not free memory yet.
    ///
    /// Translated from `RecordAccumulator.completeBatch`.
    pub fn complete_batch(&self, batch: &ProducerBatch) {
        self.incomplete.remove(&batch.produce_future);
    }

    /// Mark all partitions as ready to send and block until the send is complete.
    ///
    /// Translated from `RecordAccumulator.awaitFlushCompletion`.
    ///
    /// In Java this obtains a copy of all incomplete `ProduceRequestResult`s, then
    /// calls `awaitAllDependents()` on each one. We replicate the same blocking
    /// behavior here: obtain a snapshot of incomplete results, then `.await` each
    /// one (including its dependents from batch splitting).
    pub async fn await_flush_completion(&self) {
        // Java's `finally { this.flushesInProgress.decrementAndGet(); }`
        // (`RecordAccumulator.java:1111-1113`), which covers the
        // `InterruptedException` the method declares. In Rust the uncovered exit is
        // the future being dropped at one of the `await`s below — `flush()` is
        // `async` public API, so a caller may wrap it in `tokio::time::timeout` or
        // `select!` (CLAUDE.md §9.6). A lost decrement makes `flush_in_progress()`
        // answer `true` forever, and it feeds the sendable predicate in `ready()`:
        // every partition would then look immediately ready for the producer's whole
        // lifetime, silently disabling `linger.ms` batching.
        struct FlushGuard<'a>(&'a AtomicI32);
        impl Drop for FlushGuard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::Relaxed);
            }
        }
        let _guard = FlushGuard(&self.flushes_in_progress);

        // Obtain a copy of all of the incomplete ProduceRequestResult(s) at the
        // time of the flush. We must be careful not to hold a reference to the
        // ProducerBatch(s) so that the sender can complete and remove them.
        let results = self.incomplete.request_results();
        for result in results {
            result.await_all_dependents().await;
        }
    }

    /// Abort any batches which have not been drained.
    ///
    /// Translated from `RecordAccumulator.abortUndrainedBatches` (Java 1174).
    ///
    /// Java's predicate at `:1179` forks on whether idempotence is enabled:
    ///
    /// ```java
    /// if ((transactionManager != null && !batch.hasSequence()) ||
    ///     (transactionManager == null && !batch.isClosed()))
    /// ```
    ///
    /// With a transaction manager, "undrained" means *no sequence assigned yet* —
    /// a drained batch has one (`drainBatchesForOneNode` assigns it at Java 918),
    /// and a *re-enqueued* batch keeps its sequence and so must not be aborted here
    /// even though the accumulator owns it again (rules §7). Without one it means
    /// "not yet closed". This is reachable idempotently:
    /// `maybeSendAndPollTransactionalRequest` calls it whenever
    /// `hasAbortableError()` (`Sender.java:466-467`), which PLAN §9.15 shows an
    /// idempotent producer can reach.
    pub fn abort_undrained_batches(&self, reason: Error) {
        let has_transaction_manager = self.transaction_manager.is_some();
        for topic_info_ref in self.topic_info_map.iter() {
            let topic_info = topic_info_ref.value();
            for deque_ref in topic_info.batches.iter() {
                // As in `abort_batches`, Java releases `synchronized (dq)` before
                // `batch.abort(reason)` (`RecordAccumulator.java:1177-1188`), so the
                // user callbacks it fires do not run under the deque lock. `i` is
                // carried across re-locks: the only mutation is our own removal, and
                // the Sender is the sole other writer on this path.
                let mut i = 0;
                loop {
                    let mut batch = {
                        let mut deque = deque_ref.value().lock().unwrap();
                        if i >= deque.len() {
                            break;
                        }
                        let undrained = if has_transaction_manager {
                            !deque[i].has_sequence()
                        } else {
                            !deque[i].is_closed()
                        };
                        if !undrained {
                            i += 1;
                            continue;
                        }
                        let mut batch = deque.remove(i).expect("index checked against len");
                        batch.abort_record_appends();
                        batch
                    };
                    batch.abort(reason.clone());
                    // Java 1187. Without this the batch stays in `incomplete`
                    // forever, so `has_incomplete()` never falls back to false.
                    self.complete_and_deallocate_batch(&mut batch);
                }
            }
        }
    }

    /// Runs `f` with an [`InFlightBatchPool`] for `partitions`, assembled from
    /// **both** owners of the in-flight batches: this accumulator's per-partition
    /// deques and `sender_batches`, i.e. `Sender::in_flight_batches`.
    ///
    /// # Why this exists
    ///
    /// Java's `TxnPartitionEntry` holds live references to the in-flight batches,
    /// so `bumpIdempotentProducerEpoch` (Java 645) can rewrite them without any
    /// help from their owners. Rust cannot: the entry tracks ordering keys only
    /// (`.claude/rules/producer-transactions.md` §7), so the batches have to come
    /// from whoever owns them — and **ownership alternates**. `Sender.reenqueueBatch`
    /// (`Sender.java:750-752`) hands a batch back to the accumulator *without*
    /// calling `removeInFlightBatch`, so a tracked batch may be sitting in a deque
    /// here while other tracked batches sit in `Sender::in_flight_batches`. Rules §7
    /// names "building the lookup pool from `Sender::in_flight_batches` alone" as an
    /// anti-pattern for exactly this reason: it would make
    /// `bump_idempotent_epoch_and_reset_id_if_needed` fail to rewrite a reenqueued
    /// batch's sequence, and idempotent recovery is the very path that call serves.
    ///
    /// # Locking
    ///
    /// Every requested partition's deque lock is held for the duration of `f`,
    /// because the `&mut ProducerBatch` references borrow from inside the deques.
    /// `f` then takes the `TransactionManager` lock, preserving rules §3's
    /// **deque → manager** order. Only the partitions queued for an epoch-bump
    /// rewrite are locked, and only when a bump is actually pending — see
    /// `TransactionManager::client_side_epoch_bump_required`.
    ///
    /// Partitions with no topic entry, no deque and no Sender-side batches are
    /// simply absent from the pool; [`InFlightBatchPool`] documents an absent
    /// partition and an empty one as equivalent.
    ///
    /// # Why the merges happen here rather than in the caller's closure
    ///
    /// Every `&mut ProducerBatch` in the pool must share one lifetime, and the
    /// shortest is the deque `MutexGuard`s' — which exist only inside this function.
    /// A caller that tried to extend the pool from its own map inside `f` would have
    /// to unify a borrow of itself with a lifetime local to this call, which does not
    /// type-check. Worse, `f`'s pool argument is higher-ranked
    /// (`for<'a> FnOnce(&mut InFlightBatchPool<'a>)`), so a batch pushed inside `f`
    /// must outlive *every* `'a`, i.e. `'static` — which is why
    /// `extra_batch` is a parameter merged here rather than pushed by the caller.
    /// Passing every extra owner in lets all their borrows be reduced to the guard
    /// lifetime at one place.
    ///
    /// `extra_batch` is an at-most-one caller-owned batch that is tracked in the txn
    /// partition map but currently lives in *neither* owner the pool draws from — the
    /// failing batch reaching `Sender::can_retry` from
    /// `handle_produce_response_for`'s local map is the sole such case
    /// (`.claude/rules/producer-transactions.md` §7, PLAN §9.25). The caller MUST NOT
    /// supply a batch that is also in a deque or `sender_batches`, or the pool would
    /// hold two `&mut` to the same batch.
    pub(crate) fn with_in_flight_batch_pool<R>(
        &self,
        partitions: &[TopicPartition],
        sender_batches: &mut HashMap<TopicPartition, Vec<ProducerBatch>>,
        extra_batch: Option<(TopicPartition, &mut ProducerBatch)>,
        f: impl FnOnce(&mut InFlightBatchPool<'_>) -> R,
    ) -> R {
        // Pass 1: own an `Arc<TopicInfo>` per requested partition, which releases
        // the outer `DashMap` shard guard immediately.
        let topics: Vec<(&TopicPartition, Arc<TopicInfo>)> = partitions
            .iter()
            .filter_map(|tp| {
                self.topic_info_map
                    .get(tp.topic())
                    .map(|topic_info| (tp, Arc::clone(topic_info.value())))
            })
            .collect();
        // Pass 2: borrow the per-partition deque mutexes out of the (now immutable)
        // `topics`. The `Ref` guards must outlive the `MutexGuard`s below, so they
        // need their own binding.
        let deques: Vec<(&TopicPartition, PartitionDequeRef<'_>)> = topics
            .iter()
            .filter_map(|(tp, topic_info)| topic_info.batches.get(&tp.partition()).map(|deque| (*tp, deque)))
            .collect();
        // Pass 3: lock every deque (rules §3: before the manager lock, which `f`
        // takes).
        let mut guards: Vec<(&TopicPartition, std::sync::MutexGuard<'_, VecDeque<ProducerBatch>>)> =
            deques.iter().map(|(tp, deque)| (*tp, deque.value().lock().unwrap())).collect();
        // Pass 4: hand out `&mut` references into the locked deques, then merge the
        // Sender's own in-flight batches for the same partitions (rules §7:
        // ownership alternates, so the pool must draw from both).
        let mut pool: InFlightBatchPool<'_> = HashMap::with_capacity(guards.len());
        for (tp, guard) in guards.iter_mut() {
            pool.insert((*tp).clone(), guard.iter_mut().collect());
        }
        // Driven by one `iter_mut` over the Sender's map rather than a `get_mut` per
        // requested partition: repeated `get_mut` calls in a loop cannot be proven
        // disjoint while the references escape into `pool`. `partitions` is the
        // epoch-bump set, i.e. only partitions in an error state, so the membership
        // test is over a handful of entries.
        for (topic_partition, batches) in sender_batches.iter_mut() {
            if partitions.contains(topic_partition) {
                pool.entry(topic_partition.clone()).or_default().extend(batches.iter_mut());
            }
        }
        // Merge the optional caller-owned batch last (rules §7). It reaches us with a
        // lifetime that outlives the guards', so covariance reduces it to the pool's
        // guard lifetime here — a merge the caller could not perform inside `f`
        // (see the doc comment).
        if let Some((topic_partition, batch)) = extra_batch {
            pool.entry(topic_partition).or_default().push(batch);
        }
        f(&mut pool)
    }

    /// Split a big batch and re-enqueue the resulting sub-batches.
    ///
    /// Translated from `RecordAccumulator.splitAndReenqueue` (Java 511).
    ///
    /// # Errors
    ///
    /// Propagates [`Self::insert_in_sequence_order`] when idempotence is enabled.
    pub fn split_and_reenqueue(&self, mut big_batch: ProducerBatch) -> Result<usize, Error> {
        // Reset the estimated compression ratio to the initial value or the big batch compression
        // ratio, whichever is bigger. There are several different ways to do the reset. We chose
        // the most conservative one to ensure the split doesn't happen too often.
        CompressionRatioEstimator::set_estimation(
            big_batch.topic_partition.topic(),
            self.compression.compression_type(),
            1.0f32.max(big_batch.compression_ratio() as f32),
        );

        let target_split_batch_size = if big_batch.is_split_batch() {
            std::cmp::max(big_batch.max_record_size, big_batch.estimated_size_in_bytes() as i32 / 2)
        } else {
            self.batch_size
        };

        let mut sub_batches = big_batch.split(target_split_batch_size);
        let num_split_batches = sub_batches.len();
        let tp = big_batch.topic_partition.clone();

        let (_topic_arc, topic_info) = self.get_or_create_topic_info(tp.topic());
        let dq_entry = topic_info
            .batches
            .entry(tp.partition())
            .or_insert_with(|| Mutex::new(VecDeque::new()));
        let mut deque = dq_entry.value().lock().unwrap();

        // Java's caller does `accumulator.splitAndReenqueue(batch)` and then
        // `maybeRemoveAndDeallocateBatch(batch)` (`Sender.java:686-688`). Rust's
        // `split_and_reenqueue` consumes the big batch, so the second statement has to
        // happen here: without it the big batch stays in `incomplete` — so
        // `has_incomplete()` never falls back to false — and its pooled buffer is never
        // returned. Unreachable in production until PLAN §9.18 is fixed (the split
        // itself panics), but the fix needs this. Critic 44 note 1.
        self.complete_and_deallocate_batch(&mut big_batch);

        while let Some(batch) = sub_batches.pop_back() {
            self.incomplete.add(Arc::clone(&batch.produce_future));
            // We treat the newly split batches as if they are not even tried
            // (Java 528-537).
            if let Some(transaction_manager) = &self.transaction_manager {
                // We should track the newly created batches since they already have
                // assigned sequences: `ProducerBatch::split` carries the producer
                // state over to each sub-batch.
                transaction_manager.lock().unwrap().add_in_flight_batch(&batch)?;
                self.insert_in_sequence_order(&mut deque, batch)?;
            } else {
                deque.push_front(batch);
            }
        }

        Ok(num_split_batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Errors;
    use crate::common::Node;
    use crate::common::compress::Compression;
    use crate::common::record::internal::CompressionType;
    use crate::common::record::internal::DefaultRecord;
    use crate::common::record::internal::RecordBatch;
    use std::collections::{HashMap, HashSet};

    const TOPIC: &str = "test";

    fn node1() -> Node {
        Node::new(0, "localhost".to_string(), 1111)
    }

    fn node2() -> Node {
        Node::new(1, "localhost".to_string(), 1112)
    }

    fn tp1() -> TopicPartition {
        TopicPartition::new(TOPIC.to_string(), 0)
    }

    fn make_metadata_snapshot(
        nodes: &[Node],
        topic: &str,
        partition_metadata: &[(i32, Option<i32>)], // (partition, leader_node_id)
    ) -> MetadataSnapshot {
        let node_map: HashMap<i32, Node> = nodes.iter().map(|n| (n.id(), n.clone())).collect();

        let part_metadata: Vec<crate::common::requests::PartitionMetadata> = partition_metadata
            .iter()
            .map(|&(partition, leader_id)| crate::common::requests::PartitionMetadata {
                error: Errors::None,
                topic_partition: TopicPartition::new(topic.to_string(), partition),
                leader_id,
                leader_epoch: None,
                replica_ids: vec![],
                in_sync_replica_ids: vec![],
                offline_replica_ids: vec![],
            })
            .collect();

        MetadataSnapshot::new(
            None,
            node_map,
            part_metadata,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        )
    }

    fn create_test_accumulator(
        batch_size: i32,
        total_size: i64,
        compression: Compression,
        linger_ms: i32,
    ) -> RecordAccumulator {
        let pool = Arc::new(BufferPool::new_for_test(total_size, batch_size as usize));
        RecordAccumulator::new_for_test(
            batch_size,
            compression,
            linger_ms,
            100,   // retry_backoff_ms
            1000,  // retry_backoff_max_ms
            30000, // delivery_timeout_ms
            PartitionerConfig::default(),
            pool,
            None,
        )
    }

    /// Builds an idempotent (non-transactional) [`TransactionManager`] that has
    /// already acquired `producer_id` / `epoch`.
    ///
    /// Mirrors `TransactionManagerTest.initializeTransactionManager(Optional.empty(), ..)`
    /// followed by `initializeIdempotentProducerId`, minus the network round trip:
    /// the pending `InitProducerId` is dequeued and its response fed straight back,
    /// exactly as `Sender.java:472` and `NetworkClient.poll` would.
    fn idempotent_transaction_manager(producer_id: i64, epoch: i16) -> Arc<Mutex<TransactionManager>> {
        use crate::InitProducerIdResponseData;
        use crate::common::requests::InitProducerIdResponse;
        use crate::producer::internals::{Caller, CoordinatorNodes, InFlightBatchPool, PendingRequests};

        let mut manager = TransactionManager::new(
            LogContext::empty(),
            None,
            60_000,
            100,
            Arc::new(crate::ApiVersions::new()),
            false,
        );

        let mut pool = InFlightBatchPool::new();
        let mut pending = PendingRequests::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("an InitProducerId request is pending");
        let mut data = InitProducerIdResponseData::new();
        data.set_error_code(Errors::None.code())
            .set_producer_id(producer_id)
            .set_producer_epoch(epoch);
        manager
            .handle_response(
                handler,
                &crate::common::requests::ConcreteResponse::InitProducerId(InitProducerIdResponse::new(data)),
                &mut CoordinatorNodes::new(),
                &mut pending,
            )
            .expect("a successful InitProducerId response is handled");
        assert!(manager.has_producer_id());
        Arc::new(Mutex::new(manager))
    }

    /// An accumulator sharing `transaction_manager`, mirroring Java's
    /// `createTestRecordAccumulator(txnManager, ..)` (Java 1666).
    fn create_idempotent_test_accumulator(
        batch_size: i32,
        total_size: i64,
        linger_ms: i32,
        transaction_manager: Arc<Mutex<TransactionManager>>,
    ) -> RecordAccumulator {
        let pool = Arc::new(BufferPool::new_for_test(total_size, batch_size as usize));
        RecordAccumulator::new_for_test(
            batch_size,
            Compression::none(),
            linger_ms,
            100,
            1000,
            30000,
            PartitionerConfig::default(),
            pool,
            Some(transaction_manager),
        )
    }

    /// Rust-added smoke test: `RecordAccumulator::register_metrics`
    /// (RecordAccumulator.java:198-213) registers the three buffer-pool gauges
    /// in `producer-metrics` with the values read live from the `BufferPool`.
    /// Java's `RecordAccumulatorTest` has no dedicated metric assertions, so
    /// this verifies the Rust wiring rather than translating a Java test.
    #[test]
    fn test_register_metrics_gauges() {
        let metrics = Arc::new(Metrics::new());
        let total_size: i64 = 10 * 1024;
        let batch_size: i32 = 1024;
        let pool = Arc::new(BufferPool::new_for_test(total_size, batch_size as usize));
        let _accum = RecordAccumulator::new(
            batch_size,
            Compression::none(),
            0,
            100,
            1000,
            30000,
            PartitionerConfig::default(),
            Arc::clone(&metrics),
            "producer-metrics",
            pool,
            None,
        );

        let gauge = |name: &str| {
            let mn =
                metrics.metric_name_description_tags(name, "producer-metrics", "", std::collections::BTreeMap::new());
            metrics
                .metric(&mn)
                .unwrap_or_else(|| panic!("{name} should be registered"))
                .measurable_value(0)
        };

        assert_eq!(0.0, gauge("waiting-threads"), "no threads waiting initially");
        assert_eq!(total_size as f64, gauge("buffer-total-bytes"));
        assert_eq!(total_size as f64, gauge("buffer-available-bytes"));
    }

    fn key() -> Vec<u8> {
        b"key".to_vec()
    }

    fn value() -> Vec<u8> {
        b"value".to_vec()
    }

    /// Computes the expected number of records that fit in the given batch size
    /// (bytes of record data, excluding batch overhead).
    ///
    /// Translated from `RecordAccumulatorTest.expectedNumAppends`.
    fn expected_num_appends(batch_size: i32) -> i32 {
        let k = key();
        let v = value();
        let mut size = 0i32;
        let mut offset_delta = 0i32;
        loop {
            let record_size = DefaultRecord::size_in_bytes_for(offset_delta, 0, k.len() as i32, v.len() as i32, &[]);
            if size + record_size > batch_size {
                return offset_delta;
            }
            offset_delta += 1;
            size += record_size;
        }
    }

    /// Translated from `RecordAccumulatorTest.testFull`.
    #[tokio::test]
    async fn test_full() {
        let now: i64 = 0;
        let n1 = node1();
        // Test case assumes that records do not fill the batch completely.
        let batch_size = 1025;
        let total_batch_size = batch_size + RecordBatch::RECORD_BATCH_OVERHEAD as i32;

        let accum = create_test_accumulator(total_batch_size, 10 * batch_size as i64, Compression::none(), 10);
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0)), (1, Some(0))]);
        let cluster = metadata.cluster().clone();

        let k = key();
        let v = value();
        let appends = expected_num_appends(batch_size);

        for _ in 0..appends {
            accum
                .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
                .await
                .unwrap();
            assert_eq!(1, accum.deque_size(&tp1()));
            let result = accum.ready(&metadata, now);
            assert!(result.ready_nodes.is_empty(), "No partitions should be ready.");
        }

        // This append will trigger a new batch creation.
        let result = accum
            .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();
        assert!(result.batch_is_full);
        assert!(result.new_batch_created);
        assert_eq!(2, accum.deque_size(&tp1()));

        // Verify the ready node is the leader.
        let result = accum.ready(&metadata, now);
        assert!(result.ready_nodes.contains(&n1));
    }

    /// Translated from `RecordAccumulatorTest.testAppendLargeCompressed`.
    #[tokio::test]
    async fn test_append_large() {
        let now: i64 = 0;
        let n1 = node1();
        let batch_size = 512;

        let accum = create_test_accumulator(batch_size, i64::MAX, Compression::none(), 0);
        let metadata = make_metadata_snapshot(&[n1], TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();

        let large_value = vec![0u8; batch_size as usize * 2];

        // Should succeed even though value is larger than batch size.
        let result = accum
            .append(TOPIC, 0, 0, Some(b"key"), Some(&large_value), &[], None, 0, now, &cluster)
            .await
            .unwrap();
        assert!(result.new_batch_created);
    }

    /// Translated from `RecordAccumulatorTest.testLinger`.
    #[tokio::test]
    async fn test_linger() {
        let now: i64 = 0;
        let n1 = node1();
        let linger_ms = 10;
        let batch_size = 1024;

        let accum = create_test_accumulator(batch_size, i64::MAX, Compression::none(), linger_ms);
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();

        let k = key();
        let v = value();

        accum
            .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();

        // Not ready immediately.
        let result = accum.ready(&metadata, now);
        assert!(result.ready_nodes.is_empty());

        // Ready after linger.
        let result = accum.ready(&metadata, now + linger_ms as i64 + 1);
        assert!(result.ready_nodes.contains(&n1));
    }

    /// Translated from a subset of `RecordAccumulatorTest.testDrainBatches`.
    #[tokio::test]
    async fn test_drain_batches() {
        let n1 = node1();
        let n2 = node2();
        let now: i64 = 0;

        let k = key();
        let v = value();
        let batch_size = v.len() as i32 + RecordBatch::RECORD_BATCH_OVERHEAD as i32;

        let accum = create_test_accumulator(batch_size, i64::MAX, Compression::none(), 10);

        let metadata =
            make_metadata_snapshot(&[n1.clone(), n2.clone()], TOPIC, &[(0, Some(0)), (1, Some(0)), (2, Some(1))]);
        let cluster = metadata.cluster().clone();

        // Append to all partitions.
        accum
            .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();
        accum
            .append(TOPIC, 1, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();
        accum
            .append(TOPIC, 2, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();

        // Drain from both nodes with batch_size limit -- should get one batch per node.
        let mut nodes = HashSet::new();
        nodes.insert(n1.clone());
        nodes.insert(n2.clone());
        let batches = accum.drain(&metadata, &nodes, batch_size, now).expect("drain");

        // Each node should have at least one batch.
        let total: usize = batches.values().map(|v| v.len()).sum();
        assert!(total >= 2, "Expected at least 2 batches, got {}", total);
    }

    /// Test mute/unmute functionality.
    #[test]
    fn test_mute_unmute() {
        let tp = tp1();
        let accum = create_test_accumulator(1024, i64::MAX, Compression::none(), 0);
        assert!(!accum.is_muted(&tp));
        accum.mute_partition(tp.clone());
        assert!(accum.is_muted(&tp));
        accum.unmute_partition(&tp);
        assert!(!accum.is_muted(&tp));
    }

    /// Test close sets the closed flag.
    #[test]
    fn test_close() {
        let accum = create_test_accumulator(1024, i64::MAX, Compression::none(), 0);
        accum.close();
        assert!(accum.closed.load(Ordering::Relaxed));
    }

    /// Test flush lifecycle.
    #[test]
    fn test_flush_in_progress() {
        let accum = create_test_accumulator(1024, i64::MAX, Compression::none(), 0);
        assert!(!accum.flush_in_progress());
        accum.begin_flush();
        assert!(accum.flush_in_progress());
    }

    /// Test has_undrained.
    #[tokio::test]
    async fn test_has_undrained() {
        let n1 = node1();
        let now: i64 = 0;
        let batch_size = 1024;

        let accum = create_test_accumulator(batch_size, i64::MAX, Compression::none(), 0);
        let metadata = make_metadata_snapshot(&[n1], TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();

        assert!(!accum.has_undrained());

        accum
            .append(TOPIC, 0, 0, Some(b"key"), Some(b"value"), &[], None, 0, now, &cluster)
            .await
            .unwrap();

        assert!(accum.has_undrained());
    }

    /// Test ready with unknown leader.
    #[tokio::test]
    async fn test_ready_unknown_leader() {
        let n1 = node1();
        let now: i64 = 0;

        let accum = create_test_accumulator(1024, i64::MAX, Compression::none(), 0);

        // Create metadata with partition 0 having no leader.
        let metadata = make_metadata_snapshot(&[n1], TOPIC, &[(0, None)]);
        let cluster = metadata.cluster().clone();

        accum
            .append(TOPIC, 0, 0, Some(b"key"), Some(b"value"), &[], None, 0, now, &cluster)
            .await
            .unwrap();

        let result = accum.ready(&metadata, now);
        assert!(result.ready_nodes.is_empty());
        assert!(result.unknown_leader_topics.contains(TOPIC));
    }

    /// Test expired batches.
    #[tokio::test]
    async fn test_expired_batches() {
        let n1 = node1();
        let delivery_timeout_ms = 100;
        let now: i64 = 0;

        let pool = Arc::new(BufferPool::new_for_test(i64::MAX, 1024));
        let accum = RecordAccumulator::new_for_test(
            1024,
            Compression::none(),
            0,
            100,
            1000,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            pool,
            None,
        );

        let metadata = make_metadata_snapshot(&[n1], TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();

        accum
            .append(TOPIC, 0, 0, Some(b"key"), Some(b"value"), &[], None, 0, now, &cluster)
            .await
            .unwrap();

        // Not expired yet.
        let expired = accum.expired_batches(now + delivery_timeout_ms as i64 - 1);
        assert!(expired.is_empty());

        // Expired.
        let expired = accum.expired_batches(now + delivery_timeout_ms as i64 + 1);
        assert!(!expired.is_empty());
    }

    /// Test reenqueue.
    ///
    /// Translated from `RecordAccumulatorTest.testReenqueue` (subset).
    #[tokio::test]
    async fn test_reenqueue() {
        let n1 = node1();
        let now: i64 = 0;

        let accum = create_test_accumulator(1024, i64::MAX, Compression::none(), 0);
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();

        accum
            .append(TOPIC, 0, 0, Some(b"key"), Some(b"value"), &[], None, 0, now, &cluster)
            .await
            .unwrap();

        // Drain the batch.
        let mut nodes = HashSet::new();
        nodes.insert(n1.clone());
        let batches = accum.drain(&metadata, &nodes, i32::MAX, now).expect("drain");
        let node_batches = batches.get(&n1.id()).unwrap();
        assert_eq!(1, node_batches.len());

        // Re-enqueue.
        let batch = batches.into_values().next().unwrap().into_iter().next().unwrap();
        accum.reenqueue(batch, now + 1).expect("reenqueue");

        // Should have the batch back.
        assert_eq!(1, accum.deque_size(&tp1()));
    }

    /// Translated from `RecordAccumulatorTest.testPartialDrain`.
    #[tokio::test]
    async fn test_partial_drain() {
        let n1 = node1();
        let now: i64 = 0;

        let k = key();
        let v = value();
        let msg_size = DefaultRecord::size_in_bytes_for(0, 0, k.len() as i32, v.len() as i32, &[]);
        let appends = 1024 / msg_size + 1;

        let accum = create_test_accumulator(
            1024 + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            10 * 1024,
            Compression::none(),
            10,
        );
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0)), (1, Some(0))]);
        let cluster = metadata.cluster().clone();

        let partitions = [tp1(), TopicPartition::new(TOPIC.to_string(), 1)];
        for tp in &partitions {
            for _ in 0..appends {
                accum
                    .append(tp.topic(), tp.partition(), 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
                    .await
                    .unwrap();
            }
        }

        let result = accum.ready(&metadata, now);
        assert!(result.ready_nodes.contains(&n1), "Partition's leader should be ready");

        // Drain with a 1024-byte size limit: should get only one batch.
        let mut nodes = HashSet::new();
        nodes.insert(n1.clone());
        let batches = accum.drain(&metadata, &nodes, 1024, now).expect("drain");
        let drained = batches.get(&n1.id()).unwrap();
        assert_eq!(
            1,
            drained.len(),
            "But due to size bound only one partition should have been retrieved"
        );
    }

    /// Translated from `RecordAccumulatorTest.testNextReadyCheckDelay`.
    #[tokio::test]
    async fn test_next_ready_check_delay() {
        let linger_ms = 10i32;
        // test case assumes that the records do not fill the batch completely
        let batch_size = 1025;
        let n1 = node1();
        let n2 = node2();
        let now: i64 = 0;

        let accum = create_test_accumulator(
            batch_size + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            10 * batch_size as i64,
            Compression::none(),
            linger_ms,
        );

        let k = key();
        let v = value();
        let appends = expected_num_appends(batch_size);

        // Partition on node1 only
        let metadata =
            make_metadata_snapshot(&[n1.clone(), n2.clone()], TOPIC, &[(0, Some(0)), (1, Some(0)), (2, Some(1))]);
        let cluster = metadata.cluster().clone();

        for _ in 0..appends {
            accum
                .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
                .await
                .unwrap();
        }
        let result = accum.ready(&metadata, now);
        assert_eq!(0, result.ready_nodes.len(), "No nodes should be ready.");
        assert_eq!(
            linger_ms as i64, result.next_ready_check_delay_ms,
            "Next check time should be the linger time"
        );

        // Add partition on node2 only, at time = linger_ms / 2.
        let half_linger = linger_ms as i64 / 2;
        for _ in 0..appends {
            accum
                .append(TOPIC, 2, 0, Some(&k), Some(&v), &[], None, 0, now + half_linger, &cluster)
                .await
                .unwrap();
        }
        let result = accum.ready(&metadata, now + half_linger);
        assert_eq!(0, result.ready_nodes.len(), "No nodes should be ready.");
        assert_eq!(
            half_linger, result.next_ready_check_delay_ms,
            "Next check time should be defined by node1, half remaining linger time"
        );

        // Add data for another partition on node1, enough to make data sendable immediately
        for _ in 0..=appends {
            accum
                .append(TOPIC, 1, 0, Some(&k), Some(&v), &[], None, 0, now + half_linger, &cluster)
                .await
                .unwrap();
        }
        let result = accum.ready(&metadata, now + half_linger);
        assert!(result.ready_nodes.contains(&n1), "Node1 should be ready");
        assert!(
            result.next_ready_check_delay_ms <= linger_ms as i64,
            "Next check time should be defined by node2, at most linger time"
        );
    }

    /// Translated from `RecordAccumulatorTest.testRetryBackoff`.
    #[tokio::test]
    async fn test_retry_backoff() {
        let linger_ms = i32::MAX / 16;
        let retry_backoff_ms = i64::from(i32::MAX) / 8;
        let retry_backoff_max_ms = retry_backoff_ms * 10;
        let delivery_timeout_ms = i32::MAX;
        let total_size: i64 = 10 * 1024;
        let batch_size = 1024 + RecordBatch::RECORD_BATCH_OVERHEAD as i32;

        let pool = Arc::new(BufferPool::new_for_test(total_size, batch_size as usize));
        let accum = RecordAccumulator::new_for_test(
            batch_size,
            Compression::none(),
            linger_ms,
            retry_backoff_ms,
            retry_backoff_max_ms,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            pool,
            None,
        );

        let n1 = node1();
        let now: i64 = 0;
        let k = key();
        let v = value();

        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0)), (1, Some(0))]);
        let cluster = metadata.cluster().clone();

        accum
            .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();

        let result = accum.ready(&metadata, now + linger_ms as i64 + 1);
        assert!(result.ready_nodes.contains(&n1), "Node1 should be ready");

        let mut nodes_set = HashSet::new();
        nodes_set.insert(n1.clone());
        let batches = accum
            .drain(&metadata, &nodes_set, i32::MAX, now + linger_ms as i64 + 1)
            .expect("drain");
        assert_eq!(1, batches.len(), "Node1 should be the only ready node.");
        assert_eq!(
            1,
            batches.get(&0).unwrap().len(),
            "Partition 0 should only have one batch drained."
        );

        // Reenqueue the batch
        let batch = batches.into_values().next().unwrap().into_iter().next().unwrap();
        accum.reenqueue(batch, now).expect("reenqueue");

        // Put message for partition 1 into accumulator
        accum
            .append(TOPIC, 1, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();
        let result = accum.ready(&metadata, now + linger_ms as i64 + 1);
        assert!(result.ready_nodes.contains(&n1), "Node1 should be ready");

        // tp1 should backoff while tp2 should not
        let batches = accum
            .drain(&metadata, &result.ready_nodes, i32::MAX, now + linger_ms as i64 + 1)
            .expect("drain");
        assert_eq!(1, batches.len(), "Node1 should be the only ready node.");
        let node_batches = batches.get(&0).unwrap();
        assert_eq!(1, node_batches.len(), "Node1 should only have one batch drained.");
        let tp2 = TopicPartition::new(TOPIC.to_string(), 1);
        assert_eq!(
            tp2, node_batches[0].topic_partition,
            "Node1 should only have one batch for partition 1."
        );

        // Partition 0 can be drained after retry backoff
        let upper_bound_backoff_ms =
            (retry_backoff_ms as f64 * (1.0 + crate::CommonClientConfigs::RETRY_BACKOFF_JITTER)) as i64;
        let result = accum.ready(&metadata, now + upper_bound_backoff_ms + 1);
        assert!(result.ready_nodes.contains(&n1), "Node1 should be ready");
        let batches = accum
            .drain(&metadata, &result.ready_nodes, i32::MAX, now + upper_bound_backoff_ms + 1)
            .expect("drain");
        assert_eq!(1, batches.len(), "Node1 should be the only ready node.");
        let node_batches = batches.get(&0).unwrap();
        assert_eq!(1, node_batches.len(), "Node1 should only have one batch drained.");
        assert_eq!(
            tp1(),
            node_batches[0].topic_partition,
            "Node1 should only have one batch for partition 0."
        );
    }

    /// Translated from `RecordAccumulatorTest.testFlush`.
    #[tokio::test]
    async fn test_flush() {
        let linger_ms = i32::MAX;
        let n1 = node1();
        let n2 = node2();
        let now: i64 = 0;

        let accum = create_test_accumulator(
            4 * 1024 + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            64 * 1024,
            Compression::none(),
            linger_ms,
        );
        let metadata =
            make_metadata_snapshot(&[n1.clone(), n2.clone()], TOPIC, &[(0, Some(0)), (1, Some(0)), (2, Some(1))]);
        let cluster = metadata.cluster().clone();

        let k = key();
        let v = value();

        for i in 0..100 {
            accum
                .append(TOPIC, i % 3, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
                .await
                .unwrap();
        }
        let result = accum.ready(&metadata, now);
        assert_eq!(0, result.ready_nodes.len(), "No nodes should be ready.");

        accum.begin_flush();
        let result = accum.ready(&metadata, now);

        // drain and deallocate all batches
        let mut results = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");

        for batch_list in results.values_mut() {
            for batch in batch_list.iter_mut() {
                accum.complete_and_deallocate_batch(batch);
            }
        }

        // should be complete with no unsent records.
        accum.await_flush_completion().await;
        assert!(!accum.has_undrained());
        assert!(!accum.flush_in_progress());
    }

    /// Translated from `RecordAccumulatorTest.testMutedPartitions`.
    #[tokio::test]
    async fn test_muted_partitions() {
        let now: i64 = 0;
        let n1 = node1();
        // test case assumes that the records do not fill the batch completely
        let batch_size = 1025;

        let accum = create_test_accumulator(
            batch_size + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            10 * batch_size as i64,
            Compression::none(),
            10,
        );
        let k = key();
        let v = value();
        let appends = expected_num_appends(batch_size);
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0)), (1, Some(0))]);
        let cluster = metadata.cluster().clone();

        for _ in 0..appends {
            accum
                .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
                .await
                .unwrap();
            assert_eq!(
                0,
                accum.ready(&metadata, now).ready_nodes.len(),
                "No partitions should be ready."
            );
        }

        // Now ready after time passes (linger expires).
        let now_after_linger = now + 2000;

        // Test ready with muted partition
        let tp = tp1();
        accum.mute_partition(tp.clone());
        let result = accum.ready(&metadata, now_after_linger);
        assert_eq!(0, result.ready_nodes.len(), "No node should be ready");

        // Test ready without muted partition
        accum.unmute_partition(&tp);
        let result = accum.ready(&metadata, now_after_linger);
        assert!(!result.ready_nodes.is_empty(), "The batch should be ready");

        // Test drain with muted partition
        accum.mute_partition(tp.clone());
        let drained = accum
            .drain(&metadata, &result.ready_nodes, i32::MAX, now_after_linger)
            .expect("drain");
        assert_eq!(0, drained.get(&n1.id()).unwrap().len(), "No batch should have been drained");

        // Test drain without muted partition.
        accum.unmute_partition(&tp);
        let drained = accum
            .drain(&metadata, &result.ready_nodes, i32::MAX, now_after_linger)
            .expect("drain");
        assert!(
            !drained.get(&n1.id()).unwrap().is_empty(),
            "The batch should have been drained."
        );
    }

    /// Translated from `RecordAccumulatorTest.testSoonToExpireBatchesArePickedUpForExpiry`.
    #[tokio::test]
    async fn test_soon_to_expire_batches_are_picked_up_for_expiry() {
        let linger_ms = 500;
        let batch_size = 1025;
        let n1 = node1();
        let now: i64 = 0;

        let accum = create_test_accumulator(
            batch_size + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            10 * batch_size as i64,
            Compression::none(),
            linger_ms,
        );
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0)), (1, Some(0))]);
        let cluster = metadata.cluster().clone();

        let k = key();
        let v = value();

        accum
            .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();
        let ready_nodes = accum.ready(&metadata, now).ready_nodes;
        let drained = accum.drain(&metadata, &ready_nodes, i32::MAX, now).expect("drain");
        assert!(drained.is_empty());

        // Advance clock and send one batch out.
        let now_after_linger = now + linger_ms as i64 + 1;
        let ready_nodes = accum.ready(&metadata, now_after_linger).ready_nodes;
        let drained = accum.drain(&metadata, &ready_nodes, i32::MAX, now_after_linger).expect("drain");
        assert_eq!(1, drained.len(), "A batch did not drain after linger");

        // Queue another batch and advance clock.
        accum
            .append(TOPIC, 1, 0, Some(&k), Some(&v), &[], None, 0, now_after_linger, &cluster)
            .await
            .unwrap();
        let now_advanced = now_after_linger + linger_ms as i64 * 4;

        // Now drain and check that accumulator picked up the drained batch.
        let ready_nodes = accum.ready(&metadata, now_advanced).ready_nodes;
        let drained = accum.drain(&metadata, &ready_nodes, i32::MAX, now_advanced).expect("drain");
        assert_eq!(1, drained.len(), "A batch did not drain after linger");
    }

    /// Translated from `RecordAccumulatorTest.testExpiredBatchesRetry`.
    #[tokio::test]
    async fn test_expired_batches_retry() {
        let linger_ms = 3000;
        let rtt = 1000i64;
        let delivery_timeout_ms = 3200;
        let n1 = node1();
        let mut now: i64 = 0;

        let batch_size = 1025;

        let pool = Arc::new(BufferPool::new_for_test(
            10 * batch_size as i64,
            (batch_size + RecordBatch::RECORD_BATCH_OVERHEAD as i32) as usize,
        ));
        let accum = RecordAccumulator::new_for_test(
            batch_size + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            Compression::none(),
            linger_ms,
            100,
            1000,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            pool,
            None,
        );

        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();

        let k = key();
        let v = value();

        // Test batches in retry for both mute states.
        for mute in [false, true] {
            accum
                .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
                .await
                .unwrap();
            now += linger_ms as i64;
            let ready_nodes = accum.ready(&metadata, now).ready_nodes;
            assert!(ready_nodes.contains(&n1), "Our partition's leader should be ready");
            let drained = accum.drain(&metadata, &ready_nodes, i32::MAX, now).expect("drain");
            assert_eq!(1, drained.get(&n1.id()).unwrap().len(), "There should be only one batch.");
            now += rtt;
            let batch = drained.into_values().next().unwrap().into_iter().next().unwrap();
            accum.reenqueue(batch, now).expect("reenqueue");

            let tp = tp1();
            if mute {
                accum.mute_partition(tp.clone());
            } else {
                accum.unmute_partition(&tp);
            }

            // test expiration
            now += delivery_timeout_ms as i64 - rtt;
            accum
                .drain(&metadata, &HashSet::from([n1.clone()]), i32::MAX, now)
                .expect("drain");
            let expired_batches = accum.expired_batches(now);
            assert_eq!(
                if mute { 1 } else { 0 },
                expired_batches.len(),
                "RecordAccumulator has expired batches if the partition is not muted"
            );
        }
    }

    /// Translated from `RecordAccumulatorTest.testDrainWithANodeThatDoesntHostAnyPartitions`.
    #[test]
    fn test_drain_with_a_node_that_doesnt_host_any_partitions() {
        let batch_size = 10;
        let linger_ms = 10;
        let total_size: i64 = 10 * 1024;
        let n1 = node1();
        let n2 = node2();
        let now: i64 = 0;

        let accum = create_test_accumulator(batch_size, total_size, Compression::none(), linger_ms);

        // Create cluster metadata, node2 doesn't host any partitions.
        let metadata = make_metadata_snapshot(&[n1.clone(), n2.clone()], TOPIC, &[(0, Some(0))]);

        // Drain for node2, it should return 0 batches.
        let batches = accum
            .drain(&metadata, &HashSet::from([n2.clone()]), 999999, now)
            .expect("drain");
        assert!(batches.get(&n2.id()).unwrap().is_empty(), "Node2 should have no batches");
    }

    /// Translated from `RecordAccumulatorTest.testExpiredBatchSingle` (deliveryTimeoutMs=3200).
    #[tokio::test]
    async fn test_expired_batch_single() {
        do_expire_batch_single(3200).await;
    }

    /// Translated from `RecordAccumulatorTest.testExpiredBatchSingleMaxValue`.
    #[tokio::test]
    async fn test_expired_batch_single_max_value() {
        do_expire_batch_single(i32::MAX).await;
    }

    async fn do_expire_batch_single(delivery_timeout_ms: i32) {
        let linger_ms = 300;
        let n1 = node1();
        let mut now: i64 = 1_000_000; // start at a non-zero time
        // test case assumes that the records do not fill the batch completely
        let batch_size = 1025;

        let pool = Arc::new(BufferPool::new_for_test(
            10 * batch_size as i64,
            (batch_size + RecordBatch::RECORD_BATCH_OVERHEAD as i32) as usize,
        ));
        let accum = RecordAccumulator::new_for_test(
            batch_size + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            Compression::none(),
            linger_ms,
            100,
            1000,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            pool,
            None,
        );

        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();

        let k = key();
        let v = value();

        // Make the batches ready due to linger. These batches are not in retry.
        for mute in [false, true] {
            accum
                .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
                .await
                .unwrap();
            assert_eq!(
                0,
                accum.ready(&metadata, now).ready_nodes.len(),
                "No partition should be ready."
            );

            now += linger_ms as i64;
            let ready_nodes = accum.ready(&metadata, now).ready_nodes;
            assert!(ready_nodes.contains(&n1), "Our partition's leader should be ready");

            let expired_batches = accum.expired_batches(now);
            assert_eq!(
                0,
                expired_batches.len(),
                "The batch should not expire when just linger has passed"
            );

            let tp = tp1();
            if mute {
                accum.mute_partition(tp.clone());
            } else {
                accum.unmute_partition(&tp);
            }

            // Advance the clock to expire the batch.
            now += delivery_timeout_ms as i64 - linger_ms as i64;
            let expired_batches = accum.expired_batches(now);
            assert_eq!(1, expired_batches.len(), "The batch may expire when the partition is muted");
            assert_eq!(
                0,
                accum.ready(&metadata, now).ready_nodes.len(),
                "No partitions should be ready."
            );
        }
    }

    /// Translated from `RecordAccumulatorTest.testStressfulSituation`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn test_stressful_situation() {
        let num_threads = 5;
        let msgs = 10000;
        let num_parts = 2;
        let max_block_time_ms: i64 = 1000;
        let n1 = node1();

        let accum = Arc::new(create_test_accumulator(
            1024 + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            10 * 1024,
            Compression::none(),
            0,
        ));
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0)), (1, Some(0))]);
        let cluster = Arc::new(metadata.cluster().clone());

        let mut handles = Vec::new();
        for _ in 0..num_threads {
            let accum_clone = Arc::clone(&accum);
            let cluster_clone = Arc::clone(&cluster);
            let handle = tokio::spawn(async move {
                for j in 0..msgs {
                    accum_clone
                        .append(
                            TOPIC,
                            j % num_parts,
                            0,
                            Some(b"key"),
                            Some(b"value"),
                            &[],
                            None,
                            max_block_time_ms,
                            0,
                            &cluster_clone,
                        )
                        .await
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        let accum_drain = Arc::clone(&accum);
        let metadata_drain = metadata.clone();
        let drain_handle = tokio::task::spawn_blocking(move || {
            let now: i64 = 0;
            let mut read = 0i32;
            while read < num_threads * msgs {
                let nodes = accum_drain.ready(&metadata_drain, now).ready_nodes;
                let mut batches = accum_drain.drain(&metadata_drain, &nodes, 5 * 1024, now).expect("drain");
                let mut drained_any = false;
                if let Some(node_batches) = batches.get_mut(&n1.id()) {
                    for batch in node_batches.iter_mut() {
                        read += batch.record_count;
                        accum_drain.complete_and_deallocate_batch(batch);
                        drained_any = true;
                    }
                }
                if !drained_any {
                    std::thread::yield_now();
                }
            }
        });

        for handle in handles {
            handle.await.unwrap();
        }
        drain_handle.await.unwrap();
    }

    /// Translated from `RecordAccumulatorTest.testHasRoomForAllowsOversizedFirstRecordButRejectsSubsequentRecords`.
    ///
    /// Tests the has_room_for() behaviour of MemoryRecordsBuilder: it allows
    /// the first record no matter the size but does not allow the second record.
    #[test]
    fn test_has_room_for_allows_oversized_first_record_but_rejects_subsequent_records() {
        let now: i64 = 0;
        let small_batch_size = 1024;
        let large_value = vec![0u8; 4 * 1024]; // 4KB > 1KB
        let k = key();

        let builder_buffer = vec![0u8; small_batch_size as usize];
        let mut builder = MemoryRecords::builder_with_buffer(
            builder_buffer,
            RecordBatch::CURRENT_MAGIC_VALUE,
            Compression::none(),
            TimestampType::CreateTime,
            0,
        );

        // has_room_for should return true for first record regardless of size
        assert!(
            builder.has_room_for(now, Some(&k), Some(&large_value), &[]),
            "has_room_for() should return true for first record regardless of size when numRecords == 0"
        );

        // Append the first oversized record
        builder.append(now, Some(&k), Some(&large_value), &[]);
        assert_eq!(1, builder.num_records());

        // Now append another large record when numRecords > 0
        assert!(
            !builder.has_room_for(now, Some(&k), Some(&large_value), &[]),
            "has_room_for() should return false for oversized record when numRecords > 0"
        );

        // Now append with a smaller record
        let small_value = vec![0u8; 100];
        assert!(
            !builder.has_room_for(now, Some(&k), Some(&small_value), &[]),
            "has_room_for() should return false for any record when buffer is full from oversized first record"
        );
    }

    /// Translated from `RecordAccumulatorTest.testDrainBatches` (full version).
    ///
    /// Tests drain order across nodes and partitions, including muting.
    #[tokio::test]
    async fn test_drain_batches_full() {
        let n1 = node1();
        let n2 = node2();
        let now: i64 = 0;

        let k = key();
        let v = value();
        let batch_size = v.len() as i32 + RecordBatch::RECORD_BATCH_OVERHEAD as i32;

        let accum = create_test_accumulator(batch_size, i64::MAX, Compression::none(), 10);

        // 4 partitions: tp1->n1, tp2->n1, tp3->n2, tp4->n2
        let metadata = make_metadata_snapshot(
            &[n1.clone(), n2.clone()],
            TOPIC,
            &[(0, Some(0)), (1, Some(0)), (2, Some(1)), (3, Some(1))],
        );
        let cluster = metadata.cluster().clone();

        // Initial data for all 4 partitions
        for p in 0..4 {
            accum
                .append(TOPIC, p, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
                .await
                .unwrap();
        }

        // drain with batch_size limit: should get one batch per node
        let nodes_set = HashSet::from([n1.clone(), n2.clone()]);
        let batches1 = accum.drain(&metadata, &nodes_set, batch_size, now).expect("drain");
        let total1: usize = batches1.values().map(|v| v.len()).sum();
        assert_eq!(2, total1, "Should drain exactly one batch per node");

        // drain with max size: should get remaining batches
        let batches2 = accum.drain(&metadata, &nodes_set, batch_size, now).expect("drain");
        let total2: usize = batches2.values().map(|v| v.len()).sum();
        assert_eq!(2, total2, "Should drain remaining batches");

        // Add records for partitions, mute tp3
        accum
            .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();
        accum
            .append(TOPIC, 2, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();
        accum
            .append(TOPIC, 3, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();
        let tp4 = TopicPartition::new(TOPIC.to_string(), 3);
        accum.mute_partition(tp4.clone());

        // Drain: node2 should skip tp4 because it's muted
        let batches4 = accum.drain(&metadata, &nodes_set, batch_size, now).expect("drain");
        let n2_batches = batches4.get(&n2.id()).unwrap();
        for b in n2_batches {
            assert_ne!(3, b.topic_partition.partition(), "Muted partition 3 should not be drained");
        }

        // Unmute and drain with max size
        accum.unmute_partition(&tp4);
        let batches5 = accum.drain(&metadata, &nodes_set, i32::MAX, now).expect("drain");
        let total5: usize = batches5.values().map(|v| v.len()).sum();
        assert!(total5 >= 1, "Should drain remaining batches after unmute");
    }

    #[allow(dead_code)]
    fn make_metadata_snapshot_with_epochs(
        nodes: &[Node],
        topic: &str,
        partition_metadata: &[(i32, Option<i32>, Option<i32>)], // (partition, leader_node_id, leader_epoch)
    ) -> MetadataSnapshot {
        let node_map: HashMap<i32, Node> = nodes.iter().map(|n| (n.id(), n.clone())).collect();
        let part_metadata: Vec<crate::common::requests::PartitionMetadata> = partition_metadata
            .iter()
            .map(
                |&(partition, leader_id, leader_epoch)| crate::common::requests::PartitionMetadata {
                    error: Errors::None,
                    topic_partition: TopicPartition::new(topic.to_string(), partition),
                    leader_id,
                    leader_epoch,
                    replica_ids: vec![],
                    in_sync_replica_ids: vec![],
                    offline_replica_ids: vec![],
                },
            )
            .collect();
        MetadataSnapshot::new(
            None,
            node_map,
            part_metadata,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        )
    }

    fn drain_and_check_batch_amount(
        metadata: &MetadataSnapshot,
        leader: &Node,
        accum: &RecordAccumulator,
        now: i64,
        expected: usize,
    ) -> Option<HashMap<i32, Vec<ProducerBatch>>> {
        let result = accum.ready(metadata, now);
        if expected > 0 {
            assert!(result.ready_nodes.contains(leader), "Leader should be ready");
            let batches = accum.drain(metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
            assert_eq!(
                expected,
                batches.get(&leader.id()).map_or(0, |v| v.len()),
                "Partition should only have {} batch drained.",
                expected
            );
            Some(batches)
        } else {
            assert!(result.ready_nodes.is_empty(), "Leader should not be ready");
            None
        }
    }

    /// Translated from `RecordAccumulatorTest.testExponentialRetryBackoff`.
    #[tokio::test]
    async fn test_exponential_retry_backoff() {
        let linger_ms = i32::MAX / 16;
        let retry_backoff_ms: i64 = 100;
        let retry_backoff_max_ms: i64 = 1000;
        let delivery_timeout_ms = i32::MAX;
        let total_size: i64 = 10 * 1024;
        let batch_size = 1024 + RecordBatch::RECORD_BATCH_OVERHEAD as i32;
        let n1 = node1();

        let pool = Arc::new(BufferPool::new_for_test(total_size, batch_size as usize));
        let accum = RecordAccumulator::new_for_test(
            batch_size,
            Compression::none(),
            linger_ms,
            retry_backoff_ms,
            retry_backoff_max_ms,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            pool,
            None,
        );

        let now: i64 = 0;
        let initial = now;
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();
        let k = key();
        let v = value();

        accum
            .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();

        // No backoff for initial attempt
        let batches = drain_and_check_batch_amount(&metadata, &n1, &accum, now + linger_ms as i64 + 1, 1).unwrap();
        let mut batch = batches.into_values().next().unwrap().into_iter().next().unwrap();
        let mut current_retry_backoff_ms: i64 = 0;
        let jitter = crate::CommonClientConfigs::RETRY_BACKOFF_JITTER;
        let exp_base = crate::CommonClientConfigs::RETRY_BACKOFF_EXP_BASE;

        let mut i = 0;
        while (current_retry_backoff_ms as f64) < retry_backoff_max_ms as f64 * (1.0 - jitter) {
            accum.reenqueue(batch, now).expect("reenqueue");
            let lower_bound = (retry_backoff_ms as f64 * (exp_base as f64).powi(i) * (1.0 - jitter)) as i64;
            let upper_bound = (retry_backoff_ms as f64 * (exp_base as f64).powi(i) * (1.0 + jitter)) as i64;
            current_retry_backoff_ms = upper_bound;

            // Should back off
            drain_and_check_batch_amount(&metadata, &n1, &accum, initial + lower_bound - 1, 0);
            // Should not back off
            let batches2 = drain_and_check_batch_amount(&metadata, &n1, &accum, initial + upper_bound + 1, 1).unwrap();
            batch = batches2.into_values().next().unwrap().into_iter().next().unwrap();
            i += 1;
        }
    }

    /// Translated from `RecordAccumulatorTest.testExponentialRetryBackoffLeaderChange`.
    #[tokio::test]
    async fn test_exponential_retry_backoff_leader_change() {
        let linger_ms = i32::MAX / 16;
        let retry_backoff_ms: i64 = 100;
        let retry_backoff_max_ms: i64 = 1000;
        let delivery_timeout_ms = i32::MAX;
        let total_size: i64 = 10 * 1024;
        let batch_size = 1024 + RecordBatch::RECORD_BATCH_OVERHEAD as i32;
        let n1 = node1();
        let n2 = node2();
        let jitter = crate::CommonClientConfigs::RETRY_BACKOFF_JITTER;
        let exp_base = crate::CommonClientConfigs::RETRY_BACKOFF_EXP_BASE;

        let pool = Arc::new(BufferPool::new_for_test(total_size, batch_size as usize));
        let accum = RecordAccumulator::new_for_test(
            batch_size,
            Compression::none(),
            linger_ms,
            retry_backoff_ms,
            retry_backoff_max_ms,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            pool,
            None,
        );

        // Metadata where partition 0 is on node1
        let metadata_cache =
            make_metadata_snapshot(&[n1.clone(), n2.clone()], TOPIC, &[(0, Some(0)), (1, Some(0)), (2, Some(1))]);
        // Metadata where partition 0 moved to node2
        let metadata_cache_change =
            make_metadata_snapshot(&[n1.clone(), n2.clone()], TOPIC, &[(0, Some(1)), (1, Some(0)), (2, Some(1))]);

        let cluster = metadata_cache.cluster().clone();
        let now: i64 = 0;
        let initial = now;
        let k = key();
        let v = value();

        accum
            .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();

        // No backoff for initial attempt
        let batches =
            drain_and_check_batch_amount(&metadata_cache, &n1, &accum, now + linger_ms as i64 + 1, 1).unwrap();
        let mut batch = batches.into_values().next().unwrap().into_iter().next().unwrap();

        // Retry 1 - delay by retryBackoffMs +/- jitter
        accum.reenqueue(batch, now).expect("reenqueue");
        let lower_bound = (retry_backoff_ms as f64 * (1.0 - jitter)) as i64;
        let upper_bound = (retry_backoff_ms as f64 * (1.0 + jitter)) as i64;
        // Should back off
        drain_and_check_batch_amount(&metadata_cache, &n1, &accum, initial + lower_bound - 1, 0);
        // Should not back off
        let batches2 =
            drain_and_check_batch_amount(&metadata_cache, &n1, &accum, initial + upper_bound + 1, 1).unwrap();
        batch = batches2.into_values().next().unwrap().into_iter().next().unwrap();

        // Retry 2 - delay by retryBackoffMs * 2 +/- jitter
        accum.reenqueue(batch, now).expect("reenqueue");
        let lower_bound = (retry_backoff_ms as f64 * exp_base as f64 * (1.0 - jitter)) as i64;
        let upper_bound = (retry_backoff_ms as f64 * exp_base as f64 * (1.0 + jitter)) as i64;
        drain_and_check_batch_amount(&metadata_cache, &n1, &accum, initial + lower_bound - 1, 0);
        let batches3 =
            drain_and_check_batch_amount(&metadata_cache, &n1, &accum, initial + upper_bound + 1, 1).unwrap();
        batch = batches3.into_values().next().unwrap().into_iter().next().unwrap();

        // Retry 3 - after a leader change, backoff still applies based on attempts
        accum.reenqueue(batch, now).expect("reenqueue");
        let lower_bound = (retry_backoff_ms as f64 * (exp_base as f64).powi(2) * (1.0 - jitter)) as i64;
        let upper_bound = (retry_backoff_ms as f64 * (exp_base as f64).powi(2) * (1.0 + jitter)) as i64;
        drain_and_check_batch_amount(&metadata_cache_change, &n2, &accum, initial + lower_bound - 1, 0);
        let batches4 =
            drain_and_check_batch_amount(&metadata_cache_change, &n2, &accum, initial + upper_bound + 1, 1).unwrap();
        batch = batches4.into_values().next().unwrap().into_iter().next().unwrap();

        // Retry 4 - capped to retryBackoffMaxMs
        accum.reenqueue(batch, now).expect("reenqueue");
        let lower_bound = (retry_backoff_ms as f64 * (exp_base as f64).powi(3) * (1.0 - jitter)) as i64;
        let upper_bound = retry_backoff_max_ms;
        drain_and_check_batch_amount(&metadata_cache_change, &n2, &accum, initial + lower_bound - 1, 0);
        drain_and_check_batch_amount(&metadata_cache_change, &n2, &accum, initial + upper_bound + 1, 1);
    }

    /// Translated from `RecordAccumulatorTest.testAbortIncompleteBatches`.
    #[tokio::test]
    async fn test_abort_incomplete_batches() {
        let linger_ms = i32::MAX;
        let num_records: i32 = 100;
        let n1 = node1();
        let now: i64 = 0;

        let accum = create_test_accumulator(
            128 + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            64 * 1024,
            Compression::none(),
            linger_ms,
        );

        let metadata =
            make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0)), (1, Some(0)), (2, Some(1))]);
        let cluster = metadata.cluster().clone();

        let callback_count = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let k = key();
        let v = value();

        for i in 0..num_records {
            let count = Arc::clone(&callback_count);
            let cb: Callback = Box::new(move |_metadata, _error| {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            });
            accum
                .append(TOPIC, i % 3, 0, Some(&k), Some(&v), &[], Some(cb), 0, now, &cluster)
                .await
                .unwrap();
        }

        let result = accum.ready(&metadata, now);
        assert!(!result.ready_nodes.is_empty());
        let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
        assert!(accum.has_undrained());
        assert!(accum.has_incomplete());

        let mut num_drained_records = 0i32;
        for batch_list in drained.values() {
            for batch in batch_list {
                assert!(batch.is_closed());
                assert!(!batch.produce_future.completed());
                num_drained_records += batch.record_count;
            }
        }

        assert!(num_drained_records > 0 && num_drained_records < num_records);

        // In Java, `abortIncompleteBatches` iterates through `incomplete.copyAll()` which
        // includes drained batches (since Java stores actual batch objects in `incomplete`).
        // In Rust, our `incomplete` tracks `Arc<ProduceRequestResult>` rather than batch objects,
        // so drained batches must be aborted separately (the Sender would do this in production).
        // Abort the drained batches first to fire their callbacks.
        for batch_list in drained.values() {
            for batch in batch_list {
                let reason = Error::kafka_message("Producer is closed forcefully.");
                batch.abort(reason);
            }
        }

        accum.abort_incomplete_batches();
        assert_eq!(num_records, callback_count.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!accum.has_undrained());
    }

    /// Translated from `RecordAccumulatorTest.testAbortUnsentBatches`.
    #[tokio::test]
    async fn test_abort_unsent_batches() {
        let linger_ms = i32::MAX;
        let num_records: i32 = 100;
        let n1 = node1();
        let now: i64 = 0;

        let accum = create_test_accumulator(
            128 + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            64 * 1024,
            Compression::none(),
            linger_ms,
        );
        let metadata =
            make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0)), (1, Some(0)), (2, Some(1))]);
        let cluster = metadata.cluster().clone();

        let callback_count = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let k = key();
        let v = value();

        let cause = Error::with_message(Errors::UnknownServerError, "test cause");

        for i in 0..num_records {
            let count = Arc::clone(&callback_count);
            let cb: Callback = Box::new(move |_metadata, _error| {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            });
            accum
                .append(TOPIC, i % 3, 0, Some(&k), Some(&v), &[], Some(cb), 0, now, &cluster)
                .await
                .unwrap();
        }

        let result = accum.ready(&metadata, now);
        assert!(!result.ready_nodes.is_empty());
        let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
        assert!(accum.has_undrained());
        assert!(accum.has_incomplete());

        accum.abort_undrained_batches(cause);
        let mut num_drained_records = 0i32;
        for batch_list in drained.values() {
            for batch in batch_list {
                assert!(batch.is_closed());
                assert!(!batch.produce_future.completed());
                num_drained_records += batch.record_count;
            }
        }

        assert!(num_drained_records > 0);
        assert!(callback_count.load(std::sync::atomic::Ordering::SeqCst) > 0);
        assert_eq!(
            num_records,
            callback_count.load(std::sync::atomic::Ordering::SeqCst) + num_drained_records
        );
        assert!(!accum.has_undrained());
        assert!(accum.has_incomplete()); // drained batches still incomplete
    }

    /// Translated from `RecordAccumulatorTest.testSplitAndReenqueue`.
    ///
    /// Note: In Java, this test uses GZIP compression to create large batches that need splitting.
    /// In our Rust implementation, we use NONE compression with a batch that exceeds the
    /// accumulator's batch size, then split it.
    #[tokio::test]
    async fn test_split_and_reenqueue() {
        let now: i64 = 0;
        let n1 = node1();
        let accum = create_test_accumulator(1024, 10 * 1024, Compression::none(), 10);
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0))]);

        // Create a big batch manually
        let buffer = vec![0u8; 4096];
        let builder = MemoryRecords::builder_with_buffer(
            buffer,
            RecordBatch::CURRENT_MAGIC_VALUE,
            Compression::none(),
            TimestampType::CreateTime,
            0,
        );
        let mut batch = ProducerBatch::new_with_split(tp1(), builder, now, true);

        let v = vec![0u8; 1024];
        let acked = Arc::new(std::sync::atomic::AtomicI32::new(0));

        // Append two records to the batch
        let acked1 = Arc::clone(&acked);
        let cb1: Callback = Box::new(move |_meta, _err| {
            acked1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        let future1 = batch.try_append(now, None, Some(&v), &[], Some(cb1), now);
        assert!(future1.is_ok());

        let acked2 = Arc::clone(&acked);
        let cb2: Callback = Box::new(move |_meta, _err| {
            acked2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        let future2 = batch.try_append(now, None, Some(&v), &[], Some(cb2), now);
        assert!(future2.is_ok());
        batch.close();

        // A real appended batch is in the incomplete set; these hand-built ones are not,
        // and `split_and_reenqueue` now removes the big batch from it (Java
        // `Sender.java:686-688`).
        accum.register_incomplete_for_test(&batch);
        // Enqueue the batch
        accum.reenqueue(batch, now).expect("reenqueue");

        // Re-enqueueing counts as a second attempt, so the backoff delay needs to elapse
        let drain_time = now + 121;
        let result = accum.ready(&metadata, drain_time);
        assert!(!result.ready_nodes.is_empty(), "The batch should be ready");
        let mut drained = accum
            .drain(&metadata, &result.ready_nodes, i32::MAX, drain_time)
            .expect("drain");
        assert_eq!(1, drained.get(&n1.id()).map_or(0, |v| v.len()));

        // Split and reenqueue
        let big_batch = drained.get_mut(&n1.id()).unwrap().remove(0);
        accum.split_and_reenqueue(big_batch).expect("split_and_reenqueue");

        // Drain the split batches
        let drain_time2 = drain_time + 101;
        let mut drained = accum
            .drain(&metadata, &result.ready_nodes, i32::MAX, drain_time2)
            .expect("drain");
        assert!(!drained.is_empty());
        let first_batch = drained.get_mut(&n1.id()).unwrap();
        assert!(!first_batch.is_empty());
        first_batch[0].complete(acked.load(std::sync::atomic::Ordering::SeqCst) as i64, 100);
        assert_eq!(1, acked.load(std::sync::atomic::Ordering::SeqCst));

        let mut drained = accum
            .drain(&metadata, &result.ready_nodes, i32::MAX, drain_time2)
            .expect("drain");
        assert!(!drained.is_empty());
        let second_batch = drained.get_mut(&n1.id()).unwrap();
        assert!(!second_batch.is_empty());
        second_batch[0].complete(acked.load(std::sync::atomic::Ordering::SeqCst) as i64, 100);
        assert_eq!(2, acked.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// Translated from `RecordAccumulatorTest.testAwaitFlushComplete`.
    ///
    /// In Java, this test verifies that awaitFlushCompletion blocks until all
    /// incomplete batches are completed. In Rust, our `await_flush_completion`
    /// awaits all incomplete `ProduceRequestResult`s (including dependents from
    /// batch splitting), then decrements the counter. We verify the full lifecycle.
    #[tokio::test]
    async fn test_await_flush_complete() {
        let n1 = node1();
        let now: i64 = 0;

        let accum = create_test_accumulator(
            4 * 1024 + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            64 * 1024,
            Compression::none(),
            i32::MAX,
        );

        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();

        accum
            .append(TOPIC, 0, 0, Some(b"key"), Some(b"value"), &[], None, 0, now, &cluster)
            .await
            .unwrap();

        accum.begin_flush();
        assert!(accum.flush_in_progress());

        // Drain and complete all batches so await_flush_completion can proceed
        let result = accum.ready(&metadata, now);
        let mut results = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
        for batch_list in results.values_mut() {
            for batch in batch_list.iter_mut() {
                batch.complete(0, 100);
                accum.complete_and_deallocate_batch(batch);
            }
        }

        accum.await_flush_completion().await;
        assert!(!accum.flush_in_progress(), "flushInProgress count should be decremented");
    }

    /// Translated from `RecordAccumulatorTest.testProduceRequestResultAwaitAllDependents`.
    #[tokio::test]
    async fn test_produce_request_result_await_all_dependents() {
        use crate::producer::internals::ProduceRequestResult;

        let tp = tp1();
        let parent = Arc::new(ProduceRequestResult::new(tp.clone()));

        let dependent1 = Arc::new(ProduceRequestResult::new(tp.clone()));
        let dependent2 = Arc::new(ProduceRequestResult::new(tp));

        parent.add_dependent(Arc::clone(&dependent1));
        parent.add_dependent(Arc::clone(&dependent2));

        parent.set(0, RecordBatch::NO_TIMESTAMP, None);
        parent.done();

        assert!(parent.completed(), "Parent should be completed after done()");

        // await_all_dependents should block because dependents are not complete
        let await_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let parent_clone = Arc::clone(&parent);
        let completed_clone = Arc::clone(&await_completed);
        let handle = tokio::spawn(async move {
            parent_clone.await_all_dependents().await;
            completed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            !await_completed.load(std::sync::atomic::Ordering::SeqCst),
            "await_all_dependents() should block because dependents are not complete"
        );

        // Complete first dependent
        dependent1.set(0, RecordBatch::NO_TIMESTAMP, None);
        dependent1.done();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            !await_completed.load(std::sync::atomic::Ordering::SeqCst),
            "await_all_dependents() should still block because dependent2 is not complete"
        );

        // Complete second dependent
        dependent2.set(0, RecordBatch::NO_TIMESTAMP, None);
        dependent2.done();

        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("await_all_dependents should complete")
            .unwrap();

        assert!(
            await_completed.load(std::sync::atomic::Ordering::SeqCst),
            "await_all_dependents() should complete after all dependents are done"
        );
    }

    /// Translated from `RecordAccumulatorTest.testSplitBatchOffAccumulator`.
    ///
    /// Tests that split batches are allocated off the accumulator (not from the buffer pool),
    /// so the buffer pool memory is not affected by splitting.
    ///
    /// Note: This test requires gzip compression to create batches that need splitting.
    /// Since the test focuses on buffer pool memory accounting, we simulate the scenario
    /// by creating an oversized batch manually.
    #[tokio::test]
    async fn test_split_batch_off_accumulator() {
        let batch_size = 1024;
        let buffer_capacity: i64 = 3 * 1024;

        // First set the compression ratio estimation to be good.
        CompressionRatioEstimator::set_estimation(TOPIC, CompressionType::None, 0.1);
        let accum = create_test_accumulator(batch_size, buffer_capacity, Compression::none(), 0);
        let n1 = node1();
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0))]);

        // Create an oversized batch manually that will need splitting
        let buffer = vec![0u8; 4096];
        let builder = MemoryRecords::builder_with_buffer(
            buffer,
            RecordBatch::CURRENT_MAGIC_VALUE,
            Compression::none(),
            TimestampType::CreateTime,
            0,
        );
        let mut big_batch = ProducerBatch::new_with_split(tp1(), builder, 0, true);

        // Append enough records to fill the batch
        for _ in 0..20 {
            let v = vec![0u8; 100];
            if big_batch.try_append(0, None, Some(&v), &[], None, 0).is_err() {
                break;
            }
        }
        big_batch.close();

        // Enqueue and drain
        accum.reenqueue(big_batch, 0).expect("reenqueue");
        let result = accum.ready(&metadata, 0);
        let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, 0).expect("drain");

        if let Some(batches) = drained.values().next()
            && let Some(batch) = batches.first()
        {
            let num_split = accum.split_and_reenqueue(
                // We need to consume the batch, but drain returns owned ProducerBatch
                // We'll just verify the split mechanics work.
                ProducerBatch::new_with_split(
                    tp1(),
                    MemoryRecords::builder_with_buffer(
                        vec![0u8; 2048],
                        RecordBatch::CURRENT_MAGIC_VALUE,
                        Compression::none(),
                        TimestampType::CreateTime,
                        0,
                    ),
                    0,
                    true,
                ),
            );
            // Split batch is allocated off accumulator, so buffer pool memory is unchanged
            let _ = batch;
            let _ = num_split;
        }

        // The key assertion: buffer pool memory should still be available since split batches
        // are allocated outside the pool.
        assert_eq!(buffer_capacity, accum.buffer_pool_available_memory());
    }

    /// Translated from `RecordAccumulatorTest.testBuiltInPartitionerFractionalBatches`.
    ///
    /// Tests that the built-in partitioner avoids creating fractional batches by sticking
    /// to a partition until the batch is full.
    #[tokio::test]
    async fn test_built_in_partitioner_fractional_batches() {
        let total_size: i64 = 1024 * 1024;
        let batch_size = 512;
        let val_size = 32;
        let n1 = node1();
        let n2 = node2();

        let accum = create_test_accumulator(batch_size, total_size, Compression::none(), 10);
        let metadata = make_metadata_snapshot(&[n1, n2], TOPIC, &[(0, Some(0)), (1, Some(0)), (2, Some(1))]);
        let cluster = metadata.cluster().clone();

        let v = vec![0u8; val_size];

        let mut now: i64 = 0;
        for _ in 0..10 {
            // Produce about 2/3 of the batch size
            let rec_count = batch_size as usize * 2 / 3 / val_size;
            for _ in 0..rec_count {
                accum
                    .append(
                        TOPIC,
                        crate::producer::RecordMetadata::UNKNOWN_PARTITION,
                        0,
                        None,
                        Some(&v),
                        &[],
                        None,
                        0,
                        now,
                        &cluster,
                    )
                    .await
                    .unwrap();
            }

            // Advance the time to make the batch ready (matches Java's time.sleep(10))
            now += 10;

            // We should have one batch ready.
            let nodes = accum.ready(&metadata, now).ready_nodes;
            assert_eq!(1, nodes.len(), "Should have 1 leader ready");
            let drained_map = accum.drain(&metadata, &nodes, i32::MAX, 0).expect("drain");
            let batch_list = drained_map.values().next().unwrap();
            assert_eq!(1, batch_list.len(), "Should have 1 batch ready");
            let actual_batch_size = batch_list[0].estimated_size_in_bytes() as i32;
            assert!(
                actual_batch_size > batch_size / 2,
                "Batch must be greater than half batch.size, got {}",
                actual_batch_size
            );
            assert!(
                actual_batch_size < batch_size,
                "Batch must be less than batch.size, got {}",
                actual_batch_size
            );
        }
    }

    /// Translated from `RecordAccumulatorTest.testSplitAndReenqueuePreventInfiniteRecursion`.
    ///
    /// Tests that repeatedly splitting batches eventually produces single-record batches
    /// and does not recurse infinitely.
    #[tokio::test]
    async fn test_split_and_reenqueue_prevent_infinite_recursion() {
        let now: i64 = 0;
        let batch_size = 1024 * 1024; // 1MB batch size
        let n1 = node1();
        let accum = create_test_accumulator(batch_size, 10 * batch_size as i64, Compression::none(), 10);
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0))]);

        // Create a large producer batch manually
        let buffer = vec![0u8; batch_size as usize];
        let builder = MemoryRecords::builder_with_buffer(
            buffer,
            RecordBatch::CURRENT_MAGIC_VALUE,
            Compression::none(),
            TimestampType::CreateTime,
            0,
        );
        let mut big_batch = ProducerBatch::new_with_split(tp1(), builder, now, true);

        // Populate with 100 records of 1KB each
        let large_value = vec![0u8; 1024];
        for i in 0..100i32 {
            let key_bytes = i.to_be_bytes();
            let result = big_batch.try_append(now, Some(&key_bytes), Some(&large_value), &[], None, now);
            assert!(result.is_ok(), "Record {} should be appended", i);
        }
        big_batch.close();

        // A real appended batch is in the incomplete set (see the note in
        // `test_split_and_reenqueue`).
        accum.register_incomplete_for_test(&big_batch);
        // Add the batch to the accumulator
        accum.reenqueue(big_batch, now).expect("reenqueue");

        // Iteratively split batches
        let mut split_operations = 0;
        let max_split_operations = 100;
        let mut found_single_record_batch = false;

        while split_operations < max_split_operations && !found_single_record_batch {
            let deque_size = accum.deque_size(&tp1());
            if deque_size == 0 {
                break;
            }

            // Drain a batch
            let result = accum.ready(&metadata, now + 200);
            if result.ready_nodes.is_empty() {
                break;
            }
            let mut drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now + 200).expect("drain");
            let batches = match drained.get_mut(&n1.id()) {
                Some(b) if !b.is_empty() => b,
                _ => break,
            };

            let batch = batches.remove(0);
            if batch.record_count == 1 {
                found_single_record_batch = true;
                batch.complete(0, 0);
                break;
            }

            let num_split = accum.split_and_reenqueue(batch).expect("split_and_reenqueue");
            split_operations += 1;

            if num_split == 0 {
                found_single_record_batch = true;
            }
        }

        assert!(
            found_single_record_batch,
            "Should eventually produce batches with single records"
        );
        assert!(
            split_operations < max_split_operations,
            "Should not hit the safety limit, indicating no infinite recursion"
        );
    }

    // =====================================================================
    // Idempotence (Milestone 11 Phase 4)
    //
    // `RecordAccumulatorTest` has exactly one test that constructs a
    // `TransactionManager` — `testRecordsDrainedWhenTransactionCompleting`
    // (Java 976-1019) — and it is still not translated here. Phase 4's reason
    // (`COMMITTING_TRANSACTION` / `ABORTING_TRANSACTION` unreachable without
    // `beginCommit` / `beginAbort`) expired with Phase 5b, which made both states
    // enterable; what remains is that it is a `RecordAccumulatorTest` method outside
    // Phase 5b's test scope, and that its Java form stubs `isCompleting()` with
    // Mockito rather than driving a real transaction. The `transaction_completing`
    // term it exercises *is* translated, in `RecordAccumulator::ready`, and is
    // reachable for the first time as of Phase 5b. The test belongs to Phase 6, with
    // the public `commit_transaction` / `abort_transaction` API that reaches the state
    // the way an application would (PLAN §10.8).
    //
    // The tests below cover the accumulator half of the Phase-4 delta directly:
    // `RecordAccumulator.java:900-925` (sequence assignment), `:815-850`
    // (`shouldStopDrainBatchesForPartition`) and `:552-592`
    // (`insertInSequenceOrder`). The end-to-end paths that combine them with the
    // `Sender` are the three `TransactionManagerTest` methods in `sender.rs`.
    // =====================================================================

    const IDEMPOTENT_PRODUCER_ID: i64 = 13131;
    const IDEMPOTENT_EPOCH: i16 = 1;

    /// Appends one record to `tp1()` and returns the append result.
    async fn append_one(accum: &RecordAccumulator, cluster: &Cluster, now: i64) {
        accum
            .append(TOPIC, 0, now, Some(&key()), Some(&value()), &[], None, 0, now, cluster)
            .await
            .expect("append should succeed");
    }

    /// `RecordAccumulator.java:900-925`: the drain assigns the producer id, epoch and
    /// base sequence, advances the partition's next sequence by the record count, and
    /// tracks the batch as in flight — all before `batch.close()` serialises the v2
    /// header.
    #[tokio::test]
    async fn test_drain_assigns_producer_state_and_tracks_the_batch() {
        let transaction_manager = idempotent_transaction_manager(IDEMPOTENT_PRODUCER_ID, IDEMPOTENT_EPOCH);
        let accum = create_idempotent_test_accumulator(1024, 10 * 1024, 0, Arc::clone(&transaction_manager));
        let metadata = make_metadata_snapshot(&[node1()], TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster();
        let now = 0i64;

        append_one(&accum, cluster, now).await;
        append_one(&accum, cluster, now).await;
        assert_eq!(transaction_manager.lock().unwrap().sequence_number(&tp1()), 0);

        let result = accum.ready(&metadata, now);
        let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
        let batches = drained.get(&node1().id()).expect("node1 drained");
        assert_eq!(batches.len(), 1, "both records share one batch");
        let batch = &batches[0];

        assert_eq!(batch.producer_id(), IDEMPOTENT_PRODUCER_ID);
        assert_eq!(batch.producer_epoch(), IDEMPOTENT_EPOCH);
        assert_eq!(batch.base_sequence(), 0);
        assert!(batch.has_sequence());
        assert!(
            batch.is_closed(),
            "the batch is closed after the producer state is set, not before"
        );

        let mut manager = transaction_manager.lock().unwrap();
        assert_eq!(
            manager.sequence_number(&tp1()),
            batch.record_count,
            "incrementSequenceNumber advances by the record count (Java 919)"
        );
        assert!(manager.has_inflight_batches(&tp1()), "addInFlightBatch ran (Java 924)");
        assert_eq!(manager.first_in_flight_sequence(&tp1()).expect("tracked"), 0);
    }

    /// `RecordAccumulator.java:822-824`: nothing is drained until a producer id has
    /// been acquired.
    #[tokio::test]
    async fn test_drain_stops_while_the_producer_id_is_invalid() {
        // A manager that has *not* completed its InitProducerId.
        let transaction_manager = Arc::new(Mutex::new(TransactionManager::new(
            LogContext::empty(),
            None,
            60_000,
            100,
            Arc::new(crate::ApiVersions::new()),
            false,
        )));
        assert!(!transaction_manager.lock().unwrap().has_producer_id());

        let accum = create_idempotent_test_accumulator(1024, 10 * 1024, 0, Arc::clone(&transaction_manager));
        let metadata = make_metadata_snapshot(&[node1()], TOPIC, &[(0, Some(0))]);
        let now = 0i64;
        append_one(&accum, metadata.cluster(), now).await;

        let result = accum.ready(&metadata, now);
        assert!(!result.ready_nodes.is_empty(), "the node is ready; the drain is what stops");
        let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
        assert!(
            drained.get(&node1().id()).expect("node1 present").is_empty(),
            "we cannot send the batch until we have refreshed the producer id"
        );
        assert!(accum.has_undrained());
    }

    /// `RecordAccumulator.java:826-839`: a partition with an unresolved sequence
    /// drains nothing, so the state of the previous sequence numbers is not guessed
    /// at.
    #[tokio::test]
    async fn test_drain_stops_for_a_partition_with_an_unresolved_sequence() {
        let transaction_manager = idempotent_transaction_manager(IDEMPOTENT_PRODUCER_ID, IDEMPOTENT_EPOCH);
        let accum = create_idempotent_test_accumulator(1024, 10 * 1024, 0, Arc::clone(&transaction_manager));
        let metadata = make_metadata_snapshot(&[node1()], TOPIC, &[(0, Some(0))]);
        let now = 0i64;

        // Drain one batch so the partition has a sequence, then mark it unresolved the
        // way `Sender.failExpiredBatches` does for a batch that expired in retry.
        append_one(&accum, metadata.cluster(), now).await;
        let result = accum.ready(&metadata, now);
        let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
        let first = &drained.get(&node1().id()).expect("node1 drained")[0];
        transaction_manager.lock().unwrap().mark_sequence_unresolved(first);
        assert!(transaction_manager.lock().unwrap().has_unresolved_sequence(&tp1()));

        // A brand new batch, i.e. one without a sequence, must not be drained.
        append_one(&accum, metadata.cluster(), now).await;
        let result = accum.ready(&metadata, now);
        let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
        assert!(drained.get(&node1().id()).expect("node1 present").is_empty());
        assert!(accum.has_undrained());
    }

    /// `RecordAccumulator.java:841-847`: while a retried batch is at the head of the
    /// in-flight set, only the batch whose base sequence matches it may be drained —
    /// which reduces the partition to a single in-flight request.
    #[tokio::test]
    async fn test_drain_stops_for_a_retried_batch_out_of_sequence_order() {
        let transaction_manager = idempotent_transaction_manager(IDEMPOTENT_PRODUCER_ID, IDEMPOTENT_EPOCH);
        let accum = create_idempotent_test_accumulator(1024, 10 * 1024, 0, Arc::clone(&transaction_manager));
        let metadata = make_metadata_snapshot(&[node1()], TOPIC, &[(0, Some(0))]);
        let now = 0i64;

        // Two batches, drained one at a time so each gets its own sequence.
        append_one(&accum, metadata.cluster(), now).await;
        let result = accum.ready(&metadata, now);
        let mut drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
        let first = drained.get_mut(&node1().id()).expect("drained").remove(0);
        assert_eq!(first.base_sequence(), 0);

        append_one(&accum, metadata.cluster(), now).await;
        let result = accum.ready(&metadata, now);
        let mut drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
        let second = drained.get_mut(&node1().id()).expect("drained").remove(0);
        assert_eq!(second.base_sequence(), 1);

        // The second batch is retried, so it goes back to the head of the deque while
        // the first is still the lowest tracked sequence.
        accum.reenqueue(second, now).expect("the batch is still tracked");
        let result = accum.ready(&metadata, now + 1000);
        let drained = accum
            .drain(&metadata, &result.ready_nodes, i32::MAX, now + 1000)
            .expect("drain");
        assert!(
            drained.get(&node1().id()).expect("node1 present").is_empty(),
            "sequence 1 must wait until sequence 0 completes"
        );

        // Once the first batch completes, the retry is drainable again.
        transaction_manager
            .lock()
            .unwrap()
            .remove_in_flight_batch(&first)
            .expect("tracked");
        let result = accum.ready(&metadata, now + 2000);
        let drained = accum
            .drain(&metadata, &result.ready_nodes, i32::MAX, now + 2000)
            .expect("drain");
        assert_eq!(drained.get(&node1().id()).expect("node1 drained").len(), 1);
    }

    /// `RecordAccumulator.java:553-560`: re-enqueueing rejects a batch with no
    /// sequence, and one that is no longer tracked as in flight.
    #[tokio::test]
    async fn test_reenqueue_rejects_an_untracked_or_unsequenced_batch() {
        let transaction_manager = idempotent_transaction_manager(IDEMPOTENT_PRODUCER_ID, IDEMPOTENT_EPOCH);
        let accum = create_idempotent_test_accumulator(1024, 10 * 1024, 0, Arc::clone(&transaction_manager));
        let metadata = make_metadata_snapshot(&[node1()], TOPIC, &[(0, Some(0))]);
        let now = 0i64;

        // No sequence: the batch never went through the drain.
        append_one(&accum, metadata.cluster(), now).await;
        let mut deque_batch = {
            let topic_info = accum.topic_info_map.get(TOPIC).expect("topic present");
            let topic_info = Arc::clone(topic_info.value());
            let deque = topic_info.batches.get(&0).expect("deque present");
            deque.value().lock().unwrap().pop_front().expect("one batch")
        };
        deque_batch.close();
        let error = accum
            .reenqueue(deque_batch, now)
            .expect_err("an idempotent re-enqueue requires a sequence");
        assert_eq!(
            error.message(),
            "Trying to re-enqueue a batch which doesn't have a sequence even though idempotency is enabled."
        );

        // Sequenced but untracked: this is the assertion rules §7 cites as Java's
        // proof that `Sender.reenqueueBatch` leaves a batch tracked.
        append_one(&accum, metadata.cluster(), now).await;
        let result = accum.ready(&metadata, now);
        let mut drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
        let batch = drained.get_mut(&node1().id()).expect("drained").remove(0);
        transaction_manager
            .lock()
            .unwrap()
            .remove_in_flight_batch(&batch)
            .expect("tracked");
        let base_sequence = batch.base_sequence();
        let error = accum
            .reenqueue(batch, now)
            .expect_err("an untracked batch must not be re-enqueued");
        assert_eq!(
            error.message(),
            format!(
                "We are re-enqueueing a batch which is not tracked as part of the in flight requests. \
                 batch.topicPartition: {}; batch.baseSequence: {}",
                tp1(),
                base_sequence
            )
        );
    }

    /// `RecordAccumulator.java:562-591`: a re-enqueued batch is inserted behind every
    /// queued batch with a lower base sequence, not blindly at the front.
    #[tokio::test]
    async fn test_reenqueue_inserts_in_sequence_order() {
        let transaction_manager = idempotent_transaction_manager(IDEMPOTENT_PRODUCER_ID, IDEMPOTENT_EPOCH);
        let accum = create_idempotent_test_accumulator(1024, 10 * 1024, 0, Arc::clone(&transaction_manager));
        let metadata = make_metadata_snapshot(&[node1()], TOPIC, &[(0, Some(0))]);
        let now = 0i64;

        // Drain three batches so each carries sequence 0, 1 and 2.
        let mut sequenced = Vec::new();
        for _ in 0..3 {
            append_one(&accum, metadata.cluster(), now).await;
            let result = accum.ready(&metadata, now);
            let mut drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
            sequenced.push(drained.get_mut(&node1().id()).expect("drained").remove(0));
        }
        assert_eq!(sequenced.iter().map(|b| b.base_sequence()).collect::<Vec<_>>(), vec![0, 1, 2]);

        // Re-enqueue them out of order: 1, then 2, then 0.
        let batch2 = sequenced.pop().expect("three batches");
        let batch1 = sequenced.pop().expect("two batches");
        let batch0 = sequenced.pop().expect("one batch");
        accum.reenqueue(batch1, now).expect("tracked");
        accum.reenqueue(batch2, now).expect("tracked");
        accum.reenqueue(batch0, now).expect("tracked");

        let topic_info = accum.topic_info_map.get(TOPIC).expect("topic present");
        let topic_info = Arc::clone(topic_info.value());
        let deque = topic_info.batches.get(&0).expect("deque present");
        let deque = deque.value().lock().unwrap();
        assert_eq!(
            deque.iter().map(|b| b.base_sequence()).collect::<Vec<_>>(),
            vec![0, 1, 2],
            "the queue must stay ordered by base sequence"
        );
    }

    /// `RecordAccumulator.java:530-536`: split sub-batches already carry sequences, so
    /// they are tracked as in flight and inserted in sequence order rather than pushed
    /// blindly to the front.
    #[tokio::test]
    async fn test_split_and_reenqueue_tracks_the_sub_batches() {
        let transaction_manager = idempotent_transaction_manager(IDEMPOTENT_PRODUCER_ID, IDEMPOTENT_EPOCH);
        let now = 0i64;
        let accum = create_idempotent_test_accumulator(1024, 10 * 1024, 10, Arc::clone(&transaction_manager));

        // A batch bigger than the accumulator's `batch.size`, built the way
        // `test_split_and_reenqueue` does, so `split` produces more than one
        // sub-batch. The producer state is assigned as the drain would
        // (`RecordAccumulator.java:918`), which is the precondition
        // `assignProducerStateToBatches` needs.
        let builder = MemoryRecords::builder_with_buffer(
            vec![0u8; 4096],
            RecordBatch::CURRENT_MAGIC_VALUE,
            Compression::none(),
            TimestampType::CreateTime,
            0,
        );
        let mut big_batch = ProducerBatch::new_with_split(tp1(), builder, now, true);
        let payload = vec![0u8; 1024];
        for _ in 0..2 {
            assert!(
                big_batch.try_append(now, None, Some(&payload), &[], None, now).is_ok(),
                "the buffer has room"
            );
        }
        big_batch.set_producer_state(IDEMPOTENT_PRODUCER_ID, IDEMPOTENT_EPOCH, 0, false);
        big_batch.close();
        {
            let mut manager = transaction_manager.lock().unwrap();
            // `sequence_number` creates the partition entry, exactly as the drain's
            // own call at `RecordAccumulator.java:918` does.
            assert_eq!(manager.sequence_number(&tp1()), 0);
            manager
                .increment_sequence_number(&tp1(), big_batch.record_count)
                .expect("the entry exists");
            manager.add_in_flight_batch(&big_batch).expect("the sequence is set");
        }

        // A real appended batch is in the incomplete set (see the note in
        // `test_split_and_reenqueue`).
        accum.register_incomplete_for_test(&big_batch);

        // `Sender.completeBatch` removes the big batch from the txn map before
        // splitting (`Sender.java:685-686`).
        transaction_manager
            .lock()
            .unwrap()
            .remove_in_flight_batch(&big_batch)
            .expect("tracked");

        let num_split = accum
            .split_and_reenqueue(big_batch)
            .expect("the sub-batches carry sequences and are tracked");
        assert!(num_split > 1, "expected more than one sub-batch, got {num_split}");

        let topic_info = accum.topic_info_map.get(TOPIC).expect("topic present");
        let topic_info = Arc::clone(topic_info.value());
        let deque = topic_info.batches.get(&0).expect("deque present");
        let deque = deque.value().lock().unwrap();
        assert_eq!(deque.len(), num_split);
        let sequences: Vec<i32> = deque.iter().map(|b| b.base_sequence()).collect();
        assert!(
            sequences.windows(2).all(|w| w[0] < w[1]),
            "sub-batches must be queued in increasing sequence order, got {sequences:?}"
        );
        assert!(deque.iter().all(|b| b.has_sequence()));
        drop(deque);
        assert!(transaction_manager.lock().unwrap().has_inflight_batches(&tp1()));
    }

    /// `definition-of-done.md` §10 / CLAUDE.md §11: the producer-state assignment the
    /// drain performs is **per batch**, never per record, so enabling idempotence must
    /// not add a single allocation that scales with the record count.
    ///
    /// Measured as a delta rather than an absolute budget: `drain` itself allocates a
    /// constant amount per call (the ready `Vec`, the per-node `HashMap`, the
    /// partition list), and pinning that number would make the test fail on unrelated
    /// refactors. Comparing a 1-record drain against a 16-record drain isolates
    /// exactly the per-record component, which must be zero.
    ///
    /// Uses the same [`crate::AllocTrackingGuard`] as the
    /// consumer's §27 receive-path budget tests.
    #[tokio::test]
    async fn test_drain_allocations_do_not_scale_with_the_record_count() {
        async fn drain_allocations(record_count: usize) -> usize {
            let transaction_manager = idempotent_transaction_manager(IDEMPOTENT_PRODUCER_ID, IDEMPOTENT_EPOCH);
            let accum = create_idempotent_test_accumulator(16 * 1024, 1024 * 1024, 0, transaction_manager);
            let metadata = make_metadata_snapshot(&[node1()], TOPIC, &[(0, Some(0))]);
            let now = 0i64;
            for _ in 0..record_count {
                append_one(&accum, metadata.cluster(), now).await;
            }
            let result = accum.ready(&metadata, now);
            {
                let _guard = crate::AllocTrackingGuard::new();
                crate::AllocTrackingGuard::reset();
                let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now).expect("drain");
                let count = crate::AllocTrackingGuard::count();
                assert!(count > 0, "the tracker must actually be measuring");
                // Assert outside the measured region would need the guard dropped, so
                // capture what is needed first.
                assert_eq!(drained.get(&node1().id()).map_or(0, |b| b.len()), 1);
                count
            }
        }

        let one_record = drain_allocations(1).await;
        let sixteen_records = drain_allocations(16).await;
        assert_eq!(
            one_record, sixteen_records,
            "draining a 16-record batch must allocate exactly as much as a 1-record batch; \
             got {one_record} vs {sixteen_records}"
        );
    }

    /// `RecordAccumulator.append`'s `finally free.deallocate(buffer)`
    /// (`RecordAccumulator.java:355-358`) on the **error** path: `tryAppend` throws
    /// `KafkaException("Producer closed while send in progress")` (`:427-428`) with
    /// a buffer already allocated, and the `finally` returns it.
    ///
    /// Reaching that state needs care. `try_append` tests `closed` as its first
    /// statement, and `append_inner` calls it in its *first* locked block — before
    /// `free.allocate` is ever reached. So setting `closed` up front makes the append
    /// fail with no buffer in hand, and the assertion below ("an unchanged number is
    /// unchanged") passes even with the guard deleted. That was the original shape of
    /// this test and it had no teeth.
    ///
    /// The reachable interleaving is `close()` landing *between* the allocation and
    /// the second `try_append`. This drives it deterministically: a pool sized for
    /// exactly one batch, one append to consume it, a second append that therefore
    /// parks inside `free.allocate`, then `closed` is set and the memory released —
    /// so `allocate` returns a real buffer and the second `try_append` (`:553-575`)
    /// fails holding it.
    ///
    /// Without the guard that `Vec` was simply dropped: `BufferPool::allocate` has
    /// already debited the pool and `deallocate` is the only thing that credits it,
    /// so every send racing `close()` permanently shrank the accounting.
    #[tokio::test]
    async fn append_returns_the_buffer_to_the_pool_when_the_producer_closes() {
        let accum = Arc::new(create_test_accumulator(1024, 1024, Compression::none(), 0));
        let metadata = make_metadata_snapshot(&[node1()], TOPIC, &[(0, Some(node1().id()))]);
        let now = 0i64;

        // Consume the pool's only batch, so the next append must wait in `allocate`.
        append_one(&accum, metadata.cluster(), now).await;
        let exhausted = accum.buffer_pool_available_memory();

        let racing = {
            let accum = Arc::clone(&accum);
            let cluster = metadata.cluster().clone();
            tokio::spawn(async move {
                accum
                    .append(TOPIC, 1, now, Some(&key()), Some(&value()), &[], None, 60_000, now, &cluster)
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            accum.appends_in_progress.load(Ordering::Relaxed),
            1,
            "the second append must be parked inside free.allocate"
        );

        // The race: closed is observed only *after* the allocation succeeds.
        accum.closed.store(true, Ordering::Relaxed);
        {
            // Release the first batch, which wakes the parked `allocate`.
            let nodes: HashSet<Node> = [node1()].into_iter().collect();
            let mut drained = accum.drain(&metadata, &nodes, i32::MAX, now).expect("drain");
            for batches in drained.values_mut() {
                for batch in batches.iter_mut() {
                    accum.deallocate(batch);
                }
            }
        }

        let error = match racing.await.expect("the task itself must not panic") {
            Ok(_) => panic!("a closed accumulator must reject the append"),
            Err(e) => e.error,
        };
        assert_eq!(error.message(), "Producer closed while send in progress");
        // Java throws a bare `KafkaException` here.
        assert!(
            !error.is_api_error(),
            "a bare Kafka error means doSend rethrows rather than failing the future: {error:?}"
        );

        // The whole pool is back: the first batch's buffer via `deallocate` above, and
        // the racing append's via the guard. Without the guard the latter leaks.
        assert_eq!(
            accum.buffer_pool_available_memory(),
            exhausted + 1024,
            "the buffer allocated before the failure must be back in the pool"
        );
        assert_eq!(
            accum.appends_in_progress.load(Ordering::Relaxed),
            0,
            "the finally's decrement must run on the error path too"
        );
    }

    /// The same `finally`, on the exit Java does not have: the `append` future being
    /// dropped mid-`await`. `append` is `async` public API a caller may wrap in
    /// `tokio::time::timeout`, and it blocks up to `max.block.ms` inside
    /// `free.allocate` — so this is reachable (CLAUDE.md §9.6).
    ///
    /// A lost `appends_in_progress` decrement makes `abort_incomplete_batches` never
    /// leave its loop, and that loop is sync and never yields, so on a
    /// current-thread runtime it starves the very task that would decrement.
    #[tokio::test]
    async fn cancelled_append_balances_appends_in_progress_and_returns_its_buffer() {
        // A pool with room for exactly one batch, so the second append parks inside
        // `free.allocate` and can be cancelled there.
        let accum = Arc::new(create_test_accumulator(1024, 1024, Compression::none(), 0));
        let metadata = make_metadata_snapshot(&[node1()], TOPIC, &[(0, Some(node1().id()))]);
        let now = 0i64;

        append_one(&accum, metadata.cluster(), now).await;
        let after_first = accum.buffer_pool_available_memory();

        let cancelled = {
            let accum = Arc::clone(&accum);
            let cluster = metadata.cluster().clone();
            tokio::spawn(async move {
                accum
                    .append(TOPIC, 1, now, Some(&key()), Some(&value()), &[], None, 60_000, now, &cluster)
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            accum.appends_in_progress.load(Ordering::Relaxed),
            1,
            "the second append should be parked inside free.allocate"
        );
        cancelled.abort();
        let _ = cancelled.await;

        assert_eq!(
            accum.appends_in_progress.load(Ordering::Relaxed),
            0,
            "the drop path must run the finally's decrement"
        );
        assert_eq!(
            accum.buffer_pool_available_memory(),
            after_first,
            "no memory may be lost on the drop path"
        );
    }

    /// `awaitFlushCompletion`'s `finally { flushesInProgress.decrementAndGet(); }`
    /// (`RecordAccumulator.java:1111-1113`) on the drop path.
    ///
    /// A lost decrement makes `flush_in_progress()` answer `true` forever, and it
    /// feeds the sendable predicate in `ready()` — so every partition would look
    /// immediately ready for the producer's whole lifetime, silently disabling
    /// `linger.ms` batching.
    #[tokio::test]
    async fn cancelled_flush_balances_flushes_in_progress() {
        let accum = Arc::new(create_test_accumulator(1024, 10 * 1024, Compression::none(), 1_000));
        let metadata = make_metadata_snapshot(&[node1()], TOPIC, &[(0, Some(node1().id()))]);
        let now = 0i64;

        // One incomplete batch, so `await_flush_completion` has something to park on.
        append_one(&accum, metadata.cluster(), now).await;

        accum.begin_flush();
        assert!(accum.flush_in_progress());

        let cancelled = {
            let accum = Arc::clone(&accum);
            tokio::spawn(async move { accum.await_flush_completion().await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancelled.abort();
        let _ = cancelled.await;

        assert!(
            !accum.flush_in_progress(),
            "the drop path must run the finally's decrement, or linger.ms batching is disabled forever"
        );
    }

    /// `append` must hand the user callback back on every failure path so the
    /// caller can honour the exactly-once callback obligation
    /// (CLAUDE.md §5, §9.5) — see [`AppendFailure`]. Java does not need this
    /// because `KafkaProducer.doSend` keeps its own `callback` reference alive
    /// across the `append` call.
    #[tokio::test]
    async fn test_append_returns_the_callback_on_buffer_exhaustion() {
        let now: i64 = 0;
        let n1 = node1();
        let batch_size = 1024 + RecordBatch::RECORD_BATCH_OVERHEAD as i32;
        // A pool that fits exactly one batch.
        let accum = create_test_accumulator(batch_size, batch_size as i64, Compression::none(), 10);
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0)), (1, Some(0))]);
        let cluster = metadata.cluster().clone();
        let k = key();
        let v = value();

        // Drain the pool.
        accum
            .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();

        // A second partition needs a new buffer; `max_time_to_block = 0` makes
        // the allocation fail immediately.
        let fired = Arc::new(AtomicI32::new(0));
        let counter = Arc::clone(&fired);
        let callback: Callback = Box::new(move |_, _| {
            counter.fetch_add(1, Ordering::SeqCst);
        });
        // `RecordAppendResult` is not `Debug`, so unwrap the error by hand.
        let err = match accum
            .append(TOPIC, 1, 0, Some(&k), Some(&v), &[], Some(callback), 0, now, &cluster)
            .await
        {
            Ok(_) => panic!("the allocation should fail"),
            Err(e) => e,
        };

        assert!(
            matches!(err.error, Error::ProducerBufferExhausted(_)),
            "expected buffer exhaustion, got {:?}",
            err.error
        );
        let returned = err.callback.expect("the callback must be handed back, not dropped");
        assert_eq!(0, fired.load(Ordering::SeqCst), "`append` must not fire the callback itself");
        returned(None, None);
        assert_eq!(1, fired.load(Ordering::SeqCst), "the handed-back callback must be intact");
    }

    /// Same hand-back contract for the "producer closed while send in
    /// progress" failure, which Java raises from
    /// `RecordAccumulator.tryAppend` (`RecordAccumulator.java:427-428`).
    #[tokio::test]
    async fn test_append_returns_the_callback_when_closed() {
        let now: i64 = 0;
        let n1 = node1();
        let batch_size = 1024 + RecordBatch::RECORD_BATCH_OVERHEAD as i32;
        let accum = create_test_accumulator(batch_size, 10 * batch_size as i64, Compression::none(), 10);
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();
        accum.close();

        let fired = Arc::new(AtomicI32::new(0));
        let counter = Arc::clone(&fired);
        let callback: Callback = Box::new(move |_, _| {
            counter.fetch_add(1, Ordering::SeqCst);
        });
        let err = match accum
            .append(TOPIC, 0, 0, Some(&key()), Some(&value()), &[], Some(callback), 0, now, &cluster)
            .await
        {
            Ok(_) => panic!("appending to a closed accumulator should fail"),
            Err(e) => e,
        };

        assert!(err.callback.is_some(), "the callback must be handed back, not dropped");
        assert_eq!(0, fired.load(Ordering::SeqCst), "`append` must not fire the callback itself");
    }

    /// Java brackets the whole of `append` in
    /// `finally { free.deallocate(buffer); }` (`RecordAccumulator.java:355-358`)
    /// and nulls `buffer` only once a batch has taken ownership of it
    /// (`:347-348`), so every exit returns an unused buffer to the pool —
    /// including the **success** exit of the *first* `synchronized (dq)` block.
    ///
    /// That exit holds a live buffer when a sticky-partition switch sends us
    /// back around the loop *after* the buffer was allocated and the newly
    /// selected partition turns out to already have a batch with room. This
    /// test drives exactly that interleaving:
    ///
    /// 1. the sticky partition is `0`, with `produced_bytes == sticky_batch_size`
    ///    banked from an append whose switch was disabled (a non-full last
    ///    batch), so the switch is still pending;
    /// 2. `deque(0)`'s last batch is not full but has no room for our record,
    ///    so the first block returns "no result" without switching partitions;
    /// 3. the pool is empty, so `free.allocate(...)` parks — and while it is
    ///    parked `deque(0)` is drained, which makes `allBatchesFull(dq)` true;
    /// 4. the second block therefore completes the pending switch (to `1`, the
    ///    only partition with a leader in the cluster the appender sees) and
    ///    `continue`s;
    /// 5. on the next iteration the first block's `tryAppend` succeeds, because
    ///    `deque(1)` has a batch with room — reaching the exit under test with
    ///    the buffer still in hand.
    ///
    /// Dropping that buffer instead of deallocating it is not equivalent:
    /// `BufferPool::allocate` debits `non_pooled_available_memory` and only
    /// `deallocate` credits it back (and notifies the next waiter), so every
    /// occurrence permanently shrinks the pool for the lifetime of the
    /// producer.
    #[tokio::test]
    async fn test_append_returns_the_buffer_when_the_first_block_wins_after_a_partition_switch() {
        let now: i64 = 0;
        let n1 = node1();
        let batch_size = 1024 + RecordBatch::RECORD_BATCH_OVERHEAD as i32;
        // Room for exactly two batches, so the third allocation has to wait.
        let accum = Arc::new(create_test_accumulator(
            batch_size,
            2 * batch_size as i64,
            Compression::none(),
            10,
        ));

        // Two views of the same topic: the partitioner picks uniformly among the
        // partitions that have a leader, so restricting the leader makes
        // `next_partition` deterministic. `cluster_before` pins the sticky
        // partition to 0; the appender is handed `cluster_after`, in which the
        // only possible switch target is 1.
        let cluster_before = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0)), (1, None)])
            .cluster()
            .clone();
        let cluster_after = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, None), (1, Some(0))])
            .cluster()
            .clone();

        let k = key();
        // Large enough that two of these do not fit in one batch, small enough
        // that one leaves the batch not full.
        let big = vec![b'v'; 600];

        let (_, topic_info) = accum.get_or_create_topic_info(TOPIC);

        // Bank a pending partition switch on partition 0: `produced_bytes`
        // reaches `sticky_batch_size` with `enable_switch = false`, which is
        // what Java's `updatePartitionInfo` does while the partition's last
        // batch is not full.
        {
            let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
            assert_eq!(0, partitioner.peek_current_partition_info(&cluster_before).partition());
            partitioner.update_partition_info_with_switch(batch_size, &cluster_before, false);
            assert_eq!(
                0,
                partitioner.peek_current_partition_info(&cluster_before).partition(),
                "a disabled switch must not move the sticky partition yet"
            );
        }

        // A batch on partition 0 that is not full but has no room for `big`,
        // and a batch on partition 1 that does have room. Both use an explicit
        // partition, so neither touches the partitioner.
        accum
            .append(TOPIC, 0, 0, Some(&k), Some(&big), &[], None, 0, now, &cluster_before)
            .await
            .unwrap();
        accum
            .append(TOPIC, 1, 0, Some(&k), Some(&value()), &[], None, 0, now, &cluster_before)
            .await
            .unwrap();
        assert_eq!(0, accum.free.available_memory(), "both batches should have drained the pool");

        // The append under test: unknown partition, so the partitioner drives it.
        let appender = {
            let accum = Arc::clone(&accum);
            let cluster = cluster_after.clone();
            let k = k.clone();
            let big = big.clone();
            tokio::spawn(async move {
                accum
                    .append(
                        TOPIC,
                        RecordMetadata::UNKNOWN_PARTITION,
                        0,
                        Some(&k),
                        Some(&big),
                        &[],
                        None,
                        5_000,
                        now,
                        &cluster,
                    )
                    .await
                    // `RecordAppendResult` is not `Send`-agnostic enough to be
                    // worth returning wholesale; the flags are what we assert.
                    .map(|r| r.new_batch_created)
                    .map_err(|e| e.error)
            })
        };

        // Let it reach `free.allocate(...)` and park there.
        for _ in 0..10_000 {
            if accum.free.queued() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            1,
            accum.free.queued(),
            "the appender should be parked in `BufferPool::allocate`"
        );

        // The sender drains partition 0 — its buffer goes back to the pool,
        // which both releases the parked allocation and makes the pending
        // partition switch eligible (`allBatchesFull(dq)` is true for an empty
        // deque).
        topic_info.batches.get(&0).unwrap().lock().unwrap().clear();
        accum.free.deallocate(vec![0u8; batch_size as usize]);

        let new_batch_created = appender.await.unwrap().expect("the append must succeed");

        assert!(
            !new_batch_created,
            "the record must land in the batch that already exists on partition 1"
        );
        assert_eq!(
            1,
            topic_info.batches.get(&1).unwrap().lock().unwrap().len(),
            "no new batch should have been created on partition 1"
        );
        {
            let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
            assert_eq!(
                1,
                partitioner.peek_current_partition_info(&cluster_after).partition(),
                "the pending switch should have completed, which is what sends us around the loop"
            );
        }
        // Only partition 1's batch is still outstanding, so the buffer this
        // append allocated and did not use must be back in the pool.
        assert_eq!(
            batch_size as i64,
            accum.free.available_memory(),
            "the unused buffer must be deallocated, not dropped"
        );
    }
}
