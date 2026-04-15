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

//! Queue that accumulates records into [`MemoryRecords`] instances to be sent
//! to the server.
//!
//! Translated from `org.apache.kafka.clients.producer.internals.RecordAccumulator`.
//!
//! The accumulator uses a bounded amount of memory and append calls will block
//! when that memory is exhausted, unless this behavior is explicitly disabled.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use log::{trace, warn};

use crate::clients::metadata_snapshot::MetadataSnapshot;
use crate::clients::producer::internals::buffer_pool::BufferPool;
use crate::clients::producer::internals::built_in_partitioner::BuiltInPartitioner;
use crate::clients::producer::internals::future_record_metadata::FutureRecordMetadata;
use crate::clients::producer::internals::incomplete_batches::IncompleteBatches;
use crate::clients::producer::internals::producer_batch::{Callback, ProducerBatch};
use crate::clients::producer::record_metadata;
use crate::common::cluster::Cluster;
use crate::common::header::internals::RecordHeader;
use crate::common::kafka_error::KafkaError;
use crate::common::node::Node;
use crate::common::protocol::Errors;
use crate::common::record::abstract_records;
use crate::common::record::memory_records::MemoryRecords;
use crate::common::record::memory_records_builder::MemoryRecordsBuilder;
use crate::common::record::record_batch::RecordBatch;
use crate::common::record::timestamp_type::TimestampType;
use crate::common::topic_partition::TopicPartition;
use crate::common::utils::ExponentialBackoff;

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
    pub unknown_leader_topics: HashSet<String>,
}

/// Callbacks passed into append.
///
/// Translated from `RecordAccumulator.AppendCallbacks`.
pub trait AppendCallbacks: Send {
    /// Called to set the partition when it is resolved.
    fn set_partition(&mut self, partition: i32);
    /// Called when the record has been acknowledged or errored.
    fn on_completion(&self, metadata: Option<&crate::clients::producer::RecordMetadata>, error: Option<&KafkaError>);
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
    topic_info_map: DashMap<String, Arc<TopicInfo>>,
    node_stats: DashMap<i32, NodeLatencyStats>,
    incomplete: IncompleteBatches,
    /// Only accessed by the sender thread, so no synchronization needed.
    muted: Mutex<HashSet<TopicPartition>>,
    /// Only accessed by the sender thread.
    nodes_drain_index: Mutex<HashMap<String, usize>>,
    next_batch_expiry_time_ms: Mutex<i64>,
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
    /// * `buffer_pool` - The buffer pool
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_size: i32,
        compression: crate::common::compress::Compression,
        linger_ms: i32,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        delivery_timeout_ms: i32,
        partitioner_config: PartitionerConfig,
        buffer_pool: Arc<BufferPool>,
    ) -> Self {
        let retry_backoff = ExponentialBackoff::new(
            retry_backoff_ms,
            crate::clients::common_client_configs::RETRY_BACKOFF_EXP_BASE,
            retry_backoff_max_ms,
            crate::clients::common_client_configs::RETRY_BACKOFF_JITTER,
        )
        .expect("Invalid backoff parameters");

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
            muted: Mutex::new(HashSet::new()),
            nodes_drain_index: Mutex::new(HashMap::new()),
            next_batch_expiry_time_ms: Mutex::new(i64::MAX),
        }
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
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        _max_time_to_block: i64,
        now_ms: i64,
        cluster: &Cluster,
    ) -> Result<RecordAppendResult, KafkaError> {
        let topic_info = self.get_or_create_topic_info(topic);

        self.appends_in_progress.fetch_add(1, Ordering::Relaxed);

        let result = self.append_inner(
            topic,
            partition,
            timestamp,
            key,
            value,
            headers,
            callback,
            now_ms,
            cluster,
            &topic_info,
        );

        self.appends_in_progress.fetch_sub(1, Ordering::Relaxed);

        result
    }

    #[allow(clippy::too_many_arguments)]
    fn append_inner(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        now_ms: i64,
        cluster: &Cluster,
        topic_info: &Arc<TopicInfo>,
    ) -> Result<RecordAppendResult, KafkaError> {
        // Determine the effective partition.
        let effective_partition = if partition == record_metadata::UNKNOWN_PARTITION {
            let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
            partitioner.peek_current_partition_info(cluster).partition()
        } else {
            partition
        };

        // Get or create the deque for this partition.
        let dq_entry = topic_info
            .batches
            .entry(effective_partition)
            .or_insert_with(|| Mutex::new(VecDeque::new()));
        let dq = dq_entry.value();

        // Try to append to an existing batch.
        {
            let mut deque = dq.lock().unwrap();
            if let Some(result) =
                self.try_append(timestamp, key, value, headers, callback.as_ref(), &mut deque, now_ms)?
            {
                // Update partitioner info.
                if partition == record_metadata::UNKNOWN_PARTITION {
                    let enable_switch = Self::all_batches_full(&deque);
                    let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
                    partitioner.update_partition_info_with_switch(result.appended_bytes, cluster, enable_switch);
                }
                return Ok(result);
            }
        }

        // Need a new batch. Allocate a buffer.
        let estimated = abstract_records::estimate_size_in_bytes_upper_bound(
            RecordBatch::CURRENT_MAGIC_VALUE,
            self.compression.compression_type(),
            key,
            value,
            headers,
        );
        let size = self.batch_size.max(estimated);

        trace!(
            "Allocating a new {} byte message buffer for topic {} partition {}",
            size, topic, effective_partition
        );

        let buffer = vec![0u8; size as usize];

        // Try again under lock -- another thread might have created the batch.
        {
            let mut deque = dq.lock().unwrap();

            if let Some(result) =
                self.try_append(timestamp, key, value, headers, callback.as_ref(), &mut deque, now_ms)?
            {
                // Someone else created the batch, use it.
                self.free.deallocate(buffer);
                if partition == record_metadata::UNKNOWN_PARTITION {
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
                callback,
                buffer,
                now_ms,
            );

            if partition == record_metadata::UNKNOWN_PARTITION {
                let enable_switch = Self::all_batches_full(&deque);
                let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
                partitioner.update_partition_info_with_switch(result.appended_bytes, cluster, enable_switch);
            }

            Ok(result)
        }
    }

    /// Append a new batch to the queue.
    #[allow(clippy::too_many_arguments)]
    fn append_new_batch(
        &self,
        topic: &str,
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
        debug_assert!(partition != record_metadata::UNKNOWN_PARTITION);

        let records_builder = self.records_builder(buffer);
        let tp = TopicPartition::new(topic.to_string(), partition);
        let mut batch = ProducerBatch::new(tp, records_builder, now_ms);

        let future = batch
            .try_append(timestamp, key, value, headers, callback, now_ms)
            .expect("Newly created batch should have room for at least one record");

        let estimated_size = batch.estimated_size_in_bytes() as i32;
        let batch_is_full = !deque.is_empty() || batch.is_full();

        deque.push_back(batch);

        RecordAppendResult { future, batch_is_full, new_batch_created: true, appended_bytes: estimated_size }
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

    /// Check if all batches in the queue are full.
    fn all_batches_full(deque: &VecDeque<ProducerBatch>) -> bool {
        match deque.back() {
            None => true,
            Some(last) => last.is_full(),
        }
    }

    /// Try to append to a ProducerBatch.
    ///
    /// If it is full, we return None and a new batch is created.
    #[allow(clippy::too_many_arguments)]
    fn try_append(
        &self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        _callback: Option<&Callback>,
        deque: &mut VecDeque<ProducerBatch>,
        now_ms: i64,
    ) -> Result<Option<RecordAppendResult>, KafkaError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(KafkaError::with_message(
                Errors::UnknownServerError,
                "Producer closed while send in progress",
            ));
        }

        if let Some(last) = deque.back_mut() {
            let initial_bytes = last.estimated_size_in_bytes() as i32;
            let future = last.try_append(timestamp, key, value, headers, None, now_ms);
            if let Some(future) = future {
                let appended_bytes = last.estimated_size_in_bytes() as i32 - initial_bytes;
                let is_full = last.is_full();
                let batch_is_full = deque.len() > 1 || is_full;
                return Ok(Some(RecordAppendResult {
                    future,
                    batch_is_full,
                    new_batch_created: false,
                    appended_bytes,
                }));
            } else {
                last.close_for_record_appends();
            }
        }
        Ok(None)
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
            warn!(
                "Skipping next batch expiry time update due to addition overflow: \
                 batch.created_ms={}, delivery_timeout_ms={}",
                batch.created_ms, self.delivery_timeout_ms
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
    pub fn reenqueue(&self, mut batch: ProducerBatch, now: i64) {
        batch.reenqueued(now);
        let tp = batch.topic_partition.clone();
        let topic_info = self.get_or_create_topic_info(tp.topic());
        let dq_entry = topic_info
            .batches
            .entry(tp.partition())
            .or_insert_with(|| Mutex::new(VecDeque::new()));
        let mut deque = dq_entry.value().lock().unwrap();
        deque.push_front(batch);
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
            let sendable =
                full || expired || exhausted || self.closed.load(Ordering::Relaxed) || self.flush_in_progress();
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
        topic: &str,
        topic_info: &TopicInfo,
        mut next_ready_check_delay_ms: i64,
        ready_nodes: &mut HashSet<Node>,
        unknown_leader_topics: &mut HashSet<String>,
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
            let part = TopicPartition::new(topic.to_string(), partition);

            let leader = cluster.leader_for(&part);
            if leader.is_some() && queue_sizes.is_some() {
                queue_sizes_index += 1;
                if let Some(ref mut pids) = partition_ids
                    && (queue_sizes_index as usize) < pids.len()
                {
                    pids[queue_sizes_index as usize] = part.partition();
                }
            }

            let deque = deque_mutex.lock().unwrap();

            let batch = match deque.front() {
                Some(b) => b,
                None => continue,
            };

            let waited_time_ms = batch.waited_time_ms(now_ms);
            let backing_off = self.should_backoff(false, batch, waited_time_ms);
            let backoff_attempts = batch.attempts();
            let deque_size = deque.len();
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
                    next_ready_check_delay_ms,
                    ready_nodes,
                );
            } else {
                unknown_leader_topics.insert(part.topic().to_string());
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

        for entry in self.topic_info_map.iter() {
            let topic = entry.key();
            let topic_info = entry.value();
            next_ready_check_delay_ms = self.partition_ready(
                metadata_snapshot,
                now_ms,
                topic,
                topic_info,
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
        if should_backoff {
            trace!("For batch {:?}, will backoff", batch.topic_partition);
        } else {
            trace!(
                "For batch {:?}, will not backoff, should_wait_more {}, has_leader_changed {}",
                batch.topic_partition, should_wait_more, has_leader_changed
            );
        }
        should_backoff
    }

    fn drain_batches_for_one_node(
        &self,
        metadata_snapshot: &MetadataSnapshot,
        node: &Node,
        max_size: i32,
        now: i64,
    ) -> Vec<ProducerBatch> {
        let mut size = 0i32;
        let parts = metadata_snapshot.cluster().partitions_for_node(node.id());
        let mut ready = Vec::new();
        if parts.is_empty() {
            return ready;
        }

        let mut drain_index = self.get_drain_index(node.id_string());
        drain_index %= parts.len();
        let start = drain_index;

        loop {
            let part = &parts[drain_index];
            let tp = TopicPartition::new(part.topic().to_string(), part.partition());

            self.update_drain_index(node.id_string(), drain_index);
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

            let batch = {
                let mut deque = deque_ref.lock().unwrap();
                let first = match deque.front() {
                    Some(b) => b,
                    None => {
                        drop(deque);
                        if start == drain_index {
                            break;
                        }
                        continue;
                    },
                };

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
                    break;
                }

                deque.pop_front().unwrap()
            };

            let mut batch = batch;
            batch.close();
            size += batch.estimated_size_in_bytes() as i32;
            batch.drained(now);
            ready.push(batch);

            if start == drain_index {
                break;
            }
        }
        ready
    }

    fn get_drain_index(&self, id_string: &str) -> usize {
        let map = self.nodes_drain_index.lock().unwrap();
        map.get(id_string).copied().unwrap_or(0)
    }

    fn update_drain_index(&self, id_string: &str, drain_index: usize) {
        let mut map = self.nodes_drain_index.lock().unwrap();
        map.insert(id_string.to_string(), drain_index);
    }

    /// Drain all the data for the given nodes and collate them into a list of
    /// batches that will fit within the specified size on a per-node basis.
    pub fn drain(
        &self,
        metadata_snapshot: &MetadataSnapshot,
        nodes: &HashSet<Node>,
        max_size: i32,
        now: i64,
    ) -> HashMap<i32, Vec<ProducerBatch>> {
        if nodes.is_empty() {
            return HashMap::new();
        }
        let mut batches = HashMap::new();
        for node in nodes {
            let ready = self.drain_batches_for_one_node(metadata_snapshot, node, max_size, now);
            batches.insert(node.id(), ready);
        }
        batches
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
    fn get_or_create_topic_info(&self, topic: &str) -> Arc<TopicInfo> {
        let entry = self
            .topic_info_map
            .entry(topic.to_string())
            .or_insert_with(|| Arc::new(TopicInfo::new(BuiltInPartitioner::new(topic, self.batch_size))));
        Arc::clone(entry.value())
    }

    /// Deallocate the batch buffer back to the pool.
    pub fn deallocate(&self, batch: &ProducerBatch) {
        if !batch.is_split_batch() {
            let capacity = batch.initial_capacity();
            self.free.deallocate(vec![0u8; capacity]);
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
            self.abort_batches();
            if !self.appends_in_progress() {
                break;
            }
        }
        self.abort_batches();
        self.topic_info_map.clear();
    }

    fn abort_batches(&self) {
        for topic_info_ref in self.topic_info_map.iter() {
            let topic_info = topic_info_ref.value();
            for deque_ref in topic_info.batches.iter() {
                let mut deque = deque_ref.value().lock().unwrap();
                while let Some(mut batch) = deque.pop_front() {
                    batch.abort_record_appends();
                    let reason = KafkaError::with_message(Errors::UnknownServerError, "Producer is closed forcefully.");
                    batch.abort(reason);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::compress::Compression;
    use crate::common::node::Node;
    use crate::common::protocol::Errors;
    use crate::common::record::default_record::DefaultRecord;
    use crate::common::record::record_batch::RecordBatch;
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

        let part_metadata: Vec<crate::common::requests::metadata_response::PartitionMetadata> = partition_metadata
            .iter()
            .map(
                |&(partition, leader_id)| crate::common::requests::metadata_response::PartitionMetadata {
                    error: Errors::None,
                    topic_partition: TopicPartition::new(topic.to_string(), partition),
                    leader_id,
                    leader_epoch: None,
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

    fn create_test_accumulator(
        batch_size: i32,
        total_size: i64,
        compression: Compression,
        linger_ms: i32,
    ) -> RecordAccumulator {
        let pool = Arc::new(BufferPool::new(total_size, batch_size as usize));
        RecordAccumulator::new(
            batch_size,
            compression,
            linger_ms,
            100,   // retry_backoff_ms
            1000,  // retry_backoff_max_ms
            30000, // delivery_timeout_ms
            PartitionerConfig::default(),
            pool,
        )
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
    #[test]
    fn test_full() {
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
                .unwrap();
            assert_eq!(1, accum.deque_size(&tp1()));
            let result = accum.ready(&metadata, now);
            assert!(result.ready_nodes.is_empty(), "No partitions should be ready.");
        }

        // This append will trigger a new batch creation.
        let result = accum
            .append(TOPIC, 0, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .unwrap();
        assert!(result.batch_is_full);
        assert!(result.new_batch_created);
        assert_eq!(2, accum.deque_size(&tp1()));

        // Verify the ready node is the leader.
        let result = accum.ready(&metadata, now);
        assert!(result.ready_nodes.contains(&n1));
    }

    /// Translated from `RecordAccumulatorTest.testAppendLargeCompressed`.
    #[test]
    fn test_append_large() {
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
            .unwrap();
        assert!(result.new_batch_created);
    }

    /// Translated from `RecordAccumulatorTest.testLinger`.
    #[test]
    fn test_linger() {
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
            .unwrap();

        // Not ready immediately.
        let result = accum.ready(&metadata, now);
        assert!(result.ready_nodes.is_empty());

        // Ready after linger.
        let result = accum.ready(&metadata, now + linger_ms as i64 + 1);
        assert!(result.ready_nodes.contains(&n1));
    }

    /// Translated from a subset of `RecordAccumulatorTest.testDrainBatches`.
    #[test]
    fn test_drain_batches() {
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
            .unwrap();
        accum
            .append(TOPIC, 1, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .unwrap();
        accum
            .append(TOPIC, 2, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .unwrap();

        // Drain from both nodes with batch_size limit -- should get one batch per node.
        let mut nodes = HashSet::new();
        nodes.insert(n1.clone());
        nodes.insert(n2.clone());
        let batches = accum.drain(&metadata, &nodes, batch_size, now);

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
    #[test]
    fn test_has_undrained() {
        let n1 = node1();
        let now: i64 = 0;
        let batch_size = 1024;

        let accum = create_test_accumulator(batch_size, i64::MAX, Compression::none(), 0);
        let metadata = make_metadata_snapshot(&[n1], TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();

        assert!(!accum.has_undrained());

        accum
            .append(TOPIC, 0, 0, Some(b"key"), Some(b"value"), &[], None, 0, now, &cluster)
            .unwrap();

        assert!(accum.has_undrained());
    }

    /// Test ready with unknown leader.
    #[test]
    fn test_ready_unknown_leader() {
        let n1 = node1();
        let now: i64 = 0;

        let accum = create_test_accumulator(1024, i64::MAX, Compression::none(), 0);

        // Create metadata with partition 0 having no leader.
        let metadata = make_metadata_snapshot(&[n1], TOPIC, &[(0, None)]);
        let cluster = metadata.cluster().clone();

        accum
            .append(TOPIC, 0, 0, Some(b"key"), Some(b"value"), &[], None, 0, now, &cluster)
            .unwrap();

        let result = accum.ready(&metadata, now);
        assert!(result.ready_nodes.is_empty());
        assert!(result.unknown_leader_topics.contains(TOPIC));
    }

    /// Test expired batches.
    #[test]
    fn test_expired_batches() {
        let n1 = node1();
        let delivery_timeout_ms = 100;
        let now: i64 = 0;

        let pool = Arc::new(BufferPool::new(i64::MAX, 1024));
        let accum = RecordAccumulator::new(
            1024,
            Compression::none(),
            0,
            100,
            1000,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            pool,
        );

        let metadata = make_metadata_snapshot(&[n1], TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();

        accum
            .append(TOPIC, 0, 0, Some(b"key"), Some(b"value"), &[], None, 0, now, &cluster)
            .unwrap();

        // Not expired yet.
        let expired = accum.expired_batches(now + delivery_timeout_ms as i64 - 1);
        assert!(expired.is_empty());

        // Expired.
        let expired = accum.expired_batches(now + delivery_timeout_ms as i64 + 1);
        assert!(!expired.is_empty());
    }

    /// Test reenqueue.
    #[test]
    fn test_reenqueue() {
        let n1 = node1();
        let now: i64 = 0;

        let accum = create_test_accumulator(1024, i64::MAX, Compression::none(), 0);
        let metadata = make_metadata_snapshot(std::slice::from_ref(&n1), TOPIC, &[(0, Some(0))]);
        let cluster = metadata.cluster().clone();

        accum
            .append(TOPIC, 0, 0, Some(b"key"), Some(b"value"), &[], None, 0, now, &cluster)
            .unwrap();

        // Drain the batch.
        let mut nodes = HashSet::new();
        nodes.insert(n1.clone());
        let batches = accum.drain(&metadata, &nodes, i32::MAX, now);
        let node_batches = batches.get(&n1.id()).unwrap();
        assert_eq!(1, node_batches.len());

        // Re-enqueue.
        let batch = batches.into_values().next().unwrap().into_iter().next().unwrap();
        accum.reenqueue(batch, now + 1);

        // Should have the batch back.
        assert_eq!(1, accum.deque_size(&tp1()));
    }
}
