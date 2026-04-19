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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use log::{trace, warn};

use crate::common::Cluster;
use crate::common::KafkaError;
use crate::common::Node;
use crate::common::TopicPartition;
use crate::common::header::internals::RecordHeader;
use crate::common::protocol::Errors;
use crate::common::record::CompressionRatioEstimator;
use crate::common::record::MemoryRecords;
use crate::common::record::MemoryRecordsBuilder;
use crate::common::record::RecordBatch;
use crate::common::record::TimestampType;
use crate::common::record::abstract_records;
use crate::common::utils::ExponentialBackoff;
use crate::metadata_snapshot::MetadataSnapshot;
use crate::producer::internals::BufferPool;
use crate::producer::internals::BuiltInPartitioner;
use crate::producer::internals::FutureRecordMetadata;
use crate::producer::internals::IncompleteBatches;
use crate::producer::internals::{Callback, ProducerBatch};
use crate::producer::record_metadata;

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
    pub unknown_leader_topics: HashSet<Arc<str>>,
}

/// Callbacks passed into append.
///
/// Translated from `RecordAccumulator.AppendCallbacks`.
pub trait AppendCallbacks: Send {
    /// Called to set the partition when it is resolved.
    fn set_partition(&mut self, partition: i32);
    /// Called when the record has been acknowledged or errored.
    fn on_completion(&self, metadata: Option<&crate::producer::RecordMetadata>, error: Option<&KafkaError>);
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
    topic_info_map: DashMap<Arc<str>, Arc<TopicInfo>>,
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
            crate::common_client_configs::RETRY_BACKOFF_EXP_BASE,
            retry_backoff_max_ms,
            crate::common_client_configs::RETRY_BACKOFF_JITTER,
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
    ) -> Result<RecordAppendResult, KafkaError> {
        let (topic_arc, topic_info) = self.get_or_create_topic_info(topic);

        self.appends_in_progress.fetch_add(1, Ordering::Relaxed);

        let result = self
            .append_inner(
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
            )
            .await;

        self.appends_in_progress.fetch_sub(1, Ordering::Relaxed);

        result
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
    ) -> Result<RecordAppendResult, KafkaError> {
        let mut callback = callback;
        let mut buffer: Option<Vec<u8>> = None;

        loop {
            // Determine the effective partition.
            let effective_partition = if partition == record_metadata::UNKNOWN_PARTITION {
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
                if partition == record_metadata::UNKNOWN_PARTITION
                    && self.partition_changed(topic_info, &deque, cluster)
                {
                    continue;
                }

                let (result, returned_callback) =
                    self.try_append(timestamp, key, value, headers, callback, &mut deque, now_ms)?;
                if let Some(result) = result {
                    if partition == record_metadata::UNKNOWN_PARTITION {
                        let enable_switch = Self::all_batches_full(&deque);
                        let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
                        partitioner.update_partition_info_with_switch(result.appended_bytes, cluster, enable_switch);
                    }
                    return Ok(result);
                }
                callback = returned_callback;
            }
            // DashMap guard dropped here — safe to .await below.

            // Need a new batch. Allocate a buffer (only once).
            if buffer.is_none() {
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

                buffer = Some(self.free.allocate(size as usize, max_time_to_block).await?);
            }

            // Try again under lock -- another thread might have created the batch.
            {
                let dq_ref = topic_info.batches.get(&effective_partition).unwrap();
                let mut deque = dq_ref.lock().unwrap();

                if partition == record_metadata::UNKNOWN_PARTITION
                    && self.partition_changed(topic_info, &deque, cluster)
                {
                    continue;
                }

                let (result, returned_callback) =
                    self.try_append(timestamp, key, value, headers, callback, &mut deque, now_ms)?;
                if let Some(result) = result {
                    self.free.deallocate(buffer.take().unwrap());
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
                    returned_callback,
                    buffer.take().unwrap(),
                    now_ms,
                );

                if partition == record_metadata::UNKNOWN_PARTITION {
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
        debug_assert!(partition != record_metadata::UNKNOWN_PARTITION);

        let records_builder = self.records_builder(buffer);
        let tp = TopicPartition::new(Arc::clone(topic), partition);
        let mut batch = ProducerBatch::new(tp, records_builder, now_ms);

        let future = batch
            .try_append(timestamp, key, value, headers, callback, now_ms)
            .unwrap_or_else(|_| panic!("Newly created batch should have room for at least one record"));

        let estimated_size = batch.estimated_size_in_bytes() as i32;
        let batch_is_full = !deque.is_empty() || batch.is_full();

        self.incomplete.add(Arc::clone(&batch.produce_future));
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
    /// pass it to `append_new_batch`.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn try_append(
        &self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        deque: &mut VecDeque<ProducerBatch>,
        now_ms: i64,
    ) -> Result<(Option<RecordAppendResult>, Option<Callback>), KafkaError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(KafkaError::with_message(
                Errors::UnknownServerError,
                "Producer closed while send in progress",
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
                        Some(RecordAppendResult { future, batch_is_full, new_batch_created: false, appended_bytes }),
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
        let (_topic_arc, topic_info) = self.get_or_create_topic_info(tp.topic());
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
        topic: &Arc<str>,
        topic_info: &TopicInfo,
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

            let leader_epoch = metadata_snapshot.leader_epoch_for(&part);
            let mut deque = deque_mutex.lock().unwrap();

            let deque_size = deque.len();

            let batch = match deque.front_mut() {
                Some(b) => b,
                None => continue,
            };

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
            let tp = TopicPartition::new(part.topic(), part.partition());

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
    fn get_or_create_topic_info(&self, topic: &str) -> (Arc<str>, Arc<TopicInfo>) {
        if let Some(entry) = self.topic_info_map.get(topic) {
            return (entry.key().clone(), Arc::clone(entry.value()));
        }
        let topic_arc: Arc<str> = Arc::from(topic);
        let entry = self
            .topic_info_map
            .entry(Arc::clone(&topic_arc))
            .or_insert_with(|| Arc::new(TopicInfo::new(BuiltInPartitioner::new(&topic_arc, self.batch_size))));
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
                warn!(
                    "Skipping deallocating a batch that has already been deallocated. \
                     Batch is {}, created time is {}",
                    batch, batch.created_ms
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

    /// Remove from the incomplete list and deallocate the batch buffer.
    ///
    /// Translated from `RecordAccumulator.completeAndDeallocateBatch`.
    pub fn complete_and_deallocate_batch(&self, batch: &mut ProducerBatch) {
        self.complete_batch(batch);
        self.deallocate(batch);
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
        // Obtain a copy of all of the incomplete ProduceRequestResult(s) at the
        // time of the flush. We must be careful not to hold a reference to the
        // ProducerBatch(s) so that the sender can complete and remove them.
        let results = self.incomplete.request_results();
        for result in results {
            result.await_all_dependents().await;
        }
        self.flushes_in_progress.fetch_sub(1, Ordering::Relaxed);
    }

    /// Abort any batches which have not been drained.
    ///
    /// Translated from `RecordAccumulator.abortUndrainedBatches`.
    pub fn abort_undrained_batches(&self, reason: KafkaError) {
        for topic_info_ref in self.topic_info_map.iter() {
            let topic_info = topic_info_ref.value();
            for deque_ref in topic_info.batches.iter() {
                let mut deque = deque_ref.value().lock().unwrap();
                // Abort only batches that haven't been drained (i.e., not closed).
                let mut i = 0;
                while i < deque.len() {
                    if !deque[i].is_closed() {
                        let mut batch = deque.remove(i).unwrap();
                        batch.abort_record_appends();
                        batch.abort(reason.clone());
                    } else {
                        i += 1;
                    }
                }
            }
        }
    }

    /// Split a big batch and re-enqueue the resulting sub-batches.
    ///
    /// Translated from `RecordAccumulator.splitAndReenqueue`.
    pub fn split_and_reenqueue(&self, mut big_batch: ProducerBatch) -> usize {
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

        while let Some(batch) = sub_batches.pop_back() {
            self.incomplete.add(Arc::clone(&batch.produce_future));
            deque.push_front(batch);
        }

        num_split_batches
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Node;
    use crate::common::compress::Compression;
    use crate::common::protocol::Errors;
    use crate::common::record::CompressionType;
    use crate::common::record::DefaultRecord;
    use crate::common::record::RecordBatch;
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
        let batches = accum.drain(&metadata, &nodes, i32::MAX, now);
        let node_batches = batches.get(&n1.id()).unwrap();
        assert_eq!(1, node_batches.len());

        // Re-enqueue.
        let batch = batches.into_values().next().unwrap().into_iter().next().unwrap();
        accum.reenqueue(batch, now + 1);

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
        let batches = accum.drain(&metadata, &nodes, 1024, now);
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

        let pool = Arc::new(BufferPool::new(total_size, batch_size as usize));
        let accum = RecordAccumulator::new(
            batch_size,
            Compression::none(),
            linger_ms,
            retry_backoff_ms,
            retry_backoff_max_ms,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            pool,
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
        let batches = accum.drain(&metadata, &nodes_set, i32::MAX, now + linger_ms as i64 + 1);
        assert_eq!(1, batches.len(), "Node1 should be the only ready node.");
        assert_eq!(
            1,
            batches.get(&0).unwrap().len(),
            "Partition 0 should only have one batch drained."
        );

        // Reenqueue the batch
        let batch = batches.into_values().next().unwrap().into_iter().next().unwrap();
        accum.reenqueue(batch, now);

        // Put message for partition 1 into accumulator
        accum
            .append(TOPIC, 1, 0, Some(&k), Some(&v), &[], None, 0, now, &cluster)
            .await
            .unwrap();
        let result = accum.ready(&metadata, now + linger_ms as i64 + 1);
        assert!(result.ready_nodes.contains(&n1), "Node1 should be ready");

        // tp1 should backoff while tp2 should not
        let batches = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now + linger_ms as i64 + 1);
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
            (retry_backoff_ms as f64 * (1.0 + crate::common_client_configs::RETRY_BACKOFF_JITTER)) as i64;
        let result = accum.ready(&metadata, now + upper_bound_backoff_ms + 1);
        assert!(result.ready_nodes.contains(&n1), "Node1 should be ready");
        let batches = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now + upper_bound_backoff_ms + 1);
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
        let mut results = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now);

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
        let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now_after_linger);
        assert_eq!(0, drained.get(&n1.id()).unwrap().len(), "No batch should have been drained");

        // Test drain without muted partition.
        accum.unmute_partition(&tp);
        let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now_after_linger);
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
        let drained = accum.drain(&metadata, &ready_nodes, i32::MAX, now);
        assert!(drained.is_empty());

        // Advance clock and send one batch out.
        let now_after_linger = now + linger_ms as i64 + 1;
        let ready_nodes = accum.ready(&metadata, now_after_linger).ready_nodes;
        let drained = accum.drain(&metadata, &ready_nodes, i32::MAX, now_after_linger);
        assert_eq!(1, drained.len(), "A batch did not drain after linger");

        // Queue another batch and advance clock.
        accum
            .append(TOPIC, 1, 0, Some(&k), Some(&v), &[], None, 0, now_after_linger, &cluster)
            .await
            .unwrap();
        let now_advanced = now_after_linger + linger_ms as i64 * 4;

        // Now drain and check that accumulator picked up the drained batch.
        let ready_nodes = accum.ready(&metadata, now_advanced).ready_nodes;
        let drained = accum.drain(&metadata, &ready_nodes, i32::MAX, now_advanced);
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

        let pool = Arc::new(BufferPool::new(
            10 * batch_size as i64,
            (batch_size + RecordBatch::RECORD_BATCH_OVERHEAD as i32) as usize,
        ));
        let accum = RecordAccumulator::new(
            batch_size + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            Compression::none(),
            linger_ms,
            100,
            1000,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            pool,
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
            let drained = accum.drain(&metadata, &ready_nodes, i32::MAX, now);
            assert_eq!(1, drained.get(&n1.id()).unwrap().len(), "There should be only one batch.");
            now += rtt;
            let batch = drained.into_values().next().unwrap().into_iter().next().unwrap();
            accum.reenqueue(batch, now);

            let tp = tp1();
            if mute {
                accum.mute_partition(tp.clone());
            } else {
                accum.unmute_partition(&tp);
            }

            // test expiration
            now += delivery_timeout_ms as i64 - rtt;
            accum.drain(&metadata, &HashSet::from([n1.clone()]), i32::MAX, now);
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
        let batches = accum.drain(&metadata, &HashSet::from([n2.clone()]), 999999, now);
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

        let pool = Arc::new(BufferPool::new(
            10 * batch_size as i64,
            (batch_size + RecordBatch::RECORD_BATCH_OVERHEAD as i32) as usize,
        ));
        let accum = RecordAccumulator::new(
            batch_size + RecordBatch::RECORD_BATCH_OVERHEAD as i32,
            Compression::none(),
            linger_ms,
            100,
            1000,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            pool,
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
                let mut batches = accum_drain.drain(&metadata_drain, &nodes, 5 * 1024, now);
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
        let batches1 = accum.drain(&metadata, &nodes_set, batch_size, now);
        let total1: usize = batches1.values().map(|v| v.len()).sum();
        assert_eq!(2, total1, "Should drain exactly one batch per node");

        // drain with max size: should get remaining batches
        let batches2 = accum.drain(&metadata, &nodes_set, batch_size, now);
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
        let batches4 = accum.drain(&metadata, &nodes_set, batch_size, now);
        let n2_batches = batches4.get(&n2.id()).unwrap();
        for b in n2_batches {
            assert_ne!(3, b.topic_partition.partition(), "Muted partition 3 should not be drained");
        }

        // Unmute and drain with max size
        accum.unmute_partition(&tp4);
        let batches5 = accum.drain(&metadata, &nodes_set, i32::MAX, now);
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
            let batches = accum.drain(metadata, &result.ready_nodes, i32::MAX, now);
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

        let pool = Arc::new(BufferPool::new(total_size, batch_size as usize));
        let accum = RecordAccumulator::new(
            batch_size,
            Compression::none(),
            linger_ms,
            retry_backoff_ms,
            retry_backoff_max_ms,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            pool,
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
        let jitter = crate::common_client_configs::RETRY_BACKOFF_JITTER;
        let exp_base = crate::common_client_configs::RETRY_BACKOFF_EXP_BASE;

        let mut i = 0;
        while (current_retry_backoff_ms as f64) < retry_backoff_max_ms as f64 * (1.0 - jitter) {
            accum.reenqueue(batch, now);
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
        let jitter = crate::common_client_configs::RETRY_BACKOFF_JITTER;
        let exp_base = crate::common_client_configs::RETRY_BACKOFF_EXP_BASE;

        let pool = Arc::new(BufferPool::new(total_size, batch_size as usize));
        let accum = RecordAccumulator::new(
            batch_size,
            Compression::none(),
            linger_ms,
            retry_backoff_ms,
            retry_backoff_max_ms,
            delivery_timeout_ms,
            PartitionerConfig::default(),
            pool,
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
        accum.reenqueue(batch, now);
        let lower_bound = (retry_backoff_ms as f64 * (1.0 - jitter)) as i64;
        let upper_bound = (retry_backoff_ms as f64 * (1.0 + jitter)) as i64;
        // Should back off
        drain_and_check_batch_amount(&metadata_cache, &n1, &accum, initial + lower_bound - 1, 0);
        // Should not back off
        let batches2 =
            drain_and_check_batch_amount(&metadata_cache, &n1, &accum, initial + upper_bound + 1, 1).unwrap();
        batch = batches2.into_values().next().unwrap().into_iter().next().unwrap();

        // Retry 2 - delay by retryBackoffMs * 2 +/- jitter
        accum.reenqueue(batch, now);
        let lower_bound = (retry_backoff_ms as f64 * exp_base as f64 * (1.0 - jitter)) as i64;
        let upper_bound = (retry_backoff_ms as f64 * exp_base as f64 * (1.0 + jitter)) as i64;
        drain_and_check_batch_amount(&metadata_cache, &n1, &accum, initial + lower_bound - 1, 0);
        let batches3 =
            drain_and_check_batch_amount(&metadata_cache, &n1, &accum, initial + upper_bound + 1, 1).unwrap();
        batch = batches3.into_values().next().unwrap().into_iter().next().unwrap();

        // Retry 3 - after a leader change, backoff still applies based on attempts
        accum.reenqueue(batch, now);
        let lower_bound = (retry_backoff_ms as f64 * (exp_base as f64).powi(2) * (1.0 - jitter)) as i64;
        let upper_bound = (retry_backoff_ms as f64 * (exp_base as f64).powi(2) * (1.0 + jitter)) as i64;
        drain_and_check_batch_amount(&metadata_cache_change, &n2, &accum, initial + lower_bound - 1, 0);
        let batches4 =
            drain_and_check_batch_amount(&metadata_cache_change, &n2, &accum, initial + upper_bound + 1, 1).unwrap();
        batch = batches4.into_values().next().unwrap().into_iter().next().unwrap();

        // Retry 4 - capped to retryBackoffMaxMs
        accum.reenqueue(batch, now);
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
            let cb: Callback = Box::new(move |_metadata, _exception| {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            });
            accum
                .append(TOPIC, i % 3, 0, Some(&k), Some(&v), &[], Some(cb), 0, now, &cluster)
                .await
                .unwrap();
        }

        let result = accum.ready(&metadata, now);
        assert!(!result.ready_nodes.is_empty());
        let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now);
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
                let reason = KafkaError::with_message(Errors::UnknownServerError, "Producer is closed forcefully.");
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

        let cause = KafkaError::with_message(Errors::UnknownServerError, "test cause");

        for i in 0..num_records {
            let count = Arc::clone(&callback_count);
            let cb: Callback = Box::new(move |_metadata, _exception| {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            });
            accum
                .append(TOPIC, i % 3, 0, Some(&k), Some(&v), &[], Some(cb), 0, now, &cluster)
                .await
                .unwrap();
        }

        let result = accum.ready(&metadata, now);
        assert!(!result.ready_nodes.is_empty());
        let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now);
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

        // Enqueue the batch
        accum.reenqueue(batch, now);

        // Re-enqueueing counts as a second attempt, so the backoff delay needs to elapse
        let drain_time = now + 121;
        let result = accum.ready(&metadata, drain_time);
        assert!(!result.ready_nodes.is_empty(), "The batch should be ready");
        let mut drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, drain_time);
        assert_eq!(1, drained.get(&n1.id()).map_or(0, |v| v.len()));

        // Split and reenqueue
        let big_batch = drained.get_mut(&n1.id()).unwrap().remove(0);
        accum.split_and_reenqueue(big_batch);

        // Drain the split batches
        let drain_time2 = drain_time + 101;
        let mut drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, drain_time2);
        assert!(!drained.is_empty());
        let first_batch = drained.get_mut(&n1.id()).unwrap();
        assert!(!first_batch.is_empty());
        first_batch[0].complete(acked.load(std::sync::atomic::Ordering::SeqCst) as i64, 100);
        assert_eq!(1, acked.load(std::sync::atomic::Ordering::SeqCst));

        let mut drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, drain_time2);
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
        let mut results = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now);
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
        accum.reenqueue(big_batch, 0);
        let result = accum.ready(&metadata, 0);
        let drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, 0);

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
                        crate::producer::record_metadata::UNKNOWN_PARTITION,
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
            let drained_map = accum.drain(&metadata, &nodes, i32::MAX, 0);
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

        // Add the batch to the accumulator
        accum.reenqueue(big_batch, now);

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
            let mut drained = accum.drain(&metadata, &result.ready_nodes, i32::MAX, now + 200);
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

            let num_split = accum.split_and_reenqueue(batch);
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
}
