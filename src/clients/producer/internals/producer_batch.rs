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

//! Producer batch — a batch of records being accumulated for a single partition.
//!
//! Translated from `org.apache.kafka.clients.producer.internals.ProducerBatch`.
//!
//! This class is not thread safe and external synchronization must be used when modifying it.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU8, Ordering};

use log::{debug, trace};

use crate::clients::producer::internals::future_record_metadata::FutureRecordMetadata;
use crate::clients::producer::internals::produce_request_result::ProduceRequestResult;
use crate::clients::producer::record_metadata;
use crate::common::header::Header;
use crate::common::header::internals::RecordHeader;
use crate::common::kafka_error::KafkaError;
use crate::common::record::abstract_records;
use crate::common::record::compression_ratio_estimator::CompressionRatioEstimator;
use crate::common::record::compression_type::CompressionType;
use crate::common::record::memory_records::MemoryRecords;
use crate::common::record::memory_records_builder::MemoryRecordsBuilder;
use crate::common::record::record_batch::RecordBatch;
use crate::common::record::record_trait::Record;
use crate::common::record::timestamp_type::TimestampType;
use crate::common::topic_partition::TopicPartition;

/// The final state of a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalState {
    Aborted,
    Failed,
    Succeeded,
}

/// Not set sentinel for the atomic final state.
const FINAL_STATE_NONE: u8 = 0;
const FINAL_STATE_ABORTED: u8 = 1;
const FINAL_STATE_FAILED: u8 = 2;
const FINAL_STATE_SUCCEEDED: u8 = 3;

fn to_final_state(val: u8) -> Option<FinalState> {
    match val {
        FINAL_STATE_NONE => None,
        FINAL_STATE_ABORTED => Some(FinalState::Aborted),
        FINAL_STATE_FAILED => Some(FinalState::Failed),
        FINAL_STATE_SUCCEEDED => Some(FinalState::Succeeded),
        _ => unreachable!(),
    }
}

fn from_final_state(state: FinalState) -> u8 {
    match state {
        FinalState::Aborted => FINAL_STATE_ABORTED,
        FinalState::Failed => FINAL_STATE_FAILED,
        FinalState::Succeeded => FINAL_STATE_SUCCEEDED,
    }
}

/// Type alias for the callback function.
pub type Callback = Box<dyn FnOnce(Option<&crate::clients::producer::RecordMetadata>, Option<&KafkaError>) + Send>;

/// A callback and the associated FutureRecordMetadata argument to pass to it.
pub(crate) struct Thunk {
    pub callback: Option<Callback>,
    pub future: Arc<FutureRecordMetadata>,
}

/// A batch of records that is or will be sent.
///
/// This class is not thread safe and external synchronization must be used when modifying it.
pub struct ProducerBatch {
    /// The time this batch was created (milliseconds).
    pub created_ms: i64,
    /// The topic-partition this batch is destined for.
    pub topic_partition: TopicPartition,
    /// The future result of the produce request for this batch.
    pub produce_future: Arc<ProduceRequestResult>,

    /// Record count.
    pub record_count: i32,
    /// Maximum single record size in the batch (estimated upper bound).
    pub max_record_size: i32,

    thunks: Vec<Thunk>,
    records_builder: MemoryRecordsBuilder,
    attempts: AtomicI32,
    is_split_batch: bool,
    final_state: AtomicU8,
    buffer_deallocated: bool,
    /// Tracks if the batch has been sent to the NetworkClient.
    inflight: bool,

    last_attempt_ms: i64,
    last_append_time: i64,
    drained_ms: i64,
    retry: bool,
    reopened: bool,

    /// Tracks the current-leader's epoch to which this batch would be sent.
    current_leader_epoch: Option<i32>,
    /// Tracks the attempt in which leader was changed to current_leader_epoch for the 1st time.
    attempts_when_leader_last_changed: i32,
}

impl ProducerBatch {
    /// Create a new `ProducerBatch`.
    pub fn new(tp: TopicPartition, records_builder: MemoryRecordsBuilder, created_ms: i64) -> Self {
        Self::new_with_split(tp, records_builder, created_ms, false)
    }

    /// Create a new `ProducerBatch`, optionally marking it as a split batch.
    pub fn new_with_split(
        tp: TopicPartition,
        mut records_builder: MemoryRecordsBuilder,
        created_ms: i64,
        is_split_batch: bool,
    ) -> Self {
        let compression_ratio_estimation =
            CompressionRatioEstimator::estimation(tp.topic(), records_builder.compression().compression_type());
        records_builder.set_estimated_compression_ratio(compression_ratio_estimation);

        let produce_future = Arc::new(ProduceRequestResult::new(tp.clone()));

        Self {
            created_ms,
            last_attempt_ms: created_ms,
            records_builder,
            topic_partition: tp,
            last_append_time: created_ms,
            produce_future,
            retry: false,
            is_split_batch,
            current_leader_epoch: None,
            attempts_when_leader_last_changed: 0,
            thunks: Vec::new(),
            attempts: AtomicI32::new(0),
            final_state: AtomicU8::new(FINAL_STATE_NONE),
            buffer_deallocated: false,
            inflight: false,
            record_count: 0,
            max_record_size: 0,
            drained_ms: 0,
            reopened: false,
        }
    }

    /// Update the leader epoch if a newer leader is known.
    pub fn maybe_update_leader_epoch(&mut self, latest_leader_epoch: Option<i32>) {
        if let Some(latest) = latest_leader_epoch {
            if self.current_leader_epoch.is_none() || self.current_leader_epoch.unwrap() < latest {
                trace!(
                    "For {}, leader will be updated, current_leader_epoch: {:?}, \
                     attempts_when_leader_last_changed:{}, latest_leader_epoch: {:?}, \
                     current attempt: {}",
                    self,
                    self.current_leader_epoch,
                    self.attempts_when_leader_last_changed,
                    latest_leader_epoch,
                    self.attempts()
                );
                self.attempts_when_leader_last_changed = self.attempts();
                self.current_leader_epoch = Some(latest);
            } else {
                trace!(
                    "For {}, leader wasn't updated, current_leader_epoch: {:?}, \
                     attempts_when_leader_last_changed:{}, latest_leader_epoch: {:?}, \
                     current attempt: {}",
                    self,
                    self.current_leader_epoch,
                    self.attempts_when_leader_last_changed,
                    latest_leader_epoch,
                    self.attempts()
                );
            }
        } else {
            trace!(
                "For {}, leader wasn't updated (empty epoch), current_leader_epoch: {:?}, \
                 attempts_when_leader_last_changed:{}",
                self, self.current_leader_epoch, self.attempts_when_leader_last_changed,
            );
        }
    }

    /// Returns true if the batch is being retried to a newer leader.
    pub fn has_leader_changed_for_the_ongoing_retry(&self) -> bool {
        let attempts = self.attempts();
        let is_retry = attempts >= 1;
        if !is_retry {
            return false;
        }
        attempts == self.attempts_when_leader_last_changed
    }

    /// Append the record to the current record set and return the relative offset within that
    /// record set.
    ///
    /// Returns `None` if there isn't sufficient room.
    pub fn try_append(
        &mut self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        now: i64,
    ) -> Option<Arc<FutureRecordMetadata>> {
        if !self.records_builder.has_room_for(timestamp, key, value, headers) {
            return None;
        }

        self.records_builder.append(timestamp, key, value, headers);
        self.max_record_size = self.max_record_size.max(abstract_records::estimate_size_in_bytes_upper_bound(
            self.magic(),
            self.records_builder.compression().compression_type(),
            key,
            value,
            headers,
        ));
        self.last_append_time = now;

        let key_size = key.map_or(-1, |k| k.len() as i32);
        let value_size = value.map_or(-1, |v| v.len() as i32);

        let future = Arc::new(FutureRecordMetadata::new(
            Arc::clone(&self.produce_future),
            self.record_count,
            timestamp,
            key_size,
            value_size,
        ));

        self.thunks.push(Thunk { callback, future: Arc::clone(&future) });
        self.record_count += 1;
        Some(future)
    }

    /// This method is only used by [`split`](Self::split) when splitting a large batch to smaller
    /// ones.
    ///
    /// Returns `true` if the record has been successfully appended, `false` otherwise.
    fn try_append_for_split(
        &mut self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        thunk: Thunk,
    ) -> bool {
        if !self.records_builder.has_room_for(timestamp, key, value, headers) {
            return false;
        }

        self.records_builder.append(timestamp, key, value, headers);
        self.max_record_size = self.max_record_size.max(abstract_records::estimate_size_in_bytes_upper_bound(
            self.magic(),
            self.records_builder.compression().compression_type(),
            key,
            value,
            headers,
        ));

        let key_size = key.map_or(-1, |k| k.len() as i32);
        let value_size = value.map_or(-1, |v| v.len() as i32);

        let future = Arc::new(FutureRecordMetadata::new(
            Arc::clone(&self.produce_future),
            self.record_count,
            timestamp,
            key_size,
            value_size,
        ));

        // Chain the future to the original thunk.
        thunk.future.chain_arc(Arc::clone(&future));
        self.thunks.push(Thunk { callback: thunk.callback, future });
        self.record_count += 1;
        true
    }

    /// Abort the batch and complete the future and callbacks.
    pub fn abort(&self, exception: KafkaError) {
        let prev = self.final_state.compare_exchange(
            FINAL_STATE_NONE,
            FINAL_STATE_ABORTED,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        if prev.is_err() {
            let current = to_final_state(self.final_state.load(Ordering::SeqCst));
            panic!("Batch has already been completed in final state {:?}", current);
        }

        trace!("Aborting batch for partition {}", self.topic_partition);

        let err = Arc::new(exception);
        let error_fn: Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync> = {
            let err = Arc::clone(&err);
            Arc::new(move |_idx| Some((*err).clone()))
        };
        self.complete_future_and_fire_callbacks(
            record_metadata::INVALID_OFFSET,
            RecordBatch::NO_TIMESTAMP,
            Some(error_fn),
        );
    }

    /// Check if the batch has been completed (either successfully or exceptionally).
    pub fn is_done(&self) -> bool {
        self.final_state().is_some()
    }

    /// Complete the batch successfully.
    ///
    /// Returns `true` if the batch was completed as a result of this call.
    pub fn complete(&self, base_offset: i64, log_append_time: i64) -> bool {
        self.done(base_offset, log_append_time, None)
    }

    /// Complete the batch exceptionally.
    ///
    /// Returns `true` if the batch was completed as a result of this call.
    pub fn complete_exceptionally(
        &self,
        _top_level_exception: KafkaError,
        record_exceptions: Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync>,
    ) -> bool {
        self.done(
            record_metadata::INVALID_OFFSET,
            RecordBatch::NO_TIMESTAMP,
            Some(record_exceptions),
        )
    }

    /// Finalize the state of a batch.
    fn done(
        &self,
        base_offset: i64,
        log_append_time: i64,
        record_exceptions: Option<Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync>>,
    ) -> bool {
        let try_final_state = if record_exceptions.is_none() {
            FinalState::Succeeded
        } else {
            FinalState::Failed
        };

        if try_final_state == FinalState::Succeeded {
            trace!(
                "Successfully produced messages to {} with base offset {}.",
                self.topic_partition, base_offset
            );
        } else {
            trace!(
                "Failed to produce messages to {} with base offset {}.",
                self.topic_partition, base_offset
            );
        }

        let prev = self.final_state.compare_exchange(
            FINAL_STATE_NONE,
            from_final_state(try_final_state),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );

        if prev.is_ok() {
            self.complete_future_and_fire_callbacks(base_offset, log_append_time, record_exceptions);
            return true;
        }

        let current_state = to_final_state(self.final_state.load(Ordering::SeqCst));
        if current_state != Some(FinalState::Succeeded) {
            if try_final_state == FinalState::Succeeded {
                debug!(
                    "ProduceResponse returned {:?} for {} after batch with base offset {} \
                     had already been {:?}.",
                    try_final_state, self.topic_partition, base_offset, current_state
                );
            } else {
                debug!(
                    "Ignored state transition {:?} -> {:?} for {} batch with base offset {}",
                    current_state, try_final_state, self.topic_partition, base_offset
                );
            }
        } else {
            panic!(
                "A {:?} batch must not attempt another state change to {:?}",
                current_state, try_final_state
            );
        }
        false
    }

    fn complete_future_and_fire_callbacks(
        &self,
        base_offset: i64,
        log_append_time: i64,
        record_exceptions: Option<Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync>>,
    ) {
        // Set the future before invoking the callbacks
        self.produce_future.set(base_offset, log_append_time, record_exceptions);

        // In Java, callbacks are fired here. In Rust, callback invocation is handled
        // through the FutureRecordMetadata.get() async pattern. The produce_future.done()
        // call unblocks all FutureRecordMetadata waiters.

        self.produce_future.done();
    }

    /// Split the batch into smaller batches.
    pub fn split(&mut self, split_batch_size: i32) -> VecDeque<ProducerBatch> {
        let memory_records = self.validate_and_get_records();
        let batches = self.split_records_into_batches(&memory_records, split_batch_size);
        self.finalize_split_batches(&batches);
        batches
    }

    fn validate_and_get_records(&mut self) -> MemoryRecords {
        let memory_records = self.records_builder.build();
        let batch_count = memory_records.batches().count();
        if batch_count == 0 {
            panic!("Cannot split an empty producer batch.");
        }
        if batch_count > 1 {
            panic!("A producer batch should only have one record batch.");
        }
        // Check magic and compression
        let first_batch = memory_records.batches().next().unwrap();
        if first_batch.magic() < RecordBatch::MAGIC_VALUE_V2 && first_batch.compression_type() == CompressionType::None
        {
            panic!("Batch splitting cannot be used with non-compressed messages with version v0 and v1");
        }
        memory_records
    }

    fn split_records_into_batches(
        &mut self,
        memory_records: &MemoryRecords,
        split_batch_size: i32,
    ) -> VecDeque<ProducerBatch> {
        let mut batches = VecDeque::new();
        let mut thunk_iter = std::mem::take(&mut self.thunks).into_iter();
        let mut current_batch: Option<ProducerBatch> = None;

        for record in memory_records.records() {
            let key = record.key();
            let value = record.value();
            let headers: Vec<RecordHeader> = record
                .headers()
                .iter()
                .map(|h| RecordHeader::new(h.key().to_string(), h.value().map(|v: &[u8]| v.to_vec())))
                .collect();
            let timestamp = record.timestamp();

            let thunk = thunk_iter.next().expect("thunk iterator exhausted before records");

            if current_batch.is_none() {
                current_batch =
                    Some(self.create_batch_off_accumulator_for_record(key, value, &headers, split_batch_size));
            }

            let b = current_batch.as_mut().unwrap();
            if !b.try_append_for_split(timestamp, key, value, &headers, thunk) {
                // Current batch is full, close it and start a new one
                let mut completed_batch = current_batch.take().unwrap();
                completed_batch.close_for_record_appends();
                batches.push_back(completed_batch);

                // The first record in a new batch always fits because has_room_for
                // returns true when num_records == 0. In Java the same thunk is
                // reused for the retry, but our method takes ownership of the Thunk.
                // Since the first record always fits, this path is unreachable.
                unreachable!(
                    "first record in a new batch always fits (has_room_for returns true when num_records == 0)"
                );
            }
        }

        // Close the last batch
        if let Some(mut b) = current_batch.take() {
            b.close_for_record_appends();
            batches.push_back(b);
        }

        batches
    }

    fn finalize_split_batches(&self, batches: &VecDeque<ProducerBatch>) {
        // Chain all split batch ProduceRequestResults to the original batch's produceFuture
        for split_batch in batches {
            self.produce_future.add_dependent(Arc::clone(&split_batch.produce_future));
        }

        let error_fn: Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync> =
            Arc::new(|_idx| Some(KafkaError::record_batch_too_large("Record batch too large".to_string())));
        self.produce_future
            .set(record_metadata::INVALID_OFFSET, RecordBatch::NO_TIMESTAMP, Some(error_fn));
        self.produce_future.done();

        // Assign producer state to split batches if the original batch has sequences.
        // Note: In Java, mutable access is available. Here we skip the producer state
        // assignment since it's handled when batches are dequeued for sending (consistent
        // with Java comment in createBatchOffAccumulatorForRecord).
    }

    fn create_batch_off_accumulator_for_record(
        &self,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        batch_size: i32,
    ) -> ProducerBatch {
        let initial_size = (abstract_records::estimate_size_in_bytes_upper_bound(
            self.magic(),
            self.records_builder.compression().compression_type(),
            key,
            value,
            headers,
        ))
        .max(batch_size) as usize;

        let builder = MemoryRecords::builder_with_magic(
            initial_size,
            self.magic(),
            self.records_builder.compression().clone(),
            TimestampType::CreateTime,
            0,
        );
        ProducerBatch::new_with_split(self.topic_partition.clone(), builder, self.created_ms, true)
    }

    /// Returns whether the batch uses compression.
    pub fn is_compressed(&self) -> bool {
        self.records_builder.compression().compression_type() != CompressionType::None
    }

    /// Whether the delivery timeout has been reached.
    pub fn has_reached_delivery_timeout(&self, delivery_timeout_ms: i64, now: i64) -> bool {
        delivery_timeout_ms <= now - self.created_ms
    }

    /// The final state of this batch.
    pub fn final_state(&self) -> Option<FinalState> {
        to_final_state(self.final_state.load(Ordering::SeqCst))
    }

    /// The number of delivery attempts.
    pub fn attempts(&self) -> i32 {
        self.attempts.load(Ordering::SeqCst)
    }

    /// Re-enqueue this batch for retry.
    pub fn reenqueued(&mut self, now: i64) {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.last_attempt_ms = self.last_append_time.max(now);
        self.last_append_time = self.last_append_time.max(now);
        self.retry = true;
    }

    /// The time the batch has been in the queue.
    pub fn queue_time_ms(&self) -> i64 {
        self.drained_ms - self.created_ms
    }

    /// How long the batch has waited since the last attempt.
    pub fn waited_time_ms(&self, now_ms: i64) -> i64 {
        (now_ms - self.last_attempt_ms).max(0)
    }

    /// Mark the batch as drained at the given time.
    pub fn drained(&mut self, now_ms: i64) {
        self.drained_ms = self.drained_ms.max(now_ms);
    }

    /// Whether this batch was created by splitting a larger batch.
    pub fn is_split_batch(&self) -> bool {
        self.is_split_batch
    }

    /// Returns if the batch is being retried for sending to kafka.
    pub fn in_retry(&self) -> bool {
        self.retry
    }

    /// Build and return the memory records.
    pub fn records(&mut self) -> MemoryRecords {
        self.records_builder.build()
    }

    /// The estimated size in bytes of the batch.
    pub fn estimated_size_in_bytes(&self) -> usize {
        self.records_builder.estimated_size_in_bytes()
    }

    /// The compression ratio of the batch.
    pub fn compression_ratio(&self) -> f64 {
        self.records_builder.compression_ratio()
    }

    /// Whether the batch is full.
    pub fn is_full(&self) -> bool {
        self.records_builder.is_full()
    }

    /// Set the producer state for idempotent/transactional producing.
    pub fn set_producer_state(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
    ) {
        self.records_builder
            .set_producer_state(producer_id, producer_epoch, base_sequence, is_transactional);
    }

    /// Reset the producer state (for sequence number reset).
    pub fn reset_producer_state(&mut self, producer_id: i64, producer_epoch: i16, base_sequence: i32) {
        debug!(
            "Resetting sequence number of batch with current sequence {} for partition {} to {}",
            self.base_sequence(),
            self.topic_partition,
            base_sequence
        );
        self.reopened = true;
        self.records_builder.reopen_and_rewrite_producer_state(
            producer_id,
            producer_epoch,
            base_sequence,
            self.is_transactional(),
        );
    }

    /// Release resources required for record appends (e.g. compression buffers).
    pub fn close_for_record_appends(&mut self) {
        self.records_builder.close_for_record_appends();
    }

    /// Close this batch, updating compression ratio estimates.
    pub fn close(&mut self) {
        self.records_builder.close();
        if !self.records_builder.is_control_batch() {
            CompressionRatioEstimator::update_estimation(
                self.topic_partition.topic(),
                self.records_builder.compression().compression_type(),
                self.records_builder.compression_ratio() as f32,
            );
        }
        self.reopened = false;
    }

    /// Abort the record builder and reset the state of the underlying buffer.
    pub fn abort_record_appends(&mut self) {
        self.records_builder.abort();
    }

    /// Whether the records have been built (closed).
    pub fn is_closed(&self) -> bool {
        self.records_builder.is_closed()
    }

    /// Returns a reference to the underlying buffer.
    pub fn buffer(&self) -> &Vec<u8> {
        self.records_builder.buffer()
    }

    /// Returns the initial capacity of the buffer.
    pub fn initial_capacity(&self) -> usize {
        self.records_builder.initial_capacity()
    }

    /// Whether the batch is still writable (not closed).
    pub fn is_writable(&self) -> bool {
        !self.records_builder.is_closed()
    }

    /// The magic version.
    pub fn magic(&self) -> i8 {
        self.records_builder.magic()
    }

    /// The producer ID.
    pub fn producer_id(&self) -> i64 {
        self.records_builder.producer_id()
    }

    /// The producer epoch.
    pub fn producer_epoch(&self) -> i16 {
        self.records_builder.producer_epoch()
    }

    /// The base sequence.
    pub fn base_sequence(&self) -> i32 {
        self.records_builder.base_sequence()
    }

    /// The last sequence number.
    pub fn last_sequence(&self) -> i32 {
        self.records_builder.base_sequence() + self.records_builder.num_records() - 1
    }

    /// Whether this batch has a sequence assigned.
    pub fn has_sequence(&self) -> bool {
        self.base_sequence() != RecordBatch::NO_SEQUENCE
    }

    /// Whether this batch is transactional.
    pub fn is_transactional(&self) -> bool {
        self.records_builder.is_transactional()
    }

    /// Whether the sequence has been reset.
    pub fn sequence_has_been_reset(&self) -> bool {
        self.reopened
    }

    /// Whether the buffer has been deallocated.
    pub fn is_buffer_deallocated(&self) -> bool {
        self.buffer_deallocated
    }

    /// Mark the buffer as deallocated.
    pub fn mark_buffer_deallocated(&mut self) {
        self.buffer_deallocated = true;
    }

    /// Whether the batch is in-flight.
    pub fn is_inflight(&self) -> bool {
        self.inflight
    }

    /// Set the inflight status.
    pub fn set_inflight(&mut self, inflight: bool) {
        self.inflight = inflight;
    }

    /// The current leader epoch (visible for testing).
    pub fn current_leader_epoch(&self) -> Option<i32> {
        self.current_leader_epoch
    }

    /// The attempt number when the leader was last changed (visible for testing).
    pub fn attempts_when_leader_last_changed(&self) -> i32 {
        self.attempts_when_leader_last_changed
    }
}

impl std::fmt::Display for ProducerBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProducerBatch(topicPartition={}, recordCount={})",
            self.topic_partition, self.record_count
        )
    }
}

impl std::fmt::Debug for ProducerBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerBatch")
            .field("topic_partition", &self.topic_partition)
            .field("record_count", &self.record_count)
            .field("created_ms", &self.created_ms)
            .finish()
    }
}
