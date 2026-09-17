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
use std::sync::atomic::{AtomicI32, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use log::{debug, error, trace};

use crate::common::Error;
use crate::common::TopicPartition;
use crate::common::header::Header;
use crate::common::header::internals::RecordHeader;
use crate::common::record::TimestampType;
use crate::common::record::internal::CompressionRatioEstimator;
use crate::common::record::internal::CompressionType;
use crate::common::record::internal::MemoryRecords;
use crate::common::record::internal::MemoryRecordsBuilder;
use crate::common::record::internal::Record;
use crate::common::record::internal::RecordBatch;
use crate::common::record::internal::abstract_records;
use crate::producer::Callback;
use crate::producer::internals::FutureRecordMetadata;
use crate::producer::internals::ProduceRequestResult;
use crate::producer::record_metadata;

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

    /// The list of thunks (callback + future) for each record appended to this batch.
    /// Wrapped in a `Mutex` to allow `complete_future_and_fire_callbacks` (which takes
    /// `&self` due to the atomic state machine) to take ownership of the callbacks.
    thunks: Mutex<Vec<Thunk>>,
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
            thunks: Mutex::new(Vec::new()),
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
    /// Returns `Ok(future)` if the record was appended, or `Err(callback)` if there isn't
    /// sufficient room (the callback is returned so the caller can retry with a new batch).
    pub fn try_append(
        &mut self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        now: i64,
    ) -> Result<Arc<FutureRecordMetadata>, Option<Callback>> {
        if !self.records_builder.has_room_for(timestamp, key, value, headers) {
            return Err(callback);
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

        self.thunks
            .lock()
            .unwrap()
            .push(Thunk { callback, future: Arc::clone(&future) });
        self.record_count += 1;
        Ok(future)
    }

    /// This method is only used by [`split`](Self::split) when splitting a large batch to smaller
    /// ones.
    ///
    /// Returns `Ok(())` if the record has been successfully appended, or returns the
    /// `Thunk` back via `Err(thunk)` if there was no room so it can be reused.
    fn try_append_for_split(
        &mut self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        thunk: Thunk,
    ) -> Result<(), Thunk> {
        if !self.records_builder.has_room_for(timestamp, key, value, headers) {
            return Err(thunk);
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
        self.thunks.lock().unwrap().push(Thunk { callback: thunk.callback, future });
        self.record_count += 1;
        Ok(())
    }

    /// Abort the batch and complete the future and callbacks.
    pub fn abort(&self, error: Error) {
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

        let err = Arc::new(error);
        let error_fn: Arc<dyn Fn(i32) -> Option<Error> + Send + Sync> = {
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
    pub fn complete_with_error(
        &self,
        _top_level_error: Error,
        record_errors: Arc<dyn Fn(i32) -> Option<Error> + Send + Sync>,
    ) -> bool {
        self.done(record_metadata::INVALID_OFFSET, RecordBatch::NO_TIMESTAMP, Some(record_errors))
    }

    /// Finalize the state of a batch.
    fn done(
        &self,
        base_offset: i64,
        log_append_time: i64,
        record_errors: Option<Arc<dyn Fn(i32) -> Option<Error> + Send + Sync>>,
    ) -> bool {
        let try_final_state = if record_errors.is_none() {
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
            self.complete_future_and_fire_callbacks(base_offset, log_append_time, record_errors);
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
        record_errors: Option<Arc<dyn Fn(i32) -> Option<Error> + Send + Sync>>,
    ) {
        // Set the future before invoking the callbacks as we rely on its state for the
        // `on_completion` call.
        self.produce_future.set(base_offset, log_append_time, record_errors.clone());

        // Execute callbacks — matches Java's loop in completeFutureAndFireCallbacks.
        // Take ownership of the thunks so we can consume FnOnce callbacks.
        let mut thunks = self.thunks.lock().unwrap();
        for (i, thunk) in thunks.iter_mut().enumerate() {
            if let Some(callback) = thunk.callback.take() {
                // Java's try/catch sits INSIDE the loop
                // (`ProducerBatch.java:307-322`), which is what isolates each user
                // callback: one bad callback must neither skip the remaining records
                // nor stop `produceFuture.done()` from running.
                //
                // `Callback` is infallible in Rust, so the only failure channel is a
                // panic. Left uncaught it escaped this loop, so `produce_future` was
                // never marked done — `ProduceRequestResult::await_completion` waits
                // on a `watch` that would then never be set, hanging every
                // `send().await` in the batch and any `flush()` — and it poisoned
                // `self.thunks` while its guard was held. Same shape as
                // `NetworkClient::complete_responses`, which translates the same Java
                // idiom.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if let Some(ref errors_fn) = record_errors {
                        let error = errors_fn(i as i32);
                        callback(None, error.as_ref());
                    } else {
                        let metadata = thunk.future.value();
                        callback(Some(&metadata), None);
                    }
                }));
                if let Err(payload) = result {
                    error!(
                        "Error executing user-provided callback on message for topic-partition '{}': {:?}",
                        self.topic_partition, payload
                    );
                }
            }
        }
        drop(thunks);

        self.produce_future.done();
    }

    /// Split the batch into smaller batches.
    pub fn split(&mut self, split_batch_size: i32) -> VecDeque<ProducerBatch> {
        let memory_records = self.validate_and_get_records();
        let mut batches = self.split_records_into_batches(&memory_records, split_batch_size);
        self.finalize_split_batches(&mut batches);
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
        let mut thunk_iter = std::mem::take(&mut *self.thunks.lock().unwrap()).into_iter();
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
            if let Err(returned_thunk) = b.try_append_for_split(timestamp, key, value, &headers, thunk) {
                // Current batch is full, close it and start a new one
                let mut completed_batch = current_batch.take().unwrap();
                completed_batch.close_for_record_appends();
                batches.push_back(completed_batch);

                let mut new_batch =
                    self.create_batch_off_accumulator_for_record(key, value, &headers, split_batch_size);
                // The first record in a new batch always fits because has_room_for
                // returns true when num_records == 0.
                if new_batch
                    .try_append_for_split(timestamp, key, value, &headers, returned_thunk)
                    .is_err()
                {
                    panic!("first record in a new batch always fits");
                }
                current_batch = Some(new_batch);
            }
        }

        // Close the last batch
        if let Some(mut b) = current_batch.take() {
            b.close_for_record_appends();
            batches.push_back(b);
        }

        batches
    }

    fn finalize_split_batches(&self, batches: &mut VecDeque<ProducerBatch>) {
        // Chain all split batch ProduceRequestResults to the original batch's produceFuture
        for split_batch in batches.iter() {
            self.produce_future.add_dependent(Arc::clone(&split_batch.produce_future));
        }

        let error_fn: Arc<dyn Fn(i32) -> Option<Error> + Send + Sync> =
            Arc::new(|_idx| Some(Error::record_batch_too_large("Record batch too large".to_string())));
        self.produce_future
            .set(record_metadata::INVALID_OFFSET, RecordBatch::NO_TIMESTAMP, Some(error_fn));
        self.produce_future.done();

        self.assign_producer_state_to_batches(batches);
    }

    /// Carries this batch's producer state onto the split batches, giving each one a
    /// base sequence derived from its predecessor's record count.
    ///
    /// Translated from `assignProducerStateToBatches(Deque<ProducerBatch>)`
    /// (`ProducerBatch.java:395-405`), reached from `finalizeSplitBatches` at `:392`.
    ///
    /// This was previously omitted, on the strength of
    /// `createBatchOffAccumulatorForRecord`'s comment that producer state "will be set
    /// when the batch is dequeued for sending". That comment is about the *fresh*
    /// batch; `assignProducerStateToBatches` then fills it in before `split` returns,
    /// which is what `splitAndReenqueue`'s own comment ("they already have assigned
    /// sequences", `RecordAccumulator.java:531`) refers to. Without it,
    /// `RecordAccumulator::split_and_reenqueue` cannot call `addInFlightBatch` on an
    /// idempotent producer — it fails with "Can't track batch for partition … when
    /// sequence is not set" — so a `MESSAGE_TOO_LARGE` response would break the
    /// idempotent send path. Surfaced by Milestone 11 Phase 4, which is the first
    /// phase where the accumulator has a `TransactionManager` at all.
    fn assign_producer_state_to_batches(&self, batches: &mut VecDeque<ProducerBatch>) {
        if !self.has_sequence() {
            return;
        }
        let mut sequence = self.base_sequence();
        let producer_id = self.producer_id();
        let producer_epoch = self.producer_epoch();
        let is_transactional = self.is_transactional();
        for new_batch in batches.iter_mut() {
            new_batch.set_producer_state(producer_id, producer_epoch, sequence, is_transactional);
            sequence += new_batch.record_count;
        }
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

    /// The built memory records for this batch.
    ///
    /// Translated from `ProducerBatch.records()` (`ProducerBatch.java:483-485`), which
    /// is a bare `recordsBuilder.build()` and is therefore **re-callable**: the send
    /// path calls it to serialise the produce request, and `split` calls it again (via
    /// `validateAndGetRecordBatch`) when the broker answers `MESSAGE_TOO_LARGE`.
    ///
    /// The returned [`MemoryRecords`] wraps a refcounted [`bytes::Bytes`], so this
    /// clones a handle, not the record bytes (CLAUDE.md §12).
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

    /// Takes ownership of the underlying buffer, leaving an empty Vec in its place.
    ///
    /// Used by [`RecordAccumulator::deallocate`] to return the actual batch buffer
    /// to the pool rather than allocating a new one.
    pub fn take_buffer(&mut self) -> Vec<u8> {
        self.records_builder.take_buffer()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::compress::Compression;
    use crate::common::protocol::Errors;

    const NOW: i64 = 1488748346917;

    fn make_tp() -> TopicPartition {
        TopicPartition::new("topic".to_string(), 1)
    }

    fn make_builder() -> MemoryRecordsBuilder {
        MemoryRecords::builder(512, Compression::none(), TimestampType::CreateTime, 128)
    }

    /// Translated from `ProducerBatchTest.testBatchAbort`.
    #[test]
    fn test_batch_abort() {
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        let future = batch
            .try_append(NOW, None, Some(&[0u8; 10]), &[], None, NOW)
            .unwrap_or_else(|_| panic!("Append should succeed"));

        let error = Error::with_message(Errors::UnknownServerError, "test abort");
        batch.abort(error);
        assert!(future.is_done());

        // subsequent completion should be ignored
        assert!(!batch.complete(500, 2342342341));
        assert!(batch.is_done());
    }

    /// Translated from `ProducerBatchTest.testBatchCannotAbortTwice`.
    #[test]
    #[should_panic(expected = "Batch has already been completed")]
    fn test_batch_cannot_abort_twice() {
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        batch
            .try_append(NOW, None, Some(&[0u8; 10]), &[], None, NOW)
            .unwrap_or_else(|_| panic!("Append should succeed"));

        let error = Error::with_message(Errors::UnknownServerError, "test abort");
        batch.abort(error);

        // This should panic
        let error2 = Error::with_message(Errors::UnknownServerError, "test abort 2");
        batch.abort(error2);
    }

    /// Translated from `ProducerBatchTest.testBatchCannotCompleteTwice`.
    ///
    /// Java: `assertThrows(IllegalStateException.class, () -> batch.complete(1000L, 20L))`
    /// Rust: panics because a Succeeded batch must not attempt another state change to Succeeded.
    #[test]
    fn test_batch_cannot_complete_twice() {
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        batch
            .try_append(NOW, None, Some(&[0u8; 10]), &[], None, NOW)
            .unwrap_or_else(|_| panic!("Append should succeed"));

        assert!(batch.complete(500, 10));

        // Second complete should panic (IllegalStateException in Java).
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            batch.complete(1000, 20);
        }));
        assert!(result.is_err(), "Second complete should panic");
    }

    /// `ProducerBatch.completeFutureAndFireCallbacks` puts its `try`/`catch`
    /// **inside** the per-record loop (`ProducerBatch.java:307-322`), so one bad user
    /// callback neither skips the remaining records nor stops
    /// `produceFuture.done()`:
    ///
    /// ```java
    /// } catch (Exception e) {
    ///     log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
    /// }
    /// ```
    ///
    /// `Callback` is infallible in Rust, so the only failure channel is a panic.
    /// Uncaught it escaped the loop, which meant (1) the later records' callbacks
    /// were skipped, (2) `produce_future.done()` never ran — so every
    /// `send().await` in the batch and any `flush()` hung forever — and (3)
    /// `self.thunks` was poisoned while its guard was held.
    #[tokio::test]
    async fn a_panicking_user_callback_does_not_abort_the_batch_completion() {
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);

        let first_ran = Arc::new(AtomicI32::new(0));
        let third_ran = Arc::new(AtomicI32::new(0));

        let first = Arc::clone(&first_ran);
        batch
            .try_append(
                NOW,
                None,
                Some(&[0u8; 10]),
                &[],
                Some(Box::new(move |_, _| {
                    first.fetch_add(1, Ordering::SeqCst);
                })),
                NOW,
            )
            .unwrap_or_else(|_| panic!("Append should succeed"));

        batch
            .try_append(
                NOW,
                None,
                Some(&[0u8; 10]),
                &[],
                Some(Box::new(|_, _| panic!("user callback blew up"))),
                NOW,
            )
            .unwrap_or_else(|_| panic!("Append should succeed"));

        let third = Arc::clone(&third_ran);
        batch
            .try_append(
                NOW,
                None,
                Some(&[0u8; 10]),
                &[],
                Some(Box::new(move |_, _| {
                    third.fetch_add(1, Ordering::SeqCst);
                })),
                NOW,
            )
            .unwrap_or_else(|_| panic!("Append should succeed"));

        // The panicking callback must not escape `complete`.
        assert!(batch.complete(500, 10), "the batch must complete");

        assert_eq!(
            first_ran.load(Ordering::SeqCst),
            1,
            "the callback before the panic must have run"
        );
        assert_eq!(
            third_ran.load(Ordering::SeqCst),
            1,
            "the callback after the panic must still run — Java's catch is inside the loop"
        );
        assert!(batch.is_done(), "the batch must be marked done");
        // `produce_future.done()` must have run: this would hang forever otherwise.
        tokio::time::timeout(std::time::Duration::from_secs(5), batch.produce_future.await_completion())
            .await
            .expect("produce_future.done() must run even when a user callback panics");
        // The thunks mutex must not be poisoned.
        assert!(batch.thunks.lock().is_ok(), "the thunks mutex must not be poisoned");
    }

    /// Translated from `ProducerBatchTest.testBatchExpiration`.
    #[test]
    fn test_batch_expiration() {
        let delivery_timeout_ms: i64 = 10240;
        let batch = ProducerBatch::new(make_tp(), make_builder(), NOW);

        // Set `now` to 2ms before the create time.
        assert!(!batch.has_reached_delivery_timeout(delivery_timeout_ms, NOW - 2));
        // Set `now` to deliveryTimeoutMs.
        assert!(batch.has_reached_delivery_timeout(delivery_timeout_ms, NOW + delivery_timeout_ms));
    }

    /// Translated from `ProducerBatchTest.testBatchExpirationAfterReenqueue`.
    #[test]
    fn test_batch_expiration_after_reenqueue() {
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        // Set batch.retry = true
        batch.reenqueued(NOW);
        // Set `now` to 2ms before the create time.
        assert!(!batch.has_reached_delivery_timeout(10240, NOW - 2));
    }

    /// Translated from `ProducerBatchTest.testShouldNotAttemptAppendOnceRecordsBuilderIsClosedForAppends`.
    #[test]
    fn test_should_not_attempt_append_once_records_builder_is_closed_for_appends() {
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        let result0 = batch.try_append(NOW, None, Some(&[0u8; 10]), &[], None, NOW);
        assert!(result0.is_ok());

        batch.close_for_record_appends();

        // After closing for record appends, try_append should return Err (no room).
        let result1 = batch.try_append(NOW + 1, None, Some(&[0u8; 10]), &[], None, NOW + 1);
        assert!(result1.is_err());
    }

    /// Translated from `ProducerBatchTest.testSplitPreservesHeaders`.
    ///
    /// Only tests with NONE compression since we only support NONE currently
    /// in record-level iteration.
    #[test]
    fn test_split_preserves_headers() {
        let builder = MemoryRecords::builder_with_buffer(
            vec![0u8; 1024],
            RecordBatch::CURRENT_MAGIC_VALUE,
            Compression::none(),
            TimestampType::CreateTime,
            0,
        );
        let mut batch = ProducerBatch::new(make_tp(), builder, NOW);

        let header = RecordHeader::new("header-key".to_string(), Some(b"header-value".to_vec()));

        let mut count = 0;
        loop {
            let result = batch.try_append(NOW, Some(b"hi"), Some(b"there"), std::slice::from_ref(&header), None, NOW);
            if result.is_err() {
                break;
            }
            count += 1;
        }
        assert!(count > 1, "Should have appended multiple records");

        let batches = batch.split(200);
        assert!(batches.len() >= 2, "This batch should be split to multiple small batches.");

        for mut split_batch in batches {
            let records = split_batch.records();
            for record_batch in records.batches() {
                use crate::common::record::internal::Record;
                for record in record_batch.iter_records().unwrap() {
                    let hdrs = record.headers();
                    assert_eq!(1, hdrs.len(), "Header size should be 1.");
                    assert_eq!("header-key", hdrs[0].key(), "Header key should be 'header-key'.");
                    assert_eq!(
                        b"header-value",
                        hdrs[0].value().unwrap(),
                        "Header value should be 'header-value'."
                    );
                }
            }
        }
    }

    /// Translated from `ProducerBatchTest.testWithLeaderChangesAcrossRetries`.
    #[test]
    fn test_with_leader_changes_across_retries() {
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);

        // Starting state for the batch, no attempt made to send it yet.
        assert_eq!(None, batch.current_leader_epoch());
        assert_eq!(0, batch.attempts_when_leader_last_changed());
        batch.maybe_update_leader_epoch(None);
        assert!(!batch.has_leader_changed_for_the_ongoing_retry());

        // 1st attempt [Not a retry] to send the batch.
        let mut batch_leader_epoch = 100;
        batch.maybe_update_leader_epoch(Some(batch_leader_epoch));
        assert!(
            !batch.has_leader_changed_for_the_ongoing_retry(),
            "batch leader is assigned for 1st time"
        );
        assert_eq!(Some(batch_leader_epoch), batch.current_leader_epoch());
        assert_eq!(0, batch.attempts_when_leader_last_changed());

        // 2nd attempt [1st retry] to send the batch to a new leader.
        batch_leader_epoch = 101;
        batch.reenqueued(0);
        batch.maybe_update_leader_epoch(Some(batch_leader_epoch));
        assert!(batch.has_leader_changed_for_the_ongoing_retry(), "batch leader has changed");
        assert_eq!(Some(batch_leader_epoch), batch.current_leader_epoch());
        assert_eq!(1, batch.attempts_when_leader_last_changed());

        // 2nd attempt [1st retry] still ongoing, yet to be made.
        batch.maybe_update_leader_epoch(Some(batch_leader_epoch));
        assert!(batch.has_leader_changed_for_the_ongoing_retry(), "batch leader has changed");
        assert_eq!(Some(batch_leader_epoch), batch.current_leader_epoch());
        assert_eq!(1, batch.attempts_when_leader_last_changed());

        // 3rd attempt [2nd retry] to the same leader-epoch(101).
        batch.reenqueued(0);
        batch.maybe_update_leader_epoch(Some(batch_leader_epoch));
        assert!(
            !batch.has_leader_changed_for_the_ongoing_retry(),
            "batch leader has not changed"
        );
        assert_eq!(Some(batch_leader_epoch), batch.current_leader_epoch());
        assert_eq!(1, batch.attempts_when_leader_last_changed());

        // Attempt made to update batch leader-epoch to an older leader-epoch(100).
        batch.maybe_update_leader_epoch(Some(batch_leader_epoch - 1));
        assert!(
            !batch.has_leader_changed_for_the_ongoing_retry(),
            "batch leader has not changed"
        );
        assert_eq!(Some(batch_leader_epoch), batch.current_leader_epoch());
        assert_eq!(1, batch.attempts_when_leader_last_changed());

        // Attempt made to update batch leader-epoch to an unknown leader(None).
        batch.maybe_update_leader_epoch(None);
        assert!(
            !batch.has_leader_changed_for_the_ongoing_retry(),
            "batch leader has not changed"
        );
        assert_eq!(Some(batch_leader_epoch), batch.current_leader_epoch());
        assert_eq!(1, batch.attempts_when_leader_last_changed());
    }

    /// Translated from `ProducerBatchTest.testCompleteExceptionallyWithRecordErrors`.
    #[test]
    fn test_complete_with_error_and_record_errors() {
        let record_count = 5;
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);

        let mut futures = Vec::new();
        for _ in 0..record_count {
            let future = batch
                .try_append(NOW, None, Some(&[0u8; 10]), &[], None, NOW)
                .unwrap_or_else(|_| panic!("Append should succeed"));
            futures.push(future);
        }
        assert_eq!(record_count, batch.record_count);

        // Create per-record exceptions for records 0 and 3.
        let record_errors: Arc<dyn Fn(i32) -> Option<Error> + Send + Sync> = Arc::new(|idx: i32| -> Option<Error> {
            match idx {
                0 | 3 => Some(Error::with_message(Errors::UnknownServerError, format!("record error {}", idx))),
                _ => Some(Error::with_message(Errors::UnknownServerError, "top level")),
            }
        });

        let top_level_error = Error::with_message(Errors::UnknownServerError, "top level");
        batch.complete_with_error(top_level_error, record_errors);
        assert!(batch.is_done());

        for future in &futures {
            assert!(future.is_done());
        }
    }

    /// Basic test: try_append succeeds and returns a FutureRecordMetadata.
    #[test]
    fn test_try_append_basic() {
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        let future = batch.try_append(NOW, Some(b"key"), Some(b"value"), &[], None, NOW);
        assert!(future.is_ok(), "First append should succeed");
        assert_eq!(1, batch.record_count);

        let future2 = batch.try_append(NOW, Some(b"key2"), Some(b"value2"), &[], None, NOW);
        assert!(future2.is_ok(), "Second append should succeed");
        assert_eq!(2, batch.record_count);
    }

    /// Test that estimated_size_in_bytes increases as records are appended.
    #[test]
    fn test_estimated_size_increases() {
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        let initial_size = batch.estimated_size_in_bytes();
        let _ = batch.try_append(NOW, Some(b"key"), Some(b"value"), &[], None, NOW);
        let after_first = batch.estimated_size_in_bytes();
        assert!(after_first > initial_size, "Size should increase after appending a record");
    }

    /// Test close and is_closed.
    #[test]
    fn test_close_and_is_closed() {
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        assert!(!batch.is_closed());
        let _ = batch.try_append(NOW, Some(b"key"), Some(b"value"), &[], None, NOW);
        batch.close();
        assert!(batch.is_closed());
    }

    /// Test reenqueue increments attempts.
    #[test]
    fn test_reenqueue_increments_attempts() {
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        assert_eq!(0, batch.attempts());
        batch.reenqueued(NOW);
        assert_eq!(1, batch.attempts());
        assert!(batch.in_retry());
        batch.reenqueued(NOW + 10);
        assert_eq!(2, batch.attempts());
    }

    /// Test is_split_batch default and explicit.
    #[test]
    fn test_is_split_batch() {
        let batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        assert!(!batch.is_split_batch());

        let builder2 = make_builder();
        let batch2 = ProducerBatch::new_with_split(make_tp(), builder2, NOW, true);
        assert!(batch2.is_split_batch());
    }

    /// Test magic returns current magic value.
    #[test]
    fn test_magic() {
        let batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        assert_eq!(RecordBatch::CURRENT_MAGIC_VALUE, batch.magic());
    }

    /// Translated from `ProducerBatchTest.testSplitPreservesMagicAndCompressionType`.
    ///
    /// Tests that split batches preserve the magic version and compression type from the
    /// original batch. Only tests magic V2 + NONE compression since our MemoryRecordsBuilder
    /// only supports magic V2 and record-level iteration for NONE compression.
    #[test]
    fn test_split_preserves_magic_and_compression_type() {
        // We only support magic V2 and NONE compression for record-level iteration.
        let magic = RecordBatch::CURRENT_MAGIC_VALUE;
        let builder = MemoryRecords::builder_with_buffer(
            vec![0u8; 1024],
            magic,
            Compression::none(),
            TimestampType::CreateTime,
            0,
        );
        let mut batch = ProducerBatch::new(make_tp(), builder, NOW);

        loop {
            let result = batch.try_append(NOW, Some(b"hi"), Some(b"there"), &[], None, NOW);
            if result.is_err() {
                break;
            }
        }

        let batches = batch.split(512);
        assert!(batches.len() >= 2, "Batch should split into multiple sub-batches");

        for mut split_batch in batches {
            assert_eq!(magic, split_batch.magic(), "Split batch magic should match original");
            assert!(split_batch.is_split_batch(), "Split batch should be marked as split");

            let records = split_batch.records();
            for record_batch in records.batches() {
                assert_eq!(magic, record_batch.magic(), "Record batch magic should match original");
                assert_eq!(0, record_batch.base_offset(), "Base offset should be 0");
                assert_eq!(
                    CompressionType::None,
                    record_batch.compression_type(),
                    "Compression type should match"
                );
            }
        }
    }

    /// A batch is splittable **after** the send path has already read its records.
    ///
    /// No Java counterpart, because Java cannot get this wrong:
    /// `ProducerBatch.records()` is a bare `recordsBuilder.build()`
    /// (`ProducerBatch.java:483-485`) and Java's `build()` memoises, so it is
    /// re-callable by construction. Rust's used to hand its only copy away, and
    /// `Sender.completeBatch`'s `MESSAGE_TOO_LARGE` arm (`Sender.java:674-688`) is
    /// exactly a `split()` after a `records()` — the batch cannot receive that error
    /// before being sent. PLAN §9.18; this is the unit-level shape of the end-to-end
    /// reproducer `sender.rs::test_too_large_batches_are_safely_removed`.
    #[test]
    fn test_records_readable_after_send_then_split() {
        let magic = RecordBatch::CURRENT_MAGIC_VALUE;
        let builder = MemoryRecords::builder_with_buffer(
            vec![0u8; 1024],
            magic,
            Compression::none(),
            TimestampType::CreateTime,
            0,
        );
        let mut batch = ProducerBatch::new(make_tp(), builder, NOW);

        let mut appended = 0;
        loop {
            if batch.try_append(NOW, Some(b"hi"), Some(b"there"), &[], None, NOW).is_err() {
                break;
            }
            appended += 1;
        }
        assert!(appended > 1, "the batch must hold more than one record to be splittable");

        // What `Sender::send_producer_data` does to build the produce request.
        let sent = batch.records();
        assert!(sent.size_in_bytes() > 0);

        // What `RecordAccumulator::split_and_reenqueue` does when the broker answers
        // MESSAGE_TOO_LARGE. Before the §9.18 fix this panicked with
        // "build() called but no records built".
        let reread = batch.records();
        assert_eq!(sent.buffer(), reread.buffer(), "the second read must see the same bytes");

        let sub_batches = batch.split(512);
        assert!(sub_batches.len() >= 2, "batch should split into multiple sub-batches");
        let split_record_count: i32 = sub_batches.iter().map(|b| b.record_count).sum();
        assert_eq!(split_record_count, appended, "no record may be lost by the split");
    }

    /// `definition-of-done.md` §10 / CLAUDE.md §11-12 for
    /// [`ProducerBatch::records`], which `Sender::send_producer_data` calls once per
    /// drained batch.
    ///
    /// Two properties, both measured with the same
    /// [`crate::test_alloc_tracker::AllocTrackingGuard`] the consumer's §27 budget
    /// tests use:
    ///
    ///   1. **Nothing scales with the record count.** A 1-record batch and a
    ///      64-record batch must cost the same, so the per-record component is
    ///      exactly zero — this is the property DoD §10 is about.
    ///   2. **Re-reading is free and copies nothing.** Every call after the first
    ///      allocates zero and hands back the *same* bytes, at the same address. That
    ///      is what keeps the `MESSAGE_TOO_LARGE` split off the copy path
    ///      (CLAUDE.md §12) now that it re-reads the batch instead of panicking.
    ///
    /// Absolute counts on this build: the first call costs 1 allocation — `bytes`
    /// promotes a `Bytes::from(Vec)` to a shared representation on its first clone, a
    /// single 3-word `Shared` — and every later call costs 0. Before the §9.18 fix the
    /// first call cost 0 (it moved the value out) and the second cost 1, but that one
    /// was a **full re-copy of the batch** through `take_batch_data`, so the fix trades
    /// a per-batch 24-byte allocation for removing a whole-batch `memcpy` from the
    /// split path. Neither is per record.
    #[test]
    fn test_records_allocations_do_not_scale_with_the_record_count() {
        fn records_allocations(record_count: usize) -> usize {
            let builder = MemoryRecords::builder_with_buffer(
                vec![0u8; 4096],
                RecordBatch::CURRENT_MAGIC_VALUE,
                Compression::none(),
                TimestampType::CreateTime,
                0,
            );
            let mut batch = ProducerBatch::new(make_tp(), builder, NOW);
            for _ in 0..record_count {
                assert!(
                    batch.try_append(NOW, Some(b"hi"), Some(b"there"), &[], None, NOW).is_ok(),
                    "the buffer is sized to hold every record"
                );
            }
            // `RecordAccumulator::drain` closes the batch before the Sender reads it.
            batch.close();

            let _guard = crate::test_alloc_tracker::AllocTrackingGuard::new();
            crate::test_alloc_tracker::AllocTrackingGuard::reset();
            let first = batch.records();
            let first_count = crate::test_alloc_tracker::AllocTrackingGuard::count();

            crate::test_alloc_tracker::AllocTrackingGuard::reset();
            let second = batch.records();
            let second_count = crate::test_alloc_tracker::AllocTrackingGuard::count();
            let third = batch.records();
            let third_count = crate::test_alloc_tracker::AllocTrackingGuard::count();

            assert_eq!(second_count, 0, "re-reading a batch must not allocate");
            assert_eq!(third_count, 0, "re-reading a batch must not allocate");
            assert_eq!(
                first.buffer().as_ptr(),
                second.buffer().as_ptr(),
                "re-reading a batch must not copy it"
            );
            assert_eq!(first.buffer().as_ptr(), third.buffer().as_ptr());
            assert!(first_count > 0, "the tracker must actually be measuring");
            first_count
        }

        let one_record = records_allocations(1);
        let sixty_four_records = records_allocations(64);
        assert_eq!(
            one_record, sixty_four_records,
            "reading a 64-record batch must allocate exactly as much as a 1-record batch; \
             got {one_record} vs {sixty_four_records}"
        );
    }

    /// Translated from `ProducerBatchTest.testCompleteExceptionallyWithNullRecordErrors`.
    ///
    /// In Java, passing `null` for the `recordExceptions` function to `completeExceptionally`
    /// results in a `NullPointerException` when the code tries to call `recordExceptions.apply(i)`.
    /// In Rust, `complete_with_error` takes a non-optional `Arc<dyn Fn(...)>`, so passing
    /// "null" is not possible at the type level. This test verifies that the function is invoked
    /// correctly by providing a function that returns `None` for all indices (the closest Rust
    /// analog of a "null" result from the function).
    #[test]
    fn test_complete_with_error_and_none_returning_error_fn() {
        let record_count = 5;
        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);

        let mut futures = Vec::new();
        for _ in 0..record_count {
            let future = batch
                .try_append(NOW, None, Some(&[0u8; 10]), &[], None, NOW)
                .unwrap_or_else(|_| panic!("Append should succeed"));
            futures.push(future);
        }
        assert_eq!(record_count, batch.record_count);

        // A function that returns None for all indices (closest to Java null behavior).
        let record_errors: Arc<dyn Fn(i32) -> Option<Error> + Send + Sync> = Arc::new(|_idx| None);

        let top_level_error = Error::with_message(Errors::UnknownServerError, "top level");
        batch.complete_with_error(top_level_error, record_errors);
        assert!(batch.is_done());

        for future in &futures {
            assert!(future.is_done());
        }
    }

    /// Translated from `ProducerBatchTest.testBatchAbort` - extended version with callback
    /// verification.
    ///
    /// Verifies that callbacks are invoked exactly once when a batch is aborted.
    #[test]
    fn test_batch_abort_with_callback() {
        use std::sync::atomic::{AtomicI32, Ordering};

        let invocations = Arc::new(AtomicI32::new(0));
        let got_error = Arc::new(Mutex::new(false));
        let got_metadata = Arc::new(Mutex::new(false));

        let inv = Arc::clone(&invocations);
        let err_flag = Arc::clone(&got_error);
        let meta_flag = Arc::clone(&got_metadata);

        let callback: Callback = Box::new(move |metadata, error| {
            inv.fetch_add(1, Ordering::SeqCst);
            *err_flag.lock().unwrap() = error.is_some();
            *meta_flag.lock().unwrap() = metadata.is_some();
        });

        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        let future = batch
            .try_append(NOW, None, Some(&[0u8; 10]), &[], Some(callback), NOW)
            .unwrap_or_else(|_| panic!("Append should succeed"));

        let error = Error::with_message(Errors::UnknownServerError, "test abort");
        batch.abort(error);
        assert!(future.is_done());
        assert_eq!(1, invocations.load(Ordering::SeqCst));
        assert!(*got_error.lock().unwrap(), "Callback should receive error");
        assert!(!*got_metadata.lock().unwrap(), "Callback should not receive metadata on abort");

        // subsequent completion should be ignored
        assert!(!batch.complete(500, 2342342341));
        assert_eq!(1, invocations.load(Ordering::SeqCst), "Callback should not be invoked again");
    }

    /// Translated from `ProducerBatchTest.testBatchCannotCompleteTwice` - extended version
    /// with callback verification.
    ///
    /// Verifies that callbacks are invoked exactly once when a batch completes successfully.
    #[test]
    fn test_batch_complete_with_callback() {
        use std::sync::atomic::{AtomicI32, Ordering};

        let invocations = Arc::new(AtomicI32::new(0));
        let got_error = Arc::new(Mutex::new(false));
        let got_metadata = Arc::new(Mutex::new(false));

        let inv = Arc::clone(&invocations);
        let err_flag = Arc::clone(&got_error);
        let meta_flag = Arc::clone(&got_metadata);

        let callback: Callback = Box::new(move |metadata, error| {
            inv.fetch_add(1, Ordering::SeqCst);
            *err_flag.lock().unwrap() = error.is_some();
            *meta_flag.lock().unwrap() = metadata.is_some();
        });

        let mut batch = ProducerBatch::new(make_tp(), make_builder(), NOW);
        batch
            .try_append(NOW, None, Some(&[0u8; 10]), &[], Some(callback), NOW)
            .unwrap_or_else(|_| panic!("Append should succeed"));

        assert!(batch.complete(500, 10));
        assert_eq!(1, invocations.load(Ordering::SeqCst));
        assert!(!*got_error.lock().unwrap(), "Callback should not receive error on success");
        assert!(*got_metadata.lock().unwrap(), "Callback should receive metadata on success");
    }
}
