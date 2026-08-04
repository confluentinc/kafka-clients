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

// Staged ahead of its callers: `TransactionManager` (Phase 3/5) and the send
// path (Phase 4) are the only consumers, so under `#![deny(warnings)]` these
// methods are dead code until then. Same mechanism as `network_client.rs:15`.
#![allow(dead_code)]

//! Per-partition idempotence/transaction bookkeeping.

use std::collections::{BTreeSet, HashMap};

use crate::common::KafkaError;
use crate::common::TopicPartition;
use crate::common::record::default_record_batch::increment_sequence;
use crate::common::requests::produce_response::INVALID_OFFSET;
use crate::common::utils::ProducerIdAndEpoch;
use crate::producer::internals::ProducerBatch;

/// Ordering key for an in-flight batch: `(producer_id, producer_epoch,
/// base_sequence)`.
///
/// The tuple's derived `Ord` is lexicographic, which is exactly Java's
/// comparator chain
/// `comparingLong(producerId).thenComparingInt(producerEpoch).thenComparingInt(baseSequence)`
/// (`TxnPartitionEntry.java:62-65`).
pub(crate) type InFlightBatchKey = (i64, i16, i32);

/// Idempotence and transaction state for a single topic-partition.
///
/// Translated from
/// `org.apache.kafka.clients.producer.internals.TxnPartitionEntry`.
///
/// # Deviation: this type does not own `ProducerBatch`
///
/// Java's `inflightBatchesBySequence` is a `TreeSet<ProducerBatch>` holding
/// **references** to batches that the accumulator's deque and the Sender's
/// in-flight map also reference. Rust has no second owner available:
/// `RecordAccumulator` stores batches by value, `drain()` moves them out, and
/// `Sender::in_flight_batches` takes ownership. `ProducerBatch` is not `Clone`.
///
/// Ownership **alternates**: on the retry path it moves back to the accumulator's
/// deque while the batch stays tracked here, so a tracked batch lives in either
/// owner. Callers assembling the batch pool must draw from both — see
/// [`Self::reset_sequence_numbers`] and rules §7.
///
/// So this type tracks in-flight **ordering keys**
/// ([`InFlightBatchKey`]), and the two methods that Java implements by mutating
/// the contained batches — [`Self::start_sequences_at_beginning`] and
/// [`Self::adjust_sequences_due_to_failed_batch`] — receive the batches from
/// their owner instead.
///
/// See `.claude/rules/producer-transactions.md` §7 for the full rationale,
/// including why `Arc<Mutex<ProducerBatch>>` was rejected (it would put a
/// per-batch lock on the drain path, a hot path per CLAUDE.md §11).
///
/// No interior `Mutex`: Java relies on the caller holding the
/// `TransactionManager` monitor, and Phase 3 wraps the whole manager. A lock
/// here would nest for nothing.
#[derive(Debug)]
pub(crate) struct TxnPartitionEntry {
    topic_partition: TopicPartition,

    /// The producer id/epoch being used for this partition.
    producer_id_and_epoch: ProducerIdAndEpoch,

    /// The base sequence of the next batch bound for this partition.
    next_sequence: i32,

    /// The sequence number of the last record of the last ack'd batch. When
    /// there are no in-flight requests for a partition,
    /// `last_acked_sequence == next_sequence - 1`.
    last_acked_sequence: i32,

    // `inflight_batches_by_sequence` should only have batches with the same
    // producer id and producer epoch, but there is an edge case where we may
    // remove the wrong batch if the comparator only takes `baseSequence` into
    // account.
    // See https://github.com/apache/kafka/pull/12096#pullrequestreview-955554191
    // for details.
    /// In-flight batch keys, ordered by sequence. This keeps batches ordered by
    /// sequence number even when responses come back out of order during leader
    /// failover. A key is added when the batch is drained and removed when the
    /// batch completes (successfully or through a fatal failure).
    inflight_batches_by_sequence: BTreeSet<InFlightBatchKey>,

    /// The last acknowledged offset, tracked per partition to disambiguate
    /// `UnknownProducer` responses caused by the retention period elapsing from
    /// those caused by actual lost data.
    last_acked_offset: i64,
}

impl TxnPartitionEntry {
    /// Sentinel for "no batch has been acknowledged yet".
    pub(crate) const NO_LAST_ACKED_SEQUENCE_NUMBER: i32 = -1;

    /// Creates an entry for `topic_partition` with no producer state.
    pub(crate) fn new(topic_partition: TopicPartition) -> Self {
        Self {
            topic_partition,
            producer_id_and_epoch: ProducerIdAndEpoch::NONE,
            next_sequence: 0,
            last_acked_sequence: Self::NO_LAST_ACKED_SEQUENCE_NUMBER,
            last_acked_offset: INVALID_OFFSET,
            inflight_batches_by_sequence: BTreeSet::new(),
        }
    }

    /// The ordering key for a batch.
    fn batch_key(batch: &ProducerBatch) -> InFlightBatchKey {
        (batch.producer_id(), batch.producer_epoch(), batch.base_sequence())
    }

    /// The producer id/epoch in use for this partition.
    pub(crate) fn producer_id_and_epoch(&self) -> ProducerIdAndEpoch {
        self.producer_id_and_epoch
    }

    /// The base sequence of the next batch bound for this partition.
    pub(crate) fn next_sequence(&self) -> i32 {
        self.next_sequence
    }

    /// The last acknowledged offset, or `None` if none has been acknowledged.
    pub(crate) fn last_acked_offset(&self) -> Option<i64> {
        if self.last_acked_offset != INVALID_OFFSET {
            Some(self.last_acked_offset)
        } else {
            None
        }
    }

    /// The last acknowledged sequence, or `None` if none has been acknowledged.
    pub(crate) fn last_acked_sequence(&self) -> Option<i32> {
        if self.last_acked_sequence != Self::NO_LAST_ACKED_SEQUENCE_NUMBER {
            Some(self.last_acked_sequence)
        } else {
            None
        }
    }

    /// Whether any batches are in flight for this partition.
    pub(crate) fn has_inflight_batches(&self) -> bool {
        !self.inflight_batches_by_sequence.is_empty()
    }

    /// The key of the lowest-sequence in-flight batch, or `None` if there are
    /// none.
    ///
    /// Java returns the `ProducerBatch` itself (`TreeSet::first`); this returns
    /// the ordering key, and the caller — which owns the batches — resolves it.
    /// See the type-level deviation note.
    pub(crate) fn next_batch_by_sequence(&self) -> Option<InFlightBatchKey> {
        self.inflight_batches_by_sequence.first().copied()
    }

    /// Advances the next sequence by `increment`, wrapping at `i32::MAX`.
    ///
    /// Delegates to the shared wrapping helper, as Java delegates to
    /// `DefaultRecordBatch.incrementSequence`.
    pub(crate) fn increment_sequence(&mut self, increment: i32) {
        self.next_sequence = increment_sequence(self.next_sequence, increment);
    }

    /// Records `batch` as in flight for this partition.
    ///
    /// The batch is borrowed to derive its key, never stored.
    pub(crate) fn add_inflight_batch(&mut self, batch: &ProducerBatch) {
        self.inflight_batches_by_sequence.insert(Self::batch_key(batch));
    }

    /// Sets the last acknowledged offset.
    pub(crate) fn set_last_acked_offset(&mut self, last_acked_offset: i64) {
        self.last_acked_offset = last_acked_offset;
    }

    /// Removes `batch` from the in-flight set.
    ///
    /// The batch is borrowed to derive its key, never stored.
    pub(crate) fn remove_in_flight_batch(&mut self, batch: &ProducerBatch) {
        self.inflight_batches_by_sequence.remove(&Self::batch_key(batch));
    }

    /// Rewrites every in-flight batch to start at sequence 0 under
    /// `new_producer_id_and_epoch`, then adopts that producer state.
    ///
    /// `batches` must be the in-flight batches for this partition, supplied by
    /// their owner (see the type-level deviation note). They are visited in
    /// key order, matching Java's `TreeSet` iteration, because each batch's new
    /// base sequence depends on the record counts of the batches before it.
    pub(crate) fn start_sequences_at_beginning(
        &mut self,
        new_producer_id_and_epoch: ProducerIdAndEpoch,
        batches: &mut [&mut ProducerBatch],
    ) -> Result<(), KafkaError> {
        let mut sequence = 0;
        // Fallible: `reset_sequence_numbers` errors if `batches` is missing a
        // batch this entry tracks. Propagated rather than discarded, because
        // `next_sequence` below is derived from the batches actually visited —
        // swallowing the error would silently rewind the sequence counter.
        self.reset_sequence_numbers(batches, |batch| {
            batch.reset_producer_state(
                new_producer_id_and_epoch.producer_id,
                new_producer_id_and_epoch.epoch,
                sequence,
            );
            sequence += batch.record_count;
            Ok(())
        })?;
        self.producer_id_and_epoch = new_producer_id_and_epoch;
        self.next_sequence = sequence;
        self.last_acked_sequence = Self::NO_LAST_ACKED_SEQUENCE_NUMBER;
        Ok(())
    }

    /// Raises the last acknowledged sequence to `sequence` if it is higher,
    /// returning the resulting value.
    pub(crate) fn maybe_update_last_acked_sequence(&mut self, sequence: i32) -> i32 {
        if sequence > self.last_acked_sequence {
            self.last_acked_sequence = sequence;
            return sequence;
        }
        self.last_acked_sequence
    }

    /// Shifts sequence numbers down by `record_count` after a batch was failed
    /// fatally by the broker, so subsequent batches do not fail with
    /// `OutOfOrderSequenceException`.
    ///
    /// Only batches at or after `base_sequence` are shifted. `batches` must be
    /// the in-flight batches for this partition, supplied by their owner.
    ///
    /// `base_sequence` is `i64` to match Java's `long` parameter, even though it
    /// is compared against an `i32` base sequence.
    pub(crate) fn adjust_sequences_due_to_failed_batch(
        &mut self,
        base_sequence: i64,
        record_count: i32,
        batches: &mut [&mut ProducerBatch],
    ) -> Result<(), KafkaError> {
        self.decrement_sequence(record_count)?;
        let topic_partition = self.topic_partition.clone();
        self.reset_sequence_numbers(batches, |batch| {
            if i64::from(batch.base_sequence()) < base_sequence {
                return Ok(());
            }

            let new_sequence = batch.base_sequence() - record_count;
            if new_sequence < 0 {
                // Java throws IllegalStateException; per CLAUDE.md §10.2 this
                // is a Result, not a panic. Message text preserved.
                return Err(KafkaError::illegal_state(format!(
                    "Sequence number for batch with sequence {} for partition {} is going to become negative: {}",
                    batch.base_sequence(),
                    topic_partition,
                    new_sequence
                )));
            }

            batch.reset_producer_state(batch.producer_id(), batch.producer_epoch(), new_sequence);
            Ok(())
        })
    }

    /// Applies `reset` to every **tracked** in-flight batch in key order, then
    /// rebuilds the key set from the mutated batches.
    ///
    /// Java (154-161) iterates **its own** `inflightBatchesBySequence` and
    /// re-adds exactly those elements to a fresh `TreeSet`: membership is
    /// invariant, only the sort keys change. This preserves that invariant.
    ///
    /// `batches` is a lookup **pool** supplied by the owner (see the type-level
    /// deviation note), not the source of membership:
    ///
    ///   - a batch in `batches` that this entry does not track is **ignored**;
    ///   - a tracked key with no matching batch in `batches` is an **error**.
    ///
    /// Driving membership from `batches` instead would let a caller silently
    /// shrink or grow the tracked set. Because
    /// [`Self::start_sequences_at_beginning`] derives `next_sequence` from the
    /// batches it visits, a short slice would silently rewind the partition's
    /// sequence counter — the exact corruption this type exists to prevent, and
    /// one that surfaces only later as a broker-side
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER`.
    ///
    /// Both directions of mismatch are real rather than defensive:
    ///
    /// **Pool ⊃ tracked** — `Sender.failBatch` calls `handleFailedBatch`
    /// (`Sender.java:848`) *before* `maybeRemoveAndDeallocateBatch` (`:854`),
    /// while `handleFailedBatch` removes the batch from the txn map at
    /// `TransactionManager.java:790` and only then calls
    /// `adjustSequencesDueToFailedBatch` at `:818`. So the failed batch is
    /// already untracked here but still owned by the Sender.
    ///
    /// **Tracked ⊃ one owner** — on the retry path the batch moves owners while
    /// *staying* tracked. `Sender.reenqueueBatch` (`Sender.java:750-752`) calls
    /// `accumulator.reenqueue(..)` and then `maybeRemoveFromInflightBatches(..)`,
    /// but — unlike the `MESSAGE_TOO_LARGE` split path at `:685` — it does **not**
    /// call `transactionManager.removeInFlightBatch`. Java relies on this and
    /// asserts it: `RecordAccumulator.insertInSequenceOrder`
    /// (`RecordAccumulator.java:558-560`) throws
    /// `IllegalStateException("We are re-enqueueing a batch which is not tracked
    /// as part of the in flight requests")` when the batch is *not* still
    /// tracked.
    ///
    /// So the caller MUST supply every tracked batch **wherever it currently
    /// lives** — `Sender::in_flight_batches` *and* the accumulator's deque. A
    /// reenqueued batch sits in the latter, and
    /// `bump_idempotent_producer_epoch` → `start_sequences_at_beginning`
    /// (`TransactionManager.java:655`) exists precisely to rewrite it
    /// (`:652-653`). Supplying only the Sender's map there would hit the error
    /// below and break idempotent recovery.
    fn reset_sequence_numbers<F>(&mut self, batches: &mut [&mut ProducerBatch], mut reset: F) -> Result<(), KafkaError>
    where
        F: FnMut(&mut ProducerBatch) -> Result<(), KafkaError>,
    {
        // Index the pool by key. Stored as indices rather than `&mut` references
        // so the borrow checker permits handing out one mutable batch at a time.
        let mut pool: HashMap<InFlightBatchKey, usize> = HashMap::with_capacity(batches.len());
        for (index, batch) in batches.iter().enumerate() {
            pool.insert(Self::batch_key(batch), index);
        }

        // Iterate the tracked keys in order — Java's `TreeSet` iteration order.
        let tracked: Vec<InFlightBatchKey> = self.inflight_batches_by_sequence.iter().copied().collect();

        // Resolve every tracked key BEFORE mutating anything, so a missing batch
        // leaves the caller's batches untouched. Without this pre-pass the loop
        // rewrites the batches it reaches and then errors on a later missing key,
        // leaving the caller with a half-rewritten set and no way to tell how far
        // it got. Java has no equivalent error — the missing-batch check is a Rust
        // safety net (see the doc comment) — so there is no Java behaviour to
        // match here, and atomic is the only defensible choice.
        //
        // Note the *other* error path, `adjust_sequences_due_to_failed_batch`'s
        // negative-sequence rejection, deliberately keeps Java's non-atomic
        // behaviour: Java's lambda throws mid-iteration and leaves earlier
        // elements mutated, so matching it is the faithful translation.
        let mut resolved = Vec::with_capacity(tracked.len());
        for key in &tracked {
            let Some(&index) = pool.get(key) else {
                return Err(KafkaError::illegal_state(format!(
                    "No in-flight batch supplied for tracked sequence {:?} on partition {}; \
                     the caller must supply every batch this entry tracks",
                    key, self.topic_partition
                )));
            };
            resolved.push(index);
        }

        let mut new_inflights = BTreeSet::new();
        for index in resolved {
            let batch = &mut *batches[index];
            reset(batch)?;
            new_inflights.insert(Self::batch_key(batch));
        }
        self.inflight_batches_by_sequence = new_inflights;
        Ok(())
    }

    /// Reduces the next sequence by `decrement`.
    ///
    /// Plain subtraction with an error on underflow — this deliberately does
    /// **not** use the wrapping `decrement_sequence` helper, because Java's
    /// private `decrementSequence` (163-173) does plain subtraction and throws
    /// `IllegalStateException`. Only `incrementSequence` delegates to
    /// `DefaultRecordBatch`. Wrapping here would silently produce a large
    /// positive sequence instead of an error. See
    /// `.claude/rules/producer-transactions.md` §8.
    fn decrement_sequence(&mut self, decrement: i32) -> Result<(), KafkaError> {
        let updated_sequence = self.next_sequence - decrement;
        if updated_sequence < 0 {
            return Err(KafkaError::illegal_state(format!(
                "Sequence number for partition {} is going to become negative: {}",
                self.topic_partition, updated_sequence
            )));
        }
        self.next_sequence = updated_sequence;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::compress::Compression;
    use crate::common::record::TimestampType;
    use crate::common::record::memory_records::MemoryRecords;

    fn tp() -> TopicPartition {
        TopicPartition::new("topic".to_string(), 0)
    }

    /// Builds a batch with `record_count` records and the given producer state.
    fn batch(producer_id: i64, epoch: i16, base_sequence: i32, record_count: i32) -> ProducerBatch {
        let builder = MemoryRecords::builder(512, Compression::none(), TimestampType::CreateTime, 128);
        let mut b = ProducerBatch::new(tp(), builder, 0);
        b.record_count = record_count;
        b.set_producer_state(producer_id, epoch, base_sequence, false);
        b
    }

    #[test]
    fn test_new_defaults_match_java() {
        let entry = TxnPartitionEntry::new(tp());
        assert_eq!(entry.producer_id_and_epoch(), ProducerIdAndEpoch::NONE);
        assert_eq!(entry.next_sequence(), 0);
        assert_eq!(entry.last_acked_sequence(), None);
        assert_eq!(entry.last_acked_offset(), None);
        assert!(!entry.has_inflight_batches());
        assert_eq!(entry.next_batch_by_sequence(), None);
    }

    #[test]
    fn test_last_acked_offset_sentinel_maps_to_none() {
        let mut entry = TxnPartitionEntry::new(tp());
        assert_eq!(entry.last_acked_offset(), None);
        entry.set_last_acked_offset(INVALID_OFFSET);
        assert_eq!(entry.last_acked_offset(), None);
        entry.set_last_acked_offset(0);
        assert_eq!(entry.last_acked_offset(), Some(0));
        entry.set_last_acked_offset(42);
        assert_eq!(entry.last_acked_offset(), Some(42));
    }

    #[test]
    fn test_maybe_update_last_acked_sequence_only_increases() {
        let mut entry = TxnPartitionEntry::new(tp());
        assert_eq!(entry.maybe_update_last_acked_sequence(5), 5);
        assert_eq!(entry.last_acked_sequence(), Some(5));
        // A lower value is ignored and the current value returned.
        assert_eq!(entry.maybe_update_last_acked_sequence(3), 5);
        assert_eq!(entry.last_acked_sequence(), Some(5));
        assert_eq!(entry.maybe_update_last_acked_sequence(9), 9);
        assert_eq!(entry.last_acked_sequence(), Some(9));
    }

    #[test]
    fn test_increment_sequence_wraps_at_i32_max() {
        let mut entry = TxnPartitionEntry::new(tp());
        entry.increment_sequence(10);
        assert_eq!(entry.next_sequence(), 10);

        // Drive next_sequence close to i32::MAX and confirm it wraps rather
        // than overflowing (Java: DefaultRecordBatch.incrementSequence).
        entry.increment_sequence(i32::MAX - 10);
        assert_eq!(entry.next_sequence(), i32::MAX);
        entry.increment_sequence(1);
        assert_eq!(entry.next_sequence(), 0);
        entry.increment_sequence(5);
        assert_eq!(entry.next_sequence(), 5);
    }

    #[test]
    fn test_add_and_remove_inflight_batch() {
        let mut entry = TxnPartitionEntry::new(tp());
        let b = batch(1, 0, 0, 3);
        entry.add_inflight_batch(&b);
        assert!(entry.has_inflight_batches());
        assert_eq!(entry.next_batch_by_sequence(), Some((1, 0, 0)));

        entry.remove_in_flight_batch(&b);
        assert!(!entry.has_inflight_batches());
        assert_eq!(entry.next_batch_by_sequence(), None);
    }

    #[test]
    fn test_next_batch_by_sequence_returns_lowest() {
        let mut entry = TxnPartitionEntry::new(tp());
        for base in [10, 0, 5] {
            entry.add_inflight_batch(&batch(1, 0, base, 1));
        }
        assert_eq!(entry.next_batch_by_sequence(), Some((1, 0, 0)));
    }

    /// The 3-key comparator exists so that batches sharing a base sequence
    /// across an epoch bump are distinct entries — the bug referenced by the
    /// Java comment at TxnPartitionEntry.java:58-61.
    #[test]
    fn test_ordering_distinguishes_epoch_at_same_base_sequence() {
        let mut entry = TxnPartitionEntry::new(tp());
        let old_epoch = batch(1, 0, 0, 1);
        let new_epoch = batch(1, 1, 0, 1);
        entry.add_inflight_batch(&old_epoch);
        entry.add_inflight_batch(&new_epoch);

        // Two distinct entries, not one — a base-sequence-only key would collapse
        // them and remove the wrong batch.
        assert_eq!(entry.inflight_batches_by_sequence.len(), 2);
        assert_eq!(entry.next_batch_by_sequence(), Some((1, 0, 0)));

        entry.remove_in_flight_batch(&old_epoch);
        assert_eq!(entry.next_batch_by_sequence(), Some((1, 1, 0)));
    }

    #[test]
    fn test_ordering_distinguishes_producer_id_first() {
        let mut entry = TxnPartitionEntry::new(tp());
        entry.add_inflight_batch(&batch(2, 0, 0, 1));
        entry.add_inflight_batch(&batch(1, 9, 9, 1));
        // producer_id is the primary key, so (1, 9, 9) sorts before (2, 0, 0).
        assert_eq!(entry.next_batch_by_sequence(), Some((1, 9, 9)));
    }

    #[test]
    fn test_start_sequences_at_beginning_rewrites_batches_from_zero() {
        let mut entry = TxnPartitionEntry::new(tp());
        let mut b0 = batch(1, 0, 100, 3);
        let mut b1 = batch(1, 0, 103, 2);
        let mut b2 = batch(1, 0, 105, 4);
        entry.add_inflight_batch(&b0);
        entry.add_inflight_batch(&b1);
        entry.add_inflight_batch(&b2);
        entry.increment_sequence(9);

        let new_pid = ProducerIdAndEpoch::new(7, 2);
        entry
            .start_sequences_at_beginning(new_pid, &mut [&mut b0, &mut b1, &mut b2])
            .expect("all tracked batches supplied");

        // Sequences restart at 0 and accumulate record counts in key order.
        assert_eq!(b0.base_sequence(), 0);
        assert_eq!(b1.base_sequence(), 3);
        assert_eq!(b2.base_sequence(), 5);
        // Producer state is adopted by every batch and by the entry.
        assert_eq!(b0.producer_id(), 7);
        assert_eq!(b0.producer_epoch(), 2);
        assert_eq!(entry.producer_id_and_epoch(), new_pid);
        // next_sequence is the total record count; last acked resets.
        assert_eq!(entry.next_sequence(), 9);
        assert_eq!(entry.last_acked_sequence(), None);
        // The key set was rebuilt under the new producer state.
        assert_eq!(entry.next_batch_by_sequence(), Some((7, 2, 0)));
    }

    /// The caller may pass batches in any order; Java iterates its `TreeSet`, so
    /// the rewrite must follow key order regardless.
    #[test]
    fn test_start_sequences_at_beginning_is_order_independent() {
        let mut entry = TxnPartitionEntry::new(tp());
        let mut b0 = batch(1, 0, 0, 3);
        let mut b1 = batch(1, 0, 3, 2);
        entry.add_inflight_batch(&b0);
        entry.add_inflight_batch(&b1);

        // Deliberately reversed.
        entry
            .start_sequences_at_beginning(ProducerIdAndEpoch::new(7, 0), &mut [&mut b1, &mut b0])
            .expect("all tracked batches supplied");

        assert_eq!(b0.base_sequence(), 0);
        assert_eq!(b1.base_sequence(), 3);
    }

    /// The failed batch has already been removed from the in-flight set by the
    /// time Java calls this (it was fatally completed), so only the surviving
    /// batches are supplied. Passing the failed batch itself would drive its own
    /// sequence negative, which is the error path covered by the next test.
    #[test]
    fn test_adjust_sequences_shifts_only_batches_at_or_after_base_sequence() {
        let mut entry = TxnPartitionEntry::new(tp());
        let mut before = batch(1, 0, 0, 2);
        let mut after = batch(1, 0, 5, 4);
        entry.add_inflight_batch(&before);
        entry.add_inflight_batch(&after);
        entry.increment_sequence(9);

        // The failed batch was base_sequence 2 with 3 records.
        entry
            .adjust_sequences_due_to_failed_batch(2, 3, &mut [&mut before, &mut after])
            .expect("adjustment should succeed");

        // Below base_sequence: untouched.
        assert_eq!(before.base_sequence(), 0);
        // At or after base_sequence: shifted down by record_count.
        assert_eq!(after.base_sequence(), 2);
        // next_sequence drops by the failed batch's record count.
        assert_eq!(entry.next_sequence(), 6);
        // The key set was rebuilt under the new base sequences.
        assert_eq!(entry.next_batch_by_sequence(), Some((1, 0, 0)));
    }

    #[test]
    fn test_adjust_sequences_errors_when_a_batch_would_go_negative() {
        let mut entry = TxnPartitionEntry::new(tp());
        let mut b = batch(1, 0, 1, 5);
        entry.add_inflight_batch(&b);
        entry.increment_sequence(10);

        let error = entry
            .adjust_sequences_due_to_failed_batch(0, 5, &mut [&mut b])
            .expect_err("should reject a negative batch sequence");
        assert_eq!(
            error.message(),
            "Sequence number for batch with sequence 1 for partition topic-0 is going to become negative: -4"
        );
    }

    #[test]
    fn test_adjust_sequences_errors_when_next_sequence_would_go_negative() {
        let mut entry = TxnPartitionEntry::new(tp());
        entry.increment_sequence(2);

        let error = entry
            .adjust_sequences_due_to_failed_batch(0, 5, &mut [])
            .expect_err("should reject a negative next sequence");
        assert_eq!(
            error.message(),
            "Sequence number for partition topic-0 is going to become negative: -3"
        );
        // Java throws before mutating, so next_sequence is unchanged.
        assert_eq!(entry.next_sequence(), 2);
    }

    /// Rules file §8: the decrement must error rather than wrap. If it were
    /// routed through the wrapping helper this would silently pass with a large
    /// positive sequence.
    #[test]
    fn test_decrement_does_not_wrap() {
        let mut entry = TxnPartitionEntry::new(tp());
        entry.increment_sequence(1);
        assert!(entry.decrement_sequence(2).is_err());
        assert_eq!(entry.next_sequence(), 1, "must not have wrapped to a large positive");
    }

    #[test]
    fn test_decrement_succeeds_within_range() {
        let mut entry = TxnPartitionEntry::new(tp());
        entry.increment_sequence(10);
        entry.decrement_sequence(4).expect("should succeed");
        assert_eq!(entry.next_sequence(), 6);
        entry.decrement_sequence(6).expect("should reach exactly zero");
        assert_eq!(entry.next_sequence(), 0);
    }

    // -- Membership invariant of `reset_sequence_numbers` --------------------
    //
    // Java's `resetSequenceNumbers` iterates its OWN set, so membership is
    // invariant across the rebuild. These pin that the Rust version does the
    // same and does not take membership from the caller's slice. Regression
    // tests for Critic 41 finding 1.

    /// A short slice must ERROR, not silently clear the tracked set. Before the
    /// fix this passed while wiping the set and rewinding `next_sequence` to 0 —
    /// the exact corruption this type exists to prevent.
    #[test]
    fn test_start_sequences_errors_when_a_tracked_batch_is_missing() {
        let mut entry = TxnPartitionEntry::new(tp());
        let mut b0 = batch(1, 0, 0, 3);
        let b1 = batch(1, 0, 3, 2);
        entry.add_inflight_batch(&b0);
        entry.add_inflight_batch(&b1);
        entry.increment_sequence(5);

        // b1 is tracked but not supplied.
        let error = entry
            .start_sequences_at_beginning(ProducerIdAndEpoch::new(7, 0), &mut [&mut b0])
            .expect_err("a tracked batch missing from the pool must error");
        assert!(
            error.message().contains("No in-flight batch supplied for tracked sequence"),
            "got: {}",
            error.message()
        );
        // State must be untouched — in particular next_sequence must NOT rewind.
        assert_eq!(entry.next_sequence(), 5);
        assert_eq!(entry.producer_id_and_epoch(), ProducerIdAndEpoch::NONE);
    }

    #[test]
    fn test_empty_slice_errors_rather_than_clearing_a_tracked_set() {
        let mut entry = TxnPartitionEntry::new(tp());
        entry.add_inflight_batch(&batch(1, 0, 0, 3));
        entry.increment_sequence(3);

        assert!(
            entry
                .start_sequences_at_beginning(ProducerIdAndEpoch::new(7, 0), &mut [])
                .is_err(),
            "an empty pool must not silently clear a non-empty tracked set"
        );
        assert!(entry.has_inflight_batches(), "tracked set must survive the failed call");
        assert_eq!(entry.next_sequence(), 3, "next_sequence must not rewind");
    }

    /// An empty tracked set with an empty pool is a legitimate no-op.
    #[test]
    fn test_empty_slice_is_fine_when_nothing_is_tracked() {
        let mut entry = TxnPartitionEntry::new(tp());
        entry
            .start_sequences_at_beginning(ProducerIdAndEpoch::new(7, 1), &mut [])
            .expect("no tracked batches means nothing to resolve");
        assert_eq!(entry.next_sequence(), 0);
        assert_eq!(entry.producer_id_and_epoch(), ProducerIdAndEpoch::new(7, 1));
    }

    /// Batches in the pool that this entry does not track must be IGNORED, not
    /// inserted. This is the concrete Phase 4 shape: `Sender.failBatch` calls
    /// `handleFailedBatch` (`Sender.java:848`) before removing the batch from
    /// `Sender::in_flight_batches` (`:854`), while the txn map already dropped it
    /// (`TransactionManager.java:790` then `:818`). So the pool legitimately
    /// contains a batch the entry no longer tracks.
    #[test]
    fn test_untracked_batches_in_the_pool_are_ignored() {
        let mut entry = TxnPartitionEntry::new(tp());
        let mut surviving = batch(1, 0, 5, 4);
        entry.add_inflight_batch(&surviving);
        entry.increment_sequence(9);

        // The failed batch: still owned by the Sender, already untracked here.
        let mut failed = batch(1, 0, 2, 3);

        entry
            .adjust_sequences_due_to_failed_batch(2, 3, &mut [&mut failed, &mut surviving])
            .expect("an untracked batch in the pool must be ignored, not shifted");

        // Only the tracked batch was shifted.
        assert_eq!(surviving.base_sequence(), 2);
        // The untracked failed batch was left alone — before the fix it was
        // re-inserted AND shifted to 2 - 3 = -1, producing a spurious error.
        assert_eq!(failed.base_sequence(), 2);
        assert_eq!(entry.next_sequence(), 6);
        // And it was not re-added to the tracked set.
        assert_eq!(entry.inflight_batches_by_sequence.len(), 1);
        assert_eq!(entry.next_batch_by_sequence(), Some((1, 0, 2)));
    }

    /// The reenqueue (retry) path: a batch stays **tracked** while moving from
    /// the Sender's in-flight map to the accumulator's deque, so the pool must be
    /// drawn from both owners.
    ///
    /// Regression test for Critic 41 second-pass finding. The first-pass audit
    /// justified the `TransactionManager.java:655` call site as "those batches are
    /// in flight, so the Sender holds them" — false on this path.
    /// `Sender.reenqueueBatch` (`Sender.java:750-752`) does **not** call
    /// `transactionManager.removeInFlightBatch`, unlike the split path at `:685`,
    /// and Java asserts the batch is still tracked
    /// (`RecordAccumulator.java:558-560`).
    ///
    /// Both halves are pinned: supplying only one owner's batches errors, and
    /// supplying both succeeds. `bump_idempotent_producer_epoch` exists precisely
    /// to rewrite the reenqueued batch, so the erroring case would break
    /// idempotent recovery outright.
    #[test]
    fn test_reenqueued_batch_stays_tracked_and_must_be_supplied() {
        let mut entry = TxnPartitionEntry::new(tp());
        // `still_in_flight` remains with the Sender; `reenqueued` has moved to the
        // accumulator's deque after a retriable error. Both stay tracked.
        let mut still_in_flight = batch(1, 0, 0, 2);
        let mut reenqueued = batch(1, 0, 2, 3);
        entry.add_inflight_batch(&still_in_flight);
        entry.add_inflight_batch(&reenqueued);
        entry.increment_sequence(5);

        // Pool from the Sender's map alone — the reenqueued batch is missing.
        let error = entry
            .start_sequences_at_beginning(ProducerIdAndEpoch::new(7, 1), &mut [&mut still_in_flight])
            .expect_err("a pool missing the reenqueued batch must error, not silently proceed");
        assert!(
            error.message().contains("No in-flight batch supplied for tracked sequence"),
            "got: {}",
            error.message()
        );
        // Nothing moved — in particular the sequence counter did not rewind.
        assert_eq!(entry.next_sequence(), 5);
        assert_eq!(entry.producer_id_and_epoch(), ProducerIdAndEpoch::NONE);

        // Pool drawn from BOTH owners — the epoch bump succeeds and rewrites both.
        entry
            .start_sequences_at_beginning(ProducerIdAndEpoch::new(7, 1), &mut [&mut still_in_flight, &mut reenqueued])
            .expect("supplying every tracked batch, from both owners, must succeed");

        assert_eq!(still_in_flight.base_sequence(), 0);
        assert_eq!(reenqueued.base_sequence(), 2, "rewritten after the first batch's records");
        assert_eq!(entry.producer_id_and_epoch(), ProducerIdAndEpoch::new(7, 1));
        assert_eq!(entry.next_sequence(), 5);
        assert_eq!(entry.inflight_batches_by_sequence.len(), 2, "both still tracked");
    }

    /// Membership count is preserved across a rebuild regardless of pool size.
    #[test]
    fn test_membership_count_is_invariant_across_rebuild() {
        let mut entry = TxnPartitionEntry::new(tp());
        let mut b0 = batch(1, 0, 0, 1);
        let mut b1 = batch(1, 0, 1, 1);
        let mut extra = batch(9, 9, 9, 1); // never tracked
        entry.add_inflight_batch(&b0);
        entry.add_inflight_batch(&b1);
        assert_eq!(entry.inflight_batches_by_sequence.len(), 2);

        entry
            .start_sequences_at_beginning(ProducerIdAndEpoch::new(7, 0), &mut [&mut extra, &mut b1, &mut b0])
            .expect("superset pool is fine");

        assert_eq!(entry.inflight_batches_by_sequence.len(), 2, "membership must be invariant");
        assert_eq!(extra.base_sequence(), 9, "untracked batch must be untouched");
    }
    /// Exhaustive dry-run of the tracked-set / supplied-pool matrix, including
    /// the shapes of all three Java `startSequencesAtBeginning` call sites.
    #[test]
    fn test_reset_matrix_all_combinations() {
        // Case B: nothing tracked, but the pool has batches (the
        // `TransactionManager.java:594` shape — that site is guarded by
        // `!hasInflightBatches`, so the tracked set is empty while the Sender
        // may still hold batches). Extras must be left alone.
        {
            let mut entry = TxnPartitionEntry::new(tp());
            let mut extra = batch(1, 0, 7, 2);
            entry
                .start_sequences_at_beginning(ProducerIdAndEpoch::new(5, 1), &mut [&mut extra])
                .expect("empty tracked set with a non-empty pool is a no-op");
            assert_eq!(extra.base_sequence(), 7, "untracked batch must not be rewritten");
            assert_eq!(entry.next_sequence(), 0);
            assert_eq!(entry.producer_id_and_epoch(), ProducerIdAndEpoch::new(5, 1));
            assert!(!entry.has_inflight_batches());
        }

        // Case C: exact match, single batch.
        {
            let mut entry = TxnPartitionEntry::new(tp());
            let mut a = batch(1, 0, 4, 3);
            entry.add_inflight_batch(&a);
            entry
                .start_sequences_at_beginning(ProducerIdAndEpoch::new(5, 1), &mut [&mut a])
                .expect("exact pool");
            assert_eq!(a.base_sequence(), 0);
            assert_eq!(entry.next_sequence(), 3);
            assert_eq!(entry.inflight_batches_by_sequence.len(), 1);
        }

        // Case G: exact match, pool in reverse order — key order must win.
        {
            let mut entry = TxnPartitionEntry::new(tp());
            let mut a = batch(1, 0, 0, 3);
            let mut c = batch(1, 0, 3, 4);
            entry.add_inflight_batch(&a);
            entry.add_inflight_batch(&c);
            entry
                .start_sequences_at_beginning(ProducerIdAndEpoch::new(5, 1), &mut [&mut c, &mut a])
                .expect("reversed pool");
            assert_eq!(a.base_sequence(), 0, "lowest key first regardless of pool order");
            assert_eq!(c.base_sequence(), 3);
            assert_eq!(entry.next_sequence(), 7);
        }

        // The `:1048` shape: a single partition's in-flight batches rewritten
        // during produce-response handling. Sender still owns all of them.
        {
            let mut entry = TxnPartitionEntry::new(tp());
            let mut b0 = batch(1, 0, 10, 2);
            let mut b1 = batch(1, 0, 12, 2);
            entry.add_inflight_batch(&b0);
            entry.add_inflight_batch(&b1);
            entry.increment_sequence(14);
            entry
                .start_sequences_at_beginning(ProducerIdAndEpoch::new(2, 5), &mut [&mut b0, &mut b1])
                .expect("all in-flight batches supplied");
            assert_eq!((b0.base_sequence(), b1.base_sequence()), (0, 2));
            assert_eq!(entry.next_sequence(), 4);
            assert_eq!(entry.inflight_batches_by_sequence.len(), 2);
        }

        // The `:818` adjust shape, exhaustively: failed batch present in the pool
        // but untracked; one batch below base_sequence; one at; one after.
        {
            let mut entry = TxnPartitionEntry::new(tp());
            let mut below = batch(1, 0, 0, 2);
            let mut at = batch(1, 0, 4, 1);
            let mut after = batch(1, 0, 5, 3);
            entry.add_inflight_batch(&below);
            entry.add_inflight_batch(&at);
            entry.add_inflight_batch(&after);
            entry.increment_sequence(10);
            let mut failed = batch(1, 0, 2, 2); // untracked, still owned by Sender

            entry
                .adjust_sequences_due_to_failed_batch(4, 2, &mut [&mut failed, &mut after, &mut below, &mut at])
                .expect("superset pool with an untracked failed batch");

            assert_eq!(below.base_sequence(), 0, "below base_sequence: untouched");
            assert_eq!(at.base_sequence(), 2, "at base_sequence: shifted by record_count");
            assert_eq!(after.base_sequence(), 3, "after base_sequence: shifted");
            assert_eq!(failed.base_sequence(), 2, "untracked failed batch: untouched");
            assert_eq!(entry.next_sequence(), 8);
            assert_eq!(entry.inflight_batches_by_sequence.len(), 3, "membership invariant");
        }
    }
}
