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

//! Per-partition idempotence/transaction bookkeeping, keyed by topic-partition.

use std::collections::HashMap;

use crate::common::KafkaError;
use crate::common::TopicPartition;
use crate::common::requests::produce_response::INVALID_OFFSET;
use crate::common::utils::{LogContext, ProducerIdAndEpoch};
use crate::kafka_trace;
use crate::producer::internals::ProducerBatch;
use crate::producer::internals::txn_partition_entry::{InFlightBatchKey, TxnPartitionEntry};

/// The per-partition sequence and offset bookkeeping for an idempotent or
/// transactional producer.
///
/// Translated from
/// `org.apache.kafka.clients.producer.internals.TxnPartitionMap`.
///
/// No interior `Mutex`: Java relies on the caller holding the
/// `TransactionManager` monitor, and Phase 3 wraps the whole manager. A lock
/// here would nest for nothing.
pub(crate) struct TxnPartitionMap {
    log_context: LogContext,
    topic_partitions: HashMap<TopicPartition, TxnPartitionEntry>,
}

impl TxnPartitionMap {
    /// Creates an empty map.
    pub(crate) fn new(log_context: LogContext) -> Self {
        Self { log_context, topic_partitions: HashMap::new() }
    }

    /// The entry for `topic_partition`, which must already exist.
    ///
    /// Java throws `IllegalStateException` when absent; per CLAUDE.md §10.2 that
    /// becomes an `Err`. Note the deliberate asymmetry with
    /// [`Self::get_or_create`] and the tolerant accessors below — see the
    /// comment on [`Self::last_acked_offset`].
    pub(crate) fn get(&self, topic_partition: &TopicPartition) -> Result<&TxnPartitionEntry, KafkaError> {
        self.topic_partitions.get(topic_partition).ok_or_else(|| {
            KafkaError::illegal_state(format!(
                "Trying to get txnPartitionEntry for {topic_partition}, but it was never set for this partition."
            ))
        })
    }

    /// The entry for `topic_partition` for mutation, which must already exist.
    ///
    /// Java has no separate mutable accessor — Java references are implicitly
    /// mutable — so this is the `&mut` half of [`Self::get`], not an extra
    /// method. Same error behavior.
    pub(crate) fn get_mut(&mut self, topic_partition: &TopicPartition) -> Result<&mut TxnPartitionEntry, KafkaError> {
        self.topic_partitions.get_mut(topic_partition).ok_or_else(|| {
            KafkaError::illegal_state(format!(
                "Trying to get txnPartitionEntry for {topic_partition}, but it was never set for this partition."
            ))
        })
    }

    /// The entry for `topic_partition`, creating it if absent.
    pub(crate) fn get_or_create(&mut self, topic_partition: &TopicPartition) -> &mut TxnPartitionEntry {
        self.topic_partitions
            .entry(topic_partition.clone())
            .or_insert_with(|| TxnPartitionEntry::new(topic_partition.clone()))
    }

    /// Whether an entry exists for `topic_partition`.
    pub(crate) fn contains(&self, topic_partition: &TopicPartition) -> bool {
        self.topic_partitions.contains_key(topic_partition)
    }

    /// Drops all entries.
    pub(crate) fn reset(&mut self) {
        self.topic_partitions.clear();
    }

    /// The last acknowledged offset for `topic_partition`.
    ///
    /// Tolerates a missing entry by returning `None`, unlike [`Self::get`] which
    /// errors. The asymmetry is deliberate and matches Java: the accessors that
    /// callers use opportunistically (this one, [`Self::last_acked_sequence`],
    /// [`Self::maybe_update_last_acked_sequence`]) treat absence as "nothing
    /// recorded yet", while `get` is used where the caller has already
    /// established that sequences are being tracked, so absence is a bug.
    pub(crate) fn last_acked_offset(&self, topic_partition: &TopicPartition) -> Option<i64> {
        self.topic_partitions
            .get(topic_partition)
            .and_then(TxnPartitionEntry::last_acked_offset)
    }

    /// The last acknowledged sequence for `topic_partition`, or `None` if the
    /// entry is absent or nothing has been acknowledged.
    pub(crate) fn last_acked_sequence(&self, topic_partition: &TopicPartition) -> Option<i32> {
        self.topic_partitions
            .get(topic_partition)
            .and_then(TxnPartitionEntry::last_acked_sequence)
    }

    /// Rewrites the in-flight batches for `topic_partition` to start at sequence
    /// 0 under `new_producer_id_and_epoch`.
    ///
    /// `batches` must be the in-flight batches for the partition, supplied by
    /// their owner — see `.claude/rules/producer-transactions.md` §7.
    ///
    /// Java calls `get()` (which throws when absent) and *then* null-checks the
    /// result, so its null branch is unreachable. Only the reachable behavior is
    /// translated.
    pub(crate) fn start_sequences_at_beginning(
        &mut self,
        topic_partition: &TopicPartition,
        new_producer_id_and_epoch: ProducerIdAndEpoch,
        batches: &mut [&mut ProducerBatch],
    ) -> Result<(), KafkaError> {
        self.get_mut(topic_partition)?
            .start_sequences_at_beginning(new_producer_id_and_epoch, batches);
        Ok(())
    }

    /// Drops the entry for `topic_partition`.
    pub(crate) fn remove(&mut self, topic_partition: &TopicPartition) {
        self.topic_partitions.remove(topic_partition);
    }

    /// Raises the last acknowledged offset for `topic_partition` to
    /// `last_offset` if it is higher.
    pub(crate) fn update_last_acked_offset(
        &mut self,
        topic_partition: &TopicPartition,
        is_transactional: bool,
        last_offset: i64,
    ) -> Result<(), KafkaError> {
        let last_acked_offset = self.last_acked_offset(topic_partition);
        // It might happen that the TransactionManager has been reset while a
        // request was reenqueued and got a valid response for this. This can
        // happen only if the producer is only idempotent (not transactional) and
        // in this case there will be no tracked bookkeeper entry about it, so we
        // have to insert one.
        if last_acked_offset.is_none() && !is_transactional {
            self.get_or_create(topic_partition);
        }
        if last_offset > last_acked_offset.unwrap_or(INVALID_OFFSET) {
            self.get_mut(topic_partition)?.set_last_acked_offset(last_offset);
        } else {
            kafka_trace!(
                self.log_context,
                "Partition {} keeps lastOffset at {}",
                topic_partition,
                last_offset
            );
        }
        Ok(())
    }

    /// Shifts sequence numbers down for `batch`'s partition after `batch` was
    /// failed fatally by the broker, so future batches do not fail with
    /// `OutOfOrderSequenceException`.
    ///
    /// Must only be called once the broker has unequivocally failed the batch —
    /// i.e. a confirmed fatal status such as `MessageTooLarge`.
    ///
    /// `batches` must be the *remaining* in-flight batches for the partition,
    /// supplied by their owner — see
    /// `.claude/rules/producer-transactions.md` §7.
    pub(crate) fn adjust_sequences_due_to_failed_batch(
        &mut self,
        batch: &ProducerBatch,
        batches: &mut [&mut ProducerBatch],
    ) -> Result<(), KafkaError> {
        if !self.contains(&batch.topic_partition) {
            // Sequence numbers are not being tracked for this partition. This
            // could happen if the producer id was just reset due to a previous
            // OutOfOrderSequenceException.
            return Ok(());
        }
        kafka_trace!(
            self.log_context,
            "producerId: {}, send to partition {} failed fatally. Reducing future sequence numbers by {}",
            batch.producer_id(),
            batch.topic_partition,
            batch.record_count
        );

        let base_sequence = i64::from(batch.base_sequence());
        let record_count = batch.record_count;
        self.get_mut(&batch.topic_partition)?
            .adjust_sequences_due_to_failed_batch(base_sequence, record_count, batches)
    }

    /// Raises the last acknowledged sequence for `topic_partition` to
    /// `sequence` if it is higher, returning the resulting value.
    ///
    /// Returns [`TxnPartitionEntry::NO_LAST_ACKED_SEQUENCE_NUMBER`] when no
    /// entry exists, tolerating absence as Java does.
    pub(crate) fn maybe_update_last_acked_sequence(&mut self, topic_partition: &TopicPartition, sequence: i32) -> i32 {
        match self.topic_partitions.get_mut(topic_partition) {
            Some(entry) => entry.maybe_update_last_acked_sequence(sequence),
            None => TxnPartitionEntry::NO_LAST_ACKED_SEQUENCE_NUMBER,
        }
    }

    /// The key of the lowest-sequence in-flight batch for `topic_partition`.
    pub(crate) fn next_batch_by_sequence(
        &self,
        topic_partition: &TopicPartition,
    ) -> Result<Option<InFlightBatchKey>, KafkaError> {
        Ok(self.get(topic_partition)?.next_batch_by_sequence())
    }

    /// Removes `batch` from the in-flight set for its partition.
    pub(crate) fn remove_in_flight_batch(&mut self, batch: &ProducerBatch) -> Result<(), KafkaError> {
        self.get_mut(&batch.topic_partition)?.remove_in_flight_batch(batch);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::compress::Compression;
    use crate::common::record::TimestampType;
    use crate::common::record::memory_records::MemoryRecords;

    fn tp(partition: i32) -> TopicPartition {
        TopicPartition::new("topic".to_string(), partition)
    }

    fn map() -> TxnPartitionMap {
        TxnPartitionMap::new(LogContext::empty())
    }

    fn batch_for(
        topic_partition: TopicPartition,
        producer_id: i64,
        epoch: i16,
        base_sequence: i32,
        record_count: i32,
    ) -> ProducerBatch {
        let builder = MemoryRecords::builder(512, Compression::none(), TimestampType::CreateTime, 128);
        let mut b = ProducerBatch::new(topic_partition, builder, 0);
        b.record_count = record_count;
        b.set_producer_state(producer_id, epoch, base_sequence, false);
        b
    }

    #[test]
    fn test_get_errors_when_never_set() {
        let m = map();
        let error = m.get(&tp(0)).expect_err("absent entry must error");
        assert_eq!(
            error.message(),
            "Trying to get txnPartitionEntry for topic-0, but it was never set for this partition."
        );
    }

    #[test]
    fn test_get_or_create_inserts_then_returns_same_entry() {
        let mut m = map();
        assert!(!m.contains(&tp(0)));
        m.get_or_create(&tp(0)).increment_sequence(4);
        assert!(m.contains(&tp(0)));
        // Second call must not reset the entry.
        assert_eq!(m.get_or_create(&tp(0)).next_sequence(), 4);
        assert_eq!(m.get(&tp(0)).expect("entry exists").next_sequence(), 4);
    }

    #[test]
    fn test_reset_clears_all_entries() {
        let mut m = map();
        m.get_or_create(&tp(0));
        m.get_or_create(&tp(1));
        m.reset();
        assert!(!m.contains(&tp(0)));
        assert!(!m.contains(&tp(1)));
    }

    #[test]
    fn test_remove_drops_only_that_partition() {
        let mut m = map();
        m.get_or_create(&tp(0));
        m.get_or_create(&tp(1));
        m.remove(&tp(0));
        assert!(!m.contains(&tp(0)));
        assert!(m.contains(&tp(1)));
    }

    /// The tolerant accessors return `None` for a missing entry rather than
    /// erroring, unlike `get`.
    #[test]
    fn test_tolerant_accessors_allow_missing_entry() {
        let m = map();
        assert_eq!(m.last_acked_offset(&tp(0)), None);
        assert_eq!(m.last_acked_sequence(&tp(0)), None);
    }

    #[test]
    fn test_maybe_update_last_acked_sequence_tolerates_missing_entry() {
        let mut m = map();
        assert_eq!(
            m.maybe_update_last_acked_sequence(&tp(0), 5),
            TxnPartitionEntry::NO_LAST_ACKED_SEQUENCE_NUMBER
        );
        // Absence must not implicitly create an entry.
        assert!(!m.contains(&tp(0)));
    }

    #[test]
    fn test_maybe_update_last_acked_sequence_delegates_to_entry() {
        let mut m = map();
        m.get_or_create(&tp(0));
        assert_eq!(m.maybe_update_last_acked_sequence(&tp(0), 7), 7);
        assert_eq!(m.maybe_update_last_acked_sequence(&tp(0), 3), 7);
        assert_eq!(m.last_acked_sequence(&tp(0)), Some(7));
    }

    #[test]
    fn test_update_last_acked_offset_raises_only_on_higher_offset() {
        let mut m = map();
        m.get_or_create(&tp(0));
        m.update_last_acked_offset(&tp(0), true, 10).expect("should succeed");
        assert_eq!(m.last_acked_offset(&tp(0)), Some(10));

        // Lower offset is ignored.
        m.update_last_acked_offset(&tp(0), true, 5).expect("should succeed");
        assert_eq!(m.last_acked_offset(&tp(0)), Some(10));

        m.update_last_acked_offset(&tp(0), true, 11).expect("should succeed");
        assert_eq!(m.last_acked_offset(&tp(0)), Some(11));
    }

    /// The idempotent-only lazy-create path (Java 90-94): the manager may have
    /// been reset while a request was reenqueued, so a non-transactional
    /// producer must get an entry inserted rather than erroring.
    #[test]
    fn test_update_last_acked_offset_creates_entry_when_idempotent_only() {
        let mut m = map();
        assert!(!m.contains(&tp(0)));
        m.update_last_acked_offset(&tp(0), false, 10)
            .expect("should lazily create the entry");
        assert!(m.contains(&tp(0)));
        assert_eq!(m.last_acked_offset(&tp(0)), Some(10));
    }

    /// The transactional case does NOT lazily create, so it errors instead.
    #[test]
    fn test_update_last_acked_offset_errors_when_transactional_and_absent() {
        let mut m = map();
        let error = m
            .update_last_acked_offset(&tp(0), true, 10)
            .expect_err("transactional path must not lazily create");
        assert_eq!(
            error.message(),
            "Trying to get txnPartitionEntry for topic-0, but it was never set for this partition."
        );
        assert!(!m.contains(&tp(0)));
    }

    #[test]
    fn test_adjust_sequences_is_a_noop_for_untracked_partition() {
        let mut m = map();
        let failed = batch_for(tp(0), 1, 0, 0, 3);
        // Not tracked: must return Ok without erroring.
        m.adjust_sequences_due_to_failed_batch(&failed, &mut [])
            .expect("untracked partition must be a no-op");
        assert!(!m.contains(&tp(0)));
    }

    #[test]
    fn test_adjust_sequences_delegates_to_entry() {
        let mut m = map();
        m.get_or_create(&tp(0)).increment_sequence(9);
        let mut surviving = batch_for(tp(0), 1, 0, 5, 4);
        m.get_or_create(&tp(0)).add_inflight_batch(&surviving);

        let failed = batch_for(tp(0), 1, 0, 2, 3);
        m.adjust_sequences_due_to_failed_batch(&failed, &mut [&mut surviving])
            .expect("should succeed");

        assert_eq!(surviving.base_sequence(), 2);
        assert_eq!(m.get(&tp(0)).expect("entry exists").next_sequence(), 6);
    }

    #[test]
    fn test_add_and_remove_in_flight_batch_round_trip() {
        let mut m = map();
        let b = batch_for(tp(0), 1, 0, 0, 3);
        m.get_or_create(&tp(0)).add_inflight_batch(&b);
        assert_eq!(m.next_batch_by_sequence(&tp(0)).expect("entry exists"), Some((1, 0, 0)));

        m.remove_in_flight_batch(&b).expect("should succeed");
        assert_eq!(m.next_batch_by_sequence(&tp(0)).expect("entry exists"), None);
    }

    #[test]
    fn test_next_batch_by_sequence_errors_for_untracked_partition() {
        let m = map();
        assert!(m.next_batch_by_sequence(&tp(0)).is_err());
    }

    #[test]
    fn test_remove_in_flight_batch_errors_for_untracked_partition() {
        let mut m = map();
        let b = batch_for(tp(0), 1, 0, 0, 1);
        assert!(m.remove_in_flight_batch(&b).is_err());
    }

    #[test]
    fn test_start_sequences_at_beginning_delegates_and_errors_when_absent() {
        let mut m = map();
        let mut b = batch_for(tp(0), 1, 0, 100, 3);

        // Absent entry errors, matching Java's reachable `get()` behavior.
        assert!(
            m.start_sequences_at_beginning(&tp(0), ProducerIdAndEpoch::new(7, 1), &mut [&mut b])
                .is_err()
        );

        m.get_or_create(&tp(0)).add_inflight_batch(&b);
        m.start_sequences_at_beginning(&tp(0), ProducerIdAndEpoch::new(7, 1), &mut [&mut b])
            .expect("should succeed once the entry exists");
        assert_eq!(b.base_sequence(), 0);
        assert_eq!(b.producer_id(), 7);
        assert_eq!(
            m.get(&tp(0)).expect("entry exists").producer_id_and_epoch(),
            ProducerIdAndEpoch::new(7, 1)
        );
    }

    #[test]
    fn test_entries_are_independent_per_partition() {
        let mut m = map();
        m.get_or_create(&tp(0)).increment_sequence(3);
        m.get_or_create(&tp(1)).increment_sequence(8);
        assert_eq!(m.get(&tp(0)).expect("exists").next_sequence(), 3);
        assert_eq!(m.get(&tp(1)).expect("exists").next_sequence(), 8);
    }
}
