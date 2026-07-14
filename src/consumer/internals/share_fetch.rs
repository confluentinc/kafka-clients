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

//! Records fetched from the broker for a share consumer, ready to return to
//! the user (KIP-932).
//!
//! Corresponds to
//! `org.apache.kafka.clients.consumer.internals.ShareFetch`.

// The share fetch path is wired into `AsyncKafkaShareConsumer` /
// `ShareConsumeRequestManager` in a later phase.
#![allow(dead_code)]

use indexmap::IndexMap;

use crate::common::{KafkaError, TopicIdPartition, TopicPartition};
use crate::consumer::{AcknowledgeType, ConsumerRecord};

use super::acknowledgements::Acknowledgements;
use super::node_acknowledgements::NodeAcknowledgements;
use super::share_in_flight_batch::ShareInFlightBatch;

/// [`ShareFetch`] represents the records fetched from the broker to be returned
/// to the consumer to satisfy a share-consumer `poll(Duration)` call. The
/// records can come from multiple topic-partitions.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.ShareFetch<K, V>`.
///
/// `batches` uses an [`IndexMap`] to give deterministic, insertion-ordered
/// iteration (Java's `batches` is a `HashMap`, but the derived `records()` /
/// `takeAcknowledgedRecords()` results build `LinkedHashMap`s; preserving
/// insertion order here keeps the delivered ordering stable and review-friendly).
pub(crate) struct ShareFetch<K, V> {
    batches: IndexMap<TopicIdPartition, ShareInFlightBatch<K, V>>,
    acquisition_lock_timeout_ms: Option<i32>,
    acquisition_lock_timeout_ms_renewed: Option<i32>,
}

impl<K, V> ShareFetch<K, V> {
    /// Returns an empty share fetch. Mirrors Java's static `empty()`.
    pub(crate) fn empty() -> Self {
        Self {
            batches: IndexMap::new(),
            acquisition_lock_timeout_ms: None,
            acquisition_lock_timeout_ms_renewed: None,
        }
    }

    /// Adds another [`ShareInFlightBatch`] to this one; all of its records will
    /// be added to this object's [`records`](Self::records).
    ///
    /// Mirrors Java's `void add(TopicIdPartition, ShareInFlightBatch<K, V>)`.
    ///
    /// # Ownership note (carryover from Phase 2)
    ///
    /// Java reads `batch.getAcquisitionLockTimeoutMs()` *after* `currentBatch.merge(batch)`.
    /// Rust's [`ShareInFlightBatch::merge`] consumes `batch` by value, so the
    /// timeout is read *before* the merge here. The end state is identical:
    /// `merge` does not touch the acquisition-lock timeout.
    pub(crate) fn add(&mut self, partition: TopicIdPartition, batch: ShareInFlightBatch<K, V>) {
        let batch_timeout = batch.get_acquisition_lock_timeout_ms();
        match self.batches.get_mut(&partition) {
            Some(current_batch) => {
                // This case shouldn't usually happen because we only send one
                // fetch at a time per partition, but it might conceivably
                // happen in some rare cases (such as partition leader changes).
                current_batch.merge(batch);
            },
            None => {
                self.batches.insert(partition, batch);
            },
        }
        if batch_timeout.is_some() {
            self.acquisition_lock_timeout_ms = batch_timeout;
        }
    }

    /// Returns all the non-control messages for this fetch, grouped by
    /// partition, as borrowed references.
    ///
    /// Mirrors Java's `Map<TopicPartition, List<ConsumerRecord<K, V>>> records()`.
    ///
    /// # Deviation from Java
    ///
    /// Java returns owned `List<ConsumerRecord>`s that share the batch's record
    /// objects (Java records are reference types). `ConsumerRecord` is not
    /// `Clone` in Rust (receive-path zero-copy contract, §27), so this returns
    /// borrowed references — a non-destructive read view. To hand owned records
    /// to the user (Java's `ShareConsumerImpl.poll` path), use
    /// [`Self::take_records`].
    pub(crate) fn records(&self) -> IndexMap<TopicPartition, Vec<&ConsumerRecord<K, V>>> {
        let mut result: IndexMap<TopicPartition, Vec<&ConsumerRecord<K, V>>> = IndexMap::new();
        for (tip, batch) in &self.batches {
            result.insert(tip.topic_partition().clone(), batch.get_in_flight_records());
        }
        result
    }

    /// Drains all the non-control messages for this fetch, transferring
    /// ownership to the caller, grouped by partition.
    ///
    /// # Owned-record delivery (carryover from Phase 2)
    ///
    /// There is no direct Java analog: Java's `records()` is non-destructive
    /// because its records are shared reference types. In Rust, delivering
    /// owned `ConsumerRecord`s to the user requires moving them out of the
    /// in-flight set (`ConsumerRecord` is not `Clone`). This drains the
    /// in-flight records out of each batch. The (later-phase)
    /// `AsyncKafkaShareConsumer.poll` handoff must therefore take any needed
    /// acknowledgements / renewals into account *before* draining, since the
    /// batch no longer holds the records afterwards.
    pub(crate) fn take_records(&mut self) -> IndexMap<TopicPartition, Vec<ConsumerRecord<K, V>>> {
        let mut result: IndexMap<TopicPartition, Vec<ConsumerRecord<K, V>>> = IndexMap::new();
        for (tip, batch) in &mut self.batches {
            result.insert(tip.topic_partition().clone(), batch.take_in_flight_records());
        }
        result
    }

    /// Returns the total number of non-control messages for this fetch, across
    /// all partitions.
    ///
    /// Mirrors Java's `int numRecords()`. Like Java, this prunes empty batches
    /// that have no records pending renewal (so it takes `&mut self`).
    pub(crate) fn num_records(&mut self) -> usize {
        let mut num_records = 0;
        if !self.batches.is_empty() {
            self.batches.retain(|_tip, batch| {
                if batch.is_empty() {
                    // Keep only if it still has records pending renewal.
                    batch.has_renewals()
                } else {
                    num_records += batch.num_records();
                    true
                }
            });
        }
        num_records
    }

    /// Returns `true` if and only if this fetch did not return any non-control
    /// records.
    ///
    /// Mirrors Java's `boolean isEmpty()`.
    pub(crate) fn is_empty(&mut self) -> bool {
        self.num_records() == 0
    }

    /// Returns the most up-to-date value of the acquisition lock timeout, if
    /// available. Mirrors Java's `Optional<Integer> acquisitionLockTimeoutMs()`.
    pub(crate) fn acquisition_lock_timeout_ms(&self) -> Option<i32> {
        self.acquisition_lock_timeout_ms
    }

    /// Returns `true` if this fetch contains records being renewed.
    ///
    /// Mirrors Java's `boolean hasRenewals()`.
    pub(crate) fn has_renewals(&self) -> bool {
        self.batches.values().any(ShareInFlightBatch::has_renewals)
    }

    /// Takes any renewed records and moves them back into in-flight state.
    ///
    /// Mirrors Java's `void takeRenewedRecords()`.
    pub(crate) fn take_renewed_records(&mut self) {
        for batch in self.batches.values_mut() {
            batch.take_renewals();
        }
        // Any acquisition lock timeout updated by renewal is applied as the
        // renewed records are moved back to in-flight.
        if self.acquisition_lock_timeout_ms_renewed.is_some() {
            self.acquisition_lock_timeout_ms = self.acquisition_lock_timeout_ms_renewed;
        }
    }

    /// Acknowledges a single record in the current batch.
    ///
    /// Mirrors Java's `void acknowledge(ConsumerRecord<K, V>, AcknowledgeType)`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::illegal_state`] with `"The record cannot be
    /// acknowledged."` if no batch owns the record (matching Java's
    /// `IllegalStateException`). Also propagates the error from
    /// [`ShareInFlightBatch::acknowledge`] when the record's partition matches
    /// but the record is not in flight.
    pub(crate) fn acknowledge(
        &mut self,
        record: &ConsumerRecord<K, V>,
        ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError> {
        for (tip, batch) in &mut self.batches {
            if tip.topic() == record.topic() && tip.partition() == record.partition() {
                return batch.acknowledge(record, ack_type);
            }
        }
        Err(KafkaError::illegal_state("The record cannot be acknowledged."))
    }

    /// Acknowledges a single record which experienced an exception during its
    /// delivery, identified by topic, partition and offset. This is
    /// specifically for overriding the default acknowledge type for records
    /// whose delivery failed.
    ///
    /// Mirrors Java's `void acknowledge(String, int, long, AcknowledgeType)`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::illegal_state`] with `"The record cannot be
    /// acknowledged."` if no batch has a cached exception covering the offset.
    pub(crate) fn acknowledge_on_exception(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError> {
        for (tip, batch) in &mut self.batches {
            let covered = batch
                .get_exception()
                .is_some_and(|exception| exception.offsets().contains(&offset));
            if tip.topic() == topic && tip.partition() == partition && covered {
                batch.add_acknowledgement(offset, ack_type);
                return Ok(());
            }
        }
        Err(KafkaError::illegal_state("The record cannot be acknowledged."))
    }

    /// Acknowledges all records in the current batch. If any records in the
    /// batch already have been acknowledged, those acknowledgements are not
    /// overwritten.
    ///
    /// Mirrors Java's `void acknowledgeAll(AcknowledgeType)`.
    pub(crate) fn acknowledge_all(&mut self, ack_type: AcknowledgeType) {
        for batch in self.batches.values_mut() {
            batch.acknowledge_all(ack_type);
        }
    }

    /// Checks whether all in-flight records have been acknowledged. This is
    /// required for explicit acknowledgement mode.
    ///
    /// Mirrors Java's `boolean checkAllInFlightAreAcknowledged()`.
    pub(crate) fn check_all_in_flight_are_acknowledged(&self) -> bool {
        self.batches
            .values()
            .all(ShareInFlightBatch::check_all_in_flight_are_acknowledged)
    }

    /// Removes all acknowledged records from the in-flight records and returns
    /// the map of acknowledgements to send. If some records were not
    /// acknowledged, the in-flight records will not be empty after this method.
    ///
    /// Mirrors Java's
    /// `Map<TopicIdPartition, NodeAcknowledgements> takeAcknowledgedRecords()`.
    pub(crate) fn take_acknowledged_records(&mut self) -> IndexMap<TopicIdPartition, NodeAcknowledgements> {
        let mut acknowledgement_map: IndexMap<TopicIdPartition, NodeAcknowledgements> = IndexMap::new();
        for (tip, batch) in &mut self.batches {
            let node_id = batch.node_id();
            let acknowledgements = batch.take_acknowledged_records();
            if !acknowledgements.is_empty() {
                acknowledgement_map.insert(tip.clone(), NodeAcknowledgements::new(node_id, acknowledgements));
            }
        }
        acknowledgement_map
    }

    /// Handles completed renew acknowledgements by returning successfully
    /// renewed records to the set of in-flight records. Returns the number of
    /// records renewed.
    ///
    /// Mirrors Java's
    /// `int renew(Map<TopicIdPartition, Acknowledgements>, Optional<Integer>)`.
    ///
    /// # Errors
    ///
    /// Propagates the error from [`ShareInFlightBatch::renew`] (uncompleted
    /// acknowledgements). Panics-free: an acknowledgements entry for an unknown
    /// partition is ignored (Java would NPE on `batches.get(key).renew(...)`;
    /// callers always pass partitions present in `batches`).
    pub(crate) fn renew(
        &mut self,
        acknowledgements_map: &IndexMap<TopicIdPartition, Acknowledgements>,
        acquisition_lock_timeout_ms: Option<i32>,
    ) -> Result<i32, KafkaError> {
        let mut records_renewed = 0;
        for (tip, acknowledgements) in acknowledgements_map {
            if let Some(batch) = self.batches.get_mut(tip) {
                records_renewed += batch.renew(acknowledgements)?;
            }
        }
        self.acquisition_lock_timeout_ms_renewed = acquisition_lock_timeout_ms;
        Ok(records_renewed)
    }
}

impl<K, V> std::fmt::Debug for ShareFetch<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareFetch")
            .field("num_batches", &self.batches.len())
            .field("acquisition_lock_timeout_ms", &self.acquisition_lock_timeout_ms)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    // No dedicated Java test file exists for `ShareFetch`; its behaviour is
    // exercised by `ShareFetchCollectorTest` (translated in
    // `share_fetch_collector.rs`). These smoke tests cover the add / merge /
    // acknowledge / take / renew bookkeeping directly so the type is not left
    // untested, and verify the two Phase-2 ownership carryover notes.
    use super::*;
    use crate::common::{TopicPartition, Uuid};

    fn tip(topic: &str, partition: i32) -> TopicIdPartition {
        TopicIdPartition::new(Uuid::random_uuid(), TopicPartition::new(topic.to_string(), partition))
    }

    fn record(topic: &str, partition: i32, offset: i64) -> ConsumerRecord<String, String> {
        ConsumerRecord::new(topic, partition, offset, None, None)
    }

    fn batch_with_record(
        node_id: i32,
        tip: &TopicIdPartition,
        offset: i64,
        timeout: Option<i32>,
    ) -> ShareInFlightBatch<String, String> {
        let mut batch = ShareInFlightBatch::new(node_id, tip.clone(), timeout);
        batch.add_record(record(tip.topic(), tip.partition(), offset));
        batch
    }

    #[test]
    fn test_add_and_num_records() {
        let mut fetch: ShareFetch<String, String> = ShareFetch::empty();
        assert!(fetch.is_empty());
        let tp0 = tip("topic-a", 0);
        fetch.add(tp0.clone(), batch_with_record(0, &tp0, 0, Some(30_000)));
        assert_eq!(1, fetch.num_records());
        assert!(!fetch.is_empty());
        assert_eq!(Some(30_000), fetch.acquisition_lock_timeout_ms());
    }

    #[test]
    fn test_add_merges_same_partition() {
        // Carryover note #1: `add` reads the acquisition-lock timeout before the
        // consuming merge; the second batch's timeout wins.
        let mut fetch: ShareFetch<String, String> = ShareFetch::empty();
        let tp0 = tip("topic-a", 0);
        fetch.add(tp0.clone(), batch_with_record(0, &tp0, 0, Some(30_000)));
        fetch.add(tp0.clone(), batch_with_record(0, &tp0, 1, Some(20_000)));
        assert_eq!(2, fetch.num_records());
        assert_eq!(Some(20_000), fetch.acquisition_lock_timeout_ms());
    }

    #[test]
    fn test_records_borrowed_view() {
        let mut fetch: ShareFetch<String, String> = ShareFetch::empty();
        let tp0 = tip("topic-a", 0);
        fetch.add(tp0.clone(), batch_with_record(0, &tp0, 7, None));
        let records = fetch.records();
        let list = records.get(tp0.topic_partition()).expect("partition present");
        assert_eq!(1, list.len());
        assert_eq!(7, list[0].offset());
    }

    #[test]
    fn test_take_records_transfers_ownership() {
        let mut fetch: ShareFetch<String, String> = ShareFetch::empty();
        let tp0 = tip("topic-a", 0);
        fetch.add(tp0.clone(), batch_with_record(0, &tp0, 7, None));
        let taken = fetch.take_records();
        let list = taken.get(tp0.topic_partition()).expect("partition present");
        assert_eq!(1, list.len());
        assert_eq!(7, list[0].offset());
        // After draining, the batch no longer holds the records.
        assert_eq!(0, fetch.num_records());
    }

    #[test]
    fn test_acknowledge_matches_by_topic_partition() {
        let mut fetch: ShareFetch<String, String> = ShareFetch::empty();
        let tp0 = tip("topic-a", 0);
        fetch.add(tp0.clone(), batch_with_record(0, &tp0, 0, None));
        fetch.acknowledge(&record("topic-a", 0, 0), AcknowledgeType::Accept).unwrap();
        assert!(fetch.check_all_in_flight_are_acknowledged());
    }

    #[test]
    fn test_acknowledge_unknown_partition_is_illegal_state() {
        let mut fetch: ShareFetch<String, String> = ShareFetch::empty();
        let tp0 = tip("topic-a", 0);
        fetch.add(tp0.clone(), batch_with_record(0, &tp0, 0, None));
        let err = fetch
            .acknowledge(&record("other", 9, 0), AcknowledgeType::Accept)
            .expect_err("unknown partition must fail");
        assert!(err.to_string().contains("The record cannot be acknowledged."), "got: {err}");
    }

    #[test]
    fn test_renew_flow() {
        let mut fetch: ShareFetch<String, String> = ShareFetch::empty();
        let tp0 = tip("topic-a", 0);
        fetch.add(tp0.clone(), batch_with_record(0, &tp0, 0, Some(30_000)));
        fetch.acknowledge(&record("topic-a", 0, 0), AcknowledgeType::Renew).unwrap();
        assert!(!fetch.has_renewals());

        let acks_map = fetch.take_acknowledged_records();
        assert!(fetch.has_renewals());
        assert_eq!(0, fetch.num_records());

        let mut acks = acks_map.get(&tp0).expect("acks present").acknowledgements().clone();
        acks.complete(None);
        let mut renew_map: IndexMap<TopicIdPartition, Acknowledgements> = IndexMap::new();
        renew_map.insert(tp0.clone(), acks);
        let renewed = fetch.renew(&renew_map, Some(20_000)).unwrap();
        assert_eq!(1, renewed);
        assert!(fetch.has_renewals());

        fetch.take_renewed_records();
        assert!(!fetch.has_renewals());
        assert_eq!(1, fetch.num_records());
        assert_eq!(Some(20_000), fetch.acquisition_lock_timeout_ms());
    }

    #[test]
    fn test_acknowledge_on_exception() {
        use super::super::share_in_flight_batch_exception::ShareInFlightBatchException;
        use crate::common::protocol::errors::Errors;
        let mut fetch: ShareFetch<String, String> = ShareFetch::empty();
        let tp0 = tip("topic-a", 0);
        let mut batch = ShareInFlightBatch::new(0, tp0.clone(), None);
        batch.set_exception(ShareInFlightBatchException::new(
            KafkaError::new(Errors::InvalidRecordState),
            [5].into_iter().collect(),
        ));
        fetch.add(tp0.clone(), batch);
        fetch
            .acknowledge_on_exception("topic-a", 0, 5, AcknowledgeType::Release)
            .unwrap();
        let err = fetch
            .acknowledge_on_exception("topic-a", 0, 6, AcknowledgeType::Release)
            .expect_err("offset not covered by exception must fail");
        assert!(err.to_string().contains("The record cannot be acknowledged."), "got: {err}");
    }
}
