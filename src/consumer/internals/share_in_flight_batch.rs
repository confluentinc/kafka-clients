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

//! Per-acknowledge bookkeeping for a batch of records acquired from one
//! topic-partition by a share consumer (KIP-932).
//!
//! Corresponds to
//! `org.apache.kafka.clients.consumer.internals.ShareInFlightBatch`.

// Phase 2 (M9) lands this bookkeeping type; the share fetch path
// (`ShareCompletedFetch` / `ShareConsumeRequestManager`) that drives it
// arrives in a later phase.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::common::{KafkaError, TopicIdPartition};
use crate::consumer::{AcknowledgeType, ConsumerRecord};

use super::acknowledgements::Acknowledgements;
use super::share_in_flight_batch_exception::ShareInFlightBatchException;

/// Tracks the records acquired in a single share fetch for one
/// topic-partition, along with the acknowledgements made against them.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.ShareInFlightBatch<K, V>`.
///
/// This is per-acknowledge bookkeeping, not a per-record hot path, so normal
/// owned storage is used. `in_flight_records` and `acknowledged_records` use
/// ordered collections (`BTreeMap` / `BTreeSet`) to mirror Java's `TreeMap` /
/// `TreeSet` (offset-sorted iteration).
pub(crate) struct ShareInFlightBatch<K, V> {
    node_id: i32,
    partition: TopicIdPartition,
    in_flight_records: BTreeMap<i64, ConsumerRecord<K, V>>,
    /// The set of offsets that are in flight (delivered but not yet
    /// acknowledged-and-removed). This is the authoritative membership set for
    /// [`Self::acknowledge`] / [`Self::acknowledge_all`] /
    /// [`Self::check_all_in_flight_are_acknowledged`].
    ///
    /// # Deviation from Java
    ///
    /// Java's `ShareInFlightBatch` uses `inFlightRecords.keySet()` for these
    /// checks because its `ConsumerRecord`s are shared reference types:
    /// `ShareConsumerImpl.poll` hands the records to the user AND keeps them in
    /// `inFlightRecords`. In Rust `ConsumerRecord` is not `Clone` (receive-path
    /// zero-copy contract, §27), so `poll` MOVES the records out of the batch
    /// via [`Self::take_in_flight_records`]. This `in_flight_offsets` set
    /// survives that move, preserving offset-level in-flight tracking so
    /// `acknowledge` (called with the user's now-owned record) still succeeds
    /// and `check_all_in_flight_are_acknowledged` remains correct.
    in_flight_offsets: BTreeSet<i64>,
    /// Clones of records acknowledged with [`AcknowledgeType::Renew`], keyed by
    /// offset, captured at `acknowledge(_, RENEW)` time.
    ///
    /// # Why (RENEW re-delivery vs. zero-copy move-out)
    ///
    /// Java keeps every record object in `inFlightRecords` and, in
    /// `takeAcknowledgedRecords`, moves the RENEW-acked object into
    /// `renewingRecords` (`ShareInFlightBatch.java:124-127`); it is later
    /// re-delivered by `poll`. In Rust, `poll` MOVES record objects out of the
    /// batch to the user (`take_in_flight_records`, §27 zero-copy — no
    /// per-record clone), so the object is gone by the time the user
    /// acknowledges. To preserve Java's re-delivery, we capture a clone of the
    /// record **only** when it is RENEW-acked — the rare path — leaving the hot
    /// ACCEPT/RELEASE/REJECT path allocation-free (it records only the offset).
    /// `take_acknowledged_records` then routes these captured records into
    /// `renewing_records` exactly as Java routes `inFlightRecords`.
    renew_records: HashMap<i64, ConsumerRecord<K, V>>,
    /// Lazily created, mirroring Java's nullable `renewingRecords`.
    renewing_records: Option<HashMap<i64, ConsumerRecord<K, V>>>,
    /// Lazily created, mirroring Java's nullable `renewedRecords`.
    renewed_records: Option<HashMap<i64, ConsumerRecord<K, V>>>,
    acknowledged_records: BTreeSet<i64>,
    acknowledgements: Acknowledgements,
    acquisition_lock_timeout_ms: Option<i32>,
    exception: Option<ShareInFlightBatchException>,
    has_cached_exception: bool,
    check_for_renew_acknowledgements: bool,
}

impl<K, V> ShareInFlightBatch<K, V> {
    /// Constructs a new in-flight batch for the given node and partition.
    ///
    /// Mirrors Java's
    /// `ShareInFlightBatch(int nodeId, TopicIdPartition partition, Optional<Integer> acquisitionLockTimeoutMs)`.
    pub(crate) fn new(node_id: i32, partition: TopicIdPartition, acquisition_lock_timeout_ms: Option<i32>) -> Self {
        Self {
            node_id,
            partition,
            in_flight_records: BTreeMap::new(),
            in_flight_offsets: BTreeSet::new(),
            renew_records: HashMap::new(),
            renewing_records: None,
            renewed_records: None,
            acknowledged_records: BTreeSet::new(),
            acknowledgements: Acknowledgements::empty(),
            acquisition_lock_timeout_ms,
            exception: None,
            has_cached_exception: false,
            check_for_renew_acknowledgements: false,
        }
    }

    /// Adds an acknowledgement for an offset without requiring the record to
    /// be in flight. Mirrors Java's `addAcknowledgement(long, AcknowledgeType)`.
    pub(crate) fn add_acknowledgement(&mut self, offset: i64, ack_type: AcknowledgeType) {
        self.acknowledgements.add(offset, ack_type);
        if ack_type == AcknowledgeType::Renew {
            self.check_for_renew_acknowledgements = true;
        }
    }

    /// Acknowledges an in-flight record.
    ///
    /// Mirrors Java's `acknowledge(ConsumerRecord, AcknowledgeType)`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalState`] if the record is not in flight,
    /// matching Java's `IllegalStateException("The record cannot be acknowledged.")`.
    pub(crate) fn acknowledge(
        &mut self,
        record: &ConsumerRecord<K, V>,
        ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError>
    where
        K: Clone,
        V: Clone,
    {
        if self.in_flight_offsets.contains(&record.offset()) {
            self.acknowledgements.add(record.offset(), ack_type);
            self.acknowledged_records.insert(record.offset());
            if ack_type == AcknowledgeType::Renew {
                self.check_for_renew_acknowledgements = true;
                // Capture the record so it can be re-delivered after renewal
                // (the original object was moved out to the user by
                // `take_in_flight_records`). RENEW is the rare path; the hot
                // ACCEPT/RELEASE/REJECT path does not clone. See `renew_records`.
                if !self.in_flight_records.contains_key(&record.offset()) {
                    self.renew_records.insert(record.offset(), record.clone());
                }
            } else {
                // A non-RENEW ack for an offset previously RENEW-captured
                // supersedes the renewal — drop the captured clone.
                self.renew_records.remove(&record.offset());
            }
            return Ok(());
        }
        Err(KafkaError::illegal_state("The record cannot be acknowledged."))
    }

    /// Acknowledges all in-flight records with the given type (only those not
    /// already acknowledged). Mirrors Java's `acknowledgeAll(AcknowledgeType)`.
    pub(crate) fn acknowledge_all(&mut self, ack_type: AcknowledgeType) {
        let offsets: Vec<i64> = self.in_flight_offsets.iter().copied().collect();
        for offset in offsets {
            if self.acknowledgements.add_if_absent(offset, ack_type) {
                self.acknowledged_records.insert(offset);
            }
        }
        if ack_type == AcknowledgeType::Renew {
            self.check_for_renew_acknowledgements = true;
        }
    }

    /// Returns whether every in-flight record has been acknowledged.
    ///
    /// Mirrors Java's `checkAllInFlightAreAcknowledged()`.
    pub(crate) fn check_all_in_flight_are_acknowledged(&self) -> bool {
        self.in_flight_offsets.len() == self.acknowledged_records.len()
    }

    /// Adds a record to the in-flight set. Mirrors Java's `addRecord(ConsumerRecord)`.
    pub(crate) fn add_record(&mut self, record: ConsumerRecord<K, V>) {
        self.in_flight_offsets.insert(record.offset());
        self.in_flight_records.insert(record.offset(), record);
    }

    /// Records a gap in the acknowledgements at the given offset.
    ///
    /// Mirrors Java's `addGap(long)`.
    pub(crate) fn add_gap(&mut self, offset: i64) {
        self.acknowledgements.add_gap(offset);
    }

    /// Merges the in-flight records of `other` into this batch.
    ///
    /// Mirrors Java's `merge(ShareInFlightBatch<K, V> other)`.
    ///
    /// Deviation from Java: takes `other` by value and drains its in-flight
    /// records rather than copying references (`ConsumerRecord` is not
    /// `Clone`; see receive-path zero-copy contract). The net state is
    /// identical — Java discards `other` after `merge` in practice.
    pub(crate) fn merge(&mut self, other: ShareInFlightBatch<K, V>) {
        self.in_flight_offsets.extend(other.in_flight_offsets);
        self.in_flight_records.extend(other.in_flight_records);
        if other.check_for_renew_acknowledgements {
            self.check_for_renew_acknowledgements = true;
        }
    }

    /// Returns the in-flight records in offset order.
    ///
    /// Mirrors Java's package-private `List<ConsumerRecord<K, V>> getInFlightRecords()`.
    ///
    /// Deviation from Java: returns borrowed references rather than a fresh
    /// owned list (`ConsumerRecord` is not `Clone`; the records remain owned
    /// by this batch — receive-path zero-copy contract).
    pub(crate) fn get_in_flight_records(&self) -> Vec<&ConsumerRecord<K, V>> {
        self.in_flight_records.values().collect()
    }

    /// Drains the in-flight records out of the batch, transferring ownership
    /// to the caller in offset order.
    ///
    /// There is no direct Java analog. Java's `ShareFetch.records()` reads the
    /// in-flight records non-destructively (Java `ConsumerRecord`s are shared
    /// reference types). Because `ConsumerRecord` is not `Clone` in Rust
    /// (receive-path zero-copy contract, §27), delivering owned records to the
    /// user requires moving them out. Used by [`ShareFetch::take_records`].
    ///
    /// The `in_flight_offsets` set is intentionally NOT cleared — offset-level
    /// in-flight tracking survives the move so `acknowledge` (called with the
    /// user's now-owned record) and `check_all_in_flight_are_acknowledged`
    /// remain correct. See the `in_flight_offsets` field doc.
    ///
    /// [`ShareFetch::take_records`]: super::share_fetch::ShareFetch::take_records
    pub(crate) fn take_in_flight_records(&mut self) -> Vec<ConsumerRecord<K, V>> {
        std::mem::take(&mut self.in_flight_records).into_values().collect()
    }

    /// Number of in-flight records. Mirrors Java's package-private `numRecords()`.
    pub(crate) fn num_records(&self) -> usize {
        self.in_flight_records.len()
    }

    /// The node id from which this batch was fetched. Mirrors Java's
    /// package-private `nodeId()`.
    pub(crate) fn node_id(&self) -> i32 {
        self.node_id
    }

    /// The topic-partition this batch belongs to.
    pub(crate) fn partition(&self) -> &TopicIdPartition {
        &self.partition
    }

    /// Takes the acknowledged records out of the batch, returning the
    /// accumulated [`Acknowledgements`] and resetting the batch for further
    /// fetching. Records acknowledged with `RENEW` are moved to the renewing
    /// set for a subsequent [`Self::renew`] call.
    ///
    /// Mirrors Java's package-private `Acknowledgements takeAcknowledgedRecords()`.
    ///
    /// Deviation from Java: to avoid cloning non-`Clone` `ConsumerRecord`s,
    /// the acknowledged records are removed from `in_flight_records` and the
    /// `RENEW`-typed ones are moved into `renewing_records` in a single pass,
    /// rather than Java's copy-references-then-remove two-step. The resulting
    /// state is identical.
    pub(crate) fn take_acknowledged_records(&mut self) -> Acknowledgements {
        if self.check_for_renew_acknowledgements {
            if self.renewing_records.is_none() {
                self.renewing_records = Some(HashMap::new());
            }
            if self.renewed_records.is_none() {
                self.renewed_records = Some(HashMap::new());
            }
        }

        // Determine which acknowledged offsets were acknowledged with RENEW,
        // so those records can be routed into `renewing_records`.
        let renew_offsets: BTreeSet<i64> = if self.check_for_renew_acknowledgements {
            let ack_type_map = self.acknowledgements.get_acknowledgements_type_map();
            self.acknowledged_records
                .iter()
                .copied()
                .filter(|offset| ack_type_map.get(offset) == Some(&Some(AcknowledgeType::Renew)))
                .collect()
        } else {
            BTreeSet::new()
        };

        // Remove every acknowledged record from the in-flight set; RENEW ones
        // are moved into `renewing_records`, the rest are dropped. This is the
        // single-pass equivalent of Java's clear/remove step (the special-case
        // `clear()` when all records are acknowledged is purely an
        // optimisation with identical end state).
        let acknowledged: Vec<i64> = self.acknowledged_records.iter().copied().collect();
        for offset in acknowledged {
            // The offset leaves the in-flight set regardless of whether the
            // record object is still held (it may have been moved out to the
            // user via `take_in_flight_records`).
            self.in_flight_offsets.remove(&offset);
            // Remove the record from wherever it lives: still in-flight (batch
            // never drained) or the RENEW-captured clone (drained + RENEW-acked).
            let in_flight = self.in_flight_records.remove(&offset);
            let captured = self.renew_records.remove(&offset);
            if renew_offsets.contains(&offset)
                && let Some(record) = in_flight.or(captured)
                && let Some(renewing) = self.renewing_records.as_mut()
            {
                // `renewing_records` is `Some` because `check_for_renew_acknowledgements`
                // is set whenever a RENEW acknowledgement was recorded. Java
                // moves `inFlightRecords.get(offset)` here; Rust uses the
                // still-in-flight object if present, else the RENEW clone
                // captured at `acknowledge(_, RENEW)` time.
                renewing.insert(offset, record);
            }
        }
        self.acknowledged_records.clear();
        self.exception = None;

        // Java: `currentAcknowledgements = acknowledgements; acknowledgements = Acknowledgements.empty();`
        let current_acknowledgements = std::mem::replace(&mut self.acknowledgements, Acknowledgements::empty());
        self.check_for_renew_acknowledgements = false;
        current_acknowledgements
    }

    /// Processes the acknowledgements returned by the broker for renewing
    /// records, moving successfully renewed records into the renewed set.
    /// Returns the number of records renewed.
    ///
    /// Mirrors Java's package-private `int renew(Acknowledgements acknowledgements)`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalState`] if the acknowledgements are not
    /// completed, matching Java's `IllegalStateException("Renewing with uncompleted acknowledgements")`.
    pub(crate) fn renew(&mut self, acknowledgements: &Acknowledgements) -> Result<i32, KafkaError> {
        let mut records_renewed = 0;
        let is_completed_exceptionally = acknowledgements.is_completed_exceptionally();
        if acknowledgements.is_completed() {
            if self.renewing_records.is_some() {
                let ack_type_map = acknowledgements.get_acknowledgements_type_map().clone();
                for (offset, ack_type) in ack_type_map {
                    // The record is always removed from the renewing set (Java
                    // calls `renewingRecords.remove(offset)` unconditionally).
                    let record = self.renewing_records.as_mut().and_then(|r| r.remove(&offset));
                    if ack_type == Some(AcknowledgeType::Renew)
                        && let Some(record) = record
                        && !is_completed_exceptionally
                        && let Some(renewed) = self.renewed_records.as_mut()
                    {
                        // The record is moved into renewed state, and will then
                        // become in-flight later.
                        renewed.insert(offset, record);
                        records_renewed += 1;
                    }
                }
            }
        } else {
            return Err(KafkaError::illegal_state("Renewing with uncompleted acknowledgements"));
        }
        Ok(records_renewed)
    }

    /// Returns whether there are records pending renewal or already renewed.
    ///
    /// Mirrors Java's package-private `boolean hasRenewals()`.
    pub(crate) fn has_renewals(&self) -> bool {
        match &self.renewing_records {
            None => false,
            Some(renewing) => !renewing.is_empty() || self.renewed_records.as_ref().is_some_and(|r| !r.is_empty()),
        }
    }

    /// Moves renewed records back into the in-flight set.
    ///
    /// Mirrors Java's package-private `void takeRenewals()`.
    pub(crate) fn take_renewals(&mut self) {
        if let Some(renewed) = self.renewed_records.as_mut() {
            let drained: Vec<(i64, ConsumerRecord<K, V>)> = renewed.drain().collect();
            for (offset, _) in &drained {
                self.in_flight_offsets.insert(*offset);
            }
            self.in_flight_records.extend(drained);
        }
    }

    /// Returns the accumulated acknowledgements. Mirrors Java's package-private
    /// `Acknowledgements getAcknowledgements()`.
    pub(crate) fn get_acknowledgements(&self) -> &Acknowledgements {
        &self.acknowledgements
    }

    /// Returns the acquisition lock timeout, if any. Mirrors Java's
    /// package-private `Optional<Integer> getAcquisitionLockTimeoutMs()`.
    pub(crate) fn get_acquisition_lock_timeout_ms(&self) -> Option<i32> {
        self.acquisition_lock_timeout_ms
    }

    /// Returns whether the batch has neither in-flight records nor
    /// acknowledgements. Mirrors Java's `isEmpty()`.
    pub(crate) fn is_empty(&self) -> bool {
        self.in_flight_records.is_empty() && self.acknowledgements.is_empty()
    }

    /// Sets the cached deserialization exception. Mirrors Java's
    /// `setException(ShareInFlightBatchException)`.
    pub(crate) fn set_exception(&mut self, exception: ShareInFlightBatchException) {
        self.exception = Some(exception);
    }

    /// Returns the cached exception, if any. Mirrors Java's
    /// `getException()`.
    pub(crate) fn get_exception(&self) -> Option<&ShareInFlightBatchException> {
        self.exception.as_ref()
    }

    /// Sets whether a cached exception is present. Mirrors Java's
    /// `setHasCachedException(boolean)`.
    pub(crate) fn set_has_cached_exception(&mut self, has_cached_exception: bool) {
        self.has_cached_exception = has_cached_exception;
    }

    /// Returns whether a cached exception is present. Mirrors Java's
    /// `hasCachedException()`.
    pub(crate) fn has_cached_exception(&self) -> bool {
        self.has_cached_exception
    }
}

#[cfg(test)]
mod tests {
    // No dedicated Java test exists for `ShareInFlightBatch`; its behaviour is
    // exercised by `ShareConsumeRequestManagerTest` (translated in a later
    // phase). These smoke tests cover the core add/acknowledge/take/renew
    // bookkeeping so the type is not left untested in this phase.
    use super::*;
    use crate::common::{TopicPartition, Uuid};

    fn tip() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::random_uuid(), TopicPartition::new("t".to_string(), 0))
    }

    fn record(offset: i64) -> ConsumerRecord<String, String> {
        ConsumerRecord::new("t", 0, offset, None, None)
    }

    #[test]
    fn test_add_and_acknowledge() {
        let mut batch: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), None);
        batch.add_record(record(0));
        batch.add_record(record(1));
        assert_eq!(batch.num_records(), 2);
        assert_eq!(batch.node_id(), 1);
        assert!(!batch.check_all_in_flight_are_acknowledged());

        batch.acknowledge(&record(0), AcknowledgeType::Accept).unwrap();
        assert!(!batch.check_all_in_flight_are_acknowledged());
        batch.acknowledge(&record(1), AcknowledgeType::Accept).unwrap();
        assert!(batch.check_all_in_flight_are_acknowledged());
    }

    #[test]
    fn test_acknowledge_unknown_record_is_illegal_state() {
        let mut batch: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), None);
        batch.add_record(record(0));
        let err = batch
            .acknowledge(&record(5), AcknowledgeType::Accept)
            .expect_err("unknown record must fail");
        assert!(err.to_string().contains("The record cannot be acknowledged."), "got: {err}");
    }

    #[test]
    fn test_acknowledge_all() {
        let mut batch: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), None);
        batch.add_record(record(0));
        batch.add_record(record(1));
        batch.acknowledge_all(AcknowledgeType::Accept);
        assert!(batch.check_all_in_flight_are_acknowledged());
    }

    #[test]
    fn test_take_acknowledged_records_clears_in_flight() {
        let mut batch: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), None);
        batch.add_record(record(0));
        batch.add_record(record(1));
        batch.acknowledge_all(AcknowledgeType::Accept);
        let acks = batch.take_acknowledged_records();
        assert_eq!(acks.size(), 2);
        assert_eq!(batch.num_records(), 0);
        assert!(!batch.has_renewals());
    }

    #[test]
    fn test_take_acknowledged_records_keeps_unacknowledged() {
        let mut batch: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), None);
        batch.add_record(record(0));
        batch.add_record(record(1));
        batch.acknowledge(&record(0), AcknowledgeType::Accept).unwrap();
        batch.take_acknowledged_records();
        // Only offset 1 remains in flight.
        assert_eq!(batch.num_records(), 1);
        assert_eq!(batch.get_in_flight_records()[0].offset(), 1);
    }

    #[test]
    fn test_renew_flow() {
        let mut batch: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), None);
        batch.add_record(record(0));
        batch.acknowledge(&record(0), AcknowledgeType::Renew).unwrap();
        let _acks = batch.take_acknowledged_records();
        // Offset 0 was RENEW-acknowledged, so it moved to the renewing set.
        assert!(batch.has_renewals());
        assert_eq!(batch.num_records(), 0);

        let mut renew_acks = Acknowledgements::empty();
        renew_acks.add(0, AcknowledgeType::Renew);
        renew_acks.complete(None);
        let renewed = batch.renew(&renew_acks).unwrap();
        assert_eq!(renewed, 1);

        batch.take_renewals();
        assert_eq!(batch.num_records(), 1);
    }

    #[test]
    fn test_renew_uncompleted_is_illegal_state() {
        let mut batch: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), None);
        let acks = Acknowledgements::empty();
        let err = batch.renew(&acks).expect_err("uncompleted acknowledgements must fail");
        assert!(
            err.to_string().contains("Renewing with uncompleted acknowledgements"),
            "got: {err}"
        );
    }

    #[test]
    fn test_is_empty_and_gap() {
        let mut batch: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), None);
        assert!(batch.is_empty());
        batch.add_gap(0);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_take_in_flight_records_retains_offset_tracking() {
        // After the records are moved out to the user (drain path), the
        // offset-level in-flight tracking survives so acknowledge/check work.
        let mut batch: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), None);
        batch.add_record(record(0));
        batch.add_record(record(1));

        let taken = batch.take_in_flight_records();
        assert_eq!(taken.len(), 2, "records moved out to the user");
        assert_eq!(batch.num_records(), 0, "no record objects left in the batch");

        // The offsets are still tracked in-flight, so acknowledging the
        // user's now-owned records still succeeds.
        assert!(!batch.check_all_in_flight_are_acknowledged());
        batch.acknowledge(&record(0), AcknowledgeType::Accept).unwrap();
        assert!(!batch.check_all_in_flight_are_acknowledged());
        batch.acknowledge(&record(1), AcknowledgeType::Accept).unwrap();
        assert!(batch.check_all_in_flight_are_acknowledged());

        // take_acknowledged_records clears the offset tracking.
        let acks = batch.take_acknowledged_records();
        assert_eq!(acks.size(), 2);
        assert!(batch.check_all_in_flight_are_acknowledged(), "0 in-flight == 0 acked");
    }

    #[test]
    fn test_merge() {
        let mut batch: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), None);
        batch.add_record(record(0));
        let mut other: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), None);
        other.add_record(record(1));
        batch.merge(other);
        assert_eq!(batch.num_records(), 2);
    }

    #[test]
    fn test_exception_and_cached_exception() {
        use crate::common::protocol::errors::Errors;
        let mut batch: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), None);
        assert!(batch.get_exception().is_none());
        assert!(!batch.has_cached_exception());
        batch.set_exception(ShareInFlightBatchException::new(
            KafkaError::new(Errors::InvalidRecordState),
            [0].into_iter().collect(),
        ));
        assert!(batch.get_exception().is_some());
        batch.set_has_cached_exception(true);
        assert!(batch.has_cached_exception());
    }

    #[test]
    fn test_acquisition_lock_timeout() {
        let batch: ShareInFlightBatch<String, String> = ShareInFlightBatch::new(1, tip(), Some(30_000));
        assert_eq!(batch.get_acquisition_lock_timeout_ms(), Some(30_000));
    }
}
