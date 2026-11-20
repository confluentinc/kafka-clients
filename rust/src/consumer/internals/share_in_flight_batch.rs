// Copyright 2026 Confluent Inc.
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

//! `ShareInFlightBatch` — the records of one partition delivered to the
//! application by a share fetch, and their acknowledgements.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareInFlightBatch`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::common::Error;
use crate::common::errors::CorruptRecordError;
use crate::common::utils::internals::BufferSupplier;
use crate::common::{TopicIdPartition, TopicPartition};
use crate::consumer::AcknowledgeType;
use crate::consumer::ConsumerRecord;
use crate::consumer::internals::Acknowledgements;
use crate::consumer::internals::ConsumerShareInFlightBatchError;
use crate::consumer::internals::Deserializers;
use crate::consumer::internals::batch_cursor::{BatchCursor, parse_record};

/// The last sentence of a share fetch's deserialization error
/// (`ShareCompletedFetch#newRecordDeserializationException`).
pub(crate) const RECORD_RELEASED: &str = "The record has been released.";

/// The records of one partition that a share fetch delivered to the
/// application, keyed by offset, and the acknowledgements the application has
/// given them.
///
/// Corresponds to `org.apache.kafka.clients.consumer.internals.ShareInFlightBatch`.
///
/// # Record retention for RENEW (decision D5, option (c))
///
/// Java keeps the `ConsumerRecord` objects it hands to the application
/// (the fields `ShareInFlightBatch#inFlightRecords`,
/// `ShareInFlightBatch#renewingRecords` and `ShareInFlightBatch#renewedRecords`),
/// so a record acknowledged with `RENEW` returns to the in-flight set and the
/// next `poll` returns **the same object** (`ShareInFlightBatch#takeRenewals`). A
/// Rust `ConsumerRecords` owns its records and is consumed by value, so the
/// batch cannot keep them once they are handed out. Instead:
///
/// - the three Java maps are offset-keyed bookkeeping here (offset → delivery
///   count), with Java's transitions; acknowledging is offset-based anyway
///   (`ShareInFlightBatch#acknowledge` looks the record up by `record.offset()`);
/// - the batch keeps **one** refcounted handle (`Bytes`) on the raw record
///   batches its records came from, never a per-record copy;
/// - a renewed record is delivered again by decoding it from those bytes
///   (decompressing up to it when its batch is compressed) and running the
///   deserializers again.
///
/// **That whole scheme lives behind one seam,
/// [`Self::redelivery_walk`].** Nothing outside this type — `ShareFetch`, the
/// request manager, `poll` — knows how a redelivered record is produced, so
/// the scheme can change by touching only this type and its tests.
///
/// Deviations from Java 4.4 (those three fields and
/// `ShareInFlightBatch#takeRenewals`), both following from the above:
///
/// - the deserializers run again for a renewed record, so a deserializer
///   that counts or caches sees the record once per delivery; a failure then
///   takes the per-record deserialization-error path a first delivery takes
///   (the record is released, see [`Self::take_renewals`]);
/// - a redelivered record is a new value, equal in content to the first, not
///   the same object.
///
/// **Memory.** A `Bytes` slice keeps its whole allocation alive, and a fetch
/// response's record batches are all slices of one response buffer
/// (`ShareFetchResponseData.PartitionData.records`, read with
/// `BytesReader::read_bytes_owned`). Once a batch's only remaining in-flight
/// records are renewed ones, it copies the record batches holding them into a
/// buffer of its own and lets go of the response buffer
/// ([`Self::take_acknowledged_records`]), so a record renewed for a long time
/// pins only its own record batch. The copy happens only on the renew path.
/// When nothing is in flight or renewing any more, the bytes are dropped.
///
/// **Delivery.** [`Self::in_flight_records`] hands each decoded record out
/// once, where Java's `getInFlightRecords` returns every in-flight record
/// each time. At the only production call site (`ShareConsumerImpl#poll`, via
/// `ShareFetch#records`) every record a previous `poll` returned has already
/// been acknowledged and taken (`acknowledgeBatchIfImplicitAcknowledgement` /
/// `ensureInFlightAcknowledgedIfExplicitAcknowledgement`, then
/// `takeAcknowledgedRecords`), so the records Java returns there are exactly
/// the ones not yet handed out.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch")]
pub(crate) struct ShareInFlightBatch<K, V> {
    node_id: i32,
    partition: TopicIdPartition,
    /// Java's `inFlightRecords` (`TreeMap<Long, ConsumerRecord>`): the offset
    /// and delivery count of each record in flight.
    in_flight_records: BTreeMap<i64, Option<i16>>,
    /// The in-flight records decoded and not yet handed out by
    /// [`Self::in_flight_records`], in offset order.
    undelivered_records: Vec<ConsumerRecord<K, V>>,
    /// Java's `renewingRecords`; `None` until the first RENEW is taken.
    renewing_records: Option<HashMap<i64, Option<i16>>>,
    /// Java's `renewedRecords`; `None` until the first RENEW is taken.
    renewed_records: Option<HashMap<i64, Option<i16>>>,
    acknowledged_records: BTreeSet<i64>,
    acknowledgements: Acknowledgements,
    acquisition_lock_timeout_ms: Option<i32>,
    error: Option<ConsumerShareInFlightBatchError>,
    has_cached_error: bool,
    check_for_renew_acknowledgements: bool,
    /// The raw record batches the records were decoded from: the response's
    /// buffer, or an owned copy once only renewed records remain. `None` for a
    /// batch built record by record ([`Self::add_record`] alone), or once
    /// nothing is left to redeliver.
    record_batches: Option<bytes::Bytes>,
    /// Whether `record_batches` is the batch's own copy.
    record_batches_owned: bool,
    /// A renewed record whose redelivery failed after others in the same call
    /// were redelivered: it is released on the next [`Self::take_renewals`],
    /// as `ShareCompletedFetch` releases a record whose first delivery failed
    /// after others (`cachedRecordException`).
    cached_redelivery_error: Option<(i64, Error)>,
    /// What [`ShareCompletedFetch`](super::ShareCompletedFetch) reads about
    /// this batch, from the background task as well.
    status: Arc<ShareInFlightBatchStatus>,
    /// How many record batches redelivery has decoded, for the test that a
    /// batch is walked once however many of its records are renewed.
    #[cfg(test)]
    redelivery_batches_installed: usize,
}

/// What a [`ShareCompletedFetch`](super::ShareCompletedFetch) needs to know
/// about the batch it delivered last: how many records are in flight and
/// whether any are being renewed.
///
/// **Rust-only (DoD #7).** Java's `ShareCompletedFetch#deliveredBatch` is a
/// reference to the `ShareInFlightBatch` itself, which `ShareFetch` owns on the
/// application thread and `ShareFetchBuffer#bufferedPartitions` reads through
/// `ShareCompletedFetch#hasPendingAcknowledgements` on the background thread.
/// The batch cannot be shared that way in Rust, so it publishes the two values
/// `hasPendingAcknowledgements` reads (`numRecords()`, `hasRenewals()`) here,
/// after every change, and the fetch keeps this instead.
#[derive(Debug, Default)]
pub(crate) struct ShareInFlightBatchStatus {
    num_records: AtomicUsize,
    has_renewals: AtomicBool,
}

impl ShareInFlightBatchStatus {
    /// Java's `deliveredBatch.numRecords()`.
    pub(crate) fn num_records(&self) -> usize {
        self.num_records.load(Ordering::Acquire)
    }

    /// Java's `deliveredBatch.hasRenewals()`.
    pub(crate) fn has_renewals(&self) -> bool {
        self.has_renewals.load(Ordering::Acquire)
    }
}

impl<K, V> ShareInFlightBatch<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// An empty batch for `partition`, fetched from `node_id`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#ShareInFlightBatch")]
    pub(crate) fn new(node_id: i32, partition: TopicIdPartition, acquisition_lock_timeout_ms: Option<i32>) -> Self {
        Self {
            node_id,
            partition,
            in_flight_records: BTreeMap::new(),
            undelivered_records: Vec::new(),
            renewing_records: None,
            renewed_records: None,
            acknowledged_records: BTreeSet::new(),
            acknowledgements: Acknowledgements::empty(),
            acquisition_lock_timeout_ms,
            error: None,
            has_cached_error: false,
            check_for_renew_acknowledgements: false,
            record_batches: None,
            record_batches_owned: false,
            cached_redelivery_error: None,
            status: Arc::new(ShareInFlightBatchStatus::default()),
            #[cfg(test)]
            redelivery_batches_installed: 0,
        }
    }

    /// Gives the batch the raw record batches its records are decoded from, so
    /// that a renewed record can be delivered again ([`Self::redelivery_walk`]).
    /// `ShareCompletedFetch` calls it with the partition's records; nothing is
    /// copied.
    ///
    /// Rust-only: Java keeps the record objects instead (see the type's doc).
    pub(crate) fn set_record_batches(&mut self, record_batches: bytes::Bytes) {
        self.record_batches = Some(record_batches);
        self.record_batches_owned = false;
    }

    /// Records an acknowledgement for `offset`, whether or not it is in flight.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#addAcknowledgement")]
    pub(crate) fn add_acknowledgement(&mut self, offset: i64, r#type: AcknowledgeType) {
        self.acknowledgements.add(offset, Some(r#type));
        if r#type == AcknowledgeType::Renew {
            self.check_for_renew_acknowledgements = true;
        }
    }

    /// Acknowledges an in-flight record.
    ///
    /// # Errors
    ///
    /// `IllegalStateError("The record cannot be acknowledged.")` when the
    /// record is not in flight in this batch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#acknowledge")]
    pub(crate) fn acknowledge(&mut self, record: &ConsumerRecord<K, V>, r#type: AcknowledgeType) -> Result<(), Error> {
        if self.in_flight_records.contains_key(&record.offset()) {
            self.acknowledgements.add(record.offset(), Some(r#type));
            self.acknowledged_records.insert(record.offset());
            if r#type == AcknowledgeType::Renew {
                self.check_for_renew_acknowledgements = true;
            }
            return Ok(());
        }
        Err(Error::local_illegal_state("The record cannot be acknowledged."))
    }

    /// Acknowledges every in-flight record not acknowledged yet.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#acknowledgeAll")]
    pub(crate) fn acknowledge_all(&mut self, r#type: AcknowledgeType) {
        for &offset in self.in_flight_records.keys() {
            if self.acknowledgements.add_if_absent(offset, r#type) {
                self.acknowledged_records.insert(offset);
            }
        }
        if r#type == AcknowledgeType::Renew {
            self.check_for_renew_acknowledgements = true;
        }
    }

    /// Whether every in-flight record has been acknowledged.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#checkAllInFlightAreAcknowledged")]
    pub(crate) fn check_all_in_flight_are_acknowledged(&self) -> bool {
        self.in_flight_records.len() == self.acknowledged_records.len()
    }

    /// Adds a decoded record to the in-flight set, to be handed out by
    /// [`Self::in_flight_records`].
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#addRecord")]
    pub(crate) fn add_record(&mut self, record: ConsumerRecord<K, V>) {
        let offset = record.offset();
        let delivery_count = record.delivery_count();
        // Java's `inFlightRecords.put` replaces a record at the same offset.
        if self.in_flight_records.insert(offset, delivery_count).is_some() {
            self.undelivered_records.retain(|r| r.offset() != offset);
        }
        match self.undelivered_records.last() {
            Some(last) if last.offset() > offset => {
                let at = self.undelivered_records.partition_point(|r| r.offset() < offset);
                self.undelivered_records.insert(at, record);
            },
            _ => self.undelivered_records.push(record),
        }
        self.publish_status();
    }

    /// Records a gap: an acquired offset with no record to deliver.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#addGap")]
    pub(crate) fn add_gap(&mut self, offset: i64) {
        self.acknowledgements.add_gap(offset);
    }

    /// Whether the record at `offset` is in flight.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#isInFlight")]
    pub(crate) fn is_in_flight(&self, offset: i64) -> bool {
        self.in_flight_records.contains_key(&offset)
    }

    /// Hands out the in-flight records not handed out before, in offset order.
    ///
    /// Java's `getInFlightRecords` returns every in-flight record each time;
    /// see the type's doc for why returning each once is equivalent where
    /// production calls it.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#getInFlightRecords")]
    pub(crate) fn in_flight_records(&mut self) -> Vec<ConsumerRecord<K, V>> {
        std::mem::take(&mut self.undelivered_records)
    }

    /// The number of records in flight.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#numRecords")]
    pub(crate) fn num_records(&self) -> usize {
        self.in_flight_records.len()
    }

    /// The node the records were fetched from.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#nodeId")]
    pub(crate) fn node_id(&self) -> i32 {
        self.node_id
    }

    /// Removes the acknowledged records from the in-flight set and returns
    /// their acknowledgements. Records acknowledged with `RENEW` move to the
    /// renewing set.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#takeAcknowledgedRecords")]
    pub(crate) fn take_acknowledged_records(&mut self) -> Acknowledgements {
        if self.check_for_renew_acknowledgements {
            let renewing = self.renewing_records.get_or_insert_with(HashMap::new);
            self.renewed_records.get_or_insert_with(HashMap::new);
            let ack_type_map = self.acknowledgements.acknowledgements_type_map();
            for offset in &self.acknowledged_records {
                if ack_type_map.get(offset) == Some(&Some(AcknowledgeType::Renew)) {
                    renewing.insert(*offset, self.in_flight_records.get(offset).copied().flatten());
                }
            }
        }
        // Usually, all records will be acknowledged, so we can just clear the in-flight records leaving
        // an empty batch, which will trigger more fetching
        if self.acknowledged_records.len() == self.in_flight_records.len() {
            self.in_flight_records.clear();
        } else {
            for offset in &self.acknowledged_records {
                self.in_flight_records.remove(offset);
            }
        }
        // A record `acknowledge_all` acknowledged before it was handed out is
        // taken without being handed out.
        if !self.undelivered_records.is_empty() {
            let in_flight = &self.in_flight_records;
            self.undelivered_records.retain(|r| in_flight.contains_key(&r.offset()));
        }
        self.acknowledged_records.clear();
        self.error = None;
        let current_acknowledgements = std::mem::replace(&mut self.acknowledgements, Acknowledgements::empty());
        self.check_for_renew_acknowledgements = false;
        self.retain_record_batches_for_redelivery();
        self.publish_status();
        current_acknowledgements
    }

    /// Moves the records whose RENEW `acknowledgements` confirmed from the
    /// renewing set to the renewed set, and returns how many.
    ///
    /// # Errors
    ///
    /// `IllegalStateError("Renewing with uncompleted acknowledgements")` when
    /// `acknowledgements` is not completed.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#renew")]
    pub(crate) fn renew(&mut self, acknowledgements: &Acknowledgements) -> Result<usize, Error> {
        let mut records_renewed = 0;
        let is_completed_with_error = acknowledgements.is_completed_with_error();
        if !acknowledgements.is_completed() {
            return Err(Error::local_illegal_state("Renewing with uncompleted acknowledgements"));
        }
        if let (Some(renewing), Some(renewed)) = (self.renewing_records.as_mut(), self.renewed_records.as_mut()) {
            for (&offset, &ack_type) in acknowledgements.acknowledgements_type_map() {
                let record = renewing.remove(&offset);
                if ack_type == Some(AcknowledgeType::Renew)
                    && let Some(delivery_count) = record
                    && !is_completed_with_error
                {
                    // The record is moved into renewed state, and will then become in-flight later.
                    renewed.insert(offset, delivery_count);
                    records_renewed += 1;
                }
            }
        }
        self.retain_record_batches_for_redelivery();
        self.publish_status();
        Ok(records_renewed)
    }

    /// Whether any record is being renewed, or renewed and waiting to be
    /// delivered again.
    ///
    /// Also `true` while a redelivery failure waits to be reported
    /// ([`Self::take_renewals`]), so that the next `poll` takes the renewals
    /// again and reports it, as Java reports a cached record failure on the
    /// next fetch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#hasRenewals")]
    pub(crate) fn has_renewals(&self) -> bool {
        let (Some(renewing), Some(renewed)) = (&self.renewing_records, &self.renewed_records) else {
            return false;
        };
        !renewing.is_empty() || !renewed.is_empty() || self.cached_redelivery_error.is_some()
    }

    /// Moves the renewed records back into the in-flight set, decoding each
    /// again for delivery ([`Self::redelivery_walk`]), in offset order, in one
    /// pass over the record batches.
    ///
    /// Java moves the record objects (`inFlightRecords.putAll(renewedRecords)`).
    /// A record whose redelivery fails takes the path a record whose first
    /// delivery fails takes in `ShareCompletedFetch#fetchRecords`:
    ///
    /// - if no record was redelivered before it in this call, it is released
    ///   (acknowledged `RELEASE`, Java's `SerializationException` catch), the
    ///   batch carries a [`ConsumerShareInFlightBatchError`] naming its offset, and its
    ///   error is returned; the remaining renewed records wait for the next
    ///   call;
    /// - otherwise the records before it are delivered, and the failure is kept
    ///   for the next call (`cachedRecordException`), which releases the
    ///   record and returns the error before redelivering anything more.
    ///
    /// Returns the error of a record released in this call, which `ShareFetch`
    /// reports or swallows as `ShareFetchCollector#collect` does.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#takeRenewals")]
    pub(crate) async fn take_renewals(&mut self, deserializers: &Deserializers<K, V>) -> Option<Error> {
        if let Some((offset, cause)) = self.cached_redelivery_error.take() {
            self.release_after_failed_delivery(offset, cause.clone());
            self.publish_status();
            return Some(cause);
        }
        let renewed = self.renewed_records.as_ref()?;
        if renewed.is_empty() {
            return None;
        }
        // A snapshot: each offset leaves `renewed` only once its redelivery has
        // resolved, so a call dropped mid-way leaves every record it did not
        // redeliver still renewed for the next call.
        let mut offsets: Vec<(i64, Option<i16>)> = renewed.iter().map(|(&offset, &count)| (offset, count)).collect();
        offsets.sort_unstable_by_key(|&(offset, _)| offset);
        let mut walk = self.redelivery_walk();
        let mut redelivered = 0;
        let mut released = None;
        for (offset, delivery_count) in offsets {
            let result = walk.redeliver_record(offset, delivery_count, deserializers).await;
            if let Some(renewed) = self.renewed_records.as_mut() {
                renewed.remove(&offset);
            }
            match result {
                Ok(record) => {
                    self.add_record(record);
                    redelivered += 1;
                },
                Err(cause) if redelivered == 0 => {
                    self.release_after_failed_delivery(offset, cause.clone());
                    released = Some(cause);
                    break;
                },
                Err(cause) => {
                    self.cached_redelivery_error = Some((offset, cause));
                    self.has_cached_error = true;
                    break;
                },
            }
        }
        #[cfg(test)]
        {
            self.redelivery_batches_installed += walk.batches_installed;
        }
        self.publish_status();
        released
    }

    /// The acknowledgements given so far.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#getAcknowledgements")]
    pub(crate) fn acknowledgements(&self) -> &Acknowledgements {
        &self.acknowledgements
    }

    /// The acquisition lock timeout the broker sent with the records.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#getAcquisitionLockTimeoutMs")]
    pub(crate) fn acquisition_lock_timeout_ms(&self) -> Option<i32> {
        self.acquisition_lock_timeout_ms
    }

    /// Whether nothing is in flight and nothing is acknowledged.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#isEmpty")]
    pub(crate) fn is_empty(&self) -> bool {
        self.in_flight_records.is_empty() && self.acknowledgements.is_empty()
    }

    /// Sets the failure that ended the decoding of this batch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#setException")]
    pub(crate) fn set_error(&mut self, error: ConsumerShareInFlightBatchError) {
        self.error = Some(error);
    }

    /// The failure that ended the decoding of this batch, if any.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#getException")]
    pub(crate) fn error(&self) -> Option<&ConsumerShareInFlightBatchError> {
        self.error.as_ref()
    }

    /// Sets whether a failure was kept for the next fetch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#setHasCachedException")]
    pub(crate) fn set_has_cached_error(&mut self, has_cached_error: bool) {
        self.has_cached_error = has_cached_error;
    }

    /// Whether a failure was kept for the next fetch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatch#hasCachedException")]
    pub(crate) fn has_cached_error(&self) -> bool {
        self.has_cached_error
    }

    /// What a `ShareCompletedFetch` keeps about the batch it delivered last.
    pub(crate) fn status(&self) -> Arc<ShareInFlightBatchStatus> {
        Arc::clone(&self.status)
    }

    /// Whether the batch holds a buffer of its own for its renewed records
    /// rather than a slice of the fetch response.
    #[cfg(test)]
    pub(crate) fn holds_own_record_batches(&self) -> bool {
        self.record_batches_owned
    }

    /// Releases `offset`, whose delivery failed with `cause`: Java's
    /// `inFlightBatch.addAcknowledgement(offset, RELEASE)` and
    /// `setException(new ShareInFlightBatchException(cause, Set.of(offset)))`.
    fn release_after_failed_delivery(&mut self, offset: i64, cause: Error) {
        self.add_acknowledgement(offset, AcknowledgeType::Release);
        self.set_error(ConsumerShareInFlightBatchError::new(cause, HashSet::from([offset])));
    }

    /// **The redelivery seam** (decision D5): a walk over the batch's raw
    /// record batches that hands back in-flight records for delivery again, as
    /// new values decoded from those bytes and deserialized again, with the
    /// delivery count they were first delivered with
    /// ([`RedeliveryWalk::redeliver_record`]). The offsets are taken in
    /// ascending order, so each record batch is read once, and a compressed one
    /// decompressed once up to its last renewed record, however many of its
    /// records are renewed.
    ///
    /// Everything about how a renewed record is kept and produced is here, in
    /// [`RedeliveryWalk`] and in [`Self::retain_record_batches_for_redelivery`];
    /// see the type's doc.
    fn redelivery_walk(&self) -> RedeliveryWalk {
        RedeliveryWalk {
            cursor: self
                .record_batches
                .clone()
                .map(|record_batches| BatchCursor::new(record_batches, corrupt_batch_error, share_stream_error)),
            partition: self.partition.topic_partition().clone(),
            topic: Arc::from(self.partition.topic()),
            // A buffer of the codec's size per stream, returned when it is dropped.
            buffer_supplier: Arc::new(BufferSupplier::no_caching()),
            #[cfg(test)]
            batches_installed: 0,
        }
    }

    /// Applies the memory policy for the raw record batches (see the type's
    /// doc): kept as the response's buffer while any record that was not
    /// renewed is in flight; copied into a buffer of the batch's own, holding
    /// only the record batches the renewing and renewed records are in, once
    /// those are all that remain; dropped once nothing remains.
    fn retain_record_batches_for_redelivery(&mut self) {
        let Some(record_batches) = self.record_batches.as_ref() else {
            return;
        };
        if !self.in_flight_records.is_empty() {
            return;
        }
        let renewing = self.renewing_records.iter().flat_map(HashMap::keys);
        let renewed = self.renewed_records.iter().flat_map(HashMap::keys);
        let cached = self.cached_redelivery_error.iter().map(|(offset, _)| offset);
        let held: Vec<i64> = renewing.chain(renewed).chain(cached).copied().collect();
        if held.is_empty() {
            self.record_batches = None;
            self.record_batches_owned = false;
            return;
        }
        if self.record_batches_owned {
            return;
        }
        let (Some(&first), Some(&last)) = (held.iter().min(), held.iter().max()) else {
            return;
        };
        // The span of the record batches holding `first` to `last`. The bytes were
        // checked when the records were first delivered.
        let partition = self.partition.topic_partition();
        let mut cursor = BatchCursor::new(record_batches.clone(), corrupt_batch_error, share_stream_error);
        let mut span: Option<(usize, usize)> = None;
        // A batch the fetch refused (and moved past) holds nothing to
        // redeliver and is skipped; a batch whose size is unusable ends the
        // walk, as it ended the fetch.
        loop {
            let meta = match cursor.next_batch_moving_past_refused(partition, false) {
                Ok(Some((meta, _, _))) => meta,
                Err((_, Some(_))) => continue,
                Ok(None) | Err((_, None)) => break,
            };
            if meta.last_offset >= first && meta.base_offset <= last {
                span = Some((span.map_or(meta.batch_start, |(start, _)| start), meta.batch_end));
            }
        }
        let copy = match span {
            Some((start, end)) => bytes::Bytes::copy_from_slice(&record_batches[start..end]),
            None => bytes::Bytes::new(),
        };
        self.record_batches = Some(copy);
        self.record_batches_owned = true;
    }

    /// Publishes what `ShareCompletedFetch#hasPendingAcknowledgements` reads.
    fn publish_status(&self) {
        self.status.num_records.store(self.in_flight_records.len(), Ordering::Release);
        self.status.has_renewals.store(self.has_renewals(), Ordering::Release);
    }
}

impl<K, V> std::fmt::Debug for ShareInFlightBatch<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareInFlightBatch")
            .field("node_id", &self.node_id)
            .field("partition", &self.partition)
            .field("in_flight_records", &self.in_flight_records)
            .field("acknowledgements", &self.acknowledgements)
            .finish_non_exhaustive()
    }
}

/// One pass over an in-flight batch's raw record batches, delivering renewed
/// records again in ascending offset order (the D5 seam, see
/// [`ShareInFlightBatch::redelivery_walk`]). A private helper of the seam.
struct RedeliveryWalk {
    /// `None` when the batch holds no record batches any more.
    cursor: Option<BatchCursor>,
    partition: TopicPartition,
    topic: Arc<str>,
    buffer_supplier: Arc<BufferSupplier>,
    #[cfg(test)]
    batches_installed: usize,
}

impl RedeliveryWalk {
    /// Decodes the record at `offset`, which must follow the one this walk
    /// decoded before, and runs the deserializers over it.
    ///
    /// # Errors
    ///
    /// The deserializers' failure, as Java's `RecordDeserializationException`
    /// ("... The record has been released."), a decoding failure, or an
    /// illegal-state error when the record is not in the bytes. The record
    /// batches were checked when the record was first delivered, so their
    /// checksum is not checked again.
    async fn redeliver_record<K, V>(
        &mut self,
        offset: i64,
        delivery_count: Option<i16>,
        deserializers: &Deserializers<K, V>,
    ) -> Result<ConsumerRecord<K, V>, Error>
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        self.step_to(offset)?;
        let Some(cursor) = self.cursor.as_ref() else {
            return Err(self.not_found(offset));
        };
        let (record, _, _) = parse_record(
            cursor,
            &self.partition,
            &self.topic,
            deserializers.key_deserializer(),
            deserializers.value_deserializer(),
            delivery_count,
            RECORD_RELEASED,
        )
        .await?;
        Ok(record)
    }

    /// Moves the cursor onto the record at `offset`, reading on from where it
    /// is: within the installed record batch, or through the batches after it.
    fn step_to(&mut self, offset: i64) -> Result<(), Error> {
        let Some(cursor) = self.cursor.as_mut() else {
            return Err(self.not_found(offset));
        };
        loop {
            let in_current_batch = cursor.records_remaining > 0
                && cursor.current_batch.as_ref().is_some_and(|batch| batch.last_offset >= offset);
            if in_current_batch {
                cursor.read_current_record(&self.partition)?;
                let (record_offset, size) = match cursor.peek_current_record(&self.partition)? {
                    Some((record, _)) => (record.offset(), record.size_in_bytes() as usize),
                    None => break,
                };
                if record_offset == offset {
                    return Ok(());
                }
                if record_offset > offset {
                    break;
                }
                cursor.advance_record_cursor(size);
                continue;
            }
            cursor.close_record_stream();
            cursor.records_remaining = 0;
            // A batch the fetch refused (and moved past) holds nothing to
            // redeliver and is skipped; one whose size is unusable ends the walk.
            let (meta, source, records_count) = match cursor.next_batch_moving_past_refused(&self.partition, false) {
                Ok(Some(batch)) => batch,
                Err((_, Some(_))) => continue,
                Ok(None) => break,
                Err((error, None)) => return Err(error),
            };
            if meta.last_offset < offset || meta.is_control_batch {
                continue;
            }
            // Installed even when it starts past `offset`, for the offsets after it.
            cursor.install_batch(&self.partition, meta, source, records_count, &self.buffer_supplier)?;
            #[cfg(test)]
            {
                self.batches_installed += 1;
            }
        }
        Err(self.not_found(offset))
    }

    fn not_found(&self, offset: i64) -> Error {
        Error::local_illegal_state(format!(
            "The renewed record at offset {offset} of partition {} is no longer held for redelivery",
            self.partition
        ))
    }
}

/// A compressed batch's failing stream in a share fetch: the bare
/// `KafkaException` Java's stream throws
/// (`DefaultRecordBatch.StreamRecordIterator#readNext`), which escapes
/// `ShareCompletedFetch#fetchRecords`.
pub(crate) fn share_stream_error(_partition: &TopicPartition, _base_offset: i64, cause: &str) -> Error {
    Error::kafka_message(cause)
}

/// Java's `ShareCompletedFetch#maybeEnsureValid(RecordBatch, boolean)`:
/// `new CorruptRecordException("Record batch for partition " + partition +
/// " at offset " + batch.baseOffset() + " is invalid, cause: " + e.getMessage())`.
pub(crate) fn corrupt_batch_error(partition: &TopicPartition, base_offset: i64, cause: &str) -> Error {
    Error::CorruptRecord(CorruptRecordError::new(format!(
        "Record batch for partition {partition} at offset {base_offset} is invalid, cause: {cause}"
    )))
}

#[cfg(test)]
mod tests {
    //! `ShareInFlightBatch` has no Java test of its own: `ShareCompletedFetchTest`,
    //! `ShareFetchTest` and `ShareFetchCollectorTest` cover it. These tests pin
    //! the Java contract those do not reach (the error texts) and the D5
    //! retention scheme, which has no Java counterpart.

    use std::sync::Mutex;
    use std::sync::atomic::AtomicU32;

    use futures_util::FutureExt;

    use super::*;
    use crate::common::compress::Compression;
    use crate::common::serialization::{Deserializer, StringDeserializer};
    use crate::consumer::internals::share_completed_fetch::tests::{
        acquired_records, fetch, new_records, new_records_compressed, new_share_completed_fetch,
        new_string_deserializers, partition_data, tip,
    };

    fn record(offset: i64) -> ConsumerRecord<String, String> {
        ConsumerRecord::new(
            "test",
            0,
            offset,
            Some(format!("key-{offset}")),
            Some(format!("value-{offset}")),
        )
    }

    fn take_renewals_now<K: Send + 'static, V: Send + 'static>(
        batch: &mut ShareInFlightBatch<K, V>,
        deserializers: &Deserializers<K, V>,
    ) -> Option<Error> {
        batch
            .take_renewals(deserializers)
            .now_or_never()
            .expect("the test deserializers are always ready")
    }

    /// Acknowledges `renew` with RENEW and every other in-flight record with
    /// ACCEPT, takes the acknowledgements, completes them and renews.
    fn renew<K: Send + 'static, V: Send + 'static>(
        batch: &mut ShareInFlightBatch<K, V>,
        delivered: &[ConsumerRecord<K, V>],
        renew: &[i64],
    ) -> usize {
        for record in delivered {
            let r#type = if renew.contains(&record.offset()) {
                AcknowledgeType::Renew
            } else {
                AcknowledgeType::Accept
            };
            batch.acknowledge(record, r#type).expect("the record is in flight");
        }
        let mut acknowledgements = batch.take_acknowledged_records();
        acknowledgements.complete(None);
        batch.renew(&acknowledgements).expect("the acknowledgements are completed")
    }

    #[test]
    fn test_acknowledge_a_record_not_in_flight_fails() {
        let mut batch = ShareInFlightBatch::<String, String>::new(0, tip(), None);
        batch.add_record(record(0));
        assert!(batch.is_in_flight(0));
        assert!(!batch.is_in_flight(1));
        assert_eq!(0, batch.node_id());
        let error = batch.acknowledge(&record(1), AcknowledgeType::Accept).unwrap_err();
        assert!(matches!(error, Error::LocalIllegalState(_)), "{error:?}");
        assert_eq!("The record cannot be acknowledged.", error.message());
        assert!(!batch.check_all_in_flight_are_acknowledged());
        batch
            .acknowledge(&record(0), AcknowledgeType::Accept)
            .expect("the record is in flight");
        assert!(batch.check_all_in_flight_are_acknowledged());
    }

    #[test]
    fn test_acknowledge_all_does_not_overwrite() {
        let mut batch = ShareInFlightBatch::<String, String>::new(0, tip(), None);
        batch.add_record(record(0));
        batch.add_record(record(1));
        batch
            .acknowledge(&record(0), AcknowledgeType::Reject)
            .expect("the record is in flight");
        batch.acknowledge_all(AcknowledgeType::Accept);
        assert!(batch.check_all_in_flight_are_acknowledged());
        let acknowledgements = batch.take_acknowledged_records();
        assert_eq!(Some(AcknowledgeType::Reject), acknowledgements.get(0));
        assert_eq!(Some(AcknowledgeType::Accept), acknowledgements.get(1));
        assert!(batch.is_empty());
    }

    #[test]
    fn test_renew_with_uncompleted_acknowledgements_fails() {
        let mut batch = ShareInFlightBatch::<String, String>::new(0, tip(), None);
        let error = batch.renew(&Acknowledgements::empty()).unwrap_err();
        assert!(matches!(error, Error::LocalIllegalState(_)), "{error:?}");
        assert_eq!("Renewing with uncompleted acknowledgements", error.message());
    }

    fn redelivers_a_renewed_record(records: bytes::Bytes) {
        let tip = tip();
        let mut completed_fetch =
            new_share_completed_fetch(&tip, partition_data(Some(records), acquired_records(0, 3)));
        let deserializers = new_string_deserializers();
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        let delivered = batch.in_flight_records();
        assert_eq!(3, delivered.len());

        assert_eq!(1, renew(&mut batch, &delivered, &[1]));
        assert!(batch.has_renewals());
        assert_eq!(0, batch.num_records());
        assert!(take_renewals_now(&mut batch, &deserializers).is_none());

        assert_eq!(1, batch.num_records());
        assert!(!batch.has_renewals());
        let redelivered = batch.in_flight_records();
        assert_eq!(1, redelivered.len());
        let (first, again) = (&delivered[1], &redelivered[0]);
        assert_eq!(first.offset(), again.offset());
        assert_eq!(first.key(), again.key());
        assert_eq!(first.value(), again.value());
        assert_eq!(first.delivery_count(), again.delivery_count());
        assert_eq!(first.timestamp(), again.timestamp());
        assert_eq!(first.leader_epoch(), again.leader_epoch());
        // The redelivered record can be acknowledged as the first one could.
        batch
            .acknowledge(again, AcknowledgeType::Accept)
            .expect("the redelivered record is in flight");
    }

    #[test]
    fn test_renewed_record_is_redelivered() {
        redelivers_a_renewed_record(new_records(0, 3));
    }

    /// A compressed batch is decompressed again up to the record.
    #[test]
    fn test_renewed_record_of_a_compressed_batch_is_redelivered() {
        redelivers_a_renewed_record(new_records_compressed(0, 3, Compression::gzip().build()));
    }

    /// Deserializes UTF-8, counting its calls, and fails once told to.
    #[derive(Clone, Default)]
    struct CountingDeserializer {
        calls: Arc<AtomicU32>,
        failing: Arc<Mutex<HashSet<i64>>>,
    }

    impl Deserializer<String> for CountingDeserializer {
        async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let text = String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(e.to_string()))?;
            if self
                .failing
                .lock()
                .expect("not poisoned")
                .iter()
                .any(|o| text == format!("value-{o}"))
            {
                return Err(Error::serialization(format!("cannot deserialize {text}")));
            }
            Ok(text)
        }
    }

    fn records_with_distinct_values(count: i64) -> bytes::Bytes {
        let mut buffer = Vec::new();
        crate::consumer::internals::share_completed_fetch::tests::create_batch(&mut buffer, 0, count as i32);
        bytes::Bytes::from(buffer)
    }

    /// The deserializers run again for a redelivered record (D5 deviation).
    #[test]
    fn test_deserializer_runs_again_for_a_renewed_record() {
        let tip = tip();
        let data = partition_data(Some(records_with_distinct_values(2)), acquired_records(0, 2));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let value = CountingDeserializer::default();
        let deserializers = Deserializers::new(Box::new(StringDeserializer::new()), Box::new(value.clone()));
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        let delivered = batch.in_flight_records();
        assert_eq!(2, value.calls.load(Ordering::SeqCst));
        renew(&mut batch, &delivered, &[0, 1]);
        assert!(take_renewals_now(&mut batch, &deserializers).is_none());
        assert_eq!(4, value.calls.load(Ordering::SeqCst));
        assert_eq!(
            vec!["value-0", "value-1"],
            batch
                .in_flight_records()
                .iter()
                .map(|r| r.value().unwrap().as_str())
                .collect::<Vec<_>>()
        );
    }

    /// A redelivery that fails before any other is released, as a first
    /// delivery that fails is (`ShareCompletedFetch#fetchRecords`).
    #[test]
    fn test_renewed_record_failing_to_deserialize_is_released() {
        let tip = tip();
        let data = partition_data(Some(records_with_distinct_values(2)), acquired_records(0, 2));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let value = CountingDeserializer::default();
        let deserializers = Deserializers::new(Box::new(StringDeserializer::new()), Box::new(value.clone()));
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        let delivered = batch.in_flight_records();
        renew(&mut batch, &delivered, &[0, 1]);

        value.failing.lock().unwrap().insert(0);
        let error = take_renewals_now(&mut batch, &deserializers).expect("the redelivery fails");
        let Error::RecordDeserialization(thrown) = &error else {
            panic!("expected a RecordDeserializationError, got {error:?}");
        };
        assert_eq!(0, thrown.offset());
        assert_eq!(
            "Error deserializing VALUE for partition test-0 at offset 0. The record has been released.",
            thrown.message()
        );
        assert_eq!(Some(AcknowledgeType::Release), batch.acknowledgements().get(0));
        assert_eq!(
            Some(&HashSet::from([0])),
            batch.error().map(ConsumerShareInFlightBatchError::offsets)
        );
        assert_eq!(0, batch.num_records());
        // Offset 1 waits for the next call.
        assert!(batch.has_renewals());
        assert!(take_renewals_now(&mut batch, &deserializers).is_none());
        assert_eq!(
            vec![1],
            batch.in_flight_records().iter().map(ConsumerRecord::offset).collect::<Vec<_>>()
        );
    }

    /// A redelivery that fails after another is kept for the next call, as
    /// `ShareCompletedFetch` keeps `cachedRecordException`.
    #[test]
    fn test_renewed_record_failing_after_another_is_released_on_the_next_call() {
        let tip = tip();
        let data = partition_data(Some(records_with_distinct_values(2)), acquired_records(0, 2));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let value = CountingDeserializer::default();
        let deserializers = Deserializers::new(Box::new(StringDeserializer::new()), Box::new(value.clone()));
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        let delivered = batch.in_flight_records();
        renew(&mut batch, &delivered, &[0, 1]);

        value.failing.lock().unwrap().insert(1);
        assert!(take_renewals_now(&mut batch, &deserializers).is_none());
        assert!(batch.has_cached_error());
        assert!(batch.has_renewals(), "the failure waits to be reported");
        assert_eq!(
            vec![0],
            batch.in_flight_records().iter().map(ConsumerRecord::offset).collect::<Vec<_>>()
        );

        let error = take_renewals_now(&mut batch, &deserializers).expect("the kept failure is reported");
        assert!(matches!(error, Error::RecordDeserialization(_)), "{error:?}");
        assert_eq!(Some(AcknowledgeType::Release), batch.acknowledgements().get(1));
        assert_eq!(
            Some(&HashSet::from([1])),
            batch.error().map(ConsumerShareInFlightBatchError::offsets)
        );
        assert!(!batch.has_renewals());
    }

    /// An owner whose drop is observable: the response buffer.
    struct ResponseBuffer {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }

    impl AsRef<[u8]> for ResponseBuffer {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for ResponseBuffer {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    /// A fetch response's records are slices of one response buffer
    /// (`BytesReader::read_bytes_owned`, `network_client.rs`). Builds that shape:
    /// the partition's records sliced out of a bigger buffer, whose drop is seen.
    fn response_slice(records: &[u8]) -> (bytes::Bytes, Arc<AtomicBool>) {
        let dropped = Arc::new(AtomicBool::new(false));
        let mut bytes = vec![0xAB; 4096];
        bytes.extend_from_slice(records);
        bytes.extend_from_slice(&[0xCD; 4096]);
        let response = bytes::Bytes::from_owner(ResponseBuffer { bytes, dropped: Arc::clone(&dropped) });
        (response.slice(4096..4096 + records.len()), dropped)
    }

    /// D5 memory policy: once only renewed records remain, the batch copies
    /// their record batches and lets go of the response buffer.
    #[test]
    fn test_renewed_record_does_not_keep_the_response_buffer_alive() {
        let tip = tip();
        let (records, response_dropped) = response_slice(&records_with_distinct_values(3));
        let mut completed_fetch =
            new_share_completed_fetch(&tip, partition_data(Some(records), acquired_records(0, 3)));
        let deserializers = new_string_deserializers();
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        drop(completed_fetch);
        let delivered = batch.in_flight_records();
        assert!(
            !response_dropped.load(Ordering::SeqCst),
            "the in-flight records hold the response"
        );

        renew(&mut batch, &delivered, &[2]);
        drop(delivered);
        assert!(batch.holds_own_record_batches());
        assert!(
            response_dropped.load(Ordering::SeqCst),
            "the renewed record must not pin the response buffer"
        );

        assert!(take_renewals_now(&mut batch, &deserializers).is_none());
        let redelivered = batch.in_flight_records();
        assert_eq!(vec![2], redelivered.iter().map(ConsumerRecord::offset).collect::<Vec<_>>());
        assert_eq!(Some("value-2"), redelivered[0].value().map(String::as_str));
    }

    /// The bytes are dropped once nothing is in flight or renewing.
    #[test]
    fn test_record_batches_are_dropped_once_nothing_remains() {
        let tip = tip();
        let (records, response_dropped) = response_slice(&records_with_distinct_values(2));
        let mut completed_fetch =
            new_share_completed_fetch(&tip, partition_data(Some(records), acquired_records(0, 2)));
        let deserializers = new_string_deserializers();
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        drop(completed_fetch);
        let delivered = batch.in_flight_records();
        drop(delivered);
        assert!(!response_dropped.load(Ordering::SeqCst));
        batch.acknowledge_all(AcknowledgeType::Accept);
        batch.take_acknowledged_records();
        assert!(response_dropped.load(Ordering::SeqCst));
        assert!(!batch.holds_own_record_batches());
    }

    /// Deserializer that allocates nothing: the record's length.
    struct LenDeserializer;
    impl Deserializer<usize> for LenDeserializer {
        async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<usize, Error> {
            Ok(data.len())
        }
    }

    /// §27 / `share-consumer.md` §4: the share receive path allocates nothing
    /// per record beyond the deserializers' output. With deserializers that
    /// allocate nothing, what a fetch of `RECORD_COUNT` records allocates is
    /// the growth of the batch's collections (amortized, a few dozen for 200
    /// records), never one allocation per record.
    #[test]
    fn test_share_receive_path_allocates_nothing_per_record() {
        const RECORD_COUNT: i32 = 200;
        let tip = tip();
        let data = partition_data(Some(new_records(0, RECORD_COUNT)), acquired_records(0, i64::from(RECORD_COUNT)));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers: Deserializers<usize, usize> =
            Deserializers::new(Box::new(LenDeserializer), Box::new(LenDeserializer));

        let alloc_count;
        let mut batch;
        {
            let _guard = crate::AllocTrackingGuard::new();
            crate::AllocTrackingGuard::reset();
            batch = fetch(&mut completed_fetch, &deserializers, RECORD_COUNT, true);
            alloc_count = crate::AllocTrackingGuard::count();
        }
        assert_eq!(RECORD_COUNT as usize, batch.num_records());
        assert_eq!(RECORD_COUNT as usize, batch.in_flight_records().len());
        let per_record_budget = RECORD_COUNT as usize / 4;
        assert!(
            alloc_count <= per_record_budget,
            "{alloc_count} allocations for {RECORD_COUNT} records: the share receive path must not allocate per \
             record beyond the deserializers' output (CLAUDE.md §13 / §27)"
        );
    }

    /// Critic 251 Issue 2 (R4 item 8): a `take_renewals` dropped while a
    /// redelivery is pending leaves every renewal it did not redeliver pending,
    /// and the next call redelivers them.
    #[test]
    fn test_dropped_take_renewals_keeps_the_renewals_pending() {
        use crate::consumer::internals::share_completed_fetch::tests::PendOnceDeserializer;

        for pend_on_redelivery in 1..=3 {
            let tip = tip();
            let data = partition_data(Some(records_with_distinct_values(3)), acquired_records(0, 3));
            let mut completed_fetch = new_share_completed_fetch(&tip, data);
            let value = PendOnceDeserializer::default();
            let deserializers = Deserializers::new(Box::new(StringDeserializer::new()), Box::new(value.clone()));
            let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
            let delivered = batch.in_flight_records();
            assert_eq!(3, renew(&mut batch, &delivered, &[0, 1, 2]));

            // The first delivery made 3 calls.
            value.pend_on.store(3 + pend_on_redelivery, Ordering::SeqCst);
            assert!(
                batch.take_renewals(&deserializers).now_or_never().is_none(),
                "dropped mid-redelivery"
            );
            let redelivered_before_drop = pend_on_redelivery - 1;
            assert_eq!(redelivered_before_drop, batch.num_records());
            assert!(batch.has_renewals(), "the rest are still renewed");
            assert!(completed_fetch.has_pending_acknowledgements());

            assert!(take_renewals_now(&mut batch, &deserializers).is_none());
            assert!(!batch.has_renewals());
            assert_eq!(3, batch.num_records());
            assert_eq!(
                vec![0, 1, 2],
                batch.in_flight_records().iter().map(ConsumerRecord::offset).collect::<Vec<_>>(),
                "pending on redelivery {pend_on_redelivery}"
            );
            assert_eq!(0, batch.acknowledgements().size());
        }
    }

    /// Critic 251 Issue 6: renewed records of one compressed batch are
    /// redelivered in one walk, decoding the batch once.
    #[test]
    fn test_renewed_records_of_a_batch_are_redelivered_in_one_walk() {
        let tip = tip();
        let records = new_records_compressed(0, 5, Compression::lz4().build());
        let mut completed_fetch =
            new_share_completed_fetch(&tip, partition_data(Some(records), acquired_records(0, 5)));
        let deserializers = new_string_deserializers();
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        let delivered = batch.in_flight_records();
        assert_eq!(4, renew(&mut batch, &delivered, &[0, 2, 3, 4]));
        assert!(take_renewals_now(&mut batch, &deserializers).is_none());
        assert_eq!(
            vec![0, 2, 3, 4],
            batch.in_flight_records().iter().map(ConsumerRecord::offset).collect::<Vec<_>>()
        );
        assert_eq!(1, batch.redelivery_batches_installed, "the batch is walked once");
    }

    /// Critic 251 Issue 7 (probe H): records 0-2, a refused v1 batch at 3,
    /// then 4-6. Offset 5, renewed, is redelivered past the refused batch,
    /// both when only renewed records remain (the copied span) and when 6 is
    /// still in flight (the response's bytes).
    #[test]
    fn test_renewed_record_after_a_refused_batch_is_redelivered() {
        use crate::consumer::internals::share_completed_fetch::tests::legacy_v1_record;

        for keep_six_in_flight in [false, true] {
            let mut records = new_records(0, 3).to_vec();
            records.extend_from_slice(&legacy_v1_record(3, b"legacy"));
            records.extend_from_slice(&new_records(4, 3));
            let tip = tip();
            let data = partition_data(Some(bytes::Bytes::from(records)), acquired_records(0, 7));
            let mut completed_fetch = new_share_completed_fetch(&tip, data);
            let deserializers = new_string_deserializers();

            let mut first = fetch(&mut completed_fetch, &deserializers, 3, false);
            assert_eq!(
                vec![0, 1, 2],
                first.in_flight_records().iter().map(ConsumerRecord::offset).collect::<Vec<_>>()
            );
            let refused = fetch(&mut completed_fetch, &deserializers, 3, false);
            assert_eq!(Some(AcknowledgeType::Reject), refused.acknowledgements().get(3));
            let mut batch = fetch(&mut completed_fetch, &deserializers, 3, false);
            let delivered = batch.in_flight_records();
            assert_eq!(vec![4, 5, 6], delivered.iter().map(ConsumerRecord::offset).collect::<Vec<_>>());

            batch.acknowledge(&delivered[0], AcknowledgeType::Accept).unwrap();
            batch.acknowledge(&delivered[1], AcknowledgeType::Renew).unwrap();
            if !keep_six_in_flight {
                batch.acknowledge(&delivered[2], AcknowledgeType::Accept).unwrap();
            }
            let mut acknowledgements = batch.take_acknowledged_records();
            assert_eq!(!keep_six_in_flight, batch.holds_own_record_batches());
            acknowledgements.complete(None);
            assert_eq!(1, batch.renew(&acknowledgements).unwrap());

            assert!(
                take_renewals_now(&mut batch, &deserializers).is_none(),
                "keep_six_in_flight={keep_six_in_flight}"
            );
            assert!(batch.error().is_none());
            assert_eq!(None, batch.acknowledgements().get(5));
            let redelivered = batch.in_flight_records();
            assert_eq!(vec![5], redelivered.iter().map(ConsumerRecord::offset).collect::<Vec<_>>());
            assert_eq!(Some("value-"), redelivered[0].value().map(String::as_str));
        }
    }
}
