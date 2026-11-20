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

//! `ShareFetch` — the records a share consumer's `poll` returns, by partition.
//!
//! Translated from `org.apache.kafka.clients.consumer.internals.ShareFetch`.

use std::collections::HashMap;

use indexmap::IndexMap;

use crate::common::Error;
use crate::common::{TopicIdPartition, TopicPartition};
use crate::consumer::AcknowledgeType;
use crate::consumer::ConsumerRecord;
use crate::consumer::internals::Acknowledgements;
use crate::consumer::internals::Deserializers;
use crate::consumer::internals::NodeAcknowledgements;
use crate::consumer::internals::ShareInFlightBatch;

/// The records fetched from the broker to be returned to the consumer to
/// satisfy a `KafkaShareConsumer#poll(Duration)` call. The records can come
/// from multiple topic-partitions.
///
/// Corresponds to `org.apache.kafka.clients.consumer.internals.ShareFetch`.
///
/// Confined to the application task (`share-consumer.md` §6): a plain value,
/// no lock.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch")]
pub(crate) struct ShareFetch<K, V> {
    batches: HashMap<TopicIdPartition, Vec<ShareInFlightBatch<K, V>>>,
    acquisition_lock_timeout_ms: Option<i32>,
    acquisition_lock_timeout_ms_renewed: Option<i32>,
}

impl<K, V> ShareFetch<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// An empty fetch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#empty")]
    pub(crate) fn empty() -> Self {
        Self {
            batches: HashMap::new(),
            acquisition_lock_timeout_ms: None,
            acquisition_lock_timeout_ms_renewed: None,
        }
    }

    /// Add another [`ShareInFlightBatch`] to this one; all of its records will be added to this object's
    /// [`Self::records`]. Generally, we will only have one batch for a partition
    /// at a time, but in some cases (such as a repeated request after an empty response, or partition leader
    /// changes), there might be more than one.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#add")]
    pub(crate) fn add(&mut self, partition: TopicIdPartition, batch: ShareInFlightBatch<K, V>) {
        if batch.acquisition_lock_timeout_ms().is_some() {
            self.acquisition_lock_timeout_ms = batch.acquisition_lock_timeout_ms();
        }
        self.batches.entry(partition).or_default().push(batch);
    }

    /// Returns all the non-control messages for this fetch, grouped by
    /// partition. Partitions with no in-flight records (such as those whose
    /// acquired offsets were all control records) are omitted.
    ///
    /// Each record is handed out once ([`ShareInFlightBatch::in_flight_records`]);
    /// Java's unmodifiable map is a fresh map the caller owns here.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#records")]
    pub(crate) fn records(&mut self) -> HashMap<TopicPartition, Vec<ConsumerRecord<K, V>>> {
        let mut result = HashMap::with_capacity(self.batches.len());
        for (tip, batch_list) in &mut self.batches {
            let mut records = Vec::new();
            for batch in batch_list {
                records.append(&mut batch.in_flight_records());
            }
            if !records.is_empty() {
                result.insert(tip.topic_partition().clone(), records);
            }
        }
        result
    }

    /// Returns the total number of non-control messages for this fetch, across
    /// all partitions. Drops the batches with nothing in flight, acknowledged
    /// or renewing, as Java does.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#numRecords")]
    pub(crate) fn num_records(&mut self) -> usize {
        let mut num_records = 0;
        self.batches.retain(|_, batch_list| {
            // Java's `removeIf(batch -> batch.isEmpty() && !batch.hasRenewals())`.
            batch_list.retain(|batch| !batch.is_empty() || batch.has_renewals());
            num_records += batch_list.iter().map(ShareInFlightBatch::num_records).sum::<usize>();
            !batch_list.is_empty()
        });
        num_records
    }

    /// Returns `true` if and only if this fetch did not return any non-control
    /// records.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#isEmpty")]
    pub(crate) fn is_empty(&mut self) -> bool {
        self.num_records() == 0
    }

    /// Returns the most up-to-date value of acquisition lock timeout, if
    /// available.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#acquisitionLockTimeoutMs")]
    pub(crate) fn acquisition_lock_timeout_ms(&self) -> Option<i32> {
        self.acquisition_lock_timeout_ms
    }

    /// Returns `true` if this fetch contains records being renewed.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#hasRenewals")]
    pub(crate) fn has_renewals(&self) -> bool {
        self.batches.values().flatten().any(ShareInFlightBatch::has_renewals)
    }

    /// Take any renewed records and move them back into in-flight state.
    ///
    /// Each batch delivers its renewed records again
    /// ([`ShareInFlightBatch::take_renewals`]); `deserializers` are the
    /// consumer's (Java's batches keep the record objects and need none, see
    /// the D5 note on [`ShareInFlightBatch`]). A record whose redelivery fails
    /// is released and its batch carries the failure, as a record whose first
    /// delivery fails; the failure is returned only if no record is in flight
    /// afterwards, as `ShareFetchCollector#collect` throws a batch's failure
    /// only when the fetch is otherwise empty. As the collector stops at the
    /// first partition that fails, so does this: the batches after it take
    /// their renewals, and report their own failures, on later calls.
    ///
    /// # Cancellation
    ///
    /// A call dropped mid-way leaves every renewal it did not redeliver
    /// pending for the next call ([`ShareInFlightBatch::take_renewals`]). The application can then
    /// acknowledge the released record differently, as after any delivery
    /// failure (`ShareFetch#acknowledge(String, int, long, AcknowledgeType)`).
    ///
    /// # Errors
    ///
    /// The failure of the first record whose redelivery failed, when no record
    /// is in flight.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#takeRenewedRecords")]
    pub(crate) async fn take_renewed_records(&mut self, deserializers: &Deserializers<K, V>) -> Result<(), Error> {
        // Java's collector stops at the first partition that fails, and the
        // partitions after it are read on later polls: so do the renewals, whose
        // batches keep their renewals pending (`has_renewals`) until then.
        let mut first_error = None;
        for batch in self.batches.values_mut().flatten() {
            if let Some(error) = batch.take_renewals(deserializers).await {
                first_error = Some(error);
                break;
            }
        }
        // Any acquisition lock timeout updated by renewal is applied as the renewed records are move back to in-flight
        if self.acquisition_lock_timeout_ms_renewed.is_some() {
            self.acquisition_lock_timeout_ms = self.acquisition_lock_timeout_ms_renewed;
        }
        match first_error {
            Some(error) if self.num_records() == 0 => Err(error),
            _ => Ok(()),
        }
    }

    /// Acknowledge a single record in the current batch.
    ///
    /// # Errors
    ///
    /// `IllegalStateError("The record cannot be acknowledged.")` when the
    /// record is not in flight.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#acknowledge")]
    pub(crate) fn acknowledge_with_record(
        &mut self,
        record: &ConsumerRecord<K, V>,
        r#type: AcknowledgeType,
    ) -> Result<(), Error> {
        for (tip, batch_list) in &mut self.batches {
            if tip.topic() == record.topic() && tip.partition() == record.partition() {
                for batch in batch_list {
                    if batch.is_in_flight(record.offset()) {
                        return batch.acknowledge(record, r#type);
                    }
                }
            }
        }
        Err(Error::local_illegal_state("The record cannot be acknowledged."))
    }

    /// Acknowledge a single record which experienced an exception during its delivery by its topic, partition
    /// and offset in the current batch. This method is specifically for overriding the default acknowledge
    /// type for records whose delivery failed.
    ///
    /// # Errors
    ///
    /// `IllegalStateError("The record cannot be acknowledged.")` when no batch
    /// of the partition failed to deliver the record at `offset`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#acknowledge")]
    pub(crate) fn acknowledge_with_topic_partition_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        r#type: AcknowledgeType,
    ) -> Result<(), Error> {
        for (tip, batch_list) in &mut self.batches {
            if tip.topic() == topic && tip.partition() == partition {
                for batch in batch_list {
                    if batch.error().is_some_and(|error| error.offsets().contains(&offset)) {
                        batch.add_acknowledgement(offset, r#type);
                        return Ok(());
                    }
                }
            }
        }
        Err(Error::local_illegal_state("The record cannot be acknowledged."))
    }

    /// Acknowledge all records in the current batch. If any records in the batch already have
    /// been acknowledged, those acknowledgements are not overwritten.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#acknowledgeAll")]
    pub(crate) fn acknowledge_all(&mut self, r#type: AcknowledgeType) {
        for batch in self.batches.values_mut().flatten() {
            batch.acknowledge_all(r#type);
        }
    }

    /// Checks whether all in-flight records have been acknowledged. This is
    /// required for explicit acknowledgement mode.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#checkAllInFlightAreAcknowledged")]
    pub(crate) fn check_all_in_flight_are_acknowledged(&self) -> bool {
        self.batches
            .values()
            .flatten()
            .all(ShareInFlightBatch::check_all_in_flight_are_acknowledged)
    }

    /// Removes all acknowledged records from the in-flight records and returns
    /// the map of acknowledgements to send, along with node information. If
    /// some records were not acknowledged, the in-flight records will not be
    /// empty after this method.
    ///
    /// Java fills a `LinkedHashMap` in the iteration order of its `HashMap` of
    /// batches; the `IndexMap` keeps that order (translation-checklist R5).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#takeAcknowledgedRecords")]
    pub(crate) fn take_acknowledged_records(&mut self) -> IndexMap<TopicIdPartition, NodeAcknowledgements> {
        let mut acknowledgement_map = IndexMap::new();
        for (tip, batch_list) in &mut self.batches {
            let Some(node_id) = batch_list.first().map(ShareInFlightBatch::node_id) else {
                continue;
            };
            let mut acknowledgements = Acknowledgements::empty();
            for batch in batch_list.iter_mut() {
                acknowledgements.merge(&batch.take_acknowledged_records());
            }
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
    /// # Errors
    ///
    /// `IllegalStateError("Renewing with uncompleted acknowledgements")` when
    /// an entry of `acknowledgements_map` is not completed.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetch#renew")]
    pub(crate) fn renew(
        &mut self,
        acknowledgements_map: &HashMap<TopicIdPartition, Acknowledgements>,
        acquisition_lock_timeout_ms: Option<i32>,
    ) -> Result<usize, Error> {
        let mut records_renewed = 0;
        for (tip, acknowledgements) in acknowledgements_map {
            if let Some(batch_list) = self.batches.get_mut(tip) {
                for batch in batch_list {
                    records_renewed += batch.renew(acknowledgements)?;
                }
            }
        }
        self.acquisition_lock_timeout_ms_renewed = acquisition_lock_timeout_ms;
        Ok(records_renewed)
    }
}

impl<K, V> std::fmt::Debug for ShareFetch<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareFetch")
            .field("batches", &self.batches)
            .field("acquisition_lock_timeout_ms", &self.acquisition_lock_timeout_ms)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    //! Translated from `ShareFetchTest` (9 tests). `testRecordsIsUnmodifiable`
    //! is not translated: it asserts that Java's returned map throws
    //! `UnsupportedOperationException` on `remove` / `put`; [`ShareFetch::records`]
    //! returns a map the caller owns, through which the fetch cannot be
    //! changed, so there is nothing to assert.

    use super::*;
    use crate::common::Uuid;

    const NODE_ID: i32 = 0;
    const TOPIC: &str = "topic-a";

    fn partitions() -> (TopicIdPartition, TopicIdPartition) {
        let partition0 = TopicIdPartition::with_partition_topic(Uuid::random_uuid(), 0, TOPIC);
        let partition1 = TopicIdPartition::with_partition_topic(partition0.topic_id(), 1, TOPIC);
        (partition0, partition1)
    }

    fn batch_with_records(
        partition: &TopicIdPartition,
        base_offset: i64,
        count: i64,
    ) -> ShareInFlightBatch<String, String> {
        let mut batch = ShareInFlightBatch::new(NODE_ID, partition.clone(), None);
        for offset in base_offset..base_offset + count {
            batch.add_record(ConsumerRecord::new(
                partition.topic(),
                partition.partition(),
                offset,
                Some(format!("key-{offset}")),
                Some(format!("value-{offset}")),
            ));
        }
        batch
    }

    fn batch_with_gaps(partition: &TopicIdPartition, offsets: &[i64]) -> ShareInFlightBatch<String, String> {
        let mut batch = ShareInFlightBatch::new(NODE_ID, partition.clone(), None);
        for &offset in offsets {
            batch.add_gap(offset);
        }
        batch
    }

    fn offsets(records: Option<&Vec<ConsumerRecord<String, String>>>) -> Vec<i64> {
        records
            .map(|r| r.iter().map(ConsumerRecord::offset).collect())
            .unwrap_or_default()
    }

    #[test]
    fn test_empty_fetch() {
        let mut fetch = ShareFetch::<String, String>::empty();
        assert!(fetch.is_empty());
        assert_eq!(0, fetch.num_records());
        assert!(fetch.records().is_empty());
        assert!(fetch.take_acknowledged_records().is_empty());
        assert_eq!(None, fetch.acquisition_lock_timeout_ms());
    }

    #[test]
    fn test_records_grouped_by_partition() {
        let (partition0, partition1) = partitions();
        let mut fetch = ShareFetch::empty();
        fetch.add(partition0.clone(), batch_with_records(&partition0, 0, 2));
        fetch.add(partition1.clone(), batch_with_records(&partition1, 10, 3));

        assert!(!fetch.is_empty());
        assert_eq!(5, fetch.num_records());

        let records = fetch.records();
        assert_eq!(2, records.len());
        assert_eq!(vec![0, 1], offsets(records.get(partition0.topic_partition())));
        assert_eq!(vec![10, 11, 12], offsets(records.get(partition1.topic_partition())));
    }

    #[test]
    fn test_records_combines_multiple_batches_for_partition() {
        let (partition0, _) = partitions();
        let mut fetch = ShareFetch::empty();
        fetch.add(partition0.clone(), batch_with_records(&partition0, 0, 2));
        fetch.add(partition0.clone(), batch_with_records(&partition0, 2, 2));

        assert_eq!(4, fetch.num_records());

        let records = fetch.records();
        assert_eq!(1, records.len());
        assert_eq!(vec![0, 1, 2, 3], offsets(records.get(partition0.topic_partition())));
    }

    #[test]
    fn test_records_omits_partition_with_only_gaps() {
        // Partition 0 delivered real records. Partition 1 had acquired offsets which were all control
        // records, so it has gap acknowledgements to send but nothing to deliver to the application.
        let (partition0, partition1) = partitions();
        let mut fetch = ShareFetch::empty();
        fetch.add(partition0.clone(), batch_with_records(&partition0, 0, 2));
        fetch.add(partition1.clone(), batch_with_gaps(&partition1, &[5, 6]));

        assert!(!fetch.is_empty());
        assert_eq!(2, fetch.num_records());

        // The application must not see a partition with an empty list of records.
        let records = fetch.records();
        assert_eq!(1, records.len());
        assert_eq!(vec![0, 1], offsets(records.get(partition0.topic_partition())));
        assert!(!records.contains_key(partition1.topic_partition()));

        // But the gap acknowledgements for the omitted partition are still sent.
        fetch.acknowledge_all(AcknowledgeType::Accept);
        let acknowledgements = fetch.take_acknowledged_records();
        assert_eq!(2, acknowledgements.len());
        let acks0 = acknowledgements[&partition0].acknowledgements();
        assert_eq!(2, acks0.size());
        assert_eq!(Some(AcknowledgeType::Accept), acks0.get(0));
        assert_eq!(Some(AcknowledgeType::Accept), acks0.get(1));
        let acks1 = acknowledgements[&partition1].acknowledgements();
        assert_eq!(2, acks1.size());
        assert_eq!(None, acks1.get(5));
        assert_eq!(None, acks1.get(6));
    }

    #[test]
    fn test_records_includes_partition_with_gaps_and_records_in_separate_batches() {
        // A gap-only batch does not hide the records from another batch for the same partition.
        let (partition0, _) = partitions();
        let mut fetch = ShareFetch::empty();
        fetch.add(partition0.clone(), batch_with_gaps(&partition0, &[0]));
        fetch.add(partition0.clone(), batch_with_records(&partition0, 1, 2));

        assert_eq!(2, fetch.num_records());

        let records = fetch.records();
        assert_eq!(1, records.len());
        assert_eq!(vec![1, 2], offsets(records.get(partition0.topic_partition())));
    }

    #[test]
    fn test_records_empty_after_all_acknowledged() {
        let (partition0, partition1) = partitions();
        let mut fetch = ShareFetch::empty();
        fetch.add(partition0.clone(), batch_with_records(&partition0, 0, 2));
        fetch.add(partition1.clone(), batch_with_records(&partition1, 0, 1));

        fetch.acknowledge_all(AcknowledgeType::Accept);
        let acknowledgements = fetch.take_acknowledged_records();
        assert_eq!(2, acknowledgements.len());
        assert_eq!(NODE_ID, acknowledgements[&partition0].node_id());

        assert!(fetch.records().is_empty());
        assert!(fetch.is_empty());
        assert!(fetch.take_acknowledged_records().is_empty());
    }

    /// Java reads `records()` again after the acknowledgements were taken and
    /// expects `[1]`, the record still in flight. Here each record is handed
    /// out once ([`ShareInFlightBatch::in_flight_records`]), so the second
    /// `records()` returns none; the test asserts instead that offset 1 is the
    /// only record still in flight, through `num_records` and by acknowledging
    /// the record the first `records()` returned.
    #[test]
    fn test_records_only_includes_unacknowledged_records() {
        let (partition0, _) = partitions();
        let mut fetch = ShareFetch::empty();
        fetch.add(partition0.clone(), batch_with_records(&partition0, 0, 3));

        let delivered = fetch
            .records()
            .remove(partition0.topic_partition())
            .expect("partition 0 has records");
        fetch.acknowledge_with_record(&delivered[0], AcknowledgeType::Accept).unwrap();
        fetch.acknowledge_with_record(&delivered[2], AcknowledgeType::Release).unwrap();
        assert!(!fetch.check_all_in_flight_are_acknowledged());
        fetch.take_acknowledged_records();

        assert_eq!(1, fetch.num_records());
        assert!(fetch.records().is_empty(), "records are handed out once");
        let error = fetch
            .acknowledge_with_record(&delivered[0], AcknowledgeType::Accept)
            .unwrap_err();
        assert_eq!("The record cannot be acknowledged.", error.message());
        fetch
            .acknowledge_with_record(&delivered[1], AcknowledgeType::Accept)
            .expect("offset 1 is still in flight");
        assert!(fetch.check_all_in_flight_are_acknowledged());
    }

    #[test]
    fn test_acquisition_lock_timeout_taken_from_latest_batch() {
        let (partition0, partition1) = partitions();
        let mut fetch = ShareFetch::<String, String>::empty();
        fetch.add(
            partition0.clone(),
            ShareInFlightBatch::new(NODE_ID, partition0.clone(), Some(1000)),
        );
        assert_eq!(Some(1000), fetch.acquisition_lock_timeout_ms());

        fetch.add(partition1.clone(), ShareInFlightBatch::new(NODE_ID, partition1.clone(), None));
        assert_eq!(Some(1000), fetch.acquisition_lock_timeout_ms());

        fetch.add(
            partition1.clone(),
            ShareInFlightBatch::new(NODE_ID, partition1.clone(), Some(2000)),
        );
        assert_eq!(Some(2000), fetch.acquisition_lock_timeout_ms());
    }

    /// Rust-only: `acknowledge(topic, partition, offset, type)` overrides only
    /// a record whose delivery failed, with Java's text otherwise.
    #[test]
    fn test_acknowledge_with_topic_partition_offset() {
        use std::collections::HashSet;

        use crate::consumer::internals::ConsumerShareInFlightBatchError;

        let (partition0, _) = partitions();
        let mut fetch = ShareFetch::<String, String>::empty();
        let mut batch = ShareInFlightBatch::new(NODE_ID, partition0.clone(), None);
        batch.add_acknowledgement(4, AcknowledgeType::Release);
        batch.set_error(ConsumerShareInFlightBatchError::new(
            Error::kafka_message("bad"),
            HashSet::from([4]),
        ));
        fetch.add(partition0.clone(), batch);

        let error = fetch
            .acknowledge_with_topic_partition_offset(TOPIC, 0, 5, AcknowledgeType::Reject)
            .unwrap_err();
        assert!(matches!(error, Error::LocalIllegalState(_)), "{error:?}");
        assert_eq!("The record cannot be acknowledged.", error.message());
        fetch
            .acknowledge_with_topic_partition_offset(TOPIC, 0, 4, AcknowledgeType::Reject)
            .unwrap();
        let acknowledgements = fetch.take_acknowledged_records();
        assert_eq!(
            Some(AcknowledgeType::Reject),
            acknowledgements[&partition0].acknowledgements().get(4)
        );
    }

    /// Critic 251 Q1: as `ShareFetchCollector#collect` stops at the first
    /// partition that fails, `take_renewed_records` stops at the first batch
    /// whose redelivery fails; the next partition's failure is reported by the
    /// next call.
    #[test]
    fn test_take_renewed_records_reports_one_failing_partition_per_call() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        use futures_util::FutureExt;

        use crate::consumer::internals::share_completed_fetch::tests::{
            acquired_records, fetch, new_records, new_share_completed_fetch, partition_data,
        };

        #[derive(Clone, Default)]
        struct FailWhenArmed(Arc<AtomicBool>);
        impl crate::common::serialization::Deserializer<String> for FailWhenArmed {
            async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
                if self.0.load(Ordering::SeqCst) {
                    return Err(Error::serialization("armed"));
                }
                Ok(String::from_utf8_lossy(data).into_owned())
            }
        }

        let (partition0, partition1) = partitions();
        let value = FailWhenArmed::default();
        let deserializers = Deserializers::new(
            Box::new(crate::common::serialization::StringDeserializer::new()),
            Box::new(value.clone()),
        );
        let mut fetch_ = ShareFetch::empty();
        for partition in [&partition0, &partition1] {
            let mut completed_fetch =
                new_share_completed_fetch(partition, partition_data(Some(new_records(0, 1)), acquired_records(0, 1)));
            fetch_.add(partition.clone(), fetch(&mut completed_fetch, &deserializers, 10, true));
        }
        assert_eq!(2, fetch_.records().len());
        fetch_.acknowledge_all(AcknowledgeType::Renew);
        let acknowledgements: HashMap<TopicIdPartition, Acknowledgements> = fetch_
            .take_acknowledged_records()
            .into_iter()
            .map(|(tip, node_acknowledgements)| {
                let mut acks = node_acknowledgements.acknowledgements().clone();
                acks.complete(None);
                (tip, acks)
            })
            .collect();
        assert_eq!(2, fetch_.renew(&acknowledgements, None).unwrap());

        value.0.store(true, Ordering::SeqCst);
        let take =
            |f: &mut ShareFetch<String, String>| f.take_renewed_records(&deserializers).now_or_never().expect("ready");
        let released = |f: &mut ShareFetch<String, String>| {
            f.take_acknowledged_records()
                .values()
                .filter(|acks| acks.acknowledgements().get(0) == Some(AcknowledgeType::Release))
                .count()
        };

        let first = take(&mut fetch_).expect_err("the first failing partition is reported");
        assert!(matches!(first, Error::RecordDeserialization(_)), "{first:?}");
        assert!(fetch_.has_renewals(), "the other partition's renewal waits for the next call");
        assert_eq!(1, released(&mut fetch_));

        let second = take(&mut fetch_).expect_err("the other partition is reported next");
        assert!(matches!(second, Error::RecordDeserialization(_)), "{second:?}");
        assert!(!fetch_.has_renewals());
        assert_eq!(1, released(&mut fetch_));
        take(&mut fetch_).expect("nothing left");
    }
}
