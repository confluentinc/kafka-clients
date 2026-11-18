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

//! The acknowledgements of the records of one share-group topic-partition.
//!
//! Translated from `org.apache.kafka.clients.consumer.internals.Acknowledgements`.

use std::collections::BTreeMap;
use std::fmt;

use crate::common::Error;
use crate::consumer::AcknowledgeType;
use crate::consumer::internals::AcknowledgementBatch;

/// This class maintains the acknowledgement and gap information for a set of
/// records on a single topic-partition being delivered to a consumer in a
/// share group.
///
/// `Clone` is Rust-only: where Java hands the same object to two readers
/// (the acknowledgement event and the `commitSync` result), the share
/// consumer may clone it (`share-consumer.md` §6).
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements")]
pub(crate) struct Acknowledgements {
    // The acknowledgements keyed by offset. If the record is a gap, the AcknowledgeType will be null.
    // Java's `TreeMap`: iterated in offset order (translation-checklist R5).
    acknowledgements: BTreeMap<i64, Option<AcknowledgeType>>,

    // When the broker responds to the acknowledgements, this is the exception thrown.
    acknowledge_error: Option<Error>,

    // Set when the broker has responded to the acknowledgements.
    completed: bool,
}

impl Acknowledgements {
    /// The acknowledge type sent for a gap, an offset with no record.
    pub(crate) const ACKNOWLEDGE_TYPE_GAP: i8 = 0;
    /// The run of one acknowledge type past which a batch is split so that
    /// the run is sent as a single type.
    pub(crate) const MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE: usize = 10;

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#empty")]
    pub(crate) fn empty() -> Self {
        Self { acknowledgements: BTreeMap::new(), acknowledge_error: None, completed: false }
    }

    /// Adds an acknowledgement for a specific offset. Will overwrite an
    /// existing acknowledgement for the same offset.
    ///
    /// `None` records a gap, as Java's `add(offset, null)` does.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#add")]
    pub(crate) fn add(&mut self, offset: i64, r#type: Option<AcknowledgeType>) {
        self.acknowledgements.insert(offset, r#type);
    }

    /// Adds an acknowledgement for a specific offset. Will **not** overwrite
    /// an existing acknowledgement for the same offset.
    ///
    /// Returns whether the acknowledgement was added.
    // Java's `putIfAbsent(offset, type) == null` also replaces a gap (a `null`
    // value) and reports it added.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#addIfAbsent")]
    pub(crate) fn add_if_absent(&mut self, offset: i64, r#type: AcknowledgeType) -> bool {
        let entry = self.acknowledgements.entry(offset).or_insert(None);
        if entry.is_some() {
            return false;
        }
        *entry = Some(r#type);
        true
    }

    /// Adds a gap for the specified offset. This means the broker expected a
    /// record at this offset but when the record batch was parsed, the desired
    /// offset was missing. This occurs when the record has been removed by
    /// the log compactor.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#addGap")]
    pub(crate) fn add_gap(&mut self, offset: i64) {
        self.acknowledgements.insert(offset, None);
    }

    /// Gets the acknowledgement type for an offset: `None` if no
    /// acknowledgement is present, or if the offset is a gap.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#get")]
    pub(crate) fn get(&self, offset: i64) -> Option<AcknowledgeType> {
        self.acknowledgements.get(&offset).copied().flatten()
    }

    /// Whether the set of acknowledgements is empty.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#isEmpty")]
    pub(crate) fn is_empty(&self) -> bool {
        self.acknowledgements.is_empty()
    }

    /// Returns the size of the set of acknowledgements.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#size")]
    pub(crate) fn size(&self) -> usize {
        self.acknowledgements.len()
    }

    /// Whether the acknowledgements were sent to the broker and a response
    /// received.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#isCompleted")]
    pub(crate) fn is_completed(&self) -> bool {
        self.completed
    }

    /// Completes the acknowledgements when the response has been received
    /// from the broker. `acknowledge_error` is `None` if successful. Only the
    /// first call has an effect.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#complete")]
    pub(crate) fn complete(&mut self, acknowledge_error: Option<Error>) {
        if !self.completed {
            if acknowledge_error.is_some() {
                self.acknowledge_error = acknowledge_error;
            }
            self.completed = true;
        }
    }

    /// Get the acknowledgement error when the response has been received from
    /// the broker.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#getAcknowledgeException")]
    pub(crate) fn acknowledge_error(&self) -> Option<&Error> {
        self.acknowledge_error.as_ref()
    }

    /// Whether an acknowledgement error code was received in the response from
    /// the broker.
    ///
    /// Translated from `Acknowledgements.isCompletedExceptionally`. The Rust
    /// name differs because CLAUDE.md §2 keeps the word "exception" out of
    /// Rust identifiers, as `KafkaFutureImpl::complete_with_error` does.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#isCompleted")]
    pub(crate) fn is_completed_with_error(&self) -> bool {
        self.acknowledge_error.is_some()
    }

    /// Merges two sets of acknowledgements. If there are overlapping
    /// acknowledgements, the merged set wins.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#merge")]
    pub(crate) fn merge(&mut self, other: &Acknowledgements) -> &mut Self {
        self.acknowledgements
            .extend(other.acknowledgements.iter().map(|(&offset, &r#type)| (offset, r#type)));
        self
    }

    /// Returns the Map of Acknowledgements for the offsets, a gap mapping to
    /// `None`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#getAcknowledgementsTypeMap")]
    pub(crate) fn acknowledgements_type_map(&self) -> &BTreeMap<i64, Option<AcknowledgeType>> {
        &self.acknowledgements
    }

    /// Converts the acknowledgements into a list of [`AcknowledgementBatch`]
    /// which can easily be converted into the form required for the RPC
    /// requests.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.Acknowledgements#getAcknowledgementBatches")]
    pub(crate) fn acknowledgement_batches(&self) -> Vec<AcknowledgementBatch> {
        let mut batches = Vec::new();
        if self.acknowledgements.is_empty() {
            return batches;
        }

        let mut current_batch: Option<AcknowledgementBatch> = None;
        for (&offset, r#type) in &self.acknowledgements {
            let batch = match current_batch.take() {
                None => {
                    let mut batch = AcknowledgementBatch::new();
                    batch.set_first_offset(offset);
                    batch
                },
                Some(batch) => Self::maybe_create_new_batch(batch, offset, &mut batches),
            };
            let batch = current_batch.insert(batch);
            batch.set_last_offset(offset);
            batch
                .acknowledge_types_mut()
                .push(r#type.map_or(Self::ACKNOWLEDGE_TYPE_GAP, AcknowledgeType::id));
        }
        Self::add_optimal_batches(current_batch.as_ref(), &mut batches);
        batches
    }

    /// The tail Java repeats in `getAcknowledgementBatches` and
    /// `maybeCreateNewBatch`: splits `current_batch` into its optimal batches,
    /// shortens a batch of a single acknowledge type to one type, and appends
    /// them to `batches`.
    fn add_optimal_batches(current_batch: Option<&AcknowledgementBatch>, batches: &mut Vec<AcknowledgementBatch>) {
        for mut batch in Self::maybe_optimise_acknowledge_types(current_batch) {
            if Self::can_optimise_for_single_acknowledge_type(Some(&batch)) {
                // If the batch had a single acknowledgement type, we optimise the array independent
                // of the number of records.
                batch.acknowledge_types_mut().truncate(1);
            }
            batches.push(batch);
        }
    }

    /// Creates a new current batch if the next offset is not one higher than
    /// the current batch's last offset.
    fn maybe_create_new_batch(
        current_batch: AcknowledgementBatch,
        next_offset: i64,
        batches: &mut Vec<AcknowledgementBatch>,
    ) -> AcknowledgementBatch {
        if next_offset != current_batch.last_offset() + 1 {
            Self::add_optimal_batches(Some(&current_batch), batches);

            let mut current_batch = AcknowledgementBatch::new();
            current_batch.set_first_offset(next_offset);
            return current_batch;
        }

        current_batch
    }

    /// Traverses the acknowledgementBatch and splits it into optimal batches
    /// wherever possible. Optimisation happens when a batch has continuous
    /// records of the same acknowledge type whose count exceeds the default
    /// value. In this case, the batch is split into 2 such that the batch with
    /// the continuous records has only 1 acknowledge type in its array.
    fn maybe_optimise_acknowledge_types(
        current_acknowledge_batch: Option<&AcknowledgementBatch>,
    ) -> Vec<AcknowledgementBatch> {
        let mut batches = Vec::new();
        let Some(current_acknowledge_batch) = current_acknowledge_batch else {
            return batches;
        };
        let types = current_acknowledge_batch.acknowledge_types();
        // Java's `int` index arithmetic on `long` offsets; a batch holds far
        // fewer than `i64::MAX` types, so the conversions are exact.
        let offset_of = |index: usize| index as i64;

        let mut current_offset = current_acknowledge_batch.first_offset();
        let mut current_start_index = 0;
        let mut records_with_same_acknowledge_type = 1;
        let mut i = 1;
        while i < types.len() {
            let acknowledge_type = types[i];
            // If we have a continuous set of records with the same acknowledgement type exceeding the default count,
            // then we optimise the batches to include only start and end offset and have only 1 acknowledge type in the array.
            let prev_acknowledge_type = types[i - 1];
            if acknowledge_type == prev_acknowledge_type
                && records_with_same_acknowledge_type >= Self::MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE
            {
                // We continue traversing until we have the same acknowledge type.
                while i < types.len() {
                    let acknowledge_type2 = types[i];

                    if acknowledge_type2 != types[i - 1] {
                        break;
                    }
                    i += 1;
                    records_with_same_acknowledge_type += 1;
                }

                // Now we prepare 2 batches, one starting just before the batch with single acknowledge type
                // and one with the single acknowledge type.
                let mut batch1 = AcknowledgementBatch::new();
                batch1.set_first_offset(current_offset);
                batch1.set_last_offset(
                    current_offset + offset_of(i)
                        - offset_of(records_with_same_acknowledge_type)
                        - offset_of(current_start_index)
                        - 1,
                );
                if batch1.last_offset() >= batch1.first_offset() {
                    batch1.set_acknowledge_types(
                        types[current_start_index..i - records_with_same_acknowledge_type].to_vec(),
                    );
                    batches.push(batch1);
                }

                let mut batch2 = AcknowledgementBatch::new();
                batch2.set_first_offset(
                    current_offset + offset_of(i)
                        - offset_of(records_with_same_acknowledge_type)
                        - offset_of(current_start_index),
                );
                batch2.set_last_offset(current_offset + offset_of(i) - offset_of(current_start_index) - 1);
                batch2.acknowledge_types_mut().push(acknowledge_type);

                batches.push(batch2);
                records_with_same_acknowledge_type = 1;

                // Updating the offset and startIndex for further iterations.
                current_offset = current_offset + offset_of(i) - offset_of(current_start_index);
                current_start_index = i;
            } else if acknowledge_type == prev_acknowledge_type {
                // The maximum limit has not yet been reached, we increment the count and move ahead.
                records_with_same_acknowledge_type += 1;
            } else {
                records_with_same_acknowledge_type = 1;
            }
            i += 1;
        }
        if current_start_index < types.len() {
            let mut batch = AcknowledgementBatch::new();
            batch.set_first_offset(current_offset);
            batch.set_last_offset(current_offset + offset_of(types.len()) - offset_of(current_start_index) - 1);
            batch.set_acknowledge_types(types[current_start_index..].to_vec());
            batches.push(batch);
        }
        batches
    }

    /// Returns true if the array of acknowledge types in the share fetch batch
    /// contains a single acknowledge type and the array size can be reduced
    /// to 1. Returns false when the array has more than one acknowledge type
    /// or is already optimised.
    fn can_optimise_for_single_acknowledge_type(acknowledgement_batch: Option<&AcknowledgementBatch>) -> bool {
        let Some(acknowledgement_batch) = acknowledgement_batch else {
            return false;
        };
        let types = acknowledgement_batch.acknowledge_types();
        if types.len() == 1 {
            return false;
        }
        // Java's `get(0)` on an empty list would throw; every batch built
        // above holds at least one type.
        let Some(&first_acknowledge_type) = types.first() else {
            return false;
        };
        types[1..]
            .iter()
            .all(|&acknowledge_type| acknowledge_type == first_acknowledge_type)
    }
}

impl fmt::Display for Acknowledgements {
    // Java's `toString()`: the `TreeMap` as `{offset=type, ...}` (`null` for a
    // gap) and the error as its `Errors` code.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Acknowledgements({")?;
        for (index, (offset, r#type)) in self.acknowledgements.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            match r#type {
                Some(r#type) => write!(f, "{offset}={type}")?,
                None => write!(f, "{offset}=null")?,
            }
        }
        f.write_str("}, acknowledgeException=")?;
        match &self.acknowledge_error {
            Some(error) => write!(f, "{}", error.error())?,
            None => f.write_str("null")?,
        }
        write!(f, ", completed={})", self.completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::Errors;

    const MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE: i64 = Acknowledgements::MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE as i64;
    const ACCEPT: Option<AcknowledgeType> = Some(AcknowledgeType::Accept);
    const RELEASE: Option<AcknowledgeType> = Some(AcknowledgeType::Release);
    const REJECT: Option<AcknowledgeType> = Some(AcknowledgeType::Reject);
    const RENEW: Option<AcknowledgeType> = Some(AcknowledgeType::Renew);

    /// Java builds each test's assertions twice, on two calls of
    /// `getAcknowledgementBatches`, to show the call does not change the
    /// acknowledgements; `assert_batches_twice` runs `check` on both.
    fn assert_batches_twice(acks: &Acknowledgements, check: impl Fn(&[AcknowledgementBatch])) {
        let ack_list = acks.acknowledgement_batches();
        check(&ack_list);
        let ack_list2 = acks.acknowledgement_batches();
        check(&ack_list2);
    }

    #[test]
    fn test_empty_batch() {
        let acks = Acknowledgements::empty();
        assert_batches_twice(&acks, |ack_list| assert!(ack_list.is_empty()));
    }

    #[test]
    fn test_single_state_single_record() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, ACCEPT);

        assert_batches_twice(&acks, |ack_list| {
            assert_eq!(1, ack_list.len());
            assert_eq!(0, ack_list[0].first_offset());
            assert_eq!(0, ack_list[0].last_offset());
            assert_eq!(1, ack_list[0].acknowledge_types().len());
            assert_eq!(AcknowledgeType::Accept.id(), ack_list[0].acknowledge_types()[0]);
        });
    }

    #[test]
    fn test_single_state_multi_record() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, ACCEPT);
        acks.add(1, ACCEPT);
        acks.add(2, ACCEPT);
        acks.add(3, ACCEPT);
        acks.add(4, ACCEPT);

        assert_batches_twice(&acks, |ack_list| {
            assert_eq!(1, ack_list.len());
            assert_eq!(0, ack_list[0].first_offset());
            assert_eq!(4, ack_list[0].last_offset());
            assert_ne!(0, ack_list[0].acknowledge_types().len());
            assert_eq!(AcknowledgeType::Accept.id(), ack_list[0].acknowledge_types()[0]);
        });
    }

    #[test]
    fn test_single_acknowledge_type_exceeding_limit() {
        let mut acks = Acknowledgements::empty();
        let mut i = 0;
        while i < MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE {
            acks.add(i, ACCEPT);
            i += 1;
        }
        acks.add(i, ACCEPT);
        i += 1;
        acks.add(i, ACCEPT);
        i += 1;
        for j in 0..=MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE {
            acks.add(i + j, REJECT);
        }

        assert_batches_twice(&acks, |ack_list| {
            assert_eq!(2, ack_list.len());
            assert_eq!(0, ack_list[0].first_offset());
            assert_eq!(MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE + 1, ack_list[0].last_offset());
            assert_eq!(1, ack_list[0].acknowledge_types().len());
            assert_eq!(AcknowledgeType::Accept.id(), ack_list[0].acknowledge_types()[0]);
            assert_eq!(MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE + 2, ack_list[1].first_offset());
            assert_eq!(i + MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE, ack_list[1].last_offset());
            assert_eq!(1, ack_list[1].acknowledge_types().len());
            assert_eq!(AcknowledgeType::Reject.id(), ack_list[1].acknowledge_types()[0]);
        });
    }

    #[test]
    fn test_single_acknowledge_type_with_gap() {
        let mut acks = Acknowledgements::empty();
        for i in 0..MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE {
            acks.add(i, None);
        }
        acks.add(MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE, None);
        acks.add(MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE + 1, None);

        assert_batches_twice(&acks, |ack_list| {
            assert_eq!(1, ack_list.len());
            assert_eq!(0, ack_list[0].first_offset());
            assert_eq!(MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE + 1, ack_list[0].last_offset());
            assert_eq!(1, ack_list[0].acknowledge_types().len());
            assert_eq!(Acknowledgements::ACKNOWLEDGE_TYPE_GAP, ack_list[0].acknowledge_types()[0]);
        });
    }

    #[test]
    fn test_optimise_batches() {
        let mut acks = Acknowledgements::empty();
        let mut offset = 0;
        while offset < MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE {
            acks.add(offset, ACCEPT);
            offset += 1;
        }
        acks.add(offset, REJECT);
        offset += 1;
        acks.add(offset, ACCEPT);
        offset += 1;
        acks.add(offset, RELEASE);
        offset += 1;
        acks.add(offset, None);
        offset += 1;

        // Adding more than the max records
        for j in 0..=MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE {
            acks.add(offset + j, ACCEPT);
        }
        offset += MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE + 1;

        // Adding 2 more records of different type
        acks.add(offset, REJECT);
        offset += 1;
        acks.add(offset, RELEASE);

        assert_batches_twice(&acks, |ack_list| {
            assert_eq!(3, ack_list.len());
            assert_eq!(0, ack_list[0].first_offset());
            assert_eq!(MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE + 3, ack_list[0].last_offset());
            assert_eq!(MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE + 4, ack_list[1].first_offset());
            assert_eq!(2 * MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE + 4, ack_list[1].last_offset());
            assert_eq!(1, ack_list[1].acknowledge_types().len());
            assert_eq!(offset - 1, ack_list[2].first_offset());
            assert_eq!(offset, ack_list[2].last_offset());
            assert_eq!(2, ack_list[2].acknowledge_types().len());
        });
    }

    #[test]
    fn test_single_acknowledge_type_within_limit() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, ACCEPT);
        acks.add(1, ACCEPT);
        acks.add(2, ACCEPT);

        let ack_list = acks.acknowledgement_batches();
        assert_eq!(1, ack_list.len());
        assert_eq!(1, ack_list[0].acknowledge_types().len());
    }

    /// The five offsets 0-4 in one batch, one type each.
    fn assert_single_batch_of_five(acks: &Acknowledgements, expected_types: [i8; 5]) {
        assert_batches_twice(acks, |ack_list| {
            assert_eq!(1, ack_list.len());
            assert_eq!(0, ack_list[0].first_offset());
            assert_eq!(4, ack_list[0].last_offset());
            assert_eq!(5, ack_list[0].acknowledge_types().len());
            for (index, expected) in expected_types.iter().enumerate() {
                assert_eq!(*expected, ack_list[0].acknowledge_types()[index], "type {index}");
            }
        });
    }

    const ACCEPT_ID: i8 = 1;
    const RELEASE_ID: i8 = 2;
    const GAP: i8 = Acknowledgements::ACKNOWLEDGE_TYPE_GAP;

    #[test]
    fn test_multi_state_multi_record() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, ACCEPT);
        acks.add(1, ACCEPT);
        acks.add(2, ACCEPT);
        acks.add(3, RELEASE);
        acks.add(4, RELEASE);

        assert_single_batch_of_five(&acks, [ACCEPT_ID, ACCEPT_ID, ACCEPT_ID, RELEASE_ID, RELEASE_ID]);
    }

    #[test]
    fn test_multi_state_single_multi_record() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, ACCEPT);
        acks.add(1, RELEASE);
        acks.add(2, RELEASE);
        acks.add(3, RELEASE);
        acks.add(4, RELEASE);

        assert_single_batch_of_five(&acks, [ACCEPT_ID, RELEASE_ID, RELEASE_ID, RELEASE_ID, RELEASE_ID]);
    }

    #[test]
    fn test_multi_state_multi_single_record() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, ACCEPT);
        acks.add(1, ACCEPT);
        acks.add(2, ACCEPT);
        acks.add(3, ACCEPT);
        acks.add(4, RELEASE);

        assert_single_batch_of_five(&acks, [ACCEPT_ID, ACCEPT_ID, ACCEPT_ID, ACCEPT_ID, RELEASE_ID]);
    }

    #[test]
    fn test_single_gap() {
        let mut acks = Acknowledgements::empty();
        acks.add_gap(0);

        assert_batches_twice(&acks, |ack_list| {
            assert_eq!(1, ack_list.len());
            assert_eq!(0, ack_list[0].first_offset());
            assert_eq!(0, ack_list[0].last_offset());
            assert_eq!(1, ack_list[0].acknowledge_types().len());
            assert_eq!(GAP, ack_list[0].acknowledge_types()[0]);
        });
    }

    #[test]
    fn test_multi_gap() {
        let mut acks = Acknowledgements::empty();
        acks.add_gap(0);
        acks.add_gap(1);

        assert_batches_twice(&acks, |ack_list| {
            assert_eq!(1, ack_list.len());
            assert_eq!(0, ack_list[0].first_offset());
            assert_eq!(1, ack_list[0].last_offset());
            assert_eq!(1, ack_list[0].acknowledge_types().len());
            assert_eq!(GAP, ack_list[0].acknowledge_types()[0]);
        });
    }

    #[test]
    fn test_single_gap_single_state() {
        let mut acks = Acknowledgements::empty();
        acks.add_gap(0);
        acks.add(1, ACCEPT);

        assert_batches_twice(&acks, |ack_list| {
            assert_eq!(1, ack_list.len());
            assert_eq!(0, ack_list[0].first_offset());
            assert_eq!(1, ack_list[0].last_offset());
            assert_eq!(2, ack_list[0].acknowledge_types().len());
            assert_eq!(GAP, ack_list[0].acknowledge_types()[0]);
            assert_eq!(AcknowledgeType::Accept.id(), ack_list[0].acknowledge_types()[1]);
        });
    }

    #[test]
    fn test_single_state_single_gap() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, ACCEPT);
        acks.add_gap(1);

        assert_batches_twice(&acks, |ack_list| {
            assert_eq!(1, ack_list.len());
            assert_eq!(0, ack_list[0].first_offset());
            assert_eq!(1, ack_list[0].last_offset());
            assert_eq!(2, ack_list[0].acknowledge_types().len());
            assert_eq!(AcknowledgeType::Accept.id(), ack_list[0].acknowledge_types()[0]);
            assert_eq!(GAP, ack_list[0].acknowledge_types()[1]);
        });
    }

    #[test]
    fn test_multi_state_multi_gap() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, RELEASE);
        acks.add_gap(1);
        acks.add_gap(2);
        acks.add(3, ACCEPT);
        acks.add(4, ACCEPT);

        assert_single_batch_of_five(&acks, [RELEASE_ID, GAP, GAP, ACCEPT_ID, ACCEPT_ID]);
    }

    #[test]
    fn test_multi_state_multi_gaps() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, ACCEPT);
        acks.add(1, RELEASE);
        acks.add_gap(2);
        acks.add(3, RELEASE);
        acks.add_gap(4);

        assert_single_batch_of_five(&acks, [ACCEPT_ID, RELEASE_ID, GAP, RELEASE_ID, GAP]);
    }

    #[test]
    fn test_noncontiguous_batches() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, ACCEPT);
        acks.add(1, RELEASE);
        acks.add(3, REJECT);
        acks.add(4, REJECT);
        acks.add(6, REJECT);

        assert_batches_twice(&acks, |ack_list| {
            assert_eq!(3, ack_list.len());
            assert_eq!(0, ack_list[0].first_offset());
            assert_eq!(1, ack_list[0].last_offset());
            assert_eq!(2, ack_list[0].acknowledge_types().len());
            assert_eq!(AcknowledgeType::Accept.id(), ack_list[0].acknowledge_types()[0]);
            assert_eq!(AcknowledgeType::Release.id(), ack_list[0].acknowledge_types()[1]);
            assert_eq!(3, ack_list[1].first_offset());
            assert_eq!(4, ack_list[1].last_offset());
            assert_eq!(1, ack_list[1].acknowledge_types().len());
            assert_eq!(AcknowledgeType::Reject.id(), ack_list[1].acknowledge_types()[0]);
            assert_eq!(6, ack_list[2].first_offset());
            assert_eq!(6, ack_list[2].last_offset());
            assert_eq!(1, ack_list[2].acknowledge_types().len());
            assert_eq!(AcknowledgeType::Reject.id(), ack_list[2].acknowledge_types()[0]);
        });
    }

    #[test]
    fn test_noncontiguous_gaps() {
        let mut acks = Acknowledgements::empty();
        acks.add_gap(2);
        acks.add_gap(4);

        assert_batches_twice(&acks, |ack_list| {
            assert_eq!(2, ack_list.len());
            assert_eq!(2, ack_list[0].first_offset());
            assert_eq!(2, ack_list[0].last_offset());
            assert_eq!(1, ack_list[0].acknowledge_types().len());
            assert_eq!(GAP, ack_list[0].acknowledge_types()[0]);
            assert_eq!(4, ack_list[1].first_offset());
            assert_eq!(4, ack_list[1].last_offset());
            assert_eq!(1, ack_list[1].acknowledge_types().len());
            assert_eq!(GAP, ack_list[1].acknowledge_types()[0]);
        });
    }

    #[test]
    fn test_complete_success() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, RENEW);
        assert!(!acks.is_completed());

        acks.complete(None);
        assert!(acks.is_completed());
        assert!(acks.acknowledge_error().is_none());
    }

    #[test]
    fn test_complete_exception() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, RENEW);
        assert!(!acks.is_completed());

        acks.complete(Some(Error::kafka()));
        assert!(acks.is_completed());
        assert!(acks.acknowledge_error().is_some());
    }

    // The tests below are not in `AcknowledgementsTest`; they pin the parts of
    // the Java class it does not exercise.

    /// A run longer than the limit in the middle of a batch is sent as a
    /// batch of its own with a single type, between the offsets before it and
    /// those after it. Offsets are added out of order to show the `TreeMap`
    /// ordering.
    #[test]
    fn test_long_run_in_the_middle_splits_three_ways() {
        let mut acks = Acknowledgements::empty();
        let run = MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE + 3;
        acks.add(run + 2, RELEASE);
        for offset in (2..run + 2).rev() {
            acks.add(offset, ACCEPT);
        }
        acks.add(1, REJECT);
        acks.add(0, RELEASE);

        let ack_list = acks.acknowledgement_batches();
        assert_eq!(3, ack_list.len(), "{ack_list:?}");
        assert_eq!((0, 1), (ack_list[0].first_offset(), ack_list[0].last_offset()));
        assert_eq!(&[RELEASE_ID, AcknowledgeType::Reject.id()], ack_list[0].acknowledge_types());
        assert_eq!((2, run + 1), (ack_list[1].first_offset(), ack_list[1].last_offset()));
        assert_eq!(&[ACCEPT_ID], ack_list[1].acknowledge_types());
        assert_eq!((run + 2, run + 2), (ack_list[2].first_offset(), ack_list[2].last_offset()));
        assert_eq!(&[RELEASE_ID], ack_list[2].acknowledge_types());
    }

    #[test]
    fn test_add_if_absent_does_not_overwrite_but_fills_a_gap() {
        let mut acks = Acknowledgements::empty();
        assert!(acks.add_if_absent(5, AcknowledgeType::Accept));
        assert!(!acks.add_if_absent(5, AcknowledgeType::Reject));
        assert_eq!(Some(AcknowledgeType::Accept), acks.get(5));

        // Java's `putIfAbsent` treats a `null` value (a gap) as absent.
        acks.add_gap(6);
        assert!(acks.add_if_absent(6, AcknowledgeType::Release));
        assert_eq!(Some(AcknowledgeType::Release), acks.get(6));

        // `add` overwrites.
        acks.add(5, REJECT);
        assert_eq!(Some(AcknowledgeType::Reject), acks.get(5));
        assert_eq!(2, acks.size());
        assert!(!acks.is_empty());
        assert_eq!(None, acks.get(7));
    }

    #[test]
    fn test_merge_overwrites_with_the_other_set() {
        let mut acks = Acknowledgements::empty();
        acks.add(3, ACCEPT);
        acks.add(1, ACCEPT);
        let mut other = Acknowledgements::empty();
        other.add(1, REJECT);
        other.add_gap(2);

        acks.merge(&other);
        assert_eq!(
            vec![(1, REJECT), (2, None), (3, ACCEPT)],
            acks.acknowledgements_type_map()
                .iter()
                .map(|(&o, &t)| (o, t))
                .collect::<Vec<_>>()
        );
        // The other set is unchanged.
        assert_eq!(2, other.size());
    }

    #[test]
    fn test_complete_keeps_the_first_outcome() {
        let mut acks = Acknowledgements::empty();
        acks.complete(Some(Errors::InvalidRecordState.error().unwrap()));
        assert!(acks.is_completed_with_error());
        acks.complete(None);
        acks.complete(Some(Errors::TopicAuthorizationFailed.error().unwrap()));
        assert!(matches!(acks.acknowledge_error(), Some(Error::InvalidRecordState(_))));

        let mut acks = Acknowledgements::empty();
        acks.complete(None);
        acks.complete(Some(Error::kafka()));
        assert!(acks.is_completed());
        assert!(!acks.is_completed_with_error());
    }

    #[test]
    fn test_to_string() {
        let mut acks = Acknowledgements::empty();
        acks.add(1, ACCEPT);
        acks.add_gap(0);
        assert_eq!(
            "Acknowledgements({0=null, 1=accept}, acknowledgeException=null, completed=false)",
            acks.to_string()
        );
        acks.complete(Some(Errors::InvalidRecordState.error().unwrap()));
        assert_eq!(
            "Acknowledgements({0=null, 1=accept}, acknowledgeException=INVALID_RECORD_STATE, completed=true)",
            acks.to_string()
        );
    }
}
