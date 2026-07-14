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

//! Per-partition record acknowledgement and gap tracking (KIP-932).
//!
//! Corresponds to `org.apache.kafka.clients.consumer.internals.Acknowledgements`.

// Phase 1 (M9) translates the share wire/session layer; the share fetch/ack
// path that consumes these types lands in a later phase. Until then, some items
// are exercised only by tests (same precedent as `fetch_session_handler`).
#![allow(dead_code)]

use std::collections::BTreeMap;

use crate::common::KafkaError;
use crate::consumer::AcknowledgeType;
use crate::consumer::internals::acknowledgement_batch::AcknowledgementBatch;

/// The acknowledge type id used to represent a gap.
///
/// Corresponds to Java's `Acknowledgements.ACKNOWLEDGE_TYPE_GAP`.
pub(crate) const ACKNOWLEDGE_TYPE_GAP: i8 = 0;

/// The maximum number of contiguous records with the same acknowledge type
/// before the batch is optimised to carry a single acknowledge type.
///
/// Corresponds to Java's `Acknowledgements.MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE`.
pub(crate) const MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE: i32 = 10;

/// Maintains the acknowledgement and gap information for a set of records on a
/// single topic-partition being delivered to a consumer in a share group.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.Acknowledgements`.
#[derive(Clone, Debug)]
pub(crate) struct Acknowledgements {
    /// The acknowledgements keyed by offset. If the record is a gap, the
    /// acknowledge type is `None` (Java stores a `null` value).
    acknowledgements: BTreeMap<i64, Option<AcknowledgeType>>,
    /// When the broker responds to the acknowledgements, this is the error.
    acknowledge_exception: Option<KafkaError>,
    /// Set when the broker has responded to the acknowledgements.
    completed: bool,
}

impl Acknowledgements {
    /// Creates an empty set of acknowledgements.
    ///
    /// Corresponds to Java's `Acknowledgements.empty()`.
    pub(crate) fn empty() -> Self {
        Self { acknowledgements: BTreeMap::new(), acknowledge_exception: None, completed: false }
    }

    /// Adds an acknowledgement for a specific offset. Overwrites an existing
    /// acknowledgement for the same offset.
    pub(crate) fn add(&mut self, offset: i64, ack_type: AcknowledgeType) {
        self.acknowledgements.insert(offset, Some(ack_type));
    }

    /// Adds an acknowledgement for a specific offset. Does **not** overwrite an
    /// existing acknowledgement for the same offset.
    ///
    /// Returns whether the acknowledgement was added.
    pub(crate) fn add_if_absent(&mut self, offset: i64, ack_type: AcknowledgeType) -> bool {
        use std::collections::btree_map::Entry;
        match self.acknowledgements.entry(offset) {
            Entry::Vacant(e) => {
                e.insert(Some(ack_type));
                true
            },
            Entry::Occupied(_) => false,
        }
    }

    /// Adds a gap for the specified offset.
    pub(crate) fn add_gap(&mut self, offset: i64) {
        self.acknowledgements.insert(offset, None);
    }

    /// Gets the acknowledge type for an offset, or `None` if absent or a gap
    /// (matching Java's `get` which returns `null` for both).
    pub(crate) fn get(&self, offset: i64) -> Option<AcknowledgeType> {
        self.acknowledgements.get(&offset).copied().flatten()
    }

    /// Whether the set of acknowledgements is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.acknowledgements.is_empty()
    }

    /// Returns the size of the set of acknowledgements.
    pub(crate) fn size(&self) -> usize {
        self.acknowledgements.len()
    }

    /// Whether the acknowledgements were sent to the broker and a response
    /// received.
    pub(crate) fn is_completed(&self) -> bool {
        self.completed
    }

    /// Completes the acknowledgements when the response has been received from
    /// the broker.
    pub(crate) fn complete(&mut self, acknowledge_exception: Option<KafkaError>) {
        self.acknowledge_exception = acknowledge_exception;
        self.completed = true;
    }

    /// Gets the acknowledgement error received in the response from the broker.
    pub(crate) fn get_acknowledge_exception(&self) -> Option<&KafkaError> {
        self.acknowledge_exception.as_ref()
    }

    /// Whether an acknowledgement error was received in the response.
    pub(crate) fn is_completed_exceptionally(&self) -> bool {
        self.acknowledge_exception.is_some()
    }

    /// Merges two sets of acknowledgements. Overlapping acknowledgements from
    /// `other` win.
    pub(crate) fn merge(&mut self, other: &Acknowledgements) -> &mut Self {
        for (offset, ack) in &other.acknowledgements {
            self.acknowledgements.insert(*offset, *ack);
        }
        self
    }

    /// Returns the map of acknowledgements keyed by offset.
    pub(crate) fn get_acknowledgements_type_map(&self) -> &BTreeMap<i64, Option<AcknowledgeType>> {
        &self.acknowledgements
    }

    /// Converts the acknowledgements into a list of [`AcknowledgementBatch`]
    /// which can easily be converted into the form required for the RPC
    /// requests.
    ///
    /// Corresponds to Java's `getAcknowledgementBatches()`.
    pub(crate) fn get_acknowledgement_batches(&self) -> Vec<AcknowledgementBatch> {
        let mut batches: Vec<AcknowledgementBatch> = Vec::new();
        if self.acknowledgements.is_empty() {
            return batches;
        }

        let mut current_batch: Option<AcknowledgementBatch> = None;
        for (offset, ack) in &self.acknowledgements {
            let mut batch = match current_batch.take() {
                None => {
                    let mut b = AcknowledgementBatch::new();
                    b.set_first_offset(*offset);
                    b
                },
                Some(b) => Self::maybe_create_new_batch(b, *offset, &mut batches),
            };
            batch.set_last_offset(*offset);
            match ack {
                Some(t) => batch.acknowledge_types_mut().push(t.id()),
                None => batch.acknowledge_types_mut().push(ACKNOWLEDGE_TYPE_GAP),
            }
            current_batch = Some(batch);
        }

        Self::append_optimised(current_batch.as_ref(), &mut batches);
        batches
    }

    /// Creates a new current batch if the next offset is not one higher than
    /// the current batch's last offset.
    ///
    /// Corresponds to Java's `maybeCreateNewBatch`.
    fn maybe_create_new_batch(
        current_batch: AcknowledgementBatch,
        next_offset: i64,
        batches: &mut Vec<AcknowledgementBatch>,
    ) -> AcknowledgementBatch {
        if next_offset != current_batch.last_offset() + 1 {
            Self::append_optimised(Some(&current_batch), batches);
            let mut new_batch = AcknowledgementBatch::new();
            new_batch.set_first_offset(next_offset);
            new_batch
        } else {
            current_batch
        }
    }

    /// Optimises `current` into one or more batches and appends them to
    /// `batches`, collapsing single-acknowledge-type batches to a single entry.
    ///
    /// Mirrors the repeated block in Java's `getAcknowledgementBatches` and
    /// `maybeCreateNewBatch`.
    fn append_optimised(current: Option<&AcknowledgementBatch>, batches: &mut Vec<AcknowledgementBatch>) {
        let optimal_batches = Self::maybe_optimise_acknowledge_types(current);
        for mut batch in optimal_batches {
            if Self::can_optimise_for_single_acknowledge_type(&batch) {
                // If the batch had a single acknowledgement type, we optimise
                // the array independent of the number of records.
                batch.acknowledge_types_mut().truncate(1);
            }
            batches.push(batch);
        }
    }

    /// Traverses the acknowledgement batch and splits it into optimal batches
    /// wherever possible.
    ///
    /// Corresponds to Java's `maybeOptimiseAcknowledgeTypes`.
    fn maybe_optimise_acknowledge_types(
        current_acknowledge_batch: Option<&AcknowledgementBatch>,
    ) -> Vec<AcknowledgementBatch> {
        let mut batches: Vec<AcknowledgementBatch> = Vec::new();
        let current = match current_acknowledge_batch {
            None => return batches,
            Some(c) => c,
        };

        let types = current.acknowledge_types();
        let mut current_offset = current.first_offset();
        let mut current_start_index: i64 = 0;
        let mut records_with_same_acknowledge_type: i64 = 1;
        let size = types.len() as i64;
        let mut i: i64 = 1;
        while i < size {
            let acknowledge_type = types[i as usize];
            // If we have a continuous set of records with the same acknowledgement type exceeding the
            // default count, then we optimise the batches to include only start and end offset and have
            // only 1 acknowledge type in the array.
            let prev_acknowledge_type = types[(i - 1) as usize];
            if acknowledge_type == prev_acknowledge_type
                && records_with_same_acknowledge_type >= i64::from(MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE)
            {
                // We continue traversing until we have the same acknowledge type.
                while i < size {
                    let acknowledge_type2 = types[i as usize];
                    if acknowledge_type2 != types[(i - 1) as usize] {
                        break;
                    }
                    i += 1;
                    records_with_same_acknowledge_type += 1;
                }

                // Now we prepare 2 batches, one starting just before the batch with single acknowledge
                // type and one with the single acknowledge type.
                let mut batch1 = AcknowledgementBatch::new();
                batch1.set_first_offset(current_offset);
                batch1
                    .set_last_offset(current_offset + i - records_with_same_acknowledge_type - current_start_index - 1);
                if batch1.last_offset() >= batch1.first_offset() {
                    let slice =
                        types[current_start_index as usize..(i - records_with_same_acknowledge_type) as usize].to_vec();
                    batch1.set_acknowledge_types(slice);
                    batches.push(batch1);
                }

                let mut batch2 = AcknowledgementBatch::new();
                batch2.set_first_offset(current_offset + i - records_with_same_acknowledge_type - current_start_index);
                batch2.set_last_offset(current_offset + i - current_start_index - 1);
                batch2.acknowledge_types_mut().push(acknowledge_type);

                batches.push(batch2);
                records_with_same_acknowledge_type = 1;

                // Updating the offset and startIndex for further iterations.
                current_offset = current_offset + i - current_start_index;
                current_start_index = i;
            } else if acknowledge_type == prev_acknowledge_type {
                // The maximum limit has not yet been reached, we increment the count and move ahead.
                records_with_same_acknowledge_type += 1;
                i += 1;
            } else {
                records_with_same_acknowledge_type = 1;
                i += 1;
            }
        }
        if current_start_index < size {
            let mut batch = AcknowledgementBatch::new();
            batch.set_first_offset(current_offset);
            batch.set_last_offset(current_offset + size - current_start_index - 1);
            let slice = types[current_start_index as usize..size as usize].to_vec();
            batch.set_acknowledge_types(slice);
            batches.push(batch);
        }
        batches
    }

    /// Returns true if the array of acknowledge types contains a single
    /// acknowledge type and can be reduced to a single entry.
    ///
    /// Corresponds to Java's `canOptimiseForSingleAcknowledgeType`.
    fn can_optimise_for_single_acknowledge_type(acknowledgement_batch: &AcknowledgementBatch) -> bool {
        let types = acknowledgement_batch.acknowledge_types();
        if types.len() == 1 {
            return false;
        }
        let first_acknowledge_type = types[0];
        for &t in types.iter().skip(1) {
            if t != first_acknowledge_type {
                return false;
            }
        }
        true
    }
}

impl std::fmt::Display for Acknowledgements {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Acknowledgements({:?}, acknowledgeException=", self.acknowledgements)?;
        match &self.acknowledge_exception {
            Some(e) => write!(f, "{e}")?,
            None => write!(f, "null")?,
        }
        write!(f, ", completed={})", self.completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let acks = Acknowledgements::empty();
        assert!(acks.is_empty());
        assert_eq!(acks.size(), 0);
        assert!(acks.get_acknowledgement_batches().is_empty());
        assert!(!acks.is_completed());
        assert!(!acks.is_completed_exceptionally());
    }

    #[test]
    fn test_add_and_get() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, AcknowledgeType::Accept);
        assert_eq!(acks.get(0), Some(AcknowledgeType::Accept));
        // Overwrite.
        acks.add(0, AcknowledgeType::Reject);
        assert_eq!(acks.get(0), Some(AcknowledgeType::Reject));
        assert_eq!(acks.size(), 1);
    }

    #[test]
    fn test_add_if_absent() {
        let mut acks = Acknowledgements::empty();
        assert!(acks.add_if_absent(5, AcknowledgeType::Accept));
        assert!(!acks.add_if_absent(5, AcknowledgeType::Reject));
        assert_eq!(acks.get(5), Some(AcknowledgeType::Accept));
    }

    #[test]
    fn test_gap_reads_as_none() {
        let mut acks = Acknowledgements::empty();
        acks.add_gap(3);
        // Java's `get` returns null for both a gap and an absent offset.
        assert_eq!(acks.get(3), None);
        assert_eq!(acks.size(), 1);
    }

    #[test]
    fn test_single_contiguous_batch_mixed_types_not_collapsed() {
        // Contiguous offsets with mixed acknowledge types produce a single
        // batch whose type array is preserved (not collapsed, since the types
        // are not all identical).
        let mut acks = Acknowledgements::empty();
        acks.add(0, AcknowledgeType::Accept);
        acks.add(1, AcknowledgeType::Release);
        acks.add(2, AcknowledgeType::Accept);
        let batches = acks.get_acknowledgement_batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].first_offset(), 0);
        assert_eq!(batches[0].last_offset(), 2);
        assert_eq!(batches[0].acknowledge_types(), &vec![1, 2, 1]);
    }

    #[test]
    fn test_gap_splits_into_two_batches() {
        let mut acks = Acknowledgements::empty();
        acks.add(0, AcknowledgeType::Accept);
        // Offset 1 skipped -> the next offset breaks contiguity.
        acks.add(2, AcknowledgeType::Release);
        let batches = acks.get_acknowledgement_batches();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].first_offset(), 0);
        assert_eq!(batches[0].last_offset(), 0);
        assert_eq!(batches[1].first_offset(), 2);
        assert_eq!(batches[1].last_offset(), 2);
    }

    #[test]
    fn test_single_type_collapses_when_below_optimise_limit() {
        // Fewer than MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE records but all the
        // same type -> canOptimiseForSingleAcknowledgeType collapses the array.
        let mut acks = Acknowledgements::empty();
        for offset in 0..5 {
            acks.add(offset, AcknowledgeType::Accept);
        }
        let batches = acks.get_acknowledgement_batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].first_offset(), 0);
        assert_eq!(batches[0].last_offset(), 4);
        // Collapsed to a single acknowledge type.
        assert_eq!(batches[0].acknowledge_types(), &vec![1]);
    }

    #[test]
    fn test_merge_other_wins() {
        let mut a = Acknowledgements::empty();
        a.add(0, AcknowledgeType::Accept);
        let mut b = Acknowledgements::empty();
        b.add(0, AcknowledgeType::Reject);
        b.add(1, AcknowledgeType::Release);
        a.merge(&b);
        assert_eq!(a.get(0), Some(AcknowledgeType::Reject));
        assert_eq!(a.get(1), Some(AcknowledgeType::Release));
        assert_eq!(a.size(), 2);
    }

    #[test]
    fn test_complete_with_exception() {
        let mut acks = Acknowledgements::empty();
        acks.complete(Some(KafkaError::illegal_state("boom")));
        assert!(acks.is_completed());
        assert!(acks.is_completed_exceptionally());
        assert!(acks.get_acknowledge_exception().is_some());
    }

    #[test]
    fn test_optimise_long_run_of_same_type() {
        // A long run (> MAX) of the same type surrounded by other types must be
        // split so the long run carries a single acknowledge type.
        let mut acks = Acknowledgements::empty();
        acks.add(0, AcknowledgeType::Release);
        for offset in 1..=15 {
            acks.add(offset, AcknowledgeType::Accept);
        }
        acks.add(16, AcknowledgeType::Reject);
        let batches = acks.get_acknowledgement_batches();
        // All offsets are contiguous so the whole range is covered.
        let covered_first = batches.first().unwrap().first_offset();
        let covered_last = batches.last().unwrap().last_offset();
        assert_eq!(covered_first, 0);
        assert_eq!(covered_last, 16);
        // The middle run of Accept must be collapsed to a single-type batch.
        assert!(
            batches
                .iter()
                .any(|b| b.acknowledge_types() == &vec![1] && b.last_offset() - b.first_offset() >= 9),
            "expected a collapsed single-type run: {batches:?}"
        );
    }
}
