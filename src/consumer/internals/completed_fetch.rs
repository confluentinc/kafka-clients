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

//! `CompletedFetch` — per-partition batch state and record iteration.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.CompletedFetch`.
//!
//! **THIS IS THE ZERO-COPY HOT PATH** per `consumer-threading.md` §27.
//! Anti-patterns to flag in review:
//!
//! - `Bytes::copy_from_slice(records_bytes)` — must borrow from the
//!   underlying `Option<Vec<u8>>` in `PartitionData`.
//! - Eager `records_vec: Vec<ConsumerRecord>` field — must be lazy via a
//!   cursor.
//! - `String::from_utf8(topic_bytes.clone())` per record — clone the
//!   `Arc<str>` from `SubscriptionState` instead.
//! - Per-record `tokio::spawn` — forbidden by CLAUDE.md §11.
//!
//! Per-record allocation budget (expected, asserted in Phase 7b's
//! `FetchCollector` allocation test): only the user-supplied
//! `Deserializer<T>` allocations for the key and value `T`. No topic
//! allocation, no headers buffer clone, no fetch-buffer clone.
//!
//! This commit (Phase 7a 6/8) introduces the minimal struct shape needed
//! for `FetchBuffer` (commit 7/8 of this phase): `partition`, `is_consumed`
//! accessor, and a `drain()` no-op. The full per-record iteration,
//! aborted-transaction state, decompression, and `fetch_records` API
//! lands in Phase 7a commit 7/8 alongside `CompletedFetchTest`
//! translations.

#![allow(dead_code)]

use crate::common::TopicPartition;
use crate::fetch_response_data::PartitionData;

/// A batch of records returned for a single partition by a fetch request.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.CompletedFetch`.
///
/// # Minimal struct for Phase 7a commit 6
///
/// At this commit, the type carries only what `FetchBuffer` needs: the
/// partition and a flag indicating whether records have been drained. The
/// full record-iteration state lands in commit 7/8 of Phase 7a.
#[derive(Debug)]
pub(crate) struct CompletedFetch {
    /// The partition this batch belongs to.
    pub(crate) partition: TopicPartition,
    /// Raw response data — held to drive the §27 zero-copy iteration in
    /// commit 7. Borrowed by the per-record cursor.
    pub(crate) partition_data: PartitionData,
    /// Marks that all records have been consumed (or the batch was
    /// drained). Mirrors Java's `isConsumed`.
    is_consumed: bool,
    /// Set to true on the first call to `drain()` — Java's idempotent
    /// drain semantics rely on this.
    drained: bool,
}

impl CompletedFetch {
    /// Constructs a `CompletedFetch` for the given partition.
    ///
    /// In Java the constructor also takes a `SubscriptionState`,
    /// `BufferSupplier`, `FetchMetricsAggregator`, and a `fetchOffset`
    /// long. Those are wired in commit 7 alongside the full
    /// `fetch_records` implementation.
    pub(crate) fn new(partition: TopicPartition, partition_data: PartitionData) -> Self {
        Self { partition, partition_data, is_consumed: false, drained: false }
    }

    /// Returns whether the fetch has been fully consumed (or drained).
    ///
    /// Mirrors Java's `boolean isConsumed()`.
    pub(crate) fn is_consumed(&self) -> bool {
        self.is_consumed
    }

    /// Marks the fetch as consumed and releases any iteration state.
    ///
    /// Mirrors Java's `void drain()` — idempotent. The full impl in
    /// commit 7 closes the record stream, clears cached exceptions, and
    /// nudges `SubscriptionState::move_partition_to_end` when bytes were
    /// read. This commit's minimal impl just flips the flag.
    pub(crate) fn drain(&mut self) {
        if !self.drained {
            self.drained = true;
            self.is_consumed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    #[test]
    fn test_new_completed_fetch_not_consumed() {
        let cf = CompletedFetch::new(tp("t", 0), PartitionData::new());
        assert_eq!("t", cf.partition.topic());
        assert_eq!(0, cf.partition.partition());
        assert!(!cf.is_consumed());
    }

    #[test]
    fn test_drain_sets_consumed() {
        let mut cf = CompletedFetch::new(tp("t", 0), PartitionData::new());
        cf.drain();
        assert!(cf.is_consumed());
    }

    #[test]
    fn test_drain_is_idempotent() {
        let mut cf = CompletedFetch::new(tp("t", 0), PartitionData::new());
        cf.drain();
        cf.drain();
        assert!(cf.is_consumed());
    }
}
