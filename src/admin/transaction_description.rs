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

//! The description of a transaction, as reported by `DescribeTransactions`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.TransactionDescription`.

use std::collections::HashSet;

use crate::admin::TransactionState;
use crate::common::TopicPartition;

/// The description of a transaction.
///
/// Corresponds to `org.apache.kafka.clients.admin.TransactionDescription`.
/// `producer_id` and `transaction_timeout_ms` are `i64` (Java `long`);
/// `coordinator_id` and `producer_epoch` are `i32` (Java `int`);
/// `transaction_start_time_ms` is optional (Java `OptionalLong`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionDescription {
    coordinator_id: i32,
    state: TransactionState,
    producer_id: i64,
    producer_epoch: i32,
    transaction_timeout_ms: i64,
    transaction_start_time_ms: Option<i64>,
    topic_partitions: HashSet<TopicPartition>,
}

impl TransactionDescription {
    /// Creates a new `TransactionDescription`.
    pub fn new(
        coordinator_id: i32,
        state: TransactionState,
        producer_id: i64,
        producer_epoch: i32,
        transaction_timeout_ms: i64,
        transaction_start_time_ms: Option<i64>,
        topic_partitions: HashSet<TopicPartition>,
    ) -> Self {
        Self {
            coordinator_id,
            state,
            producer_id,
            producer_epoch,
            transaction_timeout_ms,
            transaction_start_time_ms,
            topic_partitions,
        }
    }

    /// The id of the coordinator that owns this transaction.
    pub fn coordinator_id(&self) -> i32 {
        self.coordinator_id
    }

    /// The current state of the transaction.
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// The producer id of the transaction.
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// The producer epoch of the transaction.
    pub fn producer_epoch(&self) -> i32 {
        self.producer_epoch
    }

    /// The transaction timeout in milliseconds.
    pub fn transaction_timeout_ms(&self) -> i64 {
        self.transaction_timeout_ms
    }

    /// The transaction start time in milliseconds, if a transaction is in
    /// progress.
    pub fn transaction_start_time_ms(&self) -> Option<i64> {
        self.transaction_start_time_ms
    }

    /// The set of topic partitions that have been added to the transaction.
    pub fn topic_partitions(&self) -> &HashSet<TopicPartition> {
        &self.topic_partitions
    }
}

impl std::fmt::Display for TransactionDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TransactionDescription(coordinatorId={}, state={}, producerId={}, producerEpoch={}, \
             transactionTimeoutMs={}, transactionStartTimeMs={:?}, topicPartitions={:?})",
            self.coordinator_id,
            self.state,
            self.producer_id,
            self.producer_epoch,
            self.transaction_timeout_ms,
            self.transaction_start_time_ms,
            self.topic_partitions,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_return_constructor_values() {
        let partitions = HashSet::from([TopicPartition::new("foo", 1)]);
        let d = TransactionDescription::new(
            2,
            TransactionState::Ongoing,
            12345,
            15,
            10000,
            Some(1_599_151_791),
            partitions.clone(),
        );
        assert_eq!(d.coordinator_id(), 2);
        assert_eq!(d.state(), TransactionState::Ongoing);
        assert_eq!(d.producer_id(), 12345);
        assert_eq!(d.producer_epoch(), 15);
        assert_eq!(d.transaction_timeout_ms(), 10000);
        assert_eq!(d.transaction_start_time_ms(), Some(1_599_151_791));
        assert_eq!(d.topic_partitions(), &partitions);
    }

    #[test]
    fn equality_compares_all_fields() {
        let a = TransactionDescription::new(1, TransactionState::Empty, 1, 2, 3, None, HashSet::new());
        let b = TransactionDescription::new(1, TransactionState::Empty, 1, 2, 3, None, HashSet::new());
        assert_eq!(a, b);
        assert_ne!(
            a,
            TransactionDescription::new(1, TransactionState::Ongoing, 1, 2, 3, None, HashSet::new())
        );
    }
}
