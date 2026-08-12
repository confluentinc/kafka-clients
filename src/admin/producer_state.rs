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

//! State of an active producer as reported by `DescribeProducers`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ProducerState`.

/// The state of an active producer for a partition.
///
/// Corresponds to `org.apache.kafka.clients.admin.ProducerState`. `producer_id`
/// and `last_timestamp` are `i64` (Java `long`); `coordinator_epoch` and
/// `current_transaction_start_offset` are optional (Java `OptionalInt` /
/// `OptionalLong`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProducerState {
    producer_id: i64,
    producer_epoch: i32,
    last_sequence: i32,
    last_timestamp: i64,
    coordinator_epoch: Option<i32>,
    current_transaction_start_offset: Option<i64>,
}

impl ProducerState {
    /// Creates a new `ProducerState`.
    pub fn new(
        producer_id: i64,
        producer_epoch: i32,
        last_sequence: i32,
        last_timestamp: i64,
        coordinator_epoch: Option<i32>,
        current_transaction_start_offset: Option<i64>,
    ) -> Self {
        Self {
            producer_id,
            producer_epoch,
            last_sequence,
            last_timestamp,
            coordinator_epoch,
            current_transaction_start_offset,
        }
    }

    /// The producer id.
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// The producer epoch.
    pub fn producer_epoch(&self) -> i32 {
        self.producer_epoch
    }

    /// The last sequence number written by this producer.
    pub fn last_sequence(&self) -> i32 {
        self.last_sequence
    }

    /// The last timestamp written by this producer.
    pub fn last_timestamp(&self) -> i64 {
        self.last_timestamp
    }

    /// The offset of the first record in the current transaction, if any.
    pub fn current_transaction_start_offset(&self) -> Option<i64> {
        self.current_transaction_start_offset
    }

    /// The coordinator epoch, if any.
    pub fn coordinator_epoch(&self) -> Option<i32> {
        self.coordinator_epoch
    }
}

impl std::fmt::Display for ProducerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProducerState(producerId={}, producerEpoch={}, lastSequence={}, lastTimestamp={}, \
             coordinatorEpoch={:?}, currentTransactionStartOffset={:?})",
            self.producer_id,
            self.producer_epoch,
            self.last_sequence,
            self.last_timestamp,
            self.coordinator_epoch,
            self.current_transaction_start_offset,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_return_constructor_values() {
        let state = ProducerState::new(12345, 15, 9, 1_600_000_000_000, Some(3), Some(100));
        assert_eq!(state.producer_id(), 12345);
        assert_eq!(state.producer_epoch(), 15);
        assert_eq!(state.last_sequence(), 9);
        assert_eq!(state.last_timestamp(), 1_600_000_000_000);
        assert_eq!(state.coordinator_epoch(), Some(3));
        assert_eq!(state.current_transaction_start_offset(), Some(100));
    }

    #[test]
    fn equality_uses_all_fields() {
        let a = ProducerState::new(1, 2, 3, 4, Some(5), None);
        let b = ProducerState::new(1, 2, 3, 4, Some(5), None);
        assert_eq!(a, b);
        assert_ne!(a, ProducerState::new(1, 2, 3, 4, Some(5), Some(6)));
    }
}
