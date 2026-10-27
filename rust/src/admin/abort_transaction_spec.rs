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

//! The specification of a transaction to abort.

use crate::common::TopicPartition;

/// The identifiers required to abort a hanging transaction on a partition.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.admin.AbortTransactionSpec")]
pub struct AbortTransactionSpec {
    topic_partition: TopicPartition,
    producer_id: i64,
    producer_epoch: i16,
    coordinator_epoch: i32,
}

impl AbortTransactionSpec {
    /// Creates a new `AbortTransactionSpec`.
    ///
    /// # Arguments
    ///
    /// - `topic_partition` - the partition of the hanging transaction
    /// - `producer_id` - the producer id of the hanging transaction
    /// - `producer_epoch` - the producer epoch of the hanging transaction
    /// - `coordinator_epoch` - the coordinator epoch of the hanging transaction
    #[doc(alias = "org.apache.kafka.clients.admin.AbortTransactionSpec#AbortTransactionSpec")]
    pub fn new(topic_partition: TopicPartition, producer_id: i64, producer_epoch: i16, coordinator_epoch: i32) -> Self {
        Self { topic_partition, producer_id, producer_epoch, coordinator_epoch }
    }

    /// Returns the topic partition of the hanging transaction.
    #[doc(alias = "org.apache.kafka.clients.admin.AbortTransactionSpec#topicPartition")]
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }

    /// Returns the producer id of the hanging transaction.
    #[doc(alias = "org.apache.kafka.clients.admin.AbortTransactionSpec#producerId")]
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns the producer epoch of the hanging transaction.
    #[doc(alias = "org.apache.kafka.clients.admin.AbortTransactionSpec#producerEpoch")]
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Returns the coordinator epoch of the hanging transaction.
    #[doc(alias = "org.apache.kafka.clients.admin.AbortTransactionSpec#coordinatorEpoch")]
    pub fn coordinator_epoch(&self) -> i32 {
        self.coordinator_epoch
    }
}

impl std::fmt::Display for AbortTransactionSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AbortTransactionSpec(topicPartition={}, producerId={}, producerEpoch={}, coordinatorEpoch={})",
            self.topic_partition, self.producer_id, self.producer_epoch, self.coordinator_epoch,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_return_constructor_values() {
        let spec = AbortTransactionSpec::new(TopicPartition::new("t", 3), 12345, 7, 4);
        assert_eq!(spec.topic_partition(), &TopicPartition::new("t", 3));
        assert_eq!(spec.producer_id(), 12345);
        assert_eq!(spec.producer_epoch(), 7);
        assert_eq!(spec.coordinator_epoch(), 4);
    }

    #[test]
    fn display_matches_java() {
        let spec = AbortTransactionSpec::new(TopicPartition::new("t", 3), 12345, 7, 4);
        assert_eq!(
            spec.to_string(),
            "AbortTransactionSpec(topicPartition=t-3, producerId=12345, producerEpoch=7, coordinatorEpoch=4)"
        );
    }
}
