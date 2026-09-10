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

//! Specification of consumer group offsets to list.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec`.

use crate::common::TopicPartition;

/// Specification of consumer group offsets to list using
/// `Admin::list_consumer_group_offsets`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec`.
///
/// `topic_partitions == None` includes all topic partitions of the group.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListConsumerGroupOffsetsSpec {
    topic_partitions: Option<Vec<TopicPartition>>,
}

impl ListConsumerGroupOffsetsSpec {
    /// Creates a spec that lists all topic partitions of the group.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the topic partitions whose offsets are to be listed for a consumer
    /// group. `None` includes all topic partitions.
    ///
    /// Mirrors `topicPartitions(Collection<TopicPartition>)`.
    #[must_use]
    pub fn set_topic_partitions(mut self, topic_partitions: Option<Vec<TopicPartition>>) -> Self {
        self.topic_partitions = topic_partitions;
        self
    }

    /// Returns the topic partitions whose offsets are to be listed for a
    /// consumer group. `None` indicates that offsets of all partitions of the
    /// group are to be listed.
    ///
    /// Mirrors `topicPartitions()`.
    pub fn topic_partitions(&self) -> Option<&[TopicPartition]> {
        self.topic_partitions.as_deref()
    }
}

impl std::fmt::Display for ListConsumerGroupOffsetsSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListConsumerGroupOffsetsSpec(topicPartitions={:?})", self.topic_partitions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_all_partitions() {
        let spec = ListConsumerGroupOffsetsSpec::new();
        assert_eq!(spec.topic_partitions(), None);
    }

    #[test]
    fn carries_topic_partitions() {
        let tps = vec![TopicPartition::new("t", 0), TopicPartition::new("t", 1)];
        let spec = ListConsumerGroupOffsetsSpec::new().set_topic_partitions(Some(tps.clone()));
        assert_eq!(spec.topic_partitions(), Some(tps.as_slice()));
    }

    #[test]
    fn equality_matches_java_semantics() {
        let a = ListConsumerGroupOffsetsSpec::new().set_topic_partitions(Some(vec![TopicPartition::new("t", 0)]));
        let b = ListConsumerGroupOffsetsSpec::new().set_topic_partitions(Some(vec![TopicPartition::new("t", 0)]));
        assert_eq!(a, b);
    }
}
