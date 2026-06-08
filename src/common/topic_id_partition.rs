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

//! A topic-partition pair carrying the topic's universally unique identifier.
//!
//! Translated from `org.apache.kafka.common.TopicIdPartition`. Distinguishes
//! topics recreated under the same name — two topics with the same name and
//! partition layout have different [`TopicIdPartition`]s if their [`Uuid`]s
//! differ.

use std::fmt;

use crate::common::TopicPartition;
use crate::common::Uuid;

/// A universally unique identifier paired with a [`TopicPartition`].
///
/// Corresponds to Java's `org.apache.kafka.common.TopicIdPartition`.
#[derive(Clone, Debug)]
pub struct TopicIdPartition {
    topic_id: Uuid,
    topic_partition: TopicPartition,
}

impl TopicIdPartition {
    /// Creates an instance with the provided topic ID and topic partition.
    ///
    /// Translates Java's `TopicIdPartition(Uuid topicId, TopicPartition topicPartition)`.
    pub fn new(topic_id: Uuid, topic_partition: TopicPartition) -> Self {
        Self { topic_id, topic_partition }
    }

    /// Creates an instance from a topic ID, partition number, and topic name.
    ///
    /// Translates Java's `TopicIdPartition(Uuid topicId, int partition, String topic)`.
    pub fn from_parts(topic_id: Uuid, partition: i32, topic: impl Into<String>) -> Self {
        Self { topic_id, topic_partition: TopicPartition::new(topic.into(), partition) }
    }

    /// Returns the universally unique id representing this topic partition.
    pub fn topic_id(&self) -> Uuid {
        self.topic_id
    }

    /// Returns the topic name (may be empty if unknown).
    pub fn topic(&self) -> &str {
        self.topic_partition.topic()
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.topic_partition.partition()
    }

    /// Returns the underlying topic partition.
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }
}

impl PartialEq for TopicIdPartition {
    fn eq(&self, other: &Self) -> bool {
        self.topic_id == other.topic_id && self.topic_partition == other.topic_partition
    }
}

impl Eq for TopicIdPartition {}

impl std::hash::Hash for TopicIdPartition {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.topic_id.hash(state);
        self.topic_partition.hash(state);
    }
}

impl fmt::Display for TopicIdPartition {
    /// Matches Java's `toString()`: `<topicId>:<topic>-<partition>`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}-{}",
            self.topic_id,
            self.topic_partition.topic(),
            self.topic_partition.partition()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construction_via_parts() {
        let id = Uuid::new(1, 2);
        let tip = TopicIdPartition::from_parts(id, 7, "mytopic");
        assert_eq!(id, tip.topic_id());
        assert_eq!("mytopic", tip.topic());
        assert_eq!(7, tip.partition());
    }

    #[test]
    fn test_construction_via_topic_partition() {
        let id = Uuid::new(1, 2);
        let tp = TopicPartition::new("mytopic".to_string(), 3);
        let tip = TopicIdPartition::new(id, tp.clone());
        assert_eq!(&tp, tip.topic_partition());
    }

    #[test]
    fn test_equality() {
        let id_a = Uuid::new(1, 2);
        let id_b = Uuid::new(3, 4);
        let tp_0 = TopicPartition::new("t".to_string(), 0);
        let tp_1 = TopicPartition::new("t".to_string(), 1);

        let tip_a0 = TopicIdPartition::new(id_a, tp_0.clone());
        let tip_a0_again = TopicIdPartition::new(id_a, tp_0.clone());
        let tip_a1 = TopicIdPartition::new(id_a, tp_1);
        let tip_b0 = TopicIdPartition::new(id_b, tp_0);

        assert_eq!(tip_a0, tip_a0_again);
        assert_ne!(tip_a0, tip_a1);
        assert_ne!(tip_a0, tip_b0);
    }

    #[test]
    fn test_hash_consistency() {
        use std::collections::HashSet;
        let id = Uuid::new(1, 2);
        let mut set = HashSet::new();
        set.insert(TopicIdPartition::from_parts(id, 0, "t"));
        set.insert(TopicIdPartition::from_parts(id, 0, "t"));
        assert_eq!(set.len(), 1);
        set.insert(TopicIdPartition::from_parts(id, 1, "t"));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_display() {
        let id = Uuid::new(0x1111_2222_3333_4444, 0x5555_6666_7777_8888);
        let tip = TopicIdPartition::from_parts(id, 5, "mytopic");
        // Display delegates to Uuid::Display + ":<topic>-<partition>".
        let s = format!("{tip}");
        assert!(s.ends_with(":mytopic-5"), "got {s}");
        assert!(s.starts_with(&id.to_string()), "got {s}");
    }
}
