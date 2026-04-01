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

//! TopicPartition identifies a specific partition of a Kafka topic.
//!
//! Corresponds to org.apache.kafka.common.TopicPartition.

use std::fmt;

/// Identifies a specific partition of a topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    topic: String,
    partition: i32,
}

impl TopicPartition {
    /// Create a new TopicPartition.
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        TopicPartition {
            topic: topic.into(),
            partition,
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.partition
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_partition() {
        let tp = TopicPartition::new("my-topic", 3);
        assert_eq!(tp.topic(), "my-topic");
        assert_eq!(tp.partition(), 3);
        assert_eq!(format!("{tp}"), "my-topic-3");
    }

    #[test]
    fn test_equality_and_hash() {
        use std::collections::HashSet;
        let tp1 = TopicPartition::new("topic", 0);
        let tp2 = TopicPartition::new("topic", 0);
        let tp3 = TopicPartition::new("topic", 1);
        assert_eq!(tp1, tp2);
        assert_ne!(tp1, tp3);

        let mut set = HashSet::new();
        set.insert(tp1.clone());
        assert!(set.contains(&tp2));
        assert!(!set.contains(&tp3));
    }
}
