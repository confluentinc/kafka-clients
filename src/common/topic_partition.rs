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

//! A topic name and partition number.

use std::fmt;

/// A topic name and partition number.
#[derive(Clone, Debug, Eq)]
pub struct TopicPartition {
    partition: i32,
    topic: String,
}

impl TopicPartition {
    /// Creates a new `TopicPartition` with the given topic and partition.
    pub fn new(topic: String, partition: i32) -> Self {
        Self { partition, topic }
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl PartialEq for TopicPartition {
    fn eq(&self, other: &Self) -> bool {
        self.partition == other.partition && self.topic == other.topic
    }
}

impl std::hash::Hash for TopicPartition {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.partition.hash(state);
        self.topic.hash(state);
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
    fn test_topic_partition_creation() {
        let tp = TopicPartition::new("mytopic".to_string(), 5);
        assert_eq!(tp.partition(), 5);
        assert_eq!(tp.topic(), "mytopic");
    }

    #[test]
    fn test_topic_partition_equality() {
        let tp1 = TopicPartition::new("test".to_string(), 0);
        let tp2 = TopicPartition::new("test".to_string(), 0);
        let tp3 = TopicPartition::new("test".to_string(), 1);
        let tp4 = TopicPartition::new("other".to_string(), 0);

        assert_eq!(tp1, tp2);
        assert_ne!(tp1, tp3);
        assert_ne!(tp1, tp4);
    }

    #[test]
    fn test_topic_partition_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TopicPartition::new("test".to_string(), 0));
        set.insert(TopicPartition::new("test".to_string(), 0));
        assert_eq!(set.len(), 1);

        set.insert(TopicPartition::new("test".to_string(), 1));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_topic_partition_display() {
        let tp = TopicPartition::new("mytopic".to_string(), 5);
        assert_eq!(tp.to_string(), "mytopic-5");
    }
}
