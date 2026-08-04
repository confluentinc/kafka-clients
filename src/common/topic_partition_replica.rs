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

//! The topic name, partition number and the brokerId of a replica.

use std::fmt;

/// The topic name, partition number and the brokerId of the replica.
///
/// Corresponds to `org.apache.kafka.common.TopicPartitionReplica`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopicPartitionReplica {
    broker_id: i32,
    partition: i32,
    topic: String,
}

impl TopicPartitionReplica {
    /// Creates a new `TopicPartitionReplica`.
    pub fn new(topic: impl Into<String>, partition: i32, broker_id: i32) -> Self {
        Self { broker_id, partition, topic: topic.into() }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the broker id.
    pub fn broker_id(&self) -> i32 {
        self.broker_id
    }
}

impl fmt::Display for TopicPartitionReplica {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", self.topic, self.partition, self.broker_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn accessors() {
        let tpr = TopicPartitionReplica::new("topic", 12, 1);
        assert_eq!(tpr.topic(), "topic");
        assert_eq!(tpr.partition(), 12);
        assert_eq!(tpr.broker_id(), 1);
    }

    #[test]
    fn equality_and_hash() {
        let a = TopicPartitionReplica::new("topic", 12, 1);
        let b = TopicPartitionReplica::new("topic", 12, 1);
        let c = TopicPartitionReplica::new("topic", 12, 2);
        let d = TopicPartitionReplica::new("other", 12, 1);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);

        let mut set = HashSet::new();
        set.insert(a.clone());
        set.insert(b);
        assert_eq!(set.len(), 1);
        set.insert(c);
        assert_eq!(set.len(), 2);
    }

    /// Mirrors Java's `TopicPartitionReplica.toString` (`"%s-%d-%d"`).
    #[test]
    fn display_format() {
        let tpr = TopicPartitionReplica::new("topic", 12, 3);
        assert_eq!(tpr.to_string(), "topic-12-3");
    }
}
