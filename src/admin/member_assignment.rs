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

//! A description of the assignments of a specific group member.
//!
//! Corresponds to `org.apache.kafka.clients.admin.MemberAssignment`.

use std::collections::HashSet;

use crate::common::TopicPartition;

/// A description of the assignments of a specific group member.
///
/// Corresponds to `org.apache.kafka.clients.admin.MemberAssignment`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemberAssignment {
    topic_partitions: HashSet<TopicPartition>,
}

impl MemberAssignment {
    /// Creates an instance with the specified topic partitions.
    ///
    /// Mirrors `new MemberAssignment(Set<TopicPartition>)`.
    pub fn new(topic_partitions: HashSet<TopicPartition>) -> Self {
        Self { topic_partitions }
    }

    /// The topic partitions assigned to a group member.
    ///
    /// Mirrors `topicPartitions()`.
    pub fn topic_partitions(&self) -> &HashSet<TopicPartition> {
        &self.topic_partitions
    }
}

impl std::fmt::Display for MemberAssignment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts: Vec<String> = self.topic_partitions.iter().map(|tp| tp.to_string()).collect();
        parts.sort();
        write!(f, "(topicPartitions={})", parts.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equality_is_set_based() {
        let a = MemberAssignment::new(HashSet::from([TopicPartition::new("t", 0), TopicPartition::new("t", 1)]));
        let b = MemberAssignment::new(HashSet::from([TopicPartition::new("t", 1), TopicPartition::new("t", 0)]));
        assert_eq!(a, b);
    }

    #[test]
    fn empty_default() {
        assert!(MemberAssignment::default().topic_partitions().is_empty());
    }
}
