// Copyright 2026 Confluent Inc.
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

//! A description of the assignments of a specific share group member.

use std::collections::HashSet;

use crate::common::TopicPartition;

/// A description of the assignments of a specific share group member.
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.clients.admin.ShareMemberAssignment")]
pub struct ShareMemberAssignment {
    topic_partitions: HashSet<TopicPartition>,
}

impl ShareMemberAssignment {
    /// Creates an instance with the specified parameters.
    ///
    /// # Arguments
    ///
    /// - `topic_partitions` - List of topic partitions
    #[doc(alias = "org.apache.kafka.clients.admin.ShareMemberAssignment#ShareMemberAssignment")]
    pub fn new(topic_partitions: HashSet<TopicPartition>) -> Self {
        Self { topic_partitions }
    }

    /// Returns the topic partitions assigned to a group member.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareMemberAssignment#topicPartitions")]
    pub fn topic_partitions(&self) -> &HashSet<TopicPartition> {
        &self.topic_partitions
    }
}

impl std::hash::Hash for ShareMemberAssignment {
    // Java's `ShareMemberAssignment.hashCode()` hashes `topicPartitions`, a set,
    // independently of its order.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::common::utils::Utils::hash_unordered(&self.topic_partitions, state);
    }
}

impl std::fmt::Display for ShareMemberAssignment {
    /// Java's `toString()`: `(topicPartitions=t-0,t-1)`. Java joins the set in its
    /// iteration order, which is unspecified; the partitions are sorted here so
    /// the text is stable, as `MemberAssignment` does.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts: Vec<String> = self.topic_partitions.iter().map(ToString::to_string).collect();
        parts.sort();
        write!(f, "(topicPartitions={})", parts.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// New test, no Java original (Java has no `ShareMemberAssignmentTest`):
    /// `equals` compares the sets, so the insertion order does not matter.
    #[test]
    fn equality_is_set_based() {
        let a = ShareMemberAssignment::new(HashSet::from([TopicPartition::new("t", 0), TopicPartition::new("t", 1)]));
        let b = ShareMemberAssignment::new(HashSet::from([TopicPartition::new("t", 1), TopicPartition::new("t", 0)]));
        assert_eq!(a, b);
        assert_ne!(a, ShareMemberAssignment::new(HashSet::from([TopicPartition::new("t", 0)])));
    }

    /// New test, no Java original: equal assignments hash equally, whatever
    /// order the set iterates in.
    #[test]
    fn equal_assignments_hash_equally() {
        use std::hash::{BuildHasher, RandomState};
        let state = RandomState::new();
        let a = ShareMemberAssignment::new((0..16).map(|p| TopicPartition::new("t", p)).collect());
        let b = ShareMemberAssignment::new((0..16).rev().map(|p| TopicPartition::new("t", p)).collect());
        assert_eq!(state.hash_one(&a), state.hash_one(&b));
    }

    /// New test, no Java original: `toString()` follows Java's format.
    #[test]
    fn to_string_follows_java() {
        let assignment =
            ShareMemberAssignment::new(HashSet::from([TopicPartition::new("t", 1), TopicPartition::new("t", 0)]));
        assert_eq!(assignment.to_string(), "(topicPartitions=t-0,t-1)");
        assert_eq!(ShareMemberAssignment::new(HashSet::new()).to_string(), "(topicPartitions=)");
    }
}
