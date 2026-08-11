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

//! A detailed description of a single topic in the cluster.
//!
//! Corresponds to `org.apache.kafka.clients.admin.TopicDescription`.

use std::collections::BTreeSet;

use crate::common::acl::AclOperation;
use crate::common::{TopicPartitionInfo, Uuid};

/// A detailed description of a single topic in the cluster.
///
/// Corresponds to `org.apache.kafka.clients.admin.TopicDescription`.
///
/// `authorized_operations` uses `BTreeSet` for a deterministic ordering; Java
/// uses an unordered `Set<AclOperation>`.
///
/// Equality mirrors Java's `TopicDescription.equals`, which compares `name`,
/// `internal`, `partitions` and `authorized_operations` but **not** `topic_id`
/// — see the manual [`PartialEq`] impl below.
#[derive(Clone, Debug, Eq)]
pub struct TopicDescription {
    name: String,
    internal: bool,
    partitions: Vec<TopicPartitionInfo>,
    authorized_operations: BTreeSet<AclOperation>,
    topic_id: Uuid,
}

impl PartialEq for TopicDescription {
    fn eq(&self, other: &Self) -> bool {
        self.internal == other.internal
            && self.name == other.name
            && self.partitions == other.partitions
            && self.authorized_operations == other.authorized_operations
    }
}

impl TopicDescription {
    /// Create an instance with name, internal flag and partitions (empty
    /// authorized operations, zero topic id).
    pub fn new(name: impl Into<String>, internal: bool, partitions: Vec<TopicPartitionInfo>) -> Self {
        Self::with_authorized_operations(name, internal, partitions, BTreeSet::new(), Uuid::zero())
    }

    /// Create an instance with the specified parameters.
    ///
    /// * `name` - the topic name
    /// * `internal` - whether the topic is internal to Kafka
    /// * `partitions` - a list of partitions where the index represents the
    ///   partition id and the element contains leadership and replica
    ///   information for that partition
    /// * `authorized_operations` - authorized operations for this topic, or
    ///   empty set if this is not known
    /// * `topic_id` - the topic id
    pub fn with_authorized_operations(
        name: impl Into<String>,
        internal: bool,
        partitions: Vec<TopicPartitionInfo>,
        authorized_operations: BTreeSet<AclOperation>,
        topic_id: Uuid,
    ) -> Self {
        Self { name: name.into(), internal, partitions, authorized_operations, topic_id }
    }

    /// The name of the topic.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether the topic is internal to Kafka. An example of an internal topic
    /// is the offsets and group management topic: `__consumer_offsets`.
    pub fn is_internal(&self) -> bool {
        self.internal
    }

    /// The topic id.
    pub fn topic_id(&self) -> Uuid {
        self.topic_id
    }

    /// A list of partitions where the index represents the partition id and the
    /// element contains leadership and replica information for that partition.
    pub fn partitions(&self) -> &[TopicPartitionInfo] {
        &self.partitions
    }

    /// Authorized operations for this topic, or an empty set if this is not
    /// known.
    pub fn authorized_operations(&self) -> &BTreeSet<AclOperation> {
        &self.authorized_operations
    }
}

impl std::fmt::Display for TopicDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let partitions = self.partitions.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(",");
        write!(
            f,
            "(name={}, internal={}, partitions={}, authorizedOperations={:?})",
            self.name, self.internal, partitions, self.authorized_operations
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_constructor_defaults() {
        let desc = TopicDescription::new("t", false, vec![]);
        assert_eq!(desc.name(), "t");
        assert!(!desc.is_internal());
        assert!(desc.partitions().is_empty());
        assert!(desc.authorized_operations().is_empty());
        assert_eq!(desc.topic_id(), Uuid::zero());
    }

    #[test]
    fn equality_ignores_topic_id_like_java() {
        // Java TopicDescription.equals does NOT compare topicId.
        let a = TopicDescription::with_authorized_operations("t", false, vec![], BTreeSet::new(), Uuid::new(1, 1));
        let b = TopicDescription::with_authorized_operations("t", false, vec![], BTreeSet::new(), Uuid::new(2, 2));
        // Only topic_id differs, which equals() ignores -> equal.
        assert_eq!(a, b);
    }

    #[test]
    fn equality_compares_name_and_partitions() {
        let a = TopicDescription::new("t", false, vec![]);
        let b = TopicDescription::new("other", false, vec![]);
        assert_ne!(a, b);
    }
}
