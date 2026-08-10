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
/// uses an unordered `Set<AclOperation>`. It is `Option` because Java's field is
/// nullable: `KafkaAdminClient` fills it from `AdminUtils.validAclOperations`,
/// which returns `null` when the broker did not report the operations — distinct
/// from a broker reporting an empty set.
///
/// Equality mirrors Java's `TopicDescription.equals`, which compares `name`,
/// `internal`, `partitions` and `authorized_operations` but **not** `topic_id`
/// — see the manual [`PartialEq`] impl below.
#[derive(Clone, Debug, Eq)]
pub struct TopicDescription {
    name: String,
    internal: bool,
    partitions: Vec<TopicPartitionInfo>,
    authorized_operations: Option<BTreeSet<AclOperation>>,
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
    ///
    /// Mirrors `TopicDescription(String, boolean, List<TopicPartitionInfo>)`,
    /// which passes `Collections.emptySet()` — a reported-but-empty set, i.e.
    /// `Some(empty)` rather than `None`.
    pub fn new(name: impl Into<String>, internal: bool, partitions: Vec<TopicPartitionInfo>) -> Self {
        Self::with_authorized_operations(name, internal, partitions, Some(BTreeSet::new()), Uuid::zero())
    }

    /// Create an instance with the specified parameters.
    ///
    /// * `name` - the topic name
    /// * `internal` - whether the topic is internal to Kafka
    /// * `partitions` - a list of partitions where the index represents the
    ///   partition id and the element contains leadership and replica
    ///   information for that partition
    /// * `authorized_operations` - authorized operations for this topic, or
    ///   `None` if this is not known (Java's nullable `Set<AclOperation>`)
    /// * `topic_id` - the topic id
    pub fn with_authorized_operations(
        name: impl Into<String>,
        internal: bool,
        partitions: Vec<TopicPartitionInfo>,
        authorized_operations: Option<BTreeSet<AclOperation>>,
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

    /// Authorized operations for this topic, or `None` if the broker did not
    /// report them (Java returns `null` in that case). `Some` holding an empty
    /// set means the broker reported that no operation is authorized.
    pub fn authorized_operations(&self) -> Option<&BTreeSet<AclOperation>> {
        self.authorized_operations.as_ref()
    }
}

impl std::fmt::Display for TopicDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let partitions = self.partitions.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(",");
        // Java concatenates the nullable set directly, so an absent set prints
        // as "null".
        let operations = match &self.authorized_operations {
            Some(operations) => format!("{operations:?}"),
            None => "null".to_string(),
        };
        write!(
            f,
            "(name={}, internal={}, partitions={}, authorizedOperations={})",
            self.name, self.internal, partitions, operations
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
        // Java's 3-arg constructor passes an empty set, not null.
        assert_eq!(desc.authorized_operations(), Some(&BTreeSet::new()));
        assert_eq!(desc.topic_id(), Uuid::zero());
    }

    #[test]
    fn equality_ignores_topic_id_like_java() {
        // Java TopicDescription.equals does NOT compare topicId.
        let a =
            TopicDescription::with_authorized_operations("t", false, vec![], Some(BTreeSet::new()), Uuid::new(1, 1));
        let b =
            TopicDescription::with_authorized_operations("t", false, vec![], Some(BTreeSet::new()), Uuid::new(2, 2));
        // Only topic_id differs, which equals() ignores -> equal.
        assert_eq!(a, b);
    }

    #[test]
    fn equality_compares_name_and_partitions() {
        let a = TopicDescription::new("t", false, vec![]);
        let b = TopicDescription::new("other", false, vec![]);
        assert_ne!(a, b);
    }

    #[test]
    fn equality_separates_unreported_from_reported_empty_operations() {
        // Java's equals uses Objects.equals on the nullable set, so null and an
        // empty set are different topics.
        let unreported = TopicDescription::with_authorized_operations("t", false, vec![], None, Uuid::zero());
        let reported_empty =
            TopicDescription::with_authorized_operations("t", false, vec![], Some(BTreeSet::new()), Uuid::zero());
        assert_ne!(unreported, reported_empty);
        assert_eq!(unreported.authorized_operations(), None);
        assert_eq!(reported_empty.authorized_operations(), Some(&BTreeSet::new()));
    }
}
