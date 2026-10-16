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

//! A detailed description of a single classic group in the cluster.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ClassicGroupDescription`.

use std::collections::BTreeSet;

use crate::admin::MemberDescription;
use crate::common::acl::AclOperation;
use crate::common::{ClassicGroupState, Node};

/// A detailed description of a single classic group in the cluster.
///
/// Corresponds to `org.apache.kafka.clients.admin.ClassicGroupDescription`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.clients.admin.ClassicGroupDescription")]
pub struct ClassicGroupDescription {
    group_id: String,
    protocol: String,
    protocol_data: String,
    members: Vec<MemberDescription>,
    state: ClassicGroupState,
    coordinator: Option<Node>,
    /// `Option` because Java's field is nullable (see
    /// [`ConsumerGroupDescription`](crate::admin::ConsumerGroupDescription)).
    authorized_operations: Option<BTreeSet<AclOperation>>,
}

impl ClassicGroupDescription {
    /// Creates a classic group description with no authorized operations.
    ///
    /// Mirrors `ClassicGroupDescription(String, String, String,
    /// Collection<MemberDescription>, ClassicGroupState, Node)`
    /// (`ClassicGroupDescription.java:44-51`), which passes `Set.of()`, an
    /// empty set rather than `null`, as the authorized operations. Its
    /// parameters are the intersection of Java's two constructors, so it owns
    /// the plain name (CLAUDE.md §2). `coordinator` is `None` when the
    /// coordinator is not known (Java's nullable `Node`).
    #[doc(
        alias = "org.apache.kafka.clients.admin.ClassicGroupDescription#ClassicGroupDescription(String,String,String,Collection,ClassicGroupState,Node)"
    )]
    pub fn new(
        group_id: impl Into<String>,
        protocol: impl Into<String>,
        protocol_data: impl Into<String>,
        members: Vec<MemberDescription>,
        state: ClassicGroupState,
        coordinator: Option<Node>,
    ) -> Self {
        Self::with_authorized_operations(
            group_id,
            protocol,
            protocol_data,
            members,
            state,
            coordinator,
            Some(BTreeSet::new()),
        )
    }

    /// Creates a classic group description with all fields.
    ///
    /// Mirrors `ClassicGroupDescription(String, String, String,
    /// Collection<MemberDescription>, ClassicGroupState, Node,
    /// Set<AclOperation>)` (`ClassicGroupDescription.java:53-67`).
    /// `coordinator` is `None` when the coordinator is not known (Java's
    /// nullable `Node`), and `authorized_operations` is `None` when the broker
    /// did not report them (Java's `null`).
    #[doc(
        alias = "org.apache.kafka.clients.admin.ClassicGroupDescription#ClassicGroupDescription(String,String,String,Collection,ClassicGroupState,Node,Set)"
    )]
    pub fn with_authorized_operations(
        group_id: impl Into<String>,
        protocol: impl Into<String>,
        protocol_data: impl Into<String>,
        members: Vec<MemberDescription>,
        state: ClassicGroupState,
        coordinator: Option<Node>,
        authorized_operations: Option<BTreeSet<AclOperation>>,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            protocol: protocol.into(),
            protocol_data: protocol_data.into(),
            members,
            state,
            coordinator,
            authorized_operations,
        }
    }

    /// The id of the classic group. Mirrors `groupId()`.
    #[doc(alias = "org.apache.kafka.clients.admin.ClassicGroupDescription#groupId")]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// The group protocol type. Mirrors `protocol()`.
    #[doc(alias = "org.apache.kafka.clients.admin.ClassicGroupDescription#protocol")]
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// The group protocol data. Mirrors `protocolData()`.
    #[doc(alias = "org.apache.kafka.clients.admin.ClassicGroupDescription#protocolData")]
    pub fn protocol_data(&self) -> &str {
        &self.protocol_data
    }

    /// Whether the group is a simple consumer group (empty protocol). Mirrors
    /// `isSimpleConsumerGroup()`.
    #[doc(alias = "org.apache.kafka.clients.admin.ClassicGroupDescription#isSimpleConsumerGroup")]
    pub fn is_simple_consumer_group(&self) -> bool {
        self.protocol.is_empty()
    }

    /// The members of the classic group. Mirrors `members()`.
    #[doc(alias = "org.apache.kafka.clients.admin.ClassicGroupDescription#members")]
    pub fn members(&self) -> &[MemberDescription] {
        &self.members
    }

    /// The classic group state. Mirrors `state()`.
    #[doc(alias = "org.apache.kafka.clients.admin.ClassicGroupDescription#state")]
    pub fn state(&self) -> ClassicGroupState {
        self.state
    }

    /// The classic group coordinator, or `None` if not known. Mirrors
    /// `coordinator()`.
    #[doc(alias = "org.apache.kafka.clients.admin.ClassicGroupDescription#coordinator")]
    pub fn coordinator(&self) -> Option<&Node> {
        self.coordinator.as_ref()
    }

    /// The authorized operations for this group. Mirrors `authorizedOperations()`.
    #[doc(alias = "org.apache.kafka.clients.admin.ClassicGroupDescription#authorizedOperations")]
    pub fn authorized_operations(&self) -> Option<&BTreeSet<AclOperation>> {
        self.authorized_operations.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_simple_consumer_group_when_protocol_empty() {
        let d = ClassicGroupDescription::new("g", "", "", Vec::new(), ClassicGroupState::Empty, None);
        assert!(d.is_simple_consumer_group());

        let d = ClassicGroupDescription::new("g", "consumer", "range", Vec::new(), ClassicGroupState::Stable, None);
        assert!(!d.is_simple_consumer_group());
    }

    /// New test, no Java original (Java has no `ClassicGroupDescriptionTest`):
    /// the six-argument constructor is the seven-argument one with Java's
    /// `Set.of()`, an empty set of authorized operations, not `null`
    /// (`ClassicGroupDescription.java:44-51`).
    #[test]
    fn new_passes_an_empty_set_of_authorized_operations() {
        let coordinator = Node::new(1, "localhost".to_string(), 9092);
        let short = ClassicGroupDescription::new(
            "g",
            "consumer",
            "range",
            Vec::new(),
            ClassicGroupState::Stable,
            Some(coordinator.clone()),
        );
        assert_eq!(short.authorized_operations(), Some(&BTreeSet::new()));
        assert_eq!(
            short,
            ClassicGroupDescription::with_authorized_operations(
                "g",
                "consumer",
                "range",
                Vec::new(),
                ClassicGroupState::Stable,
                Some(coordinator.clone()),
                Some(BTreeSet::new()),
            )
        );
        assert_ne!(
            short,
            ClassicGroupDescription::with_authorized_operations(
                "g",
                "consumer",
                "range",
                Vec::new(),
                ClassicGroupState::Stable,
                Some(coordinator),
                None,
            )
        );
    }
}
