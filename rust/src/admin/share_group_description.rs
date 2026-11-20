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

//! A detailed description of a single share group in the cluster.

use std::collections::BTreeSet;

use crate::admin::ShareMemberDescription;
use crate::common::acl::AclOperation;
use crate::common::{GroupState, Node};

/// A detailed description of a single share group in the cluster.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.admin.ShareGroupDescription")]
pub struct ShareGroupDescription {
    group_id: String,
    members: Vec<ShareMemberDescription>,
    group_state: GroupState,
    coordinator: Option<Node>,
    group_epoch: i32,
    target_assignment_epoch: i32,
    /// `Option` because Java's field is nullable: the admin client fills it
    /// from `AdminUtils.validAclOperations`, which returns `null` when the broker
    /// did not report the operations.
    authorized_operations: Option<BTreeSet<AclOperation>>,
}

impl ShareGroupDescription {
    /// Creates a share group description with no authorized operations.
    ///
    /// Java's six-argument constructor passes `Collections.emptySet()`, an empty
    /// set rather than `None`, as the authorized operations.
    ///
    /// # Arguments
    ///
    /// - `group_id` - the id of the share group.
    /// - `members` - the members of the group.
    /// - `group_state` - the group state.
    /// - `coordinator` - the group coordinator, or `None` if not known.
    /// - `group_epoch` - the epoch of the group.
    /// - `target_assignment_epoch` - the epoch of the target assignment.
    #[doc(
        alias = "org.apache.kafka.clients.admin.ShareGroupDescription#ShareGroupDescription(String,Collection,GroupState,Node,int,int)"
    )]
    pub fn new(
        group_id: impl Into<String>,
        members: Vec<ShareMemberDescription>,
        group_state: GroupState,
        coordinator: Option<Node>,
        group_epoch: i32,
        target_assignment_epoch: i32,
    ) -> Self {
        Self::with_authorized_operations(
            group_id,
            members,
            group_state,
            coordinator,
            group_epoch,
            target_assignment_epoch,
            Some(BTreeSet::new()),
        )
    }

    /// Creates a share group description with all fields.
    ///
    /// Java maps a `null` group id to `""` and a `null` member collection to an
    /// empty list; Rust has no `null` for these, so the caller passes the empty
    /// value.
    ///
    /// # Arguments
    ///
    /// - `group_id` - the id of the share group.
    /// - `members` - the members of the group.
    /// - `group_state` - the group state.
    /// - `coordinator` - the group coordinator, or `None` if not known.
    /// - `group_epoch` - the epoch of the group.
    /// - `target_assignment_epoch` - the epoch of the target assignment.
    /// - `authorized_operations` - the authorized operations for this group, or `None` if not
    ///   known.
    #[doc(
        alias = "org.apache.kafka.clients.admin.ShareGroupDescription#ShareGroupDescription(String,Collection,GroupState,Node,int,int,Set)"
    )]
    pub fn with_authorized_operations(
        group_id: impl Into<String>,
        members: Vec<ShareMemberDescription>,
        group_state: GroupState,
        coordinator: Option<Node>,
        group_epoch: i32,
        target_assignment_epoch: i32,
        authorized_operations: Option<BTreeSet<AclOperation>>,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            members,
            group_state,
            coordinator,
            group_epoch,
            target_assignment_epoch,
            authorized_operations,
        }
    }

    /// Returns the id of the share group.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareGroupDescription#groupId")]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns a list of the members of the share group.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareGroupDescription#members")]
    pub fn members(&self) -> &[ShareMemberDescription] {
        &self.members
    }

    /// Returns the group state, or [`GroupState::Unknown`] if the state is too new for us to parse.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareGroupDescription#groupState")]
    pub fn group_state(&self) -> GroupState {
        self.group_state
    }

    /// Returns the share group coordinator, or `None` if the coordinator is not known.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareGroupDescription#coordinator")]
    pub fn coordinator(&self) -> Option<&Node> {
        self.coordinator.as_ref()
    }

    /// Returns the authorized operations for this group, or `None` if that information is not
    /// known.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareGroupDescription#authorizedOperations")]
    pub fn authorized_operations(&self) -> Option<&BTreeSet<AclOperation>> {
        self.authorized_operations.as_ref()
    }

    /// Returns the epoch of the share group.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareGroupDescription#groupEpoch")]
    pub fn group_epoch(&self) -> i32 {
        self.group_epoch
    }

    /// Returns the epoch of the target assignment.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareGroupDescription#targetAssignmentEpoch")]
    pub fn target_assignment_epoch(&self) -> i32 {
        self.target_assignment_epoch
    }
}

impl std::fmt::Display for ShareGroupDescription {
    /// Java's `toString()`: an absent coordinator or operation set prints as
    /// `null`, and the operations as Java's `[A, B]`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let members = self.members.iter().map(ToString::to_string).collect::<Vec<_>>().join(",");
        let coordinator = self
            .coordinator
            .as_ref()
            .map_or_else(|| "null".to_string(), ToString::to_string);
        let operations = self.authorized_operations.as_ref().map_or_else(
            || "null".to_string(),
            |operations| {
                format!(
                    "[{}]",
                    operations.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
                )
            },
        );
        write!(
            f,
            "(groupId={}, members={}, groupState={}, coordinator={}, groupEpoch={}, targetAssignmentEpoch={}, \
             authorizedOperations={})",
            self.group_id,
            members,
            self.group_state,
            coordinator,
            self.group_epoch,
            self.target_assignment_epoch,
            operations,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::admin::ShareMemberAssignment;
    use crate::common::TopicPartition;

    fn member(id: &str) -> ShareMemberDescription {
        ShareMemberDescription::new(
            id,
            None,
            "client",
            "host",
            ShareMemberAssignment::new(HashSet::from([TopicPartition::new("t", 0)])),
            1,
        )
    }

    fn coordinator() -> Node {
        Node::new(1, "localhost".to_string(), 9092)
    }

    /// New test, no Java original (Java has no `ShareGroupDescriptionTest`):
    /// the six-argument constructor is the seven-argument one with Java's
    /// `Collections.emptySet()`, an empty set of authorized operations, not
    /// `null`.
    #[test]
    fn new_passes_an_empty_set_of_authorized_operations() {
        let short = ShareGroupDescription::new("g", vec![member("m")], GroupState::Stable, Some(coordinator()), 2, 3);
        assert_eq!(short.authorized_operations(), Some(&BTreeSet::new()));
        assert_eq!(
            short,
            ShareGroupDescription::with_authorized_operations(
                "g",
                vec![member("m")],
                GroupState::Stable,
                Some(coordinator()),
                2,
                3,
                Some(BTreeSet::new()),
            )
        );
        assert_ne!(
            short,
            ShareGroupDescription::with_authorized_operations(
                "g",
                vec![member("m")],
                GroupState::Stable,
                Some(coordinator()),
                2,
                3,
                None,
            )
        );
    }

    /// New test, no Java original: `equals` compares every field, and the
    /// members as a list (order matters, as Java's `List.equals`).
    #[test]
    fn equality_compares_every_field() {
        let base = || {
            ShareGroupDescription::with_authorized_operations(
                "g",
                vec![member("a"), member("b")],
                GroupState::Stable,
                Some(coordinator()),
                2,
                3,
                Some(BTreeSet::from([AclOperation::Read])),
            )
        };
        assert_eq!(base(), base());
        let differing = [
            ShareGroupDescription::with_authorized_operations(
                "other",
                vec![member("a"), member("b")],
                GroupState::Stable,
                Some(coordinator()),
                2,
                3,
                Some(BTreeSet::from([AclOperation::Read])),
            ),
            ShareGroupDescription::with_authorized_operations(
                "g",
                vec![member("b"), member("a")],
                GroupState::Stable,
                Some(coordinator()),
                2,
                3,
                Some(BTreeSet::from([AclOperation::Read])),
            ),
            ShareGroupDescription::with_authorized_operations(
                "g",
                vec![member("a"), member("b")],
                GroupState::Empty,
                Some(coordinator()),
                2,
                3,
                Some(BTreeSet::from([AclOperation::Read])),
            ),
            ShareGroupDescription::with_authorized_operations(
                "g",
                vec![member("a"), member("b")],
                GroupState::Stable,
                None,
                2,
                3,
                Some(BTreeSet::from([AclOperation::Read])),
            ),
            ShareGroupDescription::with_authorized_operations(
                "g",
                vec![member("a"), member("b")],
                GroupState::Stable,
                Some(coordinator()),
                9,
                3,
                Some(BTreeSet::from([AclOperation::Read])),
            ),
            ShareGroupDescription::with_authorized_operations(
                "g",
                vec![member("a"), member("b")],
                GroupState::Stable,
                Some(coordinator()),
                2,
                9,
                Some(BTreeSet::from([AclOperation::Read])),
            ),
            ShareGroupDescription::with_authorized_operations(
                "g",
                vec![member("a"), member("b")],
                GroupState::Stable,
                Some(coordinator()),
                2,
                3,
                Some(BTreeSet::from([AclOperation::Write])),
            ),
        ];
        for other in differing {
            assert_ne!(base(), other);
        }
    }

    /// New test, no Java original: the getters return what was passed in.
    #[test]
    fn getters_return_the_constructor_arguments() {
        let description = ShareGroupDescription::with_authorized_operations(
            "g",
            vec![member("a")],
            GroupState::Empty,
            Some(coordinator()),
            2,
            3,
            None,
        );
        assert_eq!(description.group_id(), "g");
        assert_eq!(description.members(), &[member("a")]);
        assert_eq!(description.group_state(), GroupState::Empty);
        assert_eq!(description.coordinator(), Some(&coordinator()));
        assert_eq!(description.group_epoch(), 2);
        assert_eq!(description.target_assignment_epoch(), 3);
        assert_eq!(description.authorized_operations(), None);
    }

    /// New test, no Java original: `toString()` follows Java's format.
    #[test]
    fn to_string_follows_java() {
        let description = ShareGroupDescription::with_authorized_operations(
            "g",
            vec![member("a"), member("b")],
            GroupState::Stable,
            None,
            2,
            3,
            None,
        );
        assert_eq!(
            description.to_string(),
            "(groupId=g, members=(memberId=a, rackId=null, clientId=client, host=host, \
             assignment=(topicPartitions=t-0), memberEpoch=1),(memberId=b, rackId=null, clientId=client, \
             host=host, assignment=(topicPartitions=t-0), memberEpoch=1), groupState=Stable, coordinator=null, \
             groupEpoch=2, targetAssignmentEpoch=3, authorizedOperations=null)"
        );
        let with_operations = ShareGroupDescription::with_authorized_operations(
            "g",
            Vec::new(),
            GroupState::Empty,
            None,
            0,
            0,
            Some(BTreeSet::from([AclOperation::Read, AclOperation::Describe])),
        );
        assert!(
            with_operations.to_string().ends_with("authorizedOperations=[READ, DESCRIBE])"),
            "{with_operations}"
        );
    }
}
