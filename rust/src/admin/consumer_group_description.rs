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

//! A detailed description of a single consumer group in the cluster.

use std::collections::BTreeSet;

use crate::admin::MemberDescription;
use crate::common::acl::AclOperation;
use crate::common::{GroupState, GroupType, Node};

/// A detailed description of a single consumer group in the cluster.
// Note: Java's `@Deprecated(forRemoval = true)` constructors that accept a
// `ConsumerGroupState`, and the deprecated `state()` accessor, are not
// translated; use [`new`](Self::new) and [`group_state`](Self::group_state).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.admin.ConsumerGroupDescription")]
pub struct ConsumerGroupDescription {
    group_id: String,
    is_simple_consumer_group: bool,
    members: Vec<MemberDescription>,
    partition_assignor: String,
    group_type: GroupType,
    group_state: GroupState,
    coordinator: Option<Node>,
    /// `Option` because Java's field is nullable: the admin client fills it
    /// from `AdminUtils.validAclOperations`, which returns `null` when the broker
    /// did not report the operations.
    authorized_operations: Option<BTreeSet<AclOperation>>,
    group_epoch: Option<i32>,
    target_assignment_epoch: Option<i32>,
}

impl ConsumerGroupDescription {
    /// Creates a consumer group description with all fields.
    ///
    /// # Arguments
    ///
    /// - `group_id` - the id of the consumer group.
    /// - `is_simple_consumer_group` - whether the group is a simple consumer group.
    /// - `members` - the members of the group.
    /// - `partition_assignor` - the name of the partition assignor.
    /// - `group_type` - the group protocol type.
    /// - `group_state` - the group state.
    /// - `coordinator` - the group coordinator, or `None` if not known.
    /// - `authorized_operations` - the authorized operations for this group, or `None` if not
    ///   known.
    /// - `group_epoch` - the epoch of the group, or `None` for a classic group.
    /// - `target_assignment_epoch` - the epoch of the target assignment, or `None` for a
    ///   classic group.
    #[expect(clippy::too_many_arguments)]
    #[doc(
        alias = "org.apache.kafka.clients.admin.ConsumerGroupDescription#ConsumerGroupDescription(String,boolean,Collection,String,GroupType,GroupState,Node,Set,Optional,Optional)"
    )]
    pub fn new(
        group_id: impl Into<String>,
        is_simple_consumer_group: bool,
        members: Vec<MemberDescription>,
        partition_assignor: impl Into<String>,
        group_type: GroupType,
        group_state: GroupState,
        coordinator: Option<Node>,
        authorized_operations: Option<BTreeSet<AclOperation>>,
        group_epoch: Option<i32>,
        target_assignment_epoch: Option<i32>,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            is_simple_consumer_group,
            members,
            partition_assignor: partition_assignor.into(),
            group_type,
            group_state,
            coordinator,
            authorized_operations,
            group_epoch,
            target_assignment_epoch,
        }
    }

    /// Returns the id of the consumer group.
    #[doc(alias = "org.apache.kafka.clients.admin.ConsumerGroupDescription#groupId")]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns whether the consumer group is simple.
    #[doc(alias = "org.apache.kafka.clients.admin.ConsumerGroupDescription#isSimpleConsumerGroup")]
    pub fn is_simple_consumer_group(&self) -> bool {
        self.is_simple_consumer_group
    }

    /// Returns the members of the consumer group.
    #[doc(alias = "org.apache.kafka.clients.admin.ConsumerGroupDescription#members")]
    pub fn members(&self) -> &[MemberDescription] {
        &self.members
    }

    /// Returns the consumer group partition assignor.
    #[doc(alias = "org.apache.kafka.clients.admin.ConsumerGroupDescription#partitionAssignor")]
    pub fn partition_assignor(&self) -> &str {
        &self.partition_assignor
    }

    /// Returns the group type.
    #[doc(alias = "org.apache.kafka.clients.admin.ConsumerGroupDescription#type")]
    pub fn r#type(&self) -> GroupType {
        self.group_type
    }

    /// Returns the group state, or [`GroupState::Unknown`] if the state is too new for us to
    /// parse.
    #[doc(alias = "org.apache.kafka.clients.admin.ConsumerGroupDescription#groupState")]
    pub fn group_state(&self) -> GroupState {
        self.group_state
    }

    /// Returns the consumer group coordinator, or `None` if not known.
    #[doc(alias = "org.apache.kafka.clients.admin.ConsumerGroupDescription#coordinator")]
    pub fn coordinator(&self) -> Option<&Node> {
        self.coordinator.as_ref()
    }

    /// Returns the authorized operations for this group, or `None` if that information is
    /// not known.
    #[doc(alias = "org.apache.kafka.clients.admin.ConsumerGroupDescription#authorizedOperations")]
    pub fn authorized_operations(&self) -> Option<&BTreeSet<AclOperation>> {
        self.authorized_operations.as_ref()
    }

    /// Returns the epoch of the consumer group. The optional is set to an integer if it is a
    /// [`GroupType::Consumer`] group, and to `None` if it is a [`GroupType::Classic`] group.
    #[doc(alias = "org.apache.kafka.clients.admin.ConsumerGroupDescription#groupEpoch")]
    pub fn group_epoch(&self) -> Option<i32> {
        self.group_epoch
    }

    /// Returns the epoch of the target assignment. The optional is set to an integer if it is a
    /// [`GroupType::Consumer`] group, and to `None` if it is a [`GroupType::Classic`] group.
    #[doc(alias = "org.apache.kafka.clients.admin.ConsumerGroupDescription#targetAssignmentEpoch")]
    pub fn target_assignment_epoch(&self) -> Option<i32> {
        self.target_assignment_epoch
    }
}

impl std::fmt::Display for ConsumerGroupDescription {
    /// Java's `toString()`: an absent coordinator, operation set or epoch
    /// prints as `null`, and the operations as Java's `[A, B]`.
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
        let null_or = |epoch: Option<i32>| epoch.map_or_else(|| "null".to_string(), |e| e.to_string());
        write!(
            f,
            "(groupId={}, isSimpleConsumerGroup={}, members={}, partitionAssignor={}, type={}, groupState={}, \
             coordinator={}, authorizedOperations={}, groupEpoch={}, targetAssignmentEpoch={})",
            self.group_id,
            self.is_simple_consumer_group,
            members,
            self.partition_assignor,
            self.group_type,
            self.group_state,
            coordinator,
            operations,
            null_or(self.group_epoch),
            null_or(self.target_assignment_epoch),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_string_follows_java() {
        let description = ConsumerGroupDescription::new(
            "g",
            false,
            Vec::new(),
            "range",
            GroupType::Consumer,
            GroupState::Stable,
            None,
            Some(BTreeSet::from([AclOperation::Write, AclOperation::Read])),
            Some(3),
            None,
        );
        assert_eq!(
            description.to_string(),
            "(groupId=g, isSimpleConsumerGroup=false, members=, partitionAssignor=range, type=Consumer, \
             groupState=Stable, coordinator=null, authorizedOperations=[READ, WRITE], groupEpoch=3, \
             targetAssignmentEpoch=null)"
        );
    }

    /// Translated from `ConsumerGroupDescriptionTest.testGroupState`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.ConsumerGroupDescriptionTest#testGroupState")]
    fn test_group_state() {
        for group_state in [
            GroupState::Unknown,
            GroupState::PreparingRebalance,
            GroupState::CompletingRebalance,
            GroupState::Stable,
            GroupState::Dead,
            GroupState::Empty,
            GroupState::Assigning,
            GroupState::Reconciling,
            GroupState::NotReady,
        ] {
            let description = ConsumerGroupDescription::new(
                "groupId",
                false,
                Vec::new(),
                "assignor",
                GroupType::Consumer,
                group_state,
                None,
                Some(BTreeSet::new()),
                None,
                None,
            );
            assert_eq!(description.group_state(), group_state);
        }
    }
}
