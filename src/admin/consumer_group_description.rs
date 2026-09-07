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
//!
//! Corresponds to `org.apache.kafka.clients.admin.ConsumerGroupDescription`.

use std::collections::BTreeSet;

use crate::admin::MemberDescription;
#[allow(deprecated)]
use crate::common::ConsumerGroupState;
use crate::common::acl::AclOperation;
use crate::common::{GroupState, GroupType, Node};

/// A detailed description of a single consumer group in the cluster.
///
/// Corresponds to `org.apache.kafka.clients.admin.ConsumerGroupDescription`.
///
/// Note: Java's `@Deprecated(forRemoval = true)` constructors that accept a
/// [`ConsumerGroupState`] are omitted (they only default-fill fields already
/// expressible through [`new`]); no in-scope caller uses them. The deprecated
/// [`state`](Self::state) accessor is retained because it is part of the public
/// surface exercised by callers migrating off `ConsumerGroupState`.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// Mirrors the primary
    /// `ConsumerGroupDescription(String, boolean, Collection<MemberDescription>,
    /// String, GroupType, GroupState, Node, Set<AclOperation>, Optional<Integer>,
    /// Optional<Integer>)` constructor. `coordinator` is `None` when the
    /// coordinator is not known (Java's nullable `Node`).
    #[allow(clippy::too_many_arguments)]
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

    /// The id of the consumer group. Mirrors `groupId()`.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Whether the consumer group is simple. Mirrors `isSimpleConsumerGroup()`.
    pub fn is_simple_consumer_group(&self) -> bool {
        self.is_simple_consumer_group
    }

    /// The members of the consumer group. Mirrors `members()`.
    pub fn members(&self) -> &[MemberDescription] {
        &self.members
    }

    /// The consumer group partition assignor. Mirrors `partitionAssignor()`.
    pub fn partition_assignor(&self) -> &str {
        &self.partition_assignor
    }

    /// The group type. Mirrors `type()`.
    pub fn group_type(&self) -> GroupType {
        self.group_type
    }

    /// The consumer group state (deprecated). Mirrors `state()`, mapping the
    /// group state via `ConsumerGroupState.parse(groupState.toString())`.
    #[allow(deprecated)]
    pub fn state(&self) -> ConsumerGroupState {
        ConsumerGroupState::parse(self.group_state.name())
    }

    /// The group state. Mirrors `groupState()`.
    pub fn group_state(&self) -> GroupState {
        self.group_state
    }

    /// The consumer group coordinator, or `None` if not known. Mirrors
    /// `coordinator()`.
    pub fn coordinator(&self) -> Option<&Node> {
        self.coordinator.as_ref()
    }

    /// The authorized operations for this group, or `None` if the broker did not
    /// report them (Java returns `null`). Mirrors `authorizedOperations()`.
    pub fn authorized_operations(&self) -> Option<&BTreeSet<AclOperation>> {
        self.authorized_operations.as_ref()
    }

    /// The epoch of the consumer group. Mirrors `groupEpoch()`.
    pub fn group_epoch(&self) -> Option<i32> {
        self.group_epoch
    }

    /// The epoch of the target assignment. Mirrors `targetAssignmentEpoch()`.
    pub fn target_assignment_epoch(&self) -> Option<i32> {
        self.target_assignment_epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `ConsumerGroupDescriptionTest.testGroupState`.
    #[test]
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
