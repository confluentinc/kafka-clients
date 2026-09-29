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

//! A listing of a consumer group in the cluster.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ConsumerGroupListing`
//! (deprecated since 4.1 in favor of `Admin::list_groups` / `GroupListing`).

#![allow(deprecated)]

use crate::common::{ConsumerGroupState, GroupState, GroupType};

/// A listing of a consumer group in the cluster.
///
/// Corresponds to `org.apache.kafka.clients.admin.ConsumerGroupListing`
/// (deprecated since 4.1).
#[deprecated(since = "4.1.0", note = "Use Admin::list_groups and GroupListing instead")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerGroupListing {
    group_id: String,
    is_simple_consumer_group: bool,
    group_state: Option<GroupState>,
    group_type: Option<GroupType>,
}

impl ConsumerGroupListing {
    /// Creates a consumer group listing with a group state and type.
    ///
    /// Mirrors `ConsumerGroupListing(String, Optional<GroupState>,
    /// Optional<GroupType>, boolean)` — the constructor used by
    /// `KafkaAdminClient.listConsumerGroups`.
    ///
    /// The plain `new` is not used by any constructor here: the intersection
    /// across Java's five overloads is `{groupId, isSimpleConsumerGroup}`, and
    /// the overload matching it (`ConsumerGroupListing.java:45`) is not
    /// translated (CLAUDE.md §2).
    pub fn with_group_state_group_type(
        group_id: impl Into<String>,
        group_state: Option<GroupState>,
        group_type: Option<GroupType>,
        is_simple_consumer_group: bool,
    ) -> Self {
        Self { group_id: group_id.into(), is_simple_consumer_group, group_state, group_type }
    }

    /// Creates a consumer group listing from a deprecated
    /// [`ConsumerGroupState`].
    ///
    /// Mirrors the deprecated
    /// `ConsumerGroupListing(String, boolean, Optional<ConsumerGroupState>)`
    /// constructor, which maps the state via `GroupState.parse(state.toString())`.
    pub fn with_state(
        group_id: impl Into<String>,
        is_simple_consumer_group: bool,
        state: Option<ConsumerGroupState>,
    ) -> Self {
        Self::with_group_state_group_type(
            group_id,
            state.map(|s| GroupState::parse(s.name())),
            None,
            is_simple_consumer_group,
        )
    }

    /// The consumer group id. Mirrors `groupId()`.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Whether the consumer group is simple. Mirrors `isSimpleConsumerGroup()`.
    pub fn is_simple_consumer_group(&self) -> bool {
        self.is_simple_consumer_group
    }

    /// The group state. Mirrors `groupState()`.
    pub fn group_state(&self) -> Option<GroupState> {
        self.group_state
    }

    /// The consumer group state (deprecated). Mirrors `state()`, mapping the
    /// group state via `ConsumerGroupState.parse(groupState.toString())`.
    pub fn state(&self) -> Option<ConsumerGroupState> {
        self.group_state.map(|s| ConsumerGroupState::parse(s.name()))
    }

    /// The type of the consumer group. Mirrors `type()`.
    pub fn group_type(&self) -> Option<GroupType> {
        self.group_type
    }
}

impl std::fmt::Display for ConsumerGroupListing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(groupId='{}', isSimpleConsumerGroup={}, groupState={:?}, type={:?})",
            self.group_id, self.is_simple_consumer_group, self.group_state, self.group_type,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `ConsumerGroupListingTest.testState`.
    #[test]
    fn test_state() {
        for state in [
            ConsumerGroupState::Unknown,
            ConsumerGroupState::PreparingRebalance,
            ConsumerGroupState::CompletingRebalance,
            ConsumerGroupState::Stable,
            ConsumerGroupState::Dead,
            ConsumerGroupState::Empty,
            ConsumerGroupState::Assigning,
            ConsumerGroupState::Reconciling,
        ] {
            let listing = ConsumerGroupListing::with_state("groupId", false, Some(state));
            assert_eq!(listing.state().unwrap(), state);
        }
    }
}
