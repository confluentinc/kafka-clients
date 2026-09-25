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

//! Options for `Admin::list_consumer_groups`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListConsumerGroupsOptions`
//! (deprecated since 4.1 in favor of `Admin::list_groups`).

#![allow(deprecated)]

use std::collections::HashSet;

use crate::common::{ConsumerGroupState, GroupState, GroupType};

/// Options for `Admin::list_consumer_groups`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListConsumerGroupsOptions`
/// (deprecated since 4.1).
#[deprecated(since = "4.1.0", note = "Use Admin::list_groups instead")]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListConsumerGroupsOptions {
    timeout_ms: Option<i32>,
    group_states: HashSet<GroupState>,
    types: HashSet<GroupType>,
}

impl ListConsumerGroupsOptions {
    /// Creates default options (no filters).
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by group states. Mirrors `inGroupStates`.
    #[must_use]
    pub fn in_group_states(mut self, group_states: HashSet<GroupState>) -> Self {
        self.group_states = group_states;
        self
    }

    /// Filter by deprecated [`ConsumerGroupState`]s. Mirrors the deprecated
    /// `inStates`, mapping each via `GroupState.parse(state.toString())`.
    #[must_use]
    pub fn in_states(mut self, states: HashSet<ConsumerGroupState>) -> Self {
        self.group_states = states.into_iter().map(|s| GroupState::parse(s.name())).collect();
        self
    }

    /// Filter by group types. Mirrors `withTypes`.
    #[must_use]
    pub fn with_types(mut self, types: HashSet<GroupType>) -> Self {
        self.types = types;
        self
    }

    /// Set the operation timeout in milliseconds (or `None` for the default).
    #[must_use]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// The requested group states. Mirrors `groupStates()`.
    pub fn group_states(&self) -> &HashSet<GroupState> {
        &self.group_states
    }

    /// The requested states as deprecated [`ConsumerGroupState`]s. Mirrors the
    /// deprecated `states()`.
    pub fn states(&self) -> HashSet<ConsumerGroupState> {
        self.group_states.iter().map(|s| ConsumerGroupState::parse(s.name())).collect()
    }

    /// The requested group types. Mirrors `types()`.
    pub fn types(&self) -> &HashSet<GroupType> {
        &self.types
    }

    /// The operation timeout in milliseconds, or `None` for the default.
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `ListConsumerGroupsOptionsTest.testState`.
    #[test]
    fn test_state() {
        let states = HashSet::from([
            ConsumerGroupState::Unknown,
            ConsumerGroupState::PreparingRebalance,
            ConsumerGroupState::CompletingRebalance,
            ConsumerGroupState::Stable,
            ConsumerGroupState::Dead,
            ConsumerGroupState::Empty,
            ConsumerGroupState::Assigning,
            ConsumerGroupState::Reconciling,
        ]);
        let options = ListConsumerGroupsOptions::new().in_states(states.clone());
        assert_eq!(options.states(), states);
    }
}
