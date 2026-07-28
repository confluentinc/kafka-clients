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

//! Options for `Admin::list_groups`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListGroupsOptions`.

use std::collections::HashSet;

use crate::common::{GroupState, GroupType};
use crate::consumer::internals::consumer_protocol::PROTOCOL_TYPE;

/// Options for `Admin::list_groups`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListGroupsOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListGroupsOptions {
    timeout_ms: Option<i32>,
    group_states: HashSet<GroupState>,
    types: HashSet<GroupType>,
    protocol_types: HashSet<String>,
}

impl ListGroupsOptions {
    /// Creates default options (no filters).
    pub fn new() -> Self {
        Self::default()
    }

    /// Options selecting only consumer groups (classic + consumer types,
    /// empty/`consumer` protocol types).
    ///
    /// Mirrors `ListGroupsOptions.forConsumerGroups`.
    pub fn for_consumer_groups() -> Self {
        Self::new()
            .with_types(HashSet::from([GroupType::Classic, GroupType::Consumer]))
            .with_protocol_types(HashSet::from([String::new(), PROTOCOL_TYPE.to_string()]))
    }

    /// Options selecting only share groups.
    ///
    /// Mirrors `ListGroupsOptions.forShareGroups`.
    pub fn for_share_groups() -> Self {
        Self::new().with_types(HashSet::from([GroupType::Share]))
    }

    /// Options selecting only streams groups.
    ///
    /// Mirrors `ListGroupsOptions.forStreamsGroups`.
    pub fn for_streams_groups() -> Self {
        Self::new().with_types(HashSet::from([GroupType::Streams]))
    }

    /// Filter by group states. Mirrors `inGroupStates`.
    #[must_use]
    pub fn in_group_states(mut self, group_states: HashSet<GroupState>) -> Self {
        self.group_states = group_states;
        self
    }

    /// Filter by protocol types. Mirrors `withProtocolTypes`.
    #[must_use]
    pub fn with_protocol_types(mut self, protocol_types: HashSet<String>) -> Self {
        self.protocol_types = protocol_types;
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
    pub fn timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// The requested group states. Mirrors `groupStates()`.
    pub fn group_states(&self) -> &HashSet<GroupState> {
        &self.group_states
    }

    /// The requested protocol types. Mirrors `protocolTypes()`.
    pub fn protocol_types(&self) -> &HashSet<String> {
        &self.protocol_types
    }

    /// The requested group types. Mirrors `types()`.
    pub fn types(&self) -> &HashSet<GroupType> {
        &self.types
    }

    /// The operation timeout in milliseconds, or `None` for the default.
    pub fn timeout(&self) -> Option<i32> {
        self.timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `ListGroupsOptionsTest.testForConsumerGroups`.
    #[test]
    fn test_for_consumer_groups() {
        let options = ListGroupsOptions::for_consumer_groups();
        assert!(options.group_states().is_empty());
        assert_eq!(*options.types(), HashSet::from([GroupType::Consumer, GroupType::Classic]));
        assert_eq!(
            *options.protocol_types(),
            HashSet::from([String::new(), PROTOCOL_TYPE.to_string()])
        );

        let options = options
            .in_group_states(HashSet::from([GroupState::Stable]))
            .with_types(HashSet::from([GroupType::Consumer]))
            .with_protocol_types(HashSet::from([PROTOCOL_TYPE.to_string()]));
        assert_eq!(*options.group_states(), HashSet::from([GroupState::Stable]));
        assert_eq!(*options.types(), HashSet::from([GroupType::Consumer]));
        assert_eq!(*options.protocol_types(), HashSet::from([PROTOCOL_TYPE.to_string()]));
    }

    /// Translated from `ListGroupsOptionsTest.testForShareGroups`.
    #[test]
    fn test_for_share_groups() {
        let options = ListGroupsOptions::for_share_groups();
        assert!(options.group_states().is_empty());
        assert_eq!(*options.types(), HashSet::from([GroupType::Share]));
        assert!(options.protocol_types().is_empty());
    }

    /// Translated from `ListGroupsOptionsTest.testForStreamsGroups`.
    #[test]
    fn test_for_streams_groups() {
        let options = ListGroupsOptions::for_streams_groups();
        assert!(options.group_states().is_empty());
        assert_eq!(*options.types(), HashSet::from([GroupType::Streams]));
        assert!(options.protocol_types().is_empty());
    }

    /// Translated from `ListGroupsOptionsTest.testGroupStates`.
    #[test]
    fn test_group_states() {
        let options = ListGroupsOptions::new();
        assert!(options.group_states().is_empty());

        let options = ListGroupsOptions::new().in_group_states(HashSet::from([GroupState::Dead]));
        assert_eq!(*options.group_states(), HashSet::from([GroupState::Dead]));
    }

    /// Translated from `ListGroupsOptionsTest.testConsumerGroupStates`.
    #[test]
    fn test_consumer_group_states() {
        let group_states = GroupState::group_states_for_type(GroupType::Consumer);
        let options = ListGroupsOptions::new().in_group_states(group_states.clone());
        assert_eq!(*options.group_states(), group_states);
    }

    /// Translated from `ListGroupsOptionsTest.testProtocolTypes`.
    #[test]
    fn test_protocol_types() {
        let options = ListGroupsOptions::new();
        assert!(options.protocol_types().is_empty());

        let protocol_types = HashSet::from([String::new(), "consumer".to_string(), "share".to_string()]);
        let options = ListGroupsOptions::new().with_protocol_types(protocol_types.clone());
        assert_eq!(*options.protocol_types(), protocol_types);
    }

    /// Translated from `ListGroupsOptionsTest.testTypes`.
    #[test]
    fn test_types() {
        let options = ListGroupsOptions::new();
        assert!(options.types().is_empty());

        let group_types = HashSet::from([
            GroupType::Unknown,
            GroupType::Consumer,
            GroupType::Classic,
            GroupType::Share,
            GroupType::Streams,
        ]);
        let options = ListGroupsOptions::new().with_types(group_types.clone());
        assert_eq!(*options.types(), group_types);
    }
}
