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

use crate::consumer::internals::ConsumerProtocol;
use std::collections::HashSet;

use crate::common::{GroupState, GroupType};

/// Options for `Admin::list_groups`.
#[derive(Clone, Debug, Default)]
#[doc(alias = "org.apache.kafka.clients.admin.ListGroupsOptions")]
pub struct ListGroupsOptions {
    timeout_ms: Option<i32>,
    group_states: HashSet<GroupState>,
    types: HashSet<GroupType>,
    protocol_types: HashSet<String>,
}

impl ListGroupsOptions {
    /// Creates default options (no filters).
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsOptions#ListGroupsOptions")]
    pub fn new() -> Self {
        Self::default()
    }

    /// Only consumer groups will be returned by `list_groups()`.
    /// This operation sets filters on group type and protocol type which select consumer groups.
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsOptions#forConsumerGroups")]
    pub fn for_consumer_groups() -> Self {
        Self::new()
            .with_types(HashSet::from([GroupType::Classic, GroupType::Consumer]))
            .with_protocol_types(HashSet::from([String::new(), ConsumerProtocol::PROTOCOL_TYPE.to_string()]))
    }

    /// Only share groups will be returned by `list_groups()`.
    /// This operation sets a filter on group type which select share groups.
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsOptions#forShareGroups")]
    pub fn for_share_groups() -> Self {
        Self::new().with_types(HashSet::from([GroupType::Share]))
    }

    /// Only streams groups will be returned by `list_groups()`.
    /// This operation sets a filter on group type which select streams groups.
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsOptions#forStreamsGroups")]
    pub fn for_streams_groups() -> Self {
        Self::new().with_types(HashSet::from([GroupType::Streams]))
    }

    /// If `group_states` is set, only groups in these states will be returned by `list_groups()`.
    /// Otherwise, all groups are returned.
    /// This operation is supported by brokers with version 2.6.0 or later.
    ///
    /// # Arguments
    ///
    /// - `group_states` - the group states to filter by.
    ///
    /// # Returns
    ///
    /// This `ListGroupsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsOptions#inGroupStates")]
    pub fn in_group_states(mut self, group_states: HashSet<GroupState>) -> Self {
        self.group_states = group_states;
        self
    }

    /// If protocol types is set, only groups of these protocol types will be returned by
    /// `list_groups()`. Otherwise, all groups are returned.
    ///
    /// # Arguments
    ///
    /// - `protocol_types` - the protocol types to filter by.
    ///
    /// # Returns
    ///
    /// This `ListGroupsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsOptions#withProtocolTypes")]
    pub fn with_protocol_types(mut self, protocol_types: HashSet<String>) -> Self {
        self.protocol_types = protocol_types;
        self
    }

    /// If types is set, only groups of these types will be returned by `list_groups()`.
    /// Otherwise, all groups are returned.
    ///
    /// # Arguments
    ///
    /// - `types` - the group types to filter by.
    ///
    /// # Returns
    ///
    /// This `ListGroupsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsOptions#withTypes")]
    pub fn with_types(mut self, types: HashSet<GroupType>) -> Self {
        self.types = types;
        self
    }

    /// Set the timeout in milliseconds for this operation, or `None` if the default
    /// API timeout for the `AdminClient` should be used.
    ///
    /// # Arguments
    ///
    /// - `timeout_ms` - the timeout in milliseconds, or `None` for the default API timeout.
    ///
    /// # Returns
    ///
    /// This `ListGroupsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Returns the list of group states that are requested or empty if no states have been
    /// specified.
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsOptions#groupStates")]
    pub fn group_states(&self) -> &HashSet<GroupState> {
        &self.group_states
    }

    /// Returns the list of protocol types that are requested or empty if no protocol types have
    /// been specified.
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsOptions#protocolTypes")]
    pub fn protocol_types(&self) -> &HashSet<String> {
        &self.protocol_types
    }

    /// Returns the list of group types that are requested or empty if no types have been
    /// specified.
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsOptions#types")]
    pub fn types(&self) -> &HashSet<GroupType> {
        &self.types
    }

    /// Returns the timeout in milliseconds for this operation, or `None` if the
    /// default API timeout for the `AdminClient` should be used.
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    pub fn timeout_ms(&self) -> Option<i32> {
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
            HashSet::from([String::new(), ConsumerProtocol::PROTOCOL_TYPE.to_string()])
        );

        let options = options
            .in_group_states(HashSet::from([GroupState::Stable]))
            .with_types(HashSet::from([GroupType::Consumer]))
            .with_protocol_types(HashSet::from([ConsumerProtocol::PROTOCOL_TYPE.to_string()]));
        assert_eq!(*options.group_states(), HashSet::from([GroupState::Stable]));
        assert_eq!(*options.types(), HashSet::from([GroupType::Consumer]));
        assert_eq!(
            *options.protocol_types(),
            HashSet::from([ConsumerProtocol::PROTOCOL_TYPE.to_string()])
        );
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
