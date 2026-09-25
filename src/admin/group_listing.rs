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

//! A listing of a group in the cluster.
//!
//! Corresponds to `org.apache.kafka.clients.admin.GroupListing`.

use crate::common::{GroupState, GroupType};

/// A listing of a group in the cluster.
///
/// Corresponds to `org.apache.kafka.clients.admin.GroupListing`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GroupListing {
    group_id: String,
    group_type: Option<GroupType>,
    protocol: String,
    group_state: Option<GroupState>,
}

impl GroupListing {
    /// Creates a group listing.
    ///
    /// Mirrors `new GroupListing(String, Optional<GroupType>, String,
    /// Optional<GroupState>)`.
    pub fn new(
        group_id: impl Into<String>,
        group_type: Option<GroupType>,
        protocol: impl Into<String>,
        group_state: Option<GroupState>,
    ) -> Self {
        Self { group_id: group_id.into(), group_type, protocol: protocol.into(), group_state }
    }

    /// The group id. Mirrors `groupId()`.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// The type of the group, if available. Mirrors `type()`.
    pub fn group_type(&self) -> Option<GroupType> {
        self.group_type
    }

    /// The protocol of the group. Mirrors `protocol()`.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// The group state, if available. Mirrors `groupState()`.
    pub fn group_state(&self) -> Option<GroupState> {
        self.group_state
    }

    /// Whether the group is a simple consumer group. Mirrors
    /// `isSimpleConsumerGroup()`: a classic group with an empty protocol.
    pub fn is_simple_consumer_group(&self) -> bool {
        self.group_type == Some(GroupType::Classic) && self.protocol.is_empty()
    }
}

impl std::fmt::Display for GroupListing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(groupId='{}', type={}, protocol='{}', groupState={})",
            self.group_id,
            self.group_type.map_or_else(|| "none".to_string(), |t| t.to_string()),
            self.protocol,
            self.group_state.map_or_else(|| "none".to_string(), |s| s.to_string()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::internals::ConsumerProtocol;

    const GROUP_ID: &str = "mygroup";

    /// Translated from `GroupListingTest.testSimpleConsumerGroup`.
    #[test]
    fn test_simple_consumer_group() {
        let gl = GroupListing::new(GROUP_ID, Some(GroupType::Classic), "", Some(GroupState::Empty));
        assert!(gl.is_simple_consumer_group());

        let gl = GroupListing::new(
            GROUP_ID,
            Some(GroupType::Classic),
            ConsumerProtocol::PROTOCOL_TYPE,
            Some(GroupState::Stable),
        );
        assert!(!gl.is_simple_consumer_group());

        let gl = GroupListing::new(GROUP_ID, Some(GroupType::Consumer), "", Some(GroupState::Empty));
        assert!(!gl.is_simple_consumer_group());

        let gl = GroupListing::new(GROUP_ID, None, "", None);
        assert!(!gl.is_simple_consumer_group());
    }
}
