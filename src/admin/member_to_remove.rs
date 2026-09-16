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

//! A member to be removed from a consumer group.
//!
//! Corresponds to `org.apache.kafka.clients.admin.MemberToRemove`.

use crate::common::requests::JoinGroupRequest;
use crate::leave_group_request_data::MemberIdentity;

/// A struct containing information about the member to be removed.
///
/// Corresponds to `org.apache.kafka.clients.admin.MemberToRemove`. Members are
/// identified by their `group.instance.id` (static membership); the member id
/// is left as [`JoinGroupRequest::UNKNOWN_MEMBER_ID`] so the broker resolves it by instance id.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MemberToRemove {
    group_instance_id: String,
}

impl MemberToRemove {
    /// Creates a `MemberToRemove` for the given `group.instance.id`.
    pub fn new(group_instance_id: impl Into<String>) -> Self {
        Self { group_instance_id: group_instance_id.into() }
    }

    /// Converts this member to a wire [`MemberIdentity`] (with an unknown member
    /// id). Mirrors Java's `MemberToRemove.toMemberIdentity`.
    pub(crate) fn to_member_identity(&self) -> MemberIdentity {
        let mut identity = MemberIdentity::new();
        identity
            .set_group_instance_id(Some(self.group_instance_id.clone()))
            .set_member_id(JoinGroupRequest::UNKNOWN_MEMBER_ID.to_string());
        identity
    }

    /// The `group.instance.id` of the member to remove.
    pub fn group_instance_id(&self) -> &str {
        &self.group_instance_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equality_is_by_group_instance_id() {
        assert_eq!(MemberToRemove::new("instance-1"), MemberToRemove::new("instance-1"));
        assert_ne!(MemberToRemove::new("instance-1"), MemberToRemove::new("instance-2"));
    }

    #[test]
    fn to_member_identity_sets_instance_and_unknown_member_id() {
        let identity = MemberToRemove::new("instance-1").to_member_identity();
        assert_eq!(identity.group_instance_id.as_deref(), Some("instance-1"));
        assert_eq!(identity.member_id, JoinGroupRequest::UNKNOWN_MEMBER_ID);
    }
}
