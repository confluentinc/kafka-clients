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

//! A detailed description of a single group member.

use crate::admin::MemberAssignment;

/// A detailed description of a single group member in the cluster.
// Note: Java's four `@Deprecated(forRemoval = true)` convenience constructors
// (which only supply default values already expressible through [`new`](Self::new)) are
// omitted; the full-field [`new`](Self::new) constructor is the sole entry point and no
// in-scope caller or test uses the deprecated overloads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.admin.MemberDescription")]
pub struct MemberDescription {
    member_id: String,
    group_instance_id: Option<String>,
    rack_id: Option<String>,
    client_id: String,
    host: String,
    assignment: MemberAssignment,
    target_assignment: Option<MemberAssignment>,
    member_epoch: Option<i32>,
    upgraded: Option<bool>,
}

impl MemberDescription {
    /// Creates a member description with all fields.
    ///
    /// # Arguments
    ///
    /// - `member_id` - the consumer id of the group member.
    /// - `group_instance_id` - the instance id of the group member, or `None` if not set.
    /// - `rack_id` - the rack id of the group member, or `None` if not set.
    /// - `client_id` - the client id of the group member.
    /// - `host` - the host where the group member is running.
    /// - `assignment` - the assignment of the group member.
    /// - `target_assignment` - the target assignment of the member (consumer groups only).
    /// - `member_epoch` - the epoch of the member (consumer groups only).
    /// - `upgraded` - whether the member uses the consumer protocol (consumer groups only).
    #[expect(clippy::too_many_arguments)]
    #[doc(
        alias = "org.apache.kafka.clients.admin.MemberDescription#MemberDescription(String,Optional,Optional,String,String,MemberAssignment,Optional,Optional,Optional)"
    )]
    pub fn new(
        member_id: impl Into<String>,
        group_instance_id: Option<String>,
        rack_id: Option<String>,
        client_id: impl Into<String>,
        host: impl Into<String>,
        assignment: MemberAssignment,
        target_assignment: Option<MemberAssignment>,
        member_epoch: Option<i32>,
        upgraded: Option<bool>,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            group_instance_id,
            rack_id,
            client_id: client_id.into(),
            host: host.into(),
            assignment,
            target_assignment,
            member_epoch,
            upgraded,
        }
    }

    /// Returns the consumer id of the group member.
    #[doc(alias = "org.apache.kafka.clients.admin.MemberDescription#consumerId")]
    pub fn consumer_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the instance id of the group member.
    #[doc(alias = "org.apache.kafka.clients.admin.MemberDescription#groupInstanceId")]
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Returns the rack id of the group member.
    ///
    /// It is only available for consumer groups using the new consumer group protocol
    /// (`group.protocol=consumer`).
    #[doc(alias = "org.apache.kafka.clients.admin.MemberDescription#rackId")]
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Returns the client id of the group member.
    #[doc(alias = "org.apache.kafka.clients.admin.MemberDescription#clientId")]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the host where the group member is running.
    #[doc(alias = "org.apache.kafka.clients.admin.MemberDescription#host")]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the assignment of the group member. Provided for both classic group and consumer
    /// group.
    #[doc(alias = "org.apache.kafka.clients.admin.MemberDescription#assignment")]
    pub fn assignment(&self) -> &MemberAssignment {
        &self.assignment
    }

    /// Returns the target assignment of the member (consumer groups only).
    #[doc(alias = "org.apache.kafka.clients.admin.MemberDescription#targetAssignment")]
    pub fn target_assignment(&self) -> Option<&MemberAssignment> {
        self.target_assignment.as_ref()
    }

    /// Returns the epoch of the group member. The optional is set to an integer if the member is
    /// in a [`GroupType::Consumer`](crate::common::GroupType::Consumer) group, and to `None` if
    /// it is in a [`GroupType::Classic`](crate::common::GroupType::Classic) group.
    #[doc(alias = "org.apache.kafka.clients.admin.MemberDescription#memberEpoch")]
    pub fn member_epoch(&self) -> Option<i32> {
        self.member_epoch
    }

    /// Returns the flag indicating whether a member within a
    /// [`GroupType::Consumer`](crate::common::GroupType::Consumer) group uses the
    /// [`GroupType::Consumer`](crate::common::GroupType::Consumer) protocol. The optional is set
    /// to true if it does, to false if it does not, and to `None` if it is unknown or if the
    /// group is a [`GroupType::Classic`](crate::common::GroupType::Classic) group.
    #[doc(alias = "org.apache.kafka.clients.admin.MemberDescription#upgraded")]
    pub fn upgraded(&self) -> Option<bool> {
        self.upgraded
    }
}

impl std::fmt::Display for MemberDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(memberId={}, groupInstanceId={}, rackId={}, clientId={}, host={}, assignment={}, \
             targetAssignment={}, memberEpoch={}, upgraded={})",
            self.member_id,
            self.group_instance_id.as_deref().unwrap_or("null"),
            self.rack_id.as_deref().unwrap_or("null"),
            self.client_id,
            self.host,
            self.assignment,
            self.target_assignment
                .as_ref()
                .map_or_else(|| "null".to_string(), |a| a.to_string()),
            self.member_epoch.map_or_else(|| "null".to_string(), |e| e.to_string()),
            self.upgraded.map_or_else(|| "null".to_string(), |u| u.to_string()),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::common::TopicPartition;

    fn assignment() -> MemberAssignment {
        MemberAssignment::new(HashSet::from([TopicPartition::new("topic", 1)]))
    }

    fn static_member() -> MemberDescription {
        MemberDescription::new(
            "member_id",
            Some("instanceId".to_string()),
            Some("rackId".to_string()),
            "client_id",
            "host",
            assignment(),
            None,
            None,
            None,
        )
    }

    /// Translated from `testEqualsWithoutGroupInstanceId`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.MemberDescriptionTest#testEqualsWithoutGroupInstanceId")]
    fn test_equals_without_group_instance_id() {
        let dynamic =
            MemberDescription::new("member_id", None, None, "client_id", "host", assignment(), None, None, None);
        let identity =
            MemberDescription::new("member_id", None, None, "client_id", "host", assignment(), None, None, None);
        assert_ne!(static_member(), dynamic);
        assert_eq!(dynamic, identity);
    }

    /// Translated from `testEqualsWithGroupInstanceId`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.MemberDescriptionTest#testEqualsWithGroupInstanceId")]
    fn test_equals_with_group_instance_id() {
        let identity = MemberDescription::new(
            "member_id",
            Some("instanceId".to_string()),
            Some("rackId".to_string()),
            "client_id",
            "host",
            assignment(),
            None,
            None,
            None,
        );
        assert_eq!(static_member(), identity);
    }

    /// Translated from `testNonEqual`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.MemberDescriptionTest#testNonEqual")]
    fn test_non_equal() {
        let new_member = MemberDescription::new(
            "new_member",
            Some("instanceId".to_string()),
            Some("rackId".to_string()),
            "client_id",
            "host",
            assignment(),
            None,
            None,
            None,
        );
        assert_ne!(static_member(), new_member);

        let new_instance = MemberDescription::new(
            "member_id",
            Some("new_instance".to_string()),
            Some("rackId".to_string()),
            "client_id",
            "host",
            assignment(),
            None,
            None,
            None,
        );
        assert_ne!(static_member(), new_instance);

        let new_target = MemberDescription::new(
            "member_id",
            Some("instanceId".to_string()),
            Some("rackId".to_string()),
            "client_id",
            "host",
            assignment(),
            Some(assignment()),
            None,
            None,
        );
        assert_ne!(static_member(), new_target);

        let new_epoch = MemberDescription::new(
            "member_id",
            Some("instanceId".to_string()),
            Some("rackId".to_string()),
            "client_id",
            "host",
            assignment(),
            None,
            Some(1),
            None,
        );
        assert_ne!(static_member(), new_epoch);

        let new_is_classic = MemberDescription::new(
            "member_id",
            Some("instanceId".to_string()),
            Some("rackId".to_string()),
            "client_id",
            "host",
            assignment(),
            None,
            None,
            Some(false),
        );
        assert_ne!(static_member(), new_is_classic);
    }
}
