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

//! A detailed description of a single share group member in the cluster.

use crate::admin::ShareMemberAssignment;

/// A detailed description of a single share group member in the cluster.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.admin.ShareMemberDescription")]
pub struct ShareMemberDescription {
    member_id: String,
    rack_id: Option<String>,
    client_id: String,
    host: String,
    assignment: ShareMemberAssignment,
    member_epoch: i32,
}

impl ShareMemberDescription {
    /// Creates a share member description.
    ///
    /// Java maps a `null` member id, client id or host to `""` and a `null`
    /// assignment to an empty one; Rust has no `null` for these, so the caller
    /// passes the empty value.
    ///
    /// # Arguments
    ///
    /// - `member_id` - the consumer id of the group member.
    /// - `rack_id` - the rack id of the group member, or `None` if not set.
    /// - `client_id` - the client id of the group member.
    /// - `host` - the host where the group member is running.
    /// - `assignment` - the assignment of the group member.
    /// - `member_epoch` - the epoch of the group member.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareMemberDescription#ShareMemberDescription")]
    pub fn new(
        member_id: impl Into<String>,
        rack_id: Option<String>,
        client_id: impl Into<String>,
        host: impl Into<String>,
        assignment: ShareMemberAssignment,
        member_epoch: i32,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            rack_id,
            client_id: client_id.into(),
            host: host.into(),
            assignment,
            member_epoch,
        }
    }

    /// Returns the consumer id of the group member.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareMemberDescription#consumerId")]
    pub fn consumer_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the rack id of the group member.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareMemberDescription#rackId")]
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Returns the client id of the group member.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareMemberDescription#clientId")]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the host where the group member is running.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareMemberDescription#host")]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the assignment of the group member.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareMemberDescription#assignment")]
    pub fn assignment(&self) -> &ShareMemberAssignment {
        &self.assignment
    }

    /// Returns the epoch of the group member.
    #[doc(alias = "org.apache.kafka.clients.admin.ShareMemberDescription#memberEpoch")]
    pub fn member_epoch(&self) -> i32 {
        self.member_epoch
    }
}

impl std::fmt::Display for ShareMemberDescription {
    /// Java's `toString()`; an absent rack id prints as `null`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(memberId={}, rackId={}, clientId={}, host={}, assignment={}, memberEpoch={})",
            self.member_id,
            self.rack_id.as_deref().unwrap_or("null"),
            self.client_id,
            self.host,
            self.assignment,
            self.member_epoch,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::common::TopicPartition;

    fn assignment() -> ShareMemberAssignment {
        ShareMemberAssignment::new(HashSet::from([TopicPartition::new("topic", 1)]))
    }

    fn member() -> ShareMemberDescription {
        ShareMemberDescription::new("member", Some("rack".to_string()), "client", "host", assignment(), 3)
    }

    /// New test, no Java original (Java has no `ShareMemberDescriptionTest`):
    /// `equals` compares every field.
    #[test]
    fn equality_compares_every_field() {
        assert_eq!(member(), member());
        let differing = [
            ShareMemberDescription::new("other", Some("rack".to_string()), "client", "host", assignment(), 3),
            ShareMemberDescription::new("member", None, "client", "host", assignment(), 3),
            ShareMemberDescription::new("member", Some("rack".to_string()), "other", "host", assignment(), 3),
            ShareMemberDescription::new("member", Some("rack".to_string()), "client", "other", assignment(), 3),
            ShareMemberDescription::new(
                "member",
                Some("rack".to_string()),
                "client",
                "host",
                ShareMemberAssignment::new(HashSet::new()),
                3,
            ),
            ShareMemberDescription::new("member", Some("rack".to_string()), "client", "host", assignment(), 4),
        ];
        for other in differing {
            assert_ne!(member(), other);
        }
    }

    /// New test, no Java original: the getters return what was passed in.
    #[test]
    fn getters_return_the_constructor_arguments() {
        let member = member();
        assert_eq!(member.consumer_id(), "member");
        assert_eq!(member.rack_id(), Some("rack"));
        assert_eq!(member.client_id(), "client");
        assert_eq!(member.host(), "host");
        assert_eq!(member.assignment(), &assignment());
        assert_eq!(member.member_epoch(), 3);
    }

    /// New test, no Java original: `toString()` follows Java's format.
    #[test]
    fn to_string_follows_java() {
        assert_eq!(
            member().to_string(),
            "(memberId=member, rackId=rack, clientId=client, host=host, assignment=(topicPartitions=topic-1), \
             memberEpoch=3)"
        );
        let no_rack = ShareMemberDescription::new("m", None, "c", "h", ShareMemberAssignment::new(HashSet::new()), 0);
        assert_eq!(
            no_rack.to_string(),
            "(memberId=m, rackId=null, clientId=c, host=h, assignment=(topicPartitions=), memberEpoch=0)"
        );
    }
}
