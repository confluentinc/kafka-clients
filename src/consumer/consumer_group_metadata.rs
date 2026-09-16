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

//! Metadata struct containing consumer group information.
//!
//! Translated from `org.apache.kafka.clients.consumer.ConsumerGroupMetadata`.

use std::fmt;

/// `JoinGroupRequest.UNKNOWN_GENERATION_ID` from Java's request module.
///
/// NOTE: a later phase will wire this to the generated `JoinGroupRequest`
/// constant once the consumer membership translation lands; for Phase 1 we
/// hardcode the value with a comment pointing to the Java source.
const UNKNOWN_GENERATION_ID: i32 = -1;

/// `JoinGroupRequest.UNKNOWN_MEMBER_ID` from Java's request module.
///
/// NOTE: a later phase will wire this to the generated `JoinGroupRequest`
/// constant once the consumer membership translation lands; for Phase 1 we
/// hardcode the value (empty string) with a comment pointing to the Java
/// source.
const UNKNOWN_MEMBER_ID: &str = "";

/// A metadata struct containing the consumer group information.
///
/// Corresponds to Java's
/// `org.apache.kafka.clients.consumer.ConsumerGroupMetadata`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConsumerGroupMetadata {
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
}

impl ConsumerGroupMetadata {
    /// Create new `ConsumerGroupMetadata` with the given group ID and default
    /// generation/member IDs (unknown).
    ///
    /// Corresponds to Java's `new ConsumerGroupMetadata(String)`. Java marks
    /// the constructor `@Deprecated(since = "4.2", forRemoval = true)`.
    #[deprecated(
        since = "4.2.0",
        note = "Use Consumer::group_metadata() instead. This struct will become a trait in a future release."
    )]
    pub fn new(group_id: impl Into<String>) -> Self {
        #[allow(deprecated)]
        Self::with_generation_id_member_id_group_instance_id(group_id, UNKNOWN_GENERATION_ID, UNKNOWN_MEMBER_ID, None)
    }

    /// Create new `ConsumerGroupMetadata` with full details.
    ///
    /// Corresponds to Java's 4-arg constructor. Java marks this constructor
    /// `@Deprecated(since = "4.2", forRemoval = true)`.
    #[deprecated(
        since = "4.2.0",
        note = "Use Consumer::group_metadata() instead. This struct will become a trait in a future release."
    )]
    pub fn with_generation_id_member_id_group_instance_id(
        group_id: impl Into<String>,
        generation_id: i32,
        member_id: impl Into<String>,
        group_instance_id: Option<String>,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            generation_id,
            member_id: member_id.into(),
            group_instance_id,
        }
    }

    /// The consumer group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// The current generation ID, or `-1` (`UNKNOWN_GENERATION_ID`) if unknown.
    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    /// The member ID, or an empty string (`UNKNOWN_MEMBER_ID`) if unknown.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// The group instance ID, if the consumer is a static member.
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }
}

impl fmt::Display for ConsumerGroupMetadata {
    /// Matches Java's
    /// `String.format("GroupMetadata(groupId = %s, generationId = %d, memberId = %s, groupInstanceId = %s)", …)`
    /// where `groupInstanceId` is replaced with an empty string if absent.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let group_instance_id_str = self.group_instance_id.as_deref().unwrap_or("");
        write!(
            f,
            "GroupMetadata(groupId = {}, generationId = {}, memberId = {}, groupInstanceId = {})",
            self.group_id, self.generation_id, self.member_id, group_instance_id_str
        )
    }
}

#[cfg(test)]
#[allow(deprecated)] // tests intentionally exercise the deprecated public constructors
mod tests {
    use super::*;

    #[test]
    fn test_new_unknown_defaults() {
        let m = ConsumerGroupMetadata::new("g");
        assert_eq!(m.group_id(), "g");
        assert_eq!(m.generation_id(), UNKNOWN_GENERATION_ID);
        assert_eq!(m.member_id(), UNKNOWN_MEMBER_ID);
        assert_eq!(m.group_instance_id(), None);
    }

    #[test]
    fn test_with_details() {
        let m = ConsumerGroupMetadata::with_generation_id_member_id_group_instance_id(
            "g",
            2,
            "member",
            Some("instance".into()),
        );
        assert_eq!(m.group_id(), "g");
        assert_eq!(m.generation_id(), 2);
        assert_eq!(m.member_id(), "member");
        assert_eq!(m.group_instance_id(), Some("instance"));
    }

    #[test]
    fn test_display() {
        let m = ConsumerGroupMetadata::with_generation_id_member_id_group_instance_id(
            "g",
            2,
            "member",
            Some("instance".into()),
        );
        assert_eq!(
            m.to_string(),
            "GroupMetadata(groupId = g, generationId = 2, memberId = member, groupInstanceId = instance)"
        );

        let m = ConsumerGroupMetadata::new("g");
        // groupInstanceId rendered as empty string (matches Java)
        assert_eq!(
            m.to_string(),
            "GroupMetadata(groupId = g, generationId = -1, memberId = , groupInstanceId = )"
        );
    }
}
