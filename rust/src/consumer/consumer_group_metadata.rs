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

//! Metadata containing consumer group information.
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

/// The consumer group information: group id, generation id, member id and
/// group instance id.
///
/// Corresponds to Java's
/// `org.apache.kafka.clients.consumer.ConsumerGroupMetadata`.
///
/// Java has deprecated both public constructors since 4.2
/// (`@Deprecated(since = "4.2", forRemoval = true)`): "please use
/// `KafkaConsumer#groupMetadata()` instead. This class will be an interface in
/// Kafka 5.0." The Rust client translates the class directly in its 5.0 shape,
/// a trait, so the deprecated constructors are never public. The only way to
/// obtain a value is [`Consumer::group_metadata`](super::Consumer::group_metadata),
/// which returns the implementation owned by the consumer. The trait is
/// dyn-compatible, so `send_offsets_to_transaction` accepts the metadata of any
/// consumer implementation, including a future classic-protocol one.
///
/// [`Display`](fmt::Display) on `dyn ConsumerGroupMetadata` renders Java's
/// `toString()`.
#[doc(alias = "org.apache.kafka.clients.consumer.ConsumerGroupMetadata")]
pub trait ConsumerGroupMetadata: Send + Sync + fmt::Debug + 'static {
    /// The consumer group ID.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerGroupMetadata#groupId")]
    fn group_id(&self) -> &str;

    /// The current generation ID, or `-1` (`UNKNOWN_GENERATION_ID`) if unknown.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerGroupMetadata#generationId")]
    fn generation_id(&self) -> i32;

    /// The member ID, or an empty string (`UNKNOWN_MEMBER_ID`) if unknown.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerGroupMetadata#memberId")]
    fn member_id(&self) -> &str;

    /// The group instance ID, if the consumer is a static member.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerGroupMetadata#groupInstanceId")]
    fn group_instance_id(&self) -> Option<&str>;
}

impl fmt::Display for dyn ConsumerGroupMetadata {
    /// Matches Java's
    /// `String.format("GroupMetadata(groupId = %s, generationId = %d, memberId = %s, groupInstanceId = %s)", …)`
    /// where `groupInstanceId` is replaced with an empty string if absent.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GroupMetadata(groupId = {}, generationId = {}, memberId = {}, groupInstanceId = {})",
            self.group_id(),
            self.generation_id(),
            self.member_id(),
            self.group_instance_id().unwrap_or("")
        )
    }
}

/// The crate's own [`ConsumerGroupMetadata`] implementation. It holds the four
/// fields of Java's class; the consumers build it and the transaction manager
/// snapshots into it.
///
/// Java has no such type, because `ConsumerGroupMetadata` is still a class
/// there (DoD #7 deviation). Turning the class into a trait separates the
/// public type from its data, and the data needs a holder. The struct and its
/// constructors are `pub(crate)`, so the deprecated Java constructors never
/// reach the public surface. `PartialEq` / `Hash` translate Java's `equals` /
/// `hashCode`, which compare only instances of the same class.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ConsumerGroupMetadataImpl {
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
}

impl ConsumerGroupMetadataImpl {
    /// Metadata for `group_id` with unknown generation and member ids.
    ///
    /// Corresponds to Java's `new ConsumerGroupMetadata(String)`.
    pub(crate) fn new(group_id: impl Into<String>) -> Self {
        Self::with_generation_id_member_id_group_instance_id(group_id, UNKNOWN_GENERATION_ID, UNKNOWN_MEMBER_ID, None)
    }

    /// Metadata with all four fields.
    ///
    /// Corresponds to Java's four-argument constructor.
    pub(crate) fn with_generation_id_member_id_group_instance_id(
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

    /// Copies the fields of any [`ConsumerGroupMetadata`] implementation.
    ///
    /// Java keeps the caller's reference. The fields are immutable, so an
    /// owned copy is equivalent, and it lets the transaction manager hold the
    /// metadata after the caller's borrow ends.
    pub(crate) fn copy_of(group_metadata: &dyn ConsumerGroupMetadata) -> Self {
        Self::with_generation_id_member_id_group_instance_id(
            group_metadata.group_id(),
            group_metadata.generation_id(),
            group_metadata.member_id(),
            group_metadata.group_instance_id().map(ToString::to_string),
        )
    }
}

impl ConsumerGroupMetadata for ConsumerGroupMetadataImpl {
    fn group_id(&self) -> &str {
        &self.group_id
    }

    fn generation_id(&self) -> i32 {
        self.generation_id
    }

    fn member_id(&self) -> &str {
        &self.member_id
    }

    fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }
}

impl fmt::Display for ConsumerGroupMetadataImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self as &dyn ConsumerGroupMetadata, f)
    }
}

/// Translated from `org.apache.kafka.clients.consumer.ConsumerGroupMetadataTest`.
///
/// The Java constructors under test are deprecated, and the Rust client has
/// no public ones, so the tests exercise the crate-internal
/// [`ConsumerGroupMetadataImpl`] constructors that translate them.
///
/// Skipped tests:
/// - `testInvalidGroupId`, `testInvalidMemberId`, `testInvalidInstanceId`
///   — Java asserts `NullPointerException` for nullable arguments. In Rust
///   the constructor takes `impl Into<String>` (non-null by the type
///   system) and `Option<String>` for the instance ID (`None` is the
///   "absent" sentinel). There is no equivalent runtime check to test.
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    const GROUP_ID: &str = "group";

    /// Translated from `ConsumerGroupMetadataTest.testAssignmentConstructor`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerGroupMetadataTest#testAssignmentConstructor")]
    fn test_assignment_constructor() {
        let member_id = "member";
        let generation_id = 2;
        let group_instance_id = "instance";

        let group_metadata = ConsumerGroupMetadataImpl::with_generation_id_member_id_group_instance_id(
            GROUP_ID,
            generation_id,
            member_id,
            Some(group_instance_id.to_string()),
        );

        assert_eq!(group_metadata.group_id(), GROUP_ID);
        assert_eq!(group_metadata.generation_id(), generation_id);
        assert_eq!(group_metadata.member_id(), member_id);
        assert!(group_metadata.group_instance_id().is_some());
        assert_eq!(group_metadata.group_instance_id(), Some(group_instance_id));
    }

    /// Translated from `ConsumerGroupMetadataTest.testGroupIdConstructor`.
    ///
    /// `JoinGroupRequest.UNKNOWN_GENERATION_ID = -1` and
    /// `JoinGroupRequest.UNKNOWN_MEMBER_ID = ""` per the Java request module.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerGroupMetadataTest#testGroupIdConstructor")]
    fn test_group_id_constructor() {
        let group_metadata = ConsumerGroupMetadataImpl::new(GROUP_ID);

        assert_eq!(group_metadata.group_id(), GROUP_ID);
        assert_eq!(group_metadata.generation_id(), UNKNOWN_GENERATION_ID);
        assert_eq!(group_metadata.member_id(), UNKNOWN_MEMBER_ID);
        assert!(group_metadata.group_instance_id().is_none());
    }

    #[test]
    fn test_display() {
        let m = ConsumerGroupMetadataImpl::with_generation_id_member_id_group_instance_id(
            "g",
            2,
            "member",
            Some("instance".into()),
        );
        assert_eq!(
            m.to_string(),
            "GroupMetadata(groupId = g, generationId = 2, memberId = member, groupInstanceId = instance)"
        );

        let m = ConsumerGroupMetadataImpl::new("g");
        // groupInstanceId rendered as empty string (matches Java)
        assert_eq!(
            m.to_string(),
            "GroupMetadata(groupId = g, generationId = -1, memberId = , groupInstanceId = )"
        );
    }

    /// The trait-object form a caller holds (`Consumer::group_metadata`'s
    /// return type) renders the same `toString()`.
    #[test]
    fn test_display_through_trait_object() {
        let m: Arc<dyn ConsumerGroupMetadata> =
            Arc::new(ConsumerGroupMetadataImpl::with_generation_id_member_id_group_instance_id(
                "g",
                2,
                "member",
                Some("instance".into()),
            ));
        assert_eq!(
            m.to_string(),
            "GroupMetadata(groupId = g, generationId = 2, memberId = member, groupInstanceId = instance)"
        );
    }

    /// `copy_of` preserves every field, including an absent instance id.
    #[test]
    fn test_copy_of() {
        let with_instance =
            ConsumerGroupMetadataImpl::with_generation_id_member_id_group_instance_id("g", 5, "m", Some("i".into()));
        assert_eq!(ConsumerGroupMetadataImpl::copy_of(&with_instance), with_instance);
        let without = ConsumerGroupMetadataImpl::new("g");
        assert_eq!(ConsumerGroupMetadataImpl::copy_of(&without), without);
    }
}
