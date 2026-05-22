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

//! Translated from
//! `org.apache.kafka.clients.consumer.ConsumerGroupMetadataTest`.
//!
//! Skipped tests:
//! - `testInvalidGroupId`, `testInvalidMemberId`, `testInvalidInstanceId`
//!   — Java asserts `NullPointerException` for nullable arguments. In Rust
//!   the constructor takes `impl Into<String>` (non-null by the type
//!   system) and `Option<String>` for the instance ID (`None` is the
//!   "absent" sentinel). There is no equivalent runtime check to test.

use confluent_kafka::consumer::ConsumerGroupMetadata;

const GROUP_ID: &str = "group";

/// Translated from `ConsumerGroupMetadataTest.testAssignmentConstructor`.
#[test]
fn test_assignment_constructor() {
    let member_id = "member";
    let generation_id = 2;
    let group_instance_id = "instance";

    let group_metadata =
        ConsumerGroupMetadata::with_details(GROUP_ID, generation_id, member_id, Some(group_instance_id.to_string()));

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
fn test_group_id_constructor() {
    let group_metadata = ConsumerGroupMetadata::new(GROUP_ID);

    assert_eq!(group_metadata.group_id(), GROUP_ID);
    assert_eq!(group_metadata.generation_id(), -1);
    assert_eq!(group_metadata.member_id(), "");
    assert!(group_metadata.group_instance_id().is_none());
}
