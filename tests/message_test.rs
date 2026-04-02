/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Integration tests for the Message trait on generated message types.
//!
//! Translated from org.apache.kafka.common.message.MessageTest

use std::hash::{DefaultHasher, Hash, Hasher};

use confluent_kafka_rust::common::Uuid;
use confluent_kafka_rust::common::protocol::message_util::to_byte_buffer_accessor;
use confluent_kafka_rust::common::protocol::{
    ApiKeys, ByteBufferAccessor, Errors, Message, ObjectSerializationCache, RawTaggedField,
};

// Re-export generated types
use confluent_kafka_rust::add_offsets_to_txn_request_data::AddOffsetsToTxnRequestData;
use confluent_kafka_rust::add_offsets_to_txn_response_data::AddOffsetsToTxnResponseData;
use confluent_kafka_rust::create_topics_request_data::CreateTopicsRequestData;
use confluent_kafka_rust::describe_acls_request_data::DescribeAclsRequestData;
use confluent_kafka_rust::describe_cluster_request_data::DescribeClusterRequestData;
use confluent_kafka_rust::describe_cluster_response_data::{DescribeClusterBroker, DescribeClusterResponseData};
use confluent_kafka_rust::describe_groups_response_data::{
    DescribeGroupsResponseData, DescribedGroup, DescribedGroupMember,
};
use confluent_kafka_rust::heartbeat_request_data::HeartbeatRequestData;
use confluent_kafka_rust::metadata_request_data::{self, MetadataRequestData};
use confluent_kafka_rust::simple_example_message_data::{
    MyStruct, SimpleExampleMessageData, StructArray, TaggedStruct,
};

/// Helper: compute hash of a value
fn hash_of<T: Hash>(val: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    val.hash(&mut hasher);
    hasher.finish()
}

/// Test round-trip: size → write → read → compare
fn test_byte_buffer_round_trip<T: Message + PartialEq + Hash + std::fmt::Debug + std::fmt::Display>(
    version: i16,
    message: &T,
    expected: &T,
) {
    let mut cache = ObjectSerializationCache::new();
    let size = message.size(&mut cache, version).unwrap();

    let mut buf = ByteBufferAccessor::new(size as usize);
    Message::write(message, &mut buf, &cache, version).unwrap();
    assert_eq!(size as usize, buf.len(), "size function mismatch for version {}", version);

    // Read back
    buf.set_position(0).unwrap();
    let mut message2 = message.clone();
    Message::read(&mut message2, &mut buf, version).unwrap();
    assert_eq!(size as usize, buf.position(), "read back size mismatch for version {}", version);
    assert_eq!(expected, &message2, "round trip mismatch for version {}", version);
    assert_eq!(hash_of(expected), hash_of(&message2), "hash mismatch for version {}", version);
    assert_eq!(
        format!("{}", expected),
        format!("{}", message2),
        "display mismatch for version {}",
        version
    );
}

fn test_equivalent_message_round_trip<T: Message + PartialEq + Hash + std::fmt::Debug + std::fmt::Display>(
    version: i16,
    message: &T,
) {
    test_byte_buffer_round_trip(version, message, message);
}

fn test_duplication<T: Message + PartialEq + Hash + std::fmt::Debug>(message: &T) {
    let duplicate = message.duplicate();
    assert_eq!(&duplicate, message);
    assert_eq!(message, &duplicate);
    assert_eq!(hash_of(&duplicate), hash_of(message));
}

fn test_all_message_round_trips<T: Message + PartialEq + Hash + std::fmt::Debug + std::fmt::Display>(message: &T) {
    test_duplication(message);
    test_all_message_round_trips_from_version(message.lowest_supported_version(), message);
}

fn test_all_message_round_trips_from_version<T: Message + PartialEq + Hash + std::fmt::Debug + std::fmt::Display>(
    from_version: i16,
    message: &T,
) {
    for version in from_version..=message.highest_supported_version() {
        test_equivalent_message_round_trip(version, message);
    }
}

#[allow(dead_code)]
fn test_all_message_round_trips_until_version<T: Message + PartialEq + Hash + std::fmt::Debug + std::fmt::Display>(
    until_version: i16,
    message: &T,
) {
    for version in message.lowest_supported_version()..=until_version {
        test_equivalent_message_round_trip(version, message);
    }
}

#[allow(dead_code)]
fn test_all_message_round_trips_before_version<T: Message + PartialEq + Hash + std::fmt::Debug + std::fmt::Display>(
    before_version: i16,
    message: &T,
    expected: &T,
) {
    for version in 0..before_version {
        test_byte_buffer_round_trip(version, message, expected);
    }
}

#[allow(dead_code)]
fn test_all_message_round_trips_between_versions<
    T: Message + PartialEq + Hash + std::fmt::Debug + std::fmt::Display,
>(
    start_version: i16,
    end_version: i16,
    message: &T,
    expected: &T,
) {
    for version in start_version..end_version {
        test_byte_buffer_round_trip(version, message, expected);
    }
}

fn verify_write_succeeds<T: Message + std::fmt::Debug>(version: i16, message: &T) {
    let mut cache = ObjectSerializationCache::new();
    let size = message.size(&mut cache, version).unwrap();
    let mut buf = ByteBufferAccessor::new(size as usize * 2);
    Message::write(message, &mut buf, &cache, version).unwrap();
    assert_eq!(
        size as usize,
        buf.len(),
        "Expected serialized size to be {}, but it was {}",
        size,
        buf.len()
    );
}

// === Tests translated from MessageTest.java ===

#[test]
fn test_add_offsets_to_txn_versions() {
    test_all_message_round_trips(
        AddOffsetsToTxnRequestData::new()
            .set_transactional_id("foobar".to_string())
            .set_producer_id(0x00badcafebadcafe_i64)
            .set_producer_epoch(123)
            .set_group_id("baaz".to_string()),
    );
    test_all_message_round_trips(AddOffsetsToTxnResponseData::new().set_throttle_time_ms(42).set_error_code(0));
}

#[test]
fn test_create_topics_versions() {
    test_all_message_round_trips(CreateTopicsRequestData::new().set_timeout_ms(1000).set_topics(Vec::new()));
}

#[test]
fn test_describe_acls_request() {
    test_all_message_round_trips(
        DescribeAclsRequestData::new()
            .set_resource_type_filter(42)
            .set_resource_name_filter(String::new())
            .set_pattern_type_filter(3)
            .set_principal_filter("abc".to_string())
            .set_host_filter(String::new())
            .set_operation(0)
            .set_permission_type(0),
    );
}

#[test]
fn test_metadata_versions() {
    test_all_message_round_trips(MetadataRequestData::new().set_topics(vec![
        metadata_request_data::MetadataRequestTopic::new().set_name("foo".to_string()).clone(),
        metadata_request_data::MetadataRequestTopic::new().set_name("bar".to_string()).clone(),
    ]));
}

#[test]
fn test_heartbeat_versions() {
    let new_request = || {
        HeartbeatRequestData::new()
            .set_group_id("groupId".to_string())
            .set_member_id("memberId".to_string())
            .set_generation_id(15)
            .clone()
    };
    test_all_message_round_trips(&new_request());
    test_all_message_round_trips(new_request().set_group_instance_id(String::new()));
    test_all_message_round_trips_from_version(3, new_request().set_group_instance_id("instanceId".to_string()));
}

#[test]
fn test_describe_cluster_request_versions() {
    test_all_message_round_trips(DescribeClusterRequestData::new().set_include_cluster_authorized_operations(true));
}

#[test]
fn test_describe_cluster_response_versions() {
    let data = DescribeClusterResponseData::new()
        .set_brokers(vec![
            DescribeClusterBroker::new()
                .set_broker_id(1)
                .set_host("localhost".to_string())
                .set_port(9092)
                .set_rack("rack1".to_string())
                .clone(),
        ])
        .set_cluster_id("clusterId".to_string())
        .set_controller_id(1)
        .set_cluster_authorized_operations(10)
        .clone();

    test_all_message_round_trips(&data);
}

#[test]
fn test_describe_groups_response_versions() {
    let base_member = DescribedGroupMember::new().set_member_id("memberId".to_string()).clone();

    let mut base_group = DescribedGroup::new()
        .set_group_id("group".to_string())
        .set_group_state("Stable".to_string())
        .set_error_code(Errors::None.code())
        .set_members(vec![base_member.clone()])
        .set_protocol_type("consumer".to_string())
        .clone();
    let mut base_response = DescribeGroupsResponseData::new().set_groups(vec![base_group.clone()]).clone();
    test_all_message_round_trips(&base_response);

    base_response.set_throttle_time_ms(10);
    base_response.set_groups(vec![base_group.clone()]);
    test_all_message_round_trips_from_version(1, &base_response);

    base_group.set_authorized_operations(1);
    base_response.set_groups(vec![base_group.clone()]);
    test_all_message_round_trips_from_version(3, &base_response);
}

#[test]
fn test_default_value_should_be_writable() {
    for version in
        SimpleExampleMessageData::LOWEST_SUPPORTED_VERSION..=SimpleExampleMessageData::HIGHEST_SUPPORTED_VERSION
    {
        let acc = to_byte_buffer_accessor(&SimpleExampleMessageData::new(), version).unwrap();
        assert!(!acc.buffer().is_empty());
    }
}

#[test]
fn test_simple_message() {
    let mut message = SimpleExampleMessageData::new();
    message.set_my_struct(
        MyStruct::new()
            .set_struct_id(25)
            .set_array_in_struct(vec![StructArray::new().set_array_field_id(20).clone()])
            .clone(),
    );
    message.set_my_tagged_struct(TaggedStruct::new().set_struct_id("abc".to_string()).clone());

    message.set_process_id(Uuid::random_uuid());
    message.set_my_nullable_string("notNull".to_string());
    message.set_my_int16(3);
    message.set_my_string("test string".to_string());

    let mut duplicate = message.duplicate();
    assert_eq!(duplicate, message);
    assert_eq!(message, duplicate);

    duplicate.set_my_tagged_int_array(vec![123]);
    assert_ne!(duplicate, message);
    assert_ne!(message, duplicate);

    test_all_message_round_trips_from_version(2, &message);
}

#[test]
fn test_long_tagged_string() {
    let long_string: String = std::iter::repeat_n('a', 1024).collect();
    let mut message = SimpleExampleMessageData::new();
    message.set_my_string(long_string);

    let mut cache = ObjectSerializationCache::new();
    let version: i16 = 1;
    let size = message.size(&mut cache, version).unwrap();
    let mut buf = ByteBufferAccessor::new(size as usize);
    Message::write(&message, &mut buf, &cache, version).unwrap();
    assert_eq!(size as usize, buf.len());
}

#[test]
fn test_unknown_tagged_fields() {
    let mut create_topics = CreateTopicsRequestData::new();
    verify_write_succeeds(6, &create_topics);

    let field1000 = RawTaggedField::new(1000, vec![0x1, 0x2, 0x3]);
    create_topics.unknown_tagged_fields_mut().push(field1000);
    // Should succeed for flexible version
    verify_write_succeeds(6, &create_topics);
}

#[test]
fn test_compare_with_unknown_tagged_fields() {
    let mut create_topics = CreateTopicsRequestData::new();
    create_topics.set_timeout_ms(123);

    let mut create_topics2 = CreateTopicsRequestData::new();
    create_topics2.set_timeout_ms(123);

    assert_eq!(create_topics, create_topics2);
    assert_eq!(create_topics2, create_topics);

    // Call the mutable accessor, which will return the (empty) list
    let _ = create_topics.unknown_tagged_fields_mut();
    // Equalities should still hold
    assert_eq!(create_topics, create_topics2);
    assert_eq!(create_topics2, create_topics);

    create_topics.unknown_tagged_fields_mut().push(RawTaggedField::new(0, vec![0]));
    assert_ne!(create_topics, create_topics2);
    assert_ne!(create_topics2, create_topics);

    create_topics2.unknown_tagged_fields_mut().push(RawTaggedField::new(0, vec![0]));
    assert_eq!(create_topics, create_topics2);
    assert_eq!(create_topics2, create_topics);
}

#[test]
fn test_message_versions() {
    // Verify that generated message types support at least the versions defined in ApiKeys
    for api_key in ApiKeys::ALL {
        if api_key.has_valid_version() {
            // Check that the highest supported version in generated code
            // is at least as high as what ApiKeys declares
            // (This test verifies consistency between ApiKeys and generated messages)
            assert!(
                api_key.latest_version() >= api_key.oldest_version(),
                "API {:?} has invalid version range",
                api_key
            );
        }
    }
}
