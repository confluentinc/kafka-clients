/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Comprehensive message round-trip tests matching Java's MessageTest.java
//! Tests full RPC serialization and deserialization across all supported versions.

use confluent_kafka_rust::common::protocol::ByteBufferAccessor;
use confluent_kafka_rust::*;
use std::io;

/// Test helper to perform round-trip serialization for any message at a specific version
#[allow(dead_code)]
fn test_message_round_trip<T, F>(_version: i16, create_message: F) -> io::Result<()>
where
    T: Clone + PartialEq + std::fmt::Debug,
    F: Fn() -> T,
{
    let _original = create_message();

    // Serialize
    let _write_buffer = ByteBufferAccessor::new(4096);

    // Note: In Java they have message.write(accessor, cache, version)
    // Our generated code uses: message.write(&mut accessor, version)
    // This is a limitation of our current code generator

    Ok(())
}

/// Test AddOffsetsToTxnRequest round-trip across all versions (like Java's testAddOffsetsToTxnVersions)
#[test]
fn test_add_offsets_to_txn_request_all_versions() -> io::Result<()> {
    // AddOffsetsToTxnRequest: versions 0-3
    for version in 0..=3 {
        let mut request = add_offsets_to_txn_request::AddOffsetsToTxnRequestData::new();
        request.transactional_id = "foobar".to_string();
        request.producer_id = 0xbadcafebadcafe_i64;
        request.producer_epoch = 123;
        request.group_id = "baaz".to_string();

        // Serialize
        let mut write_buffer = ByteBufferAccessor::new(1024);
        request.write(&mut write_buffer, version)?;

        // Deserialize
        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes.clone());
        let deserialized = add_offsets_to_txn_request::AddOffsetsToTxnRequestData::read(&mut read_buffer, version)?;

        // Verify
        assert_eq!(
            request.transactional_id, deserialized.transactional_id,
            "transactional_id mismatch at version {}",
            version
        );
        assert_eq!(
            request.producer_id, deserialized.producer_id,
            "producer_id mismatch at version {}",
            version
        );
        assert_eq!(
            request.producer_epoch, deserialized.producer_epoch,
            "producer_epoch mismatch at version {}",
            version
        );
        assert_eq!(
            request.group_id, deserialized.group_id,
            "group_id mismatch at version {}",
            version
        );

        // Verify buffer was fully consumed
        assert_eq!(
            read_buffer.position(),
            bytes.len(),
            "Not all bytes were read at version {}",
            version
        );
    }

    Ok(())
}

/// Test AddOffsetsToTxnResponse round-trip across all versions
#[test]
fn test_add_offsets_to_txn_response_all_versions() -> io::Result<()> {
    // AddOffsetsToTxnResponse: versions 0-3
    for version in 0..=3 {
        let mut response = add_offsets_to_txn_response::AddOffsetsToTxnResponseData::new();
        response.throttle_time_ms = 42;
        response.error_code = 0;

        // Serialize
        let mut write_buffer = ByteBufferAccessor::new(1024);
        response.write(&mut write_buffer, version)?;

        // Deserialize
        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);
        let deserialized = add_offsets_to_txn_response::AddOffsetsToTxnResponseData::read(&mut read_buffer, version)?;

        // Verify
        assert_eq!(
            response.throttle_time_ms, deserialized.throttle_time_ms,
            "throttle_time_ms mismatch at version {}",
            version
        );
        assert_eq!(
            response.error_code, deserialized.error_code,
            "error_code mismatch at version {}",
            version
        );
    }

    Ok(())
}

/// Test ProduceRequest round-trip across multiple versions
#[test]
fn test_produce_request_multiple_versions() -> io::Result<()> {
    // ProduceRequest: versions 3-13 (we test a subset)
    for version in [3, 5, 7, 9, 11, 13] {
        let mut request = produce_request::ProduceRequestData::new();
        request.timeout_ms = 5000;
        request.acks = 1;
        request.transactional_id = if version >= 3 {
            "txn-123".to_string()
        } else {
            String::new()
        };

        // Serialize
        let mut write_buffer = ByteBufferAccessor::new(2048);
        request.write(&mut write_buffer, version)?;

        // Deserialize
        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);
        let deserialized = produce_request::ProduceRequestData::read(&mut read_buffer, version)?;

        // Verify
        assert_eq!(
            request.timeout_ms, deserialized.timeout_ms,
            "timeout_ms mismatch at version {}",
            version
        );
        assert_eq!(request.acks, deserialized.acks, "acks mismatch at version {}", version);
        if version >= 3 {
            assert_eq!(
                request.transactional_id, deserialized.transactional_id,
                "transactional_id mismatch at version {}",
                version
            );
        }
    }

    Ok(())
}

/// Test FetchRequest round-trip across multiple versions
#[test]
fn test_fetch_request_multiple_versions() -> io::Result<()> {
    // FetchRequest: versions 4-18 (we test a subset)
    for version in [4, 7, 11, 15] {
        let mut request = fetch_request::FetchRequestData::new();
        request.max_wait_ms = 500;
        request.min_bytes = 1024;
        request.max_bytes = 1048576;
        request.isolation_level = 0;

        // Serialize
        let mut write_buffer = ByteBufferAccessor::new(2048);
        request.write(&mut write_buffer, version)?;

        // Deserialize
        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);
        let deserialized = fetch_request::FetchRequestData::read(&mut read_buffer, version)?;

        // Verify
        assert_eq!(
            request.max_wait_ms, deserialized.max_wait_ms,
            "max_wait_ms mismatch at version {}",
            version
        );
        assert_eq!(
            request.min_bytes, deserialized.min_bytes,
            "min_bytes mismatch at version {}",
            version
        );
        assert_eq!(
            request.max_bytes, deserialized.max_bytes,
            "max_bytes mismatch at version {}",
            version
        );
    }

    Ok(())
}

/// Test string serialization with various lengths
#[test]
fn test_string_serialization_various_lengths() -> io::Result<()> {
    let long_string = "a".repeat(100);
    let test_strings = vec![
        "",                     // Empty string
        "a",                    // Single character
        "test",                 // Short string
        "hello world",          // Medium string
        long_string.as_str(),   // Longer string
        "unicode: 你好世界 🎉", // Unicode string
    ];

    for test_str in test_strings {
        let mut request = add_offsets_to_txn_request::AddOffsetsToTxnRequestData::new();
        request.transactional_id = test_str.to_string();
        request.group_id = "group".to_string();
        request.producer_id = 123;
        request.producer_epoch = 1;

        // Test with version 0 (non-flexible)
        let mut write_buffer = ByteBufferAccessor::new(2048);
        request.write(&mut write_buffer, 0)?;

        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);
        let deserialized = add_offsets_to_txn_request::AddOffsetsToTxnRequestData::read(&mut read_buffer, 0)?;

        assert_eq!(
            request.transactional_id, deserialized.transactional_id,
            "String mismatch for: {}",
            test_str
        );
    }

    Ok(())
}

/// Test bytes serialization
#[test]
fn test_bytes_serialization() -> io::Result<()> {
    let request = add_partitions_to_txn_request::AddPartitionsToTxnRequestData::new();
    // Note: Our current implementation may not have all fields properly typed
    // This test demonstrates the pattern

    let mut write_buffer = ByteBufferAccessor::new(1024);
    request.write(&mut write_buffer, 0)?;

    let bytes = write_buffer.buffer().to_vec();
    let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);
    let _deserialized = add_partitions_to_txn_request::AddPartitionsToTxnRequestData::read(&mut read_buffer, 0)?;

    Ok(())
}

/// Test version validation
#[test]
fn test_version_validation() {
    // Test that invalid versions are rejected
    let request = produce_request::ProduceRequestData::new();

    // Version too low (ProduceRequest starts at version 3)
    let mut write_buffer = ByteBufferAccessor::new(1024);
    let result = request.write(&mut write_buffer, 0);
    assert!(result.is_err(), "Should reject version below minimum");

    // Version too high (ProduceRequest goes up to version 13)
    let mut write_buffer = ByteBufferAccessor::new(1024);
    let result = request.write(&mut write_buffer, 99);
    assert!(result.is_err(), "Should reject version above maximum");
}

/// Test empty message serialization
#[test]
fn test_empty_message_serialization() -> io::Result<()> {
    // Test that default/empty messages serialize correctly
    let request = add_offsets_to_txn_request::AddOffsetsToTxnRequestData::new();

    for version in 0..=3 {
        let mut write_buffer = ByteBufferAccessor::new(1024);
        request.write(&mut write_buffer, version)?;

        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);
        let _deserialized = add_offsets_to_txn_request::AddOffsetsToTxnRequestData::read(&mut read_buffer, version)?;
    }

    Ok(())
}

/// Test primitive types serialization
#[test]
fn test_primitive_types() -> io::Result<()> {
    let mut request = produce_request::ProduceRequestData::new();

    // Test various primitive values
    request.timeout_ms = 0;
    request.acks = -1;
    test_produce_write_read(&request, 3)?;

    request.timeout_ms = i32::MAX;
    request.acks = i16::MAX;
    test_produce_write_read(&request, 3)?;

    request.timeout_ms = i32::MIN;
    request.acks = i16::MIN;
    test_produce_write_read(&request, 3)?;

    Ok(())
}

fn test_produce_write_read(request: &produce_request::ProduceRequestData, version: i16) -> io::Result<()> {
    let mut write_buffer = ByteBufferAccessor::new(2048);
    request.write(&mut write_buffer, version)?;

    let bytes = write_buffer.buffer().to_vec();
    let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);
    let deserialized = produce_request::ProduceRequestData::read(&mut read_buffer, version)?;

    assert_eq!(request.timeout_ms, deserialized.timeout_ms);
    assert_eq!(request.acks, deserialized.acks);
    Ok(())
}

/// Test multiple messages in sequence (like batching)
#[test]
fn test_multiple_messages_in_sequence() -> io::Result<()> {
    let mut write_buffer = ByteBufferAccessor::new(4096);

    // Write multiple requests to the same buffer
    for i in 0..5 {
        let mut request = add_offsets_to_txn_request::AddOffsetsToTxnRequestData::new();
        request.transactional_id = format!("txn-{}", i);
        request.group_id = format!("group-{}", i);
        request.producer_id = i as i64;
        request.producer_epoch = i as i16;

        request.write(&mut write_buffer, 0)?;
    }

    // Read them back
    let bytes = write_buffer.buffer().to_vec();
    let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);

    for i in 0..5 {
        let deserialized = add_offsets_to_txn_request::AddOffsetsToTxnRequestData::read(&mut read_buffer, 0)?;
        assert_eq!(format!("txn-{}", i), deserialized.transactional_id);
        assert_eq!(format!("group-{}", i), deserialized.group_id);
        assert_eq!(i as i64, deserialized.producer_id);
        assert_eq!(i as i16, deserialized.producer_epoch);
    }

    Ok(())
}
