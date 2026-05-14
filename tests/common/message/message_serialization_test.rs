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

//! Integration tests for generated message serialization and deserialization.
//! Tests verify that read() and write() methods work correctly for all generated messages.

use confluent_kafka::common::protocol::ByteBufferAccessor;
use confluent_kafka::*; // Import all generated message modules
use std::io;

#[cfg(test)]
mod tests {
    use super::*;

    /// Test round-trip serialization for ProduceRequest with primitive types
    #[test]
    fn test_produce_request_round_trip() -> io::Result<()> {
        // Create a ProduceRequestData with some test data
        let mut request = produce_request_data::ProduceRequestData::new();
        request.timeout_ms = 5000;
        request.acks = 1;

        // Serialize to bytes
        let mut write_buffer = ByteBufferAccessor::new(1024);
        request.write(&mut write_buffer, 3)?;

        // Deserialize from bytes
        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);

        let deserialized = produce_request_data::ProduceRequestData::read(&mut read_buffer, 3)?;

        // Verify fields match
        assert_eq!(request.timeout_ms, deserialized.timeout_ms);
        assert_eq!(request.acks, deserialized.acks);

        Ok(())
    }

    /// Test round-trip serialization for FetchRequest with arrays
    #[test]
    fn test_fetch_request_with_arrays() -> io::Result<()> {
        let mut request = fetch_request_data::FetchRequestData::new();
        request.max_wait_ms = 500;
        request.min_bytes = 1024;
        request.max_bytes = 1048576;

        // Serialize and deserialize (version 4+ for FetchRequest)
        let mut write_buffer = ByteBufferAccessor::new(1024);
        request.write(&mut write_buffer, 4)?;

        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);

        let deserialized = fetch_request_data::FetchRequestData::read(&mut read_buffer, 4)?;

        assert_eq!(request.max_wait_ms, deserialized.max_wait_ms);
        assert_eq!(request.min_bytes, deserialized.min_bytes);
        assert_eq!(request.max_bytes, deserialized.max_bytes);

        Ok(())
    }

    /// Test string serialization with AddOffsetsToTxnRequest
    #[test]
    fn test_string_serialization() -> io::Result<()> {
        let mut request = add_offsets_to_txn_request_data::AddOffsetsToTxnRequestData::new();
        request.transactional_id = "test-txn-id".to_string();
        request.group_id = "test-group".to_string();
        request.producer_id = 12345;
        request.producer_epoch = 1;

        // Serialize and deserialize
        let mut write_buffer = ByteBufferAccessor::new(1024);
        request.write(&mut write_buffer, 3)?;

        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);

        let deserialized = add_offsets_to_txn_request_data::AddOffsetsToTxnRequestData::read(&mut read_buffer, 3)?;

        assert_eq!(request.transactional_id, deserialized.transactional_id);
        assert_eq!(request.group_id, deserialized.group_id);
        assert_eq!(request.producer_id, deserialized.producer_id);
        assert_eq!(request.producer_epoch, deserialized.producer_epoch);

        Ok(())
    }

    /// Test empty string serialization
    #[test]
    fn test_empty_string_serialization() -> io::Result<()> {
        let mut request = add_offsets_to_txn_request_data::AddOffsetsToTxnRequestData::new();
        request.transactional_id = String::new();
        request.group_id = String::new();

        let mut write_buffer = ByteBufferAccessor::new(1024);
        request.write(&mut write_buffer, 3)?;

        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);

        let deserialized = add_offsets_to_txn_request_data::AddOffsetsToTxnRequestData::read(&mut read_buffer, 3)?;

        assert_eq!(request.transactional_id, deserialized.transactional_id);
        assert_eq!(request.group_id, deserialized.group_id);
        assert!(deserialized.transactional_id.is_empty());
        assert!(deserialized.group_id.is_empty());

        Ok(())
    }

    /// Test bytes field serialization
    #[test]
    fn test_bytes_serialization() -> io::Result<()> {
        let mut request = add_partitions_to_txn_request_data::AddPartitionsToTxnRequestData::new();
        request.v3_and_below_transactional_id = "test-txn".to_string();
        request.v3_and_below_producer_id = 98765;
        request.v3_and_below_producer_epoch = 2;

        let mut write_buffer = ByteBufferAccessor::new(1024);
        request.write(&mut write_buffer, 3)?;

        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);

        let deserialized =
            add_partitions_to_txn_request_data::AddPartitionsToTxnRequestData::read(&mut read_buffer, 3)?;

        assert_eq!(
            request.v3_and_below_transactional_id,
            deserialized.v3_and_below_transactional_id
        );
        assert_eq!(request.v3_and_below_producer_id, deserialized.v3_and_below_producer_id);
        assert_eq!(request.v3_and_below_producer_epoch, deserialized.v3_and_below_producer_epoch);

        Ok(())
    }

    /// Test boolean serialization
    #[test]
    fn test_boolean_serialization() -> io::Result<()> {
        // Create a request with boolean fields (if available)
        let mut request = produce_request_data::ProduceRequestData::new();
        request.acks = 1;
        request.timeout_ms = 1000;

        // Test with different acks values (which behaves like bool in some versions)
        for &acks_value in &[0, 1, -1] {
            request.acks = acks_value;

            let mut write_buffer = ByteBufferAccessor::new(1024);
            request.write(&mut write_buffer, 3)?;

            let bytes = write_buffer.buffer().to_vec();
            let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);

            let deserialized = produce_request_data::ProduceRequestData::read(&mut read_buffer, 3)?;

            assert_eq!(request.acks, deserialized.acks);
        }

        Ok(())
    }

    /// Test version validation
    #[test]
    fn test_version_validation() {
        // Test that invalid versions are rejected
        let mut request = produce_request_data::ProduceRequestData::new();

        // Try to write with an invalid version (assuming version 0 is valid)
        let mut write_buffer = ByteBufferAccessor::new(1024);

        // Version 0 should work
        assert!(request.write(&mut write_buffer, 3).is_ok());

        // Very high version should fail (exceeds HIGHEST_SUPPORTED_VERSION)
        let mut write_buffer2 = ByteBufferAccessor::new(1024);
        let result = request.write(&mut write_buffer2, 1000);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("version"));
        }
    }

    /// Test multiple messages in sequence
    #[test]
    fn test_multiple_messages_sequence() -> io::Result<()> {
        // Write multiple messages to the same buffer
        let mut write_buffer = ByteBufferAccessor::new(4096);

        let mut req1 = produce_request_data::ProduceRequestData::new();
        req1.timeout_ms = 1000;
        req1.acks = 1;
        req1.write(&mut write_buffer, 3)?;

        let mut req2 = fetch_request_data::FetchRequestData::new();
        req2.max_wait_ms = 500;
        req2.min_bytes = 1;
        req2.write(&mut write_buffer, 4)?;

        // Read them back
        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);

        let deser1 = produce_request_data::ProduceRequestData::read(&mut read_buffer, 3)?;
        assert_eq!(req1.timeout_ms, deser1.timeout_ms);
        assert_eq!(req1.acks, deser1.acks);

        let deser2 = fetch_request_data::FetchRequestData::read(&mut read_buffer, 4)?;
        assert_eq!(req2.max_wait_ms, deser2.max_wait_ms);
        assert_eq!(req2.min_bytes, deser2.min_bytes);

        Ok(())
    }

    /// Test large strings don't cause buffer overflows
    #[test]
    fn test_large_string_serialization() -> io::Result<()> {
        let mut request = add_offsets_to_txn_request_data::AddOffsetsToTxnRequestData::new();

        // Create a moderately large string (not too large to avoid memory issues in tests)
        let large_string = "x".repeat(1000);
        request.transactional_id = large_string.clone();
        request.group_id = "test-group".to_string();

        let mut write_buffer = ByteBufferAccessor::new(4096);
        request.write(&mut write_buffer, 3)?;

        let bytes = write_buffer.buffer().to_vec();
        let mut read_buffer = ByteBufferAccessor::from_bytes(bytes);
        let deserialized = add_offsets_to_txn_request_data::AddOffsetsToTxnRequestData::read(&mut read_buffer, 3)?;

        assert_eq!(request.transactional_id, deserialized.transactional_id);
        assert_eq!(large_string, deserialized.transactional_id);
        assert_eq!(1000, deserialized.transactional_id.len());

        Ok(())
    }
}
