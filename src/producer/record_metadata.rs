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

//! The metadata for a record that has been acknowledged by the server.
//!
//! Translated from `org.apache.kafka.clients.producer.RecordMetadata`.

use std::fmt;

use crate::common::TopicPartition;
use crate::common::record::internal::RecordBatch;

/// Value used when the offset is unknown (i.e., `ProduceResponse.INVALID_OFFSET`).
pub const INVALID_OFFSET: i64 = -1;

/// Partition value for record without partition assigned.
pub const UNKNOWN_PARTITION: i32 = -1;

/// The metadata for a record that has been acknowledged by the server.
#[derive(Clone, Debug)]
pub struct RecordMetadata {
    /// The offset of the record in the topic/partition.
    offset: i64,
    /// The timestamp of the message.
    /// If LogAppendTime is used for the topic, the timestamp will be the timestamp returned
    /// by the broker.
    /// If CreateTime is used for the topic, the timestamp is the timestamp in the
    /// corresponding ProducerRecord if the user provided one. Otherwise, it will be the
    /// producer local time when the producer record was handed to the producer.
    timestamp: i64,
    /// The size of the serialized, uncompressed key in bytes. -1 if key is null.
    serialized_key_size: i32,
    /// The size of the serialized, uncompressed value in bytes. -1 if value is null.
    serialized_value_size: i32,
    /// The topic and partition the record was sent to.
    topic_partition: TopicPartition,
}

impl RecordMetadata {
    /// Creates a new instance with the provided parameters.
    pub fn new(
        topic_partition: TopicPartition,
        base_offset: i64,
        batch_index: i32,
        timestamp: i64,
        serialized_key_size: i32,
        serialized_value_size: i32,
    ) -> Self {
        // Ignore the batch_index if the base offset is -1, since this indicates the offset
        // is unknown
        let offset = if base_offset == -1 {
            base_offset
        } else {
            base_offset + batch_index as i64
        };
        Self { offset, timestamp, serialized_key_size, serialized_value_size, topic_partition }
    }

    /// Indicates whether the record metadata includes the offset.
    pub fn has_offset(&self) -> bool {
        self.offset != INVALID_OFFSET
    }

    /// The offset of the record in the topic/partition.
    /// Returns -1 if [`has_offset()`](Self::has_offset) returns false.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Indicates whether the record metadata includes the timestamp.
    pub fn has_timestamp(&self) -> bool {
        self.timestamp != RecordBatch::NO_TIMESTAMP
    }

    /// The timestamp of the record in the topic/partition.
    /// Returns -1 if [`has_timestamp()`](Self::has_timestamp) returns false.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// The size of the serialized, uncompressed key in bytes.
    /// Returns -1 if key is null.
    pub fn serialized_key_size(&self) -> i32 {
        self.serialized_key_size
    }

    /// The size of the serialized, uncompressed value in bytes.
    /// Returns -1 if value is null.
    pub fn serialized_value_size(&self) -> i32 {
        self.serialized_value_size
    }

    /// The topic the record was appended to.
    pub fn topic(&self) -> &str {
        self.topic_partition.topic()
    }

    /// The partition the record was sent to.
    pub fn partition(&self) -> i32 {
        self.topic_partition.partition()
    }

    /// The topic and partition the record was sent to.
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }
}

impl fmt::Display for RecordMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.topic_partition, self.offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `RecordMetadataTest.testConstructionWithMissingBatchIndex`.
    #[test]
    fn test_construction_with_missing_batch_index() {
        let tp = TopicPartition::new("foo".to_string(), 0);
        let timestamp = 2340234_i64;
        let key_size = 3;
        let value_size = 5;

        let metadata = RecordMetadata::new(tp, -1, -1, timestamp, key_size, value_size);
        assert_eq!("foo", metadata.topic());
        assert_eq!(0, metadata.partition());
        assert_eq!(timestamp, metadata.timestamp());
        assert!(!metadata.has_offset());
        assert_eq!(-1, metadata.offset());
        assert_eq!(key_size, metadata.serialized_key_size());
        assert_eq!(value_size, metadata.serialized_value_size());
    }

    /// Translated from `RecordMetadataTest.testConstructionWithBatchIndexOffset`.
    #[test]
    fn test_construction_with_batch_index_offset() {
        let tp = TopicPartition::new("foo".to_string(), 0);
        let timestamp = 2340234_i64;
        let key_size = 3;
        let value_size = 5;
        let base_offset = 15_i64;
        let batch_index = 3;

        let metadata = RecordMetadata::new(tp, base_offset, batch_index, timestamp, key_size, value_size);
        assert_eq!("foo", metadata.topic());
        assert_eq!(0, metadata.partition());
        assert_eq!(timestamp, metadata.timestamp());
        assert_eq!(base_offset + batch_index as i64, metadata.offset());
        assert_eq!(key_size, metadata.serialized_key_size());
        assert_eq!(value_size, metadata.serialized_value_size());
    }

    #[test]
    fn test_record_metadata_with_offset() {
        let tp = TopicPartition::new("test-topic".to_string(), 0);
        let metadata = RecordMetadata::new(tp, 100, 5, 1234567890, 10, 20);
        assert!(metadata.has_offset());
        assert_eq!(metadata.offset(), 105);
        assert!(metadata.has_timestamp());
        assert_eq!(metadata.timestamp(), 1234567890);
        assert_eq!(metadata.serialized_key_size(), 10);
        assert_eq!(metadata.serialized_value_size(), 20);
        assert_eq!(metadata.topic(), "test-topic");
        assert_eq!(metadata.partition(), 0);
    }

    #[test]
    fn test_record_metadata_unknown_offset() {
        let tp = TopicPartition::new("test-topic".to_string(), 0);
        let metadata = RecordMetadata::new(tp, -1, 5, RecordBatch::NO_TIMESTAMP, -1, -1);
        assert!(!metadata.has_offset());
        assert_eq!(metadata.offset(), -1);
        assert!(!metadata.has_timestamp());
    }

    #[test]
    fn test_record_metadata_display() {
        let tp = TopicPartition::new("test-topic".to_string(), 3);
        let metadata = RecordMetadata::new(tp, 42, 0, 0, 0, 0);
        assert_eq!(metadata.to_string(), "test-topic-3@42");
    }
}
