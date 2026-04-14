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

//! Record metadata returned after a record has been acknowledged by the server.
//!
//! Corresponds to Java's `org.apache.kafka.clients.producer.RecordMetadata`.

use std::fmt;

use crate::common::TopicPartition;

/// Partition value for record without partition assigned.
pub const UNKNOWN_PARTITION: i32 = -1;

/// The invalid offset constant, matching Java's `ProduceResponse.INVALID_OFFSET`.
const INVALID_OFFSET: i64 = -1;

/// The no-timestamp constant, matching Java's `RecordBatch.NO_TIMESTAMP`.
const NO_TIMESTAMP: i64 = -1;

/// The metadata for a record that has been acknowledged by the server.
///
/// Corresponds to Java's `org.apache.kafka.clients.producer.RecordMetadata`.
#[derive(Clone, Debug)]
pub struct RecordMetadata {
    /// The offset of the record in the topic/partition.
    offset: i64,
    /// The timestamp of the message.
    ///
    /// If `LogAppendTime` is used for the topic, the timestamp will be the
    /// timestamp returned by the broker. If `CreateTime` is used for the topic,
    /// the timestamp is the timestamp in the corresponding `ProducerRecord` if
    /// the user provided one. Otherwise, it will be the producer local time
    /// when the producer record was handed to the producer.
    timestamp: i64,
    /// The size of the serialized, uncompressed key in bytes. -1 if key is null.
    serialized_key_size: i32,
    /// The size of the serialized, uncompressed value in bytes. -1 if value is null.
    serialized_value_size: i32,
    /// The topic-partition the record was sent to.
    topic_partition: TopicPartition,
}

impl RecordMetadata {
    /// Creates a new `RecordMetadata`.
    ///
    /// The `offset` is computed as `base_offset + batch_index`, unless
    /// `base_offset` is -1 (unknown), in which case the offset remains -1.
    ///
    /// This matches the Java constructor:
    /// `RecordMetadata(TopicPartition, long baseOffset, int batchIndex, long timestamp,
    ///                  int serializedKeySize, int serializedValueSize)`
    pub fn new(
        topic_partition: TopicPartition,
        base_offset: i64,
        batch_index: i32,
        timestamp: i64,
        serialized_key_size: i32,
        serialized_value_size: i32,
    ) -> Self {
        // Ignore the batchIndex if the base offset is -1, since this indicates
        // the offset is unknown.
        let offset = if base_offset == -1 {
            base_offset
        } else {
            base_offset + i64::from(batch_index)
        };
        Self { offset, timestamp, serialized_key_size, serialized_value_size, topic_partition }
    }

    /// Indicates whether the record metadata includes the offset.
    ///
    /// Returns `true` if the offset is included in the metadata, `false`
    /// otherwise.
    pub fn has_offset(&self) -> bool {
        self.offset != INVALID_OFFSET
    }

    /// The offset of the record in the topic/partition.
    ///
    /// Returns -1 if [`has_offset()`](Self::has_offset) returns `false`.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Indicates whether the record metadata includes the timestamp.
    ///
    /// Returns `true` if a valid timestamp exists, `false` otherwise.
    pub fn has_timestamp(&self) -> bool {
        self.timestamp != NO_TIMESTAMP
    }

    /// The timestamp of the record in the topic/partition.
    ///
    /// Returns -1 if [`has_timestamp()`](Self::has_timestamp) returns `false`.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// The size of the serialized, uncompressed key in bytes.
    ///
    /// If key is null, the returned size is -1.
    pub fn serialized_key_size(&self) -> i32 {
        self.serialized_key_size
    }

    /// The size of the serialized, uncompressed value in bytes.
    ///
    /// If value is null, the returned size is -1.
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
}

impl fmt::Display for RecordMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.topic_partition, self.offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Translated from RecordMetadataTest.java
    // -----------------------------------------------------------------------

    /// Translated from `RecordMetadataTest.testConstructionWithMissingBatchIndex`.
    #[test]
    fn test_construction_with_missing_batch_index() {
        let tp = TopicPartition::new("foo".to_string(), 0);
        let timestamp = 2_340_234_i64;
        let key_size = 3;
        let value_size = 5;

        let metadata = RecordMetadata::new(tp.clone(), -1, -1, timestamp, key_size, value_size);
        assert_eq!(metadata.topic(), tp.topic());
        assert_eq!(metadata.partition(), tp.partition());
        assert_eq!(metadata.timestamp(), timestamp);
        assert!(!metadata.has_offset());
        assert_eq!(metadata.offset(), -1);
        assert_eq!(metadata.serialized_key_size(), key_size);
        assert_eq!(metadata.serialized_value_size(), value_size);
    }

    /// Translated from `RecordMetadataTest.testConstructionWithBatchIndexOffset`.
    #[test]
    fn test_construction_with_batch_index_offset() {
        let tp = TopicPartition::new("foo".to_string(), 0);
        let timestamp = 2_340_234_i64;
        let key_size = 3;
        let value_size = 5;
        let base_offset = 15_i64;
        let batch_index = 3;

        let metadata = RecordMetadata::new(tp.clone(), base_offset, batch_index, timestamp, key_size, value_size);
        assert_eq!(metadata.topic(), tp.topic());
        assert_eq!(metadata.partition(), tp.partition());
        assert_eq!(metadata.timestamp(), timestamp);
        assert_eq!(metadata.offset(), base_offset + i64::from(batch_index));
        assert_eq!(metadata.serialized_key_size(), key_size);
        assert_eq!(metadata.serialized_value_size(), value_size);
    }

    // -----------------------------------------------------------------------
    // Additional unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_display() {
        let tp = TopicPartition::new("my-topic".to_string(), 3);
        let metadata = RecordMetadata::new(tp, 10, 2, 1000, 4, 8);
        assert_eq!(metadata.to_string(), "my-topic-3@12");
    }

    #[test]
    fn test_has_timestamp() {
        let tp = TopicPartition::new("t".to_string(), 0);

        let with_ts = RecordMetadata::new(tp.clone(), 0, 0, 1000, 0, 0);
        assert!(with_ts.has_timestamp());

        let no_ts = RecordMetadata::new(tp, 0, 0, NO_TIMESTAMP, 0, 0);
        assert!(!no_ts.has_timestamp());
    }

    #[test]
    fn test_has_offset() {
        let tp = TopicPartition::new("t".to_string(), 0);

        let with_offset = RecordMetadata::new(tp.clone(), 5, 0, 0, 0, 0);
        assert!(with_offset.has_offset());

        let no_offset = RecordMetadata::new(tp, -1, 0, 0, 0, 0);
        assert!(!no_offset.has_offset());
    }

    #[test]
    fn test_unknown_partition_constant() {
        assert_eq!(UNKNOWN_PARTITION, -1);
    }
}
