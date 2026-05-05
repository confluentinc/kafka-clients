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

//! Translation of `org.apache.kafka.clients.producer.RecordMetadata`.

use std::fmt;

use crate::common::record::record_batch::NO_TIMESTAMP;
use crate::common::requests::produce_response::ProduceResponse;
use crate::common::topic_partition::TopicPartition;

/// The metadata for a record that has been acknowledged by the server.
///
/// Returned to user-supplied [`Callback`](crate::producer::Callback)
/// implementations and as the `Output` of a successful
/// [`FutureRecordMetadata`](crate::producer::internals::FutureRecordMetadata).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    offset: i64,
    /// The timestamp of the message.
    /// If `LogAppendTime` is used for the topic, the timestamp will be the
    /// timestamp returned by the broker. If `CreateTime` is used, the
    /// timestamp is the one in the corresponding `ProducerRecord` if the
    /// user provided one, otherwise the producer-local time when the
    /// record was handed to the producer.
    timestamp: i64,
    serialized_key_size: i32,
    serialized_value_size: i32,
    topic_partition: TopicPartition,
}

impl RecordMetadata {
    /// Partition value used when no partition has been chosen.
    /// Mirrors `RecordMetadata.UNKNOWN_PARTITION`.
    pub const UNKNOWN_PARTITION: i32 = -1;

    /// Create a new instance with the provided parameters. Mirrors the
    /// 6-arg Java constructor.
    pub fn new(
        topic_partition: TopicPartition,
        base_offset: i64,
        batch_index: i32,
        timestamp: i64,
        serialized_key_size: i32,
        serialized_value_size: i32,
    ) -> Self {
        // ignore the batchIndex if the base offset is -1, since this
        // indicates the offset is unknown.
        let offset = if base_offset == -1 {
            base_offset
        } else {
            base_offset + batch_index as i64
        };
        RecordMetadata { offset, timestamp, serialized_key_size, serialized_value_size, topic_partition }
    }

    /// Indicates whether the record metadata includes the offset.
    pub fn has_offset(&self) -> bool {
        self.offset != ProduceResponse::INVALID_OFFSET
    }

    /// The offset of the record in the topic/partition, or -1 if
    /// [`RecordMetadata::has_offset`] returns false.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Indicates whether the record metadata includes the timestamp.
    pub fn has_timestamp(&self) -> bool {
        self.timestamp != NO_TIMESTAMP
    }

    /// The timestamp of the record in the topic/partition, or -1 if
    /// [`RecordMetadata::has_timestamp`] returns false.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// The size of the serialized, uncompressed key in bytes. If the key
    /// is null, the returned size is -1.
    pub fn serialized_key_size(&self) -> i32 {
        self.serialized_key_size
    }

    /// The size of the serialized, uncompressed value in bytes. If the
    /// value is null, the returned size is -1.
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

    /// The full topic-partition descriptor.
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }
}

impl fmt::Display for RecordMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Mirrors Java's `topicPartition.toString() + "@" + offset`.
        write!(f, "{}@{}", self.topic_partition, self.offset)
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `org.apache.kafka.clients.producer.RecordMetadataTest`.

    use super::*;

    /// Java: `RecordMetadataTest#testConstructionWithMissingBatchIndex`.
    #[test]
    fn test_construction_with_missing_batch_index() {
        let tp = TopicPartition::new("foo", 0);
        let timestamp: i64 = 2_340_234;
        let key_size: i32 = 3;
        let value_size: i32 = 5;

        let metadata = RecordMetadata::new(tp.clone(), -1, -1, timestamp, key_size, value_size);
        assert_eq!(tp.topic(), metadata.topic());
        assert_eq!(tp.partition(), metadata.partition());
        assert_eq!(timestamp, metadata.timestamp());
        assert!(!metadata.has_offset());
        assert_eq!(-1, metadata.offset());
        assert_eq!(key_size, metadata.serialized_key_size());
        assert_eq!(value_size, metadata.serialized_value_size());
    }

    /// Java: `RecordMetadataTest#testConstructionWithBatchIndexOffset`.
    #[test]
    fn test_construction_with_batch_index_offset() {
        let tp = TopicPartition::new("foo", 0);
        let timestamp: i64 = 2_340_234;
        let key_size: i32 = 3;
        let value_size: i32 = 5;
        let base_offset: i64 = 15;
        let batch_index: i32 = 3;

        let metadata = RecordMetadata::new(tp.clone(), base_offset, batch_index, timestamp, key_size, value_size);
        assert_eq!(tp.topic(), metadata.topic());
        assert_eq!(tp.partition(), metadata.partition());
        assert_eq!(timestamp, metadata.timestamp());
        assert_eq!(base_offset + batch_index as i64, metadata.offset());
        assert_eq!(key_size, metadata.serialized_key_size());
        assert_eq!(value_size, metadata.serialized_value_size());
    }
}
