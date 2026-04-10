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

//! Producer record and metadata types.
//!
//! Corresponds to org.apache.kafka.clients.producer.ProducerRecord
//! and org.apache.kafka.clients.producer.RecordMetadata.

/// A key-value pair to be sent to Kafka.
///
/// The key, value, and header values are borrowed to avoid unnecessary copies.
/// The accumulator copies them into the batch buffer exactly once during `append()`.
#[derive(Debug)]
pub struct ProducerRecord<'a> {
    topic: &'a str,
    partition: Option<i32>,
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
    timestamp: Option<i64>,
    headers: Vec<Header<'a>>,
}

/// A single header attached to a producer record.
#[derive(Debug, Clone)]
pub struct Header<'a> {
    key: &'a str,
    value: Option<&'a [u8]>,
}

impl<'a> Header<'a> {
    /// Create a new header.
    pub fn new(key: &'a str, value: Option<&'a [u8]>) -> Self {
        Header { key, value }
    }

    /// Returns the header key.
    pub fn key(&self) -> &str {
        self.key
    }

    /// Returns the header value, if present.
    pub fn value(&self) -> Option<&[u8]> {
        self.value
    }
}

impl<'a> ProducerRecord<'a> {
    /// Create a new record for the given topic.
    pub fn new(topic: &'a str) -> Self {
        ProducerRecord {
            topic,
            partition: None,
            key: None,
            value: None,
            timestamp: None,
            headers: Vec::new(),
        }
    }

    /// Set the target partition.
    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Set the record key.
    pub fn key(mut self, key: &'a [u8]) -> Self {
        self.key = Some(key);
        self
    }

    /// Set the record value.
    pub fn value(mut self, value: &'a [u8]) -> Self {
        self.value = Some(value);
        self
    }

    /// Set the record timestamp.
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Add a header to the record.
    pub fn header(mut self, key: &'a str, value: Option<&'a [u8]>) -> Self {
        self.headers.push(Header::new(key, value));
        self
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        self.topic
    }

    /// Returns the partition hint, if set by the user.
    pub fn partition_hint(&self) -> Option<i32> {
        self.partition
    }

    /// Returns the key bytes, if set.
    pub fn key_bytes(&self) -> Option<&[u8]> {
        self.key
    }

    /// Returns the value bytes, if set.
    pub fn value_bytes(&self) -> Option<&[u8]> {
        self.value
    }

    /// Returns the timestamp, if set.
    pub fn timestamp_value(&self) -> Option<i64> {
        self.timestamp
    }

    /// Returns the headers.
    pub fn headers(&self) -> &[Header<'a>] {
        &self.headers
    }

    /// Estimated serialized size in bytes for memory accounting.
    pub fn estimated_size(&self) -> usize {
        let key_size = self.key.map_or(0, |k| k.len());
        let value_size = self.value.map_or(0, |v| v.len());
        let headers_size: usize = self.headers.iter().map(|h| h.key.len() + h.value.map_or(0, |v| v.len())).sum();
        // 64 bytes overhead for record framing, timestamps, offsets, etc.
        key_size + value_size + headers_size + self.topic.len() + 64
    }
}

/// Metadata about a record that has been acknowledged by the server.
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp: i64,
    serialized_key_size: i32,
    serialized_value_size: i32,
}

impl RecordMetadata {
    /// Create new record metadata.
    pub fn new(
        topic: String,
        partition: i32,
        offset: i64,
        timestamp: i64,
        serialized_key_size: i32,
        serialized_value_size: i32,
    ) -> Self {
        RecordMetadata { topic, partition, offset, timestamp, serialized_key_size, serialized_value_size }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the offset assigned by the broker.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the timestamp (broker-assigned or record-provided).
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Returns the serialized key size in bytes, or -1 if the key was null.
    pub fn serialized_key_size(&self) -> i32 {
        self.serialized_key_size
    }

    /// Returns the serialized value size in bytes, or -1 if the value was null.
    pub fn serialized_value_size(&self) -> i32 {
        self.serialized_value_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_record_builder() {
        let key = b"my-key";
        let value = b"my-value";
        let record = ProducerRecord::new("my-topic")
            .partition(3)
            .key(key)
            .value(value)
            .timestamp(1234567890)
            .header("trace-id", Some(b"abc123"));

        assert_eq!(record.topic(), "my-topic");
        assert_eq!(record.partition_hint(), Some(3));
        assert_eq!(record.key_bytes(), Some(b"my-key".as_slice()));
        assert_eq!(record.value_bytes(), Some(b"my-value".as_slice()));
        assert_eq!(record.timestamp_value(), Some(1234567890));
        assert_eq!(record.headers().len(), 1);
        assert_eq!(record.headers()[0].key(), "trace-id");
    }

    #[test]
    fn test_producer_record_minimal() {
        let record = ProducerRecord::new("topic");
        assert_eq!(record.topic(), "topic");
        assert_eq!(record.partition_hint(), None);
        assert_eq!(record.key_bytes(), None);
        assert_eq!(record.value_bytes(), None);
    }

    #[test]
    fn test_estimated_size() {
        let record = ProducerRecord::new("topic").key(b"key").value(b"value");
        let size = record.estimated_size();
        // 3 (key) + 5 (value) + 5 (topic) + 64 (overhead)
        assert_eq!(size, 77);
    }

    #[test]
    fn test_record_metadata() {
        let meta = RecordMetadata::new("topic".to_string(), 0, 42, 1000, 3, 5);
        assert_eq!(meta.topic(), "topic");
        assert_eq!(meta.partition(), 0);
        assert_eq!(meta.offset(), 42);
        assert_eq!(meta.timestamp(), 1000);
        assert_eq!(meta.serialized_key_size(), 3);
        assert_eq!(meta.serialized_value_size(), 5);
    }
}
