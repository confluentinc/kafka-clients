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

//! Producer record and header types.
//!
//! Corresponds to Java's `org.apache.kafka.clients.producer.ProducerRecord`
//! and `org.apache.kafka.common.header.Header` / `RecordHeader`.

use std::fmt;

use crate::common::kafka_error::KafkaError;

/// A key-value pair attached to a Kafka record.
///
/// Corresponds to Java's `org.apache.kafka.common.header.Header` interface
/// and `org.apache.kafka.common.header.internals.RecordHeader` implementation.
///
/// # Examples
///
/// ```
/// use confluent_kafka::clients::producer::Header;
///
/// let header = Header::new("trace-id", Some(b"abc123".to_vec())).unwrap();
/// assert_eq!(header.key(), "trace-id");
/// assert_eq!(header.value(), Some(b"abc123".as_slice()));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Header {
    key: String,
    value: Option<Vec<u8>>,
}

impl Header {
    /// Creates a new header with the given key and optional value.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if `key` is empty (matching Java's
    /// `Objects.requireNonNull` on header keys which throws
    /// `IllegalArgumentException`).
    pub fn new(key: impl Into<String>, value: Option<Vec<u8>>) -> Result<Self, KafkaError> {
        let key = key.into();
        if key.is_empty() {
            return Err(KafkaError::illegal_argument("Null header keys are not permitted"));
        }
        Ok(Self { key, value })
    }

    /// Returns the key of the header.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the value of the header, or `None` if the value is null.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordHeader(key = {}, value = {:?})", self.key, self.value.as_deref())
    }
}

/// A key/value pair to be sent to Kafka.
///
/// This consists of a topic name to which the record is being sent, an optional
/// partition number, and an optional key and value.
///
/// If a valid partition number is specified that partition will be used when
/// sending the record. If no partition is specified but a key is present a
/// partition will be chosen using a hash of the key. If neither key nor
/// partition is present a partition will be assigned in a round-robin fashion.
/// Note that partition numbers are 0-indexed.
///
/// The record also has an associated timestamp. If the user did not provide a
/// timestamp, the producer will stamp the record with its current time. The
/// timestamp eventually used by Kafka depends on the timestamp type configured
/// for the topic:
///
/// - If the topic is configured to use `CreateTime`, the timestamp in the
///   producer record will be used by the broker.
/// - If the topic is configured to use `LogAppendTime`, the timestamp in the
///   producer record will be overwritten by the broker with the broker local
///   time when it appends the message to its log.
///
/// In either of the cases above, the timestamp that has actually been used
/// will be returned to user in [`RecordMetadata`](super::RecordMetadata).
///
/// Unlike Java's generic `ProducerRecord<K, V>`, this type is byte-oriented:
/// key and value are pre-serialized `Vec<u8>`.
///
/// # Builder-style construction
///
/// ```
/// use confluent_kafka::clients::producer::ProducerRecord;
///
/// let record = ProducerRecord::new("my-topic").unwrap()
///     .with_partition(0).unwrap()
///     .with_timestamp(1234567890).unwrap()
///     .with_key(b"my-key".to_vec())
///     .with_value(b"my-value".to_vec());
///
/// assert_eq!(record.topic(), "my-topic");
/// assert_eq!(record.partition(), Some(0));
/// ```
///
/// Corresponds to Java's `org.apache.kafka.clients.producer.ProducerRecord`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProducerRecord {
    topic: String,
    partition: Option<i32>,
    timestamp: Option<i64>,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<Header>,
}

impl ProducerRecord {
    /// Creates a new `ProducerRecord` with only the topic set.
    ///
    /// Use the builder methods (`with_partition`, `with_key`, etc.) to set
    /// additional fields.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if `topic` is empty (matching Java's
    /// `IllegalArgumentException` for null topic).
    pub fn new(topic: impl Into<String>) -> Result<Self, KafkaError> {
        let topic = topic.into();
        if topic.is_empty() {
            return Err(KafkaError::illegal_argument("Topic cannot be null."));
        }
        Ok(Self {
            topic,
            partition: None,
            timestamp: None,
            key: None,
            value: None,
            headers: Vec::new(),
        })
    }

    /// Creates a `ProducerRecord` with all fields set explicitly.
    ///
    /// This mirrors Java's most complete constructor:
    /// `ProducerRecord(topic, partition, timestamp, key, value, headers)`.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if:
    /// - `topic` is empty.
    /// - `partition` is `Some` and negative.
    /// - `timestamp` is `Some` and negative.
    pub fn with_all_fields(
        topic: impl Into<String>,
        partition: Option<i32>,
        timestamp: Option<i64>,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<Header>,
    ) -> Result<Self, KafkaError> {
        let topic = topic.into();
        if topic.is_empty() {
            return Err(KafkaError::illegal_argument("Topic cannot be null."));
        }
        if let Some(ts) = timestamp
            && ts < 0
        {
            return Err(KafkaError::illegal_argument(format!(
                "Invalid timestamp: {ts}. Timestamp should always be non-negative or null."
            )));
        }
        if let Some(p) = partition
            && p < 0
        {
            return Err(KafkaError::illegal_argument(format!(
                "Invalid partition: {p}. Partition number should always be non-negative or null."
            )));
        }
        Ok(Self { topic, partition, timestamp, key, value, headers })
    }

    // -- Builder methods (consume self, return Result<Self>) ------------------

    /// Sets the partition for this record.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if `partition` is negative.
    pub fn with_partition(mut self, partition: i32) -> Result<Self, KafkaError> {
        if partition < 0 {
            return Err(KafkaError::illegal_argument(format!(
                "Invalid partition: {partition}. Partition number should always be non-negative or null."
            )));
        }
        self.partition = Some(partition);
        Ok(self)
    }

    /// Sets the timestamp for this record, in milliseconds since epoch.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if `timestamp` is negative.
    pub fn with_timestamp(mut self, timestamp: i64) -> Result<Self, KafkaError> {
        if timestamp < 0 {
            return Err(KafkaError::illegal_argument(format!(
                "Invalid timestamp: {timestamp}. Timestamp should always be non-negative or null."
            )));
        }
        self.timestamp = Some(timestamp);
        Ok(self)
    }

    /// Sets the key for this record.
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Sets the value for this record.
    pub fn with_value(mut self, value: impl Into<Vec<u8>>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// Adds a header to this record by key and value.
    ///
    /// # Errors
    ///
    /// Returns `Err(KafkaError)` if the header key is empty.
    pub fn with_header(mut self, key: impl Into<String>, value: Option<Vec<u8>>) -> Result<Self, KafkaError> {
        self.headers.push(Header::new(key, value)?);
        Ok(self)
    }

    /// Adds a pre-constructed [`Header`] to this record.
    pub fn add_header(mut self, header: Header) -> Self {
        self.headers.push(header);
        self
    }

    // -- Getters (borrow self, return borrowed references) --------------------

    /// Returns the topic this record is being sent to.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition to which the record will be sent, or `None`
    /// if no partition was specified.
    pub fn partition(&self) -> Option<i32> {
        self.partition
    }

    /// Returns the timestamp in milliseconds since epoch, or `None` if
    /// not set by the user.
    pub fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// Returns the key, or `None` if no key is specified.
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Returns the value, or `None` if no value is specified.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Returns the headers attached to this record.
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }
}

impl fmt::Display for ProducerRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers = format!("{:?}", self.headers);
        let key = match &self.key {
            Some(k) => format!("{k:?}"),
            None => "null".to_string(),
        };
        let value = match &self.value {
            Some(v) => format!("{v:?}"),
            None => "null".to_string(),
        };
        let timestamp = match self.timestamp {
            Some(ts) => ts.to_string(),
            None => "null".to_string(),
        };
        write!(
            f,
            "ProducerRecord(topic={}, partition={:?}, headers={}, key={}, value={}, timestamp={})",
            self.topic, self.partition, headers, key, value, timestamp
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Translated from ProducerRecordTest.java
    // -----------------------------------------------------------------------

    /// Translated from `ProducerRecordTest.testEqualsAndHashCode`.
    #[test]
    fn test_equals_and_hash_code() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_of(record: &ProducerRecord) -> u64 {
            let mut hasher = DefaultHasher::new();
            record.hash(&mut hasher);
            hasher.finish()
        }

        // ProducerRecord("test", partition=1, key="key", value=1)
        let producer_record = ProducerRecord::with_all_fields(
            "test",
            Some(1),
            None,
            Some(b"key".to_vec()),
            Some(b"\x01".to_vec()),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(producer_record, producer_record.clone());
        assert_eq!(hash_of(&producer_record), hash_of(&producer_record));

        let equal_record = ProducerRecord::with_all_fields(
            "test",
            Some(1),
            None,
            Some(b"key".to_vec()),
            Some(b"\x01".to_vec()),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(producer_record, equal_record);
        assert_eq!(hash_of(&producer_record), hash_of(&equal_record));

        let topic_mismatch = ProducerRecord::with_all_fields(
            "test-1",
            Some(1),
            None,
            Some(b"key".to_vec()),
            Some(b"\x01".to_vec()),
            Vec::new(),
        )
        .unwrap();
        assert_ne!(producer_record, topic_mismatch);

        let partition_mismatch = ProducerRecord::with_all_fields(
            "test",
            Some(2),
            None,
            Some(b"key".to_vec()),
            Some(b"\x01".to_vec()),
            Vec::new(),
        )
        .unwrap();
        assert_ne!(producer_record, partition_mismatch);

        let key_mismatch = ProducerRecord::with_all_fields(
            "test",
            Some(1),
            None,
            Some(b"key-1".to_vec()),
            Some(b"\x01".to_vec()),
            Vec::new(),
        )
        .unwrap();
        assert_ne!(producer_record, key_mismatch);

        let value_mismatch = ProducerRecord::with_all_fields(
            "test",
            Some(1),
            None,
            Some(b"key".to_vec()),
            Some(b"\x02".to_vec()),
            Vec::new(),
        )
        .unwrap();
        assert_ne!(producer_record, value_mismatch);

        // Null fields record: topic only, everything else None/empty
        let null_fields_record = ProducerRecord::with_all_fields("topic", None, None, None, None, Vec::new()).unwrap();
        assert_eq!(null_fields_record, null_fields_record.clone());
        assert_eq!(hash_of(&null_fields_record), hash_of(&null_fields_record));
    }

    /// Translated from `ProducerRecordTest.testInvalidRecords`.
    #[test]
    fn test_invalid_records() {
        // Empty topic (Java: null topic)
        let result = ProducerRecord::with_all_fields(
            "",
            Some(0),
            None,
            Some(b"key".to_vec()),
            Some(b"\x01".to_vec()),
            Vec::new(),
        );
        assert!(result.is_err(), "Expected error because topic is empty");
        assert!(result.unwrap_err().message().contains("Topic cannot be null"));

        // Negative timestamp
        let result = ProducerRecord::with_all_fields(
            "test",
            Some(0),
            Some(-1),
            Some(b"key".to_vec()),
            Some(b"\x01".to_vec()),
            Vec::new(),
        );
        assert!(result.is_err(), "Expected error because of negative timestamp");
        assert!(result.unwrap_err().message().contains("Invalid timestamp"));

        // Negative partition
        let result = ProducerRecord::with_all_fields(
            "test",
            Some(-1),
            None,
            Some(b"key".to_vec()),
            Some(b"\x01".to_vec()),
            Vec::new(),
        );
        assert!(result.is_err(), "Expected error because of negative partition");
        assert!(result.unwrap_err().message().contains("Invalid partition"));
    }

    // -----------------------------------------------------------------------
    // Additional unit tests for builder pattern and getters
    // -----------------------------------------------------------------------

    #[test]
    fn test_builder_minimal() {
        let record = ProducerRecord::new("my-topic").unwrap();
        assert_eq!(record.topic(), "my-topic");
        assert_eq!(record.partition(), None);
        assert_eq!(record.timestamp(), None);
        assert_eq!(record.key(), None);
        assert_eq!(record.value(), None);
        assert!(record.headers().is_empty());
    }

    #[test]
    fn test_builder_full() {
        let record = ProducerRecord::new("topic")
            .unwrap()
            .with_partition(3)
            .unwrap()
            .with_timestamp(1000)
            .unwrap()
            .with_key(b"k".to_vec())
            .with_value(b"v".to_vec())
            .with_header("h1", Some(b"hv1".to_vec()))
            .unwrap();

        assert_eq!(record.topic(), "topic");
        assert_eq!(record.partition(), Some(3));
        assert_eq!(record.timestamp(), Some(1000));
        assert_eq!(record.key(), Some(b"k".as_slice()));
        assert_eq!(record.value(), Some(b"v".as_slice()));
        assert_eq!(record.headers().len(), 1);
        assert_eq!(record.headers()[0].key(), "h1");
        assert_eq!(record.headers()[0].value(), Some(b"hv1".as_slice()));
    }

    #[test]
    fn test_new_empty_topic_returns_error() {
        let result = ProducerRecord::new("");
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("Topic cannot be null"));
    }

    #[test]
    fn test_builder_negative_partition_returns_error() {
        let result = ProducerRecord::new("topic").unwrap().with_partition(-1);
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("Invalid partition"));
    }

    #[test]
    fn test_builder_negative_timestamp_returns_error() {
        let result = ProducerRecord::new("topic").unwrap().with_timestamp(-1);
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("Invalid timestamp"));
    }

    #[test]
    fn test_display() {
        let record = ProducerRecord::new("test").unwrap().with_key(b"key".to_vec());
        let display = format!("{record}");
        assert!(display.contains("topic=test"));
        assert!(display.contains("key="));
    }

    #[test]
    fn test_header_display() {
        let header = Header::new("trace-id", Some(b"abc".to_vec())).unwrap();
        let display = format!("{header}");
        assert!(display.contains("trace-id"));
    }

    #[test]
    fn test_header_empty_key_returns_error() {
        let result = Header::new("", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("Null header keys are not permitted"));
    }

    #[test]
    fn test_header_null_value() {
        let header = Header::new("key", None).unwrap();
        assert_eq!(header.key(), "key");
        assert_eq!(header.value(), None);
    }

    #[test]
    fn test_clone() {
        let record = ProducerRecord::new("topic")
            .unwrap()
            .with_partition(1)
            .unwrap()
            .with_key(b"k".to_vec())
            .with_value(b"v".to_vec());
        let cloned = record.clone();
        assert_eq!(record, cloned);
    }
}
