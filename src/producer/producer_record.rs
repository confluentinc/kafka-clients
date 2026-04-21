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

//! A key/value pair to be sent to Kafka.
//!
//! Translated from `org.apache.kafka.clients.producer.ProducerRecord`.

use std::fmt;
use std::hash::{Hash, Hasher};

use crate::common::header::internals::RecordHeaders;

/// A key/value pair to be sent to Kafka. This consists of a topic name to which the record
/// is being sent, an optional partition number, and an optional key and value.
///
/// If a valid partition number is specified that partition will be used when sending the
/// record. If no partition is specified but a key is present a partition will be chosen
/// using a hash of the key. If neither key nor partition is present a partition will be
/// assigned in a round-robin fashion. Note that partition numbers are 0-indexed.
///
/// The record also has an associated timestamp. If the user did not provide a timestamp,
/// the producer will stamp the record with its current time. The timestamp eventually
/// used by Kafka depends on the timestamp type configured for the topic.
///
/// - If the topic is configured to use `CreateTime`, the timestamp in the producer record
///   will be used by the broker.
/// - If the topic is configured to use `LogAppendTime`, the timestamp in the producer
///   record will be overwritten by the broker with the broker local time when it appends
///   the message to its log.
///
/// In either case, the timestamp that has actually been used will be returned to the user
/// in [`RecordMetadata`](super::RecordMetadata).
#[derive(Clone, Debug)]
pub struct ProducerRecord<K, V> {
    topic: String,
    partition: Option<i32>,
    headers: RecordHeaders,
    key: Option<K>,
    value: Option<V>,
    timestamp: Option<i64>,
}

impl<K, V> ProducerRecord<K, V> {
    /// Creates a record with a specified timestamp to be sent to a specified topic and
    /// partition.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic the record will be appended to
    /// * `partition` - The partition to which the record should be sent
    /// * `timestamp` - The timestamp of the record, in milliseconds since epoch. If `None`,
    ///   the producer will assign the timestamp using the system clock.
    /// * `key` - The key that will be included in the record
    /// * `value` - The record contents
    /// * `headers` - The headers that will be included in the record
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The timestamp is negative
    /// - The partition is negative
    pub fn new(
        topic: String,
        partition: Option<i32>,
        timestamp: Option<i64>,
        key: Option<K>,
        value: Option<V>,
        headers: Option<RecordHeaders>,
    ) -> Result<Self, IllegalArgumentError> {
        if let Some(ts) = timestamp
            && ts < 0
        {
            return Err(IllegalArgumentError::new(format!(
                "Invalid timestamp: {ts}. Timestamp should always be non-negative or null."
            )));
        }
        if let Some(p) = partition
            && p < 0
        {
            return Err(IllegalArgumentError::new(format!(
                "Invalid partition: {p}. Partition number should always be non-negative or null."
            )));
        }
        Ok(Self { topic, partition, key, value, timestamp, headers: headers.unwrap_or_default() })
    }

    /// Creates a record with a specified timestamp to be sent to a specified topic and
    /// partition (without headers).
    ///
    /// # Errors
    ///
    /// Returns an error if the timestamp is negative or the partition is negative.
    pub fn with_timestamp(
        topic: String,
        partition: Option<i32>,
        timestamp: Option<i64>,
        key: Option<K>,
        value: Option<V>,
    ) -> Result<Self, IllegalArgumentError> {
        Self::new(topic, partition, timestamp, key, value, None)
    }

    /// Creates a record to be sent to a specified topic and partition (with headers,
    /// no timestamp).
    ///
    /// # Errors
    ///
    /// Returns an error if the partition is negative.
    pub fn with_headers(
        topic: String,
        partition: Option<i32>,
        key: Option<K>,
        value: Option<V>,
        headers: RecordHeaders,
    ) -> Result<Self, IllegalArgumentError> {
        Self::new(topic, partition, None, key, value, Some(headers))
    }

    /// Creates a record to be sent to a specified topic and partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition is negative.
    pub fn with_partition(
        topic: String,
        partition: Option<i32>,
        key: Option<K>,
        value: Option<V>,
    ) -> Result<Self, IllegalArgumentError> {
        Self::new(topic, partition, None, key, value, None)
    }

    /// Creates a record to be sent to Kafka with a key and value (no partition, no
    /// timestamp, no headers).
    pub fn with_key(topic: String, key: Option<K>, value: Option<V>) -> Self {
        // Cannot fail: no partition, no timestamp to validate
        Self::new(topic, None, None, key, value, None).unwrap()
    }

    /// Creates a record with no key (no partition, no timestamp, no headers).
    pub fn with_value(topic: String, value: Option<V>) -> Self {
        // Cannot fail: no partition, no timestamp to validate
        Self::new(topic, None, None, None, value, None).unwrap()
    }

    /// Returns the topic this record is being sent to.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the headers.
    pub fn headers(&self) -> &RecordHeaders {
        &self.headers
    }

    /// Returns a mutable reference to the headers.
    pub fn headers_mut(&mut self) -> &mut RecordHeaders {
        &mut self.headers
    }

    /// Returns the key (or `None` if no key is specified).
    pub fn key(&self) -> Option<&K> {
        self.key.as_ref()
    }

    /// Returns the value.
    pub fn value(&self) -> Option<&V> {
        self.value.as_ref()
    }

    /// Returns the timestamp, which is in milliseconds since epoch.
    pub fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// Returns the partition to which the record will be sent (or `None` if no partition
    /// was specified).
    pub fn partition(&self) -> Option<i32> {
        self.partition
    }
}

impl<K: PartialEq, V: PartialEq> PartialEq for ProducerRecord<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.partition == other.partition
            && self.topic == other.topic
            && self.headers == other.headers
            && self.value == other.value
            && self.timestamp == other.timestamp
    }
}

impl<K: Eq, V: Eq> Eq for ProducerRecord<K, V> {}

impl<K: Hash, V: Hash> Hash for ProducerRecord<K, V> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.partition.hash(state);
        self.headers.hash(state);
        self.key.hash(state);
        self.value.hash(state);
        self.timestamp.hash(state);
    }
}

impl<K: fmt::Debug, V: fmt::Debug> fmt::Display for ProducerRecord<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers = format!("{}", self.headers);
        let key = match &self.key {
            Some(k) => format!("{:?}", k),
            None => "null".to_string(),
        };
        let value = match &self.value {
            Some(v) => format!("{:?}", v),
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

/// Error returned when an argument is invalid (e.g. negative timestamp or partition).
///
/// Corresponds to Java's `IllegalArgumentException`.
#[derive(Clone, Debug)]
pub struct IllegalArgumentError {
    message: String,
}

impl IllegalArgumentError {
    /// Creates a new `IllegalArgumentError` with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for IllegalArgumentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for IllegalArgumentError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `ProducerRecordTest.testEqualsAndHashCode`.
    #[test]
    fn test_equals_and_hash_code() {
        let producer_record =
            ProducerRecord::with_partition("test".to_string(), Some(1), Some("key".to_string()), Some(1)).unwrap();
        assert_eq!(producer_record, producer_record.clone());

        let equal_record =
            ProducerRecord::with_partition("test".to_string(), Some(1), Some("key".to_string()), Some(1)).unwrap();
        assert_eq!(producer_record, equal_record);
        assert_eq!(hash_of(&producer_record), hash_of(&equal_record));

        let topic_mismatch =
            ProducerRecord::with_partition("test-1".to_string(), Some(1), Some("key".to_string()), Some(1)).unwrap();
        assert_ne!(producer_record, topic_mismatch);

        let partition_mismatch =
            ProducerRecord::with_partition("test".to_string(), Some(2), Some("key".to_string()), Some(1)).unwrap();
        assert_ne!(producer_record, partition_mismatch);

        let key_mismatch =
            ProducerRecord::with_partition("test".to_string(), Some(1), Some("key-1".to_string()), Some(1)).unwrap();
        assert_ne!(producer_record, key_mismatch);

        let value_mismatch =
            ProducerRecord::with_partition("test".to_string(), Some(1), Some("key".to_string()), Some(2)).unwrap();
        assert_ne!(producer_record, value_mismatch);

        let null_fields_record: ProducerRecord<String, String> =
            ProducerRecord::new("topic".to_string(), None, None, None, None, None).unwrap();
        assert_eq!(null_fields_record, null_fields_record.clone());
        assert_eq!(hash_of(&null_fields_record), hash_of(&null_fields_record));
    }

    /// Translated from `ProducerRecordTest.testInvalidRecords`.
    #[test]
    fn test_invalid_records() {
        // Negative timestamp
        let result =
            ProducerRecord::with_timestamp("test".to_string(), Some(0), Some(-1), Some("key".to_string()), Some(1));
        assert!(result.is_err(), "Expected error because of negative timestamp");

        // Negative partition
        let result = ProducerRecord::with_partition("test".to_string(), Some(-1), Some("key".to_string()), Some(1));
        assert!(result.is_err(), "Expected error because of negative partition");
    }

    fn hash_of<T: Hash>(value: &T) -> u64 {
        use std::hash::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }
}
