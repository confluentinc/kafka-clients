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

use crate::common::Error;
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::common::LocalIllegalArgumentError;
use crate::common::header::RecordHeaders;

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

/// The parameters of Java's widest `ProducerRecord` constructor
/// (`ProducerRecord(String, Integer, Long, K, V, Iterable<Header>)`,
/// `ProducerRecord.java:69`).
///
/// This struct has **no Java counterpart** (DoD #7). It exists solely to
/// satisfy CLAUDE.md §2's cap on derived overload names: that constructor
/// differs from the group's intersection `{topic, value}` by four parameters
/// — `partition`, `timestamp`, `key`, `headers` — so the cap fires and this
/// struct becomes the method's *only* parameter, carrying every Java
/// parameter including the intersection's own.
///
/// It deliberately has **no** `Default`. `topic` and `value` are what even
/// Java's narrowest constructor (`:142`) takes from its caller, so neither has
/// a Java-derived default — and a synthesised empty topic would silently send
/// the record nowhere. Construct it with [`ProducerRecordOptionsBuilder::new`]
/// and set them: [`ProducerRecordOptionsBuilder::build`] returns an error if any of `topic`, `value` was not set.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ProducerRecordOptions<K, V> {
    /// The topic the record will be appended to. Java's `topic`.
    pub topic: String,
    /// The partition to which the record should be sent. Java's `partition`.
    /// Starts as `None`, as in `:142`.
    pub partition: Option<i32>,
    /// The timestamp of the record, in milliseconds since epoch. If `None`,
    /// the producer will assign the timestamp using the system clock. Java's
    /// `timestamp`; starts as `None`, as in `:142`.
    pub timestamp: Option<i64>,
    /// The key that will be included in the record. Java's `key`. Starts as
    /// `None`, as in `:142`.
    pub key: Option<K>,
    /// The record contents. Java's `value`.
    pub value: Option<V>,
    /// The headers that will be included in the record. Java's `headers`.
    /// Starts as `None`, as in `:142`.
    pub headers: Option<RecordHeaders>,
}

/// Fluent builder for [`ProducerRecordOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — returning
/// [`Error::LocalIllegalArgument`] if they were not set. Like [`ProducerRecordOptions`] it has no Java counterpart and
/// exists solely to satisfy that naming rule (DoD #7).
pub struct ProducerRecordOptionsBuilder<K, V> {
    topic: Option<String>,
    partition: Option<i32>,
    timestamp: Option<i64>,
    key: Option<K>,
    value: Option<Option<V>>,
    headers: Option<RecordHeaders>,
}

impl<K, V> Default for ProducerRecordOptionsBuilder<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ProducerRecordOptionsBuilder<K, V> {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    pub fn new() -> Self {
        Self {
            topic: None,
            partition: None,
            timestamp: None,
            key: None,
            value: None,
            headers: None,
        }
    }

    /// Sets [`ProducerRecordOptions::topic`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_topic(mut self, topic: String) -> Self {
        self.topic = Some(topic);
        self
    }
    /// Sets [`ProducerRecordOptions::partition`].
    pub fn set_partition(mut self, partition: Option<i32>) -> Self {
        self.partition = partition;
        self
    }
    /// Sets [`ProducerRecordOptions::timestamp`].
    pub fn set_timestamp(mut self, timestamp: Option<i64>) -> Self {
        self.timestamp = timestamp;
        self
    }
    /// Sets [`ProducerRecordOptions::key`].
    pub fn set_key(mut self, key: Option<K>) -> Self {
        self.key = key;
        self
    }
    /// Sets [`ProducerRecordOptions::value`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_value(mut self, value: Option<V>) -> Self {
        self.value = Some(value);
        self
    }
    /// Sets [`ProducerRecordOptions::headers`].
    pub fn set_headers(mut self, headers: Option<RecordHeaders>) -> Self {
        self.headers = headers;
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor, so a later Java version that makes one of
    /// them optional changes the set this accepts instead of adding a second
    /// constructor. Today there is one mandatory set: `topic`, `value`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter of that
    /// set which was not given a setter call. Only presence is checked here;
    /// semantic validation belongs to the method the options are passed to
    /// (CLAUDE.md §2).
    pub fn build(self) -> Result<ProducerRecordOptions<K, V>, Error> {
        Ok(ProducerRecordOptions {
            topic: self.topic.ok_or_else(|| Self::missing("topic"))?,
            partition: self.partition,
            timestamp: self.timestamp,
            key: self.key,
            value: self.value.ok_or_else(|| Self::missing("value"))?,
            headers: self.headers,
        })
    }

    /// Builds the [`Error::LocalIllegalArgument`] naming a mandatory parameter
    /// [`Self::build`] found unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "ProducerRecordOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

impl<K, V> ProducerRecord<K, V> {
    /// Creates a record with a specified timestamp to be sent to a specified topic and
    /// partition.
    ///
    /// # Arguments
    ///
    /// * `options` - Every parameter of Java's widest constructor: topic,
    ///   partition, timestamp, key, value and headers
    ///
    /// Corresponds to Java's `ProducerRecord(String, Integer, Long, K, V, Iterable<Header>)`
    /// (`ProducerRecord.java:69`). Its six parameters exceed CLAUDE.md §2's
    /// three-parameter cap on derived overload names, so
    /// [`ProducerRecordOptions`] is this method's only parameter.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The timestamp is negative
    /// - The partition is negative
    pub fn with_options(options: ProducerRecordOptions<K, V>) -> Result<Self, LocalIllegalArgumentError> {
        let ProducerRecordOptions { topic, partition, timestamp, key, value, headers } = options;
        if let Some(ts) = timestamp
            && ts < 0
        {
            return Err(LocalIllegalArgumentError::new(format!(
                "Invalid timestamp: {ts}. Timestamp should always be non-negative or null."
            )));
        }
        if let Some(p) = partition
            && p < 0
        {
            return Err(LocalIllegalArgumentError::new(format!(
                "Invalid partition: {p}. Partition number should always be non-negative or null."
            )));
        }
        Ok(Self { topic, partition, key, value, timestamp, headers: headers.unwrap_or_default() })
    }

    /// Creates a record with a specified timestamp to be sent to a specified topic and
    /// partition (without headers).
    ///
    /// Corresponds to Java's `ProducerRecord(String, Integer, Long, K, V)`
    /// (`ProducerRecord.java:96`).
    ///
    /// # Errors
    ///
    /// Returns an error if the timestamp is negative or the partition is negative.
    pub fn with_partition_timestamp_key(
        topic: String,
        partition: Option<i32>,
        timestamp: Option<i64>,
        key: Option<K>,
        value: Option<V>,
    ) -> Result<Self, LocalIllegalArgumentError> {
        Self::with_options(
            ProducerRecordOptionsBuilder::new()
                .set_topic(topic)
                .set_value(value)
                .set_partition(partition)
                .set_timestamp(timestamp)
                .set_key(key)
                .build()
                .expect("ProducerRecordOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Creates a record to be sent to a specified topic and partition (with headers,
    /// no timestamp).
    ///
    /// Corresponds to Java's `ProducerRecord(String, Integer, K, V, Iterable<Header>)`
    /// (`ProducerRecord.java:109`).
    ///
    /// # Errors
    ///
    /// Returns an error if the partition is negative.
    pub fn with_partition_key_headers(
        topic: String,
        partition: Option<i32>,
        key: Option<K>,
        value: Option<V>,
        headers: RecordHeaders,
    ) -> Result<Self, LocalIllegalArgumentError> {
        Self::with_options(
            ProducerRecordOptionsBuilder::new()
                .set_topic(topic)
                .set_value(value)
                .set_partition(partition)
                .set_key(key)
                .set_headers(Some(headers))
                .build()
                .expect("ProducerRecordOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Creates a record to be sent to a specified topic and partition.
    ///
    /// Corresponds to Java's `ProducerRecord(String, Integer, K, V)`
    /// (`ProducerRecord.java:121`).
    ///
    /// # Errors
    ///
    /// Returns an error if the partition is negative.
    pub fn with_partition_key(
        topic: String,
        partition: Option<i32>,
        key: Option<K>,
        value: Option<V>,
    ) -> Result<Self, LocalIllegalArgumentError> {
        Self::with_options(
            ProducerRecordOptionsBuilder::new()
                .set_topic(topic)
                .set_value(value)
                .set_partition(partition)
                .set_key(key)
                .build()
                .expect("ProducerRecordOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Creates a record to be sent to Kafka with a key and value (no partition, no
    /// timestamp, no headers).
    ///
    /// Corresponds to Java's `ProducerRecord(String, K, V)` (`ProducerRecord.java:132`).
    pub fn with_key(topic: String, key: Option<K>, value: Option<V>) -> Self {
        // Cannot fail: no partition, no timestamp to validate
        Self::with_options(
            ProducerRecordOptionsBuilder::new()
                .set_topic(topic)
                .set_value(value)
                .set_key(key)
                .build()
                .expect("ProducerRecordOptionsBuilder::build: every mandatory parameter is set above"),
        )
        .unwrap()
    }

    /// Creates a record with no key (no partition, no timestamp, no headers).
    ///
    /// Corresponds to Java's `ProducerRecord(String, V)` (`ProducerRecord.java:142`),
    /// whose parameters `{topic, value}` are the intersection across the six
    /// constructors — so it owns the plain name (CLAUDE.md §2).
    pub fn new(topic: String, value: Option<V>) -> Self {
        // Cannot fail: no partition, no timestamp to validate
        Self::with_options(
            ProducerRecordOptionsBuilder::new()
                .set_topic(topic)
                .set_value(value)
                .build()
                .expect("ProducerRecordOptionsBuilder::build: every mandatory parameter is set above"),
        )
        .unwrap()
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

    /// Consume this record and return its parts.
    pub fn into_parts(self) -> (String, Option<i32>, Option<i64>, RecordHeaders, Option<K>, Option<V>) {
        (self.topic, self.partition, self.timestamp, self.headers, self.key, self.value)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `ProducerRecordTest.testEqualsAndHashCode`.
    #[test]
    fn test_equals_and_hash_code() {
        let producer_record =
            ProducerRecord::with_partition_key("test".to_string(), Some(1), Some("key".to_string()), Some(1)).unwrap();
        assert_eq!(producer_record, producer_record.clone());

        let equal_record =
            ProducerRecord::with_partition_key("test".to_string(), Some(1), Some("key".to_string()), Some(1)).unwrap();
        assert_eq!(producer_record, equal_record);
        assert_eq!(hash_of(&producer_record), hash_of(&equal_record));

        let topic_mismatch =
            ProducerRecord::with_partition_key("test-1".to_string(), Some(1), Some("key".to_string()), Some(1))
                .unwrap();
        assert_ne!(producer_record, topic_mismatch);

        let partition_mismatch =
            ProducerRecord::with_partition_key("test".to_string(), Some(2), Some("key".to_string()), Some(1)).unwrap();
        assert_ne!(producer_record, partition_mismatch);

        let key_mismatch =
            ProducerRecord::with_partition_key("test".to_string(), Some(1), Some("key-1".to_string()), Some(1))
                .unwrap();
        assert_ne!(producer_record, key_mismatch);

        let value_mismatch =
            ProducerRecord::with_partition_key("test".to_string(), Some(1), Some("key".to_string()), Some(2)).unwrap();
        assert_ne!(producer_record, value_mismatch);

        let null_fields_record: ProducerRecord<String, String> = ProducerRecord::with_options(
            ProducerRecordOptionsBuilder::new()
                .set_topic("topic".to_string())
                .set_value(None)
                .build()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(null_fields_record, null_fields_record.clone());
        assert_eq!(hash_of(&null_fields_record), hash_of(&null_fields_record));
    }

    /// Translated from `ProducerRecordTest.testInvalidRecords`.
    #[test]
    fn test_invalid_records() {
        // Negative timestamp
        let result = ProducerRecord::with_partition_timestamp_key(
            "test".to_string(),
            Some(0),
            Some(-1),
            Some("key".to_string()),
            Some(1),
        );
        assert!(result.is_err(), "Expected error because of negative timestamp");

        // Negative partition
        let result = ProducerRecord::with_partition_key("test".to_string(), Some(-1), Some("key".to_string()), Some(1));
        assert!(result.is_err(), "Expected error because of negative partition");
    }

    fn hash_of<T: Hash>(value: &T) -> u64 {
        use std::hash::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// CLAUDE.md §2: the mandatory parameters are validated in
    /// [`ProducerRecordOptionsBuilder::build`], not named in the constructor, so a
    /// builder left untouched panics naming the first one it finds unset.
    #[test]
    fn producer_record_options_builder_build_errors_when_no_mandatory_parameter_is_set() {
        let Err(error) = ProducerRecordOptionsBuilder::<String, String>::new().build() else {
            panic!("build must reject the unset mandatory parameter");
        };
        assert!(matches!(error, Error::LocalIllegalArgument(_)), "{error:?}");
        assert_eq!(
            error.message(),
            "ProducerRecordOptionsBuilder::build: mandatory parameter `topic` was not set"
        );
    }

    /// Validation covers every mandatory parameter, not just the first: setting
    /// all but one still panics, naming the one left unset.
    #[test]
    fn producer_record_options_builder_build_errors_when_only_value_is_unset() {
        let Err(error) = ProducerRecordOptionsBuilder::<String, String>::new()
            .set_topic("topic".to_string())
            .build()
        else {
            panic!("build must reject the unset mandatory parameter");
        };
        assert!(matches!(error, Error::LocalIllegalArgument(_)), "{error:?}");
        assert_eq!(
            error.message(),
            "ProducerRecordOptionsBuilder::build: mandatory parameter `value` was not set"
        );
    }
}
