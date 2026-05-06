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

//! Translation of `org.apache.kafka.clients.producer.ProducerRecord`.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::common::header::{RecordHeader, RecordHeaders};

/// Errors returned by [`ProducerRecord`] constructors. Mirrors Java's
/// `IllegalArgumentException` cases.
///
/// # Translation note
///
/// Java's `if (topic == null) throw new IllegalArgumentException("Topic
/// cannot be null.")` is enforced at the type level in Rust: the `topic`
/// parameter is `impl Into<Arc<str>>`, which has no `null` representation.
/// There is therefore no `NullTopic` variant — the only way for a caller
/// to express "no topic" in Java is to pass `null`, and that is a
/// compile-time error in Rust. Empty strings (`""`) are accepted at
/// construction (matching Java) and rejected later by metadata lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProducerRecordError {
    /// Mirrors `"Invalid timestamp: %d. Timestamp should always be non-negative or null."`.
    NegativeTimestamp(i64),
    /// Mirrors `"Invalid partition: %d. Partition number should always be non-negative or null."`.
    NegativePartition(i32),
}

impl fmt::Display for ProducerRecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProducerRecordError::NegativeTimestamp(ts) => {
                write!(f, "Invalid timestamp: {}. Timestamp should always be non-negative or null.", ts)
            },
            ProducerRecordError::NegativePartition(p) => write!(
                f,
                "Invalid partition: {}. Partition number should always be non-negative or null.",
                p
            ),
        }
    }
}

impl std::error::Error for ProducerRecordError {}

/// A key/value pair to be sent to Kafka. This consists of a topic name to
/// which the record is being sent, an optional partition number, and an
/// optional key and value.
///
/// If a valid partition number is specified that partition will be used when
/// sending the record. If no partition is specified but a key is present a
/// partition will be chosen using a hash of the key. If neither key nor
/// partition is present a partition will be assigned in a round-robin
/// fashion. Note that partition numbers are 0-indexed.
///
/// The record also has an associated timestamp. If the user did not provide a
/// timestamp, the producer will stamp the record with its current time. The
/// timestamp eventually used by Kafka depends on the timestamp type
/// configured for the topic.
///
/// # Translation notes
///
/// The Java class stores the topic as a `String`. We use `Arc<str>` per
/// CLAUDE.md rule 11 — every clone of a `ProducerRecord` (e.g. an
/// interceptor that returns a new record reusing the same topic) becomes a
/// refcount bump rather than a heap copy on the producer hot path.
///
/// Constructors return `Result<Self, ProducerRecordError>` because Java
/// throws `IllegalArgumentException` for negative timestamp or negative
/// partition. Java's "null topic" guard is enforced at the Rust type
/// level (the `impl Into<Arc<str>>` parameter has no `null`
/// representation), so there is no runtime check or error variant for
/// it. Empty-string topics are accepted at construction (matching
/// Java) and later rejected by the broker during metadata lookup. Per
/// CLAUDE.md rule 10, the remaining checks are surfaced as a
/// recoverable error rather than panicking.
pub struct ProducerRecord<K, V> {
    topic: Arc<str>,
    partition: Option<i32>,
    headers: RecordHeaders,
    key: Option<K>,
    value: Option<V>,
    timestamp: Option<i64>,
}

impl<K, V> ProducerRecord<K, V> {
    /// Creates a record with a specified timestamp to be sent to a specified
    /// topic and partition. Mirrors the 6-arg Java constructor.
    ///
    /// * `topic` — The topic the record will be appended to.
    /// * `partition` — The partition to which the record should be sent.
    /// * `timestamp` — The timestamp of the record, in milliseconds since
    ///   epoch. If `None`, the producer will assign the timestamp using the
    ///   current system time.
    /// * `key` — The key that will be included in the record.
    /// * `value` — The record contents.
    /// * `headers` — The headers that will be included in the record.
    pub fn with_full(
        topic: impl Into<Arc<str>>,
        partition: Option<i32>,
        timestamp: Option<i64>,
        key: Option<K>,
        value: Option<V>,
        headers: Option<Vec<RecordHeader>>,
    ) -> Result<Self, ProducerRecordError> {
        // Java's `if (topic == null) throw new IllegalArgumentException(...)`
        // is enforced at the type level here: `impl Into<Arc<str>>` has no
        // `null` representation. Empty strings (`""`) are accepted at
        // construction (matching Java's behavior) and rejected later by
        // metadata lookup on the broker side.
        let topic: Arc<str> = topic.into();
        if let Some(ts) = timestamp
            && ts < 0
        {
            return Err(ProducerRecordError::NegativeTimestamp(ts));
        }
        if let Some(p) = partition
            && p < 0
        {
            return Err(ProducerRecordError::NegativePartition(p));
        }
        let headers = headers.map(RecordHeaders::from_headers).unwrap_or_default();
        Ok(ProducerRecord { topic, partition, headers, key, value, timestamp })
    }

    /// Creates a record with a specified timestamp to be sent to a specified
    /// topic and partition. Mirrors the 5-arg Java constructor (no headers).
    pub fn with_timestamp(
        topic: impl Into<Arc<str>>,
        partition: Option<i32>,
        timestamp: Option<i64>,
        key: Option<K>,
        value: Option<V>,
    ) -> Result<Self, ProducerRecordError> {
        Self::with_full(topic, partition, timestamp, key, value, None)
    }

    /// Creates a record to be sent to a specified topic and partition.
    /// Mirrors the 5-arg Java constructor (`topic, partition, key, value, headers`).
    pub fn with_partition_and_headers(
        topic: impl Into<Arc<str>>,
        partition: Option<i32>,
        key: Option<K>,
        value: Option<V>,
        headers: Option<Vec<RecordHeader>>,
    ) -> Result<Self, ProducerRecordError> {
        Self::with_full(topic, partition, None, key, value, headers)
    }

    /// Creates a record to be sent to a specified topic and partition.
    /// Mirrors the 4-arg Java constructor.
    pub fn with_partition(
        topic: impl Into<Arc<str>>,
        partition: Option<i32>,
        key: Option<K>,
        value: Option<V>,
    ) -> Result<Self, ProducerRecordError> {
        Self::with_full(topic, partition, None, key, value, None)
    }

    /// Create a record to be sent to Kafka. Mirrors the 3-arg Java
    /// constructor (`topic, key, value`).
    pub fn with_key(topic: impl Into<Arc<str>>, key: Option<K>, value: Option<V>) -> Result<Self, ProducerRecordError> {
        Self::with_full(topic, None, None, key, value, None)
    }

    /// Create a record with no key. Mirrors the 2-arg Java constructor.
    pub fn new(topic: impl Into<Arc<str>>, value: Option<V>) -> Result<Self, ProducerRecordError> {
        Self::with_full(topic, None, None, None, value, None)
    }

    /// The topic this record is being sent to. Returns a borrowed `&str` —
    /// no allocation per call (CLAUDE.md rule 12).
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Borrow the topic as the shared `Arc<str>` for callers that want to
    /// share-clone (refcount bump) into a `TopicPartition` or interning map.
    pub fn topic_arc(&self) -> &Arc<str> {
        &self.topic
    }

    /// The headers (mutable accessor mirrors Java's `headers()` returning
    /// the underlying `Headers` interface).
    pub fn headers(&self) -> &RecordHeaders {
        &self.headers
    }

    /// Mutable accessor for the headers — Java returns `Headers` (an
    /// interface) which exposes mutating methods. Rust requires explicit
    /// `&mut`; we provide it for parity with `KafkaProducer` interceptors
    /// that want to mutate headers.
    pub fn headers_mut(&mut self) -> &mut RecordHeaders {
        &mut self.headers
    }

    /// The key (or `None` if no key is specified).
    pub fn key(&self) -> Option<&K> {
        self.key.as_ref()
    }

    /// The value (or `None` if no value is specified).
    pub fn value(&self) -> Option<&V> {
        self.value.as_ref()
    }

    /// The timestamp, in milliseconds since epoch, or `None` if the producer
    /// should stamp it.
    pub fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// The partition to which the record will be sent (or `None` if no
    /// partition was specified).
    pub fn partition(&self) -> Option<i32> {
        self.partition
    }
}

impl<K: Clone, V: Clone> Clone for ProducerRecord<K, V> {
    fn clone(&self) -> Self {
        ProducerRecord {
            // `Arc::clone` is a refcount bump.
            topic: Arc::clone(&self.topic),
            partition: self.partition,
            headers: self.headers.clone(),
            key: self.key.clone(),
            value: self.value.clone(),
            timestamp: self.timestamp,
        }
    }
}

impl<K, V> fmt::Debug for ProducerRecord<K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Mirrors Java's `toString`. Java prints `null` for unset fields.
        write!(f, "ProducerRecord(topic={}, partition=", self.topic)?;
        match self.partition {
            Some(p) => write!(f, "{}", p)?,
            None => f.write_str("null")?,
        }
        write!(f, ", headers={:?}, key=", self.headers)?;
        match &self.key {
            Some(k) => write!(f, "{:?}", k)?,
            None => f.write_str("null")?,
        }
        f.write_str(", value=")?;
        match &self.value {
            Some(v) => write!(f, "{:?}", v)?,
            None => f.write_str("null")?,
        }
        f.write_str(", timestamp=")?;
        match self.timestamp {
            Some(ts) => write!(f, "{}", ts)?,
            None => f.write_str("null")?,
        }
        f.write_str(")")
    }
}

impl<K, V> fmt::Display for ProducerRecord<K, V>
where
    K: fmt::Display,
    V: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Mirrors Java's `toString`. Java prints `null` for unset fields.
        write!(f, "ProducerRecord(topic={}, partition=", self.topic)?;
        match self.partition {
            Some(p) => write!(f, "{}", p)?,
            None => f.write_str("null")?,
        }
        write!(f, ", headers={:?}, key=", self.headers)?;
        match &self.key {
            Some(k) => write!(f, "{}", k)?,
            None => f.write_str("null")?,
        }
        f.write_str(", value=")?;
        match &self.value {
            Some(v) => write!(f, "{}", v)?,
            None => f.write_str("null")?,
        }
        f.write_str(", timestamp=")?;
        match self.timestamp {
            Some(ts) => write!(f, "{}", ts)?,
            None => f.write_str("null")?,
        }
        f.write_str(")")
    }
}

impl<K: PartialEq, V: PartialEq> PartialEq for ProducerRecord<K, V> {
    fn eq(&self, other: &Self) -> bool {
        // Compare topic by string contents — `Arc::ptr_eq` would miss equal
        // topics that arrived through different allocations (cf. Java's
        // `Objects.equals(topic, that.topic)`).
        *self.topic == *other.topic
            && self.partition == other.partition
            && self.headers == other.headers
            && self.key == other.key
            && self.value == other.value
            && self.timestamp == other.timestamp
    }
}

impl<K: Eq, V: Eq> Eq for ProducerRecord<K, V> {}

impl<K: Hash, V: Hash> Hash for ProducerRecord<K, V> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Mirrors Java's `hashCode()` — order: topic, partition, headers,
        // key, value, timestamp.
        (*self.topic).hash(state);
        self.partition.hash(state);
        // RecordHeaders does not implement Hash; hash by the slice of header
        // entries (RecordHeader implements Hash).
        for h in self.headers.iter() {
            h.hash(state);
        }
        self.key.hash(state);
        self.value.hash(state);
        self.timestamp.hash(state);
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `ProducerRecordTest`.

    use super::*;

    /// Java: `testEqualsAndHashCode`.
    #[test]
    fn equals_and_hash_code() {
        let producer_record =
            ProducerRecord::<String, i32>::with_partition("test", Some(1), Some("key".to_string()), Some(1)).unwrap();
        assert_eq!(producer_record, producer_record);

        let equal_record =
            ProducerRecord::<String, i32>::with_partition("test", Some(1), Some("key".to_string()), Some(1)).unwrap();
        assert_eq!(producer_record, equal_record);
        assert_eq!(record_hash(&producer_record), record_hash(&equal_record));

        let topic_mismatch =
            ProducerRecord::<String, i32>::with_partition("test-1", Some(1), Some("key".to_string()), Some(1)).unwrap();
        assert_ne!(producer_record, topic_mismatch);

        let partition_mismatch =
            ProducerRecord::<String, i32>::with_partition("test", Some(2), Some("key".to_string()), Some(1)).unwrap();
        assert_ne!(producer_record, partition_mismatch);

        let key_mismatch =
            ProducerRecord::<String, i32>::with_partition("test", Some(1), Some("key-1".to_string()), Some(1)).unwrap();
        assert_ne!(producer_record, key_mismatch);

        let value_mismatch =
            ProducerRecord::<String, i32>::with_partition("test", Some(1), Some("key".to_string()), Some(2)).unwrap();
        assert_ne!(producer_record, value_mismatch);

        let null_fields_record =
            ProducerRecord::<String, i32>::with_full("topic", None, None, None, None, None).unwrap();
        assert_eq!(null_fields_record, null_fields_record);
        assert_eq!(record_hash(&null_fields_record), record_hash(&null_fields_record));
    }

    /// Java: `testInvalidRecords`.
    ///
    /// The Java test has three cases: null topic, negative timestamp,
    /// negative partition. The "null topic" case (Java passes `null`) is
    /// elided in Rust because the `impl Into<Arc<str>>` parameter has
    /// no `null` representation — the constraint is enforced at the
    /// type level, so the runtime guard is unnecessary. The other two
    /// cases are translated directly.
    #[test]
    fn invalid_records() {
        // Java: negative timestamp
        let err =
            ProducerRecord::<String, i32>::with_timestamp("test", Some(0), Some(-1), Some("key".to_string()), Some(1))
                .expect_err("Expected error to be raised because of negative timestamp");
        assert_eq!(err, ProducerRecordError::NegativeTimestamp(-1));

        // Java: negative partition
        let err = ProducerRecord::<String, i32>::with_partition("test", Some(-1), Some("key".to_string()), Some(1))
            .expect_err("Expected error to be raised because of negative partition");
        assert_eq!(err, ProducerRecordError::NegativePartition(-1));
    }

    /// Java accepts `""` as a valid topic at construction (the broker
    /// rejects later in metadata lookup). Verify the Rust constructor
    /// preserves that contract — see Issue 1 from Phase 6c Round 1.
    #[test]
    fn empty_topic_is_accepted() {
        let record =
            ProducerRecord::<String, i32>::with_partition("", Some(0), Some("key".to_string()), Some(1)).unwrap();
        assert_eq!(record.topic(), "");
        assert_eq!(record.partition(), Some(0));
    }

    fn record_hash<K: Hash, V: Hash>(r: &ProducerRecord<K, V>) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        r.hash(&mut hasher);
        hasher.finish()
    }
}
