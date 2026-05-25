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

//! A key/value pair received from Kafka.
//!
//! Translated from `org.apache.kafka.clients.consumer.ConsumerRecord`.

use std::fmt;
use std::sync::Arc;

use crate::common::header::internals::RecordHeaders;
use crate::common::record::TimestampType;

/// Sentinel value indicating no timestamp is associated with a record.
///
/// Corresponds to Java's `RecordBatch.NO_TIMESTAMP` referenced via
/// `ConsumerRecord.NO_TIMESTAMP`.
pub const NO_TIMESTAMP: i64 = -1;

/// Sentinel value used for `serialized_key_size` / `serialized_value_size`
/// when the key/value is `None`.
///
/// Corresponds to Java's `ConsumerRecord.NULL_SIZE`.
pub const NULL_SIZE: i32 = -1;

/// A key/value pair received from Kafka.
///
/// Corresponds to Java's `org.apache.kafka.clients.consumer.ConsumerRecord<K,V>`.
///
/// Per `consumer-threading.md` §27, the `topic` is an [`Arc<str>`] that is
/// shared cheaply across all records from the same topic-partition (via
/// `SubscriptionState`), and the `headers` are owned for this milestone
/// (matching Java's allocation behavior).
///
/// # Thread safety
///
/// Mirrors Java's behavior: this struct is not designed for concurrent
/// mutation; `Headers` are mutable. Concurrent reads are safe by Rust's
/// borrow rules.
///
/// # Equality
///
/// `PartialEq` / `Eq` are derived (gated on `K: PartialEq, V: PartialEq` /
/// `K: Eq, V: Eq`) so that tests can compare two record batches for
/// structural equality. This mirrors Java's behavior where
/// `ConsumerRecord` equality is value-based; the Java class itself does
/// not override `equals`, but its fields are all value types, so two
/// records with identical fields compare equal via `Objects.equals`.
/// The cost is paid only by callers that opt in to `PartialEq` types
/// (e.g. tests using `i32` keys); users with non-`PartialEq` `K`/`V`
/// continue to work because the bounds are gated by the derive.
#[derive(PartialEq, Eq)]
pub struct ConsumerRecord<K, V> {
    topic: Arc<str>,
    partition: i32,
    offset: i64,
    timestamp: i64,
    timestamp_type: TimestampType,
    serialized_key_size: i32,
    serialized_value_size: i32,
    headers: RecordHeaders,
    key: Option<K>,
    value: Option<V>,
    leader_epoch: Option<i32>,
    delivery_count: Option<i16>,
}

impl<K, V> ConsumerRecord<K, V> {
    /// Creates a record from a specified topic and partition (provided for
    /// compatibility with the 5-arg Java constructor).
    ///
    /// The timestamp is set to [`NO_TIMESTAMP`], the timestamp type to
    /// [`TimestampType::NoTimestampType`], the serialized sizes to
    /// [`NULL_SIZE`], headers to an empty [`RecordHeaders`], and both
    /// `leader_epoch` and `delivery_count` to `None`.
    pub fn new(topic: impl Into<Arc<str>>, partition: i32, offset: i64, key: Option<K>, value: Option<V>) -> Self {
        Self::with_all(
            topic,
            partition,
            offset,
            NO_TIMESTAMP,
            TimestampType::NoTimestampType,
            NULL_SIZE,
            NULL_SIZE,
            key,
            value,
            RecordHeaders::new(),
            None,
            None,
        )
    }

    /// Creates a record with full metadata except `delivery_count`.
    ///
    /// Mirrors Java's 11-arg constructor.
    #[allow(clippy::too_many_arguments)]
    pub fn with_headers(
        topic: impl Into<Arc<str>>,
        partition: i32,
        offset: i64,
        timestamp: i64,
        timestamp_type: TimestampType,
        serialized_key_size: i32,
        serialized_value_size: i32,
        key: Option<K>,
        value: Option<V>,
        headers: RecordHeaders,
        leader_epoch: Option<i32>,
    ) -> Self {
        Self::with_all(
            topic,
            partition,
            offset,
            timestamp,
            timestamp_type,
            serialized_key_size,
            serialized_value_size,
            key,
            value,
            headers,
            leader_epoch,
            None,
        )
    }

    /// Creates a record with full metadata including `delivery_count`.
    ///
    /// Mirrors Java's 12-arg constructor.
    #[allow(clippy::too_many_arguments)]
    pub fn with_all(
        topic: impl Into<Arc<str>>,
        partition: i32,
        offset: i64,
        timestamp: i64,
        timestamp_type: TimestampType,
        serialized_key_size: i32,
        serialized_value_size: i32,
        key: Option<K>,
        value: Option<V>,
        headers: RecordHeaders,
        leader_epoch: Option<i32>,
        delivery_count: Option<i16>,
    ) -> Self {
        // Java validates `topic != null` and `headers != null`; both are
        // type-system invariants in Rust (Arc<str> and RecordHeaders).
        Self {
            topic: topic.into(),
            partition,
            offset,
            timestamp,
            timestamp_type,
            serialized_key_size,
            serialized_value_size,
            headers,
            key,
            value,
            leader_epoch,
            delivery_count,
        }
    }

    /// The topic this record is received from (never null).
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The partition from which this record is received.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// The position of this record in the corresponding Kafka partition.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// The timestamp of this record, in milliseconds elapsed since unix
    /// epoch.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// The timestamp type of this record.
    pub fn timestamp_type(&self) -> TimestampType {
        self.timestamp_type
    }

    /// The size of the serialized, uncompressed key in bytes. Returns
    /// [`NULL_SIZE`] (`-1`) if the key is `None`.
    pub fn serialized_key_size(&self) -> i32 {
        self.serialized_key_size
    }

    /// The size of the serialized, uncompressed value in bytes. Returns
    /// [`NULL_SIZE`] (`-1`) if the value is `None`.
    pub fn serialized_value_size(&self) -> i32 {
        self.serialized_value_size
    }

    /// The key (or `None` if no key was specified).
    pub fn key(&self) -> Option<&K> {
        self.key.as_ref()
    }

    /// The value (or `None` if no value was specified).
    pub fn value(&self) -> Option<&V> {
        self.value.as_ref()
    }

    /// The headers (never null).
    pub fn headers(&self) -> &RecordHeaders {
        &self.headers
    }

    /// Get the leader epoch for the record if available.
    pub fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Get the delivery count for the record if available.
    ///
    /// Deliveries are counted for records delivered by share groups.
    pub fn delivery_count(&self) -> Option<i16> {
        self.delivery_count
    }
}

impl<K, V> fmt::Debug for ConsumerRecord<K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerRecord")
            .field("topic", &&*self.topic)
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .field("timestamp", &self.timestamp)
            .field("timestamp_type", &self.timestamp_type)
            .field("serialized_key_size", &self.serialized_key_size)
            .field("serialized_value_size", &self.serialized_value_size)
            .field("headers", &self.headers)
            .field("key", &self.key)
            .field("value", &self.value)
            .field("leader_epoch", &self.leader_epoch)
            .field("delivery_count", &self.delivery_count)
            .finish()
    }
}

impl<K, V> fmt::Display for ConsumerRecord<K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    /// Matches Java's `toString()`:
    ///
    /// ```text
    /// ConsumerRecord(topic = T, partition = P, leaderEpoch = E, offset = O, TS_TYPE = TS,
    ///   deliveryCount = D, serialized key size = SKS, serialized value size = SVS,
    ///   headers = H, key = K, value = V)
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let leader_epoch_str = match self.leader_epoch {
            Some(e) => e.to_string(),
            None => "null".to_string(),
        };
        let delivery_count_str = match self.delivery_count {
            Some(d) => d.to_string(),
            None => "null".to_string(),
        };
        let key_str = match &self.key {
            Some(k) => format!("{:?}", k),
            None => "null".to_string(),
        };
        let value_str = match &self.value {
            Some(v) => format!("{:?}", v),
            None => "null".to_string(),
        };
        write!(
            f,
            "ConsumerRecord(topic = {}, partition = {}, leaderEpoch = {}, offset = {}, {} = {}, deliveryCount = {}, serialized key size = {}, serialized value size = {}, headers = {}, key = {}, value = {})",
            self.topic,
            self.partition,
            leader_epoch_str,
            self.offset,
            self.timestamp_type,
            self.timestamp,
            delivery_count_str,
            self.serialized_key_size,
            self.serialized_value_size,
            self.headers,
            key_str,
            value_str
        )
    }
}
