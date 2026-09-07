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

/// Every parameter of Java's widest `ConsumerRecord` constructor
/// (`ConsumerRecord.java:138`).
///
/// This struct has **no Java counterpart** (DoD #7). It exists solely to
/// satisfy CLAUDE.md §2's cap on derived overload names: that constructor
/// differs from the group's intersection
/// `{topic, partition, offset, key, value}` by seven parameters, so the cap
/// fires and this struct becomes the method's *only* parameter, carrying
/// every Java parameter including the intersection's own.
///
/// It deliberately has **no** `Default`. `topic`, `partition`, `offset`,
/// `key` and `value` are what even Java's narrowest constructor
/// (`ConsumerRecord.java:83`) takes from its caller, so none of them has a
/// Java-derived default — and a synthesised empty topic would name no
/// partition at all. Construct it with [`ConsumerRecordOptionsBuilder::new`]
/// and set them: [`ConsumerRecordOptionsBuilder::build`] panics if any of `topic`, `partition`, `offset`, `key`, `value` was not set.
#[non_exhaustive]
pub struct ConsumerRecordOptions<K, V> {
    /// The topic this record is received from. Java's `topic`.
    pub topic: Arc<str>,
    /// The partition of the topic this record is received from. Java's
    /// `partition`.
    pub partition: i32,
    /// The offset of this record in the corresponding Kafka partition.
    /// Java's `offset`.
    pub offset: i64,
    /// The timestamp of the record. Java's `timestamp`; starts as
    /// [`NO_TIMESTAMP`], as in `:83`.
    pub timestamp: i64,
    /// The timestamp type of the record. Java's `timestampType`; starts as
    /// [`TimestampType::NoTimestampType`], as in `:83`.
    pub timestamp_type: TimestampType,
    /// The length of the serialized key. Java's `serializedKeySize`; starts
    /// as [`NULL_SIZE`], as in `:83`.
    pub serialized_key_size: i32,
    /// The length of the serialized value. Java's `serializedValueSize`;
    /// starts as [`NULL_SIZE`], as in `:83`.
    pub serialized_value_size: i32,
    /// The key of the record, if one exists. Java's `key`.
    pub key: Option<K>,
    /// The record contents. Java's `value`.
    pub value: Option<V>,
    /// The headers of the record. Java's `headers`; starts empty, as in
    /// `:83` (`new RecordHeaders()`).
    pub headers: RecordHeaders,
    /// The leader epoch, if available. Java's `leaderEpoch`; starts as
    /// `None`, as in `:83` (`Optional.empty()`).
    pub leader_epoch: Option<i32>,
    /// The delivery count, if available. Java's `deliveryCount`; starts as
    /// `None`, as in `:107`/`:83` (`Optional.empty()`).
    pub delivery_count: Option<i16>,
}

/// Fluent builder for [`ConsumerRecordOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — panicking
/// if they were not set. Like [`ConsumerRecordOptions`] it has no Java counterpart and
/// exists solely to satisfy that naming rule (DoD #7).
pub struct ConsumerRecordOptionsBuilder<K, V> {
    topic: Option<Arc<str>>,
    partition: Option<i32>,
    offset: Option<i64>,
    timestamp: i64,
    timestamp_type: TimestampType,
    serialized_key_size: i32,
    serialized_value_size: i32,
    key: Option<Option<K>>,
    value: Option<Option<V>>,
    headers: RecordHeaders,
    leader_epoch: Option<i32>,
    delivery_count: Option<i16>,
}

impl<K, V> Default for ConsumerRecordOptionsBuilder<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ConsumerRecordOptionsBuilder<K, V> {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    pub fn new() -> Self {
        Self {
            topic: None,
            partition: None,
            offset: None,
            timestamp: NO_TIMESTAMP,
            timestamp_type: TimestampType::NoTimestampType,
            serialized_key_size: NULL_SIZE,
            serialized_value_size: NULL_SIZE,
            key: None,
            value: None,
            headers: RecordHeaders::new(),
            leader_epoch: None,
            delivery_count: None,
        }
    }

    /// Sets [`ConsumerRecordOptions::topic`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_topic(mut self, topic: impl Into<Arc<str>>) -> Self {
        self.topic = Some(topic.into());
        self
    }
    /// Sets [`ConsumerRecordOptions::partition`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }
    /// Sets [`ConsumerRecordOptions::offset`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_offset(mut self, offset: i64) -> Self {
        self.offset = Some(offset);
        self
    }
    /// Sets [`ConsumerRecordOptions::timestamp`].
    pub fn set_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
    }
    /// Sets [`ConsumerRecordOptions::timestamp_type`].
    pub fn set_timestamp_type(mut self, timestamp_type: TimestampType) -> Self {
        self.timestamp_type = timestamp_type;
        self
    }
    /// Sets [`ConsumerRecordOptions::serialized_key_size`].
    pub fn set_serialized_key_size(mut self, serialized_key_size: i32) -> Self {
        self.serialized_key_size = serialized_key_size;
        self
    }
    /// Sets [`ConsumerRecordOptions::serialized_value_size`].
    pub fn set_serialized_value_size(mut self, serialized_value_size: i32) -> Self {
        self.serialized_value_size = serialized_value_size;
        self
    }
    /// Sets [`ConsumerRecordOptions::key`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_key(mut self, key: Option<K>) -> Self {
        self.key = Some(key);
        self
    }
    /// Sets [`ConsumerRecordOptions::value`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_value(mut self, value: Option<V>) -> Self {
        self.value = Some(value);
        self
    }
    /// Sets [`ConsumerRecordOptions::headers`].
    pub fn set_headers(mut self, headers: RecordHeaders) -> Self {
        self.headers = headers;
        self
    }
    /// Sets [`ConsumerRecordOptions::leader_epoch`].
    pub fn set_leader_epoch(mut self, leader_epoch: Option<i32>) -> Self {
        self.leader_epoch = leader_epoch;
        self
    }
    /// Sets [`ConsumerRecordOptions::delivery_count`].
    pub fn set_delivery_count(mut self, delivery_count: Option<i16>) -> Self {
        self.delivery_count = delivery_count;
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor, so a later Java version that makes one of
    /// them optional changes the set this accepts instead of adding a second
    /// constructor. Today there is one mandatory set: `topic`, `partition`, `offset`, `key`, `value`.
    ///
    /// # Panics
    ///
    /// Panics if any parameter of that set was not given a setter call.
    pub fn build(self) -> ConsumerRecordOptions<K, V> {
        ConsumerRecordOptions {
            topic: self.topic.unwrap_or_else(|| Self::missing("topic")),
            partition: self.partition.unwrap_or_else(|| Self::missing("partition")),
            offset: self.offset.unwrap_or_else(|| Self::missing("offset")),
            timestamp: self.timestamp,
            timestamp_type: self.timestamp_type,
            serialized_key_size: self.serialized_key_size,
            serialized_value_size: self.serialized_value_size,
            key: self.key.unwrap_or_else(|| Self::missing("key")),
            value: self.value.unwrap_or_else(|| Self::missing("value")),
            headers: self.headers,
            leader_epoch: self.leader_epoch,
            delivery_count: self.delivery_count,
        }
    }

    /// Panics naming a mandatory parameter [`Self::build`] found unset.
    fn missing(parameter: &str) -> ! {
        panic!("ConsumerRecordOptionsBuilder::build: mandatory parameter `{parameter}` was not set");
    }
}

impl<K, V> ConsumerRecord<K, V> {
    /// Creates a record from a specified topic and partition.
    ///
    /// Corresponds to Java's `ConsumerRecord(String, int, long, K, V)`
    /// (`ConsumerRecord.java:83`), whose parameters
    /// `{topic, partition, offset, key, value}` are the intersection across
    /// the three constructors — so it owns the plain name (CLAUDE.md §2).
    ///
    /// The timestamp is set to [`NO_TIMESTAMP`], the timestamp type to
    /// [`TimestampType::NoTimestampType`], the serialized sizes to
    /// [`NULL_SIZE`], headers to an empty [`RecordHeaders`], and both
    /// `leader_epoch` and `delivery_count` to `None`.
    pub fn new(topic: impl Into<Arc<str>>, partition: i32, offset: i64, key: Option<K>, value: Option<V>) -> Self {
        Self::new_options(
            ConsumerRecordOptionsBuilder::new()
                .set_topic(topic)
                .set_partition(partition)
                .set_offset(offset)
                .set_key(key)
                .set_value(value)
                .build(),
        )
    }

    /// Creates a record with full metadata.
    ///
    /// Corresponds to Java's widest constructor
    /// (`ConsumerRecord.java:138`), which takes `deliveryCount` alongside
    /// every other field. Its twelve parameters exceed CLAUDE.md §2's
    /// three-parameter cap on derived overload names, so
    /// [`ConsumerRecordOptions`] is this method's only parameter and carries
    /// all of them.
    ///
    /// Java's intermediate 11-arg constructor (`ConsumerRecord.java:107`) is
    /// *not* a separate Rust method: its body is literally this one with
    /// `deliveryCount = Optional.empty()`, and under CLAUDE.md §2 both derive
    /// the same name `new_options` once the surplus parameters move into
    /// [`ConsumerRecordOptions`]. Callers get the 11-arg form by leaving
    /// [`ConsumerRecordOptions::delivery_count`] at `None`.
    ///
    /// * `options` - every parameter of Java's widest constructor
    pub fn new_options(options: ConsumerRecordOptions<K, V>) -> Self {
        // Java validates `topic != null` and `headers != null`; both are
        // type-system invariants in Rust (Arc<str> and RecordHeaders).
        let ConsumerRecordOptions {
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
            delivery_count,
        } = options;
        Self {
            topic,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// CLAUDE.md §2: the mandatory parameters are validated in
    /// [`ConsumerRecordOptionsBuilder::build`], not named in the constructor, so a
    /// builder left untouched panics naming the first one it finds unset.
    #[test]
    #[should_panic(expected = "ConsumerRecordOptionsBuilder::build: mandatory parameter `topic` was not set")]
    fn consumer_record_options_builder_build_panics_when_no_mandatory_parameter_is_set() {
        let _ = ConsumerRecordOptionsBuilder::<i32, i32>::new().build();
    }
}
