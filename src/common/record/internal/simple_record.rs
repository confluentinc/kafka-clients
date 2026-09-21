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

//! A high-level representation of a Kafka record.
//!
//! This is useful when building record sets to avoid depending on a specific
//! magic version.
//!
//! Corresponds to Java's `org.apache.kafka.common.record.SimpleRecord`.

use crate::common::Error;
use crate::common::header::RecordHeader;
use crate::common::record::internal::RecordBatch;

/// High-level representation of a Kafka record.
///
/// This is useful when building record sets to avoid depending on a specific
/// magic version. It owns its key, value, and headers data.
///
/// Corresponds to Java's `org.apache.kafka.common.record.SimpleRecord`.
#[derive(Clone, Debug)]
pub struct SimpleRecord {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    timestamp: i64,
    headers: Vec<RecordHeader>,
}

/// Parameters for [`SimpleRecord::with_options`].
///
/// This struct has **no Java counterpart** (DoD #7). It exists solely to satisfy
/// CLAUDE.md §2's cap on derived overload names: Java's nine `SimpleRecord`
/// constructors (`SimpleRecord.java:36,44,48,52,56,60,64,68,72`) have an EMPTY
/// parameter-name intersection and none of them is no-arg, so nobody keeps the
/// plain translated name and every overload is suffixed with its full parameter
/// list. For the widest one — `SimpleRecord(long, ByteBuffer, ByteBuffer,
/// Header[])` (`:36`) — that list is four parameters long, past §2's cap of
/// three, so this struct becomes the constructor's *only* parameter and carries
/// every Java parameter.
///
/// It is `pub(crate)` together with [`SimpleRecord::with_options`]: the package
/// is `record.internal`, where §2 mandates `pub(crate)`, and the narrow public
/// re-export documented in [`crate::common::record`] covers only the two Java
/// class names — a Rust-only options type is not part of it, and no
/// crate-external caller needs the four-parameter form.
#[non_exhaustive]
pub(crate) struct SimpleRecordOptions {
    /// The record timestamp. Java's `timestamp`; starts at
    /// [`RecordBatch::NO_TIMESTAMP`], the value `:60`/`:64` passes on the
    /// caller's behalf.
    pub timestamp: i64,
    /// The record key, or `None` for no key. Java's `key`, which `:56`, `:60`
    /// and `:64` pass as `null`.
    pub key: Option<Vec<u8>>,
    /// The record value, or `None` for no value. Java's `value` — the one
    /// parameter no constructor supplies for its caller, hence mandatory.
    pub value: Option<Vec<u8>>,
    /// The record headers. Java's `headers`; starts empty, the value
    /// `Record.EMPTY_HEADERS` that `:48` passes on the caller's behalf. Java's
    /// `:36` guards it with `Objects.requireNonNull`; in Rust a `Vec` cannot be
    /// null, so the check has no translation.
    pub headers: Vec<RecordHeader>,
}

/// Fluent builder for [`SimpleRecordOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — returning
/// [`Error::LocalIllegalArgument`] if they were not set. Like
/// [`SimpleRecordOptions`] it has no Java counterpart and exists solely to
/// satisfy that naming rule (DoD #7).
pub(crate) struct SimpleRecordOptionsBuilder {
    timestamp: Option<i64>,
    key: Option<Vec<u8>>,
    value: Option<Option<Vec<u8>>>,
    headers: Option<Vec<RecordHeader>>,
}

impl Default for SimpleRecordOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleRecordOptionsBuilder {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    pub(crate) fn new() -> Self {
        Self { timestamp: None, key: None, value: None, headers: None }
    }

    /// Sets [`SimpleRecordOptions::timestamp`].
    pub(crate) fn set_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Sets [`SimpleRecordOptions::key`].
    pub(crate) fn set_key(mut self, key: Option<Vec<u8>>) -> Self {
        self.key = key;
        self
    }

    /// Sets [`SimpleRecordOptions::value`], a mandatory parameter:
    /// [`Self::build`] returns an error if it was not set. `None` is a valid
    /// value to set — Java's `value` is nullable — so "not set" and "set to
    /// `None`" are distinct here.
    pub(crate) fn set_value(mut self, value: Option<Vec<u8>>) -> Self {
        self.value = Some(value);
        self
    }

    /// Sets [`SimpleRecordOptions::headers`].
    pub(crate) fn set_headers(mut self, headers: Vec<RecordHeader>) -> Self {
        self.headers = Some(headers);
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor. Today there is one mandatory set:
    /// `value`. The other three are not in it because Java's narrower
    /// constructors supply them themselves — `timestamp` at `:60`/`:64`, `key`
    /// at `:56`/`:60`/`:64`, `headers` at `:48`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter of
    /// that set which was not given a setter call. Only presence is checked
    /// here; semantic validation belongs to the method the options are passed to
    /// (CLAUDE.md §2).
    pub(crate) fn build(self) -> Result<SimpleRecordOptions, Error> {
        Ok(SimpleRecordOptions {
            timestamp: self.timestamp.unwrap_or(RecordBatch::NO_TIMESTAMP),
            key: self.key,
            value: self.value.ok_or_else(|| Self::missing("value"))?,
            headers: self.headers.unwrap_or_default(),
        })
    }

    /// Builds the [`Error::LocalIllegalArgument`] naming a mandatory parameter
    /// [`Self::build`] found unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "SimpleRecordOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

impl SimpleRecord {
    // Java's nine `SimpleRecord` constructors
    // (`SimpleRecord.java:36,44,48,52,56,60,64,68,72`) have an EMPTY
    // parameter-name intersection and none of them is no-arg, so under
    // CLAUDE.md §2 nobody keeps the plain translated name and every overload
    // carries its full parameter list as the suffix. `:36`/`:44` and `:48`/`:52`
    // collapse pairwise, because Rust has no `ByteBuffer`/`byte[]` distinction
    // to discriminate on.

    /// Create a new `SimpleRecord` with all fields specified.
    ///
    /// # Arguments
    ///
    /// * `options` - Every Java parameter, built through
    ///   [`SimpleRecordOptionsBuilder`]. [`SimpleRecordOptions`] is this
    ///   constructor's only parameter because the derived name would list four
    ///   parameters, past CLAUDE.md §2's cap of three.
    ///
    /// Corresponds to Java's `SimpleRecord(long, ByteBuffer, ByteBuffer,
    /// Header[])` (`SimpleRecord.java:36`) and its `byte[]` twin (`:44`).
    pub(crate) fn with_options(options: SimpleRecordOptions) -> Self {
        let SimpleRecordOptions { timestamp, key, value, headers } = options;
        Self { key, value, timestamp, headers }
    }

    /// Create a new `SimpleRecord` with timestamp, key, and value (no headers).
    ///
    /// Corresponds to Java's `SimpleRecord(long, ByteBuffer, ByteBuffer)`
    /// (`SimpleRecord.java:48`), which passes `Record.EMPTY_HEADERS` itself.
    pub fn with_timestamp_key_value(timestamp: i64, key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self::with_options(
            SimpleRecordOptionsBuilder::new()
                .set_timestamp(timestamp)
                .set_key(key)
                .set_value(value)
                .build()
                .expect("SimpleRecordOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Create a new `SimpleRecord` with timestamp and value only (no key, no headers).
    ///
    /// Corresponds to Java's `SimpleRecord(long, byte[])` (`SimpleRecord.java:56`).
    pub fn with_timestamp_value(timestamp: i64, value: Option<Vec<u8>>) -> Self {
        Self::with_timestamp_key_value(timestamp, None, value)
    }

    /// Create a new `SimpleRecord` with value only (no timestamp, no key, no headers).
    ///
    /// Uses `RecordBatch::NO_TIMESTAMP` as the timestamp.
    ///
    /// Corresponds to Java's `SimpleRecord(byte[])` (`SimpleRecord.java:60`) and
    /// its `ByteBuffer` twin (`:64`).
    pub fn with_value(value: Option<Vec<u8>>) -> Self {
        Self::with_timestamp_key_value(RecordBatch::NO_TIMESTAMP, None, value)
    }

    /// Create a new `SimpleRecord` with key and value only (no timestamp, no headers).
    ///
    /// Uses `RecordBatch::NO_TIMESTAMP` as the timestamp.
    ///
    /// Corresponds to Java's `SimpleRecord(byte[], byte[])` (`SimpleRecord.java:68`).
    pub fn with_key_value(key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self::with_timestamp_key_value(RecordBatch::NO_TIMESTAMP, key, value)
    }

    /// Create a `SimpleRecord` from a `Record` trait implementor.
    ///
    /// Copies the key, value, and headers from the record.
    ///
    /// Corresponds to Java's `SimpleRecord(Record)` (`SimpleRecord.java:72`).
    pub fn with_record(record: &dyn super::Record) -> Self {
        Self::with_options(
            SimpleRecordOptionsBuilder::new()
                .set_timestamp(record.timestamp())
                .set_key(record.key().map(|k| k.to_vec()))
                .set_value(record.value().map(|v| v.to_vec()))
                .set_headers(record.headers().to_vec())
                .build()
                .expect("SimpleRecordOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Returns the key, or `None` if there is no key.
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Returns the value, or `None` if there is no value.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Returns the timestamp.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Returns the headers.
    pub fn headers(&self) -> &[RecordHeader] {
        &self.headers
    }
}

impl PartialEq for SimpleRecord {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
            && self.key == other.key
            && self.value == other.value
            && self.headers == other.headers
    }
}

impl Eq for SimpleRecord {}

impl std::hash::Hash for SimpleRecord {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.value.hash(state);
        self.timestamp.hash(state);
        self.headers.hash(state);
    }
}

impl std::fmt::Display for SimpleRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SimpleRecord(timestamp={}, key={} bytes, value={} bytes)",
            self.timestamp,
            self.key.as_ref().map_or(0, |k| k.len()),
            self.value.as_ref().map_or(0, |v| v.len()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::header::RecordHeader;

    #[test]
    fn test_new_with_all_fields() {
        let headers = vec![RecordHeader::new("h1".to_string(), Some(b"v1".to_vec()))];
        let record = SimpleRecord::with_options(
            SimpleRecordOptionsBuilder::new()
                .set_timestamp(100)
                .set_key(Some(b"key".to_vec()))
                .set_value(Some(b"value".to_vec()))
                .set_headers(headers)
                .build()
                .expect("SimpleRecordOptionsBuilder::build: every mandatory parameter is set above"),
        );
        assert_eq!(record.timestamp(), 100);
        assert_eq!(record.key(), Some(b"key".as_slice()));
        assert_eq!(record.value(), Some(b"value".as_slice()));
        assert_eq!(record.headers().len(), 1);
    }

    #[test]
    fn test_new_with_key_value() {
        let record = SimpleRecord::with_timestamp_key_value(100, Some(b"key".to_vec()), Some(b"value".to_vec()));
        assert_eq!(record.timestamp(), 100);
        assert_eq!(record.key(), Some(b"key".as_slice()));
        assert_eq!(record.value(), Some(b"value".as_slice()));
        assert!(record.headers().is_empty());
    }

    #[test]
    fn test_new_with_value() {
        let record = SimpleRecord::with_value(Some(b"value".to_vec()));
        assert_eq!(record.timestamp(), RecordBatch::NO_TIMESTAMP);
        assert_eq!(record.key(), None);
        assert_eq!(record.value(), Some(b"value".as_slice()));
    }

    #[test]
    fn test_null_key_and_value() {
        let record = SimpleRecord::with_timestamp_key_value(100, None, None);
        assert_eq!(record.key(), None);
        assert_eq!(record.value(), None);
    }

    #[test]
    fn test_equality() {
        let r1 = SimpleRecord::with_timestamp_key_value(100, Some(b"k".to_vec()), Some(b"v".to_vec()));
        let r2 = SimpleRecord::with_timestamp_key_value(100, Some(b"k".to_vec()), Some(b"v".to_vec()));
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_inequality_different_timestamp() {
        let r1 = SimpleRecord::with_timestamp_key_value(100, Some(b"k".to_vec()), Some(b"v".to_vec()));
        let r2 = SimpleRecord::with_timestamp_key_value(200, Some(b"k".to_vec()), Some(b"v".to_vec()));
        assert_ne!(r1, r2);
    }

    #[test]
    fn test_display() {
        let record = SimpleRecord::with_timestamp_key_value(100, Some(b"hi".to_vec()), Some(b"there".to_vec()));
        let display = format!("{}", record);
        assert!(display.contains("SimpleRecord"));
        assert!(display.contains("timestamp=100"));
        assert!(display.contains("key=2 bytes"));
        assert!(display.contains("value=5 bytes"));
    }

    #[test]
    fn test_display_null_key_value() {
        let record = SimpleRecord::with_timestamp_key_value(100, None, None);
        let display = format!("{}", record);
        assert!(display.contains("key=0 bytes"));
        assert!(display.contains("value=0 bytes"));
    }
}
