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

use crate::common::header::internals::RecordHeader;
use crate::common::record::RecordBatch;

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

impl SimpleRecord {
    /// Create a new `SimpleRecord` with all fields specified.
    ///
    /// Corresponds to Java's `SimpleRecord(long, ByteBuffer, ByteBuffer, Header[])`.
    pub fn new(timestamp: i64, key: Option<Vec<u8>>, value: Option<Vec<u8>>, headers: Vec<RecordHeader>) -> Self {
        Self { key, value, timestamp, headers }
    }

    /// Create a new `SimpleRecord` with timestamp, key, and value (no headers).
    ///
    /// Corresponds to Java's `SimpleRecord(long, byte[], byte[])`.
    pub fn new_with_key_value(timestamp: i64, key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self::new(timestamp, key, value, Vec::new())
    }

    /// Create a new `SimpleRecord` with timestamp and value only (no key, no headers).
    ///
    /// Corresponds to Java's `SimpleRecord(long, byte[])`.
    pub fn new_with_timestamp_value(timestamp: i64, value: Option<Vec<u8>>) -> Self {
        Self::new(timestamp, None, value, Vec::new())
    }

    /// Create a new `SimpleRecord` with value only (no timestamp, no key, no headers).
    ///
    /// Uses `RecordBatch::NO_TIMESTAMP` as the timestamp.
    ///
    /// Corresponds to Java's `SimpleRecord(byte[])`.
    pub fn new_with_value(value: Option<Vec<u8>>) -> Self {
        Self::new(RecordBatch::NO_TIMESTAMP, None, value, Vec::new())
    }

    /// Create a new `SimpleRecord` with key and value only (no timestamp, no headers).
    ///
    /// Uses `RecordBatch::NO_TIMESTAMP` as the timestamp.
    ///
    /// Corresponds to Java's `SimpleRecord(byte[], byte[])`.
    pub fn new_with_key_value_no_timestamp(key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self::new(RecordBatch::NO_TIMESTAMP, key, value, Vec::new())
    }

    /// Create a `SimpleRecord` from a `Record` trait implementor.
    ///
    /// Copies the key, value, and headers from the record.
    ///
    /// Corresponds to Java's `SimpleRecord(Record)`.
    pub fn from_record(record: &dyn super::record_trait::Record) -> Self {
        Self::new(
            record.timestamp(),
            record.key().map(|k| k.to_vec()),
            record.value().map(|v| v.to_vec()),
            record.headers().to_vec(),
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
    use crate::common::header::internals::RecordHeader;

    #[test]
    fn test_new_with_all_fields() {
        let headers = vec![RecordHeader::new("h1".to_string(), Some(b"v1".to_vec()))];
        let record = SimpleRecord::new(100, Some(b"key".to_vec()), Some(b"value".to_vec()), headers);
        assert_eq!(record.timestamp(), 100);
        assert_eq!(record.key(), Some(b"key".as_slice()));
        assert_eq!(record.value(), Some(b"value".as_slice()));
        assert_eq!(record.headers().len(), 1);
    }

    #[test]
    fn test_new_with_key_value() {
        let record = SimpleRecord::new_with_key_value(100, Some(b"key".to_vec()), Some(b"value".to_vec()));
        assert_eq!(record.timestamp(), 100);
        assert_eq!(record.key(), Some(b"key".as_slice()));
        assert_eq!(record.value(), Some(b"value".as_slice()));
        assert!(record.headers().is_empty());
    }

    #[test]
    fn test_new_with_value() {
        let record = SimpleRecord::new_with_value(Some(b"value".to_vec()));
        assert_eq!(record.timestamp(), RecordBatch::NO_TIMESTAMP);
        assert_eq!(record.key(), None);
        assert_eq!(record.value(), Some(b"value".as_slice()));
    }

    #[test]
    fn test_null_key_and_value() {
        let record = SimpleRecord::new_with_key_value(100, None, None);
        assert_eq!(record.key(), None);
        assert_eq!(record.value(), None);
    }

    #[test]
    fn test_equality() {
        let r1 = SimpleRecord::new_with_key_value(100, Some(b"k".to_vec()), Some(b"v".to_vec()));
        let r2 = SimpleRecord::new_with_key_value(100, Some(b"k".to_vec()), Some(b"v".to_vec()));
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_inequality_different_timestamp() {
        let r1 = SimpleRecord::new_with_key_value(100, Some(b"k".to_vec()), Some(b"v".to_vec()));
        let r2 = SimpleRecord::new_with_key_value(200, Some(b"k".to_vec()), Some(b"v".to_vec()));
        assert_ne!(r1, r2);
    }

    #[test]
    fn test_display() {
        let record = SimpleRecord::new_with_key_value(100, Some(b"hi".to_vec()), Some(b"there".to_vec()));
        let display = format!("{}", record);
        assert!(display.contains("SimpleRecord"));
        assert!(display.contains("timestamp=100"));
        assert!(display.contains("key=2 bytes"));
        assert!(display.contains("value=5 bytes"));
    }

    #[test]
    fn test_display_null_key_value() {
        let record = SimpleRecord::new_with_key_value(100, None, None);
        let display = format!("{}", record);
        assert!(display.contains("key=0 bytes"));
        assert!(display.contains("value=0 bytes"));
    }
}
