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

//! A concrete record header implementation.
//!
//! Corresponds to Java's `org.apache.kafka.common.header.internals.RecordHeader`.

use crate::common::header::Header;

/// A concrete record header consisting of a key-value pair.
///
/// In Java, `RecordHeader` supports lazy deserialization from `ByteBuffer`.
/// In Rust, we always store the deserialized `String` key and `Option<Vec<u8>>`
/// value directly, since there is no equivalent lazy pattern needed.
///
/// Corresponds to Java's `org.apache.kafka.common.header.internals.RecordHeader`.
#[derive(Clone, Debug)]
pub struct RecordHeader {
    key: String,
    value: Option<Vec<u8>>,
}

impl RecordHeader {
    /// Create a new `RecordHeader` with the given key and value.
    ///
    /// # Panics
    ///
    /// This method does not panic. The key must be a valid `String`
    /// (Rust's type system guarantees non-null).
    pub fn new(key: String, value: Option<Vec<u8>>) -> Self {
        Self { key, value }
    }
}

impl Header for RecordHeader {
    fn key(&self) -> &str {
        &self.key
    }

    fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

impl PartialEq for RecordHeader {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
}

impl Eq for RecordHeader {}

impl std::hash::Hash for RecordHeader {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.value.hash(state);
    }
}

impl std::fmt::Display for RecordHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordHeader(key = {}, value = {:?})", self.key, self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::header::Header;

    #[test]
    fn test_key_and_value() {
        let header = RecordHeader::new("key".to_string(), Some(b"value".to_vec()));
        assert_eq!(header.key(), "key");
        assert_eq!(header.value(), Some(b"value".as_slice()));
    }

    #[test]
    fn test_null_value() {
        let header = RecordHeader::new("key".to_string(), None);
        assert_eq!(header.key(), "key");
        assert_eq!(header.value(), None);
    }

    #[test]
    fn test_equality() {
        let h1 = RecordHeader::new("key".to_string(), Some(b"value".to_vec()));
        let h2 = RecordHeader::new("key".to_string(), Some(b"value".to_vec()));
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_inequality_different_key() {
        let h1 = RecordHeader::new("key1".to_string(), Some(b"value".to_vec()));
        let h2 = RecordHeader::new("key2".to_string(), Some(b"value".to_vec()));
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_inequality_different_value() {
        let h1 = RecordHeader::new("key".to_string(), Some(b"value1".to_vec()));
        let h2 = RecordHeader::new("key".to_string(), Some(b"value2".to_vec()));
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let h1 = RecordHeader::new("key".to_string(), Some(b"value".to_vec()));
        let h2 = RecordHeader::new("key".to_string(), Some(b"value".to_vec()));

        let mut hasher1 = DefaultHasher::new();
        h1.hash(&mut hasher1);
        let mut hasher2 = DefaultHasher::new();
        h2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_display() {
        let header = RecordHeader::new("key".to_string(), Some(b"value".to_vec()));
        let display = format!("{}", header);
        assert!(display.contains("key"));
        assert!(display.contains("RecordHeader"));
    }

    /// Corresponds to Java's testRecordHeaderIsReadThreadSafe.
    /// In Rust, RecordHeader fields are not lazily initialized, so thread
    /// safety is guaranteed by the type system. We verify concurrent reads
    /// work correctly nonetheless.
    #[test]
    fn test_record_header_is_read_thread_safe() {
        use std::sync::Arc;

        let header = Arc::new(RecordHeader::new("key".to_string(), Some(b"value".to_vec())));

        let mut handles = Vec::new();
        for _ in 0..16 {
            let h = Arc::clone(&header);
            handles.push(std::thread::spawn(move || {
                assert_eq!(h.key(), "key");
                assert_eq!(h.value(), Some(b"value".as_slice()));
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    /// Corresponds to Java's testRecordHeaderWithNullValueIsReadThreadSafe.
    #[test]
    fn test_record_header_with_null_value_is_read_thread_safe() {
        use std::sync::Arc;

        let header = Arc::new(RecordHeader::new("key".to_string(), None));

        let mut handles = Vec::new();
        for _ in 0..16 {
            let h = Arc::clone(&header);
            handles.push(std::thread::spawn(move || {
                assert_eq!(h.key(), "key");
                assert_eq!(h.value(), None);
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
