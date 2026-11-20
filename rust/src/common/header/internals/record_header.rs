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
use crate::common::utils::Utils;
use bytes::Bytes;
use std::sync::OnceLock;

/// A concrete record header consisting of a key-value pair.
///
/// A header read from a fetched record holds, as in Java, slices of the
/// buffer the record was read from: the value is handed out as a view of that
/// buffer, and the key is decoded the first time it is read. A header built
/// with [`RecordHeader::new`] owns the key and value it was given.
#[derive(Clone)]
#[doc(alias = "org.apache.kafka.common.header.internals.RecordHeader")]
// the only `Header` implementation; users build one to attach headers to a ProducerRecord, as Java users do despite the internals package
#[doc(alias = "public-in-rust")]
pub struct RecordHeader {
    key: HeaderKey,
    value: Option<HeaderValue>,
}

/// Java's `keyBuffer` / `key` pair.
#[derive(Clone)]
enum HeaderKey {
    /// Java's `key`: the key the header was built with.
    Decoded(String),
    /// Java's `keyBuffer`: the key's bytes as read, a slice of the record's
    /// buffer. `lossy` is set on the first read of the key: `None` when the
    /// bytes are valid UTF-8, so they are the key and nothing is copied, or
    /// the key decoded with U+FFFD for each malformed sequence, as Java's
    /// `Utils.utf8` decodes it ([`Utils::utf8`]).
    Buffer {
        bytes: Bytes,
        lossy: OnceLock<Option<String>>,
    },
}

/// Java's `valueBuffer` / `value` pair.
#[derive(Clone)]
enum HeaderValue {
    /// Java's `value`: the value the header was built with.
    Owned(Vec<u8>),
    /// Java's `valueBuffer`: a slice of the record's buffer, read in place.
    Buffer(Bytes),
}

impl RecordHeader {
    /// Create a new `RecordHeader` with the given key and value.
    ///
    /// # Arguments
    ///
    /// - `key` - the header's key
    /// - `value` - the header's value, `None` for no value
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeader#RecordHeader")]
    pub fn new(key: String, value: Option<Vec<u8>>) -> Self {
        Self { key: HeaderKey::Decoded(key), value: value.map(HeaderValue::Owned) }
    }

    /// Create a `RecordHeader` over the key and value bytes of a record, both
    /// slices of the buffer the record was read from. Nothing is copied: the
    /// key is decoded the first time it is read, and the value is read in
    /// place.
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeader#RecordHeader(ByteBuffer,ByteBuffer)")]
    pub(crate) fn with_key_buffer_value_buffer(key_buffer: Bytes, value_buffer: Option<Bytes>) -> Self {
        Self {
            key: HeaderKey::Buffer { bytes: key_buffer, lossy: OnceLock::new() },
            value: value_buffer.map(HeaderValue::Buffer),
        }
    }
}

impl Header for RecordHeader {
    fn key(&self) -> &str {
        match &self.key {
            HeaderKey::Decoded(key) => key,
            HeaderKey::Buffer { bytes, lossy } => {
                let lossy = lossy.get_or_init(|| match std::str::from_utf8(bytes) {
                    Ok(_) => None,
                    Err(_) => Some(Utils::utf8(bytes).into_owned()),
                });
                match lossy {
                    Some(decoded) => decoded,
                    // SAFETY: `lossy` is `None` only when `std::str::from_utf8` accepted
                    // `bytes`; `bytes` is immutable, so it is still the valid UTF-8 that
                    // was checked.
                    None => unsafe { std::str::from_utf8_unchecked(bytes) },
                }
            },
        }
    }

    fn value(&self) -> Option<&[u8]> {
        match &self.value {
            None => None,
            Some(HeaderValue::Owned(value)) => Some(value),
            Some(HeaderValue::Buffer(value)) => Some(value),
        }
    }
}

impl std::fmt::Debug for RecordHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordHeader")
            .field("key", &self.key())
            .field("value", &self.value())
            .finish()
    }
}

impl PartialEq for RecordHeader {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key() && self.value() == other.value()
    }
}

impl Eq for RecordHeader {}

impl std::hash::Hash for RecordHeader {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key().hash(state);
        self.value().hash(state);
    }
}

impl std::fmt::Display for RecordHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordHeader(key = {}, value = {:?})", self.key(), self.value())
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

    /// The assertion of Java's `RecordHeadersTest#assertRecordHeaderReadThreadSafe`:
    /// 16 tasks read the key and the value of a header nobody has read yet.
    fn assert_record_header_read_thread_safe(header: RecordHeader, key: &str, value: Option<&[u8]>) {
        use std::sync::{Arc, Barrier};

        let thread_count = 16;
        let header = Arc::new(header);
        let start = Arc::new(Barrier::new(thread_count));
        let handles: Vec<_> = (0..thread_count)
            .map(|_| {
                let header = Arc::clone(&header);
                let start = Arc::clone(&start);
                let key = key.to_string();
                let value = value.map(<[u8]>::to_vec);
                std::thread::spawn(move || {
                    start.wait();
                    assert_eq!(header.key(), key);
                    assert_eq!(header.value(), value.as_deref());
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
    }

    /// Java's `RecordHeadersTest#testRecordHeaderIsReadThreadSafe`, a
    /// `@RepeatedTest(100)`.
    #[test]
    fn test_record_header_is_read_thread_safe() {
        for _ in 0..100 {
            let header = RecordHeader::with_key_buffer_value_buffer(
                Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"value")),
            );
            assert_record_header_read_thread_safe(header, "key", Some(b"value"));
        }
    }

    /// Java's `RecordHeadersTest#testRecordHeaderWithNullValueIsReadThreadSafe`,
    /// a `@RepeatedTest(100)`.
    #[test]
    fn test_record_header_with_null_value_is_read_thread_safe() {
        for _ in 0..100 {
            let header = RecordHeader::with_key_buffer_value_buffer(Bytes::from_static(b"key"), None);
            assert_record_header_read_thread_safe(header, "key", None);
        }
    }

    /// Whether `inner` lies inside `outer`.
    fn is_within(inner: &[u8], outer: &[u8]) -> bool {
        let outer = outer.as_ptr_range();
        let inner = inner.as_ptr_range();
        outer.start <= inner.start && inner.end <= outer.end
    }

    /// A header over a buffer hands out views of that buffer: neither the key
    /// nor the value is copied.
    #[test]
    fn test_buffer_header_reads_the_buffer_in_place() {
        let buffer = Bytes::from_static(b"keyvalue");
        let header = RecordHeader::with_key_buffer_value_buffer(buffer.slice(0..3), Some(buffer.slice(3..)));

        assert_eq!(header.key(), "key");
        assert_eq!(header.value(), Some(b"value".as_slice()));
        assert!(is_within(header.key().as_bytes(), &buffer));
        assert!(is_within(header.value().unwrap(), &buffer));
        // Read again: the same view.
        assert!(is_within(header.key().as_bytes(), &buffer));
    }

    /// A key that is not valid UTF-8 is decoded as Java's `Utils.utf8`
    /// decodes it, with U+FFFD for the malformed bytes, and only then copied.
    #[test]
    fn test_buffer_header_decodes_an_invalid_key_lossily() {
        let buffer = Bytes::from_static(b"k\xffy");
        let header = RecordHeader::with_key_buffer_value_buffer(buffer.clone(), None);

        assert_eq!(header.key(), "k\u{fffd}y");
        assert!(!is_within(header.key().as_bytes(), &buffer));
        assert_eq!(header.value(), None);

        // A surrogate code point written in three bytes is one malformed
        // sequence for Java and three for `String::from_utf8_lossy`: the
        // expected keys are Java's (`Utils::utf8`'s tests have the full list).
        let cases: &[(&[u8], &str)] = &[
            (b"\xED\xA0\x80", "\u{fffd}"),
            (b"\xED\xA0\xBD\xED\xB8\x80", "\u{fffd}\u{fffd}"),
            (b"\xED\xB0\x80", "\u{fffd}"),
            (b"\xED\xA0", "\u{fffd}"),
        ];
        for (key, expected) in cases {
            let header = RecordHeader::with_key_buffer_value_buffer(Bytes::from_static(key), None);
            assert_eq!(*expected, header.key(), "decoding {key:02X?}");
        }
    }

    /// Equality, hash, `Display` and `Debug` read the key and value, whatever
    /// holds them.
    #[test]
    fn test_buffer_header_equals_the_same_header_built_from_owned_parts() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let owned = RecordHeader::new("key".to_string(), Some(b"value".to_vec()));
        let buffer =
            RecordHeader::with_key_buffer_value_buffer(Bytes::from_static(b"key"), Some(Bytes::from_static(b"value")));

        assert_eq!(owned, buffer);
        assert_eq!(buffer.clone(), owned);
        let hash = |header: &RecordHeader| {
            let mut hasher = DefaultHasher::new();
            header.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(hash(&owned), hash(&buffer));
        assert_eq!(owned.to_string(), buffer.to_string());
        assert_eq!(format!("{owned:?}"), format!("{buffer:?}"));
        assert_eq!(
            format!("{buffer:?}"),
            "RecordHeader { key: \"key\", value: Some([118, 97, 108, 117, 101]) }"
        );
        assert_eq!(
            buffer.to_string(),
            "RecordHeader(key = key, value = Some([118, 97, 108, 117, 101]))"
        );
    }
}
