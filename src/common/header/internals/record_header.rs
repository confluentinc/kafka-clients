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

//! Translation of `org.apache.kafka.common.header.internals.RecordHeader`.

use std::fmt;
use std::sync::Arc;

use crate::common::header::Header;

/// A concrete `Header` carrying an owned key (`Arc<str>`) and an optional
/// owned value.
///
/// In Java the constructor accepts either a `(String, byte[])` pair or two
/// `ByteBuffer`s (which are decoded lazily on first call to `key()` /
/// `value()`). The Java lazy-decode dance exists to avoid eagerly decoding
/// UTF-8 / copying bytes when a header is parsed from a record batch but
/// never read by user code.
///
/// In Rust, the lazy-decode pattern is unnecessary because:
///
/// 1. We control the parser; it can hand us an already-validated `&str` slice
///    over the original buffer (zero-copy via lifetimes) when the consumer
///    path needs that.
/// 2. The producer path constructs `RecordHeader` from a `&str` / `&[u8]`
///    typed by the user — we store an `Arc<str>` for the key (cheap clones
///    if the same key appears across records) and a `Bytes`-style owned
///    value. This is faster on the hot send path than re-implementing a
///    "double-checked locking" lazy-decode.
///
/// We therefore expose a single eager constructor [`RecordHeader::new`] and
/// a [`RecordHeader::from_bytes`] alternative that decodes UTF-8 from a
/// borrowed key buffer (matching the Java `RecordHeader(ByteBuffer, ByteBuffer)`
/// behaviour: invalid UTF-8 is replaced with the replacement character, so
/// no decode failure is possible at runtime).
#[derive(Clone)]
pub struct RecordHeader {
    key: Arc<str>,
    value: Option<Arc<[u8]>>,
}

impl RecordHeader {
    /// Construct a header from an owned key and an owned, optional value.
    /// Mirrors `RecordHeader(String, byte[])` (Java permits null `value`).
    pub fn new(key: &str, value: Option<&[u8]>) -> Self {
        RecordHeader { key: Arc::from(key), value: value.map(Arc::from) }
    }

    /// Construct a header from raw byte buffers, decoding the key as UTF-8
    /// (replacing invalid sequences with the Unicode replacement character,
    /// matching Java's `Utils.utf8(ByteBuffer)` lossy decode).
    /// Mirrors `RecordHeader(ByteBuffer, ByteBuffer)`.
    pub fn from_bytes(key: &[u8], value: Option<&[u8]>) -> Self {
        let key_str: String = std::str::from_utf8(key)
            .map(str::to_owned)
            .unwrap_or_else(|_| String::from_utf8_lossy(key).into_owned());
        RecordHeader { key: Arc::from(key_str.as_str()), value: value.map(Arc::from) }
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

impl fmt::Debug for RecordHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordHeader(key = {}, value = {:?})", self.key(), self.value())
    }
}

#[cfg(test)]
mod tests {
    // Translation of the producer-relevant `RecordHeader` cases inside
    // `RecordHeadersTest`. The Java test suite has two `@RepeatedTest(100)`
    // methods (`testRecordHeaderIsReadThreadSafe`,
    // `testRecordHeaderWithNullValueIsReadThreadSafe`) that exercise the
    // double-checked-locking lazy decode under contention. Our Rust
    // translation is eager (no lazy decode), so those tests are vacuously
    // true — we still translate them as a single-iteration smoke check
    // that hammers `key()` / `value()` from many threads to confirm the
    // immutable shared state is safely shared via `Arc`.

    use std::sync::Arc as StdArc;
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    #[test]
    fn key_and_value_round_trip() {
        let h = RecordHeader::new("k", Some(b"v"));
        assert_eq!(h.key(), "k");
        assert_eq!(h.value(), Some(b"v".as_slice()));
    }

    #[test]
    fn null_value_is_some_or_none_per_input() {
        let h = RecordHeader::new("k", None);
        assert_eq!(h.value(), None);
    }

    #[test]
    fn from_bytes_decodes_utf8() {
        let h = RecordHeader::from_bytes(b"hello", Some(b"world"));
        assert_eq!(h.key(), "hello");
        assert_eq!(h.value(), Some(b"world".as_slice()));
    }

    /// Java: `testRecordHeaderIsReadThreadSafe` — single iteration using a
    /// barrier + 16 threads.
    #[test]
    fn record_header_concurrent_reads_are_safe() {
        let header = StdArc::new(RecordHeader::new("key", Some(b"value")));
        let n = 16;
        let barrier = StdArc::new(Barrier::new(n));
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let h = header.clone();
            let b = barrier.clone();
            handles.push(thread::spawn(move || {
                b.wait();
                let _k = h.key();
                let _v = h.value();
            }));
        }
        for j in handles {
            j.join().unwrap();
        }
    }

    /// Java: `testRecordHeaderWithNullValueIsReadThreadSafe`.
    #[test]
    fn record_header_with_null_value_concurrent_reads_are_safe() {
        let header = StdArc::new(RecordHeader::new("key", None));
        let n = 16;
        let barrier = StdArc::new(Barrier::new(n));
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let h = header.clone();
            let b = barrier.clone();
            handles.push(thread::spawn(move || {
                b.wait();
                let _k = h.key();
                let _v = h.value();
            }));
        }
        for j in handles {
            j.join().unwrap();
        }
    }
}
