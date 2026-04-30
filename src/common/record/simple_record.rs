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

//! Translation of `org.apache.kafka.common.record.SimpleRecord`.

use std::fmt;
use std::sync::Arc;

use crate::common::header::RecordHeader;
use crate::common::record::record_batch::NO_TIMESTAMP;

/// High-level representation of a Kafka record.
///
/// Useful when building record sets to avoid depending on a specific magic
/// version — `SimpleRecord` carries the record's logical fields without any
/// wire-format coupling.
///
/// Storage:
///
/// * `key` and `value` are `Option<Arc<[u8]>>`. Java accepts `byte[]` or
///   `ByteBuffer` and stores a `ByteBuffer` reference. In Rust we keep the
///   payload behind an `Arc<[u8]>` so cloning a `SimpleRecord` (e.g. when it
///   gets pushed onto the producer accumulator) costs only a refcount
///   bump — no payload copy. This honors CLAUDE.md rule 12: the bytes
///   travel from user → accumulator → batch buffer with zero copies.
/// * `headers` is `Arc<[RecordHeader]>` for the same cheap-clone reason. The
///   Java constructor `requireNonNull(headers)` semantic is preserved by
///   making `headers` non-`Option` (always at least an empty slice).
#[derive(Clone)]
pub struct SimpleRecord {
    key: Option<Arc<[u8]>>,
    value: Option<Arc<[u8]>>,
    timestamp: i64,
    headers: Arc<[RecordHeader]>,
}

impl SimpleRecord {
    /// Construct a record from borrowed byte slices and headers. Mirrors
    /// Java's `SimpleRecord(long, byte[], byte[], Header[])` /
    /// `(long, ByteBuffer, ByteBuffer, Header[])`.
    pub fn new(timestamp: i64, key: Option<&[u8]>, value: Option<&[u8]>, headers: &[RecordHeader]) -> Self {
        SimpleRecord {
            key: key.map(Arc::from),
            value: value.map(Arc::from),
            timestamp,
            headers: Arc::from(headers.to_vec().into_boxed_slice()),
        }
    }

    /// Construct a record from already-shared byte payloads. Avoids re-copying
    /// when the caller already has `Arc<[u8]>` (e.g. cloned from a batch).
    pub fn from_arcs(
        timestamp: i64,
        key: Option<Arc<[u8]>>,
        value: Option<Arc<[u8]>>,
        headers: Arc<[RecordHeader]>,
    ) -> Self {
        SimpleRecord { key, value, timestamp, headers }
    }

    /// Construct a record without headers. Mirrors Java's
    /// `SimpleRecord(long, byte[], byte[])` /
    /// `(long, ByteBuffer, ByteBuffer)`.
    pub fn with_no_headers(timestamp: i64, key: Option<&[u8]>, value: Option<&[u8]>) -> Self {
        SimpleRecord::new(timestamp, key, value, &[])
    }

    /// Construct a record carrying only a value. Mirrors Java's
    /// `SimpleRecord(long, byte[])`.
    pub fn with_value(timestamp: i64, value: Option<&[u8]>) -> Self {
        SimpleRecord::with_no_headers(timestamp, None, value)
    }

    /// Construct a record carrying only a value, without an explicit
    /// timestamp. Mirrors Java's `SimpleRecord(byte[])` and
    /// `SimpleRecord(ByteBuffer)`.
    pub fn from_value(value: Option<&[u8]>) -> Self {
        SimpleRecord::with_value(NO_TIMESTAMP, value)
    }

    /// Construct a record carrying both a key and a value, without an
    /// explicit timestamp. Mirrors Java's `SimpleRecord(byte[], byte[])`.
    pub fn from_key_value(key: Option<&[u8]>, value: Option<&[u8]>) -> Self {
        SimpleRecord::with_no_headers(NO_TIMESTAMP, key, value)
    }

    /// Get the record's key as a borrowed slice, or `None` if absent.
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Get the record's value as a borrowed slice, or `None` if absent.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Get the record's timestamp.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Get the record's headers as a borrowed slice.
    pub fn headers(&self) -> &[RecordHeader] {
        &self.headers
    }
}

impl PartialEq for SimpleRecord {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
            && self.key() == other.key()
            && self.value() == other.value()
            && self.headers() == other.headers()
    }
}

impl Eq for SimpleRecord {}

impl std::hash::Hash for SimpleRecord {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Mirror Java's `Objects.hash` semantics: key bytes, value bytes,
        // timestamp, and the header array all contribute.
        self.key().hash(state);
        self.value().hash(state);
        self.timestamp.hash(state);
        self.headers().hash(state);
    }
}

impl fmt::Debug for SimpleRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Java's `toString` prints byte sizes rather than the bytes themselves;
        // mirror that to keep test logs readable.
        write!(
            f,
            "SimpleRecord(timestamp={}, key={} bytes, value={} bytes)",
            self.timestamp,
            self.key().map(<[u8]>::len).unwrap_or(0),
            self.value().map(<[u8]>::len).unwrap_or(0),
        )
    }
}

impl fmt::Display for SimpleRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_value_carries_no_timestamp() {
        let r = SimpleRecord::from_value(Some(b"v"));
        assert_eq!(r.timestamp(), NO_TIMESTAMP);
        assert_eq!(r.key(), None);
        assert_eq!(r.value(), Some(b"v".as_slice()));
        assert_eq!(r.headers(), &[]);
    }

    #[test]
    fn from_value_handles_null_value() {
        let r = SimpleRecord::from_value(None);
        assert_eq!(r.value(), None);
    }

    #[test]
    fn from_key_value_no_timestamp() {
        let r = SimpleRecord::from_key_value(Some(b"k"), Some(b"v"));
        assert_eq!(r.timestamp(), NO_TIMESTAMP);
        assert_eq!(r.key(), Some(b"k".as_slice()));
        assert_eq!(r.value(), Some(b"v".as_slice()));
    }

    #[test]
    fn full_constructor_round_trip() {
        let h = RecordHeader::new("h-key", Some(b"h-value"));
        let r = SimpleRecord::new(42, Some(b"k"), Some(b"v"), std::slice::from_ref(&h));
        assert_eq!(r.timestamp(), 42);
        assert_eq!(r.key(), Some(b"k".as_slice()));
        assert_eq!(r.value(), Some(b"v".as_slice()));
        assert_eq!(r.headers().len(), 1);
        assert_eq!(r.headers()[0], h);
    }

    #[test]
    fn equality_compares_all_fields() {
        let h = RecordHeader::new("h", Some(b"v"));
        let a = SimpleRecord::new(1, Some(b"k"), Some(b"v"), std::slice::from_ref(&h));
        let b = SimpleRecord::new(1, Some(b"k"), Some(b"v"), std::slice::from_ref(&h));
        assert_eq!(a, b);

        let c = SimpleRecord::new(2, Some(b"k"), Some(b"v"), std::slice::from_ref(&h));
        assert_ne!(a, c);

        let d = SimpleRecord::new(1, Some(b"K"), Some(b"v"), std::slice::from_ref(&h));
        assert_ne!(a, d);

        let e = SimpleRecord::new(1, Some(b"k"), Some(b"V"), std::slice::from_ref(&h));
        assert_ne!(a, e);

        let f = SimpleRecord::new(1, Some(b"k"), Some(b"v"), &[]);
        assert_ne!(a, f);
    }

    #[test]
    fn clone_is_cheap_arc_share() {
        // Crude check that cloning shares storage rather than re-copying:
        // both clones must read the same bytes through the same address.
        let value = vec![1u8, 2, 3, 4];
        let r = SimpleRecord::with_value(0, Some(&value));
        let r2 = r.clone();
        let p1 = r.value().unwrap().as_ptr();
        let p2 = r2.value().unwrap().as_ptr();
        assert_eq!(p1, p2);
    }

    #[test]
    fn from_arcs_avoids_recopy() {
        let value: Arc<[u8]> = Arc::from([10u8, 20, 30].as_slice());
        let p_in = value.as_ptr();
        let r = SimpleRecord::from_arcs(0, None, Some(value), Arc::from(Vec::<RecordHeader>::new().into_boxed_slice()));
        let p_out = r.value().unwrap().as_ptr();
        assert_eq!(p_in, p_out);
    }

    #[test]
    fn debug_formats_byte_sizes() {
        let r = SimpleRecord::new(7, Some(b"abc"), Some(b"de"), &[]);
        let s = format!("{r:?}");
        assert_eq!(s, "SimpleRecord(timestamp=7, key=3 bytes, value=2 bytes)");
    }

    #[test]
    fn debug_with_null_key_value() {
        let r = SimpleRecord::with_value(7, None);
        let s = format!("{r:?}");
        assert_eq!(s, "SimpleRecord(timestamp=7, key=0 bytes, value=0 bytes)");
    }
}
