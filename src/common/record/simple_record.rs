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

use bytes::Bytes;

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
/// * `key` and `value` are `Option<Bytes>`. Java accepts `byte[]` or
///   `ByteBuffer` and stores a `ByteBuffer` reference; `Utils.wrapNullable`
///   produces a `ByteBuffer.wrap(byte[])` view that does NOT copy. The
///   `bytes::Bytes` type is the closest Rust equivalent: it carries
///   shared-ownership semantics over a refcounted backing buffer, so cloning
///   a `SimpleRecord` (e.g. when it gets pushed onto the producer
///   accumulator) costs only a refcount bump — no payload copy. The canonical
///   constructor [`SimpleRecord::new`] takes `Option<Bytes>` so callers that
///   already own a `Bytes` (e.g. `MemoryRecordsBuilder` once it lands in
///   Phase 3c) pass through with zero copies, satisfying CLAUDE.md rule 12.
/// * `headers` is `Arc<[RecordHeader]>` for cheap clones. The Java constructor
///   `requireNonNull(headers)` semantic is preserved by making `headers`
///   non-`Option` (always at least an empty slice).
#[derive(Clone)]
pub struct SimpleRecord {
    key: Option<Bytes>,
    value: Option<Bytes>,
    timestamp: i64,
    headers: Arc<[RecordHeader]>,
}

impl SimpleRecord {
    /// Construct a record from already-shared `Bytes` payloads — the
    /// canonical, **zero-copy** constructor. Mirrors Java's
    /// `SimpleRecord(long, ByteBuffer, ByteBuffer, Header[])` where
    /// `ByteBuffer.wrap(byte[])` produces a non-copying view.
    ///
    /// The producer write path (Phase 3c `MemoryRecordsBuilder` and
    /// callers) is expected to invoke this constructor with `Bytes`
    /// payloads it already owns.
    pub fn new(timestamp: i64, key: Option<Bytes>, value: Option<Bytes>, headers: &[RecordHeader]) -> Self {
        SimpleRecord { key, value, timestamp, headers: Arc::from(headers.to_vec().into_boxed_slice()) }
    }

    /// Construct a record by **copying** borrowed byte slices into freshly
    /// allocated `Bytes` payloads. Convenience for tests and callers that
    /// only have a `&[u8]`. This path is **not zero-copy** — each `Some(_)`
    /// argument allocates and memcpys via `Bytes::copy_from_slice`.
    /// Prefer [`SimpleRecord::new`] on the hot path.
    pub fn new_from_slice(timestamp: i64, key: Option<&[u8]>, value: Option<&[u8]>, headers: &[RecordHeader]) -> Self {
        SimpleRecord::new(
            timestamp,
            key.map(Bytes::copy_from_slice),
            value.map(Bytes::copy_from_slice),
            headers,
        )
    }

    /// Construct a record without headers (zero-copy). Mirrors Java's
    /// `SimpleRecord(long, ByteBuffer, ByteBuffer)`.
    pub fn with_no_headers(timestamp: i64, key: Option<Bytes>, value: Option<Bytes>) -> Self {
        SimpleRecord::new(timestamp, key, value, &[])
    }

    /// Construct a record carrying only a value (zero-copy).
    /// Mirrors Java's `SimpleRecord(long, ByteBuffer)`.
    pub fn with_value(timestamp: i64, value: Option<Bytes>) -> Self {
        SimpleRecord::with_no_headers(timestamp, None, value)
    }

    /// Construct a record carrying only a value, without an explicit
    /// timestamp (zero-copy). Mirrors Java's `SimpleRecord(ByteBuffer)`.
    pub fn from_value(value: Option<Bytes>) -> Self {
        SimpleRecord::with_value(NO_TIMESTAMP, value)
    }

    /// Construct a record carrying both a key and a value, without an
    /// explicit timestamp (zero-copy). Mirrors Java's
    /// `SimpleRecord(ByteBuffer, ByteBuffer)` (and the `byte[], byte[]`
    /// overload, since Java's `wrapNullable` views the array without
    /// copying).
    pub fn from_key_value(key: Option<Bytes>, value: Option<Bytes>) -> Self {
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
        let r = SimpleRecord::from_value(Some(Bytes::from_static(b"v")));
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
        let r = SimpleRecord::from_key_value(Some(Bytes::from_static(b"k")), Some(Bytes::from_static(b"v")));
        assert_eq!(r.timestamp(), NO_TIMESTAMP);
        assert_eq!(r.key(), Some(b"k".as_slice()));
        assert_eq!(r.value(), Some(b"v".as_slice()));
    }

    #[test]
    fn full_constructor_round_trip() {
        let h = RecordHeader::new("h-key", Some(b"h-value"));
        let r = SimpleRecord::new(
            42,
            Some(Bytes::from_static(b"k")),
            Some(Bytes::from_static(b"v")),
            std::slice::from_ref(&h),
        );
        assert_eq!(r.timestamp(), 42);
        assert_eq!(r.key(), Some(b"k".as_slice()));
        assert_eq!(r.value(), Some(b"v".as_slice()));
        assert_eq!(r.headers().len(), 1);
        assert_eq!(r.headers()[0], h);
    }

    #[test]
    fn equality_compares_all_fields() {
        let h = RecordHeader::new("h", Some(b"v"));
        let a = SimpleRecord::new_from_slice(1, Some(b"k"), Some(b"v"), std::slice::from_ref(&h));
        let b = SimpleRecord::new_from_slice(1, Some(b"k"), Some(b"v"), std::slice::from_ref(&h));
        assert_eq!(a, b);

        let c = SimpleRecord::new_from_slice(2, Some(b"k"), Some(b"v"), std::slice::from_ref(&h));
        assert_ne!(a, c);

        let d = SimpleRecord::new_from_slice(1, Some(b"K"), Some(b"v"), std::slice::from_ref(&h));
        assert_ne!(a, d);

        let e = SimpleRecord::new_from_slice(1, Some(b"k"), Some(b"V"), std::slice::from_ref(&h));
        assert_ne!(a, e);

        let f = SimpleRecord::new_from_slice(1, Some(b"k"), Some(b"v"), &[]);
        assert_ne!(a, f);
    }

    #[test]
    fn clone_is_cheap_bytes_share() {
        // Crude check that cloning shares storage rather than re-copying:
        // both clones must read the same bytes through the same address.
        let value = Bytes::from(vec![1u8, 2, 3, 4]);
        let r = SimpleRecord::with_value(0, Some(value));
        let r2 = r.clone();
        let p1 = r.value().unwrap().as_ptr();
        let p2 = r2.value().unwrap().as_ptr();
        assert_eq!(p1, p2);
    }

    #[test]
    fn new_with_bytes_is_zero_copy() {
        // The canonical zero-copy contract: passing a `Bytes` into `new`
        // must NOT copy. The constructed record's slice must alias the
        // input's backing storage (same pointer).
        let payload: Bytes = Bytes::from(vec![10u8, 20, 30, 40, 50]);
        let p_in = payload.as_ptr();
        let r = SimpleRecord::new(0, None, Some(payload), &[]);
        let p_out = r.value().unwrap().as_ptr();
        assert_eq!(p_in, p_out, "SimpleRecord::new with Some(Bytes) must alias the input — no copy",);
    }

    #[test]
    fn new_from_slice_copies() {
        // The convenience copying path: `new_from_slice` must NOT alias
        // the caller's stack slice (it produces a fresh allocation).
        let stack = [99u8, 100, 101];
        let p_in = stack.as_ptr();
        let r = SimpleRecord::new_from_slice(0, None, Some(&stack), &[]);
        let p_out = r.value().unwrap().as_ptr();
        assert_ne!(p_in, p_out);
        // Contents must still match.
        assert_eq!(r.value(), Some(stack.as_slice()));
    }

    #[test]
    fn debug_formats_byte_sizes() {
        let r = SimpleRecord::new_from_slice(7, Some(b"abc"), Some(b"de"), &[]);
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
