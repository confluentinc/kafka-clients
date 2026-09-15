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

//! Zero-copy byte-buffer deserializer.
//!
//! Returns each record's key/value as a refcounted [`bytes::Bytes`] slice of
//! the owning fetch buffer rather than an owned `Vec<u8>` copy.

use bytes::Bytes;

use crate::common::Error;
use crate::common::header::Headers;
use crate::common::serialization::Deserializer;

/// Deserializes byte arrays into refcounted [`bytes::Bytes`].
///
/// This is the zero-copy counterpart to
/// [`ByteArrayDeserializer`](crate::common::serialization::ByteArrayDeserializer):
/// where the latter must copy the borrowed `&[u8]` into an owned `Vec<u8>`,
/// `BytesDeserializer` slices the owning fetch buffer
/// ([`deserialize_from_shared`](Deserializer::deserialize_from_shared)) so the
/// returned `Bytes` shares the single buffer that owns the whole fetch payload
/// (consumer-threading.md §27). No per-record key/value copy occurs on the
/// receive path.
///
/// The plain [`deserialize`](Deserializer::deserialize) entry point (used when
/// no shared source is available, e.g. unit tests) still copies via
/// [`Bytes::copy_from_slice`], matching the §27 "safe copy fallback" contract.
#[derive(Clone, Debug, Default)]
pub struct BytesDeserializer;

impl BytesDeserializer {
    /// Create a new `BytesDeserializer`.
    pub fn new() -> Self {
        Self
    }
}

impl Deserializer<Bytes> for BytesDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Bytes, Error> {
        // Fallback path: no shared owning buffer available, so copy.
        Ok(Bytes::copy_from_slice(data))
    }

    fn deserialize_from_shared(&self, _topic: &str, source: &Bytes, data: &[u8]) -> Result<Bytes, Error> {
        // Zero-copy: `data` is a subslice of `source` (the owning fetch /
        // decompression buffer), so `slice_ref` hands out a refcounted view
        // into the same allocation with no copy. `slice_ref` requires `data`
        // to lie within `source`'s allocation, which the receive path
        // guarantees (the key/value bytes are borrowed from `source`).
        Ok(source.slice_ref(data))
    }

    fn deserialize_from_shared_with_headers(
        &self,
        _topic: &str,
        _headers: &dyn Headers,
        source: &Bytes,
        data: &[u8],
    ) -> Result<Bytes, Error> {
        // ByteArray semantics ignore headers; keep the zero-copy slice path
        // (the trait default would route through the copying `deserialize`).
        Ok(source.slice_ref(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::header::internals::RecordHeaders;

    #[test]
    fn test_deserialize_copy_fallback() {
        let de = BytesDeserializer::new();
        let data = b"my bytes";
        let result = de.deserialize("topic", data.as_slice()).unwrap();
        assert_eq!(&result[..], b"my bytes");
    }

    #[test]
    fn test_deserialize_empty() {
        let de = BytesDeserializer::new();
        let result = de.deserialize("topic", &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_deserialize_from_shared_is_zero_copy() {
        let de = BytesDeserializer::new();
        let source = Bytes::from(vec![0u8, 1, 2, 3, 4, 5, 6, 7]);
        // A subslice borrowed from the source allocation.
        let data: &[u8] = &source[2..5];
        let result = de.deserialize_from_shared("topic", &source, data).unwrap();
        assert_eq!(&result[..], &[2, 3, 4]);
        // The returned Bytes shares the source allocation (same data ptr).
        assert_eq!(result.as_ptr(), data.as_ptr());
    }

    #[test]
    fn test_deserialize_from_shared_with_headers_delegates() {
        let de = BytesDeserializer::new();
        let source = Bytes::from(vec![9u8, 8, 7, 6]);
        let data: &[u8] = &source[1..3];
        let headers = RecordHeaders::new();
        let result = de
            .deserialize_from_shared_with_headers("topic", &headers, &source, data)
            .unwrap();
        assert_eq!(&result[..], &[8, 7]);
        assert_eq!(result.as_ptr(), data.as_ptr());
    }
}
