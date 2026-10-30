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
//! the buffer that owns the record's bytes rather than an owned `Vec<u8>`
//! copy.

use bytes::Bytes;

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::Deserializer;

/// Deserializes byte arrays into refcounted [`bytes::Bytes`].
///
/// This is the zero-copy counterpart to
/// [`ByteArrayDeserializer`](crate::common::serialization::ByteArrayDeserializer):
/// where the latter must copy the borrowed `&[u8]` into an owned `Vec<u8>`,
/// `BytesDeserializer` returns the record's key/value as the consumer hands it
/// to [`deserialize_with_headers_bytes`](Deserializer::deserialize_with_headers_bytes):
/// a refcounted slice of the buffer that owns the record's bytes
/// (consumer-threading.md §27) — the fetched records buffer for an uncompressed
/// batch, the buffer the record was decompressed into for a compressed one. No
/// per-record key/value copy occurs on the receive path.
///
/// The plain [`deserialize`](Deserializer::deserialize) entry point (used when
/// no shared buffer is available, e.g. unit tests) still copies via
/// [`Bytes::copy_from_slice`], matching the §27 "safe copy fallback" contract.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.serialization.BytesDeserializer")]
pub struct BytesDeserializer;

impl BytesDeserializer {
    /// Create a new `BytesDeserializer`.
    #[doc(alias = "org.apache.kafka.common.serialization.BytesDeserializer#BytesDeserializer")]
    pub fn new() -> Self {
        Self
    }
}

impl Deserializer<Bytes> for BytesDeserializer {
    async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Bytes, Error> {
        // Fallback path: no shared owning buffer available, so copy.
        Ok(Bytes::copy_from_slice(data))
    }

    async fn deserialize_with_headers_bytes(
        &self,
        _topic: &str,
        _headers: &mut RecordHeaders,
        data: &Bytes,
    ) -> Result<Bytes, Error> {
        // Zero-copy: `data` is already a refcounted slice of the buffer that
        // owns the record's bytes, so handing out a clone bumps a refcount and
        // copies nothing. Bytes semantics ignore headers.
        Ok(data.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_deserialize_copy_fallback() {
        let de = BytesDeserializer::new();
        let data = b"my bytes";
        let result = de.deserialize("topic", data.as_slice()).await.unwrap();
        assert_eq!(&result[..], b"my bytes");
        assert_ne!(
            result.as_ptr(),
            data.as_ptr(),
            "the plain form has no shared buffer to slice, so it copies"
        );
    }

    #[tokio::test]
    async fn test_deserialize_empty() {
        let de = BytesDeserializer::new();
        let result = de.deserialize("topic", &[]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_deserialize_with_headers_bytes_is_zero_copy() {
        let de = BytesDeserializer::new();
        let source = Bytes::from(vec![0u8, 1, 2, 3, 4, 5, 6, 7]);
        // A subslice of the source allocation, as the receive path passes it.
        let data = source.slice(2..5);
        let mut headers = RecordHeaders::new();
        let result = de.deserialize_with_headers_bytes("topic", &mut headers, &data).await.unwrap();
        assert_eq!(&result[..], &[2, 3, 4]);
        // The returned Bytes shares the source allocation (same data ptr).
        assert_eq!(result.as_ptr(), source[2..].as_ptr());
    }
}
