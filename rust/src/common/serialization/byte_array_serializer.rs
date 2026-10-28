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

//! Byte array serializer.
//!
//! Passes through byte arrays unchanged.
//!
//! Corresponds to Java's `org.apache.kafka.common.serialization.ByteArraySerializer`.

use std::borrow::Cow;

use crate::common::Error;
use crate::common::serialization::Serializer;

/// Serializes byte arrays by passing them through unchanged.
///
/// The serialized bytes borrow the input ([`Cow::Borrowed`]): Java returns the
/// input `byte[]` itself, and this neither allocates nor copies.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.serialization.ByteArraySerializer")]
pub struct ByteArraySerializer;

impl ByteArraySerializer {
    /// Create a new `ByteArraySerializer`.
    #[doc(alias = "org.apache.kafka.common.serialization.ByteArraySerializer#ByteArraySerializer")]
    pub fn new() -> Self {
        Self
    }
}

impl Serializer<[u8]> for ByteArraySerializer {
    async fn serialize<'a>(&'a self, _topic: &'a str, data: Option<&'a [u8]>) -> Result<Option<Cow<'a, [u8]>>, Error> {
        Ok(data.map(Cow::Borrowed))
    }
}

impl Serializer<Vec<u8>> for ByteArraySerializer {
    async fn serialize<'a>(
        &'a self,
        _topic: &'a str,
        data: Option<&'a Vec<u8>>,
    ) -> Result<Option<Cow<'a, [u8]>>, Error> {
        Ok(data.map(|d| Cow::Borrowed(d.as_slice())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::header::RecordHeaders;

    #[tokio::test]
    async fn test_serialize_null() {
        let serializer = ByteArraySerializer::new();
        let result = Serializer::<[u8]>::serialize(&serializer, "topic", None).await;
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_serialize_bytes() {
        let serializer = ByteArraySerializer::new();
        let data = b"my bytes";
        let result = Serializer::<[u8]>::serialize(&serializer, "topic", Some(data.as_slice()))
            .await
            .unwrap();
        assert_eq!(result.as_deref(), Some(&b"my bytes"[..]));
    }

    #[tokio::test]
    async fn test_serialize_vec() {
        let serializer = ByteArraySerializer::new();
        let data = vec![1u8, 2, 3, 4, 5];
        let result = Serializer::<Vec<u8>>::serialize(&serializer, "topic", Some(&data))
            .await
            .unwrap();
        assert_eq!(result.as_deref(), Some(&[1u8, 2, 3, 4, 5][..]));
    }

    #[tokio::test]
    async fn test_serialize_empty() {
        let serializer = ByteArraySerializer::new();
        let data: &[u8] = &[];
        let result = Serializer::<[u8]>::serialize(&serializer, "topic", Some(data)).await.unwrap();
        assert_eq!(result.as_deref(), Some(&[][..]));
    }

    /// The output borrows the input: no allocation and no copy, for both the
    /// slice and the `Vec<u8>` form, with and without headers.
    #[tokio::test]
    async fn test_serialize_borrows_the_input() {
        let serializer = ByteArraySerializer::new();
        let mut headers = RecordHeaders::new();
        let data = vec![7u8, 8, 9];
        for result in [
            Serializer::<[u8]>::serialize(&serializer, "topic", Some(&data[..])).await,
            Serializer::<[u8]>::serialize_with_headers(&serializer, "topic", &mut headers, Some(&data[..])).await,
            Serializer::<Vec<u8>>::serialize(&serializer, "topic", Some(&data)).await,
            Serializer::<Vec<u8>>::serialize_with_headers(&serializer, "topic", &mut headers, Some(&data)).await,
        ] {
            match result.unwrap() {
                Some(Cow::Borrowed(bytes)) => assert_eq!(data.as_ptr(), bytes.as_ptr()),
                other => panic!("expected borrowed bytes, got {other:?}"),
            }
        }
    }
}
