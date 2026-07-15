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

//! A [`Deserializer`] that yields the raw record bytes as [`bytes::Bytes`].
//!
//! The C FFI consumer surface is typed `<Bytes, Bytes>`: it hands the caller
//! the untouched key/value bytes and lets the embedding language decode them.
//! `Bytes` is cheap to clone (refcounted), which satisfies the share
//! consumer's `K/V: Clone` bound used for RENEW record retention.

use bytes::Bytes;

use crate::common::KafkaError;
use crate::common::serialization::Deserializer;

/// Passes record bytes through untouched, wrapped in a [`Bytes`] buffer.
pub struct BytesDeserializer;

impl Deserializer<Bytes> for BytesDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Bytes, KafkaError> {
        Ok(Bytes::copy_from_slice(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_returns_the_input_bytes() {
        let result = BytesDeserializer.deserialize("topic", &[1, 2, 3]).unwrap();
        assert_eq!(result, Bytes::from_static(&[1, 2, 3]));
    }

    #[test]
    fn deserialize_empty_slice_yields_empty_bytes() {
        let result = BytesDeserializer.deserialize("topic", &[]).unwrap();
        assert!(result.is_empty());
    }
}
