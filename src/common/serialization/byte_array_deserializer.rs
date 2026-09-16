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

//! Byte array deserializer.
//!
//! Passes through byte arrays unchanged.
//!
//! Corresponds to Java's `org.apache.kafka.common.serialization.ByteArrayDeserializer`.

use crate::common::Error;
use crate::common::serialization::Deserializer;

/// Deserializes byte arrays by returning the bytes unchanged.
///
/// Corresponds to Java's
/// `org.apache.kafka.common.serialization.ByteArrayDeserializer`, whose
/// `deserialize` returns the input `byte[]` as-is.
///
/// In Java the input array is returned by reference (no copy). In Rust the
/// [`Deserializer`] trait hands `deserialize` a `&[u8]` borrowed from the
/// `CompletedFetch` buffer (`consumer-threading.md` §27), so the only way to
/// return the owned `Vec<u8>` the API promises is to copy via
/// [`slice::to_vec`]. That single allocation is the user-deserializer's
/// budget per §27 and is unavoidable for an owned-`Vec<u8>` value type.
#[derive(Clone, Debug, Default)]
pub struct ByteArrayDeserializer;

impl ByteArrayDeserializer {
    /// Create a new `ByteArrayDeserializer`.
    pub fn new() -> Self {
        Self
    }
}

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_bytes() {
        let deserializer = ByteArrayDeserializer::new();
        let data = b"my bytes";
        let result = deserializer.deserialize("topic", data.as_slice()).unwrap();
        assert_eq!(result, b"my bytes".to_vec());
    }

    #[test]
    fn test_deserialize_empty() {
        let deserializer = ByteArrayDeserializer::new();
        let data: &[u8] = &[];
        let result = deserializer.deserialize("topic", data).unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }
}
