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

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::Serializer;

/// Serializes byte arrays by passing them through unchanged.
///
/// Corresponds to Java's `org.apache.kafka.common.serialization.ByteArraySerializer`.
#[derive(Clone, Debug, Default)]
pub struct ByteArraySerializer;

impl ByteArraySerializer {
    /// Create a new `ByteArraySerializer`.
    pub fn new() -> Self {
        Self
    }
}

impl Serializer<[u8]> for ByteArraySerializer {
    fn serialize(&self, _topic: &str, data: Option<&[u8]>) -> Result<Option<Vec<u8>>, Error> {
        Ok(data.map(|d| d.to_vec()))
    }
}

impl Serializer<Vec<u8>> for ByteArraySerializer {
    fn serialize(&self, _topic: &str, data: Option<&Vec<u8>>) -> Result<Option<Vec<u8>>, Error> {
        Ok(data.cloned())
    }

    fn serialize_owned_headers(
        &self,
        _topic: &str,
        _headers: &RecordHeaders,
        data: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, Error> {
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_null() {
        let serializer = ByteArraySerializer::new();
        let result: Result<Option<Vec<u8>>, Error> = Serializer::<[u8]>::serialize(&serializer, "topic", None);
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_serialize_bytes() {
        let serializer = ByteArraySerializer::new();
        let data = b"my bytes";
        let result = Serializer::<[u8]>::serialize(&serializer, "topic", Some(data.as_slice())).unwrap();
        assert_eq!(result, Some(b"my bytes".to_vec()));
    }

    #[test]
    fn test_serialize_vec() {
        let serializer = ByteArraySerializer::new();
        let data = vec![1u8, 2, 3, 4, 5];
        let result = Serializer::<Vec<u8>>::serialize(&serializer, "topic", Some(&data)).unwrap();
        assert_eq!(result, Some(vec![1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_serialize_empty() {
        let serializer = ByteArraySerializer::new();
        let data: &[u8] = &[];
        let result = Serializer::<[u8]>::serialize(&serializer, "topic", Some(data)).unwrap();
        assert_eq!(result, Some(Vec::new()));
    }
}
