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

//! Translation of `org.apache.kafka.common.serialization.BytesSerializer`.

use bytes::Bytes;

use crate::common::KafkaError;
use crate::common::serialization::Serializer;

/// Identity serializer for [`bytes::Bytes`]. Mirrors Java's
/// `BytesSerializer implements Serializer<Bytes>`.
///
/// Java's `org.apache.kafka.common.utils.Bytes` is a wrapper around
/// `byte[]`. Per CLAUDE.md rule 1 we use the canonical Rust analog
/// `bytes::Bytes` (already a direct dep) instead of translating Java's
/// `Bytes` class — `bytes::Bytes` already provides cheap shared ownership,
/// equality, and hashing over a refcounted backing buffer.
#[derive(Default, Debug, Clone, Copy)]
pub struct BytesSerializer;

impl Serializer<Bytes> for BytesSerializer {
    fn serialize(&self, _topic: &str, data: Option<&Bytes>) -> Result<Option<Vec<u8>>, KafkaError> {
        Ok(data.map(|b| b.to_vec()))
    }

    fn serialize_to(&self, _topic: &str, data: Option<&Bytes>, out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        match data {
            Some(b) => {
                out.extend_from_slice(b);
                Ok(true)
            },
            None => Ok(false),
        }
    }
}
