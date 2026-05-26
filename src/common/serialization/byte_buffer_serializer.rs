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

//! Translation of `org.apache.kafka.common.serialization.ByteBufferSerializer`.

use crate::common::KafkaError;
use crate::common::serialization::Serializer;

/// Identity serializer for `Vec<u8>`. Mirrors Java's `ByteBufferSerializer`.
///
/// Java's `ByteBuffer` carries position/limit cursors and the serializer
/// rewinds the buffer to extract the populated payload. Rust does not have
/// a stdlib `ByteBuffer`; the closest analog for "owned mutable byte
/// container" is `Vec<u8>`. We therefore implement
/// `Serializer<Vec<u8>>` — the caller is expected to pass a `Vec<u8>` whose
/// length is the populated size (mirroring Java's `ByteBuffer.flip()` +
/// `array()` discipline).
#[derive(Default, Debug, Clone, Copy)]
pub struct ByteBufferSerializer;

impl Serializer<Vec<u8>> for ByteBufferSerializer {
    fn serialize(&self, _topic: &str, data: Option<&Vec<u8>>) -> Result<Option<Vec<u8>>, KafkaError> {
        Ok(data.cloned())
    }

    fn serialize_to(&self, _topic: &str, data: Option<&Vec<u8>>, out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        match data {
            Some(v) => {
                out.extend_from_slice(v);
                Ok(true)
            },
            None => Ok(false),
        }
    }
}
