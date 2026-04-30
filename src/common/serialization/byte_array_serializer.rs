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

//! Translation of `org.apache.kafka.common.serialization.ByteArraySerializer`.

use crate::common::KafkaError;
use crate::common::serialization::Serializer;

/// Identity serializer for `[u8]`. Mirrors Java's
/// `ByteArraySerializer implements Serializer<byte[]>`.
///
/// Hot-path zero-copy: [`Serializer::serialize_to`] does **not** allocate;
/// it writes directly into the caller's buffer. The owned-`Vec<u8>` API
/// (`serialize`) does allocate (`to_vec`) — that path is for callers who
/// genuinely need an owned copy.
#[derive(Default, Debug, Clone, Copy)]
pub struct ByteArraySerializer;

impl Serializer<[u8]> for ByteArraySerializer {
    fn serialize(&self, _topic: &str, data: Option<&[u8]>) -> Result<Option<Vec<u8>>, KafkaError> {
        Ok(data.map(<[u8]>::to_vec))
    }

    fn serialize_to(&self, _topic: &str, data: Option<&[u8]>, out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        match data {
            Some(d) => {
                out.extend_from_slice(d);
                Ok(true)
            },
            None => Ok(false),
        }
    }
}
