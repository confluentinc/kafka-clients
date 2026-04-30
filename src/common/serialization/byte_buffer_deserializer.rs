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

//! Translation of `org.apache.kafka.common.serialization.ByteBufferDeserializer`.

use crate::common::KafkaError;
use crate::common::serialization::Deserializer;

/// Identity deserializer for `Vec<u8>`. Mirrors Java's
/// `ByteBufferDeserializer` — wraps the input bytes into a buffer-typed
/// container. See [`super::ByteBufferSerializer`] for why we use
/// `Vec<u8>` as the Rust analog of `java.nio.ByteBuffer`.
#[derive(Default, Debug, Clone, Copy)]
pub struct ByteBufferDeserializer;

impl Deserializer<Vec<u8>> for ByteBufferDeserializer {
    fn deserialize(&self, _topic: &str, data: Option<&[u8]>) -> Result<Option<Vec<u8>>, KafkaError> {
        Ok(data.map(<[u8]>::to_vec))
    }
}
