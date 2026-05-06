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

//! Translation of `org.apache.kafka.common.serialization.FloatDeserializer`.

use crate::common::KafkaError;
use crate::common::serialization::Deserializer;

/// Big-endian IEEE-754 32-bit deserializer for Java's `Float`. Mirrors
/// Java's `FloatDeserializer`. Uses `f32::from_bits` so NaN payloads
/// round-trip with the exact bit pattern preserved.
#[derive(Default, Debug, Clone, Copy)]
pub struct FloatDeserializer;

impl Deserializer<f32> for FloatDeserializer {
    fn deserialize(&self, _topic: &str, data: Option<&[u8]>) -> Result<Option<f32>, KafkaError> {
        match data {
            None => Ok(None),
            Some(d) => {
                if d.len() != 4 {
                    return Err(KafkaError::Serialization(
                        "Size of data received by Deserializer is not 4".to_string(),
                    ));
                }
                let bits = u32::from_be_bytes([d[0], d[1], d[2], d[3]]);
                Ok(Some(f32::from_bits(bits)))
            },
        }
    }
}
