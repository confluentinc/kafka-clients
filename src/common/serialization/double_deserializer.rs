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

//! Translation of `org.apache.kafka.common.serialization.DoubleDeserializer`.

use crate::common::KafkaError;
use crate::common::serialization::Deserializer;

/// Big-endian IEEE-754 64-bit deserializer for Java's `Double`. Mirrors
/// Java's `DoubleDeserializer`.
#[derive(Default, Debug, Clone, Copy)]
pub struct DoubleDeserializer;

impl Deserializer<f64> for DoubleDeserializer {
    fn deserialize(&self, _topic: &str, data: Option<&[u8]>) -> Result<Option<f64>, KafkaError> {
        match data {
            None => Ok(None),
            Some(d) => {
                if d.len() != 8 {
                    return Err(KafkaError::Serialization(
                        "Size of data received by Deserializer is not 8".to_string(),
                    ));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(d);
                Ok(Some(f64::from_bits(u64::from_be_bytes(buf))))
            },
        }
    }
}
