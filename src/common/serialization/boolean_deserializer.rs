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

//! Translation of `org.apache.kafka.common.serialization.BooleanDeserializer`.

use crate::common::KafkaError;
use crate::common::serialization::Deserializer;

const TRUE: u8 = 0x01;
const FALSE: u8 = 0x00;

/// 1-byte deserializer for Java's `Boolean`. Mirrors Java's
/// `BooleanDeserializer`. Returns `KafkaError::Serialization` when the
/// payload is not exactly 1 byte or when the byte is not `0x00`/`0x01`.
#[derive(Default, Debug, Clone, Copy)]
pub struct BooleanDeserializer;

impl Deserializer<bool> for BooleanDeserializer {
    fn deserialize(&self, _topic: &str, data: Option<&[u8]>) -> Result<Option<bool>, KafkaError> {
        match data {
            None => Ok(None),
            Some(d) => {
                if d.len() != 1 {
                    return Err(KafkaError::Serialization(
                        "Size of data received by BooleanDeserializer is not 1".to_string(),
                    ));
                }
                match d[0] {
                    TRUE => Ok(Some(true)),
                    FALSE => Ok(Some(false)),
                    other => Err(KafkaError::Serialization(format!(
                        "Unexpected byte received by BooleanDeserializer: {other}"
                    ))),
                }
            },
        }
    }
}
