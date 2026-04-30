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

//! Translation of `org.apache.kafka.common.serialization.ShortDeserializer`.

use crate::common::KafkaError;
use crate::common::serialization::Deserializer;

/// Big-endian 2-byte deserializer for Java's `Short`. Mirrors Java's
/// `ShortDeserializer`. Returns `KafkaError::Serialization` when the
/// payload is not exactly 2 bytes.
#[derive(Default, Debug, Clone, Copy)]
pub struct ShortDeserializer;

impl Deserializer<i16> for ShortDeserializer {
    fn deserialize(&self, _topic: &str, data: Option<&[u8]>) -> Result<Option<i16>, KafkaError> {
        match data {
            None => Ok(None),
            Some(d) => {
                if d.len() != 2 {
                    return Err(KafkaError::Serialization(
                        "Size of data received by ShortDeserializer is not 2".to_string(),
                    ));
                }
                Ok(Some(i16::from_be_bytes([d[0], d[1]])))
            },
        }
    }
}
