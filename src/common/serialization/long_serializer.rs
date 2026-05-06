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

//! Translation of `org.apache.kafka.common.serialization.LongSerializer`.

use crate::common::KafkaError;
use crate::common::serialization::Serializer;

/// Big-endian 8-byte serializer for Java's `Long` (64-bit signed). Mirrors
/// Java's `LongSerializer`.
#[derive(Default, Debug, Clone, Copy)]
pub struct LongSerializer;

impl Serializer<i64> for LongSerializer {
    fn serialize(&self, _topic: &str, data: Option<&i64>) -> Result<Option<Vec<u8>>, KafkaError> {
        Ok(data.map(|v| v.to_be_bytes().to_vec()))
    }

    fn serialize_to(&self, _topic: &str, data: Option<&i64>, out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        match data {
            Some(v) => {
                out.extend_from_slice(&v.to_be_bytes());
                Ok(true)
            },
            None => Ok(false),
        }
    }
}
