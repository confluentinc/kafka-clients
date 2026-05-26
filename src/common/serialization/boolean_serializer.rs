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

//! Translation of `org.apache.kafka.common.serialization.BooleanSerializer`.

use crate::common::KafkaError;
use crate::common::serialization::Serializer;

const TRUE: u8 = 0x01;
const FALSE: u8 = 0x00;

/// 1-byte serializer for Java's `Boolean`. Mirrors Java's `BooleanSerializer`.
#[derive(Default, Debug, Clone, Copy)]
pub struct BooleanSerializer;

impl Serializer<bool> for BooleanSerializer {
    fn serialize(&self, _topic: &str, data: Option<&bool>) -> Result<Option<Vec<u8>>, KafkaError> {
        Ok(data.map(|v| vec![if *v { TRUE } else { FALSE }]))
    }

    fn serialize_to(&self, _topic: &str, data: Option<&bool>, out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        match data {
            Some(v) => {
                out.push(if *v { TRUE } else { FALSE });
                Ok(true)
            },
            None => Ok(false),
        }
    }
}
