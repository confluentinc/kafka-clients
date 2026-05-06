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

//! Translation of `org.apache.kafka.common.serialization.VoidDeserializer`.

use crate::common::KafkaError;
use crate::common::serialization::Deserializer;

/// Deserializer that requires `null` data and always returns `None`.
/// Mirrors Java's `VoidDeserializer` — non-null input throws
/// `IllegalArgumentException`. We map that to
/// `KafkaError::Serialization` (closest equivalent for client-side
/// "you handed me bad input"; CLAUDE.md rule 10 — return `Result`, don't
/// `panic!` for caller-recoverable errors).
#[derive(Default, Debug, Clone, Copy)]
pub struct VoidDeserializer;

impl Deserializer<()> for VoidDeserializer {
    fn deserialize(&self, _topic: &str, data: Option<&[u8]>) -> Result<Option<()>, KafkaError> {
        if data.is_some() {
            return Err(KafkaError::Serialization(
                "Data should be null for a VoidDeserializer.".to_string(),
            ));
        }
        Ok(None)
    }
}
