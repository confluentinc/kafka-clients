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

//! Translation of `org.apache.kafka.common.serialization.UUIDDeserializer`.

use std::collections::HashMap;

use uuid::Uuid;

use crate::common::KafkaError;
use crate::common::serialization::Deserializer;
use crate::common::serialization::string_serializer::StringEncoding;

/// Convert bytes to a `uuid::Uuid` by first decoding as a string, then
/// parsing via `Uuid::parse_str`. Mirrors Java's `UUIDDeserializer`.
///
/// Encoding defaults to UTF-8 and can be overridden via
/// `key.deserializer.encoding`, `value.deserializer.encoding`, or
/// `deserializer.encoding` (the last is the fallback).
#[derive(Debug, Clone, Copy)]
pub struct UUIDDeserializer {
    encoding: StringEncoding,
}

impl Default for UUIDDeserializer {
    fn default() -> Self {
        UUIDDeserializer { encoding: StringEncoding::Utf8 }
    }
}

impl UUIDDeserializer {
    /// Construct with the default UTF-8 encoding.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Deserializer<Uuid> for UUIDDeserializer {
    fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) -> Result<(), KafkaError> {
        let property_name = if is_key {
            "key.deserializer.encoding"
        } else {
            "value.deserializer.encoding"
        };
        let encoding_value = configs.get(property_name).or_else(|| configs.get("deserializer.encoding"));
        if let Some(name) = encoding_value {
            self.encoding = StringEncoding::for_name(name)?;
        }
        Ok(())
    }

    fn deserialize(&self, _topic: &str, data: Option<&[u8]>) -> Result<Option<Uuid>, KafkaError> {
        match data {
            None => Ok(None),
            Some(d) => {
                let s = self.encoding.decode(d)?;
                Uuid::parse_str(&s)
                    .map(Some)
                    .map_err(|e| KafkaError::Serialization(format!("Error parsing data into UUID: {e}")))
            },
        }
    }
}
