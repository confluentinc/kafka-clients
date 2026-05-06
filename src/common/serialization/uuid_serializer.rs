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

//! Translation of `org.apache.kafka.common.serialization.UUIDSerializer`.

use std::collections::HashMap;

use uuid::Uuid;

use crate::common::KafkaError;
use crate::common::serialization::Serializer;
use crate::common::serialization::string_serializer::StringEncoding;

/// Convert a `uuid::Uuid` to bytes by first formatting it as a string
/// (the standard 36-char hyphenated form, `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`),
/// then encoding the string. Mirrors Java's `UUIDSerializer`, which serializes
/// `java.util.UUID` — Kafka's own `org.apache.kafka.common.Uuid` (which is
/// base64-encoded) is a separate type and is not the subject of this serializer.
///
/// Encoding defaults to UTF-8 and can be overridden via
/// `key.serializer.encoding`, `value.serializer.encoding`, or
/// `serializer.encoding` (the last is the fallback).
#[derive(Debug, Clone, Copy)]
pub struct UUIDSerializer {
    encoding: StringEncoding,
}

impl Default for UUIDSerializer {
    fn default() -> Self {
        UUIDSerializer { encoding: StringEncoding::Utf8 }
    }
}

impl UUIDSerializer {
    /// Construct with the default UTF-8 encoding.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Serializer<Uuid> for UUIDSerializer {
    fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) -> Result<(), KafkaError> {
        let property_name = if is_key {
            "key.serializer.encoding"
        } else {
            "value.serializer.encoding"
        };
        let encoding_value = configs.get(property_name).or_else(|| configs.get("serializer.encoding"));
        if let Some(name) = encoding_value {
            self.encoding = StringEncoding::for_name(name)?;
        }
        Ok(())
    }

    fn serialize(&self, _topic: &str, data: Option<&Uuid>) -> Result<Option<Vec<u8>>, KafkaError> {
        Ok(data.map(|u| self.encoding.encode(&u.hyphenated().to_string())))
    }

    fn serialize_to(&self, _topic: &str, data: Option<&Uuid>, out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        match data {
            Some(u) => {
                self.encoding.encode_to(&u.hyphenated().to_string(), out);
                Ok(true)
            },
            None => Ok(false),
        }
    }
}
