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

//! Translation of `org.apache.kafka.common.serialization.StringDeserializer`.

use std::collections::HashMap;

use crate::common::KafkaError;
use crate::common::serialization::Deserializer;
use crate::common::serialization::string_serializer::StringEncoding;

/// String encoding defaults to UTF-8 and can be customized by setting the
/// property `key.deserializer.encoding`, `value.deserializer.encoding` or
/// `deserializer.encoding`. The first two take precedence over the last.
#[derive(Debug, Clone, Copy)]
pub struct StringDeserializer {
    encoding: StringEncoding,
}

impl Default for StringDeserializer {
    fn default() -> Self {
        StringDeserializer { encoding: StringEncoding::Utf8 }
    }
}

impl StringDeserializer {
    /// Construct with the default UTF-8 encoding.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct with a specific encoding.
    pub fn with_encoding(encoding: StringEncoding) -> Self {
        StringDeserializer { encoding }
    }
}

impl Deserializer<String> for StringDeserializer {
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

    fn deserialize(&self, _topic: &str, data: Option<&[u8]>) -> Result<Option<String>, KafkaError> {
        match data {
            Some(bytes) => Ok(Some(self.encoding.decode(bytes)?)),
            None => Ok(None),
        }
    }
}
