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

//! Translation of `org.apache.kafka.common.serialization.StringSerializer`.

use std::collections::HashMap;

use crate::common::KafkaError;
use crate::common::serialization::Serializer;

/// Encodings supported by [`StringSerializer`]. Matches the subset of
/// `java.nio.charset.Charset` named values that the producer client
/// actually exercises (UTF-8 default, plus UTF-16 BE and UTF-16 LE).
///
/// Java accepts the full JDK charset registry via `Charset.forName(String)`;
/// Rust does not have a builtin charset registry. We support the encodings
/// the SerializationTest exercises (UTF-8, UTF-16). Unknown encoding names
/// produce `KafkaError::Serialization`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringEncoding {
    /// `UTF-8` (default).
    Utf8,
    /// `UTF-16` — Java's `UTF-16` is BE with a leading BOM (`Charset.forName("UTF-16")`).
    Utf16,
    /// `UTF-16BE` — big-endian without BOM.
    Utf16Be,
    /// `UTF-16LE` — little-endian without BOM.
    Utf16Le,
}

impl StringEncoding {
    /// Look up an encoding by Java charset name. Case-insensitive, mirrors
    /// `Charset.forName` for the supported set.
    pub fn for_name(name: &str) -> Result<Self, KafkaError> {
        // Java's Charset.forName matches case-insensitively after canonical
        // alias resolution; we replicate the canonical names + common aliases
        // for the encodings the producer client actually uses.
        let upper = name.to_ascii_uppercase();
        match upper.as_str() {
            "UTF-8" | "UTF8" => Ok(StringEncoding::Utf8),
            "UTF-16" | "UTF16" => Ok(StringEncoding::Utf16),
            "UTF-16BE" | "UTF16BE" => Ok(StringEncoding::Utf16Be),
            "UTF-16LE" | "UTF16LE" => Ok(StringEncoding::Utf16Le),
            _ => Err(KafkaError::Serialization(format!("Unsupported encoding {name}"))),
        }
    }

    /// Encode a string slice into bytes using this encoding.
    pub fn encode(self, s: &str) -> Vec<u8> {
        match self {
            StringEncoding::Utf8 => s.as_bytes().to_vec(),
            StringEncoding::Utf16 => {
                // Java's "UTF-16" is BE with a leading BOM (0xFE 0xFF).
                let mut out: Vec<u8> = Vec::with_capacity(2 + s.encode_utf16().count() * 2);
                out.extend_from_slice(&[0xFE, 0xFF]);
                for u in s.encode_utf16() {
                    out.extend_from_slice(&u.to_be_bytes());
                }
                out
            },
            StringEncoding::Utf16Be => {
                let mut out: Vec<u8> = Vec::with_capacity(s.encode_utf16().count() * 2);
                for u in s.encode_utf16() {
                    out.extend_from_slice(&u.to_be_bytes());
                }
                out
            },
            StringEncoding::Utf16Le => {
                let mut out: Vec<u8> = Vec::with_capacity(s.encode_utf16().count() * 2);
                for u in s.encode_utf16() {
                    out.extend_from_slice(&u.to_le_bytes());
                }
                out
            },
        }
    }

    /// Encode a string slice directly into the given buffer using this
    /// encoding. Equivalent to `out.extend_from_slice(&self.encode(s))` but
    /// avoids the intermediate allocation for the UTF-8 path.
    pub fn encode_to(self, s: &str, out: &mut Vec<u8>) {
        match self {
            StringEncoding::Utf8 => out.extend_from_slice(s.as_bytes()),
            StringEncoding::Utf16 => {
                out.extend_from_slice(&[0xFE, 0xFF]);
                for u in s.encode_utf16() {
                    out.extend_from_slice(&u.to_be_bytes());
                }
            },
            StringEncoding::Utf16Be => {
                for u in s.encode_utf16() {
                    out.extend_from_slice(&u.to_be_bytes());
                }
            },
            StringEncoding::Utf16Le => {
                for u in s.encode_utf16() {
                    out.extend_from_slice(&u.to_le_bytes());
                }
            },
        }
    }

    /// Decode bytes into a `String`. UTF-8 path is zero-copy when the bytes
    /// are valid UTF-8 (one allocation for the `String` itself).
    pub fn decode(self, data: &[u8]) -> Result<String, KafkaError> {
        match self {
            StringEncoding::Utf8 => String::from_utf8(data.to_vec())
                .map_err(|e| KafkaError::Serialization(format!("Error decoding bytes as UTF-8: {e}"))),
            StringEncoding::Utf16 => {
                // Java's "UTF-16" reads a BOM. Strip it if present.
                let (bytes, big_endian) = if data.len() >= 2 && data[0] == 0xFE && data[1] == 0xFF {
                    (&data[2..], true)
                } else if data.len() >= 2 && data[0] == 0xFF && data[1] == 0xFE {
                    (&data[2..], false)
                } else {
                    // No BOM: Java's UTF-16 charset defaults to BE.
                    (data, true)
                };
                decode_utf16_pairs(bytes, big_endian)
            },
            StringEncoding::Utf16Be => decode_utf16_pairs(data, true),
            StringEncoding::Utf16Le => decode_utf16_pairs(data, false),
        }
    }
}

fn decode_utf16_pairs(data: &[u8], big_endian: bool) -> Result<String, KafkaError> {
    if !data.len().is_multiple_of(2) {
        return Err(KafkaError::Serialization("UTF-16 byte stream has odd length".to_string()));
    }
    let units: Vec<u16> = data
        .chunks_exact(2)
        .map(|c| {
            if big_endian {
                u16::from_be_bytes([c[0], c[1]])
            } else {
                u16::from_le_bytes([c[0], c[1]])
            }
        })
        .collect();
    String::from_utf16(&units).map_err(|e| KafkaError::Serialization(format!("Error decoding bytes as UTF-16: {e}")))
}

/// String encoding defaults to UTF-8 and can be customized by setting the
/// property `key.serializer.encoding`, `value.serializer.encoding` or
/// `serializer.encoding`. The first two take precedence over the last.
#[derive(Debug, Clone, Copy)]
pub struct StringSerializer {
    encoding: StringEncoding,
}

impl Default for StringSerializer {
    fn default() -> Self {
        StringSerializer { encoding: StringEncoding::Utf8 }
    }
}

impl StringSerializer {
    /// Construct with the default UTF-8 encoding.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct with a specific encoding.
    pub fn with_encoding(encoding: StringEncoding) -> Self {
        StringSerializer { encoding }
    }
}

impl Serializer<str> for StringSerializer {
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

    fn serialize(&self, _topic: &str, data: Option<&str>) -> Result<Option<Vec<u8>>, KafkaError> {
        Ok(data.map(|s| self.encoding.encode(s)))
    }

    fn serialize_to(&self, _topic: &str, data: Option<&str>, out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        match data {
            Some(s) => {
                self.encoding.encode_to(s, out);
                Ok(true)
            },
            None => Ok(false),
        }
    }
}
