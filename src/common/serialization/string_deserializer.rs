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

//! String deserializer.
//!
//! String encoding defaults to UTF-8. Unlike the Java implementation which
//! supports configurable charsets (via `key.deserializer.encoding` /
//! `value.deserializer.encoding` / `deserializer.encoding`), this deserializer
//! always decodes as UTF-8.
//!
//! Corresponds to Java's `org.apache.kafka.common.serialization.StringDeserializer`.

use crate::common::Error;
use crate::common::serialization::Deserializer;

/// Deserializes UTF-8 encoded bytes to strings.
///
/// In Java, `StringDeserializer` supports configurable character encodings.
/// In Rust, this deserializer always uses UTF-8: it mirrors Java's
/// `new String(data, StandardCharsets.UTF_8)`, which replaces malformed byte
/// sequences with the Unicode replacement character (U+FFFD) rather than
/// failing — i.e. a lossy UTF-8 decode.
///
/// Corresponds to Java's `org.apache.kafka.common.serialization.StringDeserializer`.
#[derive(Clone, Debug, Default)]
pub struct StringDeserializer;

impl StringDeserializer {
    /// Create a new `StringDeserializer`.
    pub fn new() -> Self {
        Self
    }
}

impl Deserializer<String> for StringDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
        // Java's `new String(data, UTF_8)` replaces malformed sequences with
        // U+FFFD; `from_utf8_lossy` reproduces that behavior exactly.
        Ok(String::from_utf8_lossy(data).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::serialization::Serializer;
    use crate::common::serialization::StringSerializer;

    #[test]
    fn test_deserialize_string() {
        let deserializer = StringDeserializer::new();
        let result = deserializer.deserialize("topic", b"my string").unwrap();
        assert_eq!(result, "my string".to_string());
    }

    #[test]
    fn test_deserialize_empty() {
        let deserializer = StringDeserializer::new();
        let result = deserializer.deserialize("topic", b"").unwrap();
        assert_eq!(result, String::new());
    }

    #[test]
    fn test_deserialize_utf8() {
        let deserializer = StringDeserializer::new();
        let unicode_str = "\u{00e9}\u{00e8}\u{00ea}"; // accented chars
        let result = deserializer.deserialize("topic", unicode_str.as_bytes()).unwrap();
        assert_eq!(result, unicode_str.to_string());
    }

    #[test]
    fn test_deserialize_malformed_is_lossy() {
        // Java's `new String(bytes, UTF_8)` replaces malformed bytes with
        // U+FFFD instead of throwing; the Rust translation matches.
        let deserializer = StringDeserializer::new();
        let result = deserializer.deserialize("topic", &[0xff, 0xfe]).unwrap();
        assert_eq!(result, "\u{fffd}\u{fffd}".to_string());
    }

    #[test]
    fn test_serialize_deserialize_round_trip() {
        let serializer = StringSerializer::new();
        let deserializer = StringDeserializer::new();
        let original = "round trip \u{00e9}";
        let bytes = Serializer::<str>::serialize(&serializer, "topic", Some(original))
            .unwrap()
            .unwrap();
        let decoded = deserializer.deserialize("topic", &bytes).unwrap();
        assert_eq!(decoded, original.to_string());
    }
}
