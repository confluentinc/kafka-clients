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

//! String serializer.
//!
//! String encoding defaults to UTF-8. Unlike the Java implementation which
//! supports configurable charsets, Rust strings are always valid UTF-8, so
//! this serializer simply converts to UTF-8 bytes.
//!
//! Corresponds to Java's `org.apache.kafka.common.serialization.StringSerializer`.

use crate::common::Error;
use crate::common::serialization::Serializer;

/// Serializes strings to UTF-8 encoded bytes.
///
/// In Java, `StringSerializer` supports configurable character encodings.
/// In Rust, all `String` and `str` values are guaranteed to be valid UTF-8,
/// so this serializer always uses UTF-8 encoding.
///
/// Corresponds to Java's `org.apache.kafka.common.serialization.StringSerializer`.
#[derive(Clone, Debug, Default)]
pub struct StringSerializer;

impl StringSerializer {
    /// Create a new `StringSerializer`.
    pub fn new() -> Self {
        Self
    }
}

impl Serializer<str> for StringSerializer {
    fn serialize(&self, _topic: &str, data: Option<&str>) -> Result<Option<Vec<u8>>, Error> {
        Ok(data.map(|s| s.as_bytes().to_vec()))
    }
}

impl Serializer<String> for StringSerializer {
    fn serialize(&self, _topic: &str, data: Option<&String>) -> Result<Option<Vec<u8>>, Error> {
        Ok(data.map(|s| s.as_bytes().to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_null() {
        let serializer = StringSerializer::new();
        let result: Result<Option<Vec<u8>>, Error> = Serializer::<str>::serialize(&serializer, "topic", None);
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_serialize_string() {
        let serializer = StringSerializer::new();
        let result = Serializer::<str>::serialize(&serializer, "topic", Some("my string")).unwrap();
        assert_eq!(result, Some(b"my string".to_vec()));
    }

    #[test]
    fn test_serialize_owned_string() {
        let serializer = StringSerializer::new();
        let data = "my string".to_string();
        let result = Serializer::<String>::serialize(&serializer, "topic", Some(&data)).unwrap();
        assert_eq!(result, Some(b"my string".to_vec()));
    }

    #[test]
    fn test_serialize_empty_string() {
        let serializer = StringSerializer::new();
        let result = Serializer::<str>::serialize(&serializer, "topic", Some("")).unwrap();
        assert_eq!(result, Some(Vec::new()));
    }

    #[test]
    fn test_serialize_utf8() {
        let serializer = StringSerializer::new();
        let unicode_str = "\u{00e9}\u{00e8}\u{00ea}"; // accented chars
        let result = Serializer::<str>::serialize(&serializer, "topic", Some(unicode_str)).unwrap();
        assert_eq!(result, Some(unicode_str.as_bytes().to_vec()));
    }
}
