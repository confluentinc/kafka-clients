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

use std::borrow::Cow;

use crate::common::Error;
use crate::common::serialization::Serializer;

/// Serializes strings to UTF-8 encoded bytes.
///
/// In Java, `StringSerializer` supports configurable character encodings.
/// In Rust, all `String` and `str` values are guaranteed to be valid UTF-8,
/// so this serializer always uses UTF-8 encoding. A Rust string already is its
/// UTF-8 bytes, so the serialized bytes borrow it ([`Cow::Borrowed`]) and
/// nothing is allocated or copied (CLAUDE.md §14).
///
/// Corresponds to Java's `org.apache.kafka.common.serialization.StringSerializer`.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.serialization.StringSerializer")]
pub struct StringSerializer;

impl StringSerializer {
    /// Create a new `StringSerializer`.
    #[doc(alias = "org.apache.kafka.common.serialization.StringSerializer#StringSerializer")]
    pub fn new() -> Self {
        Self
    }
}

impl Serializer<str> for StringSerializer {
    async fn serialize<'a>(&'a self, _topic: &'a str, data: Option<&'a str>) -> Result<Option<Cow<'a, [u8]>>, Error> {
        Ok(data.map(|s| Cow::Borrowed(s.as_bytes())))
    }
}

impl Serializer<String> for StringSerializer {
    async fn serialize<'a>(
        &'a self,
        _topic: &'a str,
        data: Option<&'a String>,
    ) -> Result<Option<Cow<'a, [u8]>>, Error> {
        Ok(data.map(|s| Cow::Borrowed(s.as_bytes())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_serialize_null() {
        let serializer = StringSerializer::new();
        let result = Serializer::<str>::serialize(&serializer, "topic", None).await;
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_serialize_string() {
        let serializer = StringSerializer::new();
        let result = Serializer::<str>::serialize(&serializer, "topic", Some("my string"))
            .await
            .unwrap();
        assert_eq!(result.as_deref(), Some(&b"my string"[..]));
    }

    #[tokio::test]
    async fn test_serialize_owned_string() {
        let serializer = StringSerializer::new();
        let data = "my string".to_string();
        let result = Serializer::<String>::serialize(&serializer, "topic", Some(&data))
            .await
            .unwrap();
        assert_eq!(result.as_deref(), Some(&b"my string"[..]));
    }

    #[tokio::test]
    async fn test_serialize_empty_string() {
        let serializer = StringSerializer::new();
        let result = Serializer::<str>::serialize(&serializer, "topic", Some("")).await.unwrap();
        assert_eq!(result.as_deref(), Some(&[][..]));
    }

    #[tokio::test]
    async fn test_serialize_utf8() {
        let serializer = StringSerializer::new();
        let unicode_str = "\u{00e9}\u{00e8}\u{00ea}"; // accented chars
        let result = Serializer::<str>::serialize(&serializer, "topic", Some(unicode_str))
            .await
            .unwrap();
        assert_eq!(result.as_deref(), Some(unicode_str.as_bytes()));
    }

    /// The output borrows the string's own UTF-8 bytes: no allocation, no copy.
    #[tokio::test]
    async fn test_serialize_borrows_the_input() {
        let serializer = StringSerializer::new();
        let data = "borrowed".to_string();
        for result in [
            Serializer::<str>::serialize(&serializer, "topic", Some(data.as_str())).await,
            Serializer::<String>::serialize(&serializer, "topic", Some(&data)).await,
        ] {
            match result.unwrap() {
                Some(Cow::Borrowed(bytes)) => assert_eq!(data.as_ptr(), bytes.as_ptr()),
                other => panic!("expected borrowed bytes, got {other:?}"),
            }
        }
    }
}
