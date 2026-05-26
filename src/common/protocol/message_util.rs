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

//! Translation of `org.apache.kafka.common.protocol.MessageUtil`.
//!
//! The Java class is a static helper bag. Per CLAUDE.md "static functions
//! must be exported only by the file defining them", we keep these as
//! free functions in this module — callers should use the path
//! `crate::common::protocol::message_util::*` rather than re-importing
//! through `protocol::mod`.

use crate::common::errors::KafkaError;
use crate::common::protocol::types::RawTaggedField;
use crate::common::protocol::{ApiMessage, ByteBufferAccessor, Message, ObjectSerializationCache, Writable};

/// Mirrors `MessageUtil.UNSIGNED_INT_MAX`.
pub const UNSIGNED_INT_MAX: i64 = 4_294_967_295;

/// Mirrors `MessageUtil.UNSIGNED_SHORT_MAX`.
pub const UNSIGNED_SHORT_MAX: i32 = 65_535;

/// Copy a byte slice into a freshly allocated `Vec`. Mirrors
/// `byteBufferToArray`. Java's version reads the buffer's *remaining* bytes
/// without changing position; in Rust the input is already a slice so we
/// simply copy it.
pub fn byte_buffer_to_array(buf: &[u8]) -> Vec<u8> {
    buf.to_vec()
}

/// Mirrors `MessageUtil.deepToString(Iterator)`. Renders an iterable as
/// `[a, b, c]` with `to_string()` on each element.
pub fn deep_to_string<I, T>(iter: I) -> String
where
    I: IntoIterator<Item = T>,
    T: std::fmt::Display,
{
    let mut s = String::from("[");
    let mut first = true;
    for item in iter {
        if !first {
            s.push_str(", ");
        }
        s.push_str(&item.to_string());
        first = false;
    }
    s.push(']');
    s
}

/// Mirrors `MessageUtil.duplicate(byte[])`. Returns `None` for `None`,
/// otherwise an owned copy.
pub fn duplicate(array: Option<&[u8]>) -> Option<Vec<u8>> {
    array.map(|s| s.to_vec())
}

/// Mirrors `MessageUtil.compareRawTaggedFields(List, List)`. A `None` list is
/// equivalent to an empty one for comparison purposes.
pub fn compare_raw_tagged_fields(first: Option<&[RawTaggedField]>, second: Option<&[RawTaggedField]>) -> bool {
    match (first, second) {
        (None, None) => true,
        (None, Some(s)) => s.is_empty(),
        (Some(f), None) => f.is_empty(),
        (Some(f), Some(s)) => f == s,
    }
}

/// Serialize `message` at `version` into a [`ByteBufferAccessor`] in read
/// mode. Mirrors `MessageUtil.toByteBufferAccessor(Message, short)`.
pub fn to_byte_buffer_accessor<M: Message + ?Sized>(
    message: &M,
    version: i16,
) -> Result<ByteBufferAccessor, KafkaError> {
    let mut cache = ObjectSerializationCache::new();
    let message_size = message.size(&mut cache, version);
    let mut bytes = ByteBufferAccessor::allocate(message_size as usize);
    message.write(&mut bytes, &cache, version)?;
    bytes.flip();
    Ok(bytes)
}

/// Serialize `message` at `version`, prefixed with a big-endian `int16`
/// version header. Returns an in-read-mode `ByteBufferAccessor`. Mirrors
/// `MessageUtil.toVersionPrefixedByteBuffer(short, Message)`.
pub fn to_version_prefixed_byte_buffer<M: Message + ?Sized>(
    version: i16,
    message: &M,
) -> Result<ByteBufferAccessor, KafkaError> {
    let mut cache = ObjectSerializationCache::new();
    let message_size = message.size(&mut cache, version);
    let mut bytes = ByteBufferAccessor::allocate(message_size as usize + 2);
    bytes.write_short(version);
    message.write(&mut bytes, &cache, version)?;
    bytes.flip();
    Ok(bytes)
}

/// Mirrors `MessageUtil.toVersionPrefixedBytes(short, Message)`.
pub fn to_version_prefixed_bytes<M: Message + ?Sized>(version: i16, message: &M) -> Result<Vec<u8>, KafkaError> {
    let buffer = to_version_prefixed_byte_buffer(version, message)?;
    Ok(buffer.buffer().to_vec())
}

/// Mirrors `MessageUtil.toCoordinatorTypePrefixedByteBuffer(ApiMessage)`.
pub fn to_coordinator_type_prefixed_byte_buffer<M: ApiMessage + ?Sized>(
    message: &M,
) -> Result<ByteBufferAccessor, KafkaError> {
    if message.api_key() < 0 {
        return Err(KafkaError::IllegalArgument(
            "Cannot serialize a message without an api key.".to_string(),
        ));
    }
    if message.highest_supported_version() != 0 || message.lowest_supported_version() != 0 {
        return Err(KafkaError::IllegalArgument(
            "Cannot serialize a message with a different version than 0.".to_string(),
        ));
    }
    let mut cache = ObjectSerializationCache::new();
    let message_size = message.size(&mut cache, 0);
    let mut bytes = ByteBufferAccessor::allocate(message_size as usize + 2);
    bytes.write_short(message.api_key());
    message.write(&mut bytes, &cache, 0)?;
    bytes.flip();
    Ok(bytes)
}

/// Mirrors `MessageUtil.toCoordinatorTypePrefixedBytes(ApiMessage)`.
pub fn to_coordinator_type_prefixed_bytes<M: ApiMessage + ?Sized>(message: &M) -> Result<Vec<u8>, KafkaError> {
    let buffer = to_coordinator_type_prefixed_byte_buffer(message)?;
    Ok(buffer.buffer().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translation of `MessageUtilTest#testDeepToString`.
    #[test]
    fn deep_to_string_works() {
        assert_eq!(deep_to_string([1, 2, 3]), "[1, 2, 3]");
        assert_eq!(deep_to_string(["foo"]), "[foo]");
    }

    /// Translation of `MessageUtilTest#testByteBufferToArray`.
    #[test]
    fn byte_buffer_to_array_copies() {
        assert_eq!(byte_buffer_to_array(&[1, 2, 3]), vec![1u8, 2, 3]);
        assert_eq!(byte_buffer_to_array(&[]), Vec::<u8>::new());
    }

    /// Translation of `MessageUtilTest#testDuplicate`.
    #[test]
    fn duplicate_handles_null() {
        assert_eq!(duplicate(None), None);
        assert_eq!(duplicate(Some(&[][..])), Some(vec![]));
        assert_eq!(duplicate(Some(&[1u8, 2, 3][..])), Some(vec![1u8, 2, 3]));
    }

    /// Translation of `MessageUtilTest#testCompareRawTaggedFields`.
    #[test]
    fn compare_raw_tagged_fields_treats_null_as_empty() {
        assert!(compare_raw_tagged_fields(None, None));
        assert!(compare_raw_tagged_fields(None, Some(&[])));
        assert!(compare_raw_tagged_fields(Some(&[]), None));

        let one = vec![RawTaggedField::new(1, vec![1u8])];
        assert!(!compare_raw_tagged_fields(Some(&[]), Some(&one)));
        assert!(!compare_raw_tagged_fields(None, Some(&one)));
        assert!(!compare_raw_tagged_fields(Some(&one), Some(&[])));

        let pair_a = vec![RawTaggedField::new(1, vec![1u8]), RawTaggedField::new(2, vec![])];
        let pair_b = vec![RawTaggedField::new(1, vec![1u8]), RawTaggedField::new(2, vec![])];
        assert!(compare_raw_tagged_fields(Some(&pair_a), Some(&pair_b)));
    }

    /// Translation of `MessageUtilTest#testConstants`.
    #[test]
    fn constants_match_java() {
        assert_eq!(UNSIGNED_SHORT_MAX, 0xFFFF);
        assert_eq!(UNSIGNED_INT_MAX, 0xFFFFFFFFi64);
    }

    // Note: `testBinaryNode` and `testInvalidBinaryNode` exercise Jackson's
    // Base64 binary node decoding. We do not have a Jackson equivalent in the
    // Rust client (the JSON-driven generator runs at build time, not on a
    // hot path), so the corresponding helpers are not translated.
    // Phase 2c-DEFERRED: jsonNodeTo* helpers are unused by the generator's
    // runtime output and would require pulling in serde_json as a runtime
    // dependency; defer until the JSON-driven message reflection path is
    // wired up (post-Milestone 1).
}
