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

#![allow(dead_code)]
//! Utility methods for working with protocol messages.
//!
//! Corresponds to org.apache.kafka.common.protocol.MessageUtil

use std::io;

use super::ByteBufferAccessor;
use super::Message;
use super::ObjectSerializationCache;
use super::RawTaggedField;

/// Maximum value of an unsigned 16-bit integer.
pub const UNSIGNED_SHORT_MAX: u32 = 0xFFFF;

/// Maximum value of an unsigned 32-bit integer.
pub const UNSIGNED_INT_MAX: u64 = 0xFFFF_FFFF;

/// Compares two lists of raw tagged fields.
///
/// An empty slice is considered equivalent to no tagged fields.
/// This matches Java's `MessageUtil.compareRawTaggedFields` where
/// `null` is equivalent to an empty list.
pub fn compare_raw_tagged_fields(first: Option<&[RawTaggedField]>, second: Option<&[RawTaggedField]>) -> bool {
    let first = first.unwrap_or(&[]);
    let second = second.unwrap_or(&[]);
    first == second
}

/// Serializes a message to a ByteBufferAccessor positioned at the beginning for reading.
///
/// This method:
/// 1. Calculates the serialized size using the two-pass approach
/// 2. Allocates a buffer of that size
/// 3. Writes the message to the buffer
/// 4. Resets the buffer position to 0 for reading
pub fn to_byte_buffer_accessor(message: &mut impl Message, version: i16) -> io::Result<ByteBufferAccessor> {
    let mut cache = ObjectSerializationCache::new();
    let message_size = message.size(&mut cache, version)?;
    let mut bytes = ByteBufferAccessor::new(message_size as usize);
    message.write(&mut bytes, &cache, version)?;
    bytes.set_position(0)?;
    Ok(bytes)
}

/// Serializes a message prefixed with its 2-byte (big-endian) version.
///
/// Corresponds to `MessageUtil.toVersionPrefixedByteBuffer`. The returned
/// buffer holds the version `short` followed by the message body, positioned at
/// the beginning for reading.
pub fn to_version_prefixed_byte_buffer(version: i16, message: &mut impl Message) -> io::Result<ByteBufferAccessor> {
    use super::Writable;

    let mut cache = ObjectSerializationCache::new();
    let message_size = message.size(&mut cache, version)?;
    let mut bytes = ByteBufferAccessor::new(2 + message_size as usize);
    bytes.write_short(version)?;
    message.write(&mut bytes, &cache, version)?;
    bytes.set_position(0)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::MessageSizeAccumulator;
    use crate::common::protocol::RawTaggedField;
    use crate::common::protocol::Readable;
    use crate::common::protocol::Writable;

    #[derive(Debug, Clone, PartialEq)]
    struct TestMsg {
        value: i32,
    }

    impl Message for TestMsg {
        fn lowest_supported_version(&self) -> i16 {
            0
        }
        fn highest_supported_version(&self) -> i16 {
            0
        }
        fn add_size(
            &self,
            size: &mut MessageSizeAccumulator,
            _cache: &mut ObjectSerializationCache,
            _version: i16,
        ) -> io::Result<()> {
            size.add_bytes(4);
            Ok(())
        }
        fn write(
            &mut self,
            writable: &mut dyn Writable,
            _cache: &ObjectSerializationCache,
            _version: i16,
        ) -> io::Result<()> {
            writable.write_int(self.value)
        }
        fn read(&mut self, readable: &mut dyn Readable, _version: i16) -> io::Result<()> {
            self.value = readable.read_int()?;
            Ok(())
        }
        fn unknown_tagged_fields(&self) -> &[RawTaggedField] {
            &[]
        }
    }

    #[test]
    fn test_to_byte_buffer_accessor() {
        let mut msg = TestMsg { value: 42 };
        let mut acc = to_byte_buffer_accessor(&mut msg, 0).unwrap();
        assert_eq!(acc.read_int().unwrap(), 42);
    }

    /// Translated from Java MessageUtilTest.testCompareRawTaggedFields.
    /// Verifies comparison semantics for RawTaggedField lists:
    /// None vs empty, different fields, matching fields.
    #[test]
    fn test_compare_raw_tagged_fields() {
        // null vs null
        assert!(compare_raw_tagged_fields(None, None));
        // null vs empty
        assert!(compare_raw_tagged_fields(None, Some(&[])));
        // empty vs null
        assert!(compare_raw_tagged_fields(Some(&[]), None));
        // empty vs non-empty
        assert!(!compare_raw_tagged_fields(Some(&[]), Some(&[RawTaggedField::new(1, vec![1])])));
        // null vs non-empty
        assert!(!compare_raw_tagged_fields(None, Some(&[RawTaggedField::new(1, vec![1])])));
        // non-empty vs empty
        assert!(!compare_raw_tagged_fields(Some(&[RawTaggedField::new(1, vec![1])]), Some(&[])));
        // matching lists
        assert!(compare_raw_tagged_fields(
            Some(&[RawTaggedField::new(1, vec![1]), RawTaggedField::new(2, vec![])]),
            Some(&[RawTaggedField::new(1, vec![1]), RawTaggedField::new(2, vec![])])
        ));
    }

    /// Translated from Java MessageUtilTest.testConstants.
    /// Verifies UNSIGNED_SHORT_MAX and UNSIGNED_INT_MAX values.
    #[test]
    fn test_constants() {
        assert_eq!(UNSIGNED_SHORT_MAX, 0xFFFF);
        assert_eq!(UNSIGNED_INT_MAX, 0xFFFF_FFFF);
    }
}
