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

#![cfg_attr(not(test), expect(dead_code))]
//! Utility methods for working with protocol messages.
//!
//! Corresponds to org.apache.kafka.common.protocol.MessageUtil

use std::io;

use super::ByteBufferAccessor;
use super::Message;
use super::ObjectSerializationCache;
use super::types::RawTaggedField;

/// Translates the Java static-utility class `org.apache.kafka.common.protocol.MessageUtil`,
/// which has no instance state, so it becomes a unit struct hosting its
/// statics as associated items.
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.protocol.MessageUtil")]
pub struct MessageUtil;

impl MessageUtil {
    /// Maximum value of an unsigned 16-bit integer.
    pub const UNSIGNED_SHORT_MAX: u32 = 0xFFFF;

    /// Maximum value of an unsigned 32-bit integer.
    pub const UNSIGNED_INT_MAX: u64 = 0xFFFF_FFFF;

    /// Upper bound on the initial capacity a generated reader pre-allocates for
    /// an array field. A larger declared count still parses: the collection
    /// grows on demand as elements are read.
    pub const MAX_PREALLOCATED_ARRAY_CAPACITY: i32 = 1000;

    /// Largest array length a generated reader accepts; a larger declared
    /// length is rejected before any element is read.
    pub const MAX_ARRAY_LENGTH: i32 = 1_000_000;

    /// Largest number of tagged fields a generated reader accepts in one
    /// tagged-field section.
    pub const MAX_TAGGED_FIELD_COUNT: i32 = 10_000;

    /// Compares two lists of raw tagged fields.
    ///
    /// An empty slice is considered equivalent to no tagged fields.
    /// This matches Java's `MessageUtil.compareRawTaggedFields` where
    /// `null` is equivalent to an empty list.
    #[doc(alias = "org.apache.kafka.common.protocol.MessageUtil#compareRawTaggedFields")]
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
    #[doc(alias = "org.apache.kafka.common.protocol.MessageUtil#toByteBufferAccessor")]
    pub fn to_byte_buffer_accessor(message: &mut impl Message, version: i16) -> io::Result<ByteBufferAccessor> {
        let mut cache = ObjectSerializationCache::new();
        let message_size = message.size(&mut cache, version)?;
        let mut bytes = ByteBufferAccessor::new(Vec::with_capacity(message_size as usize));
        message.write(&mut bytes, &cache, version)?;
        bytes.set_position(0)?;
        Ok(bytes)
    }

    /// Serializes a message prefixed with its 2-byte (big-endian) version.
    ///
    /// Corresponds to `MessageUtil.toVersionPrefixedByteBuffer`. The returned
    /// buffer holds the version `short` followed by the message body, positioned at
    /// the beginning for reading.
    #[doc(alias = "org.apache.kafka.common.protocol.MessageUtil#toVersionPrefixedByteBuffer")]
    pub fn to_version_prefixed_byte_buffer(version: i16, message: &mut impl Message) -> io::Result<ByteBufferAccessor> {
        use super::Writable;

        let mut cache = ObjectSerializationCache::new();
        let message_size = message.size(&mut cache, version)?;
        let mut bytes = ByteBufferAccessor::new(Vec::with_capacity(2 + message_size as usize));
        bytes.write_short(version)?;
        message.write(&mut bytes, &cache, version)?;
        bytes.set_position(0)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::MessageSizeAccumulator;
    use crate::common::protocol::Readable;
    use crate::common::protocol::Writable;
    use crate::common::protocol::types::RawTaggedField;

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
        let mut acc = MessageUtil::to_byte_buffer_accessor(&mut msg, 0).unwrap();
        assert_eq!(acc.read_int().unwrap(), 42);
    }

    /// Translated from Java MessageUtilTest.testCompareRawTaggedFields.
    /// Verifies comparison semantics for RawTaggedField lists:
    /// None vs empty, different fields, matching fields.
    #[test]
    #[doc(alias = "org.apache.kafka.common.protocol.MessageUtilTest#testCompareRawTaggedFields")]
    fn test_compare_raw_tagged_fields() {
        // null vs null
        assert!(MessageUtil::compare_raw_tagged_fields(None, None));
        // null vs empty
        assert!(MessageUtil::compare_raw_tagged_fields(None, Some(&[])));
        // empty vs null
        assert!(MessageUtil::compare_raw_tagged_fields(Some(&[]), None));
        // empty vs non-empty
        assert!(!MessageUtil::compare_raw_tagged_fields(
            Some(&[]),
            Some(&[RawTaggedField::new(1, vec![1])])
        ));
        // null vs non-empty
        assert!(!MessageUtil::compare_raw_tagged_fields(
            None,
            Some(&[RawTaggedField::new(1, vec![1])])
        ));
        // non-empty vs empty
        assert!(!MessageUtil::compare_raw_tagged_fields(
            Some(&[RawTaggedField::new(1, vec![1])]),
            Some(&[])
        ));
        // matching lists
        assert!(MessageUtil::compare_raw_tagged_fields(
            Some(&[RawTaggedField::new(1, vec![1]), RawTaggedField::new(2, vec![])]),
            Some(&[RawTaggedField::new(1, vec![1]), RawTaggedField::new(2, vec![])])
        ));
    }

    /// Translated from Java MessageUtilTest.testConstants.
    /// Verifies UNSIGNED_SHORT_MAX and UNSIGNED_INT_MAX values.
    #[test]
    #[doc(alias = "org.apache.kafka.common.protocol.MessageUtilTest#testConstants")]
    fn test_constants() {
        assert_eq!(MessageUtil::UNSIGNED_SHORT_MAX, 0xFFFF);
        assert_eq!(MessageUtil::UNSIGNED_INT_MAX, 0xFFFF_FFFF);
    }
}
