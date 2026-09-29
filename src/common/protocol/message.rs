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

//! Message trait for protocol message serialization.
//!
//! An object that can serialize itself. The serialization protocol is versioned.
//! Messages also implement Display, PartialEq, and Clone.
//!
//! Corresponds to org.apache.kafka.common.protocol.Message

use std::io;

use super::MessageSizeAccumulator;
use super::ObjectSerializationCache;
use super::RawTaggedField;
use super::Readable;
use super::Writable;

/// Trait for Kafka protocol messages that can serialize and deserialize themselves.
///
/// This trait defines the core serialization contract for all Kafka protocol messages.
/// It supports versioned serialization with a two-pass approach:
/// 1. First pass: calculate size using [`size`](Message::size) (which calls [`add_size`](Message::add_size))
/// 2. Second pass: serialize using [`write`](Message::write)
pub trait Message: Clone {
    /// Returns the lowest supported version of this message, inclusive.
    fn lowest_supported_version(&self) -> i16;

    /// Returns the highest supported version of this message, inclusive.
    fn highest_supported_version(&self) -> i16;

    /// Returns the number of bytes it would take to write out this message.
    ///
    /// Populates the serialization cache with intermediate values needed for the
    /// subsequent write pass.
    ///
    /// # Errors
    ///
    /// Returns an error if the specified version is not supported.
    fn size(&self, cache: &mut ObjectSerializationCache, version: i16) -> io::Result<i32> {
        let mut size = MessageSizeAccumulator::new();
        self.add_size(&mut size, cache, version)?;
        Ok(size.total_size())
    }

    /// Add the size of this message to an accumulator.
    ///
    /// # Errors
    ///
    /// Returns an error if the specified version is not supported.
    fn add_size(
        &self,
        size: &mut MessageSizeAccumulator,
        cache: &mut ObjectSerializationCache,
        version: i16,
    ) -> io::Result<()>;

    /// Writes out this message to the given Writable.
    ///
    /// The serialization cache must have been previously populated using [`size`](Message::size).
    ///
    /// # Errors
    ///
    /// Returns an error if the specified version is not supported.
    fn write(&mut self, writable: &mut dyn Writable, cache: &ObjectSerializationCache, version: i16) -> io::Result<()>;

    /// Reads this message from the given Readable. This will overwrite all
    /// relevant fields with information from the byte buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the specified version is not supported.
    fn read(&mut self, readable: &mut dyn Readable, version: i16) -> io::Result<()>;

    /// Returns a list of tagged fields which this software can't understand.
    fn unknown_tagged_fields(&self) -> &[RawTaggedField];

    /// Make a deep copy of the message.
    fn duplicate(&self) -> Self {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ApiMessage;

    /// A minimal test message to verify the trait works correctly.
    #[derive(Debug, Clone, PartialEq)]
    struct TestMessage {
        value: i32,
        unknown_tagged_fields: Vec<RawTaggedField>,
    }

    impl TestMessage {
        fn new(value: i32) -> Self {
            Self { value, unknown_tagged_fields: Vec::new() }
        }
    }

    impl Message for TestMessage {
        fn lowest_supported_version(&self) -> i16 {
            0
        }

        fn highest_supported_version(&self) -> i16 {
            1
        }

        fn add_size(
            &self,
            size: &mut MessageSizeAccumulator,
            _cache: &mut ObjectSerializationCache,
            version: i16,
        ) -> io::Result<()> {
            if !(0..=1).contains(&version) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid version {version} for TestMessage"),
                ));
            }
            // i32 = 4 bytes
            size.add_bytes(4);
            Ok(())
        }

        fn write(
            &mut self,
            writable: &mut dyn Writable,
            _cache: &ObjectSerializationCache,
            version: i16,
        ) -> io::Result<()> {
            if !(0..=1).contains(&version) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid version {version} for TestMessage"),
                ));
            }
            writable.write_int(self.value)
        }

        fn read(&mut self, readable: &mut dyn Readable, version: i16) -> io::Result<()> {
            if !(0..=1).contains(&version) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid version {version} for TestMessage"),
                ));
            }
            self.value = readable.read_int()?;
            Ok(())
        }

        fn unknown_tagged_fields(&self) -> &[RawTaggedField] {
            &self.unknown_tagged_fields
        }
    }

    /// A test API message that extends Message with an API key.
    #[derive(Debug, Clone, PartialEq)]
    struct TestApiMessage {
        inner: TestMessage,
    }

    impl TestApiMessage {
        fn new(value: i32) -> Self {
            Self { inner: TestMessage::new(value) }
        }
    }

    impl Message for TestApiMessage {
        fn lowest_supported_version(&self) -> i16 {
            self.inner.lowest_supported_version()
        }

        fn highest_supported_version(&self) -> i16 {
            self.inner.highest_supported_version()
        }

        fn add_size(
            &self,
            size: &mut MessageSizeAccumulator,
            cache: &mut ObjectSerializationCache,
            version: i16,
        ) -> io::Result<()> {
            self.inner.add_size(size, cache, version)
        }

        fn write(
            &mut self,
            writable: &mut dyn Writable,
            cache: &ObjectSerializationCache,
            version: i16,
        ) -> io::Result<()> {
            self.inner.write(writable, cache, version)
        }

        fn read(&mut self, readable: &mut dyn Readable, version: i16) -> io::Result<()> {
            self.inner.read(readable, version)
        }

        fn unknown_tagged_fields(&self) -> &[RawTaggedField] {
            self.inner.unknown_tagged_fields()
        }
    }

    impl ApiMessage for TestApiMessage {
        fn api_key(&self) -> i16 {
            42
        }
    }

    #[test]
    fn test_message_version_range() {
        let msg = TestMessage::new(123);
        assert_eq!(msg.lowest_supported_version(), 0);
        assert_eq!(msg.highest_supported_version(), 1);
    }

    #[test]
    fn test_message_size() {
        let msg = TestMessage::new(123);
        let mut cache = ObjectSerializationCache::new();
        let size = msg.size(&mut cache, 0).unwrap();
        assert_eq!(size, 4); // i32 = 4 bytes
    }

    #[test]
    fn test_message_write_and_read() {
        use crate::common::ByteBufferAccessor;

        let mut msg = TestMessage::new(42);
        let mut cache = ObjectSerializationCache::new();
        let size = msg.size(&mut cache, 0).unwrap();

        // Write
        let mut accessor = ByteBufferAccessor::new(Vec::with_capacity(size as usize));
        msg.write(&mut accessor, &cache, 0).unwrap();

        // Read
        let buf = accessor.buffer().to_vec();
        let mut read_accessor = ByteBufferAccessor::new(buf);
        let mut read_msg = TestMessage::new(0);
        read_msg.read(&mut read_accessor, 0).unwrap();

        assert_eq!(read_msg.value, 42);
    }

    #[test]
    fn test_message_invalid_version() {
        let msg = TestMessage::new(123);
        let mut cache = ObjectSerializationCache::new();
        assert!(msg.size(&mut cache, 5).is_err());
    }

    #[test]
    fn test_message_duplicate() {
        let msg = TestMessage::new(99);
        let dup = msg.duplicate();
        assert_eq!(msg, dup);
    }

    #[test]
    fn test_message_unknown_tagged_fields() {
        let msg = TestMessage::new(0);
        assert!(msg.unknown_tagged_fields().is_empty());
    }

    #[test]
    fn test_api_message_key() {
        let msg = TestApiMessage::new(0);
        assert_eq!(msg.api_key(), 42);
    }

    #[test]
    fn test_api_message_is_message() {
        let msg = TestApiMessage::new(10);
        let mut cache = ObjectSerializationCache::new();
        let size = msg.size(&mut cache, 0).unwrap();
        assert_eq!(size, 4);
    }
}
