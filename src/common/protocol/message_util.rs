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

//! Utility methods for working with protocol messages.
//!
//! Corresponds to org.apache.kafka.common.protocol.MessageUtil

use std::io;

use super::byte_buffer_accessor::ByteBufferAccessor;
use super::message::Message;
use super::object_serialization_cache::ObjectSerializationCache;

/// Serializes a message to a ByteBufferAccessor positioned at the beginning for reading.
///
/// This method:
/// 1. Calculates the serialized size using the two-pass approach
/// 2. Allocates a buffer of that size
/// 3. Writes the message to the buffer
/// 4. Resets the buffer position to 0 for reading
pub fn to_byte_buffer_accessor(message: &impl Message, version: i16) -> io::Result<ByteBufferAccessor> {
    let mut cache = ObjectSerializationCache::new();
    let message_size = message.size(&mut cache, version)?;
    let mut bytes = ByteBufferAccessor::new(message_size as usize);
    message.write(&mut bytes, &cache, version)?;
    bytes.set_position(0)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::Readable;
    use crate::common::protocol::Writable;
    use crate::common::protocol::message_size_accumulator::MessageSizeAccumulator;
    use crate::common::protocol::readable::RawTaggedField;

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
            &self,
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
        let msg = TestMsg { value: 42 };
        let mut acc = to_byte_buffer_accessor(&msg, 0).unwrap();
        assert_eq!(acc.read_int().unwrap(), 42);
    }
}
