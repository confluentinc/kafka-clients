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

//! Translation of
//! `org.apache.kafka.common.protocol.types.RawTaggedFieldWriter`.

use crate::common::errors::KafkaError;
use crate::common::protocol::Writable;
use crate::common::protocol::types::raw_tagged_field::RawTaggedField;

/// The `RawTaggedFieldWriter` is used by `Message` subclasses to serialise
/// their lists of raw tags. Mirrors
/// `org.apache.kafka.common.protocol.types.RawTaggedFieldWriter`.
#[derive(Debug)]
pub struct RawTaggedFieldWriter {
    fields: Vec<RawTaggedField>,
    /// Index of the next field to be visited by [`Self::write_raw_tags`].
    cursor: usize,
    /// The previous tag observed; matches Java's `prevTag` initialised to
    /// `-1`.
    prev_tag: i32,
}

impl RawTaggedFieldWriter {
    /// Construct a `RawTaggedFieldWriter` over an owned list of raw fields.
    /// Mirrors `RawTaggedFieldWriter.forFields(List<RawTaggedField>)`. A
    /// `null` Java argument is represented by an empty Rust `Vec`.
    pub fn for_fields(fields: Vec<RawTaggedField>) -> Self {
        RawTaggedFieldWriter { fields, cursor: 0, prev_tag: -1 }
    }

    /// Construct a writer with no fields. Mirrors the
    /// `EMPTY_WRITER` singleton in Java (returned when `forFields` is
    /// passed `null`).
    pub fn empty() -> Self {
        Self::for_fields(Vec::new())
    }

    /// Number of raw tag fields the writer manages. Mirrors
    /// `RawTaggedFieldWriter#numFields`.
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Write all raw tag fields whose tag is strictly less than
    /// `next_defined_tag`. Mirrors `RawTaggedFieldWriter#writeRawTags`.
    ///
    /// On error returns a [`KafkaError::Generic`] wrapping the message
    /// Java's `RuntimeException` would carry; this matches the Java
    /// `assertThrows(RuntimeException.class, ...)` tests.
    pub fn write_raw_tags<W: Writable>(&mut self, writable: &mut W, next_defined_tag: i32) -> Result<(), KafkaError> {
        while self.cursor < self.fields.len() {
            let field = self.fields[self.cursor].clone();
            let tag = field.tag();
            if tag >= next_defined_tag {
                if tag == next_defined_tag {
                    return Err(KafkaError::Generic(format!("Attempted to use tag {tag} as an undefined tag.")));
                }
                // Step back so the next call observes this same field.
                return Ok(());
            }
            if tag <= self.prev_tag {
                return Err(KafkaError::Generic(format!(
                    "Invalid raw tag field list: tag {tag} comes after tag {}, but is not higher than it.",
                    self.prev_tag
                )));
            }
            writable.write_unsigned_varint(field.tag() as u32);
            writable.write_unsigned_varint(field.data().len() as u32);
            writable.write_byte_array(field.data());
            self.prev_tag = tag;
            self.cursor += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ByteBufferAccessor;

    /// Mirrors `RawTaggedFieldWriterTest.testWritingZeroRawTaggedFields`.
    #[test]
    fn writing_zero_raw_tagged_fields() {
        let mut writer = RawTaggedFieldWriter::empty();
        assert_eq!(writer.num_fields(), 0);
        let mut accessor = ByteBufferAccessor::allocate(0);
        writer.write_raw_tags(&mut accessor, i32::MAX).unwrap();
    }

    /// Mirrors `RawTaggedFieldWriterTest.testWritingSeveralRawTaggedFields`.
    #[test]
    fn writing_several_raw_tagged_fields() {
        let tags = vec![
            RawTaggedField::new(2, vec![0x1, 0x2, 0x3]),
            RawTaggedField::new(5, vec![0x4, 0x5]),
        ];
        let mut writer = RawTaggedFieldWriter::for_fields(tags);
        assert_eq!(writer.num_fields(), 2);
        // The Java test uses a single ByteBufferAccessor across all calls;
        // its position keeps advancing. We mirror that exactly here.
        let mut accessor = ByteBufferAccessor::allocate(9);

        writer.write_raw_tags(&mut accessor, 1).unwrap();
        assert_eq!(accessor.position(), 0);
        assert_eq!(&accessor.raw_buffer()[..9], &[0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0]);

        writer.write_raw_tags(&mut accessor, 3).unwrap();
        assert_eq!(accessor.position(), 5);
        assert_eq!(&accessor.raw_buffer()[..9], &[0x2, 0x3, 0x1, 0x2, 0x3, 0x0, 0x0, 0x0, 0x0]);

        writer.write_raw_tags(&mut accessor, 7).unwrap();
        assert_eq!(&accessor.raw_buffer()[..9], &[0x2, 0x3, 0x1, 0x2, 0x3, 0x5, 0x2, 0x4, 0x5]);

        writer.write_raw_tags(&mut accessor, i32::MAX).unwrap();
        assert_eq!(&accessor.raw_buffer()[..9], &[0x2, 0x3, 0x1, 0x2, 0x3, 0x5, 0x2, 0x4, 0x5]);
    }

    /// Mirrors `RawTaggedFieldWriterTest.testInvalidNextDefinedTag`.
    #[test]
    fn invalid_next_defined_tag() {
        let tags = vec![
            RawTaggedField::new(2, vec![0x1, 0x2, 0x3]),
            RawTaggedField::new(5, vec![0x4, 0x5, 0x6]),
            RawTaggedField::new(7, vec![0x0]),
        ];
        let mut writer = RawTaggedFieldWriter::for_fields(tags);
        assert_eq!(writer.num_fields(), 3);
        let mut accessor = ByteBufferAccessor::allocate(1024);
        let err = writer.write_raw_tags(&mut accessor, 2).unwrap_err();
        assert_eq!(err.message(), "Attempted to use tag 2 as an undefined tag.");
    }

    /// Mirrors `RawTaggedFieldWriterTest.testOutOfOrderTags`.
    #[test]
    fn out_of_order_tags() {
        let tags = vec![
            RawTaggedField::new(5, vec![0x4, 0x5, 0x6]),
            RawTaggedField::new(2, vec![0x1, 0x2, 0x3]),
            RawTaggedField::new(7, vec![0x0]),
        ];
        let mut writer = RawTaggedFieldWriter::for_fields(tags);
        assert_eq!(writer.num_fields(), 3);
        let mut accessor = ByteBufferAccessor::allocate(1024);
        let err = writer.write_raw_tags(&mut accessor, 8).unwrap_err();
        assert_eq!(
            err.message(),
            "Invalid raw tag field list: tag 2 comes after tag 5, but is not higher than it."
        );
    }
}
