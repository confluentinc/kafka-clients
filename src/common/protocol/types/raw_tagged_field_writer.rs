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
use crate::common::protocol::types::io::Writable;
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
