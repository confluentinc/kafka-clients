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

//! Translation of `org.apache.kafka.common.protocol.Readable`.
//!
//! `Readable` is a generic source of typed wire-protocol primitives, mirroring
//! the Java interface of the same name. The canonical implementation is
//! [`crate::common::protocol::ByteBufferAccessor`], but other clients (e.g.
//! the network layer) implement it on their own buffers.
//!
//! In Java, the read methods throw `RuntimeException` when the source has
//! insufficient bytes. Following CLAUDE.md rule 10, we surface these as
//! `Result<T, KafkaError>` here. The default-method helpers
//! (`read_string`, `read_uuid`, `read_unsigned_short`, …) are translated as
//! provided trait methods so any implementor inherits them automatically.

use crate::common::Uuid;
use crate::common::errors::KafkaError;
use crate::common::protocol::types::RawTaggedField;

/// Translation of `org.apache.kafka.common.protocol.Readable`.
pub trait Readable {
    /// Mirrors `readByte`.
    fn read_byte(&mut self) -> Result<i8, KafkaError>;

    /// Mirrors `readShort`.
    fn read_short(&mut self) -> Result<i16, KafkaError>;

    /// Mirrors `readInt`.
    fn read_int(&mut self) -> Result<i32, KafkaError>;

    /// Mirrors `readLong`.
    fn read_long(&mut self) -> Result<i64, KafkaError>;

    /// Mirrors `readDouble`.
    fn read_double(&mut self) -> Result<f64, KafkaError>;

    /// Mirrors `readArray(int length)`. Reads `length` bytes into a freshly
    /// allocated `Vec`. Returns an error if fewer than `length` bytes remain.
    fn read_array(&mut self, length: usize) -> Result<Vec<u8>, KafkaError>;

    /// Mirrors `readUnsignedVarint`.
    fn read_unsigned_varint(&mut self) -> Result<u32, KafkaError>;

    /// Mirrors `readByteBuffer(int length)`. Reads `length` bytes and returns
    /// them as an owned `Vec` whose contents may be wrapped in a buffer by
    /// the caller. Java returns a `ByteBuffer` slice that shares storage with
    /// the source; for the producer client we translate this as a `Vec<u8>`
    /// since none of the Phase 2c+ call sites mutate the source after read.
    fn read_byte_buffer(&mut self, length: usize) -> Result<Vec<u8>, KafkaError>;

    /// Mirrors `readVarint`.
    fn read_varint(&mut self) -> Result<i32, KafkaError>;

    /// Mirrors `readVarlong`.
    fn read_varlong(&mut self) -> Result<i64, KafkaError>;

    /// Mirrors `remaining()`.
    fn remaining(&self) -> usize;

    /// Mirrors `slice()`. Returns a new `Box<dyn Readable>` whose content
    /// shares the same bytes as `self`, starting at `self`'s current position.
    /// The two readables advance independently after `slice()` is called.
    fn slice(&mut self) -> Result<Box<dyn Readable + '_>, KafkaError>;

    /// Mirrors the Java default `readString(int length)`.
    fn read_string(&mut self, length: usize) -> Result<String, KafkaError> {
        let arr = self.read_array(length)?;
        String::from_utf8(arr).map_err(|e| KafkaError::Generic(format!("Invalid UTF-8: {e}")))
    }

    /// Mirrors the Java default
    /// `readUnknownTaggedField(List<RawTaggedField>, int tag, int size)`.
    fn read_unknown_tagged_field(
        &mut self,
        unknowns: Option<Vec<RawTaggedField>>,
        tag: i32,
        size: usize,
    ) -> Result<Vec<RawTaggedField>, KafkaError> {
        let mut list = unknowns.unwrap_or_default();
        let data = self.read_array(size)?;
        list.push(RawTaggedField::new(tag, data));
        Ok(list)
    }

    /// Mirrors the Java default `readUuid()`. Reads two big-endian longs.
    fn read_uuid(&mut self) -> Result<Uuid, KafkaError> {
        let msb = self.read_long()?;
        let lsb = self.read_long()?;
        Ok(Uuid::new(msb, lsb))
    }

    /// Mirrors `readUnsignedShort`.
    fn read_unsigned_short(&mut self) -> Result<u16, KafkaError> {
        Ok(self.read_short()? as u16)
    }

    /// Mirrors `readUnsignedInt`.
    fn read_unsigned_int(&mut self) -> Result<u32, KafkaError> {
        Ok(self.read_int()? as u32)
    }
}
