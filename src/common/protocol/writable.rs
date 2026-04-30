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

//! Translation of `org.apache.kafka.common.protocol.Writable`.
//!
//! `Writable` is the generic sink for typed wire-protocol primitives,
//! mirroring the Java interface of the same name. Its canonical
//! implementation is [`crate::common::protocol::ByteBufferAccessor`]; the
//! [`crate::common::protocol::DataOutputStreamWritable`] adapter targets a
//! `std::io::Write` instead of an in-memory buffer, and
//! [`crate::common::protocol::SendBuilder`] uses it to assemble vectored
//! sends.
//!
//! As with [`super::Readable`], the default `writeRecords`/`writeUuid`/
//! `writeUnsignedShort`/`writeUnsignedInt` helpers are translated as
//! provided trait methods so every implementor inherits them automatically.

use crate::common::Uuid;
use crate::common::utils::byte_utils;

/// Translation of `org.apache.kafka.common.protocol.Writable`.
pub trait Writable {
    /// Mirrors `writeByte(byte)`.
    fn write_byte(&mut self, val: i8);

    /// Mirrors `writeShort(short)`.
    fn write_short(&mut self, val: i16);

    /// Mirrors `writeInt(int)`.
    fn write_int(&mut self, val: i32);

    /// Mirrors `writeLong(long)`.
    fn write_long(&mut self, val: i64);

    /// Mirrors `writeDouble(double)`.
    fn write_double(&mut self, val: f64);

    /// Mirrors `writeByteArray(byte[])`.
    fn write_byte_array(&mut self, arr: &[u8]);

    /// Mirrors `writeUnsignedVarint(int)`.
    fn write_unsigned_varint(&mut self, value: u32);

    /// Mirrors `writeByteBuffer(ByteBuffer)`. Writes the entire byte slice.
    /// Implementations that support zero-copy (e.g. [`SendBuilder`]) may
    /// retain a reference rather than copying.
    fn write_byte_buffer(&mut self, buf: &[u8]);

    /// Mirrors `writeVarint(int)`.
    fn write_varint(&mut self, value: i32);

    /// Mirrors `writeVarlong(long)`.
    fn write_varlong(&mut self, value: i64);

    /// Mirrors the Java default `writeUuid(Uuid)`.
    fn write_uuid(&mut self, uuid: &Uuid) {
        self.write_long(uuid.most_significant_bits());
        self.write_long(uuid.least_significant_bits());
    }

    /// Mirrors the Java default `writeUnsignedShort(int)`.
    fn write_unsigned_short(&mut self, value: u16) {
        self.write_short(value as i16);
    }

    /// Mirrors the Java default `writeUnsignedInt(long)`.
    fn write_unsigned_int(&mut self, value: u32) {
        self.write_int(value as i32);
    }
}

/// Convenience [`Writable`] impl over `Vec<u8>` — appends each primitive at
/// the current end of the vector. Used by tests and by simple builder paths
/// that do not need position tracking.
impl Writable for Vec<u8> {
    fn write_byte(&mut self, val: i8) {
        self.push(val as u8);
    }

    fn write_short(&mut self, val: i16) {
        self.extend_from_slice(&val.to_be_bytes());
    }

    fn write_int(&mut self, val: i32) {
        self.extend_from_slice(&val.to_be_bytes());
    }

    fn write_long(&mut self, val: i64) {
        self.extend_from_slice(&val.to_be_bytes());
    }

    fn write_double(&mut self, val: f64) {
        self.extend_from_slice(&val.to_be_bytes());
    }

    fn write_byte_array(&mut self, arr: &[u8]) {
        self.extend_from_slice(arr);
    }

    fn write_unsigned_varint(&mut self, value: u32) {
        byte_utils::write_unsigned_varint(value, self);
    }

    fn write_byte_buffer(&mut self, buf: &[u8]) {
        self.extend_from_slice(buf);
    }

    fn write_varint(&mut self, value: i32) {
        byte_utils::write_varint(value, self);
    }

    fn write_varlong(&mut self, value: i64) {
        byte_utils::write_varlong(value, self);
    }
}
