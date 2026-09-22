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

//! Readable trait for deserializing Kafka protocol messages.
//!
//! Corresponds to org.apache.kafka.common.protocol.Readable

use super::RawTaggedField;
use crate::common::Uuid;
use std::io;

/// Trait for reading Kafka protocol data types from a byte stream.
///
/// This trait provides methods for reading primitive types and Kafka-specific
/// types like varints, UUIDs, and strings.
pub trait Readable {
    /// Read a single byte.
    fn read_byte(&mut self) -> io::Result<i8>;

    /// Read a 16-bit signed integer (big-endian).
    fn read_short(&mut self) -> io::Result<i16>;

    /// Read a 32-bit signed integer (big-endian).
    fn read_int(&mut self) -> io::Result<i32>;

    /// Read a 64-bit signed integer (big-endian).
    fn read_long(&mut self) -> io::Result<i64>;

    /// Read a 64-bit floating point number (big-endian).
    fn read_double(&mut self) -> io::Result<f64>;

    /// Read an array of bytes with the given length.
    fn read_array(&mut self, length: usize) -> io::Result<Vec<u8>>;

    /// Read `length` bytes as an owned, reference-counted [`bytes::Bytes`].
    ///
    /// This is the zero-copy entry point used for the wire `records` field on
    /// the receive path (see `consumer-threading.md` §27). The default
    /// implementation falls back to a copy through [`read_array`]; readers
    /// backed by a [`bytes::Bytes`] buffer (e.g. `BytesReader`) override this
    /// to return an O(1) refcounted slice of the source buffer instead.
    fn read_bytes_owned(&mut self, length: usize) -> io::Result<bytes::Bytes> {
        Ok(bytes::Bytes::from(self.read_array(length)?))
    }

    /// Read an unsigned varint (for sizes, lengths, counts).
    fn read_unsigned_varint(&mut self) -> io::Result<u32>;

    /// Read a signed varint (zig-zag encoded).
    fn read_varint(&mut self) -> io::Result<i32>;

    /// Read a signed varlong (zig-zag encoded).
    fn read_varlong(&mut self) -> io::Result<i64>;

    /// Returns the number of bytes remaining to be read.
    fn remaining(&self) -> usize;

    /// Read a UTF-8 string of the given length.
    fn read_string(&mut self, length: usize) -> io::Result<String> {
        let bytes = self.read_array(length)?;
        String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Read bytes into the provided buffer.
    fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let data = self.read_array(buf.len())?;
        buf.copy_from_slice(&data);
        Ok(())
    }

    /// Read a UUID (128-bit value, most significant bits first).
    fn read_uuid(&mut self) -> io::Result<Uuid> {
        let most_sig_bits = self.read_long()? as u64;
        let least_sig_bits = self.read_long()? as u64;
        Ok(Uuid::new(most_sig_bits, least_sig_bits))
    }

    /// Read an unsigned 16-bit integer.
    fn read_unsigned_short(&mut self) -> io::Result<u16> {
        let value = self.read_short()?;
        Ok(value as u16)
    }

    /// Read an unsigned 32-bit integer.
    fn read_unsigned_int(&mut self) -> io::Result<u32> {
        let value = self.read_int()?;
        Ok(value as u32)
    }

    /// Read an unknown tagged field and add it to the list.
    /// Returns the updated list of unknown tagged fields.
    fn read_unknown_tagged_field(
        &mut self,
        mut unknowns: Vec<RawTaggedField>,
        tag: u32,
        size: u32,
    ) -> io::Result<Vec<RawTaggedField>> {
        let data = self.read_array(size as usize)?;
        unknowns.push(RawTaggedField::new(tag, data));
        Ok(unknowns)
    }
}
