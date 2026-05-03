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

//! Writable trait for serializing Kafka protocol messages.
//!
//! Corresponds to org.apache.kafka.common.protocol.Writable

use crate::common::Uuid;
use std::io;

/// Trait for writing Kafka protocol data types to a byte stream.
///
/// This trait provides methods for writing primitive types and Kafka-specific
/// types like varints, UUIDs, and byte arrays.
pub trait Writable {
    /// Write a single byte.
    fn write_byte(&mut self, val: i8) -> io::Result<()>;

    /// Write a 16-bit signed integer (big-endian).
    fn write_short(&mut self, val: i16) -> io::Result<()>;

    /// Write a 32-bit signed integer (big-endian).
    fn write_int(&mut self, val: i32) -> io::Result<()>;

    /// Write a 64-bit signed integer (big-endian).
    fn write_long(&mut self, val: i64) -> io::Result<()>;

    /// Write a 64-bit floating point number (big-endian).
    fn write_double(&mut self, val: f64) -> io::Result<()>;

    /// Write a byte array.
    fn write_byte_array(&mut self, arr: &[u8]) -> io::Result<()>;

    /// Write an unsigned varint (for sizes, lengths, counts).
    fn write_unsigned_varint(&mut self, val: u32) -> io::Result<()>;

    /// Write a signed varint (zig-zag encoded).
    fn write_varint(&mut self, val: i32) -> io::Result<()>;

    /// Write a signed varlong (zig-zag encoded).
    fn write_varlong(&mut self, val: i64) -> io::Result<()>;

    /// Write a UUID (128-bit value, most significant bits first).
    fn write_uuid(&mut self, uuid: &Uuid) -> io::Result<()> {
        self.write_long(uuid.most_sig_bits() as i64)?;
        self.write_long(uuid.least_sig_bits() as i64)?;
        Ok(())
    }

    /// Write bytes from a slice (convenience method).
    fn write_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.write_byte_array(data)
    }

    /// Write an unsigned 16-bit integer.
    fn write_unsigned_short(&mut self, val: u16) -> io::Result<()> {
        self.write_short(val as i16)
    }

    /// Write an unsigned 32-bit integer.
    fn write_unsigned_int(&mut self, val: u32) -> io::Result<()> {
        self.write_int(val as i32)
    }

    /// Write record bytes with zero-copy support.
    ///
    /// The default implementation copies the bytes into the main buffer.
    /// Scatter-gather implementations override this to store the buffer
    /// separately for vectored I/O, avoiding the copy.
    fn write_records(&mut self, data: Vec<u8>) -> io::Result<()> {
        self.write_byte_array(&data)
    }
}
