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

//! ByteBufferAccessor provides reading and writing to a byte buffer.
//!
//! Corresponds to org.apache.kafka.common.protocol.ByteBufferAccessor

use super::Readable;
use super::Writable;
use super::varint;
use std::io;

/// A struct that implements both Readable and Writable traits for a byte buffer.
///
/// This provides a mutable view into a byte slice with position tracking.
pub struct ByteBufferAccessor {
    buffer: Vec<u8>,
    position: usize,
}

impl ByteBufferAccessor {
    /// Create a new ByteBufferAccessor with the given capacity.
    pub fn new(capacity: usize) -> Self {
        ByteBufferAccessor { buffer: Vec::with_capacity(capacity), position: 0 }
    }

    /// Create a ByteBufferAccessor from existing bytes.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        ByteBufferAccessor { buffer: bytes, position: 0 }
    }

    /// Get the current position in the buffer.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Set the position in the buffer.
    ///
    /// # Errors
    /// Returns an error if the position is beyond the buffer length.
    pub fn set_position(&mut self, pos: usize) -> io::Result<()> {
        if pos > self.buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Position {} is beyond buffer length {}", pos, self.buffer.len()),
            ));
        }
        self.position = pos;
        Ok(())
    }

    /// Get a reference to the underlying buffer.
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Consume this accessor and return the underlying buffer.
    pub fn into_buffer(self) -> Vec<u8> {
        self.buffer
    }

    /// Get the total length of the buffer.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Flip the buffer for reading - resets position to 0.
    pub fn flip(&mut self) {
        self.position = 0;
    }

    /// Returns a new `ByteBufferAccessor` containing a copy of the remaining bytes
    /// from the current position to the end of the buffer.
    ///
    /// The new accessor's position is set to 0. The original accessor is unchanged.
    pub fn snapshot_remaining(&self) -> Self {
        let remaining = self.buffer[self.position..].to_vec();
        ByteBufferAccessor::from_bytes(remaining)
    }

    /// Ensure we have at least `size` bytes available to read.
    ///
    /// The error message matches Java's ByteBufferAccessor.readArray format:
    /// "Error reading byte array of X byte(s): only Y byte(s) available"
    fn check_remaining(&self, size: usize) -> io::Result<()> {
        let remaining = self.buffer.len() - self.position;
        if size > remaining {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "Error reading byte array of {} byte(s): only {} byte(s) available",
                    size, remaining
                ),
            ));
        }
        Ok(())
    }
}

impl Readable for ByteBufferAccessor {
    fn read_byte(&mut self) -> io::Result<i8> {
        self.check_remaining(1)?;
        let value = self.buffer[self.position] as i8;
        self.position += 1;
        Ok(value)
    }

    fn read_short(&mut self) -> io::Result<i16> {
        self.check_remaining(2)?;
        let value = i16::from_be_bytes([self.buffer[self.position], self.buffer[self.position + 1]]);
        self.position += 2;
        Ok(value)
    }

    fn read_int(&mut self) -> io::Result<i32> {
        self.check_remaining(4)?;
        let value = i32::from_be_bytes([
            self.buffer[self.position],
            self.buffer[self.position + 1],
            self.buffer[self.position + 2],
            self.buffer[self.position + 3],
        ]);
        self.position += 4;
        Ok(value)
    }

    fn read_long(&mut self) -> io::Result<i64> {
        self.check_remaining(8)?;
        let value = i64::from_be_bytes([
            self.buffer[self.position],
            self.buffer[self.position + 1],
            self.buffer[self.position + 2],
            self.buffer[self.position + 3],
            self.buffer[self.position + 4],
            self.buffer[self.position + 5],
            self.buffer[self.position + 6],
            self.buffer[self.position + 7],
        ]);
        self.position += 8;
        Ok(value)
    }

    fn read_double(&mut self) -> io::Result<f64> {
        self.check_remaining(8)?;
        let value = f64::from_be_bytes([
            self.buffer[self.position],
            self.buffer[self.position + 1],
            self.buffer[self.position + 2],
            self.buffer[self.position + 3],
            self.buffer[self.position + 4],
            self.buffer[self.position + 5],
            self.buffer[self.position + 6],
            self.buffer[self.position + 7],
        ]);
        self.position += 8;
        Ok(value)
    }

    fn read_array(&mut self, length: usize) -> io::Result<Vec<u8>> {
        self.check_remaining(length)?;
        let mut arr = vec![0u8; length];
        arr.copy_from_slice(&self.buffer[self.position..self.position + length]);
        self.position += length;
        Ok(arr)
    }

    fn read_unsigned_varint(&mut self) -> io::Result<u32> {
        let (value, size) = varint::read_unsigned_varint(&self.buffer[self.position..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.position += size;
        Ok(value)
    }

    fn read_varint(&mut self) -> io::Result<i32> {
        let (value, size) = varint::read_varint(&self.buffer[self.position..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.position += size;
        Ok(value)
    }

    fn read_varlong(&mut self) -> io::Result<i64> {
        let (value, size) = varint::read_varlong(&self.buffer[self.position..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.position += size;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.buffer.len() - self.position
    }
}

impl Writable for ByteBufferAccessor {
    fn write_byte(&mut self, val: i8) -> io::Result<()> {
        self.buffer.push(val as u8);
        Ok(())
    }

    fn write_short(&mut self, val: i16) -> io::Result<()> {
        self.buffer.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }

    fn write_int(&mut self, val: i32) -> io::Result<()> {
        self.buffer.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }

    fn write_long(&mut self, val: i64) -> io::Result<()> {
        self.buffer.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }

    fn write_double(&mut self, val: f64) -> io::Result<()> {
        self.buffer.extend_from_slice(&val.to_be_bytes());
        Ok(())
    }

    fn write_byte_array(&mut self, arr: &[u8]) -> io::Result<()> {
        self.buffer.extend_from_slice(arr);
        Ok(())
    }

    fn write_unsigned_varint(&mut self, val: u32) -> io::Result<()> {
        varint::write_unsigned_varint(val, &mut self.buffer)
    }

    fn write_varint(&mut self, val: i32) -> io::Result<()> {
        varint::write_varint(val, &mut self.buffer)
    }

    fn write_varlong(&mut self, val: i64) -> io::Result<()> {
        varint::write_varlong(val, &mut self.buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Uuid;

    #[test]
    fn test_read_write_byte() {
        let mut buf = ByteBufferAccessor::new(10);
        buf.write_byte(42).unwrap();
        buf.write_byte(-128).unwrap();

        buf.flip();
        assert_eq!(buf.read_byte().unwrap(), 42);
        assert_eq!(buf.read_byte().unwrap(), -128);
    }

    #[test]
    fn test_read_write_short() {
        let mut buf = ByteBufferAccessor::new(10);
        buf.write_short(1000).unwrap();
        buf.write_short(-1000).unwrap();

        buf.flip();
        assert_eq!(buf.read_short().unwrap(), 1000);
        assert_eq!(buf.read_short().unwrap(), -1000);
    }

    #[test]
    fn test_read_write_int() {
        let mut buf = ByteBufferAccessor::new(10);
        buf.write_int(1000000).unwrap();
        buf.write_int(-1000000).unwrap();

        buf.flip();
        assert_eq!(buf.read_int().unwrap(), 1000000);
        assert_eq!(buf.read_int().unwrap(), -1000000);
    }

    #[test]
    fn test_read_write_long() {
        let mut buf = ByteBufferAccessor::new(20);
        buf.write_long(1000000000000).unwrap();
        buf.write_long(-1000000000000).unwrap();

        buf.flip();
        assert_eq!(buf.read_long().unwrap(), 1000000000000);
        assert_eq!(buf.read_long().unwrap(), -1000000000000);
    }

    #[test]
    fn test_read_write_double() {
        let mut buf = ByteBufferAccessor::new(20);
        buf.write_double(std::f64::consts::PI).unwrap();
        buf.write_double(-std::f64::consts::E).unwrap();

        buf.flip();
        assert!((buf.read_double().unwrap() - std::f64::consts::PI).abs() < 0.00001);
        assert!((buf.read_double().unwrap() - (-std::f64::consts::E)).abs() < 0.00001);
    }

    #[test]
    fn test_read_write_array() {
        let mut buf = ByteBufferAccessor::new(20);
        let data = vec![1u8, 2, 3, 4, 5];
        buf.write_byte_array(&data).unwrap();

        buf.flip();
        let read_data = buf.read_array(5).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_read_write_varint() {
        let mut buf = ByteBufferAccessor::new(20);
        buf.write_varint(0).unwrap();
        buf.write_varint(1).unwrap();
        buf.write_varint(-1).unwrap();
        buf.write_varint(300).unwrap();
        buf.write_varint(-300).unwrap();

        buf.flip();
        assert_eq!(buf.read_varint().unwrap(), 0);
        assert_eq!(buf.read_varint().unwrap(), 1);
        assert_eq!(buf.read_varint().unwrap(), -1);
        assert_eq!(buf.read_varint().unwrap(), 300);
        assert_eq!(buf.read_varint().unwrap(), -300);
    }

    #[test]
    fn test_read_write_unsigned_varint() {
        let mut buf = ByteBufferAccessor::new(20);
        buf.write_unsigned_varint(0).unwrap();
        buf.write_unsigned_varint(127).unwrap();
        buf.write_unsigned_varint(128).unwrap();
        buf.write_unsigned_varint(16383).unwrap();
        buf.write_unsigned_varint(16384).unwrap();

        buf.flip();
        assert_eq!(buf.read_unsigned_varint().unwrap(), 0);
        assert_eq!(buf.read_unsigned_varint().unwrap(), 127);
        assert_eq!(buf.read_unsigned_varint().unwrap(), 128);
        assert_eq!(buf.read_unsigned_varint().unwrap(), 16383);
        assert_eq!(buf.read_unsigned_varint().unwrap(), 16384);
    }

    #[test]
    fn test_read_write_varlong() {
        let mut buf = ByteBufferAccessor::new(30);
        buf.write_varlong(0).unwrap();
        buf.write_varlong(1).unwrap();
        buf.write_varlong(-1).unwrap();
        buf.write_varlong(1000000000).unwrap();
        buf.write_varlong(-1000000000).unwrap();

        buf.flip();
        assert_eq!(buf.read_varlong().unwrap(), 0);
        assert_eq!(buf.read_varlong().unwrap(), 1);
        assert_eq!(buf.read_varlong().unwrap(), -1);
        assert_eq!(buf.read_varlong().unwrap(), 1000000000);
        assert_eq!(buf.read_varlong().unwrap(), -1000000000);
    }

    #[test]
    fn test_read_write_uuid() {
        let mut buf = ByteBufferAccessor::new(20);
        let uuid = Uuid::new(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        buf.write_uuid(&uuid).unwrap();

        buf.flip();
        let read_uuid = buf.read_uuid().unwrap();
        assert_eq!(read_uuid, uuid);
    }

    /// Verify exact byte representation of UUID on the wire.
    ///
    /// UUID must be serialized as MSB (big-endian i64) followed by LSB (big-endian i64).
    /// This test catches bugs where MSB/LSB order or endianness is wrong, which a
    /// round-trip test alone would not detect.
    #[test]
    fn test_uuid_wire_protocol_byte_representation() {
        // Test a known UUID with distinct bytes in each position
        let uuid = Uuid::new(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        let mut buf = ByteBufferAccessor::new(16);
        buf.write_uuid(&uuid).unwrap();

        let expected_bytes: [u8; 16] = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, // MSB big-endian
            0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10, // LSB big-endian
        ];
        assert_eq!(
            buf.buffer(),
            &expected_bytes,
            "UUID wire bytes do not match expected big-endian MSB-first layout"
        );

        // Verify zero UUID serializes to 16 zero bytes
        let zero_uuid = Uuid::ZERO_UUID;
        let mut buf2 = ByteBufferAccessor::new(16);
        buf2.write_uuid(&zero_uuid).unwrap();
        assert_eq!(buf2.buffer(), &[0u8; 16], "Zero UUID should serialize to 16 zero bytes");

        // Read back and verify round-trip
        buf.flip();
        let read_uuid = buf.read_uuid().unwrap();
        assert_eq!(read_uuid, uuid);

        buf2.flip();
        let read_zero = buf2.read_uuid().unwrap();
        assert_eq!(read_zero, zero_uuid);
    }

    #[test]
    fn test_read_write_string() {
        let mut buf = ByteBufferAccessor::new(20);
        let text = "Hello, Kafka!";
        buf.write_byte_array(text.as_bytes()).unwrap();

        buf.flip();
        let read_text = buf.read_string(text.len()).unwrap();
        assert_eq!(read_text, text);
    }

    #[test]
    fn test_remaining() {
        let mut buf = ByteBufferAccessor::new(10);
        buf.write_int(42).unwrap();
        buf.write_int(43).unwrap();

        buf.flip();
        assert_eq!(buf.remaining(), 8);
        buf.read_int().unwrap();
        assert_eq!(buf.remaining(), 4);
        buf.read_int().unwrap();
        assert_eq!(buf.remaining(), 0);
    }

    #[test]
    fn test_position() {
        let mut buf = ByteBufferAccessor::new(10);
        buf.write_int(42).unwrap();
        assert_eq!(buf.len(), 4);

        buf.flip();
        assert_eq!(buf.position(), 0);
        buf.read_byte().unwrap();
        assert_eq!(buf.position(), 1);

        buf.set_position(0).unwrap();
        assert_eq!(buf.position(), 0);
    }

    #[test]
    fn test_snapshot_remaining() {
        let mut buf = ByteBufferAccessor::new(20);
        buf.write_int(1).unwrap();
        buf.write_int(2).unwrap();
        buf.write_int(3).unwrap();

        buf.flip();
        // Read the first int to advance position
        assert_eq!(buf.read_int().unwrap(), 1);

        // Snapshot remaining should have ints 2 and 3
        let mut snapshot = buf.snapshot_remaining();
        assert_eq!(snapshot.remaining(), 8);
        assert_eq!(snapshot.position(), 0);
        assert_eq!(snapshot.read_int().unwrap(), 2);
        assert_eq!(snapshot.read_int().unwrap(), 3);
        assert_eq!(snapshot.remaining(), 0);

        // Original buffer should be unchanged
        assert_eq!(buf.remaining(), 8);
        assert_eq!(buf.read_int().unwrap(), 2);
    }

    #[test]
    fn test_snapshot_remaining_empty() {
        let mut buf = ByteBufferAccessor::from_bytes(vec![1, 2]);
        buf.read_byte().unwrap();
        buf.read_byte().unwrap();

        let snapshot = buf.snapshot_remaining();
        assert_eq!(snapshot.remaining(), 0);
        assert!(snapshot.is_empty());
    }

    #[test]
    fn test_insufficient_data() {
        let mut buf = ByteBufferAccessor::from_bytes(vec![1, 2, 3]);
        assert!(buf.read_int().is_err());
    }

    /// Translated from Java ByteBufferAccessorTest.testReadArray.
    /// Writes an array and an int, reads them back, then verifies error
    /// message when reading beyond available bytes.
    #[test]
    fn test_read_array_error_message() {
        let mut accessor = ByteBufferAccessor::new(1024);
        let test_array: Vec<u8> = vec![0x4b, 0x61, 0x46];
        accessor.write_byte_array(&test_array).unwrap();
        accessor.write_int(12345).unwrap();
        accessor.flip();

        let test_array2 = accessor.read_array(3).unwrap();
        assert_eq!(test_array, test_array2);
        assert_eq!(12345, accessor.read_int().unwrap());

        let err = accessor.read_array(3).unwrap_err();
        assert_eq!(
            "Error reading byte array of 3 byte(s): only 0 byte(s) available",
            err.to_string()
        );
    }

    /// Translated from Java ByteBufferAccessorTest.testReadString.
    /// Writes a string's bytes, reads it back, then verifies error
    /// message when reading beyond available bytes.
    #[test]
    fn test_read_string_error_message() {
        let mut accessor = ByteBufferAccessor::new(1024);
        let test_string = "ABC";
        let test_array = test_string.as_bytes();
        accessor.write_byte_array(test_array).unwrap();
        accessor.flip();

        assert_eq!("ABC", accessor.read_string(3).unwrap());

        let err = accessor.read_string(2).unwrap_err();
        assert_eq!(
            "Error reading byte array of 2 byte(s): only 0 byte(s) available",
            err.to_string()
        );
    }
}
