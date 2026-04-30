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

//! Translation of `org.apache.kafka.common.protocol.ByteBufferAccessor`.
//!
//! Mirrors the Java class which wraps a `ByteBuffer` and implements both
//! `Readable` and `Writable`. In Rust, the underlying storage is a
//! [`Vec<u8>`] together with an explicit `position` and `limit`. This faithfully
//! emulates Java's `ByteBuffer` lifecycle:
//!
//! * Constructed in *write mode*: `position = 0`, `limit = capacity`. Writes
//!   advance `position`, expanding the buffer past the original capacity if
//!   needed (Java throws `BufferOverflowException` here; we instead grow,
//!   matching the practical use of `MessageUtil::toByteBufferAccessor` which
//!   always pre-sizes the buffer to the exact message size — the grow path is
//!   purely defensive).
//! * Calling [`flip`](Self::flip) switches to *read mode*: `limit = position`,
//!   `position = 0`.
//! * Reads consume bytes between `position` and `limit`.

use crate::common::errors::KafkaError;
use crate::common::protocol::{Readable, Writable};
use crate::common::utils::byte_utils;

/// Position-tracking accessor over a `Vec<u8>` that implements both
/// [`Readable`] and [`Writable`]. Mirrors `ByteBufferAccessor` in Java.
pub struct ByteBufferAccessor {
    buf: Vec<u8>,
    position: usize,
    limit: usize,
}

impl ByteBufferAccessor {
    /// Construct an accessor in *write mode* with the given capacity. Mirrors
    /// `new ByteBufferAccessor(ByteBuffer.allocate(size))`.
    pub fn allocate(size: usize) -> Self {
        ByteBufferAccessor { buf: vec![0u8; size], position: 0, limit: size }
    }

    /// Wrap an existing buffer in *read mode*. Mirrors
    /// `new ByteBufferAccessor(ByteBuffer.wrap(arr))`.
    pub fn wrap(buf: Vec<u8>) -> Self {
        let limit = buf.len();
        ByteBufferAccessor { buf, position: 0, limit }
    }

    /// Mirrors `ByteBuffer#flip()`. Sets `limit = position`, then `position = 0`.
    pub fn flip(&mut self) {
        self.limit = self.position;
        self.position = 0;
    }

    /// Mirrors `ByteBuffer#position()`.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Mirrors `ByteBuffer#position(int)`.
    pub fn set_position(&mut self, position: usize) {
        self.position = position;
    }

    /// Mirrors `ByteBuffer#limit()`.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Returns the underlying buffer up to the current limit. Mirrors
    /// `ByteBuffer#array()` when the buffer has been flipped — i.e. returns
    /// the bytes that have been written and are now ready to be sent.
    pub fn buffer(&self) -> &[u8] {
        &self.buf[..self.limit]
    }

    /// Returns the entire backing storage (positions ignored). Used by
    /// `MessageUtil::to_version_prefixed_bytes`.
    pub fn raw_buffer(&self) -> &[u8] {
        &self.buf
    }

    /// Returns the underlying buffer including its full capacity. Allows
    /// taking ownership of the storage when callers want a `Vec<u8>`.
    pub fn into_buffer(self) -> Vec<u8> {
        self.buf
    }

    fn ensure_remaining_read(&self, n: usize) -> Result<(), KafkaError> {
        let rem = self.remaining();
        if n > rem {
            return Err(KafkaError::Generic(format!(
                "Error reading byte array of {n} byte(s): only {rem} byte(s) available"
            )));
        }
        Ok(())
    }

    fn ensure_remaining_write(&mut self, n: usize) {
        let needed = self.position + n;
        if needed > self.buf.len() {
            self.buf.resize(needed, 0);
        }
        if needed > self.limit {
            self.limit = needed;
        }
    }
}

impl Readable for ByteBufferAccessor {
    fn read_byte(&mut self) -> Result<i8, KafkaError> {
        self.ensure_remaining_read(1)?;
        let v = self.buf[self.position] as i8;
        self.position += 1;
        Ok(v)
    }

    fn read_short(&mut self) -> Result<i16, KafkaError> {
        self.ensure_remaining_read(2)?;
        let bytes: [u8; 2] = self.buf[self.position..self.position + 2].try_into().unwrap();
        self.position += 2;
        Ok(i16::from_be_bytes(bytes))
    }

    fn read_int(&mut self) -> Result<i32, KafkaError> {
        self.ensure_remaining_read(4)?;
        let bytes: [u8; 4] = self.buf[self.position..self.position + 4].try_into().unwrap();
        self.position += 4;
        Ok(i32::from_be_bytes(bytes))
    }

    fn read_long(&mut self) -> Result<i64, KafkaError> {
        self.ensure_remaining_read(8)?;
        let bytes: [u8; 8] = self.buf[self.position..self.position + 8].try_into().unwrap();
        self.position += 8;
        Ok(i64::from_be_bytes(bytes))
    }

    fn read_double(&mut self) -> Result<f64, KafkaError> {
        self.ensure_remaining_read(8)?;
        let v = byte_utils::read_double_at(&self.buf, self.position);
        self.position += 8;
        Ok(v)
    }

    fn read_array(&mut self, length: usize) -> Result<Vec<u8>, KafkaError> {
        self.ensure_remaining_read(length)?;
        let v = self.buf[self.position..self.position + length].to_vec();
        self.position += length;
        Ok(v)
    }

    fn read_unsigned_varint(&mut self) -> Result<u32, KafkaError> {
        let (value, len) = byte_utils::read_unsigned_varint(&self.buf[self.position..self.limit])?;
        self.position += len;
        Ok(value)
    }

    fn read_byte_buffer(&mut self, length: usize) -> Result<Vec<u8>, KafkaError> {
        // Java returns a slice that shares storage with the source. The
        // producer-relevant call sites all consume the result before mutating
        // the source again, so an owned copy is semantically equivalent.
        self.read_array(length)
    }

    fn read_varint(&mut self) -> Result<i32, KafkaError> {
        let (value, len) = byte_utils::read_varint(&self.buf[self.position..self.limit])?;
        self.position += len;
        Ok(value)
    }

    fn read_varlong(&mut self) -> Result<i64, KafkaError> {
        let (value, len) = byte_utils::read_varlong(&self.buf[self.position..self.limit])?;
        self.position += len;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.position)
    }

    fn slice(&mut self) -> Result<Box<dyn Readable + '_>, KafkaError> {
        // Java's `ByteBuffer#slice()` returns a buffer that shares storage
        // with the source. We model this with a borrowed `SliceReadable` so
        // the parent buffer's data is reused without copying.
        Ok(Box::new(SliceReadable {
            data: &self.buf[self.position..self.limit],
            position: 0,
        }))
    }
}

impl Writable for ByteBufferAccessor {
    fn write_byte(&mut self, val: i8) {
        self.ensure_remaining_write(1);
        self.buf[self.position] = val as u8;
        self.position += 1;
    }

    fn write_short(&mut self, val: i16) {
        self.ensure_remaining_write(2);
        self.buf[self.position..self.position + 2].copy_from_slice(&val.to_be_bytes());
        self.position += 2;
    }

    fn write_int(&mut self, val: i32) {
        self.ensure_remaining_write(4);
        self.buf[self.position..self.position + 4].copy_from_slice(&val.to_be_bytes());
        self.position += 4;
    }

    fn write_long(&mut self, val: i64) {
        self.ensure_remaining_write(8);
        self.buf[self.position..self.position + 8].copy_from_slice(&val.to_be_bytes());
        self.position += 8;
    }

    fn write_double(&mut self, val: f64) {
        self.ensure_remaining_write(8);
        let bytes = val.to_be_bytes();
        self.buf[self.position..self.position + 8].copy_from_slice(&bytes);
        self.position += 8;
    }

    fn write_byte_array(&mut self, arr: &[u8]) {
        self.ensure_remaining_write(arr.len());
        self.buf[self.position..self.position + arr.len()].copy_from_slice(arr);
        self.position += arr.len();
    }

    fn write_unsigned_varint(&mut self, value: u32) {
        let mut tmp: Vec<u8> = Vec::with_capacity(5);
        byte_utils::write_unsigned_varint(value, &mut tmp);
        self.write_byte_array(&tmp);
    }

    fn write_byte_buffer(&mut self, buf: &[u8]) {
        self.write_byte_array(buf);
    }

    fn write_varint(&mut self, value: i32) {
        let mut tmp: Vec<u8> = Vec::with_capacity(5);
        byte_utils::write_varint(value, &mut tmp);
        self.write_byte_array(&tmp);
    }

    fn write_varlong(&mut self, value: i64) {
        let mut tmp: Vec<u8> = Vec::with_capacity(10);
        byte_utils::write_varlong(value, &mut tmp);
        self.write_byte_array(&tmp);
    }
}

/// Borrowed read-only view over a slice. Returned by
/// [`ByteBufferAccessor::slice`] and constructible directly via
/// [`SliceReadable::new`] for tests / network parsing.
pub struct SliceReadable<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> SliceReadable<'a> {
    /// Construct a `SliceReadable` over the entire slice. Convenience for
    /// callers that already have a `&[u8]` they want to read from.
    pub fn new(data: &'a [u8]) -> Self {
        SliceReadable { data, position: 0 }
    }

    /// Number of bytes already consumed from the slice. Mirrors
    /// `ByteBuffer#position()`.
    pub fn position(&self) -> usize {
        self.position
    }
}

impl<'a> SliceReadable<'a> {
    fn ensure(&self, n: usize) -> Result<(), KafkaError> {
        let rem = self.data.len().saturating_sub(self.position);
        if n > rem {
            return Err(KafkaError::Generic(format!(
                "Error reading byte array of {n} byte(s): only {rem} byte(s) available"
            )));
        }
        Ok(())
    }
}

impl Readable for SliceReadable<'_> {
    fn read_byte(&mut self) -> Result<i8, KafkaError> {
        self.ensure(1)?;
        let v = self.data[self.position] as i8;
        self.position += 1;
        Ok(v)
    }

    fn read_short(&mut self) -> Result<i16, KafkaError> {
        self.ensure(2)?;
        let bytes: [u8; 2] = self.data[self.position..self.position + 2].try_into().unwrap();
        self.position += 2;
        Ok(i16::from_be_bytes(bytes))
    }

    fn read_int(&mut self) -> Result<i32, KafkaError> {
        self.ensure(4)?;
        let bytes: [u8; 4] = self.data[self.position..self.position + 4].try_into().unwrap();
        self.position += 4;
        Ok(i32::from_be_bytes(bytes))
    }

    fn read_long(&mut self) -> Result<i64, KafkaError> {
        self.ensure(8)?;
        let bytes: [u8; 8] = self.data[self.position..self.position + 8].try_into().unwrap();
        self.position += 8;
        Ok(i64::from_be_bytes(bytes))
    }

    fn read_double(&mut self) -> Result<f64, KafkaError> {
        self.ensure(8)?;
        let v = byte_utils::read_double_at(self.data, self.position);
        self.position += 8;
        Ok(v)
    }

    fn read_array(&mut self, length: usize) -> Result<Vec<u8>, KafkaError> {
        self.ensure(length)?;
        let v = self.data[self.position..self.position + length].to_vec();
        self.position += length;
        Ok(v)
    }

    fn read_unsigned_varint(&mut self) -> Result<u32, KafkaError> {
        let (value, len) = byte_utils::read_unsigned_varint(&self.data[self.position..])?;
        self.position += len;
        Ok(value)
    }

    fn read_byte_buffer(&mut self, length: usize) -> Result<Vec<u8>, KafkaError> {
        self.read_array(length)
    }

    fn read_varint(&mut self) -> Result<i32, KafkaError> {
        let (value, len) = byte_utils::read_varint(&self.data[self.position..])?;
        self.position += len;
        Ok(value)
    }

    fn read_varlong(&mut self) -> Result<i64, KafkaError> {
        let (value, len) = byte_utils::read_varlong(&self.data[self.position..])?;
        self.position += len;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.position)
    }

    fn slice(&mut self) -> Result<Box<dyn Readable + '_>, KafkaError> {
        Ok(Box::new(SliceReadable { data: &self.data[self.position..], position: 0 }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translation of `ByteBufferAccessorTest#testReadArray`.
    #[test]
    fn read_array() {
        let mut accessor = ByteBufferAccessor::allocate(1024);
        let test_array: [u8; 3] = [0x4b, 0x61, 0x46];
        accessor.write_byte_array(&test_array);
        accessor.write_int(12345);
        accessor.flip();
        let read_array = accessor.read_array(3).expect("read 3 bytes");
        assert_eq!(read_array.as_slice(), test_array.as_slice());
        assert_eq!(accessor.read_int().expect("read int"), 12345);
        let err = accessor.read_array(3).expect_err("should overflow");
        assert_eq!(err.message(), "Error reading byte array of 3 byte(s): only 0 byte(s) available");
    }

    /// Translation of `ByteBufferAccessorTest#testReadString`.
    #[test]
    fn read_string() {
        let mut accessor = ByteBufferAccessor::allocate(1024);
        let test_array = "ABC".as_bytes();
        accessor.write_byte_array(test_array);
        accessor.flip();
        assert_eq!(accessor.read_string(3).expect("read string"), "ABC");
        let err = accessor.read_string(2).expect_err("should overflow");
        assert_eq!(err.message(), "Error reading byte array of 2 byte(s): only 0 byte(s) available");
    }

    #[test]
    fn read_short_int_long() {
        let mut accessor = ByteBufferAccessor::allocate(64);
        accessor.write_short(0x1234);
        accessor.write_int(0x0a0b0c0d);
        accessor.write_long(0x0102030405060708);
        accessor.flip();
        assert_eq!(accessor.read_short().unwrap(), 0x1234);
        assert_eq!(accessor.read_int().unwrap(), 0x0a0b0c0d);
        assert_eq!(accessor.read_long().unwrap(), 0x0102030405060708);
    }

    #[test]
    fn varints_round_trip() {
        let mut accessor = ByteBufferAccessor::allocate(64);
        accessor.write_unsigned_varint(300);
        accessor.write_varint(-7);
        accessor.write_varlong(-1_000_000_000);
        accessor.flip();
        assert_eq!(accessor.read_unsigned_varint().unwrap(), 300);
        assert_eq!(accessor.read_varint().unwrap(), -7);
        assert_eq!(accessor.read_varlong().unwrap(), -1_000_000_000);
    }

    #[test]
    fn double_round_trip() {
        let mut accessor = ByteBufferAccessor::allocate(8);
        accessor.write_double(std::f64::consts::PI);
        accessor.flip();
        assert_eq!(accessor.read_double().unwrap(), std::f64::consts::PI);
    }

    #[test]
    fn slice_starts_at_parent_position() {
        let mut accessor = ByteBufferAccessor::allocate(16);
        accessor.write_int(11);
        accessor.write_int(22);
        accessor.flip();
        // Parent already advanced once.
        accessor.read_int().unwrap();
        // Slice should start where parent currently is and read independently.
        // The slice borrows the parent's buffer; once it's dropped the parent
        // can be used again.
        {
            let mut sliced = accessor.slice().unwrap();
            assert_eq!(sliced.read_int().unwrap(), 22);
        }
        // Parent position must not have advanced because of the slice read.
        assert_eq!(accessor.read_int().unwrap(), 22);
    }

    #[test]
    fn uuid_round_trip() {
        let mut accessor = ByteBufferAccessor::allocate(16);
        let uuid = crate::common::Uuid::new(0x0102030405060708, 0x090a0b0c0d0e0f10);
        accessor.write_uuid(&uuid);
        accessor.flip();
        let decoded = accessor.read_uuid().unwrap();
        assert_eq!(decoded.most_significant_bits(), uuid.most_significant_bits());
        assert_eq!(decoded.least_significant_bits(), uuid.least_significant_bits());
    }
}
