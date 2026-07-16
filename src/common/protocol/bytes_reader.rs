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

//! A read-only [`Readable`] backed by a refcounted [`bytes::Bytes`] buffer.
//!
//! This is the zero-copy counterpart to [`ByteBufferAccessor`] on the receive
//! path. Where `ByteBufferAccessor` owns a `Vec<u8>` and is used for both
//! reading and writing, `BytesReader` borrows from a single [`bytes::Bytes`]
//! that owns the whole network payload, so the wire `records` field can be
//! handed out as an O(1) refcounted slice (`Bytes::slice`) instead of being
//! copied (`consumer-threading.md` §27).
//!
//! [`ByteBufferAccessor`]: super::ByteBufferAccessor

use bytes::Bytes;
use std::io;

use super::Readable;
use super::varint;

/// A [`Readable`] over a [`bytes::Bytes`] buffer with position tracking.
///
/// Primitive reads behave exactly like [`ByteBufferAccessor`]'s read path; the
/// only difference is [`read_bytes_owned`](BytesReader::read_bytes_owned),
/// which returns a zero-copy `Bytes` slice of the backing buffer.
///
/// [`ByteBufferAccessor`]: super::ByteBufferAccessor
pub struct BytesReader {
    buf: Bytes,
    position: usize,
}

impl BytesReader {
    /// Create a new `BytesReader` reading from the start of `buf`.
    pub fn new(buf: Bytes) -> Self {
        BytesReader { buf, position: 0 }
    }

    /// Get the current position in the buffer.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Ensure we have at least `size` bytes available to read.
    ///
    /// The error message matches Java's `ByteBufferAccessor.readArray` format:
    /// "Error reading byte array of X byte(s): only Y byte(s) available".
    fn check_remaining(&self, size: usize) -> io::Result<()> {
        let remaining = self.buf.len() - self.position;
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

impl Readable for BytesReader {
    fn read_byte(&mut self) -> io::Result<i8> {
        self.check_remaining(1)?;
        let value = self.buf[self.position] as i8;
        self.position += 1;
        Ok(value)
    }

    fn read_short(&mut self) -> io::Result<i16> {
        self.check_remaining(2)?;
        let value = i16::from_be_bytes([self.buf[self.position], self.buf[self.position + 1]]);
        self.position += 2;
        Ok(value)
    }

    fn read_int(&mut self) -> io::Result<i32> {
        self.check_remaining(4)?;
        let value = i32::from_be_bytes([
            self.buf[self.position],
            self.buf[self.position + 1],
            self.buf[self.position + 2],
            self.buf[self.position + 3],
        ]);
        self.position += 4;
        Ok(value)
    }

    fn read_long(&mut self) -> io::Result<i64> {
        self.check_remaining(8)?;
        let value = i64::from_be_bytes([
            self.buf[self.position],
            self.buf[self.position + 1],
            self.buf[self.position + 2],
            self.buf[self.position + 3],
            self.buf[self.position + 4],
            self.buf[self.position + 5],
            self.buf[self.position + 6],
            self.buf[self.position + 7],
        ]);
        self.position += 8;
        Ok(value)
    }

    fn read_double(&mut self) -> io::Result<f64> {
        self.check_remaining(8)?;
        let value = f64::from_be_bytes([
            self.buf[self.position],
            self.buf[self.position + 1],
            self.buf[self.position + 2],
            self.buf[self.position + 3],
            self.buf[self.position + 4],
            self.buf[self.position + 5],
            self.buf[self.position + 6],
            self.buf[self.position + 7],
        ]);
        self.position += 8;
        Ok(value)
    }

    fn read_array(&mut self, length: usize) -> io::Result<Vec<u8>> {
        self.check_remaining(length)?;
        let arr = self.buf[self.position..self.position + length].to_vec();
        self.position += length;
        Ok(arr)
    }

    fn read_bytes_owned(&mut self, length: usize) -> io::Result<Bytes> {
        // Zero-copy: hand out a refcounted slice of the backing buffer instead
        // of allocating + copying. This is the §27 receive-path fast path used
        // for the wire `records` field.
        self.check_remaining(length)?;
        let slice = self.buf.slice(self.position..self.position + length);
        self.position += length;
        Ok(slice)
    }

    fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let length = buf.len();
        self.check_remaining(length)?;
        buf.copy_from_slice(&self.buf[self.position..self.position + length]);
        self.position += length;
        Ok(())
    }

    fn read_unsigned_varint(&mut self) -> io::Result<u32> {
        let (value, size) = varint::read_unsigned_varint(&self.buf[self.position..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.position += size;
        Ok(value)
    }

    fn read_varint(&mut self) -> io::Result<i32> {
        let (value, size) = varint::read_varint(&self.buf[self.position..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.position += size;
        Ok(value)
    }

    fn read_varlong(&mut self) -> io::Result<i64> {
        let (value, size) = varint::read_varlong(&self.buf[self.position..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.position += size;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.position
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Uuid;

    #[test]
    fn test_primitive_reads() {
        let mut data = Vec::new();
        data.push(42u8);
        data.extend_from_slice(&1000i16.to_be_bytes());
        data.extend_from_slice(&1_000_000i32.to_be_bytes());
        data.extend_from_slice(&1_000_000_000_000i64.to_be_bytes());

        let mut reader = BytesReader::new(Bytes::from(data));
        assert_eq!(reader.read_byte().unwrap(), 42);
        assert_eq!(reader.read_short().unwrap(), 1000);
        assert_eq!(reader.read_int().unwrap(), 1_000_000);
        assert_eq!(reader.read_long().unwrap(), 1_000_000_000_000);
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn test_read_array_matches_slice() {
        let data = vec![1u8, 2, 3, 4, 5];
        let mut reader = BytesReader::new(Bytes::from(data.clone()));
        let read = reader.read_array(5).unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn test_read_bytes_owned_is_zero_copy() {
        let data = Bytes::from(vec![10u8, 20, 30, 40, 50]);
        let mut reader = BytesReader::new(data.clone());
        // Skip the first byte, then take the next 3 as a refcounted slice.
        assert_eq!(reader.read_byte().unwrap(), 10);
        let slice = reader.read_bytes_owned(3).unwrap();
        assert_eq!(&slice[..], &[20, 30, 40]);
        // Slicing into the same allocation keeps the original buffer alive
        // without copying; remaining reflects consumed bytes.
        assert_eq!(reader.remaining(), 1);
        assert_eq!(reader.read_byte().unwrap(), 50);
    }

    #[test]
    fn test_varint_and_uuid_reads() {
        // Build a buffer via the writable accessor to ensure encodings match.
        use crate::common::protocol::{ByteBufferAccessor, Writable};
        let mut w = ByteBufferAccessor::new(64);
        w.write_unsigned_varint(16384).unwrap();
        w.write_varint(-300).unwrap();
        w.write_varlong(-1_000_000_000).unwrap();
        let uuid = Uuid::new(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        w.write_uuid(&uuid).unwrap();
        let buf = w.into_buffer();

        let mut reader = BytesReader::new(Bytes::from(buf));
        assert_eq!(reader.read_unsigned_varint().unwrap(), 16384);
        assert_eq!(reader.read_varint().unwrap(), -300);
        assert_eq!(reader.read_varlong().unwrap(), -1_000_000_000);
        assert_eq!(reader.read_uuid().unwrap(), uuid);
    }

    #[test]
    fn test_insufficient_data_error_message() {
        let mut reader = BytesReader::new(Bytes::from(vec![1u8, 2, 3]));
        let err = reader.read_array(5).unwrap_err();
        assert_eq!(
            "Error reading byte array of 5 byte(s): only 3 byte(s) available",
            err.to_string()
        );
    }
}
