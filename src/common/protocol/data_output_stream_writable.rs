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
//! `org.apache.kafka.common.protocol.DataOutputStreamWritable`.
//!
//! Java backs this writer with a `java.io.DataOutputStream`. In Rust we
//! target any type that implements [`std::io::Write`]; the framing matches
//! Java's big-endian `DataOutput` contract.

use std::io::Write;

use crate::common::errors::KafkaError;
use crate::common::protocol::Writable;
use crate::common::utils::byte_utils;

/// Adapter that turns a [`std::io::Write`] into a [`Writable`].
///
/// Java throws an unchecked `RuntimeException(IOException)` whenever the
/// underlying stream errors out. We instead surface the most recent IO
/// failure on demand via [`Self::take_error`]; the trait methods themselves
/// silently no-op after the first error so the call sequence terminates
/// without panicking. This keeps the public API non-panicking per CLAUDE.md
/// rule 10 while still preserving the Java contract for callers that decide
/// to check at the end.
pub struct DataOutputStreamWritable<'a> {
    out: &'a mut dyn Write,
    error: Option<KafkaError>,
}

impl<'a> DataOutputStreamWritable<'a> {
    /// Wrap a `Write` sink. Mirrors the Java single-argument constructor.
    pub fn new(out: &'a mut dyn Write) -> Self {
        DataOutputStreamWritable { out, error: None }
    }

    /// Mirrors `flush()`. Returns the first IO error encountered, if any.
    pub fn flush(&mut self) -> Result<(), KafkaError> {
        if let Some(err) = self.error.take() {
            return Err(err);
        }
        self.out.flush().map_err(|e| KafkaError::Generic(e.to_string()))
    }

    /// Returns the deferred error, if any. After this returns `Some`, the
    /// adapter is left in a clean state — subsequent writes record fresh
    /// errors.
    pub fn take_error(&mut self) -> Option<KafkaError> {
        self.error.take()
    }

    fn record_io<F>(&mut self, op: F)
    where
        F: FnOnce(&mut dyn Write) -> std::io::Result<()>,
    {
        if self.error.is_some() {
            return;
        }
        if let Err(e) = op(self.out) {
            self.error = Some(KafkaError::Generic(e.to_string()));
        }
    }
}

impl Writable for DataOutputStreamWritable<'_> {
    fn write_byte(&mut self, val: i8) {
        self.record_io(|w| w.write_all(&[val as u8]));
    }

    fn write_short(&mut self, val: i16) {
        self.record_io(|w| w.write_all(&val.to_be_bytes()));
    }

    fn write_int(&mut self, val: i32) {
        self.record_io(|w| w.write_all(&val.to_be_bytes()));
    }

    fn write_long(&mut self, val: i64) {
        self.record_io(|w| w.write_all(&val.to_be_bytes()));
    }

    fn write_double(&mut self, val: f64) {
        self.record_io(|w| w.write_all(&val.to_be_bytes()));
    }

    fn write_byte_array(&mut self, arr: &[u8]) {
        self.record_io(|w| w.write_all(arr));
    }

    fn write_unsigned_varint(&mut self, value: u32) {
        if self.error.is_some() {
            return;
        }
        if let Err(e) = byte_utils::write_unsigned_varint_to_stream(value, self.out) {
            self.error = Some(KafkaError::Generic(e.to_string()));
        }
    }

    fn write_byte_buffer(&mut self, buf: &[u8]) {
        self.write_byte_array(buf);
    }

    fn write_varint(&mut self, value: i32) {
        if self.error.is_some() {
            return;
        }
        if let Err(e) = byte_utils::write_varint_to_stream(value, self.out) {
            self.error = Some(KafkaError::Generic(e.to_string()));
        }
    }

    fn write_varlong(&mut self, value: i64) {
        if self.error.is_some() {
            return;
        }
        if let Err(e) = byte_utils::write_varlong_to_stream(value, self.out) {
            self.error = Some(KafkaError::Generic(e.to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translation of `DataOutputStreamWritableTest#testWritingSlicedByteBuffer`.
    ///
    /// Java's test exercises a `ByteBuffer` slice whose backing array has a
    /// non-zero offset. In Rust the input is just a `&[u8]`; we still verify
    /// that writing the slice ends up at offset 0 of the result buffer and
    /// the writer's logical "position" equals the number of bytes written.
    #[test]
    fn writing_sliced_byte_buffer() {
        let source: [u8; 4] = [0u8, 1, 2, 3];
        // Java does `sourceBuffer.position(2); sourceBuffer.slice();` →
        // resulting slice is `[2, 3]`.
        let sliced = &source[2..];
        let mut result_buffer = [0u8; 4];
        let mut cursor = std::io::Cursor::new(&mut result_buffer[..]);
        let mut writable = DataOutputStreamWritable::new(&mut cursor);
        writable.write_byte_buffer(sliced);
        writable.flush().expect("no error");
        let pos = cursor.position();
        assert_eq!(pos, 2);
        assert_eq!(&result_buffer, &[2, 3, 0, 0]);
    }

    /// Translation of
    /// `DataOutputStreamWritableTest#testWritingSlicedByteBufferWithNonZeroPosition`.
    #[test]
    fn writing_sliced_byte_buffer_with_non_zero_position() {
        let original: [u8; 4] = [0u8, 1, 2, 3];
        // Java: `originalBuffer.position(2); slice; slice.position(1);` →
        // remaining bytes are `[3]`.
        let sliced = &original[2..];
        let further = &sliced[1..];
        let mut result_buffer = [0u8; 4];
        let mut cursor = std::io::Cursor::new(&mut result_buffer[..]);
        let mut writable = DataOutputStreamWritable::new(&mut cursor);
        writable.write_byte_buffer(further);
        writable.flush().expect("no error");
        let pos = cursor.position();
        assert_eq!(pos, 1);
        assert_eq!(&result_buffer, &[3, 0, 0, 0]);
    }

    #[test]
    fn primitives_round_trip_through_byte_buffer_accessor() {
        let mut buf: Vec<u8> = Vec::new();
        let mut writable = DataOutputStreamWritable::new(&mut buf);
        writable.write_byte(0x12);
        writable.write_short(0x1234);
        writable.write_int(0x0a0b0c0d);
        writable.write_long(0x0102030405060708);
        writable.write_double(std::f64::consts::PI);
        writable.write_unsigned_varint(300);
        writable.write_varint(-7);
        writable.write_varlong(-1_000_000_000);
        writable.flush().unwrap();

        // Now read it back via ByteBufferAccessor.
        let mut accessor = crate::common::protocol::ByteBufferAccessor::wrap(buf);
        use crate::common::protocol::Readable;
        assert_eq!(accessor.read_byte().unwrap(), 0x12);
        assert_eq!(accessor.read_short().unwrap(), 0x1234);
        assert_eq!(accessor.read_int().unwrap(), 0x0a0b0c0d);
        assert_eq!(accessor.read_long().unwrap(), 0x0102030405060708);
        assert_eq!(accessor.read_double().unwrap(), std::f64::consts::PI);
        assert_eq!(accessor.read_unsigned_varint().unwrap(), 300);
        assert_eq!(accessor.read_varint().unwrap(), -7);
        assert_eq!(accessor.read_varlong().unwrap(), -1_000_000_000);
    }
}
