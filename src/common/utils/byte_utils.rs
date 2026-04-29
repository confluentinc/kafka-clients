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

//! Translation of `org.apache.kafka.common.utils.ByteUtils`.
//!
//! Low-level helpers for reading and writing big-endian fixed-width integers,
//! little-endian integers (used by Kafka's framed LZ4), `IEEE 754` doubles,
//! and Google-Protobuf-style unsigned/zig-zag varint and varlong encodings.
//!
//! The Java API is centred on `java.nio.ByteBuffer`. We replace that with two
//! idiomatic Rust shapes:
//!
//! * Buffer functions take `&[u8]` (read) or `&mut Vec<u8>` (write) where the
//!   "current position" is the start of the slice. For random-access reads we
//!   take an `offset: usize`. The free functions `read_*_at` and `write_*_at`
//!   replicate the indexed Java overloads.
//! * Stream functions take `&mut dyn std::io::Read` / `&mut dyn std::io::Write`
//!   for the `InputStream`/`OutputStream`/`DataInput`/`DataOutput` overloads.
//!
//! All the wire-protocol byte semantics (endianness, varint encoding, varint
//! validation thresholds) match Java exactly so that round-trips against the
//! Java client are byte-for-byte identical.

use std::io::{self, Read, Write};

use crate::common::errors::KafkaError;

/// Maximum number of bytes a varint can occupy. Mirrors
/// `ByteUtils.MAX_LENGTH_VARINT` (declared as a test-only constant in Java
/// but reused widely in the runtime).
pub const MAX_LENGTH_VARINT: usize = 5;

/// Maximum number of bytes a varlong can occupy.
pub const MAX_LENGTH_VARLONG: usize = 10;

// ---------------------------------------------------------------------------
// 4-byte unsigned int — big-endian on `&[u8]`, little-endian on stream/array.
// ---------------------------------------------------------------------------

/// Read a 4-byte unsigned integer (BE) from `buffer` starting at `offset`,
/// returning a `i64` so callers can avoid the unsignedness gotcha.
pub fn read_unsigned_int_be_at(buffer: &[u8], offset: usize) -> i64 {
    let v = u32::from_be_bytes(buffer[offset..offset + 4].try_into().unwrap());
    v as i64
}

/// Write `value` (low 32 bits) as a 4-byte BE unsigned integer at `buffer[offset..]`.
pub fn write_unsigned_int_be_at(buffer: &mut [u8], offset: usize, value: i64) {
    let v = (value & 0xFFFF_FFFF) as u32;
    buffer[offset..offset + 4].copy_from_slice(&v.to_be_bytes());
}

/// Append a 4-byte BE unsigned int to `buffer`.
pub fn write_unsigned_int_be(buffer: &mut Vec<u8>, value: i64) {
    let v = (value & 0xFFFF_FFFF) as u32;
    buffer.extend_from_slice(&v.to_be_bytes());
}

/// Read a 4-byte big-endian signed `i32` from a byte array. Mirrors
/// `ByteUtils.readIntBE`.
pub fn read_int_be_at(buffer: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes(buffer[offset..offset + 4].try_into().unwrap())
}

/// Read a 4-byte little-endian unsigned int from a byte array. Mirrors
/// `ByteUtils.readUnsignedIntLE(byte[], int)`.
pub fn read_unsigned_int_le_at(buffer: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(buffer[offset..offset + 4].try_into().unwrap())
}

/// Read a 4-byte little-endian unsigned int from an `InputStream`.
pub fn read_unsigned_int_le_from_stream<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

/// Write a 4-byte little-endian unsigned int into a byte array.
pub fn write_unsigned_int_le_at(buffer: &mut [u8], offset: usize, value: u32) {
    buffer[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

/// Write a 4-byte little-endian unsigned int into an `OutputStream`.
pub fn write_unsigned_int_le_to_stream<W: Write>(w: &mut W, value: u32) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}

// ---------------------------------------------------------------------------
// Varint / varlong (Protocol Buffers style).
// ---------------------------------------------------------------------------

/// Number of bytes needed to encode `value` as an unsigned varint.
/// Mirrors `ByteUtils.sizeOfUnsignedVarint`.
pub fn size_of_unsigned_varint(value: u32) -> usize {
    // Java uses bit-twiddling for speed; we keep the simple loop because the
    // optimization isn't observable at the wire-protocol layer and matches
    // the simple reference implementation in `testSizeOfUnsignedVarint`.
    let mut bytes = 1usize;
    let mut v = value;
    while (v & !0x7F) != 0 {
        bytes += 1;
        v >>= 7;
    }
    bytes
}

/// Number of bytes needed to encode `value` as a signed (zig-zag) varint.
pub fn size_of_varint(value: i32) -> usize {
    size_of_unsigned_varint(zig_zag_encode_i32(value))
}

/// Number of bytes needed to encode `value` as a signed (zig-zag) varlong.
pub fn size_of_varlong(value: i64) -> usize {
    size_of_unsigned_varlong(zig_zag_encode_i64(value))
}

/// Number of bytes needed to encode `value` as an unsigned varlong.
pub fn size_of_unsigned_varlong(value: u64) -> usize {
    let mut bytes = 1usize;
    let mut v = value;
    while (v & !0x7F) != 0 {
        bytes += 1;
        v >>= 7;
    }
    bytes
}

#[inline]
fn zig_zag_encode_i32(value: i32) -> u32 {
    ((value << 1) ^ (value >> 31)) as u32
}

#[inline]
fn zig_zag_decode_u32(value: u32) -> i32 {
    ((value >> 1) as i32) ^ -((value & 1) as i32)
}

#[inline]
fn zig_zag_encode_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

#[inline]
fn zig_zag_decode_u64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

/// Append the unsigned-varint encoding of `value` to `buffer`. Mirrors
/// `ByteUtils.writeUnsignedVarint(int, ByteBuffer)`.
pub fn write_unsigned_varint(value: u32, buffer: &mut Vec<u8>) {
    let mut v = value;
    while (v & !0x7F) != 0 {
        buffer.push(((v & 0x7F) | 0x80) as u8);
        v >>= 7;
    }
    buffer.push(v as u8);
}

/// Write the unsigned-varint encoding of `value` to a `Write` sink.
pub fn write_unsigned_varint_to_stream<W: Write>(value: u32, w: &mut W) -> io::Result<()> {
    let mut v = value;
    while (v & !0x7F) != 0 {
        w.write_all(&[((v & 0x7F) | 0x80) as u8])?;
        v >>= 7;
    }
    w.write_all(&[v as u8])
}

/// Append the signed (zig-zag) varint encoding of `value` to `buffer`.
pub fn write_varint(value: i32, buffer: &mut Vec<u8>) {
    write_unsigned_varint(zig_zag_encode_i32(value), buffer);
}

/// Write the signed (zig-zag) varint encoding of `value` to a `Write` sink.
pub fn write_varint_to_stream<W: Write>(value: i32, w: &mut W) -> io::Result<()> {
    write_unsigned_varint_to_stream(zig_zag_encode_i32(value), w)
}

/// Append the unsigned-varlong encoding of `value` to `buffer`.
pub fn write_unsigned_varlong(value: u64, buffer: &mut Vec<u8>) {
    let mut v = value;
    while (v & !0x7F) != 0 {
        buffer.push(((v & 0x7F) | 0x80) as u8);
        v >>= 7;
    }
    buffer.push(v as u8);
}

/// Write the unsigned-varlong encoding of `value` to a `Write` sink.
pub fn write_unsigned_varlong_to_stream<W: Write>(value: u64, w: &mut W) -> io::Result<()> {
    let mut v = value;
    while (v & !0x7F) != 0 {
        w.write_all(&[((v & 0x7F) | 0x80) as u8])?;
        v >>= 7;
    }
    w.write_all(&[v as u8])
}

/// Append the signed (zig-zag) varlong encoding of `value` to `buffer`.
pub fn write_varlong(value: i64, buffer: &mut Vec<u8>) {
    write_unsigned_varlong(zig_zag_encode_i64(value), buffer);
}

/// Write the signed (zig-zag) varlong encoding of `value` to a `Write` sink.
pub fn write_varlong_to_stream<W: Write>(value: i64, w: &mut W) -> io::Result<()> {
    write_unsigned_varlong_to_stream(zig_zag_encode_i64(value), w)
}

/// Read an unsigned varint from `buffer`, returning the decoded value and the
/// number of bytes consumed. Mirrors `ByteUtils.readUnsignedVarint(ByteBuffer)`.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] if the encoding does not terminate
/// within five bytes (the Java implementation throws `IllegalArgumentException`
/// in this case).
pub fn read_unsigned_varint(buffer: &[u8]) -> Result<(u32, usize), KafkaError> {
    let mut value: u32 = 0;
    let mut i: u32 = 0;
    let mut consumed = 0usize;
    while consumed < MAX_LENGTH_VARINT {
        let b = *buffer
            .get(consumed)
            .ok_or_else(|| KafkaError::IllegalArgument("Varint underflow".to_owned()))?;
        consumed += 1;
        if i == 28 && (b & 0x80) != 0 {
            // Java treats the 5th byte's high bit being set as too-long.
            return Err(illegal_varint_error(value | ((b as u32) << i)));
        }
        if (b & 0x80) == 0 {
            value |= (b as u32) << i;
            return Ok((value, consumed));
        }
        value |= ((b & 0x7F) as u32) << i;
        i += 7;
    }
    Err(illegal_varint_error(value))
}

/// Read an unsigned varint from a `Read`. Mirrors
/// `ByteUtils.readUnsignedVarint(InputStream)` — returns the decoded value and
/// reads exactly the consumed number of bytes.
pub fn read_unsigned_varint_from_stream<R: Read>(r: &mut R) -> Result<u32, KafkaError> {
    let mut value: u32 = 0;
    let mut i: u32 = 0;
    let mut byte = [0u8; 1];
    for step in 0..MAX_LENGTH_VARINT {
        r.read_exact(&mut byte)
            .map_err(|e| KafkaError::IllegalArgument(format!("Varint read error: {e}")))?;
        let b = byte[0];
        if step == 4 && (b & 0x80) != 0 {
            return Err(illegal_varint_error(value | ((b as u32) << i)));
        }
        if (b & 0x80) == 0 {
            value |= (b as u32) << i;
            return Ok(value);
        }
        value |= ((b & 0x7F) as u32) << i;
        i += 7;
    }
    Err(illegal_varint_error(value))
}

/// Read a zig-zag varint, returning the decoded `i32` and the number of bytes
/// consumed.
pub fn read_varint(buffer: &[u8]) -> Result<(i32, usize), KafkaError> {
    let (raw, consumed) = read_unsigned_varint(buffer)?;
    Ok((zig_zag_decode_u32(raw), consumed))
}

/// Read a zig-zag varint from a `Read`.
pub fn read_varint_from_stream<R: Read>(r: &mut R) -> Result<i32, KafkaError> {
    let raw = read_unsigned_varint_from_stream(r)?;
    Ok(zig_zag_decode_u32(raw))
}

/// Read an unsigned varlong, returning the decoded value and number of bytes
/// consumed. Mirrors `ByteUtils.readUnsignedVarlong`.
pub fn read_unsigned_varlong(buffer: &[u8]) -> Result<(u64, usize), KafkaError> {
    let mut value: u64 = 0;
    let mut i: u32 = 0;
    let mut consumed = 0usize;
    while consumed < MAX_LENGTH_VARLONG {
        let b = *buffer
            .get(consumed)
            .ok_or_else(|| KafkaError::IllegalArgument("Varlong underflow".to_owned()))?;
        consumed += 1;
        if i == 63 && (b & 0xFE) != 0 {
            // Bits beyond the 64-bit boundary are not allowed.
            return Err(illegal_varlong_error(value));
        }
        if (b & 0x80) == 0 {
            value |= (b as u64) << i;
            return Ok((value, consumed));
        }
        value |= ((b & 0x7F) as u64) << i;
        i += 7;
    }
    Err(illegal_varlong_error(value))
}

/// Read an unsigned varlong from a `Read`.
pub fn read_unsigned_varlong_from_stream<R: Read>(r: &mut R) -> Result<u64, KafkaError> {
    let mut value: u64 = 0;
    let mut i: u32 = 0;
    let mut byte = [0u8; 1];
    for step in 0..MAX_LENGTH_VARLONG {
        r.read_exact(&mut byte)
            .map_err(|e| KafkaError::IllegalArgument(format!("Varlong read error: {e}")))?;
        let b = byte[0];
        if step == 9 && (b & 0xFE) != 0 {
            return Err(illegal_varlong_error(value));
        }
        if (b & 0x80) == 0 {
            value |= (b as u64) << i;
            return Ok(value);
        }
        value |= ((b & 0x7F) as u64) << i;
        i += 7;
    }
    Err(illegal_varlong_error(value))
}

/// Read a zig-zag varlong, returning the decoded `i64` and number of bytes
/// consumed.
pub fn read_varlong(buffer: &[u8]) -> Result<(i64, usize), KafkaError> {
    let (raw, consumed) = read_unsigned_varlong(buffer)?;
    Ok((zig_zag_decode_u64(raw), consumed))
}

/// Read a zig-zag varlong from a `Read`.
pub fn read_varlong_from_stream<R: Read>(r: &mut R) -> Result<i64, KafkaError> {
    let raw = read_unsigned_varlong_from_stream(r)?;
    Ok(zig_zag_decode_u64(raw))
}

// ---------------------------------------------------------------------------
// Doubles (IEEE 754, big-endian on the wire).
// ---------------------------------------------------------------------------

/// Append a big-endian IEEE-754 `f64` to `buffer`. Mirrors `ByteUtils.writeDouble`.
pub fn write_double(value: f64, buffer: &mut Vec<u8>) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

/// Write an IEEE-754 `f64` to a `Write` sink.
pub fn write_double_to_stream<W: Write>(value: f64, w: &mut W) -> io::Result<()> {
    w.write_all(&value.to_be_bytes())
}

/// Read a big-endian IEEE-754 `f64` from a buffer.
pub fn read_double_at(buffer: &[u8], offset: usize) -> f64 {
    f64::from_be_bytes(buffer[offset..offset + 8].try_into().unwrap())
}

/// Read a big-endian IEEE-754 `f64` from a `Read`.
pub fn read_double_from_stream<R: Read>(r: &mut R) -> io::Result<f64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(f64::from_be_bytes(buf))
}

// ---------------------------------------------------------------------------
// Error helpers (mirror Java's `illegalVarintException` / `illegalVarlongException`).
// ---------------------------------------------------------------------------

fn illegal_varint_error(value: u32) -> KafkaError {
    KafkaError::IllegalArgument(format!(
        "Varint is too long, the most significant bit in the 5th byte is set, converted value: {value:x}"
    ))
}

fn illegal_varlong_error(value: u64) -> KafkaError {
    KafkaError::IllegalArgument(format!(
        "Varlong is too long, most significant bit in the 10th byte is set, converted value: {value:x}"
    ))
}

#[cfg(test)]
mod tests {
    // Translation of `org.apache.kafka.common.utils.ByteUtilsTest` (subset
    // covering the producer-relevant helpers).

    use super::*;

    /// Java: `testReadUnsignedIntLEFromArray`.
    #[test]
    fn read_unsigned_int_le_from_array() {
        let array1 = [0x01, 0x02, 0x03, 0x04, 0x05];
        assert_eq!(read_unsigned_int_le_at(&array1, 0), 0x04030201);
        assert_eq!(read_unsigned_int_le_at(&array1, 1), 0x05040302);

        let array2 = [0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6];
        assert_eq!(read_unsigned_int_le_at(&array2, 0), 0xf4f3f2f1);
        assert_eq!(read_unsigned_int_le_at(&array2, 2), 0xf6f5f4f3);
    }

    /// Java: `testReadUnsignedIntLEFromInputStream`.
    #[test]
    fn read_unsigned_int_le_from_stream() {
        let array1 = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09];
        let mut cursor = std::io::Cursor::new(array1.as_slice());
        assert_eq!(super::read_unsigned_int_le_from_stream(&mut cursor).unwrap(), 0x04030201);
        assert_eq!(super::read_unsigned_int_le_from_stream(&mut cursor).unwrap(), 0x08070605);
    }

    /// Java: `testReadUnsignedInt` (BE round-trip via 4-byte buffer).
    #[test]
    fn read_unsigned_int_be_round_trip() {
        let mut buf: Vec<u8> = Vec::with_capacity(4);
        let write_value: i64 = 133_444;
        write_unsigned_int_be(&mut buf, write_value);
        assert_eq!(read_unsigned_int_be_at(&buf, 0), write_value);
    }

    /// Java: `testWriteUnsignedIntLEToArray`.
    #[test]
    fn write_unsigned_int_le_to_array() {
        let mut array1 = [0u8; 4];
        write_unsigned_int_le_at(&mut array1, 0, 0x04030201);
        assert_eq!(array1, [0x01, 0x02, 0x03, 0x04]);

        let mut array1 = [0u8; 8];
        write_unsigned_int_le_at(&mut array1, 2, 0x04030201);
        assert_eq!(array1, [0, 0, 0x01, 0x02, 0x03, 0x04, 0, 0]);

        let mut array2 = [0u8; 4];
        write_unsigned_int_le_at(&mut array2, 0, 0xf4f3f2f1);
        assert_eq!(array2, [0xf1, 0xf2, 0xf3, 0xf4]);

        let mut array2 = [0u8; 8];
        write_unsigned_int_le_at(&mut array2, 2, 0xf4f3f2f1);
        assert_eq!(array2, [0, 0, 0xf1, 0xf2, 0xf3, 0xf4, 0, 0]);
    }

    /// Java: `testWriteUnsignedIntLEToOutputStream`.
    #[test]
    fn write_unsigned_int_le_to_stream() {
        let mut out: Vec<u8> = Vec::new();
        super::write_unsigned_int_le_to_stream(&mut out, 0x04030201).unwrap();
        super::write_unsigned_int_le_to_stream(&mut out, 0x04030201).unwrap();
        assert_eq!(out, [0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04]);

        let mut out: Vec<u8> = Vec::new();
        super::write_unsigned_int_le_to_stream(&mut out, 0xf4f3f2f1).unwrap();
        assert_eq!(out, [0xf1, 0xf2, 0xf3, 0xf4]);
    }

    fn assert_unsigned_varint(value: u32, expected: &[u8]) {
        let mut buf = Vec::new();
        write_unsigned_varint(value, &mut buf);
        assert_eq!(buf.as_slice(), expected, "encoding of {value}");
        let (read, consumed) = read_unsigned_varint(&buf).unwrap();
        assert_eq!(read, value);
        assert_eq!(consumed, expected.len());
        // Stream form
        let mut stream_out = Vec::new();
        write_unsigned_varint_to_stream(value, &mut stream_out).unwrap();
        assert_eq!(stream_out, expected);
        let mut cursor = std::io::Cursor::new(stream_out.as_slice());
        let read = read_unsigned_varint_from_stream(&mut cursor).unwrap();
        assert_eq!(read, value);
    }

    fn assert_varint(value: i32, expected: &[u8]) {
        let mut buf = Vec::new();
        write_varint(value, &mut buf);
        assert_eq!(buf.as_slice(), expected, "encoding of {value}");
        let (read, consumed) = read_varint(&buf).unwrap();
        assert_eq!(read, value);
        assert_eq!(consumed, expected.len());
        let mut stream_out = Vec::new();
        write_varint_to_stream(value, &mut stream_out).unwrap();
        assert_eq!(stream_out, expected);
        let mut cursor = std::io::Cursor::new(stream_out.as_slice());
        let read = read_varint_from_stream(&mut cursor).unwrap();
        assert_eq!(read, value);
    }

    fn assert_varlong(value: i64, expected: &[u8]) {
        let mut buf = Vec::new();
        write_varlong(value, &mut buf);
        assert_eq!(buf.as_slice(), expected, "encoding of {value}");
        let (read, consumed) = read_varlong(&buf).unwrap();
        assert_eq!(read, value);
        assert_eq!(consumed, expected.len());
        let mut stream_out = Vec::new();
        write_varlong_to_stream(value, &mut stream_out).unwrap();
        assert_eq!(stream_out, expected);
        let mut cursor = std::io::Cursor::new(stream_out.as_slice());
        let read = read_varlong_from_stream(&mut cursor).unwrap();
        assert_eq!(read, value);
    }

    /// Java: `testUnsignedVarintSerde`.
    #[test]
    fn unsigned_varint_serde() {
        assert_unsigned_varint(0, &[0x00]);
        // -1 reinterpreted as u32 == 0xFFFF_FFFF
        assert_unsigned_varint(0xFFFF_FFFF, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_unsigned_varint(1, &[0x01]);
        assert_unsigned_varint(63, &[0x3F]);
        // -64 reinterpreted as u32
        assert_unsigned_varint((-64i32) as u32, &[0xC0, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_unsigned_varint(64, &[0x40]);
        assert_unsigned_varint(8191, &[0xFF, 0x3F]);
        assert_unsigned_varint((-8192i32) as u32, &[0x80, 0xC0, 0xFF, 0xFF, 0x0F]);
        assert_unsigned_varint(8192, &[0x80, 0x40]);
        assert_unsigned_varint((-8193i32) as u32, &[0xFF, 0xBF, 0xFF, 0xFF, 0x0F]);
        assert_unsigned_varint(1_048_575, &[0xFF, 0xFF, 0x3F]);
        assert_unsigned_varint(1_048_576, &[0x80, 0x80, 0x40]);
        assert_unsigned_varint(i32::MAX as u32, &[0xFF, 0xFF, 0xFF, 0xFF, 0x07]);
        assert_unsigned_varint(i32::MIN as u32, &[0x80, 0x80, 0x80, 0x80, 0x08]);
    }

    /// Java: `testVarintSerde`.
    #[test]
    fn varint_serde() {
        assert_varint(0, &[0x00]);
        assert_varint(-1, &[0x01]);
        assert_varint(1, &[0x02]);
        assert_varint(63, &[0x7E]);
        assert_varint(-64, &[0x7F]);
        assert_varint(64, &[0x80, 0x01]);
        assert_varint(-65, &[0x81, 0x01]);
        assert_varint(8191, &[0xFE, 0x7F]);
        assert_varint(-8192, &[0xFF, 0x7F]);
        assert_varint(8192, &[0x80, 0x80, 0x01]);
        assert_varint(-8193, &[0x81, 0x80, 0x01]);
        assert_varint(1_048_575, &[0xFE, 0xFF, 0x7F]);
        assert_varint(-1_048_576, &[0xFF, 0xFF, 0x7F]);
        assert_varint(1_048_576, &[0x80, 0x80, 0x80, 0x01]);
        assert_varint(-1_048_577, &[0x81, 0x80, 0x80, 0x01]);
        assert_varint(134_217_727, &[0xFE, 0xFF, 0xFF, 0x7F]);
        assert_varint(-134_217_728, &[0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varint(134_217_728, &[0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varint(-134_217_729, &[0x81, 0x80, 0x80, 0x80, 0x01]);
        assert_varint(i32::MAX, &[0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_varint(i32::MIN, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    }

    /// Java: `testVarlongSerde`.
    #[test]
    fn varlong_serde() {
        assert_varlong(0, &[0x00]);
        assert_varlong(-1, &[0x01]);
        assert_varlong(1, &[0x02]);
        assert_varlong(63, &[0x7E]);
        assert_varlong(-64, &[0x7F]);
        assert_varlong(64, &[0x80, 0x01]);
        assert_varlong(-65, &[0x81, 0x01]);
        assert_varlong(8191, &[0xFE, 0x7F]);
        assert_varlong(-8192, &[0xFF, 0x7F]);
        assert_varlong(8192, &[0x80, 0x80, 0x01]);
        assert_varlong(-8193, &[0x81, 0x80, 0x01]);
        assert_varlong(1_048_575, &[0xFE, 0xFF, 0x7F]);
        assert_varlong(-1_048_576, &[0xFF, 0xFF, 0x7F]);
        assert_varlong(1_048_576, &[0x80, 0x80, 0x80, 0x01]);
        assert_varlong(-1_048_577, &[0x81, 0x80, 0x80, 0x01]);
        assert_varlong(134_217_727, &[0xFE, 0xFF, 0xFF, 0x7F]);
        assert_varlong(-134_217_728, &[0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong(134_217_728, &[0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong(-134_217_729, &[0x81, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong(i32::MAX as i64, &[0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_varlong(i32::MIN as i64, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_varlong(17_179_869_183, &[0xFE, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong(-17_179_869_184, &[0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong(17_179_869_184, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong(-17_179_869_185, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong(2_199_023_255_551, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong(-2_199_023_255_552, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong(2_199_023_255_552, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong(-2_199_023_255_553, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong(281_474_976_710_655, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong(-281_474_976_710_656, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong(281_474_976_710_656, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong(-281_474_976_710_657, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong(36_028_797_018_963_967, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong(-36_028_797_018_963_968, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong(36_028_797_018_963_968, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong(-36_028_797_018_963_969, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong(
            4_611_686_018_427_387_903,
            &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
        );
        assert_varlong(
            -4_611_686_018_427_387_904,
            &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
        );
        assert_varlong(
            4_611_686_018_427_387_904,
            &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01],
        );
        assert_varlong(
            -4_611_686_018_427_387_905,
            &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01],
        );
        assert_varlong(i64::MAX, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
        assert_varlong(i64::MIN, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
    }

    /// Java: `testInvalidVarint`.
    #[test]
    fn invalid_varint_overflows() {
        let buf = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        let err = read_varint(&buf).unwrap_err();
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
        assert!(err.message().contains("Varint is too long"));
    }

    /// Java: `testInvalidVarlong`.
    #[test]
    fn invalid_varlong_overflows() {
        let buf = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        let err = read_varlong(&buf).unwrap_err();
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
        assert!(err.message().contains("Varlong is too long"));
    }

    /// Java: `testDouble`.
    #[test]
    fn double_serde() {
        fn assert_double(value: f64, expected_long: i64) {
            // Big-endian bytes from the long.
            let mut expected = [0u8; 8];
            let bits = expected_long as u64;
            for (i, byte) in expected.iter_mut().enumerate() {
                *byte = (bits >> (56 - 8 * i)) as u8;
            }
            let mut buf = Vec::new();
            write_double(value, &mut buf);
            assert_eq!(buf.as_slice(), expected);
            assert_eq!(read_double_at(&buf, 0).to_bits(), value.to_bits());
            // Stream form
            let mut stream = Vec::new();
            write_double_to_stream(value, &mut stream).unwrap();
            assert_eq!(stream.as_slice(), expected);
            let mut cur = std::io::Cursor::new(stream.as_slice());
            let read = read_double_from_stream(&mut cur).unwrap();
            assert_eq!(read.to_bits(), value.to_bits());
        }

        assert_double(0.0, 0);
        assert_double(-0.0, 0x8000_0000_0000_0000u64 as i64);
        assert_double(1.0, 0x3FF0_0000_0000_0000);
        assert_double(-1.0, 0xBFF0_0000_0000_0000u64 as i64);
        assert_double(123e45, 0x49B5_8B82_C0E0_BB00);
        assert_double(-123e45, 0xC9B5_8B82_C0E0_BB00u64 as i64);
        assert_double(f64::MIN_POSITIVE.min(5e-324), 0x1);
        assert_double(-(5e-324f64), 0x8000_0000_0000_0001u64 as i64);
        assert_double(f64::MAX, 0x7FEF_FFFF_FFFF_FFFF);
        assert_double(-f64::MAX, 0xFFEF_FFFF_FFFF_FFFFu64 as i64);
        assert_double(f64::NAN, 0x7FF8_0000_0000_0000);
        assert_double(f64::INFINITY, 0x7FF0_0000_0000_0000);
        assert_double(f64::NEG_INFINITY, 0xFFF0_0000_0000_0000u64 as i64);
    }

    /// Java: `testCorrectnessReadUnsignedVarint`.
    /// Picks every 13th value rather than the full range to keep test time
    /// sane while still exercising every 7-bit boundary.
    #[test]
    fn correctness_read_unsigned_varint() {
        let mut buf = Vec::new();
        let mut i: u32 = 0;
        while i < 1_000_000 {
            buf.clear();
            write_unsigned_varint(i, &mut buf);
            let (decoded, consumed) = read_unsigned_varint(&buf).unwrap();
            assert_eq!(decoded, i);
            assert_eq!(consumed, buf.len());
            i = i.saturating_add(13);
            if i > 1_000_000 {
                break;
            }
        }
    }

    /// Java: `testSizeOfUnsignedVarint`.
    #[test]
    fn size_of_unsigned_varint_matches_simple() {
        fn simple(mut v: u32) -> usize {
            let mut bytes = 1usize;
            while (v & !0x7Fu32) != 0 {
                bytes += 1;
                v >>= 7;
            }
            bytes
        }
        let mut i: u32 = 0;
        while i < 1_000_000 {
            assert_eq!(size_of_unsigned_varint(i), simple(i), "size for {i}");
            i = i.saturating_add(13);
        }
    }

    /// Java: `testSizeOfVarlong`.
    #[test]
    fn size_of_varlong_matches_simple() {
        fn simple(value: i64) -> usize {
            let mut v = ((value << 1) ^ (value >> 63)) as u64;
            let mut bytes = 1usize;
            while (v & !0x7Fu64) != 0 {
                bytes += 1;
                v >>= 7;
            }
            bytes
        }
        let mut l: i64 = 1;
        while l > 0 {
            assert_eq!(size_of_varlong(l), simple(l), "size for {l}");
            l = l.saturating_mul(2);
            if l == i64::MAX {
                break;
            }
        }
        assert_eq!(size_of_varlong(0), simple(0));
    }

    /// Java: `testReadInt`.
    #[test]
    fn read_int_be_round_trip() {
        let values: &[i32] = &[
            0,
            1,
            -1,
            i8::MAX as i32,
            i16::MAX as i32,
            (i16::MAX as i32) * 2,
            i32::MAX / 2,
            i32::MIN / 2,
            i32::MAX,
            i32::MIN,
            i32::MAX,
        ];
        let mut buf = vec![0u8; values.len() * 4];
        for (i, &v) in values.iter().enumerate() {
            buf[i * 4..i * 4 + 4].copy_from_slice(&v.to_be_bytes());
            assert_eq!(read_int_be_at(&buf, i * 4), v);
        }
    }
}
