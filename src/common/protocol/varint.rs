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

//! Variable-length integer encoding/decoding using Protocol Buffers encoding.
//!
//! This module implements:
//! - Unsigned varint encoding (for sizes, counts, etc.)
//! - Signed varint encoding with zig-zag encoding (for negative values)
//! - Varlong encoding for 64-bit values
//!
//! Based on Kafka's ByteUtils.java implementation.

use std::io::{self, Write};

/// Read an unsigned varint from a byte slice, returning (value, bytes_consumed).
///
/// This uses Protocol Buffers unsigned encoding.
/// Maximum 5 bytes for a 32-bit value.
///
/// # Errors
/// Returns an error if the varint doesn't terminate after 5 bytes.
pub fn read_unsigned_varint(buffer: &[u8]) -> Result<(u32, usize), String> {
    if buffer.is_empty() {
        return Err("Buffer is empty".to_string());
    }

    let mut tmp = buffer[0] as i8;
    if tmp >= 0 {
        return Ok((tmp as u32, 1));
    }

    let mut result = (tmp & 0x7F) as u32;
    let mut pos = 1;

    if pos >= buffer.len() {
        return Err("Incomplete varint".to_string());
    }
    tmp = buffer[pos] as i8;
    if tmp >= 0 {
        result |= (tmp as u32) << 7;
        return Ok((result, pos + 1));
    }
    result |= ((tmp & 0x7F) as u32) << 7;
    pos += 1;

    if pos >= buffer.len() {
        return Err("Incomplete varint".to_string());
    }
    tmp = buffer[pos] as i8;
    if tmp >= 0 {
        result |= (tmp as u32) << 14;
        return Ok((result, pos + 1));
    }
    result |= ((tmp & 0x7F) as u32) << 14;
    pos += 1;

    if pos >= buffer.len() {
        return Err("Incomplete varint".to_string());
    }
    tmp = buffer[pos] as i8;
    if tmp >= 0 {
        result |= (tmp as u32) << 21;
        return Ok((result, pos + 1));
    }
    result |= ((tmp & 0x7F) as u32) << 21;
    pos += 1;

    if pos >= buffer.len() {
        return Err("Incomplete varint".to_string());
    }
    tmp = buffer[pos] as i8;
    result |= (tmp as u32) << 28;
    if tmp < 0 {
        return Err(format!("Varint is too long, value so far: {}", result));
    }

    Ok((result, pos + 1))
}

/// Write an unsigned varint to a writer.
///
/// # Errors
/// Returns an I/O error if writing fails.
pub fn write_unsigned_varint<W: Write>(value: u32, writer: &mut W) -> io::Result<()> {
    if (value & (0xFFFFFFFF << 7)) == 0 {
        writer.write_all(&[value as u8])?;
    } else {
        writer.write_all(&[(value & 0x7F | 0x80) as u8])?;
        if (value & (0xFFFFFFFF << 14)) == 0 {
            writer.write_all(&[(value >> 7) as u8])?;
        } else {
            writer.write_all(&[((value >> 7) & 0x7F | 0x80) as u8])?;
            if (value & (0xFFFFFFFF << 21)) == 0 {
                writer.write_all(&[(value >> 14) as u8])?;
            } else {
                writer.write_all(&[((value >> 14) & 0x7F | 0x80) as u8])?;
                if (value & (0xFFFFFFFF << 28)) == 0 {
                    writer.write_all(&[(value >> 21) as u8])?;
                } else {
                    writer.write_all(&[((value >> 21) & 0x7F | 0x80) as u8])?;
                    writer.write_all(&[(value >> 28) as u8])?;
                }
            }
        }
    }
    Ok(())
}

/// Read a signed varint using zig-zag decoding.
///
/// Zig-zag encoding maps signed integers to unsigned integers:
/// - 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, etc.
///
/// # Errors
/// Returns an error if the varint is malformed.
pub fn read_varint(buffer: &[u8]) -> Result<(i32, usize), String> {
    let (value, size) = read_unsigned_varint(buffer)?;
    // Zig-zag decode: (n >>> 1) ^ -(n & 1)
    let decoded = ((value >> 1) as i32) ^ -((value & 1) as i32);
    Ok((decoded, size))
}

/// Write a signed varint using zig-zag encoding.
///
/// # Errors
/// Returns an I/O error if writing fails.
pub fn write_varint<W: Write>(value: i32, writer: &mut W) -> io::Result<()> {
    // Zig-zag encode: (n << 1) ^ (n >> 31)
    let encoded = ((value << 1) ^ (value >> 31)) as u32;
    write_unsigned_varint(encoded, writer)
}

/// Read an unsigned varlong from a byte slice, returning (value, bytes_consumed).
///
/// Maximum 10 bytes for a 64-bit value.
///
/// # Errors
/// Returns an error if the varlong doesn't terminate after 10 bytes.
pub fn read_unsigned_varlong(buffer: &[u8]) -> Result<(u64, usize), String> {
    let mut value = 0u64;
    let mut shift = 0;
    let mut pos = 0;

    loop {
        if pos >= buffer.len() {
            return Err("Incomplete varlong".to_string());
        }

        let b = buffer[pos] as u64;
        pos += 1;

        if (b & 0x80) != 0 {
            value |= (b & 0x7F) << shift;
            shift += 7;
            if shift > 63 {
                return Err(format!("Varlong is too long, value so far: {}", value));
            }
        } else {
            value |= b << shift;
            return Ok((value, pos));
        }
    }
}

/// Read a signed varlong using zig-zag decoding.
///
/// # Errors
/// Returns an error if the varlong is malformed.
pub fn read_varlong(buffer: &[u8]) -> Result<(i64, usize), String> {
    let (raw, size) = read_unsigned_varlong(buffer)?;
    // Zig-zag decode: (n >>> 1) ^ -(n & 1)
    let decoded = ((raw >> 1) as i64) ^ -((raw & 1) as i64);
    Ok((decoded, size))
}

/// Write an unsigned varlong to a writer.
///
/// # Errors
/// Returns an I/O error if writing fails.
pub fn write_unsigned_varlong<W: Write>(mut value: u64, writer: &mut W) -> io::Result<()> {
    while (value & 0xFFFFFFFFFFFFFF80) != 0 {
        let b = ((value & 0x7F) | 0x80) as u8;
        writer.write_all(&[b])?;
        value >>= 7;
    }
    writer.write_all(&[value as u8])?;
    Ok(())
}

/// Write a signed varlong using zig-zag encoding.
///
/// # Errors
/// Returns an I/O error if writing fails.
pub fn write_varlong<W: Write>(value: i64, writer: &mut W) -> io::Result<()> {
    // Zig-zag encode: (n << 1) ^ (n >> 63)
    let encoded = ((value << 1) ^ (value >> 63)) as u64;
    write_unsigned_varlong(encoded, writer)
}

/// Returns the number of bytes needed to encode a value as an unsigned varint.
///
/// Corresponds to Java's `ByteUtils.sizeOfUnsignedVarint()`.
pub const fn size_of_unsigned_varint(value: u32) -> i32 {
    let leading_zeros = value.leading_zeros() as i32;
    // Equivalent to: ceil((32 - leading_zeros) / 7.0), min 1
    // Uses the same bit trick as the Java implementation
    (((38 - leading_zeros) * 0b10010010010010011i32) >> 19) + (leading_zeros >> 5)
}

/// Returns the number of bytes needed to encode a signed varint (zig-zag encoded).
///
/// Corresponds to Java's `ByteUtils.sizeOfVarint()`.
pub const fn size_of_varint(value: i32) -> i32 {
    let encoded = ((value << 1) ^ (value >> 31)) as u32;
    size_of_unsigned_varint(encoded)
}

/// Returns the number of bytes needed to encode an unsigned varlong.
///
/// Corresponds to Java's `ByteUtils.sizeOfUnsignedVarlong()`.
pub const fn size_of_unsigned_varlong(v: u64) -> i32 {
    let leading_zeros = v.leading_zeros() as i32;
    let leading_zeros_below_70_divided_by_7 = ((70 - leading_zeros) * 0b10010010010010011i32) >> 19;
    leading_zeros_below_70_divided_by_7 + (leading_zeros >> 6)
}

/// Returns the number of bytes needed to encode a signed varlong (zig-zag encoded).
///
/// Corresponds to Java's `ByteUtils.sizeOfVarlong()`.
pub const fn size_of_varlong(value: i64) -> i32 {
    let encoded = ((value << 1) ^ (value >> 63)) as u64;
    size_of_unsigned_varlong(encoded)
}

/// Read an unsigned varint from a `Read` stream.
///
/// Corresponds to Java's `ByteUtils.readUnsignedVarint(InputStream)`.
///
/// # Errors
/// Returns an I/O error if reading fails or the varint doesn't terminate after 5 bytes.
pub fn read_unsigned_varint_reader<R: io::Read>(reader: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 1];

    reader.read_exact(&mut buf)?;
    let mut tmp = buf[0] as i8;
    if tmp >= 0 {
        return Ok(tmp as u32);
    }
    let mut result = (tmp & 0x7F) as u32;

    reader.read_exact(&mut buf)?;
    tmp = buf[0] as i8;
    if tmp >= 0 {
        result |= (tmp as u32) << 7;
        return Ok(result);
    }
    result |= ((tmp & 0x7F) as u32) << 7;

    reader.read_exact(&mut buf)?;
    tmp = buf[0] as i8;
    if tmp >= 0 {
        result |= (tmp as u32) << 14;
        return Ok(result);
    }
    result |= ((tmp & 0x7F) as u32) << 14;

    reader.read_exact(&mut buf)?;
    tmp = buf[0] as i8;
    if tmp >= 0 {
        result |= (tmp as u32) << 21;
        return Ok(result);
    }
    result |= ((tmp & 0x7F) as u32) << 21;

    reader.read_exact(&mut buf)?;
    tmp = buf[0] as i8;
    result |= (tmp as u32) << 28;
    if tmp < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Varint is too long, value so far: {}", result),
        ));
    }

    Ok(result)
}

/// Read a signed varint from a `Read` stream using zig-zag decoding.
///
/// Corresponds to Java's `ByteUtils.readVarint(InputStream)`.
///
/// # Errors
/// Returns an I/O error if reading fails or the varint is malformed.
pub fn read_varint_reader<R: io::Read>(reader: &mut R) -> io::Result<i32> {
    let value = read_unsigned_varint_reader(reader)?;
    Ok(((value >> 1) as i32) ^ -((value & 1) as i32))
}

/// Read a signed varlong from a `Read` stream using zig-zag decoding.
///
/// Corresponds to Java's `ByteUtils.readVarlong(InputStream)`.
///
/// # Errors
/// Returns an I/O error if reading fails or the varlong is malformed.
pub fn read_varlong_reader<R: io::Read>(reader: &mut R) -> io::Result<i64> {
    let mut value = 0u64;
    let mut i = 0;
    let mut buf = [0u8; 1];
    loop {
        reader.read_exact(&mut buf)?;
        let b = buf[0] as u64;
        if (b & 0x80) != 0 {
            value |= (b & 0x7F) << i;
            i += 7;
            if i > 63 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Varlong is too long, value so far: {}", value),
                ));
            }
        } else {
            value |= b << i;
            let decoded = ((value >> 1) as i64) ^ -((value & 1) as i64);
            return Ok(decoded);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unsigned_varint_single_byte() {
        let buf = [0x05];
        let (value, size) = read_unsigned_varint(&buf).unwrap();
        assert_eq!(value, 5);
        assert_eq!(size, 1);

        let mut out = Vec::new();
        write_unsigned_varint(5, &mut out).unwrap();
        assert_eq!(out, vec![0x05]);
    }

    #[test]
    fn test_unsigned_varint_two_bytes() {
        let buf = [0x96, 0x01]; // 150
        let (value, size) = read_unsigned_varint(&buf).unwrap();
        assert_eq!(value, 150);
        assert_eq!(size, 2);

        let mut out = Vec::new();
        write_unsigned_varint(150, &mut out).unwrap();
        assert_eq!(out, vec![0x96, 0x01]);
    }

    #[test]
    fn test_unsigned_varint_max_value() {
        // u32::MAX requires 5 bytes
        let buf = [0xFF, 0xFF, 0xFF, 0xFF, 0x0F];
        let (value, size) = read_unsigned_varint(&buf).unwrap();
        assert_eq!(value, u32::MAX);
        assert_eq!(size, 5);

        let mut out = Vec::new();
        write_unsigned_varint(u32::MAX, &mut out).unwrap();
        assert_eq!(out, vec![0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    }

    #[test]
    fn test_varint_positive() {
        let buf = [0x02]; // 1 encoded
        let (value, size) = read_varint(&buf).unwrap();
        assert_eq!(value, 1);
        assert_eq!(size, 1);

        let mut out = Vec::new();
        write_varint(1, &mut out).unwrap();
        assert_eq!(out, vec![0x02]);
    }

    #[test]
    fn test_varint_negative() {
        let buf = [0x01]; // -1 encoded
        let (value, size) = read_varint(&buf).unwrap();
        assert_eq!(value, -1);
        assert_eq!(size, 1);

        let mut out = Vec::new();
        write_varint(-1, &mut out).unwrap();
        assert_eq!(out, vec![0x01]);
    }

    #[test]
    fn test_varint_zero() {
        let buf = [0x00];
        let (value, size) = read_varint(&buf).unwrap();
        assert_eq!(value, 0);
        assert_eq!(size, 1);

        let mut out = Vec::new();
        write_varint(0, &mut out).unwrap();
        assert_eq!(out, vec![0x00]);
    }

    #[test]
    fn test_varlong_small_value() {
        let buf = [0x05];
        let (value, size) = read_unsigned_varlong(&buf).unwrap();
        assert_eq!(value, 5);
        assert_eq!(size, 1);

        let mut out = Vec::new();
        write_unsigned_varlong(5, &mut out).unwrap();
        assert_eq!(out, vec![0x05]);
    }

    #[test]
    fn test_varlong_large_value() {
        // Test with a large value that requires multiple bytes
        let value = 0x0123456789ABCDEFu64;
        let mut buf = Vec::new();
        write_unsigned_varlong(value, &mut buf).unwrap();

        let (decoded, _) = read_unsigned_varlong(&buf).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_size_of_unsigned_varint() {
        assert_eq!(size_of_unsigned_varint(0), 1);
        assert_eq!(size_of_unsigned_varint(1), 1);
        assert_eq!(size_of_unsigned_varint(127), 1);
        assert_eq!(size_of_unsigned_varint(128), 2);
        assert_eq!(size_of_unsigned_varint(16383), 2);
        assert_eq!(size_of_unsigned_varint(16384), 3);
        assert_eq!(size_of_unsigned_varint(2097151), 3);
        assert_eq!(size_of_unsigned_varint(2097152), 4);
        assert_eq!(size_of_unsigned_varint(268435455), 4);
        assert_eq!(size_of_unsigned_varint(268435456), 5);
        assert_eq!(size_of_unsigned_varint(u32::MAX), 5);
    }

    #[test]
    fn test_varlong_zig_zag() {
        let test_values = vec![0i64, 1, -1, 100, -100, i64::MAX, i64::MIN];

        for value in test_values {
            let mut buf = Vec::new();
            write_varlong(value, &mut buf).unwrap();
            let (decoded, _) = read_varlong(&buf).unwrap();
            assert_eq!(decoded, value, "Failed for value: {}", value);
        }
    }

    // ========================================================================
    // Byte-level encoding verification tests translated from
    // org.apache.kafka.common.utils.ByteUtilsTest
    // ========================================================================

    /// Helper: assert that encoding an unsigned varint produces the expected bytes
    /// and that decoding those bytes returns the original value.
    fn assert_unsigned_varint_serde(value: i32, expected: &[u8]) {
        let mut buf = Vec::new();
        // Java treats the value as int (signed 32-bit) but writes as unsigned varint,
        // so we reinterpret as u32 to match.
        write_unsigned_varint(value as u32, &mut buf).unwrap();
        assert_eq!(buf, expected, "Unsigned varint encoding mismatch for value {}", value);
        let (decoded, size) = read_unsigned_varint(&buf).unwrap();
        assert_eq!(decoded, value as u32, "Unsigned varint decode mismatch for value {}", value);
        assert_eq!(size, expected.len());
    }

    /// Helper: assert that zig-zag varint encoding produces the expected bytes
    /// and that decoding returns the original value.
    fn assert_varint_serde(value: i32, expected: &[u8]) {
        let mut buf = Vec::new();
        write_varint(value, &mut buf).unwrap();
        assert_eq!(buf, expected, "Varint encoding mismatch for value {}", value);
        let (decoded, size) = read_varint(&buf).unwrap();
        assert_eq!(decoded, value, "Varint decode mismatch for value {}", value);
        assert_eq!(size, expected.len());
    }

    /// Helper: assert that zig-zag varlong encoding produces the expected bytes
    /// and that decoding returns the original value.
    fn assert_varlong_serde(value: i64, expected: &[u8]) {
        let mut buf = Vec::new();
        write_varlong(value, &mut buf).unwrap();
        assert_eq!(buf, expected, "Varlong encoding mismatch for value {}", value);
        let (decoded, size) = read_varlong(&buf).unwrap();
        assert_eq!(decoded, value, "Varlong decode mismatch for value {}", value);
        assert_eq!(size, expected.len());
    }

    /// Translated from ByteUtilsTest.testUnsignedVarintSerde.
    /// Verifies exact byte encodings for 14 unsigned varint values.
    #[test]
    fn test_unsigned_varint_serde_byte_level() {
        assert_unsigned_varint_serde(0, &[0x00]);
        assert_unsigned_varint_serde(-1, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]); // -1 as u32 = 0xFFFFFFFF
        assert_unsigned_varint_serde(1, &[0x01]);
        assert_unsigned_varint_serde(63, &[0x3F]);
        assert_unsigned_varint_serde(-64, &[0xC0, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_unsigned_varint_serde(64, &[0x40]);
        assert_unsigned_varint_serde(8191, &[0xFF, 0x3F]);
        assert_unsigned_varint_serde(-8192, &[0x80, 0xC0, 0xFF, 0xFF, 0x0F]);
        assert_unsigned_varint_serde(8192, &[0x80, 0x40]);
        assert_unsigned_varint_serde(-8193, &[0xFF, 0xBF, 0xFF, 0xFF, 0x0F]);
        assert_unsigned_varint_serde(1048575, &[0xFF, 0xFF, 0x3F]);
        assert_unsigned_varint_serde(1048576, &[0x80, 0x80, 0x40]);
        assert_unsigned_varint_serde(i32::MAX, &[0xFF, 0xFF, 0xFF, 0xFF, 0x07]);
        assert_unsigned_varint_serde(i32::MIN, &[0x80, 0x80, 0x80, 0x80, 0x08]);
    }

    /// Translated from ByteUtilsTest.testVarintSerde.
    /// Verifies exact byte encodings for 20 signed varint values with zig-zag encoding.
    #[test]
    fn test_varint_serde_byte_level() {
        assert_varint_serde(0, &[0x00]);
        assert_varint_serde(-1, &[0x01]);
        assert_varint_serde(1, &[0x02]);
        assert_varint_serde(63, &[0x7E]);
        assert_varint_serde(-64, &[0x7F]);
        assert_varint_serde(64, &[0x80, 0x01]);
        assert_varint_serde(-65, &[0x81, 0x01]);
        assert_varint_serde(8191, &[0xFE, 0x7F]);
        assert_varint_serde(-8192, &[0xFF, 0x7F]);
        assert_varint_serde(8192, &[0x80, 0x80, 0x01]);
        assert_varint_serde(-8193, &[0x81, 0x80, 0x01]);
        assert_varint_serde(1048575, &[0xFE, 0xFF, 0x7F]);
        assert_varint_serde(-1048576, &[0xFF, 0xFF, 0x7F]);
        assert_varint_serde(1048576, &[0x80, 0x80, 0x80, 0x01]);
        assert_varint_serde(-1048577, &[0x81, 0x80, 0x80, 0x01]);
        assert_varint_serde(134217727, &[0xFE, 0xFF, 0xFF, 0x7F]);
        assert_varint_serde(-134217728, &[0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varint_serde(134217728, &[0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varint_serde(-134217729, &[0x81, 0x80, 0x80, 0x80, 0x01]);
        assert_varint_serde(i32::MAX, &[0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_varint_serde(i32::MIN, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    }

    /// Translated from ByteUtilsTest.testVarlongSerde.
    /// Verifies exact byte encodings for 42 signed varlong values with zig-zag encoding.
    #[test]
    fn test_varlong_serde_byte_level() {
        assert_varlong_serde(0, &[0x00]);
        assert_varlong_serde(-1, &[0x01]);
        assert_varlong_serde(1, &[0x02]);
        assert_varlong_serde(63, &[0x7E]);
        assert_varlong_serde(-64, &[0x7F]);
        assert_varlong_serde(64, &[0x80, 0x01]);
        assert_varlong_serde(-65, &[0x81, 0x01]);
        assert_varlong_serde(8191, &[0xFE, 0x7F]);
        assert_varlong_serde(-8192, &[0xFF, 0x7F]);
        assert_varlong_serde(8192, &[0x80, 0x80, 0x01]);
        assert_varlong_serde(-8193, &[0x81, 0x80, 0x01]);
        assert_varlong_serde(1048575, &[0xFE, 0xFF, 0x7F]);
        assert_varlong_serde(-1048576, &[0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(1048576, &[0x80, 0x80, 0x80, 0x01]);
        assert_varlong_serde(-1048577, &[0x81, 0x80, 0x80, 0x01]);
        assert_varlong_serde(134217727, &[0xFE, 0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(-134217728, &[0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(134217728, &[0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong_serde(-134217729, &[0x81, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong_serde(i32::MAX as i64, &[0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_varlong_serde(i32::MIN as i64, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_varlong_serde(17179869183, &[0xFE, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(-17179869184, &[0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(17179869184, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong_serde(-17179869185, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong_serde(2199023255551, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(-2199023255552, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(2199023255552, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong_serde(-2199023255553, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong_serde(281474976710655, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(-281474976710656, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(281474976710656, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong_serde(-281474976710657, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong_serde(36028797018963967, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(-36028797018963968, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(36028797018963968, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong_serde(-36028797018963969, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varlong_serde(4611686018427387903, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(-4611686018427387904, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varlong_serde(
            4611686018427387904,
            &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01],
        );
        assert_varlong_serde(
            -4611686018427387905,
            &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01],
        );
        assert_varlong_serde(i64::MAX, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
        assert_varlong_serde(i64::MIN, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
    }

    #[test]
    fn test_size_of_varint() {
        assert_eq!(size_of_varint(0), 1);
        assert_eq!(size_of_varint(1), 1);
        assert_eq!(size_of_varint(-1), 1);
        assert_eq!(size_of_varint(63), 1);
        assert_eq!(size_of_varint(-64), 1);
        assert_eq!(size_of_varint(64), 2);
        assert_eq!(size_of_varint(-65), 2);
        assert_eq!(size_of_varint(i32::MAX), 5);
        assert_eq!(size_of_varint(i32::MIN), 5);
    }

    #[test]
    fn test_size_of_varlong() {
        assert_eq!(size_of_varlong(0), 1);
        assert_eq!(size_of_varlong(1), 1);
        assert_eq!(size_of_varlong(-1), 1);
        assert_eq!(size_of_varlong(63), 1);
        assert_eq!(size_of_varlong(-64), 1);
        assert_eq!(size_of_varlong(64), 2);
        assert_eq!(size_of_varlong(-65), 2);
        assert_eq!(size_of_varlong(i64::MAX), 10);
        assert_eq!(size_of_varlong(i64::MIN), 10);
    }

    #[test]
    fn test_read_varint_reader() {
        use std::io::Cursor;
        // encode 42 and read it back via reader
        let mut buf = Vec::new();
        write_varint(42, &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded = read_varint_reader(&mut cursor).unwrap();
        assert_eq!(decoded, 42);

        // negative
        let mut buf = Vec::new();
        write_varint(-123, &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded = read_varint_reader(&mut cursor).unwrap();
        assert_eq!(decoded, -123);
    }

    #[test]
    fn test_read_varlong_reader() {
        use std::io::Cursor;
        let test_values = vec![0i64, 1, -1, 100, -100, i64::MAX, i64::MIN];
        for value in test_values {
            let mut buf = Vec::new();
            write_varlong(value, &mut buf).unwrap();
            let mut cursor = Cursor::new(buf);
            let decoded = read_varlong_reader(&mut cursor).unwrap();
            assert_eq!(decoded, value, "Failed for value: {}", value);
        }
    }

    /// Translated from ByteUtilsTest.testInvalidVarint.
    /// A 6-byte varint (overflow) must be rejected.
    #[test]
    fn test_invalid_varint() {
        let buf = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        let result = read_unsigned_varint(&buf);
        assert!(result.is_err(), "Expected error for overlong varint");
    }

    /// Translated from ByteUtilsTest.testInvalidVarlong.
    /// An 11-byte varlong (overflow) must be rejected.
    #[test]
    fn test_invalid_varlong() {
        let buf = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        let result = read_unsigned_varlong(&buf);
        assert!(result.is_err(), "Expected error for overlong varlong");
    }

    /// Translated from ByteUtilsTest.testDouble.
    /// Verifies double-to-bits encoding for 13 values including NaN, infinities, and edge cases.
    #[test]
    fn test_double_encoding() {
        fn assert_double_serde(value: f64, expected_bits: u64) {
            // Verify that Rust's f64::to_bits matches expected Java Double.doubleToLongBits
            let actual_bits = value.to_bits();
            assert_eq!(
                actual_bits, expected_bits,
                "Double to bits mismatch for value {}: expected 0x{:016X}, got 0x{:016X}",
                value, expected_bits, actual_bits
            );

            // Verify big-endian byte encoding matches
            let expected_bytes = expected_bits.to_be_bytes();
            let actual_bytes = value.to_be_bytes();
            assert_eq!(
                actual_bytes, expected_bytes,
                "Double byte encoding mismatch for value {}",
                value
            );

            // Verify round-trip through f64::from_bits
            let reconstructed = f64::from_bits(expected_bits);
            if value.is_nan() {
                assert!(reconstructed.is_nan());
            } else {
                assert_eq!(reconstructed, value);
            }
        }

        assert_double_serde(0.0, 0x0);
        assert_double_serde(-0.0, 0x8000000000000000);
        assert_double_serde(1.0, 0x3FF0000000000000);
        assert_double_serde(-1.0, 0xBFF0000000000000);
        assert_double_serde(123e45, 0x49B58B82C0E0BB00);
        assert_double_serde(-123e45, 0xC9B58B82C0E0BB00);
        // Java's Double.MIN_VALUE is the smallest positive subnormal, not Rust's f64::MIN_POSITIVE
        let double_min_value: f64 = f64::from_bits(0x1); // 4.9e-324
        assert_double_serde(double_min_value, 0x1);
        assert_double_serde(-double_min_value, 0x8000000000000001);
        assert_double_serde(f64::MAX, 0x7FEFFFFFFFFFFFFF);
        assert_double_serde(-f64::MAX, 0xFFEFFFFFFFFFFFFF);
        assert_double_serde(f64::NAN, 0x7FF8000000000000);
        assert_double_serde(f64::INFINITY, 0x7FF0000000000000);
        assert_double_serde(f64::NEG_INFINITY, 0xFFF0000000000000);
    }
}
