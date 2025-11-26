// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
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
        return Err(format!(
            "Varint is too long, value so far: {}",
            result
        ));
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
    fn test_varlong_zig_zag() {
        let test_values = vec![0i64, 1, -1, 100, -100, i64::MAX, i64::MIN];
        
        for value in test_values {
            let mut buf = Vec::new();
            write_varlong(value, &mut buf).unwrap();
            let (decoded, _) = read_varlong(&buf).unwrap();
            assert_eq!(decoded, value, "Failed for value: {}", value);
        }
    }
}
