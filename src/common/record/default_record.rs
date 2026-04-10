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

//! Inner record format for magic v2 and above.
//!
//! Translated from `org.apache.kafka.common.record.DefaultRecord`.
//!
//! Record schema:
//! ```text
//! Record =>
//!   Length => Varint
//!   Attributes => Int8
//!   TimestampDelta => Varlong
//!   OffsetDelta => Varint
//!   KeyLength => Varint
//!   Key => Bytes
//!   ValueLength => Varint
//!   Value => Bytes
//!   HeadersCount => Varint
//!   Headers => [HeaderKey HeaderValue]
//!     HeaderKeyLength => Varint
//!     HeaderKey => String
//!     HeaderValueLength => Varint
//!     HeaderValue => Bytes
//! ```

use std::io::Write;

use crate::common::protocol::varint;
use crate::common::record::{MAGIC_VALUE_V2, NO_SEQUENCE, RecordHeader, default_record_batch, invalid_record_error};
use crate::errors::Result;

/// Maximum per-record overhead: 5 bytes length + 10 bytes timestamp + 5 bytes offset + 1 byte attributes.
pub const MAX_RECORD_OVERHEAD: usize = 21;

/// Size in bytes of varint(-1), used for null fields.
const NULL_VARINT_SIZE_BYTES: i32 = 1; // varint(-1) = zig-zag(0x01) = 1 byte

/// A single record in magic v2 format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultRecord {
    /// Total size in bytes (including the varint length prefix).
    size_in_bytes: i32,
    /// Record attributes (currently unused, always 0).
    attributes: u8,
    /// The absolute offset of this record.
    offset: i64,
    /// The absolute timestamp of this record.
    timestamp: i64,
    /// The sequence number of this record.
    sequence: i32,
    /// The record key (None if null).
    key: Option<Vec<u8>>,
    /// The record value (None if null).
    value: Option<Vec<u8>>,
    /// The record headers.
    headers: Vec<RecordHeader>,
}

impl DefaultRecord {
    /// Returns the absolute offset of this record in the log.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the sequence number assigned by the producer.
    pub fn sequence(&self) -> i32 {
        self.sequence
    }

    /// Returns the total size in bytes of this record (including the varint length prefix).
    pub fn size_in_bytes(&self) -> i32 {
        self.size_in_bytes
    }

    /// Returns the timestamp of this record.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Returns the record attributes byte.
    pub fn attributes(&self) -> u8 {
        self.attributes
    }

    /// Returns the size of the key in bytes, or -1 if the key is null.
    pub fn key_size(&self) -> i32 {
        match &self.key {
            Some(k) => k.len() as i32,
            None => -1,
        }
    }

    /// Returns whether this record has a key.
    pub fn has_key(&self) -> bool {
        self.key.is_some()
    }

    /// Returns a reference to the key bytes, or None if the key is null.
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Returns the size of the value in bytes, or -1 if the value is null.
    pub fn value_size(&self) -> i32 {
        match &self.value {
            Some(v) => v.len() as i32,
            None => -1,
        }
    }

    /// Returns whether this record has a value.
    pub fn has_value(&self) -> bool {
        self.value.is_some()
    }

    /// Returns a reference to the value bytes, or None if the value is null.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Returns a reference to the record headers.
    pub fn headers(&self) -> &[RecordHeader] {
        &self.headers
    }

    /// Returns true if the given magic is >= v2 (this is always a v2+ record).
    pub fn has_magic(&self, magic: i8) -> bool {
        magic >= MAGIC_VALUE_V2
    }

    /// For v2 records, this is always false.
    pub fn is_compressed(&self) -> bool {
        false
    }

    /// For v2 records, this is always false.
    pub fn has_timestamp_type(&self, _timestamp_type: super::TimestampType) -> bool {
        false
    }

    /// Validate the record (no-op for v2).
    pub fn ensure_valid(&self) {}

    // ========================================================================
    // Static methods: writing
    // ========================================================================

    /// Write a record to the given writer and return the total size in bytes
    /// (including the varint length prefix).
    ///
    /// # Errors
    /// Returns an error if the headers are invalid (null header key) or I/O fails.
    pub fn write_to<W: Write>(
        writer: &mut W,
        offset_delta: i32,
        timestamp_delta: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> Result<i32> {
        let body_size = Self::size_of_body_in_bytes_from_slices(offset_delta, timestamp_delta, key, value, headers);

        varint::write_varint(body_size, writer)?;

        // attributes byte (currently unused)
        writer.write_all(&[0u8])?;

        varint::write_varlong(timestamp_delta, writer)?;
        varint::write_varint(offset_delta, writer)?;

        // write key
        match key {
            None => {
                varint::write_varint(-1, writer)?;
            },
            Some(k) => {
                varint::write_varint(k.len() as i32, writer)?;
                writer.write_all(k)?;
            },
        }

        // write value
        match value {
            None => {
                varint::write_varint(-1, writer)?;
            },
            Some(v) => {
                varint::write_varint(v.len() as i32, writer)?;
                writer.write_all(v)?;
            },
        }

        // write headers
        varint::write_varint(headers.len() as i32, writer)?;

        for header in headers {
            let key_bytes = header.key.as_bytes();
            varint::write_varint(key_bytes.len() as i32, writer)?;
            writer.write_all(key_bytes)?;

            match &header.value {
                None => {
                    varint::write_varint(-1, writer)?;
                },
                Some(v) => {
                    varint::write_varint(v.len() as i32, writer)?;
                    writer.write_all(v)?;
                },
            }
        }

        Ok(varint::size_of_varint(body_size) + body_size)
    }

    // ========================================================================
    // Static methods: reading
    // ========================================================================

    /// Read a record from a byte buffer.
    ///
    /// The buffer position should be at the start of the record (the varint length prefix).
    ///
    /// # Errors
    /// Returns `KafkaError` (CorruptRecord) if the record is malformed.
    pub fn read_from(
        buffer: &[u8],
        pos: &mut usize,
        base_offset: i64,
        base_timestamp: i64,
        base_sequence: i32,
        log_append_time: Option<i64>,
    ) -> Result<DefaultRecord> {
        let (body_size, varint_len) = read_varint_at(buffer, *pos)?;
        *pos += varint_len;

        if body_size < 0 {
            return Err(invalid_record_error(format!(
                "Invalid negative record body size: {}",
                body_size
            )));
        }
        let body_size = body_size as usize;

        if buffer.len() - *pos < body_size {
            return Err(invalid_record_error(format!(
                "Invalid record size: expected {} bytes in record payload, but instead the buffer has only {} remaining bytes.",
                body_size,
                buffer.len() - *pos
            )));
        }

        let record_start = *pos;

        // attributes
        check_remaining(buffer, *pos, 1)?;
        let attributes = buffer[*pos];
        *pos += 1;

        // timestamp delta
        let (timestamp_delta, varlong_len) = read_varlong_at(buffer, *pos)?;
        *pos += varlong_len;
        let mut timestamp = base_timestamp + timestamp_delta;
        if let Some(lat) = log_append_time {
            timestamp = lat;
        }

        // offset delta
        let (offset_delta, vi_len) = read_varint_at(buffer, *pos)?;
        *pos += vi_len;
        let offset = base_offset + offset_delta as i64;

        let sequence = if base_sequence >= 0 {
            default_record_batch::increment_sequence(base_sequence, offset_delta)
        } else {
            NO_SEQUENCE
        };

        // key
        let (key_size, vi_len) = read_varint_at(buffer, *pos)?;
        *pos += vi_len;
        let key = read_bytes(buffer, pos, key_size)?;

        // value
        let (value_size, vi_len) = read_varint_at(buffer, *pos)?;
        *pos += vi_len;
        let value = read_bytes(buffer, pos, value_size)?;

        // headers
        let (num_headers, vi_len) = read_varint_at(buffer, *pos)?;
        *pos += vi_len;
        if num_headers < 0 {
            return Err(invalid_record_error(format!(
                "Found invalid number of record headers {}",
                num_headers
            )));
        }
        let num_headers = num_headers as usize;
        if num_headers > buffer.len() - *pos {
            return Err(invalid_record_error(format!(
                "Found invalid number of record headers. {} is larger than the remaining size of the buffer",
                num_headers
            )));
        }

        let headers = if num_headers == 0 {
            Vec::new()
        } else {
            read_headers(buffer, pos, num_headers)?
        };

        // validate we consumed exactly body_size bytes
        if *pos - record_start != body_size {
            return Err(invalid_record_error(format!(
                "Invalid record size: expected to read {} bytes in record payload, but instead read {}",
                body_size,
                *pos - record_start
            )));
        }

        let total_size = varint_len as i32 + body_size as i32;
        Ok(DefaultRecord {
            size_in_bytes: total_size,
            attributes,
            offset,
            timestamp,
            sequence,
            key,
            value,
            headers,
        })
    }

    // ========================================================================
    // Static methods: size calculation
    // ========================================================================

    /// Calculate the total size in bytes of a record with the given fields
    /// (including the varint length prefix).
    pub fn size_in_bytes_with_slices(
        offset_delta: i32,
        timestamp_delta: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> i32 {
        let body_size = Self::size_of_body_in_bytes_from_slices(offset_delta, timestamp_delta, key, value, headers);
        body_size + varint::size_of_varint(body_size)
    }

    /// Calculate the total size in bytes using key/value sizes
    /// (including the varint length prefix).
    pub fn size_in_bytes_with_sizes(
        offset_delta: i32,
        timestamp_delta: i64,
        key_size: i32,
        value_size: i32,
        headers: &[RecordHeader],
    ) -> i32 {
        let body_size = Self::size_of_body_in_bytes(offset_delta, timestamp_delta, key_size, value_size, headers);
        body_size + varint::size_of_varint(body_size)
    }

    /// Calculate the body size (excluding the varint length prefix) using slices.
    fn size_of_body_in_bytes_from_slices(
        offset_delta: i32,
        timestamp_delta: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> i32 {
        let key_size = match key {
            Some(k) => k.len() as i32,
            None => -1,
        };
        let value_size = match value {
            Some(v) => v.len() as i32,
            None => -1,
        };
        Self::size_of_body_in_bytes(offset_delta, timestamp_delta, key_size, value_size, headers)
    }

    /// Calculate the body size (excluding the varint length prefix) using sizes.
    pub fn size_of_body_in_bytes(
        offset_delta: i32,
        timestamp_delta: i64,
        key_size: i32,
        value_size: i32,
        headers: &[RecordHeader],
    ) -> i32 {
        let mut size = 1i32; // always one byte for attributes
        size += varint::size_of_varint(offset_delta);
        size += varint::size_of_varlong(timestamp_delta);
        size += size_of_key_value_headers(key_size, value_size, headers);
        size
    }

    /// Upper bound on a record size given key, value, and headers.
    pub fn record_size_upper_bound(key: Option<&[u8]>, value: Option<&[u8]>, headers: &[RecordHeader]) -> usize {
        let key_size = match key {
            Some(k) => k.len() as i32,
            None => -1,
        };
        let value_size = match value {
            Some(v) => v.len() as i32,
            None => -1,
        };
        MAX_RECORD_OVERHEAD + size_of_key_value_headers(key_size, value_size, headers) as usize
    }
}

/// Calculate the size contribution of key, value, and headers.
fn size_of_key_value_headers(key_size: i32, value_size: i32, headers: &[RecordHeader]) -> i32 {
    let mut size = 0i32;

    if key_size < 0 {
        size += NULL_VARINT_SIZE_BYTES;
    } else {
        size += varint::size_of_varint(key_size) + key_size;
    }

    if value_size < 0 {
        size += NULL_VARINT_SIZE_BYTES;
    } else {
        size += varint::size_of_varint(value_size) + value_size;
    }

    size += varint::size_of_varint(headers.len() as i32);
    for header in headers {
        let header_key_size = header.key.len() as i32;
        size += varint::size_of_varint(header_key_size) + header_key_size;

        match &header.value {
            None => {
                size += NULL_VARINT_SIZE_BYTES;
            },
            Some(v) => {
                size += varint::size_of_varint(v.len() as i32) + v.len() as i32;
            },
        }
    }
    size
}

// ============================================================================
// Internal reading helpers
// ============================================================================

/// Read a zig-zag varint from buffer at the given position.
fn read_varint_at(buffer: &[u8], pos: usize) -> Result<(i32, usize)> {
    if pos >= buffer.len() {
        return Err(invalid_record_error("Found invalid record structure"));
    }
    varint::read_varint(&buffer[pos..]).map_err(|e| invalid_record_error(e.to_string()))
}

/// Read a zig-zag varlong from buffer at the given position.
fn read_varlong_at(buffer: &[u8], pos: usize) -> Result<(i64, usize)> {
    if pos >= buffer.len() {
        return Err(invalid_record_error("Found invalid record structure"));
    }
    varint::read_varlong(&buffer[pos..]).map_err(|e| invalid_record_error(e.to_string()))
}

/// Check that we have at least `needed` bytes from `pos`.
fn check_remaining(buffer: &[u8], pos: usize, needed: usize) -> Result<()> {
    if buffer.len() - pos < needed {
        Err(invalid_record_error("Found invalid record structure"))
    } else {
        Ok(())
    }
}

/// Read `size` bytes from the buffer. If size < 0, returns None (null).
fn read_bytes(buffer: &[u8], pos: &mut usize, size: i32) -> Result<Option<Vec<u8>>> {
    if size < 0 {
        return Ok(None);
    }
    let size = size as usize;
    if buffer.len() - *pos < size {
        return Err(invalid_record_error("Found invalid record structure"));
    }
    let data = buffer[*pos..*pos + size].to_vec();
    *pos += size;
    Ok(Some(data))
}

/// Read `num_headers` record headers from the buffer.
fn read_headers(buffer: &[u8], pos: &mut usize, num_headers: usize) -> Result<Vec<RecordHeader>> {
    let mut headers = Vec::with_capacity(num_headers);
    for _ in 0..num_headers {
        let (header_key_size, vi_len) = read_varint_at(buffer, *pos)?;
        *pos += vi_len;
        if header_key_size < 0 {
            return Err(invalid_record_error(format!(
                "Invalid negative header key size {}",
                header_key_size
            )));
        }
        let header_key_size = header_key_size as usize;
        if buffer.len() - *pos < header_key_size {
            return Err(invalid_record_error("Found invalid record structure"));
        }
        let key_str = std::str::from_utf8(&buffer[*pos..*pos + header_key_size])
            .map_err(|e| invalid_record_error(format!("Invalid UTF-8 in header key: {}", e)))?
            .to_owned();
        *pos += header_key_size;

        let (header_value_size, vi_len) = read_varint_at(buffer, *pos)?;
        *pos += vi_len;
        let header_value = read_bytes(buffer, pos, header_value_size)?;

        headers.push(RecordHeader { key: key_str, value: header_value });
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now_ms() -> i64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
    }

    /// Translated from DefaultRecordTest.testBasicSerde
    #[test]
    fn test_basic_serde() {
        let headers = vec![
            RecordHeader::new("foo", Some(b"value".to_vec())),
            RecordHeader::new("bar", None),
            RecordHeader::new("\"A\\u00ea\\u00f1\\u00fcC\"", Some(b"value".to_vec())),
        ];

        let records = vec![
            SimpleRecord::with_key_value(Some(b"hi".to_vec()), Some(b"there".to_vec())),
            SimpleRecord::with_key_value(None, Some(b"there".to_vec())),
            SimpleRecord::with_key_value(Some(b"hi".to_vec()), None),
            SimpleRecord::with_key_value(None, None),
            SimpleRecord::new(15, Some(b"hi".to_vec()), Some(b"there".to_vec()), headers),
        ];

        for record in &records {
            let base_sequence: i32 = 723;
            let base_offset: i64 = 37;
            let offset_delta: i32 = 10;
            let base_timestamp: i64 = now_ms();
            let timestamp_delta: i64 = 323;

            let mut buf = Vec::with_capacity(1024);
            DefaultRecord::write_to(
                &mut buf,
                offset_delta,
                timestamp_delta,
                record.key.as_deref(),
                record.value.as_deref(),
                &record.headers,
            )
            .unwrap();

            let mut pos = 0;
            let log_record =
                DefaultRecord::read_from(&buf, &mut pos, base_offset, base_timestamp, base_sequence, None).unwrap();

            assert_eq!(base_offset + offset_delta as i64, log_record.offset());
            assert_eq!(base_sequence + offset_delta, log_record.sequence());
            assert_eq!(base_timestamp + timestamp_delta, log_record.timestamp());
            assert_eq!(record.key.as_deref(), log_record.key());
            assert_eq!(record.value.as_deref(), log_record.value());
            assert_eq!(&record.headers, log_record.headers());
            assert_eq!(
                DefaultRecord::size_in_bytes_with_slices(
                    offset_delta,
                    timestamp_delta,
                    record.key.as_deref(),
                    record.value.as_deref(),
                    &record.headers,
                ),
                log_record.size_in_bytes()
            );
        }
    }

    /// Helper: build a record buffer for invalid-record tests
    fn build_invalid_record_buf(size_of_body: i32, alloc_size: usize, write_fn: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut buf = Vec::with_capacity(alloc_size);
        varint::write_varint(size_of_body, &mut buf).unwrap();
        write_fn(&mut buf);
        // pad to alloc_size
        buf.resize(alloc_size, 0);
        buf
    }

    /// Translated from DefaultRecordTest.testInvalidKeySize
    #[test]
    fn test_invalid_key_size() {
        let size_of_body: i32 = 100;
        let key_size: i32 = 105; // larger than full message

        let buf = build_invalid_record_buf(
            size_of_body,
            size_of_body as usize + varint::size_of_varint(size_of_body) as usize,
            |b| {
                b.push(0u8); // attributes
                varint::write_varlong(2, b).unwrap(); // timestampDelta
                varint::write_varint(1, b).unwrap(); // offsetDelta
                varint::write_varint(key_size, b).unwrap(); // keySize too large
            },
        );

        let mut pos = 0;
        let result = DefaultRecord::read_from(&buf, &mut pos, 0, 0, NO_SEQUENCE, None);
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordTest.testInvalidValueSize
    #[test]
    fn test_invalid_value_size() {
        let size_of_body: i32 = 100;
        let value_size: i32 = 105;

        let buf = build_invalid_record_buf(
            size_of_body,
            size_of_body as usize + varint::size_of_varint(size_of_body) as usize,
            |b| {
                b.push(0u8); // attributes
                varint::write_varlong(2, b).unwrap();
                varint::write_varint(1, b).unwrap();
                varint::write_varint(-1, b).unwrap(); // null key
                varint::write_varint(value_size, b).unwrap(); // value too large
            },
        );

        let mut pos = 0;
        let result = DefaultRecord::read_from(&buf, &mut pos, 0, 0, NO_SEQUENCE, None);
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordTest.testInvalidNumHeaders (negative count)
    #[test]
    fn test_invalid_num_headers_negative() {
        let size_of_body: i32 = 100;

        let buf = build_invalid_record_buf(
            size_of_body,
            size_of_body as usize + varint::size_of_varint(size_of_body) as usize,
            |b| {
                b.push(0u8);
                varint::write_varlong(2, b).unwrap();
                varint::write_varint(1, b).unwrap();
                varint::write_varint(-1, b).unwrap(); // null key
                varint::write_varint(-1, b).unwrap(); // null value
                varint::write_varint(-1, b).unwrap(); // -1 num.headers
            },
        );

        let mut pos = 0;
        let result = DefaultRecord::read_from(&buf, &mut pos, 0, 0, NO_SEQUENCE, None);
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordTest.testInvalidNumHeaders (count too large)
    #[test]
    fn test_invalid_num_headers_too_large() {
        let size_of_body: i32 = 100;

        let buf = build_invalid_record_buf(
            size_of_body,
            size_of_body as usize + varint::size_of_varint(size_of_body) as usize,
            |b| {
                b.push(0u8);
                varint::write_varlong(2, b).unwrap();
                varint::write_varint(1, b).unwrap();
                varint::write_varint(-1, b).unwrap();
                varint::write_varint(-1, b).unwrap();
                varint::write_varint(size_of_body, b).unwrap(); // more headers than remaining
            },
        );

        let mut pos = 0;
        let result = DefaultRecord::read_from(&buf, &mut pos, 0, 0, NO_SEQUENCE, None);
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordTest.testInvalidHeaderKey
    #[test]
    fn test_invalid_header_key() {
        let size_of_body: i32 = 100;

        let buf = build_invalid_record_buf(
            size_of_body,
            size_of_body as usize + varint::size_of_varint(size_of_body) as usize,
            |b| {
                b.push(0u8);
                varint::write_varlong(2, b).unwrap();
                varint::write_varint(1, b).unwrap();
                varint::write_varint(-1, b).unwrap();
                varint::write_varint(-1, b).unwrap();
                varint::write_varint(1, b).unwrap(); // 1 header
                varint::write_varint(105, b).unwrap(); // header key too long
            },
        );

        let mut pos = 0;
        let result = DefaultRecord::read_from(&buf, &mut pos, 0, 0, NO_SEQUENCE, None);
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordTest.testNullHeaderKey
    #[test]
    fn test_null_header_key() {
        let size_of_body: i32 = 100;

        let buf = build_invalid_record_buf(
            size_of_body,
            size_of_body as usize + varint::size_of_varint(size_of_body) as usize,
            |b| {
                b.push(0u8);
                varint::write_varlong(2, b).unwrap();
                varint::write_varint(1, b).unwrap();
                varint::write_varint(-1, b).unwrap();
                varint::write_varint(-1, b).unwrap();
                varint::write_varint(1, b).unwrap();
                varint::write_varint(-1, b).unwrap(); // null header key
            },
        );

        let mut pos = 0;
        let result = DefaultRecord::read_from(&buf, &mut pos, 0, 0, NO_SEQUENCE, None);
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordTest.testInvalidHeaderValue
    #[test]
    fn test_invalid_header_value() {
        let size_of_body: i32 = 100;

        let buf = build_invalid_record_buf(
            size_of_body,
            size_of_body as usize + varint::size_of_varint(size_of_body) as usize,
            |b| {
                b.push(0u8);
                varint::write_varlong(2, b).unwrap();
                varint::write_varint(1, b).unwrap();
                varint::write_varint(-1, b).unwrap();
                varint::write_varint(-1, b).unwrap();
                varint::write_varint(1, b).unwrap();
                varint::write_varint(1, b).unwrap(); // header key length 1
                b.push(b'x'); // header key byte
                varint::write_varint(105, b).unwrap(); // header value too long
            },
        );

        let mut pos = 0;
        let result = DefaultRecord::read_from(&buf, &mut pos, 0, 0, NO_SEQUENCE, None);
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordTest.testUnderflowReadingTimestamp
    #[test]
    fn test_underflow_reading_timestamp() {
        let size_of_body: i32 = 1;
        let mut buf = Vec::new();
        varint::write_varint(size_of_body, &mut buf).unwrap();
        buf.push(0u8); // attributes only, no timestamp

        let mut pos = 0;
        let result = DefaultRecord::read_from(&buf, &mut pos, 0, 0, NO_SEQUENCE, None);
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordTest.testSerdeNoSequence
    #[test]
    fn test_serde_no_sequence() {
        let key = b"hi";
        let value = b"there";
        let base_offset: i64 = 37;
        let offset_delta: i32 = 10;
        let base_timestamp: i64 = now_ms();
        let timestamp_delta: i64 = 323;

        let mut buf = Vec::with_capacity(1024);
        DefaultRecord::write_to(&mut buf, offset_delta, timestamp_delta, Some(key), Some(value), &[]).unwrap();

        let mut pos = 0;
        let record = DefaultRecord::read_from(&buf, &mut pos, base_offset, base_timestamp, NO_SEQUENCE, None).unwrap();
        assert_eq!(NO_SEQUENCE, record.sequence());
    }

    /// Translated from DefaultRecordTest.testInvalidSizeOfBodyInBytes
    #[test]
    fn test_invalid_size_of_body_in_bytes() {
        let size_of_body: i32 = 10;
        let mut buf = vec![0u8; 5];
        let mut writer: &mut [u8] = &mut buf;
        let written_len = {
            let mut v = Vec::new();
            varint::write_varint(size_of_body, &mut v).unwrap();
            writer.write_all(&v).unwrap();
            v.len()
        };
        let buf = &buf[..written_len]; // only the varint

        // Pad the buf just enough to read the varint but not the body
        let mut padded = buf.to_vec();
        padded.resize(written_len + 3, 0); // not enough for 10 bytes

        let mut pos = 0;
        let result = DefaultRecord::read_from(&padded, &mut pos, 0, 0, NO_SEQUENCE, None);
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordTest.testBasicSerdeInvalidHeaderCountTooHigh
    #[test]
    fn test_basic_serde_invalid_header_count_too_high() {
        let headers = vec![
            RecordHeader::new("foo", Some(b"value".to_vec())),
            RecordHeader::new("bar", None),
            RecordHeader::new("\"A\\u00ea\\u00f1\\u00fcC\"", Some(b"value".to_vec())),
        ];
        let record = SimpleRecord::new(15, Some(b"hi".to_vec()), Some(b"there".to_vec()), headers);

        let base_sequence: i32 = 723;
        let base_offset: i64 = 37;
        let offset_delta: i32 = 10;
        let base_timestamp: i64 = now_ms();
        let timestamp_delta: i64 = 323;

        let mut buf = Vec::with_capacity(1024);
        DefaultRecord::write_to(
            &mut buf,
            offset_delta,
            timestamp_delta,
            record.key.as_deref(),
            record.value.as_deref(),
            &record.headers,
        )
        .unwrap();

        // Corrupt the header count: set it to 8 (too high)
        // The header count is at a fixed position in our serialized record.
        // Find it: after varint(body_size) + 1(attr) + varlong(323) + varint(10) + varint(2) + "hi" + varint(5) + "there"
        // We need to find byte index 14 as in Java test. Actually the byte index depends on encoding.
        // Let's compute: the header count varint starts at the position after key+value
        // For this specific test case, we can find the position of the header count byte
        // and set it to 8 (zig-zag encoded as 16 = 0x10).
        // Safer approach: re-serialize with wrong header count by manually building.
        // But the Java test uses buffer.put(14, (byte) 8) where 8 is zig-zag encoded varint for 4.
        // In Java's varint encoding, byte value 8 = unsigned 8 = zig-zag decode = 4. But wait:
        // The Java test puts literal byte 8 at position 14. In varint format for small values,
        // a single byte < 128 is the value itself (unsigned). For header count, it's unsigned varint
        // from ByteUtils.readVarint which is zig-zag, so byte 8 = (8 >> 1) ^ -(8 & 1) = 4 ^ 0 = 4.
        // That changes header count from 3 to 4, so reading will try to parse 4 headers but only 3 exist.
        // We just need to verify that corrupting header count causes an error.

        // Re-read to find the header count position
        // The body size varint comes first, then the body.
        // Let's find where the header count is by scanning.
        let (body_size_val, body_varint_len) = varint::read_varint(&buf).unwrap();
        let _body_start = body_varint_len;
        // Instead of finding the exact position, let's just test that mismatched body size causes error
        // by corrupting the body to make it inconsistent.

        // Approach: manually write a record with header count = 8 (too high)
        let mut corrupt_buf = Vec::with_capacity(1024);
        // Write body size varint first (same as original)
        varint::write_varint(body_size_val, &mut corrupt_buf).unwrap();
        // Copy the body from original, but change the header count
        let body = &buf[body_varint_len..body_varint_len + body_size_val as usize];
        corrupt_buf.extend_from_slice(body);

        // Now find and corrupt the header count in corrupt_buf
        // We need to scan through the body to find the header count position
        let mut scan_pos = body_varint_len;
        scan_pos += 1; // attributes
        let (_, vl) = varint::read_varlong(&buf[scan_pos..]).unwrap();
        scan_pos += vl; // timestamp delta
        let (_, vl) = varint::read_varint(&buf[scan_pos..]).unwrap();
        scan_pos += vl; // offset delta
        let (ks, vl) = varint::read_varint(&buf[scan_pos..]).unwrap();
        scan_pos += vl; // key size
        if ks >= 0 {
            scan_pos += ks as usize;
        } // key
        let (vs, vl) = varint::read_varint(&buf[scan_pos..]).unwrap();
        scan_pos += vl; // value size
        if vs >= 0 {
            scan_pos += vs as usize;
        } // value
        // scan_pos now points to header count
        corrupt_buf[scan_pos] = 8; // set header count to 4 (zig-zag)

        let mut pos = 0;
        let result = DefaultRecord::read_from(&corrupt_buf, &mut pos, base_offset, base_timestamp, base_sequence, None);
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordTest.testBasicSerdeInvalidHeaderCountTooLow
    #[test]
    fn test_basic_serde_invalid_header_count_too_low() {
        let headers = vec![
            RecordHeader::new("foo", Some(b"value".to_vec())),
            RecordHeader::new("bar", None),
            RecordHeader::new("\"A\\u00ea\\u00f1\\u00fcC\"", Some(b"value".to_vec())),
        ];
        let record = SimpleRecord::new(15, Some(b"hi".to_vec()), Some(b"there".to_vec()), headers);

        let offset_delta: i32 = 10;
        let base_timestamp: i64 = now_ms();
        let timestamp_delta: i64 = 323;

        let mut buf = Vec::with_capacity(1024);
        DefaultRecord::write_to(
            &mut buf,
            offset_delta,
            timestamp_delta,
            record.key.as_deref(),
            record.value.as_deref(),
            &record.headers,
        )
        .unwrap();

        // Find header count position and set it to 4 (zig-zag encoded as 4 meaning 2 headers)
        let mut scan_pos = 0;
        let (_, vl) = varint::read_varint(&buf).unwrap();
        scan_pos += vl;
        scan_pos += 1; // attributes
        let (_, vl) = varint::read_varlong(&buf[scan_pos..]).unwrap();
        scan_pos += vl;
        let (_, vl) = varint::read_varint(&buf[scan_pos..]).unwrap();
        scan_pos += vl;
        let (ks, vl) = varint::read_varint(&buf[scan_pos..]).unwrap();
        scan_pos += vl;
        if ks >= 0 {
            scan_pos += ks as usize;
        }
        let (vs, vl) = varint::read_varint(&buf[scan_pos..]).unwrap();
        scan_pos += vl;
        if vs >= 0 {
            scan_pos += vs as usize;
        }
        // Change header count from 3 (zig-zag 6) to 2 (zig-zag 4)
        buf[scan_pos] = 4;

        let mut pos = 0;
        let result = DefaultRecord::read_from(&buf, &mut pos, 37, base_timestamp, 723, None);
        // Should fail because body size won't match after reading only 2 headers
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordTest.testUnderflowReadingVarlong
    ///
    /// Tests that reading a record where the varlong timestamp is truncated (not
    /// enough bytes in the body) returns an error.
    #[test]
    fn test_underflow_reading_varlong() {
        let attributes: u8 = 0;
        let size_of_body: i32 = 2; // one byte for attributes, one byte for partial timestamp
        // 156 needs 2 bytes in varlong encoding but body only has 1 byte left
        let timestamp_delta: i64 = 156;

        let mut buf = Vec::new();
        varint::write_varint(size_of_body, &mut buf).unwrap();
        buf.push(attributes);
        // Write the full varlong (2 bytes) for timestampDelta
        let mut varlong_buf = Vec::new();
        varint::write_varlong(timestamp_delta, &mut varlong_buf).unwrap();
        assert!(varlong_buf.len() >= 2, "156 should need >= 2 bytes in varlong");
        // Only write 1 byte of the varlong, simulating truncation
        buf.push(varlong_buf[0]);

        let mut pos = 0;
        let result = DefaultRecord::read_from(&buf, &mut pos, 0, 0, NO_SEQUENCE, None);
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordTest.testInvalidVarlong
    ///
    /// Tests that a varlong with an invalid final byte (the 10th byte with high
    /// bit set or with illegal bits in the 10th position) produces an error.
    #[test]
    fn test_invalid_varlong() {
        let attributes: u8 = 0;
        let size_of_body: i32 = 11; // one byte for attributes, 10 bytes for max timestamp

        let mut buf = Vec::new();
        varint::write_varint(size_of_body, &mut buf).unwrap();
        let record_start = buf.len();

        buf.push(attributes);
        // Write Long.MAX_VALUE as varlong (takes 10 bytes)
        varint::write_varlong(i64::MAX, &mut buf).unwrap();
        // Corrupt the last byte of the varlong to make it invalid
        // The 10th byte of the varlong is at record_start + 10
        // Set it to i8::MIN (0x80) which is an invalid final byte
        buf[record_start + 10] = 0x80u8;

        let mut pos = 0;
        let result = DefaultRecord::read_from(&buf, &mut pos, 0, 0, NO_SEQUENCE, None);
        assert!(result.is_err());
    }

    use crate::common::record::SimpleRecord;
}
