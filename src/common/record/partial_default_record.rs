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

//! Translation of `org.apache.kafka.common.record.PartialDefaultRecord`.
//!
//! A "partial" record retains only the metadata (offset, timestamp, sequence,
//! attributes) and the key/value sizes — the actual key, value, and header
//! payloads are skipped during decode. This is used by the broker validation
//! path to walk a record set without paying the cost of materializing payloads.

use std::fmt;
use std::io::Read;

use crate::common::errors::KafkaError;
use crate::common::record::TimestampType;
use crate::common::record::default_record_batch::increment_sequence;
use crate::common::record::record::Record;
use crate::common::record::record_batch::{MAGIC_VALUE_V2, NO_SEQUENCE};
use crate::common::utils::byte_utils;

use crate::common::header::RecordHeader;

/// `org.apache.kafka.common.record.PartialDefaultRecord`. Carries only
/// metadata + key/value sizes; calling `key()`, `value()`, or `headers()`
/// returns `None`/`&[]` (Java throws `UnsupportedOperationException`; we
/// surface the absence rather than panicking — see CLAUDE.md rule 10 on
/// avoiding panics in public APIs).
#[derive(Clone, Debug)]
pub struct PartialDefaultRecord {
    size_in_bytes: i32,
    attributes: i8,
    offset: i64,
    timestamp: i64,
    sequence: i32,
    key_size: i32,
    value_size: i32,
}

impl PartialDefaultRecord {
    pub(crate) fn new(
        size_in_bytes: i32,
        attributes: i8,
        offset: i64,
        timestamp: i64,
        sequence: i32,
        key_size: i32,
        value_size: i32,
    ) -> Self {
        PartialDefaultRecord { size_in_bytes, attributes, offset, timestamp, sequence, key_size, value_size }
    }

    /// Mirrors `attributes()` (no `Record` trait method in Java).
    pub fn attributes(&self) -> i8 {
        self.attributes
    }
}

impl Record for PartialDefaultRecord {
    fn offset(&self) -> i64 {
        self.offset
    }

    fn sequence(&self) -> i32 {
        self.sequence
    }

    fn size_in_bytes(&self) -> i32 {
        self.size_in_bytes
    }

    fn timestamp(&self) -> i64 {
        self.timestamp
    }

    fn ensure_valid(&self) -> Result<(), KafkaError> {
        Ok(())
    }

    fn key_size(&self) -> i32 {
        self.key_size
    }

    fn has_key(&self) -> bool {
        self.key_size >= 0
    }

    fn key(&self) -> Option<&[u8]> {
        // Java throws UnsupportedOperationException; we return None to avoid
        // a panic in public-API surfaces. Callers that need the payload
        // should use `DefaultRecord::read_from_buffer` instead.
        None
    }

    fn value_size(&self) -> i32 {
        self.value_size
    }

    fn has_value(&self) -> bool {
        self.value_size >= 0
    }

    fn value(&self) -> Option<&[u8]> {
        None
    }

    fn has_magic(&self, magic: i8) -> bool {
        magic >= MAGIC_VALUE_V2
    }

    fn is_compressed(&self) -> bool {
        false
    }

    fn has_timestamp_type(&self, _timestamp_type: TimestampType) -> bool {
        false
    }

    fn headers(&self) -> &[RecordHeader] {
        &[]
    }
}

impl PartialEq for PartialDefaultRecord {
    fn eq(&self, other: &Self) -> bool {
        self.size_in_bytes == other.size_in_bytes
            && self.attributes == other.attributes
            && self.offset == other.offset
            && self.timestamp == other.timestamp
            && self.sequence == other.sequence
            && self.key_size == other.key_size
            && self.value_size == other.value_size
    }
}

impl Eq for PartialDefaultRecord {}

impl fmt::Display for PartialDefaultRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PartialDefaultRecord(offset={}, timestamp={}, key={} bytes, value={} bytes)",
            self.offset, self.timestamp, self.key_size, self.value_size
        )
    }
}

/// Mirrors Java's `DefaultRecord.readPartiallyFrom(InputStream, ...)`. Reads
/// the size-prefix varint and then the body, skipping over the key, value,
/// and header payloads while validating their varint-prefixed lengths.
///
/// # Errors
///
/// Returns [`KafkaError::InvalidRecord`] for any malformed input (truncated
/// stream, oversized varint, negative header key size, …).
pub fn read_partially_from<R: Read>(
    input: &mut R,
    base_offset: i64,
    base_timestamp: i64,
    base_sequence: i32,
    log_append_time: Option<i64>,
) -> Result<PartialDefaultRecord, KafkaError> {
    let size_of_body = byte_utils::read_varint_from_stream(input).map_err(map_invalid)?;
    if size_of_body < 0 {
        return Err(invalid_record_struct());
    }
    let total_size_in_bytes = byte_utils::size_of_varint(size_of_body) as i32 + size_of_body;
    read_partially_from_inner(
        input,
        total_size_in_bytes,
        size_of_body,
        base_offset,
        base_timestamp,
        base_sequence,
        log_append_time,
    )
}

fn read_partially_from_inner<R: Read>(
    input: &mut R,
    size_in_bytes: i32,
    size_of_body: i32,
    base_offset: i64,
    base_timestamp: i64,
    base_sequence: i32,
    log_append_time: Option<i64>,
) -> Result<PartialDefaultRecord, KafkaError> {
    // We read into a body-sized scratch buffer so we can both (a) bound the
    // varint readers to the declared body length and (b) emit a clear
    // "invalid structure" error when the body is truncated. This mirrors what
    // the Java code does conceptually (reading from a stream while validating
    // the byte budget); Java's implementation skips bytes via
    // `InputStream.skip` to avoid the read, but since `read_partially_from`
    // is used only by the broker validation path (consumer-side), the
    // performance is not on the producer hot path.
    let body_size_usize = size_of_body as usize;
    let mut body = vec![0u8; body_size_usize];
    let n = read_fully(input, &mut body)?;
    if n != body_size_usize {
        return Err(invalid_record_struct());
    }
    let mut cursor = std::io::Cursor::new(body);

    let attributes = read_u8(&mut cursor)? as i8;
    let timestamp_delta = byte_utils::read_varlong_from_stream(&mut cursor).map_err(map_invalid)?;
    let mut timestamp = base_timestamp.wrapping_add(timestamp_delta);
    if let Some(lat) = log_append_time {
        timestamp = lat;
    }

    let offset_delta = byte_utils::read_varint_from_stream(&mut cursor).map_err(map_invalid)?;
    let offset = base_offset.wrapping_add(offset_delta as i64);
    let sequence = if base_sequence >= 0 {
        increment_sequence(base_sequence, offset_delta)
    } else {
        NO_SEQUENCE
    };

    // Skip key
    let key_size = byte_utils::read_varint_from_stream(&mut cursor).map_err(map_invalid)?;
    skip_bytes(&mut cursor, key_size)?;
    // Skip value
    let value_size = byte_utils::read_varint_from_stream(&mut cursor).map_err(map_invalid)?;
    skip_bytes(&mut cursor, value_size)?;

    // Skip headers
    let num_headers = byte_utils::read_varint_from_stream(&mut cursor).map_err(map_invalid)?;
    if num_headers < 0 {
        return Err(KafkaError::InvalidRecord(format!(
            "Found invalid number of record headers {}",
            num_headers
        )));
    }
    for _ in 0..num_headers {
        let header_key_size = byte_utils::read_varint_from_stream(&mut cursor).map_err(map_invalid)?;
        if header_key_size < 0 {
            return Err(KafkaError::InvalidRecord(format!(
                "Invalid negative header key size {}",
                header_key_size
            )));
        }
        skip_bytes(&mut cursor, header_key_size)?;
        let header_value_size = byte_utils::read_varint_from_stream(&mut cursor).map_err(map_invalid)?;
        skip_bytes(&mut cursor, header_value_size)?;
    }

    Ok(PartialDefaultRecord::new(
        size_in_bytes,
        attributes,
        offset,
        timestamp,
        sequence,
        key_size,
        value_size,
    ))
}

fn read_u8<R: Read>(reader: &mut R) -> Result<u8, KafkaError> {
    let mut byte = [0u8; 1];
    reader.read_exact(&mut byte).map_err(|_| invalid_record_struct())?;
    Ok(byte[0])
}

/// Skip `bytes_to_skip` bytes from `reader`. No-op when `bytes_to_skip <= 0`
/// (some fields encode null as `-1`).
fn skip_bytes<R: Read>(reader: &mut R, bytes_to_skip: i32) -> Result<(), KafkaError> {
    if bytes_to_skip <= 0 {
        return Ok(());
    }
    let mut buf = [0u8; 64];
    let mut remaining = bytes_to_skip as usize;
    while remaining > 0 {
        let chunk = remaining.min(buf.len());
        match reader.read(&mut buf[..chunk]) {
            Ok(0) => {
                return Err(KafkaError::InvalidRecord(format!(
                    "Reached end of input stream before skipping all bytes. Remaining bytes:{}",
                    remaining
                )));
            },
            Ok(n) => remaining -= n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(invalid_record_struct()),
        }
    }
    Ok(())
}

fn read_fully<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<usize, KafkaError> {
    let mut total = 0;
    while total < buf.len() {
        match reader.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(invalid_record_struct()),
        }
    }
    Ok(total)
}

fn invalid_record_struct() -> KafkaError {
    KafkaError::InvalidRecord("Found invalid record structure".to_owned())
}

fn map_invalid(_e: KafkaError) -> KafkaError {
    invalid_record_struct()
}

#[cfg(test)]
mod tests {
    // Translation of the partial-decode tests in `DefaultRecordTest` (the
    // `*Partial` variants).

    use super::*;
    use crate::common::header::RecordHeader;
    use crate::common::record::default_record::write_to;
    use crate::common::utils::byte_utils;

    fn pad_to_body(buf: &mut Vec<u8>, size_of_body: i32) {
        let prefix_len = byte_utils::size_of_varint(size_of_body);
        while buf.len() < prefix_len + size_of_body as usize {
            buf.push(0);
        }
    }

    /// Java: `testInvalidKeySizePartial`.
    #[test]
    fn invalid_key_size_partial() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let key_size: i32 = 105;
        let mut buf: Vec<u8> = Vec::new();
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(attributes);
        byte_utils::write_varlong(timestamp_delta, &mut buf);
        byte_utils::write_varint(offset_delta, &mut buf);
        byte_utils::write_varint(key_size, &mut buf);
        pad_to_body(&mut buf, size_of_body);

        let mut cursor = std::io::Cursor::new(buf);
        let err = read_partially_from(&mut cursor, 0, 0, NO_SEQUENCE, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
    }

    /// Java: `testInvalidValueSizePartial`.
    #[test]
    fn invalid_value_size_partial() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let value_size: i32 = 105;
        let mut buf: Vec<u8> = Vec::new();
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(attributes);
        byte_utils::write_varlong(timestamp_delta, &mut buf);
        byte_utils::write_varint(offset_delta, &mut buf);
        byte_utils::write_varint(-1, &mut buf); // null key
        byte_utils::write_varint(value_size, &mut buf);
        pad_to_body(&mut buf, size_of_body);

        let mut cursor = std::io::Cursor::new(buf);
        let err = read_partially_from(&mut cursor, 0, 0, NO_SEQUENCE, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
    }

    /// Java: `testInvalidNumHeadersPartial`.
    #[test]
    fn invalid_num_headers_partial() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let mut buf: Vec<u8> = Vec::new();
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(attributes);
        byte_utils::write_varlong(timestamp_delta, &mut buf);
        byte_utils::write_varint(offset_delta, &mut buf);
        byte_utils::write_varint(-1, &mut buf); // null key
        byte_utils::write_varint(-1, &mut buf); // null value
        byte_utils::write_varint(-1, &mut buf); // -1 num.headers, not allowed
        pad_to_body(&mut buf, size_of_body);

        let mut cursor = std::io::Cursor::new(buf);
        let err = read_partially_from(&mut cursor, 0, 0, NO_SEQUENCE, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
    }

    /// Java: `testInvalidHeaderKeyPartial`.
    #[test]
    fn invalid_header_key_partial() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let mut buf: Vec<u8> = Vec::new();
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(attributes);
        byte_utils::write_varlong(timestamp_delta, &mut buf);
        byte_utils::write_varint(offset_delta, &mut buf);
        byte_utils::write_varint(-1, &mut buf);
        byte_utils::write_varint(-1, &mut buf);
        byte_utils::write_varint(1, &mut buf);
        byte_utils::write_varint(105, &mut buf); // header key too long
        pad_to_body(&mut buf, size_of_body);

        let mut cursor = std::io::Cursor::new(buf);
        let err = read_partially_from(&mut cursor, 0, 0, NO_SEQUENCE, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
    }

    /// Java: `testNullHeaderKeyPartial`.
    #[test]
    fn null_header_key_partial() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let mut buf: Vec<u8> = Vec::new();
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(attributes);
        byte_utils::write_varlong(timestamp_delta, &mut buf);
        byte_utils::write_varint(offset_delta, &mut buf);
        byte_utils::write_varint(-1, &mut buf);
        byte_utils::write_varint(-1, &mut buf);
        byte_utils::write_varint(1, &mut buf);
        byte_utils::write_varint(-1, &mut buf); // null header key not allowed
        pad_to_body(&mut buf, size_of_body);

        let mut cursor = std::io::Cursor::new(buf);
        let err = read_partially_from(&mut cursor, 0, 0, NO_SEQUENCE, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
    }

    /// Java: `testInvalidHeaderValuePartial`.
    #[test]
    fn invalid_header_value_partial() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let mut buf: Vec<u8> = Vec::new();
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(attributes);
        byte_utils::write_varlong(timestamp_delta, &mut buf);
        byte_utils::write_varint(offset_delta, &mut buf);
        byte_utils::write_varint(-1, &mut buf);
        byte_utils::write_varint(-1, &mut buf);
        byte_utils::write_varint(1, &mut buf);
        byte_utils::write_varint(1, &mut buf); // header key size = 1
        buf.push(1); // one byte header key
        byte_utils::write_varint(105, &mut buf); // header value too long
        pad_to_body(&mut buf, size_of_body);

        let mut cursor = std::io::Cursor::new(buf);
        let err = read_partially_from(&mut cursor, 0, 0, NO_SEQUENCE, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
    }

    /// Round-trips a valid record through `read_partially_from` and confirms
    /// metadata + key/value sizes match.
    #[test]
    fn round_trip_yields_correct_metadata_and_sizes() {
        let header = RecordHeader::new("h", Some(b"v"));
        let mut out: Vec<u8> = Vec::new();
        write_to(&mut out, 1, 2, Some(b"key"), Some(b"value"), std::slice::from_ref(&header)).unwrap();

        let mut cursor = std::io::Cursor::new(out.as_slice());
        let pdr = read_partially_from(&mut cursor, 100, 1000, 50, None).unwrap();
        assert_eq!(pdr.offset(), 101);
        assert_eq!(pdr.timestamp(), 1002);
        assert_eq!(pdr.sequence(), 51);
        assert_eq!(pdr.key_size(), 3);
        assert_eq!(pdr.value_size(), 5);
        assert!(pdr.has_key());
        assert!(pdr.has_value());
    }

    /// `key()` / `value()` / `headers()` return absent values (Java throws —
    /// see CLAUDE.md rule 10 on avoiding panics in public APIs).
    #[test]
    fn payload_accessors_return_empty() {
        let header = RecordHeader::new("h", Some(b"v"));
        let mut out: Vec<u8> = Vec::new();
        write_to(&mut out, 1, 2, Some(b"key"), Some(b"value"), std::slice::from_ref(&header)).unwrap();

        let mut cursor = std::io::Cursor::new(out.as_slice());
        let pdr = read_partially_from(&mut cursor, 0, 0, 0, None).unwrap();
        assert_eq!(pdr.key(), None);
        assert_eq!(pdr.value(), None);
        assert!(pdr.headers().is_empty());
    }
}
