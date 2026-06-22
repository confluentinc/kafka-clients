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

//! The default (v2) record format for Kafka.
//!
//! This module implements the inner record format for magic 2 and above.
//! The schema is:
//!
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
//!
//! The offset and timestamp deltas compute the difference relative to the base
//! offset and base timestamp of the batch that this record is contained in.
//!
//! Corresponds to Java's `org.apache.kafka.common.record.DefaultRecord`.

use std::io::{self, Read, Write};

use crate::common::header::Header;
use crate::common::header::internals::RecordHeader;
use crate::common::protocol::varint;
use crate::common::record::InvalidRecordError;
use crate::common::record::RecordBatch;
use crate::common::record::TimestampType;

/// Maximum overhead of a default record, excluding key, value, and headers.
///
/// 5 bytes (length varint) + 10 bytes (timestamp varlong) + 5 bytes (offset varint) + 1 byte (attributes) = 21.
pub const MAX_RECORD_OVERHEAD: i32 = 21;

/// Size of varint-encoded -1 (null marker).
const NULL_VARINT_SIZE_BYTES: i32 = varint::size_of_varint(-1);

/// The default (v2) record format for Kafka.
///
/// Corresponds to Java's `org.apache.kafka.common.record.DefaultRecord`.
#[derive(Clone, Debug)]
pub struct DefaultRecord {
    size_in_bytes: i32,
    attributes: i8,
    offset: i64,
    timestamp: i64,
    sequence: i32,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<RecordHeader>,
}

impl DefaultRecord {
    /// Create a new `DefaultRecord` with all fields.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        size_in_bytes: i32,
        attributes: i8,
        offset: i64,
        timestamp: i64,
        sequence: i32,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<RecordHeader>,
    ) -> Self {
        Self { size_in_bytes, attributes, offset, timestamp, sequence, key, value, headers }
    }

    /// Returns the record attributes byte.
    pub fn attributes(&self) -> i8 {
        self.attributes
    }

    /// Write a record to `out` and return its total serialized size in bytes.
    ///
    /// The record is written in the v2 format: length prefix (varint) followed
    /// by attributes, timestamp delta, offset delta, key, value, and headers.
    ///
    /// # Errors
    /// Returns an I/O error if writing fails.
    pub fn write_to<W: Write>(
        out: &mut W,
        offset_delta: i32,
        timestamp_delta: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> io::Result<i32> {
        let key_size = key.map_or(-1, |k| k.len() as i32);
        let value_size = value.map_or(-1, |v| v.len() as i32);
        let size_of_body = size_of_body_in_bytes(offset_delta, timestamp_delta, key_size, value_size, headers);

        varint::write_varint(size_of_body, out)?;

        // attributes: currently unused, always 0
        out.write_all(&[0u8])?;

        varint::write_varlong(timestamp_delta, out)?;
        varint::write_varint(offset_delta, out)?;

        // key
        match key {
            None => {
                varint::write_varint(-1, out)?;
            },
            Some(k) => {
                varint::write_varint(k.len() as i32, out)?;
                out.write_all(k)?;
            },
        }

        // value
        match value {
            None => {
                varint::write_varint(-1, out)?;
            },
            Some(v) => {
                varint::write_varint(v.len() as i32, out)?;
                out.write_all(v)?;
            },
        }

        // headers
        varint::write_varint(headers.len() as i32, out)?;

        for header in headers {
            let header_key = header.key();
            let utf8_bytes = header_key.as_bytes();
            varint::write_varint(utf8_bytes.len() as i32, out)?;
            out.write_all(utf8_bytes)?;

            match header.value() {
                None => {
                    varint::write_varint(-1, out)?;
                },
                Some(header_value) => {
                    varint::write_varint(header_value.len() as i32, out)?;
                    out.write_all(header_value)?;
                },
            }
        }

        Ok(varint::size_of_varint(size_of_body) + size_of_body)
    }

    /// Read a `DefaultRecord` from a byte buffer.
    ///
    /// The buffer must be positioned at the start of the record (the length varint).
    ///
    /// # Errors
    /// Returns `InvalidRecordError` if the record is malformed.
    pub fn read_from_buffer(
        buffer: &[u8],
        base_offset: i64,
        base_timestamp: i64,
        base_sequence: i32,
        log_append_time: Option<i64>,
    ) -> Result<(DefaultRecord, usize), InvalidRecordError> {
        let (size_of_body, varint_size) = varint::read_varint(buffer)
            .map_err(|e| InvalidRecordError::new(format!("Failed to read record size: {}", e)))?;

        if size_of_body < 0 {
            return Err(InvalidRecordError::new(format!(
                "Invalid record size: expected non-negative size but got {}",
                size_of_body
            )));
        }

        let body_start = varint_size;
        let body = &buffer[body_start..];

        if (body.len() as i32) < size_of_body {
            return Err(InvalidRecordError::new(format!(
                "Invalid record size: expected {} bytes in record payload, but instead the buffer has only {} remaining bytes.",
                size_of_body,
                body.len()
            )));
        }

        let record_body = &body[..size_of_body as usize];
        let record = Self::read_from_body(
            record_body,
            size_of_body,
            base_offset,
            base_timestamp,
            base_sequence,
            log_append_time,
        )?;

        let total_consumed = varint_size + size_of_body as usize;
        Ok((record, total_consumed))
    }

    /// Read a `DefaultRecord` from a `Read` stream.
    ///
    /// # Errors
    /// Returns `InvalidRecordError` if the record is malformed or the stream ends early.
    pub fn read_from_stream<R: Read>(
        input: &mut R,
        base_offset: i64,
        base_timestamp: i64,
        base_sequence: i32,
        log_append_time: Option<i64>,
    ) -> Result<DefaultRecord, InvalidRecordError> {
        let size_of_body = varint::read_varint_reader(input)
            .map_err(|e| InvalidRecordError::new(format!("Failed to read record size: {}", e)))?;

        if size_of_body < 0 {
            return Err(InvalidRecordError::new(format!(
                "Invalid record size: expected non-negative size but got {}",
                size_of_body
            )));
        }

        let mut record_buffer = vec![0u8; size_of_body as usize];
        let bytes_read = read_fully(input, &mut record_buffer)?;
        if bytes_read != size_of_body as usize {
            return Err(InvalidRecordError::new(format!(
                "Invalid record size: expected {} bytes in record payload, but the record payload reached EOF.",
                size_of_body
            )));
        }

        Self::read_from_body(
            &record_buffer,
            size_of_body,
            base_offset,
            base_timestamp,
            base_sequence,
            log_append_time,
        )
    }

    /// Read a record body from a byte slice. The slice should contain exactly
    /// `size_of_body` bytes starting after the length varint.
    ///
    /// This is the owned form: it deep-copies key/value/header bytes out of
    /// `body`. The zero-copy receive path (`consumer-threading.md` §27) uses
    /// [`DefaultRecord::read_ref_from_buffer`] instead, which borrows from the
    /// buffer. Both share the same parse logic via [`DefaultRecordRef`].
    fn read_from_body(
        body: &[u8],
        size_of_body: i32,
        base_offset: i64,
        base_timestamp: i64,
        base_sequence: i32,
        log_append_time: Option<i64>,
    ) -> Result<DefaultRecord, InvalidRecordError> {
        let record_ref = DefaultRecordRef::parse_body(
            body,
            size_of_body,
            base_offset,
            base_timestamp,
            base_sequence,
            log_append_time,
        )?;
        Ok(record_ref.to_owned_record())
    }

    /// Read a borrowing [`DefaultRecordRef`] from a byte buffer, plus the
    /// number of bytes consumed (length varint + body).
    ///
    /// The buffer must be positioned at the start of the record (the length
    /// varint). The returned view borrows key/value/header bytes directly from
    /// `buffer` — **no copy** is made. This is the zero-copy entry point used
    /// by the consumer receive path (`consumer-threading.md` §27).
    ///
    /// # Errors
    /// Returns `InvalidRecordError` if the record is malformed.
    pub fn read_ref_from_buffer(
        buffer: &[u8],
        base_offset: i64,
        base_timestamp: i64,
        base_sequence: i32,
        log_append_time: Option<i64>,
    ) -> Result<(DefaultRecordRef<'_>, usize), InvalidRecordError> {
        let (size_of_body, varint_size) = varint::read_varint(buffer)
            .map_err(|e| InvalidRecordError::new(format!("Failed to read record size: {}", e)))?;

        if size_of_body < 0 {
            return Err(InvalidRecordError::new(format!(
                "Invalid record size: expected non-negative size but got {}",
                size_of_body
            )));
        }

        let body_start = varint_size;
        let body = &buffer[body_start..];

        if (body.len() as i32) < size_of_body {
            return Err(InvalidRecordError::new(format!(
                "Invalid record size: expected {} bytes in record payload, but instead the buffer has only {} remaining bytes.",
                size_of_body,
                body.len()
            )));
        }

        let record_body = &body[..size_of_body as usize];
        let record_ref = DefaultRecordRef::parse_body(
            record_body,
            size_of_body,
            base_offset,
            base_timestamp,
            base_sequence,
            log_append_time,
        )?;

        let total_consumed = varint_size + size_of_body as usize;
        Ok((record_ref, total_consumed))
    }

    /// Compute the total serialized size of a record with the given parameters.
    ///
    /// This includes the length prefix varint.
    pub fn size_in_bytes_for(
        offset_delta: i32,
        timestamp_delta: i64,
        key_size: i32,
        value_size: i32,
        headers: &[RecordHeader],
    ) -> i32 {
        let body_size = size_of_body_in_bytes(offset_delta, timestamp_delta, key_size, value_size, headers);
        body_size + varint::size_of_varint(body_size)
    }

    /// Compute the total serialized size from key/value slices.
    pub fn size_in_bytes_with_slices(
        offset_delta: i32,
        timestamp_delta: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> i32 {
        let key_size = key.map_or(-1, |k| k.len() as i32);
        let value_size = value.map_or(-1, |v| v.len() as i32);
        Self::size_in_bytes_for(offset_delta, timestamp_delta, key_size, value_size, headers)
    }

    /// Compute the upper bound of the record size.
    ///
    /// Uses MAX_RECORD_OVERHEAD for the fixed overhead.
    pub fn record_size_upper_bound(key: Option<&[u8]>, value: Option<&[u8]>, headers: &[RecordHeader]) -> i32 {
        let key_size = key.map_or(-1, |k| k.len() as i32);
        let value_size = value.map_or(-1, |v| v.len() as i32);
        MAX_RECORD_OVERHEAD + size_of_key_value_headers(key_size, value_size, headers)
    }
}

/// Compute the size of the record body in bytes (excluding the length prefix).
///
/// Corresponds to Java's `DefaultRecord.sizeOfBodyInBytes(int, long, int, int, Header[])`.
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

/// Compute the size of key, value, and headers fields.
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
        let header_key = header.key();
        let header_key_size = header_key.len() as i32;
        size += varint::size_of_varint(header_key_size) + header_key_size;

        match header.value() {
            None => {
                size += NULL_VARINT_SIZE_BYTES;
            },
            Some(header_value) => {
                let hv_size = header_value.len() as i32;
                size += varint::size_of_varint(hv_size) + hv_size;
            },
        }
    }

    size
}

/// Borrow `size` bytes from `buffer` at `*pos`, advancing the cursor. If
/// `size` is negative (null marker), returns `None`. Zero-copy: returns a
/// slice into `buffer`, never a fresh allocation.
fn slice_bytes<'a>(buffer: &'a [u8], pos: &mut usize, size: i32) -> Result<Option<&'a [u8]>, InvalidRecordError> {
    if size < 0 {
        return Ok(None);
    }
    let size = size as usize;
    if *pos + size > buffer.len() {
        return Err(InvalidRecordError::new("Found invalid record structure"));
    }
    let data = &buffer[*pos..*pos + size];
    *pos += size;
    Ok(Some(data))
}

/// A borrowing view of a single v2 record, parsed from a batch buffer without
/// copying its key, value, or header bytes.
///
/// Corresponds to the per-record data of Java's `DefaultRecord`, but the
/// key/value/header bytes remain borrowed from the underlying fetch buffer
/// (see `consumer-threading.md` §27). The header bytes are kept as the raw
/// (unparsed) slice and decoded on demand by [`DefaultRecordRef::headers`] so
/// that records whose headers are never read incur no header-parsing cost.
#[derive(Clone, Debug)]
pub struct DefaultRecordRef<'a> {
    size_in_bytes: i32,
    attributes: i8,
    offset: i64,
    timestamp: i64,
    sequence: i32,
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
    /// Raw, still-encoded header section ([HeaderKeyLength HeaderKey
    /// HeaderValueLength HeaderValue]*), borrowed from the buffer.
    headers_bytes: &'a [u8],
    num_headers: i32,
}

impl<'a> DefaultRecordRef<'a> {
    /// Parse a record body (the bytes after the length varint) into a
    /// borrowing view. `body` must contain exactly `size_of_body` bytes.
    fn parse_body(
        body: &'a [u8],
        size_of_body: i32,
        base_offset: i64,
        base_timestamp: i64,
        base_sequence: i32,
        log_append_time: Option<i64>,
    ) -> Result<DefaultRecordRef<'a>, InvalidRecordError> {
        if size_of_body < 0 {
            return Err(InvalidRecordError::new(format!(
                "Invalid record size: expected non-negative size but got {}",
                size_of_body
            )));
        }

        if (body.len() as i32) < size_of_body {
            return Err(InvalidRecordError::new(format!(
                "Invalid record size: expected {} bytes in record payload, but instead the buffer has only {} remaining bytes.",
                size_of_body,
                body.len()
            )));
        }

        let mut pos = 0;

        // attributes
        if pos >= body.len() {
            return Err(InvalidRecordError::new("Found invalid record structure"));
        }
        let attributes = body[pos] as i8;
        pos += 1;

        // timestamp delta
        let (timestamp_delta, consumed) = varint::read_varlong(&body[pos..])
            .map_err(|_| InvalidRecordError::new("Found invalid record structure"))?;
        pos += consumed;

        let mut timestamp = base_timestamp + timestamp_delta;
        if let Some(lat) = log_append_time {
            timestamp = lat;
        }

        // offset delta
        let (offset_delta, consumed) =
            varint::read_varint(&body[pos..]).map_err(|_| InvalidRecordError::new("Found invalid record structure"))?;
        pos += consumed;

        let offset = base_offset + offset_delta as i64;
        let sequence = if base_sequence >= 0 {
            increment_sequence(base_sequence, offset_delta)
        } else {
            RecordBatch::NO_SEQUENCE
        };

        // key
        let (key_size, consumed) =
            varint::read_varint(&body[pos..]).map_err(|_| InvalidRecordError::new("Found invalid record structure"))?;
        pos += consumed;

        let key = slice_bytes(body, &mut pos, key_size)?;

        // value
        let (value_size, consumed) =
            varint::read_varint(&body[pos..]).map_err(|_| InvalidRecordError::new("Found invalid record structure"))?;
        pos += consumed;

        let value = slice_bytes(body, &mut pos, value_size)?;

        // headers
        let (num_headers, consumed) =
            varint::read_varint(&body[pos..]).map_err(|_| InvalidRecordError::new("Found invalid record structure"))?;
        pos += consumed;

        if num_headers < 0 {
            return Err(InvalidRecordError::new(format!(
                "Found invalid number of record headers {}",
                num_headers
            )));
        }

        let remaining = body.len() - pos;
        if (num_headers as usize) > remaining {
            return Err(InvalidRecordError::new(format!(
                "Found invalid number of record headers. {} is larger than the remaining size of the buffer",
                num_headers
            )));
        }

        // The header section is the rest of the body. We borrow it raw and
        // validate by fully parsing it (so malformed-header tests still
        // fail), but we do NOT allocate owned `RecordHeader`s here — that is
        // deferred to `headers()` / `to_owned_record()`.
        let headers_start = pos;
        let mut header_pos = pos;
        for _ in 0..num_headers {
            parse_one_header(body, &mut header_pos)?;
        }
        let headers_bytes = &body[headers_start..header_pos];

        // validate that we consumed exactly the right number of bytes
        if header_pos != size_of_body as usize {
            return Err(InvalidRecordError::new(format!(
                "Invalid record size: expected to read {} bytes in record payload, but instead read {}",
                size_of_body, header_pos
            )));
        }

        let total_size_in_bytes = varint::size_of_varint(size_of_body) + size_of_body;
        Ok(DefaultRecordRef {
            size_in_bytes: total_size_in_bytes,
            attributes,
            offset,
            timestamp,
            sequence,
            key,
            value,
            headers_bytes,
            num_headers,
        })
    }

    /// The record's offset in the log.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// The producer-assigned sequence number.
    pub fn sequence(&self) -> i32 {
        self.sequence
    }

    /// The record's total serialized size, including the length prefix.
    pub fn size_in_bytes(&self) -> i32 {
        self.size_in_bytes
    }

    /// The record's timestamp.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// The record attributes byte.
    pub fn attributes(&self) -> i8 {
        self.attributes
    }

    /// The size in bytes of the key, or -1 if there is no key.
    pub fn key_size(&self) -> i32 {
        self.key.map_or(-1, |k| k.len() as i32)
    }

    /// Whether this record has a key.
    pub fn has_key(&self) -> bool {
        self.key.is_some()
    }

    /// The record's key bytes, borrowed from the buffer, or `None`.
    pub fn key(&self) -> Option<&'a [u8]> {
        self.key
    }

    /// The size in bytes of the value, or -1 if the value is null.
    pub fn value_size(&self) -> i32 {
        self.value.map_or(-1, |v| v.len() as i32)
    }

    /// Whether a value is present (i.e. the value is not null).
    pub fn has_value(&self) -> bool {
        self.value.is_some()
    }

    /// The record's value bytes, borrowed from the buffer, or `None`.
    pub fn value(&self) -> Option<&'a [u8]> {
        self.value
    }

    /// Decode and return the record's headers as owned `RecordHeader`s.
    ///
    /// Per `consumer-threading.md` §27, Milestone-8 holds owned headers on the
    /// emitted `ConsumerRecord`; this is the single point where that owned copy
    /// is produced (only when the caller actually needs headers).
    pub fn headers(&self) -> Result<Vec<RecordHeader>, InvalidRecordError> {
        if self.num_headers == 0 {
            return Ok(Vec::new());
        }
        let mut headers = Vec::with_capacity(self.num_headers as usize);
        let mut pos = 0;
        for _ in 0..self.num_headers {
            let (header_key, header_value) = parse_one_header(self.headers_bytes, &mut pos)?;
            let header_key =
                std::str::from_utf8(header_key).map_err(|_| InvalidRecordError::new("Invalid UTF-8 in header key"))?;
            headers.push(RecordHeader::new(header_key.to_string(), header_value.map(|v| v.to_vec())));
        }
        Ok(headers)
    }

    /// Materialize this borrowing view into an owned [`DefaultRecord`],
    /// copying the key, value, and header bytes.
    fn to_owned_record(&self) -> DefaultRecord {
        // Header parsing here cannot fail: `parse_body` already validated the
        // header section fully. `expect` documents that invariant.
        let headers = self.headers().expect("header section validated in parse_body");
        DefaultRecord::new(
            self.size_in_bytes,
            self.attributes,
            self.offset,
            self.timestamp,
            self.sequence,
            self.key.map(|k| k.to_vec()),
            self.value.map(|v| v.to_vec()),
            headers,
        )
    }
}

/// Parse a single header `[HeaderKeyLength HeaderKey HeaderValueLength
/// HeaderValue]` from `buffer` at `*pos`, advancing the cursor. Returns the
/// key bytes (borrowed) and the optional value bytes (borrowed). The header
/// key may not be null.
fn parse_one_header<'a>(buffer: &'a [u8], pos: &mut usize) -> Result<(&'a [u8], Option<&'a [u8]>), InvalidRecordError> {
    let (header_key_size, consumed) =
        varint::read_varint(&buffer[*pos..]).map_err(|_| InvalidRecordError::new("Found invalid record structure"))?;
    *pos += consumed;

    if header_key_size < 0 {
        return Err(InvalidRecordError::new(format!(
            "Invalid negative header key size {}",
            header_key_size
        )));
    }

    let header_key = slice_bytes(buffer, pos, header_key_size)?
        .ok_or_else(|| InvalidRecordError::new("Header key cannot be null"))?;

    let (header_value_size, consumed) =
        varint::read_varint(&buffer[*pos..]).map_err(|_| InvalidRecordError::new("Found invalid record structure"))?;
    *pos += consumed;

    let header_value = slice_bytes(buffer, pos, header_value_size)?;

    Ok((header_key, header_value))
}

/// Read up to `buf.len()` bytes from `reader`, returning the number of bytes read.
///
/// Corresponds to Java's `Utils.readFully(InputStream, ByteBuffer)`.
fn read_fully<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<usize, InvalidRecordError> {
    let mut total = 0;
    while total < buf.len() {
        match reader.read(&mut buf[total..]) {
            Ok(0) => break, // EOF
            Ok(n) => total += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(InvalidRecordError::new(e.to_string())),
        }
    }
    Ok(total)
}

/// Increment a sequence number, wrapping around at `i32::MAX`.
///
/// Corresponds to Java's `DefaultRecordBatch.incrementSequence(int, int)`.
pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    if sequence > i32::MAX - increment {
        increment - (i32::MAX - sequence) - 1
    } else {
        sequence + increment
    }
}

impl super::record_trait::Record for DefaultRecord {
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

    fn ensure_valid(&self) -> Result<(), InvalidRecordError> {
        // No validation needed for v2 records
        Ok(())
    }

    fn key_size(&self) -> i32 {
        self.key.as_ref().map_or(-1, |k| k.len() as i32)
    }

    fn has_key(&self) -> bool {
        self.key.is_some()
    }

    fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    fn value_size(&self) -> i32 {
        self.value.as_ref().map_or(-1, |v| v.len() as i32)
    }

    fn has_value(&self) -> bool {
        self.value.is_some()
    }

    fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    fn has_magic(&self, magic: i8) -> bool {
        magic >= RecordBatch::MAGIC_VALUE_V2
    }

    fn is_compressed(&self) -> bool {
        false
    }

    fn has_timestamp_type(&self, _timestamp_type: TimestampType) -> bool {
        false
    }

    fn headers(&self) -> &[RecordHeader] {
        &self.headers
    }
}

impl PartialEq for DefaultRecord {
    fn eq(&self, other: &Self) -> bool {
        self.size_in_bytes == other.size_in_bytes
            && self.attributes == other.attributes
            && self.offset == other.offset
            && self.timestamp == other.timestamp
            && self.sequence == other.sequence
            && self.key == other.key
            && self.value == other.value
            && self.headers == other.headers
    }
}

impl Eq for DefaultRecord {}

impl std::hash::Hash for DefaultRecord {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.size_in_bytes.hash(state);
        self.attributes.hash(state);
        self.offset.hash(state);
        self.timestamp.hash(state);
        self.sequence.hash(state);
        self.key.hash(state);
        self.value.hash(state);
        self.headers.hash(state);
    }
}

impl std::fmt::Display for DefaultRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DefaultRecord(offset={}, timestamp={}, key={} bytes, value={} bytes)",
            self.offset,
            self.timestamp,
            self.key.as_ref().map_or(0, |k| k.len()),
            self.value.as_ref().map_or(0, |v| v.len()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::Record;

    #[test]
    fn test_max_record_overhead() {
        // 5 bytes length + 10 bytes timestamp + 5 bytes offset + 1 byte attributes = 21
        assert_eq!(MAX_RECORD_OVERHEAD, 21);
    }

    #[test]
    fn test_increment_sequence_no_wrap() {
        assert_eq!(increment_sequence(10, 5), 15);
        assert_eq!(increment_sequence(0, 0), 0);
    }

    #[test]
    fn test_increment_sequence_wrap() {
        assert_eq!(increment_sequence(i32::MAX, 1), 0);
        assert_eq!(increment_sequence(i32::MAX - 1, 2), 0);
        assert_eq!(increment_sequence(i32::MAX - 5, 10), 4);
    }

    #[test]
    fn test_basic_serde() {
        let headers = vec![
            RecordHeader::new("foo".to_string(), Some(b"value".to_vec())),
            RecordHeader::new("bar".to_string(), None),
            RecordHeader::new("\"A\\u00ea\\u00f1\\u00fcC\"".to_string(), Some(b"value".to_vec())),
        ];

        let records = vec![
            (Some(b"hi".to_vec()), Some(b"there".to_vec()), Vec::new()),
            (None, Some(b"there".to_vec()), Vec::new()),
            (Some(b"hi".to_vec()), None, Vec::new()),
            (None, None, Vec::new()),
            (Some(b"hi".to_vec()), Some(b"there".to_vec()), headers.clone()),
        ];

        for (key, value, hdrs) in &records {
            let base_sequence = 723;
            let base_offset: i64 = 37;
            let offset_delta: i32 = 10;
            let base_timestamp: i64 = 1000000;
            let timestamp_delta: i64 = 323;

            let mut out = Vec::new();
            DefaultRecord::write_to(&mut out, offset_delta, timestamp_delta, key.as_deref(), value.as_deref(), hdrs)
                .unwrap();

            let (log_record, _) =
                DefaultRecord::read_from_buffer(&out, base_offset, base_timestamp, base_sequence, None).unwrap();

            assert_eq!(base_offset + offset_delta as i64, log_record.offset());
            assert_eq!(base_sequence + offset_delta, log_record.sequence());
            assert_eq!(base_timestamp + timestamp_delta, log_record.timestamp());
            assert_eq!(key.as_deref(), log_record.key());
            assert_eq!(value.as_deref(), log_record.value());
            assert_eq!(hdrs.as_slice(), log_record.headers());
            assert_eq!(
                DefaultRecord::size_in_bytes_with_slices(
                    offset_delta,
                    timestamp_delta,
                    key.as_deref(),
                    value.as_deref(),
                    hdrs,
                ),
                log_record.size_in_bytes()
            );
        }
    }

    #[test]
    fn test_basic_serde_invalid_header_count_too_high() {
        let headers = vec![
            RecordHeader::new("foo".to_string(), Some(b"value".to_vec())),
            RecordHeader::new("bar".to_string(), None),
            RecordHeader::new("\"A\\u00ea\\u00f1\\u00fcC\"".to_string(), Some(b"value".to_vec())),
        ];

        let base_sequence = 723;
        let base_offset: i64 = 37;
        let offset_delta: i32 = 10;
        let base_timestamp: i64 = 1000000;
        let timestamp_delta: i64 = 323;

        let mut out = Vec::new();
        DefaultRecord::write_to(&mut out, offset_delta, timestamp_delta, Some(b"hi"), Some(b"there"), &headers)
            .unwrap();

        // Corrupt the header count byte: set it to 8 (higher than actual 3)
        out[14] = 8;

        // test for stream input
        let mut cursor = std::io::Cursor::new(out.clone());
        let result = DefaultRecord::read_from_stream(&mut cursor, base_offset, base_timestamp, base_sequence, None);
        assert!(result.is_err());

        // test for buffer input
        let result = DefaultRecord::read_from_buffer(&out, base_offset, base_timestamp, base_sequence, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_basic_serde_invalid_header_count_too_low() {
        let headers = vec![
            RecordHeader::new("foo".to_string(), Some(b"value".to_vec())),
            RecordHeader::new("bar".to_string(), None),
            RecordHeader::new("\"A\\u00ea\\u00f1\\u00fcC\"".to_string(), Some(b"value".to_vec())),
        ];

        let base_sequence = 723;
        let base_offset: i64 = 37;
        let offset_delta: i32 = 10;
        let base_timestamp: i64 = 1000000;
        let timestamp_delta: i64 = 323;

        let mut out = Vec::new();
        DefaultRecord::write_to(&mut out, offset_delta, timestamp_delta, Some(b"hi"), Some(b"there"), &headers)
            .unwrap();

        // Corrupt the header count byte: set it to 4 (a varint-encoded value)
        out[14] = 4;

        // test for buffer input
        let result = DefaultRecord::read_from_buffer(&out, base_offset, base_timestamp, base_sequence, None);
        assert!(result.is_err());
    }

    /// Helper to build an invalid record buffer for testing error cases.
    fn build_invalid_record_buf(size_of_body: i32, writer: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut buf = vec![0u8; (size_of_body + varint::size_of_varint(size_of_body)) as usize];
        let mut pos = Vec::new();
        varint::write_varint(size_of_body, &mut pos).unwrap();
        buf[..pos.len()].copy_from_slice(&pos);

        // Use a separate buffer for the body
        let mut body = Vec::new();
        writer(&mut body);

        // Copy body after the varint prefix
        let body_start = pos.len();
        let copy_len = body.len().min(buf.len() - body_start);
        buf[body_start..body_start + copy_len].copy_from_slice(&body[..copy_len]);

        buf
    }

    fn assert_decoding_from_buffer_throws(buf: &[u8]) {
        // test for stream input
        let mut cursor = std::io::Cursor::new(buf.to_vec());
        let result = DefaultRecord::read_from_stream(&mut cursor, 0, 0, RecordBatch::NO_SEQUENCE, None);
        assert!(result.is_err(), "Expected error for stream, got: {:?}", result);

        // test for buffer input
        let result = DefaultRecord::read_from_buffer(buf, 0, 0, RecordBatch::NO_SEQUENCE, None);
        assert!(result.is_err(), "Expected error for buffer, got: {:?}", result);
    }

    #[test]
    fn test_invalid_key_size() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let key_size: i32 = 105; // larger than full message

        let buf = build_invalid_record_buf(size_of_body, |body| {
            body.push(attributes);
            varint::write_varlong(timestamp_delta, body).unwrap();
            varint::write_varint(offset_delta, body).unwrap();
            varint::write_varint(key_size, body).unwrap();
        });

        assert_decoding_from_buffer_throws(&buf);
    }

    #[test]
    fn test_invalid_value_size() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let value_size: i32 = 105;

        let buf = build_invalid_record_buf(size_of_body, |body| {
            body.push(attributes);
            varint::write_varlong(timestamp_delta, body).unwrap();
            varint::write_varint(offset_delta, body).unwrap();
            varint::write_varint(-1, body).unwrap(); // null key
            varint::write_varint(value_size, body).unwrap();
        });

        assert_decoding_from_buffer_throws(&buf);
    }

    #[test]
    fn test_invalid_num_headers() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;

        // negative num headers
        let buf = build_invalid_record_buf(size_of_body, |body| {
            body.push(attributes);
            varint::write_varlong(timestamp_delta, body).unwrap();
            varint::write_varint(offset_delta, body).unwrap();
            varint::write_varint(-1, body).unwrap(); // null key
            varint::write_varint(-1, body).unwrap(); // null value
            varint::write_varint(-1, body).unwrap(); // -1 num.headers, not allowed
        });

        assert_decoding_from_buffer_throws(&buf);

        // num headers larger than remaining buffer
        let buf2 = build_invalid_record_buf(size_of_body, |body| {
            body.push(attributes);
            varint::write_varlong(timestamp_delta, body).unwrap();
            varint::write_varint(offset_delta, body).unwrap();
            varint::write_varint(-1, body).unwrap(); // null key
            varint::write_varint(-1, body).unwrap(); // null value
            varint::write_varint(size_of_body, body).unwrap(); // more headers than remaining
        });

        assert_decoding_from_buffer_throws(&buf2);
    }

    #[test]
    fn test_invalid_header_key() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;

        let buf = build_invalid_record_buf(size_of_body, |body| {
            body.push(attributes);
            varint::write_varlong(timestamp_delta, body).unwrap();
            varint::write_varint(offset_delta, body).unwrap();
            varint::write_varint(-1, body).unwrap(); // null key
            varint::write_varint(-1, body).unwrap(); // null value
            varint::write_varint(1, body).unwrap(); // 1 header
            varint::write_varint(105, body).unwrap(); // header key too long
        });

        assert_decoding_from_buffer_throws(&buf);
    }

    #[test]
    fn test_null_header_key() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;

        let buf = build_invalid_record_buf(size_of_body, |body| {
            body.push(attributes);
            varint::write_varlong(timestamp_delta, body).unwrap();
            varint::write_varint(offset_delta, body).unwrap();
            varint::write_varint(-1, body).unwrap(); // null key
            varint::write_varint(-1, body).unwrap(); // null value
            varint::write_varint(1, body).unwrap(); // 1 header
            varint::write_varint(-1, body).unwrap(); // null header key not allowed
        });

        assert_decoding_from_buffer_throws(&buf);
    }

    #[test]
    fn test_invalid_header_value() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;

        let buf = build_invalid_record_buf(size_of_body, |body| {
            body.push(attributes);
            varint::write_varlong(timestamp_delta, body).unwrap();
            varint::write_varint(offset_delta, body).unwrap();
            varint::write_varint(-1, body).unwrap(); // null key
            varint::write_varint(-1, body).unwrap(); // null value
            varint::write_varint(1, body).unwrap(); // 1 header
            varint::write_varint(1, body).unwrap(); // header key len = 1
            body.push(1); // header key byte
            varint::write_varint(105, body).unwrap(); // header value too long
        });

        assert_decoding_from_buffer_throws(&buf);
    }

    #[test]
    fn test_underflow_reading_timestamp() {
        let attributes: u8 = 0;
        let size_of_body: i32 = 1;
        let mut buf = vec![0u8; (size_of_body + varint::size_of_varint(size_of_body)) as usize];
        let mut prefix = Vec::new();
        varint::write_varint(size_of_body, &mut prefix).unwrap();
        buf[..prefix.len()].copy_from_slice(&prefix);
        buf[prefix.len()] = attributes;

        assert_decoding_from_buffer_throws(&buf);
    }

    #[test]
    fn test_underflow_reading_varlong() {
        let attributes: u8 = 0;
        let size_of_body: i32 = 2; // one byte for attributes, one byte for partial timestamp
        let total_size = (size_of_body + varint::size_of_varint(size_of_body)) as usize;
        let mut buf = vec![0u8; total_size];
        let mut prefix = Vec::new();
        varint::write_varint(size_of_body, &mut prefix).unwrap();
        buf[..prefix.len()].copy_from_slice(&prefix);
        let body_start = prefix.len();
        buf[body_start] = attributes;
        // Write a varlong that needs 2 bytes but only provide 1 byte of space
        let mut varlong_buf = Vec::new();
        varint::write_varlong(156, &mut varlong_buf).unwrap();
        // Only copy the first byte (incomplete varlong)
        if body_start + 1 < buf.len() {
            buf[body_start + 1] = varlong_buf[0];
        }

        assert_decoding_from_buffer_throws(&buf);
    }

    #[test]
    fn test_invalid_varlong() {
        let attributes: u8 = 0;
        let size_of_body: i32 = 11; // one byte for attributes, 10 bytes for max timestamp
        let total_size = (size_of_body + varint::size_of_varint(size_of_body)) as usize;
        let mut buf = vec![0u8; total_size];
        let mut prefix = Vec::new();
        varint::write_varint(size_of_body, &mut prefix).unwrap();
        buf[..prefix.len()].copy_from_slice(&prefix);
        let body_start = prefix.len();
        buf[body_start] = attributes;

        // Write a valid varlong that takes 10 bytes (i64::MAX)
        let mut varlong_buf = Vec::new();
        varint::write_varlong(i64::MAX, &mut varlong_buf).unwrap();
        for (i, &b) in varlong_buf.iter().enumerate() {
            if body_start + 1 + i < buf.len() {
                buf[body_start + 1 + i] = b;
            }
        }
        // Corrupt the last byte of the varlong to make it invalid
        // The 10th byte (index 9) of the varlong should have its MSB set to make it overflow
        buf[body_start + 10] = i8::MIN as u8; // 0x80 - makes the varlong too long

        assert_decoding_from_buffer_throws(&buf);
    }

    #[test]
    fn test_serde_no_sequence() {
        let base_offset: i64 = 37;
        let offset_delta: i32 = 10;
        let base_timestamp: i64 = 1000000;
        let timestamp_delta: i64 = 323;

        let mut out = Vec::new();
        DefaultRecord::write_to(&mut out, offset_delta, timestamp_delta, Some(b"hi"), Some(b"there"), &[]).unwrap();

        // test for stream input
        let mut cursor = std::io::Cursor::new(out.clone());
        let record =
            DefaultRecord::read_from_stream(&mut cursor, base_offset, base_timestamp, RecordBatch::NO_SEQUENCE, None)
                .unwrap();
        assert_eq!(RecordBatch::NO_SEQUENCE, record.sequence());

        // test for buffer input
        let (record, _) =
            DefaultRecord::read_from_buffer(&out, base_offset, base_timestamp, RecordBatch::NO_SEQUENCE, None).unwrap();
        assert_eq!(RecordBatch::NO_SEQUENCE, record.sequence());
    }

    #[test]
    fn test_invalid_size_of_body_in_bytes() {
        // size_of_body = 10 but buffer only has 5 bytes total
        let size_of_body: i32 = 10;
        let mut buf = vec![0u8; 5];
        let mut prefix = Vec::new();
        varint::write_varint(size_of_body, &mut prefix).unwrap();
        buf[..prefix.len()].copy_from_slice(&prefix);

        assert_decoding_from_buffer_throws(&buf);
    }

    #[test]
    fn test_negative_size_of_body() {
        // A negative size_of_body should return InvalidRecordError, not panic.
        let size_of_body: i32 = -1;
        let mut buf = Vec::new();
        varint::write_varint(size_of_body, &mut buf).unwrap();

        assert_decoding_from_buffer_throws(&buf);
    }

    #[test]
    fn test_write_and_read_with_headers() {
        let headers = vec![
            RecordHeader::new("key1".to_string(), Some(b"val1".to_vec())),
            RecordHeader::new("key2".to_string(), None),
        ];

        let offset_delta = 5;
        let timestamp_delta = 100i64;

        let mut out = Vec::new();
        let written_size = DefaultRecord::write_to(
            &mut out,
            offset_delta,
            timestamp_delta,
            Some(b"mykey"),
            Some(b"myvalue"),
            &headers,
        )
        .unwrap();

        assert_eq!(out.len() as i32, written_size);

        let (record, consumed) = DefaultRecord::read_from_buffer(&out, 0, 0, 0, None).unwrap();

        assert_eq!(consumed as i32, written_size);
        assert_eq!(record.offset(), 5);
        assert_eq!(record.timestamp(), 100);
        assert_eq!(record.key(), Some(b"mykey".as_slice()));
        assert_eq!(record.value(), Some(b"myvalue".as_slice()));
        assert_eq!(record.headers().len(), 2);
        assert_eq!(record.headers()[0].key(), "key1");
        assert_eq!(record.headers()[1].key(), "key2");
    }

    #[test]
    fn test_size_calculation() {
        let headers = vec![RecordHeader::new("h".to_string(), Some(b"v".to_vec()))];

        let offset_delta = 10;
        let timestamp_delta: i64 = 500;

        // Write and check that size_in_bytes matches actual written bytes
        let mut out = Vec::new();
        let written =
            DefaultRecord::write_to(&mut out, offset_delta, timestamp_delta, Some(b"key"), Some(b"value"), &headers)
                .unwrap();

        let computed = DefaultRecord::size_in_bytes_with_slices(
            offset_delta,
            timestamp_delta,
            Some(b"key"),
            Some(b"value"),
            &headers,
        );

        assert_eq!(written, computed);
        assert_eq!(out.len() as i32, computed);
    }

    #[test]
    fn test_display() {
        let record =
            DefaultRecord::new(42, 0, 100, 1000, 5, Some(b"key".to_vec()), Some(b"value".to_vec()), Vec::new());
        let display = format!("{}", record);
        assert!(display.contains("DefaultRecord"));
        assert!(display.contains("offset=100"));
        assert!(display.contains("timestamp=1000"));
    }

    /// §27 zero-copy proof: the key/value slices returned by
    /// `read_ref_from_buffer` point INTO the source buffer — no copy is
    /// made. We assert this by comparing the slice's address range to the
    /// source buffer's address range.
    #[test]
    fn test_read_ref_is_zero_copy() {
        let mut out = Vec::new();
        DefaultRecord::write_to(&mut out, 5, 100, Some(b"mykey"), Some(b"myvalue"), &[]).unwrap();

        let (record_ref, consumed) = DefaultRecord::read_ref_from_buffer(&out, 0, 0, 0, None).unwrap();
        assert_eq!(consumed, out.len());
        assert_eq!(record_ref.offset(), 5);
        assert_eq!(record_ref.timestamp(), 100);
        assert_eq!(record_ref.key(), Some(b"mykey".as_slice()));
        assert_eq!(record_ref.value(), Some(b"myvalue".as_slice()));

        let buf_start = out.as_ptr() as usize;
        let buf_end = buf_start + out.len();
        for slice in [record_ref.key().unwrap(), record_ref.value().unwrap()] {
            let slice_start = slice.as_ptr() as usize;
            assert!(
                slice_start >= buf_start && slice_start + slice.len() <= buf_end,
                "borrowed slice must point into the source buffer (zero-copy)"
            );
        }
    }

    /// The borrowing `read_ref_from_buffer` and the owned `read_from_buffer`
    /// must decode to equivalent records (the owned path is implemented in
    /// terms of the borrowed one).
    #[test]
    fn test_read_ref_matches_owned() {
        let headers = vec![
            RecordHeader::new("key1".to_string(), Some(b"val1".to_vec())),
            RecordHeader::new("key2".to_string(), None),
        ];
        let mut out = Vec::new();
        DefaultRecord::write_to(&mut out, 3, 42, Some(b"k"), Some(b"v"), &headers).unwrap();

        let (owned, _) = DefaultRecord::read_from_buffer(&out, 10, 1000, 7, None).unwrap();
        let (record_ref, _) = DefaultRecord::read_ref_from_buffer(&out, 10, 1000, 7, None).unwrap();

        assert_eq!(owned.offset(), record_ref.offset());
        assert_eq!(owned.timestamp(), record_ref.timestamp());
        assert_eq!(owned.sequence(), record_ref.sequence());
        assert_eq!(owned.size_in_bytes(), record_ref.size_in_bytes());
        assert_eq!(owned.key(), record_ref.key());
        assert_eq!(owned.value(), record_ref.value());
        assert_eq!(owned.headers(), record_ref.headers().unwrap().as_slice());
    }

    #[test]
    fn test_record_trait_methods() {
        let record = DefaultRecord::new(42, 0, 100, 1000, 5, Some(b"key".to_vec()), None, Vec::new());

        assert!(record.has_key());
        assert!(!record.has_value());
        assert_eq!(record.key_size(), 3);
        assert_eq!(record.value_size(), -1);
        assert!(record.has_magic(2));
        assert!(record.has_magic(3));
        assert!(!record.has_magic(1));
        assert!(!record.is_compressed());
        assert!(!record.has_timestamp_type(TimestampType::CreateTime));
        assert!(record.ensure_valid().is_ok());
    }
}
