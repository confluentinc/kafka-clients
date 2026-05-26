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

//! Translation of `org.apache.kafka.common.record.DefaultRecord`.
//!
//! Implements the inner record format for magic 2 and above:
//!
//! ```text
//! Record =>
//!   Length         => Varint
//!   Attributes     => Int8
//!   TimestampDelta => Varlong
//!   OffsetDelta    => Varint
//!   KeyLength      => Varint
//!   Key            => Bytes
//!   ValueLength    => Varint
//!   Value          => Bytes
//!   HeadersCount   => Varint
//!   Headers        => [HeaderKey HeaderValue]
//!     HeaderKeyLength   => Varint
//!     HeaderKey         => String (UTF-8)
//!     HeaderValueLength => Varint
//!     HeaderValue       => Bytes
//! ```
//!
//! The offset and timestamp deltas compute the difference relative to the base
//! offset and base timestamp of the batch this record is contained in.

use std::fmt;
use std::io::{self, Read, Write};

use bytes::Bytes;

use crate::common::errors::KafkaError;
use crate::common::header::{Header, RecordHeader};
use crate::common::record::Record;
use crate::common::record::TimestampType;
use crate::common::record::default_record_batch::increment_sequence;
use crate::common::record::record_batch::{MAGIC_VALUE_V2, NO_SEQUENCE};
use crate::common::utils::byte_utils;

/// Maximum overhead bytes (excluding key, value and headers): 5 bytes length +
/// 10 bytes timestamp + 5 bytes offset + 1 byte attributes. Mirrors Java's
/// `DefaultRecord.MAX_RECORD_OVERHEAD`.
pub const MAX_RECORD_OVERHEAD: i32 = 21;

/// Number of bytes used to represent a "null" length-prefixed field as a
/// signed (zig-zag) varint of `-1`. Mirrors Java's `NULL_VARINT_SIZE_BYTES`.
const NULL_VARINT_SIZE_BYTES: i32 = 1;

/// `org.apache.kafka.common.record.DefaultRecord`.
///
/// Records carry borrowed `Bytes` views into the source buffer for the key
/// and value (zero-copy: see CLAUDE.md rule 12). The record's `headers` are
/// `Vec<RecordHeader>` whose internal buffers are themselves `Arc<[u8]>`
/// (cheap clones, see [`RecordHeader`]).
#[derive(Clone)]
pub struct DefaultRecord {
    size_in_bytes: i32,
    attributes: i8,
    offset: i64,
    timestamp: i64,
    sequence: i32,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<RecordHeader>,
}

impl DefaultRecord {
    /// Construct a `DefaultRecord` directly. Mirrors Java's package-private
    /// constructor; kept `pub(crate)` because user code generally goes
    /// through [`read_from_buffer`] or [`read_from_stream`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        size_in_bytes: i32,
        attributes: i8,
        offset: i64,
        timestamp: i64,
        sequence: i32,
        key: Option<Bytes>,
        value: Option<Bytes>,
        headers: Vec<RecordHeader>,
    ) -> Self {
        DefaultRecord { size_in_bytes, attributes, offset, timestamp, sequence, key, value, headers }
    }

    /// Mirrors `attributes()` (no `Record` trait method in Java).
    pub fn attributes(&self) -> i8 {
        self.attributes
    }
}

impl Record for DefaultRecord {
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
        Ok(()) // Java is a no-op; CRC lives on the batch.
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
        magic >= MAGIC_VALUE_V2
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

impl fmt::Debug for DefaultRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DefaultRecord(offset={}, timestamp={}, key={} bytes, value={} bytes)",
            self.offset,
            self.timestamp,
            self.key.as_ref().map_or(0, Bytes::len),
            self.value.as_ref().map_or(0, Bytes::len),
        )
    }
}

impl fmt::Display for DefaultRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

// ---------------------------------------------------------------------------
// Static helpers (free functions per CLAUDE.md rule 2 — exported only by the
// defining file).
// ---------------------------------------------------------------------------

/// Write `(offset_delta, timestamp_delta, key, value, headers)` to `out` in
/// the magic-2 record format and return the total size in bytes (length-prefix
/// + body). Mirrors Java's `DefaultRecord.writeTo(DataOutputStream, ...)`.
///
/// Per CLAUDE.md rule 12 (zero-copy), `key`/`value` are borrowed `&[u8]`
/// slices: the bytes are copied directly into `out` without an intermediate
/// allocation. Phase 3d-4 (`MemoryRecordsBuilder`) will pass a `&mut Vec<u8>`
/// pointing at the batch buffer, completing the zero-copy path from
/// serializer → batch.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] when a header key is a null
/// pointer; in Rust the type system prevents that, but we keep the contract
/// for symmetry with Java.
pub fn write_to(
    out: &mut Vec<u8>,
    offset_delta: i32,
    timestamp_delta: i64,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[RecordHeader],
) -> Result<i32, KafkaError> {
    let body_size = size_of_body_in_bytes_from_buffers(offset_delta, timestamp_delta, key, value, headers);
    byte_utils::write_varint(body_size, out);

    let attributes: i8 = 0; // No attributes are currently used.
    out.push(attributes as u8);

    byte_utils::write_varlong(timestamp_delta, out);
    byte_utils::write_varint(offset_delta, out);

    match key {
        None => byte_utils::write_varint(-1, out),
        Some(k) => {
            byte_utils::write_varint(k.len() as i32, out);
            out.extend_from_slice(k);
        },
    }

    match value {
        None => byte_utils::write_varint(-1, out),
        Some(v) => {
            byte_utils::write_varint(v.len() as i32, out);
            out.extend_from_slice(v);
        },
    }

    byte_utils::write_varint(headers.len() as i32, out);

    for header in headers {
        let utf8 = header.key().as_bytes();
        byte_utils::write_varint(utf8.len() as i32, out);
        out.extend_from_slice(utf8);

        match header.value() {
            None => byte_utils::write_varint(-1, out),
            Some(hv) => {
                byte_utils::write_varint(hv.len() as i32, out);
                out.extend_from_slice(hv);
            },
        }
    }

    Ok(byte_utils::size_of_varint(body_size) as i32 + body_size)
}

/// Write a record to a stream-style `&mut dyn Write`. Mirrors the Java
/// `DefaultRecord.writeTo(DataOutputStream, ...)` overload used by
/// `MemoryRecordsBuilder` when an output codec wraps the underlying buffer.
///
/// For the no-compression path callers should still prefer [`write_to`] with
/// `&mut Vec<u8>` (zero-copy into the batch buffer; see CLAUDE.md rule 12).
/// This stream variant exists for the compressed path where the codec
/// ([`crate::common::record::CompressionType::wrap_for_output`]) returns a
/// `Box<dyn Write>` that is not a `Vec<u8>`.
///
/// Returns the total wire size in bytes (length-prefix varint + body).
pub fn write_to_stream<W: Write + ?Sized>(
    out: &mut W,
    offset_delta: i32,
    timestamp_delta: i64,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[RecordHeader],
) -> Result<i32, KafkaError> {
    let body_size = size_of_body_in_bytes_from_buffers(offset_delta, timestamp_delta, key, value, headers);
    byte_utils::write_varint_to_stream(body_size, out).map_err(map_io_err)?;

    let attributes: i8 = 0;
    out.write_all(&[attributes as u8]).map_err(map_io_err)?;

    byte_utils::write_varlong_to_stream(timestamp_delta, out).map_err(map_io_err)?;
    byte_utils::write_varint_to_stream(offset_delta, out).map_err(map_io_err)?;

    match key {
        None => byte_utils::write_varint_to_stream(-1, out).map_err(map_io_err)?,
        Some(k) => {
            byte_utils::write_varint_to_stream(k.len() as i32, out).map_err(map_io_err)?;
            out.write_all(k).map_err(map_io_err)?;
        },
    }

    match value {
        None => byte_utils::write_varint_to_stream(-1, out).map_err(map_io_err)?,
        Some(v) => {
            byte_utils::write_varint_to_stream(v.len() as i32, out).map_err(map_io_err)?;
            out.write_all(v).map_err(map_io_err)?;
        },
    }

    byte_utils::write_varint_to_stream(headers.len() as i32, out).map_err(map_io_err)?;

    for header in headers {
        let utf8 = header.key().as_bytes();
        byte_utils::write_varint_to_stream(utf8.len() as i32, out).map_err(map_io_err)?;
        out.write_all(utf8).map_err(map_io_err)?;

        match header.value() {
            None => byte_utils::write_varint_to_stream(-1, out).map_err(map_io_err)?,
            Some(hv) => {
                byte_utils::write_varint_to_stream(hv.len() as i32, out).map_err(map_io_err)?;
                out.write_all(hv).map_err(map_io_err)?;
            },
        }
    }

    Ok(byte_utils::size_of_varint(body_size) as i32 + body_size)
}

fn map_io_err(e: io::Error) -> KafkaError {
    // Java surfaces I/O exceptions from `DataOutputStream.write*` as
    // `KafkaException("I/O exception when writing to the append stream, closing", e)`.
    // We map to `KafkaError::Generic` with the same wording.
    KafkaError::Generic(format!("I/O exception when writing to the append stream, closing: {e}"))
}

/// Read a record from `buffer`, returning a `DefaultRecord` whose key/value
/// slices alias the source buffer. Mirrors Java's
/// `DefaultRecord.readFrom(ByteBuffer, long, long, int, Long)`.
///
/// `buffer` is consumed from the front: on success the consumed prefix is
/// removed from the input. The returned record's `key`/`value` `Bytes`
/// payloads share storage with the original `buffer` — no per-record
/// payload copy.
///
/// # Errors
///
/// Returns [`KafkaError::InvalidRecord`] for any of:
/// * truncated input or malformed varint;
/// * `numHeaders < 0` or larger than the remaining buffer;
/// * the consumed-bytes count does not equal the declared body length.
pub fn read_from_buffer(
    buffer: &mut Bytes,
    base_offset: i64,
    base_timestamp: i64,
    base_sequence: i32,
    log_append_time: Option<i64>,
) -> Result<DefaultRecord, KafkaError> {
    let size_of_body = read_varint_from_bytes(buffer)?;
    read_from_buffer_inner(
        buffer,
        size_of_body,
        base_offset,
        base_timestamp,
        base_sequence,
        log_append_time,
    )
}

/// Read a record from `input`, mirroring Java's
/// `DefaultRecord.readFrom(InputStream, long, long, int, Long)`.
///
/// The stream variant necessarily allocates a body-sized scratch buffer
/// (Java does the same via `ByteBuffer.allocate(sizeOfBodyInBytes)`). For the
/// zero-copy fast path use [`read_from_buffer`].
pub fn read_from_stream<R: Read>(
    input: &mut R,
    base_offset: i64,
    base_timestamp: i64,
    base_sequence: i32,
    log_append_time: Option<i64>,
) -> Result<DefaultRecord, KafkaError> {
    let size_of_body = byte_utils::read_varint_from_stream(input)?;
    if size_of_body < 0 {
        return Err(invalid_record_struct());
    }
    let mut record_buf = vec![0u8; size_of_body as usize];
    let read = read_fully(input, &mut record_buf)?;
    if read != size_of_body as usize {
        return Err(KafkaError::InvalidRecord(format!(
            "Invalid record size: expected {} bytes in record payload, but the record payload reached EOF.",
            size_of_body
        )));
    }
    let mut bytes = Bytes::from(record_buf);
    read_from_buffer_inner(
        &mut bytes,
        size_of_body,
        base_offset,
        base_timestamp,
        base_sequence,
        log_append_time,
    )
}

fn read_from_buffer_inner(
    buffer: &mut Bytes,
    size_of_body: i32,
    base_offset: i64,
    base_timestamp: i64,
    base_sequence: i32,
    log_append_time: Option<i64>,
) -> Result<DefaultRecord, KafkaError> {
    if size_of_body < 0 {
        return Err(invalid_record_struct());
    }
    let body_size = size_of_body as usize;
    if buffer.len() < body_size {
        return Err(KafkaError::InvalidRecord(format!(
            "Invalid record size: expected {} bytes in record payload, but instead the buffer has only {} remaining bytes.",
            size_of_body,
            buffer.len()
        )));
    }
    // Slice off the body and operate on it in isolation so we can verify the
    // exact-size invariant the Java code expresses via `position - recordStart`.
    let mut body = buffer.split_to(body_size);
    let body_start_len = body.len();

    let attributes = read_byte_from_bytes(&mut body)? as i8;
    let timestamp_delta = read_varlong_from_bytes(&mut body)?;
    let mut timestamp = base_timestamp.wrapping_add(timestamp_delta);
    if let Some(lat) = log_append_time {
        timestamp = lat;
    }

    let offset_delta = read_varint_from_bytes(&mut body)?;
    let offset = base_offset.wrapping_add(offset_delta as i64);
    let sequence = if base_sequence >= 0 {
        increment_sequence(base_sequence, offset_delta)
    } else {
        NO_SEQUENCE
    };

    // Key
    let key_size = read_varint_from_bytes(&mut body)?;
    let key = read_bytes_field(&mut body, key_size)?;

    // Value
    let value_size = read_varint_from_bytes(&mut body)?;
    let value = read_bytes_field(&mut body, value_size)?;

    let num_headers = read_varint_from_bytes(&mut body)?;
    if num_headers < 0 {
        return Err(KafkaError::InvalidRecord(format!(
            "Found invalid number of record headers {}",
            num_headers
        )));
    }
    if num_headers as usize > body.len() {
        return Err(KafkaError::InvalidRecord(format!(
            "Found invalid number of record headers. {} is larger than the remaining size of the buffer",
            num_headers
        )));
    }

    let headers = if num_headers == 0 {
        Vec::new()
    } else {
        read_headers(&mut body, num_headers)?
    };

    // Validate that we consumed exactly the declared body bytes.
    let consumed = body_start_len - body.len();
    if consumed != body_size {
        return Err(KafkaError::InvalidRecord(format!(
            "Invalid record size: expected to read {} bytes in record payload, but instead read {}",
            size_of_body, consumed
        )));
    }

    let total_size = byte_utils::size_of_varint(size_of_body) as i32 + size_of_body;
    Ok(DefaultRecord::new(
        total_size, attributes, offset, timestamp, sequence, key, value, headers,
    ))
}

fn read_headers(body: &mut Bytes, num_headers: i32) -> Result<Vec<RecordHeader>, KafkaError> {
    let mut headers = Vec::with_capacity(num_headers as usize);
    for _ in 0..num_headers {
        let header_key_size = read_varint_from_bytes(body)?;
        if header_key_size < 0 {
            return Err(KafkaError::InvalidRecord(format!(
                "Invalid negative header key size {}",
                header_key_size
            )));
        }
        let header_key_bytes = read_bytes_field(body, header_key_size)?.ok_or_else(|| {
            KafkaError::InvalidRecord(format!("Invalid negative header key size {}", header_key_size))
        })?;
        let header_value_size = read_varint_from_bytes(body)?;
        let header_value = read_bytes_field(body, header_value_size)?;

        // RecordHeader::from_bytes performs the lossy UTF-8 decode that
        // matches Java's `Utils.utf8(ByteBuffer)` semantics.
        let header = RecordHeader::from_bytes(&header_key_bytes, header_value.as_deref());
        headers.push(header);
    }
    Ok(headers)
}

/// Total wire size (length-prefix varint + body) of a record. Mirrors Java's
/// `DefaultRecord.sizeInBytes(int, long, ByteBuffer, ByteBuffer, Header[])`.
pub fn size_in_bytes(
    offset_delta: i32,
    timestamp_delta: i64,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[RecordHeader],
) -> i32 {
    let body = size_of_body_in_bytes_from_buffers(offset_delta, timestamp_delta, key, value, headers);
    body + byte_utils::size_of_varint(body) as i32
}

/// Total wire size given pre-computed key/value sizes (use `-1` for null).
/// Mirrors Java's `sizeInBytes(int, long, int, int, Header[])`.
pub fn size_in_bytes_with_sizes(
    offset_delta: i32,
    timestamp_delta: i64,
    key_size: i32,
    value_size: i32,
    headers: &[RecordHeader],
) -> i32 {
    let body = size_of_body_in_bytes(offset_delta, timestamp_delta, key_size, value_size, headers);
    body + byte_utils::size_of_varint(body) as i32
}

/// Body size given pre-computed key/value sizes. Mirrors Java's
/// `sizeOfBodyInBytes(int, long, int, int, Header[])`.
pub fn size_of_body_in_bytes(
    offset_delta: i32,
    timestamp_delta: i64,
    key_size: i32,
    value_size: i32,
    headers: &[RecordHeader],
) -> i32 {
    let mut size = 1i32; // attributes
    size += byte_utils::size_of_varint(offset_delta) as i32;
    size += byte_utils::size_of_varlong(timestamp_delta) as i32;
    size += size_of(key_size, value_size, headers);
    size
}

/// Upper bound on the on-wire size of a record carrying `key`, `value` and
/// `headers`. Mirrors Java's package-private `recordSizeUpperBound`. Used by
/// `DefaultRecordBatch::estimate_batch_size_upper_bound`, which is in turn
/// used by `MemoryRecordsBuilder::has_room_for`.
#[allow(dead_code)]
pub(crate) fn record_size_upper_bound(key: Option<&[u8]>, value: Option<&[u8]>, headers: &[RecordHeader]) -> i32 {
    let key_size = key.map_or(-1, |k| k.len() as i32);
    let value_size = value.map_or(-1, |v| v.len() as i32);
    MAX_RECORD_OVERHEAD + size_of(key_size, value_size, headers)
}

fn size_of_body_in_bytes_from_buffers(
    offset_delta: i32,
    timestamp_delta: i64,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[RecordHeader],
) -> i32 {
    let key_size = key.map_or(-1, |k| k.len() as i32);
    let value_size = value.map_or(-1, |v| v.len() as i32);
    size_of_body_in_bytes(offset_delta, timestamp_delta, key_size, value_size, headers)
}

fn size_of(key_size: i32, value_size: i32, headers: &[RecordHeader]) -> i32 {
    let mut size = 0i32;
    if key_size < 0 {
        size += NULL_VARINT_SIZE_BYTES;
    } else {
        size += byte_utils::size_of_varint(key_size) as i32 + key_size;
    }

    if value_size < 0 {
        size += NULL_VARINT_SIZE_BYTES;
    } else {
        size += byte_utils::size_of_varint(value_size) as i32 + value_size;
    }

    size += byte_utils::size_of_varint(headers.len() as i32) as i32;
    for header in headers {
        let header_key_size = header.key().len() as i32;
        size += byte_utils::size_of_varint(header_key_size) as i32 + header_key_size;
        match header.value() {
            None => size += NULL_VARINT_SIZE_BYTES,
            Some(hv) => size += byte_utils::size_of_varint(hv.len() as i32) as i32 + hv.len() as i32,
        }
    }
    size
}

// ---------------------------------------------------------------------------
// Bytes-cursor varint/byte readers. The runtime `Readable` trait was designed
// for the broker-protocol layer (typed primitives over a pre-bounded slice).
// Records use a lower-level shape — varint-prefixed length fields with
// validated-on-read sizes — so we work directly against `&mut Bytes` here.
// ---------------------------------------------------------------------------

fn read_byte_from_bytes(buf: &mut Bytes) -> Result<u8, KafkaError> {
    if buf.is_empty() {
        return Err(invalid_record_struct());
    }
    let b = buf[0];
    let _ = buf.split_to(1);
    Ok(b)
}

fn read_varint_from_bytes(buf: &mut Bytes) -> Result<i32, KafkaError> {
    let (value, consumed) = byte_utils::read_varint(buf).map_err(map_varint_err)?;
    let _ = buf.split_to(consumed);
    Ok(value)
}

fn read_varlong_from_bytes(buf: &mut Bytes) -> Result<i64, KafkaError> {
    let (value, consumed) = byte_utils::read_varlong(buf).map_err(map_varint_err)?;
    let _ = buf.split_to(consumed);
    Ok(value)
}

/// Read a length-prefixed bytes field (key, value, header value): `-1`
/// returns `None`; otherwise we slice `len` bytes off `body`. Slicing is
/// `Bytes::split_to` which is zero-copy (refcount bump).
fn read_bytes_field(body: &mut Bytes, size: i32) -> Result<Option<Bytes>, KafkaError> {
    if size < 0 {
        Ok(None)
    } else if size as usize > body.len() {
        Err(invalid_record_struct())
    } else {
        Ok(Some(body.split_to(size as usize)))
    }
}

fn invalid_record_struct() -> KafkaError {
    KafkaError::InvalidRecord("Found invalid record structure".to_owned())
}

fn map_varint_err(e: KafkaError) -> KafkaError {
    // Java wraps `BufferUnderflowException | IllegalArgumentException` from a
    // varint read in `InvalidRecordException("Found invalid record structure", e)`.
    let _ = e;
    invalid_record_struct()
}

/// Read up to `buf.len()` bytes into `buf`, looping over short reads. Returns
/// the number of bytes actually read. Mirrors the producer-side use of
/// `Utils.readFully` (Java's helper for `InputStream`).
fn read_fully<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<usize, KafkaError> {
    let mut total = 0;
    while total < buf.len() {
        match reader.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(KafkaError::InvalidRecord(format!("I/O error reading record: {e}"))),
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    // Translation of `org.apache.kafka.common.record.DefaultRecordTest`.
    //
    // The Java test suite uses a random base timestamp (`System.currentTimeMillis()`).
    // We pin the timestamp in the test bodies so the byte-level fixture test
    // can use a stable hex literal; the round-trip tests are timestamp-agnostic.

    use super::*;
    use crate::common::header::RecordHeader;

    fn h(key: &str, value: Option<&[u8]>) -> RecordHeader {
        RecordHeader::new(key, value)
    }

    /// Mirrors Java's `SimpleRecord` test helper used in `testBasicSerde`.
    /// Java's `SimpleRecord` carries a per-record timestamp but
    /// `testBasicSerde` writes via `(offset_delta, timestamp_delta)` and
    /// drives the per-record timestamp via the batch's `base_timestamp`, so
    /// the per-record field is unused at the call site.
    struct TestRecord {
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<RecordHeader>,
    }

    impl TestRecord {
        fn key_slice(&self) -> Option<&[u8]> {
            self.key.as_deref()
        }
        fn value_slice(&self) -> Option<&[u8]> {
            self.value.as_deref()
        }
    }

    /// Java: `testBasicSerde`.
    #[test]
    fn basic_serde() {
        let header_set = vec![
            h("foo", Some(b"value")),
            h("bar", None),
            h("\"A\\u00ea\\u00f1\\u00fcC\"", Some(b"value")),
        ];

        let records = vec![
            TestRecord { key: Some(b"hi".to_vec()), value: Some(b"there".to_vec()), headers: vec![] },
            TestRecord { key: None, value: Some(b"there".to_vec()), headers: vec![] },
            TestRecord { key: Some(b"hi".to_vec()), value: None, headers: vec![] },
            TestRecord { key: None, value: None, headers: vec![] },
            TestRecord {
                key: Some(b"hi".to_vec()),
                value: Some(b"there".to_vec()),
                headers: header_set.clone(),
            },
        ];

        for record in &records {
            let base_sequence: i32 = 723;
            let base_offset: i64 = 37;
            let offset_delta: i32 = 10;
            let base_timestamp: i64 = 1_700_000_000_000;
            let timestamp_delta: i64 = 323;

            let mut out: Vec<u8> = Vec::with_capacity(1024);
            let written = write_to(
                &mut out,
                offset_delta,
                timestamp_delta,
                record.key_slice(),
                record.value_slice(),
                &record.headers,
            )
            .unwrap();
            assert_eq!(written as usize, out.len(), "writeTo's return matches written byte count");

            let mut buffer = Bytes::from(out);
            let log_record = read_from_buffer(&mut buffer, base_offset, base_timestamp, base_sequence, None).unwrap();

            assert_eq!(log_record.offset(), base_offset + offset_delta as i64);
            assert_eq!(log_record.sequence(), base_sequence + offset_delta);
            assert_eq!(log_record.timestamp(), base_timestamp + timestamp_delta);
            assert_eq!(log_record.key(), record.key_slice());
            assert_eq!(log_record.value(), record.value_slice());
            assert_eq!(log_record.headers(), record.headers.as_slice());
            assert_eq!(
                log_record.size_in_bytes(),
                size_in_bytes(
                    offset_delta,
                    timestamp_delta,
                    record.key_slice(),
                    record.value_slice(),
                    &record.headers
                )
            );
        }
    }

    /// Java: `testBasicSerdeInvalidHeaderCountTooHigh`.
    #[test]
    fn basic_serde_invalid_header_count_too_high() {
        let headers = vec![
            h("foo", Some(b"value")),
            h("bar", None),
            h("\"A\\u00ea\\u00f1\\u00fcC\"", Some(b"value")),
        ];

        let mut out: Vec<u8> = Vec::with_capacity(1024);
        write_to(&mut out, 10, 323, Some(b"hi"), Some(b"there"), &headers).unwrap();
        // Index 14 in the body — same as Java's `buffer.put(14, (byte) 8)`.
        out[14] = 8;
        let buffer_for_stream = out.clone();

        // InputStream variant
        let mut cursor = std::io::Cursor::new(buffer_for_stream.as_slice());
        let err = read_from_stream(&mut cursor, 37, 1_700_000_000_000, 723, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));

        // Buffer variant
        let mut buffer = Bytes::from(out);
        let err = read_from_buffer(&mut buffer, 37, 1_700_000_000_000, 723, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
    }

    /// Java: `testBasicSerdeInvalidHeaderCountTooLow`.
    #[test]
    fn basic_serde_invalid_header_count_too_low() {
        let headers = vec![
            h("foo", Some(b"value")),
            h("bar", None),
            h("\"A\\u00ea\\u00f1\\u00fcC\"", Some(b"value")),
        ];

        let mut out: Vec<u8> = Vec::with_capacity(1024);
        write_to(&mut out, 10, 323, Some(b"hi"), Some(b"there"), &headers).unwrap();
        out[14] = 4;
        let mut buffer = Bytes::from(out);
        let err = read_from_buffer(&mut buffer, 37, 1_700_000_000_000, 723, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
    }

    /// Java: `testInvalidKeySize`.
    #[test]
    fn invalid_key_size() {
        let buf = build_invalid_buffer_with_oversized_key();
        assert_decoding_throws_invalid(&buf);
    }

    /// Java: `testInvalidValueSize`.
    #[test]
    fn invalid_value_size() {
        let buf = build_invalid_buffer_with_oversized_value();
        assert_decoding_throws_invalid(&buf);
    }

    /// Java: `testInvalidNumHeaders`.
    #[test]
    fn invalid_num_headers() {
        // -1 num.headers
        let buf = build_invalid_buffer_with_num_headers(-1);
        assert_decoding_throws_invalid(&buf);
        // num.headers larger than remaining buffer
        let buf2 = build_invalid_buffer_with_num_headers(100);
        assert_decoding_throws_invalid(&buf2);
    }

    /// Java: `testInvalidHeaderKey`.
    #[test]
    fn invalid_header_key_size_too_long() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let mut buf: Vec<u8> = Vec::with_capacity(size_of_body as usize + byte_utils::size_of_varint(size_of_body));
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(attributes);
        byte_utils::write_varlong(timestamp_delta, &mut buf);
        byte_utils::write_varint(offset_delta, &mut buf);
        byte_utils::write_varint(-1, &mut buf); // null key
        byte_utils::write_varint(-1, &mut buf); // null value
        byte_utils::write_varint(1, &mut buf); // 1 header
        byte_utils::write_varint(105, &mut buf); // header key too long
        // pad to the declared body length
        let prefix_len = byte_utils::size_of_varint(size_of_body);
        while buf.len() < prefix_len + size_of_body as usize {
            buf.push(0);
        }
        assert_decoding_throws_invalid(&buf);
    }

    /// Java: `testNullHeaderKey`.
    #[test]
    fn null_header_key_rejected() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let mut buf: Vec<u8> = Vec::with_capacity(size_of_body as usize + byte_utils::size_of_varint(size_of_body));
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(attributes);
        byte_utils::write_varlong(timestamp_delta, &mut buf);
        byte_utils::write_varint(offset_delta, &mut buf);
        byte_utils::write_varint(-1, &mut buf);
        byte_utils::write_varint(-1, &mut buf);
        byte_utils::write_varint(1, &mut buf); // 1 header
        byte_utils::write_varint(-1, &mut buf); // null header key not allowed
        let prefix_len = byte_utils::size_of_varint(size_of_body);
        while buf.len() < prefix_len + size_of_body as usize {
            buf.push(0);
        }
        assert_decoding_throws_invalid(&buf);
    }

    /// Java: `testInvalidHeaderValue`.
    #[test]
    fn invalid_header_value_size_too_long() {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let mut buf: Vec<u8> = Vec::with_capacity(size_of_body as usize + byte_utils::size_of_varint(size_of_body));
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(attributes);
        byte_utils::write_varlong(timestamp_delta, &mut buf);
        byte_utils::write_varint(offset_delta, &mut buf);
        byte_utils::write_varint(-1, &mut buf);
        byte_utils::write_varint(-1, &mut buf);
        byte_utils::write_varint(1, &mut buf);
        byte_utils::write_varint(1, &mut buf); // header key size
        buf.push(1); // one byte header key
        byte_utils::write_varint(105, &mut buf); // header value too long
        let prefix_len = byte_utils::size_of_varint(size_of_body);
        while buf.len() < prefix_len + size_of_body as usize {
            buf.push(0);
        }
        assert_decoding_throws_invalid(&buf);
    }

    /// Java: `testUnderflowReadingTimestamp`.
    #[test]
    fn underflow_reading_timestamp() {
        let size_of_body: i32 = 1;
        let mut buf: Vec<u8> = Vec::with_capacity(size_of_body as usize + byte_utils::size_of_varint(size_of_body));
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(0); // attributes
        // No timestamp varlong byte written → underflow.
        assert_decoding_throws_invalid(&buf);
    }

    /// Java: `testUnderflowReadingVarlong`.
    #[test]
    fn underflow_reading_varlong() {
        let size_of_body: i32 = 2; // one byte for attributes, one byte for partial timestamp
        // We need to allocate enough for the varint prefix + size_of_body + 1
        // (Java did `+ 1`).
        let mut buf: Vec<u8> = Vec::with_capacity(size_of_body as usize + byte_utils::size_of_varint(size_of_body) + 1);
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(0);
        // Write a varlong of 156 (which needs 2 bytes), but truncate to only the
        // first byte.
        let mut varlong = Vec::new();
        byte_utils::write_varlong(156, &mut varlong);
        assert!(varlong.len() >= 2, "varlong(156) must take at least 2 bytes");
        buf.push(varlong[0]); // only the first byte
        assert_decoding_throws_invalid(&buf);
    }

    /// Java: `testInvalidVarlong`. Writes a max varlong with the final byte
    /// corrupted to set the high bit (so the varlong decoder errors).
    #[test]
    fn invalid_varlong() {
        let size_of_body: i32 = 11; // 1 byte attributes + 10 bytes timestamp
        let mut buf: Vec<u8> = Vec::with_capacity(size_of_body as usize + byte_utils::size_of_varint(size_of_body) + 1);
        byte_utils::write_varint(size_of_body, &mut buf);
        let prefix_len = byte_utils::size_of_varint(size_of_body);
        buf.push(0);
        byte_utils::write_varlong(i64::MAX, &mut buf);
        // Overwrite the final varlong byte to be invalid (high bit + 0).
        buf[prefix_len + 10] = i8::MIN as u8;
        assert_decoding_throws_invalid(&buf);
    }

    /// Java: `testSerdeNoSequence`. Confirms `NO_SEQUENCE` propagates when
    /// `base_sequence < 0`.
    #[test]
    fn serde_no_sequence() {
        let key = b"hi";
        let value = b"there";
        let base_offset: i64 = 37;
        let offset_delta: i32 = 10;
        let base_timestamp: i64 = 1_700_000_000_000;
        let timestamp_delta: i64 = 323;

        let mut out: Vec<u8> = Vec::with_capacity(1024);
        write_to(&mut out, offset_delta, timestamp_delta, Some(key), Some(value), &[]).unwrap();

        // Stream
        let mut cursor = std::io::Cursor::new(out.as_slice());
        let record = read_from_stream(&mut cursor, base_offset, base_timestamp, NO_SEQUENCE, None).unwrap();
        assert_eq!(record.sequence(), NO_SEQUENCE);

        // Buffer
        let mut buffer = Bytes::from(out);
        let record = read_from_buffer(&mut buffer, base_offset, base_timestamp, NO_SEQUENCE, None).unwrap();
        assert_eq!(record.sequence(), NO_SEQUENCE);
    }

    /// Java: `testInvalidSizeOfBodyInBytes`. Encodes only the size-prefix and
    /// checks that the reader fails (insufficient bytes for the body).
    #[test]
    fn invalid_size_of_body_in_bytes() {
        let size_of_body: i32 = 10;
        let mut buf: Vec<u8> = Vec::with_capacity(5);
        byte_utils::write_varint(size_of_body, &mut buf);
        // No body bytes present.
        let mut cursor = std::io::Cursor::new(buf.as_slice());
        let err = read_from_stream(&mut cursor, 0, 0, NO_SEQUENCE, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
        let mut buffer = Bytes::from(buf);
        let err = read_from_buffer(&mut buffer, 0, 0, NO_SEQUENCE, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
    }

    /// Required by PLAN.md DoD: assert byte-level encoding against a known
    /// vector. Builds a record with all fields fixed and asserts every emitted
    /// byte. Hand-computed from the v2 record format spec.
    ///
    /// Record fields:
    ///   offset_delta      = 1
    ///   timestamp_delta   = 2
    ///   key               = "k" (1 byte)
    ///   value             = "v" (1 byte)
    ///   headers           = [("h", "vh")]
    ///
    /// Body layout:
    ///   attributes      0x00              (1 byte)
    ///   tsDelta         varlong(2) = 04   (1 byte)
    ///   offsetDelta     varint(1)  = 02   (1 byte)
    ///   keySize         varint(1)  = 02   (1 byte)
    ///   key             "k"              (1 byte)
    ///   valueSize       varint(1)  = 02   (1 byte)
    ///   value           "v"              (1 byte)
    ///   numHeaders      varint(1)  = 02   (1 byte)
    ///   hdrKeySize      varint(1)  = 02   (1 byte)
    ///   hdrKey          "h"              (1 byte)
    ///   hdrValueSize    varint(2)  = 04   (1 byte)
    ///   hdrValue        "vh"             (2 bytes)
    /// Total body = 13 bytes
    /// Length prefix = varint(13) = 0x1A (1 byte)
    /// Total record  = 14 bytes
    #[test]
    fn byte_level_fixture() {
        let header = h("h", Some(b"vh"));
        let mut out: Vec<u8> = Vec::new();
        let written = write_to(&mut out, 1, 2, Some(b"k"), Some(b"v"), std::slice::from_ref(&header)).unwrap();

        let expected: &[u8] = &[
            0x1A, // length prefix: zigzag(13)=26
            0x00, // attributes
            0x04, // tsDelta zigzag(2)=4
            0x02, // offsetDelta zigzag(1)=2
            0x02, // keySize zigzag(1)=2
            b'k', 0x02, // valueSize zigzag(1)=2
            b'v', 0x02, // numHeaders zigzag(1)=2
            0x02, // hdrKeySize zigzag(1)=2
            b'h', 0x04, // hdrValueSize zigzag(2)=4
            b'v', b'h',
        ];

        assert_eq!(out.as_slice(), expected, "byte-level encoding must match spec");
        assert_eq!(written as usize, expected.len());

        // Round-trip the same bytes back and confirm fields.
        let mut buffer = Bytes::from(out);
        let record = read_from_buffer(&mut buffer, 100, 1000, 50, None).unwrap();
        assert_eq!(record.offset(), 101);
        assert_eq!(record.timestamp(), 1002);
        assert_eq!(record.sequence(), 51);
        assert_eq!(record.key(), Some(b"k".as_slice()));
        assert_eq!(record.value(), Some(b"v".as_slice()));
        assert_eq!(record.headers().len(), 1);
        assert_eq!(record.headers()[0], header);
    }

    /// CLAUDE.md rule 12 (zero-copy on the read path): a record returned by
    /// `read_from_buffer` must alias the source buffer for both key and value.
    #[test]
    fn read_from_buffer_is_zero_copy() {
        let mut out: Vec<u8> = Vec::new();
        write_to(&mut out, 1, 2, Some(b"keykey"), Some(b"valval"), &[]).unwrap();

        let bytes = Bytes::from(out);
        let p_in = bytes.as_ptr();
        let p_in_len = bytes.len();

        // Take the original Bytes and pass through `read_from_buffer`. The
        // returned record's key/value slices must point inside the same
        // underlying buffer as the source `bytes`.
        let mut buffer = bytes;
        let record = read_from_buffer(&mut buffer, 100, 1000, 50, None).unwrap();

        let key_p = record.key().unwrap().as_ptr() as usize;
        let value_p = record.value().unwrap().as_ptr() as usize;
        let buf_start = p_in as usize;
        let buf_end = buf_start + p_in_len;

        assert!(
            key_p >= buf_start && key_p < buf_end,
            "key slice must alias source buffer (no copy)"
        );
        assert!(
            value_p >= buf_start && value_p < buf_end,
            "value slice must alias source buffer (no copy)"
        );
    }

    /// Round-trips a record with a Unicode-tinged header key to confirm the
    /// `Utils.utf8` lossy decode path on the read side.
    #[test]
    fn header_key_unicode_round_trip() {
        let header = h("\"A\u{00ea}\u{00f1}\u{00fc}C\"", Some(b"value"));
        let mut out: Vec<u8> = Vec::new();
        write_to(&mut out, 0, 0, None, None, std::slice::from_ref(&header)).unwrap();
        let mut buffer = Bytes::from(out);
        let record = read_from_buffer(&mut buffer, 0, 0, NO_SEQUENCE, None).unwrap();
        assert_eq!(record.headers().len(), 1);
        assert_eq!(record.headers()[0].key(), header.key());
        assert_eq!(record.headers()[0].value(), header.value());
    }

    /// `record_size_upper_bound` includes overhead and the variable parts.
    #[test]
    fn record_size_upper_bound_matches_size_of_plus_overhead() {
        let key = b"key";
        let value = b"value";
        let header = h("h", Some(b"v"));
        let bound = record_size_upper_bound(Some(key), Some(value), std::slice::from_ref(&header));
        let actual = size_in_bytes(0, 0, Some(key), Some(value), std::slice::from_ref(&header));
        // The upper bound must always be >= the actual size for any specific
        // (offset, timestamp) delta pair (since varint widths can be smaller
        // than the maximum).
        assert!(bound >= actual, "upper bound ({}) must be >= actual ({})", bound, actual);
    }

    /// `size_in_bytes_with_sizes` matches `size_in_bytes` for the same key/value
    /// payloads.
    #[test]
    fn size_in_bytes_with_sizes_matches() {
        let key = b"key";
        let value = b"value";
        let header = h("h", Some(b"v"));
        let s1 = size_in_bytes(7, 13, Some(key), Some(value), std::slice::from_ref(&header));
        let s2 = size_in_bytes_with_sizes(7, 13, key.len() as i32, value.len() as i32, std::slice::from_ref(&header));
        assert_eq!(s1, s2);

        // Null key + value path
        let s3 = size_in_bytes(7, 13, None, None, &[]);
        let s4 = size_in_bytes_with_sizes(7, 13, -1, -1, &[]);
        assert_eq!(s3, s4);
    }

    // `increment_sequence` test moved to `default_record_batch.rs` in
    // Phase 3d-2 (the function lives there now).

    /// Sanity: `attributes()` always returns 0 for a record we constructed.
    #[test]
    fn attributes_is_zero() {
        let mut out: Vec<u8> = Vec::new();
        write_to(&mut out, 1, 2, Some(b"k"), Some(b"v"), &[]).unwrap();
        let mut buffer = Bytes::from(out);
        let record = read_from_buffer(&mut buffer, 0, 0, 0, None).unwrap();
        assert_eq!(record.attributes(), 0);
    }

    // -----------------------------------------------------------------------
    // Helpers building the malformed-buffer fixtures for the *Invalid* tests
    // — these mirror the Java helpers `assertDecodingRecordFromBufferThrows…`.
    // -----------------------------------------------------------------------

    fn assert_decoding_throws_invalid(buf: &[u8]) {
        // Stream input
        let mut cursor = std::io::Cursor::new(buf);
        let err = read_from_stream(&mut cursor, 0, 0, NO_SEQUENCE, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)), "stream variant must error");

        // Buffer input
        let mut buffer = Bytes::copy_from_slice(buf);
        let err = read_from_buffer(&mut buffer, 0, 0, NO_SEQUENCE, None).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)), "buffer variant must error");
    }

    fn build_invalid_buffer_with_oversized_key() -> Vec<u8> {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let key_size: i32 = 105; // larger than the body
        let mut buf: Vec<u8> = Vec::with_capacity(size_of_body as usize + byte_utils::size_of_varint(size_of_body));
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(attributes);
        byte_utils::write_varlong(timestamp_delta, &mut buf);
        byte_utils::write_varint(offset_delta, &mut buf);
        byte_utils::write_varint(key_size, &mut buf);
        let prefix_len = byte_utils::size_of_varint(size_of_body);
        while buf.len() < prefix_len + size_of_body as usize {
            buf.push(0);
        }
        buf
    }

    fn build_invalid_buffer_with_oversized_value() -> Vec<u8> {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let value_size: i32 = 105;
        let mut buf: Vec<u8> = Vec::with_capacity(size_of_body as usize + byte_utils::size_of_varint(size_of_body));
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(attributes);
        byte_utils::write_varlong(timestamp_delta, &mut buf);
        byte_utils::write_varint(offset_delta, &mut buf);
        byte_utils::write_varint(-1, &mut buf); // null key
        byte_utils::write_varint(value_size, &mut buf);
        let prefix_len = byte_utils::size_of_varint(size_of_body);
        while buf.len() < prefix_len + size_of_body as usize {
            buf.push(0);
        }
        buf
    }

    fn build_invalid_buffer_with_num_headers(num_headers: i32) -> Vec<u8> {
        let attributes: u8 = 0;
        let timestamp_delta: i64 = 2;
        let offset_delta: i32 = 1;
        let size_of_body: i32 = 100;
        let mut buf: Vec<u8> = Vec::with_capacity(size_of_body as usize + byte_utils::size_of_varint(size_of_body));
        byte_utils::write_varint(size_of_body, &mut buf);
        buf.push(attributes);
        byte_utils::write_varlong(timestamp_delta, &mut buf);
        byte_utils::write_varint(offset_delta, &mut buf);
        byte_utils::write_varint(-1, &mut buf); // null key
        byte_utils::write_varint(-1, &mut buf); // null value
        byte_utils::write_varint(num_headers, &mut buf);
        let prefix_len = byte_utils::size_of_varint(size_of_body);
        while buf.len() < prefix_len + size_of_body as usize {
            buf.push(0);
        }
        buf
    }
}
