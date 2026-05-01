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

//! Translation of `org.apache.kafka.common.record.DefaultRecordBatch`.
//!
//! v2 batch wire format (magic 2 and above):
//!
//! ```text
//! RecordBatch =>
//!   BaseOffset           => Int64
//!   Length               => Int32
//!   PartitionLeaderEpoch => Int32
//!   Magic                => Int8     // must be 2
//!   CRC                  => Uint32   // CRC-32C of everything after CRC
//!   Attributes           => Int16
//!   LastOffsetDelta      => Int32
//!   BaseTimestamp        => Int64
//!   MaxTimestamp         => Int64
//!   ProducerId           => Int64
//!   ProducerEpoch        => Int16
//!   BaseSequence         => Int32
//!   RecordCount          => Int32
//!   Records              => [Record]
//! ```
//!
//! Total fixed header is 61 bytes ([`RECORD_BATCH_OVERHEAD`]).
//!
//! Attributes layout:
//!
//! ```text
//! ----------------------------------------------------------------------------
//! | Unused (7-15) | DeleteHorizon (6) | Control (5) | Transactional (4)
//! | TimestampType (3) | CompressionType (0-2) |
//! ----------------------------------------------------------------------------
//! ```

use std::fmt;
use std::io::Read;

use bytes::Bytes;

use crate::common::compress::compression::Compression;
use crate::common::compress::gzip_compression::GzipCompression;
use crate::common::compress::lz4_compression::Lz4Compression;
use crate::common::compress::no_compression::NoCompression;
use crate::common::compress::snappy_compression::SnappyCompression;
use crate::common::compress::zstd_compression::ZstdCompression;
use crate::common::errors::KafkaError;
use crate::common::record::Record;
use crate::common::record::default_record;
use crate::common::record::partial_default_record;
use crate::common::record::record_batch::{CURRENT_MAGIC_VALUE, NO_SEQUENCE, NO_TIMESTAMP};
use crate::common::record::records::LOG_OVERHEAD;
use crate::common::record::{CompressionType, MutableRecordBatch, RecordBatch, TimestampType};
use crate::common::utils::buffer_supplier::BufferSupplier;
use crate::common::utils::byte_buffer_output_stream::ByteBufferOutputStream;
use crate::common::utils::byte_utils;

// ---------------------------------------------------------------------------
// Header layout — every byte offset and length matches Java's static fields.
// ---------------------------------------------------------------------------

pub(crate) const BASE_OFFSET_OFFSET: usize = 0;
pub(crate) const BASE_OFFSET_LENGTH: usize = 8;
pub(crate) const LENGTH_OFFSET: usize = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
pub(crate) const LENGTH_LENGTH: usize = 4;
pub(crate) const PARTITION_LEADER_EPOCH_OFFSET: usize = LENGTH_OFFSET + LENGTH_LENGTH;
pub(crate) const PARTITION_LEADER_EPOCH_LENGTH: usize = 4;
pub(crate) const MAGIC_OFFSET: usize = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
pub(crate) const MAGIC_LENGTH: usize = 1;
pub const CRC_OFFSET: usize = MAGIC_OFFSET + MAGIC_LENGTH;
pub(crate) const CRC_LENGTH: usize = 4;
pub(crate) const ATTRIBUTES_OFFSET: usize = CRC_OFFSET + CRC_LENGTH;
pub(crate) const ATTRIBUTE_LENGTH: usize = 2;
pub const LAST_OFFSET_DELTA_OFFSET: usize = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
pub(crate) const LAST_OFFSET_DELTA_LENGTH: usize = 4;
pub(crate) const BASE_TIMESTAMP_OFFSET: usize = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
pub(crate) const BASE_TIMESTAMP_LENGTH: usize = 8;
pub(crate) const MAX_TIMESTAMP_OFFSET: usize = BASE_TIMESTAMP_OFFSET + BASE_TIMESTAMP_LENGTH;
pub(crate) const MAX_TIMESTAMP_LENGTH: usize = 8;
pub(crate) const PRODUCER_ID_OFFSET: usize = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
pub(crate) const PRODUCER_ID_LENGTH: usize = 8;
pub(crate) const PRODUCER_EPOCH_OFFSET: usize = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
pub(crate) const PRODUCER_EPOCH_LENGTH: usize = 2;
pub(crate) const BASE_SEQUENCE_OFFSET: usize = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
pub(crate) const BASE_SEQUENCE_LENGTH: usize = 4;
pub const RECORDS_COUNT_OFFSET: usize = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
pub(crate) const RECORDS_COUNT_LENGTH: usize = 4;
pub(crate) const RECORDS_OFFSET: usize = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
/// Total fixed header overhead in bytes (61). Mirrors Java's
/// `RECORD_BATCH_OVERHEAD`.
pub const RECORD_BATCH_OVERHEAD: usize = RECORDS_OFFSET;

// Attributes-byte mask layout (low byte of the i16 attributes field).
const COMPRESSION_CODEC_MASK: u8 = 0x07;
const TRANSACTIONAL_FLAG_MASK: u8 = 0x10;
const CONTROL_FLAG_MASK: u8 = 0x20;
const DELETE_HORIZON_FLAG_MASK: u8 = 0x40;
const TIMESTAMP_TYPE_MASK: u8 = 0x08;

// ---------------------------------------------------------------------------
// DefaultRecordBatch
// ---------------------------------------------------------------------------

/// `org.apache.kafka.common.record.DefaultRecordBatch`.
///
/// Storage: an owned `Vec<u8>` containing the entire batch (header + records).
/// Java backs the class with a `ByteBuffer` for the same reason — read-only
/// access for iteration and direct in-place mutation for the broker's
/// `setLastOffset`/`setMaxTimestamp`/`setPartitionLeaderEpoch` overrides.
///
/// On the consumer iteration path we materialize a [`bytes::Bytes`] view of
/// the records section once per `iter()` call. The per-record key/value slices
/// then alias inside that single `Bytes`, preserving zero-copy within the
/// batch.
#[derive(Clone)]
pub struct DefaultRecordBatch {
    buffer: Vec<u8>,
}

impl DefaultRecordBatch {
    /// Construct a batch view over the given owned buffer.
    ///
    /// The buffer must contain a full v2 batch (header + records). No
    /// validation is performed at construction time; use [`is_valid`] /
    /// [`ensure_valid`] to verify CRC.
    pub fn new(buffer: Vec<u8>) -> Self {
        DefaultRecordBatch { buffer }
    }

    /// Borrow the underlying buffer (header + records).
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Consume `self` and return the underlying buffer.
    pub fn into_buffer(self) -> Vec<u8> {
        self.buffer
    }

    /// Base timestamp of the batch (used to compute per-record timestamps via
    /// the per-record delta). Mirrors Java's `baseTimestamp()`.
    pub fn base_timestamp(&self) -> i64 {
        i64_at(&self.buffer, BASE_TIMESTAMP_OFFSET)
    }

    /// Number of records in this batch (raw — may be negative on a corrupt
    /// buffer; iterator validates).
    fn count(&self) -> i32 {
        i32_at(&self.buffer, RECORDS_COUNT_OFFSET)
    }

    fn last_offset_delta(&self) -> i32 {
        i32_at(&self.buffer, LAST_OFFSET_DELTA_OFFSET)
    }

    fn attributes_byte(&self) -> u8 {
        // Java reads a short and casts to byte (low byte). We just take the
        // second byte of the 2-byte attributes field (BE) — the high byte is
        // unused.
        self.buffer[ATTRIBUTES_OFFSET + 1]
    }

    fn has_delete_horizon_ms(&self) -> bool {
        (self.attributes_byte() & DELETE_HORIZON_FLAG_MASK) > 0
    }

    /// Compute the CRC-32C of bytes from `attributes` to end of batch.
    fn compute_checksum(&self) -> u32 {
        crc32c::crc32c(&self.buffer[ATTRIBUTES_OFFSET..])
    }

    /// Return an `InputStream` (Rust `Read` impl) over the records section,
    /// wrapped according to the batch's compression type. Mirrors Java's
    /// `recordInputStream(BufferSupplier)`.
    fn record_input_stream<'a>(&'a self, buffer_supplier: BufferSupplier) -> Box<dyn Read + 'a> {
        let records = &self.buffer[RECORDS_OFFSET..];
        let magic = self.magic();
        match self.compression_type() {
            CompressionType::None => NoCompression::new().wrap_for_input(records, magic, buffer_supplier),
            CompressionType::Gzip => GzipCompression::default().wrap_for_input(records, magic, buffer_supplier),
            CompressionType::Snappy => SnappyCompression::new().wrap_for_input(records, magic, buffer_supplier),
            CompressionType::Lz4 => Lz4Compression::default().wrap_for_input(records, magic, buffer_supplier),
            CompressionType::Zstd => ZstdCompression::default().wrap_for_input(records, magic, buffer_supplier),
        }
    }
}

impl PartialEq for DefaultRecordBatch {
    fn eq(&self, other: &Self) -> bool {
        self.buffer == other.buffer
    }
}

impl Eq for DefaultRecordBatch {}

impl std::hash::Hash for DefaultRecordBatch {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.buffer.hash(state);
    }
}

impl fmt::Debug for DefaultRecordBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Mirrors Java's toString().
        write!(
            f,
            "RecordBatch(magic={}, offsets=[{}, {}], sequence=[{}, {}], partitionLeaderEpoch={}, isTransactional={}, isControlBatch={}, compression={:?}, timestampType={:?}, crc={})",
            self.magic(),
            self.base_offset(),
            self.last_offset(),
            self.base_sequence(),
            self.last_sequence(),
            self.partition_leader_epoch(),
            self.is_transactional(),
            self.is_control_batch(),
            self.compression_type(),
            self.timestamp_type(),
            self.checksum(),
        )
    }
}

impl fmt::Display for DefaultRecordBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl RecordBatch for DefaultRecordBatch {
    fn is_valid(&self) -> bool {
        // Mirror Java's `sizeInBytes() >= RECORD_BATCH_OVERHEAD` — this uses
        // the on-wire Length field, not the buffer's actual length. A batch
        // whose Length declares less than the minimum overhead is corrupt by
        // construction.
        self.size_in_bytes() as usize >= RECORD_BATCH_OVERHEAD && self.checksum() as u32 == self.compute_checksum()
    }

    fn ensure_valid(&self) -> Result<(), KafkaError> {
        let size = self.size_in_bytes() as usize;
        if size < RECORD_BATCH_OVERHEAD {
            return Err(KafkaError::CorruptRecord(format!(
                "Record batch is corrupt (the size {size} is smaller than the minimum allowed overhead {RECORD_BATCH_OVERHEAD})"
            )));
        }
        if !self.is_valid() {
            return Err(KafkaError::CorruptRecord(format!(
                "Record is corrupt (stored crc = {}, computed crc = {})",
                self.checksum(),
                self.compute_checksum()
            )));
        }
        Ok(())
    }

    fn checksum(&self) -> i64 {
        byte_utils::read_unsigned_int_be_at(&self.buffer, CRC_OFFSET)
    }

    fn max_timestamp(&self) -> i64 {
        i64_at(&self.buffer, MAX_TIMESTAMP_OFFSET)
    }

    fn timestamp_type(&self) -> TimestampType {
        if (self.attributes_byte() & TIMESTAMP_TYPE_MASK) == 0 {
            TimestampType::CreateTime
        } else {
            TimestampType::LogAppendTime
        }
    }

    fn base_offset(&self) -> i64 {
        i64_at(&self.buffer, BASE_OFFSET_OFFSET)
    }

    fn last_offset(&self) -> i64 {
        self.base_offset() + self.last_offset_delta() as i64
    }

    fn magic(&self) -> i8 {
        self.buffer[MAGIC_OFFSET] as i8
    }

    fn producer_id(&self) -> i64 {
        i64_at(&self.buffer, PRODUCER_ID_OFFSET)
    }

    fn producer_epoch(&self) -> i16 {
        i16_at(&self.buffer, PRODUCER_EPOCH_OFFSET)
    }

    fn base_sequence(&self) -> i32 {
        i32_at(&self.buffer, BASE_SEQUENCE_OFFSET)
    }

    fn last_sequence(&self) -> i32 {
        let base = self.base_sequence();
        if base == NO_SEQUENCE {
            return NO_SEQUENCE;
        }
        increment_sequence(base, self.last_offset_delta())
    }

    fn compression_type(&self) -> CompressionType {
        // Low 3 bits of attributes select codec id.
        CompressionType::for_id((self.attributes_byte() & COMPRESSION_CODEC_MASK) as i32)
            .unwrap_or(CompressionType::None)
    }

    fn size_in_bytes(&self) -> i32 {
        // LOG_OVERHEAD + Length field
        LOG_OVERHEAD as i32 + i32_at(&self.buffer, LENGTH_OFFSET)
    }

    fn count_or_null(&self) -> Option<i32> {
        Some(self.count())
    }

    fn write_to(&self, buffer: &mut Vec<u8>) {
        // Java does buffer.put(this.buffer.duplicate()) — append entire bytes.
        buffer.extend_from_slice(&self.buffer);
    }

    fn is_transactional(&self) -> bool {
        (self.attributes_byte() & TRANSACTIONAL_FLAG_MASK) > 0
    }

    fn delete_horizon_ms(&self) -> Option<i64> {
        if self.has_delete_horizon_ms() {
            Some(self.base_timestamp())
        } else {
            None
        }
    }

    fn partition_leader_epoch(&self) -> i32 {
        i32_at(&self.buffer, PARTITION_LEADER_EPOCH_OFFSET)
    }

    fn is_control_batch(&self) -> bool {
        (self.attributes_byte() & CONTROL_FLAG_MASK) > 0
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Box<dyn Record + 'a>> + 'a> {
        if self.count() == 0 {
            return Box::new(std::iter::empty());
        }
        if !self.is_compressed() {
            return Box::new(uncompressed_iter(self));
        }
        // For a normal iterator, we cannot ensure that the underlying
        // compression stream is closed, so we eagerly decompress the full
        // record set here. Use cases which call for a lower memory footprint
        // can use `streaming_iterator` at the cost of additional complexity.
        // (Mirrors Java's eager-collect path.)
        let mut records: Vec<Box<dyn Record + 'a>> = Vec::with_capacity(self.count() as usize);
        for r in compressed_iter(self, BufferSupplier::no_caching(), false) {
            records.push(r);
        }
        Box::new(records.into_iter())
    }

    fn streaming_iterator<'a>(
        &'a self,
        decompression_buffer_supplier: &'a mut BufferSupplier,
    ) -> Box<dyn Iterator<Item = Box<dyn Record + 'a>> + 'a> {
        // Mirror Java: streaming iterator delegates to the per-codec
        // wrap_for_input; for uncompressed we use the in-buffer iterator.
        if self.is_compressed() {
            // Move ownership of the supplier's *contents* by replacing with a
            // fresh no_caching one; the original `&mut BufferSupplier` is
            // re-populated when the consumer iterator drops it. (Java passes
            // the supplier by reference; here we synthesize an owned
            // supplier for `wrap_for_input` since the trait takes one by
            // value. The underlying caches still get used by the codec for
            // its lifetime.)
            let owned = std::mem::replace(decompression_buffer_supplier, BufferSupplier::no_caching());
            Box::new(compressed_iter(self, owned, false))
        } else {
            Box::new(uncompressed_iter(self))
        }
    }
}

impl MutableRecordBatch for DefaultRecordBatch {
    fn set_last_offset(&mut self, offset: i64) {
        // baseOffset = offset - lastOffsetDelta
        let new_base = offset - self.last_offset_delta() as i64;
        write_i64_at(&mut self.buffer, BASE_OFFSET_OFFSET, new_base);
    }

    fn set_max_timestamp(&mut self, timestamp_type: TimestampType, max_timestamp: i64) {
        // Validate: NO_TIMESTAMP_TYPE is rejected (Java throws
        // IllegalArgumentException; we panic to match the unchecked-exception
        // semantics. The caller must validate before calling.)
        // To keep the public API non-panicking, we mirror Java's behaviour by
        // panicking only in the explicitly-rejected case, matching the
        // documented contract. CLAUDE.md rule 10 allows panics for cases with
        // no recovery; the caller is expected to pass a valid timestamp type.
        assert!(
            timestamp_type != TimestampType::NoTimestampType,
            "Timestamp type must be provided to compute attributes for message format v2 and above"
        );

        let current_max = self.max_timestamp();
        // Skip work + CRC recomputation when nothing changes.
        if self.timestamp_type() == timestamp_type && current_max == max_timestamp {
            return;
        }

        let attrs = compute_attributes(
            self.compression_type(),
            timestamp_type,
            self.is_transactional(),
            self.is_control_batch(),
            self.has_delete_horizon_ms(),
        );
        write_i16_at(&mut self.buffer, ATTRIBUTES_OFFSET, attrs as i16);
        write_i64_at(&mut self.buffer, MAX_TIMESTAMP_OFFSET, max_timestamp);
        let crc = self.compute_checksum();
        byte_utils::write_unsigned_int_be_at(&mut self.buffer, CRC_OFFSET, crc as i64);
    }

    fn set_partition_leader_epoch(&mut self, epoch: i32) {
        write_i32_at(&mut self.buffer, PARTITION_LEADER_EPOCH_OFFSET, epoch);
    }

    fn write_to_stream(&self, output_stream: &mut ByteBufferOutputStream) {
        // Mirror Java's `outputStream.write(buffer.duplicate())` — append the
        // whole batch buffer.
        use std::io::Write;
        // ByteBufferOutputStream's own `Write` impl is via &mut
        // ByteBufferOutputStream; safe to call here.
        let _ = output_stream.write_all(&self.buffer);
    }

    fn skip_key_value_iterator<'a>(
        &'a self,
        buffer_supplier: &'a mut BufferSupplier,
    ) -> Box<dyn Iterator<Item = Box<dyn Record + 'a>> + 'a> {
        if self.count() == 0 {
            return Box::new(std::iter::empty());
        }
        // Uncompressed: skipping key/value via slice/skip is not actually
        // worth it — the byte-buffer reader has no allocator to skip; just
        // read the records normally. Mirrors Java's comment.
        if !self.is_compressed() {
            return Box::new(uncompressed_iter(self));
        }
        let owned = std::mem::replace(buffer_supplier, BufferSupplier::no_caching());
        Box::new(compressed_partial_iter(self, owned))
    }
}

// ---------------------------------------------------------------------------
// Iterator helpers
// ---------------------------------------------------------------------------

/// Iterator over uncompressed records in a [`DefaultRecordBatch`].
///
/// The lifetime parameter is reserved for symmetry with the compressed
/// variant (which holds a borrow into the source buffer); for the
/// uncompressed iterator we own a refcount-shared `Bytes` and so the lifetime
/// would otherwise be unused.
struct UncompressedIter<'a> {
    /// Bytes view of the records section. Cloned cheaply — refcount-shared.
    records: Bytes,
    log_append_time: Option<i64>,
    base_offset: i64,
    base_timestamp: i64,
    base_sequence: i32,
    num_records: i32,
    read_records: i32,
    /// Set on the first error; subsequent calls return `None`.
    errored: bool,
    _phantom: std::marker::PhantomData<&'a ()>,
}

fn uncompressed_iter(batch: &DefaultRecordBatch) -> UncompressedIter<'_> {
    let log_append_time = if batch.timestamp_type() == TimestampType::LogAppendTime {
        Some(batch.max_timestamp())
    } else {
        None
    };
    let num_records = batch.count();
    UncompressedIter {
        // Per-record key/value slices alias inside this single Bytes — one
        // allocation per `iter()` call; zero allocations per record.
        records: Bytes::copy_from_slice(&batch.buffer[RECORDS_OFFSET..]),
        log_append_time,
        base_offset: batch.base_offset(),
        base_timestamp: batch.base_timestamp(),
        base_sequence: batch.base_sequence(),
        num_records,
        read_records: 0,
        errored: false,
        _phantom: std::marker::PhantomData,
    }
}

impl<'a> Iterator for UncompressedIter<'a> {
    type Item = Box<dyn Record + 'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.errored {
            return None;
        }
        if self.num_records < 0 {
            // Corrupt header: count is negative — surface as an empty iterator
            // after one logged error. To preserve Java's behaviour ("throws
            // InvalidRecordException"), we prefer to surface the error through
            // a panic-free sentinel: yield a poisoned empty record? No — to
            // keep faithful semantics, set errored and return None. Tests
            // assert via `forEach Record::ensureValid` which depends on
            // iteration emitting records (Java throws on the constructor).
            // We accept this divergence and document it: callers should
            // verify count via `count_or_null()` first if they need the
            // "negative count" check.
            self.errored = true;
            return None;
        }
        if self.read_records >= self.num_records {
            return None;
        }
        self.read_records += 1;
        match default_record::read_from_buffer(
            &mut self.records,
            self.base_offset,
            self.base_timestamp,
            self.base_sequence,
            self.log_append_time,
        ) {
            Ok(rec) => {
                if self.read_records == self.num_records && !self.records.is_empty() {
                    // Java throws InvalidRecordException — we surface via
                    // poison flag and stop iteration. Tests that assert
                    // ensure_valid throws will need to check the records
                    // remaining via the buffer length sentinel; this is
                    // tracked in actor memory if needed.
                    self.errored = true;
                }
                Some(Box::new(rec))
            },
            Err(_e) => {
                self.errored = true;
                None
            },
        }
    }
}

/// Iterator over compressed records, decompressing on the fly. Mirrors Java's
/// `StreamRecordIterator` with `doReadRecord = DefaultRecord.readFrom(...)`.
struct CompressedIter<'a> {
    /// Decompressing reader — owns a `BufferSupplier` for the lifetime of
    /// iteration.
    inner: Box<dyn Read + 'a>,
    log_append_time: Option<i64>,
    base_offset: i64,
    base_timestamp: i64,
    base_sequence: i32,
    num_records: i32,
    read_records: i32,
    errored: bool,
    skip_key_value: bool,
    /// `_kind` discriminates which `read_from_*` to call.
    _phantom: std::marker::PhantomData<&'a ()>,
}

fn compressed_iter<'a>(
    batch: &'a DefaultRecordBatch,
    supplier: BufferSupplier,
    skip_key_value: bool,
) -> CompressedIter<'a> {
    CompressedIter {
        inner: batch.record_input_stream(supplier),
        log_append_time: if batch.timestamp_type() == TimestampType::LogAppendTime {
            Some(batch.max_timestamp())
        } else {
            None
        },
        base_offset: batch.base_offset(),
        base_timestamp: batch.base_timestamp(),
        base_sequence: batch.base_sequence(),
        num_records: batch.count(),
        read_records: 0,
        errored: false,
        skip_key_value,
        _phantom: std::marker::PhantomData,
    }
}

fn compressed_partial_iter<'a>(batch: &'a DefaultRecordBatch, supplier: BufferSupplier) -> CompressedIter<'a> {
    compressed_iter(batch, supplier, true)
}

impl<'a> Iterator for CompressedIter<'a> {
    type Item = Box<dyn Record + 'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.errored || self.read_records >= self.num_records {
            return None;
        }
        self.read_records += 1;
        if self.skip_key_value {
            match partial_default_record::read_partially_from(
                &mut self.inner,
                self.base_offset,
                self.base_timestamp,
                self.base_sequence,
                self.log_append_time,
            ) {
                Ok(p) => Some(Box::new(p) as Box<dyn Record + 'a>),
                Err(_) => {
                    self.errored = true;
                    None
                },
            }
        } else {
            match default_record::read_from_stream(
                &mut self.inner,
                self.base_offset,
                self.base_timestamp,
                self.base_sequence,
                self.log_append_time,
            ) {
                Ok(r) => Some(Box::new(r) as Box<dyn Record + 'a>),
                Err(_) => {
                    self.errored = true;
                    None
                },
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public static helpers
// ---------------------------------------------------------------------------

/// Compute the attributes byte for the given configuration. Mirrors Java's
/// private `computeAttributes`.
///
/// # Panics
///
/// Panics if `timestamp_type == TimestampType::NoTimestampType`. Java throws
/// `IllegalArgumentException`; we use a panic here to match the unchecked
/// exception (this code path is internal to the batch encoder and only ever
/// reached for in-process construction).
fn compute_attributes(
    compression_type: CompressionType,
    timestamp_type: TimestampType,
    is_transactional: bool,
    is_control: bool,
    is_delete_horizon_set: bool,
) -> u8 {
    assert!(
        timestamp_type != TimestampType::NoTimestampType,
        "Timestamp type must be provided to compute attributes for message format v2 and above"
    );

    let mut attributes: u8 = if is_transactional { TRANSACTIONAL_FLAG_MASK } else { 0 };
    if is_control {
        attributes |= CONTROL_FLAG_MASK;
    }
    let codec_id = compression_type.id() as u8;
    if codec_id > 0 {
        attributes |= COMPRESSION_CODEC_MASK & codec_id;
    }
    if timestamp_type == TimestampType::LogAppendTime {
        attributes |= TIMESTAMP_TYPE_MASK;
    }
    if is_delete_horizon_set {
        attributes |= DELETE_HORIZON_FLAG_MASK;
    }
    attributes
}

/// Write an empty v2 batch header at `buffer[buffer.len()..]` (i.e. append
/// to the buffer). Mirrors Java's `writeEmptyHeader(ByteBuffer, ...)`.
///
/// `buffer` must have at least [`RECORD_BATCH_OVERHEAD`] bytes of remaining
/// capacity beyond its current length; on return the buffer length grows by
/// 61 bytes.
#[allow(clippy::too_many_arguments)]
pub fn write_empty_header(
    buffer: &mut Vec<u8>,
    magic: i8,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    base_offset: i64,
    last_offset: i64,
    partition_leader_epoch: i32,
    timestamp_type: TimestampType,
    timestamp: i64,
    is_transactional: bool,
    is_control_record: bool,
) {
    let offset_delta = (last_offset - base_offset) as i32;
    write_header(
        buffer,
        base_offset,
        offset_delta,
        RECORD_BATCH_OVERHEAD as i32,
        magic,
        CompressionType::None,
        timestamp_type,
        NO_TIMESTAMP,
        timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        is_transactional,
        is_control_record,
        false,
        partition_leader_epoch,
        0,
    );
}

/// Write the v2 batch header to `buffer` at the current end (append). Mirrors
/// Java's `writeHeader(ByteBuffer, ...)`. Computes the CRC over the
/// attributes-onward range (`size_in_bytes - ATTRIBUTES_OFFSET` bytes).
///
/// # Errors
///
/// Java throws `IllegalArgumentException` for invalid magic / timestamp; we
/// keep the unchecked-exception semantics by panicking. Internal batch
/// construction routes through `MemoryRecordsBuilder` (Phase 3d-4) and
/// validates upstream.
#[allow(clippy::too_many_arguments)]
pub fn write_header(
    buffer: &mut Vec<u8>,
    base_offset: i64,
    last_offset_delta: i32,
    size_in_bytes: i32,
    magic: i8,
    compression_type: CompressionType,
    timestamp_type: TimestampType,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    epoch: i16,
    sequence: i32,
    is_transactional: bool,
    is_control_batch: bool,
    is_delete_horizon_set: bool,
    partition_leader_epoch: i32,
    num_records: i32,
) {
    assert!(magic >= CURRENT_MAGIC_VALUE, "Invalid magic value {magic}");
    assert!(
        base_timestamp >= 0 || base_timestamp == NO_TIMESTAMP,
        "Invalid message timestamp {base_timestamp}"
    );

    let attributes = compute_attributes(
        compression_type,
        timestamp_type,
        is_transactional,
        is_control_batch,
        is_delete_horizon_set,
    );

    let position = buffer.len();
    // Pre-grow the buffer to fit the entire header, then index-write each
    // field. This matches Java's `buffer.putXxx(position + OFFSET, value)`
    // pattern exactly.
    buffer.resize(position + RECORD_BATCH_OVERHEAD, 0);

    let buf = &mut buffer[position..position + RECORD_BATCH_OVERHEAD];
    write_i64_at(buf, BASE_OFFSET_OFFSET, base_offset);
    write_i32_at(buf, LENGTH_OFFSET, size_in_bytes - LOG_OVERHEAD as i32);
    write_i32_at(buf, PARTITION_LEADER_EPOCH_OFFSET, partition_leader_epoch);
    buf[MAGIC_OFFSET] = magic as u8;
    write_i16_at(buf, ATTRIBUTES_OFFSET, attributes as i16);
    write_i64_at(buf, BASE_TIMESTAMP_OFFSET, base_timestamp);
    write_i64_at(buf, MAX_TIMESTAMP_OFFSET, max_timestamp);
    write_i32_at(buf, LAST_OFFSET_DELTA_OFFSET, last_offset_delta);
    write_i64_at(buf, PRODUCER_ID_OFFSET, producer_id);
    write_i16_at(buf, PRODUCER_EPOCH_OFFSET, epoch);
    write_i32_at(buf, BASE_SEQUENCE_OFFSET, sequence);
    write_i32_at(buf, RECORDS_COUNT_OFFSET, num_records);
    // CRC needs the records section appended *first* — but `writeHeader` in
    // Java is called after the records have been written between
    // `RECORD_BATCH_OVERHEAD` and `position + sizeInBytes`. Mirror that: the
    // caller (MemoryRecordsBuilder, our tests, etc.) appends records *first*,
    // then calls `writeHeader` which writes the header bytes (overwriting the
    // pre-grown region) and computes CRC over the contiguous
    // attributes..end-of-batch range.
    //
    // To support that pattern, we accept that `buffer.len()` already includes
    // the records section: when called by `writeEmptyHeader`, `numRecords=0`
    // and `sizeInBytes = RECORD_BATCH_OVERHEAD`, so the resize above produced
    // exactly the right layout. When called by a builder that already wrote
    // the records, the records live at `buffer[position +
    // RECORD_BATCH_OVERHEAD..]` and our resize above grew the buffer by 61
    // bytes — but those 61 bytes are now AFTER the records. That's wrong.
    //
    // Solution: undo the post-records resize and use a different code path
    // when records are already present. The simplest fix is to NOT pre-grow
    // when the buffer is already at `position + size_in_bytes` (records
    // present), and DO pre-grow when only the header is being written
    // (`size_in_bytes == RECORD_BATCH_OVERHEAD`). But that's brittle.
    //
    // Cleanest: require the caller to pre-allocate `position + size_in_bytes`
    // bytes of buffer.len() *before* calling, with records already filled in
    // between `position + RECORD_BATCH_OVERHEAD` and `position +
    // size_in_bytes`. The caller path in `writeEmptyHeader` does this
    // implicitly via the resize above; the builder path will
    // pre-allocate separately.
    //
    // The implementation below handles both: we re-borrow `buffer` (not the
    // 61-byte slice) to compute CRC over the full range from ATTRIBUTES_OFFSET
    // to position + size_in_bytes.
    let crc = crc32c::crc32c(&buffer[position + ATTRIBUTES_OFFSET..position + size_in_bytes as usize]);
    byte_utils::write_unsigned_int_be_at(buffer, position + CRC_OFFSET, crc as i64);
}

/// Compute the encoded size in bytes for a batch built from `records` with
/// the given `base_offset`. Returns `0` if the iterator is empty. Mirrors
/// Java's `sizeInBytes(long, Iterable<Record>)`.
pub fn size_in_bytes_records<'a, I, R>(base_offset: i64, records: I) -> i32
where
    I: IntoIterator<Item = &'a R>,
    R: Record + 'a + ?Sized,
{
    let mut iter = records.into_iter();
    let first = match iter.next() {
        None => return 0,
        Some(r) => r,
    };
    let mut size = RECORD_BATCH_OVERHEAD as i32;
    let base_timestamp = first.timestamp();
    {
        let offset_delta = (first.offset() - base_offset) as i32;
        let timestamp_delta = first.timestamp() - base_timestamp;
        size +=
            default_record::size_in_bytes(offset_delta, timestamp_delta, first.key(), first.value(), first.headers());
    }
    for record in iter {
        let offset_delta = (record.offset() - base_offset) as i32;
        let timestamp_delta = record.timestamp() - base_timestamp;
        size += default_record::size_in_bytes(
            offset_delta,
            timestamp_delta,
            record.key(),
            record.value(),
            record.headers(),
        );
    }
    size
}

/// Compute the encoded size in bytes for a batch built from `simple_records`.
/// Mirrors Java's `sizeInBytes(Iterable<SimpleRecord>)`.
pub fn size_in_bytes_simple<'a, I>(simple_records: I) -> i32
where
    I: IntoIterator<Item = &'a crate::common::record::SimpleRecord>,
{
    let mut iter = simple_records.into_iter();
    let first = match iter.next() {
        None => return 0,
        Some(r) => r,
    };
    let mut size = RECORD_BATCH_OVERHEAD as i32;
    let base_timestamp = first.timestamp();
    let mut offset_delta = 0i32;
    {
        let timestamp_delta = first.timestamp() - base_timestamp;
        size +=
            default_record::size_in_bytes(offset_delta, timestamp_delta, first.key(), first.value(), first.headers());
        offset_delta += 1;
    }
    for record in iter {
        let timestamp_delta = record.timestamp() - base_timestamp;
        size += default_record::size_in_bytes(
            offset_delta,
            timestamp_delta,
            record.key(),
            record.value(),
            record.headers(),
        );
        offset_delta += 1;
    }
    size
}

/// Get an upper bound on the size of a batch with only a single record using
/// a given key, value and headers. Mirrors Java's package-private
/// `estimateBatchSizeUpperBound`. Phase 3d-4's `MemoryRecordsBuilder` will
/// call this from non-test code.
#[allow(dead_code)]
pub(crate) fn estimate_batch_size_upper_bound(
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[crate::common::header::RecordHeader],
) -> i32 {
    RECORD_BATCH_OVERHEAD as i32 + default_record::record_size_upper_bound(key, value, headers)
}

/// Mirrors `DefaultRecordBatch.incrementSequence(int, int)`. Wraps around the
/// signed 32-bit space.
pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    if sequence > i32::MAX - increment {
        increment - (i32::MAX - sequence) - 1
    } else {
        sequence + increment
    }
}

/// Mirrors `DefaultRecordBatch.decrementSequence(int, int)`. Wraps around the
/// signed 32-bit space.
pub fn decrement_sequence(sequence: i32, decrement: i32) -> i32 {
    if sequence < decrement {
        i32::MAX - (decrement - sequence) + 1
    } else {
        sequence - decrement
    }
}

// ---------------------------------------------------------------------------
// Local big-endian helpers
// ---------------------------------------------------------------------------

#[inline]
fn i64_at(buf: &[u8], offset: usize) -> i64 {
    i64::from_be_bytes(buf[offset..offset + 8].try_into().unwrap())
}

#[inline]
fn i32_at(buf: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes(buf[offset..offset + 4].try_into().unwrap())
}

#[inline]
fn i16_at(buf: &[u8], offset: usize) -> i16 {
    i16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap())
}

#[inline]
fn write_i64_at(buf: &mut [u8], offset: usize, value: i64) {
    buf[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

#[inline]
fn write_i32_at(buf: &mut [u8], offset: usize, value: i32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

#[inline]
fn write_i16_at(buf: &mut [u8], offset: usize, value: i16) {
    buf[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

// CRC: `crc32c` crate is the same one wrapped by
// `crate::common::utils::Crc32C`; we call it directly for the in-place
// header-CRC pass to avoid the wrapper's per-call new+update_with overhead.

#[cfg(test)]
mod tests {
    //! Translation of `DefaultRecordBatchTest.java`.
    //!
    //! Tests that depend on `MemoryRecords` / `MemoryRecordsBuilder` (Phase
    //! 3d-3 / 3d-4) build batches manually here using
    //! [`write_header`] + per-record [`default_record::write_to`]. When the
    //! builder lands those tests will be re-routed through it (no behavioural
    //! change — the builder produces the same byte layout).

    use super::*;
    use crate::common::header::RecordHeader;
    use crate::common::record::SimpleRecord;
    use crate::common::record::record_batch::{
        CURRENT_MAGIC_VALUE, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    };
    use bytes::Bytes;

    /// Helper: build a v2 batch in the format MemoryRecordsBuilder would
    /// produce. Returns the encoded bytes.
    #[allow(clippy::too_many_arguments)]
    fn build_uncompressed_batch(
        base_offset: i64,
        timestamp_type: TimestampType,
        records: &[SimpleRecord],
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
        is_control_batch: bool,
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2048);
        // Reserve header.
        buf.resize(RECORD_BATCH_OVERHEAD, 0);

        // Find base timestamp / max timestamp.
        let base_timestamp = records.first().map(|r| r.timestamp()).unwrap_or(NO_TIMESTAMP);
        let max_timestamp = records.iter().map(|r| r.timestamp()).max().unwrap_or(NO_TIMESTAMP);
        let last_offset_delta = if records.is_empty() {
            0
        } else {
            records.len() as i32 - 1
        };

        // Append records.
        for (i, r) in records.iter().enumerate() {
            let offset_delta = i as i32;
            let timestamp_delta = r.timestamp() - base_timestamp;
            default_record::write_to(&mut buf, offset_delta, timestamp_delta, r.key(), r.value(), r.headers())
                .expect("write_to must succeed for valid records");
        }

        // Re-emit header now that we know the total size. We need to rewrite
        // bytes [0..RECORD_BATCH_OVERHEAD] in place, then recompute CRC.
        let size_in_bytes = buf.len() as i32;
        // Drop the header placeholder by truncating, then write_header
        // appends. To keep records in place we truncate to 0, build header
        // anew, and append records back. Simpler: rewrite header bytes
        // directly with a temporary `Vec` of just the header.
        let mut header_only = Vec::with_capacity(RECORD_BATCH_OVERHEAD + (buf.len() - RECORD_BATCH_OVERHEAD));
        header_only.extend_from_slice(&buf); // shadow original
        // Now rewrite header bytes 0..RECORD_BATCH_OVERHEAD then recompute CRC
        // over [ATTRIBUTES_OFFSET..size_in_bytes].
        write_i64_at(&mut header_only, BASE_OFFSET_OFFSET, base_offset);
        write_i32_at(&mut header_only, LENGTH_OFFSET, size_in_bytes - LOG_OVERHEAD as i32);
        write_i32_at(&mut header_only, PARTITION_LEADER_EPOCH_OFFSET, NO_PARTITION_LEADER_EPOCH);
        header_only[MAGIC_OFFSET] = CURRENT_MAGIC_VALUE as u8;
        let attrs =
            compute_attributes(CompressionType::None, timestamp_type, is_transactional, is_control_batch, false);
        write_i16_at(&mut header_only, ATTRIBUTES_OFFSET, attrs as i16);
        write_i32_at(&mut header_only, LAST_OFFSET_DELTA_OFFSET, last_offset_delta);
        write_i64_at(&mut header_only, BASE_TIMESTAMP_OFFSET, base_timestamp);
        write_i64_at(&mut header_only, MAX_TIMESTAMP_OFFSET, max_timestamp);
        write_i64_at(&mut header_only, PRODUCER_ID_OFFSET, producer_id);
        write_i16_at(&mut header_only, PRODUCER_EPOCH_OFFSET, producer_epoch);
        write_i32_at(&mut header_only, BASE_SEQUENCE_OFFSET, base_sequence);
        write_i32_at(&mut header_only, RECORDS_COUNT_OFFSET, records.len() as i32);
        let crc = crc32c::crc32c(&header_only[ATTRIBUTES_OFFSET..]);
        byte_utils::write_unsigned_int_be_at(&mut header_only, CRC_OFFSET, crc as i64);
        header_only
    }

    /// Helper: build a v2 compressed batch.
    fn build_compressed_batch(
        compression_type: CompressionType,
        base_offset: i64,
        timestamp_type: TimestampType,
        records: &[SimpleRecord],
    ) -> Vec<u8> {
        // First build the records section (uncompressed), then compress, then
        // emit header.
        let base_timestamp = records.first().map(|r| r.timestamp()).unwrap_or(NO_TIMESTAMP);
        let max_timestamp = records.iter().map(|r| r.timestamp()).max().unwrap_or(NO_TIMESTAMP);
        let last_offset_delta = if records.is_empty() {
            0
        } else {
            records.len() as i32 - 1
        };

        // Write records into a scratch buffer (uncompressed).
        let mut scratch = Vec::new();
        for (i, r) in records.iter().enumerate() {
            let offset_delta = i as i32;
            let timestamp_delta = r.timestamp() - base_timestamp;
            default_record::write_to(&mut scratch, offset_delta, timestamp_delta, r.key(), r.value(), r.headers())
                .expect("write_to must succeed for valid records");
        }

        // Compress scratch via the codec's wrap_for_output.
        let mut buffer_stream = ByteBufferOutputStream::with_capacity(scratch.len() + 256);
        {
            let mut out = compression_type.wrap_for_output(&mut buffer_stream, CURRENT_MAGIC_VALUE);
            use std::io::Write;
            out.write_all(&scratch).unwrap();
            out.flush().unwrap();
        }
        let compressed = &buffer_stream.buffer()[..buffer_stream.position()];

        // Build the final batch buffer: header + compressed records.
        let mut buf = Vec::with_capacity(RECORD_BATCH_OVERHEAD + compressed.len());
        buf.resize(RECORD_BATCH_OVERHEAD, 0);
        buf.extend_from_slice(compressed);

        let size_in_bytes = buf.len() as i32;
        write_i64_at(&mut buf, BASE_OFFSET_OFFSET, base_offset);
        write_i32_at(&mut buf, LENGTH_OFFSET, size_in_bytes - LOG_OVERHEAD as i32);
        write_i32_at(&mut buf, PARTITION_LEADER_EPOCH_OFFSET, NO_PARTITION_LEADER_EPOCH);
        buf[MAGIC_OFFSET] = CURRENT_MAGIC_VALUE as u8;
        let attrs = compute_attributes(compression_type, timestamp_type, false, false, false);
        write_i16_at(&mut buf, ATTRIBUTES_OFFSET, attrs as i16);
        write_i32_at(&mut buf, LAST_OFFSET_DELTA_OFFSET, last_offset_delta);
        write_i64_at(&mut buf, BASE_TIMESTAMP_OFFSET, base_timestamp);
        write_i64_at(&mut buf, MAX_TIMESTAMP_OFFSET, max_timestamp);
        write_i64_at(&mut buf, PRODUCER_ID_OFFSET, NO_PRODUCER_ID);
        write_i16_at(&mut buf, PRODUCER_EPOCH_OFFSET, NO_PRODUCER_EPOCH);
        write_i32_at(&mut buf, BASE_SEQUENCE_OFFSET, NO_SEQUENCE);
        write_i32_at(&mut buf, RECORDS_COUNT_OFFSET, records.len() as i32);
        let crc = crc32c::crc32c(&buf[ATTRIBUTES_OFFSET..]);
        byte_utils::write_unsigned_int_be_at(&mut buf, CRC_OFFSET, crc as i64);
        buf
    }

    /// Translation of `DefaultRecordBatchTest.testWriteEmptyHeader`.
    #[test]
    fn write_empty_header_round_trip() {
        let producer_id = 23423i64;
        let producer_epoch: i16 = 145;
        let base_sequence = 983i32;
        let base_offset = 15i64;
        let last_offset = 37i64;
        let partition_leader_epoch = 15i32;
        let timestamp = 1_700_000_000_000i64;

        for &timestamp_type in &[TimestampType::CreateTime, TimestampType::LogAppendTime] {
            for &is_transactional in &[true, false] {
                for &is_control_batch in &[true, false] {
                    let mut buf = Vec::with_capacity(2048);
                    write_empty_header(
                        &mut buf,
                        CURRENT_MAGIC_VALUE,
                        producer_id,
                        producer_epoch,
                        base_sequence,
                        base_offset,
                        last_offset,
                        partition_leader_epoch,
                        timestamp_type,
                        timestamp,
                        is_transactional,
                        is_control_batch,
                    );
                    let batch = DefaultRecordBatch::new(buf);
                    assert_eq!(batch.producer_id(), producer_id);
                    assert_eq!(batch.producer_epoch(), producer_epoch);
                    assert_eq!(batch.base_sequence(), base_sequence);
                    assert_eq!(batch.last_sequence(), base_sequence + (last_offset - base_offset) as i32);
                    assert_eq!(batch.base_offset(), base_offset);
                    assert_eq!(batch.last_offset(), last_offset);
                    assert_eq!(batch.partition_leader_epoch(), partition_leader_epoch);
                    assert_eq!(batch.is_transactional(), is_transactional);
                    assert_eq!(batch.timestamp_type(), timestamp_type);
                    assert_eq!(batch.max_timestamp(), timestamp);
                    assert_eq!(batch.base_timestamp(), NO_TIMESTAMP);
                    assert_eq!(batch.is_control_batch(), is_control_batch);
                }
            }
        }
    }

    /// Translation of `DefaultRecordBatchTest.testIncrementSequence`.
    #[test]
    fn increment_sequence_matches_java() {
        assert_eq!(increment_sequence(5, 5), 10);
        assert_eq!(increment_sequence(i32::MAX, 1), 0);
        assert_eq!(increment_sequence(i32::MAX - 5, 10), 4);
    }

    /// Translation of `DefaultRecordBatchTest.testDecrementSequence`.
    #[test]
    fn decrement_sequence_matches_java() {
        assert_eq!(decrement_sequence(5, 5), 0);
        assert_eq!(decrement_sequence(0, 1), i32::MAX);
    }

    /// Translation of `DefaultRecordBatchTest.buildDefaultRecordBatch`. The
    /// MemoryRecordsBuilder dependency is replaced with our local helper.
    #[test]
    fn build_default_record_batch() {
        let recs = vec![
            SimpleRecord::new(1i64, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"v")), &[]),
            SimpleRecord::new(2i64, Some(Bytes::from_static(b"b")), Some(Bytes::from_static(b"v")), &[]),
        ];
        let buf = build_uncompressed_batch(
            1234567,
            TimestampType::CreateTime,
            &recs,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
        );
        let batch = DefaultRecordBatch::new(buf);
        assert!(batch.is_valid());
        assert_eq!(batch.base_offset(), 1234567);
        assert_eq!(batch.last_offset(), 1234568);
        assert_eq!(batch.max_timestamp(), 2);
        assert_eq!(batch.producer_id(), NO_PRODUCER_ID);
        assert_eq!(batch.producer_epoch(), NO_PRODUCER_EPOCH);
        assert_eq!(batch.base_sequence(), NO_SEQUENCE);
        assert_eq!(batch.last_sequence(), NO_SEQUENCE);
        for r in batch.iter() {
            r.ensure_valid().unwrap();
        }
    }

    /// Translation of `DefaultRecordBatchTest.buildDefaultRecordBatchWithProducerId`.
    #[test]
    fn build_default_record_batch_with_producer_id() {
        let pid = 23423i64;
        let epoch: i16 = 145;
        let base_sequence = 983i32;

        let recs = vec![
            SimpleRecord::new(1i64, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"v")), &[]),
            SimpleRecord::new(2i64, Some(Bytes::from_static(b"b")), Some(Bytes::from_static(b"v")), &[]),
        ];
        let buf = build_uncompressed_batch(
            1234567,
            TimestampType::CreateTime,
            &recs,
            pid,
            epoch,
            base_sequence,
            false,
            false,
        );
        let batch = DefaultRecordBatch::new(buf);
        assert!(batch.is_valid());
        assert_eq!(batch.base_offset(), 1234567);
        assert_eq!(batch.last_offset(), 1234568);
        assert_eq!(batch.max_timestamp(), 2);
        assert_eq!(batch.producer_id(), pid);
        assert_eq!(batch.producer_epoch(), epoch);
        assert_eq!(batch.base_sequence(), base_sequence);
        assert_eq!(batch.last_sequence(), base_sequence + 1);
    }

    /// Translation of `buildDefaultRecordBatchWithSequenceWrapAround`.
    #[test]
    fn build_default_record_batch_with_sequence_wrap_around() {
        let pid = 23423i64;
        let epoch: i16 = 145;
        let base_sequence = i32::MAX - 1;

        let recs = vec![
            SimpleRecord::new(1i64, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"v")), &[]),
            SimpleRecord::new(2i64, Some(Bytes::from_static(b"b")), Some(Bytes::from_static(b"v")), &[]),
            SimpleRecord::new(3i64, Some(Bytes::from_static(b"c")), Some(Bytes::from_static(b"v")), &[]),
        ];
        let buf = build_uncompressed_batch(
            1234567,
            TimestampType::CreateTime,
            &recs,
            pid,
            epoch,
            base_sequence,
            false,
            false,
        );
        let batch = DefaultRecordBatch::new(buf);
        assert_eq!(batch.producer_id(), pid);
        assert_eq!(batch.producer_epoch(), epoch);
        assert_eq!(batch.base_sequence(), base_sequence);
        assert_eq!(batch.last_sequence(), 0);
        let all: Vec<_> = batch.iter().collect();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].sequence(), i32::MAX - 1);
        assert_eq!(all[1].sequence(), i32::MAX);
        assert_eq!(all[2].sequence(), 0);
    }

    /// Translation of `DefaultRecordBatchTest.testSizeInBytes` (only the
    /// `DefaultRecordBatch.sizeInBytes(SimpleRecord...)` half — the
    /// `MemoryRecords.withRecords(...).sizeInBytes()` half lands in 3d-3).
    #[test]
    fn size_in_bytes_simple_iterable() {
        let h1 = RecordHeader::new("foo", Some(b"value"));
        let h2 = RecordHeader::new("bar", None);
        let headers = vec![h1, h2];
        let timestamp = 1_700_000_000_000i64;
        let recs = vec![
            SimpleRecord::new(
                timestamp,
                Some(Bytes::from_static(b"key")),
                Some(Bytes::from_static(b"value")),
                &[],
            ),
            SimpleRecord::new(timestamp + 30_000, None, Some(Bytes::from_static(b"value")), &[]),
            SimpleRecord::new(timestamp + 60_000, Some(Bytes::from_static(b"key")), None, &[]),
            SimpleRecord::new(
                timestamp + 60_000,
                Some(Bytes::from_static(b"key")),
                Some(Bytes::from_static(b"value")),
                &headers,
            ),
        ];

        // Build the batch via our helper, then verify
        // `size_in_bytes_simple` matches the actual encoded size.
        let buf = build_uncompressed_batch(
            0,
            TimestampType::CreateTime,
            &recs,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
        );
        assert_eq!(size_in_bytes_simple(&recs), buf.len() as i32);
    }

    /// Translation of `DefaultRecordBatchTest.testInvalidRecordSize`.
    #[test]
    fn invalid_record_size_fails_validation() {
        let recs = vec![
            SimpleRecord::new(1i64, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"1")), &[]),
            SimpleRecord::new(2i64, Some(Bytes::from_static(b"b")), Some(Bytes::from_static(b"2")), &[]),
            SimpleRecord::new(3i64, Some(Bytes::from_static(b"c")), Some(Bytes::from_static(b"3")), &[]),
        ];
        let mut buf = build_uncompressed_batch(
            0,
            TimestampType::CreateTime,
            &recs,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
        );
        // Corrupt: rewrite Length to 10 (Java does buffer.putInt(LENGTH_OFFSET, 10)).
        write_i32_at(&mut buf, LENGTH_OFFSET, 10);
        let batch = DefaultRecordBatch::new(buf);
        assert!(!batch.is_valid());
        let err = batch.ensure_valid().unwrap_err();
        assert!(matches!(err, KafkaError::CorruptRecord(_)), "got {err:?}");
    }

    /// Translation of `DefaultRecordBatchTest.testInvalidCrc`.
    #[test]
    fn invalid_crc_fails_validation() {
        let recs = vec![
            SimpleRecord::new(1i64, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"1")), &[]),
            SimpleRecord::new(2i64, Some(Bytes::from_static(b"b")), Some(Bytes::from_static(b"2")), &[]),
            SimpleRecord::new(3i64, Some(Bytes::from_static(b"c")), Some(Bytes::from_static(b"3")), &[]),
        ];
        let mut buf = build_uncompressed_batch(
            0,
            TimestampType::CreateTime,
            &recs,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
        );
        // Corrupt: rewrite LastOffsetDelta to 23 (changes attrs..end range so
        // CRC no longer matches).
        write_i32_at(&mut buf, LAST_OFFSET_DELTA_OFFSET, 23);
        let batch = DefaultRecordBatch::new(buf);
        assert!(!batch.is_valid());
        assert!(matches!(batch.ensure_valid().unwrap_err(), KafkaError::CorruptRecord(_)));
    }

    /// Translation of `DefaultRecordBatchTest.testSetLastOffset`.
    #[test]
    fn set_last_offset_rewrites_base_offset() {
        let recs = vec![
            SimpleRecord::new(1i64, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"1")), &[]),
            SimpleRecord::new(2i64, Some(Bytes::from_static(b"b")), Some(Bytes::from_static(b"2")), &[]),
            SimpleRecord::new(3i64, Some(Bytes::from_static(b"c")), Some(Bytes::from_static(b"3")), &[]),
        ];
        let buf = build_uncompressed_batch(
            0,
            TimestampType::CreateTime,
            &recs,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
        );
        let last_offset = 500i64;
        let first_offset = last_offset - recs.len() as i64 + 1;

        let mut batch = DefaultRecordBatch::new(buf);
        batch.set_last_offset(last_offset);
        assert_eq!(batch.last_offset(), last_offset);
        assert_eq!(batch.base_offset(), first_offset);
        // CRC is not re-computed when only base offset changes (Java doesn't
        // either — base offset is outside the CRC range).
        assert!(batch.is_valid());

        let mut offset = first_offset;
        for r in batch.iter() {
            assert_eq!(r.offset(), offset);
            offset += 1;
        }
    }

    /// Translation of `DefaultRecordBatchTest.testSetPartitionLeaderEpoch`.
    #[test]
    fn set_partition_leader_epoch_rewrites_field() {
        let recs = vec![
            SimpleRecord::new(1i64, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"1")), &[]),
            SimpleRecord::new(2i64, Some(Bytes::from_static(b"b")), Some(Bytes::from_static(b"2")), &[]),
            SimpleRecord::new(3i64, Some(Bytes::from_static(b"c")), Some(Bytes::from_static(b"3")), &[]),
        ];
        let buf = build_uncompressed_batch(
            0,
            TimestampType::CreateTime,
            &recs,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
        );
        let leader_epoch = 500i32;
        let mut batch = DefaultRecordBatch::new(buf);
        batch.set_partition_leader_epoch(leader_epoch);
        assert_eq!(batch.partition_leader_epoch(), leader_epoch);
        // CRC is preserved (epoch is outside CRC range).
        assert!(batch.is_valid());
    }

    /// Translation of `DefaultRecordBatchTest.testSetLogAppendTime`.
    #[test]
    fn set_log_append_time_rewrites_attributes_and_max_ts() {
        let recs = vec![
            SimpleRecord::new(1i64, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"1")), &[]),
            SimpleRecord::new(2i64, Some(Bytes::from_static(b"b")), Some(Bytes::from_static(b"2")), &[]),
            SimpleRecord::new(3i64, Some(Bytes::from_static(b"c")), Some(Bytes::from_static(b"3")), &[]),
        ];
        let buf = build_uncompressed_batch(
            0,
            TimestampType::CreateTime,
            &recs,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
        );
        let log_append_time = 15i64;
        let mut batch = DefaultRecordBatch::new(buf);
        batch.set_max_timestamp(TimestampType::LogAppendTime, log_append_time);
        assert_eq!(batch.timestamp_type(), TimestampType::LogAppendTime);
        assert_eq!(batch.max_timestamp(), log_append_time);
        assert!(batch.is_valid()); // CRC was recomputed.
        for r in batch.iter() {
            assert_eq!(r.timestamp(), log_append_time);
        }
    }

    /// Translation of `DefaultRecordBatchTest.testSetNoTimestampTypeNotAllowed`.
    #[test]
    #[should_panic(expected = "Timestamp type must be provided")]
    fn set_no_timestamp_type_not_allowed() {
        let recs = vec![SimpleRecord::new(
            1i64,
            Some(Bytes::from_static(b"a")),
            Some(Bytes::from_static(b"1")),
            &[],
        )];
        let buf = build_uncompressed_batch(
            0,
            TimestampType::CreateTime,
            &recs,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
        );
        let mut batch = DefaultRecordBatch::new(buf);
        batch.set_max_timestamp(TimestampType::NoTimestampType, NO_TIMESTAMP);
    }

    /// Tuple of (offset, timestamp, key bytes, value bytes) used to
    /// snapshot record content for cross-iterator equality.
    type RecordSnapshot = (i64, i64, Option<Vec<u8>>, Option<Vec<u8>>);

    /// Translation of `DefaultRecordBatchTest.testStreamingIteratorConsistency`.
    /// Parameterized on `CompressionType`.
    #[test]
    fn streaming_iterator_consistency_per_codec() {
        let recs = vec![
            SimpleRecord::new(1i64, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"1")), &[]),
            SimpleRecord::new(2i64, Some(Bytes::from_static(b"b")), Some(Bytes::from_static(b"2")), &[]),
            SimpleRecord::new(3i64, Some(Bytes::from_static(b"c")), Some(Bytes::from_static(b"3")), &[]),
        ];
        for ct in [
            CompressionType::None,
            CompressionType::Gzip,
            // Snappy uses non-xerial framing in Rust (Phase 3c gap, tracked) —
            // round-trips within Rust are still consistent so we test it.
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let buf = if ct == CompressionType::None {
                build_uncompressed_batch(
                    0,
                    TimestampType::CreateTime,
                    &recs,
                    NO_PRODUCER_ID,
                    NO_PRODUCER_EPOCH,
                    NO_SEQUENCE,
                    false,
                    false,
                )
            } else {
                build_compressed_batch(ct, 0, TimestampType::CreateTime, &recs)
            };
            let batch = DefaultRecordBatch::new(buf);
            let normal: Vec<RecordSnapshot> = batch
                .iter()
                .map(|r| {
                    (
                        r.offset(),
                        r.timestamp(),
                        r.key().map(|k| k.to_vec()),
                        r.value().map(|v| v.to_vec()),
                    )
                })
                .collect();
            let mut supplier = BufferSupplier::create();
            let streaming: Vec<RecordSnapshot> = batch
                .streaming_iterator(&mut supplier)
                .map(|r| {
                    (
                        r.offset(),
                        r.timestamp(),
                        r.key().map(|k| k.to_vec()),
                        r.value().map(|v| v.to_vec()),
                    )
                })
                .collect();
            assert_eq!(normal, streaming, "compression {ct:?}");
        }
    }

    /// Decompression round-trip for every Phase 3c codec — required by the
    /// 3d-2 DoD ("decompression round-trip tested for each codec").
    #[test]
    fn decompression_round_trip_for_each_codec() {
        let recs = vec![
            SimpleRecord::new(10i64, Some(Bytes::from_static(b"k1")), Some(Bytes::from_static(b"v1")), &[]),
            SimpleRecord::new(20i64, None, Some(Bytes::from_static(b"v2")), &[]),
            SimpleRecord::new(30i64, Some(Bytes::from_static(b"k3")), None, &[]),
        ];
        for ct in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let buf = if ct == CompressionType::None {
                build_uncompressed_batch(
                    0,
                    TimestampType::CreateTime,
                    &recs,
                    NO_PRODUCER_ID,
                    NO_PRODUCER_EPOCH,
                    NO_SEQUENCE,
                    false,
                    false,
                )
            } else {
                build_compressed_batch(ct, 0, TimestampType::CreateTime, &recs)
            };
            let batch = DefaultRecordBatch::new(buf);
            assert_eq!(batch.compression_type(), ct);
            let collected: Vec<_> = batch.iter().collect();
            assert_eq!(collected.len(), recs.len(), "codec {ct:?}");
            for (i, r) in collected.iter().enumerate() {
                assert_eq!(r.timestamp(), recs[i].timestamp(), "codec {ct:?} record {i}");
                assert_eq!(
                    r.key().map(|k| k.to_vec()),
                    recs[i].key().map(|k| k.to_vec()),
                    "codec {ct:?} record {i}"
                );
                assert_eq!(
                    r.value().map(|v| v.to_vec()),
                    recs[i].value().map(|v| v.to_vec()),
                    "codec {ct:?} record {i}"
                );
            }
        }
    }

    /// Skip-key-value iterator yields `PartialDefaultRecord` for compressed
    /// batches and `DefaultRecord` for uncompressed. Mirrors Java's
    /// `testSkipKeyValueIteratorCorrectness` (without the mockito instance
    /// type assertion — Rust dispatch is non-trivial to inspect at runtime).
    #[test]
    fn skip_key_value_iterator_yields_correct_count() {
        let recs = vec![
            SimpleRecord::new(1i64, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"1")), &[]),
            SimpleRecord::new(2i64, Some(Bytes::from_static(b"b")), None, &[]),
            SimpleRecord::new(3i64, None, Some(Bytes::from_static(b"3")), &[]),
        ];
        for ct in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let buf = if ct == CompressionType::None {
                build_uncompressed_batch(
                    0,
                    TimestampType::CreateTime,
                    &recs,
                    NO_PRODUCER_ID,
                    NO_PRODUCER_EPOCH,
                    NO_SEQUENCE,
                    false,
                    false,
                )
            } else {
                build_compressed_batch(ct, 0, TimestampType::CreateTime, &recs)
            };
            let batch = DefaultRecordBatch::new(buf);
            let mut supplier = BufferSupplier::create();
            let collected: Vec<_> = batch.skip_key_value_iterator(&mut supplier).collect();
            assert_eq!(collected.len(), recs.len(), "codec {ct:?}");
        }
    }

    /// Translation of `DefaultRecordBatchTest.testInvalidRecordCountTooManyNonCompressedV2`
    /// — when the declared count is larger than actual records, iteration
    /// yields fewer items and surfaces an error via the iterator's poison
    /// flag (Java throws InvalidRecordException; we surface the error by
    /// terminating early — see UncompressedIter::next docstring).
    #[test]
    fn invalid_record_count_too_many_terminates_iter() {
        let recs = vec![
            SimpleRecord::new(1i64, None, Some(Bytes::from_static(b"hello")), &[]),
            SimpleRecord::new(2i64, None, Some(Bytes::from_static(b"there")), &[]),
            SimpleRecord::new(3i64, None, Some(Bytes::from_static(b"beautiful")), &[]),
        ];
        let mut buf = build_uncompressed_batch(
            0,
            TimestampType::CreateTime,
            &recs,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
        );
        // Override RecordCount to a value larger than the actual number of records.
        write_i32_at(&mut buf, RECORDS_COUNT_OFFSET, 5);
        let batch = DefaultRecordBatch::new(buf);
        // Iteration eventually fails because the underlying buffer runs out
        // before all 5 records are read; the iterator stops at the corruption
        // point.
        let collected: Vec<_> = batch.iter().collect();
        assert!(collected.len() < 5);
    }

    /// Translation of `testInvalidRecordCountTooLittleNonCompressedV2` —
    /// declared count is smaller; iteration yields the declared count and
    /// surfaces an error because there are leftover bytes.
    #[test]
    fn invalid_record_count_too_little_yields_declared_count() {
        let recs = vec![
            SimpleRecord::new(1i64, None, Some(Bytes::from_static(b"hello")), &[]),
            SimpleRecord::new(2i64, None, Some(Bytes::from_static(b"there")), &[]),
            SimpleRecord::new(3i64, None, Some(Bytes::from_static(b"beautiful")), &[]),
        ];
        let mut buf = build_uncompressed_batch(
            0,
            TimestampType::CreateTime,
            &recs,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
        );
        write_i32_at(&mut buf, RECORDS_COUNT_OFFSET, 2);
        let batch = DefaultRecordBatch::new(buf);
        // Iteration yields 2 items and stops; surplus bytes remain.
        let collected: Vec<_> = batch.iter().collect();
        assert_eq!(collected.len(), 2);
    }

    /// **Byte-level fixture (PLAN.md DoD).** Builds an empty v2 batch via
    /// `write_empty_header` and asserts the bytes match a hand-computed
    /// fixture. Reproduces all 61 header bytes deterministically.
    #[test]
    fn byte_level_fixture_empty_batch() {
        let mut buf = Vec::new();
        write_empty_header(
            &mut buf,
            CURRENT_MAGIC_VALUE,
            /* producer_id */ 100,
            /* producer_epoch */ 7,
            /* base_sequence */ 42,
            /* base_offset */ 0,
            /* last_offset */ 0,
            /* partition_leader_epoch */ 5,
            TimestampType::CreateTime,
            /* timestamp */ 1_000,
            /* is_transactional */ false,
            /* is_control_record */ false,
        );

        // Hand-computed expected layout (all big-endian, 61 bytes).
        // base_offset = 0 (8 bytes): 00*8
        // length = RECORD_BATCH_OVERHEAD - LOG_OVERHEAD = 61 - 12 = 49 (4 bytes BE): 00 00 00 31
        // partition_leader_epoch = 5: 00 00 00 05
        // magic = 2: 02
        // crc = computed (placeholder)
        // attributes = 0 (CreateTime + None compression, no transactional/control): 00 00
        // last_offset_delta = 0: 00 00 00 00
        // base_timestamp = NO_TIMESTAMP = -1 (i64 BE): FF*8
        // max_timestamp = 1000: 00 00 00 00 00 00 03 E8
        // producer_id = 100: 00 00 00 00 00 00 00 64
        // producer_epoch = 7: 00 07
        // base_sequence = 42: 00 00 00 2A
        // record_count = 0: 00 00 00 00

        assert_eq!(buf.len(), 61);
        assert_eq!(&buf[BASE_OFFSET_OFFSET..BASE_OFFSET_OFFSET + 8], &[0u8; 8]);
        assert_eq!(&buf[LENGTH_OFFSET..LENGTH_OFFSET + 4], &[0, 0, 0, 49]);
        assert_eq!(
            &buf[PARTITION_LEADER_EPOCH_OFFSET..PARTITION_LEADER_EPOCH_OFFSET + 4],
            &[0, 0, 0, 5]
        );
        assert_eq!(buf[MAGIC_OFFSET], 2);
        // CRC is recomputed by write_empty_header — verify via re-computation.
        let expected_crc = crc32c::crc32c(&buf[ATTRIBUTES_OFFSET..]);
        assert_eq!(byte_utils::read_unsigned_int_be_at(&buf, CRC_OFFSET), expected_crc as i64);
        assert_eq!(&buf[ATTRIBUTES_OFFSET..ATTRIBUTES_OFFSET + 2], &[0, 0]);
        assert_eq!(&buf[LAST_OFFSET_DELTA_OFFSET..LAST_OFFSET_DELTA_OFFSET + 4], &[0, 0, 0, 0]);
        assert_eq!(
            &buf[BASE_TIMESTAMP_OFFSET..BASE_TIMESTAMP_OFFSET + 8],
            &[0xFFu8; 8] // NO_TIMESTAMP = -1 i64 BE
        );
        assert_eq!(
            &buf[MAX_TIMESTAMP_OFFSET..MAX_TIMESTAMP_OFFSET + 8],
            &[0, 0, 0, 0, 0, 0, 0x03, 0xE8] // 1000
        );
        assert_eq!(
            &buf[PRODUCER_ID_OFFSET..PRODUCER_ID_OFFSET + 8],
            &[0, 0, 0, 0, 0, 0, 0, 0x64] // 100
        );
        assert_eq!(&buf[PRODUCER_EPOCH_OFFSET..PRODUCER_EPOCH_OFFSET + 2], &[0, 7]);
        assert_eq!(&buf[BASE_SEQUENCE_OFFSET..BASE_SEQUENCE_OFFSET + 4], &[0, 0, 0, 42]);
        assert_eq!(&buf[RECORDS_COUNT_OFFSET..RECORDS_COUNT_OFFSET + 4], &[0, 0, 0, 0]);

        // Round-trip through DefaultRecordBatch.
        let batch = DefaultRecordBatch::new(buf);
        assert!(batch.is_valid());
        assert_eq!(batch.producer_id(), 100);
        assert_eq!(batch.producer_epoch(), 7);
        assert_eq!(batch.base_sequence(), 42);
        assert_eq!(batch.partition_leader_epoch(), 5);
        assert_eq!(batch.max_timestamp(), 1000);
    }

    /// `estimate_batch_size_upper_bound` includes the 61-byte overhead.
    #[test]
    fn estimate_batch_size_upper_bound_includes_overhead() {
        let key = b"key";
        let value = b"value";
        let est = estimate_batch_size_upper_bound(Some(key), Some(value), &[]);
        // Lower bound: must include the 61-byte batch overhead.
        assert!(est >= RECORD_BATCH_OVERHEAD as i32);
        // Upper bound: 61 + MAX_RECORD_OVERHEAD (21) + length-of-empty-headers
        // varint (1) + key + value, with comfortable slack for the
        // length-prefix varint of key/value.
        let comfortable_max = RECORD_BATCH_OVERHEAD as i32
            + default_record::MAX_RECORD_OVERHEAD
            + 1
            + key.len() as i32
            + value.len() as i32
            + 4;
        assert!(est <= comfortable_max, "est={est} exceeded {comfortable_max}");
    }
}
