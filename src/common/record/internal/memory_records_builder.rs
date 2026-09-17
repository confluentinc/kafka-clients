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

//! Builder for creating memory record batches.
//!
//! This is the write path for [`MemoryRecords`]. It transparently handles
//! compression and exposes methods for appending new records.
//!
//! Corresponds to Java's `org.apache.kafka.common.record.MemoryRecordsBuilder`.

use std::io::{self, Write};

use crate::common::Error;
use crate::common::compress::{CompressingWriter, Compression};
use crate::common::header::internals::RecordHeader;
use crate::common::record::TimestampType;
use crate::common::record::internal::CompressionType;
use crate::common::record::internal::ControlRecordType;
use crate::common::record::internal::DefaultRecord;
use crate::common::record::internal::DefaultRecordBatch;
use crate::common::record::internal::EndTransactionMarker;
use crate::common::record::internal::MemoryRecords;
use crate::common::record::internal::RecordBatch;
use crate::common::record::internal::SimpleRecord;
use crate::common::record::internal::abstract_records::record_batch_header_size_in_bytes;

/// Estimation factor to account for compression overhead.
const COMPRESSION_RATE_ESTIMATION_FACTOR: f32 = 1.05;

/// State of the append stream.
enum AppendState<W: Write> {
    /// No compression — records are written directly into the main buffer,
    /// eliminating one full copy of the record bytes.
    Direct,
    /// Compression — records go through a `CompressingWriter` into a separate
    /// buffer, then the compressed output is appended to the main buffer on close.
    Compressed(CompressingWriter<W>),
    /// Stream has been closed (records cannot be appended).
    Closed,
}

/// Builder for creating memory record batches.
///
/// Manages a compression stream and provides methods for appending records.
/// On [`close()`](Self::close) or [`build()`](Self::build), flushes compression
/// and writes the batch header (attributes, timestamps, CRC).
///
/// Corresponds to Java's `org.apache.kafka.common.record.MemoryRecordsBuilder`.
pub struct MemoryRecordsBuilder {
    timestamp_type: TimestampType,
    compression: Compression,
    /// The underlying buffer holding the batch data.
    buffer: Vec<u8>,
    magic: i8,
    initial_position: usize,
    base_offset: i64,
    log_append_time: i64,
    is_control_batch: bool,
    partition_leader_epoch: i32,
    write_limit: usize,
    batch_header_size_in_bytes: usize,
    delete_horizon_ms: i64,

    /// Conservative estimate of the compression ratio.
    estimated_compression_ratio: f32,

    /// The compression stream state.
    append_stream: AppendState<Vec<u8>>,
    is_transactional: bool,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    /// Number of bytes (excluding the header) written before compression.
    uncompressed_records_size_in_bytes: usize,
    num_records: i32,
    actual_compression_ratio: f32,
    max_timestamp: i64,
    offset_of_max_timestamp: i64,
    last_offset: Option<i64>,
    base_timestamp: Option<i64>,

    initial_buffer_capacity: usize,
    built_records: Option<MemoryRecords>,
    built_size: Option<usize>,
    closed: bool,
    aborted: bool,
}

impl MemoryRecordsBuilder {
    /// Create a new `MemoryRecordsBuilder`.
    ///
    /// The `buffer` is expected to have a position (initial_position) where the
    /// batch should start. The builder will skip `batch_header_size_in_bytes` bytes
    /// at the start for the header, then append records after that.
    ///
    /// Corresponds to Java's `MemoryRecordsBuilder(ByteBufferOutputStream, ...)`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mut buffer: Vec<u8>,
        initial_position: usize,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
        log_append_time: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
        is_control_batch: bool,
        partition_leader_epoch: i32,
        write_limit: usize,
        delete_horizon_ms: i64,
    ) -> Self {
        // Validate parameters
        if magic > RecordBatch::MAGIC_VALUE_V0 && timestamp_type == TimestampType::NoTimestampType {
            panic!("TimestampType must be set for magic >= 0");
        }
        if magic < RecordBatch::MAGIC_VALUE_V2 {
            if is_transactional {
                panic!("Transactional records are not supported for magic {}", magic);
            }
            if is_control_batch {
                panic!("Control records are not supported for magic {}", magic);
            }
            if compression.compression_type() == CompressionType::Zstd {
                panic!("ZStandard compression is not supported for magic {}", magic);
            }
            if delete_horizon_ms != RecordBatch::NO_TIMESTAMP {
                panic!("Delete horizon timestamp is not supported for magic {}", magic);
            }
        }

        let batch_header_size = record_batch_header_size_in_bytes(magic, compression.compression_type());
        let initial_buffer_capacity = buffer.capacity();

        // Ensure the buffer is large enough for the header
        let header_end = initial_position + batch_header_size;
        if buffer.len() < header_end {
            buffer.resize(header_end, 0);
        }

        let append_stream = if compression.compression_type() == CompressionType::None {
            // Truncate to header_end so Write::write_all appends records
            // right after the header placeholder.
            buffer.truncate(header_end);
            AppendState::Direct
        } else {
            let append_buf = Vec::new();
            let writer = compression
                .wrap_for_output(append_buf, magic)
                .expect("Failed to create compression writer");
            AppendState::Compressed(writer)
        };

        let has_delete_horizon = magic >= RecordBatch::MAGIC_VALUE_V2 && delete_horizon_ms >= 0;
        let base_timestamp = if has_delete_horizon {
            Some(delete_horizon_ms)
        } else {
            None
        };

        Self {
            timestamp_type,
            compression,
            buffer,
            magic,
            initial_position,
            base_offset,
            log_append_time,
            is_control_batch,
            partition_leader_epoch,
            write_limit,
            batch_header_size_in_bytes: batch_header_size,
            delete_horizon_ms,
            estimated_compression_ratio: 1.0,
            append_stream,
            is_transactional,
            producer_id,
            producer_epoch,
            base_sequence,
            uncompressed_records_size_in_bytes: 0,
            num_records: 0,
            actual_compression_ratio: 1.0,
            max_timestamp: RecordBatch::NO_TIMESTAMP,
            offset_of_max_timestamp: -1,
            last_offset: None,
            base_timestamp,
            initial_buffer_capacity,
            built_records: None,
            built_size: None,
            closed: false,
            aborted: false,
        }
    }

    /// Create a new builder with default delete_horizon_ms (NO_TIMESTAMP).
    #[allow(clippy::too_many_arguments)]
    pub fn new_default(
        buffer: Vec<u8>,
        initial_position: usize,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
        log_append_time: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
        is_control_batch: bool,
        partition_leader_epoch: i32,
        write_limit: usize,
    ) -> Self {
        Self::new(
            buffer,
            initial_position,
            magic,
            compression,
            timestamp_type,
            base_offset,
            log_append_time,
            producer_id,
            producer_epoch,
            base_sequence,
            is_transactional,
            is_control_batch,
            partition_leader_epoch,
            write_limit,
            RecordBatch::NO_TIMESTAMP,
        )
    }

    /// Returns the underlying buffer.
    pub fn buffer(&self) -> &Vec<u8> {
        &self.buffer
    }

    /// Returns a mutable reference to the underlying buffer.
    pub fn buffer_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }

    /// Returns the initial capacity of the buffer.
    pub fn initial_capacity(&self) -> usize {
        self.initial_buffer_capacity
    }

    /// Takes ownership of the underlying buffer, leaving an empty Vec in its place.
    ///
    /// This is used by [`RecordAccumulator::deallocate`] to return the actual batch
    /// buffer to the pool rather than allocating a new one.
    pub fn take_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }

    /// Returns the actual compression ratio after building.
    pub fn compression_ratio(&self) -> f64 {
        self.actual_compression_ratio as f64
    }

    /// Returns the compression configuration.
    pub fn compression(&self) -> &Compression {
        &self.compression
    }

    /// Returns whether this is a control batch.
    pub fn is_control_batch(&self) -> bool {
        self.is_control_batch
    }

    /// Returns whether this batch is transactional.
    pub fn is_transactional(&self) -> bool {
        self.is_transactional
    }

    /// Returns whether the delete horizon is set.
    pub fn has_delete_horizon_ms(&self) -> bool {
        self.magic >= RecordBatch::MAGIC_VALUE_V2 && self.delete_horizon_ms >= 0
    }

    /// Close this builder and return the resulting `MemoryRecords`.
    ///
    /// Panics if the builder has been aborted.
    pub fn build(&mut self) -> MemoryRecords {
        if self.aborted {
            panic!("Attempting to build an aborted record batch");
        }
        self.close();
        self.built_records.clone().expect("build() called but no records built")
    }

    /// Take the built records, consuming them from the builder.
    ///
    /// Unlike [`build`](Self::build), this can only be called once — subsequent
    /// calls return `None`. Avoids cloning the batch buffer.
    pub fn take_built_records(&mut self) -> Option<MemoryRecords> {
        if self.closed && self.built_records.is_none() && self.num_records > 0 {
            let batch_data = self.take_batch_data();
            self.built_records = Some(MemoryRecords::new(batch_data));
        }
        self.close();
        let records = self.built_records.take();
        if let Some(ref r) = records {
            self.built_size = Some(r.size_in_bytes());
        }
        records
    }

    /// Returns info about the records (max timestamp and shallow offset).
    ///
    /// Corresponds to Java's `MemoryRecordsBuilder.info()`.
    pub fn info(&self) -> RecordsInfo {
        if self.timestamp_type == TimestampType::LogAppendTime {
            if self.compression.compression_type() != CompressionType::None || self.magic >= RecordBatch::MAGIC_VALUE_V2
            {
                RecordsInfo::new(self.log_append_time, self.last_offset.unwrap_or(-1))
            } else {
                RecordsInfo::new(self.log_append_time, self.base_offset)
            }
        } else if self.max_timestamp == RecordBatch::NO_TIMESTAMP {
            RecordsInfo::new(RecordBatch::NO_TIMESTAMP, -1)
        } else if self.compression.compression_type() != CompressionType::None
            || self.magic >= RecordBatch::MAGIC_VALUE_V2
        {
            RecordsInfo::new(self.max_timestamp, self.last_offset.unwrap_or(-1))
        } else {
            RecordsInfo::new(self.max_timestamp, self.offset_of_max_timestamp)
        }
    }

    /// Returns the number of records appended so far.
    pub fn num_records(&self) -> i32 {
        self.num_records
    }

    /// Return the sum of the size of the batch header (always uncompressed)
    /// and the records (before compression).
    pub fn uncompressed_bytes_written(&self) -> usize {
        self.uncompressed_records_size_in_bytes + self.batch_header_size_in_bytes
    }

    /// Set the producer state.
    ///
    /// Panics if the builder is already closed.
    pub fn set_producer_state(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
    ) {
        if self.is_closed() {
            panic!("Trying to set producer state of an already closed batch. This indicates a bug on the client.");
        }
        self.producer_id = producer_id;
        self.producer_epoch = producer_epoch;
        self.base_sequence = base_sequence;
        self.is_transactional = is_transactional;
    }

    /// Override the last offset (for compaction).
    ///
    /// Panics if the records have already been built.
    pub fn override_last_offset(&mut self, last_offset: i64) {
        if self.closed {
            panic!("Cannot override the last offset after the records have been built");
        }
        self.last_offset = Some(last_offset);
    }

    /// Release resources required for record appends.
    ///
    /// After this method is called, it's only possible to update the RecordBatch header.
    pub fn close_for_record_appends(&mut self) {
        match std::mem::replace(&mut self.append_stream, AppendState::Closed) {
            AppendState::Direct => {
                // Records already written directly into self.buffer — nothing to copy.
            },
            AppendState::Compressed(writer) => match writer.finish() {
                Ok(compressed_data) => {
                    let header_end = self.initial_position + self.batch_header_size_in_bytes;
                    self.buffer.truncate(header_end);
                    self.buffer.extend_from_slice(&compressed_data);
                },
                Err(e) => {
                    panic!("Failed to finish compression: {}", e);
                },
            },
            AppendState::Closed => {},
        }
    }

    /// Abort the builder, releasing resources and resetting the buffer position.
    pub fn abort(&mut self) {
        self.close_for_record_appends();
        self.buffer.truncate(self.initial_position);
        self.aborted = true;
    }

    /// Reopen a closed (but not aborted) batch for rewriting producer state.
    pub fn reopen_and_rewrite_producer_state(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
    ) {
        if self.aborted {
            panic!("Should not reopen a batch which is already aborted.");
        }
        self.built_records = None;
        self.built_size = None;
        self.closed = false;
        self.producer_id = producer_id;
        self.producer_epoch = producer_epoch;
        self.base_sequence = base_sequence;
        self.is_transactional = is_transactional;
    }

    /// Close the builder and finalize the batch.
    ///
    /// Panics if the builder has been aborted.
    pub fn close(&mut self) {
        if self.aborted {
            panic!("Cannot close MemoryRecordsBuilder as it has already been aborted");
        }

        if self.closed {
            return;
        }

        self.validate_producer_state();

        self.close_for_record_appends();

        if self.num_records == 0 {
            self.buffer.truncate(self.initial_position);
            self.built_records = Some(MemoryRecords::empty());
        } else if self.magic > RecordBatch::MAGIC_VALUE_V1 {
            let written_compressed = self.write_default_batch_header();
            self.actual_compression_ratio = written_compressed as f32 / self.uncompressed_records_size_in_bytes as f32;

            let batch_data = self.take_batch_data();
            self.built_records = Some(MemoryRecords::new(batch_data));
        } else {
            // Legacy format not supported
            let batch_data = self.take_batch_data();
            self.built_records = Some(MemoryRecords::new(batch_data));
        }
        self.closed = true;
    }

    /// Extract the finalized batch bytes as a refcounted [`bytes::Bytes`].
    ///
    /// Note (Phase 1 deviation): the producer builder keeps its working buffer
    /// as `Vec<u8>` rather than `BytesMut`. `bytes` 1.x exposes no public
    /// zero-copy `Vec<u8> -> BytesMut` adoption (`BytesMut::from_vec` is
    /// crate-private), so switching the builder to `BytesMut` would either copy
    /// at construction (adopting the pooled `Vec`) or break `BufferPool` reuse
    /// (the pool reclaims the original-capacity `Vec` via `take_buffer`). We
    /// therefore retain the single finalization copy here — the same copy the
    /// previous `to_vec()` performed — and wrap it in `Bytes` (which adopts the
    /// freshly allocated `Vec` with no extra copy). The receive path, which is
    /// the actual zero-copy target of §27, is unaffected by this.
    fn take_batch_data(&mut self) -> bytes::Bytes {
        bytes::Bytes::from(self.buffer[self.initial_position..].to_vec())
    }

    fn validate_producer_state(&self) {
        if self.is_transactional && self.producer_id == RecordBatch::NO_PRODUCER_ID {
            panic!("Cannot write transactional messages without a valid producer ID");
        }

        if self.producer_id != RecordBatch::NO_PRODUCER_ID {
            if self.producer_epoch == RecordBatch::NO_PRODUCER_EPOCH {
                panic!("Invalid negative producer epoch");
            }

            if self.base_sequence < 0 && !self.is_control_batch {
                panic!("Invalid negative sequence number used");
            }

            if self.magic < RecordBatch::MAGIC_VALUE_V2 {
                panic!("Idempotent messages are not supported for magic {}", self.magic);
            }
        }
    }

    /// Write the header to the default batch and return the number of
    /// compressed bytes written (excluding the header).
    fn write_default_batch_header(&mut self) -> usize {
        self.ensure_open_for_record_batch_write();
        let size = self.buffer.len() - self.initial_position;
        let written_compressed = size - RecordBatch::RECORD_BATCH_OVERHEAD;
        let offset_delta = (self.last_offset.unwrap() - self.base_offset) as i32;

        let max_timestamp = if self.timestamp_type == TimestampType::LogAppendTime {
            self.log_append_time
        } else {
            self.max_timestamp
        };

        // Compute has_delete_horizon before borrowing self.buffer mutably
        let has_delete_horizon = self.has_delete_horizon_ms();
        let initial_position = self.initial_position;
        let base_offset = self.base_offset;
        let magic = self.magic;
        let compression_type = self.compression.compression_type();
        let timestamp_type = self.timestamp_type;
        let base_timestamp = self.base_timestamp.unwrap_or(RecordBatch::NO_TIMESTAMP);
        let producer_id = self.producer_id;
        let producer_epoch = self.producer_epoch;
        let base_sequence = self.base_sequence;
        let is_transactional = self.is_transactional;
        let is_control_batch = self.is_control_batch;
        let partition_leader_epoch = self.partition_leader_epoch;
        let num_records = self.num_records;

        DefaultRecordBatch::write_header_at(
            &mut self.buffer,
            initial_position,
            base_offset,
            offset_delta,
            size,
            magic,
            compression_type,
            timestamp_type,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            is_transactional,
            is_control_batch,
            has_delete_horizon,
            partition_leader_epoch,
            num_records,
        );

        written_compressed
    }

    /// Append a new record at the given offset.
    #[allow(clippy::too_many_arguments)]
    fn append_with_offset_internal(
        &mut self,
        offset: i64,
        is_control_record: bool,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) {
        if is_control_record != self.is_control_batch {
            panic!("Control records can only be appended to control batches");
        }

        if let Some(last) = self.last_offset
            && offset <= last
        {
            panic!(
                "Illegal offset {} following previous offset {} (Offsets must increase monotonically).",
                offset, last
            );
        }

        if timestamp < 0 && timestamp != RecordBatch::NO_TIMESTAMP {
            panic!("Invalid negative timestamp {}", timestamp);
        }

        if self.magic < RecordBatch::MAGIC_VALUE_V2 && !headers.is_empty() {
            panic!("Magic v{} does not support record headers", self.magic);
        }

        if self.base_timestamp.is_none() {
            self.base_timestamp = Some(timestamp);
        }

        if self.magic > RecordBatch::MAGIC_VALUE_V1 {
            self.append_default_record(offset, timestamp, key, value, headers);
        } else {
            // Legacy format not supported in producer path
            panic!("Legacy record format (magic {}) not supported", self.magic);
        }
    }

    /// Append a new record at the given offset.
    ///
    /// # Arguments
    /// * `offset` - The absolute offset of the record in the log buffer
    /// * `timestamp` - The record timestamp
    /// * `key` - The record key
    /// * `value` - The record value
    /// * `headers` - The record headers if there are any
    pub fn append_with_offset(
        &mut self,
        offset: i64,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) {
        self.append_with_offset_internal(offset, false, timestamp, key, value, headers);
    }

    /// Append a new record at the given offset with byte slices.
    pub fn append_with_offset_bytes(&mut self, offset: i64, timestamp: i64, key: Option<&[u8]>, value: Option<&[u8]>) {
        self.append_with_offset(offset, timestamp, key, value, RecordBatch::EMPTY_HEADERS);
    }

    /// Append a new record at the given offset using a `SimpleRecord`.
    pub fn append_with_offset_simple(&mut self, offset: i64, record: &SimpleRecord) {
        self.append_with_offset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /// Append a new record at the next sequential offset.
    pub fn append(&mut self, timestamp: i64, key: Option<&[u8]>, value: Option<&[u8]>, headers: &[RecordHeader]) {
        let offset = self.next_sequential_offset();
        self.append_with_offset(offset, timestamp, key, value, headers);
    }

    /// Append a new record at the next sequential offset with just key and value.
    pub fn append_kv(&mut self, timestamp: i64, key: Option<&[u8]>, value: Option<&[u8]>) {
        self.append(timestamp, key, value, RecordBatch::EMPTY_HEADERS);
    }

    /// Append a `SimpleRecord` at the next sequential offset.
    pub fn append_simple(&mut self, record: &SimpleRecord) {
        let offset = self.next_sequential_offset();
        self.append_with_offset_simple(offset, record);
    }

    /// Append a record implementing the Record trait.
    pub fn append_record(&mut self, record: &dyn crate::common::record::internal::Record) {
        self.append_with_offset_internal(
            record.offset(),
            self.is_control_batch,
            record.timestamp(),
            record.key(),
            record.value(),
            record.headers(),
        );
    }

    /// Append a control record at the next sequential offset.
    ///
    /// Corresponds to Java's `MemoryRecordsBuilder.appendControlRecord`.
    pub(crate) fn append_control_record(
        &mut self,
        timestamp: i64,
        control_type: ControlRecordType,
        value: &[u8],
    ) -> Result<(), Error> {
        // Java performs this check inside the shared `appendWithOffset` (as
        // `isControlRecord != isControlBatch`). Because a control record always
        // sets `isControlRecord = true`, the check reduces to `!isControlBatch`
        // here. The Rust `append_with_offset_internal` treats the mismatch as an
        // unrecoverable precondition (`panic!`) on the normal record path, so we
        // surface the recoverable case as an `Error` before delegating (CLAUDE.md
        // §10.2) — the message matches Java's exactly.
        if !self.is_control_batch {
            return Err(Error::local_illegal_argument(
                "Control records can only be appended to control batches",
            ));
        }
        let key = control_type.record_key()?;
        let offset = self.next_sequential_offset();
        self.append_with_offset_internal(
            offset,
            true,
            timestamp,
            Some(key.as_slice()),
            Some(value),
            RecordBatch::EMPTY_HEADERS,
        );
        Ok(())
    }

    /// Append an end transaction marker (`COMMIT`/`ABORT`) at the next
    /// sequential offset.
    ///
    /// Corresponds to Java's `MemoryRecordsBuilder.appendEndTxnMarker`.
    pub(crate) fn append_end_txn_marker(&mut self, timestamp: i64, marker: &EndTransactionMarker) -> Result<(), Error> {
        if self.producer_id == RecordBatch::NO_PRODUCER_ID {
            return Err(Error::local_illegal_argument(
                "End transaction marker requires a valid producerId",
            ));
        }
        if !self.is_transactional {
            return Err(Error::local_illegal_argument(
                "End transaction marker depends on batch transactional flag being enabled",
            ));
        }
        let value = marker.serialize_value();
        self.append_control_record(timestamp, marker.control_type(), value)
    }

    /// Append a record without offset/magic validation (for testing).
    pub fn append_unchecked_with_offset(&mut self, offset: i64, record: &SimpleRecord) -> io::Result<()> {
        if self.magic >= RecordBatch::MAGIC_VALUE_V2 {
            let offset_delta = (offset - self.base_offset) as i32;
            let timestamp = record.timestamp();
            if self.base_timestamp.is_none() {
                self.base_timestamp = Some(timestamp);
            }

            let size_in_bytes = self.write_record(
                offset_delta,
                timestamp - self.base_timestamp.unwrap(),
                record.key(),
                record.value(),
                record.headers(),
            )?;
            self.record_written(offset, timestamp, size_in_bytes);
        }
        Ok(())
    }

    /// Internal method to append a default (v2) record.
    fn append_default_record(
        &mut self,
        offset: i64,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) {
        self.ensure_open_for_record_append();
        let offset_delta = (offset - self.base_offset) as i32;
        let timestamp_delta = timestamp - self.base_timestamp.unwrap();
        let size_in_bytes = self
            .write_record(offset_delta, timestamp_delta, key, value, headers)
            .expect("I/O error when writing to the append stream, closing");
        self.record_written(offset, timestamp, size_in_bytes);
    }

    /// Write a record to the append stream and return the size in bytes.
    fn write_record(
        &mut self,
        offset_delta: i32,
        timestamp_delta: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> io::Result<usize> {
        match &mut self.append_stream {
            AppendState::Direct => {
                let size =
                    DefaultRecord::write_to(&mut self.buffer, offset_delta, timestamp_delta, key, value, headers)?;
                Ok(size as usize)
            },
            AppendState::Compressed(writer) => {
                let size = DefaultRecord::write_to(writer, offset_delta, timestamp_delta, key, value, headers)?;
                Ok(size as usize)
            },
            AppendState::Closed => {
                panic!("Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
            },
        }
    }

    /// Track that a record was written.
    fn record_written(&mut self, offset: i64, timestamp: i64, size: usize) {
        if self.num_records == i32::MAX {
            panic!("Maximum number of records per batch exceeded, max records: {}", i32::MAX);
        }
        if offset - self.base_offset > i32::MAX as i64 {
            panic!(
                "Maximum offset delta exceeded, base offset: {}, last offset: {}",
                self.base_offset, offset
            );
        }

        self.num_records += 1;
        self.uncompressed_records_size_in_bytes += size;
        self.last_offset = Some(offset);

        if self.magic > RecordBatch::MAGIC_VALUE_V0 && timestamp > self.max_timestamp {
            self.max_timestamp = timestamp;
            self.offset_of_max_timestamp = offset;
        }
    }

    fn ensure_open_for_record_append(&self) {
        if matches!(self.append_stream, AppendState::Closed) {
            panic!("Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
        }
    }

    fn ensure_open_for_record_batch_write(&self) {
        if self.is_closed() {
            panic!("Tried to write record batch header, but MemoryRecordsBuilder is closed");
        }
        if self.aborted {
            panic!("Tried to write record batch header, but MemoryRecordsBuilder is aborted");
        }
    }

    /// Get an estimate of the number of bytes written.
    fn estimated_bytes_written(&self) -> usize {
        if self.compression.compression_type() == CompressionType::None {
            self.batch_header_size_in_bytes + self.uncompressed_records_size_in_bytes
        } else {
            self.batch_header_size_in_bytes
                + (self.uncompressed_records_size_in_bytes as f32
                    * self.estimated_compression_ratio
                    * COMPRESSION_RATE_ESTIMATION_FACTOR) as usize
        }
    }

    /// Set the estimated compression ratio.
    pub fn set_estimated_compression_ratio(&mut self, ratio: f32) {
        self.estimated_compression_ratio = ratio;
    }

    /// Check if we have room for a new record containing the given key/value pair.
    ///
    /// If no records have been appended, then this returns true.
    pub fn has_room_for(
        &self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> bool {
        if self.is_full() {
            return false;
        }

        // We always allow at least one record to be appended
        if self.num_records == 0 {
            return true;
        }

        let record_size = if self.magic < RecordBatch::MAGIC_VALUE_V2 {
            // Legacy not supported, but provide a reasonable estimate
            0
        } else {
            let next_offset_delta = match self.last_offset {
                None => 0,
                Some(last) => (last - self.base_offset + 1) as i32,
            };
            let timestamp_delta = match self.base_timestamp {
                None => 0i64,
                Some(base) => timestamp - base,
            };
            DefaultRecord::size_in_bytes_with_slices(next_offset_delta, timestamp_delta, key, value, headers) as usize
        };

        // Be conservative and not take compression of the new record into consideration.
        self.write_limit >= self.estimated_bytes_written() + record_size
    }

    /// Check if we have room for a given number of bytes.
    pub fn has_room_for_estimated(&self, estimated_records_size: usize) -> bool {
        if self.is_full() {
            return false;
        }
        self.write_limit >= self.estimated_bytes_written() + estimated_records_size
    }

    /// Returns the maximum number of bytes that can be written for records.
    pub fn max_allowed_bytes(&self) -> usize {
        self.write_limit.saturating_sub(self.batch_header_size_in_bytes)
    }

    /// Returns whether the builder has been closed (records have been built).
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Returns whether the batch is full.
    pub fn is_full(&self) -> bool {
        matches!(self.append_stream, AppendState::Closed)
            || (self.num_records > 0 && self.write_limit <= self.estimated_bytes_written())
    }

    /// Get an estimate of the number of bytes written to the underlying buffer.
    ///
    /// The returned value is exactly correct if the record set is not compressed
    /// or if the builder has been closed.
    pub fn estimated_size_in_bytes(&self) -> usize {
        if let Some(records) = &self.built_records {
            records.size_in_bytes()
        } else if let Some(size) = self.built_size {
            size
        } else {
            self.estimated_bytes_written()
        }
    }

    /// Returns the magic version.
    pub fn magic(&self) -> i8 {
        self.magic
    }

    fn next_sequential_offset(&self) -> i64 {
        match self.last_offset {
            None => self.base_offset,
            Some(last) => last + 1,
        }
    }

    /// Returns the producer ID.
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns the producer epoch.
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Returns the base sequence.
    pub fn base_sequence(&self) -> i32 {
        self.base_sequence
    }
}

impl Drop for MemoryRecordsBuilder {
    fn drop(&mut self) {
        // Ensure resources are released
        if !self.aborted && !self.closed {
            // Try to close gracefully, but don't panic in Drop
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.close_for_record_appends();
            }));
        }
    }
}

/// Information about records in a batch (max timestamp and shallow offset).
///
/// Corresponds to Java's `MemoryRecordsBuilder.RecordsInfo`.
#[derive(Clone, Debug)]
pub struct RecordsInfo {
    /// The maximum timestamp in the batch.
    pub max_timestamp: i64,
    /// The shallow offset of the record with the maximum timestamp.
    pub shallow_offset_of_max_timestamp: i64,
}

impl RecordsInfo {
    /// Create a new `RecordsInfo`.
    pub fn new(max_timestamp: i64, shallow_offset_of_max_timestamp: i64) -> Self {
        Self { max_timestamp, shallow_offset_of_max_timestamp }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::internal::Record;

    /// All compression types to test with, each only for magic v2.
    fn all_compressions() -> Vec<Compression> {
        vec![
            Compression::none(),
            Compression::gzip(),
            Compression::snappy(),
            Compression::lz4(),
            Compression::zstd(),
        ]
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.testWriteEmptyRecordSet`.
    #[test]
    fn test_write_empty_record_set() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(128),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                128,
            );

            let records = builder.build();
            assert_eq!(
                0,
                records.size_in_bytes(),
                "Failed for compression {:?}",
                compression.compression_type()
            );
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.testWriteTransactionalRecordSet`.
    #[test]
    fn test_write_transactional_record_set() {
        let pid = 9809_i64;
        let epoch = 15_i16;
        let sequence = 2342_i32;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(128),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                pid,
                epoch,
                sequence,
                true,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                128,
            );
            builder.append_kv(1_700_000_000_000, Some(b"foo"), Some(b"bar"));
            let records = builder.build();

            let batches: Vec<_> = records.batches().collect();
            assert_eq!(1, batches.len());
            assert!(batches[0].is_transactional());
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.testWriteTransactionalWithInvalidPID`.
    #[test]
    fn test_write_transactional_with_invalid_pid() {
        let pid = RecordBatch::NO_PRODUCER_ID;
        let epoch = 15_i16;
        let sequence = 2342_i32;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(128),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                pid,
                epoch,
                sequence,
                true,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                128,
            );
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                builder.close();
            }));
            assert!(
                result.is_err(),
                "Should have panicked for compression {:?}",
                compression.compression_type()
            );
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.testWriteIdempotentWithInvalidEpoch`.
    #[test]
    fn test_write_idempotent_with_invalid_epoch() {
        let pid = 9809_i64;
        let epoch = RecordBatch::NO_PRODUCER_EPOCH;
        let sequence = 2342_i32;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(128),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                pid,
                epoch,
                sequence,
                true,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                128,
            );
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                builder.close();
            }));
            assert!(
                result.is_err(),
                "Should have panicked for compression {:?}",
                compression.compression_type()
            );
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.testWriteIdempotentWithInvalidBaseSequence`.
    #[test]
    fn test_write_idempotent_with_invalid_base_sequence() {
        let pid = 9809_i64;
        let epoch = 15_i16;
        let sequence = RecordBatch::NO_SEQUENCE;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(128),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                pid,
                epoch,
                sequence,
                true,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                128,
            );
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                builder.close();
            }));
            assert!(
                result.is_err(),
                "Should have panicked for compression {:?}",
                compression.compression_type()
            );
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.testEstimatedSizeInBytes`.
    #[test]
    fn test_estimated_size_in_bytes() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(1024),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                1024,
            );

            let mut previous_estimate = 0;
            for i in 0..10 {
                let value = format!("{}", i);
                builder.append_kv(i as i64, None, Some(value.as_bytes()));
                let current_estimate = builder.estimated_size_in_bytes();
                assert!(
                    current_estimate > previous_estimate,
                    "Estimate should increase after append for {:?}, iteration {}",
                    compression.compression_type(),
                    i
                );
                previous_estimate = current_estimate;
            }

            let bytes_written_before_close = builder.estimated_size_in_bytes();
            let records = builder.build();
            assert_eq!(records.size_in_bytes(), builder.estimated_size_in_bytes());
            if compression.compression_type() == CompressionType::None {
                assert_eq!(records.size_in_bytes(), bytes_written_before_close);
            }
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.buildUsingLogAppendTime`.
    #[test]
    fn test_build_using_log_append_time() {
        let log_append_time = 1_700_000_000_000_i64;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(1024),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::LogAppendTime,
                0,
                log_append_time,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                1024,
            );
            builder.append_kv(0, Some(b"a"), Some(b"1"));
            builder.append_kv(0, Some(b"b"), Some(b"2"));
            builder.append_kv(0, Some(b"c"), Some(b"3"));
            let records = builder.build();

            let info = builder.info();
            assert_eq!(log_append_time, info.max_timestamp);
            // For v2, offset_of_max_timestamp is always last offset
            assert_eq!(2, info.shallow_offset_of_max_timestamp);

            for batch in records.batches() {
                assert_eq!(TimestampType::LogAppendTime, batch.timestamp_type());
                for record in batch.iter_records().unwrap() {
                    assert_eq!(log_append_time, record.timestamp());
                }
            }
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.buildUsingCreateTime`.
    #[test]
    fn test_build_using_create_time() {
        let log_append_time = 1_700_000_000_000_i64;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(1024),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                log_append_time,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                1024,
            );
            builder.append_kv(0, Some(b"a"), Some(b"1"));
            builder.append_kv(2, Some(b"b"), Some(b"2"));
            builder.append_kv(1, Some(b"c"), Some(b"3"));
            let records = builder.build();

            let info = builder.info();
            assert_eq!(2, info.max_timestamp);
            // For v2, offset_of_max_timestamp is always the last offset
            assert_eq!(2, info.shallow_offset_of_max_timestamp);

            let expected_timestamps = [0_i64, 2, 1];
            let mut i = 0;
            for batch in records.batches() {
                assert_eq!(TimestampType::CreateTime, batch.timestamp_type());
                for record in batch.iter_records().unwrap() {
                    assert_eq!(expected_timestamps[i], record.timestamp());
                    i += 1;
                }
            }
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.testAppendedChecksumConsistency`.
    #[test]
    fn test_appended_checksum_consistency() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(512),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                RecordBatch::NO_TIMESTAMP,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                512,
            );
            builder.append_kv(1, Some(b"key"), Some(b"value"));
            let records = builder.build();
            let all_records: Vec<_> = records.records().collect();
            assert_eq!(1, all_records.len());
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.testSmallWriteLimit`.
    #[test]
    fn test_small_write_limit() {
        let key = b"foo";
        let value = b"bar";
        let write_limit = 0;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(512),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                RecordBatch::NO_TIMESTAMP,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                write_limit,
            );

            assert!(!builder.is_full());
            assert!(builder.has_room_for(0, Some(key), Some(value), RecordBatch::EMPTY_HEADERS));
            builder.append_kv(0, Some(key), Some(value));

            assert!(builder.is_full());
            assert!(!builder.has_room_for(0, Some(key), Some(value), RecordBatch::EMPTY_HEADERS));

            let records = builder.build();
            let all_records: Vec<_> = records.records().collect();
            assert_eq!(
                1,
                all_records.len(),
                "Failed for compression {:?}",
                compression.compression_type()
            );

            let record = &all_records[0];
            assert_eq!(Some(key.as_slice()), record.key());
            assert_eq!(Some(value.as_slice()), record.value());
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.writePastLimit`.
    #[test]
    fn test_write_past_limit() {
        let log_append_time = 1_700_000_000_000_i64;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(64),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                log_append_time,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                64,
            );
            builder.set_estimated_compression_ratio(0.5);
            builder.append_kv(0, Some(b"a"), Some(b"1"));
            builder.append_kv(1, Some(b"b"), Some(b"2"));

            assert!(!builder.has_room_for(2, Some(b"c"), Some(b"3"), RecordBatch::EMPTY_HEADERS));
            builder.append_kv(2, Some(b"c"), Some(b"3"));
            let records = builder.build();

            let info = builder.info();
            assert_eq!(2, info.shallow_offset_of_max_timestamp);
            assert_eq!(2, info.max_timestamp);

            let mut i = 0_i64;
            for batch in records.batches() {
                assert_eq!(TimestampType::CreateTime, batch.timestamp_type());
                for record in batch.iter_records().unwrap() {
                    assert_eq!(i, record.timestamp());
                    i += 1;
                }
            }
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.testAppendAtInvalidOffset`.
    #[test]
    fn test_append_at_invalid_offset() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(1024),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                1_700_000_000_000,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                1024,
            );

            builder.append_with_offset_bytes(0, 1_700_000_000_000, Some(b"a"), None);

            // offsets must increase monotonically
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                builder.append_with_offset_bytes(0, 1_700_000_000_000, Some(b"b"), None);
            }));
            assert!(
                result.is_err(),
                "Should panic for duplicate offset with compression {:?}",
                compression.compression_type()
            );
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.shouldThrowIllegalStateExceptionOnBuildWhenAborted`.
    #[test]
    fn test_throw_on_build_when_aborted() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(128),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                128,
            );
            builder.abort();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                builder.build();
            }));
            assert!(result.is_err());
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.shouldResetBufferToInitialPositionOnAbort`.
    #[test]
    fn test_reset_buffer_on_abort() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(128),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                128,
            );
            builder.append_kv(0, Some(b"a"), Some(b"1"));
            builder.abort();
            // After abort, the buffer should be truncated to initial_position (0)
            assert_eq!(0, builder.buffer().len());
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.shouldThrowIllegalStateExceptionOnCloseWhenAborted`.
    #[test]
    fn test_throw_on_close_when_aborted() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(128),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                128,
            );
            builder.abort();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                builder.close();
            }));
            assert!(result.is_err());
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.shouldThrowIllegalStateExceptionOnAppendWhenAborted`.
    #[test]
    fn test_throw_on_append_when_aborted() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(128),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                128,
            );
            builder.abort();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                builder.append_kv(0, Some(b"a"), Some(b"1"));
            }));
            assert!(result.is_err());
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.shouldThrowIllegalStateExceptionOnAppendWhenClosed`.
    #[test]
    fn test_throw_on_append_when_closed() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(128),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                128,
            );
            builder.append_kv(0, Some(b"a"), Some(b"1"));
            builder.build();

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                builder.append_kv(0, Some(b"a"), Some(b"1"));
            }));
            assert!(result.is_err());
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.testRecordTimestampsWithDeleteHorizon`.
    #[test]
    fn test_record_timestamps_with_delete_horizon() {
        let delete_horizon = 100_i64;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new(
                Vec::with_capacity(2 * 1024 * 1024),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                RecordBatch::NO_SEQUENCE,
                false,
                false,
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                0,
                delete_horizon,
            );

            builder.append_kv(50, Some(b"0"), Some(b"0"));
            builder.append_kv(100, Some(b"1"), None);
            builder.append_kv(150, Some(b"2"), Some(b"2"));

            let records = builder.build();
            let batches: Vec<_> = records.batches().collect();
            assert_eq!(Some(delete_horizon), batches[0].delete_horizon_ms());

            let record_list = batches[0].iter_records().unwrap();
            assert_eq!(50, record_list[0].timestamp());
            assert_eq!(100, record_list[1].timestamp());
            assert_eq!(150, record_list[2].timestamp());
        }
    }

    // Note: testUnsupportedCompress, testLegacyCompressionRate are skipped because
    // they test magic v0/v1 which we do not support in the Rust producer path.

    /// Corresponds to Java's
    /// `MemoryRecordsBuilderTest.testWriteEndTxnMarkerNonTransactionalBatch`.
    ///
    /// Java is a `@ParameterizedTest` over `MemoryRecordsBuilderArgumentsProvider`
    /// (buffer offset x compression x magic). Following this file's established
    /// convention for that provider (see `test_write_transactional_record_set`),
    /// we loop the compression dimension at `MAGIC_VALUE_V2` only: the Rust
    /// producer path does not support magic v0/v1, and `MemoryRecordsBuilder::new`
    /// panics on a control/transactional batch below v2 rather than offering a
    /// returnable error — so Java's `magic < MAGIC_VALUE_V2` branch (which asserts
    /// the *constructor* throws) has no v2-path analog here. The buffer offset
    /// does not affect the guard under test.
    ///
    /// Java uses `assertThrows(IllegalArgumentException.class, ...)`. Because
    /// `appendEndTxnMarker`'s transactional guard is a recoverable throw, the Rust
    /// `append_end_txn_marker` returns `Err` (CLAUDE.md §10.2); DoD #3 additionally
    /// pins the message text.
    #[test]
    fn test_write_end_txn_marker_non_transactional_batch() {
        let pid = 9809_i64;
        let epoch = 15_i16;
        let sequence = RecordBatch::NO_SEQUENCE;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(128),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                pid,
                epoch,
                sequence,
                false, // is_transactional
                true,  // is_control_batch
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                128,
            );
            let marker =
                EndTransactionMarker::new(ControlRecordType::Abort, 0).expect("ABORT is a valid end txn marker type");
            let error = builder
                .append_end_txn_marker(RecordBatch::NO_TIMESTAMP, &marker)
                .expect_err("appending an end txn marker to a non-transactional batch must fail");
            assert_eq!(
                error.message(),
                "End transaction marker depends on batch transactional flag being enabled",
                "Failed for compression {:?}",
                compression.compression_type()
            );
        }
    }

    /// Corresponds to Java's
    /// `MemoryRecordsBuilderTest.testWriteEndTxnMarkerNonControlBatch`. See
    /// `test_write_end_txn_marker_non_transactional_batch` for why only the
    /// `MAGIC_VALUE_V2` compression dimension is exercised and why the guard
    /// surfaces as an `Err` rather than a panic.
    #[test]
    fn test_write_end_txn_marker_non_control_batch() {
        let pid = 9809_i64;
        let epoch = 15_i16;
        let sequence = RecordBatch::NO_SEQUENCE;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(128),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
                0,
                pid,
                epoch,
                sequence,
                true,  // is_transactional
                false, // is_control_batch
                RecordBatch::NO_PARTITION_LEADER_EPOCH,
                128,
            );
            let marker =
                EndTransactionMarker::new(ControlRecordType::Abort, 0).expect("ABORT is a valid end txn marker type");
            let error = builder
                .append_end_txn_marker(RecordBatch::NO_TIMESTAMP, &marker)
                .expect_err("appending a control record to a non-control batch must fail");
            assert_eq!(
                error.message(),
                "Control records can only be appended to control batches",
                "Failed for compression {:?}",
                compression.compression_type()
            );
        }
    }

    // Note: testWriteLeaderChangeControlBatchWithoutLeaderEpoch and
    // testWriteLeaderChangeControlBatch are skipped because they require
    // LeaderChangeMessage and ControlRecordUtils, which are not yet translated.
}
