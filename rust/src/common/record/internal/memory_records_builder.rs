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
//! Corresponds to Java's `org.apache.kafka.common.record.internal.MemoryRecordsBuilder`.

use std::io::{self, Write};

use crate::common::Error;
use crate::common::compress::{CompressingWriter, Compression};
use crate::common::header::RecordHeader;
use crate::common::record::TimestampType;
use crate::common::record::internal::AbstractRecords;
use crate::common::record::internal::CompressionType;
use crate::common::record::internal::DefaultRecord;
use crate::common::record::internal::DefaultRecordBatch;
use crate::common::record::internal::MemoryRecords;
use crate::common::record::internal::RecordBatch;
use crate::common::record::internal::SimpleRecord;
use crate::common::utils::internals::ByteBufferOutputStream;

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
/// Corresponds to Java's `org.apache.kafka.common.record.internal.MemoryRecordsBuilder`.
#[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder")]
pub struct MemoryRecordsBuilder {
    timestamp_type: TimestampType,
    compression: Compression,
    /// The underlying stream holding the batch data: a single growable buffer, or KIP-1332's
    /// fixed chunks (Java's `bufferStream`).
    buffer_stream: ByteBufferOutputStream,
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
    #[expect(clippy::too_many_arguments)]
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#MemoryRecordsBuilder")]
    pub fn new(
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
        delete_horizon_ms: i64,
    ) -> Self {
        Self::with_stream_at(
            ByteBufferOutputStream::Single(buffer),
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
            delete_horizon_ms,
        )
        .expect("a single-buffer stream never fails to position")
    }

    /// Create a builder over an existing output stream, starting at the stream's current
    /// position. Corresponds to Java's `MemoryRecordsBuilder(ByteBufferOutputStream, ...)`; the
    /// incremental allocation strategy (KIP-1332) passes a
    /// [`ByteBufferOutputStream::Chunked`] stream here.
    ///
    /// # Errors
    ///
    /// The error from positioning the stream past the batch header (Java's
    /// `bufferStream.position(initialPosition + batchHeaderSizeInBytes)`), e.g. `IllegalState` for
    /// a chunked stream that has already been written to, or `IllegalArgument` when its chunks
    /// cannot hold the header.
    #[expect(clippy::too_many_arguments)]
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#MemoryRecordsBuilder")]
    pub(crate) fn with_buffer_stream(
        buffer_stream: ByteBufferOutputStream,
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
    ) -> Result<Self, Error> {
        let initial_position = buffer_stream.position()?;
        Self::with_stream_at(
            buffer_stream,
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
            delete_horizon_ms,
        )
    }

    /// The constructor body shared by [`new`](Self::new) (Java's `ByteBuffer` constructors,
    /// which take the start from the buffer's position; Rust passes it explicitly) and
    /// [`with_buffer_stream`](Self::with_buffer_stream).
    #[expect(clippy::too_many_arguments)]
    fn with_stream_at(
        mut buffer_stream: ByteBufferOutputStream,
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
    ) -> Result<Self, Error> {
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

        let batch_header_size =
            AbstractRecords::record_batch_header_size_in_bytes(magic, compression.compression_type());
        let header_end = initial_position + batch_header_size;
        let initial_buffer_capacity = match &mut buffer_stream {
            ByteBufferOutputStream::Single(buffer) => {
                let initial_buffer_capacity = buffer.capacity();
                // Ensure the buffer is large enough for the header
                if buffer.len() < header_end {
                    buffer.resize(header_end, 0);
                }
                if compression.compression_type() == CompressionType::None {
                    // Truncate to header_end so Write::write_all appends records
                    // right after the header placeholder.
                    buffer.truncate(header_end);
                }
                initial_buffer_capacity
            },
            ByteBufferOutputStream::Chunked(stream) => {
                // Java's `bufferStream.position(initialPosition + batchHeaderSizeInBytes)`: the
                // header slot is skipped in the chunks and written into the flattened buffer at
                // close. Compressed output is appended at close, so for both codecs the records
                // start right after the header.
                let initial_buffer_capacity = stream.initial_capacity()?;
                stream.set_position(header_end)?;
                initial_buffer_capacity
            },
        };

        let append_stream = if compression.compression_type() == CompressionType::None {
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

        Ok(Self {
            timestamp_type,
            compression,
            buffer_stream,
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
            closed: false,
            aborted: false,
        })
    }

    /// Create a new builder with default delete_horizon_ms (NO_TIMESTAMP).
    #[expect(clippy::too_many_arguments)]
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#MemoryRecordsBuilder")]
    pub fn with_default(
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

    /// Returns the underlying buffer (Java's `bufferStream.buffer()`).
    ///
    /// Takes `&mut self` because a chunked stream flattens its chunks on the first call (Java's
    /// `ChunkedByteBufferOutputStream.buffer()`).
    ///
    /// # Errors
    ///
    /// `IllegalState` for a chunked stream that has not been closed for appends, or has been
    /// deallocated.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#buffer")]
    pub fn buffer(&mut self) -> Result<&[u8], Error> {
        match &mut self.buffer_stream {
            ByteBufferOutputStream::Single(buffer) => Ok(buffer),
            ByteBufferOutputStream::Chunked(stream) => stream.buffer().map(|bytes| &bytes[..]),
        }
    }

    /// The underlying output stream, exposed so the incremental strategy can manage its
    /// chunk-backed stream. Rust needs the mutable form to attach and return chunks.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#bufferStream")]
    pub(crate) fn buffer_stream(&self) -> &ByteBufferOutputStream {
        &self.buffer_stream
    }

    /// Mutable access to the underlying output stream; see [`buffer_stream`](Self::buffer_stream).
    /// Rust-only: Java's single `bufferStream()` returns a mutable object.
    pub(crate) fn buffer_stream_mut(&mut self) -> &mut ByteBufferOutputStream {
        &mut self.buffer_stream
    }

    /// Returns the initial capacity of the buffer.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#initialCapacity")]
    pub fn initial_capacity(&self) -> usize {
        self.initial_buffer_capacity
    }

    /// Takes ownership of the underlying buffer, leaving an empty Vec in its place.
    ///
    /// This is used by [`RecordAccumulator::deallocate`] to return the actual batch
    /// buffer to the pool rather than allocating a new one.
    ///
    /// Single-buffer streams only: a chunked stream's memory is returned through
    /// [`buffer_stream_mut`](Self::buffer_stream_mut) and
    /// `ChunkedByteBufferOutputStream::deallocate` (Java's `ChunkedProducerBatch.deallocateBuffer`),
    /// so for it this returns an empty `Vec` and leaves the chunks attached.
    pub fn take_buffer(&mut self) -> Vec<u8> {
        match &mut self.buffer_stream {
            ByteBufferOutputStream::Single(buffer) => std::mem::take(buffer),
            ByteBufferOutputStream::Chunked(_) => Vec::new(),
        }
    }

    /// Returns the actual compression ratio after building.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#compressionRatio")]
    pub fn compression_ratio(&self) -> f64 {
        self.actual_compression_ratio as f64
    }

    /// Returns the compression configuration.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#compression")]
    pub fn compression(&self) -> &Compression {
        &self.compression
    }

    /// Returns whether this is a control batch.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#isControlBatch")]
    pub fn is_control_batch(&self) -> bool {
        self.is_control_batch
    }

    /// Returns whether this batch is transactional.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#isTransactional")]
    pub fn is_transactional(&self) -> bool {
        self.is_transactional
    }

    /// Returns whether the delete horizon is set.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#hasDeleteHorizonMs")]
    pub fn has_delete_horizon_ms(&self) -> bool {
        self.magic >= RecordBatch::MAGIC_VALUE_V2 && self.delete_horizon_ms >= 0
    }

    /// Close this builder and return the resulting `MemoryRecords`.
    ///
    /// Corresponds to Java's `MemoryRecordsBuilder.build()`
    /// (`MemoryRecordsBuilder.java:246-252`), and shares its **idempotence**: Java
    /// memoises the result in `builtRecords`, `close()` returns early once that field
    /// is set (`:373-374`), and nothing but `reopenAndRewriteProducerState` ever clears
    /// it. So Java's `build()` may be called any number of times and hands back the
    /// same `MemoryRecords` view every time. Callers depend on that —
    /// `ProducerBatch.records()` (`ProducerBatch.java:502-504`) is `build()`, and it is
    /// called once to serialise the produce request and again by
    /// `ProducerBatch.split` → `validateAndGetRecordBatch` (`:353`) when the broker
    /// answers `MESSAGE_TOO_LARGE`.
    ///
    /// The returned value is a cheap clone: [`MemoryRecords`] wraps a refcounted
    /// [`bytes::Bytes`], so this is an O(1) refcount bump and copies no record bytes
    /// (CLAUDE.md §14). The single finalisation copy lives in
    /// [`take_batch_data`](Self::take_batch_data) and runs once, inside `close()`.
    ///
    /// Panics if the builder has been aborted.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#build")]
    pub fn build(&mut self) -> MemoryRecords {
        if self.aborted {
            panic!("Attempting to build an aborted record batch");
        }
        self.close();
        // `close()` always populates `built_records` unless it early-returned because
        // the builder was already closed, in which case the field was populated by the
        // earlier call. Only `reopen_and_rewrite_producer_state` clears it, and it
        // clears `closed` with it — so `closed ⇔ built_records.is_some()` holds at every
        // point, and this `expect` is unreachable.
        //
        // That invariant is worth stating because it is what aligns Rust's
        // [`is_closed`](Self::is_closed) with Java's. Java has no `closed` field: its
        // `isClosed()` *is* `builtRecords != null` (`MemoryRecordsBuilder.java:914-916`).
        // The deleted `take_built_records` broke the correspondence — it left
        // `closed == true` with `built_records == None` — and the `built_size` shadow
        // field existed precisely to paper over the gap in
        // [`estimated_size_in_bytes`](Self::estimated_size_in_bytes). With the
        // correspondence restored, that accessor collapses back to Java's two-arm form
        // (`:928-930`) as a consequence rather than a coincidence. Raised by Critic 50.
        self.built_records.clone().expect("build() called but no records built")
    }

    /// Returns info about the records (max timestamp and shallow offset).
    ///
    /// Corresponds to Java's `MemoryRecordsBuilder.info()`.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#info")]
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#numRecords")]
    pub fn num_records(&self) -> i32 {
        self.num_records
    }

    /// Return the sum of the size of the batch header (always uncompressed)
    /// and the records (before compression).
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#uncompressedBytesWritten")]
    pub fn uncompressed_bytes_written(&self) -> usize {
        self.uncompressed_records_size_in_bytes + self.batch_header_size_in_bytes
    }

    /// Set the producer state.
    ///
    /// Panics if the builder is already closed.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#setProducerState")]
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#overrideLastOffset")]
    pub fn override_last_offset(&mut self, last_offset: i64) {
        if self.closed {
            panic!("Cannot override the last offset after the records have been built");
        }
        self.last_offset = Some(last_offset);
    }

    /// Release resources required for record appends.
    ///
    /// After this method is called, it's only possible to update the RecordBatch header.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#closeForRecordAppends")]
    pub fn close_for_record_appends(&mut self) {
        match std::mem::replace(&mut self.append_stream, AppendState::Closed) {
            AppendState::Direct => {
                // Records already written directly into the stream — nothing to copy.
            },
            AppendState::Compressed(writer) => match writer.finish() {
                Ok(compressed_data) => match &mut self.buffer_stream {
                    ByteBufferOutputStream::Single(buffer) => {
                        let header_end = self.initial_position + self.batch_header_size_in_bytes;
                        buffer.truncate(header_end);
                        buffer.extend_from_slice(&compressed_data);
                    },
                    ByteBufferOutputStream::Chunked(stream) => {
                        // Java writes the compressor's output into the chunked stream, which
                        // throws once the attached chunks are full (growth is KAFKA-20579); the
                        // producer rejects compression with the incremental strategy.
                        if let Err(e) = stream.write_with_bytes(&compressed_data) {
                            panic!("Failed to finish compression: {}", e);
                        }
                    },
                },
                Err(e) => {
                    panic!("Failed to finish compression: {}", e);
                },
            },
            AppendState::Closed => return,
        }
        // Java closes the append stream, which closes the underlying stream: a no-op for a single
        // buffer, and for a chunked stream it stops appends and releases the unused chunks.
        if let ByteBufferOutputStream::Chunked(stream) = &mut self.buffer_stream {
            stream.close();
        }
    }

    /// Abort the builder, releasing resources and resetting the buffer position.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#abort")]
    pub fn abort(&mut self) {
        self.close_for_record_appends();
        self.reset_buffer_to_initial_position();
        self.aborted = true;
    }

    /// Java's `buffer().position(initialPosition)`: discard everything written after the start
    /// of the batch.
    fn reset_buffer_to_initial_position(&mut self) {
        let initial_position = self.initial_position;
        self.with_written_buffer(|buffer| buffer.truncate(initial_position));
    }

    /// Runs `f` on the written bytes as one contiguous, mutable buffer (Java's `bufferStream.buffer()`
    /// written to in place): the builder's own `Vec` for a single-buffer stream, the flattened
    /// buffer for a chunked one. Only called once the builder is closed for appends.
    fn with_written_buffer<R>(&mut self, f: impl FnOnce(&mut Vec<u8>) -> R) -> R {
        match &mut self.buffer_stream {
            ByteBufferOutputStream::Single(buffer) => f(buffer),
            // A builder's chunked stream is closed by `close_for_record_appends` and deallocated
            // only when its batch completes, after the builder's last use; any other state is a
            // bug in the caller (Java's `IllegalStateException`).
            ByteBufferOutputStream::Chunked(stream) => match stream.rewrite_buffer(f) {
                Ok(result) => result,
                Err(e) => panic!("{}", e.message()),
            },
        }
    }

    /// Reopen a closed (but not aborted) batch for rewriting producer state.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#reopenAndRewriteProducerState")]
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
        self.closed = false;
        self.producer_id = producer_id;
        self.producer_epoch = producer_epoch;
        self.base_sequence = base_sequence;
        self.is_transactional = is_transactional;
    }

    /// Close the builder and finalize the batch.
    ///
    /// Panics if the builder has been aborted.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#close")]
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
            self.reset_buffer_to_initial_position();
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
    ///
    /// A chunked stream (KIP-1332) already made its one copy when it flattened its chunks, and
    /// caches the result as a `Bytes`, so its batch is an O(1) slice of that buffer: the chunked
    /// path copies exactly once, like the single-buffer path.
    fn take_batch_data(&mut self) -> bytes::Bytes {
        match &mut self.buffer_stream {
            ByteBufferOutputStream::Single(buffer) => bytes::Bytes::from(buffer[self.initial_position..].to_vec()),
            ByteBufferOutputStream::Chunked(stream) => match stream.buffer() {
                Ok(flattened) => flattened.slice(self.initial_position..),
                // See `with_written_buffer`.
                Err(e) => panic!("{}", e.message()),
            },
        }
    }

    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#validateProducerState")]
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#writeDefaultBatchHeader")]
    fn write_default_batch_header(&mut self) -> usize {
        self.ensure_open_for_record_batch_write();
        let offset_delta = (self.last_offset.unwrap() - self.base_offset) as i32;

        let max_timestamp = if self.timestamp_type == TimestampType::LogAppendTime {
            self.log_append_time
        } else {
            self.max_timestamp
        };

        // Compute has_delete_horizon before borrowing the buffer stream mutably
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

        self.with_written_buffer(|buffer| {
            let size = buffer.len() - initial_position;
            DefaultRecordBatch::write_header_at(
                buffer,
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
            size - RecordBatch::RECORD_BATCH_OVERHEAD
        })
    }

    /// Append a new record at the given offset.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#append")]
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#appendWithOffset")]
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#append")]
    pub fn append_with_offset_bytes(&mut self, offset: i64, timestamp: i64, key: Option<&[u8]>, value: Option<&[u8]>) {
        self.append_with_offset(offset, timestamp, key, value, RecordBatch::EMPTY_HEADERS);
    }

    /// Append a new record at the given offset using a `SimpleRecord`.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#append")]
    pub fn append_with_offset_simple(&mut self, offset: i64, record: &SimpleRecord) {
        self.append_with_offset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /// Append a new record at the next sequential offset.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#append")]
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

    /// Append a record without offset/magic validation (for testing).
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#appendUncheckedWithOffset")]
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#appendDefaultRecord")]
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
                let size = match &mut self.buffer_stream {
                    ByteBufferOutputStream::Single(buffer) => {
                        DefaultRecord::write_to(buffer, offset_delta, timestamp_delta, key, value, headers)?
                    },
                    ByteBufferOutputStream::Chunked(stream) => {
                        DefaultRecord::write_to(stream, offset_delta, timestamp_delta, key, value, headers)?
                    },
                };
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#recordWritten")]
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

    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#ensureOpenForRecordAppend")]
    fn ensure_open_for_record_append(&self) {
        if matches!(self.append_stream, AppendState::Closed) {
            panic!("Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
        }
    }

    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#ensureOpenForRecordBatchWrite")]
    fn ensure_open_for_record_batch_write(&self) {
        if self.is_closed() {
            panic!("Tried to write record batch header, but MemoryRecordsBuilder is closed");
        }
        if self.aborted {
            panic!("Tried to write record batch header, but MemoryRecordsBuilder is aborted");
        }
    }

    /// Get an estimate of the number of bytes written.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#estimatedBytesWritten")]
    fn estimated_bytes_written(&self) -> usize {
        self.estimated_bytes_written_with_uncompressed_size(self.uncompressed_records_size_in_bytes)
    }

    /// Returns the projected number of bytes the builder would write for the given uncompressed
    /// record bytes: exact for uncompressed, a ratio-aware estimate for compressed.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#estimatedBytesWritten")]
    fn estimated_bytes_written_with_uncompressed_size(&self, uncompressed_size: usize) -> usize {
        if self.compression.compression_type() == CompressionType::None {
            self.batch_header_size_in_bytes + uncompressed_size
        } else {
            self.batch_header_size_in_bytes
                + (uncompressed_size as f32 * self.estimated_compression_ratio * COMPRESSION_RATE_ESTIMATION_FACTOR)
                    as usize
        }
    }

    /// Projected value of [`estimated_bytes_written`](Self::estimated_bytes_written) after
    /// appending one more record with the given fields, using the record's worst-case
    /// (upper-bound) per-record size. Used by the incremental strategy to size mid-batch chunk
    /// extensions.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#estimatedBytesWrittenAfter")]
    pub(crate) fn estimated_bytes_written_after(
        &self,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> usize {
        let record_size = if self.magic < RecordBatch::MAGIC_VALUE_V2 {
            // `Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value)`. `LegacyRecord`
            // is not translated (this builder rejects legacy appends), so its
            // `recordOverhead(magic) + keySize + valueSize` is spelled out: the overhead is
            // `RECORD_OVERHEAD_V0` = 14 (crc 4, magic 1, attributes 1, key and value sizes 4 + 4)
            // for magic 0, and `RECORD_OVERHEAD_V1` = 22 (plus an 8-byte timestamp) for magic 1.
            let record_overhead = if self.magic == RecordBatch::MAGIC_VALUE_V0 {
                14
            } else {
                22
            };
            AbstractRecords::LOG_OVERHEAD + record_overhead + key.map_or(0, <[u8]>::len) + value.map_or(0, <[u8]>::len)
        } else {
            DefaultRecord::record_size_upper_bound(key, value, headers) as usize
        };
        self.estimated_bytes_written_with_uncompressed_size(self.uncompressed_records_size_in_bytes + record_size)
    }

    /// Set the estimated compression ratio.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#setEstimatedCompressionRatio")]
    pub fn set_estimated_compression_ratio(&mut self, ratio: f32) {
        self.estimated_compression_ratio = ratio;
    }

    /// Check if we have room for a new record containing the given key/value pair.
    ///
    /// If no records have been appended, then this returns true.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#hasRoomFor")]
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#maxAllowedBytes")]
    pub fn max_allowed_bytes(&self) -> usize {
        self.write_limit.saturating_sub(self.batch_header_size_in_bytes)
    }

    /// Returns whether the builder has been closed (records have been built).
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#isClosed")]
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Returns whether the batch is full.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#isFull")]
    pub fn is_full(&self) -> bool {
        matches!(self.append_stream, AppendState::Closed)
            || (self.num_records > 0 && self.write_limit <= self.estimated_bytes_written())
    }

    /// Get an estimate of the number of bytes written to the underlying buffer.
    ///
    /// The returned value is exactly correct if the record set is not compressed
    /// or if the builder has been closed.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#estimatedSizeInBytes")]
    pub fn estimated_size_in_bytes(&self) -> usize {
        if let Some(records) = &self.built_records {
            records.size_in_bytes()
        } else {
            self.estimated_bytes_written()
        }
    }

    /// Returns the magic version.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#magic")]
    pub fn magic(&self) -> i8 {
        self.magic
    }

    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#nextSequentialOffset")]
    fn next_sequential_offset(&self) -> i64 {
        match self.last_offset {
            None => self.base_offset,
            Some(last) => last + 1,
        }
    }

    /// Returns the producer ID.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#producerId")]
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns the producer epoch.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#producerEpoch")]
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Returns the base sequence.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder#baseSequence")]
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
#[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder$RecordsInfo")]
pub struct RecordsInfo {
    /// The maximum timestamp in the batch.
    pub(crate) max_timestamp: i64,
    /// The shallow offset of the record with the maximum timestamp.
    pub(crate) shallow_offset_of_max_timestamp: i64,
}

impl RecordsInfo {
    /// Create a new `RecordsInfo`.
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilder$RecordsInfo#RecordsInfo")]
    pub fn new(max_timestamp: i64, shallow_offset_of_max_timestamp: i64) -> Self {
        Self { max_timestamp, shallow_offset_of_max_timestamp }
    }

    /// The maximum timestamp in the batch. Java's public `RecordsInfo.maxTimestamp`.
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    /// The shallow offset of the record with the maximum timestamp. Java's public
    /// `RecordsInfo.shallowOffsetOfMaxTimestamp`.
    pub fn shallow_offset_of_max_timestamp(&self) -> i64 {
        self.shallow_offset_of_max_timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::internal::Record;

    /// All compression types to test with, each only for magic v2.
    fn all_compressions() -> Vec<Compression> {
        vec![
            Compression::none().build(),
            Compression::gzip().build(),
            Compression::snappy().build(),
            Compression::lz4().build(),
            Compression::zstd().build(),
        ]
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.testWriteEmptyRecordSet`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilderTest#testWriteEmptyRecordSet")]
    fn test_write_empty_record_set() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::with_default(
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilderTest#testWriteTransactionalRecordSet")]
    fn test_write_transactional_record_set() {
        let pid = 9809_i64;
        let epoch = 15_i16;
        let sequence = 2342_i32;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::with_default(
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
    #[doc(
        alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilderTest#testWriteTransactionalWithInvalidPID"
    )]
    fn test_write_transactional_with_invalid_pid() {
        let pid = RecordBatch::NO_PRODUCER_ID;
        let epoch = 15_i16;
        let sequence = 2342_i32;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::with_default(
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
    #[doc(
        alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilderTest#testWriteIdempotentWithInvalidEpoch"
    )]
    fn test_write_idempotent_with_invalid_epoch() {
        let pid = 9809_i64;
        let epoch = RecordBatch::NO_PRODUCER_EPOCH;
        let sequence = 2342_i32;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::with_default(
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
    #[doc(
        alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilderTest#testWriteIdempotentWithInvalidBaseSequence"
    )]
    fn test_write_idempotent_with_invalid_base_sequence() {
        let pid = 9809_i64;
        let epoch = 15_i16;
        let sequence = RecordBatch::NO_SEQUENCE;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::with_default(
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilderTest#testEstimatedSizeInBytes")]
    fn test_estimated_size_in_bytes() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::with_default(
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
            let mut builder = MemoryRecordsBuilder::with_default(
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
            let mut builder = MemoryRecordsBuilder::with_default(
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilderTest#testAppendedChecksumConsistency")]
    fn test_appended_checksum_consistency() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::with_default(
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilderTest#testSmallWriteLimit")]
    fn test_small_write_limit() {
        let key = b"foo";
        let value = b"bar";
        let write_limit = 0;

        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::with_default(
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
            let mut builder = MemoryRecordsBuilder::with_default(
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
    #[doc(alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilderTest#testAppendAtInvalidOffset")]
    fn test_append_at_invalid_offset() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::with_default(
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
            let mut builder = MemoryRecordsBuilder::with_default(
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
            let payload = result.expect_err("build() on an aborted builder must panic");
            assert_eq!(
                payload.downcast_ref::<&str>().copied(),
                Some("Attempting to build an aborted record batch"),
                "compression {:?}",
                compression.compression_type()
            );
        }
    }

    /// `build()` is **idempotent**, matching Java: `MemoryRecordsBuilder.build()`
    /// (`MemoryRecordsBuilder.java:246-252`) memoises into `builtRecords`, `close()`
    /// returns early once that field is set (`:373-374`), and nothing but
    /// `reopenAndRewriteProducerState` clears it. Every call therefore yields the same
    /// bytes.
    ///
    /// Regression cover for PLAN §9.18: the Rust `build()` used to hand its only copy
    /// away through a `take_built_records()` sibling that `ProducerBatch::records()`
    /// called on the send path, so the second call — `ProducerBatch::split` re-reading
    /// the batch after a `MESSAGE_TOO_LARGE` response — panicked with `build() called
    /// but no records built`.
    #[test]
    fn test_build_is_idempotent() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::with_default(
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
            builder.append_with_offset_bytes(0, 1_700_000_000_000, Some(b"k1"), Some(b"v1"));
            builder.append_with_offset_bytes(1, 1_700_000_000_001, Some(b"k2"), Some(b"v2"));

            let first = builder.build();
            let second = builder.build();
            let third = builder.build();
            assert_eq!(
                first.buffer(),
                second.buffer(),
                "compression {:?}",
                compression.compression_type()
            );
            assert_eq!(
                first.buffer(),
                third.buffer(),
                "compression {:?}",
                compression.compression_type()
            );
            assert_eq!(first.batches().count(), 1);
            // The size accessor keeps reporting the built size, as Java's
            // `estimatedSizeInBytes()` does while `builtRecords != null`.
            assert_eq!(builder.estimated_size_in_bytes(), first.size_in_bytes());
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.shouldResetBufferToInitialPositionOnAbort`.
    #[test]
    fn test_reset_buffer_on_abort() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::with_default(
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
            assert_eq!(0, builder.buffer().unwrap().len());
        }
    }

    /// Corresponds to Java's `MemoryRecordsBuilderTest.shouldThrowIllegalStateExceptionOnCloseWhenAborted`.
    #[test]
    fn test_throw_on_close_when_aborted() {
        for compression in all_compressions() {
            let mut builder = MemoryRecordsBuilder::with_default(
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
            let mut builder = MemoryRecordsBuilder::with_default(
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
            let mut builder = MemoryRecordsBuilder::with_default(
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
    #[doc(
        alias = "org.apache.kafka.common.record.internal.MemoryRecordsBuilderTest#testRecordTimestampsWithDeleteHorizon"
    )]
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

    // Note: testWriteEndTxnMarkerNonTransactionalBatch, testWriteEndTxnMarkerNonControlBatch,
    // testWriteLeaderChangeControlBatchWithoutLeaderEpoch, testWriteLeaderChangeControlBatch
    // are skipped because they require EndTransactionMarker, ControlRecordType, and
    // LeaderChangeMessage which are not yet implemented.
}

/// Rust tests for the KIP-1332 chunked stream behind the builder (KAFKA-20578). Java covers the
/// builder's chunked path only through `ChunkedRecordAccumulatorTest` (Phase 8); these pin the
/// builder-level contract that test relies on.
#[cfg(test)]
mod chunked_stream_tests {
    use std::sync::Arc;

    use super::*;
    use crate::producer::internals::{BufferPool, ChunkedByteBufferOutputStream};

    /// Small chunks, so the 61-byte batch header and the records both straddle chunk boundaries.
    const CHUNK_SIZE: usize = 32;

    fn chunked_builder(pool: &Arc<BufferPool>, chunks: usize, compression: Compression) -> MemoryRecordsBuilder {
        let initial = pool.try_allocate_chunks((chunks * CHUNK_SIZE) as i32).unwrap();
        let stream = ChunkedByteBufferOutputStream::new(initial, CHUNK_SIZE, Some(Arc::clone(pool))).unwrap();
        MemoryRecordsBuilder::with_buffer_stream(
            ByteBufferOutputStream::Chunked(stream),
            RecordBatch::CURRENT_MAGIC_VALUE,
            compression,
            TimestampType::CreateTime,
            0,
            RecordBatch::NO_TIMESTAMP,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            1024,
            RecordBatch::NO_TIMESTAMP,
        )
        .unwrap()
    }

    fn single_builder() -> MemoryRecordsBuilder {
        MemoryRecordsBuilder::with_default(
            Vec::with_capacity(1024),
            0,
            RecordBatch::CURRENT_MAGIC_VALUE,
            Compression::none().build(),
            TimestampType::CreateTime,
            0,
            RecordBatch::NO_TIMESTAMP,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            1024,
        )
    }

    fn append_records(builder: &mut MemoryRecordsBuilder) {
        for i in 0..5u8 {
            builder.append(1_700_000_000_000 + i as i64, Some(&[i; 7]), Some(&[i; 23]), &[]);
        }
    }

    /// The chunked stream produces byte-for-byte the batch the single-buffer stream does, and
    /// closing releases the chunks the records never reached.
    #[test]
    fn test_chunked_stream_builds_the_same_batch_as_a_single_buffer() {
        let pool = Arc::new(BufferPool::new_incremental_for_test(64 * CHUNK_SIZE as i64, CHUNK_SIZE));
        let mut chunked = chunked_builder(&pool, 16, Compression::none().build());
        assert!(chunked.buffer_stream().is_chunked());
        assert_eq!(CHUNK_SIZE, chunked.initial_capacity());
        let mut single = single_builder();
        append_records(&mut chunked);
        append_records(&mut single);
        assert_eq!(single.estimated_size_in_bytes(), chunked.estimated_size_in_bytes());

        let expected = single.build();
        let records = chunked.build();
        assert_eq!(expected.buffer(), records.buffer());
        assert_eq!(5, records.records().count());

        // 61-byte header + 5 records of 37 bytes = 246 bytes: 8 chunks hold data, 8 were released
        // at close.
        assert_eq!(246, records.size_in_bytes());
        assert_eq!(56 * CHUNK_SIZE as i64, pool.available_memory());
        // The data-bearing chunks stay until the batch completes.
        chunked.buffer_stream_mut().as_chunked_mut().unwrap().deallocate();
        assert_eq!(64 * CHUNK_SIZE as i64, pool.available_memory());
        // The built batch lives in the flattened buffer, not the chunks.
        assert_eq!(expected.buffer(), records.buffer());
    }

    /// `reopen_and_rewrite_producer_state` on a chunked builder: the second close rewrites the
    /// header of the flattened buffer with the new producer state, leaving the records intact and
    /// the batch handed out by the first close unchanged — the same result as the single-buffer
    /// path.
    #[test]
    fn test_reopen_and_rewrite_producer_state_on_chunked_stream() {
        let pool = Arc::new(BufferPool::new_incremental_for_test(64 * CHUNK_SIZE as i64, CHUNK_SIZE));
        let mut chunked = chunked_builder(&pool, 16, Compression::none().build());
        let mut single = single_builder();
        for builder in [&mut chunked, &mut single] {
            builder.set_producer_state(1000, 5, 7, false);
            append_records(builder);
        }
        let first = chunked.build();
        let first_bytes = first.buffer().to_vec();
        assert_eq!(1000, first.batches().next().unwrap().producer_id());

        for builder in [&mut chunked, &mut single] {
            builder.reopen_and_rewrite_producer_state(2000, 6, 42, true);
        }
        assert!(!chunked.is_closed());
        let rewritten = chunked.build();
        let expected = single.build();
        assert_eq!(expected.buffer(), rewritten.buffer(), "same bytes as the single-buffer path");
        let batch = rewritten.batches().next().unwrap();
        assert_eq!(2000, batch.producer_id());
        assert_eq!(6, batch.producer_epoch());
        assert_eq!(42, batch.base_sequence());
        assert!(batch.is_transactional());
        assert!(batch.is_valid(), "the CRC covers the rewritten header");
        assert_eq!(5, rewritten.records().count());
        assert_eq!(first_bytes, first.buffer(), "the earlier batch is not mutated");
        assert_eq!(&first_bytes[61..], &rewritten.buffer()[61..], "the records are untouched");
    }

    /// Abort and an empty close leave a chunked builder with nothing written past the batch
    /// start, as `buffer().position(initialPosition)` does in Java.
    #[test]
    fn test_abort_and_empty_close_on_chunked_stream() {
        let pool = Arc::new(BufferPool::new_incremental_for_test(64 * CHUNK_SIZE as i64, CHUNK_SIZE));
        let mut aborted = chunked_builder(&pool, 16, Compression::none().build());
        append_records(&mut aborted);
        aborted.abort();
        assert_eq!(0, aborted.buffer().unwrap().len());

        let mut empty = chunked_builder(&pool, 4, Compression::none().build());
        let records = empty.build();
        assert_eq!(0, records.size_in_bytes());
        assert_eq!(0, empty.buffer().unwrap().len());
        drop(aborted);
        drop(empty);
        assert_eq!(
            64 * CHUNK_SIZE as i64,
            pool.available_memory(),
            "dropping the builders returns their chunks"
        );
    }

    /// The chunked stream must be positioned before any write, so a stream that was already
    /// written to is refused with the stream's error instead of a panic.
    #[test]
    fn test_with_buffer_stream_rejects_a_written_stream() {
        let pool = Arc::new(BufferPool::new_incremental_for_test(64 * CHUNK_SIZE as i64, CHUNK_SIZE));
        let initial = pool.try_allocate_chunks(4 * CHUNK_SIZE as i32).unwrap();
        let mut stream = ChunkedByteBufferOutputStream::new(initial, CHUNK_SIZE, Some(Arc::clone(&pool))).unwrap();
        stream.write_with_b(1).unwrap();
        let err = MemoryRecordsBuilder::with_buffer_stream(
            ByteBufferOutputStream::Chunked(stream),
            RecordBatch::CURRENT_MAGIC_VALUE,
            Compression::none().build(),
            TimestampType::CreateTime,
            0,
            RecordBatch::NO_TIMESTAMP,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            1024,
            RecordBatch::NO_TIMESTAMP,
        )
        .err()
        .expect("a written stream cannot be positioned");
        assert_eq!("position() can only be called before any writes", err.message());
    }

    /// The single-buffer stream ignores its `Vec`'s contents past the position, as before.
    #[test]
    fn test_with_buffer_stream_single_starts_at_the_position() {
        let mut builder = MemoryRecordsBuilder::with_buffer_stream(
            ByteBufferOutputStream::Single(vec![0xAB; 3]),
            RecordBatch::CURRENT_MAGIC_VALUE,
            Compression::none().build(),
            TimestampType::CreateTime,
            0,
            RecordBatch::NO_TIMESTAMP,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            1024,
            RecordBatch::NO_TIMESTAMP,
        )
        .unwrap();
        append_records(&mut builder);
        let records = builder.build();
        let mut expected = single_builder();
        append_records(&mut expected);
        assert_eq!(expected.build().buffer(), records.buffer());
        assert_eq!(
            &[0xAB; 3][..],
            &builder.buffer().unwrap()[..3],
            "bytes before the batch are kept"
        );
    }

    /// `estimated_bytes_written_after` (Java's `estimatedBytesWrittenAfter`): the current estimate
    /// plus the record's upper-bound size, ratio-adjusted when compressed.
    #[test]
    fn test_estimated_bytes_written_after() {
        let key = [1u8; 7];
        let value = [2u8; 23];
        let upper_bound = DefaultRecord::record_size_upper_bound(Some(&key), Some(&value), &[]) as usize;

        let mut builder = single_builder();
        assert_eq!(
            61 + upper_bound,
            builder.estimated_bytes_written_after(Some(&key), Some(&value), &[])
        );
        append_records(&mut builder);
        assert_eq!(61 + 5 * 37, builder.estimated_size_in_bytes());
        assert_eq!(
            61 + 5 * 37 + upper_bound,
            builder.estimated_bytes_written_after(Some(&key), Some(&value), &[])
        );
        assert_eq!(
            61 + 5 * 37 + DefaultRecord::record_size_upper_bound(None, None, &[]) as usize,
            builder.estimated_bytes_written_after(None, None, &[])
        );

        let mut compressed = MemoryRecordsBuilder::with_default(
            Vec::with_capacity(1024),
            0,
            RecordBatch::CURRENT_MAGIC_VALUE,
            Compression::gzip().build(),
            TimestampType::CreateTime,
            0,
            RecordBatch::NO_TIMESTAMP,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            1024,
        );
        compressed.set_estimated_compression_ratio(0.5);
        append_records(&mut compressed);
        let uncompressed = 5 * 37 + upper_bound;
        assert_eq!(
            61 + (uncompressed as f32 * 0.5 * COMPRESSION_RATE_ESTIMATION_FACTOR) as usize,
            compressed.estimated_bytes_written_after(Some(&key), Some(&value), &[])
        );
    }
}
