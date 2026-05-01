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

//! Translation of `org.apache.kafka.common.record.MemoryRecordsBuilder`.
//!
//! Writes new log data into an in-memory batch buffer. This is the producer
//! write path — see CLAUDE.md rule 12 (zero-copy through the entire write
//! path) and the PLAN.md zero-copy DoD:
//!
//! > "MemoryRecordsBuilder::append writes serialized bytes directly into
//! > the batch buffer — no intermediate Vec<u8> per record."
//!
//! Both the uncompressed and compressed paths stream directly into the
//! batch buffer:
//!
//! * **Uncompressed**: `append` writes through the codec's pass-through
//!   wrapper into `buffer_stream` via `default_record::write_to_stream` —
//!   no per-record intermediate buffer.
//! * **Compressed**: at construction time, the codec is wrapped around
//!   `buffer_stream` and held as the persistent `append_stream` (mirrors
//!   Java's `MemoryRecordsBuilder.java:143` —
//!   `appendStream = new DataOutputStream(compression.wrapForOutput(this.bufferStream, magic))`).
//!   Each `append` streams record bytes through that codec writer
//!   directly into `buffer_stream`. The codec's stateful compressor
//!   (gzip's deflate state, zstd's dictionary, etc.) is preserved across
//!   appends. `close()` flushes/drops the codec writer to emit any
//!   trailing block before back-patching the batch header.
//!
//! ### Self-referential lifetime
//!
//! Java's `appendStream` is just a field whose lifetime is bounded by the
//! builder thanks to GC. Rust's borrow checker cannot express
//! "this `Box<dyn Write>` borrows the buffer_stream that lives on the
//! same struct". We resolve the self-referential constraint by:
//!
//! * Boxing the `ByteBufferOutputStream` so its address is stable across
//!   moves of the builder (a stack-allocated stream would invalidate
//!   the codec writer's pointer the first time the builder is moved).
//! * Constructing a `'static`-erased `Box<dyn Write + 'static>` over a
//!   raw `*mut` pointer to the boxed stream (see `install_append_stream`).
//!   The `'static` is a controlled lie — the writer is dropped before
//!   the boxed stream via the explicit `Drop` impl on the builder.
//! * Funneling all access through `&mut self` methods so the borrow on
//!   `buffer_stream` while the codec writes is always exclusive, and
//!   ensuring the codec writer is dropped (in `close()` / `abort()` /
//!   `Drop`) before any code re-borrows `buffer_stream`.
//!
//! ## Magic version scope
//!
//! Phase 3 only supports magic v2 (the producer code path). The builder's
//! constructor rejects v0/v1 with `IllegalArgumentException`-equivalent
//! errors mirroring Java's checks; a few legacy parameter validations are
//! kept for parity with the Java unit tests
//! (`testUnsupportedCompress`, etc.).

use crate::common::compress::{
    Compression, GzipCompression, Lz4Compression, NoCompression, SnappyCompression, ZstdCompression,
};
use crate::common::errors::KafkaError;
use crate::common::header::RecordHeader;
use crate::common::record::default_record_batch::{RECORD_BATCH_OVERHEAD, write_header_at};
use crate::common::record::record_batch::{
    MAGIC_VALUE_V0, MAGIC_VALUE_V2, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_TIMESTAMP,
};
use crate::common::record::records::LOG_OVERHEAD;
use crate::common::record::{CompressionType, MemoryRecords, SimpleRecord, TimestampType, default_record};
use crate::common::utils::byte_buffer_output_stream::ByteBufferOutputStream;

/// Estimation factor mirroring Java's
/// `MemoryRecordsBuilder.COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f`.
const COMPRESSION_RATE_ESTIMATION_FACTOR: f32 = 1.05;

/// Records info returned by [`MemoryRecordsBuilder::info`]. Mirrors Java's
/// nested `RecordsInfo` POD.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecordsInfo {
    pub max_timestamp: i64,
    pub shallow_offset_of_max_timestamp: i64,
}

impl RecordsInfo {
    pub fn new(max_timestamp: i64, shallow_offset_of_max_timestamp: i64) -> Self {
        RecordsInfo { max_timestamp, shallow_offset_of_max_timestamp }
    }
}

/// Writer for new log data. Mirrors Java's `MemoryRecordsBuilder`. See the
/// module-level docs for the zero-copy contract.
pub struct MemoryRecordsBuilder {
    timestamp_type: TimestampType,
    compression_type: CompressionType,
    compression_level: Option<i32>,
    /// Owning storage for the batch payload. The Java `ByteBufferOutputStream`
    /// maps to Rust's [`ByteBufferOutputStream`].
    ///
    /// Boxed so the stream's address is stable across moves of the
    /// builder — required for the self-referential `append_stream` field
    /// (which holds a raw pointer into this stream). Stack-allocating
    /// `ByteBufferOutputStream` here would invalidate the codec writer's
    /// pointer the first time the builder is moved (e.g. when returned
    /// from a constructor by value).
    buffer_stream: Box<ByteBufferOutputStream>,
    magic: i8,
    initial_position: usize,
    base_offset: i64,
    log_append_time: i64,
    is_control_batch: bool,
    partition_leader_epoch: i32,
    write_limit: i32,
    batch_header_size_in_bytes: i32,
    delete_horizon_ms: i64,

    estimated_compression_ratio: f32,

    /// Records the codec is closed for further appends.
    closed_for_appends: bool,
    is_transactional: bool,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    /// Number of bytes written before compression (excludes batch header).
    uncompressed_records_size_in_bytes: i32,
    num_records: i32,
    actual_compression_ratio: f32,
    max_timestamp: i64,
    offset_of_max_timestamp: i64,
    last_offset: Option<i64>,
    base_timestamp: Option<i64>,

    /// Set to a frozen `MemoryRecords` after `build()`. Subsequent calls
    /// short-circuit.
    built_records: Option<MemoryRecords>,
    aborted: bool,

    /// Persistent codec writer that streams records into
    /// [`Self::buffer_stream`]. For [`CompressionType::None`] this is
    /// `None` and writes go directly to `buffer_stream`. For compressed
    /// batches this is `Some(codec_writer)` constructed at builder
    /// creation; each `append` writes through it, and `close()` drops
    /// it (which flushes any trailing block) before back-patching the
    /// header.
    ///
    /// The writer borrows the boxed `buffer_stream` via a raw pointer.
    /// See the module-level "Self-referential lifetime" docs and the
    /// safety comment on [`Self::install_append_stream`].
    ///
    /// `'static` is a controlled lie — the writer is alive only as long
    /// as `buffer_stream` is valid. All access is funneled through
    /// `&mut self` methods, the writer is never exposed outside the
    /// builder, and the explicit `Drop` impl drops the writer before
    /// the rest of the fields.
    append_stream: Option<Box<dyn std::io::Write + 'static>>,
}

impl MemoryRecordsBuilder {
    /// Construct from an owned [`ByteBufferOutputStream`]. Mirrors Java's
    /// 14-arg constructor.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalArgument`] for the same configurations
    /// Java rejects with `IllegalArgumentException`:
    /// * `magic > 0` and `timestamp_type == NoTimestampType`,
    /// * transactional/control records on `magic < V2`,
    /// * ZSTD on `magic < V2`,
    /// * delete-horizon timestamp set on `magic < V2`.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stream(
        mut buffer_stream: ByteBufferOutputStream,
        magic: i8,
        compression_type: CompressionType,
        timestamp_type: TimestampType,
        base_offset: i64,
        log_append_time: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
        is_control_batch: bool,
        partition_leader_epoch: i32,
        write_limit: i32,
        delete_horizon_ms: i64,
    ) -> Result<Self, KafkaError> {
        if magic > MAGIC_VALUE_V0 && timestamp_type == TimestampType::NoTimestampType {
            return Err(KafkaError::IllegalArgument(
                "TimestampType must be set for magic >= 0".to_string(),
            ));
        }
        if magic < MAGIC_VALUE_V2 {
            if is_transactional {
                return Err(KafkaError::IllegalArgument(format!(
                    "Transactional records are not supported for magic {magic}"
                )));
            }
            if is_control_batch {
                return Err(KafkaError::IllegalArgument(format!(
                    "Control records are not supported for magic {magic}"
                )));
            }
            if compression_type == CompressionType::Zstd {
                return Err(KafkaError::IllegalArgument(format!(
                    "ZStandard compression is not supported for magic {magic}"
                )));
            }
            if delete_horizon_ms != NO_TIMESTAMP {
                return Err(KafkaError::IllegalArgument(format!(
                    "Delete horizon timestamp is not supported for magic {magic}"
                )));
            }
        }

        let initial_position = buffer_stream.position();
        let batch_header_size_in_bytes = record_batch_header_size_in_bytes(magic, compression_type);

        // Reserve the header region so records start writing right after.
        buffer_stream.set_position(initial_position + batch_header_size_in_bytes as usize);

        // Move the stream onto the heap so its address is stable —
        // required for the self-referential `append_stream` field.
        let buffer_stream = Box::new(buffer_stream);

        let mut builder = MemoryRecordsBuilder {
            timestamp_type,
            compression_type,
            compression_level: None,
            buffer_stream,
            magic,
            initial_position,
            base_offset,
            log_append_time,
            is_control_batch,
            partition_leader_epoch,
            write_limit,
            batch_header_size_in_bytes,
            delete_horizon_ms,
            estimated_compression_ratio: 1.0,
            closed_for_appends: false,
            is_transactional,
            producer_id,
            producer_epoch,
            base_sequence,
            uncompressed_records_size_in_bytes: 0,
            num_records: 0,
            actual_compression_ratio: 1.0,
            max_timestamp: NO_TIMESTAMP,
            offset_of_max_timestamp: -1,
            last_offset: None,
            base_timestamp: None,
            built_records: None,
            aborted: false,
            append_stream: None,
        };
        if builder.has_delete_horizon_ms() {
            builder.base_timestamp = Some(delete_horizon_ms);
        }
        // Wrap the codec around `buffer_stream` for compressed batches.
        // Mirrors Java's MemoryRecordsBuilder.java:143:
        //   appendStream = new DataOutputStream(compression.wrapForOutput(this.bufferStream, magic));
        // For uncompressed batches we leave `append_stream = None` and
        // fast-path writes directly to `buffer_stream`; this avoids
        // boxing for the no-compression case (which is the producer fast
        // path).
        if compression_type != CompressionType::None {
            builder.install_append_stream();
        }
        Ok(builder)
    }

    /// Construct the persistent codec writer that streams compressed
    /// records into `buffer_stream`.
    ///
    /// SAFETY: We take a raw pointer to `self.buffer_stream`, manufacture
    /// an `&'static mut ByteBufferOutputStream` from it, and pass that
    /// into the codec's `wrap_for_output` to produce a
    /// `Box<dyn Write + 'static>`. The `'static` is a lie — the writer
    /// is only valid for as long as `self.buffer_stream` is. We uphold
    /// the invariant by:
    ///   * Storing the writer as a field (`append_stream`) that is
    ///     dropped *before* `buffer_stream` is moved (see `close()` and
    ///     `Drop`).
    ///   * Only invoking `Write` on the writer from `&mut self` methods,
    ///     so the borrow is exclusive while writes happen.
    ///   * Never letting the writer escape the builder.
    ///
    /// This mirrors Java's `appendStream` field at
    /// `MemoryRecordsBuilder.java:78` whose lifetime is bounded by the
    /// builder thanks to GC.
    fn install_append_stream(&mut self) {
        // Take the heap address of the boxed stream — stable across
        // moves of `self`. Auto-deref of `Box<ByteBufferOutputStream>`
        // gives us `&mut ByteBufferOutputStream` whose address we
        // capture as a raw pointer.
        let ptr: *mut ByteBufferOutputStream = &mut *self.buffer_stream;
        // SAFETY: see the function-level safety comment.
        let stream_ref: &'static mut ByteBufferOutputStream = unsafe { &mut *ptr };
        let writer: Box<dyn std::io::Write + 'static> =
            wrap_codec_for_output(self.compression_type, stream_ref, self.magic, self.compression_level);
        self.append_stream = Some(writer);
    }

    /// 13-arg constructor (no `delete_horizon_ms`); defaults to
    /// `NO_TIMESTAMP`. Mirrors Java's overload.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stream_no_delete_horizon(
        buffer_stream: ByteBufferOutputStream,
        magic: i8,
        compression_type: CompressionType,
        timestamp_type: TimestampType,
        base_offset: i64,
        log_append_time: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
        is_control_batch: bool,
        partition_leader_epoch: i32,
        write_limit: i32,
    ) -> Result<Self, KafkaError> {
        Self::from_stream(
            buffer_stream,
            magic,
            compression_type,
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
            NO_TIMESTAMP,
        )
    }

    /// Construct from a pre-sized owned `Vec<u8>` (the convenience entry
    /// equivalent to Java's `(ByteBuffer, ...)` constructor).
    ///
    /// The vec is moved in. The buffer's position starts at 0; the
    /// builder reserves the header region internally.
    #[allow(clippy::too_many_arguments)]
    pub fn from_buffer(
        buffer: Vec<u8>,
        magic: i8,
        compression_type: CompressionType,
        timestamp_type: TimestampType,
        base_offset: i64,
        log_append_time: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
        is_control_batch: bool,
        partition_leader_epoch: i32,
        write_limit: i32,
    ) -> Result<Self, KafkaError> {
        let stream = ByteBufferOutputStream::from_buffer(buffer);
        Self::from_stream_no_delete_horizon(
            stream,
            magic,
            compression_type,
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
        )
    }

    /// Borrow the underlying buffer up to its current limit. Mirrors Java's
    /// `buffer()` (which returns a duplicate; we return a slice view).
    pub fn buffer(&self) -> &[u8] {
        self.buffer_stream.buffer()
    }

    /// Initial capacity at construction. Mirrors Java's `initialCapacity()`.
    pub fn initial_capacity(&self) -> usize {
        self.buffer_stream.initial_capacity()
    }

    /// The actual compression ratio after `build()` (1.0 if no records or
    /// uncompressed). Mirrors Java's `compressionRatio()`.
    pub fn compression_ratio(&self) -> f64 {
        self.actual_compression_ratio as f64
    }

    /// Compression type configured for this builder. Mirrors Java's
    /// `compression()` (which returns a `Compression` instance — we expose
    /// just the type).
    pub fn compression(&self) -> CompressionType {
        self.compression_type
    }

    /// Whether this builder is producing a control batch. Mirrors Java's
    /// `isControlBatch()`.
    pub fn is_control_batch(&self) -> bool {
        self.is_control_batch
    }

    /// Whether this builder is producing a transactional batch. Mirrors
    /// Java's `isTransactional()`.
    pub fn is_transactional(&self) -> bool {
        self.is_transactional
    }

    /// Whether the batch carries a delete-horizon timestamp. Mirrors Java's
    /// `hasDeleteHorizonMs()`.
    pub fn has_delete_horizon_ms(&self) -> bool {
        self.magic >= MAGIC_VALUE_V2 && self.delete_horizon_ms >= 0
    }

    /// Build the resulting [`MemoryRecords`]. Mirrors Java's `build()`.
    ///
    /// # Errors
    ///
    /// * [`KafkaError::IllegalState`] when called after [`Self::abort`].
    /// * [`KafkaError::IllegalArgument`] when producer state is invalid
    ///   (transactional with no producer id, etc.) — Java throws on
    ///   `close()` which `build` calls into.
    pub fn build(&mut self) -> Result<MemoryRecords, KafkaError> {
        if self.aborted {
            return Err(KafkaError::IllegalState(
                "Attempting to build an aborted record batch".to_string(),
            ));
        }
        self.close()?;
        // Once `close` runs, `built_records` is set unless we hit an empty
        // batch which sets it to `MemoryRecords::EMPTY`.
        Ok(self.built_records.clone().unwrap_or_else(|| MemoryRecords::empty().clone()))
    }

    /// Java's `info()`. See the long doc-comment in the Java source for the
    /// case analysis.
    pub fn info(&self) -> RecordsInfo {
        if self.timestamp_type == TimestampType::LogAppendTime {
            if self.compression_type != CompressionType::None || self.magic >= MAGIC_VALUE_V2 {
                RecordsInfo::new(self.log_append_time, self.last_offset.unwrap_or(-1))
            } else {
                RecordsInfo::new(self.log_append_time, self.base_offset)
            }
        } else if self.max_timestamp == NO_TIMESTAMP {
            RecordsInfo::new(NO_TIMESTAMP, -1)
        } else if self.compression_type != CompressionType::None || self.magic >= MAGIC_VALUE_V2 {
            RecordsInfo::new(self.max_timestamp, self.last_offset.unwrap_or(-1))
        } else {
            RecordsInfo::new(self.max_timestamp, self.offset_of_max_timestamp)
        }
    }

    /// Number of records appended so far. Mirrors Java's `numRecords()`.
    pub fn num_records(&self) -> i32 {
        self.num_records
    }

    /// Sum of the batch header (uncompressed) and the records (uncompressed).
    /// Mirrors Java's `uncompressedBytesWritten()`.
    pub fn uncompressed_bytes_written(&self) -> i32 {
        self.uncompressed_records_size_in_bytes + self.batch_header_size_in_bytes
    }

    /// Update the producer state mid-batch. Mirrors Java's `setProducerState`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalState`] if the batch is already closed.
    pub fn set_producer_state(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
    ) -> Result<(), KafkaError> {
        if self.is_closed() {
            return Err(KafkaError::IllegalState(
                "Trying to set producer state of an already closed batch. This indicates a bug on the client."
                    .to_string(),
            ));
        }
        self.producer_id = producer_id;
        self.producer_epoch = producer_epoch;
        self.base_sequence = base_sequence;
        self.is_transactional = is_transactional;
        Ok(())
    }

    /// Override the last offset (used by log-compaction rebuild). Mirrors
    /// Java's `overrideLastOffset(long)`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalState`] if the batch is already built.
    pub fn override_last_offset(&mut self, last_offset: i64) -> Result<(), KafkaError> {
        if self.built_records.is_some() {
            return Err(KafkaError::IllegalState(
                "Cannot override the last offset after the records have been built".to_string(),
            ));
        }
        self.last_offset = Some(last_offset);
        Ok(())
    }

    /// Stop accepting record appends; flush any codec buffers. After this
    /// only batch-header writes are allowed. Mirrors Java's
    /// `closeForRecordAppends()`.
    pub fn close_for_record_appends(&mut self) {
        self.closed_for_appends = true;
    }

    /// Abort the in-progress batch. Mirrors Java's `abort()`.
    pub fn abort(&mut self) {
        // Drop the codec writer first to release the borrow on
        // buffer_stream before we mutate it.
        self.append_stream = None;
        self.close_for_record_appends();
        self.buffer_stream.set_position(self.initial_position);
        self.aborted = true;
    }

    /// Reopen a built batch with new producer state. Mirrors Java's
    /// `reopenAndRewriteProducerState`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalState`] if the batch was already
    /// aborted.
    pub fn reopen_and_rewrite_producer_state(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
    ) -> Result<(), KafkaError> {
        if self.aborted {
            return Err(KafkaError::IllegalState(
                "Should not reopen a batch which is already aborted.".to_string(),
            ));
        }
        self.built_records = None;
        self.producer_id = producer_id;
        self.producer_epoch = producer_epoch;
        self.base_sequence = base_sequence;
        self.is_transactional = is_transactional;
        Ok(())
    }

    /// Finalize the batch in place: flush the codec (if needed) and
    /// stamp the batch header. Mirrors Java's `close()`.
    ///
    /// # Errors
    ///
    /// * [`KafkaError::IllegalState`] when the builder was previously
    ///   aborted.
    /// * [`KafkaError::IllegalArgument`] when producer state is invalid.
    pub fn close(&mut self) -> Result<(), KafkaError> {
        if self.aborted {
            return Err(KafkaError::IllegalState(
                "Cannot close MemoryRecordsBuilder as it has already been aborted".to_string(),
            ));
        }
        if self.built_records.is_some() {
            return Ok(());
        }

        self.validate_producer_state()?;

        if self.num_records == 0 {
            // Mirrors Java: reset position and emit an empty MemoryRecords.
            // Drop the codec writer first to release the buffer_stream
            // borrow.
            self.append_stream = None;
            self.close_for_record_appends();
            self.buffer_stream.set_position(self.initial_position);
            self.built_records = Some(MemoryRecords::empty().clone());
            return Ok(());
        }

        // For compressed batches, flush the codec writer to emit any
        // trailing block (gzip/zstd require a final flush + finish).
        // Dropping the `Box<dyn Write>` runs the codec's destructor,
        // which finishes the compressed stream and writes the final
        // bytes into `buffer_stream`. Mirrors Java's
        // `MemoryRecordsBuilder.java:333-340`:
        //   if (appendStream != CLOSED_STREAM) {
        //       try { appendStream.close(); } catch (...) { ... }
        //   }
        if self.compression_type != CompressionType::None {
            // Explicit flush before drop so we surface I/O errors.
            if let Some(ref mut stream) = self.append_stream {
                std::io::Write::flush(stream).map_err(|e| {
                    KafkaError::Generic(format!("I/O exception when writing to the append stream, closing: {e}"))
                })?;
            }
            // Drop the writer; this finishes the compressed stream
            // (gzip/zstd emit the trailing footer here) and releases the
            // self-borrow on `buffer_stream` so the borrow checker's
            // invariant is upheld going forward.
            self.append_stream = None;
        }
        self.close_for_record_appends();

        if self.magic > MAGIC_VALUE_V0 + 1 {
            let written_compressed = self.write_default_batch_header()?;
            self.actual_compression_ratio = if self.uncompressed_records_size_in_bytes > 0 {
                written_compressed as f32 / self.uncompressed_records_size_in_bytes as f32
            } else {
                1.0
            };
        }

        // Materialize the final MemoryRecords. Per CLAUDE.md rule 12 and
        // the PLAN.md zero-copy DoD ("Batch finalization (build) computes
        // CRC + writes the header in place; it does not copy the
        // already-written record bytes"), we MOVE the underlying Vec out
        // of the bufferStream and wrap it in a `Bytes` (which is a free
        // `Vec<u8> -> Bytes` conversion — no payload copy).
        //
        // The bufferStream is replaced with an empty stub. After build()
        // the builder retains its scalar getters (info(), compressionRatio,
        // numRecords, etc.) and the `built_records` MemoryRecords; further
        // appends are rejected by `closed_for_appends`.
        let position = self.buffer_stream.position();
        let initial_position = self.initial_position;
        // Take ownership of the boxed stream and unbox it to extract the
        // underlying Vec. The stub box keeps the field valid while the
        // builder lives on (callers may still hit scalar getters after
        // build()).
        let boxed_stream =
            std::mem::replace(&mut self.buffer_stream, Box::new(ByteBufferOutputStream::with_capacity(0)));
        let mut owned = (*boxed_stream).into_buffer();
        // `owned.len() == position` after `into_buffer()` because the
        // stream's `into_buffer()` truncates to `position`.
        debug_assert_eq!(owned.len(), position);
        // Drop any prefix bytes before `initial_position` (they aren't
        // part of the new MemoryRecords). For the common case
        // `initial_position == 0` this is a no-op. Otherwise we drain the
        // prefix to keep zero-copy on the records section: the Vec's
        // backing allocation is preserved across `drain(0..initial_position)`.
        if initial_position > 0 {
            owned.drain(0..initial_position);
        }
        self.built_records = Some(MemoryRecords::readable_records_from_vec(owned));
        Ok(())
    }

    fn validate_producer_state(&self) -> Result<(), KafkaError> {
        if self.is_transactional && self.producer_id == NO_PRODUCER_ID {
            return Err(KafkaError::IllegalArgument(
                "Cannot write transactional messages without a valid producer ID".to_string(),
            ));
        }
        if self.producer_id != NO_PRODUCER_ID {
            if self.producer_epoch == NO_PRODUCER_EPOCH {
                return Err(KafkaError::IllegalArgument("Invalid negative producer epoch".to_string()));
            }
            if self.base_sequence < 0 && !self.is_control_batch {
                return Err(KafkaError::IllegalArgument("Invalid negative sequence number used".to_string()));
            }
            if self.magic < MAGIC_VALUE_V2 {
                return Err(KafkaError::IllegalArgument(format!(
                    "Idempotent messages are not supported for magic {}",
                    self.magic
                )));
            }
        }
        Ok(())
    }

    /// Write the v2 batch header in place at `initial_position`. Returns the
    /// compressed-records size (i.e., `pos - initial_position - RECORD_BATCH_OVERHEAD`).
    /// Mirrors Java's `writeDefaultBatchHeader`.
    fn write_default_batch_header(&mut self) -> Result<i32, KafkaError> {
        self.ensure_open_for_record_batch_write()?;
        let pos = self.buffer_stream.position();
        let size = (pos - self.initial_position) as i32;
        let written_compressed = size - RECORD_BATCH_OVERHEAD as i32;
        let last_offset = self.last_offset.unwrap_or(self.base_offset);
        let offset_delta = (last_offset - self.base_offset) as i32;
        let max_timestamp = if self.timestamp_type == TimestampType::LogAppendTime {
            self.log_append_time
        } else {
            self.max_timestamp
        };
        let base_timestamp = self.base_timestamp.unwrap_or(NO_TIMESTAMP);
        // Snapshot the immutable fields we need so we don't fight the
        // borrow checker over `self` while taking `&mut buffer_stream`.
        let initial_position = self.initial_position;
        let base_offset = self.base_offset;
        let magic = self.magic;
        let compression_type = self.compression_type;
        let timestamp_type = self.timestamp_type;
        let producer_id = self.producer_id;
        let producer_epoch = self.producer_epoch;
        let base_sequence = self.base_sequence;
        let is_transactional = self.is_transactional;
        let is_control_batch = self.is_control_batch;
        let has_delete_horizon_ms = self.has_delete_horizon_ms();
        let partition_leader_epoch = self.partition_leader_epoch;
        let num_records = self.num_records;

        write_header_at(
            self.buffer_stream.buffer_mut(),
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
            has_delete_horizon_ms,
            partition_leader_epoch,
            num_records,
        )?;

        Ok(written_compressed)
    }

    /// Append a record at the next sequential offset with a `SimpleRecord`.
    /// Mirrors Java's `append(SimpleRecord)`.
    pub fn append_simple(&mut self, record: &SimpleRecord) -> Result<(), KafkaError> {
        let offset = self.next_sequential_offset();
        self.append_with_offset_simple(offset, record)
    }

    /// Append a record at the next sequential offset.
    pub fn append(
        &mut self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> Result<(), KafkaError> {
        let offset = self.next_sequential_offset();
        self.append_with_offset(offset, false, timestamp, key, value, headers)
    }

    /// Convenience: append without headers. Mirrors Java's
    /// `append(long, byte[], byte[])`.
    pub fn append_no_headers(
        &mut self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<(), KafkaError> {
        self.append(timestamp, key, value, &[])
    }

    /// Append a record at the given offset with a `SimpleRecord`. Mirrors
    /// Java's `appendWithOffset(long, SimpleRecord)`.
    pub fn append_with_offset_simple(&mut self, offset: i64, record: &SimpleRecord) -> Result<(), KafkaError> {
        self.append_with_offset(
            offset,
            false,
            record.timestamp(),
            record.key(),
            record.value(),
            record.headers(),
        )
    }

    /// Append a record at the given offset. Mirrors Java's
    /// `appendWithOffset(long, long, byte[], byte[], Header[])`.
    pub fn append_with_offset_full(
        &mut self,
        offset: i64,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> Result<(), KafkaError> {
        self.append_with_offset(offset, false, timestamp, key, value, headers)
    }

    /// Internal append path. Mirrors Java's private
    /// `appendWithOffset(long, boolean, long, ByteBuffer, ByteBuffer, Header[])`.
    fn append_with_offset(
        &mut self,
        offset: i64,
        is_control_record: bool,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> Result<(), KafkaError> {
        if is_control_record != self.is_control_batch {
            return Err(KafkaError::IllegalArgument(
                "Control records can only be appended to control batches".to_string(),
            ));
        }
        if let Some(last) = self.last_offset
            && offset <= last
        {
            return Err(KafkaError::IllegalArgument(format!(
                "Illegal offset {offset} following previous offset {last} (Offsets must increase monotonically)."
            )));
        }
        if timestamp < 0 && timestamp != NO_TIMESTAMP {
            return Err(KafkaError::IllegalArgument(format!("Invalid negative timestamp {timestamp}")));
        }
        if self.magic < MAGIC_VALUE_V2 && !headers.is_empty() {
            return Err(KafkaError::IllegalArgument(format!(
                "Magic v{} does not support record headers",
                self.magic
            )));
        }

        if self.base_timestamp.is_none() {
            self.base_timestamp = Some(timestamp);
        }

        if self.magic > MAGIC_VALUE_V0 + 1 {
            self.append_default_record(offset, timestamp, key, value, headers)?;
        } else {
            return Err(KafkaError::IllegalArgument(format!(
                "Magic v{} is not supported in this implementation; only v2 records are produced",
                self.magic
            )));
        }
        Ok(())
    }

    fn append_default_record(
        &mut self,
        offset: i64,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> Result<(), KafkaError> {
        self.ensure_open_for_record_append()?;
        let offset_delta = (offset - self.base_offset) as i32;
        let timestamp_delta = timestamp - self.base_timestamp.unwrap_or(timestamp);

        let size_in_bytes = if self.compression_type == CompressionType::None {
            // Zero-copy fast path: write directly into the batch buffer.
            // `default_record::write_to_stream` writes through the
            // ByteBufferOutputStream's `Write` impl which extends the
            // underlying Vec<u8> in place — no per-record intermediate
            // buffer (CLAUDE.md rule 12 / PLAN.md zero-copy DoD).
            default_record::write_to_stream(
                &mut self.buffer_stream,
                offset_delta,
                timestamp_delta,
                key,
                value,
                headers,
            )?
        } else {
            // Streaming compressed path: write through the persistent
            // codec writer that was wrapped around `buffer_stream` at
            // construction. Mirrors Java's
            // `MemoryRecordsBuilder.java:763`:
            //   DefaultRecord.writeTo(appendStream, offsetDelta,
            //                         timestampDelta, key, value, headers)
            // Records flow straight from caller bytes through the codec
            // into `buffer_stream` — no intermediate `Vec<u8>` per
            // record, no batch-sized accumulator. The codec's stateful
            // compressor (gzip's deflate state, zstd's dictionary, etc.)
            // is preserved across appends.
            let stream = self
                .append_stream
                .as_mut()
                .expect("append_stream is Some for compressed builders");
            default_record::write_to_stream(stream, offset_delta, timestamp_delta, key, value, headers)?
        };
        self.record_written(offset, timestamp, size_in_bytes)
    }

    fn record_written(&mut self, offset: i64, timestamp: i64, size: i32) -> Result<(), KafkaError> {
        if self.num_records == i32::MAX {
            return Err(KafkaError::IllegalArgument(format!(
                "Maximum number of records per batch exceeded, max records: {}",
                i32::MAX
            )));
        }
        if offset.checked_sub(self.base_offset).is_none_or(|d| d > i32::MAX as i64) {
            return Err(KafkaError::IllegalArgument(format!(
                "Maximum offset delta exceeded, base offset: {}, last offset: {}",
                self.base_offset, offset
            )));
        }

        self.num_records += 1;
        self.uncompressed_records_size_in_bytes += size;
        self.last_offset = Some(offset);

        if self.magic > MAGIC_VALUE_V0 && timestamp > self.max_timestamp {
            self.max_timestamp = timestamp;
            self.offset_of_max_timestamp = offset;
        }
        Ok(())
    }

    fn ensure_open_for_record_append(&self) -> Result<(), KafkaError> {
        if self.closed_for_appends {
            return Err(KafkaError::IllegalState(
                "Tried to append a record, but MemoryRecordsBuilder is closed for record appends".to_string(),
            ));
        }
        Ok(())
    }

    fn ensure_open_for_record_batch_write(&self) -> Result<(), KafkaError> {
        if self.is_closed() {
            return Err(KafkaError::IllegalState(
                "Tried to write record batch header, but MemoryRecordsBuilder is closed".to_string(),
            ));
        }
        if self.aborted {
            return Err(KafkaError::IllegalState(
                "Tried to write record batch header, but MemoryRecordsBuilder is aborted".to_string(),
            ));
        }
        Ok(())
    }

    /// Estimate of bytes written to the underlying buffer. Mirrors Java's
    /// private `estimatedBytesWritten`.
    fn estimated_bytes_written(&self) -> i32 {
        if self.compression_type == CompressionType::None {
            self.batch_header_size_in_bytes + self.uncompressed_records_size_in_bytes
        } else {
            self.batch_header_size_in_bytes
                + (self.uncompressed_records_size_in_bytes as f32
                    * self.estimated_compression_ratio
                    * COMPRESSION_RATE_ESTIMATION_FACTOR) as i32
        }
    }

    /// Set the estimated compression ratio. Mirrors Java's
    /// `setEstimatedCompressionRatio`.
    pub fn set_estimated_compression_ratio(&mut self, ratio: f32) {
        self.estimated_compression_ratio = ratio;
    }

    /// Check if there is room for a new record. Mirrors Java's
    /// `hasRoomFor(long, byte[], byte[], Header[])`.
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
        if self.num_records == 0 {
            return true;
        }
        let next_offset_delta = self.last_offset.map_or(0, |last| (last - self.base_offset + 1) as i32);
        let timestamp_delta = self.base_timestamp.map_or(0, |bt| timestamp - bt);
        let record_size = if self.magic >= MAGIC_VALUE_V2 {
            default_record::size_in_bytes(next_offset_delta, timestamp_delta, key, value, headers)
        } else {
            // Phase 3 only supports v2 writes; for v0/v1 we conservatively
            // return false — Java handles this via LegacyRecord which is
            // out of scope for the producer path.
            return false;
        };
        self.write_limit >= self.estimated_bytes_written() + record_size
    }

    /// Check if there is room for an estimated record size. Mirrors Java's
    /// `hasRoomFor(int)`.
    pub fn has_room_for_size(&self, estimated_records_size: i32) -> bool {
        if self.is_full() {
            return false;
        }
        self.write_limit >= self.estimated_bytes_written() + estimated_records_size
    }

    /// Maximum allowed record bytes after the header. Mirrors Java's
    /// `maxAllowedBytes()`.
    pub fn max_allowed_bytes(&self) -> i32 {
        self.write_limit - self.batch_header_size_in_bytes
    }

    /// Whether `build()` has run. Mirrors Java's `isClosed()`.
    pub fn is_closed(&self) -> bool {
        self.built_records.is_some()
    }

    /// Whether the builder is full or the codec is closed. Mirrors Java's
    /// `isFull()`.
    pub fn is_full(&self) -> bool {
        self.closed_for_appends || (self.num_records > 0 && self.write_limit <= self.estimated_bytes_written())
    }

    /// Estimate of bytes written; exact when uncompressed or after close.
    /// Mirrors Java's `estimatedSizeInBytes()`.
    pub fn estimated_size_in_bytes(&self) -> i32 {
        if let Some(ref records) = self.built_records {
            <MemoryRecords as crate::common::record::BaseRecords>::size_in_bytes(records)
        } else {
            self.estimated_bytes_written()
        }
    }

    /// Magic byte for this batch. Mirrors Java's `magic()`.
    pub fn magic(&self) -> i8 {
        self.magic
    }

    fn next_sequential_offset(&self) -> i64 {
        self.last_offset.map_or(self.base_offset, |last| last + 1)
    }

    /// Producer id of the resulting batch. Mirrors Java's `producerId()`.
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Producer epoch of the resulting batch. Mirrors Java's `producerEpoch()`.
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Base sequence of the resulting batch. Mirrors Java's `baseSequence()`.
    pub fn base_sequence(&self) -> i32 {
        self.base_sequence
    }
}

impl Drop for MemoryRecordsBuilder {
    /// SAFETY: The codec writer in `append_stream` holds a raw pointer
    /// to `buffer_stream`. Rust drops fields in declaration order, but
    /// `buffer_stream` is declared *before* `append_stream`, so without
    /// this explicit drop the writer would briefly outlive its referent.
    /// We force the writer to drop first here, then the rest of the
    /// fields drop in their declared order.
    fn drop(&mut self) {
        self.append_stream = None;
    }
}

/// Java's `AbstractRecords.recordBatchHeaderSizeInBytes(byte, CompressionType)`.
/// Phase 3 produces magic-v2 batches; this returns
/// [`RECORD_BATCH_OVERHEAD`] for v2. v0/v1 paths return 0/`LOG_OVERHEAD`
/// for parity with Java's behavior; the builder rejects v0/v1 writes
/// anyway.
pub(crate) fn record_batch_header_size_in_bytes(magic: i8, compression_type: CompressionType) -> i32 {
    if magic > MAGIC_VALUE_V0 + 1 {
        RECORD_BATCH_OVERHEAD as i32
    } else if compression_type != CompressionType::None {
        // Java: `Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic)`.
        // Phase 3 doesn't ship LegacyRecord; the v0/v1 producer path is
        // out of scope. Returning LOG_OVERHEAD here keeps `hasRoomFor`
        // estimates conservative for v1 callers.
        LOG_OVERHEAD as i32
    } else {
        0
    }
}

fn wrap_codec_for_output<'a>(
    compression_type: CompressionType,
    buffer_stream: &'a mut ByteBufferOutputStream,
    magic: i8,
    _level: Option<i32>,
) -> Box<dyn std::io::Write + 'a> {
    // Phase 3d-4 always wraps with the codec's default level; explicit
    // levels live in the producer config layer (Phase 7+).
    match compression_type {
        CompressionType::None => NoCompression::new().wrap_for_output(buffer_stream, magic),
        CompressionType::Gzip => GzipCompression::default().wrap_for_output(buffer_stream, magic),
        CompressionType::Snappy => SnappyCompression::new().wrap_for_output(buffer_stream, magic),
        CompressionType::Lz4 => Lz4Compression::default().wrap_for_output(buffer_stream, magic),
        CompressionType::Zstd => ZstdCompression::default().wrap_for_output(buffer_stream, magic),
    }
}

// `MemoryRecords::with_records` factory variants live in `memory_records.rs`
// (mirroring Java's static factories on `MemoryRecords`). They are wired
// to call into this builder.

#[cfg(test)]
mod tests {
    //! Translation of `MemoryRecordsBuilderTest.java`. Java parameterizes
    //! tests over `(bufferOffset, compression, magic)`; we run the
    //! v2-specific subset across the 5 codec variants.
    //!
    //! Java tests deferred (need legacy v0/v1 records, control records,
    //! filterTo, end-txn-marker, leader-change-message) per Phase 3d-4
    //! scope:
    //!
    //! * `testLegacyCompressionRate` — magic v0/v1 only, no v2 path
    //! * `testWriteLeaderChangeControlBatch` — needs LeaderChangeMessage
    //!   (KRaft control record) — Phase 7+
    //! * `testRecordTimestampsWithDeleteHorizon` — needs
    //!   DeleteHorizon-aware streaming iterator (Phase 6+)
    //!
    //! All other `@Test` and `@ParameterizedTest` cases are translated.

    use super::*;
    use crate::common::record::Records as RecordsTrait;
    use crate::common::record::record_batch::{CURRENT_MAGIC_VALUE, NO_PARTITION_LEADER_EPOCH};

    fn allocate_buffer(size: usize, buffer_offset: usize) -> ByteBufferOutputStream {
        let buf = vec![0u8; size];
        let mut stream = ByteBufferOutputStream::from_buffer(buf);
        stream.set_position(buffer_offset);
        stream
    }

    /// Run a closure for each compression codec and bufferOffset that the
    /// Java `MemoryRecordsBuilderArgumentsProvider` would emit for v2.
    fn for_each_v2_args<F: FnMut(usize, CompressionType)>(mut f: F) {
        for buffer_offset in [0usize, 15] {
            for compression in [
                CompressionType::None,
                CompressionType::Gzip,
                CompressionType::Snappy,
                CompressionType::Lz4,
                CompressionType::Zstd,
            ] {
                f(buffer_offset, compression);
            }
        }
    }

    /// Java: `testUnsupportedCompress`. Magic v0/v1 + ZSTD must error.
    #[test]
    fn unsupported_compress_zstd_on_legacy_magic() {
        for magic in [MAGIC_VALUE_V0, MAGIC_VALUE_V0 + 1] {
            let stream = ByteBufferOutputStream::with_capacity(128);
            let result = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                magic,
                CompressionType::Zstd,
                TimestampType::CreateTime,
                0,
                0,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                128,
            );
            let err = match result {
                Err(e) => e,
                Ok(_) => panic!("expected IllegalArgument for ZSTD on magic {magic}"),
            };
            assert!(matches!(err, KafkaError::IllegalArgument(_)));
            assert!(
                err.to_string()
                    .contains(&format!("ZStandard compression is not supported for magic {magic}"))
            );
        }
    }

    /// Java: `testWriteEmptyRecordSet` — empty batch returns 0 size and
    /// resets buffer position to the initial offset.
    #[test]
    fn write_empty_record_set() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(128, buffer_offset);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                0,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                128,
            )
            .unwrap();
            let records = builder.build().unwrap();
            assert_eq!(
                <MemoryRecords as crate::common::record::BaseRecords>::size_in_bytes(&records),
                0
            );
        });
    }

    /// Java: `testWriteTransactionalRecordSet` (v2 portion).
    #[test]
    fn write_transactional_record_set_v2() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(128, buffer_offset);
            let pid: i64 = 9809;
            let epoch: i16 = 15;
            let sequence: i32 = 2342;
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                0,
                pid,
                epoch,
                sequence,
                true,
                false,
                NO_PARTITION_LEADER_EPOCH,
                128,
            )
            .unwrap();
            builder.append(0, Some(b"foo"), Some(b"bar"), &[]).unwrap();
            let records = builder.build().unwrap();
            let batches: Vec<_> = records.batches().map(|r| r.unwrap()).collect();
            assert_eq!(batches.len(), 1);
            assert!(batches[0].is_transactional());
        });
    }

    /// Java: `testWriteTransactionalWithInvalidPID`.
    #[test]
    fn write_transactional_with_invalid_pid() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(128, buffer_offset);
            let pid: i64 = NO_PRODUCER_ID;
            let epoch: i16 = 15;
            let sequence: i32 = 2342;
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                0,
                pid,
                epoch,
                sequence,
                true,
                false,
                NO_PARTITION_LEADER_EPOCH,
                128,
            )
            .unwrap();
            // Append at least one record so close() validates producer state.
            builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap();
            assert!(matches!(builder.close(), Err(KafkaError::IllegalArgument(_))));
        });
    }

    /// Java: `testWriteIdempotentWithInvalidEpoch`.
    #[test]
    fn write_idempotent_with_invalid_epoch() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(128, buffer_offset);
            let pid: i64 = 9809;
            let epoch: i16 = NO_PRODUCER_EPOCH;
            let sequence: i32 = 2342;
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                0,
                pid,
                epoch,
                sequence,
                true,
                false,
                NO_PARTITION_LEADER_EPOCH,
                128,
            )
            .unwrap();
            builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap();
            assert!(matches!(builder.close(), Err(KafkaError::IllegalArgument(_))));
        });
    }

    /// Java: `testWriteIdempotentWithInvalidBaseSequence`.
    #[test]
    fn write_idempotent_with_invalid_base_sequence() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(128, buffer_offset);
            let pid: i64 = 9809;
            let epoch: i16 = 15;
            let sequence: i32 = crate::common::record::record_batch::NO_SEQUENCE;
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                0,
                pid,
                epoch,
                sequence,
                true,
                false,
                NO_PARTITION_LEADER_EPOCH,
                128,
            )
            .unwrap();
            builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap();
            assert!(matches!(builder.close(), Err(KafkaError::IllegalArgument(_))));
        });
    }

    /// Java: `testEstimatedSizeInBytes` (v2 path). Each append grows the
    /// estimate; after build the estimate equals records.sizeInBytes().
    #[test]
    fn estimated_size_in_bytes_grows_per_append() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(1024, buffer_offset);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                0,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                1024,
            )
            .unwrap();
            let mut previous = 0;
            for i in 0..10 {
                let key = format!("{i}");
                builder.append(i as i64, None, Some(key.as_bytes()), &[]).unwrap();
                let current = builder.estimated_size_in_bytes();
                assert!(
                    current > previous,
                    "estimate must grow per append (compression={compression:?})"
                );
                previous = current;
            }
            let bytes_before_close = builder.estimated_size_in_bytes();
            let records = builder.build().unwrap();
            let actual_size = <MemoryRecords as crate::common::record::BaseRecords>::size_in_bytes(&records);
            assert_eq!(actual_size, builder.estimated_size_in_bytes());
            if compression == CompressionType::None {
                assert_eq!(actual_size, bytes_before_close);
            }
        });
    }

    /// Java: `buildUsingLogAppendTime` (v2 path).
    #[test]
    fn build_using_log_append_time() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(1024, buffer_offset);
            let log_append_time: i64 = 1_700_000_000_000;
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::LogAppendTime,
                0,
                log_append_time,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                1024,
            )
            .unwrap();
            builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap();
            builder.append(0, Some(b"b"), Some(b"2"), &[]).unwrap();
            builder.append(0, Some(b"c"), Some(b"3"), &[]).unwrap();
            let records = builder.build().unwrap();
            let info = builder.info();
            assert_eq!(info.max_timestamp, log_append_time);
            // For v2 (with or without compression), shallowOffsetOfMaxTimestamp = lastOffset = 2.
            assert_eq!(info.shallow_offset_of_max_timestamp, 2);
            for batch in records.batches().map(|r| r.unwrap()) {
                assert_eq!(batch.timestamp_type(), TimestampType::LogAppendTime);
                for record in batch.iter().map(|r| r.unwrap()) {
                    assert_eq!(record.timestamp(), log_append_time);
                }
            }
        });
    }

    /// Java: `buildUsingCreateTime` (v2 path).
    #[test]
    fn build_using_create_time() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(1024, buffer_offset);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                0,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                1024,
            )
            .unwrap();
            builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap();
            builder.append(2, Some(b"b"), Some(b"2"), &[]).unwrap();
            builder.append(1, Some(b"c"), Some(b"3"), &[]).unwrap();
            let records = builder.build().unwrap();
            let info = builder.info();
            assert_eq!(info.max_timestamp, 2);
            // v2: shallowOffsetOfMaxTimestamp = lastOffset = 2 (or compressed, also 2).
            assert_eq!(info.shallow_offset_of_max_timestamp, 2);
            let expected_timestamps = [0i64, 2, 1];
            let mut idx = 0;
            for batch in records.batches().map(|r| r.unwrap()) {
                assert_eq!(batch.timestamp_type(), TimestampType::CreateTime);
                for record in batch.iter().map(|r| r.unwrap()) {
                    assert_eq!(record.timestamp(), expected_timestamps[idx]);
                    idx += 1;
                }
            }
        });
    }

    /// Java: `testAppendedChecksumConsistency`.
    #[test]
    fn appended_checksum_consistency() {
        for_each_v2_args(|_buffer_offset, compression| {
            let stream = ByteBufferOutputStream::with_capacity(512);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                NO_TIMESTAMP,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                512,
            )
            .unwrap();
            builder.append(1, Some(b"key"), Some(b"value"), &[]).unwrap();
            let records = builder.build().unwrap();
            let count = records.records().count();
            assert_eq!(count, 1);
        });
    }

    /// Java: `testSmallWriteLimit`. Even with writeLimit=0 we always allow
    /// the first record; subsequent appends are rejected by hasRoomFor.
    #[test]
    fn small_write_limit_allows_first_record() {
        for_each_v2_args(|_buffer_offset, compression| {
            let key = b"foo";
            let value = b"bar";
            let stream = ByteBufferOutputStream::with_capacity(512);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                NO_TIMESTAMP,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                0, // writeLimit = 0
            )
            .unwrap();
            assert!(!builder.is_full());
            assert!(builder.has_room_for(0, Some(key), Some(value), &[]));
            builder.append(0, Some(key), Some(value), &[]).unwrap();
            assert!(builder.is_full());
            assert!(!builder.has_room_for(0, Some(key), Some(value), &[]));
            let records = builder.build().unwrap();
            let recs: Vec<_> = records.records().collect();
            assert_eq!(recs.len(), 1);
            assert_eq!(recs[0].key(), Some(key.as_slice()));
            assert_eq!(recs[0].value(), Some(value.as_slice()));
        });
    }

    /// Java: `writePastLimit`.
    #[test]
    fn write_past_limit() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(64, buffer_offset);
            let log_append_time: i64 = 1_700_000_000_000;
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                log_append_time,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                64,
            )
            .unwrap();
            builder.set_estimated_compression_ratio(0.5);
            builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap();
            builder.append(1, Some(b"b"), Some(b"2"), &[]).unwrap();
            // hasRoomFor returns false (limit reached), but append still
            // succeeds (Java semantics: hasRoomFor is advisory).
            assert!(!builder.has_room_for(2, Some(b"c"), Some(b"3"), &[]));
            builder.append(2, Some(b"c"), Some(b"3"), &[]).unwrap();
            let records = builder.build().unwrap();
            let info = builder.info();
            assert_eq!(info.max_timestamp, 2);
            assert_eq!(info.shallow_offset_of_max_timestamp, 2);
            let mut i = 0i64;
            for batch in records.batches().map(|r| r.unwrap()) {
                assert_eq!(batch.timestamp_type(), TimestampType::CreateTime);
                for record in batch.iter().map(|r| r.unwrap()) {
                    assert_eq!(record.timestamp(), i);
                    i += 1;
                }
            }
        });
    }

    /// Java: `testAppendAtInvalidOffset`. Subsequent appends at the same
    /// offset must error.
    #[test]
    fn append_at_invalid_offset_rejected() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(1024, buffer_offset);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                1_700_000_000_000,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                1024,
            )
            .unwrap();
            builder
                .append_with_offset_full(0, 1_700_000_000_000, Some(b"a"), None, &[])
                .unwrap();
            let err = builder
                .append_with_offset_full(0, 1_700_000_000_000, Some(b"b"), None, &[])
                .unwrap_err();
            assert!(matches!(err, KafkaError::IllegalArgument(_)));
        });
    }

    /// Java: `shouldThrowIllegalStateExceptionOnBuildWhenAborted`.
    #[test]
    fn build_when_aborted_errors() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(128, buffer_offset);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                0,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                128,
            )
            .unwrap();
            builder.abort();
            assert!(matches!(builder.build(), Err(KafkaError::IllegalState(_))));
        });
    }

    /// Java: `shouldResetBufferToInitialPositionOnAbort`.
    #[test]
    fn abort_resets_buffer_position() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(128, buffer_offset);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                0,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                128,
            )
            .unwrap();
            builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap();
            builder.abort();
            assert_eq!(builder.buffer_stream.position(), buffer_offset);
        });
    }

    /// Java: `shouldThrowIllegalStateExceptionOnCloseWhenAborted`.
    #[test]
    fn close_when_aborted_errors() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(128, buffer_offset);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                0,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                128,
            )
            .unwrap();
            builder.abort();
            assert!(matches!(builder.close(), Err(KafkaError::IllegalState(_))));
        });
    }

    /// Java: `shouldThrowIllegalStateExceptionOnAppendWhenAborted`. Java
    /// expects the append itself to throw; in our design `abort()` sets
    /// `closed_for_appends`, so the next append errors with
    /// IllegalState.
    #[test]
    fn append_when_aborted_errors() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(128, buffer_offset);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                0,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                128,
            )
            .unwrap();
            builder.abort();
            let err = builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap_err();
            assert!(matches!(err, KafkaError::IllegalState(_)));
        });
    }

    /// Java: `shouldThrowIllegalStateExceptionOnAppendWhenClosed`. After
    /// `build()` the next `append` errors with the exact message from
    /// Java.
    #[test]
    fn append_when_closed_errors_with_specific_message() {
        for_each_v2_args(|buffer_offset, compression| {
            let stream = allocate_buffer(128, buffer_offset);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                0,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                128,
            )
            .unwrap();
            builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap();
            builder.build().unwrap();
            let err = builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap_err();
            assert!(
                err.to_string()
                    .contains("Tried to append a record, but MemoryRecordsBuilder is closed for record appends")
            );
        });
    }

    // -----------------------------------------------------------------------
    // Translation of the `MemoryRecordsTest` cases that 3d-3 deferred to
    // 3d-4 (need the builder).
    // -----------------------------------------------------------------------

    /// Java: `MemoryRecordsTest.testIterator` (v2 portion).
    #[test]
    fn iterator_v2() {
        for compression in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            // V2 with a producer id triggers idempotent path.
            let pid: i64 = 134_234_234;
            let epoch: i16 = 28;
            let first_sequence: i32 = 682;
            let first_offset: i64 = 1234;
            let partition_leader_epoch: i32 = 998;
            let log_append_time: i64 = 1_700_000_000_000;
            let stream = ByteBufferOutputStream::with_capacity(1024);
            let records_in = [
                SimpleRecord::new_from_slice(1, Some(b"a"), Some(b"1"), &[]),
                SimpleRecord::new_from_slice(2, Some(b"b"), Some(b"2"), &[]),
                SimpleRecord::new_from_slice(3, Some(b"c"), Some(b"3"), &[]),
                SimpleRecord::new_from_slice(4, None, Some(b"4"), &[]),
                SimpleRecord::new_from_slice(5, Some(b"d"), None, &[]),
                SimpleRecord::new_from_slice(6, None, None, &[]),
            ];
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                first_offset,
                log_append_time,
                pid,
                epoch,
                first_sequence,
                false,
                false,
                partition_leader_epoch,
                1024,
            )
            .unwrap();
            for r in &records_in {
                builder.append_simple(r).unwrap();
            }
            let memory_records = builder.build().unwrap();
            // Iterate twice (matches Java loop).
            for _iter in 0..2 {
                let mut total = 0;
                for batch in memory_records.batches().map(|r| r.unwrap()) {
                    assert!(batch.is_valid());
                    assert_eq!(batch.compression_type(), compression);
                    assert_eq!(batch.base_offset(), first_offset + total);
                    assert_eq!(batch.producer_id(), pid);
                    assert_eq!(batch.producer_epoch(), epoch);
                    assert_eq!(batch.base_sequence(), first_sequence + total as i32);
                    assert_eq!(batch.partition_leader_epoch(), partition_leader_epoch);
                    assert_eq!(batch.count_or_null(), Some(records_in.len() as i32));
                    assert_eq!(batch.timestamp_type(), TimestampType::CreateTime);
                    assert_eq!(batch.max_timestamp(), records_in[records_in.len() - 1].timestamp());
                    let mut record_count = 0;
                    for record in batch.iter().map(|r| r.unwrap()) {
                        record.ensure_valid().unwrap();
                        assert!(record.has_magic(batch.magic()));
                        assert!(!record.is_compressed());
                        assert_eq!(record.offset(), first_offset + total);
                        assert_eq!(record.key(), records_in[total as usize].key());
                        assert_eq!(record.value(), records_in[total as usize].value());
                        assert_eq!(record.sequence(), first_sequence + total as i32);
                        total += 1;
                        record_count += 1;
                    }
                    assert_eq!(batch.last_offset(), batch.base_offset() + record_count as i64 - 1);
                }
                assert_eq!(total as usize, records_in.len());
            }
        }
    }

    /// Java: `MemoryRecordsTest.testHasRoomForMethod`.
    #[test]
    fn has_room_for_method() {
        for compression in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let stream = ByteBufferOutputStream::with_capacity(1024);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                NO_TIMESTAMP,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                1024,
            )
            .unwrap();
            builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap();
            assert!(builder.has_room_for(1, Some(b"b"), Some(b"2"), &[]));
            builder.close().unwrap();
            assert!(!builder.has_room_for(1, Some(b"b"), Some(b"2"), &[]));
        }
    }

    /// Java: `MemoryRecordsTest.testHasRoomForMethodWithHeaders` (v2 only).
    #[test]
    fn has_room_for_method_with_headers_v2() {
        for compression in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let stream = ByteBufferOutputStream::with_capacity(120);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::CreateTime,
                0,
                1_700_000_000_000,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                120,
            )
            .unwrap();
            builder.append(1_700_000_000_000, Some(b"key"), Some(b"value"), &[]).unwrap();
            let mut headers: Vec<RecordHeader> = Vec::new();
            for _ in 0..10 {
                headers.push(RecordHeader::new("hello", Some(b"world.world")));
            }
            assert!(builder.has_room_for(1_700_000_000_000, Some(b"key"), Some(b"value"), &[]));
            assert!(!builder.has_room_for(1_700_000_000_000, Some(b"key"), Some(b"value"), &headers));
        }
    }

    /// Java: `MemoryRecordsTest.testWithRecords`.
    #[test]
    fn with_records_via_factory() {
        for compression in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let records_in = [SimpleRecord::new_from_slice(10, Some(b"key1"), Some(b"value1"), &[])];
            let memory_records = MemoryRecords::with_records(
                CURRENT_MAGIC_VALUE,
                0,
                compression,
                TimestampType::CreateTime,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                NO_PARTITION_LEADER_EPOCH,
                false,
                &records_in,
            )
            .unwrap();
            let first_batch = memory_records.batches().next().unwrap().unwrap();
            let first_record = first_batch.iter().next().unwrap().unwrap();
            assert_eq!(first_record.key(), Some(b"key1".as_slice()));
        }
    }

    /// Java: `MemoryRecordsTest.testUnsupportedCompress`. v0/v1 + ZSTD via
    /// `withRecords` factory.
    #[test]
    fn with_records_unsupported_compress_zstd_on_legacy_magic() {
        for magic in [MAGIC_VALUE_V0, MAGIC_VALUE_V0 + 1] {
            let records_in = [SimpleRecord::new_from_slice(10, Some(b"key1"), Some(b"value1"), &[])];
            let err = MemoryRecords::with_records(
                magic,
                0,
                CompressionType::Zstd,
                TimestampType::CreateTime,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                NO_PARTITION_LEADER_EPOCH,
                false,
                &records_in,
            )
            .unwrap_err();
            assert!(matches!(err, KafkaError::IllegalArgument(_)));
            assert!(
                err.to_string()
                    .contains(&format!("ZStandard compression is not supported for magic {magic}"))
            );
        }
    }

    /// Java: `MemoryRecordsTest.testNextBatchSize` (builder-construction half).
    /// Phase 3d-3 translated the buffer-limit half (read-path subset);
    /// this exercises the builder-construction round-trip from append →
    /// build → firstBatchSize.
    #[test]
    fn next_batch_size_via_builder() {
        for compression in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let stream = ByteBufferOutputStream::with_capacity(2048);
            let log_append_time: i64 = 1_700_000_000_000;
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                compression,
                TimestampType::LogAppendTime,
                0,
                log_append_time,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                2048,
            )
            .unwrap();
            builder.append(10, None, Some(b"abc"), &[]).unwrap();
            let records = builder.build().unwrap();
            let size = <MemoryRecords as crate::common::record::BaseRecords>::size_in_bytes(&records);
            // Java: `assertEquals(size, records.firstBatchSize().intValue())`.
            assert_eq!(records.first_batch_size().unwrap(), Some(size));
        }
    }

    /// Java: `MemoryRecordsTest.testChecksum` byte-level checksum lock for
    /// magic v2. Locks a known checksum value against a fixed input set.
    /// Mirrors Java's hard-coded `expectedChecksum = 3851219455L` (uncompressed)
    /// and `2745969314L` (LZ4) for v2.
    ///
    /// Per Java's exact test setup: `withRecords(magic, compression,
    /// SimpleRecord(283843L, "key1"=>val), SimpleRecord(1234L, "key2"=>val))`,
    /// with `initialOffset = 0L`, all defaults. We assert the same.
    #[test]
    fn checksum_v2_uncompressed_matches_java() {
        // Java's `withRecords(byte magic, Compression compression,
        // SimpleRecord... records)` flows through the parameterized factory
        // with magic = MAGIC_VALUE_V2, initialOffset = 0L,
        // timestampType = CREATE_TIME.
        let records_in = [
            SimpleRecord::new_from_slice(283843, Some(b"key1"), Some(b"value1"), &[]),
            SimpleRecord::new_from_slice(1234, Some(b"key2"), Some(b"value2"), &[]),
        ];
        let memory_records = MemoryRecords::with_records_default(CompressionType::None, &records_in).unwrap();
        let batch = memory_records.batches().next().unwrap().unwrap();
        // Java uses `(long) batch.checksum()` returning the unsigned int
        // checksum widened to long. Our `checksum()` returns `i64` already
        // upcast.
        assert_eq!(batch.checksum(), 3_851_219_455_i64);
    }

    // -----------------------------------------------------------------------
    // Zero-copy DoD verification: see CLAUDE.md rule 12 + PLAN.md
    // -----------------------------------------------------------------------

    /// Per PLAN.md zero-copy DoD: for the no-compression path,
    /// `MemoryRecordsBuilder::append` writes directly into the batch
    /// buffer with no per-record intermediate Vec<u8> and no realloc-copy
    /// during build.
    ///
    /// Verification approach: capture the underlying buffer's address
    /// before any append, append two records, build, and verify the
    /// final `MemoryRecords::buffer()` slice has the same `as_ptr()` as
    /// the captured pre-build address — proving the records' backing
    /// allocation was MOVED into the MemoryRecords (not copied).
    #[test]
    fn append_writes_directly_into_batch_buffer_uncompressed() {
        // Pre-allocate enough capacity that no growth happens during
        // append. A 2KB buffer is way more than enough for two
        // small records.
        let stream = ByteBufferOutputStream::with_capacity(2048);
        let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
            stream,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            crate::common::record::record_batch::NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            2048,
        )
        .unwrap();
        // Capture buffer pointer BEFORE any append, to verify no realloc
        // happens during the append + build cycle.
        let initial_ptr = builder.buffer_stream.buffer().as_ptr();
        // Capture the starting position right after construction (header
        // region is reserved). Subsequent appends should write at this
        // offset and onwards.
        let records_start_offset = builder.buffer_stream.position();
        assert_eq!(records_start_offset, RECORD_BATCH_OVERHEAD);

        builder.append(1, Some(b"k1"), Some(b"v1"), &[]).unwrap();
        builder.append(2, Some(b"k2"), Some(b"v2"), &[]).unwrap();

        // After append, the buffer pointer must NOT have changed (no
        // realloc since we pre-sized).
        let post_append_ptr = builder.buffer_stream.buffer().as_ptr();
        assert_eq!(initial_ptr, post_append_ptr, "buffer must not have reallocated during append");

        let built = builder.build().unwrap();
        // PLAN.md DoD: MemoryRecords::buffer()'s as_ptr() MUST equal
        // the captured pre-build pointer — proving the underlying Vec
        // allocation was MOVED (not copied) into the final MemoryRecords.
        // Note: this test uses `initial_position == 0` so no prefix
        // drain happens; for `initial_position > 0` the drain shifts
        // the pointer (handled correctly via Vec::drain semantics).
        let built_ptr = built.buffer().as_ptr();
        assert_eq!(
            built_ptr, initial_ptr,
            "MemoryRecords::buffer() must alias the original bufferStream allocation \
             (zero-copy build per CLAUDE.md rule 12 / PLAN.md zero-copy DoD)"
        );
    }

    /// Companion zero-copy test: for `initial_position > 0`, the
    /// MemoryRecords' bytes still come from the original allocation
    /// (Vec::drain preserves the backing alloc); the byte-content is
    /// the records section after the drained prefix.
    #[test]
    fn append_writes_directly_into_batch_buffer_with_initial_offset() {
        let mut stream = ByteBufferOutputStream::with_capacity(2048);
        stream.set_position(15); // matches the 'bufferOffset = 15' Java arg
        let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
            stream,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            crate::common::record::record_batch::NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            2048,
        )
        .unwrap();
        let initial_alloc_ptr = builder.buffer_stream.buffer().as_ptr();
        builder.append(1, Some(b"k1"), Some(b"v1"), &[]).unwrap();
        builder.append(2, Some(b"k2"), Some(b"v2"), &[]).unwrap();
        let built = builder.build().unwrap();
        // After Vec::drain(0..15), the Vec's data pointer is the same
        // (drain shifts elements left, preserving the allocation).
        let built_ptr = built.buffer().as_ptr();
        assert_eq!(
            built_ptr, initial_alloc_ptr,
            "Vec::drain must preserve allocation; MemoryRecords::buffer() aliases the original alloc"
        );
        // Sanity: the size matches just the records section (everything
        // from initial_position onwards).
        assert!(<MemoryRecords as crate::common::record::BaseRecords>::size_in_bytes(&built) > 0);
    }

    /// Issue #20 fix verification: for compressed batches, records must
    /// flow through the codec writer directly into `buffer_stream` as
    /// they are appended — there must NOT be an intermediate per-batch
    /// `Vec<u8>` accumulator that's compressed in one shot at close.
    ///
    /// We verify this indirectly by:
    ///   1. Appending many records (LZ4's frame-level buffer flushes
    ///      every 64 KiB; smaller codecs like gzip flush more often).
    ///      For LZ4 specifically, appending > 64 KiB of records forces
    ///      at least one frame block to be emitted before close().
    ///   2. Asserting `builder.buffer_stream.position()` advances past
    ///      the reserved batch header BEFORE close() runs.
    ///
    /// Under the old design (`uncompressed_buf` materialized then
    /// compressed at close), the buffer_stream's position would stay
    /// pinned at `RECORD_BATCH_OVERHEAD` until close() — this assertion
    /// would fail. Under the streaming design, the codec emits frame
    /// blocks during append and the position advances in-flight.
    #[test]
    fn compressed_append_streams_into_buffer_stream_in_flight() {
        // Pick LZ4: its block size is 64 KiB, so appending ~150 KiB of
        // records forces at least one block to be emitted before close.
        let stream = ByteBufferOutputStream::with_capacity(256 * 1024);
        let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
            stream,
            CURRENT_MAGIC_VALUE,
            CompressionType::Lz4,
            TimestampType::CreateTime,
            0,
            0,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            crate::common::record::record_batch::NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            256 * 1024,
        )
        .unwrap();

        // Position right after construction: the batch header is
        // reserved (RECORD_BATCH_OVERHEAD) and the codec MAY have
        // already written a frame header (lz4 writes 7 bytes of magic
        // + flags at construction; gzip waits until the first write).
        // Capture whatever the position is post-construction as the
        // baseline; the streaming assertion is "position grows DURING
        // append", not "position grows past a specific offset".
        let baseline_position = builder.buffer_stream.position();
        assert!(baseline_position >= RECORD_BATCH_OVERHEAD);

        // Append a 1KB record 200 times (200 KB of uncompressed data) —
        // larger than LZ4's 64 KiB block size, so the codec MUST emit
        // at least one block while we're still appending.
        let big_value = vec![b'x'; 1024];
        for i in 0..200 {
            builder.append(i as i64, Some(b"k"), Some(&big_value), &[]).unwrap();
        }

        // CRITICAL ASSERTION: the buffer_stream's position must have
        // advanced past `baseline_position` while we were still
        // appending. Under the old `uncompressed_buf` design, the
        // position would have stayed at `baseline_position` because
        // nothing had been compressed yet. Under streaming compression,
        // the codec has emitted at least one block.
        let mid_position = builder.buffer_stream.position();
        assert!(
            mid_position > baseline_position,
            "compressed records must stream into buffer_stream during append (streaming codec); \
             position is {mid_position} but should be > baseline {baseline_position}"
        );

        // Now close & build. Round-trip the records to confirm
        // correctness end-to-end.
        let built = builder.build().unwrap();
        let batches: Vec<_> = built.batches().map(|r| r.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].count_or_null().unwrap_or(0), 200);
    }

    /// Round-trip sanity for every compressed codec — exercises the
    /// streaming-codec path end-to-end (append → close → decode) for
    /// gzip, snappy, lz4, and zstd. If the streaming wiring corrupted
    /// any codec's internal state, the decoded records would mismatch.
    #[test]
    fn compressed_streaming_round_trip_per_codec() {
        for codec in [
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let stream = ByteBufferOutputStream::with_capacity(8192);
            let mut builder = MemoryRecordsBuilder::from_stream_no_delete_horizon(
                stream,
                CURRENT_MAGIC_VALUE,
                codec,
                TimestampType::CreateTime,
                0,
                0,
                NO_PRODUCER_ID,
                NO_PRODUCER_EPOCH,
                crate::common::record::record_batch::NO_SEQUENCE,
                false,
                false,
                NO_PARTITION_LEADER_EPOCH,
                8192,
            )
            .unwrap();
            let payloads = ["alpha", "bravo", "charlie", "delta", "echo"];
            for (i, p) in payloads.iter().enumerate() {
                builder
                    .append(100 + i as i64, Some(format!("k{i}").as_bytes()), Some(p.as_bytes()), &[])
                    .unwrap();
            }
            let built = builder.build().unwrap();
            let batches: Vec<_> = built.batches().map(|r| r.unwrap()).collect();
            assert_eq!(batches.len(), 1, "codec {codec:?}: expected exactly one batch");
            let count = batches[0].count_or_null().unwrap_or(0);
            assert_eq!(count, payloads.len() as i32, "codec {codec:?}: record count mismatch");
            let decoded: Vec<_> = batches[0].iter().map(|r| r.unwrap()).collect();
            assert_eq!(decoded.len(), payloads.len(), "codec {codec:?}: decoded count");
            for (i, rec) in decoded.iter().enumerate() {
                let v = rec.value().expect("value present");
                assert_eq!(v, payloads[i].as_bytes(), "codec {codec:?}: payload {i} round-trip");
            }
        }
    }
}
