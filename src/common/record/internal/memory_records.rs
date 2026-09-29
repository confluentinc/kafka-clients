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

//! A records implementation backed by a byte buffer.
//!
//! This is used only for reading or modifying in-place an existing buffer of
//! record batches. To create a new buffer see [`MemoryRecordsBuilder`],
//! or one of the [`builder_with_initial_capacity()`](MemoryRecords::builder_with_initial_capacity) variants.
//!
//! Corresponds to Java's `org.apache.kafka.common.record.MemoryRecords`.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::Error;
use crate::common::Errors;
use crate::common::compress::Compression;
use crate::common::record::TimestampType;
use crate::common::record::internal::AbstractRecords;
use crate::common::record::internal::DefaultRecord;
use crate::common::record::internal::DefaultRecordBatch;
use crate::common::record::internal::MemoryRecordsBuilder;
use crate::common::record::internal::RecordBatch;
use crate::common::record::internal::SimpleRecord;

/// A records implementation backed by a byte buffer.
///
/// Contains one or more complete record batches in serialized form.
///
/// Corresponds to Java's `org.apache.kafka.common.record.MemoryRecords`.
#[derive(Clone, Debug)]
pub struct MemoryRecords {
    /// The single owning buffer for all record bytes in this set, held as a
    /// refcounted [`bytes::Bytes`]. On the receive path this is a zero-copy
    /// slice of the FetchResponse payload; every downstream `DefaultRecordRef`
    /// borrows from it and never copies key/value bytes (consumer-threading.md
    /// §27). `Clone` is an O(1) refcount bump.
    buffer: bytes::Bytes,
}

impl MemoryRecords {
    /// Create a new `MemoryRecords` wrapping the given buffer.
    ///
    /// Accepts an owned [`bytes::Bytes`]; callers holding a `Vec<u8>` can pass
    /// `vec.into()` (which adopts the allocation without copying).
    pub fn new(buffer: bytes::Bytes) -> Self {
        Self { buffer }
    }

    /// Create an empty `MemoryRecords`.
    pub fn empty() -> Self {
        Self { buffer: bytes::Bytes::new() }
    }

    /// Create a `MemoryRecords` from a byte slice (copies the data).
    pub fn readable_records(data: &[u8]) -> Self {
        Self { buffer: bytes::Bytes::copy_from_slice(data) }
    }

    /// Returns the total size of this records set in bytes.
    pub fn size_in_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Returns a reference to the underlying buffer.
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Returns a reference to the underlying refcounted buffer.
    ///
    /// Used on the receive path to slice individual record key/value bytes
    /// out of the owning buffer as zero-copy `Bytes` via
    /// [`bytes::Bytes::slice_ref`] (consumer-threading.md §27).
    pub fn buffer_bytes(&self) -> &bytes::Bytes {
        &self.buffer
    }

    /// Consume this `MemoryRecords` and return the underlying buffer.
    ///
    /// Returns the refcounted [`bytes::Bytes`]; on the write path this is
    /// moved straight into the network send (no copy).
    pub fn into_buffer(self) -> bytes::Bytes {
        self.buffer
    }

    /// Returns an iterator over the batches in this records set.
    ///
    /// Each batch is a `DefaultRecordBatch` containing the full batch header
    /// and record data.
    pub fn batches(&self) -> BatchIterator<'_> {
        BatchIterator { data: &self.buffer, pos: 0 }
    }

    /// Returns an iterator over all individual records across all batches.
    ///
    /// A batch this client cannot parse contributes no records. Java's
    /// `RecordBatchIterator` throws instead (`DefaultRecordBatch.java:645-652`), so
    /// the failure is logged here rather than passed silently: an `Iterator` cannot
    /// report it, and the alternative — a fallible signature — would reach eight
    /// call sites for a case only a corrupt buffer produces. The one production
    /// caller is `ProducerBatch::split`, where yielding nothing would strand the
    /// batch's thunks and leave those `send()` futures unresolved, so a log line is
    /// the difference between a diagnosable hang and a silent one.
    pub fn records(&self) -> impl Iterator<Item = DefaultRecord> + '_ {
        self.batches().flat_map(|batch| match batch.iter_records() {
            Ok(records) => records,
            Err(e) => {
                log::error!("Skipping an unparseable record batch while iterating records: {e}");
                Vec::new()
            },
        })
    }

    /// The total number of valid bytes (excluding any partial, trailing data).
    pub fn valid_bytes(&self) -> usize {
        let mut bytes = 0;
        for batch in self.batches() {
            bytes += batch.size_in_bytes();
        }
        bytes
    }

    /// Validates the header of the first batch and returns batch size.
    ///
    /// Returns `Ok(None)` if the buffer does not contain enough bytes for a
    /// header. Returns `Err(CorruptMessage)` if the record size is invalid
    /// (too small, too large, or negative) or if the magic byte is invalid.
    ///
    /// Corresponds to Java's `MemoryRecords.firstBatchSize()` which delegates
    /// to `ByteBufferLogInputStream.nextBatchSize()`.
    pub fn first_batch_size(&self) -> Result<Option<usize>, Error> {
        // Minimum overhead for LegacyRecord v0:
        //   CRC(4) + Magic(1) + Attributes(1) + KeySize(4) + ValueSize(4) = 14
        const LEGACY_RECORD_OVERHEAD_V0: i32 = 14;

        if self.buffer.len() < AbstractRecords::LOG_OVERHEAD {
            return Ok(None);
        }

        // Read the record size (length) field
        let record_size = i32::from_be_bytes(
            self.buffer[RecordBatch::LENGTH_OFFSET..RecordBatch::LENGTH_OFFSET + 4]
                .try_into()
                .map_err(|_| Error::with_message(Errors::CorruptMessage, "Failed to read record size"))?,
        );

        // Validate minimum record size (V0 has the smallest overhead)
        if record_size < LEGACY_RECORD_OVERHEAD_V0 {
            return Err(Error::with_message(
                Errors::CorruptMessage,
                format!(
                    "Record size {} is less than the minimum record overhead ({})",
                    record_size, LEGACY_RECORD_OVERHEAD_V0
                ),
            ));
        }

        // Validate maximum message size (use i32::MAX like Java's Integer.MAX_VALUE)
        // Java passes Integer.MAX_VALUE as maxMessageSize from firstBatchSize(),
        // so this check only catches negative values that wrapped or truly
        // enormous sizes. Since we already checked >= LEGACY_RECORD_OVERHEAD_V0
        // and record_size is i32, the max check here matches Java behavior.

        if self.buffer.len() < AbstractRecords::HEADER_SIZE_UP_TO_MAGIC {
            return Ok(None);
        }

        // Validate magic byte
        let magic = self.buffer[RecordBatch::MAGIC_OFFSET] as i8;
        if !(0..=RecordBatch::CURRENT_MAGIC_VALUE).contains(&magic) {
            return Err(Error::with_message(
                Errors::CorruptMessage,
                format!("Invalid magic found in record: {}", magic),
            ));
        }

        Ok(Some(AbstractRecords::LOG_OVERHEAD + record_size as usize))
    }

    /// Returns `true` if the buffer holds at least one *complete* record batch.
    ///
    /// Corresponds to Java's `records().batches().iterator().hasNext()`, which is
    /// `ByteBufferLogInputStream.nextBatch() != null`
    /// (`ByteBufferLogInputStream.java:41-46`):
    ///
    /// ```java
    /// public MutableRecordBatch nextBatch() {
    ///     int remaining = buffer.remaining();
    ///     Integer batchSize = nextBatchSize();
    ///     if (batchSize == null || remaining < batchSize)
    ///         return null;
    /// ```
    ///
    /// So it is [`first_batch_size`](Self::first_batch_size) — Java's
    /// `nextBatchSize()`, which only validates the header up to the magic byte —
    /// **plus** the completeness test `remaining < batchSize`. The distinction
    /// matters: a buffer holding an intact header that declares `N` bytes but
    /// carrying fewer than `N` bytes of payload (a broker cutting a fetch
    /// response mid-batch at `max.partition.fetch.bytes`) has a batch *size* but
    /// no readable batch, and Java reports it as no batch.
    ///
    /// # Errors
    ///
    /// Propagates the [`Errors::CorruptMessage`] error `first_batch_size`
    /// raises for an invalid record size or magic byte, exactly as Java's
    /// `hasNext()` propagates `CorruptRecordException` out of `nextBatchSize()`
    /// (`ByteBufferLogInputStream.java:73`, `:76`, `:84`). A corrupt header is
    /// NOT "no batch"; conflating the two loses both the error class and the
    /// message that says what is wrong.
    pub fn has_complete_first_batch(&self) -> Result<bool, Error> {
        match self.first_batch_size()? {
            // Java: `remaining < batchSize` -> null. `remaining` is the whole
            // buffer here because the check runs at position 0.
            Some(batch_size) => Ok(batch_size <= self.buffer.len()),
            None => Ok(false),
        }
    }

    /// Returns a slice of the records data at the given position and size.
    ///
    /// The `size` parameter is clamped to the available bytes from `position`
    /// to the end of the buffer, matching Java's
    /// `MemoryRecords.slice(int, int)` which uses
    /// `Math.min(size, buffer.limit() - position)`.
    pub fn slice(&self, position: usize, size: usize) -> MemoryRecords {
        assert!(
            position <= self.buffer.len(),
            "Slice from position {} exceeds end position",
            position
        );
        let available_bytes = size.min(self.buffer.len() - position);
        // `Bytes::slice` is O(1) (refcount bump + range), not a copy.
        MemoryRecords::new(self.buffer.slice(position..position + available_bytes))
    }

    // -- Builder factory methods --

    // Java's nine `builder` overloads
    // (`MemoryRecords.java:475,482,496,507,520,531,543,556,571`) have the
    // parameter-name intersection {buffer, compression, baseOffset}, and NO
    // overload has exactly that signature — so under CLAUDE.md §2 nobody keeps
    // the plain translated name `builder`, and every form is suffixed with the
    // parameters it adds beyond the intersection.
    //
    // Rust translates Java's `buffer` two ways: as `initial_capacity`, letting
    // the constructor allocate, and as a caller-supplied `buffer`. They are the
    // same Java parameter, so the Rust group's own intersection narrows to
    // {compression, timestamp_type, base_offset} and the buffer parameter enters
    // each derived name under its Rust name — which is also what §2's
    // same-name-different-type clause asks for, since without it
    // [`Self::builder_with_initial_capacity_magic`] and
    // [`Self::builder_with_buffer_magic`] would collide.
    //
    // Java's `:520`, `:531`, `:543`, `:556` and `:571` each add four or more
    // parameters, past §2's cap of three, so all five are served by
    // [`Self::builder_with_options`] and [`MemoryRecordsBuilderOptions`]. `:520`,
    // `:531` and `:556` have no Rust caller and are not separately translated;
    // the options builder expresses them (DoD #2).

    /// Create a builder with default parameters.
    ///
    /// Corresponds to Java's `builder(ByteBuffer, Compression, TimestampType,
    /// long)` (`MemoryRecords.java:475`), with the buffer allocated here from
    /// `initial_capacity`.
    pub fn builder_with_initial_capacity(
        initial_capacity: usize,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
    ) -> MemoryRecordsBuilder {
        Self::builder_with_initial_capacity_magic(
            initial_capacity,
            RecordBatch::CURRENT_MAGIC_VALUE,
            compression,
            timestamp_type,
            base_offset,
        )
    }

    /// Create a builder with a specific magic value.
    ///
    /// Corresponds to Java's `builder(ByteBuffer, byte, Compression,
    /// TimestampType, long)` (`MemoryRecords.java:507`), with the buffer
    /// allocated here from `initial_capacity`.
    pub fn builder_with_initial_capacity_magic(
        initial_capacity: usize,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
    ) -> MemoryRecordsBuilder {
        Self::builder_with_options(
            MemoryRecordsBuilderOptionsBuilder::new()
                .set_initial_capacity(initial_capacity)
                .set_magic(magic)
                .set_compression(compression)
                .set_timestamp_type(timestamp_type)
                .set_base_offset(base_offset)
                .build()
                .expect("MemoryRecordsBuilderOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Create a builder using a pre-allocated buffer.
    ///
    /// Corresponds to Java's `builder(ByteBuffer, byte, Compression,
    /// TimestampType, long)` (`MemoryRecords.java:507`) taking the buffer
    /// directly — e.g. one from a buffer pool — rather than allocating it. The
    /// `buffer` token in the name discriminates it from
    /// [`Self::builder_with_initial_capacity_magic`], which translates the same
    /// Java overload with an allocated buffer (CLAUDE.md §2).
    pub fn builder_with_buffer_magic(
        buffer: Vec<u8>,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
    ) -> MemoryRecordsBuilder {
        let write_limit = buffer.capacity();
        MemoryRecordsBuilder::with_default(
            buffer,
            0,
            magic,
            compression,
            timestamp_type,
            base_offset,
            Self::default_log_append_time(timestamp_type),
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            write_limit,
        )
    }

    /// Create a builder with a max size limit.
    ///
    /// Corresponds to Java's `builder(ByteBuffer, Compression, TimestampType,
    /// long, int)` (`MemoryRecords.java:482`), with the buffer allocated here
    /// from `initial_capacity`.
    pub fn builder_with_initial_capacity_max_size(
        initial_capacity: usize,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
        max_size: usize,
    ) -> MemoryRecordsBuilder {
        Self::builder_with_options(
            MemoryRecordsBuilderOptionsBuilder::new()
                .set_initial_capacity(initial_capacity)
                .set_compression(compression)
                .set_timestamp_type(timestamp_type)
                .set_base_offset(base_offset)
                .set_write_limit(max_size)
                .build()
                .expect("MemoryRecordsBuilderOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Create a builder with magic, compression, timestamp type, and log append time.
    ///
    /// Corresponds to Java's `builder(ByteBuffer, byte, Compression,
    /// TimestampType, long, long)` (`MemoryRecords.java:496`), with the buffer
    /// allocated here from `initial_capacity`.
    pub fn builder_with_initial_capacity_magic_log_append_time(
        initial_capacity: usize,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
        log_append_time: i64,
    ) -> MemoryRecordsBuilder {
        Self::builder_with_options(
            MemoryRecordsBuilderOptionsBuilder::new()
                .set_initial_capacity(initial_capacity)
                .set_magic(magic)
                .set_compression(compression)
                .set_timestamp_type(timestamp_type)
                .set_base_offset(base_offset)
                .set_log_append_time(log_append_time)
                .build()
                .expect("MemoryRecordsBuilderOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Create a builder from the full parameter set.
    ///
    /// # Arguments
    ///
    /// * `options` - Every Java parameter, built through
    ///   [`MemoryRecordsBuilderOptionsBuilder`]. [`MemoryRecordsBuilderOptions`]
    ///   is this method's only parameter because the derived name would list
    ///   nine parameters beyond the group's intersection, past CLAUDE.md §2's
    ///   cap of three.
    ///
    /// Corresponds to Java's widest `builder` overload (`MemoryRecords.java:571`)
    /// and, through the builder's defaults, to `:520`, `:531`, `:543` and `:556`
    /// as well. Setting `delete_horizon_ms` selects
    /// [`MemoryRecordsBuilder::new`] over
    /// [`MemoryRecordsBuilder::with_default`]; Java has no `builder` overload
    /// carrying a delete horizon, only the `MemoryRecordsBuilder` constructor
    /// that takes one.
    pub(crate) fn builder_with_options(options: MemoryRecordsBuilderOptions) -> MemoryRecordsBuilder {
        let MemoryRecordsBuilderOptions {
            initial_capacity,
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
        } = options;

        let buffer = Vec::with_capacity(initial_capacity);
        match delete_horizon_ms {
            Some(delete_horizon_ms) => MemoryRecordsBuilder::new(
                buffer,
                0,
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
            ),
            None => MemoryRecordsBuilder::with_default(
                buffer,
                0,
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
            ),
        }
    }

    /// The `logAppendTime` every Java `builder` overload that does not take one
    /// computes for itself (`MemoryRecords.java:487-489`, `:511-513`).
    fn default_log_append_time(timestamp_type: TimestampType) -> i64 {
        if timestamp_type == TimestampType::LogAppendTime {
            current_time_millis()
        } else {
            RecordBatch::NO_TIMESTAMP
        }
    }

    // -- Convenience factory methods for creating records directly --

    // Java's eight `withRecords` overloads
    // (`MemoryRecords.java:588,592,598,602,607,611,656,663`) have the
    // parameter-name intersection {compression, records}, and `:588` has
    // exactly that signature — so under CLAUDE.md §2 it keeps the plain
    // translated name and every sibling is suffixed with the parameters it adds
    // beyond the intersection, in Java declaration order. `:663` adds eight,
    // past §2's cap of three, so it is served by
    // [`Self::with_records_with_options`] and [`MemoryRecordsOptions`] rather
    // than by a name listing them all.
    //
    // Three Java overloads have no Rust caller and are not translated: `:592`
    // (`compression, partitionLeaderEpoch, records`), `:602`
    // (`initialOffset, compression, records`) and `:607`
    // (`magic, initialOffset, compression, records`). That is a DoD #2
    // completeness gap, not a naming one.

    /// Create a `MemoryRecords` with the given records using default settings.
    ///
    /// Corresponds to Java's `withRecords(Compression, SimpleRecord...)`
    /// (`MemoryRecords.java:588`) — the overload whose parameters are the
    /// group's intersection, which is why it keeps the plain name.
    pub fn with_records(compression: Compression, records: &[SimpleRecord]) -> MemoryRecords {
        Self::with_records_with_magic(RecordBatch::CURRENT_MAGIC_VALUE, compression, records)
    }

    /// Create a `MemoryRecords` with a specific magic value.
    ///
    /// Corresponds to Java's `withRecords(byte, Compression, SimpleRecord...)`
    /// (`MemoryRecords.java:598`).
    pub fn with_records_with_magic(magic: i8, compression: Compression, records: &[SimpleRecord]) -> MemoryRecords {
        Self::with_records_with_magic_initial_offset_timestamp_type(
            magic,
            0,
            compression,
            TimestampType::CreateTime,
            records,
        )
    }

    /// Create a `MemoryRecords` with records starting at a specific offset.
    ///
    /// Corresponds to Java's `withRecords(byte, long, Compression,
    /// TimestampType, SimpleRecord...)` (`MemoryRecords.java:656`).
    pub fn with_records_with_magic_initial_offset_timestamp_type(
        magic: i8,
        initial_offset: i64,
        compression: Compression,
        timestamp_type: TimestampType,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        Self::with_records_with_options(
            MemoryRecordsOptionsBuilder::new()
                .set_magic(magic)
                .set_initial_offset(initial_offset)
                .set_compression(compression)
                .set_timestamp_type(timestamp_type)
                .set_records(records)
                .build()
                .expect("MemoryRecordsOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Create a `MemoryRecords` with records at a specific offset and partition leader epoch.
    ///
    /// Corresponds to Java's `withRecords(long, Compression, int,
    /// SimpleRecord...)` (`MemoryRecords.java:611`).
    pub fn with_records_with_initial_offset_partition_leader_epoch(
        initial_offset: i64,
        compression: Compression,
        partition_leader_epoch: i32,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        Self::with_records_with_options(
            MemoryRecordsOptionsBuilder::new()
                .set_initial_offset(initial_offset)
                .set_compression(compression)
                .set_partition_leader_epoch(partition_leader_epoch)
                .set_records(records)
                .build()
                .expect("MemoryRecordsOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Create idempotent records.
    ///
    /// Corresponds to Java's `withIdempotentRecords(Compression, long, short,
    /// int, SimpleRecord...)` (`MemoryRecords.java:616`) — the overload whose
    /// parameters are that group's intersection, which is why it keeps the
    /// plain name. Java's other two (`:622`, `:629`) have no Rust caller and are
    /// not translated (DoD #2).
    pub fn with_idempotent_records(
        compression: Compression,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        Self::with_records_with_options(
            MemoryRecordsOptionsBuilder::new()
                .set_compression(compression)
                .set_producer_id(producer_id)
                .set_producer_epoch(producer_epoch)
                .set_base_sequence(base_sequence)
                .set_records(records)
                .build()
                .expect("MemoryRecordsOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Create transactional records.
    ///
    /// Corresponds to Java's `withTransactionalRecords(Compression, long,
    /// short, int, SimpleRecord...)` (`MemoryRecords.java:636`) — the overload
    /// whose parameters are that group's intersection, which is why it keeps the
    /// plain name. Java's other two (`:642`, `:649`) have no Rust caller and are
    /// not translated (DoD #2).
    pub fn with_transactional_records(
        compression: Compression,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        Self::with_records_with_options(
            MemoryRecordsOptionsBuilder::new()
                .set_compression(compression)
                .set_producer_id(producer_id)
                .set_producer_epoch(producer_epoch)
                .set_base_sequence(base_sequence)
                .set_is_transactional(true)
                .set_records(records)
                .build()
                .expect("MemoryRecordsOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Create a `MemoryRecords` with full parameters.
    ///
    /// # Arguments
    ///
    /// * `options` - Every Java parameter, built through
    ///   [`MemoryRecordsOptionsBuilder`]. [`MemoryRecordsOptions`] is this
    ///   method's only parameter because the derived name would list eight
    ///   parameters beyond the group's intersection, past CLAUDE.md §2's cap of
    ///   three.
    ///
    /// Corresponds to Java's `withRecords(byte, long, Compression,
    /// TimestampType, long, short, int, int, boolean, SimpleRecord...)`
    /// (`MemoryRecords.java:663`).
    pub(crate) fn with_records_with_options(options: MemoryRecordsOptions<'_>) -> MemoryRecords {
        let MemoryRecordsOptions {
            magic,
            initial_offset,
            compression,
            timestamp_type,
            producer_id,
            producer_epoch,
            base_sequence,
            partition_leader_epoch,
            is_transactional,
            records,
        } = options;

        if records.is_empty() {
            return MemoryRecords::empty();
        }

        let size_estimate = AbstractRecords::estimate_size_in_bytes(magic, compression.compression_type(), records);
        let log_append_time = if timestamp_type == TimestampType::LogAppendTime {
            current_time_millis()
        } else {
            RecordBatch::NO_TIMESTAMP
        };

        let mut builder = MemoryRecordsBuilder::with_default(
            Vec::with_capacity(size_estimate),
            0,
            magic,
            compression,
            timestamp_type,
            initial_offset,
            log_append_time,
            producer_id,
            producer_epoch,
            base_sequence,
            is_transactional,
            false,
            partition_leader_epoch,
            size_estimate,
        );

        for record in records {
            builder.append_simple(record);
        }

        builder.build()
    }
}

/// Parameters for [`MemoryRecords::builder_with_options`].
///
/// This struct has **no Java counterpart** (DoD #7). It exists solely to satisfy
/// CLAUDE.md §2's cap on derived overload names: Java's `builder` group has the
/// parameter-name intersection {`buffer`, `compression`, `baseOffset`}, and its
/// five widest overloads (`MemoryRecords.java:520,531,543,556,571`) each add
/// four or more parameters beyond it — so the cap fires and this struct becomes
/// the method's *only* parameter, carrying every Java parameter.
///
/// It is `pub(crate)`, not `pub`: the package is `record.internal`, where §2
/// mandates `pub(crate)`, and the narrow public re-export documented in
/// [`crate::common::record`] covers only the two Java class names.
///
/// The mandatory set is exactly the group's intersection, on the same principle
/// as [`MemoryRecordsOptions`]: a parameter is optional here precisely when some
/// narrower Java overload supplies it on the caller's behalf, and the
/// intersection is what no overload can supply.
#[non_exhaustive]
pub(crate) struct MemoryRecordsBuilderOptions {
    /// The capacity of the buffer to allocate. The Rust translation of Java's
    /// `buffer` for the overloads that let the constructor allocate — in the
    /// group's intersection, hence mandatory.
    pub initial_capacity: usize,
    /// The record format version. Java's `magic`; starts at
    /// [`RecordBatch::CURRENT_MAGIC_VALUE`], the value `:475`, `:482` and `:531`
    /// pass on the caller's behalf.
    pub magic: i8,
    /// The compression to apply. Java's `compression` — in the group's
    /// intersection, hence mandatory.
    pub compression: Compression,
    /// How record timestamps are interpreted. Java's `timestampType`; starts at
    /// [`TimestampType::CreateTime`], the value `:531` passes on the caller's
    /// behalf.
    pub timestamp_type: TimestampType,
    /// The offset of the first record. Java's `baseOffset` — in the group's
    /// intersection, hence mandatory.
    pub base_offset: i64,
    /// The log-append timestamp. Java's `logAppendTime`; starts at the value
    /// every overload that does not take one computes for itself — `now()` for
    /// [`TimestampType::LogAppendTime`], else [`RecordBatch::NO_TIMESTAMP`]
    /// (`MemoryRecords.java:487-489`). Because that default reads
    /// `timestamp_type`, setting `timestamp_type` after this field still yields
    /// the right value: the derivation happens in
    /// [`MemoryRecordsBuilderOptionsBuilder::build`], not in the setter.
    pub log_append_time: i64,
    /// The producer id. Java's `producerId`; starts at
    /// [`RecordBatch::NO_PRODUCER_ID`].
    pub producer_id: i64,
    /// The producer epoch. Java's `producerEpoch`; starts at
    /// [`RecordBatch::NO_PRODUCER_EPOCH`].
    pub producer_epoch: i16,
    /// The base sequence number. Java's `baseSequence`; starts at
    /// [`RecordBatch::NO_SEQUENCE`].
    pub base_sequence: i32,
    /// Whether the batch is transactional. Java's `isTransactional`; starts at
    /// `false`.
    pub is_transactional: bool,
    /// Whether the batch is a control batch. Java's `isControlBatch`; starts at
    /// `false`.
    pub is_control_batch: bool,
    /// The partition leader epoch. Java's `partitionLeaderEpoch`; starts at
    /// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub partition_leader_epoch: i32,
    /// The byte budget for the batch. Java's `writeLimit`, which `:571` fills
    /// with `buffer.remaining()`; starts at `initial_capacity`, the Rust
    /// equivalent of that expression. This is also where Java's `:482` `maxSize`
    /// lands.
    pub write_limit: usize,
    /// The delete horizon, or `None` for a batch without one. No Java `builder`
    /// overload carries this; it reaches `MemoryRecordsBuilder`'s own
    /// constructor. `None` selects
    /// [`MemoryRecordsBuilder::with_default`] over
    /// [`MemoryRecordsBuilder::new`].
    pub delete_horizon_ms: Option<i64>,
}

/// Fluent builder for [`MemoryRecordsBuilderOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — returning
/// [`Error::LocalIllegalArgument`] if they were not set. Like
/// [`MemoryRecordsBuilderOptions`] it has no Java counterpart and exists solely
/// to satisfy that naming rule (DoD #7).
pub(crate) struct MemoryRecordsBuilderOptionsBuilder {
    initial_capacity: Option<usize>,
    magic: Option<i8>,
    compression: Option<Compression>,
    timestamp_type: Option<TimestampType>,
    base_offset: Option<i64>,
    log_append_time: Option<i64>,
    producer_id: Option<i64>,
    producer_epoch: Option<i16>,
    base_sequence: Option<i32>,
    is_transactional: Option<bool>,
    is_control_batch: Option<bool>,
    partition_leader_epoch: Option<i32>,
    write_limit: Option<usize>,
    delete_horizon_ms: Option<i64>,
}

impl Default for MemoryRecordsBuilderOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryRecordsBuilderOptionsBuilder {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    pub(crate) fn new() -> Self {
        Self {
            initial_capacity: None,
            magic: None,
            compression: None,
            timestamp_type: None,
            base_offset: None,
            log_append_time: None,
            producer_id: None,
            producer_epoch: None,
            base_sequence: None,
            is_transactional: None,
            is_control_batch: None,
            partition_leader_epoch: None,
            write_limit: None,
            delete_horizon_ms: None,
        }
    }

    /// Sets [`MemoryRecordsBuilderOptions::initial_capacity`], a mandatory
    /// parameter: [`Self::build`] returns an error if it was not set.
    pub(crate) fn set_initial_capacity(mut self, initial_capacity: usize) -> Self {
        self.initial_capacity = Some(initial_capacity);
        self
    }

    /// Sets [`MemoryRecordsBuilderOptions::magic`].
    pub(crate) fn set_magic(mut self, magic: i8) -> Self {
        self.magic = Some(magic);
        self
    }

    /// Sets [`MemoryRecordsBuilderOptions::compression`], a mandatory
    /// parameter: [`Self::build`] returns an error if it was not set.
    pub(crate) fn set_compression(mut self, compression: Compression) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Sets [`MemoryRecordsBuilderOptions::timestamp_type`].
    pub(crate) fn set_timestamp_type(mut self, timestamp_type: TimestampType) -> Self {
        self.timestamp_type = Some(timestamp_type);
        self
    }

    /// Sets [`MemoryRecordsBuilderOptions::base_offset`], a mandatory
    /// parameter: [`Self::build`] returns an error if it was not set.
    pub(crate) fn set_base_offset(mut self, base_offset: i64) -> Self {
        self.base_offset = Some(base_offset);
        self
    }

    /// Sets [`MemoryRecordsBuilderOptions::log_append_time`].
    pub(crate) fn set_log_append_time(mut self, log_append_time: i64) -> Self {
        self.log_append_time = Some(log_append_time);
        self
    }

    // No Rust caller sets this today (outside the unit tests that go through
    // `builder_with_options` directly). Kept because CLAUDE.md §2 requires a
    // fluent setter for *every* optional parameter of an `Options` struct, so
    // the builder can express Java's `:520`/`:531`/`:543`/`:556`/`:571`
    // overloads; the `dead_code` lint sees only the subset today's callers use.
    /// Sets [`MemoryRecordsBuilderOptions::producer_id`].
    #[allow(dead_code)]
    pub(crate) fn set_producer_id(mut self, producer_id: i64) -> Self {
        self.producer_id = Some(producer_id);
        self
    }

    /// Sets [`MemoryRecordsBuilderOptions::producer_epoch`].
    #[allow(dead_code)]
    pub(crate) fn set_producer_epoch(mut self, producer_epoch: i16) -> Self {
        self.producer_epoch = Some(producer_epoch);
        self
    }

    /// Sets [`MemoryRecordsBuilderOptions::base_sequence`].
    #[allow(dead_code)]
    pub(crate) fn set_base_sequence(mut self, base_sequence: i32) -> Self {
        self.base_sequence = Some(base_sequence);
        self
    }

    /// Sets [`MemoryRecordsBuilderOptions::is_transactional`].
    #[allow(dead_code)]
    pub(crate) fn set_is_transactional(mut self, is_transactional: bool) -> Self {
        self.is_transactional = Some(is_transactional);
        self
    }

    /// Sets [`MemoryRecordsBuilderOptions::is_control_batch`].
    #[allow(dead_code)]
    pub(crate) fn set_is_control_batch(mut self, is_control_batch: bool) -> Self {
        self.is_control_batch = Some(is_control_batch);
        self
    }

    /// Sets [`MemoryRecordsBuilderOptions::partition_leader_epoch`].
    #[allow(dead_code)]
    pub(crate) fn set_partition_leader_epoch(mut self, partition_leader_epoch: i32) -> Self {
        self.partition_leader_epoch = Some(partition_leader_epoch);
        self
    }

    /// Sets [`MemoryRecordsBuilderOptions::write_limit`].
    pub(crate) fn set_write_limit(mut self, write_limit: usize) -> Self {
        self.write_limit = Some(write_limit);
        self
    }

    /// Sets [`MemoryRecordsBuilderOptions::delete_horizon_ms`].
    #[allow(dead_code)]
    pub(crate) fn set_delete_horizon_ms(mut self, delete_horizon_ms: i64) -> Self {
        self.delete_horizon_ms = Some(delete_horizon_ms);
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the method. Today there is one mandatory set:
    /// `initial_capacity`, `compression` and `base_offset` — the Java group's
    /// parameter-name intersection. The other eleven are not in it because
    /// Java's narrower overloads supply them themselves.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter of
    /// that set which was not given a setter call. Only presence is checked
    /// here; semantic validation belongs to the method the options are passed to
    /// (CLAUDE.md §2).
    pub(crate) fn build(self) -> Result<MemoryRecordsBuilderOptions, Error> {
        let initial_capacity = self.initial_capacity.ok_or_else(|| Self::missing("initial_capacity"))?;
        let timestamp_type = self.timestamp_type.unwrap_or(TimestampType::CreateTime);
        Ok(MemoryRecordsBuilderOptions {
            initial_capacity,
            magic: self.magic.unwrap_or(RecordBatch::CURRENT_MAGIC_VALUE),
            compression: self.compression.ok_or_else(|| Self::missing("compression"))?,
            timestamp_type,
            base_offset: self.base_offset.ok_or_else(|| Self::missing("base_offset"))?,
            log_append_time: self
                .log_append_time
                .unwrap_or_else(|| MemoryRecords::default_log_append_time(timestamp_type)),
            producer_id: self.producer_id.unwrap_or(RecordBatch::NO_PRODUCER_ID),
            producer_epoch: self.producer_epoch.unwrap_or(RecordBatch::NO_PRODUCER_EPOCH),
            base_sequence: self.base_sequence.unwrap_or(RecordBatch::NO_SEQUENCE),
            is_transactional: self.is_transactional.unwrap_or(false),
            is_control_batch: self.is_control_batch.unwrap_or(false),
            partition_leader_epoch: self.partition_leader_epoch.unwrap_or(RecordBatch::NO_PARTITION_LEADER_EPOCH),
            write_limit: self.write_limit.unwrap_or(initial_capacity),
            delete_horizon_ms: self.delete_horizon_ms,
        })
    }

    /// Builds the [`Error::LocalIllegalArgument`] naming a mandatory parameter
    /// [`Self::build`] found unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "MemoryRecordsBuilderOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

/// Parameters for [`MemoryRecords::with_records_with_options`].
///
/// This struct has **no Java counterpart** (DoD #7). It exists solely to satisfy
/// CLAUDE.md §2's cap on derived overload names: Java's widest `withRecords`
/// overload (`MemoryRecords.java:663`) adds eight parameters beyond the
/// `withRecords` group's parameter-name intersection — which is
/// {`compression`, `records`} — so the cap fires and this struct becomes the
/// method's *only* parameter, carrying every Java parameter.
///
/// It is `pub(crate)`, not `pub`: the package is `record.internal`, where §2
/// mandates `pub(crate)`, and the narrow public re-export documented in
/// [`crate::common::record`] covers only the two Java class names.
///
/// The mandatory set is exactly the group's intersection, which is not a
/// coincidence: a parameter is optional here precisely when some narrower Java
/// overload supplies it on the caller's behalf, and the intersection is what no
/// overload can supply.
#[non_exhaustive]
pub(crate) struct MemoryRecordsOptions<'a> {
    /// The record format version. Java's `magic`; starts at
    /// [`RecordBatch::CURRENT_MAGIC_VALUE`], the value `:588` passes on the
    /// caller's behalf.
    pub magic: i8,
    /// The base offset of the first record. Java's `initialOffset`; starts at
    /// `0`, the value `:598` passes on the caller's behalf.
    pub initial_offset: i64,
    /// The compression to apply. Java's `compression` — in the group's
    /// intersection, hence mandatory.
    pub compression: Compression,
    /// How record timestamps are interpreted. Java's `timestampType`; starts at
    /// [`TimestampType::CreateTime`], the value `:656` passes on the caller's
    /// behalf.
    pub timestamp_type: TimestampType,
    /// The producer id. Java's `producerId`; starts at
    /// [`RecordBatch::NO_PRODUCER_ID`], the value `:656` passes on the caller's
    /// behalf.
    pub producer_id: i64,
    /// The producer epoch. Java's `producerEpoch`; starts at
    /// [`RecordBatch::NO_PRODUCER_EPOCH`].
    pub producer_epoch: i16,
    /// The base sequence number. Java's `baseSequence`; starts at
    /// [`RecordBatch::NO_SEQUENCE`].
    pub base_sequence: i32,
    /// The partition leader epoch. Java's `partitionLeaderEpoch`; starts at
    /// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub partition_leader_epoch: i32,
    /// Whether the batch is transactional. Java's `isTransactional`; starts at
    /// `false`, the value every narrower overload but `withTransactionalRecords`
    /// passes on the caller's behalf.
    pub is_transactional: bool,
    /// The records to write. Java's `records` varargs — in the group's
    /// intersection, hence mandatory.
    pub records: &'a [SimpleRecord],
}

/// Fluent builder for [`MemoryRecordsOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — returning
/// [`Error::LocalIllegalArgument`] if they were not set. Like
/// [`MemoryRecordsOptions`] it has no Java counterpart and exists solely to
/// satisfy that naming rule (DoD #7).
pub(crate) struct MemoryRecordsOptionsBuilder<'a> {
    magic: Option<i8>,
    initial_offset: Option<i64>,
    compression: Option<Compression>,
    timestamp_type: Option<TimestampType>,
    producer_id: Option<i64>,
    producer_epoch: Option<i16>,
    base_sequence: Option<i32>,
    partition_leader_epoch: Option<i32>,
    is_transactional: Option<bool>,
    records: Option<&'a [SimpleRecord]>,
}

impl<'a> Default for MemoryRecordsOptionsBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> MemoryRecordsOptionsBuilder<'a> {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    pub(crate) fn new() -> Self {
        Self {
            magic: None,
            initial_offset: None,
            compression: None,
            timestamp_type: None,
            producer_id: None,
            producer_epoch: None,
            base_sequence: None,
            partition_leader_epoch: None,
            is_transactional: None,
            records: None,
        }
    }

    /// Sets [`MemoryRecordsOptions::magic`].
    pub(crate) fn set_magic(mut self, magic: i8) -> Self {
        self.magic = Some(magic);
        self
    }

    /// Sets [`MemoryRecordsOptions::initial_offset`].
    pub(crate) fn set_initial_offset(mut self, initial_offset: i64) -> Self {
        self.initial_offset = Some(initial_offset);
        self
    }

    /// Sets [`MemoryRecordsOptions::compression`], a mandatory parameter:
    /// [`Self::build`] returns an error if it was not set.
    pub(crate) fn set_compression(mut self, compression: Compression) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Sets [`MemoryRecordsOptions::timestamp_type`].
    pub(crate) fn set_timestamp_type(mut self, timestamp_type: TimestampType) -> Self {
        self.timestamp_type = Some(timestamp_type);
        self
    }

    /// Sets [`MemoryRecordsOptions::producer_id`].
    pub(crate) fn set_producer_id(mut self, producer_id: i64) -> Self {
        self.producer_id = Some(producer_id);
        self
    }

    /// Sets [`MemoryRecordsOptions::producer_epoch`].
    pub(crate) fn set_producer_epoch(mut self, producer_epoch: i16) -> Self {
        self.producer_epoch = Some(producer_epoch);
        self
    }

    /// Sets [`MemoryRecordsOptions::base_sequence`].
    pub(crate) fn set_base_sequence(mut self, base_sequence: i32) -> Self {
        self.base_sequence = Some(base_sequence);
        self
    }

    /// Sets [`MemoryRecordsOptions::partition_leader_epoch`].
    pub(crate) fn set_partition_leader_epoch(mut self, partition_leader_epoch: i32) -> Self {
        self.partition_leader_epoch = Some(partition_leader_epoch);
        self
    }

    /// Sets [`MemoryRecordsOptions::is_transactional`].
    pub(crate) fn set_is_transactional(mut self, is_transactional: bool) -> Self {
        self.is_transactional = Some(is_transactional);
        self
    }

    /// Sets [`MemoryRecordsOptions::records`], a mandatory parameter:
    /// [`Self::build`] returns an error if it was not set. An empty slice is a
    /// valid value — Java's `:663` returns `MemoryRecords.EMPTY` for it — so
    /// "not set" and "set to empty" stay distinct.
    pub(crate) fn set_records(mut self, records: &'a [SimpleRecord]) -> Self {
        self.records = Some(records);
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the method. Today there is one mandatory set:
    /// `compression` and `records`. The other eight are not in it because
    /// Java's narrower overloads supply them themselves.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter of
    /// that set which was not given a setter call. Only presence is checked
    /// here; semantic validation belongs to the method the options are passed to
    /// (CLAUDE.md §2).
    pub(crate) fn build(self) -> Result<MemoryRecordsOptions<'a>, Error> {
        Ok(MemoryRecordsOptions {
            magic: self.magic.unwrap_or(RecordBatch::CURRENT_MAGIC_VALUE),
            initial_offset: self.initial_offset.unwrap_or(0),
            compression: self.compression.ok_or_else(|| Self::missing("compression"))?,
            timestamp_type: self.timestamp_type.unwrap_or(TimestampType::CreateTime),
            producer_id: self.producer_id.unwrap_or(RecordBatch::NO_PRODUCER_ID),
            producer_epoch: self.producer_epoch.unwrap_or(RecordBatch::NO_PRODUCER_EPOCH),
            base_sequence: self.base_sequence.unwrap_or(RecordBatch::NO_SEQUENCE),
            partition_leader_epoch: self.partition_leader_epoch.unwrap_or(RecordBatch::NO_PARTITION_LEADER_EPOCH),
            is_transactional: self.is_transactional.unwrap_or(false),
            records: self.records.ok_or_else(|| Self::missing("records"))?,
        })
    }

    /// Builds the [`Error::LocalIllegalArgument`] naming a mandatory parameter
    /// [`Self::build`] found unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "MemoryRecordsOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

impl PartialEq for MemoryRecords {
    fn eq(&self, other: &Self) -> bool {
        self.buffer == other.buffer
    }
}

impl Eq for MemoryRecords {}

impl std::hash::Hash for MemoryRecords {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.buffer.hash(state);
    }
}

impl std::fmt::Display for MemoryRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemoryRecords(size={})", self.size_in_bytes())
    }
}

/// Iterator over record batches in a `MemoryRecords`.
pub struct BatchIterator<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BatchIterator<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
}

impl<'a> Iterator for BatchIterator<'a> {
    type Item = DefaultRecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        // Need at least LOG_OVERHEAD bytes to read base_offset + length
        if self.pos + AbstractRecords::LOG_OVERHEAD > self.data.len() {
            return None;
        }

        // Read the batch length from the length field
        let length_bytes = &self.data[self.pos + RecordBatch::LENGTH_OFFSET..self.pos + RecordBatch::LENGTH_OFFSET + 4];
        let batch_length = i32::from_be_bytes(length_bytes.try_into().ok()?) as usize;
        let total_batch_size = AbstractRecords::LOG_OVERHEAD + batch_length;

        if self.pos + total_batch_size > self.data.len() {
            return None;
        }

        let batch_data = self.data[self.pos..self.pos + total_batch_size].to_vec();
        self.pos += total_batch_size;

        Some(DefaultRecordBatch::new(batch_data))
    }
}

/// Get the current time in milliseconds since epoch.
fn current_time_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::header::RecordHeader as HeaderImpl;
    use crate::common::record::internal::DefaultRecordBatch;
    use crate::common::record::internal::Record;

    /// All compression types to test with.
    fn all_compressions() -> Vec<Compression> {
        vec![
            Compression::none(),
            Compression::gzip(),
            Compression::snappy(),
            Compression::lz4(),
            Compression::zstd(),
        ]
    }

    /// Corresponds to Java's `MemoryRecordsTest.testIterator`.
    #[test]
    fn test_iterator() {
        let log_append_time = current_time_millis();

        for compression in all_compressions() {
            let first_offset = 0_i64;
            let pid = 134234_i64;
            let epoch = 28_i16;
            let first_sequence = 777_i32;
            let partition_leader_epoch = 998;

            let records = vec![
                SimpleRecord::with_timestamp_key_value(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
                SimpleRecord::with_timestamp_key_value(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
                SimpleRecord::with_timestamp_key_value(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
                SimpleRecord::with_timestamp_key_value(4, None, Some(b"4".to_vec())),
                SimpleRecord::with_timestamp_key_value(5, Some(b"d".to_vec()), None),
                SimpleRecord::with_timestamp_key_value(6, None, None),
            ];

            let mut builder = MemoryRecordsBuilder::with_default(
                Vec::with_capacity(1024),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
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
            );
            for record in &records {
                builder.append_simple(record);
            }
            let memory_records = builder.build();

            // Iterate twice to verify idempotency
            for _iteration in 0..2 {
                let mut total = 0;
                for batch in memory_records.batches() {
                    assert!(batch.is_valid());
                    assert_eq!(compression.compression_type(), batch.compression_type());
                    assert_eq!(first_offset + total as i64, batch.base_offset());

                    assert_eq!(pid, batch.producer_id());
                    assert_eq!(epoch, batch.producer_epoch());
                    assert_eq!(first_sequence + total as i32, batch.base_sequence());
                    assert_eq!(partition_leader_epoch, batch.partition_leader_epoch());
                    assert_eq!(Some(records.len() as i32), batch.count_or_null());
                    assert_eq!(TimestampType::CreateTime, batch.timestamp_type());
                    assert_eq!(records[records.len() - 1].timestamp(), batch.max_timestamp());

                    let mut record_count = 0;
                    for record in batch.iter_records().unwrap() {
                        record.ensure_valid().unwrap();
                        assert!(record.has_magic(batch.magic()));
                        assert!(!record.is_compressed());
                        assert_eq!(first_offset + total as i64, record.offset());
                        assert_eq!(records[total].key(), record.key());
                        assert_eq!(records[total].value(), record.value());
                        assert_eq!(first_sequence + total as i32, record.sequence());
                        assert!(!record.has_timestamp_type(TimestampType::LogAppendTime));
                        assert_eq!(records[total].timestamp(), record.timestamp());
                        assert!(!record.has_timestamp_type(TimestampType::NoTimestampType));
                        // For v2, has_timestamp_type(CreateTime) returns false
                        assert!(!record.has_timestamp_type(TimestampType::CreateTime));

                        total += 1;
                        record_count += 1;
                    }

                    assert_eq!(batch.base_offset() + record_count as i64 - 1, batch.last_offset());
                }
            }
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testHasRoomForMethod`.
    #[test]
    fn test_has_room_for_method() {
        for compression in all_compressions() {
            let mut builder = MemoryRecords::builder_with_initial_capacity_magic(
                1024,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
            );
            builder.append_kv(0, Some(b"a"), Some(b"1"));
            assert!(builder.has_room_for(1, Some(b"b"), Some(b"2"), RecordBatch::EMPTY_HEADERS));
            builder.close();
            assert!(!builder.has_room_for(1, Some(b"b"), Some(b"2"), RecordBatch::EMPTY_HEADERS));
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testHasRoomForMethodWithHeaders`.
    #[test]
    fn test_has_room_for_method_with_headers() {
        let log_append_time = current_time_millis();

        for compression in all_compressions() {
            let mut builder = MemoryRecords::builder_with_initial_capacity_magic(
                120,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
            );
            builder.append_kv(log_append_time, Some(b"key"), Some(b"value"));

            let mut headers = Vec::new();
            for _ in 0..10 {
                headers.push(HeaderImpl::new("hello".to_string(), Some(b"world.world".to_vec())));
            }

            // A record without headers should fit
            assert!(builder.has_room_for(log_append_time, Some(b"key"), Some(b"value"), RecordBatch::EMPTY_HEADERS,));
            // A record with many headers should not fit (for v2)
            assert!(!builder.has_room_for(log_append_time, Some(b"key"), Some(b"value"), &headers));
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testChecksum` (v2 only).
    #[test]
    fn test_checksum_v2() {
        // We get reasonable coverage with uncompressed and one compression type
        for (compression, expected_checksum) in &[
            (Compression::none(), 3851219455_u32),
            (Compression::lz4(), 2745969314_u32),
        ] {
            let records = vec![
                SimpleRecord::with_timestamp_key_value(283843, Some(b"key1".to_vec()), Some(b"value1".to_vec())),
                SimpleRecord::with_timestamp_key_value(1234, Some(b"key2".to_vec()), Some(b"value2".to_vec())),
            ];
            let mem_records =
                MemoryRecords::with_records_with_magic(RecordBatch::MAGIC_VALUE_V2, compression.clone(), &records);
            let batch = mem_records.batches().next().unwrap();
            assert_eq!(
                *expected_checksum,
                batch.checksum(),
                "Unexpected checksum for compression {:?}",
                compression.compression_type()
            );
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testWithRecords`.
    #[test]
    fn test_with_records() {
        for compression in all_compressions() {
            let mem_records = MemoryRecords::with_records_with_magic(
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                &[SimpleRecord::with_timestamp_key_value(
                    10,
                    Some(b"key1".to_vec()),
                    Some(b"value1".to_vec()),
                )],
            );
            let batch = mem_records.batches().next().unwrap();
            let record = batch.iter_records().unwrap().into_iter().next().unwrap();
            assert_eq!(Some(b"key1".as_slice()), record.key());
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testNextBatchSize` (v2 only).
    #[test]
    fn test_first_batch_size() {
        let log_append_time = current_time_millis();

        for compression in all_compressions() {
            let mut builder = MemoryRecords::builder_with_initial_capacity_magic_log_append_time(
                2048,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::LogAppendTime,
                0,
                log_append_time,
            );
            builder.append_kv(10, None, Some(b"abc"));
            let records = builder.build();

            let size = records.size_in_bytes();
            assert_eq!(Some(size), records.first_batch_size().unwrap());

            // size not in buffer (only 1 byte)
            let short_records = MemoryRecords::new(records.buffer()[..1].to_vec().into());
            assert_eq!(None, short_records.first_batch_size().unwrap());

            // magic not in buffer (only LOG_OVERHEAD bytes = 12)
            let short_records = MemoryRecords::new(records.buffer()[..AbstractRecords::LOG_OVERHEAD].to_vec().into());
            assert_eq!(None, short_records.first_batch_size().unwrap());

            // payload not in buffer, but header up to magic is present
            let short_records =
                MemoryRecords::new(records.buffer()[..AbstractRecords::HEADER_SIZE_UP_TO_MAGIC].to_vec().into());
            assert_eq!(Some(size), short_records.first_batch_size().unwrap());

            // Invalid magic byte (10) should return CorruptMessage error
            let mut corrupt_magic_buf = records.buffer().to_vec();
            corrupt_magic_buf[RecordBatch::MAGIC_OFFSET] = 10;
            let corrupt_records = MemoryRecords::new(corrupt_magic_buf.into());
            let err = corrupt_records.first_batch_size().unwrap_err();
            assert_eq!(err.error(), Errors::CorruptMessage);

            // Invalid record size (set LSB of size field to 0, making it too small)
            let mut corrupt_size_buf = records.buffer().to_vec();
            corrupt_size_buf[RecordBatch::LENGTH_OFFSET + 3] = 0;
            let corrupt_records = MemoryRecords::new(corrupt_size_buf.into());
            let err = corrupt_records.first_batch_size().unwrap_err();
            assert_eq!(err.error(), Errors::CorruptMessage);
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testSlice` (v2 only).
    #[test]
    fn test_slice() {
        for compression in all_compressions() {
            // Create records with multiple batches
            let mut buf = Vec::new();
            for (offset, count) in &[(0_i64, 3_usize), (6_i64, 8_usize), (15_i64, 4_usize)] {
                let mut builder = MemoryRecords::builder_with_initial_capacity_magic(
                    1024,
                    RecordBatch::MAGIC_VALUE_V2,
                    compression.clone(),
                    TimestampType::CreateTime,
                    *offset,
                );
                for i in 0..*count {
                    builder.append_with_offset_bytes(
                        *offset + i as i64,
                        0,
                        Some(format!("key{}", i).as_bytes()),
                        Some(format!("val{}", i).as_bytes()),
                    );
                }
                let batch_records = builder.build();
                buf.extend_from_slice(batch_records.buffer());
            }

            let records = MemoryRecords::new(buf.into());

            // Test slicing from start
            let sliced = records.slice(0, records.size_in_bytes());
            assert_eq!(records.size_in_bytes(), sliced.size_in_bytes());
            assert_eq!(records.valid_bytes(), sliced.valid_bytes());

            let items: Vec<_> = records.batches().collect();

            // Test slicing past first batch
            let first_size = items[0].size_in_bytes();
            let sliced = records.slice(first_size, records.size_in_bytes() - first_size);
            assert_eq!(records.size_in_bytes() - first_size, sliced.size_in_bytes());

            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(items.len() - 1, sliced_batches.len());
            assert!(sliced.valid_bytes() <= sliced.size_in_bytes());

            // Read from second message and size is past the end of the file
            // (Java: records.slice(first.sizeInBytes(), records.sizeInBytes()))
            let sliced = records.slice(first_size, records.size_in_bytes());
            assert_eq!(records.size_in_bytes() - first_size, sliced.size_in_bytes());
            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(items.len() - 1, sliced_batches.len());
            assert!(sliced.valid_bytes() <= sliced.size_in_bytes());

            // Read from second message and position + size overflows
            // (Java: records.slice(first.sizeInBytes(), Integer.MAX_VALUE))
            let sliced = records.slice(first_size, usize::MAX);
            assert_eq!(records.size_in_bytes() - first_size, sliced.size_in_bytes());
            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(items.len() - 1, sliced_batches.len());
            assert!(sliced.valid_bytes() <= sliced.size_in_bytes());

            // Read a single batch starting from second batch
            let second_size = items[1].size_in_bytes();
            let sliced = records.slice(first_size, second_size);
            assert_eq!(second_size, sliced.size_in_bytes());
            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(1, sliced_batches.len());

            // Read from second message and size is past the end on an already-sliced view
            // (Java: records.slice(1, records.sizeInBytes() - 1)
            //               .slice(first.sizeInBytes() - 1, records.sizeInBytes()))
            let sliced = records
                .slice(1, records.size_in_bytes() - 1)
                .slice(first_size - 1, records.size_in_bytes());
            assert_eq!(records.size_in_bytes() - first_size, sliced.size_in_bytes());
            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(items.len() - 1, sliced_batches.len());
            assert!(sliced.valid_bytes() <= sliced.size_in_bytes());

            // Read from second message and position + size overflows on already-sliced view
            // (Java: records.slice(1, records.sizeInBytes() - 1)
            //               .slice(first.sizeInBytes() - 1, Integer.MAX_VALUE))
            let sliced = records.slice(1, records.size_in_bytes() - 1).slice(first_size - 1, usize::MAX);
            assert_eq!(records.size_in_bytes() - first_size, sliced.size_in_bytes());
            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(items.len() - 1, sliced_batches.len());
            assert!(sliced.valid_bytes() <= sliced.size_in_bytes());
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testSliceEmptyRecords`.
    #[test]
    fn test_slice_empty_records() {
        let empty = MemoryRecords::empty();
        let sliced = empty.slice(0, 0);
        assert_eq!(0, sliced.size_in_bytes());
        assert_eq!(0, sliced.batches().count());
    }

    /// Corresponds to Java's `MemoryRecordsTest.testSliceInvalidPosition`.
    #[test]
    #[should_panic(expected = "Slice from position")]
    fn test_slice_invalid_position() {
        let records = MemoryRecords::with_records(
            Compression::none(),
            &[SimpleRecord::with_timestamp_key_value(
                1,
                Some(b"k".to_vec()),
                Some(b"v".to_vec()),
            )],
        );
        records.slice(records.size_in_bytes() + 1, records.size_in_bytes());
    }

    /// Corresponds to Java's `MemoryRecordsTest.testSliceForAlreadySlicedMemoryRecords`.
    #[test]
    fn test_slice_for_already_sliced_memory_records() {
        for compression in all_compressions() {
            // Create records with multiple batches
            let mut buf = Vec::new();
            for (offset, count) in &[
                (0_i64, 5_usize),
                (5_i64, 10_usize),
                (15_i64, 12_usize),
                (27_i64, 4_usize),
            ] {
                let mut builder = MemoryRecords::builder_with_initial_capacity_magic(
                    1024,
                    RecordBatch::MAGIC_VALUE_V2,
                    compression.clone(),
                    TimestampType::CreateTime,
                    *offset,
                );
                for i in 0..*count {
                    builder.append_with_offset_bytes(
                        *offset + i as i64,
                        0,
                        Some(format!("key{}", i).as_bytes()),
                        Some(format!("val{}", i).as_bytes()),
                    );
                }
                let batch_records = builder.build();
                buf.extend_from_slice(batch_records.buffer());
            }
            let records = MemoryRecords::new(buf.into());

            let items: Vec<DefaultRecordBatch> = records.batches().collect();

            // Slice from third batch
            let position: usize = items[0].size_in_bytes() + items[1].size_in_bytes();
            let sliced = records.slice(position, records.size_in_bytes() - position);
            assert_eq!(records.size_in_bytes() - position, sliced.size_in_bytes());
            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(items.len() - 2, sliced_batches.len());

            // Further slice from fourth batch
            let position2 = items[2].size_in_bytes();
            let final_sliced = sliced.slice(position2, sliced.size_in_bytes() - position2);
            assert_eq!(sliced.size_in_bytes() - position2, final_sliced.size_in_bytes());
            let final_batches: Vec<_> = final_sliced.batches().collect();
            assert_eq!(items.len() - 3, final_batches.len());
        }
    }

    // Note: filterTo tests (testFilterToPreservesPartitionLeaderEpoch, testFilterToEmptyBatchRetention,
    // testEmptyBatchRetention, testEmptyBatchDeletion, testBaseTimestampToDeleteHorizonConversion,
    // testFilterToBatchDiscard, testFilterToAlreadyCompactedLog, testFilterToPreservesProducerInfo,
    // testFilterToWithUndersizedBuffer, testFilterTo, testFilterToPreservesLogAppendTime) are
    // skipped because filterTo is not implemented in the Rust version. The filterTo method is
    // a server-side operation used for log compaction and not needed for the producer path.

    // Note: testBuildEndTxnMarker and testBuildLeaderChangeMessage are skipped because
    // EndTransactionMarker, ControlRecordType, and LeaderChangeMessage/ControlRecordUtils
    // are not yet implemented.

    // Note: testUnsupportedCompress is skipped because it tests magic v0/v1 which
    // we do not support in the Rust producer path.

    // ── `has_complete_first_batch` — Java's `batches().iterator().hasNext()`

    fn one_record_batch() -> Vec<u8> {
        let records = [SimpleRecord::with_timestamp_key_value(
            0,
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
        )];
        MemoryRecords::with_records_with_magic_initial_offset_timestamp_type(
            2,
            0,
            Compression::none(),
            TimestampType::CreateTime,
            &records,
        )
        .buffer()
        .to_vec()
    }

    /// A complete batch: `nextBatchSize()` returns a size AND the whole batch
    /// is present, so `nextBatch()` is non-null.
    #[test]
    fn test_has_complete_first_batch_complete() {
        let records = MemoryRecords::readable_records(&one_record_batch());
        assert!(records.has_complete_first_batch().unwrap());
        // Consistency with the iterator that carries the same completeness
        // test, so the two cannot drift apart.
        assert!(records.batches().next().is_some());
    }

    /// An intact header declaring more bytes than are present. This is the case
    /// Java's `nextBatch()` rejects via `remaining < batchSize`
    /// (`ByteBufferLogInputStream.java:44-45`) but `nextBatchSize()` accepts —
    /// so `first_batch_size` alone reports a batch where `hasNext()` is false.
    /// A broker cutting a fetch response at `max.partition.fetch.bytes`
    /// produces exactly this.
    #[test]
    fn test_has_complete_first_batch_truncated_body() {
        let full = one_record_batch();
        let truncated = &full[..full.len() - 1];
        let records = MemoryRecords::readable_records(truncated);
        // The header still validates and still declares the full size...
        let declared = records.first_batch_size().unwrap().expect("header is intact");
        assert!(declared > truncated.len());
        // ...but there is no readable batch.
        assert!(!records.has_complete_first_batch().unwrap());
        assert!(records.batches().next().is_none());
    }

    /// Fewer bytes than `LOG_OVERHEAD`: `nextBatchSize()` returns null.
    #[test]
    fn test_has_complete_first_batch_no_header() {
        let records = MemoryRecords::readable_records(&[0u8; AbstractRecords::LOG_OVERHEAD - 1]);
        assert_eq!(None, records.first_batch_size().unwrap());
        assert!(!records.has_complete_first_batch().unwrap());
    }

    /// A record size below the minimum overhead: Java's `nextBatchSize()`
    /// throws `CorruptRecordException` (`ByteBufferLogInputStream.java:72-74`),
    /// so `hasNext()` propagates it rather than answering "no batch". Folding
    /// the two would relabel a retriable `CORRUPT_MESSAGE` as something else.
    #[test]
    fn test_has_complete_first_batch_propagates_corrupt_size() {
        let mut buf = vec![0u8; AbstractRecords::LOG_OVERHEAD];
        buf[RecordBatch::LENGTH_OFFSET..RecordBatch::LENGTH_OFFSET + 4].copy_from_slice(&3i32.to_be_bytes());
        let err = MemoryRecords::readable_records(&buf).has_complete_first_batch().unwrap_err();
        assert_eq!(Errors::CorruptMessage, err.error());
        assert_eq!("Record size 3 is less than the minimum record overhead (14)", err.message());
    }

    /// The magic-byte half of the same check
    /// (`ByteBufferLogInputStream.java:83-84`).
    #[test]
    fn test_has_complete_first_batch_propagates_corrupt_magic() {
        let mut buf = vec![0u8; AbstractRecords::HEADER_SIZE_UP_TO_MAGIC];
        buf[RecordBatch::LENGTH_OFFSET..RecordBatch::LENGTH_OFFSET + 4].copy_from_slice(&64i32.to_be_bytes());
        buf[RecordBatch::MAGIC_OFFSET] = 99;
        let err = MemoryRecords::readable_records(&buf).has_complete_first_batch().unwrap_err();
        assert_eq!(Errors::CorruptMessage, err.error());
        assert_eq!("Invalid magic found in record: 99", err.message());
    }

    /// An empty buffer has no batch and no error.
    #[test]
    fn test_has_complete_first_batch_empty() {
        assert!(!MemoryRecords::empty().has_complete_first_batch().unwrap());
    }

    /// A batch whose header parses (so the batch iterator yields it) but whose
    /// record stream does not contributes NO records, rather than aborting the
    /// whole iteration or yielding garbage.
    ///
    /// Java's `RecordBatchIterator` throws instead
    /// (`DefaultRecordBatch.java:645-652`); Rust cannot report from an
    /// `Iterator`, so the failure is logged and the batch skipped — the
    /// deviation documented on [`MemoryRecords::records`]. This pins the
    /// contract that the *following* well-formed batch is still iterated, which
    /// is what makes skipping (rather than truncating) the right choice: a
    /// caller like `ProducerBatch::split` must still see the records it can
    /// parse.
    #[test]
    fn test_records_skips_an_unparseable_batch_and_keeps_going() {
        let mut first = one_record_batch();
        // Declare 5 records in a batch that holds 1: `iter_records` fails with
        // "Incorrect declared batch size, premature EOF reached" while the
        // batch LENGTH field — the only thing `BatchIterator` reads — is
        // untouched, so the batch is still yielded.
        first[RecordBatch::RECORDS_COUNT_OFFSET..RecordBatch::RECORDS_COUNT_OFFSET + 4]
            .copy_from_slice(&5i32.to_be_bytes());
        assert!(
            DefaultRecordBatch::new(first.clone()).iter_records().is_err(),
            "fixture precondition: the batch must be unparseable"
        );

        let good = [SimpleRecord::with_timestamp_key_value(
            0,
            Some(b"k2".to_vec()),
            Some(b"v2".to_vec()),
        )];
        let second = MemoryRecords::with_records_with_magic_initial_offset_timestamp_type(
            2,
            10,
            Compression::none(),
            TimestampType::CreateTime,
            &good,
        )
        .buffer()
        .to_vec();

        let mut buf = first;
        buf.extend_from_slice(&second);
        let records = MemoryRecords::readable_records(&buf);
        assert_eq!(2, records.batches().count(), "both batch headers are readable");

        let decoded: Vec<Vec<u8>> = records
            .records()
            .map(|r| r.value().map(<[u8]>::to_vec).unwrap_or_default())
            .collect();
        assert_eq!(vec![b"v2".to_vec()], decoded, "only the parseable batch contributes");
    }
}
