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
//! or one of the [`builder()`](MemoryRecords::builder) variants.
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

    /// Create a builder with default parameters.
    pub fn builder(
        initial_capacity: usize,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
    ) -> MemoryRecordsBuilder {
        Self::builder_with_magic(
            initial_capacity,
            RecordBatch::CURRENT_MAGIC_VALUE,
            compression,
            timestamp_type,
            base_offset,
        )
    }

    /// Create a builder with a specific magic value.
    pub fn builder_with_magic(
        initial_capacity: usize,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
    ) -> MemoryRecordsBuilder {
        let log_append_time = if timestamp_type == TimestampType::LogAppendTime {
            current_time_millis()
        } else {
            RecordBatch::NO_TIMESTAMP
        };
        Self::builder_full(
            initial_capacity,
            magic,
            compression,
            timestamp_type,
            base_offset,
            log_append_time,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            initial_capacity,
        )
    }

    /// Create a builder using a pre-allocated buffer.
    ///
    /// This is the equivalent of Java's `MemoryRecords.builder(ByteBuffer, ...)` overload
    /// that accepts an existing buffer (e.g., from a buffer pool) instead of allocating a
    /// new one.
    pub fn builder_with_buffer(
        buffer: Vec<u8>,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
    ) -> MemoryRecordsBuilder {
        let write_limit = buffer.capacity();
        let log_append_time = if timestamp_type == TimestampType::LogAppendTime {
            current_time_millis()
        } else {
            RecordBatch::NO_TIMESTAMP
        };
        MemoryRecordsBuilder::new_default(
            buffer,
            0,
            magic,
            compression,
            timestamp_type,
            base_offset,
            log_append_time,
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
    pub fn builder_with_max_size(
        initial_capacity: usize,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
        max_size: usize,
    ) -> MemoryRecordsBuilder {
        let log_append_time = if timestamp_type == TimestampType::LogAppendTime {
            current_time_millis()
        } else {
            RecordBatch::NO_TIMESTAMP
        };
        Self::builder_full(
            initial_capacity,
            RecordBatch::CURRENT_MAGIC_VALUE,
            compression,
            timestamp_type,
            base_offset,
            log_append_time,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            max_size,
        )
    }

    /// Create a builder with magic, compression, timestamp type, and log append time.
    pub fn builder_with_log_append_time(
        initial_capacity: usize,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
        log_append_time: i64,
    ) -> MemoryRecordsBuilder {
        Self::builder_full(
            initial_capacity,
            magic,
            compression,
            timestamp_type,
            base_offset,
            log_append_time,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            initial_capacity,
        )
    }

    /// Create a builder with producer state.
    #[allow(clippy::too_many_arguments)]
    pub fn builder_with_producer(
        initial_capacity: usize,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
        log_append_time: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
    ) -> MemoryRecordsBuilder {
        Self::builder_full(
            initial_capacity,
            magic,
            compression,
            timestamp_type,
            base_offset,
            log_append_time,
            producer_id,
            producer_epoch,
            base_sequence,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            initial_capacity,
        )
    }

    /// Create a builder with full parameters (no delete horizon).
    #[allow(clippy::too_many_arguments)]
    pub fn builder_full(
        initial_capacity: usize,
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
    ) -> MemoryRecordsBuilder {
        let buffer = Vec::with_capacity(initial_capacity);
        MemoryRecordsBuilder::new_default(
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
        )
    }

    /// Create a builder with full parameters including delete horizon.
    #[allow(clippy::too_many_arguments)]
    pub fn builder_full_with_delete_horizon(
        initial_capacity: usize,
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
    ) -> MemoryRecordsBuilder {
        let buffer = Vec::with_capacity(initial_capacity);
        MemoryRecordsBuilder::new(
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
        )
    }

    // -- Convenience factory methods for creating records directly --

    /// Create a `MemoryRecords` with the given records using default settings.
    pub fn with_records(compression: Compression, records: &[SimpleRecord]) -> MemoryRecords {
        Self::with_records_magic(RecordBatch::CURRENT_MAGIC_VALUE, compression, records)
    }

    /// Create a `MemoryRecords` with a specific magic value.
    pub fn with_records_magic(magic: i8, compression: Compression, records: &[SimpleRecord]) -> MemoryRecords {
        Self::with_records_at_offset(magic, 0, compression, TimestampType::CreateTime, records)
    }

    /// Create a `MemoryRecords` with records starting at a specific offset.
    pub fn with_records_at_offset(
        magic: i8,
        initial_offset: i64,
        compression: Compression,
        timestamp_type: TimestampType,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        Self::with_records_full(
            magic,
            initial_offset,
            compression,
            timestamp_type,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            false,
            records,
        )
    }

    /// Create a `MemoryRecords` with records at a specific offset and partition leader epoch.
    pub fn with_records_at_offset_plep(
        initial_offset: i64,
        compression: Compression,
        partition_leader_epoch: i32,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        Self::with_records_full(
            RecordBatch::CURRENT_MAGIC_VALUE,
            initial_offset,
            compression,
            TimestampType::CreateTime,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            partition_leader_epoch,
            false,
            records,
        )
    }

    /// Create idempotent records.
    pub fn with_idempotent_records(
        compression: Compression,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        Self::with_records_full(
            RecordBatch::CURRENT_MAGIC_VALUE,
            0,
            compression,
            TimestampType::CreateTime,
            producer_id,
            producer_epoch,
            base_sequence,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            false,
            records,
        )
    }

    /// Create transactional records.
    pub fn with_transactional_records(
        compression: Compression,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        Self::with_records_full(
            RecordBatch::CURRENT_MAGIC_VALUE,
            0,
            compression,
            TimestampType::CreateTime,
            producer_id,
            producer_epoch,
            base_sequence,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            true,
            records,
        )
    }

    /// Create a `MemoryRecords` with full parameters.
    #[allow(clippy::too_many_arguments)]
    pub fn with_records_full(
        magic: i8,
        initial_offset: i64,
        compression: Compression,
        timestamp_type: TimestampType,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        partition_leader_epoch: i32,
        is_transactional: bool,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        if records.is_empty() {
            return MemoryRecords::empty();
        }

        let size_estimate = AbstractRecords::estimate_size_in_bytes(magic, compression.compression_type(), records);
        let log_append_time = if timestamp_type == TimestampType::LogAppendTime {
            current_time_millis()
        } else {
            RecordBatch::NO_TIMESTAMP
        };

        let mut builder = MemoryRecordsBuilder::new_default(
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
                SimpleRecord::new_with_key_value(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
                SimpleRecord::new_with_key_value(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
                SimpleRecord::new_with_key_value(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
                SimpleRecord::new_with_key_value(4, None, Some(b"4".to_vec())),
                SimpleRecord::new_with_key_value(5, Some(b"d".to_vec()), None),
                SimpleRecord::new_with_key_value(6, None, None),
            ];

            let mut builder = MemoryRecordsBuilder::new_default(
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
            let mut builder = MemoryRecords::builder_with_magic(
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
            let mut builder = MemoryRecords::builder_with_magic(
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
                SimpleRecord::new_with_key_value(283843, Some(b"key1".to_vec()), Some(b"value1".to_vec())),
                SimpleRecord::new_with_key_value(1234, Some(b"key2".to_vec()), Some(b"value2".to_vec())),
            ];
            let mem_records =
                MemoryRecords::with_records_magic(RecordBatch::MAGIC_VALUE_V2, compression.clone(), &records);
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
            let mem_records = MemoryRecords::with_records_magic(
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                &[SimpleRecord::new_with_key_value(
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
            let mut builder = MemoryRecords::builder_with_log_append_time(
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
                let mut builder = MemoryRecords::builder_with_magic(
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
            &[SimpleRecord::new_with_key_value(
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
                let mut builder = MemoryRecords::builder_with_magic(
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
        let records = [SimpleRecord::new(
            0,
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
            vec![],
        )];
        MemoryRecords::with_records_at_offset(2, 0, Compression::none(), TimestampType::CreateTime, &records)
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

        let good = [SimpleRecord::new(0, Some(b"k2".to_vec()), Some(b"v2".to_vec()), vec![])];
        let second =
            MemoryRecords::with_records_at_offset(2, 10, Compression::none(), TimestampType::CreateTime, &good)
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
