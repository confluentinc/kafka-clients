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

//! Translation of `org.apache.kafka.common.record.MemoryRecords`.
//!
//! A [`Records`] implementation backed by a [`bytes::Bytes`] buffer of one or
//! more record batches. The Java class wraps a `ByteBuffer`; we wrap a
//! `Bytes` so:
//!
//! * `clone()` is a cheap refcount bump (matches Java's `buffer.duplicate()`
//!   semantics).
//! * [`MemoryRecords::slice`] is zero-copy via [`Bytes::slice`] — the sliced
//!   view aliases the same backing storage exactly as
//!   `ByteBuffer.duplicate().position(p).limit(p + size).slice()` does in
//!   Java.
//! * The downstream `RecordsSend` path (Phase 5 wire send) can hand a
//!   `Bytes` chunk to `write_vectored` without an intermediate copy.
//!
//! The Phase 3d-3 surface covers the *read* path — construction from an
//! existing buffer (`readableRecords`), iteration through batches/records,
//! slicing, and `firstBatchSize`. The construction-from-`SimpleRecord`
//! factories (`withRecords`, `withIdempotentRecords`,
//! `withTransactionalRecords`, `withEndTransactionMarker`,
//! `withLeaderChangeMessage`, etc.) are deferred to Phase 3d-4 because they
//! all internally route through `MemoryRecordsBuilder`. Likewise the
//! `filterTo` rewriter and the `RecordFilter`/`FilterResult` nested classes
//! are deferred — they too rely on `MemoryRecordsBuilder` for batch
//! reconstruction.

use std::sync::OnceLock;

use bytes::Bytes;

use crate::common::errors::KafkaError;
use crate::common::record::byte_buffer_log_input_stream::ByteBufferLogInputStream;
use crate::common::record::default_record::DefaultRecord;
use crate::common::record::default_record_batch::DefaultRecordBatch;
use crate::common::record::record_batch_iterator::RecordBatchIterator;
use crate::common::record::{BaseRecords, DefaultRecordsSend, Record, RecordBatch, Records, TransferableRecords};

/// A [`Records`] implementation backed by a [`Bytes`] buffer of contiguous
/// record batches.
///
/// Mirrors Java's `org.apache.kafka.common.record.MemoryRecords`. The buffer
/// must contain zero or more concatenated v2 batches; legacy v0/v1 magic
/// bytes are rejected by [`ByteBufferLogInputStream`] when iterated.
#[derive(Clone, Debug)]
pub struct MemoryRecords {
    buffer: Bytes,
}

impl MemoryRecords {
    /// The empty record set. Mirrors Java's
    /// `MemoryRecords.EMPTY = readableRecords(ByteBuffer.allocate(0))`.
    pub fn empty() -> &'static MemoryRecords {
        static EMPTY: OnceLock<MemoryRecords> = OnceLock::new();
        EMPTY.get_or_init(|| MemoryRecords::readable_records(Bytes::new()))
    }

    /// Construct an instance for reading the supplied buffer. Mirrors Java's
    /// `MemoryRecords.readableRecords(ByteBuffer)` static factory.
    ///
    /// The `Bytes` is moved in — no copy — and downstream slicing is also
    /// zero-copy via [`Bytes::slice`].
    pub fn readable_records(buffer: Bytes) -> Self {
        MemoryRecords { buffer }
    }

    /// Convenience: construct from an owned `Vec<u8>`. The vec is moved into a
    /// `Bytes` without copying.
    pub fn readable_records_from_vec(vec: Vec<u8>) -> Self {
        MemoryRecords::readable_records(Bytes::from(vec))
    }

    /// Borrow the underlying buffer (read-only). Mirrors Java's `buffer()`
    /// (which returns `buffer.duplicate()`); both expose a read-only view
    /// over the same backing storage.
    pub fn buffer(&self) -> &Bytes {
        &self.buffer
    }

    /// The total number of bytes in this message set not including any
    /// partial, trailing messages. Mirrors Java's `validBytes()`. Walks
    /// every batch; corrupt records terminate early.
    ///
    /// Java caches the result on first call. We do not — `MemoryRecords` is
    /// cheaply clonable and the caller can cache themselves if needed.
    pub fn valid_bytes(&self) -> i32 {
        let mut bytes = 0i32;
        for batch in self.batch_iterator() {
            // Java returns the running total on `CorruptRecordException`
            // (it propagates the exception). We instead stop at the first
            // corrupt batch and return what we've counted, mirroring
            // `Records::batches`'s "skip-corrupt-and-stop" semantic.
            match batch {
                Ok(b) => bytes += b.size_in_bytes(),
                Err(_) => break,
            }
        }
        bytes
    }

    /// Validate the header of the first batch and return its full size
    /// (including [`crate::common::record::records::LOG_OVERHEAD`]).
    ///
    /// Mirrors Java's `firstBatchSize()`. Returns:
    ///
    /// * `Ok(Some(n))` — header is valid, batch size is `n` bytes;
    /// * `Ok(None)` — buffer doesn't yet hold enough bytes for the header;
    /// * `Err(KafkaError::CorruptRecord)` — record size or magic is invalid.
    pub fn first_batch_size(&self) -> Result<Option<i32>, KafkaError> {
        // Java: `if (buffer.remaining() < HEADER_SIZE_UP_TO_MAGIC) return null;`
        // before calling `nextBatchSize()`. This early-out short-circuits
        // the SIZE-field validation in `next_batch_size`: a buffer with a
        // partial header skips raising a CorruptRecord and just returns
        // None, mirroring Java's "not enough yet, try again" semantic.
        if self.buffer.len() < crate::common::record::records::HEADER_SIZE_UP_TO_MAGIC {
            return Ok(None);
        }
        ByteBufferLogInputStream::new(self.buffer.as_ref(), i32::MAX).next_batch_size()
    }

    /// Iterator over batches that surfaces corrupt-record errors. Used
    /// internally by [`MemoryRecords::valid_bytes`] and the trait-level
    /// `batches()` implementation.
    fn batch_iterator(&self) -> RecordBatchIterator<ByteBufferLogInputStream<'_>, DefaultRecordBatch> {
        // Java passes `Integer.MAX_VALUE` for the per-batch size cap.
        RecordBatchIterator::new(ByteBufferLogInputStream::new(self.buffer.as_ref(), i32::MAX))
    }

    /// Build a [`DefaultRecordsSend`] sized to this record set's full size.
    ///
    /// Mirrors Java's `AbstractRecords#toSend()` override (which returns
    /// `new DefaultRecordsSend<>(this)`). Consuming `self` here mirrors
    /// Java's reference-passing — the underlying `Bytes` is refcount-shared
    /// so callers that still need the records can `clone()` first.
    pub fn to_send(self) -> DefaultRecordsSend<MemoryRecords> {
        DefaultRecordsSend::new(self)
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
        // Mirrors Java's toString().
        write!(f, "MemoryRecords(size={}, buffer={:?})", self.size_in_bytes(), self.buffer)
    }
}

impl BaseRecords for MemoryRecords {
    fn size_in_bytes(&self) -> i32 {
        // Java returns `buffer.limit()`. With `Bytes` the "limit" is the
        // length of the slice — they're the same: the materialized window
        // that callers can read.
        self.buffer.len() as i32
    }
}

impl TransferableRecords for MemoryRecords {}

impl Records for MemoryRecords {
    fn batches<'a>(&'a self) -> Box<dyn Iterator<Item = Result<Box<dyn RecordBatch + 'a>, KafkaError>> + 'a> {
        // Mirrors Java's `MemoryRecordsBatchIterator` which throws
        // `CorruptRecordException` on the first malformed batch. The Rust
        // translation surfaces that signal as `Err(KafkaError::CorruptRecord)`
        // at the same iteration step. The underlying `RecordBatchIterator`
        // is "poisoned" after the first error, so subsequent calls return
        // `None` (matching Java's "throw and stop" semantic).
        Box::new(
            self.batch_iterator()
                .map(|r| r.map(|b| Box::new(b) as Box<dyn RecordBatch + 'a>)),
        )
    }

    fn records<'a>(&'a self) -> Box<dyn Iterator<Item = Box<dyn Record + 'a>> + 'a> {
        // Mirrors Java's `AbstractRecords.records()` — flat-map each batch
        // into its records. Although `RecordBatch::iter` yields
        // `Box<dyn Record + '_>` borrowing from the batch buffer, every
        // concrete record produced by the v2 batch path
        // (`DefaultRecord`) owns its key/value/headers (`Option<Bytes>`,
        // `Vec<RecordHeader>`). The borrow in the trait signature is
        // therefore conservative — the records survive the batch drop.
        //
        // We collect each batch's decoded records into a `Vec<DefaultRecord>`
        // and discard the batch wrapper. Per-batch buffering is unavoidable
        // because the trait object's lifetime is tied to the borrowing
        // batch; routing through the concrete `DefaultRecord` lets us drop
        // the batch and yield owned records.
        //
        // Key/value `Bytes` still alias the original `Bytes` payload — the
        // collect cost is one `Box<dyn Record>` per record (Rc-shared
        // payload bytes are not copied). This is the *consumer-side*
        // iterator (broker rewriting / metrics) and is **not on the
        // producer hot path**.
        let batches = self.batch_iterator();
        Box::new(BatchFlatIter { batches, current: Vec::new().into_iter() })
    }

    fn slice(&self, position: i32, size: i32) -> Result<Box<dyn Records + '_>, KafkaError> {
        Ok(Box::new(self.slice_inner(position, size)?))
    }
}

impl MemoryRecords {
    /// Concrete typed slice — used by the trait `slice` and by callers that
    /// need a `MemoryRecords` directly. Mirrors Java's
    /// `MemoryRecords#slice(int position, int size)` returning
    /// `MemoryRecords`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalArgument`] when `position` is negative,
    /// when `position` exceeds the buffer length, or when `size` is
    /// negative — mirroring Java's `IllegalArgumentException`.
    pub fn slice_inner(&self, position: i32, size: i32) -> Result<MemoryRecords, KafkaError> {
        if position < 0 {
            return Err(KafkaError::IllegalArgument(format!(
                "Invalid position: {position} in read from {self}"
            )));
        }
        if position as usize > self.buffer.len() {
            return Err(KafkaError::IllegalArgument(format!(
                "Slice from position {position} exceeds end position of {self}"
            )));
        }
        if size < 0 {
            return Err(KafkaError::IllegalArgument(format!("Invalid size: {size} in read from {self}")));
        }
        let position = position as usize;
        let size = size as usize;
        let available = (self.buffer.len() - position).min(size);
        // Zero-copy slice: shares the same backing allocation.
        let sliced = self.buffer.slice(position..position + available);
        Ok(MemoryRecords::readable_records(sliced))
    }
}

/// Internal iterator that flattens batches into records. Owns the current
/// batch's record set as a `Vec<DefaultRecord>`; on each pull, advances
/// inside the buffered records first, then loads the next batch.
///
/// We materialize each batch into a `Vec<DefaultRecord>` (rather than
/// `Vec<Box<dyn Record>>`) so the records are owned and the batch can be
/// dropped — the `DefaultRecord` struct stores `Option<Bytes>` + headers,
/// none of which borrow from the source batch.
struct BatchFlatIter<'a> {
    batches: RecordBatchIterator<ByteBufferLogInputStream<'a>, DefaultRecordBatch>,
    current: std::vec::IntoIter<DefaultRecord>,
}

impl<'a> Iterator for BatchFlatIter<'a> {
    type Item = Box<dyn Record + 'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(rec) = self.current.next() {
                return Some(Box::new(rec));
            }
            let next_batch = self.batches.next()?;
            match next_batch {
                Ok(batch) => {
                    let owned = drain_batch_into_owned_records(&batch);
                    self.current = owned.into_iter();
                },
                Err(_) => return None,
            }
        }
    }
}

/// Drain a batch's records into an owned `Vec<DefaultRecord>`. Stops at the
/// first corrupt record (matching Java's "throw and stop" iterator).
fn drain_batch_into_owned_records(batch: &DefaultRecordBatch) -> Vec<DefaultRecord> {
    let mut out: Vec<DefaultRecord> = Vec::with_capacity(batch.count_or_null().unwrap_or(0).max(0) as usize);
    for r in batch.iter() {
        match r {
            Ok(boxed) => match downcast_to_default_record(boxed) {
                Some(d) => out.push(d),
                None => break,
            },
            Err(_) => break,
        }
    }
    out
}

/// Helper: pull the concrete `DefaultRecord` out of a `Box<dyn Record>`
/// produced by `DefaultRecordBatch::iter`. The current `iter()` impl always
/// yields `DefaultRecord`, but the trait erasure is lossy. We re-decode by
/// copying out the public accessor results into a fresh `DefaultRecord`
/// — at the cost of one allocation for `headers` and refcount bumps for
/// `key`/`value`.
fn downcast_to_default_record(record: Box<dyn Record + '_>) -> Option<DefaultRecord> {
    use bytes::Bytes;

    // Reconstruct a DefaultRecord from the public accessors. Header
    // contents are cloned (one `Vec<RecordHeader>` allocation per record);
    // key/value cross over via `Bytes::copy_from_slice` — this is a copy.
    //
    // To avoid the key/value copy, we'd need either:
    // (a) Add a `as_any` / downcasting hook to the `Record` trait — not
    //     present in Java and would force an API change.
    // (b) Expose `DefaultRecordBatch::iter_default()` returning concrete
    //     records — diverges from Java's `RecordBatch.iterator()` shape.
    //
    // For the consumer-side flat-records iterator (Phase 5+ broker /
    // metrics path), the copy is acceptable. The producer-side hot path
    // does NOT use `Records::records()` — it uses the typed
    // `MemoryRecordsBuilder` (Phase 3d-4) and `DefaultRecordBatch::iter`
    // directly, both of which preserve zero-copy.
    let key = record.key().map(Bytes::copy_from_slice);
    let value = record.value().map(Bytes::copy_from_slice);
    let headers: Vec<crate::common::header::RecordHeader> = record.headers().to_vec();
    // The v2 record `attributes` byte is unused (always 0 in current
    // protocol); see `default_record::write_to`. Using 0 here matches the
    // round-trip-decoded value.
    Some(DefaultRecord::new(
        record.size_in_bytes(),
        0,
        record.offset(),
        record.timestamp(),
        record.sequence(),
        key,
        value,
        headers,
    ))
}

#[cfg(test)]
mod tests {
    //! Translation of the parts of `MemoryRecordsTest.java` that exercise the
    //! read path. Tests requiring `MemoryRecordsBuilder` (Phase 3d-4) are
    //! noted explicitly with the Java method name and the deferral reason.
    //!
    //! Deferred to Phase 3d-4 (need `MemoryRecordsBuilder`):
    //! * `testIterator` — builds via builder
    //! * `testHasRoomForMethod` — builder API
    //! * `testHasRoomForMethodWithHeaders` — builder API
    //! * `testChecksum` — builds via `withRecords`
    //! * `testFilterToPreservesPartitionLeaderEpoch` — needs `filterTo`
    //! * `testFilterToEmptyBatchRetention` — needs `filterTo`
    //! * `testEmptyBatchRetention` — needs `filterTo`
    //! * `testEmptyBatchDeletion` — needs `filterTo`
    //! * `testBuildEndTxnMarker` — needs `withEndTransactionMarker` (txn
    //!   markers, deferred per PLAN.md scope)
    //! * `testBaseTimestampToDeleteHorizonConversion` — needs `filterTo`
    //! * `testBuildLeaderChangeMessage` — KRaft control records, out of
    //!   scope (PLAN.md skips control records beyond the enum)
    //! * `testFilterToBatchDiscard` — needs `filterTo`
    //! * `testFilterToAlreadyCompactedLog` — needs `filterTo`
    //! * `testFilterToPreservesProducerInfo` — needs `filterTo`
    //! * `testFilterToWithUndersizedBuffer` — needs `filterTo`
    //! * `testFilterTo` — needs `filterTo`
    //! * `testFilterToPreservesLogAppendTime` — needs `filterTo`
    //! * `testWithRecords` — needs `withRecords`
    //! * `testUnsupportedCompress` — needs `withRecords`
    //!
    //! Translated here (read-path coverage):
    //! * `testNextBatchSize` (parts that don't construct via `withRecords`)
    //! * `testSlice`
    //! * `testSliceInvalidPosition`
    //! * `testSliceInvalidSize`
    //! * `testSliceEmptyRecords`
    //! * `testSliceForAlreadySlicedMemoryRecords`
    //!
    //! Plus Rust-only zero-copy and round-trip checks.

    use super::*;
    use crate::common::record::SimpleRecord;
    use crate::common::record::default_record;
    use crate::common::record::default_record_batch::{
        ATTRIBUTES_OFFSET, BASE_OFFSET_OFFSET, BASE_SEQUENCE_OFFSET, BASE_TIMESTAMP_OFFSET, CRC_OFFSET,
        LAST_OFFSET_DELTA_OFFSET, LENGTH_OFFSET, MAGIC_OFFSET, MAX_TIMESTAMP_OFFSET, PARTITION_LEADER_EPOCH_OFFSET,
        PRODUCER_EPOCH_OFFSET, PRODUCER_ID_OFFSET, RECORD_BATCH_OVERHEAD, RECORDS_COUNT_OFFSET,
    };
    use crate::common::record::record_batch::{
        CURRENT_MAGIC_VALUE, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, NO_TIMESTAMP,
    };
    use crate::common::record::records::{LOG_OVERHEAD, MAGIC_OFFSET as RECORDS_MAGIC_OFFSET, SIZE_OFFSET};
    use bytes::Bytes;

    /// Build an uncompressed v2 batch — the same helper used in
    /// `default_record_batch::tests`, `byte_buffer_log_input_stream::tests`,
    /// and `record_batch_iterator::tests`. Phase 3d-4 will replace these
    /// duplicated helpers with `MemoryRecordsBuilder`.
    fn build_batch(base_offset: i64, records: &[SimpleRecord]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2048);
        buf.resize(RECORD_BATCH_OVERHEAD, 0);
        let base_timestamp = records.first().map(|r| r.timestamp()).unwrap_or(NO_TIMESTAMP);
        let max_timestamp = records.iter().map(|r| r.timestamp()).max().unwrap_or(NO_TIMESTAMP);
        let last_offset_delta = if records.is_empty() {
            0
        } else {
            records.len() as i32 - 1
        };
        for (i, r) in records.iter().enumerate() {
            let offset_delta = i as i32;
            let timestamp_delta = r.timestamp() - base_timestamp;
            default_record::write_to(&mut buf, offset_delta, timestamp_delta, r.key(), r.value(), r.headers()).unwrap();
        }
        let size_in_bytes = buf.len() as i32;
        buf[BASE_OFFSET_OFFSET..BASE_OFFSET_OFFSET + 8].copy_from_slice(&base_offset.to_be_bytes());
        buf[LENGTH_OFFSET..LENGTH_OFFSET + 4].copy_from_slice(&(size_in_bytes - LOG_OVERHEAD as i32).to_be_bytes());
        buf[PARTITION_LEADER_EPOCH_OFFSET..PARTITION_LEADER_EPOCH_OFFSET + 4]
            .copy_from_slice(&NO_PARTITION_LEADER_EPOCH.to_be_bytes());
        buf[MAGIC_OFFSET] = CURRENT_MAGIC_VALUE as u8;
        buf[ATTRIBUTES_OFFSET..ATTRIBUTES_OFFSET + 2].copy_from_slice(&0i16.to_be_bytes());
        buf[LAST_OFFSET_DELTA_OFFSET..LAST_OFFSET_DELTA_OFFSET + 4].copy_from_slice(&last_offset_delta.to_be_bytes());
        buf[BASE_TIMESTAMP_OFFSET..BASE_TIMESTAMP_OFFSET + 8].copy_from_slice(&base_timestamp.to_be_bytes());
        buf[MAX_TIMESTAMP_OFFSET..MAX_TIMESTAMP_OFFSET + 8].copy_from_slice(&max_timestamp.to_be_bytes());
        buf[PRODUCER_ID_OFFSET..PRODUCER_ID_OFFSET + 8].copy_from_slice(&NO_PRODUCER_ID.to_be_bytes());
        buf[PRODUCER_EPOCH_OFFSET..PRODUCER_EPOCH_OFFSET + 2].copy_from_slice(&NO_PRODUCER_EPOCH.to_be_bytes());
        buf[BASE_SEQUENCE_OFFSET..BASE_SEQUENCE_OFFSET + 4].copy_from_slice(&NO_SEQUENCE.to_be_bytes());
        buf[RECORDS_COUNT_OFFSET..RECORDS_COUNT_OFFSET + 4].copy_from_slice(&(records.len() as i32).to_be_bytes());
        let crc = crc32c::crc32c(&buf[ATTRIBUTES_OFFSET..]);
        buf[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
        buf
    }

    fn three_batches() -> (Vec<u8>, Vec<usize>) {
        // Three contiguous v2 batches at offsets 0, 6, 14, with 6, 8, 4
        // records respectively. Returns (concatenated buffer, per-batch
        // sizes).
        let mut combined = Vec::new();
        let mut sizes = Vec::new();

        let r1: Vec<SimpleRecord> = (0..6)
            .map(|i| {
                SimpleRecord::new(
                    100 + i,
                    Some(Bytes::from(format!("k{i}"))),
                    Some(Bytes::from(format!("v{i}"))),
                    &[],
                )
            })
            .collect();
        let b1 = build_batch(0, &r1);
        sizes.push(b1.len());
        combined.extend_from_slice(&b1);

        let r2: Vec<SimpleRecord> = (0..8)
            .map(|i| {
                SimpleRecord::new(
                    200 + i,
                    Some(Bytes::from(format!("kk{i}"))),
                    Some(Bytes::from(format!("vv{i}"))),
                    &[],
                )
            })
            .collect();
        let b2 = build_batch(6, &r2);
        sizes.push(b2.len());
        combined.extend_from_slice(&b2);

        let r3: Vec<SimpleRecord> = (0..4)
            .map(|i| {
                SimpleRecord::new(
                    300 + i,
                    Some(Bytes::from(format!("kkk{i}"))),
                    Some(Bytes::from(format!("vvv{i}"))),
                    &[],
                )
            })
            .collect();
        let b3 = build_batch(14, &r3);
        sizes.push(b3.len());
        combined.extend_from_slice(&b3);

        (combined, sizes)
    }

    /// Read-path coverage: empty `MemoryRecords` is well-formed.
    #[test]
    fn empty_returns_zero_size_singleton() {
        let e1 = MemoryRecords::empty();
        let e2 = MemoryRecords::empty();
        assert_eq!(e1.size_in_bytes(), 0);
        assert!(std::ptr::eq(e1, e2));
        assert_eq!(e1.batches().count(), 0);
        assert_eq!(e1.records().count(), 0);
    }

    /// Helper: collect batches, asserting all parsed successfully.
    fn ok_batches(records: &MemoryRecords) -> Vec<Box<dyn RecordBatch + '_>> {
        records.batches().map(|r| r.expect("batch should parse")).collect()
    }

    /// Translation of `testNextBatchSize` (read-path subset). Exercises the
    /// `firstBatchSize` API directly from a hand-built batch.
    #[test]
    fn first_batch_size_returns_full_batch_size() {
        let (combined, sizes) = three_batches();
        let records = MemoryRecords::readable_records_from_vec(combined);
        // First call returns the size of just the first batch.
        assert_eq!(records.first_batch_size().unwrap(), Some(sizes[0] as i32));
    }

    #[test]
    fn first_batch_size_returns_none_when_buffer_truncated_before_size() {
        // Buffer shorter than LOG_OVERHEAD: not enough to read SIZE field.
        let truncated = vec![0u8; 1];
        let records = MemoryRecords::readable_records_from_vec(truncated);
        assert_eq!(records.first_batch_size().unwrap(), None);
    }

    #[test]
    fn first_batch_size_returns_none_when_buffer_truncated_before_magic() {
        // Buffer >= LOG_OVERHEAD but < HEADER_SIZE_UP_TO_MAGIC: returns
        // None per Java spec.
        let mut truncated = vec![0u8; LOG_OVERHEAD];
        // SIZE field must be valid (>= LEGACY_RECORD_OVERHEAD_V0 = 14) so
        // we don't trigger the corrupt path before the magic check.
        truncated[SIZE_OFFSET..SIZE_OFFSET + 4].copy_from_slice(&100i32.to_be_bytes());
        let records = MemoryRecords::readable_records_from_vec(truncated);
        assert_eq!(records.first_batch_size().unwrap(), None);
    }

    /// Translation of Java `MemoryRecordsTest.testNextBatchSize` lines
    /// 1056-1057: with `buffer.limit(Records.HEADER_SIZE_UP_TO_MAGIC)`
    /// (i.e. exactly 17 bytes), `firstBatchSize()` returns the full
    /// declared batch size — NOT null.
    ///
    /// Positive boundary test: at exactly `len == HEADER_SIZE_UP_TO_MAGIC`
    /// the early-out (`<` not `<=`) does NOT fire, so the underlying
    /// `next_batch_size` validates the SIZE and MAGIC fields and returns
    /// `Some(LOG_OVERHEAD + declared_size)`.
    #[test]
    fn first_batch_size_at_header_size_up_to_magic_boundary() {
        use crate::common::record::records::HEADER_SIZE_UP_TO_MAGIC;
        // Exactly 17 bytes: [base_offset(8)] [size(4)] [4 unspecified] [magic(1)].
        let mut buf = vec![0u8; HEADER_SIZE_UP_TO_MAGIC];
        // Declared batch length = 12345 bytes (the SIZE field; >=
        // LEGACY_RECORD_OVERHEAD_V0=14 so it doesn't trigger corruption).
        let declared_len = 12345i32;
        buf[SIZE_OFFSET..SIZE_OFFSET + 4].copy_from_slice(&declared_len.to_be_bytes());
        // Magic must be in the valid range [0, CURRENT_MAGIC_VALUE].
        buf[RECORDS_MAGIC_OFFSET] = CURRENT_MAGIC_VALUE as u8;

        let records = MemoryRecords::readable_records_from_vec(buf);
        // `first_batch_size` returns `LOG_OVERHEAD + declared_len`. The
        // payload bytes are NOT required to be present — Java's
        // `firstBatchSize()` validates only the header.
        assert_eq!(
            records.first_batch_size().unwrap(),
            Some(LOG_OVERHEAD as i32 + declared_len),
            "len == HEADER_SIZE_UP_TO_MAGIC must return the declared full size, not None"
        );
    }

    #[test]
    fn first_batch_size_raises_on_invalid_magic() {
        let (mut combined, _) = three_batches();
        // Corrupt the first batch's magic byte.
        combined[RECORDS_MAGIC_OFFSET] = 10;
        let records = MemoryRecords::readable_records_from_vec(combined);
        assert!(matches!(records.first_batch_size(), Err(KafkaError::CorruptRecord(_))));
    }

    #[test]
    fn first_batch_size_raises_on_corrupt_size() {
        let (mut combined, _) = three_batches();
        // Corrupt the SIZE field to a value below the legacy v0 minimum
        // (14 bytes) — Java's `assertThrows(CorruptRecordException...)`.
        combined[SIZE_OFFSET..SIZE_OFFSET + 4].copy_from_slice(&5i32.to_be_bytes());
        let records = MemoryRecords::readable_records_from_vec(combined);
        assert!(matches!(records.first_batch_size(), Err(KafkaError::CorruptRecord(_))));
    }

    /// Translation of `testSlice`. Slices a multi-batch `MemoryRecords` from
    /// various positions and asserts both byte-length and batch-equivalence.
    #[test]
    fn slice_yields_zero_copy_views_at_batch_boundaries() {
        let (combined, sizes) = three_batches();
        let total = combined.len() as i32;
        let records = MemoryRecords::readable_records_from_vec(combined);

        // Slice from start: identical to original.
        let s0 = records.slice_inner(0, total).unwrap();
        assert_eq!(s0.size_in_bytes(), total);
        assert_eq!(s0.valid_bytes(), records.valid_bytes());

        // Slice from after first batch.
        let after_first = sizes[0] as i32;
        let s1 = records.slice_inner(after_first, total - after_first).unwrap();
        assert_eq!(s1.size_in_bytes(), total - after_first);
        assert_eq!(ok_batches(&s1).len(), 2);

        // Slice from after first, size > remaining: clamps to remaining.
        let s2 = records.slice_inner(after_first, total).unwrap();
        assert_eq!(s2.size_in_bytes(), total - after_first);

        // Slice from after first, size = i32::MAX: clamps to remaining.
        let s3 = records.slice_inner(after_first, i32::MAX).unwrap();
        assert_eq!(s3.size_in_bytes(), total - after_first);

        // Read a single batch starting at the second.
        let second_size = sizes[1] as i32;
        let s4 = records.slice_inner(after_first, second_size).unwrap();
        assert_eq!(s4.size_in_bytes(), second_size);
        assert_eq!(ok_batches(&s4).len(), 1);

        // Slice of a slice: read from the third batch onward.
        let after_second = (sizes[0] + sizes[1]) as i32;
        let s5 = s1.slice_inner(second_size, total - after_second).expect("slice of slice");
        assert_eq!(s5.size_in_bytes(), total - after_second);
        assert_eq!(ok_batches(&s5).len(), 1);
    }

    /// Translation of `testSliceInvalidPosition`.
    #[test]
    fn slice_invalid_position_returns_err() {
        let (combined, _) = three_batches();
        let records = MemoryRecords::readable_records_from_vec(combined);
        assert!(matches!(
            records.slice_inner(-1, records.size_in_bytes()),
            Err(KafkaError::IllegalArgument(_))
        ));
        assert!(matches!(
            records.slice_inner(records.size_in_bytes() + 1, records.size_in_bytes()),
            Err(KafkaError::IllegalArgument(_))
        ));
    }

    /// Boundary: position == buffer.len() is allowed (Java accepts it,
    /// only rejects strictly greater). Yields an empty slice.
    #[test]
    fn slice_at_end_position_returns_empty() {
        let (combined, _) = three_batches();
        let records = MemoryRecords::readable_records_from_vec(combined);
        let total = records.size_in_bytes();
        let sliced = records.slice_inner(total, 100).unwrap();
        assert_eq!(sliced.size_in_bytes(), 0);
    }

    /// Translation of `testSliceInvalidSize`.
    #[test]
    fn slice_invalid_size_returns_err() {
        let (combined, _) = three_batches();
        let records = MemoryRecords::readable_records_from_vec(combined);
        assert!(matches!(records.slice_inner(0, -1), Err(KafkaError::IllegalArgument(_))));
    }

    /// Translation of `testSliceEmptyRecords`.
    #[test]
    fn slice_empty_records_returns_empty() {
        let empty = MemoryRecords::empty();
        let sliced = empty.slice_inner(0, 0).unwrap();
        assert_eq!(sliced.size_in_bytes(), 0);
        assert_eq!(ok_batches(&sliced).len(), 0);
    }

    /// Translation of `testSliceForAlreadySlicedMemoryRecords`.
    #[test]
    fn slice_of_already_sliced_records() {
        let (combined, sizes) = three_batches();
        let records = MemoryRecords::readable_records_from_vec(combined);
        // First, slice from the third batch onward.
        let position = (sizes[0] + sizes[1]) as i32;
        let sliced = records.slice_inner(position, records.size_in_bytes() - position).unwrap();
        assert_eq!(sliced.size_in_bytes(), records.size_in_bytes() - position);
        assert_eq!(ok_batches(&sliced).len(), 1);

        // Slice the slice further: from beyond its end -> empty.
        let further = sliced.slice_inner(sliced.size_in_bytes(), 0).unwrap();
        assert_eq!(further.size_in_bytes(), 0);
        assert_eq!(ok_batches(&further).len(), 0);
    }

    /// Zero-copy contract: `slice` must alias the original buffer.
    #[test]
    fn slice_is_zero_copy() {
        let (combined, sizes) = three_batches();
        let records = MemoryRecords::readable_records_from_vec(combined);
        let after_first = sizes[0] as i32;
        let sliced = records.slice_inner(after_first, records.size_in_bytes() - after_first).unwrap();

        let p_orig = records.buffer().as_ptr();
        let p_slice = sliced.buffer().as_ptr();
        // The slice's pointer must lie *inside* the original buffer's range.
        // (Bytes::slice shares the same backing allocation; the slice's
        // start address is `p_orig + after_first`.)
        let off = unsafe { p_slice.offset_from(p_orig) };
        assert_eq!(off, after_first as isize, "slice must alias the same backing storage");
    }

    /// Records iteration spans multiple batches. Mirrors the
    /// flat-records portion of `testIterator`.
    #[test]
    fn records_iterator_flattens_across_batches() {
        let (combined, _) = three_batches();
        let records = MemoryRecords::readable_records_from_vec(combined);
        // Total: 6 + 8 + 4 = 18 records.
        assert_eq!(records.records().count(), 18);
    }

    /// Batches iterator yields all three batches in order.
    #[test]
    fn batches_iterator_yields_all_batches() {
        let (combined, _) = three_batches();
        let records = MemoryRecords::readable_records_from_vec(combined);
        let batches: Vec<_> = records.batches().map(|r| r.expect("clean batch parses")).collect();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].base_offset(), 0);
        assert_eq!(batches[1].base_offset(), 6);
        assert_eq!(batches[2].base_offset(), 14);
    }

    /// Translation of Java's `validBytes()` semantic.
    #[test]
    fn valid_bytes_sums_all_batches_when_clean() {
        let (combined, sizes) = three_batches();
        let records = MemoryRecords::readable_records_from_vec(combined);
        assert_eq!(records.valid_bytes(), sizes.iter().map(|&s| s as i32).sum::<i32>());
    }

    /// Trailing partial batch: `valid_bytes` returns only the complete
    /// portion, mirroring Java's behavior (the partial trailing batch is
    /// ignored).
    #[test]
    fn valid_bytes_excludes_trailing_partial_batch() {
        let (mut combined, sizes) = three_batches();
        // Drop the last 5 bytes of the last batch.
        combined.truncate(combined.len() - 5);
        let records = MemoryRecords::readable_records_from_vec(combined);
        // Only the first two batches are complete.
        assert_eq!(records.valid_bytes(), (sizes[0] + sizes[1]) as i32);
    }

    /// Display formatting matches Java's toString-like form.
    #[test]
    fn display_includes_size() {
        let r = MemoryRecords::empty();
        let s = format!("{r}");
        assert!(s.starts_with("MemoryRecords(size=0"));
    }

    /// Equality is based on the buffer contents.
    #[test]
    fn equality_compares_buffer_contents() {
        let (a, _) = three_batches();
        let r1 = MemoryRecords::readable_records_from_vec(a.clone());
        let r2 = MemoryRecords::readable_records_from_vec(a);
        assert_eq!(r1, r2);

        let r3 = MemoryRecords::readable_records_from_vec(vec![0u8; 4]);
        assert_ne!(r1, r3);
    }

    /// Cloning shares the same backing storage (refcount bump).
    #[test]
    fn clone_shares_storage() {
        let (a, _) = three_batches();
        let r1 = MemoryRecords::readable_records_from_vec(a);
        let r2 = r1.clone();
        let p1 = r1.buffer().as_ptr();
        let p2 = r2.buffer().as_ptr();
        assert_eq!(p1, p2);
    }

    /// `to_send()` produces a `DefaultRecordsSend` sized to the record
    /// set. Mirrors Java's `AbstractRecords#toSend()`.
    #[test]
    fn to_send_returns_default_records_send_sized_to_self() {
        let (a, _) = three_batches();
        let total = a.len() as i32;
        let r = MemoryRecords::readable_records_from_vec(a);
        let send = r.to_send();
        assert_eq!(send.size(), total as i64);
        assert_eq!(send.remaining(), total);
        assert!(!send.completed());
    }

    /// Issue 18 fix: `Records::batches()` yields `Err` for a corrupt batch
    /// at the same iteration step where Java raises
    /// `CorruptRecordException`, then stops. Mirrors Java's
    /// `MemoryRecordsBatchIterator` "throw and stop" semantic.
    #[test]
    fn batches_yields_err_on_corrupt_second_batch_then_stops() {
        let (mut combined, sizes) = three_batches();
        // Corrupt the second batch's magic byte.
        let second_batch_offset = sizes[0];
        combined[second_batch_offset + RECORDS_MAGIC_OFFSET] = 10;
        let records = MemoryRecords::readable_records_from_vec(combined);

        let mut it = records.batches();
        // First batch parses cleanly.
        let first = it.next().expect("first batch present");
        let first = match first {
            Ok(b) => b,
            Err(e) => panic!("first batch should parse cleanly: {e:?}"),
        };
        assert_eq!(first.base_offset(), 0);

        // Second batch surfaces the corruption error.
        let second = it.next().expect("second batch present (as Err)");
        match second {
            Err(KafkaError::CorruptRecord(_)) => {},
            Ok(_) => panic!("expected CorruptRecord error, got Ok"),
            Err(e) => panic!("expected CorruptRecord error, got {e:?}"),
        }

        // Iterator stops — no more items, even though a third batch exists
        // in the buffer (the underlying RecordBatchIterator is poisoned).
        assert!(it.next().is_none(), "iterator must stop after first error");
    }
}
