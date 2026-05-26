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

//! Translation of `org.apache.kafka.common.record.ByteBufferLogInputStream`.

#![allow(dead_code)] // Used by tests + Phase 3d-3 `MemoryRecords::batches()`.

use crate::common::errors::KafkaError;
use crate::common::record::default_record_batch::DefaultRecordBatch;
use crate::common::record::log_input_stream::LogInputStream;
use crate::common::record::record_batch::CURRENT_MAGIC_VALUE;
use crate::common::record::records::{HEADER_SIZE_UP_TO_MAGIC, LOG_OVERHEAD, MAGIC_OFFSET, SIZE_OFFSET};
use crate::common::utils::byte_utils;

/// Minimum overhead of a v0 record (legacy). Java keeps this on
/// `LegacyRecord.RECORD_OVERHEAD_V0`. We hard-code it here because legacy
/// formats are explicitly out of scope (PLAN.md), but we still need the
/// minimum-batch-size sanity check that uses it.
const LEGACY_RECORD_OVERHEAD_V0: i32 = 14;

/// A byte buffer backed log input stream. This class avoids the need to copy
/// records by returning slices from the underlying byte buffer.
///
/// Mirrors Java's `ByteBufferLogInputStream` (package-private). For magic >= 2
/// it returns [`DefaultRecordBatch`] instances; legacy v0/v1 batches are
/// out of scope for the producer client and we do not translate
/// `AbstractLegacyRecordBatch`. Buffers containing legacy magic bytes are
/// rejected with [`KafkaError::CorruptRecord`].
pub(crate) struct ByteBufferLogInputStream<'a> {
    /// Source buffer; we walk the cursor `position` along it as batches are
    /// consumed.
    buffer: &'a [u8],
    position: usize,
    max_message_size: i32,
}

impl<'a> ByteBufferLogInputStream<'a> {
    /// Construct a new stream over `buffer`. `max_message_size` bounds the
    /// per-batch record-size header field; batches declaring a larger size
    /// are rejected with [`KafkaError::CorruptRecord`].
    pub(crate) fn new(buffer: &'a [u8], max_message_size: i32) -> Self {
        ByteBufferLogInputStream { buffer, position: 0, max_message_size }
    }

    /// Bytes remaining in the underlying buffer at the current cursor.
    fn remaining(&self) -> usize {
        self.buffer.len().saturating_sub(self.position)
    }

    /// Validates the header of the next batch and returns its full size
    /// (including [`LOG_OVERHEAD`]). Mirrors Java's package-private
    /// `nextBatchSize()`:
    ///
    /// * `Ok(Some(n))` — header is valid, batch size is `n` bytes;
    /// * `Ok(None)` — buffer doesn't yet hold enough bytes for the header;
    /// * `Err(KafkaError::CorruptRecord)` — record size or magic is invalid.
    pub(crate) fn next_batch_size(&self) -> Result<Option<i32>, KafkaError> {
        let remaining = self.remaining();
        if remaining < LOG_OVERHEAD {
            return Ok(None);
        }
        let record_size = byte_utils::read_int_be_at(self.buffer, self.position + SIZE_OFFSET);
        // V0 has the smallest overhead, stricter checking is done later (Java
        // matches).
        if record_size < LEGACY_RECORD_OVERHEAD_V0 {
            return Err(KafkaError::CorruptRecord(format!(
                "Record size {record_size} is less than the minimum record overhead ({LEGACY_RECORD_OVERHEAD_V0})"
            )));
        }
        if record_size > self.max_message_size {
            return Err(KafkaError::CorruptRecord(format!(
                "Record size {record_size} exceeds the largest allowable message size ({}).",
                self.max_message_size
            )));
        }

        if remaining < HEADER_SIZE_UP_TO_MAGIC {
            return Ok(None);
        }

        let magic = self.buffer[self.position + MAGIC_OFFSET] as i8;
        if !(0..=CURRENT_MAGIC_VALUE).contains(&magic) {
            return Err(KafkaError::CorruptRecord(format!("Invalid magic found in record: {magic}")));
        }

        Ok(Some(record_size + LOG_OVERHEAD as i32))
    }
}

impl<'a> LogInputStream<DefaultRecordBatch> for ByteBufferLogInputStream<'a> {
    fn next_batch(&mut self) -> Result<Option<DefaultRecordBatch>, KafkaError> {
        let remaining = self.remaining();

        let batch_size = match self.next_batch_size()? {
            Some(n) => n,
            None => return Ok(None),
        };
        if (remaining as i32) < batch_size {
            return Ok(None);
        }

        let magic = self.buffer[self.position + MAGIC_OFFSET] as i8;

        let end = self.position + batch_size as usize;
        let slice = self.buffer[self.position..end].to_vec();
        self.position = end;

        if magic > 1 {
            // V2+ — DefaultRecordBatch.
            Ok(Some(DefaultRecordBatch::new(slice)))
        } else {
            // V0/V1 legacy formats are out of scope for the producer client.
            // Java would return an `AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch`
            // here; we surface a `CorruptRecord` so the iterator stops.
            Err(KafkaError::CorruptRecord(format!(
                "Legacy magic {magic} not supported by the Rust producer client (PLAN.md scope)"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `ByteBufferLogInputStreamTest.java`.
    //!
    //! Java's tests use `MemoryRecordsBuilder` (Phase 3d-3/3d-4) to construct
    //! batches. We use our `default_record_batch::tests` helpers (re-exposed
    //! through a small `super::` shim) to build the same batches manually.
    //! When `MemoryRecordsBuilder` lands these tests can re-route through it
    //! without changing assertions.

    use super::*;
    use crate::common::record::RecordBatch;
    use crate::common::record::SimpleRecord;
    use crate::common::record::default_record;
    use crate::common::record::default_record_batch::{
        ATTRIBUTES_OFFSET, BASE_OFFSET_OFFSET, BASE_SEQUENCE_OFFSET, BASE_TIMESTAMP_OFFSET, CRC_OFFSET,
        LAST_OFFSET_DELTA_OFFSET, LENGTH_OFFSET, MAGIC_OFFSET as DRB_MAGIC_OFFSET, MAX_TIMESTAMP_OFFSET,
        PARTITION_LEADER_EPOCH_OFFSET, PRODUCER_EPOCH_OFFSET, PRODUCER_ID_OFFSET, RECORD_BATCH_OVERHEAD,
        RECORDS_COUNT_OFFSET,
    };
    use crate::common::record::record_batch::{
        CURRENT_MAGIC_VALUE, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, NO_TIMESTAMP,
    };
    use crate::common::record::records::LOG_OVERHEAD;
    use bytes::Bytes;

    /// Mini in-line copy of the helper from `default_record_batch::tests`.
    /// Builds an uncompressed v2 batch.
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
        buf[DRB_MAGIC_OFFSET] = CURRENT_MAGIC_VALUE as u8;
        // attributes = 0 (CreateTime, NoCompression)
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

    /// Translation of `iteratorIgnoresIncompleteEntries`. Constructs two
    /// batches concatenated, drops the last 5 bytes, and asserts only the
    /// first batch is yielded.
    #[test]
    fn iterator_ignores_incomplete_entries() {
        let r1 = SimpleRecord::new(15, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"1")), &[]);
        let r2 = SimpleRecord::new(20, Some(Bytes::from_static(b"b")), Some(Bytes::from_static(b"2")), &[]);
        let r3 = SimpleRecord::new(30, Some(Bytes::from_static(b"c")), Some(Bytes::from_static(b"3")), &[]);
        let r4 = SimpleRecord::new(40, Some(Bytes::from_static(b"d")), Some(Bytes::from_static(b"4")), &[]);

        let mut combined = Vec::new();
        combined.extend_from_slice(&build_batch(0, &[r1, r2]));
        combined.extend_from_slice(&build_batch(2, &[r3, r4]));

        // Drop the last 5 bytes — Java does buffer.limit(buffer.limit() - 5).
        combined.truncate(combined.len() - 5);

        let mut log_input_stream = ByteBufferLogInputStream::new(&combined, i32::MAX);
        let first = log_input_stream.next_batch().unwrap().expect("first batch");
        assert_eq!(first.last_offset(), 1);
        // Second batch is incomplete -> next_batch returns None.
        assert!(log_input_stream.next_batch().unwrap().is_none());
    }

    /// Translation of `iteratorRaisesOnTooSmallRecords`.
    #[test]
    fn iterator_raises_on_too_small_records() {
        let r1 = SimpleRecord::new(15, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"1")), &[]);
        let r2 = SimpleRecord::new(20, Some(Bytes::from_static(b"b")), Some(Bytes::from_static(b"2")), &[]);
        let r3 = SimpleRecord::new(30, Some(Bytes::from_static(b"c")), Some(Bytes::from_static(b"3")), &[]);
        let r4 = SimpleRecord::new(40, Some(Bytes::from_static(b"d")), Some(Bytes::from_static(b"4")), &[]);

        let batch1 = build_batch(0, &[r1, r2]);
        let position = batch1.len();
        let mut combined = batch1;
        combined.extend_from_slice(&build_batch(2, &[r3, r4]));

        // Corrupt: rewrite Length to 9 in the second batch (Java does
        // putInt(position + LENGTH_OFFSET, 9)).
        combined[position + LENGTH_OFFSET..position + LENGTH_OFFSET + 4].copy_from_slice(&9i32.to_be_bytes());

        let mut log_input_stream = ByteBufferLogInputStream::new(&combined, i32::MAX);
        // First batch decodes successfully.
        assert!(log_input_stream.next_batch().unwrap().is_some());
        // Second batch fails the `record_size < RECORD_OVERHEAD_V0` check.
        let err = log_input_stream.next_batch().unwrap_err();
        assert!(matches!(err, KafkaError::CorruptRecord(_)), "got {err:?}");
    }

    /// Translation of `iteratorRaisesOnInvalidMagic`.
    #[test]
    fn iterator_raises_on_invalid_magic() {
        let r1 = SimpleRecord::new(15, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"1")), &[]);
        let r2 = SimpleRecord::new(20, Some(Bytes::from_static(b"b")), Some(Bytes::from_static(b"2")), &[]);
        let r3 = SimpleRecord::new(30, Some(Bytes::from_static(b"c")), Some(Bytes::from_static(b"3")), &[]);
        let r4 = SimpleRecord::new(40, Some(Bytes::from_static(b"d")), Some(Bytes::from_static(b"4")), &[]);

        let batch1 = build_batch(0, &[r1, r2]);
        let position = batch1.len();
        let mut combined = batch1;
        combined.extend_from_slice(&build_batch(2, &[r3, r4]));

        // Corrupt: write magic = 37 in the second batch.
        combined[position + DRB_MAGIC_OFFSET] = 37;

        let mut log_input_stream = ByteBufferLogInputStream::new(&combined, i32::MAX);
        assert!(log_input_stream.next_batch().unwrap().is_some());
        let err = log_input_stream.next_batch().unwrap_err();
        assert!(matches!(err, KafkaError::CorruptRecord(_)));
    }

    /// Translation of `iteratorRaisesOnTooLargeRecords`.
    #[test]
    fn iterator_raises_on_too_large_records() {
        let r1 = SimpleRecord::new(15, Some(Bytes::from_static(b"a")), Some(Bytes::from_static(b"1")), &[]);
        let r3 = SimpleRecord::new(30, Some(Bytes::from_static(b"c")), Some(Bytes::from_static(b"3")), &[]);
        let r4 = SimpleRecord::new(40, Some(Bytes::from_static(b"d")), Some(Bytes::from_static(b"4")), &[]);

        let mut combined = Vec::new();
        combined.extend_from_slice(&build_batch(0, &[r1]));
        combined.extend_from_slice(&build_batch(2, &[r3, r4]));

        // Java: ByteBufferLogInputStream(buffer, 60) — the second batch is
        // larger than 60 bytes (header is 61 alone), so size > max triggers.
        let mut log_input_stream = ByteBufferLogInputStream::new(&combined, 60);
        assert!(log_input_stream.next_batch().unwrap().is_some());
        let err = log_input_stream.next_batch().unwrap_err();
        assert!(matches!(err, KafkaError::CorruptRecord(_)), "got {err:?}");
    }

    /// Sanity: empty buffer yields None.
    #[test]
    fn empty_buffer_yields_none() {
        let mut log_input_stream = ByteBufferLogInputStream::new(&[], i32::MAX);
        assert!(log_input_stream.next_batch().unwrap().is_none());
    }
}
