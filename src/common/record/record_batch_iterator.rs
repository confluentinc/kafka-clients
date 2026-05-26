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

//! Translation of `org.apache.kafka.common.record.RecordBatchIterator`.

#![allow(dead_code)] // Used by tests + Phase 3d-3 `MemoryRecords::batches()`.

use crate::common::errors::KafkaError;
use crate::common::record::RecordBatch;
use crate::common::record::log_input_stream::LogInputStream;

/// Iterator wrapping a [`LogInputStream`]. Yields successive batches until the
/// underlying stream returns `None`.
///
/// Java's iterator throws `CorruptRecordException` mid-iteration when the
/// underlying stream surfaces an `EOFException`/`IOException`. The Rust
/// translation surfaces those as `Err(KafkaError)` items in the iterator
/// stream, leaving the caller to decide whether to halt or continue. After
/// an `Err` we also stop yielding, matching Java's "throw and stop"
/// semantics.
pub(crate) struct RecordBatchIterator<S, T> {
    inner: S,
    poisoned: bool,
    _phantom: std::marker::PhantomData<T>,
}

impl<S, T> RecordBatchIterator<S, T>
where
    S: LogInputStream<T>,
    T: RecordBatch,
{
    /// Construct a new iterator over `log_input_stream`.
    pub(crate) fn new(log_input_stream: S) -> Self {
        RecordBatchIterator { inner: log_input_stream, poisoned: false, _phantom: std::marker::PhantomData }
    }
}

impl<S, T> Iterator for RecordBatchIterator<S, T>
where
    S: LogInputStream<T>,
    T: RecordBatch,
{
    type Item = Result<T, KafkaError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return None;
        }
        match self.inner.next_batch() {
            Ok(Some(b)) => Some(Ok(b)),
            Ok(None) => None,
            Err(e) => {
                self.poisoned = true;
                Some(Err(e))
            },
        }
    }
}

#[cfg(test)]
mod tests {
    //! Java has no dedicated `RecordBatchIteratorTest`. The class is
    //! exercised indirectly through `MemoryRecordsTest` (Phase 3d-3). We
    //! validate the wrapper semantics here against
    //! [`crate::common::record::byte_buffer_log_input_stream::ByteBufferLogInputStream`].

    use super::*;
    use crate::common::record::SimpleRecord;
    use crate::common::record::byte_buffer_log_input_stream::ByteBufferLogInputStream;
    use crate::common::record::default_record;
    use crate::common::record::default_record_batch::{
        ATTRIBUTES_OFFSET, BASE_OFFSET_OFFSET, BASE_SEQUENCE_OFFSET, BASE_TIMESTAMP_OFFSET, CRC_OFFSET,
        LAST_OFFSET_DELTA_OFFSET, LENGTH_OFFSET, MAGIC_OFFSET, MAX_TIMESTAMP_OFFSET, PARTITION_LEADER_EPOCH_OFFSET,
        PRODUCER_EPOCH_OFFSET, PRODUCER_ID_OFFSET, RECORD_BATCH_OVERHEAD, RECORDS_COUNT_OFFSET,
    };
    use crate::common::record::record_batch::{
        CURRENT_MAGIC_VALUE, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, NO_TIMESTAMP,
    };
    use crate::common::record::records::LOG_OVERHEAD;
    use bytes::Bytes;

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

    #[test]
    fn iterates_two_concatenated_batches() {
        let r1 = SimpleRecord::new(10, Some(Bytes::from_static(b"k1")), Some(Bytes::from_static(b"v1")), &[]);
        let r2 = SimpleRecord::new(20, Some(Bytes::from_static(b"k2")), Some(Bytes::from_static(b"v2")), &[]);
        let r3 = SimpleRecord::new(30, Some(Bytes::from_static(b"k3")), Some(Bytes::from_static(b"v3")), &[]);

        let mut combined = Vec::new();
        combined.extend_from_slice(&build_batch(0, &[r1, r2]));
        combined.extend_from_slice(&build_batch(2, &[r3]));

        let lis = ByteBufferLogInputStream::new(&combined, i32::MAX);
        let it = RecordBatchIterator::new(lis);
        let batches: Vec<_> = it.collect();
        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|b| b.is_ok()));
    }

    #[test]
    fn empty_input_yields_no_batches() {
        let lis = ByteBufferLogInputStream::new(&[], i32::MAX);
        let it = RecordBatchIterator::new(lis);
        let batches: Vec<_> = it.collect();
        assert!(batches.is_empty());
    }

    #[test]
    fn corrupt_input_yields_err_then_stops() {
        let r = SimpleRecord::new(10, None, Some(Bytes::from_static(b"v")), &[]);
        let mut combined = build_batch(0, &[r]);
        // Append a bogus second-batch header (record size = 0 → corrupt).
        combined.extend_from_slice(&[0u8; LOG_OVERHEAD]);

        let lis = ByteBufferLogInputStream::new(&combined, i32::MAX);
        let mut it = RecordBatchIterator::new(lis);
        assert!(it.next().unwrap().is_ok());
        assert!(matches!(it.next().unwrap(), Err(KafkaError::CorruptRecord(_))));
        // Poisoned: subsequent calls return None.
        assert!(it.next().is_none());
    }
}
