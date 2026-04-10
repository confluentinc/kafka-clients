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

//! ProducerBatch accumulates records destined for a single TopicPartition.
//!
//! Records are serialized into proper Kafka RecordBatch wire format using
//! [`MemoryRecordsBuilder`](crate::common::record::MemoryRecordsBuilder).
//!
//! Corresponds to `org.apache.kafka.clients.producer.internals.ProducerBatch`.

use crate::common::TopicPartition;
use crate::common::record::compression_type::CompressionType;
use crate::common::record::default_record::DefaultRecord;
use crate::common::record::memory_records_builder::MemoryRecordsBuilder;
use crate::common::record::timestamp_type::TimestampType;
use crate::common::record::{
    CURRENT_MAGIC_VALUE, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, RecordHeader,
    record_batch_header_size_in_bytes,
};
use crate::errors::{ErrorCode, KafkaError};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::sync::oneshot;

use super::record::{Header, RecordMetadata};

/// A future that resolves when a record has been acknowledged by the broker.
///
/// Returned by `KafkaProducer::send()`. The caller can `.await` this to get
/// the `RecordMetadata` or an error.
#[derive(Debug)]
pub struct SendFuture {
    rx: oneshot::Receiver<Result<RecordMetadata, KafkaError>>,
}

impl Future for SendFuture {
    type Output = Result<RecordMetadata, KafkaError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(KafkaError::new(
                ErrorCode::Unexpected,
                "producer was closed before record could be acknowledged",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Tracks a single record's completion channel within a batch.
struct BatchedRecord {
    tx: oneshot::Sender<Result<RecordMetadata, KafkaError>>,
    offset_delta: i32,
    key_size: i32,
    value_size: i32,
}

/// A batch of records destined for a single TopicPartition.
///
/// Records are serialized in proper Kafka RecordBatch wire format via
/// [`MemoryRecordsBuilder`]. Each record's key, value, and headers are
/// written into the batch exactly once during `try_append()`.
pub struct ProducerBatch {
    tp: TopicPartition,
    records_builder: MemoryRecordsBuilder,
    pending: Vec<BatchedRecord>,
    record_count: i32,
    created_at: Instant,
    closed: bool,
    /// Cached size after finalization (set by `finalized_bytes()`).
    finalized_size: Option<usize>,
}

impl ProducerBatch {
    /// Create a new batch for the given TopicPartition.
    pub fn new(tp: TopicPartition, max_bytes: usize) -> Self {
        // Create a MemoryRecordsBuilder that writes records in proper RecordBatch format.
        // base_offset=0 since the broker assigns the actual offset.
        let records_builder = MemoryRecordsBuilder::new(
            max_bytes.min(1024),
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0, // base_offset
            0, // log_append_time (unused for CreateTime)
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false, // is_transactional
            false, // is_control_batch
            crate::common::record::NO_PARTITION_LEADER_EPOCH,
            max_bytes,
        )
        .expect("MemoryRecordsBuilder::new must succeed with valid parameters");

        ProducerBatch {
            tp,
            records_builder,
            pending: Vec::new(),
            record_count: 0,
            created_at: Instant::now(),
            closed: false,
            finalized_size: None,
        }
    }

    /// Try to append a record to this batch.
    ///
    /// Returns `Some(SendFuture)` on success, `None` if the batch is full or closed.
    /// This is the single copy point: key/value/headers are written into the
    /// RecordBatch buffer in proper Kafka wire format.
    pub fn try_append(
        &mut self,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[Header<'_>],
        timestamp: i64,
    ) -> Option<SendFuture> {
        if self.closed {
            return None;
        }

        // Convert producer Header<'a> (borrowed) to RecordHeader (owned) for
        // the MemoryRecordsBuilder. This is the single copy point for headers.
        let record_headers: Vec<RecordHeader> = headers
            .iter()
            .map(|h| RecordHeader::new(h.key(), h.value().map(|v| v.to_vec())))
            .collect();

        if !self.records_builder.has_room_for(timestamp, key, value, &record_headers) {
            return None;
        }

        // Append the record in proper RecordBatch format.
        // If this fails, the builder is in a bad state (should not happen with valid inputs).
        if self.records_builder.append(timestamp, key, value, &record_headers).is_err() {
            return None;
        }

        let key_size = key.map_or(-1i32, |k| k.len() as i32);
        let value_size = value.map_or(-1i32, |v| v.len() as i32);

        let (tx, rx) = oneshot::channel();
        let offset_delta = self.record_count;
        self.pending.push(BatchedRecord { tx, offset_delta, key_size, value_size });
        self.record_count += 1;

        Some(SendFuture { rx })
    }

    /// Close this batch, preventing further appends.
    pub fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.records_builder.close_for_record_appends();
        }
    }

    /// Returns true if the batch has reached its size limit.
    pub fn is_full(&self) -> bool {
        self.records_builder.is_full()
    }

    /// Returns the number of records in this batch.
    pub fn record_count(&self) -> i32 {
        self.record_count
    }

    /// Returns when this batch was created.
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Returns the target TopicPartition.
    pub fn tp(&self) -> &TopicPartition {
        &self.tp
    }

    /// Returns the estimated serialized size in bytes (including batch header).
    ///
    /// After finalization, returns the exact finalized size.
    pub fn written_bytes(&self) -> usize {
        self.finalized_size
            .unwrap_or_else(|| self.records_builder.estimated_size_in_bytes())
    }

    /// Finalize the batch and return the serialized RecordBatch bytes.
    ///
    /// This closes the builder, writes the batch header (including CRC), and
    /// returns the complete RecordBatch bytes ready for the Kafka wire protocol.
    pub fn finalized_bytes(&mut self) -> Vec<u8> {
        match self.records_builder.close() {
            Ok(()) => {},
            Err(_) => {
                // Already closed or aborted; return empty
                return Vec::new();
            },
        }
        // Cache the finalized size before extracting bytes (since
        // extract_built_bytes replaces the builder with a dummy).
        self.finalized_size = Some(self.records_builder.estimated_size_in_bytes());
        self.extract_built_bytes()
    }

    /// Extract the built bytes by replacing the builder with a dummy one.
    fn extract_built_bytes(&mut self) -> Vec<u8> {
        // Create a dummy builder to swap in
        let dummy = MemoryRecordsBuilder::new(
            0,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            crate::common::record::NO_PARTITION_LEADER_EPOCH,
            0,
        )
        .expect("dummy builder creation must succeed");

        let real_builder = std::mem::replace(&mut self.records_builder, dummy);
        match real_builder.build() {
            Ok(memory_records) => memory_records.into_buffer(),
            Err(_) => Vec::new(),
        }
    }

    /// Returns the serialized batch data.
    ///
    /// If the batch has not been finalized yet, this finalizes it first.
    pub fn buffer(&mut self) -> Vec<u8> {
        self.finalized_bytes()
    }

    /// Complete all pending records with the given base offset and optional error.
    ///
    /// Called by the Sender after receiving a ProduceResponse. Consumes the batch.
    pub fn complete(self, base_offset: i64, log_append_time: i64, error: Option<&KafkaError>) {
        let topic = self.tp.topic().to_owned();
        let partition = self.tp.partition();

        for rec in self.pending {
            let result = match error {
                Some(e) => Err(KafkaError::new(e.code(), e.to_string())),
                None => Ok(RecordMetadata::new(
                    topic.clone(),
                    partition,
                    base_offset + rec.offset_delta as i64,
                    log_append_time,
                    rec.key_size,
                    rec.value_size,
                )),
            };
            // Ignore send failure -- receiver may have been dropped if caller
            // didn't await the SendFuture.
            let _ = rec.tx.send(result);
        }
    }

    /// Estimate the serialized size of a single record using the proper
    /// DefaultRecord wire format size calculation.
    pub fn estimate_record_size(key: Option<&[u8]>, value: Option<&[u8]>, headers: &[Header<'_>]) -> usize {
        let record_headers: Vec<RecordHeader> = headers
            .iter()
            .map(|h| RecordHeader::new(h.key(), h.value().map(|v| v.to_vec())))
            .collect();
        // Use the record batch overhead for the first record, plus the record itself.
        // For subsequent records, just the record size.
        let record_upper_bound = DefaultRecord::record_size_upper_bound(key, value, &record_headers);
        record_batch_header_size_in_bytes(CURRENT_MAGIC_VALUE, CompressionType::None) + record_upper_bound
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::default_record_batch;

    #[test]
    fn test_append_single_record() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let mut batch = ProducerBatch::new(tp, 4096);

        let future = batch.try_append(Some(b"key"), Some(b"value"), &[], 1000);
        assert!(future.is_some());
        assert_eq!(batch.record_count(), 1);
        // written_bytes includes the RecordBatch header (61 bytes) plus the record
        assert!(batch.written_bytes() > default_record_batch::RECORD_BATCH_OVERHEAD);
    }

    #[test]
    fn test_append_fills_batch() {
        let tp = TopicPartition::new("test".to_string(), 0);
        // RecordBatch header is 61 bytes. A record with key="k" and value="v"
        // needs about 8 bytes in varint format. Set max_bytes so the first record
        // fits but the second does not.
        // First record always fits (MemoryRecordsBuilder allows at least one record
        // even if write_limit is exceeded). Set write_limit to 0 so the first record
        // fits but the batch is immediately full afterward.
        let mut batch = ProducerBatch::new(tp, 0);

        let f1 = batch.try_append(Some(b"k"), Some(b"v"), &[], 1000);
        assert!(f1.is_some());
        assert!(batch.is_full());

        // Second record should fail -- batch is full
        let f2 = batch.try_append(Some(b"k2"), Some(b"v2"), &[], 1001);
        assert!(f2.is_none());
    }

    #[test]
    fn test_append_to_closed_batch() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let mut batch = ProducerBatch::new(tp, 4096);
        batch.close();

        let future = batch.try_append(Some(b"key"), Some(b"value"), &[], 1000);
        assert!(future.is_none());
    }

    #[tokio::test]
    async fn test_complete_resolves_futures() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let mut batch = ProducerBatch::new(tp, 4096);

        let f1 = batch.try_append(Some(b"k1"), Some(b"v1"), &[], 1000).unwrap();
        let f2 = batch.try_append(Some(b"k2"), Some(b"v2"), &[], 1001).unwrap();

        batch.complete(100, 2000, None);

        let m1 = f1.await.unwrap();
        assert_eq!(m1.offset(), 100);
        assert_eq!(m1.partition(), 0);
        assert_eq!(m1.topic(), "test");

        let m2 = f2.await.unwrap();
        assert_eq!(m2.offset(), 101);
    }

    #[tokio::test]
    async fn test_complete_with_error() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let mut batch = ProducerBatch::new(tp, 4096);

        let future = batch.try_append(Some(b"key"), Some(b"value"), &[], 1000).unwrap();

        let err = KafkaError::new(ErrorCode::NotLeaderOrFollower, "not leader");
        batch.complete(0, 0, Some(&err));

        let result = future.await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_retriable());
    }

    #[test]
    fn test_batch_with_headers() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let mut batch = ProducerBatch::new(tp, 4096);

        let headers = vec![Header::new("trace-id", Some(b"abc")), Header::new("source", None)];
        let future = batch.try_append(Some(b"key"), Some(b"value"), &headers, 1000);
        assert!(future.is_some());
        assert_eq!(batch.record_count(), 1);
    }

    #[test]
    fn test_finalized_bytes_produces_valid_record_batch() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let mut batch = ProducerBatch::new(tp, 4096);

        batch.try_append(Some(b"key1"), Some(b"value1"), &[], 1000).unwrap();
        batch.try_append(Some(b"key2"), Some(b"value2"), &[], 2000).unwrap();

        let bytes = batch.finalized_bytes();
        assert!(!bytes.is_empty());

        // The bytes should be a valid MemoryRecords buffer
        let mem_records = crate::common::record::MemoryRecords::from_buffer(bytes);
        let batches = mem_records.batches();
        assert_eq!(1, batches.len());

        let record_batch = &batches[0];
        assert!(record_batch.is_valid());
        assert_eq!(2, record_batch.count());

        let records = record_batch.iter_records().unwrap();
        assert_eq!(2, records.len());
        assert_eq!(Some(b"key1".as_slice()), records[0].key());
        assert_eq!(Some(b"value1".as_slice()), records[0].value());
        assert_eq!(Some(b"key2".as_slice()), records[1].key());
        assert_eq!(Some(b"value2".as_slice()), records[1].value());
    }

    #[test]
    fn test_finalized_bytes_with_headers() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let mut batch = ProducerBatch::new(tp, 4096);

        let headers = vec![Header::new("trace-id", Some(b"abc")), Header::new("source", None)];
        batch.try_append(Some(b"key"), Some(b"value"), &headers, 1000).unwrap();

        let bytes = batch.finalized_bytes();
        let mem_records = crate::common::record::MemoryRecords::from_buffer(bytes);
        let batches = mem_records.batches();
        assert_eq!(1, batches.len());

        let records = batches[0].iter_records().unwrap();
        assert_eq!(1, records.len());
        assert_eq!(2, records[0].headers().len());
        assert_eq!("trace-id", records[0].headers()[0].key);
        assert_eq!(Some(b"abc".to_vec()), records[0].headers()[0].value);
        assert_eq!("source", records[0].headers()[1].key);
        assert_eq!(None, records[0].headers()[1].value);
    }

    #[test]
    fn test_estimate_record_size_includes_batch_overhead() {
        let size = ProducerBatch::estimate_record_size(Some(b"key"), Some(b"value"), &[]);
        // Must include the RecordBatch header overhead (61 bytes) plus the record size
        assert!(size > default_record_batch::RECORD_BATCH_OVERHEAD);
    }
}
