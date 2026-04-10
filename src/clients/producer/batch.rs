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
//! Corresponds to org.apache.kafka.clients.producer.internals.ProducerBatch.

use crate::common::TopicPartition;
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
/// Records are serialized into a contiguous byte buffer. Each record's
/// key/value/headers are copied exactly once during `try_append()`.
pub struct ProducerBatch {
    tp: TopicPartition,
    buffer: Vec<u8>,
    pending: Vec<BatchedRecord>,
    record_count: i32,
    max_bytes: usize,
    created_at: Instant,
    closed: bool,
}

impl ProducerBatch {
    /// Create a new batch for the given TopicPartition.
    pub fn new(tp: TopicPartition, max_bytes: usize) -> Self {
        ProducerBatch {
            tp,
            buffer: Vec::with_capacity(max_bytes.min(1024)),
            pending: Vec::new(),
            record_count: 0,
            max_bytes,
            created_at: Instant::now(),
            closed: false,
        }
    }

    /// Try to append a record to this batch.
    ///
    /// Returns `Some(SendFuture)` on success, `None` if the batch is full or closed.
    /// This is the single copy point: key/value/headers are written into the buffer.
    pub fn try_append(
        &mut self,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[Header<'_>],
        _timestamp: i64,
    ) -> Option<SendFuture> {
        if self.closed {
            return None;
        }

        let record_size = Self::estimate_record_size(key, value, headers);
        if !self.buffer.is_empty() && self.buffer.len() + record_size > self.max_bytes {
            return None;
        }

        // Serialize the record into the buffer (single copy of key/value/headers).
        // Simplified format: [key_len:4][key][value_len:4][value][header_count:4][headers...]
        let key_size = key.map_or(-1i32, |k| k.len() as i32);
        let value_size = value.map_or(-1i32, |v| v.len() as i32);

        self.buffer.extend_from_slice(&key_size.to_be_bytes());
        if let Some(k) = key {
            self.buffer.extend_from_slice(k);
        }

        self.buffer.extend_from_slice(&value_size.to_be_bytes());
        if let Some(v) = value {
            self.buffer.extend_from_slice(v);
        }

        let header_count = headers.len() as i32;
        self.buffer.extend_from_slice(&header_count.to_be_bytes());
        for h in headers {
            let hk = h.key().as_bytes();
            self.buffer
                .extend_from_slice(&(hk.len() as i32).to_be_bytes());
            self.buffer.extend_from_slice(hk);
            let hv_size = h.value().map_or(-1i32, |v| v.len() as i32);
            self.buffer.extend_from_slice(&hv_size.to_be_bytes());
            if let Some(hv) = h.value() {
                self.buffer.extend_from_slice(hv);
            }
        }

        let (tx, rx) = oneshot::channel();
        let offset_delta = self.record_count;
        self.pending.push(BatchedRecord {
            tx,
            offset_delta,
            key_size,
            value_size,
        });
        self.record_count += 1;

        Some(SendFuture { rx })
    }

    /// Close this batch, preventing further appends.
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Returns true if the batch has reached its size limit.
    pub fn is_full(&self) -> bool {
        self.buffer.len() >= self.max_bytes
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

    /// Returns the current serialized size in bytes.
    pub fn written_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the serialized batch data.
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
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
            // Ignore send failure — receiver may have been dropped if caller
            // didn't await the SendFuture.
            let _ = rec.tx.send(result);
        }
    }

    /// Estimate the serialized size of a single record.
    fn estimate_record_size(
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[Header<'_>],
    ) -> usize {
        let key_bytes = key.map_or(0, |k| k.len());
        let value_bytes = value.map_or(0, |v| v.len());
        let header_bytes: usize = headers
            .iter()
            .map(|h| 8 + h.key().len() + h.value().map_or(0, |v| v.len()))
            .sum();
        // 4 (key_len) + key + 4 (value_len) + value + 4 (header_count) + headers
        12 + key_bytes + value_bytes + header_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_single_record() {
        let tp = TopicPartition::new("test".to_string(), 0);
        let mut batch = ProducerBatch::new(tp, 4096);

        let future = batch.try_append(Some(b"key"), Some(b"value"), &[], 1000);
        assert!(future.is_some());
        assert_eq!(batch.record_count(), 1);
        assert!(batch.written_bytes() > 0);
    }

    #[test]
    fn test_append_fills_batch() {
        let tp = TopicPartition::new("test".to_string(), 0);
        // First record: key(1) + value(1) + overhead(12) = 14 bytes.
        // Set max to 14 so first record fills exactly, second is rejected.
        let mut batch = ProducerBatch::new(tp, 14);

        let f1 = batch.try_append(Some(b"k"), Some(b"v"), &[], 1000);
        assert!(f1.is_some());
        assert!(batch.is_full());

        // Second record should fail — batch is full
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

        let headers = vec![
            Header::new("trace-id", Some(b"abc")),
            Header::new("source", None),
        ];
        let future = batch.try_append(Some(b"key"), Some(b"value"), &headers, 1000);
        assert!(future.is_some());
        assert_eq!(batch.record_count(), 1);
    }
}
