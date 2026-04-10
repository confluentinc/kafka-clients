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

//! RecordAccumulator batches records by TopicPartition.
//!
//! Corresponds to org.apache.kafka.clients.producer.internals.RecordAccumulator.

use crate::clients::producer::batch::{ProducerBatch, SendFuture};
use crate::clients::producer::config::ProducerConfig;
use crate::clients::producer::record::Header;
use crate::common::TopicPartition;
use crate::errors::{ErrorCode, KafkaError};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, Semaphore};

/// Result of appending a record to the accumulator.
#[derive(Debug)]
pub struct AppendResult {
    /// Future that resolves when the record is acknowledged.
    pub future: SendFuture,
    /// Whether the batch is now full.
    pub batch_is_full: bool,
    /// Whether a new batch was created for this record.
    pub new_batch_created: bool,
}

struct AccumulatorInner {
    /// Map from TopicPartition to the current open batch.
    current_batches: HashMap<TopicPartition, ProducerBatch>,
    /// Batches that are full or have exceeded linger.ms, ready to be sent.
    ready_batches: Vec<ProducerBatch>,
    /// Whether the accumulator has been closed.
    closed: bool,
}

/// Accumulates records into ProducerBatches grouped by TopicPartition.
///
/// Thread-safe: inner state is behind a Mutex.
/// Memory-bounded: uses a Semaphore with permits equal to `buffer.memory` bytes.
pub struct RecordAccumulator {
    inner: Mutex<AccumulatorInner>,
    /// Semaphore with permits = buffer.memory bytes.
    /// Each append acquires permits equal to the record's estimated size.
    memory_semaphore: Arc<Semaphore>,
    /// Notifies the sender task that a batch is ready.
    batch_ready_notify: Notify,
    config: Arc<ProducerConfig>,
}

impl RecordAccumulator {
    /// Create a new accumulator with the given configuration.
    pub fn new(config: Arc<ProducerConfig>) -> Self {
        let permits = config.buffer_memory();
        RecordAccumulator {
            inner: Mutex::new(AccumulatorInner {
                current_batches: HashMap::new(),
                ready_batches: Vec::new(),
                closed: false,
            }),
            memory_semaphore: Arc::new(Semaphore::new(permits)),
            batch_ready_notify: Notify::new(),
            config,
        }
    }

    /// Append a record to the accumulator.
    ///
    /// 1. Acquires memory permits (blocks up to max.block.ms).
    /// 2. Finds or creates a ProducerBatch for the target partition.
    /// 3. Copies key/value/headers into the batch buffer.
    /// 4. If the batch is full, moves it to ready_batches and notifies the sender.
    pub async fn append(
        &self,
        tp: &TopicPartition,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[Header<'_>],
        timestamp: i64,
    ) -> crate::errors::Result<AppendResult> {
        let estimated_size = Self::estimate_size(key, value, headers);

        // Acquire memory permits with timeout.
        let permit_result = tokio::time::timeout(
            self.config.max_block(),
            self.memory_semaphore.acquire_many(estimated_size as u32),
        )
        .await;

        match permit_result {
            Ok(Ok(permit)) => {
                // Forget the permit — we'll release it manually when the batch completes.
                permit.forget();
            },
            Ok(Err(_closed)) => {
                return Err(KafkaError::new(ErrorCode::Unexpected, "producer is shutting down"));
            },
            Err(_timeout) => {
                return Err(KafkaError::new(
                    ErrorCode::BufferExhausted,
                    format!(
                        "failed to allocate {} bytes within {:?}",
                        estimated_size,
                        self.config.max_block()
                    ),
                ));
            },
        }

        let mut inner = self.inner.lock().await;

        if inner.closed {
            // Release the memory we just acquired.
            self.memory_semaphore.add_permits(estimated_size);
            return Err(KafkaError::new(ErrorCode::Unexpected, "accumulator is closed"));
        }

        // Try to append to existing batch.
        if let Some(batch) = inner.current_batches.get_mut(tp) {
            match batch.try_append(key, value, headers, timestamp) {
                Ok(Some(future)) => {
                    batch.add_permits(estimated_size);
                    let batch_is_full = batch.is_full();
                    if batch_is_full {
                        let full_batch = inner.current_batches.remove(tp).unwrap();
                        inner.ready_batches.push(full_batch);
                        self.batch_ready_notify.notify_one();
                    }
                    return Ok(AppendResult { future, batch_is_full, new_batch_created: false });
                },
                Ok(None) => {
                    // Current batch is full — move it to ready and create a new one.
                    let full_batch = inner.current_batches.remove(tp).unwrap();
                    inner.ready_batches.push(full_batch);
                    self.batch_ready_notify.notify_one();
                },
                Err(e) => {
                    // Input validation error (e.g. invalid timestamp) — release
                    // memory and propagate.
                    self.memory_semaphore.add_permits(estimated_size);
                    return Err(e);
                },
            }
        }

        // Create a new batch.
        let mut batch = ProducerBatch::new(tp.clone(), self.config.batch_size());
        let future = match batch.try_append(key, value, headers, timestamp) {
            Ok(Some(f)) => f,
            Ok(None) => {
                // Should not happen: a new batch must always accept the first record.
                self.memory_semaphore.add_permits(estimated_size);
                return Err(KafkaError::new(ErrorCode::Unexpected, "new batch rejected first record"));
            },
            Err(e) => {
                self.memory_semaphore.add_permits(estimated_size);
                return Err(e);
            },
        };
        batch.add_permits(estimated_size);
        let new_batch_created = true;
        let batch_is_full = batch.is_full();

        if batch_is_full {
            inner.ready_batches.push(batch);
            self.batch_ready_notify.notify_one();
        } else {
            inner.current_batches.insert(tp.clone(), batch);
        }

        Ok(AppendResult { future, batch_is_full, new_batch_created })
    }

    /// Drain all ready batches. Called by the Sender task.
    pub async fn drain(&self) -> Vec<ProducerBatch> {
        let mut inner = self.inner.lock().await;
        std::mem::take(&mut inner.ready_batches)
    }

    /// Move batches that have exceeded linger.ms to the ready queue.
    pub async fn expire_lingering_batches(&self) {
        let linger = self.config.linger();
        if linger.is_zero() {
            return;
        }

        let mut inner = self.inner.lock().await;
        let expired_tps: Vec<TopicPartition> = inner
            .current_batches
            .iter()
            .filter(|(_, batch)| batch.created_at().elapsed() >= linger)
            .map(|(tp, _)| tp.clone())
            .collect();

        for tp in expired_tps {
            if let Some(batch) = inner.current_batches.remove(&tp) {
                inner.ready_batches.push(batch);
            }
        }

        if !inner.ready_batches.is_empty() {
            self.batch_ready_notify.notify_one();
        }
    }

    /// Force all current batches to become ready (for flush).
    pub async fn flush_all(&self) {
        let mut inner = self.inner.lock().await;
        let tps: Vec<TopicPartition> = inner.current_batches.keys().cloned().collect();
        for tp in tps {
            if let Some(mut batch) = inner.current_batches.remove(&tp) {
                batch.close();
                inner.ready_batches.push(batch);
            }
        }
        if !inner.ready_batches.is_empty() {
            self.batch_ready_notify.notify_one();
        }
    }

    /// Close the accumulator. Flushes remaining batches and prevents new appends.
    pub async fn close(&self) {
        self.flush_all().await;
        let mut inner = self.inner.lock().await;
        inner.closed = true;
        self.batch_ready_notify.notify_one();
    }

    /// Returns whether the accumulator has been closed.
    pub async fn is_closed(&self) -> bool {
        let inner = self.inner.lock().await;
        inner.closed && inner.ready_batches.is_empty() && inner.current_batches.is_empty()
    }

    /// Wait for a batch to become ready, with a timeout.
    pub async fn wait_for_batch_ready(&self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, self.batch_ready_notify.notified()).await;
    }

    /// Release memory permits after a batch has been completed by the sender.
    pub fn release_memory(&self, bytes: usize) {
        self.memory_semaphore.add_permits(bytes);
    }

    fn estimate_size(key: Option<&[u8]>, value: Option<&[u8]>, headers: &[Header<'_>]) -> usize {
        // Use the record-only size estimate. The batch header overhead is shared
        // across all records in a batch and is accounted for by the batch itself
        // via `written_bytes()` / `estimated_size_in_bytes()`. Acquiring only the
        // record-level permits per append prevents leaking `(N-1) * 61` bytes of
        // semaphore permits per batch (where 61 is the batch header overhead).
        ProducerBatch::estimate_record_size(key, value, headers).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Arc<ProducerConfig> {
        Arc::new(
            ProducerConfig::builder()
                .bootstrap_servers(vec!["localhost:9092".to_string()])
                .batch_size(1024)
                .linger_ms(0)
                .buffer_memory(65536)
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_append_creates_batch() {
        let acc = RecordAccumulator::new(test_config());
        let tp = TopicPartition::new("test".to_string(), 0);

        let result = acc.append(&tp, Some(b"key"), Some(b"value"), &[], 1000).await;
        assert!(result.is_ok());
        let r = result.unwrap();
        assert!(r.new_batch_created);
    }

    #[tokio::test]
    async fn test_append_reuses_batch() {
        let acc = RecordAccumulator::new(test_config());
        let tp = TopicPartition::new("test".to_string(), 0);

        let r1 = acc.append(&tp, Some(b"k1"), Some(b"v1"), &[], 1000).await.unwrap();
        assert!(r1.new_batch_created);

        let r2 = acc.append(&tp, Some(b"k2"), Some(b"v2"), &[], 1001).await.unwrap();
        assert!(!r2.new_batch_created);
    }

    #[tokio::test]
    async fn test_drain_returns_ready_batches() {
        let config = Arc::new(
            ProducerConfig::builder()
                .bootstrap_servers(vec!["localhost:9092".to_string()])
                .batch_size(20) // Very small batch
                .buffer_memory(65536)
                .build()
                .unwrap(),
        );
        let acc = RecordAccumulator::new(config);
        let tp = TopicPartition::new("test".to_string(), 0);

        // Fill the batch to trigger it becoming ready.
        acc.append(&tp, Some(b"key"), Some(b"value"), &[], 1000).await.unwrap();

        // The batch should be full and moved to ready.
        let batches = acc.drain().await;
        assert_eq!(batches.len(), 1);
    }

    #[tokio::test]
    async fn test_flush_moves_all_to_ready() {
        let acc = RecordAccumulator::new(test_config());
        let tp1 = TopicPartition::new("topic1".to_string(), 0);
        let tp2 = TopicPartition::new("topic2".to_string(), 0);

        acc.append(&tp1, Some(b"k1"), Some(b"v1"), &[], 1000).await.unwrap();
        acc.append(&tp2, Some(b"k2"), Some(b"v2"), &[], 1001).await.unwrap();

        acc.flush_all().await;
        let batches = acc.drain().await;
        assert_eq!(batches.len(), 2);
    }

    #[tokio::test]
    async fn test_close_prevents_appends() {
        let acc = RecordAccumulator::new(test_config());
        let tp = TopicPartition::new("test".to_string(), 0);

        acc.close().await;

        let result = acc.append(&tp, Some(b"key"), Some(b"value"), &[], 1000).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::Unexpected);
    }

    #[tokio::test]
    async fn test_memory_permits_balanced_after_multi_record_batch() {
        // Verify that appending N records to one batch does NOT leak
        // (N-1) * batch_header_overhead permits. After draining and releasing,
        // available permits should return to the original pool size.
        let buffer_memory = 65536usize;
        let config = Arc::new(
            ProducerConfig::builder()
                .bootstrap_servers(vec!["localhost:9092".to_string()])
                .batch_size(4096)
                .linger_ms(0)
                .buffer_memory(buffer_memory)
                .build()
                .unwrap(),
        );
        let acc = RecordAccumulator::new(config);
        let tp = TopicPartition::new("test".to_string(), 0);

        let initial_permits = acc.memory_semaphore.available_permits();
        assert_eq!(initial_permits, buffer_memory);

        // Append 10 records to the same batch.
        for i in 0..10 {
            acc.append(&tp, Some(b"key"), Some(b"value"), &[], 1000 + i).await.unwrap();
        }

        // Flush and drain the batch.
        acc.flush_all().await;
        let batches = acc.drain().await;
        assert_eq!(1, batches.len());

        // Release memory using the permits_acquired (what the sender does).
        for batch in batches {
            let permits = batch.permits_acquired();
            assert!(permits > 0);
            acc.release_memory(permits);
        }

        // All permits should be returned (within a small margin for rounding).
        // With the old bug, this would lose (10-1)*61 = 549 permits.
        let final_permits = acc.memory_semaphore.available_permits();
        assert_eq!(
            final_permits,
            initial_permits,
            "Permits leaked: acquired {}, released back to {}",
            initial_permits - final_permits,
            final_permits
        );
    }

    #[tokio::test]
    async fn test_append_invalid_timestamp_returns_error() {
        let acc = RecordAccumulator::new(test_config());
        let tp = TopicPartition::new("test".to_string(), 0);

        // Negative timestamp (not NO_TIMESTAMP=-1) should propagate as an error.
        let result = acc.append(&tp, Some(b"key"), Some(b"value"), &[], -5).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::InvalidArgument);

        // Verify that the memory permits were released on error.
        let permits = acc.memory_semaphore.available_permits();
        assert_eq!(permits, 65536, "Permits should be fully returned after error");
    }
}
