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
    /// Permits are acquired per-batch: `max(batch_size, estimated_record_size)`,
    /// matching Java's `BufferPool.allocate()` model.
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
    /// Matches Java's `RecordAccumulator.append()` flow:
    ///
    /// 1. Try to append to an existing batch first (no new allocation needed —
    ///    the batch was pre-allocated when it was created).
    /// 2. If no batch exists or the current batch is full, release the lock,
    ///    allocate `max(batch_size, estimated_record_size)` permits from the
    ///    memory semaphore (potentially blocking up to `max.block.ms`), then
    ///    re-acquire the lock.
    /// 3. After re-acquiring the lock, try the existing batch again (another
    ///    caller may have created one while we were waiting for permits).
    /// 4. If still needed, create a new batch with the full allocation size.
    ///
    /// This per-batch allocation model matches Java's `BufferPool.allocate(size)`
    /// where `size = Math.max(batchSize, estimateSizeInBytesUpperBound(...))`.
    pub async fn append(
        &self,
        tp: &TopicPartition,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[Header<'_>],
        timestamp: i64,
    ) -> crate::errors::Result<AppendResult> {
        // --- Phase 1: try existing batch under the lock (no allocation) ---
        {
            let mut inner = self.inner.lock().await;

            if inner.closed {
                return Err(KafkaError::new(ErrorCode::Unexpected, "accumulator is closed"));
            }

            if let Some(batch) = inner.current_batches.get_mut(tp) {
                match batch.try_append(key, value, headers, timestamp) {
                    Ok(Some(future)) => {
                        // Appended to existing batch — no permit acquisition needed.
                        let batch_is_full = batch.is_full();
                        if batch_is_full {
                            let full_batch = inner.current_batches.remove(tp).unwrap();
                            inner.ready_batches.push(full_batch);
                            self.batch_ready_notify.notify_one();
                        }
                        return Ok(AppendResult { future, batch_is_full, new_batch_created: false });
                    },
                    Ok(None) => {
                        // Current batch is full — move it to ready.
                        let full_batch = inner.current_batches.remove(tp).unwrap();
                        inner.ready_batches.push(full_batch);
                        self.batch_ready_notify.notify_one();
                        // Fall through to allocate a new batch.
                    },
                    Err(e) => {
                        return Err(e);
                    },
                }
            }
            // Release the lock before the potentially-blocking semaphore acquire.
        }

        // --- Phase 2: allocate memory for a new batch ---
        // Match Java: size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(...))
        let estimated_size = Self::estimate_size(key, value, headers);
        let batch_alloc_size = self.config.batch_size().max(estimated_size);

        let permit_result = tokio::time::timeout(
            self.config.max_block(),
            self.memory_semaphore.acquire_many(batch_alloc_size as u32),
        )
        .await;

        match permit_result {
            Ok(Ok(permit)) => {
                // Forget the permit — we release manually via release_memory()
                // when the batch completes.
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
                        batch_alloc_size,
                        self.config.max_block()
                    ),
                ));
            },
        }

        // --- Phase 3: re-acquire lock and double-check ---
        let mut inner = self.inner.lock().await;

        if inner.closed {
            self.memory_semaphore.add_permits(batch_alloc_size);
            return Err(KafkaError::new(ErrorCode::Unexpected, "accumulator is closed"));
        }

        // Another caller may have created a batch while we were waiting for
        // permits. Try the existing batch again before creating a new one.
        if let Some(batch) = inner.current_batches.get_mut(tp) {
            match batch.try_append(key, value, headers, timestamp) {
                Ok(Some(future)) => {
                    // Appended — release the permits we just acquired.
                    self.memory_semaphore.add_permits(batch_alloc_size);
                    let batch_is_full = batch.is_full();
                    if batch_is_full {
                        let full_batch = inner.current_batches.remove(tp).unwrap();
                        inner.ready_batches.push(full_batch);
                        self.batch_ready_notify.notify_one();
                    }
                    return Ok(AppendResult { future, batch_is_full, new_batch_created: false });
                },
                Ok(None) => {
                    // Batch appeared but is already full — move to ready.
                    let full_batch = inner.current_batches.remove(tp).unwrap();
                    inner.ready_batches.push(full_batch);
                    self.batch_ready_notify.notify_one();
                },
                Err(e) => {
                    self.memory_semaphore.add_permits(batch_alloc_size);
                    return Err(e);
                },
            }
        }

        // --- Phase 4: create new batch with full allocation size ---
        let mut batch = ProducerBatch::new(tp.clone(), batch_alloc_size);
        batch.set_permits_acquired(batch_alloc_size);
        let future = match batch.try_append(key, value, headers, timestamp) {
            Ok(Some(f)) => f,
            Ok(None) => {
                // Should not happen: a new batch must always accept the first record.
                self.memory_semaphore.add_permits(batch_alloc_size);
                return Err(KafkaError::new(ErrorCode::Unexpected, "new batch rejected first record"));
            },
            Err(e) => {
                self.memory_semaphore.add_permits(batch_alloc_size);
                return Err(e);
            },
        };
        let batch_is_full = batch.is_full();

        if batch_is_full {
            inner.ready_batches.push(batch);
            self.batch_ready_notify.notify_one();
        } else {
            inner.current_batches.insert(tp.clone(), batch);
        }

        Ok(AppendResult { future, batch_is_full, new_batch_created: true })
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
        // Estimate the full batch size (record + batch header overhead),
        // matching Java's AbstractRecords.estimateSizeInBytesUpperBound()
        // which returns RECORD_BATCH_OVERHEAD (61) + record size.
        // The allocation size is max(batch_size, this estimate).
        (ProducerBatch::estimate_record_size(key, value, headers) + ProducerBatch::batch_header_overhead()).max(1)
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
        let acc = RecordAccumulator::new(test_config());
        let tp = TopicPartition::new("test".to_string(), 0);

        acc.append(&tp, Some(b"key"), Some(b"value"), &[], 1000).await.unwrap();

        // Flush to move the batch to ready (matching how the sender triggers drain).
        acc.flush_all().await;

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
        // Verify that the per-batch allocation model correctly returns all
        // permits after a batch is drained and released. With per-batch
        // allocation, permits_acquired equals max(batch_size, record_estimate)
        // — set once at batch creation, not per-record.
        let buffer_memory = 65536usize;
        let batch_size = 4096usize;
        let config = Arc::new(
            ProducerConfig::builder()
                .bootstrap_servers(vec!["localhost:9092".to_string()])
                .batch_size(batch_size)
                .linger_ms(0)
                .buffer_memory(buffer_memory)
                .build()
                .unwrap(),
        );
        let acc = RecordAccumulator::new(config);
        let tp = TopicPartition::new("test".to_string(), 0);

        let initial_permits = acc.memory_semaphore.available_permits();
        assert_eq!(initial_permits, buffer_memory);

        // Append 10 records to the same batch. Only the first record triggers
        // permit acquisition (batch_size permits). Subsequent records append
        // to the existing batch without acquiring additional permits.
        for i in 0..10 {
            acc.append(&tp, Some(b"key"), Some(b"value"), &[], 1000 + i).await.unwrap();
        }

        // After one batch is created, exactly batch_size permits should be held.
        let permits_after_append = acc.memory_semaphore.available_permits();
        assert_eq!(
            permits_after_append,
            initial_permits - batch_size,
            "Should have acquired exactly batch_size permits"
        );

        // Flush and drain the batch.
        acc.flush_all().await;
        let batches = acc.drain().await;
        assert_eq!(1, batches.len());

        // Release memory using the permits_acquired (what the sender does).
        for batch in batches {
            let permits = batch.permits_acquired();
            assert_eq!(permits, batch_size, "permits_acquired should equal batch_size");
            acc.release_memory(permits);
        }

        // All permits should be returned.
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
        // With per-batch allocation, max(batch_size, record_estimate) permits
        // are acquired for the new batch, then released on error.
        let permits = acc.memory_semaphore.available_permits();
        assert_eq!(permits, 65536, "Permits should be fully returned after error");
    }

    #[tokio::test]
    async fn test_large_record_allocates_more_than_batch_size() {
        // When a record is larger than batch_size, the allocation should be
        // max(batch_size, estimated_record_size), matching Java's
        // Math.max(batchSize, estimateSizeInBytesUpperBound(...)).
        let buffer_memory = 65536usize;
        let batch_size = 64usize; // Very small batch size
        let config = Arc::new(
            ProducerConfig::builder()
                .bootstrap_servers(vec!["localhost:9092".to_string()])
                .batch_size(batch_size)
                .buffer_memory(buffer_memory)
                .build()
                .unwrap(),
        );
        let acc = RecordAccumulator::new(config);
        let tp = TopicPartition::new("test".to_string(), 0);

        // Create a value larger than batch_size.
        let large_value = vec![b'x'; 200];
        let estimated = RecordAccumulator::estimate_size(Some(b"key"), Some(&large_value), &[]);
        assert!(
            estimated > batch_size,
            "batch estimate ({}) should exceed batch_size ({})",
            estimated,
            batch_size
        );

        let initial_permits = acc.memory_semaphore.available_permits();

        let result = acc.append(&tp, Some(b"key"), Some(&large_value), &[], 1000).await;
        assert!(result.is_ok());

        // The permits acquired should be the batch estimate (larger than batch_size).
        let permits_used = initial_permits - acc.memory_semaphore.available_permits();
        assert_eq!(
            permits_used, estimated,
            "should allocate batch estimate ({}) when it exceeds batch_size ({})",
            estimated, batch_size
        );

        // Drain and release — permits should be fully returned.
        acc.flush_all().await;
        let batches = acc.drain().await;
        assert_eq!(1, batches.len());
        for batch in batches {
            assert_eq!(batch.permits_acquired(), estimated);
            acc.release_memory(batch.permits_acquired());
        }
        assert_eq!(acc.memory_semaphore.available_permits(), initial_permits);
    }

    #[tokio::test]
    async fn test_batch_overflow_creates_new_batch_with_separate_allocation() {
        // When a batch is full and a new record arrives, a new batch should be
        // created with its own per-batch allocation.
        //
        // Use a large record so the estimate upper-bound ≈ actual written size.
        // With a 200-byte value, the record's varint overheads are dwarfed by
        // the payload, so one record fills the batch.  batch_size is set to the
        // estimate so that write_limit = estimate = batch_alloc_size.
        let buffer_memory = 65536usize;
        let big_value = vec![0u8; 200];
        let estimated_one = RecordAccumulator::estimate_size(Some(b"k"), Some(&big_value), &[]);
        let batch_size = estimated_one; // write_limit = estimate, one big record fills it
        let config = Arc::new(
            ProducerConfig::builder()
                .bootstrap_servers(vec!["localhost:9092".to_string()])
                .batch_size(batch_size)
                .buffer_memory(buffer_memory)
                .build()
                .unwrap(),
        );
        let acc = RecordAccumulator::new(config);
        let tp = TopicPartition::new("test".to_string(), 0);

        let initial_permits = acc.memory_semaphore.available_permits();

        // First record creates batch 1 and fills it.
        let r1 = acc.append(&tp, Some(b"k"), Some(&big_value), &[], 1000).await.unwrap();
        assert!(r1.new_batch_created);
        let permits_after_first = acc.memory_semaphore.available_permits();

        // Second record should create a new batch with its own allocation.
        let r2 = acc.append(&tp, Some(b"k"), Some(&big_value), &[], 1001).await.unwrap();
        assert!(r2.new_batch_created);
        let permits_after_second = acc.memory_semaphore.available_permits();

        // Two batches should have been allocated.
        let total_acquired = initial_permits - permits_after_second;
        let first_acquired = initial_permits - permits_after_first;
        let second_acquired = permits_after_first - permits_after_second;
        assert!(
            first_acquired > 0 && second_acquired > 0,
            "each batch should acquire its own permits: first={}, second={}",
            first_acquired,
            second_acquired
        );
        assert_eq!(total_acquired, first_acquired + second_acquired);
    }

    #[tokio::test]
    async fn test_no_permits_acquired_for_appending_to_existing_batch() {
        // When appending to an existing batch that has room, no new permits
        // should be acquired — the batch was already pre-allocated.
        let buffer_memory = 65536usize;
        let batch_size = 4096usize;
        let config = Arc::new(
            ProducerConfig::builder()
                .bootstrap_servers(vec!["localhost:9092".to_string()])
                .batch_size(batch_size)
                .buffer_memory(buffer_memory)
                .build()
                .unwrap(),
        );
        let acc = RecordAccumulator::new(config);
        let tp = TopicPartition::new("test".to_string(), 0);

        // First append creates a batch, acquiring batch_size permits.
        let r1 = acc.append(&tp, Some(b"k1"), Some(b"v1"), &[], 1000).await.unwrap();
        assert!(r1.new_batch_created);
        let permits_after_first = acc.memory_semaphore.available_permits();
        assert_eq!(permits_after_first, buffer_memory - batch_size);

        // Second append to the same partition — should NOT acquire any permits.
        let r2 = acc.append(&tp, Some(b"k2"), Some(b"v2"), &[], 1001).await.unwrap();
        assert!(!r2.new_batch_created);
        let permits_after_second = acc.memory_semaphore.available_permits();
        assert_eq!(
            permits_after_second, permits_after_first,
            "appending to existing batch should not acquire additional permits"
        );
    }
}
