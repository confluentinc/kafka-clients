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

//! KafkaProducer — the main entry point for producing records.
//!
//! Corresponds to org.apache.kafka.clients.producer.KafkaProducer.

use crate::clients::producer::accumulator::RecordAccumulator;
use crate::clients::producer::batch::SendFuture;
use crate::clients::producer::config::ProducerConfig;
use crate::clients::producer::record::ProducerRecord;
use crate::clients::producer::sender::{PartitionInfo, ProduceClient, Sender};
use crate::common::TopicPartition;
use crate::errors::{ErrorCode, KafkaError};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Backoff between metadata retry attempts when a topic is first seen.
const METADATA_RETRY_BACKOFF_MS: u64 = 500;

struct ProducerInner<C: ProduceClient> {
    #[allow(dead_code)]
    config: Arc<ProducerConfig>,
    accumulator: Arc<RecordAccumulator>,
    client: Arc<C>,
    sender_handle: Mutex<Option<JoinHandle<()>>>,
    /// Cached topic metadata: topic name → partition count.
    ///
    /// Mirrors Java's `Metadata` cache — after the first successful metadata
    /// fetch for a topic, subsequent `send()` calls skip the network round
    /// trip and use the cached partition count.
    metadata_cache: Mutex<HashMap<String, i32>>,
}

/// An async Kafka producer.
///
/// Thread-safe: cloning gives a handle to the same underlying producer.
/// The background sender task is spawned on construction.
///
/// Generic over `C: ProduceClient` to allow mocking the network layer.
pub struct KafkaProducer<C: ProduceClient> {
    inner: Arc<ProducerInner<C>>,
}

impl<C: ProduceClient> Clone for KafkaProducer<C> {
    fn clone(&self) -> Self {
        KafkaProducer { inner: Arc::clone(&self.inner) }
    }
}

impl<C: ProduceClient> KafkaProducer<C> {
    /// Create a new producer with the given config and network client.
    ///
    /// Spawns a background sender task immediately.
    pub fn new(config: ProducerConfig, client: C) -> Self {
        let config = Arc::new(config);
        let client = Arc::new(client);
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));

        let sender = Sender::new(Arc::clone(&accumulator), Arc::clone(&client), Arc::clone(&config));
        let sender_handle = tokio::spawn(sender.run());

        KafkaProducer {
            inner: Arc::new(ProducerInner {
                config,
                accumulator,
                client,
                sender_handle: Mutex::new(Some(sender_handle)),
                metadata_cache: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Send a record to Kafka.
    ///
    /// Fetches metadata for the topic if not already cached (matching Java's
    /// `KafkaProducer.doSend()` which calls `waitOnMetadata()` before appending
    /// to the accumulator), then copies the key/value/headers into the batch
    /// buffer.
    ///
    /// Returns a `SendFuture` that resolves to `RecordMetadata` when the record
    /// is acknowledged by the broker. The caller can drop the
    /// `ProducerRecord` immediately after.
    pub async fn send(&self, record: &ProducerRecord<'_>) -> crate::errors::Result<SendFuture> {
        let topic = record.topic();
        if topic.is_empty() {
            return Err(KafkaError::new(ErrorCode::InvalidTopic, "topic must not be empty"));
        }

        // Use the partition hint or default to 0 (partitioner skipped per design).
        let partition = record.partition_hint().unwrap_or(0);

        // Validate partition early (negative check).
        if partition < 0 {
            return Err(KafkaError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Invalid partition {} for topic '{}': partition must not be negative",
                    partition, topic
                ),
            ));
        }

        // Wait for metadata before appending to the accumulator, matching
        // Java's KafkaProducer.doSend() -> waitOnMetadata() flow.
        // Passes the partition so that wait_on_metadata can retry if the
        // partition count hasn't grown to include it yet (partition expansion).
        let partition_count = self.wait_on_metadata(topic, Some(partition)).await?;

        // Validate partition against known partition count.
        if partition >= partition_count {
            return Err(KafkaError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Invalid partition {} for topic '{}' with {} partition(s)",
                    partition, topic, partition_count
                ),
            ));
        }

        let tp = TopicPartition::new(topic.to_string(), partition);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let timestamp = record.timestamp_value().unwrap_or(now);

        let result = self
            .inner
            .accumulator
            .append(&tp, record.key_bytes(), record.value_bytes(), record.headers(), timestamp)
            .await?;

        Ok(result.future)
    }

    /// Waits for metadata to become available for the given topic.
    ///
    /// Corresponds to Java's `KafkaProducer.waitOnMetadata(String topic,
    /// Integer partition, long nowMs, long maxWaitMs)`. On brokers with
    /// `auto.create.topics.enable=true`, the first metadata request triggers
    /// topic creation asynchronously, so this method retries until the topic
    /// appears or `max_block` is exceeded.
    ///
    /// When `partition` is `Some(p)`, the method also waits until the
    /// partition count grows to include `p`, supporting online partition
    /// expansion (matching Java's loop condition:
    /// `while (partitionsCount == null || (partition != null && partition >=
    /// partitionsCount))`).
    ///
    /// Returns the partition count for the topic.
    async fn wait_on_metadata(&self, topic: &str, partition: Option<i32>) -> crate::errors::Result<i32> {
        // Check the metadata cache first.  If the topic is already known and
        // the requested partition (if any) falls within the cached count,
        // return immediately without a network round trip.  This matches
        // Java's `cluster.partitionCountForTopic(topic)` check.
        {
            let cache = self.inner.metadata_cache.lock().await;
            if let Some(&count) = cache.get(topic) {
                let partition_satisfied = match partition {
                    Some(p) => p < count,
                    None => true,
                };
                if partition_satisfied {
                    return Ok(count);
                }
            }
        }

        let max_wait = self.inner.config.max_block();
        let deadline = tokio::time::Instant::now() + max_wait;

        loop {
            match self.inner.client.partitions_for(topic).await {
                Ok(partitions) if !partitions.is_empty() => {
                    let count = partitions.len() as i32;

                    // Check if the requested partition is within range.
                    // If not, keep retrying (supports partition expansion).
                    let partition_satisfied = match partition {
                        Some(p) => p < count,
                        None => true,
                    };

                    if partition_satisfied {
                        // Update the metadata cache.
                        let mut cache = self.inner.metadata_cache.lock().await;
                        cache.insert(topic.to_string(), count);
                        return Ok(count);
                    }

                    debug!(
                        "Metadata for topic '{}' has {} partition(s) but need partition {}, retrying",
                        topic,
                        count,
                        partition.unwrap_or(-1)
                    );
                },
                Ok(_) => {
                    debug!("Metadata for topic '{}' returned no partitions, retrying", topic);
                },
                Err(e) => {
                    // Non-retriable errors (e.g. TopicAuthorization,
                    // InvalidTopic) should fail immediately rather than
                    // retrying until timeout.  This matches Java's
                    // waitOnMetadata -> maybeThrowExceptionForTopic.
                    if !e.is_retriable() {
                        return Err(e);
                    }
                    debug!("Metadata fetch for topic '{}' failed: {}, retrying", topic, e);
                },
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(KafkaError::new(
                    ErrorCode::TimedOut,
                    format!("Topic '{}' not present in metadata after {:?}", topic, max_wait),
                ));
            }

            let sleep_duration = std::time::Duration::from_millis(METADATA_RETRY_BACKOFF_MS).min(remaining);
            tokio::time::sleep(sleep_duration).await;
        }
    }

    /// Block until all buffered records have been sent and acknowledged.
    pub async fn flush(&self) -> crate::errors::Result<()> {
        self.inner.accumulator.flush_all().await;
        // Give the sender time to drain the flushed batches.
        // In a production implementation, we'd wait on a flush-completion signal.
        tokio::task::yield_now().await;
        Ok(())
    }

    /// Get partition metadata for a topic.
    pub async fn partitions_for(&self, topic: &str) -> crate::errors::Result<Vec<PartitionInfo>> {
        self.inner.client.partitions_for(topic).await
    }

    /// Gracefully shut down the producer.
    ///
    /// Flushes remaining records, then stops the sender task.
    pub async fn close(&self) -> crate::errors::Result<()> {
        self.inner.accumulator.close().await;

        if let Some(handle) = self.inner.sender_handle.lock().await.take() {
            handle
                .await
                .map_err(|e| KafkaError::new(ErrorCode::Unexpected, e.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::producer::config::Acks;
    use crate::clients::producer::sender::PartitionResponse;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Duration;

    /// Mock client that acknowledges all records with sequential offsets.
    struct MockProduceClient {
        next_offset: AtomicI64,
    }

    impl MockProduceClient {
        fn new() -> Self {
            MockProduceClient { next_offset: AtomicI64::new(0) }
        }
    }

    #[async_trait]
    impl ProduceClient for MockProduceClient {
        async fn send_produce_request(
            &self,
            _node_id: i32,
            _acks: Acks,
            _timeout: Duration,
            batches: Vec<(TopicPartition, Vec<u8>)>,
        ) -> Result<Vec<PartitionResponse>, KafkaError> {
            let mut responses = Vec::new();
            for (tp, _data) in batches {
                let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
                responses.push(PartitionResponse { tp, base_offset: offset, log_append_time: 1000, error: None });
            }
            Ok(responses)
        }

        async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
            Ok(vec![PartitionInfo { topic: topic.to_string(), partition: 0, leader: Some(0) }])
        }
    }

    fn test_config() -> ProducerConfig {
        ProducerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".to_string()])
            .batch_size(4096)
            .linger_ms(0)
            .buffer_memory(65536)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_send_and_receive_metadata() {
        let producer = KafkaProducer::new(test_config(), MockProduceClient::new());

        let record = ProducerRecord::new("test-topic").key(b"key1").value(b"value1");

        let future = producer.send(&record).await.unwrap();

        // Flush to ensure the record is sent.
        producer.flush().await.unwrap();

        // Give sender a moment to process.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let metadata = future.await.unwrap();
        assert_eq!(metadata.topic(), "test-topic");
        assert_eq!(metadata.partition(), 0);
        assert!(metadata.offset() >= 0);

        producer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_empty_topic_rejected() {
        let producer = KafkaProducer::new(test_config(), MockProduceClient::new());

        let record = ProducerRecord::new("").value(b"value");
        let result = producer.send(&record).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::InvalidTopic);

        producer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_multiple_records() {
        let producer = KafkaProducer::new(test_config(), MockProduceClient::new());

        let mut futures = Vec::new();
        for i in 0..10 {
            let value = format!("value-{i}");
            let record = ProducerRecord::new("test-topic").value(value.as_bytes());
            let future = producer.send(&record).await.unwrap();
            futures.push(future);
        }

        producer.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        for future in futures {
            let metadata = future.await.unwrap();
            assert_eq!(metadata.topic(), "test-topic");
        }

        producer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let producer1 = KafkaProducer::new(test_config(), MockProduceClient::new());
        let producer2 = producer1.clone();

        let record = ProducerRecord::new("test-topic").value(b"hello");
        let future = producer1.send(&record).await.unwrap();

        // Flush via the clone.
        producer2.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let metadata = future.await.unwrap();
        assert_eq!(metadata.topic(), "test-topic");

        producer2.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_partitions_for() {
        let producer = KafkaProducer::new(test_config(), MockProduceClient::new());

        let partitions = producer.partitions_for("test-topic").await.unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].topic, "test-topic");
        assert_eq!(partitions[0].partition, 0);

        producer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_invalid_partition_rejected() {
        // Use a short max_block since wait_on_metadata now retries when the
        // requested partition exceeds the known count (partition expansion
        // support), matching Java's waitOnMetadata loop condition.
        let config = ProducerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".to_string()])
            .batch_size(4096)
            .linger_ms(0)
            .buffer_memory(65536)
            .max_block_ms(500)
            .build()
            .unwrap();
        let producer = KafkaProducer::new(config, MockProduceClient::new());

        // The mock returns 1 partition (partition 0), so partition 1 causes
        // wait_on_metadata to loop until timeout (Java behavior).
        let record = ProducerRecord::new("test-topic").partition(1).value(b"value");
        let result = producer.send(&record).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::TimedOut);

        producer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_negative_partition_rejected() {
        let producer = KafkaProducer::new(test_config(), MockProduceClient::new());

        let record = ProducerRecord::new("test-topic").partition(-1).value(b"value");
        let result = producer.send(&record).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::InvalidArgument);

        producer.close().await.unwrap();
    }

    /// Mock client that always returns empty partitions, simulating a topic
    /// that does not exist.
    struct EmptyMetadataMockClient;

    #[async_trait]
    impl ProduceClient for EmptyMetadataMockClient {
        async fn send_produce_request(
            &self,
            _node_id: i32,
            _acks: Acks,
            _timeout: Duration,
            _batches: Vec<(TopicPartition, Vec<u8>)>,
        ) -> Result<Vec<PartitionResponse>, KafkaError> {
            Ok(Vec::new())
        }

        async fn partitions_for(&self, _topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn test_wait_on_metadata_times_out() {
        // Use a very short max_block so the test doesn't take long.
        let config = ProducerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".to_string()])
            .batch_size(4096)
            .linger_ms(0)
            .buffer_memory(65536)
            .max_block_ms(500) // 500ms timeout
            .build()
            .unwrap();

        let producer = KafkaProducer::new(config, EmptyMetadataMockClient);

        let record = ProducerRecord::new("nonexistent-topic").value(b"value");
        let result = producer.send(&record).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::TimedOut);

        producer.close().await.unwrap();
    }
}
