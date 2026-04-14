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

use crate::clients::kafka_client::KafkaClient;
use crate::clients::producer::accumulator::RecordAccumulator;
use crate::clients::producer::batch::SendFuture;
use crate::clients::producer::config::ProducerConfig;
use crate::clients::producer::producer_metadata::ProducerMetadata;
use crate::clients::producer::record::ProducerRecord;
use crate::clients::producer::sender::Sender;
use crate::common::TopicPartition;
use crate::errors::{ErrorCode, KafkaError};
use log::debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Returns the current time in milliseconds since the Unix epoch.
fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

struct ProducerInner {
    #[allow(dead_code)]
    config: Arc<ProducerConfig>,
    accumulator: Arc<RecordAccumulator>,
    metadata: Arc<ProducerMetadata>,
    sender_handle: Mutex<Option<JoinHandle<()>>>,
    /// Flag shared with the Sender to signal shutdown.
    running: Arc<AtomicBool>,
}

/// An async Kafka producer.
///
/// Thread-safe: cloning gives a handle to the same underlying producer.
/// The background sender task is spawned on construction.
///
/// Corresponds to `org.apache.kafka.clients.producer.KafkaProducer`.
pub struct KafkaProducer {
    inner: Arc<ProducerInner>,
}

impl Clone for KafkaProducer {
    fn clone(&self) -> Self {
        KafkaProducer { inner: Arc::clone(&self.inner) }
    }
}

impl KafkaProducer {
    /// Create a new producer with the given config, network client, and metadata.
    ///
    /// The `client` is moved into the `Sender` which owns it exclusively (no
    /// shared lock). The `metadata` is shared between the producer and sender
    /// for partition lookups and topic tracking.
    ///
    /// Spawns a background sender task immediately.
    ///
    /// Corresponds to Java's `KafkaProducer` constructor which takes a
    /// `ProducerMetadata` shared between the producer and sender.
    pub fn new<C: KafkaClient + Send + 'static>(
        config: ProducerConfig,
        client: C,
        metadata: Arc<ProducerMetadata>,
    ) -> Self {
        let config = Arc::new(config);
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));
        let running = Arc::new(AtomicBool::new(true));

        let sender = Sender::new(
            client,
            Arc::clone(&accumulator),
            Arc::clone(&metadata),
            Arc::clone(&config),
            Arc::clone(&running),
        );
        let sender_handle = tokio::spawn(sender.run());

        KafkaProducer {
            inner: Arc::new(ProducerInner {
                config,
                accumulator,
                metadata,
                sender_handle: Mutex::new(Some(sender_handle)),
                running,
            }),
        }
    }

    /// Send a record to Kafka.
    ///
    /// Matches Java's `KafkaProducer.doSend()`:
    /// 1. Validate the topic name
    /// 2. Wait for metadata for the topic (with optional partition expansion)
    /// 3. Validate the partition against the known partition count
    /// 4. Append to the record accumulator
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
    /// Integer partition, long nowMs, long maxWaitMs)`.
    ///
    /// Uses `ProducerMetadata` to track the topic and wait for metadata
    /// version changes. The background sender drives `client.poll()` which
    /// triggers `DefaultMetadataUpdater` to send metadata requests.
    ///
    /// When `partition` is `Some(p)`, the method also waits until the
    /// partition count grows to include `p`, supporting online partition
    /// expansion.
    ///
    /// Returns the partition count for the topic.
    async fn wait_on_metadata(&self, topic: &str, partition: Option<i32>) -> crate::errors::Result<i32> {
        let now = current_time_ms();
        self.inner.metadata.add(topic, now);

        let max_wait = self.inner.config.max_block();
        let max_wait_ms = max_wait.as_millis() as i64;
        let start = tokio::time::Instant::now();

        loop {
            // Check if the topic partition count is already known and sufficient.
            // This check before await_update matches Java's pattern where
            // partitionCount is checked after awaitUpdate returns, but also
            // handles the case where metadata was pre-populated before send().
            if let Some(count) = self.partition_count_from_metadata(topic) {
                let partition_satisfied = match partition {
                    Some(p) => p < count,
                    None => true,
                };
                if partition_satisfied {
                    return Ok(count);
                }
                debug!(
                    "Metadata for topic '{}' has {} partition(s) but need partition {}, retrying",
                    topic,
                    count,
                    partition.unwrap_or(-1)
                );
            } else {
                debug!("Metadata for topic '{}' not yet available, retrying", topic);
            }

            // Request a metadata update for this topic.
            let version = self.inner.metadata.request_update_for_topic(topic);

            // Check remaining time.
            let elapsed = start.elapsed().as_millis() as i64;
            let remaining = max_wait_ms - elapsed;
            if remaining <= 0 {
                let msg = format!("Topic '{}' not present in metadata after {:?}", topic, max_wait);
                return Err(KafkaError::new(ErrorCode::TimedOut, msg));
            }

            // Wait for the metadata version to advance.
            self.inner.metadata.await_update(version, remaining).await?;
        }
    }

    /// Look up the partition count for a topic from the shared Metadata cache.
    fn partition_count_from_metadata(&self, topic: &str) -> Option<i32> {
        let cluster = self.inner.metadata.fetch();
        cluster.partition_count_for_topic(topic).map(|c| c as i32)
    }

    /// Block until all buffered records have been sent and acknowledged.
    pub async fn flush(&self) -> crate::errors::Result<()> {
        self.inner.accumulator.flush_all().await;
        // Give the sender time to drain the flushed batches.
        tokio::task::yield_now().await;
        Ok(())
    }

    /// Returns a reference to the shared `ProducerMetadata`.
    ///
    /// Used by callers who need direct access to the producer's metadata
    /// (e.g. for integration tests or manual topic tracking).
    pub fn metadata(&self) -> &Arc<ProducerMetadata> {
        &self.inner.metadata
    }

    /// Gracefully shut down the producer.
    ///
    /// Flushes remaining records, then stops the sender task.
    pub async fn close(&self) -> crate::errors::Result<()> {
        // Close the accumulator (flushes current batches and prevents new appends).
        self.inner.accumulator.close().await;

        // Signal the sender to stop after draining.
        self.inner.running.store(false, Ordering::Release);

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
    use crate::clients::client_response::ClientResponse;
    use crate::clients::kafka_client::KafkaClient;
    use crate::clients::least_loaded_node::LeastLoadedNode;
    use crate::clients::{ClientRequest, RequestCompletionHandler};
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::node::Node;
    use crate::common::protocol::Errors;
    use crate::common::requests::abstract_response::ConcreteResponse;
    use crate::common::requests::produce_response::ProduceResponse;
    use crate::common::requests::{RequestBuilder, RequestHeader};
    use crate::produce_response_data::{PartitionProduceResponse, ProduceResponseData, TopicProduceResponse};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
    use std::time::Duration;

    /// A mock `KafkaClient` that auto-completes produce requests with success.
    ///
    /// This replaces the old `MockProduceClient` — all producer tests now mock
    /// at the `KafkaClient` level, matching Java's test infrastructure.
    struct MockKafkaClient {
        /// Pending requests that have been sent but not yet polled.
        pending_requests: tokio::sync::Mutex<Vec<ClientRequest>>,
        /// Monotonically increasing correlation ID.
        next_correlation_id: AtomicI32,
        /// Next offset to assign in produce responses.
        next_offset: AtomicI64,
        /// Whether the client is active.
        active: AtomicBool,
    }

    impl MockKafkaClient {
        fn new() -> Self {
            MockKafkaClient {
                pending_requests: tokio::sync::Mutex::new(Vec::new()),
                next_correlation_id: AtomicI32::new(0),
                next_offset: AtomicI64::new(0),
                active: AtomicBool::new(true),
            }
        }

        /// Build a successful ProduceResponse for the given request.
        fn build_produce_response(&self, _request: &ClientRequest) -> ConcreteResponse {
            let mut response_data = ProduceResponseData::new();
            let mut topic_response = TopicProduceResponse::new();
            topic_response.set_name("test-topic".to_string());
            let mut partition_response = PartitionProduceResponse::new();
            partition_response.set_index(0);
            partition_response.set_base_offset(self.next_offset.fetch_add(1, Ordering::SeqCst));
            partition_response.set_log_append_time_ms(1000);
            partition_response.set_error_code(Errors::None.code());
            topic_response.set_partition_responses(vec![partition_response]);
            response_data.set_responses(vec![topic_response]);
            ConcreteResponse::Produce(ProduceResponse::new(response_data))
        }

        fn build_response_header(&self) -> RequestHeader {
            RequestHeader::new(
                &crate::common::protocol::ApiKeys::PRODUCE,
                0,
                "test-client",
                self.next_correlation_id.fetch_add(1, Ordering::SeqCst),
            )
            .expect("Failed to build request header")
        }
    }

    #[async_trait]
    impl KafkaClient for MockKafkaClient {
        fn is_ready(&self, _node: &Node, _now: i64) -> bool {
            true
        }

        async fn ready(&mut self, _node: &Node, _now: i64) -> bool {
            true
        }

        fn connection_delay(&self, _node: &Node, _now: i64) -> i64 {
            0
        }

        fn poll_delay_ms(&self, _node: &Node, _now: i64) -> i64 {
            0
        }

        fn connection_failed(&self, _node: &Node) -> bool {
            false
        }

        fn authentication_error(&self, _node: &Node) -> Option<String> {
            None
        }

        fn send(&mut self, request: ClientRequest, _now: i64) {
            if let Ok(mut pending) = self.pending_requests.try_lock() {
                pending.push(request);
            }
        }

        async fn poll(&mut self, _timeout: i64, _now: i64) -> Vec<ClientResponse> {
            let mut pending = self.pending_requests.lock().await;
            let requests: Vec<ClientRequest> = pending.drain(..).collect();
            drop(pending);

            let mut responses = Vec::new();
            for mut request in requests {
                let response_body = self.build_produce_response(&request);
                let header = self.build_response_header();
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                let mut client_response = ClientResponse::new(
                    header,
                    request.take_callback(),
                    request.destination(),
                    now_ms,
                    now_ms,
                    false,
                    None,
                    None,
                    Some(response_body),
                );
                client_response.on_complete();
                responses.push(client_response);
            }
            responses
        }

        async fn disconnect(&mut self, _node_id: &str) {}

        async fn close_connection(&mut self, _node_id: &str) {}

        fn least_loaded_node(&self, _now: i64) -> LeastLoadedNode {
            LeastLoadedNode::new(Some(Node::new(0, "localhost".to_string(), 9092)), true)
        }

        fn in_flight_request_count(&self) -> i32 {
            0
        }

        fn has_in_flight_requests(&self) -> bool {
            false
        }

        fn in_flight_request_count_for_node(&self, _node_id: &str) -> usize {
            0
        }

        fn has_in_flight_requests_for_node(&self, _node_id: &str) -> bool {
            false
        }

        fn has_ready_nodes(&self, _now: i64) -> bool {
            true
        }

        fn wakeup(&self) {}

        fn new_client_request(
            &mut self,
            node_id: &str,
            request_builder: Box<dyn RequestBuilder + Send>,
            created_time_ms: i64,
            expect_response: bool,
        ) -> ClientRequest {
            ClientRequest::new(
                node_id,
                request_builder,
                self.next_correlation_id.fetch_add(1, Ordering::SeqCst),
                "test-client",
                created_time_ms,
                expect_response,
                0,
                None,
            )
        }

        fn new_client_request_with_timeout(
            &mut self,
            node_id: &str,
            request_builder: Box<dyn RequestBuilder + Send>,
            created_time_ms: i64,
            expect_response: bool,
            request_timeout_ms: i32,
            callback: Option<RequestCompletionHandler>,
        ) -> ClientRequest {
            ClientRequest::new(
                node_id,
                request_builder,
                self.next_correlation_id.fetch_add(1, Ordering::SeqCst),
                "test-client",
                created_time_ms,
                expect_response,
                request_timeout_ms,
                callback,
            )
        }

        fn initiate_close(&self) {
            self.active.store(false, Ordering::Release);
        }

        fn active(&self) -> bool {
            self.active.load(Ordering::Acquire)
        }

        async fn close(&mut self) {
            self.active.store(false, Ordering::Release);
        }
    }

    /// Create a test ProducerMetadata instance pre-populated with topics.
    fn test_metadata_with(topic_partitions: &HashMap<String, i32>) -> Arc<ProducerMetadata> {
        let metadata = Arc::new(ProducerMetadata::new(50, 50, 5000, 60_000, ClusterResourceListeners::new()));
        let now = current_time_ms();
        for topic in topic_partitions.keys() {
            metadata.add(topic, now);
        }
        let metadata_response = crate::common::requests::request_test_utils::metadata_update_with(1, topic_partitions);
        metadata.update_with_current_request_version(&metadata_response, false, now);
        metadata
    }

    /// Create a test ProducerMetadata instance with a single topic having the
    /// given number of partitions.
    fn test_metadata(topic: &str, num_partitions: i32) -> Arc<ProducerMetadata> {
        let mut topics = HashMap::new();
        topics.insert(topic.to_string(), num_partitions);
        test_metadata_with(&topics)
    }

    /// Create an empty ProducerMetadata instance (no topics known).
    fn empty_metadata() -> Arc<ProducerMetadata> {
        Arc::new(ProducerMetadata::new(50, 50, 5000, 60_000, ClusterResourceListeners::new()))
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
        let metadata = test_metadata("test-topic", 1);
        let producer = KafkaProducer::new(test_config(), MockKafkaClient::new(), metadata);

        let record = ProducerRecord::new("test-topic").key(b"key1").value(b"value1");

        let future = producer.send(&record).await.unwrap();

        // Flush to ensure the record is sent.
        producer.flush().await.unwrap();

        // Give sender a moment to process.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let result = future.await.unwrap();
        assert_eq!(result.topic(), "test-topic");
        assert_eq!(result.partition(), 0);
        assert!(result.offset() >= 0);

        producer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_empty_topic_rejected() {
        let metadata = test_metadata("test-topic", 1);
        let producer = KafkaProducer::new(test_config(), MockKafkaClient::new(), metadata);

        let record = ProducerRecord::new("").value(b"value");
        let result = producer.send(&record).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::InvalidTopic);

        producer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_multiple_records() {
        let metadata = test_metadata("test-topic", 1);
        let producer = KafkaProducer::new(test_config(), MockKafkaClient::new(), metadata);

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
            let result = future.await.unwrap();
            assert_eq!(result.topic(), "test-topic");
        }

        producer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let metadata = test_metadata("test-topic", 1);
        let producer1 = KafkaProducer::new(test_config(), MockKafkaClient::new(), metadata);
        let producer2 = producer1.clone();

        let record = ProducerRecord::new("test-topic").value(b"hello");
        let future = producer1.send(&record).await.unwrap();

        // Flush via the clone.
        producer2.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let result = future.await.unwrap();
        assert_eq!(result.topic(), "test-topic");

        producer2.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_invalid_partition_rejected() {
        let config = ProducerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".to_string()])
            .batch_size(4096)
            .linger_ms(0)
            .buffer_memory(65536)
            .max_block_ms(500)
            .build()
            .unwrap();
        let metadata = test_metadata("test-topic", 1);
        let producer = KafkaProducer::new(config, MockKafkaClient::new(), metadata);

        let record = ProducerRecord::new("test-topic").partition(1).value(b"value");
        let result = producer.send(&record).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::TimedOut);

        producer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_negative_partition_rejected() {
        let metadata = test_metadata("test-topic", 1);
        let producer = KafkaProducer::new(test_config(), MockKafkaClient::new(), metadata);

        let record = ProducerRecord::new("test-topic").partition(-1).value(b"value");
        let result = producer.send(&record).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::InvalidArgument);

        producer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_on_metadata_times_out() {
        let config = ProducerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".to_string()])
            .batch_size(4096)
            .linger_ms(0)
            .buffer_memory(65536)
            .max_block_ms(500)
            .build()
            .unwrap();

        let metadata = empty_metadata();
        let producer = KafkaProducer::new(config, MockKafkaClient::new(), metadata);

        let record = ProducerRecord::new("nonexistent-topic").value(b"value");
        let result = producer.send(&record).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::TimedOut);

        producer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_close_when_waiting_for_metadata_update() {
        let config = ProducerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".to_string()])
            .batch_size(4096)
            .linger_ms(0)
            .buffer_memory(65536)
            .max_block_ms(500)
            .build()
            .unwrap();

        let metadata = empty_metadata();
        let producer = KafkaProducer::new(config, MockKafkaClient::new(), metadata);
        let producer_for_close = producer.clone();

        let send_handle = tokio::spawn(async move {
            let record = ProducerRecord::new("test-topic").value(b"value");
            producer.send(&record).await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        producer_for_close.close().await.unwrap();

        let result = send_handle.await.expect("send task should not panic");
        assert!(
            result.is_err(),
            "send should fail when producer is closed or metadata times out"
        );
        assert_eq!(result.unwrap_err().code(), ErrorCode::TimedOut);
    }
}
