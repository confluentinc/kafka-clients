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

//! Integration tests for the KafkaProducer against a real Kafka 4.2.0 broker.
//!
//! Translated from:
//! - `org.apache.kafka.clients.producer.ProducerCompressionTest`
//! - `org.apache.kafka.clients.producer.ProducerFailureHandlingTest`
//!
//! These tests verify end-to-end produce functionality including:
//! - Single and multiple record production
//! - Key-based partitioning
//! - Compression types
//! - Error handling (invalid topic, record too large)
//! - Flush and close semantics

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use confluent_kafka_rust::clients::metadata_recovery_strategy::MetadataRecoveryStrategy;
use confluent_kafka_rust::clients::network_client::NetworkClient;
use confluent_kafka_rust::clients::producer::internals::buffer_pool::BufferPool;
use confluent_kafka_rust::clients::producer::internals::producer_metadata::ProducerMetadata;
use confluent_kafka_rust::clients::producer::internals::record_accumulator::{PartitionerConfig, RecordAccumulator};
use confluent_kafka_rust::clients::producer::kafka_producer::KafkaProducer;
use confluent_kafka_rust::clients::producer::producer_config::ProducerConfig;
use confluent_kafka_rust::clients::producer::producer_record::ProducerRecord;
use confluent_kafka_rust::clients::producer::producer_trait::Producer;
use confluent_kafka_rust::clients::{ApiVersions, DefaultHostResolver};
use confluent_kafka_rust::common::compress::Compression;
use confluent_kafka_rust::common::internals::ClusterResourceListeners;
use confluent_kafka_rust::common::network::plaintext_channel_builder::PlaintextChannelBuilder;
use confluent_kafka_rust::common::network::selector::{NO_IDLE_TIMEOUT_MS, Selector};
use confluent_kafka_rust::common::serialization::StringSerializer;

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

/// Default time provider using system clock.
fn default_time_provider() -> Arc<dyn Fn() -> i64 + Send + Sync> {
    Arc::new(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    })
}

/// Create a ProducerConfig with the given bootstrap servers and optional overrides.
fn make_config(bootstrap_servers: &str) -> ProducerConfig {
    ProducerConfig {
        bootstrap_servers: vec![bootstrap_servers.to_string()],
        client_id: "integration-test-producer".to_string(),
        // Use acks=all for reliability in integration tests.
        acks: -1,
        // Use a short max_block_ms so tests don't hang.
        max_block_ms: 30_000,
        // Short linger to avoid waiting.
        linger_ms: 0,
        ..Default::default()
    }
}

/// Create a fully-wired KafkaProducer connected to a real broker.
///
/// This constructs the full pipeline:
/// 1. Selector with PlaintextChannelBuilder
/// 2. ProducerMetadata bootstrapped with the broker address
/// 3. NetworkClient wired to the metadata
/// 4. RecordAccumulator for batching
/// 5. KafkaProducer with Sender background task
fn create_producer(
    bootstrap_servers: &str,
    config: &ProducerConfig,
    compression: Compression,
) -> KafkaProducer<String, String> {
    let addr: SocketAddr = bootstrap_servers
        .parse()
        .unwrap_or_else(|_| panic!("Failed to parse bootstrap address: {}", bootstrap_servers));

    let time_provider = default_time_provider();

    // Create ProducerMetadata and bootstrap it with the broker address.
    let metadata = Arc::new(ProducerMetadata::new(
        config.reconnect_backoff_ms,
        config.reconnect_backoff_max_ms,
        config.metadata_max_age_ms,
        config.metadata_max_idle_ms,
        ClusterResourceListeners::new(),
    ));
    metadata.bootstrap(vec![addr]);

    // Get the shared Metadata Arc from ProducerMetadata so the NetworkClient
    // uses the same Metadata instance. This mirrors Java's inheritance where
    // ProducerMetadata extends Metadata.
    let shared_metadata = metadata.metadata_arc();

    // Create Selector + NetworkClient
    let channel_builder = Box::new(PlaintextChannelBuilder::new(None));
    let selector = Selector::with_defaults(NO_IDLE_TIMEOUT_MS, channel_builder);
    let api_versions = Arc::new(ApiVersions::new());

    let client = NetworkClient::with_metadata(
        selector,
        shared_metadata,
        &config.client_id,
        config.max_in_flight_requests_per_connection as usize,
        config.reconnect_backoff_ms,
        config.reconnect_backoff_max_ms,
        config.send_buffer_bytes,
        config.receive_buffer_bytes,
        config.request_timeout_ms,
        config.socket_connection_setup_timeout_ms,
        config.socket_connection_setup_timeout_max_ms,
        true, // discover_broker_versions
        api_versions,
        DefaultHostResolver::new(),
        300_000, // rebootstrap_trigger_ms
        MetadataRecoveryStrategy::None,
    );

    // Create RecordAccumulator
    let buffer_pool = Arc::new(BufferPool::new(config.buffer_memory, config.batch_size as usize));
    let accumulator = Arc::new(RecordAccumulator::new(
        config.batch_size,
        compression,
        config.linger_ms as i32,
        config.retry_backoff_ms,
        config.retry_backoff_max_ms,
        config.delivery_timeout_ms,
        PartitionerConfig {
            enable_adaptive_partitioning: config.partitioner_adaptive_partitioning_enable,
            partition_availability_timeout_ms: config.partitioner_availability_timeout_ms,
        },
        buffer_pool,
    ));

    KafkaProducer::with_client(
        config,
        Box::new(StringSerializer),
        Box::new(StringSerializer),
        metadata,
        accumulator,
        client,
        time_provider,
    )
}

/// Test: Create a KafkaProducer, send a single record with key and value,
/// verify the future completes successfully with valid RecordMetadata.
///
/// Translated from `ProducerCompressionTest` — basic produce path.
#[tokio::test(flavor = "multi_thread")]
async fn test_produce_single_record() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("single_record");
    let config = make_config(ctx.bootstrap_servers());

    let producer = create_producer(ctx.bootstrap_servers(), &config, Compression::none());

    let record = ProducerRecord::with_key(topic.clone(), Some("test-key".to_string()), Some("test-value".to_string()));
    let future = producer.send(record).expect("send should succeed");

    // Wait for the record to be acknowledged
    let metadata = future
        .get_timeout(Duration::from_secs(30))
        .await
        .expect("produce should succeed");

    assert!(
        metadata.offset() >= 0,
        "Offset should be non-negative, got: {}",
        metadata.offset()
    );
    assert_eq!(metadata.topic(), topic, "Topic should match");
    assert!(metadata.partition() >= 0, "Partition should be non-negative");

    producer.close().expect("close should succeed");
}

/// Test: Send records with a specific key, verify they go to the same partition.
///
/// Key-based partitioning should be deterministic: all records with the same key
/// must go to the same partition.
///
/// Translated from partitioning semantics tested in `ProducerFailureHandlingTest`.
#[tokio::test(flavor = "multi_thread")]
async fn test_produce_with_key() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("with_key");
    let config = make_config(ctx.bootstrap_servers());

    let producer = create_producer(ctx.bootstrap_servers(), &config, Compression::none());

    let key = "deterministic-key".to_string();
    let mut partitions = Vec::new();

    for i in 0..5 {
        let record = ProducerRecord::with_key(topic.clone(), Some(key.clone()), Some(format!("value-{}", i)));
        let future = producer.send(record).expect("send should succeed");
        let metadata = future
            .get_timeout(Duration::from_secs(30))
            .await
            .expect("produce should succeed");
        partitions.push(metadata.partition());
    }

    // All records with the same key should go to the same partition
    let first_partition = partitions[0];
    for (i, &p) in partitions.iter().enumerate() {
        assert_eq!(
            p, first_partition,
            "Record {} went to partition {} but expected {} (same key should mean same partition)",
            i, p, first_partition
        );
    }

    producer.close().expect("close should succeed");
}

/// Test: Send multiple records to the same partition, verify offsets are sequential.
///
/// Translated from ordering semantics in `ProducerFailureHandlingTest`.
#[tokio::test(flavor = "multi_thread")]
async fn test_produce_multiple_records_ordering() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("ordering");
    let config = make_config(ctx.bootstrap_servers());

    let producer = create_producer(ctx.bootstrap_servers(), &config, Compression::none());

    // Send to an explicit partition so we can verify ordering
    let mut offsets = Vec::new();
    for i in 0..5 {
        let record = ProducerRecord::with_partition(
            topic.clone(),
            Some(0),
            Some(format!("key-{}", i)),
            Some(format!("value-{}", i)),
        )
        .expect("record creation should succeed");
        let future = producer.send(record).expect("send should succeed");
        let metadata = future
            .get_timeout(Duration::from_secs(30))
            .await
            .expect("produce should succeed");
        offsets.push(metadata.offset());
    }

    // Verify offsets are sequential (each offset is previous + 1)
    for i in 1..offsets.len() {
        assert_eq!(
            offsets[i],
            offsets[i - 1] + 1,
            "Offset {} should be sequential: expected {}, got {}",
            i,
            offsets[i - 1] + 1,
            offsets[i]
        );
    }

    producer.close().expect("close should succeed");
}

/// Test: Verify each compression type produces successfully.
///
/// Translated from `ProducerCompressionTest.testCompressedMessages`.
#[tokio::test(flavor = "multi_thread")]
async fn test_produce_with_compression() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let config = make_config(ctx.bootstrap_servers());

    let compressions: Vec<(&str, Compression)> = vec![
        ("none", Compression::none()),
        ("gzip", Compression::gzip()),
        ("snappy", Compression::snappy()),
        ("lz4", Compression::lz4()),
        ("zstd", Compression::zstd()),
    ];

    for (name, compression) in compressions {
        let topic = ctx.topic(&format!("compress_{}", name));

        let producer = create_producer(ctx.bootstrap_servers(), &config, compression);

        let record = ProducerRecord::with_key(
            topic.clone(),
            Some("key".to_string()),
            Some(format!("value-compressed-with-{}", name)),
        );
        let future = producer.send(record).expect("send should succeed");

        let metadata = future
            .get_timeout(Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("produce with compression '{}' should succeed, got: {:?}", name, e));

        assert!(
            metadata.offset() >= 0,
            "Offset for compression '{}' should be non-negative, got: {}",
            name,
            metadata.offset()
        );
        assert_eq!(metadata.topic(), topic, "Topic should match for compression '{}'", name);

        producer.close().expect("close should succeed");
    }
}

/// Test: Try to produce to a topic with invalid characters, expect an error.
///
/// Translated from `ProducerFailureHandlingTest.testSendToInvalidTopic`.
///
/// Kafka rejects topic names with invalid characters. The producer should
/// propagate the broker's InvalidTopicException as a KafkaError.
#[tokio::test(flavor = "multi_thread")]
async fn test_produce_to_invalid_topic() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let config = make_config(ctx.bootstrap_servers());

    let producer = create_producer(ctx.bootstrap_servers(), &config, Compression::none());

    // Topic names with spaces or special characters are invalid in Kafka
    let invalid_topic = "topic with spaces!@#$".to_string();
    let record = ProducerRecord::with_value(invalid_topic, Some("value".to_string()));

    let future = producer.send(record).expect("send returns Ok with a failed future");

    // The future should complete with an error (InvalidTopic or similar)
    let result = future.get_timeout(Duration::from_secs(30)).await;
    assert!(
        result.is_err(),
        "Producing to an invalid topic should result in an error, got: {:?}",
        result
    );

    producer.close().expect("close should succeed");
}

/// Test: Send a record larger than max.request.size, expect RecordTooLarge error.
///
/// Translated from `ProducerFailureHandlingTest.testSendMessageTooLargeWithAck`.
#[tokio::test(flavor = "multi_thread")]
async fn test_produce_record_too_large() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let mut config = make_config(ctx.bootstrap_servers());
    // Set a very small max request size to trigger the error
    config.max_request_size = 100;

    let producer = create_producer(ctx.bootstrap_servers(), &config, Compression::none());

    // Create a record larger than 100 bytes
    let large_value = "x".repeat(200);
    let record = ProducerRecord::with_value("too-large-topic".to_string(), Some(large_value));

    let future = producer.send(record).expect("send returns Ok with a failed future");

    // The future should be immediately done with a RecordTooLarge error
    assert!(future.is_done(), "RecordTooLarge future should be immediately done");
    let result = future.get().await;
    assert!(result.is_err(), "RecordTooLarge should return an error");
    let err = result.unwrap_err();
    assert!(
        matches!(err, confluent_kafka_rust::common::kafka_error::KafkaError::RecordTooLarge(_)),
        "Expected RecordTooLarge error, got: {:?}",
        err
    );

    producer.close().expect("close should succeed");
}

/// Test: Send records, call flush(), verify all futures are complete after flush returns.
///
/// Translated from `KafkaProducerTest.testFlushCompleteSendOfInflightBatches`.
#[tokio::test(flavor = "multi_thread")]
async fn test_flush_sends_pending_records() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("flush_test");
    let config = make_config(ctx.bootstrap_servers());

    let producer = create_producer(ctx.bootstrap_servers(), &config, Compression::none());

    // Send multiple records without awaiting them
    let mut futures = Vec::new();
    for i in 0..5 {
        let record = ProducerRecord::with_key(topic.clone(), Some(format!("key-{}", i)), Some(format!("value-{}", i)));
        let future = producer.send(record).expect("send should succeed");
        futures.push(future);
    }

    // Flush to ensure all records are sent
    producer.flush().expect("flush should succeed");

    // After flush, all futures should be complete
    for (i, future) in futures.iter().enumerate() {
        assert!(future.is_done(), "Future {} should be done after flush", i);
        let metadata = future
            .get()
            .await
            .unwrap_or_else(|e| panic!("Future {} should succeed after flush, got: {:?}", i, e));
        assert!(metadata.offset() >= 0, "Record {} should have a valid offset", i);
    }

    producer.close().expect("close should succeed");
}

/// Test: Send records, call close(), verify records were delivered.
///
/// Translated from `KafkaProducerTest.testCloseCompleteSendOfInflightBatches`.
#[tokio::test(flavor = "multi_thread")]
async fn test_close_flushes_pending() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("close_flush");
    let config = make_config(ctx.bootstrap_servers());

    let producer = create_producer(ctx.bootstrap_servers(), &config, Compression::none());

    // Send records
    let mut futures = Vec::new();
    for i in 0..3 {
        let record = ProducerRecord::with_key(topic.clone(), Some(format!("key-{}", i)), Some(format!("value-{}", i)));
        let future = producer.send(record).expect("send should succeed");
        futures.push(future);
    }

    // Close should flush pending records
    producer.close().expect("close should succeed");

    // After close, all futures should be complete
    for (i, future) in futures.iter().enumerate() {
        assert!(future.is_done(), "Future {} should be done after close", i);
        let metadata = future
            .get()
            .await
            .unwrap_or_else(|e| panic!("Future {} should succeed after close, got: {:?}", i, e));
        assert!(metadata.offset() >= 0, "Record {} should have a valid offset after close", i);
    }
}
