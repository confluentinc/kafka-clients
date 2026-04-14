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

//! Integration tests for the end-to-end producer pipeline.
//!
//! Tests produce records to a real Kafka broker via the full
//! `KafkaProducer` + `NetworkClient` pipeline, verifying that records
//! are accepted and valid offsets are returned.
//!
//! These tests require Docker to be running and are feature-gated behind
//! `integration-tests`.

#![cfg(feature = "integration-tests")]

mod common;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use confluent_kafka_rust::clients::api_versions::ApiVersions;
use confluent_kafka_rust::clients::metadata::Metadata;
use confluent_kafka_rust::clients::network_client::NetworkClient;
use confluent_kafka_rust::clients::producer::config::Acks;
use confluent_kafka_rust::clients::producer::{KafkaProducer, ProducerConfig, ProducerRecord};
use confluent_kafka_rust::common::internals::ClusterResourceListeners;
use confluent_kafka_rust::common::network::plaintext_channel_builder::PlaintextChannelBuilder;
use confluent_kafka_rust::common::network::selectable::USE_DEFAULT_BUFFER_SIZE;
use confluent_kafka_rust::common::network::selector::{NO_IDLE_TIMEOUT_MS, Selector};
use confluent_kafka_rust::errors::ErrorCode;

use common::cluster_config::ClusterConfig;
use common::test_context::TestContext;

/// Parse bootstrap server address string into a `SocketAddr`.
fn parse_bootstrap_addr(bootstrap_servers: &str) -> SocketAddr {
    bootstrap_servers
        .parse::<SocketAddr>()
        .unwrap_or_else(|_| panic!("Failed to parse bootstrap servers address: {bootstrap_servers}"))
}

/// Create a `NetworkClient` and shared `Metadata` connected to the test cluster.
fn create_network_client(bootstrap_servers: &str) -> (NetworkClient, Arc<Metadata>) {
    let addr = parse_bootstrap_addr(bootstrap_servers);

    let metadata = Arc::new(Metadata::new(
        50,      // refresh_backoff_ms
        5000,    // refresh_backoff_max_ms
        300_000, // metadata_expire_ms
        ClusterResourceListeners::new(),
    ));
    metadata.bootstrap(vec![addr]);

    let channel_builder = Box::new(PlaintextChannelBuilder::new(None));
    let selector = Selector::new(USE_DEFAULT_BUFFER_SIZE, NO_IDLE_TIMEOUT_MS, channel_builder);
    let api_versions = Arc::new(ApiVersions::new());
    let host_resolver = confluent_kafka_rust::clients::DefaultHostResolver;

    let client = NetworkClient::with_metadata(
        selector,
        Arc::clone(&metadata),
        "integration-producer-test",
        5,    // max_in_flight_requests_per_connection
        50,   // reconnect_backoff_ms
        5000, // reconnect_backoff_max_ms
        USE_DEFAULT_BUFFER_SIZE,
        USE_DEFAULT_BUFFER_SIZE,
        30_000,  // default_request_timeout_ms
        10_000,  // connection_setup_timeout_ms
        127_000, // connection_setup_timeout_max_ms
        true,    // discover_broker_versions
        api_versions,
        host_resolver,
        300_000, // rebootstrap_trigger_ms
        confluent_kafka_rust::clients::metadata_recovery_strategy::MetadataRecoveryStrategy::None,
    );

    (client, metadata)
}

/// Create a `ProducerConfig` for integration tests.
fn test_producer_config(bootstrap_servers: &str) -> ProducerConfig {
    ProducerConfig::builder()
        .bootstrap_servers(vec![bootstrap_servers.to_string()])
        .batch_size(16384)
        .linger_ms(0)
        .buffer_memory(65536)
        .acks(Acks::All)
        .request_timeout_ms(30000)
        .build()
        .unwrap()
}

/// Test: Produce a single record to a real Kafka broker using the full
/// `KafkaProducer` + `NetworkClient` pipeline.
///
/// Verifies that:
/// - The record is accepted without error
/// - A valid (non-negative) offset is returned
/// - The topic and partition in the metadata are correct
#[tokio::test]
async fn test_produce_single_record() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("single_record");

    let (client, metadata) = create_network_client(ctx.bootstrap_servers());
    let config = test_producer_config(ctx.bootstrap_servers());
    let producer = KafkaProducer::new(config, client, metadata);

    let record = ProducerRecord::new(&topic).value(b"hello-kafka");
    let future = producer.send(&record).await.expect("send should succeed");

    producer.flush().await.expect("flush should succeed");

    let metadata = future.await.expect("record should be acknowledged");
    assert_eq!(metadata.topic(), topic, "topic should match");
    assert_eq!(metadata.partition(), 0, "partition should be 0");
    assert!(
        metadata.offset() >= 0,
        "offset should be non-negative, got {}",
        metadata.offset()
    );

    producer.close().await.expect("close should succeed");
    ctx.cleanup().await;
}

/// Test: Produce multiple records and verify sequential offsets.
///
/// Sends 5 records and verifies:
/// - All records are acknowledged without error
/// - Offsets are non-negative
/// - Offsets are sequential (each offset >= previous)
#[tokio::test]
async fn test_produce_multiple_records() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("multi_record");

    let (client, metadata) = create_network_client(ctx.bootstrap_servers());
    let config = test_producer_config(ctx.bootstrap_servers());
    let producer = KafkaProducer::new(config, client, metadata);

    let num_records = 5;
    let mut futures = Vec::new();

    for i in 0..num_records {
        let value = format!("value-{i}");
        let record = ProducerRecord::new(&topic).value(value.as_bytes());
        let future = producer.send(&record).await.expect("send should succeed");
        futures.push(future);
    }

    producer.flush().await.expect("flush should succeed");

    let mut offsets = Vec::new();
    for future in futures {
        let metadata = future.await.expect("record should be acknowledged");
        assert_eq!(metadata.topic(), topic, "topic should match");
        assert_eq!(metadata.partition(), 0, "partition should be 0");
        assert!(metadata.offset() >= 0, "offset should be non-negative");
        offsets.push(metadata.offset());
    }

    // Verify offsets are monotonically non-decreasing.
    // They may not be strictly sequential if batched together (same base offset).
    for i in 1..offsets.len() {
        assert!(
            offsets[i] >= offsets[i - 1],
            "offsets should be non-decreasing: offset[{}]={} < offset[{}]={}",
            i,
            offsets[i],
            i - 1,
            offsets[i - 1]
        );
    }

    producer.close().await.expect("close should succeed");
    ctx.cleanup().await;
}

/// Test: Produce a record with key and headers, verify the offset is returned.
///
/// Verifies that:
/// - Records with keys and headers are accepted
/// - A valid offset is returned
#[tokio::test]
async fn test_produce_with_key_and_headers() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("key_headers");

    let (client, metadata) = create_network_client(ctx.bootstrap_servers());
    let config = test_producer_config(ctx.bootstrap_servers());
    let producer = KafkaProducer::new(config, client, metadata);

    let record = ProducerRecord::new(&topic)
        .key(b"my-key")
        .value(b"my-value")
        .header("trace-id", Some(b"abc123"))
        .header("empty-header", None);

    let future = producer.send(&record).await.expect("send should succeed");

    producer.flush().await.expect("flush should succeed");

    let metadata = future.await.expect("record should be acknowledged");
    assert_eq!(metadata.topic(), topic, "topic should match");
    assert_eq!(metadata.partition(), 0, "partition should be 0");
    assert!(
        metadata.offset() >= 0,
        "offset should be non-negative, got {}",
        metadata.offset()
    );

    producer.close().await.expect("close should succeed");
    ctx.cleanup().await;
}

/// Test: Produce to a non-existent topic when auto-create is disabled.
///
/// Verifies that:
/// - `send()` returns an error because `wait_on_metadata()` times out
///   (the broker returns `UNKNOWN_TOPIC_OR_PARTITION` which is retriable,
///   so `wait_on_metadata` keeps retrying until `max_block_ms` is exhausted)
/// - Uses a short `max_block_ms` so the test finishes quickly
///
/// This exercises the metadata error path from
/// `KafkaProducer.send()` -> `wait_on_metadata()` -> `fetch_partitions()`.
/// Java's `KafkaProducer.waitOnMetadata()` behaves identically: it retries
/// on retriable errors until `max.block.ms`, then throws `TimeoutException`.
#[tokio::test]
async fn test_produce_to_nonexistent_topic() {
    // Start a cluster with auto.create.topics.enable=false so that
    // producing to a non-existent topic returns an error instead of
    // silently creating the topic.
    let mut props = BTreeMap::new();
    props.insert("KAFKA_AUTO_CREATE_TOPICS_ENABLE".to_string(), "false".to_string());
    let config = ClusterConfig::with_properties(props);

    let mut ctx = TestContext::new(config).await;
    // Use a topic name that definitely does not exist.
    let topic = ctx.topic("nonexistent_topic_that_should_not_exist");

    let (client, metadata) = create_network_client(ctx.bootstrap_servers());
    // Use a short max_block_ms so the test doesn't hang for 60 seconds.
    let config = ProducerConfig::builder()
        .bootstrap_servers(vec![ctx.bootstrap_servers().to_string()])
        .batch_size(16384)
        .linger_ms(0)
        .buffer_memory(65536)
        .acks(Acks::All)
        .request_timeout_ms(30000)
        .max_block_ms(3000)
        .build()
        .unwrap();
    let producer = KafkaProducer::new(config, client, metadata);

    let record = ProducerRecord::new(&topic).value(b"should-fail");
    // send() now calls wait_on_metadata() which loops until max_block_ms.
    // Since UNKNOWN_TOPIC_OR_PARTITION is retriable, it will time out.
    let result = producer.send(&record).await;
    assert!(result.is_err(), "send to a non-existent topic should fail at wait_on_metadata");

    let err = result.unwrap_err();
    assert_eq!(
        err.code(),
        ErrorCode::TimedOut,
        "error code should be TimedOut (metadata retry exhausted), got {:?}: {}",
        err.code(),
        err
    );

    producer.close().await.expect("close should succeed");
    ctx.cleanup().await;
}
