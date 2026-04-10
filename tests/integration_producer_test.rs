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
//! `KafkaProducer<KafkaProduceClient>` pipeline, verifying that records
//! are accepted and valid offsets are returned.
//!
//! These tests require Docker to be running and are feature-gated behind
//! `integration-tests`.

#![cfg(feature = "integration-tests")]

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use confluent_kafka_rust::clients::producer::config::Acks;
use confluent_kafka_rust::clients::producer::{KafkaProduceClient, KafkaProducer, ProducerConfig, ProducerRecord};

use common::cluster_config::ClusterConfig;
use common::test_context::TestContext;

/// Parse bootstrap server address string into a `SocketAddr`.
fn parse_bootstrap_addr(bootstrap_servers: &str) -> SocketAddr {
    bootstrap_servers
        .parse::<SocketAddr>()
        .unwrap_or_else(|_| panic!("Failed to parse bootstrap servers address: {bootstrap_servers}"))
}

/// Create a `KafkaProduceClient` connected to the test cluster.
fn create_produce_client(bootstrap_servers: &str) -> KafkaProduceClient {
    let addr = parse_bootstrap_addr(bootstrap_servers);
    KafkaProduceClient::new(addr, "integration-producer-test")
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
/// `KafkaProducer<KafkaProduceClient>` pipeline.
///
/// Verifies that:
/// - The record is accepted without error
/// - A valid (non-negative) offset is returned
/// - The topic and partition in the metadata are correct
#[tokio::test]
async fn test_produce_single_record() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("single_record");

    let client = create_produce_client(ctx.bootstrap_servers());
    let config = test_producer_config(ctx.bootstrap_servers());
    let producer = KafkaProducer::new(config, client);

    let record = ProducerRecord::new(&topic).value(b"hello-kafka");
    let future = producer.send(&record).await.expect("send should succeed");

    producer.flush().await.expect("flush should succeed");

    // Give the sender task time to process the batch.
    tokio::time::sleep(Duration::from_millis(200)).await;

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

    let client = create_produce_client(ctx.bootstrap_servers());
    let config = test_producer_config(ctx.bootstrap_servers());
    let producer = KafkaProducer::new(config, client);

    let num_records = 5;
    let mut futures = Vec::new();

    for i in 0..num_records {
        let value = format!("value-{i}");
        let record = ProducerRecord::new(&topic).value(value.as_bytes());
        let future = producer.send(&record).await.expect("send should succeed");
        futures.push(future);
    }

    producer.flush().await.expect("flush should succeed");
    tokio::time::sleep(Duration::from_millis(500)).await;

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

    let client = create_produce_client(ctx.bootstrap_servers());
    let config = test_producer_config(ctx.bootstrap_servers());
    let producer = KafkaProducer::new(config, client);

    let record = ProducerRecord::new(&topic)
        .key(b"my-key")
        .value(b"my-value")
        .header("trace-id", Some(b"abc123"))
        .header("empty-header", None);

    let future = producer.send(&record).await.expect("send should succeed");

    producer.flush().await.expect("flush should succeed");
    tokio::time::sleep(Duration::from_millis(200)).await;

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
