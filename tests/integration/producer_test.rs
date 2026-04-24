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
//!
//! When the `multilanguage-tests` feature is enabled, each test is
//! instantiated three times via [`multilanguage_test!`] — once per
//! backend (rust / python / c). Test bodies are generic over
//! [`ProducerBackendFactory`] so the same scenario exercises the native
//! Rust producer, the Python binding, and the C binding through their
//! gRPC server containers.
//!
//! See `design/history/MILESTONE-6/DESIGN-multilanguage-tests.md`.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerRecord;

use crate::common::backend_factory::ProducerBackendFactory;
use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a baseline producer config with sensible test defaults.
fn make_config(bootstrap_servers: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("client.id".to_string(), "integration-test-producer".to_string()),
        // Use acks=all for reliability in integration tests.
        ("acks".to_string(), "all".to_string()),
        // Use a short max_block_ms so tests don't hang.
        ("max.block.ms".to_string(), "30000".to_string()),
        // Short linger to avoid waiting.
        ("linger.ms".to_string(), "0".to_string()),
    ])
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// Test bodies — each is generic over ProducerBackendFactory
// ---------------------------------------------------------------------------

/// Test: Create a producer, send a single record with key and value, verify
/// the future completes successfully with valid RecordMetadata.
///
/// Translated from `ProducerCompressionTest` — basic produce path.
async fn produce_single_record_inner<F: ProducerBackendFactory>(factory: &F) {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("single_record");
    let producer = factory
        .create(make_config(ctx.bootstrap_servers()))
        .await
        .expect("Failed to create producer");

    let record = ProducerRecord::with_key(topic.clone(), Some(b("test-key")), Some(b("test-value")));
    let future = producer.send(record).await.expect("send should succeed");

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

    producer.close().await.expect("close should succeed");
}

/// Test: Send records with a specific key, verify they go to the same
/// partition.
///
/// Key-based partitioning should be deterministic: all records with the
/// same key must go to the same partition.
async fn produce_with_key_inner<F: ProducerBackendFactory>(factory: &F) {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("with_key");
    let producer = factory
        .create(make_config(ctx.bootstrap_servers()))
        .await
        .expect("Failed to create producer");

    let key = b("deterministic-key");
    let mut partitions = Vec::new();

    for i in 0..5 {
        let record = ProducerRecord::with_key(topic.clone(), Some(key.clone()), Some(b(&format!("value-{i}"))));
        let future = producer.send(record).await.expect("send should succeed");
        let metadata = future
            .get_timeout(Duration::from_secs(30))
            .await
            .expect("produce should succeed");
        partitions.push(metadata.partition());
    }

    let first_partition = partitions[0];
    for (i, &p) in partitions.iter().enumerate() {
        assert_eq!(
            p, first_partition,
            "Record {i} went to partition {p} but expected {first_partition} (same key should mean same partition)"
        );
    }

    producer.close().await.expect("close should succeed");
}

/// Test: Send multiple records to the same partition, verify offsets are
/// sequential.
async fn produce_multiple_records_ordering_inner<F: ProducerBackendFactory>(factory: &F) {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("ordering");
    let producer = factory
        .create(make_config(ctx.bootstrap_servers()))
        .await
        .expect("Failed to create producer");

    let mut offsets = Vec::new();
    for i in 0..5 {
        let record = ProducerRecord::with_partition(
            topic.clone(),
            Some(0),
            Some(b(&format!("key-{i}"))),
            Some(b(&format!("value-{i}"))),
        )
        .expect("record creation should succeed");
        let future = producer.send(record).await.expect("send should succeed");
        let metadata = future
            .get_timeout(Duration::from_secs(30))
            .await
            .expect("produce should succeed");
        offsets.push(metadata.offset());
    }

    for i in 1..offsets.len() {
        assert_eq!(
            offsets[i],
            offsets[i - 1] + 1,
            "Offset {i} should be sequential: expected {}, got {}",
            offsets[i - 1] + 1,
            offsets[i]
        );
    }

    producer.close().await.expect("close should succeed");
}

/// Test: Verify each compression type produces successfully.
///
/// Translated from `ProducerCompressionTest.testCompressedMessages`.
async fn produce_with_compression_inner<F: ProducerBackendFactory>(factory: &F) {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let bootstrap = ctx.bootstrap_servers().to_string();

    for name in ["none", "gzip", "snappy", "lz4", "zstd"] {
        let topic = ctx.topic(&format!("compress_{name}"));

        let mut config = make_config(&bootstrap);
        config.insert("compression.type".to_string(), name.to_string());
        let producer = factory.create(config).await.expect("Failed to create producer");

        let record =
            ProducerRecord::with_key(topic.clone(), Some(b("key")), Some(b(&format!("value-compressed-with-{name}"))));
        let future = producer.send(record).await.expect("send should succeed");

        let metadata = future
            .get_timeout(Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("produce with compression '{name}' should succeed, got: {e:?}"));

        assert!(
            metadata.offset() >= 0,
            "Offset for compression '{name}' should be non-negative, got: {}",
            metadata.offset()
        );
        assert_eq!(metadata.topic(), topic, "Topic should match for compression '{name}'");

        producer.close().await.expect("close should succeed");
    }
}

/// Test: Try to produce to a topic with invalid characters, expect an error.
async fn produce_to_invalid_topic_inner<F: ProducerBackendFactory>(factory: &F) {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let producer = factory
        .create(make_config(ctx.bootstrap_servers()))
        .await
        .expect("Failed to create producer");

    let invalid_topic = "topic with spaces!@#$".to_string();
    let record = ProducerRecord::with_value(invalid_topic, Some(b("value")));

    let future = producer.send(record).await.expect("send returns Ok with a failed future");

    let result = future.get_timeout(Duration::from_secs(30)).await;
    assert!(
        result.is_err(),
        "Producing to an invalid topic should result in an error, got: {result:?}"
    );

    producer.close().await.expect("close should succeed");
}

/// Test: Send a record larger than max.request.size, expect RecordTooLarge.
async fn produce_record_too_large_inner<F: ProducerBackendFactory>(factory: &F) {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let mut config = make_config(ctx.bootstrap_servers());
    // Set a very small max request size to trigger the error
    config.insert("max.request.size".to_string(), "100".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    let large_value = vec![b'x'; 200];
    let record = ProducerRecord::with_value("too-large-topic".to_string(), Some(large_value));

    let future = producer.send(record).await.expect("send returns Ok with a failed future");

    assert!(future.is_done(), "RecordTooLarge future should be immediately done");
    let result = future.get().await;
    assert!(result.is_err(), "RecordTooLarge should return an error");
    let err = result.unwrap_err();
    assert!(
        matches!(err, confluent_kafka::common::KafkaError::RecordTooLarge(_)),
        "Expected RecordTooLarge error, got: {err:?}"
    );

    producer.close().await.expect("close should succeed");
}

/// Test: Send records, call flush(), verify all futures are complete after
/// flush returns.
async fn flush_sends_pending_records_inner<F: ProducerBackendFactory>(factory: &F) {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("flush_test");
    let producer = factory
        .create(make_config(ctx.bootstrap_servers()))
        .await
        .expect("Failed to create producer");

    let mut futures = Vec::new();
    for i in 0..5 {
        let record =
            ProducerRecord::with_key(topic.clone(), Some(b(&format!("key-{i}"))), Some(b(&format!("value-{i}"))));
        let future = producer.send(record).await.expect("send should succeed");
        futures.push(future);
    }

    producer.flush().await.expect("flush should succeed");

    for (i, future) in futures.iter().enumerate() {
        assert!(future.is_done(), "Future {i} should be done after flush");
        let metadata = future
            .get()
            .await
            .unwrap_or_else(|e| panic!("Future {i} should succeed after flush, got: {e:?}"));
        assert!(metadata.offset() >= 0, "Record {i} should have a valid offset");
    }

    producer.close().await.expect("close should succeed");
}

/// Test: Send records, call close(), verify records were delivered.
async fn close_flushes_pending_inner<F: ProducerBackendFactory>(factory: &F) {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("close_flush");
    let producer = factory
        .create(make_config(ctx.bootstrap_servers()))
        .await
        .expect("Failed to create producer");

    let mut futures = Vec::new();
    for i in 0..3 {
        let record =
            ProducerRecord::with_key(topic.clone(), Some(b(&format!("key-{i}"))), Some(b(&format!("value-{i}"))));
        let future = producer.send(record).await.expect("send should succeed");
        futures.push(future);
    }

    producer.close().await.expect("close should succeed");

    for (i, future) in futures.iter().enumerate() {
        assert!(future.is_done(), "Future {i} should be done after close");
        let metadata = future
            .get()
            .await
            .unwrap_or_else(|e| panic!("Future {i} should succeed after close, got: {e:?}"));
        assert!(metadata.offset() >= 0, "Record {i} should have a valid offset after close");
    }
}

// ---------------------------------------------------------------------------
// Test instantiations
// ---------------------------------------------------------------------------
//
// When the `multilanguage-tests` feature is enabled, each test fans out to
// rust/python/c backends via the multilanguage_test! macro. Otherwise we
// only run the rust variant — the macro/factories aren't available without
// the feature.

#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_single_record, produce_single_record_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_with_key, produce_with_key_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_multiple_records_ordering, produce_multiple_records_ordering_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_with_compression, produce_with_compression_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_to_invalid_topic, produce_to_invalid_topic_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_record_too_large, produce_record_too_large_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_flush_sends_pending_records, flush_sends_pending_records_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_close_flushes_pending, close_flushes_pending_inner);

// Fallback: with only `integration-tests`, run each test against the
// native Rust backend so existing CI behavior is preserved. The macro is
// only available under multilanguage-tests; here we instantiate manually
// using the always-available RustNativeFactory.

#[cfg(all(feature = "integration-tests", not(feature = "multilanguage-tests")))]
mod rust_only_fallback {
    use super::*;
    use crate::common::backend_factory::RustNativeFactory;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_single_record() {
        produce_single_record_inner(&RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_with_key() {
        produce_with_key_inner(&RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_multiple_records_ordering() {
        produce_multiple_records_ordering_inner(&RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_with_compression() {
        produce_with_compression_inner(&RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_to_invalid_topic() {
        produce_to_invalid_topic_inner(&RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_record_too_large() {
        produce_record_too_large_inner(&RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_flush_sends_pending_records() {
        flush_sends_pending_records_inner(&RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_close_flushes_pending() {
        close_flushes_pending_inner(&RustNativeFactory).await;
    }
}
