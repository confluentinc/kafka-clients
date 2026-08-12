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

/// Pick the bootstrap address the factory's backend can actually reach.
/// gRPC backends run in containers and need the broker's CONTAINER
/// listener; native rust uses the host loopback.
fn bootstrap_for<F: ProducerBackendFactory>(factory: &F, ctx: &TestContext) -> String {
    if factory.needs_container_bootstrap() {
        ctx.container_bootstrap_servers().to_string()
    } else {
        ctx.bootstrap_servers().to_string()
    }
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// Test bodies — each is generic over ProducerBackendFactory
// ---------------------------------------------------------------------------

/// Test: Create a producer, send a single record with key and value, verify
/// the future completes successfully with valid RecordMetadata.
async fn produce_single_record_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("single_record");
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
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
async fn produce_with_key_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("with_key");
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
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
async fn produce_multiple_records_ordering_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ordering");
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
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
async fn produce_with_compression_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let bootstrap = bootstrap_for(factory, ctx);

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
async fn produce_to_invalid_topic_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
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
async fn produce_record_too_large_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let mut config = make_config(&bootstrap_for(factory, ctx));
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
        matches!(err, confluent_kafka::common::Error::RecordTooLarge(_)),
        "Expected RecordTooLarge error, got: {err:?}"
    );

    producer.close().await.expect("close should succeed");
}

/// Test: Send records, call flush(), verify all futures are complete after
/// flush returns.
async fn flush_sends_pending_records_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("flush_test");
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
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
async fn close_flushes_pending_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("close_flush");
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
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
// Java-source-derived test bodies (translated from
// kafka/clients/clients-integration-tests/.../ProducerFailureHandlingTest.java
// and kafka/core/src/test/scala/.../{Base,Plaintext}ProducerSendTest.scala —
// see design/history/MILESTONE-6/COVERAGE-ASSESSMENT.md)
// ---------------------------------------------------------------------------

/// Cluster with topic auto-creation disabled. Used by
/// `testNonExistentTopic` to prove the producer times out when a topic
/// truly never exists.
///
/// The `BTreeMap` keys are `KAFKA_*` env vars; the Kafka Docker image
/// converts them to dotted server.properties at startup.
fn no_auto_create_cluster_config() -> ClusterConfig {
    let mut props = std::collections::BTreeMap::new();
    props.insert("KAFKA_AUTO_CREATE_TOPICS_ENABLE".to_string(), "false".to_string());
    ClusterConfig::with_properties(props)
}

/// Cluster with a small `message.max.bytes` so server-side rejection of
/// oversized records is easy to trigger. Auto-create stays on so the
/// test topic exists by the time the producer sends.
fn small_max_bytes_cluster_config() -> ClusterConfig {
    let mut props = std::collections::BTreeMap::new();
    props.insert("KAFKA_MESSAGE_MAX_BYTES".to_string(), "15000".to_string());
    ClusterConfig::with_properties(props)
}

/// Translated from `ProducerFailureHandlingTest.testTooLargeRecordWithAckZero`.
/// With acks=0 the broker doesn't ack, so the producer returns a
/// completed future with `offset == -1` regardless of payload size.
async fn produce_too_large_record_acks_zero_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ack0_too_large");
    let mut config = make_config(&bootstrap_for(factory, ctx));
    config.insert("acks".to_string(), "0".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    // Any record under acks=0 returns offset=-1; the "too large" framing in
    // the Java test is incidental.
    let record = ProducerRecord::with_key(topic.clone(), Some(b("key")), Some(vec![0u8; 16_000]));
    let future = producer.send(record).await.expect("send should succeed");
    let metadata = future
        .get_timeout(Duration::from_secs(30))
        .await
        .expect("ack=0 produce should not error");

    assert!(!metadata.has_offset(), "acks=0 metadata.has_offset() should be false");
    assert_eq!(metadata.offset(), -1, "acks=0 metadata.offset() should be -1");

    producer.close().await.expect("close should succeed");
}

/// Translated from `ProducerFailureHandlingTest.testTooLargeRecordWithAckOne`.
/// With acks=1 the broker rejects an oversized record (server-side
/// `message.max.bytes=15000` from the restrictive cluster) and the
/// producer surfaces RecordTooLarge.
async fn produce_too_large_record_acks_one_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ack1_too_large");
    let mut config = make_config(&bootstrap_for(factory, ctx));
    config.insert("acks".to_string(), "1".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    let record = ProducerRecord::with_key(topic.clone(), Some(b("key")), Some(vec![0u8; 16_000]));
    let future = producer.send(record).await.expect("send should accept the request");
    let result = future.get_timeout(Duration::from_secs(30)).await;

    assert!(result.is_err(), "Oversized record under acks=1 should error, got: {result:?}");
    let err = result.unwrap_err();
    // Java's Producer throws RecordTooLargeException for both client-side
    // (max.request.size) and broker-side (message.max.bytes) rejections.
    // The Rust client uses the dedicated RecordTooLarge variant only for
    // client-side rejections; broker-side rejections come back through
    // the response path as Generic(MessageTooLarge). Accept either.
    let too_large = match &err {
        confluent_kafka::common::Error::RecordTooLarge(_) => true,
        confluent_kafka::common::Error::Generic(g) => {
            g.error() == confluent_kafka::common::protocol::Errors::MessageTooLarge
        },
        _ => false,
    };
    assert!(
        too_large,
        "Expected RecordTooLarge or Generic(MessageTooLarge) from server, got: {err:?}"
    );

    producer.close().await.expect("close should succeed");
}

/// Translated from `ProducerFailureHandlingTest.testNonExistentTopic`.
/// With `auto.create.topics.enable=false` (restrictive cluster), sending
/// to a never-existed topic times out trying to fetch metadata.
async fn produce_to_non_existent_topic_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let mut config = make_config(&bootstrap_for(factory, ctx));
    // Short max.block.ms so the test doesn't sit on the default 30s.
    config.insert("max.block.ms".to_string(), "5000".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    let topic = ctx.topic("never_existed");
    let record = ProducerRecord::with_key(topic, Some(b("key")), Some(b("value")));
    let future = producer.send(record).await.expect("send should accept the request");
    let result = future.get_timeout(Duration::from_secs(30)).await;

    assert!(result.is_err(), "Send to non-existent topic should error, got: {result:?}");
    let err = result.unwrap_err();
    assert!(
        matches!(err, confluent_kafka::common::Error::Timeout(_)),
        "Expected Timeout for non-existent topic, got: {err:?}"
    );

    producer.close().await.expect("close should succeed");
}

/// Translated from `ProducerFailureHandlingTest.testWrongBrokerList`.
/// Producer with bootstrap pointing at non-existent brokers; metadata
/// fetch times out within `max.block.ms`.
async fn produce_with_wrong_broker_list_inner<F: ProducerBackendFactory>(_ctx: &mut TestContext, factory: &F) {
    // Don't use ctx.bootstrap_servers — explicitly point at a dead
    // address. 127.0.0.1:1/2 are valid IP literals (so bootstrap
    // validation passes) but ports 1/2 are unused, so connection
    // attempts get RST'd and the producer hits max.block.ms. Same
    // behavior for native rust (loopback on the test process) and the
    // gRPC backends (loopback inside their container).
    let _ = factory; // silence unused warning when no needs_container_bootstrap branch
    let bootstrap = "127.0.0.1:1,127.0.0.1:2";
    let mut config = make_config(bootstrap);
    config.insert("max.block.ms".to_string(), "3000".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    let record = ProducerRecord::with_key("any-topic".to_string(), Some(b("key")), Some(b("value")));
    let future = producer.send(record).await.expect("send should accept the request");
    let result = future.get_timeout(Duration::from_secs(30)).await;

    assert!(result.is_err(), "Send with wrong broker list should error, got: {result:?}");
    let err = result.unwrap_err();
    assert!(
        matches!(err, confluent_kafka::common::Error::Timeout(_)),
        "Expected Timeout from unreachable bootstrap, got: {err:?}"
    );

    producer.close().await.expect("close should succeed");
}

/// Translated from `ProducerFailureHandlingTest.testInvalidPartition`.
/// Send with explicit partition >= partition-count of the (auto-created)
/// topic. The producer waits for metadata that never resolves the
/// requested partition and times out within `max.block.ms`.
async fn produce_invalid_partition_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("invalid_partition");
    let mut config = make_config(&bootstrap_for(factory, ctx));
    config.insert("max.block.ms".to_string(), "5000".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    // First, create the topic with 1 partition by sending to partition 0.
    let warmup = ProducerRecord::with_partition(topic.clone(), Some(0), Some(b("k")), Some(b("v")))
        .expect("record creation should succeed");
    producer
        .send(warmup)
        .await
        .expect("warmup send should accept")
        .get_timeout(Duration::from_secs(30))
        .await
        .expect("warmup send should succeed");

    // Now send to partition 99 which doesn't exist.
    let record = ProducerRecord::with_partition(topic, Some(99), Some(b("k")), Some(b("v")))
        .expect("record creation should succeed");
    let future = producer.send(record).await.expect("send should accept the request");
    let result = future.get_timeout(Duration::from_secs(30)).await;

    assert!(result.is_err(), "Send to invalid partition should error, got: {result:?}");
    let err = result.unwrap_err();
    assert!(
        matches!(err, confluent_kafka::common::Error::Timeout(_)),
        "Expected Timeout for invalid partition, got: {err:?}"
    );

    producer.close().await.expect("close should succeed");
}

/// Translated from `ProducerFailureHandlingTest.testSendAfterClosed`.
/// Calling send() after close() returns IllegalState.
async fn send_after_closed_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("send_after_closed");
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
        .await
        .expect("Failed to create producer");

    // Warmup to ensure metadata is fresh, mirroring the Java test.
    let record = ProducerRecord::with_key(topic.clone(), Some(b("key")), Some(b("value")));
    producer
        .send(record.clone())
        .await
        .expect("warmup send should accept")
        .get_timeout(Duration::from_secs(30))
        .await
        .expect("warmup send should succeed");

    producer.close().await.expect("close should succeed");

    // Native Rust returns Err directly from send() once closed; the
    // gRPC backends return Ok(KafkaFuture) where the future resolves to
    // an IllegalState error (the gRPC server's lookup of the closed
    // producer_id fails). Accept both shapes.
    let send_result = producer.send(record).await;
    let err = match send_result {
        Err(e) => e,
        Ok(future) => future
            .get_timeout(Duration::from_secs(5))
            .await
            .expect_err("future after close should be Err"),
    };
    assert!(
        matches!(err, confluent_kafka::common::Error::IllegalState(_)),
        "Expected IllegalState after close, got: {err:?}"
    );
}

/// Translated from `PlaintextProducerSendTest.testBatchSizeZero`.
/// With batch.size=0, each record is its own batch and gets dispatched
/// immediately. Sends should succeed without needing flush.
async fn produce_batch_size_zero_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("batch_zero");
    let mut config = make_config(&bootstrap_for(factory, ctx));
    config.insert("batch.size".to_string(), "0".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    for i in 0..5 {
        let record =
            ProducerRecord::with_key(topic.clone(), Some(b(&format!("key-{i}"))), Some(b(&format!("value-{i}"))));
        let future = producer.send(record).await.expect("send should succeed");
        let metadata = future
            .get_timeout(Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("send {i} with batch.size=0 should succeed, got: {e:?}"));
        assert!(metadata.offset() >= 0, "Record {i} should have a valid offset");
    }

    producer.close().await.expect("close should succeed");
}

/// Translated (simplified) from `PlaintextProducerSendTest.testNonBlockingProducer`.
/// With max.block.ms=0 and cold metadata, the first send returns a
/// future that's immediately done with Timeout. The buffer-exhaustion
/// subtest from the Scala original isn't ported because gRPC sends are
/// synchronous, eliminating the in-flight buffer accumulation pattern
/// that subtest exercises.
async fn produce_non_blocking_max_block_zero_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("non_blocking");
    let mut config = make_config(&bootstrap_for(factory, ctx));
    config.insert("max.block.ms".to_string(), "0".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    // Cold metadata + max.block.ms=0 → producer can't wait, send fails
    // immediately with Timeout (the future is already resolved).
    let record = ProducerRecord::with_key(topic, Some(b("key")), Some(b("value")));
    let future = producer.send(record).await.expect("send should accept the request");
    assert!(future.is_done(), "max.block.ms=0 future should be immediately done");
    let result = future.get().await;
    assert!(
        result.is_err(),
        "max.block.ms=0 with cold metadata should error, got: {result:?}"
    );
    let err = result.unwrap_err();
    assert!(
        matches!(err, confluent_kafka::common::Error::Timeout(_)),
        "Expected Timeout for cold metadata under max.block.ms=0, got: {err:?}"
    );

    producer.close().await.expect("close should succeed");
}

// ---------------------------------------------------------------------------
// Test instantiations
// ---------------------------------------------------------------------------
//
// Under multilanguage-tests, each scenario fans out to rust/python/c via
// the macro. Otherwise the rust_only_fallback module instantiates each
// scenario manually against RustNativeFactory.

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
/// Test: partitionsFor returns metadata for an existing topic. Exercises the
/// producer PartitionsFor RPC across all backends (the C/Python servers now
/// expose partitions_for).
async fn produce_partitions_for_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("partitions_for");
    let producer = factory.create(make_config(&bootstrap_for(factory, ctx))).await.expect("create");
    // Produce one record so the topic exists.
    let record = ProducerRecord::with_key(topic.clone(), Some(b("k")), Some(b("v")));
    producer
        .send(record)
        .await
        .expect("send")
        .get_timeout(Duration::from_secs(30))
        .await
        .expect("produce");

    let infos = producer.partitions_for(&topic).await.expect("partitions_for should succeed");
    assert!(!infos.is_empty(), "{} backend: expected >=1 partition", factory.name());
    assert!(
        infos.iter().any(|p| p.topic() == topic && p.partition() == 0),
        "{} backend: expected partition 0 of {topic}",
        factory.name()
    );
    producer.close().await.expect("close");
}

#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_flush_sends_pending_records, flush_sends_pending_records_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_close_flushes_pending, close_flushes_pending_inner);

// New tests translated from Java/Scala sources (see COVERAGE-ASSESSMENT.md).
// Tests using the restrictive cluster (no auto-create + small message.max.bytes)
// pass it as the macro's optional 3rd argument.

#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_produce_too_large_record_acks_zero,
    produce_too_large_record_acks_zero_inner
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_produce_too_large_record_acks_one,
    produce_too_large_record_acks_one_inner,
    small_max_bytes_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_produce_to_non_existent_topic,
    produce_to_non_existent_topic_inner,
    no_auto_create_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_with_wrong_broker_list, produce_with_wrong_broker_list_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_invalid_partition, produce_invalid_partition_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_send_after_closed, send_after_closed_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_batch_size_zero, produce_batch_size_zero_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_produce_non_blocking_max_block_zero,
    produce_non_blocking_max_block_zero_inner
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_partitions_for, produce_partitions_for_inner);

// ---------------------------------------------------------------------------
// Rust-native-only tests — the Producer trait surface they exercise
// (KafkaFuture cancellation under close_timeout(0); Box<dyn Serializer>
// error path) doesn't survive the gRPC bytes-on-the-wire boundary, so
// they aren't multilanguageable. See COVERAGE-ASSESSMENT.md.
// ---------------------------------------------------------------------------

/// A test-only Serializer that always returns Err — used to translate
/// the Scala `testWrongSerializer` scenario where the producer's
/// configured serializer fails for the input data.
#[cfg(feature = "integration-tests")]
struct FailingSerializer;

#[cfg(feature = "integration-tests")]
impl confluent_kafka::common::serialization::Serializer<Vec<u8>> for FailingSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _data: Option<&Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, confluent_kafka::common::Error> {
        Err(confluent_kafka::common::Error::serialization("FailingSerializer always fails"))
    }
}

/// Translated from `BaseProducerSendTest.testCloseWithZeroTimeoutFromCallerThread`.
/// linger.ms=MAX makes records sit in the accumulator; close_timeout(0)
/// must abort them so all the futures complete with an error.
#[cfg(feature = "integration-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn test_close_with_zero_timeout_aborts_pending() {
    use confluent_kafka::common::serialization::ByteArraySerializer;
    use confluent_kafka::producer::{KafkaProducer, ProducerConfig};

    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("close_zero");

    let mut props = make_config(ctx.bootstrap_servers());
    // Pin everything in the accumulator so close_timeout(0) has work to abort.
    props.insert("linger.ms".to_string(), "60000".to_string());
    props.insert("delivery.timeout.ms".to_string(), "120000".to_string());
    let producer_config = ProducerConfig::from_properties(&props).expect("Invalid test config");
    let producer =
        KafkaProducer::from_config(producer_config, Box::new(ByteArraySerializer), Box::new(ByteArraySerializer))
            .expect("Failed to create producer");

    let mut futures = Vec::new();
    for i in 0..5 {
        let record = ProducerRecord::with_partition(
            topic.clone(),
            Some(0),
            Some(b(&format!("key-{i}"))),
            Some(b(&format!("value-{i}"))),
        )
        .expect("record creation should succeed");
        // KafkaProducer<Vec<u8>, Vec<u8>> has an inherent send() that
        // shadows the trait method; call via UFCS to use the trait.
        let future = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
            .await
            .expect("send should accept the request");
        futures.push(future);
    }

    Producer::close_timeout(&producer, Duration::ZERO)
        .await
        .expect("close_timeout(0) should return Ok");

    for (i, future) in futures.iter().enumerate() {
        let result = future.get_timeout(Duration::from_secs(5)).await;
        assert!(
            result.is_err(),
            "Future {i} should have been aborted by close_timeout(0), got Ok: {result:?}"
        );
    }
}

/// Translated from `PlaintextProducerSendTest.testWrongSerializer`.
/// A serializer that always errors causes send to surface a
/// `Error::Serialization`. The Producer trait wraps the serializer
/// in `Box<dyn Serializer>` — that surface only exists in the native
/// Rust path, hence rust-only.
#[cfg(feature = "integration-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn test_wrong_serializer_errors_send() {
    use confluent_kafka::common::serialization::ByteArraySerializer;
    use confluent_kafka::producer::{KafkaProducer, ProducerConfig};

    let ctx = TestContext::new(ClusterConfig::default()).await;
    let props = make_config(ctx.bootstrap_servers());
    let producer_config = ProducerConfig::from_properties(&props).expect("Invalid test config");
    let producer =
        KafkaProducer::from_config(producer_config, Box::new(ByteArraySerializer), Box::new(FailingSerializer))
            .expect("Failed to create producer");

    let record = ProducerRecord::with_key("any-topic".to_string(), Some(b("key")), Some(b("value")));
    // UFCS to call the trait method past the inherent shadow.
    let send_result = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record).await;

    // Either send returns Err directly, or returns Ok(future) where
    // future.get() errors — both are acceptable for serialization
    // failure depending on whether the producer fails fast or at the
    // accumulator-add boundary.
    let err = match send_result {
        Err(e) => e,
        Ok(future) => future
            .get_timeout(Duration::from_secs(5))
            .await
            .expect_err("future should be Err for failing serializer"),
    };
    assert!(
        matches!(err, confluent_kafka::common::Error::Serialization(_)),
        "Expected Serialization error, got: {err:?}"
    );

    Producer::close(&producer).await.expect("close should succeed");
}

#[cfg(all(feature = "integration-tests", not(feature = "multilanguage-tests")))]
mod rust_only_fallback {
    use super::*;
    use crate::common::backend_factory::RustNativeFactory;

    async fn ctx() -> TestContext {
        TestContext::new(ClusterConfig::default()).await
    }

    async fn no_auto_create_ctx() -> TestContext {
        TestContext::new(no_auto_create_cluster_config()).await
    }

    async fn small_max_bytes_ctx() -> TestContext {
        TestContext::new(small_max_bytes_cluster_config()).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_single_record() {
        produce_single_record_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_with_key() {
        produce_with_key_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_multiple_records_ordering() {
        produce_multiple_records_ordering_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_with_compression() {
        produce_with_compression_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_to_invalid_topic() {
        produce_to_invalid_topic_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_record_too_large() {
        produce_record_too_large_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_flush_sends_pending_records() {
        flush_sends_pending_records_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_close_flushes_pending() {
        close_flushes_pending_inner(&mut ctx().await, &RustNativeFactory).await;
    }

    // The 8 newly translated multi-language tests, run rust-only when
    // the multilanguage-tests feature isn't enabled.

    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_too_large_record_acks_zero() {
        produce_too_large_record_acks_zero_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_too_large_record_acks_one() {
        produce_too_large_record_acks_one_inner(&mut small_max_bytes_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_to_non_existent_topic() {
        produce_to_non_existent_topic_inner(&mut no_auto_create_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_with_wrong_broker_list() {
        produce_with_wrong_broker_list_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_invalid_partition() {
        produce_invalid_partition_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_after_closed() {
        send_after_closed_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_batch_size_zero() {
        produce_batch_size_zero_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_non_blocking_max_block_zero() {
        produce_non_blocking_max_block_zero_inner(&mut ctx().await, &RustNativeFactory).await;
    }
}
