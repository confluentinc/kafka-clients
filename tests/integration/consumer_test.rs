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

//! Integration tests for the AsyncKafkaConsumer against a real
//! Kafka 4.2.0 broker.
//!
//! These tests exercise the production constructor wired in Phase 12
//! (commit 4/N): `new_consumer::<K, V>(...)` returns a real
//! `AsyncKafkaConsumer` that spawns its bg task, opens a `NetworkClient`
//! against the broker, and runs the KIP-848 group-protocol membership
//! state machine end-to-end.
//!
//! All four tests run against a testcontainers-provisioned Kafka
//! 4.2.0 broker with `group.coordinator.rebalance.protocols=classic,consumer`
//! enabled, exercising the full subscribe/poll/commit/seek loop end
//! to end. Phase 12.5 wired the per-RM response routing
//! (FindCoordinator, Heartbeat, TopicMetadata, Fetch, OffsetCommit,
//! OffsetFetch) so the bg task drives all manager state machines to
//! completion against a real broker.
//!
//! The docker-free smoke test at
//! `tests/consumer/async_kafka_consumer_test.rs` exercises the
//! production ctor (channels + NetworkClient + bg-task spawn + clean
//! close) without requiring a broker.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;

use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::ConsumerRebalanceListener;
use confluent_kafka::consumer::new_consumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

/// Cluster config with KIP-848 (`group.protocol=consumer`) enabled
/// on the broker side.
///
/// Kafka 4.2.0 ships KIP-848 support, but the broker default
/// `group.coordinator.rebalance.protocols` is `classic` only. We
/// override to `classic,consumer` so the consumer-side
/// `group.protocol=consumer` is accepted. PLAN.md is the source of
/// truth for this override; do NOT mutate `ClusterConfig::default()`
/// — other integration suites depend on its hash key for cluster
/// pooling.
fn cluster_config_with_kip848() -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert(
        "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS".to_string(),
        "classic,consumer".to_string(),
    );
    ClusterConfig::with_properties(props)
}

/// Local string deserializer for integration tests. The shared
/// `common::serialization` module currently exports only
/// `StringSerializer`; the consumer side gets this inline impl until
/// a `StringDeserializer` is added crate-wide.
struct StringDeserializer;

impl Deserializer<String> for StringDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
        String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(format!("invalid utf-8: {}", e)))
    }
}

/// Build a `ConsumerConfig` aligned with the producer integration
/// tests (PLAINTEXT listener), with KIP-848 group protocol.
fn make_consumer_config(bootstrap: &str, group_id: &str) -> ConsumerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
    ]);
    ConsumerConfig::from_properties(&props).expect("invalid test config")
}

/// Variant of `make_consumer_config` without a `group.id` — for the
/// assignment-only test below.
fn make_consumer_config_groupless(bootstrap: &str) -> ConsumerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer-noassign".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
    ]);
    ConsumerConfig::from_properties(&props).expect("invalid test config")
}

/// Build a `ProducerConfig` matching the existing producer integration
/// test pattern (acks=all, short linger/max-block to avoid hangs).
fn make_producer_config(bootstrap: &str) -> ProducerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "integration-test-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
    ]);
    ProducerConfig::from_properties(&props).expect("invalid producer test config")
}

/// Produce `count` records with deterministic keys `k0..k{count-1}` and
/// values `v0..v{count-1}` to the given topic, blocking on each ack so
/// the consumer is guaranteed to see them. Producer is closed on exit.
async fn produce_deterministic_records(bootstrap: &str, topic: &str, count: usize) {
    let producer: KafkaProducer<String, String> = KafkaProducer::from_config(
        make_producer_config(bootstrap),
        Box::new(StringSerializer),
        Box::new(StringSerializer),
    )
    .expect("Failed to build test producer");

    for i in 0..count {
        let rec = ProducerRecord::with_key(topic.to_string(), Some(format!("k{}", i)), Some(format!("v{}", i)));
        let future = producer.send(rec).await.expect("send should succeed");
        future
            .get_timeout(Duration::from_secs(30))
            .await
            .expect("produce should succeed");
    }
    producer.close().await.expect("producer close should succeed");
}

/// Test: subscribe to a freshly-created topic, produce 10 records via
/// the existing `KafkaProducer`, poll the consumer until 10 records
/// have been collected, then assert every record carries the expected
/// key/value/topic/partition/offset.
#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_and_poll_records() {
    let mut ctx = TestContext::new(cluster_config_with_kip848()).await;
    let topic = ctx.topic("subscribe_poll");
    let group_id = ctx.group_id("g1");

    produce_deterministic_records(ctx.bootstrap_servers(), &topic, 10).await;

    let mut consumer = new_consumer::<String, String>(
        make_consumer_config(ctx.bootstrap_servers(), &group_id),
        Box::new(StringDeserializer),
        Box::new(StringDeserializer),
    )
    .expect("new_consumer should succeed");

    consumer.subscribe(vec![topic.clone()]).await.expect("subscribe should succeed");

    let mut collected: Vec<(String, String, i32, i64)> = Vec::new();
    let start = Instant::now();
    while collected.len() < 10 && start.elapsed() < Duration::from_secs(30) {
        let records = consumer.poll(Duration::from_secs(5)).await.expect("poll should succeed");
        for record in &records {
            let key = record.key().expect("record key should be present").clone();
            let value = record.value().expect("record value should be present").clone();
            assert_eq!(record.topic(), topic, "record topic should match subscribed topic");
            assert!(record.partition() >= 0, "partition should be non-negative");
            collected.push((key, value, record.partition(), record.offset()));
        }
    }

    assert_eq!(
        collected.len(),
        10,
        "should have polled all 10 records within the deadline, got {}",
        collected.len()
    );

    let mut produced_keys: Vec<String> = (0..10).map(|i| format!("k{}", i)).collect();
    let mut collected_keys: Vec<String> = collected.iter().map(|(k, _, _, _)| k.clone()).collect();
    produced_keys.sort();
    collected_keys.sort();
    assert_eq!(produced_keys, collected_keys, "every produced key should appear exactly once");

    for (k, v, _, _) in &collected {
        let n_from_k: i32 = k.strip_prefix('k').and_then(|s| s.parse().ok()).expect("key should be kN");
        let n_from_v: i32 = v.strip_prefix('v').and_then(|s| s.parse().ok()).expect("value should be vN");
        assert_eq!(n_from_k, n_from_v, "key kN must pair with value vN");
    }

    let mut by_partition: HashMap<i32, Vec<i64>> = HashMap::new();
    for (_, _, p, o) in &collected {
        by_partition.entry(*p).or_default().push(*o);
    }
    for (p, offsets) in &by_partition {
        let mut sorted = offsets.clone();
        sorted.sort();
        for (i, o) in offsets.iter().enumerate() {
            assert_eq!(
                *o, sorted[i],
                "offsets in partition {} should be strictly monotonic in arrival order",
                p
            );
        }
    }

    consumer.close().await.expect("consumer close should succeed");
}

/// Test: explicit-assignment consumer (no `group.id`) reads records
/// from `partition=0`. Verifies the production ctor wiring builds a
/// groupless consumer (no membership / heartbeat / commit managers in
/// `RequestManagers` — `coordinator`/`commit`/`consumer_heartbeat` are
/// `None` when `group.id` is absent) and that the fetch path still
/// works without going through the membership state machine.
#[tokio::test(flavor = "multi_thread")]
async fn test_assign_partitions_and_poll() {
    let mut ctx = TestContext::new(cluster_config_with_kip848()).await;
    let topic = ctx.topic("assign_poll");

    produce_deterministic_records(ctx.bootstrap_servers(), &topic, 5).await;

    let mut consumer = new_consumer::<String, String>(
        make_consumer_config_groupless(ctx.bootstrap_servers()),
        Box::new(StringDeserializer),
        Box::new(StringDeserializer),
    )
    .expect("new_consumer should succeed for groupless consumer");

    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");

    let mut collected: Vec<(String, String)> = Vec::new();
    let start = Instant::now();
    while collected.len() < 5 && start.elapsed() < Duration::from_secs(30) {
        let records = consumer.poll(Duration::from_secs(5)).await.expect("poll should succeed");
        for record in &records {
            let key = record.key().expect("record key should be present").clone();
            let value = record.value().expect("record value should be present").clone();
            assert_eq!(record.topic(), topic);
            assert_eq!(record.partition(), 0, "all records should be on partition 0");
            collected.push((key, value));
        }
    }

    assert_eq!(collected.len(), 5, "should have polled all 5 records, got {}", collected.len());

    consumer.close().await.expect("consumer close should succeed");
}

/// Test: consumer 1 subscribes in group `G1`, polls 5 records,
/// `commit_sync().await`, closes. Consumer 2 in the same `G1`, polls,
/// asserts the remaining records (offsets 5..10) are visible from the
/// committed offset (not re-fetched from offset 0).
#[tokio::test(flavor = "multi_thread")]
async fn test_commit_sync_then_resume_in_same_group() {
    let mut ctx = TestContext::new(cluster_config_with_kip848()).await;
    let topic = ctx.topic("commit_resume");
    let group_id = ctx.group_id("g_resume");

    produce_deterministic_records(ctx.bootstrap_servers(), &topic, 10).await;

    // Consumer 1: poll 5 records, commit, close.
    //
    // `max.poll.records=5` caps each `poll()` at five records so the
    // `collected < 5` loop terminates at exactly 5, not 10. Without
    // this override the default `max.poll.records=500` would let the
    // broker return all 10 records in one fetch, advancing
    // consumer1's position to offset 10 and committing the
    // high-watermark — consumer2 would then see zero records. This
    // mirrors the Java `KafkaConsumerTest` "commit-then-resume"
    // pattern.
    {
        let consumer1_props = HashMap::from([
            ("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string()),
            ("group.id".to_string(), group_id.clone()),
            ("group.protocol".to_string(), "consumer".to_string()),
            ("auto.offset.reset".to_string(), "earliest".to_string()),
            ("client.id".to_string(), "integration-test-consumer".to_string()),
            ("enable.auto.commit".to_string(), "false".to_string()),
            ("max.poll.records".to_string(), "5".to_string()),
        ]);
        let consumer1_config = ConsumerConfig::from_properties(&consumer1_props).expect("invalid test config");

        let mut consumer1 = new_consumer::<String, String>(
            consumer1_config,
            Box::new(StringDeserializer),
            Box::new(StringDeserializer),
        )
        .expect("consumer1 ctor should succeed");

        consumer1
            .subscribe(vec![topic.clone()])
            .await
            .expect("subscribe should succeed");

        let mut collected = 0;
        let start = Instant::now();
        while collected < 5 && start.elapsed() < Duration::from_secs(30) {
            let records = consumer1.poll(Duration::from_secs(5)).await.expect("poll should succeed");
            collected += records.count();
        }
        assert!(collected >= 5, "consumer1 should have polled at least 5 records");

        consumer1.commit_sync().await.expect("commit_sync should succeed");
        consumer1.close().await.expect("consumer1 close should succeed");
    }

    // Give the broker a moment to fully process consumer1's LeaveGroup
    // before consumer2 joins the same group. Without this, consumer2 may
    // race the LeaveGroup completion at the broker and either (a) join a
    // stale assignment generation or (b) wait out the full session
    // timeout (45s by default) before being assigned partitions.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Consumer 2: same group, polls only the remaining records.
    {
        let mut consumer2 = new_consumer::<String, String>(
            make_consumer_config(ctx.bootstrap_servers(), &group_id),
            Box::new(StringDeserializer),
            Box::new(StringDeserializer),
        )
        .expect("consumer2 ctor should succeed");

        consumer2
            .subscribe(vec![topic.clone()])
            .await
            .expect("subscribe should succeed");

        let mut total = 0;
        let start = Instant::now();
        // 60s is generous — KIP-848 rebalance typically completes well
        // within 10s, but consumer2 in the same group may still wait
        // out part of consumer1's session timeout if LeaveGroup at
        // close races partition reassignment broker-side.
        while start.elapsed() < Duration::from_secs(60) {
            let records = consumer2.poll(Duration::from_secs(5)).await.expect("poll should succeed");
            total += records.count();
            // Give the broker an extra few seconds to surface stragglers.
            if total > 0 && start.elapsed() > Duration::from_secs(15) {
                break;
            }
        }

        // We polled 5 records before commit; consumer2 should see the
        // remaining 5 (not all 10).
        assert!(
            total > 0 && total <= 5,
            "consumer2 should see remaining records (>0 and <=5), saw {}",
            total
        );

        consumer2.close().await.expect("consumer2 close should succeed");
    }
}

/// Test: subscribe, poll a few records to establish position, then
/// `seek_to_beginning(&assigned)`, poll again, assert all records can
/// be re-read from offset 0.
#[tokio::test(flavor = "multi_thread")]
async fn test_seek_to_beginning_re_reads_records() {
    let mut ctx = TestContext::new(cluster_config_with_kip848()).await;
    let topic = ctx.topic("seek_to_beginning");
    let group_id = ctx.group_id("g_seek");

    produce_deterministic_records(ctx.bootstrap_servers(), &topic, 5).await;

    let mut consumer = new_consumer::<String, String>(
        make_consumer_config(ctx.bootstrap_servers(), &group_id),
        Box::new(StringDeserializer),
        Box::new(StringDeserializer),
    )
    .expect("new_consumer should succeed");

    consumer.subscribe(vec![topic.clone()]).await.expect("subscribe should succeed");

    // First poll: wait for assignment to land, then drain at least one
    // batch to establish position.
    let mut first_pass_count = 0;
    let start = Instant::now();
    while first_pass_count < 5 && start.elapsed() < Duration::from_secs(30) {
        let records = consumer.poll(Duration::from_secs(5)).await.expect("poll should succeed");
        first_pass_count += records.count();
    }
    assert_eq!(first_pass_count, 5, "first pass should drain all 5 records");

    // Seek every assigned partition back to the start.
    let assigned: Vec<TopicPartition> = consumer.assignment().into_iter().collect();
    assert!(!assigned.is_empty(), "seek_to_beginning requires non-empty assignment");
    consumer
        .seek_to_beginning(&assigned)
        .await
        .expect("seek_to_beginning should succeed");

    // Second pass: same 5 records arrive again.
    let mut second_pass_count = 0;
    let restart = Instant::now();
    while second_pass_count < 5 && restart.elapsed() < Duration::from_secs(30) {
        let records = consumer.poll(Duration::from_secs(5)).await.expect("poll should succeed");
        second_pass_count += records.count();
    }
    assert_eq!(
        second_pass_count, 5,
        "second pass after seek_to_beginning should re-read all 5 records"
    );

    consumer.close().await.expect("consumer close should succeed");
}

// ── Failed-listener recovery (ConsumerIntegrationTest) ─────────────────
//
// Translated from
// `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/ConsumerIntegrationTest.java`
// (Apache Kafka 4.2). KIP-848 (`GroupProtocol.CONSUMER`) arm only; the
// `*WithGroupProtocolClassic` twins are OUT_OF_SCOPE per
// `consumer-threading.md` §20. These listeners throw (return `Err`) from
// `on_partitions_assigned` and need NO consumer reentrancy, so — unlike
// the `PlaintextConsumerCallbackTest` reentrancy suite — they run.

/// Listener that returns `Err` from the FIRST `on_partitions_assigned`
/// invocation only; subsequent invocations succeed. Mirrors Java's
/// anonymous listener in `testFetchPartitionsAfterFailedListener` (count
/// == 1 → throw `IllegalArgumentException("temporary error")`).
struct FailOnceAssignedListener {
    count: Arc<AtomicUsize>,
}

#[async_trait]
impl ConsumerRebalanceListener for FailOnceAssignedListener {
    async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        Ok(())
    }

    async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        let n = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        if n == 1 {
            return Err(Error::illegal_state("temporary error"));
        }
        Ok(())
    }
}

/// Listener that ALWAYS returns `Err` from `on_partitions_assigned`.
/// Mirrors Java's listener in `testFetchPartitionsWithAlwaysFailedListener`
/// (always `throw new IllegalArgumentException("always failed")`).
struct AlwaysFailAssignedListener;

#[async_trait]
impl ConsumerRebalanceListener for AlwaysFailAssignedListener {
    async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        Ok(())
    }

    async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        Err(Error::illegal_state("always failed"))
    }
}

/// Translates Java's
/// `testFetchPartitionsAfterFailedListenerWithGroupProtocolConsumer`
/// (line 100).
///
/// The listener throws on the first `onPartitionsAssigned`; the consumer
/// must recover and still deliver the single produced record. We poll
/// (tolerating the first error surfaced by the failed callback) until one
/// record arrives.
#[tokio::test(flavor = "multi_thread")]
async fn test_fetch_partitions_after_failed_listener() {
    let mut ctx = TestContext::new(cluster_config_with_kip848()).await;
    let topic = ctx.topic("failed_listener");
    let group_id = ctx.group_id("g_failed_listener");

    produce_deterministic_records(ctx.bootstrap_servers(), &topic, 1).await;

    let mut consumer = new_consumer::<String, String>(
        make_consumer_config(ctx.bootstrap_servers(), &group_id),
        Box::new(StringDeserializer),
        Box::new(StringDeserializer),
    )
    .expect("new_consumer should succeed");

    let count = Arc::new(AtomicUsize::new(0));
    let listener: Arc<dyn ConsumerRebalanceListener> = Arc::new(FailOnceAssignedListener { count: Arc::clone(&count) });
    consumer
        .subscribe_with_listener(vec![topic.clone()], listener)
        .await
        .expect("subscribe_with_listener should succeed");

    // Java: `waitForCondition(() -> consumer.poll(1s).count() == 1, 5000)`.
    // The first poll may surface the failed-callback error; tolerate it and
    // keep polling — the consumer recovers on the retry (count == 2 → Ok).
    let mut delivered = 0usize;
    let deadline = Instant::now() + Duration::from_secs(30);
    while delivered < 1 && Instant::now() < deadline {
        match consumer.poll(Duration::from_secs(1)).await {
            Ok(records) => {
                delivered += records.count();
            },
            Err(err) => {
                // The temporary listener error must not be fatal; keep polling.
                assert!(
                    !err.is_fatal_error(),
                    "first-listener failure should be recoverable, got fatal: {err}"
                );
            },
        }
    }

    assert_eq!(
        delivered, 1,
        "consumer should recover from the one-time listener failure and deliver the record"
    );
    assert!(
        count.load(Ordering::SeqCst) >= 1,
        "the assigned listener should have been invoked"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testFetchPartitionsWithAlwaysFailedListenerWithGroupProtocolConsumer`
/// (line 148).
///
/// The listener always throws. Java's contract: for ~3 seconds, each
/// `poll(1s)` either returns 0 records (the assigned partitions are never
/// marked fetchable because the callback never completes successfully) OR
/// throws a `KafkaException("User rebalance callback throws an error")`.
///
/// The listener throws a non-`KafkaException` (`IllegalState("always
/// failed")`, the analog of Java's `IllegalArgumentException("always
/// failed")`). Java's `invokeRebalanceCallbacks` runs that through
/// `maybeWrapAsKafkaException(e, "User rebalance callback throws an
/// error")` (`AsyncKafkaConsumer.java:2334`), so `poll()` surfaces a
/// `KafkaException` whose `getMessage()` is exactly `"User rebalance
/// callback throws an error"`. The Rust production code now mirrors this
/// wrap (Phase 39 fixup), so this test asserts BOTH:
///   1. **no successful poll ever returns a record** (the always-failing
///      callback keeps every assigned partition un-fetchable);
///   2. when an error IS surfaced, its message is exactly Java's
///      `"User rebalance callback throws an error"`.
#[tokio::test(flavor = "multi_thread")]
async fn test_fetch_partitions_with_always_failed_listener() {
    let mut ctx = TestContext::new(cluster_config_with_kip848()).await;
    let topic = ctx.topic("always_failed_listener");
    let group_id = ctx.group_id("g_always_failed_listener");

    produce_deterministic_records(ctx.bootstrap_servers(), &topic, 1).await;

    let mut consumer = new_consumer::<String, String>(
        make_consumer_config(ctx.bootstrap_servers(), &group_id),
        Box::new(StringDeserializer),
        Box::new(StringDeserializer),
    )
    .expect("new_consumer should succeed");

    let listener: Arc<dyn ConsumerRebalanceListener> = Arc::new(AlwaysFailAssignedListener);
    consumer
        .subscribe_with_listener(vec![topic.clone()], listener)
        .await
        .expect("subscribe_with_listener should succeed");

    // Java loops for 3s asserting `poll().count() == 0` (or catching the
    // callback KafkaException). Mirror that window.
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(3) {
        match consumer.poll(Duration::from_secs(1)).await {
            Ok(records) => {
                assert_eq!(
                    records.count(),
                    0,
                    "an always-failing assigned-listener must keep all partitions un-fetchable; \
                     poll should never return records"
                );
            },
            Err(err) => {
                // Java's optional `catch (KafkaException ex)` arm asserts
                // `assertEquals("User rebalance callback throws an error",
                // ex.getMessage())` (ConsumerIntegrationTest.java:185). The
                // Rust production wrap surfaces the same message, so pin it
                // exactly — a regression that masked the callback path with
                // an unrelated error (timeout, connection) would now fail.
                assert_eq!(
                    err.to_string(),
                    "User rebalance callback throws an error",
                    "a surfaced poll error must be the wrapped rebalance-callback error, got: {err}"
                );
            },
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    consumer.close().await.expect("consumer close should succeed");
}
