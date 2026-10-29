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
//! (commit 4/N): `KafkaConsumer::<K, V>::new(...)` returns a real
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
//!
//! `ConsumerIntegrationTest.testAsyncConsumerWithConsumerProtocolDisabled` is
//! translated as
//! `test_async_consumer_with_consumer_protocol_disabled` on its own
//! single-broker cluster with `group.version` downgraded to 0.
//!
//! `ConsumerIntegrationTest.testLeaderEpoch` is translated as
//! `test_leader_epoch` on a dedicated `Type::Kraft` cluster (3 brokers +
//! 1 isolated controller), stopping the partition leader with
//! `KafkaCluster::shutdown_broker`.
//!
//! `ConsumerIntegrationTest.testRackAwareAssignment` (added in 4.1.0 by
//! `949617b0b2`, changed in 4.4 by `90a4c93979`)
//! is NOT translated: it configures the broker with
//! `group.consumer.assignors=org.apache.kafka.clients.consumer.RackAwareTestAssignor`,
//! a Java test class on the Java test cluster's classpath that the Docker
//! broker image cannot load. What it proves on the client side — that
//! `client.rack` reaches the group coordinator over KIP-848 (the bug fixed in
//! `55cc35be`) — is covered instead by `test_client_rack_reaches_the_broker`,
//! which reads the member's rack back through `describe_consumer_groups`
//! (`MemberDescription.rackId`).
//!
//! Every test is a multilanguage scenario ([`multilanguage_consumer_test!`]):
//! it runs against the native Rust consumer and, through the gRPC backends,
//! the Python (sync, asyncio) and C consumers, each name gaining the backend
//! suffix (`__rust`, `__grpc_python`, `__grpc_python_async`, `__grpc_c`). The
//! consumers under test are bytes in, bytes out, as the gRPC backends are; the
//! tests that used a local `StringDeserializer` compare the bytes as UTF-8
//! text instead, the deserializer not being their subject. The producers and
//! admins are native fixtures. The failing listeners are the backend's logging
//! listener given a `Fail` [`ListenerAction`] step.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::DynConsumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::backend_factory::ConsumerBackendFactory;
use crate::common::callback_log::{KIND_ASSIGNED, ListenerAction, ListenerStep, ListenerTrigger};
use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::multilanguage_consumer_test;

/// The consumer under test, whatever its backend.
type BytesConsumer = Box<dyn DynConsumer<Vec<u8>, Vec<u8>>>;

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

/// The run's protocol bootstrap the backend reaches, with its security keys.
fn backend_props<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    props: &[(&str, &str)],
) -> HashMap<String, String> {
    let mut config = HashMap::from([(
        "bootstrap.servers".to_string(),
        ctx.protocol_bootstrap_servers_for(factory.needs_container_bootstrap())
            .to_string(),
    )]);
    config.extend(props.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())));
    ctx.apply_security(&mut config);
    config
}

/// The PLAINTEXT bootstrap the backend reaches (the CONTAINER listener for a
/// containerized backend, host loopback otherwise), for the tests whose Java
/// cluster is reached without the run's protocol.
fn plaintext_bootstrap_for<'a, F: ConsumerBackendFactory>(ctx: &'a TestContext, factory: &F) -> &'a str {
    if factory.needs_container_bootstrap() {
        ctx.container_bootstrap_servers()
    } else {
        ctx.bootstrap_servers()
    }
}

/// The consumer config aligned with the producer integration tests (the run's
/// selected protocol listener), with KIP-848 group protocol.
fn consumer_config<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    group_id: &str,
) -> HashMap<String, String> {
    backend_props(
        ctx,
        factory,
        &[
            ("group.id", group_id),
            ("group.protocol", "consumer"),
            ("auto.offset.reset", "earliest"),
            ("client.id", "integration-test-consumer"),
            ("enable.auto.commit", "false"),
        ],
    )
}

/// Variant of [`consumer_config`] without a `group.id` — for the
/// assignment-only test below.
fn consumer_config_groupless<F: ConsumerBackendFactory>(ctx: &TestContext, factory: &F) -> HashMap<String, String> {
    backend_props(
        ctx,
        factory,
        &[
            ("group.protocol", "consumer"),
            ("auto.offset.reset", "earliest"),
            ("client.id", "integration-test-consumer-noassign"),
            ("enable.auto.commit", "false"),
        ],
    )
}

/// The backend's consumer over `config`.
async fn create_consumer<F: ConsumerBackendFactory>(factory: &F, config: HashMap<String, String>) -> BytesConsumer {
    factory
        .create(config)
        .await
        .unwrap_or_else(|e| panic!("{} backend: KafkaConsumer::new should succeed: {e}", factory.name()))
}

/// Build a `ProducerConfig` matching the existing producer integration
/// test pattern (acks=all, short linger/max-block to avoid hangs).
fn make_producer_config(ctx: &TestContext) -> ProducerConfig {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.protocol_bootstrap_servers().to_string()),
        ("client.id".to_string(), "integration-test-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
    ]);
    ctx.apply_security(&mut props);
    ProducerConfig::new(&props).expect("invalid producer test config")
}

/// Produce `count` records with deterministic keys `k0..k{count-1}` and
/// values `v0..v{count-1}` to the given topic, blocking on each ack so
/// the consumer is guaranteed to see them. Producer is closed on exit.
async fn produce_deterministic_records(ctx: &TestContext, topic: &str, count: usize) {
    let producer: KafkaProducer<String, String> = KafkaProducer::new(
        make_producer_config(ctx),
        StringSerializer::default(),
        StringSerializer::default(),
    )
    .expect("Failed to build test producer");

    for i in 0..count {
        let rec = ProducerRecord::with_key(topic.to_string(), Some(format!("k{}", i)), Some(format!("v{}", i)));
        let future = producer.send(rec).await.expect("send should succeed");
        future
            .get_with_timeout(Duration::from_secs(30))
            .await
            .expect("produce should succeed");
    }
    producer.close().await.expect("producer close should succeed");
}

/// The UTF-8 text of a record key or value. The consumers under test are
/// bytes in, bytes out (the serde is not these tests' subject), so the
/// records are compared as the strings the producer serialized.
fn text(bytes: Option<&Vec<u8>>, what: &str) -> String {
    let bytes = bytes.unwrap_or_else(|| panic!("record {what} should be present"));
    String::from_utf8(bytes.clone()).unwrap_or_else(|e| panic!("record {what} is not UTF-8: {e}"))
}

/// Test: subscribe to a freshly-created topic, produce 10 records via
/// the existing `KafkaProducer`, poll the consumer until 10 records
/// have been collected, then assert every record carries the expected
/// key/value/topic/partition/offset.
async fn subscribe_and_poll_records<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("subscribe_poll");
    let group_id = ctx.group_id("g1");

    produce_deterministic_records(ctx, &topic, 10).await;

    let mut consumer = create_consumer(factory, consumer_config(ctx, factory, &group_id)).await;
    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));

    let mut collected: Vec<(String, String, i32, i64)> = Vec::new();
    let start = Instant::now();
    while collected.len() < 10 && start.elapsed() < Duration::from_secs(30) {
        let records = consumer
            .poll(Duration::from_secs(5))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
        for record in &records {
            let key = text(record.key(), "key");
            let value = text(record.value(), "value");
            assert_eq!(
                record.topic(),
                topic,
                "{backend} backend: record topic should match subscribed topic"
            );
            assert!(record.partition() >= 0, "{backend} backend: partition should be non-negative");
            collected.push((key, value, record.partition(), record.offset()));
        }
    }

    assert_eq!(
        collected.len(),
        10,
        "{backend} backend: should have polled all 10 records within the deadline, got {}",
        collected.len()
    );

    let mut produced_keys: Vec<String> = (0..10).map(|i| format!("k{}", i)).collect();
    let mut collected_keys: Vec<String> = collected.iter().map(|(k, _, _, _)| k.clone()).collect();
    produced_keys.sort();
    collected_keys.sort();
    assert_eq!(
        produced_keys, collected_keys,
        "{backend} backend: every produced key should appear exactly once"
    );

    for (k, v, _, _) in &collected {
        let n_from_k: i32 = k.strip_prefix('k').and_then(|s| s.parse().ok()).expect("key should be kN");
        let n_from_v: i32 = v.strip_prefix('v').and_then(|s| s.parse().ok()).expect("value should be vN");
        assert_eq!(n_from_k, n_from_v, "{backend} backend: key kN must pair with value vN");
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
                "{backend} backend: offsets in partition {} should be strictly monotonic in arrival order",
                p
            );
        }
    }

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

/// Test: explicit-assignment consumer (no `group.id`) reads records
/// from `partition=0`. Verifies the production ctor wiring builds a
/// groupless consumer (no membership / heartbeat / commit managers in
/// `RequestManagers` — `coordinator`/`commit`/`consumer_heartbeat` are
/// `None` when `group.id` is absent) and that the fetch path still
/// works without going through the membership state machine.
async fn assign_partitions_and_poll<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("assign_poll");

    produce_deterministic_records(ctx, &topic, 5).await;

    let mut consumer = create_consumer(factory, consumer_config_groupless(ctx, factory)).await;

    let tp = TopicPartition::new(topic.clone(), 0);
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

    let mut collected: Vec<(String, String)> = Vec::new();
    let start = Instant::now();
    while collected.len() < 5 && start.elapsed() < Duration::from_secs(30) {
        let records = consumer
            .poll(Duration::from_secs(5))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
        for record in &records {
            let key = text(record.key(), "key");
            let value = text(record.value(), "value");
            assert_eq!(record.topic(), topic, "{backend} backend");
            assert_eq!(record.partition(), 0, "{backend} backend: all records should be on partition 0");
            collected.push((key, value));
        }
    }

    assert_eq!(
        collected.len(),
        5,
        "{backend} backend: should have polled all 5 records, got {}",
        collected.len()
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

/// Test: consumer 1 subscribes in group `G1`, polls 5 records,
/// `commit_sync().await`, closes. Consumer 2 in the same `G1`, polls,
/// asserts the remaining records (offsets 5..10) are visible from the
/// committed offset (not re-fetched from offset 0).
async fn commit_sync_then_resume_in_same_group<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("commit_resume");
    let group_id = ctx.group_id("g_resume");

    produce_deterministic_records(ctx, &topic, 10).await;

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
        let mut consumer1_props = consumer_config(ctx, factory, &group_id);
        consumer1_props.insert("max.poll.records".to_string(), "5".to_string());
        let mut consumer1 = create_consumer(factory, consumer1_props).await;

        consumer1
            .subscribe_with_topics(vec![topic.clone()])
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));

        let mut collected = 0;
        let start = Instant::now();
        while collected < 5 && start.elapsed() < Duration::from_secs(30) {
            let records = consumer1
                .poll(Duration::from_secs(5))
                .await
                .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
            collected += records.count();
        }
        assert!(
            collected >= 5,
            "{backend} backend: consumer1 should have polled at least 5 records"
        );

        consumer1
            .commit_sync()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: commit_sync should succeed: {e}"));
        consumer1
            .close()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: consumer1 close should succeed: {e}"));
    }

    // Give the broker a moment to fully process consumer1's LeaveGroup
    // before consumer2 joins the same group. Without this, consumer2 may
    // race the LeaveGroup completion at the broker and either (a) join a
    // stale assignment generation or (b) wait out the full session
    // timeout (45s by default) before being assigned partitions.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Consumer 2: same group, polls only the remaining records.
    {
        let mut consumer2 = create_consumer(factory, consumer_config(ctx, factory, &group_id)).await;

        consumer2
            .subscribe_with_topics(vec![topic.clone()])
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));

        let mut total = 0;
        let start = Instant::now();
        // 60s is generous — KIP-848 rebalance typically completes well
        // within 10s, but consumer2 in the same group may still wait
        // out part of consumer1's session timeout if LeaveGroup at
        // close races partition reassignment broker-side.
        while start.elapsed() < Duration::from_secs(60) {
            let records = consumer2
                .poll(Duration::from_secs(5))
                .await
                .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
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
            "{backend} backend: consumer2 should see remaining records (>0 and <=5), saw {}",
            total
        );

        consumer2
            .close()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: consumer2 close should succeed: {e}"));
    }
}

/// Test: subscribe, poll a few records to establish position, then
/// `seek_to_beginning(&assigned)`, poll again, assert all records can
/// be re-read from offset 0.
async fn seek_to_beginning_re_reads_records<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("seek_to_beginning");
    let group_id = ctx.group_id("g_seek");

    produce_deterministic_records(ctx, &topic, 5).await;

    let mut consumer = create_consumer(factory, consumer_config(ctx, factory, &group_id)).await;
    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));

    // First poll: wait for assignment to land, then drain at least one
    // batch to establish position.
    let mut first_pass_count = 0;
    let start = Instant::now();
    while first_pass_count < 5 && start.elapsed() < Duration::from_secs(30) {
        let records = consumer
            .poll(Duration::from_secs(5))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
        first_pass_count += records.count();
    }
    assert_eq!(first_pass_count, 5, "{backend} backend: first pass should drain all 5 records");

    // Seek every assigned partition back to the start.
    let assigned: Vec<TopicPartition> = consumer.assignment().into_iter().collect();
    assert!(
        !assigned.is_empty(),
        "{backend} backend: seek_to_beginning requires non-empty assignment"
    );
    consumer
        .seek_to_beginning(&assigned)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek_to_beginning should succeed: {e}"));

    // Second pass: same 5 records arrive again.
    let mut second_pass_count = 0;
    let restart = Instant::now();
    while second_pass_count < 5 && restart.elapsed() < Duration::from_secs(30) {
        let records = consumer
            .poll(Duration::from_secs(5))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
        second_pass_count += records.count();
    }
    assert_eq!(
        second_pass_count, 5,
        "{backend} backend: second pass after seek_to_beginning should re-read all 5 records"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

// ── Failed-listener recovery (ConsumerIntegrationTest) ─────────────────
//
// Translated from
// `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/ConsumerIntegrationTest.java`
// (Apache Kafka 4.2). KIP-848 (`GroupProtocol.CONSUMER`) arm only; the
// `*WithGroupProtocolClassic` twins are OUT_OF_SCOPE per
// `consumer-threading.md` §20. These listeners throw from
// `on_partitions_assigned` and need NO consumer reentrancy. The listener is
// the backend's logging listener given a `Fail` step: the method fails with a
// LocalIllegalState error, the class the Rust tests have always returned
// (Java's listeners throw `IllegalArgumentException`).

/// Translates Java's
/// `testFetchPartitionsAfterFailedListenerWithGroupProtocolConsumer`.
///
/// The listener throws on the first `onPartitionsAssigned` only (Java's
/// `count == 1 → throw IllegalArgumentException("temporary error")`); the
/// consumer must recover and still deliver the single produced record. We
/// poll (tolerating the first error surfaced by the failed callback) until
/// one record arrives.
async fn fetch_partitions_after_failed_listener<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("failed_listener");
    let group_id = ctx.group_id("g_failed_listener");

    produce_deterministic_records(ctx, &topic, 1).await;

    let (mut consumer, log) = factory
        .create_with_callback_log(consumer_config(ctx, factory, &group_id))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: KafkaConsumer::new should succeed: {e}"));
    let action = ListenerAction::new().on_assigned(
        ListenerTrigger::FirstNCalls(1),
        vec![ListenerStep::Fail("temporary error".to_string())],
    );
    log.subscribe_with_logging_listener_action(&mut consumer, vec![topic.clone()], action)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe_with_topics_listener should succeed: {e}"));

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
                // (Java's `RequestUtils.isFatalException`, from public predicates.)
                let fatal = err.is_authentication_error()
                    || err.is_authorization_error()
                    || matches!(
                        err,
                        Error::MismatchedEndpointType(_)
                            | Error::SecurityDisabled(_)
                            | Error::UnsupportedVersion(_)
                            | Error::UnsupportedEndpointType(_)
                            | Error::UnsupportedForMessageFormat(_)
                    );
                assert!(
                    !fatal,
                    "{backend} backend: first-listener failure should be recoverable, got fatal: {err}"
                );
            },
        }
    }

    assert_eq!(
        delivered, 1,
        "{backend} backend: consumer should recover from the one-time listener failure and deliver the record"
    );
    let assigned_calls = log
        .entries()
        .await
        .expect("read consumer callback log")
        .iter()
        .filter(|e| e.kind == KIND_ASSIGNED)
        .count();
    assert!(
        assigned_calls >= 1,
        "{backend} backend: the assigned listener should have been invoked"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

/// Translates Java's
/// `testFetchPartitionsWithAlwaysFailedListenerWithGroupProtocolConsumer`.
///
/// The listener always throws (Java: `throw new
/// IllegalArgumentException("always failed")`). Java's contract: for ~3
/// seconds, each `poll(1s)` either returns 0 records (the assigned partitions
/// are never marked fetchable because the callback never completes
/// successfully) OR throws a `KafkaException("User rebalance callback throws
/// an error")`.
///
/// The listener throws a non-`KafkaException`. Java's
/// `invokeRebalanceCallbacks` runs that through `maybeWrapAsKafkaException(e,
/// "User rebalance callback throws an error")`
/// (`AsyncKafkaConsumer#invokeRebalanceCallbacks`), so `poll()` surfaces a
/// `KafkaException` whose `getMessage()` is exactly `"User rebalance callback
/// throws an error"`. This test asserts BOTH:
///   1. **no successful poll ever returns a record** (the always-failing
///      callback keeps every assigned partition un-fetchable);
///   2. when an error IS surfaced, its message is exactly Java's
///      `"User rebalance callback throws an error"`.
async fn fetch_partitions_with_always_failed_listener<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("always_failed_listener");
    let group_id = ctx.group_id("g_always_failed_listener");

    produce_deterministic_records(ctx, &topic, 1).await;

    let (mut consumer, log) = factory
        .create_with_callback_log(consumer_config(ctx, factory, &group_id))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: KafkaConsumer::new should succeed: {e}"));
    let action = ListenerAction::new()
        .on_assigned(ListenerTrigger::Always, vec![ListenerStep::Fail("always failed".to_string())]);
    log.subscribe_with_logging_listener_action(&mut consumer, vec![topic.clone()], action)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe_with_topics_listener should succeed: {e}"));

    // Java loops for 3s asserting `poll().count() == 0` (or catching the
    // callback KafkaException). Mirror that window.
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(3) {
        match consumer.poll(Duration::from_secs(1)).await {
            Ok(records) => {
                assert_eq!(
                    records.count(),
                    0,
                    "{backend} backend: an always-failing assigned-listener must keep all partitions \
                     un-fetchable; poll should never return records"
                );
            },
            Err(err) => {
                // Java's optional `catch (KafkaException ex)` arm of
                // org.apache.kafka.clients.consumer.ConsumerIntegrationTest#testFetchPartitionsWithAlwaysFailedListener
                // asserts `assertEquals("User rebalance callback throws an error",
                // ex.getMessage())`. Pin it exactly — a regression that masked the
                // callback path with an unrelated error (timeout, connection)
                // would fail. Java catches only `KafkaException`: any other
                // class escapes and fails the test.
                assert!(
                    err.is_kafka_error(),
                    "{backend} backend: a surfaced poll error must be a KafkaException, got: {err:?}"
                );
                assert_eq!(
                    err.message(),
                    "User rebalance callback throws an error",
                    "{backend} backend: a surfaced poll error must be the wrapped rebalance-callback error, got: {err}"
                );
            },
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

/// `AbstractHeartbeatRequestManager.CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG` — the
/// crate constant is `pub(crate)`, so the exact Java text is pinned here.
const CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG: &str = "The cluster does not support the new CONSUMER group protocol. \
     Set group.protocol=classic on the consumer configs to revert to the CLASSIC protocol \
     until the cluster is upgraded.";

/// Cluster for [`async_consumer_with_consumer_protocol_disabled`]: Java's
/// `serverProperties` (`offsets.topic.num.partitions=1`,
/// `offsets.topic.replication.factor=1`) on one broker.
///
/// Java also sets `@ClusterFeature(feature = GROUP_VERSION, version = 0)`, which
/// formats the cluster with `group.version=0`. The pooled `apache/kafka` image
/// is formatted by its stock entrypoint with no feature overrides, so the test
/// downgrades `group.version` to 0 through `Admin::update_features` instead —
/// the same end state (`KafkaApis.isConsumerGroupProtocolEnabled()` is false, so
/// every `ConsumerGroupHeartbeat` is answered `UNSUPPORTED_VERSION`). Because
/// that mutates cluster-wide state, this property set must stay unique to this
/// test so the pool never hands the downgraded container to another suite.
fn consumer_protocol_disabled_cluster_config() -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS".to_string(), "1".to_string());
    props.insert("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), "1".to_string());
    ClusterConfig::with_properties(props)
}

/// Translates `ConsumerIntegrationTest#testAsyncConsumerWithConsumerProtocolDisabled`.
///
/// With the consumer rebalance protocol disabled on the cluster, polling a
/// `group.protocol=consumer` consumer must eventually fail with an
/// `UnsupportedVersionException` whose message is exactly
/// `CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG`.
async fn async_consumer_with_consumer_protocol_disabled<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    use confluent_kafka::admin::AdminClient;
    use confluent_kafka::admin::AdminClientConfig;
    use confluent_kafka::admin::FeatureUpdate;
    use confluent_kafka::admin::UpgradeType;

    use crate::common::test_utils;

    let backend = factory.name();
    let admin = AdminClient::create(
        AdminClientConfig::new(&HashMap::from([(
            "bootstrap.servers".to_string(),
            ctx.bootstrap_servers().to_string(),
        )]))
        .expect("valid admin config"),
    )
    .expect("admin client");

    // `@ClusterFeature(feature = Feature.GROUP_VERSION, version = 0)` — see
    // `consumer_protocol_disabled_cluster_config`. Idempotent: re-running on a
    // pooled, already-downgraded container is a no-op update.
    let updates = HashMap::from([(
        "group.version".to_string(),
        FeatureUpdate::new(0, UpgradeType::SafeDowngrade).expect("valid feature update"),
    )]);
    admin
        .update_features(&updates)
        .expect("update_features should enqueue")
        .all()
        .get_with_timeout(Duration::from_secs(30))
        .await
        .expect("downgrading group.version to 0 should succeed");

    let topic = ctx.topic("test-topic");
    test_utils::create_topic(admin.as_ref(), &topic, 1, 1).await;

    let props = HashMap::from([
        (
            "bootstrap.servers".to_string(),
            plaintext_bootstrap_for(ctx, factory).to_string(),
        ),
        ("group.id".to_string(), ctx.group_id("test-group")),
        ("group.protocol".to_string(), "CONSUMER".to_string()),
    ]);
    let mut consumer = create_consumer(factory, props).await;
    consumer
        .subscribe_with_topics(vec![topic])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));

    // `TestUtils.waitForCondition` (default 15 s): a successful poll keeps
    // waiting, an `UnsupportedVersionException` ends the wait iff its message
    // is exactly the protocol-not-supported text, and any other error escapes
    // the lambda and fails the test.
    let deadline = Instant::now() + Duration::from_millis(15_000);
    loop {
        match consumer.poll(Duration::from_millis(1000)).await {
            Ok(_) => {},
            Err(err) if err.is_unsupported_version_error() => {
                if err.message() == CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG {
                    break;
                }
            },
            Err(err) => panic!("{backend} backend: unexpected poll error: {err:?}"),
        }
        assert!(
            Instant::now() < deadline,
            "{backend} backend: Should get UnsupportedVersionException and how to revert to classic protocol"
        );
    }

    // try-with-resources: a close failure would fail the Java test too.
    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
    admin.close().await;
    ctx.cleanup().await;
}

// ── Leader epoch across a leader failover (ConsumerIntegrationTest) ────

/// Java's `ConsumerIntegrationTest#sendMsg`, called as
/// `sendMsg(clusterInstance, topic, sendMsgNum)`: a String-serialized producer with
/// `acks=-1` sends `key_i` / `value_i` for `i` in `0..send_msg_num`, then
/// flushes.
async fn send_msg(bootstrap: &str, topic: &str, send_msg_num: usize) {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("acks".to_string(), "-1".to_string()),
    ]);
    let producer: KafkaProducer<String, String> = KafkaProducer::new(
        ProducerConfig::new(&props).expect("valid producer config"),
        StringSerializer::default(),
        StringSerializer::default(),
    )
    .expect("Failed to build test producer");
    for i in 0..send_msg_num {
        let record = ProducerRecord::with_key(topic.to_string(), Some(format!("key_{i}")), Some(format!("value_{i}")));
        producer.send(record).await.expect("send should succeed");
    }
    producer.flush().await.expect("flush should succeed");
    producer.close().await.expect("producer close should succeed");
}

/// Java's `ClusterInstance#getLeaderBrokerId`, called as
/// `getLeaderBrokerId(topicPartition)`: the current leader of `tp` per
/// `describeTopics`.
async fn leader_broker_id(admin: &dyn confluent_kafka::admin::Admin, tp: &TopicPartition) -> i32 {
    let described = admin
        .describe_topics_with_topic_names(&[tp.topic().to_string()])
        .all_topic_names()
        .expect("described by name")
        .get()
        .await
        .expect("describeTopics should succeed");
    described
        .get(tp.topic())
        .expect("topic described")
        .partitions()
        .iter()
        .filter(|info| info.partition() == tp.partition())
        .find_map(|info| info.leader().map(|leader| leader.id()))
        .unwrap_or_else(|| panic!("Leader not found for tp {tp}"))
}

/// Polls `consumer` with `poll(1000ms)` until `msg_num` records arrived,
/// asserting every record carries leader epoch `expected_epoch`.
///
/// Java loops `while (consumed < msgNum)` with no bound; the Rust loop is
/// bounded at 60 s so a regression fails instead of hanging the suite.
async fn consume_asserting_leader_epoch(
    consumer: &mut BytesConsumer,
    backend: &str,
    msg_num: usize,
    expected_epoch: i32,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut consumed = 0;
    while consumed < msg_num {
        assert!(
            Instant::now() < deadline,
            "{backend} backend: consumed only {consumed} of {msg_num} records with leader epoch {expected_epoch}"
        );
        let records = consumer
            .poll(Duration::from_millis(1000))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
        for record in &records {
            assert_eq!(
                record.leader_epoch(),
                Some(expected_epoch),
                "{backend} backend: record at offset {} has the wrong leader epoch",
                record.offset()
            );
        }
        consumed += records.count();
    }
}

/// Translates `ConsumerIntegrationTest#testLeaderEpoch`, on the
/// Java shape `@ClusterTest(types = {Type.KRAFT}, brokers = 3)` — a dedicated
/// cluster with an isolated controller, since the test stops a broker.
///
/// Records written under the first leader carry leader epoch 0; after the
/// leader broker is shut down the partition fails over to the other replica,
/// and records written then carry leader epoch 1.
///
/// Java's `clusterInstance.consumer()` leaves `group.protocol` at its default;
/// the test only `assign`s, so the group protocol is never exercised, and the
/// KIP-848 `consumer` protocol is used here (`consumer-threading.md` §20).
async fn leader_epoch<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    use confluent_kafka::admin::AdminClient;
    use confluent_kafka::admin::AdminClientConfig;

    use crate::common::test_utils;

    let backend = factory.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("test-topic");
    let admin = AdminClient::create(
        AdminClientConfig::new(&HashMap::from([("bootstrap.servers".to_string(), bootstrap.clone())]))
            .expect("valid admin config"),
    )
    .expect("admin client");
    test_utils::create_topic(admin.as_ref(), &topic, 1, 2).await;
    let msg_num = 10;
    send_msg(&bootstrap, &topic, msg_num).await;

    let group_id = ctx.group_id("group");
    let mut consumer = create_consumer(factory, consumer_config(ctx, factory, &group_id)).await;
    let target_topic_partition = TopicPartition::new(topic.clone(), 0);
    consumer
        .assign(vec![target_topic_partition.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));
    consumer
        .seek_to_beginning(std::slice::from_ref(&target_topic_partition))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seekToBeginning should succeed: {e}"));

    consume_asserting_leader_epoch(&mut consumer, backend, msg_num, 0).await;

    // make the leader epoch increment by shutdown the leader broker
    let leader = leader_broker_id(admin.as_ref(), &target_topic_partition).await;
    ctx.cluster().shutdown_broker(leader).await;

    send_msg(&bootstrap, &topic, msg_num).await;

    consume_asserting_leader_epoch(&mut consumer, backend, msg_num, 1).await;

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Replacement for `ConsumerIntegrationTest.testRackAwareAssignment` (see the
/// module doc for why that test is not translated): a KIP-848 consumer's
/// `client.rack` reaches the group coordinator on its heartbeat, so
/// `describe_consumer_groups` reports it as the member's rack
/// (`MemberDescription.rackId`). Before `55cc35be` the constructor never
/// passed the rack to the membership manager and the broker saw none.
async fn client_rack_reaches_the_broker<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    use confluent_kafka::admin::AdminClient;
    use confluent_kafka::admin::AdminClientConfig;
    use confluent_kafka::common::GroupState;

    use crate::common::test_utils;

    let backend = factory.name();
    let topic = ctx.topic("rack_topic");
    let group_id = ctx.group_id("g_rack");
    let mut admin_props =
        HashMap::from([("bootstrap.servers".to_string(), ctx.protocol_bootstrap_servers().to_string())]);
    ctx.apply_security(&mut admin_props);
    let admin =
        AdminClient::create(AdminClientConfig::new(&admin_props).expect("valid admin config")).expect("admin client");
    test_utils::create_topic(admin.as_ref(), &topic, 1, 1).await;

    let props = backend_props(
        ctx,
        factory,
        &[
            ("group.id", &group_id),
            ("group.protocol", "consumer"),
            ("enable.auto.commit", "false"),
            (ConsumerConfig::CLIENT_RACK_CONFIG, "rack0"),
        ],
    );
    let mut consumer = create_consumer(factory, props).await;
    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe: {e}"));

    let deadline = Instant::now() + Duration::from_secs(60);
    let rack = loop {
        let _ = consumer.poll(Duration::from_millis(200)).await;
        let described = admin
            .describe_consumer_groups(std::slice::from_ref(&group_id))
            .all()
            .get()
            .await;
        if let Ok(described) = described
            && let Some(desc) = described.get(&group_id)
            && desc.group_state() == GroupState::Stable
            && desc.members().len() == 1
        {
            break desc.members()[0].rack_id().map(str::to_string);
        }
        assert!(
            Instant::now() < deadline,
            "{backend} backend: group {group_id} never became Stable with one member"
        );
    };
    assert_eq!(
        Some("rack0".to_string()),
        rack,
        "{backend} backend: the broker reports the consumer's client.rack"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

multilanguage_consumer_test!(
    test_subscribe_and_poll_records,
    subscribe_and_poll_records,
    cluster_config_with_kip848()
);
multilanguage_consumer_test!(
    test_assign_partitions_and_poll,
    assign_partitions_and_poll,
    cluster_config_with_kip848()
);
multilanguage_consumer_test!(
    test_commit_sync_then_resume_in_same_group,
    commit_sync_then_resume_in_same_group,
    cluster_config_with_kip848()
);
multilanguage_consumer_test!(
    test_seek_to_beginning_re_reads_records,
    seek_to_beginning_re_reads_records,
    cluster_config_with_kip848()
);
multilanguage_consumer_test!(
    test_fetch_partitions_after_failed_listener,
    fetch_partitions_after_failed_listener,
    cluster_config_with_kip848()
);
multilanguage_consumer_test!(
    test_fetch_partitions_with_always_failed_listener,
    fetch_partitions_with_always_failed_listener,
    cluster_config_with_kip848()
);
multilanguage_consumer_test!(
    test_async_consumer_with_consumer_protocol_disabled,
    async_consumer_with_consumer_protocol_disabled,
    consumer_protocol_disabled_cluster_config()
);
multilanguage_consumer_test!(test_leader_epoch, leader_epoch, ClusterConfig::kraft_dedicated(3, 1));
multilanguage_consumer_test!(
    test_client_rack_reaches_the_broker,
    client_rack_reaches_the_broker,
    cluster_config_with_kip848()
);
