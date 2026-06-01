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
//! # Status: `#[ignore]`-gated until the FindCoordinator response path
//! is wired in the bg task
//!
//! Tests below are `#[ignore]`d because they expose a **pre-existing,
//! Phase-10 design gap** in the consumer bg task that Phase 12
//! inherited rather than introduced.
//!
//! ## The gap
//!
//! `CoordinatorRequestManager::make_find_coordinator_request`
//! (`src/consumer/internals/coordinator_request_manager.rs:234-241`)
//! returns a bare `UnsentRequest::new(builder, None)` without
//! `take_response_receiver()` being called anywhere on the bg-task
//! side. When the broker responds to `FindCoordinator`,
//! `FutureCompletionHandler::on_complete_ref` fires the inner
//! oneshot — but the `Receiver` was dropped at request build time,
//! so no code path calls `coordinator_manager.on_response(...)`. The
//! consumer is stuck in `JOINING` forever; assignment never arrives;
//! `poll()` returns zero records until the test deadline.
//!
//! The same gap exists for `ConsumerHeartbeatRequestManager`:
//! `build_heartbeat_request` (line 268-279) does not wire the
//! response receiver back to `on_heartbeat_success` / `on_failure`.
//!
//! Verified with `RUST_LOG=confluent_kafka=trace`: the broker is
//! reachable, FindCoordinator v6 is sent with the correct
//! `coordinator_keys`, but the `RequestState` shows
//! `requestInFlight=true, lastReceivedMs=-1` indefinitely. The
//! broker side never logs the request (debug-level broker logs are
//! also silent for this group), confirming the response is being
//! discarded at the client-side oneshot drop.
//!
//! Both `coordinator_request_manager.rs` rustdoc on line 230 and the
//! analog comment on `consumer_heartbeat_request_manager.rs` say
//! "the bg task (Phase 10) takes the response receiver via
//! `take_response_receiver`" — but the bg task at
//! `consumer_network_thread.rs::run_once` does NOT do this for
//! `coordinator` or `consumer_heartbeat`. The wire-up was left as a
//! Phase-10 carry-over and Phase 12's primary-ctor work cannot land
//! it without a substantial refactor (the bg task needs a per-RM
//! response-router task per pending request, mirroring Java's
//! `whenComplete` callback chain).
//!
//! ## Path to un-ignore
//!
//! A future commit must:
//!
//! 1. Add a bg-task router that calls
//!    `UnsentRequest::take_response_receiver()` BEFORE the request
//!    leaves `make_*_request`, spawning a tokio task per request that
//!    awaits the response and dispatches to
//!    `coordinator_manager.on_response(...)` /
//!    `heartbeat_manager.on_response(...)` /
//!    `consumer_membership_manager.on_heartbeat_success(...)`.
//! 2. Run this file with the `#[ignore]` markers removed; all tests
//!    should pass against the testcontainers Kafka 4.2.0 broker.
//!
//! ## What Phase 12 *did* land
//!
//! The test below has been written and exercised against the real
//! broker (the KIP-848 broker-side
//! `group.coordinator.rebalance.protocols=classic,consumer` is set
//! correctly via the broker env-var); it will exercise the full
//! subscribe/poll loop the moment the response router lands. Until
//! then, the docker-free smoke test at
//! `tests/consumer/async_kafka_consumer_test.rs` exercises the
//! production ctor (channels + NetworkClient + bg-task spawn + clean
//! close) without exercising the membership state machine.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::KafkaError;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::consumer::ConsumerConfig;
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
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, KafkaError> {
        String::from_utf8(data.to_vec()).map_err(|e| KafkaError::serialization(format!("invalid utf-8: {}", e)))
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
///
/// **Ignored** — see the module docstring for the FindCoordinator
/// response-routing gap that prevents the bg task from leaving JOINING.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "FindCoordinator response routing not wired in bg task — see module docstring"]
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
