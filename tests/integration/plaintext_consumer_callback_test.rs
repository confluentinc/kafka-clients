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

//! Integration tests translated from
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerCallbackTest.java`
//! (Apache Kafka 4.2).
//!
//! These exercise `consumer-threading.md` §31 rebalance-listener
//! reentrancy: in Java, the rebalance listener runs on the caller's task
//! and may call back into the consumer API (assign / assignment /
//! beginningOffsets / position / seek / pause) from inside the callback.
//!
//! # Structural gap (Issue 8 — Phase-13)
//!
//! A Rust rebalance listener is held as `Arc<dyn ConsumerRebalanceListener>`
//! and its methods take only `&self`. The consumer methods exercised
//! inside the Java callbacks — `assign`, `position`, `beginning_offsets`,
//! `seek`, `pause`, `resume` — are all `async fn(&mut self)` on
//! `AsyncKafkaConsumer`, and `Box<dyn Consumer>` is not `Clone`. The
//! listener therefore has no handle to call them. A channel handshake to
//! a driver task deadlocks, because per §31 the listener runs ON the
//! caller's task, which is the task currently blocked inside
//! `consumer.poll()` — the driver cannot service the request until
//! `poll()` returns, but `poll()` will not return until the listener
//! does. This is the same gap that `#[ignore]`s the poll-suite's
//! `test_async_consumer_max_poll_interval_ms_delay_in_revocation`
//! (Issue 8 in `design/history/Milestone-8/Phase-13/COMMENTS.1.md`).
//!
//! Consequently every in-callback-reentrancy test in this file is
//! translated as an `#[ignore]`d body (wired into CI, documenting the
//! gap, with the Java assertion shape preserved). The single test that
//! needs NO consumer reentrancy — `testOnPartitionsAssignedCalledWith
//! NewPartitionsOnlyForAsyncConsumer` (the listener only reads its
//! `partitions` argument) — runs.
//!
//! # Translated (KIP-848 / `GroupProtocol.CONSUMER` arm only)
//!
//! Runs:
//! - `testOnPartitionsAssignedCalledWithNewPartitionsOnlyForAsyncConsumer`
//!   (line 179) → `test_on_partitions_assigned_called_with_new_partitions_only`
//!
//! `#[ignore]`d (Issue 8 — listener calls a `&mut self` consumer method):
//! - `testAsyncConsumerRebalanceListenerAssignOnPartitionsAssigned` (66)
//!   → `test_rebalance_listener_assign_on_partitions_assigned`
//! - `testAsyncConsumerRebalanceListenerAssignmentOnPartitionsAssigned` (85)
//!   → `test_rebalance_listener_assignment_on_partitions_assigned`
//! - `testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsAssigned` (103)
//!   → `test_rebalance_listener_beginning_offsets_on_partitions_assigned`
//! - `testAsyncConsumerRebalanceListenerAssignOnPartitionsRevoked` (123)
//!   → `test_rebalance_listener_assign_on_partitions_revoked`
//! - `testAsyncConsumerRebalanceListenerAssignmentOnPartitionsRevoked` (142)
//!   → `test_rebalance_listener_assignment_on_partitions_revoked`
//! - `testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsRevoked` (154)
//!   → `test_rebalance_listener_beginning_offsets_on_partitions_revoked`
//! - `testAsyncConsumerGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback` (246)
//!   → `test_get_position_of_newly_assigned_partition_on_partitions_assigned_callback`
//! - `testAsyncConsumerSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback` (264)
//!   → `test_seek_position_and_pause_newly_assigned_partition_on_partitions_assigned_callback`
//!
//! # SKIPped (classic-protocol-only — `consumer-threading.md` §20)
//!
//! - SKIP: `testClassicConsumerRebalanceListenerAssignOnPartitionsAssigned`
//! - SKIP: `testClassicConsumerRebalanceListenerAssignmentOnPartitionsAssigned`
//! - SKIP: `testClassicConsumerRebalanceListenerBeginningOffsetsOnPartitionsAssigned`
//! - SKIP: `testClassicConsumerRebalanceListenerAssignOnPartitionsRevoked`
//! - SKIP: `testClassicConsumerRebalanceListenerAssignmentOnPartitionsRevoked`
//! - SKIP: `testClassicConsumerRebalanceListenerBeginningOffsetsOnPartitionsRevoked`
//! - SKIP: `testClassicConsumerGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback`
//! - SKIP: `testClassicConsumerSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback`
//! - SKIP: `testOnPartitionsAssignedCalledWithNewPartitionsOnlyForClassicCooperative` — classic-protocol-only
//! - SKIP: `testOnPartitionsAssignedCalledWithNewPartitionsOnlyForClassicEager` — classic-protocol-only

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;

use confluent_kafka::common::KafkaError;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::ConsumerRebalanceListener;
use confluent_kafka::consumer::new_consumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
use crate::common::test_context::TestContext;

type BytesConsumer = dyn Consumer<Vec<u8>, Vec<u8>>;

// ── Cluster config (Java: 3 brokers, KIP-848, no extra serverProperties) ─

fn cluster_config_with_kip848_3brokers() -> ClusterConfig {
    // Auto-created topics get a single partition (Java's `topic`/`newTopic`
    // both have a single relevant partition `(topic, 0)` in this suite); the
    // canonical helper supplies the shared KIP-848 broker tuning.
    kip848_3_broker(1)
}

// ── Byte-array deserializer ───────────────────────────────────────────

struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, KafkaError> {
        Ok(data.to_vec())
    }
}

// ── Config + producer helpers ─────────────────────────────────────────

/// Java's `createConsumer(protocol)` sets `group.protocol=consumer` and
/// `enable.auto.commit=false` (no `group.id` override in the Java helper,
/// so a random one is supplied per test). Default
/// `auto.offset.reset=earliest`.
fn make_consumer_config_bytes(bootstrap: &str, group_id: &str) -> ConsumerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("group.id".to_string(), group_id.to_string()),
    ]);
    ConsumerConfig::from_properties(&props).expect("invalid test config")
}

fn new_bytes_consumer(config: ConsumerConfig) -> Box<dyn Consumer<Vec<u8>, Vec<u8>>> {
    new_consumer::<Vec<u8>, Vec<u8>>(config, Box::new(ByteArrayDeserializer), Box::new(ByteArrayDeserializer))
        .expect("new_consumer should succeed")
}

fn make_producer_config(bootstrap: &str) -> ProducerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "integration-test-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "5".to_string()),
    ]);
    ProducerConfig::from_properties(&props).expect("invalid producer test config")
}

fn build_producer_bytes(bootstrap: &str) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    KafkaProducer::from_config(
        make_producer_config(bootstrap),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("Failed to build test producer")
}

/// Provision a single-partition topic (this suite only cares about
/// partition 0 of `topic` and `newTopic`).
async fn ensure_topic(producer: &KafkaProducer<Vec<u8>, Vec<u8>>, topic: &str) {
    let record = ProducerRecord::with_partition(
        topic.to_string(),
        Some(0),
        Some(b"__provisioner__".to_vec()),
        Some(b"__provisioner__".to_vec()),
    )
    .expect("ProducerRecord::with_partition should succeed");
    let fut = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(producer, record)
        .await
        .expect("provisioner send should succeed");
    fut.get_timeout(Duration::from_secs(30))
        .await
        .expect("provisioner send should ack");
    producer.flush().await.expect("producer.flush should succeed");
}

// ── Listener that records the partitions it was handed ────────────────

/// Mirrors the anonymous listener in Java's `subscribeAndExpectOn
/// PartitionsAssigned`: when `onPartitionsAssigned` is invoked with a set
/// containing all expected partitions, record the exact set passed and
/// flag completion. Needs no consumer reentrancy.
struct RecordingAssignedListener {
    expected: HashSet<TopicPartition>,
    captured: Arc<Mutex<Option<Vec<TopicPartition>>>>,
}

#[async_trait]
impl ConsumerRebalanceListener for RecordingAssignedListener {
    async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        Ok(())
    }

    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        let got: HashSet<TopicPartition> = partitions.iter().cloned().collect();
        // Java: `if (partitions.containsAll(expectedPartitionsInCallback))`.
        if self.expected.iter().all(|tp| got.contains(tp)) {
            *self.captured.lock().expect("captured mutex poisoned") = Some(partitions.to_vec());
        }
        Ok(())
    }
}

/// Drive `poll(100ms)` until `captured` is set or the deadline elapses.
async fn poll_until_captured(
    consumer: &mut BytesConsumer,
    captured: &Arc<Mutex<Option<Vec<TopicPartition>>>>,
    deadline_duration: Duration,
) -> Vec<TopicPartition> {
    let deadline = Instant::now() + deadline_duration;
    while Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
        if let Some(v) = captured.lock().expect("captured mutex poisoned").clone() {
            return v;
        }
    }
    panic!("Timed out before expected rebalance completed");
}

// ── Runnable test: KIP-848 new-partitions-only ────────────────────────

/// Translates Java's
/// `testOnPartitionsAssignedCalledWithNewPartitionsOnlyForAsyncConsumer`
/// (line 179) via `testOnPartitionsAssignedCalledWithExpectedPartitions(
/// consumer, /*expectNewPartitionsOnlyInCallback=*/ true)`.
///
/// KIP-848 (CONSUMER protocol): when the subscription is expanded to add a
/// new topic, the `onPartitionsAssigned` callback receives ONLY the newly
/// added partition, not the full assignment. This needs no consumer
/// reentrancy — the listener only reads its `partitions` argument.
#[tokio::test(flavor = "multi_thread")]
async fn test_on_partitions_assigned_called_with_new_partitions_only() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let new_topic = ctx.topic("newTopic");
    let group_id = ctx.group_id("g_new_partitions_only");
    let tp = TopicPartition::new(topic.clone(), 0);
    let added = TopicPartition::new(new_topic.clone(), 0);

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic(&producer, &topic).await;
    ensure_topic(&producer, &new_topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id));

    // First subscription: expect `tp` in the callback.
    let captured1: Arc<Mutex<Option<Vec<TopicPartition>>>> = Arc::new(Mutex::new(None));
    let listener1: Arc<dyn ConsumerRebalanceListener> = Arc::new(RecordingAssignedListener {
        expected: [tp.clone()].into_iter().collect(),
        captured: Arc::clone(&captured1),
    });
    consumer
        .subscribe_with_listener(vec![topic.clone()], listener1)
        .await
        .expect("first subscribe should succeed");
    let got1 = poll_until_captured(consumer.as_mut(), &captured1, Duration::from_secs(90)).await;
    let got1_set: HashSet<TopicPartition> = got1.iter().cloned().collect();
    assert_eq!(
        got1_set,
        [tp.clone()].into_iter().collect::<HashSet<_>>(),
        "first onPartitionsAssigned should be exactly {{tp}}"
    );
    assert_eq!(consumer.assignment(), [tp.clone()].into_iter().collect::<HashSet<_>>());

    // Expand subscription to add `newTopic`. The callback should receive
    // ONLY the newly added partition (KIP-848 incremental assignment).
    let captured2: Arc<Mutex<Option<Vec<TopicPartition>>>> = Arc::new(Mutex::new(None));
    let listener2: Arc<dyn ConsumerRebalanceListener> = Arc::new(RecordingAssignedListener {
        expected: [added.clone()].into_iter().collect(),
        captured: Arc::clone(&captured2),
    });
    consumer
        .subscribe_with_listener(vec![topic.clone(), new_topic.clone()], listener2)
        .await
        .expect("expand subscribe should succeed");
    let got2 = poll_until_captured(consumer.as_mut(), &captured2, Duration::from_secs(90)).await;
    let got2_set: HashSet<TopicPartition> = got2.iter().cloned().collect();
    assert_eq!(
        got2_set,
        [added.clone()].into_iter().collect::<HashSet<_>>(),
        "expand onPartitionsAssigned should be exactly the newly-added partition {{added}}, \
         got {got2:?}"
    );
    // The full assignment now contains both partitions.
    assert_eq!(
        consumer.assignment(),
        [tp.clone(), added.clone()].into_iter().collect::<HashSet<_>>()
    );

    consumer.close().await.expect("consumer close should succeed");
}

// ── #[ignore]d in-callback-reentrancy tests (Issue 8) ─────────────────
//
// Each of the following mirrors a Java callback-reentrancy test whose
// listener calls a `&mut self` consumer method. They are `#[ignore]`d for
// the structural reason documented at the top of this file. The bodies
// are intentionally minimal stubs that fail loudly if ever un-ignored
// without the structural gap being closed first — they are NOT meant to
// pass as written; they exist to keep the Java test inventory traceable
// and wired into CI.

const ISSUE_8: &str = "Issue 8 (Phase-13): a Rust rebalance listener holds only `&self` \
                       (as `Arc<dyn ConsumerRebalanceListener>`) and cannot call the \
                       consumer's `&mut self` methods (assign/position/beginning_offsets/\
                       seek/pause) from inside the callback; the §31 same-task contract \
                       deadlocks a channel handshake. Structurally unsupported.";

/// Java `testAsyncConsumerRebalanceListenerAssignOnPartitionsAssigned`
/// (line 66): `assign()` inside `onPartitionsAssigned` throws
/// `IllegalState` "Subscription to topics, partitions and pattern are
/// mutually exclusive". Requires the listener to call `consumer.assign()`
/// (Issue 8).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Issue 8: listener calls consumer.assign() inside the callback — \
            see module-level docs and design/history/Milestone-8/Phase-13/COMMENTS.1.md"]
async fn test_rebalance_listener_assign_on_partitions_assigned() {
    panic!("{ISSUE_8}");
}

/// Java `testAsyncConsumerRebalanceListenerAssignmentOnPartitionsAssigned`
/// (line 85): `assignment()` inside the callback contains tp. Requires a
/// listener-side handle to the consumer (Issue 8).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Issue 8: listener calls consumer.assignment() inside the callback"]
async fn test_rebalance_listener_assignment_on_partitions_assigned() {
    panic!("{ISSUE_8}");
}

/// Java `testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsAssigned`
/// (line 103): `beginningOffsets()` inside the callback (Issue 8).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Issue 8: listener calls consumer.beginning_offsets() inside the callback"]
async fn test_rebalance_listener_beginning_offsets_on_partitions_assigned() {
    panic!("{ISSUE_8}");
}

/// Java `testAsyncConsumerRebalanceListenerAssignOnPartitionsRevoked`
/// (line 123): `assign()` inside `onPartitionsRevoked` throws (Issue 8).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Issue 8: listener calls consumer.assign() inside the revoked callback"]
async fn test_rebalance_listener_assign_on_partitions_revoked() {
    panic!("{ISSUE_8}");
}

/// Java `testAsyncConsumerRebalanceListenerAssignmentOnPartitionsRevoked`
/// (line 142): `assignment()` inside revoked contains tp (Issue 8).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Issue 8: listener calls consumer.assignment() inside the revoked callback"]
async fn test_rebalance_listener_assignment_on_partitions_revoked() {
    panic!("{ISSUE_8}");
}

/// Java `testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsRevoked`
/// (line 154): `beginningOffsets()` inside revoked (Issue 8).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Issue 8: listener calls consumer.beginning_offsets() inside the revoked callback"]
async fn test_rebalance_listener_beginning_offsets_on_partitions_revoked() {
    panic!("{ISSUE_8}");
}

/// Java `testAsyncConsumerGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback`
/// (line 246): `position()` inside the assigned callback does not throw
/// (Issue 8).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Issue 8: listener calls consumer.position() inside the callback"]
async fn test_get_position_of_newly_assigned_partition_on_partitions_assigned_callback() {
    panic!("{ISSUE_8}");
}

/// Java `testAsyncConsumerSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback`
/// (line 264): `seek()` + `pause()` inside the assigned callback, then
/// resume + consume. The only place pause/resume is exercised in the Java
/// integration callback suite — and it requires `&mut self` consumer
/// calls from the listener (Issue 8).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Issue 8: listener calls consumer.seek()+pause() inside the callback"]
async fn test_seek_position_and_pause_newly_assigned_partition_on_partitions_assigned_callback() {
    panic!("{ISSUE_8}");
}
