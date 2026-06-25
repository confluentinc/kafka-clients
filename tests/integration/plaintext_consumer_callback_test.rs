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
//! # Issue 8 closed by Phase 41 (`ConsumerHandle`)
//!
//! A Rust rebalance listener is held as `Arc<dyn ConsumerRebalanceListener>`
//! and its methods take only `&self`; the consumer ops exercised inside the
//! Java callbacks (`assign` / `position` / `beginning_offsets` / `seek` /
//! `pause` / `resume`) are `async fn(&mut self)` on `AsyncKafkaConsumer`,
//! and `Box<dyn Consumer>` is not `Clone`. Phase 41 closes this:
//!   1. The user captures a `Clone + Send + Sync` [`ConsumerHandle`]
//!      (`consumer.handle()`) into the listener struct — the Rust
//!      equivalent of Java capturing the `consumer` variable. The handle
//!      exposes the reentrant-safe ops; the listener trait stays
//!      Java-identical (`&self` + partitions).
//!   2. The bg loop no longer freezes during the callback (Phase 41b), so a
//!      reentrant op that routes through the bg task completes instead of
//!      deadlocking.
//!
//! Every in-callback-reentrancy test below is therefore now a real test
//! (the previous `#[ignore]`d stubs are gone), keeping the Java assertion
//! shape. These are integration tests (Docker-gated like the rest of the
//! suite; compile-verified locally, run in CI).
//!
//! # Translated (KIP-848 / `GroupProtocol.CONSUMER` arm only)
//!
//! Runs:
//! - `testOnPartitionsAssignedCalledWithNewPartitionsOnlyForAsyncConsumer`
//!   (line 179) → `test_on_partitions_assigned_called_with_new_partitions_only`
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

use std::collections::BTreeMap;
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
use confluent_kafka::consumer::ConsumerHandle;
use confluent_kafka::consumer::ConsumerRebalanceListener;
use confluent_kafka::consumer::new_consumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

type BytesConsumer = dyn Consumer<Vec<u8>, Vec<u8>>;

// ── Cluster config (Java: 3 brokers, KIP-848, no extra serverProperties) ─

fn cluster_config_with_kip848_3brokers() -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert(
        "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS".to_string(),
        "classic,consumer".to_string(),
    );
    props.insert("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), "3".to_string());
    props.insert("KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS".to_string(), "500".to_string());
    props.insert("KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS".to_string(), "500".to_string());
    props.insert("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS".to_string(), "10".to_string());
    // Auto-created topics get a single partition (Java's `topic`/`newTopic`
    // both have a single relevant partition `(topic, 0)` in this suite).
    props.insert("KAFKA_NUM_PARTITIONS".to_string(), "1".to_string());
    let mut cfg = ClusterConfig::with_brokers(3);
    cfg.server_properties = props;
    cfg
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

// ── In-callback-reentrancy tests (Issue 8 — closed by Phase 41) ───────
//
// Each listener captures a `ConsumerHandle` (the Rust equivalent of Java
// capturing the `consumer` variable) and, on the matching callback for the
// test partition, runs the op the Java test runs and records its outcome.
// The harness then asserts the recorded outcome matches Java's assertion.

/// The reentrant action a listener runs against its captured handle, and
/// where it records the result for the harness to assert.
enum CallbackAction {
    /// Java: `assertThrows(IllegalStateException, () -> consumer.assign(...))`
    /// then assert the exact message. Records the resulting error.
    Assign {
        tp: TopicPartition,
        result: Arc<Mutex<Option<Result<(), KafkaError>>>>,
    },
    /// Java: `assertTrue(consumer.assignment().contains(tp))`. Records the
    /// `assignment()` set seen from inside the callback.
    Assignment {
        captured_assignment: Arc<Mutex<Option<HashSet<TopicPartition>>>>,
    },
    /// Java: `consumer.beginningOffsets([tp])` → `map.get(tp) == 0`. Records
    /// the returned map.
    BeginningOffsets {
        tp: TopicPartition,
        captured: Arc<Mutex<Option<HashMap<TopicPartition, i64>>>>,
    },
    /// Java: `assertDoesNotThrow(() -> consumer.position(tp))`. Records the
    /// position result.
    Position {
        tp: TopicPartition,
        result: Arc<Mutex<Option<Result<i64, KafkaError>>>>,
    },
    /// Java: `consumer.seek(tp, offset); consumer.pause([tp])`. Records the
    /// combined result.
    SeekAndPause {
        tp: TopicPartition,
        offset: i64,
        result: Arc<Mutex<Option<Result<(), KafkaError>>>>,
    },
}

impl CallbackAction {
    async fn run(&self, handle: &ConsumerHandle) {
        match self {
            CallbackAction::Assign { tp, result } => {
                let r = handle.assign(vec![tp.clone()]).await;
                *result.lock().expect("result mutex") = Some(r);
            },
            CallbackAction::Assignment { captured_assignment } => {
                *captured_assignment.lock().expect("assignment mutex") = Some(handle.assignment());
            },
            CallbackAction::BeginningOffsets { tp, captured } => {
                let map = handle
                    .beginning_offsets(std::slice::from_ref(tp))
                    .await
                    .expect("beginning_offsets should succeed");
                *captured.lock().expect("offsets mutex") = Some(map);
            },
            CallbackAction::Position { tp, result } => {
                let r = handle.position(tp).await;
                *result.lock().expect("position mutex") = Some(r);
            },
            CallbackAction::SeekAndPause { tp, offset, result } => {
                let r = async {
                    handle.seek(tp.clone(), *offset).await?;
                    handle.pause(std::slice::from_ref(tp)).await
                }
                .await;
                *result.lock().expect("seek+pause mutex") = Some(r);
            },
        }
    }
}

/// Listener that runs a [`CallbackAction`] against a captured
/// [`ConsumerHandle`] on the targeted method, once `partitions` contains
/// `tp`. Mirrors Java's anonymous listener that captures `consumer` and
/// calls `execute.accept(consumer, partitions)`.
struct ReentrantListener {
    handle: ConsumerHandle,
    tp: TopicPartition,
    /// `true` to run the action on `onPartitionsAssigned`, `false` on
    /// `onPartitionsRevoked`.
    on_assigned: bool,
    action: CallbackAction,
    /// Set once the action has run (Java's `partitionsAssigned`/`Revoked`
    /// `AtomicBoolean`).
    done: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl ConsumerRebalanceListener for ReentrantListener {
    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        if self.on_assigned && partitions.contains(&self.tp) {
            self.action.run(&self.handle).await;
            self.done.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }

    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        if !self.on_assigned && partitions.contains(&self.tp) {
            self.action.run(&self.handle).await;
            self.done.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }
}

/// Java's `triggerOnPartitionsAssigned(tp, consumer, execute)`: subscribe
/// with a listener that runs `action` from inside `onPartitionsAssigned`
/// once `tp` is assigned, then poll until the action has run.
async fn trigger_on_partitions_assigned(
    consumer: &mut BytesConsumer,
    topic: &str,
    tp: &TopicPartition,
    action: CallbackAction,
) {
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let listener: Arc<dyn ConsumerRebalanceListener> = Arc::new(ReentrantListener {
        handle: consumer.handle(),
        tp: tp.clone(),
        on_assigned: true,
        action,
        done: Arc::clone(&done),
    });
    consumer
        .subscribe_with_listener(vec![topic.to_string()], listener)
        .await
        .expect("subscribe should succeed");
    poll_until_flag(consumer, &done, Duration::from_secs(90)).await;
}

/// Java's `triggerOnPartitionsRevoked(tp, protocol, execute)`: subscribe a
/// listener that runs `action` from inside `onPartitionsRevoked` once `tp`
/// is revoked. Poll until assigned, then `unsubscribe()` to force the
/// revocation, then assert the action ran.
async fn trigger_on_partitions_revoked(
    consumer: &mut BytesConsumer,
    topic: &str,
    tp: &TopicPartition,
    action: CallbackAction,
) {
    let assigned = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let revoked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let listener: Arc<dyn ConsumerRebalanceListener> = Arc::new(RevokeTrackingListener {
        handle: consumer.handle(),
        tp: tp.clone(),
        action,
        assigned: Arc::clone(&assigned),
        revoked: Arc::clone(&revoked),
    });
    consumer
        .subscribe_with_listener(vec![topic.to_string()], listener)
        .await
        .expect("subscribe should succeed");
    poll_until_flag(consumer, &assigned, Duration::from_secs(90)).await;

    // Force a revocation: unsubscribe drives `onPartitionsRevoked`. Poll a
    // few times so the bg loop drives the reconcile + the app side drains
    // the callback.
    consumer.unsubscribe().await.expect("unsubscribe should succeed");
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && !revoked.load(std::sync::atomic::Ordering::SeqCst) {
        let _ = consumer.poll(Duration::from_millis(100)).await;
    }
    assert!(
        revoked.load(std::sync::atomic::Ordering::SeqCst),
        "onPartitionsRevoked must have run the reentrant action"
    );
}

/// Listener for the revoked-callback tests: tracks assignment (so the
/// harness knows when to unsubscribe) and runs `action` on revocation.
struct RevokeTrackingListener {
    handle: ConsumerHandle,
    tp: TopicPartition,
    action: CallbackAction,
    assigned: Arc<std::sync::atomic::AtomicBool>,
    revoked: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl ConsumerRebalanceListener for RevokeTrackingListener {
    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        if partitions.contains(&self.tp) {
            self.assigned.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }

    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        if partitions.contains(&self.tp) {
            self.action.run(&self.handle).await;
            self.revoked.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }
}

/// Drive `poll(100ms)` until `flag` is set or the deadline elapses.
async fn poll_until_flag(
    consumer: &mut BytesConsumer,
    flag: &Arc<std::sync::atomic::AtomicBool>,
    deadline_duration: Duration,
) {
    let deadline = Instant::now() + deadline_duration;
    while Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
        if flag.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
    }
    panic!("Timed out before expected rebalance callback ran");
}

const MUTUALLY_EXCLUSIVE_MSG: &str = "Subscription to topics, partitions and pattern are mutually exclusive";

/// Java `testAsyncConsumerRebalanceListenerAssignOnPartitionsAssigned`
/// (line 66): `assign()` inside `onPartitionsAssigned` throws
/// `IllegalState` "Subscription to topics, partitions and pattern are
/// mutually exclusive".
#[tokio::test(flavor = "multi_thread")]
async fn test_rebalance_listener_assign_on_partitions_assigned() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_assign_on_assigned");
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic(&producer, &topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id));
    let result: Arc<Mutex<Option<Result<(), KafkaError>>>> = Arc::new(Mutex::new(None));
    trigger_on_partitions_assigned(
        consumer.as_mut(),
        &topic,
        &tp,
        CallbackAction::Assign { tp: tp.clone(), result: Arc::clone(&result) },
    )
    .await;

    let err = result
        .lock()
        .unwrap()
        .take()
        .expect("action ran")
        .expect_err("assign() must fail inside callback");
    assert!(
        matches!(&err, KafkaError::IllegalState(msg) if msg == MUTUALLY_EXCLUSIVE_MSG),
        "expected IllegalState '{MUTUALLY_EXCLUSIVE_MSG}', got {err:?}"
    );
    consumer.close().await.expect("consumer close should succeed");
}

/// Java `testAsyncConsumerRebalanceListenerAssignmentOnPartitionsAssigned`
/// (line 85): `assignment()` inside the callback contains tp.
#[tokio::test(flavor = "multi_thread")]
async fn test_rebalance_listener_assignment_on_partitions_assigned() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_assignment_on_assigned");
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic(&producer, &topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id));
    let captured: Arc<Mutex<Option<HashSet<TopicPartition>>>> = Arc::new(Mutex::new(None));
    trigger_on_partitions_assigned(
        consumer.as_mut(),
        &topic,
        &tp,
        CallbackAction::Assignment { captured_assignment: Arc::clone(&captured) },
    )
    .await;

    let assignment = captured.lock().unwrap().take().expect("action ran");
    assert!(
        assignment.contains(&tp),
        "assignment() inside the callback must contain {tp}, got {assignment:?}"
    );
    consumer.close().await.expect("consumer close should succeed");
}

/// Java `testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsAssigned`
/// (line 103): `beginningOffsets([tp])` inside the callback → `get(tp) == 0`.
#[tokio::test(flavor = "multi_thread")]
async fn test_rebalance_listener_beginning_offsets_on_partitions_assigned() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_begin_on_assigned");
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic(&producer, &topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id));
    let captured: Arc<Mutex<Option<HashMap<TopicPartition, i64>>>> = Arc::new(Mutex::new(None));
    trigger_on_partitions_assigned(
        consumer.as_mut(),
        &topic,
        &tp,
        CallbackAction::BeginningOffsets { tp: tp.clone(), captured: Arc::clone(&captured) },
    )
    .await;

    let map = captured.lock().unwrap().take().expect("action ran");
    assert!(map.contains_key(&tp), "beginningOffsets must contain {tp}");
    assert_eq!(map.get(&tp), Some(&0), "beginningOffsets({tp}) must be 0");
    consumer.close().await.expect("consumer close should succeed");
}

/// Java `testAsyncConsumerRebalanceListenerAssignOnPartitionsRevoked`
/// (line 123): `assign()` inside `onPartitionsRevoked` throws IllegalState.
#[tokio::test(flavor = "multi_thread")]
async fn test_rebalance_listener_assign_on_partitions_revoked() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_assign_on_revoked");
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic(&producer, &topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id));
    let result: Arc<Mutex<Option<Result<(), KafkaError>>>> = Arc::new(Mutex::new(None));
    trigger_on_partitions_revoked(
        consumer.as_mut(),
        &topic,
        &tp,
        CallbackAction::Assign { tp: tp.clone(), result: Arc::clone(&result) },
    )
    .await;

    let err = result
        .lock()
        .unwrap()
        .take()
        .expect("action ran")
        .expect_err("assign() must fail inside callback");
    assert!(
        matches!(&err, KafkaError::IllegalState(msg) if msg == MUTUALLY_EXCLUSIVE_MSG),
        "expected IllegalState '{MUTUALLY_EXCLUSIVE_MSG}', got {err:?}"
    );
    consumer.close().await.expect("consumer close should succeed");
}

/// Java `testAsyncConsumerRebalanceListenerAssignmentOnPartitionsRevoked`
/// (line 142): `assignment()` inside revoked contains tp.
#[tokio::test(flavor = "multi_thread")]
async fn test_rebalance_listener_assignment_on_partitions_revoked() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_assignment_on_revoked");
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic(&producer, &topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id));
    let captured: Arc<Mutex<Option<HashSet<TopicPartition>>>> = Arc::new(Mutex::new(None));
    trigger_on_partitions_revoked(
        consumer.as_mut(),
        &topic,
        &tp,
        CallbackAction::Assignment { captured_assignment: Arc::clone(&captured) },
    )
    .await;

    let assignment = captured.lock().unwrap().take().expect("action ran");
    assert!(
        assignment.contains(&tp),
        "assignment() inside the revoked callback must contain {tp}, got {assignment:?}"
    );
    consumer.close().await.expect("consumer close should succeed");
}

/// Java `testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsRevoked`
/// (line 154): `beginningOffsets([tp])` inside revoked → `get(tp) == 0`.
#[tokio::test(flavor = "multi_thread")]
async fn test_rebalance_listener_beginning_offsets_on_partitions_revoked() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_begin_on_revoked");
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic(&producer, &topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id));
    let captured: Arc<Mutex<Option<HashMap<TopicPartition, i64>>>> = Arc::new(Mutex::new(None));
    trigger_on_partitions_revoked(
        consumer.as_mut(),
        &topic,
        &tp,
        CallbackAction::BeginningOffsets { tp: tp.clone(), captured: Arc::clone(&captured) },
    )
    .await;

    let map = captured.lock().unwrap().take().expect("action ran");
    assert!(map.contains_key(&tp), "beginningOffsets must contain {tp}");
    assert_eq!(map.get(&tp), Some(&0), "beginningOffsets({tp}) must be 0");
    consumer.close().await.expect("consumer close should succeed");
}

/// Java `testAsyncConsumerGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback`
/// (line 246): `position(tp)` inside the assigned callback does not throw.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_position_of_newly_assigned_partition_on_partitions_assigned_callback() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_position_on_assigned");
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic(&producer, &topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id));
    let result: Arc<Mutex<Option<Result<i64, KafkaError>>>> = Arc::new(Mutex::new(None));
    trigger_on_partitions_assigned(
        consumer.as_mut(),
        &topic,
        &tp,
        CallbackAction::Position { tp: tp.clone(), result: Arc::clone(&result) },
    )
    .await;

    let position = result.lock().unwrap().take().expect("action ran");
    assert!(
        position.is_ok(),
        "position() inside the callback must not error, got {position:?}"
    );
    consumer.close().await.expect("consumer close should succeed");
}

/// Java `testAsyncConsumerSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback`
/// (line 264): `seek(tp, 100)` + `pause([tp])` inside the assigned callback,
/// then resume + consume the remaining records from offset 100.
#[tokio::test(flavor = "multi_thread")]
async fn test_seek_position_and_pause_newly_assigned_partition_on_partitions_assigned_callback() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_seek_pause_on_assigned");
    let tp = TopicPartition::new(topic.clone(), 0);

    let starting_offset: i64 = 100;
    let total_records: usize = 120;

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    for i in 0..total_records {
        let record = ProducerRecord::with_partition(
            topic.clone(),
            Some(0),
            Some(format!("key-{i}").into_bytes()),
            Some(format!("value-{i}").into_bytes()),
        )
        .expect("ProducerRecord::with_partition should succeed");
        let fut = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
            .await
            .expect("send should succeed");
        fut.get_timeout(Duration::from_secs(30)).await.expect("send should ack");
    }
    producer.flush().await.expect("producer.flush should succeed");
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id));
    let result: Arc<Mutex<Option<Result<(), KafkaError>>>> = Arc::new(Mutex::new(None));
    trigger_on_partitions_assigned(
        consumer.as_mut(),
        &topic,
        &tp,
        CallbackAction::SeekAndPause { tp: tp.clone(), offset: starting_offset, result: Arc::clone(&result) },
    )
    .await;

    result
        .lock()
        .unwrap()
        .take()
        .expect("action ran")
        .expect("seek+pause inside callback must succeed");

    // Java: `assertTrue(consumer.paused().contains(tp))`.
    assert!(
        consumer.paused().contains(&tp),
        "tp must be paused after the callback paused it"
    );

    // Resume and consume the remaining records from `starting_offset`.
    consumer.resume(std::slice::from_ref(&tp)).await.expect("resume should succeed");
    let mut consumed: usize = 0;
    let mut next_offset = starting_offset;
    let deadline = Instant::now() + Duration::from_secs(60);
    let expected = total_records - starting_offset as usize;
    while consumed < expected && Instant::now() < deadline {
        let records = consumer.poll(Duration::from_millis(200)).await.expect("poll should succeed");
        for rec in records.records_for_partition(&tp) {
            assert_eq!(
                rec.offset(),
                next_offset,
                "records must resume contiguously from {starting_offset}"
            );
            next_offset += 1;
            consumed += 1;
        }
    }
    assert_eq!(
        consumed, expected,
        "must consume the remaining {expected} records after resuming from {starting_offset}"
    );
    consumer.close().await.expect("consumer close should succeed");
}
