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
//! (Apache Kafka 4.2), run as multilanguage scenarios: each one runs against
//! the native Rust consumer and, through the gRPC backends, the Python (sync
//! and asyncio) and C consumers ([`multilanguage_consumer_test!`]). The setup
//! producer is a native fixture; only the consumer is under test.
//!
//! These exercise `consumer-threading.md` §31 rebalance-listener
//! reentrancy: in Java, the rebalance listener runs on the caller's task
//! and may call back into the consumer API (assign / assignment /
//! beginningOffsets / position / seek / pause) from inside the callback.
//!
//! # The listener under test
//!
//! A gRPC backend cannot take a Rust listener object, so every scenario
//! registers the backend's own logging listener
//! ([`ConsumerCallbackLog`]) and asserts on what it logged. For the
//! in-callback-reentrancy scenarios the listener is given a
//! [`ListenerAction`] script: the consumer calls Java's anonymous listener
//! makes (`assignment()`, `beginningOffsets`, `position`, `seek` then
//! `pause`), run once from inside the targeted method when its partitions
//! contain the test partition, each step's result logged as a
//! `listener_action` entry. The native
//! listener makes the call through a captured [`ConsumerHandle`] (the Rust
//! equivalent of Java capturing the `consumer` variable; the listener trait
//! stays Java-identical, `&self` + partitions); the Python servers through the
//! consumer object, the C server through the `kafka_consumer_ConsumerHandle_*`
//! entry points. The bg loop does not freeze during the callback (Phase 41b),
//! so a reentrant op that routes through the bg task completes instead of
//! deadlocking.
//!
//! # Translated (KIP-848 / `GroupProtocol.CONSUMER` arm only)
//!
//! Runs:
//! - `testOnPartitionsAssignedCalledWithNewPartitionsOnlyForAsyncConsumer`
//!   → `test_on_partitions_assigned_called_with_new_partitions_only`
//! - `testAsyncConsumerRebalanceListenerAssignmentOnPartitionsAssigned`
//!   → `test_rebalance_listener_assignment_on_partitions_assigned`
//! - `testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsAssigned`
//!   → `test_rebalance_listener_beginning_offsets_on_partitions_assigned`
//! - `testAsyncConsumerRebalanceListenerAssignmentOnPartitionsRevoked`
//!   → `test_rebalance_listener_assignment_on_partitions_revoked`
//! - `testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsRevoked`
//!   → `test_rebalance_listener_beginning_offsets_on_partitions_revoked`
//! - `testAsyncConsumerGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback`
//!   → `test_get_position_of_newly_assigned_partition_on_partitions_assigned_callback`
//! - `testAsyncConsumerSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback`
//!   → `test_seek_position_and_pause_newly_assigned_partition_on_partitions_assigned_callback`
//!
//! Each name gains the macro's backend suffix (`__rust`, `__grpc_python`,
//! `__grpc_python_async`, `__grpc_c`).
//!
//! # SKIPped (no subject on the Rust side)
//!
//! - SKIP: `testAsyncConsumerRebalanceListenerAssignOnPartitionsAssigned`
//! - SKIP: `testAsyncConsumerRebalanceListenerAssignOnPartitionsRevoked`
//!   Java asserts that `consumer.assign(...)` called from inside a rebalance
//!   callback throws IllegalState "Subscription to topics, partitions and
//!   pattern are mutually exclusive". In Rust a listener reaches its consumer
//!   only through [`ConsumerHandle`], whose surface is KIP-1306's
//!   `RebalanceConsumer` (plus `wakeup`), which has no `assign`: the call the
//!   Java test makes cannot be written, so there is nothing to assert
//!   (`cargo xtask lint-custom` `check-consumer-handle` keeps `assign` off the
//!   handle). The owning `Consumer::assign` is `&mut self`, which the borrow
//!   checker refuses while `poll()` holds the consumer.
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
//!
//! [`ConsumerHandle`]: confluent_kafka::consumer::ConsumerHandle

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::consumer::DynConsumer;
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::backend_factory::ConsumerBackendFactory;
use crate::common::callback_log::{
    CallbackLogEntry, ConsumerCallbackLog, KIND_ASSIGNED, KIND_LISTENER_ACTION, KIND_REVOKED, ListenerAction,
    ListenerStep, ListenerTrigger,
};
use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
use crate::common::consumer_scenario::{consumer_config_for, poll_until_entry, produce};
use crate::common::test_context::TestContext;
use crate::multilanguage_consumer_test;

type BytesConsumer = Box<dyn DynConsumer<Vec<u8>, Vec<u8>>>;

// ── Cluster config (Java: 3 brokers, KIP-848, no extra serverProperties) ─

fn cluster_config_with_kip848_3brokers() -> ClusterConfig {
    // Auto-created topics get a single partition (Java's `topic`/`newTopic`
    // both have a single relevant partition `(topic, 0)` in this suite); the
    // canonical helper supplies the shared KIP-848 broker tuning.
    kip848_3_broker(1)
}

// ── Fixtures ──────────────────────────────────────────────────────────

/// Provision a single-partition topic (this suite only cares about
/// partition 0 of `topic` and `newTopic`) with a native producer.
async fn ensure_topic(ctx: &TestContext, topic: &str) {
    produce(ctx, topic, &[("__provisioner__", "__provisioner__")]).await;
}

/// Java's `ClientsTestUtils#sendRecords(ClusterInstance,TopicPartition,int,long)`:
/// a fresh native producer sends `num_records` records `key i` / `value i` to
/// `tp`, record `i` with timestamp `starting_timestamp + i`, then flushes and
/// closes.
async fn send_records(ctx: &TestContext, tp: &TopicPartition, num_records: usize, starting_timestamp: i64) {
    let mut props = HashMap::from([("acks".to_string(), "all".to_string())]);
    ctx.configure(&mut props);
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> = KafkaProducer::new(
        ProducerConfig::new(&props).expect("producer config"),
        ByteArraySerializer::default(),
        ByteArraySerializer::default(),
    )
    .expect("create producer");
    for i in 0..num_records {
        let record = ProducerRecord::with_partition_timestamp_key(
            tp.topic().to_string(),
            Some(tp.partition()),
            Some(starting_timestamp + i as i64),
            Some(format!("key {i}").into_bytes()),
            Some(format!("value {i}").into_bytes()),
        )
        .expect("valid producer record");
        // Fully-qualified trait call: KafkaProducer also has an inherent
        // 2-arg send(record, callback) that would otherwise shadow this.
        Producer::send(&producer, record).await.expect("send");
    }
    producer.flush().await.expect("producer flush");
    producer.close().await.expect("producer close");
}

/// Java's `createConsumer(protocol)` sets `group.protocol=consumer` and
/// `enable.auto.commit=false` (no `group.id` override in the Java helper,
/// so a random one is supplied per test). Default
/// `auto.offset.reset=earliest`. The consumer comes with its callback log,
/// since every scenario observes the listener.
async fn create_consumer<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    group_id: &str,
) -> (BytesConsumer, ConsumerCallbackLog) {
    factory
        .create_with_callback_log(consumer_config_for(factory, ctx, group_id))
        .await
        .unwrap_or_else(|e| panic!("{} backend: create consumer: {e}", factory.name()))
}

/// The partitions of `entry` as a set.
fn partition_set(partitions: &[(String, i32)]) -> HashSet<TopicPartition> {
    partitions.iter().map(|(t, p)| TopicPartition::new(t.clone(), *p)).collect()
}

/// The first `kind` entry of `entries` that `wanted` accepts.
fn first_entry<'a>(
    entries: &'a [CallbackLogEntry],
    kind: &str,
    wanted: impl Fn(&CallbackLogEntry) -> bool,
) -> Option<&'a CallbackLogEntry> {
    entries.iter().find(|e| e.kind == kind && wanted(e))
}

// ── Runnable test: KIP-848 new-partitions-only ────────────────────────

/// Translates Java's
/// `testOnPartitionsAssignedCalledWithNewPartitionsOnlyForAsyncConsumer`
/// via `testOnPartitionsAssignedCalledWithExpectedPartitions(
/// consumer, /*expectNewPartitionsOnlyInCallback=*/ true)`.
///
/// KIP-848 (CONSUMER protocol): when the subscription is expanded to add a
/// new topic, the `onPartitionsAssigned` callback receives ONLY the newly
/// added partition, not the full assignment. This needs no consumer
/// reentrancy — the listener only logs its `partitions` argument. Java's
/// listener records the invocation whose partitions contain all expected
/// ones; the scenario takes the first logged `assigned` entry that does.
async fn on_partitions_assigned_called_with_new_partitions_only<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let new_topic = ctx.topic("newTopic");
    let group_id = ctx.group_id("g_new_partitions_only");
    let tp = TopicPartition::new(topic.clone(), 0);
    let added = TopicPartition::new(new_topic.clone(), 0);

    ensure_topic(ctx, &topic).await;
    ensure_topic(ctx, &new_topic).await;

    let (mut consumer, log) = create_consumer(ctx, factory, &group_id).await;

    // First subscription: expect `tp` in the callback.
    log.subscribe_with_logging_listener(&mut consumer, vec![topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: first subscribe: {e}"));
    let has_tp = |e: &CallbackLogEntry| e.has_partition(tp.topic(), tp.partition());
    let entries = poll_until_entry(&mut consumer, &log, KIND_ASSIGNED, has_tp, Duration::from_secs(90)).await;
    let got1 = first_entry(&entries, KIND_ASSIGNED, has_tp)
        .unwrap_or_else(|| panic!("{backend} backend: Timed out before expected rebalance completed: {entries:?}"));
    assert_eq!(
        partition_set(&got1.partitions),
        [tp.clone()].into_iter().collect::<HashSet<_>>(),
        "{backend} backend: first onPartitionsAssigned should be exactly {{tp}}"
    );
    assert_eq!(
        consumer.assignment(),
        [tp.clone()].into_iter().collect::<HashSet<_>>(),
        "{backend} backend"
    );

    // Expand subscription to add `newTopic`. The callback should receive
    // ONLY the newly added partition (KIP-848 incremental assignment).
    log.subscribe_with_logging_listener(&mut consumer, vec![topic.clone(), new_topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: expand subscribe: {e}"));
    let has_added = |e: &CallbackLogEntry| e.has_partition(added.topic(), added.partition());
    let entries = poll_until_entry(&mut consumer, &log, KIND_ASSIGNED, has_added, Duration::from_secs(90)).await;
    let got2 = first_entry(&entries, KIND_ASSIGNED, has_added)
        .unwrap_or_else(|| panic!("{backend} backend: Timed out before expected rebalance completed: {entries:?}"));
    assert_eq!(
        partition_set(&got2.partitions),
        [added.clone()].into_iter().collect::<HashSet<_>>(),
        "{backend} backend: expand onPartitionsAssigned should be exactly the newly-added partition {{added}}, \
         got {:?}",
        got2.partitions
    );
    // The full assignment now contains both partitions.
    assert_eq!(
        consumer.assignment(),
        [tp.clone(), added.clone()].into_iter().collect::<HashSet<_>>(),
        "{backend} backend"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

// ── In-callback-reentrancy tests (Issue 8 — closed by Phase 41) ───────
//
// Each listener runs the op the Java test runs on the matching callback for
// the test partition and logs its outcome (a `listener_action` entry). The
// harness then asserts the logged outcome matches Java's assertion.

/// The `listener_action` entries of `entries`, in log order.
fn action_entries(entries: &[CallbackLogEntry]) -> Vec<CallbackLogEntry> {
    entries.iter().filter(|e| e.kind == KIND_LISTENER_ACTION).cloned().collect()
}

/// Java's `triggerOnPartitionsAssigned(tp, consumer, execute)`: subscribe
/// with a listener that runs `steps` from inside `onPartitionsAssigned`
/// once `tp` is assigned, then poll until they have run. Returns the steps'
/// log entries, in order.
async fn trigger_on_partitions_assigned(
    consumer: &mut BytesConsumer,
    log: &ConsumerCallbackLog,
    backend: &str,
    topic: &str,
    tp: &TopicPartition,
    steps: Vec<ListenerStep>,
) -> Vec<CallbackLogEntry> {
    let want = steps.len();
    log.subscribe_with_logging_listener_action(
        consumer,
        vec![topic.to_string()],
        ListenerAction::new().on_assigned(ListenerTrigger::OnceWhenContains(tp.clone()), steps),
    )
    .await
    .unwrap_or_else(|e| panic!("{backend} backend: subscribe: {e}"));
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let entries = log.entries().await.expect("read consumer callback log");
        let actions = action_entries(&entries);
        if actions.len() >= want {
            return actions;
        }
        assert!(
            Instant::now() < deadline,
            "{backend} backend: Timed out before expected rebalance callback ran: {entries:?}"
        );
        consumer
            .poll(Duration::from_millis(500))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll: {e}"));
    }
}

/// Java's `triggerOnPartitionsRevoked(tp, protocol, execute)`: subscribe a
/// listener that runs `steps` from inside `onPartitionsRevoked` once `tp`
/// is revoked. Poll until assigned, then `unsubscribe()` to force the
/// revocation, then assert the steps ran. Returns their log entries, in order.
async fn trigger_on_partitions_revoked(
    consumer: &mut BytesConsumer,
    log: &ConsumerCallbackLog,
    backend: &str,
    topic: &str,
    tp: &TopicPartition,
    steps: Vec<ListenerStep>,
) -> Vec<CallbackLogEntry> {
    let want = steps.len();
    log.subscribe_with_logging_listener_action(
        consumer,
        vec![topic.to_string()],
        ListenerAction::new().on_revoked(ListenerTrigger::OnceWhenContains(tp.clone()), steps),
    )
    .await
    .unwrap_or_else(|e| panic!("{backend} backend: subscribe: {e}"));
    let has_tp = |e: &CallbackLogEntry| e.has_partition(tp.topic(), tp.partition());
    let entries = poll_until_entry(consumer, log, KIND_ASSIGNED, has_tp, Duration::from_secs(90)).await;
    assert!(
        first_entry(&entries, KIND_ASSIGNED, has_tp).is_some(),
        "{backend} backend: Timed out before expected rebalance callback ran: {entries:?}"
    );

    // Force a revocation: unsubscribe drives `onPartitionsRevoked`. Poll a
    // few times so the bg loop drives the reconcile + the app side drains
    // the callback. The consumer has no subscription any more, so a poll may
    // fail; as in the Java helper only the callback matters.
    consumer
        .unsubscribe()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: unsubscribe: {e}"));
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let entries = log.entries().await.expect("read consumer callback log");
        let actions = action_entries(&entries);
        if actions.len() >= want {
            return actions;
        }
        assert!(
            Instant::now() < deadline,
            "{backend} backend: onPartitionsRevoked must have run the reentrant action: {entries:?}"
        );
        let _ = consumer.poll(Duration::from_millis(100)).await;
    }
}

/// Java `testAsyncConsumerRebalanceListenerAssignmentOnPartitionsAssigned`:
/// `assignment()` inside the callback contains tp.
async fn rebalance_listener_assignment_on_partitions_assigned<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_assignment_on_assigned");
    let tp = TopicPartition::new(topic.clone(), 0);
    ensure_topic(ctx, &topic).await;

    let (mut consumer, log) = create_consumer(ctx, factory, &group_id).await;
    let entry =
        trigger_on_partitions_assigned(&mut consumer, &log, backend, &topic, &tp, vec![ListenerStep::Assignment])
            .await
            .remove(0);

    assert_eq!(entry.method, KIND_ASSIGNED, "{backend} backend: {entry:?}");
    assert!(
        entry.error.is_empty(),
        "{backend} backend: assignment() failed: {}",
        entry.error
    );
    let assignment = partition_set(&entry.result_partitions);
    assert!(
        assignment.contains(&tp),
        "{backend} backend: assignment() inside the callback must contain {tp}, got {assignment:?}"
    );
    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

/// Java `testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsAssigned`:
/// `beginningOffsets([tp])` inside the callback → `get(tp) == 0`.
async fn rebalance_listener_beginning_offsets_on_partitions_assigned<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_begin_on_assigned");
    let tp = TopicPartition::new(topic.clone(), 0);
    ensure_topic(ctx, &topic).await;

    let (mut consumer, log) = create_consumer(ctx, factory, &group_id).await;
    let entry = trigger_on_partitions_assigned(
        &mut consumer,
        &log,
        backend,
        &topic,
        &tp,
        vec![ListenerStep::BeginningOffsets(vec![tp.clone()])],
    )
    .await
    .remove(0);

    assert_eq!(entry.method, KIND_ASSIGNED, "{backend} backend: {entry:?}");
    assert!(
        entry.error.is_empty(),
        "{backend} backend: beginning_offsets failed: {}",
        entry.error
    );
    let offset = entry.offset_for(tp.topic(), tp.partition());
    assert!(offset.is_some(), "{backend} backend: beginningOffsets must contain {tp}");
    assert_eq!(offset, Some(0), "{backend} backend: beginningOffsets({tp}) must be 0");
    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

/// Java `testAsyncConsumerRebalanceListenerAssignmentOnPartitionsRevoked`:
/// `assignment()` inside revoked contains tp.
async fn rebalance_listener_assignment_on_partitions_revoked<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_assignment_on_revoked");
    let tp = TopicPartition::new(topic.clone(), 0);
    ensure_topic(ctx, &topic).await;

    let (mut consumer, log) = create_consumer(ctx, factory, &group_id).await;
    let entry =
        trigger_on_partitions_revoked(&mut consumer, &log, backend, &topic, &tp, vec![ListenerStep::Assignment])
            .await
            .remove(0);

    assert_eq!(entry.method, KIND_REVOKED, "{backend} backend: {entry:?}");
    assert!(
        entry.error.is_empty(),
        "{backend} backend: assignment() failed: {}",
        entry.error
    );
    let assignment = partition_set(&entry.result_partitions);
    assert!(
        assignment.contains(&tp),
        "{backend} backend: assignment() inside the revoked callback must contain {tp}, got {assignment:?}"
    );
    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

/// Java `testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsRevoked`:
/// `beginningOffsets([tp])` inside revoked → `get(tp) == 0`.
async fn rebalance_listener_beginning_offsets_on_partitions_revoked<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_begin_on_revoked");
    let tp = TopicPartition::new(topic.clone(), 0);
    ensure_topic(ctx, &topic).await;

    let (mut consumer, log) = create_consumer(ctx, factory, &group_id).await;
    let entry = trigger_on_partitions_revoked(
        &mut consumer,
        &log,
        backend,
        &topic,
        &tp,
        vec![ListenerStep::BeginningOffsets(vec![tp.clone()])],
    )
    .await
    .remove(0);

    assert_eq!(entry.method, KIND_REVOKED, "{backend} backend: {entry:?}");
    assert!(
        entry.error.is_empty(),
        "{backend} backend: beginning_offsets failed: {}",
        entry.error
    );
    let offset = entry.offset_for(tp.topic(), tp.partition());
    assert!(offset.is_some(), "{backend} backend: beginningOffsets must contain {tp}");
    assert_eq!(offset, Some(0), "{backend} backend: beginningOffsets({tp}) must be 0");
    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

/// Java `testAsyncConsumerGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback`:
/// `position(tp)` inside the assigned callback does not throw.
async fn get_position_of_newly_assigned_partition_on_partitions_assigned_callback<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_position_on_assigned");
    let tp = TopicPartition::new(topic.clone(), 0);
    ensure_topic(ctx, &topic).await;

    let (mut consumer, log) = create_consumer(ctx, factory, &group_id).await;
    let entry = trigger_on_partitions_assigned(
        &mut consumer,
        &log,
        backend,
        &topic,
        &tp,
        vec![ListenerStep::Position(tp.clone())],
    )
    .await
    .remove(0);

    assert_eq!(entry.method, KIND_ASSIGNED, "{backend} backend: {entry:?}");
    assert!(
        entry.error.is_empty(),
        "{backend} backend: position() inside the callback must not error, got {}",
        entry.error
    );
    assert!(
        entry.offset_for(tp.topic(), tp.partition()).is_some(),
        "{backend} backend: position() must report a position for {tp}: {entry:?}"
    );
    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

/// Java `testAsyncConsumerSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback`:
/// `seek(tp, 100)` + `pause([tp])` inside the assigned callback,
/// then resume + consume the remaining records from offset 100.
async fn seek_position_and_pause_newly_assigned_partition_on_partitions_assigned_callback<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_seek_pause_on_assigned");
    let tp = TopicPartition::new(topic.clone(), 0);

    let starting_offset: i64 = 100;
    let total_records: usize = 120;
    let starting_timestamp: i64 = 0;

    send_records(ctx, &tp, total_records, starting_timestamp).await;

    let (mut consumer, log) = create_consumer(ctx, factory, &group_id).await;
    let entries = trigger_on_partitions_assigned(
        &mut consumer,
        &log,
        backend,
        &topic,
        &tp,
        vec![
            ListenerStep::Seek(tp.clone(), starting_offset),
            ListenerStep::Pause(vec![tp.clone()]),
        ],
    )
    .await;

    let steps: Vec<&str> = entries.iter().map(|e| e.step.as_str()).collect();
    assert_eq!(steps, ["SEEK", "PAUSE"], "{backend} backend: {entries:?}");
    for entry in &entries {
        assert_eq!(entry.method, KIND_ASSIGNED, "{backend} backend: {entry:?}");
        assert!(
            entry.error.is_empty(),
            "{backend} backend: seek+pause inside callback must succeed, got {}",
            entry.error
        );
    }

    // Java: `assertTrue(consumer.paused().contains(tp))`.
    assert!(
        consumer.paused().contains(&tp),
        "{backend} backend: tp must be paused after the callback paused it"
    );

    // Resume and consume the remaining records from `starting_offset`:
    // Java's `consumeAndVerifyRecords(consumer, tp, totalRecords - startingOffset,
    // startingOffset, startingOffset, startingOffset)`, which checks each
    // record's partition, timestamp type, timestamp, offset, key, value and
    // serialized sizes.
    consumer
        .resume(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: resume: {e}"));
    let mut consumed: usize = 0;
    let deadline = Instant::now() + Duration::from_secs(60);
    let expected = total_records - starting_offset as usize;
    while consumed < expected && Instant::now() < deadline {
        let records = consumer
            .poll(Duration::from_millis(200))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll: {e}"));
        for rec in records.records_with_partition(&tp) {
            let i = starting_offset + consumed as i64;
            assert_eq!(rec.topic(), tp.topic(), "{backend} backend");
            assert_eq!(rec.partition(), tp.partition(), "{backend} backend");
            assert_eq!(rec.timestamp_type(), TimestampType::CreateTime, "{backend} backend");
            assert_eq!(rec.timestamp(), starting_timestamp + i, "{backend} backend");
            assert_eq!(
                rec.offset(),
                i,
                "{backend} backend: records must resume contiguously from {starting_offset}"
            );
            assert_eq!(
                rec.key().map(Vec::as_slice),
                Some(format!("key {i}").as_bytes()),
                "{backend} backend"
            );
            assert_eq!(
                rec.value().map(Vec::as_slice),
                Some(format!("value {i}").as_bytes()),
                "{backend} backend"
            );
            assert_eq!(rec.serialized_key_size(), format!("key {i}").len() as i32, "{backend} backend");
            assert_eq!(
                rec.serialized_value_size(),
                format!("value {i}").len() as i32,
                "{backend} backend"
            );
            consumed += 1;
        }
    }
    assert_eq!(
        consumed, expected,
        "{backend} backend: must consume the remaining {expected} records after resuming from {starting_offset}"
    );
    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_on_partitions_assigned_called_with_new_partitions_only,
    on_partitions_assigned_called_with_new_partitions_only,
    cluster_config_with_kip848_3brokers()
);
multilanguage_consumer_test!(
    test_rebalance_listener_assignment_on_partitions_assigned,
    rebalance_listener_assignment_on_partitions_assigned,
    cluster_config_with_kip848_3brokers()
);
multilanguage_consumer_test!(
    test_rebalance_listener_beginning_offsets_on_partitions_assigned,
    rebalance_listener_beginning_offsets_on_partitions_assigned,
    cluster_config_with_kip848_3brokers()
);
multilanguage_consumer_test!(
    test_rebalance_listener_assignment_on_partitions_revoked,
    rebalance_listener_assignment_on_partitions_revoked,
    cluster_config_with_kip848_3brokers()
);
multilanguage_consumer_test!(
    test_rebalance_listener_beginning_offsets_on_partitions_revoked,
    rebalance_listener_beginning_offsets_on_partitions_revoked,
    cluster_config_with_kip848_3brokers()
);
multilanguage_consumer_test!(
    test_get_position_of_newly_assigned_partition_on_partitions_assigned_callback,
    get_position_of_newly_assigned_partition_on_partitions_assigned_callback,
    cluster_config_with_kip848_3brokers()
);
multilanguage_consumer_test!(
    test_seek_position_and_pause_newly_assigned_partition_on_partitions_assigned_callback,
    seek_position_and_pause_newly_assigned_partition_on_partitions_assigned_callback,
    cluster_config_with_kip848_3brokers()
);
