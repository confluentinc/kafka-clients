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
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerCommitTest.java`
//! (Apache Kafka 4.2; the two tests under "Added in Apache Kafka 4.3.1" follow
//! the 4.3.1 source and cite its line numbers).
//!
//! These exercise the consumer **commit path**: auto-commit-on-close,
//! commit metadata round-trip, async commit + callback success counts,
//! per-partition specified-offset commits, auto-commit-on-rebalance,
//! member-id propagation on subscribe+commit, position/commit interplay
//! across two consumers, and the async-callback-completion ordering
//! guarantees — against a real 3-broker Kafka 4.2.0 cluster with KIP-848
//! (`group.protocol=consumer`) enabled.
//!
//! They run as multilanguage scenarios: each one runs against the native
//! Rust consumer and, through the gRPC backends, the Python (sync and
//! asyncio) and C consumers ([`multilanguage_consumer_test!`]); each name
//! below gains the macro's backend suffix (`__rust`, `__grpc_python`,
//! `__grpc_python_async`, `__grpc_c`). The setup producer and admin client
//! are native fixtures; every consumer is built by the backend factory. Java's
//! `CountConsumerCommitCallback` and `RetryCommitCallback` become the
//! backend's own logging commit callback ([`ConsumerCallbackLog`]): a success
//! is a `commit` entry without an error, a failure one with it, and the
//! counts are the numbers of such entries.
//!
//! # Translated (KIP-848 / `GroupProtocol.CONSUMER` arm only)
//!
//! - `testAsyncConsumerAutoCommitOnClose`
//!   → `test_async_consumer_auto_commit_on_close`
//! - `testAsyncConsumerAutoCommitOnCloseAfterWakeup`
//!   → `test_async_consumer_auto_commit_on_close_after_wakeup`
//! - `testAsyncConsumerCommitMetadata`
//!   → `test_async_consumer_commit_metadata`
//! - `testAsyncConsumerAsyncCommit`
//!   → `test_async_consumer_async_commit`
//! - `testAsyncConsumerCommitSpecifiedOffsets`
//!   → `test_async_consumer_commit_specified_offsets`
//! - `testAsyncConsumerAutoCommitOnRebalance`
//!   → `test_async_consumer_auto_commit_on_rebalance` (see deviation below)
//! - `testAsyncConsumerSubscribeAndCommitSync`
//!   → `test_async_consumer_subscribe_and_commit_sync`
//! - `testAsyncConsumerPositionAndCommit`
//!   → `test_async_consumer_position_and_commit`
//! - `testCommitAsyncCompletedBeforeConsumerCloses`
//!   → `test_commit_async_completed_before_consumer_closes`
//! - `testCommitAsyncCompletedBeforeCommitSyncReturns`
//!   → `test_commit_async_completed_before_commit_sync_returns`
//!
//! # Added in Apache Kafka 4.3.1
//!
//! - `testAsyncConsumerNoCommittedOffsets`
//!   → `test_async_consumer_no_committed_offsets`
//! - `testAsyncConsumerCommittedDeletedTopic` (KAFKA-20165)
//!   → `test_async_consumer_committed_deleted_topic`
//!
//! # Broker fault injection (Phase 16)
//!
//! - `testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose` (4.3.1)
//!   → `test_commit_async_fails_when_coordinator_unavailable_during_close`
//!   — runs on its own dedicated `Type::Kraft` cluster (3 brokers + 1
//!   isolated controller) because it stops every broker with
//!   `KafkaCluster::shutdown_broker`, which a pooled cluster must never do.
//!
//! # SKIPped (CONSUMER-arm)
//!
//! - SKIP: `testAsyncConsumerAutoCommitIntercept` —
//!   `ConsumerInterceptor` integration (`MockConsumerInterceptor.
//!   ON_COMMIT_COUNT`) is not in scope for this phase (report 07: "no
//!   interceptor integration coverage at all"). Also depends on
//!   pause-inside-callback (Issue 8 in the Phase-13 COMMENTS), which is
//!   structurally unsupported in Rust. The worklist permits skipping when
//!   interceptor integration is not done — it is not.
//!
//! # SKIPped (classic-protocol-only — `consumer-threading.md` §20)
//!
//! - SKIP: `testClassicConsumerAutoCommitOnClose` — classic-protocol-only
//! - SKIP: `testClassicConsumerAutoCommitOnCloseAfterWakeup` — classic-protocol-only
//! - SKIP: `testClassicConsumerCommitMetadata` — classic-protocol-only
//! - SKIP: `testClassicConsumerAsyncCommit` — classic-protocol-only
//! - SKIP: `testClassicConsumerAutoCommitIntercept` — classic-protocol-only
//! - SKIP: `testClassicConsumerCommitSpecifiedOffsets` — classic-protocol-only
//! - SKIP: `testClassicConsumerAutoCommitOnRebalance` — classic-protocol-only
//! - SKIP: `testClassicConsumerSubscribeAndCommitSync` — classic-protocol-only
//! - SKIP: `testClassicConsumerPositionAndCommit` — classic-protocol-only
//! - SKIP: `testClassicConsumerNoCommittedOffsets` — classic-protocol-only
//! - SKIP: `testClassicConsumerCommittedDeletedTopic` — classic-protocol-only

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::admin::Admin;
use confluent_kafka::admin::AdminClient;
use confluent_kafka::admin::AdminClientConfig;
use confluent_kafka::common::Error;
use confluent_kafka::common::TopicCollection;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::consumer::DynConsumer;
use confluent_kafka::consumer::OffsetAndMetadata;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::backend_factory::ConsumerBackendFactory;
use crate::common::callback_log::{CallbackLogEntry, ConsumerCallbackLog, KIND_COMMIT};
use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
use crate::common::test_context::TestContext;
use crate::common::test_utils;
use crate::multilanguage_consumer_test;

// The bytes-typed consumer trait object every backend factory returns. Used in
// helper signatures; the tests pass `consumer.as_mut()`.
type BytesConsumer = dyn DynConsumer<Vec<u8>, Vec<u8>>;

// ── Cluster config ────────────────────────────────────────────────────

/// Cluster config matching the Java suite's `@ClusterTestDefaults`:
/// 3 brokers, KIP-848 enabled on the coordinator, plus the broker
/// properties from the `serverProperties` annotation.
///
/// Mirrors `PlaintextConsumerCommitTest`:
/// ```text
/// brokers = BROKER_COUNT (= 3)
/// offsets.topic.num.partitions     = 1
/// offsets.topic.replication.factor = 3
/// group.min.session.timeout.ms     = 100
/// ```
///
/// The suite's `@BeforeEach` topic is created explicitly by
/// [`create_test_topic`]; `num.partitions=2` only matters for auto-created
/// topics.
fn cluster_config_with_kip848_3brokers() -> ClusterConfig {
    // The canonical helper supplies the shared KIP-848 broker tuning.
    kip848_3_broker(2)
}

// ── Consumer config builder ───────────────────────────────────────────

/// Build the consumer properties matching Java's `createConsumer(protocol,
/// enableAutoCommit)` which sets `group.id=test-group`,
/// `group.protocol=consumer`, and `enable.auto.commit`, on the bootstrap the
/// backend under test reaches. Default `auto.offset.reset=earliest`. Caller
/// overrides win.
fn make_consumer_props<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    group_id: &str,
    enable_auto_commit: bool,
    overrides: &[(&str, &str)],
) -> HashMap<String, String> {
    let mut props = HashMap::from([
        (
            "bootstrap.servers".to_string(),
            ctx.protocol_bootstrap_servers_for(factory.needs_container_bootstrap())
                .to_string(),
        ),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), enable_auto_commit.to_string()),
        ("group.id".to_string(), group_id.to_string()),
    ]);
    for (k, v) in overrides {
        props.insert((*k).to_string(), (*v).to_string());
    }
    ctx.apply_security(&mut props);
    props
}

/// Create a consumer under test through `factory`.
async fn create_consumer<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    group_id: &str,
    enable_auto_commit: bool,
    overrides: &[(&str, &str)],
) -> Box<BytesConsumer> {
    factory
        .create(make_consumer_props(ctx, factory, group_id, enable_auto_commit, overrides))
        .await
        .unwrap_or_else(|e| panic!("{} backend: create consumer: {e}", factory.name()))
}

/// [`create_consumer`] together with the consumer's callback log, for the
/// tests that observe the commit callback.
async fn create_consumer_with_log<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    group_id: &str,
    enable_auto_commit: bool,
) -> (Box<BytesConsumer>, ConsumerCallbackLog) {
    factory
        .create_with_callback_log(make_consumer_props(ctx, factory, group_id, enable_auto_commit, &[]))
        .await
        .unwrap_or_else(|e| panic!("{} backend: create consumer: {e}", factory.name()))
}

// ── Producer helpers (mirror Java's ClientsTestUtils.sendRecords) ─────

fn make_producer_config(ctx: &TestContext) -> ProducerConfig {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.protocol_bootstrap_servers().to_string()),
        ("client.id".to_string(), "integration-test-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "5".to_string()),
    ]);
    ctx.apply_security(&mut props);
    ProducerConfig::new(&props).expect("invalid producer test config")
}

fn build_producer_bytes(ctx: &TestContext) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    KafkaProducer::new(
        make_producer_config(ctx),
        ByteArraySerializer::default(),
        ByteArraySerializer::default(),
    )
    .expect("Failed to build test producer")
}

/// Translates `ClientsTestUtils.sendRecords(producer, tp, num,
/// startingTimestamp)` (1ms-per-record default increment).
async fn send_records_with_producer(
    producer: &KafkaProducer<Vec<u8>, Vec<u8>>,
    tp: &TopicPartition,
    num_records: usize,
    starting_timestamp: i64,
) {
    let mut last_future = None;
    for i in 0..num_records {
        let timestamp = starting_timestamp + i as i64;
        let key = format!("key {i}").into_bytes();
        let value = format!("value {i}").into_bytes();
        let record = ProducerRecord::with_partition_timestamp_key(
            tp.topic().to_string(),
            Some(tp.partition()),
            Some(timestamp),
            Some(key),
            Some(value),
        )
        .expect("ProducerRecord::with_partition_timestamp_key should not fail");
        last_future = Some(
            <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(producer, record)
                .await
                .expect("send should not fail"),
        );
    }
    producer.flush().await.expect("producer.flush should succeed");
    if let Some(f) = last_future {
        f.get_with_timeout(Duration::from_secs(30))
            .await
            .expect("last send should succeed");
    }
}

/// Translates `ClientsTestUtils.sendRecords(cluster, tp, num,
/// startingTimestamp)` — fresh producer, send, close.
async fn send_records_bytes(ctx: &TestContext, tp: &TopicPartition, num_records: usize, starting_timestamp: i64) {
    let producer = build_producer_bytes(ctx);
    send_records_with_producer(&producer, tp, num_records, starting_timestamp).await;
    producer.close().await.expect("producer close should succeed");
}

/// Java `PlaintextConsumerCommitTest#BROKER_COUNT`.
const BROKER_COUNT: i16 = 3;

/// Translates `cluster.createTopic(topic, numPartitions, (short) BROKER_COUNT)`,
/// as the suite's `@BeforeEach` (`PlaintextConsumerCommitTest#setup`) does for
/// `topic`. Creates the topic EMPTY (the first produced record lands at offset 0).
///
/// Creates the topic through the admin client and then waits until every
/// partition leader accepts leader-only requests, before the test produces.
/// Auto-creating the topic (by producing or via metadata) instead races leader
/// election on the 3-broker cluster: `NOT_LEADER_OR_FOLLOWER` retries reorder
/// the non-idempotent sends.
async fn create_test_topic(ctx: &TestContext, topic: &str, num_partitions: i32) {
    let admin = admin_for(ctx);
    test_utils::create_topic(admin.as_ref(), topic, num_partitions, BROKER_COUNT).await;
    test_utils::wait_for_partition_leaders(admin.as_ref(), topic, 0..num_partitions).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

// ── Consumer test helpers ─────────────────────────────────────────────

/// Translates `ClientsTestUtils.awaitAssignment(consumer,
/// expectedAssignment)`. Drives `poll(100ms)` until `assignment()`
/// equals `expected` or the deadline elapses.
async fn await_assignment(
    consumer: &mut BytesConsumer,
    backend: &str,
    expected: &HashSet<TopicPartition>,
    deadline_duration: Duration,
) {
    let deadline = Instant::now() + deadline_duration;
    while Instant::now() < deadline {
        let _ = consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed in await_assignment: {e}"));
        if &consumer.assignment() == expected {
            return;
        }
    }
    panic!(
        "{backend} backend: Timed out awaiting expected assignment of {} partitions; current {:?}",
        expected.len(),
        consumer.assignment()
    );
}

/// The `commit` entries of `log`, oldest first.
async fn commit_entries(log: &ConsumerCallbackLog, backend: &str) -> Vec<CallbackLogEntry> {
    log.entries()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: read consumer callback log: {e}"))
        .into_iter()
        .filter(|e| e.kind == KIND_COMMIT)
        .collect()
}

/// The number of successful completions the commit callback logged: Java's
/// `CountConsumerCommitCallback.successCount`.
async fn successful_commits(log: &ConsumerCallbackLog, backend: &str) -> usize {
    commit_entries(log, backend)
        .await
        .iter()
        .filter(|e| e.error_value.is_none())
        .count()
}

/// Translates `ClientsTestUtils.pollUntilTrue(consumer, predicate, msg)` for
/// the predicate the commit tests use on Java's `CountConsumerCommitCallback`
/// (`successCount >= want || lastError.isPresent()`): drives `poll(100ms)` in
/// a loop (which also delivers any pending `OffsetCommitCallback`, §31) until
/// the log holds `want` successful `commit` entries or one with an error, and
/// returns the `commit` entries.
async fn poll_until_commits(
    consumer: &mut BytesConsumer,
    log: &ConsumerCallbackLog,
    backend: &str,
    want: usize,
    wait_time: Duration,
    msg: &str,
) -> Vec<CallbackLogEntry> {
    let deadline = Instant::now() + wait_time;
    loop {
        let commits = commit_entries(log, backend).await;
        let successes = commits.iter().filter(|e| e.error_value.is_none()).count();
        if successes >= want || commits.iter().any(|e| e.error_value.is_some()) {
            return commits;
        }
        if Instant::now() >= deadline {
            panic!("{backend} backend: {msg}: {commits:?}");
        }
        let _ = consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should not fail in poll_until_true: {e}"));
    }
}

/// Translates `ClientsTestUtils.consumeAndVerifyRecords` (1ms-per-record
/// timestamp increment). Verifies topic/partition/timestamp/offset/key/
/// value inline.
async fn consume_and_verify_records_bytes(
    consumer: &mut BytesConsumer,
    backend: &str,
    tp: &TopicPartition,
    num_records: usize,
    starting_offset: i64,
    starting_key_and_value_index: usize,
    starting_timestamp: i64,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut next_index: usize = 0;
    while next_index < num_records && Instant::now() < deadline {
        let records = consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
        for record in records {
            if record.topic() != tp.topic() || record.partition() != tp.partition() {
                continue;
            }
            if next_index >= num_records {
                break;
            }
            let i = next_index;
            let offset = starting_offset + i as i64;
            assert_eq!(record.offset(), offset, "record offset should be {offset} ({backend} backend)");
            assert_eq!(
                record.timestamp_type(),
                TimestampType::CreateTime,
                "record timestamp_type should be CreateTime ({backend} backend)"
            );
            let expected_ts = starting_timestamp + i as i64;
            assert_eq!(
                record.timestamp(),
                expected_ts,
                "record timestamp should be {expected_ts} ({backend} backend)"
            );
            let key_and_value_index = starting_key_and_value_index + i;
            let expected_key = format!("key {key_and_value_index}").into_bytes();
            let expected_value = format!("value {key_and_value_index}").into_bytes();
            assert_eq!(
                record.key().expect("key present"),
                &expected_key,
                "key at {i} ({backend} backend)"
            );
            assert_eq!(
                record.value().expect("value present"),
                &expected_value,
                "value at {i} ({backend} backend)"
            );
            assert_eq!(
                record.serialized_key_size(),
                expected_key.len() as i32,
                "serialized key size at {i} ({backend} backend)"
            );
            assert_eq!(
                record.serialized_value_size(),
                expected_value.len() as i32,
                "serialized value size at {i} ({backend} backend)"
            );
            next_index += 1;
        }
    }
    assert_eq!(
        next_index, num_records,
        "timed out before consuming {num_records} records (got {next_index}) ({backend} backend)"
    );
}

/// Mirror of Java's `sendAndAwaitAsyncCommit` + `RetryCommitCallback`:
/// commitAsync the given offsets with the logging callback and drive `poll()`
/// until the callback logs a completion; a `RetriableCommitFailedException`
/// resends the commit (Java's `RetryCommitCallback` resends from inside
/// `onComplete`; here the resend follows the logged failure); any other
/// error fails the test.
async fn send_and_await_async_commit(
    consumer: &mut Box<BytesConsumer>,
    log: &ConsumerCallbackLog,
    backend: &str,
    offsets: HashMap<TopicPartition, OffsetAndMetadata>,
) {
    let mut seen = commit_entries(log, backend).await.len();
    log.commit_async_offsets_with_logging_callback(consumer, offsets.clone())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_async_with_offsets_callback should enqueue: {e}"));

    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let commits = commit_entries(log, backend).await;
        for entry in &commits[seen..] {
            match &entry.error_value {
                None => return,
                Some(e) if e.is_consumer_retriable_commit_failed_error() => {
                    log.commit_async_offsets_with_logging_callback(consumer, offsets.clone())
                        .await
                        .unwrap_or_else(|e| panic!("{backend} backend: resend should enqueue: {e}"));
                },
                Some(e) => panic!("{backend} backend: async commit failed with non-retriable error: {e}"),
            }
        }
        seen = commits.len();
        let _ = consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
    }
    panic!("{backend} backend: Failed to observe commit callback before timeout");
}

// ── Local utilities ────────────────────────────────────────────────────

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as i64
}

// ── Tests ─────────────────────────────────────────────────────────────

/// Translates Java's `testAsyncConsumerAutoCommitOnClose`.
///
/// With `enable.auto.commit=true`, seek positions are auto-committed
/// when the consumer closes; another consumer in the same group sees the
/// committed positions.
async fn async_consumer_auto_commit_on_close<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_auto_commit_on_close");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    {
        let mut consumer = create_consumer(ctx, factory, &group_id, true, &[]).await;
        // Empty topic (both partitions), so seeks define the committed
        // positions deterministically.
        create_test_topic(ctx, &topic, 2).await;
        send_records_bytes(ctx, &tp, 1000, current_time_ms()).await;

        consumer
            .subscribe_with_topics(vec![topic.clone()])
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));
        let expected: HashSet<TopicPartition> = [tp.clone(), tp1.clone()].into_iter().collect();
        await_assignment(consumer.as_mut(), backend, &expected, Duration::from_secs(90)).await;
        // Should auto-commit sought positions before closing.
        consumer
            .seek_with_offset(tp.clone(), 300)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: seek tp should succeed: {e}"));
        consumer
            .seek_with_offset(tp1.clone(), 500)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: seek tp1 should succeed: {e}"));
        consumer
            .close()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
    }

    // Now we should see the committed positions from another consumer.
    let mut another = create_consumer(ctx, factory, &group_id, true, &[]).await;
    let committed = another
        .committed(&[tp.clone(), tp1.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        committed
            .get(&tp)
            .and_then(Option::as_ref)
            .expect("tp committed present")
            .offset(),
        300,
        "values ({backend} backend)"
    );
    assert_eq!(
        committed
            .get(&tp1)
            .and_then(Option::as_ref)
            .expect("tp1 committed present")
            .offset(),
        500,
        "values ({backend} backend)"
    );
    another
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: another close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_auto_commit_on_close,
    async_consumer_auto_commit_on_close,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncConsumerAutoCommitOnCloseAfterWakeup`.
/// Same as above but `wakeup()` is called before close to
/// simulate breaking a poll loop from another thread; auto-commit still
/// flushes.
async fn async_consumer_auto_commit_on_close_after_wakeup<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_auto_commit_on_close_wakeup");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    {
        let mut consumer = create_consumer(ctx, factory, &group_id, true, &[]).await;
        // Empty topic (both partitions), as Java's `@BeforeEach` creates it.
        create_test_topic(ctx, &topic, 2).await;
        send_records_bytes(ctx, &tp, 1000, current_time_ms()).await;

        consumer
            .subscribe_with_topics(vec![topic.clone()])
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));
        let expected: HashSet<TopicPartition> = [tp.clone(), tp1.clone()].into_iter().collect();
        await_assignment(consumer.as_mut(), backend, &expected, Duration::from_secs(90)).await;
        consumer
            .seek_with_offset(tp.clone(), 300)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: seek tp should succeed: {e}"));
        consumer
            .seek_with_offset(tp1.clone(), 500)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: seek tp1 should succeed: {e}"));
        // Wakeup before closing to simulate breaking a poll loop from
        // another thread. The pending wakeup must not prevent close-path
        // auto-commit from flushing.
        consumer.wakeup();
        consumer
            .close()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed after wakeup: {e}"));
    }

    let mut another = create_consumer(ctx, factory, &group_id, true, &[]).await;
    let committed = another
        .committed(&[tp.clone(), tp1.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        committed
            .get(&tp)
            .and_then(Option::as_ref)
            .expect("tp committed present")
            .offset(),
        300,
        "values ({backend} backend)"
    );
    assert_eq!(
        committed
            .get(&tp1)
            .and_then(Option::as_ref)
            .expect("tp1 committed present")
            .offset(),
        500,
        "values ({backend} backend)"
    );
    another
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: another close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_auto_commit_on_close_after_wakeup,
    async_consumer_auto_commit_on_close_after_wakeup,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncConsumerCommitMetadata`.
///
/// Sync commit with leaderEpoch + metadata round-trips; async commit with
/// metadata round-trips; null metadata (empty string in Rust) round-trips.
async fn async_consumer_commit_metadata<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_commit_metadata");
    let tp = TopicPartition::new(topic.clone(), 0);

    let (mut consumer, log) = create_consumer_with_log(ctx, factory, &group_id, true).await;
    // Ensure the topic exists so assign() resolves a real partition.
    create_test_topic(ctx, &topic, 2).await;
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

    // Sync commit: offset 5, leaderEpoch 15, metadata "foo".
    let sync_metadata = OffsetAndMetadata::with_leader_epoch_metadata(5, Some(15), "foo").expect("OffsetAndMetadata");
    let mut sync_offsets = HashMap::new();
    sync_offsets.insert(tp.clone(), sync_metadata.clone());
    consumer
        .commit_sync_with_offsets(sync_offsets)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync_with_offsets should succeed: {e}"));
    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        committed.get(&tp).and_then(Option::as_ref).expect("tp committed present"),
        &sync_metadata,
        "values ({backend} backend)"
    );

    // Async commit: offset 10, metadata "bar".
    let async_metadata = OffsetAndMetadata::with_metadata(10, "bar").expect("OffsetAndMetadata");
    let mut async_offsets = HashMap::new();
    async_offsets.insert(tp.clone(), async_metadata.clone());
    send_and_await_async_commit(&mut consumer, &log, backend, async_offsets).await;
    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        committed.get(&tp).and_then(Option::as_ref).expect("tp committed present"),
        &async_metadata,
        "values ({backend} backend)"
    );

    // Null metadata: Java `new OffsetAndMetadata(5, null)` normalizes to
    // empty metadata. Rust `OffsetAndMetadata::new(5)` is the same
    // (empty-string metadata, no leader epoch).
    let null_metadata = OffsetAndMetadata::new(5).expect("OffsetAndMetadata");
    let mut null_offsets = HashMap::new();
    null_offsets.insert(tp.clone(), null_metadata.clone());
    consumer
        .commit_sync_with_offsets(null_offsets)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync_with_offsets should succeed: {e}"));
    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        committed.get(&tp).and_then(Option::as_ref).expect("tp committed present"),
        &null_metadata,
        "values ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_commit_metadata,
    async_consumer_commit_metadata,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncConsumerAsyncCommit`.
///
/// Five `commitAsync` calls with offsets 1..=5; poll until the callback
/// has observed 5 successes; assert no error, success count 5, and the
/// committed offset is 5.
async fn async_consumer_async_commit<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_async_commit");
    let tp = TopicPartition::new(topic.clone(), 0);

    let (mut consumer, log) = create_consumer_with_log(ctx, factory, &group_id, false).await;
    create_test_topic(ctx, &topic, 2).await;
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

    // Java pre-creates the GROUP_METADATA_TOPIC_NAME (offsets) topic up front to
    // prevent transient RetriableCommitFailed errors during the async commits
    // below (`org.apache.kafka.clients.consumer.PlaintextConsumerCommitTest#testAsyncCommit`).
    // This test uses a
    // `committed()` query instead: it discovers the coordinator and makes the
    // broker create the offsets topic, so the five async commits all succeed
    // rather than racing coordinator load. The topic is not created explicitly
    // because the cluster is pooled and its group coordinator is shared with
    // other tests.
    let _ = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed (coordinator readiness) should succeed: {e}"));

    let count = 5;
    for i in 1..=count {
        let mut offsets = HashMap::new();
        offsets.insert(tp.clone(), OffsetAndMetadata::new(i).expect("OffsetAndMetadata"));
        log.commit_async_offsets_with_logging_callback(&mut consumer, offsets)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: commit_async should enqueue: {e}"));
    }

    let commits = poll_until_commits(
        consumer.as_mut(),
        &log,
        backend,
        count as usize,
        Duration::from_secs(30),
        "Failed to observe commit callback before timeout",
    )
    .await;

    assert!(
        commits.iter().all(|e| e.error_value.is_none()),
        "no commit error expected: {commits:?} ({backend} backend)"
    );
    assert_eq!(
        commits.len(),
        count as usize,
        "success count should be {count} ({backend} backend)"
    );
    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        committed
            .get(&tp)
            .and_then(Option::as_ref)
            .expect("tp committed present")
            .offset(),
        count,
        "values ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_async_commit,
    async_consumer_async_commit,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncConsumerCommitSpecifiedOffsets`.
///
/// Commits specific per-partition offsets; positions are unchanged by the
/// commit; async commit picks up the committed changes.
async fn async_consumer_commit_specified_offsets<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_commit_specified_offsets");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    let (mut consumer, log) = create_consumer_with_log(ctx, factory, &group_id, false).await;
    create_test_topic(ctx, &topic, 2).await;
    let producer = build_producer_bytes(ctx);
    let now = current_time_ms();
    send_records_with_producer(&producer, &tp, 5, now).await;
    send_records_with_producer(&producer, &tp1, 7, now).await;
    producer.close().await.expect("producer close should succeed");

    consumer
        .assign(vec![tp.clone(), tp1.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

    let pos1 = consumer
        .position(&tp)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: position tp: {e}"));
    let pos2 = consumer
        .position(&tp1)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: position tp1: {e}"));

    let mut commit1 = HashMap::new();
    commit1.insert(tp.clone(), OffsetAndMetadata::new(3).expect("OffsetAndMetadata"));
    consumer
        .commit_sync_with_offsets(commit1)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync_with_offsets tp: {e}"));

    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed tp: {e}"));
    assert_eq!(
        committed
            .get(&tp)
            .and_then(Option::as_ref)
            .expect("tp committed present")
            .offset(),
        3,
        "values ({backend} backend)"
    );
    let committed_tp1 = consumer
        .committed(std::slice::from_ref(&tp1))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed tp1: {e}"));
    assert_eq!(
        committed_tp1.get(&tp1),
        Some(&None),
        "tp1 should not be committed yet ({backend} backend)"
    );

    // Positions should not change.
    assert_eq!(
        consumer
            .position(&tp)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: position tp: {e}")),
        pos1,
        "values ({backend} backend)"
    );
    assert_eq!(
        consumer
            .position(&tp1)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: position tp1: {e}")),
        pos2,
        "values ({backend} backend)"
    );

    let mut commit2 = HashMap::new();
    commit2.insert(tp1.clone(), OffsetAndMetadata::new(5).expect("OffsetAndMetadata"));
    consumer
        .commit_sync_with_offsets(commit2)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync_with_offsets tp1: {e}"));

    let committed = consumer
        .committed(&[tp.clone(), tp1.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed both: {e}"));
    assert_eq!(
        committed.get(&tp).and_then(Option::as_ref).expect("tp committed").offset(),
        3,
        "values ({backend} backend)"
    );
    assert_eq!(
        committed.get(&tp1).and_then(Option::as_ref).expect("tp1 committed").offset(),
        5,
        "values ({backend} backend)"
    );

    // Async should pick up the committed changes after commit completes.
    let mut commit3 = HashMap::new();
    commit3.insert(tp1.clone(), OffsetAndMetadata::new(7).expect("OffsetAndMetadata"));
    send_and_await_async_commit(&mut consumer, &log, backend, commit3).await;
    let committed = consumer
        .committed(std::slice::from_ref(&tp1))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed tp1: {e}"));
    assert_eq!(
        committed.get(&tp1).and_then(Option::as_ref).expect("tp1 committed").offset(),
        7,
        "values ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_commit_specified_offsets,
    async_consumer_commit_specified_offsets,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncConsumerAutoCommitOnRebalance`.
///
/// Auto-commit fires on rebalance; after the rebalance, `committed()`
/// reflects the seeks.
///
/// **Deviation (Issue 8):** Java's listener calls
/// `consumer.pause(partitions)` inside `onPartitionsAssigned` so that the
/// `awaitAssignment` poll loop does not advance `tp`'s fetch position past
/// the test's explicit seek (300) before the rebalance auto-commits it
/// (Java's `tp` has 1000 records). A Rust rebalance listener holds only
/// `&self` (as `Arc<dyn ConsumerRebalanceListener>`) and cannot call the
/// consumer's `&mut self` `pause()` (Issue 8 in the Phase-13 COMMENTS, the
/// structural gap documented in this phase's PLAN.md and the same gap that
/// `#[ignore]`s the poll-suite's commit-in-revocation test).
///
/// We make the seeks VALID instead of pausing: produce exactly 300 records
/// to `tp` and 500 to `tp1`, so `seek(tp, 300)` and `seek(tp1, 500)` land at
/// the log END. A fetch at the log end returns empty and does NOT advance or
/// reset the position, so the seeked positions survive until the rebalance
/// auto-commit captures them — deterministically, without a pause. (Java
/// produces 1000 to `tp` only and pauses `tp1`; producing up to each seek
/// target is the pause-free equivalent.) The auto-commit-on-rebalance +
/// `committed()` readback (the actual contract) is preserved faithfully.
async fn async_consumer_auto_commit_on_rebalance<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let topic2 = ctx.topic("topic2");
    let group_id = ctx.group_id("g_auto_commit_on_rebalance");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    let mut consumer = create_consumer(ctx, factory, &group_id, true, &[]).await;
    // Empty topics so produced records start at offset 0 (Java parity).
    create_test_topic(ctx, &topic, 2).await;
    create_test_topic(ctx, &topic2, 2).await;

    // Produce up to each seek target so the seeks below are in-range (log
    // end), so no fetch advances or resets the position — see the
    // pause-free deviation note above.
    let producer = build_producer_bytes(ctx);
    let now = current_time_ms();
    send_records_with_producer(&producer, &tp, 300, now).await;
    send_records_with_producer(&producer, &tp1, 500, now).await;
    producer.close().await.expect("producer close should succeed");

    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));
    let expected: HashSet<TopicPartition> = [tp.clone(), tp1.clone()].into_iter().collect();
    await_assignment(consumer.as_mut(), backend, &expected, Duration::from_secs(90)).await;

    consumer
        .seek_with_offset(tp.clone(), 300)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek tp: {e}"));
    consumer
        .seek_with_offset(tp1.clone(), 500)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek tp1: {e}"));

    // Change subscription to trigger a rebalance — auto-commit fires on
    // the revocation that precedes the new assignment.
    consumer
        .subscribe_with_topics(vec![topic.clone(), topic2.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: re-subscribe should succeed: {e}"));

    let new_assignment: HashSet<TopicPartition> = [
        tp.clone(),
        tp1.clone(),
        TopicPartition::new(topic2.clone(), 0),
        TopicPartition::new(topic2.clone(), 1),
    ]
    .into_iter()
    .collect();
    await_assignment(consumer.as_mut(), backend, &new_assignment, Duration::from_secs(90)).await;

    // After rebalancing, we should have reset to the committed positions.
    let committed = consumer
        .committed(&[tp.clone(), tp1.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        committed.get(&tp).and_then(Option::as_ref).expect("tp committed").offset(),
        300,
        "values ({backend} backend)"
    );
    assert_eq!(
        committed.get(&tp1).and_then(Option::as_ref).expect("tp1 committed").offset(),
        500,
        "values ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_auto_commit_on_rebalance,
    async_consumer_auto_commit_on_rebalance,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncConsumerSubscribeAndCommitSync`.
///
/// Ensures the member ID is propagated from the group coordinator when
/// the assignment is received into a subsequent offset commit:
/// subscribe → await assignment → seek(0) → `commit_sync()`.
async fn async_consumer_subscribe_and_commit_sync<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_subscribe_and_commit_sync");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    let mut consumer = create_consumer(ctx, factory, &group_id, false, &[]).await;
    create_test_topic(ctx, &topic, 2).await;
    assert_eq!(consumer.assignment().len(), 0, "assignment should be empty ({backend} backend)");
    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));
    let expected: HashSet<TopicPartition> = [tp.clone(), tp1.clone()].into_iter().collect();
    await_assignment(consumer.as_mut(), backend, &expected, Duration::from_secs(90)).await;

    consumer
        .seek_with_offset(tp.clone(), 0)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek tp: {e}"));
    consumer
        .commit_sync()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync should succeed: {e}"));

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_subscribe_and_commit_sync,
    async_consumer_subscribe_and_commit_sync,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncConsumerPositionAndCommit`.
///
/// `position()` on an unassigned partition throws `LocalIllegalState`; after
/// assigning, position resets to 0; commit/position interplay; another
/// consumer in the same group reads from the committed position.
async fn async_consumer_position_and_commit<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_position_and_commit");
    let tp = TopicPartition::new(topic.clone(), 0);

    let mut consumer = create_consumer(ctx, factory, &group_id, false, &[]).await;
    let mut other = create_consumer(ctx, factory, &group_id, false, &[]).await;

    // Empty topic so produced records start at offset 0 (Java parity).
    create_test_topic(ctx, &topic, 2).await;
    let producer = build_producer_bytes(ctx);
    let starting_timestamp = current_time_ms();
    send_records_with_producer(&producer, &tp, 5, starting_timestamp).await;

    // Partition 15 does not exist / is not assigned.
    let unassigned = TopicPartition::new(topic.clone(), 15);
    let committed = consumer
        .committed(std::slice::from_ref(&unassigned))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        committed.get(&unassigned),
        Some(&None),
        "unassigned tp should have no committed offset ({backend} backend)"
    );

    // position() on a partition we are not subscribed to throws.
    let err = consumer
        .position(&unassigned)
        .await
        .expect_err("position on unassigned should err");
    assert!(
        matches!(err, Error::LocalIllegalState(_)),
        "expected IllegalState for position() on unassigned partition, got {err:?} ({backend} backend)"
    );
    // The message of org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#position,
    // so the class check cannot pass on any other IllegalState.
    assert_eq!(
        err.message(),
        "You can only check the position for partitions assigned to this consumer.",
        "position() on unassigned partition message ({backend} backend)"
    );

    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

    assert_eq!(
        consumer
            .position(&tp)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: position tp: {e}")),
        0,
        "position() on a subscribed partition should reset the offset to 0"
    );
    consumer
        .commit_sync()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync should succeed: {e}"));
    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        committed.get(&tp).and_then(Option::as_ref).expect("tp committed").offset(),
        0,
        "values ({backend} backend)"
    );

    consume_and_verify_records_bytes(consumer.as_mut(), backend, &tp, 5, 0, 0, starting_timestamp).await;
    assert_eq!(
        consumer
            .position(&tp)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: position tp: {e}")),
        5,
        "after consuming 5 records, position should be 5"
    );
    consumer
        .commit_sync()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync should succeed: {e}"));
    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        committed.get(&tp).and_then(Option::as_ref).expect("tp committed").offset(),
        5,
        "committed offset should be returned ({backend} backend)"
    );

    let starting_timestamp2 = current_time_ms();
    send_records_with_producer(&producer, &tp, 1, starting_timestamp2).await;
    producer.close().await.expect("producer close should succeed");

    // Another consumer in the same group should get the same position.
    other
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: other assign should succeed: {e}"));
    consume_and_verify_records_bytes(other.as_mut(), backend, &tp, 1, 5, 0, starting_timestamp2).await;

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
    other
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: other close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_position_and_commit,
    async_consumer_position_and_commit,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `PlaintextConsumerCommitTest#testCommitAsyncCompletedBeforeConsumerCloses`.
///
/// Contract: async offset commits complete before the consumer is closed,
/// even when no commit-sync is performed as part of close (auto-commit
/// disabled).
async fn commit_async_completed_before_consumer_closes<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_async_completed_before_close");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    // Create offsets topic to ensure coordinator is available during close, as
    // org.apache.kafka.clients.consumer.PlaintextConsumerCommitTest#testCommitAsyncCompletedBeforeConsumerCloses
    // does with `cluster.createTopic(Topic.GROUP_METADATA_TOPIC_NAME,
    // OFFSETS_TOPIC_PARTITIONS = 1, OFFSETS_TOPIC_REPLICATION = 3)`, the constants
    // being `PlaintextConsumerCommitTest#OFFSETS_TOPIC_PARTITIONS` and
    // `PlaintextConsumerCommitTest#OFFSETS_TOPIC_REPLICATION`. Java gets a fresh
    // cluster per test; the pooled cluster may already hold the topic from an
    // earlier test, in which case `TopicExists` means the precondition holds.
    {
        const GROUP_METADATA_TOPIC_NAME: &str = "__consumer_offsets";
        let admin = admin_for(ctx);
        let new_topic = confluent_kafka::admin::NewTopic::with_num_partitions_replication_factor(
            GROUP_METADATA_TOPIC_NAME.to_string(),
            Some(1),
            Some(3),
        );
        match admin.create_topics(&[new_topic]).all().get().await {
            Ok(()) | Err(Error::TopicExists(_)) => {},
            Err(e) => panic!("creating {GROUP_METADATA_TOPIC_NAME} failed: {e:?}"),
        }
        test_utils::wait_for_all_partitions_metadata(admin.as_ref(), GROUP_METADATA_TOPIC_NAME, 1).await;
        admin.close_with_timeout(Duration::from_secs(5)).await;
    }

    let log = {
        let (mut consumer, log) = create_consumer_with_log(ctx, factory, &group_id, false).await;
        create_test_topic(ctx, &topic, 2).await;

        let producer = build_producer_bytes(ctx);
        let now = current_time_ms();
        send_records_with_producer(&producer, &tp, 3, now).await;
        send_records_with_producer(&producer, &tp1, 3, now).await;
        producer.close().await.expect("producer close should succeed");

        consumer
            .assign(vec![tp.clone(), tp1.clone()])
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

        // Try without looking up the coordinator first.
        let mut o1 = HashMap::new();
        o1.insert(tp.clone(), OffsetAndMetadata::new(1).expect("OffsetAndMetadata"));
        log.commit_async_offsets_with_logging_callback(&mut consumer, o1)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: commitAsync 1: {e}"));
        let mut o2 = HashMap::new();
        o2.insert(tp1.clone(), OffsetAndMetadata::new(1).expect("OffsetAndMetadata"));
        log.commit_async_offsets_with_logging_callback(&mut consumer, o2)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: commitAsync 2: {e}"));

        consumer
            .close()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
        log
    };
    // The two async commits must have completed before close returned.
    assert_eq!(
        successful_commits(&log, backend).await,
        2,
        "both async commits should complete before close ({backend} backend)"
    );
}

multilanguage_consumer_test!(
    test_commit_async_completed_before_consumer_closes,
    commit_async_completed_before_consumer_closes,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testCommitAsyncCompletedBeforeCommitSyncReturns`.
///
/// Contract: async commits sent before a `commitSync` are guaranteed to
/// have their callbacks invoked prior to completion of `commitSync`.
async fn commit_async_completed_before_commit_sync_returns<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_async_completed_before_commit_sync");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    let (mut consumer, log) = create_consumer_with_log(ctx, factory, &group_id, false).await;
    create_test_topic(ctx, &topic, 2).await;
    let producer = build_producer_bytes(ctx);
    let now = current_time_ms();
    send_records_with_producer(&producer, &tp, 3, now).await;
    send_records_with_producer(&producer, &tp1, 3, now).await;
    producer.close().await.expect("producer close should succeed");

    consumer
        .assign(vec![tp.clone(), tp1.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

    // Try without looking up the coordinator first.
    let mut o1 = HashMap::new();
    o1.insert(tp.clone(), OffsetAndMetadata::new(1).expect("OffsetAndMetadata"));
    log.commit_async_offsets_with_logging_callback(&mut consumer, o1)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commitAsync 1: {e}"));
    // Empty sync commit: the async callback must fire before it returns.
    consumer
        .commit_sync_with_offsets(HashMap::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync empty: {e}"));

    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed: {e}"));
    assert_eq!(
        committed.get(&tp).and_then(Option::as_ref).expect("tp committed").offset(),
        1,
        "values ({backend} backend)"
    );
    assert_eq!(
        successful_commits(&log, backend).await,
        1,
        "async callback should fire before commitSync returns ({backend} backend)"
    );

    // Try with coordinator known.
    let mut o2 = HashMap::new();
    o2.insert(tp.clone(), OffsetAndMetadata::new(2).expect("OffsetAndMetadata"));
    log.commit_async_offsets_with_logging_callback(&mut consumer, o2)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commitAsync 2: {e}"));
    let mut sync2 = HashMap::new();
    sync2.insert(tp1.clone(), OffsetAndMetadata::new(2).expect("OffsetAndMetadata"));
    consumer
        .commit_sync_with_offsets(sync2)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync tp1: {e}"));

    let committed = consumer
        .committed(&[tp.clone(), tp1.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed: {e}"));
    assert_eq!(
        committed.get(&tp).and_then(Option::as_ref).expect("tp committed").offset(),
        2,
        "values ({backend} backend)"
    );
    assert_eq!(
        committed.get(&tp1).and_then(Option::as_ref).expect("tp1 committed").offset(),
        2,
        "values ({backend} backend)"
    );
    assert_eq!(successful_commits(&log, backend).await, 2, "success count ({backend} backend)");

    // Try with empty sync commit.
    let mut o3 = HashMap::new();
    o3.insert(tp.clone(), OffsetAndMetadata::new(3).expect("OffsetAndMetadata"));
    log.commit_async_offsets_with_logging_callback(&mut consumer, o3)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commitAsync 3: {e}"));
    consumer
        .commit_sync_with_offsets(HashMap::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync empty: {e}"));

    let committed = consumer
        .committed(&[tp.clone(), tp1.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed: {e}"));
    assert_eq!(
        committed.get(&tp).and_then(Option::as_ref).expect("tp committed").offset(),
        3,
        "values ({backend} backend)"
    );
    assert_eq!(
        committed.get(&tp1).and_then(Option::as_ref).expect("tp1 committed").offset(),
        2,
        "values ({backend} backend)"
    );
    assert_eq!(successful_commits(&log, backend).await, 3, "success count ({backend} backend)");

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_commit_async_completed_before_commit_sync_returns,
    commit_async_completed_before_commit_sync_returns,
    cluster_config_with_kip848_3brokers()
);

/// Cluster for [`test_commit_async_fails_when_coordinator_unavailable_during_close`]:
/// the suite's `@ClusterTestDefaults` shape (`types = {Type.KRAFT}`,
/// `brokers = BROKER_COUNT` = 3, offsets topic 1 partition / RF 3,
/// `group.min.session.timeout.ms=100`) as a **dedicated** cluster with an
/// isolated controller, since the test stops every broker.
fn dedicated_kraft_3brokers() -> ClusterConfig {
    let props = BTreeMap::from([
        (
            "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS".to_string(),
            "classic,consumer".to_string(),
        ),
        ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS".to_string(), "1".to_string()),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), "3".to_string()),
        ("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS".to_string(), "100".to_string()),
    ]);
    ClusterConfig::kraft_dedicated(3, 1).set_server_properties(props)
}

/// Translates Java's `PlaintextConsumerCommitTest#testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose`
/// (AK 4.3.1).
///
/// Every broker is stopped before the consumer ever looks up its coordinator
/// (it only `assign`s), so the async commit issued afterwards can never find
/// one; closing with a 500 ms timeout must fail that commit with a
/// `CommitFailedException` carrying the exact message
/// `"Failed to commit offsets: Coordinator unknown and consumer is closing"`,
/// fire the callback exactly once, and return in under 1 s.
async fn commit_async_fails_when_coordinator_unavailable_during_close<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_coordinator_unavailable_during_close");
    let tp = TopicPartition::new(topic.clone(), 0);

    // Java `@BeforeEach`: `cluster.createTopic(topic, 2, (short) BROKER_COUNT)`.
    create_test_topic(ctx, &topic, 2).await;

    let (mut consumer, log) = create_consumer_with_log(ctx, factory, &group_id, false).await;
    let producer = build_producer_bytes(ctx);
    send_records_with_producer(&producer, &tp, 3, current_time_ms()).await;
    producer.close().await.expect("producer close should succeed");

    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

    // Close the coordinator before committing because otherwise the commit
    // will fail to find the coordinator.
    // Java: `cluster.brokerIds().forEach(cluster::shutdownBroker)`.
    for broker_id in ctx.cluster().broker_ids() {
        ctx.cluster().shutdown_broker(broker_id).await;
    }
    // Java: `waitForCondition(() -> cluster.aliveBrokers().isEmpty(), ...)`.
    // `shutdown_broker` returns once the container has stopped, so the alive
    // set is already empty here.
    assert!(
        ctx.cluster().alive_broker_ids().is_empty(),
        "All brokers should be shut down ({backend} backend)"
    );

    consumer
        .poll(Duration::from_millis(500))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: poll with every broker down should not fail: {e}"));
    let mut offsets = HashMap::new();
    offsets.insert(tp.clone(), OffsetAndMetadata::new(1).expect("OffsetAndMetadata"));
    log.commit_async_offsets_with_logging_callback(&mut consumer, offsets)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commitAsync: {e}"));

    let start = Instant::now();
    consumer
        .close_with_options(confluent_kafka::consumer::CloseOptions::new_with_timeout(
            Duration::from_millis(500),
        ))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close should complete: {e}"));
    let close_duration = start.elapsed();

    assert!(
        close_duration < Duration::from_secs(1),
        "close too long: {close_duration:?} ({backend} backend)"
    );
    let commits = commit_entries(&log, backend).await;
    let failures: Vec<&CallbackLogEntry> = commits.iter().filter(|e| e.error_value.is_some()).collect();
    let err = failures
        .last()
        .and_then(|e| e.error_value.clone())
        .unwrap_or_else(|| panic!("callback should have recorded an error: {commits:?} ({backend} backend)"));
    // Java asserts `CommitFailedException`, which is now its own Rust class
    // rather than being flattened into `Error::LocalIllegalState`.
    assert!(
        matches!(&err, Error::ConsumerCommitFailed(e)
            if e.message() == "Failed to commit offsets: Coordinator unknown and consumer is closing"),
        "expected the exact commit-failed message, got {err:?} ({backend} backend)"
    );
    // Java's `getMessage()` is `Error::message()`; `Display` is `toString()`,
    // which now prefixes the class name for every translated class.
    assert_eq!(
        err.message(),
        "Failed to commit offsets: Coordinator unknown and consumer is closing",
        "message ({backend} backend)"
    );
    assert_eq!(failures.len(), 1, "error count: {commits:?} ({backend} backend)");
}

multilanguage_consumer_test!(
    test_commit_async_fails_when_coordinator_unavailable_during_close,
    commit_async_fails_when_coordinator_unavailable_during_close,
    dedicated_kraft_3brokers()
);

// ── Tests added in Apache Kafka 4.3.1 ─────────────────────────────────

/// Builds an admin client for topic provisioning / deletion. Mirrors the
/// per-file `admin_for` helper the other integration tests use (Java's
/// `cluster.admin()`).
fn admin_for(ctx: &TestContext) -> Box<dyn Admin> {
    let mut props = HashMap::from([
        ("client.id".to_string(), "commit-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    ctx.configure(&mut props);
    let config = AdminClientConfig::new(&props).expect("valid admin config");
    AdminClient::create(config).expect("admin client")
}

/// Translates Java's `PlaintextConsumerCommitTest#testAsyncConsumerNoCommittedOffsets`
/// (body `PlaintextConsumerCommitTest#testNoCommittedOffsets`).
///
/// Fetches committed offsets for three partitions: `tp` (committed), `tp1`
/// (exists, never committed) and `tp2` (partition 2 of a 2-partition topic —
/// does not exist).
async fn async_consumer_no_committed_offsets<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_no_committed_offsets");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    // Java `@BeforeEach`: `cluster.createTopic(topic, 2, BROKER_COUNT)`.
    create_test_topic(ctx, &topic, 2).await;

    // Java: `createConsumer(groupProtocol, true)`.
    let mut consumer = create_consumer(ctx, factory, &group_id, true, &[]).await;
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

    // commit for one partition
    let metadata = OffsetAndMetadata::with_leader_epoch_metadata(5, Some(15), "foo").expect("OffsetAndMetadata");
    consumer
        .commit_sync_with_offsets(HashMap::from([(tp.clone(), metadata.clone())]))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync_with_offsets should succeed: {e}"));

    let tp2 = TopicPartition::new(topic.clone(), 2);
    // fetch offset for three partitions:
    // 1. tp: exists and has committed offset
    // 2. tp1: exists but has NO committed offset
    // 3. tp2: does not exist
    let committed = consumer
        .committed(&[tp.clone(), tp1.clone(), tp2.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        3,
        committed.len(),
        "unexpected committed map: {committed:?} ({backend} backend)"
    );
    assert_eq!(Some(&Some(metadata)), committed.get(&tp), "values ({backend} backend)");
    assert_eq!(Some(&None), committed.get(&tp1), "values ({backend} backend)");
    assert_eq!(Some(&None), committed.get(&tp2), "values ({backend} backend)");

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_no_committed_offsets,
    async_consumer_no_committed_offsets,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `PlaintextConsumerCommitTest#testAsyncConsumerCommittedDeletedTopic`
/// (body `PlaintextConsumerCommitTest#testConsumerCommittedDeletedTopic`) — validates
/// KAFKA-20165.
///
/// Calling `committed()` for a deleted topic eventually returns "no offset"
/// (Java `null`, Rust `None`) for the partition: the group coordinator
/// answers `UNKNOWN_TOPIC_ID` for the deleted topic when the client uses topic
/// ids, which the consumer handles as a retriable partition error and reports
/// as no committed offset.
async fn async_consumer_committed_deleted_topic<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic_to_delete = ctx.topic("topic-to-delete");
    let group_id = ctx.group_id("g_committed_deleted_topic");
    // Java: `cluster.createTopic(topicToDelete, 1, (short) BROKER_COUNT)`.
    let admin = admin_for(ctx);
    test_utils::create_topic(admin.as_ref(), &topic_to_delete, 1, 3).await;
    let tp_to_delete = TopicPartition::new(topic_to_delete.clone(), 0);

    // Java: `createConsumer(groupProtocol, false)`.
    let mut consumer = create_consumer(ctx, factory, &group_id, false, &[]).await;
    consumer
        .assign(vec![tp_to_delete.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

    // Commit an offset to ensure the consumer has the topic ID cached and there's data to fetch
    consumer
        .commit_sync_with_offsets(HashMap::from([(
            tp_to_delete.clone(),
            OffsetAndMetadata::new(0).expect("OffsetAndMetadata"),
        )]))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync_with_offsets should succeed: {e}"));

    // Verify the commit was successful
    let committed = consumer
        .committed(std::slice::from_ref(&tp_to_delete))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        0,
        committed
            .get(&tp_to_delete)
            .and_then(Option::as_ref)
            .expect("committed entry present")
            .offset(),
        "values ({backend} backend)"
    );

    // Delete the topic
    admin
        .delete_topics_with_topic_collection(TopicCollection::of_topic_names(vec![topic_to_delete.clone()]))
        .all()
        .get()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete_topics should succeed: {e}"));

    // Eventually, the response should return null for the deleted topic partition.
    // Java: `TestUtils.waitForCondition(..., 10000, ...)`, which retries on both
    // a false condition and an exception thrown by `committed(...)`.
    let expected_end = Instant::now() + Duration::from_millis(10_000);
    loop {
        let failure = match consumer
            .committed_with_timeout(std::slice::from_ref(&tp_to_delete), Duration::from_millis(5000))
            .await
        {
            Ok(committed) if committed.get(&tp_to_delete).is_some_and(Option::is_none) => break,
            Ok(committed) => format!(
                "Condition not met within timeout 10000. Expected null for deleted topic partition (got {committed:?})"
            ),
            Err(err) => format!("committed() failed: {err}"),
        };
        if Instant::now() >= expected_end {
            panic!("Assertion failed after 10000ms: {failure}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

multilanguage_consumer_test!(
    test_async_consumer_committed_deleted_topic,
    async_consumer_committed_deleted_topic,
    cluster_config_with_kip848_3brokers()
);
