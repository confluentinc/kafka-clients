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
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerAssignTest.java`
//! at pinned commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.
//!
//! These tests exercise the **manual-assignment** consumer API
//! (`assign`, `commit_sync`, `commit_async`, `committed`, `position`,
//! `seek`) against a real 3-broker Kafka 4.2.0 cluster with KIP-848
//! (`group.protocol=consumer`) enabled.
//!
//! They run as multilanguage scenarios: each one runs against the native
//! Rust consumer and, through the gRPC backends, the Python (sync and
//! asyncio) and C consumers ([`multilanguage_consumer_test!`]); each name
//! below gains the macro's backend suffix (`__rust`, `__grpc_python`,
//! `__grpc_python_async`, `__grpc_c`). The setup producer and admin client
//! are native fixtures; only the consumer is under test. The commit callback
//! of `test_async_assign_and_commit_async_not_committed` is the backend's own
//! logging callback ([`ConsumerCallbackLog`]), asserted through its log.
//!
//! # Translated methods (KIP-848 / `GroupProtocol.CONSUMER` arm only)
//!
//! All 8 KIP-848 tests are translated and pass. Phase-13a's pilot
//! `#[ignore]` markers were closed by Phase-13a's `fetch_offsets_with_retries`
//! retry loop + coordinator-unknown wiring (see commit fixing Issue 1 in
//! `design/history/Milestone-8/Phase-13/COMMENTS.DONE.1.md`); that single
//! fix resolved all four documented production gaps because the retry
//! driver also feeds the OffsetFetch-on-startup path and the
//! commit-after-poll path.
//!
//! - `testAsyncAssignAndCommitAsyncNotCommitted`
//!   → `test_async_assign_and_commit_async_not_committed`
//! - `testAsyncAssignAndCommitSyncNotCommitted`
//!   → `test_async_assign_and_commit_sync_not_committed`
//! - `testAsyncAssignAndCommitSyncAllConsumed`
//!   → `test_async_assign_and_commit_sync_all_consumed`
//! - `testAsyncAssignAndConsume`
//!   → `test_async_assign_and_consume`
//! - `testAsyncAssignAndConsumeSkippingPosition`
//!   → `test_async_assign_and_consume_skipping_position`
//! - `testAsyncAssignAndFetchCommittedOffsets`
//!   → `test_async_assign_and_fetch_committed_offsets`
//! - `testAsyncAssignAndConsumeFromCommittedOffsets`
//!   → `test_async_assign_and_consume_from_committed_offsets`
//! - `testAsyncAssignAndRetrievingCommittedOffsetsMultipleTimes`
//!   → `test_async_assign_and_retrieving_committed_offsets_multiple_times`
//!
//! # Added in Apache Kafka 4.3.1
//!
//! - `testAsyncPollAfterTopicDeleted` (4.3.1, KAFKA-20165)
//!   → `test_async_poll_after_topic_deleted`
//!
//! # SKIPped methods
//!
//! The 9 `testClassic*` twin methods are SKIPped — the project targets
//! KIP-848 only per `.claude/rules/consumer-threading.md` §20. They are
//! enumerated here for traceability:
//!
//! - SKIP: `testClassicAssignAndCommitAsyncNotCommitted` — classic-protocol-only
//! - SKIP: `testClassicAssignAndCommitSyncNotCommitted` — classic-protocol-only
//! - SKIP: `testClassicAssignAndCommitSyncAllConsumed` — classic-protocol-only
//! - SKIP: `testClassicAssignAndConsume` — classic-protocol-only
//! - SKIP: `testClassicAssignAndConsumeSkippingPosition` — classic-protocol-only
//! - SKIP: `testClassicAssignAndFetchCommittedOffsets` — classic-protocol-only
//! - SKIP: `testClassicAssignAndConsumeFromCommittedOffsets` — classic-protocol-only
//! - SKIP: `testClassicAssignAndRetrievingCommittedOffsetsMultipleTimes` — classic-protocol-only
//! - SKIP: `testClassicPollAfterTopicDeleted` — classic-protocol-only

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::admin::Admin;
use confluent_kafka::admin::AdminClient;
use confluent_kafka::admin::AdminClientConfig;
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
use crate::common::callback_log::{ConsumerCallbackLog, KIND_COMMIT};
use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
use crate::common::consumer_scenario::poll_until_entry;
use crate::common::test_context::TestContext;
use crate::common::test_utils;
use crate::multilanguage_consumer_test;

// The bytes-typed consumer trait object every backend factory returns. Used in
// helper signatures; the tests pass `&mut *consumer` (a reborrow through the
// `Box<dyn DynConsumer<Vec<u8>, Vec<u8>>>`).
type BytesConsumer = dyn DynConsumer<Vec<u8>, Vec<u8>>;

// ── Cluster config ────────────────────────────────────────────────────

/// Cluster config matching the Java suite's `@ClusterTestDefaults`:
/// 3 brokers, KIP-848 enabled on the coordinator, plus the broker
/// properties from the `serverProperties` annotation.
///
/// Mirrors:
/// ```text
/// brokers = PlaintextConsumerAssignTest.BROKER_COUNT (= 3)
/// offsets.topic.replication.factor = 3
/// offsets.topic.num.partitions     = 1
/// group.min.session.timeout.ms     = 100
/// group.initial.rebalance.delay.ms = 10
/// ```
///
/// All tests in this file share this cluster config so the pool in
/// `tests/common/cluster_pool.rs` materializes one 3-broker cluster
/// and amortizes its 30–60s startup across the suite.
fn cluster_config_with_kip848_3brokers() -> ClusterConfig {
    // The canonical helper supplies the shared KIP-848 broker tuning.
    kip848_3_broker(1)
}

// ── Consumer config builder ───────────────────────────────────────────

/// Build the consumer properties matching what Java's
/// `clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, "consumer"))`
/// produces, on the bootstrap the backend under test reaches.
///
/// `auto.offset.reset=earliest` and `enable.auto.commit=false` so the
/// tests' explicit `seek` / `commit_sync` / `commit_async` calls are the
/// only offset-state transitions.
fn make_consumer_props<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    group_id: &str,
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
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("group.id".to_string(), group_id.to_string()),
    ]);
    ctx.apply_security(&mut props);
    props
}

/// Create the consumer under test through `factory`.
async fn create_consumer<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    group_id: &str,
) -> Box<BytesConsumer> {
    factory
        .create(make_consumer_props(ctx, factory, group_id))
        .await
        .unwrap_or_else(|e| panic!("{} backend: create consumer: {e}", factory.name()))
}

// ── Producer helpers (mirror Java's ClientsTestUtils.sendRecords) ─────

/// Build a `ProducerConfig` aligned with the existing producer
/// integration tests (acks=all so produced records are durable before
/// the consumer reads them).
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

/// Translates Java's `ClientsTestUtils.sendRecords(cluster, tp, num,
/// startingTimestamp)` (with the default `timestampIncrement = -1`,
/// which Java interprets as "use 1ms per record").
///
/// Keys are `b"key {i}"` and values are `b"value {i}"`, matching Java's
/// `KEY_PREFIX = "key "` and `VALUE_PREFIX = "value "`. Timestamps are
/// `starting_timestamp + i` (1ms increment, mirroring Java's
/// `ClientsTestUtils.consumeAndVerifyRecords`).
///
/// All records are fired to the producer up front, then `flush()` waits
/// for the broker acks. The producer is then closed.
async fn send_records_bytes(ctx: &TestContext, tp: &TopicPartition, num_records: usize, starting_timestamp: i64) {
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> = KafkaProducer::new(
        make_producer_config(ctx),
        ByteArraySerializer::default(),
        ByteArraySerializer::default(),
    )
    .expect("Failed to build test producer");

    let mut last_future = None;
    for i in 0..num_records {
        // Java: `timestamp = startingTimestamp + i * (timestampIncrement > 0 ? timestampIncrement : 1)`
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
        .expect("ProducerRecord::with_partition_timestamp_key should not fail for non-negative ts/partition");
        last_future = Some(
            <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
                .await
                .expect("send should not fail"),
        );
    }
    // Flush ensures every send is acked before we hand back; the last
    // future's wait would also suffice but `flush()` better matches the
    // Java helper's semantics (`producer.flush()` at end).
    producer.flush().await.expect("producer.flush should succeed");
    if let Some(f) = last_future {
        // Belt-and-braces — `flush` already awaited; this is a no-op for
        // a completed future but surfaces any error.
        f.get_with_timeout(Duration::from_secs(30))
            .await
            .expect("last send should succeed");
    }
    producer.close().await.expect("producer close should succeed");
}

/// Java `PlaintextConsumerAssignTest#BROKER_COUNT`.
const BROKER_COUNT: i16 = 3;

/// Pre-create an EMPTY topic, mirroring Java's
/// `PlaintextConsumerAssignTest#setup` which does
/// `clusterInstance.createTopic(topic, 2, (short) BROKER_COUNT)` in
/// `@BeforeEach` — i.e. the topic
/// is created with no records BEFORE any records are produced.
///
/// Creates the topic through the admin client and then waits until every
/// partition leader accepts leader-only requests. Auto-creating the topic
/// (by producing or via metadata) instead races leader election on the
/// 3-broker cluster: `NOT_LEADER_OR_FOLLOWER` retries reorder the
/// non-idempotent sends, breaking the exact-offset/timestamp verification.
async fn create_test_topic(ctx: &TestContext, topic: &str, num_partitions: i32) {
    let admin = admin_for(ctx);
    test_utils::create_topic(admin.as_ref(), topic, num_partitions, BROKER_COUNT).await;
    test_utils::wait_for_partition_leaders(admin.as_ref(), topic, 0..num_partitions).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

// ── Consumer test helpers (mirror ClientsTestUtils.poll/consume) ──────

/// Translates Java's `ClientsTestUtils.consumeAndVerifyRecords(consumer,
/// tp, numRecords, startingOffset, startingKeyAndValueIndex,
/// startingTimestamp)` (with `timestampIncrement = -1`, i.e. 1ms per
/// record).
///
/// Drives `poll(100ms)` in a loop until `num_records` records have been
/// collected (or the 60s wall-clock budget elapses), then asserts on
/// `topic`, `partition`, `timestamp_type == CreateTime`, `timestamp`,
/// `offset`, key/value bytes, and the serialized-size accessors — the
/// full Java assertion set. Inlines verification because
/// `ConsumerRecord` is not `Clone`.
async fn consume_and_verify_records_bytes(
    consumer: &mut BytesConsumer,
    tp: &TopicPartition,
    num_records: usize,
    starting_offset: i64,
    starting_key_and_value_index: usize,
    starting_timestamp: i64,
    backend: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut next_index: usize = 0;
    while next_index < num_records && Instant::now() < deadline {
        let records = consumer.poll(Duration::from_millis(100)).await.unwrap_or_else(|e| {
            panic!("{backend} backend: poll should succeed in consume_and_verify_records_bytes: {e}")
        });
        // `into_iter()` consumes the batch; we own each record and can
        // inspect every field. `ConsumerRecord` is not `Clone`, so we
        // verify inline rather than buffering into a `Vec`.
        for record in records {
            if next_index >= num_records {
                // We have already verified `num_records` records; the
                // poll returned more — that's allowed by Java's
                // `consumeRecords` semantics (`assertTrue(records.count()
                // <= maxPollRecords)` only enforces an upper bound).
                // Stop verifying additional records.
                break;
            }
            let i = next_index;
            let offset = starting_offset + i as i64;

            assert_eq!(
                record.topic(),
                tp.topic(),
                "record topic should match tp.topic() ({backend} backend)"
            );
            assert_eq!(
                record.partition(),
                tp.partition(),
                "record partition should match tp.partition() ({backend} backend)"
            );

            // Offset is asserted BEFORE timestamp deliberately. Record `i` is
            // produced with timestamp `starting_timestamp + i`, so a timestamp
            // mismatch identifies *which* record arrived — but only the offset
            // says whether the wrong record was delivered at the right offset
            // (producer reordering) or the right record at the wrong offset
            // (consumer position). Asserting timestamp first hides the offset
            // and makes the failure un-diagnosable.
            assert_eq!(record.offset(), offset, "record offset should be {offset} ({backend} backend)");

            assert_eq!(
                record.timestamp_type(),
                TimestampType::CreateTime,
                "record timestamp_type should be CreateTime (broker default) ({backend} backend)"
            );
            let expected_ts = starting_timestamp + i as i64;
            assert_eq!(
                record.timestamp(),
                expected_ts,
                "record timestamp should be {expected_ts} (expected record #{i} at offset {offset}); \
                 the observed timestamp is that of produced record #{} ({backend} backend)",
                record.timestamp() - starting_timestamp
            );

            let key_and_value_index = starting_key_and_value_index + i;
            let expected_key = format!("key {key_and_value_index}").into_bytes();
            let expected_value = format!("value {key_and_value_index}").into_bytes();
            assert_eq!(
                record.key().expect("record key should be present"),
                &expected_key,
                "key at index {i} mismatched ({backend} backend)"
            );
            assert_eq!(
                record.value().expect("record value should be present"),
                &expected_value,
                "value at index {i} mismatched ({backend} backend)"
            );

            assert_eq!(
                record.serialized_key_size() as usize,
                expected_key.len(),
                "serialized_key_size mismatch at index {i} ({backend} backend)"
            );
            assert_eq!(
                record.serialized_value_size() as usize,
                expected_value.len(),
                "serialized_value_size mismatch at index {i} ({backend} backend)"
            );

            next_index += 1;
        }
    }
    assert_eq!(
        next_index, num_records,
        "Timed out before consuming expected {num_records} records (got {next_index}) ({backend} backend)"
    );
}

// ── Tests ─────────────────────────────────────────────────────────────

/// Translates Java's `testAsyncAssignAndCommitAsyncNotCommitted`.
/// The consumer assigns a partition (no `poll()` first) and
/// fires `commitAsync(callback)`. Because there is no fetch position,
/// the commit completes successfully but commits no offset — the
/// follow-up `committed(...)` returns a map with no entry for `tp`.
///
/// Java's `CountConsumerCommitCallback` counts successes and keeps the last
/// error; `pollUntilTrue` waits for `successCount >= 1 || lastError.isPresent()`.
/// Here the backend's logging callback records each completion as a `commit`
/// entry (its error, if any, in the entry), so waiting for the first `commit`
/// entry is the same condition.
///
/// §31 parity: the `OffsetCommitCallback` is delivered on the caller's
/// task by the polling loop (each `poll()` invocation drains
/// the pending-callback queue via
/// `OffsetCommitCallbackInvoker::invoke_pending_callbacks`).
async fn async_assign_and_commit_async_not_committed<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_commit_async_not_committed");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records = 10_000;
    let starting_timestamp = current_time_ms();

    let (mut consumer, log): (Box<BytesConsumer>, ConsumerCallbackLog) = factory
        .create_with_callback_log(make_consumer_props(ctx, factory, &group_id))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create consumer: {e}"));

    // Java's `@BeforeEach setup()` pre-creates the topic empty before any
    // records are produced; mirror that here to close the consumer-side
    // metadata race during produce-triggered auto-create.
    create_test_topic(ctx, &topic, 2).await;
    send_records_bytes(ctx, &tp, num_records, starting_timestamp).await;
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

    log.commit_async_with_logging_callback(&mut consumer)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_async_with_callback should succeed: {e}"));

    let entries = poll_until_entry(&mut consumer, &log, KIND_COMMIT, |_| true, Duration::from_secs(10)).await;
    assert!(
        entries.iter().any(|e| e.kind == KIND_COMMIT),
        "{backend} backend: Failed to observe commit callback before timeout: {entries:?}"
    );

    let committed_offset = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    // Java: `assertNotNull(committedOffset)`, then `assertNull(committedOffset.get(tp))`.
    assert_eq!(
        committed_offset.get(&tp),
        Some(&None),
        "committed offset for {tp} should be None (no fetch position was established) ({backend} backend)"
    );

    let assignment = consumer.assignment();
    assert!(
        assignment.contains(&tp),
        "consumer assignment should contain {tp}, got {assignment:?} ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_assign_and_commit_async_not_committed,
    async_assign_and_commit_async_not_committed,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncAssignAndCommitSyncNotCommitted`.
/// Same shape as the async variant, but `commit_sync()`
/// (no offsets argument) commits "all consumed" — which is empty since
/// `poll()` was never called.
async fn async_assign_and_commit_sync_not_committed<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_commit_sync_not_committed");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records = 10_000;
    let starting_timestamp = current_time_ms();

    let mut consumer = create_consumer(ctx, factory, &group_id).await;

    // Java's `@BeforeEach setup()` pre-creates the topic empty before any
    // records are produced; mirror that here to close the consumer-side
    // metadata race during produce-triggered auto-create.
    create_test_topic(ctx, &topic, 2).await;
    send_records_bytes(ctx, &tp, num_records, starting_timestamp).await;
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));
    consumer
        .commit_sync()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync should succeed: {e}"));

    let committed_offset = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    assert_eq!(
        committed_offset.get(&tp),
        Some(&None),
        "committed offset for {tp} should be None (no fetch position was established) ({backend} backend)"
    );

    let assignment = consumer.assignment();
    assert!(
        assignment.contains(&tp),
        "consumer assignment should contain {tp}, got {assignment:?} ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_assign_and_commit_sync_not_committed,
    async_assign_and_commit_sync_not_committed,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncAssignAndCommitSyncAllConsumed`.
/// Assign + `seek(tp, 0)`, consume all 10,000 records, then
/// `commit_sync()` — the committed offset should equal `num_records`.
async fn async_assign_and_commit_sync_all_consumed<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_commit_sync_all_consumed");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records: usize = 10_000;

    let mut consumer = create_consumer(ctx, factory, &group_id).await;

    let starting_timestamp = current_time_ms();
    // Java's `@BeforeEach setup()` pre-creates the topic empty before any
    // records are produced; mirror that here to close the consumer-side
    // metadata race during produce-triggered auto-create.
    create_test_topic(ctx, &topic, 2).await;
    send_records_bytes(ctx, &tp, num_records, starting_timestamp).await;
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));
    consumer
        .seek_with_offset(tp.clone(), 0)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek should succeed: {e}"));
    consume_and_verify_records_bytes(&mut *consumer, &tp, num_records, 0, 0, starting_timestamp, backend).await;

    consumer
        .commit_sync()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync should succeed: {e}"));
    let committed_offset = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
    let entry = committed_offset
        .get(&tp)
        .and_then(Option::as_ref)
        .unwrap_or_else(|| panic!("{backend} backend: committed offset for tp should be present after commit_sync"));
    assert_eq!(
        entry.offset(),
        num_records as i64,
        "committed offset should equal num_records after consuming everything ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_assign_and_commit_sync_all_consumed,
    async_assign_and_commit_sync_all_consumed,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncAssignAndConsume`. Assign,
/// consume 10 records via `consumeAndVerifyRecords`, assert
/// `position(tp) == numRecords`.
async fn async_assign_and_consume<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_consume");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records: usize = 10;

    let mut consumer = create_consumer(ctx, factory, &group_id).await;

    let starting_timestamp = current_time_ms();
    // Java's `@BeforeEach setup()` pre-creates the topic empty before any
    // records are produced; mirror that here to close the consumer-side
    // metadata race during produce-triggered auto-create.
    create_test_topic(ctx, &topic, 2).await;
    send_records_bytes(ctx, &tp, num_records, starting_timestamp).await;
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));
    consume_and_verify_records_bytes(&mut *consumer, &tp, num_records, 0, 0, starting_timestamp, backend).await;

    let pos = consumer
        .position(&tp)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: position should succeed: {e}"));
    assert_eq!(
        pos, num_records as i64,
        "position(tp) should equal num_records after consuming everything ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_assign_and_consume,
    async_assign_and_consume,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncAssignAndConsumeSkippingPosition`.
/// Same as `consume` but `seek(tp, 1)` then consume
/// `numRecords - 1` records starting at offset 1.
async fn async_assign_and_consume_skipping_position<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_consume_skipping");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records: usize = 10;

    let mut consumer = create_consumer(ctx, factory, &group_id).await;

    let starting_timestamp = current_time_ms();
    // Java's `@BeforeEach setup()` pre-creates the topic empty before any
    // records are produced; mirror that here to close the consumer-side
    // metadata race during produce-triggered auto-create.
    create_test_topic(ctx, &topic, 2).await;
    send_records_bytes(ctx, &tp, num_records, starting_timestamp).await;
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));
    let offset: i64 = 1;
    consumer
        .seek_with_offset(tp.clone(), offset)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek should succeed: {e}"));
    consume_and_verify_records_bytes(
        &mut *consumer,
        &tp,
        num_records - offset as usize,
        offset,
        offset as usize,
        starting_timestamp + offset,
        backend,
    )
    .await;

    let pos = consumer
        .position(&tp)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: position should succeed: {e}"));
    assert_eq!(
        pos, num_records as i64,
        "position(tp) should equal num_records after consuming everything ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_assign_and_consume_skipping_position,
    async_assign_and_consume_skipping_position,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncAssignAndFetchCommittedOffsets`.
/// Consumer #1 assigns, seeks, consumes 100, commits;
/// consumer #2 in the same group asserts the same committed offset is
/// visible. Both consumers are subjects, so both come from `factory`.
async fn async_assign_and_fetch_committed_offsets<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    // Both consumers must share the same group.id (Java uses literal
    // "group1"; we make it unique-per-test for parallelism safety).
    let group_id = ctx.group_id("g_fetch_committed");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records: usize = 100;
    let starting_timestamp = current_time_ms();

    {
        let mut consumer = create_consumer(ctx, factory, &group_id).await;

        // Java's `@BeforeEach setup()` pre-creates the topic empty before any
        // records are produced; mirror that here to close the consumer-side
        // metadata race during produce-triggered auto-create.
        create_test_topic(ctx, &topic, 2).await;
        send_records_bytes(ctx, &tp, num_records, starting_timestamp).await;
        consumer
            .assign(vec![tp.clone()])
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));
        consumer
            .seek_with_offset(tp.clone(), 0)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: seek should succeed: {e}"));
        consume_and_verify_records_bytes(&mut *consumer, &tp, num_records, 0, 0, starting_timestamp, backend).await;
        consumer
            .commit_sync()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: commit_sync should succeed: {e}"));

        let committed = consumer
            .committed(std::slice::from_ref(&tp))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
        let entry = committed
            .get(&tp)
            .and_then(Option::as_ref)
            .unwrap_or_else(|| panic!("{backend} backend: committed offset for tp should be present"));
        assert_eq!(entry.offset(), num_records as i64, "committed offset ({backend} backend)");

        consumer
            .close()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: consumer 1 close should succeed: {e}"));
    }

    {
        let mut another = create_consumer(ctx, factory, &group_id).await;

        another
            .assign(vec![tp.clone()])
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));
        let committed = another
            .committed(std::slice::from_ref(&tp))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
        let entry = committed.get(&tp).and_then(Option::as_ref).unwrap_or_else(|| {
            panic!("{backend} backend: committed offset for tp should be visible to another consumer in same group")
        });
        assert_eq!(entry.offset(), num_records as i64, "committed offset ({backend} backend)");

        another
            .close()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: consumer 2 close should succeed: {e}"));
    }
}

multilanguage_consumer_test!(
    test_async_assign_and_fetch_committed_offsets,
    async_assign_and_fetch_committed_offsets,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `testAsyncAssignAndConsumeFromCommittedOffsets`.
/// Consumer #1 commits a manual offset (10) via
/// `commit_sync_with_offsets(...)`; consumer #2 reads from that offset and
/// verifies the remaining records. Both consumers are subjects, so both come
/// from `factory`.
async fn async_assign_and_consume_from_committed_offsets<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_consume_from_committed");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records: usize = 100;
    let offset: i64 = 10;
    let starting_timestamp = current_time_ms();

    {
        let mut consumer = create_consumer(ctx, factory, &group_id).await;

        // Java's `@BeforeEach setup()` pre-creates the topic empty before any
        // records are produced; mirror that here to close the consumer-side
        // metadata race during produce-triggered auto-create.
        create_test_topic(ctx, &topic, 2).await;
        send_records_bytes(ctx, &tp, num_records, starting_timestamp).await;
        consumer
            .assign(vec![tp.clone()])
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

        // Java: `consumer.commitSync(Map.of(tp, new OffsetAndMetadata(offset)))`.
        let mut offsets = HashMap::new();
        offsets.insert(
            tp.clone(),
            OffsetAndMetadata::new(offset).expect("OffsetAndMetadata::new should not fail for offset=10"),
        );
        consumer
            .commit_sync_with_offsets(offsets)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: commit_sync_with_offsets should succeed: {e}"));

        let committed = consumer
            .committed(std::slice::from_ref(&tp))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
        let entry = committed
            .get(&tp)
            .and_then(Option::as_ref)
            .unwrap_or_else(|| panic!("{backend} backend: committed entry should be present"));
        assert_eq!(entry.offset(), offset, "committed offset ({backend} backend)");

        consumer
            .close()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: consumer 1 close should succeed: {e}"));
    }

    {
        let mut another = create_consumer(ctx, factory, &group_id).await;

        let committed = another
            .committed(std::slice::from_ref(&tp))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: committed should succeed: {e}"));
        let entry = committed.get(&tp).and_then(Option::as_ref).unwrap_or_else(|| {
            panic!("{backend} backend: committed entry should be visible to another consumer in same group")
        });
        assert_eq!(entry.offset(), offset, "committed offset ({backend} backend)");

        another
            .assign(vec![tp.clone()])
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));
        consume_and_verify_records_bytes(
            &mut *another,
            &tp,
            num_records - offset as usize,
            offset,
            offset as usize,
            starting_timestamp + offset,
            backend,
        )
        .await;

        another
            .close()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: consumer 2 close should succeed: {e}"));
    }
}

multilanguage_consumer_test!(
    test_async_assign_and_consume_from_committed_offsets,
    async_assign_and_consume_from_committed_offsets,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's
/// `testAsyncAssignAndRetrievingCommittedOffsetsMultipleTimes`. Verifies that calling `committed(...)` twice on the same
/// consumer returns the same value both times (i.e. the API is
/// idempotent — no caching bug that returns a stale snapshot the second
/// time).
async fn async_assign_and_retrieving_committed_offsets_multiple_times<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_committed_multiple_times");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records: usize = 100;
    let starting_timestamp = current_time_ms();

    let mut consumer = create_consumer(ctx, factory, &group_id).await;

    // Java's `@BeforeEach setup()` pre-creates the topic empty before any
    // records are produced; mirror that here to close the consumer-side
    // metadata race during produce-triggered auto-create.
    create_test_topic(ctx, &topic, 2).await;
    send_records_bytes(ctx, &tp, num_records, starting_timestamp).await;
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

    consumer
        .seek_with_offset(tp.clone(), 0)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek should succeed: {e}"));
    consume_and_verify_records_bytes(&mut *consumer, &tp, num_records, 0, 0, starting_timestamp, backend).await;
    consumer
        .commit_sync()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync should succeed: {e}"));

    let first = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed (first call) should succeed: {e}"));
    assert_eq!(
        first
            .get(&tp)
            .and_then(Option::as_ref)
            .unwrap_or_else(|| panic!("{backend} backend: committed entry should be present (first call)"))
            .offset(),
        num_records as i64,
        "first committed offset ({backend} backend)"
    );

    let second = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed (second call) should succeed: {e}"));
    assert_eq!(
        second
            .get(&tp)
            .and_then(Option::as_ref)
            .unwrap_or_else(|| panic!("{backend} backend: committed entry should be present (second call)"))
            .offset(),
        num_records as i64,
        "second committed offset ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_assign_and_retrieving_committed_offsets_multiple_times,
    async_assign_and_retrieving_committed_offsets_multiple_times,
    cluster_config_with_kip848_3brokers()
);

/// Translates Java's `PlaintextConsumerAssignTest#testAsyncPollAfterTopicDeleted`
/// (body `PlaintextConsumerAssignTest#testPollAfterTopicDeleted`) — validates
/// KAFKA-20165.
///
/// `poll()` must not fail to retrieve committed offsets after the assigned
/// topic is deleted: the consumer still has the topic id cached, so the
/// OffsetFetch it issues on the re-assignment gets `UNKNOWN_TOPIC_ID`, which
/// it must treat as a retriable partition error and recover from.
async fn async_poll_after_topic_deleted<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic_to_delete = ctx.topic("topic-to-delete-assign");
    // Java: `GROUP_ID_CONFIG, "test-group"` (namespaced per test run here).
    let group_id = ctx.group_id("test-group");

    // Java: `clusterInstance.createTopic(topicToDelete, 1, (short) BROKER_COUNT)`.
    let admin = admin_for(ctx);
    test_utils::create_topic(admin.as_ref(), &topic_to_delete, 1, 3).await;
    let tp_to_delete = TopicPartition::new(topic_to_delete.clone(), 0);

    let num_records: usize = 10;
    let starting_timestamp = current_time_ms();

    // Java: `clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, ...,
    // GROUP_ID_CONFIG, "test-group", AUTO_OFFSET_RESET_CONFIG, "earliest"))`.
    // Unlike this file's `make_consumer_props`, Java does NOT disable
    // auto-commit here, so `enable.auto.commit` is left at its default (true).
    let mut props = HashMap::from([
        (
            "bootstrap.servers".to_string(),
            ctx.protocol_bootstrap_servers_for(factory.needs_container_bootstrap())
                .to_string(),
        ),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("group.id".to_string(), group_id.clone()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
    ]);
    ctx.apply_security(&mut props);
    let mut consumer = factory
        .create(props)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create consumer: {e}"));

    // Send records and consume them (this caches the topic ID in the consumer)
    send_records_bytes(ctx, &tp_to_delete, num_records, starting_timestamp).await;
    consumer
        .assign(vec![tp_to_delete.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));
    consumer
        .seek_with_offset(tp_to_delete.clone(), 0)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek should succeed: {e}"));
    consume_and_verify_records_bytes(&mut *consumer, &tp_to_delete, num_records, 0, 0, starting_timestamp, backend)
        .await;
    consumer
        .commit_sync()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit_sync should succeed: {e}"));

    // Delete the topic and wait for deletion to propagate to metadata
    admin
        .delete_topics_with_topic_collection(TopicCollection::of_topic_names(vec![topic_to_delete.clone()]))
        .all()
        .get()
        .await
        .expect("delete_topics should succeed");
    test_utils::wait_until_true_with_timeout(
        || async {
            // Java's `waitForCondition` retries when `listTopics()` throws;
            // an error here is treated as "not yet" for the same reason.
            match admin.list_topics().names().get().await {
                Ok(names) => !names.contains(&topic_to_delete),
                Err(_) => false,
            }
        },
        "Condition not met within timeout 10000. Topic should be removed from metadata",
        10_000,
        test_utils::DEFAULT_PAUSE_MS,
    )
    .await;

    // Change assignment to force the consumer to fetch committed offsets on next poll()
    // The consumer still has the topic ID cached, so it will use version 10+
    // and get UNKNOWN_TOPIC_ID from the broker
    consumer
        .unsubscribe()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: unsubscribe should succeed: {e}"));
    consumer
        .assign(vec![tp_to_delete.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));

    // poll() should not throw - internally fetches committed offsets for deleted topic and recovers
    let result = consumer.poll(Duration::from_millis(5000)).await;
    assert!(
        result.is_ok(),
        "poll() after topic deletion should not fail: {:?} ({backend} backend)",
        result.err()
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

multilanguage_consumer_test!(
    test_async_poll_after_topic_deleted,
    async_poll_after_topic_deleted,
    cluster_config_with_kip848_3brokers()
);

/// Builds an admin client for topic provisioning / deletion (Java's
/// `clusterInstance.admin()`). Mirrors the per-file `admin_for` helper the
/// other integration tests use.
fn admin_for(ctx: &TestContext) -> Box<dyn Admin> {
    let mut props = HashMap::from([
        ("client.id".to_string(), "assign-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    ctx.configure(&mut props);
    let config = AdminClientConfig::new(&props).expect("valid admin config");
    AdminClient::create(config).expect("admin client")
}

// ── Local utilities ────────────────────────────────────────────────────

/// Returns the current wall-clock time in milliseconds since the Unix
/// epoch — Java's `System.currentTimeMillis()`.
fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as i64
}
