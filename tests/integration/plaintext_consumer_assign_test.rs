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
//! # Translated methods (KIP-848 / `GroupProtocol.CONSUMER` arm only)
//!
//! - Translated (PASSING):
//!   - `testAsyncAssignAndConsumeSkippingPosition`
//!     → `test_async_assign_and_consume_skipping_position`
//! - Translated (`#[ignore]`-gated on production-code gaps documented in
//!   `design/history/Milestone-8/Phase-13/COMMENTS.1.md`):
//!   - `testAsyncAssignAndCommitAsyncNotCommitted`
//!     → `test_async_assign_and_commit_async_not_committed` — Issue 1
//!     (committed() does not retry NotCoordinator)
//!   - `testAsyncAssignAndCommitSyncNotCommitted`
//!     → `test_async_assign_and_commit_sync_not_committed` — Issue 1
//!   - `testAsyncAssignAndCommitSyncAllConsumed`
//!     → `test_async_assign_and_commit_sync_all_consumed` — Issue 3
//!     (commit_sync after successful poll still times out on assign-only flows)
//!   - `testAsyncAssignAndConsume`
//!     → `test_async_assign_and_consume` — Issue 4
//!     (poll() surfaces NotCoordinator as fatal on the OffsetFetch-on-startup path)
//!   - `testAsyncAssignAndFetchCommittedOffsets`
//!     → `test_async_assign_and_fetch_committed_offsets` — Issue 1
//!   - `testAsyncAssignAndConsumeFromCommittedOffsets`
//!     → `test_async_assign_and_consume_from_committed_offsets` — Issue 2
//!     (commit_sync_offsets on a fresh consumer never recovers from NotCoordinator)
//!   - `testAsyncAssignAndRetrievingCommittedOffsetsMultipleTimes`
//!     → `test_async_assign_and_retrieving_committed_offsets_multiple_times`
//!     — Issues 3 + 4
//!
//! # SKIPped methods
//!
//! The 8 `testClassic*` twin methods are SKIPped — the project targets
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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;

use confluent_kafka::common::KafkaError;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::OffsetAndMetadata;
use confluent_kafka::consumer::OffsetCommitCallback;
use confluent_kafka::consumer::new_consumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

// Type alias matching the bytes-typed `Consumer` trait object returned
// by `new_consumer::<Vec<u8>, Vec<u8>>`. Used in helper signatures so
// the tests pass `&mut consumer` (which deref-coerces from
// `Box<dyn Consumer<Vec<u8>, Vec<u8>>>`).
type BytesConsumer = dyn Consumer<Vec<u8>, Vec<u8>>;

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
    let mut props = BTreeMap::new();
    props.insert(
        "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS".to_string(),
        "classic,consumer".to_string(),
    );
    props.insert("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), "3".to_string());
    props.insert("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS".to_string(), "1".to_string());
    props.insert("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS".to_string(), "100".to_string());
    props.insert("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS".to_string(), "10".to_string());
    let mut cfg = ClusterConfig::with_brokers(3);
    cfg.server_properties = props;
    cfg
}

// ── Byte-array deserializer (Java uses `byte[]` keys and values) ──────

/// Local byte-array deserializer for these tests. The crate exports
/// `ByteArraySerializer` but no symmetric `ByteArrayDeserializer`; this
/// inline impl is identical to what such a struct would do.
struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, KafkaError> {
        Ok(data.to_vec())
    }
}

// ── Consumer config builder ───────────────────────────────────────────

/// Build a `ConsumerConfig` matching what Java's
/// `clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, "consumer"))`
/// produces.
///
/// `auto.offset.reset=earliest` and `enable.auto.commit=false` so the
/// tests' explicit `seek` / `commit_sync` / `commit_async` calls are the
/// only offset-state transitions.
fn make_consumer_config_bytes(bootstrap: &str, group_id: Option<&str>) -> ConsumerConfig {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
    ]);
    if let Some(gid) = group_id {
        props.insert("group.id".to_string(), gid.to_string());
    }
    ConsumerConfig::from_properties(&props).expect("invalid test config")
}

// ── Producer helpers (mirror Java's ClientsTestUtils.sendRecords) ─────

/// Build a `ProducerConfig` aligned with the existing producer
/// integration tests (acks=all so produced records are durable before
/// the consumer reads them).
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

/// Translates Java's `ClientsTestUtils.sendRecords(cluster, tp, num,
/// startingTimestamp)` (with the default `timestampIncrement = -1`,
/// which Java interprets as "use 1ms per record").
///
/// Keys are `b"key {i}"` and values are `b"value {i}"`, matching Java's
/// `KEY_PREFIX = "key "` and `VALUE_PREFIX = "value "`. Timestamps are
/// `starting_timestamp + i` (1ms increment, mirroring Java line 185-186
/// of `ClientsTestUtils.consumeAndVerifyRecords`).
///
/// All records are fired to the producer up front, then `flush()` waits
/// for the broker acks. The producer is then closed.
async fn send_records_bytes(bootstrap: &str, tp: &TopicPartition, num_records: usize, starting_timestamp: i64) {
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> = KafkaProducer::from_config(
        make_producer_config(bootstrap),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("Failed to build test producer");

    let mut last_future = None;
    for i in 0..num_records {
        // Java: `timestamp = startingTimestamp + i * (timestampIncrement > 0 ? timestampIncrement : 1)`
        let timestamp = starting_timestamp + i as i64;
        let key = format!("key {i}").into_bytes();
        let value = format!("value {i}").into_bytes();
        let record = ProducerRecord::with_timestamp(
            tp.topic().to_string(),
            Some(tp.partition()),
            Some(timestamp),
            Some(key),
            Some(value),
        )
        .expect("ProducerRecord::with_timestamp should not fail for non-negative ts/partition");
        last_future = Some(producer.send(record).await.expect("send should not fail"));
    }
    // Flush ensures every send is acked before we hand back; the last
    // future's wait would also suffice but `flush()` better matches the
    // Java helper's semantics (`producer.flush()` at end).
    producer.flush().await.expect("producer.flush should succeed");
    if let Some(f) = last_future {
        // Belt-and-braces — `flush` already awaited; this is a no-op for
        // a completed future but surfaces any error.
        f.get_timeout(Duration::from_secs(30)).await.expect("last send should succeed");
    }
    producer.close().await.expect("producer close should succeed");
}

// ── Consumer test helpers (mirror ClientsTestUtils.poll/consume) ──────

/// Translates Java's `ClientsTestUtils.pollUntilTrue(consumer, predicate,
/// waitTimeMs, msg)`. Drives `consumer.poll(...)` in a tight loop until
/// `predicate()` returns true or the deadline passes.
///
/// Per `consumer-threading.md` §31, driving `poll()` is what delivers
/// any `OffsetCommitCallback` enqueued by a prior `commit_async` to the
/// caller's task — without `poll()`, the callback never fires.
async fn poll_until_true<F>(consumer: &mut BytesConsumer, mut predicate: F, wait_time: Duration, msg: &str)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + wait_time;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        let _ = consumer
            .poll(Duration::from_millis(100))
            .await
            .expect("poll should not return a fatal error in poll_until_true");
        if predicate() {
            return;
        }
    }
    panic!("{msg}");
}

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
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut next_index: usize = 0;
    while next_index < num_records && Instant::now() < deadline {
        let records = consumer
            .poll(Duration::from_millis(100))
            .await
            .expect("poll should succeed in consume_and_verify_records_bytes");
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

            assert_eq!(record.topic(), tp.topic(), "record topic should match tp.topic()");
            assert_eq!(
                record.partition(),
                tp.partition(),
                "record partition should match tp.partition()"
            );

            assert_eq!(
                record.timestamp_type(),
                TimestampType::CreateTime,
                "record timestamp_type should be CreateTime (broker default)"
            );
            let expected_ts = starting_timestamp + i as i64;
            assert_eq!(record.timestamp(), expected_ts, "record timestamp should be {expected_ts}");

            assert_eq!(record.offset(), offset, "record offset should be {offset}");

            let key_and_value_index = starting_key_and_value_index + i;
            let expected_key = format!("key {key_and_value_index}").into_bytes();
            let expected_value = format!("value {key_and_value_index}").into_bytes();
            assert_eq!(
                record.key().expect("record key should be present"),
                &expected_key,
                "key at index {i} mismatched"
            );
            assert_eq!(
                record.value().expect("record value should be present"),
                &expected_value,
                "value at index {i} mismatched"
            );

            assert_eq!(
                record.serialized_key_size() as usize,
                expected_key.len(),
                "serialized_key_size mismatch at index {i}"
            );
            assert_eq!(
                record.serialized_value_size() as usize,
                expected_value.len(),
                "serialized_value_size mismatch at index {i}"
            );

            next_index += 1;
        }
    }
    assert_eq!(
        next_index, num_records,
        "Timed out before consuming expected {num_records} records (got {next_index})"
    );
}

// ── OffsetCommitCallback recorder (mirrors CountConsumerCommitCallback) ─

/// Translates Java's private `CountConsumerCommitCallback`:
///
/// ```java
/// private static class CountConsumerCommitCallback implements OffsetCommitCallback {
///     int successCount = 0;
///     int failCount = 0;
///     Optional<Exception> lastError = Optional.empty();
///     public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
///         if (exception == null) { successCount += 1; }
///         else { failCount += 1; lastError = Optional.of(exception); }
///     }
/// }
/// ```
///
/// Rust counterpart uses `Arc<AtomicUsize>` for the counters (the
/// callback is shared across the test task and the bg task via
/// `Arc<dyn OffsetCommitCallback>`) and `Arc<Mutex<Option<KafkaError>>>`
/// for the last error.
struct CountConsumerCommitCallback {
    success_count: Arc<AtomicUsize>,
    fail_count: Arc<AtomicUsize>,
    last_error: Arc<Mutex<Option<KafkaError>>>,
}

impl CountConsumerCommitCallback {
    fn new() -> Self {
        Self {
            success_count: Arc::new(AtomicUsize::new(0)),
            fail_count: Arc::new(AtomicUsize::new(0)),
            last_error: Arc::new(Mutex::new(None)),
        }
    }

    fn handles(&self) -> CountConsumerCommitCallbackHandles {
        CountConsumerCommitCallbackHandles {
            success_count: Arc::clone(&self.success_count),
            fail_count: Arc::clone(&self.fail_count),
            last_error: Arc::clone(&self.last_error),
        }
    }
}

/// Test-side accessor for the recorded counters. The callback itself is
/// moved into an `Arc<dyn OffsetCommitCallback>` and consumed by the
/// consumer; the test keeps these handles to read the counts.
struct CountConsumerCommitCallbackHandles {
    success_count: Arc<AtomicUsize>,
    fail_count: Arc<AtomicUsize>,
    last_error: Arc<Mutex<Option<KafkaError>>>,
}

impl CountConsumerCommitCallbackHandles {
    fn success_count(&self) -> usize {
        self.success_count.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    fn fail_count(&self) -> usize {
        self.fail_count.load(Ordering::SeqCst)
    }

    fn last_error_is_some(&self) -> bool {
        // Lock + read in a single short critical section that does not
        // cross `.await` — §16-compliant.
        self.last_error.lock().expect("mutex poisoned").is_some()
    }
}

#[async_trait]
impl OffsetCommitCallback for CountConsumerCommitCallback {
    async fn on_complete(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>, error: Option<&KafkaError>) {
        match error {
            None => {
                self.success_count.fetch_add(1, Ordering::SeqCst);
            },
            Some(err) => {
                self.fail_count.fetch_add(1, Ordering::SeqCst);
                // §16: do not hold the guard across an `.await`. The
                // callback body has no further await, so the guard
                // simply drops at the end of the scope.
                let mut guard = self.last_error.lock().expect("mutex poisoned");
                *guard = Some(err.clone());
            },
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

/// Translates Java's `testAsyncAssignAndCommitAsyncNotCommitted`
/// (line 83). The consumer assigns a partition (no `poll()` first) and
/// fires `commitAsync(callback)`. Because there is no fetch position,
/// the commit completes successfully but commits no offset — the
/// follow-up `committed(...)` returns a map with no entry for `tp`.
///
/// §31 parity: the `OffsetCommitCallback` is delivered on the caller's
/// task by the `poll_until_true` loop (each `poll()` invocation drains
/// the pending-callback queue via
/// `OffsetCommitCallbackInvoker::invoke_pending_callbacks`).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Phase-13a production gap (Issue 1 in design/history/Milestone-8/Phase-13/COMMENTS.1.md): \
    `committed()` does not retry on NotCoordinator — un-ignore once \
    fetch_offsets_with_retries in commit_request_manager.rs grows a retry loop"]
async fn test_async_assign_and_commit_async_not_committed() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_commit_async_not_committed");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records = 10_000;
    let starting_timestamp = current_time_ms();

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), Some(&group_id)),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    send_records_bytes(ctx.bootstrap_servers(), &tp, num_records, starting_timestamp).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");

    let cb = CountConsumerCommitCallback::new();
    let handles = cb.handles();
    consumer
        .commit_async_with_callback(Arc::new(cb))
        .await
        .expect("commit_async_with_callback should succeed");

    poll_until_true(
        consumer.as_mut(),
        || handles.success_count() >= 1 || handles.last_error_is_some(),
        Duration::from_secs(10),
        "Failed to observe commit callback before timeout",
    )
    .await;

    let committed_offset = consumer.committed(&[tp.clone()]).await.expect("committed should succeed");
    // Java: `assertNotNull(committedOffset)`. The Rust analog is "we got
    // a Map back, not an error" — which we already have. Java then
    // asserts `committedOffset.get(tp)` is null; in the Rust map, no
    // entry exists for `tp` (since nothing was actually committed).
    assert!(
        committed_offset.get(&tp).is_none(),
        "committed offset for {tp} should be absent (no fetch position was established), got {:?}",
        committed_offset.get(&tp)
    );

    let assignment = consumer.assignment();
    assert!(
        assignment.contains(&tp),
        "consumer assignment should contain {tp}, got {assignment:?}"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncAssignAndCommitSyncNotCommitted`
/// (line 113). Same shape as the async variant, but `commit_sync()`
/// (no offsets argument) commits "all consumed" — which is empty since
/// `poll()` was never called.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Phase-13a production gap (Issue 1 in design/history/Milestone-8/Phase-13/COMMENTS.1.md): \
    `committed()` does not retry on NotCoordinator — un-ignore once \
    fetch_offsets_with_retries in commit_request_manager.rs grows a retry loop"]
async fn test_async_assign_and_commit_sync_not_committed() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_commit_sync_not_committed");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records = 10_000;
    let starting_timestamp = current_time_ms();

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), Some(&group_id)),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    send_records_bytes(ctx.bootstrap_servers(), &tp, num_records, starting_timestamp).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");
    consumer.commit_sync().await.expect("commit_sync should succeed");

    let committed_offset = consumer.committed(&[tp.clone()]).await.expect("committed should succeed");
    assert!(
        committed_offset.get(&tp).is_none(),
        "committed offset for {tp} should be absent (no fetch position was established), got {:?}",
        committed_offset.get(&tp)
    );

    let assignment = consumer.assignment();
    assert!(
        assignment.contains(&tp),
        "consumer assignment should contain {tp}, got {assignment:?}"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncAssignAndCommitSyncAllConsumed`
/// (line 140). Assign + `seek(tp, 0)`, consume all 10,000 records, then
/// `commit_sync()` — the committed offset should equal `num_records`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Phase-13a production gap (Issue 3 in design/history/Milestone-8/Phase-13/COMMENTS.1.md): \
    commit_sync after a successful poll cycle still times out at 60s for assign-only flows — \
    coordinator-channel liveness audit needed"]
async fn test_async_assign_and_commit_sync_all_consumed() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_commit_sync_all_consumed");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records: usize = 10_000;

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), Some(&group_id)),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    let starting_timestamp = current_time_ms();
    send_records_bytes(ctx.bootstrap_servers(), &tp, num_records, starting_timestamp).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");
    consumer.seek(tp.clone(), 0).await.expect("seek should succeed");
    consume_and_verify_records_bytes(consumer.as_mut(), &tp, num_records, 0, 0, starting_timestamp).await;

    consumer.commit_sync().await.expect("commit_sync should succeed");
    let committed_offset = consumer.committed(&[tp.clone()]).await.expect("committed should succeed");
    let entry = committed_offset
        .get(&tp)
        .expect("committed offset for tp should be present after commit_sync");
    assert_eq!(
        entry.offset(),
        num_records as i64,
        "committed offset should equal num_records after consuming everything"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncAssignAndConsume` (line 168). Assign,
/// consume 10 records via `consumeAndVerifyRecords`, assert
/// `position(tp) == numRecords`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Phase-13a production gap (Issue 4 in design/history/Milestone-8/Phase-13/COMMENTS.1.md): \
    poll() surfaces NotCoordinator as fatal — the OffsetFetch-on-startup path that \
    resolves the initial position does not retry NotCoordinator. Workaround test \
    `test_async_assign_and_consume_skipping_position` inserts a `seek` to bypass."]
async fn test_async_assign_and_consume() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_consume");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records: usize = 10;

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), Some(&group_id)),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    let starting_timestamp = current_time_ms();
    send_records_bytes(ctx.bootstrap_servers(), &tp, num_records, starting_timestamp).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");
    consume_and_verify_records_bytes(consumer.as_mut(), &tp, num_records, 0, 0, starting_timestamp).await;

    let pos = consumer.position(&tp).await.expect("position should succeed");
    assert_eq!(
        pos, num_records as i64,
        "position(tp) should equal num_records after consuming everything"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncAssignAndConsumeSkippingPosition`
/// (line 191). Same as `consume` but `seek(tp, 1)` then consume
/// `numRecords - 1` records starting at offset 1.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_assign_and_consume_skipping_position() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_consume_skipping");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records: usize = 10;

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), Some(&group_id)),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    let starting_timestamp = current_time_ms();
    send_records_bytes(ctx.bootstrap_servers(), &tp, num_records, starting_timestamp).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");
    let offset: i64 = 1;
    consumer.seek(tp.clone(), offset).await.expect("seek should succeed");
    consume_and_verify_records_bytes(
        consumer.as_mut(),
        &tp,
        num_records - offset as usize,
        offset,
        offset as usize,
        starting_timestamp + offset,
    )
    .await;

    let pos = consumer.position(&tp).await.expect("position should succeed");
    assert_eq!(
        pos, num_records as i64,
        "position(tp) should equal num_records after consuming everything"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncAssignAndFetchCommittedOffsets`
/// (line 216). Consumer #1 assigns, seeks, consumes 100, commits;
/// consumer #2 in the same group asserts the same committed offset is
/// visible.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Phase-13a production gap (Issue 1 in design/history/Milestone-8/Phase-13/COMMENTS.1.md): \
    consumer #2's first `committed()` call hits NotCoordinator without retry — \
    blocks on the fetch_offsets_with_retries gap"]
async fn test_async_assign_and_fetch_committed_offsets() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    // Both consumers must share the same group.id (Java uses literal
    // "group1"; we make it unique-per-test for parallelism safety).
    let group_id = ctx.group_id("g_fetch_committed");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records: usize = 100;
    let starting_timestamp = current_time_ms();

    {
        let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
            make_consumer_config_bytes(ctx.bootstrap_servers(), Some(&group_id)),
            Box::new(ByteArrayDeserializer),
            Box::new(ByteArrayDeserializer),
        )
        .expect("new_consumer should succeed (consumer 1)");

        send_records_bytes(ctx.bootstrap_servers(), &tp, num_records, starting_timestamp).await;
        consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");
        consumer.seek(tp.clone(), 0).await.expect("seek should succeed");
        consume_and_verify_records_bytes(consumer.as_mut(), &tp, num_records, 0, 0, starting_timestamp).await;
        consumer.commit_sync().await.expect("commit_sync should succeed");

        let committed = consumer.committed(&[tp.clone()]).await.expect("committed should succeed");
        let entry = committed.get(&tp).expect("committed offset for tp should be present");
        assert_eq!(entry.offset(), num_records as i64);

        consumer.close().await.expect("consumer 1 close should succeed");
    }

    {
        let mut another = new_consumer::<Vec<u8>, Vec<u8>>(
            make_consumer_config_bytes(ctx.bootstrap_servers(), Some(&group_id)),
            Box::new(ByteArrayDeserializer),
            Box::new(ByteArrayDeserializer),
        )
        .expect("new_consumer should succeed (consumer 2)");

        another.assign(vec![tp.clone()]).await.expect("assign should succeed");
        let committed = another.committed(&[tp.clone()]).await.expect("committed should succeed");
        let entry = committed
            .get(&tp)
            .expect("committed offset for tp should be visible to another consumer in same group");
        assert_eq!(entry.offset(), num_records as i64);

        another.close().await.expect("consumer 2 close should succeed");
    }
}

/// Translates Java's `testAsyncAssignAndConsumeFromCommittedOffsets`
/// (line 247). Consumer #1 commits a manual offset (10) via
/// `commit_sync_offsets(...)`; consumer #2 reads from that offset and
/// verifies the remaining records.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Phase-13a production gap (Issue 2 in design/history/Milestone-8/Phase-13/COMMENTS.1.md): \
    `commit_sync_offsets` immediately after assign on a fresh consumer never recovers \
    from NotCoordinator (times out at 60s) — bg-task FindCoordinator wiring on the \
    assign-only path needs audit"]
async fn test_async_assign_and_consume_from_committed_offsets() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_consume_from_committed");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records: usize = 100;
    let offset: i64 = 10;
    let starting_timestamp = current_time_ms();

    {
        let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
            make_consumer_config_bytes(ctx.bootstrap_servers(), Some(&group_id)),
            Box::new(ByteArrayDeserializer),
            Box::new(ByteArrayDeserializer),
        )
        .expect("new_consumer should succeed (consumer 1)");

        send_records_bytes(ctx.bootstrap_servers(), &tp, num_records, starting_timestamp).await;
        consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");

        // Java: `consumer.commitSync(Map.of(tp, new OffsetAndMetadata(offset)))`.
        let mut offsets = HashMap::new();
        offsets.insert(
            tp.clone(),
            OffsetAndMetadata::new(offset).expect("OffsetAndMetadata::new should not fail for offset=10"),
        );
        consumer
            .commit_sync_offsets(offsets)
            .await
            .expect("commit_sync_offsets should succeed");

        let committed = consumer.committed(&[tp.clone()]).await.expect("committed should succeed");
        let entry = committed.get(&tp).expect("committed entry should be present");
        assert_eq!(entry.offset(), offset);

        consumer.close().await.expect("consumer 1 close should succeed");
    }

    {
        let mut another = new_consumer::<Vec<u8>, Vec<u8>>(
            make_consumer_config_bytes(ctx.bootstrap_servers(), Some(&group_id)),
            Box::new(ByteArrayDeserializer),
            Box::new(ByteArrayDeserializer),
        )
        .expect("new_consumer should succeed (consumer 2)");

        let committed = another.committed(&[tp.clone()]).await.expect("committed should succeed");
        let entry = committed
            .get(&tp)
            .expect("committed entry should be visible to another consumer in same group");
        assert_eq!(entry.offset(), offset);

        another.assign(vec![tp.clone()]).await.expect("assign should succeed");
        consume_and_verify_records_bytes(
            another.as_mut(),
            &tp,
            num_records - offset as usize,
            offset,
            offset as usize,
            starting_timestamp + offset,
        )
        .await;

        another.close().await.expect("consumer 2 close should succeed");
    }
}

/// Translates Java's
/// `testAsyncAssignAndRetrievingCommittedOffsetsMultipleTimes` (line
/// 278). Verifies that calling `committed(...)` twice on the same
/// consumer returns the same value both times (i.e. the API is
/// idempotent — no caching bug that returns a stale snapshot the second
/// time).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Phase-13a production gaps (Issues 3 and 4 in design/history/Milestone-8/Phase-13/COMMENTS.1.md): \
    the consume_and_verify_records_bytes path hits Issue 4 (poll NotCoordinator) and the \
    subsequent commit_sync hits Issue 3 (commit_sync timeout after consume on assign-only flows). \
    Flaky depending on which broker the consumer initially routes to."]
async fn test_async_assign_and_retrieving_committed_offsets_multiple_times() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_committed_multiple_times");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_records: usize = 100;
    let starting_timestamp = current_time_ms();

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), Some(&group_id)),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    send_records_bytes(ctx.bootstrap_servers(), &tp, num_records, starting_timestamp).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");

    consumer.seek(tp.clone(), 0).await.expect("seek should succeed");
    consume_and_verify_records_bytes(consumer.as_mut(), &tp, num_records, 0, 0, starting_timestamp).await;
    consumer.commit_sync().await.expect("commit_sync should succeed");

    let first = consumer
        .committed(&[tp.clone()])
        .await
        .expect("committed (first call) should succeed");
    assert_eq!(
        first.get(&tp).expect("committed entry should be present (first call)").offset(),
        num_records as i64
    );

    let second = consumer
        .committed(&[tp.clone()])
        .await
        .expect("committed (second call) should succeed");
    assert_eq!(
        second
            .get(&tp)
            .expect("committed entry should be present (second call)")
            .offset(),
        num_records as i64
    );

    consumer.close().await.expect("consumer close should succeed");
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
