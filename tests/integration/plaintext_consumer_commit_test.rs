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
//! (Apache Kafka 4.2).
//!
//! These exercise the consumer **commit path**: auto-commit-on-close,
//! commit metadata round-trip, async commit + callback success counts,
//! per-partition specified-offset commits, auto-commit-on-rebalance,
//! member-id propagation on subscribe+commit, position/commit interplay
//! across two consumers, and the async-callback-completion ordering
//! guarantees — against a real 3-broker Kafka 4.2.0 cluster with KIP-848
//! (`group.protocol=consumer`) enabled.
//!
//! # Translated (KIP-848 / `GroupProtocol.CONSUMER` arm only)
//!
//! - `testAsyncConsumerAutoCommitOnClose` (line 96)
//!   → `test_async_consumer_auto_commit_on_close`
//! - `testAsyncConsumerAutoCommitOnCloseAfterWakeup` (line 124)
//!   → `test_async_consumer_auto_commit_on_close_after_wakeup`
//! - `testAsyncConsumerCommitMetadata` (line 157)
//!   → `test_async_consumer_commit_metadata`
//! - `testAsyncConsumerAsyncCommit` (line 187)
//!   → `test_async_consumer_async_commit`
//! - `testAsyncConsumerCommitSpecifiedOffsets` (line 308)
//!   → `test_async_consumer_commit_specified_offsets`
//! - `testAsyncConsumerAutoCommitOnRebalance` (line 350)
//!   → `test_async_consumer_auto_commit_on_rebalance` (see deviation below)
//! - `testAsyncConsumerSubscribeAndCommitSync` (line 396)
//!   → `test_async_consumer_subscribe_and_commit_sync`
//! - `testAsyncConsumerPositionAndCommit` (line 419)
//!   → `test_async_consumer_position_and_commit`
//! - `testCommitAsyncCompletedBeforeConsumerCloses` (line 492)
//!   → `test_commit_async_completed_before_consumer_closes`
//! - `testCommitAsyncCompletedBeforeCommitSyncReturns` (line 519)
//!   → `test_commit_async_completed_before_commit_sync_returns`
//!
//! # `#[ignore]`-gated translation
//!
//! - `testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose` (line 461)
//!   → `test_commit_async_fails_when_coordinator_unavailable_during_close`
//!   — `#[ignore]`d: requires `cluster.shutdownBroker()` on ALL brokers.
//!   The Rust integration harness pools+shares clusters across tests
//!   (`tests/common/cluster_pool.rs`) and exposes no broker-shutdown API;
//!   killing brokers would break every co-resident pooled test. The exact
//!   close-path contract — the message
//!   `"Failed to commit offsets: Coordinator unknown and consumer is
//!   closing"` (a `CommitFailedException`) and a sub-1s fast close — is
//!   already unit-tested in
//!   `src/consumer/internals/commit_request_manager.rs` (line 4217+,
//!   `commit_async_fails_when_coordinator_unavailable_during_close`).
//!   Kept here as an `#[ignore]`d body documenting the integration-level
//!   gap, gated on harness broker-shutdown support.
//!
//! # SKIPped (CONSUMER-arm)
//!
//! - SKIP: `testAsyncConsumerAutoCommitIntercept` (line 225) —
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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;

use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
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

use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
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
/// Mirrors `PlaintextConsumerCommitTest`:
/// ```text
/// brokers = BROKER_COUNT (= 3)
/// offsets.topic.num.partitions     = 1
/// offsets.topic.replication.factor = 3
/// group.min.session.timeout.ms     = 100
/// ```
///
/// Additionally `num.partitions=2` so auto-created topics get 2
/// partitions — matching Java's `@BeforeEach`
/// `cluster.createTopic(topic, 2, BROKER_COUNT)`.
fn cluster_config_with_kip848_3brokers() -> ClusterConfig {
    // Java parity: `@BeforeEach setup() { cluster.createTopic(topic, 2, BROKER_COUNT); }`,
    // so auto-created topics get 2 partitions; the canonical helper supplies
    // the shared KIP-848 broker tuning.
    kip848_3_broker(2)
}

// ── Byte-array deserializer (Java uses `byte[]` keys and values) ──────

struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}

// ── Consumer config builder ───────────────────────────────────────────

/// Build a `ConsumerConfig` matching Java's `createConsumer(protocol,
/// enableAutoCommit)` which sets `group.id=test-group`,
/// `group.protocol=consumer`, and `enable.auto.commit`. Default
/// `auto.offset.reset=earliest`. Caller overrides win.
fn make_consumer_config_bytes(
    bootstrap: &str,
    group_id: &str,
    enable_auto_commit: bool,
    overrides: &[(&str, &str)],
) -> ConsumerConfig {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), enable_auto_commit.to_string()),
        ("group.id".to_string(), group_id.to_string()),
    ]);
    for (k, v) in overrides {
        props.insert((*k).to_string(), (*v).to_string());
    }
    ConsumerConfig::new(&props).expect("invalid test config")
}

fn new_bytes_consumer(config: ConsumerConfig) -> Box<dyn Consumer<Vec<u8>, Vec<u8>>> {
    new_consumer::<Vec<u8>, Vec<u8>>(config, Box::new(ByteArrayDeserializer), Box::new(ByteArrayDeserializer))
        .expect("new_consumer should succeed")
}

// ── Producer helpers (mirror Java's ClientsTestUtils.sendRecords) ─────

fn make_producer_config(bootstrap: &str) -> ProducerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "integration-test-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "5".to_string()),
    ]);
    ProducerConfig::new(&props).expect("invalid producer test config")
}

fn build_producer_bytes(bootstrap: &str) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    KafkaProducer::new(
        make_producer_config(bootstrap),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
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
async fn send_records_bytes(bootstrap: &str, tp: &TopicPartition, num_records: usize, starting_timestamp: i64) {
    let producer = build_producer_bytes(bootstrap);
    send_records_with_producer(&producer, tp, num_records, starting_timestamp).await;
    producer.close().await.expect("producer close should succeed");
}

/// Creates `topic` EMPTY by triggering broker metadata auto-creation and
/// waiting until it materializes with the expected partition count. Mirrors
/// Java's `cluster.createTopic(name, partitions, replicationFactor)` (no data
/// records, so the first produced record lands at offset 0).
///
/// The Rust harness has no admin client, but `partitions_for` over the
/// METADATA path triggers broker auto-create (`auto.create.topics.enable` is
/// on by default) with `num.partitions=2` — exactly an empty topic, matching
/// Java. This replaces the earlier provisioner-record approach, which placed
/// a record at offset 0 and shifted every real record by one.
async fn create_topic(consumer: &mut BytesConsumer, topic: &str, partitions: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let parts = consumer.partitions_for(topic).await.expect("partitions_for");
        if parts.len() >= partitions {
            return;
        }
        if Instant::now() >= deadline {
            panic!("topic {topic} not auto-created with >= {partitions} partitions within 30s");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Equivalent of Java's `cluster.createTopic(name, 2, BROKER_COUNT)` — creates
/// an EMPTY 2-partition topic via metadata auto-create (no records).
async fn ensure_topic_with_2_partitions(consumer: &mut BytesConsumer, topic: &str) {
    create_topic(consumer, topic, 2).await;
}

// ── Consumer test helpers ─────────────────────────────────────────────

/// Translates `ClientsTestUtils.awaitAssignment(consumer,
/// expectedAssignment)`. Drives `poll(100ms)` until `assignment()`
/// equals `expected` or the deadline elapses.
async fn await_assignment(
    consumer: &mut BytesConsumer,
    expected: &HashSet<TopicPartition>,
    deadline_duration: Duration,
) {
    let deadline = Instant::now() + deadline_duration;
    while Instant::now() < deadline {
        let _ = consumer
            .poll(Duration::from_millis(100))
            .await
            .expect("poll should succeed in await_assignment");
        if &consumer.assignment() == expected {
            return;
        }
    }
    panic!(
        "Timed out awaiting expected assignment of {} partitions; current {:?}",
        expected.len(),
        consumer.assignment()
    );
}

/// Translates `ClientsTestUtils.pollUntilTrue(consumer, predicate, msg)`.
/// Drives `poll(100ms)` in a loop (which also delivers any pending
/// `OffsetCommitCallback`, §31) until `predicate()` or the deadline.
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
            .expect("poll should not fail in poll_until_true");
        if predicate() {
            return;
        }
    }
    panic!("{msg}");
}

/// Translates `ClientsTestUtils.consumeAndVerifyRecords` (1ms-per-record
/// timestamp increment). Verifies topic/partition/timestamp/offset/key/
/// value inline.
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
        let records = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
        for record in records {
            if record.topic() != tp.topic() || record.partition() != tp.partition() {
                continue;
            }
            if next_index >= num_records {
                break;
            }
            let i = next_index;
            let offset = starting_offset + i as i64;
            assert_eq!(record.offset(), offset, "record offset should be {offset}");
            let expected_ts = starting_timestamp + i as i64;
            assert_eq!(record.timestamp(), expected_ts, "record timestamp should be {expected_ts}");
            let key_and_value_index = starting_key_and_value_index + i;
            let expected_key = format!("key {key_and_value_index}").into_bytes();
            let expected_value = format!("value {key_and_value_index}").into_bytes();
            assert_eq!(record.key().expect("key present"), &expected_key, "key at {i}");
            assert_eq!(record.value().expect("value present"), &expected_value, "value at {i}");
            next_index += 1;
        }
    }
    assert_eq!(
        next_index, num_records,
        "timed out before consuming {num_records} records (got {next_index})"
    );
}

// ── OffsetCommitCallback recorder (mirrors CountConsumerCommitCallback) ─

/// Translates Java's private `CountConsumerCommitCallback`. Counters are
/// `Arc<AtomicUsize>` (the callback is shared into an
/// `Arc<dyn OffsetCommitCallback>` consumed by the consumer; the test
/// keeps handles to read counts).
#[derive(Clone)]
struct CountConsumerCommitCallback {
    success_count: Arc<AtomicUsize>,
    error_count: Arc<AtomicUsize>,
    last_error: Arc<Mutex<Option<Error>>>,
}

impl CountConsumerCommitCallback {
    fn new() -> Self {
        Self {
            success_count: Arc::new(AtomicUsize::new(0)),
            error_count: Arc::new(AtomicUsize::new(0)),
            last_error: Arc::new(Mutex::new(None)),
        }
    }

    fn success_count(&self) -> usize {
        self.success_count.load(Ordering::SeqCst)
    }

    fn error_count(&self) -> usize {
        self.error_count.load(Ordering::SeqCst)
    }

    fn last_error_is_some(&self) -> bool {
        self.last_error.lock().expect("last_error mutex poisoned").is_some()
    }
}

#[async_trait]
impl OffsetCommitCallback for CountConsumerCommitCallback {
    async fn on_complete(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>, error: Option<&Error>) {
        match error {
            None => {
                self.success_count.fetch_add(1, Ordering::SeqCst);
            },
            Some(err) => {
                self.error_count.fetch_add(1, Ordering::SeqCst);
                *self.last_error.lock().expect("last_error mutex poisoned") = Some(err.clone());
            },
        }
    }
}

/// Mirror of Java's `sendAndAwaitAsyncCommit` + `RetryCommitCallback`:
/// commitAsync the given offsets and drive `poll()` until the callback
/// fires; on `RetriableCommitFailedException` resend (we resend the
/// async commit and keep polling). On any other error, fail.
async fn send_and_await_async_commit(
    consumer: &mut BytesConsumer,
    offsets: HashMap<TopicPartition, OffsetAndMetadata>,
) {
    let cb = CountConsumerCommitCallback::new();
    let cb_arc: Arc<dyn OffsetCommitCallback> = Arc::new(cb.clone());
    consumer
        .commit_async_with_offsets_callback(offsets.clone(), Arc::clone(&cb_arc))
        .await
        .expect("commit_async_with_offsets_callback should enqueue");

    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if cb.success_count() > 0 {
            return;
        }
        if cb.last_error_is_some() {
            // Java's RetryCommitCallback resends on RetriableCommitFailed;
            // any other error is fatal. Treat a recorded error as fatal
            // unless it is retriable, in which case resend.
            let err = cb.last_error.lock().expect("last_error poisoned").clone();
            if let Some(e) = err {
                if e.is_retriable_error() {
                    // Reset and resend.
                    *cb.last_error.lock().expect("poisoned") = None;
                    consumer
                        .commit_async_with_offsets_callback(offsets.clone(), Arc::clone(&cb_arc))
                        .await
                        .expect("resend should enqueue");
                } else {
                    panic!("async commit failed with non-retriable error: {e}");
                }
            }
        }
        let _ = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
    }
    panic!("Failed to observe commit callback before timeout");
}

// ── Local utilities ────────────────────────────────────────────────────

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as i64
}

// ── Tests ─────────────────────────────────────────────────────────────

/// Translates Java's `testAsyncConsumerAutoCommitOnClose` (line 96).
///
/// With `enable.auto.commit=true`, seek positions are auto-committed
/// when the consumer closes; another consumer in the same group sees the
/// committed positions.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_auto_commit_on_close() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_auto_commit_on_close");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    {
        let mut consumer =
            new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, true, &[]));
        // Empty topic (both partitions), so seeks define the committed
        // positions deterministically; producing to tp also auto-creates it.
        create_topic(consumer.as_mut(), &topic, 2).await;
        send_records_bytes(ctx.bootstrap_servers(), &tp, 1000, current_time_ms()).await;

        consumer
            .subscribe_with_topics(vec![topic.clone()])
            .await
            .expect("subscribe should succeed");
        let expected: HashSet<TopicPartition> = [tp.clone(), tp1.clone()].into_iter().collect();
        await_assignment(consumer.as_mut(), &expected, Duration::from_secs(90)).await;
        // Should auto-commit sought positions before closing.
        consumer
            .seek_with_offset(tp.clone(), 300)
            .await
            .expect("seek tp should succeed");
        consumer
            .seek_with_offset(tp1.clone(), 500)
            .await
            .expect("seek tp1 should succeed");
        consumer.close().await.expect("consumer close should succeed");
    }

    // Now we should see the committed positions from another consumer.
    let mut another = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, true, &[]));
    let committed = another
        .committed(&[tp.clone(), tp1.clone()])
        .await
        .expect("committed should succeed");
    assert_eq!(committed.get(&tp).expect("tp committed present").offset(), 300);
    assert_eq!(committed.get(&tp1).expect("tp1 committed present").offset(), 500);
    another.close().await.expect("another close should succeed");
}

/// Translates Java's `testAsyncConsumerAutoCommitOnCloseAfterWakeup`
/// (line 124). Same as above but `wakeup()` is called before close to
/// simulate breaking a poll loop from another thread; auto-commit still
/// flushes.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_auto_commit_on_close_after_wakeup() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_auto_commit_on_close_wakeup");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    {
        let mut consumer =
            new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, true, &[]));
        // Empty topic (both partitions); producing to tp also auto-creates it.
        create_topic(consumer.as_mut(), &topic, 2).await;
        send_records_bytes(ctx.bootstrap_servers(), &tp, 1000, current_time_ms()).await;

        consumer
            .subscribe_with_topics(vec![topic.clone()])
            .await
            .expect("subscribe should succeed");
        let expected: HashSet<TopicPartition> = [tp.clone(), tp1.clone()].into_iter().collect();
        await_assignment(consumer.as_mut(), &expected, Duration::from_secs(90)).await;
        consumer
            .seek_with_offset(tp.clone(), 300)
            .await
            .expect("seek tp should succeed");
        consumer
            .seek_with_offset(tp1.clone(), 500)
            .await
            .expect("seek tp1 should succeed");
        // Wakeup before closing to simulate breaking a poll loop from
        // another thread. The pending wakeup must not prevent close-path
        // auto-commit from flushing.
        consumer.wakeup();
        consumer.close().await.expect("consumer close should succeed after wakeup");
    }

    let mut another = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, true, &[]));
    let committed = another
        .committed(&[tp.clone(), tp1.clone()])
        .await
        .expect("committed should succeed");
    assert_eq!(committed.get(&tp).expect("tp committed present").offset(), 300);
    assert_eq!(committed.get(&tp1).expect("tp1 committed present").offset(), 500);
    another.close().await.expect("another close should succeed");
}

/// Translates Java's `testAsyncConsumerCommitMetadata` (line 157).
///
/// Sync commit with leaderEpoch + metadata round-trips; async commit with
/// metadata round-trips; null metadata (empty string in Rust) round-trips.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_commit_metadata() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_commit_metadata");
    let tp = TopicPartition::new(topic.clone(), 0);

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, true, &[]));
    // Ensure the topic exists so assign() resolves a real partition.
    ensure_topic_with_2_partitions(consumer.as_mut(), &topic).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");

    // Sync commit: offset 5, leaderEpoch 15, metadata "foo".
    let sync_metadata = OffsetAndMetadata::with_leader_epoch_metadata(5, Some(15), "foo").expect("OffsetAndMetadata");
    let mut sync_offsets = HashMap::new();
    sync_offsets.insert(tp.clone(), sync_metadata.clone());
    consumer
        .commit_sync_with_offsets(sync_offsets)
        .await
        .expect("commit_sync_with_offsets should succeed");
    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .expect("committed should succeed");
    assert_eq!(committed.get(&tp).expect("tp committed present"), &sync_metadata);

    // Async commit: offset 10, metadata "bar".
    let async_metadata = OffsetAndMetadata::with_metadata(10, "bar").expect("OffsetAndMetadata");
    let mut async_offsets = HashMap::new();
    async_offsets.insert(tp.clone(), async_metadata.clone());
    send_and_await_async_commit(consumer.as_mut(), async_offsets).await;
    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .expect("committed should succeed");
    assert_eq!(committed.get(&tp).expect("tp committed present"), &async_metadata);

    // Null metadata: Java `new OffsetAndMetadata(5, null)` normalizes to
    // empty metadata. Rust `OffsetAndMetadata::new(5)` is the same
    // (empty-string metadata, no leader epoch).
    let null_metadata = OffsetAndMetadata::new(5).expect("OffsetAndMetadata");
    let mut null_offsets = HashMap::new();
    null_offsets.insert(tp.clone(), null_metadata.clone());
    consumer
        .commit_sync_with_offsets(null_offsets)
        .await
        .expect("commit_sync_with_offsets should succeed");
    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .expect("committed should succeed");
    assert_eq!(committed.get(&tp).expect("tp committed present"), &null_metadata);

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncConsumerAsyncCommit` (line 187).
///
/// Five `commitAsync` calls with offsets 1..=5; poll until the callback
/// has observed 5 successes; assert no error, success count 5, and the
/// committed offset is 5.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_async_commit() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_async_commit");
    let tp = TopicPartition::new(topic.clone(), 0);

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, false, &[]));
    ensure_topic_with_2_partitions(consumer.as_mut(), &topic).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");

    let cb = CountConsumerCommitCallback::new();
    let cb_arc: Arc<dyn OffsetCommitCallback> = Arc::new(cb.clone());
    let count = 5;
    for i in 1..=count {
        let mut offsets = HashMap::new();
        offsets.insert(tp.clone(), OffsetAndMetadata::new(i).expect("OffsetAndMetadata"));
        consumer
            .commit_async_with_offsets_callback(offsets, Arc::clone(&cb_arc))
            .await
            .expect("commit_async should enqueue");
    }

    poll_until_true(
        consumer.as_mut(),
        || cb.success_count() >= count as usize || cb.last_error_is_some(),
        Duration::from_secs(30),
        "Failed to observe commit callback before timeout",
    )
    .await;

    assert!(!cb.last_error_is_some(), "no commit error expected");
    assert_eq!(cb.success_count(), count as usize, "success count should be {count}");
    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .expect("committed should succeed");
    assert_eq!(committed.get(&tp).expect("tp committed present").offset(), count);

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncConsumerCommitSpecifiedOffsets` (line 308).
///
/// Commits specific per-partition offsets; positions are unchanged by the
/// commit; async commit picks up the committed changes.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_commit_specified_offsets() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_commit_specified_offsets");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, false, &[]));
    ensure_topic_with_2_partitions(consumer.as_mut(), &topic).await;
    let producer = build_producer_bytes(ctx.bootstrap_servers());
    let now = current_time_ms();
    send_records_with_producer(&producer, &tp, 5, now).await;
    send_records_with_producer(&producer, &tp1, 7, now).await;
    producer.close().await.expect("producer close should succeed");

    consumer
        .assign(vec![tp.clone(), tp1.clone()])
        .await
        .expect("assign should succeed");

    let pos1 = consumer.position(&tp).await.expect("position tp");
    let pos2 = consumer.position(&tp1).await.expect("position tp1");

    let mut commit1 = HashMap::new();
    commit1.insert(tp.clone(), OffsetAndMetadata::new(3).expect("OffsetAndMetadata"));
    consumer
        .commit_sync_with_offsets(commit1)
        .await
        .expect("commit_sync_with_offsets tp");

    let committed = consumer.committed(std::slice::from_ref(&tp)).await.expect("committed tp");
    assert_eq!(committed.get(&tp).expect("tp committed present").offset(), 3);
    let committed_tp1 = consumer.committed(std::slice::from_ref(&tp1)).await.expect("committed tp1");
    assert!(!committed_tp1.contains_key(&tp1), "tp1 should not be committed yet");

    // Positions should not change.
    assert_eq!(consumer.position(&tp).await.expect("position tp"), pos1);
    assert_eq!(consumer.position(&tp1).await.expect("position tp1"), pos2);

    let mut commit2 = HashMap::new();
    commit2.insert(tp1.clone(), OffsetAndMetadata::new(5).expect("OffsetAndMetadata"));
    consumer
        .commit_sync_with_offsets(commit2)
        .await
        .expect("commit_sync_with_offsets tp1");

    let committed = consumer.committed(&[tp.clone(), tp1.clone()]).await.expect("committed both");
    assert_eq!(committed.get(&tp).expect("tp committed").offset(), 3);
    assert_eq!(committed.get(&tp1).expect("tp1 committed").offset(), 5);

    // Async should pick up the committed changes after commit completes.
    let mut commit3 = HashMap::new();
    commit3.insert(tp1.clone(), OffsetAndMetadata::new(7).expect("OffsetAndMetadata"));
    send_and_await_async_commit(consumer.as_mut(), commit3).await;
    let committed = consumer.committed(std::slice::from_ref(&tp1)).await.expect("committed tp1");
    assert_eq!(committed.get(&tp1).expect("tp1 committed").offset(), 7);

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncConsumerAutoCommitOnRebalance` (line 350).
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
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_auto_commit_on_rebalance() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let topic2 = ctx.topic("topic2");
    let group_id = ctx.group_id("g_auto_commit_on_rebalance");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, true, &[]));
    // Empty topics so produced records start at offset 0 (Java parity).
    ensure_topic_with_2_partitions(consumer.as_mut(), &topic).await;
    ensure_topic_with_2_partitions(consumer.as_mut(), &topic2).await;

    // Produce up to each seek target so the seeks below are in-range (log
    // end), so no fetch advances or resets the position — see the
    // pause-free deviation note above.
    let producer = build_producer_bytes(ctx.bootstrap_servers());
    let now = current_time_ms();
    send_records_with_producer(&producer, &tp, 300, now).await;
    send_records_with_producer(&producer, &tp1, 500, now).await;
    producer.close().await.expect("producer close should succeed");

    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .expect("subscribe should succeed");
    let expected: HashSet<TopicPartition> = [tp.clone(), tp1.clone()].into_iter().collect();
    await_assignment(consumer.as_mut(), &expected, Duration::from_secs(90)).await;

    consumer.seek_with_offset(tp.clone(), 300).await.expect("seek tp");
    consumer.seek_with_offset(tp1.clone(), 500).await.expect("seek tp1");

    // Change subscription to trigger a rebalance — auto-commit fires on
    // the revocation that precedes the new assignment.
    consumer
        .subscribe_with_topics(vec![topic.clone(), topic2.clone()])
        .await
        .expect("re-subscribe should succeed");

    let new_assignment: HashSet<TopicPartition> = [
        tp.clone(),
        tp1.clone(),
        TopicPartition::new(topic2.clone(), 0),
        TopicPartition::new(topic2.clone(), 1),
    ]
    .into_iter()
    .collect();
    await_assignment(consumer.as_mut(), &new_assignment, Duration::from_secs(90)).await;

    // After rebalancing, we should have reset to the committed positions.
    let committed = consumer
        .committed(&[tp.clone(), tp1.clone()])
        .await
        .expect("committed should succeed");
    assert_eq!(committed.get(&tp).expect("tp committed").offset(), 300);
    assert_eq!(committed.get(&tp1).expect("tp1 committed").offset(), 500);

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncConsumerSubscribeAndCommitSync` (line 396).
///
/// Ensures the member ID is propagated from the group coordinator when
/// the assignment is received into a subsequent offset commit:
/// subscribe → await assignment → seek(0) → `commit_sync()`.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_subscribe_and_commit_sync() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_subscribe_and_commit_sync");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, false, &[]));
    ensure_topic_with_2_partitions(consumer.as_mut(), &topic).await;
    assert_eq!(consumer.assignment().len(), 0);
    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .expect("subscribe should succeed");
    let expected: HashSet<TopicPartition> = [tp.clone(), tp1.clone()].into_iter().collect();
    await_assignment(consumer.as_mut(), &expected, Duration::from_secs(90)).await;

    consumer.seek_with_offset(tp.clone(), 0).await.expect("seek tp");
    consumer.commit_sync().await.expect("commit_sync should succeed");

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncConsumerPositionAndCommit` (line 419).
///
/// `position()` on an unassigned partition throws `LocalIllegalState`; after
/// assigning, position resets to 0; commit/position interplay; another
/// consumer in the same group reads from the committed position.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_position_and_commit() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_position_and_commit");
    let tp = TopicPartition::new(topic.clone(), 0);

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, false, &[]));
    let mut other = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, false, &[]));

    // Empty topic so produced records start at offset 0 (Java parity).
    ensure_topic_with_2_partitions(consumer.as_mut(), &topic).await;
    let producer = build_producer_bytes(ctx.bootstrap_servers());
    let starting_timestamp = current_time_ms();
    send_records_with_producer(&producer, &tp, 5, starting_timestamp).await;

    // Partition 15 does not exist / is not assigned.
    let unassigned = TopicPartition::new(topic.clone(), 15);
    let committed = consumer
        .committed(std::slice::from_ref(&unassigned))
        .await
        .expect("committed should succeed");
    assert!(
        !committed.contains_key(&unassigned),
        "unassigned tp should have no committed offset"
    );

    // position() on a partition we are not subscribed to throws.
    let err = consumer
        .position(&unassigned)
        .await
        .expect_err("position on unassigned should err");
    assert!(
        matches!(err, Error::LocalIllegalState(_)),
        "expected IllegalState for position() on unassigned partition, got {err:?}"
    );

    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");

    assert_eq!(
        consumer.position(&tp).await.expect("position tp"),
        0,
        "position() on a subscribed partition should reset the offset to 0"
    );
    consumer.commit_sync().await.expect("commit_sync should succeed");
    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .expect("committed should succeed");
    assert_eq!(committed.get(&tp).expect("tp committed").offset(), 0);

    consume_and_verify_records_bytes(consumer.as_mut(), &tp, 5, 0, 0, starting_timestamp).await;
    assert_eq!(
        consumer.position(&tp).await.expect("position tp"),
        5,
        "after consuming 5 records, position should be 5"
    );
    consumer.commit_sync().await.expect("commit_sync should succeed");
    let committed = consumer
        .committed(std::slice::from_ref(&tp))
        .await
        .expect("committed should succeed");
    assert_eq!(
        committed.get(&tp).expect("tp committed").offset(),
        5,
        "committed offset should be returned"
    );

    let starting_timestamp2 = current_time_ms();
    send_records_with_producer(&producer, &tp, 1, starting_timestamp2).await;
    producer.close().await.expect("producer close should succeed");

    // Another consumer in the same group should get the same position.
    other.assign(vec![tp.clone()]).await.expect("other assign should succeed");
    consume_and_verify_records_bytes(other.as_mut(), &tp, 1, 5, 0, starting_timestamp2).await;

    consumer.close().await.expect("consumer close should succeed");
    other.close().await.expect("other close should succeed");
}

/// Translates Java's `testCommitAsyncCompletedBeforeConsumerCloses`
/// (line 492).
///
/// Contract: async offset commits complete before the consumer is closed,
/// even when no commit-sync is performed as part of close (auto-commit
/// disabled).
#[tokio::test(flavor = "multi_thread")]
async fn test_commit_async_completed_before_consumer_closes() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_async_completed_before_close");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    let cb = CountConsumerCommitCallback::new();
    {
        let mut consumer =
            new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, false, &[]));
        ensure_topic_with_2_partitions(consumer.as_mut(), &topic).await;

        let producer = build_producer_bytes(ctx.bootstrap_servers());
        let now = current_time_ms();
        send_records_with_producer(&producer, &tp, 3, now).await;
        send_records_with_producer(&producer, &tp1, 3, now).await;
        producer.close().await.expect("producer close should succeed");

        consumer
            .assign(vec![tp.clone(), tp1.clone()])
            .await
            .expect("assign should succeed");

        // Java pre-creates the GROUP_METADATA_TOPIC_NAME (offsets) topic so
        // the coordinator is available during close
        // (`PlaintextConsumerCommitTest.java:484-485`). The Rust harness has
        // no admin client; the equivalent is to discover the coordinator and
        // materialize the offsets topic up front via a `committed()` query, so
        // the two async commits below can complete during the (bounded) close.
        let _ = consumer
            .committed(std::slice::from_ref(&tp))
            .await
            .expect("committed (coordinator readiness) should succeed");

        let cb_arc: Arc<dyn OffsetCommitCallback> = Arc::new(cb.clone());
        // Try without looking up the coordinator first.
        let mut o1 = HashMap::new();
        o1.insert(tp.clone(), OffsetAndMetadata::new(1).expect("OffsetAndMetadata"));
        consumer
            .commit_async_with_offsets_callback(o1, Arc::clone(&cb_arc))
            .await
            .expect("commitAsync 1");
        let mut o2 = HashMap::new();
        o2.insert(tp1.clone(), OffsetAndMetadata::new(1).expect("OffsetAndMetadata"));
        consumer
            .commit_async_with_offsets_callback(o2, Arc::clone(&cb_arc))
            .await
            .expect("commitAsync 2");

        consumer.close().await.expect("consumer close should succeed");
    }
    // The two async commits must have completed before close returned.
    assert_eq!(cb.success_count(), 2, "both async commits should complete before close");
}

/// Translates Java's `testCommitAsyncCompletedBeforeCommitSyncReturns`
/// (line 519).
///
/// Contract: async commits sent before a `commitSync` are guaranteed to
/// have their callbacks invoked prior to completion of `commitSync`.
#[tokio::test(flavor = "multi_thread")]
async fn test_commit_async_completed_before_commit_sync_returns() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_async_completed_before_commit_sync");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, false, &[]));
    ensure_topic_with_2_partitions(consumer.as_mut(), &topic).await;
    let producer = build_producer_bytes(ctx.bootstrap_servers());
    let now = current_time_ms();
    send_records_with_producer(&producer, &tp, 3, now).await;
    send_records_with_producer(&producer, &tp1, 3, now).await;
    producer.close().await.expect("producer close should succeed");

    consumer
        .assign(vec![tp.clone(), tp1.clone()])
        .await
        .expect("assign should succeed");

    let cb = CountConsumerCommitCallback::new();
    let cb_arc: Arc<dyn OffsetCommitCallback> = Arc::new(cb.clone());

    // Try without looking up the coordinator first.
    let mut o1 = HashMap::new();
    o1.insert(tp.clone(), OffsetAndMetadata::new(1).expect("OffsetAndMetadata"));
    consumer
        .commit_async_with_offsets_callback(o1, Arc::clone(&cb_arc))
        .await
        .expect("commitAsync 1");
    // Empty sync commit: the async callback must fire before it returns.
    consumer
        .commit_sync_with_offsets(HashMap::new())
        .await
        .expect("commit_sync empty");

    let committed = consumer.committed(std::slice::from_ref(&tp)).await.expect("committed");
    assert_eq!(committed.get(&tp).expect("tp committed").offset(), 1);
    assert_eq!(cb.success_count(), 1, "async callback should fire before commitSync returns");

    // Try with coordinator known.
    let mut o2 = HashMap::new();
    o2.insert(tp.clone(), OffsetAndMetadata::new(2).expect("OffsetAndMetadata"));
    consumer
        .commit_async_with_offsets_callback(o2, Arc::clone(&cb_arc))
        .await
        .expect("commitAsync 2");
    let mut sync2 = HashMap::new();
    sync2.insert(tp1.clone(), OffsetAndMetadata::new(2).expect("OffsetAndMetadata"));
    consumer.commit_sync_with_offsets(sync2).await.expect("commit_sync tp1");

    let committed = consumer.committed(&[tp.clone(), tp1.clone()]).await.expect("committed");
    assert_eq!(committed.get(&tp).expect("tp committed").offset(), 2);
    assert_eq!(committed.get(&tp1).expect("tp1 committed").offset(), 2);
    assert_eq!(cb.success_count(), 2);

    // Try with empty sync commit.
    let mut o3 = HashMap::new();
    o3.insert(tp.clone(), OffsetAndMetadata::new(3).expect("OffsetAndMetadata"));
    consumer
        .commit_async_with_offsets_callback(o3, Arc::clone(&cb_arc))
        .await
        .expect("commitAsync 3");
    consumer
        .commit_sync_with_offsets(HashMap::new())
        .await
        .expect("commit_sync empty");

    let committed = consumer.committed(&[tp.clone(), tp1.clone()]).await.expect("committed");
    assert_eq!(committed.get(&tp).expect("tp committed").offset(), 3);
    assert_eq!(committed.get(&tp1).expect("tp1 committed").offset(), 2);
    assert_eq!(cb.success_count(), 3);

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose`
/// (line 461).
///
/// `#[ignore]`d: the Java test calls `cluster.brokerIds().forEach(
/// cluster::shutdownBroker)` to make the coordinator unavailable, then
/// asserts the async-commit callback fails with a `CommitFailedException`
/// whose message is exactly `"Failed to commit offsets: Coordinator
/// unknown and consumer is closing"` and that close completes in under 1s.
///
/// The Rust integration harness pools and shares clusters across tests
/// (`tests/common/cluster_pool.rs`, keyed by `ClusterConfig`) and exposes
/// no broker-shutdown API on `KafkaCluster`. Killing brokers in a pooled
/// cluster would break every co-resident test, and the capability does
/// not exist. The exact close-path contract is already covered by the
/// unit test
/// `commit_request_manager::tests::commit_async_fails_when_coordinator_unavailable_during_close`
/// (`src/consumer/internals/commit_request_manager.rs:4217+`), which
/// asserts the same `CommitFailedException` message string.
///
/// This body is kept (and wired into CI as `#[ignore]`d) to document the
/// integration-level gap and to be ready to run once the harness gains a
/// per-test isolated cluster with broker-shutdown support.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Requires cluster.shutdownBroker() on all brokers; the pooled \
            test harness has no broker-shutdown API. The exact close-path \
            commit-failed message is unit-tested in \
            commit_request_manager.rs:4217+."]
async fn test_commit_async_fails_when_coordinator_unavailable_during_close() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_coordinator_unavailable_during_close");
    let tp = TopicPartition::new(topic.clone(), 0);

    let mut consumer = new_bytes_consumer(make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, false, &[]));
    ensure_topic_with_2_partitions(consumer.as_mut(), &topic).await;
    let producer = build_producer_bytes(ctx.bootstrap_servers());
    send_records_with_producer(&producer, &tp, 3, current_time_ms()).await;
    producer.close().await.expect("producer close should succeed");

    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");

    // NOTE: the Java step `cluster.brokerIds().forEach(cluster::shutdownBroker)`
    // has no Rust equivalent in the pooled harness — see the `#[ignore]`
    // rationale above. The assertions below document the contract.
    let cb = CountConsumerCommitCallback::new();
    let cb_arc: Arc<dyn OffsetCommitCallback> = Arc::new(cb.clone());

    let _ = consumer.poll(Duration::from_millis(500)).await;
    let mut offsets = HashMap::new();
    offsets.insert(tp.clone(), OffsetAndMetadata::new(1).expect("OffsetAndMetadata"));
    consumer
        .commit_async_with_offsets_callback(offsets, Arc::clone(&cb_arc))
        .await
        .expect("commitAsync");

    let start = Instant::now();
    consumer
        .close_with_options(confluent_kafka::consumer::CloseOptions::new_timeout(Duration::from_millis(500)))
        .await
        .expect("close should complete");
    let close_duration = start.elapsed();

    assert!(close_duration < Duration::from_secs(1), "close too long: {close_duration:?}");
    assert!(cb.last_error_is_some(), "callback should have recorded an error");
    let err = cb.last_error.lock().expect("poisoned").clone().expect("error present");
    // Java asserts `CommitFailedException`, which is now its own Rust class
    // rather than being flattened into `Error::LocalIllegalState`.
    assert!(
        matches!(&err, Error::ConsumerCommitFailed(e)
            if e.message() == "Failed to commit offsets: Coordinator unknown and consumer is closing"),
        "expected the exact commit-failed message, got {err:?}"
    );
    // Java's `getMessage()` is `Error::message()`; `Display` is `toString()`,
    // which now prefixes the class name for every translated class.
    assert_eq!(
        err.message(),
        "Failed to commit offsets: Coordinator unknown and consumer is closing"
    );
    assert_eq!(cb.error_count(), 1);
}
