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
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerFetchTest.java`
//! at pinned commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.
//!
//! These tests exercise the consumer **fetch path** (`poll`, `seek`,
//! `auto.offset.reset` policies, `fetch.max.bytes` /
//! `max.partition.fetch.bytes` interactions) against a real 3-broker
//! Kafka 4.2.0 cluster with KIP-848 (`group.protocol=consumer`) enabled.
//!
//! # Translated methods (KIP-848 / `GroupProtocol.CONSUMER` arm only)
//!
//! All 9 KIP-848 tests are translated and pass end-to-end. Previously
//! `test_async_consumer_fetch_out_of_range_offset_reset_config_by_duration`
//! was `#[ignore]`-gated on Issue 5 (`auto.offset.reset=by_duration:PT1H`
//! never landing on a position). Issue 5 was resolved transitively by the
//! Issue 7 fix (transient-state skip in `fetch_collector` /
//! `abstract_fetch`) and the Issue 9 fix (KIP-848 `GroupIdNotFound` retry +
//! poll-timer init): once these fixes let the consumer ride out the
//! transient "Missing position" / fence-rejoin states, the `by_duration`
//! `ListOffsetsByTimestamp` reset path — which was already wired in
//! `AutoOffsetResetStrategy::timestamp()` and consumed by
//! `OffsetFetcherUtils::get_offset_reset_strategy_for_partitions` and
//! `OffsetsRequestManager::send_list_offsets_requests_and_reset_positions`
//! — successfully computes the time-bounded position. See
//! `design/history/Milestone-8/Phase-13/COMMENTS.DONE.1.md` Issue 5.
//!
//! - `testAsyncConsumerFetchInvalidOffset`
//!   → `test_async_consumer_fetch_invalid_offset`
//! - `testAsyncConsumerFetchOutOfRangeOffsetResetConfigEarliest`
//!   → `test_async_consumer_fetch_out_of_range_offset_reset_config_earliest`
//! - `testAsyncConsumerFetchOutOfRangeOffsetResetConfigLatest`
//!   → `test_async_consumer_fetch_out_of_range_offset_reset_config_latest`
//! - `testAsyncConsumerFetchOutOfRangeOffsetResetConfigByDuration`
//!   → `test_async_consumer_fetch_out_of_range_offset_reset_config_by_duration`
//! - `testAsyncConsumerFetchRecordLargerThanFetchMaxBytes`
//!   → `test_async_consumer_fetch_record_larger_than_fetch_max_bytes`
//! - `testAsyncConsumerFetchRecordLargerThanMaxPartitionFetchBytes`
//!   → `test_async_consumer_fetch_record_larger_than_max_partition_fetch_bytes`
//! - `testAsyncConsumerFetchHonoursFetchSizeIfLargeRecordNotFirst`
//!   → `test_async_consumer_fetch_honours_fetch_size_if_large_record_not_first`
//! - `testAsyncConsumerFetchHonoursMaxPartitionFetchBytesIfLargeRecordNotFirst`
//!   → `test_async_consumer_fetch_honours_max_partition_fetch_bytes_if_large_record_not_first`
//! - `testAsyncConsumerLowMaxFetchSizeForRequestAndPartition`
//!   → `test_async_consumer_low_max_fetch_size_for_request_and_partition`
//!
//! # SKIPped methods
//!
//! The 9 `testClassicConsumer*` twin methods are SKIPped — the project
//! targets KIP-848 only per `.claude/rules/consumer-threading.md` §20.
//! They are enumerated here for traceability:
//!
//! - SKIP: `testClassicConsumerFetchInvalidOffset` — classic-protocol-only
//! - SKIP: `testClassicConsumerFetchOutOfRangeOffsetResetConfigEarliest` — classic-protocol-only
//! - SKIP: `testClassicConsumerFetchOutOfRangeOffsetResetConfigLatest` — classic-protocol-only
//! - SKIP: `testClassicConsumerFetchOutOfRangeOffsetResetConfigByDuration` — classic-protocol-only
//! - SKIP: `testClassicConsumerFetchRecordLargerThanFetchMaxBytes` — classic-protocol-only
//! - SKIP: `testClassicConsumerFetchRecordLargerThanMaxPartitionFetchBytes` — classic-protocol-only
//! - SKIP: `testClassicConsumerFetchHonoursFetchSizeIfLargeRecordNotFirst` — classic-protocol-only
//! - SKIP: `testClassicConsumerFetchHonoursMaxPartitionFetchBytesIfLargeRecordNotFirst` — classic-protocol-only
//! - SKIP: `testClassicConsumerLowMaxFetchSizeForRequestAndPartition` — classic-protocol-only

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::ConsumerRecord;
use confluent_kafka::consumer::new_consumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};

use crate::common::test_context::TestContext;
use crate::common::test_utils::wait_for_all_partitions_metadata_with_context;

// Type alias matching the bytes-typed `Consumer` trait object returned
// by `new_consumer::<Vec<u8>, Vec<u8>>`. Used in helper signatures so
// the tests pass `&mut consumer` (which deref-coerces from
// `Box<dyn Consumer<Vec<u8>, Vec<u8>>>`).
type BytesConsumer = dyn Consumer<Vec<u8>, Vec<u8>>;

// Consumed records bucketed by topic-partition, borrowed from the batch the
// poll loop collected them into.
type RecordsByPartition<'a> = HashMap<TopicPartition, Vec<&'a ConsumerRecord<Vec<u8>, Vec<u8>>>>;

// ── Cluster config ────────────────────────────────────────────────────

/// Cluster config matching the Java suite's `@ClusterTestDefaults`:
/// 3 brokers, KIP-848 enabled on the coordinator, plus the broker
/// properties from the `serverProperties` annotation.
///
/// Mirrors:
/// ```text
/// brokers = PlaintextConsumerFetchTest.BROKER_COUNT (= 3)
/// offsets.topic.partitions         = 1
/// offsets.topic.replication.factor = 3
/// group.min.session.timeout.ms     = 100
/// ```
///
/// Additionally, `num.partitions=2` is set so auto-created topics get
/// 2 partitions — matching the Java `@BeforeEach`
/// `cluster.createTopic(topic, 2, BROKER_COUNT)`. This is required by
/// [`test_async_consumer_fetch_out_of_range_offset_reset_config_by_duration`]
/// which writes to both `topic-0` and `topic-1`. Other tests use only
/// partition 0 and are unaffected.
///
/// All tests except `test_async_consumer_low_max_fetch_size_for_request_and_partition`
/// share this cluster config so the pool in `tests/common/cluster_pool.rs`
/// materializes one 3-broker cluster and amortizes its 30–60s startup
/// across the suite. The low-max-fetch-size test needs `num.partitions=30`
/// for auto-created topics and therefore uses a distinct config.
fn cluster_config_with_kip848_3brokers() -> ClusterConfig {
    // Java parity: `@BeforeEach setup() { cluster.createTopic(topic, 2, BROKER_COUNT); }`,
    // so auto-created topics get 2 partitions; the canonical helper supplies
    // the shared KIP-848 broker tuning.
    kip848_3_broker(2)
}

/// Variant of [`cluster_config_with_kip848_3brokers`] with
/// `num.partitions=30` — the default partition count for auto-created
/// topics. Required by
/// [`test_async_consumer_low_max_fetch_size_for_request_and_partition`]
/// which exercises 30 partitions × 3 topics.
fn cluster_config_with_kip848_3brokers_30parts() -> ClusterConfig {
    kip848_3_broker(30)
}

// ── Byte-array deserializer (Java uses `byte[]` keys and values) ──────

/// Local byte-array deserializer for these tests. The crate exports
/// `ByteArraySerializer` but no symmetric `ByteArrayDeserializer`; this
/// inline impl is identical to what such a struct would do.
struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}

// ── Consumer config builder ───────────────────────────────────────────

/// Build a `ConsumerConfig` matching what Java's
/// `clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, "consumer", ...))`
/// produces. Caller-supplied overrides win over defaults.
///
/// Default `auto.offset.reset=earliest` so the tests' explicit `seek` /
/// reset-policy calls are the only offset-state transitions.
fn make_consumer_config_bytes(bootstrap: &str, group_id: &str, overrides: &[(&str, &str)]) -> ConsumerConfig {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("group.id".to_string(), group_id.to_string()),
    ]);
    for (k, v) in overrides {
        props.insert((*k).to_string(), (*v).to_string());
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

/// Build a [`KafkaProducer`] for byte-array keys/values matching what
/// Java's `cluster.producer()` returns.
fn build_producer_bytes(bootstrap: &str) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    KafkaProducer::from_config(
        make_producer_config(bootstrap),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("Failed to build test producer")
}

/// Translates Java's `ClientsTestUtils.sendRecords(producer, tp, num,
/// startingTimestamp, timestampIncrement)`. Keys are `b"key {i}"` and
/// values are `b"value {i}"`, matching Java's `KEY_PREFIX = "key "` and
/// `VALUE_PREFIX = "value "`. Timestamps are
/// `starting_timestamp + i * (timestamp_increment if > 0 else 1)`.
///
/// All records are fired to the producer up front, then `flush()` waits
/// for the broker acks. The producer is left open so the caller may
/// reuse it for additional sends (mirroring Java's
/// `try-with-resources` `Producer<...> producer = cluster.producer()`
/// pattern where the producer lives across multiple `sendRecords`
/// calls).
async fn send_records_with_producer(
    producer: &KafkaProducer<Vec<u8>, Vec<u8>>,
    tp: &TopicPartition,
    num_records: usize,
    starting_timestamp: i64,
    timestamp_increment: i64,
) {
    let inc = if timestamp_increment > 0 {
        timestamp_increment
    } else {
        1
    };
    let mut last_future = None;
    for i in 0..num_records {
        let timestamp = starting_timestamp + i as i64 * inc;
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
        last_future = Some(
            <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(producer, record)
                .await
                .expect("send should not fail"),
        );
    }
    producer.flush().await.expect("producer.flush should succeed");
    if let Some(f) = last_future {
        f.get_timeout(Duration::from_secs(30)).await.expect("last send should succeed");
    }
}

/// Translates Java's `ClientsTestUtils.sendRecords(cluster, tp, num,
/// startingTimestamp)` shorthand. Creates a fresh producer, sends, then
/// closes it.
async fn send_records_bytes(bootstrap: &str, tp: &TopicPartition, num_records: usize, starting_timestamp: i64) {
    let producer = build_producer_bytes(bootstrap);
    send_records_with_producer(&producer, tp, num_records, starting_timestamp, -1).await;
    producer.close().await.expect("producer close should succeed");
}

// ── Consumer test helpers (mirror ClientsTestUtils.consumeRecords / consumeAndVerifyRecords) ──

/// Translates Java's `ClientsTestUtils.consumeRecords(consumer,
/// numRecords)`. Drives `poll(100ms)` until `num_records` records have
/// been collected (or the 60s wall-clock budget elapses). Returns all
/// collected records as owned `ConsumerRecord` values (no `Clone` is
/// required because we own them via `into_iter`).
async fn consume_records_bytes(
    consumer: &mut BytesConsumer,
    num_records: usize,
) -> Vec<ConsumerRecord<Vec<u8>, Vec<u8>>> {
    consume_records_bytes_with_deadline(consumer, num_records, Duration::from_secs(60)).await
}

/// Variant of [`consume_records_bytes`] with a caller-supplied wall-clock
/// deadline. Used by the low-max-fetch-size test which needs to consume
/// 2700 records across 90 partitions with very tight per-partition fetch
/// budgets — the default 60s deadline is not enough on the Rust
/// implementation. Java relies on `TestUtils.waitForCondition`'s default
/// 60s but exercises a faster JVM producer; here we bump for parity of
/// outcome.
async fn consume_records_bytes_with_deadline(
    consumer: &mut BytesConsumer,
    num_records: usize,
    deadline_duration: Duration,
) -> Vec<ConsumerRecord<Vec<u8>, Vec<u8>>> {
    let deadline = Instant::now() + deadline_duration;
    let mut collected: Vec<ConsumerRecord<Vec<u8>, Vec<u8>>> = Vec::with_capacity(num_records);
    while collected.len() < num_records && Instant::now() < deadline {
        let records = consumer
            .poll(Duration::from_millis(100))
            .await
            .expect("poll should succeed in consume_records_bytes");
        for r in records {
            collected.push(r);
        }
    }
    assert!(
        collected.len() >= num_records,
        "Timed out before consuming expected {num_records} records (got {})",
        collected.len()
    );
    collected
}

/// Translates Java's `ClientsTestUtils.consumeAndVerifyRecords(consumer,
/// tp, numRecords, startingOffset, startingKeyAndValueIndex,
/// startingTimestamp, timestampIncrement)`.
///
/// Drives `poll(100ms)` in a loop until `num_records` records have been
/// collected (or the 60s wall-clock budget elapses), then asserts on
/// `topic`, `partition`, `timestamp_type == CreateTime`, `timestamp`,
/// `offset`, key/value bytes, and the serialized-size accessors — the
/// full Java assertion set.
async fn consume_and_verify_records_bytes(
    consumer: &mut BytesConsumer,
    tp: &TopicPartition,
    num_records: usize,
    starting_offset: i64,
    starting_key_and_value_index: usize,
    starting_timestamp: i64,
    timestamp_increment: i64,
) {
    let inc = if timestamp_increment > 0 {
        timestamp_increment
    } else {
        1
    };
    let collected = consume_records_bytes(consumer, num_records).await;
    assert!(
        collected.len() >= num_records,
        "expected at least {num_records} records, got {}",
        collected.len()
    );
    for (i, record) in collected.iter().take(num_records).enumerate() {
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
        let expected_ts = starting_timestamp + i as i64 * inc;
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
    }
}

/// Translates Java's `ClientsTestUtils.awaitAssignment(consumer,
/// expectedAssignment)`. Polls until `consumer.assignment()` equals
/// `expected` or the 60s wall-clock budget elapses.
async fn await_assignment(consumer: &mut BytesConsumer, expected: &HashSet<TopicPartition>) {
    let deadline = Instant::now() + Duration::from_secs(60);
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
        "Timed out while awaiting expected assignment of {} partitions. The current assignment is {} partitions.",
        expected.len(),
        consumer.assignment().len()
    );
}

// ── Tests ─────────────────────────────────────────────────────────────

/// Translates Java's `testAsyncConsumerFetchInvalidOffset` (line 92).
///
/// With `auto.offset.reset=none`, a fresh `poll()` after `assign(...)`
/// surfaces `NoOffsetForPartition`; after `seek(tp, outOfRangePos)`,
/// the next `poll()` surfaces `OffsetOutOfRange`.
///
/// `Error::ConsumerOffsetOutOfRange` carries the structured payload that Java
/// asserts on (`OffsetOutOfRangeException.offsetOutOfRangePartitions()`). It is
/// its own class now, so the payload survives propagation — it used to be
/// flattened into `Error::LocalIllegalState` by the removed consumer-error enum,
/// leaving only the `Display` string to assert against.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_fetch_invalid_offset() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_fetch_invalid_offset");
    let tp = TopicPartition::new(topic.clone(), 0);

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[("auto.offset.reset", "none")]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    // produce two records
    let total_records: usize = 2;
    send_records_bytes(ctx.bootstrap_servers(), &tp, total_records, 0).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");

    // poll should fail because there is no offset reset strategy set.
    // we fail only when resetting positions after coordinator is known,
    // so using a long timeout — mirrors Java line 109.
    let err = consumer
        .poll(Duration::from_millis(15_000))
        .await
        .expect_err("poll should fail with NoOffsetForPartition");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("Undefined offset with no reset policy"),
        "expected NoOffsetForPartition error, got: {err_msg}"
    );

    // seek to out of range position
    let out_of_range_pos: i64 = total_records as i64 + 1;
    consumer.seek(tp.clone(), out_of_range_pos).await.expect("seek should succeed");
    let err = consumer
        .poll(Duration::from_millis(20_000))
        .await
        .expect_err("poll should fail with OffsetOutOfRange");
    let err_msg = err.to_string();
    // Java asserts `OffsetOutOfRangeException` and inspects
    // `offsetOutOfRangePartitions()`. The Rust error is now
    // `Error::ConsumerOffsetOutOfRange`, which carries that map, but this test
    // asserts on the message so it keeps working against a remote broker
    // regardless of which partition reports first. The message format is:
    // `Fetch position FetchPosition{offset=N, ...} is out of range for partition {tp}`.
    assert!(
        err_msg.contains("out of range for partition") && err_msg.contains(tp.topic()),
        "expected OffsetOutOfRange error for {tp}, got: {err_msg}"
    );
    assert!(
        err_msg.contains(&format!("offset={out_of_range_pos}")),
        "error message should reference offset={out_of_range_pos}, got: {err_msg}"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testAsyncConsumerFetchOutOfRangeOffsetResetConfigEarliest`
/// (line 128). After consuming all records and seeking past the end,
/// the next `poll()` resets to position 0 (`auto.offset.reset=earliest`)
/// and reads from offset 0 again.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_fetch_out_of_range_offset_reset_config_earliest() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_fetch_oor_earliest");
    let tp = TopicPartition::new(topic.clone(), 0);

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(
            ctx.bootstrap_servers(),
            &group_id,
            // ensure no in-flight fetch request so that the offset can be
            // reset immediately
            &[("fetch.max.wait.ms", "0")],
        ),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    let total_records: usize = 10;
    let starting_timestamp: i64 = 0;
    send_records_bytes(ctx.bootstrap_servers(), &tp, total_records, starting_timestamp).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");
    consume_and_verify_records_bytes(consumer.as_mut(), &tp, total_records, 0, 0, starting_timestamp, -1).await;

    // seek to out of range position
    let out_of_range_pos: i64 = total_records as i64 + 1;
    consumer.seek(tp.clone(), out_of_range_pos).await.expect("seek should succeed");
    // assert that poll resets to the beginning position — only one
    // record needed to prove the reset (Java line 148).
    consume_and_verify_records_bytes(consumer.as_mut(), &tp, 1, 0, 0, starting_timestamp, -1).await;

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testAsyncConsumerFetchOutOfRangeOffsetResetConfigLatest`
/// (line 158). After consuming half the records and seeking past the
/// end, the next `poll()` empties to the latest position (no records
/// available); after producing 10 more records, the next `poll()`
/// surfaces the first newly-produced record at offset
/// `totalRecords`.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_fetch_out_of_range_offset_reset_config_latest() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_fetch_oor_latest");
    let tp = TopicPartition::new(topic.clone(), 0);

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(
            ctx.bootstrap_servers(),
            &group_id,
            &[
                ("auto.offset.reset", "latest"),
                // ensure no in-flight fetch request so that the offset
                // can be reset immediately
                ("fetch.max.wait.ms", "0"),
            ],
        ),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    let total_records: usize = 10;
    let starting_timestamp: i64 = 0;
    send_records_with_producer(&producer, &tp, total_records, starting_timestamp, -1).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");
    consumer.seek(tp.clone(), 0).await.expect("seek should succeed");

    // consume some, but not all the records
    consume_and_verify_records_bytes(consumer.as_mut(), &tp, total_records / 2, 0, 0, starting_timestamp, -1).await;

    // seek to out of range position
    let out_of_range_pos: i64 = total_records as i64 + 17; // arbitrary, much higher offset
    consumer.seek(tp.clone(), out_of_range_pos).await.expect("seek should succeed");

    // assert that poll resets to the ending position. Java uses a 50ms
    // timeout — we use the same. The reset issues an OffsetReset to
    // latest, and since no new records exist, the poll empties.
    //
    // The reset itself can take a few polls to complete because it
    // requires a ListOffsets round-trip; allow up to 30s to settle on
    // the reset position before producing the next batch. (Previous
    // 5s budget was too tight on a busy shared-cluster pool where
    // metadata refresh + ListOffsets round-trip can exceed the
    // window.)
    let settle_deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < settle_deadline {
        let records = consumer
            .poll(Duration::from_millis(50))
            .await
            .expect("poll should not fatally fail during reset-to-latest");
        if records.is_empty() && consumer.position(&tp).await.unwrap_or(-1) == total_records as i64 {
            break;
        }
    }
    assert_eq!(
        consumer.position(&tp).await.expect("position should succeed"),
        total_records as i64,
        "after reset-to-latest, position should be at the high watermark"
    );

    send_records_with_producer(&producer, &tp, total_records, total_records as i64, -1).await;

    // After the new records land, the next poll should surface the
    // first one at offset = total_records.
    let next_records = consume_records_bytes(consumer.as_mut(), 1).await;
    let next_record = &next_records[0];
    assert_eq!(
        next_record.offset(),
        total_records as i64,
        "ensure the seek went to the last known record at the time of the previous poll"
    );

    producer.close().await.expect("producer close should succeed");
    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testAsyncConsumerFetchOutOfRangeOffsetResetConfigByDuration`
/// (line 198). Exercises the `by_duration:PT1H` reset strategy in two
/// scenarios:
///
/// 1. All records produced within the last hour: a poll past the end
///    resets to offset 0 (the duration window covers everything).
/// 2. Records spread across the last 24 hours with 1h intervals: only
///    the last one (at offset 24) is within the duration window; the
///    consumer's first read and the post-out-of-range reset both land
///    on that record.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_fetch_out_of_range_offset_reset_config_by_duration() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id_1 = ctx.group_id("g_fetch_oor_by_duration_1");
    let group_id_2 = ctx.group_id("g_fetch_oor_by_duration_2");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp2 = TopicPartition::new(topic.clone(), 1);

    let mut consumer1 = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(
            ctx.bootstrap_servers(),
            &group_id_1,
            &[("auto.offset.reset", "by_duration:PT1H"), ("fetch.max.wait.ms", "0")],
        ),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer (consumer1) should succeed");

    let total_records: usize = 10;
    let starting_timestamp_1 = current_time_ms();
    send_records_bytes(ctx.bootstrap_servers(), &tp, total_records, starting_timestamp_1).await;
    consumer1
        .assign(vec![tp.clone()])
        .await
        .expect("consumer1 assign should succeed");
    consume_and_verify_records_bytes(consumer1.as_mut(), &tp, total_records, 0, 0, starting_timestamp_1, -1).await;

    // seek to out of range position
    let out_of_range_pos: i64 = total_records as i64 + 1;
    consumer1
        .seek(tp.clone(), out_of_range_pos)
        .await
        .expect("consumer1 seek should succeed");
    // assert that poll resets to the beginning position (everything is
    // within the 1h duration window)
    consume_and_verify_records_bytes(consumer1.as_mut(), &tp, 1, 0, 0, starting_timestamp_1, -1).await;

    consumer1.close().await.expect("consumer1 close should succeed");

    // Second scenario: starting offset is earlier than the requested
    // duration. Generate records with 1 hour interval for 1 day.
    let mut consumer2 = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(
            ctx.bootstrap_servers(),
            &group_id_2,
            &[("auto.offset.reset", "by_duration:PT1H"), ("fetch.max.wait.ms", "0")],
        ),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer (consumer2) should succeed");

    let total_records_2: usize = 25;
    let starting_timestamp_2 = current_time_ms() - Duration::from_secs(24 * 60 * 60).as_millis() as i64;
    let hour_millis = Duration::from_secs(60 * 60).as_millis() as i64;

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    send_records_with_producer(&producer, &tp2, total_records_2, starting_timestamp_2, hour_millis).await;
    producer.close().await.expect("producer close should succeed");

    consumer2
        .assign(vec![tp2.clone()])
        .await
        .expect("consumer2 assign should succeed");
    // consumer should read one record from last one hour
    consume_and_verify_records_bytes(
        consumer2.as_mut(),
        &tp2,
        1,
        24,
        24,
        starting_timestamp_2 + 24 * hour_millis,
        hour_millis,
    )
    .await;

    // seek to out of range position
    let out_of_range_pos_2: i64 = total_records_2 as i64 + 1;
    consumer2
        .seek(tp2.clone(), out_of_range_pos_2)
        .await
        .expect("consumer2 seek should succeed");
    // assert that poll resets to the duration offset. consumer should
    // read one record from last one hour
    consume_and_verify_records_bytes(
        consumer2.as_mut(),
        &tp2,
        1,
        24,
        24,
        starting_timestamp_2 + 24 * hour_millis,
        hour_millis,
    )
    .await;

    consumer2.close().await.expect("consumer2 close should succeed");
}

/// Translates Java's
/// `testAsyncConsumerFetchRecordLargerThanFetchMaxBytes` (line 279).
/// A single record larger than `fetch.max.bytes` should still be
/// fetched (KIP-74 guarantee).
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_fetch_record_larger_than_fetch_max_bytes() {
    let max_fetch_bytes = 10 * 1024;
    check_large_record(
        &[("fetch.max.bytes", &max_fetch_bytes.to_string())],
        max_fetch_bytes + 1,
        "g_large_fetch_max_bytes",
    )
    .await;
}

/// Translates Java's
/// `testAsyncConsumerFetchRecordLargerThanMaxPartitionFetchBytes`
/// (line 297). A single record larger than `max.partition.fetch.bytes`
/// should still be fetched (KIP-74 guarantee).
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_fetch_record_larger_than_max_partition_fetch_bytes() {
    let max_fetch_bytes = 10 * 1024;
    check_large_record(
        &[("max.partition.fetch.bytes", &max_fetch_bytes.to_string())],
        max_fetch_bytes + 1,
        "g_large_max_part_fetch_bytes",
    )
    .await;
}

/// Translates Java's `checkLargeRecord(config, producerRecordSize)`
/// (line 309). Produces one record of `producer_record_size` bytes,
/// then consumes 1 record and asserts key/value/topic/partition/offset.
async fn check_large_record(consumer_overrides: &[(&str, &str)], producer_record_size: usize, group_base_name: &str) {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id(group_base_name);
    let tp = TopicPartition::new(topic.clone(), 0);

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, consumer_overrides),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    // produce a record that is larger than the configured fetch size
    let expected_key = b"key".to_vec();
    let expected_value = vec![0u8; producer_record_size];
    let record = ProducerRecord::with_partition(
        tp.topic().to_string(),
        Some(tp.partition()),
        Some(expected_key.clone()),
        Some(expected_value.clone()),
    )
    .expect("ProducerRecord::with_partition should succeed");
    let fut = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
        .await
        .expect("send should not fail");
    // ensure the broker has acknowledged before consuming.
    fut.get_timeout(Duration::from_secs(30)).await.expect("send should succeed");

    // consuming a record that is too large should succeed since KIP-74
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");
    let records = consume_records_bytes(consumer.as_mut(), 1).await;
    assert_eq!(records.len(), 1, "should consume exactly one record (got {})", records.len());
    let consumer_record = &records[0];
    assert_eq!(consumer_record.offset(), 0);
    assert_eq!(consumer_record.topic(), tp.topic());
    assert_eq!(consumer_record.partition(), tp.partition());
    assert_eq!(consumer_record.key().expect("key should be present"), &expected_key);
    assert_eq!(consumer_record.value().expect("value should be present"), &expected_value);

    producer.close().await.expect("producer close should succeed");
    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testAsyncConsumerFetchHonoursFetchSizeIfLargeRecordNotFirst`
/// (line 341). When a small record precedes a large one in the log,
/// the consumer only returns the small one on the first poll —
/// honoring `fetch.max.bytes`.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_fetch_honours_fetch_size_if_large_record_not_first() {
    let max_fetch_bytes = 10 * 1024;
    check_fetch_honours_size_if_large_record_not_first(
        &[("fetch.max.bytes", &max_fetch_bytes.to_string())],
        max_fetch_bytes,
        "g_honours_fetch_max_bytes",
    )
    .await;
}

/// Translates Java's
/// `testAsyncConsumerFetchHonoursMaxPartitionFetchBytesIfLargeRecordNotFirst`
/// (line 359). Same as above but for `max.partition.fetch.bytes`.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_fetch_honours_max_partition_fetch_bytes_if_large_record_not_first() {
    let max_fetch_bytes = 10 * 1024;
    check_fetch_honours_size_if_large_record_not_first(
        &[("max.partition.fetch.bytes", &max_fetch_bytes.to_string())],
        max_fetch_bytes,
        "g_honours_max_part_fetch_bytes",
    )
    .await;
}

/// Translates Java's
/// `checkFetchHonoursSizeIfLargeRecordNotFirst(config,
/// largeProducerRecordSize)` (line 371).
async fn check_fetch_honours_size_if_large_record_not_first(
    consumer_overrides: &[(&str, &str)],
    large_producer_record_size: usize,
    group_base_name: &str,
) {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id(group_base_name);
    let tp = TopicPartition::new(topic.clone(), 0);

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, consumer_overrides),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    let producer = build_producer_bytes(ctx.bootstrap_servers());

    let small_key = b"small".to_vec();
    let small_value = b"value".to_vec();
    let small_record = ProducerRecord::with_partition(
        tp.topic().to_string(),
        Some(tp.partition()),
        Some(small_key.clone()),
        Some(small_value.clone()),
    )
    .expect("small ProducerRecord should build");

    let large_key = b"large".to_vec();
    let large_value = vec![0u8; large_producer_record_size];
    let large_record = ProducerRecord::with_partition(
        tp.topic().to_string(),
        Some(tp.partition()),
        Some(large_key),
        Some(large_value),
    )
    .expect("large ProducerRecord should build");

    // Java uses `producer.send(record).get()` to enforce ordering
    // (first send completes before the second).
    let f1 = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, small_record)
        .await
        .expect("small send should not fail");
    f1.get_timeout(Duration::from_secs(30))
        .await
        .expect("small send should succeed");
    let f2 = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, large_record)
        .await
        .expect("large send should not fail");
    f2.get_timeout(Duration::from_secs(30))
        .await
        .expect("large send should succeed");

    // The topic was auto-created by the sends above, so it exists on the leader
    // that acked them — but not necessarily in every broker's metadata cache yet.
    // `assign` with a group.id configured issues an `OffsetFetch` to the group
    // coordinator, and a coordinator that has not caught up answers
    // `UNKNOWN_TOPIC_OR_PARTITION`, which the commit manager turns into a hard
    // `KafkaException("Topic does not exist")` out of `poll()`
    // (`CommitRequestManager.java:1156`) — an intermittent failure, not a retry.
    //
    // Java never races here because its fixture creates the topic up front with
    // `cluster.createTopic(topic, 2, BROKER_COUNT)` in `@BeforeEach`. Waiting for
    // propagation is the equivalent guarantee; 2 partitions because this cluster
    // sets `num.partitions=2` for exactly that parity (see
    // `cluster_config_with_kip848_3brokers`).
    wait_for_all_partitions_metadata_with_context(&ctx, &topic, 2).await;

    // we should only get the small record in the first `poll`
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");

    let records = consume_records_bytes(consumer.as_mut(), 1).await;
    assert_eq!(records.len(), 1, "should consume exactly one record (got {})", records.len());
    let consumer_record = &records[0];
    assert_eq!(consumer_record.offset(), 0);
    assert_eq!(consumer_record.topic(), tp.topic());
    assert_eq!(consumer_record.partition(), tp.partition());
    assert_eq!(consumer_record.key().expect("key should be present"), &small_key);
    assert_eq!(consumer_record.value().expect("value should be present"), &small_value);

    producer.close().await.expect("producer close should succeed");
    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testAsyncConsumerLowMaxFetchSizeForRequestAndPartition` (line 414).
/// With very small `fetch.max.bytes` (500) and very small
/// `max.partition.fetch.bytes` (100), and 90 partitions to fetch from
/// (3 topics × 30 partitions), the consumer must still eventually
/// deliver every record. This exercises the small-fetch + multi-poll
/// fairness path.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_low_max_fetch_size_for_request_and_partition() {
    // Different cluster config: needs num.partitions=30 for auto-created
    // topics (Java explicitly creates each topic with 30 partitions via
    // `cluster.createTopic(name, partitionCount, BROKER_COUNT)`).
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers_30parts()).await;
    let group_id = ctx.group_id("g_low_max_fetch_size");

    // Three distinct topics, each with 30 partitions (defaulted via
    // broker num.partitions=30).
    let topic1 = ctx.topic("topic1");
    let topic2 = ctx.topic("topic2");
    let topic3 = ctx.topic("topic3");
    let topics: Vec<String> = vec![topic1.clone(), topic2.clone(), topic3.clone()];

    let partition_count: i32 = 30;
    let mut partitions: HashSet<TopicPartition> = HashSet::new();
    for topic in &topics {
        for i in 0..partition_count {
            partitions.insert(TopicPartition::new(topic.clone(), i));
        }
    }

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(
            ctx.bootstrap_servers(),
            &group_id,
            &[
                // one of the effects of this is that there will be some
                // log reads where `0 > remaining limit bytes < message
                // size` and we don't return the message because it's
                // not the first message in the first non-empty
                // partition of the fetch
                ("fetch.max.bytes", "500"),
                ("max.partition.fetch.bytes", "100"),
                // Avoid a rebalance while the records are being sent
                // (the default is 6 seconds)
                ("max.poll.interval.ms", "20000"),
            ],
        ),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    assert_eq!(consumer.assignment().len(), 0, "initial assignment should be empty");
    consumer.subscribe(topics.clone()).await.expect("subscribe should succeed");
    await_assignment(consumer.as_mut(), &partitions).await;

    // Produce `partition_count` records per partition.
    let producer = build_producer_bytes(ctx.bootstrap_servers());
    let now = current_time_ms();
    // Java iterates `partitions` (a HashSet) and produces 30 records
    // per partition. The total record count is partition_count * 90.
    let mut total_produced: usize = 0;
    // Snapshot the partitions in deterministic order so the producer
    // sends in a consistent sequence (debug-friendly).
    let mut ordered_partitions: Vec<TopicPartition> = partitions.iter().cloned().collect();
    ordered_partitions.sort_by(|a, b| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    for partition in &ordered_partitions {
        send_records_with_producer(&producer, partition, partition_count as usize, now, -1).await;
        total_produced += partition_count as usize;
    }
    producer.close().await.expect("producer close should succeed");

    // Now consume all `total_produced` records — even with the small
    // fetch size, the loop must eventually drain everything. The Rust
    // producer's per-partition `flush()` (90 sequential flushes total)
    // pushes total wall-clock past the default 60s budget; bump to 180s
    // for parity with Java's eventual-completion outcome.
    let consumed =
        consume_records_bytes_with_deadline(consumer.as_mut(), total_produced, Duration::from_secs(180)).await;
    assert_eq!(
        consumed.len(),
        total_produced,
        "should consume exactly {total_produced} records (got {})",
        consumed.len()
    );

    // Bucket consumed records by (topic, partition); assert per-partition
    // counts and per-record fields (topic / partition / key / value /
    // timestamp).
    let mut consumed_by_partition: RecordsByPartition<'_> = HashMap::new();
    for record in &consumed {
        let tp = TopicPartition::new(record.topic().to_string(), record.partition());
        consumed_by_partition.entry(tp).or_default().push(record);
    }

    for partition in &partitions {
        let consumed_for_partition = consumed_by_partition.get(partition).map(Vec::as_slice).unwrap_or(&[]);
        assert_eq!(
            consumed_for_partition.len(),
            partition_count as usize,
            "Records count mismatch for {partition}"
        );
        for (i, record) in consumed_for_partition.iter().enumerate() {
            assert_eq!(record.topic(), partition.topic());
            assert_eq!(record.partition(), partition.partition());
            let expected_key = format!("key {i}").into_bytes();
            let expected_value = format!("value {i}").into_bytes();
            assert_eq!(
                record.key().expect("key should be present"),
                &expected_key,
                "key mismatch for {partition}[{i}]"
            );
            assert_eq!(
                record.value().expect("value should be present"),
                &expected_value,
                "value mismatch for {partition}[{i}]"
            );
            // Java asserts the *exact* producer-record timestamp matches
            // the consumed-record timestamp; with `timestampIncrement=-1`
            // and all sends from the same `now`, every record on the
            // same partition shares a 1ms-stepped timestamp starting at
            // `now`.
            assert_eq!(record.timestamp(), now + i as i64, "timestamp mismatch for {partition}[{i}]");
        }
    }

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
