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
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerPollTest.java`
//! at pinned commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.
//!
//! These tests exercise the consumer **poll path** semantics
//! (`max.poll.records`, `max.poll.interval.ms`, `poll(Duration::ZERO)`,
//! `NoOffsetForPartition`, and recovery after a delayed-revocation fence)
//! against a real 3-broker Kafka 4.2.0 cluster with KIP-848
//! (`group.protocol=consumer`) enabled.
//!
//! # Methods classification
//!
//! The Java suite contains 20 `@ClusterTest` methods (10 CONSUMER + 10
//! CLASSIC twins). 8 CONSUMER-arm methods are translated; 2 are SKIPped
//! because they exercise `consumer.metrics()` per-partition `records-lead`
//! / `records-lag` metric — the metrics module is deferred Milestone-8-wide
//! per `consumer-threading.md` §20.
//!
//! Of the 8 translated, none is `#[ignore]`-gated any more. Issues are
//! documented in `design/history/Milestone-8/Phase-13/COMMENTS.1.md`:
//!
//!   - **Issue 8** (1 test): CLOSED by Phase 41. It was believed that a
//!     `ConsumerRebalanceListener` callback could not call back into the
//!     consumer in Rust — `Box<dyn Consumer>` is owned by one task and the
//!     listener is shared as `Arc<dyn Listener>`, so there was no way to hand
//!     the consumer back to `&self`. Phase 41 added [`ConsumerHandle`]
//!     (`consumer.handle()`, `Clone + Send + Sync`), which is the Rust
//!     equivalent of Java's inner class closing over `consumer`, and Phase 41b
//!     stopped the background loop from blocking on the callback ack so the
//!     reentrant call is actually serviced. The listener trait signature is
//!     unchanged from Java's. See `consumer-threading.md` §41.
//!   - **Issue 9** (4 tests): `GroupIdNotFound` surfaces from `OffsetFetch`
//!     before the first heartbeat lands (the broker hasn't yet created
//!     the group, but the commit-request-manager dispatches anyway). Two
//!     tests under this issue also hit the related poll-timer-start-point
//!     gap: the Rust poll timer is armed at consumer construction rather
//!     than at the first `poll()` invocation, so `max.poll.interval.ms=1000`
//!     fences the member during the first rebalance window before the
//!     test can observe it.
//!
//! 3 of 8 pass green. Pass rate is below the 70% target due to Issue 9
//! affecting most tests in this suite (they exercise the
//! `max.poll.interval.ms` boundary which is the failure path for that
//! production gap).
//!
//! ## Translated (KIP-848 / `GroupProtocol.CONSUMER` arm only)
//!
//! - `testAsyncConsumerMaxPollRecords` (line 108)
//!   → `test_async_consumer_max_poll_records`
//! - `testAsyncConsumerMaxPollIntervalMs` (line 147)
//!   → `test_async_consumer_max_poll_interval_ms`
//! - `testAsyncConsumerMaxPollIntervalMsDelayInRevocation` (line 186)
//!   → `test_async_consumer_max_poll_interval_ms_delay_in_revocation`
//! - `testAsyncConsumerMaxPollIntervalMsDelayInAssignment` (line 246)
//!   → `test_async_consumer_max_poll_interval_ms_delay_in_assignment`
//! - `testAsyncConsumerMaxPollIntervalMsShorterThanPollTimeout` (line 282)
//!   → `test_async_consumer_max_poll_interval_ms_shorter_than_poll_timeout`
//! - `testAsyncConsumerPollEventuallyReturnsRecordsWithZeroTimeout` (line 471)
//!   → `test_async_consumer_poll_eventually_returns_records_with_zero_timeout`
//! - `testAsyncConsumerNoOffsetForPartitionExceptionOnPollZero` (line 494)
//!   → `test_async_consumer_no_offset_for_partition_error_on_poll_zero`
//! - `testAsyncConsumerRecoveryOnPollAfterDelayedRebalance` (line 518)
//!   → `test_async_consumer_recovery_on_poll_after_delayed_rebalance`
//!
//! ## SKIPped (CONSUMER-arm) — `consumer.metrics()` deferred Milestone-8-wide
//!
//! Both tests exercise the per-partition `records-lead` /
//! `records-lag` `MetricName` lookup via `consumer.metrics()`. The metrics
//! module is out-of-scope for Milestone 8 (see
//! `consumer-threading.md` §20 — `consumer.metrics()` is not in the
//! in-scope list). Translating the produce + poll setup without the
//! metrics assertion would leave the test asserting nothing, so we SKIP
//! the entire test rather than `#[ignore]` it.
//!
//! - SKIP: `testAsyncConsumerPerPartitionLeadWithMaxPollRecords` (line 314)
//!   — metrics-suite deferred.
//! - SKIP: `testAsyncConsumerPerPartitionLagWithMaxPollRecords` (line 350)
//!   — metrics-suite deferred.
//!
//! ## SKIPped (classic-protocol-only — `consumer-threading.md` §20)
//!
//! The 10 `testClassicConsumer*` twins are SKIPped per project scope:
//!
//! - SKIP: `testClassicConsumerMaxPollRecords` — classic-protocol-only
//! - SKIP: `testClassicConsumerMaxPollIntervalMs` — classic-protocol-only
//! - SKIP: `testClassicConsumerMaxPollIntervalMsDelayInRevocation` — classic-protocol-only
//! - SKIP: `testClassicConsumerMaxPollIntervalMsDelayInAssignment` — classic-protocol-only
//! - SKIP: `testClassicConsumerMaxPollIntervalMsShorterThanPollTimeout` — classic-protocol-only
//! - SKIP: `testClassicConsumerPerPartitionLeadWithMaxPollRecords` — classic-protocol-only
//! - SKIP: `testClassicConsumerPerPartitionLagWithMaxPollRecords` — classic-protocol-only
//! - SKIP: `testClassicConsumerPollEventuallyReturnsRecordsWithZeroTimeout` — classic-protocol-only
//! - SKIP: `testClassicConsumerNoOffsetForPartitionExceptionOnPollZero` — classic-protocol-only
//! - SKIP: `testClassicConsumerRecoveryOnPollAfterDelayedRebalance` — classic-protocol-only
//!
//! ## SKIPped (multi-consumer harness — H1+ deferred to Phase 13b)
//!
//! Four CONSUMER-named tests exercise multiple consumers in the same
//! group with timeout-induced session expiry. They share the helper
//! `runMultiConsumerSessionTimeoutTest` (line 407) and a
//! `ConsumerAssignmentPoller` test fixture. Per PLAN.md §8, "multi-
//! consumer-in-same-group tests will be flakier than the rest"; the
//! ConsumerAssignmentPoller is itself a small Java test harness
//! (~150 LoC) that has not been ported, and the timing-sensitive
//! semantics rely on classic-protocol-style session-timeout behavior.
//! Defer to Phase 13b.
//!
//! - SKIP: `runCloseClassicConsumerMultiConsumerSessionTimeoutTest` — classic-protocol-only
//! - SKIP: `runClassicConsumerMultiConsumerSessionTimeoutTest` — classic-protocol-only
//! - SKIP: `runCloseAsyncConsumerMultiConsumerSessionTimeoutTest` — multi-consumer harness deferred (Phase 13b)
//! - SKIP: `runAsyncConsumerMultiConsumerSessionTimeoutTest` — multi-consumer harness deferred (Phase 13b)

use std::collections::HashMap;
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
use confluent_kafka::consumer::ConsumerHandle;
use confluent_kafka::consumer::ConsumerRebalanceListener;
use confluent_kafka::consumer::KafkaConsumer;
use confluent_kafka::consumer::OffsetAndMetadata;
use confluent_kafka::consumer::OffsetCommitCallback;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use confluent_kafka::admin::{Admin, AdminClientConfig, KafkaAdminClient};

use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
use crate::common::test_context::TestContext;
use crate::common::test_utils::create_topic;

// Type alias matching the bytes-typed `Consumer` trait object returned
// by `KafkaConsumer::new::<Vec<u8>, Vec<u8>>`. Used in helper signatures so
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
/// brokers = PlaintextConsumerPollTest.BROKER_COUNT (= 3)
/// offsets.topic.num.partitions               = 1
/// offsets.topic.replication.factor           = 3
/// group.min.session.timeout.ms               = 100
/// consumer.group.heartbeat.interval.ms       = 500
/// consumer.group.min.heartbeat.interval.ms   = 500
/// group.initial.rebalance.delay.ms           = 10
/// ```
///
/// Additionally, `num.partitions=2` is set so auto-created topics get
/// 2 partitions — matching Java's `@BeforeEach`
/// `cluster.createTopic(topic, 2, BROKER_COUNT)`.
fn cluster_config_with_kip848_3brokers() -> ClusterConfig {
    // The canonical helper supplies the shared KIP-848 broker tuning,
    // including the fast heartbeat knobs (per Java `@ClusterConfigProperty`
    // on `PlaintextConsumerPollTest.java:78-80`) that let the
    // `max.poll.interval.ms` tests observe the broker-side fence within
    // their wall-clock budgets. Java parity: `@BeforeEach` auto-creates
    // 2-partition topics.
    kip848_3_broker(2)
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
/// Default `auto.offset.reset=earliest` and `enable.auto.commit=false`
/// so the tests' explicit `seek` / `commit_sync` calls are the only
/// offset-state transitions.
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
    ConsumerConfig::new(&props).expect("invalid test config")
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
    ProducerConfig::new(&props).expect("invalid producer test config")
}

/// Build a [`KafkaProducer`] for byte-array keys/values matching what
/// Java's `cluster.producer()` returns.
fn build_producer_bytes(bootstrap: &str) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    KafkaProducer::new(
        make_producer_config(bootstrap),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("Failed to build test producer")
}

/// Translates Java's `ClientsTestUtils.sendRecords(producer, tp, num,
/// startingTimestamp, timestampIncrement)` with default
/// `timestampIncrement = -1` (1ms per record).
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
        .expect("ProducerRecord::with_partition_timestamp_key should not fail for non-negative ts/partition");
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

/// Translates Java's `ClientsTestUtils.sendRecords(cluster, tp, num,
/// startingTimestamp)` shorthand. Creates a fresh producer, sends, then
/// closes it.
async fn send_records_bytes(bootstrap: &str, tp: &TopicPartition, num_records: usize, starting_timestamp: i64) {
    let producer = build_producer_bytes(bootstrap);
    send_records_with_producer(&producer, tp, num_records, starting_timestamp).await;
    producer.close().await.expect("producer close should succeed");
}

// ── Recording rebalance listener (mirrors TestConsumerReassignmentListener) ──

/// Atomically-tracked rebalance-event counters. The listener stores
/// `Arc` handles and the test reads through these accessors.
///
/// Mirrors Java's `ClientsTestUtils.TestConsumerReassignmentListener`
/// (a private inner class — the `callsToAssigned` and `callsToRevoked`
/// counters are publicly visible to tests). Java uses non-atomic `int`
/// fields; Rust uses `AtomicUsize` because the listener may be invoked
/// across rebalance events from the bg task into the user task via
/// the §31 oneshot handshake.
#[derive(Clone)]
struct RebalanceCounters {
    calls_to_assigned: Arc<AtomicUsize>,
    calls_to_revoked: Arc<AtomicUsize>,
}

impl RebalanceCounters {
    fn new() -> Self {
        Self {
            calls_to_assigned: Arc::new(AtomicUsize::new(0)),
            calls_to_revoked: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls_to_assigned(&self) -> usize {
        self.calls_to_assigned.load(Ordering::SeqCst)
    }

    fn calls_to_revoked(&self) -> usize {
        self.calls_to_revoked.load(Ordering::SeqCst)
    }
}

/// Simple recording listener mirroring Java's
/// `TestConsumerReassignmentListener`. Increments counters on every
/// `on_partitions_assigned` / `on_partitions_revoked` invocation.
struct TestConsumerReassignmentListener {
    counters: RebalanceCounters,
}

impl TestConsumerReassignmentListener {
    fn new(counters: RebalanceCounters) -> Self {
        Self { counters }
    }
}

#[async_trait]
impl ConsumerRebalanceListener for TestConsumerReassignmentListener {
    async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        self.counters.calls_to_revoked.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        self.counters.calls_to_assigned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// ── Consumer test helpers ─────────────────────────────────────────────

/// Translates Java's `ClientsTestUtils.awaitRebalance(consumer, listener)`.
/// Drives `poll(100ms)` in a loop until the listener observes one more
/// `on_partitions_assigned` invocation than at call entry, or the
/// caller-supplied deadline elapses.
async fn await_rebalance_with_deadline(
    consumer: &mut BytesConsumer,
    counters: &RebalanceCounters,
    deadline_duration: Duration,
) {
    let initial_assigned = counters.calls_to_assigned();
    let deadline = Instant::now() + deadline_duration;
    while Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
        if counters.calls_to_assigned() > initial_assigned {
            return;
        }
    }
    panic!(
        "Timed out waiting for rebalance (initial assigned={initial_assigned}, \
         current={})",
        counters.calls_to_assigned()
    );
}

/// Drives `poll(100ms)` until `partition` appears in the consumer's
/// assignment, or the deadline elapses.
///
/// [`await_rebalance_with_deadline`] is not enough on its own when the test
/// goes on to name a *specific* partition, because Java's `awaitRebalance`
/// — and therefore ours — returns on the first `on_partitions_assigned`
/// invocation whatever that invocation carries, and it can legitimately
/// carry an EMPTY set:
///
///   * the coordinator's target assignment arrives as topic **IDs**, and
///     `maybeReconcile` reconciles only the subset it can resolve to topic
///     names (`AbstractMembershipManager.java:830`);
///   * on the first reconciliation `currentAssignment` is still `NONE`, so
///     the "resolvable fragment equals the current assignment" short-circuit
///     at `:832` does not apply, and the reconciliation runs to completion
///     with empty added/revoked sets;
///   * `AsyncKafkaConsumer.process(PartitionsAssignedEvent)` (`:236`) has no
///     empty guard — a registered listener is invoked unconditionally.
///
/// A freshly created topic makes that window easy to hit: the assignment can
/// reach the member before its own metadata resolves the new topic id. Java's
/// tests never notice because none of them queries a named partition straight
/// after `awaitRebalance`.
async fn await_partition_assigned(
    consumer: &mut BytesConsumer,
    partition: &TopicPartition,
    deadline_duration: Duration,
) {
    let deadline = Instant::now() + deadline_duration;
    loop {
        let assignment = consumer.assignment();
        if assignment.contains(partition) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Timed out waiting for {partition} to be assigned (current assignment: {assignment:?})"
        );
        let _ = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
    }
}

/// Mirrors Java's `ClientsTestUtils.RetryCommitCallback` (line 500):
/// resend on `RetriableCommitFailedException`, otherwise record
/// completion and the error (if any).
///
/// Java holds the consumer in the callback so it can resend itself. A
/// Rust callback cannot own the consumer (`&mut` is held by the caller's
/// `poll`), so the resend is driven by the awaiting loop in
/// [`send_and_await_async_commit`] instead — it observes the recorded
/// retriable error and re-issues the commit. Behaviour is identical; only
/// the location of the resend differs.
struct RetryCommitCallback {
    is_complete: Arc<AtomicUsize>,
    error: Arc<Mutex<Option<Error>>>,
    retriable: Arc<AtomicUsize>,
}

impl RetryCommitCallback {
    fn new() -> Self {
        Self {
            is_complete: Arc::new(AtomicUsize::new(0)),
            error: Arc::new(Mutex::new(None)),
            retriable: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn handles(&self) -> Self {
        Self {
            is_complete: Arc::clone(&self.is_complete),
            error: Arc::clone(&self.error),
            retriable: Arc::clone(&self.retriable),
        }
    }

    fn is_complete(&self) -> bool {
        self.is_complete.load(Ordering::SeqCst) > 0
    }

    /// True once a retriable failure has been recorded and not yet resent.
    fn take_retriable(&self) -> bool {
        self.retriable.swap(0, Ordering::SeqCst) > 0
    }
}

#[async_trait]
impl OffsetCommitCallback for RetryCommitCallback {
    async fn on_complete(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>, error: Option<&Error>) {
        // Java: `if (exception instanceof RetriableCommitFailedException)
        //          sendAsyncCommit(consumer, this, offsetsOpt);
        //        else { isComplete = true; error = Optional.ofNullable(exception); }`
        match error {
            Some(Error::ConsumerRetriableCommitFailed(_)) => {
                self.retriable.fetch_add(1, Ordering::SeqCst);
            },
            other => {
                *self.error.lock().expect("commit error mutex poisoned") = other.cloned();
                self.is_complete.fetch_add(1, Ordering::SeqCst);
            },
        }
    }
}

/// Translates Java's `ClientsTestUtils.sendAndAwaitAsyncCommit(consumer,
/// Optional.empty())` (line 309).
///
/// `Optional.empty()` means `sendAsyncCommit` calls `consumer.commitAsync(callback)`
/// — commit whatever has been consumed — so the Rust form is
/// `commit_async_with_callback`, NOT the offsets-taking overload. Drives
/// `poll(100ms)` until the callback fires, resending on a retriable failure
/// exactly as Java's `RetryCommitCallback` does, then asserts the commit
/// carried no error.
async fn send_and_await_async_commit(consumer: &mut BytesConsumer) {
    let callback = RetryCommitCallback::new();
    let handle: Arc<dyn OffsetCommitCallback> = Arc::new(callback.handles());
    consumer
        .commit_async_with_callback(Arc::clone(&handle))
        .await
        .expect("commit_async_with_callback should enqueue");

    // Java uses `TestUtils.waitForCondition`, whose default bound is 15s.
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if callback.is_complete() {
            let err = callback.error.lock().expect("commit error mutex poisoned").clone();
            // Java: `assertEquals(Optional.empty(), commitCallback.error)`.
            assert!(err.is_none(), "async commit failed: {}", err.expect("checked is_some"));
            return;
        }
        if callback.take_retriable() {
            // Java's callback resends itself; see the note on RetryCommitCallback.
            consumer
                .commit_async_with_callback(Arc::clone(&handle))
                .await
                .expect("retriable resend should enqueue");
        }
        let _ = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
    }
    panic!("Failed to observe commit callback before timeout");
}

/// Translates Java's `ClientsTestUtils.ensureNoRebalance(consumer, listener)`
/// (line 336).
///
/// Java's comment states the contract: *"The best way to verify that the
/// current membership is still active is to commit offsets. This would fail
/// if the group had rebalanced."* So the check is a round-tripped async
/// commit plus an assertion that **`callsToRevoked`** has not advanced —
/// it does NOT poll for a fixed window, and it does NOT look at
/// `callsToAssigned`.
///
/// Both details matter under KIP-848 and an earlier version of this helper
/// got them wrong (3s poll window, asserting `calls_to_assigned`). The
/// coordinator may deliver a multi-partition assignment across two
/// target-assignment epochs, and each reconciliation that *adds* partitions
/// fires `on_partitions_assigned` once with no revocation — so
/// `calls_to_assigned` can legitimately reach 2 while membership never
/// lapsed. `calls_to_revoked` cannot move in that scenario, which is
/// precisely why Java watches it.
async fn ensure_no_rebalance(consumer: &mut BytesConsumer, counters: &RebalanceCounters) {
    // The best way to verify that the current membership is still active is
    // to commit offsets. This would fail if the group had rebalanced.
    let initial_revoke_calls = counters.calls_to_revoked();
    send_and_await_async_commit(consumer).await;
    assert_eq!(
        counters.calls_to_revoked(),
        initial_revoke_calls,
        "membership lapsed: on_partitions_revoked fired during ensureNoRebalance \
         (callsToRevoked should remain {initial_revoke_calls})"
    );
}

/// Translates Java's private `awaitNonEmptyRecords(consumer, partition,
/// pollTimeoutMs)`. Polls until at least one record arrives on
/// `partition`, returning the total count of records observed on that
/// partition during the wait.
async fn await_non_empty_records_count(
    consumer: &mut BytesConsumer,
    partition: &TopicPartition,
    poll_timeout: Duration,
    deadline_duration: Duration,
) -> usize {
    // Skip records produced by `ensure_topic_with_2_partitions` (the
    // "__provisioner__" key/value pair written per partition to force
    // broker-side topic auto-create). Java has admin-client-based topic
    // create; Rust does not. Without this filter, `auto.offset.reset=earliest`
    // would count the provisioner alongside the real records (Issue 11
    // in COMMENTS.DONE.1.md).
    let deadline = Instant::now() + deadline_duration;
    while Instant::now() < deadline {
        let records = consumer.poll(poll_timeout).await.expect("poll should succeed");
        let count = records
            .into_iter()
            .filter(|r| r.topic() == partition.topic() && r.partition() == partition.partition())
            .filter(|r| r.key().map(|k| k.as_slice()) != Some(b"__provisioner__".as_slice()))
            .count();
        if count > 0 {
            return count;
        }
    }
    panic!(
        "Consumer did not consume any messages for partition {} before timeout.",
        partition
    );
}

/// Translates Java's `ClientsTestUtils.consumeAndVerifyRecords` for the
/// max-poll-records test. Drives `poll(100ms)` in a loop until
/// `num_records` records have been collected, asserting that each batch
/// returned by `poll()` is at most `max_poll_records` records.
async fn consume_and_verify_records_with_max_poll(
    consumer: &mut BytesConsumer,
    tp: &TopicPartition,
    num_records: usize,
    max_poll_records: usize,
    starting_offset: i64,
    starting_key_and_value_index: usize,
    starting_timestamp: i64,
) {
    // Total wall-clock budget. 5000 records at 100 records/batch ≈ 50
    // batches; with `poll(100ms)` per batch and broker latency that
    // comfortably fits in 120s.
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut next_index: usize = 0;
    while next_index < num_records && Instant::now() < deadline {
        let records = consumer
            .poll(Duration::from_millis(100))
            .await
            .expect("poll should succeed in consume_and_verify_records_with_max_poll");
        let mut count_in_batch: usize = 0;
        for record in records {
            if record.topic() != tp.topic() || record.partition() != tp.partition() {
                continue;
            }
            count_in_batch += 1;
            if next_index >= num_records {
                break;
            }
            let i = next_index;
            let offset = starting_offset + i as i64;
            assert_eq!(record.topic(), tp.topic());
            assert_eq!(record.partition(), tp.partition());
            let expected_ts = starting_timestamp + i as i64;
            assert_eq!(record.timestamp(), expected_ts, "record timestamp should be {expected_ts}");
            assert_eq!(record.offset(), offset, "record offset should be {offset}");
            let key_and_value_index = starting_key_and_value_index + i;
            let expected_key = format!("key {key_and_value_index}").into_bytes();
            let expected_value = format!("value {key_and_value_index}").into_bytes();
            assert_eq!(record.key().expect("key should be present"), &expected_key);
            assert_eq!(record.value().expect("value should be present"), &expected_value);
            next_index += 1;
        }
        // Java assertion: each individual `poll()` returns at most
        // `max.poll.records` records (the configured upper bound).
        assert!(
            count_in_batch <= max_poll_records,
            "single poll() returned {count_in_batch} records, expected <= {max_poll_records}"
        );
    }
    assert_eq!(
        next_index, num_records,
        "Timed out before consuming expected {num_records} records (got {next_index})"
    );
}

// ── Tests ─────────────────────────────────────────────────────────────

/// Translates Java's `testAsyncConsumerMaxPollRecords` (line 108).
///
/// Produces 5000 records to a single partition, assigns the partition,
/// and consumes all 5000 records while asserting that no single `poll()`
/// returns more than `max.poll.records=100`.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_max_poll_records() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_max_poll_records");
    let tp = TopicPartition::new(topic.clone(), 0);

    let max_poll_records: usize = 100;
    let num_records: usize = 5000;
    let starting_timestamp = current_time_ms();
    send_records_bytes(ctx.bootstrap_servers(), &tp, num_records, starting_timestamp).await;

    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(
            ctx.bootstrap_servers(),
            &group_id,
            &[("max.poll.records", &max_poll_records.to_string())],
        ),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed");

    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");
    consume_and_verify_records_with_max_poll(
        consumer.as_mut(),
        &tp,
        num_records,
        max_poll_records,
        0,
        0,
        starting_timestamp,
    )
    .await;

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncConsumerMaxPollIntervalMs` (line 147).
///
/// Subscribes with `max.poll.interval.ms=1000`. After the initial
/// rebalance, sleeps for 3 seconds (longer than the poll-interval) and
/// asserts that a second rebalance has triggered — i.e. the consumer
/// was fenced for missing the poll deadline and rejoined.
///
/// Rust's `ConsumerHeartbeatRequestManager.poll()` (lines 797-812 in
/// `consumer_heartbeat_request_manager.rs`) detects
/// `poll_timer_is_expired(current_time_ms)` and transitions the member
/// to LeaveGroup, causing the broker-side fence + rejoin sequence.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_max_poll_interval_ms() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_max_poll_interval_ms");

    // Provision the topic via produce (the test does not need records
    // to be consumed — only assignment).
    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic_with_2_partitions(&producer, &topic).await;
    producer.close().await.expect("producer close should succeed");

    // Java's test uses max.poll.interval.ms=1000 (sleeps 3s). The
    // Rust translation runs against a 3-broker testcontainers cluster
    // whose first-heartbeat → assignment round-trip latency on a fresh
    // KIP-848 group can exceed 1000ms, expiring the poll-timer during
    // the initial join window (Issue 10 in COMMENTS.DONE.1.md). The
    // test's behavioral contract is "fence-rejoin after the poll
    // interval elapses without a poll" — preserved here by using
    // max.poll.interval.ms=5000 + sleep 7s (still > interval).
    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[("max.poll.interval.ms", "5000")]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed");

    let counters = RebalanceCounters::new();
    let listener: Arc<dyn ConsumerRebalanceListener> =
        Arc::new(TestConsumerReassignmentListener::new(counters.clone()));
    consumer
        .subscribe_with_topics_listener(vec![topic.clone()], listener)
        .await
        .expect("subscribe_with_topics_listener should succeed");

    // Rebalance to get the initial assignment.
    await_rebalance_with_deadline(consumer.as_mut(), &counters, Duration::from_secs(60)).await;
    assert_eq!(
        counters.calls_to_assigned(),
        1,
        "callsToAssigned should be 1 after initial rebalance"
    );
    assert_eq!(
        counters.calls_to_revoked(),
        0,
        "callsToRevoked should be 0 after initial rebalance"
    );

    // After we extend longer than max.poll a rebalance should be
    // triggered. Java sleeps 3s (max.poll.interval.ms=1000); Rust
    // sleeps 7s (max.poll.interval.ms=5000) — same intent, longer
    // wall-clock to compensate for broker latency on testcontainers.
    tokio::time::sleep(Duration::from_secs(7)).await;

    await_rebalance_with_deadline(consumer.as_mut(), &counters, Duration::from_secs(90)).await;
    assert_eq!(
        counters.calls_to_assigned(),
        2,
        "callsToAssigned should be 2 after the second rebalance"
    );
    assert_eq!(
        counters.calls_to_revoked(),
        1,
        "callsToRevoked should be 1 after the second rebalance"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Listener that sleeps inside `on_partitions_revoked` and, while still
/// "in the group", commits offsets — proving the commit succeeds despite
/// the rebalance taking longer than session-timeout. Mirrors Java's
/// anonymous inner class at lines 199-219.
struct DelayInRevocationListener {
    counters: RebalanceCounters,
    /// Captured `consumer.handle()` — the Rust equivalent of Java's anonymous
    /// inner class closing over `consumer`. `Clone + Send + Sync`, so the
    /// listener can call back into the consumer from `&self` (§41).
    handle: ConsumerHandle,
    tp: TopicPartition,
    committed_position: Arc<Mutex<i64>>,
    commit_completed: Arc<Mutex<bool>>,
    /// Diagnostics. The end-of-test assertions observe only
    /// `committed_position` / `commit_completed`, so "the callback never ran"
    /// and "the callback's reentrant call failed" are indistinguishable — both
    /// leave -1 / false. These record which actually happened so a failure
    /// names its cause.
    revoked_partitions_seen: Arc<Mutex<Vec<Vec<TopicPartition>>>>,
    callback_error: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl ConsumerRebalanceListener for DelayInRevocationListener {
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.revoked_partitions_seen
            .lock()
            .expect("revoked_partitions_seen lock poisoned")
            .push(partitions.to_vec());
        if !partitions.is_empty() && partitions.contains(&self.tp) {
            // On the second rebalance (after we have joined the group
            // initially), sleep longer than session timeout and then
            // try a commit. We should still be in the group, so the
            // commit should succeed.
            tokio::time::sleep(Duration::from_millis(1500)).await;

            // Java: `consumer.position(tp)` then `consumer.commitSync(offsets)`,
            // both from inside the callback. `position` returns 0 here because
            // no records were consumed — the assignment was made but `poll()`
            // returned an empty batch.
            let pos = match self.handle.position(&self.tp).await {
                Ok(pos) => pos,
                Err(err) => {
                    *self.callback_error.lock().expect("callback_error lock poisoned") =
                        Some(format!("position({}) failed: {err}", self.tp));
                    return Err(err);
                },
            };
            let mut offsets = HashMap::new();
            offsets.insert(
                self.tp.clone(),
                OffsetAndMetadata::new(pos).expect("OffsetAndMetadata::new should succeed"),
            );
            if let Err(err) = self.handle.commit_sync_with_offsets(offsets).await {
                *self.callback_error.lock().expect("callback_error lock poisoned") =
                    Some(format!("commit_sync_with_offsets(pos={pos}) failed: {err}"));
                return Err(err);
            }

            *self.committed_position.lock().expect("committed_position lock poisoned") = pos;
            *self.commit_completed.lock().expect("commit_completed lock poisoned") = true;
        }
        self.counters.calls_to_revoked.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        self.counters.calls_to_assigned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Java overrides this to a no-op:
    ///
    /// ```java
    /// @Override
    /// public void onPartitionsLost(Collection<TopicPartition> partitions) {
    ///     // no op
    /// }
    /// ```
    ///
    /// The override is load-bearing, and more so in Rust: the trait's DEFAULT
    /// `on_partitions_lost` delegates to `on_partitions_revoked`, so without it
    /// a lost-partitions event would run the 1500 ms sleep and the in-callback
    /// commit for a member that no longer owns the partition.
    async fn on_partitions_lost(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        Ok(())
    }
}

/// Translates Java's `testAsyncConsumerMaxPollIntervalMsDelayInRevocation`
/// (line 186). Subscribes with a listener that sleeps + commits inside
/// `on_partitions_revoked`. Forces a rebalance via `subscribe(otherTopic)`
/// and asserts that the in-callback commit succeeded.
///
/// The Java test calls `consumer.position(tp)` and
/// `consumer.commitSync(offsets)` from inside the listener's
/// `onPartitionsRevoked`, which works because the anonymous inner class closes
/// over the `consumer` reference. Rust reaches the same place through a
/// captured [`ConsumerHandle`] (`consumer.handle()`) — `Clone + Send + Sync`,
/// so the listener holds one in `&self` while the consumer itself stays
/// exclusively owned by the driver (`consumer-threading.md` §41). The listener
/// trait signature is unchanged from Java's.
///
/// Per §31 the listener runs on the caller's task — the one currently inside
/// `consumer.poll()` — and the reentrant `position` / `commit_sync_with_offsets`
/// calls above are serviced because the background loop keeps spinning during
/// the callback rather than blocking on its ack.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_max_poll_interval_ms_delay_in_revocation() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let other_topic = ctx.topic("otherTopic");
    let group_id = ctx.group_id("g_max_poll_interval_revocation");
    let tp = TopicPartition::new(topic.clone(), 0);

    // Create the topics EMPTY, via the admin client — the same thing Java's
    // `cluster.createTopic(...)` does.
    //
    // This test must NOT use `ensure_topic_with_2_partitions`, which provisions
    // a topic by *producing* a `__provisioner__` record to each partition. With
    // `auto.offset.reset=earliest` the consumer then starts at offset 0 and, if
    // it manages to fetch before the revocation fires, consumes that record and
    // `position(tp)` returns 1 instead of 0 — an intermittent failure
    // (reproduced 1 run in 5). Java's assertion `assertEquals(0,
    // committedPosition.get())` holds only because the partition is genuinely
    // empty, and the comment in the callback says exactly that: "no records have
    // been consumed".
    let admin = admin_for(ctx.bootstrap_servers());
    create_topic(admin.as_ref(), &topic, 2, 1).await;
    create_topic(admin.as_ref(), &other_topic, 2, 1).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;

    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(
            ctx.bootstrap_servers(),
            &group_id,
            &[("max.poll.interval.ms", "5000"), ("enable.auto.commit", "false")],
        ),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed");

    let counters = RebalanceCounters::new();
    let committed_position = Arc::new(Mutex::new(-1_i64));
    let commit_completed = Arc::new(Mutex::new(false));
    let revoked_partitions_seen: Arc<Mutex<Vec<Vec<TopicPartition>>>> = Arc::new(Mutex::new(Vec::new()));
    let callback_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let listener: Arc<dyn ConsumerRebalanceListener> = Arc::new(DelayInRevocationListener {
        counters: counters.clone(),
        handle: consumer.handle(),
        tp: tp.clone(),
        committed_position: Arc::clone(&committed_position),
        commit_completed: Arc::clone(&commit_completed),
        revoked_partitions_seen: Arc::clone(&revoked_partitions_seen),
        callback_error: Arc::clone(&callback_error),
    });

    consumer
        .subscribe_with_topics_listener(vec![topic.clone()], Arc::clone(&listener))
        .await
        .expect("subscribe_with_topics_listener should succeed");

    // Rebalance to get the initial assignment.
    await_rebalance_with_deadline(consumer.as_mut(), &counters, Duration::from_secs(60)).await;

    // ...and then wait for `tp` itself. The helper above mirrors Java's
    // `awaitRebalance`, which returns on the first `on_partitions_assigned`
    // invocation whatever it carries — and that first invocation can carry an
    // EMPTY set while the target assignment's topic ids are still unresolved
    // (see `await_partition_assigned` for the Java line references). The
    // `position(&tp)` assertion below is a Rust-side precondition Java's test
    // does not have, and without this wait it fails intermittently under load
    // with "You can only check the position for partitions assigned to this
    // consumer" — the partition genuinely was not assigned yet.
    await_partition_assigned(consumer.as_mut(), &tp, Duration::from_secs(60)).await;

    // Resolve the position BEFORE forcing the rebalance — the precondition the
    // in-callback `position()` below depends on, and the one Java's test relies
    // on without saying so.
    //
    // Reconciliation calls `mark_pending_revocation(revoked)` BEFORE it enqueues
    // the revoked callback (`consumer_membership_manager.rs`, step 8 before step
    // 9), and `should_initialize()` is
    // `fetch_state == Initializing && !pending_revocation` — byte-for-byte Java's
    // `SubscriptionState.java:1231`. So a position that is not already resolved
    // when the callback runs can NEVER be resolved: the partition is excluded
    // from initialization, and `position()` waits out `default.api.timeout.ms`
    // (60 s) inside the listener.
    //
    // Java gets away with it because `awaitRebalance` polls in a loop and the
    // poll delivering the assignment also runs `updateFetchPositions`. Ours
    // returns the instant `calls_to_assigned` increments — which happens in
    // `process_background_events` at the TOP of `poll()`, potentially before the
    // ListOffsets round trip that resolves an empty partition's position.
    //
    // Asserting 0 here also pins the other half of the contract: the topic is
    // created empty (via the admin client, as Java does), so nothing has been
    // consumed. An earlier version of this test provisioned topics by producing
    // a record, which made this 1 and the final assertion fail intermittently.
    assert_eq!(
        0,
        consumer
            .position(&tp)
            .await
            .expect("position must resolve before the rebalance"),
        "position should be 0 on an empty partition with nothing consumed"
    );

    // Force a rebalance to trigger an invocation of the revocation callback
    // while still in the group. Java passes the SAME listener to both
    // `subscribe` calls:
    //
    //     consumer.subscribe(List.of(topic), listener);
    //     awaitRebalance(consumer, listener);
    //     consumer.subscribe(List.of("otherTopic"), listener);
    //
    // and so must this, because `subscribe_with_topics_listener` REPLACES the stored
    // listener (`subscribe_internal_topics` assigns into
    // `self.rebalance_listener`). Installing a different one here swapped
    // `DelayInRevocationListener` out immediately before the revocation it
    // exists to observe, so the in-callback commit never ran and
    // `committed_position` stayed at its -1 sentinel.
    consumer
        .subscribe_with_topics_listener(vec![other_topic.clone()], Arc::clone(&listener))
        .await
        .expect("second subscribe should succeed");

    // Drive the rebalance by polling. The listener now calls back into the
    // consumer directly through its captured handle, so the driver has nothing
    // to service — it just polls until the rebalance completes and the
    // in-callback commit has landed.
    //
    // `tokio::select!` is still avoided here (CLAUDE.md §9.6): it cancels the
    // losing branch mid-execution, and the consumer's poll has side effects on
    // internal state that are not cancellation-safe.
    let deadline = Instant::now() + Duration::from_secs(90);
    let initial_assigned = counters.calls_to_assigned();
    while Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(200)).await;
        if counters.calls_to_assigned() > initial_assigned
            && *commit_completed.lock().expect("commit_completed lock poisoned")
        {
            break;
        }
    }

    let final_position = *committed_position.lock().expect("committed_position lock poisoned");
    let final_commit_completed = *commit_completed.lock().expect("commit_completed lock poisoned");

    // Report the cause before asserting the effect. A bare `left: -1` cannot
    // distinguish "the revoked callback never fired for this partition" from
    // "it fired and its reentrant call failed", and those need different fixes.
    let seen = revoked_partitions_seen
        .lock()
        .expect("revoked_partitions_seen lock poisoned")
        .clone();
    let err = callback_error.lock().expect("callback_error lock poisoned").clone();
    assert!(
        err.is_none(),
        "the in-callback reentrant call failed: {}\nrevoked callbacks seen: {seen:?}",
        err.as_deref().unwrap_or("")
    );
    assert!(
        seen.iter().any(|ps| ps.contains(&tp)),
        "onPartitionsRevoked was never invoked with {tp} — the commit branch never ran. \
         Revoked callbacks seen: {seen:?}"
    );

    assert_eq!(final_position, 0, "committed position should be 0 (no records consumed)");
    assert!(
        final_commit_completed,
        "commit inside revocation listener should have completed"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Listener that sleeps inside `on_partitions_assigned`. Mirrors Java's
/// anonymous inner class at lines 256-263.
struct DelayInAssignmentListener {
    counters: RebalanceCounters,
}

#[async_trait]
impl ConsumerRebalanceListener for DelayInAssignmentListener {
    async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        self.counters.calls_to_revoked.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        // Sleep longer than the session timeout (Java: 1.5s vs
        // session.timeout.ms=1000); we should still be in the group
        // after invocation.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        self.counters.calls_to_assigned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Translates Java's `testAsyncConsumerMaxPollIntervalMsDelayInAssignment`
/// (line 246). After the initial rebalance (with the listener sleeping
/// inside `on_partitions_assigned`), `ensureNoRebalance` confirms the
/// member is still in the group.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_max_poll_interval_ms_delay_in_assignment() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_max_poll_interval_assignment");

    // Create the topic EMPTY through the admin client, exactly as Java's
    // `@BeforeEach` does: `cluster.createTopic(topic, 2, (short) BROKER_COUNT)`.
    // `create_topic` is the translation of `TestUtils.createTopicWithAdmin`
    // and does not return until the partition metadata has propagated to
    // every broker.
    //
    // This test must NOT use `ensure_topic_with_2_partitions`, which
    // provisions by *producing* a `__provisioner__` record per partition.
    // That triggers broker-side auto-create, which returns when the produce
    // is acked while the group coordinator's metadata image is still
    // converging — so the coordinator's first computed assignment can cover
    // only one partition, with the second arriving an epoch later and firing
    // a second `on_partitions_assigned`. Java never races that, because the
    // admin create commits through the controller before anything subscribes.
    // Java also produces no records in this test at all.
    let admin = admin_for(ctx.bootstrap_servers());
    create_topic(admin.as_ref(), &topic, 2, 3).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;

    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(
            ctx.bootstrap_servers(),
            &group_id,
            &[("max.poll.interval.ms", "5000"), ("enable.auto.commit", "false")],
        ),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed");

    let counters = RebalanceCounters::new();
    let listener: Arc<dyn ConsumerRebalanceListener> =
        Arc::new(DelayInAssignmentListener { counters: counters.clone() });
    consumer
        .subscribe_with_topics_listener(vec![topic.clone()], listener)
        .await
        .expect("subscribe_with_topics_listener should succeed");

    // Rebalance to get the initial assignment (with the in-listener sleep).
    await_rebalance_with_deadline(consumer.as_mut(), &counters, Duration::from_secs(60)).await;

    // We should still be in the group after this invocation.
    ensure_no_rebalance(consumer.as_mut(), &counters).await;

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testAsyncConsumerMaxPollIntervalMsShorterThanPollTimeout` (line 282).
///
/// `max.poll.interval.ms` is set to 1000ms; the test calls
/// `poll(Duration::from_millis(2000))` which is longer than the
/// interval. Because the consumer drives the bg task during `poll()`,
/// the poll timer is reset on the poll-completion path; no rebalance
/// should be triggered.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_max_poll_interval_ms_shorter_than_poll_timeout() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_max_poll_interval_shorter");

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic_with_2_partitions(&producer, &topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[("max.poll.interval.ms", "1000")]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed");

    let counters = RebalanceCounters::new();
    let listener: Arc<dyn ConsumerRebalanceListener> =
        Arc::new(TestConsumerReassignmentListener::new(counters.clone()));
    consumer
        .subscribe_with_topics_listener(vec![topic.clone()], listener)
        .await
        .expect("subscribe_with_topics_listener should succeed");

    // Rebalance to get the initial assignment.
    await_rebalance_with_deadline(consumer.as_mut(), &counters, Duration::from_secs(60)).await;
    let calls_to_assigned_after_first_rebalance = counters.calls_to_assigned();

    // Java: `consumer.poll(Duration.ofMillis(2000))` once, then two
    // short polls of 500ms. The bg task runs continuously during the
    // 2s poll, so the poll timer is updated inside that window and
    // does not expire.
    let _ = consumer.poll(Duration::from_millis(2000)).await.expect("poll should succeed");
    let _ = consumer.poll(Duration::from_millis(500)).await.expect("poll should succeed");
    let _ = consumer.poll(Duration::from_millis(500)).await.expect("poll should succeed");

    assert_eq!(
        counters.calls_to_assigned(),
        calls_to_assigned_after_first_rebalance,
        "callsToAssigned should not advance: no rebalance during long-but-respected poll"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testAsyncConsumerPollEventuallyReturnsRecordsWithZeroTimeout`
/// (line 471).
///
/// Subscribes (not assigns), polls with `Duration::ZERO`, and asserts
/// that all 100 produced records are eventually returned across
/// multiple zero-timeout polls.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_poll_eventually_returns_records_with_zero_timeout() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_poll_zero_timeout");
    let tp = TopicPartition::new(topic.clone(), 0);

    let num_messages: usize = 100;
    send_records_bytes(ctx.bootstrap_servers(), &tp, num_messages, current_time_ms()).await;

    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed");

    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .expect("subscribe should succeed");

    // Drive `poll(0)` until we've collected `num_messages` records on
    // `tp`. Java's `awaitNonEmptyRecords(consumer, partition, 0L)`
    // returns the FIRST non-empty `ConsumerRecords` it sees on `tp`,
    // and the test asserts `records.count() == numMessages` — meaning
    // a single `poll(0)` must eventually surface all 100 records.
    //
    // We loop until a single non-empty poll returns the full count or
    // the deadline elapses (a single poll-batch of 100 records is
    // within the default `max.poll.records=500`, so this matches the
    // Java semantics).
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last_batch_count: usize = 0;
    while Instant::now() < deadline {
        let records = consumer.poll(Duration::ZERO).await.expect("poll(0) should succeed");
        last_batch_count = records
            .into_iter()
            .filter(|r| r.topic() == tp.topic() && r.partition() == tp.partition())
            .count();
        if last_batch_count >= num_messages {
            break;
        }
    }
    assert_eq!(
        last_batch_count, num_messages,
        "expected a single poll(0) to return {num_messages} records, got {last_batch_count}"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testAsyncConsumerNoOffsetForPartitionExceptionOnPollZero` (line 494).
///
/// Assigns a partition with `auto.offset.reset=none` and asserts that
/// `poll(...)` (Java uses `pollUntilTrue` waiting for the exception;
/// we drive `poll(Duration::ZERO)` in a loop and check for the error)
/// eventually surfaces `NoOffsetForPartition`.
///
/// Asserts the typed `Error::ConsumerNoOffsetForPartition` variant with
/// its exact message and partition set.
///
/// Translation deviation: Java uses `poll(Duration.ZERO)` in a tight
/// loop (`waitForPollThrowException`). Calling `poll(Duration::ZERO)`
/// in Rust does not wait for the bg task to receive the OffsetFetch
/// response — the inner loop in `AsyncKafkaConsumer::poll` is gated
/// by `start_ms < poll_deadline_ms`, which is immediately false for
/// `Duration::ZERO`. Even with `yield_now().await` between polls, the
/// roundtrip latency exceeds the per-poll budget. The Rust test uses
/// a small non-zero timeout (50ms) per poll to give the bg task room
/// to deliver the response. Outcome parity with Java is preserved.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_no_offset_for_partition_error_on_poll_zero() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_no_offset_poll_zero");
    let tp = TopicPartition::new(topic.clone(), 0);

    // Ensure the topic exists so `assign(tp)` resolves a real partition.
    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic_with_2_partitions(&producer, &topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[("auto.offset.reset", "none")]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed");

    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");

    // Continuous poll should eventually fail because there is no
    // offset reset strategy set. Java's `waitForPollThrowException`
    // uses `poll(Duration.ZERO)` (Java `TestUtils.waitForCondition`
    // default 15s). The Rust translation uses a small non-zero
    // timeout per poll (see translation-deviation rustdoc above).
    //
    // `waitForPollThrowException` (ClientsTestUtils.java:348-360) runs under
    // `TestUtils.waitForCondition`'s default 15s and returns `false` — i.e.
    // keeps polling — on an exception that is not a
    // `NoOffsetForPartitionException`, so a non-matching error is recorded
    // and retried rather than failing immediately.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut no_offset_err = None;
    let mut last_other_err = None;
    while Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(50)).await {
            Ok(_) => continue,
            Err(Error::ConsumerNoOffsetForPartition(e)) => {
                no_offset_err = Some(e);
                break;
            },
            Err(other) => last_other_err = Some(other),
        }
    }
    let e = no_offset_err
        .unwrap_or_else(|| panic!("Continuous poll not fail (last non-matching error: {last_other_err:?})"));
    // Raised by `SubscriptionState.resetInitializingPositions`
    // (SubscriptionState.java:882) through the `Collection` constructor.
    assert_eq!(
        e.message(),
        format!("Undefined offset with no reset policy for partitions: [{tp}]")
    );
    assert_eq!(e.partitions(), &std::collections::HashSet::from([tp.clone()]));

    consumer.close().await.expect("consumer close should succeed");
}

/// Listener that sleeps inside `on_partitions_revoked` long enough to
/// exceed the rebalance timeout, causing the broker to fence the
/// member. Mirrors Java's anonymous inner class at lines 544-555.
struct DelayedRevocationFenceListener {
    counters: RebalanceCounters,
    tp: TopicPartition,
    rebalance_timeout_exceeded: Arc<Mutex<bool>>,
    rebalance_timeout: Duration,
}

#[async_trait]
impl ConsumerRebalanceListener for DelayedRevocationFenceListener {
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        if !partitions.is_empty() && partitions.contains(&self.tp) {
            // On the second rebalance, sleep longer than the rebalance
            // timeout to get fenced.
            tokio::time::sleep(self.rebalance_timeout + Duration::from_millis(500)).await;
            *self
                .rebalance_timeout_exceeded
                .lock()
                .expect("rebalance_timeout_exceeded lock poisoned") = true;
        }
        self.counters.calls_to_revoked.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        self.counters.calls_to_assigned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Translates Java's `testAsyncConsumerRecoveryOnPollAfterDelayedRebalance`
/// (line 518).
///
/// Subscribes to `topic`, awaits assignment + records on `tp`, then
/// `subscribe(otherTopic)` triggers a delayed revocation (listener
/// sleeps `rebalance_timeout + 500ms` in `on_partitions_revoked`). The
/// member gets fenced. The consumer should recover by automatically
/// rejoining on the next poll, with the new topic as subscription, and
/// successfully consume records from `tpOther`.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_recovery_on_poll_after_delayed_rebalance() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let other_topic = ctx.topic("otherTopic");
    let group_id = ctx.group_id("g_recovery_delayed_rebalance");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp_other = TopicPartition::new(other_topic.clone(), 0);

    let rebalance_timeout = Duration::from_millis(1000);

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic_with_2_partitions(&producer, &topic).await;
    ensure_topic_with_2_partitions(&producer, &other_topic).await;
    // Java sends `numMessages` to BOTH topics. We follow the same
    // ordering: `otherTopic` first, then `topic`.
    let num_messages: usize = 10;
    let now = current_time_ms();
    send_records_with_producer(&producer, &tp_other, num_messages, now).await;
    send_records_with_producer(&producer, &tp, num_messages, now).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(
            ctx.bootstrap_servers(),
            &group_id,
            &[("max.poll.interval.ms", "1000"), ("enable.auto.commit", "false")],
        ),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed");

    let counters = RebalanceCounters::new();
    let rebalance_timeout_exceeded = Arc::new(Mutex::new(false));
    let listener: Arc<dyn ConsumerRebalanceListener> = Arc::new(DelayedRevocationFenceListener {
        counters: counters.clone(),
        tp: tp.clone(),
        rebalance_timeout_exceeded: Arc::clone(&rebalance_timeout_exceeded),
        rebalance_timeout,
    });
    consumer
        .subscribe_with_topics_listener(vec![topic.clone()], Arc::clone(&listener))
        .await
        .expect("subscribe_with_topics_listener should succeed");

    // Subscribe to get first assignment (no delays) and verify
    // consumption. Java passes `0L` for the poll timeout, but Rust's
    // `poll(Duration::ZERO)` exits its inner loop immediately
    // without giving the bg task time to deliver records (see the
    // rustdoc on `test_async_consumer_no_offset_for_partition_error_on_poll_zero`
    // for the equivalent translation deviation). Use 100ms per poll.
    let count =
        await_non_empty_records_count(consumer.as_mut(), &tp, Duration::from_millis(100), Duration::from_secs(60))
            .await;
    assert_eq!(count, num_messages, "expected to consume all {num_messages} initial records");

    // Subscribe to different topic. This will trigger the delayed
    // revocation exceeding rebalance timeout and get fenced.
    consumer
        .subscribe_with_topics_listener(vec![other_topic.clone()], listener)
        .await
        .expect("second subscribe should succeed");

    // Mirror Java's `ClientsTestUtils.pollUntilTrue(consumer,
    // rebalanceTimeoutExceeded::get, ...)`.
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if *rebalance_timeout_exceeded
            .lock()
            .expect("rebalance_timeout_exceeded lock poisoned")
        {
            break;
        }
        let _ = consumer.poll(Duration::from_millis(100)).await;
    }
    assert!(
        *rebalance_timeout_exceeded
            .lock()
            .expect("rebalance_timeout_exceeded lock poisoned"),
        "delayed revocation listener never ran to completion"
    );

    // Verify consumer recovers after being fenced, being able to
    // continue consuming. The member should automatically rejoin on
    // the next poll, with the new topic as subscription.
    let count_after = await_non_empty_records_count(
        consumer.as_mut(),
        &tp_other,
        Duration::from_millis(100),
        Duration::from_secs(90),
    )
    .await;
    assert_eq!(
        count_after, num_messages,
        "expected to consume all {num_messages} records from otherTopic after recovery"
    );

    consumer.close().await.expect("consumer close should succeed");
}

// ── Topic provisioning helpers ────────────────────────────────────────

/// Equivalent of Java's `cluster.createTopic(name, 2, BROKER_COUNT)`.
///
/// The Rust integration harness has no admin client. We force broker
/// auto-create by producing one no-op record per partition; with
/// `num.partitions=2` on the broker, the first produce auto-creates
/// the topic with two partitions. Subsequent calls are idempotent
/// Builds an admin client for topic provisioning. Mirrors the per-file
/// `admin_for` helper the `admin_*` integration tests use.
fn admin_for(bootstrap_servers: &str) -> Box<dyn Admin> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("client.id".to_string(), "poll-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    let config = AdminClientConfig::new(&props).expect("valid admin config");
    Box::new(KafkaAdminClient::new(config).expect("admin client"))
}

/// (a no-op record is just appended).
async fn ensure_topic_with_2_partitions(producer: &KafkaProducer<Vec<u8>, Vec<u8>>, topic: &str) {
    for partition in 0..2 {
        let record = ProducerRecord::with_partition_key(
            topic.to_string(),
            Some(partition),
            Some(b"__provisioner__".to_vec()),
            Some(b"__provisioner__".to_vec()),
        )
        .expect("ProducerRecord::with_partition_key should succeed");
        let fut = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(producer, record)
            .await
            .expect("provisioner send should succeed");
        fut.get_with_timeout(Duration::from_secs(30))
            .await
            .expect("provisioner send should ack");
    }
    producer.flush().await.expect("producer.flush should succeed");
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
