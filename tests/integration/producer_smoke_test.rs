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

//! Phase 8a/8b — PLAINTEXT producer-side smoke test.
//!
//! Spins up a single-broker KRaft Kafka cluster via Testcontainers
//! (shared across tests through `cluster_pool`), pre-creates a topic
//! with 3 partitions via `docker exec kafka-topics`, then drives
//! [`KafkaProducer`] end-to-end:
//!
//! - `producer_smoke_plaintext_1000_records` — 1000 records with
//!   **explicit** partition assignment (record `i` → partition `i %
//!   TOPIC_PARTITIONS`). Asserts every send future resolves with
//!   `RecordMetadata`, the ack partition equals the explicit partition
//!   (KafkaProducer::partition's "explicit-partition honored" branch,
//!   Java `KafkaProducer.java:1014-1024`), per-partition offsets are
//!   strictly monotonic in send order, and all 3 partitions see
//!   traffic. Producer-side only — end-to-end consume fidelity is
//!   Phase 8c.
//! - `producer_smoke_plaintext_auto_partition` — 1000 records with no
//!   partition and no key. Asserts the partition the producer's
//!   partitioner selects at `send()` time equals the partition the
//!   broker echoes back in the [`RecordMetadata`] ack — i.e. no
//!   mangling between `do_send_inner` and ProduceRequest. Uses the
//!   `set_partition_observer` test seam (see `KafkaProducer::
//!   set_partition_observer` rustdoc for design).
//! - `flush_drains_50_records_through_public_api` — Phase-7f
//!   carry-over: 50 records sent via `producer.send(...).await`, then
//!   `producer.flush().await`. Pins the public-API flush path's
//!   fidelity through `Producer::flush` (NOT the accumulator-direct
//!   shortcut Phase 7e was forced into). Asserts all 50 acks land,
//!   each ack carries the full `RecordMetadata` shape, and per-
//!   partition offsets are monotonic.
//! - `close_flushes_pending_inflight` — 50 records, immediate
//!   `close_with_timeout`, asserts all 50 send futures resolve after
//!   close returns (the Phase-7 carry-over: graceful-close drains
//!   in-flight before tearing the Sender down).
//!
//! Requires Docker on `$PATH`. Run with:
//!
//! ```sh
//! cargo test --features integration-tests producer_smoke -- --nocapture
//! ```

use std::collections::{HashMap, HashSet};
use std::process::Command;
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant};

use log::{Level, LevelFilter, Metadata as LogMetadata, Record};

/// Minimal stderr logger so the `log::trace!`/`debug!`/`info!`/`warn!`
/// emissions inside the producer-side code path show up under
/// `cargo test -- --nocapture`. Activated via `RUST_LOG=trace` or
/// equivalent — defaults to `INFO`.
///
/// Hand-rolled to avoid pulling `env_logger` into dev-dependencies
/// (Phase 8a brief: no new deps).
struct StderrLogger;

impl log::Log for StderrLogger {
    fn enabled(&self, metadata: &LogMetadata) -> bool {
        metadata.level() <= Level::Trace
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            eprintln!("[{} {}] {}", record.level(), record.target(), record.args());
        }
    }

    fn flush(&self) {}
}

static LOGGER: StderrLogger = StderrLogger;
static LOGGER_INIT: Once = Once::new();

fn init_logger() {
    LOGGER_INIT.call_once(|| {
        let _ = log::set_logger(&LOGGER);
        let level = std::env::var("RUST_LOG")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(LevelFilter::Info);
        log::set_max_level(level);
    });
}

use confluent_kafka::common::KafkaError;
use confluent_kafka::common::serialization::serdes::ByteArrayOwnedSerializer;
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerRecord, RecordMetadata};

use crate::common::cluster_config::ClusterConfig;
use crate::common::cluster_pool;
use crate::common::test_context::TestContext;

/// Number of records the happy-path sender pushes.
const HAPPY_PATH_RECORDS: usize = 1_000;
/// Partition count for the pre-created topic.
const TOPIC_PARTITIONS: i32 = 3;
/// Number of records the close-flush test pushes.
const CLOSE_FLUSH_RECORDS: usize = 50;

/// Pre-creates a topic on the live broker via `docker exec kafka-topics`.
///
/// Java's integration tests reach for `AdminClient::create_topics`; we
/// do not have an `AdminClient` translation yet (Milestone-1 scope). The
/// `kafka-topics` CLI shipped inside the broker container provides the
/// equivalent capability and matches the convention documented in
/// `design/history/Milestone-1/Phase-8/NOTES.md`:
///
/// > Pre-create the test topic via a one-shot `docker exec
/// > kafka-topics --create` in the test harness. Do not rely on
/// > `auto.create.topics.enable` — it races against the producer's
/// > first MetadataRequest.
///
/// **Bootstrap from inside the container**: every client-facing
/// listener (PLAINTEXT, SSL, SASL_*) advertises `127.0.0.1:<host-
/// mapped port>` — addresses that resolve to the *host's* loopback,
/// not the container's. The AdminClient initiated by `kafka-topics`
/// would talk to its bootstrap, receive `127.0.0.1:<host-port>` back
/// as the broker's metadata, then fail to connect from inside the
/// container. The BROKER (inter-broker) listener is the only one
/// whose advertised address (`<container_name>:9093` per
/// `tests/common/kafka_cluster.rs:174-181`) resolves *inside* the
/// container: Docker sets the container's own hostname to its name,
/// so `<container_name>:9093` round-trips. We therefore bootstrap on
/// `localhost:9093`, which Kafka redirects via metadata to
/// `<container_name>:9093`, which resolves to the container's own
/// loopback. No host port involved.
fn create_topic(container_id: &str, topic: &str, partitions: i32) {
    let create = Command::new("docker")
        .args([
            "exec",
            container_id,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            // BROKER inter-broker listener, advertised as
            // `<container_name>:9093` — the only address the
            // container can reach itself by. See the function rustdoc.
            "localhost:9093",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            &partitions.to_string(),
            "--replication-factor",
            "1",
        ])
        .output()
        .expect("failed to invoke `docker exec kafka-topics.sh` — is Docker on PATH?");
    assert!(
        create.status.success(),
        "kafka-topics --create failed (status: {:?})\nstdout: {}\nstderr: {}",
        create.status,
        String::from_utf8_lossy(&create.stdout),
        String::from_utf8_lossy(&create.stderr),
    );

    // Verify topic exists via `--describe`. Catches the case where
    // `--if-not-exists` swallows a partial failure or the topic was
    // created on a different cluster.
    let describe = Command::new("docker")
        .args([
            "exec",
            container_id,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9093",
            "--describe",
            "--topic",
            topic,
        ])
        .output()
        .expect("failed to invoke `docker exec kafka-topics.sh --describe`");
    let stdout = String::from_utf8_lossy(&describe.stdout);
    assert!(
        stdout.contains(topic),
        "kafka-topics --describe did not show topic {topic}; stdout: {stdout}\nstderr: {}",
        String::from_utf8_lossy(&describe.stderr),
    );
}

/// Build the standard PLAINTEXT producer config used by both tests.
///
/// Mirrors the Phase 8a brief verbatim:
/// `acks=all`, `linger.ms=10`, no compression, default partitioner,
/// `client.id=producer-smoke-test`.
///
/// **`key.serializer` / `value.serializer` placeholders**: Phase 7a's
/// `ProducerConfig` keeps Java's contract that both are required keys
/// with no default. Java's `KafkaProducer(Map, Serializer, Serializer)`
/// constructor *overwrites* whatever FQCNs the user supplied with the
/// passed instance's class name (Java line ~316) — but the validator
/// still demands the key be present. The Rust `with_serializers` path
/// runs the same validator before swapping in the supplied instances,
/// so we set placeholder FQCNs here. Phase 8.0's `log::warn!` on
/// FQCN-set-but-ignored fires for these placeholders, which is the
/// documented behavior (see `kafka_producer.rs:337-352` and Critic 8's
/// Suggestion 3 disposition).
fn build_props(bootstrap_servers: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("acks".to_string(), "all".to_string()),
        ("linger.ms".to_string(), "10".to_string()),
        ("compression.type".to_string(), "none".to_string()),
        ("client.id".to_string(), "producer-smoke-test".to_string()),
        // Required-key placeholder; the instance passed to
        // `with_serializers` wins.
        (
            "key.serializer".to_string(),
            "org.apache.kafka.common.serialization.ByteArraySerializer".to_string(),
        ),
        (
            "value.serializer".to_string(),
            "org.apache.kafka.common.serialization.ByteArraySerializer".to_string(),
        ),
    ])
}

/// Macro-style inline producer-build snippet. The concrete
/// `KafkaProducer<Vec<u8>, Vec<u8>, NetworkClient<Selector,
/// DefaultMetadataUpdater>>` return type is opaque at the test-crate
/// boundary — `DefaultMetadataUpdater` is `pub` but `#[doc(hidden)]`
/// (Phase 8a's visibility fix on top of Phase 8.0's inner-class
/// translation: Java's inner class has no public surface, but Rust's
/// visibility model forces us to expose the type so downstream
/// callers can *name* the value the public constructor returns).
/// Inlining keeps every test fn body free of the long type path —
/// type inference fills in `C` from the constructor.
macro_rules! build_producer {
    ($bootstrap_servers:expr) => {{
        let key_ser: Box<dyn confluent_kafka::common::serialization::Serializer<Vec<u8>>> =
            Box::new(ByteArrayOwnedSerializer);
        let value_ser: Box<dyn confluent_kafka::common::serialization::Serializer<Vec<u8>>> =
            Box::new(ByteArrayOwnedSerializer);
        KafkaProducer::with_serializers(build_props($bootstrap_servers), key_ser, value_ser)
            .expect("KafkaProducer::with_serializers failed")
    }};
}

// ---------------------------------------------------------------------------
// Test 1: 1000-record happy path — EXPLICIT-PARTITION path (Phase 8b)
// ---------------------------------------------------------------------------

/// Drives 1000 distinct keyed records through a real broker over
/// PLAINTEXT with **explicit** partition assignment: record `i` is
/// sent with `partition = i % TOPIC_PARTITIONS`. Asserts:
///
/// 1. **Ack count** (8a) — every send resolves with `RecordMetadata`.
/// 2. **`RecordMetadata` shape** (8a) — topic match, partition in
///    `[0, 3)`, non-negative offset, non-`-1` timestamp.
/// 3. **Partition consistency, explicit path** (8b) — for record `i`,
///    `m.partition() == i % TOPIC_PARTITIONS`. Exercises Java
///    `KafkaProducer.partition()`'s "explicit-partition honored"
///    branch at `KafkaProducer.java:1014-1024` (Rust equivalent at
///    `kafka_producer.rs::partition()`, "explicit partition wins"
///    early-return).
/// 4. **Per-partition monotonic offsets** (8b) — within each
///    partition, the broker assigns offsets in strict send order. The
///    explicit-partition path makes the per-partition send order
///    deterministic at the test side, so the assertion is exact:
///    offsets are strictly increasing as `i` increases through the
///    records that landed on a given partition.
/// 5. **Multi-partition coverage** (8b) — all 3 partitions see
///    roughly equal traffic (each partition gets `HAPPY_PATH_RECORDS
///    / 3` records by construction).
#[tokio::test(flavor = "multi_thread")]
async fn producer_smoke_plaintext_1000_records() {
    init_logger();
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("smoke_1000");
    let bootstrap_servers = ctx.bootstrap_servers().to_string();

    // Topic pre-creation via `docker exec kafka-topics` — `auto.create
    // .topics.enable` races the producer's first MetadataRequest.
    let cluster = cluster_pool::get_or_create(&ClusterConfig::default()).await;
    create_topic(&cluster.container_ids()[0], &topic, TOPIC_PARTITIONS);

    let producer = Arc::new(build_producer!(&bootstrap_servers));

    // Issue 1000 `send()` calls sequentially on the test task. This
    // is intentional — `send()` returns immediately after the
    // accumulator append, so the loop body is non-blocking (the
    // broker ack is awaited later via the returned `KafkaFuture`).
    // Sequential enqueue is required to make the per-partition send
    // order deterministic for the monotonic-offset assertion below:
    // the producer appends to its per-partition buffer in call order,
    // and the broker assigns offsets in append order. A `tokio::
    // spawn`-per-record fan-out would let the Tokio runtime reorder
    // the `send()` calls, breaking the per-partition send-order
    // invariant. Java's `KafkaProducerTest` follows the same pattern.
    //
    // We collect the returned `KafkaFuture`s and await their broker
    // acks concurrently afterward (`futures::future::join_all`).
    let topic_arc: Arc<str> = Arc::from(topic.as_str());
    let mut futures = Vec::with_capacity(HAPPY_PATH_RECORDS);
    for i in 0..HAPPY_PATH_RECORDS {
        let topic = topic_arc.clone();
        let key = format!("k{i:04}").into_bytes();
        let value = format!("v{i:04}").into_bytes();
        let expected_partition = (i as i32) % TOPIC_PARTITIONS;
        // Explicit-partition constructor — `ProducerRecord::
        // with_partition` sets the partition Java honors at
        // `KafkaProducer.partition()`'s early-return.
        let record = ProducerRecord::with_partition(topic, Some(expected_partition), Some(key), Some(value))
            .expect("ProducerRecord::with_partition");
        // Phase 7g (Java parity): send() returns
        // Result<KafkaFuture<RecordMetadata>, KafkaError>. The outer
        // Result is the sync-throw enqueue (we await it here so the
        // accumulator append happens in loop order); the inner
        // KafkaFuture is the broker-ack future (awaited concurrently
        // below).
        let fut = producer
            .send(record)
            .await
            .unwrap_or_else(|e| panic!("send #{i} enqueue failed: {e:?}"));
        futures.push(fut);
    }

    // Collect every broker ack. `join_all` is fine — `KafkaFuture` is
    // `Send`, and we want concurrent ack resolution.
    let results = futures_util::future::join_all(futures.into_iter().map(|f| async move { f.get().await })).await;
    let mut metadatas: Vec<RecordMetadata> = Vec::with_capacity(HAPPY_PATH_RECORDS);
    for (i, r) in results.into_iter().enumerate() {
        let meta = r.unwrap_or_else(|e| panic!("send #{i} broker ack failed: {e:?}"));
        metadatas.push(meta);
    }

    // --- Assertions ---

    // (1) Ack count: exactly 1000.
    assert_eq!(
        metadatas.len(),
        HAPPY_PATH_RECORDS,
        "expected {HAPPY_PATH_RECORDS} acked records, got {}",
        metadatas.len(),
    );

    // (2) RecordMetadata shape: topic matches, partition in [0, 3),
    // non-negative offset, non `-1` timestamp.
    // (3) Partition consistency, explicit path: `m.partition() == i %
    // TOPIC_PARTITIONS`.
    for (i, m) in metadatas.iter().enumerate() {
        let expected_partition = (i as i32) % TOPIC_PARTITIONS;
        assert_eq!(m.topic(), topic.as_str(), "record #{i}: topic mismatch");
        assert_eq!(
            m.partition(),
            expected_partition,
            "record #{i}: partition mismatch — expected {expected_partition} (explicit), got {}",
            m.partition(),
        );
        assert!(
            (0..TOPIC_PARTITIONS).contains(&m.partition()),
            "record #{i}: partition {} not in [0, {})",
            m.partition(),
            TOPIC_PARTITIONS,
        );
        assert!(m.offset() >= 0, "record #{i}: negative offset {}", m.offset());
        assert!(m.has_timestamp(), "record #{i}: timestamp is -1 (NO_TIMESTAMP)");
    }

    // (4) Per-partition monotonic offsets: within each partition, the
    // broker assigns offsets strictly increasing in send order. With
    // explicit-partition assignment the send order at the test side
    // is `0..HAPPY_PATH_RECORDS`, so iterating `metadatas` in index
    // order and grouping by partition preserves the per-partition
    // send order. Adjacent offsets must satisfy `prev < curr`.
    let mut by_partition: HashMap<i32, Vec<i64>> = HashMap::new();
    for m in &metadatas {
        by_partition.entry(m.partition()).or_default().push(m.offset());
    }
    for (partition, offsets) in &by_partition {
        for w in offsets.windows(2) {
            assert!(
                w[0] < w[1],
                "partition {partition}: offsets not strictly monotonic — prev={} curr={} (full: {offsets:?})",
                w[0],
                w[1],
            );
        }
    }

    // (5) Multi-partition coverage: all 3 partitions saw traffic. The
    // explicit-partition path makes this exact — every partition in
    // [0, TOPIC_PARTITIONS) is populated by construction
    // (`HAPPY_PATH_RECORDS = 1000`, `TOPIC_PARTITIONS = 3`, so each
    // partition holds ~333 records).
    let partitions: HashSet<i32> = metadatas.iter().map(RecordMetadata::partition).collect();
    assert_eq!(
        partitions.len(),
        TOPIC_PARTITIONS as usize,
        "expected all {TOPIC_PARTITIONS} partitions to see traffic, got {} (partitions: {partitions:?})",
        partitions.len(),
    );

    // Graceful close — drains pending in-flight, joins Sender task.
    // 30s timeout matches the brief and is generous for a 1000-record
    // flush on a localhost broker.
    let producer = Arc::try_unwrap(producer)
        .map_err(|_| ())
        .expect("producer Arc had outstanding refs at close");
    producer
        .close_with_timeout(Duration::from_secs(30))
        .await
        .expect("graceful close failed");
}

// ---------------------------------------------------------------------------
// Test 2: 1000-record auto-partition consistency (Phase 8b)
// ---------------------------------------------------------------------------

/// Drives 1000 records through a real broker over PLAINTEXT with
/// **no** explicit partition and **no** key (every record built via
/// `ProducerRecord::new(topic, value)`). Asserts that the partition
/// the producer's partitioner selects at `send()` time — observed
/// pre-network through the [`KafkaProducer::set_partition_observer`]
/// test seam — equals the partition the broker echoes back in the
/// [`RecordMetadata`] ack. Pins partitioner-vs-broker agreement on
/// the auto-partition path.
///
/// **Why a test seam and not coverage assertions**: with no key and
/// no explicit partition, partition selection is delegated to the
/// configured partitioner (the default sticky partitioner during
/// Milestone-1). The sticky partitioner intentionally batches into
/// **one** partition until the in-flight batch fills or the linger
/// window elapses, so a 1000-record burst in a single linger window
/// may land entirely on a single partition. We therefore cannot
/// assert 3-partition coverage here — test 1 (explicit-partition
/// path) is what asserts coverage. What we **can** assert is that
/// whatever partition the partitioner picked equals the partition
/// in the ack, which is the partitioner-vs-broker contract.
///
/// **Observer correlation**: with sequential `send().await` the
/// observer fires synchronously during each `send().await` — see
/// `KafkaProducer::do_send_inner`'s observation point, which runs
/// after `accumulator.append().await` and before the function
/// returns. So pushing into a `Vec<i32>` from inside the observer
/// produces a vector indexed by send order (== record index), and
/// the i-th element correlates with the i-th ack. No `AtomicUsize`
/// counter is needed — the sequential structure already pins the
/// ordering. The observer fires exactly once per `do_send` (Sender-
/// driven retries do not re-enter `do_send_inner`), which is the
/// CLAUDE.md rule 9.5 callback-obligation contract.
///
/// **Java parity**: Java's `KafkaProducerTest` observes the
/// auto-partition selection through an interceptor's
/// `onAcknowledgement` callback, which sees the resolved partition
/// in its `RecordMetadata` argument — but that interceptor fires
/// AFTER the broker round-trip, so it cannot independently witness
/// the pre-network selection. To independently prove that the
/// pre-network partition equals the post-network partition we need
/// to observe both ends; Java does so implicitly via the
/// `Partitioner.partition()` return value, which is observable
/// inside the partitioner's own mock. Rust's
/// `set_partition_observer` plays the same role: it captures the
/// partition the producer chose at the same code path point the
/// Java partitioner would publish it from.
#[tokio::test(flavor = "multi_thread")]
async fn producer_smoke_plaintext_auto_partition() {
    init_logger();
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("smoke_auto_partition");
    let bootstrap_servers = ctx.bootstrap_servers().to_string();

    let cluster = cluster_pool::get_or_create(&ClusterConfig::default()).await;
    create_topic(&cluster.container_ids()[0], &topic, TOPIC_PARTITIONS);

    let producer = Arc::new(build_producer!(&bootstrap_servers));

    // Register the partition observer BEFORE any `send()` calls.
    // The observer pushes `(topic, partition)` into a shared `Vec`;
    // the test thread reads it after all sends resolve. Using a
    // `Mutex<Vec<...>>` (not `Arc<Mutex<...>>` for the closure side
    // — the closure captures the `Arc` and clones it into the
    // closure body via the `move` keyword below) keeps the
    // single-Mutex pattern from the `set_partition_observer`
    // rustdoc: the closure does NOT hold the lock across any
    // `.await`, because the closure has no `.await` — it is a sync
    // `Fn`.
    let observed: Arc<Mutex<Vec<(String, i32)>>> = Arc::new(Mutex::new(Vec::with_capacity(HAPPY_PATH_RECORDS)));
    {
        let observed = observed.clone();
        producer.set_partition_observer(move |topic, partition| {
            // Lock-acquire is local to this sync closure body. No
            // `.await` here means the guard cannot straddle a yield
            // point (CLAUDE.md rule 9.6).
            observed
                .lock()
                .expect("observed mutex poisoned")
                .push((topic.to_string(), partition));
        });
    }

    // Issue 1000 `send()` calls sequentially. Same rationale as
    // test 1: sequential enqueue keeps observation order aligned
    // with record index. The records carry NO key (so the
    // partitioner cannot hash-route them) and NO explicit
    // partition (so the partitioner is consulted).
    let topic_arc: Arc<str> = Arc::from(topic.as_str());
    let mut futures = Vec::with_capacity(HAPPY_PATH_RECORDS);
    for i in 0..HAPPY_PATH_RECORDS {
        let topic = topic_arc.clone();
        let value = format!("v{i:04}").into_bytes();
        // No key, no partition — auto-partition path. `Producer
        // Record::new(topic, value)` is the no-key, no-partition
        // constructor.
        let record = ProducerRecord::new(topic, Some(value)).expect("ProducerRecord::new");
        let fut = producer
            .send(record)
            .await
            .unwrap_or_else(|e| panic!("send #{i} enqueue failed: {e:?}"));
        futures.push(fut);
    }

    // Collect every broker ack concurrently.
    let results = futures_util::future::join_all(futures.into_iter().map(|f| async move { f.get().await })).await;
    let mut metadatas: Vec<RecordMetadata> = Vec::with_capacity(HAPPY_PATH_RECORDS);
    for (i, r) in results.into_iter().enumerate() {
        let meta = r.unwrap_or_else(|e| panic!("send #{i} broker ack failed: {e:?}"));
        metadatas.push(meta);
    }

    // --- Assertions ---

    // (1) Ack count.
    assert_eq!(
        metadatas.len(),
        HAPPY_PATH_RECORDS,
        "expected {HAPPY_PATH_RECORDS} acks, got {}",
        metadatas.len(),
    );

    // (2) Observer fired exactly once per record. If the observer
    // double-fired (e.g. on Sender-driven retry), `observed.len()`
    // would exceed `HAPPY_PATH_RECORDS`; if it under-fired (e.g.
    // some send path bypassed `do_send_inner`'s observation
    // point), it would be short. Either is a test failure — the
    // observer is part of the CLAUDE.md rule 9.5 callback contract.
    let observed = observed.lock().expect("observed mutex poisoned");
    assert_eq!(
        observed.len(),
        HAPPY_PATH_RECORDS,
        "expected observer to fire exactly {HAPPY_PATH_RECORDS} times, got {} (callback-obligation contract \
         broken — see do_send_inner observation point)",
        observed.len(),
    );

    // (3) `RecordMetadata` shape: topic match, partition in [0,
    // TOPIC_PARTITIONS), non-negative offset, non-`-1` timestamp.
    for (i, m) in metadatas.iter().enumerate() {
        assert_eq!(m.topic(), topic.as_str(), "record #{i}: topic mismatch");
        assert!(
            (0..TOPIC_PARTITIONS).contains(&m.partition()),
            "record #{i}: partition {} not in [0, {})",
            m.partition(),
            TOPIC_PARTITIONS,
        );
        assert!(m.offset() >= 0, "record #{i}: negative offset {}", m.offset());
        assert!(m.has_timestamp(), "record #{i}: timestamp is -1 (NO_TIMESTAMP)");
    }

    // (4) Partitioner-vs-broker agreement: for each record, the
    // partition observed pre-network equals the partition in the
    // ack. This is the auto-partition consistency contract — it
    // says nothing about *which* partition was chosen (the sticky
    // partitioner's choice can be all-on-one), only that whatever
    // was chosen pre-network is what the broker acked.
    for (i, m) in metadatas.iter().enumerate() {
        let (obs_topic, obs_partition) = &observed[i];
        assert_eq!(
            obs_topic.as_str(),
            topic.as_str(),
            "record #{i}: observer saw topic {obs_topic}, ack saw topic {}",
            m.topic(),
        );
        assert_eq!(
            *obs_partition,
            m.partition(),
            "record #{i}: observer saw partition {obs_partition}, ack saw partition {} (partitioner-vs-broker \
             disagreement)",
            m.partition(),
        );
    }
    drop(observed);

    // Graceful close.
    let producer = Arc::try_unwrap(producer)
        .map_err(|_| ())
        .expect("producer Arc had outstanding refs at close");
    producer
        .close_with_timeout(Duration::from_secs(30))
        .await
        .expect("graceful close failed");
}

// ---------------------------------------------------------------------------
// Test 3: flush() drains 50 records through the public Producer API
//   (Phase 7f carry-over, retired in Phase 8b)
// ---------------------------------------------------------------------------

/// Pins the public-API flush path's fidelity. Phase 7f shipped
/// `flush()` but its end-to-end coverage was deferred — Phase 7's
/// `metadata.close()` lift point flagged "50-record `flush()`
/// fidelity" as a Phase-8 carry-over (`NOTES.md` "Phase-7 carry-overs
/// retired here" #2).
///
/// Shape: send 50 records via the public `Producer::send(...).await`
/// path with **explicit-partition** assignment (record `i` →
/// partition `i % TOPIC_PARTITIONS`), capturing each returned
/// `KafkaFuture<RecordMetadata>` WITHOUT awaiting its broker ack.
/// Then call `producer.flush().await` and finally await each
/// captured future. The producer must remain usable afterward
/// (the Sender task is alive — `flush` does NOT tear it down,
/// unlike `close_with_timeout`).
///
/// Why explicit-partition: keeps the per-partition send order
/// deterministic at the test side, the same pattern test 1 uses.
/// The sticky partitioner would otherwise let a single partition
/// hold all 50 records in one linger window, leaving the
/// monotonic-offset assertion meaningful only over a single
/// partition — explicit-partition fans the records across all 3,
/// so the assertion exercises every partition.
///
/// Why this test exists alongside `close_flushes_pending_inflight`:
/// `flush()` and `close()` flush pending sends through different
/// code paths in the `Sender` loop:
/// - `close_with_timeout` calls `close_with_timeout_inner`, which
///   sets `force_close` / `running = false` and tells the Sender
///   to drain-then-exit. The Sender task is **torn down**
///   afterward.
/// - `flush()` calls `accumulator.begin_flush()` +
///   `await_flush_completion`, which marks every batch as
///   flushable and waits for them to be ack'd, but does NOT
///   touch `running`. The Sender task continues processing
///   subsequent sends.
///
/// Both paths must drain in-flight before returning; both deserve
/// their own integration coverage. This test pins the second
/// path; `close_flushes_pending_inflight` pins the first.
#[tokio::test(flavor = "multi_thread")]
async fn flush_drains_50_records_through_public_api() {
    init_logger();
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("smoke_flush_50");
    let bootstrap_servers = ctx.bootstrap_servers().to_string();

    let cluster = cluster_pool::get_or_create(&ClusterConfig::default()).await;
    create_topic(&cluster.container_ids()[0], &topic, TOPIC_PARTITIONS);

    let producer = Arc::new(build_producer!(&bootstrap_servers));
    let topic_arc: Arc<str> = Arc::from(topic.as_str());

    // Send 50 records sequentially without awaiting their broker
    // acks. The accumulator append (inside `do_send`) completes
    // synchronously per `send().await`, so the loop returns 50
    // pending `KafkaFuture<RecordMetadata>`s with records sitting
    // in per-partition buffers waiting for the Sender to drain.
    let mut futures = Vec::with_capacity(CLOSE_FLUSH_RECORDS);
    for i in 0..CLOSE_FLUSH_RECORDS {
        let topic = topic_arc.clone();
        let key = format!("fk{i:02}").into_bytes();
        let value = format!("fv{i:02}").into_bytes();
        let expected_partition = (i as i32) % TOPIC_PARTITIONS;
        let record = ProducerRecord::with_partition(topic, Some(expected_partition), Some(key), Some(value))
            .expect("ProducerRecord::with_partition");
        let fut = producer
            .send(record)
            .await
            .unwrap_or_else(|e| panic!("send #{i} enqueue failed: {e:?}"));
        futures.push(fut);
    }

    // Call the public `flush()` — the test contract under
    // scrutiny. Java's `KafkaProducer::flush()` blocks until every
    // record currently in the accumulator is ack'd; Rust's mirror
    // is the `Producer::flush` async fn (parity: `flush().await`
    // == Java `flush()`).
    //
    // 30s timeout backstop via wall-clock measurement — if flush
    // exceeds 30s for 50 small records on localhost something is
    // wrong (sender-wakeup miss or accumulator never seeing the
    // flushable mark). The actual flush is typically under 100 ms.
    const FLUSH_TIMEOUT: Duration = Duration::from_secs(30);
    let flush_start = Instant::now();
    producer.flush().await.expect("flush failed");
    let flush_elapsed = flush_start.elapsed();
    assert!(
        flush_elapsed < FLUSH_TIMEOUT,
        "flush did not drain before 30s: elapsed={flush_elapsed:?}",
    );

    // After `flush()` returns, every captured future MUST resolve
    // immediately with `Ok(RecordMetadata)` — the flush contract
    // says the broker has already ack'd every pending record by
    // the time `flush()` returns. So the inner `.get().await`s
    // are nominally synchronous.
    let results = futures_util::future::join_all(futures.into_iter().map(|f| async move { f.get().await })).await;
    let mut metas: Vec<RecordMetadata> = Vec::with_capacity(CLOSE_FLUSH_RECORDS);
    for (i, r) in results.into_iter().enumerate() {
        let meta = r.unwrap_or_else(|e| panic!("send #{i} broker ack failed (post-flush): {e:?}"));
        metas.push(meta);
    }

    // --- Assertions ---

    // (1) All 50 acks landed.
    assert_eq!(
        metas.len(),
        CLOSE_FLUSH_RECORDS,
        "expected {CLOSE_FLUSH_RECORDS} acks post-flush, got {}",
        metas.len(),
    );

    // (2) `RecordMetadata` shape: topic / partition / offset /
    // timestamp (the full 8a shape contract, same as test 1).
    for (i, m) in metas.iter().enumerate() {
        let expected_partition = (i as i32) % TOPIC_PARTITIONS;
        assert_eq!(m.topic(), topic.as_str(), "record #{i}: topic mismatch");
        assert_eq!(
            m.partition(),
            expected_partition,
            "record #{i}: partition mismatch — expected {expected_partition} (explicit), got {}",
            m.partition(),
        );
        assert!(
            (0..TOPIC_PARTITIONS).contains(&m.partition()),
            "record #{i}: partition {} not in [0, {})",
            m.partition(),
            TOPIC_PARTITIONS,
        );
        assert!(m.offset() >= 0, "record #{i}: negative offset {}", m.offset());
        assert!(m.has_timestamp(), "record #{i}: timestamp is -1 (NO_TIMESTAMP)");
    }

    // (3) Per-partition strict-monotonic offsets — explicit
    // assignment means the per-partition send order is the same
    // index order, so adjacent offsets within a partition must
    // satisfy `prev < curr`.
    let mut by_partition: HashMap<i32, Vec<i64>> = HashMap::new();
    for m in &metas {
        by_partition.entry(m.partition()).or_default().push(m.offset());
    }
    for (partition, offsets) in &by_partition {
        for w in offsets.windows(2) {
            assert!(
                w[0] < w[1],
                "partition {partition}: offsets not strictly monotonic — prev={} curr={} (full: {offsets:?})",
                w[0],
                w[1],
            );
        }
    }

    // (4) Producer is still usable post-flush. Send + ack one
    // more record through the same producer to prove `flush()`
    // did NOT tear the Sender down. Without this assertion a
    // regression that turned `flush()` into a `close()` would
    // pass tests 1-3 silently — the captured futures would
    // resolve fine, but subsequent sends would fail.
    {
        let key = b"post_flush_k".to_vec();
        let value = b"post_flush_v".to_vec();
        let record =
            ProducerRecord::with_key(topic_arc.clone(), Some(key), Some(value)).expect("ProducerRecord::with_key");
        let meta = producer
            .send(record)
            .await
            .expect("post-flush send enqueue failed (flush appears to have torn the Sender down)")
            .get()
            .await
            .expect("post-flush broker ack failed");
        assert_eq!(meta.topic(), topic.as_str(), "post-flush record: topic mismatch");
    }

    // Graceful close.
    let producer = Arc::try_unwrap(producer)
        .map_err(|_| ())
        .expect("producer Arc had outstanding refs at close");
    producer
        .close_with_timeout(Duration::from_secs(30))
        .await
        .expect("graceful close failed");
}

// ---------------------------------------------------------------------------
// Test 4: close-flushes-pending-in-flight (Phase 7 carry-over)
// ---------------------------------------------------------------------------

/// Pins the graceful-close-flushes-in-flight contract. The Phase-7
/// `metadata.close()` lift point asked for an "explicit close after
/// pending in-flight" assertion; NOTES.md folded that into Phase 8a.
///
/// Shape: fire 50 records without awaiting their send futures, then
/// immediately call `close_with_timeout`. After close returns, every
/// send future must have resolved with `Ok(RecordMetadata)`. If
/// `close` short-circuited (force-close path) the futures would be
/// `Err(KafkaError::Generic("Producer closed while send in progress:
/// ..."))` — see `kafka_producer.rs:1199`.
#[tokio::test(flavor = "multi_thread")]
async fn close_flushes_pending_inflight() {
    init_logger();
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("smoke_close_flush");
    let bootstrap_servers = ctx.bootstrap_servers().to_string();

    let cluster = cluster_pool::get_or_create(&ClusterConfig::default()).await;
    create_topic(&cluster.container_ids()[0], &topic, TOPIC_PARTITIONS);

    let producer = Arc::new(build_producer!(&bootstrap_servers));
    let topic_arc: Arc<str> = Arc::from(topic.as_str());

    // Warmup send: drive one record through `send().await` so the
    // producer's `ProducerMetadata` caches the topic and the
    // subsequent 50 sends never block on metadata. Without this
    // warmup, the 50 sends queue up inside `wait_on_metadata`
    // (Phase 7d), the `close` fires while they're still in the
    // metadata-wait phase (not yet in the accumulator), and the
    // graceful drain completes early because the accumulator is
    // empty. The producer would then `metadata.close()`, wake every
    // pending metadata waiter with `Err("Requested metadata update
    // after close")` (`producer_metadata.rs:256`), and the 50 send
    // futures resolve with Err. Java's `close()` has the same
    // behavior — records that haven't reached the accumulator yet
    // are not "in-flight" in the sense that `close` flushes them.
    {
        let record = ProducerRecord::with_key(topic_arc.clone(), Some(b"warmup".to_vec()), Some(b"warmup".to_vec()))
            .expect("ProducerRecord::with_key");
        // Phase 7g: await the broker ack via `send().await?.get().await` —
        // the warmup is `Future#get()`-shaped, blocking until the broker
        // resolves the metadata-cache-populating record.
        producer
            .send(record)
            .await
            .expect("warmup enqueue failed")
            .get()
            .await
            .expect("warmup send failed");
    }

    // Build 50 send futures WITHOUT spawning. Each future is held by
    // a `FuturesUnordered` and driven concurrently with the close
    // future via a single `tokio::join!`. This keeps the entire
    // "fire 50 then close" sequence on the test thread's runtime —
    // no per-task scheduling latency between the spawn loop and the
    // close trigger.
    use futures_util::stream::{FuturesUnordered, StreamExt};
    let send_futures: FuturesUnordered<_> = (0..CLOSE_FLUSH_RECORDS)
        .map(|i| {
            let producer = producer.clone();
            let topic = topic_arc.clone();
            let key = format!("ck{i:02}").into_bytes();
            let value = format!("cv{i:02}").into_bytes();
            async move {
                let record = ProducerRecord::with_key(topic, Some(key), Some(value)).expect("ProducerRecord::with_key");
                // Phase 7g: send().await returns
                // Result<KafkaFuture<RecordMetadata>, KafkaError>. Chain
                // .get().await to await the broker ack, mirroring Java's
                // `producer.send(...).get()`.
                producer.send(record).await?.get().await
            }
        })
        .collect();

    // Drive all 50 sends through `do_send` (enqueue into accumulator,
    // but NOT yet broker-acked) by polling `FuturesUnordered` once
    // without consuming any items: every `send()` future's first poll
    // walks through `wait_on_metadata` (instant — cached) →
    // `do_send` → accumulator.append, and then suspends awaiting the
    // broker ack. After this `now_or_never`-like dance, all 50
    // records are sitting in the accumulator. We then collect the
    // remaining outputs concurrently with the close.
    //
    // The simplest stable way: `tokio::join!` the `send_futures`
    // stream (drained to completion) and the close future. The close
    // path's `accumulator.begin_flush()` + `await_flush_completion`
    // snapshot now includes every batch (because they're all in the
    // accumulator before either future blocks meaningfully — Tokio
    // polls each branch of `join!` at least once before suspending).
    // Close-timeout sizing: 5s is a tight watchdog. With Phase 8a.0
    // Round 2 Suggestion 1's `tokio::sync::Notify`-backed
    // `sender_wakeup` (commit `397dc09`), the wake primitive fires
    // in microseconds (a `Notify::notify_one()` CAS); close-drain
    // end-to-end is in the low-millisecond range, dominated by the
    // broker-ack RTT for 50 small records on localhost (observed:
    // 2-4 ms per run-cycle). Pre-Suggestion-1, this used to sit at
    // 90s to accommodate the 30s `default.request.timeout.ms`
    // backstop tick (the no-op `sender_wakeup` left close-drain
    // bounded only by that cap). With the real wake mechanism, 5s
    // is ~1000x the expected drain time and still catches any
    // future missed-wake regression.
    //
    // Contract pinned: graceful close MUST flush all pending sends
    // before its deadline. We assert that by measuring elapsed wall-
    // clock and requiring it to be strictly less than the timeout —
    // proving the close path drained rather than tripping the force-
    // close branch. Every send future must then resolve with
    // `Ok(RecordMetadata)`; any `Err` (including `IllegalState` from
    // force-close) is a test failure. Do NOT tolerate `IllegalState`
    // here — this test exists specifically to pin the graceful-flush
    // contract.
    const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
    let producer_for_close = producer.clone();
    let close_start = Instant::now();
    let (send_results, close_result) = tokio::join!(
        async {
            let mut results: Vec<Result<RecordMetadata, KafkaError>> = Vec::with_capacity(CLOSE_FLUSH_RECORDS);
            let mut send_futures = send_futures;
            while let Some(r) = send_futures.next().await {
                results.push(r);
            }
            results
        },
        async move { producer_for_close.close_with_timeout(CLOSE_TIMEOUT).await },
    );
    let close_elapsed = close_start.elapsed();
    close_result.expect("graceful close failed");
    assert!(
        close_elapsed < CLOSE_TIMEOUT,
        "close did not drain before timeout: elapsed={close_elapsed:?} >= timeout={CLOSE_TIMEOUT:?} (force-close path likely)",
    );
    println!("close drained in {close_elapsed:?}");

    let mut metas: Vec<RecordMetadata> = Vec::with_capacity(CLOSE_FLUSH_RECORDS);
    for (i, r) in send_results.into_iter().enumerate() {
        let meta = r.unwrap_or_else(|e| {
            panic!("send #{i} failed after close (topic={topic}, expected graceful flush): {e:?}",)
        });
        metas.push(meta);
    }

    assert_eq!(
        metas.len(),
        CLOSE_FLUSH_RECORDS,
        "expected {CLOSE_FLUSH_RECORDS} acked records after close, got {}",
        metas.len(),
    );

    // Full `RecordMetadata` shape check — same contract as test 1's
    // per-record loop. Phase 8a Round 1 Suggestion 2: tightens the
    // graceful-close contract from "offset ≥ 0" to "the entire
    // RecordMetadata shape is intact post-flush". Catches a
    // regression where the graceful-close path could resolve a
    // future with a synthetic `RecordMetadata` (wrong topic,
    // `UNKNOWN_PARTITION`, or `NO_TIMESTAMP`) and still pass the
    // bare offset check.
    for (i, m) in metas.iter().enumerate() {
        assert_eq!(m.topic(), topic.as_str(), "record #{i}: topic mismatch");
        assert!(
            (0..TOPIC_PARTITIONS).contains(&m.partition()),
            "record #{i}: partition {} not in [0, {})",
            m.partition(),
            TOPIC_PARTITIONS,
        );
        assert!(m.offset() >= 0, "record #{i}: negative offset {}", m.offset());
        assert!(m.has_timestamp(), "record #{i}: timestamp is -1 (NO_TIMESTAMP)");
    }
}
