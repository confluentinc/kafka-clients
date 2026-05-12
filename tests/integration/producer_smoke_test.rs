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

//! Phase 8a — PLAINTEXT producer-side smoke test.
//!
//! Spins up a single-broker KRaft Kafka cluster via Testcontainers
//! (shared across tests through `cluster_pool`), pre-creates a topic
//! with 3 partitions via `docker exec kafka-topics`, then drives
//! [`KafkaProducer`] end-to-end:
//!
//! - `producer_smoke_plaintext_1000_records` — 1000 distinct keyed
//!   records, asserts every send future resolves with `RecordMetadata`
//!   and the partitioner covered ≥2 of the 3 partitions. Producer-side
//!   only — end-to-end consume fidelity is Phase 8c.
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
use std::sync::{Arc, Once};
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
// Test 1: 1000-record happy path
// ---------------------------------------------------------------------------

/// Drives 1000 distinct keyed records through a real broker over
/// PLAINTEXT. Asserts every send resolves with a `RecordMetadata` whose
/// shape (topic / partition in `[0, 3)` / non-negative offset / non
/// `-1` timestamp) matches what Java's `KafkaProducer.send().get()`
/// surfaces.
///
/// **Partition-coverage tolerance**: the default sticky partitioner is
/// not required to hit all 3 partitions during a single linger window
/// — it sticks to one partition until the batch fills or the linger
/// elapses, so a fast sender may close out before all 3 partitions
/// see traffic. The brief codifies this as "≥2 of the 3 partitions",
/// which is the tightest assertion we can make without flakiness.
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

    // Fan out 1000 sends through `tokio::spawn` so the linger window
    // can batch them. Per the brief: spawn happens in the test
    // harness, NOT inside `KafkaProducer::send` — CLAUDE.md rule 11
    // forbids per-message spawn in production code.
    let topic_arc: Arc<str> = Arc::from(topic.as_str());
    let mut handles: Vec<tokio::task::JoinHandle<Result<RecordMetadata, KafkaError>>> =
        Vec::with_capacity(HAPPY_PATH_RECORDS);
    for i in 0..HAPPY_PATH_RECORDS {
        let producer = producer.clone();
        let topic = topic_arc.clone();
        let key = format!("k{i:04}").into_bytes();
        let value = format!("v{i:04}").into_bytes();
        handles.push(tokio::spawn(async move {
            let record = ProducerRecord::with_key(topic, Some(key), Some(value)).expect("ProducerRecord::with_key");
            producer.send(record).await
        }));
    }

    // Collect every result. We assert per-record so a single failure
    // produces a precise error message instead of a misleading aggregate.
    let mut metadatas: Vec<RecordMetadata> = Vec::with_capacity(HAPPY_PATH_RECORDS);
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await.expect("tokio join failed");
        let meta = result.unwrap_or_else(|e| panic!("send #{i} failed: {e:?}"));
        metadatas.push(meta);
    }

    // --- Assertions ---

    // Ack count: exactly 1000.
    assert_eq!(
        metadatas.len(),
        HAPPY_PATH_RECORDS,
        "expected {HAPPY_PATH_RECORDS} acked records, got {}",
        metadatas.len(),
    );

    // RecordMetadata shape: topic matches, partition in [0, 3),
    // non-negative offset, non `-1` timestamp.
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

    // Partition coverage: ≥2 of 3 partitions saw traffic. See the
    // function rustdoc for why we don't assert ==3.
    let partitions: HashSet<i32> = metadatas.iter().map(RecordMetadata::partition).collect();
    assert!(
        partitions.len() >= 2,
        "expected ≥2 partitions to see traffic, got {} (partitions: {:?})",
        partitions.len(),
        partitions,
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
// Test 2: close-flushes-pending-in-flight (Phase 7 carry-over)
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
        producer.send(record).await.expect("warmup send failed");
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
                producer.send(record).await
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
    // Close-timeout sizing: must comfortably exceed any realistic broker
    // ack window for 50 small records with acks=all on a single-broker
    // Testcontainer. Earlier 30s value raced ack-latency on slower
    // machines, producing force-close and `IllegalState` send results.
    // 90s gives generous headroom while still acting as a watchdog.
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
    const CLOSE_TIMEOUT: Duration = Duration::from_secs(90);
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
    for (i, m) in metas.iter().enumerate() {
        assert!(m.offset() >= 0, "record #{i}: negative offset {}", m.offset());
    }
}
