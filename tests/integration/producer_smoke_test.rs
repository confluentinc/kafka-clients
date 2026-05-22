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
//!   traffic. Producer-side only — end-to-end byte fidelity is
//!   asserted separately in `producer_smoke_plaintext_byte_fidelity`.
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
//! - `producer_smoke_plaintext_byte_fidelity` — Phase 8c: end-to-end
//!   byte-fidelity. Sends 100 explicit-partition records and consumes
//!   them back via `docker exec kafka-console-consumer`. Asserts the
//!   (key, value) byte sequence produced into each partition equals
//!   the sequence the consumer reads back from the broker — closing
//!   the producer-side zero-copy guarantee (CLAUDE.md §12) at the
//!   broker boundary.
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
/// Number of records the byte-fidelity test pushes per run. Smaller
/// than `HAPPY_PATH_RECORDS` because the assertion (consume + per-
/// partition byte compare) is the cost driver here, not the
/// producer-side enqueue rate.
const BYTE_FIDELITY_RECORDS: usize = 100;

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

/// Consume records from a topic via `docker exec kafka-console-consumer`.
///
/// Reads from `--from-beginning` up to `max_messages` records (or until
/// `timeout_ms` elapses) and returns `(partition, key, value)` tuples
/// in the order the consumer printed them — which is **per-partition
/// offset order** (one consumer thread reads each partition
/// sequentially), but the order **across** partitions is unspecified
/// (the console consumer interleaves whichever partition has data
/// ready). Callers asserting per-partition order must group by
/// partition first.
///
/// # Byte-fidelity caveat (Phase 8c scope)
///
/// `kafka-console-consumer` defaults to the **string** key/value
/// deserializers, which lossily decode bytes as UTF-8 (invalid
/// sequences become U+FFFD). Phase 8c's test fixtures use only ASCII
/// keys and values (`format!("k{i:04}")`, `format!("v{i:04}")` —
/// digits + ASCII letters), so the lossy decode is the identity
/// function and the bytes-out bytes-in comparison is exact.
///
/// **If a future test uses non-ASCII bytes** (binary keys, protobuf,
/// random fuzz, etc.), this helper must be extended to pass
/// `--formatter-property key.deserializer=org.apache.kafka.common
/// .serialization.ByteArrayDeserializer` (and `value.deserializer`)
/// AND a wire format that survives binary output (the default
/// `LineMessageFormatter` writes the bytes raw, so the unit-separator
/// delimiter remains the right approach — but `--formatter-property
/// print.partition=true` then prepends a string anyway, so the
/// parsing below would still work as long as the separator
/// (`\x1F`, ASCII unit separator) does not collide with any byte in
/// the payload). Document the choice when extending.
///
/// # Format conventions
///
/// - `--formatter-property key.separator=\x1F` (ASCII unit separator,
///   0x1F) — chosen because it cannot appear in printable ASCII
///   payloads and is the documented "field separator" control
///   character.
/// - `--formatter-property print.partition=true` — prepends
///   `Partition:<n><key.separator>` to each line so the helper can
///   return the partition without parsing protobuf-style record
///   headers.
/// - Final per-line format: `Partition:<n>\x1F<key>\x1F<value>\n`
///   (the `DefaultMessageFormatter` joins every printed field with
///   the configured `key.separator`).
///
/// # Two byte-collision risks for non-ASCII payloads
///
/// 1. **`\x1F` (ASCII 0x1F, unit separator)** — used as the in-line
///    field separator. A binary payload containing 0x1F will be
///    mis-split. The parsing logic below uses `splitn(3, '\x1F')`, so
///    a key (the first segment after `Partition:<n>`) containing 0x1F
///    splits the value across the wrong boundary; a value containing
///    0x1F means the helper's tuple receives a truncated value.
/// 2. **`\n` (ASCII 0x0A, line-feed)** — used by
///    `DefaultMessageFormatter` as the inter-record terminator and is
///    not configurable through `--formatter-property` in the same
///    way as `key.separator`. A binary payload containing 0x0A will
///    be split across multiple lines, with the second line missing
///    the `Partition:<n>\x1F<key>\x1F` prefix entirely — the parser
///    below would either drop or panic on the malformed line. The
///    Phase 8c byte-fidelity test (`producer_smoke_plaintext_byte
///    _fidelity`) avoids both collisions by using only ASCII letters
///    + digits in keys and values; future tests with binary payloads
///    must either (a) length-prefix the record (so the helper can
///    seek past raw bytes deterministically) or (b) switch to a
///    consumer that emits a structured format like JSON-with-base64,
///    not `DefaultMessageFormatter`.
///
/// We pass `--formatter-property` rather than the older `--property`
/// because recent Kafka releases print a deprecation warning on
/// `--property` (which lands on the same stdout the test parses).
///
/// # Timeout
///
/// `timeout_ms` is the wall-clock the consumer waits without
/// receiving a message before exiting. The helper does not impose
/// any additional deadline — `Command::output` blocks until
/// `kafka-console-consumer` terminates, which it always does once
/// either `--max-messages` is reached or `--timeout-ms` elapses.
///
/// # Synchronous by design
///
/// Mirrors [`create_topic`]: blocks on `Command::output`. Async tests
/// should call this via [`tokio::task::spawn_blocking`] to avoid
/// stalling the runtime — `kafka-console-consumer` can take up to
/// `timeout_ms` real time to terminate.
fn consume_records(
    container_id: &str,
    topic: &str,
    max_messages: usize,
    timeout_ms: u32,
) -> Vec<(i32, Vec<u8>, Vec<u8>)> {
    // ASCII unit separator (0x1F). `kafka-console-consumer` reads
    // this via `--formatter-property key.separator=<literal char>`;
    // we pass the single 0x1F byte by writing it directly into the
    // argv string (Rust string literals support `\u{1F}`).
    const SEP: &str = "\u{1F}";

    let output = Command::new("docker")
        .args([
            "exec",
            container_id,
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            "localhost:9093",
            "--topic",
            topic,
            "--from-beginning",
            "--timeout-ms",
            &timeout_ms.to_string(),
            "--max-messages",
            &max_messages.to_string(),
            "--formatter-property",
            "print.key=true",
            "--formatter-property",
            "print.partition=true",
            "--formatter-property",
            &format!("key.separator={SEP}"),
        ])
        .output()
        .expect("failed to invoke `docker exec kafka-console-consumer.sh` — is Docker on PATH?");

    // `kafka-console-consumer` exits with code 1 when `--timeout-ms`
    // elapses, even after a successful read of `--max-messages`. We
    // therefore do NOT assert on `status.success()`; instead we
    // require the parsed record count to equal `max_messages`
    // below — that is the contract a caller cares about.
    let stdout = String::from_utf8(output.stdout)
        .expect("kafka-console-consumer stdout was not UTF-8 — non-ASCII payload? See helper rustdoc");

    let mut records: Vec<(i32, Vec<u8>, Vec<u8>)> = Vec::with_capacity(max_messages);
    for (line_no, line) in stdout.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        // `kafka-console-consumer`'s `DefaultMessageFormatter` joins
        // `Partition`, `key`, and `value` with the configured
        // `key.separator` — they are NOT separated by tab. Observed
        // per-line shape: `Partition:<n><SEP><key><SEP><value>`.
        let mut parts = line.splitn(3, SEP);
        let partition_part = parts.next().unwrap_or_else(|| {
            panic!(
                "consume line #{line_no} empty after split: {line:?}\nfull stderr: {}",
                String::from_utf8_lossy(&output.stderr),
            )
        });
        let key = parts.next().unwrap_or_else(|| {
            panic!(
                "consume line #{line_no} missing key field (first `\\x1F` separator not found): {line:?}\n\
                 full stderr: {}",
                String::from_utf8_lossy(&output.stderr),
            )
        });
        let value = parts.next().unwrap_or_else(|| {
            panic!(
                "consume line #{line_no} missing value field (second `\\x1F` separator not found): {line:?}\n\
                 full stderr: {}",
                String::from_utf8_lossy(&output.stderr),
            )
        });

        let partition_str = partition_part.strip_prefix("Partition:").unwrap_or_else(|| {
            panic!("consume line #{line_no} missing `Partition:` prefix: {partition_part:?} (full line: {line:?})",)
        });
        let partition: i32 = partition_str
            .parse()
            .unwrap_or_else(|e| panic!("consume line #{line_no}: bad partition {partition_str:?}: {e}"));

        records.push((partition, key.as_bytes().to_vec(), value.as_bytes().to_vec()));
    }

    assert_eq!(
        records.len(),
        max_messages,
        "kafka-console-consumer returned {} records, expected {max_messages} \
         (status: {:?}, timeout_ms: {timeout_ms}, stderr: {})",
        records.len(),
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    records
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
// Test 1b: 1000 records over SSL (Phase 9c.4 / folded-in Phase 8e)
// ---------------------------------------------------------------------------

/// Same flow as [`producer_smoke_plaintext_1000_records`] but over the
/// broker's SSL listener. Sends 1000 explicit-partition records and
/// asserts the same ack/shape/per-partition-monotonic-offset/multi-
/// partition-coverage contract — proving SSL plumbing on the producer
/// side wires through end-to-end at the same level of confidence as
/// the PLAINTEXT path.
///
/// **TLS configuration.** Uses the test cluster's self-signed CA cert
/// (`ctx.ca_cert_pem()`) written to a [`tempfile::NamedTempFile`]
/// kept alive for the entire test scope (Drop closes the file —
/// dropping the binding mid-test would invalidate the truststore
/// path). The broker hostname in `ssl_bootstrap_servers` is the same
/// localhost-loopback address the test container is bound to, so
/// `ssl.endpoint.identification.algorithm` stays at its default
/// `"https"` (Phase 9c.1 supports both `"https"` and `""`).
///
/// **Folds in Phase 8e**: PLAN.md:91-93 deferred standalone TLS
/// happy-path coverage to this phase. The PLAINTEXT path was Phase 8a,
/// SSL path is here. SASL_PLAINTEXT / SASL_SSL coverage follows in 9d
/// / 9e.
#[tokio::test(flavor = "multi_thread")]
async fn producer_smoke_ssl_1000_records() {
    use std::io::Write;

    init_logger();
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("smoke_1000_ssl");
    let bootstrap_servers = ctx.ssl_bootstrap_servers().to_string();

    // Write the test cluster's CA cert to a tempfile. Keep the
    // `NamedTempFile` binding alive for the whole test — dropping it
    // closes (and on Unix, deletes) the file, after which the
    // producer's truststore lookup at first connect would fail.
    let mut truststore_file = tempfile::NamedTempFile::new().expect("temp file");
    truststore_file.write_all(ctx.ca_cert_pem().as_bytes()).expect("write ca pem");
    truststore_file.flush().expect("flush ca pem");
    let truststore_path = truststore_file.path().to_str().expect("utf-8 path").to_owned();

    // Topic pre-creation via `docker exec kafka-topics`.
    let cluster = cluster_pool::get_or_create(&ClusterConfig::default()).await;
    create_topic(&cluster.container_ids()[0], &topic, TOPIC_PARTITIONS);

    // Build props inline — `build_props` is PLAINTEXT-only.
    let mut props: HashMap<String, String> = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers),
        ("acks".to_string(), "all".to_string()),
        ("linger.ms".to_string(), "10".to_string()),
        ("compression.type".to_string(), "none".to_string()),
        ("client.id".to_string(), "producer-smoke-test-ssl".to_string()),
        (
            "key.serializer".to_string(),
            "org.apache.kafka.common.serialization.ByteArraySerializer".to_string(),
        ),
        (
            "value.serializer".to_string(),
            "org.apache.kafka.common.serialization.ByteArraySerializer".to_string(),
        ),
        ("security.protocol".to_string(), "SSL".to_string()),
        ("ssl.truststore.location".to_string(), truststore_path),
        ("ssl.truststore.type".to_string(), "PEM".to_string()),
    ]);
    // The test broker's cert carries SANs for `localhost`, the
    // container hostname, AND IP `127.0.0.1`
    // (`tests/common/test_certs.rs:60-65`). `ssl_bootstrap_servers`
    // is `127.0.0.1:<port>`, so the SNI host is the IP literal and
    // rustls performs an IP-SAN match (SNI itself is omitted per
    // RFC 6066 §3 for IP literals). Default `https` endpoint-id
    // works. Keep it explicit for documentation.
    props.insert("ssl.endpoint.identification.algorithm".to_string(), "https".to_string());

    let key_ser: Box<dyn confluent_kafka::common::serialization::Serializer<Vec<u8>>> =
        Box::new(ByteArrayOwnedSerializer);
    let value_ser: Box<dyn confluent_kafka::common::serialization::Serializer<Vec<u8>>> =
        Box::new(ByteArrayOwnedSerializer);
    let producer = Arc::new(
        KafkaProducer::with_serializers(props, key_ser, value_ser).expect("KafkaProducer::with_serializers failed"),
    );

    // Identical send loop + assertions as the PLAINTEXT test above.
    let topic_arc: Arc<str> = Arc::from(topic.as_str());
    let mut futures = Vec::with_capacity(HAPPY_PATH_RECORDS);
    for i in 0..HAPPY_PATH_RECORDS {
        let topic = topic_arc.clone();
        let key = format!("k{i:04}").into_bytes();
        let value = format!("v{i:04}").into_bytes();
        let expected_partition = (i as i32) % TOPIC_PARTITIONS;
        let record = ProducerRecord::with_partition(topic, Some(expected_partition), Some(key), Some(value))
            .expect("ProducerRecord::with_partition");
        let fut = producer
            .send(record)
            .await
            .unwrap_or_else(|e| panic!("send #{i} enqueue failed: {e:?}"));
        futures.push(fut);
    }

    let results = futures_util::future::join_all(futures.into_iter().map(|f| async move { f.get().await })).await;
    let mut metadatas: Vec<RecordMetadata> = Vec::with_capacity(HAPPY_PATH_RECORDS);
    for (i, r) in results.into_iter().enumerate() {
        let meta = r.unwrap_or_else(|e| panic!("send #{i} broker ack failed: {e:?}"));
        metadatas.push(meta);
    }

    // (1) Ack count.
    assert_eq!(
        metadatas.len(),
        HAPPY_PATH_RECORDS,
        "expected {HAPPY_PATH_RECORDS} acked records, got {}",
        metadatas.len(),
    );

    // (2) + (3) RecordMetadata shape + partition consistency.
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

    // (4) Per-partition monotonic offsets.
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

    // (5) Multi-partition coverage.
    let partitions: HashSet<i32> = metadatas.iter().map(RecordMetadata::partition).collect();
    assert_eq!(
        partitions.len(),
        TOPIC_PARTITIONS as usize,
        "expected all {TOPIC_PARTITIONS} partitions to see traffic, got {} (partitions: {partitions:?})",
        partitions.len(),
    );

    let producer = Arc::into_inner(producer).expect("producer Arc had outstanding refs at close");
    producer
        .close_with_timeout(Duration::from_secs(30))
        .await
        .expect("graceful close failed");

    // Keep the truststore alive until after close — dropped here.
    drop(truststore_file);
}

// ---------------------------------------------------------------------------
// Test 1c: 1000 records over SASL_PLAINTEXT + PLAIN credentials (Phase 9d)
// ---------------------------------------------------------------------------

/// Same flow as [`producer_smoke_plaintext_1000_records`] but over the
/// broker's `SASL_PLAINTEXT` listener with the PLAIN mechanism and a
/// canonical `sasl.jaas.config` string. Sends 1000 explicit-partition
/// records and asserts the same ack/shape/per-partition-monotonic-
/// offset/multi-partition-coverage contract — proving the SASL handshake
/// + PLAIN credential exchange wired through Phase 9a (state machine) +
/// Phase 9b (channel + config plumbing) interoperate with the real
/// Apache Kafka 4.2 broker end-to-end.
///
/// **Java-runtime cross-verification of the SASL hex fixtures.** A
/// successful PLAIN handshake against the real Java 4.2 broker
/// empirically validates the 14 hand-derived hex fixtures captured in
/// `src/common/requests/sasl_*.rs` — the broker rejects malformed
/// SASL frames at the wire level, so byte-level divergence in any of
/// `SaslHandshake{Request,Response}` or `SaslAuthenticate{Request,
/// Response}` would surface as an authentication failure or connection
/// drop. Reaching ack #1000 means every byte the producer put on the
/// wire matched what the Java broker expected. This retires the Phase
/// 9.0 / 9b / 9c "awaiting Java-runtime byte capture" carry-over
/// (NOTES.md `Sub-phase 9.0 — closed`, `Fixture provenance note`).
///
/// **Listener port.** The test cluster's `SASL_PLAINTEXT` listener
/// binds container port `9095` (per `tests/common/kafka_cluster.rs:
/// SASL_PLAINTEXT_PORT`). PLAN.md and the original sub-phase ladder
/// referenced `9094` as a hypothetical port; the repo scaffolding is
/// the source of truth and the listener is on 9095. The
/// `ctx.sasl_plaintext_bootstrap_servers()` accessor returns the host-
/// mapped port automatically.
///
/// **Credentials.** Uses `SASL_USERNAME` + `SASL_PASSWORD` from
/// `tests/common/kafka_cluster.rs` (canonical broker-side credentials
/// configured in the test JAAS file). The `sasl.jaas.config` string is
/// composed inline using `PLAIN_LOGIN_MODULE`, mirroring the exact
/// shape `src/common/security/jaas_config.rs` parses (the canonical
/// Java source path — the `sasl.username` / `sasl.password` shortcut
/// is intentionally unit-pinned in Phase 9b, not retested here).
///
/// **References:** PLAN.md:381 (Phase 9d), `NOTES.md:49` (sub-phase
/// ladder), `NOTES.md:78` (multi-listener topology), CLAUDE.md "wire-
/// protocol byte-vector divergence" risk #1.
#[tokio::test(flavor = "multi_thread")]
async fn producer_smoke_sasl_plaintext_1000_records() {
    init_logger();
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("smoke_1000_sasl_plaintext");
    let bootstrap_servers = ctx.sasl_plaintext_bootstrap_servers().to_string();

    // Topic pre-creation via `docker exec kafka-topics`.
    let cluster = cluster_pool::get_or_create(&ClusterConfig::default()).await;
    create_topic(&cluster.container_ids()[0], &topic, TOPIC_PARTITIONS);

    // Compose the canonical Java `sasl.jaas.config` string for PLAIN.
    // Shape (exactly as `parse_plain_jaas_config` recognises):
    //   `<PLAIN_LOGIN_MODULE> required username="<u>" password="<p>";`
    // Imported from the SASL-side JAAS parser to guarantee parity.
    //
    // Phase 9d Round 2: exercised live against Apache Kafka 4.2 after
    // the `Selector::poll` readability-filter fix landed; see commit
    // `Phase 9d Round 2 fixup — selector readability filter Java parity`.
    let jaas_config = format!(
        r#"{module} required username="{user}" password="{pass}";"#,
        module = confluent_kafka::common::security::jaas_config::PLAIN_LOGIN_MODULE,
        user = crate::common::kafka_cluster::SASL_USERNAME,
        pass = crate::common::kafka_cluster::SASL_PASSWORD,
    );

    // Build props inline — `build_props` is PLAINTEXT-only; no
    // truststore plumbing (plaintext transport under SASL framing).
    let props: HashMap<String, String> = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers),
        ("acks".to_string(), "all".to_string()),
        ("linger.ms".to_string(), "10".to_string()),
        ("compression.type".to_string(), "none".to_string()),
        ("client.id".to_string(), "producer-smoke-test-sasl-plaintext".to_string()),
        (
            "key.serializer".to_string(),
            "org.apache.kafka.common.serialization.ByteArraySerializer".to_string(),
        ),
        (
            "value.serializer".to_string(),
            "org.apache.kafka.common.serialization.ByteArraySerializer".to_string(),
        ),
        ("security.protocol".to_string(), "SASL_PLAINTEXT".to_string()),
        ("sasl.mechanism".to_string(), "PLAIN".to_string()),
        ("sasl.jaas.config".to_string(), jaas_config),
    ]);

    let key_ser: Box<dyn confluent_kafka::common::serialization::Serializer<Vec<u8>>> =
        Box::new(ByteArrayOwnedSerializer);
    let value_ser: Box<dyn confluent_kafka::common::serialization::Serializer<Vec<u8>>> =
        Box::new(ByteArrayOwnedSerializer);
    let producer = Arc::new(
        KafkaProducer::with_serializers(props, key_ser, value_ser).expect("KafkaProducer::with_serializers failed"),
    );

    // Identical send loop + assertions as the PLAINTEXT/SSL tests above.
    let topic_arc: Arc<str> = Arc::from(topic.as_str());
    let mut futures = Vec::with_capacity(HAPPY_PATH_RECORDS);
    for i in 0..HAPPY_PATH_RECORDS {
        let topic = topic_arc.clone();
        let key = format!("k{i:04}").into_bytes();
        let value = format!("v{i:04}").into_bytes();
        let expected_partition = (i as i32) % TOPIC_PARTITIONS;
        let record = ProducerRecord::with_partition(topic, Some(expected_partition), Some(key), Some(value))
            .expect("ProducerRecord::with_partition");
        let fut = producer
            .send(record)
            .await
            .unwrap_or_else(|e| panic!("send #{i} enqueue failed: {e:?}"));
        futures.push(fut);
    }

    let results = futures_util::future::join_all(futures.into_iter().map(|f| async move { f.get().await })).await;
    let mut metadatas: Vec<RecordMetadata> = Vec::with_capacity(HAPPY_PATH_RECORDS);
    for (i, r) in results.into_iter().enumerate() {
        let meta = r.unwrap_or_else(|e| panic!("send #{i} broker ack failed: {e:?}"));
        metadatas.push(meta);
    }

    // (1) Ack count.
    assert_eq!(
        metadatas.len(),
        HAPPY_PATH_RECORDS,
        "expected {HAPPY_PATH_RECORDS} acked records, got {}",
        metadatas.len(),
    );

    // (2) + (3) RecordMetadata shape + partition consistency.
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

    // (4) Per-partition monotonic offsets.
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

    // (5) Multi-partition coverage.
    let partitions: HashSet<i32> = metadatas.iter().map(RecordMetadata::partition).collect();
    assert_eq!(
        partitions.len(),
        TOPIC_PARTITIONS as usize,
        "expected all {TOPIC_PARTITIONS} partitions to see traffic, got {} (partitions: {partitions:?})",
        partitions.len(),
    );

    let producer = Arc::into_inner(producer).expect("producer Arc had outstanding refs at close");
    producer
        .close_with_timeout(Duration::from_secs(30))
        .await
        .expect("graceful close failed");
}

// ---------------------------------------------------------------------------
// Test 1d: 1000 records over SASL_SSL + PLAIN credentials (Phase 9e)
// ---------------------------------------------------------------------------

/// Same flow as [`producer_smoke_plaintext_1000_records`] but over the
/// broker's `SASL_SSL` listener — TLS-encrypted transport with the PLAIN
/// SASL mechanism layered on top — sending 1000 explicit-partition
/// records and asserting the same ack/shape/per-partition-monotonic-
/// offset/multi-partition-coverage contract. This is the **combined**
/// security transport: TLS handshake (rustls) completes first, then the
/// SASL handshake + authenticate exchange runs over the now-encrypted
/// channel, before the channel becomes ready for Kafka API traffic.
///
/// **Empirical cross-verification of the combined-transport path.**
/// The two legs of this test are individually validated by:
/// - [`producer_smoke_ssl_1000_records`] (Phase 9c) — TLS-only path
///   with truststore + IP-SAN endpoint identification.
/// - [`producer_smoke_sasl_plaintext_1000_records`] (Phase 9d) — SASL
///   PLAIN handshake with JAAS-config credentials over plaintext.
///
/// Phase 9e proves they compose correctly **in sequence on the same
/// channel**, exercising
/// [`SaslChannelBuilder::build_sasl_ssl_channel`]
/// (`src/common/network/sasl_channel_builder.rs:164`) end-to-end against
/// the real Apache Kafka 4.2 broker. Reaching ack #1000 over SASL_SSL
/// means the same Rust producer that completed a TLS-only run can now
/// complete a TLS-then-SASL composed run, with byte-level wire
/// compatibility throughout (broker rejects malformed SASL or TLS
/// frames at the wire level — see also the Phase 9d rustdoc on hex
/// fixture cross-verification).
///
/// **Selector readability-filter coverage.** The SASL_SSL handshake
/// has *two* sequential mid-channel phases on a single connection:
/// (1) `SslTransportLayer` drives the rustls handshake until
/// `transport.ready() == true`; (2) `SaslClientAuthenticator` then runs
/// SaslHandshake + SaslAuthenticate exchanges until
/// `authenticator.complete() == true`. The Phase 9d Round 2 fix to
/// `Selector::poll`'s `wait_any_transport_readable` filter (now
/// `c.transport_layer_ref().is_open() && !c.is_muted()`, matching
/// Java's `OP_READ`-from-finishConnect-until-mute rule —
/// `Selector.java:525-548`, `KafkaChannel.java:252-269`) covers BOTH
/// phases by parity. Phase 9e empirically verifies that both phases
/// receive their broker-side reply bytes within the connection-setup
/// budget on the same channel without a poll-loop timeout starve.
///
/// **Listener port.** The test cluster's `SASL_SSL` listener binds
/// container port `9097` (per `tests/common/kafka_cluster.rs:
/// SASL_SSL_PORT`). The `ctx.sasl_ssl_bootstrap_servers()` accessor
/// returns the host-mapped port automatically.
///
/// **Truststore.** Same shape as the SSL-only test: the test cluster's
/// CA cert is written to a `tempfile::NamedTempFile` kept alive for the
/// whole test scope (dropping closes the file on Unix). The broker
/// cert carries `127.0.0.1` IP-SAN, so default `https` endpoint
/// identification works against the IP-literal bootstrap address; the
/// disabled-hostname-check variant is intentionally not retested here
/// because Phase 9c's `NoHostnameVerifier` unit tests already pin the
/// chain-validation behaviour under that mode and an integration test
/// would duplicate cluster-startup cost without adding new wire-level
/// evidence.
///
/// **Credentials.** Same canonical `sasl.jaas.config` string as
/// [`producer_smoke_sasl_plaintext_1000_records`] — composed inline
/// from [`confluent_kafka::common::security::jaas_config::PLAIN_LOGIN_MODULE`]
/// + the broker-side `SASL_USERNAME` / `SASL_PASSWORD` constants in
/// `tests/common/kafka_cluster.rs`. The `sasl.username` /
/// `sasl.password` shortcut is unit-pinned in Phase 9b and not
/// retested here.
///
/// **References:** PLAN.md (Phase 9e), `NOTES.md:50` (sub-phase
/// ladder), `NOTES.md` (Phase 9d Round 2 — selector readability
/// filter fix), CLAUDE.md "wire-protocol byte-vector divergence"
/// risk #1.
#[tokio::test(flavor = "multi_thread")]
async fn producer_smoke_sasl_ssl_1000_records() {
    use std::io::Write;

    init_logger();
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("smoke_1000_sasl_ssl");
    let bootstrap_servers = ctx.sasl_ssl_bootstrap_servers().to_string();

    // Write the test cluster's CA cert to a tempfile. Keep the
    // `NamedTempFile` binding alive for the whole test — dropping it
    // closes (and on Unix, deletes) the file, after which the
    // producer's truststore lookup at first connect would fail. Same
    // shape as `producer_smoke_ssl_1000_records`.
    let mut truststore_file = tempfile::NamedTempFile::new().expect("temp file");
    truststore_file.write_all(ctx.ca_cert_pem().as_bytes()).expect("write ca pem");
    truststore_file.flush().expect("flush ca pem");
    let truststore_path = truststore_file.path().to_str().expect("utf-8 path").to_owned();

    // Topic pre-creation via `docker exec kafka-topics`.
    let cluster = cluster_pool::get_or_create(&ClusterConfig::default()).await;
    create_topic(&cluster.container_ids()[0], &topic, TOPIC_PARTITIONS);

    // Compose the canonical Java `sasl.jaas.config` string for PLAIN.
    // Shape (exactly as `parse_plain_jaas_config` recognises):
    //   `<PLAIN_LOGIN_MODULE> required username="<u>" password="<p>";`
    // Imported from the SASL-side JAAS parser to guarantee parity.
    let jaas_config = format!(
        r#"{module} required username="{user}" password="{pass}";"#,
        module = confluent_kafka::common::security::jaas_config::PLAIN_LOGIN_MODULE,
        user = crate::common::kafka_cluster::SASL_USERNAME,
        pass = crate::common::kafka_cluster::SASL_PASSWORD,
    );

    // Build props inline — `build_props` is PLAINTEXT-only. Combines
    // the truststore plumbing from the SSL test with the SASL/JAAS
    // plumbing from the SASL_PLAINTEXT test.
    let mut props: HashMap<String, String> = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers),
        ("acks".to_string(), "all".to_string()),
        ("linger.ms".to_string(), "10".to_string()),
        ("compression.type".to_string(), "none".to_string()),
        ("client.id".to_string(), "producer-smoke-test-sasl-ssl".to_string()),
        (
            "key.serializer".to_string(),
            "org.apache.kafka.common.serialization.ByteArraySerializer".to_string(),
        ),
        (
            "value.serializer".to_string(),
            "org.apache.kafka.common.serialization.ByteArraySerializer".to_string(),
        ),
        ("security.protocol".to_string(), "SASL_SSL".to_string()),
        ("ssl.truststore.location".to_string(), truststore_path),
        ("ssl.truststore.type".to_string(), "PEM".to_string()),
        ("sasl.mechanism".to_string(), "PLAIN".to_string()),
        ("sasl.jaas.config".to_string(), jaas_config),
    ]);
    // Broker cert carries `127.0.0.1` IP-SAN; default `https`
    // endpoint-id works against the IP-literal bootstrap address.
    // Kept explicit for documentation parity with the SSL test.
    props.insert("ssl.endpoint.identification.algorithm".to_string(), "https".to_string());

    let key_ser: Box<dyn confluent_kafka::common::serialization::Serializer<Vec<u8>>> =
        Box::new(ByteArrayOwnedSerializer);
    let value_ser: Box<dyn confluent_kafka::common::serialization::Serializer<Vec<u8>>> =
        Box::new(ByteArrayOwnedSerializer);
    let producer = Arc::new(
        KafkaProducer::with_serializers(props, key_ser, value_ser).expect("KafkaProducer::with_serializers failed"),
    );

    // Identical send loop + assertions as the PLAINTEXT/SSL/SASL_PLAINTEXT
    // tests above.
    let topic_arc: Arc<str> = Arc::from(topic.as_str());
    let mut futures = Vec::with_capacity(HAPPY_PATH_RECORDS);
    for i in 0..HAPPY_PATH_RECORDS {
        let topic = topic_arc.clone();
        let key = format!("k{i:04}").into_bytes();
        let value = format!("v{i:04}").into_bytes();
        let expected_partition = (i as i32) % TOPIC_PARTITIONS;
        let record = ProducerRecord::with_partition(topic, Some(expected_partition), Some(key), Some(value))
            .expect("ProducerRecord::with_partition");
        let fut = producer
            .send(record)
            .await
            .unwrap_or_else(|e| panic!("send #{i} enqueue failed: {e:?}"));
        futures.push(fut);
    }

    let results = futures_util::future::join_all(futures.into_iter().map(|f| async move { f.get().await })).await;
    let mut metadatas: Vec<RecordMetadata> = Vec::with_capacity(HAPPY_PATH_RECORDS);
    for (i, r) in results.into_iter().enumerate() {
        let meta = r.unwrap_or_else(|e| panic!("send #{i} broker ack failed: {e:?}"));
        metadatas.push(meta);
    }

    // (1) Ack count.
    assert_eq!(
        metadatas.len(),
        HAPPY_PATH_RECORDS,
        "expected {HAPPY_PATH_RECORDS} acked records, got {}",
        metadatas.len(),
    );

    // (2) + (3) RecordMetadata shape + partition consistency.
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

    // (4) Per-partition monotonic offsets.
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

    // (5) Multi-partition coverage.
    let partitions: HashSet<i32> = metadatas.iter().map(RecordMetadata::partition).collect();
    assert_eq!(
        partitions.len(),
        TOPIC_PARTITIONS as usize,
        "expected all {TOPIC_PARTITIONS} partitions to see traffic, got {} (partitions: {partitions:?})",
        partitions.len(),
    );

    let producer = Arc::into_inner(producer).expect("producer Arc had outstanding refs at close");
    producer
        .close_with_timeout(Duration::from_secs(30))
        .await
        .expect("graceful close failed");

    // Keep the truststore alive until after close — dropped here.
    drop(truststore_file);
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

// ---------------------------------------------------------------------------
// Test 5: end-to-end byte fidelity via kafka-console-consumer (Phase 8c)
// ---------------------------------------------------------------------------

/// Closes the producer-side zero-copy guarantee at the broker
/// boundary: drives `BYTE_FIDELITY_RECORDS` records through a real
/// broker over PLAINTEXT, then reads them back via `docker exec
/// kafka-console-consumer` and asserts the (key, value) byte sequence
/// observed on the consume side equals the sequence produced — per
/// partition.
///
/// **Contract under test**: CLAUDE.md §12 demands the bytes the user
/// hands to `ProducerRecord` reach the wire unmodified ("no
/// intermediate copy buffers, no batch-finalization copies, no
/// `IoSlice` framing-header / payload mixing"). Production-side
/// audits in Phases 6b/6d/7g checked that the bytes flow through the
/// accumulator and the network send path without being touched. This
/// test audits the **end-to-end** assertion: the bytes the broker
/// writes to its log are identical to the bytes the producer
/// serialized. We do not assert anything about the wire bytes
/// themselves (that is Phase 2d/3e's fixture coverage); we assert
/// the broker's log content matches.
///
/// # Design
///
/// 1. **Explicit-partition send**, record `i` → partition
///    `i % TOPIC_PARTITIONS`. This is identical to test 1 — keeps
///    the per-partition send order deterministic at the test side.
///    Auto-partition would route the sticky partitioner's choice,
///    leaving the per-partition byte sequence dependent on which
///    partition got the sticky burst.
/// 2. **Await all acks** via `KafkaFuture::get()`, then **close
///    gracefully** so any remaining batches drain to the broker
///    before we consume.
/// 3. **Consume `BYTE_FIDELITY_RECORDS` records** via the
///    `consume_records` helper. Returns `(partition, key, value)`
///    in console-consumer output order. Group by partition.
/// 4. **Group expected records by partition** using the partition
///    reported in each `RecordMetadata` ack — NOT the
///    explicit-partition input. The ack's partition is what the
///    broker recorded; the explicit partition is what the producer
///    asked for. In practice they match because Java's
///    `KafkaProducer.partition()` honours explicit partitions and
///    the broker writes to that exact partition, but using the ack
///    partition as the grouping key is the only correct choice if
///    those ever diverged (it is a per-partition byte-fidelity
///    claim, not a partition-routing claim).
/// 5. **Compare**: for each partition, the consume-side
///    (key, value) sequence in offset order must equal the
///    produce-side (key, value) sequence in the order the producer
///    enqueued records into that partition.
///
/// # `kafka-console-consumer` ordering note
///
/// The console consumer reads each partition with a single thread
/// in offset order, but it interleaves across partitions in
/// whatever order data arrives at the consumer's poll. We therefore
/// **cannot** rely on the top-level output order — only on
/// per-partition order. The helper preserves the printed order;
/// grouping by partition recovers per-partition offset order.
///
/// # Java parity
///
/// Java's `KafkaProducerTest` does not include an exact analog —
/// most of its end-to-end coverage runs through embedded test
/// brokers and asserts producer behaviour rather than byte
/// fidelity. The closest parallel is
/// `ProducerSendWhileDeletionTest` / `TransactionsTest`, which
/// roundtrip records and assert content equality without going
/// through the wire format explicitly. This test fills that gap on
/// the Rust side: it pins the zero-copy contract that the Java
/// codebase enforces by construction (JIT-friendly heap layout
/// that hides identifier clones, JVM GC that absorbs intermediate
/// buffers) but Rust makes explicit by code review.
#[tokio::test(flavor = "multi_thread")]
async fn producer_smoke_plaintext_byte_fidelity() {
    init_logger();
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("smoke_byte_fidelity");
    let bootstrap_servers = ctx.bootstrap_servers().to_string();

    let cluster = cluster_pool::get_or_create(&ClusterConfig::default()).await;
    create_topic(&cluster.container_ids()[0], &topic, TOPIC_PARTITIONS);

    let producer = Arc::new(build_producer!(&bootstrap_servers));
    let topic_arc: Arc<str> = Arc::from(topic.as_str());

    // Build the input set: record `i` → partition `i %
    // TOPIC_PARTITIONS`, key `k{i:04}`, value `v{i:04}` (pure
    // ASCII — see `consume_records` rustdoc on the ASCII-only
    // constraint of the kafka-console-consumer string-deserializer
    // path used here).
    let mut input: Vec<(i32, Vec<u8>, Vec<u8>)> = Vec::with_capacity(BYTE_FIDELITY_RECORDS);
    for i in 0..BYTE_FIDELITY_RECORDS {
        let expected_partition = (i as i32) % TOPIC_PARTITIONS;
        let key = format!("k{i:04}").into_bytes();
        let value = format!("v{i:04}").into_bytes();
        input.push((expected_partition, key, value));
    }

    // Send sequentially — keeps per-partition enqueue order
    // deterministic (same rationale as test 1's loop).
    let mut futures = Vec::with_capacity(BYTE_FIDELITY_RECORDS);
    for (i, (partition, key, value)) in input.iter().enumerate() {
        let topic = topic_arc.clone();
        let record = ProducerRecord::with_partition(topic, Some(*partition), Some(key.clone()), Some(value.clone()))
            .expect("ProducerRecord::with_partition");
        let fut = producer
            .send(record)
            .await
            .unwrap_or_else(|e| panic!("send #{i} enqueue failed: {e:?}"));
        futures.push(fut);
    }

    // Await every broker ack.
    let results = futures_util::future::join_all(futures.into_iter().map(|f| async move { f.get().await })).await;
    let mut metadatas: Vec<RecordMetadata> = Vec::with_capacity(BYTE_FIDELITY_RECORDS);
    for (i, r) in results.into_iter().enumerate() {
        let meta = r.unwrap_or_else(|e| panic!("send #{i} broker ack failed: {e:?}"));
        metadatas.push(meta);
    }
    assert_eq!(
        metadatas.len(),
        BYTE_FIDELITY_RECORDS,
        "expected {BYTE_FIDELITY_RECORDS} acks, got {}",
        metadatas.len(),
    );

    // Close gracefully so any straggler batches drain to the broker
    // before we invoke the consumer. This is belt-and-braces — we
    // already awaited every ack, so the broker has every record
    // committed before this line; but a future regression that
    // returned acks early (e.g. acks=1 with linger after the ack)
    // would be caught here.
    //
    // `Arc::into_inner` returns `Some(T)` iff `self` is the **last**
    // outstanding strong reference (which it is — `producer` is the
    // sole binding in this scope; no other clones were made). No
    // clone-then-drop dance is needed.
    let producer = Arc::into_inner(producer).expect("producer Arc had outstanding refs at close");
    producer
        .close_with_timeout(Duration::from_secs(30))
        .await
        .expect("graceful close failed");

    // Now consume the records back. Run inside `spawn_blocking`
    // because `consume_records` blocks on `docker exec` which can
    // take up to `timeout_ms`. 30s is the consume-side timeout —
    // generous, since 100 records on localhost finish in well under
    // a second. The container id is moved into the closure to keep
    // the await point clean of cross-thread borrow obligations.
    let container_id = cluster.container_ids()[0].clone();
    let consume_topic = topic.clone();
    let consumed: Vec<(i32, Vec<u8>, Vec<u8>)> = tokio::task::spawn_blocking(move || {
        consume_records(&container_id, &consume_topic, BYTE_FIDELITY_RECORDS, 30_000)
    })
    .await
    .expect("spawn_blocking(consume_records) panicked");
    assert_eq!(
        consumed.len(),
        BYTE_FIDELITY_RECORDS,
        "expected {BYTE_FIDELITY_RECORDS} consumed records, got {}",
        consumed.len(),
    );

    // Group expected records by the partition the broker
    // acknowledged. Within each partition the enqueue order at the
    // test side equals the offset order at the broker (test 1
    // proves per-partition strict-monotonic offsets), so iterating
    // `metadatas` in index order and grouping yields the
    // produce-side (key, value) sequence per partition.
    let mut expected_by_partition: HashMap<i32, Vec<(Vec<u8>, Vec<u8>)>> = HashMap::new();
    for (i, m) in metadatas.iter().enumerate() {
        let (_input_partition, key, value) = &input[i];
        expected_by_partition
            .entry(m.partition())
            .or_default()
            .push((key.clone(), value.clone()));
    }

    // Group consumed records by partition. The console consumer
    // already prints per-partition in offset order, so the per-
    // partition `Vec` preserves the broker's stored order.
    let mut consumed_by_partition: HashMap<i32, Vec<(Vec<u8>, Vec<u8>)>> = HashMap::new();
    for (partition, key, value) in consumed {
        consumed_by_partition.entry(partition).or_default().push((key, value));
    }

    // Both grouping maps must cover the same partition set.
    let expected_partitions: HashSet<i32> = expected_by_partition.keys().copied().collect();
    let consumed_partitions: HashSet<i32> = consumed_by_partition.keys().copied().collect();
    assert_eq!(
        expected_partitions, consumed_partitions,
        "partition sets diverge — expected: {expected_partitions:?}, consumed: {consumed_partitions:?}",
    );

    // Per-partition byte-by-byte equality. Asserts the producer-
    // side zero-copy guarantee at the broker boundary: the bytes
    // the broker stored in its log equal the bytes the producer
    // serialized. Any divergence here points at:
    //   - serialization (unlikely — `ByteArrayOwnedSerializer` is
    //     identity; tested in unit suite)
    //   - accumulator copy (Phase 6d/6b)
    //   - wire framing (Phase 2d, fixture-tested)
    //   - broker-side decoding (out of scope — would be a Kafka
    //     broker bug)
    for (partition, expected) in &expected_by_partition {
        let consumed = consumed_by_partition
            .get(partition)
            .unwrap_or_else(|| panic!("partition {partition} missing from consumed"));
        assert_eq!(
            consumed.len(),
            expected.len(),
            "partition {partition}: record-count mismatch — expected {} produced records, got {} consumed",
            expected.len(),
            consumed.len(),
        );
        for (i, ((exp_key, exp_value), (act_key, act_value))) in expected.iter().zip(consumed.iter()).enumerate() {
            assert_eq!(
                act_key, exp_key,
                "partition {partition} record #{i}: key bytes diverge (expected {exp_key:?}, got {act_key:?})",
            );
            assert_eq!(
                act_value, exp_value,
                "partition {partition} record #{i}: value bytes diverge (expected {exp_value:?}, got {act_value:?})",
            );
        }
    }
}
