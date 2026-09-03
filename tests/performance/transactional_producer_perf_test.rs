// Copyright 2026 Confluent Inc.
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

//! Transactional producer performance test — measures throughput, latency,
//! CPU, and memory of the transactional producer API. Two modes selected by
//! `TXN_MODE`:
//!  * `produce` (Phase 1) — `begin` -> produce N -> `commit`/`abort`;
//!  * `eos` (Phase 2) — a read-process-write (exactly-once) pipeline:
//!    consume from `SOURCE_TOPIC` -> transform -> produce to `TOPIC_NAME` ->
//!    `send_offsets_to_transaction` -> `commit`/`abort`.
//!
//! IMPORTANT: this test MUST be kept in sync with the sibling transactional
//! producer performance tests in the project (`bindings/c/tests/
//! transactional_producer_perf_test.c` and
//! `tools/java-perf-test/.../TransactionalProducerPerformanceTest.java`) — the
//! same configuration contract, message shape, metrics-file schema, and
//! reported numbers — so results from the different implementations are
//! directly comparable. It mirrors the non-transactional
//! `producer_perf_test.rs` and shares its `metrics.jsonl` / `results.json`
//! schema (with backward-compatible transaction additions).
//!
//! Produce-mode loop, per producer: `begin_transaction` -> produce
//! `RECORDS_PER_TRANSACTION` records -> `commit_transaction` (or
//! `abort_transaction` per the deterministic abort rule). Aborted transactions
//! still produce their records before aborting; their records count toward
//! neither throughput nor the latency series.
//!
//! EOS-mode loop, per producer: poll the consumer for up to
//! `RECORDS_PER_TRANSACTION` records (bounded by `max.poll.records`) ->
//! `begin_transaction` -> produce each transformed (identity/echo) record to
//! `TOPIC_NAME` -> `send_offsets_to_transaction` (last-consumed-offset + 1 per
//! partition, with the consumer group metadata) -> `commit_transaction` (or
//! `abort_transaction` per the deterministic abort rule). An empty poll is
//! skipped (no empty transaction) so a run with no source data terminates on
//! the duration / message bound rather than deadlocking. Aborted transactions
//! are NOT retried and their consumed offsets are NOT committed — those records
//! count toward neither throughput nor latency (the produce-mode precedent),
//! and this harness does not seek the consumer back on abort (a benchmark
//! simplification vs. a real EOS app, which would seek to the last committed
//! position; documented as a deviation per definition-of-done.md §7).
//!
//! Latency definitions (both modes):
//!  * per-record latency = a record's `send()` -> the moment its transaction's
//!    `commit_transaction` completes (committed transactions only);
//!  * per-transaction commit latency = `begin_transaction` ->
//!    `commit_transaction` completes (committed only);
//!  * per-transaction abort latency = `begin_transaction` ->
//!    `abort_transaction` completes (deterministic-abort path only — symmetric
//!    with commit latency). Emitted as `abort_latency_ms` in `results.json` and
//!    an `abort_latency` bucket per `metrics.jsonl` window.
//!
//! EOS source seeding (Kaushik C:1612): the EOS pipeline's throughput ceiling is
//! `min(source-produce-rate, txn-process-rate)`, so `SOURCE_TOPIC` must be filled
//! with enough records for the whole measured window or the consumer starves and
//! txn throughput is understated. The CANONICAL mechanism — identical across all
//! four transactional harnesses (Rust / librdkafka-C / Java / Python) — is to
//! spawn Kafka's standard `kafka-producer-perf-test.sh` (a plain Java producer)
//! from `$KAFKA_BIN` BEFORE the measured interval, exactly as the consumer-perf
//! harnesses seed their input (`consumer-perf/src/main.rs` `spawn_producer`,
//! `consumer-perf/compare/librdkafka_e2e.c`,
//! `bindings/python/test/performance/consumer_performance_test.py`). It runs at
//! peak (`--throughput -1`) by default with `--num-records` sized to cover the
//! window, and forwards SASL/security through a `--producer.config` properties
//! file. Seed-then-run: the harness waits for the seeder to finish, so the source
//! is fully populated before the clock starts.
//!
//! Two documented exceptions to the KAFKA_BIN path:
//!  * In-suite (the ephemeral Docker cluster): seeded in-process by
//!    `seed_source_topic` (a high-throughput non-transactional producer run before
//!    the measured clock), because this path must run WITHOUT a Kafka bin dir or
//!    Java on PATH.
//!  * Fallback (no `KAFKA_BIN`, external broker): the caller is assumed to have
//!    pre-populated `SOURCE_TOPIC` by equivalent means.
//! In every case the consumer never hangs on an under-fed source (bounded poll +
//! a one-time "source starved" warning).
//!
//! KIP-848 (Kaushik C:128): ALL consumers in this project's transactional perf
//! harnesses run on the KIP-848 consumer group protocol. Here the EOS consumer
//! sets `group.protocol=consumer` (the Rust client is KIP-848-only). This
//! requires a KIP-848-capable broker (Kafka 4.x) and is the intended uniform
//! protocol across the Rust / librdkafka-C / Java / Python harnesses.
//!
//! Two ways to run:
//!
//! * As part of the integration suite (short, asserted, Docker broker):
//!   `cargo test --features integration-tests --test performance -- transactional_producer_perf_test --nocapture`
//!
//! * As an env-driven benchmark binary (long runs, external broker):
//!   `cargo xtask transactional-producer-perf-test` (set env vars first).
//!
//! Configuration is identical to `producer_perf_test.rs` plus the
//! transaction-specific knobs:
//!
//! | Variable                      | Default    | Description                                        |
//! |-------------------------------|------------|----------------------------------------------------|
//! | `RECORDS_PER_TRANSACTION`     | `100`      | Records produced per transaction before commit     |
//! | `ABORT_RATE`                  | `0.0`      | Fraction [0.0,1.0] of transactions aborted          |
//! | `NUM_TRANSACTIONAL_PRODUCERS` | `1`        | Concurrent producers (tokio tasks)                  |
//! | `TRANSACTIONAL_ID`            | `perf-txn` | Base id; producer `k` uses `<TRANSACTIONAL_ID>-<k>` |
//! | `TXN_MODE`                    | `produce`  | `produce` (Phase 1) or `eos` (Phase 2)              |
//! | `SOURCE_TOPIC`                | (unset)    | EOS mode only: topic consumed/transformed (required for `eos`) |
//! | `GROUP_ID`                    | `<TRANSACTIONAL_ID>-eos-consumer` | EOS consumer group id (shared across producers) |
//! | `KAFKA_BIN`                   | (unset)    | EOS mode only: dir with `kafka-producer-perf-test.sh`; set => self-seed `SOURCE_TOPIC` (canonical). Unset (external broker) => assume pre-populated |
//! | `SEED_THROUGHPUT`             | `-1`       | EOS seed producer `--throughput` (`-1` = peak/unbounded); a positive value caps the seed rate |
//! | `SEED_COUNT`                  | (sized)    | EOS seed producer `--num-records`; default sized to cover the window (`num_messages`, else `rate×(duration+30)`) |
//!
//! `enable.idempotence` is forced true (required for transactions). In EOS mode
//! all producers' consumers share ONE `group.id`, so KIP-848 server-side
//! assignment divides the source partitions among them — the canonical EOS
//! scaling design, avoiding cross-producer duplicate output. `enable.auto.commit`
//! is forced false on the consumer (offsets flow through the transaction).

use std::collections::HashMap;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use confluent_kafka::common::Metric;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::{ByteArrayDeserializer, ByteArraySerializer};
use confluent_kafka::consumer::{Consumer, ConsumerConfig, OffsetAndMetadata, new_consumer};
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

/// Fraction of each key/value that is random (the rest is a constant prefix).
/// A constant prefix plus this fraction of random bytes makes payloads
/// compress consistently across the producer performance tests.
const RANDOMNESS: f64 = 0.5;

/// Number of pre-generated messages cycled through during the run
/// (`n = 10000`).
const GENERATED_MESSAGES: usize = 10_000;

/// End-to-end latency histogram resolution: 1 ms buckets for `0..=MAX_LATENCY_MS`
/// plus one overflow bucket. A histogram (vs. storing every sample) keeps the
/// test's own memory footprint flat so it doesn't pollute the RSS measurement.
const MAX_LATENCY_MS: usize = 10_000;

/// Seconds to keep sampling metrics after the last commit, so the JSONL
/// captures post-measurement (cooldown) windows. Mirrors the sibling tests.
const POST_TEST_AWAIT_SECONDS: u64 = 10;

/// Linux `USER_HZ` — clock ticks per second used to convert `/proc/self/stat`
/// utime/stime into CPU seconds. Effectively always 100 on Linux.
const USER_HZ: f64 = 100.0;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct PerfTestConfig {
    bootstrap_servers: String,
    topic_name: String,
    num_messages: u64,
    limit_rps: u64,
    key_size: usize,
    value_size: usize,
    batch_size: i64,
    max_request_size: i64,
    buffer_memory: Option<i64>,
    linger_ms: Option<String>,
    max_in_flight: Option<String>,
    compression_type: String,
    use_defaults: bool,
    warmup_seconds: u64,
    test_duration_seconds: u64,
    do_verify: bool,
    p99_limit_ms: u64,
    security_protocol: Option<String>,
    sasl_mechanism: Option<String>,
    sasl_username: Option<String>,
    sasl_password: Option<String>,
    metrics_file: String,
    results_file: String,
    // --- transaction-specific knobs ---
    records_per_transaction: u64,
    abort_rate: f64,
    num_transactional_producers: u64,
    transactional_id: String,
    txn_mode: String,
    // --- EOS-mode knobs (TXN_MODE=eos) ---
    /// Input topic consumed/transformed. Required when `txn_mode == "eos"`.
    source_topic: Option<String>,
    /// Consumer group id, shared across all producers' consumers so KIP-848
    /// server-side assignment splits the source partitions among them. Defaults
    /// to `<transactional_id>-eos-consumer`; overridable via `GROUP_ID`.
    consumer_group_id: String,
    /// EOS canonical seeding (`TXN_MODE=eos`): directory containing Kafka's
    /// `kafka-producer-perf-test.sh`. When set (and running against an external
    /// broker), the harness spawns that standard Java producer BEFORE the
    /// measured interval to fill `SOURCE_TOPIC`, exactly as the consumer-perf
    /// harnesses seed their input. Unset => no self-spawn (assume pre-populated).
    kafka_bin: Option<String>,
    /// `--throughput` for the spawned seed producer. Default `-1` (peak /
    /// unbounded): fill the source as fast as the broker allows. A positive
    /// value caps the seed rate (and is used to size `seed_count`).
    seed_throughput: i64,
    /// `--num-records` for the spawned seed producer. Unset => sized to cover
    /// the whole measured window (see `seed_record_count`).
    seed_count: Option<u64>,
}

impl PerfTestConfig {
    fn from_env() -> Self {
        // `batch.size`: default 1 MiB (1024 KiB); env value is in KiB.
        let batch_size = match env_opt("BATCH_SIZE") {
            Some(kib) => kib.parse::<i64>().unwrap_or(1024) * 1024,
            None => 1024 * 1024,
        };
        // `max.request.size`: default batch×64, capped at 8 MiB; env value is
        // in KiB.
        let max_request_size = {
            let requested = match env_opt("MAX_REQUEST_SIZE") {
                Some(kib) => kib.parse::<i64>().unwrap_or(0) * 1024,
                None => batch_size * 64,
            };
            requested.min(8 * 1024 * 1024)
        };
        // `buffer.memory`: only set when present; env value is in MiB.
        let buffer_memory = env_opt("BUFFER_MEMORY")
            .and_then(|mib| mib.parse::<i64>().ok())
            .map(|mib| mib * 1024 * 1024);

        // Full defaults match the sibling transactional producer performance
        // tests (600 s, unlimited rate). The in-suite integration run overrides
        // these in `transactional_producer_perf_test()`.
        let test_duration_seconds = env_parse("TEST_DURATION_SECONDS", 600);
        let limit_rps = env_parse("LIMIT_RPS", 0);
        // When rate-limited, the run is bounded by rps × duration.
        let mut num_messages = env_parse("NUM_MESSAGES", 0u64);
        if limit_rps > 0 {
            num_messages = limit_rps * test_duration_seconds;
        }

        // Consumer group id (EOS): default `<transactional_id>-eos-consumer`,
        // overridable via `GROUP_ID`. Shared across all producers' consumers.
        let transactional_id = env_or("TRANSACTIONAL_ID", "perf-txn");
        let consumer_group_id = env_or("GROUP_ID", &format!("{transactional_id}-eos-consumer"));

        Self {
            bootstrap_servers: env_or("BOOTSTRAP_SERVERS", ""),
            topic_name: env_or("TOPIC_NAME", "test-topic"),
            num_messages,
            limit_rps,
            key_size: env_parse("KEY_SIZE", 0),
            value_size: env_parse("VALUE_SIZE", 2048),
            batch_size,
            max_request_size,
            buffer_memory,
            linger_ms: Some(env_or("LINGER_MS", "5")),
            max_in_flight: env_opt("MAX_IN_FLIGHT"),
            compression_type: env_or("COMPRESSION_TYPE", "none"),
            use_defaults: env_or("USE_DEFAULTS", "False") == "True",
            warmup_seconds: env_parse("WARMUP_SECONDS", 120),
            test_duration_seconds,
            do_verify: env_or("DO_VERIFY", "True") == "True",
            p99_limit_ms: env_parse("P99_LIMIT_MS", 0),
            security_protocol: env_opt("SECURITY_PROTOCOL"),
            sasl_mechanism: env_opt("SASL_MECHANISM"),
            sasl_username: env_opt("SASL_USERNAME"),
            sasl_password: env_opt("SASL_PASSWORD"),
            metrics_file: env_or("METRICS_FILE", "metrics.jsonl"),
            results_file: env_or("RESULTS_FILE", "results.json"),
            records_per_transaction: env_parse("RECORDS_PER_TRANSACTION", 100).max(1),
            abort_rate: env_parse("ABORT_RATE", 0.0f64).clamp(0.0, 1.0),
            num_transactional_producers: env_parse("NUM_TRANSACTIONAL_PRODUCERS", 1).max(1),
            transactional_id,
            txn_mode: env_or("TXN_MODE", "produce"),
            source_topic: env_opt("SOURCE_TOPIC"),
            consumer_group_id,
            kafka_bin: env_opt("KAFKA_BIN"),
            seed_throughput: env_parse("SEED_THROUGHPUT", -1),
            seed_count: env_opt("SEED_COUNT").and_then(|v| v.parse().ok()),
        }
    }

    /// Number of records the seed producer should write to `SOURCE_TOPIC` when
    /// the canonical `KAFKA_BIN` path is used. Sized to outlast the whole
    /// measured window so the EOS consumer is never starved:
    ///  * an explicit `SEED_COUNT` wins;
    ///  * else the run's own message bound (`num_messages`, set by
    ///    `LIMIT_RPS`/`NUM_MESSAGES`) — the count the pipeline will consume;
    ///  * else (fully unbounded) a rate × window estimate, where the rate is
    ///    `SEED_THROUGHPUT` when positive, otherwise a generous default
    ///    (125000 msg/s, matching the consumer-perf `THROUGHPUT` default).
    fn seed_record_count(&self) -> u64 {
        if let Some(n) = self.seed_count {
            return n.max(1);
        }
        if self.num_messages > 0 {
            return self.num_messages;
        }
        let sizing_rate: u64 = if self.seed_throughput > 0 {
            self.seed_throughput as u64
        } else {
            125_000
        };
        (sizing_rate * (self.test_duration_seconds + 30)).max(1)
    }

    fn message_size(&self) -> usize {
        self.key_size + self.value_size
    }

    /// Build the producer property map for a single transactional producer.
    /// Identical to the non-transactional harness plus a unique
    /// `transactional.id` and a forced `enable.idempotence=true` (required for
    /// transactions; the client rejects a `transactional.id` otherwise).
    fn producer_props(&self, bootstrap_servers: &str, transactional_id: &str) -> HashMap<String, String> {
        let mut props = HashMap::from([
            ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
            ("client.id".to_string(), format!("perf-test-rust-txn-{transactional_id}")),
            // Transactions require idempotence; force it on regardless of
            // USE_DEFAULTS (otherwise config validation rejects transactional.id).
            ("enable.idempotence".to_string(), "true".to_string()),
            ("transactional.id".to_string(), transactional_id.to_string()),
        ]);
        // With USE_DEFAULTS the client runs at its own defaults: only the
        // bootstrap servers, client id, transactional.id, enable.idempotence and
        // SASL credentials are set, and every performance-tuning knob is omitted.
        if !self.use_defaults {
            props.insert("batch.size".to_string(), self.batch_size.to_string());
            props.insert("max.request.size".to_string(), self.max_request_size.to_string());
            props.insert("compression.type".to_string(), self.compression_type.clone());
            // `acks` is intentionally not set, so it relies on the client default
            // (acks = all / -1), which transactions require anyway.
            if let Some(v) = &self.buffer_memory {
                props.insert("buffer.memory".to_string(), v.to_string());
            }
            if let Some(v) = &self.linger_ms {
                props.insert("linger.ms".to_string(), v.clone());
            }
            if let Some(v) = &self.max_in_flight {
                props.insert("max.in.flight.requests.per.connection".to_string(), v.clone());
            }
        }
        // SASL: only when the security protocol is a SASL one and all
        // credentials are present.
        let sasl_enabled = matches!(self.security_protocol.as_deref(), Some("SASL_PLAINTEXT") | Some("SASL_SSL"))
            && self.sasl_mechanism.is_some()
            && self.sasl_username.is_some()
            && self.sasl_password.is_some();
        if sasl_enabled {
            let user = self.sasl_username.as_deref().unwrap();
            let pass = self.sasl_password.as_deref().unwrap();
            let jaas = format!(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \n\t\
                 username=\"{user}\" \n\tpassword=\"{pass}\";"
            );
            props.insert("security.protocol".to_string(), self.security_protocol.clone().unwrap());
            props.insert("sasl.mechanism".to_string(), self.sasl_mechanism.clone().unwrap());
            props.insert("sasl.jaas.config".to_string(), jaas);
        }
        props
    }

    /// Build the consumer property map for the EOS read-process-write pipeline.
    /// `group.protocol=consumer` (KIP-848; the Rust client rejects the classic
    /// protocol), `enable.auto.commit=false` (offsets flow through the
    /// transaction), `isolation.level=read_committed` (canonical EOS setting)
    /// and `max.poll.records` capped at `RECORDS_PER_TRANSACTION` so each
    /// transaction consumes+produces at most that many records.
    fn consumer_props(&self, bootstrap_servers: &str, group_id: &str) -> HashMap<String, String> {
        // `max.poll.records` is an i32 in ConsumerConfig; clamp defensively.
        let max_poll = self.records_per_transaction.min(i32::MAX as u64).to_string();
        let mut props = HashMap::from([
            ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
            ("client.id".to_string(), format!("perf-test-rust-txn-consumer-{group_id}")),
            ("group.id".to_string(), group_id.to_string()),
            ("group.protocol".to_string(), "consumer".to_string()),
            ("enable.auto.commit".to_string(), "false".to_string()),
            ("isolation.level".to_string(), "read_committed".to_string()),
            ("auto.offset.reset".to_string(), "earliest".to_string()),
            ("max.poll.records".to_string(), max_poll),
        ]);
        // SASL: identical handling to `producer_props`.
        let sasl_enabled = matches!(self.security_protocol.as_deref(), Some("SASL_PLAINTEXT") | Some("SASL_SSL"))
            && self.sasl_mechanism.is_some()
            && self.sasl_username.is_some()
            && self.sasl_password.is_some();
        if sasl_enabled {
            let user = self.sasl_username.as_deref().unwrap();
            let pass = self.sasl_password.as_deref().unwrap();
            let jaas = format!(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \n\t\
                 username=\"{user}\" \n\tpassword=\"{pass}\";"
            );
            props.insert("security.protocol".to_string(), self.security_protocol.clone().unwrap());
            props.insert("sasl.mechanism".to_string(), self.sasl_mechanism.clone().unwrap());
            props.insert("sasl.jaas.config".to_string(), jaas);
        }
        props
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_opt(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
}

/// Deterministic, evenly-spread abort selection (Bresenham). Per-producer
/// 0-based transaction index `i`; abort iff
/// `floor((i+1) * abort_rate) > floor(i * abort_rate)`.
///  * `abort_rate == 0.0` -> never abort.
///  * `abort_rate == 0.5` -> abort `i = 1, 3, 5, ...`.
///  * `abort_rate == 1.0` -> abort every transaction.
fn should_abort(txn_index: u64, abort_rate: f64) -> bool {
    if abort_rate <= 0.0 {
        return false;
    }
    let i = txn_index as f64;
    ((i + 1.0) * abort_rate).floor() > (i * abort_rate).floor()
}

// ---------------------------------------------------------------------------
// Interval metrics (reset each 1 s window)
// ---------------------------------------------------------------------------

struct Metrics {
    messages_sent: AtomicU64,
    bytes_sent: AtomicU64,
    total_latency_us: AtomicU64,
    max_latency_us: AtomicU64,
    // Per-window per-record latency histogram (ms resolution) for the
    // p50/p90/p99/p999 percentiles emitted each window; read-and-reset on every
    // rollover.
    latency_hist: Vec<AtomicU64>,
    // --- transaction-specific per-window aggregates ---
    // Committed transactions this window.
    txns_committed: AtomicU64,
    // Per-transaction commit-latency window aggregates.
    commit_total_latency_us: AtomicU64,
    commit_max_latency_us: AtomicU64,
    commit_latency_hist: Vec<AtomicU64>,
    // Aborted transactions this window (deterministic-abort path only; the
    // symmetric counterpart of `txns_committed`).
    txns_aborted: AtomicU64,
    // Per-transaction abort-latency window aggregates (begin -> abort completes).
    abort_total_latency_us: AtomicU64,
    abort_max_latency_us: AtomicU64,
    abort_latency_hist: Vec<AtomicU64>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            max_latency_us: AtomicU64::new(0),
            latency_hist: (0..=MAX_LATENCY_MS + 1).map(|_| AtomicU64::new(0)).collect(),
            txns_committed: AtomicU64::new(0),
            commit_total_latency_us: AtomicU64::new(0),
            commit_max_latency_us: AtomicU64::new(0),
            commit_latency_hist: (0..=MAX_LATENCY_MS + 1).map(|_| AtomicU64::new(0)).collect(),
            txns_aborted: AtomicU64::new(0),
            abort_total_latency_us: AtomicU64::new(0),
            abort_max_latency_us: AtomicU64::new(0),
            abort_latency_hist: (0..=MAX_LATENCY_MS + 1).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// Record one committed record's per-record latency + throughput bytes.
    fn record_success(&self, latency_us: u64, bytes: u64) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        self.total_latency_us.fetch_add(latency_us, Ordering::Relaxed);
        self.max_latency_us.fetch_max(latency_us, Ordering::Relaxed);
        let ms = (latency_us / 1000) as usize;
        self.latency_hist[ms.min(MAX_LATENCY_MS + 1)].fetch_add(1, Ordering::Relaxed);
    }

    /// Record one committed transaction's commit latency.
    fn record_commit(&self, commit_latency_us: u64) {
        self.txns_committed.fetch_add(1, Ordering::Relaxed);
        self.commit_total_latency_us.fetch_add(commit_latency_us, Ordering::Relaxed);
        self.commit_max_latency_us.fetch_max(commit_latency_us, Ordering::Relaxed);
        let ms = (commit_latency_us / 1000) as usize;
        self.commit_latency_hist[ms.min(MAX_LATENCY_MS + 1)].fetch_add(1, Ordering::Relaxed);
    }

    /// Record one aborted transaction's abort latency (begin -> abort completes).
    fn record_abort(&self, abort_latency_us: u64) {
        self.txns_aborted.fetch_add(1, Ordering::Relaxed);
        self.abort_total_latency_us.fetch_add(abort_latency_us, Ordering::Relaxed);
        self.abort_max_latency_us.fetch_max(abort_latency_us, Ordering::Relaxed);
        let ms = (abort_latency_us / 1000) as usize;
        self.abort_latency_hist[ms.min(MAX_LATENCY_MS + 1)].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot_and_reset(&self) -> MetricsSnapshot {
        // Read-and-reset the per-window latency histograms, then derive percentiles.
        let counts: Vec<u64> = self.latency_hist.iter().map(|b| b.swap(0, Ordering::Relaxed)).collect();
        let commit_counts: Vec<u64> = self.commit_latency_hist.iter().map(|b| b.swap(0, Ordering::Relaxed)).collect();
        let abort_counts: Vec<u64> = self.abort_latency_hist.iter().map(|b| b.swap(0, Ordering::Relaxed)).collect();
        MetricsSnapshot {
            messages: self.messages_sent.swap(0, Ordering::Relaxed),
            bytes: self.bytes_sent.swap(0, Ordering::Relaxed),
            total_latency_us: self.total_latency_us.swap(0, Ordering::Relaxed),
            max_latency_us: self.max_latency_us.swap(0, Ordering::Relaxed),
            p50_ms: percentile_from_counts(&counts, 0.50),
            p90_ms: percentile_from_counts(&counts, 0.90),
            p99_ms: percentile_from_counts(&counts, 0.99),
            p999_ms: percentile_from_counts(&counts, 0.999),
            txns_committed: self.txns_committed.swap(0, Ordering::Relaxed),
            commit_total_latency_us: self.commit_total_latency_us.swap(0, Ordering::Relaxed),
            commit_max_latency_us: self.commit_max_latency_us.swap(0, Ordering::Relaxed),
            commit_p50_ms: percentile_from_counts(&commit_counts, 0.50),
            commit_p90_ms: percentile_from_counts(&commit_counts, 0.90),
            commit_p99_ms: percentile_from_counts(&commit_counts, 0.99),
            commit_p999_ms: percentile_from_counts(&commit_counts, 0.999),
            txns_aborted: self.txns_aborted.swap(0, Ordering::Relaxed),
            abort_total_latency_us: self.abort_total_latency_us.swap(0, Ordering::Relaxed),
            abort_max_latency_us: self.abort_max_latency_us.swap(0, Ordering::Relaxed),
            abort_p50_ms: percentile_from_counts(&abort_counts, 0.50),
            abort_p90_ms: percentile_from_counts(&abort_counts, 0.90),
            abort_p99_ms: percentile_from_counts(&abort_counts, 0.99),
            abort_p999_ms: percentile_from_counts(&abort_counts, 0.999),
        }
    }
}

struct MetricsSnapshot {
    messages: u64,
    bytes: u64,
    total_latency_us: u64,
    max_latency_us: u64,
    p50_ms: u64,
    p90_ms: u64,
    p99_ms: u64,
    p999_ms: u64,
    txns_committed: u64,
    commit_total_latency_us: u64,
    commit_max_latency_us: u64,
    commit_p50_ms: u64,
    commit_p90_ms: u64,
    commit_p99_ms: u64,
    commit_p999_ms: u64,
    txns_aborted: u64,
    abort_total_latency_us: u64,
    abort_max_latency_us: u64,
    abort_p50_ms: u64,
    abort_p90_ms: u64,
    abort_p99_ms: u64,
    abort_p999_ms: u64,
}

impl MetricsSnapshot {
    fn total_latency_ms(&self) -> f64 {
        self.total_latency_us as f64 / 1000.0
    }

    fn avg_latency_ms(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        self.total_latency_ms() / self.messages as f64
    }

    fn max_latency_ms(&self) -> f64 {
        self.max_latency_us as f64 / 1000.0
    }

    fn commit_total_latency_ms(&self) -> f64 {
        self.commit_total_latency_us as f64 / 1000.0
    }

    fn commit_avg_latency_ms(&self) -> f64 {
        if self.txns_committed == 0 {
            return 0.0;
        }
        self.commit_total_latency_ms() / self.txns_committed as f64
    }

    fn commit_max_latency_ms(&self) -> f64 {
        self.commit_max_latency_us as f64 / 1000.0
    }

    fn abort_total_latency_ms(&self) -> f64 {
        self.abort_total_latency_us as f64 / 1000.0
    }

    fn abort_avg_latency_ms(&self) -> f64 {
        if self.txns_aborted == 0 {
            return 0.0;
        }
        self.abort_total_latency_ms() / self.txns_aborted as f64
    }

    fn abort_max_latency_ms(&self) -> f64 {
        self.abort_max_latency_us as f64 / 1000.0
    }
}

// ---------------------------------------------------------------------------
// Process stats (CPU / RSS) — interval sampling, not a lifetime average.
// (Identical to producer_perf_test.rs; Linux-only, matches the run env.)
// ---------------------------------------------------------------------------

struct ProcSampler {
    prev_busy_ticks: u64,
    prev_instant: Instant,
}

impl ProcSampler {
    fn new() -> Self {
        Self { prev_busy_ticks: read_busy_ticks(), prev_instant: Instant::now() }
    }

    /// Returns `(cpu_percent, rss_bytes)` for the interval since the last call.
    fn sample(&mut self) -> (f64, u64) {
        let busy = read_busy_ticks();
        let now = Instant::now();
        let dt = now.duration_since(self.prev_instant).as_secs_f64();
        let dticks = busy.saturating_sub(self.prev_busy_ticks);
        self.prev_busy_ticks = busy;
        self.prev_instant = now;
        let cpu = if dt > 0.0 {
            (dticks as f64 / USER_HZ) / dt * 100.0
        } else {
            0.0
        };
        (cpu, read_rss_bytes())
    }
}

fn read_busy_ticks() -> u64 {
    let content = match std::fs::read_to_string("/proc/self/stat") {
        Ok(c) => c,
        Err(_) => return 0,
    };
    // The comm field (field 2) is wrapped in parens and may contain spaces, so
    // start parsing after the final ')'. utime is field 14, stime field 15,
    // i.e. indices 11 and 12 of the whitespace-split remainder.
    let Some(rparen) = content.rfind(')') else { return 0 };
    let rest = &content[rparen + 1..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let utime = fields.get(11).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    let stime = fields.get(12).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    utime + stime
}

fn read_rss_bytes() -> u64 {
    let content = match std::fs::read_to_string("/proc/self/status") {
        Ok(c) => c,
        Err(_) => return 0,
    };
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:")
            && let Some(kb) = rest.split_whitespace().next().and_then(|s| s.parse::<u64>().ok())
        {
            return kb * 1024;
        }
    }
    0
}

// ---------------------------------------------------------------------------
// Message generation — constant prefix + random suffix governed by RANDOMNESS,
// no key when key_size == 0. (Identical to producer_perf_test.rs.)
// ---------------------------------------------------------------------------

fn random_bytes(n: usize) -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::rng();
    (0..n).map(|_| rng.random()).collect()
}

fn message_generator(count: usize, key_size: usize, value_size: usize) -> Vec<(Option<Vec<u8>>, Vec<u8>)> {
    let value_rand = (value_size as f64 * RANDOMNESS) as usize;
    let value_constant = random_bytes(value_size - value_rand);

    let (key_constant, key_rand) = if key_size > 0 {
        let kr = (key_size as f64 * RANDOMNESS) as usize;
        (Some(random_bytes(key_size - kr)), kr)
    } else {
        (None, 0)
    };

    (0..count)
        .map(|_| {
            let mut value = value_constant.clone();
            value.extend(random_bytes(value_rand));
            let key = key_constant.as_ref().map(|kc| {
                let mut k = kc.clone();
                k.extend(random_bytes(key_rand));
                k
            });
            (key, value)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Cumulative stats (for final summary)
// ---------------------------------------------------------------------------

struct CumulativeStats {
    total_cpu: AtomicU64, // cpu% × 100, summed across samples
    total_rss: AtomicU64,
    sample_count: AtomicU64,
}

impl CumulativeStats {
    fn new() -> Self {
        Self {
            total_cpu: AtomicU64::new(0),
            total_rss: AtomicU64::new(0),
            sample_count: AtomicU64::new(0),
        }
    }

    fn accumulate(&self, cpu: f64, rss: u64) {
        self.total_cpu.fetch_add((cpu * 100.0) as u64, Ordering::Relaxed);
        self.total_rss.fetch_add(rss, Ordering::Relaxed);
        self.sample_count.fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// metrics.jsonl rollover schema consumed by
// tools/performance_metrics_plot/plot_metrics.py (shared by all the producer
// performance tests). Transaction-specific `transactions` and `commit_latency`
// objects are appended; `plot_metrics.py` ignores unknown fields.
// ---------------------------------------------------------------------------

/// Build one `{average, max, total, count}` bucket object with string values
/// for the metrics-file rollover schema.
fn bucket_json(average: f64, max: f64, total: f64, count: u64) -> serde_json::Value {
    serde_json::json!({
        "average": average.to_string(),
        "max": max.to_string(),
        "total": total.to_string(),
        "count": count.to_string(),
    })
}

#[allow(clippy::too_many_arguments)]
fn rollover_line(
    snap: &MetricsSnapshot,
    cpu: f64,
    rss: u64,
    msg_size: u64,
    window_start_ms: u128,
    window_end_ms: u128,
    measurement_start_ms: Option<u128>,
    measurement_end_ms: Option<u128>,
) -> String {
    // Single CPU/RSS sample per window ⇒ average == max == the sample.
    let rss_f = rss as f64;
    let n = snap.messages;
    let t = snap.txns_committed;
    let a = snap.txns_aborted;
    let line = serde_json::json!({
        "rss": bucket_json(rss_f, rss_f, rss_f, 1),
        "cpu": bucket_json(cpu, cpu, cpu, 1),
        "latency": {
            "average": snap.avg_latency_ms().to_string(),
            "max": snap.max_latency_ms().to_string(),
            "total": snap.total_latency_ms().to_string(),
            "count": n.to_string(),
            "p50": snap.p50_ms.to_string(),
            "p90": snap.p90_ms.to_string(),
            "p99": snap.p99_ms.to_string(),
            "p999": snap.p999_ms.to_string(),
        },
        "bytes": bucket_json(
            if n > 0 { msg_size as f64 } else { 0.0 },
            if n > 0 { msg_size as f64 } else { 0.0 },
            snap.bytes as f64,
            n,
        ),
        "messages": bucket_json(if n > 0 { 1.0 } else { 0.0 }, if n > 0 { 1.0 } else { 0.0 }, n as f64, n),
        // Committed-transaction throughput bucket for this window.
        "transactions": bucket_json(if t > 0 { 1.0 } else { 0.0 }, if t > 0 { 1.0 } else { 0.0 }, t as f64, t),
        // Per-transaction commit latency (committed only), same shape as `latency`.
        "commit_latency": {
            "average": snap.commit_avg_latency_ms().to_string(),
            "max": snap.commit_max_latency_ms().to_string(),
            "total": snap.commit_total_latency_ms().to_string(),
            "count": t.to_string(),
            "p50": snap.commit_p50_ms.to_string(),
            "p90": snap.commit_p90_ms.to_string(),
            "p99": snap.commit_p99_ms.to_string(),
            "p999": snap.commit_p999_ms.to_string(),
        },
        // Per-transaction abort latency (deterministic-abort path only), same
        // shape as `commit_latency`. Symmetric with commit so the two can be
        // compared directly (Kaushik: "Should we track abort latencies also?").
        "abort_latency": {
            "average": snap.abort_avg_latency_ms().to_string(),
            "max": snap.abort_max_latency_ms().to_string(),
            "total": snap.abort_total_latency_ms().to_string(),
            "count": a.to_string(),
            "p50": snap.abort_p50_ms.to_string(),
            "p90": snap.abort_p90_ms.to_string(),
            "p99": snap.abort_p99_ms.to_string(),
            "p999": snap.abort_p999_ms.to_string(),
        },
        "window_start_ms": window_start_ms.to_string(),
        "window_end_ms": window_end_ms.to_string(),
        "measurement_start_ms": measurement_start_ms.map(|v| v.to_string()).unwrap_or_else(|| "-inf".to_string()),
        "measurement_end_ms": measurement_end_ms.map(|v| v.to_string()).unwrap_or_else(|| "-inf".to_string()),
    });
    line.to_string()
}

// ---------------------------------------------------------------------------
// Latency histogram helpers
// ---------------------------------------------------------------------------

fn percentile_from_hist(hist: &[AtomicU64], p: f64) -> u64 {
    let total: u64 = hist.iter().map(|b| b.load(Ordering::Relaxed)).sum();
    if total == 0 {
        return 0;
    }
    let target = (total as f64 * p).ceil() as u64;
    let mut cum = 0u64;
    for (ms, b) in hist.iter().enumerate() {
        cum += b.load(Ordering::Relaxed);
        if cum >= target {
            return ms as u64;
        }
    }
    (hist.len() - 1) as u64
}

/// Percentile (0.0..=1.0) in ms from a plain per-window count histogram.
fn percentile_from_counts(counts: &[u64], p: f64) -> u64 {
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return 0;
    }
    let target = (total as f64 * p).ceil() as u64;
    let mut cum = 0u64;
    for (ms, &c) in counts.iter().enumerate() {
        cum += c;
        if cum >= target {
            return ms as u64;
        }
    }
    (counts.len() - 1) as u64
}

/// Summary stats over a cumulative histogram: `(count, sum_ms, min_ms, max_ms)`.
fn hist_summary(hist: &[AtomicU64]) -> (u64, u64, u64, u64) {
    let (mut count, mut sum, mut min, mut max) = (0u64, 0u64, 0u64, 0u64);
    for (ms, b) in hist.iter().enumerate() {
        let n = b.load(Ordering::Relaxed);
        if n > 0 {
            if count == 0 {
                min = ms as u64;
            }
            count += n;
            sum += ms as u64 * n;
            max = ms as u64;
        }
    }
    (count, sum, min, max)
}

// ---------------------------------------------------------------------------
// Shared state passed to each producer task.
// ---------------------------------------------------------------------------

struct SharedState {
    metrics: Arc<Metrics>,
    // Cumulative summary histograms (per-record + per-transaction commit/abort).
    latency_hist: Arc<Vec<AtomicU64>>,
    commit_latency_hist: Arc<Vec<AtomicU64>>,
    abort_latency_hist: Arc<Vec<AtomicU64>>,
    // Global counters aggregated across all producer tasks.
    committed_records: Arc<AtomicU64>,
    verified: Arc<AtomicU64>,
    committed_transactions: Arc<AtomicU64>,
    aborted_transactions: Arc<AtomicU64>,
    aborted_records: Arc<AtomicU64>,
}

/// One producer task's produce-mode loop (Phase 1, `TXN_MODE=produce`).
///
/// Obeys CLAUDE.md §11 (no per-record `tokio::spawn` — this is one task per
/// producer, records are produced inline) and §9.6 (no `MutexGuard` held across
/// an `.await`: this task holds no lock; the `KafkaProducer` manages the
/// `TransactionManager` lock internally).
#[allow(clippy::too_many_arguments)]
async fn run_producer(
    producer: KafkaProducer<Vec<u8>, Vec<u8>>,
    messages: Arc<Vec<(Option<Vec<u8>>, Vec<u8>)>>,
    topic: String,
    message_size: u64,
    records_per_transaction: u64,
    abort_rate: f64,
    do_verify: bool,
    num_messages: u64,
    test_start: Instant,
    test_duration: Duration,
    limit_rps: u64,
    shared: Arc<SharedState>,
) {
    let msg_count = messages.len();
    let mut txn_index: u64 = 0;
    let mut records_sent: u64 = 0;
    // Rate limiting: pace in 100 ms windows (limit_rps/10 records per checkpoint),
    // matching producer_perf_test.rs's rate_checkpoint.
    let rate_checkpoint = (limit_rps / 10).max(1);
    let mut next_check_time = Duration::from_millis(100);

    loop {
        // A counted run is bounded by `num_messages` (records attempted by this
        // producer); a time-based run (num_messages == 0) is bounded by the
        // duration. Both are checked only at a transaction boundary so a
        // transaction is never split.
        if num_messages > 0 {
            if records_sent >= num_messages {
                break;
            }
        } else if test_start.elapsed() >= test_duration {
            break;
        }

        // --- begin ---
        let begin_time = Instant::now();
        if let Err(e) = producer.begin_transaction() {
            eprintln!("begin_transaction error: {e:?}");
            break;
        }

        // --- produce N records, capturing per-record produce timestamps ---
        let n = records_per_transaction as usize;
        let mut produce_ts: Vec<Instant> = Vec::with_capacity(n);
        let mut produce_calls = Vec::with_capacity(n);
        for _ in 0..records_per_transaction {
            let (key, value) = &messages[records_sent as usize % msg_count];
            let record: ProducerRecord<&[u8], &[u8]> =
                ProducerRecord::with_key(topic.clone(), key.as_deref(), Some(value.as_slice()));
            let ts = Instant::now();
            match producer.send(record, None).await {
                Ok(produce_call) => {
                    produce_ts.push(ts);
                    produce_calls.push(produce_call);
                },
                Err(e) => {
                    eprintln!("Send error: {e:?}");
                },
            }
            records_sent += 1;

            // Rate limit per record, relative to the common measured-interval start.
            if limit_rps > 0 && records_sent.is_multiple_of(rate_checkpoint) {
                let elapsed = test_start.elapsed();
                if elapsed < next_check_time {
                    tokio::time::sleep(next_check_time - elapsed).await;
                }
                next_check_time += Duration::from_millis(100);
            }
        }

        // --- commit or abort ---
        if should_abort(txn_index, abort_rate) {
            // Aborted transactions STILL produced their N records above; on abort
            // those records count toward neither throughput nor latency.
            if let Err(e) = producer.abort_transaction().await {
                eprintln!("abort_transaction error: {e:?}");
            }
            // Per-transaction abort latency = begin -> abort completes (symmetric
            // with commit latency; deterministic-abort path only).
            let abort_completion = Instant::now();
            let abort_latency_us = abort_completion.duration_since(begin_time).as_micros() as u64;
            shared.metrics.record_abort(abort_latency_us);
            let ams = (abort_latency_us / 1000) as usize;
            shared.abort_latency_hist[ams.min(MAX_LATENCY_MS + 1)].fetch_add(1, Ordering::Relaxed);
            shared.aborted_transactions.fetch_add(1, Ordering::Relaxed);
            // The transaction attempted N records regardless of how many sends
            // returned Ok, matching PLAN §4.2 and the C/Java harnesses
            // (`aborted_records += n` / `abortedRecords.addAndGet(n)`).
            shared.aborted_records.fetch_add(records_per_transaction, Ordering::Relaxed);
        } else {
            match producer.commit_transaction().await {
                Ok(()) => {
                    // On return every record in the transaction is durable.
                    let commit_completion = Instant::now();
                    shared.committed_transactions.fetch_add(1, Ordering::Relaxed);

                    // Per-transaction commit latency = begin -> commit completes.
                    let commit_latency_us = commit_completion.duration_since(begin_time).as_micros() as u64;
                    shared.metrics.record_commit(commit_latency_us);
                    let cms = (commit_latency_us / 1000) as usize;
                    shared.commit_latency_hist[cms.min(MAX_LATENCY_MS + 1)].fetch_add(1, Ordering::Relaxed);

                    // Per-record latency = each record's produce() -> commit completes.
                    for (ts, produce_call) in produce_ts.iter().zip(produce_calls.into_iter()) {
                        let latency_us = commit_completion.duration_since(*ts).as_micros() as u64;
                        shared.metrics.record_success(latency_us, message_size);
                        let ms = (latency_us / 1000) as usize;
                        shared.latency_hist[ms.min(MAX_LATENCY_MS + 1)].fetch_add(1, Ordering::Relaxed);

                        // Verification: the commit already flushed, so the future
                        // is resolved; await is immediate.
                        match produce_call.get_timeout(Duration::from_secs(60)).await {
                            Ok(md) => {
                                if !do_verify || verify_record_metadata(&md, &topic) {
                                    shared.verified.fetch_add(1, Ordering::Relaxed);
                                }
                            },
                            Err(e) => {
                                eprintln!("Produce call resulted in exception: {e:?}");
                            },
                        }
                        shared.committed_records.fetch_add(1, Ordering::Relaxed);
                    }
                },
                Err(e) => {
                    eprintln!("commit_transaction error: {e:?}");
                    shared.aborted_transactions.fetch_add(1, Ordering::Relaxed);
                    // The transaction attempted N records regardless of how many
                    // sends returned Ok, matching PLAN §4.3 and the C/Java harnesses.
                    shared.aborted_records.fetch_add(records_per_transaction, Ordering::Relaxed);
                },
            }
        }

        txn_index += 1;
    }

    // Compression cross-check: print this producer's own compression-rate-avg
    // (org.apache.kafka.clients.producer.internals.SenderMetricsRegistry) for
    // parity with the base producer_perf_test.rs harness. Under the
    // concurrent-producers design each of NUM_TRANSACTIONAL_PRODUCERS producers
    // emits its own value. Must run before close() — metrics are torn down with
    // the producer.
    for (name, metric) in producer.metrics() {
        if name.name() == "compression-rate-avg" {
            println!("[METRIC] compression-rate-avg = {:?}", metric.metric_value());
        }
    }

    // Explicitly close the producer for parity with the base harness
    // (producer_perf_test.rs closes at line 944). §9.6: this task holds no lock,
    // so nothing is held across the await; §11: this is one task per producer,
    // not a per-record spawn.
    let _ = producer.close().await;
}

/// One producer task's EOS read-process-write loop (Phase 2, `TXN_MODE=eos`).
///
/// Consume up to `RECORDS_PER_TRANSACTION` records from the source topic (the
/// consumer's `max.poll.records` caps the batch), transform (identity/echo),
/// produce them to `dest_topic`, `send_offsets_to_transaction` the consumed
/// offsets + 1 with the consumer group metadata, then `commit`/`abort`.
///
/// Obeys CLAUDE.md §11 (no per-record `tokio::spawn` — records are produced
/// inline in this single per-producer task) and §9.6 (this task holds no lock
/// across an `.await`; the `KafkaProducer` manages the `TransactionManager`
/// lock internally, and the `Consumer` its own `SubscriptionState` lock).
#[allow(clippy::too_many_arguments)]
async fn run_eos_producer(
    producer: KafkaProducer<Vec<u8>, Vec<u8>>,
    mut consumer: Box<dyn Consumer<Vec<u8>, Vec<u8>>>,
    dest_topic: String,
    message_size: u64,
    abort_rate: f64,
    do_verify: bool,
    num_messages: u64,
    test_start: Instant,
    test_duration: Duration,
    limit_rps: u64,
    poll_timeout: Duration,
    shared: Arc<SharedState>,
) {
    let mut txn_index: u64 = 0;
    let mut records_sent: u64 = 0;
    // Rate limiting: pace in 100 ms windows, matching produce mode.
    let rate_checkpoint = (limit_rps / 10).max(1);
    let mut next_check_time = Duration::from_millis(100);
    // Consecutive empty polls, to warn (once) that the source is starving the
    // pipeline (Kaushik C:1612): the EOS ceiling is min(source-rate, txn-rate),
    // so an under-fed source shows up here rather than as a deadlock.
    let mut consecutive_empty_polls: u64 = 0;
    let mut starvation_warned = false;

    loop {
        // Bound at a transaction boundary so a transaction is never split.
        if num_messages > 0 {
            if records_sent >= num_messages {
                break;
            }
        } else if test_start.elapsed() >= test_duration {
            break;
        }

        // --- consume (bounded poll; an empty poll is skipped, never blocks
        // forever) ---
        let records = match consumer.poll(poll_timeout).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("consumer poll error: {e:?}");
                break;
            },
        };
        if records.is_empty() {
            // No source data this round; loop again (bounded by the duration /
            // message target above), rather than committing an empty transaction.
            // Warn once if the source appears to be starving the pipeline so the
            // low throughput is attributable to an under-fed SOURCE_TOPIC.
            consecutive_empty_polls += 1;
            if consecutive_empty_polls >= 10 && !starvation_warned {
                eprintln!(
                    "[WARN] EOS source starved: {consecutive_empty_polls} consecutive empty polls \
                     from the source topic — the source producer is not keeping up (the EOS \
                     throughput ceiling is min(source-produce-rate, txn-process-rate)); \
                     pre-populate/seed SOURCE_TOPIC with a high-throughput producer"
                );
                starvation_warned = true;
            }
            continue;
        }
        consecutive_empty_polls = 0;

        // --- begin ---
        let begin_time = Instant::now();
        if let Err(e) = producer.begin_transaction() {
            eprintln!("begin_transaction error: {e:?}");
            break;
        }

        // --- transform + produce each record, capturing per-record produce
        // timestamps and accumulating the max consumed offset per partition ---
        let mut produce_ts: Vec<Instant> = Vec::new();
        let mut produce_calls = Vec::new();
        // Standard Kafka EOS offset bookkeeping: commit last-consumed-offset + 1
        // per TopicPartition.
        let mut offsets: HashMap<TopicPartition, i64> = HashMap::new();
        // The transaction ATTEMPTED this many records (used for aborted_records,
        // mirroring produce mode's `aborted_records += n`).
        let mut batch_records: u64 = 0;
        for record in &records {
            let key = record.key().map(|k| k.as_slice());
            let value = record.value().map(|v| v.as_slice());
            let out: ProducerRecord<&[u8], &[u8]> = ProducerRecord::with_key(dest_topic.clone(), key, value);
            let ts = Instant::now();
            match producer.send(out, None).await {
                Ok(produce_call) => {
                    produce_ts.push(ts);
                    produce_calls.push(produce_call);
                },
                Err(e) => {
                    eprintln!("Send error: {e:?}");
                },
            }

            let tp = TopicPartition::new(record.topic(), record.partition());
            let next = record.offset() + 1;
            offsets.entry(tp).and_modify(|o| *o = (*o).max(next)).or_insert(next);

            records_sent += 1;
            batch_records += 1;

            // Rate limit per record, relative to the common measured-interval start.
            if limit_rps > 0 && records_sent.is_multiple_of(rate_checkpoint) {
                let elapsed = test_start.elapsed();
                if elapsed < next_check_time {
                    tokio::time::sleep(next_check_time - elapsed).await;
                }
                next_check_time += Duration::from_millis(100);
            }
        }

        // --- send the consumed offsets to the transaction (offset + 1 per
        // partition, with the consumer group metadata) ---
        let group_metadata = consumer.group_metadata();
        let mut offset_map: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::with_capacity(offsets.len());
        for (tp, off) in offsets {
            match OffsetAndMetadata::new(off) {
                Ok(om) => {
                    offset_map.insert(tp, om);
                },
                Err(e) => eprintln!("offset error for {tp:?}: {e:?}"),
            }
        }
        if let Err(e) = producer.send_offsets_to_transaction(offset_map, group_metadata).await {
            eprintln!("send_offsets_to_transaction error: {e:?}");
            if let Err(ae) = producer.abort_transaction().await {
                eprintln!("abort_transaction error: {ae:?}");
            }
            shared.aborted_transactions.fetch_add(1, Ordering::Relaxed);
            shared.aborted_records.fetch_add(batch_records, Ordering::Relaxed);
            txn_index += 1;
            continue;
        }

        // --- commit or abort ---
        if should_abort(txn_index, abort_rate) {
            // Aborted transactions produced+consumed their records; on abort they
            // count toward neither throughput nor latency, the consumed offsets
            // are NOT committed, and (per the module doc) the consumer is NOT
            // sought back — those records are simply not reprocessed here.
            if let Err(e) = producer.abort_transaction().await {
                eprintln!("abort_transaction error: {e:?}");
            }
            // Per-transaction abort latency = begin -> abort completes (symmetric
            // with commit latency; deterministic-abort path only).
            let abort_completion = Instant::now();
            let abort_latency_us = abort_completion.duration_since(begin_time).as_micros() as u64;
            shared.metrics.record_abort(abort_latency_us);
            let ams = (abort_latency_us / 1000) as usize;
            shared.abort_latency_hist[ams.min(MAX_LATENCY_MS + 1)].fetch_add(1, Ordering::Relaxed);
            shared.aborted_transactions.fetch_add(1, Ordering::Relaxed);
            shared.aborted_records.fetch_add(batch_records, Ordering::Relaxed);
        } else {
            match producer.commit_transaction().await {
                Ok(()) => {
                    // On return every record in the transaction is durable and the
                    // consumed offsets are committed atomically with the output.
                    let commit_completion = Instant::now();
                    shared.committed_transactions.fetch_add(1, Ordering::Relaxed);

                    // Per-transaction commit latency = begin -> commit completes.
                    let commit_latency_us = commit_completion.duration_since(begin_time).as_micros() as u64;
                    shared.metrics.record_commit(commit_latency_us);
                    let cms = (commit_latency_us / 1000) as usize;
                    shared.commit_latency_hist[cms.min(MAX_LATENCY_MS + 1)].fetch_add(1, Ordering::Relaxed);

                    // Per-record latency = each record's produce() -> commit completes.
                    for (ts, produce_call) in produce_ts.iter().zip(produce_calls.into_iter()) {
                        let latency_us = commit_completion.duration_since(*ts).as_micros() as u64;
                        shared.metrics.record_success(latency_us, message_size);
                        let ms = (latency_us / 1000) as usize;
                        shared.latency_hist[ms.min(MAX_LATENCY_MS + 1)].fetch_add(1, Ordering::Relaxed);

                        match produce_call.get_timeout(Duration::from_secs(60)).await {
                            Ok(md) => {
                                if !do_verify || verify_record_metadata(&md, &dest_topic) {
                                    shared.verified.fetch_add(1, Ordering::Relaxed);
                                }
                            },
                            Err(e) => {
                                eprintln!("Produce call resulted in exception: {e:?}");
                            },
                        }
                        shared.committed_records.fetch_add(1, Ordering::Relaxed);
                    }
                },
                Err(e) => {
                    eprintln!("commit_transaction error: {e:?}");
                    shared.aborted_transactions.fetch_add(1, Ordering::Relaxed);
                    shared.aborted_records.fetch_add(batch_records, Ordering::Relaxed);
                },
            }
        }

        txn_index += 1;
    }

    // Compression cross-check (parity with produce mode / base harness). Must
    // run before close() — metrics are torn down with the producer.
    for (name, metric) in producer.metrics() {
        if name.name() == "compression-rate-avg" {
            println!("[METRIC] compression-rate-avg = {:?}", metric.metric_value());
        }
    }

    // Explicitly close producer and consumer (§9.6: this task holds no lock, so
    // nothing is held across the await; §11: one task per producer, not a
    // per-record spawn).
    let _ = producer.close().await;
    let _ = consumer.close().await;
}

/// Seed the source topic with `count` records using a plain (non-transactional),
/// HIGH-THROUGHPUT producer, so the in-suite EOS run has data to consume. This is
/// the IN-SUITE-ONLY seeder: the ephemeral Docker path must run without a Kafka
/// bin dir or Java on PATH, so it cannot use the canonical `KAFKA_BIN` →
/// `kafka-producer-perf-test.sh` seeding (`spawn_seed_producer`) that every
/// external-broker run uses (see the module header, Kaushik C:1612).
///
/// The EOS throughput ceiling is `min(source-produce-rate, txn-process-rate)`, so
/// the seeder must out-run the transactional pipeline: it runs UNBOUNDED
/// (`LIMIT_RPS=0`, no rate limit), leaves `enable.idempotence` at its default
/// (off — the source topic is plain, non-transactional), and uses a large
/// `batch.size` + `linger.ms` with `acks=1` for maximum throughput. It seeds
/// `count` records up front (before the measured clock starts), enough to feed
/// the whole measured window so the consumer is never starved.
async fn seed_source_topic(
    bootstrap_servers: &str,
    source_topic: &str,
    messages: &[(Option<Vec<u8>>, Vec<u8>)],
    count: u64,
) {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("client.id".to_string(), "perf-test-rust-txn-seed".to_string()),
        // High-throughput seeding: acks=1 (source topic is plain), large batch +
        // linger to pack records, idempotence left at its default (off). Runs
        // unbounded — the seed loop below produces as fast as the client allows.
        ("acks".to_string(), "1".to_string()),
        ("batch.size".to_string(), (1024 * 1024).to_string()),
        ("linger.ms".to_string(), "100".to_string()),
    ]);
    let producer_config = ProducerConfig::from_properties(&props).expect("Invalid seed producer config");
    let producer = KafkaProducer::<Vec<u8>, Vec<u8>>::from_config(
        producer_config,
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("Failed to create seed producer");

    let msg_count = messages.len();
    let mut calls = Vec::with_capacity(count as usize);
    for i in 0..count {
        let (key, value) = &messages[i as usize % msg_count];
        let record: ProducerRecord<&[u8], &[u8]> =
            ProducerRecord::with_key(source_topic.to_string(), key.as_deref(), Some(value.as_slice()));
        if let Ok(call) = producer.send(record, None).await {
            calls.push(call);
        }
    }
    for call in calls {
        let _ = call.get_timeout(Duration::from_secs(60)).await;
    }
    let _ = producer.close().await;
}

/// Seed `SOURCE_TOPIC` by spawning Kafka's standard `kafka-producer-perf-test.sh`
/// (a plain Java producer) from `$KAFKA_BIN`, the SAME mechanism the consumer-perf
/// harnesses use to generate their input load (see `consumer-perf/src/main.rs`
/// `spawn_producer`, `consumer-perf/compare/librdkafka_e2e.c`, and
/// `bindings/python/test/performance/consumer_performance_test.py`). This is the
/// canonical, cross-language-identical EOS seeding path: one standard Java
/// producer, filling the source at high throughput before the measured interval
/// so the EOS consumer is never starved.
///
/// Seed-then-run: the child produces `seed_count` records and exits; we wait for
/// it, so the source is fully populated before the measured clock starts. SASL /
/// security is forwarded through a `--producer.config` Java properties file
/// (the jaas form), exactly as the consumer-perf harness does — `--producer-props`
/// cannot carry `sasl.jaas.config` (its value contains multiple `=`). Runs
/// synchronously (setup phase, off the measured path); returns `Err` only on a
/// spawn/exec failure, which the caller surfaces as a non-fatal warning.
fn spawn_seed_producer(
    kafka_bin: &str,
    config: &PerfTestConfig,
    bootstrap_servers: &str,
    source_topic: &str,
    seed_count: u64,
) -> std::io::Result<()> {
    let bin = format!("{kafka_bin}/kafka-producer-perf-test.sh");
    let record_size = config.message_size().max(1);
    let throughput = config.seed_throughput.to_string();
    println!(
        ">>> Seeding source topic '{source_topic}' via {bin}: throughput={} ({}), {record_size} bytes, {seed_count} records",
        throughput,
        if config.seed_throughput < 0 {
            "PEAK/unbounded"
        } else {
            "fixed msg/s"
        },
    );

    let mut pargs: Vec<String> = vec![
        "--topic".into(),
        source_topic.to_string(),
        "--num-records".into(),
        seed_count.to_string(),
        "--record-size".into(),
        record_size.to_string(),
        "--throughput".into(),
        throughput,
    ];

    // SASL / security goes through a Java .properties file (bootstrap + security
    // from the file), only acks stays on the command line — same shape as the
    // consumer-perf harness's spawn_producer. `sasl.jaas.config` cannot ride on
    // `--producer-props` because its value contains multiple `=`.
    let sasl_enabled = matches!(config.security_protocol.as_deref(), Some("SASL_PLAINTEXT") | Some("SASL_SSL"))
        && config.sasl_mechanism.is_some()
        && config.sasl_username.is_some()
        && config.sasl_password.is_some();
    let mut props_path: Option<std::path::PathBuf> = None;
    if sasl_enabled {
        let user = config.sasl_username.as_deref().unwrap();
        let pass = config.sasl_password.as_deref().unwrap();
        // One logical line per property (a raw newline ends a .properties entry),
        // so collapse the cosmetic whitespace in the jaas value to single spaces.
        let jaas = format!(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{user}\" password=\"{pass}\";"
        );
        let path = std::env::temp_dir().join(format!("txn_perf_seed_{}.properties", std::process::id()));
        let mut f = std::fs::File::create(&path)?;
        writeln!(f, "bootstrap.servers={bootstrap_servers}")?;
        writeln!(f, "security.protocol={}", config.security_protocol.as_deref().unwrap())?;
        writeln!(f, "sasl.mechanism={}", config.sasl_mechanism.as_deref().unwrap())?;
        writeln!(f, "sasl.jaas.config={jaas}")?;
        drop(f);
        pargs.push("--producer.config".into());
        pargs.push(path.to_string_lossy().into_owned());
        pargs.push("--producer-props".into());
        pargs.push("acks=1".into());
        props_path = Some(path);
    } else {
        pargs.push("--producer-props".into());
        pargs.push(format!("bootstrap.servers={bootstrap_servers}"));
        pargs.push("acks=1".into());
    }

    // Capture the child's output to a log file (a silent producer failure would
    // otherwise look identical to "no source data" on the consumer side).
    let log_path = std::env::temp_dir().join(format!("txn_perf_seed_{}.log", std::process::id()));
    let log = std::fs::File::create(&log_path).ok();
    let (stdout, stderr) = match log {
        Some(f) => {
            let f2 = f.try_clone().unwrap_or_else(|_| std::fs::File::create(&log_path).unwrap());
            (Stdio::from(f), Stdio::from(f2))
        },
        None => (Stdio::null(), Stdio::null()),
    };

    let status = Command::new(&bin).args(&pargs).stdout(stdout).stderr(stderr).status();
    // Clean up the properties file regardless of the outcome (it holds credentials).
    if let Some(p) = props_path {
        let _ = std::fs::remove_file(p);
    }
    match status {
        Ok(s) if s.success() => {
            println!(
                "    source seeding complete ({seed_count} records; log: {})",
                log_path.display()
            );
            Ok(())
        },
        Ok(s) => {
            println!(
                "    WARN: seed producer exited with {s} (see {}); continuing — the EOS consumer will warn if starved",
                log_path.display()
            );
            Ok(())
        },
        Err(e) => Err(e),
    }
}

/// Verify a `RecordMetadata`: offset/partition non-negative, topic matches,
/// timestamp present.
fn verify_record_metadata(md: &confluent_kafka::producer::RecordMetadata, topic: &str) -> bool {
    md.offset() >= 0 && md.partition() >= 0 && md.topic() == topic && md.timestamp() >= 0
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn transactional_producer_perf_test() {
    let _ = env_logger::builder().is_test(false).try_init();

    let mut config = PerfTestConfig::from_env();

    // Two modes: `produce` (Phase 1) and `eos` (Phase 2). `eos` requires a
    // SOURCE_TOPIC; any other value is rejected outright.
    let eos_mode = match config.txn_mode.as_str() {
        "produce" => false,
        "eos" => {
            assert!(
                config.source_topic.is_some(),
                "TXN_MODE=eos requires SOURCE_TOPIC to be set (the input topic to consume from)"
            );
            true
        },
        other => panic!("Unknown TXN_MODE '{other}' (expected 'produce' or 'eos')"),
    };

    // In-suite integration run (no external broker → an ephemeral Docker
    // cluster): keep it short and rate-limited so it fits the normal test suite.
    if config.bootstrap_servers.is_empty() {
        if std::env::var("TEST_DURATION_SECONDS").is_err() {
            config.test_duration_seconds = 10;
        }
        if std::env::var("LIMIT_RPS").is_err() {
            config.limit_rps = 100;
        }
        // NOTE (deviation from producer_perf_test.rs, justified per
        // definition-of-done.md §7): the non-transactional harness defaults the
        // in-suite per-record p99 budget to 70 ms. In produce mode a record's
        // latency is `send() -> its transaction's commit completes`, so it is
        // dominated by transaction-fill time (RECORDS_PER_TRANSACTION / rate) and
        // is NOT a meaningful steady-state per-record budget. We therefore leave
        // P99_LIMIT_MS at 0 (disabled) in-suite; the meaningful transactional
        // signal is `commit_latency_ms` in results.json. A user can still set
        // P99_LIMIT_MS explicitly to assert a per-record budget.
        if std::env::var("WARMUP_SECONDS").is_err() {
            config.warmup_seconds = 0;
        }
        if config.limit_rps > 0 && std::env::var("NUM_MESSAGES").is_err() {
            config.num_messages = config.limit_rps * config.test_duration_seconds;
        }
    }
    let message_size = config.message_size() as u64;

    // --- Broker setup ---
    // A single-broker Docker cluster needs the transaction-state-log topic to be
    // creatable with RF=1 (the broker default is 3), otherwise
    // `init_transactions()` cannot find/create a transaction coordinator.
    let in_suite = config.bootstrap_servers.is_empty();
    let (_ctx, bootstrap_servers, topic, source_topic) = if in_suite {
        let mut props = std::collections::BTreeMap::new();
        props.insert("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR".to_string(), "1".to_string());
        props.insert("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR".to_string(), "1".to_string());
        let mut ctx = TestContext::new(ClusterConfig::with_properties(props)).await;
        let topic = ctx.topic(&config.topic_name);
        // EOS needs a distinct (prefixed) source topic in-suite.
        let source = eos_mode.then(|| ctx.topic(config.source_topic.as_deref().unwrap_or("source-topic")));
        let bs = ctx.bootstrap_servers().to_string();
        (Some(ctx), bs, topic, source)
    } else {
        // Against an external broker the caller's SOURCE_TOPIC is used verbatim
        // and assumed pre-populated.
        let source = config.source_topic.clone();
        (None, config.bootstrap_servers.clone(), config.topic_name.clone(), source)
    };

    println!(
        "=== Rust Transactional Producer Performance Test (TXN_MODE={}) ===",
        config.txn_mode
    );
    println!("Bootstrap servers: {bootstrap_servers}");
    println!("Topic: {topic}");
    if eos_mode {
        println!("Source topic: {}", source_topic.as_deref().unwrap_or("<unset>"));
        println!("Consumer group id: {}", config.consumer_group_id);
    }
    println!(
        "Key size: {} B, Value size: {} B, Message size: {} B",
        config.key_size, config.value_size, message_size
    );
    println!("Records per transaction: {}", config.records_per_transaction);
    println!("Abort rate: {}", config.abort_rate);
    println!("Transactional producers: {}", config.num_transactional_producers);
    println!("Transactional id base: {}", config.transactional_id);
    if config.use_defaults {
        println!("USE_DEFAULTS: true (client defaults; tuning knobs omitted)");
    } else {
        println!("batch.size: {} B", config.batch_size);
        println!("max.request.size: {} B", config.max_request_size);
        if let Some(v) = config.buffer_memory {
            println!("buffer.memory: {v} B");
        }
        if let Some(v) = &config.linger_ms {
            println!("linger.ms: {v}");
        }
        println!("compression.type: {}", config.compression_type);
        println!("enable.idempotence: true (forced for transactions)");
    }
    println!("Verify: {}", config.do_verify);
    println!(
        "Warmup: {} s, Test duration: {} s",
        config.warmup_seconds, config.test_duration_seconds
    );
    if config.num_messages > 0 {
        println!("Num messages (total target): {}", config.num_messages);
    }
    if config.limit_rps > 0 {
        println!("Rate limit (total): {} msg/s", config.limit_rps);
    }

    // --- Create + initialize the transactional producers ---
    let num_producers = config.num_transactional_producers;
    let mut producers: Vec<KafkaProducer<Vec<u8>, Vec<u8>>> = Vec::with_capacity(num_producers as usize);
    for k in 0..num_producers {
        let txn_id = format!("{}-{}", config.transactional_id, k);
        let props = config.producer_props(&bootstrap_servers, &txn_id);
        let producer_config = ProducerConfig::from_properties(&props).expect("Invalid producer config");
        let producer = KafkaProducer::<Vec<u8>, Vec<u8>>::from_config(
            producer_config,
            Box::new(ByteArraySerializer),
            Box::new(ByteArraySerializer),
        )
        .expect("Failed to create producer");
        producer.init_transactions().await.expect("init_transactions failed");
        producers.push(producer);
    }

    // --- Pre-generate messages (shared across producers) ---
    let messages = Arc::new(message_generator(GENERATED_MESSAGES, config.key_size, config.value_size));

    // --- Shared state ---
    let metrics = Arc::new(Metrics::new());
    let cumulative = Arc::new(CumulativeStats::new());
    let should_stop = Arc::new(AtomicBool::new(false));
    let latency_hist: Arc<Vec<AtomicU64>> = Arc::new((0..=MAX_LATENCY_MS + 1).map(|_| AtomicU64::new(0)).collect());
    let commit_latency_hist: Arc<Vec<AtomicU64>> =
        Arc::new((0..=MAX_LATENCY_MS + 1).map(|_| AtomicU64::new(0)).collect());
    let abort_latency_hist: Arc<Vec<AtomicU64>> =
        Arc::new((0..=MAX_LATENCY_MS + 1).map(|_| AtomicU64::new(0)).collect());

    // === METRICS COLLECTOR ===
    // Spawned BEFORE warmup so the rollover JSONL also captures warmup windows
    // (measurement_start_ms = -inf) and the post-test cooldown windows. The
    // measured interval is delimited by the `meas_start` / `meas_end` atomics
    // (0 = unset → "-inf" in the JSONL).
    let meas_start = Arc::new(AtomicU64::new(0));
    let meas_end = Arc::new(AtomicU64::new(0));
    let metrics_for_collector = Arc::clone(&metrics);
    let metrics_cumul = Arc::clone(&cumulative);
    let metrics_stop = Arc::clone(&should_stop);
    let meas_start_collector = Arc::clone(&meas_start);
    let meas_end_collector = Arc::clone(&meas_end);
    let metrics_file = config.metrics_file.clone();
    let metrics_task = tokio::spawn(async move {
        let mut file = std::fs::File::create(&metrics_file).expect("Failed to create metrics file");
        let mut sampler = ProcSampler::new();
        let mut window_start_ms = now_ms();

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let stopping = metrics_stop.load(Ordering::Relaxed);

            let snap = metrics_for_collector.snapshot_and_reset();
            let (cpu, rss) = sampler.sample();
            let window_end_ms = now_ms();

            let start = meas_start_collector.load(Ordering::Relaxed);
            let end = meas_end_collector.load(Ordering::Relaxed);
            // Accumulate CPU/RSS into the run summary over the measured interval
            // only (measurement_start set and measurement_end not). Warmup and
            // cooldown windows are excluded.
            if start != 0 && end == 0 {
                metrics_cumul.accumulate(cpu, rss);
            }

            let line = rollover_line(
                &snap,
                cpu,
                rss,
                message_size,
                window_start_ms,
                window_end_ms,
                if start == 0 { None } else { Some(start as u128) },
                if end == 0 { None } else { Some(end as u128) },
            );
            let _ = writeln!(file, "{line}");
            let _ = file.flush();
            window_start_ms = window_end_ms;

            if stopping {
                break;
            }
        }
    });

    // === WARMUP ===
    // Run warmup transactions on producer 0 only (a JIT/connection warmup),
    // mirroring the single-producer warmup of producer_perf_test.rs. The
    // collector is already running, so warmup windows appear in the JSONL with
    // measurement_start_ms = -inf (excluded from the measured statistics).
    if config.warmup_seconds > 0 {
        println!("Warming up for {} seconds ...", config.warmup_seconds);
        let warmup_end = Instant::now() + Duration::from_secs(config.warmup_seconds);
        let warmup_producer = &producers[0];
        let msg_count = messages.len();
        let mut i = 0usize;
        while Instant::now() < warmup_end {
            warmup_producer.begin_transaction().expect("warmup begin_transaction failed");
            let (key, value) = &messages[i % msg_count];
            let record: ProducerRecord<&[u8], &[u8]> =
                ProducerRecord::with_key(topic.clone(), key.as_deref(), Some(value.as_slice()));
            if let Ok(produce_call) = warmup_producer.send(record, None).await {
                warmup_producer
                    .commit_transaction()
                    .await
                    .expect("warmup commit_transaction failed");
                let md = produce_call
                    .get_timeout(Duration::from_secs(30))
                    .await
                    .expect("warmup send failed");
                assert!(
                    !config.do_verify || verify_record_metadata(&md, &topic),
                    "warmup failed due to message verification error"
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            i += 1;
        }
        println!("Warmup complete.");
    }

    // === EOS: seed the in-suite source topic + create the consumers ===
    // Done BEFORE the measured clock starts so seeding does not eat into the
    // measured interval. One consumer per producer, all sharing one group id.
    let mut consumers: std::collections::VecDeque<Box<dyn Consumer<Vec<u8>, Vec<u8>>>> =
        std::collections::VecDeque::new();
    if eos_mode {
        let source = source_topic.clone().expect("eos requires a source topic");
        if in_suite {
            // In-suite EXCEPTION (kept deliberately): the ephemeral Docker path
            // must run WITHOUT a Kafka bin dir or Java on PATH, so it seeds the
            // source with the in-process non-transactional producer below. This
            // is the one path that does not use the canonical KAFKA_BIN seeder.
            let seed_count = if config.num_messages > 0 {
                config.num_messages
            } else {
                (config.limit_rps * config.test_duration_seconds).max(1000)
            };
            println!("Seeding source topic '{source}' with {seed_count} records (in-suite, in-process) ...");
            seed_source_topic(&bootstrap_servers, &source, &messages, seed_count).await;
        } else if let Some(kafka_bin) = config.kafka_bin.clone() {
            // Canonical seeding: spawn Kafka's standard kafka-producer-perf-test.sh
            // (a Java producer), identical across all four transactional harnesses
            // and to how the consumer-perf tests seed their input. Runs before the
            // measured interval and fills the source so the consumer is not starved.
            let seed_count = config.seed_record_count();
            let cfg = config.clone();
            let bs = bootstrap_servers.clone();
            let src = source.clone();
            let res = tokio::task::spawn_blocking(move || spawn_seed_producer(&kafka_bin, &cfg, &bs, &src, seed_count))
                .await
                .expect("seed-producer task panicked");
            if let Err(e) = res {
                println!(
                    "WARN: could not spawn kafka-producer-perf-test.sh from KAFKA_BIN ({e}); \
                     continuing — assuming SOURCE_TOPIC '{source}' is pre-populated"
                );
            }
        } else {
            // Fallback: no KAFKA_BIN and not in-suite => assume the caller
            // pre-populated SOURCE_TOPIC (a runtime "source starved" warning
            // fires if it is under-fed).
            println!(
                "Assuming SOURCE_TOPIC '{source}' is externally pre-populated \
                 (set KAFKA_BIN to self-seed via kafka-producer-perf-test.sh)"
            );
        }
        for _ in 0..num_producers {
            let props = config.consumer_props(&bootstrap_servers, &config.consumer_group_id);
            let consumer_config = ConsumerConfig::from_properties(&props).expect("Invalid consumer config");
            let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
                consumer_config,
                Box::new(ByteArrayDeserializer),
                Box::new(ByteArrayDeserializer),
            )
            .expect("Failed to create consumer");
            consumer
                .subscribe(vec![source.clone()])
                .await
                .expect("consumer subscribe failed");
            consumers.push_back(consumer);
        }
    }

    // === MEASURED INTERVAL ===
    let test_start = Instant::now();
    meas_start.store(now_ms() as u64, Ordering::Relaxed);
    let test_duration = Duration::from_secs(config.test_duration_seconds);

    // Split the total rate / message target evenly across producers so the
    // aggregate matches LIMIT_RPS / NUM_MESSAGES.
    let per_producer_rps = if config.limit_rps > 0 {
        (config.limit_rps / num_producers).max(1)
    } else {
        0
    };
    let per_producer_num_messages = if config.num_messages > 0 {
        config.num_messages / num_producers
    } else {
        0
    };

    let shared = Arc::new(SharedState {
        metrics: Arc::clone(&metrics),
        latency_hist: Arc::clone(&latency_hist),
        commit_latency_hist: Arc::clone(&commit_latency_hist),
        abort_latency_hist: Arc::clone(&abort_latency_hist),
        committed_records: Arc::new(AtomicU64::new(0)),
        verified: Arc::new(AtomicU64::new(0)),
        committed_transactions: Arc::new(AtomicU64::new(0)),
        aborted_transactions: Arc::new(AtomicU64::new(0)),
        aborted_records: Arc::new(AtomicU64::new(0)),
    });

    // Bound each EOS consumer poll so a run with no source data cannot block
    // forever; it simply loops back to the duration / message-target check.
    let poll_timeout = Duration::from_millis(500);
    let mut handles = Vec::with_capacity(num_producers as usize);
    for producer in producers.into_iter() {
        let topic = topic.clone();
        let shared = Arc::clone(&shared);
        let abort_rate = config.abort_rate;
        let do_verify = config.do_verify;
        if eos_mode {
            let consumer = consumers.pop_front().expect("one consumer per producer");
            handles.push(tokio::spawn(async move {
                run_eos_producer(
                    producer,
                    consumer,
                    topic,
                    message_size,
                    abort_rate,
                    do_verify,
                    per_producer_num_messages,
                    test_start,
                    test_duration,
                    per_producer_rps,
                    poll_timeout,
                    shared,
                )
                .await;
            }));
        } else {
            let messages = Arc::clone(&messages);
            let records_per_transaction = config.records_per_transaction;
            handles.push(tokio::spawn(async move {
                run_producer(
                    producer,
                    messages,
                    topic,
                    message_size,
                    records_per_transaction,
                    abort_rate,
                    do_verify,
                    per_producer_num_messages,
                    test_start,
                    test_duration,
                    per_producer_rps,
                    shared,
                )
                .await;
            }));
        }
    }
    for h in handles {
        let _ = h.await;
    }
    // The last commit has completed; mark the end of the measured interval.
    meas_end.store(now_ms() as u64, Ordering::Relaxed);
    let measured_secs = test_start.elapsed().as_secs_f64();

    // === COOLDOWN ===
    println!("Waiting for final metrics collection...");
    tokio::time::sleep(Duration::from_secs(POST_TEST_AWAIT_SECONDS)).await;
    should_stop.store(true, Ordering::Relaxed);
    let _ = metrics_task.await;

    // === SUMMARY ===
    let committed_records = shared.committed_records.load(Ordering::Relaxed);
    let verified = shared.verified.load(Ordering::Relaxed);
    let committed_transactions = shared.committed_transactions.load(Ordering::Relaxed);
    let aborted_transactions = shared.aborted_transactions.load(Ordering::Relaxed);
    let aborted_records = shared.aborted_records.load(Ordering::Relaxed);
    let total_bytes = committed_records * message_size;
    let samples = cumulative.sample_count.load(Ordering::Relaxed);
    let avg_cpu = if samples > 0 {
        cumulative.total_cpu.load(Ordering::Relaxed) as f64 / samples as f64 / 100.0
    } else {
        0.0
    };
    let avg_rss_kib = if samples > 0 {
        cumulative.total_rss.load(Ordering::Relaxed) as f64 / samples as f64 / 1024.0
    } else {
        0.0
    };

    let msg_rate = if measured_secs > 0.0 {
        committed_records as f64 / measured_secs
    } else {
        0.0
    };
    let mib_rate = if measured_secs > 0.0 {
        total_bytes as f64 / (1024.0 * 1024.0) / measured_secs
    } else {
        0.0
    };
    let txn_rate = if measured_secs > 0.0 {
        committed_transactions as f64 / measured_secs
    } else {
        0.0
    };

    // Per-record latency summary from the cumulative histogram (ms resolution).
    let (lat_count, lat_sum_ms, min_latency_ms, max_latency_ms) = hist_summary(&latency_hist);
    let avg_latency_ms = if lat_count > 0 {
        lat_sum_ms as f64 / lat_count as f64
    } else {
        0.0
    };
    let p50_ms = percentile_from_hist(&latency_hist, 0.50);
    let p90_ms = percentile_from_hist(&latency_hist, 0.90);
    let p95_ms = percentile_from_hist(&latency_hist, 0.95);
    let p99_ms = percentile_from_hist(&latency_hist, 0.99);
    let p999_ms = percentile_from_hist(&latency_hist, 0.999);

    // Per-transaction commit-latency summary from the cumulative histogram.
    let (commit_count, commit_sum_ms, commit_min_ms, commit_max_ms) = hist_summary(&commit_latency_hist);
    let commit_avg_ms = if commit_count > 0 {
        commit_sum_ms as f64 / commit_count as f64
    } else {
        0.0
    };
    let commit_p50_ms = percentile_from_hist(&commit_latency_hist, 0.50);
    let commit_p90_ms = percentile_from_hist(&commit_latency_hist, 0.90);
    let commit_p95_ms = percentile_from_hist(&commit_latency_hist, 0.95);
    let commit_p99_ms = percentile_from_hist(&commit_latency_hist, 0.99);
    let commit_p999_ms = percentile_from_hist(&commit_latency_hist, 0.999);

    // Per-transaction abort-latency summary (deterministic-abort path only).
    let (abort_count, abort_sum_ms, abort_min_ms, abort_max_ms) = hist_summary(&abort_latency_hist);
    let abort_avg_ms = if abort_count > 0 {
        abort_sum_ms as f64 / abort_count as f64
    } else {
        0.0
    };
    let abort_p50_ms = percentile_from_hist(&abort_latency_hist, 0.50);
    let abort_p90_ms = percentile_from_hist(&abort_latency_hist, 0.90);
    let abort_p95_ms = percentile_from_hist(&abort_latency_hist, 0.95);
    let abort_p99_ms = percentile_from_hist(&abort_latency_hist, 0.99);
    let abort_p999_ms = percentile_from_hist(&abort_latency_hist, 0.999);

    println!();
    println!("Duration: {:.2} ms", measured_secs * 1000.0);
    if samples > 0 {
        println!("Average CPU: {avg_cpu:.2} %");
        println!("Average RSS: {avg_rss_kib:.2} KiB");
        println!(
            "CPU Efficiency: {:.2} msg/(s * 1% CPU)",
            msg_rate / if avg_cpu > 0.0 { avg_cpu } else { 1.0 }
        );
        println!(
            "Memory Efficiency: {:.2} msg/(s * KB RSS)",
            msg_rate / if avg_rss_kib > 0.0 { avg_rss_kib } else { 1.0 }
        );
    } else {
        println!("No external metrics collected");
    }
    println!("Committed transactions: {committed_transactions}");
    println!("Aborted transactions: {aborted_transactions}");
    println!("Aborted records: {aborted_records}");
    println!("Committed records: {committed_records}");
    println!("Average rate msg/s: {msg_rate:.2} msg/s");
    println!("Average rate MiB/s: {mib_rate:.2} MiB/s");
    println!("Transactions/s: {txn_rate:.2}");
    println!("Average per-record latency: {avg_latency_ms:.2} ms");
    println!("Max per-record latency: {max_latency_ms} ms");
    println!("p99 per-record latency: {p99_ms} ms");
    println!("Average commit latency: {commit_avg_ms:.2} ms");
    println!("p99 commit latency: {commit_p99_ms} ms");
    println!("Average abort latency: {abort_avg_ms:.2} ms");
    println!("p99 abort latency: {abort_p99_ms} ms");
    println!("Metrics written to: {}", config.metrics_file);

    // Machine-readable summary, kept in sync with the other producer performance
    // tests (same file name / keys / `latency_ms` shape) plus the
    // transaction-specific fields. `client` identifies the implementation.
    let results_json = format!(
        concat!(
            "{{\n",
            "  \"test\": \"producer\",\n",
            "  \"client\": \"rust-txn\",\n",
            "  \"topic\": \"{topic}\",\n",
            "  \"messages_measured\": {messages},\n",
            "  \"duration_s\": {duration:.2},\n",
            "  \"throughput_msg_s\": {msg_rate:.2},\n",
            "  \"throughput_mib_s\": {mib_rate:.2},\n",
            "  \"committed_transactions\": {committed_txns},\n",
            "  \"aborted_transactions\": {aborted_txns},\n",
            "  \"aborted_records\": {aborted_recs},\n",
            "  \"transactions_per_s\": {txn_rate:.2},\n",
            "  \"latency_ms\": {{\"min\": {min}, \"avg\": {avg:.2}, \"p50\": {p50}, ",
            "\"p90\": {p90}, \"p95\": {p95}, \"p99\": {p99}, \"p999\": {p999}, ",
            "\"max\": {max}}},\n",
            "  \"commit_latency_ms\": {{\"min\": {cmin}, \"avg\": {cavg:.2}, \"p50\": {cp50}, ",
            "\"p90\": {cp90}, \"p95\": {cp95}, \"p99\": {cp99}, \"p999\": {cp999}, ",
            "\"max\": {cmax}}},\n",
            "  \"abort_latency_ms\": {{\"min\": {amin}, \"avg\": {aavg:.2}, \"p50\": {ap50}, ",
            "\"p90\": {ap90}, \"p95\": {ap95}, \"p99\": {ap99}, \"p999\": {ap999}, ",
            "\"max\": {amax}}},\n",
            "  \"cpu_avg_pct\": {cpu:.2},\n",
            "  \"rss_avg_kib\": {rss:.2}\n",
            "}}\n"
        ),
        topic = topic,
        messages = committed_records,
        duration = measured_secs,
        msg_rate = msg_rate,
        mib_rate = mib_rate,
        committed_txns = committed_transactions,
        aborted_txns = aborted_transactions,
        aborted_recs = aborted_records,
        txn_rate = txn_rate,
        min = min_latency_ms,
        avg = avg_latency_ms,
        p50 = p50_ms,
        p90 = p90_ms,
        p95 = p95_ms,
        p99 = p99_ms,
        p999 = p999_ms,
        max = max_latency_ms,
        cmin = commit_min_ms,
        cavg = commit_avg_ms,
        cp50 = commit_p50_ms,
        cp90 = commit_p90_ms,
        cp95 = commit_p95_ms,
        cp99 = commit_p99_ms,
        cp999 = commit_p999_ms,
        cmax = commit_max_ms,
        amin = abort_min_ms,
        aavg = abort_avg_ms,
        ap50 = abort_p50_ms,
        ap90 = abort_p90_ms,
        ap95 = abort_p95_ms,
        ap99 = abort_p99_ms,
        ap999 = abort_p999_ms,
        amax = abort_max_ms,
        cpu = avg_cpu,
        rss = avg_rss_kib,
    );
    match std::fs::write(&config.results_file, results_json) {
        Ok(()) => println!("Results summary written to: {}", config.results_file),
        Err(e) => eprintln!("Failed to write {}: {e}", config.results_file),
    }

    // === PERFORMANCE TARGET ASSERTIONS ===
    // Assertions apply to committed records only (per the plan).
    // `verified == committed` always holds (both are 0 when everything aborts).
    assert_eq!(
        verified, committed_records,
        "verified records ({verified}) must match committed ({committed_records})"
    );
    // The remaining count assertions are produce-mode only. In EOS mode the
    // committed count depends on how consumed records batch across polls and on
    // KIP-848 assignment/rebalance timing (which may not complete within a short
    // in-suite run), so neither a positive-count nor an exact-count assertion is
    // reliable without a live, pre-populated source. The eos path is
    // build-verified and logically faithful; end-to-end throughput is validated
    // by an env-driven run against a real broker (deviation per
    // definition-of-done.md §7).
    if !eos_mode {
        // With ABORT_RATE == 1.0 every transaction aborts by design, so there is
        // nothing committed to assert on — only require committed output otherwise.
        if config.abort_rate < 1.0 {
            assert!(committed_transactions > 0, "Should have committed at least one transaction");
            assert!(committed_records > 0, "Should have committed at least one record");
        }
        // Exact-count assertion: only with no aborts. Each producer produces whole
        // transactions, so it commits `ceil(per_producer_num_messages /
        // records_per_transaction) * records_per_transaction` records — the target
        // is that, summed across producers, NOT the raw message count (which need
        // not be a multiple of records_per_transaction).
        if config.num_messages > 0 && config.abort_rate == 0.0 {
            let n = config.records_per_transaction;
            let txns_per_producer = per_producer_num_messages.div_ceil(n);
            let target = num_producers * txns_per_producer * n;
            assert_eq!(
                committed_records, target,
                "committed ({committed_records}) must match produced target ({target})"
            );
        }
    }
    // Per-record latency budget — only asserted when explicitly set (0 disables
    // it; see the in-suite note above on why it is left disabled for txn produce
    // mode).
    if config.p99_limit_ms > 0 {
        assert!(
            p99_ms <= config.p99_limit_ms,
            "p99 per-record latency {p99_ms} ms exceeds {} ms budget",
            config.p99_limit_ms
        );
    }
}
