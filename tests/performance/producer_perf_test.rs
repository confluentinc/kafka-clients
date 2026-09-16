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

//! Producer performance test — measures throughput, latency, CPU, and memory.
//!
//! IMPORTANT: this test MUST be kept in sync with all other producer
//! performance tests in the project — the same configuration contract, message
//! shape, metrics-file schema, and reported numbers — so that results from the
//! different implementations are directly comparable.
//!
//! Two ways to run:
//!
//! * As part of the integration suite (short, asserted, Docker broker):
//!   `cargo test --features integration-tests --test integration -- producer_perf_test --nocapture`
//!   The in-suite defaults are short (10 s, 2 KiB values, ~100 msg/s) so the
//!   run is quick and the latency budget is meaningful.
//!
//! * As an env-driven benchmark binary (long runs, external broker):
//!   `cargo xtask producer-perf-test` (set env vars first; see table below).
//!
//! Configuration via environment variables (all optional). Sizes use the same
//! units as the other producer performance tests: `BATCH_SIZE`/`MAX_REQUEST_SIZE`
//! are KiB, `BUFFER_MEMORY` is MiB.
//!
//! | Variable               | Default            | Description                                |
//! |------------------------|--------------------|--------------------------------------------|
//! | `BOOTSTRAP_SERVERS`    | (Docker)           | Kafka broker address                       |
//! | `TOPIC_NAME`           | `test-topic`       | Topic to produce to                        |
//! | `NUM_MESSAGES`         | `0` (time-based)   | Total messages (0 = unlimited)             |
//! | `LIMIT_RPS`            | `0` unlimited *    | Rate limit in msg/s (0 = unlimited)        |
//! | `KEY_SIZE`             | `0`                | Key size in bytes (0 = no key)             |
//! | `VALUE_SIZE`           | `2048`             | Value size in bytes                        |
//! | `BATCH_SIZE`           | `1024` (1 MiB)     | `batch.size` in **KiB** (×1024)            |
//! | `MAX_REQUEST_SIZE`     | (`batch×64`, ≤8MiB)| `max.request.size` in **KiB** (×1024), capped 8 MiB |
//! | `BUFFER_MEMORY`        | (client default)   | `buffer.memory` in **MiB** (×1024×1024)    |
//! | `LINGER_MS`            | `5`                | `linger.ms`                                |
//! | `MAX_IN_FLIGHT`        | (client default)   | `max.in.flight.requests.per.connection`    |
//! | `COMPRESSION_TYPE`     | `none`             | Compression: none/gzip/snappy/lz4/zstd     |
//! | `ENABLE_IDEMPOTENCE`   | `false`            | `enable.idempotence`                       |
//! | `USE_DEFAULTS`         | `False`            | Omit all tuning knobs; use client defaults |
//! | `WARMUP_SECONDS`       | `120` *            | Warmup duration                            |
//! | `TEST_DURATION_SECONDS`| `600` *            | Measured interval duration                 |
//! | `DO_VERIFY`            | `True`             | Verify `RecordMetadata` per message        |
//! | `VERIFY_CONSUMED`      | `False`            | Consumer-side verification (not yet avail.)|
//! | `P99_LIMIT_MS`         | `0` off *          | Per-message p99 latency budget (asserted)  |
//! | `SECURITY_PROTOCOL`    | (none)             | `PLAINTEXT`/`SASL_PLAINTEXT`/`SASL_SSL`     |
//! | `SASL_MECHANISM`       | (none)             | e.g. `PLAIN`                               |
//! | `SASL_USERNAME`        | (none)             | SASL username                              |
//! | `SASL_PASSWORD`        | (none)             | SASL password                              |
//! | `METRICS_FILE`         | `metrics.jsonl`    | Output file path                           |
//!
//! The defaults marked `*` are overridden for the in-suite integration
//! run (no `BOOTSTRAP_SERVERS`) to keep it short and latency-asserted:
//! `TEST_DURATION_SECONDS=10`, `LIMIT_RPS=100`, `P99_LIMIT_MS=70`,
//! `WARMUP_SECONDS=0`. Setting any of them explicitly disables that override.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use confluent_kafka::common::serialization::ByteArraySerializer;
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

/// Seconds to keep sampling metrics after the last message's response, so the
/// JSONL captures post-measurement (cooldown) windows. Mirrors the C test's
/// trailing wait. These windows carry `measurement_end_ms`, so they appear in
/// the plot but are excluded from the measured-interval statistics.
const POST_TEST_AWAIT_SECONDS: u64 = 10;

/// Linux `USER_HZ` — clock ticks per second used to convert `/proc/self/stat`
/// utime/stime into CPU seconds. Effectively always 100 on Linux.
const USER_HZ: f64 = 100.0;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

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
    enable_idempotence: String,
    use_defaults: bool,
    warmup_seconds: u64,
    test_duration_seconds: u64,
    do_verify: bool,
    verify_consumed: bool,
    p99_limit_ms: u64,
    security_protocol: Option<String>,
    sasl_mechanism: Option<String>,
    sasl_username: Option<String>,
    sasl_password: Option<String>,
    metrics_file: String,
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

        // Full defaults match the other producer performance tests (600 s,
        // unlimited rate). The in-suite integration run overrides these in
        // `producer_perf_test()`.
        let test_duration_seconds = env_parse("TEST_DURATION_SECONDS", 600);
        let limit_rps = env_parse("LIMIT_RPS", 0);
        // When rate-limited, the run is bounded by rps × duration.
        let mut num_messages = env_parse("NUM_MESSAGES", 0u64);
        if limit_rps > 0 {
            num_messages = limit_rps * test_duration_seconds;
        }

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
            enable_idempotence: env_or("ENABLE_IDEMPOTENCE", "false"),
            use_defaults: env_or("USE_DEFAULTS", "False") == "True",
            warmup_seconds: env_parse("WARMUP_SECONDS", 120),
            test_duration_seconds,
            do_verify: env_or("DO_VERIFY", "True") == "True",
            verify_consumed: env_or("VERIFY_CONSUMED", "False") == "True",
            p99_limit_ms: env_parse("P99_LIMIT_MS", 0),
            security_protocol: env_opt("SECURITY_PROTOCOL"),
            sasl_mechanism: env_opt("SASL_MECHANISM"),
            sasl_username: env_opt("SASL_USERNAME"),
            sasl_password: env_opt("SASL_PASSWORD"),
            metrics_file: env_or("METRICS_FILE", "metrics.jsonl"),
        }
    }

    fn message_size(&self) -> usize {
        self.key_size + self.value_size
    }

    /// Build the producer property map shared with the other producer
    /// performance tests (config + SASL config from the environment).
    fn producer_props(&self, bootstrap_servers: &str) -> HashMap<String, String> {
        let mut props = HashMap::from([
            ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
            ("client.id".to_string(), "perf-test-rust".to_string()),
        ]);
        // With USE_DEFAULTS the client runs at its own defaults: only the
        // bootstrap servers, client id and SASL credentials are set, and every
        // performance-tuning knob is omitted. Mirrors the Python producer
        // performance test's USE_DEFAULTS behavior.
        if !self.use_defaults {
            props.insert("batch.size".to_string(), self.batch_size.to_string());
            props.insert("max.request.size".to_string(), self.max_request_size.to_string());
            props.insert("compression.type".to_string(), self.compression_type.clone());
            props.insert("enable.idempotence".to_string(), self.enable_idempotence.clone());
            // `acks` is intentionally not set, so it relies on the client default
            // (acks = all / -1), consistent with the other performance tests.
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

// ---------------------------------------------------------------------------
// Interval metrics (reset each 1 s window)
// ---------------------------------------------------------------------------

struct Metrics {
    messages_sent: AtomicU64,
    bytes_sent: AtomicU64,
    total_latency_us: AtomicU64,
    max_latency_us: AtomicU64,
    // Per-window latency histogram (ms resolution) for the p50/p90/p99/p999
    // percentiles emitted each window; read-and-reset on every rollover.
    latency_hist: Vec<AtomicU64>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            max_latency_us: AtomicU64::new(0),
            latency_hist: (0..=MAX_LATENCY_MS + 1).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn record_success(&self, latency_us: u64, bytes: u64) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        self.total_latency_us.fetch_add(latency_us, Ordering::Relaxed);
        self.max_latency_us.fetch_max(latency_us, Ordering::Relaxed);
        let ms = (latency_us / 1000) as usize;
        self.latency_hist[ms.min(MAX_LATENCY_MS + 1)].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot_and_reset(&self) -> MetricsSnapshot {
        // Read-and-reset the per-window latency histogram, then derive percentiles.
        let counts: Vec<u64> = self.latency_hist.iter().map(|b| b.swap(0, Ordering::Relaxed)).collect();
        MetricsSnapshot {
            messages: self.messages_sent.swap(0, Ordering::Relaxed),
            bytes: self.bytes_sent.swap(0, Ordering::Relaxed),
            total_latency_us: self.total_latency_us.swap(0, Ordering::Relaxed),
            max_latency_us: self.max_latency_us.swap(0, Ordering::Relaxed),
            p50_ms: percentile_from_counts(&counts, 0.50),
            p90_ms: percentile_from_counts(&counts, 0.90),
            p99_ms: percentile_from_counts(&counts, 0.99),
            p999_ms: percentile_from_counts(&counts, 0.999),
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
}

// ---------------------------------------------------------------------------
// Process stats (CPU / RSS) — interval sampling, not a lifetime average.
//
// `/proc/self/stat` gives cumulative utime+stime in clock ticks; we diff over
// the window to get interval CPU% (NOT the lifetime average `ps -o %cpu`
// reports). RSS comes from `/proc/self/status` VmRSS (kB). Linux-only, which
// matches the run environment; no extra crate (CLAUDE.md rule 1).
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
// no key when key_size == 0.
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
    // Future-queue (in-flight) depth, sampled once per measured window. By
    // Little's Law the average should sit at throughput × latency (λ·W); a
    // depth that grows over time signals the completion side lagging.
    total_queue: AtomicU64,
    max_queue: AtomicU64,
}

impl CumulativeStats {
    fn new() -> Self {
        Self {
            total_cpu: AtomicU64::new(0),
            total_rss: AtomicU64::new(0),
            sample_count: AtomicU64::new(0),
            total_queue: AtomicU64::new(0),
            max_queue: AtomicU64::new(0),
        }
    }

    fn accumulate(&self, cpu: f64, rss: u64, queue_depth: u64) {
        self.total_cpu.fetch_add((cpu * 100.0) as u64, Ordering::Relaxed);
        self.total_rss.fetch_add(rss, Ordering::Relaxed);
        self.sample_count.fetch_add(1, Ordering::Relaxed);
        self.total_queue.fetch_add(queue_depth, Ordering::Relaxed);
        self.max_queue.fetch_max(queue_depth, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// metrics.jsonl rollover schema consumed by
// tools/performance_metrics_plot/plot_metrics.py (shared by all the producer
// performance tests).
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
        "window_start_ms": window_start_ms.to_string(),
        "window_end_ms": window_end_ms.to_string(),
        "measurement_start_ms": measurement_start_ms.map(|v| v.to_string()).unwrap_or_else(|| "-inf".to_string()),
        "measurement_end_ms": measurement_end_ms.map(|v| v.to_string()).unwrap_or_else(|| "-inf".to_string()),
    });
    line.to_string()
}

// ---------------------------------------------------------------------------
// Latency histogram (for p99 assertion)
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

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn producer_perf_test() {
    let _ = env_logger::builder().is_test(false).try_init();

    let mut config = PerfTestConfig::from_env();
    // In-suite integration run (no external broker → an ephemeral Docker
    // cluster): keep it short, rate-limited and latency-asserted so it fits the
    // normal test suite. Standalone benchmarks point BOOTSTRAP_SERVERS at a real
    // broker (or set the env vars explicitly) and get the full defaults.
    if config.bootstrap_servers.is_empty() {
        if std::env::var("TEST_DURATION_SECONDS").is_err() {
            config.test_duration_seconds = 10;
        }
        if std::env::var("LIMIT_RPS").is_err() {
            config.limit_rps = 100;
        }
        if std::env::var("P99_LIMIT_MS").is_err() {
            config.p99_limit_ms = 70;
        }
        // The 120 s standalone-benchmark warmup default would dominate the
        // short in-suite run, so skip warmup unless explicitly requested.
        if std::env::var("WARMUP_SECONDS").is_err() {
            config.warmup_seconds = 0;
        }
        if config.limit_rps > 0 && std::env::var("NUM_MESSAGES").is_err() {
            config.num_messages = config.limit_rps * config.test_duration_seconds;
        }
    }
    let message_size = config.message_size() as u64;

    // --- Broker setup ---
    let (_ctx, bootstrap_servers, topic) = if config.bootstrap_servers.is_empty() {
        let mut ctx = TestContext::new(ClusterConfig::default()).await;
        let topic = ctx.topic(&config.topic_name);
        let bs = ctx.bootstrap_servers().to_string();
        (Some(ctx), bs, topic)
    } else {
        (None, config.bootstrap_servers.clone(), config.topic_name.clone())
    };

    println!("=== Rust Producer Performance Test ===");
    println!("Bootstrap servers: {bootstrap_servers}");
    println!("Topic: {topic}");
    println!(
        "Key size: {} B, Value size: {} B, Message size: {} B",
        config.key_size, config.value_size, message_size
    );
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
        println!("enable.idempotence: {}", config.enable_idempotence);
    }
    println!("Verify: {}", config.do_verify);
    println!(
        "Warmup: {} s, Test duration: {} s",
        config.warmup_seconds, config.test_duration_seconds
    );
    if config.num_messages > 0 {
        println!("Num messages: {}", config.num_messages);
    }
    if config.limit_rps > 0 {
        println!("Rate limit: {} msg/s", config.limit_rps);
    }

    // --- Create producer ---
    let props = config.producer_props(&bootstrap_servers);
    let producer_config = ProducerConfig::new(&props).expect("Invalid producer config");

    let producer = KafkaProducer::<Vec<u8>, Vec<u8>>::new_config(
        producer_config,
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("Failed to create producer");

    // --- Consumer-side verification (VERIFY_CONSUMED) ---
    // Blocked on master: the Consumer API is not yet merged here (only on the
    // unmerged `consumer-impl` branch). murmur2/partition_for_key DO exist, so
    // this can be wired up once a Consumer lands.
    if config.verify_consumed {
        println!(
            "Consumer verification requested (VERIFY_CONSUMED=True) but skipped: \
             the Consumer API is not available in this build."
        );
    }

    // --- Pre-generate messages ---
    let messages = message_generator(GENERATED_MESSAGES, config.key_size, config.value_size);
    let msg_count = messages.len();

    // --- Shared state ---
    let metrics = Arc::new(Metrics::new());
    let cumulative = Arc::new(CumulativeStats::new());
    let should_stop = Arc::new(AtomicBool::new(false));

    let messages_sent = Arc::new(AtomicU64::new(0));
    let completed_messages = Arc::new(AtomicU64::new(0));
    let in_flight = Arc::new(AtomicU64::new(0));
    let verified = Arc::new(AtomicU64::new(0));
    let latency_hist: Arc<Vec<AtomicU64>> = Arc::new((0..=MAX_LATENCY_MS + 1).map(|_| AtomicU64::new(0)).collect());

    // === METRICS COLLECTOR ===
    // Spawned BEFORE warmup so the rollover JSONL also captures warmup windows
    // (measurement_start_ms = -inf) and the post-test cooldown windows
    // (measurement_end_ms set). The measured interval is delimited by the
    // `meas_start` / `meas_end` atomics (0 = unset → "-inf" in the JSONL); a
    // window counts as measured only while start is set and end is not.
    let meas_start = Arc::new(AtomicU64::new(0));
    let meas_end = Arc::new(AtomicU64::new(0));
    let metrics_for_collector = Arc::clone(&metrics);
    let metrics_cumul = Arc::clone(&cumulative);
    let in_flight_for_collector = Arc::clone(&in_flight);
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
            // The JSONL line below is written for every window (warmup, measured,
            // cooldown), but the run SUMMARY must reflect only the measured
            // interval: accumulate CPU/RSS only while measurement_start is set
            // and measurement_end is not (between meas_start and the last
            // response). Warmup and cooldown windows are excluded.
            if start != 0 && end == 0 {
                let queue_depth = in_flight_for_collector.load(Ordering::Relaxed);
                metrics_cumul.accumulate(cpu, rss, queue_depth);
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
    // Send one record, await + verify it, sleep 100 ms between. The collector is
    // already running, so warmup windows appear in the JSONL with
    // measurement_start_ms = -inf (excluded from the measured statistics).
    if config.warmup_seconds > 0 {
        println!("Warming up for {} seconds ...", config.warmup_seconds);
        let warmup_end = Instant::now() + Duration::from_secs(config.warmup_seconds);
        let mut i = 0usize;
        while Instant::now() < warmup_end {
            let (key, value) = &messages[i % msg_count];
            let record: ProducerRecord<&[u8], &[u8]> =
                ProducerRecord::new_key(topic.clone(), key.as_deref(), Some(value.as_slice()));
            if let Ok(produce_call) = producer.send(record, None).await {
                let md = produce_call
                    .get_with_timeout(Duration::from_secs(30))
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

    // === MEASURED INTERVAL ===
    let test_start = Instant::now();
    // Mark the measured interval start; the collector picks this up on its next
    // window. `meas_end` is set when the last message's response is received.
    meas_start.store(now_ms() as u64, Ordering::Relaxed);
    let test_duration = Duration::from_secs(config.test_duration_seconds);

    // -- send loop --
    // Rate limiting: every `limit_rps` messages, sleep until the
    // next 1 s boundary (relative to the measured-interval start).
    let mut next_check_time = Duration::from_secs(1);

    // Single completion task consuming futures over a channel (avoids per-message
    // tokio::spawn cost): resolves each future, records latency, and verifies
    // metadata.
    use confluent_kafka::common::KafkaFuture;
    use confluent_kafka::producer::RecordMetadata;
    type FutureType = KafkaFuture<RecordMetadata>;
    let (produce_calls_tx, mut produce_calls_rx) = tokio::sync::mpsc::unbounded_channel::<(FutureType, Instant)>();
    let metrics_for_completion = Arc::clone(&metrics);
    let completed_messages_for_completion = Arc::clone(&completed_messages);
    let in_flight_for_completion = Arc::clone(&in_flight);
    let verified_for_completion = Arc::clone(&verified);
    let latency_hist_for_completion = Arc::clone(&latency_hist);
    let topic_for_completion = topic.clone();
    let do_verify = config.do_verify;
    let meas_end_completion = Arc::clone(&meas_end);
    let record_completed_calls_loop = tokio::spawn(async move {
        let record_completed_calls = |result: Result<RecordMetadata, _>, start_time: Instant| {
            match result {
                Ok(md) => {
                    let latency_us = start_time.elapsed().as_micros() as u64;
                    metrics_for_completion.record_success(latency_us, message_size);
                    let ms = (latency_us / 1000) as usize;
                    latency_hist_for_completion[ms.min(MAX_LATENCY_MS + 1)].fetch_add(1, Ordering::Relaxed);
                    // verify_record_metadata: !do_verify counts all; do_verify
                    // counts only when metadata is valid. On error neither branch
                    // increments `verified`, so `verified == completed` fails.
                    if !do_verify || verify_record_metadata(&md, &topic_for_completion) {
                        verified_for_completion.fetch_add(1, Ordering::Relaxed);
                    }
                },
                Err(e) => {
                    eprintln!("Produce call resulted in an error: {e:?}");
                },
            }
            completed_messages_for_completion.fetch_add(1, Ordering::Relaxed);
            in_flight_for_completion.fetch_sub(1, Ordering::Relaxed);
        };

        // Process completions in send order, one future at a time — mirrors the
        // C performance test's single blocking-`get` consumer. Awaiting a single
        // future keeps the in-flight backlog passive in the channel (cheap
        // handles) rather than activating the whole set in a `FuturesUnordered`,
        // whose per-poll cost grows with the set size and collapses throughput
        // at max rate. This keeps pace with the producer, so the unbounded
        // channel never deeply fills.
        while let Some((produce_call, start_time)) = produce_calls_rx.recv().await {
            let result = produce_call.get_with_timeout(Duration::from_secs(60)).await;
            record_completed_calls(result, start_time);
        }
        // The channel is closed and drained: the response for the last message
        // sent has just been received, so mark the end of the measured interval.
        meas_end_completion.store(now_ms() as u64, Ordering::Relaxed);
    });

    loop {
        // A counted run is bounded by `num_messages`; a time-based run
        // (num_messages == 0) is bounded by the duration.
        if config.num_messages > 0 {
            if messages_sent.load(Ordering::Relaxed) >= config.num_messages {
                break;
            }
        } else if test_start.elapsed() >= test_duration {
            break;
        }

        let (key, value) = &messages[messages_sent.load(Ordering::Relaxed) as usize % msg_count];
        let record: ProducerRecord<&[u8], &[u8]> =
            ProducerRecord::new_key(topic.clone(), key.as_deref(), Some(value.as_slice()));

        let start_time = Instant::now();
        match producer.send(record, None).await {
            Ok(produce_call) => {
                messages_sent.fetch_add(1, Ordering::Relaxed);
                in_flight.fetch_add(1, Ordering::Relaxed);
                let _ = produce_calls_tx.send((produce_call, start_time));
            },
            Err(e) => {
                eprintln!("Send error: {e:?}");
            },
        }

        if config.limit_rps > 0 && messages_sent.load(Ordering::Relaxed).is_multiple_of(config.limit_rps) {
            let elapsed = test_start.elapsed();
            if elapsed < next_check_time {
                tokio::time::sleep(next_check_time - elapsed).await;
            }
            next_check_time += Duration::from_secs(1);
        }
    }

    // === COOLDOWN ===
    println!(
        "Stopping after {} messages sent, waiting for in-flight ...",
        messages_sent.load(Ordering::Relaxed)
    );
    drop(produce_calls_tx);
    while in_flight.load(Ordering::Relaxed) > 0 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = record_completed_calls_loop.await;
    // Measured time spans the send loop + drain, captured before the cooldown.
    // (`meas_end` was set inside the completion task on the last response.)
    let measured_secs = test_start.elapsed().as_secs_f64();

    // Post-test await: keep the collector sampling for a short cooldown so the
    // JSONL captures post-measurement windows. They carry measurement_end_ms and
    // are excluded from the measured statistics but appear in the plot.
    println!("Waiting for final metrics collection...");
    tokio::time::sleep(Duration::from_secs(POST_TEST_AWAIT_SECONDS)).await;
    should_stop.store(true, Ordering::Relaxed);
    let _ = metrics_task.await;

    let _ = producer.close().await;

    // === SUMMARY ===
    // Results summary.
    let completed_messages = completed_messages.load(Ordering::Relaxed);
    let verified = verified.load(Ordering::Relaxed);
    let total_bytes = completed_messages * message_size;
    let samples = cumulative.sample_count.load(Ordering::Relaxed);
    let avg_cpu = if samples > 0 {
        cumulative.total_cpu.load(Ordering::Relaxed) as f64 / samples as f64 / 100.0
    } else {
        0.0
    };
    // Average RSS in KiB.
    let avg_rss_kib = if samples > 0 {
        cumulative.total_rss.load(Ordering::Relaxed) as f64 / samples as f64 / 1024.0
    } else {
        0.0
    };

    let msg_rate = if measured_secs > 0.0 {
        completed_messages as f64 / measured_secs
    } else {
        0.0
    };
    let mib_rate = if measured_secs > 0.0 {
        total_bytes as f64 / (1024.0 * 1024.0) / measured_secs
    } else {
        0.0
    };

    // Average/max latency and p99 from the histogram (ms resolution).
    let (lat_count, lat_sum_ms, max_latency_ms) = {
        let (mut count, mut sum, mut max) = (0u64, 0u64, 0u64);
        for (ms, b) in latency_hist.iter().enumerate() {
            let n = b.load(Ordering::Relaxed);
            if n > 0 {
                count += n;
                sum += ms as u64 * n;
                max = ms as u64;
            }
        }
        (count, sum, max)
    };
    let avg_latency_ms = if lat_count > 0 {
        lat_sum_ms as f64 / lat_count as f64
    } else {
        0.0
    };
    let p99_ms = percentile_from_hist(&latency_hist, 0.99);

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
        let avg_queue = cumulative.total_queue.load(Ordering::Relaxed) as f64 / samples as f64;
        let max_queue = cumulative.max_queue.load(Ordering::Relaxed);
        println!("Average future queue size: {avg_queue:.2}");
        println!("Max future queue size: {max_queue}");
    } else {
        println!("No external metrics collected");
    }
    let avg_time_ms = if completed_messages > 0 {
        measured_secs * 1000.0 / completed_messages as f64
    } else {
        0.0
    };
    println!("Average time: {avg_time_ms:.2} ms");
    println!("Average rate msg/s: {msg_rate:.2} msg/s");
    println!("Average rate MiB/s: {mib_rate:.2} MiB/s");
    println!("Average latency: {avg_latency_ms:.2} ms");
    println!("Max latency: {max_latency_ms} ms");
    println!("p99 latency: {p99_ms} ms");
    println!("Metrics written to: {}", config.metrics_file);

    // === PERFORMANCE TARGET ASSERTIONS ===
    // verified == completed, completed == produced target, and the p99 latency
    // budget for the rate-limited in-suite run.
    assert!(completed_messages > 0, "Should have completed at least one message");
    assert_eq!(
        verified, completed_messages,
        "verified records ({verified}) must match completed ({completed_messages})"
    );
    if config.num_messages > 0 {
        assert_eq!(
            completed_messages, config.num_messages,
            "completed ({completed_messages}) must match produced target ({})",
            config.num_messages
        );
    }
    // Latency budget — only asserted when set (P99_LIMIT_MS=0 disables it for
    // max-rate benchmark runs, where queueing makes per-message latency moot).
    if config.p99_limit_ms > 0 {
        assert!(
            p99_ms <= config.p99_limit_ms,
            "p99 latency {p99_ms} ms exceeds {} ms budget",
            config.p99_limit_ms
        );
    }
}

/// Verify a `RecordMetadata`: offset/partition non-negative, topic matches,
/// timestamp present.
fn verify_record_metadata(md: &confluent_kafka::producer::RecordMetadata, topic: &str) -> bool {
    md.offset() >= 0 && md.partition() >= 0 && md.topic() == topic && md.timestamp() >= 0
}
