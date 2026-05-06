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
//! Run: `cargo test --features performance-tests -- performance_test --nocapture`
//!
//! Configuration via environment variables (all optional):
//!
//! | Variable               | Default            | Description                        |
//! |------------------------|--------------------|------------------------------------|
//! | `BOOTSTRAP_SERVERS`    | (Docker)           | Kafka broker address               |
//! | `TOPIC_NAME`           | `performance-test` | Topic to produce to                |
//! | `NUM_MESSAGES`         | `0` (time-based)   | Total messages (0 = unlimited)     |
//! | `LIMIT_RPS`            | `0` (unlimited)    | Rate limit in msg/s                |
//! | `KEY_SIZE`             | `8`                | Key size in bytes                  |
//! | `VALUE_SIZE`           | `1024`             | Value size in bytes                |
//! | `BATCH_SIZE`           | `65536`            | `batch.size` in bytes              |
//! | `BUFFER_MEMORY`        | `33554432`         | `buffer.memory` in bytes           |
//! | `MAX_REQUEST_SIZE`     | `1048576`          | `max.request.size` in bytes        |
//! | `LINGER_MS`            | `10`               | `linger.ms`                        |
//! | `COMPRESSION_TYPE`     | `none`             | Compression: none/gzip/snappy/lz4/zstd |
//! | `WARMUP_SECONDS`       | `10`               | Warmup duration                    |
//! | `TEST_DURATION_SECONDS`| `60`               | Measured interval duration         |
//! | `METRICS_FILE`         | `metrics.jsonl`    | Output file path                   |

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

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
    batch_size: String,
    buffer_memory: String,
    max_request_size: String,
    linger_ms: String,
    compression_type: String,
    max_in_flight: String,
    warmup_seconds: u64,
    test_duration_seconds: u64,
    metrics_file: String,
}

impl PerfTestConfig {
    fn from_env() -> Self {
        Self {
            bootstrap_servers: env_or("BOOTSTRAP_SERVERS", ""),
            topic_name: env_or("TOPIC_NAME", "performance-test"),
            num_messages: env_parse("NUM_MESSAGES", 0),
            limit_rps: env_parse("LIMIT_RPS", 0),
            key_size: env_parse("KEY_SIZE", 8),
            value_size: env_parse("VALUE_SIZE", 1024),
            batch_size: env_or("BATCH_SIZE", "65536"),
            buffer_memory: env_or("BUFFER_MEMORY", "33554432"),
            max_request_size: env_or("MAX_REQUEST_SIZE", "1048576"),
            linger_ms: env_or("LINGER_MS", "10"),
            compression_type: env_or("COMPRESSION_TYPE", "none"),
            max_in_flight: env_or("MAX_IN_FLIGHT", "5"),
            warmup_seconds: env_parse("WARMUP_SECONDS", 10),
            test_duration_seconds: env_parse("TEST_DURATION_SECONDS", 60),
            metrics_file: env_or("METRICS_FILE", "metrics.jsonl"),
        }
    }

    fn message_size(&self) -> usize {
        self.key_size + self.value_size
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

struct IntervalMetrics {
    messages_sent: AtomicU64,
    bytes_sent: AtomicU64,
    total_latency_us: AtomicU64,
    max_latency_us: AtomicU64,
    errors: AtomicU64,
}

impl IntervalMetrics {
    fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            max_latency_us: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    fn record_success(&self, latency_us: u64, bytes: u64) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        self.total_latency_us.fetch_add(latency_us, Ordering::Relaxed);
        self.max_latency_us.fetch_max(latency_us, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot_and_reset(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            messages: self.messages_sent.swap(0, Ordering::Relaxed),
            bytes: self.bytes_sent.swap(0, Ordering::Relaxed),
            total_latency_us: self.total_latency_us.swap(0, Ordering::Relaxed),
            max_latency_us: self.max_latency_us.swap(0, Ordering::Relaxed),
            errors: self.errors.swap(0, Ordering::Relaxed),
        }
    }
}

struct MetricsSnapshot {
    messages: u64,
    bytes: u64,
    total_latency_us: u64,
    max_latency_us: u64,
    errors: u64,
}

impl MetricsSnapshot {
    fn avg_latency_ms(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        self.total_latency_us as f64 / self.messages as f64 / 1000.0
    }

    fn max_latency_ms(&self) -> f64 {
        self.max_latency_us as f64 / 1000.0
    }
}

// ---------------------------------------------------------------------------
// Process stats (CPU / RSS)
// ---------------------------------------------------------------------------

fn get_process_stats() -> (f64, u64) {
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "%cpu=,rss=", "-p", &pid.to_string()])
        .output();
    match output {
        Ok(out) => {
            let text = String::from_utf8_lossy(&out.stdout);
            let parts: Vec<&str> = text.split_whitespace().collect();
            if parts.len() >= 2 {
                let cpu = parts[0].parse::<f64>().unwrap_or(0.0);
                let rss_kb = parts[1].parse::<u64>().unwrap_or(0);
                (cpu, rss_kb * 1024)
            } else {
                (0.0, 0)
            }
        },
        Err(_) => (0.0, 0),
    }
}

// ---------------------------------------------------------------------------
// Message generation
// ---------------------------------------------------------------------------

fn generate_messages(count: usize, key_size: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    use rand::Rng;
    let mut rng = rand::rng();
    (0..count)
        .map(|_| {
            let key: Vec<u8> = (0..key_size).map(|_| rng.random()).collect();
            let value: Vec<u8> = (0..value_size).map(|_| rng.random()).collect();
            (key, value)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Cumulative stats (for final summary)
// ---------------------------------------------------------------------------

struct CumulativeStats {
    total_messages: AtomicU64,
    total_bytes: AtomicU64,
    total_latency_us: AtomicU64,
    max_latency_us: AtomicU64,
    total_errors: AtomicU64,
    total_cpu: AtomicU64,
    total_rss: AtomicU64,
    sample_count: AtomicU64,
}

impl CumulativeStats {
    fn new() -> Self {
        Self {
            total_messages: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            max_latency_us: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            total_cpu: AtomicU64::new(0),
            total_rss: AtomicU64::new(0),
            sample_count: AtomicU64::new(0),
        }
    }

    fn accumulate(&self, snap: &MetricsSnapshot, cpu: f64, rss: u64) {
        self.total_messages.fetch_add(snap.messages, Ordering::Relaxed);
        self.total_bytes.fetch_add(snap.bytes, Ordering::Relaxed);
        self.total_latency_us.fetch_add(snap.total_latency_us, Ordering::Relaxed);
        self.max_latency_us.fetch_max(snap.max_latency_us, Ordering::Relaxed);
        self.total_errors.fetch_add(snap.errors, Ordering::Relaxed);
        self.total_cpu.fetch_add((cpu * 100.0) as u64, Ordering::Relaxed);
        self.total_rss.fetch_add(rss, Ordering::Relaxed);
        self.sample_count.fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn performance_test() {
    let _ = env_logger::builder().is_test(false).try_init();

    let config = PerfTestConfig::from_env();
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
    println!("Batch size: {} B", config.batch_size);
    println!("Buffer memory: {} B", config.buffer_memory);
    println!("Max request size: {} B", config.max_request_size);
    println!("Linger ms: {}", config.linger_ms);
    println!("Compression: {}", config.compression_type);
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
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.clone()),
        ("client.id".to_string(), "perf-test-rust".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("batch.size".to_string(), config.batch_size.clone()),
        ("buffer.memory".to_string(), config.buffer_memory.clone()),
        ("max.request.size".to_string(), config.max_request_size.clone()),
        ("linger.ms".to_string(), config.linger_ms.clone()),
        ("compression.type".to_string(), config.compression_type.clone()),
        (
            "max.in.flight.requests.per.connection".to_string(),
            config.max_in_flight.clone(),
        ),
        ("max.block.ms".to_string(), "60000".to_string()),
    ]);
    let producer_config = ProducerConfig::from_properties(&props).expect("Invalid producer config");

    let producer = KafkaProducer::<Vec<u8>, Vec<u8>>::from_config(
        producer_config,
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("Failed to create producer");

    // --- Pre-generate messages ---
    let messages = generate_messages(10_000, config.key_size, config.value_size);
    let msg_count = messages.len();

    // --- Shared state ---
    let interval_metrics = Arc::new(IntervalMetrics::new());
    let cumulative = Arc::new(CumulativeStats::new());
    let should_stop = Arc::new(AtomicBool::new(false));

    let dbg_sent = Arc::new(AtomicU64::new(0));
    let dbg_completed = Arc::new(AtomicU64::new(0));
    let in_flight = Arc::new(AtomicU64::new(0));

    // === WARMUP ===
    if config.warmup_seconds > 0 {
        println!("Warming up for {} seconds ...", config.warmup_seconds);
        let warmup_end = Instant::now() + Duration::from_secs(config.warmup_seconds);
        let mut i = 0usize;
        while Instant::now() < warmup_end {
            let (key, value) = &messages[i % msg_count];
            let record = ProducerRecord::with_key(topic.clone(), Some(key.as_slice()), Some(value.as_slice()));
            if let Ok(future) = producer.send(record, None).await {
                let _ = future.get_timeout(Duration::from_secs(30)).await;
            }
            i += 1;
        }
        println!("Warmup complete.");
    }

    // === MEASURED INTERVAL ===
    let test_start = Instant::now();
    let test_duration = Duration::from_secs(config.test_duration_seconds);

    // -- metrics_task: snapshot every 1s, write JSONL + diagnostic prints --
    let metrics_interval = Arc::clone(&interval_metrics);
    let metrics_cumul = Arc::clone(&cumulative);
    let metrics_stop = Arc::clone(&should_stop);
    let metrics_file = config.metrics_file.clone();
    let dbg_sent2 = Arc::clone(&dbg_sent);
    let dbg_completed2 = Arc::clone(&dbg_completed);
    let in_flight2 = Arc::clone(&in_flight);
    let metrics_task = tokio::spawn(async move {
        let mut file = std::fs::File::create(&metrics_file).expect("Failed to create metrics file");
        let start = Instant::now();
        let mut prev_sent: u64 = 0;
        let mut prev_completed: u64 = 0;

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if metrics_stop.load(Ordering::Relaxed) {
                break;
            }

            let snap = metrics_interval.snapshot_and_reset();
            let (cpu, rss) = get_process_stats();
            let elapsed = start.elapsed().as_secs_f64();

            metrics_cumul.accumulate(&snap, cpu, rss);

            let cur_sent = dbg_sent2.load(Ordering::Relaxed);
            let cur_completed = dbg_completed2.load(Ordering::Relaxed);
            let send_rate = cur_sent - prev_sent;
            let complete_rate = cur_completed - prev_completed;
            let cur_in_flight = in_flight2.load(Ordering::Relaxed);
            prev_sent = cur_sent;
            prev_completed = cur_completed;

            println!(
                "[{elapsed:6.1}s] sent={send_rate}/s  completed={complete_rate}/s  \
                 in_flight={cur_in_flight}  \
                 lat={:.2}ms  msgs={}",
                snap.avg_latency_ms(),
                snap.messages,
            );

            let line = serde_json::json!({
                "elapsed_s": format!("{:.1}", elapsed),
                "messages": snap.messages,
                "msg_per_s": format!("{:.1}", snap.messages as f64),
                "mib_per_s": format!("{:.3}", snap.bytes as f64 / (1024.0 * 1024.0)),
                "avg_latency_ms": format!("{:.2}", snap.avg_latency_ms()),
                "max_latency_ms": format!("{:.2}", snap.max_latency_ms()),
                "errors": snap.errors,
                "cpu_percent": format!("{:.1}", cpu),
                "rss_mib": format!("{:.1}", rss as f64 / (1024.0 * 1024.0)),
            });
            let _ = writeln!(file, "{line}");
            let _ = file.flush();
        }
    });

    // -- send loop: produce as fast as possible --
    let mut messages_sent: u64 = 0;
    let checkpoint_interval: u64 = if config.limit_rps > 0 {
        (config.limit_rps / 10).max(1)
    } else {
        0
    };
    let limit_rpns: f64 = if config.limit_rps > 0 {
        config.limit_rps as f64 / 1e9
    } else {
        0.0
    };
    let mut next_checkpoint: u64 = checkpoint_interval;

    let send_total_us = Arc::new(AtomicU64::new(0));
    // Disabled: spawn_total_us and record_build_total_us always reported 0.00 us/msg
    // (each Instant::now() is ~25ns on macOS, below their measurement signal). They
    // only added clock-read overhead — see PLAN-FixB.md for reasoning. Re-enable if
    // diagnosing record-construction or tx.send overhead specifically.
    // let spawn_total_us = Arc::new(AtomicU64::new(0));
    // let record_build_total_us = Arc::new(AtomicU64::new(0));

    // === TEMP: bucketed histogram of producer.send().await latency ===
    // Buckets (us): <5, <10, <20, <50, <100, <500, <2000, >=2000
    let send_buckets: Arc<[AtomicU64; 8]> = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));

    // Single completion task that consumes futures over a channel.
    // Avoids the per-message tokio::spawn cost (~18% of test-thread time observed).
    use confluent_kafka::common::KafkaFuture;
    use confluent_kafka::producer::RecordMetadata;
    type FutureType = KafkaFuture<RecordMetadata>;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(FutureType, Instant)>();
    let completion_metrics = Arc::clone(&interval_metrics);
    let completion_completed = Arc::clone(&dbg_completed);
    let completion_flight = Arc::clone(&in_flight);
    let completion_task = tokio::spawn(async move {
        use futures_util::stream::{FuturesUnordered, StreamExt};
        let mut pending: FuturesUnordered<_> = FuturesUnordered::new();
        loop {
            tokio::select! {
                biased;
                maybe_msg = rx.recv() => {
                    match maybe_msg {
                        Some((future, start_time)) => {
                            pending.push(async move {
                                (future.get_timeout(Duration::from_secs(60)).await, start_time)
                            });
                        }
                        None => break,
                    }
                }
                Some((result, start_time)) = pending.next(), if !pending.is_empty() => {
                    match result {
                        Ok(_metadata) => {
                            let latency = start_time.elapsed();
                            completion_metrics.record_success(latency.as_micros() as u64, message_size);
                        },
                        Err(_) => {
                            completion_metrics.record_error();
                        },
                    }
                    completion_completed.fetch_add(1, Ordering::Relaxed);
                    completion_flight.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
        // Drain any remaining after channel closed
        while let Some((result, start_time)) = pending.next().await {
            match result {
                Ok(_) => {
                    let latency = start_time.elapsed();
                    completion_metrics.record_success(latency.as_micros() as u64, message_size);
                },
                Err(_) => completion_metrics.record_error(),
            }
            completion_completed.fetch_add(1, Ordering::Relaxed);
            completion_flight.fetch_sub(1, Ordering::Relaxed);
        }
    });

    loop {
        if config.num_messages > 0 && messages_sent >= config.num_messages {
            break;
        }
        if test_start.elapsed() >= test_duration {
            break;
        }

        // Disabled: record_build timing always reports 0.00 us/msg.
        // let t0 = Instant::now();
        let (key, value) = &messages[messages_sent as usize % msg_count];
        let record = ProducerRecord::with_key(topic.clone(), Some(key.as_slice()), Some(value.as_slice()));
        // record_build_total_us.fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);

        // start_time is reused for both end-to-end latency tracking (in completion task)
        // and producer.send() histogram bucketing — saves one Instant::now() per msg.
        let start_time = Instant::now();
        match producer.send(record, None).await {
            Ok(future) => {
                let send_us = start_time.elapsed().as_micros() as u64;
                send_total_us.fetch_add(send_us, Ordering::Relaxed);

                // Bucket the send latency
                let bucket = match send_us {
                    0..=4 => 0,
                    5..=9 => 1,
                    10..=19 => 2,
                    20..=49 => 3,
                    50..=99 => 4,
                    100..=499 => 5,
                    500..=1999 => 6,
                    _ => 7,
                };
                send_buckets[bucket].fetch_add(1, Ordering::Relaxed);

                messages_sent += 1;
                dbg_sent.fetch_add(1, Ordering::Relaxed);
                in_flight.fetch_add(1, Ordering::Relaxed);

                // Disabled: spawn timing always reports 0.00 us/msg.
                // let t2 = Instant::now();
                let _ = tx.send((future, start_time));
                // spawn_total_us.fetch_add(t2.elapsed().as_micros() as u64, Ordering::Relaxed);
            },
            Err(e) => {
                eprintln!("Send error: {e:?}");
                interval_metrics.record_error();
            },
        }

        // Checkpoint-based rate limiting (only when LIMIT_RPS > 0)
        if checkpoint_interval > 0 && messages_sent >= next_checkpoint {
            let elapsed_ns = test_start.elapsed().as_nanos() as f64;
            let expected = elapsed_ns * limit_rpns;
            if messages_sent as f64 > expected {
                let sleep_s = (messages_sent as f64 - expected) / config.limit_rps as f64;
                tokio::time::sleep(Duration::from_secs_f64(sleep_s)).await;
            }
            next_checkpoint += checkpoint_interval;
        }
    }

    // === COOLDOWN ===
    println!("Stopping after {messages_sent} messages sent, waiting for in-flight ...");

    // Drop the channel sender so the completion task can drain and exit.
    drop(tx);

    while in_flight.load(Ordering::Relaxed) > 0 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = completion_task.await;

    let final_snap = interval_metrics.snapshot_and_reset();
    let (final_cpu, final_rss) = get_process_stats();
    cumulative.accumulate(&final_snap, final_cpu, final_rss);

    should_stop.store(true, Ordering::Relaxed);
    let _ = metrics_task.await;

    let _ = producer.close().await;

    // === SUMMARY ===
    let total_duration = test_start.elapsed();
    let total_secs = total_duration.as_secs_f64();
    let total_msgs = cumulative.total_messages.load(Ordering::Relaxed);
    let total_bytes = cumulative.total_bytes.load(Ordering::Relaxed);
    let total_lat_us = cumulative.total_latency_us.load(Ordering::Relaxed);
    let max_lat_us = cumulative.max_latency_us.load(Ordering::Relaxed);
    let total_errors = cumulative.total_errors.load(Ordering::Relaxed);
    let samples = cumulative.sample_count.load(Ordering::Relaxed);
    let avg_cpu = if samples > 0 {
        cumulative.total_cpu.load(Ordering::Relaxed) as f64 / samples as f64 / 100.0
    } else {
        0.0
    };
    let avg_rss = if samples > 0 {
        cumulative.total_rss.load(Ordering::Relaxed) as f64 / samples as f64
    } else {
        0.0
    };
    let avg_rss_mib = avg_rss / (1024.0 * 1024.0);

    let msg_rate = if total_secs > 0.0 {
        total_msgs as f64 / total_secs
    } else {
        0.0
    };
    let mib_rate = if total_secs > 0.0 {
        total_bytes as f64 / (1024.0 * 1024.0) / total_secs
    } else {
        0.0
    };
    let avg_latency_ms = if total_msgs > 0 {
        total_lat_us as f64 / total_msgs as f64 / 1000.0
    } else {
        0.0
    };
    let max_latency_ms = max_lat_us as f64 / 1000.0;

    println!();
    println!("=== Results ===");
    println!("Duration:           {total_secs:.2} s");
    println!("Messages sent:      {messages_sent}");
    println!("Messages completed: {total_msgs}");
    println!("Errors:             {total_errors}");
    println!("Throughput:         {msg_rate:.2} msg/s");
    println!("Throughput:         {mib_rate:.2} MiB/s");
    println!("Avg latency:        {avg_latency_ms:.2} ms");
    println!("Max latency:        {max_latency_ms:.2} ms");
    println!("Avg CPU:            {avg_cpu:.2} %");
    println!("Avg RSS:            {avg_rss_mib:.2} MiB");
    if avg_cpu > 0.0 {
        println!("CPU efficiency:     {:.2} msg/(s * 1% CPU)", msg_rate / avg_cpu);
    }
    if avg_rss_mib > 0.0 {
        println!("Memory efficiency:  {:.2} msg/(s * MiB RSS)", msg_rate / avg_rss_mib);
    }
    println!("Metrics written to: {}", config.metrics_file);

    println!();
    println!("=== Send-path Timing Breakdown ===");
    if messages_sent > 0 {
        let avg_send_us = send_total_us.load(Ordering::Relaxed) as f64 / messages_sent as f64;
        // Record build and tokio::spawn timing disabled — always reported 0.00 us/msg.
        // let avg_record_us = record_build_total_us.load(Ordering::Relaxed) as f64 / messages_sent as f64;
        // let avg_spawn_us = spawn_total_us.load(Ordering::Relaxed) as f64 / messages_sent as f64;
        println!("producer.send():    {avg_send_us:.2} us/msg");
        if avg_send_us > 0.0 {
            println!("Max possible rate:  {:.0} msg/s", 1_000_000.0 / avg_send_us);
        }
    }

    // === producer.send() latency distribution ===
    println!();
    println!("=== producer.send() latency distribution ===");
    let labels = [
        "<5us",
        "5-9us",
        "10-19us",
        "20-49us",
        "50-99us",
        "100-499us",
        "500-1999us",
        ">=2000us",
    ];
    let total: u64 = send_buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum();
    for (i, lbl) in labels.iter().enumerate() {
        let n = send_buckets[i].load(Ordering::Relaxed);
        let pct = if total > 0 {
            100.0 * n as f64 / total as f64
        } else {
            0.0
        };
        println!("  {lbl:<14} {n:>10} ({pct:5.2}%)");
    }

    assert!(total_msgs > 0, "Should have completed at least one message");
}
