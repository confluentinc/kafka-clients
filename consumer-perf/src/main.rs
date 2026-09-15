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

//! End-to-end latency / CPU / memory benchmark for the Rust Kafka consumer.
//!
//! Methodology (see `design/current/consumer-perf-benchmark-analysis.md`):
//!
//!   1. Start the consumer with `auto.offset.reset=latest` so it only ever
//!      sees *new* records — the e2e latency `now - record.timestamp()` then
//!      reflects produce→consume delay, not the age of a disk backlog.
//!   2. Wait until the consumer has joined the group and been assigned
//!      partitions (KIP-848 cold-join can take tens of seconds).
//!   3. *Then* launch `kafka-producer-perf-test.sh` at a **fixed** throughput
//!      (not `--throughput -1`) so latency reflects steady state.
//!   4. Poll in a tight loop, recording each record's e2e latency into a
//!      fixed-bucket streaming histogram (O(1) memory, no per-record alloc).
//!   5. Every `--interval` seconds, sample the consumer process's CPU% and RSS
//!      and emit a JSONL metric line; reset the per-interval histogram.
//!   6. Stop after `--duration` seconds of measurement and print + persist a
//!      final summary (overall percentiles).
//!
//! The value/key bytes are never copied: the deserializer returns the byte
//! *length* (`usize`), so the poll loop only touches a counter and the record
//! timestamp.
//!
//! Run, e.g.:
//!
//! ```sh
//! cargo run -p consumer-perf --release -- \
//!     --topic consumer-perf-bench --throughput 50000 --duration 120
//! ```

use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::{Consumer, ConsumerConfig, new_consumer};
use sysinfo::{MINIMUM_CPU_UPDATE_INTERVAL, Pid, ProcessesToUpdate, System};

/// Zero-copy deserializer: returns the byte length instead of the bytes, so the
/// poll loop never allocates per record. Used for both key and value.
struct LenDeserializer;

impl Deserializer<usize> for LenDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<usize, Error> {
        Ok(data.len())
    }
}

/// Wall-clock milliseconds since the Unix epoch. Both the producer
/// (`CreateTime`) and this consumer run on the same host, so the difference is
/// a meaningful e2e latency (no clock-skew correction needed).
fn now_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

/// Fixed-memory histogram for streaming percentile estimation. 1 ms buckets
/// from 0 to `MAX_LATENCY_MS`; anything larger lands in the last bucket. Mirrors
/// the Java `E2ELatencyBenchmark.LatencyHistogram`.
struct LatencyHistogram {
    buckets: Vec<u64>,
    count: u64,
    sum: u128,
    min: i64,
    max: i64,
}

impl LatencyHistogram {
    // 10 minutes: under --peak saturation, e2e latency is backlog age and can far
    // exceed 60s; a 10-min ceiling avoids clamping the tail within a 10-min run.
    // 600k * 8 bytes ≈ 4.8 MB per histogram — negligible.
    const MAX_LATENCY_MS: usize = 600_000;

    fn new() -> Self {
        Self {
            buckets: vec![0; Self::MAX_LATENCY_MS + 1],
            count: 0,
            sum: 0,
            min: i64::MAX,
            max: i64::MIN,
        }
    }

    fn record(&mut self, latency_ms: i64) {
        let idx = latency_ms.clamp(0, Self::MAX_LATENCY_MS as i64) as usize;
        self.buckets[idx] += 1;
        self.count += 1;
        self.sum += latency_ms.max(0) as u128;
        self.min = self.min.min(latency_ms);
        self.max = self.max.max(latency_ms);
    }

    fn min(&self) -> i64 {
        if self.count == 0 { 0 } else { self.min }
    }
    fn max(&self) -> i64 {
        if self.count == 0 { 0 } else { self.max }
    }
    fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum as f64 / self.count as f64
        }
    }

    /// Latency stddev computed from the exact histogram (1 ms quantized).
    fn stddev(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let mean = self.sum as f64 / self.count as f64;
        let mut var = 0.0;
        for (i, &c) in self.buckets.iter().enumerate() {
            if c > 0 {
                let d = i as f64 - mean;
                var += c as f64 * d * d;
            }
        }
        (var / self.count as f64).sqrt()
    }

    fn percentile(&self, pct: f64) -> i64 {
        if self.count == 0 {
            return 0;
        }
        let target = (pct / 100.0 * self.count as f64).ceil() as u64;
        let mut cumulative = 0u64;
        for (i, &c) in self.buckets.iter().enumerate() {
            cumulative += c;
            if cumulative >= target {
                return i as i64;
            }
        }
        Self::MAX_LATENCY_MS as i64
    }
}

/// Records-per-poll distribution over non-empty polls. Each `record(n)` logs the
/// number of records one `poll()` returned. Surfaces mean / min / p50 / p99 / max.
/// Bucketed 0..=MAX_RPP (max.poll.records is bounded, so this is exact for the
/// configured caps in these experiments).
struct RecordsPerPoll {
    buckets: Vec<u64>,
    count: u64,
    sum: u128,
    min: u64,
    max: u64,
}

impl RecordsPerPoll {
    const MAX_RPP: usize = 100_000;

    fn new() -> Self {
        Self { buckets: vec![0; Self::MAX_RPP + 1], count: 0, sum: 0, min: u64::MAX, max: 0 }
    }

    fn record(&mut self, n: u64) {
        let idx = (n as usize).min(Self::MAX_RPP);
        self.buckets[idx] += 1;
        self.count += 1;
        self.sum += n as u128;
        self.min = self.min.min(n);
        self.max = self.max.max(n);
    }

    fn min(&self) -> u64 {
        if self.count == 0 { 0 } else { self.min }
    }
    fn max(&self) -> u64 {
        self.max
    }
    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum as f64 / self.count as f64
        }
    }
    fn percentile(&self, pct: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = (pct / 100.0 * self.count as f64).ceil() as u64;
        let mut cumulative = 0u64;
        for (i, &c) in self.buckets.iter().enumerate() {
            cumulative += c;
            if cumulative >= target {
                return i as u64;
            }
        }
        Self::MAX_RPP as u64
    }
}

/// Samples this process's CPU usage (percent of a single core; may exceed 100%
/// on multi-core work) and resident set size, between successive calls.
struct ResourceSampler {
    sys: System,
    pid: Pid,
}

impl ResourceSampler {
    fn new() -> Self {
        let pid = Pid::from_u32(std::process::id());
        let mut sys = System::new();
        // Refresh with `ProcessesToUpdate::All`, NOT `Some(&[pid])`: sysinfo's
        // Linux backend only computes per-process `cpu_usage()` inside the
        // `ProcessesToUpdate::All` branch of `refresh_processes_specifics`
        // (sysinfo 0.32 `unix/linux/system.rs`), so a `Some(...)` refresh
        // reports 0.0% CPU forever on Linux (verified on EC2; macOS computes
        // per-process CPU on a different path and worked either way).
        //
        // sysinfo also only produces a valid `cpu_usage()` after the process
        // has been refreshed twice (the first registers it, the second
        // establishes the CPU baseline). Warm it with two spaced refreshes so
        // the caller's first `sample()` already yields a real reading.
        sys.refresh_processes(ProcessesToUpdate::All, true);
        std::thread::sleep(MINIMUM_CPU_UPDATE_INTERVAL);
        sys.refresh_processes(ProcessesToUpdate::All, true);
        Self { sys, pid }
    }

    /// Returns `(cpu_percent_of_one_core, rss_mb)`.
    fn sample(&mut self) -> (f32, f64) {
        self.sys.refresh_processes(ProcessesToUpdate::All, true);
        match self.sys.process(self.pid) {
            Some(p) => (p.cpu_usage(), p.memory() as f64 / (1024.0 * 1024.0)),
            None => (0.0, 0.0),
        }
    }
}

/// Diagnostic: verify the CPU/RSS sampler against a known CPU-burning workload.
fn selftest_cpu() {
    println!("ResourceSampler self-test (burning CPU on this thread)...");
    let mut sampler = ResourceSampler::new();
    let mut x: u64 = 0;
    for i in 0..5 {
        let spin_until = Instant::now() + Duration::from_millis(800);
        while Instant::now() < spin_until {
            for _ in 0..200_000 {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
            }
        }
        let (cpu, rss) = sampler.sample();
        println!("  sample {i}: cpu={cpu:.1}% rss={rss:.1}MB  (sink={})", x & 0xff);
    }
}

struct Args {
    bootstrap: String,
    topic: String,
    group_id: String,
    throughput: u64,
    duration_s: u64,
    message_size: u64,
    partitions: u32,
    warmup_messages: u64,
    interval_s: u64,
    poll_timeout_ms: u64,
    join_timeout_s: u64,
    offset_reset: String,
    kafka_bin: String,
    results_dir: String,
    produce: bool,
    create_topic: bool,
    verbose: bool,
    /// Run the producer at unbounded peak rate (`--throughput -1`).
    peak: bool,
    /// Topic `retention.ms` used at create time (bounds disk under peak load).
    retention_ms: i64,
    /// Override the producer's `--num-records` (else derived from throughput×duration).
    num_records: Option<u64>,
    /// Fetch-config overrides injected into the consumer via
    /// `ConsumerConfig::from_properties`. `None` => use the client default.
    fetch_min_bytes: Option<i64>,
    fetch_max_wait_ms: Option<i64>,
    max_partition_fetch_bytes: Option<i64>,
    fetch_max_bytes: Option<i64>,
    max_poll_records: Option<i64>,
    /// Kafka client properties file (e.g. SASL_SSL creds for Confluent Cloud).
    /// Merged into the consumer props AND passed to the producer via `--producer.config`.
    client_config: Option<String>,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        let ts = now_millis();
        let mut a = Args {
            bootstrap: "localhost:9092".to_string(),
            topic: "consumer-perf-bench".to_string(),
            group_id: format!("consumer-perf-{ts}"),
            throughput: 50_000,
            duration_s: 60,
            message_size: 1024,
            partitions: 8,
            warmup_messages: 5_000,
            interval_s: 5,
            poll_timeout_ms: 500,
            join_timeout_s: 120,
            offset_reset: "latest".to_string(),
            kafka_bin: format!("{home}/dev/opensource/kafka/bin"),
            results_dir: "consumer-perf/results".to_string(),
            produce: true,
            create_topic: true,
            verbose: false,
            peak: false,
            retention_ms: 3_600_000,
            num_records: None,
            fetch_min_bytes: None,
            fetch_max_wait_ms: None,
            max_partition_fetch_bytes: None,
            fetch_max_bytes: None,
            max_poll_records: None,
            client_config: None,
        };
        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            let mut next = || it.next().ok_or_else(|| format!("missing value for {arg}"));
            match arg.as_str() {
                "--bootstrap" | "-b" => a.bootstrap = next()?,
                "--topic" | "-t" => a.topic = next()?,
                "--group-id" | "-g" => a.group_id = next()?,
                "--throughput" | "-r" => a.throughput = next()?.parse().map_err(|e| format!("{e}"))?,
                "--duration" | "-d" => a.duration_s = next()?.parse().map_err(|e| format!("{e}"))?,
                "--message-size" => a.message_size = next()?.parse().map_err(|e| format!("{e}"))?,
                "--partitions" => a.partitions = next()?.parse().map_err(|e| format!("{e}"))?,
                "--warmup-messages" | "-w" => a.warmup_messages = next()?.parse().map_err(|e| format!("{e}"))?,
                "--interval" => a.interval_s = next()?.parse().map_err(|e| format!("{e}"))?,
                "--poll-timeout-ms" => a.poll_timeout_ms = next()?.parse().map_err(|e| format!("{e}"))?,
                "--join-timeout" => a.join_timeout_s = next()?.parse().map_err(|e| format!("{e}"))?,
                "--offset-reset" => a.offset_reset = next()?,
                "--kafka-bin" => a.kafka_bin = next()?,
                "--results-dir" => a.results_dir = next()?,
                "--no-produce" => a.produce = false,
                "--no-create-topic" => a.create_topic = false,
                "--verbose" | "-v" => a.verbose = true,
                "--peak" => a.peak = true,
                "--retention-ms" => a.retention_ms = next()?.parse().map_err(|e| format!("{e}"))?,
                "--num-records" => a.num_records = Some(next()?.parse().map_err(|e| format!("{e}"))?),
                "--fetch-min-bytes" => a.fetch_min_bytes = Some(next()?.parse().map_err(|e| format!("{e}"))?),
                "--fetch-max-wait-ms" => a.fetch_max_wait_ms = Some(next()?.parse().map_err(|e| format!("{e}"))?),
                "--max-partition-fetch-bytes" => {
                    a.max_partition_fetch_bytes = Some(next()?.parse().map_err(|e| format!("{e}"))?)
                },
                "--fetch-max-bytes" => a.fetch_max_bytes = Some(next()?.parse().map_err(|e| format!("{e}"))?),
                "--max-poll-records" => a.max_poll_records = Some(next()?.parse().map_err(|e| format!("{e}"))?),
                "--client-config" => a.client_config = Some(next()?),
                "--help" | "-h" => return Err("help".to_string()),
                other => return Err(format!("unknown argument: {other}")),
            }
        }
        Ok(a)
    }
}

fn print_usage() {
    eprintln!(
        "Consumer e2e-latency benchmark (Rust Kafka consumer)\n\n\
         USAGE: cargo run -p consumer-perf --release -- [OPTIONS]\n\n\
         OPTIONS:\n  \
           -b, --bootstrap <HOST:PORT>   Bootstrap servers (default: localhost:9092)\n  \
           -t, --topic <NAME>            Topic (default: consumer-perf-bench)\n  \
           -g, --group-id <ID>           Consumer group id (default: consumer-perf-<ts>)\n  \
           -r, --throughput <MSG/S>      Producer target throughput (default: 50000)\n  \
           -d, --duration <SECONDS>      Measurement window after warmup (default: 60)\n  \
               --message-size <BYTES>    Record size for the producer (default: 1024)\n  \
               --partitions <N>          Partitions when creating the topic (default: 8)\n  \
           -w, --warmup-messages <N>     Records to skip before measuring (default: 5000)\n  \
               --interval <SECONDS>      Metric reporting interval (default: 5)\n  \
               --poll-timeout-ms <MS>    poll() timeout (default: 500)\n  \
               --join-timeout <SECONDS>  Max wait for partition assignment (default: 120)\n  \
               --offset-reset <latest|earliest>  auto.offset.reset (default: latest)\n  \
               --kafka-bin <DIR>         Kafka bin dir (default: ~/dev/opensource/kafka/bin)\n  \
               --results-dir <DIR>       Output dir (default: consumer-perf/results)\n  \
               --no-produce              Don't launch a producer (drive it externally)\n  \
               --no-create-topic         Don't create/verify the topic\n  \
               --peak                    Run the producer unbounded (--throughput -1); saturates the consumer\n  \
               --retention-ms <MS>       Topic retention.ms at create time (default: 3600000; lower bounds disk under peak)\n  \
               --num-records <N>         Override producer --num-records (default: throughput×(duration+30))\n  \
               --fetch-min-bytes <N>     fetch.min.bytes (client default 1)\n  \
               --fetch-max-wait-ms <N>   fetch.max.wait.ms (client default 500)\n  \
               --max-partition-fetch-bytes <N>  max.partition.fetch.bytes (client default 1MB)\n  \
               --fetch-max-bytes <N>     fetch.max.bytes (client default 50MB)\n  \
               --max-poll-records <N>    max.poll.records (client default 500)\n  \
           -v, --verbose                 Log every poll (record counts / heartbeats)\n"
    );
}

/// Creates (or verifies) the topic via `kafka-topics.sh`. Best-effort: a
/// failure here is non-fatal (the topic may already exist, or auto-create may
/// be on); we log and continue.
fn ensure_topic(args: &Args) {
    let bin = format!("{}/kafka-topics.sh", args.kafka_bin);
    println!(">>> Ensuring topic '{}' ({} partitions)...", args.topic, args.partitions);
    // Roll segments at 512 MB so retention can actually purge old data during a
    // long high-throughput run (bounds disk under --peak).
    let retention_cfg = format!("retention.ms={}", args.retention_ms);
    // Replication factor is left to the broker default (a hardcoded RF=1 is
    // rejected by Confluent Cloud), matching the librdkafka/Java arms and the
    // Python harness's recreate_topic.
    let mut topic_args: Vec<String> = vec![
        "--bootstrap-server".into(),
        args.bootstrap.clone(),
        "--create".into(),
        "--topic".into(),
        args.topic.clone(),
        "--partitions".into(),
        args.partitions.to_string(),
        "--config".into(),
        retention_cfg,
        "--config".into(),
        "segment.bytes=536870912".into(),
        "--if-not-exists".into(),
    ];
    // Forward the client config so topic creation authenticates like the
    // consumer and the spawned load generator already do.
    if let Some(cfg) = &args.client_config {
        topic_args.push("--command-config".into());
        topic_args.push(cfg.clone());
    }
    let status = Command::new(&bin)
        .args(&topic_args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match status {
        Ok(s) if s.success() => println!("    topic ready"),
        Ok(_) => println!("    (topic create returned non-zero; assuming it already exists)"),
        Err(e) => println!("    WARN: could not run {bin}: {e} (continuing)"),
    }
}

/// Spawns `kafka-producer-perf-test.sh` at a fixed throughput. Produces enough
/// records to cover warmup + the full measurement window plus slack, so the
/// stream never dries up mid-measurement.
fn spawn_producer(args: &Args) -> std::io::Result<Child> {
    let bin = format!("{}/kafka-producer-perf-test.sh", args.kafka_bin);
    // Records must outlast warmup + the measurement window. For --peak the rate
    // is unbounded, so size num-records to a large count that won't be exhausted
    // within the window (override with --num-records). For a fixed rate, derive
    // from throughput×duration + slack.
    let total_records = args
        .num_records
        .unwrap_or_else(|| args.throughput * (args.duration_s + 30) + args.warmup_messages);
    // `-1` = unbounded peak (kafka-producer-perf-test convention).
    let throughput_arg = if args.peak {
        "-1".to_string()
    } else {
        args.throughput.to_string()
    };
    println!(
        ">>> Launching producer: throughput={} ({}), {} bytes, ~{} records",
        throughput_arg,
        if args.peak { "PEAK/unbounded" } else { "fixed msg/s" },
        args.message_size,
        total_records
    );
    let mut pargs: Vec<String> = vec![
        "--topic".into(),
        args.topic.clone(),
        "--num-records".into(),
        total_records.to_string(),
        "--record-size".into(),
        args.message_size.to_string(),
        "--throughput".into(),
        throughput_arg.clone(),
    ];
    if let Some(ref path) = args.client_config {
        // SASL_SSL (or other) client properties for the producer; bootstrap +
        // security come from the file. acks via --producer-props.
        pargs.push("--producer.config".into());
        pargs.push(path.clone());
        pargs.push("--producer-props".into());
        pargs.push("acks=1".into());
    } else {
        pargs.push("--producer-props".into());
        pargs.push(format!("bootstrap.servers={}", args.bootstrap));
        pargs.push("acks=1".into());
    }
    Command::new(&bin)
        .args(&pargs)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

#[tokio::main]
async fn main() {
    // Install a backend for the `log` facade so the client's join / heartbeat /
    // coordinator / fetch logs surface. Controlled by RUST_LOG, e.g.:
    //   RUST_LOG=confluent_kafka=debug          (all client logs)
    //   RUST_LOG=confluent_kafka::consumer::internals::coordinator_request_manager=debug,\
    //            confluent_kafka::consumer::internals::consumer_membership_manager=trace
    // Default (RUST_LOG unset) shows warn+ so a stuck join still surfaces warnings.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .format_timestamp_millis()
        .init();

    if std::env::args().any(|a| a == "--selftest-cpu") {
        selftest_cpu();
        return;
    }
    let args = match Args::parse() {
        Ok(a) => a,
        Err(msg) => {
            if msg != "help" {
                eprintln!("error: {msg}\n");
            }
            print_usage();
            std::process::exit(if msg == "help" { 0 } else { 2 });
        },
    };

    if let Err(e) = run(args).await {
        eprintln!("benchmark failed: {e}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", "=".repeat(70));
    println!("Consumer E2E Latency Benchmark — Rust Kafka Consumer");
    println!("{}", "=".repeat(70));
    println!("Bootstrap:   {}", args.bootstrap);
    println!("Topic:       {}", args.topic);
    println!("Group:       {}", args.group_id);
    println!("Throughput:  {} msg/s", args.throughput);
    println!("Duration:    {} s (after warmup)", args.duration_s);
    println!("Msg size:    {} bytes", args.message_size);
    println!("Warmup:      {} messages", args.warmup_messages);
    println!("Interval:    {} s", args.interval_s);
    println!("{}", "=".repeat(70));

    if args.create_topic {
        ensure_topic(&args);
    }

    // KIP-848 ("consumer") protocol only. `latest` (default) so we measure live
    // records; `earliest` available via --offset-reset for comparison.
    // Auto-commit on: this is a throughput/latency probe, commit cost is part
    // of a realistic consumer; offsets don't matter (fresh group each run).
    println!("Offset reset: {}", args.offset_reset);
    // Build the base config from a properties map seeded with bootstrap + any
    // fetch-knob overrides (so the fetcher actually picks them up), THEN chain
    // the existing builders. When no fetch flags are passed the map carries only
    // bootstrap and the client defaults apply — identical to prior behavior.
    let mut props: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    props.insert("bootstrap.servers".to_string(), args.bootstrap.clone());
    if let Some(v) = args.fetch_min_bytes {
        props.insert("fetch.min.bytes".to_string(), v.to_string());
    }
    if let Some(v) = args.fetch_max_wait_ms {
        props.insert("fetch.max.wait.ms".to_string(), v.to_string());
    }
    if let Some(v) = args.max_partition_fetch_bytes {
        props.insert("max.partition.fetch.bytes".to_string(), v.to_string());
    }
    if let Some(v) = args.fetch_max_bytes {
        props.insert("fetch.max.bytes".to_string(), v.to_string());
    }
    if let Some(v) = args.max_poll_records {
        props.insert("max.poll.records".to_string(), v.to_string());
    }
    if let Some(ref path) = args.client_config {
        let content =
            std::fs::read_to_string(path).map_err(|e| format!("failed to read --client-config {path}: {e}"))?;
        for line in content.lines() {
            let l = line.trim();
            if l.is_empty() || l.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = l.split_once('=') {
                props.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
        println!("Client config: loaded {path} (SASL/SSL etc.)");
    }
    println!(
        "Fetch config: fetch.min.bytes={} fetch.max.wait.ms={} max.partition.fetch.bytes={} \
         fetch.max.bytes={} max.poll.records={}",
        args.fetch_min_bytes.map_or("default".to_string(), |v| v.to_string()),
        args.fetch_max_wait_ms.map_or("default".to_string(), |v| v.to_string()),
        args.max_partition_fetch_bytes.map_or("default".to_string(), |v| v.to_string()),
        args.fetch_max_bytes.map_or("default".to_string(), |v| v.to_string()),
        args.max_poll_records.map_or("default".to_string(), |v| v.to_string()),
    );
    let config = ConsumerConfig::from_properties(&props)?
        .with_client_id("consumer-perf")
        .with_group_id(args.group_id.clone())
        .with_group_protocol("consumer")
        .with_auto_offset_reset(args.offset_reset.clone())
        .with_enable_auto_commit(true);

    let mut consumer: Box<dyn Consumer<usize, usize>> =
        new_consumer::<usize, usize>(config, Box::new(LenDeserializer), Box::new(LenDeserializer))?;

    consumer.subscribe(vec![args.topic.clone()]).await?;
    println!("\n>>> Subscribed; waiting for partition assignment (KIP-848 join)...");

    // Wait for assignment before starting the producer, so the very first
    // produced records are not unfairly counted against join latency.
    let poll_timeout = Duration::from_millis(args.poll_timeout_ms);
    let join_start = Instant::now();
    let join_timeout = Duration::from_secs(args.join_timeout_s);
    let mut last_join_log = Instant::now();
    loop {
        // Poll drives the background join handshake and the app-side event loop.
        let recs = consumer.poll(poll_timeout).await?;
        let assigned = consumer.assignment().len();
        if assigned > 0 {
            println!(
                "    assigned {assigned} partition(s) after {:.1}s",
                join_start.elapsed().as_secs_f64()
            );
            break;
        }
        // Heartbeat so a slow join is visible rather than looking hung.
        if last_join_log.elapsed() >= Duration::from_secs(5) {
            println!(
                "    [join] {:.0}s elapsed, assignment=0, last poll returned {} records",
                join_start.elapsed().as_secs_f64(),
                recs.count()
            );
            last_join_log = Instant::now();
        }
        if join_start.elapsed() >= join_timeout {
            return Err(format!(
                "timed out ({}s) waiting for partition assignment — the KIP-848 join did not \
                 complete (see the known consumer initial-join-latency bug); raise --join-timeout \
                 or investigate the client join path",
                args.join_timeout_s
            )
            .into());
        }
    }

    // Settle to the live edge BEFORE starting the producer: poll (discarding any
    // records) until an empty poll, so the consumer is positioned at `latest` and
    // fully caught up. This guarantees every measured record was produced *after*
    // the consumer was already polling — no pre-existing/past data is counted.
    if args.produce {
        println!(">>> Settling to the live edge (polling until empty) before starting producer...");
        let settle_deadline = Instant::now() + Duration::from_secs(15);
        let mut empties = 0u32;
        loop {
            let recs = consumer.poll(poll_timeout).await?;
            if recs.is_empty() {
                empties += 1;
                if empties >= 2 {
                    break; // two consecutive empty polls ⇒ positioned at latest, caught up
                }
            } else {
                empties = 0; // drained pre-existing tail; keep going until empty
            }
            if Instant::now() >= settle_deadline {
                println!("    (settle timeout — proceeding; positions should be at latest)");
                break;
            }
        }
        println!("    at live edge; starting producer now.");
    }

    // Start the live producer now that the consumer is polling at the live edge.
    let mut producer: Option<Child> = if args.produce {
        match spawn_producer(&args) {
            Ok(child) => Some(child),
            Err(e) => return Err(format!("failed to launch producer ({e}); is --kafka-bin correct?").into()),
        }
    } else {
        println!(">>> --no-produce: drive the producer externally now.");
        None
    };

    let mut overall = LatencyHistogram::new();
    let mut interval_hist = LatencyHistogram::new();
    let mut sampler = ResourceSampler::new();
    // Run-level current-RSS aggregation (avg/min/max over interval samples).
    let mut rss_sum = 0.0f64;
    let mut rss_min = f64::MAX;
    let mut rss_max = 0.0f64;
    let mut rss_samples: u64 = 0;
    let mut cpu_sum = 0.0f64;
    let mut cpu_min = f64::MAX;
    let mut cpu_max = 0.0f64;

    let mut messages_consumed: u64 = 0;
    let mut warmup_complete = false;
    let mut measure_start = Instant::now();
    let mut interval_start = Instant::now();
    let mut interval_count: u64 = 0;

    // Prepare output sink.
    let run_dir = PathBuf::from(&args.results_dir).join(args.group_id.clone());
    fs::create_dir_all(&run_dir)?;
    let mut jsonl = File::create(run_dir.join("metrics.jsonl"))?;
    write_config(&run_dir, &args)?;

    println!(
        "\n>>> Measuring (warmup {} msgs, then {} s)...\n",
        args.warmup_messages, args.duration_s
    );

    // Safety bound so a dead producer can't hang the run forever.
    let loop_start = Instant::now();
    let no_data_deadline = loop_start + Duration::from_secs(120);
    let mut first_record_seen = false;
    let mut polls: u64 = 0;
    // Localize the bottleneck: wall time spent inside `poll().await` vs. inside
    // the harness's own per-record processing. If poll dominates, the ceiling is
    // in the client, not this loop.
    let mut poll_nanos: u128 = 0;
    let mut proc_nanos: u128 = 0;
    // Records-per-poll distribution over NON-EMPTY polls during the measurement
    // window only (mirrors the C harness's batch_calls/batch_records, which count
    // only batches that returned records, post-warmup). Each entry is the record
    // count of one poll() that returned >=1 record.
    let mut rpp = RecordsPerPoll::new();

    'outer: loop {
        let p0 = Instant::now();
        let records = consumer.poll(poll_timeout).await?;
        poll_nanos += p0.elapsed().as_nanos();
        polls += 1;

        if records.is_empty() {
            if !warmup_complete && Instant::now() >= no_data_deadline && messages_consumed == 0 {
                return Err("no records received within 120s (producer not running?)".into());
            }
            if warmup_complete && measure_start.elapsed() >= Duration::from_secs(args.duration_s) {
                break 'outer;
            }
            continue;
        }
        if !first_record_seen {
            first_record_seen = true;
            println!(
                ">>> first records arrived {:.1}s after producer start (after {polls} polls)",
                loop_start.elapsed().as_secs_f64()
            );
        }

        // Tight per-record loop: count, skip warmup, record e2e latency. No key/
        // value decode or assignment lookups — the LenDeserializer already ran in
        // the client, and bytes are derived from the fixed record size at summary.
        // Track the records-per-poll distribution over the measurement window
        // (post-warmup, non-empty polls only).
        if warmup_complete {
            rpp.record(records.count() as u64);
        }

        let q0 = Instant::now();
        let poll_now = now_millis();
        for record in &records {
            messages_consumed += 1;
            if messages_consumed <= args.warmup_messages {
                if messages_consumed == args.warmup_messages {
                    warmup_complete = true;
                    measure_start = Instant::now();
                    interval_start = measure_start;
                    println!("    warmup complete ({} msgs); measuring now", args.warmup_messages);
                }
                continue;
            }
            let ts = record.timestamp();
            if ts > 0 {
                let latency = poll_now - ts;
                overall.record(latency);
                interval_hist.record(latency);
            }
        }
        proc_nanos += q0.elapsed().as_nanos();

        // Interval reporting (only after warmup).
        if warmup_complete && interval_start.elapsed() >= Duration::from_secs(args.interval_s) {
            let elapsed_s = interval_start.elapsed().as_secs_f64();
            let (cpu, rss_mb) = sampler.sample();
            rss_sum += rss_mb;
            if rss_mb < rss_min {
                rss_min = rss_mb;
            }
            if rss_mb > rss_max {
                rss_max = rss_mb;
            }
            rss_samples += 1;
            let cpu_f64 = cpu as f64;
            cpu_sum += cpu_f64;
            if cpu_f64 < cpu_min {
                cpu_min = cpu_f64;
            }
            if cpu_f64 > cpu_max {
                cpu_max = cpu_f64;
            }
            let icount = interval_hist.count;
            let throughput = icount as f64 / elapsed_s;
            emit_interval(
                &mut jsonl,
                interval_count,
                measure_start.elapsed().as_secs_f64(),
                elapsed_s,
                icount,
                throughput,
                &interval_hist,
                cpu,
                rss_mb,
            )?;
            interval_hist = LatencyHistogram::new();
            interval_start = Instant::now();
            interval_count += 1;
        }

        if warmup_complete && measure_start.elapsed() >= Duration::from_secs(args.duration_s) {
            break 'outer;
        }
    }

    let measured_duration_s = measure_start.elapsed().as_secs_f64();

    // Stop the producer (it may still be producing its slack records).
    if let Some(child) = producer.as_mut() {
        let _ = child.kill();
        let _ = child.wait();
    }

    consumer.close().await?;

    // Bytes derived from the fixed producer record size (kafka-producer-perf-test
    // emits exactly --record-size payloads), so the per-record loop never reads
    // key/value at all.
    let total_bytes = overall.count as u128 * args.message_size as u128;

    // Bottleneck localization: share of wall time in poll() vs. our processing.
    let busy = (poll_nanos + proc_nanos).max(1) as f64;
    println!(
        "\nLoop breakdown over {polls} polls: poll().await={:.1}%  per-record processing={:.1}%  \
         (avg {:.0} records/poll)",
        100.0 * poll_nanos as f64 / busy,
        100.0 * proc_nanos as f64 / busy,
        overall.count as f64 / polls.max(1) as f64,
    );
    println!(
        "Records/poll distribution (non-empty polls, post-warmup): \
         n={} min={} mean={:.1} p50={} p99={} max={}",
        rpp.count,
        rpp.min(),
        rpp.mean(),
        rpp.percentile(50.0),
        rpp.percentile(99.0),
        rpp.max(),
    );

    let rss_avg = if rss_samples > 0 {
        rss_sum / rss_samples as f64
    } else {
        0.0
    };
    let rss_min_out = if rss_samples > 0 { rss_min } else { 0.0 };
    let cpu_avg = if rss_samples > 0 {
        cpu_sum / rss_samples as f64
    } else {
        0.0
    };
    let cpu_min_out = if rss_samples > 0 { cpu_min } else { 0.0 };
    write_summary(
        &mut jsonl,
        &run_dir,
        &overall,
        total_bytes,
        measured_duration_s,
        args.warmup_messages,
        &rpp,
        cpu_avg,
        cpu_min_out,
        cpu_max,
        rss_avg,
        rss_min_out,
        rss_max,
    )?;

    println!("\nResults written to: {}", run_dir.display());
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_interval(
    jsonl: &mut File,
    idx: u64,
    elapsed_total_s: f64,
    interval_s: f64,
    msgs: u64,
    throughput: f64,
    hist: &LatencyHistogram,
    cpu: f32,
    rss_mb: f64,
) -> std::io::Result<()> {
    let avg = hist.avg();
    let p50 = hist.percentile(50.0);
    let p99 = hist.percentile(99.0);
    let p999 = hist.percentile(99.9);
    let max = hist.max();
    println!(
        "[interval {idx}] t={elapsed_total_s:6.1}s msgs={msgs:>7} thr={throughput:>9.0} msg/s  \
         avg={avg:6.2} p50={p50} p99={p99} p999={p999} max={max} ms  cpu={cpu:5.1}% rss={rss_mb:6.1}MB"
    );
    writeln!(
        jsonl,
        "{{\"type\":\"interval\",\"idx\":{idx},\"elapsed_s\":{elapsed_total_s:.2},\
         \"interval_s\":{interval_s:.2},\"msgs\":{msgs},\"throughput_msg_s\":{throughput:.2},\
         \"lat_avg_ms\":{avg:.2},\"lat_p50_ms\":{p50},\"lat_p99_ms\":{p99},\"lat_p999_ms\":{p999},\
         \"lat_max_ms\":{max},\"cpu_pct\":{cpu:.1},\"rss_mb\":{rss_mb:.1}}}"
    )?;
    jsonl.flush()
}

#[allow(clippy::too_many_arguments)]
fn write_summary(
    jsonl: &mut File,
    run_dir: &Path,
    hist: &LatencyHistogram,
    total_bytes: u128,
    duration_s: f64,
    warmup: u64,
    rpp: &RecordsPerPoll,
    cpu_avg: f64,
    cpu_min: f64,
    cpu_max: f64,
    rss_avg: f64,
    rss_min: f64,
    rss_max: f64,
) -> std::io::Result<()> {
    let measured = hist.count;
    let throughput_msg_s = if duration_s > 0.0 {
        measured as f64 / duration_s
    } else {
        0.0
    };
    let throughput_mb_s = if duration_s > 0.0 {
        (total_bytes as f64 / (1024.0 * 1024.0)) / duration_s
    } else {
        0.0
    };
    let (min, avg, max) = (hist.min(), hist.avg(), hist.max());
    let stddev = hist.stddev();
    let p50 = hist.percentile(50.0);
    let p90 = hist.percentile(90.0);
    let p95 = hist.percentile(95.0);
    let p99 = hist.percentile(99.0);
    let p999 = hist.percentile(99.9);

    println!("\n{}", "=".repeat(70));
    println!("SUMMARY — Rust Kafka Consumer (warmup {warmup} excluded)");
    println!("{}", "=".repeat(70));
    println!("Measured messages: {measured}");
    println!("Duration:          {duration_s:.2} s");
    println!("Throughput:        {throughput_msg_s:.0} msg/s  ({throughput_mb_s:.2} MiB/s)");
    println!(
        "E2E latency (ms):  min={min} avg={avg:.2} stddev={stddev:.2} p50={p50} p90={p90} p95={p95} p99={p99} p99.9={p999} max={max}"
    );
    println!("CPU (% one core):  avg={cpu_avg:.1} min={cpu_min:.1} max={cpu_max:.1}");
    println!("RSS (MB, current): avg={rss_avg:.1} min={rss_min:.1} max={rss_max:.1}");
    println!("{}", "=".repeat(70));

    let rpp_n = rpp.count;
    let rpp_min = rpp.min();
    let rpp_mean = rpp.mean();
    let rpp_p50 = rpp.percentile(50.0);
    let rpp_p99 = rpp.percentile(99.0);
    let rpp_max = rpp.max();
    writeln!(
        jsonl,
        "{{\"type\":\"summary\",\"client\":\"rust\",\"messages\":{measured},\"duration_s\":{duration_s:.2},\
         \"throughput_msg_s\":{throughput_msg_s:.2},\"throughput_mib_s\":{throughput_mb_s:.2},\
         \"lat_min_ms\":{min},\"lat_avg_ms\":{avg:.2},\"lat_stddev_ms\":{stddev:.2},\"lat_p50_ms\":{p50},\"lat_p90_ms\":{p90},\
         \"lat_p95_ms\":{p95},\"lat_p99_ms\":{p99},\"lat_p999_ms\":{p999},\"lat_max_ms\":{max},\
         \"cpu_avg_pct\":{cpu_avg:.1},\"cpu_min_pct\":{cpu_min:.1},\"cpu_max_pct\":{cpu_max:.1},\
         \"rss_avg_mb\":{rss_avg:.1},\"rss_min_mb\":{rss_min:.1},\"rss_max_mb\":{rss_max:.1},\
         \"rpp_n\":{rpp_n},\"rpp_min\":{rpp_min},\"rpp_mean\":{rpp_mean:.2},\"rpp_p50\":{rpp_p50},\
         \"rpp_p99\":{rpp_p99},\"rpp_max\":{rpp_max}}}"
    )?;
    jsonl.flush()?;

    // Human-readable summary alongside the JSONL.
    let mut md = File::create(run_dir.join("summary.md"))?;
    writeln!(md, "# Consumer E2E Latency — Rust\n")?;
    writeln!(md, "| metric | value |")?;
    writeln!(md, "|---|---|")?;
    writeln!(md, "| measured messages | {measured} |")?;
    writeln!(md, "| duration (s) | {duration_s:.2} |")?;
    writeln!(md, "| throughput (msg/s) | {throughput_msg_s:.0} |")?;
    writeln!(md, "| throughput (MiB/s) | {throughput_mb_s:.2} |")?;
    writeln!(md, "| latency min (ms) | {min} |")?;
    writeln!(md, "| latency avg (ms) | {avg:.2} |")?;
    writeln!(md, "| latency p50 (ms) | {p50} |")?;
    writeln!(md, "| latency p90 (ms) | {p90} |")?;
    writeln!(md, "| latency p95 (ms) | {p95} |")?;
    writeln!(md, "| latency p99 (ms) | {p99} |")?;
    writeln!(md, "| latency p99.9 (ms) | {p999} |")?;
    writeln!(md, "| latency max (ms) | {max} |")?;
    md.flush()
}

fn write_config(run_dir: &Path, args: &Args) -> std::io::Result<()> {
    let mut f = File::create(run_dir.join("config.json"))?;
    writeln!(
        f,
        "{{\"bootstrap\":\"{}\",\"topic\":\"{}\",\"group_id\":\"{}\",\"throughput_msg_s\":{},\
         \"duration_s\":{},\"message_size\":{},\"partitions\":{},\"warmup_messages\":{},\
         \"interval_s\":{},\"poll_timeout_ms\":{},\"produce\":{}}}",
        args.bootstrap,
        args.topic,
        args.group_id,
        args.throughput,
        args.duration_s,
        args.message_size,
        args.partitions,
        args.warmup_messages,
        args.interval_s,
        args.poll_timeout_ms,
        args.produce,
    )
}
