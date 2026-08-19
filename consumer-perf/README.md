# consumer-perf — E2E latency / CPU / memory benchmark

A small, self-contained benchmark for the Rust Kafka consumer that measures
**end-to-end latency** (`consume_time − record.timestamp()`) under a **live
producer** running at a **fixed throughput**, while sampling the consumer
process's **CPU and RSS** at a regular interval.

It is a separate workspace crate so the client crate's dependency tree stays
clean (`sysinfo` is a benchmark-only dependency). Design rationale and the
analysis of prior art (the `example-confluent-kafka-native-java` suite, OMB,
coordinated omission) live in
[`design/current/consumer-perf-benchmark-analysis.md`](../design/current/consumer-perf-benchmark-analysis.md).

## What it does

1. Subscribes with `auto.offset.reset=latest` (KIP-848 `group.protocol=consumer`),
   so it only sees **new** records — the e2e latency reflects produce→consume
   delay, not the age of a disk backlog.
2. Waits until partitions are **assigned** (KIP-848 cold-join can take tens of
   seconds) before doing anything else.
3. **Then** launches `kafka-producer-perf-test.sh` at the requested fixed
   throughput. Consumer-first ordering is guaranteed because the producer is a
   child process spawned only after assignment.
4. Polls in a tight loop, recording each record's latency into a fixed-bucket
   **streaming histogram** (1 ms buckets, 0–60 s; O(1) memory, **no per-record
   allocation**). Key/value bytes are never copied — the deserializer returns
   the byte *length*.
5. Every `--interval` seconds: samples CPU% + RSS, prints a line, and appends a
   JSONL metric record; resets the per-interval histogram.
6. After `--duration` seconds of measurement, stops the producer, closes the
   consumer, and writes a final summary.

Warmup messages are consumed but excluded from measurement (connection ramp,
cache warm-up).

## Prerequisites

- A Kafka broker on `localhost:9092` (override with `--bootstrap`).
- Apache Kafka CLI scripts (`kafka-producer-perf-test.sh`, `kafka-topics.sh`).
  Default location `~/dev/opensource/kafka/bin`; override with `--kafka-bin`.

## Usage

```sh
# Defaults: localhost:9092, topic consumer-perf-bench, 50k msg/s, 60s, 1KiB
cargo run -p consumer-perf --release

# 200k msg/s for 2 minutes, 2KiB records, 12 partitions
cargo run -p consumer-perf --release -- \
    --throughput 200000 --duration 120 --message-size 2048 --partitions 12

# Sweep a few rates (each run is one fixed throughput)
for r in 5000 50000 200000; do
  cargo run -p consumer-perf --release -- --throughput "$r" --duration 90
done

# Drive the producer yourself (e.g. to compare clients on one stream)
cargo run -p consumer-perf --release -- --no-produce --throughput 50000
```

Always build/run with `--release` for representative numbers.

### Remote runs (EC2 / bare metal)

`tools/deploy_and_run_perf/deploy_and_run_perf.py` deploys, builds, and runs
this harness — and the `compare/` librdkafka and Java arms — on a remote
Debian/Ubuntu host over SSH, then copies the results back:

```sh
python3 tools/deploy_and_run_perf/deploy_and_run_perf.py user@host \
    --test rust-consumer --env-file ../consumer.env --results-dir ./perf-results
# Same .env, other clients: --test librdkafka-consumer / java-consumer /
# python-consumer. Configure BOOTSTRAP_SERVERS, TOPIC_NAME, THROUGHPUT,
# TEST_DURATION_SECONDS, VALUE_SIZE, PARTITIONS, WARMUP_MESSAGES,
# INTERVAL_SECONDS in the .env; EXTRA_CONSUMER_ARGS passes harness-specific
# flags (e.g. --peak, fetch tuning, --client-config) through verbatim.
```

### Options

| flag | default | meaning |
|---|---|---|
| `-b, --bootstrap` | `localhost:9092` | bootstrap servers |
| `-t, --topic` | `consumer-perf-bench` | topic |
| `-g, --group-id` | `consumer-perf-<ts>` | consumer group (fresh per run) |
| `-r, --throughput` | `50000` | producer target msg/s (fixed) |
| `-d, --duration` | `60` | measurement window (s), after warmup |
| `--message-size` | `1024` | producer record size (bytes) |
| `--partitions` | `8` | partitions when creating the topic |
| `-w, --warmup-messages` | `5000` | records skipped before measuring |
| `--interval` | `5` | metric reporting interval (s) |
| `--poll-timeout-ms` | `500` | `poll()` timeout |
| `--join-timeout` | `120` | max wait (s) for partition assignment |
| `--offset-reset` | `latest` | `latest` (live) or `earliest` (drains backlog) |
| `--kafka-bin` | `~/dev/opensource/kafka/bin` | Kafka CLI scripts dir |
| `--results-dir` | `consumer-perf/results` | output directory |
| `--no-produce` | (off) | don't launch a producer |
| `--no-create-topic` | (off) | don't create/verify the topic |
| `-v, --verbose` | (off) | log every poll (record counts / heartbeats) |

`--selftest-cpu` runs a standalone CPU/RSS sampler check (burns CPU, prints
readings) and exits — useful to confirm the `sysinfo` sampler works on your
platform.

> **Note on `--offset-reset`:** `latest` measures genuine live e2e latency.
> `earliest` drains any existing backlog first, so its reported latency is the
> *age of the backlog* (often large/clamped), not client processing latency —
> use it only to verify the consume path, not for latency numbers.

## Output

Each run writes to `consumer-perf/results/<group-id>/` (git-ignored):

- `config.json` — the run parameters.
- `metrics.jsonl` — one `{"type":"interval", ...}` line per interval plus a
  final `{"type":"summary", ...}` line.
- `summary.md` — human-readable final table.

### Metric schema (JSONL)

Interval line:

```json
{"type":"interval","idx":0,"elapsed_s":5.0,"interval_s":5.0,"msgs":250000,
 "throughput_msg_s":50000.0,"lat_avg_ms":6.4,"lat_p50_ms":6,"lat_p99_ms":12,
 "lat_p999_ms":31,"lat_max_ms":210,"cpu_pct":31.5,"rss_mb":42.1}
```

Summary line (carries `"client":"rust"` so a future Java / librdkafka arm can
emit the same schema and be compared by a single plotting script):

```json
{"type":"summary","client":"rust","messages":3000000,"duration_s":60.0,
 "throughput_msg_s":50000.0,"throughput_mib_s":48.8,"lat_min_ms":2,
 "lat_avg_ms":6.5,"lat_p50_ms":6,"lat_p90_ms":8,"lat_p95_ms":9,"lat_p99_ms":12,
 "lat_p999_ms":32,"lat_max_ms":252}
```

## Debugging the client (logging)

The client logs through the `log` facade (rich coverage of the
join / heartbeat / coordinator / fetch paths), and this binary installs
`env_logger`, so set `RUST_LOG` to see it. With `RUST_LOG` unset it defaults to
`warn`.

```sh
# All client logs at debug (verbose — includes the per-event-loop network spam)
RUST_LOG=confluent_kafka=debug cargo run -p consumer-perf --release -- ...

# Focused on the join: coordinator discovery + membership state machine,
# while silencing the high-volume "Node is not ready" delegate spam
RUST_LOG=confluent_kafka=debug,confluent_kafka::consumer::internals::network_client_delegate=info \
  cargo run -p consumer-perf --release -- ...

# Just the membership/heartbeat/coordinator managers at trace
RUST_LOG=confluent_kafka::consumer::internals::consumer_membership_manager=trace,\
confluent_kafka::consumer::internals::coordinator_request_manager=debug,\
confluent_kafka::consumer::internals::consumer_heartbeat_request_manager=debug \
  cargo run -p consumer-perf --release -- ...
```

Key targets for the slow/stuck-join investigation:

| target | what it tells you |
|---|---|
| `…::coordinator_request_manager` | FindCoordinator success/failure, coordinator discovery |
| `…::abstract_membership_manager` | member state transitions (UNSUBSCRIBED→JOINING→…) |
| `…::consumer_heartbeat_request_manager` | heartbeat send/receive, assignment delivery |
| `…::network_client_delegate` | connection readiness ("Node is not ready") — **noisy** |
| `confluent_kafka::network_client` | connection initiation, disconnects |

> When the join stalls, the tell-tale signature is a `FindCoordinator …
> server disconnected before a response was received` followed by
> `network_client_delegate` repeating `Node is not ready … FindCoordinator`
> indefinitely — the bootstrap connection is not re-established and the member
> never leaves `JOINING`.

## Notes & caveats

- **Shared clock**: e2e latency assumes producer and consumer share a wall
  clock. Running both on one host (the default) satisfies this; cross-host runs
  need NTP-tight clocks. The topic must use `CreateTime` timestamps (Kafka
  default), not `LogAppendTime`.
- **CPU%** is reported as percent of a single core (may exceed 100% under
  multi-core load), matching the Java MXBean-style measurement.
- **Fetch tuning**: this benchmark uses the client defaults (`fetch.min.bytes=1`,
  `max.poll.records=500`), which favor low latency — appropriate for an e2e
  latency test. Tuning these for throughput would require public config builders
  on `ConsumerConfig` (not yet exposed).
- **Pick a rate below saturation**: at rates the consumer cannot keep up with,
  latency grows without bound (queue buildup) — that is a real signal, but make
  sure you are measuring steady state, not catch-up.
- **librdkafka / Java arms**: deferred. The JSONL schema is shared so they can
  be added later and compared head-to-head.
