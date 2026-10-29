# Java producer performance test

A Java equivalent of `python/test/performance/producer_performance_test.py`.
Same env-variable interface, same `metrics.jsonl` output schema, same
test workflow (warmup → measured → drain → optional consumer verification).
Lets you benchmark the upstream Apache Kafka Java client against the same
cluster, workload, and metrics format the Rust client is measured with —
useful as a reference for "what should the bottom-line throughput / CPU
look like for this workload."

## Requirements

- JDK 17+
- Gradle 8.x (or use `gradle wrapper` once to generate `gradlew`)

## Build

```bash
cd tools/java-perf-test
gradle shadowJar
```

Produces `build/libs/java-perf-test-all.jar`.

## Run

Same env-variable interface as the Python harness. Example against a SASL_SSL
cluster:

```bash
SECURITY_PROTOCOL=SASL_SSL \
SASL_MECHANISM=PLAIN \
SASL_USERNAME=... \
SASL_PASSWORD=... \
BOOTSTRAP_SERVERS=<bootstrap-host>:9092 \
TOPIC_NAME=test-topic \
KEY_SIZE=100 \
VALUE_SIZE=2048 \
LIMIT_RPS=10000 \
LINGER_MS=5 \
MAX_IN_FLIGHT=5 \
WARMUP_SECONDS=10 \
TEST_DURATION_SECONDS=60 \
java -jar build/libs/java-perf-test-all.jar
```

`metrics.jsonl` is written to the current working directory in the same
schema the Python harness uses — `tools/performance_metrics_plot/plot_metrics.py`
will plot it the same way.

Or use the Gradle `application` plugin directly:

```bash
gradle run
```

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `TOPIC_NAME` | `test-topic` | Deleted and re-created before the run unless `CREATE_TOPIC=False`. |
| `CREATE_TOPIC` | `True` | Delete the topic (if present) and re-create it, waiting 10 s after each step. |
| `PARTITIONS` | `-1` (broker default) | Partition count of the re-created topic; the replication factor is always the broker default. |
| `NUM_MESSAGES` | `0` (time-based) | If > 0, run until this many messages are produced. |
| `LIMIT_RPS` | `0` (unlimited) | Send-rate cap; the run is then bounded by `LIMIT_RPS × TEST_DURATION_SECONDS` messages or `TEST_DURATION_SECONDS` + 10 s, whichever comes first. |
| `KEY_SIZE` | `0` (no key) | Bytes. Set to >0 to test keyed partitioning. |
| `VALUE_SIZE` | `2048` | Bytes. |
| `BATCH_SIZE` | `1024` (KiB → 1 MiB) | Interpreted in **KiB** (`× 1024`). Matches the C/Rust tests. |
| `MAX_REQUEST_SIZE` | `batch × 64`, capped 8 MiB | KiB. Matches the C/Rust tests. |
| `BUFFER_MEMORY` | unset | MiB if set. |
| `BUFFER_MEMORY_ALLOCATION_STRATEGY` | unset | `full` / `incremental`; maps to `buffer.memory.allocation.strategy` (kafka-clients 4.4+). |
| `LINGER_MS` | `5` | ms. Matches the C/Rust tests. |
| `COMPRESSION_TYPE` | `none` | `none` / `gzip` / `snappy` / `lz4` / `zstd` |
| `ENABLE_IDEMPOTENCE` | `false` | |
| `ACKS` | `all` | `all` / `-1` / `1` / `0`. Matches the C/Rust/Python tests. |
| `MAX_IN_FLIGHT` | unset | maps to `max.in.flight.requests.per.connection` |
| `USE_DEFAULTS` | `False` | `True`: set only the bootstrap servers, serializers and security settings, leaving every tuning knob above at the client default. |
| `WARMUP_SECONDS` | `120` | Matches the C/Rust tests. |
| `TEST_DURATION_SECONDS` | `600` | |
| `P99_LIMIT_MS` | `0` (off) | Per-message p99 latency budget (ms); a breach exits non-zero. |
| `RESULTS_FILE` | `results.json` | Machine-readable summary file (matches the C/Rust/Python tests). |
| `DO_VERIFY` | `True` | Assert RecordMetadata fields are populated. |
| `VERIFY_CONSUMED` | `False` | Consume back and check murmur2 partition placement (Java default; NOT cross-client — see notes). |
| `SECURITY_PROTOCOL` | unset | `SSL` / `SASL_PLAINTEXT` / `SASL_SSL` |
| `SASL_MECHANISM` | unset | `PLAIN` / `SCRAM-SHA-256` / etc. |
| `SASL_USERNAME` | unset | |
| `SASL_PASSWORD` | unset | |

A numeric variable that does not parse (`LIMIT_RPS=10k`) fails the run, naming
the variable, rather than falling back to its default.

## Output

`metrics.jsonl` (cwd, hard-coded filename to match the Python harness). One
JSON object per second of measured interval. Schema:

```jsonc
{
  "rss":      {"average": "...", "max": "...", "total": "...", "count": "..."},
  "cpu":      {"average": "...", "max": "...", "total": "...", "count": "..."},
  "latency":  {"average": "...", "max": "...", "total": "...", "count": "..."},  // ms
  "bytes":    {"average": "...", "max": "...", "total": "...", "count": "..."},
  "messages": {"average": "...", "max": "...", "total": "...", "count": "..."},
  "window_start_ms":      "...",
  "window_end_ms":        "...",
  "measurement_start_ms": "...",
  "measurement_end_ms":   "..."
}
```

`results.json` (cwd; override with `RESULTS_FILE`). A machine-readable run
summary written by every producer perf test in the project with the same keys
(schema only; values shown as 0)
(`latency_ms` shape matches the consumer perf test's `results.json`); `client`
identifies the implementation (`java` here, `rust`, `librdkafka`, `rust-c-ffi`,
`python-rust`, `python-librdkafka` elsewhere):

```jsonc
{
  "test": "producer",
  "client": "java",
  "topic": "test-topic",
  "messages_measured": 0,
  "duration_s": 0.0,
  "throughput_msg_s": 0.0,
  "throughput_mib_s": 0.0,
  "latency_ms": {"min": 0, "avg": 0.0, "p50": 0, "p90": 0, "p95": 0,
                 "p99": 0, "p999": 0, "max": 0},
  "cpu_avg_pct": 0.0,
  "rss_avg_kib": 0.0
}
```

Empty bucket max is the literal string `"-inf"` (matches Python's
`str(-math.inf)`).

On SIGINT / SIGTERM the test stops sending and still writes `results.json`
(the JVM waits up to 25 s for it), then exits with the signal's status.

## Consistency with the C and Rust tests

This test is aligned with `c/tests/producer_perf_test.c` and
`rust/tests/performance/producer_perf_test.rs` so all three are directly comparable:

- **CPU** is an interval rate from `/proc/self/stat` (utime+stime over each
  window, `USER_HZ = 100`) — per-core percent where 100 % = one core fully
  used, identical to the C/Rust tests. (Not `getProcessCpuLoad()`, which
  reports a fraction of *total* machine capacity — a different scale.)
- **RSS** is read from `/proc/self/status` VmRSS (Linux), same as C/Rust.
- **Config defaults** match: `batch.size` 1 MiB, `max.request.size` batch×64
  capped at 8 MiB, `linger.ms` 5, `WARMUP_SECONDS` 120, `acks=all` (`ACKS`).
- **Metrics lifecycle** matches: the collector runs from before warmup, so the
  JSONL captures warmup windows (`measurement_start_ms = -inf`); the measured
  interval is marked by `measurement_start_ms`/`measurement_end_ms`; a post-test
  await captures cooldown windows. The run summary averages CPU/RSS over the
  measured interval only.
- **p99 latency** is computed from a ms-resolution histogram and asserted
  against `P99_LIMIT_MS` (0 = off), exiting non-zero on a breach.

Java-specific notes:

- No `CLIENT_VERSION` switch — there's only one Java client.
- `VERIFY_CONSUMED` (a Java-only extra) checks **murmur2** partition placement,
  because the Java client's default partitioner is murmur2. It is **not**
  cross-client: the Rust client and librdkafka default to CRC-32
  (`consistent_random`), so their keyed records land in different partitions
  than murmur2 would choose. Only enable `VERIFY_CONSUMED` against a producer
  actually using murmur2. See `design/current/partitioner.md` for the
  divergence and the mixed-fleet warning.
