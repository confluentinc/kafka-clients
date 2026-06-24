# Java producer performance test

A Java equivalent of `bindings/python/test/performance/producer_performance_test.py`.
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

Same env-variable interface as the Python harness — the same `run_test.sh`
wrapper from the project root works if you point it at this jar instead of
`producer_performance_test.py`. Example against a SASL_SSL cluster:

```bash
SECURITY_PROTOCOL=SASL_SSL \
SASL_MECHANISM=PLAIN \
SASL_USERNAME=... \
SASL_PASSWORD=... \
BOOTSTRAP_SERVERS=pkc-xxxx.us-west-2.aws.devel.cpdev.cloud:9092 \
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
| `TOPIC_NAME` | `test-topic` | Topic must exist; this test does not auto-create. |
| `NUM_MESSAGES` | `0` (time-based) | If > 0, run until this many messages are produced. |
| `LIMIT_RPS` | `0` (unlimited) | Send-rate cap. |
| `KEY_SIZE` | `0` (no key) | Bytes. Set to >0 to test keyed partitioning. |
| `VALUE_SIZE` | `2048` | Bytes. |
| `BATCH_SIZE` | `1024` (KB → 1 MiB) | Interpreted in **KB** (`× 1024`). Matches the C/Rust tests. |
| `MAX_REQUEST_SIZE` | `batch × 64`, capped 8 MiB | KB. Matches the C/Rust tests. |
| `BUFFER_MEMORY` | unset | MB if set. |
| `LINGER_MS` | `5` | ms. Matches the C/Rust tests. |
| `COMPRESSION_TYPE` | `none` | `none` / `gzip` / `snappy` / `lz4` / `zstd` |
| `ENABLE_IDEMPOTENCE` | `false` | |
| `MAX_IN_FLIGHT` | unset | maps to `max.in.flight.requests.per.connection` |
| `WARMUP_SECONDS` | `120` | Matches the C/Rust tests. |
| `TEST_DURATION_SECONDS` | `600` | |
| `P99_LIMIT_MS` | `0` (off) | Per-message p99 latency budget (ms); a breach exits non-zero. |
| `DO_VERIFY` | `True` | Assert RecordMetadata fields are populated. |
| `VERIFY_CONSUMED` | `False` | Consume back and check murmur2 partition placement. |
| `SECURITY_PROTOCOL` | unset | `SSL` / `SASL_PLAINTEXT` / `SASL_SSL` |
| `SASL_MECHANISM` | unset | `PLAIN` / `SCRAM-SHA-256` / etc. |
| `SASL_USERNAME` | unset | |
| `SASL_PASSWORD` | unset | |

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

Empty bucket max is the literal string `"-inf"` (matches Python's
`str(-math.inf)`).

## Consistency with the C and Rust tests

This test is aligned with `bindings/c/tests/producer_perf_test.c` and
`tests/integration/producer_perf_test.rs` so all three are directly comparable:

- **CPU** is an interval rate from `/proc/self/stat` (utime+stime over each
  window, `USER_HZ = 100`) — per-core percent where 100 % = one core fully
  used, identical to the C/Rust tests. (Not `getProcessCpuLoad()`, which
  reports a fraction of *total* machine capacity — a different scale.)
- **RSS** is read from `/proc/self/status` VmRSS (Linux), same as C/Rust.
- **Config defaults** match: `batch.size` 1 MiB, `max.request.size` batch×64
  capped at 8 MiB, `linger.ms` 5, `WARMUP_SECONDS` 120, `acks=all`.
- **Metrics lifecycle** matches: the collector runs from before warmup, so the
  JSONL captures warmup windows (`measurement_start_ms = -inf`); the measured
  interval is marked by `measurement_start_ms`/`measurement_end_ms`; a post-test
  await captures cooldown windows. The run summary averages CPU/RSS over the
  measured interval only.
- **p99 latency** is computed from a ms-resolution histogram and asserted
  against `P99_LIMIT_MS` (0 = off), exiting non-zero on a breach.

Java-specific notes:

- No `CLIENT_VERSION` switch — there's only one Java client.
- The Java client's default partitioner is the same murmur2 as the other
  clients, so consumer-side partition verification (`VERIFY_CONSUMED`, a
  Java-only extra) works cross-client.
