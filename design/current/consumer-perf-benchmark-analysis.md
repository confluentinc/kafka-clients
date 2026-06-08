# Consumer Performance Benchmark — Analysis & Findings

Status: analysis only (no code yet). Goal: design a **consumer e2e-latency**
benchmark for the Rust client that can be compared head-to-head against the
Java client and librdkafka (confluent-kafka-python), driven by a **live
producer** (`kafka-producer-perf-test.sh`) running at a fixed throughput for a
fixed duration.

This document (1) summarizes the existing benchmark suite in
`~/dev/njc-spike/example-confluent-kafka-native-java/consumer_tests`, (2) judges
whether it does the job efficiently, (3) summarizes external best practice, and
(4) lays out a gap analysis + recommended design for the Rust benchmark.

---

## 1. What exists today (example-confluent-kafka-native-java)

The repo the user linked
(`confluentinc/example-confluent-kafka-native-java`, branch
`test_consumer_benchmark_c_sync`) compares **three** consumer clients:

- **GraalVM native** — the Java Kafka client compiled to a native `.so`/`.dylib`
  via `native-image`, called from Python through generated bindings (`consumer.py`).
- **librdkafka** — via `confluent-kafka` Python.
- **Java** — the stock JVM `KafkaConsumer`, run via Gradle.

### 1.1 File inventory (`consumer_tests/`)

| File | Role |
|------|------|
| `run_consumer_benchmarks.sh` | **Main orchestrator.** Produces data, runs the 3 consumers (optionally in randomized order), generates a comparison report. |
| `run_orchestrator_1m.py` | Python orchestrator variant: creates topic, produces 1M msgs, runs the 3 clients via subprocess, writes `combined_results.json`. |
| `consumer_perf_test.py` | Simple throughput test (GraalVM vs librdkafka): "consume N messages as fast as possible, report msg/s + MB/s." |
| `benchmark_consumer_threaded.py` | Multi-threaded variant; N consumers sharing one GraalVM isolate; aggregates throughput + latency + RSS. |
| `consumer_performance_java.py` | Thin Python wrapper that shells out to Gradle (`runConsumerBenchmark`) to run the Java consumer. |
| `setup.sh` | venv + deps + `make` native lib. |
| `benchmark_results/...` | Captured runs (`metrics_*.jsonl`, `*_output.txt`, `config.json`, `*_report.md`). |

Plus, in the **examples** Gradle subproject (the actually-interesting one for our
purpose):

- `examples/.../E2ELatencyBenchmark.java` — **the e2e-latency benchmark** (the
  closest match to what the user wants; see §1.3).
- `examples/.../performance/JavaPerformanceTest.java`, `Metrics.java`,
  `CPUBucket.java`, `MemoryBucket.java`, `Bucket.java` — a more elaborate
  bucketed metrics framework.

### 1.2 The dominant pattern: "drain a backlog" throughput test

`consumer_perf_test.py`, `benchmark_consumer_threaded.py`, and the two
orchestrators all share one shape:

1. **Produce all the data first** with `kafka-producer-perf-test.sh ... --throughput -1`
   (as fast as possible).
2. **Then** start the consumer with `auto.offset.reset=earliest` and a fresh
   group id, and drain the whole backlog.
3. Measure wall-clock duration of the drain → `msg/s`, `MB/s`. A warmup count
   (first 1–2k messages) is excluded from timing.

It also computes a "latency" as `now_ms - record.timestamp()`, but because the
data was produced minutes earlier and is being drained from disk, **that number
is meaningless as e2e latency** — it is just "how old is this backlogged
record," dominated by the produce→consume gap, not by client processing latency.

**This is a throughput / catch-up benchmark, not an e2e-latency benchmark.**

### 1.3 The exception: `E2ELatencyBenchmark.java` (the good one)

This standalone class is the only piece that measures e2e latency *correctly*,
and it is the template to copy. Key design choices:

- `auto.offset.reset=latest` — consume only **new** records, so
  `pollTimeMs - record.timestamp()` is genuinely "time from produce to
  consumer-visible." (The backlog tests get this wrong by using `earliest`.)
- **Streaming fixed-bucket histogram** (`LatencyHistogram`): a `long[60001]`
  with 1 ms buckets, plus running count/sum/min/max. O(1) memory, O(1) record,
  percentiles by a single cumulative scan. **No per-record `List.add()`** — this
  is the efficient choice (contrast with the Python scripts; see §2).
- **Per-interval reporting** (default 5 s): every interval it prints
  throughput, avg/p50/p99/p999 latency, **CPU %**, and **heap MB**, then resets
  the interval histogram. Overall histogram is kept separately for the final
  summary.
- **CPU %** via `OperatingSystemMXBean.getProcessCpuTime()` delta ÷ wall-clock
  delta (process CPU time / elapsed). **Memory** via
  `MemoryMXBean.getHeapMemoryUsage().getUsed()`.
- Warmup messages excluded; final summary prints min/avg/p50/p90/p95/p99/p999/max
  and a one-line JSON blob for downstream aggregation.
- Tuning relevant to a consumer latency test: `max.poll.records=2500`,
  `fetch.min.bytes=4MiB`, `max.partition.fetch.bytes=4MiB`, `ByteArray`
  (de)serializers (no decode cost), `group.protocol=classic`.

Caveats to fix when we adapt it:
- It **hardcodes Confluent Cloud SASL_SSL credentials** in the source. Must be
  parameterized (env / args), never committed.
- It does **not launch the producer** — it assumes something else is producing.
  The user wants the harness to start consumer→producer itself.
- `group.protocol=classic` — our Rust client is **KIP-848 only** (`consumer`).
  Must override.

---

## 2. Is it efficient? Assessment

### What's good (keep / copy)
- ✅ **`E2ELatencyBenchmark`'s streaming histogram** — the right way to do
  latency at high message rates: bounded memory, no allocation per record, cheap
  percentile extraction.
- ✅ **Warmup exclusion** — avoids JIT / page-cache / connection-setup skew.
- ✅ **Per-interval CPU/mem/latency sampling** — exactly the "measure every
  interval" cadence the user asked for, and it's done off the message count
  boundary so it's nearly free.
- ✅ **Randomized client order + cold JVM (`--no-daemon`)** in
  `run_consumer_benchmarks.sh` — sensible fairness measures against page-cache
  and JIT-warmup bias.
- ✅ **ByteArray deserializers** — keeps the measurement about the *client*, not
  about user decode cost.

### What's inefficient / wrong (do NOT copy)
- ❌ **Per-record `latencies.append()` in the Python scripts**
  (`benchmark_consumer_threaded.py`, `consumer_perf_test.py`): unbounded list
  growth → GC pressure, cache misses, and at 1M+ records the list itself
  perturbs the measurement. Percentiles are then computed by `sorted()` over the
  whole list. This is the classic benchmark anti-pattern. Use a histogram.
- ❌ **Percentile-by-index on an unsorted/partially-handled list**
  (`latencies[int(len*0.95)]`) — in the threaded script this indexes into a list
  that isn't reliably sorted, and aggregates *per-consumer averages* as if they
  were samples. The reported p95/p99 are not trustworthy.
- ❌ **`earliest` + pre-produced backlog labeled as "latency"** — measures
  staleness of disk data, not e2e latency (see §1.2). Misleading.
- ❌ **`time.time()` (wall clock, ms) for latency on the Python side** — fine for
  ms-scale e2e but the Python interpreter + binding overhead is in the hot loop;
  the Python harness is not a faithful CPU/throughput measurement of the
  *client*. (For Rust vs Java vs librdkafka we should drive each client in its
  **native** language harness, not through Python, to avoid the binding tax —
  except librdkafka, which is legitimately measured via its Python binding since
  that's a normal way it's consumed. Decide per §4.)
- ❌ **CPU % = 0 hardcoded** in `benchmark_consumer_threaded.py`'s result blob —
  it never actually samples CPU; only `E2ELatencyBenchmark.java` does CPU
  correctly.
- ❌ **`consecutive_empty_polls` exit heuristic** — a fixed "20 empty polls then
  quit" is racy for a *live* producer (a transient stall ends the run early).
  For a duration-bounded live test, bound by **wall-clock duration**, not by
  empty polls.

### Net
The suite is a decent **throughput** comparison harness with good orchestration
ergonomics, but only `E2ELatencyBenchmark.java` measures **e2e latency**
properly, and even it doesn't drive a live producer. The Python latency numbers
are not reliable. For our purpose we take the *methodology* of
`E2ELatencyBenchmark.java` and the *orchestration* of `run_consumer_benchmarks.sh`,
and discard the backlog/Python-latency parts.

---

## 3. External best practice (web research)

- **Coordinated omission** is the central latency-benchmarking pitfall: if a
  consumer stalls, a naive harness simply measures fewer samples during the
  stall and under-reports the tail. The fix is HdrHistogram-style correction or
  a producer-driven schedule.
  - **Important nuance for *our* design:** in a producer-driven e2e test where
    latency = `consume_time - record.produce_timestamp`, the **producer** sets
    the schedule and every record carries its own intended timeline. As long as
    we measure *every* delivered record against its embedded timestamp (not
    against a consumer-side "expected poll time"), coordinated omission is
    largely neutralized — a consumer stall shows up directly as a latency spike
    on the backed-up records, not as missing samples. This is exactly what
    `E2ELatencyBenchmark.java` does. We must keep `auto.offset.reset=latest` and
    a **fixed-throughput** producer (not `--throughput -1`) for this to hold.
- **HdrHistogram** — recommended for retaining high resolution across a wide
  dynamic range and for coordinated-omission correction. A fixed 1 ms-bucket
  array (as in the Java class) is a simpler equivalent that's adequate for ms-scale
  e2e latency; HdrHistogram (crate `hdrhistogram` in Rust) is better if we want
  µs resolution and log-scale range with the same memory. **Recommendation: use
  the `hdrhistogram` crate in Rust** — it's the standard, gives correct
  percentiles, and is allocation-free on the record path.
- **OpenMessaging Benchmark (OMB)** is the de-facto open standard for
  reproducible Kafka workloads + high-fidelity latency histograms; worth citing
  as prior art and mirroring its methodology (fixed target rate, long steady
  window, full distribution).
- **Methodology consensus:** warm up ~5 min (page cache, GC, JIT for Java),
  then hold a **fixed target throughput** for **10+ min**, and report the full
  distribution — p50/p95/p99/p999 *and* max — not just one percentile. Tail
  latency is the point.
- **Confluent's own guidance** ("99th Percentile Latency at Scale", "Kafka
  Performance"): latency and throughput trade off; measure at a *fixed* offered
  load and report percentiles; `fetch.min.bytes` / `fetch.max.wait.ms` /
  `max.poll.records` directly shape consumer latency and must be pinned and
  reported.

Sources:
- [Benchmarking Message Queue Latency — Brave New Geek](https://bravenewgeek.com/benchmarking-message-queue-latency/)
- [Coordinated omission — Brave New Geek](https://bravenewgeek.com/tag/coordinated-omission/)
- [On Coordinated Omission — ScyllaDB](https://www.scylladb.com/2021/04/22/on-coordinated-omission/)
- [99th Percentile Latency at Scale with Apache Kafka — Confluent](https://www.confluent.io/blog/configure-kafka-to-minimize-latency/)
- [Apache Kafka Performance, Latency, Throughput — Confluent Developer](https://developer.confluent.io/learn/kafka-performance/)
- [Kafka Latency: Measuring p50, p99, p99.9 — Conduktor](https://www.conduktor.io/blog/kafka-latency)
- [Kafka Benchmark Analysis — RisingWave](https://risingwave.com/blog/kafka-benchmark-analysis-performance-and-latency/)
- [Load testing producer/consumer/e2e latencies for Kafka on GKE — Medium](https://medium.com/google-cloud/load-testing-producer-consumer-and-end-to-end-latencies-for-kafka-on-google-kubernetes-engine-8191c09f57e7)

---

## 4. Gap analysis: existing suite vs. what we want

| Requirement (user) | Existing suite | Gap |
|---|---|---|
| Measure **e2e latency** = `now - record.timestamp()` per record | Only `E2ELatencyBenchmark.java`; Python scripts mislabel staleness as latency | Build a proper e2e harness for **Rust**; reuse Java one as reference |
| **Live producer at fixed throughput**, consumer started first | All scripts pre-produce a backlog with `--throughput -1` | Need new orchestration: start consumer (`latest`) → wait for join → start `kafka-producer-perf-test.sh --throughput R` |
| Specify **duration** and **producer throughput** per test | Specify message *count* and unbounded throughput | Switch to duration-bounded + rate-bounded |
| **CPU + memory every interval** | Done well in Java only (CPU=0 in Python) | Implement interval CPU/RSS sampling in the Rust harness |
| **Efficient consumer loop** (simple latency monitoring, no wasteful processing) | Python appends every latency to a list | Use a streaming histogram (`hdrhistogram`); ByteArray deserializer; no per-record alloc |
| **Rust vs Java vs librdkafka** comparison | GraalVM-native vs librdkafka vs JVM-Java | Add a **Rust** harness; keep Java (`E2ELatencyBenchmark`) + librdkafka as the other two arms |

### Key design decisions to make (for the plan step)
1. **Producer reference clock.** `kafka-producer-perf-test.sh` stamps records
   with `CreateTime` = wall clock at send. e2e latency = `consume_wall_clock -
   record.timestamp()`. This requires **consumer and producer to share a clock**
   — trivially true when both run on the same host (our case), but must be
   stated as a constraint (no clock skew). Topic must use `CreateTime`
   (`message.timestamp.type=CreateTime`, the default), not `LogAppendTime`.
2. **Fixed throughput, not max.** Producer runs `--throughput R --num-records
   (R × duration)` (or a large count and kill at duration). Pick a rate well
   below saturation so latency reflects steady state, not queue buildup. Offer a
   sweep (e.g. 5k, 50k, 200k msg/s).
3. **Consumer-first ordering.** Start consumer, wait until it has **joined the
   group and been assigned partitions** (KIP-848 — and note the known
   `consumer_initial_join_latency` ~25–36 s cold-join issue: warm up / wait for
   first assignment before starting the producer, else the first records are
   late through no fault of steady-state latency).
4. **Histogram.** Use `hdrhistogram` crate in the Rust harness; mirror with the
   Java fixed-bucket histogram (already present) and librdkafka via Python +
   `numpy`/`hdrhistogram`-py for percentiles. Report identical percentile set.
5. **Harness language per client.** To avoid the Python-binding tax distorting
   CPU/throughput, run **Rust** and **Java** in their native harnesses
   (a `--bin` for Rust, the Gradle task for Java). librdkafka is fairly measured
   through its idiomatic Python binding. All three emit the **same JSONL metric
   schema** so one plotter compares them.
6. **Metric schema (JSONL, one line per interval + a final summary line).**
   Suggest: `{ts, elapsed_s, interval_msgs, throughput_msg_s, throughput_mb_s,
   lat_avg_ms, lat_p50, lat_p99, lat_p999, lat_max, cpu_pct, rss_mb}` and a
   final `{summary:true, ...overall percentiles...}`. Lets us reuse a single
   plotting script across all three clients.

---

## 5. Recommended shape for the Rust benchmark (preview — to be planned)

- **`src/bin/consumer_perf.rs`** (Rust): subscribe with `auto.offset.reset=latest`,
  `ByteArrayDeserializer`, tuned fetch config; poll loop; per-record
  `hdrhistogram` record of `now_ms - record.timestamp()`; interval sampler
  (every `--interval` s) emitting CPU% (read own `/proc` or `getrusage` on
  Linux; on macOS use `libproc`/`task_info` or shell out to `ps`) and RSS;
  bounded by `--duration`; warmup skip; JSONL output. Efficient: zero per-record
  heap alloc beyond what the histogram needs, no decode (bytes only).
- **Orchestrator** (`xtask` per CLAUDE.md §6 — "use xtask Rust programs instead
  of shell scripts", though the *reference* repo used bash): create topic →
  start chosen consumer (Rust/Java/librdkafka) → wait for partition assignment
  → launch `kafka-producer-perf-test.sh --throughput R` for `duration` →
  on duration elapse, stop consumer → collect JSONL → optional comparison
  report. Parameterized by `--duration`, `--throughput`, `--message-size`,
  `--partitions`, `--clients`.
- **Reuse** Java `E2ELatencyBenchmark.java` (parameterized, creds removed,
  `group.protocol` configurable) as the Java arm; a small Python script
  (mirroring `consumer_perf_test.py` but latest-offset + histogram + interval
  CPU/RSS via `psutil`) as the librdkafka arm.
- **Fairness:** warm both page cache and consumer group membership before the
  measured window; pin and report all fetch-tuning configs; run each client in a
  fresh group; randomize client order across repeated runs.

### Anti-patterns to avoid (carried from §2)
- No per-record `Vec<latency>`; histogram only.
- No `earliest`-backlog masquerading as latency.
- No empty-poll exit heuristic for a live test; bound by duration.
- Don't put the interval CPU/RSS syscall inside the per-record path; sample on
  the interval boundary only.
- Keep the poll loop allocation-free and decode-free (ByteArray).

---

## 6. Decisions (resolved with user) & implementation

Resolved:

1. **Location** — separate workspace crate **`consumer-perf/`** (binary
   `consumer-perf`). Keeps `sysinfo` out of the client crate's dependency tree;
   convenient for frequent `cargo run -p consumer-perf --release` invocations.
   Results go to **`consumer-perf/results/<group-id>/`**, which is git-ignored so
   the repo stays readable.
2. **librdkafka / Java arms** — deferred. The JSONL schema carries
   `"client":"rust"` so other arms can emit the same shape and be compared by one
   plotter later.
3. **Throughput** — each run is a single fixed `--throughput`; sweeps are a shell
   `for` loop over runs (documented in the crate README).
4. **Target** — `localhost:9092` to start.

Implemented in `consumer-perf/src/main.rs`:

  - `auto.offset.reset=latest`, KIP-848 `group.protocol=consumer`, zero-copy
    length-only deserializer (no per-record byte copy), streaming fixed-bucket
    `LatencyHistogram` (the §1.3 pattern, dependency-free), per-`--interval`
    CPU%/RSS via `sysinfo`, duration-bounded measurement window, warmup
    exclusion, JSONL + `summary.md` + `config.json` output.
  - Consumer-first ordering: it subscribes, **waits for partition assignment**
    (`--join-timeout`, default 120 s), then spawns `kafka-producer-perf-test.sh`
    at the fixed rate as a child process.

### 6.1 Empirical findings while smoke-testing

Environment confirmed healthy: local broker `localhost:9092`, `group.version=1`
finalized (KIP-848 supported server-side); `kafka-producer-perf-test.sh`
completed **2,000,000 records at 3000 msg/s with ~4 ms avg / 18 ms p99.9
latency** on each of two topics — so the broker and producer path are fine.

**The benchmark works** when the consumer joins. One run (`--offset-reset
earliest`) joined in **0.5 s** and then consumed **842 k+ records at ~140 k
msg/s (135 MiB/s)**, emitting a full interval line + final summary. The
diagnostic `[poll]` heartbeats showed the exact lifecycle: fast assignment →
~10 s of empty polls (initial fetch warm-up) → 500-record batches at full speed.

**The dominant blocker is a severely flaky KIP-848 join** (the
`consumer_initial_join_latency` issue, far worse than the documented ~25–36 s):

  | run | client | join time | result |
  |---|---|---|---|
  | #1 | consumer-perf (latest) | 135 s | then no data, timed out |
  | #2 | consumer-perf (earliest) | **0.5 s** | ✅ 842 k recs @ ~140 k msg/s |
  | #3 | consumer-perf (latest) | >200 s | timed out |
  | #4 | consumer-perf (latest) | >150 s | timed out |
  | #5 | `src/bin/consumer_test.rs` (earliest) | **>6 min, never** | stuck |

Same binary, same broker, same topic shape — join time ranged from 0.5 s to
never. The `[join]` heartbeats show the consumer polling with `assignment=0` for
the entire wait. Reproduced with the repo's existing `consumer_test`, confirming
this is the **client join/reconciliation path**, not the benchmark.

So: build / lint / CLI / topic creation / producer orchestration / assignment
wait / measurement loop / histogram / interval reporting / summary + results
files are all **verified working**. Clean steady-state *live* (`latest`) latency
numbers are gated on the client join becoming reliable. Investigating the
intermittent join stall is the highest-value next step for the consumer itself.

### 6.1.1 Root cause surfaced by logging

The client logs through the `log` facade but **no binary installed a backend**,
so all of it was dropped (and `RUST_LOG` did nothing). After wiring `env_logger`
into `consumer-perf` (init in `main`, default filter `warn`), `RUST_LOG=…=debug`
immediately revealed the stall mechanism:

```
coordinator_request_manager  FindCoordinator request failed due to retriable
                             exception: The server disconnected before a
                             response was received.
abstract_membership_manager  Member … transitioned from UNSUBSCRIBED to JOINING.
network_client_delegate      Node is not ready, handle the request in the next
                             event loop: node=127.0.0.1:9092 (id: -2 …),
                             request=UnsentRequest { api_key: "FindCoordinator",
                             … enqueue_time_ms: <fixed> }     ← repeats forever
```

The bootstrap node's first `FindCoordinator` hits a "server disconnected before
response," and from then on the **same** unsent `FindCoordinator` request (note
the constant `enqueue_time_ms`) is re-evaluated against the bootstrap node
(`id: -2`) every event-loop iteration — **102,894 times in ~22 s** (~4.7k/s, a
busy-loop that also burns CPU) — while the node stays "not ready." The bootstrap
connection is never re-established after the disconnect, so the coordinator is
never discovered and the member never leaves `JOINING`.

This points the investigation at the `NetworkClientDelegate` connection-readiness
/ reconnect path (and `network_client` connection re-initiation after a
disconnect) — not at the membership or heartbeat managers. The
README "Debugging the client" section documents the `RUST_LOG` targets.

### 6.2 Bug found & fixed in the benchmark

The first per-interval CPU reading came out as `0.0%`. A standalone
`--selftest-cpu` check isolated it: on macOS, `sysinfo`'s per-process
`cpu_usage()` only becomes valid after the process has been refreshed **twice**,
so a single prime refresh left the first `sample()` reading at zero. Fixed by
warming the sampler with two `MINIMUM_CPU_UPDATE_INTERVAL`-spaced refreshes in
`ResourceSampler::new()`; the self-test now reads ~100 % under a CPU-burn loop
from sample 0.
