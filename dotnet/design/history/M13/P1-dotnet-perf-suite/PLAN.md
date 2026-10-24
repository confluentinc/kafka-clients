# M13 / P1 — .NET performance-test suite (v2-vs-v3 head-to-head)

- **Binding:** `bindings/dotnet` (own milestone/phase numbering, independent of the repo-root Rust `design/`).
- **Agent number:** **N = 35** (next free: M11/P5 = N=33, M12/P1 = N=34, both DONE/archived; M13 is unused in `design/current` and `design/history`).
- **Mode:** **A** — binding + test-harness only. NO change to `src/ffi/**`, `target/include/confluent_kafka.h`, `cbindgen.toml`, or core `src/**`. If a Mode-B need surfaces, STOP and flag to main.
- **Branch:** `prashah_dev_dotnet_performance` (HEAD `8e3a1a90`, stacked on `prashah_dev_producer_integration`). Two "Dummy commit" commits sit on top of the M12/P1 close-out; the phase work stacks on `8e3a1a90`.
- **Status of this document:** DRAFT for approval. No code has been written; no actor/critic loop has run.

---

## 1. Objective

Build a .NET performance-test suite that **mirrors the Python perf suite's setup and the same tests, on the same lines** — running the **same benchmark against both client versions** (v3 = our binding, v2 = `confluent-kafka-dotnet`/librdkafka) for a head-to-head throughput/latency/CPU/memory comparison. The suite produces a `metrics.jsonl` byte-compatible with the Rust/Python suites so cross-language comparison and `tools/performance_metrics_plot` keep working, and it enforces the same shared p99 budget as a CI gate.

**Reference to mirror** (verified against source, not taken on faith):
- `bindings/python/test/performance/performance_common.py` — the `Metrics` harness, 1 ms latency histogram, `percentile_from_hist`, `metrics.jsonl` schema, warmup→measured→cooldown gating, `recreate_topic`.
- `bindings/python/test/performance/producer_performance_test.py` — pipelined producer throughput+latency, sync + async, `P99_LIMIT_MS` gate, optional `VERIFY_CONSUMED` + murmur2.
- `bindings/python/test/performance/consumer_performance_test.py` — end-to-end consumer latency, settle-to-live-edge, batch vs single poll, `results.json`, `P99_LIMIT_MS` gate.
- `bindings/python/test/performance/conftest.py` — the in-suite `apache/kafka:4.2.0` KRaft + KIP-848 testcontainer, subprocess re-invoke of the perf scripts with a short budget config.
- `bindings/python/test/performance/partitioner.py` — murmur2 (Java default partitioner).
- Rust: `tests/performance/main.rs`, `tests/performance/producer_perf_test.rs` (in-suite config), Makefile perf targets.

### 1.1 The v2-vs-v3 split — IN SCOPE (D1, reversed by the user 2026-08-18)

**[SETTLED by the user — reversal of the earlier "our-binding-only" stance]** The .NET suite mirrors Python's `CLIENT_VERSION` v2-vs-v3 head-to-head:
- **v3 = our binding** (`Confluent.Kafka`, this repo's dotnet binding).
- **v2 = `confluent-kafka-dotnet` (ckd)** — the librdkafka-based incumbent, added as a NuGet `PackageReference`.
- A `CLIENT_VERSION` switch runs the **identical benchmark** against either client, mirroring Python's `configuration_from_env(v2=...)` / `sasl_config_from_env(v2=...)` / v2 producer+consumer backends. Every Python `if client_version == 2: … else: …` fork is restored (§1.2), not collapsed.

#### 1.1.1 ⚠ CRITICAL structural constraint — the same-assembly collision

Our binding **and** ckd are **both** the `Confluent.Kafka` assembly + namespace. Per binding `CLAUDE.md` §4 (line 453): *"a shared id means a project can hold ckd 2.x **or** this client, never both."* Two assemblies with the same **simple name** cannot be referenced/loaded in one project — they collide in the output directory, and `extern alias` does **not** resolve a same-simple-name clash (it disambiguates two DLLs with distinct identities, not two DLLs both named `Confluent.Kafka`). Python sidesteps this because its clients import under **different module names** (`producer` vs `confluent_kafka`); **.NET cannot.**

**Therefore the split MUST be separate per-client executables** (this is D8, §4):
- A **client-agnostic `PerformanceCommon` library** — `Metrics`, the 1 ms histogram, `PercentileFromHist`, the measured-loop engine, env parsing, SASL mapping, the `metrics.jsonl` writer, the murmur2 partitioner, and the **backend abstraction** (`IProducerBackend` / `IConsumerBackend`). It has **NO client dependency**.
- A **v3 executable** referencing OUR binding + `PerformanceCommon`, and a **v2 executable** referencing **ckd** + `PerformanceCommon`. Each adapts its client to the shared backend interface. Because they are separate projects with separate output dirs, the two `Confluent.Kafka` assemblies never coexist — the collision is structurally avoided.
- `CLIENT_VERSION` selects **which executable** the launcher (Makefile / in-suite xUnit) runs — the analog of Python's subprocess-invoke-with-env. `CLIENT_VERSION` stays in the env-var contract for cross-language consistency; the launcher maps it to the exe.

### 1.2 Restore Python's v2 config/API branching (from the Explore extraction)

- **Config form** — v3 = **Java-form**; v2/ckd = **librdkafka-form**. Mirror the Python mappings exactly:
  | Concept | v3 (Java-form, our binding) | v2 (librdkafka-form, ckd) |
  |---|---|---|
  | max request size | `max.request.size` | `message.max.bytes` |
  | producer buffer | `buffer.memory` (bytes) | `queue.buffering.max.kbytes` + `queue.buffering.max.messages=2147483647` |
  | SASL | `sasl.mechanism` + `sasl.jaas.config` (PlainLoginModule) | `sasl.mechanism` + `sasl.username` / `sasl.password` |
  | partitioner | murmur2 (Java default) | **`partitioner=murmur2_random`** (set explicitly so partition verification matches Java's murmur2 — Python `v2_producer`, `producer_performance_test.py:515-516`) |
- **ckd API adapters** (mirror Python's `CompatibleProducer` / `AsyncCompatibleProducer` shapes):
  - **ckd producer** = `IProducer<byte[],byte[]>`: `Produce(topic, message, deliveryHandler)` (fire-and-forget + delivery-report callback; must call `Poll`/`Flush` to serve callbacks) for the **sync** path via a background poll thread; `ProduceAsync(...) → Task<DeliveryResult<byte[],byte[]>>` for the **async** path. ckd has **no** AIO-style async producer (Python's aio has one); the async v2 backend **wraps `ProduceAsync`** — document this.
  - **ckd consumer** = `IConsumer<byte[],byte[]>`: `Consume(timeout) → ConsumeResult` returns **one** message per call (ckd has no many-at-once batch consume). The v2 consumer backend loops `Consume` up to the batch size to approximate our v3 batch `Poll` — document this adapter difference.
- The **v3 backend keeps our binding's sync-serial vs async-pipelined split (D5)**; the v2 backend keeps ckd's own shapes above. Each backend feeds the **same** shared measured-loop engine so the two are directly comparable.
- The `client_version` field in the consumer `results.json` reflects the actual version run (`"2"` / `"3"`), preserving cross-language plot compatibility.

---

## 2. Deliverables

1. **Client-agnostic shared harness** (`PerformanceCommon` lib) — C# analog of `performance_common.py`: the same `metrics.jsonl` schema, 1 ms histogram, `percentile_from_hist`, warmup/measured/cooldown gating, topic-recreate handling, the measured-loop engine, env parsing, SASL mapping, murmur2, and the `IProducerBackend` / `IConsumerBackend` abstraction. **No client dependency.** CPU/RSS via `System.Diagnostics.Process` (the psutil stand-in).
2. **v3 executable** (references OUR binding + `PerformanceCommon`) — producer + consumer benchmarks over our binding; producer sync = serial blocking / async = pipelined (D5); consumer e2e-latency; Java-form config; the full summaries + `P99_LIMIT_MS` gate; optional `VERIFY_CONSUMED` (+ murmur2).
3. **v2 executable** (references **ckd** + `PerformanceCommon`) — the identical benchmarks over `confluent-kafka-dotnet`: librdkafka-form config, ckd `Produce`+poll-thread (sync) / `ProduceAsync` (async) producer adapter, `Consume`-loop consumer adapter, `partitioner=murmur2_random`. Manual-comparison baseline (not the p99 gate — D10).
4. **In-suite smoke** (xUnit) + a broker + the shared budget config (100 rps / 10 s / p99 ≤ 70 ms / 2048-byte values) → assert exit 0. **Gates v3 only** (D10); v2 runnable manually.
5. **Makefile targets** — `producer-perf-test-dotnet` / `consumer-perf-test-dotnet` (manual, env-driven, `CLIENT_VERSION` selects the exe) + `test-integration-perf-dotnet` (in-suite), mirroring the Python targets, plus wiring the repo-root Makefile.
6. **Env-var contract** — the same env-var names as the Python/Rust suites, for cross-language consistency (§7), including `CLIENT_VERSION`, with the SASL mapping in **Java-form** (v3) / **librdkafka-form** (v2) keys per §1.2.

---

## 3. Byte-exact shared contract (mirror exactly — cross-language comparability)

These are non-negotiable for `metrics.jsonl` compatibility. The Actor MUST reproduce them exactly.

### 3.1 Latency histogram + percentile
- `MAX_LATENCY_MS = 10000`. Histogram = 1 ms buckets `0..10000` plus one overflow bucket → array length `MAX_LATENCY_MS + 2`. A measurement `m` (ms) increments bucket `min(max((int)m, 0), MAX_LATENCY_MS + 1)`.
- `PercentileFromHist(hist, p)`: `total = Σ hist`; if `total == 0` return `0`; `target = p * total`; walk buckets accumulating count, return the **first** bucket index whose cumulative count ≥ `target`.

### 3.2 `metrics.jsonl` schema (one JSON object per line, per 1 s window)
Top-level keys, **all values stringified**:
```
rss, cpu, latency, bytes, messages,
window_start_ms, window_end_ms, measurement_start_ms, measurement_end_ms
```
- Each of `rss` / `cpu` / `latency` / `bytes` / `messages` rolls over to an object `{"average","max","total","count"}`, **each a string**.
- `latency` additionally carries `"p50","p90","p99","p999"` (strings).
- `window_start_ms` / `window_end_ms` = the wall-clock ms bounds of the window. `measurement_start_ms` / `measurement_end_ms` = the measured-interval bounds (a `-inf` sentinel until set — serialize the same textual form Python emits so the plot tooling parses it identically; confirm against `performance_common.py` output during implementation).
- **CPU/RSS averaged over ONLY the measured window:** a window contributes to `total_cpu`/`total_rss`/`total_external_metrics` iff `measurement_start_ms` is set AND `measurement_end_ms` is unset (i.e. currently inside the measured interval) — warmup and cooldown windows are excluded from the averages but still written to the file.

### 3.3 CPU/RSS sampling (psutil analog)
- **RSS** = `Process.WorkingSet64` (bytes), sampled once per rollover (the `MemoryBucket.add_single_measurement` analog). Summary prints "Average RSS (KiB)" = bytes/1024.
- **CPU%** = the `CPUBucket` analog. psutil's `Process.cpu_percent()` returns utilization since the previous call and **can exceed 100 %** on multi-core (per-core sum). The .NET analog samples `Process.TotalProcessorTime` and computes `Δ(TotalProcessorTime) / Δ(wall-clock) * 100` between rollovers — **do NOT divide by `ProcessorCount`** (match psutil's >100 %-capable semantics). First sample has no baseline → emit `0.0` (mirrors psutil's first-call behavior).
- Sampler runs on a background thread/timer at `INTERVAL_SECONDS` (default 1 s), a daemon/background thread so a startup crash still lets the process exit (mirrors `daemon=True`), joined on `StopCollecting()`.

### 3.4 Message shape
Constant prefix + `RANDOMNESS = 0.5` random suffix; **10000 pre-generated messages cycled** round-robin; default value size 2048 B, default key size 0 (no key). Port faithfully.

### 3.5 SASL — Java-form keys ONLY
Enabled iff `SECURITY_PROTOCOL ∈ {SASL_PLAINTEXT, SASL_SSL}` AND mechanism + username + password are all set. When enabled, set `security.protocol`, `sasl.mechanism`, and `sasl.jaas.config` (a `PlainLoginModule` JAAS string) — **NOT** the librdkafka `sasl.username`/`sasl.password` form. Otherwise emit no SASL keys.

### 3.6 Shared in-suite budget (all languages)
`LIMIT_RPS = 100`, `TEST_DURATION_SECONDS = 10`, `WARMUP_SECONDS = 0`, `VALUE_SIZE = 2048`, `P99_LIMIT_MS = 70`; non-zero process exit if the budget is exceeded.

---

## 4. Layout & project structure  **[D4 + D8 — recommendation below]**

The same-assembly collision (§1.1.1) forces the client dimension onto the **executable boundary**: a shared client-agnostic lib + **one exe per client**. `CLIENT_VERSION` picks the exe.

**Recommendation:** put the perf suite under a new `bindings/dotnet/tests/Performance/` directory, **kept OUT of `Confluent.Kafka.sln`** (so `dotnet test`/`verify-dotnet` never drags it into the default gate — exactly how `grpc-server` is excluded, and matching the Rust/Python precedent of isolating perf tests from the functional gate so p99 budgets don't fail under contention). **The two per-client exes must ALSO stay out of any single sln together** — a sln that builds both would recreate the collision in a shared build graph; keep them independent, built by path.

```
bindings/dotnet/tests/Performance/
  PerformanceCommon/               # class-lib, NO client dependency: Metrics, Bucket/LatencyBucket,
                                   #   PercentileFromHist, histogram, CPU/RSS sampler, message generator,
                                   #   Murmur2/Partitioner, env parsing, SASL mapping, metrics.jsonl writer,
                                   #   the measured-loop engine, and the backend abstraction
                                   #   (IProducerBackend / IConsumerBackend).
  PerfV3/                          # console exe — references OUR binding + PerformanceCommon. Adapts our
                                   #   Producer/Consumer to the backend interface. MODE={producer,consumer}.
  PerfV2/                          # console exe — references ckd (Confluent.Kafka 2.x NuGet) + PerformanceCommon.
                                   #   Adapts ckd Produce/ProduceAsync + Consume to the backend interface.
                                   #   MODE={producer,consumer}.
  Confluent.Kafka.PerformanceTests/  # xUnit — in-suite smoke: broker + subprocess-invoke PerfV3 with the
                                   #   budget config (v3-only gate, D10), assert exit 0.
```

- **Exe count (D8) — recommend 2 exes, not 4.** Collapse the producer/consumer dimension into a `MODE` env (`MODE=producer|consumer`) inside each per-client exe, since the client dimension is the one forced onto the exe boundary. `PerfV3` and `PerfV2` each dispatch to the shared producer-benchmark or consumer-benchmark engine in `PerformanceCommon` by `MODE`. This is leaner than 4 exes (producer/consumer × v2/v3) with less duplication, and the client adapter is the only per-exe code. The launcher maps `CLIENT_VERSION`→exe and passes `MODE`.
- **TFM:** target **`net8.0;net10.0`** for `PerformanceCommon`, `PerfV3`, and the xUnit smoke (matches where the unit suite RUNs — `test-dotnet` runs net8.0 + net10.0). `PerfV2`/ckd: confirm ckd 2.x supports both TFMs (ckd ships net8.0 assets; net10.0 resolves via them) — if a TFM is unsupported by the pinned ckd, drop the v2 exe to net8.0 only and note it. net462 is excluded (perf is never run on net462).
- **`Directory.Build.props` inheritance:** these live under `bindings/dotnet/`, so the shared props auto-apply (`#nullable enable`, dotnet/runtime code style, `TreatWarningsAsErrors`, strong-name signing) — same as `grpc-server`. Set `IsPackable=false` and `GenerateDocumentationFile=false`; add a scoped `NoWarn` only for analyzer rules that genuinely clash with console/perf or ckd-interop patterns (e.g. `CA1031` broad-catch, or ckd-obsolete-API warnings under `TreatWarningsAsErrors`), each justified in a comment (the `grpc-server` precedent). **Watch:** ckd's public API may trip style/CA analyzers that our own code does not — the `PerfV2` project may need a slightly wider `NoWarn` than `PerfV3`; keep it scoped and documented.
- **No sln required:** the Makefile invokes the csproj paths directly (`dotnet run --project …`, `dotnet test <csproj>`), like `grpc-server` (Docker-built, no sln). Do NOT add these to any sln that `verify-dotnet` touches, and do NOT put `PerfV2` + `PerfV3` in one sln together.

---

## 5. Producer perf spec

### 5.1 Sync vs async methodology — the key .NET-specific deviation  **[DEVIATION — flag]**
The .NET binding's **sync** `IProducer<TKey,TValue>.Send(record)` **serializes then blocks and returns `RecordMetadata` directly** (= Java `send(record).get()`) — it does **not** return a future. The **async** `IAsyncProducer<TKey,TValue>.Send(record, ct)` returns `Task<RecordMetadata>`. Python's producer test pipelines *both* sync and async through a future + bounded queue + recorder, because `confluent-kafka` returns a future even on the sync path. The .NET sync path has no future, so:

- **Async path** — port the pipelined methodology directly: the send loop fires `Send(record)` (returns `Task<RecordMetadata>`), pushes `(task, startMs)` onto a **bounded queue** (`maxsize = 2 GiB / messageSize`; a blocking put is the backpressure); a **separate recorder task** awaits each task in order, computes `latencyMs = nowMs − startMs`, verifies the returned `RecordMetadata`, and feeds `metrics.latency`, `metrics.messages(1)`, `metrics.bytes(messageSize)` plus the cumulative module histogram. Use `System.Threading.Channels` (bounded) for the queue.
- **Sync path** — a **serial blocking measurement**: `startMs = now; meta = Send(record); latencyMs = now − startMs; record(...)`. No pipelining (there is no future to pipeline). This is a faithful reflection of what the .NET sync API actually does (and is itself Java-faithful: sync send = blocking `get()`). **Do NOT fake pipelining with `Task.Run`** — that would measure threadpool overhead, not the API. State this deviation in the phase self-review.

Common to both: `acks` **hardcoded to `all`** (Python parity; the binding default is also `all`, and the Rust reference relies on that default — we pick the explicit Python form and note it). Warmup sends are awaited/measured **inline and never fed to the recorder/histogram**; `verified` is reset to 0 after warmup. `measurement_start_ms` set at the measured-interval start; `measurement_end_ms` set when writing the summary. Cooldown = `GC.Collect()` + sleep `POST_TEST_AWAIT_SECONDS` (const 10 s) with the collector still writing (those windows excluded from averages). Duration guard checked every 10000 messages.

### 5.2 Env vars (name / default)
| Env var | Default | Maps to |
|---|---|---|
| `TOPIC_NAME` | `test-topic` | topic |
| `KEY_SIZE` | `0` | key bytes (0 = no key) |
| `VALUE_SIZE` | `2048` | value bytes |
| `NUM_MESSAGES` | `0` | total messages (0 = duration-bounded) |
| `TEST_DURATION_SECONDS` | `600` | measured duration |
| `WARMUP_SECONDS` | `120` | warmup duration |
| `LIMIT_RPS` | *(unset)* | if set, `num_messages = limit_rps × duration` (rate limiter) |
| `P99_LIMIT_MS` | `0` (disabled) | budget → exit 1 if exceeded |
| `CREATE_TOPIC` | `True` **→ see §8 deviation (default `False` for .NET)** | topic (re)creation |
| `PARTITIONS` | `-1` | partition count for create |
| `ASYNC` | `False` | select async vs sync path |
| `DO_VERIFY` | `True` | verify `RecordMetadata` in recorder |
| `VERIFY_CONSUMED` | `False` | post-run consume-all correctness (§5.4) |
| `USE_DEFAULTS` | `False` | if true, skip all producer-config vars below |
| `POST_TEST_AWAIT_SECONDS` | `10` (const) | cooldown |

Producer-config vars (skipped entirely if `USE_DEFAULTS`): `acks` hardcoded `all`; `BATCH_SIZE` KiB × 1024 (default 1 MiB) → `batch.size`; `MAX_REQUEST_SIZE` KiB × 1024 else `min(batch × 64, 8 MiB)` → `max.request.size`; `COMPRESSION_TYPE` default `none`; `ENABLE_IDEMPOTENCE` default `false`; `MAX_IN_FLIGHT` → `max.in.flight.requests.per.connection`; `BUFFER_MEMORY` MiB × 1024 × 1024 → `buffer.memory`; `LINGER_MS` default `5`.

### 5.3 Summary (stdout — **no `results.json` for the producer**)
Print: End time, Duration, Average CPU %, Average RSS (KiB), CPU Efficiency (msg/(s·1 %CPU)), Memory Efficiency (msg/(s·KB RSS)), Average time, Average rate msg/s, Average rate MiB/s, Average latency, Max latency, p50/p90/p99/p999. If `P99_LIMIT_MS > 0` and measured p99 > budget → **exit 1**.

### 5.4 `VERIFY_CONSUMED` (optional flag)  **[FLAG — recommend IN SCOPE for P1]**
Capture baseline end-offsets pre-produce; post-run consume-all from the baseline; assert `count == warmup_sent + measured_sent`; and (if keyed) assert each record landed in `murmur2(key) & 0x7FFFFFFF % num_partitions`.
- **Feasibility (verified):** the .NET consumer exposes `EndOffsets(...)` and `Position(...)`, and the producer exposes `PartitionsFor(topic)` — so the baseline capture, consume-all count, and partition-count for the murmur2 check are all achievable with shipped APIs. Port `partitioner.py` → a C# `Murmur2` / `PartitionForKey` (seed `0x9747B28C`, M `0x5BD1E995`, R `24`, 32-bit masked, little-endian 4-byte chunks) with the `UtilsTest.testMurmur2` vectors as an xUnit self-check.
- **Recommendation:** include in P1 — it is off by default (`VERIFY_CONSUMED=False`, and default `KEY_SIZE=0` means the keyed check is inert), the required APIs exist, and the murmur2 port is a small self-contained unit with known test vectors, giving real cross-language parity. It is the **first scope-trim candidate** to a P2 follow-up if the loop runs long.

### 5.5 In-suite invocation
Subprocess re-invoke **`PerfV3` with `MODE=producer`** (v3-only gate — D10) and: `ASYNC=False`, `WARMUP_SECONDS=0`, `TEST_DURATION_SECONDS=10`, `LIMIT_RPS=100`, `VALUE_SIZE=2048`, `P99_LIMIT_MS=70`, `DO_VERIFY=False`, `CREATE_TOPIC=False`; assert process exit 0. Add an async case (`ASYNC=True`). The v2 (`PerfV2`) producer path is **not** gated — it runs on demand for manual comparison.

---

## 6. Consumer perf spec

### 6.1 Methodology
Assignment wait (poll-drain until assigned; abort after `JOIN_TIMEOUT_SECONDS`) → **settle to live edge** (poll until 2 consecutive empty polls, or `SETTLE_TIMEOUT_SECONDS`) → measure. Per-record **e2e latency = `nowMs − record.Timestamp`** (`ConsumerRecord.Timestamp` is `long`; `-1` = no timestamp), recorded only if `timestamp > 0 && latency >= 0`; `nbytes = value.Length + key.Length`. Warmup gated by elapsed time since the first record. Done when `NUM_MESSAGES` reached; time-limit at `TEST_DURATION_SECONDS`; no-data-timeout 120 s. **Sync (`IConsumer.Poll`) + async (`IAsyncConsumer.Poll`)** funnel every record through one shared measurement path.
- **`POLL_SINGLE`:** the binding has no single-message poll API (only the batch `Poll(TimeSpan) → ConsumerRecords`), so `POLL_SINGLE=True` **delegates to batch poll** — a faithful mirror of the Python note (its binding is the same shape). Keep the flag for env parity; document the delegation.

### 6.2 Env vars (name / default)
| Env var | Default | Notes |
|---|---|---|
| `BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `TOPIC_NAME` | `test-topic` | |
| `GROUP_ID` | `benchmark-<ver>-<epoch>` | |
| `WARMUP_SECONDS` | `120` | |
| `TEST_DURATION_SECONDS` | `600` | |
| `INTERVAL_SECONDS` | `1` | sampler tick |
| `POLL_TIMEOUT_MS` | `1000` | |
| `VALUE_SIZE` | `2048` | |
| `THROUGHPUT` | `125000` | load generation |
| `NUM_MESSAGES` | `0` | |
| `PARTITIONS` | `-1` | |
| `CREATE_TOPIC` | `True` **→ see §8 (default `False` for .NET)** | |
| `P99_LIMIT_MS` | `0` | budget |
| `JOIN_TIMEOUT_SECONDS` | `120` | assignment abort |
| `SETTLE_TIMEOUT_SECONDS` | `15` | settle abort |
| `KAFKA_BIN` | *(unset)* | in-container load driver dir |
| `FETCH_MIN_BYTES` | 4 MiB | `fetch.min.bytes` |
| `MAX_PARTITION_FETCH_BYTES` | 4 MiB | `max.partition.fetch.bytes` |
| `CONSUMER_BATCH_SIZE` | `2000` | drives `max.poll.records = batch + 500` |
| `ASYNC` | `False` | sync vs async |
| `POLL_SINGLE` | `False` | delegates to batch poll |
| `USE_DEFAULTS` | `False` | skip explicit consumer config |

Consumer config (our binding): `group.protocol=consumer`, `auto.offset.reset=latest`, `enable.auto.commit=true`, `client.id`, and (unless `USE_DEFAULTS`) `fetch.min.bytes`, `max.partition.fetch.bytes`, `max.poll.records = batch_size + 500`, `check.crcs=false`.

### 6.3 `results.json` schema (also printed)
```
{ client_version, topic, messages_measured,
  duration_s (round2), throughput_msg_s (round2), throughput_mib_s (round2),
  latency_ms: { min, avg (round2), p50, p90, p95, p99, p999, max } }
```
Note the consumer computes **p95** (the producer does not). `min` = first non-empty bucket, `max` = last non-empty bucket, `avg = Σ(ms × count) / total`. If `P99_LIMIT_MS > 0` and p99 > budget, OR `messages_measured <= 0` → **exit 1**.

### 6.4 Consumer load
The consumer test needs **CreateTime-timestamped** records on the topic. Python uses `kafka-producer-perf-test.sh` (in-container, via `KAFKA_BIN`) or an in-container producer. For the .NET in-suite smoke, feed the topic with a ~100 msg/s in-container producer (broker-exec `kafka-producer-perf-test.sh`, mirroring conftest's `produce_perf_in_container`) — this depends on the in-suite broker decision (§4/§8 FLAG). For manual runs, the user provides a populated topic or sets `KAFKA_BIN`.

### 6.5 In-suite invocation (`_smoke_env`)
Subprocess re-invoke **`PerfV3` with `MODE=consumer`** (v3-only gate — D10) and: `WARMUP_SECONDS=0`, `TEST_DURATION_SECONDS=10`, `INTERVAL_SECONDS=1`, `POLL_TIMEOUT_MS=500`, `VALUE_SIZE=2048`, **`FETCH_MIN_BYTES=1`** (so low-rate fetches return immediately), `P99_LIMIT_MS=70`, `JOIN_TIMEOUT_SECONDS=60`, `SETTLE_TIMEOUT_SECONDS=5`, `CREATE_TOPIC=False`, `KAFKA_BIN` removed. Run both sync and async cases; assert exit 0. A ~100 msg/s in-container producer (broker-exec `kafka-producer-perf-test.sh`) feeds the topic with CreateTime-stamped records. The v2 (`PerfV2`) consumer path is not gated — manual comparison only.

---

## 7. Env-var contract (shared, cross-language)

Reuse the **same env-var names** as the Python/Rust suites so a single config drives all languages: `CLIENT_VERSION`, `MODE`, `BOOTSTRAP_SERVERS`, `TOPIC_NAME`, `KEY_SIZE`, `VALUE_SIZE`, `NUM_MESSAGES`, `TEST_DURATION_SECONDS`, `WARMUP_SECONDS`, `LIMIT_RPS`, `BATCH_SIZE`, `LINGER_MS`, `COMPRESSION_TYPE`, `ENABLE_IDEMPOTENCE`, `BUFFER_MEMORY`, `MAX_IN_FLIGHT`, `P99_LIMIT_MS`, `CREATE_TOPIC`, `PARTITIONS`, `ASYNC`, `POLL_SINGLE`, and `SASL_*` (`SECURITY_PROTOCOL`, `SASL_MECHANISM`, `SASL_USERNAME`, `SASL_PASSWORD`).
- **`CLIENT_VERSION`** = the cross-language client selector: `3` = our binding (→ `PerfV3` exe), `2` = ckd (→ `PerfV2` exe). The launcher (Makefile / in-suite) maps it to the executable (§1.1.1) — a per-client exe cannot dynamically load the other client (the collision), so this is a launch-time dispatch, not an in-process branch. `MODE` = `producer` | `consumer` (§4, D8).
- **SASL mapping is version-dependent (§1.2):** v3 emits **Java-form** keys (`security.protocol` + `sasl.mechanism` + `sasl.jaas.config` PlainLoginModule); v2/ckd emits **librdkafka-form** keys (`security.protocol` + `sasl.mechanism` + `sasl.username` / `sasl.password`). Same env-var inputs, different config output per client — the `PerformanceCommon` SASL mapper takes the target form as a parameter.

---

## 8. Broker & topic provisioning  **[FLAG × 2]**

### 8.1 No .NET AdminClient — CREATE_TOPIC has no in-harness analog  **[DEVIATION]**
Python's `recreate_topic` / `create_topics` uses `confluent-kafka`'s `AdminClient` (librdkafka). The .NET binding **ships no AdminClient** (verified — no admin surface under `src/Confluent.Kafka/`). Therefore:
- The `CREATE_TOPIC` path cannot be honored from within the .NET perf harness. **Default `CREATE_TOPIC=False` for the .NET suite** (a deliberate deviation from Python's `True`), and topic provisioning is **external**: the in-suite smoke creates the topic via broker-exec `kafka-topics.sh` (mirroring conftest's `create_topic`), and manual runs require a user-provided topic. This matches what the Python in-suite already does (it passes `CREATE_TOPIC=False` and relies on conftest for topic creation).

### 8.2 In-suite broker source  **[FLAG — recommendation]**
The .NET binding has **no existing C# testcontainers usage** (verified); the gRPC harness broker is spun by the *Rust* side. The in-suite smoke needs a broker. Options:
- **Option A — add the `Testcontainers` .NET NuGet** (a new *test-only* dependency, the direct analog of Python's `testcontainers`). Enables a self-contained `test-integration-perf-dotnet` that spins `apache/kafka:4.2.0` KRaft with KIP-848 (`KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer`), EXTERNAL+INTERNAL listeners, skipping cleanly when Docker is unavailable — a faithful mirror of `conftest.py`.
- **Option B — manual-only-first**: ship the console apps env-driven against a user-provided `BOOTSTRAP_SERVERS`; defer the in-suite xUnit smoke to a later phase. No new dependency, no Docker; loses the automated p99 gate and cross-language parity of the in-suite smoke.

**Recommendation: Option A, sequenced as two sub-slices within P1** so the loop can close cleanly regardless of local Docker:
- **Slice 1 (no Docker):** the shared harness + both console apps (manual, env-driven) + the murmur2 self-check unit — fully build/lint/test-verifiable **locally** with no broker.
- **Slice 2 (Docker-gated):** the xUnit in-suite smoke using `Testcontainers`, Docker-gated and **skipping cleanly without Docker**. Per the binding's established pattern (M11/M12 harness phases), the Docker conformance gate is **CI-only in this local env** — so the loop closes CLEAN on Slice 1 with Slice 2's Docker gate **PENDING-on-CI**, exactly as M12/P1's Docker conformance gate did. Flag this explicitly at close-out.

This preserves full Python parity (standalone script + in-suite subprocess) while keeping the phase completable locally. **Confirm Option A + the new `Testcontainers` test-only dependency with the user** (CLAUDE.md translation-rule 1.2: a new crate/package dependency needs approval; the .NET analog applies).

---

## 9. Makefile targets (deliverable #5)

Mirror the Python targets. In `bindings/dotnet/Makefile`, the target maps `CLIENT_VERSION`→exe and passes `MODE` (default `CLIENT_VERSION=3` = our binding; set `CLIENT_VERSION=2` for the ckd baseline):
- `producer-perf-test-dotnet` — runs `PerfV3` (or `PerfV2` when `CLIENT_VERSION=2`) with `MODE=producer` via `dotnet run -c Release --project tests/Performance/PerfV3` (env-driven; two-stage native build prerequisite per CLAUDE.md §7.1 for the v3 exe, which P/Invokes our native — the v2/ckd exe needs no native build).
- `consumer-perf-test-dotnet` — same, `MODE=consumer`.
- `test-integration-perf-dotnet` — `dotnet test … tests/Performance/Confluent.Kafka.PerformanceTests` (the in-suite smoke; Docker-gated; **v3-only gate**, D10).

Because `PerfV2` and `PerfV3` are separate projects (never one sln — §4), the target selects a project path by `CLIENT_VERSION`; there is no runtime client switch inside one process.

In the repo-root `Makefile`, add the `-dotnet` variants alongside the existing `producer-perf-test-python` / `consumer-perf-test-python` / `test-integration-perf-python`, delegating into `bindings/dotnet`. **Do NOT add the perf stage to `verify-dotnet`** — `verify-dotnet` deliberately has no perf stage (the perf suite is isolated from the functional gate, mirroring `verify-python`'s separation and the Rust separate-target design). The perf suite is invoked only by the explicit `test-integration-perf-dotnet` / `*-perf-test-dotnet` targets.

---

## 10. Definition of Done (adapted for a test-harness phase)

1. **CLAUDE.md + rules:** consistent with `bindings/dotnet/CLAUDE.md`, `ffi-marshalling.md`, and the general rules. Mode-A boundary honored (no `src/**` / ffi / header / cbindgen changes). The same-assembly collision (§1.1.1) is respected — `PerfV2` and `PerfV3` are never in one project/sln; the shared lib has no client dependency.
2. **Faithful mirror (both clients):** the `metrics.jsonl` schema, histogram, `PercentileFromHist`, warmup/measured/cooldown gating, message shape, SASL mapping, and both summaries match the Python reference byte-for-byte (per §3/§5/§6), for **both** v3 and v2. The Python v2/v3 config-form + partitioner + adapter branching (§1.2) is reproduced. Every deliberate deviation (per-client-exe split forced by the collision; sync serial vs async pipelined on v3; ckd `ProduceAsync` async v2 / `Consume`-loop batch on v2; no AdminClient → CREATE_TOPIC default False; Testcontainers CI-only smoke; v3-only gate) is stated in the phase self-review with its rationale.
3. **Tests translated:** the murmur2 self-check (`UtilsTest.testMurmur2` vectors) as an xUnit test; the in-suite **v3** producer + consumer smokes (sync + async) asserting exit 0 under the budget config (Slice 2, Docker-gated/CI-only). No Python perf test is silently dropped — the v2 arm is **implemented** (not skipped) but not gated (D10); any skip is explained.
4. **Build/lint/format:** `dotnet build`, `dotnet format --verify-no-changes`, and `TreatWarningsAsErrors`/analyzers all pass on every perf project (net8.0 + net10.0), including `PerfV2` — a wider scoped `NoWarn` for ckd-API analyzer clashes is acceptable if documented (§4). rustfmt/clippy N/A (no Rust delta).
5. **Slice split (D11) — Slice-1 local gate CLEAN:** `PerformanceCommon` + `PerfV3` (our binding) + the murmur2 unit build and pass locally with no broker. Slice-2 (`PerfV2`/ckd exe + Docker xUnit smoke) PENDING-on-CI is an accepted close-out state (flagged), not a defect.
6. **No duplicated/invented surface:** the harness mirrors `performance_common.py` structure; no C# types invented beyond the Python reference's shapes (Metrics/Bucket/LatencyBucket/Partitioner + config/env helpers + the two backend adapters). DoD hot-path allocation audit (repo DoD #10): N/A — perf harness is not on the shipped send/receive path; state explicitly.
7. **No TODO/FIXME.** Completeness per CLAUDE.md §5.
8. **New dependency approved:** the ckd `Confluent.Kafka` 2.x NuGet (D9) and the `Testcontainers` NuGet (D2) are approved test-only dependencies before they are added (CLAUDE.md translation-rule 1.2 analog); the exact pinned versions are recorded in the phase self-review.

---

## 11. Execution-loop notes (for the eventual N=35 loop — NOT this draft)

- **Actor N=35** implements per the approved plan; **Critic N=35** reviews via `cargo xtask await-commit` / commit-by-commit, writing `COMMENTS.35.md`; resolved items move to `COMMENTS.DONE.35.md` (file-locked). Run the Critic after every Actor commit cycle until no approved issues remain.
- **Subagents:** use `dotnet-actor` / `dotnet-critic`. They report to `main` (not to a PM name); main relays. Independently verify their git/build claims — do not take green build/Critic-clean on faith (binding-phase parity pre-flight).
- **Git discipline:** per-path `git add` only. NEVER stage `.claude/agents/dotnet-*.md`, `COMMENTS.*`, `agent-memory/**`, `target-linux*`, or build artifacts (`bin/`, `obj/`). Commits `--no-gpg-sign`, trailer `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. **No push.**
- **Mode-A guard:** if any step appears to need a new ABI function / header change / core `src/**` edit, STOP and flag to main — do not invent an FFI change.
- **Close-out (after the loop, a separate step — not part of planning):** update `design/current/STATUS.md`; archive this PLAN + `COMMENTS.DONE.35.md` to `design/history/M13/P1-dotnet-perf-suite/`; reset `COMMENTS.35.md`. Flag Slice-2's Docker gate as CI-PENDING.

---

## 12. Decisions summary (recommendations + flags for the user)

| # | Decision | Status | Recommendation |
|---|---|---|---|
| D1 | v2-vs-v3 split (v3 = our binding, v2 = ckd/librdkafka) IN scope | **SETTLED (user, REVERSED 2026-08-18)** | In scope. `CLIENT_VERSION` selects the client; forced into a per-client-exe split by the same-assembly collision (§1.1.1, D8). `client_version` in `results.json` = the actual version run. |
| D2 | In-suite broker source | **FLAG** | **Option A** (`Testcontainers` .NET NuGet, test-only), two-slice: Slice 1 no-Docker (harness + v3 exe) closes clean locally; Slice 2 Docker smoke CI-only. Needs user OK on the new dependency. |
| D3 | `VERIFY_CONSUMED` + murmur2 partition check | **FLAG** | **IN SCOPE for P1** — feasible (`EndOffsets`/`PartitionsFor` exist), off-by-default, murmur2 self-checked (shared by both clients; ckd uses `partitioner=murmur2_random`); first scope-trim candidate if the loop runs long. |
| D4 | Layout / TFM | **FLAG** | `tests/Performance/` (`PerformanceCommon` lib + `PerfV3` + `PerfV2` exes + 1 xUnit smoke), **out of `Confluent.Kafka.sln`** and never both exes in one sln, TFM `net8.0;net10.0`. |
| D5 | Sync + async both (v3) | Confirmed feasible | v3 async = pipelined (Task future); v3 **sync = serial blocking** (no future on `IProducer.Send`) — deviation, stated. v2 keeps ckd shapes (D-new in §1.2). |
| D6 | No .NET AdminClient → CREATE_TOPIC | **DEVIATION** | Default `CREATE_TOPIC=False`; topic provisioning external / in-suite broker-exec `kafka-topics.sh`. (ckd *does* have an AdminClient, but we keep one uniform provisioning path across both clients — don't special-case v2.) |
| D7 | `acks` | Decision | Hardcode `acks=all` (Python parity; = binding default; ckd default is also `all` but set it explicitly). |
| **D8** | **Project structure for the collision** | **FLAG (NEW)** | **Shared `PerformanceCommon` lib (no client dep) + separate per-client exes; `CLIENT_VERSION` picks the exe.** Recommend **2 exes** (`PerfV3`, `PerfV2`), each dispatching producer/consumer by a `MODE` env — leaner than 4 (producer/consumer × v2/v3). |
| **D9** | **ckd version to pin** | **FLAG (NEW)** | Pin the **latest stable `Confluent.Kafka` 2.x** NuGet (exact version resolved at implementation time, recorded in the self-review). Confirm the pin. |
| **D10** | **In-suite gate scope** | **FLAG (NEW)** | **Gate v3 only** (mirrors Python: the in-suite p99 budget runs `CLIENT_VERSION=3`; v2 is manual-comparison-only). `PerfV2` is implemented + runnable, not part of the Docker smoke gate. |
| **D11** | **Slice split** | **FLAG (NEW)** | **Slice 1** = `PerformanceCommon` + `PerfV3` (our binding, no Docker, closes clean locally); **Slice 2** = `PerfV2`/ckd exe + the Docker xUnit smoke (CI-only). Confirm. |

---

## 13. Approval checklist (please confirm before the loop starts)

- [ ] **N = 35** and the **M13/P1** label are correct.
- [ ] **D1** — v2-vs-v3 split IN scope (reversal confirmed); `CLIENT_VERSION` selects the client.
- [ ] **D8** — the **collision structure**: shared `PerformanceCommon` lib + separate `PerfV3`/`PerfV2` exes, `CLIENT_VERSION`→exe, **2 exes** with a `MODE` switch (recommended) vs 4 exes?
- [ ] **D9** — pin the **latest stable `Confluent.Kafka` 2.x** ckd NuGet (recommended)? Any specific version to pin?
- [ ] **D2** — approve **Testcontainers .NET NuGet** as a new test-only dependency (Option A, two-slice, Docker gate CI-only)? Or prefer Option B (manual-only-first, defer in-suite smoke)?
- [ ] **D10** — in-suite Docker smoke gates **v3 only** (recommended), v2 manual-comparison-only?
- [ ] **D11** — Slice 1 = `PerformanceCommon` + `PerfV3` (closes clean locally); Slice 2 = `PerfV2` + Docker smoke (CI-only): OK?
- [ ] **D3** — `VERIFY_CONSUMED` + murmur2 IN SCOPE for P1 (recommended) or deferred to P2?
- [ ] **D4** — layout `tests/Performance/`, out-of-sln (and never both exes in one sln), TFM `net8.0;net10.0`: OK? (or net8.0-only?)
- [ ] **D5/D6/D7** — v3 sync-serial vs async-pipelined fork (+ ckd adapter shapes for v2), `CREATE_TOPIC=False` default, `acks=all`: OK?
- [ ] **Mode A** confirmed — no `src/**`/ffi/header/cbindgen changes expected.
