# Plan — Transactional Producer Performance Harnesses (Rust / librdkafka-C / Java)

**Status:** DRAFT — awaiting approval. Nothing has been spawned or committed.
**Branch:** `perf-harness-txn-producer` (created off `origin/perf-harness-fixes` @ `72842f95`).
**PR target:** `perf-harness-fixes` (NOT master). Commit messages carry **no** AI co-author trailer.
**Agent number for this work:** **N = 70** (0,1,3,4,41–49,52,62–64 already used).

---

## 1. Goal

Add three NEW, standalone performance harnesses that mirror the existing
non-transactional producer perf harnesses but drive the transactional producer
API. Same env-config contract, same `metrics.jsonl` + `results.json` schema
(plus transaction-specific additions), same warmup / measured / cooldown
structure, same CPU/RSS + latency-histogram machinery.

| Client              | Existing (mirror)                                                               | New file                                                                               |
|---------------------|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| Confluent Rust      | `tests/performance/producer_perf_test.rs`                                      | `tests/performance/transactional_producer_perf_test.rs`                                |
| librdkafka (C)      | `bindings/c/tests/producer_perf_test.c` (v2 path)                              | `bindings/c/tests/transactional_producer_perf_test.c`                                  |
| Java                | `tools/java-perf-test/.../ProducerPerformanceTest.java`                         | `tools/java-perf-test/.../TransactionalProducerPerformanceTest.java`                   |

**Deliverable:** three *compiling* harnesses + a PR. NO live-broker perf runs
(these are opt-in benchmarks, like the existing ones).

---

## 2. Verified facts (grounded on `origin/perf-harness-fixes`)

- Rust `KafkaProducer` / `Producer` trait expose: `init_transactions().await`,
  `begin_transaction()`, `send_offsets_to_transaction(offsets, group_metadata).await`,
  `commit_transaction().await`, `abort_transaction().await`; config key
  `transactional.id` (`producer_config.rs:338`). Confirmed present.
- The base harnesses are the **richer** perf-harness-fixes versions:
  delivery-callback latency stamping, 100 ms `rate_checkpoint` pacing,
  `results.json` output. (The milestone-12 worktree HEAD this session started on
  was an *older* copy — corrected by rebasing the working branch onto the
  mandated base.)
- **C-FFI has NO transactional functions** (`src/ffi/producer.rs` on base has no
  `init_transactions`/`begin_transaction`/…). Therefore the C harness's v3 (Rust
  C-FFI) backend CANNOT do transactions. See scope decision D1.
- Build wiring on the base is exactly as mapped: xtask arm at
  `xtask/src/main.rs:33`, `mod producer_perf_test;` in `tests/performance/main.rs`,
  `add_executable(producer_perf_test …)` under `if(RDKAFKA_FOUND)` in
  `bindings/c/CMakeLists.txt:101`, single `mainClass` shadowJar in
  `tools/java-perf-test/build.gradle:30`.
- `tools/performance_metrics_plot/plot_metrics.py` consumes only `metrics.jsonl`
  and only these fields: `window_end_ms`, `rss.max`, `cpu.max`,
  `latency.{average,max,p50,p90,p99,p999}`, `bytes.total`, `messages.total`,
  `measurement_{start,end}_ms`. It ignores unknown fields — so added
  transaction fields are safe.

---

## 3. Scope decisions (Critic must validate these)

- **D1 — C harness = librdkafka (v2) only.** The Rust C-FFI (v3) has no
  transactional entry points, and adding them is out of scope (bindings are a
  deferred future task). The transactional C harness therefore defaults to and
  supports only `CLIENT_VERSION=2` (librdkafka), and if `CLIENT_VERSION=3` is
  requested it prints a clear message and exits non-zero (does NOT silently run
  a non-transactional path). This is faithful to the task's "three clients:
  Rust, librdkafka (C), Java" framing — the C harness *is* the librdkafka
  client. `client` id in `results.json` = `librdkafka-txn`.
- **D2 — Two phases.** Phase 1 = `TXN_MODE=produce` (the core), fully landed and
  reviewed clean first. Phase 2 = `TXN_MODE=eos` (consume→transform→produce→
  `send_offsets_to_transaction`), added only after Phase 1 is clean. Kept in the
  same PR if time permits, else noted as a follow-up. Phase 2 gets its own
  detailed plan after Phase 1 lands.
- **D3 — Java second entry point.** The `application`/`shadowJar` plugin supports
  one `mainClass`. Recommended: keep the single fat jar (it already bundles the
  new class) and add a dedicated runnable artifact/task for the txn class OR
  document/invoke via
  `java -cp build/libs/java-perf-test-all.jar io.confluent.kafka.perftest.TransactionalProducerPerformanceTest`.
  Actor's default: add a second Gradle `Jar`/run task so the harness is
  discoverable the same way as the existing one, without breaking the existing
  `mainClass`. Final mechanism is an Actor implementation detail; DoD is "gradle
  build produces a runnable artifact for the txn class."
- **D4 — Deploy-script wiring is out of scope.** Building (cargo/cmake/gradle) is
  the DoD. Wiring these into `deploy_and_run_perf` / Makefile perf targets is a
  follow-up, noted in the PR description.

---

## 4. Shared design (identical across all three languages)

### 4.1 New env config knobs
| Variable                     | Default    | Meaning                                                                 |
|------------------------------|------------|-------------------------------------------------------------------------|
| `RECORDS_PER_TRANSACTION`    | `100`      | Records produced per transaction before commit/abort                    |
| `ABORT_RATE`                 | `0`        | Fraction [0.0,1.0] of transactions aborted (deterministic, even spread) |
| `NUM_TRANSACTIONAL_PRODUCERS`| `1`        | Concurrent producers (tasks in Rust, threads in C/Java)                 |
| `TRANSACTIONAL_ID`           | `perf-txn` | Base id; producer *k* uses `<TRANSACTIONAL_ID>-<k>`                      |
| `TXN_MODE`                   | `produce`  | `produce` (Phase 1) or `eos` (Phase 2)                                  |
| `SOURCE_TOPIC`               | (unset)    | EOS mode only (Phase 2): topic to consume/transform                     |

Everything else (`VALUE_SIZE`, `KEY_SIZE`, `BATCH_SIZE`, `LINGER_MS`,
`COMPRESSION_TYPE`, `WARMUP_SECONDS`, `TEST_DURATION_SECONDS`, `LIMIT_RPS`,
`NUM_MESSAGES`, `P99_LIMIT_MS`, `DO_VERIFY`, SASL/SSL, `METRICS_FILE`,
`RESULTS_FILE`, `TOPIC_NAME`, …) is **identical** to the non-transactional
harness. `enable.idempotence` is **forced true** (required for transactions;
librdkafka auto-enables it when `transactional.id` is set, but the Rust/Java
harnesses set it explicitly).

### 4.2 Produce-mode loop (per producer task/thread)
```
init_transactions()                    // once, after producer creation
loop until duration / NUM_MESSAGES reached:
    begin_transaction()
    for r in 0..RECORDS_PER_TRANSACTION:
        produce(record)                // capture per-record produce timestamp
    if should_abort(txn_index):
        abort_transaction()            // records NOT counted as committed
        aborted_transactions += 1; aborted_records += N
    else:
        commit_transaction()           // atomic; on return all N are durable
        committed_transactions += 1
        commit_completion = now()
        for each of the N records: per-record latency = commit_completion - produce_ts[r]
        commit latency = commit_completion - begin_ts
    txn_index += 1
```

### 4.3 Deterministic abort selection (identical formula, all languages)
Per-producer 0-based transaction index `i`; abort iff
`floor((i+1) * ABORT_RATE) > floor(i * ABORT_RATE)` (Bresenham even spread).
- `ABORT_RATE=0` → never abort.
- `ABORT_RATE=0.5` → abort `i = 1,3,5,…` (exactly half, evenly spread).
- `ABORT_RATE=1.0` → abort every transaction.
Each of the `NUM_TRANSACTIONAL_PRODUCERS` producers applies this to its own `i`,
so every producer hits `ABORT_RATE` exactly. Aborted transactions STILL produce
their `N` records before calling abort (per task requirement).

### 4.4 Latency definitions (mandated)
- **Per-record latency** = record's `produce()` call → the moment its transaction
  **commit completes**. Committed transactions only. Feeds the existing
  per-record `latency` histogram / percentiles / `metrics.jsonl` `latency`
  bucket, unchanged in shape.
- **Per-transaction commit latency** = `begin_transaction` → `commit_transaction`
  completes. Committed only. New `commit_latency` histogram + `commit_latency_ms`
  percentiles in `results.json`, and a `commit_latency` bucket per `metrics.jsonl`
  window.
- Aborted transactions contribute to neither latency series.

### 4.5 Throughput & counters
- `throughput_msg_s` = **committed** records / measured seconds.
- `throughput_mib_s` = **committed** bytes / measured seconds.
- `transactions_per_s` = committed transactions / measured seconds.
- Aborted counts reported separately: `aborted_transactions`, `aborted_records`.
- Concurrency: all producers share one atomic `Metrics` + histograms; metrics
  are aggregated across producers. Each producer owns its own client with a
  unique `transactional.id`.

### 4.6 Metrics schema additions (backward compatible)
`results.json` adds: `committed_transactions`, `aborted_transactions`,
`aborted_records`, `transactions_per_s`, `commit_latency_ms`
`{min,avg,p50,p90,p95,p99,p999,max}`. Existing keys keep their meaning
(`messages_measured` = committed records; `latency_ms` = per-record latency).
`client` = `rust-txn` / `librdkafka-txn` / `java-txn`.

`metrics.jsonl` per-window adds a `transactions` bucket (`{average,max,total,count}`
of committed txns) and a `commit_latency` object (`{average,max,total,count,p50,
p90,p99,p999}`), mirroring the existing bucket shape. All existing fields keep
their exact shape so `plot_metrics.py` is unaffected.

---

## 5. Per-file implementation

### 5.1 Rust — `tests/performance/transactional_producer_perf_test.rs`
- Copy the structure of `producer_perf_test.rs`: `PerfTestConfig` (extended),
  `Metrics`/`MetricsSnapshot`, `ProcSampler`, `message_generator`,
  `CumulativeStats`, `rollover_line`, percentile helpers — reused where possible,
  extended for the txn counters/`commit_latency`.
- `#[tokio::test(flavor = "multi_thread")] async fn transactional_producer_perf_test()`.
- `NUM_TRANSACTIONAL_PRODUCERS` producers via `tokio::spawn`, each its own
  `KafkaProducer` with `transactional.id = <base>-<k>`, `enable.idempotence=true`.
  `init_transactions().await` per producer before the measured interval.
- Within a txn: send `N` records (collect futures / use delivery callbacks),
  then `commit_transaction().await` (or `abort_transaction().await`). Per-record
  latency stamped at commit completion. Commit latency = elapsed since begin.
  Follow CLAUDE.md §9.6 (no `MutexGuard` across `.await`) and §11 (no per-record
  `tokio::spawn`; reuse the shared-aggregation pattern).
- In-suite short-run defaults mirror the base (short duration, rate-limited,
  p99 budget) so it can live in the `performance` test binary. With `ABORT_RATE`
  set, the p99/verify assertions apply to committed records only.
- Register: add `mod transactional_producer_perf_test;` to
  `tests/performance/main.rs`.
- xtask: add `Some("transactional-producer-perf-test") => …` arm + fn (copy of
  `producer_perf_test()` with `--exact transactional_producer_perf_test::transactional_producer_perf_test`)
  + `print_help()` entry.

### 5.2 C — `bindings/c/tests/transactional_producer_perf_test.c`
- Mirror `producer_perf_test.c`'s **v2 (librdkafka)** path only (D1). Reuse the
  metrics thread, `ProcSampler`, message generation, rollover schema, rate
  limiter, summary + `results.json` writer.
- Transactional librdkafka calls with `rd_kafka_error_t` handling:
  `rd_kafka_init_transactions(rk, timeout)` once; per loop
  `rd_kafka_begin_transaction`, `rd_kafka_producev` × N,
  `rd_kafka_commit_transaction`/`rd_kafka_abort_transaction`. Per-record
  produce ts captured at `producev`; per-record latency uses commit-completion
  time per §4.4. Config sets `transactional.id`, `enable.idempotence=true`,
  `acks=all`.
- Concurrency: `NUM_TRANSACTIONAL_PRODUCERS` pthreads, each its own `rd_kafka_t`
  with a unique `transactional.id`; shared atomic counters/histograms.
- If `CLIENT_VERSION=3`: print an explanatory message and exit non-zero (D1).
- CMake: add a sibling `add_executable(transactional_producer_perf_test
  tests/transactional_producer_perf_test.c)` + the same three `target_*` calls
  inside the `if(RDKAFKA_FOUND)` guard in `bindings/c/CMakeLists.txt`. No CTest
  registration (matches perf convention).

### 5.3 Java — `.../TransactionalProducerPerformanceTest.java`
- Mirror `ProducerPerformanceTest.java`: same config helpers, `Metrics` class,
  warmup/measured/cooldown, `results.json` via Jackson. Extend for txn config +
  counters + `commit_latency`.
- Per producer thread: `KafkaProducer` with `transactional.id`,
  `enable.idempotence=true`; `initTransactions()` once; loop
  `beginTransaction` → `send` × N (Callback stamps ack) → `commitTransaction` /
  `abortTransaction`. Per-record latency at commit completion; commit latency
  begin→commit.
- Build: keep the existing `mainClass`; add a runnable artifact for the txn class
  per D3.

---

## 6. Definition of Done (per `definition-of-done.md`, adapted for harnesses)
- `cargo build` (+ the new test compiles under `--features integration-tests`);
  `cargo xtask format-check`; `cargo xtask lint` clean.
- C: `cmake` configures and `transactional_producer_perf_test` builds when
  librdkafka is present (needs `git submodule update --init --recursive` for
  `bindings/c/tests/unity` + `kafka/`).
- Java: `./gradlew shadowJar` (or the added task) builds the txn artifact.
- No live perf run required. No TODO/FIXME. Apache-2.0 headers on all new files.
- These harnesses are benchmark drivers, not translations of Java client
  classes, so DoD #2/#3 map to: "faithfully mirror the existing harness contract
  and the mandated txn semantics"; the Critic validates against Java
  transactional semantics + `producer-transactions.md`.
- Metrics/`results.json` schema stays plot-compatible (§2, §4.6).

---

## 7. Process (Actor↔Critic loop, N=70)
1. **Actor 70** — Phase 1 (produce mode): implement all three harnesses + wiring,
   build each, self-review, commit incrementally. Checks `COMMENTS.70.md`.
2. **Critic 70** — review commits (`cargo xtask await-commit`), validate against
   Java txn semantics / `producer-transactions.md` / DoD, write `COMMENTS.70.md`.
   No code changes.
3. Manager summarizes; if `COMMENTS.70.md` non-empty → Actor 70 fix cycle →
   Critic 70 again; loop until no open comments.
4. **Final handoff Phase 1**: push branch, open PR vs `perf-harness-fixes`,
   copy `COMMENTS.DONE.70.md` into this dir, reset `COMMENTS.70.md`.
5. **Phase 2 (eos mode)**: separate detailed plan + approval, then a fresh
   Actor↔Critic loop, folded into the same PR if time permits.

---

## 8. Open decisions for the user (please confirm before I spawn agents)
1. **D1** — C harness = librdkafka only (v3/C-FFI has no txn API). OK?
2. **D2 phasing** — land produce mode first, eos mode as a second phase in the
   same PR (or follow-up if time-constrained). OK?
3. **Deterministic abort formula** (§4.3, per-producer index, Bresenham). OK?
4. **Metrics additions** (§4.6 field names). OK, or do you want different names?
5. **Java second entry point** (D3) — second Gradle task vs documented `-cp`
   invocation. Any preference?
6. **Agent number N=70** and PR target `perf-harness-fixes`. OK?
