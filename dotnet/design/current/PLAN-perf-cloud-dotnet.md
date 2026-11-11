# Plan: .NET clients in `cargo xtask perf-cloud`

Add the .NET clients to the cloud performance driver as a client group, the way the `python`
group compares the Rust-backed and librdkafka-backed Python clients:

| Arm id | Label | Harness | Client |
|---|---|---|---|
| `dotnet-sync-rust` | .NET sync (Rust) | PerfV3, `ASYNC=False` | this repo's binding, `KafkaProducer` / `KafkaConsumer` |
| `dotnet-sync-librdkafka` | .NET sync (librdkafka) | PerfV2, `ASYNC=False` | Confluent.Kafka 2.x (NuGet), `Produce` / `Consume` |
| `dotnet-async-rust` | .NET async (Rust) | PerfV3, `ASYNC=True` | `AsyncKafkaProducer` / `AsyncKafkaConsumer` |
| `dotnet-async-librdkafka` | .NET async (librdkafka) | PerfV2, `ASYNC=True` | Confluent.Kafka 2.x, `ProduceAsync` / consumer on a task |

The report compares sync Rust with sync librdkafka, and async Rust with async librdkafka. The
`client` strings follow Python's order: `dotnet-rust`, `dotnet-rust-async`, `dotnet-librdkafka`,
`dotnet-librdkafka-async` (producer); `dotnet-rust-consumer`, `dotnet-rust-async-consumer`, … ;
`dotnet-rust-txn`, `dotnet-rust-async-txn`, … (transactional).

## Default configurations (decided)

- **librdkafka (PerfV2) runs the "same config" by default.** When `BUFFER_MEMORY` is unset the
  harness uses 32 MiB as `queue.buffering.max.kbytes` with `queue.buffering.max.messages=2147483647`
  (no message cap). A produce never blocks the caller in librdkafka: a full queue is retried after
  1 ms, in the sync and the async arm alike. The driver passes `BUFFER_MEMORY=32` to every
  producing arm anyway, so the two agree.
- **The Rust core (PerfV3) runs `buffer.memory.allocation.strategy=incremental` by default on the
  producer workload.** It runs `full` where the core does not run incremental: with a
  `COMPRESSION_TYPE` other than `none` (the core refuses it) or a `BATCH_SIZE` below 16 KiB (the
  core falls back to `full`). An explicit `BUFFER_MEMORY_ALLOCATION_STRATEGY` wins. The
  transactional workload runs `full`, like the other groups' transactional harnesses (the driver
  treats the strategy as a producer-only setting).
- The harness prints the **effective** strategy and writes it to `results.json`
  (`buffer_memory_allocation_strategy`); the report shows it per client.
- Unset consumer settings take each client's own defaults, as for the other groups.

## Ground rules for every phase

- One commit per phase. The author is the repo's configured user, with no co-author trailer. Stage
  only that phase's paths. The unrelated untracked files under `dotnet/.claude/agent-memory/` and
  `dotnet/design/current/RESEARCH-*` stay out.
- No tests and no critic per phase. The full suite and a critic review run once after the last
  phase.
- Each phase still leaves the tree compiling. It updates the existing tests its changes break, and
  ends with `dotnet build` of PerfV2, PerfV3, `Confluent.Kafka.PerformanceTests` and
  `Confluent.Kafka.IntegrationTests` (which compile-links `PerformanceCommon/PerfEnv.cs`), plus
  `cargo check -p xtask` for driver phases. Builds only, no test runs.
- Comments follow dotnet/CLAUDE.md's comment rules as they are written. Comments are written
  right from the start, so no separate comment-only commit is needed. Where a phase also
  rewrites existing comments in the files it touches, that cleanup goes in a second, comment-only
  commit of the same phase. This includes removing internal plan, milestone and decision IDs
  from the perf csproj headers and the perf targets in `dotnet/Makefile`. The repo is public.
- `make producer-perf-test-dotnet` keeps working: `CREATE_TOPIC` still defaults to true.

## Phase 1 — Confluent.Kafka bump and the producer harness

The .NET producer harness moves to the contract the C, Rust, Java and Python harnesses follow.

0. Bump Confluent.Kafka (PerfV2) to the latest 2.x on NuGet. Pin
   `python/test/performance/requirements-librdkafka.txt` to the same version, so both librdkafka
   arms run one librdkafka.
1. Read `ACKS` (`all|-1|1|0`), defaulting to `all`, instead of hard-coding `all`.
2. Pacing as the others: a checkpoint every `LIMIT_RPS/10` records against
   `start + n / LIMIT_RPS`, and a final wait for what the run still owes.
3. A counted run (`LIMIT_RPS > 0`) stops at `TEST_DURATION_SECONDS + 10 s` and prints a `capped:`
   line, which is not a failure. An unbounded run stops at `TEST_DURATION_SECONDS`. The time is
   checked on every send.
4. 1,000 consecutive rejected sends fail the run, as in C and Rust.
5. A failed delivery counts as completed and fails the run, as in C and Python.
   `messages_measured` and the rates count completed deliveries, as Python does; the latency covers
   the successful ones.
6. Latency clock: stamp with `Stopwatch.GetTimestamp()` (monotonic, sub-ms). Floor into the
   histogram buckets, and keep min, avg and max exact rather than reading them from the
   histogram.
7. CPU/RSS window: reset the baseline at measurement start and end it where the throughput
   duration ends (after the last delivery), as Python and C do.
8. Honour `METRICS_FILE` (the live view reads it; the schema is unchanged). `RESULTS_FILE` is
   already honoured.
9. PerfV2 async: `Send` detects a `Local_QueueFull` task that has already faulted, waits 1 ms and
   retries, so `AWAIT_ACCEPTED=True` really waits for acceptance. The queue-full subtraction from
   the sent count goes. The bytes value in kbytes that it computes when `BUFFER_MEMORY` is unset
   is replaced by the 32 MiB default.
10. PerfV2 sync: one poller only. Set `dotnet.producer.enable.background.poll=false`, keep the
    harness's poll thread, and retry a full queue after 1 ms.
11. PerfV2 partitioner: unchanged. `consistent_random` matches Python's v2 sync arm and
    librdkafka's default.
12. PerfV3: the incremental default (above), plus `BUFFER_MEMORY_ALLOCATION_STRATEGY`. Print the
    effective strategy and write it to `results.json`.
13. `AWAIT_ACCEPTED` defaults to true, as Python's async arm always awaits `send`. The harness
    prints it.
14. `MODE=version`, in both PerfV2 and PerfV3: print one line per item and exit 0:
    - the client library version (Confluent.Kafka and librdkafka for PerfV2, the binding and
      native core for PerfV3);
    - `Environment.Version`;
    - `GCSettings.IsServerGC`;
    - the TieredPGO state.

    The driver's `run_info` uses this.
15. Both csproj files pin Workstation GC (`ServerGarbageCollection=false`,
    `ConcurrentGarbageCollection=true`) and `TieredPGO` explicitly.
16. Update the existing tests the changes break: `PacingTests`, `ProducerSyncDeliveryTests`,
    `ProducerAcceptanceModeTests` and `PerfV3SmokeTests`, and the `PerfEngineCollection` env
    list.

## Phase 2 — consumer harness

1. `results.json`:
   - add `client`;
   - write both `rss_avg_mb` and `rss_avg_kib`, as Python does;
   - always write `cpu_avg_pct` and the RSS keys;
   - honour `RESULTS_FILE` and `METRICS_FILE`;
   - a write error fails the run instead of being swallowed.
2. Client defaults unless the test sets them: `FETCH_MIN_BYTES`, `MAX_PARTITION_FETCH_BYTES`,
   `FETCH_MAX_WAIT_MS`, `MAX_POLL_RECORDS`. PerfV3 stops deriving `max.poll.records` from
   `CONSUMER_BATCH_SIZE`.
3. PerfV2 read mode, from the variables the driver passes, as the Python librdkafka backend reads
   them:
   - `POLL_SINGLE=True` → one `Consume` per record;
   - otherwise batches of `MAX_POLL_RECORDS`, which defaults to 500 under `USE_DEFAULTS=True`.

   `CONSUMER_BATCH_SIZE` stays only as the harness's own fallback when it runs outside the
   driver. PerfV3 has no single-record poll, which is the same as Python's Rust-core arm.
4. Warmup is timed from the first record, as Python's is, so unbounded-load consumer tests can
   include the .NET clients. The readiness gate must not race the driver's load generator.
5. Clock and histogram, matching Python and Rust:
   - read the clock once per poll, not once per record;
   - a negative latency is clamped into bucket 0 and counted, not dropped from throughput;
   - the histogram caps at 600,000 ms;
   - min, avg and max are exact.

   Use the same monotonic or wall-clock rule as the reference consumer harnesses. The produce
   timestamp in the record is wall-clock.
6. The CPU/RSS window stays inside the measured window (it is already reset at the end of
   warmup).
7. The PerfV3 consumer reads values without a copy. Use a zero-copy deserializer, if the binding
   offers one, matching Python's memoryview arm. Otherwise document the copy in the harness
   README.
8. Update `ConsumerReadinessGateTests` and any other test the changes break.

## Phase 3 — transactional harness (new)

`MODE=transactional` in PerfV2 and PerfV3. It is written once in PerformanceCommon with backends
per client, and follows the Rust and C transactional harnesses (the reference):

- Settings:
  - `TXN_MODE=produce|eos`, `RECORDS_PER_TRANSACTION`, `ABORT_RATE`,
    `NUM_TRANSACTIONAL_PRODUCERS`, `TRANSACTIONAL_ID`, `SOURCE_TOPIC`, `SEED_COUNT`,
    `SEED_THROUGHPUT`;
  - the producer settings of Phase 1, with idempotence and `acks=all` forced;
  - the allocation strategy is `full`.
- Each producer has its own worker thread. Pacing is applied only at the transaction boundary,
  at a per-producer rate of `LIMIT_RPS / NUM_TRANSACTIONAL_PRODUCERS`.
- `ABORT_RATE` is spread deterministically (Bresenham), not at random.
- Per-record `latency_ms` runs from produce to commit completion, for committed records only.
- `results.json`: the producer keys, plus:
  - `transactions_per_s`;
  - `committed_transactions`, `aborted_transactions`, `aborted_records`,
    `error_aborted_transactions`;
  - `commit_latency_ms.{p50,p99}` (begin → commit);
  - `abort_latency_ms.{p50,p99}` (begin → abort).
- Retry: commit, abort and `send_offsets` get at most 3 attempts (`MAX_TXN_CALL_ATTEMPTS`), and
  only while the error is retriable and not fatal, as in Rust and C. An abortable error aborts
  the transaction. An error-forced abort fails the run: a rejected produce, or a failed
  `send_offsets` or commit. A fatal error fails the run.
- Sync arms use the blocking transaction calls. The Rust-core async arm awaits
  `AsyncKafkaProducer`'s `Task` calls, with no cancellation token. A cancelled token only ends
  the wait, and a retry while the core still runs the call raises
  `ConcurrentModificationException`.
- Confluent.Kafka 2.x has only blocking transaction calls. Its async arm sends with
  `ProduceAsync` and runs each producer's transaction control on a dedicated thread
  (`TaskCreationOptions.LongRunning`), so blocking calls do not starve the thread pool. The arm's
  label in the harness output says "async produce, blocking transaction control".
- PerfV2 classifies errors with `KafkaRetriableException`, `KafkaTxnRequiresAbortException` and
  `Error.IsFatal`.
- `eos` mode: consume from `SOURCE_TOPIC`, produce, call `SendOffsetsToTransaction` with the
  consumer's group metadata, and commit.

## Phase 4 — driver (`rust/xtask/src/perf_cloud.rs`, `report.rs`, `live.rs`)

1. `Group::Dotnet` (`dotnet`) and the four `Arm`s above: `id`, `label`, `expected_client`,
   `command`, `prerequisites`, `artifact_sources`.
   - `command` is `dotnet <published dll>`, with no `current_dir`: the harness writes
     `metrics.jsonl` to the working directory.
   - `MODE` (`producer|consumer|transactional`) and `ASYNC=True|False` are set where they survive
     the `HARNESS_VARS` scrub, which runs after `command()`. Either `own_env` takes the workload,
     or `run_harness` sets them after the scrub. A unit test pins it.
2. `build --group dotnet`:
   - `cargo build --features ffi --release`;
   - `dotnet publish -c Release -f net10.0` of PerfV3 and PerfV2 into separate output folders
     (only the dependency `Confluent.Kafka.dll` collides);
   - the published native library next to `PerfV3.dll` is a prerequisite;
   - staleness via `artifact_sources`:
     - PerfV3 ← `PerfV3/`, `PerformanceCommon/`, `dotnet/src/Confluent.Kafka/`, `rust/src`;
     - PerfV2 ← `PerfV2/`, `PerformanceCommon/`;
   - a check that the `dotnet` SDK is present.

   net10.0 only: it is the newest supported runtime, and the cloud runner installs one SDK.
3. Booleans the .NET harness reads are passed as exactly `True` / `False`. `idempotence_value`
   stays lowercase, because it goes straight into the client config.
4. `env_for`: the `LIBRDKAFKA_BATCH` → `POLL_SINGLE` / `MAX_POLL_RECORDS` mapping extends to the
   PerfV2 arms.
5. `check_arms`: an explicit `BUFFER_MEMORY_ALLOCATION_STRATEGY` is accepted for the Rust, Java
   and `dotnet-*-rust` arms (the message names them). The unbounded-load consumer rule already
   lets the .NET arms through; only its message changes ("runs only the Python and .NET
   clients").
6. `Metrics` gains an optional `buffer_memory_allocation_strategy`, read from `results.json`. The
   report shows a per-client "Buffer allocation strategy" row whenever a client reports one, in
   place of the test-level row for that test. The live view shows it too, if it has a place for
   it.
7. `run_info`: runs `MODE=version` for both harnesses and records the .NET runtime, GC mode,
   TieredPGO, the Confluent.Kafka and librdkafka versions, and the binding's version.
8. Report: the two comparisons (sync, async) for the dotnet group.
9. `HARNESS_VARS`: add `MODE`, `AWAIT_ACCEPTED`, and every other variable the .NET harnesses read
   that is missing. The scrub also removes inherited `DOTNET_*` and `COMPlus_*` variables, so a
   runner's GC settings cannot change one group only.
10. The `harness_vars_cover_what_the_harnesses_read` test scans the PerfV2, PerfV3 and
    PerformanceCommon sources too, with the C# patterns `PerfEnv.GetString(`,
    `GetStringOrNull(`, `GetInt(`, `GetLong(`, `GetBool(`, `Has(`, `ParseInt(` and
    `Environment.GetEnvironmentVariable(`.
11. `HELP_NOTES`, the `Group::parse` error text ("expected `rust`, `python` or `dotnet`") and its
    test, and the module docs. Unit tests mirror the Python group's.

## Phase 5 — tests, batches and docs

- Add `dotnet` to the groups of every test the `python` group runs (producer, transactional,
  consumer). `producer-max-1kb-200p-key100b-incremental` stays rust-only. The plain keyed test
  runs incremental on the .NET Rust core by default, and the report shows it.
- Batches `dotnet-producer-1`, `dotnet-producer-2`, `dotnet-consumer-1` and `dotnet-consumer-2`
  mirror the Python ones. Update the header text in `batches.txt`.
- `dotnet/DOTNET.md` perf section and a `dotnet/tests/Performance` README describe:
  - how the driver runs the .NET harnesses;
  - the default configurations above;
  - the consumer batch size when the harness runs outside the driver (`CONSUMER_BATCH_SIZE`, 2500 for both, as Python).

## After all phases

- Full suite:
  - `cargo xtask` lint, format-check and the xtask unit tests;
  - `make verify`-equivalent checks;
  - the .NET perf test project (net8.0 + net10.0) against Docker;
  - `dotnet format`.
- A critic review of the whole branch, then fix rounds.
- Outside this repo (the CI pipeline repo): install the .NET SDK in its dependency script, and
  add the dotnet group's clients and cluster size. Then run a short paid smoke run, after a
  go-ahead.
