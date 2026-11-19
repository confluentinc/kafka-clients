# COMMENTS.DONE.35 — M13/P1 .NET perf suite (Slice 1), fix cycle

Actor N=35. Both non-blocking items from `COMMENTS.35.md` (Critic review of the
Slice-1 commits `467ad864..634c6667`) are resolved per the Manager's ruling.
Fix commits: `dcc5bc60` (item 2), `21f87e46` (item 1). Neither COMMENTS file is
committed.

---

## Item 2 — RESOLVED (FIX applied): guard the sync producer measured loop

**Fix commit:** `dcc5bc60` (fixup ref `467ad864` — the PerformanceCommon commit
that introduced `ProducerBenchmark.cs`; the Critic's suggested `b95199c4` ref was
the PerfV3 commit, but the sync loop lives in PerformanceCommon, so the fixup
references `467ad864`).

**What changed:** `tests/Performance/PerformanceCommon/ProducerBenchmark.cs`,
`RunSync` measured loop. The `backend.Send(...)` call is now wrapped in a
try/catch that mirrors Python's `record_completed_calls` resilience
(`producer_performance_test.py:632-648`): on exception it prints
`"Produce call resulted in exception: {ex.Message}"` and **continues** (does not
abort the run).

**Python-exact fidelity (the rationale for the counting choice):** on a failed
sync send the record is still counted as **completed** with **recorded latency**,
but is **NOT** verified. Python increments `completed_messages` and records
latency even when `produce_call.result()` raises; verification only happens on
the success path. Leaving the failed record unverified lets the existing
`Verified != Completed` branch in `FinishAndSummarize` suppress the summary
exactly as Python does. This also makes the sync path consistent with the async
recorder (`RunAsync`), which already catches-and-continues (mirroring
`record_completed_calls_worker`, `producer_performance_test.py:842-855`).

**Implementation note (minimal + no duplication):** the shared
completed+latency tail of `RecordCompleted` was extracted into a private
`RecordCompletion` helper; `RecordCompleted` (verified path) and the new
`RecordFailed` (unverified path) both call it. `RecordCompleted`'s public
behavior/signature is unchanged, so the async caller is unaffected — the change
is localized to the sync loop plus a behavior-preserving extract-method. The
success path stays inside the `try` (latency measured immediately after `Send`,
then `RecordCompleted`, which cannot throw); the `catch` logs and calls
`RecordFailed`. `PerfRecordMetadata` is a `readonly struct`, so no nullable
gymnastics were needed.

---

## Item 1 — RESOLVED (PARTIAL fix per Manager ruling): root-Makefile
producer/consumer delegation only

**Fix commit:** `21f87e46` (fixup ref `634c6667` — the Slice-1 commit that added
the binding-local perf targets in `bindings/dotnet/Makefile`; this completes the
root-reachability half of that deliverable).

**What changed:** the **repo-root `Makefile`** now has two targets —
`producer-perf-test-dotnet` and `consumer-perf-test-dotnet` — that delegate into
the binding via `$(MAKE) -C bindings/dotnet <target>` (the same delegation shape
as the root `test-dotnet` target, passing `RUST_PROJECT_ROOT`). This makes the
binding-local Slice-1 targets reachable from the repo root, mirroring the
existing `producer-perf-test-python` / `consumer-perf-test-python`. Both new
targets are added to the root `.PHONY` list. No `build-rust` prerequisite is
needed at the root: the delegated binding targets do the two-stage native build
(`cargo build --features ffi`) themselves before `dotnet run` of PerfV3 (verified
by `make -n` dry-run, which correctly recurses into `bindings/dotnet`).

**Deferral recorded (do not silently drop):** `test-integration-perf-dotnet` is
**deliberately NOT** wired into the root Makefile now. Per the Manager's ruling,
that root wiring is **deferred to Slice 2**, where it lands together with the
Docker in-suite smoke. A `NOTE (Slice 2)` comment in the root Makefile records
this next to the two new targets so the deferral is visible in-tree, not only
here.

---

## DoD re-verification (fix cycle)

- `cargo build --features ffi` — OK (native present for PerfV3 P/Invoke).
- `dotnet build -c Release` PerformanceCommon + PerfV3 + PerformanceTests —
  **0W/0E on net8.0 AND net10.0**.
- `dotnet format --verify-no-changes` on `PerformanceCommon` — clean (exit 0).
- murmur2 xUnit — **11/11 passed on net10.0**. The .NET **8.0 runtime is not
  installed** in this env, so `dotnet test -f net8.0` cannot EXECUTE (the net8.0
  **build** is clean, giving TFM build coverage); net10.0 is the execution gate.
- Mode-A hygiene: `git diff --stat 8e3a1a90..HEAD -- src cbindgen.toml
  target/include/confluent_kafka.h` is **empty**. Managed test/harness + Makefile
  only; no `src/ffi`, header, cbindgen, or core Rust changes.
- Commit hygiene: per-path `git add` only; the two commits touch exactly
  `ProducerBenchmark.cs` and `Makefile`. Nothing excluded staged (no
  agent-memory, COMMENTS, built artifacts, or archived PLAN).

---

# Slice 2 — execution record (PerfV2/ckd + Testcontainers smoke + Makefile)

Actor N=35. Slice 2 stacks on the clean Slice-1 close-out (`21f87e46`). Four
commits `4958374c`, `32d4a603`, `b07d33a3`, `4fa62bb5`. No Critic items were open
during this slice. Neither COMMENTS file is committed.

## Pinned dependency versions (D9 / D2)

- **ckd**: `Confluent.Kafka` **2.15.0** — the latest stable `Confluent.Kafka` 2.x
  at implementation time (resolved from the NuGet flat index: 2.15.0 is newest
  stable; 2.15.0-rc2/-dev are pre-release). Confined to **PerfV2 only**.
  **TFM outcome:** 2.15.0 ships `lib/net10.0` AND `lib/net8.0` assets directly
  (verified in the nupkg), so PerfV2 keeps the suite's `net8.0;net10.0` matrix —
  **no net8.0-only drop**. Confirmed at runtime: `Confluent.Kafka.dll` resolves in
  PerfV2's `bin/Release/net8.0` and `bin/Release/net10.0`.
- **Testcontainers**: `Testcontainers` **4.14.0** — latest stable; ships
  `lib/net10.0` + `lib/net8.0`. Confined to the **smoke project only**.
- Neither NuGet appears under `src/Confluent.Kafka` (grep-verified).

## Deliberate deviations (Slice 2)

1. **ckd async producer = ProduceAsync wrap** (PLAN §1.2 / D5). ckd has no
   AIO-style async producer (Python's async v2 wraps `AIOProducer`); the async v2
   backend wraps `ProduceAsync → Task<DeliveryResult>`. The sync backend is the
   `CompatibleProducer` analog: fire-and-forget `Produce(topic, msg,
   deliveryHandler)` + a background `Poll` thread, with `Send` blocking on a
   per-send delivery `TaskCompletionSource` to present the serial-blocking backend
   shape `RunSync` expects (queue-full retried like CompatibleProducer's
   `BufferError` loop). `CA1849` NoWarn added (scoped to PerfV2) for the
   intentional TCS block on the caller thread.
2. **ckd consumer = Consume-loop batch approximation** (PLAN §1.2). ckd's
   `Consume(timeout)` returns ONE record; `PollBatch` loops it up to the batch size
   (first call blocks up to the poll timeout, the rest drain non-blocking) to
   approximate v3's batch `Poll`. **ckd has no async consumer** (no `AIOConsumer`
   analog), so `V2AsyncConsumerBackend` wraps the sync consumer via `Task.Run` —
   the manual-comparison async v2 baseline (analogous to the producer note).
3. **librdkafka-form config** (PLAN §1.2): `message.max.bytes` (not
   `max.request.size`); `queue.buffering.max.kbytes` +
   `queue.buffering.max.messages=2147483647` (not `buffer.memory`); librdkafka-form
   SASL (`sasl.username`/`sasl.password`, via `SaslForm.Librdkafka`); consumer
   `fetch.message.max.bytes` (not `max.partition.fetch.bytes`), no
   `max.poll.records`. `acks=all` set explicitly (ckd default is also all).
4. **`partitioner=murmur2_random`** set explicitly in the v2 producer config
   (Python `v2_producer`, `producer_performance_test.py:515-516`), gated on
   `!USE_DEFAULTS`. Minor divergence: Python sets it only for the sync v2 producer;
   PerfV2 sets it in the shared v2 producer config so the async path is comparable
   too — harmless (it only selects the target partition).
5. **CLIENT_VERSION pinned in the exe** (PLAN §1.1.1). PerfV2's `Program.Main` sets
   `CLIENT_VERSION=2` before any config parse — the per-client-exe boundary IS the
   client dimension, so the exe is authoritative regardless of launch env. Drives
   the consumer `results.json` `client_version="2"` and the default group.id
   `benchmark-2-<epoch>`. Runtime-verified: SUMMARY prints `CLIENT_VERSION=2` and
   `results.json` carries `"client_version": "2"`.
6. **VERIFY_CONSUMED is v3-scoped**. It needs a consumer and PerfV2 references only
   ckd; PerfV2's producer logs a skip notice when `VERIFY_CONSUMED=True` rather than
   adding a second ckd client path (PLAN deliverable #2 scopes it to v3).
7. **Same-assembly collision structurally avoided** (PLAN §1.1.1 / §4). The smoke
   project references ONLY `PerformanceCommon` + `Testcontainers`; it launches the
   built **PerfV3** exe BY PATH (`dotnet exec PerfV3.dll`, path derived from the
   test's own config+TFM, or `PERFV3_DLL`). PerfV2 and PerfV3 are never in one sln.
8. **Docker skip = logged pass** (not a real xUnit skip). xUnit 2.9.x has no dynamic
   `Assert.Skip` and `SkippableFact` is outside the approved dep scope, so a
   Docker-absent env is a clean pass with a logged notice (`KafkaBrokerFixture`
   caches the skip). The v3-only gate (D10) is enforced — PerfV2 is never in the
   smoke.

## Verification (Slice 2)

- **Builds 0W/0E** on net8.0 + net10.0: PerfV2 and the smoke project (PerfV3 +
  PerformanceCommon rebuilt clean). `dotnet format --verify-no-changes` clean on
  all four perf projects.
- **Docker smoke — GREEN on net10.0 (locally, not merely CI-pending):** Docker was
  in fact available in the dev env, so `dotnet test -f net10.0` ran the 4 v3 smoke
  cases (producer sync/async, consumer sync/async) against a fresh native + fresh
  PerfV3 and a real `apache/kafka:4.2.0` KRaft testcontainer → **15/15 passed**
  (11 murmur2 + 4 smoke), i.e. each PerfV3 subprocess exited 0 within the p99≤70ms
  budget. **CI-PENDING:** the **net8.0** leg of the smoke (no net8.0 runtime
  installed locally → build-verified only), same as the existing `test-dotnet`
  net8.0 leg.
- **PerfV2 runtime-verified** against a throwaway broker: producer sync (p99 9ms)
  + async (p99 111ms) both exit 0 with DO_VERIFY=True (proving the ckd
  delivery→PerfRecordMetadata mapping); consumer sync (601 msgs) + async (602 msgs)
  both exit 0 with `client_version="2"`.
- **Mode-A:** `git diff --stat 8e3a1a90..HEAD -- src cbindgen.toml
  target/include/confluent_kafka.h` is **empty**.
- **DoD hot-path allocation audit (repo DoD #10): N/A** — the perf harness is not on
  the shipped send/receive path.

---

# Fix cycle (Slice 2 review) — both non-blocking items RESOLVED

Actor N=35. The Critic's Slice-2 review (`COMMENTS.35.md`, reviewing
`21f87e46..4fa62bb5`) raised two NON-BLOCKING items; the Manager approved fixing
both. Fix commits: `5a96028c` (item 1), `2ef37f4d` (item 2). Neither COMMENTS
file is committed. Mode-A re-confirmed empty; PerfV2 builds 0W/0E on net8.0 +
net10.0; `dotnet format --verify-no-changes` clean on PerfV2.

## Slice-2 Item 1 — RESOLVED (FIX applied): v2 consumer `Consume` must skip errored messages

**Fix commit:** `5a96028c` (fixup ref `32d4a603` — the PerfV2 consumer-adapter
commit that introduced `V2ConsumerBackends.cs`).

**What changed:** `tests/Performance/PerfV2/V2ConsumerBackends.cs`,
`V2SyncConsumerBackend.PollBatch` and `PollSingle`. ckd's `Consume(TimeSpan)`
**throws `ConsumeException`** on a consume error (there is no per-record `.Error`
on `ConsumeResult`; only timeout→`null` and EOF→`IsPartitionEOF` are
non-throwing), which previously propagated through the shared `ConsumerBenchmark`
loop and crashed the v2 run. Each per-`Consume` call is now wrapped in
`try/catch (ConsumeException)` and **skips** the errored consume:

- `PollBatch` → `continue` (skip this drain, keep going up to the batch size),
  mirroring Python `_LibrdkafkaConsumer.poll_batch`'s
  `if msg is None or msg.error(): continue`
  (`consumer_performance_test.py:233-243`). A `finally` resets `timeout` to
  `TimeSpan.Zero` after the first attempt so the existing batch semantics hold
  (first Consume blocks up to the poll timeout, the rest drain non-blocking) —
  **preserved even across the skip path** (the reset moved out of the post-Consume
  line into `finally`, so a first-attempt throw does not leave the next iteration
  blocking).
- `PollSingle` → `return Array.Empty<PolledRecord>()` (skip, end the single
  poll), mirroring Python `poll_single`'s `if msg is None or msg.error(): return`
  (`consumer_performance_test.py:245-253`).

**Rationale / fidelity:** exact Python skip-and-continue semantics on the consume
error path; a null/timeout result and partition-EOF still end the batch as before
(unchanged). The catch is scoped to `ConsumeException` (not a broad catch), so no
new `NoWarn` was needed. Confined to the v2 consumer adapter — PerfV3 and
PerformanceCommon untouched. **Non-blocking** because this is the manual-comparison
v2 baseline error path, not the gated v3 smoke (D10); a healthy run raises no
exception.

## Slice-2 Item 2 — RESOLVED (FIX applied): root `test-integration-perf` aggregate now includes `-dotnet`

**Fix commit:** `2ef37f4d` (fixup ref `4fa62bb5` — the Slice-2 Makefile-perf-wiring
commit that added the standalone `test-integration-perf-dotnet` target +
delegation).

**What changed:** repo-root `Makefile`, the `test-integration-perf` aggregate
(recipe lines, line ~219). It ran `test-integration-perf-rust` +
`test-integration-perf-python` but not `-dotnet`; added
`$(MAKE) test-integration-perf-dotnet` as the third recipe line, mirroring how the
(also Docker-gated) Python arm is already included. The leading comment now reads
"Rust, Python and .NET performance suites" and notes the .NET arm is the
Docker-gated v3 smoke that skips cleanly without Docker. No other aggregate/target
disturbed; the standalone target + its `bindings/dotnet` delegation already
existed from `4fa62bb5`.

**Verification:** `make -pn test-integration-perf` shows the recipe now recurses
into all three (`-rust`, `-python`, `-dotnet`) in order. (A live `make -n`
recursion halts on this env's missing Python `venv` before reaching the dotnet arm
— GNU make runs `$(MAKE)` lines even under `-n` — but the make database dump and a
`MAKE=':'` override both confirm the third line is present.)

**Rationale:** removes the aggregate asymmetry the Critic flagged (Python's
Docker-gated perf was in the aggregate, dotnet's was not). Both perf arms are
Docker-gated/skip-clean, so ordering + skip behavior are consistent.
