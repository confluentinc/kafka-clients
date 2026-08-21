# PLAN — M11/P7: Producer in-flight-cap throttle (blocking gate + configurable cap)

**Binding:** `bindings/dotnet` · **Milestone/Phase:** M11 / P7 · **Agent number:** N=40
**Branch:** `prashah_dev_dotnet_binding_producer_tuned` (base **`675feb19`** = M11/P6 close-out) — do not switch.
**Mode:** **A** — .NET-managed-only. No C ABI / Rust-core / header / cbindgen change; no new `[DllImport]`. Manager verifies `git diff --stat 675feb19..HEAD -- src/ src/ffi/ target/include/confluent_kafka.h cbindgen.toml` is empty (Rust-core/ABI/header/cbindgen untouched; all changes under `bindings/dotnet/{src,tests,.claude/rules,design}`).
**Status:** **APPROVED** (human, 2026-08-21, all recommendations accepted). Settled: **D1 = Option 1** (synchronous blocking gate); **D2 = (b) configurable count cap, default 5000**, parsed from the config dict — **pure Mode A**, (c) byte-based / (d) drop-the-cap explicitly declined; **D3 = `max.block.ms` from config, default 60000**, timeout → Java-faithful `KafkaException` with asserted message; D4/D5/D6/D7 as written. Actor(40)/Critic(40) loop to follow.

---

## 1. Goal (one line)

Productionize the M11/P6-spike fix: turn the async producer's slot acquire into a **synchronous blocking gate** (so an un-awaited `Send()` actually throttles the produce loop — Java-faithful) and replace the spike's fixed constants (`MaxInflightSends = 5000`, `SpikeMaxBlockMs = 60000`) with a **configurable cap** and **real `max.block.ms` semantics**, then adapt the cap tests and docs to the blocking contract.

---

## 2. Background & confirmed problem (verified against the code + the spike)

- **What M11/P6 shipped (HEAD `675feb19`):** a max-count `SemaphoreSlim(1000)` on the **async** send path; the slow (cap-engaged) path acquires the slot with `await _inflight.WaitAsync(linked.Token)` **off** the produce loop (`SendAfterWaitAsync`).
- **The regression P6 introduced:** the async path acquires the slot with a *non-blocking-to-the-caller* `await`. The perf harness (and any un-awaited `Send()` caller) awaits the **channel write / TCS-returning call**, not the acquire — so parked `SendAfterWaitAsync` state machines **pile up unbounded**. Verified on a local broker 2026-08-21 (acks=all, 1 MB batch, linger 5, 1 KB value, async max-rate): **63.5k msg/s, ~3.0 GB RSS, p50 10,001 ms, 250% CPU, walltime 49s** (bled past the 15s window).
- **The spike fix (Option 1, currently UNCOMMITTED in the working tree):** make the slow-path acquire a **synchronous blocking** `_inflight.Wait(SpikeMaxBlockMs, linked.Token)` in `SendAfterWaitAsync` (spike hunks: `MaxInflightSends = 5000`, `const int SpikeMaxBlockMs = 60_000`, blocking `Wait` — `NativeProducer.cs:116/121/531-541`). Because the caller does **not** await `backend.Send()`, a synchronous block on the caller thread is what actually throttles the loop — Java `send()` blocking on `buffer.memory` / Python sync `space.result()`. Result at cap=5000: **591.6k msg/s, p50 7 ms, p99 12 ms, 127 MiB, 247% CPU** — beats ckd/librdkafka V2 on throughput, latency, memory AND CPU.
- **Not new in kind (important for the contract argument):** the shipped async `Send` **already** blocks the caller inline in `Producer_send` up to `max.block.ms` on the 32 MB core buffer (`NativeProducer.cs` send path; `BufferPool.java` `moreMemory.await`). Option 1 just gates **earlier and tighter** — it does not introduce a blocking behavior the async `Send` never had.
- **Cap-value sweep (spike, blocking gate, async max-rate 30s, 1 KB/acks=all/1 MB batch/linger 5):** cap=1000 → 96.9k (starves .NET — the loop stop-starts every 1000 sends, hurting BOTH throughput and latency); **cap=5000 → 591.6k, p50 7 ms (the knee / sweet spot)**; cap=10000 → 635.2k but ~2× latency (past the knee). The knee is at ~5000 for 1 KB messages.
- **Why the cap value is fragile:** it is a record **COUNT**. 5000 is optimal for 1 KB messages, but would **starve** tiny messages and **blow up memory** for large ones. Java bounds by `buffer.memory` **BYTES** (32 MB); librdkafka by `queue.buffering.max.messages` (100k) + `.kbytes` (1 GB); Python hardcodes a compile-time `#define …1000` that statically sizes its C arrays (no env/config/kwarg at all). M11/P6's .NET 1000 was a literal copy of Python's 1000, but that parity is **superficial** — Python is GIL-bound + non-blocking, .NET is true-parallel + (now) blocking, so 1000 starved .NET to 97k. This is the central design tension P7 must resolve.

---

## 3. Scope

**In scope**
- Convert the slow-path acquire from `await WaitAsync` to a **synchronous blocking gate** on the async send path only (`NativeProducer.SendAfterWaitAsync`). Fast path (`_inflight.Wait(0)`) unchanged.
- Replace the spike constants with a **hardcoded-default-5000 cap overridable by the env var `CONFLUENT_KAFKA_PRODUCER_MAX_INFLIGHT_SENDS`** (an interim, undocumented tuning knob — NOT a config-dict key, §7 D2) and a **`max.block.ms`-driven** acquire timeout (from the config dict, §7 D3), both resolved once in `Create` (defaults when absent / on the mock path).
- Timeout → a Java-faithful `KafkaException` (BufferExhausted/TimeoutException analog) with asserted message content.
- Re-walk and adapt the M11/P6 §5 exactly-once-release / deadlock matrix for the blocking gate; add the **teardown-wakes-a-blocked-caller** regression test (matrix condition #2).
- Rewrite the `PublicProducerInflightCapTests` that assumed the non-blocking (`parked.IsCompleted == false`, drive-Send-inline) contract.
- Doc-sync: `ffi-marshalling.md §A7`, `SendCompletionPump.cs` "Backpressure" xmldoc, `NativeProducer.cs` field/method xmldoc, and `bindings/dotnet/CLAUDE.md §3/§4` where the async-send / cancellation shape is described.
- Fold the uncommitted working-tree spike into the phase's first impl commit (spike markers never land verbatim — §8).

**Out of scope**
- The **sync** `NativeProducer.Send` path — blocks per-message on `FutureRecordMetadataGet`, cannot pile up, never touches the cap (unchanged).
- Any C ABI / Rust-core / header / cbindgen change (Mode A). A **byte-based cap that needs the core to expose its effective `buffer.memory`** is a Mode-B item — flagged in §7, not silently included.
- Option 2's two-stage `Task<Task<RecordMetadata>>` return (evaluated + rejected in §7 D1).
- A public transactions/metrics/headers producer surface.

---

## 4. Design

### 4.1 State changes in `NativeProducer` (spike → production)

Spike hunks currently in the working tree (to be superseded, not committed verbatim):
```
private const int MaxInflightSends = 5000;      // SPIKE
private const int SpikeMaxBlockMs = 60_000;     // SPIKE
... _inflight.Wait(SpikeMaxBlockMs, linked.Token) ...   // SPIKE blocking gate
```

Productionized (per the settled §7 decisions — **Option 1 gate + hardcoded-default-5000 cap with an env-var override**):
```
private readonly int _maxInflightSends;   // was const 5000; default 5000, env override (§7 D2)
private readonly int _maxBlockMs;         // was const 60_000; from max.block.ms in config (§7 D3), default 60_000
private readonly SemaphoreSlim _inflight; // new SemaphoreSlim(_maxInflightSends, _maxInflightSends)
private readonly CancellationTokenSource _sendGate = new CancellationTokenSource();  // unchanged
```
- `Create(config)` reads `_maxInflightSends` **once** from the env var `CONFLUENT_KAFKA_PRODUCER_MAX_INFLIGHT_SENDS` (`Environment.GetEnvironmentVariable` + `int.TryParse`; fall back to **5000** if unset/empty/non-numeric/≤0 — §7 D2), and `_maxBlockMs` from `max.block.ms` in the existing `IReadOnlyDictionary<string,string>` it holds (default 60000 — §7 D3). Both are Mode A (BCL / dict reads, no ABI). `CreateMock(...)` uses the defaults + the **internal test seam** (§5/§6) and does **NOT** read the env var (process-global → would race parallel tests).
- Max-count ctor (`new SemaphoreSlim(N, N)`) retained — over-release throws `SemaphoreFullException` (the release ≤ acquire guard). Unchanged from P6.

### 4.2 The blocking gate — `SendAfterWaitAsync` (slow path only)

Fast path in `SendViaPump` is unchanged (`_inflight.Wait(0)` → `SendAcquired`, zero per-send alloc — DoD §10). The slow path becomes:
```
private async Task<RecordMetadata> SendAfterWaitAsync(pump, record, ct)
{
    using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _sendGate.Token);

    // BLOCKING gate (Option 1). Throws OperationCanceledException if ct OR teardown (_sendGate)
    // fires (no slot acquired → nothing owed); returns false on timeout (max.block.ms elapsed).
    if (!_inflight.Wait(_maxBlockMs, linked.Token))
        throw <Java-faithful BufferExhausted/Timeout KafkaException>;   // §4.3

    if (Volatile.Read(ref _closed) != 0) { _inflight.Release(); throw new ObjectDisposedException(nameof(NativeProducer)); }

    return await SendAcquired(pump, record, ct).ConfigureAwait(false);
}
```
- Kept `async` (the tail `await SendAcquired` is unchanged) but the **acquire itself blocks the caller thread** — that is the whole point: an un-awaited `Send()` on a full cap parks the produce loop, throttling it.
- The synchronous preconditions in `SendViaPump` (`ThrowIfClosed`, `ct.ThrowIfCancellationRequested`, `EnsurePump`) are **unchanged** — a closed producer / pre-canceled token still surfaces exactly as today, before the gate.

### 4.3 Timeout semantics (replaces `SpikeMaxBlockMs`)
- `Wait(_maxBlockMs, linked.Token)` returning **false** = the cap stayed full for `max.block.ms`. Throw a flat `KafkaException` (the binding's only error type; ffi §A5) whose message mirrors Java's `TimeoutException` / `BufferExhaustedException` wording (e.g. "Failed to acquire an in-flight send slot within {N} ms (max.block.ms)"). Set an appropriate error code; **assert the message content** in a test (DoD §3).
- **Layering note (design detail for the Actor):** `max.block.ms` also governs the core's own `buffer.memory` block inside `Producer_send`. Because the managed cap is the tighter bound (5000 records ≪ 32 MB), the core almost never blocks, so the worst-case is effectively one `max.block.ms` budget. If the user sets `buffer.memory` below the cap footprint, both layers can each wait up to `max.block.ms` (a ≤2× worst case) — document, do not attempt to share the budget across the two layers this phase.

### 4.4 Release / teardown — unchanged from P6
- Release stays tied **1:1 to the pump's exactly-once future-destroy** (`SendCompletionPump` ctor takes `Action<int> releaseSlots`; release after each destroy site). No change — the gate only alters *how the slot is acquired*, not how it is released.
- Teardown ordering unchanged: `_sendGate.Cancel()` is the **first** action after winning the `TryBeginClose` latch, then `StopPump()`/`StopPumpAsync()`. `_sendGate` intentionally NOT disposed (P6 COMMENTS.39 fix (a)). Under the blocking gate, `_sendGate.Cancel()` now wakes a **blocked thread** (the linked token makes `Wait(timeout, token)` throw `OperationCanceledException`) instead of an async awaiter — same terminal state, blocking flavor (§5 row 8).

---

## 5. Safety / deadlock matrix — re-walked for the blocking gate

The M11/P6 §5 exactly-once-release table (8 rows) is re-verified against the blocking gate; the acquire mechanism changed (async `WaitAsync` → blocking `Wait(timeout, token)`), the **release** mechanism did not. A **9th terminal state (timeout)** is added.

| # | Condition | Under the blocking gate | Slot owed? | Status |
|---|---|---|---|---|
| 1 | New `Send` after teardown | `ThrowIfClosed` throws synchronously, never reaches the gate | no acquire | unchanged |
| **2** | **Teardown wakes a BLOCKED caller** | `_sendGate.Cancel()` (first teardown action) fires the linked token → `Wait` throws OCE; caller acquired nothing | no acquire | **structurally handled, NOT test-proven — NEW test (§6)** |
| 3 | Parked waiter wins a freed slot in the cancel/free race | `_closed` re-check → `_inflight.Release()` + `ObjectDisposedException` | acquired → released once | unchanged (P6 row 8) |
| 4 | Pump-join no-hang (flush-before-join) | Gate adds no blocking inside the pump; join property unchanged | n/a | unchanged |
| 5 | Sync `Send` path | Never touches the cap (blocks per-message on `_get`) | never acquires | unchanged |
| 6 | Synchronous `Producer_send` failure | `SendAcquired` pre-send `catch` releases the held slot | acquired → released once | unchanged |
| **7** | **Over-release / precondition** | Max-count ctor → `SemaphoreFullException`; pre-canceled token throws before the gate | n/a | test-proven (P6) |
| 8 | Orphan future (TCS/registration/Enqueue OOM) | `SendAcquired` orphan `catch` `destroy_all(1)` + release | acquired → released once | unchanged |
| **9** | **`max.block.ms` timeout (NEW)** | `Wait` returns false → throw `KafkaException`; acquired nothing | no acquire | **NEW test (§6)** |

**No deadlock:** a blocked caller is always woken by one of {a freed slot, `_sendGate.Cancel()` at teardown, the caller's `ct`, the `max.block.ms` timeout}. No path both blocks forever and holds a slot.

---

## 6. Test plan (mandatory — the rule is not "tested" without these)

All against `AsyncMockProducer(autoComplete: false)`, driving completions from a helper thread, under a `TestTimeout` hang guard. **Key structural change:** under the blocking gate a "parked" send is a **blocked thread**, not an incomplete `Task` — so any test that drives an overflow/parked `Send` must run it **on a helper thread** (driving it inline would block the test thread and hang the test).

**Test seam (needed for a fast, deterministic suite):** add a **test-only** way to construct a mock with a **small cap** (e.g. 4) and a short `max.block.ms` — either an `internal` mock ctor overload or an `internal` setter, exposed via the existing `InternalsVisibleTo`. Filling the default cap (5000) per test is wasteful; a small cap makes the fast/slow-path boundary and the blocking behavior trivial to hit. `s_cap` in the tests reads the **effective** configured cap via the white-box accessor (`MaxInflightSlots` becomes an instance accessor if the cap is per-instance).

Tests to **rewrite** (assumed the P6 non-blocking contract — verified against the current file):
- `Cap_Deadlock_SyncDispose_WithFilledCapAndParkedWaiters` / `Cap_Deadlock_DisposeAsync_...` — currently do `parked = producer.Send(...)` inline then `Assert.False(parked.IsCompleted)`. Rewrite: fill the cap, launch the overflow send(s) on **helper thread(s)** (they block in the gate), then `Dispose()`/`DisposeAsync()` from the test thread; assert teardown **returns without hanging** and each helper's `Send` throws `OperationCanceledException` (from `_sendGate`).
- `Cap_CancelWhileWaitingForSlot_FaultsCanceled_NoSlotLeaked` — the `Send(record, cts.Token)` now blocks; run it on a helper thread, cancel `cts`, assert the helper's `Send` throws OCE and no slot leaked (`InflightSlotsAvailable == 0`, still fully held by the filled sends).
- `Cap_BackpressureEngages_OverflowSendDoesNotSendUntilSlotFrees` — the overflow `Send` now blocks; run it on a helper thread, assert `HistoryCount()` stays at N while the helper is blocked (backpressure engaged — did not reach `Producer_send`), then free a slot and assert `HistoryCount() → N+1` and the helper completes.
- `Cap_SlotLeak_ManySends_AllComplete` / `Cap_AfterNSendsComplete_ExactlyNSlotsFree` — fire M/N sends inline with the completer already running; under the blocking gate the loop blocks briefly per contended send instead of awaiting. Re-confirm they still pass with the (smaller, test-seam) cap; assert the semaphore returns to exactly the baseline.

Tests that **pass as-is** (verified — no inline blocking send): `Cap_OverRelease_ExtraReleaseThrowsSemaphoreFull`, `Cap_PreCanceledToken_OnSlowPath_ThrowsSynchronously` (the sync `ThrowIfCancellationRequested` fires before the gate).

**NEW tests:**
- **6.a — matrix condition #2 (teardown wakes a blocked caller):** fill the cap, block one `Send` on a **helper thread** inside the gate, then `Dispose()` (and a second variant `DisposeAsync()`) from the test thread; assert teardown returns within the deadline (no hang), the helper's `Send` throws `OperationCanceledException`, and no slot is leaked. This is the memory-flagged gap ("structurally handled but not test-proven, needs a helper-thread-driven test since Send blocks").
- **6.b — matrix condition #9 (`max.block.ms` timeout):** with a **small `max.block.ms`** (test seam), fill the cap, drive one overflow `Send` on a helper thread with NO completer running; assert it throws a `KafkaException` after ~`max.block.ms`, **assert the message content** (DoD §3), and no slot is leaked.
- **6.c — re-perf (soft, not a hard xUnit gate):** rerun PerfV3 async max-rate at the chosen cap (default) vs V2/ckd; expect ≈591.6k / p50 7 ms / 127 MiB at cap=5000. Reported in the close-out, not xUnit-gated (perf is env-dependent + the full perf leg is intermittently flaky — the [[dotnet-harness-ci-only-gates]] pattern).

**Existing suites to re-confirm green:** `PublicProducerSendTests`, `PublicProducerSendAllocationBudgetTests` (fast path unchanged → budget preserved), `PublicProducerTeardownTests`.

---

## 7. Decisions to settle (each: recommendation + rationale + open trade-off)

### D1 — Throttle mechanism → **RECOMMEND Option 1 (blocking gate)**
- **Option 1 (blocking gate):** proven in the spike (591.6k / p50 7 ms), ~10-line change, Java-faithful. **Cost:** the async `Send` can block the caller thread under backpressure (a contract nuance — but **not new in kind**, since it already blocks up to `max.block.ms` on a full core buffer), and ~4 cap tests must be rewritten to the blocking contract.
- **Option 2 (two-stage `Task<Task<RecordMetadata>>`, await-the-acquire — Python's async shape):** keeps `Send` non-blocking and preserves most tests, **but** it **cannot be the public `IAsyncProducer<K,V>.Send` shape**, which must stay `Task<RecordMetadata>` (Java shape, `bindings/dotnet/CLAUDE.md §3`). The public surface would have to flatten the two stages back into one `Task`, which re-opens the exact pile-up the fix targets for any caller going through the interface — so the throttle would not hold on the public surface. Also a larger return-shape refactor.
- **Recommendation:** Option 1. It is the only option that throttles the *public* `Task<RecordMetadata>` surface, and its contract change is a tightening of an already-blocking call, not a new behavior.

### D2 — Cap value strategy → **SETTLED: hardcoded default 5000, env-var override (NOT a config-dict key)**
**Resolution (human, 2026-08-21) — a deliberate deviation from this section's drafted "configurable via a binding config key" recommendation:**
- **No config-dict key at all.** Do NOT add any binding-private key to the Kafka `config` dict. This keeps the `config` dict to **real Kafka keys only** (strict Java-shape fidelity, `bindings/dotnet/CLAUDE.md §2/§2.1`) and **moots the config-key-naming-convention question** that paused the phase. (The originally-drafted `dotnet.producer.max.inflight.sends` key is **abandoned**.)
- **Hardcoded default = 5000** (the proven sweet-spot; §2).
- **Override via env var `CONFLUENT_KAFKA_PRODUCER_MAX_INFLIGHT_SENDS`** — read **once** at real-producer construction (`NativeProducer.Create`) via `Environment.GetEnvironmentVariable`, `int.TryParse`; **fall back to 5000 if unset / empty / non-numeric / ≤ 0**.
- **Interim, UNDOCUMENTED tuning knob** — NOT a committed public API, NOT a public config surface. Document it as such (an internal escape hatch, subject to change) in the `NativeProducer` xmldoc and `ffi §A7`. Still **pure Mode A** (managed-only; `Environment.GetEnvironmentVariable` is a BCL call, no ABI).
- **Mock path does NOT read the env var:** `CreateMock(...)` uses the default 5000 + the **internal test seam** (§6). The env var is process-global and would race across parallel xUnit tests, so it must NOT be the test seam — only the real `Create` reads it.
- **Rationale for the deviation:** the config-dict-key approach put a binding-private, non-Java knob into the Kafka config surface and forced a naming-convention decision with no precedent; an env var keeps the config dict Java-pure, needs no naming convention, and is trivially an "interim knob" the team can remove or replace without a config-surface breaking change. The declined alternatives — (c) byte-based / `buffer.memory`-tied and (d) drop-the-cap — remain out of scope (both were the Mode-B options the user declined).

### D3 — `SpikeMaxBlockMs` productionization → **RECOMMEND: read `max.block.ms` from config, default 60000**
- Replace the fixed `const SpikeMaxBlockMs = 60_000` with `_maxBlockMs` parsed from `max.block.ms` in the config dict (default 60000 ms = Java/librdkafka default). Mode A.
- Timeout → flat `KafkaException` with a Java-faithful `TimeoutException`/`BufferExhausted` message (asserted, §6.b). Not a new exception type (the binding is flat, ffi §A5).
- Note the ≤2× worst-case layering vs the core's own `max.block.ms` block (§4.3) — document, do not attempt to share the budget this phase.

### D4 — Deadlock coverage → **RECOMMEND: re-walk the matrix (§5) + add the condition-#2 test (§6.a) and the timeout test (§6.b)**
- The Critic(40) must independently re-walk the §5 table against the blocking-gate code, confirming the release mechanism is untouched and the acquire-throw paths (OCE / timeout) owe no slot. Condition #2 (teardown wakes a blocked caller) gets the new helper-thread regression test.

### D5 — Test rewrites → as enumerated in §6 (blocking contract + helper threads + test seam)
- Add the small-cap / short-`max.block.ms` test seam; move overflow/parked sends onto helper threads; reinterpret "parked" as "blocked thread"; assert error-message content for the timeout.

### D6 — Doc-sync → update every place that documents the async cap
- `ffi-marshalling.md §A7` managed-cap paragraph (currently documents the async `WaitAsync` design): rewrite to the **blocking-gate** design (slow-path `Wait(timeout, token)`, fast-path `Wait(0)` unchanged), the `max.block.ms` timeout, the contract nuance ("Send can block the caller under backpressure — Java-faithful, not new in kind"), and the chosen cap value/strategy. Review the §A7 anti-patterns list (any `WaitAsync`-specific wording) and the `bindings/dotnet/CLAUDE.md §4` cancellation row ("cancels the wait, never aborts an enqueued send") for the blocking slow path.
- `SendCompletionPump.cs` "Backpressure" xmldoc; `NativeProducer.cs` field/method xmldoc (drop the `SPIKE` markers; document `_maxBlockMs`/`_maxInflightSends` sourced from config); `bindings/dotnet/CLAUDE.md §3` if it sketches the async-send shape.

### D7 — Folding the uncommitted spike → **first impl commit supersedes it (spike markers never land verbatim)**
- The working tree currently holds the spike hunks in `NativeProducer.cs` (`MaxInflightSends = 5000`, `SpikeMaxBlockMs = 60_000`, blocking `Wait`) plus untracked memory/agent files (see `git status`). The Actor(40) sequence: (1) commit the **approved** copy of this PLAN.md; (2) the first impl commit **rewrites** the spike into the productionized form (config-driven cap + `max.block.ms`, cleaned comments) — so no `// SPIKE:` marker or `SpikeMaxBlockMs` const is ever committed. Per-path `git add` of the intended files only; never `git add` the untracked agent-memory / persona files.

---

## 8. Definition of Done

Per `.claude/rules/definition-of-done.md` and `bindings/dotnet/CLAUDE.md`:
- Blocking gate implemented on the async slow path; fast path unchanged; sync path untouched. No TODO/FIXME; no `SPIKE` markers; no duplicated types.
- Cap + `max.block.ms` sourced from config (per the chosen D2/D3); defaults on the mock path; test seam for a small cap.
- Host-only scaffolding (`SemaphoreSlim`/`CancellationTokenSource`/config parse) adds **no Kafka behavior** — binding-layer flow control mirroring Java/Python intent (`bindings/CLAUDE.md §1/§6`).
- **DoD §10 (hot-path allocation):** the uncontended fast path stays `Wait(0)`-only, zero per-send allocation; guarded by `PublicProducerSendAllocationBudgetTests` staying green.
- **DoD §3:** cap tests rewritten to the blocking contract; the `max.block.ms` timeout test **asserts message content**; the condition-#2 teardown test added.
- Build 0W/0E across the TFM matrix (lib netstandard2.0/net8.0/net10.0; tests net462/net8.0/net10.0); `dotnet format --verify-no-changes` clean; **Mode A verified** (empty diff over Rust core/ffi/header/cbindgen; zero new `[DllImport]`).
- `dotnet test -f net10.0` green (new + existing). **CI-only deferrals (not loop blockers):** net8.0 **execution** (runtime absent locally — build-verified only) and the re-perf measurement (§6.c) — state explicitly in STATUS + the reset COMMENTS note ([[dotnet-harness-ci-only-gates]]).
- Docs synced (D6). No Java classes translated → `marked_classes.txt` unchanged.

---

## 9. Deliverables / files expected to change (all under `bindings/dotnet/`)
- `src/Confluent.Kafka/Internal/NativeProducer.cs` — spike → production: `_maxInflightSends` (env-var override, default 5000) + `_maxBlockMs` (from `max.block.ms` in config, default 60000) instance fields; `SendAfterWaitAsync` blocking gate + timeout throw; `Create` (reads env var once) / `CreateMock` (default + test seam, no env var) wiring; test-seam accessor; xmldoc marking the env var an interim/undocumented knob.
- `src/Confluent.Kafka/AsyncKafkaProducer.cs` / `AsyncMockProducer.cs` — pass the config-derived cap / `max.block.ms` (and the test seam on the mock) if the plumbing isn't fully inside `NativeProducer`.
- `src/Confluent.Kafka/Internal/SendCompletionPump.cs` — "Backpressure" xmldoc only (release mechanism unchanged).
- `tests/Confluent.Kafka.UnitTests/PublicProducerInflightCapTests.cs` — rewrite the blocking-contract tests; add §6.a (teardown-wakes-blocked-caller) + §6.b (`max.block.ms` timeout, message assert); add/consume the small-cap test seam.
- `.claude/rules/ffi-marshalling.md` (§A7), `bindings/dotnet/CLAUDE.md` (§3/§4 if applicable) — doc-sync.
- `design/current/STATUS.md` — M11/P7 entry (at handoff).

---

## 10. Risks / blockers
- **Test hangs from the contract flip** — the headline risk: any overflow/parked `Send` left inline will hang the test thread. Mitigated by the helper-thread rewrite (§6) + the `TestTimeout` guard. Critic(40) must confirm no rewritten test drives a blocking `Send` inline.
- **Cap-value decision is product-shaping** — D2 is the real open question; the wrong default re-introduces the starvation (cap=1000) or the memory blow-up (cap=10000). Recommend configurable-with-5000-default; user decides.
- **Byte-based / core-`buffer.memory` temptation is Mode-B** — do not add an ABI getter or change a core default under the Mode-A banner; flag to the user (D2 c/d).
- **`max.block.ms` double-budget** — documented (§4.3), not fixed this phase.
- **No blockers** — Mode A, all mechanisms (`SemaphoreSlim.Wait`, config parse) exist on the netstandard2.0 floor.

---

## 11. Execution loop (post-approval only — NOT this run)
Actor N=40 implements (fold spike → production, blocking gate, config-driven cap/`max.block.ms`, test rewrites + new tests, doc-sync) → Critic N=40 reviews commits (must re-walk §5, verify Mode-A `git diff`, fast-path allocation budget, no inline-blocking-Send in tests, timeout message assertion) → Manager summarizes `COMMENTS.40.md`/`COMMENTS.DONE.40.md` → fix cycles until no approved issues → handoff (update `design/current/STATUS.md`, archive `COMMENTS.DONE.40.md` under `design/history/M11/P7-producer-inflight-cap-throttle/`, reset binding-root `COMMENTS.40.md`). Commit only — do NOT push (human manages pushes). Commits `--no-gpg-sign`, footer `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`; per-path `git add`.
