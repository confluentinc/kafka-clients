# M3/P3 — Poll + the receive path (owned-handle completion bridge) — Actor N=7 decisions & residuals

This records the decisions taken and residuals accepted **during execution** of the
approved M3/P3 plan (`design/history/M3/P3-poll-receive-path/PLAN.md`). The plan's six
confirmed decisions are the contract; the notes below are the Actor's execution-time
calls (the plan explicitly left these to the Actor) plus source-verified findings.

---

## Execution decisions (plan left these to the Actor)

### D1 — Generic-bridge migration: subclass, not duplicate (PLAN decision 4, option a)

`OperationCompletionSource` was generalized **in place** to
`OperationCompletionSource<TResult>`, and the void path is expressed as the thin
`sealed class OperationCompletionSource : OperationCompletionSource<bool>`. This keeps
**every** M3/P1+P2 call site byte-for-byte (`new OperationCompletionSource()`,
`context.Complete(error)`, `context.Task` returning a non-generic `Task`) and preserves
the void success semantics (null error → `TrySetResult(true)`). The base's
`Complete(IntPtr)` null-error branch defers to a `private protected virtual
CompleteWithSuccessNoResult()` which the `<bool>` subclass overrides to
`CompleteWithResult(true)`; a result-returning op never hits that branch (it completes
via `CompleteWithResult`, called from the trampoline). All 5 invariants
(GCHandle free-once, cancellation→wakeup, no-throw, SafeHandle/TCS thread-safety,
`RunContinuationsAsynchronously`) are unchanged. **Gate:** the full pre-existing test
suite stays green (66 total, 0 failed — see the re-verified snapshot below; 67 before
the Critic-N=7 Finding-1 removal of the flaky in-flight-wakeup test), including the
void-bridge continuation / churn / GC / no-throw tests, plus a new explicit regression
(`ConsumerPollBridgeTests.ResultBridge_VoidPathUnchanged_SubscribeStillResolves`).

### D2 — Header representation: a small internal `readonly struct RecordHeader` (PLAN decision 2, Actor's call)

Headers are carried as `IReadOnlyList<RecordHeader>` where `RecordHeader` is a small
owned internal `readonly struct` (`string Key`, `ReadOnlyMemory<byte>? Value`) rather
than a bare tuple — clearer, self-documenting, testable, and no extra heap allocation
beyond the key string + value array it already owns. Internal only; no public `Headers`
type (that lands with the public client).

### D3 — Mock poll-readiness in tests: assign → **seek** → add_record → poll

Verified against `src/consumer/mock_consumer.rs`: the mock `poll` Step 6 calls
`update_fetch_position` for any assigned partition **without a valid position**, which
(with the default `latest` reset) needs an end offset or it errors. The canonical Rust
mock-poll test (`tests/consumer/mock_consumer_test.rs`) establishes a valid position via
`seek` first, so Step 6 is skipped. The .NET tests mirror this: `Assign((topic,0))` →
`SeekAsync(topic,0,0)` → `AddRecord(offset>=0)` → `PollAsync`. This needs no
beginning/end-offset mock drivers, so only `Consumer_assign` / `MockConsumer_add_record`
/ `MockConsumer_set_poll_error` are declared (the plan's exact set).

### D4 — `PinBytes` empty-array handling (mock driver only, not the public hot path)

`NativeConsumer.AddRecord` pins key/value call-scoped (§A4; the mock copies them
synchronously). A **null** array → `(IntPtr.Zero, -1)` (absent); an **empty** array →
`GCHandle.Alloc(Pinned)` + `AddrOfPinnedObject()` (non-null) + len 0 (a genuine empty
value). ffi §A4 notes the non-null-for-empty behavior is undocumented and prefers a
stack sentinel for the *public send path*; here it is a **broker-free test driver**
only (not the public receive/send hot path), and the
`PollAsync_EmptyKeyAndValue_MapToEmptyNonNull` test confirms it round-trips as a
non-null empty array. Acceptable for a mock driver; the public producer send path (a
later phase) will use the sentinel per §A4.

---

## Top-risk verification (PLAN Risks: the D-Q4 concurrency slice)

**Verified against `src/consumer/mock_consumer.rs` + `src/ffi/consumer.rs` (the plan's
required early check).** A `MockConsumer` poll **cannot be blocked at a
test-controlled point broker-free** through the C ABI:

- The mock `poll` future (`mock_consumer.rs:525`) runs to completion **synchronously**
  — it drains one queued poll task, then checks-and-clears the wakeup flag (Step 4),
  takes any injected poll error (Step 5), updates positions, and drains records. There
  is **no** `Notify` / channel-await / sleep a test could hold open.
- The one Java hook that could inject a block (`schedule_poll_task`,
  `mock_consumer.rs:286`) is **not exposed at the FFI** (the only mock drivers are
  `add_record` / `update_(beginning|end)_offsets` / `update_partitions` /
  `set_poll_error`). So there is no lever to make a poll stay in flight.

Consequences:

- **Wakeup one-shot — NOW DETERMINISTIC (M3/P1 D1 un-deferred).** The mock checks the
  wakeup `AtomicBool` inside `poll()` (Step 4, before draining records) and clears it.
  So `Wakeup()` then `PollAsync` **deterministically** faults with a Wakeup
  `KafkaException` **once**, then the next poll succeeds — Java's one-shot
  `WakeupException` semantics. Tested:
  `ConsumerPollWakeupCancelTests.Wakeup_ThenPoll_FaultsOnce_ThenReusable` (queues a
  record and asserts the *subsequent* poll returns it). The "flag set, then poll" driver
  is behaviorally identical to Java's one-shot contract; a *genuinely mid-flight* wakeup
  (fired while the poll is blocked) is **not** reachable (no block point) — a
  best-effort/no-corruption variant is `Wakeup_DuringInFlightPoll_DoesNotCorrupt`.

- **In-flight `CancellationToken` cancel → `OperationCanceledException`: RESIDUAL
  (not deterministic broker-free).** The pre-canceled path is deterministic and tested
  (`PollAsync_PreCanceledToken_ThrowsOperationCanceled`, via the synchronous
  `ThrowIfCancellationRequested` in `SubmitOperation`). The *in-flight* variant needs
  the token to fire **after** submit but **before/while** the poll observes the wakeup
  flag; because the mock poll completes instantly on its spawned task, that overlap is a
  genuine race, so a deterministic in-flight-cancel test would be flaky. The wiring
  (`RegisterCancellation` → `_cancellationRequested=1` + `wakeup()`; `Complete(error)`
  maps a fault to `OperationCanceledException` when cancellation was requested) is
  verified by inspection and the pre-canceled test. **Kept reachable slice + documented
  residual** (mirrors M3/P2 D-Q4), rather than shipping a flaky test.

- **Concurrency matrix (M3/P2 D-Q4 un-defer): RESIDUAL (not deterministic
  broker-free).** A genuine submit→callback **overlap** (op A in flight, op B rejected
  with `ConcurrentModification`; or a concurrent `GroupId` read → `InvalidOperationException`)
  requires op A to still hold the core guard when op B is submitted. Since the mock poll
  completes instantly and releases the guard before firing the callback, there is no
  controllable-duration guard-holding op — exactly the non-determinism M3/P2 D-Q4
  documented. The plan named `poll` as the intended controllable-duration op *if* it
  could be blocked; the source check above shows it cannot (broker-free, via the FFI).
  The concurrent → faulted-`Task` / `InvalidOperationException` mappings remain verified
  by inspection of the core inline-rejection path (`poll_async`, `consumer.rs:612-617`)
  and the `GroupId` null-handle path (already tested in
  `ConsumerAsyncOperationTests.GroupId_*`). **Kept reachable slice + documented
  residual.** This closes when a controllable-duration mock op (e.g. an FFI-exposed
  `schedule_poll_task` / a blockable mock poll) lands — a Rust-core dependency, not a
  .NET-side change.

## Header round-trip: RESIDUAL (mock `add_record` carries no headers)

The C ABI `MockConsumer_add_record(topic, partition, offset, key, value)` does **not**
accept headers, so a record built by the mock has **zero** headers — a header
*round-trip* through the mock is **not reachable** broker-free this phase. Reachable and
tested: the **empty-headers** case (`header_count == 0` → an empty, shared header list)
end to end (`ConsumerPollReceivePathTests.PollAsync_RoundTripsAllFields` asserts
`Headers` is empty; `ConsumerPollHeadersTests.PollAsync_RecordWithoutHeaders_HasEmptyHeaders`).
The header copy-out marshaller (`ConsumerRecordsMarshal.CopyHeaders`: `header_count` →
per-index length-delimited `header_key` (§B3) + copy-out `header_value`) is verified by
inspection against the header accessors, and the **§B3 length-delimited primitive the
header-key path uses is directly and thoroughly tested** — through the record **topic**
(`PollAsync_NonAsciiTopicAndBytes_RoundTripViaOutLen`) and via
`Utf8MarshalLengthDelimitedTests` (non-ASCII boundary char, the explicit
never-NUL-scan guard, zero-length-non-null, null, negative length). This mirrors the
D-Q4 precedent: the reachable driver does not exercise the header branch, so it is
documented rather than faked. It closes when a header-carrying mock driver (or the
public typed producer→consumer round-trip) lands.

---

## The central correctness obligation — free-exactly-once (self-review)

`ConsumerCallbacks.OnPoll` frees, in a `finally` (so it runs on the no-throw path too):
(1) the owned `ConsumerRecords_t` batch **after** copy-out via null-safe
`ConsumerRecordsDestroy` (a no-op when `records` is null — failure / inline rejection);
(2) the `KafkaError` on failure inside `Complete(error)` → `FromHandle` (its own
`finally`); (3) the per-op `GCHandle` via idempotent `FreeGcHandle`. Audited on all
paths — success / failure / inline core-rejection / no-throw (copy-out throws) /
submit-threw (handled by `AbandonBeforeSubmit`, native never ran). The batch destroy is
safe because `ConsumerRecordsMarshal.CopyOut` retains **no** borrowed pointer past the
loop (owned copy-out, §6.4) — nothing native-backed escapes; the
`PollAsync_ResultSurvivesBeyondBatchDestroy` test forces GC after the poll and reads the
bytes to prove it. The churn test
(`PollAsync_RecordsAndErrorsAndEmpties_Churned_NoCorruption`, 100 interleaved
records/errors/empties) is the double-free/leak detector.

---

## Verification gates (RE-VERIFIED after the Critic N=7 fixes — all pass)

1. `cargo build --features ffi` — native + header present (Mode A, no ABI change).
2. `dotnet build Confluent.Kafka.sln` — **0 warnings / 0 errors** across library TFMs
   (netstandard2.0 / net8.0 / net10.0) + test TFMs (net8.0 / net10.0);
   `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild`. No new public type → no new
   CS1591. No TODO/FIXME; Apache-2.0 header on every new `.cs`.
3. `dotnet test -f net10.0` — **66 passed, 0 failed**, no hang (~0.4 s; every awaited
   op + teardown under `TestTimeout`). 23 new poll-path tests (was 24; the flaky
   `Wakeup_DuringInFlightPoll_DoesNotCorrupt` was removed per Finding 1); all carried
   M0–M3/P2 tests green.
   **20× back-to-back re-run (the Finding-3 reproducibility proof): 20/20 runs with
   Failed=0.** 19/20 reported Total=66; run 19 reported Total=48 — a VSTest
   back-to-back *discovery under-count* (Failed=0), the known harness artifact, NOT a
   test failure. The allocation-budget and wakeup classes were also each run 5× in
   isolation: stable, 0 failed. (The pre-fix suite failed ~50% of runs, all traced to
   Findings 1 and 2 — that claim is now corrected and reproducible.)
4. `dotnet format --verify-no-changes` — clean.

net8 test *run* and net462 are CI-only (both build legs pass locally).

---

## Critic N=7 review — the 4 findings, resolved (fixup pass)

The Critic (N=7) confirmed the interop / memory-safety logic **sound** (no production
defect); the 4 findings were **test + governance**. Resolved as follows — no
receive-path / bridge production-logic change.

### Finding 1 [HIGH] — `Wakeup_DuringInFlightPoll_DoesNotCorrupt` was flaky/broken — FIXED (test-only, removed)

The test submitted a poll then called `Wakeup()`, but the mock poll runs to completion
synchronously (source-verified `src/consumer/mock_consumer.rs` poll Step 4:
check-and-clear the wakeup flag, return) and exposes no block hook at the C ABI. So the
`Wakeup()` raced the instant poll: when the poll won, the one-shot flag was left **set**
and leaked into the *next* poll (line 121), which then faulted with an **uncaught**
Wakeup `KafkaException`. It failed 10/10 in isolation, ~40% in-suite. **Removed** (option
b): the reachable, Java-faithful behavior — `Wakeup()` → next poll faults once → a
subsequent poll succeeds (one-shot + reusable) — is already covered **deterministically**
by `Wakeup_ThenPoll_FaultsOnce_ThenReusable` (asserts TYPE + one-shot + reusability, never
`Code`), so the removed test was redundant as well as flaky. A genuinely in-flight wakeup
needs a blockable mock poll (a Rust-core dependency); a `// NOTE` at the removal site
records why. The wakeup class is now stable 5/5 in isolation.

### Finding 2 [MEDIUM] — allocation-budget test measured the wrong thread — FIXED (test-only)

The copy-out runs on the foreign **dispatcher thread** and the `await` continuation
resumes on a **different** pool thread, so `GC.GetAllocatedBytesForCurrentThread()`
bracketing the `await` measured neither thread (the delta could go negative, ~8% flake).
Two changes: (a) switched to **process-wide** `GC.GetTotalAllocatedBytes(precise: true)`,
which captures the dispatcher-thread copy-out regardless of which thread ran it; and (b)
moved consumer create / assign / seek / the per-record `AddRecord` marshal loop / dispose
**outside** the measured window so only the `PollAsync` call (where the copy-out happens)
is bracketed — removing the non-copy-out allocations the Critic flagged. Still asserts the
**marginal** per-record budget (large − small, over Δrecords) against the documented
1024 B ceiling, which cancels fixed per-poll overhead and any ambient process-wide noise.
Stable 5/5 in isolation and across the 20× run. No sync-poll DllImport was added
(decision 5 preserved).

### Finding 3 [MEDIUM] — DoD "0 failed" not reproducible — RESOLVED (proven)

Root cause was Findings 1 + 2. With both fixed, the suite is deterministically green: the
**20× back-to-back re-run above is 20/20 Failed=0** (the one Total=48 is a VSTest
discovery under-count, not a failure). The verification snapshot above is corrected to the
re-run result (66 passed / 0 failed).

### Finding 4 [MEDIUM] — governance: N≥7 → N≥8 renumber + STATUS handoff — FIXED (docs/comments only)

M3/P3 **takes N=7**, so the deferred cross-thread **hardening** labels previously
"N≥7" now collide. Renumbered to **N≥8** (with a one-line "M3/P3 took N=7" note) in:
`src/Confluent.Kafka/Internal/NativeConsumer.cs` (the `Wakeup` and `GroupId` residual
docstrings), and `design/current/STATUS.md` (the M3/P1 review-outcome line + the two
M3/P2 reconciliation items). Added the **M3/P3 STATUS handoff** at the top of
`design/current/STATUS.md`: M3/P3 done; the **M3/P1 D1 wakeup-fault one-shot** moved
**deferred → done** (now deterministic via `poll`); and the **remaining residuals** kept
deferred — the in-flight `CancellationToken` cancel, the D-Q4 concurrency matrix, and the
full end-to-end header round-trip — each needing a **Rust-core dependency** (an
FFI-exposed blockable mock poll / a header-carrying `add_record`), not a .NET change. No
dangling/contradictory "N≥7" label remains in tracked source or STATUS (stale `obj/`/`bin/`
XML regenerate on build).
