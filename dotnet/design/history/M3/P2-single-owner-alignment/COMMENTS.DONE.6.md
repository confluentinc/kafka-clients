# COMMENTS.DONE.6 — M3/P2 (single-owner alignment) execution record

Working record of decisions/deviations made *during* M3/P2 execution (Actor N=6).
This binding-root file is **local-only** and is never `git add`-ed (governance,
CLAUDE.md §8.4); the tracked copy is the Manager's archive at
`design/history/M3/P2-single-owner-alignment/COMMENTS.DONE.6.md`.

No Critic (N=6) `COMMENTS.6.md` items existed at the start of execution (the Critic
pass comes after this Actor pass). The Critic then filed two findings against the
doc-update commit `fdb6900`; both are resolved below (see "Critic N=6 findings
resolved").

## Confirmed decisions applied (D-Q1…D-Q4)

- **D-Q1** — `DisposeAsync` no longer drains a separately-submitted in-flight op.
  Teardown is `close_async → destroy` (`Dispose`: `close_with_timeout → destroy`),
  no wakeup+await of a tracked op. Under single-owner the awaiter of an op is its
  disposer, so there is no concurrent submitter to drain. Matches Python's
  `close()`.
- **D-Q2** — `GroupId`: `return null` → `throw InvalidOperationException`
  ("KafkaConsumer is not safe for multi-threaded access.") on the core's
  concurrent-rejection (null-handle) path. Mirrors Python `_concurrent_error()`
  (`None → RuntimeError`) and the CLAUDE.md §3 idiom-map row.
- **D-Q3** — landed as additive commits on `prashah_dev_asyncbridge_scaffolding`
  (PR #135's head), extending #135. No new branch, no separate PR, no history
  rewrite of M3/P1's commits.
- **D-Q4** — see the determinism deviation below.

## Determinism deviation (D-Q4) — the `GroupId` concurrent-rejection mapping

**What could not be tested deterministically, and why.** The plan's ADD item is:
`GroupId` under a concurrent core-guard rejection → `InvalidOperationException`
(mirrors Python `_concurrent_error`). A genuine concurrent overlap is **not
deterministically reproducible broker-free** this phase:

- broker-free `MockConsumer` state reads (`Consumer_group_metadata`) resolve
  instantly — the core's own access guard is held only for microseconds, so a
  forced two-thread overlap almost never collides;
- the one guard-holding op with a controllable duration is `poll` (out of scope
  this phase — the entire receive path is deferred).

This mirrors exactly how M3/P1 documented its own determinism limits:
- **M3/P1 D1** — the wakeup-fault on an in-flight proof op is not reachable without
  a wakeup-observing op (`poll`); the reachable slices were tested instead.
- **M3/P1 D2** — the concurrency exception-type matrix was tested at the
  `ConsumerAccessGuard` *component* level, not via a forced native op overlap.

**What was tested instead (the reachable seam).** `GroupId` round-trips the
configured group id normally, including a non-ASCII id
(`GroupId_WhenIdle_ReturnsConfiguredId`, `GroupId_NonAsciiId_RoundTrips` in
`ConsumerAsyncOperationTests.cs`). The `null`-handle → `InvalidOperationException`
mapping (not `return null`) is asserted at the smallest reachable point — the code
of `NativeConsumer.GroupId()` — and verified by inspection: the `metadata ==
IntPtr.Zero` branch now `throw new InvalidOperationException(...)`.

**Honest cost, recorded.** Removing the managed guard also removed M3/P1's
deterministic component-level test (`ConsumerAccessGuardTests.cs`, 5 tests). So
this one concurrency behavior **regresses from deterministic (component-level) to
non-deterministic** — accepted, per D-Q4. The async-op concurrent-rejection
behavior likewise moves from the (removed) managed pre-check to the core's inline
rejection surfaced as a faulted `Task`; it is exercised indirectly by the existing
churned bridge tests (many submit→callback cycles) but, like M3/P1 D2, a *forced*
overlap is not deterministic broker-free.

## Tests dropped / repurposed / kept

- **Dropped:** `ConsumerAccessGuardTests.cs` (whole file, 5 tests — the guard is
  gone); the four `FaultTaskOnly_*` component tests in
  `ConsumerCompletionBridgeTests.cs` (the sync-`Dispose` fault machinery is gone).
  Net test count 52 → 43.
- **Repurposed:** `Dispose_WithOpInFlight_*` and `DisposeAsync_WithOpInFlight_*` in
  `ConsumerAsyncTeardownTests.cs` now assert teardown **returns without hanging**
  with an unawaited op in flight — they no longer observe the op `Task` to a
  terminal state (that machinery is removed; the strand+leak is the accepted
  residual). The churn variant is a "teardown returns under churn" no-hang check.
- **Kept unchanged:** the bridge success / failure / no-throw / GCHandle-keep-alive
  / `RunContinuationsAsynchronously` / chained-ops tests; cancellation
  (pre-canceled → `OperationCanceledException`); `Wakeup` safe/reusable;
  double/mixed/concurrent teardown safe; use-after-dispose → `ObjectDisposedException`;
  all carried M0–M2 tests.

## STATUS reconciliation applied

The two pre-labeled "N=6 deferred" items in `STATUS.md` collided with this phase
(M3/P2 takes N=6) and were resolved in `design/current/STATUS.md`:
- Item 1 (`Wakeup()`/`GroupId()` TOCTOU) → accepted-by-design residual of the
  single-owner model; any future hardening renumbers to **N≥7**.
- Item 2 (op-submit-vs-teardown publish window) → **ELIMINATED** (no tracking
  fields → no window); residual submit-vs-`destroy` handle race folded into the
  third accepted residual; future hardening **N≥7**.
No dangling/contradictory "N=6 deferred" label remains (the only N=6 is M3/P2).

## Verification (all gates green)

- `cargo build --features ffi` — native cdylib + header present (run FIRST). No ABI
  change (Mode A).
- `dotnet build` — 0 warnings / 0 errors across `netstandard2.0;net8.0;net10.0`
  (library) + `net8.0;net10.0` (tests); no dangling `ConsumerAccessGuard` /
  `FaultTaskOnly` / `_inFlight*` references (only intentional docstring prose
  recording the removal); no TODO/FIXME.
- `dotnet test -f net10.0` — 43 passed, 0 failed (~1 s); no hang (every awaited
  op/teardown under `TestTimeout`).
- `dotnet format --verify-no-changes` — clean.
- Local runtime is .NET 10 (SDK 10.0.300, runtime 10.0.8); the net8.0 test *run* +
  net462 (via ns2.0) are CI-only. Both *build* legs pass.

## Critic N=6 findings resolved (doc-only; fixup of `fdb6900`)

Both findings are doc/code-consistency (MEDIUM) — no code defect. `fdb6900` updated
§B1/§B5/§B7 to the single-owner model but left two out-of-scope sections asserting
the removed M3/P1 consumer teardown drain, so the doc contradicted itself and the
landed `NativeConsumer.Dispose`/`DisposeAsync`. Both fixed in
`.claude/rules/ffi-marshalling.md` only.

- **Finding 1 [MEDIUM] — §0.3 "Tests required (both clients)".** The shared block
  still said the consumer "wakes+awaits the in-flight op (§B1/§B7)" — the removed
  separate-op drain. Rewritten to: `Dispose` returns without hanging; the consumer
  closes gracefully — `close_(with_timeout|async)` → `Consumer_destroy`, with **no
  separate-op drain** (single-owner: the awaiter of an op is its disposer,
  §B1/§B7). Now agrees with §B1's Tests-required line.

- **Finding 2 [MEDIUM] — §B2 (handle ownership), three spots.** All three prescribed
  the removed drain as mandatory:
  - the `Consumer_t` table row (was `Dispose`: drain → `Consumer_close` →
    `Consumer_destroy`) → now `Dispose`: `close_with_timeout` → `destroy`
    (`DisposeAsync`: `close_async` → `destroy`); close-before-destroy because
    destroy is fire-and-forget (cancels in-flight ops, no bg-task join).
  - the Rule (was "`Dispose` must **drain/wakeup the in-flight op → Consumer_close
    → Consumer_destroy**") → now teardown routes through the graceful close before
    destroy; **no separate-op drain**; an *unawaited* op stranded + leaked once is
    the accepted single-owner residual (Python parity, §B7).
  - the Anti-patterns bullet (was "a bare `Consumer_destroy` without the drain") →
    now "a bare destroy without routing through `Consumer_close`
    (`_with_timeout`/`_async`) first — skips the graceful bg-task join"; do not
    re-add a `Dispose`-side separate-op drain.
  - The §B2 "Why" prose was also tightened: the reason to route through close is
    the **graceful bg-task join**, not "otherwise the in-flight op's Task never
    completes" (which overstated — close does not drain an unawaited op either;
    that is the accepted residual).

  Kept the still-true framing throughout: `Consumer_destroy` is fire-and-forget, so
  close (`_with_timeout`/`_async`) must route before destroy. Only the "drain/wakeup
  the *in-flight op*" prescription was removed.

**Verification.** Grepped the whole `ffi-marshalling.md` for any remaining
"wakes+awaits" / "drain/wakeup the in-flight op" / "must drain" phrasing — none
remain (the only surviving "drains … joins the pump" line, §A1 line 282, is the
**producer** Option-A pump-join, correct and out of scope). No production code,
tests, §B1/§B5/§B7, or the plan were touched — markdown-only, so the `fdb6900` /
`8124e02` build/test/format gates stand unchanged (a markdown edit cannot alter
`dotnet build`/`test`/`format` output).

## Environment note (not a code deviation)

`dotnet` was not on the sandbox PATH; the .NET 10 SDK (10.0.300, with runtime
10.0.8) was realized locally from the pinned nixpkgs derivation
(`dotnet-sdk_10`) via `nix-store --realise` to run the gates. No repo/build-config
change was made for this.
