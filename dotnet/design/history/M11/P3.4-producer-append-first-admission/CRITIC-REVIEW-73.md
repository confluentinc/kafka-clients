# Critic 73 — M11/P3.4, producer append-first send admission

Scope: `b855d583`, `f09bfcdf`, `a89f34de` against baseline `d8ac7c50`. Working tree
carries no tracked source delta (only `.claude/agent-memory/`, local-only).

Ground truth: `target/include/confluent_kafka.h`, the Kafka Java producer API shape,
`ffi-marshalling.md` §A1/§A2/§A4/§A6/§A7, `bindings/dotnet/CLAUDE.md` §4/§7.

Verified locally before writing: `dotnet test -c Release -f net10.0` →
**`Failed: 0, Passed: 910, Total: 910`**, run completed (no `Test Run Aborted`).
`git diff d8ac7c50 HEAD -- src/ffi cbindgen.toml` is **empty** (Mode A held);
`[DllImport]` count in `NativeMethods.cs` is **222 → 222** unchanged (the commit
message's "228" is a different tally — repo-wide `[DllImport]` across `src/` is 225;
either way the delta is zero, which is the load-bearing part). Zero `TODO`/`FIXME`
under `src/` + `tests/`.

---

## What I checked and found sound (stated so the next round does not re-derive it)

**Permit accounting — the proof holds.** I re-derived it rather than accepting it.

* Take sites: exactly one, `SendAccumulator.cs:367` (`_admission.Wait`), reached once
  per `SubmitAdmitted` that successfully appended.
* Release sites: exactly two, `SendAccumulator.cs:967` (`RunLoopCore`) and `:848`
  (`AbandonOnThreadFailure`), each fed by the single `_chainRecords`-clearing site
  `TakeChainLocked` (`:1004-1013`).
* Grow site: exactly one, `Append`'s `_chainRecords++` (`:525`), under `_gate`.
* Nothing can throw between the take and `ReleaseAdmission` in `RunLoopCore` (the
  lock exits, then the call — `:948`/`:967`).
* **`Submit` is reachable without the take from exactly one place, and it is a test
  fixture** (`SendAccumulatorTests.cs:2023`, `AppendWithoutAdmission`), used only at
  `:555`, `:624`, `:1214` where the append is asserted to throw `ObjectDisposedException`
  — so it injects no surplus into a live accumulator. Grep for `.Submit(` across
  `src/` + `tests/` returns that one call site. **This is the single check that would
  have falsified "releases == appends", and it passes.**
* Therefore surplus permits = number of `Wait`s that threw without taking, which is
  only the teardown `OperationCanceledException` path; both `_spaceGate.Cancel()` sites
  also close the accumulator, so no later send can consume the surplus.
* The stated bound `P ≤ MaxAdmittedRecords + W` checks out: with `C = MaxAdmitted + R − T ≥ 0`
  and `A = T + W'`, `P = A − R_taken ≤ A − R = MaxAdmitted + W'`. The test asserts
  `peak ≤ Cap + Senders` with `W' ≤ Senders`, which is the right constant.
* `Admission_PermitsAreReturnedExactlyOncePerRecord_AcrossRepeatedDrains` **is** a real
  drift detector, not a restatement: a +1/drain drift makes round 2's
  `Assert.Equal(0, AvailableAdmissions)` (`:833`) fail, and a −1/drain drift makes
  `:838-840` fail. Both witnesses are read. Ten rounds defeats a cancel-once constant.

**Teardown walk — complete, no new hang/strand/leak.** Traced every path with a caller
parked mid-`_admission.Wait` and its record already appended:

* `Stop` (`:744-768`): `Cancel` → `_closed`+pulse → bounded `Join`. The `Cancel` is
  before `_closed`, so a woken caller is released rather than holding teardown; its
  record is in the final chain and is sent. `Append` reads `_closed` under the same
  lock the batch thread's take uses, so no record can fall between them.
* `AbandonOnThreadFailure` (`:803-858`): `_closed` + take in **one** `_gate`
  acquisition, so an append is either counted or refused; both chains settled; then the
  unconditional `Cancel`; then a swallowed `ReleaseAdmission`. A parked caller's record
  is always in one of the two settled chains.
* `SettleAbandonedChain` cannot itself derail the handler — per-node `catch`, and
  `DeliveryRegistration.Fire` (`DeliveryRegistration.cs:123-144`) is a total no-throw
  boundary, so a **throwing** user callback on that path is contained. A **blocking**
  one parks the handler before the `Cancel`, but `Stop`'s own `Cancel` then releases the
  parked caller and the bounded `Join` still returns — so `Dispose` does not hang.
* `_spaceGate` is never disposed (no `Dispose`/`using` anywhere) — correct, and no
  `.Token`-on-disposed `ObjectDisposedException` window was opened. This matches the
  settled M11/P6 rationale; not a finding.
* `IsEmptyAndIdleLocked` collapsing to one stage is safe: append-first puts a record in
  the chain strictly before its caller can return, so "chain empty" now *implies* "nothing
  accepted is unforwarded", and it is strictly stronger than the two-stage predicate it
  replaces. `Flush` therefore includes a parked caller's record (covered by
  `Flush_IncludesASendWhoseCallerIsStillParkedOnAdmission`, both the sync and the
  `SignalIdleLocked` release site).
* Deletion safety: diffing the pre-phase `Stop` against the new one, the only removed
  steps are the queue seal, `FlushQueuedSubmissions` and `SettleQueuedSubmissions` —
  all three exclusively queue machinery. No predicate silently became always-true, and
  no settle site was lost.

**Ordering** rests on the append happening under `_gate` at call time, not on
`SemaphoreSlim` fairness — which is the only argument `ffi §A1` accepts. Per-caller
ordering holds by construction (a caller's send N+1 cannot start until send N returned,
and send N appended before it parked). ✅

**Pinning (§A4).** A parked caller now holds **no** pins — they transfer to the node
with the append — so §A4's "a blocked sender must not hold pins" is satisfied more
strongly than before. Pin/unpin balance is unchanged on every path (`Submit`'s
`finally`, `SendNode`'s `finally`, `SettleAbandonedChain` → `ReleasePins`).

**Test-count arithmetic reconciles exactly.** Old file: 58 `public void`/`public async Task`
declarations (incl. 4 `OnCompletion`); new: 45 (incl. 3). Net test methods:
−23 + 10 = **−13** in `b855d583` (922 → 909), then **+1** in `f09bfcdf` (→ 910).
Measured 910 here. No class was dropped from discovery and no `--filter` is in play.
**No discovery-undercount finding is warranted** (and per my own FP record I would not
file one on a raw count anyway).

**Item 2 (D2) implemented soundly.** The wait is genuinely uninterruptible except by
`_spaceGate`: `_admission.Wait(CancellationToken)` on a never-disposed semaphore and a
never-disposed CTS can throw only `OperationCanceledException`, which is swallowed, so
`SubmitAdmitted` cannot strand a `completion` the caller was never handed. The caller's
token is untouched (`_ = cancellationToken;`) and still cancels the returned `Task` via
`SendViaPump`'s own registration. No caller-visible path hangs past teardown.

**Item 4 — corrections, not embellishment.** Every hunk on `IAsyncProducer.cs`,
`AsyncKafkaProducer.cs` and `IDeliveryCallback.cs` is net-shorter and removes or
minimally restates statements that the deletion falsified; no new explanatory prose,
no style polish. A phrase-grep of the public tree (`src/Confluent.Kafka/*.cs`, excluding
`Internal/`) for `max.block.ms` / "admission expir" / "BufferExhaust" / "stayed
saturated" / "refuse" returns **no surviving expiry claim**. See 73.1(c) for the one
public-surface sentence that over-claims in the *new* direction.

---

## Findings — ALL THREE RESOLVED, moved to `COMMENTS.DONE.73.md`

73.1 (MEDIUM, the `max.block.ms` truth-of-contract sites), 73.2 (MEDIUM, the closable
1/8 gap on `AbandonOnThreadFailure`'s gate cancel) and 73.3 (LOW, the orphaned helper)
were each fixed by Actor 73 and moved, with their resolutions, to
`COMMENTS.DONE.73.md`. No finding from this review remains open.

---

## Not filed (checked, and deliberately not raised)

* **`ffi-marshalling.md §A1`'s `max.block.ms`-expiry / bounded-admission mandate is now
  contradicted by the shipped code.** Per this round's scope this is flagged **only** if
  it is a functional defect, and it is not: no test fails, and the user-visible contract
  change is approved (D2). Recorded here as a pointer for whoever owns the rulebook, not
  as a finding.
* **`_spaceGate` not disposed** — correct by design (no `WaitHandle`/timer touched);
  disposing it would open a `.Token` `ObjectDisposedException` window at `:367`, which is
  the site written to swallow `OperationCanceledException` only.
* **`Submit` being `internal` and bypassing the throttle** — pre-existing shape, one
  test-only caller, and every use asserts a throw. Not a live hazard.
* **`_chainRecords` overflow at `int.MaxValue`** — unreachable while the bound holds
  (it would need >2^31 records appended-but-untaken); arithmetic is unchecked, so it
  would wrap rather than throw. Noted only because 73.2 exploits the same field.
* **The two order/bound tests spin a drainer `Thread` with no back-off** while the suite
  runs with `DisableTestParallelization = false`. Pre-existing from M11/P3.2, green 910/910
  here, and the alternative (a sleep) weakens the interleaving the tests exist to hit.
* **`AppendAfterStop_...`'s comment claiming it is "the ONE assertion that would catch an
  append charging itself twice"** — the append never succeeds in that test, so the claim
  is wrong; the property is actually covered by
  `Admission_PermitsAreReturnedExactlyOncePerRecord_AcrossRepeatedDrains`. Comment-only,
  no behavioural consequence — excluded by this round's scope.
* **Commit-message "228 `[DllImport]`" vs the measured 222/225** — bookkeeping, and the
  delta (zero) is what Mode A requires. Not a finding.

---

## Verdict

Three findings: **two MEDIUM** (73.1 truth-of-contract on the `max.block.ms` surfaces;
73.2 the closable item-3 gap), **one LOW** (73.3 orphaned helper). **No correctness,
memory-safety, teardown, ordering, or permit-accounting defect found.** The mechanism
reversal is sound: the accounting is structurally balanced with a single grow site, a
single clear site and a single take site; the bound is still a bound with the stated
`+W` slack; teardown releases every parked caller on both triggers and settles every
awaiter exactly once; and the ordering property now holds by construction rather than by
a fairness assumption the rulebook forbids.
