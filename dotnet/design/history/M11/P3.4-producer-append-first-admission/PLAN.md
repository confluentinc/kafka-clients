# M11/P3.4 — Producer append-first send admission (N = 73)

**Status:** APPROVED 2026-09-12. Actor 73 loop started.
**Branch:** `prashah_dev_producer_python_alignment` @ `d8ac7c50` (M11/P3.3 S2 close-out).
**Mode:** A (no Rust / ABI / header / cbindgen change; zero new `[DllImport]`).
**Agent pair:** `dotnet-actor` / `dotnet-critic`, N = 73.

---

## 1. What this phase is

Not a fresh translation. A hand-implemented **architectural spike**, authored directly at the
user's request to validate an idea and run perf comparisons, is being put through the formal
Actor/Critic loop for the first time.

The spike lives in one file, uncommitted at plan time:
`bindings/dotnet/src/Confluent.Kafka/Internal/SendAccumulator.cs` (+33 / −57).

It replaces the send-admission mechanism:

| | before (M11/P3.3, N=72) | after (this spike) |
|---|---|---|
| order of operations | take admission permit, **then** append | **append**, then take permit |
| wait bound | `max.block.ms` | none — indefinite |
| wait cancellable by caller's `cancellationToken` | yes | **no** (teardown gate only) |
| ordering mechanism | FIFO `ConcurrentQueue` + single submitter task | append order alone |
| `_admission` ceiling | `MaxAdmittedRecords` | `int.MaxValue` |

The anchor is the Python binding's C extension `Producer_send`, which appends under a mutex
first and only then computes/waits on backpressure.

`SubmitAdmitted` now calls `SubmitCore(record, completion, delivery, holdsPermit: false)`
unconditionally — bypassing `TrySubmitInline` / `SubmitQueued` and the whole FIFO machinery —
then `_admission.Wait(_spaceGate.Token)`, swallowing `OperationCanceledException` because the
record is already appended and `completion` must still reach the caller.

## 2. Why the review centre of gravity is "does the bound still bound?"

This spike **reverses the mechanism** M11/P3.3 (N=72) shipped four commits earlier, and
M11/P3.2 (N=71) before it. Those phases exist because an unbounded accepted-but-unsent
population measured **p50 3,524 ms / RSS 2.04 GiB**; N=72's blocking admission bound fixed it
to **p50 84 ms / RSS 219 MB / 578.6k msg/s**, throughput held.

Raising the ceiling to `int.MaxValue` removes a guard (`SemaphoreFullException`) that was
*reporting a real accounting asymmetry*. If the accounting has any net-positive drift per
drain, the gate silently stops blocking and that regression returns — invisible to the test
suite, because every record is still delivered, in order, exactly once.

## 3. Decisions (resolved by the user before the loop started)

**D1 — Delete the dead machinery, in this phase.** Chosen over leaving it in place or a
partial deletion; the user wants the clean end state even though it widens the diff well
beyond the spike. In scope for deletion: `SubmitQueued`, `EnsureSubmitterRunning`,
`RunSubmitterAsync`, `AppendQueuedAsync`, `Admit`, `AdmitSlow`, `AdmissionTimedOut`,
`ReleaseQueuedSlot`, `SettleQueuedSubmissions`, `FlushQueuedSubmissions`, the
`_queued` / `_submissions` state, `QueuedSubmission` if orphaned, and their tests.
`TryAdmitAndSubmitInline` (test-only; sole caller `SendAccumulatorTests.cs:3022`) goes too
unless something legitimately still needs it. This makes the phase a real refactor — which is
a reason for *more* proof on §4 A2/A4, not less.

**D2 — Losing `max.block.ms` and the caller's `cancellationToken` on this wait is an
intentional, already-approved contract change.** User sign-off: *"We can drop the timeout for
this wait entirely matching Python exactly... okay I agree with this behavior."* The Actor
deletes the unreachable timeout/cancellation tests with authority. N=72's finding 72.1
(expiry → fire delivery callback with the −1 placeholder, fault the `Task` retriably, never
throw out of `send()`) becomes unreachable **by design**. The Critic does not reopen it as a
design question — only checks it is implemented soundly.

## 4. Actor brief (A1–A6)

- **A1 — Blast radius.** Confirm/refute each member above has no production caller; then
  execute D1. Verified at plan time: the only production caller of the admission subsystem is
  `NativeProducer.cs:615` → `SubmitAdmitted`.
- **A2 — Permit-accounting audit.** Prove lifetime balance (releases == takes); characterise
  the transient overshoot (release-on-take precedes the caller's take); state the effective
  bound (expected: `MaxAdmittedRecords` + concurrently parked callers); rule out net-positive
  drift explicitly.
- **A3 — `_space` is now inert.** `holdsPermit: false` ⇒ `_accumulated` never moves and
  nothing calls `TryAcquireSpace`, so `CONFLUENT_KAFKA_PRODUCER_MAX_ACCUMULATED` is a dead
  knob on the send path. Confirm; check no teardown predicate or `Flush` path depends on it;
  apply D1's standard to `_space` / `WaitForSpaceAsync` / `ReleaseSpace` if they are dead too.
- **A4 — Teardown, above everything else.** Six numbered items, each with a test: parked-caller
  wake at `Stop:1720` before `_closed` at `:1734`; `Append`'s ODE path with `SubmitCore`
  outside any settling try; no ODE window on the never-disposed `_spaceGate`;
  `AbandonOnThreadFailure`'s unconditional cancel; permit residue; and `Stop` / `DrainPending` /
  `IsEmptyAndIdleLocked` re-verified after the deletion. **Hard requirement: every appended
  record's `TaskCompletionSource` settles exactly once even if teardown races a parked caller.**
- **A5 — Tests.** Update or delete every stale queue/timeout/cancellation test. New tests
  required: teardown-while-parked-post-append (both `Stop` and `AbandonOnThreadFailure`
  triggers); the bounded-acceptance gate re-pointed at the new bound, asserted on
  `AdmittedRecordCount` **and** `AvailableAdmissions`; append-order FIFO under concurrent
  callers. Mutation-proof **in-suite**, K=8 bursts, fresh harness per attempt, ratio always
  quoted with its regime. Mutate fixture and production **separately** (DoD §12).
- **A6 — Gates.** Build 0W/0E on the TFM matrix; `dotnet test -f net10.0` and `-f net8.0`
  green with real counts and no aborted run; `dotnet format --verify-no-changes`; no
  TODO/FIXME; no new steady-state send-path allocation (the old `Admit` fast path was
  deliberately allocation-free); Mode A verified, not assumed.

## 5. Critic brief

Focus, ranked: **(1) teardown/shutdown** — no hang, deadlock, stranded TCS, leaked permit, or
double-settle; **(2) permit accounting** — is `int.MaxValue` masking a real imbalance, is the
bound still a bound; **(3) ordering** under concurrent callers without the queue (and never on
a `SemaphoreSlim`-fairness argument — .NET guarantees no waiter ordering); **(4) races**
between a parked caller and a concurrent drain / teardown / batch-thread death; **(5) test
correctness** — stale, vacuous, or fixture-proving tests, unjustified removals, mutation ratios
quoted without their regime.

## 6. Scope restriction (user, verbatim — binds Actor and Critic alike)

> "I don't want actor OR critic to focus on doc strings, claude rules and non code related
> stuff. I just want the entire focus to be only on code changes, logic, teardown path etc...
> no time, effort and token wasted on docstring, claude rules etc (i.e. non code stuff)."

Out of scope this round: comment/docstring quality; any proposal to add or edit `CLAUDE.md`,
`bindings/dotnet/CLAUDE.md`, or anything under `.claude/rules/`; README / STATUS / design-doc
updates. Notably `ffi-marshalling.md §A1` currently mandates the `max.block.ms` expiry
behaviour and the bounded blocking admission this phase changes — **flag only if it is a
functional defect** (a failing test, or a user-visible contract silently changed without a
replacement), never as documentation drift. Known-and-accepted N=71/N=72 residuals (the
pre-broken `--autosquash`, the two parked `CLAUDE.md §4` corrections) are not findings.

## 7. Loop

Actor 73 → Critic 73 → (fix cycle) → Critic 73 → … until `COMMENTS.73.md` is empty and DoD is
green. Expect 2–4 rounds on this lineage (N=71: 15 findings / 6 slices; N=72: 17 / 1 slice).

**Close-out:** archive `COMMENTS.DONE.73.md` here; reset `COMMENTS.73.md`; update PM memory
(numbering + phase record); `marked_classes.txt` unchanged (Mode A, no Java classes).

## 8. Deliberately NOT in this phase

A perf re-measurement against N=72's baseline (p50 84 ms / 219 MB / 578.6k msg/s). The spike
exists for perf comparison and a correctness-only suite provably cannot see the regression
class this design touches — but measurement is Docker-and-broker dependent, and N=72's own D3
precedent put it outside the Actor/Critic loop. Add as a slice on request.
