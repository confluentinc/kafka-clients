# Critic 72 — M11/P3.3 slice S1 · RESOLVED

Findings filed against `658b6e11..df97d0a9` (the admission bound) and resolved in
the S1 fix round. Nine findings: one High, two Medium, six Low. Every one judged
real. **72.3 is resolved WITHOUT a code change** (the Manager took it and directed
that the deviation be recorded, not implemented); the other eight all carry code.

**Verification for the whole round** — Rust first, then .NET, every mutation gated
on `0 Error(s)`:

| | result |
|---|---|
| `cargo build --features ffi` | ✅ |
| `dotnet build` (net462-via-ns2.0 + net8.0 + net10.0, lib + tests) | **0 Warning(s), 0 Error(s)** |
| `dotnet test -f net10.0` | **921/921**, 0 failed, no `Test Run Aborted` (×3 control) |
| `dotnet test -f net8.0` | **921/921**, 0 failed, no `Test Run Aborted` |
| `dotnet format --verify-no-changes` | clean, exit 0 |
| `cargo xtask format-check` / `cargo xtask lint` (repo root) | ✅ / ✅ |
| Mode A | `git diff --name-only 658b6e11..HEAD` outside `bindings/dotnet/` = **empty**; `internal static extern` = **219** (unchanged) |

Suite count 920 → **921**: one net-new test
(`Admission_WhenTheBoundStaysSaturated_FiresTheDeliveryCallback_WithThePlaceholder`);
the pre-existing expiry test was rewritten in place, not added.

**Perf — the bound still works, and these fixes did not move it.** Two of the
changes touch the queued path (72.6's `EnsureSubmitterRunning`) and the admission
branch (72.1), so the steady state was re-measured. ⚠ The absolute throughput on
this machine **today** is ~315k msg/s, not the 578.6k the Manager measured at
`df97d0a9`, so I ran a **back-to-back A/B against `df97d0a9` in the same
environment** rather than comparing to the recorded table — which is the only way
to separate "my change" from "the environment". Same invocation both legs
(`ASYNC=True`, `WARMUP_SECONDS=3`, `TEST_DURATION_SECONDS=15`, `CLIENT_VERSION=3`,
booleans literal `True`, no `LIMIT_RPS`):

| | throughput | p50 | p99 | RSS | CPU |
|---|---|---|---|---|---|
| `df97d0a9` (baseline, re-measured now) | 313,587 msg/s | **87 ms** | 117 ms | 199,804 KiB | 252 % |
| **this fix round** | 316,002 msg/s | **86 ms** | 119 ms | 198,859 KiB | 265 % |

Throughput +0.8 %, p50 −1 ms, RSS −0.5 % — **no regression**, all inside run-to-run
noise. And the *latency/RSS* regime the acceptance target is about
(PLAN §8.3: "p50 and RSS back to the P3.1 row's order of magnitude") matches the
Manager's recorded `df97d0a9` row almost exactly (84 ms / 219 MB there, 86 ms /
~204 MB here), so the bound is intact; only the absolute msg/s differs, and the
baseline leg proves that difference is the environment, not the code. (A first run
at the Makefile **defaults** — 600 s + 120 s warmup — gave 319,894 msg/s / p50 86 ms
/ RSS 210,394 KiB at 624 MiB/s sustained for 10 minutes, i.e. disk-bound; the
matched 15 s shape above is the comparable one.)

---

## 72.1 · HIGH · the `max.block.ms` expiry diverged from Java on three axes, and five doc blocks asserted it did not — RESOLVED (adopted Java's shape)

**Was:** admission expiry threw `AdmissionTimedOut()` synchronously out of `Send`,
fired no delivery callback, and carried `Code == 0` / `IsRetriable == false`.

**Verified the Java contract myself first, as the Manager directed — every clause
re-read in `kafka/clients/src/main/java/org/apache/kafka/`:**

| claim | file:line | verified |
|---|---|---|
| the accumulator-memory wait's expiry throws `BufferExhaustedException` | `clients/producer/internals/BufferPool.java:161` | ✅ |
| reached from `doSend`'s `try` via `accumulator.append(…, maxTimeToBlock)` | `internals/RecordAccumulator.java:333` ← `producer/KafkaProducer.java:1029-1030` | ✅ |
| that wait's budget derives from `max.block.ms` | `KafkaProducer.java:995` (`remainingWaitMs`), `:423`/`:509` (`maxBlockTimeMs` ← `MAX_BLOCK_MS_CONFIG`) | ✅ |
| `BufferExhaustedException extends TimeoutException` | `clients/producer/BufferExhaustedException.java:29` (⚠ **not** in `common/errors/`, where the Critic's path pointed) | ✅ |
| `TimeoutException extends RetriableException` | `common/errors/TimeoutException.java:22` | ✅ |
| `RetriableException extends ApiException` | `common/errors/RetriableException.java:22` | ✅ |
| so it lands in `catch (ApiException e)`, which fires the callback with the `-1` placeholder **and returns a failed future without rethrowing** | `KafkaProducer.java:1049-1061` — callback at `:1051-1055`, `new RecordMetadata(tp, -1, -1, NO_TIMESTAMP, -1, -1)` at `:1053`, `return new FutureFailure(e)` at `:1061` | ✅ |
| code 7 `REQUEST_TIMED_OUT`'s Java exception **is** `TimeoutException` | `common/protocol/Errors.java:195` | ✅ |
| the core classifies code 7 retriable | `src/common/protocol/errors.rs:429`, asserted at `:813` | ✅ |

So the finding is right on all three axes and it is a defect, not a preference.
**Adopted Java's shape (the Critic's option (a)).**

**Fixed:**

- `SendAccumulator.Admit` / `AdmitSlow` now **return `bool`** instead of throwing
  on expiry. `SubmitAdmitted`'s expiry branch fires the delivery callback and then
  faults `completion`; `Send` returns normally.
- **Ordering:** `Fire` **before** `TrySetException` — ffi §A6 form C / M14/P1 D3,
  Java's `ProducerBatch.java:303-323` (value → callbacks → `done()`).
- **⚠ The -1 placeholder trap the Manager flagged: avoided by construction.** The
  expiry branch calls `delivery?.Fire(metadata: null, failure: expired)` and lets
  **`DeliveryRegistration.Fire` build the placeholder** — the one site that owns
  that construction (`IDeliveryCallback.cs` → `DeliveryRegistration.cs:128`). No
  second placeholder construction exists anywhere, so the M14/P1 failure mode (a
  negative-partition placeholder throwing *inside* `Fire`'s no-throw swallow, making
  the callback's effect silently absent) is not reachable here. The new callback
  test's `Assert.Equal(1, callback.Count)` is the assertion that would have caught
  it, and it is red under mutation N1 (see below).
- **Classification:** `AdmissionTimedOut()` now uses `KafkaException`'s **internal
  classified ctor** (`KafkaException.cs:90`) with `isRetriable: true`,
  `isFatal: false`, and code `7`. Code 7 is a named private const
  (`RequestTimedOutCode`) at its single use site — **not** a new binding-wide
  `ErrorCode` enum, which ffi §A5 lists as an anti-pattern ("a
  confluent-kafka-dotnet-style `Error`/`ErrorCode` object").
- **The synchronous-throw category is undisturbed**, as the Manager required.
  Teardown (`ObjectDisposedException`) and cancellation (`OperationCanceledException`)
  still throw out of `Send` — Java throws for a closed producer too (an
  `IllegalStateException`, not an `ApiException`), and the caller's `Send` has not
  returned yet in either case. `SendViaPump` is **still not `async`**
  (`NativeProducer.cs`, verified in the diff).
- **Docs corrected in every place that asserted this was Java's shape:**
  `SendAccumulator.AdmitSlow` remarks, `SubmitAdmitted`'s summary +
  `<exception>` list (the `KafkaException` tag is **removed** — it no longer
  throws one), `AdmissionTimedOut`'s remarks, `IAsyncProducer` (type remarks, both
  `Send` overloads' remarks, the `<returns>`, and the removed `<exception>` tag),
  `AsyncKafkaProducer`'s remarks, `NativeProducer.SendViaPump`'s summary + the
  `delivery` param + its `<exception>` list, `SendAccumulatorSettings.MaxBlockMs`,
  and `IDeliveryCallback`'s D5 outcome list + `OnCompletion`'s summary. Each
  correction states what the old text claimed, so the change is legible rather than
  silent.

**⚠ Two Java behaviours in that `catch` block that this binding has NO surface
for — recorded, not invented** (the Manager's constraint 3):

- `this.errors.record()` (`:1056`) — the producer's metrics are the **core's**
  metric map, read through `kafka_producer_Producer_metrics` (M11/P8). The binding
  keeps no error counter of its own, and adding one would be new public surface
  with no ABI backing.
- `interceptors.onSendError(…)` (`:1057`) — producer interceptors are **deferred**
  (`bindings/dotnet/CLAUDE.md §4`, "Interceptors: Defer; reserve the Java-shaped
  name"). There is nothing to notify.

Both are stated in a code comment at the expiry branch, so a future reviewer
comparing against `:1049-1061` finds the gap explained rather than missing.

**Tests.**

- `Admission_WhenTheBoundStaysSaturated_FailsAfterMaxBlockMs` →
  **`…_FaultsTheSendAfterMaxBlockMs_WithoutThrowing`**. It now awaits the *outer*
  task for completion (that await **is** the "does not throw" assertion — the outer
  task carries whatever `SubmitAdmitted` threw, so a synchronous throw makes the
  test red there) and the *inner* task for the failure. Adds
  `IsRetriable`/`IsFatal`/`Code` assertions; keeps the elapsed-time discriminator
  and the three message assertions (DoD §3).
- **New:** `…_FiresTheDeliveryCallback_WithThePlaceholder` — exactly-once **after a
  settle window** (ffi §A6 form C), non-null placeholder metadata with `-1`
  offset/timestamp, and `Assert.Same` on the exception object shared with the
  awaiter. Async surface only, with the reason stated at the site: the admission
  bound exists only on the async surface, so ffi §A6's "both flavors" rule has
  nothing to run on the sync side here.
- **`Admission_IsBounded_WhenTheFloodOutrunsTheDrain`** (the phase's own §8.1 gate)
  **had to change and did**: it counted refusals as `catch (KafkaException)` and
  went red (`Expected 16, Actual 48`) the moment expiry stopped throwing. It now
  counts a refusal as a **faulted returned `Task`**, which is deterministic —
  `TrySetException` transitions the task synchronously and
  `RunContinuationsAsynchronously` defers only continuations, so `IsFaulted` is
  observable the instant `AppendOne` returns. It also observes `Exception` (no
  `UnobservedTaskException`) and fails loudly if a refusal faults with the wrong
  type. All four of its assertions are unchanged in substance.

**Mutation evidence (regime: in-suite, full `dotnet test -f net10.0`; every run
gated on `0 Error(s)` first; each mutation applied alone and reverted from a
byte-exact backup, verified by `grep -c MUTATION == 0`):**

| # | mutation | detected | killed by |
|---|---|---|---|
| N1 | delete `delivery?.Fire(…)` from the expiry branch | **3/3** | `…_FiresTheDeliveryCallback_WithThePlaceholder` |
| N2 | restore the pre-fix `throw AdmissionTimedOut()` | **3/3** (3 tests red) | `…_FaultsTheSendAfterMaxBlockMs_WithoutThrowing`, `…_FiresTheDeliveryCallback_…`, `Admission_IsBounded_WhenTheFloodOutrunsTheDrain` |
| N3 | `isRetriable: false` | **3/3** | `…_FaultsTheSendAfterMaxBlockMs_WithoutThrowing` |

---

## 72.2 · MEDIUM · the teardown-permit test measured a counter the release path also zeroes (0/8) — RESOLVED

**Was:** `Admission_TeardownFlushedSendsReturnTheirPermits_NotJustThePermitBackedOnes`
claimed `Assert.Equal(0, AdmittedRecordCount)` measured the leak direction. It
cannot: `AdmittedRecordCount` is `_queued + _chainRecords` and `TakeChainLocked`
zeroes `_chainRecords` whether or not the `ReleaseAdmission(admitted)` beside it
runs, so it reads 0 either way.

**Accepted in full; the Critic had already found the one-line fix.** Added
`Assert.Equal(8, harness.Accumulator.AvailableAdmissions)` after `Stop`, and
rewrote the comment to name **which direction each assertion covers** —
over-release via `Assert.True(Stop(...))` (a `SemaphoreFullException` makes `Stop`
report `false`), leak via `AvailableAdmissions`, with the explicit note that
`AdmittedRecordCount` covers neither.

**Mutation re-run at the bar the Manager set — K=8, regime in-suite, fresh harness
per attempt** (the test constructs its own `Harness`, so every rep is cold):

| # | mutation | before | after |
|---|---|---|---|
| M2 | move `_chainRecords++` into `Append`'s `if (chargedToBound)` block | **0/8** (Critic's measurement) | **8/8** |

Build gated at `0 Error(s)` before every rep, so the ratio is not a stale-binary
artefact. The failure is the intended test and the intended values —
`Admission_TeardownFlushedSendsReturnTheirPermits_NotJustThePermitBackedOnes`,
`Expected: 8, Actual: 5` — i.e. exactly the three leaked permits the Critic
predicted. HEAD control in the same tree: **921/921 × 3**.

Same class as my own MP5 self-catch last round, and the Manager is right that the
instinct transfers: *when a test asserts a resource came back, the witness must be
the resource's own counter, not a bookkeeping counter the return path also clears.*

---

## 72.3 · MEDIUM · the fairness rationale answered ordering and left starvation unaddressed — RESOLVED **as a recorded deviation, no code change**

**The Manager took this finding and ruled on it:** the Critic is right and the plan
is wrong — the claim "a blocking admission doesn't need a fair primitive" is correct
about **ordering** and wrong about **starvation**. `BufferPool` is FIFO-fair by
contract (`BufferPool.java:40`, verified: *"It is fair. That is all memory is given
to the longest waiting thread until it has sufficient memory"*), `SemaphoreSlim` is
not and can barge. The Manager is fixing the PLAN §7 / §9 text in S2 and directed
**explicitly**: *do not implement fairness this phase; record it as an explicit
deviation at the admission site.*

**Done, exactly that.** The `_admission` field's ⚠ paragraph is split in two:

- **ordering** — the claim is narrowed to what it proves (a blocked caller has at
  most one send in flight, so its own sends cannot invert; per-caller order is all
  Java promises, `ProducerConfig.java:274`; the FIFO submission queue carries it
  independently);
- **starvation** — recorded as a **divergence from Java**, not parity: Java's pool
  is fair by contract with the citation, `SemaphoreSlim` is not and its documented
  barging mechanism is named (`Admit`'s `Wait(0)` fast path can take a permit a
  `Release` just freed while an older `AdmitSlow` waiter is being woken), the
  observable consequence is stated (the oldest caller times out at `max.block.ms`
  while newer ones succeed), and the acceptance is justified rather than asserted —
  bounded by `max.block.ms`, and **after 72.1** the outcome is a *retriable failed
  `Task` plus a delivery callback*, which is precisely what Java produces on genuine
  exhaustion, so the starved caller's recovery path is the one Java gives it.
  Closing with "a fair primitive is deliberately out of scope for this slice, not a
  claim that it would be wrong."

So nobody can later read the code as claiming parity here. **Moved to DONE with
that rationale, not as "fixed".**

---

## 72.4 · LOW · two new uniqueness claims were already false — RESOLVED (quantifiers deleted)

`internal void Submit(...)` is still `internal`, still appends, and still takes no
admission permit, so both new quantifiers were false on arrival:

- `TryAdmitAndSubmitInline`'s *"The **only** other way into the accumulator"* →
  now states the local fact: *"It takes the admission permit itself, so admission
  accounting stays whole without the caller reaching `TrySubmitInline` directly."*
- `TrySubmitInline`'s *"The two ways in are `SubmitAdmitted` (blocking) and
  `TryAdmitAndSubmitInline` (non-blocking)"* → **deleted**; the paragraph keeps only
  its own local fact (the caller must already hold a permit, and why).

**Deleted rather than re-scoped**, per the M14/P1 lesson the Manager restated: a
claim not made cannot go stale, and deletion is strictly shrinking so it cannot
introduce a new false clause. I did **not** take the Critic's alternative (privatise
`Submit`, route `AppendWithoutAPermit` through a purpose-named internal) — that is a
bigger change than the claim is worth, and the Critic said so too.

---

## 72.5 · LOW · the peak-population arithmetic was wrong, and three statements said "the core" where the bound counts "the batch thread" — RESOLVED

**The Critic refuted my `cap + BatchChunk` claim and is right about the
magnitude.** The overshoot is the **in-flight chain** — permits are released for the
whole taken chain — so it is bounded by the appends possible between two takes:

    overshoot <= min(MaxAdmittedRecords, MaxAccumulatedRecords) + the teardown bypass

**min**, because a permit-backed append needs a `_space` permit and an
admission-charged one needs an `_admission` permit; the bypass appends are the only
ones outside `_space`. At the shipped defaults (`MaxAccumulated` 1000, cap 5000,
`BatchChunk` 1100) that is **~1000**, so peak accepted-but-unsent is **~6000**.
`BatchChunk` bounded it only by coincidence at those values (1000 < 1100), and
`CONFLUENT_KAFKA_PRODUCER_BATCH_CHUNK` is an independent override clamped only from
above, so `MAX_ACCUMULATED=10000` + `BATCH_CHUNK=100` gives an overshoot of 10000,
not 100. **It scales with `MaxAccumulatedRecords`, not with the chunk** — written at
the site in that form because the Manager is sizing S2's sweep off it.

The `_space`-symmetry half of my claim is confirmed and kept (both release at
`TakeChainLocked`, so both exclude the in-flight chain by construction).

Second half: *"not yet handed to the **core**"* → *"…to the **batch thread**"* in all
three user-facing places — `IAsyncProducer`'s type remarks, `NativeProducer.SendViaPump`'s
summary, and **the exception message a user reads** (`AdmissionTimedOut`). That
matches `MaxAdmittedRecords`' own doc, which was the accurate one. The message's
asserted substrings (`max.block.ms`, `250 ms`, `2 records`) are unchanged, so the
DoD §3 assertions still hold.

---

## 72.6 · LOW · `SubmitQueued` could throw after queueing, over-releasing an admission permit — RESOLVED (code, marked defensive)

**Was:** `SubmitAdmitted`'s `finally` released the permit whenever the routing call
threw, on the stated ground that *"SubmitQueued does not throw"*. It can:
`EnsureSubmitterRunning`'s `Task.Run` can fail **after** the enqueue, at which point
the permit is already owed by `ReleaseQueuedSlot` (or the chain's take) — so the
`finally`'s release is an **over-release**, i.e. a `SemaphoreFullException` on the
batch thread, the very failure `AbandonOnThreadFailure` exists to survive. The same
throw also left `_submitterRunning` latched at 1 with nothing to drain the queue.

**Fixed with the one line that closes both**, as the Critic suggested:
`EnsureSubmitterRunning` now wraps the `Task.Run` and, on a throw, resets
`_submitterRunning` to 0 (via `Interlocked.Exchange`, matching
`RunSubmitterAsync`'s own release — both sides of the handshake stay full fences)
and does not let the throw escape. A later `SubmitQueued`, or `Stop`'s own call,
restarts the loop and drains FIFO from the front; the terminal
`SettleQueuedSubmissions` sweep settles whatever is left. `SubmitAdmitted`'s
`finally` comment was corrected too — it no longer claims `SubmitQueued` cannot
throw, and states who owes the permit once a submission is queued.

**⚠ Not claimed as test-covered, and the branch is marked defensive in the code
with the reason.** `Task.Run(Func<Task>)` always queues to `TaskScheduler.Default`
rather than to an ambient scheduler, so no fixture can install a rejecting one;
what reaches the `catch` is an `OutOfMemoryException` or a thread-pool queue
failure, neither of which a broker-free test can schedule. Per the Manager's
instruction I say that explicitly rather than claiming coverage. The repo's
standard of care is to spend a line of code on an OOM residual rather than argue it
away (ffi §A6 form C's residual list names `OutOfMemoryException` explicitly).

---

## 72.7 · LOW · "every reachable refusal … happens during teardown" — RESOLVED (universal deleted, both copies)

The cancelled-queued-submission release (`AppendQueuedAsync` → `ReleaseQueuedSlot(false)`)
is **not** a teardown path and **does** have a behavioural observable — the Critic
measured it detected 3/3 by
`Admission_QueuedSubmissionSettledWithoutAppending_ReturnsItsPermit`, whose witness
is behavioural, not `AvailableAdmissions`. Only the `every` was wrong.

Deleted the universal in **both** copies, which is where this class usually leaks:

- `AvailableAdmissions`' remarks → *"On the two refusal paths that need it —
  `SubmitAdmitted`'s `finally` and `SubmitQueued`'s sealed refusal — teardown has
  also cancelled `_spaceGate` …"*;
- the test comment in `Admission_RefusedSubmit_ReturnsItsPermit_OnBothRoutes` →
  scoped to "BOTH refusal paths below", plus an explicit ⚠ naming the cancelled-submission
  path as the counter-example and pointing at the test that covers it behaviourally.

---

## 72.8 · LOW · "Three synchronous failures reach here" over a `catch (Exception)` — RESOLVED (count dropped)

The `catch` is `catch (Exception)`, so an unexpected failure reaches it too and the
count was false. Dropped the count; kept the bullets and the shared conclusion
("all of them nothing-reached-the-core, so no delivery callback"), and said
explicitly that the `catch` is deliberately broad and the conclusion holds for the
unexpected cases too — *which is why this note describes the paths rather than
counting them*. 72.1 also removed one of the three bullets (the expiry no longer
throws), and the note now says so with a pointer to `SubmitAdmitted`.

---

## 72.9 · LOW · `max.block.ms` was read lazily off the caller's live dictionary — RESOLVED (eager read; `_config` gone)

**Was:** `NativeProducer` retained the caller's `IReadOnlyDictionary` and
`FromEnvironment(_config)` ran inside `EnsureAccumulator`, i.e. on the **first async
send**. `IReadOnlyDictionary` does not stop its owner mutating the underlying
`Dictionary`, so a user editing their config map between construction and first send
silently changed the effective `max.block.ms` while the core had already been
configured from the values read at construction — and it pinned the user's
dictionary for the producer's lifetime.

**Fixed as the Critic asked, the "read it once there and store the `int`" variant:**
`SendAccumulatorSettings.ReadMaxBlockMs` is now `internal`, `NativeProducer.Create`
calls it once and stores `private readonly int _maxBlockMs`, `CreateMock` passes
`DefaultMaxBlockMs`, and `EnsureAccumulator` calls a new
`FromEnvironment(int maxBlockMs)` overload. **`_config` is gone.** The existing
`FromEnvironment(IReadOnlyDictionary?)` is kept as the composition of the two, so
the ~4 config-key parsing tests are untouched; its doc now says production does not
use it and why.

**No behavioural test, deliberately, and the reason is stronger than "hard to
test":** the fix converts a behavioural hazard into a **structural impossibility** —
the producer no longer holds a reference to the dictionary, so there is nothing left
to re-read lazily. That is a compile-time property, not one a test could usefully
assert. The environment overrides stay late-bound on purpose
(`SendAccumulatorSettings` documents why); the comment at `_maxBlockMs` records that
a *user config value* is not in the same class, which was the Critic's point.

---

## Carried to S2 — Manager-owned, NOT actor-resolvable

Recorded here so they are not lost; none is a code change and none is mine to make:

1. **PLAN §7 option A row** (*"Timeout → `KafkaException`"*) and **§9's proposed
   `ffi-marshalling.md` §A1 amendment** both still carry 72.1's false premise. The
   Critic asked for the correction in S2, before §9 becomes a rule. Not edited here:
   the PLAN is an approved archived artifact and the Manager has already claimed
   §7/§9 for the 72.3 narrowing.
2. **PLAN §7 recommendation 5's ordering** ("cancel the gate *before* the
   seal/flush") is wrong and, read literally, would regress M11/P3.2 §F2 — the
   Critic confirmed both halves of my finding independently. Also S2.
3. **`bindings/dotnet/CLAUDE.md §4`'s delivery-callback row** asserts *"There is
   **no analogue** of Java's `catch (ApiException)` row … a deviation forced by the
   ABI"*. After 72.1 there **is** one: the admission expiry fires the callback and
   returns a failed future without throwing, and it is not ABI-forced because it is
   binding-generated. I have **not** edited `CLAUDE.md` — agents do not change it
   (root `CLAUDE.md`: *"Any change to this prompt is to be avoided by automatic
   agents"*); this is filed as a suggested rule update per `agent-roles.md`. The
   Critic's own suggested-update #2 (a binding-generated `KafkaException` must carry
   the Java classification, never the message-only ctor) is the same edit and is now
   **satisfied by the code**, so it is a documentation gap only.

---

# Round 2 — RESOLVED (findings `df97d0a9..80eb685e`, fixed in `6859c4c0` + `23ef456b`)

Seven findings: one Medium (72.10), six Low. All seven judged real; all seven fixed
this round at the Manager's direction (no deferrals — slice S2 writes the permanent
`ffi §A1` rule and the STATUS record against this code's comments, so a false claim
left standing gets baked into a rule).

**Verification for the round** — Rust first, then .NET, every mutation build-gated
on `0 Error(s)`:

| | result |
|---|---|
| `cargo build --features ffi` | ✅ |
| `dotnet build` (netstandard2.0 + net8.0 + net10.0 lib, net462 + net8.0 + net10.0 tests) | **0 Warning(s), 0 Error(s)** |
| `dotnet test -f net10.0` | **922/922**, 0 failed, `Test Run Aborted` count = **0** |
| `dotnet test -f net8.0` | **922/922**, 0 failed, `Test Run Aborted` count = **0** |
| `dotnet format --verify-no-changes` | clean, exit 0 |
| `cargo xtask format-check` / `cargo xtask lint` (repo root) | ✅ / ✅ |
| Mode A | `git diff --name-only` outside `bindings/dotnet/` = **empty**; `internal static extern` = **219** (unchanged) |
| 72.15 mutation (D3 inverted) | **8/8 detected** — see 72.15 below for the regime |

921 → **922**: one net-new test
(`Admission_WhenTheBoundStaysSaturated_FiresTheCallbackBeforeFaultingTheTask`).

**Perf: not re-measured, and the reason is structural rather than a judgement about
noise.** The whole `src/` diff for this round is **documentation only** — verified
mechanically, not asserted: `git diff 80eb685e..HEAD -- bindings/dotnet/src` filtered
to non-comment, non-blank lines is **empty**. Comments do not reach codegen, so there
is no mechanism for a steady-state change. The only other change is a new test plus
one test-harness helper, neither on any production path.

---

## 72.10 · MEDIUM · the async surface's delivery-callback THREAD contract was false — RESOLVED as docs, behaviour unchanged (Manager's ruling)

**Was:** `IDeliveryCallback`'s async bullet promised *"a single thread per producer,
so callbacks of one producer never run concurrently with each other"*, and the sync
bullet used the *absence* of that guarantee as the reason a shared callback must be
thread-safe. The admission-expiry branch fires on the caller's own thread, so two
expiring senders — or one expiring sender and the pump — can enter one shared
instance at once.

**Resolved as a documentation/contract fix, on the Manager's explicit ruling, with
the threading behaviour untouched.** The behaviour is Java's: `doSend`'s
`catch (ApiException)` invokes `callback.onCompletion` synchronously on the
application thread while the Sender thread may be running others, and Java
guarantees per-partition *ordering*, never cross-thread non-concurrency. The Critic
agrees the behaviour is faithful. Routing the expiry callback through the pump would
need a side-channel (the record never reached the core, so there is no future for the
pump to read), would delay the callback and the `Task` fault behind the pump's current
blocking `get_all`, and would add a queue in the very area this phase exists to bound;
a shared lock would hold a lock across user code.

**How the correction is written** (this is the part that mattered — the other six
findings are all the same failure mode):

  - the **positive** rule: the invocations the pump makes are serialized with respect
    to one another, and a slow one there delays the other completions;
  - the other sites are **named, not counted** — a record the core rejects as the batch
    is handed over is delivered from the **send-batch thread**, and the admission
    expiry **inline on the thread that called `Send`**;
  - **no new quantifier** anywhere (ffi §A6's round-5 amendment: delete, do not
    re-scope — a claim not made cannot go stale). "The only site", "the two paths",
    "a third site" all deliberately absent;
  - the user obligation is stated plainly and once, after the list, covering both
    surfaces: **a callback instance shared across sends must itself be thread-safe**,
    because an invocation made away from the pump can overlap one the pump is making
    and two callers whose admission expires together enter it at the same instant;
  - a ⚠ line records that the guarantee *was* stated unqualified and what falsified it.

**The two dependent bullets were repaired with it**, rather than leaving a corrected
headline over stale support:

  - **reentrancy** — the mechanism is now "no managed lock is held while the callback
    runs, wherever it runs", with the expiry site's own shape stated: the reentrant
    `Send` runs **synchronously inside** the outer `Send`, on that thread, and can
    itself block for another `max.block.ms` and expire. Since the outcome is
    deliberately retriable, retry-from-inside is the shape it invites, so the doc says
    to bound the retry rather than recurse.
  - **teardown** — scoped to "while it is running on the send-completion pump thread",
    which is where the self-join hazard actually lives; where it runs inline on a
    caller's thread that hazard does not arise, and the advice is unchanged.

**⚠ One correction to the finding's premise, which strengthens rather than weakens it
(evidence below).** 72.10 and the Manager-owned `CLAUDE.md` item 4 both describe the
expiry as adding a **third** firing site, the first *async* one off the pump. It is not
the first: `SendAccumulator.CompleteNode`'s per-record-rejection branch (`:2388`) and its
null-future branch (`:2398`), plus `FaultNode` (`:2509`), all call
`DeliveryRegistration.Fire` on the **send-batch thread**, which runs concurrently with
the pump — and `CompleteNode`'s rejection branch is an ordinary outcome, not a
defensive one. So the unqualified non-concurrency guarantee was **already false before
this phase**, since M11/P3.1 gave the async surface a batch thread. This is why the
correction names the batch-thread site too: fixing only the caller-thread clause would
have left exactly the kind of copy that produces the next round's finding. It also means
the `CLAUDE.md §4` *Thread* bullet needs both sites, not one — carried below.

**Checked, as the Manager asked: nothing in the codebase relies on the
non-concurrency guarantee internally.** `DeliveryRegistration` holds three `readonly`
fields and no mutable state; `Fire` builds the placeholder, coerces the exception,
calls the user, and catches everything — it is re-entrant and thread-safe by
construction. The guarantee was a promise made *to users*, never an internal
invariant, so there is no design fork here.

---

## 72.11 · LOW · `KafkaException`'s classified ctor claimed to have one caller — RESOLVED (quantifier deleted, both sides)

**Was:** *"Used by `FromHandle(IntPtr)` — the only place a classified
`KafkaException` is constructed"*, while `AdmissionTimedOut` now calls the same
4-arg ctor.

**Fixed by deleting the quantifier**, not re-scoping it: the ctor's summary now says
what it *is* (an already-classified error), and names both kinds of caller —
`FromHandle`, which copies the values out of a core error handle, and the binding's
own classified failures, which carry the classification Java gives the exception they
stand in for.

**Its contradicting twin is deleted too.** `AdmissionTimedOut`'s remarks said *"This
is the first binding-generated classified `KafkaException`; every other one comes
from `FromHandle`"* — the Critic's point that one side was updated and the other not.
Rather than re-word it into agreement, the comparative is gone: the remark now states
the local fact (the classification is binding-generated here **because** there is no
core error handle to copy it out of — the core never saw this record). Two quantifiers
that could disagree are now two facts that cannot.

---

## 72.12 · LOW · "the one outcome the binding generates itself that still notifies" — RESOLVED (false; deleted)

**Was false, with two counterexamples in the same file**, both confirmed by reading
the code: `CompleteNode`'s `future == IntPtr.Zero` branch (`:2398`) builds its own
`KafkaException` and fires, and `FaultNode` (`:2509`) fires for every index where the
core never accepted the record. Both are binding-generated failures with no core
completion in hand.

**Fixed by deletion, keeping the local fact** — the clause now reads "The binding
generates this failure *itself*, and firing here is Java-faithful rather than an
exception to the rule above", followed by the unchanged `BufferExhaustedException`
citation chain that was the part earning its place.

**The same paragraph's headline was carrying the same claim** ("and so does **the one**
failure the binding generates for which Java also fires") and is re-stated without the
count: "and so does the `max.block.ms` admission expiry, a failure the binding
generates and for which Java fires too". Strictly shrinking, per the Manager's
instruction; this is the canonical enumeration, where a count is *permitted* but must
be true, and the safest true form here is no count.

---

## 72.13 · LOW · "Two things Java's catch does…" over an incomplete enumeration — RESOLVED (count dropped, third item added)

**Confirmed against `kafka/` 4.3.1:** `KafkaProducer.java:1058-1060` is
`if (transactionManager != null) transactionManager.maybeTransitionToErrorState(e);`
— a third thing the `catch (ApiException)` block does that this binding has no
surface for.

**Fixed:** the count is gone ("Things Java's catch does … — including …"), and the
third item is there with its reason, which is the strongest of the three: transactions
are **not exposed at all** by this binding (`CLAUDE.md §1`, quoted at the site), so
there is no transaction manager to transition and no producer here can be
transactional. The Critic's own counter-argument is answered by the same token — the
`if (transactionManager != null)` guard makes it a no-op for a non-transactional
producer, exactly as `interceptors.onSendError` is a no-op with no interceptors
registered, and the comment lists that one.

---

## 72.14 · LOW · 72.5's correction missed a public copy — RESOLVED, and the sweep found two MORE the phrase-grep could not

**Was:** `AsyncMockProducer` still said the bound counts records *"accepted but not
yet handed to the **core**"*. 72.5 established that as wrong — the admission permit
comes back at `TakeChainLocked`, i.e. when the batch thread **takes the chain**,
before `send_batch` runs — and corrected three other copies.

**Swept structurally this round, as the Manager directed** (enumerate every producer
public surface and check each, rather than grepping the one phrase). The enumeration is
`IAsyncProducer`, `AsyncKafkaProducer`, `AsyncMockProducer`, `IProducer`,
`KafkaProducer`, `MockProducer`, `IDeliveryCallback`. Result:

  - `AsyncMockProducer:81` — the flagged copy. **Fixed** → "not yet handed to its
    send-batch thread".
  - `AsyncKafkaProducer:79` — *"its bound of **accepted-but-unsent** records"*. **Not
    reachable by the finding's grep** (`handed to the core|not yet handed`), and wrong
    in the same direction, since a record the batch thread has taken is unsent yet no
    longer counted. **Fixed** to `IAsyncProducer`'s corrected wording.
  - `NativeProducer:601` (internal, same sweep) — *"MaxAdmittedRecords
    accepted-but-unsent records"*. **Fixed**, and it now states the difference
    explicitly: the permit returns at `TakeChainLocked`, so the accepted-but-unsent
    *population* exceeds the bound by the in-flight chain.
  - `IAsyncProducer:62` and `NativeProducer:449` — already correct (72.5).
  - `IProducer` / `KafkaProducer` / `MockProducer` — **no** admission text, which is
    right: the sync `Send` has no admission window.
  - `IDeliveryCallback` — names the expiry but never describes what the bound counts.

**Deliberately left:** `SendAccumulator:136`, `:2028`, `:2037` use
"accepted-but-unsent" for the **population**, which is the correct term there — `:2028`
is the overshoot arithmetic that exists to say the population exceeds the bound by the
chain. Changing those would have broken a true statement.

This is the lesson: a keyword sweep is what let the fourth copy survive three rounds,
and it would have missed the fifth and sixth too, because they use a different phrase
for the same wrong idea.

---

## 72.15 · LOW · the expiry site's D3 ordering was uncovered — RESOLVED (test added; mutation 8/8)

**Confirmed:** the ordering is correct in the code (`Fire`, then `TrySetException`,
unconditional, guarded in one place), and nothing tested it.

**Added `Admission_WhenTheBoundStaysSaturated_FiresTheCallbackBeforeFaultingTheTask`**
— ffi §A6 form C's required **deterministic** probe for this site: the send awaiter's
own `IsCompleted`, read from *inside* `OnCompletion`. `TrySetException` transitions the
`Task` synchronously (`RunContinuationsAsynchronously` defers only the *continuation*),
so the probe is exact rather than a ticket race.

**Why a new harness helper was needed, and why it is not a fixture that fixes the bug.**
The public probe shape (`probe.Task = sendTask` *after* `Send` returns) works only
because the pump fires later; this callback fires **inside** the submit call. So
`AppendOneObservingItsCompletionFromAnotherThread` is `AppendOne` with the TCS created
first and its `Task` handed to the probe — and nothing else: the record, the awaiter,
the `DeliveryRegistration` and the single `SubmitAdmitted` entry point are all
production's (DoD §12), and the helper hands over `completion.Task` only, never the
source, so it cannot settle anything itself.

**Measured — mutation D3-inverted (the two statements swapped):**

| | |
|---|---|
| mutation | `completion.TrySetException(expired)` moved **above** `delivery?.Fire(...)` at `SendAccumulator.cs:554-555` |
| **ratio** | **8/8 detected** |
| **regime** | **in-suite full `dotnet test -f net10.0`**, K=8, isolated throwaway git worktree at `23ef456b`, **each rep independently build-gated on `0 Error(s)`**, fresh `Harness` per rep (each rep is a whole suite run), `Test Run Aborted` count **0** in every rep |
| killed by | the new test **only** — `Failed: 1, Passed: 921` every rep, no collateral |
| failure message | *"the record's Task was already completed when the delivery callback ran — the callback must fire BEFORE the awaiter is released (ProducerBatch.java:303-323)"* |
| control | mutation reverted in the same tree → **922/922 × 3**, and `grep -c 'MUTATION D3-inverted'` = **0** |

---

## 72.16 · LOW · 72.6's recovery enumeration omitted `Flush` — RESOLVED (claim corrected; behaviour deliberately unchanged)

**Confirmed:** `git grep -n EnsureSubmitterRunning` gives exactly two call sites,
`SubmitQueued` and `Stop`. `DrainPending` / `DrainPendingAsync` — the `Flush` path —
call neither, so neither can restart a submitter loop that failed to start.

**Corrected the claim at the site, as the Manager directed, rather than re-engineering
`Flush`.** The remarks now state the restart sources and the true bounded outcome:

  - **any later async send restarts it** — while anything is queued the inline route is
    refused, so that send reaches `SubmitQueued` and this method with it. (Worth stating
    precisely: the window closes on the *next send of any kind*, not only on another
    queued one.)
  - **a `Flush` landing in the window with no send behind it** leaves `_queued` at 1, so
    `IsEmptyAndIdleLocked` never holds: a `DrainPending` caller waits out its **whole
    timeout** and reports `false`, and `Flush`'s `DrainPendingAsync` — which
    `NativeProducer.FlushAfterDrain` awaits with **no timeout** — stays pending until the
    caller's own `CancellationToken` fires or teardown's sweep settles the queue.

⚠ **The finding slightly understates this, and the site now records the accurate
version.** "Waits out its full bound" is true of the *sync* `DrainPending(timeout)`;
the async `Flush` has **no** timeout (`NativeProducer.cs:862` says so in as many
words), so its bound is the caller's token or teardown, not a timeout. Still bounded,
and still not a hang.

**No behaviour change**, with the reason recorded at the site: adding an
`EnsureSubmitterRunning` call to the drain path would change `Flush` for a window
reachable only on the OOM / thread-pool-queue-failure path, which is well outside this
slice.

---

## Carried to S2 — Manager-owned, NOT actor-resolvable (updated)

Items 1–3 of the earlier list stand unchanged. Item 4 is the Critic's new one, with
one amendment from 72.10's evidence above:

4. **`bindings/dotnet/CLAUDE.md §4`'s delivery-callback *Thread* bullet** needs the
   same amendment as the row above it. It reads *"It runs on the producer's
   send-completion **pump thread** for the async surface … and **inline on the
   caller's thread** for the blocking sync surface"*, and its sub-divergence paragraph
   states *"The async pump is one thread **per producer**, so one producer's callbacks
   never overlap each other"* — which is false for the same reason the public xmldoc
   was. ⚠ **Amendment:** the bullet needs **both** off-pump async sites, not one. The
   admission expiry fires on the caller's thread, and `CompleteNode`'s per-record
   rejection has fired on the **send-batch thread** since M11/P3.1 — so the async
   non-concurrency claim was already false before this phase. Filed as a suggested rule
   update per `agent-roles.md`; I have not edited `CLAUDE.md` (root `CLAUDE.md`: *"Any
   change to this prompt is to be avoided by automatic agents"*), and the Manager has
   this with the user for approval.
