# Critic 73 — RESOLVED (M11/P3.4, producer append-first send admission)

Actor 73 fix cycle. All three findings below are resolved and were moved here from
`COMMENTS.73.md`. Each entry is the Critic's original text, followed by a
**RESOLUTION** block naming the commit and what was actually verified.

Gate state at close: `cargo build --features ffi` clean; `dotnet build` 0 warnings /
0 errors across netstandard2.0 + net8.0 + net10.0 (library) and net462 + net8.0 +
net10.0 (tests); `dotnet test` **911/911** on net8.0 and net10.0, run completed (no
`Test Run Aborted`); `dotnet format --verify-no-changes` clean; Mode A held (empty
diff over `src/**`, `src/ffi`, `cbindgen.toml`, the generated header and `Cargo*`;
`[DllImport]` count unchanged).

Test-count reconciliation from the 910 baseline: **910 -> 911**, one test added
(73.2's deterministic guard). 73.3 removed a private helper, not a test. The
separate MAX_ACCUMULATED removal (user ruling, below) re-scoped three settings
tests and removed none.

Send-path allocation, measured absolutely (not marginally) with the same probe on
both sides on this box — `PlainPerSendBudgetBytes` temporarily zeroed so the
assertion prints the real figure, `d8ac7c50` checked out in a throwaway worktree
sharing the same native: **160 B/send at `d8ac7c50`, 160 B/send at HEAD**, stable
over 3 reps each. No new steady-state send-path allocation. ⚠ The fix brief cited a
prior "96 B/send against `d8ac7c50`"; that figure does **not** reproduce with this
probe *at `d8ac7c50` either*, so it came from a different measurement (a different
test, TFM, or the large-minus-small *marginal* rather than the absolute) — recorded
rather than quietly restated. The load-bearing comparison, HEAD equal to baseline
under one identical probe, holds. Consistent with the executable production delta
for the whole fix cycle, which is 8 removed lines in a construction-time settings
struct and nothing on the send path at all.

---

## 73.1 — MEDIUM — The file that owns `max.block.ms` still states the DELETED expiry contract as live behaviour

**Files:** `src/Confluent.Kafka/Internal/SendAccumulatorSettings.cs` (untouched by this
phase — `git diff d8ac7c50 HEAD` on it is empty),
`tests/Confluent.Kafka.UnitTests/Interop/SendAccumulatorTests.cs`,
`src/Confluent.Kafka/IAsyncProducer.cs`.

This is the **item-1 truth check**, not a doc-quality review: each site below asserts a
mechanism the phase deleted, or a contract the shipped code does not have.

First, the half the Actor got **right**, which I verified independently so it is not
"corrected" in the wrong direction:

* `CONFLUENT_KAFKA_PRODUCER_MAX_ACCUMULATED` → `MaxAccumulatedRecords` is **genuinely
  inert**. `grep -rn 'MaxAccumulated' src/ --include='*.cs'` returns only its own
  constant (`:130`), its parse (`:285`) and its property (`:203`). No reader. It is a
  binding-only env var with no downstream meaning. ✅
* `max.block.ms` is **unread by the binding but NOT inert**. Every entry of the user's
  config map is forwarded verbatim — `NativeProducer.cs:222`
  `NativeMethods.ProducerPropertiesPut(props, key.Pointer, value.Pointer)` in the
  per-entry loop — and the core honours it on the send path
  (`src/producer/kafka_producer.rs:1058` `wait_on_metadata(..., self.max_block_ms)`,
  `:1068` `remaining_wait_ms`, and again at `:1566`/`:1576`), plus the four transactional
  deadlines. So `NativeProducer.cs:142-149`'s *"a real Kafka producer key the core also
  honours"* is **TRUE** and must be kept. Do not rewrite it into an inertness claim.

**The defect** is that `a89f34de`'s sweep fixed `NativeProducer._maxBlockMs`,
`EnsureAccumulator` and `SendCompletionPump` but never opened the file that *declares*
the key, so four surviving sites now state the deleted contract as fact:

**(a) `SendAccumulatorSettings.cs:236-243` — `MaxBlockMs`'s own summary.**

> *"How long a `Send` blocked on the admission bound waits before the record is refused
> with a **retriable** `KafkaException` — delivered through the send's own `Task` and its
> delivery callback, Java's buffer-exhausted outcome, rather than thrown. […] Zero is
> legal and means "never block": the fast path still admits when capacity is free, and a
> saturated bound refuses immediately."*

Every clause describes machinery `b855d583` deleted. There is no expiry, no retriable
refusal, no callback fired for saturation, and `MaxBlockMs == 0` does **not** mean
"never block" — the wait is unconditional and untimed.

**(b) `SendAccumulatorSettings.cs:219-228` and `:141-146`.**
`:225-227` — *"Exceeding it makes the calling thread **wait**, bounded by `MaxBlockMs`"* —
the wait has no bound. The same paragraph (`:222-224`) names *"the inline append and the
FIFO submission queue"* as the two routes covered; the queue is deleted.
`:141-146` — *"The Java dotted config key the admission wait is bounded by."* — false.

**(c) The same class on the public surface, in the *new* direction.**
`IAsyncProducer.cs:78-79`: *"it returns when capacity frees, or when the producer is
closed underneath it (in which case the record is still sent by the closing producer's
final drain)."* That is true of `Stop`, but the **other** teardown trigger —
`AbandonOnThreadFailure`, which the accumulator's own `_spaceGate` comment
(`SendAccumulator.cs:161-166`) names as the second thing that ends the wait — **faults**
the record rather than sending it. `SubmitAdmitted`'s remarks (`:310-311`) get this right
("settled by the chain machinery either way"); the public sentence over-claims "sent".

**(d) The test file's own harness remarks, plus one stale test name.**
`SendAccumulatorTests.cs:1946-1950` still tells future authors *"a saturated admission
bound parks the caller for up to `max.block.ms`. Every test that saturates the bound on
purpose must therefore drive this from its own `Task` (**or supply a short
`maxBlockMs`**)"* — the parenthesised escape hatch no longer exists and following it
would hang a test thread forever. `:1987-1991` carries a whole paragraph on how to test
"admission **expiry**" (outer task for timing, inner for the failure), a deleted
mechanism. And `:1647` `Settings_MaxBlockMs_ZeroIsHonoured_AndMeansNeverBlock` asserts
only `settings.MaxBlockMs == 0` while its **name** asserts a behaviour the code no
longer has.

**Why this matters beyond tidiness:** (d)'s first item is actively misleading guidance
that would produce a hanging test, and (a)/(b) are the statements a future reader will
consult when deciding whether `max.block.ms` still gates `Send` — which is exactly the
question this phase changed. The pattern is the one my own review record names: *a
fix-round sweep must key on each defect the pass fixed and start with the file that owns
the symbol*, and this one skipped that file entirely because the phase's diff never
touched it.

**Suggested resolution:** restate (a)/(b)/(c)/(d) as what they now are — the value is
parsed, forwarded to the core, honoured there, and consulted by nothing in the
accumulator. Keep the knob (that disposition is the user's call, and this finding takes
no position on it). Re-name or re-scope the `ZeroIsHonoured_AndMeansNeverBlock` test to
what it actually asserts (the parse), or delete it with the rest of the expiry set.

---

---

### RESOLUTION — 73.1 (commit `ce746999`)

Accepted in full, including the Critic's framing that `max.block.ms` is **unread by
the binding but NOT inert**. The config key, its parse (`ReadMaxBlockMs`) and
`NativeProducer`'s verbatim forwarding of every config entry are **untouched**; only
the false claims that it still bounds *this binding's* admission wait were corrected.

Fixed, one per false statement:

* **(a)** `SendAccumulatorSettings.MaxBlockMs`'s summary — the retriable refusal, the
  delivery callback on saturation, and `"zero means never block"` are all gone. It now
  states what the value is, with a remark that nothing in the accumulator consults it
  while the core honours the key.
* **(b)** `MaxAdmittedRecords`'s summary lost both the `MaxBlockMs` bound and the FIFO
  submission queue as a live route; the wait is stated as untimed. `MaxBlockMsKey`'s
  summary no longer says the admission wait is bounded by it. The type-level remarks
  no longer group `MaxBlockMs` with `MaxAdmittedRecords` as "the admission bound".
* **(c)** `IAsyncProducer.Send`'s remarks now state both teardown outcomes (sent by the
  final drain on a normal close; faulted if the send-batch thread died) instead of only
  the `Stop` one. **Note:** this site was not in the fix brief's enumeration of 73.1
  sites, but it is part of the finding, so it was fixed rather than left standing under
  a closed finding.
* **(d)** The harmful one first: `Harness.AppendOne`'s "or supply a short `maxBlockMs`"
  escape hatch would now park a test thread forever — replaced with the two routes that
  work. `AppendOneFromAnotherThread`'s admission-**expiry** paragraph is replaced by what
  the two tasks actually carry. Two adjacent false statements found by the same sweep
  were fixed with them: `AppendOne`'s summary/inline comment described "queueing behind
  the bound" and an "inline-vs-queued routing decision" with the permit taken *before*
  the append, and `Settings_InvalidAdmissionOverride_FallsBackToTheDefault`'s comment
  claimed a zero cap would "block for max.block.ms and then fail".

**The stale test name: RENAMED, not deleted.**
`Settings_MaxBlockMs_ZeroIsHonoured_AndMeansNeverBlock` ->
`Settings_MaxBlockMs_ZeroIsParsed_NotTreatedAsInvalid`. Only the *name* was stale; what
it asserts is a live parse property — `ReadMaxBlockMs` keeps `"0"` where it falls back
to the default for `"-1"` (Java's `atLeast(0)` boundary), and
`Settings_InvalidMaxBlockMs_FallsBackToKafkasDefault` is the other half of that pair.
Deleting it would have dropped real coverage of a parse the binding still performs.

**Open question referred upward, NOT acted on.** The settings *property*
`SendAccumulatorSettings.MaxBlockMs` has **no production reader at all** — it is written
by the constructor and read only by tests. (`NativeProducer` has its own `_maxBlockMs`
field, already marked "SINCE M11/P3.4 NOTHING READS IT" by `a89f34de`, which it passes
into `FromEnvironment(int)` to populate this property.) It was **left in place**: the
user's ruling covered `MAX_ACCUMULATED` specifically, and a second config-surface
removal needs its own ruling.

## 73.2 — MEDIUM — Item 3: a **deterministic** seam exists for `AbandonOnThreadFailure`'s `_spaceGate.Cancel()`; the 1/8 disclosure can be closed

**File:** `src/Confluent.Kafka/Internal/SendAccumulator.cs:823-857`;
`tests/.../SendAccumulatorTests.cs:1159-1218` (the honest disclosure) and `:2052-2065`
(`InflateTheChainAccounting`), `:2109-2129` (`TruncateDeliveriesOfPendingNode`).

The disclosure is good practice and I am not penalising it. But *"not schedulable
broker-free"* does not survive checking, and the recorded pattern for this situation is
to prefer an **internal white-box seam that opens the window deterministically** over
trying to win a scheduling race. Such a seam exists here, and it needs one new harness
line — no production change.

**First, why the obvious attempts fail (so the Actor's negative result is confirmed as
far as it goes).** Let `W` be parked callers at the handler's take. A parked caller
implies the semaphore is at 0 and implies its record is in `_chainRecords`, so
`admitted ≥ W` and `Release(admitted)` wakes all of them — this is exactly the Actor's
argument, and it is correct. `InflateTheChainAccounting` cannot be composed with a parked
caller either: `SemaphoreSlim.Release(n)` throws only when `CurrentCount + n > maxCount`,
and with the ceiling at `int.MaxValue` a parked caller forces `CurrentCount == 0`, so
`Release(int.MaxValue)` **succeeds** and wakes it. Those two facts are why the obvious
constructions come back green.

**The case they both miss is the one the code's own comment names** (`:827-834`): *"a
release that releases nothing"*. `ReleaseAdmission` (`:391-397`) is a no-op for
`count <= 0`, so a **non-positive** `admitted` is the second, reachable form of that
hazard — and unlike the throwing form it is not mutually exclusive with a parked caller,
because the guard is checked before the semaphore is touched at all.

**The construction** (all levers already exist; only the injected value is new):

```
settings: maxAdmittedRecords: 2, batchWindowMs: 60_000

1. filled = harness.Append(2);                       // count -> 0, _chainRecords = 3 after step 2
2. admission = harness.AppendOneFromAnotherThread(); // PARKS (assert !IsCompleted after a settle window)
3. harness.TruncateDeliveriesOfPendingNode(keep: 1); // the existing escape-SendNode injection
4. harness.SetChainAccounting(-1);                   // NEW: one reflection write, same warrant
                                                     // as InflateTheChainAccounting
5. harness.ForceDrainWithoutWaiting();
```

Trace: `RunLoopCore` takes the chain with `admitted == -1`, publishes `_inFlight`, and
`ReleaseAdmission(-1)` is a **no-op** (`:393`) — the parked caller is *not* woken.
`SendChain` → `SendNode` → `CompleteNode` throws `IndexOutOfRangeException` reading
`node.Deliveries[1]` (`:1227`) → `FaultNode` settles index 1 and then throws on
`node.Deliveries[1] = null` (`:1374`) → escapes `SendNode` → escapes `SendChain` →
`RunLoop`'s catch → `AbandonOnThreadFailure`. There, `TakeChainLocked` yields
`admitted == 0` (`_chainRecords` was zeroed by the first take and `_closed` is set in the
same acquisition, so nothing can have been appended), `SettleAbandonedChain` settles the
remainder, and **`_spaceGate.Cancel()` at `:844` is the only thing that can release the
parked caller.** `ReleaseAdmission(0)` that follows is a no-op by construction.

Delete the `Cancel` and `await TestTimeout.Run(() => admission, deadline)` fails on its
deadline — deterministically, every rep, in-suite. That is the M6 mutation going from
**1/8 → 8/8**.

**On fairness of the fixture:** the injected state (`_chainRecords < 0`) is not
producible by production — which is precisely the warrant the Actor already claimed for
`InflateTheChainAccounting` (*"Corrupting the counter directly IS the fault"*). The
handler's own stated reason for the unconditional `Cancel` is that its trigger may have
already corrupted the accounting, so injecting corrupted accounting is the correct
fixture for it, not a proof about the fixture. The alternative — a production hook that
makes the handler's `ReleaseAdmission` throw — would add production surface for no gain.

**Verdict on item 3: (a) — a deterministic seam exists and should be used.** I have not
executed it (the Critic role forbids touching the tree), so the Actor must confirm the
red/green before closing; the prediction above is falsifiable in one run.

**Do not** weaken the comment at `:823-843` while doing this — the reasoning there is
correct and is what made the seam findable.

---

---

### RESOLUTION — 73.2 (commit `9419c2b2`)

Accepted, and the Critic's predicted trace is **confirmed empirically** — it was a
sketch, not an executed result, so it was measured before closing.

Implemented as a **new** test,
`Admission_ParkedCaller_IsReleasedByTheFailureHandlersCancel_WhenTheReleaseWakesNobody`,
alongside the existing one rather than replacing it: the sibling keeps its value as a
deterministic *contract* assertion (released, settled, teardown completes), and its
disclosure comment is re-pointed at the new test as the mechanism guard — the same
shape `f09bfcdf` used for the `Stop` twin. The comment at `SendAccumulator.cs:823-843`
was **not weakened**, as instructed.

One new harness method, `SetChainAccounting(int)` — a single reflection write.
`InflateTheChainAccounting` now delegates to it, so the reflection into `_chainRecords`
stays single-sited rather than being duplicated. **No production change.**

**MEASURED — full net10.0 suite, in-suite (no `--filter`), fresh process per rep:**

| regime | result |
|---|---|
| production mutation: delete `AbandonOnThreadFailure`'s `_spaceGate.Cancel()` | **8/8 red** |
| fixture control: mutation still applied, `SetChainAccounting(-1)` removed | **0/8 red** |

In all 8 mutated reps the suite reported `Failed: 1, Passed: 910, Total: 911` and the
**only** failing test was the new one — the sibling
`..._IsReleasedByTheBatchThreadsFailureHandler_...` stayed green all 8, which is
consistent with its recorded 1/8 and confirms the new test is the discriminator rather
than a second lucky one. Every rep ran to completion (`Failed!` summary, never
`Test Run Aborted`); the mutated runs took ~40 s against ~10 s clean, the parked
caller's 30 s deadline being the difference.

The fixture control is the part that matters for honesty: with production still broken
but the one injected line removed, the test goes green 8/8. So the 8/8 is a property of
the **seam**, not of scheduling — the two mutations were run separately, production and
fixture, per the standing rule.

Mutation hygiene: both files were **snapshotted to scratch and restored from the
snapshot**, never `git checkout --`, since the new test was uncommitted at the time.
Post-restore the tree was re-verified green at 911 and `grep` confirmed no mutation
marker survived.

## 73.3 — LOW — Orphaned dead test helper left by the deletion pass

**File:** `tests/Confluent.Kafka.UnitTests/Interop/SendAccumulatorTests.cs:644-656`.

`private static async Task AssertSettledByTheOverRelease(Task<RecordMetadata> send)` now
has **zero** call sites. At `d8ac7c50` it had four — two in
`BatchThreadFailure_ReleasesASendWaitingForSpace` and two in
`Admission_TeardownFlushedSendsReturnTheirPermits_NotJustThePermitBackedOnes`, both of
which this phase deleted. Its surviving twin assertion was inlined into
`BatchThreadFailure_SettlesTheChainItHadALREADYTaken_NotJustTheAccumulators`
(`:535-538`), so the helper is redundant as well as unreferenced.

This is a deletion-safety miss rather than style: `dotnet build` does **not** warn on an
unused private method, so the "0 warnings across all TFMs" gate could not have caught it
— the same silent-pass shape as an unreferenced fixture that later gets re-adopted with
stale semantics. Either delete it, or use it at `:535-538` in place of the inlined
assertions (which would also keep the two over-release assertions single-sourced).

---

---

### RESOLUTION — 73.3 (commit `31a8da97`)

Accepted. `AssertSettledByTheOverRelease` is **deleted**.

Of the Critic's two options — delete, or re-adopt it at `:535-538` in place of the
inlined assertions — delete was chosen: the inlined pair covers a single send, so
routing it through a shared helper would buy single-sourcing for exactly one caller.

The Critic's point that the 0-warnings gate could not have caught this is correct and
is why it was found by review rather than by the build.

