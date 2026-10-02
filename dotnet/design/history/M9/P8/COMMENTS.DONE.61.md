# COMMENTS.DONE.61 — M9/P8 `ConsumerHandle` (in-callback reentrancy)

**Agent:** N=61 (dotnet-actor). **Mode: A.** **Branch:**
`prashah_dev_dotnet_binding_consumer`, on top of `62b360b4` (M9/P9).

**Commits:** `ed99294a` (implementation) · `0d5820a1` (tests) · `4c5fbba9` (doc-sync),
plus the fix round: `3197e322` · `b9f5e155` · `e135a04a` (`fixup!`s, subjects paired
byte-for-byte and verified).

**No `COMMENTS.61.md` existed** — nothing to fix before starting.

**This closes the M9 consumer-callback-parity roadmap.** P5/P6/P7/P9 were done; P8
was the last phase.

---

## 1 · `consumer-threading.md` §31 test #1 — the sanctioned split (roadmap §5.11, verbatim)

> `consumer-threading.md` §31 test #1 is satisfied in two parts. **P8 ships the
> mechanism proof**: a listener fired by `MockConsumer.Rebalance` calls
> `handle.Assignment()` successfully while the same listener's
> `consumer.Assignment()` is rejected as concurrent access — establishing that the
> reentrancy handle bypasses the access guard that makes the consumer's own API
> unusable from a callback (`confluent_kafka.h:2810` vs `:2950`). **The
> end-to-end half** — a listener whose `commitSync` through the handle actually
> commits — requires a real broker and a cross-backend harness change, and is
> tracked as follow-up **O3** (roadmap §5.10) alongside O1/O2. This is a scoped
> split with both halves owned, **not** a third deferral.

Shipped as
`PublicConsumerReentrancyHandleTests.ReentrancyHandle_SucceedsInsideAListener_WhereTheConsumersOwnApiIsRejected`,
**with the mandatory mutation check run** (§5 below).

---

## 2 · P8-D2 — the evidence, and the conclusion

**Question.** Ref-counting means the last `Dispose` can trigger `Consumer_destroy`
on whatever thread disposed the handle. ffi §B6 says *"if either property is ever
weakened, this rule must be re-derived."* Does the new trigger weaken either?

**Conclusion: NO — neither property is weakened, and the free-site rule does not
need re-deriving.** Four independent pieces of evidence, in decreasing order of
strength:

1. **The arbitrary-thread destroy trigger is NOT new.**
   `SafeConsumerHandle.ReleaseHandle` → `Consumer_destroy` already runs
   **synchronously on whatever thread called `Dispose`** at count 1→0 — the ordinary
   clean path taken by every consumer in the binding and every test in the suite.
   §B6's property (2) ("the deferred-destroy path fires from `FreeGcHandle` **on the
   dispatcher thread**") is scoped to the *deferred* path precisely **because** the
   immediate path was always arbitrary-thread. P8 adds a caller to an existing
   trigger, not a new class of trigger.

2. **Property (1), the ref-count, is *strengthened*, and the argument is monotone.**
   P8 only ever **adds** a count holder. An extra holder can only ever **delay**
   `Consumer_destroy`, never advance it. A phase that can only delay a destroy
   cannot open a window that was previously closed — no case-analysis over
   interleavings is required, which is why this is the load-bearing argument rather
   than the thread-identity one. The invariant §B6 rests on is untouched: destroy
   runs only at count zero, and a listener or commit callback only ever runs inside
   an operation that holds a count.

3. **Property (2), the single serialised dispatcher, is untouched.** P8 adds no
   callback, no `user_data_destroy`, and no dispatcher work of any kind (§3 below).
   There is no new queued job that a destroy could race.

4. **An arbitrary-thread hook fire is the *contracted* case, not a violation.** The
   core states it outright: `destroy` *"may fire on **any thread** — whichever one
   drops the adapter (a tokio worker running the consumer's background task, the
   dispatcher thread, or the C thread calling `_destroy`)"*
   (`src/ffi/common.rs:422-425`). That is exactly why both existing hook free sites
   (`ListenerRegistration.Release`, `CommitCallbackRegistration`) are already
   `Interlocked`-guarded and thread-agnostic. Nothing about P8 requires them to
   change.

**Cited throughout: the ref-count and the single serialised dispatcher — never
P6's `Arc` argument**, which is disproved: `FfiRebalanceListener::invoke`
(`src/ffi/consumer.rs:3122`) copies `self.target.user_data` into a `SendUserData`
**before** dispatching, so the queued job carries a raw copy. Re-verified this phase
by reading the function; the same shape holds for `FfiOffsetCommitCallback`
(`:3877`).

**Not inconclusive, so not escalated.**

---

## 3 · The `GCHandle` statement (§0.2), explicitly

**P8 introduces no callbacks at all, and no P8 entry point takes a
`user_data_destroy`. There is therefore no `GCHandle` in this phase, and ffi §B6's
free-site rule ("hook present ⇒ the hook is the sole free site; no hook ⇒ the
callback frees") does not engage anywhere in it.**

Re-verified against the regenerated header: exactly **three** entry points take a
hook — `ConsumerRebalanceListener_new` (`h:2078`), `Consumer_commit_async_with_callback`
(`h:2505`), `Consumer_commit_async_offsets_with_callback` (`h:2538`) — and P8 uses
**none** of them. `Consumer_handle` and all 22 `ConsumerHandle_*` functions are
plain synchronous calls with no function-pointer parameter. §B6 was left
**unedited** as a result; this statement is the deliberate no-op record, not a
blank row.

---

## 4 · The four decisions, as executed

| # | Ruling | As shipped |
|---|---|---|
| **P8-D1** | ref-count | `SafeConsumerReentrancyHandle` holds **exactly one** `DangerousAddRef` on the consumer's `SafeConsumerHandle`, released in its own `ReleaseHandle` **after** `ConsumerHandle_destroy`. The cost — a leaked handle defers the destroy indefinitely — is documented on the public type, in ffi §B2, and in CLAUDE.md §4 |
| **P8-D2** | verify first | Verified; §2 above. Not weakened, not escalated |
| **P8-D3** | `IConsumerCommon` | `ConsumerHandle Handle();` on `IConsumerCommon` + four forwarders. Tests call it through **interface-typed** helpers, so a concrete-typed call cannot satisfy them |
| **P8-D4** | `IDisposable` only | With the required comment at the site explaining the asymmetry (every op is a synchronous C call; a `DisposeAsync` would imply an async surface that does not exist) |

**Where the reference is released is a deliberate design point, not an accident.**
It lives in the `SafeHandle`'s `ReleaseHandle`, not the public wrapper's `Dispose`,
because `SafeHandle.Dispose` only *requests* release: since every handle op passes
the handle's `SafeHandle` as a parameter, the marshaller holds a call-scoped
reference for the whole native call, so releasing from `ReleaseHandle` guarantees
the parent count cannot drop while an op is still blocked inside the core.
Releasing from the wrapper's `Dispose` would not.

---

## 5 · A real defect the tests caught (self-review, no Critic input)

The first draft took the parent reference **twice** — once in
`NativeConsumerHandle.Create` and again inside the (then) `TransferParentReference`
— against a **single** `DangerousRelease`. The count never reached zero, so
`Consumer_destroy` **never ran**: every consumer that ever produced a reentrancy
handle leaked its native resources for the process lifetime.

It had **no managed symptom**. The 543 pre-existing tests stayed green, the build
was clean, and every disposal order reported `IsClosed=False` — which reads as
"deferred", i.e. as the feature working.

What caught it: writing the deferral test as a **three-way differential** (no handle
→ releases immediately; one outstanding → does not; the handle's own `Dispose`
completes it) rather than a single assertion. Only the *contrast* is meaningful — a
single-case check cannot tell a working ref-count from a permanently unbalanced one.
That reasoning is now in ffi §B2's Tests-required so the next phase inherits it.

Fixed by folding the correction into the commit that introduced it (`ed99294a`,
amended pre-push) rather than shipping a known-leaking commit for `git bisect` to
land on. `AdoptParentReference` now *adopts* the caller's single reference and
carries a comment saying why an `AddRef` there would be wrong.

**Mutation checks (both run, both confirmed):**

| Mutation | Expected | Observed |
|---|---|---|
| Test 6: swap `handle.Assignment()` for `consumer.Assignment()` | fail | `Failed: 1, Passed: 0` — reverted, green again |
| Reintroduce the second `DangerousAddRef` | fail | `Failed: 3` (the two deferral tests + the 200-handle balance loop) — reverted, green again |

---

## 6 · Doc-sync, itemized (roadmap §5.6, both halves)

Part **(b)** — *does any existing normative sentence now forbid the shipped code* —
found **three** real hits. That is the half that recurred three times this
milestone; all three are fixed.

| § | (a) concept added | (b) sentence that forbade the shipped code |
|---|---|---|
| **§B2** | A **sixth** ownership category: caller-owned, independently destroyed, ref-counting its parent. Release ordering, the one-reference-not-two rule (with the §5 leak), the third deferred-destroy path, and the differential test shape | ⚠ **HIT** — *"the only sanctioned raw-pointer sites are the async submit helpers and the close family"* forbade the shipped `ConsumerHandleDestroy(IntPtr)`. Extended to name the two `*_destroy` entry points as structurally exempt, and to state that all 21 non-destroy handle declarations do take the `SafeHandle` |
| **§B1** | The handle deliberately bypasses the guard; ops are synchronous and `block_on` the **calling** thread — safe from the dispatcher thread and any embedder OS thread, not from inside a tokio runtime; two new anti-patterns | ⚠ **HIT** — *"One operation in flight … the core's access guard serializes ops"* described the whole consumer surface, which the handle contradicts **by design**. Now carries the sanctioned exception |
| **§B5** | `IllegalStateError` → `InvalidOperationException`; `UnsupportedVersionError` → `KafkaException` ("assert it, do not work around it"); the guard-bypass contrast as a contract; three anti-patterns; the required tests **including the mutation check** | none |
| **§B6** | **NO EDIT** — see §3 above. The Part B standing note's three families are unchanged; P8 adds no fourth | none |

Outside the rulebook, one more part-(b) hit in binding source: `NativeMethods`'
class doc claimed *"Three declarations deliberately keep `IntPtr`"* — four now.
Corrected, and it now states that the `SafeHandle`-param convention extends to the
handle family.

`CLAUDE.md` (D10): §3 records the shipped surface and adds `Handle()` to the
`IConsumerCommon` sketch with the Java marker; §4 gains the reentrancy row carrying
the **DoD #7 justification** (§7 below), the deliberately smaller method set, the
ref-count trade, and the `IDisposable`-only asymmetry; the stale *"`ConsumerHandle`,
not yet exposed"* sentence is corrected. The P6/P7 **forward references** in
`IConsumerRebalanceListener` and `IOffsetCommitCallback` both said the escape hatch
"is not yet exposed" / "arrives in a later phase" — those were due this phase and
now point at `ConsumerHandle`.

---

## 7 · DoD

- **#1/#2/#5/#8/#9** — met. No `TODO`/`FIXME`; Apache-2.0 header on both new files.
- **#3** — error-message content asserted, not just the type: the mock
  `UnsupportedVersion` message in full, and `ParamName` + custom message on all
  three managed preconditions.
- **#7** — `ConsumerHandle` is **not** a Java type. Justification recorded in
  CLAUDE.md §4 and on the type: Java needs no equivalent because its callbacks run
  on the polling thread where `acquire()` is reentrant; the C ABI's guard is held
  for the whole operation that fired the callback, so this is binding scaffolding
  **restoring a Java behavior**, not new API surface — and **Python has the
  identical type for the identical reason** (`consumer.py:378-545`).
- **#10** — **N/A**: per-callback, not per-record. There is no send or receive hot
  path here; a reentrancy handle is obtained once and used per rebalance/commit.
- **#11** — applies and is met. No `block_on`-wrapped **managed** sync façade was
  added: the `block_on` is the **core's**, inside the sync ABI, which is the shipped
  `Seek` / `CurrentLag` precedent. `IDeserializer` is untouched.
- **#12** — the tests drive the same public entry points production does; the
  ref-count tests read the production `SafeConsumerHandle` rather than a fixture.

---

## 8 · Gates (all run; real output in the phase report)

| Gate | Result |
|---|---|
| `dotnet build` (netstandard2.0 · net8.0 · net10.0 · net462 test asm) | **0 warnings, 0 errors** |
| `dotnet test -f net10.0` | **603/603** (543 baseline + 60 new), re-run **4×**, no flakes |
| `dotnet test -f net8.0` | **603/603** — genuinely executed, not build-only. Requires the `$HOME/.dotnet` driver (SDK 10.0.400 + the `NETCore.App 8.0.30` runtime); the system `/usr/local/share/dotnet` has only the net10 runtime and aborts, which is easy to misreport as a CI-only gate |
| `dotnet test -f net462` | builds clean; **cannot execute** on macOS (vstest needs a Mono host) — CI-run |
| `dotnet format --verify-no-changes` | clean |
| `cargo xtask lint` | ✅ No lint issues found |
| `cargo test --all-features -- --skip __grpc` | all suites ok (3773 + 109 + 38 + 188 + 8 + 4 passed, 0 failed) |
| harness `-- __grpc_dotnet` | **28 passed, 0 failed** — filter `--list`ed first (28 matches) so a zero-match filter could not read as a pass |
| TFM-matrix smoke | 10 smoke/sentinel tests pass on net10.0; net462 + netstandard2.0 assemblies built |

**Mode A re-verified:** `git diff a7efb0d5 HEAD -- src/ cbindgen.toml` is **empty**,
as it has been across the entire milestone, and P8 added **nothing** under the Rust
`tests/` tree (`git diff --name-only 62b360b4 HEAD -- tests/` → 0).

---

## 9 · Deliberately not done

- **O1 / O2 / O3** — Manager-level cross-backend follow-ups (roadmap §5.10), not
  actioned here. P8 closing does not close them.
- **§B6 edit** — no-op by design; §3 is the record.
- **A handle-using gRPC server listener** — follow-up O3, not P8 (roadmap §1).
- **`Poll` / `Subscribe` / `Unsubscribe` / `Close` and callback-taking commits on
  the handle** — absent from the ABI *deliberately*; wrapping them was explicitly
  out of scope.


---

# Fix round — Critic findings 1–5 (all resolved)

Critic verdict: *"P8 is done once Finding 1 is resolved; 2–4 corrected; 5 optional."*
All five are resolved. Suite **605** (was 603); the two added tests are each the **sole
detector** for the thing they pin.

## Finding 1 (MAJOR) — RESOLVED as option (b): correct the rule and the doc, not the code

§B5 (added by my own doc-sync commit) mandated `IllegalStateError` →
`InvalidOperationException`, named the shipped flat-`KafkaException` behaviour as an
anti-pattern three lines later, and the public XML doc promised users the same. Three-way
disagreement, and the Critic is right that it is exactly what a "part (c)" doc-sync check
would have caught: *does the shipped code satisfy the sentence I just wrote?*

**Decision: the rule and the doc were wrong; the code was right.** The coordinator leaned
the other way, so here is the evidence that changed the call — three independent reasons,
each sufficient on its own:

1. **The idiom map has a different scope than I claimed.** CLAUDE.md §3's row reads
   `IllegalArgumentException / IllegalStateException` → `ArgumentException` /
   `InvalidOperationException` — **"validate before the FFI call"**. It governs
   *managed-side precondition validation* of **Java** exceptions. The in-runtime rejection
   has **no Java counterpart at all** (Java has no tokio runtime) and arrives *from* the
   core on the operational channel. My §B5 sentence cited that row as "the standing idiom";
   it never reached this case. That is the actual error.
2. **It would split one condition across two exception types.** The core's `illegal_state`
   is **not** handle-specific. `ConsumerHandle::position` and
   `AsyncKafkaConsumer::position_timeout` share one implementation
   (`async_kafka_consumer.rs:316-325` → `:4516-4521`), so both surfaces return the identical
   error for an unassigned partition. **Measured, both paths:** code **`-1`**, *"You can only
   check the position for partitions assigned to this consumer."* `ensure_open`'s *"This
   consumer has already been closed."* (`:2792-2794`) is a second shared instance. Mapping
   only the handle side would make the reentrancy twin throw a different type than the
   consumer it mirrors **for the same call** — a worse defect than the one being fixed, and
   invisible to the mock-only tests.
3. **It is not implementable as written.** `illegal_state` carries the **generic code -1**,
   shared with other errors. There is no distinguishable code to branch on, so "map only the
   in-runtime one" reduces to matching on message text.

Also weighed: the binding branches on an error **code** to pick an exception type
**nowhere** — verified by grep. The only non-`KafkaException` mapping on this surface is the
concurrent-state-read **null return**, which is a structurally different channel (no error
handle exists, so the binding must synthesize something). §B5 now says that explicitly, so
the distinction is not re-litigated.

**Changed:** §B5 states the flat-`KafkaException` route including the in-runtime
`IllegalStateError`, records all three reasons, and inverts the anti-pattern to the correct
one ("do not branch on an error code to pick a managed exception type"). The public
`ConsumerHandle` Threading paragraph now says `KafkaException` and states explicitly that it
is **not** an `InvalidOperationException`, so a `catch` written against the doc works.

**Test added** — `HandleAndConsumer_ReportTheSameCoreError_Identically` pins reason 2 (the
concrete one): handle and consumer must report the same core error identically.
Reintroducing the mapping fails **only** this test (1 failed / 604 passed).

## Finding 2 (MEDIUM) — RESOLVED, and the Critic's correction to my argument is accepted

The Critic is right: **"only delays" does not entail "the same holder releases last"**, and
§B6's second clause is about precisely that — *which thread* runs the destroy. Consumer +
listener + async op in flight, disposed: before P8 the destroy runs on the **dispatcher
thread** from `FreeGcHandle`; with a live reentrancy handle it releases to 1 there and
destroys later from `ReleaseHandle` on **whatever thread disposed the handle**. Delayed
**and relocated**. My monotonicity argument would have waved that through unseen.

The **conclusion is unchanged** — no reachable UAF, no code change — but it now rests on
§B6's **first** clause alone: destroy runs only at count zero; a callback only ever runs
inside an operation that holds a count; and a dispatched job is enqueued and drained inside
the very operation that produced it, before that operation's completion releases its count.

**Changed:** `ListenerRegistration` "four .NET paths" → **five**, with (5) spelled out as
*not* dispatcher-serialised and its trip-wire split accordingly; ffi §B6's clause scoped to
**M9/P4's** path with P8's third path recorded as re-derived on the first clause;
`SafeConsumerReentrancyHandle`'s P8-D2 remarks rewritten to state what is actually proved,
carrying an explicit warning against restating the monotonicity form.

## Finding 3 (LOW) — RESOLVED

`NativeMethods.cs`'s family header said "every one of the 22 takes the
`SafeConsumerReentrancyHandle`", contradicting the class doc this phase had already
corrected. 22 entry points, **21** `SafeHandle` params; `ConsumerHandleDestroy` is the
single `IntPtr` exclusion. Fixed with the reason inline — this is the one place a maintainer
might "finish the job" and pass `this` to a `_destroy`, which both `ReleaseHandle` comments
exist to warn against.

## Finding 4 (LOW) — RESOLVED, both halves

(a) The mutation-mechanism comment was wrong. The mutation **does** fail and this test is
the sole detector, but via the `Rebalance` call throwing `KafkaException : KafkaConsumer is
not safe for multi-threaded access` — `consumer.Assignment()` sits outside the `try`, so the
listener throws and the asserts are never reached. Comment corrected to describe that, and
to note the both-succeed regression fails by the other route, which *is* reached.
(b) Added the message assertion on the guard rejection — it is what proves this is the
*guard* rejection rather than an unrelated `InvalidOperationException` (DoD §3).

## Finding 5 (LOW, optional) — DONE rather than skipped

`ParentReference_IsReleasedFromReleaseHandle_NotFromTheWrappersDispose`. The Critic's probe
was right and cheap: take a `DangerousAddRef` on the **inner** handle (standing in for the
call-scoped reference the marshaller holds while a handle op executes — every one of the 21
non-destroy ops passes that `SafeHandle` as a parameter), dispose both, assert the parent is
**not** released, then release and assert it **is**. No threads, no blocking, fully
deterministic. Needed one `internal` accessor rather than the Critic's reflection.
**Verified as the sole detector:** moving the release into the wrapper's `Dispose` gives
1 failed / 604 passed, where before it was green 603/603.

## Fix-round mutation ledger (all three re-run)

| Mutation | Result |
|---|---|
| Reintroduce `IllegalState` → `InvalidOperationException` (F1) | **RED — 1 failed / 604** (`HandleAndConsumer_ReportTheSameCoreError_Identically`, sole detector) |
| Move the parent release into the wrapper's `Dispose` (F5) | **RED — 1 failed / 604** (`ParentReference_IsReleasedFromReleaseHandle…`, sole detector) |
| Test 6: `handle.Assignment()` → `consumer.Assignment()` (F4) | **RED — 1 failed / 604** — still bites after the edits |

All reverted; suite green 605/605 afterwards.

## Fix-round gates

| Gate | Result |
|---|---|
| `dotnet build` (netstandard2.0 · net8.0 · net10.0 · net462 test asm) | **0 warnings, 0 errors** |
| `dotnet test -f net10.0` | **605/605** (603 + 2) |
| `dotnet test -f net8.0` | **605/605** |
| `dotnet format --verify-no-changes` | clean (exit 0) |
| `cargo xtask lint` | ✅ No lint issues found |
| Mode A | `git diff a7efb0d5 HEAD -- src/ cbindgen.toml` **empty**; worktree diff empty |

**Not actioned (out of scope, per the coordinator):** O1/O2/O3; the Critic's two
rule-update suggestions (§B2 owning the single authoritative destroy-path list, and the
doc-sync **part (c)** check) — both are the maintainer's. The harness flake the Critic saw
(`test_ml_partitions_for__grpc_dotnet`, environmental) was not chased.
