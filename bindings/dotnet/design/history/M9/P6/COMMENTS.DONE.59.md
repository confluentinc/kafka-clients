# COMMENTS.DONE.59 — M9/P6 (.NET binding): `IConsumerRebalanceListener`

Agent N=59, Actor. Branch `prashah_dev_dotnet_binding_consumer`, on top of `1a6c13f8`
(M9/P5). Mode **A** (C# only). Brief:
`bindings/dotnet/design/history/M9/P6/PLAN.md`; roadmap
`bindings/dotnet/design/current/PLAN-M9-consumer-callback-parity.md`.

No `COMMENTS.59.md` existed at the repo root or under `bindings/dotnet/` when this
phase started, so nothing was carried in.

This file records decisions, deviations and accepted residuals made **during**
execution, per `bindings/dotnet/CLAUDE.md` §8.4.

---

## 1 · P6-D3 — where the registration `GCHandle` is freed: **option (a)**, the
## `user_data_destroy` hook

The brief required verifying, from `src/ffi/consumer.rs`, whether the release hook can
fire concurrently with (or after) an in-flight listener callback, then choosing — never
silently.

**Finding: it cannot** — but ⚠ **the mechanism stated in the first version of this record
was wrong, and was corrected in the fixup closing Critic F2.** The conclusion (option (a))
stands; the *proof* rests on step 4, not step 2. Corrected chain:

1. **The hook has exactly one firing site.** `user_data_destroy` is invoked only from
   `CallbackTarget`'s `Drop` (`src/ffi/common.rs:442-451`), i.e. when the last reference
   to the listener object is dropped. The `CallbackTarget` is a field of
   `FfiRebalanceListener` (`src/ffi/consumer.rs:3094-3103`), which the core holds as
   `Arc<dyn ConsumerRebalanceListener>`.
2. **Every invocation site holds an owned `Arc` clone across the callback's `.await`** —
   but this proves **less** than originally claimed.
   - mock: `src/consumer/mock_consumer.rs:260-262` and `:271-272`, both binding
     `self.subscriptions.rebalance_listener()` — which is `.clone()`
     (`src/consumer/internals/subscription_state.rs:887-888`) — to a local before
     awaiting;
   - real consumer: `src/consumer/async_kafka_consumer.rs:3273` (`…lock().unwrap().clone()`)
     feeding `:3285` / `:3290` / `:3295`, and `:5317-5333`.
     (The Critic enumerated the complete production set repo-wide and confirmed these
     three are all of them; no uncited path exists.)

   **What that establishes:** a **replacing subscribe** cannot fire the hook mid-callback,
   because a replacement does not drop the invoking future.

   **What it does NOT establish** — the original record over-reached here:
   `FfiRebalanceListener::invoke` copies the pointer out *before* dispatching
   (`src/ffi/consumer.rs:3122`, `let user_data = SendUserData(self.target.user_data);`), so
   the `move` closure handed to `dispatch_and_wait` carries only a **raw** `user_data`
   copy — not the `Arc`. `dispatch_and_wait` (`src/ffi/common.rs:350-369`) enqueues and
   *then* awaits (its own comment concedes the awaiting task can be cancelled), and
   `enqueue_or_run_inline` documents that the dispatcher runs every queued job before
   exiting. So "strong count ≥ 1 during the callback" holds **only while the awaiting
   future is alive**: drop that future and the `Arc` drops in the same instant, firing the
   hook while a job may still be queued with the raw pointer.
3. **Dropping that future means destroying the consumer, which is governed by a documented
   caller precondition.** `kafka_consumer_Consumer_destroy` states: *"destroying
   concurrently with an in-flight op is a C lifetime precondition the caller must uphold"*
   (`src/ffi/consumer.rs:513-517`). If violated it **would** race — `:530-540` cancels
   in-flight futures via `runtime.shutdown_background()`, drops the consumer, and
   explicitly **detaches the dispatcher without joining** ("do NOT join"). This is the C
   UAF shape (`9465e197`).
4. **The load-bearing guarantee: the .NET binding upholds that precondition structurally,
   and the dispatcher is a single serialised thread.** M9/P4 H1 made `SafeConsumerHandle`
   release ref-counted, and a listener callback only ever runs *inside* a consumer
   operation — the rebalance blocks on it (`confluent_kafka.h:209-213`) — so
   `ReleaseHandle` → `Consumer_destroy` runs only at count zero and cannot overlap one.
   The four .NET paths to `Consumer_destroy` were enumerated:
   1. a **sync** call holds a call-scoped marshaller AddRef via its `SafeConsumerHandle`
      parameter (`ConsumerSubscribeWithListener` and `MockConsumerRebalance` are both
      declared that way for exactly this reason);
   2. an **async** op holds a span-the-op `DangerousAddRef` released in `FreeGcHandle`;
   3. the M9/P4 Q1/Q3 **deferred-destroy** path fires from `FreeGcHandle` **on the
      dispatcher thread** — the same single thread that would run a queued listener job,
      so the two are serialised, never concurrent; and the op cannot complete before its
      own listener callbacks have returned, so no listener job can be queued behind it;
   4. `Consumer_close_with_timeout` is **not** a second dropper — it passes the timeout
      *into* `close_with_options` under `block_on` (`src/ffi/consumer.rs:4057-4064`)
      rather than wrapping a droppable `tokio::time::timeout`.

**So: the ref-count + the single serialised dispatcher is what closes the window — not the
`Arc`.** If either property is ever weakened, this decision must be re-derived. The
corrected argument is recorded in the `ListenerRegistration` `<remarks>` and in
`ffi-marshalling.md` §B6's multi-shot Rule.

**Decision: option (a).** The hook is the single sanctioned free site
(`ListenerRegistration.Release()`), and no other site frees a registration.

**Why (a) rather than the roadmap's earlier lean toward (b).** Beyond the proof above,
(b) is not actually the safer trade it looks like:

- (b) does **not** buy memory safety over (a) — its free site (`NativeConsumer`
  teardown) is *also* after a `Consumer_destroy` that never joins the dispatcher, so it
  has the same theoretical straggler exposure and is closed by the same point-4
  argument.
- (b) forces the binding to **infer** when a registration died. The header spends 40
  lines explaining that this cannot be inferred — a subscribe rejected *after*
  registration keeps the registration, and an **empty-topic-list subscribe releases the
  listener while returning success** (`confluent_kafka.h:2105-2130`, and *"Do not treat
  a failed subscribe as proof that `user_data` has been released"*). The only
  inference-free form of (b) is "retain every registration until teardown", i.e. growth
  proportional to re-subscribe count.
- (a) uses the mechanism the ABI provides for precisely this question, is bounded, and
  gets triggers 2 and 3 right for free.

**Cross-layer dependency, flagged for escalation.** Point 4 is a *binding-level*
invariant, not an ABI guarantee: the ABI only states the precondition, it does not
enforce it. If a future .NET path ever calls `Consumer_destroy` without the ref-count
(an `ffi-marshalling.md` §B2 anti-pattern), the proof lapses. This is the one place a
reviewer could reasonably disagree with the choice, so it is stated rather than buried.

### Accepted residual (a-specific) — **both branches**

`Consumer_destroy` uses `runtime.shutdown_background()`, which does not specify whether a
cancelled task is dropped or leaked. The first version of this record analysed only the
benign branch; Critic F2 correctly flagged that. Both:

- **Task leaked** → it retains a listener reference, the hook never fires, and that
  registration's `GCHandle` stays allocated for the process lifetime. A **bounded leak,
  not a use-after-free**.
- **Task dropped** → the `Arc` drops, the hook fires and frees the `GCHandle`. Safety then
  rests **entirely** on no listener job being in flight or queued at that instant — which
  is precisely what step 4's ref-count guarantees (destroy runs only at count zero, and a
  listener callback implies a counted operation). This is the dangerous branch, and it is
  closed by the ref-count, not by anything in step 2.

Neither branch is reachable outside the already-documented "dispose without awaiting your
own operation" misuse path (the `ffi-marshalling.md` §B2 carve-out). Recorded, not filed.

---

## 2 · P6-D1 — option (b): three required interface methods + a public base class

`IConsumerRebalanceListener` declares all three methods; the public abstract
`ConsumerRebalanceListenerBase` carries Java's `onPartitionsLost` default (delegate to
`OnPartitionsRevoked`).

`definition-of-done.md` §7 justification for the extra type: C# default interface
methods require .NET Standard 2.1 / C# 8, and this binding's floor is **netstandard2.0**
(it must load on net462). The Java default is therefore not expressible on the
interface, and dropping it would lose a Java behaviour the binding exists to restore
(`bindings/CLAUDE.md` §2). The base class exists *because of* a Java behaviour, not to
add one. The interface stays directly implementable.

Consequence at the ABI: the binding always passes a non-NULL `on_partitions_lost`
(the ABI's NULL would reproduce the default inside the core, which is redundant here).
Documented at `NativeConsumer.NewListenerHandle`.

---

## 3 · P6-D2 — option (a): sync → sync ABI, async → async ABI

The sync `Subscribe(topics, listener)` calls `Consumer_subscribe_with_listener`; the
async one calls `…_async`. This is a deliberate, visible mechanism divergence from
Python, whose **sync** `subscribe` routes through the *async* ABI
(`bindings/python/consumer.py:786-788`) purely to avoid holding the GIL across a
callback dispatch. .NET has no GIL and its shipped sync `Subscribe` already calls the
sync ABI, so the mechanism differs while the observable behaviour does not.

Recorded at the site, in the XML doc on
`NativeConsumer.Subscribe(IReadOnlyCollection<string>, IConsumerRebalanceListener)`, so
a symbol-by-symbol parity review does not file it.

---

## 4 · `consumer-threading.md` §31 test #1 — DEFERRED to the `ConsumerHandle` phase (P8)

**This is a recorded deferral, not an oversight.** §31 states the rule is "not considered
tested" without both regression tests; #2 is satisfied (§5 below), #1 is not, and cannot
be here:

- A listener calling its own consumer's `Commit()` is rejected with
  `ConcurrentModification` by the core's access guard **by design** — the application
  thread driving the rebalance holds it (`confluent_kafka.h:209-221`). The sanctioned
  reentrancy path is `kafka_consumer_ConsumerHandle_t`, which this binding does not yet
  expose; exposing it is P8's entire scope and is explicitly out of P6's.
- Even with the handle, a .NET unit test could not cover it: every **async**
  `ConsumerHandle_*` op returns `UnsupportedVersionError` on a MockConsumer-derived
  handle (`src/ffi/consumer_handle.rs:82-86`), and .NET unit tests are Mock-only, no
  broker (`bindings/dotnet/CLAUDE.md` §7.3). Choosing a vehicle (broker-backed
  integration test, harness scenario, or a written deferral) is a P8-approval decision,
  per roadmap §6.3.

The deferral is also stated in the class doc of `PublicConsumerRebalanceListenerTests`
so it is discoverable from the code, not only from this record.

---

## 5 · §31 test #2 — satisfied, **with the mutation check actually run**

`Rebalance_DoesNotAdvanceUntilTheListenerReturns` blocks the listener on a
`ManualResetEventSlim`, drives `Rebalance` from a worker thread, proves the callback was
entered (the `entered` gate is what makes the negative assertion non-vacuous), asserts
the rebalance has **not** completed, releases, and asserts it has.

**Mutation check (phase-5 notes item 3), performed and observed** — `_release.Wait()` was
removed from the `BlockingListener` fixture and the test re-run:

```
[xUnit.net 00:00:00.07]     Confluent.Kafka.UnitTests.PublicConsumerRebalanceListenerTests.Rebalance_DoesNotAdvanceUntilTheListenerReturns [FAIL]
  Failed Confluent.Kafka.UnitTests.PublicConsumerRebalanceListenerTests.Rebalance_DoesNotAdvanceUntilTheListenerReturns [5 ms]
  Error Message:
   Rebalance returned while the listener was still blocked — the rebalance advanced early.
Failed!  - Failed:     1, Passed:     0, Skipped:     0, Total:     1
```

The fixture was then restored and the test re-run green (`Passed: 1`). The test proves
something.

DoD #12 (test-fixture fidelity): the fixture is a plain synchronous
`IConsumerRebalanceListener` invoked by the production trampoline — no primitive on the
callback path is substituted.

---

## 6 · DoD notes stated rather than skipped

- **#10 (hot-path allocation audit): N/A.** A rebalance listener fires per *rebalance*,
  not per record, so there is no per-message hot path to budget. Stated in the
  `ConsumerRebalanceListenerBridgeTests` class doc as well.
- **#11 (consumer trait surface):** no `block_on`-wrapped sync façade was introduced —
  the sync overload calls the sync ABI directly (§3 above), which is the shipped
  M5/P8a precedent, not sync-over-async. `IDeserializer<T>` is untouched.
- **#6 (no duplication):** the four subscribe entry points now share one
  `SnapshotTopics` precondition helper (the validation loop was previously duplicated in
  two of them), and `Rebalance` reuses `RunPartitionOpSync` verbatim — the ABI takes the
  same parallel `(topics[], partitions[], count)` arrays as `assign`, so no new
  marshalling was added.

---

## 7 · Divergences in force (roadmap §8 wording, applied)

- **D1 — sync listener.** Applied; recorded in `IConsumerRebalanceListener`'s XML doc and
  in the `bindings/dotnet/CLAUDE.md` §4 amendment.
- **D2 — an overload, not an optional parameter.** Applied on both interfaces.
- **D3 — callbacks run on the core's dispatcher thread**, not the caller's task.
  Recorded in the interface doc and the §4 amendment.
- **D8 — copy-out inside the trampoline before `_destroy`.** Applied by reusing
  `TopicPartitionListMarshal.CopyOutAndDestroy` (which destroys in a `finally`, so the
  delivered handle is freed exactly once even when the copy-out throws).

---

## 8 · Doc-sync (D10) — what was and was not touched

Done in `bindings/dotnet/CLAUDE.md`:

1. §3 sketch: the `Subscribe(topics, listener)` overload on `IAsyncConsumer` /
   `IConsumer`, and `Rebalance(partitions)` on both mocks.
2. §3 "Still to come": the rebalance listener moved to "already wired"; the remaining
   list is now pattern subscribe, typed headers, and `IOffsetCommitCallback`.
3. §3 idiom map: the `ConsumerRebalanceListener` row corrected from "(async) … invoked on
   the caller's task" to **sync `void`**, fired on the core's **dispatcher thread**, with
   a pointer to the §4 amendment.
4. §4: a new "⚠ §4 divergence — the rebalance listener is SYNC" block carrying the
   sanctioned D1 wording plus the D3 and P6-D1 consequences, and an explicit note that
   the "takes a completion callback → the `Task` replaces the callback" row does **not**
   govern a multi-shot registration.

Deliberately **not** touched:

- The §3 idiom-map `OffsetCommitCallback` row and the §4 "do not add a callback-taking
  overload" row. Those are roadmap **Q5**, owned by **P7 (N=60)**; editing them here
  would pre-empt a phase whose surface does not exist yet.

---

## 8b · Fix round — the three `COMMENTS.59.md` findings, all documentation-only

The Critic's review of `1dbb87ef` found **no code defect**: it reproduced all five gates,
re-performed the reported mutation check verbatim, and added two production-side
injections of its own (freeing the registration `GCHandle` in the trampoline → **6 tests
red**; `Task.Run`-ing the listener, the §31 anti-pattern → **7 tests red**, including the
§31 guard). All three findings are closed by a `fixup!` on `1dbb87ef`.

**F1 (Medium) — `ffi-marshalling.md` was never updated and now forbade the shipped
design.** The file had zero occurrences of `listener`, `multi-shot` or
`user_data_destroy`, and three of its normative statements contradicted the landed code
(§B6:1160 "freed by the callback", §B6:1167 "frees the `KafkaError`", §B7:1288 "anywhere
but the callback / `AbandonBeforeSubmit`"), while §B2 had no ownership category for a
consumed-by-callee handle. Edited under explicit authorization, following the in-file §A7
precedent (qualify each Rule inline, keep one shared Anti-patterns / Tests pair):

- A **standing note at the top of Part B**: "sole owner" language is about the
  *per-operation* `GCHandle`; a multi-shot registration has its own owner.
- **§B2** — a **fifth ownership category, "consumed by callee"**, with its table row, a
  Rule bullet (including "do not infer release from the return code"), an anti-pattern
  covering both the double free and the missed native-never-ran destroy, and a note that
  a `TopicPartitionList_t` *delivered to a callback* is Category 3, not 4.
- **§B6** — the Decision now names **two families**, and the Rule is split into
  `Rule (one-shot per-operation completion — the ~8 async ops)` (unchanged text, now
  scoped) and `Rule (multi-shot registration — the rebalance listener, M9/P6)`. The new
  rule states the `static readonly` rooting requirement, the **returned** `KafkaError*`
  whose ownership transfers (the inverse of the one-shot "frees the `KafkaError`" rule),
  the callback-owns-the-delivered-list contract, the single sanctioned free site, and —
  explicitly — *where the safety of freeing from the hook actually comes from* (the
  ref-count + single serialised dispatcher, with a "re-derive if either is weakened"
  warning). Anti-patterns and Tests-required extended with the multi-shot cases.
- **§B7** — the per-op `GCHandle` anti-pattern bullet qualified so it is not read as
  forbidding the registration's hook.

The one-shot rules are left intact and correct for the one-shot paths — the point is that
the invariant does not *generalize*, not that it was wrong.

**F2 (Medium) — the D3 rationale credited the wrong mechanism.** Corrected in §1 above
and in the `ListenerRegistration` `<remarks>`. Substance: the dispatched job carries a
**raw** `user_data` copy, not the `Arc`, so the `Arc` argument only rules out a replacing
subscribe; the ref-count plus the single serialised dispatcher is what closes the window.
The "accepted residual" now analyses **both** branches of `shutdown_background`, including
the dangerous task-dropped one. No code change — the conclusion was already right.

**F3 (Low) — a test comment overclaimed.** `Rebalance_Churned_NoCorruption`'s comment
said the loop detects a leaked `TopicPartitionList_t`; it has no memory assertion, so only
the `GCHandle` half is actually guarded. The comment now says what the test really covers
and states plainly that it cannot distinguish a correct list-free from a broken one.

**Process note (not actioned by me).** The Critic observed this is the third recurrence of
"doc-sync scoped to `CLAUDE.md` alone, `ffi-marshalling.md` drifts" and judged the PLAN
template — not the Actor — to be the defect. That is being raised with the PM separately;
per the fix-round scope I did not edit any PLAN or roadmap file.

---

## 9 · Reported upward, not fixed here

`bindings/dotnet/CLAUDE.md` §4 (Serializers row) claims the deferred `IDeserializer`
headers overload is *"addable later non-breakingly as a C# default-interface-method
forwarding to the header-less form."* **That is not achievable on the netstandard2.0
floor** — default interface methods need .NET Standard 2.1 / C# 8, which is exactly the
constraint that forced P6-D1 option (b). The claim was left in place: it is unrelated to
this phase's surface and correcting it is its own item.

---

## Appendix — `COMMENTS.59.md` as filed by the Critic (all three RESOLVED)

Moved here verbatim on resolution (`agent-roles.md`); the working file is removed.
F1 / F2 / F3 are closed by the `fixup!` on `1dbb87ef` — see §8b for what each fix
actually changed.

---

# COMMENTS.59 — Critic review of M9/P6 (`1dbb87ef`), .NET binding

**Scope reviewed:** commit `1dbb87ef` on `prashah_dev_dotnet_binding_consumer` (15 files,
+1944). Reviewed against `target/include/confluent_kafka.h` (regenerated locally,
`cargo build --features ffi`) and the Kafka Java `Consumer` / `ConsumerRebalanceListener`
public API. Rules applied: `bindings/dotnet/CLAUDE.md` §3/§4/§7, `.claude/rules/ffi-marshalling.md`
Part 0 + Part B, `consumer-threading.md` §31, `definition-of-done.md` §3/§7/§12.

**Headline verdict: the memory-safety substance is sound.** No leak, no double-free and no
reachable use-after-free was found. `ListenerRegistration.Release()` is the only
`GCHandle.Free()` site for a registration; the three trampolines correctly never free;
the delivered `TopicPartitionList_t` is destroyed exactly once; the returned `KafkaError*`
is correctly *not* destroyed; the listener handle is destroyed only on the
native-never-ran path. The P6-D3 option-(a) **conclusion** holds — but its **stated proof**
attributes the safety to the wrong step (F2).

**Gates re-run independently (all green, all reproduced):**

| Gate | Result |
|---|---|
| `cargo build --features ffi` | ✅ ok |
| `dotnet build` (ns2.0 / net462 / net8.0 / net10.0) | ✅ 0 warnings, 0 errors |
| `dotnet test -f net10.0` | ✅ **504 passed**, 0 failed (matches the Actor's count) |
| `dotnet format --verify-no-changes` | ✅ clean |
| `cargo xtask lint` | ✅ clean (both clippy passes) |

**Mutation checks re-performed** (throwaway `git worktree`, `target/` symlinked; worktree
removed and both trees verified clean afterwards):

| Injection | Result |
|---|---|
| **1 — the Actor's reported one**: delete `_release.Wait()` from `BlockingListener` | ✅ **reproduced exactly** — `Rebalance_DoesNotAdvanceUntilTheListenerReturns` **RED**, sole failure (1 failed / 503 passed), verbatim message *"Rebalance returned while the listener was still blocked — the rebalance advanced early."* |
| **2 — production**: free the registration `GCHandle` in the trampoline success path (the one-shot mistake this phase exists to prevent) | ✅ **6 tests RED** — `Rebalance_Churned_NoCorruption`, `LiveRegistration_SurvivesAggressiveGc` + 4 public ones. The multi-shot invariant is genuinely guarded. |
| **3 — production**: `Task.Run(...)` the listener call in `InvokeListener` (the `consumer-threading.md` §31 fire-and-forget anti-pattern) | ✅ **7 tests RED**, including `Rebalance_DoesNotAdvanceUntilTheListenerReturns`. The §31 #2 guard detects a real production defect, not just fixture non-vacuity. |

Injection 1 was the check DoD §12 asks for and it reproduces. Injections 2 and 3 were mine
— they establish that the §31 guard and the multi-shot rule are *detectors*, not decoration.
`BlockingListener` uses production's own primitives (a plain sync listener method reached
through the real trampoline and the real `MockConsumer_rebalance`), so DoD §12 is satisfied.

---

## F1 · MEDIUM — `ffi-marshalling.md` §B6/§B7 were not updated; by their own text the shipped multi-shot registration is an anti-pattern

**Where:** `bindings/dotnet/.claude/rules/ffi-marshalling.md` §B6 (lines 1152–1168) and
§B7 (lines 1287–1290), plus §B2's ownership-category table.
**Reference:** the C ABI header's listener contract (`confluent_kafka.h:175–236`,
`:539–605`, `:2037–2161`); `bindings/dotnet/CLAUDE.md` §5 ("the correctness contracts you
must not break live in `ffi-marshalling.md`") and §8.3 ("the concrete checklist **is** the
Anti-patterns blocks in `ffi-marshalling.md`").

The file was not touched by this commit (last change `5e7e4bae`), and it contains **zero**
occurrences of `listener`, `multi-shot`, `registration` or `user_data_destroy`. Its §B6/§B7
describe only the one-shot per-operation completion, so three of its normative statements
now directly contradict the landed code:

1. **§B6 Rule, line 1160:** *"**Per-op keep-alive.** The delegate + the `GCHandle` … freed
   **exactly once** by the callback."* → The registration `GCHandle` must **not** be freed
   by the callback; freeing there is a UAF on fire 2..N (injection 2 above turns 6 tests
   red proving it).
2. **§B6 Rule, line 1167:** *"On failure it builds the exception (`FromHandle`, §B5) and
   **frees the `KafkaError`**."* → The listener trampoline does the **inverse**: it
   *constructs* an owned `KafkaError*` via `KafkaError_new` and must **not** free it
   (`confluent_kafka.h:199–206` "Do not destroy a handle you return").
3. **§B7 Anti-patterns, lines 1288–1290:** *"freeing the per-op `GCHandle` from **anywhere
   but** the callback (the sole owner) / `AbandonBeforeSubmit` (native never ran)."* →
   `ListenerRegistration.Release()` driven by the `user_data_destroy` hook is a **third**
   free site, and it is the correct one.

Additionally, **§B2's four ownership categories have no row for a consumed-by-callee
handle.** `ConsumerRebalanceListener_t` is a fifth shape: owned by the binding between
`_new` and the subscribe call, then **consumed unconditionally — including on the error
path** (`confluent_kafka.h:181–183`, `:2106–2107`), so `_destroy` after a successful
submit is a double free. The code gets this exactly right; the rulebook does not describe it.

This is not a nitpick about scope. The PLAN's D10 named only `bindings/dotnet/CLAUDE.md`,
so the Actor followed the plan — but the DoD gate is *doc-matches-code*, not
*only-the-planned-file-matches*, and the next Actor/Critic pair reading §B6/§B7 literally
will flag `ListenerRegistration.Release()` as a use-after-free. This is the third recurrence
of the same shape in this binding's history.

**Suggested fix — use the in-file precedent, §A7.** §A7 already handles "two mechanisms in
one section" correctly by qualifying each Rule inline (`**Rule (Option A — superseded …)**`
/ `**Rule (Option B) …**`) and keeping *one* shared Anti-patterns / Tests-required pair.
Apply the same to §B6/§B7:

- Split the §B6 Rule into `Rule (one-shot per-operation completion)` and
  `Rule (multi-shot registration — rebalance listener, M9/P6)`, the latter stating: the
  `GCHandle` is freed **only** by the `user_data_destroy` hook (never by a listener
  callback); the callback **returns** an owned `KafkaError*` that must not be destroyed;
  the callback **owns and destroys** the delivered `TopicPartitionList_t`.
- Qualify the §B7 anti-pattern bullet as *"the **per-op** `GCHandle`"* and add the
  registration counterpart: *"freeing a **registration** `GCHandle` from a listener
  callback or from teardown — the release hook is its sole owner."*
- Add the §B2 fifth row: `ConsumerRebalanceListener_t` — *consumed by callee; destroy only
  if it never reached native.*

---

## F2 · MEDIUM — the D3 evidence chain attributes the safety to the wrong step, and its "accepted residual" analyses only the benign half of a two-branch outcome

**Where:** `src/Confluent.Kafka/Internal/ListenerRegistration.cs` lines 50–74 (the
`<remarks>` "P6-D3 evidence chain" and "Accepted residual" paragraphs), mirrored in the
commit message and `COMMENTS.DONE.59.md` §1 steps 2–4.

I verified step 2's citations myself and they are **individually accurate**: every
production invocation site does bind an owned `Arc` clone to a local before awaiting.
I enumerated the complete non-test set repo-wide (`invoke_partitions_*` /
`on_partitions_*` call sites) and there are exactly three, all cited:
`mock_consumer.rs:262` and `:272` (via `subscription_state.rs:887-888`, confirmed
`.clone()`), `async_kafka_consumer.rs:3285/3290/3295` (via the `:3273` clone) and
`:5327/5331` (via the `:5317` clone). No uncited path exists.

**But the conclusion drawn from them is stronger than the mechanism supports.** The
dispatched job does **not** capture the `Arc`. `FfiRebalanceListener::invoke`
(`src/ffi/consumer.rs:3118-3132`) copies the pointer out first —
`let user_data = SendUserData(self.target.user_data);` — and the `move` closure handed to
`dispatch_and_wait` carries only that raw copy plus the fn pointer. `dispatch_and_wait`
(`src/ffi/common.rs:350-369`) **enqueues the job and then awaits**; its own comment concedes
*"the only way `send` fails is a cancelled awaiting task"*, and `enqueue_or_run_inline`'s
doc states the dispatcher *"runs every queued job before exiting"*.

So the strong-count-≥-1 guarantee holds **only while the awaiting future is alive**. If that
future is ever dropped, the `Arc` drops in the same instant → `CallbackTarget::drop` →
the hook → `Release()` → `GCHandle.Free()`, while a **still-queued job holding the raw
`user_data`** can afterwards call the managed trampoline, which does
`GCHandle.FromIntPtr(userData).Target` on a freed handle. That is the UAF shape.

The "Accepted residual" paragraph names `runtime.shutdown_background()` as the drop point
but reasons about only one of its two outcomes — *"a task leaked that way could retain a
listener reference … a **bounded leak, not a use-after-free**"*. The **other** outcome
(the task **is** dropped) is the dangerous one and is not analysed anywhere.

**I traced it and it is NOT reachable today** — so this is an accuracy finding, not a bug:

- `Consumer_destroy` runs only at ref-count zero (`SafeConsumerHandle.ReleaseHandle`), and
  a listener callback only fires *inside* an operation that holds a count — a sync call via
  the `SafeConsumerHandle` marshaller AddRef (`ConsumerSubscribeWithListener`,
  `MockConsumerRebalance` — both correctly declared with the `SafeHandle` parameter), an
  async op via the span-the-op `DangerousAddRef` released in `FreeGcHandle`.
- The M9/P4 Q1/Q3 **deferred-destroy** path (dispose racing an unawaited op) fires
  `Consumer_destroy` from `FreeGcHandle` **on the dispatcher thread**, which is the same
  single thread that would run a queued listener job — so the two are serialised, never
  concurrent. And the op cannot complete before its own listener callbacks have returned,
  so no listener job can be queued behind it.
- `Consumer_close_with_timeout` is not a second dropper: it passes the timeout **into**
  `close_with_options` under `block_on` (`src/ffi/consumer.rs:4057-4064`) rather than
  wrapping the future in a droppable `tokio::time::timeout`.

**So the load-bearing guarantee is step 4 (the ref-count) plus the single serialised
dispatcher — not step 2's `Arc` argument.** Step 2 correctly rules out trigger 4 (a
replacing subscribe, which never drops the invoking future); it does not, on its own, rule
out "the hook fires while a dispatched job is still pending".

**Suggested fix** (comment only, no code change):

1. In the evidence chain, scope step 2 to what it proves: *"a replacing subscribe cannot
   fire the hook mid-callback, because it does not drop the invoking future"* — and add
   explicitly that the job carries a **raw** `user_data` copy, so the `Arc` protects it only
   for as long as the awaiting future lives.
2. Say plainly that the **ref-count + single-dispatcher serialisation is what closes the
   window**, and that the four .NET paths to `Consumer_destroy` were enumerated (the three
   bullets above are the enumeration).
3. Rewrite the "Accepted residual" paragraph to state **both** branches of
   `shutdown_background`: task leaked → bounded `GCHandle` leak (already there); task
   dropped → the hook fires, and safety then rests entirely on no listener job being
   in-flight, which the ref-count guarantees.

This matters because that comment is precisely the artifact the next maintainer will reason
from, and the Actor itself flagged the cross-layer invariant as "the one place a reviewer
could reasonably disagree". It is right to be flagged — but it should be flagged as *the*
guarantee, not as a backstop behind an `Arc` argument that does not cover the case.

---

## F3 · LOW — `Rebalance_Churned_NoCorruption`'s comment claims a detection the test cannot perform

**Where:** `tests/Confluent.Kafka.UnitTests/Interop/ConsumerRebalanceListenerBridgeTests.cs`,
`Rebalance_Churned_NoCorruption` — *"The loop is the corruption detector for the two
per-fire frees: the delivered `TopicPartitionList_t` (`CopyOutAndDestroy`) and … the
registration `GCHandle`. … a leaked list would grow the heap unboundedly."*

The GCHandle half is genuinely guarded (my injection 2 turned this test red). The **list**
half is not: the test's only assertions are `listener.Assigned.Count == 200` and
`!IsReleased` — neither observes native memory, so a leaked `TopicPartitionList_t` would
pass silently. The binding does the right thing (`CopyOutAndDestroy` frees in a `finally`,
and it is shared, pre-existing, proven code), so this is a **comment overclaim, not a
coverage gap** — I am not asking for a new memory-budget test here.

**Suggested fix:** drop the "a leaked list would grow the heap unboundedly" clause, or
qualify it as *"the list free is inherited from the shared `CopyOutAndDestroy` and is not
asserted here"*. Phrasing matters: the guard is correct but the test cannot distinguish it
from a broken one, which is a different statement from "it is tested".

---

## Verified clean — checked and found correct (recorded so the next reviewer need not redo it)

- **Multi-shot lifetime.** `ListenerRegistration.Release()` is the sole `GCHandle.Free()`
  site; `Interlocked.Exchange` + `IsAllocated` make it idempotent (a second
  `GCHandle.Free()` would throw). `NativeConsumer.Dispose` correctly does **not** free it —
  a teardown-side free would be the §B7 UAF.
- **Trampoline rooting.** All three callbacks plus the release hook are
  `static readonly` `[UnmanagedFunctionPointer(Cdecl)]` fields, so the native thunks cannot
  be collected while a registration lives. `LiveRegistration_SurvivesAggressiveGc` covers it.
- **No-throw boundary.** `InvokeListener`'s `try` covers copy-out, `GCHandle` recovery *and*
  the user call; `ListenerError` is itself no-throw (nested fallback to a null message, then
  to `IntPtr.Zero`). Nothing can unwind into native.
- **Three ownership contracts, all correct against the header.** Delivered list →
  `CopyOutAndDestroy` (destroy in a `finally`, exactly once, no hand-rolled second free, no
  borrowed pointer escaping). Returned error handle → built with `KafkaError_new`, never
  destroyed (`confluent_kafka.h:199-206`, `:629-631`). Listener handle → destroyed **only**
  in the two `catch` blocks where the P/Invoke itself threw, i.e. native never ran; both
  error paths verified, including that `SubmitVoidOperation` invokes its submit lambda
  **synchronously** so the `consumed` flag is correct when the outer `catch` reads it.
- **The header's two documented oddities are handled by construction.** The binding never
  infers release from a return code: the empty-topic-list-releases-on-success case and the
  rejected-after-registration case both work because `_listenerRegistration` is
  observability-only and `IsReleased` (set by the hook) is the authoritative signal. The
  field comment says exactly this.
- **Replacing registration.** `ReplacingListenerSubscribe_ReleasesTheOldRegistrationOnly` /
  `ReplacingListenerlessSubscribe_ReleasesTheRegistration` /
  `Unsubscribe_DoesNotReleaseTheRegistration` pin the Java-faithful rules; release is
  exactly once and cannot overlap an in-flight callback under single-owner.
- **Marshalling (§0.1).** All six declarations `Cdecl` + explicit `EntryPoint`; `int` for
  `int32_t`, no `UIntPtr`/`nint`; opaque handles as `IntPtr`; hand-rolled UTF-8 in and out;
  non-ASCII topic round-trip test present. Sync declarations take `SafeConsumerHandle`, the
  async one keeps the raw `IntPtr` for the span-the-op ref — both correct per §B2/§A2.
- **API shape.** `IConsumerRebalanceListener` = three sync `void` methods (D1, correct: the
  ABI callback is a sync fn pointer and the rebalance blocks on it, `confluent_kafka.h:208-213`);
  `ConsumerRebalanceListenerBase` carries Java's `onPartitionsLost` default as `virtual` →
  `OnPartitionsRevoked`; `Subscribe(topics, listener)` is an overload on both interfaces and
  all four impls (D2); `Rebalance` is inherent on the two mocks and correctly **absent** from
  the interfaces. Nullability is precise; preconditions throw standard .NET exceptions before
  any pin/P-Invoke, never `KafkaException`.
- **P6-D2** (sync overload → sync ABI, async → async ABI) is recorded **at the site**, in the
  `NativeConsumer.Subscribe(topics, listener)` `<remarks>`, with the Python-divergence reason.
- **D10 doc-sync.** `bindings/dotnet/CLAUDE.md` §3 sketch, §3 "Still to come", the §3
  idiom-map listener row and the new §4 divergence block all match what shipped and overclaim
  nothing. (The gap is `ffi-marshalling.md` — F1.)

---

## Not filed (approved decisions / out of scope)

§31 test #1 deferred to P8; `ConsumerRebalanceListenerBase` existing (P6-D1(b)); the sync
listener (Q6); the `IDeserializer` default-interface-method line in `CLAUDE.md` §4; the
`OffsetCommitCallback` / "do not add a callback-taking overload" rows (P7 / Q5); the missing
commit callback / `ConsumerHandle` / gRPC RPCs (P7 / P8 / P9); stale `target-linux*/` natives
and `callback_log.rs:41`; the accepted `shutdown_background` **leak** residual (its *other*
branch is F2). Rust core internals and the ABI itself are `kafka-critic`'s scope.

---

## Verdict

**P6 needs one fix round, and it is documentation-only.** F1 (rulebook) and F2 (the safety
comment) should both land before the phase closes — F1 because `ffi-marshalling.md` is the
next agent's contract and currently forbids the shipped design, F2 because the safety
argument as written credits the wrong mechanism. F3 is a one-clause comment edit. No code
change is required: the implementation is correct, the gates reproduce, and all three
mutation checks (one re-performed, two new) show the guards genuinely bite.

---

## Rule-update suggestions (`COMMENTS.FP.md` / `COMMENTS.FN.md` are both absent)

1. **`ffi-marshalling.md` — make the one-shot/multi-shot split explicit** (the F1 fix). Also
   worth adding a standing note at the top of Part B: *"§B6/§B7's 'sole owner' language is
   about the **per-operation** `GCHandle`. A multi-shot registration has its own owner — the
   ABI's `user_data_destroy` hook."*
2. **`bindings/dotnet/CLAUDE.md` §6.2 (Mode A checklist)** — add a step 5:
   *"If the feature introduces a new callback/ownership **shape**, update the matching
   `ffi-marshalling.md` section in the same commit; a doc/code mismatch there is a Critic
   finding."* Every phase PLAN so far has scoped doc-sync to `CLAUDE.md` alone, and the
   rulebook has now drifted three times.
3. **`definition-of-done.md` §12** — extend the fixture-fidelity rule with a second clause:
   *"A mutation check reported in a phase record must name the exact injection and the exact
   resulting failure message, so a reviewer can re-perform it."* The Actor did this here and
   it made verification a two-minute job; making it a rule would generalise the practice.
