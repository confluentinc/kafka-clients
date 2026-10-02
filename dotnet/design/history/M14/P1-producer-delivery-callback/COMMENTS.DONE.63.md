# COMMENTS.DONE.63 — resolved Critic findings, M14/P1 (".NET producer delivery callback")

Source review: `COMMENTS.63.md` (Critic verdict **PASS-WITH-FINDINGS**, 4 LOW, all
documentation/process) plus its fix-cycle-1 re-review, which confirmed findings 1/2/4
fixed and raised **finding 5** (LOW) — resolved in **fix cycle 2** at the end of this
file. Findings 1, 2 and 4 are resolved below. **Finding 3 (no
`design/current/STATUS.md` entry) remains OPEN in `COMMENTS.63.md` — it is
Manager-owned** (STATUS is written in the Manager's phase close-out commit; the M12/P1
precedent is `7fc34a8e` = STATUS + archived PLAN + `COMMENTS.DONE.34.md`), and
deliverable 1.6 correctly did not name STATUS. The Critic's own "possibly Manager-owned"
read is right; the Actor deliberately did not touch it.

All three fixes are **prose only** — no behavioural change, no new API, no code path
altered. Mode A held (zero diff to `src/**`, `src/ffi/**`,
`target/include/confluent_kafka.h`, `cbindgen.toml`, `tests/**`); the generated header
hash is unchanged (`7d8ad0af…3ccd7`) and the `[DllImport]` count is unchanged (227).

---

### 1. RESOLVED — the at-most-once residual was under-stated: three non-firing paths, one note

**What was wrong.** Three paths fault a send's awaiter without firing its
`IDeliveryCallback`, and only `Enqueue`'s `_stopped` fault-in-place carried a site note.
`DrainAndFaultRemaining` had none, and `RunLoop`'s `catch` → `FaultBatchCompletions` was
a **third, non-teardown** path that the public xmldoc residual (which said "teardown")
did not cover *and* which contradicted `OnCompletion`'s own guarantee — after `get_all`
returns, a part-way `ProcessBatch` throw leaves the unreached indices faulted with no
`Fire`, although the core reported completions for all of them.

**Decision — doc-scoping, NOT a latch (deliberate, and the reasoning is now recorded at
the site).** Firing from `FaultBatchCompletions` was rejected: that method faults the
batch **wholesale** (`TrySetException`'s no-op-on-completed behaviour is load-bearing
there) and keeps no per-index record of which callbacks already fired, so firing would
deliver a **duplicate** notification for every index that completed before the throw.
Under an exactly-once-per-record obligation (root `CLAUDE.md` §9.5) a duplicate is
strictly worse than a drop — a double invocation is the classic FFI callback defect the
harness's settle-window assertion exists to catch. A per-index "already fired" latch is
the only correct closure and does not earn its per-send state on an OOM-only path.

**What changed.**

- `src/Confluent.Kafka/IDeliveryCallback.cs` — `OnCompletion`'s summary now states a
  **scoped** guarantee (see the exact wording below), and the interface remarks replace
  the single "Recorded residual — teardown" paragraph with **Recorded residuals — the
  exhaustive set of paths that fault a send WITHOUT notifying you**, a numbered list of
  three: (1) teardown raced the enqueue, (2) teardown drained a still-queued send, (3)
  an unexpected managed failure (in practice OOM) between the completion arriving and
  the callback being invoked — explicitly flagged as *not* a teardown path and as the
  one where the core *did* report the completion. The rejected "fire from the fault
  path" alternative and its duplicate-vs-drop reasoning are stated there too.
  Residual 3's sync-surface counterpart (the same narrow window for one record) is
  named rather than claiming the sync surface is residual-free — it is not, under the
  same OOM lens.
- `src/Confluent.Kafka/Internal/SendCompletionPump.cs` — site notes added at
  `DrainAndFaultRemaining` (residual 2) and `FaultBatchCompletions` (residual 3,
  including the do-not-fire-here reasoning); `Enqueue`'s existing note re-labelled
  "residual 1" so the three sites and the public enumeration share one numbering.
- `.claude/rules/ffi-marshalling.md` §A6 form C — the *"State the at-most-once
  boundary"* Rule now demands the boundary be enumerated **exhaustively**, names both
  shapes (teardown ×2 sites, plus the batch-abort/OOM shape), requires a note at
  **each** faulting site so sites and public statement cannot drift, and records
  duplicate-is-worse-than-drop.
- `.claude/rules/ffi-marshalling.md` §A7 — the M14/P1 amendment now also records that
  **one at-most-once residual belongs to the pull-pump engine rather than to form C**
  (the pump loop's wholesale batch fault), so §A7's account agrees with §A6's boundary
  instead of mentioning only the slow-callback cost.
- `bindings/dotnet/CLAUDE.md` §4 — the delivery-callback divergence's
  **Exactly-once, per record** bullet now reads "**Recorded residuals — three, not
  one**" and carries the same enumeration plus the duplicate-vs-drop reasoning.

**The exact final wording of the scoped guarantee** (`IDeliveryCallback.OnCompletion`):

> Invoked when the record's send completes (Java `onCompletion(RecordMetadata,
> Exception)`, `Callback.java:61`) — **at most once per record**, never twice, on any
> path. It is invoked **exactly once** for every record whose core-reported completion
> the binding reads and turns into the send's result: every normal outcome, success or
> failure, including a record whose awaiter had already been canceled. It is **not**
> invoked where the binding faults a send *itself* instead of reporting a core
> completion — a bounded set of three conditions (two teardown paths plus an
> out-of-memory window), enumerated exhaustively under **Recorded residuals** in the
> remarks on `IDeliveryCallback`.

---

### 2. RESOLVED — the non-concurrency promise is now scoped, and the sync-flavor reality is stated as a Java divergence

**What was wrong.** The async bullet's *"It is a single thread per producer, so callbacks
of one producer never run concurrently with each other"* read as a promise for the whole
interface, while the sync bullet said only *"inline on the calling thread"* — and
`IProducer` **explicitly encourages** concurrent `Send` with no user lock (ffi §A1 lists
a binding-side send lock as an anti-pattern). So one shared `IDeliveryCallback` instance
handed to concurrent sync `Send` calls **is** entered on N caller threads at once, with
nothing in the contract saying so. Java never does this (single background I/O thread,
`Callback.java:20-21`), so a Java user never makes a `Callback` thread-safe. A user who
trusted the old wording would write an unsynchronized callback.

**What changed.**

- `src/Confluent.Kafka/IDeliveryCallback.cs` — the async bullet's guarantee is now
  scoped *per producer* (with the companion note that one instance shared across **two**
  producers is still concurrent, one pump thread each), and the sync bullet states the
  reality, the user obligation, and the Java divergence outright.
- `src/Confluent.Kafka/IProducer.cs` — the callback-taking `Send`'s remarks gained a
  paragraph deriving the obligation from the "concurrent `Send` is supported" paragraph
  already on the same interface, so a user reading only the overload sees it.
- `bindings/dotnet/CLAUDE.md` §4 — recorded as a **sub-divergence** under the
  delivery-callback divergence's **Thread** bullet (the location the Critic asked for).

**Final wording — the sync bullet** (`IDeliveryCallback` remarks, "Which thread it runs
on"):

> **Sync** (`IProducer<TKey, TValue>`) — **inline on the calling thread**, before `Send`
> returns or throws. The blocking send has no pump (it waits on the record's future
> itself), so there is no other thread to run it on — and therefore **no non-concurrency
> guarantee at all**. Concurrent `Send` on one producer is explicitly supported and
> deliberately unsynchronized (the Rust core's `Mutex` serializes, and a binding-side
> send lock is an ffi §A1 anti-pattern), so one `IDeliveryCallback` instance passed to
> `Send` from N threads **is** entered on N threads at once. **What this means for you: a
> callback instance you share across concurrent sync sends must itself be thread-safe** —
> synchronize any mutable state it touches (a per-send instance needs nothing). This is a
> recorded **divergence from Java**, where every `Callback` runs on the producer's single
> background I/O thread (`Callback.java:20-21`) and a user never has to make one
> thread-safe; see the §4 delivery-callback divergence in the binding's `CLAUDE.md`.

**Final wording — the async bullet's scoping:** *"It is a single thread per producer, so
callbacks of one producer never run concurrently with each other … (The guarantee is
per-producer, so one callback instance shared across two producers can still be entered
concurrently — one pump thread each.)"*

**Where the Java divergence is recorded:** `bindings/dotnet/CLAUDE.md` §4, inside the
"⚠ §4 divergence — the delivery callback is SYNC…" block, as a ⚠ sub-divergence appended
to the **Thread** bullet — stating that the async pump is one thread *per producer*, that
the sync surface has no such thread and no binding-side lock, that Java's single I/O
thread means Java never does this, and that the public surface therefore states the
thread-safety obligation on the user.

---

### 4. RESOLVED — "the callback and the awaiter report the same thing" corrected to match the code

**Assessment: the Critic is right that this is a prose fix, not a code fix.** The
coercion is *required* — `IDeliveryCallback.OnCompletion` carries `KafkaException?`, so a
non-`KafkaException` marshal failure (an OOM decoding the topic on an
already-succeeded send) cannot be handed over unwrapped. The claim, not the behaviour,
was inexact: the awaiter receives the raw exception while `Fire` receives
`new KafkaException("The send completed but its result could not be marshalled.",
failure)`, so type and `Message` differ on that one path. No behavioural change made.

**What changed** — all four sites that stated the equality now state the precise
relation (same *failure* always; same *object* for every `KafkaException` outcome; wrapped
on the single marshal-failure path, original preserved as `InnerException`):

- `src/Confluent.Kafka/Internal/SendCompletionPump.cs` (the pump's marshal-failure
  branch comment)
- `src/Confluent.Kafka/Internal/NativeProducer.cs` (the sync send's symmetric comment)
- `src/Confluent.Kafka/Internal/DeliveryRegistration.cs` (`Fire`'s `<param name="failure">`)
- `src/Confluent.Kafka/IDeliveryCallback.cs` ("Two distinct error surfaces")

---

## Gate results after the fix (full DoD re-run, prose-only change notwithstanding)

| Gate | Result |
|---|---|
| `cargo build --features ffi` (debug) | exit 0 |
| `cargo build --features ffi --release` | exit 0 (both profiles current — the stale-native trap) |
| Header hash before/after | `7d8ad0af…3ccd7` **unchanged** (Mode-A proof) |
| `dotnet build Confluent.Kafka.sln -c Release` | **0 Warning(s), 0 Error(s)**, 6 TFM outputs — the real gate for xmldoc edits (`TreatWarningsAsErrors` + `GenerateDocumentationFile` make CS1574/CS0419 errors) |
| Regenerated `Confluent.Kafka.xml` (×3 TFMs) | carries the new prose — proves the doc comments recompiled, not just that the build was up to date |
| `dotnet test -f net10.0 --no-build` | **Failed: 0, Passed: 818, Skipped: 0** |
| `dotnet test -f net8.0 --no-build` | **Failed: 0, Passed: 818, Skipped: 0** |
| TFM matrix | net10.0 + net8.0 suites green; **net462** assembly builds 0W/0E (execution is CI-only — no Mono/Framework on this macOS host, as in every prior phase) |
| `dotnet format --verify-no-changes` | exit 0, clean |
| `cargo xtask format-check` | clean |
| `cargo xtask lint` | no lint issues |
| `cargo test --lib` | **3693 passed; 0 failed; 3 ignored** |
| Mode A — `git diff HEAD -- src/ cbindgen.toml target/include/confluent_kafka.h tests/` | **empty** |
| `[DllImport]` count | **227 → 227**; `Producer_send_async` / `ProducerSendAsync` still not declared |

---

# Fix cycle 2 — finding 5 (raised by the Critic's re-review of `e9451d49` · `e73ea093` · `061ae2de`)

Fixed in `c0f56097` · `4acc99b0` · `9168c488` (three `fixup!`s, one per autosquash target
— same last-toucher-per-file split the Critic independently reproduced in cycle 1).
**Prose only**: comments, xmldoc and rulebook text; no code path altered, no behaviour
changed, no test changed. Mode A held; header hash `7d8ad0af…3ccd7` unchanged;
`[DllImport]` 227.

### 5. RESOLVED — the "exactly three paths, and no others" claim was one short: a FOURTH residual, not a widened third

**What was wrong.** Fix cycle 1 (finding 1) replaced a vague residual clause with an
explicit **exhaustiveness** claim, and that claim became the attack surface.
`NativeProducer.SendViaPump`'s orphaned-future `catch` satisfies the new residual
definition verbatim — the record **was accepted by the core** (`Producer_send` returned a
live future *and* a null `out_error`, which is the ABI's statement of acceptance), the
completion is never read, `delivery` is dropped un-fired, and `Send` throws (the
load-bearing "(or `Send` throws)" parenthetical) — yet it is none of the three:

  - residual 3's own wording scoped it to *"between the **completion arriving** and the
    callback being invoked"*, and here the failure is **before** the completion arrives;
  - the D5 bulleted list attributed every no-callback throw to either *"nothing was
    sent"* or *"the core rejected the record before accepting it"* — **neither** is true
    on this path;
  - and it was the only faulting site in the send path carrying **no** residual note,
    which is precisely how it stayed off the public enumeration.

**Decision — a distinct FOURTH residual, not a widened residual 3.** Both options were
offered; the fourth item was chosen for three reasons:

  1. **Residual 3's characterisation is true and load-bearing.** It is the only residual
     where the completion had already *arrived* (i.e. the core did report it), and that
     is exactly what makes its no-fire argument — *firing from a wholesale batch fault
     would duplicate the notification for indices that already fired* — apply to 3 and
     only 3. Widening the window to "after the core accepted the record" falsifies that
     sub-claim and would force an internal re-split of the item anyway: more contortion,
     not less.
  2. **The new path's no-fire reason is residuals 1 and 2's, not residual 3's.** No
     completion is ever read, so anything fired there is an *invented failure* for a
     record the core may still deliver successfully — the teardown rationale, verbatim.
     Folding it into 3 would attach the wrong reasoning to it.
  3. **It preserves the 1:1 "site note says residual N" invariant** the Critic verified:
     `Enqueue` → 1, `DrainAndFaultRemaining` → 2, `FaultBatchCompletions` (+ the sync
     `NativeProducer.Send:570` window) → 3, and now `SendViaPump`'s orphaned-future
     `catch` → 4.

**The callback is deliberately NOT fired on the new path** — the Manager's instruction and
the prose's own reasoning for residuals 1 and 2 agree, and the path is OOM-only. No
behaviour was changed to "fix" it.

**Final wording of the residual statement** (`IDeliveryCallback.cs`, remarks):

> **Recorded residuals — the exhaustive set of paths that fault a send, or throw out of
> `Send`, WITHOUT notifying you.** The callback reports a *core* completion, so wherever
> the binding faults a send *itself* instead of reading one, the notification is dropped:
> the send's `Task` faults (or `Send` throws) and the callback does not fire. The set below
> is not a remembered list — it is obtained by walking *every* site between the core's
> acceptance of the record (a live future and no synchronous error) and the callback's
> invocation that can fault the send or throw out of `Send`. There are exactly four such
> sites, and no others:
>
> 1. **Teardown raced the enqueue** (async only) …
> 2. **Teardown drained a still-queued send** (async only) …
> 3. **An unexpected managed failure between the completion arriving and the callback
>    being invoked** — in practice an `OutOfMemoryException`; *not* a teardown path, and
>    the only residual where the completion had already *arrived*, i.e. where the core did
>    report it. …
> 4. **An allocation failure between the core accepting the record and the send being
>    handed to the completion pump** (async only) — again in practice an
>    `OutOfMemoryException`, constructing the send's awaiter or its cancellation
>    registration. The record *was* accepted (the core returned a live future and no
>    error) and may still be delivered, but the binding destroys that future unread and
>    rethrows, so no completion is ever read. It is *not* a teardown path, and unlike
>    residual 3 the completion had not arrived. This is the residual that surfaces as a
>    **throw out of `Send`** rather than as a faulted `Task`, which is why the D5 outcome
>    list above cannot attribute every no-callback throw to "nothing was sent". The
>    synchronous surface has no such window: it reads its own record's completion
>    immediately, with nothing allocated in between.
>
> Read two ways: by *cause*, residuals 1 and 2 are teardown while 3 and 4 are unexpected
> managed failures (out of memory); by *what the core reported*, only residual 3 had a
> completion in hand.

**The D5 bulleted list DID need correcting**, exactly as the Critic suspected: its
dichotomy is falsified by this path. It gains a third no-callback bullet — *an allocation
failure thrown after the core accepted the record but before the binding could arrange to
read its completion → **no callback**; the one no-callback throw where the record was
accepted and may still be delivered, so it is neither "nothing was sent" nor "the core
rejected it": it is a recorded drop, residual 4* — and the D5 **rule sentence** above it
was tightened twice: *"a send whose core-reported completion the binding **reads** —
successfully or not — fires it"* (was "a send that reached the core and then completed",
which residual 4 falsifies), plus an explicit note that the rule's two halves are **not**
complements.

**What changed.**

- `src/Confluent.Kafka/IDeliveryCallback.cs` — the residual enumeration widened to four
  and given its **derivation** rather than a list to recall; the header widened to "fault
  a send, **or throw out of `Send`**"; the no-fire paragraph split so residual 3 keeps the
  duplicate-risk argument while 1/2/4 get the invented-failure one; the async-only
  attribution corrected to "1, 2 and 4" (with the Python comparison left where it belongs,
  on 1 and 2); the D5 rule sentence + bulleted list corrected as above; `OnCompletion`'s
  summary now says *"a bounded set of four conditions (two teardown paths plus two
  out-of-memory windows, one either side of the completion arriving)"*.
- `src/Confluent.Kafka/Internal/NativeProducer.cs` — **the missing site note**, at
  `SendViaPump`'s orphaned-future `catch`: recorded residual 4, why it is the only
  no-callback throw on that path where the record **was** accepted (contrasted with
  `ThrowIfClosed` / the already-canceled token / `ProducerSendMarshal.Send`'s `out_error`),
  why firing a fabricated failure would be wrong, and that it is reachable only under OOM.
  `SendViaPump`'s `delivery` param doc no longer says a throw out of it means "nothing was
  sent" — true of three of its four throw sites, false of this one.
- `src/Confluent.Kafka/Internal/SendCompletionPump.cs` — the three site notes now say
  "of the **four** enumerated"; `FaultBatchCompletions`' distinguishing claim changed from
  "the only one that is *not* a teardown path" (residual 4 is not teardown either) to
  "the only one where the completion had already **arrived**".
- `.claude/rules/ffi-marshalling.md` §A6 form C — the at-most-once Rule went from **two
  shapes to three**, and from a list to recall to a **method to apply**: *walk every site
  that faults the send or throws out of `Send` after the ABI accepted the record*. The new
  middle shape (a managed failure **before** the completion is read) is called out as the
  one that surfaces as a *throw* rather than a faulted `Task` — the reason a residual
  definition scoped to "the binding faults the send" excludes it, and the reason a public
  list may not equate "throws" with "nothing was sent". The "note at each faulting site"
  clause now says *why* it matters (a site with no note is how this path stayed unlisted).
- `.claude/rules/ffi-marshalling.md` §A7 — the engine-residual ⚠ paragraph went from
  **one** to **two** residuals owned by the pull-pump engine: the pre-enqueue window joins
  the batch abort, since both are artifacts of the pump's shape (a future the caller must
  hand over; a pump that resolves in batches) and a push engine has no handoff at all.
- `CLAUDE.md` §4 (delivery-callback divergence) — **"Recorded residuals — three, not
  one" → "four, not one"**, with the pre-handoff window described and its throw-not-fault
  surfacing called out; the *"which outcomes fire it"* bullet no longer implies that
  accepted-and-later-completed always fires. Not in the Critic's three-place list, but
  leaving it at "three" would have re-created the very drift this finding is about.

**Autosquash dry run (throwaway detached `git worktree`, removed afterwards).**
`GIT_SEQUENCE_EDITOR=true git rebase -i --autosquash 3f959b46` → *Rebasing (2/10) …
(10/10) Successfully rebased*, exit 0, **no conflict**; **4** resulting commits, **zero**
`fixup!` remaining; `git diff 9168c488 <squashed>` empty over the **whole tree** (0 lines).
Per-commit file lists confirm each fixup landed in the commit that last wrote those files.

### Gate results after the fix cycle 2

| Gate | Result |
|---|---|
| `cargo build --features ffi` (debug) | exit 0 |
| `cargo build --features ffi --release` | exit 0 (both profiles — the stale-native trap) |
| Header hash before/after | `7d8ad0af…3ccd7` **unchanged** (Mode-A proof) |
| `dotnet build Confluent.Kafka.sln -c Release` | **0 Warning(s), 0 Error(s)**, 6 TFM outputs — the real gate for xmldoc edits |
| Regenerated `Confluent.Kafka.xml` (×3 TFMs) | each carries "residual 4" ×3 + "exactly four such sites" — proves the doc comments recompiled |
| `dotnet test -c Release -f net10.0 --no-build` | **Failed: 0, Passed: 818, Skipped: 0** |
| `dotnet format --verify-no-changes` | exit 0, clean |
| `cargo xtask format-check` | clean |
| `cargo xtask lint` | no lint issues |
| `cargo test --lib` | **3693 passed; 0 failed; 3 ignored** |
| Mode A — `git diff 3f959b46..HEAD -- src/ cbindgen.toml target/include/confluent_kafka.h tests/` | **empty** |
| `[DllImport]` count | **227**; `Producer_send_async` / `ProducerSendAsync` still not declared |
| Phase-2 scope | `bindings/dotnet/grpc-server/**` untouched |

---

# Fix cycle 3 — finding 6 (raised by the Critic's re-review of `c0f56097` · `4acc99b0` · `9168c488`)

Fixed in `4ed35aa0` · `af16e7d3` · `898729f0` (three `fixup!`s, one per autosquash target —
the same last-toucher-per-file split the Critic independently reproduced in cycles 1 and 2).
**Not prose-only this time**: the Manager's ruling split the finding into a required prose
fix (part 1) and a code fix for the handle leak the Critic filed out-of-cycle (part 2).
Mode A held; header hash `7d8ad0af…3ccd7` unchanged; `[DllImport]` 227 (218 `static extern`).

### 6. RESOLVED — the pump's residual window was scoped to "AFTER the completion arrives"; it starts one frame earlier

**What was wrong.** `SendCompletionPump.ProcessBatch` allocates its three marshalling arrays
**outside** its `try`, and the `try`'s **first** statement is the `get_all` P/Invoke. So the
pump can throw with **no completion in hand** → `RunLoop`'s `catch` → `FaultBatchCompletions`
→ every TCS faulted, no `Fire`. That falsified two sentences added in fix cycle 2
(`IDeliveryCallback.cs` *"the only residual where the completion had already arrived"*;
`SendCompletionPump.cs` *"`get_all` has already returned by the time `ProcessBatch` can
throw"*) and the two axes derived from them, mirrored in `ffi §A6` shape 3 and `CLAUDE.md` §4.
The code already knew: `RunLoop`'s own catch comment names *"a native failure surfacing from
`get_all`"*. And the "OOM-only ⇒ theoretical" defence is **unavailable** here — a stale or
mismatched native surfaces `EntryPointNotFoundException`/`DllNotFoundException` from the
pump's *first* `get_all`, the exact artifact the project's own gate discipline warns about.

**Part 1 — widened residual 3, did NOT add a fifth.** The inverse of cycle 2's choice, from
the same principle (one numbered note per **site**): this condition reaches residual 3's
*existing* site, so splitting it out would force `FaultBatchCompletions` to carry two
residual numbers and break the 1:1 invariant. Final public wording (`IDeliveryCallback.cs`):

> **An unexpected failure on the completion pump, after the send was handed to it and before
> the callback was invoked** — *not* a teardown path. It spans **both sides of the
> completion's arrival**, because the pump can throw on either side of its batched read and
> one wholesale-fault site covers both. **(a) After** the read reported: it reported for the
> *whole* batch, so the core *did* report these completions; the indices the pump had already
> reached fired normally and the rest are faulted with none. In practice an
> `OutOfMemoryException`. This is the sub-case that makes firing from the fault path unsafe
> (see below), and the one the synchronous surface shares — it has the same narrow window for
> its own single record, between reading that record's completion and invoking the callback.
> **(b) Before** the read reported: the pump threw while setting the batch up, or out of the
> batched read itself, so no completion was ever in hand and the whole batch is faulted.
> Unlike every other residual here this one does **not** need an allocation failure to be
> reachable — a native-side failure surfacing from the pump's first batched read, for example
> an `EntryPointNotFoundException` against a stale or mismatched native library, lands here.

Both derived axes repaired: by **cause**, residual 4 and sub-case (a) are unexpected managed
failures but sub-case (b) is also reachable through a native-library failure, so "out of
memory" no longer characterises the non-teardown residuals as a group; by **what the core
reported**, a completion had arrived only in sub-case (a). The "not fixed by firing"
paragraph now splits by *reason* (duplicate risk for (a); invented failure for 1, 2, 4 and
(b)), and the async-only paragraph adds (b). The false
`SendCompletionPump.cs` sentence is **deleted**. Also added, because this was a
condition-vs-site confusion rather than a missing site: the walk preamble now states its
**terminating condition** (through every frame the future travels, on both threads, including
frames *outside* a method's own `try`) and says outright that there are four **sites**,
numbered per site, with residual 3's site reached by **two conditions**. Mirrored in
`ffi §A6` shape 3 + the walk instruction (recorded as the **third** miss, with the converse
trap: verify each note's *distinguishing clause*, not just the presence of a note),
`ffi §A7`'s batch-abort bullet, and `CLAUDE.md` §4. Residual 4's site note in
`NativeProducer.cs` carried the same falsified claim and was corrected too (the Critic's
finding did not list that file — found by structural sweep).

**Part 2 — the handle leak: IMPLEMENTED.** On the pre-`try` sub-path `ProcessBatch`'s
`finally` never runs, so the batch's future handles leaked, while `:396-397` claimed to
*"complete the 'free every handle on every path' pattern"*. The naive fix (move the
allocations inside the `try`) is wrong for the reason the Manager gave: the `finally` would
run with `futures` null or unpopulated. Implemented instead: a dedicated `try`/`catch` around
the three allocations whose `catch` frees the futures **from `batch` itself** — valid before
anything is allocated — with the **singular** `FutureRecordMetadata_destroy` per element,
because building the array `destroy_all` needs is exactly what failed.

Free-exactly-once proof, per throw point:

| Throw point | Freed by | Double-free? |
|---|---|---|
| `new IntPtr[count]` ×3 (the only pre-`try` throw) | the allocation `catch`, singular destroy per `batch[i].Future` | No — the `catch` rethrows, so the processing `try`/`finally` is never entered. No metadata/error handle exists yet (`get_all` not called) |
| the `futures[i] = batch[i].Future` copy loop | n/a — **cannot throw** (List<T> indexer below its own Count + readonly-struct property read, no allocation) | n/a |
| `FutureRecordMetadataGetAll` (first statement in the `try`) | the `finally`'s `destroy_all`; metadata/errors all `Zero`, so the sweep is a no-op | No — the allocation `catch` did not run |
| mid-loop at index *i* | indices `<i` freed as consumed (slots nulled); index *i* freed by its own branch (`meta` in the inner `finally`, `error` inside `FromHandle`'s `finally`); indices `>i` by the sweep; all futures by `destroy_all` | No — the null-the-slot discipline is untouched |
| post-loop | nothing follows the loop inside the `try` | n/a |

The two future-free sites are **mutually exclusive by construction**: reaching the processing
`try` requires the allocation `try` to complete normally, and the `catch` always rethrows.
The existing null-the-slot discipline is unchanged, not weakened.

**No test.** The path is not deterministically reachable: its only trigger is an allocation
failure inside a `private static` method with no injection seam, and adding one would
restructure the pump — which this cycle is explicitly scoped against.

**Two unprescribed edits, both in the same defect class** (a free path that must not depend on
allocation succeeding, and a false statement in a comment):

  - `NativeProducer.SendViaPump`'s orphaned-future `catch` freed via
    `FutureRecordMetadataDestroyAll(new[] { future }, 1)` — **allocating inside an
    OOM-only recovery path**, where that `new[]` can throw the same OOM and leak the very
    handle it exists to free. Switched to the singular destroy.
  - The comment there claimed *"the singular `FutureRecordMetadata_destroy` is **not
    wired**"*. It has been wired since M11/P4 and is used by the sync `Send` **in the same
    file** (`NativeProducer.cs:632`). Corrected, along with the singular destroy's own
    remarks in `NativeMethods.cs` (one call site named → three) and the section comment
    above it.

**Out of scope, flagged not fixed (Manager's call).** `RunLoop`'s `DrainAll()` (`:243`) sits
**outside** the `try` that guards `ProcessBatch`, so an OOM there escapes the `while` loop
entirely: the pump thread dies without `Stop` ever setting `_stopped`, the dequeued sends'
TCSes are never completed (**stranded**, not faulted) and their futures leak. It is outside
the residual definition (it neither faults a send nor throws out of `Send`, which is why the
Critic correctly declined to file it), OOM-only, and closing it would restructure the pump
loop. Recorded here so it is not lost.

## Gate results after the fix (full DoD re-run)

| Gate | Result |
|---|---|
| `cargo build --features ffi` (debug) | exit 0 |
| `cargo build --features ffi --release` | exit 0 (both profiles built before the Release .NET gate — the stale-native trap) |
| Header hash before/after | `7d8ad0afd4d1af2e108373a22f5ec76af8d970dcbe84191c473de4a830e3ccd7` **unchanged** |
| `dotnet build Confluent.Kafka.sln -c Release` | **0 Warning(s), 0 Error(s)**; 6 outputs (lib ns2.0/net8.0/net10.0 + tests net462/net8.0/net10.0) |
| `dotnet test -c Release -f net10.0 --no-build` | **Failed: 0, Passed: 818, Skipped: 0** |
| `dotnet test -c Release -f net8.0 --no-build` | **Failed: 0, Passed: 818, Skipped: 0** |
| net462 | builds clean; cannot execute locally (no Mono host) — CI-run |
| `dotnet format --verify-no-changes` | exit 0, clean |
| `cargo xtask format-check` | clean |
| `cargo xtask lint` | no lint issues |
| `cargo test --lib` | **3693 passed; 0 failed; 3 ignored** |
| Mode A — `git diff 3f959b46..HEAD -- src/ cbindgen.toml target/include/confluent_kafka.h tests/` | **empty** |
| `[DllImport]` count | **227 → 227** (218 `internal static extern` in `NativeMethods.cs`, unchanged) |
| Phase-2 scope | `bindings/dotnet/grpc-server/**` untouched |
| Autosquash dry run | throwaway worktree, `GIT_SEQUENCE_EDITOR=true git rebase -i --autosquash 3f959b46` → exit 0, 13 steps, **4** resulting commits, **zero** `fixup!` remaining, squashed-vs-current diff **0 lines**; worktree removed |

**One flake observed and excluded, with evidence.** The first `-f net10.0` run failed 1/818 in
`ConsumerPollBridgeTests.ResultBridge_RunsContinuationsAsynchronously_OffTheCompletingThread`
(`Assert.NotEqual() Failure: Not 32 / Actual 32` — the continuation landed on the same
thread-pool thread). It is a **consumer** bridge test in a file no M14/P1 commit touches, and
three consecutive re-runs were 818/818. Recorded rather than silently re-run.

---

# Fix cycle 4 — finding 7 (raised by the Critic's audit of `4ed35aa0` · `af16e7d3` · `898729f0`)

**Prose-only.** Mode A held; header hash `7d8ad0af…3ccd7` unchanged; `[DllImport]` unchanged
(**227** lines mentioning `DllImport`, of which **221** carry the literal `[DllImport` attribute
and **218** are `internal static extern` — `NativeMethods.cs` is absent from this cycle's diff
entirely; it was last touched by cycle 3's `4ed35aa0`, comment-only, as the Critic verified). The Manager's instruction was explicit that the two flagged clauses were
**not** the deliverable: the deliverable is an **exhaustive enumeration of every comparative,
distinguishing, or counting claim about the residuals across all five documents, each verified
individually against the code**. That enumeration is the table below. It found **6 further false
claims** beyond the two the Critic filed — three of them in the very code comments that the last
three rounds' fixes had touched.

### 7. RESOLVED — a repaired distinguishing clause was not carried to its public twin, and five more paraphrases of the same axes were stale

**What the Critic filed.** `af16e7d3` repaired residual 4's *"unlike residual 3, where the
completion had arrived"* clause at `NativeProducer.cs:475-477`, because `898729f0` had widened
residual 3 with sub-case (b) — where no completion was ever in hand, putting residual 4 and 3(b)
in the **same** position on that axis. The **public xmldoc** copy of the identical clause
(`IDeliveryCallback.cs:181-182`) was not carried over, and self-contradicted `:191-193` eight
lines below. Secondary: `:172-173`'s *"Unlike every other residual here this one does not need an
allocation failure"* over-reached by two — residuals 1 and 2 are teardown and need none either;
`CLAUDE.md` §4, `ffi §A7` and `:188-191` all carry the **non-teardown** qualifier and only this
clause dropped it. Both confirmed, both fixed.

## The enumeration (the deliverable)

Every claim containing an *only* / *unlike* / *every other* / *the one where* / *four* / *three* /
*two* / *both* / *never* / *always* construction that distinguishes one residual from another or
asserts a count, across the five documents. Line numbers are pre-fix. Claims that are not about
the residual set (thread-topology `never`s, close-family `unlike`s, mock-reachability `only`s) are
excluded by the Manager's own scoping and were spot-checked, not tabled.

| # | Document | Line | The claim | Verdict | Fix |
|---|---|---|---|---|---|
| 1 | `IDeliveryCallback.cs` | 181-182 | residual 4 — *"unlike residual 3 the completion had not arrived"* | **FALSE** | Now names sub-case **(a)** as the only one where a completion had arrived, and puts 4 alongside 1, 2 and 3(b) — mirrors the wording `af16e7d3` landed at `NativeProducer.cs:475-477`. **(Critic-filed primary.)** |
| 2 | `IDeliveryCallback.cs` | 172-173 | 3(b) — *"Unlike **every other residual** here this one does not need an allocation failure to be reachable"* | **FALSE** | Scoped to the **non-teardown** conditions (residual 4 and sub-case (a)), plus an explicit parenthetical that 1 and 2 need none either, being ordinary teardown races. **(Critic-filed secondary.)** |
| 3 | `SendCompletionPump.cs` | 497-498 | residual 3 — *"**the one residual that is not a teardown path**"* | **FALSE** | Residual 4 is **also** non-teardown (`IDeliveryCallback:181`, `CLAUDE.md:725-726` *"The other two are not teardown"*, `ffi §A7:958` *"Both are non-teardown members"*). Comparative dropped; the note now asserts only residual 3's own defining property (it spans both sides of the completion's arrival) and points at the canonical axes. **This is the parallel instance the Manager predicted would survive a spot fix.** |
| 4 | `SendCompletionPump.cs` | 70 | *"(deterministic, **no accepted residual** on the enqueue-vs-stop race)"* | **FALSE** *(since M14/P1)* | True pre-M14 and about **strand/leak**; M14/P1 then recorded residual **1** at exactly that branch. Now reads *"no **strand or leak** residual … That branch does drop the send's delivery notification, which is recorded residual 1 — see `Enqueue`."* A reader auditing "which races have residuals" would have been told the wrong answer by the type's own summary. |
| 5 | `IDeliveryCallback.cs` | 112 | *"(the **fourth** entry below, and residual 4 …)"* | **FALSE** (off-by-one) | The D5 outcome list has five `<item>`s; the allocation-failure entry — the one being pointed at — is the **third** (`:122-126`). Fixed to "the third entry below". |
| 6 | `NativeProducer.cs` | 361-362, 469-472 | *"On **three of the four** throw sites"* / *"the other three (ThrowIfClosed, the already-canceled token, and `ProducerSendMarshal.Send`'s synchronous `out_error`)"* | **FALSE** (undercount) | `SendViaPump` has **five** throw sites, four pre-acceptance: the two named guards, `ProducerSendMarshal.Send`, **and `EnsurePump()`** — which re-checks `ThrowIfClosed` under `_pumpLock` (`:804`) and can also fail starting the pump thread. Recast from a brittle count to the enumerated reasons, so the substantive claim (residual 4 is the **only** post-acceptance one) no longer rides on a number. |
| 7 | `IDeliveryCallback.cs` | 166-169, 206-208 | 3(a) is *"**the one** the synchronous surface shares"* / the sync surface *"shares **only** sub-case (a)"* | **FALSE** (too narrow) | The sync `Send`'s window between the blocking `get` reporting and `Fire` carries **both** conditions. (a): `KafkaException.FromHandle(getError)` allocates only on the failure branch → an OOM there drops the notification **with a completion in hand**. (b): `NativeMethods.FutureRecordMetadataGet` is its **own** `[DllImport]` (`NativeMethods.cs:2339-2340`, `EntryPoint = kafka_producer_FutureRecordMetadata_get`), resolved lazily and independently of `_get_all` (`:2298`) and of `Producer_send` — so a stale/mismatched native throws `EntryPointNotFoundException` from it **with none in hand**, which is precisely (b)'s own cited mechanism. Now states that residual 3 is shared in **both** conditions, and that what is async-only *inside* residual 3 is (b)'s batch-**setup** half plus the wholesale-fault **site**. |
| 8 | `IDeliveryCallback.cs` | 182 | residual 4 is *"**the** residual that surfaces as a throw out of `Send`"* | **FALSE** once #7 is stated | On the sync surface residual 3's shared window also throws out of `Send` (no `Task` exists there). Scoped: *"On the **async surface** this is the only residual that surfaces as a throw…"* — where it is true. |
| 9 | `IDeliveryCallback.cs` | 229-230 | *"They differ in exactly one case: on **the residual path** where the send succeeded but its metadata could not be marshalled"* | TRUE, term collision | The *claim* is correct (verified: `ProcessBatch:399-407` and `NativeProducer.Send:610-623` are the only sites handing `Fire` a non-`KafkaException`). But calling it "the residual path" collides with the defined term two paragraphs up — the callback **does** fire there. Reworded with an explicit *"not one of the recorded residuals above, since the callback does fire there"*. |
| 10 | `IDeliveryCallback.cs` | 149-153 | *"exactly **four** such sites, and no others"*; *"each site carries exactly **one** numbered note"*; *"a site is not the same as a condition"* | TRUE (count), amended | Four sites re-walked and confirmed (see below). The one-note-per-site sentence is amended only because #7's fix adds a second note for residual 3, on the sync send. |
| 11 | `IDeliveryCallback.cs` | 188-193 | *"By cause: 1 and 2 are teardown, while 4 and 3(a) are unexpected managed failures … By what the core reported: a completion had arrived **only** in 3(a); 1, 2, 4 and (b) **never** read one"* | **TRUE** | Both axes verified against the code. This paragraph is now designated the **canonical** statement of the axes (see the structural decision). |
| 12 | `IDeliveryCallback.cs` | 196-203 | *"**None of the four** is fixed by firing"*; *"3(a) … cannot tell which indices already fired"*; *"1, 2, 4 and 3(b) **never** read a completion"* | **TRUE** | `FaultBatchCompletions` faults wholesale with no per-index record; the other three destroy or abandon unread. |
| 13 | `IDeliveryCallback.cs` | 241-249 | *"**at most once** per record, **never** twice"*; *"**exactly once** for every record whose core-reported completion the binding reads"*; *"a bounded set of **four** residuals (**two** teardown, plus **two** unexpected-failure windows)"* | **TRUE** | Amended additively for #7 (naming that the sync surface shares residual 3), not corrected. |
| 14 | `IDeliveryCallback.cs` | 46-49, 52-53 | *"callbacks of **one** producer **never** run concurrently"*; *"**no** non-concurrency guarantee at all"* (sync) | **TRUE** | One pump thread per `NativeProducer` (`SendCompletionPump` ctor); the sync path has none. Cycle-1 finding 1's fix, re-verified. |
| 15 | `SendCompletionPump.cs` | 131, 457-459 | residual 1 / residual 2 notes: *"recorded residual N **of the four**"*, *"the **same reasoning** as `Enqueue`'s fault-in-place branch"* | **TRUE** | Both are teardown, neither issues a blocking `get_all`. |
| 16 | `SendCompletionPump.cs` | 183 | *"There is **no third case**"* (enqueue before vs after the gate closes) | **TRUE** | Not a residual claim; verified anyway — `Enqueue`/`CloseGate` share `_stopLock`. |
| 17 | `SendCompletionPump.cs` | 288-294, 331-334 | *"**exactly one** of those two future-free sites can ever run"*; *"Freed **exactly once**, and **never** double-freed"* | **TRUE** | The Critic's cycle-4 audit independently re-derived this line by line; re-confirmed (`catch` always rethrows, so the `try`/`finally` is never entered on that path). |
| 18 | `SendCompletionPump.cs` | 343-344 | *"**Cannot throw**: a `List<T>` indexer read below its own `Count` plus a readonly-struct property read, with no allocation"* | **TRUE** | Confirmed by the Critic's audit; `PendingSend` is a `readonly struct` with `IntPtr` / reference members, no boxing. |
| 19 | `SendCompletionPump.cs` | 361-367 | *"the finally-sweep frees **ONLY** the indices this loop never reached … so it can **never** double-free"* | **TRUE** | Slots nulled before use. |
| 20 | `SendCompletionPump.cs` | 399-404 | *"This is the **ONLY** path where the two differ; **every** `KafkaException` outcome is passed to both unwrapped"* | **TRUE** | Matches `DeliveryRegistration.Fire`'s coercion switch (`:130-136`) and `NativeProducer.Send:617`. |
| 21 | `SendCompletionPump.cs` | 507-518 | *"sub-case (a) **alone** is enough to settle it"*; *"**no** per-index record of which callbacks already fired"* | **TRUE** | — |
| 22 | `NativeProducer.cs` | 472 | residual 4 is *"the **only** no-callback throw **on this path** where the record WAS accepted"* | **TRUE** | Scoped to `SendViaPump`; the acceptance boundary is `ProducerSendMarshal.Send` returning (verified: nothing after the native call in that marshaller allocates — `FromHandle(IntPtr.Zero)` is a null-safe no-op, so it is **not** a fifth site). |
| 23 | `NativeProducer.cs` | 478-480 | *"Reachable **only** under out-of-memory — the TCS, the cancellation registration and its continuation are the **only** allocations in the try"* | TRUE (claim), incomplete list | "Only under OOM" holds. The *enumeration* omitted that `pump.Enqueue` can grow the `ConcurrentQueue`'s segment array. Harmless (a throw there leaves the future unowned, so the `catch` frees it correctly) but tightened, since an incomplete "only" list is how #6 happened. |
| 24 | `NativeProducer.cs` | 537-549 | *"**The two** throw sources are deliberately not equivalent"* (sync `Send`) | **FALSE** (incomplete) | There is a **third**: the residual-3 window this surface shares (#7), which fires **nothing** for a record that **was** accepted. Added as a third list item, explicitly flagged as *not* a fifth residual site. This also repairs the ffi §A6 *"put a note at **each** faulting site"* obligation — the sync surface's share carried no note at all. |
| 25 | `NativeProducer.cs` | 550-554 | *"**no analogue** of Java's `catch (ApiException)` row"* | **TRUE** | The core surfaces those through the record's future, not the sync out-param. |
| 26 | `DeliveryRegistration.cs` | 120-121 | *"the two surfaces report the same failure on **every** path, but not the same object on **that one**"* | **TRUE** | — |
| 27 | `CLAUDE.md` §4 | 668 | *"**Unlike the two** consumer callback divergences this is **not** an ABI-shape divergence at all"* | **TRUE** | Zero new `[DllImport]`; form C never crosses the boundary. |
| 28 | `CLAUDE.md` §4 | 671 | *"**Seven** parts"* | **TRUE** | Seven bullets counted. |
| 29 | `CLAUDE.md` §4 | 709-712 | *"as residual 4 below records"*; *"Those two clauses are **not** complements"* | **TRUE** | — |
| 30 | `CLAUDE.md` §4 | 720-727 | *"Recorded residuals — **four, not one**"*; *"**Two** are teardown"*; *"The **other two** are not teardown"* | **TRUE** | — |
| 31 | `CLAUDE.md` §4 | 734-737 | *"that half is **the one not** confined to out-of-memory"* | **TRUE** | Scoped by the preceding *"The other two are not teardown"* — which is exactly the qualifier `IDeliveryCallback:172` (row 2) had dropped. The contrast between these two sites is what makes row 2 an oversight rather than an intended reading. |
| 32 | `ffi §A6` form C | 706-710 | *"it happened **three times**"*; *"the walk yields **three shapes**"*; *"(**two** distinct sites)"* | **TRUE** | Still three "list one short" misses — round 4 was a *stale clause*, not a short list, so the count stands. |
| 33 | `ffi §A6` form C | 727-738 | *"the **only** half the duplicate-risk argument covers"*; *"the half that is **not** OOM-only"*; *"**Neither half** is a teardown path"* | **TRUE** | — |
| 34 | `ffi §A6` form C | 739-745 | *"a faulting site with no note is exactly how the pre-read window stayed off the public list while **three other** sites carried one"*; the converse trap | **TRUE** | And the converse trap is what fired this round — extended, see below. |
| 35 | `ffi §A7` | 936-966 | *"**TWO** at-most-once residuals belong to THIS engine"*; *"A push engine has **no** such handoff"*; *"**both sides** of the batch read"*; *"**unlike** the pre-enqueue window, this residual is **not** OOM-only"*; *"**Both** are non-teardown members"*; *"**Only** a per-index latch would close the after-the-read half"* | **TRUE** | Every clause verified. Note §A7 scopes its claims to the *engine's* residuals and never asserts anything about the sync surface, so #7 leaves it intact. |

**Score: 8 false rows (1–8), 1 term collision (9), 26 verified true.** Every false row is fixed in
this cycle. Six of the eight were **not** filed by the Critic; three of those six (rows 3, 4, 6)
live in code comments that cycles 1–3 had edited, which is the concrete evidence for the
structural change below.

**The four sites, re-walked (not recalled).** `ProducerSendMarshal.Send` returning a live future
and a null `out_error` → `DeliveryRegistration.Fire`, through every frame on both threads,
including frames outside each method's own `try`: (1) `SendCompletionPump.Enqueue`'s `_stopped`
branch; (2) `DrainAndFaultRemaining`; (3) `FaultBatchCompletions` via `RunLoop`'s `catch`;
(4) `SendViaPump`'s orphaned-future `catch`. **No fifth**, and two candidates were checked and
rejected: nothing in `ProducerSendMarshal` after the native call allocates, and the sync `Send`'s
window is residual 3's *condition* on a surface with no batch, not a site of its own (row 7).

## Structural decision — point, do not paraphrase (the Manager's question)

**Adopted, for the code comments; the two rulebooks keep their statements.** Argument from the
table rather than from taste:

- All 8 false rows were **paraphrases of an axis the paraphrasing site does not own**. Not one
  false row is in `ffi §A6`/`§A7` (rows 32–35: 100% true) — because those state the *method* and
  the *count*, never per-residual comparatives. `CLAUDE.md` §4 (rows 27–31) is likewise clean, and
  row 31 is the case where its correctly-**scoped** clause is what proved row 2 wrong.
- So the defect is not "five documents" — it is **paraphrase**, and it is concentrated exactly in
  the four code-side notes. Removing it there costs nothing a reader needs: a note's job is to say
  *which* residual this site is and *why* no core completion exists here, both purely local facts.
- Therefore: `IDeliveryCallback`'s remarks are now the **single** place the residuals are compared,
  and they say so; a **third** axis (*how the send surfaces the failure* — faulted `Task` vs throw
  out of `Send`) was added there because it was previously implicit in two site notes and nowhere
  stated. Each of the four notes now carries residual number + local reason + a pointer, and an
  explicit *"do not restate those axes here"*, naming that this very spot went stale.
- The rulebooks keep their prose deliberately: they are read *without the code open* and their job
  is to constrain a future agent. Stripping them would trade a verified-true statement for a
  dangling reference. `ffi §A6` instead gains the single-source rule as a **structural** rule, plus
  the round-4 lesson (*repair a duplicated clause in every document; grep its distinctive words
  before calling it done*) and a note of the three further instances this sweep found.

## Two things deliberately NOT done

- **No code change.** The Manager's constraint was to STOP and report a genuine code defect rather
  than fix it inline. The sweep found none: rows 17–22 re-confirm the finding-6 code fix, and the
  one behavioural question it raised (row 7's sync-surface window) is an already-accepted residual
  condition, correctly *recorded* rather than closed — closing it would need a per-index/per-send
  "already fired" latch on an OOM-or-stale-native path, which `ffi §A6` explicitly says does not
  earn its state.
- **Finding 3 (`STATUS.md`) untouched and left OPEN** — Manager-owned, as in cycles 1–3.

## Gate results after the fix (full DoD re-run)

| Gate | Result |
|---|---|
| `cargo build --features ffi` (debug) | exit 0 |
| `cargo build --features ffi --release` | exit 0 (both profiles built **before** the Release .NET gate — the stale-native trap) |
| Header hash before/after | `7d8ad0afd4d1af2e108373a22f5ec76af8d970dcbe84191c473de4a830e3ccd7` **unchanged** |
| `dotnet build -c Release --no-incremental` | **0 Warning(s), 0 Error(s)**; 6 outputs (lib ns2.0/net8.0/net10.0 + tests net462/net8.0/net10.0). Non-incremental deliberately: the xmldoc must be **re-parsed** for this to be a real gate — verified by grepping the new wording out of all three emitted `Confluent.Kafka.xml` files |
| `dotnet test -f net10.0` | **Failed: 0, Passed: 818, Skipped: 0** (first run clean — no thread-id flake this cycle) |
| `dotnet test -f net8.0` | **Failed: 0, Passed: 818, Skipped: 0** |
| net462 | builds clean; cannot execute locally (no Mono host) — CI-run |
| `dotnet format --verify-no-changes` | exit 0, clean |
| `cargo xtask format-check` (from the **repo root**) | clean |
| `cargo xtask lint` | doc-comment hygiene clean; no clippy issues |
| `cargo test --lib` | **3693 passed; 0 failed; 3 ignored** |
| Mode A — `git status --short -- src/ cbindgen.toml tests/ target/include/confluent_kafka.h` | **empty** |
| `[DllImport]` count | **unchanged**: 227 lines mentioning `DllImport` / 221 literal `[DllImport` attributes / 218 `internal static extern`; `NativeMethods.cs` absent from this cycle's diff (`git diff 898729f0..HEAD` = 5 files, none of them it) |
| Phase-2 scope | `bindings/dotnet/grpc-server/**` untouched |

**Counting note for the record, so it is not mistaken for a regression.** The "227" figure used
since cycle 1 is `grep -c 'DllImport'` — *lines mentioning* the token, six of which are prose/doc
mentions. `grep -c '\[DllImport'` (literal attributes) is **221** and `grep -c 'static extern'` is
**218**. All three match the pre-cycle value exactly; the file was not edited this cycle.

---

# Fix cycle 5 — finding 8 (raised by the Critic's grading of the cycle-4 enumeration, `ad8e758f` · `3db994a8` · `ef09c677`)

**Status: RESOLVED** in `12f715dc` · `7d9d21f8` · `bbbcf671` (three `fixup!` commits, targeted by
last toucher per file: `SendCompletionPump.cs` → `a133b0a1`; `NativeProducer.cs` + `CLAUDE.md` +
`ffi-marshalling.md` → `edb62286`; `IDeliveryCallback.cs` → `24465a4e`).
Prose only; zero behaviour change, zero test change. Mode A held
(`NativeMethods.cs`, `src/**`, `src/ffi/**`, the header, `cbindgen.toml` and `tests/**` all absent
from this cycle's diff), header hash `7d8ad0af…3ccd7` unchanged, `grpc-server/**` untouched.

**Finding 3 (`STATUS.md`) remains OPEN and MANAGER-OWNED** — untouched, not moved.

## The change is a different KIND of fix, and that is the point

Rounds 2, 3, 4 and 5 each **re-worded** a comparative clause about the residuals, and each
re-wording produced the next round's false clause. Finding 8's clause (b) was written *last round,
while fixing exactly this class of defect*. Re-scoping the five flagged clauses a sixth time was
therefore the one option with a track record of failing.

So this cycle does not re-scope them. **It deletes the uniqueness and count quantifiers.** A claim
that is not made cannot go stale. Concretely:

  - **The canonical enumeration** — `IDeliveryCallback`'s interface-level `<remarks>` block — remains
    the **one** place the residuals are compared with each other and counted, because it is the one
    place that enumerates all of them and can therefore be checked as a whole. Clauses (a) and (b)
    were fixed *there*, by making them accurate.
  - **Everywhere else** — the four code-side notes, the two `<param>`/summary blocks, `CLAUDE.md` §4
    and `ffi §A6`/`§A7` — every uniqueness quantifier (*the one* / *the only* / *every other*), every
    count of residuals / sites / throw sources, and every definite-article exclusivity ("*the*
    residual reachable through …") was **removed**, not re-scoped. Each site now states its own
    local fact — what happens on *this* path and why no core completion exists here — and **points**
    at the canonical enumeration for how it relates to the others.

This is a **strictly shrinking** change: it removes claims and adds none about the code, so unlike
every previous round it cannot introduce a new false claim. That property is why the shape was
chosen over a sixth re-scoping.

## The terminating condition (the primary deliverable)

**Claim.** Outside `IDeliveryCallback`'s canonical enumeration (its interface-level `<remarks>`
block, `IDeliveryCallback.cs:1-258`), there are **zero** uniqueness or count claims about
residuals / no-callback throws / throw sources.

**Corpus** — the six documents that carry the delivery-callback contract: `IDeliveryCallback.cs`
(lines 259+ only, i.e. everything after `</remarks>`), `Internal/NativeProducer.cs`,
`Internal/SendCompletionPump.cs`, `Internal/DeliveryRegistration.cs`, `CLAUDE.md`,
`.claude/rules/ffi-marshalling.md`.

**Probes** (`grep -nEi`, run over that corpus):

```
SUBJ='residuals?|no-callback|throw sources?|such sites?|faulting sites?|numbered notes?|teardown paths?'
CNT='residuals?|sites?|shapes?|throw sources?|paths?|conditions?|halves|notes?'

P1  (uniqueness) = (the (one|only|sole)|only one|every other|in contrast to)[^.]{0,70}(SUBJ)
                 | (SUBJ)[^.]{0,70}(is|are|was|were) the (one|only|sole)
                 | (SUBJ)[^.]{0,40}(the only|the one)

P2  (counts)     = \b(one|two|three|four|five|both)\b[^.]{0,35}(SUBJ)
                 | (SUBJ)[^.]{0,35}\b(two|three|four|five)\b
                 | of the four enumerated | the other (two|three|four) (CNT)
                 | four, not one | fifth residual | all (three|four) (CNT)

P3  (definite-article exclusivity on the halves/windows)
                 = the (one|only|sole)[^.]{0,40}(half|window) | the half that is
                 | reachable only through an unexpected | is the one \*\*not\*\* | is the half that
```

**Result — HEAD (before this cycle): 17 + 1 hits. Working tree (after): 1 + 1 hits, both
classified out of scope.** The probes are therefore *sensitive*: a green run means something.

| | HEAD | after |
|---|---|---|
| P1 | 2 | 1 |
| P2 | 15 | 0 |
| P3 | 3 (of which 1 out of scope) | 1 (the same out-of-scope one) |

The two survivors, in full:

  - `ffi-marshalling.md` (§A6, round-5 amendment) — *"…**no** count of residuals, sites or throw
    sources…"*. This is the **prohibition itself**, naming the forbidden vocabulary so the rule is
    checkable. Any probe for that vocabulary necessarily matches the rule that names it. Not an
    instance.
  - `SendCompletionPump.cs:436` — *"the allocation catch above, which covers the one window this
    `finally` cannot reach"*. Pre-existing, unchanged, and about the **handle-free** audit (which
    window the `finally` cannot reach), not about the callback residuals — the Critic verified that
    completeness claim TRUE in cycle 4. Out of the condition's subject matter.

Two further hits deliberately **left alone**, both out of the corpus/subject and both flagged here so
they are not miscounted:

  - `NativeMethods.cs:1603` — *"shape difference from the other three"*, about a timestamp-shape
    difference between ABI overloads. Also: `NativeMethods.cs` must stay out of the diff.
  - `NativeConsumer.cs:2791` — *"the only residual left on this type is the deferred destroy"*, the
    **consumer's** M9/P8 ref-counted-destroy residual. A different residual family entirely.
  - `ffi §A7`'s *"So, unlike the pre-enqueue window above, this residual is **not** OOM-only"* —
    correct (it is about residual 3 **as a whole** versus residual 4, asserting neither uniqueness
    nor a count), and explicitly protected by the Manager's brief. Untouched.

## The edits, before → after

### (a) MEDIUM — the unscoped uniqueness in the D5 outcome list, and its co-located copy

**1. `IDeliveryCallback.cs:124` (canonical enumeration → made accurate).**

  - before: *"…→ **no callback**. This is **the one** no-callback throw where the record *was*
    accepted and may still be delivered, so it is neither "nothing was sent" nor "the core rejected
    it": it is a recorded *drop*, residual 4 below;"*
  - after: *"…→ **no callback**. **Here** the record *was* accepted and may still be delivered, so
    **this outcome** is neither "nothing was sent" nor "the core rejected it": it is a recorded
    *drop*, residual 4 below;"*

The quantifier is **gone**, not re-scoped. The clause the Critic proved false ("the one") is no
longer asserted at all, so the sync surface's residual-3 window — the counter-example — no longer
contradicts it, and `:192-193`'s cross-reference to this list still reads correctly.

**2. `CLAUDE.md` §4 "Which outcomes fire it" (the co-located copy).**

  - before: *"…without invoking the callback, `KafkaProducer.java:1069-1081` — **this covers** the
    precondition throws, the serializer's `SerializationException`, a synchronous core rejection,
    and — as residual 4 below records — an allocation failure *after* the core accepted the record);
    … Those two clauses are not complements: **residual 4 is** a record the core accepted whose
    completion is never read."*
  - after: *"…without invoking the callback, `KafkaProducer.java:1069-1081`); … Those two clauses are
    not complements: **a record can be accepted by the core and still never have its completion
    read**. **Which throws sit on which side of that line is enumerated once, under Recorded
    residuals in `IDeliveryCallback`'s remarks — do not restate it here.**"*

The incomplete "this covers" enumeration is deleted rather than extended, and the singling-out of
residual 4 is replaced by the general fact plus a pointer.

### (b) MEDIUM — the exclusivity introduced last round, and its two rulebook copies

**3. `IDeliveryCallback.cs:171-177` (canonical enumeration → made accurate, by deletion).**

  - before: *"…the whole batch is faulted. **Unlike the *other* non-teardown conditions — residual 4
    and sub-case (a), both reachable only through an unexpected *managed* failure, in practice out of
    memory —** this one does **not** need an allocation failure to be reachable: a native-side
    failure surfacing from the pump's first batched read, for example an
    `EntryPointNotFoundException` … lands here. **(Residuals 1 and 2 need no allocation failure
    either — they are ordinary teardown races; the contrast drawn here is with the non-teardown
    conditions only.)**"*
  - after: *"…the whole batch is faulted. **This condition** does **not** need an allocation failure
    to be reachable: a native-side failure surfacing from the pump's first batched read, for example
    an `EntryPointNotFoundException` … lands here."*

Pure deletion. The false characterization of sub-case (a) — falsified by the bare non-allocating
P/Invokes in its window (`RecordMetadataDestroy` in `ProcessBatch`'s inner `finally`, which runs
before that index's `Fire`; and `KafkaException.FromHandle`'s first statement `NativeMethods.Code`,
each a separately-resolved `[DllImport]`) — is not re-scoped, it is removed. The trailing
parenthetical existed only to prop up the deleted contrast, so it went with it.

**4. `IDeliveryCallback.cs`'s "by cause" axis (same defect, one paragraph below — found by
structural sweep, not filed).** The finding cited `:171-173`, but the axes paragraph carried the
identical claim and would have been the round-6 parallel instance.

  - before: *"By *cause*: residuals 1 and 2 are teardown, while **residual 4 and residual 3's
    sub-case (a) are unexpected managed failures (in practice out of memory)** — but residual 3's
    sub-case (b) is also reachable through a native-library failure, so "out of memory" does not
    characterize the non-teardown residuals as a group."*
  - after: *"By *cause*: residuals 1 and 2 are teardown; residuals 3 and 4 are unexpected failures,
    and "out of memory" does not characterize them as a group — **the frames in residual 3's window
    include bare P/Invokes, so an entry-point failure against a stale or mismatched native library
    reaches it too.**"*

Accurate for **both** of residual 3's conditions (sub-case (a): `RecordMetadata_destroy`,
`KafkaError_code`; sub-case (b): `get_all`), and it no longer attributes a *managed*-only cause to
sub-case (a).

**4b. Two further OOM attributions to sub-case (a), removed for internal consistency with 4.** The
Critic did **not** flag these — sub-case (a)'s *"In practice an `OutOfMemoryException`."* is a hedged
statement of the typical trigger, not an exclusion, and residual 4's stronger *"Reachable only under
out-of-memory"* was explicitly tolerated in the finding. They were removed anyway because pure
deletion cannot be wrong and leaving one in tension with the repaired cause axis is exactly how the
next round's parallel instance is manufactured:

  - sub-case (a): *"…the rest are faulted with none. **In practice an `OutOfMemoryException`.** This
    is the sub-case that makes firing …"* → *"…the rest are faulted with none. This is the sub-case
    that makes firing …"*
  - residual 4: *"— **again** in practice an `OutOfMemoryException`, constructing the send's
    awaiter…"* → *"— in practice an `OutOfMemoryException`, constructing the send's awaiter…"*
    (the *"again"* back-referenced the clause just deleted).

Residual 4's own *"Reachable only under out-of-memory"* at its code-side note is **kept**: it is a
reachability statement about that one path (justifying the singular-destroy recovery shape, which
must not allocate), not a uniqueness claim about the residual set, and the Critic said so
explicitly.

**5. `CLAUDE.md` (was `:735`).**

  - before: *"…before it, the batch's read never reported at all, and *that* half **is the one not**
    confined to out-of-memory (a stale or mismatched native surfaces an
    `EntryPointNotFoundException`…)"*
  - after: *"…before it, the batch's read never reported at all, and that half **does not need an
    allocation failure to be reachable** (a stale or mismatched native surfaces an
    `EntryPointNotFoundException`…)"*

**6. `ffi §A6` form C (was `:733`).**

  - before: *"…The *before* half (…) **is the half that is not** OOM-only: a stale or mismatched
    native surfaces an `EntryPointNotFoundException` …"*
  - after: *"…The *before* half (…) **does not need an allocation failure to be reachable**: a stale
    or mismatched native surfaces an `EntryPointNotFoundException` …"*

Same paragraph also lost its sibling uniqueness: *"The *after* half is **the sharp one** on a batched
engine … and it is **the only** half the duplicate-risk argument covers"* → *"**After** the read
reported: … and **the duplicate-risk argument applies to that half**."*

### (c) LOW — the brittle count, dropped rather than incremented

**7. `NativeProducer.cs` sync `Send` remarks (was `:544`).**

  - before: *"The **three** throw sources are deliberately *not* equivalent:"*
  - after: *"The throw sources are deliberately *not* equivalent:"*

Row 6's own remedy, which the Critic endorsed: recast, do not increment. Incrementing to "four" is
what created clause (c) in the first place, and the fourth item (the metadata-marshal-failure path
that **fires** and then rethrows the raw non-`KafkaException`) is already documented twice — at
`NativeProducer.cs`'s marshal-failure `catch` and in `IDeliveryCallback`'s "two distinct error
surfaces" paragraph — so no content is lost by dropping the numeral.

### Uniqueness/count removals beyond the three filed clauses (the terminating condition's tail)

Same class, same mechanism, found by running the probes rather than by re-reading the finding. All
deletions:

| Site | before → after |
|---|---|
| `NativeProducer.cs` `SendViaPump` `<param name="delivery">` | *"**Every** throw site here **except one** is a case where nothing was sent (the disposed guard — **twice**: …). **The one exception** is the orphaned-future `catch` below…"* → *"The throw sites that **precede the core's acceptance** — the disposed guard (directly, and again inside `EnsurePump` under its lock), the already-canceled token, a pump that could not be started, and `ProducerSendMarshal.Send`'s synchronous `out_error` — are cases where nothing was sent. The orphaned-future `catch` below runs *after* `Producer_send` accepted the record…"* |
| `NativeProducer.cs` orphaned-future `catch` | *"recorded residual 4 **of the four enumerated** on IDeliveryCallback. It is **the only** no-callback throw on this path where the record WAS accepted: **every other** throw site in this method (…) throws before the core took the record, whereas here…"* → *"recorded residual 4 on IDeliveryCallback. **Here** Producer_send returned a live future AND a null out_error — the ABI's statement that the core accepted the record — so the core may still deliver it."* |
| `NativeProducer.cs` sync-`Send` residual-3 item | *"It is **not** a **fifth** residual site"* → *"It is **not** a **separate** residual site"* |
| `SendCompletionPump.cs` residual-1 / -2 / -3 notes | *"recorded residual N **of the four enumerated** on `IDeliveryCallback`"* → *"recorded residual N on `IDeliveryCallback`"* (×3); and *"**It is the residual that** spans both sides…"* → *"**This residual** spans both sides…"* |
| both pointer sites + the anchor | *"the **three** distinguishing axes"* → *"the distinguishing axes"* — renamed at the anchor (`IDeliveryCallback.cs:197`) and at **both** pointers (`NativeProducer.cs:481`, `SendCompletionPump.cs:524`), so the pointer still resolves verbatim (re-verified after the build, in all three emitted `Confluent.Kafka.xml`) |
| `IDeliveryCallback.OnCompletion` `<summary>` (outside `<remarks>`) | *"a bounded set of **four** residuals (**two** teardown paths, plus **two** unexpected-failure windows: **one** before …, and **one** on the pump …). **All four** sites are on the async surface; the synchronous surface … shares **only the pump-window one** (residual 3)"* → *"a bounded set of residuals (teardown paths, plus unexpected-failure windows before the send reaches the completion pump and on the pump itself), enumerated exhaustively under **Recorded residuals** …, which is also the one place they are compared with each other. The residual sites are on the async surface; the synchronous surface, having no pump, **shares residual 3's** read-then-fire gap for its own single record."* |
| `CLAUDE.md` §4 "Exactly-once, per record" | *"**Recorded residuals — four, not one**… **Two are** teardown paths … **The other two are not** teardown; **both** are … — **one** before …, **one** on it … **All four** *sites* … shares **only** the on-the-pump window's shape … The wording here named only teardown, **then only three paths**, … This bullet states **the count** and the shapes"* → *"**Recorded residuals**… *Teardown* shapes — … *Non-teardown* shapes — unexpected-failure windows belonging to the pull-pump engine, **before** the send reaches the pump and **on** it … The **residual sites** … shares the on-the-pump window's shape … This bullet states **the shapes and points there**"*; and the single-source sentence now reads *"puts the **enumeration, the counts and the comparisons between** residuals in exactly one place"* |
| `ffi §A6` walk | *"it happened **three times** … then at "**three paths**" … the walk yields **three shapes**"* → *"it happened **repeatedly** … then at **a fixed count of paths** … the walk yields **these shapes**"*; *"…stayed off the public list while **three other** sites carried one"* → *"…while **the other** sites carried one"*; *"Neither half is a teardown path, so a residual clause that says "teardown" misses **both**"* → *"…misses **them**"*; *"which was this rule's own **third** miss"* → *"which was **one of** this rule's own misses"*; *"A per-index latch is **the only correct way** to close…"* → *"…is **what would close**…"* |
| `ffi §A7` engine paragraph | *"⚠ **TWO at-most-once residuals** belong to THIS engine … **Both are** artifacts…"* → *"⚠ **At-most-once residuals that** belong to THIS engine … **They are** artifacts…"*; *"the only way to keep the batch's awaiters from hanging"* → *"what keeps the batch's awaiters from hanging"*; *"this **one** residual span the completion's arrival"* → *"this residual span…"*; *"**Both are** non-teardown members … and **both** must be enumerated … alongside **the two** teardown paths. **Neither is repaired by** firing…"* → *"**These are** non-teardown members … and must be enumerated … alongside **the** teardown paths — the enumeration itself, with the counts and the comparisons between residuals, lives in one place (`IDeliveryCallback`'s remarks; §A6's round-5 amendment). **Firing from the fault path is not a repair**…"*; *"**Only** a per-index latch would close…"* → *"A per-index latch **is what would** close…"*. `ffi §A7`'s *"unlike the pre-enqueue window above, this residual is not OOM-only"* is **untouched** per the Manager's explicit instruction |

## The one addition: a normative rule, not a factual claim

`ffi §A6` form C gained a **round-5 amendment** to its single-source rule. It is normative
(a prohibition on a way of writing), so it cannot be falsified by the code — which is why adding it
does not break this cycle's strictly-shrinking property:

> **Round-5 amendment — outside that one place, do NOT re-scope a comparative; DELETE it.**
> Rounds 2–5 each *re-worded* a comparative clause, and each re-wording produced the next round's
> false clause — including one written while fixing exactly this class of defect. … every site other
> than the canonical enumeration states its **own** local fact …, points at the enumeration for how
> it relates to the others, and carries **no** uniqueness quantifier (*the one* / *the only* /
> *every other*), **no** count of residuals, sites or throw sources, and no definite-article
> exclusivity … A claim that is not made cannot go stale, and the property is grep-verifiable:
> outside the canonical enumeration, zero uniqueness or count claims about residuals / no-callback
> throws / throw sources.

## Three things deliberately NOT done

- **No code change, no test change.** The finding is prose-only and says so; the behaviour is
  confirmed correct and the residuals are accepted. Nothing in this cycle touches an executable line.
- **No re-scoping.** The Manager's ruling, and the reason the fix has this shape. Where a clause
  could have been made true by adding "on the async surface" or "of the non-teardown conditions", the
  quantifier was removed instead.
- **Finding 3 (`STATUS.md`) untouched and left OPEN** — Manager-owned, as in cycles 1–4.

## Accuracy notes from the Critic's cycle-4 report, accepted for the record

The Critic's three "not findings" against the cycle-4 close-record are **accepted as correct**, and
this cycle's change acts on two of them:

  - *"Only 2 of the 4 notes carry the pointer + prohibition."* True at the time. Now **4 of 4** do:
    the residual-1 note (`SendCompletionPump.Enqueue`) and the residual-2 note
    (`DrainAndFaultRemaining`) previously had no comparative to remove and no pointer; they now
    reference `IDeliveryCallback` without the *"of the four enumerated"* count, and the residual-3
    and residual-4 notes keep their explicit *do-not-restate-those-axes* prohibition.
  - *"5 of the 8 false rows were in `IDeliveryCallback.cs`, the owning file — not concentrated in the
    code notes."* Accepted; the distribution stated in the cycle-4 record was wrong. The structural
    remedy still holds, and this cycle applies it *inside* the owning file too, by scoping the
    canonical enumeration to the interface's `<remarks>` block and stripping the counts from the
    `OnCompletion` summary that sits outside it.
  - *"Not one false row is in `ffi §A6`/`§A7`; `CLAUDE.md` §4 is likewise clean" is contradicted."*
    Accepted — rows 31 and 33 were false, and both are fixed above. The premise was wrong; the
    decision it supported (keep the rulebooks' prose, do not strip them to pointers) still stands,
    and this cycle keeps that prose while removing its quantifiers.

## Gate results after the fix (full DoD re-run)

| Gate | Result |
|---|---|
| `cargo build --features ffi` (debug) | exit 0 |
| `cargo build --features ffi --release` | exit 0 (**both** profiles built before the Release .NET gate — the stale-native trap) |
| Header hash before/after | `7d8ad0afd4d1af2e108373a22f5ec76af8d970dcbe84191c473de4a830e3ccd7` **unchanged** |
| `dotnet build -c Release --no-incremental` | **0 Warning(s), 0 Error(s)**; 6 outputs (lib ns2.0/net8.0/net10.0 + tests net462/net8.0/net10.0) |
| xmldoc gate **not vacuous** | five distinctive new strings — *"The distinguishing axes, stated here and nowhere else"*, *"so this outcome is neither"*, *"This condition does `<b>`not`</b>` need an allocation failure"*, *"which is also the one place they are compared with each"*, *"The throw sources are deliberately"* — each present **exactly once** in **all three** emitted `Confluent.Kafka.xml`; and six removed strings — *"three distinguishing axes"*, *"of the four enumerated"*, *"the one no-callback throw"*, *"a bounded set of four residuals"*, *"a fifth residual site"*, *"The three throw sources"* — **absent from all three** |
| `dotnet test -c Release -f net10.0 --no-build` | **Failed: 0, Passed: 818, Skipped: 0** (7 s; first run clean, no thread-id flake) |
| `dotnet format --verify-no-changes` | exit 0, clean |
| `cargo xtask format-check` (from the **repo root**) | ✅ all code properly formatted |
| `cargo xtask lint` | ✅ no lint issues |
| `cargo test --lib` | **3693 passed; 0 failed; 3 ignored** |
| Mode A — `git diff -- src/ src/ffi/ cbindgen.toml target/include/confluent_kafka.h tests/` | **empty** |
| `NativeMethods.cs` | **absent from this cycle's diff**; counts unchanged from baseline `3f959b46` — **218** `internal static extern` (the true declaration count, per the Critic's correction), 221 literal `[DllImport` (3 of which are xmldoc prose), 227 lines mentioning `DllImport` |
| `Producer_send_async` / `ProducerSendAsync` | **not declared** — zero hits in `NativeMethods.cs` |
| Phase-2 scope | `bindings/dotnet/grpc-server/**` **untouched** |
| Autosquash | re-verified clean in a throwaway detached worktree: 4 resulting commits, **zero** `fixup!` remaining, squashed-vs-current diff **empty** |
