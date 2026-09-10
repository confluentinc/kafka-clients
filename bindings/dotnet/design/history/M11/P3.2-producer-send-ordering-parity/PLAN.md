# M11 / Phase 3.2 (P3.2) — .NET async producer send: ordering correctness + closing the Python-parity record

> **Status:** PLAN — **all five open decisions resolved by the user on 2026-09-10 (§11).**
> No Actor / Critic spawned; the user gates that loop separately.
> ⚠ **D2 overrode the plan's recommendation:** per-node completion grouping is **implemented in this
> phase** (§3B), not deferred to a follow-up. That is the single largest change from the first draft
> and it reshapes the pump, the slice list, F6's disposition and the deviation list.
> **Phase ID:** M11/P3.2 · **Mode:** A (.NET-only; no Rust core / ABI / header change — §7)
> **Branch:** `prashah_dev_producer_python_alignment` (PR #188, base `prashah_dev_dotnet_performance_new`)
> **Follows:** `bindings/dotnet/design/history/M11/P3.1-producer-python-alignment/PLAN.md` (shipped S0–S8b)
> **Bar set by the user (2026-09-10):** *"We should be fully aligned with python producer send except the GIL part."*
> **Verified state of the tree at planning time:** `cargo build --features ffi` clean;
> `cd bindings/dotnet && dotnet test -f net10.0` → **880 passed / 0 failed**. Every finding below
> coexists with a fully green suite — which is the point: none of them is caught by anything we run.

---

## 0 · What this phase does, in one paragraph

P3.1 moved the .NET async producer's send **submission** onto the Python binding's shape: `Send`
pins the buffers, appends to a binding-side `SendAccumulator`, and a batch thread issues
`kafka_producer_Producer_send_batch` per node. This phase fixes the **one place that shape was not
copied faithfully and where the difference is a real bug** — .NET waits for accumulator space
*before* appending, Python appends *first* and only then waits, and the .NET order can therefore let
a later send overtake an earlier one (F1) — then closes out the parity record around it: the
close-behaviour and bound-strictness consequences of that same difference (F2), the
completion-*grouping* claim that the 1100 cap does not actually establish (F3), the teardown fault
window the accumulator newly exposes on every close (F4), one latent gate-cancellation gap (F5), and
two deliberate-but-unrecorded deviations (F6, F8). One item (F7) is a confirmed deliberate deviation
with **no code change** — surfaced so the user can re-affirm it against the "fully aligned" bar.

**Post-D2 addendum.** The user chose to implement completion grouping rather than record it, so this
phase now also makes the **completion** side structurally Python's — one `get_all` per `send_batch`
group, as the anchor's poll thread does one `BatchNode` per pass — instead of a flat queue bounded by
a number. That is a bigger change than the rest of the phase combined, and §3B is its design.

---

## 1 · Parity anchor (pinned first, cites re-verified for this plan)

> **Why this section leads.** The standing feedback on this binding is that a green build, green
> tests and a clean Critic already missed invented surface once (M11/P2 → P2.1). P3.1 adopted the
> rule "pin the anchor up front and review against it". This phase exists because P3.1 pinned the
> anchor's *constants* and *thread topology* but not its *append-before-wait ordering*, so the
> anchor was satisfied on every axis anyone checked.

### 1.1 The anchor

**`bindings/python/_confluentkafka.c` producer send path** and
**`bindings/python/producer.py`**, plus the analysis in
`design/current/python-binding-send-batching.md`.

**Every line number below was opened and read while writing this plan** (not recalled, and not
inherited from P3.1 or from the incoming audit):

| Anchor site | What it establishes | Used by |
|---|---|---|
| `_confluentkafka.c:19-27` | `SLOT_THRESHOLD 1000`, `SLOT_CAPACITY (THRESHOLD+100)`, `MAX_ACCUMULATED_RECORDS = SLOT_THRESHOLD`; the `:22-26` comment is the Java-faithfulness argument for a stage-1 bound | F2 |
| `:361-369` | `BatchNode` — five parallel arrays, each `SLOT_CAPACITY` wide | F3 |
| `:429-430` | `Producer_complete_callbacks` sizes its `get_all` output arrays at `SLOT_CAPACITY` (**stack** arrays) | F3 |
| `:480-514` | `Producer_poll_futures_thread`: loop condition `:484`, completes **exactly one** `BatchNode` per `get_all` (`:487-495`), advances `:501`, `cnd_wait`s only while the next node is `NULL` (`:504-507`) | F3, F4 |
| `:523-655` | `Producer_send_thread`: free-running window `:534-535`, take-the-chain `:567-577`, reset the counter `:573`, fire space callbacks `:579`, one `send_batch` per node `:583-598`, immediate-error compaction `:600-625` | F3, F6 |
| **`:804-831`** | **`py_Producer_send`'s critical section: the append at `:819-822` is UNCONDITIONAL, the counter at `:823`, the early-wake signal at `:825-827`, and the `full` flag is computed at `:830` — AFTER the append** | **F1, F2** |
| `:629-638` | the drained chain is appended to the pending list **once** and signalled **once** (`:637`) | F6 |
| `:640-652` | on exit: `send_completed = 1`, signal, `Producer_flush`, then join the poll thread | F4 |
| `:857-861` | `py_Producer_on_space_available` returns "available" when `closed` **or** under the bound — so a waiter never parks through a close | F2 |
| `:962-972` | `py_Producer_shutdown`: `closed = 1` (`:962`) under the mutex, take the space waiters (`:966`), signal the send thread (`:967`), join (`:969`), fire the waiters (`:972`) | F2 |
| `producer.py:373-385` | sync `send`: `Producer_send` at `:373`, then `if full:` register + **block** on `space.result()` (`:375-385`) — *after* the record is accumulated | F1, F7 |
| `producer.py:685-701` | async `send`: `Producer_send` at `:685`, then `if full:` `await space` (`:687-700`), and only then `return ret` (`:701`) | F1, F7 |
| `producer.py:630-637` | `_drain` — the loop-thread completion drain | F6 |

### 1.2 Cite corrections carried into this plan

Three cites in the incoming audit are off by a few lines. Recording them so the Actor and Critic use
the corrected ones and nobody "fails to find" the quoted code:

1. The incoming audit quotes `_confluentkafka.c:805` as "ALWAYS appends". `:805` is the
   `mtx_lock`; the **append is `:819-822`** and the counter increment `:823`. The claim is correct;
   the line is not.
2. "`py_Producer_shutdown` (`:967`) fires the space waiters" — `:967` is `cnd_signal` to the **send
   thread**. The waiters are *taken* at `:966` and *fired* at `:972`, after the join.
3. `on_space_available`'s early-out is `:857-861` (condition `:857-858`, `available = 1` at `:860`),
   not `:855-859`.

### 1.3 One anchor property this plan does NOT over-claim

Python's "a record already accumulated is still sent on close" is **almost** unconditional, not
unconditional. `Producer_send_thread`'s outer loop tests `!producer->closed` at `:529`
**outside** `record_batches_mutex`.

**Why the common path is safe — a TIMING argument, not a structural one.** The thread spends
almost all of its time parked in `cnd_timedwait` at `:548`, which **atomically releases**
`record_batches_mutex` for the duration of the wait and reacquires it on wake — that release is
precisely how `py_Producer_shutdown` acquires the lock at `:961` to set `closed` at `:962`. When
shutdown wins the lock *there*, the thread wakes holding the mutex again, falls out of the inner
wait (`:539`'s `&& !producer->closed`), and performs one final take-and-send before the outer test
ends it. That is the overwhelmingly likely interleaving simply because the park is where the thread
almost always is.

**But the unlocked window is the whole send loop, and it is not narrow.** The thread releases
`record_batches_mutex` at `:556`, `:563` and `:577`, and runs the entire send loop `:581-638` —
every `send_batch` call plus its GIL acquisition — holding **none** of it. So shutdown can set
`closed` at any point in there, and the outer `:529` test then ends the thread. Records appended
*during* that window went into a fresh chain that is **never taken, never sent, and never
completed**. For a full `SLOT_CAPACITY` batch that window is as long as a `send_batch` call takes.

So where this plan says ".NET should complete, as Python does", the accurate statement is
**".NET should complete, as Python does whenever close lands while its send thread is parked — and
unlike Python it also completes when close lands mid-send"** — i.e. the proposed .NET behaviour is
Python-aligned *and* strictly better. Say it that way at the site; do not claim Python is airtight
here.

⚠ **This paragraph has been corrected TWICE (2026-09-10) and both errors reached code.** First it
said the thread parks *holding* the mutex, which inverts `cnd_timedwait` and made its own conclusion
unreachable (Critic 71 finding **71.10**). The correction then claimed the thread "holds the mutex
whenever it is not parked", so "the park is the only window shutdown can win the lock" — also false,
per the three unlock sites and the unlocked send loop above (finding **71.11**). The surviving
argument is **timing** ("the thread spends its time parked"), never structure. It also mispaired the
unlock cites: `:638` is a `pending_batches_mutex` unlock, not a `record_batches_mutex` one. A Critic
re-deriving this from the C source rather than from this paragraph is what caught both.

---

## 2 · Findings

Severity-ordered. Each opens with the problem in plain language, then the mechanism, then the fix,
then the alignment statement the user asked for.

---

### F1 — HIGH · records can reach Kafka in a different order than they were sent

**In plain language.** When the binding's internal queue is full, a send waits for room *before* it
puts its record in the queue. A later send that finds room free can slip into the queue ahead of the
one still waiting, so records can reach Kafka in a different order than the application sent them.
Python never has this problem: it always puts the record in the queue first, and only then waits.
This is the one finding here that should block a merge.

**(a) What Python does.** `py_Producer_send` appends under the mutex **unconditionally** — the slot
writes at `_confluentkafka.c:819-822`, the counter at `:823` — and only *then* computes
`full = producer->accumulated_records >= PRODUCER_MAX_ACCUMULATED_RECORDS` at `:830` and returns it
(`:833`). The waiting happens in Python, *after* the record is already in the chain:
`producer.py:375-385` (sync, blocking `space.result()`) and `:687-700` (async, `await space`). So the
append order is exactly the call order, always, by construction.

**(b) What .NET does today.** `NativeProducer.SendViaPump` takes the permit **first** and defers the
append to a thread-pool continuation when it cannot get one
(`src/Confluent.Kafka/Internal/NativeProducer.cs:520-541`):

```csharp
if (accumulator.TryAcquireSpace())
{
    try { accumulator.Submit(record, completion, delivery); }   // fast path: appends NOW
    catch (Exception) { cancellationRegistration.Dispose(); throw; }
    return completion.Task;
}

return SendWhenSpaceAvailable(accumulator, record, delivery, completion, cancellationToken);  // appends LATER
```

`SendWhenSpaceAvailable` (`:554-579`) awaits `WaitForSpaceAsync` and only then calls `Submit`.

The failing interleaving needs **one** application thread:

1. `Send(A)` — the bound is full, `TryAcquireSpace()` is `false` → slow path → an incomplete `Task`
   is returned to the caller and **A is not appended**.
2. The batch thread drains and calls `ReleaseSpace(freed)` → `SemaphoreSlim.Release(n)`
   (`SendAccumulator.cs:749-755`). `Release` hands the first permit to A's queued async waiter and
   leaves the remainder available.
3. The same application thread calls `Send(B)`. `TryAcquireSpace()` → `_space.Wait(0)` succeeds on a
   remaining permit → **B is appended, inline, now**.
4. A's continuation is scheduled on the thread pool and appends **after** B.

`send_batch` then receives `[…, B, A]`. Everything downstream preserves that order —
`SendChain` walks nodes in order (`SendAccumulator.cs:766-782`), `Append` writes slots under `_gate`
in acquisition order (`:290-352`), and `send_batch_inner` loops `for i in 0..count`
(`src/ffi/producer.rs:1504`) calling `producer_send` per record — so the binding's append order *is*
the wire order. The reorder is not smoothed out anywhere.

**Why it matters.** Java documents ordering as preserved in the default configuration:
`kafka/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java:274` —
*"if retries are disabled or if `enable.idempotence` is set to true, ordering will be preserved."*
Because the reorder happens **in the binding, before the core sees the records**, no core-side or
broker-side setting can restore it: idempotence protects against retry-induced reordering, not
against being handed the records in the wrong order. The previous design (P3.1's predecessor, the
inline `Producer_send`) could not exhibit this, and Python cannot exhibit it. It appears only once the
binding-side bound saturates (>1000 buffered) — the sustained-throughput case this whole design
targets — and it fails **silently**.

**It was seen and mis-attributed.** `tests/Confluent.Kafka.UnitTests/Interop/SendAccumulatorTests.cs:715-717`:

> *"The released sender resumes on the thread pool, so its append is not ordered against the drain
> that freed it — poll rather than assume."*

The test polls around the symptom. **No test in the suite asserts append order at all.**

**(c) The fix.** Full design in §3. Summary: move the routing decision into `SendAccumulator` behind
one primitive that both production and the test fixture call, make the fast path conditional on
*nothing being queued ahead of it*, and give the slow path an explicit FIFO queue with a single
appender. Detail, options considered and the trade-offs are in §3 because this is the one finding
whose fix shape was a real design decision (**D1**, resolved in §11).

**(d) How this aligns us with Python.** The **observable property** is Python's, exactly: the order
in which records are handed to `send_batch` equals the order in which `Send` was called by a caller.
The **mechanism** cannot be Python's, and that difference is recorded as a deviation rather than
papered over: Python's `send` is a coroutine (`producer.py:650`), so `await producer.send(rec)` is a
mandatory suspension point that can carry the throttle *after* the append. .NET's
`IAsyncProducer.Send` returns `Task<RecordMetadata>` — Java's shape, and the returned task is the
*record's delivery* future, not a submission handle — so there is no post-append suspension point to
hang the throttle on, and a `tasks.Add(Send(rec))` loop never awaits it. That API-shape difference is
precisely why P3.1 deferred the append; the fix keeps the property and pays for it with a binding-side
FIFO instead of a suspension point. Recorded in §3.4 as deviation **DV-1**.

---

### F2 — MEDIUM · the same difference has two more consequences, both unrecorded

**In plain language.** The same pre-append-versus-post-append difference behind F1 also changes two
other things: what happens to a record whose send is waiting for room when the producer is closed
(Python still sends it, .NET throws it away), and how strictly the 1000-record limit is enforced
(Python's is approximate, .NET's is exact). Neither is written down anywhere as a deliberate choice.

**(a) What Python does.**

- *Close.* The record is **already accumulated** before any waiting happens, so on close it is sent:
  `py_Producer_shutdown` sets `closed = 1` under the mutex (`:962`), takes the pending space waiters
  (`:966`), signals the send thread (`:967`) and fires the waiters after the join (`:972`); and
  `py_Producer_on_space_available` returns "available" the moment `closed` is set (`:857-861`), so a
  waiter never parks through a close. The send thread's final iteration drains and sends the record
  (§1.3 records the one narrow race in this).
- *Bound strictness.* Every sender appends before checking, so with C concurrent senders the
  accumulation can reach `bound + C − 1` before anybody waits. The bound is **soft** by construction.

**(b) What .NET does today.**

- *Close.* A parked `Send` is faulted: `Stop` cancels `_spaceGate` (`SendAccumulator.cs:532`),
  `WaitForSpaceAsync` maps that to `ClosedDuringBackpressure()` — an `ObjectDisposedException`
  (`:757-760`) — and `SendWhenSpaceAvailable` routes it to `completion.TrySetException`
  (`NativeProducer.cs:571-576`). **The record never reaches the core at all.** No delivery callback
  fires either (D5's "nothing reached the core" rule), which is correct *given* the record was
  dropped — but the drop itself is the divergence.
- *Bound strictness.* `SemaphoreSlim(bound, bound)` makes the bound **exactly** 1000. Hard.

**(c) The fix.**

- *Close (behaviour change, own slice S2).* At teardown, drain the submission queue **into the node
  chain** — bypassing the bound, exactly as Python's already-accumulated record bypasses it — and only
  then set `_closed` and let the batch thread do its final drain. Ordering inside `Stop` becomes:
  cancel new submissions → flush the submission queue into the chain → `_closed = true` + pulse →
  bounded join. This slots into P3.1 §3.8's teardown handshake between its steps 2 and 3, and is what
  turns "faulted" into "sent and completed". A queued submission whose caller token already fired
  stays cancelled and is **not** appended (today's semantics for a cancelled parked send; unchanged).
- *Bound strictness (no code change).* Record it as an explicit deviation: .NET's bound is hard, and a
  hard bound is strictly more conservative than the anchor's soft one. Do **not** loosen it to imitate
  `bound + C − 1`; overshooting a memory bound to match an artefact of the anchor's check-after-append
  buys nothing and costs predictability.

**(d) How this aligns us with Python.** The close fix makes the observable outcome identical to
Python's — a send that returned before `Close` was called reaches the broker rather than being
faulted — and, per §1.3, closes a window Python leaves open. The bound-strictness item is an
acknowledged deviation with the rationale stated at the site and in the deviation list (**DV-2**),
per P3.1 §11's standing principle that no divergence from the anchor goes unrecorded.

---

### F3 — MEDIUM · we match Python's completion-batch *size* but not its *grouping*, and the plan claims both

**In plain language.** We now finish at most 1100 records at a time, which is Python's number. But
Python always finishes exactly one batch's worth — the records from a single send — whereas .NET can
lump together records from many different sends. Because the underlying call waits for *every* record
in the group to finish, one record can be held up by the slowest of up to 1100 unrelated ones. The
number matches; the shape does not, and the plan says both match.

**(a) What Python does.** `Producer_poll_futures_thread` holds **one** `BatchNode` at a time
(`_confluentkafka.c:483`), calls `Producer_complete_callbacks(..., current_pending_batch->count)` for
just that node (`:487-495`), then advances to `->next_batch` (`:501`). Its `get_all` output arrays are
**stack** arrays sized `SLOT_CAPACITY` (`:429-430`). So a completion batch is (i) at most 1100 and
(ii) **never mixes records from different drains** — one node is one drain's worth from one
`send_batch`.

**(b) What .NET does today.** `SendCompletionPump.DrainAll(DrainCap)` pulls up to 1100 items off a
flat `ConcurrentQueue<PendingSend>` (`SendCompletionPump.cs:396-412`). Futures enter that queue **one
at a time** per record (`SendAccumulator.CompleteNode` → `_pump.Enqueue(...)`, `:968`), so one drain
can span an arbitrary number of `send_batch` calls, and one `get_all` therefore blocks on the union.
`get_all` returns only when every future in the array resolves, so the first record's completion is
gated on the slowest of up to 1100 — a head-of-line delay the anchor bounds per node.

The over-broad claim lives in two places and both must be narrowed:
`P3.1 PLAN.md §12.2` and the §12 audit row *"completion-batch bound … ✅ identical"*, and the code
comment at `SendCompletionPump.cs:383-388` (*"The cap is Python parity … the one place the two
bindings' constants diverged"*). The **constant** is now identical; the **grouping** is not, and
nothing in either text says so.

**(c) The fix — D2 = implement grouping (user, 2026-09-10; overrode the plan's doc-only
recommendation).** The completion side becomes **one `get_all` per `send_batch` group**: the batch
thread hands the pump one *batch object* per `send_batch` call instead of one item per record, and
the pump processes exactly one such group per pass. The full design — how node identity is carried,
what happens to `DrainCap` and P3.1's S8a/S8b work, the array double-free discipline,
`DrainAndFaultRemaining`, and the one hazard the change introduces — is **§3B**. The claim narrowing
still happens (S0), but it now records a difference the phase **removes** rather than one it keeps.

**(d) How this aligns us with Python.** Fully, on both axes, and *structurally* rather than
numerically: a completion batch becomes exactly one `send_batch`'s worth of records, which is the
anchor's `BatchNode` unit (`:487-495` one `get_all` per node, `:593` one `send_batch` per node), and
the ≤1100 bound follows from the unit rather than from a hand-picked cap. That is the honest argument
for D2 being worth its risk: the numeric cap made the *number* match while leaving the *shape*
different, and shape is what the head-of-line behaviour depends on. **DV-3 is deleted from the
deviation list** (§10) — a phase must not record a deviation it removes.

---

### F4 — MEDIUM · at shutdown we can fail sends that Python completes, and the accumulator makes it routine

**In plain language.** When the producer shuts down, if the completion thread has not yet picked up
the last few records, .NET marks them as failed. Python instead finishes them properly before
exiting. The window was always there, but the change we just shipped now pushes the whole buffered
backlog into that thread at exactly the moment shutdown begins, so a rare case becomes a common one.

**(a) What Python does.** The poll thread's loop condition is
`while (!producer->send_completed || current_pending_batch != NULL)` (`:484`), and its inner
`cnd_wait` is guarded by `!send_completed` (`:504-507`) — so once the send thread sets
`send_completed = 1` (`:641`) the poll thread stops waiting but **keeps draining the pending chain to
empty**, completing every record. The send thread also calls `Producer_flush` (`:651`) before joining
it, precisely so nothing is unresolvable. There is **no fault-the-remainder path in Python at all.**

**(b) What .NET does today.** `SendCompletionPump.RunLoop` checks `_stopping` **before** draining
(`:310-314`) and breaks, leaving the queue to `Stop`'s terminal `DrainAndFaultRemaining` (`:287`,
`:606-608`) which **faults** every remaining awaiter. So a record the core already accepted — and
which the teardown flush already resolved — can be reported as failed because the pump thread was not
scheduled in time.

**What is new.** `StopPump` runs `StopAccumulator()` **first** (`NativeProducer.cs:1180`), which
drains the entire buffered chain into the pump, and only then `CloseGate()` (`:1188`) → flush
(`:1202`) → `Stop()`. That ordering is deliberate and correct (`:975-1004` explains why it must
precede `CloseGate`), but its effect is that **every close-with-buffered-records now dumps a burst
into the pump queue immediately before `_stopping` is set**, where previously the queue was usually
near-empty at that moment because futures trickled in as each inline send returned. The window did
not widen; the probability of being inside it went from negligible to routine. `STATUS.md:20` already
names this mechanism *"the out-of-scope pump race"*.

**(c) The fix.** Two options; **the bounded pre-stop drain is the recommendation** (decision **D3**,
§11):
- *Recommended:* after the teardown flush and **before** `_stopping` is set, wait — **bounded** — for
  the pump's queue to reach empty while the loop is still running. This is safe and cannot hang:
  `CloseGate()` has already run, so no new item can enter the queue, and the wait is therefore
  monotone; on expiry we fall back to exactly today's fault-the-remainder behaviour. It must **not**
  be implemented by draining from the teardown thread (that would enter the uninterruptible `get_all`
  on a thread that must stay responsive).
- *Rejected:* moving the `_stopping` check after the drain. That reintroduces the N=51 blocker
  directly — an unresolvable record (the `AsyncMockProducer.Clear()` premise break recorded in
  `dotnet-critic` memory and P3.1 §6.3) would park the loop in `get_all` forever and hang the join.
  `get_all` cannot be bounded, so "drain first" cannot be made safe. Say so explicitly at the site so
  the next reader does not try it.
- *Resolved (D3, 2026-09-10): the fix is taken.* It lands as **S4**, after the grouping slice —
  its wait predicate is "the pump's queue is empty" and that queue's element type changes in S3
  (§3B.2), so landing it first would mean writing it twice. The residual note is still updated, to
  the **narrowed** residual the fix leaves behind (DV-4).

**(d) How this aligns us with Python.** The recommended fix makes the normal case match Python
exactly: at shutdown every record the core accepted is completed, not faulted. It stops short of
Python's *unconditional* guarantee, because Python buys that with an unbounded join it can afford (its
own send thread flushes first and its mock has no equivalent of `Clear()`), whereas .NET has a
recorded case where the premise "the flush resolves everything" is false. The residual difference is
therefore bounded, stated, and reduced to the pathological case — recorded as **DV-4**.

---

### F5 — LOW · a dead batch thread does not wake senders waiting for room

**In plain language.** If the background batch thread dies unexpectedly, we stop accepting new
records but we never explicitly wake anyone already waiting for room. It happens to work today as a
side effect of the permits being handed back, but nothing states that, so a later change could quietly
turn it into a hang.

**(a) What Python does.** Not applicable in the same shape — Python's send thread has no managed-style
unhandled-exception path, and its space waiters are woken on close (`:966`/`:972`) and by the drain
(`:579`). The relevant anchor property is *"a space waiter is always released by some event"*, which
Python satisfies on both of its paths.

**(b) What .NET does today.** `Stop` cancels `_spaceGate` before closing (`SendAccumulator.cs:532`),
so the teardown path is explicit. The thread-death path — `RunLoop`'s `catch` →
`AbandonOnThreadFailure` (`:576-607`) — sets `_closed`, takes the chain, settles both chains and calls
`ReleaseSpace(freed)`, but **never cancels `_spaceGate`**. It is safe today only through an unstated
invariant: a waiter can only exist when the permits are exhausted, i.e. `_accumulated == bound`, so
`ReleaseSpace(freed)` with `freed == _accumulated > 0` necessarily releases at least one permit and
wakes it. That invariant depends on permit accounting that a refused `Submit` returns (`:263-271`) —
and note that the very failure mode this handler exists for is an over-release
(`SemaphoreFullException`, the fixture's injection at `SendAccumulatorTests.cs:980-985`), i.e. a case
where the accounting is *already* known-broken.

**(c) The fix.** One line — `_spaceGate.Cancel()` in `AbandonOnThreadFailure`, before or after the
lock, with a comment saying why it is unconditional rather than relying on the permit arithmetic.
Waiters then fault with `ClosedDuringBackpressure()`, which is the right answer: the accumulator is
closed and their record will never be sent. If the user prefers no behaviour change here, the fallback
is to state the invariant at the site — but the one-line fix is cheaper than the argument.

**(d) How this aligns us with Python.** It restores the anchor's property (a space waiter is always
released by an explicit event, never by arithmetic coincidence) on the one .NET-only path Python has
no counterpart for. Recorded as an alignment of the *property*, with the path itself noted as
.NET-specific.

---

### F6 — LOW · futures reach the completion thread one at a time, not one chain at a time

**In plain language.** Python hands its completion thread a whole batch of finished sends in one go;
.NET hands them over one at a time. Ours is marginally quicker to start completing, but it is not
written down as a deliberate difference — and it is the direct reason F3 exists.

**(a) What Python does.** The send thread appends the **whole drained chain** to the pending list
under one lock and signals **once**: `_confluentkafka.c:629-638` (splice at `:630-636`, `cnd_signal`
at `:637`).

**(b) What .NET does today.** `SendAccumulator.CompleteNode` calls `_pump.Enqueue(future, completion,
delivery)` per record (`:968`), and `Enqueue` sets the pump's event per item
(`SendCompletionPump.cs:225-226`). Consequence: a record can start completing before its node
finishes marshalling, which is a small latency win — and the flat per-record queue is exactly what
makes a completion batch span multiple drains (F3).

**(c) The fix — F6 is now FIXED, not recorded (D2's consequence, decided explicitly).** The first
draft proposed recording per-record enqueue as a deliberate deviation. D2 removes it: under grouping
the pump is handed **one batch per `send_batch` call**, so the per-record `Enqueue` disappears
outright. **Do not ship a §3 entry recording a deviation this phase deletes.**

What survives is **much narrower**, and it is a real residual difference that must still be recorded:
Python's send thread walks *every* node of the drained chain, then splices the whole chain onto the
pending list and signals **once** (`:629-638`); .NET's `SendChain` will hand over each group as soon
as that group's `send_batch` returns. Both then complete **one group per pass**, so the *grouping* is
identical — only the hand-off *timing* differs, and .NET's is earlier (the pump can start completing
group 1 while the batch thread is still in group 2's `send_batch`). So **DV-5 is narrowed, not
deleted**: it keeps the early-hand-off benefit and loses the "and it is the mechanism behind DV-3"
clause, because that consequence is gone.

**(d) How this aligns us with Python.** The observable grouping becomes identical; the residual is a
hand-off-timing difference that is strictly in .NET's favour and cannot change which records share a
`get_all`. Recorded as the narrowed **DV-5**.

---

### F7 — LOW · the sync path stays inline: confirmed deliberate, no code change proposed

**In plain language.** In Python, both the normal and the async producer put records through the same
batching queue. In .NET only the async producer does. That was a deliberate decision, because .NET's
normal `Send` waits for the result and returns it, so putting it behind a 0–10 ms batching window
would slow down every single send.

**(a) What Python does.** Both surfaces call the same C entry point: `producer.py:373` (sync
`Producer.send`) and `:685` (async `AsyncProducer.send`) both call `_lib.Producer_send`, which is the
accumulating `py_Producer_send`. There is no sync/async split at Python's C layer.

**(b) What .NET does today.** The sync `Send` (`NativeProducer.cs:581`+) calls the singular
`Producer_send` inline with call-scoped `fixed` pins and blocks on `FutureRecordMetadata_get`; only
`SendViaPump` goes through the accumulator. Recorded as P3.1 §3.1, user-directed.

**(c) The fix.** **None proposed.** The justification holds and is quantitative: Python's sync `send`
returns a `Future` (it does not block for the result), whereas .NET's sync `Send` returns a
materialized `RecordMetadata`, so the accumulator window would land on the critical path of **every**
sync send against a measured p50 ≈ 7 ms — a potential doubling. Note the sync path is also the one
surface with *no* buffer-mutation window (P3.1 §4.7) and no ordering exposure (F1 cannot occur on it,
since its append is inline and inside the call).

**(d) How this aligns us with Python.** It does not, deliberately. This is the phase's largest
structural divergence from the anchor and it is surfaced here **only** so the user can re-affirm it
against the "fully aligned except GIL" bar rather than have it assumed by inheritance (**D4**, §11 —
a confirm/re-open decision with no work attached to "confirm").

---

### F8 — LOW · a fifth stale record still describes .NET as Option C (my addition, not in the incoming audit)

**In plain language.** One of the design documents still says the .NET producer works the old way and
lists the new way as an unexplored idea with a blocker. That document was not in the list of records
the last phase updated, so it was left behind.

**(b) What .NET does today.** `design/current/python-binding-send-batching.md:408-459` —
*"The .NET producer is **Option C** … What it lacks is the **middle** (send/accumulator) thread"*,
followed by a "status: **blocker**" row for `Py_INCREF` and two "arguments against". All of it is now
shipped and answered (interned topic pins + `MemoryHandle`, P3.1 §4.1/§4.2). Verified: the file
contains **zero** occurrences of "P3.1". P3.1 §2.1 enumerated four records to supersede
(`NativeMethods.cs:2237-2243`, `STATUS.md:99`, `P3-producer-send/PLAN.md` §3.3, the HTML approaches
doc) — this file was not among them, and it is the document P3.1 §1.1 tells every future reader to
*cite rather than re-derive*, which makes its staleness self-propagating.

**(c) The fix.** Rewrite §408-459 the way S0 rewrote the other four: state the shipped split (sync =
inline Option C, async = accumulator, completion = pull pump), mark the `Py_INCREF` row **resolved**
with how, and keep the two arguments-against as *accepted costs* with pointers to P3.1 §3.3/§8.2
rather than as open objections. Do not delete the analysis — it is the reference the anchor section
points at.

⚠ **Rewrite it carefully, not mechanically — D2 changes one row's truth value in the opposite
direction.** The table's `Producer_poll_futures_thread` → `SendCompletionPump` row is marked
**"exists"**, which was true only of the *thread*; the grouping was never equivalent. After §3B it
becomes true of the grouping too, so that row goes from *accidentally over-stating* parity to
*actually correct* — while every other row goes from "new" to "shipped". A sweep that flips every row
the same way gets this one wrong. Do the rows individually.

**(d) How this aligns us with Python.** Indirectly but materially: this file is the parity anchor's
own analysis document, and a reader who trusts it will mis-state what .NET does. Fixing it is what
keeps future parity reviews anchored to reality.

---

## 3 · The F1 fix — design, options, and the decision behind it (D1)

### 3.1 The constraint that makes this non-obvious

Python throttles **after** the append because it has a suspension point there: `send` is a coroutine
(`producer.py:650`), so `await producer.send(rec)` necessarily gives the throttle somewhere to live,
and the sync twin simply blocks the caller (`:384`).

.NET's `IAsyncProducer.Send` returns `Task<RecordMetadata>` — Java's shape — and that task is the
**record's delivery future**. There is no second awaitable, and the idiomatic usage
(`tasks.Add(producer.Send(rec))` in a loop) never awaits at submission time at all. So "append, then
await the throttle" has nowhere to attach without either blocking the caller thread inside `Send` or
changing the public signature. That is why P3.1 deferred the append, and it is the real constraint
any fix must respect.

### 3.2 Options evaluated

| # | Option | Ordering | Throttle preserved | Public API | Verdict |
|---|---|---|---|---|---|
| **(a)** | **Route through a FIFO submission queue whenever anything is queued ahead** (recommended) | ✅ per caller, and in fact total | ✅ unchanged | ✅ unchanged | **Recommend** |
| (b) | Post-append throttle, blocking the caller inside `Send` when full | ✅ | ✅ but blocking | ✅ signature; ❌ contract (`Send` becomes blocking) | Reject |
| (b′) | Post-append throttle exposed as a second awaitable (e.g. `ValueTask<Task<RecordMetadata>>`) | ✅ | ✅ | ❌ breaks Java's shape | Reject |
| (c) | Drop the stage-1 bound; rely on the core's `buffer.memory` | ✅ (append always inline) | ❌ removed | ✅ | Reject |

- **(b)** is Python's *sync* shape exactly, and it is tempting because P3.1 §2.2's whole case for this
  design was converting a native block into a **managed, cancellable** wait — a managed block would
  keep that. It still fails: blocking inside a method whose entire contract is "returns immediately
  with a Task" would park a thread-pool thread for every saturated send in an `async` caller, which is
  the sync-over-async hazard `ffi-marshalling.md` §A1 rules out on the managed side, and it would make
  the throughput profile of the shipped design meaningless. Reject, and record the reasoning so it is
  not re-proposed as "just do what Python does".
- **(b′)** is honest about the shape mismatch but breaks the Java-faithful surface
  (`bindings/dotnet/CLAUDE.md`'s "restore the Java shape"), so it is out.
- **(c)** removes the bound whose Java-faithfulness argument the anchor states in prose at
  `_confluentkafka.c:22-26` (*"mirrors Java's `send()` blocking once `buffer.memory` is full, applied
  here at batch granularity in front of the Rust accumulator"*). Removing it is a bigger divergence
  than the one being fixed.

### 3.3 The recommended mechanism (a), and why the obvious cheap version is not enough

**The naive version — a "someone is parked" counter alone — is insufficient**, and the plan says so
up front because the incoming audit's starting proposal is exactly that. A counter fixes the
fast-path-overtakes-parked case, but **two consecutively parked sends from the same caller can still
invert**: the batch thread's `ReleaseSpace(freed)` releases many permits at once, so both waiters'
continuations become runnable together and race for `_gate` in `Append`. And it cannot be repaired by
leaning on `SemaphoreSlim` fairness — the .NET documentation states explicitly that there is **no
guaranteed order in which blocked threads enter the semaphore**, so ordering must not rest on it.

So the mechanism has two parts:

1. **Routing.** A count of submissions currently queued-or-appending, on the accumulator.
   `Send` takes the inline path only when that count is zero **and** a permit is available; otherwise
   it joins the queue. The count is incremented **synchronously, before `Send` returns**, which is
   what makes it correct for a single caller: a caller's next `Send` cannot run until the previous one
   returned, so it always observes the increment.
2. **Ordering among queued submissions.** An explicit FIFO — a `ConcurrentQueue` (documented FIFO)
   plus a **single** submitter that dequeues, awaits a permit, and appends, one at a time. One
   appender means no race; a documented-FIFO queue means no dependence on semaphore fairness.

Shape (illustrative, not prescriptive about names):

```
SendAccumulator.TrySubmitInline(record, completion, delivery) -> bool
    // false when _queued != 0 or no permit is available; true when it appended.
SendAccumulator.SubmitQueued(record, completion, delivery)
    // increments _queued synchronously, enqueues, ensures the single submitter loop is running.
    // The submitter: dequeue -> await WaitForSpaceAsync -> Submit -> decrement _queued.
```

`NativeProducer.SendViaPump` then reads: try inline, else queue, and **return `completion.Task` in
both cases** — the returned awaitable stays identical on both paths, which it is today only by
`SendWhenSpaceAvailable` re-awaiting `completion.Task` (`:578`).

**Pins are still taken after the permit.** A queued submission holds the unpinned
`SerializedProducerRecord` only; pinning stays inside `Submit`, so P3.1 §4.4's invariant — *"the pin
must not be taken before the backpressure permit, or a blocked sender holds pins while waiting"* —
is preserved verbatim. This is worth stating in the commit: a reviewer scanning for pin lifetime will
look here first.

**Both entry points must live on `SendAccumulator`, not in `NativeProducer`** — DoD §12
(test-fixture fidelity). Today the test harness re-implements the routing:
`SendAccumulatorTests.cs:951-963` (`AppendOne`) and `:1074-1088` (`TryAppendOne`) duplicate
"permit-then-`Submit`, else `SubmitWhenSpaceAvailable`". If the routing rule lands only in
`NativeProducer`, the fixture keeps the old rule and **the ordering test would be a proof about the
fixture**. Putting the decision in the accumulator and having both callers go through it is the fix
for that, and it is the same "call the same builder production calls" pattern DoD §12 mandates.

### 3.4 Consequences that must be handled in the same slice

1. **"Empty and idle" now has two stages.** `DrainPending` / `DrainPendingAsync` /
   `SignalIdleLocked` (`SendAccumulator.cs:368-484`) currently test `_head is null && !_draining`.
   With a submission queue upstream of the chain, a record that a caller's `Send` has **returned** can
   sit in the queue while the chain is empty — so `Flush` would return without it, re-opening exactly
   the gap P3.1 §3.5 fixed on purpose as a divergence *toward* Java. The idle predicate must include
   "no queued submissions and no submitter in flight", and `DrainPending` must keep re-arming until
   both stages are empty (it already re-arms per iteration and is bounded by its timeout, so this is
   an extension of the existing loop, not a new wait).
2. **`WaitForSendsToReachCore`** (`AsyncMockProducer.cs:258`, the S7 test hook) rides on the same
   predicate and is fixed by (1) — but it must be checked, not assumed.
3. **Teardown must settle the queue.** `Stop` must guarantee every queued submission settles exactly
   once: faulted in S1 (today's semantics, preserving the current close contract), or appended in S2
   (F2's fix). Nothing may be left holding an unsettled `TaskCompletionSource`.
4. **Cancellation.** A queued submission whose token fires before it is appended must cancel with the
   caller's token (today's `SendWhenSpaceAvailable` behaviour, `NativeProducer.cs:566-570`) and must
   **not** be appended. The `CancellationTokenRegistration` disposal continuation chained to
   `completion` (`:502-507`) keeps working unchanged, because both paths still settle `completion`.
5. **Deviation DV-1 recorded**: the FIFO submission queue is a .NET-only mechanism with no anchor
   counterpart, present because `Send` has no post-append suspension point (§3.1).

### 3.5 DoD §10 — hot-path allocation impact (required statement)

The send path **is** the hot path and this fix touches it.

- **Inline path (the steady state, and what the budget test measures):** adds **one volatile read** of
  the queued-count and nothing else. **No new allocation.** `PublicProducerSendAllocationBudgetTests`
  and `PublicProducerDeliveryCallbackAllocationBudgetTests` must pass **unmodified** — if either needs
  a widened budget, that is a design error in the fix, not a budget to adjust.
- **Queued path (only when the bound is saturated):** one small submission struct/box enqueued onto a
  `ConcurrentQueue` **replaces** today's per-send `async Task<RecordMetadata>` state machine box in
  `SendWhenSpaceAvailable`, plus its `await completion.Task` awaiter. Expected to be **allocation-
  neutral to slightly cheaper** per saturated send, and it removes one async state machine per send in
  favour of one shared submitter loop. State the measured comparison in the slice's self-review; do
  not claim the improvement without it.
- **No change to pin counts per record** (≤2 key/value `MemoryHandle` + one interned topic pin), and
  no change to when they are taken relative to the permit.

---

## 3B · The completion-grouping design (D2 — the user's override)

**In plain language.** Today the completion thread takes whatever records happen to be waiting and
asks the core about all of them in one call, so unrelated records get bundled together and the whole
bundle waits for its slowest member. After this change the completion thread takes exactly the
records from one send call, asks about just those, and moves on. That is what Python does, and it is
why a slow record can no longer hold up records it was never sent with.

### 3B.1 The unit is one `send_batch` call, not one node

The anchor's unit is a `BatchNode`, and in the anchor a node **is** a `send_batch` call (`:593`
issues one call per node) **is** a `get_all` pass (`:487-495`). Those three coincide there, so
"per node" and "per call" are the same statement about Python.

They can come apart in .NET: `SendAccumulatorSettings` clamps the send chunk with
`BatchChunk = Math.Min(batchChunk, SlotCapacity)` (`SendAccumulatorSettings.cs:90`), so a **lowered**
`CONFLUENT_KAFKA_PRODUCER_BATCH_CHUNK` makes one node produce `ceil(count / chunk)` `send_batch`
calls (P3.1 §3.4's formula). Grouping per *node* would then bundle records from several `send_batch`
calls into one `get_all` — reintroducing exactly the shape F3 is about, under an override.

**Decision: group per `send_batch` call.** At the defaults `chunk == SlotCapacity == node capacity`,
so it is one call per node and identical to the anchor; under a lowered chunk it stays faithful where
per-node grouping would not. State the equivalence at the site so nobody "simplifies" it back to
per-node.

### 3B.2 How the pump carries group identity: a batch object, NOT the accumulator's node

Two candidate mechanisms, and the anchor's own answer does **not** transplant:

- *Python hands over the node itself* and transfers ownership: the poll thread reads
  `&node->complete_cbs[0]` / `&node->futures[0]` with `node->count` (`:487-495`) and then
  **frees the node** (`PyMem_RawFree(batch_to_free)`, `:510`).
- **.NET cannot**, because P3.1 made nodes **recycled**, not freed: `SendChain` → `RecycleNode` keeps
  one fully-settled node in `_spare` (`SendAccumulator.cs:818-844`) so the steady-state send path
  allocates no nodes. Handing the node to the pump either kills that recycling or creates
  cross-thread ownership of a recycled object plus a return channel — strictly more machinery for no
  behavioural gain.

**Decision: the accumulator enqueues one small immutable batch object per `send_batch` call**, and
the node's lifecycle is **untouched** (still settled and recycled on the batch thread, exactly as
today). The object carries the transferred futures plus their completions and delivery
registrations, and a count:

```
PendingSendBatch { IntPtr[] Futures; TaskCompletionSource<RecordMetadata>[] Completions;
                   DeliveryRegistration?[] Deliveries; int Count }
```

The pump's queue becomes `ConcurrentQueue<PendingSendBatch>` and its loop dequeues **one** batch per
pass — which is also the anchor's shape (`cnd_wait` only while the next node is `NULL`, `:504-507`).

**Three properties this shape preserves, each load-bearing:**

1. **Compaction is free.** `CompleteNode` already settles immediate-error indices in place and hands
   over only the accepted ones (`SendAccumulator.cs:914-976`). Building the batch arrays while
   walking the group naturally contains only the survivors — no separate compaction step.
2. **⚠ The ownership-transfer nulling order must not change.** Today the node's future slot is nulled
   **only after** `Enqueue` returns (`:968-969`), precisely so that if `Enqueue` throws (its queue
   growing under OOM) ownership never transferred and `FaultNode` still frees that future — and so
   that the index is *not* mistaken for "the core never saw this record", which would fire a delivery
   callback the pump is also about to fire (a **duplicate**, which the exactly-once obligation makes
   strictly worse than the drop — root `CLAUDE.md` §9.5). Under grouping the sequence becomes: build
   the batch → `Enqueue(batch)` → **then** null the node's future slots for the transferred indices.
   Same invariant, one enqueue instead of N. Say so at the site; this is the most delicate line in
   the slice.
3. **Allocation direction is favourable, and must be stated rather than assumed.** One batch object +
   three arrays per `send_batch` call replaces up to 1100 per-record `PendingSend` enqueues and their
   `ConcurrentQueue` segment churn. Also, P3.1 §12.3 recorded `DrainAll`'s per-drain
   `List<PendingSend>` as a deliberately-not-taken reuse follow-up — **grouping removes that list
   entirely** (the pump dequeues one batch; there is nothing to accumulate into a list), so that
   follow-up is closed by construction rather than left open. A batch-object **pool** is the new
   deliberately-not-taken item: record it, do not build it.

### 3B.3 ⚠ The one hazard grouping introduces: a group can exceed the pump's fixed arrays

**This is the highest-risk consequence of D2 and it is not hypothetical.** The pump's three reused
marshalling arrays are sized by a **compile-time const**:

```csharp
internal const int DrainCap =
    SendAccumulatorSettings.DefaultSlotThreshold + SendAccumulatorSettings.SlotCapacityHeadroom;  // :116-117
private readonly IntPtr[] _futures = new IntPtr[DrainCap];   // :127-129
```

and its remarks say so deliberately: *"Derived from the **default** constants, not from a producer's
(possibly overridden) settings … this is a bound on one marshalling pass, not a tuning knob."*

But node capacity is **runtime**: `SlotCapacity = slotThreshold + SlotCapacityHeadroom`
(`SendAccumulatorSettings.cs:83`) where `slotThreshold` comes from
`CONFLUENT_KAFKA_PRODUCER_BATCH_THRESHOLD` (`:138`), and the chunk defaults to
`threshold + SlotCapacityHeadroom` (`:148`). So with `CONFLUENT_KAFKA_PRODUCER_BATCH_THRESHOLD=5000`
a group can hold 5100 records while the arrays hold 1100.

Today that is harmless: `DrainAll(DrainCap)` chops the queue into ≤1100 pieces regardless of node
size, which is why `ProcessBatch`'s remarks can assert *"`count` can never exceed the arrays'
length"* (`:443-447`). **Grouping removes that chopping**, so an oversized group would hit
`Array.Clear(futures, 0, count)` with `count > Length` → throw → `RunLoop`'s `catch` faults the whole
group. Loud, but every send fails under a legitimate documented override.

**Decision: the pump splits a group larger than `DrainCap` into ≤`DrainCap` sub-passes.** Grouping
then holds exactly whenever a group fits (the default and every sane configuration), and degrades to
today's behaviour — never to a fault — when it does not. Chosen over the alternative (size the arrays
from the producer's effective `SlotCapacity` at construction) because it keeps the arrays const-sized
and keeps P3.1's stated rationale for a const bound intact, and because the split code is a `for`
loop over offsets rather than new state threaded from the accumulator into the pump. **Record the
alternative as considered.**

### 3B.4 What happens to `DrainCap` and to P3.1's S8a / S8b

- **`DrainCap` survives, with a changed job.** It stops being a *drain* cap (there is no drain to cap
  — one pass takes one group) and becomes (i) the **capacity of the three reused arrays** and (ii) the
  **sub-pass bound** of §3B.3. Its doc comment must be rewritten to say that; leaving it describing a
  drain cap is exactly the stale-paraphrase failure P3.1 kept hitting.
- **S8a's inner drain loop (the hang fix) stays, in a new form.** Its *purpose* — never leave the
  loop waiting on a `_signal` that was already `Reset()` while items remain — is unchanged and still
  load-bearing: after processing a group, keep going while another group is queued, and only then
  fall through to `_signal.Wait()`. The **reset-before-drain rationale is also unchanged** (it covers
  *concurrently arriving* items, which the inner loop does not) and must be preserved verbatim; P3.1
  §12.2.1 records that the two do not subsume each other.
- **S8b's reused arrays stay, and so does their hazard — carry it forward verbatim.** P3.1 §12.3
  records a **double-free**: a reused array retains the previous pass's handles past the current
  count, so *"every loop and every `destroy_all` must be bounded by `count`, never `Length`"*, plus
  the `Array.Clear(0..count)` on entry. **Grouping rewrites exactly those loops**, so the discipline
  must be re-established in the new code and the large-group-then-small-group test must survive as a
  large-group-then-small-group test (§6). Do not let the hazard drop out because the surrounding code
  changed — that is how it would come back.
- **`ProcessedBatchCount` / `LargestProcessedBatch` / `DrainedSendCount` all keep working**, and are
  the witnesses the grouping tests assert on. `DrainedSendCount` is counted in `DrainAll` today as
  *"the one point every queued send passes through exactly once"* — with a batch queue that point
  moves, so it must now count **records across batches** on both consumers, or the P3.1 §3.8
  teardown-ordering guard (which reads it as a record count) silently starts reading batch counts.
  ⚠ Four teardown tests assert `Expected: 24` against it; if they still pass while it counts batches,
  the guard has been broken silently.

### 3B.5 `DrainAndFaultRemaining` must still fault EVERYTHING — now per batch

P3.1 §12.2.2's asymmetry is unchanged and must be restated in the new shape: **`ProcessBatch`'s pass
is bounded; the terminal drain is not.** It faults rather than calling `get_all`, so it needs none of
the marshalling arrays, and it is the last thing that ever touches the queue — capping it strands
`TaskCompletionSource`s (awaiters hang forever) and leaks future handles.

Under grouping it must dequeue **every** batch and fault **every** element of each, freeing every
future. Its current body drains into one list and allocates one `IntPtr[count]`
(`SendCompletionPump.cs:606-624`); the batch version loops batches and frees per batch. **Grouping
must not reintroduce a cap here by accident** — e.g. by reusing the §3B.3 sub-pass helper, which is
bounded. Keep them separate functions and say why at both sites.

Residual 2 (this path fires no delivery callback) is unchanged in kind.

### 3B.6 Residual bookkeeping — edit the axes in place, do not paraphrase

The distinguishing axes for the four recorded residuals live **once**, in the remarks on
`IDeliveryCallback` (`IDeliveryCallback.cs:141-232`), and every other site is explicitly forbidden
from restating them — `FaultBatchCompletions`' own note records that *"several review rounds went on
paraphrases that went stale one at a time."* So:

- **Residual 3(a) narrows in scope and must be edited at the axes.** Its text reads *"it reported for
  the **whole** batch, so the core **did** report these completions; the indices the pump had already
  reached fired normally and the rest are faulted with none"* (`:171-175`). Under grouping "the whole
  batch" becomes **one `send_batch` group** instead of an arbitrary mixture of up to 1100 records
  from arbitrarily many sends — so one residual-3(a) event now has a bounded, *related* blast radius.
  That is a genuine narrowing of the same kind as §12.3's, and it is recorded the same way: narrowed,
  **not** closed (the batched read still exists, and the sync surface still shares both conditions).
- **Residual 4's second trigger must be re-checked, not assumed.** It names *"a P/Invoke failure from
  a later chunk of the same node"* (`:198-199`) as a way an already-accepted record is destroyed
  unread. If the batch is enqueued immediately after **its own** `send_batch` returns (§3B.1), an
  earlier chunk's records are already handed over when a later chunk fails, so that trigger goes
  away and only the pump-queue OOM remains. **Verify against the implemented order and update the
  axes if it holds; do not write the narrowing before the code earns it.**
- **`ProcessBatch`'s own remarks** (`:422-459`) describe the reused arrays, the `count`-not-`Length`
  rule, the "RunLoop is the only caller and its drain is capped" claim, and the delivery-callback
  firing site. The middle claim becomes false as written (the bound is now the group + the §3B.3
  split) and must be rewritten rather than left as a comment that no longer matches the code.
- **No new residual is expected.** Grouping moves *when* futures are handed over, not *whether*; the
  target is the same four residuals with 3(a) narrowed. If the Actor cannot close a path, it becomes a
  documented fifth residual with the same rigor as the others — **state which outcome was reached in
  the slice self-review** (P3.1 §6.2's rule).

### 3B.7 Mode and ABI

**Still Mode A.** Grouping is entirely managed: the batch object, the queue's element type, the
pump's loop and the accumulator's hand-over site. `FutureRecordMetadataGetAll` /
`FutureRecordMetadataDestroyAll` are already declared and already take `(array, count, out, out)`, so
a group is just a different `count` — **no new `[DllImport]`, no header change, no `src/**` change.**
If the Actor finds itself wanting an ABI change, that is a Mode B dependency to **raise loudly**, not
to design around.

---

## 4 · Correctly translated, not divergent (the GIL part — no action, do not re-file)

These are Python-side mechanisms with **no** .NET counterpart, or whose .NET counterpart is already
the faithful translation. A reviewer comparing the two files will hit all of them; none is a finding.

| Python | .NET | Why it is not a divergence |
|---|---|---|
| `Py_BEGIN_ALLOW_THREADS` / `Py_END_ALLOW_THREADS` around the mutex work (`:804`, `:832`; `:855`, `:872`; `:960`, `:970`) | nothing | Releases the GIL so other Python threads run. .NET has no GIL. |
| `PyGILState_Ensure` / `Release` batching in `Producer_complete_callbacks` (`:435`+) and around the immediate-error compaction (`:603`, `:624`) | nothing | GIL acquisition amortization. No analogue. |
| `Py_INCREF(record)` / `Py_INCREF(complete_cb)` (`:800-801`), `Py_DECREF` in the completion callback (`:411-419`) | `MemoryHandle` pins + managed references held by the node's parallel arrays | Refcounting keeps a *non-moving* CPython object alive; .NET's GC moves objects, so the faithful translation is pinning (P3.1 §3.9/§4.4, `ffi-marshalling.md` §A4). |
| per-record `char* topic_owned` malloc (`:30-36`) | `PinnedTopicCache` interning (P3.1 §4.1) | .NET cannot copy per record without the allocation CLAUDE.md §12 forbids; interning is O(distinct topics) instead of O(records). The 1024 cap is already recorded as the sole non-Python constant (P3.1 §12.1). |
| the **motivation** for batching: amortizing GIL acquisition + boundary crossings | adopted anyway, on user direction | .NET buys neither (a P/Invoke is ~5 ns). P3.1 §2.2 records the *actual* .NET-side reason (a managed, cancellable wait instead of a native block inside the core's coarse mutex). Not a defect, and not to be re-argued. |
| `space_cbs` callback list + `on_space_available` (`:843-879`) | `SemaphoreSlim` permits | Different primitive, same property (no lost wakeup), already recorded at `SendAccumulator.cs:182-193`. **But note F2**: the *strictness* difference that primitive introduces is NOT covered by that note. |

---

## 5 · Slicing

Smallest-first, with the behaviour changes isolated from the doc corrections so a regression is
attributable. The Critic runs after **each** slice (`agent-roles.md`).

| S | Slice | Size | Why it sits here |
|---|---|---|---|
| **S0** | **Doc-only corrections: F8 (rewrite the stale `python-binding-send-batching.md` §408-459 — row by row, §F8(c)), F2's bound-strictness deviation, F7's confirmation, and the §8.3 supersession.** Files **DV-1, DV-2, DV-4, DV-6** only. ⚠ **Does NOT file DV-3 or the old per-record DV-5** — D2 deletes the first and §3B.1 rewrites the second; S3 files the narrowed DV-5 and DV-7 when the code lands. F3's claim-narrowing text is written **here** but must describe the difference as *removed by S3*, not as a kept deviation. | **S** | **First**, exactly as P3.1's S0 went first: a Critic reading the current record mid-phase would correctly flag S1's design as contradicting a claim in the record. Also the cheapest way to bank most of the "record it" half of the bar before touching the hot path. |
| **S1** | **F1 — the ordering fix.** Routing + FIFO submission queue + single submitter, both entry points on `SendAccumulator`, fixture migrated to them (DoD §12), the two-stage idle predicate (§3.4 items 1–2), teardown settles the queue with **today's** fault semantics (§3.4 item 3), cancellation preserved (item 4). | **L** | **Alone.** It is the only merge-blocking correctness change, it touches the hot path and the DoD §10 budget, and it must be attributable. Behaviour at teardown deliberately **unchanged** here so that S2's change is separable. |
| **S2** | **F2 — close completes a parked/queued send instead of faulting it.** `Stop` flushes the submission queue into the chain before `_closed`, ahead of P3.1 §3.8 step 3. | **S** | Gated on S1 (there is no queue to flush before it). Separate because it changes an **observable outcome** at teardown, and P3.1's teardown work is where the recorded Critic findings cluster — a fault here must not be ambiguous with S1. |
| **S3** | **F3/F6 — per-`send_batch` completion grouping** (§3B): `PendingSendBatch`, `ConcurrentQueue<PendingSendBatch>`, one group per pass, the §3B.3 oversized-group split, `DrainCap` re-documented, S8a's inner loop re-expressed, S8b's `count`-not-`Length` discipline re-established, `DrainAndFaultRemaining` per batch (still uncapped), `DrainedSendCount` still counting **records**, the §3B.6 residual/axes edits. | **L** | **The second L, and the phase's largest structural change.** Own slice, and **after** S1/S2 even though it is independent of them (§5.1) — the completion side must not be in flight while the submission side is being reviewed. |
| **S4** | **F4 — bounded pre-stop pump drain** (D3): after the teardown flush and before `_stopping`, wait bounded for the pump queue to empty; on expiry, today's behaviour. | **M** | **After S3, deliberately.** Its wait predicate is "the pump's queue is empty", and the queue's element type changes in S3 — landing this first means writing it twice and reviewing it twice. Both slices touch `RunLoop`/`Stop`, so keeping them adjacent-but-separate is what makes a regression attributable to one. |
| **S5** | **F5 — `_spaceGate.Cancel()` in `AbandonOnThreadFailure`**, plus the comment stating why it is unconditional. | **S** | Last: independent, one line, and it needs the S1 queue in place to state the invariant over both stages (a queued submission awaiting a permit is a waiter too). |

### 5.1 Sequencing — why S1 and S3 are independent but still ordered

**They are on opposite sides of the accumulator and share no state:** S1 changes what happens
*before* a record is appended (routing + FIFO submission queue); S3 changes what happens *after*
`send_batch` returns (how futures are handed to the pump). Neither reads the other's data
structures. So the ordering is **not** a dependency — it is an attributability choice: S1 is already
L and touches the DoD §10 hot path, S3 is L and rewrites the pump's safety-critical loops, and having
both in flight would make any teardown or completion regression ambiguous between them. **S1 → S2 →
S3 → S4 → S5**, each reviewed before the next.

**Not in scope** (a Critic flags any of these appearing): any change to the sync `Send` / `Flush` /
`Close` path; any public API change; any `src/**`, `src/ffi/**`, `cbindgen.toml`,
`confluent_kafka.h` change (§7); reviving `Producer_send_async` or any push-completion engine (§A7
Option B); a second pump; Option D's in-flight cap; a batch-object **pool** (§3B.2, deliberately not
taken); capping `DrainAndFaultRemaining` (§3B.5); changing `RunContinuationsAsynchronously` or the
delivery-callback ordering (callback before awaiter release, unconditional); root `CLAUDE.md`.

---

## 6 · Tests, and how each is proved to work

**The local standard is mutation/injection proof**, because P3.1's Critic round found **two**
properties whose tests passed with the property mutated (`STATUS.md:20`). Every test below names the
mutation that must make it fail. A test whose stated mutation does not fail it is not evidence and
must be redesigned, not accepted.

### S1 tests (the ordering fix)

1. **`SendAccumulator_SubmissionOrder_IsCallOrder_AcrossTheBackpressureBound`** — ⚠ **the F1
   regression test, mandatory.** Same-thread sends across a saturated bound; assert the records
   handed to `send_batch` are in **call order**.
   - *Witness:* the pending node's per-slot identity, read through the fixture's existing reflection
     seam (`PendingNode()`, `SendAccumulatorTests.cs:1052-1062`) — compare `Completions[i]` by
     reference against the list of `TaskCompletionSource` objects the fixture created in call order
     (add an `AppendOne` overload that hands the TCS back). This observes the exact array
     `send_batch` will read, needs no production hook and no `unsafe`. The alternative witness — the
     tag byte via `Marshal.ReadByte(node.Natives[i].Value)`, since `NewRecord(tag)` writes the tag
     first (`:1064-1065`) — is equivalent; pick one and say why.
   - *Proof:* revert the routing predicate to unconditional (`_queued == 0` → `true`) and the test
     must fail. Also run it against `HEAD` before the fix: it must fail there.
   - *Determinism:* assert in two parts, because the raw interleaving is a race. (i) **Deterministic:**
     with one submission queued, the inline path is refused for the next `Send` and that send lands
     behind it in the queue. (ii) **Stress:** N ≈ 200 same-thread sends across a bound of ~4 with an
     explicit-drain harness; the observed order must equal the call order on every iteration. Part (i)
     is the assertion that cannot flake; part (ii) is what would have caught the bug and must be shown
     to fail pre-fix.
2. **`SendAccumulator_TwoConsecutivelyParkedSends_AppendInCallOrder`** — the case a bare counter does
   **not** fix (§3.3): park two sends, then release many permits at once.
   - *Proof:* replace the FIFO submitter with per-send `WaitForSpaceAsync` continuations (the counter-
     only design) and the test must fail — this is the evidence for D1's choice over the cheaper one.
3. **`Flush_IncludesASendStillQueuedForSpace`** — §3.4 item 1. Saturate the bound, issue a send that
   queues, call `Flush`, assert its `Task` is resolved after `Flush` returns.
   - *Proof:* revert the idle predicate to `_head is null && !_draining` → must fail. (This is the
     guard for silently re-opening the gap P3.1 §3.5 closed on purpose.)
4. **`Teardown_SettlesEverySubmissionStillQueuedForSpace`** — every queued submission settles exactly
   once (S1: faulted), no `Task` left pending, no leaked pin (nothing is pinned while queued — assert
   that too).
   - *Proof:* skip the queue in `Stop` → the awaiting `Task` never completes → the test's hard timeout
     fails it. Must fail rather than hang.
5. **`QueuedSubmission_CancelledBeforeAppend_IsNotSentAndCancelsWithTheCallerToken`** — §3.4 item 4.
   - *Proof:* drop the pre-append cancellation check → the record is appended → assert on the core's
     history count catches it (`Harness.HistoryCount`, `:909`).
6. **Allocation budget, unmodified.** `PublicProducerSendAllocationBudgetTests` +
   `PublicProducerDeliveryCallbackAllocationBudgetTests` pass with **no** edit (§3.5).
   - *Proof:* the fixtures are pre-existing; the evidence is that the diff does not touch them. Any
     edit to a budget number is a finding against the slice. ⚠ Read the recorded note on marginal
     allocation tests (take best-of-N matched attempts, per `ffi-marshalling.md` §A4's amended
     *Tests required*) before touching the harness.
7. **Fixture-fidelity check (DoD §12):** the harness no longer contains its own routing —
   `AppendOne` / `TryAppendOne` call the accumulator's entry points.
   - *Proof:* structural. Grep for a second copy of the routing rule; its presence is the finding.

### S2 tests (close completes instead of faulting)

8. **`Close_CompletesASendThatWasQueuedForSpace_RatherThanFaultingIt`** — assert the record reaches
   the core (history count) **and** its `Task` completes successfully **and** its delivery callback
   fires exactly once.
   - *Proof:* restore the fault path → the test fails on the success assertion. Also assert the
     callback count, since exactly-once is the property most easily broken by appending a record whose
     awaiter was already faulted.
9. **`Close_WithQueuedSubmissions_DoesNotExceedTheBoundedTeardownWait`** — the bypass must not turn
   `Stop`'s bounded wait into an unbounded one.
   - *Proof:* injection — park the batch thread (the fixture's existing delivery-callback park, used
     by the `Flush` drain-expiry test) and assert teardown still returns inside the bound.

### S3 tests (per-`send_batch` completion grouping)

⚠ **"The records all arrived" cannot see any of these properties** — an ungrouped pump satisfies it
too. Every assertion below is on a **counter, a size, or a relative completion order.**

10. **`Pump_CompletesOneGroupPerSendBatchCall_WithEachGroupsOwnSize`** — the grouping property.
    Drain a chain spanning **multiple** groups and assert `ProcessedBatchCount` equals the number of
    `send_batch` calls (`SendAccumulator.SendBatchCallCount` is the independent witness, already
    exposed at `:155`) **and** that `LargestProcessedBatch` equals the largest group's size — not
    their sum.
    - *Proof:* revert to a flat per-record queue → one pass carries the union →
      `ProcessedBatchCount` collapses toward 1 and `LargestProcessedBatch` becomes the total. Must
      fail. Run it on `HEAD` too: it must fail there.
11. **`Pump_ASlowRecordInOneGroupDoesNotDelayAnotherGroupsCompletions`** — ⚠ **the head-of-line
    property, which is the entire latency argument for D2 and is unassertable from record arrival.**
    On a manual (`auto_complete: false`) mock: fill group 1 to capacity and add group 2, drain both,
    then `CompleteNext()` exactly `count(group 1)` times and assert **group 1's tasks all resolve
    while group 2's stay pending**.
    - *Determinism:* rests on the mock's `CompleteNext` completing the **oldest** pending record, and
      on group 1's `send_batch` preceding group 2's (guaranteed — `SendChain` walks in order). The
      Actor MUST verify `MockProducer::complete_next`'s FIFO semantics in the Rust core before
      relying on it, and if it is not FIFO, complete via the group-1 records' own handles instead.
    - *Proof:* revert to the flat queue → one `get_all` spans both groups → group 1's tasks stay
      pending until group 2 completes → must fail. This is the test that would have made F3 visible.
12. **`Pump_AGroupLargerThanDrainCap_IsSplitAndStillCompletes`** — ⚠ **the §3B.3 hazard, and the most
    likely place this slice breaks something that works today.** Set
    `CONFLUENT_KAFKA_PRODUCER_BATCH_THRESHOLD` above `DrainCap - SlotCapacityHeadroom` (e.g. 5000 →
    node/group capacity 5100 vs arrays of 1100), fill one full group, and assert **every** record
    completes successfully and `ProcessedBatchCount` shows `ceil(count / DrainCap)` passes.
    - *Proof:* remove the split → `Array.Clear(futures, 0, count)` throws → `RunLoop`'s `catch`
      faults the whole group → the test fails on the success assertion. Assert on success, **not** on
      an absence of a fault message (`STATUS.md:20`'s message trap).
13. **`Pump_ALargeGroupFollowedByASmallOne_FreesExactlyTheSmallGroupsHandles`** — P3.1 §12.3's
    double-free guard, **carried forward verbatim** into the grouped shape. This is the only test
    shape that catches a `Length`-instead-of-`count` bound, and S3 rewrites exactly those loops.
    - *Proof:* change one loop bound from the group's count to `Length` → a stale handle from the
      previous pass is freed a second time. ⚠ A double-free **aborts the test host and `dotnet test`
      still exits 0** — grep the output for `Test Run Aborted`; that is the only signal.
14. **`Teardown_WithGroupsQueued_FaultsEverySendInEveryGroup`** — §3B.5: the terminal drain is still
    uncapped and now per batch.
    - *Proof:* cap it at one batch (or at `DrainCap`) → the count of faulted tasks is short of the
      count enqueued → must fail. Assert **counts**, not messages.
15. **`DrainedSendCount_StillCountsRECORDS_NotGroups`** — §3B.4's silent-break guard. Enqueue a known
    number of records across ≥2 groups and assert the counter equals the **record** count.
    - *Proof:* count batches instead → the number drops to the group count → must fail. ⚠ Also
      re-run the four P3.1 teardown-ordering tests that assert `Expected: 24` against this counter;
      if they pass while it counts groups, the §3.8 guard has been broken silently.
16. **Residual/axes edits are structural (§3B.6):** residual 3(a) narrowed **at the axes** in
    `IDeliveryCallback.cs:171-175`; residual 4's second trigger re-checked against the implemented
    hand-over order and updated only if the code earns it; `ProcessBatch`'s "its drain is capped at
    `DrainCap`" claim rewritten; **no paraphrase of the axes added anywhere else.**
    - *Proof:* review-time. The finding is a stale claim left in place, or a second copy of the axes.

### S4 tests (bounded pre-stop pump drain, D3)

17. **`Teardown_CompletesSendsAlreadyQueuedToThePump_RatherThanFaultingThem`** — enqueue to the pump,
    then tear down; assert completion, not fault.
    - *Proof:* remove the bounded wait → the pre-existing behaviour returns → the test fails.
      ⚠ The exception type/message trap recorded in `STATUS.md:20` applies directly: a faulted send and
      an accepted-residual send share an identical `ObjectDisposedException` message containing
      "closed", so **assert on success and on `DrainedSendCount`/`ProcessedBatchCount`**, never on the
      absence of a particular fault message.
18. **`Teardown_WithAnUnresolvableSend_StillReturnsWithinTheBound`** — the `AsyncMockProducer.Clear()`
    premise break (P3.1 §6.3). Must return, faulting the remainder.
    - *Proof:* make the wait unbounded → the test hangs → it must be written with a hard timeout that
      **fails** rather than hangs the suite.

### S5 tests (gate cancellation on thread death)

19. **`BatchThreadFailure_ReleasesASendWaitingForSpace`** — using the existing
    `AppendWithoutAPermit` injection (`SendAccumulatorTests.cs:980-985`) to kill the thread with a
    `SemaphoreFullException`, with a send parked/queued for space.
    - *Proof:* remove `_spaceGate.Cancel()` → must fail. ⚠ If it still passes, that is the unstated
      permit-arithmetic invariant doing the work — say so in the self-review and keep the explicit
      cancel anyway (that outcome is the argument for the line, not against it).

### Suite-level

20. `make verify` / the dotnet leg green (DoD §9). Baseline to beat: **880 passed / 0 failed** on
    `-f net10.0`. Also `cargo build --features ffi` (unchanged — Mode A, §7).
21. **No perf gate** (P3.1 D3 still stands: performance is post-implementation). Do not spend Actor
    time on a perf number; do record in the S1 self-review that the inline path added one volatile
    read and no allocation, since that is a correctness-of-design claim, not a measurement.
    ⚠ **S3 is the exception worth naming:** grouping is justified by a *latency* argument, and the
    only thing this phase asserts about it is the structural head-of-line test (11). Do **not** claim
    a latency improvement without a measurement — record it as the expected effect and leave the
    number to the deferred perf follow-up (P3.1 §8.2), whose apparatus is unchanged.
22. **The existing `SendCompletionPumpDrainCapTests.cs` must be migrated, not deleted.** It asserts
    P3.1 S8a's numeric cap (tests 24–26 of that phase). Under grouping the cap's *job* changes, so
    each of its assertions is either re-expressed against groups or removed **with a stated reason**.
    A silently deleted test file is a finding.

---

## 7 · Mode declaration

**Mode A — .NET-only.** No finding's recommended fix needs a Rust or ABI change:

- F1/F2/F5 are entirely inside `SendAccumulator` + `NativeProducer.SendViaPump`.
- **F3/F6 (grouping, D2) are entirely managed** — the batch object, the queue's element type, the
  pump's loop and the accumulator's hand-over site. `FutureRecordMetadataGetAll` /
  `DestroyAll` already take `(array, count, out, out)`, so a group is just a different `count`: **no
  new `[DllImport]`, no header change** (§3B.7).
- F7/F8 are documentation.
- F4's fix is inside `SendCompletionPump` + `NativeProducer` teardown.

**Evidence to capture at close** (P3.1 §10's format): `git diff --stat` over `src/**`,
`src/ffi/**`, `confluent_kafka.h`, `cbindgen.toml` showing **zero** changes.

**Mode B dependencies flagged, not designed around:**

1. **The coarse producer mutex** (`src/ffi/producer.rs:1500`) — one `send_batch(N)` holds
   `handle.kind.lock()` across N sequential `block_on` sends, so a `Close`/`Flush`/`Metrics` can be
   parked for a whole chunk. Already recorded as accepted (P3.1 §3.4/§3.6) with finer-grained locking
   named as the only complete fix. **Unchanged by this phase; do not attempt it here.**
2. **Grouping (D2) is Mode A** — a managed pump restructure, stated here so nobody assumes the ABI is
   implicated. ⚠ If the Actor concludes otherwise (e.g. that a group needs a new ABI entry point),
   that is a **Mode B dependency to raise loudly and stop on** — not something to design around.

---

## 8 · Rules: conflicts, provenance, required amendments

### 8.1 `ffi-marshalling.md` §A4 — no conflict; one amendment needed

§A4 was amended by P3.1 and already covers the deferred-send pin window (`:531-545`), the
`MemoryHandle` primitive (`:523-530`), the static sentinel (`:560-567`) and the interned topic
(`:568-576`). The F1 fix **preserves every one of those** — the queue holds unpinned records and
pinning stays inside `Submit`.

**Amendment (deliverable, S1):** §A4 says nothing about **submission order**, and neither does §A1 or
§A7. Ordering is a real invariant of the send path that no rule states, which is a large part of why
F1 shipped. Add it — recommended home is **§A1** (the producer thread model, where the accumulator and
batch thread are described), as a Rule bullet plus a *Tests required* line:

> The submission path must hand records to the core in the order a caller called `Send`. Java
> documents ordering as preserved in the default configuration
> (`ProducerConfig.java:274`), and a binding-side reorder happens **before** the core sees the
> records, so no core-side setting can restore it. A deferred/accumulating submission path must not
> let a send that finds capacity overtake one still waiting for it. Do **not** rest this on
> `SemaphoreSlim` fairness — the .NET docs guarantee no ordering for semaphore waiters.
> *Tests required:* the order records reach `send_batch` equals call order, across a saturated bound.

### 8.2 `ffi-marshalling.md` §A1 — no conflict

§A1's "at most two background threads" (amended by P3.1, `:240-244`, `:272-279`) is unaffected: the
FIFO submitter is **not** a thread. It is a task-based loop driven by the caller that starts it, with
**at most one** in flight, and it does not poll. State that explicitly in the S1 commit and in the §A1
amendment above, because "a single submitter" will read like a third thread to a reviewer scanning the
diagram. If the Actor finds itself wanting a dedicated thread for it, that is a rule conflict to
surface — **not** a design to take unilaterally.

### 8.3 P3.1 §1.5's pump carve-out is superseded — ONE new boundary, stated once, for BOTH items

P3.1 §1.5 fixed a mechanical boundary for the pump: capping `DrainAll` plus the loop change it
requires are in scope; *"Any other `RunLoop` restructuring"*, *"Changing `ProcessBatch`'s per-index
completion logic"*, and *"Changing `Stop` / `CloseGate` / `Enqueue` / `_stopLock` semantics, or the
queue type"* are **not**.

**This phase breaches that in two places** — S3's grouping (which changes the queue type, `RunLoop`,
`ProcessBatch` and `Enqueue`) and S4's pre-stop wait (adjacent to `Stop`). Two ad-hoc breaches would
leave a Critic with no boundary at all, so the table is **superseded as a whole, once**, here.

**Provenance:** P3.1 §1.5 is a phase-scope boundary written by the Manager and approved by the user on
2026-09-07 **for P3.1**. A phase-scope boundary does not bind the next phase — but per the standing
practice it must be superseded **explicitly**, in P3.1's own idiom (§2), never quietly contradicted.
D2 (2026-09-10) is the authority for the grouping half; D3 for the teardown half.

**The new boundary, applicable mechanically:**

| In scope for M11/P3.2 | Still out of scope after this phase |
|---|---|
| The pump's queue **element type** (`PendingSend` → `PendingSendBatch`) and `Enqueue`'s signature/arity (S3) | `Enqueue`'s **semantics**: the `_stopLock`-guarded stopped-check, and the fault-in-place branch firing **no** delivery callback (residual 1) |
| `RunLoop`: one group per pass, the inner loop re-expressed over groups, the §3B.3 oversized-group split (S3) | The **reset-before-drain ordering** and its rationale (P3.1 §12.2.1 — it covers concurrently arriving items and the inner loop does not subsume it); the fault-and-**continue** `catch` |
| `ProcessBatch`: the group's arrays, its `count` bound, its remarks (S3) | Its **per-index completion logic** and ordering — delivery callback immediately before `TrySet*`, unconditional, `Fire` as the total no-throw boundary, no second `try`/`catch` |
| `DrainAndFaultRemaining` rewritten to fault **per batch** (S3) | Capping it, in any form (P3.1 §12.2.2 — strands TCSes, leaks futures) |
| `DrainCap`'s **job** (array capacity + sub-pass bound) and its doc comment (S3) | Making the arrays runtime-sized from producer settings (§3B.3's rejected alternative) |
| A bounded, **non-draining** wait for the pump queue to reach empty, after `CloseGate` + flush, before `_stopping`; and its call site in `NativeProducer` teardown (S4) | The `_stopping` check's **position** in `RunLoop` — moving it after the drain is the N=51 hang (F4(c)); `Stop`'s / `CloseGate`'s own semantics |
| — | A batch-object pool; a second pump; any push-completion engine; the `RunContinuationsAsynchronously` construction |

### 8.4 Provenance of the rules generally

§A1/§A4/§A7 each carry a stated *Why* and a cited contract, which per the standing guidance makes
them the binding class rather than agent-authored boilerplate — and none of them blocks a fix here.
Two are being **extended** (§A1 gains the ordering invariant it was missing), one is untouched (§A4's
substance), and one phase-scope table is being **narrowly superseded** with the replacement written
down (§8.3). **Do not edit root `CLAUDE.md`.**

### 8.5 Definition of Done

`.claude/rules/definition-of-done.md` applies in full. Specifically live:

- **§10** hot-path allocation audit — §3.5 states the expected impact of the F1 fix, and the two
  existing budget test files must pass **unmodified**. For **S3**, the direction is favourable and
  must still be stated rather than assumed (§3B.2 item 3): one batch object + three arrays per
  `send_batch` call replaces up to 1100 per-record enqueues and their queue-segment churn, and
  `DrainAll`'s per-drain `List<PendingSend>` disappears entirely. Completion is per-batch, not
  per-record, so it is **not** on the DoD §10 per-record send path — say that explicitly instead of
  skipping the item silently.
- **§12** test-fixture fidelity — the reason the routing decision must live on `SendAccumulator`
  (§3.3); the fixture currently duplicates it.
- **§3** error-message content asserted — with the §6/`STATUS.md:20` caveat that a fault message
  containing "closed" cannot distinguish the accepted residual from the defect, so the S2/S3 tests
  assert **success and counters**, not the absence of a message.
- **§7** structs not in Java — the submission queue is binding-internal machinery; its rationale is
  §3.1 (the API-shape constraint) and it is recorded as DV-1.
- **§9** `make verify` / the dotnet leg green.
- **§11** is consumer-only — N/A.

---

## 9 · Filing location and phase id

**This file:** `bindings/dotnet/design/history/M11/P3.2-producer-send-ordering-parity/PLAN.md`.

**A new phase directory, not an addition to P3.1's.** A phase directory holds **only** `PLAN.md` and
`COMMENTS.DONE.<N>.md` — user directive 2026-09-07, recorded at P3.1 PLAN §10 and true of every
sibling (`P3-producer-send/`, `P2.1-collapse-producer-teardown/`, `P4.1-producer-naming-cleanup/`).
So nothing may be added to `P3.1-producer-python-alignment/` beyond its own `COMMENTS.DONE`.

**Phase id `P3.2`, and why:**

- The sibling convention is `P<n>.<m>-<topic>` for a follow-up on the same surface as `P<n>`
  (`P2` → `P2.1`, `P3` → `P3.1`, `P4` → `P4.1`). This is a follow-up to `P3.1` on the same surface, so
  it takes the next decimal on the `P3` line: **`P3.2`**.
- Not `P3.1.1` — no sibling uses two decimals, and this is not a sub-slice of P3.1: it changes
  behaviour P3.1 shipped and supersedes one of its scope boundaries (§8.3).
- Not a new top-level phase (`P9`+) — the surface, the anchor and the decision record are all P3.1's,
  and a reader looking for "why does the async send path look like this" must find both plans adjacent.
- The topic suffix names the deliverable: `producer-send-ordering-parity` — the ordering fix plus the
  parity record it closes.

**`STATUS.md` edits:**

1. **At close:** a new dated `Milestone 11 / Phase 3.2` entry in the established format (DONE date, N,
   Mode A confirmation with the `git diff --stat` evidence, plan pointer, branch, commits,
   deliverables), plus an update to the accepted-residuals section for whichever of F4's outcomes
   lands.
2. **`STATUS.md:20`'s "out-of-scope pump race"** must be restated to reflect its post-accumulator
   exposure (F4) — in **S4**, alongside the fix, since D3 was approved (the note becomes "narrowed to
   the pathological case", not "widened").

**Agent number: N = 71** (assigned 2026-09-10, when the user approved the plan). Verified free —
no `COMMENTS*.71.md` exists anywhere in the tree; the .NET binding's sequence runs continuously to
**70** (66–70 are the M15 Admin binding work) and P3.1 ran as **N=65**.

**`COMMENTS` file locations — what P3.1 actually did, not what is inferable:** the working files live
at **`bindings/dotnet/COMMENTS.71.md`** and **`bindings/dotnet/COMMENTS.DONE.71.md`**, which is where
`COMMENTS.DONE.65.md` sits today (the repo also has a root-level set, used by the Rust-core agents —
do not use it for this phase). The **final** copy of `COMMENTS.DONE.71.md` is placed in this phase
directory at close, per the phase-directory convention above; P3.1's own copy has not been moved
yet, which is why its directory currently holds only `PLAN.md`.

---

## 10 · Deviation list (the §3-style record this phase produces)

Filed here and mirrored into P3.1's record by S0, so a Critic can tell "deliberate" from "missed".

| # | Deviation from the anchor | Why | Where |
|---|---|---|---|
| **DV-1** | A FIFO submission queue with a single appender in front of the accumulator; Python appends inline with no queue | .NET's `Send` returns the record's delivery `Task` (Java's shape) and has no post-append suspension point to carry the throttle; Python's `send` is a coroutine and does. Keeps the *property* (call order) at the cost of a .NET-only mechanism | §3.1, §3.3 |
| **DV-2** | The stage-1 bound is **hard** (exactly `MaxAccumulatedRecords`); Python's is **soft** (up to `bound + C − 1` with C concurrent senders) | An artefact of Python's check-after-append. A hard bound is strictly more conservative; overshooting a memory bound to imitate an artefact buys nothing | F2 |
| ~~DV-3~~ | ~~A completion batch may span many `send_batch` drains~~ | **DELETED — the phase removes it.** D2 chose to implement grouping (§3B), so a completion batch becomes exactly one `send_batch` group, as the anchor's `BatchNode` is. A phase must not record a deviation it closes; S0 must not file this entry | F3, §3B |
| **DV-4** | At teardown, a send queued to the completion pump may still be faulted rather than completed, in the pathological case | Python's poll thread drains unconditionally; .NET has a recorded case (`AsyncMockProducer.Clear()`) where "the flush resolves everything" is false, and `get_all` cannot be bounded. Reduced to the pathological case by S4 | F4 |
| **DV-5** (narrowed) | Groups are handed to the completion thread **as each `send_batch` returns**; Python walks the whole chain first, then splices it and signals **once** (`:629-638`) | Earlier first completion — the pump can start group 1 while the batch thread is in group 2's `send_batch`. **The grouping itself is now identical**, so this can no longer change *which* records share a `get_all`; only the hand-off timing differs, in .NET's favour. The old "per record, and it is the mechanism behind DV-3" form is superseded by §3B | F6, §3B.1 |
| **DV-7** | A group larger than the pump's fixed marshalling arrays is split into ≤`DrainCap` sub-passes; the anchor cannot have this case (its arrays and its node are one compile-time `#define`) | .NET's node capacity is **runtime** (`CONFLUENT_KAFKA_PRODUCER_BATCH_THRESHOLD`) while `DrainCap` is a **const** — §3B.3. Grouping holds exactly when a group fits, which is the default and every sane config; the split degrades to today's behaviour rather than to a fault | §3B.3 |
| **DV-6** | The **sync** `Send` does not use the accumulator at all; Python routes both surfaces through it | .NET's sync `Send` returns a materialized `RecordMetadata`, so the 0–10 ms window would land on every sync send against p50 ≈ 7 ms. User-directed (P3.1 §3.1); re-affirmation requested as D4 | F7 |

---

## 11 · Resolved decision record — all five settled

**All five decisions were taken by the user on 2026-09-10. There are no open questions gating the
Actor.** Recorded as settled inputs, with the recommendation D2 overrode kept in full so the
reasoning stays auditable rather than erased (P3.1 §11's practice).

| # | Decision (user, 2026-09-10) | One-line rationale | Where implemented |
|---|---|---|---|
| **D1** | **(a) — routing counter + FIFO submission queue with a single appender.** *(As recommended.)* | The only option that fixes **both** reorder sources, keeps the public API and the throttle, and does not rest on `SemaphoreSlim` fairness (which .NET explicitly does not guarantee) | §3.3, S1 |
| **D2** | **(iii) — IMPLEMENT per-`send_batch` completion grouping IN THIS PHASE.** ⚠ **Overrode the plan's recommended (ii)** "doc-only now + a scoped follow-up phase". | Full parity on the completion side, **structurally** rather than numerically: a completion batch becomes the anchor's `BatchNode` unit, so the ≤1100 bound follows from the unit instead of a hand-picked cap — and the head-of-line behaviour, which is what the difference actually costs, depends on the shape and not the number | §3B, S3 |
| **D3** | **Fix — bounded pre-stop pump drain.** *(As recommended.)* | Makes the normal case Python-identical; cannot hang, because `CloseGate` has already run so the wait is monotone; degrades to today's behaviour on expiry | F4(c), S4 |
| **D4** | **Re-affirm — the sync path stays inline. No work.** *(As recommended.)* | The quantitative case holds (p50 ≈ 7 ms baseline vs a 0–10 ms window on every sync send), and the sync surface is also the only one free of the buffer-mutation window and of F1's exposure | F7, DV-6 |
| **D5** | **Fix — the one-line `_spaceGate.Cancel()`.** *(As recommended.)* | Unconditional beats an invariant that depends on permit accounting the handler's own trigger case has already broken | F5, S5 |

**The recommendation D2 overrode, kept for the record.** The plan recommended doc-only-now plus a
scoped follow-up, on three grounds: grouping restructures the pump, which P3.1 §1.5 had excluded and
whose S8a/S8b split existed *because* pump changes need isolated attribution; it entangles a latency
change with a correctness phase; and its benefit is a latency claim this phase has no perf gate to
measure. **What the user's choice changes, and what the plan does about each:**

- the pump boundary is now superseded **once, explicitly**, for both breaching items (§8.3) rather
  than twice ad hoc;
- grouping gets its **own L slice**, sequenced after the submission-side slices even though it is
  independent of them (§5.1), so attribution survives;
- the latency benefit is asserted **structurally** (test 11's head-of-line property), and the plan
  explicitly forbids claiming a latency *number* without the deferred measurement (test 21);
- **F6 flips from "record a deviation" to "the deviation is deleted"** (F6(c), DV-3/DV-5) — the
  first draft's §3 entry must not ship as written.

**Two consequences the user's brief called out and this plan answers concretely:**

1. **The pump's node identity is a per-`send_batch` batch object, NOT the accumulator's node**
   (§3B.2). Python hands over the node and *frees* it (`:510`); .NET *recycles* nodes (`_spare`,
   P3.1's allocation work), so transferring one would either kill the recycling or need a return
   channel. The unit is the `send_batch` call rather than the node so that a lowered
   `CONFLUENT_KAFKA_PRODUCER_BATCH_CHUNK` cannot silently re-mix groups (§3B.1).
2. **`DrainCap` survives with a changed job** — array capacity + the §3B.3 sub-pass bound — because
   node capacity is runtime-tunable while `DrainCap` is a const, and a group can therefore exceed the
   arrays. That is the one hazard grouping introduces, it is **not** hypothetical, and it has its own
   test (12).

**Still flagged, not for this phase:** the coarse producer mutex in `src/ffi/producer.rs` (**Mode B**,
§7) — unchanged by D2, since grouping is entirely managed (§3B.7).

### 11.1 The register as it stood before the decisions (kept for auditability)

| # | Question | Options | Recommendation | Trade-off if the recommendation is overridden |
|---|---|---|---|---|
| **D1** | **The F1 fix shape.** | **(a)** routing counter **+ FIFO submission queue with a single appender** (§3.3); **(a-lite)** routing counter only; **(b)** post-append throttle blocking the caller inside `Send`; **(c)** drop the stage-1 bound | **(a).** It is the only option that fixes **both** reorder sources, keeps the public API and the throttle, and does not depend on `SemaphoreSlim` fairness (which .NET explicitly does not guarantee) | **(a-lite)** is ~20 fewer lines but leaves two consecutively parked same-caller sends able to invert — a partial fix to a silent correctness bug, which is worse than none for review purposes. **(b)** is Python's exact sync shape but makes `Send` block a pool thread under saturation, contradicting §A1's managed sync-over-async prohibition and making the shipped design's throughput profile meaningless. **(c)** removes the bound whose Java-faithfulness the anchor argues in prose at `_confluentkafka.c:22-26` — a bigger divergence than the one being fixed |
| **D2** | **F3 scope: narrow the claim, or implement per-node grouping?** | **(i)** doc-only narrowing (S0); **(ii)** doc-only **now** + a scoped follow-up phase for grouping; **(iii)** implement grouping in this phase | **(ii).** The claim is corrected immediately at zero risk, and the latency work is scoped where it can carry its own measurement | **(iii)** restructures the pump, which P3.1 §1.5 excluded and which S8a/S8b deliberately split *because* pump changes need isolated attribution; it would also entangle a latency change with a correctness phase. **(i)** alone is fine but leaves no record that anyone wants grouping |
| **D3** | **F4: fix or record?** | **(i)** bounded pre-stop pump drain (S3); **(ii)** re-state the residual with the widened exposure only | **(i).** It makes the normal case Python-identical, cannot hang (the gate is already closed, so the wait is monotone), and degrades to today's behaviour on expiry | **(ii)** is honest and free, but leaves every close-with-buffered-records able to report a successfully-produced record as failed — and the accumulator is what made that routine, so it is this phase's own exposure to answer |
| **D4** | **F7: re-affirm the sync-path divergence, or re-open it?** | **(i)** re-affirm (no work); **(ii)** re-open — route sync through the accumulator too | **(i).** The quantitative case holds (p50 ≈ 7 ms baseline vs a 0–10 ms window on every sync send), and the sync path is also the only surface free of the buffer-mutation window and of F1's exposure | **(ii)** would be full structural parity with the anchor at the cost of roughly doubling sync send latency, and would need its own phase, its own perf measurement and a public-doc change to the sync surface's mutation contract |
| **D5** | **F5: fix or document?** | **(i)** add `_spaceGate.Cancel()`; **(ii)** state the permit-arithmetic invariant at the site | **(i).** One line, unconditional, and the invariant it replaces depends on accounting that the handler's own trigger case has already broken | **(ii)** costs nothing today but leaves a hang reachable by a future change to permit accounting, with only a comment guarding it |

*(Resolved: D1 = a, **D2 = iii — override**, D3 = i, D4 = i, D5 = i. See the table above.)*

---

## 12 · Brief notes for the Actor / Critic (include verbatim in the spawn prompt)

**Sandbox traps in this environment — every one of these has produced a false result here:**

- **`PATH` is clobbered** (it contains a literal unexpanded `${PATH}`), so `ls`, `head`, `git`, `gh`,
  `cargo`, `dotnet` all fail with "command not found" until fixed. Start **every** Bash call with:
  `export PATH="/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin:/usr/local/bin:$HOME/.cargo/bin:$HOME/.dotnet"`
  A missing tool here is a PATH problem, **never** a "blocked" finding.
- **`cat` is aliased to `bat`** (not installed) — use `/bin/cat`. This also breaks
  `cat > file <<'EOF'` heredocs: use `/bin/cat`, or the Write tool.
- **`sed` is not available.** Use `awk 'NR>=X && NR<=Y' file` for line ranges. Every command also
  prints a harmless `airlock-environment:8: command not found: sed` line — ignore it; it is not your
  command failing.
- **Large reads are truncated** to a persisted-output file — read in ~300-line chunks with `awk`.
- **zsh does not word-split**, and it globs unquoted `--include=*.cs` — quote grep's `--include`.
- **A zero-match libtest / test filter reports success.** Assert a non-zero test count.
- Working directory persists between Bash calls; prefer absolute paths.

**Verification commands for this phase:**

```
cargo build --features ffi                          # must stay clean (Mode A evidence)
cd bindings/dotnet && dotnet test -f net10.0        # baseline to beat: 880 passed / 0 failed
git diff --stat HEAD -- src/ src/ffi/ confluent_kafka.h cbindgen.toml   # must be EMPTY
```

**For the Critic specifically:**

- The anchor is `bindings/python/_confluentkafka.c` + `producer.py`. **Re-verify §1.1's line numbers
  before quoting them** — §1.2 records three cites the incoming audit got wrong by a few lines.
- P3.1 §1.3's two naming collisions still apply: `ffi-marshalling.md` §A7's "Option A/B" is a
  different lettering from the PLAN's "Option A/B/C/D". Do not file a finding built on conflating them.
- **Do not re-file the GIL-related differences in §4** — they are correctly translated.
- **Do not re-file F7 / DV-6** (the sync path) as a parity gap; it is user-directed and D4 re-affirmed
  it on 2026-09-10.
- **Grouping (S3) is a user decision (D2), taken over the plan's recommendation.** Do not re-file the
  plan's own doc-only argument as a finding; §11 records it. What *is* fair game: the §3B.3 oversized-
  group hazard, the §3B.2 nulling order, the `count`-not-`Length` discipline, `DrainedSendCount` still
  counting records, and `DrainAndFaultRemaining` still being uncapped.
- **The residual axes live in exactly one place** (`IDeliveryCallback.cs:141-232`). A paraphrase added
  anywhere else is a finding, and so is a claim at a call site that the axes contradict — that is the
  failure mode P3.1 burned several rounds on.
- ⚠ **A double-free aborts the test host and `dotnet test` still exits 0.** Grep every run for
  `Test Run Aborted`; S3 rewrites the loops where that hazard lives.
- The exception-message trap: a faulted teardown send and an accepted-residual send share an identical
  `ObjectDisposedException` message containing "closed" (`STATUS.md:20`). Any test or finding that
  distinguishes them by message is unsound — use `DrainedSendCount` / `ProcessedBatchCount` /
  `HistoryCount`.
- Every new test must name the mutation that makes it fail, and the Actor must have **run** that
  mutation. A test whose stated mutation does not fail it is not evidence.
