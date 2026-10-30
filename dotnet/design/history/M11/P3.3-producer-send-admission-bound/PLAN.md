# M11/P3.3 — the async producer's send path needs an *admission bound* that throttles the caller

**Status:** **APPROVED 2026-09-11** — all seven open decisions (D1–D7) resolved as recommended;
see §10. Actor/Critic loop authorized.
**Phase:** M11/P3.3 (follow-on fix to M11/P3.2).
**Agent number:** **N = 72** (next free binding N; 71 was M11/P3.2 — verified against
`design/history/**/COMMENTS.DONE.*.md` and the binding-root `COMMENTS.*.md` set).
**Branch:** `prashah_dev_producer_python_alignment`, base `b46472ce` (23 unpushed commits).
**Mode:** A (managed-only; no Rust, no ABI, no new `[DllImport]`) — see §7 for the one
option that would be Mode B.
**Anchor:** `bindings/python/_confluentkafka.c` (`Producer_send` / `Producer_send_thread`);
Java `KafkaProducer.send` / `RecordAccumulator` / `BufferPool`.

---

## 0 · In simple terms

*(Required section. Written for a reader with no C# concurrency background. Everything here
is restated precisely in §1–§7.)*

### The deli counter

Picture a deli. Behind the counter, one worker can hold **1,000 sandwich orders** on the
rack at a time. Customers walk in and hand over an order slip.

**How the reference clients do it.** In Java's deli, if the rack is full the customer
**stands there holding their slip** until a space opens. In Python's deli, same thing — the
customer physically cannot leave the counter, because the act of handing over the slip *is*
the act of standing in line. Either way, the **queue outside the door never grows**, because
a customer who hasn't been served yet is still standing at the counter and therefore cannot
go back outside and send in the next customer.

**What we do today.** Our deli has a clerk at the door. If the rack is full, the clerk says
*"no problem — pop your slip in this basket, we'll get to it"* and the customer **walks away
immediately**. The basket has **no size limit**. So on a busy day the basket fills with
hundreds of thousands of slips. Every slip in it is a sandwich somebody is still waiting
for. Two things go wrong:

1. **Everyone waits far longer.** Your sandwich isn't slow to *make* — it's just that 2
   million slips went into the basket ahead of yours. Measured: the wait went from **41
   milliseconds to 3.5 seconds** (86× worse).
2. **The deli runs out of room.** All those slips have to be stored somewhere. Measured:
   memory went from **239 MB to 2.04 GB** (8.9× worse).

And the throughput actually looks *better* (+8.6%), which is exactly why nobody noticed: the
kitchen is busier than ever. It's just that the line out the door is now enormous.

### The mistake it would be easy to make

The obvious fix is *"put a lid on the basket."* We tested that idea, and **it does not
work.** We ran the benchmark with the basket path switched off entirely, so slips went
straight onto the rack instead. The result was **identical bloat** (p50 3436 ms, 2.04 GB).

Why? Because the basket was never the problem. The problem is that **the clerk never says
"wait."** If you take the basket away, the slips just pile up on the rack instead. Capping
one container simply moves the pile somewhere else.

So the fix isn't a lid on the basket. The fix is that **the clerk has to make the customer
wait at the counter** when the deli is full — which is what Java's and Python's delis do,
and is the one thing ours stopped doing.

### Why we can't just copy Python

In Python, a customer handing in a slip is a **blocking** act: the calling thread physically
stops inside the `send()` call until there's room. That single fact buys two properties for
free — the line is bounded, *and* orders keep their order (you can't hand in slip #2 before
slip #1 comes back).

.NET's `Send()` is deliberately **not** like that. It returns a `Task` — a receipt you redeem
later — so one thread can keep handing in slips without stopping. That is the whole point of
the async design, and it's why we can't transplant Python's mechanism verbatim.

But it's also the trap. Because the caller never waits for *admission*, **any "waiting"
we implement as a background continuation is not waiting at all** — it's just another basket,
with the slips relabelled. A customer who was told "we'll call you" has already left. This is
not a theory: it is the measured failure mode, twice (§4).

### Why a plain lock isn't enough either, and what is

Suppose we do make people wait. We need the *oldest* waiter to go first. The standard .NET
tool here is a `SemaphoreSlim` — a bouncer with a headcount — and its documentation says
plainly there is **no guaranteed order** in which waiting threads get in. Whoever the OS
happens to wake first wins. So a bouncer alone cannot promise first-come-first-served; that's
why M11/P3.2 built an explicit **sign-up sheet** (a FIFO queue with a single person working
down it) rather than resting on the bouncer.

Here is the part that is genuinely nice, and not obvious: **if the customer has to stand at
the counter, the sign-up sheet's ordering guarantee survives the bouncer's unfairness anyway.**
A customer standing at the counter has exactly **one** slip in play. The bouncer can wake
customers in any order it likes and still never reorder *one customer's own* slips — because
that customer cannot produce slip #2 until slip #1 is through. Unfairness can shuffle
*different* customers relative to each other, which is something we never promised (and Java
doesn't either — the guarantee is per-caller).

That is why the recommended fix **keeps M11/P3.2's sign-up sheet exactly as it is** and adds
the "wait at the counter" rule in front of it. The ordering fix stays; the missing bound gets
added; and the ordering property ends up resting on a *stronger* argument than before.

### Summary of the four points this section is required to cover

| | |
|---|---|
| **(a) What breaks today** | The deferred submission basket (`_submissions`) has no size limit, so under sustained load the client accepts an unbounded number of sends. 2 million records in flight; 3.5 s latency; 2 GB. |
| **(b) What the fix does** | Bound the *total* number of accepted-but-unsent records and make exceeding it **actually make the caller wait** (bounded by `max.block.ms`), instead of handing the record to an unbounded container. |
| **(c) Why order is still preserved** | M11/P3.2's FIFO queue + single appender is untouched. On top of that, a blocking admission means one caller has at most one send in flight, so per-caller order cannot invert regardless of which waiter the runtime wakes. |
| **(d) Why we can't copy Python** | Python's `send()` blocks the calling OS thread, which bounds the line for free. .NET's `Send()` returns a `Task` and the caller does not await admission — so any admission wait implemented as a continuation does not throttle anyone. |

---

## 1 · The defect, measured

All three rows measured by me on this machine within ~5 minutes of each other, against the
already-running local broker (`apache/kafka:4.2.0`, container `kafka-perf`,
`localhost:9092`, topic `test-topic` / 6 partitions). Identical run shape throughout:
`make producer-perf-test-dotnet CLIENT_VERSION=3`, `ASYNC=True`, no `LIMIT_RPS`, no
`NUM_MESSAGES` (so: max rate, duration-bounded), `VALUE_SIZE=1024`, `KEY_SIZE=0`,
`WARMUP_SECONDS=3`, `TEST_DURATION_SECONDS=15`, `acks=all`, `batch.size=1 MiB`,
`linger.ms=5`, idempotence off.

| Checkout / config | p50 | p99 | avg RSS | throughput |
|---|---|---|---|---|
| **P3.1 tip** `1d910b01` (before any of P3.2) | **41 ms** | 100 ms | **239,174 KiB** (233 MiB) | 536,864 msg/s |
| **HEAD** `b46472ce` (full P3.2) | **3,524 ms** | 3,696 ms | **2,140,751 KiB** (2.04 GiB) | 583,058 msg/s |
| HEAD + `CONFLUENT_KAFKA_PRODUCER_MAX_ACCUMULATED=4000000` (submission queue bypassed — §2.3) | 3,436 ms | 3,526 ms | 2,139,547 KiB (2.04 GiB) | 604,327 msg/s |

**Deltas, HEAD vs P3.1: p50 ×86, RSS ×8.95, throughput +8.6%.**

Two notes on method, so the numbers are not over-read:

- The P3.1 row was taken by a **detached checkout** of `1d910b01`, then restoring the
  branch. The branch ref was never moved; `git status --porcelain --untracked-files=no` was
  verified identical before and after, and HEAD is back at `b46472ce`. No commits were
  touched.
- These reproduce the orchestrating session's independent bisection (P3.1 ≈ 36 ms / 262 MB /
  453k; HEAD ≈ 3.2–3.4 s / 2.4–2.5 GB / 601k) closely enough to treat the effect as robust,
  not as a single-run artifact. The throughput figures differ by ~8% run to run; the p50 and
  RSS figures do not.

**Attribution.** `git show 01cf014c` confirms slice S1 of M11/P3.2 is what introduced
`_submissions` (`ConcurrentQueue<QueuedSubmission>`) and `SubmitQueued`, and what removed
`NativeProducer.SendWhenSpaceAvailable` — the per-send `await WaitForSpaceAsync` slow path
that preceded it. So the regression is in this phase's own unpushed commits.

---

## 2 · Why it happens

### 2.1 The code, as it stands

`SendAccumulator.cs:418-445` — `SubmitQueued` enqueues **unconditionally**. The only test
before the enqueue is the teardown seal:

```csharp
lock (_gate)
{
    sealedForTeardown = _queueSealed;
    if (!sealedForTeardown)
    {
        Interlocked.Increment(ref _queued);
        _submissions.Enqueue(new QueuedSubmission(record, completion, delivery, cancellationToken));
    }
}
```

There is no depth check anywhere on that path. `_queued` is read by `TrySubmitInline`
(`:356`) and by the idle predicate (`:1031`); it is never compared against a bound.

`TrySubmitInline` (`:351-368`) refuses the inline path whenever `_queued != 0`. That is
S1's ordering fix and it is correct. But it has an emergent consequence: **once one send is
queued, every subsequent send is also queued**, because `_queued` cannot return to 0 while
the producer keeps sending. So under sustained load the queue is not an occasional overflow
path — it is *the* path.

### 2.2 What the 1,000-record bound actually bounds now

`_space = new SemaphoreSlim(settings.MaxAccumulatedRecords, ...)` (`:200`,
`MaxAccumulatedRecords` = 1000 by default, `SendAccumulatorSettings.cs:57`/`:143`). A permit
is taken in `TrySubmitInline`/`AppendQueuedAsync` and released by the batch thread's
`TakeChainLocked`/`ReleaseSpace` (`:1501-1523`).

So `_space` bounds **records sitting in the node chain** — appended but not yet taken by the
batch thread. It does **not** bound **records accepted by `Send` but not yet appended**.
After S1, that second population is the unbounded one, and it is the one that matters:
`Send` has already returned to the caller for every record in it.

The accumulator's own doc comment already describes the bound as *"records appended but not
yet taken by the batch thread"* (`SendAccumulatorSettings.cs:107`) — accurate, and exactly
the gap: nothing counts the records that have not been appended yet.

### 2.3 The defect is the absent bound, not the queue — falsified experimentally

This is the finding that changes the shape of the fix, so it was tested rather than argued.

Setting `CONFLUENT_KAFKA_PRODUCER_MAX_ACCUMULATED=4000000` makes `_space` effectively
infinite. `TryAcquireSpace()` then always succeeds and `_queued` never leaves 0, so
`TrySubmitInline` always wins and **the submission queue is never used at all** — every send
appends inline, straight into the node chain.

Result (row 3 of §1): **p50 3,436 ms, RSS 2.04 GiB** — indistinguishable from HEAD.

**Conclusion: capping `_submissions` while leaving the inline path unbounded would relocate
the pile-up into the node chain and change nothing.** The bound has to cover *both* routes,
i.e. it belongs on `Send`'s admission, above the routing decision — not on one container.

### 2.4 Why "it accepted more" turns into "everything got slower"

The client's own latency did not degrade; the *pipeline* got deeper. Little's law on the
measured numbers:

- HEAD: 583,058 msg/s × 3.524 s = **2,054,700 records in flight**.
- P3.1: 536,864 msg/s × 0.041 s = **22,011 records in flight**.

And the perf harness's own buffer is a **bounded** channel sized from a ~2 GiB notional
budget divided by the record size (`ProducerBenchmark.cs:211-214`):

```csharp
// max 2 GiB of in-flight messages in the queue (bounded → backpressure), matching Python.
int capacity = (int)Math.Min(int.MaxValue, (2L * 1024 * 1024 * 1024) / Math.Max(1, config.MessageSize));
```

At `VALUE_SIZE=1024` that is **2,097,152 records**. HEAD's in-flight population is **98% of
it**: the harness's channel is *saturated*. P3.1's is 1.05% of it: nowhere near.

So the RSS figure is the per-send managed object graph for ~2.1 M outstanding sends (the
payload buffers themselves are pre-built and reused — `messages[messagesSent % messages.Length]`,
`ProducerBenchmark.cs:259` — so this is not payload duplication), and the p50 is dominated by
**queueing delay**, not service time.

### 2.5 ⚠ What P3.1's 41 ms does *not* prove

It would be convenient to say "S1 deleted the backpressure that used to exist." That is too
generous to P3.1, and the plan must not rest on it.

P3.1's slow path was `await WaitForSpaceAsync(...)` inside an `async` method whose `Task` was
returned to the caller. The caller (here, the perf harness) **does not await admission** — it
awaits its own channel write (`ProducerBenchmark.cs:263`) and moves on. So P3.1's admission
wait had no *structural* ability to throttle the caller either; parked continuations could
accumulate without bound in principle. Empirically it self-limited at ~22 k, and **the
mechanism for that is not established.** (A plausible candidate is contention on
`SemaphoreSlim`/`CancellationTokenSource` bookkeeping once the waiter list is large, which
would throttle the producing thread incidentally. Untested, and the plan does not depend on
it.)

Two consequences:

1. **"Restore P3.1's shape" is not a valid target.** It would reopen the merge-blocking
   ordering bug S1 fixed, and rest the perf on an unexplained property.
2. **An async/continuation-based admission wait must not be assumed to throttle.** §4 shows
   it measurably fails to, on a sibling branch, in this same harness.

---

## 3 · What the benchmark measures — and the honest answer to "will a cap show up in it?"

The orchestrating session asked this directly, so here it is explicitly.

**The measured latency includes the harness's own queueing.** `ProducerBenchmark.cs`:

- start stamp is taken **before** `Send` (`:261`);
- the send `Task` is **not** awaited at submission; it is queued into the bounded channel
  with its start stamp (`:262-263`);
- the end stamp is taken in a **single-reader FIFO recorder task** when it dequeues that
  record and its awaited task has returned (`:217-246`, `long latencyMs = Metrics.NowMs() - startMs`).

So one record's measured latency = *(time queued in the harness's channel)* + *(time to
delivery)* + *(recorder head-of-line wait behind earlier records)*. The repo already says so:
`RUNNING-LOCALLY.md:130-131` — *"Latency here is **latency-under-saturation** (deep pipeline),
not per-send cost."* There is exactly one start stamp and one end stamp per record; there is
no client-only latency measurement anywhere in the async path.

**Does that make the benchmark useless for validating the fix? No — and here is why.** The
harness channel saturated *because* the client accepted without bound. It is downstream. The
sequence is: client accepts unboundedly → the loop is never throttled → the loop outruns the
recorder → the channel fills to its 2.1 M cap → every record thereafter waits ~3.5 s in it.
Bound the client's admission and the loop is throttled *before* the channel fills, which is
precisely the P3.1 row's regime (channel at 1% occupancy). So a correct fix **will** move
this number, and the max-rate run is a legitimate acceptance check.

**But it is not a sufficient one, and must not be the only one**, for two reasons:

1. It conflates the client's bound with the harness's buffer. A fix that bounded the client
   at, say, 2 M records would also "pass" while leaving the defect essentially intact.
2. It needs a broker and ~20 s, and the shared-broker perf leg is already known to be
   intermittently flaky in CI (recorded in M13/P2's close-out).

**So the primary gate must be a broker-free, deterministic assertion on the accumulator's own
in-flight population** — see §8.1. The max-rate run is the corroborating measurement, recorded
in the phase record with numbers, not a CI gate.

---

## 4 · Prior art: this exact bug was already found and fixed once, on a sibling branch

This is the most important input to the fix choice, and it is not in this branch's history —
it is on `prashah_dev_dotnet_binding_producer_tuned` (M11/P6 = N 39, M11/P7 = N 40, both
closed 2026-08-20/21, user-approved). Recorded numbers, **not re-measured by me**:

| | throughput | p50 | RSS |
|---|---|---|---|
| **M11/P6** — managed in-flight cap with an **async** acquire (`await _inflight.WaitAsync`, off the produce loop) | 63.5k msg/s | **10,001 ms** | **~3.0 GB** |
| **M11/P7** — same cap, **blocking** acquire (`_inflight.Wait(maxBlockMs, linked.Token)`), cap 5000 | **591.6k msg/s** | **7 ms** | **127 MiB** |
| (reference) ckd 2.15.0 / librdkafka, same run shape | 546.7k msg/s | 24 ms | 233 MB |

The M11/P6 diagnosis, verbatim from that phase's record: *"the perf harness awaits the
**channel write**, not the acquire, so parked `SendAfterWaitAsync` tasks pile up unbounded."*
That is the same sentence one would write about HEAD today.

The M11/P7 rationale, verbatim: *"the blocking `Wait` throttles the un-awaited
`backend.Send()` call itself, so the produce loop pauses on backpressure — Java `send()`
blocking on `buffer.memory` / Python sync `space.result()`."*

**Two further findings from that work bear directly on this plan:**

1. **The cap value matters enormously, and 1000 is the wrong number for .NET.** Measured
   sweep (30 s, same shape): cap 1000 → 96.9k msg/s, p50 9 ms (*"too tight — starves the
   pipeline"*); **cap 5000 → 591.6k msg/s, p50 7 ms, 127 MiB** (the knee); cap 10000 →
   635.2k msg/s but p50 13 ms. The recorded conclusion is worth quoting because it pre-empts
   the natural instinct here: *"M11/P6's .NET cap=1000 was a literal copy of Python's 1000,
   but that parity is SUPERFICIAL — Python is GIL-bound + non-blocking so 1000 doesn't starve
   it; .NET is true-parallel + blocking so 1000 starved it. 5000-for-.NET is a justified
   divergence."* **So reusing `MaxAccumulatedRecords` (1000) as the admission bound is
   specifically contraindicated by measurement.**
2. **A record count is workload-fragile.** *"5000 is optimal for 1KB msgs but would starve
   tiny msgs / blow up large msgs. Java bounds by `buffer.memory` BYTES (32MB); librdkafka by
   `queue.buffering.max.messages` (100k) + `kbytes` (1GB)."* Left as a Stage-2 idea there.

**⚠ Caveat on transferring those numbers.** M11/P7's branch had a *different* send path —
Option C, an inline `Producer_send` per record, with the cap as an extra gate in
`NativeProducer`. This branch defers to a batch thread. The **shape** of the finding
transfers (async acquire cannot throttle; blocking acquire can); the **cap value** must be
re-measured here. I verified this branch has no `_inflight` / `MAX_INFLIGHT_SENDS` cap in
`bindings/dotnet/src/**` — M11/P7's fix is not present on this lineage.

---

## 5 · ⚠ The decision conflict this phase must resolve openly

M11/P3.2's own PLAN §D1 **considered and rejected** the blocking throttle
(`design/history/M11/P3.2-producer-send-ordering-parity/PLAN.md:1277`, option **(b)**
"post-append throttle blocking the caller inside `Send`"), on this stated rationale:

> **(b)** is Python's exact sync shape but makes `Send` block a pool thread under saturation,
> contradicting §A1's managed sync-over-async prohibition and making the shipped design's
> throughput profile meaningless.

Both clauses need examining, because the recommendation in §7 is a variant of (b):

1. *"making the shipped design's throughput profile meaningless"* — **refuted by
   measurement.** M11/P7's blocking gate produced 591.6k msg/s, *higher* than the ckd
   baseline and ~13× M11/P6's async variant. Blocking did not degrade throughput; it was the
   only thing that made the latency and memory tenable at all.
2. *"contradicting §A1's managed sync-over-async prohibition"* — **the citation does not
   hold up.** I read §A1 end to end: it contains no sync-over-async prohibition. The
   prohibition lives in §B7 (*"Wrapping the sync variants … in a `Task.Run` per op"*) and in
   `bindings/dotnet/CLAUDE.md §4` (*"wrapping the sync call in `Task.Run` would be
   sync-over-async"*). "Sync-over-async" means **blocking a thread on the completion of an
   asynchronous operation** (`.Result`, `GetAwaiter().GetResult()`, `Task.Run` + wait).
   `SemaphoreSlim.Wait(timeout, token)` is a genuine synchronous primitive, not an async
   operation being blocked on. CLAUDE.md §4 makes the same distinction explicitly for the
   consumer's sync `Seek`: *"a sync method calling the sync ABI **directly** (no `Task.Run`)
   is legitimate — not the sync-over-async footgun."*

So one clause is measurably false and the other misapplies a rule from a different section.
**This is a genuine decision to re-open with the user, not a rule to design around** — and
it is exactly the pattern worth flagging: a rule cited in an agent-authored decision table
blocked the fix that a sibling branch had already measured as correct. That said, (b) has a
*real* cost which §D1 gestured at and which the user must weigh — see D2 in §10 and the risk
register in §11. The claim here is narrow: the recorded rationale for rejecting it was wrong
on the facts, so the rejection should be re-decided, not inherited.

---

## 6 · Non-goals

- **Do NOT redesign S1.** The routing predicate (`_queued != 0`) and the FIFO queue with a
  single appender stay. They fix a real merge-blocking correctness bug, and this file needed
  15 findings across six Critic passes to stabilize (including a genuine Dekker's-pattern
  memory-model bug at `RunSubmitterAsync`'s start/stop handshake).
- **Do NOT add parallel appenders.** Throughput went *up* under S1, so the single appender is
  demonstrably not the bottleneck. More appenders would reopen the reordering risk.
- **Do NOT revert S1 or any part of M11/P3.2.**
- **Do NOT touch the sync `Send`.** It calls `Producer_send` inline and blocks its own caller
  on the core's `buffer.memory` — it already has real backpressure and no such window.
- **No Rust / ABI / header change** (Mode A), with one exception flagged as Mode B in §7
  (option D).

---

## 7 · Fix options

| | Option | Bounded? | Throttles the caller? | Ordering | Java fidelity | Mode |
|---|---|---|---|---|---|---|
| **A** | **Blocking admission** on the slow path: `Send` stays non-`async`; when the bound is saturated the calling thread waits (bounded by `max.block.ms`), then the record joins the FIFO queue. On expiry: **fire the delivery callback with the -1 placeholder and fault the returned `Task` as retriable** — `Send` does **not** throw (corrected post-72.1; see the note below). | ✅ | ✅ structurally | ✅ preserved, and *strengthened* | ✅ this is Java's shape | A |
| **B** | **Bounded async admission** (the shape originally suggested — e.g. `Channel.CreateBounded` + `WriteAsync`). | ❌ see below | ❌ | ✅ | ✗ | A |
| **C** | **Fail fast** — when saturated, fault the record's `Task` with a retriable `KafkaException` (librdkafka's `QUEUE_FULL`). | ✅ | n/a (rejects instead) | ✅ | ✗ Java blocks, it does not drop | A |
| **D** | **Byte-based bound**, or tie the managed bound to the core's `buffer.memory`. | ✅ | depends on A/B/C | ✅ | ✅✅ closest to Java | **B** |
| **E** | Let the inline path proceed while submissions are queued (i.e. relax S1's routing) and cap only the queue. | ❌ (§2.3) | ❌ | ❌ reopens the bug | ✗ | A |

### Why option B is not expressible here — the one hard constraint

This deserves stating plainly because a bounded, order-preserving channel is otherwise the
right instinct, and `Channel.CreateBounded` is indeed already a proven in-repo pattern (the
perf harness itself uses it).

`SendViaPump` **must not become `async`**. That is load-bearing and documented at
`NativeProducer.cs:543-548`: the precondition throws (`ObjectDisposedException`,
already-canceled token) and a serializer throw raised above it must surface **synchronously**,
not as a faulted `Task` — the same reason `AsyncKafkaProducer.SendValidated` is not `async`.
And the `Task` it returns is the **record's delivery future**, not an admission handle, so
there is no second awaitable for a caller to await at submission time (M11/P3.2's deviation
DV-1).

Given those two facts, a bounded channel admits only two behaviours when full:

- `TryWrite` fails → fall back to an **async** `WriteAsync` continuation. The continuations
  then pile up without bound — the queue is capped but the population is not. This is
  M11/P6's measured failure mode exactly (3.0 GB, p50 10 s), and it would also be the third
  unbounded container in this path rather than the first bounded one.
- Block on the write → that **is** option A, with a channel as the primitive.

So "bounded + ordered admission" collapses onto A. The choice is not *which container*; it is
*what happens to the caller when it's full* — wait (A), or be rejected (C). Nothing else
bounds the population.

One genuinely load-bearing simplification falls out of A, and it is worth stating because it
inverts an expected concern. The worry that a `SemaphoreSlim`'s unfairness would reintroduce a
milder reordering is **sound for the async shape and moot for the blocking shape**: with a
blocking admission, a caller has **at most one** send in flight, so whichever waiter the
runtime wakes, a single caller's own sends cannot invert. Per-caller ordering — all Java
promises (`ProducerConfig.java:274`), and all M11/P3.2 claimed — is preserved by the blocking
itself, on top of the FIFO queue that already preserves it. This is Python's own argument,
and it means A does not need a fair admission primitive.

### Recommendation

**Option A, keeping all of S1 intact, with the cap configurable and its default measured on
this branch.** Concretely:

1. One admission bound over **both** routes (§2.3), applied in `SendAccumulator` — not
   `NativeProducer` — so the test fixture routes through the same decision production does
   (DoD §12; this is the same reasoning that put `TrySubmitInline`/`SubmitQueued` there).
2. Fast path stays `Wait(0)`-shaped and allocation-free (DoD §10 budget unchanged).
3. Slow path **blocks the calling thread**, bounded by `max.block.ms` (read from the config
   dict — a real Kafka key, default 60000, the M11/P7 D3 precedent), cancellable by the
   caller's token and by teardown's gate.

   ⚠ **CORRECTED post-72.1 — this originally read "Timeout → `KafkaException` with an
   asserted message", which was implemented and found to be a Java-fidelity defect.** Java's
   `BufferExhaustedException` extends `TimeoutException` → `RetriableException` →
   `ApiException`, and `KafkaProducer.doSend`'s `catch (ApiException)` **fires the callback
   with the -1 placeholder `RecordMetadata`, records the error, notifies interceptors, and
   returns a failed future** — `send()` itself does **not** throw. So on expiry the binding
   must **fault the returned `Task`** with a **retriable** error and **fire the delivery
   callback**, reusing the existing failure-placeholder machinery in `IDeliveryCallback.cs`
   (⚠ `TopicPartition`'s ctor rejects a negative partition and the placeholder's partition is
   `-1`, so a hand-rolled construction throws *inside* the callback's no-throw swallow
   boundary and makes the callback **silently absent** — the M14/P1 trap). The
   **precondition** throws (disposed producer, already-cancelled token) stay **synchronous**;
   only the expiry case moves from throw to faulted `Task`. This also means
   `bindings/dotnet/CLAUDE.md §4` now *has* an analogue of Java's `catch (ApiException)` row,
   where it previously claimed none existed and called the absence ABI-forced — a Manager-owned
   correction, held for approval.
4. Cap default from a **measured sweep on this branch** (§8.3); override via env var
   (`CONFLUENT_KAFKA_PRODUCER_MAX_ACCUMULATED_*`-style, read once at construction), matching
   the existing `SendAccumulatorSettings` convention and the M11/P7 D2 precedent of "env var,
   not a config-dict key".
5. Teardown: a caller blocked on admission must be released by `Stop`, and by
   `AbandonOnThreadFailure`.

   ⚠ **CORRECTED — the original text of this item was wrong twice over, and the Critic
   established it is worse than first reported.** It read: *"the `_spaceGate` already does
   this for `_space` and the new gate needs the same treatment, **before** the seal/flush,
   or `Stop`'s monotonicity argument (`FlushQueuedSubmissions`, `:1246-1261`) breaks."*
   Both halves fail:
   - **The ordering is wrong, and following it would break teardown.** `Stop` seals the
     queue **first** and cancels the gate **after** (`_queueSealed = true` under `_gate`,
     then `_spaceGate.Cancel()`), precisely so the submission the cancel wakes observes the
     seal and takes the **teardown bypass** instead of faulting. Cancelling before the seal
     inverts that: the woken submission reaches `AppendQueuedAsync`'s
     `catch (ObjectDisposedException) when (_queueSealed)` filter with the filter **false**,
     so it is faulted rather than appended — regressing the M11/P3.2 §F2 behaviour that
     makes a send whose caller already returned reach the core.
   - **The stated reason is wrong.** `Stop`'s monotonicity argument rests on the **seal**
     (checked under `_gate` by `SubmitQueued`, so `_queued` can only decrease), not on the
     gate cancellation — and admission release does not feed back into the queue at all,
     because a released-but-refused caller **throws** rather than re-entering `SubmitQueued`.

   The correct requirement is simply: the admission wait must be woken by teardown on both
   paths, in `Stop` **after** the seal and unconditionally in `AbandonOnThreadFailure` — the
   latter per M11/P3.2 S5's reasoning that a waiter must be released by an **explicit event**,
   never by permit arithmetic the triggering failure may already have corrupted.

6. **Peak in-flight population** (needed to size the sweep in §8.3): it is
   `min(MaxAdmittedRecords, MaxAccumulatedRecords)` plus the teardown bypass — **≈1000 at
   today's defaults**, since `BatchChunk` is far smaller than the accumulator's own cap.
   ⚠ It is **not** `cap + BatchChunk`; that was an Actor claim the Critic refuted. Bounded,
   and structurally the same overshoot `_space` already has.

**Option D is the right long-term answer and should be recorded, not built here.** Java bounds
by bytes; a record count is workload-fragile (§4, finding 2). But it is Mode B and a separate
phase. Note it in the phase record so it is a tracked idea rather than a rediscovery.

---

## 8 · Tests, and the DoD item this phase adds

### 8.1 The new DoD item — a **bounded-in-flight** gate (primary, broker-free, deterministic)

This bug shipped past a full Actor/Critic loop and a green 898-test suite because **nothing in
the DoD exercised sustained over-offer.** That is a structural hole, not an oversight, and it
is worth closing with a structural gate.

Proposed DoD addition, phrased so it generalizes beyond this phase:

> **Bounded-acceptance audit.** For any change to a submission, accumulation or completion
> path, there must be a test that floods the path faster than it drains and asserts the
> population of *accepted-but-not-yet-forwarded* items stays within its documented bound. A
> correctness-only suite cannot see an unbounded queue: every record is still delivered, in
> order, exactly once — the suite passes and the client bloats.

Concretely, the test asserts on the accumulator's **own** counters (`QueuedSubmissionCount`
plus a new accepted-but-unappended witness — a counter is needed because §2.3 proves the
queue's depth alone is not the quantity of interest), under a synthetic flood with the batch
thread held back, through production's own entry points.

Two properties the test must have, both learned at cost in M11/P3.2:

- **The mutation proof must be demonstrated IN-SUITE, not isolated.** In S1, reverting the
  routing predicate failed the test 5/5 isolated while the **full suite passed 5/5** — a green
  DoD gate over a reverted merge-blocking fix. The remedy that worked was K=8 bursts with a
  fresh harness per attempt (6/6 in-suite detection, unchanged suite time). Sensitivity is a
  property of the *regime*; always state which regime a ratio came from.
- **Mutate the fixture and production separately.** A combined mutation cannot tell which one
  the test is actually proving (M11/P3.2 findings 71.13/71.14).

### 8.2 Why the existing perf gate could not have caught this — verified

`PerfV3SmokeTests.cs` is wired into `verify-dotnet` (M13/P2, net10.0-only). Its run shape is
**100 rps, 10 s, p99 ≤ 70 ms, 2048-byte values** (`:47`, `:120`, `:128-139`).

100 rps × 10 s = **1,000 records total**, against a stage-1 bound of **1,000 records**. The
bound never saturates, so `TrySubmitInline` never refuses, so `SubmitQueued` may never be
entered even once. **The gate is structurally blind to this entire class**, and would have
stayed green at any queue depth. Raising `P99_LIMIT_MS` would not have helped; the regime is
wrong, not the threshold.

### 8.3 Corroborating measurement (recorded, not a CI gate)

Re-run the §1 max-rate comparison after the fix and record the numbers in the phase record,
plus a **cap sweep** on this branch (§4 says 1000 starves .NET and ~5000 was the knee on a
different send path — this branch's knee is unknown). Acceptance target: p50 and RSS back to
the P3.1 row's order of magnitude (tens of ms, hundreds of MB) **without** losing the ~583k
msg/s throughput, and with the ordering tests still green.

Whether any of this becomes a CI gate is **open decision D5** — my recommendation is *no*
(shared-broker flakiness, recorded in M13/P2), with the deterministic §8.1 test carrying the
gate instead.

### 8.4 Regression coverage that must stay green

All of M11/P3.2's ordering and teardown tests, unchanged — in particular
`Close_FlushesQueuedSubmissionsToSendBatchInCallOrder`, the deterministic + stress halves of
the call-order test, and the `Flush`-includes-a-queued-send test (the two-stage idle
predicate). If option A lets us *also* argue ordering from the blocking property (§7), that
is an additional argument, never a replacement for those tests.

---

## 9 · Rule amendment (`ffi-marshalling.md` §A1)

S1's landing amended §A1 with the submission-*order* rule. The same section should now carry
the **depth** rule, because its absence is a large part of why this shipped: §A1 told the
Actor the submission path must preserve order and said nothing about it being bounded.

Proposed addition to §A1, alongside the existing "Submission order is call order" bullet:

> **A deferred submission path must be BOUNDED, and the bound must throttle the caller.**
> A bound that is enforced by handing the record to a container — a queue, a channel, a
> continuation parked on a semaphore — bounds the *container*, not the number of records the
> client has accepted. Where `Send` returns the record's delivery `Task` and the caller does
> not await admission (the shape this binding ships — M11/P3.2 DV-1), an asynchronous
> admission wait **does not throttle anyone**: the caller has already been given a receipt and
> moved on. Measured twice, in the same harness: M11/P6's `await _inflight.WaitAsync` gave
> 63.5k msg/s / 3.0 GB / p50 10,001 ms; M11/P3.2's unbounded FIFO queue gave 583k msg/s /
> 2.04 GiB / p50 3,524 ms against 537k / 239 MB / 41 ms immediately before it. The
> synchronous, `max.block.ms`-bounded wait is what throttles (M11/P7: 591.6k msg/s / 127 MiB /
> p50 7 ms), and it is Java's own shape — `KafkaProducer.send()` blocks up to `max.block.ms`
> once the accumulator is full. Note also that capping one container is not sufficient:
> with M11/P3.2's queue path bypassed, the identical bloat reappeared in the node chain
> (M11/P3.3 §2.3). The bound belongs on **admission**, covering every route.
>
> **On expiry, fault the `Task` — do not throw.** The bound's timeout is Java's
> buffer-exhaustion case, and Java does **not** throw from `send()` there:
> `BufferExhaustedException` extends `TimeoutException` → `RetriableException` →
> `ApiException`, and `KafkaProducer.doSend`'s `catch (ApiException)` fires the callback with
> the `-1` placeholder `RecordMetadata` and returns a **failed future**. So the admission
> timeout must fire the delivery callback and fault the returned `Task` with a **retriable**
> error, leaving only the *precondition* throws (disposed, already-cancelled token)
> synchronous. ⚠ Reuse the existing failure-placeholder machinery rather than constructing a
> placeholder: `TopicPartition`'s ctor rejects a negative partition and the placeholder's is
> `-1`, so a hand-rolled construction throws *inside* the callback's no-throw swallow
> boundary and makes the callback **silently absent** — green everywhere, invisible to any
> success-only test (the M14/P1 trap).
>
> ⚠ **A blocking admission does not need a fair primitive — for ORDERING. It is NOT fair for
> STARVATION, and that difference is real.** `SemaphoreSlim`'s documented lack of waiter
> ordering cannot invert a single caller's own sends under a blocking admission (that caller
> has at most one send in flight), so no fairness mechanism is needed to make **ordering**
> hold, and adding one to justify the gate is still wrong. But Java's `BufferPool` maintains a
> genuinely **FIFO-fair** waiter queue while `SemaphoreSlim` can **barge**, so a parked caller
> can be starved into a spurious `max.block.ms` expiry under contention. That is an accepted,
> **documented deviation** (M11/P3.3, finding 72.3), not parity: after the expiry rule above,
> its worst outcome is a retriable failed future plus a delivery callback — exactly what Java
> produces on genuine exhaustion. Record it at the admission site; do not let a comment claim
> fairness parity.

Also worth a one-line correction where §A1's anti-pattern list is cited: M11/P3.2's D1 table
attributes a "managed sync-over-async prohibition" to §A1, which §A1 does not contain (§5).
**D6 is decided: a dated addendum** to the archived P3.2 PLAN, not an in-place edit.

---

## 10 · Decisions — ALL RESOLVED 2026-09-11

**Every decision was approved exactly as recommended.** The recommendation column below is
therefore the decision; nothing was overridden and nothing is left open. The normative
consequences are:

- **D1 = (A)** blocking admission, bounded by `max.block.ms`.
- **D2 = (i)** `Send` blocking the calling thread under saturation is **accepted**, with the
  cap set at the measured knee so it is rare in practice, and the behaviour **documented on
  the async surface**. This is the decision that supersedes M11/P3.2 §D1's rejection of
  option (b) (§5).
- **D3 = (i)** a **new, separately-named bound** with its own env override — **not** a reuse
  of `MaxAccumulatedRecords` — defaulted from a fresh sweep on this branch (§8.3).
- **D4 = (ii)** **two slices**: **S1** = the bound + its tests; **S2** = sweep/measure + the
  §9 rule amendment + the D6 addendum + STATUS close-out.
- **D5 = (i)** **no CI gate.** The deterministic §8.1 bounded-in-flight test is the gate; the
  max-rate run is recorded manually in the phase record.
- **D6 = (ii)** a **dated addendum** to the archived M11/P3.2 PLAN's D1 — **not** an in-place
  edit.
- **D7 = (i)** out of scope this phase; recorded as a follow-up idea.

### The decision table as approved

| # | Decision | Options | My recommendation |
|---|---|---|---|
| **D1** | **Fix shape.** | **(A)** blocking admission, bounded by `max.block.ms`; **(B)** bounded async admission; **(C)** fail fast with `QUEUE_FULL`; **(D)** byte-based / `buffer.memory`-tied | **(A).** (B) is not expressible without making `Send` `async` (§7), and its degenerate form is M11/P6's measured 3.0 GB failure. (C) diverges from Java, which blocks rather than drops. (D) is the right long-term answer but is Mode B — record it, don't build it here. |
| **D2** | **Accept that `Send` blocks the calling thread under saturation?** This is the real cost of (A), it is what M11/P3.2's D1 objected to, and it is user-visible: an `async` API whose method parks a thread under load is surprising in .NET, and on a thread-pool caller it is a pool-starvation hazard. Java and Python both do exactly this. | **(i)** accept (Java/Python-faithful, M11/P7 precedent); **(ii)** reject and take (C) instead; **(iii)** accept only above a generous cap so blocking is rare in practice | **(i)**, with the cap set at the measured knee so blocking is rare, and the behaviour documented on the async surface. But this is a contract decision and is **yours**, not mine. |
| **D3** | **The cap: which quantity, and what value?** Note §4's measurement specifically contraindicates reusing `MaxAccumulatedRecords` (1000) — it starved .NET to 96.9k msg/s on the sibling branch. | **(i)** new separate bound, default from a sweep on this branch; **(ii)** reuse `MaxAccumulatedRecords` (1000); **(iii)** reuse it but raise its default | **(i)** — a separate, separately-named bound with its own env override, defaulted from §8.3's sweep. The two bounds answer different questions (records un-appended vs records un-taken) and coupling them is what made Python's 1000 look transferable when it wasn't. |
| **D4** | **Slice count.** | **(i)** one slice (bound + tests + docs); **(ii)** two (S1 bound + tests, S2 sweep/measure + rule amendment + STATUS); **(iii)** three (add a separate docs/rule slice) | **(ii).** The bound is small and cohesive; the measurement and the §9 rule amendment are separable and shouldn't gate the correctness fix. |
| **D5** | **Should the max-rate perf check become a CI gate?** | **(i)** no — deterministic §8.1 test is the gate, max-rate is recorded manually; **(ii)** yes, add a saturating in-suite smoke with p99 + RSS budgets; **(iii)** yes but manual/nightly only | **(i).** The shared-broker perf leg is already intermittently flaky (M13/P2 close-out), and a flaky gate on a phase like this is worse than none. The §8.1 test catches the class deterministically. |
| **D6** | **Correcting the archived M11/P3.2 PLAN's D1 rationale** (§5 — one clause measurably false, one misciting §A1). Archived phase records are normally immutable. | **(i)** leave it; record the correction only in this phase; **(ii)** add a dated addendum to the P3.2 PLAN; **(iii)** edit in place | **(ii).** An addendum preserves the archive's integrity while stopping the false rationale from being inherited by a third phase. |
| **D7** | **Does this phase also re-verify the harness-vs-client attribution** (§3) by adding a client-only latency measurement to the perf harness? | **(i)** no (out of scope); **(ii)** yes, add a second timestamp pair | **(i)** for this phase — it is a harness change with its own review surface, and §2.3/§2.4 already settle the attribution well enough to act. Worth recording as a follow-up idea. |

---

## 11 · Risks

1. **Thread-pool starvation** under option A if a caller sends from pool threads and the cap
   is too tight. Mitigated by the cap sweep (D3) and by `max.block.ms`. This is the risk D2
   is really about, and it is reference-faithful rather than novel — Java blocks the app
   thread, Python blocks the OS thread.
2. **Teardown deadlock** if a caller blocked on admission is not woken by `Stop`. The
   existing `_spaceGate` pattern and `Stop`'s ordering (seal → cancel → flush → close →
   sweep → join) is the template; a new gate that isn't wired into it would hang `Dispose`.
   This needs its own test, per M11/P3.2's S4/S5 experience.
3. **Cap-value regression risk**: too tight starves throughput (measured: 1000 → 96.9k
   msg/s), too loose does nothing. The default must come from a measurement on *this* branch,
   not from M11/P7's number.
4. **Suite churn**: on the sibling branch, blocking admission made ~half the cap tests hang
   because they assert `parked.IsCompleted == false` and drive `Send` inline. This branch has
   no such tests (no `_inflight` here), but any *new* tests must be written to the blocking
   contract from the start.
5. **This file is subtle.** `SendAccumulator.cs` is 2,067 lines with a Dekker's-pattern
   handshake, a two-stage idle predicate, an `_inFlight` publication invariant, and permit
   accounting that three separate comments warn about. The Actor brief must say: add the
   bound, change nothing else.

---

## 12 · Environment notes for the Actor/Critic briefs

Carry these verbatim; several agents have independently re-derived them at real cost.

- **`PATH` is clobbered** with a literal `${PATH}` and every command prints
  `sed: command not found`. Start **every** Bash call with
  `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"`.
  `command -v` is not reliable here.
- **`sed` does not exist** (use `awk`); **`grep` is aliased to `ugrep`** (use `/usr/bin/grep`
  for anything relied on as evidence); **`cat` is shadowed** by a missing `bat` alias (use
  `/bin/cat`); this is **zsh**, so unquoted `$var` is not word-split and unquoted globs abort
  the command.
- **An ABORTED `dotnet test` exits 0.** `Test Run Aborted` in the output is the only signal —
  grep it explicitly, with a control-positive `Passed:` count. A double-free has printed
  `Passed!` while running 41 of 892 tests.
- **`dotnet build -c Release` + `dotnet test --no-build` runs the DEBUG binaries**, and a
  *failed* build + `--no-build` prints a bogus `Passed!` off the stale binary. Assert
  `0 Error(s)` before trusting any mutation/injection result.
- **A libtest/xUnit filter matching zero tests exits 0.** Always confirm the expected test
  **count**, never the exit code.
- `dotnet` is not on `PATH`: `export DOTNET_ROOT="$HOME/.dotnet"; export PATH="$HOME/.dotnet:$PATH"`.
  net8.0 and net10.0 are both executable locally; net462 is build-only.
- Perf runs: `bindings/dotnet/tests/Performance/RUNNING-LOCALLY.md`. Booleans must be the
  literal `True`/`False` — `ASYNC=true` silently reads as false. The broker container
  `kafka-perf` is currently up on `localhost:9092`.
- **Do not push, do not touch `master`, do not open or modify PR #188.** Commit only.
