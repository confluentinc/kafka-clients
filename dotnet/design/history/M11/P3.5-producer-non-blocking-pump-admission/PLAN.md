# M11/P3.5 — Async producer: non-blocking (two-stage) pump admission + perf-harness bounded runs

**Status:** **APPROVED by the user on 2026-10-06**, with these inputs:
- **D1 = C** (`ValueTask<Task<RecordMetadata>>`: `await` → accepted, `await await` → delivered). D9 is moot; `RecordMetadataResult` stays excluded.
- **D2 = (c)** — *"the token lets the caller stop waiting, but the record still goes out."* This **differs from the recommendation (a)**; §2.2, §4, §8, §9 and §14 are revised for it (marked "D2 (c)").
- **D8: the §11 wording is approved and permission to edit those files is granted.** Where D2 (c) forces a minimal change to an approved row, §11 lists it (and the close-out report repeats it verbatim).
- **D10: defer** (this also answers Q2: later). D3–D7 stand as recommended.
- **Q1:** work directly on `prashah_dev_dotnet_binding`; commit, **do not push**. **Q3:** yes — include the optional PerfV2 (`CLIENT_VERSION=2`) reference runs for R1 and R5 in the perf hand-off (the user runs perf, §12).

The loop runs per §13.
**Agent number:** **N = 91** (dotnet numbering). Confirmed four ways:
- `design/current/STATUS.md:55` says "Next unused dotnet N = 91".
- The PM numbering memory says the same.
- No `COMMENTS.91.md` or `COMMENTS.DONE.91.md` exists at the binding root.
- `git grep` finds no `N=91` / `COMMENTS*.91` under `dotnet/`, and neither post-N=90 commit (`e720ae79`, `104b0b2c`) claims an N.

**Base:** `prashah_dev_dotnet_binding` @ `104b0b2c`. **Mode A:** C# only, with no core or C-ABI change. **Pair:** `dotnet-actor` / `dotnet-critic`.
**Sources ported** (all unpushed POC commits; none is an ancestor of the base):

| Commit | Branch | Content |
|---|---|---|
| `2c9cc0b8` | `prashah_dev_dotnet_binding_send_lanes_async_admission` | Pump-path async admission |
| `c67f3013` | `prashah_dev_dotnet_binding_send_lanes_poc` | The two-stage `ValueTask` API, which came with the lanes; only the API and caller parts are ported |
| `1874c9ad` | lanes POC | `AWAIT_ACCEPTED` harness switch |
| `f4cd3a57` | `prashah_dev_dotnet_binding_send_approach_2_poc` | `LIMIT_RPS_SLICE_MS`; still uses the old `bindings/dotnet/` paths |

---

## 0. The requirement (verbatim) and scope

> 1. For async producer non-blocking pump i.e. Make the pump path behave like Python's async send: it appends,
>    then suspends stage 1 until a permit is free; it never parks the caller's thread.
> 2. Add functionality to our perf harness to have bounded (non-spike) runs with LIMIT_RPS_SLICE_MS and
>    LIMIT_RPS. Basically the changes that we added it recently in prashah_dev_dotnet_binding_send_approach_2_poc.
>    Also, add the AWAIT_ACCEPTED change which we added in prashah_dev_dotnet_binding_send_lanes_poc

**In scope:**
- The async `IAsyncProducer.Send`, both overloads, on `AsyncKafkaProducer` and `AsyncMockProducer`. Both reach `NativeProducer.SendViaPump` → `SendAccumulator.SubmitAdmitted`.
- Every caller of those overloads in the repo (§3).
- The perf harness: `LIMIT_RPS_SLICE_MS` and `AWAIT_ACCEPTED`.
- The xmldoc, rule and doc text that describes the async send's contract.

**Out of scope:**
- The lanes POC send paths (lanes / direct), its lane ABI, `CONFLUENT_KAFKA_PRODUCER_ASYNC_SEND_PATH` and the lane cap. Main has only the pump, and nothing here needs a selector.
- The **sync** `IProducer` / `KafkaProducer` / `MockProducer`. They hand the record to the core inside the call and never touch the pump, so they are unchanged.
- A bytes-based cap (§7).
- The p50 gap against lanes at 50k msg/s (lanes p50 9 ms vs pump p50 15 ms). It is noted here only.

## 1. Why this is an API change (the crux)

**Main today** (`SendAccumulator.cs:350-379`):
1. `Send` appends the record (append-first, M11/P3.4).
2. Then `_admission.Wait(_spaceGate.Token)` **blocks the caller's thread** until a permit frees.
3. Then it returns the record's *delivery* `Task<RecordMetadata>`.

The block is the throttle. **Removing the block while keeping the single-stage `Task` return removes the throttle.** A caller already holds its receipt, so there is nothing left for it to await before moving on. This has been measured twice, and §A1 records both:

| Measurement | Throughput | Memory | p50 |
|---|---|---|---|
| M11/P6 `await _inflight.WaitAsync` | 63.5k msg/s | 3.0 GB | 10,001 ms |
| M11/P3.2 queue | — | 2.04 GiB | 3,524 ms |

So "never parks the caller's thread" **and** "still bounded" together *require* an awaitable admission stage ahead of the delivery task. That is exactly Python's shape: `async def send(...) -> Future` (`python/producer.py:654-705`). The coroutine is the admission stage and the returned `Future` is the delivery stage.

Breaking the public API is acceptable before publishing (`dotnet/CLAUDE.md` §3: "acceptable pre-publish"). It still goes to the user as decision **D1**.

## 2. Decisions

These were **the decisions the user had to approve or confirm before any spawn**; the "Status" column now records the user's 2026-10-06 ruling. "User-decided" means the root POC memory or an earlier phase ruling already recorded the user's answer.

| # | Decision | Status | Recommendation |
|---|---|---|---|
| **D1** | Public return type of `IAsyncProducer.Send` (both overloads) | **User-approved: C** (API break accepted) | **C: `ValueTask<Task<RecordMetadata>>`** (§2.1) |
| **D2** | What the caller's token does while stage 1 is pending | **User-approved: (c)** — differs from the recommendation | ~~(a)~~ → **(c)**: the token ends stage 1 with an OCE carrying that token; the record is still sent, its callback fires once, its delivery task is cancelled (§2.2) |
| D3 | Teardown completes a pending stage 1 *successfully* | **User-approved** (as recommended) | Keep. The record is already in the chain, and teardown settles it through the delivery task |
| D4 | An already-cancelled token, a null record, a disposed producer or a serializer throw all throw **synchronously** | **User-approved** (as recommended) | Keep, and add a strict synchronous-throw guard test (§9 T8) |
| D5 | No user continuation ever runs on the send-batch thread or the pump thread | **User-approved** (as recommended); under D2 (c) also: never inline on the thread that cancels the token | Keep (POC: `ContinueWith(..., TaskScheduler.Default)`, and the fast path completes on the caller) |
| D6 | Callers that do **not** await stage 1 are **not throttled** | **User-approved** (as recommended) | Keep, and document it on the public surface (§8) |
| D7 | Cap: count-based `MaxAdmittedRecords = 1000`, env `CONFLUENT_KAFKA_PRODUCER_MAX_ADMITTED` | **User-approved** (as recommended) | Keep. A bytes-based cap is a follow-up (§7) |
| D8 | Rule and doc edits to `dotnet/CLAUDE.md`, `ffi-marshalling.md` and the soak README | **User-approved: the §11 wording + permission to edit** | The Actor applies §11 **verbatim** in S5, plus only the minimal D2 (c) adaptations listed in §11. No other rule edits |
| D9 | If D1 = B: keep `RecordMetadataResult.Get()` / `Get(TimeSpan)`? | **Moot** (D1 = C) | — |
| D10 | Shrink the per-waiting-send allocation (about 1.5 KB) by using a non-cancellable `WaitAsync()` plus a teardown `Release(trackedWaiters)` | **User: defer** | **Defer.** It is not in the measured POC shape and changes the teardown wake. It can be done later without an API change |

### 2.1 D1 — the API shape

| Option | Shape | Verdict |
|---|---|---|
| A | Keep `Task<RecordMetadata>` and make the internal wait async | **Reject.** It throttles nobody. P3.3 rejected it after two measurements (§1) |
| B | `ValueTask<RecordMetadataResult>` (POC as-is = B1; B2 = trimmed, without `Get`) | Viable |
| **C** | **`ValueTask<Task<RecordMetadata>>`** | **Recommended** |
| D | `Task<Task<RecordMetadata>>` | **Reject.** It allocates an extra `Task` on every send, including the fast path (DoD §10) |
| E | Additive: keep `Send` and add a new staged method | **Reject.** Java has one `send`, and two contracts would coexist on one interface |
| F | A custom awaitable type | **Reject.** It is over-engineered compared with B and C |

**Why C over B.** At runtime B and C are identical to the measured POC. Both use `Wait(0)` on the fast path with zero extra allocation, and `WaitAsync` + `ContinueWith` on the slow path.
- **It is the literal composition of two existing idiom-map rows.** Java's `send` *blocks* (row :481 says async, so the outer `ValueTask`) *and* returns `Future<RecordMetadata>` (row :480 says `Task<RecordMetadata>`). The user-authored row :480 (`42583351`, Pranav Shah) stays true verbatim. Under B it would have to change.
- **It adds no public type.** There is nothing to justify under DoD §7. There is no default-struct trap: `default(RecordMetadataResult)` throws `InvalidOperationException`. There is also no `GetAwaiter` shape-parity exclusion (`PublicAdminLogDirsShapeParityTests`).
- **It is Python's shape exactly**: the `send` coroutine yields a native `Future`. It is also the harness's own internal shape (`IAsyncProducerBackend.Send` in the POC is `ValueTask<Task<PerfRecordMetadata>>`).
- **The inner value is a plain `Task<RecordMetadata>`.** It composes with `Task.WhenAll`, `ContinueWith`, `WaitAsync` and other combinators without `.AsTask()`.
- **It still honours the user's lanes-POC decision "ValueTask"** (root memory). Only the custom struct goes.
- *Costs of C:* a nested generic that looks unusual, and the familiar `Task<Task>` footgun. The statement-level footgun below is **identical under B and C**.

**Shape under C:**
```csharp
ValueTask<Task<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default);
ValueTask<Task<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, IDeliveryCallback callback,
    CancellationToken cancellationToken = default);
// await producer.Send(r)        -> Task<RecordMetadata>   (accepted)
// await await producer.Send(r)  -> RecordMetadata         (delivered)
```

**The migration hazard that the compiler will not catch.** A statement `await producer.Send(r);` still compiles and silently changes meaning: it now waits for acceptance only. `var md = await producer.Send(r); md.Offset` *is* a compile error, so the common read is caught. Every repo caller is inventoried in §3. The xmldoc carries the two-line example in §8.

**S3 code sketch under C** (the POC's `SubmitAdmitted`, adapted):
```csharp
if (_admission.Wait(0)) return new ValueTask<Task<RecordMetadata>>(completion.Task);          // fast path, 0 alloc
try {
    Task wait = _admission.WaitAsync(_spaceGate.Token);
    if (wait.IsCompleted) return new ValueTask<Task<RecordMetadata>>(completion.Task);
    return new ValueTask<Task<RecordMetadata>>(wait.ContinueWith(
        static (_, s) => (Task<RecordMetadata>)s!, completion.Task,
        CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default));
} catch (Exception) { return new ValueTask<Task<RecordMetadata>>(completion.Task); }   // see §4 "post-append no-throw"
```

### 2.2 D2 — cancellation while stage 1 is pending

| Option | Semantics | Accounting | Verdict |
|---|---|---|---|
| **(a)** POC; P3.4 D2's ruling, carried over | The token does **not** end stage 1. It still cancels the delivery task through the existing registration, so stage 1 later yields an already-cancelled delivery task | Exact | **Recommended** |
| (b) | Link the caller's token into `WaitAsync` | A cancelled `WaitAsync` takes **no** permit, but the take still releases one for that record, so the cap rises by 1 permanently on every cancel. P3.4 raised `_admission`'s ceiling to `int.MaxValue`, so nothing reports the drift | **Reject** |
| (c) Python-faithful | The stage-1 awaitable is a TCS cancelled by the caller's token (OCE carrying that token), while permit acquisition carries on in the background. The record is still sent, the callback still fires, and the delivery task is cancelled (and unreachable, like Python's orphaned `ret`) | Exact | Viable |

Trade-offs between (a) and (c):
- **(a)** keeps the throttle **inescapable** and matches the user's existing ruling on this same wait (P3.4 PLAN :24-25, D2 :49-67; root POC memory). Its cost is that `await producer.Send(r, ct)` does not respond to `ct` until capacity frees or the producer is torn down. Only Close or Dispose releases it early. This is the same as main today, where the caller's thread is parked and ignores `ct`.
- **(c)** is responsive, and it matches Python, where `await space` raises `CancelledError` while the record still sends (`producer.py:697-705`). Its costs:
  - It costs one TCS and one registration, on the saturated, cancellable path only.
  - Cancellation becomes an **escape hatch from the throttle**: a per-send short-timeout token behaves like not awaiting (D6).
  - The `ReleaseAdmission` and Stop paths stay untouched.
- Java's `InterruptException` means *not appended, no callback* (`KafkaProducer.java:1062-1065`, :953). Append-first cannot reproduce that, so neither option is Java-identical.

**User ruling (2026-10-06): (c).** *"The token lets the caller stop waiting, but the record still goes out."* This supersedes the (a) recommendation above and P3.4 D2's ruling **for the stage-1 awaitable** (the admission wait itself is still never linked to the token — that is the rejected (b)). The Python anchor is `python/producer.py`'s async `send`: append via `_lib.Producer_send`, then `await space`; a cancelled task raises `CancelledError` and the orphaned `ret` is never returned.

**D2 (c) design notes for the S3 Actor** (guidance; the Actor and Critic own the final code):
- **Fast path unchanged**: `_admission.Wait(0)` → `new ValueTask<Task<RecordMetadata>>(completion.Task)`, zero extra allocation. The `wait.IsCompleted` short-circuit is unchanged.
- **No cancellable token (`!ct.CanBeCanceled`) → exactly the POC slow path** (`ContinueWith(..., TaskScheduler.Default)`), no extra cost. The (c) machinery (one TCS + one registration) exists **only** when the bound is saturated **and** a cancellable token was passed. Use a small state object, not closures, so that path's per-send allocation is minimal; measure it and report it with its definition.
- The stage-1 TCS is built with **`RunContinuationsAsynchronously`**: the token callback runs on whichever thread calls `Cancel()` (or the timer), and D5 must still hold — no awaiter continuation inline on the canceller, the batch thread or the pump.
- **Token wins** → `TrySetCanceled(callerToken)` (the OCE must carry the caller's token). **Slot wins** → `TrySetResult(completion.Task)`. Exactly one winner, never both, never neither; either outcome means "the record is sent".
- The underlying `WaitAsync(_spaceGate.Token)` **continues** after a token win and takes its permit as usual, so the bound accounting is exact (T4). Its continuation **disposes the registration in every outcome** (slot, teardown gate cancel); otherwise registrations accumulate on long-lived tokens such as an app-shutdown token.
- The existing delivery-task token registration still cancels the delivery task, so after a token win the delivery task is cancelled and unreachable (like Python's orphaned `ret`), while the record is still sent and its delivery callback fires exactly once.
- **Post-append no-throw invariant** (§4) still holds: after `Submit` appended, `SubmitAdmitted` / `SendViaPump` must not throw (e.g. `Register` on a disposed source on net462). Keep the catch-all → return accepted.
- **D4 still holds and creates a documented ambiguity**: an *already-cancelled* token throws OCE **synchronously before the append** (= not sent), whereas a token that fires while stage 1 is pending yields OCE **after** the append (= still sent). Inside `await producer.Send(r, ct)` both look alike.
- **D3 still holds**: teardown completes a still-pending stage 1 successfully; if the token already ended it, the later gate-cancel continuation is a no-op apart from disposing the registration.

## 3. Caller inventory (Explore sweep at `104b0b2c`; 52 direct async-`Send` sites + 4 `SendViaPump` callers)

| Cat | Meaning | Sites | Action (S2) |
|---|---|---|---|
| A | Compile break | 5 | Migrate |
| B | Delivery `Task` stored or awaited later | 26 | `Task<RecordMetadata> t = await producer.Send(..)` in tests where stage 1 matters, otherwise `.Delivery()` (`ProducerSendStages`) |
| C | **Silent semantic change** (compiles, meaning changes) | 2: `PublicProducerMockControlTests.cs:108,122` (`await TestTimeout.Run(async () => await producer.Send(..))` then `HistoryCount`) | Await both stages. **Critic-checked** |
| D | Inside assertion lambdas | 8 | Migrate per site |
| E | Other | 11 | See the rows below |

Category E sites:
- `_ = producer.Send` discards: `PublicProducerSendTests.cs:370,383,445,477,571`, `PublicProducerAccumulatorTeardownTests.cs:403,427,437`, `PublicProducerMockControlTests.cs:167`, `PublicProducerDeliveryCallbackTests.cs:959` (`SendInline`).
- `PublicProducerTypedSendTests.cs:130` binds to `TestTimeout.Run(Action)` and becomes a compile error.

Other callers and helpers:
- **Reflection:** `PublicProducerDeliveryCallbackTests.cs:632` asserts `typeof(Task<RecordMetadata>)`. It fails at **runtime**; update it to `ValueTask<Task<RecordMetadata>>` for both overloads.
- **Guards that depend on a synchronous throw (keep them; they become D4 guards):** `PublicProducerSendClosedCheckTests.cs:96,109,135` (not in the POC's file list), `PublicProducerTypedSendTests.cs:226`, `PublicProducerAccumulatorTeardownTests.cs:443` (`Func<object>`).
- **Helpers:**
  - `SendOf` / `WithTimeout` (SendTests)
  - `Flavor.Send` / `SendInline` (DeliveryCallbackTests :905, :953-959)
  - `Fire` / `FireAndMeasure` (AccumulatorTeardown :447, FlushDrain :131, both AllocationBudget files)
  - `SendThrowawayBuffersAsync` (PinLifetime :353)
- **Non-test callers:**
  - `grpc-server/AsyncProducerServiceImpl.cs:182-189`: await both stages inside the existing 120 s bound (POC `Delivered` helper).
  - `soak/SoakClient/SoakClient.cs:894` (ContinueWith :936, OnDelivery :947): **must await stage 1**, otherwise the soak becomes unthrottled.
  - Harness: `V3Backends.cs:61` → `Backends.cs:92` → `ProducerBenchmark.cs:185` (warmup awaits both stages) and `:262` (loop; default awaits acceptance).
- **No change needed:**
  - Tests: PublicProducerMetrics, Peripheral, Teardown, TfmSmoke (non-send), Record, PublicSyncProducerTeardown, SendCompletionPumpPreStopDrain.
  - `grpc-server/Program.cs`, `soak/Program.cs`, PerfV2/V3 `ProducerMain.cs`.
  - The sync `IProducer.cs`, `KafkaProducer.cs`, `MockProducer.cs`.
- **Analyzer note.** CA2012 is not configured anywhere; `Directory.Build.props` sets `TreatWarningsAsErrors` + `AnalysisLevel latest`. The POC built clean with the same settings. **Fix each analyzer finding at its site; do not suppress.**

## 4. Stage-1 semantics (S3)

| Situation | Stage 1 | Delivery task |
|---|---|---|
| A permit is free (`Wait(0)`) | Completes **synchronously**, with 0 extra allocation | As today |
| Bound saturated | Pending until a permit frees, with **FIFO hand-off** (.NET Core `SemaphoreSlim` hands permits to async waiters in order under its lock — an implementation detail, **not** relied on for ordering) | As today |
| A permit frees | Completes on a **pool** thread: `TaskNode` runs continuations asynchronously, plus `ContinueWith(TaskScheduler.Default)`. Never on the batch thread | — |
| The caller's token fires while stage 1 is pending | **D2 (c), user-approved:** ends promptly with `OperationCanceledException` carrying that token, on a pool thread (never inline on the canceller). The record is **still sent**; the admission wait continues underneath and takes its permit; its registration is disposed when the wait completes | Cancelled (unreachable after a stage-1 cancel); the delivery callback still fires exactly once |
| Flush | Every pending stage 1 completes once its chain drains: after a full drain the available permits are `cap − 0 + W`. Completions are **pool-queued, so they may land just after Flush returns**; tests assert eventual completion | Settled by the drain |
| Close / Dispose / DisposeAsync / batch-thread failure | `_spaceGate.Cancel()` (Stop step 1, or `AbandonOnThreadFailure`) **completes stage 1 successfully** (D3). The surplus-permit note at `SendAccumulator.cs:375-378` still holds, because the accumulator is closed by the same events | Sent by the final drain, or faulted on thread failure |
| An already-cancelled token, null record, disposed producer, or serializer throw | **Thrown synchronously** before the append (D4); nothing is appended | — |
| A stage 1 the caller drops | Never faults, so it never raises `UnobservedTaskException` | As today |

- **Ordering** comes from **append order alone**: the append is synchronous, under `_gate`, before any wait. A non-awaiting caller's successive sends are therefore appended in call order. The asynchronous wait cannot reorder anything (§A1's per-caller ordering rule still holds).
- **Post-append no-throw invariant.** Once `Submit` has appended, `SubmitAdmitted` / `SendViaPump` **must not throw**. The record *will* be sent and its delivery callback *will* fire, so a throw would contradict `IDeliveryCallback`'s rule that "a throw out of `Send` fires nothing". That is why the POC's slow path ends with `catch (Exception) → return accepted`.
  - The remaining cost is OOM only: a permit that is never taken drifts the bound by +1. Record it at the site as an accepted residual.
  - **Never** let the method become `async`. That would move the D4 throws into the `ValueTask`. The `NativeProducer.cs` "MUST NOT become async" comment (≈:570) stays, re-justified.
- **Buffer borrow (§A4).** Stage-1 completion does **not** end the borrow of the key and value buffers. A permit is released when an *earlier* chain is taken, and the fast path completes while the record is still in the chain. A buffer may be reused only once the **delivery task** completes. This must be stated on the public surface (§8), because in the lanes POC "accepted" meant "copied".
- **§A6 at-most-once residual walk.** S3 changes nothing after ABI acceptance, so no change to the residual set is expected. The Actor still **walks** every faulting or throwing site from append to callback (§A6 method) and records the result in the S3 commit message. `IDeliveryCallback`'s canonical enumeration is edited **only** if the walk finds a change, and only there (round-5 single-source rule).

## 5. Delivery-callback ordering

Unchanged by this phase. The callback still fires on the pump thread **before** the delivery task's awaiter is released (`ProducerBatch.java:303-323`; ffi §A6 form C), unconditionally, exactly once per record. Two new facts must appear in the docs:
- The callback, and the delivery task's completion, are **unordered relative to stage 1**. Stage 2 can complete *before* stage 1 does: X's chain is taken (which releases permits), sent and completed while X's own stage-1 continuation is still queued.
- The callback overload's stage 1 follows exactly the same rules as the plain overload's.

## 6. `max.block.ms`

- P3.4 removed the binding-side admission timeout (user-approved), and append-first leaves nothing to expire at admission. So **stage 1 has no deadline and never fails**.
- Java's buffer-exhaustion outcome (`BufferPool.java:107`, `:161-162` `BufferExhaustedException` → `catch (ApiException)` → callback with the -1 placeholder plus a failed future, `KafkaProducer.java:1049-1061`) still comes from the **core**, inside `send_batch`, as a per-record failure. That is a faulted delivery task plus a callback, which is the Java-faithful result.
- Stage-1 latency is bounded only by batch-thread progress. That is unchanged since P3.4 and is now visible as a pending `ValueTask` rather than a parked thread. *Not re-verified this phase:* whether the core's `send_batch` waits up to `max.block.ms` **per record**, which would make a broker-outage stage-1 wait very long. This is risk R8.
- §A1's "On expiry, fault the `Task`", spurious-expiry and fairness text has been stale since P3.4 and is superseded in §11.

## 7. The cap

- `SendAccumulatorSettings.cs:115` `DefaultMaxAdmittedRecords = 1000`, with the env override at `:122`, both unchanged. The value comes from the P3.3 sweep: a cap of 500 starved throughput to 43.6k msg/s, and 1000 was the best at 1 KiB.
- **Bound arithmetic.** Let P be the appended-but-not-taken records and W the pending stage-1 waits. Then P ≤ cap + W.
  - Awaiting callers have at most one pending stage 1 each, so W ≤ the number of concurrent callers.
  - Peak in-binding records ≤ 2 × (cap + W), because the take precedes `send_batch`.
  - For non-awaiting callers, W is **unbounded** (D6).
- Large values pin up to cap × value size. That is pre-existing, and a bytes-based cap is a follow-up.

## 8. The unbounded non-awaiting risk: draft xmldoc for `IAsyncProducer`, S3

These paragraphs replace type-remarks paragraphs `:62-92`. **D2 = (c) was approved**, so the draft's "no timeout and never fails" and cancellation paragraphs are replaced by the **D2 (c) text** that follows the block; the block itself is kept as drafted for the record.

```
/// ⚠ <b><c>Send</c> returns in two stages, and awaiting the first is what throttles you (M11/P3.5).</b>
/// <c>Send</c> appends the record to the producer's batch chain and returns a <see cref="ValueTask{TResult}"/>
/// that completes when the record is <b>accepted</b> under the producer's bound on records not yet handed to its
/// send-batch thread. Its result is the record's <b>delivery</b> <see cref="Task{TResult}"/> — Java's
/// <c>Future&lt;RecordMetadata&gt;</c>. The first stage is usually complete when <c>Send</c> returns; once the bound
/// is reached it stays pending until capacity frees. It never parks the calling thread. This is Java's blocking
/// <c>send()</c> on an async surface, in the Python binding's async <c>send</c> order: append, then wait for space.
///   Task&lt;RecordMetadata&gt; delivery = await producer.Send(record);   // accepted
///   RecordMetadata metadata = await delivery;                            // delivered
///
/// ⚠ <b>A caller that does not await the first stage is NOT throttled.</b> The record is appended before <c>Send</c>
/// returns, so dropping or deferring the returned <see cref="ValueTask{TResult}"/> still sends it — but then nothing
/// slows the caller: every such send is accepted, and its record, its buffers and a pending admission wait (about
/// 1.5 KB per send, measured) accumulate until the cluster drains them. Offered faster than the cluster accepts,
/// memory grows without bound, and <c>Flush</c> / <c>Close</c> take as long as that backlog takes to drain. This
/// matches the Python binding's async <c>send</c>. If you do not await acceptance, bound your own outstanding sends.
///
/// <b>The first stage has no timeout and never fails.</b> The record is accepted before the wait begins (M11/P3.4),
/// so saturation delays acceptance rather than refusing the record. The first stage completes when capacity
/// frees, or when the producer is torn down — and teardown does not discard the record: it is settled by the
/// closing producer's drain (sent on a normal close; faulted if the send-batch thread itself failed) and reported
/// through the delivery task. Buffer exhaustion inside the core (Java's <c>max.block.ms</c> case) is reported the
/// same way, by faulting the delivery task.
///
/// <b>Cancellation is best-effort (no native abort).</b> A canceled token cancels the delivery task's .NET-side
/// wait but does not abort the native send. ⚠ A token that fires while the first stage is pending does <b>not</b>
/// end that stage — the record is already accepted — it only cancels the delivery task that the stage then yields.
```

**D2 (c) alternative for the last sentence:** "…fires while the first stage is pending ends it with `OperationCanceledException`. The record is still sent and its callback still fires; the delivery task is cancelled."

**D2 (c) text — approved shape; use this, not the (a) paragraphs above.** In the "first stage has no timeout" paragraph, "never fails" becomes "never fails on its own (only your own token can end it early — see Cancellation)". The cancellation paragraph becomes (wording may be tightened by the Actor; every point must survive):

```
/// <b>Cancellation is best-effort (no native abort), and it never un-sends a record.</b> A canceled token cancels
/// the .NET-side waits but does not abort the native send. ⚠ A token that is <b>already</b> canceled when you call
/// <c>Send</c> throws <see cref="OperationCanceledException"/> synchronously and nothing is appended. A token that
/// fires while the first stage is pending ends that stage with <see cref="OperationCanceledException"/> carrying the
/// token — but the record was appended before the wait began, so it is <b>still sent</b>, its delivery callback still
/// fires, and its delivery task is cancelled (as in the Python binding's async <c>send</c>). Consequences:
///   • A canceled or timed-out send may still be delivered. Use the delivery callback
///     (<see cref="Send(ProducerRecord{TKey,TValue}, IDeliveryCallback, CancellationToken)"/>) to learn the outcome.
///   • Inside <c>await producer.Send(record, token)</c> the two cases look alike: an
///     <see cref="OperationCanceledException"/> means "not appended" only for an already-canceled token; otherwise the
///     record is still sent, so <b>retrying after an <see cref="OperationCanceledException"/> can duplicate it</b>.
///     (The same duplicate-on-retry risk exists at the delivery stage, and in Java's <c>future.get(timeout)</c>.)
///   • A caller whose token ends the first stage is not throttled for that send: a short per-send token while the
///     producer is stuck lets records accumulate, one per token period — as with <c>asyncio.wait_for</c> in Python,
///     and like a caller that does not await the first stage at all (above).
```

**Per-`Send` remarks (borrow).** "⚠ Do not mutate the key / value buffers until the **delivery task** completes. The first stage completing does **not** end the borrow: an accepted record may not have reached the core yet."

**`<returns>`.** "A `ValueTask` that completes once the record is accepted, yielding a task that resolves with the record's `RecordMetadata` or faults with a `KafkaException` carrying the delivery failure. Await it once; call `AsTask()` to store it."

The same wording is reflected, not restated, in `AsyncKafkaProducer` and `AsyncMockProducer` (they point at the interface). The `IDeliveryCallback` text "the returned `Task`" becomes "the delivery task" only where it would otherwise be ambiguous.

## 9. Tests, with mutation-proofing (DoD §3, §12)

All tests drive **production's entry points**: the POC `AppendStaged` calls `Accumulator.SubmitAdmitted`. Mutate **fixture and production separately**. Run timing-dependent guards **K=8 in-suite with a fresh harness per rep**, and record each mutant's fail ratio *with its regime* (isolated vs full suite).

| # | Test | Slice | Discriminating mutant (must turn red) |
|---|---|---|---|
| T1 | `Admission_SaturatedBound_ReturnsTheCallAtOnce_WithItsFirstStagePending` (POC :780) | S3 | Main's blocking `_admission.Wait`. Run the call on a bounded thread so the mutant *fails* and does not hang the suite |
| T2 | `Admission_FirstStage_DoesNotResumeItsAwaiterOnTheBatchThread` (POC :827) | S3 | ⚠ `ExecuteSynchronously` on the `ContinueWith` is an **equivalent mutant**, because `SemaphoreSlim`'s `TaskNode` already runs continuations asynchronously. Use a stage-1 TCS **without** `RunContinuationsAsynchronously` completed from `ReleaseAdmission`. If no discriminating mutant exists, record T2 as a structural guard **and say so** |
| T3 | `Admission_CallersThatDoNotAwaitTheFirstStage_AreNotThrottled` (POC :865; cap 4, 40 sends, batch thread held back) | S3 | The blocking wait |
| T4 | **NEW** Permit accounting returns to exactly `MaxAdmittedRecords` after a full drain, including after K caller-token cancels while stage 1 was pending. Assert `_admission.CurrentCount` (the P3.3 second-witness lesson) | S3 | D2 (b), linking the caller's token into `WaitAsync`: count ends K high |
| T5 | **NEW** Flush completes every pending stage 1 (eventually), and every delivery task | S3 | `ReleaseAdmission` count off by one / skipped for the final chain |
| T6 | Renamed POC tests: `WaitingFirstStage_IsCompletedByStop`, `…StopsCancel`, `…FailureHandler`, `…FailureHandlersCancel`, `Flush_IncludesASendWhoseFirstStageIsStillWaitingOnAdmission`, `…HoldsTheFirstStage`, `…CallerTokenFiresWhileWaiting`. **Assert `IsCompletedSuccessfully`, not `IsCompleted`** (D3) for the teardown ones. ⚠ **D2 (c): `…CallerTokenFiresWhileWaiting` flips from the POC's (a) semantics to (c)** — stage 1 ends Canceled with the caller's token (see T16) | S3 | `OnlyOnRanToCompletion` (stage 1 Canceled at teardown); dropping the gate cancel in `AbandonOnThreadFailure` (stage 1 hangs) |
| T7 | **NEW** Ordering across a saturated bound for a **non-awaiting** same-thread burst (N > cap, batch thread held back): `send_batch` order == call order | S3 | Defer the append into the stage-1 continuation (the pre-P3.4 shape) |
| T8 | **NEW/verify** Strict **synchronous** throw (`Func<object>` / `Action`, *not* awaiting) for an already-cancelled token, null record, disposed producer and serializer throw, for both overloads and both flavors. Reuse the existing `SendClosedCheckTests :96/:109/:135` and `TypedSendTests :226` and add only what is missing | S2 | Mark `SendViaPump` `async` (the throws move into the `ValueTask`) |
| T9 | **NEW** Close / Dispose / DisposeAsync with a non-awaiting flood settles **both** stages exactly once, callbacks exactly once after a settle window, on both flavors | S3 | Drop the gate cancel in `Stop` (stage 1 waits on a drain that the order of the close sequence does not give) — Actor to confirm discrimination |
| T10 | Reflection `DeliveryCallbackTests :632` → `ValueTask<Task<RecordMetadata>>` for both overloads | S2 | Revert one overload's type |
| T11 | Category-C repair `MockControlTests :108/:122`: `HistoryCount` asserted after **delivery** | S2 | Await stage 1 only |
| T12 | *Optional* No `UnobservedTaskException` from dropped stage 1s across teardown (GC + finalizers) | S3 | Fault stage 1 at teardown. Mark it flaky-prone; keep only if stable K=8 |
| T13 | `RateLimitSliceTests` (5 Facts, `f4cd3a57`) | S1 | As in the POC commit |
| T14 | `ProducerAcceptanceModeTests` (3 Facts + 2 Theories, `1874c9ad`; neutral queue-full message, both QUEUE_FULL shapes kept) | S4 | Lenient parse (non-`True`/`False` accepted) |
| T15 | Allocation budgets (`PublicProducerSendAllocationBudgetTests`, `…DeliveryCallbackAllocationBudgetTests`) **unchanged**. The fast path's `new ValueTask<Task<…>>(task)` is allocation-free. Report every figure **with its definition** (marginal vs absolute, TFM, best-of-N) | S2/S3 | Inject one extra per-send allocation. The budget must catch it |

**D2 (c) additions (S3), each with a discriminating mutant that must turn red** (K=8 in-suite, fresh harness per rep, for the timing-dependent ones; regime recorded):

| # | Test | Slice | Discriminating mutant (must turn red) |
|---|---|---|---|
| T16 | Token fires while stage 1 is pending → stage 1 ends **promptly** with `OperationCanceledException` whose `CancellationToken` **is the caller's token**, **before** capacity frees (batch thread held back) | S3 | The (a) behaviour: the token is ignored by stage 1 (it stays pending until capacity frees) |
| T17 | That record is **still sent** — asserted on the core / `send_batch` record count, not only on awaiter state — its delivery callback fires **exactly once** after a settle window, and its delivery task is **cancelled**; on **both** producer flavors | S3 | Remove / skip the record after a stage-1 cancel (e.g. un-append or fail the record when the token wins) |
| T18 | The stage-1 token registration is **released** once the admission wait completes (slot and teardown outcomes) | S3 | Drop the `Dispose` of the registration. The Actor designs a sound witness (e.g. a `CancellationTokenSource` whose registration count / callback reachability is observable, or a weak reference to the state object after the wait completes) and **states its limits honestly** |
| T19 | Token-vs-slot race: exactly **one** outcome (Canceled with the token, or RanToCompletion with the delivery task), never both, never neither; the record is sent either way. K=8 in-suite, fresh harness per rep | S3 | Complete both (`SetResult` / `SetCanceled` instead of `TrySet*`), or a path that leaves stage 1 pending forever |
| T20 | The token-cancel path does not run the awaiter's continuation **inline on the cancelling thread** (cancel from a dedicated thread; assert the continuation's thread differs) — D5 under (c) | S3 | Build the stage-1 TCS without `RunContinuationsAsynchronously`. If it proves equivalent, record T20 as a structural guard **and say so** |
| T21 | Allocation: the no-token / fast-path budgets (T15) are **unchanged**; the (c) path's per-send allocation (saturated + cancellable token) is **measured and reported with its definition** (marginal vs absolute, TFM, best-of-N) | S3 | Inject one extra allocation on the fast or no-token path; the budget must catch it |

- The existing D4 guards (T8) must still show an **already-cancelled** token throws synchronously with **nothing appended** (the record-count side of the D4 ambiguity).
- The M14/P1 callback-ordering tests stay as they are. The pump changes in `2c9cc0b8` are comments only, so a saturated-variant ordering test is **optional**.
- **Counts.** At S1 the Actor records the baseline `Passed:` count per TFM (net8.0 and net10.0) for the unit and perf-unit suites. Each slice reports its delta. A zero-match filter or `Test Run Aborted` is a failure, not a pass.

## 10. The harness

- **S1, port `f4cd3a57`.** Apply with `git -C <repo> show f4cd3a57 | git -C <repo> apply -p3 --directory=dotnet --check`, then without `--check`. Files: `ProducerBenchmarkConfig.cs`, `ProducerBenchmark.cs`, `PerfEngineCollection.cs`, and the new `RateLimitSliceTests.cs`.
  - `LimitRpsSliceMs` defaults to 1000 ms. It is read only when `LIMIT_RPS` is set, must be > 0, and otherwise fails with "LIMIT_RPS_SLICE_MS must be positive".
  - The slice size is `LimitRpsSliceMessages = Math.Max(1L, (long)rps * LimitRpsSliceMs / 1000)`.
  - The pacing interval is `RateLimitSliceTicks = Stopwatch.Frequency * LimitRpsSliceMessages / rps`.
  - `PerfEngineCollection.s_managedVariables` gains `LIMIT_RPS_SLICE_MS`.
  - This commit is also the **perf "before" baseline** (§12).
- **S2, port the harness parts of `c67f3013`.** The parts are `Backends.cs` (`IAsyncProducerBackend.Send` → `ValueTask<Task<PerfRecordMetadata>>`), `V3Backends.cs`, `V2ProducerBackends.cs` (ckd is already accepted, so it wraps as a completed `ValueTask`), `ProducerBenchmark.cs` and `ProducerCancellationTests.cs` (fake backend signature).
  - `ProducerBenchmark.cs`: warmup at `:185` awaits both stages; the loop at `:262` **awaits acceptance** (`task = await send.ConfigureAwait(false)`), which preserves today's throttled behaviour.
- **S4, port `1874c9ad`, with the doc adapted.**
  - `AwaitAccepted`, strict parse: `null`, `""` or `"True"` → true; `"False"` → false; anything else → `ArgumentException("AWAIT_ACCEPTED must be True or False, not '…'")`.
  - When false, the loop uses `task = send.IsCompletedSuccessfully ? send.Result : send.AsTask().Unwrap();` with a `try/catch` that turns a synchronous throw into `Task.FromException`, and prints "Not awaiting send acceptance (AWAIT_ACCEPTED=False)".
  - `s_managedVariables` gains `AWAIT_ACCEPTED`.
  - **Drop all lanes wording.** The `ProducerBenchmarkConfig` doc lines that say "the pump behaves the same either way" are **false after S3** and must say the opposite. The `ProducerBenchmark` catch comment and the `ProducerAcceptanceModeTests` queue-full message and comment become neutral.
  - For PerfV2 (ckd), `AWAIT_ACCEPTED` has no effect, because stage 1 is always complete. Document that.
- **No change needed:** QUEUE_FULL is already printed only when > 0 (`ProducerBenchmark.cs:292-295`).

## 11. Rule and doc changes (D8). The user approves the wording; the Actor applies it verbatim in S5

Provenance comes from `git log -S` and the commit trailers. "User" means authored by Pranav Shah with no AI trailer; edits there especially need the user's eye.

| # | Location | Provenance | Before → after (short form) |
|---|---|---|---|
| R1 | `dotnet/CLAUDE.md:35` | `1bc7772ce` (agent, M4/P4b) | `producer.Send(record) ──► Task<RecordMetadata>` → `producer.Send(record) ──► ValueTask<Task<RecordMetadata>>   (await → accepted · await again → delivered)` |
| R2 | `CLAUDE.md:168-175` §3 sketch | `29c79d1a1` (agent, M11/P5) | Both `Task<RecordMetadata> Send(...)` → `ValueTask<Task<RecordMetadata>> Send(...)`, plus the comment `// outer = admission (Java's send blocking, never parks a thread — M11/P3.5); inner = the Future`. "before the Task completes" → "before the delivery Task completes" |
| R3 | `CLAUDE.md:480` idiom row | **User** (`42583351`) | **Unchanged under D1 = C.** Add a new row after it: "`send` — **blocks** *and* returns `Future<RecordMetadata>` → `ValueTask<Task<RecordMetadata>>` on `IAsyncProducer`; the outer stage is the block (admission, completes on acceptance, no thread parked), the inner `Task` is the `Future`; sync `IProducer.Send` unchanged — M11/P3.5, ffi §A1". *(Under D1 = B this user row itself would change, which is another reason for C)* |
| R4 | `CLAUDE.md:489` callback row | M14/P1 (`cb33fe1e1` squash) | "still returns the `RecordMetadata` / `Task<RecordMetadata>`" → "… / `ValueTask<Task<RecordMetadata>>`"; "before the awaiter is released" → "before the **delivery** awaiter is released (unordered relative to acceptance)" |
| R5 | `CLAUDE.md:697`, `:720` | M14/P1 | "the awaiter" → "the delivery awaiter"; `:720` "whose `Task`" → "whose delivery `Task`" |
| R6 | ffi §A1 `:296-307` (the submission queue / single submitter, ⚠ added in M11/P3.2) | `d8ac7c50` (agent, M11/P3.3) | **Stale since P3.4.** Replace with: "Superseded by M11/P3.4: append-first fixes a record's place under `_gate` before any wait, so ordering needs no queue or submitter; the M11/P3.2 machinery was deleted." |
| R7 | ffi §A1 `:308-325` "bound must THROTTLE THE CALLER … an asynchronous admission wait throttles nobody … the synchronous, `max.block.ms`-bounded wait is what throttles" | `d8ac7c50` (agent) | → "**The bound must throttle a caller that awaits admission — so admission must be awaitable.** `IAsyncProducer.Send` returns `ValueTask<Task<RecordMetadata>>`: the outer stage completes on admission (never parking a thread), the inner `Task` is delivery. An async wait behind a *single-stage* `Task` throttles nobody (measured twice: M11/P6 63.5k/3.0 GB; M11/P3.2 2.04 GiB). A caller that does not await the outer stage is deliberately not throttled (Python parity, M11/P3.5 D6), and that is documented on the public surface." The measurement history is kept, and the "capping one container is not sufficient" paragraph stays as is |
| R8 | ffi §A1 `:326-342` "On expiry, fault the `Task`…" and `:343-353` fairness / "spurious `max.block.ms` expiry" | `d8ac7c50` | → "Superseded by M11/P3.4: there is no admission timeout (append-first; the record is accepted before any wait). Java's buffer-exhaustion outcome comes from the core inside `send_batch` and faults the delivery `Task` + fires the callback through `DeliveryRegistration.Fire`. The stage-1 wait never fails. Do not claim `SemaphoreSlim` fairness; ordering rests on append order." |
| R9 | ffi §A1 anti-patterns `:402-409` | `d8ac7c50` | Reword `:402-404` to "…as a throttle where the caller has **no admission stage to await** (a single-stage `Task`)". Delete `:405-407` (the timeout throw) as superseded. Keep `:408-409` (fairness). **Add:** linking the caller's token into the admission `WaitAsync` (the permit drifts and the bound erodes silently, because the ceiling is `int.MaxValue`); a stage-1 continuation that can run on the batch or pump thread; a stage 1 that faults or cancels at teardown; a `SendViaPump` / `SubmitAdmitted` that is `async` or can throw after the append |
| R10 | ffi §A1 tests `:447-449` (the admission timeout fires the callback) | `d8ac7c50` | Delete as superseded. Add T1, T3, T4, T7 and T9 by name |
| R11 | ffi §A4 `:658` "never hold a pin across the returned `Task`" and the `:668-680` deferred-pin consequence | `:658` **User** (`959df4744`) | "the returned `Task`" → "the returned delivery `Task`". After `:680` add: "**Acceptance is not release:** the `ValueTask` stage completing does not end the borrow — a buffer may be reused only after the delivery `Task` completes." |
| R12 | ffi §A6 form C `:880` "Invoke it BEFORE the awaiter is released" | `cb33fe1e1` (M14 squash) | → "BEFORE the **delivery** awaiter is released (it is unordered relative to the admission stage)" |
| R13 | ffi §A7 `:1094-1107` Option A ("Send enqueues and returns instantly with a TCS-backed Task" / "return Task") | **User** (`959df4744`) | → "Send appends and returns a `ValueTask<Task<RecordMetadata>>` whose inner `Task` is TCS-backed"; diagram line → `append; return ValueTask<Task>`. Add a one-line note that the diagram predates the M11/P3.1 send-batch thread |
| R14 | `soak/README.md:325-329` | — | "a continuation on each send's `Task<RecordMetadata>`" → "awaits each send's acceptance stage, then a continuation on its delivery `Task<RecordMetadata>`" |
| R15 | `design/current/STATUS.md` | Manager, at close-out | Add the M11/P3.5 entry and "Next unused dotnet N = 92". Historical lines `:429`, `:704` are **not** rewritten |

`dotnet/.claude/rules/bindings.md:59,:87` ("Java Future → .NET Task") is generic and stays as is.

**D2 (c) adaptations to the approved rows — minimal, and reported verbatim at close-out.** The user approved the rows above as written; because D2 = (c), these approved texts would otherwise imply that a stage-1 wait cannot be ended by the caller's token. Apply each row as approved **plus only** the change shown:

| Row | Approved text (fragment) | Applied text (fragment) |
|---|---|---|
| R7 | "A caller that does not await the outer stage is deliberately not throttled (Python parity, M11/P3.5 D6), and that is documented on the public surface." | "A caller that does not await the outer stage — or whose own token ends it early (M11/P3.5 D2 (c); the record is still sent) — is deliberately not throttled (Python parity, M11/P3.5 D6), and that is documented on the public surface." |
| R8 | "The stage-1 wait never fails." | "The stage-1 wait never fails; only the caller's own token can end the stage-1 awaitable early, with `OperationCanceledException`, and the record is still sent (M11/P3.5 D2 (c))." |
| R9 | "**Add:** linking the caller's token into the admission `WaitAsync` (the permit drifts and the bound erodes silently, because the ceiling is `int.MaxValue`);" | "**Add:** linking the caller's token into the admission `WaitAsync` (the permit drifts and the bound erodes silently, because the ceiling is `int.MaxValue`) — the token may end only the separate stage-1 awaitable (D2 (c)), never the wait, and that awaitable's registration is disposed when the wait completes;" |
| R10 | "Add T1, T3, T4, T7 and T9 by name" | "Add T1, T3, T4, T7, T9, T16 and T17 by name" |

No other §11 row changes for D2 (c). If the S5 Actor finds another approved row that D2 (c) makes false, it stops and reports rather than rewording it.

## 12. Perf validation. **The user runs it; agents do not**

**Builds compared:**
- "Before" = the **S1 commit**: main's blocking client plus the slice support. Main's blocking send ≈ `AWAIT_ACCEPTED=True`.
- "After" = the **S4 commit**.
- Run before and after alternately (A/B/A/B) in one session, on the same local broker (apache/kafka 4.2.0), with **2 reps** each.

**Commands.** Run from any directory; zsh-safe as written. `metrics.jsonl` is written to the PerfV3 process's working directory, which is `dotnet/` under `make -C`. Check after the first run, and **copy the file after every run**, because each run overwrites it.
```
R=/Users/pranavshah/WorkSpace/Confluent/example-confluent-kafka-rust
# R1 max-rate, awaiting acceptance (default)
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=True TEST_DURATION_SECONDS=60 VALUE_SIZE=1024 PARTITIONS=6 make -C "$R" producer-perf-test-dotnet CLIENT_VERSION=3
# R2 max-rate, not awaiting (after only)
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=True AWAIT_ACCEPTED=False TEST_DURATION_SECONDS=60 VALUE_SIZE=1024 PARTITIONS=6 make -C "$R" producer-perf-test-dotnet CLIENT_VERSION=3
# R3 gzip, awaiting
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=True COMPRESSION_TYPE=gzip TEST_DURATION_SECONDS=60 VALUE_SIZE=1024 PARTITIONS=6 make -C "$R" producer-perf-test-dotnet CLIENT_VERSION=3
# R4 gzip, not awaiting (after only; expect GiB-scale RSS; 30 s took 57 s wall in the POC)
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=True COMPRESSION_TYPE=gzip AWAIT_ACCEPTED=False TEST_DURATION_SECONDS=30 VALUE_SIZE=1024 PARTITIONS=6 make -C "$R" producer-perf-test-dotnet CLIENT_VERSION=3
# R5 bounded 50k, 10 ms slices
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=True LIMIT_RPS=50000 LIMIT_RPS_SLICE_MS=10 TEST_DURATION_SECONDS=60 VALUE_SIZE=1024 PARTITIONS=6 make -C "$R" producer-perf-test-dotnet CLIENT_VERSION=3
```

**Acceptance criteria, after vs before:**

| Run | Criteria |
|---|---|
| R1 | Throughput within ±5%; p50 within ±10 ms; RSS within ±15%; CPU no more than +10% |
| R3 | Throughput ≥ −5%; RSS < 2× before |
| R5 | Achieved rate ≥ 49.5k; p50 no more than +2 ms and p99 no more than +5 ms vs before; CPU no more than +10% |
| R2 / R4 | Informational. Each must complete with no hang, a positive delivered count and 0 QUEUE_FULL, and RSS must return after the drain (no leak) |

The harness p50 includes channel residency. The p50 gap against lanes at 50k (9 vs 15 ms) is out of scope and is only noted.

**Measured POC evidence** (2026-10-06, `2c9cc0b8`, local apache/kafka 4.2.0, 1 KiB values, 6 partitions, default producer config):

| Run | Throughput | CPU | RSS | Latency | Notes |
|---|---|---|---|---|---|
| Max rate, awaiting | 587k / 608k msg/s | 374 / 391 % | ~200 MiB | p50 ~75 ms | |
| Max rate, not awaiting | 589k msg/s | 483 % | 227 MiB | p50 63 ms | 0 QUEUE_FULL |
| gzip, awaiting | 87.5k msg/s | 131 % | 88 MiB | p50 29 / p99 34 ms | |
| gzip, not awaiting | 78.7k msg/s | 165 % | ~3.1 GiB | avg 19.3 s / max 27.2 s | ≈1.5 KB per waiting send; no leak |
| 50k, 10 ms slices | ≈50k msg/s | ~104 % | — | p50 15 / p99 23 ms | |

## 13. Slicing, Critic points, context budget

| Slice | Content | Behaviour change | Commit(s) |
|---|---|---|---|
| **S1** | Port `f4cd3a57` (`LIMIT_RPS_SLICE_MS`) | Harness only; **the perf "before" baseline** | `test(dotnet): LIMIT_RPS_SLICE_MS … (M11/P3.5 S1)` |
| **S2** | The two-stage API (D1) with **blocking admission retained**: `SendViaPump` returns `new ValueTask<Task<RecordMetadata>>(completion.Task)` after the existing wait. All §3 callers, the `ProducerSendStages` helper, the harness signature (default awaits acceptance), gRPC servicer, soak, T8, T10, T11, T15. Minimal xmldoc (signature + `<returns>`) | **None** (same blocking) | `feat(dotnet)!: two-stage async Send … (M11/P3.5 S2)` |
| — | **Critic 91, pass 1 (S1 + S2):** shape = approved D1; zero behaviour change; no caller silently drops stage 1 where throttling matters (soak, harness default, servicer); category C fixed; reflection test; no lanes code or wording leaked; analyzer findings fixed rather than suppressed | | |
| **S3** | Port `2c9cc0b8` adapted to D1 / D2. `SendAccumulator`, `SendAccumulatorSettings`, `SendCompletionPump` (comments), `NativeProducer` (minus `_sendPath == Direct`), the full §8 xmldoc, T1–T7, T9, T12, and the §A6 walk recorded | **Yes** | `feat(dotnet): pump admission waits in the first stage, not the caller (M11/P3.5 S3)` |
| **S4** | Port `1874c9ad` with the doc adapted; T14 | Harness only | `test(dotnet): AWAIT_ACCEPTED … (M11/P3.5 S4)` |
| — | **Critic 91, pass 2 (S3 + S4):** exact permit accounting and no drift (P3.4 lesson); no linked token; no inline continuation; teardown → `RanToCompletion`; post-append no-throw; append-order ordering; §A6 walk result; allocation figures with definitions; honest xmldoc (unthrottled non-awaiting, borrow ≠ acceptance); strict `AWAIT_ACCEPTED` parse; T2's mutant honesty. **Plus D2 (c):** the token never reaches `WaitAsync`; the stage-1 registration is disposed in every wait outcome; the OCE carries the caller's token; the record is still sent and its callback fires exactly once (both flavors); exactly one stage-1 outcome under the race; D4 (already-cancelled → synchronous, nothing appended) vs the pending-cancel case distinguished in docs and tests; honest docs incl. duplicate-on-retry and the unthrottled-by-cancellation note; T16–T21 mutants honest | | |
| **S5** | Apply the approved §11 wording verbatim, plus `IDeliveryCallback` / `IProducer` cross-ref touch-ups | Docs and rules | `docs(dotnet): … (M11/P3.5 S5)` |
| — | **Critic 91, pass 3 (light):** docs match the approved text; no other rule edits | | |
| Close | Manager: STATUS (R15), archive `COMMENTS.DONE.91.md` here, reset `COMMENTS.91.md`, memory. (`marked_classes.txt` does not apply to the binding) | | |

Process rules:
- Fixes use `fixup!` commits referencing the slice commit, per `agent-roles.md`. The loop repeats per pass until `COMMENTS.91.md` is empty.
- **Separate Actor spawn per slice.** S1 is small; S2 and S3 each get their own spawn; S4 is small.
- **Context budget.** Prescribe grep-then-ranged-read, never whole-file reads of the large files. Apply POC hunks via `git show <sha> -- <path>` + `git apply --check`, adapting by hand where the table says "adapt". The POC worktree under `.claude/worktrees/` is **never edited**.

Line counts at `104b0b2c`:

| File | Lines |
|---|---|
| `SendAccumulator.cs` | 1514 |
| `NativeProducer.cs` | 1739 |
| `IAsyncProducer.cs` | 272 |
| `AsyncKafkaProducer.cs` | 215 |
| `AsyncMockProducer.cs` | 293 |
| `Interop/SendAccumulatorTests.cs` | 2246 (POC: 2400) |
| `PublicProducerDeliveryCallbackTests.cs` | 969 |
| `PublicProducerSendTests.cs` | 635 |
| `PublicProducerAccumulatorTeardownTests.cs` | 514 |
| `ProducerSendPinLifetimeTests.cs` | 373 |
| `PublicProducerDeliveryCallbackAllocationBudgetTests.cs` | 288 |
| `PublicProducerTypedSendTests.cs` | 260 |
| `PublicProducerMockControlTests.cs` | 208 |
| `PublicProducerSendClosedCheckTests.cs` | 168 |
| `…DeliveryCallbackTfmSmokeTests.cs` | 162 |
| `…SendAllocationBudgetTests.cs` | 148 |
| `…FlushDrainTests.cs` | 142 |
| `…SendTfmSmokeTests.cs` | 101 |
| `ffi-marshalling.md` | 2414 |
| `dotnet/CLAUDE.md` | 1087 |
| `ProducerBenchmark.cs` | 537 |
| `ProducerBenchmarkConfig.cs` | 141 |

**DoD gates for each slice:**
1. `make build-dotnet` (Rust first, §7.1).
2. `make test-dotnet`: `dotnet format --verify-no-changes`, then net8.0 + net10.0 + soak. Assert `0 Error(s)`, a positive `Passed:` count and no `Test Run Aborted`.
3. `make perf-unit-test-dotnet` (net8.0 + net10.0).
4. S2 (servicer): the gRPC multilanguage gate, locally via `test-integration-dotnet-native` (macOS recipe) or CI. Linux amd64 is CI-only.
5. `make verify-dotnet` where Docker is available, including the PerfV3 p99 smoke. Otherwise flag that it was not run.
6. Every brief carries the 9 sandbox shell traps verbatim.

**Port / adapt / exclude, per file** (paths under `dotnet/`):

| File | From | Slice | Verdict | Notes |
|---|---|---|---|---|
| `src/Confluent.Kafka/IAsyncProducer.cs` | c67f3013, 2c9cc0b8 | S2 sig, S3 docs | **adapt** | Return type ×2. Drop the lanes remarks, lane cap, `ASYNC_SEND_PATH`, "copied or consumed inside the call" and the lane `KafkaException` doc. §8 docs |
| `src/…/AsyncKafkaProducer.cs`, `AsyncMockProducer.cs` | c67f3013, 2c9cc0b8 | S2, S3 | **adapt** | Return type. **Keep the `SendViaPump` name (no `SendStaged`).** Docs drop "on the pump path; lanes …" |
| `src/…/Internal/NativeProducer.cs` | c67f3013 (type), 2c9cc0b8 | S2, S3 | **adapt** | **Exclude** `SendStaged`, `SendViaLanes`, `ResolveSendPath`, `AsyncSendPathVariable`, `SendPathOverride`, `_sendPath`, `SendDirect`. Keep "MUST NOT become async" |
| `src/…/Internal/SendAccumulator.cs` | 2c9cc0b8 | S3 | **port** (adapt to D1) | Applies cleanly to main |
| `src/…/Internal/SendAccumulatorSettings.cs`, `SendCompletionPump.cs` | 2c9cc0b8 | S3 | **port** | Comments only |
| `src/…/RecordMetadataResult.cs` | c67f3013 | — | **exclude** under C | Under B: port, minus `Get` (D9) |
| `src/…/IDeliveryCallback.cs`, `IProducer.cs` | — | S3, S5 | **review** | "the returned Task" → "the delivery task" only where ambiguous |
| `src/…/AsyncSendPath.cs`, `DirectSendCompletion.cs`, `LaneSendCompletion.cs`, `ProducerCallbacks.cs`, `ProducerSendMarshal.cs`, `Internal/Interop/NativeMethods.cs` (lane ABI) | c67f3013 | — | **exclude** | Lanes or direct only |
| `tests/…/ProducerSendStages.cs` (new) | c67f3013 | S2 | **adapt** | `send.IsCompletedSuccessfully ? send.Result : send.AsTask().Unwrap()` |
| `tests/…/Interop/SendAccumulatorTests.cs` | 2c9cc0b8 | S2 helper, S3 tests | **port** (adapt type) | POC copy: `git show 2c9cc0b8:dotnet/tests/Confluent.Kafka.UnitTests/Interop/SendAccumulatorTests.cs` |
| `ProducerSendPinLifetimeTests`, `…DeliveryCallbackAllocationBudget`, `…DeliveryCallbackTfmSmoke`, `…FlushDrain`, `…SendAllocationBudget`, `…SendTfmSmoke`, `…TypedSend`, `…MockControl` | c67f3013 | S2 | **port** | TypedSend fixes the `:130` break; MockControl fixes category C and `:167` |
| `PublicProducerAccumulatorTeardownTests.cs` | c67f3013 | S2 | **port** | The one-line `.Delivery()` only |
| `PublicProducerDeliveryCallbackTests.cs`, `PublicProducerSendTests.cs` | c67f3013 | S2 | **adapt** | `.Delivery()` at 203 / 955; reflection `:632`; **drop the `ProducerSendPath.Direct` pins** (`:526`) |
| `PublicProducerSendClosedCheckTests.cs` | — | S2 | **verify** | They are the T8 guards |
| `PublicAdminLogDirsShapeParityTests.cs` | c67f3013 | — | **exclude** under C | Only needed for B's `GetAwaiter` |
| `NativeMethodsPrelinkTests.cs` (657→658), `ProducerSendPath.cs`, `PublicProducerDirectSendTests.cs`, `PublicProducerLanesSendTests.cs` | c67f3013, 2c9cc0b8 | — | **exclude** | Lanes or direct. Generic two-stage cases are re-derived as T1–T9 |
| `grpc-server/AsyncProducerServiceImpl.cs` | c67f3013 | S2 | **port** | `Delivered` helper; 120 s bound covers both stages |
| `soak/SoakClient/SoakClient.cs` | c67f3013 | S2 | **port** (adapt to C) | `Task<RecordMetadata> task = await producer.Send(..)` |
| `soak/README.md` | — | S5 | docs | R14 |
| `tests/Performance/PerformanceCommon/Backends.cs`, `PerfV3/V3Backends.cs`, `PerfV2/V2ProducerBackends.cs`, `…PerformanceTests/ProducerCancellationTests.cs` | c67f3013 | S2 | **port** | |
| `tests/Performance/PerformanceCommon/ProducerBenchmark.cs` | f4cd3a57, c67f3013, 1874c9ad | S1, S2, S4 | **port** | In POC order |
| `…/ProducerBenchmarkConfig.cs`, `…/PerfEngineCollection.cs` | f4cd3a57, 1874c9ad | S1, S4 | **port / adapt doc** | §10 |
| `…/RateLimitSliceTests.cs` (new) | f4cd3a57 | S1 | **port** | |
| `…/ProducerAcceptanceModeTests.cs` (new) | 1874c9ad | S4 | **adapt** | Neutral messages |

**Verified excluded commits:** `effdda39`, `eadea7e2`, `a4f33ecf`, `c164fc41`, `a298819f`, `26dcecbb` and the send-path selector.

## 14. Risks and open questions

| # | Risk | Mitigation |
|---|---|---|
| R1 | API break, plus the **silent** `await producer.Send(r);` change for external users | Pre-publish. §8 example; STATUS note. Every repo caller is in §3, and Critic pass 1 checks them |
| R2 | Non-awaiting callers grow without bound (D6; ~3.1 GiB measured, gzip); Flush and Close take as long as the backlog | Documented on the surface (§8); R4 confirms no leak. D10 is a follow-up |
| R3 | Teardown's gate cancel runs W waiter registrations **on the closing thread**, so it is O(W) | Accept; observed through R4's end-to-end wall time |
| R4 | Accounting drift silently un-bounds the gate (ceiling `int.MaxValue`, P3.4) | T4 asserts `CurrentCount == cap` after a drain; D2 (b) is rejected |
| R5 | `ValueTask` misuse (double await, `.Result` while pending) | Doc "await once / `AsTask()`". The outer stage is never `IValueTaskSource`-backed today, but **do not promise** that |
| R6 | netstandard2.0 / net462 `ValueTask` comes transitively (`Microsoft.Bcl.AsyncInterfaces` 8.0.0 → `System.Threading.Tasks.Extensions`) | Confirm all TFMs build; add an explicit reference only if needed |
| R7 | Analyzer fallout (CA2012 etc.) under `TreatWarningsAsErrors` | The POC built clean. Fix each site; never suppress |
| R8 | Stage 1 has no deadline. In a broker outage it is bounded only by batch-thread progress, and whether the core waits `max.block.ms` **per record** is not re-verified | Pre-existing since P3.4 (now a pending ValueTask instead of a parked thread). Flagged for a follow-up check |
| R9 | Flaky timing tests | K=8 in-suite, a fresh harness per rep, and the full suite run ≥3× |
| R10 | `PerfV3SmokeTests` p99 gate in `verify-dotnet` | The default harness awaits acceptance, so behaviour matches; run it where Docker is available |
| R11 | Context exhaustion in the S2 / S3 spawns | Per-slice spawns, the line-count table, bounded reads |
| R12 | D2 (c) makes the throttle escapable by cancellation | **Accepted by the user (D2 = (c), 2026-10-06).** Mitigations are documentation obligations on the public surface (§8 D2 (c) text): a cancelled or timed-out send may still be delivered (use the callback); an OCE means "not appended" only for an already-cancelled token (D4), so **retrying after an OCE can duplicate the record** (that risk also exists at the delivery stage and in Java's `future.get(timeout)`; (c) extends it to stage 1); a short per-send token while the producer is stuck lets records accumulate, documented like D6. T4 / T16–T19 pin the accounting and the "still sent" contract |
| R13 | The gRPC Linux gate is CI-only | Local native recipe or CI before the S2 Critic sign-off |

**Open questions:**
- Q1: Should the phase run directly on `prashah_dev_dotnet_binding`, or on a feature branch? **Answered: directly on `prashah_dev_dotnet_binding`; commit, do not push.**
- Q2: D10 now or later? **Answered: later (D10 deferred).**
- Q3: Does the user also want PerfV2 (`CLIENT_VERSION=2`) reference runs for R1 / R5? They are optional. **Answered: yes, include them in the hand-off.**

## 15. Parity anchors

| Topic | Anchor |
|---|---|
| Python async `send` | `python/producer.py:654-705`: `Producer_send` `:689` → `_add_future` `:690` → `if full:` `:691` → `space` future `:697` → `Producer_on_space_available` `:703` → `await space` `:704` → `return ret` `:705` |
| Python harness awaits stage 1 | `python/test/performance/producer_performance_test.py:871,911` (`produce_call = await producer.send(...)`), the analogue of `AWAIT_ACCEPTED=True` |
| Java `send` and its blocking | `KafkaProducer.java` `:840` `send(record)`, `:959` `send(record, callback)`, `:975` `doSend`, `:988` `waitOnMetadata(..., maxBlockTimeMs)`, `:1029` `accumulator.append`, `:1049-1061` `catch (ApiException)` → callback with the −1 placeholder plus a failed future, `:1062-1065` `InterruptException` (not appended, no callback), `:953` `@throws InterruptException` |
| Java buffer bound | `BufferPool.java:107` `allocate(size, maxTimeToBlockMs)`, `:161-162` `BufferExhaustedException` |
| Callback ordering | `ProducerBatch.java:303-323`; `Callback.java:20-21,28-33` |
| Main code | `SendAccumulator.cs:350-379` (blocking `SubmitAdmitted`), `:389-397` (`ReleaseAdmission`), `:150` (`_admission`), `:167` (`_spaceGate`); `NativeProducer.cs:492` (`SendViaPump`), `:503` (synchronous cancelled-token throw); `SendAccumulatorSettings.cs:115,122` |
| Prior rulings | P3.3 PLAN `:400-411` (DV-1, "SendViaPump must not become async"), `:625-660` (D1–D7); P3.4 PLAN `:24-25`, `:49-67` (D1 / D2); P3.2 PLAN `:1219` (DV-1 definition) |

## 16. Contradictions with the brief and prior records

1. The Python anchor runs to `:705` (`return ret`), not `:702`.
2. The brief marks cancellation (D2) as unconfirmed. The root POC memory records "the caller's token does not end the wait" as the **user's** decision, and P3.4 D2 already ruled the same for the blocking wait. The plan asks again anyway and recommends (a).
3. "An already-cancelled token throws synchronously" is **already main's behaviour** (`NativeProducer.cs:503`), not a POC change.
4. §A1's admission text has been stale **since P3.4**, not only because of this change: the submission queue / single submitter at `:296-307` and the `max.block.ms` expiry / fault-the-Task text at `:326-353` (`SendAccumulator.cs:297` records the replacement).
5. QUEUE_FULL is already printed only when > 0 on main, so nothing needs porting there.
6. The POC files `ProducerSendPath.cs`, `PublicProducerDirectSendTests.cs`, `DirectSendCompletion.cs` and `AsyncSendPath.cs` do not exist on main.
7. `PublicProducerSendClosedCheckTests.cs`, `IDeliveryCallback.cs`, `soak/README.md`, `CLAUDE.md` and `ffi-marshalling.md` are outside the POC's file lists but are in scope.
8. `f4cd3a57` uses the pre-move `bindings/dotnet/` paths, so it is applied with path translation (`-p3 --directory=dotnet`).
9. "Make the pump path behave like Python" cannot keep today's `Task<RecordMetadata>` return type and still stay bounded (§1). The user's request therefore implies the API break in D1.
