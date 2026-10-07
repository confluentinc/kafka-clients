# M11/P4.2 — Sync producer: `Send` returns `KafkaFuture<RecordMetadata>`

**Status: APPROVED by the user (2026-10-07), with the rulings recorded in §2, §14 and §16.** The S-rows in §2 are the user's settled rulings. The D-rows carry the user's ruling in their last column. D7 (`Get(TimeSpan)`) and D7.1 (bounding the sync gRPC servicer) are **not** taken this phase; every D7-dependent item (the §3 overload and its xmldoc, §4's bounded-wait example, tests N7/N8/C13/C14, mutation M6, and the §12 E10 supersession) has been removed. The follow-ups are listed in §14.

**Agent number: N = 93** (dotnet numbering). Confirmed three ways at the base:
- `design/current/STATUS.md:31` says "Next unused dotnet N = 93".
- No `COMMENTS.93.md` / `COMMENTS.DONE.93.md` exists at the binding root. The highest `COMMENTS.9x.md` files are `COMMENTS.90.md` and `COMMENTS.91.md`.
- `git grep 'N=93\|COMMENTS.93\|COMMENTS.DONE.93\|N = 93' -- dotnet` finds only that STATUS line and the P3.6 plan's two "next N" statements.

**Base:** `prashah_dev_dotnet_binding` @ `d6ce0512`. This equals `origin/prashah_dev_dotnet_binding` and contains `origin/master` `7ccc9ffb` (merge `2c89c1b4`).
- The merge changed **no `dotnet/` code**: `git diff --stat 1e79fed4..d6ce0512 -- dotnet` shows only `STATUS.md` and the P3.6 plan.
- It did change 39 files outside `dotnet/`: the Rust core and the multilanguage tests.
- So: rebuild the native library before the first test run, and re-baseline the gRPC arm counts (F16).

**Mode A:** C# only.
- Zero new or changed `[DllImport]` / `static extern`.
- No change under `rust/` or `src/ffi`, and no header change.
- The existing `FutureRecordMetadataGet` / `FutureRecordMetadataDestroy` move from the caller's thread to the pump; they are not added or removed.

**Pair:** `dotnet-actor` / `dotnet-critic`, N = 93.

---

## 0. The requirement and scope

The sync `IProducer<TKey,TValue>.Send(record)` and `Send(record, IDeliveryCallback)` currently return `RecordMetadata` and block until delivery. They change to return **`KafkaFuture<RecordMetadata>`**, which is Java's `Future<RecordMetadata>` from `Producer.send`:
- `public readonly struct KafkaFuture<T>` holds a **managed** completion and never an `IntPtr`.
- `T Get()` is Java's `Future.get()`: it blocks until the record is acknowledged or fails.

**Engine (S-2, "Option 2"):**
1. `Send` keeps today's inline `kafka_producer_Producer_send`.
2. It hands `(native future, SyncCompletion, DeliveryRegistration?)` to the producer's existing `SendCompletionPump` through a new single-future path.
3. It returns `new KafkaFuture<RecordMetadata>(completion)`.
4. The pump then reads the future, copies the result out, fires the callback and completes the latch, in that order. It then destroys the future, and it is the only owner and the only code that frees it.

**In scope:**
- `KafkaFuture<T>` and the internal `SyncCompletion<T>`.
- The pump's single-future path and every teardown path for it.
- The retype of `IProducer` / `KafkaProducer` / `MockProducer`, `NativeProducer.Send` (+ `EnsurePump`), the caller sweep (tests, the sync gRPC servicer, PerfV3) and the mock pattern.
- Callback and error semantics.
- Docs and rules (§12, approved text only).
- Perf Step 1 (forced by the compile) and Step 2 (its own slice, after the Step-1 numbers).

**Not in scope:**
- `IsDone` / `Cancel` / `IsCancelled` on `KafkaFuture`. This follows the P3.6 D6 precedent; adding them to a struct later is non-breaking.
- A `CancellationToken` on `Get`. M11/P4 decision #4 stands for now: the producer has no `wakeup()`.
- A timed `Get(TimeSpan)` (D7, **deferred** by the user: "We will add one with cancellation token later if required"). Follow-up **FU-4**: a timed or cancellable `Get`, with a `CancellationToken`.
- Bounding the sync gRPC servicer (D7.1, **dropped**). It stays unbounded and M11/P8 D-3 stands.
- A core "completion queue / wait-for-any" ABI. That is Mode B, follow-up **FU-1** (D1).
- A flush completion barrier (D8 = (b), deferred). Follow-up **FU-2**.
- `Close` / `Dispose` from inside a delivery callback. The pump would join itself; this is a pre-existing async hazard that now extends to sync. Follow-up **FU-3**, §13 R6.
- The async surface. `AsyncKafkaFuture`, `SendViaPump`, `SendAccumulator` and the batch path are behaviourally unchanged. Only the pump's shared entry type changes (D11).
- Rust, the FFI and the header.
- Anything about the slow 383k run, the producer-metrics sampler or the `buffer.memory` latency investigation.

---

## 1. Facts verified at `d6ce0512` (so the plan does not rest on guesses)

| # | Fact | Evidence |
|---|---|---|
| F1 | Today's sync `Send` runs on the caller's thread: an inline `ProducerSendMarshal.Send`, then a **blocking** `FutureRecordMetadataGet`, `delivery?.Fire(...)`, then return or throw, with `FutureRecordMetadataDestroy` in a `finally` | `Internal/NativeProducer.cs:714-791` (docs `:660-713`) |
| F2 | `FutureRecordMetadataGet` has exactly **one** caller (`NativeProducer.cs:738`). The xmldoc of `FutureRecordMetadataDestroy` names the sync path as a user. Both stay used after the move (the pump's single path) | `Interop/NativeMethods.cs:2440-2463` |
| F3 | The pump is one thread per producer (`"confluent-kafka-producer-send-pump"`). Its parts are: `ConcurrentQueue<PendingSendBatch>`; `Enqueue` gated under `_stopLock`, which faults in place with `TeardownException()` and frees the futures once `_stopped`; `CloseGate`; `WaitForQueueDrain` (spins on `_queue.IsEmpty`); `Stop` (join, then `DrainAndFaultRemaining`); and the `RunLoop` catch, which calls `FaultGroupCompletions`. `PendingSendBatch` is a private nested sealed class | `Internal/SendCompletionPump.cs`: ctor `:207`, `Enqueue :277`, `CloseGate :334`, `WaitForQueueDrain :415`, `Stop :456`, `RunLoop :473`, `DequeueGroup :553`, `ProcessGroup :593`, `ProcessBatch :680`, `DrainAndFaultRemaining :828`, `FaultGroupCompletions :881`, `TeardownException :896`, `PendingSendBatch :931` |
| F4 | The pump is created **only** inside `EnsureAccumulator`, by `_pump ??= new SendCompletionPump()` under `_pumpLock` after `ThrowIfClosed()`. **There is no `EnsurePump`** | `NativeProducer.cs:996-1020` |
| F5 | `StopPump` is null-safe for both the pump and the accumulator. Its order is StopAccumulator → CloseGate → ProducerFlush (swallowed) → WaitForQueueDrain(30 s) → Stop → ReleaseTopicCache. Its flush already runs without a pump (M11/P8 Blocker 2). Its comments still say that only the async `SendViaPump` starts a pump (a doc twin) | `NativeProducer.cs:1226-1275`; `StopPumpAsync :1298-1338` |
| F6 | Neither `Flush` waits for the pump: neither the sync `FlushWithAccumulatorDrainBound` nor the async `FlushCore`. The pump waits at `:1268` / `:1333` are teardown only | `NativeProducer.cs:827-858`, `:280-301` |
| F7 | In the core, an `ApiException` gives `Ok(failed future)` and a null `out_error`. Every other error gives `Err` and `out_error`. So **today** a sync `ApiException` fires the callback inline and then throws out of `Send` | `rust/src/producer/kafka_producer.rs:1653-1672`, `handle_api_error :2034-2051`; `rust/src/ffi/producer.rs:1226-1256` |
| F8 | In the mock's sync send, the record is in the mock's history **before** `Producer_send` returns. In manual mode the future is pending | `rust/src/ffi/producer.rs:353-374` (`block_on(mock.send(...))`) |
| F9 | `IProducer.cs` (238 lines). `Send` is at `:127` and `:185`. The "decision #1" Python-divergence paragraph is `:50-66`. No CancellationToken: `:76-79`. The two-thread manual-mock pattern: `:81-88`. Single-owner teardown: `:90-95`. The summary "blocks until the cluster acknowledges" is at `:114-120`. Inline callback (M14/P1 D3): `:147-153`. Thread-safety obligation (finding 72.17): `:155-166`. Null callback (D8): `:168-174` | read in ranges |
| F10 | `MockProducer.CompleteNext` says "call it from a different thread" (`:185-192`); the class doc repeats this at `:53-58` and `:82-86`. `KafkaProducer.SendValidated` is at `:141-167`; `MockProducer.SendValidated` at `:144-168` | read in ranges |
| F11 | `AsyncKafkaFuture.cs` (119 lines) is the template: a struct with a private `Task<T>?` field, an internal ctor, `Get`, `==`/`!=`/`Equals`/`GetHashCode`, and `default` → `InvalidOperationException` | whole file |
| F12 | Java's `FutureRecordMetadata`:<ul><li>`get()` awaits the latch and throws `ExecutionException` via `valueOrError` (`:61-66`);</li><li>`get(timeout, unit)` throws `java.util.concurrent.TimeoutException("Timeout after waiting for N ms.")`, and the record continues (`:69-80`);</li><li>`cancel` returns `false` (`:51-53`).</li></ul>`KafkaProducer`:<ul><li>`flush()` has a guard for calls on the I/O thread (`:1216-1220`), and its post-condition is that every earlier future `isDone()` (`:1177`);</li><li>`close` called from a callback becomes `close(0)`, with no self-join (`:1398-1404`);</li><li>`catch (ApiException)` fires the callback on the **caller** and returns `FutureFailure` (`:1049-1061`).</li></ul>`ProducerBatch.completeFutureAndFireCallbacks` sets the value, fires the callbacks, then calls `done()` (`ProducerBatch.java:303-323`) | `kafka/clients/.../producer/` |
| F13 | Python:<ul><li>`Producer.send` returns a `concurrent.futures.Future`, and `on_delivery` runs after the future resolves (`python/producer.py:132-151`, `:338-389`);</li><li>`flush` has no future barrier (`:410-415`);</li><li>the sync gRPC servicer waits with `future.result(timeout=120)` (`python/grpc_server.py:228`);</li><li>sync perf `main()` pipelines (`producer_performance_test.py:597-773`; queue `:704`; warm-up `:684-699`; recorder `:653-674`; `CompatibleProducer :133-170`).</li></ul> | read in ranges |
| F14 | The reflection asserts on the sync return type:<ul><li>**one**, `PublicProducerDeliveryCallbackTests.cs:616` (`SyncSurfaces_DeclareTheCallbackOverload`). `:675` asserts the callback's *parameter* type and is unchanged;</li><li>there is **no** shape test for the plain sync `Send(record)` (the async one is at `:649`).</li></ul> | grep `ReturnType` |
| F15 | `PublicSyncProducerSendAllocationBudgetTests` measures per thread (`GC.GetAllocatedBytesForCurrentThread`). The marginal budget is (large−small)/64 ≤ 512 B, with no absolute budget. `MeasureSends` already writes `_ = producer.Send(...)` | `:58`, `:104-141` |
| F16 | P3.6's gRPC gate (145/145, producer arms 38/38) was taken at `16c4f803`, **before** #206/#212. Those arms are generated per backend, so the counts at `d6ce0512` are unknown until measured | P3.6 PLAN §8 gate 5; merge `2c89c1b4` |
| F17 | Perf:<ul><li>`V3SyncProducerBackend.Send` is at `PerfV3/V3Backends.cs:26-44`;</li><li>`IProducerBackend.Send` returns `PerfRecordMetadata` (`PerformanceCommon/Backends.cs:71-84`);</li><li>`RunSync` is serial (`ProducerBenchmark.cs:64-125`), with the "key .NET deviation (PLAN §5.1 / D5)" remark at `:51-58`;</li><li>`V2SyncProducerBackend` is Produce + Poll thread + TCS, waited serially (`PerfV2/V2ProducerBackends.cs:38-110`);</li><li>`Producer_Smoke_Sync` (`PerfV3SmokeTests.cs:104`) runs ASYNC=False, 100 rps, 10 s, 2048 B, p99 ≤ 70 ms, and asserts exit code 0. It is on the CI net10.0 perf leg.</li></ul> | read in ranges |
| F18 | The sync gRPC servicer is intentionally unbounded (M11/P8 D-3 / option S2). The reason given is that "the only way to bound it is Task.Run + WaitAsync". `CallbackLog.cs:45-46` says the sync callback runs on the handler thread | `grpc-server/ProducerServiceImpl.cs:165-191` |
| F19 | Pump test seam: `IntPtr.Zero` futures stand in on paths that never read them (destroy is null-safe) | `Interop/SendCompletionPumpGateTests.cs:38` |
| F20 | The test `Flavor` adapter's sync arm is `Task.Run(() => _producer.Send(record, callback))`. The reentrant test (`:486`) sends from inside the callback through it, so after the retype it still calls `Get` on a pool thread, never on the pump | `PublicProducerDeliveryCallbackTests.cs:953` |
| F21 | The name `KafkaFuture` appears 160 times in `src`/`tests`/`grpc-server`. All are Java-type mentions inside `<c>` text or comments (Admin xmldoc); there is no `cref="KafkaFuture`, so there is no collision | grep, `obj/`/`bin/` excluded |
| F22 | Mode A counts: `internal static extern` is 219 (`NativeMethods.cs`) + 357 (`NativeMethods.Admin.cs`) | `grep -c` |
| F23 | Per-send types: `SerializedProducerRecord` is an internal readonly **struct**; `DeliveryRegistration` and `RecordMetadata` are sealed **classes** | `Internal/SerializedProducerRecord.cs:44`, `Internal/DeliveryRegistration.cs:52`, `RecordMetadata.cs:43` |

---

## 2. Decisions

| # | Decision | Status | Recommendation / ruling |
|---|---|---|---|
| S-1 | Public shape: `public readonly struct KafkaFuture<T>`, the sync sibling of `AsyncKafkaFuture<T>`. It holds a managed completion, **never** a native pointer: a copyable struct cannot own a native handle without a leak on fire-and-forget and a use-after-free on a second `Get()` or on a copy | **Settled** | — |
| S-2 | Engine "Option 2" (§0). `Get()` blocks on a plain lock + `Monitor` (Java's latch), never on a `Task`, so it is **not** sync-over-async (consumer-threading.md §1.1) | **Settled** | — |
| S-3 | Rejected alternatives, with their reasons:<ul><li>**Option 1** (reuse the async engine plus a blocking admission wait): breaks sync buffer reuse; moves backpressure to a cap that exists only in the binding; makes the mock Send → CompleteNext → Get sequence racy; blurs sync and async. Its only win is that close can wake blocked senders. Python is closer to Option 1, but Python's bytes are immutable.</li><li>**Option 3** (`kafka_producer_Producer_send_with_callback`): a reverse P/Invoke and a `GCHandle` per send; a second completion engine; and it brings back the push-engine teardown residuals that M11/P9 Option B accepted permanently.</li></ul> | **Settled** | Recorded in §13 and in the IProducer remarks (S5) |
| S-4 | No mixing. Each producer object owns its own `NativeProducer` and its lazily created pump (`KafkaProducer.cs:72`, `AsyncKafkaProducer.cs:95`, `MockProducer.cs:72`, `AsyncMockProducer.cs:97`). **Consequence used below:** a pump serves sync singles *or* async batches, never both | **Settled** | — |
| S-5 | Perf in two steps. **Step 1** (S3): `V3SyncProducerBackend.Send` → `_producer.Send(r).Get()`; `RunSync` stays serial. **Step 2** (S6, after the Step-1 numbers, with new baselines): the sync handle on `IProducerBackend`, `RunSync` as a port of Python's `main()`, `V2SyncProducerBackend` as a port of `CompatibleProducer`, and removal of the D5 note | **Settled** | §8 S6, §11 |
| D1 | **Head-of-line.**<ul><li>**(a)** One blocking singular `_get` per single, in FIFO send order.</li><li>**(b)** Coalesce consecutive singles into one `get_all`. This brings back exactly the cross-send grouping you overrode in P3.2 (D2 override (iii), `P3.2 PLAN.md:1238`, §3B `:632`).</li></ul>With (a), a slow partition holds back later records' `Get` returns and callbacks (not their sending): milliseconds in steady state, up to `delivery.timeout.ms` (120 s default, `producer_config.rs:320`). The async pump already has head-of-line at group granularity, and so does Python | **Ruled** | **Ruling: (a), as recommended** — one single-future `get` per record, in send order. Document the head-of-line limitation in the IProducer remarks. Record follow-up **FU-1**: a core "completion queue / wait-for-any" API (Mode B). Considered for FU-1 and not taken: `get_async` (push, which re-imports the M11/P9 residuals) and `is_done` polling (spins). Perf note: (a) costs one P/Invoke per record on the pump where (b) amortizes; Step 2 measures it (§13 R9) |
| D2 | **Sync failure policy.**<ul><li>Anything before the core **accepts** the record keeps throwing **out of `Send`**, unchanged: null record or callback, closed, serializer, or a synchronous `out_error` (F7, `Err`).</li><li>Anything after acceptance surfaces from **`Get()`, after the callback fired**: an `ApiException` (failed future, null `out_error`) and every delivery failure.</li><li>`Get()` rethrows the **cause itself**, the same instance on every call (`ExceptionDispatchInfo`), unwrapped. Java wraps it in `ExecutionException`; .NET has no such type, and both today's `Send` and `await f.Get()` (async) unwrap.</li></ul>Effect: the `ApiException` row now **matches Java's `catch (ApiException)` outcome** (the callback fires and a failed future is returned without throwing). Today's sync path cannot match it. The callback runs on the pump, not the caller (a deviation forced by the ABI) | **Ruled** | **Ruling: as recommended (as stated).** Options considered and not taken: (b) wrap in `AggregateException` (`Task.Wait` style), which matches no other binding surface; (c) throw `ApiException` out of `Send`, which is impossible without blocking on the future |
| D3 | **Callback thread.** Callbacks move from inline on the caller (M14/P1 D3) to the **pump thread**. This **supersedes M14/P1 D3** and retires CLAUDE.md §4's "sync sub-divergence" (one instance entered on N caller threads at once): one producer's callbacks are now serialized on both surfaces, as in Java (`Callback.java:20-21`) | **Ruled** (supersedes M14/P1 D3) | **Ruling: (a), as recommended — the pump, as S-2 implies.** Not taken: (b) fire on the thread that calls `Get()`, because a discarded future would then never fire, which breaks exactly-once |
| D4 | **The sync producer starts the pump.** It is created lazily on the first `Send` by a new `EnsurePump()` (double-checked; under `_pumpLock` after `ThrowIfClosed()`, the same lock and latch ordering as `EnsureAccumulator`). A sync-only producer spins **exactly one** thread and never the send-batch thread. This **supersedes M11/P4 #2** ("blocking get, no pump") and ffi §A1's "the sync path starts neither thread" | **Ruled** (supersedes M11/P4 #2, ffi §A1 sync-thread text) | **Ruling: (a), as recommended — lazy.** Not taken: (b) eager in the ctor, which costs a thread for a producer that never sends |
| D5 | **M11/P4 decision #1 is superseded.** Its premise, "(forced by .NET's single Task type)", has been false since P3.5/P3.6: `ValueTask` plus `AsyncKafkaFuture` already restored the two-stage shape on the async surface. The `IProducer` xmldoc (`:50-66`, `:114-120`) is rewritten, and so is `M11/P4 PLAN.md:115-140` (history is not rewritten; STATUS records the supersession) | **Ruled** (supersedes M11/P4 #1) | **Ruling: confirmed, as recommended.** The rewritten paragraph says sync `Send` now matches both Java (`send` returns `Future`) and Python (`send` returns `Future`) |
| D6 | **Equality.** `IEquatable<KafkaFuture<T>>`, `==`/`!=`, reference identity of the completion, `default == default`. This mirrors P3.6 D4 | **Ruled** | **Ruling: (a), as recommended — as AsyncKafkaFuture.** Not taken: (b) no equality members. CA1815 is not enforced in this repo (P3.6 F3), so this is a design choice for parity with `AsyncKafkaFuture`, not an analyzer requirement |
| D7 | **A timed `Get(TimeSpan)`** (Java's `get(timeout, unit)`, F12). **D7.1:** bound the sync gRPC servicer with it. | **Ruled: deferred / dropped** | **Ruling: NO `Get(TimeSpan)` for now** — the user: "We will add one with cancellation token later if required." `KafkaFuture<T>` has only `Get()`. Recorded as follow-up **FU-4** (a timed or cancellable `Get`, with a `CancellationToken`); M11/P4 decision #4 (no `CancellationToken` on sync) stands for now. **D7.1 dropped:** the sync servicer (`grpc-server/ProducerServiceImpl.cs` ~`:165-190`) stays **unbounded** and **M11/P8 D-3 stands** (not superseded). In S3 the servicer becomes `producer.Send(...).Get()` and its comment block is rewritten for the new mechanics only (see the S3 row in §8) |
| D8 | **Flush post-condition.** Java's `flush()` guarantees every earlier future `isDone()` with its callbacks fired (F12). In .NET, the sync `Flush` (and the async one, and Python's) waits for the **core**, not for the **pump** (F6). So callbacks can trail `Flush` by the pump's lag. The sync surface did not show this before, because `Send` blocked.<ul><li>**(a)** A barrier ticket in the pump FIFO, waited after the native flush, plus Java's on-callback-thread guard (`KafkaException`, Java's message). It would apply to both flavors, which changes async `Flush`-in-callback from "works" to "throws".</li><li>**(a2)** The same barrier on the sync surface only.</li><li>**(b)** Defer, document it as shared with async and Python, and record **FU-2**.</li></ul> | **Ruled** | **Ruling: (b), as recommended — defer, FU-2.** It keeps parity and keeps risk out of the pump in a teardown-heavy phase. `Get()` after `Flush` returns at once, and `IsDone` is deferred, so the only observable gap is callback lag. Stated in the IProducer remarks |
| D9 | **A guard on `Get()` from the pump thread.** It throws `InvalidOperationException` **only** when the completion is not done **and** the caller is on the completion's **own** pump (a `[ThreadStatic]` "current pump" set in `RunLoop`, compared with the completion's owner). A `Get` on a done future returns, and a `Get` on another producer's future is allowed. Java has no such guard (its `get()` deadlocks here); the precedent is `flush()`'s I/O-thread guard (F12) | **Ruled** | **Ruling: (a), as recommended — the pump-thread guard, `InvalidOperationException`, owner-keyed.** Message: "KafkaFuture.Get() was called on this producer's send-completion thread — from inside a delivery callback — for a send that has not completed; that would deadlock. Wait for it from another thread." Not taken: (b) `KafkaException`, mirroring the type of Java's flush guard; (c) no guard, i.e. Java's silent deadlock |
| D10 | **Naming deviation note.** In Java, `org.apache.kafka.common.KafkaFuture` is the **Admin** type, which this binding maps to `Task<T>`. The sync producer's handle is Java's `java.util.concurrent.Future`. The name is the user's (pairing with `AsyncKafkaFuture`, cf. P3.6 D7) | **Ruled** | **Ruling: as recommended — the naming note. Record it** in the `KafkaFuture` xmldoc and the CLAUDE.md idiom row (E2), worded as P3.6 D7's note is |
| D11 | **Allocation budget and entry shape.** On the caller's thread `RecordMetadata` moves to the pump, and the new objects are `SyncCompletion` (≈48 B, est.) plus an entry. Entry options:<ul><li>**(a)** A `PendingSyncSend` class (≈40 B, est.) in the existing queue, under a `PendingEntry` base shared with `PendingSendBatch`.</li><li>**(b)** A struct entry in a **second** queue. With S-4 one of the two queues is always empty, so this costs 0 extra objects, but every teardown path must cover two queues.</li><li>**(c)** The completion is also the entry, holding the future in a pump-private field that is zeroed when taken. 0 extra objects, but it blurs S-1's line (the struct still never holds a pointer, but its completion object does until the pump frees it).</li></ul>**Budget:** a new **absolute** caller-thread budget equal to the S4-measured figure + 16 B (the P3.6 D11 (a) precedent), so a 24 B box is caught. Keep the marginal zero-copy test, and add a structural Critic check that `KafkaFuture` is never boxed | **Ruled** | **Ruling: (a), as recommended — shared base entry, budget = measured + 16 B, plus the no-boxing check.** Entry (a) (one queue, one ordering, the fewest teardown sites); budget as stated. Report figures with their definition: per caller thread, absolute, best of N, net8.0 and net10.0 |
| D12 | **Bare `producer.Send(r);` sweep.** After the retype, a statement `producer.Send(r);` still compiles and **silently becomes fire-and-forget**. No analyzer flags a discarded struct: CA1806 is not enforced here (P3.6 F3) and does not cover arbitrary methods anyway. Policy:<ul><li>no bare `Send(...);` statement remains in repo code;</li><li>each becomes `.Get()` where the code relied on the block, or `_ = …Send(...)` with a one-line reason where fire-and-forget is the point;</li><li>the Critic re-greps it.</li></ul> | **Ruled** | **Ruling: (a), as recommended — the sweep, plus a migration line** in the IProducer remarks and STATUS. Not taken: (b) fix only what fails, which leaves silent races in tests |
| D13 | **Mock manual completion.** Support the single-thread `var f = mock.Send(r); mock.CompleteNext(); f.Get();` and drop "call CompleteNext from a different thread". **Recorded deviation:** after `CompleteNext` returns, the callback and the latch follow on the pump a moment later, as `AsyncMockProducer` already does. Java's `MockProducer.completeNext` completes synchronously. So tests call `Get()` before asserting on a callback (the callback runs before `Get` returns) | **Ruled** | **Ruling: (a), as recommended — the single-thread mock pattern.** Not taken: (b) make `CompleteNext` wait until the pump has passed that record, which needs extra machinery that `AsyncMockProducer` lacks |
| D14 | **A serial perf knob for latency.** Optionally add a .NET-only opt-in (e.g. `SYNC_SERIAL=True`) that keeps the old send → `Get` → next loop after Step 2 | **Ruled** | **Ruling: (a), as recommended — no serial knob.** Python has none; the Step-1 runs are the recorded serial baseline, and a low `LIMIT_RPS` already measures latency. (b) Add it, default off, documented as a .NET deviation |

### 2.1 Allocations per path, before → after (D11, estimates; S4 measures)

| Path / thread | Before | After (D11 (a)) |
|---|---|---|
| Sync `Send`, caller | `ProducerRecord` (user), `DeliveryRegistration` (callback overload), `RecordMetadata` + its strings (CopyOut) | `ProducerRecord`, `DeliveryRegistration`, **`SyncCompletion`**, **`PendingSyncSend`**; `RecordMetadata` **moves** to the pump |
| Sync, pump | — (no pump) | `RecordMetadata` (moved); `ExceptionDispatchInfo` on failure only |
| Sync producer, once | nothing | one pump `Thread` + `ManualResetEventSlim` + three reused arrays (D4) |
| Async (any) | unchanged | unchanged: `PendingSendBatch` gains a base class, and the per-group dispatch is a type test or virtual call |

Note the measurement trap: the caller-thread budget no longer sees `RecordMetadata`, so the absolute caller figure may *fall*. Both definitions, caller-only and caller + pump, are stated wherever a number is quoted (DoD gate 7).

---

## 3. The exact new public API

```csharp
namespace Confluent.Kafka;

/// <summary>
/// The handle on one sync send's delivery — Java's <c>java.util.concurrent.Future&lt;RecordMetadata&gt;</c>
/// as returned by <c>Producer.send</c>. <see cref="Get()"/> blocks until the record is acknowledged or
/// fails (Java's <c>future.get()</c>).
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><b>Send does not wait for delivery.</b> <see cref="IProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue})"/>
/// returns once the core has accepted the record (it blocks only while <c>buffer.memory</c> is full, up to
/// <c>max.block.ms</c> — Java's <c>send</c> blocking). The key and value buffers are reusable on return.</item>
/// <item><b>Get blocks the calling thread</b> on a managed latch — not a <c>Task</c>, so this is not
/// sync-over-async. Call it any number of times from any number of threads; every call returns the
/// same <typeparamref name="T"/> or rethrows the same exception.</item>
/// <item><b>Failure.</b> <see cref="Get()"/> rethrows the send's exception itself (a <see cref="KafkaException"/>
/// for a delivery failure). Java wraps it in <c>ExecutionException</c>; .NET has no such type.</item>
/// <item><b>Delivery callback.</b> With <c>Send(record, callback)</c> the callback runs on the producer's
/// send-completion thread, before <see cref="Get()"/> returns (Java's ordering).</item>
/// <item><b>Inside a delivery callback</b>, calling <see cref="Get()"/> for a not-yet-completed send of the
/// same producer throws <see cref="InvalidOperationException"/> instead of deadlocking.</item>
/// <item><b>Fire-and-forget is fine.</b> Discarding the value leaks nothing — it holds no native
/// resource — and the callback still fires.</item>
/// <item><b>Order.</b> One producer completes its sends in the order <c>Send</c> returned, so a slow
/// partition delays later completions on the same producer (§D1).</item>
/// <item><b>default.</b> <see cref="Get()"/> on <c>default</c> throws <see cref="InvalidOperationException"/>.</item>
/// <item><b>Equality.</b> Two values are equal exactly when they came from the same <c>Send</c>.</item>
/// <item><b>Naming deviation.</b> Java's <c>org.apache.kafka.common.KafkaFuture</c> is the Admin result type,
/// which this binding maps to <see cref="System.Threading.Tasks.Task{TResult}"/>; this name pairs the sync
/// handle with <see cref="AsyncKafkaFuture{T}"/>.</item>
/// </list>
/// </remarks>
public readonly struct KafkaFuture<T> : IEquatable<KafkaFuture<T>>
{
    private readonly SyncCompletion<T>? _completion;

    internal KafkaFuture(SyncCompletion<T> completion) => _completion = completion;

    /// <summary>Java <c>Future.get()</c>.</summary>
    /// <exception cref="InvalidOperationException">A default value, or (D9) a call on the owning
    /// pump thread for a send that has not completed.</exception>
    public T Get();

    // No Get(TimeSpan) this phase (D7 deferred, FU-4: a timed or cancellable Get, with a CancellationToken).

    public static bool operator ==(KafkaFuture<T> left, KafkaFuture<T> right) => left.Equals(right);
    public static bool operator !=(KafkaFuture<T> left, KafkaFuture<T> right) => !left.Equals(right);
    public bool Equals(KafkaFuture<T> other) => ReferenceEquals(_completion, other._completion);
    public override bool Equals(object? obj) => obj is KafkaFuture<T> other && Equals(other);
    public override int GetHashCode() => _completion?.GetHashCode() ?? 0;
}
```

The default message is "This KafkaFuture is a default value and carries no send; only IProducer.Send returns a usable one." It is the twin of `AsyncKafkaFuture`'s.

**`IProducer<TKey,TValue>` (and `KafkaProducer`, `MockProducer`):**

```csharp
KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey, TValue> record);
KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, IDeliveryCallback callback);
```

**Internal shape (Actor-level; the Critic checks it against S-1/S-2):**
- **`SyncCompletion<T>`** (internal sealed).
  - It locks on `this`. It is unreachable by users, so this saves the 24 B lock object.
  - It holds `T _value`, `ExceptionDispatchInfo? _error` and `volatile bool _done`, plus `SendCompletionPump? _owner` for D9.
  - `TrySetResult` / `TrySetException`: the first one wins and returns `bool`. Each sets the state and then calls `Monitor.PulseAll`.
  - `Get` has a fast path on `_done`. Otherwise it takes the lock, applies the D9 check, and loops on `Monitor.Wait`. (No timed `Get`; D7 deferred.)
  - Never `Task`, `ManualResetEventSlim` per send, or `TaskCompletionSource`.
- **`PendingEntry`** (private abstract, nested in the pump), with `int Count`, `Fault(Exception)` and `DestroyFutures()`. `PendingSendBatch : PendingEntry` is unchanged in content. `PendingSyncSend : PendingEntry` holds `(IntPtr future, SyncCompletion<RecordMetadata>, DeliveryRegistration?)`.
- **`SendCompletionPump.EnqueueSingle(...)`** goes through the **same** private gate as `Enqueue`, so the residual 1 site stays one site.
- **`ProcessSingle`**: the read/fire/throw part of today's `NativeProducer.Send` (`:714-791`), **moved**. It calls the singular `FutureRecordMetadataGet`, then `CopyOut`, then `delivery?.Fire`, then `TrySetResult` or `TrySetException`, and finally `FutureRecordMetadataDestroy`.
- **`NativeProducer.Send`**, in order:
  1. `ThrowIfClosed`, then `EnsurePump`.
  2. Allocate the completion and the entry **before** `Producer_send`, which narrows the post-acceptance window to the enqueue alone.
  3. `Producer_send`.
  4. `EnqueueSingle`. On a throw: destroy the future and rethrow. This is the new sync pre-handoff residual.

---

## 4. Usage, before → after

```csharp
// 4.1 Blocking send (the old meaning)
RecordMetadata m = producer.Send(r);                          // before
RecordMetadata m = producer.Send(r).Get();                    // after

// 4.2 Pipelining — new; Java's idiom
var pending = new List<KafkaFuture<RecordMetadata>>();
foreach (var r in records) pending.Add(producer.Send(r));     // returns on acceptance
foreach (var f in pending) f.Get();                           // in send order

// 4.3 Callback, fire-and-forget
producer.Send(r, callback);       // before: blocked; callback inline on this thread
_ = producer.Send(r, callback);   // after: returns on acceptance; callback on the pump thread

// 4.4 Manual mock (D13)
var t = Task.Run(() => mock.Send(r)); mock.CompleteNext(); t.Wait();   // before: needs a second thread
var f = mock.Send(r); mock.CompleteNext(); RecordMetadata m = f.Get(); // after: one thread

// 4.5 THE compile-silent change (D12)
producer.Send(r);   // before: blocked until delivered. After: fire-and-forget. The repo has none left.

// 4.6 Inside a delivery callback
producer.Send(retry);         // fine — queued
producer.Send(retry).Get();   // InvalidOperationException (D9); Java would deadlock
```

---

## 5. Semantics that must survive — guard tests (names unchanged; retyped in S3)

| Semantic | Guard test(s) |
|---|---|
| Key/value buffers are reusable when `Send` returns (S-2's reason to keep the inline send) | `PublicSyncProducerSendAllocationBudgetTests.Send_MutatingBufferAfterSend_ProducesUnchangedRecord` |
| Zero-copy: no value-sized copy | `…Send_PerRecordAllocation_HasNoValueSizedCopy` (marginal, kept) |
| Pre-acceptance failures still throw **out of `Send`** with no callback | `PublicProducerDeliveryCallbackTests.NoCallback_OnSynchronousThrow_{AfterDispose,SerializerThrows,NullRecord}`; `NullCallback_*`; `NullRecord_IsReportedBeforeANullCallback`; `PublicProducerSendClosedCheckTests`; `PublicProducerSendSynchronousThrowTests` (sync part) |
| Exactly one callback per record; placeholder metadata on failure; a throwing callback is swallowed and traced | `ExactlyOnce_*`, `Failure_*`, `ThrowingCallback_*` (Sync flavor) |
| Callback before completion (Java order) | `Ordering_Sync_CallbackRunsBeforeSendReturns` / `…BeforeSendThrows`. Their probe moves to "when `Get` returns / throws"; the names are kept, and S4 adds the thread assertion (C3) |
| Reentrant send from a callback | `Callback_MaySendAgainFromInsideItself` (Sync), via `Task.Run` (F20) |
| Teardown releases a waiter and stays idempotent | `PublicSyncProducerTeardownTests` (all 9). `SyncClose_WithConcurrentBlockedSend_ReleasesSender` keeps its name; "blocked" now means a thread blocked in `Get()` |
| The async batch path is untouched | `SendCompletionPumpGateTests`, `…PreStopDrainTests`, `SendCompletionGroupingTests`, `…DrainCapTests`, `SendAccumulator*Tests`: **no assertion edits** |

---

## 6. Caller and test inventory (sweep at `d6ce0512`, `obj/`/`bin/` excluded)

| File | `.Send(` sites | Change |
|---|---|---|
| `src/…/IProducer.cs`, `KafkaProducer.cs`, `MockProducer.cs` | 2 + 2 + 2 decls | Retype; docs (type-truth in S3, narrative in S5) |
| `src/…/Internal/NativeProducer.cs` | `Send :714-791` | `EnsurePump` plus enqueue; the read body moves to the pump; docs `:660-713` |
| `src/…/Internal/SendCompletionPump.cs` | — | `PendingEntry`, `PendingSyncSend`, `EnqueueSingle`, `ProcessSingle`; teardown paths; `[ThreadStatic]` current pump |
| `src/…/Interop/NativeMethods.cs` | xmldoc `:2440-2461` | "the sync `Send` path" → "the pump's single-send read" (doc only, no signature change) |
| `grpc-server/ProducerServiceImpl.cs` | 2 (`:188-190`) | `.Get()` — stays **unbounded** (D7.1 dropped; M11/P8 D-3 stands); the comment block `:165-187` rewritten for the new mechanics only (§8 S3); `CallbackLog.cs:45-46` comment (the sync callback now runs on the pump thread) |
| `tests/Performance/PerfV3/V3Backends.cs` | 2 | Step 1: `.Get()` (S3); Step 2: the handle (S6) |
| `PublicProducerDeliveryCallbackTests.cs` | 22 (both flavors) | Sync arm `:953` → `.Get()` inside `Task.Run`; `:616` → `typeof(KafkaFuture<RecordMetadata>)`; the `Ordering_Sync_*` probes |
| `PublicProducerDeliveryCallbackTfmSmokeTests.cs` | 3 | `.Get()` |
| `PublicProducerSendClosedCheckTests.cs` | 8 | Mostly `Assert.Throws` on pre-acceptance paths, so unchanged; check each one |
| `PublicProducerTypedSendTests.cs` | 11 | `.Get()` |
| `PublicSyncProducerMockControlTests.cs` | 4 | `.Get()`; the two-thread pattern can become single-thread (D13), but only where the test is *about* the pattern |
| `PublicSyncProducerSendAllocationBudgetTests.cs` | 4 | `.Get()` where metadata is read; `MeasureSends` keeps `_ =` (the measurement is the send) |
| `PublicSyncProducerSendTests.cs` | 10 | `.Get()` |
| `PublicSyncProducerTeardownTests.cs` | 3 | `.Get()`; the `:163` test's thread A runs `Send(...).Get()` |
| `PublicSyncProducerTfmSmokeTests.cs` | 3 | `.Get()` |
| `PublicProducerSendSynchronousThrowTests.cs` | 2 sync (+6 async, untouched) | Check each: pre-acceptance throws stay on `Send` |
| `PublicProducerMetricsTests.cs`, `PublicSyncProducerPeripheralTests.cs`, `PerfV2/V2ProducerBackends.cs` | 0 | None in S3 (V2 changes in S6) |

**The compiler lists most of the sites** (assigning a `KafkaFuture` to `RecordMetadata` fails). The D12 sites do **not** fail to compile. They are found with `/usr/bin/grep -nE '^\s*[A-Za-z_][A-Za-z0-9_.]*\.Send\(' <files>` and by reading each multi-line call.

---

## 7. Tests (named; counts confirmed per TFM)

**S1, the type and the latch.** New `PublicKafkaFutureTests.cs` and `Interop/SyncCompletionTests.cs`:
- **N1** `Default_Get_ThrowsInvalidOperationException_WithItsMessage`.
- **N2** `Get_AfterSetResult_ReturnsTheSameInstance_OnEveryCall_FromManyThreads`.
- **N3** `Get_AfterSetException_RethrowsTheSameInstance_Unwrapped`: the type, the message, `ReferenceEquals`, twice.
- **N4** `Get_BlocksUntilCompleted_ThenReleasesEveryWaiter` (K waiters, all released, bounded by `TestTimeout`).
- **N5** `FirstCompletionWins_BothOrders`.
- **N6** `SetRacesGet_NoLostWakeup`: a 1000-iteration loop, set on one thread and `Get` on another.
- *(N7, N8: removed with D7 — no `Get(TimeSpan)` this phase. The numbers are not reused.)*
- **N9** `Equality_IsReferenceIdentityOfTheCompletion` (`==`, `!=`, both `Equals`, `GetHashCode`, default).
- **N10** `Shape_IsAPublicReadonlyStruct_WithOnlyTheApprovedMembers` (mirrors `PublicAsyncKafkaFutureTests.cs:147`).

**S2, the pump's single path.** New `Interop/SendCompletionPumpSingleTests.cs`. Real mock futures where the future is read, and `IntPtr.Zero` where it is not (F19):
- **T1** `Single_Resolved_CompletesWithTheMetadata`.
- **T2** `Single_Failed_FiresThePlaceholderThenFaults_InJavaOrder`.
- **T3** `Single_FiresOnThePumpThread` (asserts the thread name).
- **T4** `Singles_CompleteInFifoOrder` (D1 (a)).
- **T5** `CloseGate_ThenEnqueueSingle_FaultsInPlaceSynchronously_WithTheTeardownMessage_AndDoesNotFire`.
- **T6** `Stop_WithAQueuedSingle_FaultsItWithTheTeardownMessage_AndDoesNotFire`.
- **T7** `WaitForQueueDrain_SeesSingles` (the pre-stop drain pattern: a resolvable queued single is completed, not faulted).
- **T8** `RunLoopFault_FaultsASingle_WithThePumpFailureMessage`. This uses an existing fault-injection seam if there is one. **If none exists, the Actor reports that, and the row becomes a Critic structural check; no seam is invented without the Manager's approval.**
- **T9** `DrainedSendCount_CountsASingleAsOne`.
- **T10** `Get_OnTheOwningPump_NotDone_Throws_WithItsMessage`.
- **T11** `Get_OnTheOwningPump_AlreadyDone_Returns`.
- **T12** `Get_OnAnotherProducersPump_DoesNotThrow` (D9).

**S3, the switch.**
- **S3-1** `SyncSurfaces_PlainSend_ReturnsKafkaFuture` (the missing twin of `:649`).
- The `:616` retype.

Everything else in S3 is the §5 retypes.

**S4, callback and error semantics.** New `PublicSyncProducerKafkaFutureTests.cs`, plus edits to the callback tests:
- **C1** `Send_ReturnsBeforeDelivery_OnAManualMock_ThenGetAfterCompleteNext` (D13, single thread).
- **C2** `Callback_Sync_RunsOnThePumpThread_NotTheCaller` (D3).
- **C3** `Ordering_Sync_*` extended: the callback ran on the pump thread before `Get` returned or threw.
- **C4** `Callback_Sync_SharedInstance_IsNeverEnteredConcurrently_ByOneProducer`. Use N threads × M sends with a max-concurrency probe that must equal 1. This is the retired sub-divergence turned into a guarantee.
- **C5** `ApiFailure_Sync_SendReturns_CallbackFires_GetThrowsTheSameInstance` (D2).
- **C6** `GetInsideCallback_SameProducer_ThrowsInvalidOperation_AndTheSendStillCompletes` (D9).
- **C7** `SendInsideCallback_GetFromAnotherThread_Completes`.
- **C8** `Dispose_WithAPendingFuture_ReleasesGet`. The callback fires if and only if the completion was read.
- **C9** `SyncOnlyProducer_StartsThePumpOnFirstSend_AndJoinsItAtDispose` (D4, through an internal probe; InternalsVisibleTo already exists).
- **C10** `SendAfterDispose_ThrowsObjectDisposed_AndStartsNoPump`.
- **C11** `RacingFirstSendAndDispose_NeverLeavesARunningPump` (200 iterations, bounded).
- **C12** `FireAndForget_DiscardedFutures_StillFireEveryCallback`. Bounded wait after `Flush`, per D8 (b).
- *(C13, C14: removed with D7 / D7.1. The numbers are not reused.)*
- **A1** `Send_AbsoluteCallerThreadBudget` (D11; net8.0 and net10.0; best of N; the figure is in the message).

**S6, perf-unit.** In `Confluent.Kafka.PerformanceTests`, with a fake backend:
- **P1** `RunSync_RecordsCompletionsInSendOrder`.
- **P2** `RunSync_QueueCapacity_IsTwoGiBOverMessageSize`.
- **P3** `RunSync_FailedGet_IsCountedAndTheRunContinues` (Python's `record_completed_calls`).
- **P4** `RunSync_Warmup_WaitsEachSendInline`.

---

## 8. Slicing, gates, Critic points

| Slice | Content | Behaviour change | Commit |
|---|---|---|---|
| **S0** | The S1 Actor, **before any edit**: rebuild the native library, then record the baselines. These are unit counts per TFM, soak, perf-unit, gRPC arm counts (`--list`-counted, F16) and the Mode A counts. Nothing is committed | — | — |
| **S1** | `KafkaFuture.cs` (§3, full xmldoc) + `Internal/SyncCompletion.cs` + N1–N10. Production does not use them yet | None | `feat(dotnet): KafkaFuture<T>, the sync send's delivery handle (M11/P4.2 S1)` |
| **S2** | The pump: `PendingEntry`, `PendingSyncSend`, `EnqueueSingle` (shared gate), `ProcessSingle`, and every teardown path covering singles (CloseGate, WaitForQueueDrain, DrainAndFaultRemaining, Stop, Enqueue-after-stop, the RunLoop catch, `_drainedSends`), plus the `[ThreadStatic]` current pump. Add T1–T12, numbered residual notes **at the new sites**, and the async suites green and unedited. **Not wired**: the caller's-thread read in `NativeProducer.Send` still exists. That temporary duplication is deleted in S3, and the Critic checks it | None (unused) | `feat(dotnet): single-future path through the send-completion pump (M11/P4.2 S2)` |
| **S2m** | Mutation run M1–M12 (§9), as a separate Actor spawn. Nothing is committed unless a test is strengthened (then a `fixup!`). The Actor reports the literal change, the target, the regime and the fail ratio for each row | — | — |
| — | **Critic 93, pass 1 (S1 + S2 + the S2m record). Teardown focus** (§9). If needed, split into two spawns: (a) the latch and the type, (b) the pump and its teardown | | |
| **S3** | **The switch.** The retype on IProducer, KafkaProducer and MockProducer; `NativeProducer.Send` → `EnsurePump` + allocate + `Producer_send` + `EnqueueSingle`, with the caller-thread read **deleted**; the mock; the §6 sweep under D12; S3-1 and `:616`; the sync servicer (`ProducerServiceImpl.cs` ~`:165-190`), which stays **unbounded** (D7.1 dropped, M11/P8 D-3 stands):<ul><li>the call becomes `producer.Send(...).Get()`;</li><li>its comment block is rewritten **for the new mechanics only**: the wait is now `Get()`, and the callback runs on the pump thread but **before** `Get()` returns (Java's order: callbacks fire before the latch opens), so the entry is still in `_callbackLog` when the response is built;</li><li>the D-3 rationale is kept, and its "the only way to bound it is `Task.Run`" reasoning is updated only as far as needed to stay true (there is no timed `Get` yet, FU-4);</li><li>`CallbackLog.cs:45-46` is corrected (the sync callback runs on the pump thread, not the handler thread);</li></ul>`V3SyncProducerBackend` `.Get()` (perf Step 1); **type-truth docs only** | **Yes** | `feat(dotnet)!: sync Send returns KafkaFuture<RecordMetadata> (M11/P4.2 S3)` |
| **S4** | C1–C12 and A1 (D11 figure measured and applied), plus any production refinement those tests force within the approved D-rows. Re-walk the residual sites on both threads and renumber the code notes | Tests, and fixes inside the rulings | `test(dotnet): sync KafkaFuture callback and error semantics; allocation budget (M11/P4.2 S4)` |
| **S4m** | Mutation run M13–M20 (§9), same rules as S2m | — | — |
| — | **Critic 93, pass 2 (S3 + S4 + the S4m record):** the sweep (D12 re-grep), the semantics, the docs' type truth | | |
| **S5** | Narrative docs:<ul><li>`IProducer` remarks (`:50-66`, `:81-88`, `:114-120`, `:147-174`, the S-3 rejected options in one paragraph, the D1/D8/D12 limitations, the D13 pattern);</li><li>`KafkaProducer` / `MockProducer` class docs and `CompleteNext`;</li><li>`IDeliveryCallback` remarks `:45-66` and **"Recorded residuals" `:167-270` re-walked**: residuals 1, 2 and 4 lose "(async only)"; residual 3's "sync shares the window" note is replaced; the site count is whatever the walk yields (expected five, stated as an output, not an input);</li><li>`NativeProducer` `:660-713` and the `StopPump` comments;</li><li>the pump class doc;</li><li>`NativeMethods :2440-2461`.</li></ul>Then the approved §12 E-rows and F-rows, **verbatim**. STATUS | Docs, rules | `docs(dotnet): KafkaFuture in the producer docs and binding rules (M11/P4.2 S5)` |
| — | **Critic 93, pass 3 (light; S5):** the docs match the approved text; the twins are swept; the residual walk was reproduced independently | | |
| — | **User: perf Step 1** (§11 P-1). The loop pauses here | | |
| **S6** | Perf Step 2 (S-5):<ul><li>`IProducerBackend.Send` returns a sync handle (`PerfSendHandle`, struct, `PerfRecordMetadata Get()`);</li><li>`RunSync` becomes a port of Python's `main()`: the send loop with the existing limiter; a `BlockingCollection<(handle, startMs)>` bounded at `2 GiB / messageSize`; a recorder thread calling `Get()` in order and counting failures without stopping; after the loop, the recorder drains what is left and joins; an inline warm-up with a 0.1 s sleep;</li><li>`V2SyncProducerBackend` becomes a line-for-line port of `CompatibleProducer`: `Produce` + a delivery handler + a poll thread + a `BufferError` → 1 ms retry, returning the handle;</li><li>remove the D5 remark (`ProducerBenchmark.cs:51-58`, `Backends.cs:71-84`, `V2ProducerBackends.cs:153`);</li><li>state in the docs that "V2 sync" means CKD's `Produce` + delivery handler waited in order, since neither CKD nor confluent-kafka-python has a sync producer;</li><li>(D14) the knob, if approved;</li><li>P1–P4.</li></ul> | Perf harness | `perf(dotnet): pipelined sync producer benchmark, ported from Python's main() (M11/P4.2 S6)` |
| — | **Critic 93, pass 4 (S6):** a line-by-line port against `producer_performance_test.py`; the smoke test's assertions | | |
| — | **User: perf Step 2** (§11 P-2): the new baselines | | |
| Close | Manager:<ul><li>STATUS entry (E10), §16 evidence;</li><li>archive `COMMENTS.DONE.93.md` here and reset `COMMENTS.93.md`;</li><li>memory.</li></ul>(`marked_classes.txt` does not apply to the binding) | | |

**DoD gates, every slice:**
1. `make -C <repo>/dotnet build-dotnet` gives `0 Error(s)`. This covers netstandard2.0, the net462 compile, net8.0 and net10.0.
2. `make -C <repo>/dotnet test-dotnet` runs `dotnet format --verify-no-changes`, unit net8.0 + net10.0, and soak. Assert a positive `Passed:` per TFM, no `Failed:`, and no `Test Run Aborted`.
3. `make -C <repo>/dotnet perf-unit-test-dotnet` stays at 39/TFM until S6. After S6 it is 39 plus the new tests.
4. **S3 and S6 additionally:**
   - Build the projects outside the `.sln` (`dotnet build -c Release dotnet/grpc-server`, `dotnet build -c Release dotnet/tests/Performance/PerfV3`, and PerfV2 in S6). Each must give `0 Warning(s) 0 Error(s)`, plus `dotnet format <project> --verify-no-changes`.
   - Run `make -C <repo> verify-dotnet-macos-docker`, which runs native gRPC `__grpc_dotnet` + `__grpc_dotnet_async`. **Compare the arm counts with the S0 baseline, never the exit code.**
   - Run `make -C <repo> test-integration-perf-dotnet` where Docker is available. `Producer_Smoke_Sync` must pass.
5. **Mode A proof** at S3 and at close:
   - `git diff --stat d6ce0512..HEAD -- . ':!dotnet'` is empty;
   - `git grep -c 'internal static extern' HEAD -- dotnet/src` gives 219 + 357;
   - the diff has no `[DllImport]` line.
6. Every figure is reported with **its definition and the command that produced it**.

**Process:**
- Fixes are `fixup!` commits that reference the slice commit.
- Each pass repeats until `COMMENTS.93.md` is empty.
- **One Actor spawn per slice.**
- Work on `prashah_dev_dotnet_binding` and **do not push** (the user pushes with `git push-external`).
- Every commit message ends with the `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` line.

**Effort estimate:** **16–22 commits, 4 Critic passes, 8–15 findings, about 6–10 h of loop time.** This is about P3.5's size (18 commits, 13 findings), not P3.6's (9 commits, 0 findings, about 2 h). The drivers are:
- the teardown surface (P3.5-like);
- the residual re-walk, which ran through several rounds in M14/P1;
- the ~70-site sweep with D12's silent sites;
- a fourth, perf-harness pass.

---

## 9. Critic 93 focus and the mutations it must demand

**Focus (in priority order):**
1. **Teardown of singles.** Walk every path a `PendingSyncSend` can take after `Producer_send` accepted it: (a) enqueued then processed; (b) gate closed, faulted in place; (c) queued at `Stop`, drained and faulted; (d) the RunLoop catch; (e) a throw in `EnqueueSingle`, destroyed on the caller. For each one, answer:
   - is the future destroyed exactly once and only by its owner?
   - is the completion always finished?
   - does the callback fire exactly when the completion was read?
   - is the message right?
2. S-1. No `IntPtr` is reachable from `KafkaFuture`, and no `Task` sits in the sync latch.
3. Lock and latch ordering for `EnsurePump` against `TryBeginClose` / `PumpToStop` (no pump thread is created after `PumpToStop`).
4. `SyncCompletion`'s memory model: state set before `PulseAll`, the `_done` fast path, no lost wake-up, first-wins.
5. D9 is owner-keyed (not "any pump"), and applies only when the future is not done.
6. The D12 sweep, re-grepped independently.
7. Java order (set → callbacks → open) and exactly-once on the single path.
8. The async batch path is unchanged; its suites have no assertion edits.
9. Doc twins (S5): the residual enumeration is reproduced by walking the code, not by recall (ffi §A6 form C's method).

| # | Literal mutation | Must be caught by |
|---|---|---|
| M1 | `KafkaFuture.Get` returns `default` for a default value | N1 |
| M2 | `TrySetResult` pulses before it sets `_value` / `_done` | N6, N2 |
| M3 | `Monitor.Pulse` instead of `PulseAll` | N4 (bounded) |
| M4 | A second `TrySet*` overwrites the first | N5 |
| M5 | `Get` throws `new AggregateException(cause)` / a fresh copy | N3 |
| ~~M6~~ | *(removed with D7 — no `Get(TimeSpan)`)* | — |
| M7 | `EnqueueSingle` skips the `_stopped` check | T5 |
| M8 | `DrainAndFaultRemaining` skips `PendingSyncSend` | T6 |
| M9 | The RunLoop catch faults batches only | T8, or a structural check if there is no seam |
| M10 | `ProcessSingle` calls `TrySetResult` before `Fire` | T2, C3 |
| M11 | `ProcessSingle` drops `FutureRecordMetadataDestroy` | Structural (Critic); an equivalent-mutant risk, stated |
| M12 | The D9 guard is keyed on "any pump thread" | T12 |
| M13 | The D9 guard is removed | T10, C6 (timeout-bounded, so it fails rather than hangs) |
| M14 | `EnsurePump` skips `ThrowIfClosed` or takes no lock | C10, C11 |
| M15 | `NativeProducer.Send` allocates the completion **after** `Producer_send` | Structural (Critic: statement order) |
| M16 | `StopPump` returns early when there is no accumulator, skipping `Stop` | C9, the `:163` teardown test |
| M17 | The callback fires inline on the caller (an old-path remnant) | C2, C4 |
| M18 | An `ApiException` failure thrown from `Send` (not `Get`) | C5 |
| M19 | `KafkaFuture` is boxed (e.g. returned as `object` internally) | A1 (+16 B headroom catches 24 B) |
| M20 | One retyped test silently loses its `.Get()` (D12) | The Critic's re-grep, not a test |

---

## 10. Context budget per agent

The binding constraint in past phases was **reasoning volume**, not only I/O. Each spawn is one slice.

**Do not read whole:**
- `ffi-marshalling.md` (2401 lines), `dotnet/CLAUDE.md` (1094), `design/current/STATUS.md` (3000+);
- `Internal/NativeProducer.cs` (1751), `Interop/SendAccumulatorTests.cs` (2461);
- `SendCompletionPump.cs` (955): read the **code** ranges in F3 and skip the class doc `:15-130`;
- `IDeliveryCallback.cs` (352): ranges only;
- the P3.6 PLAN (574), except the sections cited.

**Never open `target/`, `obj/` or `bin/`.**

**Reading per slice:**
- **S1:** this PLAN's §1–§3 and §7 N1–N10; `AsyncKafkaFuture.cs` (119, whole); `PublicAsyncKafkaFutureTests.cs` (whole); `.editorconfig:140-177`. About 450 lines.
- **S2:** `SendCompletionPump.cs:140-955`, code only; `NativeProducer.cs:714-791`; `SendCompletionPumpGateTests.cs` (120); `SendCompletionPumpPreStopDrainTests.cs:40-120`. About 900 lines; the heaviest slice.
- **S3:** `NativeProducer.cs:660-791`, `:996-1020`, `:1176-1275`; the three producer files in ranges; `ProducerServiceImpl.cs:160-200`; `AsyncProducerServiceImpl.cs`, only around its 120 s cap; `V3Backends.cs:20-50`. For the tests, **let the compiler list the sites**, then grep the D12 statements.
- **S4:** the S3 test files in ranges; `PublicSyncProducerSendAllocationBudgetTests.cs` (156).
- **S5:** the exact lines in §6/§12. Grep, then read the range.
- **S6:** `ProducerBenchmark.cs`, `Backends.cs`, `V2ProducerBackends.cs` (whole, each small); `producer_performance_test.py:133-170`, `:597-773`.

**Output bounds:**
- Redirect every build and test command to a scratch log.
- Read only `grep -E 'Passed!|Failed!|error CS|Test Run Aborted|Total tests' <log> | head -n 40` and `tail -n 30 <log>`.
- `grep -c` before `grep -n`; use `-m` caps. Summarize anything over about 100 lines.

**Shell traps — put them in every brief verbatim:**
1. Start every Bash call with `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$HOME/.dotnet:$PATH"`.
2. `grep` may be ugrep. Use `/usr/bin/grep`, and exclude `obj/`/`bin/`.
3. `sed` may be GNU or BSD (the PM shell's `/usr/bin/sed` is BSD); prefer `awk`.
4. `cat` may be shadowed; use `/bin/cat`.
5. zsh does not word-split `$var`. Quote globs (`--include='*.cs'`). Use `echo '----'`. Write `"${ref}:rust/x"`. Never name a loop variable `path`.
6. A filter that matches zero tests exits 0. Confirm the **count**.
7. An aborted `dotnet test` exits 0; grep for `Test Run Aborted`.
8. A `-c Release` build and a `--no-build` test must use the same configuration.
9. Never use `--no-build` after a failed build. For mutation runs, rebuild and confirm the mutant compiled before reading a result.

**Git hygiene:**
- `git -C <abs path>` only; no bare `cd`; never bare `git stash`.
- Never edit `.claude/worktrees/*`.
- Leave the user's untracked agent-memory files and `tests/Performance/RUNNING-LOCALLY.md` alone.

---

## 11. Perf plan — **the user runs it; agents do not**

**P-1 (Step 1, after S3): serial sync before and after, plus an async guard.**
- Before = `d6ce0512`; after = the S3 commit (the phase has no Rust change, so the native library is identical).
- 2 reps each, alternating B/A/B/A, in one session against the same local broker.
- Async R1 is included because S2 touches the pump that the async path shares.

```
R=/Users/pranavshah/WorkSpace/Confluent/example-confluent-kafka-rust
B=/Users/pranavshah/WorkSpace/Confluent/p42-before
O=$HOME/perf-p42; mkdir -p "$O"
git -C "$R" worktree add --detach "$B" d6ce0512          # outside .claude/worktrees/

# S1 sync, max rate (serial) — before, then after; repeat once more with -2 names
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=False TEST_DURATION_SECONDS=60 VALUE_SIZE=1024 PARTITIONS=6 make -C "$B" producer-perf-test-dotnet CLIENT_VERSION=3
cp "$B/dotnet/metrics.jsonl" "$O/S1-before-1.jsonl"
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=False TEST_DURATION_SECONDS=60 VALUE_SIZE=1024 PARTITIONS=6 make -C "$R" producer-perf-test-dotnet CLIENT_VERSION=3
cp "$R/dotnet/metrics.jsonl" "$O/S1-after-1.jsonl"

# S2 sync, 100 rps (the smoke test's load, 60 s) — before, then after; repeat once more
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=False LIMIT_RPS=100 TEST_DURATION_SECONDS=60 VALUE_SIZE=2048 PARTITIONS=6 make -C "$B" producer-perf-test-dotnet CLIENT_VERSION=3
cp "$B/dotnet/metrics.jsonl" "$O/S2-before-1.jsonl"
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=False LIMIT_RPS=100 TEST_DURATION_SECONDS=60 VALUE_SIZE=2048 PARTITIONS=6 make -C "$R" producer-perf-test-dotnet CLIENT_VERSION=3
cp "$R/dotnet/metrics.jsonl" "$O/S2-after-1.jsonl"

# R1 async guard, max rate — before, then after; repeat once more
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=True TEST_DURATION_SECONDS=60 VALUE_SIZE=1024 PARTITIONS=6 make -C "$B" producer-perf-test-dotnet CLIENT_VERSION=3
cp "$B/dotnet/metrics.jsonl" "$O/R1-before-1.jsonl"
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=True TEST_DURATION_SECONDS=60 VALUE_SIZE=1024 PARTITIONS=6 make -C "$R" producer-perf-test-dotnet CLIENT_VERSION=3
cp "$R/dotnet/metrics.jsonl" "$O/R1-after-1.jsonl"

git -C "$R" worktree remove "$B"
```

`metrics.jsonl` is overwritten by every run, so copy it after **every** run. The first `make -C "$B"` builds the Rust native library in the before-worktree (slow, once).

**Acceptance (P-1, after vs before):**
- S1 sync throughput within ±5 %; p50 no more than +1 ms (the handoff adds a thread hop, which is µs against a ms round trip).
- S2 p99 ≤ 70 ms, as the smoke gate requires.
- R1 throughput within ±5 %, CPU no more than +10 %, RSS within ±15 %.

A miss is reported to the Manager with the files.

**P-2 (Step 2, after S6): new baselines, not an A/B.**
- Before (serial) = the S5 commit; after (pipelined) = the S6 commit.
- Run S1 and S2 with `CLIENT_VERSION=3`, then the same two with `CLIENT_VERSION=2`, 2 reps each.
- Optionally, Python's sync run for comparison: `ASYNC=False … make -C "$R" producer-perf-test-python`, with your usual env.
- Sync max-rate throughput is expected to rise by orders of magnitude; that is the point of the port.

**Acceptance (P-2):** `Producer_Smoke_Sync` passes, S2 p99 ≤ 70 ms, and the figures are recorded as the new sync baselines in §16. No analysis prose goes into any doc.

---

## 12. Rule-file and doc edits — **exact text for the user to approve**

**Ruling (2026-10-07):** E1–E10 and F1–F8 are **approved as written**, with only the D7/D7.1 text removed (the "M11/P8 D-3 if D7.1" supersession in E10). That includes the user-authored rows E2, E5, F4 and F8. Nothing is added or reworded beyond that removal; a row needing a substantive change goes back to the user. **X1/X2 were not ruled on and are NOT applied**; they are listed as follow-ups in §14.

Provenance comes from `git blame` at `d6ce0512` and `git log --all -S` through the pre-consolidation history. **"User"** means text authored as "Pranav Shah <prashah@confluent.io>" (`42583351`, `959df4744`), or a row P3.6 already treated as User. Everything else is agent text that reached HEAD through the consolidation `0204437a`. For User rows the proposal **appends** rather than rewriting.

**`dotnet/CLAUDE.md`**

| # | Location | Provenance | Before → after |
|---|---|---|---|
| E1 | `:188-191` §3 sketch | agent (`29c79d1a1` M11/P5; `cb33fe1e1` M14/P1) | `:188` "(blocking mirror)" → "(sync mirror)". `:189-191` "`// RecordMetadata Send(ProducerRecord<TKey,TValue>) blocks (= Java send(record).get());` / `// RecordMetadata Send(ProducerRecord<TKey,TValue>, IDeliveryCallback) — Java's second send signature,` / `// with the callback fired INLINE on the caller's thread before Send returns (M14/P1); void Flush() /`" → "`// KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey,TValue>) returns on acceptance (blocks only on` / `// buffer.memory, ≤ max.block.ms); future.Get() = Java send(record).get() (M11/P4.2);` / `// KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey,TValue>, IDeliveryCallback) — Java's second send` / `// signature, the callback fired on the send-completion pump thread before Get() returns (M11/P4.2); void Flush() /`" |
| E2 | `:486` idiom row `Future<RecordMetadata>` | **User** (`42583351`; P3.6 E3 suffix, agent `052daf8e`) | Column 2: **append** "; on `IProducer.Send` it is `KafkaFuture<RecordMetadata>` (a struct over a managed latch whose blocking `Get()` is Java's `get()`; the name pairs with `AsyncKafkaFuture` — Java's `org.apache.kafka.common.KafkaFuture` is the Admin type, mapped to `Task<T>` — M11/P4.2)". Column 3: **append** "; the sync producer's latch is completed by the same pull-pump (M11/P4.2)" |
| E3 | `:487` `send` row | agent (`cbecd70c` P3.5 S5, `052daf8e` P3.6 S4) | Column 2: "; sync `IProducer.Send` unchanged" → "; on `IProducer` it is `KafkaFuture<RecordMetadata>` — `Send` is the block (the core's inline accept, ≤ `max.block.ms`), `future.Get()` is the `Future` (M11/P4.2)". Column 3: "M11/P3.5, M11/P3.6, ffi §A1" → "M11/P3.5, M11/P3.6, M11/P4.2, ffi §A1" |
| E4 | `:496` `Callback` row | agent (`cb33fe1e1`, `cbecd70c`, `052daf8e`) | "which still returns the `RecordMetadata` / `ValueTask<AsyncKafkaFuture<RecordMetadata>>`" → "which still returns the `KafkaFuture<RecordMetadata>` / `ValueTask<AsyncKafkaFuture<RecordMetadata>>`". "Fires on the **pump thread** (async) or **inline on the caller's** (sync), **before** the **delivery** awaiter is released (unordered relative to acceptance)" → "Fires on the producer's send-completion **pump thread** on both surfaces (the sync surface fired inline on the caller's until M11/P4.2), **before** the **delivery** awaiter is released / `Get()` returns (unordered relative to acceptance)" |
| E5 | `:560` "Returns `Future<T>`" row | treat as **User** (P3.6 E6: original author not traced) | Column 2: **append** " The sync `IProducer.Send` returns `KafkaFuture<RecordMetadata>` (a blocking `Get()`) — M11/P4.2." |
| E6 | `:683-703` §4 delivery-callback **Thread** bullet and its ⚠ sub-divergence | agent (`cb33fe1e1` M14/P1) | `:684-686` "…for the async surface — .NET's analogue of the "background I/O thread" Java documents (`Callback.java:20-21`) — and **inline on the caller's thread** for the blocking sync surface, which has no pump." → "…— .NET's analogue of the "background I/O thread" Java documents (`Callback.java:20-21`) — on **both** surfaces (the sync surface ran it inline on the caller's thread until M11/P4.2)." The whole ⚠ paragraph `:689-703` ("Sub-divergence — the *sync* surface gives no non-concurrency guarantee, and Java does. … corrected in the M14/P1 review round.") → "⚠ **Sub-divergence retired (M11/P4.2).** Until M11/P4.2 the sync surface had no pump, so one `IDeliveryCallback` instance handed to concurrent sync `Send` calls was entered on N caller threads at once — a divergence from Java, whose callbacks all run on one I/O thread. The sync surface now completes on the producer's single pump thread too, so one producer's callbacks never overlap each other on either surface. What can still overlap is stated in `IDeliveryCallback`'s remarks: the async send-batch thread's per-record rejection against the pump, and one instance shared across two producers (one pump each)." |
| E7 | `:704` Ordering bullet | agent (`cbecd70c`) | "It runs **before** the delivery awaiter is released / before `Send` returns," → "It runs **before** the delivery awaiter is released / before `Get()` returns," |
| E8 | `:721-725` "Which outcomes fire it" | agent (`cb33fe1e1`) | After "…That is a deviation forced by the ABI, not a choice." **append** " Since M11/P4.2 the sync surface matches that row's *outcome*, as the async surface already did: `Send` returns, the callback fires (on the pump, not the caller), and the future's `Get()` throws." |
| E9 | `:751-753` residual comparison | agent (`cb33fe1e1`) | "The residual *sites* are on the async surface; the **sync** surface, having no pump, shares the on-the-pump window's *shape* — the same read-then-fire gap for its own single record, with the throw propagating out of `Send` instead of faulting a batch." → "Since M11/P4.2 the **sync** surface completes on the same pump, so every residual class applies to it too; its sites are enumerated with the async ones in `IDeliveryCallback`'s remarks." |
| E10 | `design/current/STATUS.md` | Manager, at close-out | Add the M11/P4.2 entry: the supersessions (M11/P4 #1, #2; M14/P1 D3), the D12 migration line, and "Next unused dotnet N = 94". Correct the P3.6 entry's "not pushed" (`origin` = `d6ce0512`) without rewriting history |

**`dotnet/.claude/rules/ffi-marshalling.md`**

| # | Location | Provenance | Before → after |
|---|---|---|---|
| F1 | §A1 diagram `:250`, `:256`, `:257` | agent (`be39ba485` P3.1 lineage) | "`SYNC  Send → Producer_send ───`" → "`SYNC  Send → Producer_send, enqueue ─`" (keep the column alignment). "`completion pump (1 bg thread):`" → "`completion pump (1 bg thread, both paths):`". "`get_all(futures) ─block_on─────►`" → "`get_all / get(sync) ─block_on─►`" |
| F2 | §A1 `:262-264` | agent (`be39ba485`) | "…it polls nothing, and the sync path starts neither thread." → "…it polls nothing, and the sync path starts only the completion pump (M11/P4.2), never the batch thread." |
| F3 | §A1 `:278-279` | agent (`be39ba485`) | "The **sync** path starts neither thread, so a sync-only producer still spins nothing." → "The **sync** path starts only the completion pump — lazily, on its first `Send` (M11/P4.2) — and never the batch thread, so a sync-only producer spins exactly one thread." |
| F4 | §A2 table `:446` `FutureRecordMetadata_t` | **User** (`959df4744`) | Column 4: **append** "; a sync send's single future: the pump's `_destroy` after `_get` (M11/P4.2)" |
| F5 | §A6 form A `:819` | agent (M14/P1 lineage) | "or the sync blocking `FutureRecordMetadata_get`." → "or, for a sync send, the pump's single `FutureRecordMetadata_get` (M11/P4.2)." |
| F6 | §A6 form C `:857-864` | agent (`cb33fe1e1`; `cbecd70c` edit) | "That is the pump thread for the async surface (.NET's analogue of Java's background I/O thread, `Callback.java:20-21`) and the *caller's* thread for the blocking sync surface, which has no pump. Both are legitimate; they are the same two threads that already free the completion's native handles." → "That is the pump thread on both surfaces (.NET's analogue of Java's background I/O thread, `Callback.java:20-21`; the blocking sync surface read it inline on the caller's thread until M11/P4.2) — the thread that already frees the completion's native handles." Then "and before the sync send returns or throws" → "and before the sync future's `Get()` returns or throws" |
| F7 | §A6 anti-pattern `:1016` | agent (`cb33fe1e1`) | "Firing it *after* `TrySetResult` / after the sync return" → "Firing it *after* `TrySetResult` / after the sync completion is set" |
| F8 | §A7 `:1100-1119` | `:1100-1103` and `:1115-1119` **User** (`959df4744`); the ⚠ P3.1 paragraph `:1104-1114` is agent | **No rewrite of the user's lines.** After `:1114` (the end of the ⚠ paragraph, "…different letterings of different questions."), **append**: "⚠ **M11/P4.2 — the sync path now follows this rule too.** Its `Send` still calls `Producer_send` inline (the core copies key and value during the call, so the caller's buffers are reusable on return), then hands `(future, latch, callback)` to this pump and returns a `KafkaFuture<RecordMetadata>`; it no longer blocks in `_get` on the caller's thread. The bullets below describe the async batch path; a sync send is read with the singular `_get` (one record per entry, in FIFO order), freed with `_destroy`, and completes a `Monitor` latch rather than a TCS, so it runs no continuation on the pump." |

**Checked, and needing no edit:**
- ffi `:876-878` "One helper for both flavors" is still true: the single and batch paths are separate code and both go through `DeliveryRegistration.Fire`.
- §A4 `:654`, `:663-664`: the sync send stays fixed, with no mutation window.
- CLAUDE.md `:729-738` (the generic pre-handoff residual) now covers sync as well, with no wording change.

**Optional X-rows (pre-existing stale text, outside this phase unless you add them):**
- X1: `CLAUDE.md:251` "before returning the `Task`" → "before returning its `ValueTask`" (agent text).
- X2: `CLAUDE.md:536` "`IProducer` still deferred" → "`IProducer` is shipped (M11/P4)" (agent text).

---

## 13. Risks and breaking-change notes

**Public surfaces touched (all breaking at source level; pre-1.0 and pre-publish, so allowed):** `IProducer<TKey,TValue>.Send` ×2, and the same two on `KafkaProducer<TKey,TValue>` and `MockProducer<TKey,TValue>`. A new public type: `KafkaFuture<T>`.

| # | Risk | Mitigation |
|---|---|---|
| R1 | `RecordMetadata m = producer.Send(r);` stops compiling | Intended, and a **compile** error. The xmldoc and STATUS carry the migration line |
| R2 | A `producer.Send(r);` **statement** compiles and becomes fire-and-forget (users' code and the repo's) | D12 for the repo; a migration warning on the public surface |
| R3 | Teardown regressions on the new single path (P3.5-class bugs) | S2 lands it unwired, with its own Critic pass, plus M7–M9 and M14–M16 |
| R4 | The residual docs under-state the boundary again (the M14/P1 history) | S5 re-walks the code and does not edit by recall; Critic pass 3 reproduces the walk |
| R5 | A sync-only producer now owns a thread (D4) | One per producer, lazy, joined at teardown; C9 and C11 |
| R6 | `Close`/`Dispose` from inside a callback now self-joins on sync too (pre-existing on async). Java turns this into `close(0)` | Recorded as FU-3; out of scope; stated in the IProducer remarks |
| R7 | Pending fire-and-forget sends now accumulate managed entries | Bounded by `buffer.memory` (the core blocks `Producer_send` ≤ `max.block.ms`), which is Java's backpressure. About 90 B of managed state per in-flight record (est.) |
| R8 | Callbacks trail `Flush` by the pump's lag (D8 (b)) | Documented; FU-2 |
| R9 | One P/Invoke per record on the pump (D1 (a)) caps pipelined sync throughput below the async path's amortized `get_all` | Measured at P-2; if it binds, the remedy is FU-1 (Mode B), not D1 (b), unless you reopen P3.2 D2 |
| R10 | The name reads as Java's `KafkaFuture` | D10's recorded deviation |
| R11 | The merge-base gRPC counts are unknown (F16), and the native library may be stale after merge `2c89c1b4` | S0 rebuilds and baselines before any edit |
| R12 | Known flakes outside this phase: `ProducerSubmitHandleRefTests…DoesNotRootTheCompletionContext` (`GetTotalMemory`) and `SafeProducerHandleTests.KafkaProducer_CreateThenDispose_HandleValidThenReleased` | Rerun once and record; do not "fix" them here |
| R13 | `Producer_Smoke_Sync` (p99 ≤ 70 ms at 100 rps) under the Step-2 pipelined engine | Python passes the same gate with the same design; P-2 measures it |

---

## 14. Open questions, and premises checked

**Questions for the user (answered 2026-10-07):**
- **Q1.** Branch: commit directly on `prashah_dev_dotnet_binding`, with no push, as in P3.6? The branch is pushed (`origin` = `d6ce0512`), so these commits would add to PR #196 when you push. → **Yes: commit directly on `prashah_dev_dotnet_binding`; no push (the user pushes).**
- **Q2.** Should S6 (perf Step 2) stay in this phase after your Step-1 run, or become its own phase number? → **Keep S6 in this phase.**
- **Q3.** Should Emanuele see the §3 shape before S3, given that it changes a shipped surface? → **No gate: the user has already reviewed the shape.**

**Follow-ups recorded by this phase (not done here):**
- **FU-1** (D1): a core "completion queue / wait-for-any" API (Mode B), to remove the pump's per-record head-of-line.
- **FU-2** (D8): a flush → pump completion barrier (Java's `flush()` post-condition), on one or both surfaces.
- **FU-3** (§13 R6): `Close`/`Dispose` from inside a delivery callback (Java's `close(0)`), no self-join.
- **FU-4** (D7): a timed or cancellable `Get`, with a `CancellationToken` — "We will add one with cancellation token later if required." M11/P4 decision #4 stands until then.
- **X1 / X2** (§12, optional, not ruled): the two pre-existing stale lines `CLAUDE.md:251` and `CLAUDE.md:536`. Not applied.

**Escalation triggers (the loop stops and the Manager reports to you):**
- an approved ruling needs changing;
- a Rust, FFI or header change turns out to be needed (Mode A broken);
- a test seam would have to be invented (T8);
- the D11 budget fails consistently;
- a §12 verbatim edit does not fit the file;
- a Critic finding would contradict a ruling.

**Premises in the brief, checked:**
- **False:** "a sync failure policy must be chosen" as an open behaviour. Sync `ApiException` failures **already** fire the callback inline and then throw, because the core returns a failed future with a null `out_error` (F7). Under Option 2 the failed future and callback fall out on the pump, so D2 reduces to confirming it and choosing the exception identity. Non-`ApiException` errors (closed, partitioner) still throw synchronously, as Java does.
- **False:** sync `Flush` waits for the pump. Neither `Flush` does (F6). That is what produced D8.
- **False:** "EnsurePump". It does not exist; the pump is created inside `EnsureAccumulator` (`:1009`, F4). D4 adds it.
- **Off by a range:** the IProducer "blocks and returns" paragraph is at `:50-66`, not about `:37-61`.
- **False:** `SoakConfig.cs` is a sync-producer user. The soak uses only the async surface; the §6 list is complete.
- **Not in tree:** the M14 roadmap PLAN. M14/P1 D3 is cited from code and CLAUDE.md, and only `COMMENTS.DONE.63.md` is archived.
- **Narrowed:** the "shape-reflection tests at `:616` and `:675`". Only `:616` asserts the sync return type; `:675` asserts the callback's parameter type and is unchanged (F14).
- **Stale:** STATUS still says P3.6 is "not pushed"; `origin` = `d6ce0512` (E10).
- **Holds:**
  - `internal static extern` = 219 + 357;
  - unit 2974/TFM, soak 165, perf-unit 39 at P3.6 close. They should be unchanged at `d6ce0512` (the merge touched no `dotnet/` code), and S0 confirms them;
  - `PublicAdminLogDirsShapeParityTests` forbids `Get[A-Z]…` only, so `Get` is allowed (`AsyncKafkaFuture` precedent);
  - no type named `KafkaFuture` exists (F21).

---

## 15. Parity anchors

- **Java:**
  - `Producer.java:81`, `:86` (`Future<RecordMetadata> send`);
  - `FutureRecordMetadata.java:51-80`;
  - `ProducerBatch.java:303-323`;
  - `KafkaProducer.java:1049-1061` (`catch (ApiException)`), `:1177` + `:1216-1220` (flush post-condition and guard), `:1398-1404` (close from a callback).
- **Python:**
  - `python/producer.py:338-389` (sync `send` → `Future`, `on_delivery` after resolution), `:410-415` (flush);
  - `python/grpc_server.py:228` (`result(timeout=120)`);
  - `producer_performance_test.py:133-170` and `:597-773` (the S6 port source).
- **.NET precedent:**
  - `AsyncKafkaFuture<T>` (P3.6: D4 equality, D6 deferral, D7 naming, D11 budget);
  - `Monitor`-based latch: `CountdownEvent` / `ManualResetEventSlim` semantics without a per-send kernel object.
- **History:**
  - M11/P4 PLAN `:115-140`, `:492-498` (decisions #1, #2, #4, #6);
  - M11/P3.2 PLAN `:1236-1240`, `:632` (D2 override (iii), D4 "the sync path stays inline");
  - M11/P8 D-3 (servicer);
  - M14/P1 D3 (inline callback; `COMMENTS.DONE.63.md`);
  - M11/P9 Option B residuals (the reason for S-3's Option 3 rejection).

## 16. Progress and evidence (Manager)

### 16.0 Approval record (2026-10-07)

The user approved this plan with these rulings:
- **D1:** (a) — one single-future `get` per record, in send order; the head-of-line limitation is documented; FU-1 recorded.
- **D2, D3, D4, D5, D6:** as recommended.
- **D7:** **no `Get(TimeSpan)` for now** ("We will add one with cancellation token later if required"). Removed from §3 (API and xmldoc), §4 (old example 4.4), §7 (N7, N8, C13), §9 (M6) and §12 (E10). Deferred as FU-4; M11/P4 decision #4 stands for now.
- **D7.1:** **dropped.** The sync gRPC servicer stays unbounded; M11/P8 D-3 stands (not superseded). In S3 it becomes `producer.Send(...).Get()` with its comment block rewritten for the new mechanics only. C14 dropped; E10's D-3 supersession dropped.
- **D8 to D14:** as recommended — D8 (b) defer (FU-2); D9 pump-thread guard; D10 naming note; D11 (a) shared base entry, budget = measured + 16 B, plus the no-boxing check; D12 sweep; D13 single-thread mock pattern; D14 no serial knob.
- **Q1:** commit directly on `prashah_dev_dotnet_binding`; no push. **Q2:** S6 stays in this phase. **Q3:** no Emanuele gate.
- **§12:** E1–E10 and F1–F8 approved as written, D7/D7.1 text removed only. X1/X2 not applied (follow-ups).

Loop: S0 → S1 → S2 → S2m → Critic pass 1 → fixes → S3 → S4 → S4m → Critic pass 2 → fixes → S5 → Critic pass 3 → fixes → **stop for the user's perf P-1** → S6 → Critic pass 4 → fixes → P-2 → close-out.

### 16.1 Slice log

*(Filled in as the slices land.)*
