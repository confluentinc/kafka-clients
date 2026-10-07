# M11/P3.6 — Async producer: `Send` yields `ValueTask<AsyncKafkaFuture<RecordMetadata>>`

**Status: APPROVED by the user on 2026-10-07.** The rulings are recorded verbatim in §2.0 and §14. Committed at approval as the phase's opening commit (as P3.5's plan was at `12ae2d7b`).

**Agent number: N = 92** (dotnet numbering). Confirmed three ways at the base:
- `design/current/STATUS.md:28` says "Next unused dotnet N = 92".
- No `COMMENTS.92.md` / `COMMENTS.DONE.92.md` exists at the binding root (`COMMENTS.91.md` is reset, 0 bytes).
- `git grep 'N=92\|COMMENTS.92\|COMMENTS.DONE.92' -- dotnet` finds nothing.

**Base:** `prashah_dev_dotnet_binding` @ `16c4f803` (M11/P3.5 close-out; `origin/prashah_dev_dotnet_binding` = `16c4f803` by the local ref, PR #196). The branch is 2 commits behind `origin/master` (`ca249696` #206, `7ccc9ffb` #212). **The plan does not depend on that merge.** If it lands before S1 starts, the Manager re-reads the base SHA and re-baselines every count in §8 before the first spawn (see §13 R6).

**Mode A:** C# only. Zero new or changed `[DllImport]` / `static extern`, no `rust/` or `src/ffi` change, no header change. **Pair:** `dotnet-actor` / `dotnet-critic`, N = 92.

---

## 0. The requirement and scope

Change the **async** producer's public `Send` return type from `ValueTask<Task<RecordMetadata>>` (M11/P3.5 D1) to `ValueTask<AsyncKafkaFuture<RecordMetadata>>`:

```csharp
ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default);
ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, IDeliveryCallback callback, CancellationToken cancellationToken = default);

var future = await producer.Send(r);   // accepted   (Java: send() returned)
var md     = await future.Get();       // delivered  (Java: future.get())
```

The maintainer's proposed shape (Emanuele): `public readonly struct AsyncKafkaFuture<T> { private Task<T> delivery; public Task<T> Get(); }` with a `Task` outer; **the user chose `ValueTask`.**

**In scope**
- New public type `AsyncKafkaFuture<T>` (`src/Confluent.Kafka/AsyncKafkaFuture.cs`, namespace `Confluent.Kafka`).
- `IAsyncProducer<TKey,TValue>.Send` ×2, `AsyncKafkaProducer<TKey,TValue>.Send` ×2, `AsyncMockProducer<TKey,TValue>.Send` ×2.
- Internal plumbing: `NativeProducer.SendViaPump`, `SendAccumulator.SubmitAdmitted`, `SendAccumulator.CancellableFirstStage`.
- Every repo caller: unit tests, `grpc-server/AsyncProducerServiceImpl.cs`, `soak/SoakClient/SoakClient.cs`, `tests/Performance/PerfV3/V3Backends.cs`.
- XML docs; `dotnet/CLAUDE.md`, `ffi-marshalling.md` and `STATUS.md` edits **only with the user's approval of §12's text**.

**Not in scope**
- **The sync `IProducer.Send` is unchanged** and keeps returning `RecordMetadata`. Verified at the base: `KafkaProducer.cs:101,:113` and `MockProducer.cs:103,:116`. Only its xmldoc cross-references to the async shape (`IProducer.cs:52-62`, `:134-141`) are reworded.
- Any change to P3.5's admission semantics (D2 (c), D3–D7), the cap, the pump, callback ordering, or `max.block.ms`. §5 lists the tests that must stay green **with unchanged names**.
- Admin's `KafkaFuture<T>` → `Task<T>` mapping (e.g. `Admin/CreatePartitionsResult.cs`). It stays as it is.
- The perf harness's own internal backend contract (`PerformanceCommon/Backends.cs:98`, `IAsyncProducerBackend.Send` → `ValueTask<Task<PerfRecordMetadata>>`). It is binding-independent and shared with PerfV2 (ckd), and `AWAIT_ACCEPTED` lives in that layer (`ProducerBenchmark.cs:255-285`). Only the V3 adapter changes (D9).
- Latency or `buffer.memory` investigation. This phase is a public-shape change; no such material goes in any doc.

---

## 1. Facts verified at `16c4f803` (so the plan does not rest on guesses)

| # | Fact | Evidence |
|---|---|---|
| F1 | The mock has **no separate send path**: `AsyncMockProducer.SendValidated` forwards to the same `NativeProducer.SendViaPump` as the real client. So mock parity holds by construction; only signatures change | `AsyncMockProducer.cs:197`, `AsyncKafkaProducer.cs:195` |
| F2 | Today's stage plumbing: fast path `new ValueTask<Task<RecordMetadata>>(deliveryTask)`; plain wait `wait.ContinueWith(static (_, s) => (Task<RecordMetadata>)s!, deliveryTask, …, TaskScheduler.Default)`; cancellable wait `CancellableFirstStage` with `TaskCompletionSource<Task<RecordMetadata>>(RunContinuationsAsynchronously)` + `TrySetResult(_delivery)` | `SendAccumulator.cs:407-409`, `:420-438`, `:444-445`, `:492-566` |
| F3 | **No analyzer that the brief names is enforced.** `Directory.Build.props` sets `TreatWarningsAsErrors`, `EnforceCodeStyleInBuild`, `AnalysisLevel latest`, with **no `AnalysisMode`** (so the Default rule set applies). `.editorconfig` raises only naming rules plus `CA1715` to warning (`:152-177`); it says nothing about CA1815/CA1066/CA1716/CA1000/CA1024 | `dotnet/Directory.Build.props`, `dotnet/.editorconfig` |
| F4 | **Empirical proof of F3.** The send-lanes POC's public `readonly struct RecordMetadataResult` had **no** `Equals` override and members named `Get`, `GetAwaiter`, `ConfigureAwait`, `IsDone`, `AsTask`, and it built clean. `git diff --stat c67f3013 16c4f803 -- dotnet/Directory.Build.props dotnet/.editorconfig dotnet/src/Confluent.Kafka/Confluent.Kafka.csproj` is **empty**, so the settings are identical | `git show c67f3013:dotnet/src/Confluent.Kafka/RecordMetadataResult.cs` |
| F5 | A public method named `Get` already ships: `Config.Get(string?)` | `Admin/Config.cs:116` |
| F6 | Repo precedent for public value types: both `TopicPartition` and `Uuid` are `readonly struct : IEquatable<T>` with `==`/`!=`, `Equals(object)`, `GetHashCode` | `TopicPartition.cs:59,:89-107`, `Uuid.cs:46,:126-132,:319-327` |
| F7 | `GenerateDocumentationFile` is on, so CS1591 (a public member without XML docs) is an error under `TreatWarningsAsErrors` | `Confluent.Kafka.csproj:19` |
| F8 | The library targets `netstandard2.0;net8.0;net10.0`, and the unit tests target `net462;net8.0;net10.0` (net462 is compile-only on macOS). So: no `HashCode.Combine`, no `Task.WaitAsync`, no `Register(Action<object?,CancellationToken>)` in the struct | `Confluent.Kafka.csproj:16`, `UnitTests.csproj:17` |
| F9 | `PublicAdminLogDirsShapeParityTests.TheBeanAccessorStyle_IsConfinedToReplicaLogDirInfo` scans **every exported type** for public `Get[A-Z]…` methods (`Name.Length > 3`). `Get` (3 characters) passes. **`GetAwaiter` would fail it**, because it is pattern-based rather than an implementation of a `System.*` interface. `Equals`/`GetHashCode` are excluded (an `object` base or the `IEquatable` interface map) | `PublicAdminLogDirsShapeParityTests.cs:263-297` |
| F10 | `PublicProducerSendAllocationBudgetTests.FireAndMeasure` measures `producer.Send(..).Delivery()` inside the window (`:172`). So T21(ii) already covers whatever the new `ProducerSendStages.Delivery` helper does on the fast path | `PublicProducerSendAllocationBudgetTests.cs:158-179` |
| F11 | **Budget headroom.** The fast-path budgets are 192 B (measured 160 on net8.0 and net10.0) and 232 B (measured 200). The saturated ceilings are 576/720/800/864 B (measured +536/+544, +680/+688, +760/+768, +824/+832). The headroom is 32–40 B: enough to catch one more `Task<T>` (72 B), **not** a boxed `AsyncKafkaFuture<T>` (24 B: header + method table + one reference) | P3.5 PLAN §17 T21; the test constants |
| F12 | Python parity anchor: `python/producer.py:654-655`, `async def send(...) -> "asyncio.Future[RecordMetadata]"`. It appends via `_lib.Producer_send`, then `await space` (`:688-705`), and returns `ret`. `await producer.send(r)` = accepted; `await fut` = delivered | `python/producer.py` |
| F13 | Java anchor: `Producer.java:81` and `:86` both return `java.util.concurrent.Future<RecordMetadata>` (`import java.util.concurrent.Future`, `:33`), **not** `org.apache.kafka.common.KafkaFuture` | `kafka/clients/.../producer/Producer.java` |
| F14 | Master's #206/#212 diff touches **no** `dotnet/` file and no proto (only `rust/`, `design/`, `service.yml`). `__grpc_dotnet_async` arms are generated per backend by the Rust harness, so #206's ~2.5k new lines in `rust/tests/integration/producer_test.rs` will drive `AsyncProducerServiceImpl`. That changes **counts** and the native lib, not servicer code | `git diff --name-only 16c4f803...origin/master` |
| F15 | P/Invoke count definition, pinned: `git grep -c 'internal static extern' <sha> -- dotnet/src` gives `NativeMethods.cs` 219 + `NativeMethods.Admin.cs` 357 = **576** at the base | memory: count `internal static extern`, not `grep -c DllImport` |

---

## 2. Decisions

"Settled" = the user (or the user's brief) has ruled; the plan does not reopen it. Every other row needs the user's yes/no, unless it is marked *shape (recommended)*, meaning an implementation detail the Critic checks.

| # | Decision | Status | Recommendation |
|---|---|---|---|
| S-1 | The outer type is `ValueTask` (not `Task`) | **Settled (user)** | — |
| S-2 | The name is `AsyncKafkaFuture<T>`, generic in `T` | **Settled (user)** | — |
| S-3 | Usage: `await producer.Send(r)` = accepted; `await future.Get()` = delivered | **Settled (user)** | — |
| S-4 | Scope = the async surface only; sync `IProducer.Send` → `RecordMetadata` unchanged | **Settled (user)**, verified (§0) | — |
| D1 | The struct's exact shape | *shape (recommended)* | Emanuele's shape, corrected for the repo's rules: `public readonly struct AsyncKafkaFuture<T>`; field **`private readonly Task<T>? _delivery;`** (`readonly` is required in a `readonly struct`, CS8340; `_camelCase` is the repo's naming rule, a warning and so an error; `?` because `default` holds null — the POC precedent); an **`internal`** constructor that **never throws** (it runs after the append — the P3.5 post-append no-throw invariant); `public Task<T> Get()`. No constraint on `T`. §3 |
| D2 | Where the struct is built | *shape (recommended)* | **Push it down; never wrap at the public boundary.** `SubmitAdmitted` and `SendViaPump` return `ValueTask<AsyncKafkaFuture<RecordMetadata>>` directly. The plain wait's `ContinueWith` projects to `new AsyncKafkaFuture<RecordMetadata>((Task<RecordMetadata>)s!)` with the **delivery task (a reference) as `state`** — never the struct, which would box. `CancellableFirstStage` holds `TaskCompletionSource<AsyncKafkaFuture<RecordMetadata>>` and an `AsyncKafkaFuture<RecordMetadata> _future` field in place of `_delivery`. §2.1 counts the allocations |
| D3 | `default(AsyncKafkaFuture<T>)` | *shape (recommended)*; message for the user's eye | `Get()` throws **`InvalidOperationException` synchronously**, with message: *"This AsyncKafkaFuture is a default value and carries no send; only IAsyncProducer.Send returns a usable one."* A test asserts it exactly. Rejected alternative: return a faulted `Task` (it allocates, and it hides a programming error until the await). Without the guard `Get()` returns null and `await null` throws `NullReferenceException` |
| D4 | Equality / `ToString` on the public value type | **User — Yes** (§2.0) | **Implement** `IEquatable<AsyncKafkaFuture<T>>`, `Equals(object?)`, `GetHashCode()` (`_delivery?.GetHashCode() ?? 0`; `Task` does not override it, so this is an identity hash) and `==`/`!=`, all by **reference identity of the delivery task**. That is what Java's `Future` identity and `ValueTask<T>`'s equality mean. Why: it is .NET's guideline for public structs (CA1815's intent, though not enforced, F3/F4); it is the repo precedent (F6); it avoids `ValueType.Equals`' reflection-and-boxing path (e.g. in `Assert.Equal`); and it is five one-line members. **No `ToString` override** (Java's `Future` has none; adding one later is non-breaking). *Alternative:* omit them all, as in the POC. That has the same semantics (the default compares the one field by reference) and is slower, with no `==` |
| D5 | `GetAwaiter()` (and then `ConfigureAwait`) on the struct, so `await future` / `await await` work | **User — No** (§2.0) | **Leave it out** (your lean). Against it: it brings back the opacity the named wrapper removes; it breaks `TheBeanAccessorStyle_IsConfinedToReplicaLogDirInfo` (F9), which would need an exclusion; and it pulls in `ConfigureAwait` as a second member, while `future.Get().ConfigureAwait(false)` already works. For it: Python's `asyncio.Future` is directly awaitable (F12). Adding it later is **non-breaking** |
| D6 | Java-`Future` forward-compat members (`IsDone`, `Get(TimeSpan)`, `IsCancelled`, `Cancel`) | **User — Defer** (§2.0) | **Defer** (your lean). Adding instance members to a struct later is source- and binary-compatible. Note: netstandard2.0 has no `Task.WaitAsync`. The POC's `Get(TimeSpan)` **blocked the thread** (sync-over-async on an async surface). If one is ever added it should return `Task<T>`. Today `future.Get().IsCompleted` / `.Status` give `isDone()` / `isCancelled()` |
| D7 | Recording the name as a deliberate deviation, plus DoD §7 | **User — Note** (§2.0; wording §12 E3/E4/E6) | Record it in the struct's xmldoc and the CLAUDE.md idiom map: a type **with** a Java counterpart (`java.util.concurrent.Future<RecordMetadata>`, F13). It maps to a struct over the `Task<T>` instead of the plain `Task<T>` that `bindings.md:59,:87` and the Admin mapping use, because the two-stage `Send` needs its second stage **named** (`ValueTask<Task<T>>` reads as one awaitable too many). It costs nothing at runtime (§2.1), and `Get()` is Java's `get()` once awaited. The name says *KafkaFuture* while Java's `KafkaFuture` maps to `Task<T>` in Admin; record that too. No change to `bindings.md` (it says "e.g.") or to Admin |
| D8 | Python parity | **Fact** (F12) | The new shape mirrors Python's async `send` one-for-one (await → future → await). The one residual difference is D5 (Python's future is awaitable itself) |
| D9 | The perf harness | *shape (recommended)* | Keep `IAsyncProducerBackend` and `ProducerBenchmark` (incl. `AWAIT_ACCEPTED`) **unchanged**; adapt only `V3Backends.cs:61-78` (`send.Result.Get()` / `(await send).Get()`) |
| D10 | Fold in P3.5's pending doc follow-ups (stale ffi §A1 `:350-415`, §A4 `:657-658` caveat) | **User — Yes** (§2.0) | **Yes**, as a separate commit (S4b) with the §12 F-row text, because E7/E9 edit the same paragraphs anyway. If declined, S4 applies only E1–E10, and the stale bullets stay on STATUS's "for the user" list |
| D11 | Tighten the allocation budgets so a **24 B box** of the new struct is caught (F11) | **User — Yes, option (a)** (§2.0) | **Yes — option (a):** set each of the six T21 budgets to *(the higher of the two TFMs' measured figures) + 16 B*: fast 192 → 176, callback fast 232 → 216, saturated no-token 576 → 560, recycled node 720 → 704, fresh node 800 → 784, new CTS 864 → 848. The S2b Actor re-measures first and adjusts if the figures moved. (b): tighten only the two fast-path budgets — this misses the realistic box site, a `ContinueWith` state on the pending path. (c): no change; rely on the Critic's structural "no struct → `object`" check plus a recorded M2 mutant. Your brief forbids *raising* the budgets; this *lowers* them, which changes P3.5-approved numbers, so it is yours to rule. Risk: a runtime servicing update that moves an internal object by 8 B turns these red, which is arguably the point |
| D12 | A public helper for "the delivery task without awaiting acceptance" (today's `send.AsTask().Unwrap()` stops compiling) | **User — No / defer** (§2.0) | **No / defer.** It is non-breaking to add later. The unthrottled idiom becomes a one-line local async function (§4.6); the internal test helper `ProducerSendStages.Delivery` stays internal |
| D13 | The §12 rule-file edits E1–E10 (+ F1–F7 if D10) | **User — Yes, verbatim** (§2.0) | Approve as written; the S4 Actor applies them **verbatim** |

### 2.0 The user's rulings (2026-10-07, verbatim)

These close every **User** row above. Where a row's "Recommendation" column offered alternatives, the ruling below selects one; the ruling wins over any wording above.

| # | Ruling (verbatim) | What it means for the slices |
|---|---|---|
| D4 | "Yes" | Implement `IEquatable<AsyncKafkaFuture<T>>`, `Equals(AsyncKafkaFuture<T>)`, `Equals(object?)`, `GetHashCode()`, `==`, `!=`, all by **reference identity of the delivery task**. **No `ToString` override.** N4 and M10 apply |
| D5 | "No" | **No `GetAwaiter`** (and no `ConfigureAwait`) on the struct. N5 pins it |
| D6 | "Defer for now" | No `IsDone` / `Get(TimeSpan)` / `IsCancelled` / `Cancel` members this phase |
| D7 | "Note" | Record the deviation in the struct's xmldoc (S1) and in the CLAUDE.md idiom rows (S4, E3/E4/E6), as the plan says |
| D10 | "Yes" | Fold in P3.5's stale ffi follow-ups as **S4b** (F1–F7); **E9b replaces E9a** |
| D11 | "Yes for now if it fails consistently then we will revisit" | **Option (a):** every T21 budget = *measured* + 16 B. The S2b Actor re-measures first. If a tightened budget then fails **consistently**, it is **neither raised nor loosened**: the loop stops and the Manager reports it to the user with the measurements |
| D12 | "No, defer for now" | No public helper for the delivery task without awaiting acceptance; §4.6's local async function is the idiom |
| D13 | "Yes" | The S4/S4b Actor applies E1–E11 and F1–F7 **verbatim** as written in §12. **No other rule-file edits.** If an Actor or Critic finds a verbatim edit wrong or no longer matching the file, the loop stops and the Manager reports it; no improvised rule text |

### 2.1 Allocations per path, before → after (D2)

Expected **before = after** on every path, because replacing a reference `TResult` (`Task<RecordMetadata>`, 8 B) with an 8-byte struct holding that reference keeps every object the same size and the same count. **This is proved by measurement in S2b, not assumed.** Definitions are P3.5's: caller-thread `GC.GetAllocatedBytesForCurrentThread` per send over 64 sends, best of 4 (harness) / 8 (public), net8.0 | net10.0.

| Path | Objects allocated for stage 1 | Before (P3.5 §17) | After (expected) | Guard |
|---|---|---|---|---|
| Fast (`_admission.Wait(0)`), public plain `Send`, absolute | none for stage 1 (`ValueTask` over the struct, on the stack); the whole send = `ProducerRecord` + TCS + its `Task` | 160 \| 160 B | 160 \| 160 B | T21(ii) `FastPathPerSendBudgetBytes` (192, or 176 under D11) |
| Fast, callback `Send`, absolute | same + `DeliveryRegistration` | 200 \| 200 B | 200 \| 200 B | 232 (or 216) |
| Fast, harness (`AppendStaged` → `SubmitAdmitted`) | none | 128 \| 128 B | 128 \| 128 B | — |
| Saturated, no token, marginal over fast | `WaitAsync` node + `ContinueWith` result task + continuation | +536 \| +544 B | +536 \| +544 B | 576 (or 560) |
| Saturated + cancelable, recycled node | `CancellableFirstStage` (`_delivery` 8 B → `_future` 8 B) + TCS + its task + continuation; registration node recycled | +680 \| +688 B | same | 720 (or 704) |
| … fresh node | + registration node | +760 \| +768 B | same | 800 (or 784) |
| … new `CancellationTokenSource` per send | + registration table | +824 \| +832 B | same | 864 (or 848) |

**Rejected: wrapping at the public boundary** (an `async` adapter or a `ContinueWith` turning `ValueTask<Task<T>>` into `ValueTask<AsyncKafkaFuture<T>>`). The fast path can stay free with an `IsCompletedSuccessfully` branch, but every pending send pays at least one more task object (≥ 72 B), plus a state-machine box for `async`. M3 (§9) proves that the T21(i) no-token ceiling catches it.

---

## 3. The exact new public API

```csharp
namespace Confluent.Kafka;

/// <summary>
/// The delivery handle of an accepted async send — the .NET realization of the
/// java.util.concurrent.Future<RecordMetadata> that Java's Producer.send returns (Producer.java:81, :86).
/// IAsyncProducer.Send's ValueTask completes when the record is ACCEPTED and yields this value;
/// awaiting Get() yields the RecordMetadata once the record is DELIVERED — `await future.Get()` is
/// Java's `future.get()`.
/// </summary>
/// <remarks>
/// [code]  AsyncKafkaFuture<RecordMetadata> future = await producer.Send(record);   // accepted
///         RecordMetadata metadata = await future.Get();                            // delivered
/// - A readonly struct over the delivery Task<T>: it costs no allocation (the common, unsaturated send allocates
///   nothing for its first stage). Get() returns the SAME task on every call and does not block; the task may be
///   awaited any number of times and stored freely (unlike the ValueTask stage, which is awaited once).
/// - Not awaitable itself: await Get(). ConfigureAwait goes on the task: await future.Get().ConfigureAwait(false).
/// - ⚠ Receiving this value (acceptance) does NOT end the borrow of the key / value buffers. Reuse a buffer only
///   after Get()'s task completes WITHOUT being canceled, after the record's delivery callback fires, or after a
///   later Flush completes successfully (M11/P3.5 91.11; see IAsyncProducer.Send's remarks).
/// - Cancellation: a token that fires after acceptance cancels Get()'s task; it never un-sends the record.
/// - default(AsyncKafkaFuture<T>) carries no send: Get() throws InvalidOperationException.
/// - Equality (if D4): two values are equal exactly when they wrap the same delivery task (reference identity).
/// - Naming (D7): deliberately not a plain Task<T> (unlike Admin's KafkaFuture<T> → Task<T>), so the two stages of
///   Send are distinguishable; zero runtime cost.
/// </remarks>
public readonly struct AsyncKafkaFuture<T> : IEquatable<AsyncKafkaFuture<T>>   // `: IEquatable<…>` only if D4 = implement
{
    private readonly Task<T>? _delivery;

    internal AsyncKafkaFuture(Task<T> delivery) => _delivery = delivery;   // never throws: runs after the append

    /// <summary>The record's delivery task: resolves with the value, or faults with a KafkaException carrying the
    /// delivery failure, or is canceled by the send's token. Await it for Java's future.get().</summary>
    /// <exception cref="InvalidOperationException">This is a default value (D3).</exception>
    public Task<T> Get() => _delivery ?? throw <D3 message>;

    // D4 (if approved): Equals(AsyncKafkaFuture<T>), Equals(object?), GetHashCode(), operator ==, operator != —
    // each with XML docs (CS1591, F7).
}
```

**`IAsyncProducer<TKey,TValue>`**, both overloads, the same parameter lists as today:

```csharp
ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default);
ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, IDeliveryCallback callback,
    CancellationToken cancellationToken = default);
```

`<returns>` (both): *"A `ValueTask{TResult}` that completes once the record is accepted, yielding the record's `AsyncKafkaFuture{T}`; await its `Get()` for the `RecordMetadata`, or for a `KafkaException` carrying the delivery failure. Await the `ValueTask` once; call `ValueTask{TResult}.AsTask` to store it."* `<exception>` lists unchanged (all synchronous, D4 of P3.5).

**`AsyncKafkaProducer<TKey,TValue>` / `AsyncMockProducer<TKey,TValue>`:** the same two signatures with `<inheritdoc/>`; `SendValidated` retyped. Mock-only members (`CompleteNext`, `ErrorNext`, `HistoryCount`, `Clear`) are unchanged.

**Internal:** `NativeProducer.SendViaPump(...)` and `SendAccumulator.SubmitAdmitted(...)` → `ValueTask<AsyncKafkaFuture<RecordMetadata>>`; `CancellableFirstStage.Start(...)` → `Task<AsyncKafkaFuture<RecordMetadata>>`. **Both stay non-`async`**; the "MUST NOT become async" comment (`NativeProducer.cs:574-584`) stays, retyped.

---

## 4. Usage patterns, before → after

**4.1 Single send**
```csharp
// before                                                  // after
Task<RecordMetadata> d = await producer.Send(r);          AsyncKafkaFuture<RecordMetadata> f = await producer.Send(r);
RecordMetadata md = await d;                              RecordMetadata md = await f.Get();
RecordMetadata md = await await producer.Send(r);         RecordMetadata md = await (await producer.Send(r)).Get();
```

**4.2 Many sends, throttled and pipelined, checking each.** Awaiting stage 1 is the throttle; it waits only when the bound is saturated.
```csharp
var futures = new List<AsyncKafkaFuture<RecordMetadata>>();
foreach (var r in records) futures.Add(await producer.Send(r));                 // before: List<Task<RecordMetadata>>
foreach (var f in futures)
{
    try { RecordMetadata md = await f.Get(); }                                    // before: await d
    catch (KafkaException e) { /* this record failed */ }
}
```

**4.3 `WhenAll`:** `RecordMetadata[] all = await Task.WhenAll(futures.Select(f => f.Get()));` (before: `Task.WhenAll(deliveries)`).

**4.4 Callback, future discarded:** `_ = await producer.Send(r, callback);` — throttled; the callback is the outcome channel. Unchanged: the discarded delivery task is never observed (a fault raises `TaskScheduler.UnobservedTaskException` at GC, as today; the soak client attaches a continuation for that reason).

**4.5 Cancellation — the D4 ambiguity.** `await producer.Send(r, ct)` gives the same `OperationCanceledException` for "already canceled → **not appended**" and "fired while waiting → **still sent**". Separating the call from the await tells them apart:
```csharp
ValueTask<AsyncKafkaFuture<RecordMetadata>> send;
try { send = producer.Send(r, ct); }                          // OCE here: token already canceled → NOT appended
catch (OperationCanceledException) { throw; }
AsyncKafkaFuture<RecordMetadata> f;
try { f = await send; }                                         // OCE here: fired while waiting → appended, STILL SENT
catch (OperationCanceledException e) when (e.CancellationToken == ct) { /* retrying may duplicate */ throw; }
RecordMetadata md = await f.Get();                              // OCE here: delivery task canceled → may still be sent
```
After either later OCE the buffers stay borrowed until the callback fires or a later `Flush` succeeds (91.11).

**4.6 Not awaiting stage 1 (unthrottled, D6)**
```csharp
// before:  Task<RecordMetadata> d = producer.Send(r).AsTask().Unwrap();
// after (D12 = no public helper):
ValueTask<AsyncKafkaFuture<RecordMetadata>> s = producer.Send(r);
Task<RecordMetadata> d = s.IsCompletedSuccessfully ? s.Result.Get() : Delivered(s);
static async Task<RecordMetadata> Delivered(ValueTask<AsyncKafkaFuture<RecordMetadata>> s) => await (await s).Get();
// or fire-and-forget:  _ = producer.Send(r);
```

**What the compiler catches.** Unlike P3.5 (two silent semantic changes, category C), **every** old use of the delivery task becomes a **compile error**: `Task<RecordMetadata> d = await …Send(..)`, `await d` on the struct (with D5 = out), `.AsTask().Unwrap()`, and `await await`. A bare `await producer.Send(r);` still compiles and keeps its meaning (accepted). The only runtime-only breaks are type checks by reflection: `PublicProducerDeliveryCallbackTests.cs:637` and `:658` (§6).

---

## 5. P3.5 semantics that must survive — guard tests (names unchanged)

"Body change" = what S2a is expected to touch, from a token sweep (`Task<Task<RecordMetadata>>`, `Unwrap()`, `Task<RecordMetadata> x = await`). **Anything beyond type plumbing (`.Get()` insertion, type names) in these tests is a Critic finding.** All names stay.

| P3.5 semantic | Guard tests | Body change |
|---|---|---|
| **D2 (c)**: the caller token ends only stage 1, with an OCE carrying the token; the record is still sent | T16 `SendAccumulatorTests.Admission_CallerTokenFiresWhileWaiting_EndsTheFirstStageCanceled_AndTheRecordIsStillSent`; T17 `PublicProducerFirstStageTests.Send_CallerTokenFiresWhileTheFirstStageWaits_CancelsBothStages_AndTheRecordIsStillSent` (both flavors, core record count) | 1 / 2 lines |
| The admission `WaitAsync` is never linked to the caller token | T4 `SendAccumulatorFirstStageTests.Admission_PermitAccounting_ReturnsToExactlyTheBound_AfterAFullDrain_IncludingCallerTokenCancels` | 4 lines |
| The registration is disposed when the wait completes | T18 `SendAccumulatorFirstStageTests.Admission_CancelableFirstStage_ReleasesItsTokenRegistration_OnceTheWaitCompletes` (Theory) | via the `StartObservedStage` helper (1 line) |
| Exactly one stage-1 outcome | T19 `SendAccumulatorFirstStageTests.Admission_TokenRacingAFreedSlot_SettlesTheFirstStageExactlyOnce_AndTheRecordIsStillSent` | 1 line |
| **D3**: teardown completes stage 1 **successfully** | `SendAccumulatorTests.Admission_WaitingFirstStage_IsCompletedByStop_AndItsRecordIsStillSent`, `…IsCompletedByStopsCancel_WhenTheBatchThreadCannotDrain`, `…IsCompletedByTheBatchThreadsFailureHandler_AndItsRecordIsSettled`, `…IsCompletedByTheFailureHandlersCancel_WhenTheReleaseWakesNobody`; `SendAccumulatorFirstStageTests.Admission_CancelableFirstStage_IsCompletedSuccessfullyByStopsCancel_WhenTheBatchThreadCannotDrain` (T9, deterministic half); T9 `PublicProducerFirstStageTests.Teardown_WithANonAwaitingFloodPastTheBound_SettlesBothStagesOfEverySendExactlyOnce`; T12 `…Teardown_WithANonAwaitingFloodPastTheBound_LeavesNoUnobservedTaskFault` | 2 lines each; `FloodTearDownAndObserve` 4 lines |
| **D4**: an already-cancelled token, null record, disposed producer or serializer throw → synchronous, before the append | T8 `PublicProducerSendSynchronousThrowTests` (all 6: `AlreadyCanceledToken_ThrowsSynchronously_AndSendsNothing`, `NullRecord_ThrowsSynchronously`, `Disposed_RealClient_CallbackOverload_ThrowsSynchronously`, `SerializerThrows_RealClient_ThrowsSynchronously`, `SendViaPump_AlreadyCanceledToken_ThrowsSynchronously`, `SendViaPump_DisposedProducer_ThrowsSynchronously`) | the private `Send` helper's return type (1 line) |
| **D5**: no stage-1 continuation on the batch / pump thread or inline on the canceller | T2 `SendAccumulatorTests.Admission_FirstStage_DoesNotResumeItsAwaiterOnTheBatchThread`; T20 `SendAccumulatorFirstStageTests.Admission_CallerTokenCancel_DoesNotResumeTheAwaiterOnTheCancellingThread` | 2 / 1 lines |
| **D6**: non-awaiting callers are not throttled | T3 `SendAccumulatorTests.Admission_CallersThatDoNotAwaitTheFirstStage_AreNotThrottled` | none expected |
| A saturated call returns at once; append-order ordering | T1 `SendAccumulatorTests.Admission_SaturatedBound_ReturnsTheCallAtOnce_WithItsFirstStagePending`; T7 `SendAccumulatorFirstStageTests.Admission_NonAwaitingBurstAcrossASaturatedBound_IsSentInCallOrder` | 5 / 2 lines |
| Flush completes pending stage 1s | T5 `PublicProducerFirstStageTests.Flush_CompletesEveryPendingFirstStage_AndEveryDelivery`; `SendAccumulatorTests.Flush_IncludesASendWhoseFirstStageIsStillWaitingOnAdmission` | 2 / 2 lines |
| `SendViaPump` / `SubmitAdmitted` stay non-`async`; no throw after the append | T8 (catches the `async` mutant, P3.5 M15); the retained catch-all in `SubmitAdmitted` (`:447-458`) and `CancellableFirstStage.Start` (`:557-562`), reviewed structurally; T9/T17 | — |
| Allocations | T21(i): `SendAccumulatorFirstStageTests.Admission_SaturatedFirstStage_PerSendAllocation_IsMeasured_AndStaysUnderItsCeiling`, `…WithAFreshTokenSource…`, `…WithANewTokenSourcePerSend…`; T21(ii): `PublicProducerSendAllocationBudgetTests.Send_FastPath_PerRecordAllocation_StaysWithinItsMeasuredBudget`, `PublicProducerDeliveryCallbackAllocationBudgetTests.CallbackSend_FastPath_PerRecordAllocation_StaysWithinItsMeasuredBudget`; plus the marginal budgets | `MeasurePerSend` 4 lines; the constants change only if D11 |
| **91.11**: acceptance is not release; neither is a cancellation | **Docs only — no test guards it**: `IAsyncProducer.cs:138-158`, `:221-229`, `AsyncKafkaProducer.cs:73-78`, `AsyncMockProducer.cs:70-75`, ffi §A4 `:657-658` | Re-pointed to "`Get()`'s task completes without being canceled"; the Critic checks every twin |
| T10 shape reflection | `PublicProducerDeliveryCallbackTests.AsyncSurfaces_DeclareTheCallbackOverload`, `…AsyncSurfaces_PlainSend_ReturnsTheTwoStageShape` | the expected `typeof` (1 line each); the name stays true (still two stages) |

---

## 6. Caller and test inventory (sweep at `16c4f803`, `obj/`/`bin/` excluded)

Columns: **VT** `ValueTask<Task<RecordMetadata>>`; **TT** `Task<Task<RecordMetadata>>` (not inside VT); **AWB** `Task<RecordMetadata> x = await <stage>`; **UNW** `.Unwrap()`; **DEL** `.Delivery()` call sites (**unchanged**: only the helper is retyped).

| File | VT | TT | AWB | UNW | DEL |
|---|---|---|---|---|---|
| `src/…/IAsyncProducer.cs` | 2 | — | — | — | — |
| `src/…/AsyncKafkaProducer.cs` / `AsyncMockProducer.cs` | 3 / 3 | — | — | — | — |
| `src/…/Internal/NativeProducer.cs` | 2 | — | — | — | — |
| `src/…/Internal/SendAccumulator.cs` | 6 | 1 | — | — | — |
| `grpc-server/AsyncProducerServiceImpl.cs` (`Delivered`, `:217-221`) | 1 | 0 | 1 | 0 | 0 |
| `soak/SoakClient/SoakClient.cs:901` (`task = await _producer.Send(..)`) | 0 | 0 | (1, assignment) | 0 | 0 |
| `tests/Performance/PerfV3/V3Backends.cs:61-78` | 2 | 0 | 0 | 0 | 0 |
| `UnitTests/ProducerSendStages.cs` (the helper) | 1 | 0 | 0 | 1 | 0 |
| `UnitTests/Interop/SendAccumulatorTests.cs` (incl. fixture `AppendStaged` `:2087`) | 3 | 12 | 9 | 1 | 2 |
| `UnitTests/Interop/SendAccumulatorFirstStageTests.cs` | 3 | 12 | 1 | 0 | 0 |
| `UnitTests/PublicProducerFirstStageTests.cs` | 3 | 8 | 0 | 0 | 2 |
| `UnitTests/PublicProducerDeliveryCallbackTests.cs` (reflection `:637`, `:658`) | 2 | 0 | 0 | 0 | 3 |
| `UnitTests/PublicProducerSendSynchronousThrowTests.cs` (`Send` helper `:194`) | 1 | 0 | 0 | 0 | 0 |
| `UnitTests/PublicProducerSendTests.cs` | 0 | 0 | 0 | 0 | 13 |
| `UnitTests/PublicProducerMockControlTests.cs` | 0 | 0 | 0 | 0 | 4 |
| `UnitTests/PublicProducer{SendTfmSmoke,DeliveryCallbackTfmSmoke,DeliveryCallbackAllocationBudget,TypedSend}Tests.cs` | 0 | 0 | 0 | 0 | 3, 2, 2, 2 |
| `UnitTests/{Interop/ProducerSendPinLifetime,PublicProducerAccumulatorTeardown,PublicProducerFlushDrain,PublicProducerSendAllocationBudget}Tests.cs` | 0 | 0 | 0 | 0 | 1 each |

- **`await await`: 0 real occurrences** in any `.cs`. The 4 grep hits (`AdminP9PerKeyStage{2..5}Tests.cs`) are `await awaitable()`. The idiom appears only in docs (`CLAUDE.md:35` "await again").
- **`.AsTask()`** on producer stages: `SendAccumulatorFirstStageTests.cs` 9, `SendAccumulatorTests.cs` 4, `PublicProducerFirstStageTests.cs` 3, `ProducerSendStages.cs` 1. They keep compiling (the result is `Task<AsyncKafkaFuture<…>>`); only the reads of their `.Result` / await change. `PerformanceCommon/ProducerBenchmark.cs:285` `.AsTask().Unwrap()` is on the harness's own type (D9) and is **unchanged**. The other `.AsTask()` hits (consumer, admin, `grpc-server/Program.cs`) are unrelated.
- **Unchanged:** `PerformanceCommon/*`, `PerfV2/*`, `Confluent.Kafka.PerformanceTests/{ProducerAcceptanceModeTests,ProducerCancellationTests}.cs` (fakes of the harness type), `soak/SoakClient.Tests` (no async producer use), all sync-producer tests.
- **Twin docs that name the return shape** (count of the phrase sweep): `IAsyncProducer.cs` 11, `NativeProducer.cs` 3, `AsyncKafkaProducer.cs` 2, `IProducer.cs` 2 (`:57`, `:139`), `AsyncMockProducer.cs` 1, `SendAccumulator.cs` 1 (`:367`), `AsyncProducerServiceImpl.cs` 1, `Internal/Interop/NativeMethods.cs:2270` (a comment only — edit allowed under Mode A, provided `internal static extern` stays at 219), plus `IDeliveryCallback.cs:20-26` ("yields the delivery `Task` (async)").

---

## 7. Tests

**New (S1 — the type alone, through its `internal` constructor; file `UnitTests/PublicAsyncKafkaFutureTests.cs`):**
- N1 `Default_Get_ThrowsInvalidOperationException_WithItsMessage`: `default(AsyncKafkaFuture<RecordMetadata>)` and `new AsyncKafkaFuture<RecordMetadata>()`. Assert the exact D3 message.
- N2 `Get_ReturnsTheWrappedTask_TheSameInstanceOnEveryCall` (`Assert.Same`, ×2 calls), for a pending, a completed, a faulted and a canceled task.
- N3 `Get_ConfigureAwait_FlowsThroughTheTask` (structural: `await f.Get().ConfigureAwait(false)` yields the value).
- N4 (if D4) `Equality_IsReferenceIdentityOfTheDeliveryTask`: same task → equal, `==`, same hash; different tasks → not equal; `default == default`; `default != non-default`; `Equals(object)` with a boxed other and with a non-future.
- N5 `Shape_IsAPublicReadonlyStruct_WithOnlyTheApprovedMembers`: `IsValueType`, `IsReadOnlyAttribute` present, namespace `Confluent.Kafka`, the declared public instance methods = `{Get}` ∪ D4's set, **no `GetAwaiter`** (pins D5 = out), no public constructor.

**New (S2b — through production's entry points, DoD §12):**
- N6 `Send_FirstStage_YieldsAFutureWhoseGetIsTheDeliveryTask` on the **fast path**, the **saturated no-token** path and the **saturated + cancelable** path (slot wins), for **both overloads**, on `AsyncMockProducer`. At harness level (`AppendStaged`), assert `Assert.Same(completion.Task, (await stage).Get())`. Real-client flavor wherever the suite already has a two-flavor fixture (T17's).
- N7 `DefaultValueTask_AwaitedFuture_GetThrows` (`default(ValueTask<AsyncKafkaFuture<RecordMetadata>>)` → `await` → `Get()` → N1's exception).
- N8 (if D11) the six tightened T21 constants, each **shown red** by the M2 box mutant (§9) on the path it guards.

**Changed (S2a):** every site in §6 (type plumbing only, names unchanged), the helper `ProducerSendStages.Delivery(this ValueTask<AsyncKafkaFuture<RecordMetadata>> send) => send.IsCompletedSuccessfully ? send.Result.Get() : <unwrap via a static local async function>`, and T10 → `typeof(ValueTask<AsyncKafkaFuture<RecordMetadata>>)`.

**Counts.** The S1 Actor records the baseline `Passed:` per TFM before any edit: unit 2956/TFM (net8.0, net10.0), soak 165, perf-unit 39/TFM at `16c4f803`. Every slice reports its delta. A zero-match filter, a missing `Passed:` or `Test Run Aborted` is a **failure**. Both TFMs, every slice.

---

## 8. Slicing, gates, Critic points

| Slice | Content | Behaviour change | Commit |
|---|---|---|---|
| **S1** | `AsyncKafkaFuture.cs` (§3, full xmldoc, D3/D4 per ruling) + N1–N5. Production does **not** use it yet | None | `feat(dotnet): AsyncKafkaFuture<T>, the async send's delivery handle (M11/P3.6 S1)` |
| **S2a** | The atomic retype: `IAsyncProducer` ×2, `AsyncKafkaProducer`, `AsyncMockProducer`, `NativeProducer.SendViaPump`, `SendAccumulator.SubmitAdmitted` + `CancellableFirstStage` (D2), the helper, every §6 site, T10, servicer `Delivered`, `SoakClient.cs:901` (+ its comment), `V3Backends.cs` (D9). **Type-truth docs only**: every `<returns>` / summary / comment sentence that names the return type (the §6 twin list), so no slice ships a false return-type doc (the P3.5 91.1–91.4 lesson) | Public shape only; runtime identical | `feat(dotnet)!: async Send yields ValueTask<AsyncKafkaFuture<RecordMetadata>> (M11/P3.6 S2)` |
| **S2b** | N6–N8; re-measure T21 on both TFMs with definitions; apply D11 if approved | Tests only | `test(dotnet): AsyncKafkaFuture identity on every first-stage path; allocation figures (M11/P3.6 S2)` |
| **S2m** | Mutation run §9 (separate Actor spawn). **Nothing committed** unless a test is strengthened (then `fixup!` to S2b). The Actor reports the **literal injected change** per row, the target, the regime (isolated K / full suite) and the fail ratio; the Manager records it in §16 | — | — |
| — | **Critic 92, pass 1 (S1 + S2a + S2b + the S2m record).** Two spawns if needed (a: production + docs, b: tests + mutation record), per the P3.5 precedent | | |
| **S3** | Narrative xmldoc: the type remarks of `IAsyncProducer` (the code sample `:69-72`, `:57`, `:129-136`), the two concrete producers' remarks, `IProducer.cs:52-62` / `:134-141`, `IDeliveryCallback.cs:20-26`, the D7 deviation note, §4.5's call/await separation as an `<example>` | Docs | `docs(dotnet): AsyncKafkaFuture in the producer docs (M11/P3.6 S3)` |
| **S4** | The approved §12 E-rows, **verbatim** | Rules/docs | `docs(dotnet): AsyncKafkaFuture in the binding rules (M11/P3.6 S4)` |
| **S4b** | (if D10) the §12 F-rows, verbatim | Rules | `docs(dotnet): retire the deleted submission-queue text from ffi §A1/§A4 (M11/P3.6 S4b)` |
| — | **Critic 92, pass 2 (light; S3 + S4 [+ S4b]):** the docs match the approved text; twins swept; no other rule edits | | |
| Close | Manager: STATUS entry (E11), `§16` evidence, archive `COMMENTS.DONE.92.md` here, reset `COMMENTS.92.md`, memory. (`marked_classes.txt` does not apply to the binding) | | |

**DoD gates, every slice** (Rust first, CLAUDE.md §7.1):
1. `make -C <repo> build-dotnet` → `0 Error(s)` (covers netstandard2.0, net462 compile, net8.0, net10.0).
2. `make -C <repo> test-dotnet` → `dotnet format --verify-no-changes`, unit net8.0 + net10.0, soak. Assert a positive `Passed:` per TFM, no `Failed:`, no `Test Run Aborted`.
3. `make -C <repo> perf-unit-test-dotnet` → 39/TFM unchanged.
4. **S2a additionally** (projects outside the `.sln`): `dotnet build -c Release dotnet/grpc-server` and `dotnet build -c Release dotnet/tests/Performance/PerfV3`, each `0 Warning(s) 0 Error(s)`, plus `dotnet format <project> --verify-no-changes` on both.
5. **S2a/S2b**: `make -C <repo> verify-dotnet-macos-docker` → native gRPC `__grpc_dotnet` + `__grpc_dotnet_async`: **145/145** (151 − 6 transaction skips) at this base, producer arms 38/38. If #206 has been merged, re-baseline first (F14). Confirm the arm **count**, never the exit code. Also `make -C <repo> test-integration-perf-dotnet` where Docker is available (PerfV3 smoke 41 passed, 2 consumer smokes skipped on macOS); otherwise flag it as not run.
6. **Mode A proof** at S2a and at close: `git diff --stat 16c4f803..HEAD -- . ':!dotnet'` is empty; `git grep -c 'internal static extern' HEAD -- dotnet/src` gives 219 + 357; no `[DllImport]` line in the diff.
7. Every figure is reported with **its definition and the command that produced it** (allocations: absolute vs marginal, TFM, best-of-N).

**Process:** fixes are `fixup!` commits referencing the slice commit (agent-roles.md); the loop repeats per pass until `COMMENTS.92.md` is empty. **One Actor spawn per slice** (S2 = three spawns: a, b, m). Commit after every slice. Work on `prashah_dev_dotnet_binding`; **do not push** (the user pushes via `git push-external`). Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

---

## 9. Critic 92 focus and the mutations it must demand

**Pass 1, what to check:**
1. **Shape** = §3 as ruled: `readonly`, `_delivery` naming and nullability, an `internal` constructor with no throw, `Get()` returning the stored task, D3's exact message, D4/D5 per ruling, XML docs on every public member. **No suppression added** (`#pragma warning disable`, `[SuppressMessage]`, `.editorconfig`/`Directory.Build.props` edits): an analyzer finding is fixed at its site.
2. **Push-down (D2):** no `async` method and no extra `ContinueWith` between `SubmitAdmitted` and the public `Send`; the struct is built where the stage completes; **no struct → `object` conversion** on the send path (a `ContinueWith` `state`, an `object` cast, a non-generic interface call). `SendViaPump` / `SubmitAdmitted` stay non-`async`; the post-append catch-alls are intact.
3. **§5's guard tests:** review `git diff -U0 16c4f803.. -- <those files>`. Only type plumbing; every name unchanged; no assertion weakened (e.g. `IsCompletedSuccessfully` → `IsCompleted`).
4. **Allocations:** S2b's figures, with definitions, equal P3.5's (§2.1); budgets not raised; D11 applied exactly as ruled.
5. **The mutation record:** a literal injected change per row; regimes; equivalent mutants called equivalent.
6. **Doc truth (type level):** no sentence still says the first stage yields a `Task`; 91.11's borrow wording survives, re-pointed to `Get()`'s task (every twin in §5's 91.11 row).
7. **Mode A** (gate 6); the servicer's 120 s bound still covers **both** stages; the soak still **awaits stage 1** (or it becomes unthrottled); the V3 adapter keeps its fast path free of state machines.

**Mutations to demand (the S2m Actor runs them on net10.0; deterministic 3× isolated; timing-dependent 8× isolated + one full suite, K=8 in-suite with a fresh harness per rep):**

| # | Injected change (literal) | Must turn red |
|---|---|---|
| M1 | Fast path returns `new ValueTask<AsyncKafkaFuture<RecordMetadata>>(Task.FromResult(new AsyncKafkaFuture<RecordMetadata>(deliveryTask)))` (one more `Task<T>`, 72 B) | T21(ii) plain, both TFMs |
| M2a/b/c | Box the struct once per send: (a) on the fast path; (b) on the plain wait, by passing the struct as the `ContinueWith` `state`; (c) in `CancellableFirstStage` (`object` field) | (a) T21(ii); (b) T21(i) no-token; (c) T21(i) recycled — **red only under D11 (a)**; under (b)/(c) record which slip |
| M3 | Public-boundary wrap: `SubmitAdmitted` keeps `ValueTask<Task<…>>`, and `SendViaPump` adapts via an `async` local function on the pending path | T21(i) no-token ceiling |
| M4 | Caller token linked into `WaitAsync` (`_admission.WaitAsync(linkedCts.Token)`) | T4 |
| M5 | `Get() => _delivery!` (guard removed) | N1, N7 |
| M6 | `Get()` returns a fresh task (`_delivery!.ContinueWith(t => t.GetAwaiter().GetResult(), TaskScheduler.Default)`) | N2, N6 (and T21) |
| M7 | `SendViaPump` marked `async` (D4 throws move into the `ValueTask`) | T8 |
| M8 | `CancellableFirstStage`'s TCS built without `RunContinuationsAsynchronously` | T20 |
| M9 | Teardown completes the cancellable stage with `TrySetResult(default)` | the D3 tests / T9 (via `Get()` → N1's exception) and N6 |
| M10 | (if D4) `Equals` compares `_delivery?.Status` instead of reference | N4 |
| M11 | The slot-wins branch of `CancellableFirstStage` yields `default(AsyncKafkaFuture<RecordMetadata>)` | N6 (cancelable path) |

Known equivalent mutant, not demanded: `ExecuteSynchronously` on the no-token `ContinueWith` (P3.5 M2a).

**Pass 2 (light):** S3/S4 text equals the approved rows; the twins are swept (multi-line, joined `///` lines); no rule edit beyond the approved rows.

---

## 10. Context budget per agent

The binding constraint in past phases was **reasoning volume**, not only I/O. So each spawn is one slice, with a short design surface already settled here.

- **Do not read whole:** `ffi-marshalling.md` (2395 lines), `dotnet/CLAUDE.md` (1089), `design/current/STATUS.md` (3024), `Interop/SendAccumulatorTests.cs` (2459), `Internal/SendAccumulator.cs` (1703), `Internal/NativeProducer.cs` (1747), `SoakClient.cs` (1617), the P3.5 `PLAN.md` (623; only §2.2 `:126-154`, §9 `:291-327`, §17 `:556-623` if needed), `COMMENTS.DONE.91.md` (only 91.11, `:186-226`). **Never open `target/include/confluent_kafka.h`** (this phase has no ABI surface).
- **Ranged reads (S2a):** `SendAccumulator.cs:330-570`, `NativeProducer.cs:390-640`, `IAsyncProducer.cs:126-272`, `AsyncKafkaProducer.cs:115-200`, `AsyncMockProducer.cs:118-200`, `ProducerSendStages.cs` (31, whole), `AsyncProducerServiceImpl.cs:175-222`, `SoakClient.cs:870-905`, `V3Backends.cs:50-80`. For the tests: `grep -n` the §6 tokens, then read ±15 lines per hit, **or let the compiler list the sites** (`dotnet build … 2>&1 | grep -E 'error CS' | sort -u | head -n 80`). Never re-read a range already loaded.
- **S1:** this PLAN §1–§3, §7 N1–N5; `TopicPartition.cs:85-125` (the equality pattern); `.editorconfig:140-177`. About 400 lines in all.
- **S2b/S2m:** `SendAccumulatorFirstStageTests.cs:600-735` (T21(i)), `PublicProducerSendAllocationBudgetTests.cs:40-60,120-196`, `PublicProducerDeliveryCallbackAllocationBudgetTests.cs:225-300`, `SendAccumulatorTests.cs:2080-2140` (the fixture).
- **S3/S4:** the exact lines listed in §6/§12; grep-then-range.
- **Output bounds:** every build/test command is redirected to a scratch log, and the agent reads only `grep -E 'Passed!|Failed!|error CS|Test Run Aborted|Total tests' <log> | head -n 40` and `tail -n 30 <log>`. A default `dotnet test` emits about 1.16 MB in one call. If any command returns more than ~100 lines, summarize and discard. `grep -c` before `grep -n`; `-m` caps.
- **Shell traps** — put them in every brief verbatim:
  1. Start every Bash call with `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$HOME/.dotnet:$PATH"`.
  2. `grep` may be ugrep. Use `/usr/bin/grep` for evidence, and exclude `obj/`/`bin/` (generated XML docs live there).
  3. `sed` is GNU here (`sed -i ''` fails); prefer `awk`.
  4. `cat` may be shadowed (`cat > f <<EOF` can write 0 bytes); use `/bin/cat`.
  5. zsh does not word-split `$var`; quote globs (`--include='*.cs'`); `echo ==` fails (use `echo '----'`); `"$ref:rust/x"` applies `:r`, so write `"${ref}:rust/x"`; never name a loop variable `path`.
  6. A filter matching zero tests exits 0. Confirm the **count**.
  7. An aborted `dotnet test` exits 0; grep `Test Run Aborted`.
  8. `-c Release` build + `--no-build` test must use the **same** configuration.
  9. A failed build + `--no-build` prints a bogus `Passed!` off a stale binary; never use `--no-build` after a failed build. For mutation runs, rebuild and confirm the mutant compiled before reading a result.
- **Git hygiene:** `git -C <abs path>` only; no bare `cd`; never bare `git stash`; never edit `.claude/worktrees/*`; leave the user's untracked agent-memory files and `tests/Performance/RUNNING-LOCALLY.md` alone (untracked, the user's; not edited this phase).

---

## 11. Perf plan — **the user runs it; agents do not**

**Is an A/B needed?** Not a full one. §2.1 predicts identical allocation and runtime, and S2b proves the allocation part at unit level. A **small confirmation** suffices: R1 (max rate, awaiting acceptance) and R5 (bounded 50k, 10 ms slices), "before" vs "after", **2 reps each, alternating B/A/B/A, one session, the same local broker**.

- **Before** = `16c4f803` (or the merged base SHA if #206/#212 land first).
- **After** = the S2b commit (the S2a commit if S2b changes only tests; the native lib is identical either way, since the phase has no Rust change).

```
R=/Users/pranavshah/WorkSpace/Confluent/example-confluent-kafka-rust
B=/Users/pranavshah/WorkSpace/Confluent/p36-before
O=$HOME/perf-p36; mkdir -p "$O"
git -C "$R" worktree add --detach "$B" 16c4f803          # outside .claude/worktrees/

# R1 max-rate, awaiting acceptance (default) — before, then after; repeat once more
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=True TEST_DURATION_SECONDS=60 VALUE_SIZE=1024 PARTITIONS=6 make -C "$B" producer-perf-test-dotnet CLIENT_VERSION=3
cp "$B/dotnet/metrics.jsonl" "$O/R1-before-1.jsonl"
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=True TEST_DURATION_SECONDS=60 VALUE_SIZE=1024 PARTITIONS=6 make -C "$R" producer-perf-test-dotnet CLIENT_VERSION=3
cp "$R/dotnet/metrics.jsonl" "$O/R1-after-1.jsonl"

# R5 bounded 50k, 10 ms slices — before, then after; repeat once more
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=True LIMIT_RPS=50000 LIMIT_RPS_SLICE_MS=10 TEST_DURATION_SECONDS=60 VALUE_SIZE=1024 PARTITIONS=6 make -C "$B" producer-perf-test-dotnet CLIENT_VERSION=3
cp "$B/dotnet/metrics.jsonl" "$O/R5-before-1.jsonl"
BOOTSTRAP_SERVERS=localhost:9092 ASYNC=True LIMIT_RPS=50000 LIMIT_RPS_SLICE_MS=10 TEST_DURATION_SECONDS=60 VALUE_SIZE=1024 PARTITIONS=6 make -C "$R" producer-perf-test-dotnet CLIENT_VERSION=3
cp "$R/dotnet/metrics.jsonl" "$O/R5-after-1.jsonl"
# (second rep: the same four commands with -2 file names)

git -C "$R" worktree remove "$B"
```

`metrics.jsonl` is written to `dotnet/` (the PerfV3 process's working directory under `make -C`), and each run overwrites it, so copy it after **every** run. The first `make -C "$B"` also builds the Rust native in the before-worktree (slow, once).

**Acceptance (after vs before):** R1 throughput within ±5 %, CPU no more than +10 %, RSS within ±15 %. R5 achieved rate ≥ 49.5k msg/s, CPU no more than +10 %. A miss is reported to the Manager with the files. No other runs, no config overrides, no analysis prose in any doc.

---

## 12. Rule-file and doc edits — **exact text for the user to approve** (D13)

Provenance: P3.5 §11 plus `git blame`/`git log -S` at the base. "User" = text the user authored (rows inherited through the consolidation commit `0204437a`, whose original author P3.5 §11 traced).

| # | Location | Provenance | Before → after |
|---|---|---|---|
| E1 | `dotnet/CLAUDE.md:35` (§1 sketch) | agent (P3.5 R1, `cbecd70c`) | `producer.Send(record)  ──►  ValueTask<Task<RecordMetadata>>   (await → accepted · await again → delivered)` → `producer.Send(record)  ──►  ValueTask<AsyncKafkaFuture<RecordMetadata>>   (await → accepted · await future.Get() → delivered)` |
| E2 | `CLAUDE.md:169-176` (§3 interface sketch) | agent (P3.5 R2) | Both `ValueTask<Task<RecordMetadata>> Send(` → `ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(`. The comment `// outer = admission (Java's send blocking, never parks a thread — M11/P3.5); inner = the Future` → `// outer = admission (Java's send blocking, never parks a thread — M11/P3.5); inner = the Future —` + new line `// AsyncKafkaFuture<RecordMetadata>, a readonly struct whose Get() is the delivery Task (M11/P3.6)`. **Add**, right after the interface's closing `}`: `public readonly struct AsyncKafkaFuture<T> {   // Java java.util.concurrent.Future<T> (producer send) — M11/P3.6` / `    public Task<T> Get();   // await future.Get() == Java future.get(); default(...) → InvalidOperationException` / `}` |
| E3 | `CLAUDE.md:481` idiom row | **User** (`42583351` per P3.5 §11 R3) | Column 2 only: `` `Task<RecordMetadata>` `` → `` `Task<RecordMetadata>`; on `IAsyncProducer.Send` wrapped as `AsyncKafkaFuture<RecordMetadata>` (a zero-cost struct whose `Get()` is that `Task`; a deliberate deviation so the two stages are distinguishable — M11/P3.6) `` . Columns 1 and 3 verbatim |
| E4 | `CLAUDE.md:482` idiom row | agent (P3.5 R3) | `` `ValueTask<Task<RecordMetadata>>` on `IAsyncProducer`; the outer stage is the block (admission, completes on acceptance, no thread parked), the inner `Task` is the `Future`; sync `IProducer.Send` unchanged `` → `` `ValueTask<AsyncKafkaFuture<RecordMetadata>>` on `IAsyncProducer`; the outer stage is the block (admission, completes on acceptance, no thread parked), the inner `AsyncKafkaFuture` is the `Future` (`await future.Get()` = Java's `future.get()`); sync `IProducer.Send` unchanged ``; column 3 `M11/P3.5, ffi §A1` → `M11/P3.5, M11/P3.6, ffi §A1` |
| E5 | `CLAUDE.md:491` Callback row | agent (P3.5 R4) | `` / `ValueTask<Task<RecordMetadata>>` `` → `` / `ValueTask<AsyncKafkaFuture<RecordMetadata>>` ``; the rest verbatim |
| E6 | `CLAUDE.md:555` §4 "Sync vs async" trigger table, row "**Returns `Future<T>`**" | via `0204437a`; original author not traced, treat as **User** | Column 2: `` `Task<T>` on the async interface; name mirrors Java (no `Async` suffix) `` → `` `Task<T>` on the async interface; name mirrors Java (no `Async` suffix). Exception: the producer's `send` yields `AsyncKafkaFuture<RecordMetadata>` (a struct over the `Task<T>`) from its admission `ValueTask` — M11/P3.6, §3 idiom map `` |
| E7 | ffi §A1 `:299-301` | agent (P3.5 R7) | `` `IAsyncProducer.Send` returns `ValueTask<Task<RecordMetadata>>`: the outer stage completes on admission (never parking a thread), the inner `Task` is delivery. `` → `` `IAsyncProducer.Send` returns `ValueTask<AsyncKafkaFuture<RecordMetadata>>`: the outer stage completes on admission (never parking a thread), the inner `AsyncKafkaFuture` is delivery (its `Get()` is the delivery `Task`, M11/P3.6). `` |
| E8 | ffi §A1 anti-patterns, new bullet after `:378` | new | `  - Wrapping the stage at the public boundary (an `async` adapter or a `ContinueWith` from an internal `ValueTask<Task<…>>`) instead of building the `AsyncKafkaFuture` where the stage completes (`SubmitAdmitted` / `CancellableFirstStage`), and boxing it (passing it as an `object` `state`): either one is a per-send allocation on the send path (M11/P3.6).` |
| E9 | ffi §A4 `:657-658` | agent (P3.5 R11; the row's `:658` origin is **User** `959df4744`) | **E9a (type only):** `` the `ValueTask` stage completing does not end the borrow — a buffer may be reused only after the delivery `Task` completes. `` → `` the `ValueTask` stage completing (yielding the `AsyncKafkaFuture`) does not end the borrow — a buffer may be reused only after the delivery `Task` (`future.Get()`) completes. `` **E9b (if D10, replaces E9a):** → `` the `ValueTask` stage completing (yielding the `AsyncKafkaFuture`) does not end the borrow — a buffer may be reused only after the delivery `Task` (`future.Get()`) completes **without being canceled**, after the record's delivery callback fires, or after a later `Flush` completes successfully. A cancellation from either stage does not end it, and neither does `Close` / `Dispose` returning, whose wait for the send-batch thread is bounded (M11/P3.5 91.11). `` |
| E10 | ffi §A7 `:1074-1076` and diagram `:1086` | **User** (`959df4744`, edited by P3.5 R13) | `` returns a `ValueTask<Task<RecordMetadata>>` whose inner `Task` is TCS-backed `` → `` returns a `ValueTask<AsyncKafkaFuture<RecordMetadata>>` whose `AsyncKafkaFuture` wraps the TCS-backed delivery `Task` (`Get()`) ``; diagram `append; return ValueTask<Task>` → `append; return ValueTask<Future>`, keeping the right-hand column aligned (two fewer spaces) |
| E11 | `design/current/STATUS.md` | Manager at close | A new top entry for M11/P3.6 (N=92, base, commits, gates, figures with definitions, the rulings) and "Next unused dotnet N = 93". It also states that **M11/P3.5 is pushed** (`origin/prashah_dev_dotnet_binding` = `16c4f803`, PR #196), correcting the stale "Not pushed" at `STATUS.md:30` **without rewriting that historical bullet** |

**Not edited:** `bindings.md:59,:87` ("e.g. Java `Future` → .NET `Task`" is an example, and D7 records the deviation); `soak/README.md:325-329` (still true: it awaits acceptance, then continues on the delivery `Task`); `tests/Performance/RUNNING-LOCALLY.md` (untracked, yours; the `AWAIT_ACCEPTED` row is unaffected); historical `design/history/**`.

**F-rows — P3.5's pending follow-ups, applied only if D10 = yes (S4b).** The lines are at `16c4f803`.

| # | Location | Before → after |
|---|---|---|
| F1 | ffi §A1 anti-patterns `:350-362` (the "dedicated thread for the submission queue's appender" bullet and the "zero appenders" Dekker bullet) | Replace both with: `  - *(Superseded by M11/P3.4: the submission queue, its appender and the start/stop handshake were deleted — append-first under `_gate` needs none of them. The general lesson stands: a two-sided start/stop handshake needs a store→load fence on both sides — an `Interlocked` operation, not a `Volatile.Write`.)*` |
| F2 | ffi §A1 tests `:385-391` (the ordering test's "with a submission queued, the inline path is refused … nothing pinned" deterministic half) | → `  - **The order records reach `send_batch` equals call order, across a saturated bound.** Append-first makes this deterministic: with the batch thread held back and the bound saturated, a non-awaiting same-thread burst reaches `send_batch` in call order (T7 below). It must be shown to **fail** when the append is deferred into the stage-1 continuation (the pre-P3.4 shape).` |
| F3 | ffi §A1 tests `:392-396` (Flush includes "a send still queued for capacity") | → `  - **A drain / `Flush` includes a send whose first stage is still waiting for capacity** — its record is already in the chain (append-first), so `Flush` drains it and its first stage completes (`PublicProducerFirstStageTests.Flush_CompletesEveryPendingFirstStage_AndEveryDelivery`, `SendAccumulatorTests.Flush_IncludesASendWhoseFirstStageIsStillWaitingOnAdmission`).` |
| F4 | ffi §A1 tests `:397-399` ("…nothing pinned while queued (the pin belongs after the permit, §A4)") | → `  - **Teardown settles both stages of every send exactly once**, with nothing left holding an unsettled `TaskCompletionSource`; a pending first stage completes **successfully** (M11/P3.5 D3). Pins are taken with the append and released by the send (§A4).` |
| F5 | ffi §A1 tests `:400-402` ("a queued submission whose token fires before it is appended … is not sent") — **contradicts D2 (c)** | → `  - **A token that fires while the first stage waits ends that stage with an `OperationCanceledException` carrying the caller's token, and the record is still sent** — asserted on the core's record count, not only on the awaiter's state (T16, T17; M11/P3.5 D2 (c)). An **already**-canceled token throws synchronously and appends nothing (D4).` |
| F6 | ffi §A1 tests `:403-415` ("Flood the submission path faster than it drains … the queue's depth alone is not the quantity of interest") | `the submission path` → `the send path, with the callers **awaiting** the first stage (the bound binds only them — M11/P3.5 D6)`; `the queue's depth alone is not the quantity of interest` → `the depth of any one container alone is not the quantity of interest`; the rest of the bullet verbatim |
| F7 | ffi §A4 `:657-658` | = E9b (instead of E9a) |

---

## 13. Risks and breaking-change notes

**Public surfaces touched (all breaking at source level; pre-1.0 and pre-publish per STATUS, so allowed):** `IAsyncProducer<TKey,TValue>.Send(ProducerRecord, CancellationToken)` and `.Send(ProducerRecord, IDeliveryCallback, CancellationToken)`; the same two on `AsyncKafkaProducer<TKey,TValue>` and `AsyncMockProducer<TKey,TValue>`; the **new** public type `AsyncKafkaFuture<T>` (+ D4's members). It is the second break of the same two signatures in consecutive phases (P3.5, then P3.6). The sync surface is untouched.

| # | Risk | Mitigation |
|---|---|---|
| R1 | `await await`, `.AsTask().Unwrap()` and `Task<RecordMetadata> d = await …Send(..)` stop compiling for users | Intended: every one is a **compile** error, none a silent change (§4). STATUS and the xmldoc carry the migration lines |
| R2 | A boxed struct (24 B) slips under the current budgets (F11) | D11; the Critic's structural check; M2 |
| R3 | Doc drift: twin sentences still say "yields a `Task`" (P3.5's 91.1–91.4 and 91.11–91.13 arrived one at a time) | S2a's type-truth pass over the §6 twin list; the Manager re-sweeps after every doc fixup (multi-line, joined `///`) and hands the Actor the full site list |
| R4 | 91.11's borrow guarantee has **no test**; a reword could weaken it | Critic check 6; E9b if D10 |
| R5 | An analyzer fires unexpectedly | Not expected (F3/F4/F5). If it does: fix at the site and report; never suppress |
| R6 | The master merge (#206/#212) lands mid-phase | Re-read the base; rebuild the native (stale-library lesson — #206 changes `rust/src/producer/kafka_producer.rs`); re-baseline gRPC arm counts (more `__grpc_dotnet_async` producer arms); no servicer code change is expected (F14). If it lands between slices, pause the loop and re-run S2a's gate 5 |
| R7 | The name reads as Java's `KafkaFuture` while Admin maps that to `Task<T>` | D7's recorded deviation in the xmldoc and the CLAUDE.md idiom rows |
| R8 | The value-type generic instantiation (`Task<AsyncKafkaFuture<RecordMetadata>>`, `TaskCompletionSource<…>`) on .NET Framework / netstandard2.0 consumers | Compile-verified by `build-dotnet` (net462); JIT cost is one-time per `T` |
| R9 | Known flakes outside this phase: `ProducerSubmitHandleRefTests.SubmitVoidOperation_WhenAddRefThrows_DoesNotRootTheCompletionContext` (`GetTotalMemory`) and `SafeProducerHandleTests.KafkaProducer_CreateThenDispose_HandleValidThenReleased` | Rerun once; record it; do not "fix" it in this phase |
| R10 | `Get()`'s name invites a sync reading (Java's `get()` blocks) | The xmldoc says "does not block; await it"; D6 rules out a blocking `Get(TimeSpan)` |

---

## 14. Open questions, and premises checked

**Questions for the user**
- Q1. Branch: commit directly on `prashah_dev_dotnet_binding` (as in P3.5), no push? The branch is now pushed (PR #196), so the P3.6 commits would add to that PR when you push.
- Q2. If #206/#212 are merged before S1, plan against the merged SHA (R6)?
- Q3. Should Emanuele see the §3 struct (D1/D3/D4/D5) before S2a, since it is his proposal?

**The user's answers (2026-10-07, verbatim where quoted)**
- Q1: "Yes, same branch" — `prashah_dev_dotnet_binding`, in the main tree. No push (the user pushes via `git push-external`).
- Q2: Work directly on `prashah_dev_dotnet_binding` at base `16c4f803`. **Do not merge** master, #206, #212 or PR #201 in this phase; that merge is a separate, later decision of the user's. So R6 does not arise in this phase, and §8 gate 5's baseline is the `16c4f803` one (145/145, producer arms 38/38).
- Q3: The user has reviewed the §3 struct. **No Emanuele review gate before S2a.**

**Escalation triggers (the loop stops and the Manager reports to the user):** an approved ruling needs changing; a Rust / ffi / header change turns out to be needed (Mode A broken); a D11 budget fails consistently; a §12 verbatim edit does not fit the file; a Critic finding would contradict a user ruling.

**Premises in the brief, checked**
- The mock's send path: **not separate** — the same `SendViaPump` (F1). Mock parity is automatic; only signatures change.
- `await await`: **zero** real uses in repo code (§6); it lives only in docs and users' code.
- The harness: `AWAIT_ACCEPTED` and the engine are binding-independent and **unchanged**; only `V3Backends.cs` changes (D9).
- "Master will bring multilanguage producer tests whose `__grpc_dotnet_async` arms drive `AsyncProducerServiceImpl`": **holds** (the arms are generated per backend), but the diff touches no `dotnet/` file or proto, so it means counts and a native rebuild, not code (F14).
- T21(ii) "verified red by one extra `Task<T>`": true, but the budget **cannot** see a 24 B box — the one new allocation class this phase can introduce (F11, D11).
- `STATUS.md:30` still says P3.5 is "Not pushed"; the local origin ref says it is (E11 corrects it without rewriting history).
- The cited line ranges (`SendViaPump` `:504-594`, `SubmitAdmitted` `:391-459`, `CancellableFirstStage` `:492-566`, the servicer `:217-221`) are accurate at the base.

---

## 15. Parity anchors

- Java: `Producer.java:81`, `:86` (`java.util.concurrent.Future<RecordMetadata>`); `Future.get()` ≙ `await future.Get()`.
- Python: `python/producer.py:654-705` (`async def send` → `asyncio.Future`; append, then `await space`).
- .NET precedent: `ValueTask<T>` (a struct over a task, `IEquatable`); `TopicPartition` / `Uuid` (repo public structs); `Config.Get` (a public `Get`).
- P3.5: PLAN §2 (D1–D10), §4, §9, §17 (T21 figures and the mutation matrix); `COMMENTS.DONE.91.md` 91.11.

## 16. Progress and evidence (Manager)

*(Filled in as the slices land.)*
