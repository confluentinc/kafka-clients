# M5/P2 — Consumer `Position` (Category B: the scalar completion-bridge shape)

**Status:** DRAFT — awaiting user approval. **Review number: N=11** (last completed:
N=10, the M5/P1 "Consumer sync read surface", which shipped `Assignment()` /
`Subscription()` / `Paused()` / `EnforceRebalance()`).

**Milestone / phase label (RESOLVED — user):** **Milestone 5 / Phase 2 — "Consumer
Position"**, slug `P2-consumer-position`, at
`bindings/dotnet/design/history/M5/P2-consumer-position/`, consistent with M5/P1.

**Mode:** **A (`.NET-only`, no Rust authored).** Both ABI functions
(`kafka_consumer_Consumer_position_async` + the sync `Consumer_position`) and the
callback typedef (`kafka_consumer_Consumer_position_callback_t`) already ship in
`target/include/confluent_kafka.h` (verified — see §0). `cargo build --features ffi`
remains a *prerequisite build step*, not a change.

**Branch:** commits land on **`prashah_dev_public_consumer_remaining`** (the branch the
user created for post-M4 consumer work; M5/P1 already shipped there). Treated as a new
PR for M5/P2.

---

## 0 · Scope & ABI ground truth (verified in the generated header)

This phase adds **`position(TopicPartition)`** — the single async member from
"Category B" of the consumer public-API coverage analysis. Its real work is a **NEW
completion-bridge shape: the scalar callback** — a result carried *directly* in the
callback as an `int64_t`, with **no owned result handle to marshal or free** (distinct
from the shipped void bridge and the shipped owned-handle poll bridge).

| Member | Java | ABI function (verified) | Result / callback |
|---|---|---|---|
| `Position` | `position(TopicPartition)` / `position(TopicPartition, Duration)` → `long` | `kafka_consumer_Consumer_position_async(consumer, topic, partition, callback, user_data)` | scalar `int64_t` via `position_callback_t` |

**Signatures (exact — verified in `target/include/confluent_kafka.h`):**

```c
// async form — NO timeout param
void kafka_consumer_Consumer_position_async(const kafka_consumer_Consumer_t *consumer,
                                            const char *topic, int32_t partition,
                                            kafka_consumer_Consumer_position_callback_t callback,
                                            void *user_data);

// the scalar completion callback
typedef void (*kafka_consumer_Consumer_position_callback_t)(int64_t,
                                                            kafka_common_KafkaError_t*,
                                                            void*);

// sync form also present (NOT used this phase — see §2)
kafka_common_KafkaError_t *kafka_consumer_Consumer_position(const kafka_consumer_Consumer_t *consumer,
                                                            const char *topic,
                                                            int32_t partition,
                                                            int64_t *out_position);
```

**Callback contract (from the header doc, verified):** on **success** `error` is null
and the `int64_t` is the offset; on **failure** `error` is non-null and the `int64_t`
is 0; the callback **owns `error`** if non-null. There is **no presence flag** —
position is never absent on success (unlike `current_lag`, which needs one). So the
scalar shape is strictly simpler than the owned-handle shape: nothing to `_destroy`
except the error on the failure path.

**Not in scope** (unchanged public shape; each is an additive later phase): the commit
family, `committed`, the other owned-handle query siblings (`offsetsForTimes` /
`beginning|endOffsets` / `partitionsFor` / `listTopics`), `currentLag`,
`subscribe(pattern)` / public `assign` / `pause` / `resume`,
`ConsumerRebalanceListener` / `OffsetCommitCallback`, serializers + generic
`IConsumer<TKey,TValue>`, typed `KafkaException` subclasses. **No Rust authored, no ABI
change, no new op semantics.** The CLAUDE.md §4 package-id pre-publish gate stays OPEN
(held shut by `IsPackable=false`).

---

## 1 · DECISION — the scalar bridge structure (the phase's real work)

The binding today has **two proven completion-bridge shapes**:

- **void** — `ConsumerCallbacks.Operation` (`op_callback_t`, `(error*, ud)`) +
  `SubmitVoidOperation` → subscribe / unsubscribe / seek / close.
- **owned-handle** — `ConsumerCallbacks.Poll` (`poll_callback_t`, `(records*, error*,
  ud)`) + `SubmitOperation<ConsumerRecords>` → poll. The trampoline copies the batch
  out on the dispatcher thread, then `_destroy`s the owned root.

`Position` needs the **third** shape — a **scalar** result in the callback,
`(int64_t, error*, ud)`, with no owned result handle.

### 1.1 Reuse `OperationCompletionSource<long>` verbatim — no bridge changes

**Decision: reuse the existing generic `OperationCompletionSource<TResult>` with
`TResult = long`. Add NO new bridge context type; make NO change to
`OperationCompletionSource.cs`.**

Rationale (verified by reading `Internal/OperationCompletionSource.cs`): the generic
bridge is already **result-type-agnostic and does no native reads** — it exposes
exactly the primitives the scalar path needs:

- `CompleteWithResult(TResult result)` — success with an already-produced managed
  value. For the scalar path the "marshalling" is trivial (`long` is blittable — no
  copy-out, no native read), so the trampoline calls `CompleteWithResult(position)`
  directly. The docstring already anticipates this: *"the already-marshalled managed
  result type (e.g. `ConsumerRecords` for poll, `bool` for the void path)"* — `long`
  is the same category.
- `Complete(IntPtr error)` — failure path: `KafkaException.FromHandle(error)` (frees
  the error handle exactly once in its own `finally`), maps to
  `OperationCanceledException` when the token fired, else faults with the
  `KafkaException`. Reused unchanged — this covers the operational-failure AND the
  inline core concurrent-rejection paths.
- `RegisterCancellation` / `AbandonBeforeSubmit` / `FreeGcHandle` /
  `SetGcHandle` — cancellation wiring + the free-exactly-once machinery, all
  result-type-agnostic. Reused unchanged.

The `RunContinuationsAsynchronously` TCS is built in the base ctor (mandatory for the
foreign dispatcher thread, ffi §B7) — inherited for free.

**Anti-pattern this avoids:** a bespoke `ScalarCompletionSource` duplicating the
5-invariant machinery. M3/P3 already generalized the bridge precisely so future result
shapes reuse it; a scalar is the easiest such reuse (no marshaller at all).

### 1.2 New scalar trampoline + delegate in `ConsumerCallbacks`

Add to `Internal/Interop/ConsumerCallbacks.cs` (mirroring the shipped `Poll` /
`OnPoll` structure, tailored to the scalar shape):

```csharp
[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void PositionCallback(long position, IntPtr error, IntPtr userData);

internal static readonly PositionCallback Position = OnPosition;

private static void OnPosition(long position, IntPtr error, IntPtr userData)
{
    OperationCompletionSource<long>? context = null;
    try
    {
        GCHandle handle = GCHandle.FromIntPtr(userData);
        context = (OperationCompletionSource<long>)handle.Target!;
        if (error != IntPtr.Zero)
        {
            // Failure (position is 0). Complete maps to KafkaException /
            // OperationCanceledException and frees the error handle via FromHandle.
            context.Complete(error);
        }
        else
        {
            // Success: the scalar offset is the result directly — no owned handle,
            // no copy-out, no _destroy. (Position is never absent on success.)
            context.CompleteWithResult(position);
        }
    }
    catch (Exception exception)
    {
        // No-throw boundary (foreign dispatcher thread): never unwind into native.
        context?.TrySetException(exception);
    }
    finally
    {
        // Sole owner of the GCHandle free, on EVERY path (ffi §B6), incl. the inline
        // core-guard rejection. NO batch destroy here — the scalar shape owns no
        // result handle (the ONLY structural difference from OnPoll).
        context?.FreeGcHandle();
    }
}
```

**Key structural difference from `OnPoll`:** the `finally` frees **only** the
`GCHandle` (`context?.FreeGcHandle()`) — there is **no** `NativeMethods.*Destroy(...)`
call, because the scalar result carries no owned handle. The error handle (failure
path) is still freed exactly once inside `Complete` → `KafkaException.FromHandle`. The
delegate is rooted in a `static readonly` field for the process lifetime (§B6
keep-alive), identical to `Operation` / `Poll`.

### 1.3 Submit helper: add `SubmitScalarOperation<T>`, do NOT disturb the proven paths

**Decision: add a new `SubmitScalarOperation<T>` helper on `NativeConsumer`
paralleling `SubmitOperation<TResult>`, rather than generalize the existing
`SubmitOperation<TResult>`.**

Rationale (verified by reading `NativeConsumer.cs` lines 1086–1109): the shipped
`SubmitOperation<TResult>` is **hard-wired** to `ConsumerCallbacks.Poll` and the
`NativeResultSubmit` delegate type — its body passes `ConsumerCallbacks.Poll` as the
callback argument. The scalar path needs a **different callback delegate type**
(`PositionCallback`, `(long, error*, ud)` vs `PollCallback`, `(records*, error*,
ud)`), which is a *different unmanaged signature*, so the native `submit` lambda's
parameter type differs. Threading a second callback type through the existing helper
(a callback parameter, or a second generic) would touch the proven poll submit path
for no benefit. A parallel helper keeps the poll path byte-for-byte:

```csharp
// New delegate type paralleling NativeResultSubmit, for the scalar callback:
private delegate void NativeScalarSubmit(IntPtr consumer, ConsumerCallbacks.PositionCallback callback, IntPtr userData);

private Task<T> SubmitScalarOperation<T>(CancellationToken cancellationToken, NativeScalarSubmit submit)
{
    ThrowIfClosed();
    cancellationToken.ThrowIfCancellationRequested();

    OperationCompletionSource<T> context = new OperationCompletionSource<T>();
    GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
    context.SetGcHandle(gcHandle);
    try
    {
        context.RegisterCancellation(cancellationToken, Wakeup);
        submit(_handle.DangerousGetHandle(), ConsumerCallbacks.Position, GCHandle.ToIntPtr(gcHandle));
    }
    catch
    {
        context.AbandonBeforeSubmit();
        throw;
    }
    return context.Task;
}
```

This is a line-for-line clone of `SubmitOperation<TResult>` with `Poll` → `Position`
and `NativeResultSubmit` → `NativeScalarSubmit` — same rooting, same cancellation
wiring, same `AbandonBeforeSubmit`-on-throw, same one-op-in-flight (core-serialized,
no managed guard). The **void** (`SubmitVoidOperation`) and **owned-handle**
(`SubmitOperation`) paths are untouched.

*(Alternative considered and rejected: generalize `SubmitOperation` to take the
callback + delegate type as parameters. Rejected because it perturbs the shipped poll
path — a proven, reviewed hot-seam — to save ~12 lines of a well-understood clone. The
parallel-helper cost is one small method; the perturbation cost is a re-review of the
poll bridge. Recorded as a decision in `COMMENTS.DONE.11.md`.)*

---

## 2 · DECISION — API shape & the `Duration`/timeout question

**`Task<long> Position(TopicPartition partition, CancellationToken cancellationToken =
default)` on `IAsyncConsumer`.**

- **Async, on `IAsyncConsumer` (NOT `IConsumerCommon`).** Java's
  `AsyncKafkaConsumer.position` **blocks** — it is in the consumer-threading §4 "blocks
  despite reading instantaneous" list (it does a cross-thread event round-trip /
  `updateFetchPositions`), so the CLAUDE.md §4 idiom map maps it to `Task<long>`. It is
  a blocking-in-Java op, so it belongs on `IAsyncConsumer`, not the non-blocking
  `IConsumerCommon`. The CLAUDE.md §3 sketch **already lists** `Task<long>
  Position(TopicPartition partition, CancellationToken ...)` on `IAsyncConsumer` — this
  phase implements the sketch-committed shape (no shape change, no deviation to
  record). Note the shipped `IAsyncConsumer.cs` "additive-growth" doc explicitly names
  `position` as a not-yet-wired member arriving later — this phase wires it as the
  planned additive member.
- **Java's two overloads collapse to one .NET method (RESOLVED — user).** Java has
  `position(tp)` and `position(tp, Duration)`. The async ABI form (`position_async`) has
  **no timeout param**, so:

  **Decision: ship ONE method, `Position(TopicPartition, CancellationToken = default)`,
  with NO `TimeSpan` overload this phase.** The Java `position(tp, Duration)` **timeout**
  overload is **deferred** until the C ABI exposes a timed `position_async` — at which
  point a faithful `Position(TopicPartition, TimeSpan)` lands as an additive (Mode B)
  overload. We do **NOT** add a `TimeSpan` overload now that silently ignores the
  `TimeSpan` (a lie), nor simulate the timeout with a binding-side deadline. This is the
  shipped **`Close`** precedent (no timed overload until the ABI has one).

  **The `CancellationToken` is NOT a timeout.** Its sole intent is **user-initiated
  cancellation** — the caller cancels the request (→ `wakeup()`, best-effort, §4) when
  *they* decide to; it is not a substitute for Java's `Duration` deadline. The binding
  neither derives nor documents a timeout/deadline from it. (A user may of course cancel
  their token from their own timer if they wish — but that is the caller's cancellation,
  not a `Position` timeout.)

- **`long` return, no new value type.** Java returns `long`; .NET returns `Task<long>`.
  No new public value type is introduced (unlike M5/P1's list marshallers) — the scalar
  is blittable.

---

## 3 · DECISION — error / precondition mapping (ffi §B5)

Two surfaces, exactly the shipped consumer discipline:

- **Operational failure → faulted `Task<long>` carrying `KafkaException`.** The failure
  path routes through the trampoline's `context.Complete(error)` →
  `KafkaException.FromHandle` (frees the error handle exactly once) → `TrySetException`.
  The canonical broker-free operational failure is **"position for an unassigned
  partition"** — the mock core returns an `illegal_argument` error with the exact
  message `"You can only check the position for partitions assigned to this consumer."`
  (verified in `src/consumer/mock_consumer.rs::position`). The test asserts that
  **message content** (DoD §3 — error messages are the behavioral contract), not just
  `is_faulted`.
- **Concurrent async op → faulted `Task<long>` (`ConcurrentModification`),
  core-delivered.** A concurrent op is rejected by the **core inline** (it fires the
  `position_callback` on the caller thread with a `ConcurrentModification` error),
  surfaced as a faulted `Task` via the same `Complete(error)` path — **not** a managed
  pre-check (M3/P2 single-owner; no managed guard). Same non-deterministic-reachability
  ceiling as every shipped async op (see §6).
- **Post-dispose → `ObjectDisposedException`.** `SubmitScalarOperation` calls
  `ThrowIfClosed()` **before** any pin / P-Invoke (the shipped gate). Deterministic.
- **Preconditions (validated BEFORE any pin / P-Invoke, ffi §B5):**
  - `partition` is a `TopicPartition` struct (a value type, cannot be null itself); its
    **`Topic` must be non-null** → `ArgumentNullException(nameof(partition))` (or a
    message naming `partition.Topic`) if `partition.Topic is null`. (Match how the
    shipped `Seek` / `Assign` validate a null topic before marshalling.)
  - **Negative partition:** validate per the shipped `Seek`/`Assign`/`AddRecord`
    precedent — those reject a negative partition with
    `ArgumentOutOfRangeException(nameof(...), partition, "Partition must not be
    negative.")` **before** the native call (the ABI silently maps negative to "unset",
    so the binding must reject it — ffi §A5/§B5). Apply the same here. **Flag:** confirm
    negative-partition → `ArgumentOutOfRangeException` (the `Seek` precedent) vs passing
    it through; default is reject-before-native, matching `Seek`.
  - **Pre-canceled token → `OperationCanceledException` synchronously**, via
    `cancellationToken.ThrowIfCancellationRequested()` in `SubmitScalarOperation`
    (before submit) — identical to `SubmitOperation`/`PollWithCallback`.

  All precondition throws happen **before** the `GCHandle.Alloc` / P-Invoke. Wakeup
  (`KafkaException`/Wakeup) is distinct from `CancellationToken` cancel
  (`OperationCanceledException`) — the shipped bridge already separates these (§B5).

---

## 4 · DECISION — cancellation / wakeup (ffi §B7)

Mirror the shipped `PollWithCallback` cancellation exactly (inherited via
`SubmitScalarOperation` → `RegisterCancellation`):

- `CancellationToken` firing during an in-flight `Position` → `wakeup()` (best-effort),
  which aborts the in-flight op → the callback fires with a Wakeup error → the
  `Task<long>` cancels/faults. `CompleteWithResult` / `Complete` dispose the
  cancellation registration first to minimize the intrinsic wakeup-vs-next-op race
  (consumer-threading §11, documented as intentional/Java-faithful).
- A **pre-canceled token** → `OperationCanceledException` thrown **synchronously** from
  `Position` (before submit), via `ThrowIfCancellationRequested()`.

No new cancellation semantics — the wiring is 100% inherited from the generic bridge.

---

## 5 · DECISION — `NativeConsumer` additions + P/Invoke + forwarding

**`NativeMethods` (`Internal/Interop/NativeMethods.cs`)** gains one DllImport (plus the
delegate type lives in `ConsumerCallbacks`):

- `Consumer_position_async(IntPtr consumer, IntPtr topic, int partition,
  ConsumerCallbacks.PositionCallback callback, IntPtr userData) → void`
  (`EntryPoint = kafka_consumer_Consumer_position_async`; `IntPtr topic` = a pinned
  NUL-terminated UTF-8 buffer per §A3/§B3; `int` partition per the type map). The
  **sync** `Consumer_position` is **NOT** declared (the async form is the one used;
  declaring the sync form would be dead code — Position blocks in Java → async, and
  wrapping the sync form in `Task.Run` is the forbidden sync-over-async, ffi §B7).

**`NativeConsumer` (internal)** gains:

- `internal Task<long> PositionWithCallback(TopicPartition partition, CancellationToken
  cancellationToken = default)` — mirrors `PollWithCallback`'s submit shape:
  1. Precondition validation (null `partition.Topic` → `ArgumentNullException`;
     negative partition → `ArgumentOutOfRangeException`) **before** anything native.
  2. Pin `partition.Topic` **call-scoped** via `Utf8Marshal.Pin` (a `using` block, ffi
     §A3/§B3) around the submit.
  3. `return SubmitScalarOperation<long>(cancellationToken, (consumer, callback,
     userData) => NativeMethods.ConsumerPositionAsync(consumer, topicPtr, partition,
     callback, userData));`
  - **Pin lifetime note (verify during implementation):** the topic pin is
    **call-scoped** — `position_async` reads/copies the topic string synchronously
    during the submit call (like every other consumer op's topic marshalling, §A3/§B3).
    The Actor must confirm the ABI does not borrow the topic pointer past the submit
    return (the shipped `SeekWithCallback` topic marshalling is the precedent — it pins
    call-scoped). If the ABI were to borrow past submit, the pin would have to span
    submit→callback; the precedent says call-scoped, so pin call-scoped and confirm.
- Plus the `SubmitScalarOperation<T>` helper + the `NativeScalarSubmit` delegate type
  (§1.3).

**Forwarding — `AsyncKafkaConsumer` and `AsyncMockConsumer`** (compose-and-forward,
matching the shipped `Poll`/`Seek` forwards):

```csharp
public Task<long> Position(TopicPartition partition, CancellationToken cancellationToken = default) =>
    _native.PositionWithCallback(partition, cancellationToken);
```

Both concrete types add the identical forward (both `impl IAsyncConsumer`).

**Interface file touched:** `IAsyncConsumer.cs` gains the `Position` declaration with
**full XML docs** (CS1591 enforced): Java mapping, blocks-in-Java → async rationale,
the `CancellationToken` → `wakeup()` note (user-initiated **cancellation**, NOT a
timeout), the no-`TimeSpan`-overload note (the timed overload deferred until a timed
ABI — the shipped `Close` precedent), and the exceptions (`ArgumentNullException` on null topic,
`ArgumentOutOfRangeException` on negative partition, `ObjectDisposedException` on
closed, `OperationCanceledException` on cancel, `KafkaException` faulted-Task on
operational failure). Remove `position` from the "not-yet-wired" list in the
`IAsyncConsumer` remarks (doc-sync).

---

## 6 · Tests (DoD §3 + CLAUDE.md §7.4; broker-free via `AsyncMockConsumer`)

**File placement — the test ROOT, `PublicConsumerPositionTests.cs`** (NOT under
`Interop/`). The M5/P1 misfiling was corrected; public-surface tests live at the test
root (`PublicConsumerSyncReadTests.cs` etc. are all at
`tests/Confluent.Kafka.UnitTests/`). Serial execution (`[assembly:
CollectionBehavior(DisableTestParallelization = true)]`, D8.8) stays enabled. Every
awaited op under a `TestTimeout` hang guard (the completion/deadlock regression guard,
§7.4).

**Canonical happy path (mock, broker-free):**

1. **`Position` after `Assign` + `Seek` returns the sought offset.** `AsyncMockConsumer`
   → `Assign([tp])` → `await Seek(tp, offset)` → `long p = await Position(tp)` → assert
   `p == offset`. (Verified reachable: mock `position` reads `subscriptions.position_or_null`;
   `seek` sets a valid position; when a position is set, `position` returns it directly.)
   Mirrors the shipped `ReadyToPoll`-style setup in the public round-trip tests.

**Operational failure (message asserted, DoD §3):**

2. **`Position` on an unassigned partition faults with `KafkaException`.** `Assign` a
   different partition (or none), `await Position(tp)` for an unassigned `tp` → the
   `Task` faults with a `KafkaException` whose message is
   `"You can only check the position for partitions assigned to this consumer."`
   (assert the **message content**, verified against `mock_consumer.rs::position`).
   Confirms the failure path frees the error handle exactly once and faults (not
   throws).

**Cancellation / wakeup:**

3. **`wakeup()` during an in-flight `Position`** → the `Task<long>` cancels/faults
   **once**, then the consumer is reusable (§B5). *(Reachability note: mock ops resolve
   instantly, so a deterministic in-flight overlap is the same D-Q4 ceiling as poll —
   assert the reachable seam + the reusable-after-wakeup property; the shipped
   `ConsumerPollWakeupCancelTests` is the precedent for what is deterministically
   assertable.)*
4. **A pre-canceled token → `OperationCanceledException` synchronously** — pass a
   `new CancellationToken(canceled: true)`; `Position(tp, token)` throws
   `OperationCanceledException` before any submit. Deterministic.

**Preconditions / lifecycle (deterministic):**

5. **Null topic → `ArgumentNullException`** — `Position(new TopicPartition(null!, 0))`
   throws before any native call.
6. **Negative partition → `ArgumentOutOfRangeException`** — `Position(new
   TopicPartition("t", -1))` throws before any native call (the `Seek` precedent).
7. **Post-dispose → `ObjectDisposedException`** — after `Dispose`/`DisposeAsync`,
   `Position(tp)` throws `ObjectDisposedException` (the `ThrowIfClosed` gate).

**Concurrency (same ceiling as shipped ops):**

8. **Concurrent async op → faulted `Task` (`ConcurrentModification`).** As with every
   shipped async op (D-Q4), a *forced* submit-overlap is not deterministically
   reproducible broker-free (mock ops resolve instantly; the one guard-holding op with
   a controllable duration is `poll`). Assert the reachable seam (the read round-trips
   on a free guard) and verify the core-rejection → faulted-`Task` mapping **by code
   inspection** of the shared `Complete(error)` path (reused verbatim from poll),
   documented in `COMMENTS.DONE.11.md` mirroring the D-Q4 / M5/P1 precedent. Honest cost
   noted: same non-deterministic ceiling as the shipped state reads/ops.

**Allocation sanity (DoD §10 spirit, §7.4 — per-RPC, NOT hot-path):**

9. **Per-op allocation sanity check.** `Position` is a per-RPC top-level API surface,
   not a hot path (CLAUDE.md §11 / consumer-threading §2 — the `Consumer` dispatch
   surface is explicitly per-RPC), so a `Task<long>` + one `GCHandle` +
   `OperationCompletionSource` per call is amortized and fine. A light sanity bound in
   the style of the existing per-op checks — **not** a zero-alloc assertion. Confirms no
   *unbounded* / per-something allocation, no accidental copy of the topic per result.

**Explicitly flagged as not reachable broker-free with today's mock** (recorded, not
silently skipped): a **deterministic forced-concurrency / in-flight-wakeup overlap**
(needs a controllable-duration guard-holding mock op — a Rust-core dependency; the M3/P3
/ D-Q4 / M5/P1 precedent). The `position(tp, Duration)` timeout behavior is not
testable because there is no timed ABI form (the overload is deferred, §2).

---

## 7 · Definition of Done (Actor must pass all)

1. `cargo build --features ffi` — native cdylib + header present (run FIRST, §7.1). **No
   ABI change (Mode A);** `position_async` + `position_callback_t` already ship (§0).
2. `dotnet build` — 0 warnings / 0 errors across all library TFMs (netstandard2.0,
   net8.0, net10.0) and all test TFMs (net462, net8.0, net10.0);
   `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + `GenerateDocumentationFile`
   active. **CS1591 satisfied on the new public member** (`IAsyncConsumer.Position` +
   the two forwards). Apache-2.0 header on every new/edited file; no TODO/FIXME.
3. `dotnet test` — all pass (previous count + the new `PublicConsumerPositionTests`);
   the D8.8 stability gate (multiple full-suite runs green, parallelism disabled) holds.
4. `dotnet format --verify-no-changes` — clean.
5. DoD §3 parity: error/behavior asserted — the unassigned-partition `KafkaException`
   **message** content, the happy-path offset value, the precondition exception types,
   the pre-canceled `OperationCanceledException`. Not-reachable Java behavior explained
   (forced concurrency; the `Duration` overload with no timed ABI).
6. Consumer-trait-surface check (DoD §11): `Position` is a single async method on
   `IAsyncConsumer` (a per-RPC dispatch surface — `Task<long>` + one `Box`-equivalent
   bridge per call is fine, CLAUDE.md §11 / consumer-threading §2); **no `block_on`
   façade** (the sync `Consumer_position` is deliberately NOT declared); the scalar
   trampoline is a sync no-throw boundary; no enum dispatch. The `Deserializer` /
   per-record paths are untouched (no `#[async_trait]` concerns — not that layer).
7. **Free-exactly-once audit (the scalar bridge's central obligation):** the per-op
   `GCHandle` is freed exactly once on **every** path (success / operational failure /
   inline core-rejection / no-throw catch / submit-threw-`AbandonBeforeSubmit`); the
   error handle is freed exactly once on the failure path (via `FromHandle`); there is
   **no** result-handle free (the scalar owns none) — verify the `OnPosition` `finally`
   does NOT call any `*Destroy` (the one structural difference from `OnPoll`).

---

## 8 · Governance / mechanics

- **N=11** this phase. `dotnet-actor` / `dotnet-critic` personas (copied to repo-root
  `.claude/agents/` per the discovery workaround; re-copy after any persona edit).
  Comments: `bindings/dotnet/COMMENTS.11.md` (working) → `COMMENTS.DONE.11.md`
  (resolved, **not** committed at the binding root; the Manager archives a copy under
  this phase directory at handoff).
- **Deviations / decisions to record in `COMMENTS.DONE.11.md`:** (a) reuse
  `OperationCompletionSource<long>` verbatim for the scalar bridge (no new context
  type, no bridge-file change); (b) `SubmitScalarOperation<T>` added as a parallel
  helper rather than generalizing `SubmitOperation` (to leave the proven poll path
  untouched); (c) one `Position` method, no `TimeSpan` overload (the shipped `Close`
  precedent for a missing timed ABI) — the timed overload is deferred until the ABI
  exposes a timed `position_async`; the `CancellationToken` is **user-initiated
  cancellation only, NOT a timeout/deadline**; (d) the
  sync `Consumer_position` deliberately not declared (async-only; no sync-over-async);
  (e) the D-Q4-style non-deterministic-concurrency + no-timed-ABI reachability limits.
- **Doc-sync commit:** update `STATUS.md` (new **M5/P2** phase entry, N=11) and the
  `IAsyncConsumer` remarks (remove `position` from the "not-yet-wired" list). The
  CLAUDE.md §3 sketch already shows `Position` on `IAsyncConsumer` — confirm it matches
  the shipped signature (`Task<long> Position(TopicPartition, CancellationToken)`); no
  sketch change expected.
- **Branching:** commits land on **`prashah_dev_public_consumer_remaining`** (the M5
  branch; M5/P1 already shipped there). Treated as a new PR for M5/P2.

---

## 9 · Resolved (user review, 2026-08-06)

All open questions are answered — the plan above reflects them:

1. **Milestone/phase label → M5/P2 "Consumer Position"** (slug `P2-consumer-position`).
2. **`Duration`/timeout → one `Position` method, NO `TimeSpan` overload.** The timed
   `position(tp, Duration)` overload is **deferred** until the C ABI exposes a timed
   `position_async`; then `Position(TopicPartition, TimeSpan)` lands as an additive
   overload (the shipped `Close` precedent). The **`CancellationToken` is user-initiated
   cancellation only — NOT a timeout/deadline**; its sole intent is for the caller to
   cancel the request (→ `wakeup()`, best-effort). The binding does not derive or
   document a deadline from it.
3. **Negative-partition → reject before the native call with
   `ArgumentOutOfRangeException`** (the shipped `Seek` / `Assign` / `AddRecord`
   precedent).
4. **Submit-helper → parallel `SubmitScalarOperation<T>`** (leave the proven poll
   `SubmitOperation` untouched).

**Awaiting:** final user approval to run the N=11 Actor → Critic loop.
