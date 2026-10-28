# M3/P1 — Completion bridge + first async op (consumer, proof-of-plumbing)

**Status:** APPROVED (2026-07-27). First async phase of the .NET binding.
**Mode:** A (consumer C ABI already landed — no Rust authoring; add `_async`/callback
DllImports + read the generated header).
**Review counter:** N=5 (global monotonic; M0/P0=1, M1/P1=2, M2/P1=3, M2/P2=4, M3/P1=5).
**Builds on:** M2 — public un-sealed `KafkaException` (`FromHandle`), Category-1
`SafeConsumerHandle`, internal `NativeConsumer` create → close → destroy lifecycle.

De-risks the foreign-thread callback → `Task` bridge — the riskiest new machinery
(`RunContinuationsAsynchronously`, GCHandle keep-alive across submit→fire, the no-throw
boundary, the one-op-in-flight guard, wakeup, async-aware teardown) — BEFORE poll and the
receive path. This activates the `_async`/callback ABI variants for the first time.

---

## Scope

**In scope:** the async-completion machinery (ffi §B1/§B6/§B7) + operation error/threading
additions (§B5), **consumer, internal-only**. One void-result completion bridge, exercised on
both success and failure. Wakeup/cancellation. Async-aware teardown. The N=5-deferred teardown
thread-safety hardening (deferred → done this phase).

**Explicitly deferred (do NOT build):** `poll` and the entire receive path (Category 3/4 handles,
`ConsumerRecord(s)`, length-delimited `out_len` strings §B3, copy-out §6.4); all other async ops
beyond the proof pair; any public client surface (`IConsumer`/`KafkaConsumer`/public
`MockConsumer`/`ConsumerGroupMetadata`/`ConsumerRecord(s)`/rebalance listeners) — the lifecycle +
bridge stay INTERNAL, no new public type (`KafkaException` remains the only public type); all
producer interop + the §A7 producer completion decision; serializers / generic
`Consumer<TKey,TValue>`; typed `KafkaException` subclasses.

---

## Source-verified findings (ground truth for the Actor)

1. **Void-result callback ABI:** `kafka_consumer_Consumer_op_callback_t =
   void(*)(kafka_common_KafkaError_t*, void* user_data)` — non-null `KafkaError*` = failure,
   null = success, **no result handle to marshal**. `src/ffi/consumer.rs:2638` + header.
2. **Rust submit contract** (`async_void_op`, `src/ffi/consumer.rs:2514`): `acquire()` the access
   guard; **on guard failure the callback fires INLINE on the caller thread with a
   `ConcurrentModification` error** (guard not taken); on success it spawns the op, awaits it,
   then a completion job **releases the guard BEFORE firing the callback** on the **foreign
   dispatcher thread**. The core already owns the guard; the .NET guard is a typed-exception
   mirror + single-TCS/GCHandle protector, NOT a serializer.
3. **`Consumer_wakeup`** (`src/ffi/consumer.rs:523`): sync, **bypasses the guard**, callable from
   any thread. **`Consumer_destroy`** (`:569`): does NOT join, does NOT guard — a bare destroy
   before the in-flight callback fires HANGS the `Task` + leaks the GCHandle → teardown MUST
   drain via `close_async` first.
4. **Broker-free error codes are indistinct:** `Wakeup`, `ConcurrentModification`, `IllegalState`,
   `IllegalArgument` all return `code() == UnknownServerError == -1`
   (`src/common/kafka_error.rs:430`, `src/common/protocol/errors.rs:31`). **Tests must assert on
   exception TYPE + `Message` + one-shot/reusability, NEVER on a distinctive `Code`.**
5. **`MockConsumer` broker-free behaviors:** `subscribe(["t"])` succeeds
   (`src/consumer/mock_consumer.rs:461`); `seek()` on an **unassigned** partition returns a
   genuine `IllegalState` error broker-free (`:739` → `subscriptions.seek(...)?`). No single op
   yields both success and failure broker-free without setup — hence one shared bridge exercised
   by two thin wrappers.
6. **M2 assets:** `NativeConsumer` (internal `IDisposable`, non-atomic `_disposed`,
   `Consumer_close_with_timeout`→`destroy`); public un-sealed `KafkaException.FromHandle(IntPtr)`
   (reads code / message-before-free / flags, `_destroy` in `finally`, freed exactly once);
   `SafeConsumerHandle`; `TestTimeout.Run(Action, TimeSpan)` sync hang-guard (a `Task`/async
   overload will be added this phase).

---

## Proof op (APPROVED decision)

**One void-result bridge, two thin `op_callback_t` wrappers (same delegate, same machinery):**

- **SUCCESS:** `subscribe_async(["proof-topic"])` on a `MockConsumer` → foreign-dispatcher
  completion, null error, GCHandle+delegate freed once.
- **FAILURE:** `seek_async(unassigned tp)` → genuine core `IllegalState` error through the
  foreign dispatcher → `Task` faults via `KafkaException.FromHandle`, freed once.

The Actor confirms `subscriptions.seek` on an unassigned partition returns `Err` on
`MockConsumer` (verified above) before wiring the failure test.

---

## Files

**New (library, all `internal`; `Internal/` = the visibility marker; `unsafe` only under
`Internal/Interop/`):**

- `src/Confluent.Kafka.ShareConsumer/Internal/OperationCompletionSource.cs` — callback→TCS
  context. `TaskCompletionSource` built with `RunContinuationsAsynchronously`; back-ref for guard
  release; freed-exactly-once book-keeping (idempotent free of its own GCHandle).
- `src/Confluent.Kafka.ShareConsumer/Internal/ConsumerAccessGuard.cs` — one-op-in-flight managed
  guard (`Interlocked` flag): concurrent async op → `KafkaException` (ConcurrentModification);
  concurrent sync state read → `InvalidOperationException`; released just before the callback
  fires.
- `src/Confluent.Kafka.ShareConsumer/Internal/Interop/ConsumerCallbacks.cs` — the
  `[UnmanagedFunctionPointer(CallingConvention.Cdecl)]` `OperationCallback(IntPtr error,
  IntPtr userData)` delegate type + one `static readonly` rooted instance; no-throw body.

**Modified:**

- `src/Confluent.Kafka.ShareConsumer/Internal/Interop/NativeMethods.cs` — DllImports below.
- `src/Confluent.Kafka.ShareConsumer/Internal/NativeConsumer.cs` — void async-op submit helper;
  the two proof-op methods (`SubscribeAsync`, `SeekAsync`); `Wakeup()`;
  `IAsyncDisposable.DisposeAsync()`; thread-safe teardown guard (folds in N=5 deferred item);
  a representative internal sync state read participating in the guard (for the
  `InvalidOperationException` test — least-scope option, e.g. reuse the existing internal
  group-metadata read).
- `tests/Confluent.Kafka.ShareConsumer.UnitTests/**` — new test files + `TestTimeout` async
  overload.
- `bindings/dotnet/design/current/STATUS.md` — handoff; move "Deferred hardening (N=5) — teardown
  thread-safety" from deferred → done.

---

## DllImport additions (full ABI symbol as `EntryPoint`; ffi §0.1 type map)

- `kafka_consumer_Consumer_subscribe_async(IntPtr consumer, IntPtr[] topics, int count,
  OperationCallback cb, IntPtr userData)` — `const char* const*` = `IntPtr[]` of pinned
  NUL-terminated UTF-8 buffers (pin call-scoped; freed after submit only if the ABI copies —
  confirm subscribe copies the topic strings synchronously, else keep pinned until the callback).
- `kafka_consumer_Consumer_seek_async(IntPtr consumer, IntPtr topicUtf8, int partition,
  long offset, OperationCallback cb, IntPtr userData)`.
- `kafka_consumer_Consumer_wakeup(IntPtr consumer)`.
- `kafka_consumer_Consumer_close_async(IntPtr consumer, OperationCallback cb, IntPtr userData)`.
- (`Consumer_close_with_timeout` / `_destroy` already present from M2.)

Type map: opaque `*_t` / `const char*` → `IntPtr`; `int32_t` → `int`; `int64_t` → `long`;
`bool` → `[MarshalAs(I1)]`; out-param → `out IntPtr`; `void(*cb)` → kept-alive Cdecl delegate;
`void* user_data` → `IntPtr` (GCHandle). No `[LibraryImport]`/`delegate* unmanaged`/`LPUTF8Str`.

---

## Bridge design (ffi §B6/§B7)

**Submit:** managed guard `Enter()` (concurrent → typed throw) → `GCHandle.Alloc(context, Normal)`
→ P/Invoke `*_async(handle, …, s_callback, (IntPtr)gcHandle)` → return `context.Task`. The
delegate is rooted via a `static readonly` field; the context (holding the TCS) is rooted via the
per-op GCHandle from submit until the callback fires (the whole op).

**Callback (no-throw, may run on the foreign dispatcher thread OR inline on guard rejection):**
```
try {
    context = recover from GCHandle(userData)
    context.ReleaseGuard()                 // release BEFORE completing the TCS
    if (error != IntPtr.Zero)
        context.Tcs.TrySetException(KafkaException.FromHandle(error))  // FromHandle owns+frees the handle
    else
        context.Tcs.TrySetResult(default)
}
catch (Exception ex) { context?.Tcs.TrySetException(ex); /* never unwind into native */ }
finally { free the GCHandle exactly once (idempotent) }   // same on the inline guard-rejection path
```
- `RunContinuationsAsynchronously` is MANDATORY — the callback runs on the foreign dispatcher
  thread; the awaiter's continuation must not run there (would stall/deadlock the core).
- Free the `KafkaError` (via `FromHandle`'s `_destroy`) + the GCHandle **exactly once on EVERY
  path**, including inline guard-rejection.

---

## Operation error + threading (ffi §B5)

- **Managed one-op-in-flight guard** (`ConsumerAccessGuard`, `Interlocked`): a concurrent **async
  op** → `KafkaException` (ConcurrentModification); a concurrent **sync state read** →
  `InvalidOperationException`. Released just before the callback fires (held for the
  submit→op-complete window). Mirrors — does not replace — the core guard; the callback still
  handles the core's inline rejection for free-exactly-once.
- **`wakeup()`** (sync, `Consumer_wakeup`): the in-flight op surfaces a flat `KafkaException`
  (Wakeup semantics) **once**, then the consumer is reusable (core owns one-shot). Distinct from
  **`CancellationToken`** → mapped to `wakeup()` internally, but the resulting fault is
  translated managed-side to `OperationCanceledException` (track that the wakeup was
  token-initiated for this op).

---

## Teardown (ffi §B7; un-defers M2/P1 D3; folds N=5 hardening)

- **`IAsyncDisposable.DisposeAsync()` (PRIMARY):** if an op is in flight → `wakeup()` + `await`
  the in-flight `Task` (drain) → `Consumer_close_async` (await; graceful, joins the bg task) →
  `SafeConsumerHandle.Dispose()` (`Consumer_destroy`).
- **`IDisposable.Dispose()` (BLOCKING fallback, shape unchanged):** direct blocking
  `Consumer_close_with_timeout` → destroy. NOT sync-over-async.
- **Thread-safe teardown guard:** replace the non-atomic `_disposed` bool with a thread-safe
  closed/disposed check (`SafeHandle.IsClosed` or an atomic flag) + the §B5 access guard →
  concurrent/double `Dispose`/`DisposeAsync` safe. Do NOT convert close/destroy to SafeHandle
  P/Invoke params or add manual AddRef (diverges from CKD; per-call cost on future hot ops).

---

## Tests (TEST project; internals via `InternalsVisibleTo`; `Interop/` layout; every awaited op
AND teardown wrapped in a `TestTimeout` hang-guard)

- **Bridge SUCCESS:** submit `subscribe_async` on `MockConsumer` → `Task` resolves; error(null) +
  GCHandle freed once (churn in a loop — corruption detector).
- **Bridge FAILURE:** `seek_async(unassigned tp)` → `Task` faults with `KafkaException`
  (assert `Code == -1`, flags, `Message` via `FromHandle`); handles freed once.
- **`RunContinuationsAsynchronously`:** the awaiter's continuation does NOT run inline on the
  dispatcher thread (assert a different managed thread id, or a continuation that re-enters the
  consumer does not deadlock).
- **No-throw:** an exception thrown inside the callback is caught (faults the `Task`), no crash /
  no unwind into native.
- **Concurrency:** concurrent async op → `KafkaException` (ConcurrentModification); concurrent
  sync state read → `InvalidOperationException`.
- **Wakeup/cancel:** `wakeup()` during the in-flight op → `Task` faults with `KafkaException`
  (Wakeup) ONCE, then the op works again; a `CancellationToken` cancel →
  `OperationCanceledException`.
- **Teardown no-hang:** `DisposeAsync` (and `Dispose`) with an op in flight RETURNS, under
  `TestTimeout`.
- **GCHandle keep-alive:** aggressive `GC.Collect()`/`WaitForPendingFinalizers` during an
  in-flight op does not collect the delegate/TCS.
- **Concurrent / double `Dispose`/`DisposeAsync`** is safe (the thread-safe closed guard).

---

## Verification gates (.NET DoD — NOT `cargo xtask` / `make verify`)

1. `cargo build --features ffi` succeeds (native + header present) — run FIRST.
2. `dotnet build` — 0 warnings / 0 errors across library TFMs (netstandard2.0;net8.0;net10.0) +
   test TFMs (net8.0;net10.0). `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` active. No new
   public type → no new CS1591 surface. Apache-2.0 header on new `.cs` files. No TODO/FIXME.
3. `dotnet test -f net10.0` — bridge / op / wakeup / teardown pass AND do NOT hang.
4. `dotnet format --verify-no-changes` — clean.
5. CI-only caveat: this machine has only the .NET 10 runtime → net8.0 test RUN + net462 are
   CI-only (build legs must pass; do NOT block DoD on those runs).

---

## Governance

- Agents: `dotnet-actor` (N=5) implements; `dotnet-critic` (N=5) reviews. NEVER the Rust
  actor-executor / kafka-critic.
- Review: working `bindings/dotnet/COMMENTS.5.md` → resolved to
  `bindings/dotnet/COMMENTS.DONE.5.md`. NEVER commit a `COMMENTS.<digit>.md` (gitignored shape).
- Tracking: binding-local personas (`bindings/dotnet/.claude/agents/dotnet-{actor,critic}.md`)
  stay TRACKED; repo-root `.claude/agents/dotnet-{actor,critic}.md` stay UNTRACKED;
  `bindings/dotnet/.claude/agent-memory/**` excluded from every commit/PR (only `.gitkeep`).
  Verify each commit's staged file list.
- Commit style: small incremental commits, each passing the .NET gates; Apache-2.0 header on new
  files.
