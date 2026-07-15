# .NET binding — FFI marshalling & ownership

Deep-dive rules for marshalling across the C ABI (`confluent_kafka.h`,
`kafka_producer_*` / `kafka_common_KafkaError_*`) from .NET via P/Invoke.
Supplements `bindings/dotnet/CLAUDE.md`. Review ground truth is the **C ABI
header** and the **Kafka Java public API** — never librdkafka or the existing
`confluent-kafka-dotnet` client (that wraps a *different* C ABI).

Each numbered section is one decision: the **Rule**, **Why**, **How to apply**,
**Anti-patterns**, and **Tests required**.

---

## Thread topology & thread-safety (shared context)

The whole-system picture that every numbered section below assumes. The C ABI
hides all Kafka I/O on native threads **owned by the Rust core**; the binding
adds only the caller threads and a single completion pump.

```
   .NET (managed)                    │ C ABI │       Rust core (native, per producer)
   ─────────────                     │       │       ─────────────────────────────────
   caller thread(s):                 │       │   tokio multi-thread runtime (worker POOL)
     SendAsync → Producer_send ──────│──────►│     RecordAccumulator (enqueue, returns fast)
   completion pump (1 bg thread):    │       │   Sender task (spawned once in _new):
     get_all(futures) ─block_on─────►│──────►│     NetworkClient + ONE async Selector
     ◄── per-message metadata/err ───│◄──────│       ↕ multiplexes ALL brokers (event-driven)
   Dispose: join pump → flush/close  │       │   (runtime+task created in KafkaProducer_new,
            → destroy handle         │       │    dropped on Producer_destroy)
```

**Rule:**

  - Behind the C ABI, each real `KafkaProducer` owns a **multi-threaded tokio
    runtime** (a small worker-thread pool) plus **one spawned Sender task** that
    owns the network stack and does all Kafka protocol I/O over a **single async
    Selector** (all brokers multiplexed — NOT one thread per broker). These
    native threads are created in `kafka_producer_KafkaProducer_new` and torn
    down by `kafka_producer_Producer_destroy`.
  - On the .NET side there are only: the **caller thread(s)** and **exactly one
    completion pump thread** (see §1). No per-send threads.
  - **There is no mandatory poll loop.** The core self-drives its I/O; the pump
    exists only to harvest per-message futures. If the pump stalls, sends still
    flow to the broker — only result delivery back to .NET pauses.
  - FFI calls may originate from any .NET thread. The core serializes them behind
    the producer handle's internal `Mutex`, so concurrent `SendAsync` is safe —
    but do NOT add your own lock around FFI calls (the core already locks).
  - `Producer_send` / `_get*` call `block_on` on the **calling** .NET thread.
    Because the runtime is multi-threaded, this parks only that one .NET thread;
    the Sender keeps running on the runtime's worker threads. A blocked pump does
    NOT stall sending.
  - Any native → managed callback runs on a **non-.NET-managed thread**:
    `RecordMetadata_copy`'s callback fires synchronously on whoever called it
    (the pump); a future push-completion callback (see §1 "Future direction")
    would fire on a tokio worker thread. Treat all such callbacks as foreign-
    thread (keep delegates alive, catch every exception, hop user continuations
    off the callback thread).

**Why:** This is Java's single-Selector NIO model translated to tokio
(CLAUDE.md §8) — deliberately unlike librdkafka, which runs a "main" thread plus
**one blocking thread per broker** and *requires* the app to pump
`rd_kafka_poll` (confluent-kafka-dotnet does this with a `LongRunning`
`callbackTask`). Consequences of the difference: our native thread count does
**not** grow with cluster size; there is **no** poll loop to run; and results
arrive as independent per-message futures, not through a shared delivery-report
event queue. The multi-threaded runtime is also why `block_on` from a .NET
thread is safe and cannot deadlock the Sender.

**How to apply:**

  - Keep exactly one pump thread; never spawn a thread (or `Task.Run`) per send.
  - Never dispose the producer `SafeHandle` while the pump may still touch
    futures derived from it — join the pump first (§1, Dispose).
  - Do not model a `confluent-kafka-dotnet`-style poll loop — there is no
    `poll()` in this ABI, and nothing needs pumping to make progress.
  - Do not hold a managed lock across any blocking FFI call (`_get_all`,
    `_flush`, `_close`).
  - Treat every native → managed callback as arriving on a foreign thread (see
    the Callback and §1 Async sections).

**Anti-patterns to flag in review:**

  - A .NET "poll loop" thread modeled on `confluent-kafka-dotnet`'s
    `callbackTask` — there is nothing to poll here.
  - Assuming one native thread per broker, or that native thread count scales
    with cluster size.
  - Wrapping FFI calls in a binding-side lock "for safety" (double-locking; the
    core already serializes via its `Mutex`).
  - Assuming FFI calls are single-threaded, or that completions arrive on the
    caller's thread.
  - Doing user work directly on a callback thread (a tokio worker).

**Tests required:**

  - Concurrent `SendAsync` from multiple .NET threads is safe and correct
    (core `Mutex` serialization holds).
  - A long-blocked pump (slow/paused broker) does not prevent new sends from
    being accepted and enqueued (the self-driving runtime property).
  - `Dispose` joins the pump before destroying the handle — no use-after-free
    (shared with §1).

---

## 1. Async / Future completion — pull-based pump + `TaskCompletionSource`

The C ABI is **pull-based**: the only ways to learn a send resolved are the
blocking `kafka_producer_FutureRecordMetadata_get` / `_get_all` or the
non-blocking poll `kafka_producer_FutureRecordMetadata_is_done`. There is **no
push completion callback** in the ABI today. Java's `Future<RecordMetadata>`
maps to .NET `Task<RecordMetadata>`, completed by a dedicated background
**completion pump** — mirroring the Python binding's `poll_futures_thread`
(see `bindings/python/.claude/rules/python-ffi.md` §6 + "Thread topology").

### Thread topology

```
Caller thread                         Completion pump (one bg thread)
─────────────                         ───────────────────────────────
SendAsync():                          loop:
  pin key/value (call-scoped)           drain a batch of (future, tcs)
  Producer_send() → future handle       get_all(futures[])   ← BLOCKS here
  new TaskCompletionSource (tcs)        for each i:
  enqueue (future, tcs)                   tcs[i].SetResult / SetException
  return tcs.Task  (no thread parked)   destroy_all(futures)
Dispose(): signal + join the pump ◄──── on shutdown: drain, fault pending, exit
```

**Rule:**

  - A method that blocks in Java returns a `Task<T>` backed by a
    `TaskCompletionSource<T>`; the caller-facing method only enqueues work and
    returns. It NEVER calls a blocking core function (`_get` / `_get_all`)
    directly on the caller's thread.
  - Exactly **one** dedicated background pump thread does all blocking waits,
    via the batched `kafka_producer_FutureRecordMetadata_get_all`. The thread
    count is O(1) regardless of in-flight send count.
  - `kafka_producer_Producer_send` runs inline on the caller's thread (it is a
    fast enqueue); only the *wait* is offloaded. Unlike Python, .NET has no GIL,
    so a separate send-batching thread is NOT required. (A send-batching thread
    that uses `_send_batch` is an optional throughput optimization, not a
    baseline requirement.)
  - Construct the `TaskCompletionSource` with
    `TaskCreationOptions.RunContinuationsAsynchronously`.
  - Completion runs on the pump thread, not the caller's — this is the .NET
    analog of Java's send-callback contract. Document the thread affinity.
  - Completion is exactly-once. Guard against completing an already-completed or
    cancelled `Task`, and free the native handles on every path.
  - **Cancellation is best-effort:** cancelling the returned `Task` does NOT
    abort an in-flight send (the record is already enqueued in the core); it
    only discards the result and frees its handles.

**Why:** The core owns its own Tokio runtime and background sender task, so the
"drive the work" loop already lives in Rust — the binding must only bridge each
resolved future to a `Task`. A pull ABI forces the wait onto *some* thread; a
single pump gives bounded threads for unbounded in-flight sends. The
alternative — `Task.Run(() => FutureRecordMetadata_get(...))` per send — parks
one thread-pool thread per in-flight message, which starves the pool under the
high in-flight concurrency a Kafka producer is designed for (sync-over-async
anti-pattern). `RunContinuationsAsynchronously` is required because, by default,
`tcs.SetResult` runs the awaiter's continuation **synchronously on the pump
thread**; a slow user continuation would then stall completion of every other
send. This design matches CLAUDE.md §11 ("avoid per-message spawn; use a shared
completion task with a channel") and keeps the .NET binding consistent with the
Python design.

**How to apply:**

  - `SendAsync`: pin key/value bytes (call-scoped, see the pinning section),
    call `kafka_producer_Producer_send` to get the future handle, check the
    synchronous `out_error` (throw `KafkaException` on non-null), create the
    `TaskCompletionSource`, enqueue `(future, tcs)`, return `tcs.Task`.
  - Use a `System.Threading.Channels.Channel` (or `BlockingCollection`) as the
    hand-off queue; the pump drains a batch (FIFO), calls `_get_all`, and
    completes each `tcs`.
  - Read result fields immediately and free the native handles on the pump
    thread: on success, extract offset/partition/topic/timestamp then
    `RecordMetadata_destroy` (or use `RecordMetadata_copy`, which extracts +
    frees in one call); on error, read `KafkaError_code` / `_message` /
    `_is_retriable` then `KafkaError_destroy`. The managed `RecordMetadata` /
    `KafkaException` hold **copied values**, not handles — so they need no
    `SafeHandle`.
  - Free the future handles with `kafka_producer_FutureRecordMetadata_destroy_all`
    after `_get_all` returns (the futures are NOT consumed by `_get_all`).
  - `Dispose` / `close`: stop accepting sends, let the pump drain (Java
    `close()` flush semantics) or fault the still-pending `Task`s with a
    `KafkaException`, then **join the pump thread**, then
    `kafka_producer_Producer_flush` → `_close` → dispose the producer
    `SafeHandle`. Never destroy the producer handle while the pump may still be
    calling `_get_all` on futures derived from it.
  - Optional fast path: if `kafka_producer_FutureRecordMetadata_is_done` is true
    at send time (auto-complete mock, or an already-acked send), complete
    synchronously and return a completed `ValueTask` without queueing.

**Anti-patterns to flag in review:**

  - `Task.Run(() => ...FutureRecordMetadata_get...)` per send, or any
    one-thread-per-in-flight-message pattern (Option A).
  - A caller-facing `SendAsync` that blocks on `_get` / `_get_all` directly.
  - `TaskCompletionSource` created without `RunContinuationsAsynchronously`.
  - Running user continuations / user code on the pump thread.
  - Completing a cancelled or already-completed `Task` without freeing the
    metadata/error handles.
  - Destroying the producer handle before draining + joining the pump.
  - Assuming `Task` cancellation aborts the in-flight send.
  - Holding a managed lock across a blocking `_get_all` FFI call.

**Tests required:**

  - `SendAsync` `Task` resolves with `RecordMetadata` on success and faults with
    `KafkaException` on error.
  - `Dispose`/`close` called with sends still in flight returns (does not hang) —
    the pump-join regression.
  - Cancelling a pending `Task` is safe and frees its native handles.
  - High-concurrency produce (many more in-flight sends than thread-pool
    threads) completes without thread-pool starvation.

---

## 2. P/Invoke declarations & type mapping

All native calls go through one central `internal static class Native`. The
binding **multi-targets `netstandard2.0` + `net8.0` + `net10.0`** — netstandard2.0
is the floor (consumed by **.NET Framework 4.6.2** and .NET Core); net8.0 and
net10.0 are first-class targets, not afterthoughts.

We use **one uniform set of classic `[DllImport]` declarations across every
target.** `[DllImport]` is fully supported on net8/net10 (it is not "the old
way" — `[LibraryImport]` is only a source-generated optimization over the same
P/Invoke machinery), so a single declaration set is the one mechanism valid
across the whole matrix. Because netstandard2.0 / net462 is in that matrix, the
interop features added later — source-generated `[LibraryImport]`, C# function
pointers (`delegate* unmanaged`), `[UnmanagedCallersOnly]`,
`UnmanagedType.LPUTF8Str`, and `Marshal.PtrToStringUTF8` — **must not be used**:
they do not exist on the floor, and adopting them would fork the declarations.
This is the same classic toolkit `confluent-kafka-dotnet` uses (it also supports
net462), pointed at our fixed-width, handle-error ABI.

**Deliberate non-goal:** a `[LibraryImport]` specialization under
`#if NET7_0_OR_GREATER` for AOT/trimming on the modern TFMs. Our signatures are
already mostly blittable (`IntPtr` / `int` / `long` + manual UTF-8 and pinning),
so the gain is small and not worth maintaining two parallel declaration sets.
Revisit only if NativeAOT becomes a hard requirement.

### C ABI → C# type map

| C ABI type (ours) | C# P/Invoke type | Notes |
|---|---|---|
| `int32_t` | `int` | 1:1 blittable. **Never `UIntPtr`/`nint`** — our ABI has no `size_t`. |
| `int64_t` | `long` | 1:1 blittable. |
| `bool` | `[MarshalAs(UnmanagedType.I1)] bool` | C `bool` is 1 byte; default marshals 4-byte Win32 `BOOL`. |
| opaque `*_t *` | `IntPtr` (wrapped in a `SafeHandle` one layer up) | Never model the `_t` typedef as a C# struct. |
| `const char *` **in** | `IntPtr` to a hand-pinned, NUL-terminated UTF-8 buffer | See UTF-8 rule below — no `LPStr`, no `LPUTF8Str`. |
| `const char *` **out** (callee-owned) | `IntPtr` → our own `Utf8.PtrToString` | Never a `string` return (marshaller would free it). |
| `const uint8_t *` + `int32_t len` | `IntPtr` (hand-pinned `byte[]`) + `int` | Zero-copy; call-scoped pin (§ zero-copy). |
| `T **` out-param | `out IntPtr` | e.g. `out_error`. |
| `T **` as array | `IntPtr[]` | e.g. `get_all` futures / out_metadata / out_errors. |
| `struct ProducerRecord_t` | `[StructLayout(LayoutKind.Sequential)] struct`; array as `ProducerRecord_t[]` | Fixed-width ⇒ identical layout. |
| `void (*cb)(...)` | a `[UnmanagedFunctionPointer(CallingConvention.Cdecl)]` **delegate**, kept alive | No function pointers / `[UnmanagedCallersOnly]` on this floor. |
| `void *` | `IntPtr` | Opaque `user_data` (a `GCHandle`). |

**Rule:**

  - Every declaration is
    `[DllImport("confluent_kafka", CallingConvention = CallingConvention.Cdecl)]`
    on a `static extern` method in one `internal static class Native`. Cdecl
    matches cbindgen's `extern "C"`. The bare name `"confluent_kafka"` resolves
    to `confluent_kafka.dll` / `libconfluent_kafka.so` / `libconfluent_kafka.dylib`.
  - Use the type map above verbatim. Sizes/lengths are `int`/`long`, **never**
    `UIntPtr`/`nint`.
  - `bool` is always `[MarshalAs(UnmanagedType.I1)]` (params and
    `[return: ...]`).
  - **UTF-8 strings are marshalled by hand.** Provide two helpers and use them
    everywhere: `Utf8.Pin(string) -> (IntPtr ptr, GCHandle pin)` producing a
    NUL-terminated UTF-8 buffer for input, and `Utf8.PtrToString(IntPtr) ->
    string` (read bytes to the NUL, then `Encoding.UTF8.GetString`) for
    callee-owned output. Do NOT use `[MarshalAs(UnmanagedType.LPStr)]` (ANSI —
    corrupts non-ASCII topics) and do NOT reach for `LPUTF8Str` /
    `Marshal.PtrToStringUTF8` (absent on netstandard2.0 & net462).
  - Callbacks use a named `[UnmanagedFunctionPointer(CallingConvention.Cdecl)]`
    delegate type. Keep the delegate instance alive (static field, or a field on
    the owning object) for as long as native code can invoke it; a collected
    delegate → call into freed memory.
  - Opaque `*_t` handles are `IntPtr` tokens at the P/Invoke layer only; wrap
    them in a `SafeHandle` at the next layer up (§ handle ownership). Never
    declare a C# struct mirroring `_private[0]`.
  - Prefer `kafka_producer_ProducerProperties_put` over `_from_configs` to avoid
    marshalling a NUL-terminated `const char *const *` array.

**Why:** net462 and netstandard2.0 predate the modern interop surface, so the
classic `DllImport` toolkit is the only portable option — which is exactly why
`confluent-kafka-dotnet` (also net462) uses `[DllImport(..., Cdecl)]`, delegate
callbacks, and its own `Util.Marshal.PtrToStringUTF8` + `StringAsPinnedUTF8`
rather than the built-in UTF-8 marshallers. We borrow their *mechanism* but not
their *signatures*: our sizes are `int`/`long` (their librdkafka uses `size_t` →
`UIntPtr`), and our errors are opaque handle out-params (theirs are an
`ErrorCode` enum + `StringBuilder errstr` buffer). `bool` needs `I1` because the
default marshals a 4-byte Win32 `BOOL`. Returned `const char*` must stay an
`IntPtr` because auto string-marshalling of a return value assumes ownership and
would free memory the core still owns.

**How to apply:**

  - Put `Native` and the `Utf8` helpers in one place, compiled for all target
    frameworks (`<TargetFrameworks>netstandard2.0;net8.0;net10.0</TargetFrameworks>`).
    The same `[DllImport]` declarations serve every target — no per-TFM `#if`.
  - `SendAsync`: `Utf8.Pin` the topic, `GCHandle`-pin key/value, pass
    `out IntPtr outError`; free the pins after the call returns (§ zero-copy).
  - Error path: read `kafka_common_KafkaError_code` (→ `int`) and
    `_message` (→ `IntPtr` → `Utf8.PtrToString`), then `_destroy`.
  - Native library discovery / RID-specific assets are a separate concern
    (§ library loading). Note `NativeLibrary.SetDllImportResolver` does not exist
    on net462, so custom probing there needs the preload approach.

**Anti-patterns to flag in review:**

  - Using `[LibraryImport]`, `delegate* unmanaged`, `[UnmanagedCallersOnly]`,
    `UnmanagedType.LPUTF8Str`, or `Marshal.PtrToStringUTF8` — unavailable on
    netstandard2.0 / net462.
  - `[MarshalAs(UnmanagedType.LPStr)]` for a UTF-8 topic/config (ANSI
    corruption).
  - `UIntPtr` / `nint` for a length or count (copying librdkafka's `size_t`
    habit).
  - Marshalling a callee-owned `const char*` return as a C# `string`
    (double-free / heap corruption).
  - A callback delegate that can be GC-collected while native holds it.
  - A C# `struct` mirroring an opaque `_t`; copying key/value bytes through a
    marshaller instead of pinning (breaks zero-copy, §12).
  - Omitting `CallingConvention.Cdecl` (works on x64 by luck, breaks on x86).
  - Porting `confluent-kafka-dotnet`'s reflection-delegate loader / 3×
    NativeMethods copies — unnecessary for our single ABI.

**Tests required:**

  - A non-ASCII (UTF-8) topic round-trips correctly through `send` →
    `RecordMetadata.Topic` (guards the manual UTF-8 marshalling; catches an
    accidental `LPStr`).
  - Each `bool`-returning function (`is_done` / `is_retriable` / `is_fatal`)
    returns the correct value (guards a missing `I1`).
  - The binding builds and its `Native` layer loads on every target framework —
    **net462** (via netstandard2.0), **net8.0**, and **net10.0** (a TFM smoke
    test) — proving the single `[DllImport]` set works across the matrix.
  - Aggressive GC while callbacks are pending does not crash (guards delegate
    keep-alive).

---

## 3. Handle ownership & lifecycle (`SafeHandle`)

Every opaque handle the ABI hands out is heap-allocated in Rust via
`Box::into_raw` and must be returned to its matching `_destroy` exactly once, or
it leaks; freeing it twice or using it after free corrupts memory. Handles fall
into **two tiers** with different lifecycle strategies.

### Ownership map

| Handle | Created by | Lifetime | Freed by (C#) — *not consumed by anything else* |
|---|---|---|---|
| `Producer_t` | `KafkaProducer_new` / `MockProducer_new` | **long-lived** (one per client) | `SafeProducerHandle.ReleaseHandle` → `Producer_destroy`, via `Dispose` |
| `ProducerProperties_t` | `ProducerProperties_new` / `_from_configs` | **transient** (construction only) | the binding, right after `KafkaProducer_new` returns — **not** consumed by it |
| `FutureRecordMetadata_t` | `Producer_send` / `_send_batch` | **transient** (per send) | the pump: `FutureRecordMetadata_destroy_all` — **not** consumed by `get_all` |
| `RecordMetadata_t` | `FutureRecordMetadata_get` / `_get_all` | **transient** (per result) | the pump: `RecordMetadata_copy` (extract+free) or `_destroy` |
| `KafkaError_t` | any `out_error` / error slot | **transient** (per error) | the reader: read `_code`/`_message`/…, then `KafkaError_destroy` |

**Rule:**

  - **Tier 1 — long-lived → `SafeHandle`.** Wrap the producer (and, if wrapped,
    properties) in a `SafeHandle` subclass: `ownsHandle: true`, `IsInvalid =>
    handle == IntPtr.Zero`, `ReleaseHandle()` calls the matching `_destroy` and
    returns `true`. The runtime then guarantees the free runs **exactly once**,
    even on exceptions.
  - **Tier 2 — transient → read-and-free.** Do **not** wrap per-message handles
    (`Future` / `RecordMetadata` / `Error`) in a `SafeHandle`. Free them
    promptly and deterministically (in a `finally`) on the pump thread:
    `RecordMetadata_copy` extracts all fields **and** frees in one call;
    `KafkaError_destroy` after reading code/message; `FutureRecordMetadata_destroy_all`
    after `get_all`. The managed `RecordMetadata` / `KafkaException` hold
    **copied values**, never the handle (see §1).
  - `ProducerProperties_t` is **not consumed** by `KafkaProducer_new` — the
    binding still owns it and must free it after construction (success *or*
    failure), independent of the producer's lifetime.
  - `FutureRecordMetadata_get_all` does **not consume** the futures — you must
    still `FutureRecordMetadata_destroy_all` them, or leak one handle per send.
  - **Exactly-once:** after `get_all`, each index has exactly one non-null of
    `{metadata, error}` — free that one, plus the future (separately). Never free
    a handle twice; never touch it after freeing. `RecordMetadata_copy` frees the
    handle itself, so do not also `_destroy` it. All `_destroy` fns are
    null-safe, and `SafeHandle` skips `ReleaseHandle` when `IsInvalid`.
  - **Parent outlives children:** every `Future` / `RecordMetadata` is derived
    from the producer, so the producer must not be destroyed while any are live.
    Enforce via `Dispose` ordering (stop sends → join the pump → *then* release
    the producer handle — §1), not `DangerousAddRef` (we don't wrap the transient
    handles).
  - **Prefer explicit `Dispose` over the finalizer for the producer.**
    `Producer_destroy` **blocks** (dropping the tokio runtime waits for the
    Sender task to exit); blocking on the finalizer thread stalls finalization
    and the order relative to the pump is nondeterministic. `Dispose` should
    flush/close gracefully (`Producer_flush` → `Producer_close`) and join the
    pump, *then* release the `SafeHandle`.
  - Guard **use-after-dispose**: the managed producer wrapper throws
    `ObjectDisposedException` once closed (mirrors `ThrowIfHandleClosed`).

**Why:** `SafeHandle` is the robust form of "remember to call `_destroy`" — the
runtime frees on `Dispose` or finalization exactly once, even through
exceptions, and `IsInvalid == zero` matches our null-safe destroy and the ABI's
"null = absent" convention. `confluent-kafka-dotnet` uses precisely this
(`SafeHandleZeroIsInvalid` + `ReleaseHandle → rd_kafka_destroy`, child-first
`Dispose`, `ThrowIfHandleClosed`). But a `SafeHandle` per **per-message** handle
would allocate a finalizable object for every record on the throughput path —
wasteful and contrary to the hot-path spirit — while their lifetime is trivially
one pump cycle, so read-and-free is both cheaper and clearer. The finalizer
caveat is real on both sides: their producer `ReleaseHandle` calls the blocking
`rd_kafka_destroy`, ours calls the blocking `Producer_destroy` — neither belongs
on the finalizer thread, so both rely on explicit `Dispose` doing the graceful
work first.

**How to apply:**

  - `SafeProducerHandle : SafeHandle` — `base(IntPtr.Zero, ownsHandle: true)`,
    `IsInvalid => handle == IntPtr.Zero`, `ReleaseHandle` →
    `Native.kafka_producer_Producer_destroy(handle); return true;`. Wrap the
    `IntPtr` returned by `KafkaProducer_new`/`MockProducer_new`.
  - Build properties in a `using`/`try-finally`; call `ProducerProperties_destroy`
    once `KafkaProducer_new` has returned (it copied what it needed).
  - Pump completion per index: `RecordMetadata_copy` (extract+free) on success or
    read + `KafkaError_destroy` on error; then `FutureRecordMetadata_destroy_all`
    for the batch.
  - `Dispose(bool disposing)`: stop accepting sends → join the pump → fault
    pending `Task`s → `Producer_flush` + `Producer_close` → `producerHandle.Dispose()`
    (triggers `ReleaseHandle`) → `GC.SuppressFinalize`.
  - `ThrowIfDisposed()` at the top of every public method.

**Anti-patterns to flag in review:**

  - Scattering manual `Producer_destroy` calls instead of a `SafeHandle` (leaks
    on exception paths).
  - Wrapping per-message `Future`/`RecordMetadata`/`Error` in a `SafeHandle` (a
    finalizable allocation per record — hot-path waste).
  - Forgetting `FutureRecordMetadata_destroy_all` after `get_all` (one leaked
    handle per send).
  - Double-free: freeing both the `metadata` and `error` slot when only one is
    set, or `_destroy`-ing a handle that `RecordMetadata_copy` already freed.
  - Using any handle after `Dispose` (access violation) — guard with
    `ObjectDisposedException`.
  - Relying on the finalizer to destroy the producer (blocking destroy on the
    finalizer thread; nondeterministic vs. the pump).
  - Destroying the producer while the pump still holds futures derived from it
    (use-after-free) — join first.
  - Freeing the properties handle at the wrong time — before `KafkaProducer_new`
    returns, or never.

**Tests required:**

  - Create and dispose many producers in a loop without leaking or crashing
    (handle-lifecycle smoke).
  - Send N records and verify every `Future`/`RecordMetadata`/`Error` handle is
    freed (debug handle counter, or the mock with no growth) — specifically
    covers `destroy_all` after `get_all`.
  - Double-`Dispose()` is safe (release is idempotent / runs once).
  - Any public method after `Dispose()` throws `ObjectDisposedException`, not a
    crash.
  - `Dispose()` with in-flight sends joins the pump before releasing the producer
    handle — no use-after-free (shared with §1).

---

## 4. String marshalling (UTF-8)

Every string that crosses the boundary is **UTF-8**: the core reads inputs via
`CStr::from_ptr(...).to_string_lossy()` and returns outputs as pointers into
cached UTF-8 `CString`s. netstandard2.0 / net462 lack `UnmanagedType.LPUTF8Str`
and `Marshal.PtrToStringUTF8` (§2), so we hand-roll two helpers and route every
string through them.

### String touchpoints

| Direction | ABI site(s) | C type | Helper |
|---|---|---|---|
| **In** (C# → C) | topic (`Producer_send`, `ProducerRecord_t.topic`), config key/value (`ProducerProperties_put` / `_from_configs`), error message (`MockProducer_error_next`) | `const char *` | `Utf8.Pin` (encode + NUL + pin) |
| **Out** — handle-owned, valid until `_destroy` | `KafkaError_message`, `RecordMetadata_topic` | `const char *` | `Utf8.PtrToString` |
| **Out** — callback-scoped, valid only during the call | `topic` arg of the `RecordMetadata_copy` callback | `const char *` | `Utf8.PtrToString`, **inside** the callback |

**Rule:**

  - **Input:** encode with a `StringAsPinnedUTF8`-style helper —
    `Encoding.UTF8.GetBytes(str)` → copy into `new byte[len + 1]` (the extra
    zero byte is the NUL terminator) → pin via `GCHandle` → pass
    `AddrOfPinnedObject()`; **unpin in a `finally`/`using`** after the call.
    String marshalling necessarily **copies** (an encoding conversion) — that is
    fine for small topic/config strings and is **not** the zero-copy path (§5
    governs key/value bytes).
  - **Output:** read a callee-owned `const char *` with a hand-rolled
    `PtrToStringUTF8(IntPtr)` — scan to the NUL byte, then
    `Encoding.UTF8.GetString(ptr, len)`; return `null` for a null pointer. This
    needs `unsafe`. A `#if NET6_0_OR_GREATER` fast path
    (`MemoryMarshal.CreateReadOnlySpanFromNullTerminated`) is optional — it is
    internal to the helper and does **not** fork the P/Invoke declarations.
  - **Copy before free / before the callback returns.** Output pointers are
    *borrowed*: `KafkaError_message` / `RecordMetadata_topic` are valid only
    until the owning handle is `_destroy`ed, and the `RecordMetadata_copy`
    callback's `topic` is valid only for the duration of the callback. Always
    `PtrToStringUTF8` into a managed `string` first. **Never store or return the
    raw pointer.**
  - **Never** `[MarshalAs(UnmanagedType.LPStr)]` (ANSI — corrupts non-ASCII,
    §2a) or `LPWStr` (UTF-16 — wrong width); **never** `LPUTF8Str` /
    `Marshal.PtrToStringUTF8` (absent on the floor).
  - Topic / config keys must not contain an interior NUL — the core reads to the
    first NUL (`CStr`) and would silently truncate. (Kafka names can't contain
    NUL; validate only if being defensive.)

**Why:** UTF-8 is the contract on both sides, and the core's
`to_string_lossy()` means invalid bytes are silently replaced, **not** rejected
— so an ANSI `LPStr` mistake corrupts topic/key names quietly rather than
erroring (and hides entirely for ASCII-only tests). Hand-rolled helpers are
mandatory because the floor TFMs predate the built-in UTF-8 marshallers — the
same reason `confluent-kafka-dotnet` ships `Util.Marshal.StringAsPinnedUTF8` +
`PtrToStringUTF8` (its net6 branch uses the span fast path; its netstandard2.0
branch walks to the NUL — we mirror that). The copy-before-free rule follows
from the core caching output strings inside the handle: the pointer dies with
the handle.

**How to apply:**

  - Provide the two helpers §2/§3 already reference:
    `Utf8.Pin(string) : IDisposable { IntPtr Ptr; }` (encode + NUL + pin; `Free`
    on `Dispose`) and `Utf8.PtrToString(IntPtr) : string?` (NUL-scan +
    `GetString`).
  - Send path: `using var t = Utf8.Pin(topic);` then pass `t.Ptr`; pin config
    strings the same way when using `_put`.
  - Error path: `Utf8.PtrToString(Native.kafka_common_KafkaError_message(err))`
    **before** `Native.kafka_common_KafkaError_destroy(err)`.
  - `RecordMetadata`: prefer `RecordMetadata_copy`; `Utf8.PtrToString` the
    `topic` argument **inside** the callback (the pointer dies when it returns).

**Anti-patterns to flag in review:**

  - `[MarshalAs(UnmanagedType.LPStr)]` / `LPWStr` for any string; `LPUTF8Str` /
    `Marshal.PtrToStringUTF8` (unavailable on the floor).
  - Marshalling a `const char *` **return** as a C# `string` (the marshaller
    frees callee-owned memory — §2).
  - Storing/returning an output pointer, or reading it after the owning handle is
    destroyed; reading the callback `topic` pointer after the callback returns.
  - Assuming ASCII (works until a non-ASCII topic corrupts silently).
  - Routing key/value bytes through the string/byte marshaller instead of pinning
    (that's §5, zero-copy).

**Tests required:**

  - A non-ASCII (multi-byte UTF-8) value round-trips unchanged through each
    direction: topic (in → `RecordMetadata.Topic` out), a config value, and an
    error message — guards both directions and catches an accidental `LPStr`.
  - `PtrToStringUTF8(IntPtr.Zero)` returns `null`.
  - Empty string, and a string whose multi-byte character sits at the buffer
    boundary, marshal correctly.
  - Reading `RecordMetadata.Topic` after the record is materialized returns the
    correct value (implicitly exercises copy-before-free).

---

## 5. Zero-copy & buffer lifetime (pinning)

CLAUDE.md §12 forbids copying key/value bytes on the send path; that contract
extends into the binding. The user's `byte[]` must reach the core **without an
intermediate managed copy** — pin it and pass its address as `IntPtr` + `int`.

**The load-bearing fact (verified in the core):** the key/value bytes are
serialized and **copied into the batch buffer synchronously during the send
call** — `Producer_send` → `block_on(send())` → `do_send_bytes` →
`RecordAccumulator::append` → `ProducerBatch::try_append` →
`MemoryRecordsBuilder::append`, which writes the bytes directly into the batch's
`Vec<u8>` (`default_record.rs` `copy_from_slice`; memory_records_builder: "Records
already written directly into self.buffer"). `block_on` drives that to
completion **before `Producer_send` returns.** Therefore the pin is
**call-scoped, not Task-scoped**: the core holds no reference to the user's
buffer after the FFI call returns.

**Rule:**

  - Pass key/value as `IntPtr` (address of the user's own `byte[]`) + `int len`.
    Do **not** copy into an intermediate buffer.
  - **Pin only for the duration of the `Producer_send` / `_send_batch` FFI call,
    then unpin.** Do NOT keep the pin alive until the returned `Task`/future
    completes — the core already copied the bytes; holding the pin longer just
    pins GC memory per in-flight message and fragments the heap.
  - Prefer a **`fixed` block** for the single-send path (stack-scoped, no
    allocation; the synchronous call fits exactly inside the `fixed` scope). Use
    `GCHandle.Alloc(arr, GCHandleType.Pinned)` + `AddrOfPinnedObject()` + a
    `finally { Free(); }` where `fixed` does not fit (e.g. the N buffers of a
    `_send_batch`).
  - For `_send_batch`, every record's key **and** value must stay pinned for the
    whole `send_batch` call (each is copied during that call).
  - Encode the ABI sentinels explicitly: **absent** key/value → `IntPtr.Zero`
    + `len = -1`; **empty** → a valid pointer + `len = 0`. Never conflate the
    two.

**Why:** verified above — the borrow of the user's buffer ends when the send
call returns, so a call-scoped pin is exactly sufficient and Task-scoped pinning
is both unnecessary and harmful (per-message pinned objects fragment the GC
heap under load). This mirrors `confluent-kafka-dotnet`, which pins the array
around `produceva` with `MSG_F_COPY` (librdkafka copies during the call) and
`Free()`s in a `finally`. Any intermediate managed→native copy
(`Marshal.AllocHGlobal` + `Marshal.Copy`, `Span.ToArray()`, a copying
marshaller) would add exactly the per-message allocation §12 exists to prevent.

**How to apply:**

  - Single send (cheapest):
    ```csharp
    fixed (byte* k = key)     // null → k == null
    fixed (byte* v = value)
    {
        future = Native.kafka_producer_Producer_send(
            handle, topicPtr, partition, timestamp,
            (IntPtr)k, key   is null ? -1 : key.Length,
            (IntPtr)v, value is null ? -1 : value.Length,
            out err);
    }
    ```
    Translate `null` → (`IntPtr.Zero`, `-1`) explicitly; a `fixed` over `null`
    yields a null pointer, so gate the length on `null`, not on the pointer.
  - Batch send: `GCHandle`-pin each record's key/value, fill the
    `ProducerRecord_t[]`, call `_send_batch`, then `Free()` every pin in a
    `finally`.
  - Unpin (`fixed` scope exit / `GCHandle.Free`) immediately after the call
    returns. Never hold a pin across the returned `Task`.

**Anti-patterns to flag in review:**

  - Any intermediate copy of key/value: `Marshal.AllocHGlobal` + `Marshal.Copy`,
    `arr.ToArray()`, `span.ToArray()`, or a marshaller that copies binary.
  - Keeping the pin alive until the `Task` completes (per-message pinned objects
    → GC heap fragmentation; unnecessary since the core copied during the call).
  - Forgetting to unpin / `Free()` (a permanent pin — worse than a leak).
  - Conflating empty (`len 0`) with absent (`len -1`).
  - Pinning a buffer then mutating/resizing the array (the address may move).
  - Routing key/value through the UTF-8 string helpers (§4) — those copy.

**Tests required:**

  - **Mutation-after-send** (proves the copy happened during the call): send a
    value, then immediately mutate the caller's `byte[]`; the produced record is
    unchanged. This is the external proof that call-scoped pinning is safe.
  - **Allocation budget** (mirrors the core's hot-path allocation test / DoD
    §10): sending a large value adds no value-sized managed allocation beyond the
    user's own buffer — i.e. no intermediate copy.
  - Absent (`len -1`) vs empty (`len 0`) key and value each produce the correct
    record.
  - Batch send with a mix of null / empty / large key & values is correct and
    leaves nothing pinned afterward.

---

## 6. Error model (operational vs. precondition)

Two distinct error categories cross this boundary, handled differently:

  - **Kafka operational errors** — reported by the core through the
    `kafka_common_KafkaError_t *` handle → a single flat `KafkaException`.
  - **Precondition (argument/state) errors** — programmer mistakes (null topic,
    closed producer, bad partition) caught in the binding **before** the FFI call
    → standard .NET argument/state exceptions, **never** `KafkaException`.

### 6a. Kafka operational errors: null-handle → `KafkaException`

The ABI signals these with an opaque `kafka_common_KafkaError_t *`: **null =
success, non-null = error** — via an `out_error` out-param (`KafkaProducer_new`,
`Producer_send`, `flush`, `close`) or an `out_errors[]` array (`send_batch`,
`get_all`). Read the accessors, then free the handle.

| ABI accessor | Type | → C# |
|---|---|---|
| `KafkaError_code` | `int32_t` (Kafka protocol code, i16 widened) | `int Code` |
| `KafkaError_message` | `const char *` (UTF-8, handle-owned) | `Message`, via `Utf8.PtrToString` (§4) |
| `KafkaError_is_retriable` | `bool` | `IsRetriable` property |
| `KafkaError_is_fatal` | `bool` | `IsFatal` property |
| `KafkaError_destroy` | — | free after reading (§3) |

**Model — one flat `KafkaException` (for now).** A single
`KafkaException : Exception` carrying `Code` / `IsRetriable` / `IsFatal` (+
`Message`), mirroring the Python sibling's `KafkaError`. Do **not** build a
per-code typed hierarchy now: the ABI only exposes code + retriable + fatal, and
the reference sibling (Python) is flat, so a flat model keeps the bindings
consistent and avoids a large `Errors`-enum `code → Type` table. This is
deliberately *"for now"* — specific Java-named subclasses (e.g.
`RecordTooLargeException`) can be added later **under** the same `KafkaException`
base, non-breakingly, if a concrete need for catch-by-type appears.

**Rule (operational):**

  - After every fallible call, check the error slot: `IntPtr.Zero` = success;
    non-null = build a `KafkaException` and dispose the handle.
  - `KafkaException.FromHandle(IntPtr err)`: read `_code`, `_message` (via
    `Utf8.PtrToString`, **before** freeing — §4), `_is_retriable`, `_is_fatal`
    into managed fields, then `KafkaError_destroy` in a `finally` so the handle
    is freed **exactly once**, even if construction throws (§3). The exception
    holds **copied values**, never the handle.
  - **Sync failures** (`KafkaProducer_new`, immediate `Producer_send`, `flush`,
    `close`) `throw`. **Async send failures** (the future resolves to an error)
    are read by the pump from `out_errors[i]` and **fault the `Task`** with the
    same exception (§1). Both paths go through the same `FromHandle`.
  - Do **not** fabricate attributes the ABI does not expose. `txn_requires_abort`
    exists in the core but is **not** in the producer ABI surface; adding
    transactions later needs a new accessor first.
  - Do **not** adopt `confluent-kafka-dotnet`'s librdkafka-shaped `Error` /
    `ErrorCode` object — expose `Code` as a plain `int` property.

### 6b. Precondition (argument/state) errors: validate in the binding

**Rule (precondition):**

  - Validate arguments and state in the managed binding **before** any pin /
    marshal / P/Invoke, throwing standard .NET exceptions — **never**
    `KafkaException`:
    - `ArgumentNullException` — null topic / record / config (and a null
      key/value buffer when its length says present).
    - `ArgumentException` / `ArgumentOutOfRangeException` — invalid partition,
      negative timeout, malformed args.
    - `ObjectDisposedException` / `InvalidOperationException` — producer already
      closed / disposed.
  - This is **mandatory**, not a nicety: the C ABI **does not validate
    preconditions** (CLAUDE.md §3 — *"Don't check for failing programming
    preconditions like NULLs on required parameters"*), and some ABI functions
    **`assert!`/panic on violation** (e.g. `send_batch`) — a Rust panic unwinding
    across the FFI boundary is **undefined behavior**. The managed binding is the
    safety net that stops bad input before it reaches native code.

**Why (both):** a flat `KafkaException` matches exactly what the ABI exposes
(code + retriable + fatal) and the Python sibling, so the bindings stay
consistent without a per-code table — and it can grow into typed subclasses
later. Preconditions are a *separate* category because they are programmer
errors, not Kafka outcomes: Java raises `IllegalArgumentException` /
`IllegalStateException` / NPE (never `KafkaException`), Python raises
`ValueError` / `TypeError` / `RuntimeError`, and `confluent-kafka-dotnet` raises
`ArgumentException` / `InvalidOperationException` — all in the high-level layer,
before the native call. Ours must too, and *must* because the ABI would
otherwise panic (UB). The read-before-free / destroy-exactly-once discipline is
the §3/§4 ownership contract applied to the error handle.

**How to apply:**

  - `KafkaException : Exception { public int Code; public bool IsRetriable;
    public bool IsFatal; }`; `static KafkaException FromHandle(IntPtr err)` reads
    the accessors (message **before** free) and frees in a `finally`.
  - Operational — sync: `if (err != IntPtr.Zero) throw KafkaException.FromHandle(err);`;
    async: the pump does `tcs.SetException(KafkaException.FromHandle(errs[i]));`.
  - Precondition — guard at the top of every public method **before** touching
    the ABI: `ThrowIfDisposed();`, then null/range checks. (On the netstandard2.0
    floor `ArgumentNullException.ThrowIfNull` may be unavailable — use an explicit
    `if (x is null) throw new ArgumentNullException(nameof(x));`.)

**Anti-patterns to flag in review:**

  - *(operational)* Not checking the error slot; leaking the handle or destroying
    it **before** reading `_message` (use-after-free — §4); `FromHandle` that can
    throw before `KafkaError_destroy` (leak — wrap in `try/finally`).
  - *(operational)* A `confluent-kafka-dotnet`-style `Error` + `ErrorCode`
    object, or fabricating attributes (`txn_requires_abort`) the ABI doesn't
    expose.
  - *(operational)* Building a per-code `KafkaException` hierarchy now — we chose
    flat "for now"; that's over-engineering vs. the Python sibling.
  - *(precondition)* Relying on the ABI to reject null / bad args (it may
    **panic → UB**); throwing `KafkaException` for a programmer error; validating
    **after** the P/Invoke instead of before.

**Tests required:**

  - *(operational)* A sync failure (invalid config to `KafkaProducer_new`, or a
    record-too-large `send`) throws `KafkaException` with the correct `Code`,
    `Message`, `IsRetriable`, `IsFatal`.
  - *(operational)* An async send failure faults the `Task` with a matching
    `KafkaException` (drive via the mock `error_next`).
  - *(operational)* The error handle is destroyed exactly once even on the throw
    path (no leak); a non-ASCII message round-trips (ties to §4).
  - *(precondition)* Null topic / record → `ArgumentNullException`; use after
    `Dispose` → `ObjectDisposedException`; bad partition → `ArgumentException` /
    `ArgumentOutOfRangeException` — each thrown **before** any native call (fails
    even with no live producer handle).
