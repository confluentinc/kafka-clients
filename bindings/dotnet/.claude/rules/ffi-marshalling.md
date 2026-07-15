# .NET binding — FFI marshalling & ownership

Deep-dive rules for marshalling across the C ABI (`confluent_kafka.h`,
`kafka_producer_*` / `kafka_common_KafkaError_*`) from .NET via P/Invoke.
Supplements `bindings/dotnet/CLAUDE.md`. Review ground truth is the **C ABI
header** and the **Kafka Java public API**.

Each section leads with **Decision** (the intent, one line), then **Rule**,
**Why**, **Anti-patterns**, and **Tests required** (`consumer-threading.md` precedent).

## Thread topology & thread-safety (shared context)

The whole-system picture every section assumes. The C ABI hides all Kafka I/O on
native threads **owned by the Rust core**; the binding adds only the caller
threads and a single completion pump.

```
   .NET (managed)                    │ C ABI │       Rust core (native, per producer)
   ─────────────                     │       │       ─────────────────────────────────
   caller thread(s):                 │       │   tokio multi-thread runtime (worker POOL)
     SendAsync → Producer_send ──────│──────►│     RecordAccumulator (enqueue, returns fast)
   completion pump (1 bg thread):    │       │   Sender task (spawned once in _new):
     get_all(futures) ─block_on─────►│──────►│     NetworkClient + ONE async Selector
     ◄── per-message metadata/err ───│◄──────│       ↕ multiplexes ALL brokers (event-driven)
   Dispose: join pump → flush/close  │       │   (created in KafkaProducer_new, dropped on _destroy)
```

**Rule:**

  - Native side (per real `KafkaProducer`): a multi-thread tokio runtime + one
    spawned Sender task doing all I/O over a **single async Selector** (all
    brokers multiplexed — NOT thread-per-broker), created in `KafkaProducer_new`,
    torn down by `Producer_destroy`.
  - .NET side: caller thread(s) + **exactly one** completion pump (§7); no
    per-send threads. **No poll loop** — the core self-drives, so a stalled pump
    delays result delivery but not sending.
  - FFI is callable from any .NET thread; the core serializes via the producer's
    internal `Mutex`, so concurrent `SendAsync` is safe — don't add your own lock.
  - `block_on` parks only the *calling* .NET thread; the Sender keeps running on
    the runtime's worker threads, so a blocked pump can't deadlock it.
  - The one callback (`RecordMetadata_copy`) fires synchronously on the caller's
    (pump) thread — not a foreign thread (§6).

**Why:** Java's single-Selector NIO model on tokio (CLAUDE.md §8) — unlike
librdkafka (a thread per broker + a mandatory `rd_kafka_poll` loop, which
confluent-kafka-dotnet services with a `LongRunning` `callbackTask`). So our
native thread count is independent of cluster size, there is no poll loop, and
the multi-thread runtime is what makes `block_on` from .NET deadlock-free.

**Anti-patterns:**

  - A `callbackTask`-style poll-loop thread — nothing to poll here.
  - Assuming thread-per-broker, or native threads scaling with cluster size.
  - A binding-side lock around FFI calls (double-locking; the core serializes).

**Tests required:**

  - Concurrent `SendAsync` from many .NET threads is correct (the `Mutex` holds).
  - A long-blocked pump does not stop new sends being enqueued.
  - `Dispose` joins the pump before destroying the handle (shared with §7).

---

## 1. P/Invoke declarations & type mapping

**Decision:** One `internal static class Native` of classic `[DllImport(...,
Cdecl)]` declarations, uniform across `netstandard2.0` + `net8.0` + `net10.0`.
netstandard2.0 is the floor (covers .NET Framework 4.6.2), so the modern interop
APIs — `[LibraryImport]`, `delegate* unmanaged`, `[UnmanagedCallersOnly]`,
`UnmanagedType.LPUTF8Str`, `Marshal.PtrToStringUTF8` — are **off-limits** (they
don't exist on the floor). Same classic toolkit as confluent-kafka-dotnet,
pointed at our fixed-width, handle-error ABI.

### C ABI → C# type map

| C ABI type | C# | Notes |
|---|---|---|
| `int32_t` | `int` | **Never `UIntPtr`/`nint`** — our ABI has no `size_t`. |
| `int64_t` | `long` | |
| `bool` | `[MarshalAs(UnmanagedType.I1)] bool` | C `bool` is 1 byte; default marshals a 4-byte Win32 `BOOL`. |
| opaque `*_t *` | `IntPtr` → `SafeHandle` above (§2) | Never a C# struct mirroring `_private[0]`. |
| `const char *` in | `IntPtr` to a pinned NUL-terminated UTF-8 buffer (§3) | No `LPStr` (ANSI), no `LPUTF8Str`. |
| `const char *` out (callee-owned) | `IntPtr` → `Utf8.PtrToString` (§3) | Never a `string` return — the marshaller would free it. |
| `const uint8_t *` + `int32_t len` | `IntPtr` (pinned `byte[]`) + `int` (§4) | Zero-copy, call-scoped pin. |
| `T **` out-param / array | `out IntPtr` / `IntPtr[]` | e.g. `out_error`; `get_all` arrays. |
| `ProducerRecord_t` | `[StructLayout(LayoutKind.Sequential)] struct`, array `[]` | Fixed-width ⇒ identical layout. |
| `void (*cb)(...)` / `void *` | Cdecl delegate, kept alive (§6) / `IntPtr` (`GCHandle`) | |

**Rule:**

  - `[DllImport("confluent_kafka", CallingConvention = CallingConvention.Cdecl)]`
    (Cdecl matches cbindgen `extern "C"`; the bare name maps to
    `confluent_kafka.dll` / `lib….so` / `lib….dylib`). One declaration set for all
    TFMs — no per-TFM `#if`.
  - Follow the type map verbatim: sizes are `int`/`long`, `bool` is `I1`, opaque
    handles stay `IntPtr` here (wrapped in a `SafeHandle` one layer up, §2), UTF-8
    strings and key/value bytes are marshalled by hand (§3, §4).
  - Prefer `ProducerProperties_put` over `_from_configs` (avoids marshalling a
    `const char *const *`).

**Why:** the floor predates the modern interop surface, so classic `DllImport` is
the only portable option — the same reason confluent-kafka-dotnet (also net462)
hand-rolls its UTF-8 helpers. We borrow their *mechanism*, not their *signatures*:
our sizes are `int`/`long` (not librdkafka's `size_t`→`UIntPtr`) and errors are
opaque handle out-params (not an `ErrorCode` enum + `errstr` buffer). A
`[LibraryImport]` `#if` specialization for AOT on modern TFMs is a deliberate
non-goal — our signatures are already mostly blittable, not worth two sets.

**Anti-patterns:**

  - `[LibraryImport]` / `delegate* unmanaged` / `[UnmanagedCallersOnly]` /
    `LPUTF8Str` / `Marshal.PtrToStringUTF8` — unavailable on the floor.
  - `[MarshalAs(LPStr)]` for UTF-8 (ANSI corruption); `UIntPtr`/`nint` for a
    length; a callee-owned `const char*` return marshalled as `string`.
  - Omitting `Cdecl` (works on x64 by luck, breaks on x86).
  - Porting confluent-kafka-dotnet's reflection loader / 3× NativeMethods (§8).

**Tests required:**

  - A non-ASCII topic round-trips (guards manual UTF-8; catches `LPStr`).
  - Each `bool`-returning fn (`is_done`/`is_retriable`/`is_fatal`) is correct
    (guards a missing `I1`).
  - `Native` loads on net462, net8.0, net10.0 (TFM smoke test).

---

## 2. Handle ownership & lifecycle (`SafeHandle`)

**Decision:** Two tiers. **Long-lived** handles (producer, properties) → a
`SafeHandle` that frees exactly once. **Transient** per-message handles (future /
metadata / error) → read-and-free promptly on the pump; do **not** wrap them (a
finalizable object per record is hot-path waste).

| Handle | Created by | Freed by (not consumed elsewhere) |
|---|---|---|
| `Producer_t` | `KafkaProducer_new` / `MockProducer_new` | `SafeProducerHandle.ReleaseHandle → Producer_destroy` (via `Dispose`) |
| `ProducerProperties_t` | `ProducerProperties_new` / `_from_configs` | the binding, after `KafkaProducer_new` (not consumed by it) |
| `FutureRecordMetadata_t` | `Producer_send` / `_send_batch` | the pump: `_destroy_all` (not consumed by `get_all`) |
| `RecordMetadata_t` | `_get` / `_get_all` | the pump: `RecordMetadata_copy` (extract+free) or `_destroy` |
| `KafkaError_t` | any `out_error` slot | the reader: read accessors, then `_destroy` |

**Rule:**

  - `SafeProducerHandle : SafeHandle` — `ownsHandle: true`, `IsInvalid => handle
    == IntPtr.Zero`, `ReleaseHandle` calls `Producer_destroy`. Runtime frees
    exactly once, even on exceptions.
  - Transient handles: free in a `finally` on the pump; the managed
    `RecordMetadata`/`KafkaException` hold **copied values**, never the handle
    (§7). After `get_all`, each index has exactly one non-null of {metadata,
    error} — free that one, plus the future via `_destroy_all`. All `_destroy`
    are null-safe; `RecordMetadata_copy` frees its own handle (don't double-free).
  - **Parent outlives children:** the producer must not be destroyed while the
    pump holds futures from it — enforce via `Dispose` ordering (stop sends → join
    pump → release handle, §7), not `DangerousAddRef`.
  - **Prefer `Dispose` over the finalizer:** `Producer_destroy` blocks (drops the
    runtime, waiting for the Sender), which is wrong on the finalizer thread.
    `Dispose` flushes/closes and joins the pump first; guard use-after-dispose
    with `ObjectDisposedException`.

**Why:** `SafeHandle` is the robust form of "call `_destroy` exactly once," even
through exceptions; `IsInvalid == zero` matches our null-safe destroy. This is
confluent-kafka-dotnet's `SafeHandleZeroIsInvalid` pattern. But a per-message
`SafeHandle` allocates a finalizable object per record, so transient handles are
read-and-freed instead (their lifetime is one pump cycle). The blocking
`Producer_destroy` is why the producer closes via `Dispose`, not the finalizer
(their blocking `rd_kafka_destroy` has the same constraint).

**Anti-patterns:**

  - Manual scattered `Producer_destroy` instead of a `SafeHandle` (leaks on
    exceptions); wrapping per-message handles in a `SafeHandle`.
  - Forgetting `_destroy_all` after `get_all` (leak per send); double-free (both
    slots, or a handle `RecordMetadata_copy` already freed).
  - Relying on the finalizer for the producer; destroying it before joining the
    pump (use-after-free).

**Tests required:**

  - Create/dispose many producers — no leak/crash; send N and assert the handle
    count returns to baseline (covers `_destroy_all`).
  - Double-`Dispose` is safe; a call after `Dispose` throws
    `ObjectDisposedException`.

---

## 3. String marshalling (UTF-8)

**Decision:** Every boundary string is UTF-8, marshalled by hand (the floor lacks
`LPUTF8Str` / `Marshal.PtrToStringUTF8`, §1): input via `Utf8.Pin` (encode + NUL +
pin), callee-owned output via `Utf8.PtrToString` (NUL-scan + `GetString`).

| Direction | Sites | Helper |
|---|---|---|
| In | topic, config key/value, `error_next` message | `Utf8.Pin` |
| Out — valid until `_destroy` | `KafkaError_message`, `RecordMetadata_topic` | `Utf8.PtrToString` |
| Out — valid only during the callback | `RecordMetadata_copy` `topic` | `Utf8.PtrToString`, inside the callback |

**Rule:**

  - Input: `Encoding.UTF8.GetBytes` → `new byte[len+1]` (trailing zero = NUL) →
    pin → pass `AddrOfPinnedObject`; unpin in `finally`. Marshalling copies (an
    encoding conversion) — fine for small topic/config, and **not** the zero-copy
    path (§4).
  - Output: read a callee-owned `const char*` with `Utf8.PtrToString` (needs
    `unsafe`; a `#if NET6_0_OR_GREATER` span fast path is an internal
    optimization). **Copy before free / before the callback returns** — the
    pointer dies with the handle; never store the raw pointer.
  - Never `[MarshalAs(LPStr)]` (ANSI) or `LPWStr` (UTF-16); never `LPUTF8Str` /
    `Marshal.PtrToStringUTF8` (absent on the floor).

**Why:** UTF-8 is the contract both ways, and the core reads input with
`to_string_lossy` — invalid bytes are silently *replaced*, not rejected, so an
`LPStr` mistake corrupts non-ASCII topics quietly (and hides in ASCII-only
tests). Hand-rolled helpers are the same reason confluent-kafka-dotnet ships
`StringAsPinnedUTF8` + `PtrToStringUTF8`. Copy-before-free follows from output
strings living in the handle's cached `CString`.

**Anti-patterns:**

  - `LPStr` / `LPWStr` for any string; a `const char*` return marshalled as
    `string`.
  - Reading an output pointer after its handle (or the callback) is gone.
  - Assuming ASCII (works until a non-ASCII topic corrupts silently).

**Tests required:**

  - A non-ASCII value round-trips through topic (in → out), a config value, and an
    error message.
  - `PtrToStringUTF8(IntPtr.Zero)` → `null`; a multi-byte char at the buffer
    boundary marshals correctly.

---

## 4. Zero-copy & buffer lifetime (pinning)

**Decision:** Pass key/value as `IntPtr` (address of the user's `byte[]`) + `int
len` with **no intermediate copy**, pinned **only for the send call** — the pin
is call-scoped, not Task-scoped.

**Verified fact:** the core copies key/value into the batch buffer *synchronously
during the send call* — `Producer_send → block_on(send()) → …
RecordAccumulator::append → MemoryRecordsBuilder::append` writes the bytes into
the batch's `Vec<u8>`, and `block_on` finishes before `Producer_send` returns. So
the core holds no reference to the user buffer afterward (CLAUDE.md §12).

**Rule:**

  - Prefer a `fixed` block (stack-scoped, no allocation) for a single send; use
    `GCHandle.Alloc(Pinned)` + `finally Free()` where `fixed` doesn't fit (the N
    buffers of `_send_batch`, all pinned for the whole call). Unpin right after
    the call — never hold a pin across the returned `Task` (per-message pinned
    objects fragment the GC heap).
  - Sentinels: absent → `IntPtr.Zero` + `len -1`; empty → valid pointer + `len 0`.
    A `fixed` over `null` yields a null pointer, so gate length on `null`:
    ```csharp
    fixed (byte* k = key)   // key null → k == null
        future = Native.kafka_producer_Producer_send(handle, topicPtr, partition,
            timestamp, (IntPtr)k, key is null ? -1 : key.Length, /* value… */ out err);
    ```
  - This call-scoped rule depends on §7's inline-send decision; a deferred-send
    design (a background send thread, as in the Python binding) would have to hold
    the buffer until the deferred send runs.

**Why:** the borrow ends when the send call returns, so a call-scoped pin is
exactly sufficient; Task-scoped pinning is unnecessary and fragments the heap.
Mirrors confluent-kafka-dotnet (pin around `produceva` with `MSG_F_COPY`, `Free`
in `finally`). Any intermediate copy (`AllocHGlobal`+`Copy`, `ToArray()`) is the
per-message allocation CLAUDE.md §12 exists to prevent.

**Anti-patterns:**

  - Any intermediate copy of key/value; keeping the pin alive until the `Task`
    completes; forgetting to unpin (a permanent pin).
  - Conflating empty (`0`) with absent (`-1`); mutating/resizing a pinned array.

**Tests required:**

  - **Mutation-after-send**: mutate the caller's `byte[]` right after `send`; the
    produced record is unchanged (proves the copy happened during the call).
  - **Allocation budget** (DoD §10): a large value adds no value-sized managed
    allocation.
  - Absent vs empty key/value each produce the correct record.

---

## 5. Error model (operational vs. precondition)

**Decision:** Two surfaces. **Core (operational) errors** cross as a
`kafka_common_KafkaError_t` handle → one flat `KafkaException` (code + retriable +
fatal + message), mirroring the Python sibling. **Precondition errors** (bad
argument / state) are validated in the binding *before* the FFI call and raise
standard .NET exceptions — never `KafkaException`.

| `KafkaError_*` accessor | → C# |
|---|---|
| `_code` (i16 widened) | `int Code` |
| `_message` (UTF-8, handle-owned) | `Message` via `Utf8.PtrToString` (§3) |
| `_is_retriable` / `_is_fatal` | `IsRetriable` / `IsFatal` |
| `_destroy` | free after reading |

**Rule:**

  - **Operational:** null out-param = success, non-null = error.
    `KafkaException.FromHandle` reads the accessors (message **before** free),
    then `_destroy` in a `finally` — freed exactly once even if construction
    throws (§2, §3); the exception holds copied values, not the handle. Sync
    failures `throw`; async send failures fault the `Task` (§7) — same
    `FromHandle`. Keep it **one flat `KafkaException` for now** (the ABI exposes
    only code/retriable/fatal); typed subclasses can be added under it later,
    non-breakingly.
  - **Precondition:** validate before any pin/marshal/P/Invoke and throw
    `ArgumentNullException` (null topic/record/config),
    `ArgumentOutOfRangeException` (a **negative partition** — the ABI silently
    maps negative to "unset", so the binding must reject it — or a negative
    timeout), or `ObjectDisposedException` / `InvalidOperationException` (closed
    producer). **Mandatory**, not optional: the ABI doesn't validate preconditions
    (CLAUDE.md §3) and some functions `assert!`/panic on violation — a panic
    across FFI is UB.

**Why:** a flat `KafkaException` matches what the ABI exposes and the Python
sibling. Preconditions are a separate surface because they are programmer errors,
not Kafka outcomes — Java raises `IllegalArgument`/`IllegalState`, Python
`ValueError`/`TypeError`, confluent-kafka-dotnet `ArgumentException`, all before
the native call. Ours must too, *and must* because the ABI would otherwise panic.

**Anti-patterns:**

  - Not checking the out-param; leaking the error handle or reading `_message`
    after `_destroy`; a `FromHandle` that can throw before its `_destroy`.
  - A confluent-kafka-dotnet-style `Error`/`ErrorCode` object; a per-code
    hierarchy now (over-engineering vs the Python sibling).
  - Relying on the ABI to reject bad args (→ panic/UB); throwing `KafkaException`
    for a programmer error; validating after the P/Invoke.

**Tests required:**

  - A sync failure throws `KafkaException` with the right code/message/flags; an
    async failure faults the `Task` (via mock `error_next`); the handle is freed
    exactly once; a non-ASCII message round-trips.
  - Null topic/record → `ArgumentNullException`; post-`Dispose` →
    `ObjectDisposedException`; negative partition → `ArgumentOutOfRangeException` —
    each before any native call.

---

## 6. Callback & delegate marshalling

**Decision:** The ABI's one callback (`RecordMetadata_copy`) is synchronous;
marshal it as a kept-alive `[UnmanagedFunctionPointer(Cdecl)]` delegate (classic —
no function pointers on the floor), keep the body no-throw, and pass context via a
`GCHandle` in `user_data`. Using it is optional — the per-field accessors
(`_offset`/`_partition`/`_topic`/`_timestamp` + `_destroy`) avoid callbacks
entirely.

The callback fires **synchronously on the caller's (pump) thread** and returns
before `RecordMetadata_copy` does; its `topic` pointer is valid **only during the
call** (the core frees the handle right after — §3):

    void cb(int64_t offset, int32_t partition, const char* topic, int64_t timestamp, void* user_data)

**Rule:**

  - Named delegate type, `[UnmanagedFunctionPointer(CallingConvention.Cdecl)]`,
    blittable params (`long`/`int`/`IntPtr`/`IntPtr`). Take `topic` as `IntPtr`
    and `Utf8.PtrToString` it *inside* the callback (§3) — never `string`. Hold
    the delegate in a `static readonly` field so the GC can't collect it while
    native holds the thunk.
  - The body is a **no-throw boundary**: `try/catch` it all (a managed exception
    unwinding into Rust is UB); stash any failure and re-surface it after the call
    returns. (Python does this via `PyErr_Print`.)
  - Pass a `GCHandle` over the target as `user_data` (`ToIntPtr` → recover with
    `FromIntPtr` → `Free()` exactly once after the call). Shape:
    `RecordMetadata_copy(md, s_copyCb, GCHandle.ToIntPtr(gch))`.

**Why:** the floor (netstandard2.0/net462) has no function pointers or
`[UnmanagedCallersOnly]`, so a kept-alive Cdecl delegate is the only portable
mechanism — the pattern confluent-kafka-dotnet uses. The GC sees only managed
refs, not the native thunk (→ keep-alive); the CLR can't propagate an exception
through a Rust frame (→ no-throw); `user_data` is C's only per-call context
channel (→ `GCHandle`).

**Anti-patterns:**

  - A delegate with no rooted reference (or a bare inline lambda) — collectible
    mid-call → crash.
  - An exception escaping the callback into native (UB).
  - `topic` typed as `string`, or its pointer read/stored after the callback
    returns (handle already freed — §3).
  - Leaking or double-freeing the `user_data` `GCHandle`.

**Tests required:**

  - Correct offset/partition/topic/timestamp delivered; a non-ASCII topic read
    inside the callback is correct (ties §3).
  - An exception thrown in the callback is caught, doesn't crash or unwind into
    native, and is re-surfaced.
  - Aggressive GC while the callback is in use doesn't crash (keep-alive; shared
    §1).

---

## 7. Async / Future completion — pull-based pump + `TaskCompletionSource`

**Decision:** Java `Future<RecordMetadata>` → .NET `Task<RecordMetadata>`,
completed by **one** background pump that does the blocking waits. `SendAsync`
enqueues and returns instantly with a `TaskCompletionSource`-backed `Task`; the
pump blocks on the batched `get_all` and completes each TCS. (The ABI is
pull-only: `get`/`get_all` block, `is_done` polls — no push callback. Mirrors the
Python binding's `poll_futures_thread`, python-ffi.md §6.)

```
Caller thread                         Completion pump (one bg thread)
─────────────                         ───────────────────────────────
SendAsync():                          loop:
  pin key/value (call-scoped, §4)       drain a batch of (future, tcs)
  Producer_send() → future handle       get_all(futures[])   ← BLOCKS
  new TaskCompletionSource (tcs)        tcs[i].SetResult / SetException
  enqueue (future, tcs); return Task    destroy_all(futures)
Dispose(): signal + join the pump ◄──── on shutdown: drain, fault pending, exit
```

**Rule:**

  - `SendAsync` never calls a blocking `_get`/`_get_all` on the caller's thread —
    it pins (§4), calls `Producer_send` (inline; a fast enqueue), checks the sync
    `out_error`, enqueues `(future, tcs)`, returns `tcs.Task`. Inline send is fine
    because .NET has no GIL (a send-batching thread is an optional throughput
    tweak, not required).
  - **Exactly one** pump thread does all waits via batched `get_all` — O(1)
    threads for unbounded in-flight sends. Per result: read fields + free handles
    on the pump (§2), `SetResult`/`SetException`, `destroy_all` the futures.
  - Build the TCS with `RunContinuationsAsynchronously` — otherwise a slow awaiter
    continuation runs on the pump thread and stalls every other completion.
  - Completion is exactly-once (guard cancelled/done, free handles on every path).
    Cancellation is best-effort: it discards the result, it does **not** abort an
    in-flight send.
  - `Dispose`: stop sends → drain/fault pending → **join the pump** →
    `flush`/`close` → release the producer `SafeHandle`. Optional fast path: if
    `is_done` at send time, complete synchronously (a `ValueTask`, no queue).

**Why:** the core's own runtime already drives the work, so the binding only
bridges each resolved future to a `Task`; a pull ABI forces the wait onto some
thread, and one pump gives bounded threads. The alternative — `Task.Run(get)` per
send — parks a thread-pool thread per in-flight message and starves the pool
(sync-over-async). Matches CLAUDE.md §11 ("shared completion task, not per-message
spawn") and the Python design.

**Anti-patterns:**

  - `Task.Run(get)` per send / any one-thread-per-message pattern; a `SendAsync`
    that blocks on `_get`/`_get_all` directly.
  - A TCS without `RunContinuationsAsynchronously`; running user code on the pump.
  - Destroying the producer before joining the pump; assuming cancel aborts the
    send; holding a managed lock across `get_all`.

**Tests required:**

  - `Task` resolves with `RecordMetadata` / faults with `KafkaException`.
  - `Dispose` with sends in flight returns (doesn't hang) — the pump-join
    regression.
  - Cancel is safe + frees handles; high-concurrency produce doesn't starve the
    thread pool.

---

## 8. Native library loading, packaging & AOT

**Decision:** The native lib is our own Rust cdylib `confluent_kafka` (from `cargo
build --features ffi`). For now, an MSBuild step copies it into the project's
output dir and default `[DllImport]` probing resolves it — **no NuGet needed**.
One `Native` class, one `DllName`, no hand-rolled loader.

**Rule:**

  - **Packaging (now):** MSBuild copies `target/<cfg>/…confluent_kafka.…` to
    `$(OutDir)` (`CopyToOutputDirectory=PreserveNewest`); default probing (app base
    dir) finds it. The bare `[DllImport("confluent_kafka")]` maps to the filenames
    Cargo emits — never hardcode a filename/absolute path. A separate redist NuGet
    (`runtimes/{rid}/native/`) is deferred and **packaging-only** — it won't touch
    the P/Invoke layer.
  - **Loading:** rely on default `[DllImport]` resolution — do **not** port
    confluent-kafka-dotnet's `Librdkafka.Initialize` (manual `dlopen`/`LoadLibraryEx`
    preload, reflection binding, distro/GSSAPI variant selection); none applies to
    one self-built cdylib. If custom probing is ever needed, use
    `NativeLibrary.SetDllImportResolver` (modern), not a reflection loader; on
    net462 keep the native in the app dir (a `LoadLibraryEx` preload is a last
    resort).
  - **Single `Native` class**, one `DllName = "confluent_kafka"` — the equivalent
    of only their default `NativeMethods`. No `_Alpine`/`_Centos8` /
    `/etc/os-release` detection (our own build can emit a musl artifact under the
    right RID).
  - **AOT:** not committed to, but kept open — direct `[DllImport]` (not a
    reflection loader) is AOT-amenable, unlike theirs. Don't add reflection-based
    loading.

**Why:** we build and control one native, so the drivers of their loader
(third-party binary placement on net462, musl/glibc + GSSAPI variants) don't exist
for us; Cargo's output names already match default P/Invoke resolution. Deferring
the NuGet is safe because packaging never touches the P/Invoke surface.

**Anti-patterns:**

  - Porting `Librdkafka.Initialize` / reflection binding / 3× NativeMethods /
    `/etc/os-release` — unnecessary, and it kills AOT.
  - Hardcoding an absolute path or platform filename in `[DllImport]`.
  - Assuming net462 auto-copies `runtimes/{rid}/native/` (it doesn't).
  - Blocking delivery on the redist NuGet (copy-to-output unblocks dev/test now).

**Tests required:**

  - A smoke call (`MockProducer` create → send → close) loads and works on net462,
    net8.0, net10.0 on Windows + Linux in CI, with the native copied to output.
  - A missing native gives a clear `DllNotFoundException`, not an obscure crash.
