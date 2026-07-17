# .NET binding — FFI marshalling & ownership

Deep-dive rules for marshalling across the C ABI (`confluent_kafka.h`,
`kafka_producer_*` / `kafka_common_KafkaError_*`) from .NET via P/Invoke.
Supplements `bindings/dotnet/CLAUDE.md`. Review ground truth is the **C ABI
header** and the **Kafka Java public API**.

Each section leads with **Decision** (the intent, one line), then **Rule**,
**Why**, **Anti-patterns**, and **Tests required** (`consumer-threading.md` precedent).

## Thread topology & thread-safety (shared context)

The whole-system picture every section assumes. The C ABI hides all Kafka I/O on
native threads **owned by the Rust core**. Thread layout differs by client: the
**producer** adds a single **.NET** completion pump; the **consumer** adds **no**
.NET thread — the core pushes completions to it from a **native** dispatcher
thread. So the .NET consumer side is *leaner*, but completions arrive on a
**foreign** thread.

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

And the **consumer** — no .NET pump; a native dispatcher fires completions:

```
   .NET (managed)                    │ C ABI │       Rust core (native, per consumer)
   ─────────────                     │       │       ─────────────────────────────────
   caller thread(s):                 │       │   tokio multi-thread runtime (worker POOL)
     PollAsync → *_async(…, cb) ─────│──────►│     consumer bg task (ConsumerNetworkThread):
       returns Task; guard acquired  │       │       NetworkClient + ONE async Selector
   (NO .NET pump)                    │       │       ↕ multiplexes ALL brokers (event-driven)
     ◄── cb fires here (→ TCS) ──────│◄──────│     callback-dispatcher thread (1, native):
         on the dispatcher thread    │       │       fires completion callbacks
   Dispose: wakeup+await → close     │       │   (created in KafkaConsumer_new, dropped on _destroy)
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
  - **Consumer** (per real `KafkaConsumer`): the core runs the consumer's own
    **background task** (single `ConsumerNetworkThread`, consumer-threading §10)
    over ONE async Selector, **plus a dedicated callback-dispatcher thread** — both
    native, created in `KafkaConsumer_new`, torn down by `Consumer_destroy`.
  - .NET side (consumer): caller thread(s) **only — no pump** (the ABI *pushes*
    completions, §7). Asymmetry: the producer's pump is a **.NET** thread; the
    consumer's dispatcher is a **native** (core) thread.
  - **One operation in flight** per consumer — the access guard serializes ops
    (concurrent → rejection, §5), released just before the callback fires; the
    completion callback runs on the **dispatcher thread (foreign)**, not the caller
    → `RunContinuationsAsynchronously` + no-throw (§6/§7). `Consumer_wakeup`
    bypasses the guard (§5/§11). Java's single-Selector NIO model on tokio (CLAUDE.md §8) — unlike
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
  - Set **`EntryPoint`** to the full ABI symbol (`kafka_<pkg>_<Type>_<method>`)
    whenever the C# method uses the short name (dropping the `kafka_<pkg>_` prefix
    per CLAUDE.md §5.3) — otherwise the marshaller probes the C# name and throws
    `EntryPointNotFoundException` at **runtime**, not compile time.
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
  - A short C# method name without `EntryPoint` (runtime `EntryPointNotFoundException`).
  - Porting confluent-kafka-dotnet's reflection loader / 3× NativeMethods (§8).

**Tests required:**

  - A non-ASCII topic round-trips (guards manual UTF-8; catches `LPStr`).
  - Each `bool`-returning fn (`is_done`/`is_retriable`/`is_fatal`) is correct
    (guards a missing `I1`).
  - `Native` loads on net462, net8.0, net10.0 (TFM smoke test).

---

## 2. Handle ownership & lifecycle (`SafeHandle`)

**Decision:** Four ownership categories — producer uses (1)+(2); the consumer
adds (3)+(4):

1. **Client / config** (producer, consumer, properties) → a `SafeHandle` that
   frees exactly once (the client's blocking destroy joins its background task
   first).
2. **Flat transient** (future / metadata / error) → read-and-free promptly,
   **not** wrapped (a finalizable object per op is hot-path waste).
3. **Owned result / container** (the consumer poll batch + every query-result
   list/map) → the caller frees once after reading; a container is a
   *borrow-root* whose `_destroy` invalidates the elements/bytes borrowed from it.
4. **Borrowed view** (`ConsumerRecord`, `Node`, every `_get` element) → **never
   freed** by the binding.

| Handle | Category | Created by | Freed by |
|---|---|---|---|
| `Producer_t` | 1 — client (`SafeHandle`) | `KafkaProducer_new` / `MockProducer_new` | `ReleaseHandle → Producer_destroy` (via `Dispose`) |
| `ProducerProperties_t` | 1 — config (`SafeHandle`, short) | `ProducerProperties_new` / `_from_configs` | the binding, after `KafkaProducer_new` |
| `FutureRecordMetadata_t` | 2 — flat transient | `Producer_send` / `_send_batch` | the pump: `_destroy_all` (`get_all` doesn't consume) |
| `RecordMetadata_t` | 2 — flat transient | `_get` / `_get_all` | the pump: `RecordMetadata_copy` (extract+free) or `_destroy` |
| `KafkaError_t` | 2 — flat transient | any `out_error` slot | the reader: read accessors, then `_destroy` |

**Consumer handles** (category per the cross-cutting rule below):

| Handle | Category | Freed by |
|---|---|---|
| `Consumer_t` | 1 — client (`SafeHandle`) | `Consumer_destroy` via `Dispose` (blocking; joins the bg task first, §7) |
| `ConsumerProperties_t` | 1 — config (`SafeHandle`, short) | the binding, after `KafkaConsumer_new` |
| `KafkaError_t` (any `out_error`) | 2 — flat transient | reader: read accessors, then `_destroy` |
| `ConsumerRecords_t` (poll batch) | 3 — owned **borrow-root** | owns the fetched bytes; **copy-out default** (§5.4), keep-alive deferred |
| `TopicPartitionList_t`, `OffsetMap_t`/`LongOffsetMap_t`/`OffsetAndTimestampMap_t`/`TopicPartitionInfoMap_t`, `PartitionInfoList_t`, `StringList_t`, `ConsumerGroupMetadata_t`, standalone value types, owned `char*` | 3 — owned result | caller: read/marshal into managed types, then `_destroy` |
| `ConsumerRecord_t`, `Node_t`, every `_get` `const *` element, borrowed `const char*` | 4 — borrowed view | **nobody** — dies with its owning container (3); never `_destroy` |

**Note — classify by the accessor, not the type.** The returning function's
const-ness decides, not the type name:

  - `const *` return (or a type with no `_destroy` of its own) → **borrowed**;
    never free it (Category 4).
  - non-`const` return with a `_destroy` → **owned**; free it once after use
    (Category 1/3).
  - the same type can be owned in one call and borrowed in another — e.g.
    `PartitionInfoList_t` is owned from `partitions_for` but borrowed as a
    `TopicPartitionInfoMap` value; `OffsetAndMetadata_t` is a borrowed `OffsetMap`
    element.

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
    with `ObjectDisposedException`. `Consumer_destroy` is the same (joins the bg
    task first).
  - **Category 3 — owned result / container.** On the **caller's** thread (not the
    pump), after a query or poll: read/iterate, then `_destroy` the root exactly
    once. A **container is a borrow-root** — its elements and any key/value/topic/
    string bytes borrow into it (§3, §5.4), so it must outlive every borrow taken
    from it. **Default: copy-out** — copy each element/byte into an owned managed
    type, then `_destroy`. Metadata collections are always copy-out (small), and
    typed deserialization reads a transient span → owned `T` (copy-out too).
    **Keep-alive** (hold the root, expose zero-copy views, `_destroy` at `Dispose`)
    is a **deferred** option for the raw-byte surface only — see §5.4.
  - **Category 4 — borrowed view.** `ConsumerRecord_t`, `Node_t`, `_get` elements,
    borrowed strings have **no `_destroy`** — never free them, and never use them
    after their owning container (3) is destroyed. Represent as a transient cursor
    over the parent; don't let it escape the parent's lifetime.

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
  - Freeing a **borrowed** view (Category 4: `ConsumerRecord`, `Node`, a `_get`
    element, a borrowed string) — double-free / UAF; only the owning container is
    freed.
  - Destroying a **borrow-root** (`ConsumerRecords_t`, a list/map) while a borrowed
    element or byte slice from it is still in use (§5.4) — use-after-free.
  - Leaking an **owned** result (forgetting `_destroy` after marshalling a query
    map/list), or freeing it twice.

**Tests required:**

  - Create/dispose many producers — no leak/crash; send N and assert the handle
    count returns to baseline (covers `_destroy_all`).
  - Double-`Dispose` is safe; a call after `Dispose` throws
    `ObjectDisposedException`.
  - Poll a batch, read records, then dispose — no leak; a borrowed key/value/topic
    used after the batch is gone is prevented (copy-out) or kept valid by the
    wrapper (keep-alive), per §5.4.
  - Each query API (`committed`/`assignment`/`partitions_for`/…) frees its owned
    result exactly once after marshalling; borrowed elements are never freed.
  - Create/close many consumers — no leak; `Dispose` joins the bg task before
    `Consumer_destroy`.

---

## 3. String marshalling (UTF-8)

**Decision:** Every boundary string is UTF-8, marshalled by hand (the floor lacks
`LPUTF8Str` / `Marshal.PtrToStringUTF8`, §1): input via `Utf8.Pin` (encode + NUL +
pin); output via `Utf8.PtrToString` in **two forms** — a NUL-terminated
callee-owned `const char*` (NUL-scan + `GetString`), or a **length-delimited**
`const char* + int32_t out_len` that **borrows into the fetch batch** (use the
length, **never** NUL-scan).

| Direction | Sites | Helper |
|---|---|---|
| In | topic, config key/value, `error_next` message | `Utf8.Pin` |
| Out — NUL-terminated, valid until the value's own `_destroy` | `KafkaError_message`, `RecordMetadata_topic`, `ConsumerGroupMetadata_*`, other getters | `Utf8.PtrToString(ptr)` — NUL-scan |
| Out — length-delimited, borrowed from the batch, valid until `ConsumerRecords_destroy` | `ConsumerRecord_topic` / `_header_key`, `Node_host` / `_rack` | `Utf8.PtrToString(ptr, len)` — use `out_len`, **no scan** |
| Out — valid only during the callback | `RecordMetadata_copy` `topic` | `Utf8.PtrToString`, inside the callback |

**All output pointers are borrowed** — .NET copies (`GetString`) before the owning
handle is freed (or, for the callback, before it returns); it never owns the raw
pointer. The rows differ only in (1) termination (NUL-scan vs `out_len`) and
(2) which handle bounds the lifetime.

**Rule:**

  - Input: `Encoding.UTF8.GetBytes` → `new byte[len+1]` (trailing zero = NUL) →
    pin → pass `AddrOfPinnedObject`; unpin in `finally`. Marshalling copies (an
    encoding conversion) — fine for small topic/config, and **not** the zero-copy
    path (§4).
  - Output — **two forms**, both copy into a managed `string` (needs `unsafe`; a
    `#if NET6_0_OR_GREATER` span fast path is an internal optimization):
    - **NUL-terminated** callee-owned `const char*` (no `out_len`) →
      `Utf8.PtrToString(ptr)`, scan to NUL. Valid until `_destroy`.
    - **Length-delimited** `const char* + int32_t out_len` (consumer receive
      path) → `Utf8.PtrToString(ptr, out_len)` using the length — **never
      NUL-scan**: the slice borrows into the batch with no terminator, so a scan
      over-reads into the next field. Valid until `ConsumerRecords_destroy` (§5.4).
    In both cases **copy before free / before the callback returns** — the pointer
    dies with the handle; never store the raw pointer.
  - Never `[MarshalAs(LPStr)]` (ANSI) or `LPWStr` (UTF-16); never `LPUTF8Str` /
    `Marshal.PtrToStringUTF8` (absent on the floor).

**Why:** UTF-8 is the contract both ways, and the core reads input with
`to_string_lossy` — invalid bytes are silently *replaced*, not rejected, so an
`LPStr` mistake corrupts non-ASCII topics quietly (and hides in ASCII-only
tests). Hand-rolled helpers are the same reason confluent-kafka-dotnet ships
`StringAsPinnedUTF8` + `PtrToStringUTF8`. Copy-before-free follows from output
strings living in the handle's cached `CString` — except the consumer
receive-path strings (`ConsumerRecord_topic`, header keys, `Node` host/rack),
which return a `&str` **slice into the fetch batch** (`str::as_ptr` + `out_len`,
no terminator) and so take the length form and must be copied out before
`ConsumerRecords_destroy` (§5.4 / §27).

**Anti-patterns:**

  - `LPStr` / `LPWStr` for any string; a `const char*` return marshalled as
    `string`.
  - Reading an output pointer after its handle (or the callback) is gone.
  - **NUL-scanning a length-delimited slice** (`ConsumerRecord_topic` etc.) —
    over-reads past the batch slice (garbage / AV); use `out_len`.
  - Assuming ASCII (works until a non-ASCII topic corrupts silently).

**Tests required:**

  - A non-ASCII value round-trips through topic (in → out), a config value, and an
    error message.
  - A consumer `ConsumerRecord.topic` / header key with a non-ASCII, non-NUL-
    terminated value round-trips via `out_len` (not a scan).
  - `PtrToStringUTF8(IntPtr.Zero)` → `null`; a multi-byte char at the buffer
    boundary marshals correctly.

---

## 4. Zero-copy & buffer lifetime

**Two directions, mirror-image mechanics.** **Send** (managed → unmanaged, below)
pins a managed buffer and passes a pointer out — a *GC-moves* hazard. **Receive**
(unmanaged → managed, at the end) borrows a view over the native batch — a
*native-frees* hazard. **Pinning applies only to send.**

**Decision (send):** Pass key/value as `IntPtr` (address of the user's `byte[]`) + `int
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
    **Note:** `fixed` also yields a null pointer for an **empty** (non-null)
    array — not just for `null` — and the core rejects `(null, len ≥ 0)`. So an
    **empty** key/value (`Length == 0`) must pass a **non-null** pointer (a stack
    sentinel byte, or `GCHandle.AddrOfPinnedObject`), not the `fixed` null. (Python
    is unaffected — an empty `bytes` is non-null.)
  - This call-scoped rule depends on §7's inline-send decision; a deferred-send
    design (a background send thread, as in the Python binding) would have to hold
    the buffer until the deferred send runs.

**Why:** the borrow ends when the send call returns, so a call-scoped pin is
exactly sufficient; Task-scoped pinning is unnecessary and fragments the heap.
Mirrors confluent-kafka-dotnet (pin around `produceva` with `MSG_F_COPY`, `Free`
in `finally`). Any intermediate copy (`AllocHGlobal`+`Copy`, `ToArray()`) is the
per-message allocation CLAUDE.md §12 exists to prevent.

**Receive (unmanaged → managed) — the mirror.** The consumer path reverses
everything: the bytes originate in the **native** batch buffer (owned by
`ConsumerRecords_t`), and `ConsumerRecord_key` / `_value` / `_topic` hand back
`(ptr, len)` **borrowing** into it (§27, §3).

  - **No pinning.** Native memory isn't GC-managed — nothing moves, so nothing to
    pin. The `fixed` / `GCHandle` machinery above is send-only.
  - **The hazard flips** from *GC-moves* to *native-frees*: a managed view over
    the batch is a use-after-free the instant `ConsumerRecords_destroy` runs. The
    fix is **lifetime binding**, not pinning — copy before destroy, or keep the
    batch alive. Ownership rules, anti-patterns, and tests live in §5.4, §2
    (Category 3/4), and §3 (borrowed strings).
  - **"Buffer lifetime"** here is the *native* buffer's validity window
    (batch-scoped, until `_destroy`) — not a managed pin's window (call-scoped).

**Raw bytes are the one copy-out case — and why.** The typed path is zero-copy:
`IDeserializer<T>.Deserialize(ReadOnlySpan<byte>)` reads a span **directly over the
native bytes** and returns an owned `T`; a ref-struct `Span` can't be stored or
awaited, so it can't outlive the batch (safe). But the **raw-byte surface**
(`ConsumerRecord.Value` as bytes) is **copy-out by default** — the single place we
copy on receive — because the only zero-copy alternative is a native-backed
`ReadOnlyMemory<byte>` coupled to the batch lifetime, and in .NET a stored
`ReadOnlyMemory` over native memory is a use-after-`Dispose` footgun (Python is
safe only via its refcounted `memoryview`). So for raw bytes we trade one gen-0
copy for safety + the Java owned-`ConsumerRecord` shape; keep-alive zero-copy is
deferred (§5.4).

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
| `_code` (i32) | `int Code` |
| `_message` (UTF-8, handle-owned) | `Message` via `Utf8.PtrToString` (§3) |
| `_is_retriable` / `_is_fatal` | `IsRetriable` / `IsFatal` |
| `_destroy` | free after reading |

**Rule:**

  - **Operational:** the error handle arrives via the `out_error` param (fns that
    also return a value — `send`, `poll`, `_new`) **or as the return value** (fns
    that are `void` in Java — consumer `assign` / `subscribe` / `seek` /
    `unsubscribe`); **null = success** in both, non-null = error.
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
    producer/consumer). **Mandatory**, not optional: the ABI doesn't validate preconditions
    (CLAUDE.md §3) and some functions `assert!`/panic on violation — a panic
    across FFI is UB.
  - **Consumer additions** (the model is shared; the consumer adds two shapes):
    - **`wakeup()`** → the interrupted `poll` / `commit` / … surfaces a flat
      `KafkaException` with a **Wakeup** code (Java `WakeupException` semantics —
      raised **once**, then the flag clears and the op works again;
      consumer-threading §11). **Not** a subclass: the Python sibling keeps it flat
      (`pytest.raises(KafkaError)`, message `"woke"`), so do we. Distinct from
      **`CancellationToken`** cancellation → **`OperationCanceledException`** (the
      .NET-native cancel, §3 / CLAUDE.md §3 — the analog of Python re-raising
      `CancelledError`).
    - **Concurrent use** (the consumer is one-operation-in-flight) splits by
      method: a sync **state read** (`Assignment` / `Subscription` / `Paused` /
      `GroupMetadata`) → **`InvalidOperationException`** ("not safe for
      multi-threaded access"); a concurrent **async op** (`PollAsync` /
      `CommitAsync`) → **`KafkaException`** (ConcurrentModification). Mirrors
      Python (`RuntimeError` for state reads, `KafkaError` for blocking ops).

**Why:** a flat `KafkaException` matches what the ABI exposes and the Python
sibling. Preconditions are a separate surface because they are programmer errors,
not Kafka outcomes — Java raises `IllegalArgument`/`IllegalState`, Python
`ValueError`/`TypeError`, confluent-kafka-dotnet `ArgumentException`, all before
the native call. Ours must too, *and must* because the ABI would otherwise panic.

**Note — flat now, typed later.** The flat `KafkaException` is the *current*
choice; a Java-style typed hierarchy can be added later **non-breakingly**
(subclasses derive from `KafkaException`, so `catch (KafkaException)` still
works). If we do, the changes are localized: `KafkaException.FromHandle` becomes
a `Code` → subclass factory, and specific cases move from "flat + code" to a type
— e.g. the consumer's **Wakeup** would become a `WakeupException`. The
two-surface model, the handle lifecycle, and the precondition exceptions are
unchanged.

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
  - Consumer: `wakeup()` → the next `poll`/`commit` throws `KafkaException`
    (Wakeup) **once**, then the consumer works again; a concurrent sync state read
    → `InvalidOperationException`.

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

## 7. Async / Future completion → `TaskCompletionSource`

**Two completion models, set by the ABI.** The **producer** ABI is *pull*
(`get`/`get_all` block, `is_done` polls — no callback) → one background **pump**
does the blocking waits. The **consumer** ABI is *push* (every async op takes a
completion callback, §6) → the callback completes the `TaskCompletionSource`
directly, **no pump**. Both bridge to `Task<T>` via a `TaskCompletionSource` built
with `RunContinuationsAsynchronously`.

**Decision (producer — pull pump):** Java `Future<RecordMetadata>` → .NET `Task<RecordMetadata>`,
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

**Consumer — push, no pump.** The consumer ABI is *push*: every async op
(`Consumer_poll_async` / `commit_async` / `position_async` / …) takes a completion
callback (§6) that fires when the op resolves. So the consumer needs **no pump** —
the callback *is* the bridge:

```
Caller thread                       Core: runtime worker ──▶ dispatcher thread (1/consumer)
─────────────                       ────────────────────────────────────────────────────
PollAsync():                        worker task: poll(timeout).await   ← runs the op
  tcs = new TaskCompletionSource                 build (records | error)
  ud  = GCHandle.Alloc(tcs)  (§6)                enqueue completion ──┐
  Consumer_poll_async(…, cb, ud) ─► (guard held submit→op-complete)   ▼
  return tcs.Task                   dispatcher:  release guard, then
  … await tcs.Task                    cb(records | error, ud): marshal + free handles (§2)
     ◄── continuation on pool ─────   tcs.SetResult / SetException  (RunContinuationsAsync)
                                    no pump · one op in flight
```

  - `PollAsync` (etc.) makes a `TaskCompletionSource`, `GCHandle.Alloc`s it as
    `user_data` (§6), submits the `*_async` op with a kept-alive Cdecl callback,
    and returns `tcs.Task` immediately — no blocked thread.
  - The callback fires on a **dedicated callback-dispatcher thread** — the core
    runs the op on a runtime worker, then hands the completed op over a channel to
    that one thread, so callbacks are **serialized** on a single foreign thread
    (guard-rejection fires inline on the caller; shutdown, inline on the worker).
    It marshals the result (copy-out §5.4, or an error via `FromHandle` §5), frees
    the handles it owns (§2 Category 3), and completes the TCS — with
    **`RunContinuationsAsynchronously`** (essential: the continuation must not run
    on that dispatcher thread), exactly once, on every path (incl. the inline
    guard-rejection error).
  - **One operation in flight** per consumer (the access guard) → at most one
    pending `(callback, TCS)` at a time — no batching, unlike the producer pump.
    The guard is **released just before the callback fires** (held for the
    submit→op-complete window), so an `await`-then-resubmit from the continuation
    is safe — it won't hit the one-op rejection.
  - **Cancellation** = `CancellationToken` → `wakeup()` (aborts the in-flight op)
    → the callback fires with a Wakeup error → the `Task` cancels/faults
    (best-effort; §5, consumer-threading §11).
  - **`Dispose`**: await or `wakeup` the in-flight op, then `Consumer_destroy`
    (join the dispatcher; parent-outlives-children, §2).

**Consumer anti-patterns:**

  - A TCS without `RunContinuationsAsynchronously` — the awaiter's continuation
    runs on the core's dispatcher/worker thread → stalls the core (or deadlocks if
    the continuation calls back into the consumer).
  - Wrapping the *sync* variants (`Consumer_poll` + `block_on`) in a pump /
    `Task.Run` per op — sync-over-async; the push ABI makes it needless.
  - Letting the callback throw (unwinds into native, §6); completing the TCS
    twice; or not freeing the result/error handle + the `GCHandle` on some path
    (especially the inline guard-rejection error).
  - `Consumer_destroy` before the in-flight op's callback fires (use-after-free);
    a `Dispose` that doesn't `wakeup` + await the op.

**Consumer tests:**

  - `PollAsync` resolves with records / faults with `KafkaException` (mock
    `set_poll_error`); the result/error handle + `GCHandle` are freed exactly once.
  - `wakeup()` during an in-flight `poll` cancels/faults the `Task` **once**, then
    the consumer is reusable (§5); a `CancellationToken` cancel →
    `OperationCanceledException`.
  - A concurrent second op → `InvalidOperationException` (§5).
  - `Dispose` with an op in flight returns (doesn't hang) — the wakeup/join
    regression.

**Future direction — producer push (the consumer already does this).** The
consumer's push model above is exactly what the producer would gain from a
`Producer_send_cb(…, on_complete, user_data)`: the core fires `on_complete` from
**one shared completion task** per producer (CLAUDE.md §11 — not a spawn per
send), .NET drops the pump, and `SendAsync` just registers a kept-alive Cdecl
callback (§6) + a `GCHandle` over the `TaskCompletionSource` — zero blocked
threads. It needs a core/ABI change (Actor/Critic), so the producer's pull pump
above stays the current design; the consumer shows the target.

---

## 8. Native library loading, packaging & AOT

**Decision:** The native lib is our own Rust cdylib `confluent_kafka` (from `cargo
build --features ffi`). **No NuGet, ever** — the binding is consumed as a
**project / source reference**, and an MSBuild step copies the native into the
consuming app's output dir where default `[DllImport]` probing finds it. One
`Native` class, one `DllName`, no hand-rolled loader.

**Rule:**

  - **Packaging:** an MSBuild target copies `target/<cfg>/…confluent_kafka.…` to
    `$(OutDir)` (`CopyToOutputDirectory=PreserveNewest`); default probing (app base
    dir) resolves it. **No NuGet / no `runtimes/{rid}/native/` package** — the build
    copies the native explicitly. The bare `[DllImport("confluent_kafka")]` maps to
    the per-OS filename Cargo emits — never hardcode a filename/absolute path.
  - **Cross-platform:** one `DllName` covers every OS — the runtime maps it to
    `confluent_kafka.dll` / `libconfluent_kafka.so` / `libconfluent_kafka.dylib`.
    OS/arch/libc is selected by *which native is copied*, keyed by RID (`win-x64`,
    `linux-x64`, `linux-musl-x64`, `osx-arm64`, …). With **no NuGet**, that
    selection happens at **build/publish** time (`dotnet publish -r <rid>` copies
    the matching native) or via `NativeLibrary.SetDllImportResolver` — never NuGet
    RID assets. musl/Alpine = the `linux-musl-x64` RID (build the musl-target
    cdylib), **not** a second `Native` class or `/etc/os-release` detection.
  - **Loading:** rely on default `[DllImport]` resolution — do **not** port
    confluent-kafka-dotnet's `Librdkafka.Initialize` (manual `dlopen`/`LoadLibraryEx`
    preload, reflection binding, distro/GSSAPI variant selection); none applies to
    one self-built cdylib. Custom probing → `NativeLibrary.SetDllImportResolver`
    (modern), never a reflection loader; on net462 keep the native in the app dir
    (a `LoadLibraryEx` preload is a last resort).
  - **Single `Native` class**, one `DllName = "confluent_kafka"` — the equivalent
    of only their default `NativeMethods`; no `_Alpine`/`_Centos8` variants (our
    pure-Rust TLS/SASL has no GSSAPI system dep, and musl is a RID, not a filename).
  - **AOT:** not committed to, but kept open — direct `[DllImport]` (not a
    reflection loader) is AOT-amenable, unlike theirs. Don't add reflection-based
    loading.

**Why:** we build and control one native and consume it by project reference, so
the drivers of their loader (third-party binary placement, musl/glibc + GSSAPI
variant selection by filename) don't exist for us; the RID *classifies* which
binary is needed, but the **build/publish (or a resolver) delivers it** — no NuGet
required. Cargo's output names already match default P/Invoke resolution.

**Anti-patterns:**

  - Porting `Librdkafka.Initialize` / reflection binding / 3× NativeMethods /
    `/etc/os-release` — unnecessary, and it kills AOT.
  - Hardcoding an absolute path or platform filename in `[DllImport]`.
  - Adding a NuGet packaging path, or assuming any `runtimes/{rid}/native/`
    auto-copy — the decision is **no NuGet**; the build copies the native.
  - A second `Native` class / `DllName` for musl — musl is the `linux-musl-x64`
    RID, same `DllName`.

**Tests required:**

  - A smoke call (`MockProducer` create → send → close) loads and works on net462,
    net8.0, net10.0 on Windows + Linux in CI, with the native copied to output.
  - A missing native gives a clear `DllNotFoundException`, not an obscure crash.
