# .NET binding — FFI marshalling & ownership

Deep-dive rules for marshalling across the C ABI (`confluent_kafka.h`,
`kafka_producer_*` / `kafka_common_KafkaError_*`) from .NET via P/Invoke.
Supplements `bindings/dotnet/CLAUDE.md`. Review ground truth is the **C ABI
header** and the **Kafka Java public API**.

Each section leads with **Decision** (the intent, one line), then **Rule**,
**Why**, **Anti-patterns**, and **Tests required** (`consumer-threading.md` precedent).

## How this file is organized

Three parts, so each client reads as one complete, self-contained story:

  - **Part 0 · Shared mechanics** — the *only* genuinely-shared content:
    P/Invoke declarations & the type map (§0.1), native loading / packaging /
    AOT (§0.2), and the shared thread-topology framing (§0.3).
  - **Part A · Producer** — the producer end to end: thread model (§A1),
    handles (§A2), strings (§A3), zero-copy **send** (§A4), error model (§A5),
    the `RecordMetadata_copy` callback (§A6), async completion (§A7).
  - **Part B · Consumer** — the consumer end to end: thread model (§B1),
    handles (§B2), strings (§B3), zero-copy **receive** (§B4), error model
    (§B5), completion callbacks (§B6), async completion (§B7).

**Parts A and B are each self-sufficient** — the shared marshalling mechanism
(strings, errors, callbacks) is written out *in full in both parts*, tailored to
that client, so you never cross-reference the other client to understand one.
Redundancy is intentional. Read Part 0 once, then A or B straight through; each
section describes only that client's decisions.

---

# Part 0 · Shared mechanics

## 0.1 P/Invoke declarations & type mapping

**Decision:** One `internal static class NativeMethods` (in `NativeMethods.cs`,
the name analyzer CA1060 requires) of classic `[DllImport(...,
Cdecl)]` declarations, uniform across `netstandard2.0` + `net8.0` + `net10.0`.
netstandard2.0 is the floor (covers .NET Framework 4.6.2), so the modern interop
APIs — `[LibraryImport]`, `delegate* unmanaged`, `[UnmanagedCallersOnly]`,
`UnmanagedType.LPUTF8Str`, `Marshal.PtrToStringUTF8` — are **off-limits** (they
don't exist on the floor). Same classic toolkit as confluent-kafka-dotnet,
pointed at our fixed-width, handle-error ABI.

**Note — support matrix (aligned with ckd 2.15.0).** We target the same reach as
ckd's `Confluent.Kafka` — **net462 · netstandard2.0 · net8.0 · net10.0** — but
satisfy **net462 through the `netstandard2.0` asset**, not a separate `net462`
target. We can because we have **no Framework-specific code**: one self-built
cdylib resolved by default `[DllImport]` probing (no `NativeLibrary` resolver, no
distro-variant `NativeMethods`, no `#if NET462`), so a net462 build would be
byte-identical to the ns2.0 one. ckd ships an explicit `net462` target only
because librdkafka's loader *is* Framework-specific (`LoadNetFrameworkDelegates`,
Mono support, hardcoded `alpine`/`centos8` DllNames) — the machinery §0.2 says
**not** to port. The only net462-via-ns2.0 cost is the `System.Memory` facade +
binding redirects (automatic in SDK-style projects; proven by the net462 TFM
smoke test — §0.2 tests / CLAUDE.md §7.4). Add an explicit `net462` target later
only if that friction bites — cheap and non-breaking.

### C ABI → C# type map

| C ABI type | C# | Notes |
|---|---|---|
| `int32_t` | `int` | **Never `UIntPtr`/`nint`** — our ABI has no `size_t`. |
| `int64_t` | `long` | |
| `bool` | `[MarshalAs(UnmanagedType.I1)] bool` | C `bool` is 1 byte; default marshals a 4-byte Win32 `BOOL`. |
| opaque `*_t *` | `IntPtr` → `SafeHandle` above (§A2/§B2) | Never a C# struct mirroring `_private[0]`. |
| `const char *` in | `IntPtr` to a pinned NUL-terminated UTF-8 buffer (§A3/§B3) | No `LPStr` (ANSI), no `LPUTF8Str`. |
| `const char *` out (callee-owned) | `IntPtr` → `Utf8Marshal.PtrToString` (§A3/§B3) | Never a `string` return — the marshaller would free it. |
| `const uint8_t *` + `int32_t len` | `IntPtr` (pinned `byte[]`) + `int` (§A4/§B4) | Zero-copy, call-scoped pin (send) or borrow (receive). |
| `T **` out-param / array | `out IntPtr` / `IntPtr[]` | e.g. `out_error`; `get_all` arrays. |
| `ProducerRecord_t` | `[StructLayout(LayoutKind.Sequential)] struct`, array `[]` | Fixed-width ⇒ identical layout. |
| `void (*cb)(...)` / `void *` | Cdecl delegate, kept alive (§A6/§B6) / `IntPtr` (`GCHandle`) | |

**Rule:**

  - `[DllImport("confluent_kafka", CallingConvention = CallingConvention.Cdecl)]`
    (Cdecl matches the Rust exports' `extern "C"` C-ABI — the generated header is
    plain C with no `extern "C"` block; the bare name maps to
    `confluent_kafka.dll` / `lib….so` / `lib….dylib`). One declaration set for all
    TFMs — no per-TFM `#if`.
  - Set **`EntryPoint`** to the full ABI symbol (`kafka_<pkg>_<Type>_<method>`)
    whenever the C# method uses the short name (dropping the `kafka_<pkg>_` prefix
    per CLAUDE.md §6.3) — otherwise the marshaller probes the C# name and throws
    `EntryPointNotFoundException` at **runtime**, not compile time.
  - Follow the type map verbatim: sizes are `int`/`long`, `bool` is `I1`, opaque
    handles stay `IntPtr` here (wrapped in a `SafeHandle` one layer up, §A2/§B2),
    UTF-8 strings and key/value bytes are marshalled by hand (§A3/§B3, §A4/§B4).
  - Prefer `ProducerProperties_put` / `ConsumerProperties_put` over `_from_configs`
    (avoids marshalling a `const char *const *`).

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
  - Porting confluent-kafka-dotnet's reflection loader / 3× NativeMethods (§0.2).

**Tests required:**

  - A non-ASCII topic round-trips (guards manual UTF-8; catches `LPStr`).
  - Each `bool`-returning fn (`is_done`/`is_retriable`/`is_fatal`) is correct
    (guards a missing `I1`).
  - `NativeMethods` loads on net462, net8.0, net10.0 (TFM smoke test).

---

## 0.2 Native library loading, packaging & AOT

**Decision:** The native lib is our own Rust cdylib `confluent_kafka` (from `cargo
build --features ffi`), delivered in **two phases**. **Now (pre-publish):** no
NuGet at all — the binding is consumed as a **project / source reference** and an
MSBuild step copies the native into the consuming app's output dir, where default
`[DllImport]` probing finds it. **At publish:** the binding ships as a NuGet
package that carries the native as **`runtimes/{rid}/native/` assets in the same
package**; NuGet copies them to output and the same probing resolves them. Only
*delivery* changes between phases — the loading path is identical: one
`NativeMethods` class, one `DllName`, no hand-rolled loader.

**Rule:**

  - **Packaging — now (pre-publish):** an MSBuild target copies
    `target/<cfg>/…confluent_kafka.…` to `$(OutDir)`
    (`CopyToOutputDirectory=PreserveNewest`); default probing (app base dir)
    resolves it. No NuGet, because the consumer builds the Rust themselves. The
    bare `[DllImport("confluent_kafka")]` maps to the per-OS filename Cargo emits
    — never hardcode a filename/absolute path.
  - **Packaging — at publish:** the binding is a NuGet package and the native
    rides along as `runtimes/{rid}/native/…` assets **in that same package**, not
    a companion `*.redist`. Same package because the cdylib is built from this
    repo at this commit and versions in lockstep with the managed assembly — one
    package id makes managed/native version skew structurally impossible. (ckd
    splits `librdkafka.redist` out only because librdkafka is a *third-party*
    artifact with its own release cadence and its own consumers; that driver does
    not apply to us.) Revisit a split only if carrying every RID makes the
    package too large. A NuGet consumer has no Rust toolchain, so RID assets are
    the **only** delivery path once we publish.
  - **Cross-platform:** one `DllName` covers every OS — the runtime maps it to
    `confluent_kafka.dll` / `libconfluent_kafka.so` / `libconfluent_kafka.dylib`.
    OS/arch/libc is selected by *which native is present*, keyed by RID
    (`win-x64`, `linux-x64`, `linux-musl-x64`, `osx-arm64`, …). **Now** that
    selection happens at build/publish time (`dotnet publish -r <rid>` copies the
    matching native) or via `NativeLibrary.SetDllImportResolver`; **at publish**
    NuGet's RID asset resolution does it. musl/Alpine = the `linux-musl-x64` RID
    (build the musl-target cdylib), **not** a second `NativeMethods` class or
    `/etc/os-release` detection.
  - **Loading (identical in both phases):** rely on default `[DllImport]` resolution — do **not** port
    confluent-kafka-dotnet's `Librdkafka.Initialize` (manual `dlopen`/`LoadLibraryEx`
    preload, reflection binding, distro/GSSAPI variant selection); none applies to
    one self-built cdylib. Custom probing → `NativeLibrary.SetDllImportResolver`
    (modern), never a reflection loader; on net462 keep the native in the app dir
    (a `LoadLibraryEx` preload is a last resort).
  - **Single `NativeMethods` class**, one `DllName = "confluent_kafka"` — the equivalent
    of only their default `NativeMethods`; no `_Alpine`/`_Centos8` variants (our
    pure-Rust TLS/SASL has no GSSAPI system dep, and musl is a RID, not a filename).
  - **AOT:** not committed to, but kept open — direct `[DllImport]` (not a
    reflection loader) is AOT-amenable, unlike theirs. Don't add reflection-based
    loading.

**Why:** we build and control one native, and it versions with the managed
assembly, so the drivers of their loader (third-party binary placement,
musl/glibc + GSSAPI variant selection by filename) don't exist for us. The RID
*classifies* which binary is needed; **who delivers it** is the only thing that
changes across the two phases — the local Rust build now, NuGet RID assets at
publish. Cargo's output names already match default P/Invoke resolution, so the
loading code is phase-independent.

**Anti-patterns:**

  - Porting `Librdkafka.Initialize` / reflection binding / 3× NativeMethods /
    `/etc/os-release` — unnecessary, and it kills AOT.
  - Hardcoding an absolute path or platform filename in `[DllImport]`.
  - **Now:** assuming a `runtimes/{rid}/native/` auto-copy — pre-publish there is
    no package, so the build copies the native explicitly. **At publish:**
    hand-rolling that copy instead of using RID assets, or splitting the native
    into a companion package without a package-size reason.
  - A second `NativeMethods` class / `DllName` for musl — musl is the `linux-musl-x64`
    RID, same `DllName`.

**Tests required:**

  - A smoke call (`MockProducer` create → send → close) loads and works on net462,
    net8.0, net10.0 on Windows + Linux in CI, with the native copied to output.
  - A missing native gives a clear `DllNotFoundException`, not an obscure crash.
  - **At publish:** `dotnet pack` emits `runtimes/{rid}/native/` for every
    supported RID, and a consuming test project resolves the native **from the
    package alone** — no Rust toolchain, no local `cargo build`.

---

## 0.3 Thread topology & thread-safety (shared framing)

The whole-system picture every section assumes. The C ABI hides all Kafka I/O on
native threads **owned by the Rust core**, and **both clients expose a pull *and*
a push completion surface** — each with its own native dispatcher thread — so the
thread layout is a **binding choice**, not ABI-fixed. As currently sketched: the
**consumer** uses push → **no** .NET thread (§B1); the **producer**'s completion
model is **open** (§A7) — the pull-pump option adds one .NET pump thread, while
the push option would use the producer's already-present native dispatcher. The
per-client thread diagrams and rules are in **§A1** (producer) and **§B1**
(consumer).

**Why:** Java's single-Selector NIO model on tokio (CLAUDE.md §8) — unlike
librdkafka (a thread per broker + a mandatory `rd_kafka_poll` loop, which
confluent-kafka-dotnet services with a `LongRunning` `callbackTask`). So our
native thread count is independent of cluster size, there is no poll loop, and
the multi-thread runtime is what makes `block_on` from .NET deadlock-free. This
applies to both clients.

**Anti-patterns (both clients):**

  - A `callbackTask`-style poll-loop thread — nothing to poll here.
  - Assuming thread-per-broker / native threads scaling with cluster size.

**Tests required (both clients):** `Dispose` drains before destroying the handle —
the producer joins the pump (§A1/§A7), the consumer wakes+awaits the in-flight op
(§B1/§B7).

---

# Part A · Producer

## A1 Thread model (producer)

**Decision:** Per real `KafkaProducer`, the core owns a multi-thread tokio
runtime + one Sender task over a single async Selector; the .NET side adds at most
**one** completion pump (§A7), never a per-send thread or a poll loop.

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
  - .NET side: caller thread(s) + **exactly one** completion pump (§A7); no
    per-send threads. **No poll loop** — the core self-drives, so a stalled pump
    delays result delivery but not sending.
  - FFI is callable from any .NET thread; the core serializes via the producer's
    internal `Mutex`, so concurrent `SendAsync` is safe — don't add your own lock.
  - `block_on` parks only the *calling* .NET thread; the Sender keeps running on
    the runtime's worker threads, so a blocked pump can't deadlock it.
  - The one callback (`RecordMetadata_copy`) fires synchronously on the caller's
    (pump) thread — not a foreign thread (§A6).

**Why:** Java's single-Selector NIO model on tokio (§0.3) — a fixed native thread
count, no poll loop, and a multi-thread runtime that makes `block_on` from the
pump deadlock-free (the Sender runs on other worker threads).

**Anti-patterns:**

  - A binding-side lock around producer sends — the producer's `Mutex` already
    serializes concurrent `SendAsync` (don't double-lock).
  - A `callbackTask`-style poll-loop thread; thread-per-broker assumptions
    (shared, §0.3).

**Tests required:**

  - Concurrent `SendAsync` from many threads is correct (the `Mutex` holds).
  - `Dispose` drains before destroying the handle — joins the pump (Option A, §A7).
  - *(Option A only)* a long-blocked pump doesn't stop new sends being enqueued.

---

## A2 Handle ownership & lifecycle (`SafeHandle`)

**Decision:** Two ownership categories:

1. **Client / config** (producer, properties) → a `SafeHandle` that frees exactly
   once (the blocking destroy joins the background task first).
2. **Flat transient** (future / metadata / error) → read-and-free promptly,
   **not** wrapped (a finalizable object per op is hot-path waste).

| Handle | Category | Created by | Freed by |
|---|---|---|---|
| `Producer_t` | 1 — client (`SafeHandle`) | `KafkaProducer_new` / `MockProducer_new` | `ReleaseHandle → Producer_destroy` (via `Dispose`) |
| `ProducerProperties_t` | 1 — config (`SafeHandle`, short) | `ProducerProperties_new` / `_from_configs` | the binding, after `KafkaProducer_new` |
| `FutureRecordMetadata_t` | 2 — flat transient | `Producer_send` / `_send_batch` | the pump: `_destroy_all` (`get_all` doesn't consume) |
| `RecordMetadata_t` | 2 — flat transient | `_get` / `_get_all` | the pump: `RecordMetadata_copy` (extract+free) or `_destroy` |
| `KafkaError_t` | 2 — flat transient | any `out_error` slot | the reader: read accessors, then `_destroy` |

**Rule:**

  - `SafeProducerHandle : SafeHandle` — `ownsHandle: true`, `IsInvalid => handle
    == IntPtr.Zero`, `ReleaseHandle` calls `Producer_destroy`. Runtime frees
    exactly once, even on exceptions.
  - Transient handles: free in a `finally` on the pump; the managed
    `RecordMetadata`/`KafkaException` hold **copied values**, never the handle
    (§A7). After `get_all`, each index has exactly one non-null of {metadata,
    error} — free that one, plus the future via `_destroy_all`. All `_destroy`
    are null-safe; `RecordMetadata_copy` frees its own handle (don't double-free).
  - **Parent outlives children:** the producer must not be destroyed while the
    pump holds futures from it — enforce via `Dispose` ordering (stop sends → join
    pump → release handle, §A7), not `DangerousAddRef`.
  - **Prefer `Dispose` over the finalizer:** `Producer_destroy` blocks (drops the
    runtime, waiting for the Sender), which is wrong on the finalizer thread.
    `Dispose` flushes/closes and joins the pump first; guard use-after-dispose
    with `ObjectDisposedException`.

**Why:** `SafeHandle` is the robust form of "call `_destroy` exactly once," even
through exceptions; `IsInvalid == zero` matches our null-safe destroy. This is
confluent-kafka-dotnet's `SafeHandleZeroIsInvalid` pattern. But a per-message
`SafeHandle` allocates a finalizable object per record, so transient handles are
read-and-freed instead (their lifetime is one pump cycle). `Producer_destroy`
blocks (it drops the runtime and waits for the Sender), so the producer closes via
`Dispose`, never the finalizer.

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

## A3 String marshalling (UTF-8)

**Decision:** Every boundary string is UTF-8, marshalled by hand (the floor lacks
`LPUTF8Str` / `Marshal.PtrToStringUTF8`, §0.1): input via `Utf8Marshal.Pin` (encode + NUL
+ pin); output via `Utf8Marshal.PtrToString` on a **NUL-terminated** callee-owned
`const char*` (NUL-scan + `GetString`), copied before the owning handle is freed.

| Direction | Sites | Helper |
|---|---|---|
| In | topic, config key/value, `error_next` message | `Utf8Marshal.Pin` |
| Out — NUL-terminated, valid until the value's own `_destroy` | `KafkaError_message`, `RecordMetadata_topic`, other getters | `Utf8Marshal.PtrToString(ptr)` — NUL-scan |
| Out — valid only during the callback | `RecordMetadata_copy` `topic` | `Utf8Marshal.PtrToString`, inside the callback |

**All output pointers are borrowed** — .NET copies (`GetString`) before the owning
handle is freed (or, for the callback, before it returns); it never owns the raw
pointer.

**Rule:**

  - Input: `Encoding.UTF8.GetBytes` → `new byte[len+1]` (trailing zero = NUL) →
    pin → pass `AddrOfPinnedObject`; unpin in `finally`. Marshalling copies (an
    encoding conversion) — fine for small topic/config, and **not** the zero-copy
    path (§A4).
  - Output — copy into a managed `string` (needs `unsafe`; a
    `#if NET6_0_OR_GREATER` span fast path is an internal optimization):
    **NUL-terminated** callee-owned `const char*` (no `out_len`) →
    `Utf8Marshal.PtrToString(ptr)`, scan to NUL. Valid until `_destroy`. **Copy before
    free / before the callback returns** — the pointer dies with the handle;
    never store the raw pointer.
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
  - `Utf8Marshal.PtrToString(IntPtr.Zero)` → `null`; a multi-byte char at the
    buffer boundary marshals correctly.

---

## A4 Zero-copy & buffer lifetime — send path

**Decision:** Pass key/value as `IntPtr` (address of the user's `byte[]`) + `int
len` with **no intermediate copy**, pinned **only for the send call** — the pin
is call-scoped, not Task-scoped. (Managed → unmanaged: a *GC-moves* hazard, fixed
by pinning.)

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
        future = NativeMethods.kafka_producer_Producer_send(handle, topicPtr, partition,
            timestamp, (IntPtr)k, key is null ? -1 : key.Length, /* value… */ out err);
    ```
    **Note:** `fixed` also yields a null pointer for an **empty** (non-null)
    array — not just for `null` — and the core rejects `(null, len ≥ 0)`. So an
    **empty** key/value (`Length == 0`) must pass a **non-null** pointer — use a
    **stack sentinel byte** (guaranteed non-null), not the `fixed` null.
    (`GCHandle.AddrOfPinnedObject` returns non-null for empty arrays on current
    runtimes too, but that's **undocumented** — prefer the sentinel. Python is
    unaffected — an empty `bytes` is non-null.)
  - This call-scoped rule depends on §A7's inline-send decision; a deferred-send
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

## A5 Error model (operational vs. precondition)

**Decision:** Two surfaces. **Core (operational) errors** cross as a
`kafka_common_KafkaError_t` handle → one flat `KafkaException` (code + retriable +
fatal + message), mirroring the Python sibling. **Precondition errors** (bad
argument / state) are validated in the binding *before* the FFI call and raise
standard .NET exceptions — never `KafkaException`.

| `KafkaError_*` accessor | → C# |
|---|---|
| `_code` (i32) | `int Code` |
| `_message` (UTF-8, handle-owned) | `Message` via `Utf8Marshal.PtrToString` (§A3) |
| `_is_retriable` / `_is_fatal` | `IsRetriable` / `IsFatal` |
| `_destroy` | free after reading |

**Rule:**

  - **Operational:** the error handle arrives via the `out_error` param (fns that
    also return a value — `send`, `_new`); **null = success**, non-null = error.
    `KafkaException.FromHandle` reads the accessors (message **before** free),
    then `_destroy` in a `finally` — freed exactly once even if construction
    throws (§A2, §A3); the exception holds copied values, not the handle. Sync
    failures `throw`; async send failures fault the `Task` (§A7) — same
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

**Note — flat now, typed later.** The flat `KafkaException` is the *current*
choice; a Java-style typed hierarchy can be added later **non-breakingly**
(subclasses derive from `KafkaException`, so `catch (KafkaException)` still
works). If we do, the changes are localized: `KafkaException.FromHandle` becomes
a `Code` → subclass factory, and specific cases move from "flat + code" to a
type. The two-surface model, the handle lifecycle, and the precondition
exceptions are unchanged.

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

## A6 Callback & delegate marshalling (`RecordMetadata_copy`)

**Decision:** `RecordMetadata_copy` is the ABI's one **synchronous** callback (an
optional copy convenience) — the async *completion* callbacks (producer `*_async`,
§A7) are the other kind. Marshal it as a kept-alive
`[UnmanagedFunctionPointer(Cdecl)]` delegate (classic — no function pointers on the
floor), keep the body no-throw, and pass context via a `GCHandle` in `user_data`.
Using it is optional — the per-field accessors
(`_offset`/`_partition`/`_topic`/`_timestamp` + `_destroy`) avoid callbacks
entirely.

The callback fires **synchronously on the caller's (pump) thread** and returns
before `RecordMetadata_copy` does; its `topic` pointer is valid **only during the
call** (the core frees the handle right after — §A3):

    void cb(int64_t offset, int32_t partition, const char* topic, int64_t timestamp, void* user_data)

**Rule:**

  - Named delegate type, `[UnmanagedFunctionPointer(CallingConvention.Cdecl)]`,
    blittable params (`long`/`int`/`IntPtr`/`IntPtr`). Take `topic` as `IntPtr`
    and `Utf8Marshal.PtrToString` it *inside* the callback (§A3) — never `string`. Hold
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
    returns (handle already freed — §A3).
  - Leaking or double-freeing the `user_data` `GCHandle`.

**Tests required:**

  - Correct offset/partition/topic/timestamp delivered; a non-ASCII topic read
    inside the callback is correct (ties §A3).
  - An exception thrown in the callback is caught, doesn't crash or unwind into
    native, and is re-surfaced.
  - Aggressive GC while the callback is in use doesn't crash (keep-alive; §A1).

---

## A7 Async / Future completion → `TaskCompletionSource`

**Decision:** The producer exposes **both** a pull surface (a future you
block/poll — `get` / `get_all` / `is_done`) and a push surface
(`Producer_send_async` takes a completion callback (§A6) fired on a native
dispatcher thread; both verified in the header, §0.3). So the producer's
completion model is a **binding choice**, not ABI-forced — **OPEN** (pull pump vs
push callback): both are viable and *shipped*; the trade-off is below, decision
deferred. Both bridge to `Task<T>` via a `TaskCompletionSource` built with
`RunContinuationsAsynchronously`.

**Option A: pull pump.** Java `Future<RecordMetadata>` → .NET
`Task<RecordMetadata>`, completed by **one** background pump. `SendAsync` enqueues
and returns instantly with a `TaskCompletionSource`-backed `Task`; the pump blocks
on the batched `get_all` and completes each TCS. Mirrors the Python binding's
`poll_futures_thread` (python-ffi.md §6).

```
Caller thread                         Completion pump (one bg thread)
─────────────                         ───────────────────────────────
SendAsync():                          loop:
  pin key/value (call-scoped, §A4)      drain a batch of (future, tcs)
  Producer_send() → future handle       get_all(futures[])   ← BLOCKS
  new TaskCompletionSource (tcs)        tcs[i].SetResult / SetException
  enqueue (future, tcs); return Task    destroy_all(futures)
Dispose(): signal + join the pump ◄──── on shutdown: drain, fault pending, exit
```

**Rule (Option A):**

  - `SendAsync` never calls a blocking `_get`/`_get_all` on the caller's thread —
    it pins (§A4), calls `Producer_send` (inline; a fast enqueue), checks the sync
    `out_error`, enqueues `(future, tcs)`, returns `tcs.Task`. Inline send is fine
    because .NET has no GIL (a send-batching thread is an optional throughput
    tweak, not required).
  - **Exactly one** pump thread does all waits via batched `get_all` — O(1)
    threads for unbounded in-flight sends. Per result: read fields + free handles
    on the pump (§A2), `SetResult`/`SetException`, `destroy_all` the futures.
  - Build the TCS with `RunContinuationsAsynchronously` — otherwise a slow awaiter
    continuation runs on the pump thread and stalls every other completion.
  - Completion is exactly-once (guard cancelled/done, free handles on every path).
    Cancellation is best-effort: it discards the result, it does **not** abort an
    in-flight send.
  - `Dispose`: stop sends → drain/fault pending → **join the pump** →
    `flush`/`close` → release the producer `SafeHandle`. Optional fast path: if
    `is_done` at send time, complete synchronously (a `ValueTask`, no queue).

**Why (Option A's case):** `Producer_send` copies key/value **synchronously** → a
**call-scoped pin** (§A4), and one pump batches many completions per `get_all` —
O(1) threads for unbounded in-flight sends. (The naive `Task.Run(get)` per send
parks a pool thread per message — sync-over-async — which the pump avoids; matches
CLAUDE.md §11 "shared completion task, not per-message spawn" and the Python
design.)

**Option B: push callback (available *now*, not future).** The producer ABI
*already* ships `Producer_send_async(…, callback, user_data)` (verified:
`src/ffi/producer.rs`, `confluent_kafka.h`), firing on a per-producer dispatcher
thread with `(RecordMetadata*, KafkaError*)`. `SendAsync` would register a
kept-alive Cdecl callback (§A6) + a `GCHandle`(TCS); the callback completes the
TCS. **Zero blocked threads, per-message, no pump.**

**Open decision — pull pump (A) vs push callback (B):**

  - **A (pull pump)** favors **call-scoped pinning** (`Producer_send` copies
    synchronously, §A4) + **`get_all` batching** of many completions; costs one
    pump thread. Python makes this choice.
  - **B (push)** favors **zero blocked threads** — but the header says
    `send_async` **borrows** key/value *"until `callback` fires"*, so the **pin
    lasts until completion** (weaker than A's call-scoped pin, §A4), and it's one
    callback per send (no `get_all` batching).

Not committed — decide when the producer send path is built (as CLAUDE.md §6.4
defers the consumer's copy-out-vs-keep-alive).

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

# Part B · Consumer

## B1 Thread model (consumer)

**Decision:** Per real `KafkaConsumer`, the core owns a background task
(`ConsumerNetworkThread`) over a single async Selector **plus a dedicated
callback-dispatcher thread** — both native. The .NET side adds **no pump**; it
uses the ABI's push surface (§B7), and completions arrive on the core's **foreign**
dispatcher thread.

```
   .NET (managed)                    │ C ABI │       Rust core (native, per consumer)
   ─────────────                     │       │       ─────────────────────────────────
   caller thread(s):                 │       │   tokio multi-thread runtime (worker POOL)
     PollAsync → *_async(…, cb) ─────│──────►│     consumer bg task (ConsumerNetworkThread):
       returns Task (core guards)    │       │       NetworkClient + ONE async Selector
   (NO .NET pump, NO managed guard)  │       │       ↕ multiplexes ALL brokers (event-driven)
     ◄── cb fires here (→ TCS) ──────│◄──────│     callback-dispatcher thread (1, native):
         on the dispatcher thread    │       │       fires completion callbacks
   Dispose: close_(w/timeout|async)  │       │   (created in KafkaConsumer_new, dropped on _destroy)
            → destroy (no drain)      │       │
```

**Rule:**

  - Native side (per real `KafkaConsumer`): the core runs the consumer's
    **background task** (single `ConsumerNetworkThread`, consumer-threading §10)
    over ONE async Selector (all brokers multiplexed — NOT thread-per-broker),
    **plus a dedicated callback-dispatcher thread** — both created in
    `KafkaConsumer_new`, torn down by `Consumer_destroy`.
  - .NET side: caller thread(s) **only — no pump** (the ABI pushes completions,
    §B7). **No poll loop** — the core self-drives.
  - **One operation in flight** per consumer, **single-owner / not thread-safe**
    (M3/P2, Python-parity). The **Rust core's own access guard** serializes ops —
    there is **no managed mirror**. A concurrent **async op** is rejected by the
    core inline (it fires the callback on the caller thread with a
    `ConcurrentModification` error) and surfaces as a **faulted `Task`** carrying a
    `KafkaException` (§B5) — not a managed synchronous pre-check throw. A concurrent
    **sync state read** surfaces as `InvalidOperationException` from the core's
    null-handle rejection path (§B5). The completion callback runs on the
    **dispatcher thread (foreign)**, not the caller → `RunContinuationsAsynchronously`
    + no-throw (§B6/§B7). `Consumer_wakeup` is the one cross-thread call (§B5 /
    consumer-threading §11).

**Why:** Java's single-Selector NIO model on tokio (§0.3) — a fixed native thread
count, no poll loop. The completion callback fires on the core's foreign
dispatcher thread, so the TCS must use `RunContinuationsAsynchronously` (§B7) or
the awaiter's continuation would run on — and stall — that thread. Serialization is
the core's job (single-owner); a managed guard mirroring it was an extra .NET-only
layer, removed in M3/P2 to match the in-repo Python sibling.

**Anti-patterns:**

  - A binding-side lock **or a managed access guard** to serialize concurrent ops
    — the consumer is single-owner, so the **core** rejects a concurrent op (§B5),
    not a managed layer.
  - A TCS **without** `RunContinuationsAsynchronously` → the continuation runs
    **inline on the dispatcher thread** (stalls it / deadlocks, §B6/§B7).
  - A `callbackTask`-style poll-loop thread; thread-per-broker assumptions
    (shared, §0.3).

**Tests required:**

  - A concurrent async op → a faulted `Task` (`KafkaException` /
    ConcurrentModification), a concurrent sync state read →
    `InvalidOperationException`, not corruption (§B5); the completion callback
    doesn't run its continuation inline on the dispatcher.
  - `Dispose` / `DisposeAsync` return without hanging even with an (unawaited) op in
    flight — teardown is `close_(with_timeout|async)` → `Consumer_destroy` with **no
    separate-op drain** (the awaiter is the disposer, §B7).

---

## B2 Handle ownership & lifecycle (`SafeHandle`)

**Decision:** Four ownership categories:

1. **Client / config** (consumer, properties) → a `SafeHandle` freed exactly once
   (teardown routes through `Consumer_close` first — see the Rule).
2. **Flat transient** (error) → read-and-free promptly, **not** wrapped (a
   finalizable object per op is waste).
3. **Owned result / container** (the poll batch + every query-result list/map) →
   the caller frees once after reading; a container is a *borrow-root* whose
   `_destroy` invalidates the elements/bytes borrowed from it.
4. **Borrowed view** (`ConsumerRecord`, `Node`, every `_get` element) → **never
   freed** by the binding.

| Handle | Category | Freed by |
|---|---|---|
| `Consumer_t` | 1 — client (`SafeHandle`) | `Dispose`: drain → `Consumer_close` → `Consumer_destroy` (destroy is **fire-and-forget** — cancels in-flight ops; §B7 + Rule) |
| `ConsumerProperties_t` | 1 — config (`SafeHandle`, short) | the binding, after `KafkaConsumer_new` |
| `KafkaError_t` (any `out_error`) | 2 — flat transient | reader: read accessors, then `_destroy` |
| `ConsumerRecords_t` (poll batch) | 3 — owned **borrow-root** | owns the fetched bytes; **copy-out default** (CLAUDE.md §6.4), keep-alive deferred |
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

  - `SafeConsumerHandle : SafeHandle` — `ownsHandle: true`, `IsInvalid => handle
    == IntPtr.Zero`. The release path is **not** a bare destroy: `Consumer_destroy`
    is **fire-and-forget** — `shutdown_background` **cancels** in-flight async ops
    (their callbacks never fire) and it does **not** join the bg task (the graceful
    join is `Consumer_close` / `await_join`, not destroy). So `Dispose` must
    **drain/wakeup the in-flight op → `Consumer_close` (joins the bg task) →
    `Consumer_destroy`**; a bare destroy hangs the `Task` and leaks the `GCHandle`
    (§B7). Guard use-after-dispose with `ObjectDisposedException`.
  - **Transient error handle:** read the accessors (message before free), then
    `_destroy` in a `finally`; the managed `KafkaException` holds copied values,
    never the handle (§B5). `_destroy` is null-safe.
  - **Category 3 — owned result / container.** On the **caller's** thread (not the
    dispatcher), after a query or poll: read/iterate, then `_destroy` the root
    exactly once. A **container is a borrow-root** — its elements and any
    key/value/topic/string bytes borrow into it (§B3, §B4, CLAUDE.md §6.4), so it
    must outlive every borrow taken from it. **Default: copy-out** — copy each
    element/byte into an owned managed type, then `_destroy`. Metadata collections
    are always copy-out (small), and typed deserialization reads a transient span →
    owned `T` (copy-out too). **Keep-alive** (hold the root, expose zero-copy views,
    `_destroy` at `Dispose`) is a **deferred** option for the raw-byte surface only
    — see CLAUDE.md §6.4.
  - **Category 4 — borrowed view.** `ConsumerRecord_t`, `Node_t`, `_get` elements,
    borrowed strings have **no `_destroy`** — never free them, and never use them
    after their owning container (3) is destroyed. Represent as a transient cursor
    over the parent; don't let it escape the parent's lifetime.

**Why:** `SafeHandle` is the robust form of "call `_destroy` exactly once," even
through exceptions; `IsInvalid == zero` matches our null-safe destroy (this is
confluent-kafka-dotnet's `SafeHandleZeroIsInvalid` pattern). A per-message
`SafeHandle` would allocate a finalizable object per record, so transient handles
and borrowed views are not wrapped. The borrow-root discipline (Category 3
outlives its Category 4 borrows) is what makes the receive-path zero-copy contract
(§B4, CLAUDE.md §6.4) safe. `Consumer_destroy` being fire-and-forget is why
teardown routes through `Consumer_close` first — otherwise the in-flight op's
callback is cancelled and its `Task` never completes.

**Anti-patterns:**

  - Freeing a **borrowed** view (Category 4: `ConsumerRecord`, `Node`, a `_get`
    element, a borrowed string) — double-free / UAF; only the owning container is
    freed.
  - Destroying a **borrow-root** (`ConsumerRecords_t`, a list/map) while a borrowed
    element or byte slice from it is still in use (CLAUDE.md §6.4) — use-after-free.
  - Leaking an **owned** result (forgetting `_destroy` after marshalling a query
    map/list), or freeing it twice.
  - A bare `Consumer_destroy` without the drain → `Consumer_close` first — hangs
    the `Task`, leaks the `GCHandle`.

**Tests required:**

  - Poll a batch, read records, then dispose — no leak; a borrowed key/value/topic
    used after the batch is gone is prevented (copy-out) or kept valid by the
    wrapper (keep-alive), per CLAUDE.md §6.4.
  - Each query API (`committed`/`assignment`/`partitions_for`/…) frees its owned
    result exactly once after marshalling; borrowed elements are never freed.
  - Create/close many consumers — no leak; `Dispose` joins the bg task
    (`Consumer_close`) before `Consumer_destroy`.
  - Double-`Dispose` is safe; a call after `Dispose` throws
    `ObjectDisposedException`.

---

## B3 String marshalling (UTF-8)

**Decision:** Every boundary string is UTF-8, marshalled by hand (the floor lacks
`LPUTF8Str` / `Marshal.PtrToStringUTF8`, §0.1): input via `Utf8Marshal.Pin` (encode + NUL
+ pin); output via `Utf8Marshal.PtrToString` in **two forms** — a NUL-terminated
callee-owned `const char*` (NUL-scan + `GetString`), or a **length-delimited**
`const char* + int32_t out_len` that **borrows into the fetch batch** (use the
length, **never** NUL-scan).

| Direction | Sites | Helper |
|---|---|---|
| In | topic, config key/value, `subscribe`/`seek` args | `Utf8Marshal.Pin` |
| Out — NUL-terminated, valid until the value's own `_destroy` | `KafkaError_message`, `ConsumerGroupMetadata_group_id` / `_member_id`, other getters | `Utf8Marshal.PtrToString(ptr)` — NUL-scan |
| Out — length-delimited, borrowed from the batch, valid until `ConsumerRecords_destroy` | `ConsumerRecord_topic` / `_header_key`, `Node_host` / `_rack` | `Utf8Marshal.PtrToString(ptr, len)` — use `out_len`, **no scan** |

**All output pointers are borrowed** — .NET copies (`GetString`) before the owning
handle is freed; it never owns the raw pointer. The two output rows differ only in
(1) termination (NUL-scan vs `out_len`) and (2) which handle bounds the lifetime
(the value's own `_destroy` vs `ConsumerRecords_destroy`).

**Rule:**

  - Input: `Encoding.UTF8.GetBytes` → `new byte[len+1]` (trailing zero = NUL) →
    pin → pass `AddrOfPinnedObject`; unpin in `finally`. Marshalling copies (an
    encoding conversion) — fine for small topic/config, and **not** the zero-copy
    path (§B4).
  - Output — two forms, both copy into a managed `string` (needs `unsafe`; a
    `#if NET6_0_OR_GREATER` span fast path is an internal optimization):
    - **NUL-terminated** callee-owned `const char*` (no `out_len`) →
      `Utf8Marshal.PtrToString(ptr)`, scan to NUL. Valid until the value's `_destroy`.
    - **Length-delimited** `const char* + int32_t out_len` (the receive path) →
      `Utf8Marshal.PtrToString(ptr, out_len)` using the length — **never NUL-scan**: the
      slice borrows into the batch with no terminator, so a scan over-reads into
      the next field. Valid until `ConsumerRecords_destroy` (CLAUDE.md §6.4).
    In both cases **copy before free** — the pointer dies with the handle; never
    store the raw pointer.
  - Never `[MarshalAs(LPStr)]` (ANSI) or `LPWStr` (UTF-16); never `LPUTF8Str` /
    `Marshal.PtrToStringUTF8` (absent on the floor).

**Why:** UTF-8 is the contract both ways, and the core reads input with
`to_string_lossy` — invalid bytes are silently *replaced*, not rejected, so an
`LPStr` mistake corrupts non-ASCII topics quietly (and hides in ASCII-only
tests). Hand-rolled helpers are the same reason confluent-kafka-dotnet ships
`StringAsPinnedUTF8` + `PtrToStringUTF8`. The receive-path strings
(`ConsumerRecord_topic`, header keys, `Node` host/rack) return a `&str` **slice
into the fetch batch** (`str::as_ptr` + `out_len`, no terminator) and so take the
length form and must be copied out before `ConsumerRecords_destroy` (CLAUDE.md
§5.4 / consumer-threading §27); the other getters cache an owned `CString`, so
they take the NUL-terminated form.

**Anti-patterns:**

  - `LPStr` / `LPWStr` for any string; a `const char*` return marshalled as
    `string`.
  - **NUL-scanning a length-delimited slice** (`ConsumerRecord_topic` etc.) —
    over-reads past the batch slice (garbage / AV); use `out_len`.
  - Reading an output pointer after its handle (or the batch) is gone.
  - Assuming ASCII (works until a non-ASCII topic corrupts silently).

**Tests required:**

  - A non-ASCII value round-trips through a config key/value and an error message
    (NUL-terminated form).
  - A `ConsumerRecord.topic` / header key with a non-ASCII, non-NUL-terminated
    value round-trips via `out_len` (not a scan); a multi-byte char at the slice
    boundary marshals correctly.
  - `Utf8Marshal.PtrToString(IntPtr.Zero)` → `null`.

---

## B4 Zero-copy & buffer lifetime — receive path

**Decision:** The consumer's key/value/topic bytes originate in the **native**
batch buffer (owned by `ConsumerRecords_t`), and `ConsumerRecord_key` / `_value` /
`_topic` hand back `(ptr, len)` **borrowing** into it (consumer-threading §27,
§B3). **No pinning** — native memory isn't GC-managed. Raw bytes are **copy-out by
default**.

**Rule:**

  - **No pinning.** Native memory isn't GC-managed — nothing moves, so nothing to
    pin.
  - **The hazard is native-frees:** a managed view over the batch is a
    use-after-free the instant `ConsumerRecords_destroy` runs. The fix is
    **lifetime binding** — copy before destroy, or keep the batch alive. Ownership
    rules, anti-patterns, and tests live in CLAUDE.md §6.4, §B2 (Category 3/4), and
    §B3 (borrowed strings).
  - **"Buffer lifetime"** here is the *native* buffer's validity window
    (batch-scoped, until `_destroy`).

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
deferred (CLAUDE.md §6.4).

**Anti-patterns:**

  - A stored `ReadOnlyMemory<byte>` over native batch memory (use-after-`Dispose`);
    eager copies where a `ReadOnlySpan<byte>` deserialize read would do.
  - Reading a borrowed key/value/topic slice after its batch is destroyed.

**Tests required:**

  - Poll a batch, read a raw `Value`, dispose the batch — the copied bytes remain
    valid (copy-out) / are kept valid by the wrapper (keep-alive), per CLAUDE.md §6.4.
  - **Allocation budget** (DoD §10, consumer-threading §27): per-record receive
    adds only the user-deserializer's `T` allocation — no allocation attributable
    to topic name, key/value on the buffer, or batch traversal.

---

## B5 Error model (operational vs. precondition)

**Decision:** Two surfaces. **Core (operational) errors** cross as a
`kafka_common_KafkaError_t` handle → one flat `KafkaException` (code + retriable +
fatal + message), mirroring the Python sibling. **Precondition errors** (bad
argument / state) are validated in the binding *before* the FFI call and raise
standard .NET exceptions — never `KafkaException`. The consumer adds two more
shapes: **wakeup** and **concurrent use**.

| `KafkaError_*` accessor | → C# |
|---|---|
| `_code` (i32) | `int Code` |
| `_message` (UTF-8, handle-owned) | `Message` via `Utf8Marshal.PtrToString` (§B3) |
| `_is_retriable` / `_is_fatal` | `IsRetriable` / `IsFatal` |
| `_destroy` | free after reading |

**Rule:**

  - **Operational:** the error handle arrives via the `out_error` param (fns that
    also return a value — `poll`, `_new`) **or as the return value** (fns that are
    `void` in Java — `assign` / `subscribe` / `seek` / `unsubscribe`); **null =
    success** in both, non-null = error. `KafkaException.FromHandle` reads the
    accessors (message **before** free), then `_destroy` in a `finally` — freed
    exactly once even if construction throws (§B2, §B3); the exception holds copied
    values, not the handle. Sync failures `throw`; async op failures fault the
    `Task` (§B7) — same `FromHandle`. Keep it **one flat `KafkaException` for now**
    (the ABI exposes only code/retriable/fatal); typed subclasses can be added
    under it later, non-breakingly.
  - **Precondition:** validate before any pin/marshal/P/Invoke and throw
    `ArgumentNullException` (null topic/partition/config),
    `ArgumentOutOfRangeException` (a negative timeout), or `ObjectDisposedException`
    / `InvalidOperationException` (closed consumer). **Mandatory**, not optional:
    the ABI doesn't validate preconditions (CLAUDE.md §3) and some functions
    `assert!`/panic on violation — a panic across FFI is UB.
  - **`wakeup()`** → the interrupted `poll` / `commit` / … surfaces a flat
    `KafkaException` with a **Wakeup** code (Java `WakeupException` semantics —
    raised **once**, then the flag clears and the op works again;
    consumer-threading §11). **Not** a subclass: the Python sibling keeps it flat
    (`pytest.raises(KafkaError)`, message `"woke"`), so do we. Distinct from
    **`CancellationToken`** cancellation → **`OperationCanceledException`** (the
    .NET-native cancel, CLAUDE.md §3 — the analog of Python re-raising
    `CancelledError`).
  - **Concurrent use** (the consumer is single-owner / one-operation-in-flight;
    serialized by the **Rust core's** guard, not a managed one — M3/P2) splits by
    method, and the split is enforced **core-side**, not by a managed pre-check: a
    concurrent **async op** (`PollAsync` / `CommitAsync` / `SubscribeAsync` /
    `SeekAsync`) is rejected by the core **inline** (it fires the completion callback
    on the caller thread with a `ConcurrentModification` error), which the bridge
    surfaces as a **faulted `Task`** carrying a **`KafkaException`** — *not* a
    managed synchronous throw. A concurrent sync **state read** (`Assignment` /
    `Subscription` / `Paused` / `GroupMetadata` / `GroupId`) → the core returns a
    **null** handle, which the getter maps to **`InvalidOperationException`** ("not
    safe for multi-threaded access"). Mirrors Python exactly (`RuntimeError` from
    `_concurrent_error()` for state reads, `KafkaError`/ConcurrentModification for
    ops).

**Why:** a flat `KafkaException` matches what the ABI exposes and the Python
sibling. Preconditions are a separate surface because they are programmer errors,
not Kafka outcomes — Java raises `IllegalArgument`/`IllegalState`, Python
`ValueError`/`TypeError`, all before the native call; ours must too, *and must*
because the ABI would otherwise panic. The wakeup and concurrent shapes mirror the
Python sibling exactly (flat `KafkaError` "woke"; the `RuntimeError`/`KafkaError`
concurrent split). The concurrent split is **delivered by the core**, not a managed
guard: the async-op `KafkaException` arrives through the faulted `Task` (the core
fires the callback inline with `ConcurrentModification`), and the state-read
`InvalidOperationException` is thrown from the getter's null-handle path — there is
no managed access guard to reject anything (M3/P2 removed it; the observable
contract is unchanged, only its mechanism is now the core's).

**Note — flat now, typed later.** The flat `KafkaException` is the *current*
choice; a Java-style typed hierarchy can be added later **non-breakingly**
(subclasses derive from `KafkaException`, so `catch (KafkaException)` still
works). If we do, `KafkaException.FromHandle` becomes a `Code` → subclass factory
and specific cases move from "flat + code" to a type — e.g. the **Wakeup** case
would become a `WakeupException`. The two-surface model, the handle lifecycle, and
the precondition exceptions are unchanged.

**Anti-patterns:**

  - Not checking the out-param / return value; leaking the error handle or reading
    `_message` after `_destroy`; a `FromHandle` that can throw before its `_destroy`.
  - A per-code hierarchy now (over-engineering vs the Python sibling); throwing
    `KafkaException` for a programmer error; validating after the P/Invoke.
  - Making `wakeup()` a distinct exception subclass now (the ABI + Python keep it
    flat); conflating a `wakeup()` (`KafkaException`/Wakeup) with a
    `CancellationToken` cancel (`OperationCanceledException`).
  - Throwing `KafkaException` for a concurrent **state read** (it's
    `InvalidOperationException`), or `InvalidOperationException` for a concurrent
    **async op** (it's `KafkaException`).

**Tests required:**

  - A sync failure throws `KafkaException` with the right code/message/flags; an
    async failure faults the `Task` (via mock `set_poll_error`); the handle is
    freed exactly once; a non-ASCII message round-trips.
  - Null topic/partition → `ArgumentNullException`; post-`Dispose` →
    `ObjectDisposedException`; each before any native call.
  - `wakeup()` → the next `poll`/`commit` throws `KafkaException` (Wakeup)
    **once**, then the consumer works again; a `CancellationToken` cancel →
    `OperationCanceledException`.
  - A concurrent sync state read → `InvalidOperationException`; a concurrent async
    op → `KafkaException`.

---

## B6 Callback & delegate marshalling — completion callbacks

**Decision:** The consumer's **~8 completion callbacks** — `Consumer_poll` / `op` /
`position` / `committed` / `offsets_for_times` / `long_offsets` / `partitions_for`
/ `list_topics` — are the **primary** mechanism for every async op. Marshal each
as a kept-alive `[UnmanagedFunctionPointer(Cdecl)]` delegate (classic — no function
pointers on the floor), keep the body no-throw, and pass context (the
`TaskCompletionSource`) via a `GCHandle` in `user_data`.

The shape is **not uniform** — always `(…, KafkaError*, void* user_data)` with a
non-null `KafkaError*` = failure, but the *result* slot varies: an **owned handle**
for `poll` / `committed` / `offsets_for_times` / `long_offsets` / `partitions_for`
/ `list_topics` (`(handle*, error*, ud)`); a **scalar** for `position`
(`(int64_t, error*, ud)`); and **none** for `op`, the void-in-Java ops
(`(error*, ud)`).

**Rule:**

  - Named delegate type, `[UnmanagedFunctionPointer(CallingConvention.Cdecl)]`,
    blittable params. Keep the delegate rooted (a `static readonly` field or the
    per-op `GCHandle`) so the GC can't collect it while native holds the thunk.
  - **Foreign thread → no-throw is mandatory.** The callback fires on the native
    callback-dispatcher thread (§B7), not the caller — no caller frame to catch, so
    an escaping exception is a crash/UB. `try/catch` all, surface via the TCS.
  - **Per-op keep-alive.** The delegate + the `GCHandle` (over the
    `TaskCompletionSource`) must stay alive from **submit until the callback
    fires** (the whole op, not a synchronous call), freed **exactly once** by the
    callback — including the inline guard-rejection error path.
  - **The callback owns any handle it gets.** For the **owned-handle** forms it
    consumes the result — marshal it (copy-out CLAUDE.md §6.4) then `_destroy`
    (§B2 Category 3); `position`'s scalar needs no free; `op` has no result. On
    failure it builds the exception (`FromHandle`, §B5) and frees the `KafkaError`.
    Then it completes the TCS.

The async *flow* (submit → dispatcher → `SetResult` with
`RunContinuationsAsynchronously`; one-op-in-flight; `Dispose`) is **§B7** — this
section is only the callback *marshalling*.

**Why:** the floor (netstandard2.0/net462) has no function pointers or
`[UnmanagedCallersOnly]`, so a kept-alive Cdecl delegate is the only portable
mechanism — the pattern confluent-kafka-dotnet uses. The GC sees only managed
refs, not the native thunk (→ keep-alive); `user_data` is C's only per-call
context channel (→ `GCHandle`). Because the callback runs on the core's **foreign**
dispatcher thread, no-throw is not optional (there is no caller frame to catch)
and the keep-alive spans the whole op (submit→fire), not a synchronous call.

**Anti-patterns:**

  - A per-op delegate / `GCHandle` not rooted for the whole submit→fire window
    (collected mid-op → crash).
  - An exception escaping into the dispatcher thread (no caller to catch → UB).
  - Not freeing the owned result/error + `GCHandle` on some path (esp. the inline
    guard-rejection).
  - Reading a borrowed `topic`/bytes pointer after its batch is destroyed (§B3/§B4).

**Tests required:**

  - Each callback delivers the right result/error and frees the owned handle +
    `GCHandle` exactly once.
  - An exception thrown in the callback is caught (no crash, no unwind into native)
    and faults the `Task`.
  - Aggressive GC during an in-flight op doesn't collect the delegate (keep-alive
    across submit→fire).

---

## B7 Async / completion → `TaskCompletionSource` (push)

**Decision:** The consumer uses the ABI's *push* surface (settled — no pump):
every async op (`Consumer_poll_async` / `commit_async` / `position_async` / …)
takes a completion callback (§B6) that fires when the op resolves, so the callback
*is* the bridge. Single-owner / one-operation-in-flight (nothing to batch),
serialized by the **Rust core's** guard — **no managed guard** (M3/P2). Bridges to
`Task<T>` via a `TaskCompletionSource` built with `RunContinuationsAsynchronously`.

```
Caller thread                       Core: runtime worker ──▶ dispatcher thread (1/consumer)
─────────────                       ────────────────────────────────────────────────────
PollAsync():                        worker task: poll(timeout).await   ← runs the op
  tcs = new TaskCompletionSource                 build (records | error)
  ud  = GCHandle.Alloc(tcs)  (§B6)                enqueue completion ──┐
  Consumer_poll_async(…, cb, ud) ─► (core guard serializes ops)       ▼
  return tcs.Task                   dispatcher:
  … await tcs.Task                    cb(records | error, ud): marshal + free handles (§B2)
     ◄── continuation on pool ─────   tcs.SetResult / SetException  (RunContinuationsAsync)
                                    no pump · no managed guard · one op in flight
```

**Rule:**

  - `PollAsync` (etc.) makes a `TaskCompletionSource`, `GCHandle.Alloc`s it as
    `user_data` (§B6), submits the `*_async` op with a kept-alive Cdecl callback,
    and returns `tcs.Task` immediately — no blocked thread.
  - The callback fires on a **dedicated callback-dispatcher thread** — the core
    runs the op on a runtime worker, then hands the completed op over a channel to
    that one thread, so callbacks are **serialized** on a single foreign thread
    (the core's concurrent-rejection fires inline on the caller; shutdown, inline on
    the worker). It marshals the result (copy-out CLAUDE.md §6.4, or an error via
    `FromHandle` §B5), frees the handles it owns (§B2 Category 3), and completes the
    TCS — with **`RunContinuationsAsynchronously`** (essential: the continuation must
    not run on that dispatcher thread), exactly once, on every path (incl. the
    inline core-rejection error). The completion callback is the **sole owner** of
    the per-op `GCHandle` free (the only other path, `AbandonBeforeSubmit`, runs
    only when the submitting P/Invoke threw so native never ran).
  - **One operation in flight** per consumer, enforced by the **core's** access
    guard (not a managed mirror) → at most one pending `(callback, TCS)` at a time,
    no batching. The core releases its guard before firing the callback, so an
    `await`-then-resubmit from the continuation is safe — it won't hit the one-op
    rejection. A concurrent op submitted while one is in flight is rejected by the
    core inline and surfaces as a **faulted `Task`** (`ConcurrentModification`, §B5).
  - **Cancellation** = `CancellationToken` → `wakeup()` (aborts the in-flight op)
    → the callback fires with a Wakeup error → the `Task` cancels/faults
    (best-effort; §B5, consumer-threading §11).
  - **`Dispose` / `DisposeAsync` (single-owner teardown):** close gracefully then
    destroy, with **no separate-op drain**. `DisposeAsync` → **`Consumer_close_async`**
    (graceful — joins the bg task via `await_join`) → `Consumer_destroy`;
    `Dispose` → **`Consumer_close_with_timeout`** → `Consumer_destroy`. Under
    single-owner the **awaiter of an op is its disposer**, so there is no concurrent
    submitter to drain — teardown never wakes+awaits a *separately-submitted* op.
    `Consumer_destroy` is fire-and-forget (it **cancels** any remaining in-flight op
    and does **not** join, §B2), so a bare `Consumer_destroy` on an **unawaited**
    op still strands the `Task` + leaks the `GCHandle` — now the **accepted
    single-owner residual** for a misuse case (Python parity: `close()` drains its
    *own* awaited op, then bare `_destroy`). `DisposeAsync` on the awaiting task is
    the clean, leak-free path.

**Why:** the consumer op has nothing to batch (one op in flight) and the ABI
already pushes a completion, so a pump would be pure overhead — the callback is
the bridge. `RunContinuationsAsynchronously` is essential because the callback
runs on the core's foreign dispatcher thread; without it the awaiter's
continuation would run there and stall the core (or deadlock if it calls back in).
Serialization is the **core's** job (single-owner) — M3/P2 removed the extra
managed guard/in-flight tracking to match the in-repo Python sibling, which also
eliminated the M3/P1 op-submit-vs-teardown publish window (no tracking fields → no
window).

**Anti-patterns:**

  - A TCS without `RunContinuationsAsynchronously` — the awaiter's continuation
    runs on the core's dispatcher/worker thread → stalls the core (or deadlocks if
    the continuation calls back into the consumer).
  - A **managed access guard / in-flight tracking** mirroring the core (removed in
    M3/P2) — the core serializes; a concurrent op faults the `Task`, a concurrent
    state read throws `InvalidOperationException` (§B5).
  - Wrapping the *sync* variants (`Consumer_poll` + `block_on`) in a `Task.Run`
    per op — sync-over-async; the push ABI makes it needless.
  - Letting the callback throw (unwinds into native, §B6); completing the TCS
    twice; freeing the per-op `GCHandle` from **anywhere but** the callback (the
    sole owner) / `AbandonBeforeSubmit` (native never ran) — a teardown-side free is
    a use-after-free against a straggler callback.
  - A teardown that wakes+awaits a *separately-submitted* op (there is no concurrent
    submitter under single-owner) or that re-adds the M3/P1 `Dispose`-side
    `FaultTaskOnly` machinery (the unawaited-op strand+leak is an accepted residual).

**Tests required:**

  - `PollAsync` resolves with records / faults with `KafkaException` (mock
    `set_poll_error`); the result/error handle + `GCHandle` are freed exactly once.
  - `wakeup()` during an in-flight `poll` cancels/faults the `Task` **once**, then
    the consumer is reusable (§B5); a `CancellationToken` cancel →
    `OperationCanceledException`.
  - A concurrent async op → a faulted `Task` (`KafkaException` /
    ConcurrentModification); a concurrent sync state read →
    `InvalidOperationException` (§B5).
  - `Dispose` / `DisposeAsync` return without hanging even with an (unawaited) op in
    flight — the teardown-returns regression (`close_(with_timeout|async)` →
    `destroy`, no separate-op drain).
