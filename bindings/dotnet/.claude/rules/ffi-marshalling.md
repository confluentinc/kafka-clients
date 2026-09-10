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
    callbacks — the `RecordMetadata_copy` and `*_async` ABI forms **plus the
    managed-only delivery callback that never crosses the ABI** (§A6), async
    completion (§A7).
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

**Tests required (both clients):** `Dispose` returns without hanging before
destroying the handle — the producer joins the pump (§A1/§A7); the consumer closes
gracefully, with **no separate-op drain** (single-owner: the awaiter of an op is its
disposer, §B1/§B7). ⚠ For the consumer's teardown routes see **§B2's authoritative
`Consumer_destroy` path list** — do not restate them here.

---

# Part A · Producer

## A1 Thread model (producer)

**Decision:** Per real `KafkaProducer`, the core owns a multi-thread tokio
runtime + one Sender task over a single async Selector; the .NET side adds at most
**two** background threads — one completion pump (§A7) and, on the **async path
only**, one send-batch thread (§A7's send-batching tweak, taken in M11/P3.1) — never
a per-send thread and never a poll loop.

```
   .NET (managed)                    │ C ABI │       Rust core (native, per producer)
   ─────────────                     │       │       ─────────────────────────────────
   caller thread(s):                 │       │   tokio multi-thread runtime (worker POOL)
     SYNC  Send → Producer_send ───│──────►│     RecordAccumulator (enqueue, returns fast)
     ASYNC Send → pin + append to  │       │   Sender task (spawned once in _new):
             the send accumulator   │       │     NetworkClient + ONE async Selector
   send-batch thread (1 bg, async): │       │       ↕ multiplexes ALL brokers (event-driven)
     _send_batch(records[]) ───────│──────►│   (created in KafkaProducer_new, dropped on _destroy)
     unpin, hand futures to the pump │     │
   completion pump (1 bg thread):    │       │
     get_all(futures) ─block_on─────►│──────►│
     ◄── per-message metadata/err ───│◄──────│
   Dispose: drain accumulator → join pump → flush/close
```

⚠ **The "at most two" is a cap on *kinds*, not a licence for more.** The batch thread
is one thread for **all** sends on a producer, it polls nothing, and the sync path
starts neither thread. A per-send thread and a poll loop remain forbidden, verbatim.

**Rule:**

  - Native side (per real `KafkaProducer`): a multi-thread tokio runtime + one
    spawned Sender task doing all I/O over a **single async Selector** (all
    brokers multiplexed — NOT thread-per-broker), created in `KafkaProducer_new`,
    torn down by `Producer_destroy`.
  - .NET side: caller thread(s) + **at most two** background threads — one completion
    pump (§A7) and, on the **async path only**, one send-batch thread; **no per-send
    threads**. **No poll loop** — the core self-drives, so a stalled pump
    delays result delivery but not sending.
    ⚠ **Amended in M11/P3.1** — this read "exactly one completion pump". The
    prohibitions it was really about are unchanged and still hold: the batch thread is
    one thread for all of a producer's sends (not per-send), and it does not poll. The
    **sync** path starts neither thread, so a sync-only producer still spins nothing.
  - **Submission order is call order.** The submission path must hand records to the
    core in the order a caller called `Send`. Java documents ordering as preserved in
    the default configuration
    (`kafka/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java:274`
    — *"if retries are disabled or if `enable.idempotence` is set to true, ordering
    will be preserved"*), and a binding-side reorder happens **before** the core sees
    the records, so **no core-side or broker-side setting can restore it** —
    idempotence protects against retry-induced reordering, not against being handed
    the records in the wrong order. A deferred / accumulating submission path must
    therefore not let a send that finds capacity overtake one still waiting for it.
    Do **not** rest this on `SemaphoreSlim` fairness: the .NET documentation
    guarantees **no ordering** in which blocked waiters enter the semaphore, so an
    ordering claim built on it is unfounded. The property is claimed **per caller**
    (a caller's own successive sends), which is all Java's guarantee is about;
    concurrent callers are not ordered against each other, and the anchor does not
    order them either.
    ⚠ **Added in M11/P3.2** — no rule stated this, which is a large part of why the
    defect shipped: the accumulator introduced in M11/P3.1 took the backpressure
    permit *before* appending and deferred the append when it could not get one, so a
    later send that found a permit appended ahead of a parked one. The fix that
    satisfies this rule is a routing count (the inline path is refused while anything
    is queued ahead) plus a documented-FIFO submission queue drained by a **single**
    appender.
    ⚠ **That single submitter is NOT a third background thread**, so the "at most
    two" cap above is unaffected: it is a task-based loop, started by whichever caller
    queued the first submission, with **at most one** in flight per producer, and it
    does not poll — it awaits a permit and appends. A *dedicated thread* for it would
    breach the cap and is not the sanctioned shape.
  - FFI is callable from any .NET thread; the core serializes via the producer's
    internal `Mutex`, so concurrent `Send` is safe — don't add your own lock.
  - `block_on` parks only the *calling* .NET thread; the Sender keeps running on
    the runtime's worker threads, so a blocked pump can't deadlock it.
  - The one **ABI** callback (`RecordMetadata_copy`) fires synchronously on the
    caller's (pump) thread — not a foreign thread (§A6). ⚠ It is no longer the
    only callback the pump thread runs: the **managed-only delivery callback**
    (§A6 form C, M14/P1) also runs there, by design — it is .NET's analogue of
    Java's "background I/O thread" (`Callback.java:20-21`). It never crosses the
    ABI, so nothing in §A6's keep-alive / no-unwind machinery applies to it.
  - **Consequence for the pump thread:** it now runs one bounded piece of user
    code per completion, so a slow delivery callback delays every other send's
    completion (documented on the public interface), and the pump's own
    correctness rests on that invocation being a **total no-throw boundary**
    (§A6 form C). The awaiter's continuation still stays off the pump via
    `RunContinuationsAsynchronously` (§A7).

**Why:** Java's single-Selector NIO model on tokio (§0.3) — a fixed native thread
count, no poll loop, and a multi-thread runtime that makes `block_on` from the
pump deadlock-free (the Sender runs on other worker threads).

**Anti-patterns:**

  - A binding-side lock around producer sends — the producer's `Mutex` already
    serializes concurrent `Send` (don't double-lock).
  - A `callbackTask`-style poll-loop thread; thread-per-broker assumptions
    (shared, §0.3).
  - A submission path that appends inline whenever capacity happens to be free, with
    an earlier send still waiting for it — the ordering defect above. Equally: an
    ordering argument that rests on `SemaphoreSlim` waiter order, or on many
    independent per-send continuations being released "in order" by one
    multi-permit `Release`.
  - A **dedicated thread** for the submission queue's appender (the cap is two);
    conversely, more than one appender draining that queue, which reintroduces the
    race the queue exists to remove.

**Tests required:**

  - Concurrent `Send` from many threads is correct (the `Mutex` holds).
  - `Dispose` drains before destroying the handle — joins the pump (Option A, §A7).
  - *(Option A only)* a long-blocked pump doesn't stop new sends being enqueued.
  - **The order records reach `send_batch` equals call order, across a saturated
    bound.** Two parts, because the raw interleaving is a race: a *deterministic*
    half (with a submission queued, the inline path is refused and the next send
    lands behind it — nothing appended, nothing pinned) and a *stress* half (a
    same-thread burst that saturates the bound, with the observed order equal to the
    call order every iteration). The stress half must be shown to **fail** without
    the routing rule; the deterministic half is the one that cannot flake.
  - **A drain / `Flush` includes a send still queued for capacity.** Once a
    submission queue sits upstream of the accumulator's chain, "empty and idle" has
    **two** stages, and a predicate that tests only the chain lets `Flush` return
    with records the caller's `Send` already returned for — re-opening the
    Java-faithfulness gap the accumulator drain exists to close.
  - **Teardown settles every queued submission exactly once**, with nothing left
    holding an unsettled `TaskCompletionSource`, and **nothing pinned while queued**
    (the pin belongs after the permit, §A4).
  - **A queued submission whose token fires before it is appended cancels with the
    caller's token and is not sent** — asserted on the core's record count, not only
    on the awaiter's state.

---

## A2 Handle ownership & lifecycle (`SafeHandle`)

**Decision:** Three ownership categories:

1. **Client / config** (producer, properties) → a `SafeHandle` that frees exactly
   once (the blocking destroy joins the background task first).
2. **Flat transient** (future / metadata / error) → read-and-free promptly,
   **not** wrapped (a finalizable object per op is hot-path waste).
3. **Owned result / container (borrow-root)** (the metric map, the partition-info
   list) → the caller reads it, copies every value into owned managed types, then
   `_destroy`s the root exactly once; the strings and elements it hands back
   **borrow into it**, so it must outlive every borrow taken from it.

| Handle | Category | Created by | Freed by |
|---|---|---|---|
| `Producer_t` | 1 — client (`SafeHandle`) | `KafkaProducer_new` / `MockProducer_new` | `ReleaseHandle → Producer_destroy` (via `Dispose`) |
| `ProducerProperties_t` | 1 — config (`SafeHandle`, short) | `ProducerProperties_new` / `_from_configs` | the binding, after `KafkaProducer_new` |
| `FutureRecordMetadata_t` | 2 — flat transient | `Producer_send` / `_send_batch` | the pump: `_destroy_all` (`get_all` doesn't consume) |
| `RecordMetadata_t` | 2 — flat transient | `_get` / `_get_all` | the pump: `RecordMetadata_copy` (extract+free) or `_destroy` |
| `KafkaError_t` | 2 — flat transient | any `out_error` slot | the reader: read accessors, then `_destroy` |
| `kafka_producer_MetricMap_t` | 3 — owned result (borrow-root) | `Producer_metrics` | the reader: copy every entry out, then `kafka_producer_MetricMap_destroy` |
| `kafka_consumer_PartitionInfoList_t` — **owned here** (a consumer type by name; the handle/accessors are shared with the consumer FFI) | 3 — owned result (borrow-root) | `Producer_partitions_for` (`*out_list`) · `_partitions_for_async` (callback arg) | the reader: copy the tree out, then `kafka_consumer_PartitionInfoList_destroy` |

**Note — classify by the accessor, not the type.** The returning function's
const-ness decides, not the type name:

  - non-`const` return (or an `out` slot / a callback argument) with a `_destroy`
    of its own → **owned**; free it exactly once after reading (Category 1/3).
  - `const *` return → **borrowed**; never free it — it dies with its owning root.
    Every `kafka_producer_MetricMap_get_*` accessor is `const`, so the name /
    group / description / tag / string-value pointers it returns are borrowed
    from the map (§A3).
  - the same type can be owned in one call and borrowed in another —
    `kafka_consumer_PartitionInfoList_t` is **owned** when the producer's
    `partitions_for` hands it back (the producer must free it), yet is a borrowed
    element elsewhere in the consumer FFI. Read the signature, not the prefix.

**Rule:**

  - `SafeProducerHandle : SafeHandle` — `ownsHandle: true`, `IsInvalid => handle
    == IntPtr.Zero`, `ReleaseHandle` calls `Producer_destroy`. Runtime frees
    exactly once, even on exceptions.
  - Transient handles: free in a `finally` on the pump; the managed
    `RecordMetadata`/`KafkaException` hold **copied values**, never the handle
    (§A7). After `get_all`, each index has exactly one non-null of {metadata,
    error} — free that one, plus the future via `_destroy_all`. All `_destroy`
    are null-safe; `RecordMetadata_copy` frees its own handle (don't double-free).
  - **Category 3 — owned result / container (borrow-root).** After a synchronous
    state read (`Producer_metrics`, `Producer_partitions_for`), or inside the
    completion callback of an `_async` form (`_partitions_for_async` hands the
    callback the owned list on the producer's dispatcher thread — §A6/§A7):
    read/iterate, **copy every value out** into owned managed types, then
    `_destroy` the root **exactly once**, in a `finally` so a throw mid-read
    cannot leak it. A container is a **borrow-root** — the `const char*` slices
    (§A3) and every element it exposes borrow into it and die with it, so nothing
    native-backed may survive the `_destroy`. **Copy-out is the only mode here**:
    these are small metadata snapshots, and unlike the consumer's fetch batch
    there is no raw-byte surface that would pay for keep-alive's lifetime
    coupling (CLAUDE.md §6.4). Not a `SafeHandle` — the root is read and freed
    within one call, so a finalizable wrapper buys nothing.
    ⚠ **Do NOT import the consumer's concurrent-access null** (§B2/§B5) when
    reading these. `Consumer_metrics` documents a null return **on a
    concurrent-access rejection**; `kafka_producer_Producer_metrics` documents no
    such null — the producer serializes through its own `Mutex` (§A1) and blocks
    instead. So a producer null guard is **defensive only** (unreachable while the
    handle is passed as the `SafeHandle`), and must not claim a rejection the
    producer ABI never signals, nor name the consumer type (M11/P8 decision D-5).
  - **Parent outlives children:** the producer must not be destroyed while the
    pump holds futures from it — enforce via `Dispose` ordering (stop sends → join
    pump → release handle, §A7), not `DangerousAddRef`.
  - **Ref-management convention — sync = `SafeHandle`-param (auto), async = manual
    `AddRef` (span-the-op).** For a client-handle call that needs the producer kept
    alive across the native call, the form depends on when the native use ends:
    - **Synchronous native call → pass the `SafeHandle` as the P/Invoke parameter.**
      The marshaler auto-`DangerousAddRef`s before the call and `DangerousRelease`s
      after — a **call-scoped** guard, which is exactly right for a synchronous op
      whose native use ends when the call returns (the core copies key/value during
      the call, §A4). A closed handle marshals to `ObjectDisposedException`. No
      manual `DangerousAddRef`/`GetHandle`/`Release` bracketing. `Producer_send`
      (`NativeMethods.ProducerSend(SafeProducerHandle, …)`) is the **first adopter**;
      the sync teardown `Producer_flush`/`_close` and the sync consumer ops are a
      **tracked follow-up migration** (part 5 of the M11/P3 close-record).
    - **Async callback op (`*_async`) → manual `DangerousAddRef` held submit→callback.**
      The auto marshaler releases its ref *before* the native call returns — i.e.
      **before** the completion callback fires — so it is **insufficient** for an op
      whose native use outlives the submit call. These keep the manual span-the-op
      ref (`DangerousAddRef` at submit, `DangerousRelease` in the completion's
      `FreeGcHandle`): `SubmitVoidOperation` / `SubmitOwnedHandleOperation` /
      `CloseWithCallbackInternal` / `FlushInternal`. An async op can **never** use
      the auto (SafeHandle-param) form.
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
  - Leaking an **owned result** (no `MetricMap_destroy` /
    `PartitionInfoList_destroy` after marshalling), freeing it twice, or freeing
    it anywhere but a `finally` (a throw mid-read leaks the root).
  - Retaining anything **borrowed** from a Category-3 root — a raw `const char*`,
    an element pointer — past its `_destroy`; copy out first.
  - Mapping a null `Producer_metrics` result to the **consumer's**
    concurrent-access `InvalidOperationException` message (D-5) — it states a
    contract the producer ABI does not have.

**Tests required:**

  - Create/dispose many producers — no leak/crash; send N and assert the handle
    count returns to baseline (covers `_destroy_all`).
  - Double-`Dispose` is safe; a call after `Dispose` throws
    `ObjectDisposedException`.
  - Each Category-3 result (`Producer_metrics`, `Producer_partitions_for`) is
    freed exactly once after marshalling, on the throwing path too, and the
    copied-out values stay valid after the root is destroyed.

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

  - Prefer a `fixed` block (stack-scoped, no allocation) for a single send; where
    `fixed` doesn't fit — the N buffers of `_send_batch`, all borrowed for the whole
    call — pin them explicitly and release in a `finally`. Unpin right after the call —
    **never hold a pin across the returned `Task`** (per-message pinned objects
    fragment the GC heap).
  - **The pinning primitive for key/value is `ReadOnlyMemory<byte>.Pin()` →
    `MemoryHandle`, NOT `GCHandle.Alloc(Pinned)`** (amended M11/P3.1). `GCHandle` pins
    *objects*, and a record's key/value is a `ReadOnlyMemory<byte>`, whose backing store
    may be an array, a string, native memory or a custom `MemoryManager`; `Pin()`
    handles all of them and exists on all three TFMs (`netstandard2.0` gets it from the
    already-referenced `System.Memory`). `GCHandle.Alloc(Pinned)` remains correct for
    buffers the binding *owns* and pins itself — the interned topic buffers and the
    empty-sentinel byte below.
  - **Deferred send: the pin spans `Send` → …accumulator… → `send_batch` returns.**
    This call-scoped rule was written against §A7's inline send. The producer's **async**
    path is now deferred (a send-batch thread, as in the Python binding), so a pin taken
    in `Send` is held until the batch thread's `send_batch` **returns** — and released
    there, in a `finally`, **before** the future reaches the completion pump. The
    underlying fact is unchanged and is what keeps this inside the rule rather than
    outside it: `send_batch_inner` runs `producer_send` — i.e.
    `rt.block_on(producer.send(record, None))` — per record and copies the topic with
    `to_string_lossy().into_owned()`, so the core's borrow still ends when the native
    call returns. "Unpin right after the call" and "never across the `Task`" both still
    hold; only *which* call moved. The **sync** path is unchanged and stays `fixed`.
    ⚠ **Consequence on the public surface:** a deferred send **borrows** the caller's
    buffers past `Send`'s return, so a mutation before the drain IS visible on the wire.
    Document that on the **async** surface only — the sync send has no such window, and
    telling sync users to defend against it states a constraint that does not exist.
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
    ⚠ **A STACK sentinel is correct only for a call-scoped send.** Once the send is
    deferred, that address is dead by the time the batch thread reads it — a
    use-after-free that "works" almost always and corrupts rarely, which is the worst
    failure mode. The deferred path must use **one process-wide, permanently pinned
    1-byte static** instead (M11/P3.1 §4.2). Test it as a *stability* property — the same
    address for a record's key and value, and across separate calls at different stack
    depths — because that is exactly what a stack sentinel cannot satisfy and an
    "it sent successfully" assertion cannot detect.
  - **The topic is the third buffer, and it is the one that gets missed.** While the send
    is call-scoped it is just a scoped `Utf8Marshal.Pin`; deferred, a call-scoped topic
    pointer is a use-after-free like the sentinel. Do not solve it with a per-record copy
    (that is the allocation this section exists to prevent): intern **one permanently
    pinned NUL-terminated buffer per distinct topic**, which is O(distinct topics)
    permanent pins instead of O(records) transient ones. Bound the cache and **never
    evict** — freeing a pinned buffer an in-flight record still points at is a
    use-after-free — and free the whole cache only at a point where nothing can still
    hold a pointer into it.

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

  - **Mutation-after-send**, per surface: on a **call-scoped** send, mutate the caller's
    `byte[]` right after `send` and the produced record is unchanged (proves the copy
    happened during the call). On a **deferred** send that guarantee does not exist, so
    the test asserts what must still hold — the mutation is *memory-safe* (the pin is
    what makes it so) and the send still resolves — and the visibility window is
    documented on the async surface instead.
  - **Allocation budget** (DoD §10): a large value adds no value-sized managed
    allocation. ⚠ Under a deferred send, whichever caller allocates or grows the
    accumulator's node pays for it, which is noise on a per-send measurement; take the
    **best of N matched attempts** rather than widening the budget, so a real per-send
    regression (which raises every attempt) still fails.
  - Absent vs empty key/value each produce the correct record — and, on a deferred send,
    that the empty sentinel's address is **stable across calls and stack depths**.
  - **Pin/unpin balance on every path**, including the failure paths (the append refused,
    the native call throwing, a node abandoned at teardown). A pinned `GCHandle` is a
    strong root, so a leak is observable without any pin-counting API: hand each send a
    buffer nothing else references and assert it becomes collectable.

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

## A6 Callback & delegate marshalling (`RecordMetadata_copy`; the managed-only delivery callback)

**Decision:** the producer has **three** callback forms, and only the first two
cross the ABI:

  - **Form A — the ABI's one *synchronous* callback, `RecordMetadata_copy`** (an
    optional copy convenience). Marshal it as a kept-alive
    `[UnmanagedFunctionPointer(Cdecl)]` delegate (classic — no function pointers on
    the floor), keep the body no-throw, and pass context via a `GCHandle` in
    `user_data`. Using it is optional — the per-field accessors
    (`_offset`/`_partition`/`_topic`/`_timestamp` + `_destroy`) avoid callbacks
    entirely.
  - **Form B — the async *completion* callbacks** (producer `*_async`, §A7). Same
    marshalling floor; the lifetime rules are §A7's.
  - **Form C — a *managed-only* callback that never crosses the ABI** (the
    delivery callback, `IDeliveryCallback`, M14/P1). Java's second `send`
    signature (`Future<RecordMetadata> send(ProducerRecord, Callback)`,
    `Producer.java:86`) is restored **on top of the completion path already in
    use** — the pull-pump's batched `FutureRecordMetadata_get_all` (§A7 Option A/C)
    or the sync blocking `FutureRecordMetadata_get`. Nothing new is registered with
    native, so there is **no delegate to root, no `GCHandle`, no `user_data`, no
    `user_data_destroy` hook and no new `[DllImport]`** — the whole form is Mode A.
    It is a *binding-layer* callback: the ABI has no idea it exists.

**⚠ Do not reason about form C with form A/B's rules.** The instinct trained by
Part B's three families is to look for a rooting site and a free site; form C has
neither, and inventing one (a `GCHandle` per send) would be a pure regression. The
one obligation it *does* inherit — a **total no-throw** invocation — comes from
where it runs, not from the ABI (see the form-C Rule).

Form A fires **synchronously on the caller's (pump) thread** and returns before
`RecordMetadata_copy` does; its `topic` pointer is valid **only during the call**
(the core frees the handle right after — §A3):

    void cb(int64_t offset, int32_t partition, const char* topic, int64_t timestamp, void* user_data)

**Rule (forms A and B — the ABI-crossing forms):**

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

**Rule (form C — the managed-only delivery callback):**

  - **Carry it, do not register it.** The per-send carrier is one nullable
    reference field alongside the pending send (`null` on the plain
    `Send(record)` path), holding the user callback plus whatever the failure-path
    placeholder needs — never a closure and never the public record, so the
    allocation-budgeted send path is unchanged (DoD §10).
  - **Fire it where the completion is read, on whichever thread reads it.** That
    is the pump thread for the async surface (.NET's analogue of Java's background
    I/O thread, `Callback.java:20-21`) and the *caller's* thread for the blocking
    sync surface, which has no pump. Both are legitimate; they are the same two
    threads that already free the completion's native handles.
  - **Invoke it BEFORE the awaiter is released** — before `TrySetResult` /
    `TrySetException`, and before the sync send returns or throws. Java's
    `ProducerBatch.completeFutureAndFireCallbacks` sets the future's value, fires
    the callbacks, and only then calls `produceFuture.done()`
    (`ProducerBatch.java:303-323`).
  - **Invoke it unconditionally** — never gated on `TrySet*`'s `bool` return. The
    notification is owed per *record*, so an already-canceled awaiter still gets it.
  - **Total no-throw, and the guard lives in exactly ONE place.** Put the
    `try`/`catch` *inside* the shared invocation helper, not around each call site:
    an escaping exception would abort the pump's per-index batch loop (stranding the
    other records' `TaskCompletionSource`s), or escape into the pump loop's own
    `catch` and fault the whole batch — one user bug becoming N failed sends. Log
    the swallowed failure to `System.Diagnostics.Trace` and make the trace call
    itself no-throw (a host `TraceListener` can throw); attribute it to the user
    callback, never to a sibling family's callback type.
  - **One helper for both flavors.** The sync and async send paths are separate
    code, so a per-flavor invocation is how the ordering / placeholder / swallow
    rules silently diverge. Route both through one method.
  - **State the at-most-once boundary rather than papering over it — and enumerate
    it EXHAUSTIVELY, by WALKING the code path rather than by recalling shapes.**
    Form C fires only where the binding reads a *core* completion, so the boundary is
    **every** path that faults a send — **or throws out of `Send`** — after the ABI
    already accepted the record (`Producer_send` returned a live future *and* a null
    `out_error`: the ABI's statement that the core has it). Derive the set
    mechanically, by walking every such site from that acceptance to the callback's
    invocation. **The walk's terminating condition is the callback invocation, and it
    must pass through every frame the future travels, on both threads — including the
    frames that sit OUTSIDE a method's own `try`.** Recalling shapes is what leaves
    the list one short — it happened repeatedly in M14/P1's own review rounds: first
    at "teardown only", then at a fixed count of paths, then at "the pump's window is
    only AFTER the completion arrives" (the walk had stopped at the pump's batch
    read, one frame short of the batch *setup* preceding it). On the pull-pump engine
    the walk yields these shapes:
      - *teardown* — a send accepted by the core with no pump left to collect its
        result (two distinct sites: the enqueue that raced the gate closing, and the
        queue drained at stop);
      - *an unexpected managed failure BEFORE the completion is read* — the window
        between the accepting `Producer_send` and the transfer of the future's
        ownership to the pump (allocating the awaiter, its cancellation
        registration). The orphaned future is destroyed **unread** and `Send`
        rethrows. Two things a "the binding faults the send" clause misses here: the
        send surfaces as a **throw**, not a faulted `Task`, so a residual definition
        scoped to faulting alone excludes it; and the record **was** accepted, so a
        public list attributing every no-callback throw to "nothing was sent / the
        core rejected it" is *false* on this path. This shape is engine-shaped rather
        than form-C-shaped — see §A7's ⚠ paragraph; and
      - *an unexpected failure on the pump, between the send being handed to it and
        the callback being invoked* — **on either side of the completion's arrival**,
        because the pump can throw before its batched read reports as well as after,
        and one wholesale-fault site covers throws from either side. **After** the read
        reported: `get_all` has already reported for the **whole** batch, so
        the core *did* report completions, yet the indices not yet reached are faulted
        with no callback — and the duplicate-risk argument applies to that half. The
        *before* half (a throw out of the `get_all` P/Invoke itself, or out of the
        defensive bound check that precedes it — the per-batch allocation of the
        marshalling arrays used to be a second trigger here and is **gone**, the arrays
        being reused fields since M11/P3.1 §12.3) does **not** need an allocation
        failure to be reachable: a stale or
        mismatched native surfaces an `EntryPointNotFoundException` from the pump's
        *first* batched read, so the "OOM-only, therefore theoretical" defence is
        unavailable for it. Neither half is a teardown path, so a residual clause that
        says "teardown" misses them — and a clause scoped to "AFTER the completion
        arrives" misses the *before* half, which was one of this rule's own misses.
    Document each on the public surface, and put a note at **each** faulting site so
    the sites and the public statement cannot drift apart — a faulting site with no
    note is exactly how the pre-read window stayed off the public list while the
    other sites carried one. But note the converse trap this rule also hit: a site
    that already carries a note can still acquire an **uncovered condition**, so
    verify each note's *distinguishing clause* against the code every round, not just
    the presence of a note.

    **State each comparative axis ONCE, on the public surface, and have every other
    site point at it.** This is the M14/P1 round-4 lesson, and it is a *structural*
    rule rather than another thing to remember. Each of rounds 2–4 fixed a flagged
    instance and left a parallel one alive — a missing site, then a stale
    distinguishing clause, then a clause repaired in a code comment but **not in its
    public-xmldoc twin** — and the round-4 sweep then found further copies of the same
    kind. Every one of those lived in a *paraphrase* of an axis the paraphrasing site
    does not own. So: the comparisons between residuals (teardown or not, completion
    arrived or not, throw versus faulted `Task`, which surface) belong in exactly one
    place — the public interface's remarks — and each faulting site's note carries only
    its own residual number, the local reason no core completion exists there, and a
    pointer to that one place. When a repair *is* made to a comparative clause, grep
    the clause's distinctive words across **every** document before calling it done; a
    fix applied to one copy of a duplicated sentence is not a fix.

    **Round-5 amendment — outside that one place, do NOT re-scope a comparative;
    DELETE it.** Rounds 2–5 each *re-worded* a comparative clause, and each re-wording
    produced the next round's false clause — including one written while fixing exactly
    this class of defect. Re-scoping keeps the claim alive, and a live claim about the
    *other* residuals goes stale the next time the set is re-partitioned. So the rule is
    stronger than "state it once": every site other than the canonical enumeration
    states its **own** local fact — what happens on *this* path and why no core
    completion exists here — points at the enumeration for how it relates to the
    others, and carries **no** uniqueness quantifier (*the one* / *the only* /
    *every other*), **no** count of residuals, sites or throw sources, and no
    definite-article exclusivity ("*the* residual reachable through …", where "the"
    silently asserts uniqueness). A claim that is not made cannot go stale, and the
    property is grep-verifiable: outside the canonical enumeration **and this rule's own
    walk narrative above**, zero uniqueness or count claims about residuals /
    no-callback throws / throw sources. The walk narrative is carved out because a
    *derivation* must be able to state its own arithmetic — a walk forbidden from saying
    how many sites it yielded cannot be checked against the code, which is the entire
    reason this rule prefers a walk to a recollection. Its counts live here, next to the
    rule, rather than in a site note or on the public surface, which is where the
    staleness the prohibition guards against actually bites.

    Do NOT synthesize a completion: for teardown, for the
    pre-read window **and for the pump window's before half** it would invent a
    *failure* for a record the core may still deliver successfully, and on the
    batch-abort path where completions had arrived a wholesale fault cannot tell which
    indices already fired, so firing there would *guarantee* duplicates for them.
    **A duplicate is worse than a drop** — the obligation is exactly-once per
    record (root `CLAUDE.md` §9.5), and a duplicate delivery notification is the
    classic FFI callback defect the settle-window test exists to catch. A per-index
    "already fired" latch is what would close the batch-abort path; on
    an OOM-only path it does not earn its per-send state, so record the drop instead.

    ⚠ **Separately from the notification, every such site owes the handle-free
    pattern (§A2).** A window that sits *outside* a method's `try` also sits outside
    its `finally`, so the walk above doubles as the audit for "free every handle on
    every path": the pull-pump's batch setup leaked the batch's future handles for
    exactly that reason. Free from a source that is valid **before** any allocation
    (the drained batch itself), with the **singular** destroy rather than the array
    form — building the array is what failed — and keep the two free sites mutually
    exclusive by rethrowing out of the recovery `catch`, so the `finally` is never
    entered on that path.

**Why:** the floor (netstandard2.0/net462) has no function pointers or
`[UnmanagedCallersOnly]`, so a kept-alive Cdecl delegate is the only portable
mechanism for forms A and B — the pattern confluent-kafka-dotnet uses. The GC sees
only managed refs, not the native thunk (→ keep-alive); the CLR can't propagate an
exception through a Rust frame (→ no-throw); `user_data` is C's only per-call
context channel (→ `GCHandle`). Form C needs none of that precisely because it
never becomes a native function pointer: it is host scaffolding restoring a Java
signature (`bindings/CLAUDE.md §1.2`) over a completion the binding already reads,
which is why it is Mode A and why the pull-pump stays the engine underneath it.

**Anti-patterns:**

  - *(forms A/B)* A delegate with no rooted reference (or a bare inline lambda) —
    collectible mid-call → crash.
  - *(forms A/B)* An exception escaping the callback into native (UB).
  - *(form A)* `topic` typed as `string`, or its pointer read/stored after the
    callback returns (handle already freed — §A3).
  - *(forms A/B)* Leaking or double-freeing the `user_data` `GCHandle`.
  - *(form C)* Registering it with native — a `GCHandle`, a rooted delegate, or a
    new `[DllImport]` (notably `Producer_send_async`) — to deliver something the
    completion the binding already reads can deliver. That is a Mode-B change to
    the completion **engine** masquerading as a callback feature, and the push
    engine is measured slower than the pull-pump.
  - *(form C)* Firing it *after* `TrySetResult` / after the sync return, or gating
    it on `TrySet*`'s `bool` — the first breaks Java's ordering, the second drops
    the notification for a canceled awaiter.
  - *(form C)* A second `try`/`catch` wrapped around the shared invocation helper
    (it shadows the real guard without adding anything), or a batch-scoped `catch`
    *instead of* the per-invocation one (one throwing callback then fails every
    record in the batch).
  - *(form C)* Delivering a `null` metadata on the failure path. Java's user
    callback never sees one — the wrapper substitutes a `-1` placeholder
    (`KafkaProducer.java:1597-1599`, `Callback.java:28-33`).
  - *(form C)* A per-send closure, a captured public record, or an eagerly-built
    placeholder — each turns the carrier into two or more allocations per send.

**Tests required:**

  - *(form A)* Correct offset/partition/topic/timestamp delivered; a non-ASCII
    topic read inside the callback is correct (ties §A3).
  - *(forms A/B)* An exception thrown in the callback is caught, doesn't crash or
    unwind into native, and is re-surfaced.
  - *(forms A/B)* Aggressive GC while the callback is in use doesn't crash
    (keep-alive; §A1).
  - *(form C)* Every behavioural test runs against **both** producer flavors — the
    two firing sites are separate code, so a one-flavor fix is the natural bug.
  - *(form C)* Exactly-once per record, asserted **after a settle window** (a first
    observation of "1" cannot distinguish one invocation from two), including an
    N-record batch so the per-index firing is exercised.
  - *(form C)* Ordering, with a **deterministic** probe — read the awaiter's own
    completion state from *inside* the callback, not a ticket comparison (the
    continuation is only *scheduled*, so tickets race).
  - *(form C)* The failure path delivers non-null placeholder metadata **and** the
    exception, with the exception's code and message asserted (DoD §3).
  - *(form C)* No invocation on a synchronous throw out of `Send` — the test that
    discriminates a correct implementation from a plausible-but-wrong one.
  - *(form C)* A throwing callback is swallowed, is **observable** via a
    `TraceListener`, leaves the send's own result intact, and does not disturb the
    other records in the same batch.
  - *(form C)* Allocation budget: the plain path unchanged, and the callback path
    adding exactly one carrier — with the budget set from a measurement and its
    sensitivity verified by injecting a second per-send allocation.

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

⚠ **A user-facing delivery callback is NOT an input to that decision (M14/P1).**
Java's `send(record, Callback)` is served by **§A6 form C** — a managed-only
callback fired where the binding already reads the completion — so it works
identically under either engine and required no ABI work (Mode A, zero new
`[DllImport]`). Do not read "the user wants a callback" as an argument for the
push option: `Producer_send_async` exists, but adopting it is an *engine* change
with its own measured cost, independent of this surface.

**Option A: pull pump.** Java `Future<RecordMetadata>` → .NET
`Task<RecordMetadata>`, completed by **one** background pump. `Send` enqueues
and returns instantly with a `TaskCompletionSource`-backed `Task`; the pump blocks
on the batched `get_all` and completes each TCS. Mirrors the Python binding's
`poll_futures_thread` (python-ffi.md §6).

```
Caller thread                         Completion pump (one bg thread)
─────────────                         ───────────────────────────────
Send():                          loop:
  pin key/value (call-scoped, §A4)      drain a batch of (future, tcs)
  Producer_send() → future handle       get_all(futures[])   ← BLOCKS
  new TaskCompletionSource (tcs)        tcs[i].SetResult / SetException
  enqueue (future, tcs); return Task    destroy_all(futures)
Dispose(): signal + join the pump ◄──── on shutdown: drain, fault pending, exit
```

**Rule (Option A):**

  - `Send` never calls a blocking `_get`/`_get_all` on the caller's thread —
    it pins (§A4), calls `Producer_send` (inline; a fast enqueue), checks the sync
    `out_error`, enqueues `(future, tcs)`, returns `tcs.Task`. Inline send is fine
    because .NET has no GIL (a send-batching thread is an optional throughput
    tweak, not required).
    ⚠ **That tweak has since been taken, on the ASYNC path only (M11/P3.1).** `Send`
    now pins and appends to a binding-side accumulator, and a send-batch thread issues
    `_send_batch`, mirroring the Python binding; the **sync** path keeps the inline
    `Producer_send` verbatim. It changes only the send *submission* side — the
    completion model below is untouched and Option A (this pull pump) remains the
    engine. It was adopted on user direction and **not** for throughput: it converts the
    caller's block from a native one inside the core's coarse mutex (which a concurrent
    close cannot wake) into a managed, cancellable wait, closer to Java's `send`. ⚠
    Lettering collision: "Option A" *here* is the pull pump, while M11/P3.1's PLAN calls
    the send-batching design "Option A" — different letterings of different questions.
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
thread with `(RecordMetadata*, KafkaError*)`. `Send` would register a
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

  - `Task.Run(get)` per send / any one-thread-per-message pattern; a `Send`
    that blocks on `_get`/`_get_all` directly.
  - A TCS without `RunContinuationsAsynchronously`; running **unbounded, awaited,
    or throw-escaping** user code on the pump.

    ⚠ **Amended in M14/P1** — this bullet used to read "running user code on the
    pump" flat, which forbade the shipped delivery callback (§A6 form C). Java
    fires its `Callback` on the producer's **background I/O thread**
    (`Callback.java:20-21`), and the pump thread is .NET's analogue of exactly
    that, so running one bounded piece of user code there is the *faithful*
    design, not a violation. What remains forbidden is the thing the original
    bullet was really about: letting an **unbounded** amount of user work attach
    itself to the pump, `await`ing user code on it, or letting a user throw
    escape into the pump loop. All three stay closed — the awaiter's
    continuation is kept off the pump by `RunContinuationsAsynchronously` (the
    first half of this same bullet), the delivery callback is a **synchronous
    `void`** with no `await` point, and its invocation is a **total no-throw**
    boundary guarded in one place (§A6 form C). The residual cost is stated on
    the public surface instead of being forbidden here: a slow delivery callback
    delays the other completions in its batch, so keep it short.

    ⚠ **At-most-once residuals that belong to THIS engine rather than to form C, so
    §A6's boundary cannot be stated without them.** They are artifacts of the
    pull-pump's shape — a future the caller must hand to a pump, and a pump that
    resolves futures in batches — not of the delivery callback:
      - *the pre-enqueue window.* `Producer_send` accepts the record and returns a
        future that must then be handed to the pump, so everything allocated in
        between (the awaiter, its cancellation registration) sits **after** the
        core's acceptance and **before** any possibility of reading the completion.
        A throw there destroys the future unread and rethrows out of `Send`: a
        record the core accepted, and may still deliver, whose notification is
        dropped. A push engine has no such handoff and therefore no such window.
      - *the batch abort.* The pump loop's own `catch` faults an aborted batch
        **wholesale** — correct, and what keeps the batch's awaiters from
        hanging — and it catches throws from **both sides** of the batch read, which
        is what makes this residual span the completion's arrival. If the throw
        came *after* `get_all` reported, completions had arrived for **every** index,
        so the indices the batch loop never reached lose their delivery notification
        even though the core did report them. If it came *before* — from the `get_all`
        P/Invoke itself against a stale native, or from the defensive bound check that
        precedes it — nothing was reported and the whole batch loses it. (The batch
        **setup** used to belong on this side too, when the three marshalling arrays
        were allocated per batch outside the processing `try`; M11/P3.1 §12.3 made them
        reused fields, so that trigger is gone.) So, unlike the pre-enqueue window
        above, this residual is **not** OOM-only.
    These are *non-teardown* members of §A6 form C's at-most-once boundary, and must
    be enumerated on the public surface alongside the teardown paths — the enumeration
    itself, with the counts and the comparisons between residuals, lives in one place
    (`IDeliveryCallback`'s remarks; §A6's round-5 amendment).
    Firing from the fault path is not a repair: on a batch aborted *after*
    `get_all` reported, a wholesale fault cannot tell which indices already fired, so
    firing would duplicate the notification for those — worse than the drop; where
    nothing was reported at all (the pre-enqueue window, and a batch aborted *before*
    the read) firing would instead invent a failure for a record the core may deliver
    successfully. A per-index "already fired" latch is what would close the
    after-the-read half, which an OOM-only path does not earn.
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

> **⚠ Standing note — "sole owner" is about the *per-operation* `GCHandle`, and
> there are THREE families, not two.**
> Most of Part B describes the **one-shot per-operation completion**: submit an
> async op, one callback fires, that callback frees the per-op `GCHandle`. Two
> other families do **not** work that way and have their **own** owner — the ABI's
> `user_data_destroy` hook:
>
>   1. a **multi-shot registration** (the rebalance listener, M9/P6), registered
>      once and fired N times; and
>   2. a **one-shot completion *with a release hook*** (the offset-commit callback,
>      M9/P7) — it fires at most once, so it *looks* like the first family, but it
>      is **not invoked at all when the call fails**, while the hook still fires.
>
> Before applying a §B6/§B7 "sole owner" sentence, check which of the three you are
> in. The one-shot invariant does not generalize: applying it to a registration is
> a use-after-free on fire 2..N, and applying it to a commit callback **leaks** the
> `GCHandle` on the failure path. All three families are written out in §B6.

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
     PollWithCallback → *_async(…, cb) ─│──────►│     consumer bg task (ConsumerNetworkThread):
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
    there is **no managed mirror**. ⚠ **One sanctioned exception: `ConsumerHandle`**
    (§B2 Category 6, M9/P8) — see the bullet below. A concurrent **async op** is rejected by the
    core inline (it fires the callback on the caller thread with a
    `ConcurrentModification` error) and surfaces as a **faulted `Task`** carrying a
    `KafkaException` (§B5) — not a managed synchronous pre-check throw. A concurrent
    **sync state read** surfaces as `InvalidOperationException` from the core's
    null-handle rejection path (§B5). The completion callback runs on the
    **dispatcher thread (foreign)**, not the caller → `RunContinuationsAsynchronously`
    + no-throw (§B6/§B7). `Consumer_wakeup` is the one cross-thread call (§B5 /
    consumer-threading §11).
  - **`ConsumerHandle` deliberately bypasses the access guard** — that is the whole
    reason the type exists: *"Nothing in this module acquires the single-owner access
    guard … A handle operation therefore succeeds while another consumer operation is
    in flight, whereas the equivalent `kafka_consumer_Consumer_*` call would be
    rejected with `ConcurrentModificationError`"*
    (`src/ffi/consumer_handle.rs:29-38`). So the "one operation in flight" rule above
    describes the `Consumer_*` surface only; a handle op runs **concurrently** with
    one, by design, and that is what makes in-callback reentrancy possible at all
    (Java gets it free — `acquire()` is reentrant on the polling thread).
  - **Handle ops are synchronous and `block_on` the *calling* thread**, not a worker:
    *"Every operation that is `async` in the core is exposed here as a **synchronous**
    C function that drives the future to completion on the *calling* thread"*
    (`:41-43`). Consequently they are **safe** from the core's callback-dispatcher
    thread — which is where every listener / commit callback runs, so this is the
    reentrancy path working as intended — and from any embedder-owned OS thread, but
    **must not** be called from inside a tokio runtime, where `Handle::block_on`
    would panic. Rather than let a panic cross the FFI boundary, every entry point
    detects that and returns `IllegalStateError` (§B5). This is the **core's**
    `block_on`, inside the sync ABI — the shipped `Seek` / `CurrentLag` precedent —
    **not** a managed sync-over-async façade (CLAUDE.md §4).

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
  - Reading the single-owner rule as applying to `ConsumerHandle` — adding a managed
    guard around handle ops, or "fixing" a handle call that succeeds during a
    consumer op. That success **is** the contract (§B2 Category 6).
  - Wrapping a handle op in `Task.Run` to "make it async" — sync-over-async (§B7);
    the op is synchronous by design and blocks the caller deliberately.
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
    flight, and with a live `ConsumerHandle` outstanding — with **no separate-op
    drain** (the awaiter is the disposer, §B7). ⚠ The teardown route is **not**
    restated here: see §B2's authoritative `Consumer_destroy` path list (five paths,
    three of them deferred).

---

## B2 Handle ownership & lifecycle (`SafeHandle`)

**Decision:** Six ownership categories:

1. **Client / config** (consumer, properties) → a `SafeHandle` freed exactly once
   (teardown routes through `Consumer_close` first — see the Rule).
2. **Flat transient** (error) → read-and-free promptly, **not** wrapped (a
   finalizable object per op is waste).
3. **Owned result / container** (the poll batch + every query-result list/map) →
   the caller frees once after reading; a container is a *borrow-root* whose
   `_destroy` invalidates the elements/bytes borrowed from it.
4. **Borrowed view** (`ConsumerRecord`, `Node`, every `_get` element) → **never
   freed** by the binding.
5. **Consumed by callee** (`ConsumerRebalanceListener_t`) → the binding owns it
   only between `_new` and the call it is handed to; that call **takes ownership
   unconditionally — including when it fails**, so `_destroy` after it is a double
   free. Freed by the binding **only** when the call never reached native.
6. **Caller-owned, independently destroyed, ref-counting its parent**
   (`ConsumerHandle_t`, M9/P8) → the binding owns it for an **arbitrary** lifetime of
   its choosing, and its `SafeHandle` holds a **reference count on the consumer's**
   `SafeConsumerHandle` for exactly that lifetime. Distinct from Category 3 (owned by
   a callback, freed in-call) and Category 5 (ownership transfers away on the call):
   nothing else here outlives the call that produced it *and* keeps a different
   handle alive.

| Handle | Category | Freed by |
|---|---|---|
| `Consumer_t` | 1 — client (`SafeHandle`) | `Dispose`: `Consumer_close_with_timeout` → `Consumer_destroy` (`DisposeAsync`: `Consumer_close_async` → `Consumer_destroy`); **close before destroy** because destroy is **fire-and-forget** — cancels in-flight ops, no bg-task join. ⚠ The destroy is **ref-counted**, so it is immediate only when no op holds a count — see Rule; §B7 + Rule |
| `ConsumerProperties_t` | 1 — config (`SafeHandle`, short) | the binding, after `KafkaConsumer_new` |
| `KafkaError_t` (any `out_error`) | 2 — flat transient | reader: read accessors, then `_destroy` |
| `ConsumerRecords_t` (poll batch) | 3 — owned **borrow-root** | owns the fetched bytes; **copy-out default** (CLAUDE.md §6.4), keep-alive deferred |
| `TopicPartitionList_t`, `OffsetMap_t`/`LongOffsetMap_t`/`OffsetAndTimestampMap_t`/`TopicPartitionInfoMap_t`, `PartitionInfoList_t`, `StringList_t`, `ConsumerGroupMetadata_t`, standalone value types, owned `char*` | 3 — owned result | caller: read/marshal into managed types, then `_destroy` |
| `ConsumerRecord_t`, `Node_t`, every `_get` `const *` element, borrowed `const char*` | 4 — borrowed view | **nobody** — dies with its owning container (3); never `_destroy` |
| `ConsumerRebalanceListener_t` | 5 — consumed by callee | the **callee**: `Consumer_subscribe_with_listener[_async]` consumes it on **every** path, success or failure. `ConsumerRebalanceListener_destroy` is for a listener that was **never** passed to a subscribe call — so the binding's only sanctioned call site is the `catch` around the submit P/Invoke itself (native never ran) |
| `ConsumerHandle_t` | 6 — caller-owned, ref-counts its parent | the **binding**, whenever it chooses: `SafeConsumerReentrancyHandle.ReleaseHandle` → `ConsumerHandle_destroy`, **then** `DangerousRelease` on the consumer's `SafeConsumerHandle` (in that order). Null-safe destroy; "never affects the owning consumer or any other handle" |

⚠ **A `TopicPartitionList_t` delivered *to* a listener callback is Category 3, not
4** — the header states the callback **owns** the handle and must
`TopicPartitionList_destroy` it ("callbacks own the handles delivered to them"). It
is the same type that is a borrowed value elsewhere; classify it by the accessor,
per the note below.

⚠ **The same applies to the `OffsetMap_t` delivered to an offset-commit callback
(M9/P7) — Category 3, owned by the callback**, under the same "callbacks own the
handles delivered to them" convention, alongside the `KafkaError_t` delivered with
it.

**Use the `CopyOutAndDestroy` twin, and prefer adding one over documenting its
absence.** `OffsetMapMarshal` originally exposed only `CopyOut` (which copies and
does **not** destroy) while `TopicPartitionListMarshal` had both — an asymmetry that
made "copy the listener trampoline's shape" a leak on **every** commit. The M9/P7
review round **removed the trap instead of warning about it**: `OffsetMapMarshal`
now has `CopyOutAndDestroy` too, so both delivered-to-a-callback containers are
released by the same shape, in one place. Keep it that way — a marshaller for a
callback-owned container should ship the destroying variant, because nothing
enforces the destroy: a *missing* one is a native leak, invisible to every managed
assertion (verified by injection), while a *double* destroy aborts the process. Use
plain `CopyOut` only where the **caller** owns the root (the query paths).

⚠ **A commit-callback registration is NOT Category 5.** Category 5 is about a
**handle** whose ownership transfers on a call (`ConsumerRebalanceListener_t`). What
transfers to `Consumer_commit_async*_with_callback` is a `void* user_data` — an
opaque `GCHandle`, not an ABI handle — so none of Category 5's `_destroy`
reasoning applies. Its lifetime is governed by the ABI's `user_data_destroy` hook
(§B6, third Rule), which is a *release notification*, not a handle destructor.

**Note — classify by the accessor, not the type.** The returning function's
const-ness decides, not the type name:

  - `const *` return (or a type with no `_destroy` of its own) → **borrowed**;
    never free it (Category 4).
  - non-`const` return with a `_destroy` → **owned**; free it once after use
    (Category 1/3).
  - a handle **passed in** to a call the header documents as taking ownership →
    **consumed** (Category 5); the binding must not free it afterwards.
  - the same type can be owned in one call and borrowed in another — e.g.
    `PartitionInfoList_t` is owned from `partitions_for` but borrowed as a
    `TopicPartitionInfoMap` value; `OffsetAndMetadata_t` is a borrowed `OffsetMap`
    element; `TopicPartitionList_t` is owned from `Consumer_assignment` **and**
    owned when delivered to a listener callback, but borrowed as a `_get` element.

**Rule:**

  - ⚠ **THE AUTHORITATIVE `Consumer_destroy` PATH LIST LIVES HERE.** There are
    **five** managed paths that can reach `Consumer_destroy`, three of them
    deferred. Every other section that mentions teardown **cross-references this
    list and must not restate it** — three separate enumerations drifted out of
    date during M9, each true when written and stale within two phases. If you are
    about to write "teardown is X → Y" anywhere else in this file, write "see §B2's
    path list" instead.

    | # | Trigger | Route | Thread | Immediate? |
    |---|---|---|---|---|
    | 1 | `Dispose`, count == 1 | `Consumer_close_with_timeout` → `Consumer_destroy` | caller's | ✅ |
    | 2 | `DisposeAsync`, count == 1 | `Consumer_close_async` (joins the bg task via `await_join`) → `Consumer_destroy` | caller's | ✅ |
    | 3 | last **async** op's `FreeGcHandle` drops the count to 0 | `ReleaseHandle` → `Consumer_destroy`, **bare** (no preceding close) | core's **dispatcher** | deferred |
    | 4 | last **sync** call's marshaller AddRef released, dropping the count to 0 | `ReleaseHandle` → `Consumer_destroy`, **bare** | the **calling** thread | deferred |
    | 5 | last `ConsumerHandle`'s `ReleaseHandle` drops the count to 0 (M9/P8, Category 6) | `ConsumerHandle_destroy`, **then** parent release → `Consumer_destroy`, **bare** | **whatever thread disposed the handle** | deferred |

    Paths 1–2 are the clean, deterministic ones. Paths 3–5 are the accepted
    deferred-destroy residuals — 3 and 4 are M9/P4 misuse-path residuals (Q1/Q3),
    **5 is normal operation of a shipped public type**, not misuse. Path 4 exists
    because M9/P4 H1 extended ref-counting to the synchronous surface; path 5
    because M9/P8's `ConsumerHandle` ref-counts its parent.

    ⚠ **Paths 4 and 5 can run the destroy on a thread that is neither the caller's
    nor the dispatcher's.** The safety argument for the deferred paths therefore
    rests on §B6's **first** clause — the ref-counted `SafeConsumerHandle`, which
    makes it impossible for a destroy to run *concurrently with* an operation —
    and **not** on the second clause about the dispatcher being serialised. A
    thread-identity argument covers path 3 only. See §B6.

  - `SafeConsumerHandle : SafeHandle` — `ownsHandle: true`, `IsInvalid => handle
    == IntPtr.Zero`. The release path is **not** a bare destroy: `Consumer_destroy`
    is **fire-and-forget** — `shutdown_background` **cancels** in-flight async ops
    (their callbacks never fire) and it does **not** join the bg task (the graceful
    join is `Consumer_close` / `await_join`, not destroy). So teardown routes
    through the graceful **close before destroy**: `Dispose` =
    **`Consumer_close_with_timeout` → `Consumer_destroy`**, `DisposeAsync` =
    **`Consumer_close_async` → `Consumer_destroy`** — no separate-op drain (§B7).
    Under single-owner the awaiter of an op *is* its disposer, so there is nothing
    to drain. Guard use-after-dispose with `ObjectDisposedException`.
  - **Release is ref-counted, so teardown is not always immediate** (M9/P4 H1).
    Every operation holds a count on the `SafeHandle` while it touches the consumer:
    an **async** op for its whole duration (the span-the-op `DangerousAddRef`,
    released in `FreeGcHandle`), and a **sync** call for the duration of the native
    call (the `SafeHandle`-as-parameter marshaller AddRef, §A2). `ReleaseHandle` →
    `Consumer_destroy` therefore runs only when the count reaches **zero**. On the
    clean path — await (or return from) the op, *then* dispose — the count is 1 at
    `Dispose` and the native release is immediate. **But "on the disposing thread" is
    NOT guaranteed** (M11/P8): the awaiter's continuation is
    `RunContinuationsAsynchronously`, so it can resume and drop its count *before* the
    dispatcher's own `DangerousRelease`, leaving the dispatcher thread to take the count
    to zero and run the destroy. That is **safe by construction in both clients** and is
    **not** a defect: each core explicitly *detaches* its dispatcher rather than joining it
    (a join would hang — its own comment says so), and the producer's completion channel is
    unbounded, so an in-flight callback cannot block the destroy either. Do not file it.
  - **Deferred destroy — the accepted single-owner residual** (M9/P4 Q1/Q3).
    Disposing with an **unawaited** op still in flight does **NOT** strand the `Task`
    and does **NOT** leak the `GCHandle`. (It did before M9/P4; that older
    description is **obsolete** — do not reason from it.) What happens now: the op
    runs to completion, and the destroy fires later from `FreeGcHandle` on the core's
    **dispatcher thread**, so native resources are retained until the op finishes
    (bounded by that op's own timeout) instead of being released at `Dispose`.
    Because the core's access guard rejects the graceful close while the in-flight op
    holds it, that deferred destroy is **bare** — no preceding `Consumer_close`. Both
    consequences are **accepted permanently** for this misuse case (Python parity);
    `DisposeAsync` on the awaiting task remains the clean, immediate path. See the
    carve-out in **Anti-patterns** below before filing either as a defect.
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
  - **Category 5 — consumed by callee** (`ConsumerRebalanceListener_t`, M9/P6).
    Build it, hand it to `Consumer_subscribe_with_listener[_async]`, and **never
    touch it again** — ownership transfers on **every** path, including the call's
    own error return. `ConsumerRebalanceListener_destroy` exists only for a listener
    that never reached a subscribe call, so the binding's single sanctioned call
    site is a `catch` around the submit P/Invoke itself (the `SafeHandle` marshaller
    can throw `ObjectDisposedException` against a concurrent teardown, in which case
    native never ran). **Do not infer release from the return code**: the header is
    explicit that a subscribe rejected *after* the core registered the listener
    keeps the registration, and that an empty-topic-list subscribe releases it while
    returning **success**. The authoritative signal is the `user_data_destroy` hook
    (§B6), not the error handle.
  - **Category 6 — caller-owned, ref-counting its parent** (`ConsumerHandle_t`,
    M9/P8). The ABI says *"A handle is usable only while its consumer is alive.
    Destroy every handle **before** destroying the consumer"*
    (`src/ffi/consumer_handle.rs:67-70`) and .NET cannot force user ordering, so the
    binding **enforces** it instead of documenting it: the handle's `SafeHandle`
    takes **exactly one** `DangerousAddRef` on the consumer's `SafeConsumerHandle`
    and releases it in its own `ReleaseHandle`, **after** `ConsumerHandle_destroy`.
    That ordering is load-bearing twice over. It makes the ABI's
    destroy-before-destroy ordering true by construction; and because every handle
    op passes the handle's `SafeHandle` as a parameter, the marshaller holds a
    call-scoped ref for the whole native call, so the parent count cannot drop while
    an op is still blocked inside the core. Releasing the parent from the public
    wrapper's `Dispose` instead would break both — `SafeHandle.Dispose` only
    *requests* release.
    **One reference, not two.** The `AddRef` is taken once, at creation, *before* the
    native call (so the consumer cannot be destroyed across handle creation) and then
    **adopted** by the `SafeHandle` rather than re-taken. Taking a second one leaves
    the count permanently above zero: `Consumer_destroy` never runs and **every**
    consumer that ever produced a handle leaks its native resources for the process
    lifetime, with no managed symptom whatsoever. That bug was real (M9/P8) and was
    caught only by the differential deferred-release test below.
  - **Category 6's consequence — a third deferred-destroy path, accepted.** A live
    reentrancy handle **defers** the consumer's native destroy past its `Dispose`
    (the handle stays usable; teardown does not hang and does not throw), and a
    handle the user never disposes defers it **indefinitely**. This joins M9/P4's two
    accepted residuals rather than contradicting them, and is the same trade for the
    same reason: a deferred destroy is a leak, a raw pointer outliving its consumer
    is corruption, and M9/P4 closed the latter class deliberately. Unlike M9/P4's
    residuals this one is **not** confined to a misuse path — it is the type's normal
    operation — so it is documented on the public type, not only here.

**Why:** `SafeHandle` is the robust form of "call `_destroy` exactly once," even
through exceptions; `IsInvalid == zero` matches our null-safe destroy (this is
confluent-kafka-dotnet's `SafeHandleZeroIsInvalid` pattern). A per-message
`SafeHandle` would allocate a finalizable object per record, so transient handles
and borrowed views are not wrapped. The borrow-root discipline (Category 3
outlives its Category 4 borrows) is what makes the receive-path zero-copy contract
(§B4, CLAUDE.md §6.4) safe. `Consumer_destroy` being fire-and-forget is why
teardown routes through the graceful `Consumer_close` (`_with_timeout` / `_async`)
first — a bare destroy skips the bg-task join. There is no separate-op drain:
under single-owner the awaiter of an op is its disposer, so there is no concurrent
submitter for close to drain.

The ref-counted release (M9/P4 H1) is what closes the use-after-free that the
pre-M9/P4 shape had: a sync call could hold a raw `DangerousGetHandle()` pointer
across a multi-second blocking native call (`Poll` with a caller-supplied timeout)
while a concurrent `Dispose` freed the consumer underneath it. Holding a count for
the duration of the native call removes that window by construction. The cost is
that teardown racing an *unawaited* op no longer releases at `Dispose` — the
deliberate trade, since the alternative (forcing cancellation at teardown) **is**
the use-after-free being fixed, and a safe version of it would need a core-side
cancel-then-join that this layer cannot provide.

**Anti-patterns:**

  - Freeing a **borrowed** view (Category 4: `ConsumerRecord`, `Node`, a `_get`
    element, a borrowed string) — double-free / UAF; only the owning container is
    freed.
  - Destroying a **borrow-root** (`ConsumerRecords_t`, a list/map) while a borrowed
    element or byte slice from it is still in use (CLAUDE.md §6.4) — use-after-free.
  - Leaking an **owned** result (forgetting `_destroy` after marshalling a query
    map/list), or freeing it twice.
  - `ConsumerRebalanceListener_destroy` on a listener a subscribe call already
    consumed (Category 5) — a double free, and it fires the release hook a second
    time. Equally wrong: skipping the destroy on the one path where it *is* correct
    (the submit P/Invoke threw, so native never ran) — that leaks the listener and
    its `user_data`.
  - A bare `Consumer_destroy` without routing through `Consumer_close`
    (`_with_timeout` / `_async`) first — skips the graceful bg-task join. Do not
    re-add a `Dispose`-side separate-op drain (there is no concurrent submitter to
    drain under single-owner).
  - Passing a **raw `DangerousGetHandle()`** to a *synchronous* consumer
    `[DllImport]`. Sync calls take the **`SafeHandle` as the parameter** so the
    marshaller holds a call-scoped ref (§A2); a raw pointer is a use-after-free
    against a concurrent `Consumer_destroy`. The only sanctioned raw-pointer sites
    are the **async submit helpers** (which hold a manual span-the-op ref instead),
    the **close family**, exempt because the one-shot `TryBeginClose` latch
    already orders close-before-destroy on the same thread in program order, and the
    two **`*_destroy` entry points themselves** (`Consumer_destroy`,
    `ConsumerHandle_destroy`), which are structurally exempt: each is called from its
    own `SafeHandle`'s `ReleaseHandle`, where passing `this` would make the
    marshaller `DangerousAddRef` a handle that is already mid-release. Every
    **other** synchronous consumer or handle `[DllImport]` takes its `SafeHandle` as
    the parameter — all 21 of M9/P8's non-destroy handle declarations included.

> **⚠ CARVE-OUT — do NOT file these two as defects** (M9/P4 Q1/Q3, decided by the
> maintainer; the full five-point argument is in
> `design/history/M9/P4/PLAN.md` §4.3):
>
> 1. **`Dispose` returning without releasing native resources** when an unawaited op
>    is in flight (deferred destroy), and
> 2. that deferred destroy being **bare** — no preceding `Consumer_close`, because
>    the core's guard rejected the close while the op held it.
>
> Both are accepted **permanently**, not deferred pending a fix, and there is
> deliberately **no tracked follow-up item** for either. They are reachable only on
> the documented-misuse path (dispose without awaiting your own op). The
> close-before-destroy rule above is **not** violated by them. A core-side
> cancel-then-join would be the theoretical clean fix; it is explicitly **not
> pursued and not tracked**.

**Tests required:**

  - Poll a batch, read records, then dispose — no leak; a borrowed key/value/topic
    used after the batch is gone is prevented (copy-out) or kept valid by the
    wrapper (keep-alive), per CLAUDE.md §6.4.
  - Each query API (`committed`/`assignment`/`partitions_for`/…) frees its owned
    result exactly once after marshalling; borrowed elements are never freed.
  - Create/close many consumers — no leak; `Dispose` joins the bg task
    (`Consumer_close`) before `Consumer_destroy`.
  - **Category 6:** the parent ref-count is asserted as a **differential**, because
    only the contrast is meaningful — with **no** handle outstanding the consumer's
    `Dispose` releases immediately, with one outstanding it does **not**, and the
    handle's own `Dispose` completes the release. A single-case assertion cannot tell
    a working ref-count from a permanently-unbalanced one (both read "not released").
    Add the reverse order and a many-handle balance loop.
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
  - **Two error channels on the commit-callback family, and they must not be
    conflated** (M9/P7). `Consumer_commit_async*_with_callback` are **sync** calls
    that *also* deliver an error later:
      - the **returned** `KafkaError*` is a commit-**initiation** failure — the
        commit never started, and the callback will never fire. It becomes a
        `KafkaException` thrown synchronously from `CommitAsync`, the ordinary
        sync-op shape above.
      - the error **delivered to the callback** is the **commit's own outcome**
        (`null` = success, mirroring Java's "exception == null means success"). It
        is freed by `FromHandle` inside the trampoline and handed to the user as a
        `KafkaException?` parameter. There is **no `Task` to fault** here — this
        family is fire-and-forget (§B6 third Rule, §B7) — so "async op failures
        fault the `Task`" does **not** apply to it.
    A commit can initiate cleanly and fail later, so both channels need their own
    coverage. Note the delivered channel has **no broker-free vehicle**: against a
    `MockConsumer` the core always delivers a null error, so testing it means
    driving the trampoline the way the core would.
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
    concurrent **async op** (`PollWithCallback` / `CommitWithCallback` /
    `SubscribeWithCallback` / `SeekWithCallback`) is rejected by the core **inline**
    (it fires the completion callback
    on the caller thread with a `ConcurrentModification` error), which the bridge
    surfaces as a **faulted `Task`** carrying a **`KafkaException`** — *not* a
    managed synchronous throw. A concurrent sync **state read** (`Assignment` /
    `Subscription` / `Paused` / `GroupMetadata` / `GroupId`) → the core returns a
    **null** handle, which the getter maps to **`InvalidOperationException`** ("not
    safe for multi-threaded access"). Mirrors Python exactly (`RuntimeError` from
    `_concurrent_error()` for state reads, `KafkaError`/ConcurrentModification for
    ops).
  - **`ConsumerHandle` errors (M9/P8) take the ordinary flat `KafkaException` route —
    including the in-runtime `IllegalStateError`.** Two conditions are handle-specific
    and both are core behavior surfaced verbatim:
      - **`UnsupportedVersionError`.** On a handle obtained from a `MockConsumer`,
        *"`wakeup` works and the sync getters return empty lists, but every async
        operation fails with `UnsupportedVersionError` — the mock has no event pipeline
        … **This is core behavior, not an FFI limitation**"* (`:82-86`). **Assert it,
        do not work around it**, and assert the exact message (DoD §3), since the code
        alone does not distinguish it from any other `UnsupportedVersion`.
      - **`IllegalStateError` when called from inside a tokio runtime.** A handle op
        cannot `block_on` there; rather than let a panic cross the FFI boundary, every
        entry point *"detects that situation and fails with an `IllegalStateError`"*
        (`src/ffi/consumer_handle.rs:49-57`). It is delivered as an ordinary error
        **handle** and surfaces as a flat `KafkaException`, like every other
        operational error.
  - ⚠ **Do NOT map the core's `illegal_state` to `InvalidOperationException`, and do
    not read CLAUDE.md §3's idiom map as asking for it.** An earlier draft of this
    section (M9/P8) added exactly that mapping and it was wrong on three counts, each
    independently sufficient:
      1. **The idiom map has a different scope.** Its row reads
         "`IllegalArgumentException` / `IllegalStateException` → `ArgumentException`
         (family) / `InvalidOperationException` — **validate before the FFI call**".
         It governs *managed-side precondition validation*, i.e. Java exceptions the
         binding raises itself before calling native. The in-runtime rejection has no
         Java counterpart at all (Java has no tokio runtime) and arrives *from* the
         core on the operational channel, so the row does not reach it.
      2. **It would split one condition across two exception types.** The core's
         `illegal_state` is not handle-specific. `handle.Position(unassignedTp)` and
         `consumer.Position(unassignedTp)` return the **identical** error — measured
         at runtime against a real consumer: code `-1`, *"You can only check the
         position for partitions assigned to this consumer."* Mapping only the handle
         side would make the reentrancy twin throw a different type than the consumer
         it mirrors, for the same call.
         ⚠ **That identity is duplicated-literal, not structural — do not describe it
         as a shared implementation.** `ConsumerHandle::position`
         (`async_kafka_consumer.rs:316-320`) delegates to
         `AsyncConsumerHandleState::position` (`:582-593`), while
         `AsyncKafkaConsumer::position_timeout` (`:4516-4527`) has its **own** body in
         a different impl; the two duplicate the assignment check and the message
         **literal**. Nothing in the compiler keeps them equal — someone can reword one
         and not the other. **The only thing holding this property is
         `HandleAndConsumer_ReportTheSameCoreError_Identically`**, which asserts `Code`
         and `Message` equality at runtime. If that test is ever deleted, this reason
         loses its evidence. (An earlier draft of this bullet also cited `ensure_open`'s
         *"This consumer has already been closed."* as a second shared instance; it is
         **not** — all 24 `ensure_open()` call sites are on `AsyncKafkaConsumer`, none
         on the handle state, so the handle surface never emits it.)
      3. **It is not implementable as stated.** `illegal_state` carries the generic
         code `-1`, shared with other errors — there is no distinguishable
         `IllegalState` code to branch on, so "map the in-runtime one only" reduces to
         matching on message text.
    **The only sanctioned non-`KafkaException` mapping on this surface remains the
    concurrent-state-read *null return*** — a structurally different channel (no error
    handle exists, so the binding must synthesize something), which is why it is an
    exception and a returned code is not.
  - **The guard-bypass contrast is itself a contract, and it is testable.** The same
    logical call has two different surfaces depending on which handle it goes
    through, and the header states both: `Consumer_assignment` returns **null** *"on
    a concurrent-access rejection (the guard could not be acquired)"* →
    `InvalidOperationException`, while `ConsumerHandle_assignment` *"Returns … a
    **non-null** …"* because it takes no guard (§B1). Inside a callback that holds
    the guard the first is rejected and the second succeeds — the reason
    `ConsumerHandle` exists, and the mock-testable form of `consumer-threading.md`
    §31's first mandatory regression test (no broker, no threads, no sleeps).

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
  - Treating `UnsupportedVersionError` on a mock-derived `ConsumerHandle` as a bug to
    route around (a managed pre-check, a silent no-op, an invented "mock
    unsupported" exception) — it is documented core behavior, and the binding's job
    is to surface it unchanged.
  - Branching on an error **code** to pick a managed exception type — the binding does
    this **nowhere**, deliberately (§B5's flat-`KafkaException` rule). In particular do
    not "fix" a handle's `illegal_state` into an `InvalidOperationException`: see the
    three reasons above.
  - Adding an `InvalidOperationException` concurrent-rejection mapping to a
    **handle** getter. It has none to map: the ABI documents the return as non-null
    precisely because no guard is taken.

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
  - **`ConsumerHandle` (M9/P8):** every async op on a mock-derived handle throws
    `KafkaException` with the `UnsupportedVersion` code **and the exact message**;
    `wakeup` and the three getters succeed and return **empty, not null**; and the
    guard-bypass contrast — inside a listener fired by `MockConsumer.Rebalance`,
    `handle.Assignment()` **succeeds** while the same listener's
    `consumer.Assignment()` throws `InvalidOperationException`. ⚠ That last one needs
    a **mutation check** (swap the handle call for a second consumer call and confirm
    it fails), or a both-succeed regression passes it silently.

---

## B6 Callback & delegate marshalling — completion callbacks

**Decision:** Three callback *families* cross this boundary, and they have
**different lifetimes**. All marshal as a kept-alive
`[UnmanagedFunctionPointer(Cdecl)]` delegate (classic — no function pointers on the
floor) with a no-throw body and context passed via a `GCHandle` in `user_data`;
everything after that differs.

  - **One-shot per-operation completions, *without* a release hook** — the
    consumer's **~8** (`Consumer_poll` / `op` / `position` / `committed` /
    `offsets_for_times` / `long_offsets` / `partitions_for` / `list_topics`), the
    **primary** mechanism for every async op. Fires **once** per submit; **none of
    these entry points takes a `user_data_destroy`**, and the callback *is* invoked
    on every path including the core's inline guard rejection — which is what makes
    "the callback is the sole owner of the per-op `GCHandle` free" *total* for them,
    and only for them.
  - **Multi-shot registrations** — the **rebalance listener** (M9/P6:
    `on_partitions_revoked` / `_assigned` / `_lost`, registered once by
    `Consumer_subscribe_with_listener[_async]`). Registered once, fires **N** times,
    bound to a *subscription* rather than an operation; the `GCHandle` is freed by
    a **separate release hook**, never by a callback.
  - **One-shot completions *with* a release hook** — the **offset-commit callback**
    (M9/P7: `Consumer_commit_async_with_callback` /
    `_commit_async_offsets_with_callback`). Fires **at most once**, bound to an
    *operation*, so by shape it belongs with the first family — but those entry
    points **do** take a `user_data_destroy`, and the callback is **not** invoked
    when the call fails. The `GCHandle` is freed by the hook, never by the callback.

⚠ **The one-shot "sole owner is the callback" invariant does NOT generalize.**
Everything §B6/§B7 says about freeing the per-op `GCHandle` is scoped to the
**hookless** per-operation family. Applying it to a registration is a
use-after-free on fire 2..N; applying it to a commit callback **leaks** the
`GCHandle` on the marshal-failure path, where the callback never runs. Each
family's rule is stated separately below; the Anti-patterns and Tests-required
blocks are shared.

**The one question that decides the free site:** *does this entry point take a
`user_data_destroy`?* If it does, the hook is the sole owner — because the hook is
then the only thing guaranteed to run on **every** path. If it does not, the
callback is, because it is.

The **one-shot** shape is **not uniform** — always `(…, KafkaError*, void* user_data)`
with a non-null `KafkaError*` = failure, but the *result* slot varies: an **owned
handle** for `poll` / `committed` / `offsets_for_times` / `long_offsets` /
`partitions_for` / `list_topics` (`(handle*, error*, ud)`); a **scalar** for
`position` (`(int64_t, error*, ud)`); and **none** for `op`, the void-in-Java ops
(`(error*, ud)`).

The **multi-shot** shape is uniform and *returns a value*:
`kafka_common_KafkaError_t* (*)(TopicPartitionList_t* partitions, void* user_data)`
for all three listener callbacks, plus a release hook `void (*)(void* user_data)`.

**Rule (one-shot per-operation completion, hookless — the ~8 async ops):**

  - Named delegate type, `[UnmanagedFunctionPointer(CallingConvention.Cdecl)]`,
    blittable params. Keep the delegate rooted (a `static readonly` field or the
    per-op `GCHandle`) so the GC can't collect it while native holds the thunk.
  - **Foreign thread → no-throw is mandatory.** The callback fires on the native
    callback-dispatcher thread (§B7), not the caller — no caller frame to catch, so
    an escaping exception is a crash/UB. `try/catch` all, surface via the TCS.
  - **Per-op keep-alive.** The delegate + the `GCHandle` (over the
    `TaskCompletionSource`) must stay alive from **submit until the callback
    fires** (the whole op, not a synchronous call), freed **exactly once** by the
    callback — including the inline guard-rejection error path. ⚠ This sentence is
    **scoped to these eight hookless entry points** and does not carry to a
    hook-bearing one; see the third Rule below.
  - **The callback owns any handle it gets.** For the **owned-handle** forms it
    consumes the result — marshal it (copy-out CLAUDE.md §6.4) then `_destroy`
    (§B2 Category 3); `position`'s scalar needs no free; `op` has no result. On
    failure it builds the exception (`FromHandle`, §B5) and frees the `KafkaError`.
    Then it completes the TCS.

**Rule (multi-shot registration — the rebalance listener, M9/P6):**

  - Same marshalling floor: a named Cdecl delegate per callback, **rooted in a
    `static readonly` field**. Per-subscribe delegate instances or inline lambdas
    are wrong here for a stronger reason than in the one-shot case — a registration
    outlives its subscribe call by definition, so there is no per-op `GCHandle` to
    root the thunk either.
  - **Foreign thread → no-throw is still mandatory**, and here the `catch` has
    somewhere to go: a managed exception is converted into an **owned
    `kafka_common_KafkaError_t*` built with `kafka_common_KafkaError_new` and
    *returned***. Ownership of that handle **transfers to the core — do not destroy
    it.** (This is the inverse of the one-shot rule, which *frees* the `KafkaError`
    it is handed.) Returning `NULL` means success. The conversion path must itself
    be no-throw.
  - **The callback owns the delivered `TopicPartitionList_t`** (§B2 Category 3, per
    the header's "callbacks own the handles delivered to them"): copy every element
    out, then `_destroy` the root exactly once — in a `finally`, so the handle is
    released even when the copy-out throws.
  - **The registration `GCHandle` is freed by the ABI's `user_data_destroy` hook,
    and by nothing else.** Not by a listener callback (that is a use-after-free on
    fire 2..N), not at binding teardown. Make the free idempotent-safe
    (`Interlocked` + `IsAllocated`) because the "native never ran" abandon path can
    reach the same site. The hook fires **exactly once**, on **any thread**, on
    every release trigger the header enumerates — including two the binding could
    not infer from a return code (a subscribe rejected *after* registration keeps
    the registration; an empty-topic-list subscribe releases it while returning
    success). Treat the hook, not the error handle, as the authoritative signal.
  - **Where the safety of freeing from the hook actually comes from** (M9/P6
    P6-D3, recorded so it is not re-derived): the hook is reachable concurrently
    with a queued listener job in principle — the dispatched job carries a **raw**
    `user_data` copy, not a reference that keeps the registration alive. What closes
    the window is the **ref-counted `SafeConsumerHandle`** (§B2) plus the **single
    serialised dispatcher**: a listener callback only ever runs inside an operation
    that holds a count, so `Consumer_destroy` cannot run concurrently with one, and
    **M9/P4's** deferred-destroy path fires from `FreeGcHandle` **on the dispatcher
    thread** — the same thread that would run a queued job. If either of those two
    properties is ever weakened, this rule must be re-derived.
    ⚠ **The dispatcher-thread clause is no longer universal (M9/P8).** §B2's Category-6
    reentrancy handle added a **third** deferred-destroy path that fires from
    `SafeConsumerReentrancyHandle.ReleaseHandle` on **whatever thread disposed the
    handle**. It is safe on the **first** clause alone, but the argument differs by
    callback family and **must not be stated as one blanket sentence**:
      - **Rebalance listener** — destroy runs only at count zero, a listener callback
        only ever runs inside an operation that holds a count, and its dispatched job
        is enqueued and drained inside the very operation that produced it, before that
        operation's completion releases its count. So at that destroy no listener
        callback is running and none is queued, on any thread.
      - **Offset-commit callback** — ⚠ **the clause above does NOT apply to it.**
        `CommitAsync(callback)` reaches the ABI as a *synchronous* call, so its
        marshaller AddRef is call-scoped and released before the callback fires;
        `CommitCallbackRegistration` takes no `DangerousAddRef`. That family is
        fire-and-forget (§B7) and holds **no managed count** — which is exactly what
        the M9/P7 Rule below means by "no `Task` or flow of its own". Its safety
        against the M9/P8 path rests on a different and simpler ground: that destroy is
        structurally the **same shape as §B2 path 1** (`Dispose` at count 1, destroy on
        the disposing thread), which has shipped since M9/P1 and was never
        dispatcher-covered either — as is §B2 path 4. Whatever makes a queued commit
        callback safe against a path-1 destroy makes it safe against a path-5 one; P8
        adds no exposure, only a later moment.
    **Both re-derivations this trip-wire asks for are therefore done** — including the
    one the M9/P7 Rule below inherits, which would otherwise be left armed and
    unserviced by the "no longer universal" statement above. Do not extend the
    dispatcher-thread clause to cover the M9/P8 path, do not read it as still
    universal, and do not re-merge these two families into one sentence.

**Rule (one-shot completion with a release hook — the offset-commit callback, M9/P7):**

  - Same marshalling floor: a named Cdecl delegate, **rooted in a `static readonly`
    field**. A per-call delegate instance or an inline lambda is wrong for the same
    reason as in the multi-shot case — against a real consumer the registration
    outlives the submitting call, so there is no synchronous frame keeping the thunk
    alive.
  - **Foreign thread → no-throw is mandatory, and here there is nowhere to surface
    it.** The ABI typedef returns `void` and there is **no `TaskCompletionSource`**,
    so unlike both other families a managed exception has no channel at all: catch
    everything and **swallow**, writing it to `System.Diagnostics.Trace` so the
    failure is not strictly silent (Java's `OffsetCommitCallback.onComplete` returns
    `void` and cannot report its own failure; Python logs and swallows). The trace
    call must itself be no-throw — a host `TraceListener` can throw, and diagnostics
    must never escalate into an unwind across the FFI.
  - **The callback owns *both* handles delivered to it** — the (always non-null)
    `OffsetMap_t` and, on failure, the `KafkaError_t`. Free the error via
    `KafkaException.FromHandle` (§B5) **first**, before anything fallible, since it
    frees in its own `finally`; hand the map to
    `OffsetMapMarshal.CopyOutAndDestroy`, which copies out and destroys the root in
    its own `finally` (§B2) — the twin of the
    `TopicPartitionListMarshal.CopyOutAndDestroy` the listener trampolines use. Keep
    a local ownership **baton** for the map (zeroed *before* the helper is called,
    with a null-safe `_destroy` of the baton in the trampoline's own `finally`) so
    "destroyed exactly once on every path" stays true even on a path that never
    reaches the helper.
  - **The `GCHandle` is freed by the ABI's `user_data_destroy` hook, and by nothing
    else** — not by the callback. The hook is the only site that runs on every path:
    `user_data` ownership transfers **unconditionally**, so the hook fires *even when
    the submitting call returns an error*, and the offsets-taking entry point returns
    a marshal error **without registering the callback** — the callback never fires,
    the hook still does. Freeing in the callback therefore leaks on that path; and on
    the **success** path the header pins the ordering that rules it out there too —
    the hook fires "after the commit completed and the callback returned"
    (`confluent_kafka.h:524-528`), so a trampoline-side free would release a
    `GCHandle` the core is still about to hand to the hook.
    Make the free idempotent-safe (`Interlocked` + `IsAllocated`): the "native never
    ran" abandon path (the submitting P/Invoke threw) can reach the same site.
  - **Where the safety of freeing from the hook comes from:** the same two properties
    as the multi-shot rule above — the **ref-counted `SafeConsumerHandle`** and the
    **single serialised dispatcher** — and, as there, **not** an owned `Arc` held
    across the callback (disproved: the core copies `user_data` out before
    dispatching). If either property is weakened, re-derive this rule.
  - **A callback-less commit is a *fourth* shape, and it needs no registration.** The
    ABI's `callback` parameter is **not nullable** and there is no plain
    `Consumer_commit_async_offsets`, so Java's legal `commitAsync(Map, null)` is
    expressed with a **no-op discard trampoline** that still frees both delivered
    handles (C's `discard_commit_complete` is the reference shape). Pass a **null**
    `user_data` and **no** hook on that path: there is nothing managed to root, so
    allocating a `GCHandle` there would create a handle with no releaser. Passing
    `NULL` for `callback` instead is **undefined behavior**.
  - **Decide the triple in one place.** The `(callback, user_data, user_data_destroy)`
    triple should come from a single named helper that both production and the tests
    call (`definition-of-done.md` §12), so a test driving the ABI directly cannot
    keep passing a hook that production has stopped passing.

The async *flow* (submit → dispatcher → `SetResult` with
`RunContinuationsAsynchronously`; one-op-in-flight; `Dispose`) is **§B7** — this
section is only the callback *marshalling*. Neither a registration nor a commit
callback has a `Task` or a flow of its own: the registration is bound to the
subscription's lifetime, and the commit callback is fire-and-forget (§B7).

**Why:** the floor (netstandard2.0/net462) has no function pointers or
`[UnmanagedCallersOnly]`, so a kept-alive Cdecl delegate is the only portable
mechanism — the pattern confluent-kafka-dotnet uses. The GC sees only managed
refs, not the native thunk (→ keep-alive); `user_data` is C's only per-call
context channel (→ `GCHandle`). Because the callback runs on the core's **foreign**
dispatcher thread, no-throw is not optional (there is no caller frame to catch)
and the keep-alive spans the whole op (submit→fire), not a synchronous call.

**Anti-patterns:**

  - A per-op delegate / `GCHandle` not rooted for the whole submit→fire window
    (collected mid-op → crash); a **registration** delegate that is not
    `static readonly` (a registration has no per-op handle to root the thunk).
  - An exception escaping into the dispatcher thread (no caller to catch → UB).
  - Not freeing the owned result/error + `GCHandle` on some path (esp. the inline
    guard-rejection).
  - Reading a borrowed `topic`/bytes pointer after its batch is destroyed (§B3/§B4).
  - **Multi-shot only:** freeing the **registration** `GCHandle` from a listener
    callback (a use-after-free on fire 2..N) **or** from binding teardown — the
    `user_data_destroy` hook is its sole owner. Destroying the `KafkaError*` the
    callback **returns** (ownership transferred to the core — a double free).
    Skipping the delivered `TopicPartitionList_t` destroy on the exception path (a
    leak), or destroying it twice by pairing a hand-rolled `_destroy` with a
    copy-out helper that already destroys. Inferring "the registration is dead"
    from a subscribe's return code instead of from the hook.
  - **Hook-bearing one-shot only (the commit callback):** freeing the `GCHandle` in
    the trampoline because "it is a one-shot" — that is the *hookless* rule, and here
    it leaks on the path where the callback never fires. Releasing the registration
    when the submitting call returns a **non-null error** (the transfer is
    unconditional; the hook fires anyway, so this is a double free). Pairing
    `OffsetMapMarshal.CopyOut` with no `OffsetMap_destroy` — a leak on **every**
    commit — where `CopyOutAndDestroy` is the intended helper. Blanket-attributing a
    swallowed exception to "the user's callback" when the same `catch` also covers
    marshalling and `GCHandle` recovery, or when the path has no user callback at
    all (the discard trampoline) — a confidently-wrong diagnostic is a worse
    debugging cliff than a silent one, which is the whole thing tracing exists to
    avoid. Passing `NULL` for the non-nullable `callback` parameter (UB), or
    allocating a `GCHandle` on the callback-less discard path (a handle with no
    releaser). Letting the swallowed exception be *silently* discarded with no
    diagnostic, or letting the diagnostic itself throw.

**Tests required:**

  - Each callback delivers the right result/error and frees the owned handle +
    `GCHandle` exactly once.
  - An exception thrown in the callback is caught (no crash, no unwind into native)
    and faults the `Task` — or, for a registration, is **returned** as a
    `KafkaError*` and surfaces on the operation that triggered the callback (assert
    the code **and** the message, `definition-of-done.md` §3).
  - Aggressive GC during an in-flight op doesn't collect the delegate (keep-alive
    across submit→fire) — and, for a registration, across the whole **live
    registration**, which is a much wider window.
  - **Multi-shot only:** the registration survives **N** fires (a churn loop — a
    per-fire `GCHandle` free turns it red on iteration 2); the release rules hold
    (a *replacing* subscribe releases it, an `unsubscribe` does **not**); the free
    site is idempotent-safe (calling it twice frees nothing and does not throw);
    and teardown with a live registration returns without hanging.
  - **Hook-bearing one-shot only (the commit callback):** the **failure path where
    the callback never fires** still releases the `GCHandle` **exactly once** — this
    is the test that discriminates the correct free site, and without it a wrong
    choice passes silently; the **success** path releases it too (so the trampoline
    freeing as well would be freeing a handle the core still holds); a **throwing**
    callback is swallowed, does not unwind, and does **not** release the
    registration; the swallowed exception is **observable** (a `TraceListener` sees
    it) rather than silently discarded; the callback-less **discard** path allocates
    no registration at all; and the ABI triple the tests pass comes from
    **production's own builder** (`definition-of-done.md` §12), so removing the hook
    from production turns those tests red rather than leaving them measuring a hook
    only the test supplied.

---

## B7 Async / completion → `TaskCompletionSource` (push)

**Decision:** The consumer uses the ABI's *push* surface (settled — no pump):
every async op (`Consumer_poll_async` / `commit_async` / `position_async` / …)
takes a completion callback (§B6) that fires when the op resolves, so the callback
*is* the bridge. Single-owner / one-operation-in-flight (nothing to batch),
serialized by the **Rust core's** guard — **no managed guard** (M3/P2). Bridges to
`Task<T>` via a `TaskCompletionSource` built with `RunContinuationsAsynchronously`.

⚠ **Scope — this section is about callbacks that complete a `Task`. Two §B6 families
complete none, and nothing here applies to them.** A **multi-shot registration**
(the rebalance listener) has no `Task` and no flow of its own. The **offset-commit
callback** (M9/P7) likewise has none: `CommitAsync(callback)` is a *sync* call that
returns the moment the commit is initiated, and the callback is **fire-and-forget** —
there is no `TaskCompletionSource`, no `RunContinuationsAsynchronously`, no
cancellation, and no `GCHandle`-freed-by-the-callback. Its lifetime rule is §B6's
third Rule (the release hook); its error channels are §B5. Do not read a sentence
below as governing either family just because it says "the callback".

```
Caller thread                       Core: runtime worker ──▶ dispatcher thread (1/consumer)
─────────────                       ────────────────────────────────────────────────────
PollWithCallback():                 worker task: poll(timeout).await   ← runs the op
  tcs = new TaskCompletionSource                 build (records | error)
  ud  = GCHandle.Alloc(tcs)  (§B6)                enqueue completion ──┐
  Consumer_poll_async(…, cb, ud) ─► (core guard serializes ops)       ▼
  return tcs.Task                   dispatcher:
  … await tcs.Task                    cb(records | error, ud): marshal + free handles (§B2)
     ◄── continuation on pool ─────   tcs.SetResult / SetException  (RunContinuationsAsync)
                                    no pump · no managed guard · one op in flight
```

**Rule:**

  - `PollWithCallback` (etc.) makes a `TaskCompletionSource`, `GCHandle.Alloc`s it as
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
    only when the submitting P/Invoke threw so native never ran) — ⚠ true of these
    **hookless** ops only, per the scope note at the head of this section.
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
    destroy, with **no separate-op drain**. Under single-owner the **awaiter of an
    op is its disposer**, so there is no concurrent submitter to drain — teardown
    never wakes+awaits a *separately-submitted* op.
    ⚠ **For the routes themselves — all five paths to `Consumer_destroy`, which are
    immediate, which deferred, and on which thread — see §B2's authoritative path
    list. Do not restate them here.** (This bullet used to carry its own
    enumeration; it went stale twice during M9, which is why the list is now
    single-sourced.) The two facts §B7 adds on top: `Consumer_destroy` is
    fire-and-forget at the ABI (it **cancels** any remaining in-flight op and does
    **not** join, §B2), and since M9/P4 H1 the binding rarely reaches it directly at
    `Dispose` because release is ref-counted. Disposing with an **unawaited** op in
    flight therefore does **NOT** strand the `Task` and does **NOT** leak the
    `GCHandle` (the pre-M9/P4 description, now **obsolete**). Retention until the op
    finishes, and that bare destroy,
    are the **accepted single-owner residuals** — see §B2's carve-out; do not file
    either. `DisposeAsync` on the awaiting task is the clean, immediate path.

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
    twice; freeing the **per-op** `GCHandle` from **anywhere but** the callback (the
    sole owner) / `AbandonBeforeSubmit` (native never ran) — a teardown-side free is
    a use-after-free against a straggler callback. ⚠ *Hookless per-op only* — both a
    **multi-shot registration** and a **commit-callback registration** have a
    different sole owner (the `user_data_destroy` hook, §B6); do not read this
    bullet as forbidding either.
  - A teardown that wakes+awaits a *separately-submitted* op (there is no concurrent
    submitter under single-owner) or that re-adds the M3/P1 `Dispose`-side
    `FaultTaskOnly` machinery — it exists to fault a `Task` that no longer strands
    (§B2), so it would now only add a second `GCHandle`-free site, i.e. the
    use-after-free this section's previous bullet forbids.
  - Releasing the span-the-op ref anywhere but `FreeGcHandle`, or taking the
    `AddRef` **outside** the `try` that routes a failure through
    `AbandonBeforeSubmit` — an `AddRef` throw would then skip the abandon path and
    permanently root the per-op `GCHandle` (M9/P4 M3).

**Tests required:**

  - `PollWithCallback` resolves with records / faults with `KafkaException` (mock
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
