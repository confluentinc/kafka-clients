# .NET binding — FFI marshalling & ownership

Deep-dive rules for marshalling across the C ABI (`confluent_kafka_bindings.h`,
`kafka_producer_*` / `kafka_common_Error_*`) from .NET via P/Invoke.
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
    callbacks — the `_cb` completions and the per-record `Callback_t` delivery
    callback (§A6), async completion (§A7).
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
| `int8_t` (an `is_*` predicate) | `sbyte`, tested `!= 0` | Not `bool`. |
| opaque `*_t *` | `IntPtr` → `SafeHandle` above (§A2/§B2) | Never a C# struct mirroring `_private[0]`. |
| `const char *` in | `IntPtr` to a pinned NUL-terminated UTF-8 buffer (§A3/§B3) | No `LPStr` (ANSI), no `LPUTF8Str`. |
| `const char *` out (callee-owned) | `IntPtr` → `Utf8Marshal.PtrToString` (§A3/§B3) | Never a `string` return — the marshaller would free it. |
| `const uint8_t *` + `int32_t len` | `IntPtr` (pinned `byte[]`) + `int` (§A4/§B4) | Zero-copy, call-scoped pin (send) or borrow (receive). |
| `T **` out-param / array | `ref IntPtr` set to `IntPtr.Zero` / `IntPtr[]` | e.g. `out_err`; `get_all` arrays. |
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
  - Bindings use `confluent_kafka_bindings.h` only. Every function that reports an error on return
    takes a trailing `kafka_common_Error_t **out_err`, passed as `ref IntPtr` set to `IntPtr.Zero`;
    a `_cb` form takes none and delivers its error to its callback. Where the plain header's
    function had no such slot, BH names it `<name>_err`; never import the plain name.
    `kafka_common_Error_source_err` is borrowed and never destroyed. `int8_t` returns marshal as
    `sbyte`. A managed exception crosses the core only as a `local_callback` error whose opaque is
    a `GCHandle`, freed by the release callback.
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
  - Each `int8_t` predicate (`is_done`, `is_retriable_error`) reads correctly as
    `sbyte`.
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

The whole-system picture every section assumes. The C ABI runs all Kafka I/O on native threads
owned by the Rust core. Since M19/P1 (the `0035d9ff` ABI, re-anchored at `851c8d95` by ledger 65)
every client has one completion model: each blocking operation has a blocking form and a `_cb`
twin. The `_cb` completions and the user callbacks (delivery, rebalance listener, offset commit)
are queued for the client's `__execute_callbacks`, with one exception: the producer's `send_cb`
and `send_with_callback_cb` fire their completion `cb` inside the call, on the calling thread,
when the producer takes or refuses the record at once (§A1; `rust/src/ffi/producer/mod.rs:131-155`
at `851c8d95`). No callback fires on a native thread of its own; the binding chooses the thread
that drains. When the queue goes from empty to non-empty, the core calls the hook set with
`__set_callbacks_notify`. The hook only signals. The core calls it after releasing its
registration lock, so it can run on any thread that queues work, including a sending thread
inside `send_with_callback_cb`, and two calls can overlap. `__set_callbacks_notify` does not wait
for a call in flight, so the hook's opaque stays valid until the client's `_destroy` returns
(`rust/src/ffi/callback_queue.rs:246-275` at `851c8d95`; BH:38467-38469).

| Client | .NET thread | Who drains `__execute_callbacks` |
|---|---|---|
| `KafkaProducer` | one `CompletionThread`, `confluent-kafka-producer-completion` | that thread, woken by the hook |
| `AsyncKafkaProducer` | one `CompletionThread`, `confluent-kafka-producer-pump` | that thread |
| `KafkaAdminClient` | `kafka-admin-client-thread` | that thread |
| `AsyncKafkaAdminClient` | `confluent-kafka-admin-pump` | that thread |
| `KafkaConsumer` | none | the caller: each blocking call drains once at the top; the blocking forms run listener and commit callbacks inline on the caller |
| `AsyncKafkaConsumer` | `confluent-kafka-consumer-pump` | that thread |

The names match Python's (decision 26). The per-client rules are in §A1/§A7 (producer) and
§B1/§B7 (consumer). Teardown is in `dotnet/CLAUDE.md` §4 (**Close/Dispose**); the consumer's
routes are in §B2's path list.

**Why:** Java's single-Selector NIO model on tokio (CLAUDE.md §8) — unlike
librdkafka (a thread per broker + a mandatory `rd_kafka_poll` loop, which
confluent-kafka-dotnet services with a `LongRunning` `callbackTask`). So our
native thread count is independent of cluster size, there is no poll loop (a drain thread sleeps until the notify hook signals it), and
the multi-thread runtime is what makes `block_on` from .NET deadlock-free. This
applies to both clients.

**Anti-patterns (both clients):**

  - A `callbackTask`-style poll-loop thread — nothing to poll here.
  - Assuming thread-per-broker / native threads scaling with cluster size.
  - A notify hook that does more than signal: running `__execute_callbacks`, calling into the
    client, or taking a lock that a thread holds across a call into the core (a `Send`, a drain).
    The hook can run on that very thread, inside the call (`callback_queue.rs:249-256` at
    `851c8d95`).
  - Freeing the hook's opaque before the client's `_destroy` returns, for example when the hook
    is replaced: `__set_callbacks_notify` does not wait for a call in flight
    (`callback_queue.rs:263-265`, `:270-275` at `851c8d95`; BH:38467-38469).
  - A client `_destroy` inside `__execute_callbacks`. A nested drain returns 0
    (`callback_queue.rs:283-284`, `:291-294` at `851c8d95`), so the destroy cannot run the
    callbacks it must run.

**Tests required (all clients):** `Dispose` returns without hanging, after the client's drain
thread is joined and its in-flight uses reach 0, and destroys on the closing thread. A close
called from inside a callback is deferred until the drain returns; it never destroys inside
`__execute_callbacks`. The hook's opaque is freed only after `_destroy` returns. On the producer,
the hook fired on the sending thread (an async `Send` whose record is taken at once while the
queue was empty) signals the drain thread without a deadlock. For the consumer's routes see
**§B2's path list**; do not restate them here.

---

# Part A · Producer

## A1 Thread model (producer)

**Decision:** Per real `KafkaProducer`, the core owns a multi-thread tokio runtime and one Sender
task over a single async Selector. A `_cb` send first tries the producer on the calling thread. A
record the producer would have to wait for (metadata, buffer memory) goes to a submission task,
which tries it again in FIFO order and otherwise leaves it to wait, up to `max.block.ms`, on a task
of its own (`rust/src/ffi/producer/mod.rs:89-101` at `851c8d95`). The .NET side adds exactly
**one** thread per producer, its `CompletionThread`: `confluent-kafka-producer-completion` on
`KafkaProducer`, `confluent-kafka-producer-pump` on `AsyncKafkaProducer` (decision 26). It drains
`Producer__execute_callbacks` when the notify hook signals. There is no send-batch thread, no
per-send thread and no poll loop (M19/P1, decisions 4 and 9).

```
   .NET (managed)                              │ C ABI │  Rust core (native, per producer)
   ─────────────                               │       │  ─────────────────────────────────
   caller thread(s):                           │       │  tokio multi-thread runtime
     SYNC  Send → Producer_send_with_callback ─│──────►│    blocks until the record is in a batch
     ASYNC Send → Producer_send_with_callback_cb ─────►│    try_send here; else submission task → own task
             first stage completes at the      │       │  Sender task: NetworkClient + ONE Selector
             handover cb                       │       │
   CompletionThread (1 per producer):          │       │  callback queue
     wait for the signal ◄─────────────────────│◄──────│    notify hook (empty → non-empty)
     Producer__execute_callbacks ──────────────│──────►│    runs a queued handover cb / on_completion here
       user callback → KafkaFutureImpl.Complete│       │
       Producer__set_callback_result ──────────│──────►│
   Close/Dispose: refuse calls → close → join the thread → wait for uses → destroy
```

**Rule:**

  - Native side (per real `KafkaProducer`): a multi-thread tokio runtime and one spawned Sender
    task doing all I/O over a **single async Selector** (all brokers multiplexed, NOT
    thread-per-broker), created in `KafkaProducer_new`, torn down by the producer's destroy.
  - .NET side: the caller threads plus the one `CompletionThread`. That thread runs every queued
    `_cb` completion and every `on_completion` inside `__execute_callbacks`, so every user
    delivery callback, and on `AsyncKafkaProducer` the chain's steps after an async callback's
    `Task` ends, outside it (ledger 54, B9; §A6). It is .NET's analogue of Java's background I/O
    thread (`Callback.java:20-21`). Two other threads run producer callbacks: the caller's, for
    the handover `cb` of an async `Send` whose record the producer takes or refuses at once
    (BH:38240-38249), and the thread that destroys the producer, for the callbacks still
    pending then (`rust/src/ffi/producer/mod.rs:166-178` at `851c8d95`): the closing thread, or
    the `CompletionThread` after its drain when `Close` is called from a callback (decision 7).
    A slow delivery callback delays every other completion on that producer; the public
    interface says so.
  - **The notify hook only signals** (decision 4c): it sets the thread's event. It is
    thread-safe and cheap, takes no lock that `Send` may hold, never runs callbacks and never
    calls into the producer, because it can run on any thread that queues work, a sending thread
    inside `send_with_callback_cb` included, and two calls can overlap (§0.3). Its opaque is freed
    only after `_destroy` returns.
  - **No binding-side cap** (decision 9): no `SendAccumulator`, no admission semaphore, no
    `CONFLUENT_KAFKA_PRODUCER_*` knobs. On the async path the only throttle is the first stage,
    which completes at the handover `cb`: a caller that awaits it is bounded, and a caller that
    does not await it can grow the core's outbox without bound. This is accepted, as Python
    accepts it (plan §3.7).
  - **Sync `Send`** calls the blocking `Producer_send_with_callback`, which returns once the record
    is registered (serialized, partitioned, appended to a batch), as Java's `send` does. The core
    applies `max.block.ms` (decision 4). It never calls
    `KafkaFuture_get`, which runs the queued callbacks on the caller (BH:38216-38219).
  - **Async `Send`** calls `Producer_send_with_callback_cb`. Its first stage
    (`ValueTask<AsyncKafkaFuture<RecordMetadata>>`) completes at the handover `cb` and never
    parks a thread. For a record the producer takes or refuses at once, the `cb` fires
    **inside** the call, on the caller's thread: the common case once the topic's metadata is
    cached (BH:38240-38249). For a queued record it fires later on the `CompletionThread`.
    So set up everything the `cb` needs (the first-stage state, the pins, the `GCHandle`s)
    before the P/Invoke. Hold no lock across the call that the `cb` also takes. Inside the call
    the `cb` only records the outcome and releases the pins, and the first stage completes
    synchronously after the call returns. The key and value stay borrowed until the `cb`
    (BH:38190-38191). The binding adds no yield after a run of synchronous completions:
    Python's `_FAST_PATH_YIELD_EVERY` keeps an asyncio loop fair, and .NET has no loop
    (ledger 68).
  - **Order is Java's, not call order (ledger 67b).** The core tries the producer first,
    whatever its queue holds, so "a record taken at once can reach the producer before one
    queued earlier, by this caller or by another" (`rust/src/ffi/producer/mod.rs:157-164` at
    `851c8d95`). A caller's records keep their order only when the caller awaits each first
    stage before its next `Send`, as Java's blocking `send` makes its caller wait. The binding
    adds no queue to restore call order for a caller that does not await: that would put
    Kafka behaviour in the binding. Concurrent callers are not ordered against each other, in
    Java either. *(Withdraws the M11/P3.4 call-order promise.)*
  - **Coverage (ledger 67a).** `Flush`, `Close` and the transaction operations cover every
    record handed over before they began. A record still waiting inside the producer is a send
    in progress: they do not wait for it, as Java's `flush()` and `commitTransaction()` do not
    wait for a `send` blocked on another thread. `Close` fails it with "Producer closed while
    send in progress", and no delivery callback fires for it (`mod.rs:103-129` at
    `851c8d95`). A timed close (`Close(TimeSpan)`, ledger 82, and the 0 ms close from inside a
    callback) spends at most its timeout on the drain and the close together, then waits for
    the delivery reports without a bound (`mod.rs:72-74`, `:122-129` at `851c8d95`;
    BH:38397-38407). *(Withdraws the M11/P3.4 `Flush` promise.)*
  - **The `Send` token** (ledgers 19, 70, 71) never cancels the record. Already cancelled
    at the call: `OperationCanceledException` before any pin or P/Invoke, and nothing is
    sent. A record taken or refused inside the call ignores it. For a queued record the
    first stage waits for the hand-over (up to `max.block.ms`); if the token fired by then,
    the stage ends Canceled with the caller's token, whatever the outcome. A record the
    producer took is still sent, and its delivery callback fires once. A record that `Close`
    fails runs no callback, and its error is dropped. A `Flush` does not wait for a queued
    record (ledger 67a). There is no token registration: the hand-over `cb` checks the
    token when it completes the stage.
  - FFI is callable from any .NET thread; the core serializes producer calls itself, so concurrent
    `Send` is safe. Don't add a lock.
  - `block_on` parks only the calling thread; the Sender keeps running on the runtime's worker
    threads.

**Why:** Java's single-Selector NIO model on tokio (§0.3): a fixed native thread count and no
poll loop. Every callback runs on a thread the binding chooses (the `CompletionThread`, the thread
calling `Send`, or the thread that destroys the producer), never on a native thread of the
core's, so no foreign-thread rule is left (§A6). One thread per producer is what Python has
(decision 26). No cap, because Python has none and the first stage already bounds a caller that
awaits it (decision 9).

**Anti-patterns:**

  - A binding-side lock around producer sends.
  - A send-batch thread, a binding-side send queue or accumulator, an admission semaphore, or any
    other binding-side cap (decision 9).
  - A notify hook that does work (§0.3).
  - Calling the producer's destroy inside `__execute_callbacks`. A nested drain returns 0
    (`callback_queue.rs:283-284`, `:291-294` at `851c8d95`), so the destroy cannot run the
    pending callbacks it must run (`producer/mod.rs:166-178`). A close from a callback is deferred
    (`dotnet/CLAUDE.md` §4 **Close/Dispose**).
  - A sync `Send` or `Get()` that calls `KafkaFuture_get` / `_get_all`.
  - Async `Send` code that assumes the handover `cb` fires later, on another thread: a lock held
    across the P/Invoke that the `cb` also takes deadlocks, and a field set after the call is read
    too early.
  - A per-send thread, a `Task.Run`, or a `callbackTask`-style poll loop (§0.3).

**Tests required:**

  - Concurrent `Send` from many threads is correct.
  - `Dispose` with sends in flight returns without hanging, joins the `CompletionThread`, and
    destroys on the closing thread.
  - A same-thread burst that awaits each first stage is delivered in call order (offsets
    increase within each partition), on both producer flavors. No test asserts call order for
    a burst that does not await (ledger 67b).
  - `Flush` covers every send whose first stage completed before it began, and returns without
    waiting for a send still waiting in the producer (an unreachable broker, `max.block.ms`
    500).
  - `Close` wakes an async `Send` waiting for metadata: its first stage faults with
    "Producer closed while send in progress", and no delivery callback fires.
  - An async `Send` whose record is taken at once (`AsyncMockProducer`) completes its first
    stage synchronously, before `Send` returns.
  - Teardown settles both stages of every send exactly once, and leaves no uncompleted
    `TaskCompletionSource` or `KafkaFutureImpl`. Exception: a mock send left uncompleted at `Close`
    stays pending, as in Java's and Python's mocks; `Dispose` flushes first.
  - A `Send` racing `Close` (decision 7 refuses a `Send` that starts after `Close` with
    `ObjectDisposedException`, before any P/Invoke, so the core's refusal inside the call is
    reached only by a `Send` already past that check): each `Send` throws
    `ObjectDisposedException`, or its first stage completes, or ends faulted with
    `InvalidOperationException` (the core's `IllegalStateError`, decision 14) or the
    `KafkaException` "Producer closed while send in progress"; no deadlock, and every first
    stage settles exactly once.
  - The `Send` token tests, by name (ledgers 19 and 71; the Actor confirms the names in P1.2):
      - T17 `PublicProducerFirstStageTests.Send_CallerTokenFiresWhileTheRecordIsQueued_EndsTheFirstStageCanceledAfterTheHandOver`
        (two cases, as Python's `test_cancelling_a_send_waiting_for_its_handover_raises_after_it`:
        on an unreachable broker the first stage is still pending after the token fired, then ends
        Canceled with the caller's token; if the metadata wait times out, the delivery callback
        fires once with the `TimeoutException`; if `Close` fails the record, no delivery callback
        fires; a `Flush` during the wait does not wait for it)
      - T18 `PublicProducerFirstStageTests.Send_CallerTokenFiresAfterTheHandOver_DoesNotCancelGet`
        (the token fires after the first stage completed; `Get()` returns the metadata)

---

## A2 Handle ownership & lifecycle (`SafeHandle`)

**Decision (M19/P1, decisions 6 and 7):** three ownership categories.

1. **Client** → one `SafeClientHandle` per producer, destroyed exactly once, by the explicit
   teardown only. Its handle is the class's `Producer` view, "a borrowed view valid until the
   class handle is destroyed" (`KafkaProducer__as_Producer_err`, BH:38605-38618;
   `MockProducer__as_Producer_err`, BH:38842-38853); there is no `kafka_producer_Producer_destroy`.
   The class pointer (`KafkaProducer_t *` / `MockProducer_t *`) is kept in a field. The
   configuration is the caller's: `ProducerConfig_new` leaves the map the caller's and delivers
   an owned config (BH:39481-39493), and `KafkaProducer_new` leaves the config the caller's
   (BH:38588-38603). The binding frees both right after `KafkaProducer_new`.
2. **Flat transient** → read and freed in the call or callback that got it, never wrapped (a
   finalizable object per record is hot-path waste): the owned `KafkaFuture_t *` a send
   delivers, freed at once and never read (§A7; BH:3807-3811, BH:38166-38170;
   `KafkaFuture_destroy_err`, BH:37992-38005); the per-send `ProducerRecord_t`, freed right after
   the send call (`ProducerRecord_destroy_err`, BH:39731); the per-send `Callback_t`, which the
   send consumes on every path, its error and a closed producer included, so the binding never
   destroys it (`rust/src/ffi/producer/callback.rs:185-202` at `3f8a5bc6`; §A6); an owned error,
   freed once the error table has read it (`Error_destroy_err`, BH:17957; §A5).
   `on_completion`'s `metadata` and `error` are borrowed and never destroyed (§A6).
3. **Owned result (borrow-root)** → copy every value out into managed types, then free the root
   once, in a `finally`: `Metrics()`'s owned `kafka_Map_t` of owned `MetricName_t *` keys and
   `KafkaMetric_t *` values (BH:38342-38355), freed with `kafka_Map_destroy_err`, which frees
   the owned keys and values too (BH:40277-40288); `PartitionsFor`'s owned `kafka_List_t` of
   owned `PartitionInfo_t *` (BH:3824-3826, BH:38316-38340), freed with `kafka_List_destroy_err`,
   which frees the owned elements too (BH:40160-40171), copied through the common
   `PartitionInfo` marshal (ledger 79 4d).

| Handle | Category | Created by | Freed by |
|---|---|---|---|
| `KafkaProducer_t` / `MockProducer_t`, held as its `Producer` view | 1 | `KafkaProducer_new`; the `MockProducer_with_*_err` constructors (BH:38809-38840) | `ReleaseHandle` → `KafkaProducer_destroy_err` (BH:38787-38800) / `MockProducer_destroy_err` (BH:38855-38867), only after `CloseExplicitly` |
| the config `kafka_Map_t`, `ProducerConfig_t` | 1, short-lived | the binding | the binding, after `KafkaProducer_new` |
| `ProducerRecord_t` | 2 | the binding, per send | the binding, right after the send call |
| `Callback_t` | 2 | the binding, per send, after the record is built | the send, which consumes it on every path |
| `KafkaFuture_t` from a send | 2 | the blocking send's return; the handover `cb` | the binding, at once, unread |
| an owned `kafka_common_Error_t` | 2 | an `out_err` slot; a `_cb` completion | the error-table read, then `Error_destroy_err` |
| `kafka_Map_t` (metrics), `kafka_List_t` (partitions) | 3 | `Metrics`; `PartitionsFor`'s `_cb` completion | the reader, after copying out, in a `finally` |

**Note — classify by the accessor, not the type.** A non-`const` return, an `out_err` slot or a
`_cb` argument the header calls owned is **owned**: free it once. A `const *` return, an
`on_completion` argument and `kafka_common_Error_source_err`'s result are **borrowed**: never
free them. The same type can be either (a `kafka_List_t` the binding builds stays the binding's;
one `PartitionsFor` delivers is the reader's). Read the signature, not the prefix.

**Rule:**

  - **The client handle** (decision 6). One `SafeClientHandle` subclass per producer class,
    `ownsHandle: true`, `IsInvalid => handle == IntPtr.Zero`. Operation P/Invokes take the
    `SafeHandle` as their parameter, so the marshaller holds a call-scoped reference and a closed
    handle throws `ObjectDisposedException`. A class-only call (the `MockProducer_*` functions)
    takes `DangerousAddRefClass` and passes the stored class pointer. The destroy runs only from
    `ReleaseHandle`, with the class pointer and no `AddRef`, and only after `CloseExplicitly`;
    a handle released any other way (the finalizer) is leaked with one `Trace.TraceWarning`
    (decision 7), because the destroy blocks and runs user callbacks.
  - **Teardown order** (decision 7; `dotnet/CLAUDE.md` §4 **Close/Dispose**): refuse new calls
    (`ObjectDisposedException`), close, stop and join the `CompletionThread`, wait for the
    in-flight uses (P/Invokes in progress) to reach 0, then release, so the destroy runs on the
    closing thread and never inside `__execute_callbacks`. A `Close` called from a callback
    leaves the teardown to the `CompletionThread` after its drain. The destroy invalidates every
    view (BH:38787-38791), runs the callbacks still pending on its thread, and completes the
    unreported ones as success (`rust/src/ffi/producer/mod.rs:166-178` at `851c8d95`), so no
    report is made after it began.
  - **References are call-scoped** (ledger 79 4b). Every `DangerousAddRef` is paired with a
    `DangerousRelease` on the calling thread after the P/Invoke returns. The calls are inside the
    use-count bracket, with the three exceptions Python makes. The drain (`__execute_callbacks`)
    is a use that `Close` does not refuse: it is counted until the destroy begins, and refused
    after, because a closing producer's completions still need it (Python's `_pump_use`,
    `python/confluent_kafka/producer/_base.py:488-503` at `851c8d95`). A callback's report
    (`Producer__set_callback_result`) is not counted, and is skipped once the destroy has begun
    (Python's `_report`, `async_producer.py:628-637`). The close submits (`close_cb`,
    `close_with_timeout_cb`), made after `Close` has begun, are not counted
    (`producer.py:672-675`, `:696`; `async_producer.py:886-890`, `:920`). A `Close` from an
    un-awaited `Task.Run` inside an async callback can move the destroy onto a pool thread; that
    edge is accepted, as Python's async close from a callback tears down on a helper thread
    (`async_producer.py:908-918`). No reference is held across a `_cb` completion, and none taken
    in one call is released in a callback or a trampoline; a call made from inside one (the
    report, a close from a callback) takes and releases its own. What keeps the producer alive
    until a pending `_cb` completion fires is the teardown order (the `CompletionThread` is joined
    before the destroy, and the destroy fires what is still pending), not a reference.
  - **Transient handles** are freed in a `finally` on the thread that read them; the managed
    `RecordMetadata`, `PartitionInfo` and exceptions hold copied values, never a handle. Every
    `_destroy_err` is null-safe.
  - **Owned results** are copied out completely before the root's destroy; nothing borrowed
    from them survives it. Never destroy an element of an owned container on its own (the root's
    destroy frees it: a double free). Copy-out is the only mode: these are small metadata
    snapshots (`dotnet/CLAUDE.md` §6.4).

**Why:** a `SafeHandle` is the robust form of "destroy exactly once", even through exceptions.
Every operation takes the view, so the view is the handle; the class pointer is needed only by
the destroy and the mock's own functions (decision 6). The destroy blocks, runs pending
callbacks on its thread and invalidates every view, so it runs only on the closing thread after
the drain thread is joined (decision 7). A reference held across a `_cb` completion would be
released on the drain thread, which could then run the destroy inside `__execute_callbacks`
(ledger 79 4b).

**Anti-patterns:**

  - A destroy of the view (there is no `kafka_producer_Producer_destroy`), a destroy from the
    finalizer or inside `__execute_callbacks`, or one before the `CompletionThread` is joined
    and the uses reach 0.
  - A raw `DangerousGetHandle()` passed to an operation P/Invoke; a `DangerousAddRef` held
    across a `_cb` completion, or released in a callback or a trampoline.
  - A `SafeHandle` around a per-send handle; reading the `KafkaFuture_t` a send delivers
    (`KafkaFuture_get` runs the queued callbacks on the caller, §A6); keeping a
    `ProducerRecord_t` past the send call; destroying a `Callback_t` once the send was reached
    (the send consumed it: a double free).
  - Destroying `on_completion`'s `metadata` or `error`, or an element of an owned `kafka_List_t`
    / `kafka_Map_t` (a double free); leaking an owned root, or freeing it outside a `finally`.
  - Keeping the config `kafka_Map_t` or `ProducerConfig_t` past `KafkaProducer_new` (a leak).

**Tests required:**

  - Create and dispose many producers of both flavors: no leak, no crash; a send burst leaves
    no native handle behind.
  - Double `Dispose` is safe; a call after `Dispose` throws `ObjectDisposedException` before any
    native call.
  - With a `PartitionsFor` in flight (an unreachable broker), `Dispose` destroys on the closing
    thread, and the pending completion fires exactly once (ledger 79 4b).
  - A producer finalized without `Dispose` is leaked with one `Trace.TraceWarning` and no
    destroy.
  - `Metrics` and `PartitionsFor` free their root exactly once, on the throwing path too, and
    the copied values stay valid after it.

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

**Decision:** Pass key and value with **no intermediate copy**. On the **sync** `Send` the pin is
call-scoped (`fixed`): `Producer_send_with_callback` reads key and value during the call and
holds no reference afterwards (BH:38166-38170, which it follows). On the **async** `Send`,
`Producer_send_with_callback_cb` copies the record but **borrows its key and value until the
handover `cb` fires** (BH:38183-38191), so the pin spans `Send` → handover `cb`. Never hold a pin
past the handover, and never across the delivery future. (Managed → unmanaged: a *GC-moves*
hazard, fixed by pinning.)

**Rule:**

  - Prefer a `fixed` block (stack-scoped, no allocation) for a single send; where
    `fixed` doesn't fit, pin explicitly and release in a `finally`. Unpin right after the call —
    **never hold a pin across the returned delivery `Task`** (per-message pinned objects
    fragment the GC heap).
  - **The pinning primitive for key/value is `ReadOnlyMemory<byte>.Pin()` →
    `MemoryHandle`, NOT `GCHandle.Alloc(Pinned)`** (amended M11/P3.1). `GCHandle` pins
    *objects*, and a record's key/value is a `ReadOnlyMemory<byte>`, whose backing store
    may be an array, a string, native memory or a custom `MemoryManager`; `Pin()`
    handles all of them and exists on all three TFMs (`netstandard2.0` gets it from the
    already-referenced `System.Memory`). `GCHandle.Alloc(Pinned)` remains correct for
    buffers the binding *owns* and pins itself — the interned topic buffers and the
    empty-sentinel byte below.
  - **Async send: the pin spans `Send` → the handover `cb`.** The `cb` fires on the
    `CompletionThread` or inside the call (§A1). Release the pins in the `cb`, before the first
    stage completes, and on every path that ends without a `cb` (the P/Invoke throwing). The sync
    path stays `fixed`.
    ⚠ **Consequence on the public surface:** the async send borrows the caller's buffers until
    the first stage completes, so a mutation before then can be visible on the wire. A buffer may
    be reused once `Send` throws or its first stage has completed, in any state (ledger 73).
    Document this on the **async** surface only; the sync send has no such window.
  - Sentinels: absent and empty must stay distinct, on both paths: absent → no key / a null
    value (a tombstone); empty → a **non-null** pointer with length 0. A `fixed` over `null`
    yields a null pointer, so gate on the `null` reference, not on the pointer. Without
    serializers the record's key and value are `kafka_Bytes_t *` that the record never copies
    (`rust/src/ffi/producer/producer_record.rs:20-27` at `851c8d95`): absent is `NULL` (a null
    key, or a null value, the tombstone); empty is a non-null `kafka_Bytes_t *` whose `data` is
    non-null and whose `len` is 0 (BH:1944-1945), so the sentinel byte of the note below applies
    to `data`. The `kafka_Bytes_t` and its bytes stay valid until the send has registered the
    record: until the sync `Send` returns, so a stack `kafka_Bytes_t` is fine there; until the
    handover `cb` on the async `Send`, so there it must not live on the stack (BH:38183-38191;
    ledger 79 5c).
    **Note:** `fixed` also yields a null pointer for an **empty** (non-null)
    array — not just for `null` — and the core rejects `(null, len ≥ 0)`. So an
    **empty** key/value (`Length == 0`) must pass a **non-null** pointer — use a
    **stack sentinel byte** (guaranteed non-null), not the `fixed` null.
    (`GCHandle.AddrOfPinnedObject` returns non-null for empty arrays on current
    runtimes too, but that's **undocumented** — prefer the sentinel. Python is
    unaffected — an empty `bytes` is non-null.)
    ⚠ **A STACK sentinel is correct only for a call-scoped send.** On the async send the core
    reads the address until the handover `cb`, after `Send`'s frame may be gone — a
    use-after-free that "works" almost always and corrupts rarely, which is the worst
    failure mode. The async path must use **one process-wide, permanently pinned
    1-byte static** instead (M11/P3.1 §4.2). Test it as a *stability* property — the same
    address for a record's key and value, and across separate calls at different stack
    depths — because that is exactly what a stack sentinel cannot satisfy and an
    "it sent successfully" assertion cannot detect.
  - **The topic is read when the record is built.** `ProducerRecord_new*` takes a NUL-terminated
    `topic` and returns an owned record (BH:39527-39539), so the topic needs only a call-scoped
    `Utf8Marshal.Pin` around that call, on both paths. No interned topic cache.

**Why:** the borrow ends when the sync call returns, or at the async handover `cb`, so a pin
of exactly that span is sufficient; Task-scoped pinning is unnecessary and fragments the heap.
Mirrors confluent-kafka-dotnet (pin around `produceva` with `MSG_F_COPY`, `Free`
in `finally`). Any intermediate copy (`AllocHGlobal`+`Copy`, `ToArray()`) is the
per-message allocation CLAUDE.md §12 exists to prevent.

**Anti-patterns:**

  - Any intermediate copy of key/value; keeping the pin alive until the `Task`
    completes; forgetting to unpin (a permanent pin).
  - Conflating empty (`0`) with absent (`-1`); mutating/resizing a pinned array.

**Tests required:**

  - **Mutation-after-send**, per surface. Sync: mutate the caller's `byte[]` right after `Send`
    returns, and the produced record is unchanged. Async: a mutation before the first stage
    completes is memory-safe (the pin), and a mutation after it completes leaves the produced
    record unchanged.
  - **Allocation budget** (DoD §10): a large value adds no value-sized managed
    allocation. Take the **best of N matched attempts** rather than widening the budget,
    so a real per-send regression (which raises every attempt) still fails.
  - Absent vs empty key/value each produce the correct record — and, on the async send,
    that the empty sentinel's address is **stable across calls and stack depths**.
  - **Pin/unpin balance on every path**, including the failure paths (the P/Invoke throwing,
    a handover `cb` carrying an error, and a refused record whose `cb` fires inside the call: a
    `Send` racing `Close`). A pinned `GCHandle` is a strong root, so a leak is observable
    without any pin-counting API: hand each send a buffer nothing else references and assert
    it becomes collectable.

---

## A5 Error model (operational vs. precondition)

**Decision:** Two surfaces. **Core (operational) errors** cross as a
`kafka_common_Error_t` handle → the error table's type (decision 14): `KafkaException`
(code, retriable, message) or a .NET built-in, mirroring the Python sibling. **Precondition errors** (bad
argument / state) are validated in the binding *before* the FFI call and raise
standard .NET exceptions — never `KafkaException`.

**Rule:**

  - **Operational:** The error arrives in `out_err` (§0.1) and is read once by the
    error-table helper; an owned handle is freed after the read;
    `kafka_common_Error_source_err` is borrowed.
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

## A6 Callback & delegate marshalling (the `_cb` completions; the per-record `Callback_t`)

**Decision:** Since M19/P1 two producer callback forms cross the ABI. The core calls both inside
`Producer__execute_callbacks`, on the `CompletionThread` (§A1), with two exceptions: the async
`Send`'s handover `cb` fires inside the call, on the caller's thread, when the producer takes or
refuses the record at once (BH:38240-38249); and the producer's destroy runs the callbacks still
pending on the thread that destroys it (`rust/src/ffi/producer/mod.rs:166-178` at `851c8d95`),
which decision 7 makes the closing thread, or the `CompletionThread` after its drain. On
`AsyncKafkaProducer` a completion queued behind a running async callback is handled later, on the
pump, outside `__execute_callbacks` (ledger 54, B9; the async delivery callback paragraph at the
end of this section).

  - **The `_cb` completions:** the async `Send`'s handover `cb`, `flush_cb`, `close_cb` /
    `close_with_timeout_cb`, the transaction `_cb`s, `partitions_for_cb`. Each fires once with the
    `opaque` the binding passed. No registration handle.
  - **The per-record `Callback_t`** (decision 10): `Callback_new_err(self_, on_completion)` for
    every sent record, once its `ProducerRecord_t` is built, passed to `send_with_callback` /
    `send_with_callback_cb`, which consume it. `on_completion(self_, metadata, error,
    callback_id)` fires once, later, and the binding reports its end through
    `Producer__set_callback_result`. A plain `Send(record)` passes one too: it is what completes
    the record's future; only the user callback is absent.

`RecordMetadata_copy` (the old form A) does not exist at `0035d9ff`, and the managed-only delivery
callback of M14/P1 (the old form C) is replaced by `Callback_t`.

**Rule (both forms):**

  - A named delegate type, `[UnmanagedFunctionPointer(CallingConvention.Cdecl)]`, blittable
    parameters, held in a `static readonly` field so the GC cannot collect it while native holds
    the thunk (netstandard2.0 has no `UnmanagedCallersOnly`).
  - The body is a **no-throw boundary**: `try/catch` all of it. A managed exception unwinding into
    Rust is UB.
  - The `opaque` / `self_` is a `GCHandle` over the per-call state (`ToIntPtr`, recovered with
    `FromIntPtr`).

**Rule (`_cb` completion):**

  - The `cb` frees its `opaque` `GCHandle`, exactly once. The only other free site is a `catch`
    around the P/Invoke, when it threw before native ran.
  - The `cb` frees what it owns (§A2) and completes its `Completion` or `TaskCompletionSource`
    (§A7).
  - The async `Send`'s handover `cb` releases the key and value pins (§A4) once. Inside the call,
    or on the pump before `Send` has looked, it only records the outcome, and `Send` completes the
    first stage from it and ignores the token. Otherwise it completes the pending stage: Canceled
    with the caller's token if that token has fired, else with the outcome (§A1).

**Rule (`Callback_t`, the delivery callback):**

  - **One `Callback_t` per record, created after the record is built, and never destroyed by the
    binding.** The send consumes it on every path, success, error or a closed producer
    (`take_callback`, `rust/src/ffi/producer/callback.rs:185-194`, called first by
    `delivery_callback`, `rust/src/ffi/producer/mod.rs:1800-1816`, both at `3f8a5bc6`). Build the
    `ProducerRecord_t` first, then `Callback_new_err`, then send, as Python does
    (`python/_confluentkafka.c:1680-1702`, `:1780-1830` at `3f8a5bc6`), so no path holds a
    `Callback_t` that no send consumed. If the record cannot be built, no `Callback_t` exists and
    the record's state was never registered: release it at once. If `Callback_new_err` fails, no
    handle exists: release the state and destroy the record. Never share one across records:
    `self_` is the only per-record identity, and `callback_id` appears only when the callback
    fires.
  - **`self_` lifetime** (decision 10): a `GCHandle` over the record's state (the user callback,
    if any, and the record's `KafkaFutureImpl`). Keep it until `on_completion` fires, and free it
    there, after reporting. After a send error (`out_err` set, or a handover `cb` with an error)
    keep it until the producer's destroy has returned, then free it: the callback may still fire
    once if the record reached the accumulator (BH:38221-38227, BH:38251-38254).
  - **`metadata` and `error` are borrowed for the call** (BH:3860-3869). Read them; never destroy
    them. `metadata` is non-null on failure too: the record's placeholder, offset -1
    (`rust/src/ffi/producer/callback.rs:29-30` at `0035d9ff`).
  - **Order** (decision 36): run the user delivery callback, if there is one; then complete the
    record's `KafkaFutureImpl` (`Complete` / `CompleteWithError`, which releases every `Get` and
    runs the `WhenComplete` actions); then report with
    `Producer__set_callback_result(view, callback_id, NULL, error)`. On the async producer, a
    completion handled by the chain (queued behind a running async callback, or the callback's own
    end) follows the async form's order (`dotnet/CLAUDE.md` §4, **The async form**): callback,
    report, then `Complete`.
  - **The user callback's `exception`** is the error table's type (decision 14, ledger 33):
    `Exception?`, and the same instance for a `local_callback` error, so an `ErrorNext(Exception)`
    exception reaches the callback unchanged (decision 15).
  - **A throwing user callback** (decision 20): catch it, report it through
    `__set_callback_result` as a `local_callback` error carrying the exception (the core logs it
    and leaves the record's outcome unchanged, BH:38215-38216), and write one `Trace.TraceError`.
    The trace call is itself no-throw (a host `TraceListener` can throw). The record's future
    still completes with the record's outcome.
  - **Once per record the core accepted, unconditionally:** a record whose `Get` wait was
    abandoned (a token or a timeout) still gets its callback. A record the send refused fires it
    only if it had reached the accumulator (BH:38221-38227, BH:38251-38254); a record that `Close`
    fails while it waits fires none (ledger 70; the core drops the callback with the rethrown
    `KafkaException`, as Java's `doSend` does, `rust/src/producer/kafka_producer.rs:2420-2424` at
    `851c8d95`). Report exactly once per `callback_id`, on every path, and never once the
    producer's destroy has begun: it completes every unreported callback as success
    (`rust/src/ffi/producer/mod.rs:174-178`).
  - The per-record state is the only managed allocation the callback adds per send: no closure,
    no captured public record (DoD §10).

**Why:** the floor (netstandard2.0/net462) has no function pointers or `UnmanagedCallersOnly`, so a
rooted Cdecl delegate is the only portable mechanism, as in confluent-kafka-dotnet. `self_` is
the only per-record identity, so the binding needs one `Callback_t` per record (decision 10). The
order is Java's (`ProducerBatch`: value set, callbacks, then `done()`) and Python's (`_complete`:
the callback, then `complete_record`) (decision 36). Every callback now runs on a thread the
binding chooses (the Decision above), never on a native thread of the core's, so the
foreign-thread rules are gone; the no-throw rule stays because the frame below is still Rust.

**Anti-patterns:**

  - A delegate with no rooted reference, or a bare inline lambda.
  - An exception escaping a callback into native.
  - Destroying the `metadata` or `error` handed to `on_completion`: they are borrowed, so that is a
    double free.
  - Freeing `self_` when the send call returned an error (the callback may still fire: a
    use-after-free), or never freeing it after destroy (a leak).
  - Destroying a `Callback_t` after the send was reached (a double free: the send consumed it),
    creating it before the record is built, or one `Callback_t` for several records.
  - Completing the future before the user callback runs (decision 36), or skipping the callback
    because no awaiter is left.
  - Reporting a throwing callback only to `Trace`, or only through `local_callback`
    (decision 20).
  - Reading the returned `KafkaFuture_t *` with `KafkaFuture_get` (it runs the queued callbacks on
    the caller, and from inside a callback it waits forever, BH:38216-38219). Free it at once.

**Tests required:**

  - Every behavioural test runs against **both** producer flavors.
  - Exactly once per record, asserted after a settle window, including an N-record burst.
  - Order, with a deterministic probe: from inside the user callback, read the record future's
    state; it is not yet done.
  - The failure path delivers non-null placeholder metadata (offset -1) and the exception, with
    its type, code and message asserted (DoD §3).
  - A send error from the call keeps `self_` alive until destroy; if the core fires the callback
    later (a record appended before `maybeAddPartition` fails, BH:38221-38225), it fires once.
  - `ErrorNext(exception)`: the same instance reaches the callback and `Get` (decision 15).
  - A throwing user callback: reported through `local_callback` (the future keeps the record's
    outcome), one `Trace.TraceError` seen by a `TraceListener`, and the other records unaffected.
  - Aggressive GC while sends are in flight does not collect a delegate.
  - `GCHandle` balance: after a burst and `Dispose`, every per-record state is collectable (no
    leak), and none is freed early (no crash under GC stress).
  - Allocation budget (DoD §10): the plain `Send` and the callback `Send`, each budget set from a
    measurement, its sensitivity checked by injecting a second per-send allocation.

**The async delivery callback (M19/P1, ledger 47).** `IAsyncDeliveryCallback` uses the sync
callback's per-record `Callback_t` and `self` `GCHandle`; nothing else crosses the boundary.
The trampoline copies the borrowed metadata and error before it returns, calls the callback on
the pump, and does not wait for its `Task`. Each step after the `Task` ends runs on the pump:
report through `kafka_producer_Producer__set_callback_result` (`NULL`, or a `local_callback`
error whose opaque is the exception's `GCHandle`); complete the record's future; handle the
next queued completion, whatever its callback. A completion queued behind a running async
callback is copied by the trampoline and reported when its turn comes. Exactly one report per
callback, on every path. A report after teardown is skipped: destroy has already completed it
as success.

---

## A7 Async / Future completion → `KafkaFutureImpl`, `Completion`, `TaskCompletionSource`

**Decision (M19/P1):** one completion model. The pull-pump (the old Option A) and the push
dispatcher (the old Option B) are both gone with the pre-`0035d9ff` ABI.

  - **A record's outcome:** `on_completion` (§A6) completes the record's `KafkaFutureImpl`.
    `KafkaFuture<RecordMetadata>` (sync) and `AsyncKafkaFuture<RecordMetadata>` (async) are views
    over it (`dotnet/CLAUDE.md` §4 futures). The owned `KafkaFuture_t *` a send returns is freed at
    once and never read.
  - **Sync waits** (decisions 4, 4b): `Send` is the blocking `send_with_callback` (§A1). `Flush`,
    the five transaction ops, `PartitionsFor` and `Close` call their `_cb` form and wait on a
    managed `Completion`. The blocking forms are not used for them, because they run the queued
    delivery callbacks on the caller's thread while they wait (BH:38278-38281), which Java never
    does.
  - **Async waits:** the same `_cb` forms, each completing a `TaskCompletionSource` built with
    `RunContinuationsAsynchronously`. The async `Send`'s first stage completes at the handover
    `cb` (§A1).

**Rule:**

  - A `_cb` completion runs on the `CompletionThread`, or inside the call (§A1). It frees its
    handles (§A2), then completes its `Completion` or TCS exactly once. No continuation runs on the
    `CompletionThread`: a TCS uses `RunContinuationsAsynchronously`, and a `Completion` waiter
    wakes on its own thread.
  - **In-callback rules** (Python's, plan §3.1 fact 3), on `KafkaProducer` and
    `AsyncKafkaProducer` (the mocks: below), for a wait called from inside a delivery callback: on
    the `CompletionThread`, or, from an `IAsyncDeliveryCallback`, after an `await` too, through an
    `AsyncLocal<bool>` flag set around `OnCompletion` and reset when it returns (Python's
    `_in_async_callback`, `python/confluent_kafka/producer/async_producer.py:138-139`, `:597-601`
    at `851c8d95`; ledger 80, pick 4):
      - `Flush` throws at once a `KafkaException` with Java's message "KafkaProducer.flush()
        invocation inside a callback is not permitted because it may lead to deadlock."
        (`KafkaProducer.java:1299-1300`). Python checks this itself, before calling native
        (`python/confluent_kafka/producer/producer.py:593-595` at `851c8d95`); the core's
        `flush_cb` reports the same error from a `KafkaProducer`'s callback (BH:38283-38287,
        BH:38298-38313). The mocks follow Java's mock as far as the core allows:
        `MockProducer.flush()` makes no in-callback check, so the `Flush` throw is
        `KafkaProducer`'s and `AsyncKafkaProducer`'s only. From inside a mock's sync delivery
        callback that the drain runs, the mock calls the **blocking** forms: the core sees the
        callback and does not wait for the reports, and a nested drain returns 0, so no other
        callback runs on the caller (BH:38283-38287; `rust/src/ffi/producer/mod.rs:396-406`,
        `rust/src/ffi/callback_queue.rs:290-294` at `851c8d95`). From a mock callback the core
        cannot see (a sync callback the async chain resumes, an async callback's synchronous part
        queued behind another, an async callback after an `await`), the mock's `Flush` runs
        Java's flush body: it calls `MockProducer_complete_next_err` until that returns false,
        then returns without waiting for the reports (`MockProducer.java:364-365`; Python's
        `_flush`, `python/confluent_kafka/producer/_mock_core.py:445-451`, and the async mock's
        skip of its callback wait, `async_mock_producer.py:177-184`, at `851c8d95`). The core
        queues each completed record's delivery callback (BH:38957-38968) where Java's mock runs
        it inline, as Python's async mock schedules it; it runs after the calling callback
        returns. `Close` from any mock callback returns at once and leaves the teardown to the
        `CompletionThread`, as on the real producer. From an async mock callback after an
        `await`, a commit or an abort keeps the flag: it waits with the `max.block.ms` deadline
        and ends with the code-7 timeout below. That is a recorded deviation from Java's and
        Python's mocks, which only a `_cb` form that skips the report wait would remove.
      - A queued wait waits at most `max.block.ms` (read from the config dictionary, default
        60000): from a sync callback `PartitionsFor` and the five transaction operations, whose
        completion is queued behind the callback that waits; from an async callback after an
        `await`, a commit or an abort, which waits for the callback's own report
        (`with_txn_control_cb` passes `in_callback = false`, `rust/src/ffi/producer/mod.rs:992`
        at `851c8d95`). It then throws a retriable `KafkaException` with code 7, Kafka's
        `TimeoutException` (ledger 79 5d; Python's `KafkaTimeoutError`,
        `python/confluent_kafka/producer/_base.py:470-472`), not `System.TimeoutException`,
        which decision 14 keeps for -5. The message is Python's: from a sync callback
        "Timeout expired after {ms}ms while awaiting {operation} from a send() callback, which
        blocks the producer's completion thread" (`_base.py:130-134`); from an async callback
        "Timeout expired after {ms}ms while awaiting {operation} from an async send() callback,
        whose own report it waits for" (`async_producer.py:150-152`). The operation may still
        take effect after the timeout.
      - `Close` from a callback overrides a non-zero timeout to 0, with Java's warning
        ("Overriding close timeout … to 0 ms in order to prevent useless blocking due to
        self-join …"), queues `close_with_timeout_cb(0)`, and leaves the teardown to the
        `CompletionThread` after its drain (`dotnet/CLAUDE.md` §4 **Close/Dispose**).
  - **Which waits wait for the delivery reports** (`ReportWait`,
    `rust/src/ffi/producer/mod.rs:482-509` at `851c8d95`): `Close`, timed or not, always,
    without a bound; `Flush`, `CommitTransaction` and `AbortTransaction` when they succeed;
    `InitTransactions`, `BeginTransaction` and `SendOffsetsToTransaction` never.
  - **No own-thread guard** (decision 39). A `Get()` on the `CompletionThread` for a future that
    only that thread can complete waits, as in Java and Python; `Get(TimeSpan)` and
    `Get(CancellationToken)` end as usual.
  - Cancellation is best-effort: a token never cancels the operation. `Get(token)` ends only the
    caller's wait; the `Send` token ends the first stage only at the hand-over (§A1, ledgers 19
    and 71).
  - `Close` / `Dispose` order: `dotnet/CLAUDE.md` §4 **Close/Dispose** (refuse new calls, close,
    join the `CompletionThread`, wait for in-flight uses, destroy on the closing thread).

**Why:** Java completes a record's future on its I/O thread and never runs callbacks on the
thread that calls `flush()`; the `_cb` forms plus a managed wait give exactly that, and Python
chose them for the same reason. `RunContinuationsAsynchronously` keeps an awaiter's continuation
from stalling every later completion.

**Anti-patterns:**

  - `Task.Run(get)` per send, or any one-thread-per-message pattern.
  - A sync `Flush`, transaction op or `Close` that calls the blocking form (delivery callbacks would
    run on the caller), except a mock's call from inside a sync callback the drain runs (above),
    where a nested drain runs none.
  - A mock `Flush` from a callback the core cannot see that waits for the reports: it waits for the
    calling callback's own report. It completes the pending sends and returns (above).
  - A TCS without `RunContinuationsAsynchronously`; awaiting user code on the
    `CompletionThread`; letting a user exception escape into the drain loop.
  - A sync wait from a callback with no `max.block.ms` deadline: it can never complete, because its
    `cb` is queued behind the callback that waits.
  - Destroying the producer inside `__execute_callbacks`, or before the `CompletionThread` is
    joined; holding a managed lock across a P/Invoke the `cb` needs.
  - An own-thread `Get()` guard (decision 39 removed D9).

**Tests required:**

  - A record's future resolves with `RecordMetadata`, or faults with the error table's type.
  - `Flush`, the transaction ops, `PartitionsFor` and `Close` complete through their `_cb` forms
    on both flavors, and run no delivery callback on the caller's thread.
  - The in-callback rules, on `KafkaProducer` and `AsyncKafkaProducer`, from a sync callback
    and from an async one after an `await`: `Flush` throws Java's message; a queued wait ends
    after `max.block.ms` with a retriable `KafkaException`, code 7, and the matching Python
    message (a short `max.block.ms` in the test); `Close` returns at once and the teardown runs
    after the drain. On the mocks, the mock rules in the `Flush` bullet above. A mock's `Flush`
    from a callback the core cannot see completes every pending send and returns at once, with no
    `max.block.ms` wait and no code-7 timeout; the completed sends' delivery callbacks run after
    the calling callback returns.
  - `Dispose` with sends in flight returns (does not hang).
  - A token ends only the wait and frees nothing early; high-concurrency produce does not starve
    the thread pool.

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
| `Consumer_t` | 1 — client (`SafeHandle`) | `Dispose`: `Consumer_close_with_timeout` → `Consumer_destroy` (`DisposeAsync`: `Consumer_close_async` → `Consumer_destroy`); **close before destroy** because destroy is not a graceful close — it does **not** join the bg task (since master #233 it does wake and wait for an in-flight `_async` op). ⚠ The destroy is **ref-counted**, so it is immediate only when no op holds a count — see Rule; §B7 + Rule |
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

    On path 3, since master #233, `Consumer_destroy` sees that it is on its own
    dispatcher thread and moves its wait and its free to a new
    `kafka-consumer-destroy` thread, returning at once
    (`rust/src/ffi/consumer.rs:736-744`). If that thread cannot be started, the core
    leaks the handle and logs an error.

    ⚠ **Paths 4 and 5 can run the destroy on a thread that is neither the caller's
    nor the dispatcher's.** The safety argument for the deferred paths therefore
    rests on §B6's **first** clause — the ref-counted `SafeConsumerHandle`, which
    makes it impossible for a destroy to run *concurrently with* an operation —
    and **not** on the second clause about the dispatcher being serialised. A
    thread-identity argument covers path 3 only. See §B6.

  - `SafeConsumerHandle : SafeHandle` — `ownsHandle: true`, `IsInvalid => handle
    == IntPtr.Zero`. The release path is **not** a bare destroy: `Consumer_destroy`
    is not a graceful close, and it does **not** join the bg task (the graceful
    join is `Consumer_close` / `await_join`, not destroy). Since master #233 it wakes
    any in-flight `_async` op and waits for its task, so that op's callback still
    fires once, typically with a `WAKEUP` error, possibly after destroy returns
    (`rust/src/ffi/consumer.rs:788-801`; before #233, `shutdown_background`
    cancelled such ops and their callbacks never fired). It does **not** wait for a
    synchronous call still running on another thread. The binding relies on neither
    behaviour: the ref-count (below) means no op is in flight when it runs. So
    teardown routes through the graceful **close before destroy**: `Dispose` =
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
(§B4, CLAUDE.md §6.4) safe. `Consumer_destroy` not being a graceful close is why
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
cancel-then-join that this layer cannot provide. (Since master #233 the core's
`Consumer_destroy` does wake and join in-flight `_async` ops, but it still does not
wait for a synchronous call in progress — the window M9/P4 H1 closed — so the
ref-count is still what keeps teardown safe.)

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
> pursued and not tracked**. Since master #233 the core's `Consumer_destroy` wakes
> and joins in-flight `_async` ops, but not a synchronous call in progress. That
> changes neither residual — the ref-count still defers the destroy past `Dispose`,
> and the deferred destroy is still bare — and the binding does not rely on it.

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
`kafka_common_Error_t` handle → the error table's type (decision 14): `KafkaException`
(code, retriable, message) or a .NET built-in, mirroring the Python sibling. **Precondition errors** (bad
argument / state) are validated in the binding *before* the FFI call and raise
standard .NET exceptions — never `KafkaException`. The consumer adds two more
shapes: **wakeup** and **concurrent use**.

**Rule:**

  - **Operational:** The error arrives in `out_err` (§0.1) and is read once by the
    error-table helper; an owned handle is freed after the read;
    `kafka_common_Error_source_err` is borrowed. `KafkaException.FromHandle` reads the
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
  - **The core's local codes map by the error table (decision 14, M19/P1).** -2 and -4 →
    `InvalidOperationException`, -3 → `ArgumentException`, -5 → `System.TimeoutException`,
    each with the core's message; a `local_callback` error → the carried instance; every
    other code → `KafkaException`. This holds on every surface, the `ConsumerHandle`
    included, so a handle op and the same consumer op throw the same type
    (`HandleAndConsumer_ReportTheSameCoreError_Identically` keeps asserting it). It
    replaces the M9/P8 rule that kept `illegal_state` a `KafkaException`.
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
  - Any type other than `InvalidOperationException` for a concurrent call (-2), a state
    read or an op, sync or async (decision 17).
  - Treating `UnsupportedVersionError` on a mock-derived `ConsumerHandle` as a bug to
    route around (a managed pre-check, a silent no-op, an invented "mock
    unsupported" exception) — it is documented core behavior, and the binding's job
    is to surface it unchanged.
  - Branching on an error code outside the one error-table helper, or mapping a code the
    table does not list.
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

**Decision:** Four callback *families* cross this boundary, and they have
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
    and only for this family.
    **Admin's single-callback group RPCs belong here too (M15/P13.1)** —
    `alterConsumerGroupOffsets` / `deleteConsumerGroupOffsets` /
    `removeMembersFromConsumerGroup`. Each fires **exactly once** per submit — also for
    an empty request, in `removeAll` mode, and inline on the submitting thread when the
    RPC cannot be submitted — with either an **owned result root** or an **owned**
    whole-call error. The root's per-key errors are **borrowed**
    (`_get_error(i)` → `FromBorrowedHandle`, never destroyed) while its `_all` is
    **owned** (`FromHandle`); the root is destroyed once, after every read, and the
    callback's `finally` is the **sole** `GCHandle` free — no countdown and no submit
    token. The hookless Rule below, written for the consumer's eight, holds for them.
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
  - **N-shot per-operation fan-out — admin's per-key ABI ("shape 4", M15/P9).** One
    submit, **N independent callbacks** that can run concurrently on different threads,
    each carrying one key's **owned** value and **owned** error — there is no result
    root to borrow from, so both go through `FromHandle` / their own `_destroy`, never
    `FromBorrowedHandle`. Sub-shapes follow **Java's return type**, not the accessor
    set: **4a** value-carrying → one `Task` per key; **4b** void → one `Task` per key,
    where a null error *is* the success value. (M15/P14: read 'one `Task` per key' as
    'one completion per key': a `TaskCompletionSource` with
    `RunContinuationsAsynchronously` on the async flavour, or a `SyncCompletion` latch on
    the sync flavour, chosen by the operation's `AdminFlavour`. `AdminCallbacks` and the
    countdown do not depend on the flavour.) Hookless, so the callback family still
    owns the `GCHandle` free — but as a **countdown released by the last of N**, armed
    at `n + 1` with a submit-owned token (which is what makes `n == 0` releasable at
    all), where `n` is read from that RPC's own ABI doc comment and is **not**
    uniformly the key count. Each callback resolves **its own key's** `Task`; the
    countdown decides only when the operation is released and settles, at zero, any
    requested key no callback answered — it assembles no aggregate.
    ⚠ **Sub-shape 4c (per-key fan-in → one aggregate `Task`) is retired (M15/P13.1).**
    Its three RPCs moved to the single-callback ABI and now belong to the first family
    above; the fan-in bridge was deleted with them. A Java `KafkaFuture<Map<K, V>>` now
    has one ABI shape here — one callback over a result root — not a countdown over
    per-key callbacks.

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
  - **Converting a shape-4 countdown RPC to a single callback (M15/P13.1):** a
    leftover countdown armed at **≥ 2** is an **equivalent mutant** — it never reaches
    zero, the callback's `finally` frees once, and nothing leaks, double-frees or is
    observable — so replaying the old submit lines proves nothing on those rows. The
    discriminating test is a **count-0 / pending** row: a seam that does not fire, then
    assert the operation is still pending and rooted after the submit returns (and past
    the client's `Dispose`). The mutation `SetPendingCallbacks(0)` +
    `ReleaseSubmitToken()` frees the handle before the callback and turns that row red.

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
    single-sourced.) The two facts §B7 adds on top: `Consumer_destroy` is not a
    graceful close at the ABI (it does **not** join the bg task; since master #233
    it wakes and waits for any remaining in-flight `_async` op, whose callback still
    fires once, §B2), and since M9/P4 H1 the binding rarely reaches it directly at
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
  - A sync admin Result composed over Tasks (`Task.WhenAll`, an `async` projection,
    `ContinueWith`) and then blocked on (`GetAwaiter().GetResult()`, `.Wait()`,
    `.Result`). This is sync-over-async: with `RunContinuationsAsynchronously` the
    composite needs a thread-pool thread to complete, so a saturated pool starves it.
    Compose over `SyncFutureSource` instead (`ProjectedSyncFuture`, `AllOfSyncFuture`,
    `GatheringAllOfSyncFuture`), which runs on the thread that calls `Get()` (M15/P14).

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
