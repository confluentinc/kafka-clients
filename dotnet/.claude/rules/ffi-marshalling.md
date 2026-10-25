# .NET binding — FFI marshalling & ownership

Deep-dive rules for marshalling across the C ABI (`confluent_kafka_bindings.h`,
`kafka_producer_*` / `kafka_common_Error_*`) from .NET via P/Invoke.
Supplements `bindings/dotnet/CLAUDE.md`. Review ground truth is the **C ABI
header** and the **Kafka Java public API**.

Each section leads with **Decision** (the intent, one line), then **Rule**,
**Why**, **Anti-patterns**, and **Tests required** (`consumer-threading.md` precedent).

## How this file is organized

Four parts, so each client reads as one complete, self-contained story:

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
  - **Part C · Admin** — what the admin client adds: its drain thread, futures and
    teardown (§C1).

**Parts A and B are each self-sufficient** — the shared marshalling mechanism
(strings, errors, callbacks) is written out *in full in both parts*, tailored to
that client, so you never cross-reference the other client to understand one.
Redundancy is intentional. Read Part 0 once, then A or B straight through; each
section describes only that client's decisions. Part C is the exception: the admin client
marshals strings and errors as Part A does (§A3, §A5), so it states only its own decisions.

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

The names match Python's (decision 26). The per-client rules are in §A1/§A7
(producer), §B1/§B7 (consumer) and §C1 (admin). Teardown is in `dotnet/CLAUDE.md` §4 (**Close/Dispose**); the consumer's
routes are in §B2's path list.

**Why:** Java's single-Selector NIO model on tokio (CLAUDE.md §8) — unlike
librdkafka (a thread per broker + a mandatory `rd_kafka_poll` loop, which
confluent-kafka-dotnet services with a `LongRunning` `callbackTask`). So our
native thread count is independent of cluster size, there is no poll loop (a drain thread sleeps until the notify hook signals it), and
the multi-thread runtime is what makes `block_on` from .NET deadlock-free. This
applies to every client.

**Anti-patterns (every client):**

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
    `TaskCompletionSource` or `KafkaFutureImpl`. Two exceptions, as in Java and Python: a mock
    send left uncompleted at `Close` stays pending (`Dispose` flushes first); and a record whose
    batch a produce response did not complete, still in flight when a graceful `Close` ends, is
    dropped by the core without a delivery, so its second stage stays pending
    (`rust/src/producer/internals/sender.rs:1379-1400` at `9bff36d1`).
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

> **⚠ Standing note — who frees what (M19/P1).** The pre-`0035d9ff` ABI freed a
> registration's `GCHandle` from a `user_data_destroy` release hook. The header has
> no such hook at `3f8a5bc6`, so the binding decides every `GCHandle` free itself, by
> the lifetime rule of the callback's family (§B6):
>
>   1. a **`_cb` completion**: one-shot; its `cb` frees its `opaque`;
>   2. a **multi-shot registration** (the rebalance listener): `self_` lives across N
>      fires, until the next `subscribe_*`, `unsubscribe` or the consumer's destroy
>      releases it (BH:32328-32332 at `3f8a5bc6`), and decision 23 says when the
>      binding frees it;
>   3. a **one-shot registration** (the offset-commit callback, including the no-op
>      one a `ConsumerHandle` commit passes, ledger 104): `self_` lives until
>      `onComplete` fired, or until the consumer's destroy (BH:33693-33696 at
>      `3f8a5bc6`). A commit that returned an error may never fire it, so its
>      `GCHandle` is kept until destroy has returned, as Python parks it
>      (`python/_confluentkafka.c:3881-3885` at `3f8a5bc6`).
>
> Before applying a "the callback frees it" sentence, check the family: applied to a
> registration it is a use-after-free on fire 2..N, and applied to a commit callback
> it leaks the `GCHandle` when the commit failed and `onComplete` never fires.

## B1 Thread model (consumer)

**Decision (M19/P1, decision 27):** Per real `KafkaConsumer`, the core owns its
background task (`ConsumerNetworkThread`) over a single async Selector, on the
consumer's own tokio runtime. There is no native callback-dispatcher thread any
more. A blocking form runs the listener and commit callbacks it triggers inline on
the calling thread; a `_cb` form queues them, and its own completion, for
`Consumer__execute_callbacks` (`rust/src/ffi/consumer/mod.rs:48-62` at
`3f8a5bc6`).

  - **`KafkaConsumer` and `MockConsumer` (sync): no .NET thread.** Each method calls
    the blocking form, so its listener and commit callbacks run on the caller's
    thread inside the call, as Java does (BH:29215-29218 at `3f8a5bc6`). Each
    blocking call first drains `__execute_callbacks` once, to run anything an
    earlier `_cb` left queued. This is a recorded deviation from Python's
    mechanism, the `_cb` form plus a drain in 100 ms slices
    (`python/confluent_kafka/consumer/_base.py:970-976` at `3f8a5bc6`): Python's
    reasons (Ctrl+C between bytecodes, its interpreter lock) do not apply to .NET,
    where `Wakeup()` interrupts a wait.
  - **`AsyncKafkaConsumer` and `AsyncMockConsumer`: one drain thread,
    `confluent-kafka-consumer-pump`**, woken by the notify hook. It runs every `_cb`
    completion and every listener and commit callback. Continuations run with
    `RunContinuationsAsynchronously`, never on it.

```
   .NET (managed)                               │ C ABI │  Rust core (native, per consumer)
   ─────────────                                │       │  ─────────────────────────────────
   KafkaConsumer caller:                        │       │  tokio multi-thread runtime
     __execute_callbacks (drain once) ──────────│──────►│
     Consumer_poll(view, timeout, &err) ────────│──────►│    ConsumerNetworkThread:
       listener / commit callback, inline ◄─────│◄──────│      NetworkClient + ONE async Selector
   AsyncKafkaConsumer caller:                   │       │
     Consumer_poll_cb(view, timeout, cb, op) ───│──────►│    callback queue
     return tcs.Task                            │       │      empty → non-empty: notify hook
   confluent-kafka-consumer-pump:               │       │
     wait for the signal; __execute_callbacks ──│──────►│    runs cb / listener / commit callback
       cb → tcs.Set* (RunContinuations-         │       │
         Asynchronously)                        │       │
   Close/Dispose: see §B2's path list           │       │
```

**Rule:**

  - Native side (per real `KafkaConsumer`): the core runs the consumer's
    **background task** (single `ConsumerNetworkThread`, consumer-threading §10)
    over ONE async Selector (all brokers multiplexed, NOT thread-per-broker),
    created in `KafkaConsumer_new` and torn down by `KafkaConsumer_destroy_err`.
  - .NET side: the sync consumers add no thread; the async consumers add the one
    drain thread. **No poll loop**: the core self-drives, and the drain thread sleeps
    until the notify hook signals. The hook only signals (BH:3387-3391 at
    `3f8a5bc6`).
  - **One operation at a time** per consumer, **single-owner / not thread-safe**, checked
    twice (ledger 136): first the **binding's owner check**, below; behind it the
    **core's single-owner flag**. The blocking form holds the flag while it runs; the
    `_cb` form holds it until its op ends and releases it just before it queues the
    completion; the getters Java guards with `acquire()` (`assignment`, `subscription`,
    `paused`, `currentLag`, `groupMetadata`) hold it for the call
    (`rust/src/ffi/consumer/mod.rs:28-46`, `:316-325`, `:678-714` at `3f8a5bc6`).
    The flag is not reentrant: a call that reaches the core while it is held (an
    overlapping call of the thread that holds the use, or a call from a callback the
    holding op runs) fails at once with `LOCAL_CONCURRENT_MODIFICATION` (-2) and the
    message "KafkaConsumer is not safe for multi-threaded access.": a blocking form or a
    getter through `out_err`, a `_cb` form through its `cb`, queued like any completion.
    Both surface as `InvalidOperationException` with the core's message, sync and async,
    state reads included (decisions 14 and 17, §B5). ⚠ **The one way back into a
    consumer from its own callback is `ConsumerHandle`**, below; inside a callback the
    consumer's own methods route to it (ledger 114).
  - **The owner check** (ledger 136), as Java's `acquire()`
    (`AsyncKafkaConsumer.java:2219-2228`) and Python's use table
    (`python/confluent_kafka/consumer/_base.py:397-416`, `:441-458` at `3f8a5bc6`). On
    `KafkaConsumer` and `AsyncKafkaConsumer` every call but `Wakeup()`, `Handle()` and
    `Dispose` / `DisposeAsync` is one **use** of the thread that makes it, `Metrics()`,
    `Close` and the state reads included. A call while another thread holds a use throws
    `InvalidOperationException` with the core's message before anything reaches the core,
    as Python's `_use()` raises `ConcurrentModificationError` with it (`_base.py:123`,
    `:410-412`); the same thread nests. A call made inside a callback (ledger 113's flag) is
    not checked, wherever ledger 114 sends it: on the async consumers a callback runs on the
    drain thread while the call that delivers it still holds its use for its caller's thread,
    where Python's callbacks run on that thread and nest (`_base.py:1505-1508`). The check
    comes after the closed check, as in Python (`_base.py:401-402`):
    `ObjectDisposedException` once the consumer is closed or while a `Dispose` tears it
    down. An explicit `Close` is checked like any call; once it passes, its thread is the
    closing thread until the close ends, as Java's `close()` holds `acquire()`
    (`AsyncKafkaConsumer.java:1566`), and a call from another thread meanwhile throws
    `InvalidOperationException`, as Python's does (`_base.py:403-405`). A `Dispose` is not
    checked and never throws (§B2).
  - **A use** is also what teardown waits for (§B2). It records the thread that made the
    call and ends when the call ends: a blocking call when it returns, an async call once
    its `_cb` completion has run and **before** its `Task` completes. So awaited calls in
    sequence never overlap, whichever thread each resumes on, and only overlapping calls
    are compared, as Python keys its table on the thread for the async consumer too
    (`threading.get_ident()`, `_base.py:399`). Whichever thread ends a use releases it for
    the thread it recorded. A step that calls the core again for the same call runs
    inside its use (a resubmit after the consumer's own wakeup, ledger 126) or after it,
    as a use of its own for the same thread (a seek-back, as Python's `_rewind_sync` /
    `_rewind_async` run after its `with self._use()`, `_base.py:1033-1037`,
    `:1123-1131`).
  - **The mocks** (`MockConsumer`, `AsyncMockConsumer`) wait instead, as Java's
    `synchronized` and Python's mock monitor do
    (`python/confluent_kafka/consumer/_mock_core.py:30-32`, `:125-150` at `3f8a5bc6`): a
    call while another thread's call holds a use waits until that use ends; the same
    thread nests. A call made inside a callback does not wait, as Python's holding task
    passes (`_mock_core.py:44-45`): `AsyncMockConsumer`'s callbacks run on the drain
    thread while the call that delivers them holds its use, so a wait there would never
    end. A call from another thread throws `InvalidOperationException` with the same
    message instead for the whole of `AsyncMockConsumer.Rebalance` and `LosePartitions`,
    whether the listener is sync or async, and while an async commit callback it
    delivers runs, from its start, its synchronous part included, as Python's mock sets
    its monitor's owner before the listener or the callback's body runs
    (`_mock_core.py:35-45`, `:212-232`, `:263-279`, `:616-635` at `3f8a5bc6`; ledger 142);
    the commit's own body and a sync commit callback still make it wait, as Python's
    `RLock` does. Every mock call waits but `Wakeup()`, `Handle()`,
    `Dispose` / `DisposeAsync` and the three Java's mock leaves unsynchronized:
    `GroupMetadata()`, `EnforceRebalance()` and `CurrentLag()` pass, as Java's mock and
    Python's do (`MockConsumer.java:764-787`, `_mock_core.py:45-47`, `:519-528` at
    `3f8a5bc6`; ledger 140). Each goes through the core mock, whose flag refuses an
    overlapping call with -2 (`rust/src/ffi/consumer/mock_consumer.rs:1207-1228`,
    `mod.rs:3554-3565`, `:667-675` at `3f8a5bc6`), so one that overlaps another thread's
    mock op throws `InvalidOperationException` *(deviation: Java's and Python's answer;
    and Python's `current_lag()` with an end offset waits inside `position()`, where the
    .NET mock reads the lag from the core mock in one call)*.
  - **Callable from any thread, at any time:** `Consumer_wakeup_err`
    (BH:30624-30625), `Consumer__set_callback_result` (BH:29189-29192) and
    `Consumer_metrics_err`, which reads through the consumer's handle, not the flag
    (BH:30594-30596, all at `3f8a5bc6`). Of the public calls over them, only `Wakeup()`
    skips the owner check: `Metrics()` is a use like any call, as Python's `metrics()` is
    (`_base.py:593-596`, `:1511-1515` at `3f8a5bc6`), although Java's takes no
    `acquire()`. `Wakeup()` is how another thread interrupts a wait, and the first step
    of a cross-thread `Dispose` (§B2's path list).
  - **Listeners on the async consumers** (decision 27, ledger 35): a sync listener
    runs inline on the drain thread and reports when it returns. An async
    (Task-returning) listener is called on the drain thread, which does not wait for
    its `Task`; the `Task`'s continuation reports through
    `Consumer__set_callback_result`, callable from any thread. The rebalance does not
    advance until the report (consumer-threading §31). The listener's interface and
    the reporting rules are in `dotnet/CLAUDE.md` §4 (the rebalance-listener note).
  - **`ConsumerHandle` is outside the flag but has its own guard** (since #262,
    ledger 104). On a `KafkaConsumer`, its async operations are admitted only while
    the consumer runs a listener method or a commit callback: Java's rule that the
    thread inside the consumer may call back in and no other thread may. Outside a
    callback they fail with -2 and the message "KafkaConsumer is not safe for
    multi-threaded access. ConsumerHandle operations are allowed only from inside a
    callback the consumer is running (ConsumerRebalanceListener,
    OffsetCommitCallback); from another thread call wakeup()." `Wakeup` and the sync
    reads (`Assignment`, `Paused`) work from any thread at any time. A
    `MockConsumer` handle is not gated (BH:30643-30649;
    `rust/src/consumer/internals/async_kafka_consumer.rs:304-309`, `:896-909`, all
    at `3f8a5bc6`). So a handle op inside a callback succeeds while the op that runs
    the callback holds the flag: that success **is** the contract.
  - **Handle ops always block the calling thread.** The core runs each one as a
    task of the consumer and waits for it on a channel, not through a nested
    `block_on`, so it is safe from the drain thread and from a sync listener on the
    caller's thread; the commit callbacks a handle commit triggers run on the
    waiting thread (BH:30974-30975; `rust/src/ffi/consumer/consumer_handle.rs:37-51`,
    `:125-177`, all at `3f8a5bc6`). The binding uses only these blocking forms,
    never the handle's `_cb` twins: those queue their `cb` for
    `__execute_callbacks`, and a nested drain from inside a callback runs nothing
    (BH:30989-30990, BH:30662-30664 at `3f8a5bc6`; decision 27). This is the
    **core's** wait, inside the ABI, not a managed sync-over-async façade
    (CLAUDE.md §4).
  - **A `MockConsumer` handle** supports `Wakeup`, the sync reads, `CurrentLag` and
    the commits. Its other operations fail with `LOCAL_UNSUPPORTED_OPERATION` (−31) and
    the message "ConsumerHandle async operations are not supported on a MockConsumer
    handle; drive the MockConsumer directly."
    (`rust/src/consumer/internals/async_kafka_consumer.rs:841-844` at `9bff36d1`), a
    `NotSupportedException` (decision 14, §B5; ledger 206).
  - After the consumer's destroy, every handle op fails with `LOCAL_ILLEGAL_STATE`
    (-4): §B2 Category 6.
  - **Inside a callback** (ledgers 113, 127) means on the thread that runs a sync listener
    method or a sync commit callback, while it runs, and inside an async one, after an
    `await` too, through an `AsyncLocal<bool>` set around it and reset when it returns, as
    the producer's in-callback flag (§A7). A thread or `Task.Run` that a sync callback
    starts is not inside, as Python's `ContextVar` does not flow into a `threading.Thread`
    (`python/confluent_kafka/consumer/_base.py:191-196`, `:702-712` at `3f8a5bc6`). Work an
    async callback starts (`Task.Run`, a task it does not await, a `new Thread`) inherits
    the `AsyncLocal` and stays inside for its whole life, after the callback returns too,
    as a task (`asyncio.create_task`) or an `asyncio.to_thread` call inherits Python's
    `ContextVar` (ledger 143) *(deviation for `new Thread`: Python's `threading.Thread`
    does not inherit it)*.
  - **A consumer call made inside a callback is routed** (ledger 114), as Python's
    binding routes it (`_base.py:1491-1509`, `:1597-1619`, `:1645-1725`;
    `python/confluent_kafka/consumer/consumer.py:490-604`, `async_consumer.py:316-434` at
    `3f8a5bc6`), on all four consumer classes:
      - **To the consumer's own `ConsumerHandle`** where the handle has the form:
        `Assignment`, `Paused`, `Pause`, `Resume`, `SeekToBeginning`, `SeekToEnd`, both
        `Seek`s, both `Position`s, `CurrentLag`, `Committed`, `BeginningOffsets`,
        `EndOffsets` and `OffsetsForTimes` without a timeout, `Commit()` and
        `Commit(offsets)`, and every `CommitAsync` form. An `IAsyncOffsetCommitCallback`
        goes through the same `OffsetCommitCallback_t` registration as outside a
        callback (BH:31789-31791 at `3f8a5bc6`). The consumer makes the handle on first
        use, as Python's `_handle()` does (`_base.py:714-719`), and destroys it after the
        consumer (safe, §B2 Category 6). A routed call blocks the calling thread (above).
        On the async consumers its `Task` comes back completed, or faulted with the
        handle's error. A routed `Commit`, `Position` or `Committed` that the consumer's
        own wakeup ends (ledger 126) is made again, as Python's `_own_wake_raised` does
        (`_base.py:1307-1315`, `:1660-1663`, `:1684-1688`, `:1699-1701`). Its
        `CancellationToken` is checked before the call, as every async op checks it, and
        not after: nothing can interrupt a handle op, as nothing cancels Python's
        in-callback call, which has no `await`.
      - **Refused at once** with `InvalidOperationException` and the core's message
        "KafkaConsumer is not safe for multi-threaded access.", before anything is
        submitted, as Python's `_refuse_inside_callback` and `_timeout_ms` raise
        (`_base.py:960-968`, `:1545-1550`): every other waiting call. These are `Poll`,
        `Subscribe`, `Unsubscribe`, `Assign`, `PartitionsFor` and `ListTopics`, and the
        timed `Commit`, `Committed`, `BeginningOffsets`, `EndOffsets` and
        `OffsetsForTimes`, whose handle forms have no timeout. On the async consumers a
        `_cb` submission's -2 would be queued behind the running callback (Python's
        reason, `_base.py:961-964`); on the sync consumers the blocking form would fail
        the same way.
      - **To the core, as outside a callback:** `Subscription`, `GroupMetadata` (after
        ledger 105's `group.id` check) and `EnforceRebalance`, which the core refuses
        with -2 while the op that delivers the callback holds its flag, as Python leaves
        `subscription()` to it (`_base.py:1497-1503`); and `Metrics()`, which it
        answers. `Wakeup()` and `Handle()` are unchanged, and `Close` and `Dispose`
        follow ledgers 113 and 120 (`dotnet/CLAUDE.md` §4).
      - **Order:** the in-callback rule comes first, before the closed check, and the
        owner check skips a call made inside a callback (above). While the consumer
        closes, a call inside a callback is not refused as closed, so a listener the
        close runs can call back in, as Java sets `closed` only once its close has run
        (`AsyncKafkaConsumer.java:1566-1576`) and Python lets the closing thread's own
        calls through (`_base.py:401-405`). Once the close has ended, the call throws
        `ObjectDisposedException`, as Python's closed check raises
        (`_base.py:605-609`). A routed call is still an in-flight use (§B2), so teardown
        waits for it before the destroy.
      - **On the mocks** the routing is the same. A `MockConsumer` handle commits and
        answers `CurrentLag` and the sync reads, and fails its other ops with the
        `KafkaException` above. Java's and Python's mocks instead let a callback call
        every method, through their reentrant monitor (`MockConsumer.java`'s
        `synchronized` methods; `_mock_core.py:30-32` at `3f8a5bc6`) *(deviation: the
        core mock's handle lacks those ops; goes when it has them)*.

**Why:** Java's single-Selector NIO model on tokio (§0.3): a fixed native thread
count, no poll loop. Java runs listeners and commit callbacks on the caller's thread
(consumer-threading §31); the blocking forms give exactly that on the sync consumers,
with no drain loop and no slice timer. The async consumers have no event loop to
drain on, so one named, FIFO, serialized drain thread is the closest analogue of
Python's loop (decision 27), and its TCSs use `RunContinuationsAsynchronously` so no
awaiter's continuation runs on, and stalls, that thread. Single ownership is checked twice, as in
Python's binding: the owner check refuses another thread at once, at every call and in the gaps
the core's flag leaves (before and after the native call, and while an async result is handed
back, `rust/src/ffi/consumer/mod.rs:705-714` at `3f8a5bc6`), and the core's flag refuses an
overlapping op of the owning thread. M3/P2 removed .NET's first managed guard to match the
Python binding of the time (`dotnet/design/history/M3/P2-single-owner-alignment/PLAN.md:20`);
Python's binding now keeps a per-thread use table, so ledger 136 restores one. The handle's gate
is the core's, so the binding adds none there.

**Anti-patterns:**

  - A binding-side lock that makes a second thread **wait** on a `KafkaConsumer` (the owner
    check refuses it at once, as Java's `acquire()` and Python's table do; only the mocks
    wait, as Java's `synchronized`), or an owner check that skips a call Java guards, keys
    on anything but the thread that makes the call, or still counts an async call as a use
    after its `Task` completed (ledger 136).
  - From inside a callback the consumer is running: calling a `Consumer_*` operation
    that the handle has (it fails with -2; route it to `ConsumerHandle`, ledger 114);
    submitting any `_cb` operation (refuse it at once: its -2 would be queued behind the
    running callback); routing a call the handle lacks, timed overloads included; or
    treating a thread that a sync callback started as inside the callback (ledger
    127).
  - The owner check, or any managed guard, around handle ops, or a managed copy of the
    handle's gate. Using the handle from another thread outside a callback, as a
    cross-thread path into the consumer: on a `KafkaConsumer` it fails with -2; call
    `Wakeup()` instead.
  - Calling a handle's `_cb` twin from the binding (its `cb` waits for a drain that
    a nested `__execute_callbacks` will not run), or wrapping a handle op or a
    blocking form in `Task.Run` (sync-over-async, §B7).
  - A sync consumer that uses the `_cb` forms plus a managed wait, or that has a
    thread of its own (decision 27 chose the blocking forms).
  - An async consumer that drains on the thread pool from the hook, or in the
    awaiting call (decision 27's options (ii) and (iii)).
  - A TCS **without** `RunContinuationsAsynchronously`: the continuation runs inline
    on the drain thread and stalls every later callback.
  - The drain thread waiting on a user `Task` (an async listener or callback).
  - A notify hook that does more than signal; a poll-loop thread; thread-per-broker
    assumptions (§0.3).

**Tests required:**

  - A call from another thread while one holds a use → `InvalidOperationException` with
    the core's message, from the owner check before the native call, for a blocking op, a
    `_cb` op and a guarded state read alike (decision 17), and in the gaps the core's flag
    leaves: just before and just after another thread's native call, and while an async
    result is handed back (ledger 136); no corruption. The same thread nests; an
    overlapping call of the owning thread gets the core's -2, as the same exception.
    Awaited async calls in sequence pass whichever thread each resumes on. `Wakeup()` and
    `Handle()` from another thread succeed meanwhile; `Metrics()` is refused like any
    call. A call made inside a callback on the drain thread passes the owner check, and on
    `AsyncMockConsumer` does not wait (no deadlock). On a mock, another thread's call waits
    for the running one, and is refused for the whole of `AsyncMockConsumer.Rebalance` and
    `LosePartitions`, a sync listener included, and from the start of an async commit
    callback, its synchronous part included (ledger 142); `GroupMetadata()`,
    `EnforceRebalance()` and `CurrentLag()` do not wait.
  - Sync consumer: a listener and a commit callback run on the caller's thread,
    inside the call that triggered them; each blocking call drains
    `__execute_callbacks` once first.
  - Async consumer: the `cb`s, listeners and commit callbacks run on
    `confluent-kafka-consumer-pump`; no continuation runs inline on it; an async
    listener whose `Task` completes later reports once, and the rebalance waits for
    it.
  - `Wakeup()` from another thread ends a blocking `Poll` promptly (ledger 34).
  - **The handle's gate:** a `KafkaConsumer` handle op outside a callback throws
    `InvalidOperationException` with the core's handle message. Inside a listener,
    on the sync and the async consumer, a handle op (`Seek`, `Position`, `Commit`,
    `CurrentLag`) succeeds while the op that runs the listener holds the flag, and
    `CommitAsync(offsets)` commits. The handle's `Wakeup` and sync reads work from
    another thread. On a `MockConsumer` handle a commit and `CurrentLag` succeed
    outside a callback, and an unwired op (`Seek`) throws `KafkaException` with the
    core's message.
  - **Routing inside a callback** (ledger 114), on the sync and the async consumer,
    inside a listener and a commit callback, and after an `await` too:
      - `Commit()`, `Seek`, `Position`, `Committed`, `CurrentLag`, `Assignment` and
        `CommitAsync(offsets)` succeed through the handle. This is
        `consumer-threading.md` §31's first test, on the public API.
      - `Poll`, `Subscribe`, `Assign` and a timed `Commit` throw
        `InvalidOperationException` at once; on the async consumer nothing is submitted.
      - `Subscription` and `GroupMetadata` throw the core's -2, and `Metrics()` answers.
      - A thread that a sync listener starts is not inside: its `Commit()` is a call
        from another thread.
      - A listener that the close runs commits through the routing.
      - On a mock, a commit and `CurrentLag` succeed and `Seek` throws the handle's
        `KafkaException`.
  - `Dispose` / `DisposeAsync` return without hanging, with an op in flight and with
    a live `ConsumerHandle`. ⚠ The teardown routes are **not** restated here: see
    §B2's path list.

---

## B2 Handle ownership & lifecycle (`SafeHandle`)

**Decision:** Six ownership categories:

1. **Client / config** (the consumer class and its view, the config) → a
   `SafeHandle`, or a short-lived handle, freed exactly once (teardown closes first:
   see the Rule).
2. **Flat transient** (an error: any `out_err`, or a `_cb` completion's `error`) →
   read-and-free promptly, **not** wrapped (a finalizable object per op is waste).
3. **Owned result / container** (the poll batch and every query-result list or map,
   whether a blocking form returns it or a `_cb` completion delivers it as its
   `value`) → the caller frees once after reading; a container is a *borrow-root*
   whose `_destroy` invalidates the elements and bytes borrowed from it.
4. **Borrowed view** (a `ConsumerRecord_t` in a batch, `Node_t`, every list or map
   element, borrowed strings, and everything a listener or `onComplete` receives) →
   **never freed** by the binding.
5. **Registration copied by the callee** (`ConsumerRebalanceListener_t`,
   `OffsetCommitCallback_t`) → the binding owns the handle and destroys it after the
   call that reads it, on every path, and also when no call was made. The core
   copies the registration during the call (BH:29284-29287, BH:33743-33744 at
   `3f8a5bc6`); only the user object (`self_`) lives on, under §B6's rules.
6. **Caller-owned, independently destroyed, weak to its parent**
   (`ConsumerHandle_t`) → the binding owns it for a lifetime of its choosing. The
   core's handle holds the consumer weakly: after the consumer's destroy a handle op
   fails with -4, and the handle is still safe to destroy (BH:30638-30641 at
   `3f8a5bc6`). It does not keep the consumer alive (ledger 34 (i)).

| Handle | Category | Freed by |
|---|---|---|
| `KafkaConsumer_t` / `MockConsumer_t` (class) and its `Consumer_t` view | 1 — client (`SafeConsumerHandle` over the view, the class pointer in a field; decision 6) | `Close` / `Dispose`: see the path list below. `KafkaConsumer_destroy_err` / `MockConsumer_destroy_err` with the class pointer, on the closing thread (BH:33170, BH:33218 at `3f8a5bc6`). The view is never destroyed (BH:33131-33134 at `3f8a5bc6`) |
| `ConsumerConfig_t`, and the `kafka_Map_t` built for it | 1 — config (short-lived) | the binding: the map after `ConsumerConfig_new`, which leaves it the caller's; the config after `KafkaConsumer_new`, which copies it (BH:30835-30838, BH:33114 at `3f8a5bc6`) |
| `kafka_common_Error_t` (any `out_err`, a `_cb` completion's `error`) | 2 — flat transient | reader: read the accessors, then `Error_destroy_err` |
| `ConsumerRecords_t` (poll batch) | 3 — owned **borrow-root** | owns the fetched bytes; **copy-out default** (CLAUDE.md §6.4), keep-alive deferred; `ConsumerRecords_destroy_err` |
| a `kafka_List_t` / `kafka_Map_t` a query returns (`assignment`, `committed`, `partitions_for`, `list_topics`, `offsets_for_times`, `beginning_offsets`, …), `ConsumerGroupMetadata_t`, any other owned value type | 3 — owned result | caller: read and marshal into managed types, then `kafka_List_destroy_err` / `kafka_Map_destroy_err` / the type's `_destroy`. A container's destroy frees the elements it owns too (BH:40285-40295, BH:40401-40412 at `3f8a5bc6`); the list `ConsumerRecords_records_with_partition_err` returns is owned, its records borrowed (BH:32964-32966 at `3f8a5bc6`) |
| a `ConsumerRecord_t` from a records handle, `Node_t`, every `kafka_List_get_err` / `kafka_Map_key_err` / `kafka_Map_value_err` / `kafka_Map_get_err` element, a borrowed `const char*`, and the partitions, offsets and error a listener or `onComplete` receives | 4 — borrowed view | **nobody**: it dies with its owner, or when the callback returns; never `_destroy` |
| `ConsumerRebalanceListener_t` | 5 — registration copied by the callee | the **binding**, right after the `subscribe_*` call returns (success or error), or when no call was made (`ConsumerRebalanceListener_destroy_err`); `self_` follows §B6 (decision 23) |
| `OffsetCommitCallback_t` (including the no-op one a `ConsumerHandle` commit passes, ledger 104) | 5 — registration copied by the callee | the **binding**, right after the `commit_async*` call that took it returns, or when no call was made (`OffsetCommitCallback_destroy_err`); `self_` follows §B6 |
| `ConsumerHandle_t` | 6 — caller-owned, weak to its parent | the **binding**, whenever it chooses: `ReleaseHandle` → `ConsumerHandle_destroy_err`, never during one of its own ops (BH:31940-31945 at `3f8a5bc6`). No reference on the consumer (ledger 34 (i)) |

⚠ **What a listener or `onComplete` receives is borrowed for the call** (M19/P1).
The partitions handed to a listener method (BH:3668-3678 at `3f8a5bc6`) and the
offsets and error handed to `onComplete` (BH:3778-3792 at `3f8a5bc6`) are borrowed:
copy what you need inside the call and **never destroy them**. The pre-`0035d9ff`
rule "callbacks own the handles delivered to them" is gone, and destroying a
delivered handle is now a double free. So a marshaller for a container delivered to
a listener or commit callback uses plain `CopyOut`; `CopyOutAndDestroy` is for owned
roots only. A `_cb` completion is the other way round: its `value` and `error` are
owned (`Consumer_poll_cb_t`, BH:3104-3108 at `3f8a5bc6`, and its siblings), so the
`cb` frees them (Categories 2 and 3).

**Note — classify by the accessor, not the type.** The returning function's
const-ness and its header text decide, not the type name:

  - `const *` return (or a type with no `_destroy` of its own) → **borrowed**;
    never free it (Category 4). So is every element read through
    `kafka_List_get_err`, `kafka_Map_key_err`, `kafka_Map_value_err` or
    `kafka_Map_get_err`, whatever its type: "The element stays owned by whoever owns
    it" (BH:40272 at `3f8a5bc6`). So is a callback argument the header calls
    "borrowed for the call".
  - non-`const` return with a `_destroy` → **owned**; free it once after use
    (Category 1/3).
  - a handle **passed in** to a call the header says consumes it → **consumed**;
    the binding must not free it afterwards. On the consumer that is the
    deserializer handles of `KafkaConsumer_new` (BH:33114-33116 at `3f8a5bc6`). A
    registration the header says is copied (Category 5) is the opposite: the binding
    frees it.
  - the same type can be owned in one call and borrowed in another: the
    `kafka_List_t` of `PartitionInfo_t` that `partitions_for` returns is owned, but
    the one inside a `list_topics` map is borrowed; the list `Consumer_assignment`
    returns is owned, while the partitions a listener receives are borrowed for the
    call.

**Rule:**

  - ⚠ **THE AUTHORITATIVE `KafkaConsumer_destroy` PATH LIST LIVES HERE.** Every
    other section that mentions consumer teardown **cross-references this list and
    must not restate it**: three separate enumerations drifted out of date during
    M9. If you are about to write "teardown is X → Y" anywhere else in this file,
    write "see §B2's path list" instead. Since M19/P1 (decision 7, ledger 34) there
    are **two** paths, and the destroy always runs on the closing thread. The M9/P4
    ref-count paths and the M9/P8 `ConsumerHandle` path are gone. "Destroy" here is
    `KafkaConsumer_destroy_err`, or `MockConsumer_destroy_err` for a mock.

    | # | Trigger | Route | Thread |
    |---|---|---|---|
    | 1 | every `Close` overload, `Dispose`, `DisposeAsync` | refuse new calls (`ObjectDisposedException`; during an explicit `Close`, another thread's call throws `InvalidOperationException`, §B1's owner check) → close (sync consumers: the blocking `close*`; async consumers: `close*_cb`, awaited) → stop and join the drain thread (async consumers) → wait until the in-flight uses reach 0 → set the explicit-teardown flag → release the `SafeHandle` → destroy | the closing thread |
    | 2 | `Dispose` / `DisposeAsync` from inside a callback on the drain thread (async consumers; a `Close` there throws `InvalidOperationException`, ledger 113) | the teardown is scheduled and the call returns; path 1's steps run once the drain's in-flight use has ended, never inside `__execute_callbacks`, and the drain thread is joined from the pool thread, never from itself | a pool thread, after the drain (as Python's runs in an executor off the loop, `python/confluent_kafka/consumer/async_consumer.py:604-608` at `3f8a5bc6`) |

    The cases, all on these two paths:
      - **`Dispose` from another thread while an op runs:** `Consumer_wakeup_err`
        first, then wait for the in-flight uses, then path 1's close, join and
        destroy. The wakeup ends an op Java's `wakeup()` interrupts (§B5's list), so its use
        drains at once. Any other op runs to its end first, and the wait for the uses has no
        bound of its own, so `Dispose` waits as long as that op: an `Unsubscribe` waits for the
        leave heartbeat's response up to `default.api.timeout.ms`
        (`rust/src/consumer/internals/async_kafka_consumer.rs:4502-4507` at `25a4d475`, Java's
        `unsubscribe`). `Dispose` does
        not throw (ledger 34 (ii); a recorded deviation, `dotnet/CLAUDE.md` §4). A call a
        callback of that op makes, routed to the handle (ledger 114), may take the wakeup
        first: it then throws the wakeup's `KafkaException` (-18) into the callback, as
        Python's routed call raises `WakeupError` for its own wake aimed at the running
        call and retries only a wake left over from an ended call
        (`_base.py:1267-1315` at `3f8a5bc6`), and `Dispose` sends the wakeup again, so the
        op that delivered the callback still ends (ledger 144). On
        the async consumers an op's in-flight use lasts until its `cb` has run on the
        drain thread, which is still running then: a `_cb` op releases the core's
        flag only just before it queues its completion
        (`rust/src/ffi/consumer/mod.rs:705-714` at `3f8a5bc6`), so a close sent
        before that `cb` ran could still fail with -2. Python counts a waiting call
        the same way, "covering its submission, its wait and its callbacks"
        (`python/confluent_kafka/consumer/_base.py:445-456` at `3f8a5bc6`).
      - **An explicit `Close` from another thread while an op runs:** the owner check
        refuses it at once → `InvalidOperationException`, before any state changes, so
        nothing is torn down and the consumer stays usable (ledgers 34 (ii), 136), as
        Java's `close()` fails in `acquire()` (`AsyncKafkaConsumer.java:1566`) and
        Python's raises (`_base.py:449-454` at `3f8a5bc6`). Call `Wakeup()` first. A
        `Close` from the thread that holds the op's use passes the check and, while the
        op is in the core, is refused by the core's flag (-2) with the same exception;
        the consumer stays open there too, as Python's does when its FFI close fails.
      - **A `Close` or `Dispose` from inside a listener or commit callback on the
        sync consumers:** the op that runs the callback holds the single-owner flag,
        and the blocking close takes it too, so the core rejects the close with -2 →
        `InvalidOperationException`, and nothing is torn down (ledger 34 (iii);
        `rust/src/ffi/consumer/mod.rs:316-325`, `:660-676`, `:3700-3706` at
        `3f8a5bc6`). Python's `close()` raises `ConcurrentModificationError` there
        and leaves the consumer open
        (`python/confluent_kafka/consumer/consumer.py:622-628`, `_base.py:960-968` at
        `3f8a5bc6`). Teardown never waits there for the op it is called from.
      - **Finalization** (the `SafeHandle` reached without the explicit-teardown
        flag): no destroy. `ReleaseHandle` writes one `Trace.TraceWarning` and leaks
        the consumer, as Java (no client finalizer) and Python (no `__del__`) do. In
        practice this is the sync consumers: a running drain thread roots its
        consumer.
      - **A live `ConsumerHandle`** does not defer the destroy (Category 6).

    Destroy first wakes the op still waiting (it fails with `WAKEUP`), awaits the
    `_cb` ops in flight, runs every pending callback on the destroying thread so each
    fires once, drops the consumer, which closes it if it was never closed, and
    shuts its runtime down (BH:33146-33153 at `3f8a5bc6`). So the closing thread can
    run user callbacks, which may use the view only to report. No other call may use
    the consumer while destroy runs (BH:33160-33162 at `3f8a5bc6`): that is what
    waiting for the in-flight uses guarantees.

  - `SafeConsumerHandle : SafeHandle`, one per consumer class, following §A2's
    `SafeClientHandle` (decision 6): the view in `handle`, the class pointer in a
    field. `ReleaseHandle` destroys only when the explicit-teardown flag is set
    (path 1); reached any other way it traces once and leaks. It passes the class
    pointer with no `AddRef`, because the handle is already being released. Close
    always comes before destroy. Guard use-after-close with
    `ObjectDisposedException`, checked before any P/Invoke.
  - **Every operation P/Invoke takes the `SafeHandle`** (decision 6), so the
    marshaller's call-scoped `AddRef` is a blocking form's in-flight use, for its
    whole run. A `_cb` form's P/Invoke returns at once, so its in-flight use is
    counted from the submission until its `cb` has run (path 1's cross-thread case).
    There is no span-the-op `DangerousAddRef` and no destroy from the last release
    any more: decision 7 replaces the M9/P4 H1 and M9/P8 paths.
  - **Transient error handle:** read the accessors (message before free), then
    `_destroy` in a `finally`; the managed `KafkaException` holds copied values,
    never the handle (§B5). `_destroy` is null-safe.
  - **Category 3 — owned result / container.** Read it where it arrives: on the
    caller's thread for a blocking form; for a `_cb` form, in its `cb` on the drain
    thread, except the poll batch, which the `cb` hands to the awaiting continuation
    (decision 11). Then `_destroy` the root exactly once. A **container is a
    borrow-root**: its elements and any key/value/topic/string bytes borrow into it
    (§B3, §B4, CLAUDE.md §6.4), so it must outlive every borrow taken from it.
    **Default: copy-out**: copy each element or byte into an owned managed type,
    then `_destroy`. Metadata collections are always copy-out (small), and typed
    deserialization reads a transient span → owned `T` (copy-out too).
    **Keep-alive** (hold the root, expose zero-copy views, `_destroy` at `Dispose`)
    is a **deferred** option for the raw-byte surface only; see CLAUDE.md §6.4.
  - **Category 4 — borrowed view.** A `ConsumerRecord_t` in a batch, `Node_t`, list
    and map elements, borrowed strings and what a callback receives are never passed
    to a `_destroy`, even where their type has one ("a record borrowed from a records
    handle is never passed here", BH:32666-32667 at `3f8a5bc6`). Never use them
    after their owner (3) is destroyed, or after the callback that received them
    returns. Represent them as a transient cursor over the parent; don't let one
    escape the parent's lifetime.
  - **Category 5 — registration copied by the callee**
    (`ConsumerRebalanceListener_t`, `OffsetCommitCallback_t`). Build the handle, pass
    it to the call, and destroy it right after the call returns, success or error,
    and also when the call never ran (the `SafeHandle` marshaller threw
    `ObjectDisposedException` against a concurrent teardown). The core copied what it
    needs during the call: "the listener registration is copied during the call (its
    handle may be destroyed afterwards)" (BH:29284-29285), and a consumer a commit
    callback was passed to "keeps its own copy of the registration" (BH:33743-33744,
    both at `3f8a5bc6`). Destroying the handle says nothing about whether the user
    object is still registered: that is §B6's `self_` rule (decision 23 for the
    listener; until `onComplete` fired, or destroy, for the commit callback,
    BH:33693-33696 at `3f8a5bc6`).
  - **Category 6 — caller-owned, weak to its parent** (`ConsumerHandle_t`, ledger 34
    (i)). The core's handle holds the consumer weakly
    (`rust/src/ffi/consumer/consumer_handle.rs:93-96` at `3f8a5bc6`), so the handle's
    `SafeHandle` takes **no** reference on the consumer's `SafeConsumerHandle`.
    Destroy the handle whenever the binding chooses, before or after the consumer,
    but never while one of its own ops runs (BH:31940-31945 at `3f8a5bc6`): every
    handle op takes the handle's `SafeHandle`, whose call-scoped `AddRef` covers
    that. After the consumer's destroy, every handle op fails with
    `LOCAL_ILLEGAL_STATE` (-4, "consumer destroyed",
    `rust/src/ffi/consumer/consumer_handle.rs:121-123` at `3f8a5bc6`) →
    `InvalidOperationException` (decision 14), and the handle is still safe to
    destroy (BH:30638-30641, BH:30974-30975 at `3f8a5bc6`). This replaces M9/P8's
    P8-D1 reference and the deferred destroy it caused: a live handle no longer
    defers the consumer's destroy.

**Why:** `SafeHandle` is the robust form of "call `_destroy` exactly once," even
through exceptions; `IsInvalid == zero` matches our null-safe destroy (this is
confluent-kafka-dotnet's `SafeHandleZeroIsInvalid` pattern). A per-message
`SafeHandle` would allocate a finalizable object per record, so transient handles
and borrowed views are not wrapped. The borrow-root discipline (Category 3 outlives
its Category 4 borrows) is what makes the receive-path zero-copy contract (§B4,
CLAUDE.md §6.4) safe. Teardown closes explicitly before it destroys: destroy closes a
consumer never closed, but has no way to report that close's error (BH:33148-33149
at `3f8a5bc6`), so the explicit close runs first, where its error can surface.

Teardown waits for the in-flight uses, so no P/Invoke is running when destroy runs,
which the header requires (BH:33160-33162 at `3f8a5bc6`). Destroy runs on the closing
thread and never inside `__execute_callbacks` (decision 7): a callback may use the
view only to report (BH:33162-33164 at `3f8a5bc6`), and destroy awaits the `_cb` ops
in flight, one of which may be waiting on that very callback.

**Anti-patterns:**

  - Freeing a **borrowed** view (Category 4: a `ConsumerRecord_t` in a batch,
    `Node_t`, a list or map element, a borrowed string, anything a callback
    receives): a double free or a use-after-free; only the owning container is
    freed.
  - Destroying a **borrow-root** (`ConsumerRecords_t`, a list or map) while a
    borrowed element or byte slice from it is still in use (CLAUDE.md §6.4): a
    use-after-free.
  - Leaking an **owned** result (forgetting `_destroy` after marshalling a query map
    or list, or a `_cb` completion's `value` or `error`), or freeing it twice.
  - Not destroying a `ConsumerRebalanceListener_t` / `OffsetCommitCallback_t` after
    its call (a leak), or freeing its `self_` when the handle is destroyed (a
    use-after-free: the registration lives on in the core).
  - Destroying without an explicit close first, or re-adding a `Dispose`-side
    separate-op drain.
  - Passing a **raw `DangerousGetHandle()`** to any consumer or handle operation
    `[DllImport]`. Every op takes its `SafeHandle` (decision 6). The exemptions: a
    class-only call (`MockConsumer_*`), which takes `DangerousAddRefClass` and passes
    the stored class pointer (§A2), and destroy, called from `ReleaseHandle` with the
    stored class pointer and no `AddRef`.
  - Calling `KafkaConsumer_destroy_err` inside `__execute_callbacks`, from a
    finalizer, or from a `ReleaseHandle` without the explicit-teardown flag.
  - Taking a reference on the consumer from a `ConsumerHandle` (the P8-D1 shape):
    the core's handle is weak, and a reference would defer the consumer's destroy
    again.

> **Retired (M19/P1).** The M9/P4 Q1/Q3 carve-out (a deferred, bare destroy when
> disposing with an unawaited op) no longer applies: decision 7's teardown waits for
> the in-flight uses and always closes before destroy.

**Tests required:**

  - Poll a batch, read records, then dispose — no leak; a borrowed key/value/topic
    used after the batch is gone is prevented (copy-out) or kept valid by the
    wrapper (keep-alive), per CLAUDE.md §6.4.
  - Each query API (`committed`/`assignment`/`partitions_for`/…) frees its owned
    result exactly once after marshalling, on the blocking and the `_cb` form;
    borrowed elements are never freed. A listener's partitions and `onComplete`'s
    offsets and error are copied, never destroyed.
  - Create and close many consumers — no leak; teardown closes before it destroys.
  - **Category 5:** a listener handle and a commit-callback handle are destroyed
    after a successful call, after a failed one and when the call never ran; after a
    call that ran, the callback still fires with its `self_` alive.
  - **Category 6:** a handle op after the consumer's destroy throws
    `InvalidOperationException` (-4); destroying the handle after the consumer is
    safe; the consumer's `Dispose` destroys at once with a handle outstanding.
  - **The path list:** a `Dispose` from another thread during a blocking `Poll`, and
    during an async `Poll`, returns and does not throw; an explicit cross-thread
    `Close` throws `InvalidOperationException` and leaves the consumer usable; a
    `Close` or `Dispose` inside a sync listener throws `InvalidOperationException`;
    a `Close` inside a callback on the async consumers throws `InvalidOperationException`
    and leaves the consumer open, while a `Dispose` there is deferred and the destroy runs
    after the drain; a finalized consumer writes one trace and does not destroy.
  - Double-`Dispose` is safe; a call after `Dispose` throws
    `ObjectDisposedException`.

---

## B3 String marshalling (UTF-8)

**Decision:** Every boundary string is UTF-8, marshalled by hand (the floor lacks
`LPUTF8Str` / `Marshal.PtrToStringUTF8`, §0.1): input via `Utf8Marshal.Pin` (encode + NUL
+ pin); output via `Utf8Marshal.PtrToString(ptr)` over a NUL-terminated `const char*`
borrowed from a handle (NUL-scan + `GetString`). Since M19/P1 no string crosses
length-delimited (no `out_len` anywhere in BH at `3f8a5bc6`): every function that returns a
`kafka_Bytes_t` returns bytes, not text (salts, passwords, record buffers, header values, an
HMAC, user data; §B4).

| Direction | Sites | Helper |
|---|---|---|
| In | topic, config key/value, `subscribe`/`seek` args | `Utf8Marshal.Pin` |
| Out — borrowed from the value, valid until its own `_destroy` | `Error_message`, `ConsumerGroupMetadata_group_id` / `_member_id`, `Node_host` / `_rack`, other getters | `Utf8Marshal.PtrToString(ptr)` — NUL-scan |
| Out — borrowed from a record, valid until `ConsumerRecords_destroy` | `ConsumerRecord_topic`, a header's `Header_key` | `Utf8Marshal.PtrToString(ptr)` — NUL-scan |

**All output pointers are borrowed** — .NET copies (`GetString`) before the owning
handle is freed; it never owns the raw pointer. The two output rows differ only in which
handle bounds the lifetime:

  - the value's own: an error's message lives until the error is destroyed, or, for a
    borrowed error, until its owner is (BH:17801-17827 at `3f8a5bc6`); a node's host and
    rack until the node is, the rack `NULL` when the node has none (BH:25898-25910,
    BH:25938-25950 at `3f8a5bc6`);
  - the record's: `ConsumerRecord_topic` points into the record's own NUL-terminated copy
    (BH:32483-32494; `rust/src/ffi/consumer/consumer_record.rs:225-233` at `3f8a5bc6`), and a
    header key is borrowed from its header (BH:21781-21792), itself borrowed from the record
    (BH:32602-32614 at `3f8a5bc6`). The records a `ConsumerRecords_t` hands out are borrowed
    from it, valid until `ConsumerRecords_destroy` (BH:32963-32995 at `3f8a5bc6`).

**Rule:**

  - Input: `Encoding.UTF8.GetBytes` → `new byte[len+1]` (trailing zero = NUL) →
    pin → pass `AddrOfPinnedObject`; unpin in `finally`. Marshalling copies (an
    encoding conversion) — fine for small topic/config, and **not** the zero-copy
    path (§B4).
  - Output: `Utf8Marshal.PtrToString(ptr)`, scanning to NUL and copying into a managed
    `string` (needs `unsafe`; a `#if NET6_0_OR_GREATER` span fast path is an internal
    optimization); `IntPtr.Zero` → `null` (Java's `null`, as a node without a rack).
    **Copy before free** — the pointer dies with the handle it is borrowed from; never
    store the raw pointer.
  - Never `[MarshalAs(LPStr)]` (ANSI) or `LPWStr` (UTF-16); never `LPUTF8Str` /
    `Marshal.PtrToStringUTF8` (absent on the floor).

**Why:** UTF-8 is the contract both ways, and the core reads input with
`to_string_lossy` (`rust/src/ffi/util.rs:98-107` at `3f8a5bc6`) — invalid bytes are
silently *replaced*, not rejected, so an `LPStr` mistake corrupts non-ASCII topics quietly
(and hides in ASCII-only tests). Hand-rolled helpers are the same reason
confluent-kafka-dotnet ships `StringAsPinnedUTF8` + `PtrToStringUTF8`. The receive-path
strings are borrowed from the record, so they must be copied out before
`ConsumerRecords_destroy` (CLAUDE.md §6.4 / consumer-threading §27); since M19/P1 the
record keeps its own NUL-terminated topic, so no string is a slice into the fetch batch.

**Anti-patterns:**

  - `LPStr` / `LPWStr` for any string; a `const char*` return marshalled as
    `string`.
  - Reading an output pointer after its handle (or the records handle) is gone.
  - Assuming ASCII (works until a non-ASCII topic corrupts silently).

**Tests required:**

  - A non-ASCII value round-trips through a config key/value and an error message.
  - A `ConsumerRecord` topic and a header key with a non-ASCII value round-trip, copied
    before `ConsumerRecords_destroy`; a multi-byte char at the end of the string marshals
    correctly.
  - `Utf8Marshal.PtrToString(IntPtr.Zero)` → `null`.

---

## B4 Zero-copy & buffer lifetime — receive path

**Decision:** The consumer's key/value bytes originate in the **native** batch buffer,
kept alive by `ConsumerRecords_t`. .NET deserializes on the managed side (decision 11), so it
passes `NULL` deserializers to `KafkaConsumer_new`, and `ConsumerRecord_key` / `_value` then
return a `kafka_Bytes_t *` borrowed from the record, pointing into the fetch buffer or, for a
compressed batch, the record's own buffer, `NULL` for Java's `null` (BH:33109-33116,
BH:32574-32600; `rust/src/ffi/consumer/consumer_record.rs:21-30` at `3f8a5bc6`). The topic is
the record's NUL-terminated string (§B3). **No pinning** — native memory isn't GC-managed. Raw
bytes are **copy-out by default**.

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
    `Task` (§B7) — same `FromHandle`. Keep it **one flat `KafkaException` for now** for
    every code the error table gives no .NET type (decision 14): it carries `Code`
    (`kafka_common_Error__enum_err`) and `IsRetriable`
    (`kafka_common_Error_is_retriable_error_err`) (BH:17798, BH:33877 at `3f8a5bc6`); the
    header has no fatal predicate. Typed subclasses can be added under it later,
    non-breakingly.
  - **Two error channels on the commit-callback family, and they must not be
    conflated** (M9/P7; M19/P1 forms). For `Consumer_commit_async_with_callback` /
    `_commit_async_with_offsets_callback` and their `_cb` twins:
      - the error **of the call** (`out_err`, or the `_cb`'s `error`) is an initiation
        failure: the commit never started. It throws from `IConsumer.CommitAsync`, or
        faults the `Task` of `IAsyncConsumer.CommitAsync` (decision 13). How long `self_`
        then lives is §B6's commit-callback rule.
      - the error **delivered to `on_complete`** is the commit's own outcome (`NULL` =
        success, Java's "exception == null means success"). It is borrowed for the call
        (BH:3777-3792 at `3f8a5bc6`): read it without freeing it, and hand it to the user
        as an `Exception?` parameter, of the type the error table gives (decision 33).
        `offsets` is `null` where Java passes `null`: on a failed commit, and for an
        explicit empty `offsets`, whose Java `commit()` returns `completedFuture(null)`
        (decision 22; `AsyncKafkaConsumer.java:1112-1144`), as Python does
        (`python/confluent_kafka/consumer/_base.py:900-948` at `3f8a5bc6`). The core always
        delivers a map, empty or not
        (`rust/src/ffi/consumer/offset_commit_callback.rs:97-114` at `3f8a5bc6`), so the
        binding decides the `null`. An exception the user callback throws propagates out
        of the consumer call that ran it (decision 12).
    A commit can start cleanly and fail later, so both channels need their own coverage. A
    `MockConsumer` always delivers success, so testing the delivered channel means driving
    the trampoline as the core would.
  - **Precondition:** validate before any pin/marshal/P/Invoke and throw
    `ArgumentNullException` (null topic/partition/config),
    `ArgumentOutOfRangeException` (a negative timeout), or `ObjectDisposedException`
    / `InvalidOperationException` (closed consumer). **Mandatory**, not optional:
    the ABI doesn't validate preconditions (CLAUDE.md §3), and a `NULL` where the
    header asks for a live handle is undefined behavior. A panic in the core does not
    cross the boundary: the `_err` twin stores it as -4 with a message beginning "Rust
    panic caught at the FFI boundary in <function>" (`rust/ffi-macros/src/lib.rs:87-88`
    at `3f8a5bc6`), which the error table maps to `InvalidOperationException`, not to
    Java's error; so a check the binding skips surfaces as that panic. The core's getters
    panic on a closed consumer
    (`rust/src/consumer/internals/async_kafka_consumer.rs:3928-3931` at `3f8a5bc6`),
    which the closed check keeps from reaching C.
  - **`GroupMetadata()` without `group.id` (ledger 105).** The core panics there too
    (`rust/src/consumer/internals/async_kafka_consumer.rs:4026-4032` at `3f8a5bc6`), so
    .NET makes Java's check itself: after its closed check and before calling C, it
    throws the `INVALID_GROUP_ID` (24) `KafkaException` with Java's message, "To use the
    group management or offset commit APIs, you must provide a valid group.id in the
    consumer configuration." (`AsyncKafkaConsumer.java:1293-1298`, `:1528-1537`), as
    Python does (`python/confluent_kafka/consumer/_base.py:1571-1577` at `3f8a5bc6`). It
    is not a precondition in the sense above: it stands in for a core check, so it throws
    the type Java's `InvalidGroupIdException` maps to, not a .NET built-in.
  - **`wakeup()`** → the interrupted op surfaces a flat `KafkaException` with the
    **Wakeup** code (-18, BH:355 at `3f8a5bc6`) (Java `WakeupException` semantics —
    raised **once**, then the flag clears and the op works again; consumer-threading
    §11). Java's `wakeup()` interrupts only the ops whose future registers with the
    `WakeupTrigger`: `poll`, `commitSync`, `position`, `committed`, `partitionsFor`,
    `listTopics` (`python/confluent_kafka/consumer/_base.py:1343-1352`;
    `rust/src/consumer/internals/async_kafka_consumer.rs:11120-11126` at `3f8a5bc6`).
    Any other op (`CommitAsync`, `Subscribe`, `Seek`, `Pause`, `OffsetsForTimes`,
    `Close`, `CurrentLag`, …) does not consume it, so a wakeup sent during one stays
    armed for the next wakeable call (`CurrentLag`:
    `python/confluent_kafka/consumer/_base.py:1425-1429` at `3f8a5bc6`). **Not** a
    subclass (decision 14), although Java and Python use one (`WakeupException`;
    Python's `WakeupError(KafkaError)`,
    `python/confluent_kafka/common/errors/wakeup_error.py:32` at `3f8a5bc6`): a
    `WakeupException : KafkaException` can be added later without a break (the Note
    below). Distinct from **`CancellationToken`** cancellation →
    **`OperationCanceledException`** (the .NET-native cancel, CLAUDE.md §3 — the analog
    of Python re-raising `CancelledError`); which ops a cancel wakes is §B7.
  - **Concurrent use** (single-owner, one op in flight; the binding's owner check, then the
    core's flag, ledger 136, §B1). A call from another thread while one holds a use throws
    `InvalidOperationException` with Java's `acquire()` message, "KafkaConsumer is not safe
    for multi-threaded access.", from the owner check before the native call. A call that
    passes the check but meets an op still in the core (an overlapping call of the owning
    thread) fails at once with `LOCAL_CONCURRENT_MODIFICATION` (-2) and the same message
    (`rust/src/ffi/consumer/mod.rs:318-325` at `3f8a5bc6`): a blocking form through
    `out_err`; a `_cb` form through its `cb`, queued for `__execute_callbacks` like any
    completion (`rust/src/ffi/consumer/mod.rs:700-702`, `:721-734` at `3f8a5bc6`); a state
    read (`Assignment` / `Subscription` / `Paused` / `GroupMetadata`) through `out_err`
    with a `NULL` return (BH:30513-30552, BH:30609-30621 at `3f8a5bc6`). Every one
    surfaces as `InvalidOperationException` with that message, sync and async (decisions
    14 and 17), as Java's
    `ConcurrentModificationException` and Python's `ConcurrentModificationError`
    (`python/confluent_kafka/concurrent_modification_error.py:31` at `3f8a5bc6`) are one
    type. This replaces M3/P2's split (a faulted `Task` carrying `KafkaException` for an
    async op).
  - **`ConsumerHandle` errors take the error table like every other call (decision 14).**
    Three conditions are handle-specific, all core behavior surfaced verbatim:
      - **The callback gate (ledger 104).** On a `KafkaConsumer` handle an async op
        (`Seek`, `Pause`, `Position`, `Commit`, `CurrentLag`, …) is admitted only while
        the consumer is running a callback (a `ConsumerRebalanceListener` method or an
        `OffsetCommitCallback`). Outside one it fails with -2 before anything is
        submitted, with the message "KafkaConsumer is not safe for multi-threaded access.
        ConsumerHandle operations are allowed only from inside a callback the consumer is
        running (ConsumerRebalanceListener, OffsetCommitCallback); from another thread call
        wakeup()." → `InvalidOperationException` (BH:30642-30648;
        `rust/src/consumer/internals/async_kafka_consumer.rs:307-309`, `:904-909` at
        `3f8a5bc6`). `Wakeup` and the sync reads (`Assignment`, `Paused`) work from any
        thread at any time. Java has no handle (a listener captures the consumer, and
        `acquire()` refuses other threads), and Python has no public one.
      - **A `MockConsumer` handle is not gated**, as Java's `MockConsumer` has no
        `acquire()`. `Wakeup`, the sync reads, `CurrentLag` and every commit work against
        the mock; every other async op fails with `LocalUnsupportedOperation` (−31) and
        the message "ConsumerHandle async operations are not supported on a MockConsumer
        handle; drive the MockConsumer directly." → `NotSupportedException`
        (`rust/src/consumer/internals/async_kafka_consumer.rs:841-844` at `9bff36d1`).
        **Assert it, do not work around it**, and assert the exact message (DoD §3),
        since the type alone does not tell it from another unsupported operation.
      - **After the consumer is destroyed** every handle op fails with -4 →
        `InvalidOperationException`, and a `_cb` handle op fires its `cb` inline (ledger
        34 (i); `rust/src/ffi/consumer/consumer_handle.rs:48-51` at `3f8a5bc6`).
    A blocking handle op runs as a task of the consumer and waits on a channel, whichever
    thread calls it, so it works from inside a listener that a blocking consumer op runs;
    the M9/P8 in-runtime `IllegalStateError` is gone
    (`rust/src/ffi/consumer/consumer_handle.rs:37-46` at `3f8a5bc6`).
  - **The core's local codes map by the error table (decision 14, M19/P1).** -2 and -4 →
    `InvalidOperationException`, -3 → `ArgumentException`, -5 → `System.TimeoutException`,
    -31 → `NotSupportedException`, each with the core's message; a `local_callback` error → the carried instance; every
    other code → `KafkaException`. This holds on every surface, the `ConsumerHandle`
    included, so a handle op and the same consumer op throw the same type
    (`HandleAndConsumer_ReportTheSameCoreError_Identically` keeps asserting it). It
    replaces the M9/P8 rule that kept `illegal_state` a `KafkaException`.
  - **The guard-bypass contrast is itself a contract, and it is testable.** The same
    logical call has two different surfaces depending on which handle it goes
    through, and the header states both: `Consumer_assignment` fails with -2 through
    `out_err` and returns `NULL` while another call holds the consumer (BH:30513-30524 at
    `3f8a5bc6`) → `InvalidOperationException`, while `ConsumerHandle_assignment` takes no
    guard and returns the owned list from any thread (BH:31850-31862, BH:30642-30648 at
    `3f8a5bc6`). Inside a callback that holds the guard the first is rejected and the
    second succeeds — the reason `ConsumerHandle` exists, and the mock-testable form of
    `consumer-threading.md` §31's first mandatory regression test (no broker, no threads,
    no sleeps). On the public surface the consumer's own `Assignment()` inside a callback
    is routed to the handle (ledger 114) and succeeds too; the contrast shows on a call
    sent to the core as outside, `Subscription()`, which throws `InvalidOperationException`
    (-2) in the same listener.

**Why:** the flat `KafkaException` with `Code` and `IsRetriable` matches what the ABI
exposes (decision 14), and the codes that stand for Java built-ins get their .NET type now,
so typed subclasses can come later without a break. Preconditions are a separate surface
because they are programmer errors, not Kafka outcomes — Java raises
`IllegalArgument`/`IllegalState`, Python its Java-named errors (`NullPointerError`,
`IllegalArgumentError`; root `CLAUDE.md:431` at `3f8a5bc6`), all before the native call;
ours must too, *and must* because the ABI would otherwise reach undefined behavior or a
core panic. The wakeup shape keeps Java's once-only semantics on the flat type (Java's
`WakeupException` and Python's `WakeupError` become a subclass later, the Note below).
Concurrent use is refused by the binding's owner check, as Java's `acquire()` and Python's use
table refuse it, and behind it by the core (ledger 136); since M19/P1 every rejection is the
same `InvalidOperationException`, sync or async (decision 17).

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
  - A per-code hierarchy now (decision 14 keeps one flat `KafkaException`); throwing
    `KafkaException` for a programmer error (the `GroupMetadata()` check of ledger 105 is
    not one: Java throws `InvalidGroupIdException` there); validating after the P/Invoke.
  - Making `wakeup()` a distinct exception subclass now (decision 14 keeps it flat; the
    Note above says how it is added later); conflating a `wakeup()`
    (`KafkaException`/Wakeup) with a `CancellationToken` cancel
    (`OperationCanceledException`).
  - Any type other than `InvalidOperationException` for a concurrent call (-2), a state
    read or an op, sync or async (decision 17).
  - Treating the −31 `NotSupportedException` on a mock-derived `ConsumerHandle`, or the -2 a
    `KafkaConsumer` handle op gets outside a callback (ledger 104), as a bug to route
    around (a managed pre-check, a silent no-op, an invented "mock unsupported"
    exception) — it is documented core behavior, and the binding's job is to surface it
    unchanged.
  - Branching on an error code outside the one error-table helper, or mapping a code the
    table does not list.
  - Adding an `InvalidOperationException` concurrent-rejection mapping to a **handle**
    sync read (`Assignment`, `Paused`). It has none to map: it takes no guard
    (BH:30642-30648 at `3f8a5bc6`).

**Tests required:**

  - A sync failure throws `KafkaException` with the right code/message/`IsRetriable`; an
    async failure faults the `Task` (via `MockConsumer.SetPollException`, decision 15); the
    handle is freed exactly once; a non-ASCII message round-trips.
  - Null topic/partition → `ArgumentNullException`; post-`Dispose` →
    `ObjectDisposedException`; each before any native call.
  - `GroupMetadata()` without `group.id` throws `KafkaException` with code 24 and Java's
    exact message; after `Dispose` it still throws `ObjectDisposedException` (ledger 105).
  - `wakeup()` → the next `poll`/`commit` throws `KafkaException` (Wakeup)
    **once**, then the consumer works again; a `CancellationToken` cancel →
    `OperationCanceledException`.
  - A concurrent sync state read → `InvalidOperationException`; a concurrent op, blocking
    or `_cb` → `InvalidOperationException` too (decision 17).
  - **`ConsumerHandle`:** on a `KafkaConsumer` handle an op outside a callback throws
    `InvalidOperationException` with the exact gate message (ledger 104); on a
    mock-derived handle `Wakeup`, the sync reads, `CurrentLag` and the commits succeed
    (`CommitAsync(offsets)` from a listener commits into the mock), and every other async
    op throws `NotSupportedException` **and the exact message**; after the consumer's destroy every handle op throws
    `InvalidOperationException`. And the guard-bypass contrast — inside a listener fired
    by `MockConsumer.Rebalance`, `handle.Assignment()` **succeeds**, the same listener's
    `consumer.Assignment()` is routed to the handle (ledger 114) and returns the same
    assignment, and its `consumer.Subscription()` throws `InvalidOperationException`.
    ⚠ That last one needs a **mutation check** (turn the routing off and confirm the
    consumer's `Assignment()` fails), or a regression that stops routing passes it
    silently.

---

## B6 Callback & delegate marshalling — completion callbacks

**Decision:** Three callback *families* cross this boundary, with **different lifetimes**.
All marshal as a rooted `[UnmanagedFunctionPointer(Cdecl)]` delegate (classic, no function
pointers on the floor) with a no-throw body. Since M19/P1 no consumer callback fires on a
native thread of its own: each runs inside `Consumer__execute_callbacks` (on the async
consumer's drain thread, §B1, or on the closing thread during destroy) or, on the sync
consumer, inline in the blocking call, on its caller's thread.

  - **One-shot `_cb` completions** — the consumer's `_cb` twins (`poll_cb`,
    `commit_sync*_cb`, `commit_async*_cb`, `position*_cb`, `committed*_cb`,
    `current_lag_cb`, …), the **primary** mechanism for every async op. Each fires
    **once** per call with an owned `value` and/or an owned `error` and the `opaque` the
    binding passed, also when the core rejects the call as concurrent (the -2 is queued
    like any completion, §B5). No registration handle, no release hook: the `cb` is the
    sole owner of the per-op `GCHandle` free.
  - **Multi-shot registrations** — the **rebalance listener**
    (`ConsumerRebalanceListener_new_err(self_, revoked, assigned, lost)`, passed to
    `subscribe_*`). Registered once, fires **N** times, bound to a *subscription* rather
    than an operation. Each listener method reports through `Consumer__set_callback_result`
    (BH:3667-3695 at `3f8a5bc6`). The `subscribe_*` call copies the registration, so its
    handle may be destroyed right after (BH:29283-29287 at `3f8a5bc6`); `self_` is a
    `GCHandle` freed by decision 23's rule (BH:32327-32341 at `3f8a5bc6`).
  - **One-shot registrations** — the **offset-commit callback**
    (`OffsetCommitCallback_new_err(self_, on_complete)`, passed to
    `commit_async*_with_callback` and their `_cb` twins). Fires **at most once**, bound to
    an *operation*. The call copies the registration (BH:29628-29646 at `3f8a5bc6`);
    `self_` stays alive until `onComplete` fired, or until the consumer was destroyed
    (BH:33692-33703 at `3f8a5bc6`).

⚠ **There is no `user_data_destroy` hook any more** (M19/P1). The earlier ABI freed a
registration's `GCHandle` from a release hook; BH has none for a registration (no
`user_data_destroy` anywhere in BH at `3f8a5bc6`), so the binding decides each `self_` free
itself, by the header's rule quoted in each Rule below. The one release hook left is a
`local_callback` error's `release(opaque)`, which the core calls exactly once, on any
thread, when the last copy of the error is dropped (BH:2030-2036, BH:17703-17733 at
`3f8a5bc6`; ledger 12). The one-shot "the callback frees it" invariant holds for the `_cb`
completions only: applied to a listener it is a use-after-free on fire 2..N.

The `_cb` shape is `(value, error, opaque)`: an owned `value` (a handle or a scalar; none for
a void op) on success, `NULL` (`-1` for a scalar) beside an owned `error` otherwise
(`kafka_consumer_Consumer_poll_cb_t`, `kafka_consumer_Consumer_position_cb_t` and their
siblings, BH:3103-3109, BH:3203-3209 at `3f8a5bc6`).

The listener shape is one function per Java method, `(self_, partitions, callback_id)`, the
partitions borrowed for the call; a `NULL` `on_partitions_lost` is Java's default
(BH:3667-3695 at `3f8a5bc6`). The commit shape is `on_complete(self_, offsets, error,
callback_id)`, both borrowed (BH:3777-3792 at `3f8a5bc6`). Both report their end with
`Consumer__set_callback_result(consumer, callback_id, NULL, error)`, where `error` is `NULL`
or an owned error the core takes over: from any thread, once per id (a second report is
ignored), and never after the consumer's destroy has returned (BH:29168-29209 at
`3f8a5bc6`).

**Rule (one-shot `_cb` completion):**

  - A named Cdecl delegate per `cb` shape, blittable parameters, rooted in a
    `static readonly` field so the GC can't collect it while native holds the thunk.
  - **No-throw is mandatory**: the frame below is Rust. `try/catch` all, and surface a
    failure through the TCS.
  - **Per-op keep-alive:** the `opaque` `GCHandle` over the op's state lives from the call
    to the `cb`. The `cb` frees it, exactly once, on every path, the queued -2 rejection
    included; the only other free site is a `catch` around the P/Invoke when it threw
    before native ran.
  - **The `cb` owns what it gets:** marshal the owned `value` (copy-out, CLAUDE.md §6.4)
    and destroy it (§B2 Category 3), or build the exception from the owned `error` (§B5),
    which frees it; a scalar `value` needs no free. Then complete the TCS.

**Rule (multi-shot registration — the rebalance listener):**

  - One named Cdecl delegate per listener method, rooted in a `static readonly` field (a
    registration has no per-op handle to root it).
  - **No-throw is mandatory.** A listener exception is reported, not thrown: build a
    `local_callback` error carrying it (`kafka_common_Error_local_callback_err`, `opaque`
    a `GCHandle` over the exception, `release` a no-throw delegate that frees that
    `GCHandle`; ledger 12) and pass it to `Consumer__set_callback_result`. The core takes
    it over, and it propagates out of the consumer call that ran the listener as the same
    instance.
  - **The partitions are borrowed** for the call: copy them out; never destroy them.
  - **Report exactly once per call**, on every path, including a method that throws
    before it returns a `Task` (the async listener, §B1). The async listener reports from
    its `Task`'s continuation, so the drain thread never waits on user code (decision 27).
  - **`self_` lifetime** (decision 23): free the old listener's `GCHandle` when a
    replacing `subscribe_*` succeeds, the new one when the `subscribe_*` fails, and the
    current one after the consumer's destroy has returned. Keep it across `unsubscribe`:
    the header says `unsubscribe` releases the listener (BH:29392-29394 at `3f8a5bc6`), but
    Java's `unsubscribe()` leaves it registered (`SubscriptionState.java:347-355`), and
    Python keeps its context across it (`python/_confluentkafka.c:3870-3879`, `:3947-3954`
    at `3f8a5bc6`).
  - Destroy the `ConsumerRebalanceListener_t` handle right after the `subscribe_*` call:
    the call copied the registration (BH:29283-29287 at `3f8a5bc6`).

**Rule (one-shot registration — the offset-commit callback):**

  - A named Cdecl delegate, rooted in a `static readonly` field; no-throw.
  - **`offsets` and `error` are borrowed** for the call (BH:3777-3792 at `3f8a5bc6`):
    copy out, never destroy; the core frees both after the call
    (`rust/src/ffi/consumer/offset_commit_callback.rs:97-114` at `3f8a5bc6`).
  - **A throwing user callback is reported**, not swallowed (decision 12): a
    `local_callback` error, built as the listener's, through
    `Consumer__set_callback_result`, which propagates out of the consumer call that ran the
    callback (`Poll`, `Commit*`, `Close`).
  - **`self_` lifetime:** free it after `on_complete` has reported. When the call fails (an
    initiation error, a -2 included), keep it until the consumer's destroy has returned,
    then free it: the header keeps `self_` alive "until `onComplete` fired (or the consumer
    was destroyed)" (BH:33692-33703 at `3f8a5bc6`). Python does the same: it parks the
    context of a failed commit on the consumer and frees it after destroy
    (`python/_confluentkafka.c:3881-3892`, `:5246-5262`, `:4312-4316` at `3f8a5bc6`).
  - Destroy the `OffsetCommitCallback_t` handle right after the call: the call copied the
    registration (BH:29628-29646 at `3f8a5bc6`).
  - **Java's `commitAsync(Map, null)`:** `callback` must be a live handle; `NULL` is
    undefined behavior (`rust/src/ffi/consumer/offset_commit_callback.rs:155-185` at
    `3f8a5bc6`). Pass a registration with no user callback, built by the same helper over
    the same per-call state, whose `on_complete` only reports `NULL` through
    `__set_callback_result` on the consumer that state names, since the report needs the
    consumer (BH:29168-29209 at `3f8a5bc6`). Python does the same, with a `NULL` adapter
    in the same context (`python/_confluentkafka.c:3887-3888`, `:5246` at `3f8a5bc6`).
  - **Build each registration in one place:** a single named helper that production and
    the tests both call (`definition-of-done.md` §12).

**The async commit callback (M19/P1, ledger 47).** `IAsyncOffsetCommitCallback` uses the
sync commit callback's `OffsetCommitCallback_t` and trampoline. The trampoline copies the
offsets and converts the error before it returns (both are freed after the call), calls the
callback, and does not wait for its `Task`. The `Task`'s continuation reports through
`kafka_consumer_Consumer__set_callback_result`, from any thread: `NULL`, or a
`local_callback` error carrying the exception, a cancel included. Exactly one report per
call, on every path. The report goes through the borrowed `__as_Consumer` view, not the
SafeHandle: the destroy runs the pending callbacks inside the handle's release, where a
SafeHandle argument is refused, and returns only after every report it waits for
(`kafka_consumer_Consumer__set_callback_result` is callable while the destroy runs, never once
it returned, BH:29188-29192 and BH:33145-33165 at `3f8a5bc6`). A report made after the destroy
returned is dropped by the binding and its error freed. Inside
a blocking `ConsumerHandle` commit, which runs the commit callbacks queued behind it on the
waiting thread (`rust/src/ffi/consumer/mod.rs:188-226` at `3f8a5bc6`), the callback is
reported to the core at once and its `Task` runs on; the callbacks after it may start before
it ends. The call that delivered the outer callback waits for the `Task`, then throws the
exception it threw, wrapped as that call wraps a callback's; a `Poll` instead returns its
records, and the next awaited call throws the exception (ledger 52).

The async *flow* (call → drain thread → `SetResult` with `RunContinuationsAsynchronously`;
one op in flight; teardown) is **§B7**. This section is only the callback *marshalling*.
Neither a registration nor a commit callback has a `Task` of its own: the registration is
bound to the subscription's lifetime, and the commit's outcome reaches the callback, not the
`Task` of `IAsyncConsumer.CommitAsync` (decision 13).

**Why:** the floor (netstandard2.0/net462) has no function pointers or
`[UnmanagedCallersOnly]`, so a kept-alive Cdecl delegate is the only portable
mechanism — the pattern confluent-kafka-dotnet uses. The GC sees only managed
refs, not the native thunk (→ keep-alive); `opaque` / `self_` is C's only per-call context
channel (→ `GCHandle`). No-throw is not optional, because the frame below every callback is
Rust, and the keep-alive spans the whole op (call → `cb`) or the whole registration, not one
synchronous call.

**Anti-patterns:**

  - A per-op delegate / `GCHandle` not rooted for the whole call → `cb` window
    (collected mid-op → crash); a **registration** delegate that is not
    `static readonly` (a registration has no per-op handle to root the thunk).
  - An exception escaping a callback into native (UB).
  - Not freeing the owned result/error + `GCHandle` on some path (esp. the queued -2
    rejection).
  - Reading a borrowed `topic`/bytes pointer after its batch is destroyed (§B3/§B4).
  - **Listener:** freeing `self_` from a listener method (a use-after-free on fire 2..N),
    at `unsubscribe`, or before the consumer's destroy has returned; destroying the
    delivered partitions (borrowed: a double free); destroying the error passed to
    `__set_callback_result` (the core took it over); reporting twice, or not at all, for
    one `callback_id`.
  - **Commit callback:** freeing `self_` when the call failed (the callback may still
    fire); destroying the delivered offsets or error; swallowing a user exception instead
    of reporting it (decision 12); passing `NULL` for `callback` (UB).
  - A `catch` whose diagnostic blames "the user's callback" when it also covers
    marshalling and `GCHandle` recovery, or a diagnostic that can itself throw.

**Tests required:**

  - Each callback delivers the right result/error and frees the owned handle +
    `GCHandle` exactly once.
  - An exception thrown in a callback is caught (no crash, no unwind into native). For a
    `_cb` it faults the `Task`; for a listener or commit callback it is reported through
    `__set_callback_result` and surfaces from the call that ran it, with the same instance
    (assert the type **and** the message, `definition-of-done.md` §3).
  - Aggressive GC during an in-flight op doesn't collect the delegate (keep-alive
    across call → `cb`) — and, for a registration, across the whole **live
    registration**, which is a much wider window.
  - **Listener:** the registration survives **N** fires (a churn loop); a replacing
    subscribe frees the old `self_`; a failed subscribe frees the new one; an
    `unsubscribe` frees nothing; destroy frees the current one after it returns; and
    teardown with a live registration returns without hanging.
  - **Commit callback:** after `on_complete` reports, `self_` is freed once; after an
    initiation error it is kept until destroy, then freed once; a throwing callback's
    exception surfaces from the call that ran it; `CommitAsync(offsets)` with no callback
    commits and its registration reports once; and the registration the tests pass comes
    from production's own helper (`definition-of-done.md` §12).
  - **Async commit callback:** one report per call, a faulted or cancelled `Task`
    included, and its exception surfaces from the call that delivered the callback; inside
    a blocking `ConsumerHandle` commit the callback is reported at once, and the delivering
    call throws after its `Task` ends (ledger 52).

---

## B7 Async / completion → `TaskCompletionSource` (the async consumer's drain thread)

**Decision (M19/P1, decision 27):** `IAsyncConsumer` calls each op's `_cb` twin, and the
`cb` completes a `TaskCompletionSource` built with `RunContinuationsAsynchronously`. The
`cb`, like every listener and commit callback, runs inside `Consumer__execute_callbacks` on
the consumer's drain thread `confluent-kafka-consumer-pump`, woken by the notify hook (§B1).
Single-owner, one op in flight: the binding's owner check, then the core's flag (§B1, ledger
136). An async call's use ends before its `Task` completes, so the next awaited call passes the
check whichever thread it resumes on.
`IConsumer` has no flow here: it calls the blocking forms (§B1).

⚠ **Scope — this section is about the `_cb` completions that complete a `Task`.** A listener
has no `Task` of its own (§B6). The `Task` of `IAsyncConsumer.CommitAsync` is its
`commit_async*_cb` completion: it says the core accepted the commit (decision 13). The
commit's outcome reaches the callback, not that `Task` (§B5, §B6).

```
Caller thread                       Rust core                       confluent-kafka-consumer-pump
─────────────                       ─────────                       ─────────────────────────────
Poll():                             runtime task: poll(timeout)
  tcs = new TCS(RunContinuationsAsynchronously)
  op  = GCHandle.Alloc(state)
  Consumer_poll_cb(view, t, cb, op) ─► done → queue the cb
  return tcs.Task                     empty → non-empty: hook ─────► signal
  … await tcs.Task                                                   __execute_callbacks(view):
                                                                       cb(records | error, op):
                                                                         marshal, free owned (§B2)
     ◄── continuation on the pool ────────────────────────────────────   tcs.Set*, free op
```

**Rule:**

  - Each async op makes a `TaskCompletionSource` with `RunContinuationsAsynchronously` and
    a `GCHandle` over its state as the `opaque`, calls the `_cb` twin with a rooted Cdecl
    `cb`, and returns `tcs.Task` at once. No thread blocks.
  - The `cb` runs on the drain thread, or on the closing thread during destroy (§B2). It
    marshals the owned `value` (copy-out, CLAUDE.md §6.4) or the owned `error` (the error
    table, §B5), frees what it owns (§B2 Category 3), completes the TCS exactly once, and
    frees the `opaque`. It is the sole owner of that free; the only other site is the
    `catch` around a P/Invoke that threw before native ran.
  - **One op in flight** per consumer. A `_cb` call while another op runs gets its `cb`
    with -2, queued like any completion, and the `Task` faults with
    `InvalidOperationException` (decision 17, §B5). The core releases its guard before it
    queues a completion (`rust/src/ffi/consumer/mod.rs:700-712` at `3f8a5bc6`), so an
    `await`-then-resubmit from the continuation is not rejected.
  - No continuation runs on the drain thread, and the drain thread never waits on user
    code: an async listener reports from its `Task`'s continuation (§B1).
  - **Cancellation**, for an op Java's `wakeup()` interrupts (`Poll`, `Commit`, `Position`,
    `Committed`, `PartitionsFor`, `ListTopics`, §B5) = `CancellationToken` →
    `Consumer_wakeup_err`, which ends the in-flight op sooner (best-effort;
    consumer-threading §11). Any other op's token sends no wakeup: the op would not
    consume it, and it would then fail the next wakeable call. Either way the op runs to
    its end (`CurrentLag` included, ledger 103), and once the token has fired its `Task`
    ends with `OperationCanceledException` whatever the op returned (ledger 123): a
    success or a failure that lands after the cancel is freed and dropped, a commit
    callback's exception the call delivered is kept for the next awaited call, and a
    dropped `Poll` result first seeks each partition back to its first record not
    returned, so none is skipped, as on Python's async consumer
    (`python/confluent_kafka/consumer/_base.py:1054-1062`, `:1123-1131`, `:1161-1168`,
    `:1343-1352` at `3f8a5bc6`).
  - **Teardown:** see §B2's path list; do not restate it. The one fact §B7 adds: destroy
    first wakes the op still waiting, then awaits the `_cb` ops in flight and runs every
    pending `cb` on the closing thread, each once, so every `Task` settles
    (BH:33145-33170 at `3f8a5bc6`).

**Why:** one named drain thread is the closest analogue of Python's event loop: FIFO and
serialized (decision 27). `RunContinuationsAsynchronously` keeps the awaiter's continuation
off it; otherwise one slow continuation stalls every later callback, and a continuation that
calls back into the consumer can block the drain. Single ownership is the owner check's and
the core's flag's (§B1, ledger 136).

**Anti-patterns:**

  - A TCS without `RunContinuationsAsynchronously`.
  - An async call that still counts as a use once its `Task` completed, or an owner check
    on a call made inside a callback (ledger 114 routes it to `ConsumerHandle`; §B1,
    ledger 136).
  - Wrapping a blocking form in `Task.Run` per op (sync-over-async): every async op has a
    `_cb` twin.
  - Letting a `cb` throw into native; completing a TCS twice; freeing the per-op
    `GCHandle` anywhere but the `cb` or the `catch` around a P/Invoke that never reached
    native.
  - A wakeup for a cancelled op Java's `wakeup()` does not interrupt (it stays armed and
    fails the next wakeable call).
  - Re-adding the M3/P1 `Dispose`-side `FaultTaskOnly` machinery: destroy settles every
    `Task` (BH:33145-33170 at `3f8a5bc6`), so it would only add a second `GCHandle`-free
    site, a use-after-free against a straggler `cb`.
  - Releasing the span-the-op ref anywhere but `FreeGcHandle`, or taking the
    `AddRef` **outside** the `try` that routes a failure through
    `AbandonBeforeSubmit` — an `AddRef` throw would then skip the abandon path and
    permanently root the per-op `GCHandle` (M9/P4 M3).
  - Destroying the consumer inside `__execute_callbacks` (§0.3, §B2).

**Tests required:**

  - `Poll` resolves with records, or faults with the exception set by
    `MockConsumer.SetPollException` (decision 15); the owned handles and the `GCHandle` are
    freed exactly once.
  - `wakeup()` during an in-flight `Poll` ends the `Task` **once**, then the consumer works
    again (§B5); a `CancellationToken` cancel → `OperationCanceledException`.
  - A cancelled `CurrentLag` sends no wakeup: the next `Poll` is not woken (ledger 103).
  - A concurrent async op → a faulted `Task` with `InvalidOperationException`; a concurrent
    state read → `InvalidOperationException` (§B5, decision 17).
  - An `await`-then-resubmit from the continuation succeeds (no -2).
  - The `cb`s, listeners and commit callbacks run on `confluent-kafka-consumer-pump`; no
    continuation runs on it.
  - `Dispose` / `DisposeAsync` return without hanging with an op in flight, and every
    pending `Task` settles (routes: §B2's path list).

---

# Part C · Admin

## C1 Drain thread, futures and teardown

**Decision (M19/P1, decisions 5, 8, 4a):** every RPC returns its Result at once, as Java's
does (`.claude/rules/admin-client.md` §1). Each future in it is a `KafkaFutureImpl`. When the
Result is built, each native future gets one `kafka_common_KafkaFuture_get_cb`, and its delivery
completes the `KafkaFutureImpl` on the client's drain thread. Both flavours share this path; only
the public struct differs (`KafkaFuture` or `AsyncKafkaFuture`, `dotnet/CLAUDE.md` §4).

The C shapes (BH at `25a4d475`):

  - `kafka_admin_Admin_<rpc>[_with_options]_err` returns an owned `<Rpc>Result_t`. An accessor
    returns an owned future, a `kafka_Map_t` that owns its futures (`kafka_Map_destroy` frees
    them, BH:6189-6193), or a future the Result owns (six accessors: BH:6025, BH:6717, BH:6846,
    BH:7078, BH:7674, BH:13797). `ListTransactionsResult_by_broker_id` returns an owned future
    whose value is a map of futures owned by that map (BH:8664-8680).
  - `get_cb`'s `cb` runs inside the client's `__execute_callbacks`. A call still waiting at the
    client's destroy fires from `_destroy`; one after it fires inline (BH:37968-37983). The
    `value` is borrowed; the `error` is owned and the `cb` frees it (BH:3803-3809).
  - A value stays valid until the handle is destroyed, and during a delivery until the `cb`
    returns (BH:38126-38128). An admin future's `get` runs no callbacks
    (`rust/src/ffi/admin/mod.rs:520-523`, `rust/src/ffi/kafka_future.rs:394-400` at `25a4d475`),
    so a done one is read at once (Python's `get_if_done`, `python/_confluentkafka.c:1387-1394`).
  - `kafka_admin_Admin_destroy_err` fails the calls still pending, then fires every pending
    callback on the destroying thread; no other call may use the handle meanwhile (BH:4221-4237;
    `rust/src/ffi/admin/mod.rs:58-66` at `25a4d475`).

**Rule:**

  - **Drain thread** (decision 26, §0.3): one `CompletionThread` per client, the mocks included
    (over their `__as_Admin` view, BH:9208): `kafka-admin-client-thread` on the sync flavour,
    `confluent-kafka-admin-pump` on the async one. It runs `kafka_admin_Admin__execute_callbacks_err`
    when the notify hook signals.
  - **Eager `get_cb`** (decision 5): one per native future, issued when the Result is built.
    Python issues it on first use (`python/confluent_kafka/common/kafka_future.py:445-461` at
    `25a4d475`); only the time differs.
  - **One SafeHandle per owned future** (decision 8), released in its own delivery after the
    managed state is settled. The `cb` copies the borrowed `value` out first (CLAUDE.md §6.4) and
    frees the owned `error`.
  - **A borrowed future holds a reference on its owner** until its own delivery: the six
    accessors' futures on the Result's SafeHandle, `by_broker_id`'s inner futures on the outer
    future's. The Result keeps its own reference for the managed Result's life; its finalizer
    releases it (a Result's destroy runs no callbacks and blocks nothing). The outer
    `by_broker_id` delivery issues the inner `get_cb`s while its value is alive, then releases the
    outer future's own reference. The inner map is built once, by whichever comes first: the outer
    delivery, or a state read that settles the outer future.
    A future in a map is borrowed from the map: a SafeHandle holds the map, and each of its
    futures holds a reference on it until its own delivery, as Python's map futures keep the map's
    capsule alive (`python/_confluentkafka.c:1555-1571` at `25a4d475`; ledger 182).
  - **Reads** (decision 8, ledger 56): while the future is pending, its value source
    (`IFutureValueSource<T>`) reads the handle. `IsDone` asks `kafka_common_KafkaFuture_is_done_err`
    and settles nothing. `GetNow`, `IsCancelled`, `IsCompletedExceptionally` and `ToString` settle
    a done handle from `kafka_common_KafkaFuture_get`. `Get()`, `Get(TimeSpan)` and
    `Get(CancellationToken)` wait in slices and re-read the handle each slice, on any thread, as
    Python's `get` does (`kafka_future.py:702-720` at `25a4d475`).
  - Every read P/Invoke takes the SafeHandle as its parameter (decision 6), so the release cannot
    run during a read. A read that finds the handle closed reads the managed state, which is
    settled by then.
  - **No own-thread guard** (ledger 39). A native-backed future answers a `Get` on the drain thread
    from its handle. A derived future (`ThenApply`, `WhenComplete`, `AllOf`) has no handle: a `Get`
    on the thread that must complete it waits, as in Java and Python.
  - **Composition** (decision 4a): `All()` and the refinements are built over `KafkaFutureImpl`
    (`AllOf`, `ThenApply`) where Java builds them with `allOf` / `thenApply`. A Result whose Java
    `all()` is its own future takes that future. This is Python's split (root `CLAUDE.md`,
    Futures). The C `kafka_common_KafkaFuture_all_of` and `_then_apply` are not used.
  - **Options** (ledger 176): each RPC builds its C options with `<X>Options_new`, which carries
    the core's Java defaults, then calls `set_<field>` only for the setters that ran, in the order
    they last ran (a setter run twice counts at its last run), as Python does. Each Options
    property's setter records, in private state, that it ran; the managed starting value feeds only
    the getter. The RPC copies the C options during the call (BH,
    `kafka_admin_Admin_create_topics_with_options`), and the binding destroys them right after it.
    A setter whose Java form takes a primitive (`DescribeFeaturesOptions.NodeId`,
    `DescribeProducersOptions.BrokerId`) throws `ArgumentNullException` for `null`, as Python's
    `TypeError`, and records nothing (ledger 181).
  - **Teardown** (decision 7): `dotnet/CLAUDE.md` §4 **Close/Dispose**. `Close()`, `Dispose` and
    `DisposeAsync` call `kafka_admin_Admin_close_cb`, which waits without a limit, as Java's
    `close()`. `Close(TimeSpan)` calls `kafka_admin_Admin_close_with_timeout_cb`. On
    `KafkaAdminClient` and `AsyncKafkaAdminClient` a negative timeout throws
    `ArgumentOutOfRangeException("The timeout cannot be negative.")` before the closed check, as
    Java does (ledgers 82, 174). The sync flavour waits on a `Completion`, the async one on a
    `TaskCompletionSource` with `RunContinuationsAsynchronously`; its `CancellationToken` ends only
    the caller's wait: the close goes on in the core, and the rest of the teardown (join the drain
    thread, wait for the in-flight uses, destroy) runs when the close ends, as Python's cancelled
    `await close()` (ledger 175). The destroy fires the pending deliveries on the closing thread,
    which releases their SafeHandles there. A `Close` from a delivery (a `ThenApply` function, a
    `WhenComplete` action) schedules the teardown, which the drain thread runs after its drain, as
    Python's `pump.stop(after=…)` (`python/confluent_kafka/admin/admin.py:505` at `25a4d475`).
  - **Mocks** (decision 21): `MockAdminClient.Close` and `AsyncMockAdminClient.Close` do nothing
    and read no timeout, as Java's mock (`MockAdminClient.java:1535`) and Python's
    (`admin/mock_admin_client.py:307-309` at `25a4d475`). The mock stays usable. `Dispose` runs the
    teardown with `kafka_admin_MockAdminClient_destroy_err` (BH:9361).

**Why:** the async flavour needs `get_cb` anyway, so one path serves both (decision 5). Keeping each
handle until its delivery lets every state read ask the core first, as Python does, with no `IsDone`
lag and no drain-thread guard (decision 8).

**Anti-patterns:**

  - A blocking `kafka_common_KafkaFuture_get` on a pending future, or `Task.Run` around one.
  - A second `get_cb` on one future, or none.
  - Destroying an owned future before its delivery ran, or a borrowed one at all; releasing an
    owner before the last future that borrows from it has been delivered.
  - Reading the delivered `value` after the `cb` returned.
  - An own-thread `Get` guard on a native-backed future (decision 8, ledger 39).
  - The C `all_of` / `then_apply` for `All()` or a refinement.
  - A sync Result composed over Tasks (`Task.WhenAll`, an `async` projection, `ContinueWith`)
    and then blocked on (`GetAwaiter().GetResult()`, `.Wait()`, `.Result`). With
    `RunContinuationsAsynchronously` the composite needs a pool thread to complete, so a saturated
    pool starves it. Compose over `KafkaFutureImpl` instead: its `Get` waits on the calling thread.
  - A `cb` that throws into native, or leaves its owned `error` unfreed.
  - Destroying the client inside `__execute_callbacks` (§0.3).
  - A mock `Close` that closes, frees or checks anything.

**Tests required:**

  - `IsDone` and `GetNow` answer from the handle before the delivery runs.
  - A `WhenComplete` action on future A that calls `B.Get()` for a done B answers on the drain thread.
  - `by_broker_id`'s inner futures stay readable after the outer delivery.
  - A `GetNow` that settles `ByBrokerId()` before its delivery builds the inner map once. The
    delivery then issues one `get_cb` per inner future, and the futures the read returned are the
    ones the delivery completes.
  - An RPC whose Options had no setter run sends the core's defaults. A setter run twice sends its
    last value. `DescribeFeatures` without `NodeId` set sends no node id.
  - Setting `NodeId` or `BrokerId` to `null` throws and leaves the field unset.
  - After every map future's delivery has run, the map is destroyed once; a map future's `IsDone`
    before its delivery answers from the handle.
  - A client destroyed with pending futures settles each of them and releases every future
    SafeHandle exactly once.
  - Every `get_cb` error is freed once; the deliveries run on the named drain thread.
  - A mock `Close` changes nothing: a later RPC works, and `Dispose` frees the mock.
