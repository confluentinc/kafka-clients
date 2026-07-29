# Confluent Kafka .NET Binding — Claude Rules

**What this is.** The rulebook for the .NET binding under `bindings/dotnet/` — a
P/Invoke wrapper that restores Apache Kafka's **Java client API shape** in
idiomatic C# on top of the Rust core's C ABI. It holds the .NET binding's
*shape, decisions, and governance*; the boundary *mechanics* live in
`.claude/rules/ffi-marshalling.md`.

**How it loads.** Working anywhere under `bindings/dotnet/` stacks three
rulebooks: root `CLAUDE.md` → `bindings/CLAUDE.md` → this file. Heavy deep-dives
live in `.claude/rules/*.md` and are read **on demand** — this rulebook links
them explicitly (never rely on nested auto-loading).

**The one law:** the binding restores the Java **shape** in C# **idiom** and holds **no Kafka
logic** — batching, partitioning, retries, offsets all live once, in the Rust
core. So it's **not a 1:1 mirror** — expect host-only types with no Java/C-ABI
counterpart: `Native`, `SafeHandle` subclasses, `IDisposable`, marshalling
helpers, the C-callback→delegate adapter, the `TaskCompletionSource` bridge.
That's expected scaffolding — plumbing for the shape + safe resource management,
never Kafka behavior. If you're writing Kafka behavior in C#, you're in the wrong
layer.

---

## 1 · Orientation

**Four layers.** Java client (the shape) → Rust core (`src/…`, all logic) → C ABI
(`src/ffi/*.rs` → generated `target/include/confluent_kafka.h`) → **.NET binding**
(a `Native` P/Invoke class + `SafeHandle`s + managed types).

**One call, end to end** (mechanics → ffi-marshalling.md):

```
producer.SendAsync(record)  ──►  Task<RecordMetadata>
   validate args · pin key/value · make TaskCompletionSource
        │  P/Invoke: Native.Producer_send(handle, …, out err) → future handle
        │  completion pump blocks on get_all(), completes each TCS
        │    (Option A — pull-pump shown; the producer's completion model is
        │     OPEN, push is Option B — ffi §A7. The consumer is push-only, no pump.)
        ▼
   C ABI  kafka_producer_Producer_send / _get_all   (flat pointers, out-params)
        ▼
   Rust   Producer::send(ProducerRecord) → KafkaFuture<RecordMetadata>   (the logic)
```

**Status:** The C ABI exposes the **producer** and **consumer**. Blocking ops
generally come in a sync form plus a callback-based `_async` form, but the
pairing is not uniform: instantaneous ops are sync-only by design
(`assignment`/`subscription`/`paused`/`client_id`/`group_metadata`/
`current_lag`/`wakeup`/`enforce_rebalance`), and the parameterized variants
`close_with_timeout` / `seek_with_metadata` have **no** `_async` form — so
there is no timed *async* close (see §4 Disposal). ⚠ `Consumer_commit_async`
is Java's `commitAsync` — a *sync* call returning `KafkaError*`, **not** a push
variant (the push variant of `commitSync` is `commit_sync_async`);
`poll_async` is the only `_async` fn taking a timeout. Admin / transactions are
**not** exposed yet. Source of truth for the surface = `src/ffi/*.rs` +
`cbindgen.toml` (the header is generated, not checked in).

---

## 2 · Project layout

**Intended file map** — the idiomatic dotnet/runtime layout: top-level `src/`
and `tests/` siblings under `bindings/dotnet/`, one folder per project (the
shared solution + build config stay at the `bindings/dotnet/` root). The library
project is split by visibility so the public *shape* is auditable at a glance and
the unsafe boundary is quarantined:

```
bindings/dotnet/
├─ Confluent.Kafka.ShareConsumer.sln    ← solution + shared config at the root
├─ Directory.Build.props · .editorconfig · .gitignore
├─ src/
│  └─ Confluent.Kafka.ShareConsumer/    ← the library project (named for the package id, §4)
│     ├─ <public API — flat at the root; topical folders (e.g. Admin/) as families grow>
│     └─ Internal/                       ← internal scaffolding — the ONLY non-public folder
│        └─ Interop/                      ← P/Invoke boundary — unsafe lives only here
└─ tests/
   └─ Confluent.Kafka.ShareConsumer.UnitTests/   ← Mock* unit tests
```

(Folders are organizational; C# accessibility is still the `internal` keyword +
the assembly.) **`Internal/` is the visibility marker — the *only* folder that
implies non-public:** every type under it is explicitly `internal` (and `sealed`
where practical), and `Internal/Interop/` is the only place `unsafe` appears.
Wanting a type under `Internal/` to be `public` is the signal it belongs in the
public tree. Public API lives **at or below** the library project root: flat at
the root while the surface is small, moving into **topical** folders (with the
matching child namespace) once a family gets large — in .NET, folders
conventionally mirror namespaces/topics, *not* accessibility, and a flat root
does not scale (ckd needed `Admin/` for 107 public types, plus `Exceptions/`,
and its root is still ~90 files). So a public `Admin/` folder is expected and
correct; what is never allowed is a public type under `Internal/`. There is no
inner `src/` inside the project: the outer top-level `src/` *is* the library
project's parent.

- **library project root** (`src/Confluent.Kafka.ShareConsumer/`) — **all public
  API** (namespace `Confluent.Kafka.ShareConsumer`), whatever the C# kind: the
  client types *and* supporting value types / enums (`ProducerRecord`,
  `RecordMetadata`, `Headers`, `TopicPartition`, later `ConsumerRecord` /
  `OffsetAndMetadata` / enums). If a user can name it, it lives here — flat while
  the surface is small, in a **topical** subfolder with the matching child
  namespace (e.g. `Admin/` → `Confluent.Kafka.ShareConsumer.Admin`, ckd's
  precedent) once a family grows.
- `Internal/` — **internal** managed scaffolding
  (`Confluent.Kafka.ShareConsumer.Internal`): the async-completion bridge (the
  consumer's callback→`TaskCompletionSource` adapter; the producer's pull-pump
  *or* push adapter — open, ffi §A7), config → properties marshalling.
- `Internal/Interop/` — the **P/Invoke boundary**
  (`Confluent.Kafka.ShareConsumer.Internal.Interop`): the `Native` `[DllImport]`
  class, `SafeHandle`s, `Utf8` helpers, callback delegates, and the blittable
  `[StructLayout]` mirror structs (e.g. the `ProducerRecord_t` mirror — the
  interop twin of the public `ProducerRecord`). `unsafe` lives only here; 1:1
  with `ffi-marshalling.md`.
- `tests/Confluent.Kafka.ShareConsumer.UnitTests/` — `Mock*` unit tests
  (`MockProducer`; `MockConsumer` as the consumer lands), a top-level sibling of
  `src/` in its own project directory **outside** the library tree — so default
  SDK compile globbing never pulls test files into the library assembly (no
  compile-scoping hack needed); `InternalsVisibleTo` grants access to internals.

---

## 3 · The target: the .NET API shape

Mirror the **Java** client in idiomatic C#. The producer surface we're building
toward (bytes-only interim per the serializer decision in §4):

```csharp
namespace Confluent.Kafka.ShareConsumer;   // interim id; folds into Confluent.Kafka later — §4

public sealed class ProducerRecord {
    public string Topic { get; }
    public int? Partition { get; }               // null = let the producer choose
    public long? Timestamp { get; }              // null = producer stamps it
    public ReadOnlyMemory<byte>? Key { get; }    // null = no key
    public ReadOnlyMemory<byte>? Value { get; }  // null = tombstone
    public ProducerRecord(string topic, ReadOnlyMemory<byte>? value,
        ReadOnlyMemory<byte>? key = null, int? partition = null, long? timestamp = null);
}

public sealed class RecordMetadata {            // getters → properties
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    public long Timestamp { get; }
}

public class KafkaException : Exception {        // flat, for now (§4, ffi §A5)
    public int Code { get; }
    public bool IsRetriable { get; }
    public bool IsFatal { get; }
}

public interface IProducer : IAsyncDisposable, IDisposable {   // Java `Producer`
    Task<RecordMetadata> SendAsync(ProducerRecord record, CancellationToken cancellationToken = default);
    Task FlushAsync(CancellationToken cancellationToken = default);
    Task CloseAsync(TimeSpan timeout, CancellationToken cancellationToken = default);
}

public sealed class KafkaProducer : IProducer {
    public KafkaProducer(IReadOnlyDictionary<string, string> config);
}

public sealed class MockProducer : IProducer {   // Java `MockProducer`
    public MockProducer(bool autoComplete = true);
    public bool CompleteNext();
    public bool ErrorNext(int code, string? message = null);
    public int HistoryCount { get; }
    public void Clear();
}
```

**Clipped to today's ABI.** The sketch is the *producer* surface the current ABI
can back — it omits Java members the ABI doesn't expose yet:
`ProducerRecord.headers()`, `RecordMetadata`'s serialized-size / `has*` accessors,
and `MockProducer.history()` (Java returns the full record list; the ABI gives
only a count, hence `HistoryCount`). Add each when the ABI grows to cover it.

**Consumer** follows the identical pattern — the Java surface in C# idiom. Its C
ABI has **landed**, so it's a **Mode A** build (§6.2); the receive-path key/value
ownership decision is §6.4. The surface we're building toward (bytes-only interim,
clipped to today's ABI):

```csharp
public sealed class ConsumerRecord {             // Java `ConsumerRecord`, getters → properties
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    public long Timestamp { get; }
    public ReadOnlyMemory<byte>? Key { get; }    // owned — copied out of the batch (§6.4)
    public ReadOnlyMemory<byte>? Value { get; }  // owned; null = tombstone
}

public sealed class ConsumerRecords : IReadOnlyCollection<ConsumerRecord> { }  // Java `ConsumerRecords`

public interface IConsumer : IAsyncDisposable, IDisposable {   // Java `Consumer`
    // blocking-in-Java / callback-at-ABI → async (Async suffix, §4)
    Task<ConsumerRecords> PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default);
    Task SubscribeAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default);
    Task UnsubscribeAsync(CancellationToken cancellationToken = default);
    Task CommitAsync(CancellationToken cancellationToken = default);           // Java commitAsync
    Task<long> PositionAsync(TopicPartition partition, CancellationToken cancellationToken = default);
    Task CloseAsync(TimeSpan timeout, CancellationToken cancellationToken = default);

    void CommitSync();                            // Java commitSync — genuinely sync, blocks (§4 note)

    // non-blocking / instantaneous in Java → stays sync (idiom map; consumer-threading §1)
    IReadOnlyCollection<TopicPartition> Assignment { get; }   // Java returns a Set; IReadOnlySet is
    IReadOnlyCollection<string> Subscription { get; }         //   post-netstandard2.0, so collection
    void Wakeup();                                // interrupt a blocked poll — one-shot (idiom map)
    ConsumerGroupMetadata GroupMetadata();
}

public sealed class KafkaConsumer : IConsumer {   // Java `KafkaConsumer` (KIP-848 group protocol)
    public KafkaConsumer(IReadOnlyDictionary<string, string> config);
}

public sealed class MockConsumer : IConsumer {    // Java `MockConsumer`
    public MockConsumer();
    public void AddRecord(ConsumerRecord record); // mock-only helpers are inherent, not on IConsumer
}
```

**Clipped to today's ABI**, like the producer — the fuller Java surface lands as
each piece is wired (async/sync split per the idiom map): `Assign`/`Seek`/`Pause`/
`Resume`, `CommittedAsync`, `BeginningOffsets`/`EndOffsets`/`OffsetsForTimes`,
`PartitionsFor`/`ListTopics`, headers on `ConsumerRecord`, and a
`ConsumerRebalanceListener` argument on `SubscribeAsync`. A typed
`Consumer<TKey,TValue>` arrives with deserializers (§4), same as the producer.

The **admin client** (`IAdminClient`) is still **Mode B** — sketched once its C
ABI lands (§6.3).

**The Java → C# idiom map** — the binding's spine. Each row: the Java construct,
its C# realization, and where the enforcing rule lives.

| Java | C# idiom | Rule / detail |
|---|---|---|
| `Future<RecordMetadata>` | `Task<RecordMetadata>` | `TaskCompletionSource` completion — producer pull-pump *or* push (open); consumer push — ffi §A7/§B7 |
| blocking / `Future`-returning / I/O call (producer `send`/`flush`/`close`; consumer `poll`/`commit`/`position`) | `async Task` + `CancellationToken` | best-effort cancel — ffi §A7/§B7, §4 |
| `close()` / `AutoCloseable` | `IAsyncDisposable.DisposeAsync()` (+ `IDisposable`) | graceful close drains the in-flight op / joins the pump — ffi §A2/§A7, §B2/§B7 |
| `KafkaException` hierarchy | one flat `KafkaException` (`Code`/`IsRetriable`/`IsFatal`) | ffi §A5 |
| `IllegalArgumentException` / `IllegalStateException` | `ArgumentException` (family) / `InvalidOperationException` (`ObjectDisposedException` when used after close) | validate **before** the FFI call — ffi §A5 |
| `wakeup()` (interrupt a blocked `poll`/`commit`) | sync `Wakeup()`; the in-flight `PollAsync`/`CommitAsync`/`CommitSync` throws flat `KafkaException` (Wakeup code, **one-shot**) | ffi §B5 |
| `ConcurrentModificationException` (consumer is one-op-in-flight) | `InvalidOperationException` (concurrent sync state read) / `KafkaException` (concurrent async op) | ffi §B5 |
| `ConsumerRebalanceListener` | `IConsumerRebalanceListener` (async) | invoked on the **caller's task** during `poll`/`commit`/`close` — consumer-threading §31 |
| `OffsetCommitCallback` | `IOffsetCommitCallback` (async) | same caller's-task model — consumer-threading §31 |
| **non-blocking / instantaneous** call (`offset()`, `assignment()`, mock helpers) | **stays sync** — property (`Offset`) or plain method | not everything becomes async — `consumer-threading.md §1` |
| method `send`, `flush`, `poll` | PascalCase + `Async` suffix (`SendAsync`, `PollAsync`) | §4 |
| `byte[]` key/value | `ReadOnlyMemory<byte>` | send: pinned zero-copy — ffi §A4; receive: copy-out (default), keep-alive deferred — ffi §B4 / §6.4 |
| opaque handle | `SafeHandle` (owned) / `IntPtr` (transient) | ffi §A2/§B2 |
| `String` topic / config | UTF-8, hand-marshalled | ffi §A3/§B3 |
| `Duration` (timeouts: `poll`/`close`/`committed`) | `TimeSpan` | ABI takes `int64_t` ms |
| `Map` / `Set` / `List` (returns) | `IReadOnlyDictionary` / `IReadOnlyCollection` / `IReadOnlyList` | `IReadOnlySet` post-dates netstandard2.0 → `IReadOnlyCollection` |
| `Producer<K,V>` / `Consumer<K,V>` (generic) | non-generic bytes **now**; generic `Producer<TKey,TValue>` / `Consumer<TKey,TValue>` when serializers land | §4 |

**Do NOT build:** the ecosystem `confluent-kafka-dotnet` shape (`ProduceAsync`,
delivery-report handlers, `Message<K,V>`, `value.serializer` kwargs). Target the
**Java** client, per `bindings/CLAUDE.md §2`.

---

## 4 · Design decisions

Defaults + rationale; the implementing agent takes the default unless the feature
argues otherwise, and records any deviation (phase PLAN, COMMENTS.DONE, or a code
comment).

| Decision | Default | Why / when |
|---|---|---|
| **Namespace / package id** | **Now:** `Confluent.Kafka.ShareConsumer` — a **distinct** package id / assembly / namespace, so it coexists with ckd's `Confluent.Kafka` 2.x (NuGet resolves **one version per package id** and the CLR binds **one assembly per simple name** — a distinct id is the only way to run alongside 2.x; `extern alias` can't bypass either wall). **Later:** switch to the bare `Confluent.Kafka` (new major, e.g. 4.x) once this client is a full **superset that replaces** 2.x. | before any public type |
| **Disposal** | Both `IAsyncDisposable.DisposeAsync()` (primary; drains the in-flight op / joins the pump, then `flush`/`close`, without blocking) and `IDisposable.Dispose()` (blocking fallback). `close(Duration)` → `CloseAsync(TimeSpan)`. *Note:* the timeout is ABI-backed only for the **consumer** (`Consumer_close_with_timeout`); `Producer_close`/`_flush` take none, so a producer `TimeSpan` is a .NET-side deadline until a timed producer close lands. | first client type |
| **Cancellation** | `CancellationToken` on every async method, honored best-effort. **Producer:** cancels the *wait*, never aborts an enqueued send (ffi §A7). **Consumer:** maps to `wakeup()` → the in-flight op cancels/faults (ffi §B7). A host-idiom addition Java lacks (allowed by `bindings/CLAUDE.md §2`). | first async method |
| **Async naming** | `Async` suffix on `Task`-returning methods (`SendAsync`); ffi-marshalling assumes this. Deviation: strict-Java `Send`. | first async method |
| **Interface naming** | `IProducer` / `IConsumer` — C#'s `I`-prefix is the lexical marker for an interface (Framework Design Guidelines; analyzer CA1715 warns without it), same idiom-layering as `Async`; the root name stays recognizable. Each has a real + mock impl (`KafkaProducer`/`MockProducer`, `KafkaConsumer`/`MockConsumer`). Deviation: strict-Java bare `Producer`/`Consumer` (fights CA1715 / dev expectation). | first interface type |
| **Key/value type** | `ReadOnlyMemory<byte>` both ways. **Producer (send):** zero-copy — pins the user buffer via `MemoryHandle` (ffi §A4). **Consumer (receive):** wraps an owned copied array (copy-out, §6.4), not a pin. `byte[]`-only is an acceptable interim. | porting `ProducerRecord` / `ConsumerRecord` |
| **Serializers** | ABI is bytes-only both ways; add .NET-side `ISerializer<T>` (`T → byte[]`) and `IDeserializer<T>` (`ReadOnlySpan<byte> → T`, zero-copy over the batch — ffi §B4) — makes `Producer<TKey,TValue>` / `Consumer<TKey,TValue>` generic later. No per-record callback through the ABI (`CLAUDE.md §11`). | porting (de)serialization |
| **Config** | `IReadOnlyDictionary<string,string>` → per-entry `ProducerProperties_put` (consumer: `ConsumerProperties_put`); keys are **Java dotted names** (`bootstrap.servers` required); coerce non-string values to `str`; classic-/consumer-only keys accepted silently. | wiring the constructor |
| **Error granularity** | One flat `KafkaException` now; typed subclasses can be added under it later, non-breakingly (ffi §A5). | if catch-by-type is needed |
| **Interceptors** | Defer; reserve the Java-shaped name. | a concrete need |
| **Nullable reference types** | `#nullable enable` project-wide; annotate the P/Invoke surface precisely. | project setup |

**Consumer-era note** — a Java sync/async *pair* (e.g. `commitSync`/`commitAsync`)
maps to `CommitSync()` (genuinely synchronous — blocks the caller; fine since
commit is low-frequency, not hot-path) **+** `CommitAsync()` (`Task`). The `Async`
suffix marks the `Task`-returner; the sync twin stays sync — no suffix, calls the
blocking-native ABI directly (not sync-over-async).

---

## 5 · Boundary rules → `ffi-marshalling.md`

The correctness contracts you must not break live in
`.claude/rules/ffi-marshalling.md` (read on demand), organized as a **Shared**
part + a **Producer** part + a **Consumer** part (each client reads end-to-end).
Index:

**Part 0 · Shared mechanics**
- §0.1 P/Invoke declarations & type map (`[DllImport]`, `Cdecl`, TFMs)
- §0.2 Native library loading, packaging & AOT
- §0.3 Thread topology & thread-safety (shared framing)

**Part A · Producer** (end to end)
- §A1 Thread model · §A2 Handle ownership (`SafeHandle` vs transient) ·
  §A3 String marshalling (UTF-8, the shared mechanism) · §A4 Zero-copy — send
  (call-scoped pin) · §A5 Error model (flat `KafkaException` + preconditions) ·
  §A6 Callback marshalling (`RecordMetadata_copy`) · §A7 Async completion
  (pull-pump *vs* push — open)

**Part B · Consumer** (end to end)
- §B1 Thread model · §B2 Handle ownership (owned container + borrowed view) ·
  §B3 String marshalling — receive path (length-delimited borrow) · §B4 Zero-copy
  — receive (borrow, copy-out) · §B5 Error model additions (wakeup, concurrent) ·
  §B6 Completion callbacks (foreign thread) · §B7 Async completion (push — settled)

This file (CLAUDE.md) never restates those; it references them by section.

---

## 6 · Adding & extending

### 6.1 The decision gate

```
Is the feature already exposed at the C ABI (src/ffi)?
        │
   yes ─┤→ MODE A · .NET-only    (Native decl + SafeHandle + managed wrapper)        → 6.2
        │
   no ──┘→ MODE B · Full-stack   (src/ffi → header → Native → managed API)           → 6.3
```

### 6.2 Mode A — .NET-only port

The `kafka_*` function already exists in the header:

1. **P/Invoke** — add the `[DllImport]` declaration to `Native` (ffi §0.1).
2. **Ownership** — a `SafeHandle` subclass for any new long-lived handle
   (ffi §A2/§B2); transient handles are read-and-freed, not wrapped.
3. **Managed API** — the Java-shaped method in the client class (`Producer.cs` /
   `Consumer.cs`); marshal strings/bytes (ffi §A3/§B3, §A4/§B4), map errors
   (ffi §A5/§B5), bridge async to `Task` (ffi §A7/§B7).
4. **Build & Test** — against `MockProducer` / `MockConsumer`, no broker (§7).

### 6.3 Mode B — full-stack port (the C-ABI-first loop)

The feature lives only in the Rust core. Walk all four layers, ABI first.

**Ownership split:** steps 1–4 (design + write the Rust ABI, regenerate) are a
**Rust-core task** — the shared C ABI is authored by the root `actor-executor`
and reviewed by `kafka-critic` against root `CLAUDE.md` (not the `dotnet-*`
personas; §8.1/§8.2). The `dotnet-actor` **depends on** them and owns **steps
5–7** (from the header down). The Manager sequences the handoff.

1. **Design the ABI surface** — opaque handles, transparent structs, functions
   (naming per "Naming across layers" below). *This is the real work.*
2. **Write `src/ffi/<area>.rs`** — the four ABI shapes (constructor / action /
   getter / destroy; canonical: `src/ffi/producer.rs`); `pub mod <area>;`.
3. **Make the types emit** — add zero-field opaque `*_t` structs to
   `cbindgen.toml` `[export].include`.
4. **Regenerate** — `cargo build --features ffi`; confirm the symbols land in
   `target/include/confluent_kafka.h`.
5. **Wrap in `Native`** — `[DllImport]` declarations + `SafeHandle`s.
6. **Expose the managed API** — the Java-shaped surface in a new class.
7. **Test & build** — Mock/parity tests → `dotnet build` → `dotnet test`.

**Naming across layers:** `kafka_<pkg-minus-clients>_<Type>_<method>` at the ABI;
C# casing above it (PascalCase, properties for getters, `Async` suffix).

### 6.4 The consumer receive-path ownership decision (Mode A)

The consumer C ABI confirms the receive-path zero-copy contract
(`consumer-threading.md §27`): `ConsumerRecord_key` / `_value` / `_topic` return a
`(ptr, int32_t len)` pair that **borrows into the batch and is valid only until
`ConsumerRecords_destroy`**. The producer send path had no such problem (bytes go
*in*, a small handle comes back). So .NET must decide how to surface those
**borrowed slices**.

**The choice only affects the raw-byte surface.** For a *typed* consumer, the
`IDeserializer<T>` reads a transient `ReadOnlySpan<byte>` over the batch during the
poll loop and returns an **owned `T`** — nothing references the batch afterward, so
both options behave identically. A `string` / topic likewise **must** be copied (a
`string` can't borrow native UTF-8; ffi §A3/§B3). The two options diverge **only** when
the user wants the raw bytes themselves (`byte[]` / `ReadOnlyMemory<byte>`).

**Default — copy-out.** For the raw-byte surface, copy each key/value into an owned
managed array, then `ConsumerRecords_destroy`. It matches Java's owned
`ConsumerRecord` shape, is safe (no `IDisposable`, no use-after-free, async- and
thread-safe, no memory amplification), and costs one gen-0 array per raw record —
Java's own allocation behavior. **This is the default.**

**Deferred alternative — keep-alive (zero-copy).** Hold the `ConsumerRecords_t`
handle alive (a `SafeConsumerRecordsHandle`; ffi §B2 Category 3) and expose the
bytes as views over the batch, `_destroy` at `Dispose`. Zero-copy on the raw-byte
path, but it diverges from the Java shape and couples record lifetime to the
handle. **Do not** mirror Python's default of native-backed `ReadOnlyMemory<byte>`:
Python's keep-alive is safe only because a `memoryview` refcounts the batch
(CPython refcount-driven `tp_dealloc`, not the cyclic GC), and .NET has no
equivalent — a stored `ReadOnlyMemory` over
native memory is a use-after-`Dispose` footgun. If the niche is ever needed, prefer
a **compile-time-safe** escape hatch — a `ReadOnlySpan<byte>` accessor or a
process-in-place `Poll(record => …)` callback (a `Span` / ref-struct can't be
stored, awaited, or sent cross-thread, so it can't outlive the batch) — not
native-backed `ReadOnlyMemory`. Deferred until a concrete need.

---

## 7 · Build, test, verify

The build order, commands, running against a broker, and test conventions.

### 7.1 The build is a two-stage pipeline: Rust → .NET (firm)

The .NET binding cannot run until the Rust side has produced the native library:

```
cargo build --features ffi [--release]                    (build-rust)
   └─ target/<profile>/{lib}confluent_kafka.{so,dylib,dll}  ← the binding P/Invokes this
                    │  (must exist first)
                    ▼
dotnet build   (an MSBuild step copies the native into $(OutDir))  (build-dotnet)
```

Never build .NET before Rust — the native won't exist.

### 7.2 Commands (intended)

| Goal | Command |
|---|---|
| Build the native | `cargo build --features ffi [--release]` |
| Build the binding | `dotnet build` (copies the native to output) |
| Unit tests (**no broker**) | `dotnet test` (`MockProducer` / `MockConsumer`) |
| Rust tests | `cargo test` |
| Format / lint (Rust) | `cargo xtask format` / `cargo xtask lint` |
| Format (C#) | `dotnet format` |

**C# style** — code follows the dotnet/runtime coding style, pinned in a
checked-in `.editorconfig` (`_camelCase`/`s_` fields, PascalCase, Allman braces,
`System.*` usings first, `I`-prefix per CA1715) and enforced by `dotnet format` +
`<EnforceCodeStyleInBuild>` analyzers. This is code hygiene only — it does not
touch the public Java shape (§3/§4).

### 7.3 Running against a broker

- **No broker** — `MockProducer` / `MockConsumer` (unit tests; also how to iterate without infra).
- **Your own local broker** — `bootstrap.servers` is just a config key.
- **Integration** — spin a broker via testcontainers (needs Docker), not a
  checked-in compose file.

### 7.4 Test conventions
- Unit tests hold a `MockProducer` / `MockConsumer` (manual-drive via
  `complete_next`/`error_next` and `add_record`/`set_poll_error`), and `await` the
  returned `Task` with a timeout — the timeout doubles as the **completion /
  deadlock regression guard** (the producer pull-pump's join; the consumer/push
  dispatcher hand-off)
- **TFM-matrix smoke test**: the binding loads and a `MockProducer` / `MockConsumer`
  round-trip on **net462** (via netstandard2.0), **net8.0**, **net10.0**
- Parity obligations (`definition-of-done.md §3`): mirror the Java/Rust tests,
  **assert error-message content**, and add a per-record **allocation-budget**
  test — on the **send path** (producer) and, for the consumer, on the
  **receive path** (`consumer-threading.md §27`; the copy-out budget).

### 7.5 Definition of done

A port is not done until it builds on the TFM matrix, unit tests pass against
`MockProducer` / `MockConsumer`, lint/format are clean, and the `ffi-marshalling.md` anti-patterns
are satisfied (`definition-of-done.md`). Integration/multi-language suites are opt-in until
CI-stable.

---

## 8 · Governance & review

Who builds and reviews this binding. The *process* is inherited from root
`agent-roles.md`; the personas add .NET review expertise. The review *criteria*
live in `ffi-marshalling.md` (anti-patterns) and §7 (verify) — this section
points at them.

### 8.1 Personas

- **`dotnet-actor`** and **`dotnet-critic`** (`.claude/agents/dotnet-*.md`)
  inherit the Actor / Critic roles and the `COMMENTS.<N>.md` loop from
  `agent-roles.md`. The Manager is the root `project-manager` (coordination is
  client-agnostic).
- They are needed because the root `actor-executor` / `kafka-critic` are
  Rust-translation-shaped and don't know P/Invoke / .NET interop.
- **Scope — the C# side, header-down.** The `dotnet-actor` builds only C# (from
  the generated header down) and **does not author Rust**; the `dotnet-critic`
  reviews only C#. When a feature needs a new ABI function (Mode B, §6.3
  steps 1–4), that's a Rust-core dependency on the root `actor-executor` /
  `kafka-critic`, not the `dotnet-*` personas.

### 8.2 Review ground truth (firm)

Review a change against the **C ABI header** (`confluent_kafka.h`) and the **Kafka
Java public API shape** — **not** Rust internals, and **not** Java implementation
logic (`bindings/CLAUDE.md §2`).

### 8.3 The Critic's lens (.NET-specific)

`SafeHandle` / `Dispose` correctness · handle leak / double-free / use-after-free ·
byte pinning & buffer lifetime · `MarshalAs(I1)` for `bool` · UTF-8 (no `LPStr`) ·
no managed exception through a callback · `RunContinuationsAsynchronously` on the
completion (pump/dispatcher) · flat `KafkaException` vs precondition .NET exceptions · **shape, not
logic**. The concrete checklist is the **Anti-patterns** blocks in
`ffi-marshalling.md` and the decision tables in §3/§4.

### 8.4 Mechanics

- Review comments: `bindings/dotnet/COMMENTS.<N>.md`; resolved →
  `COMMENTS.DONE.<N>.md`.
- Agent memory: `bindings/dotnet/.claude/agent-memory/<persona>/`.
- Plans & design docs: `bindings/dotnet/design/` — the **binding-local** mirror
  of the repo-root `design/` (do NOT put .NET plans in the root `design/`, which
  is Rust-core-only). Living status/structure/design go in `design/current/`; the
  Manager saves each **approved plan** and a copy of the closed
  `COMMENTS.DONE.<N>.md` under `design/history/<Milestone>/<Phase>/` (per the
  root `project-manager` mechanics). The binding keeps **its own** milestone /
  phase numbering, independent of the root `design/`. Split of duties: `PLAN.md`
  is the forward-looking plan (scope / deliverables / decisions), while
  `COMMENTS.DONE.<N>.md` records decisions and deviations made *during*
  execution. Never commit `.DS_Store` here.
- ⚠ **Nested-agent discovery does NOT work** — the harness does **not**
  auto-register `bindings/dotnet/.claude/agents/*.md`, so the personas are not
  invocable from where they live. To use them, **copy both persona files to the
  repo-root `.claude/agents/`** (or invoke with the persona files loaded
  explicitly); without that copy nothing in §8.1–§8.3 is reachable. The root copy
  is a snapshot, not a link — **re-copy after editing a persona**, and treat the
  binding-local file as the one you edit.
- **Persona tracking policy — two locations, opposite rules:**
  - `bindings/dotnet/.claude/agents/dotnet-{actor,critic}.md` — the
    **binding-local** personas and the **source of truth**. **Intentionally
    tracked**: keep them as-is, do **NOT** untrack them.
  - repo-root `.claude/agents/dotnet-{actor,critic}.md` — the **discovery
    copies** created by the workaround above. These must **NEVER** be committed:
    keep them untracked, never `git add` them, and never let them appear in a
    commit or PR diff.
