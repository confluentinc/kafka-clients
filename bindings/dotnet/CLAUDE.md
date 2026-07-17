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

**Status (incremental guide).** No .NET code exists yet — this rulebook precedes
and directs it; the C# signatures below are the *target*, not current code. The
**rules & decisions** here are durable, but the **status markers are
point-in-time and must be refreshed as the binding lands** — namely: this line,
§1 *What's real* + the producer-parity table, and the file map's *(intended)*
tags. Update those as code/ABI arrives; leave the shape, decisions, and boundary
rules unless a decision actually changes.

**The one law:** the binding restores the Java **shape** and holds **no Kafka
logic** — batching, partitioning, retries, offsets all live once, in the Rust
core. So it's **not a 1:1 mirror** — expect host-only types with no Java/C-ABI
counterpart: `NativeMethods`, `SafeHandle` subclasses, `IDisposable`, marshalling
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
        │  a single completion pump blocks on get_all(), completes each TCS
        ▼
   C ABI  kafka_producer_Producer_send / _get_all   (flat pointers, out-params)
        ▼
   Rust   Producer::send(ProducerRecord) → KafkaFuture<RecordMetadata>   (the logic)
```

**What's real.** The C ABI exposes the **producer** (~30 fns across Producer /
Properties / Future / RecordMetadata / KafkaError / MockProducer families;
`src/ffi/producer.rs`) **and the KIP-848 consumer** (~130 fns across Consumer /
ConsumerProperties / ConsumerRecord(s) / TopicPartition(List) / OffsetAndMetadata
/ OffsetAndTimestamp / ConsumerGroupMetadata / PartitionInfo / MockConsumer + map
helpers; `src/ffi/consumer.rs`) — both with sync **and** `_async`/callback
variants. Admin / transactions are **not** exposed yet. Source of truth for the
surface = `src/ffi/*.rs` + `cbindgen.toml` (the header is generated, not checked
in).

**To full producer parity** — Java producer features not yet in the sketch (each
tracked where its decision lives; the *Unblocked by* tag routes the work):

| Gap (vs Java) | Unblocked by | Tracked in |
|---|---|---|
| typed `Producer<TKey,TValue>` + serializers | **.NET-side** (serialize above the bytes ABI) | §3 Serializers |
| `ProducerRecord.Headers` | **ABI** (struct has no headers field) | §2 clipped note |
| `RecordMetadata` serialized-size / `Has*` accessors | **ABI** accessors | §2 clipped note |
| `MockProducer.History()` (full record list) | **ABI** (only a count today) | §2 clipped note |
| interceptors | **.NET-side**, deferred | §3 Interceptors |

The **consumer** ABI has now landed (Mode A — build the .NET surface directly,
§5.2; its receive-path ownership decision is §5.4). Admin / transactions are
still separate *families* that need the C ABI first (Mode B, §5.3).

**Intended file map** — split by visibility, so the public *shape* is auditable
at a glance and the unsafe boundary is quarantined. (Folders are organizational;
C# accessibility is still the `internal` keyword + the assembly.) Every type under
`Internal/` is explicitly `internal` (and `sealed` where practical); `public` is
reserved for `src/` — wanting a type under `Internal/` to be `public` is the
signal it belongs in `src/`.

- `src/` — **all public API**, whatever the C# kind: the client types *and*
  supporting value types / enums (`ProducerRecord`, `RecordMetadata`, `Headers`,
  `TopicPartition`, later `ConsumerRecord` / `OffsetAndMetadata` / enums). If a
  user can name it, it lives here.
- `src/Internal/` — **internal** managed scaffolding (`ConfluentKafka.Internal`):
  the completion pump, config → properties marshalling.
- `src/Internal/Interop/` — the **P/Invoke boundary**
  (`ConfluentKafka.Internal.Interop`): the `Native` `[DllImport]` class,
  `SafeHandle`s, `Utf8` helpers, callback delegates, and the blittable
  `[StructLayout]` mirror structs (e.g. the `ProducerRecord_t` mirror — the
  interop twin of the public `ProducerRecord`). `unsafe` lives only here; 1:1
  with `ffi-marshalling.md`.
- `tests/` — `MockProducer` unit tests (`InternalsVisibleTo` grants access to
  internals).

---

## 2 · The target: the .NET API shape

Mirror the **Java** client in idiomatic C#. The producer surface we're building
toward (bytes-only interim per the serializer decision in §3):

```csharp
namespace ConfluentKafka;   // NOT Confluent.Kafka — see §3

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

public class KafkaException : Exception {        // flat, for now (§3, ffi §5)
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

**Consumer** follows the identical pattern — the Java surface in C# idiom
(`IConsumer` / `KafkaConsumer` → `PollAsync` / `SubscribeAsync` / `CommitAsync`).
Its C ABI has **landed**, so it's a **Mode A** build (§5.2); the receive-path
key/value ownership decision is §5.4. The **admin client** (`IAdminClient`) is
still **Mode B** — sketched once its C ABI lands (§5.3).

**The Java → C# idiom map** — the binding's spine. Each row: the Java construct,
its C# realization, and where the enforcing rule lives.

| Java | C# idiom | Rule / detail |
|---|---|---|
| `Future<RecordMetadata>` | `Task<RecordMetadata>` | completion pump + `TaskCompletionSource` — ffi §7 |
| blocking / `Future`-returning / I/O call (`send`, `flush`, `close`) | `async Task` + `CancellationToken` | best-effort cancel — ffi §7, §3 |
| `close()` / `AutoCloseable` | `IAsyncDisposable.DisposeAsync()` (+ `IDisposable`) | graceful close joins the pump — ffi §2/§7 |
| `KafkaException` hierarchy | one flat `KafkaException` (`Code`/`IsRetriable`/`IsFatal`) | ffi §5 |
| `IllegalArgumentException` / `IllegalStateException` | `ArgumentException` / `ObjectDisposedException` | validate **before** the FFI call — ffi §5 |
| **non-blocking / instantaneous** call (`offset()`, `assignment()`, mock helpers) | **stays sync** — property (`Offset`) or plain method | not everything becomes async — `consumer-threading.md §1` |
| method `send`, `flush` | PascalCase + `Async` suffix (`SendAsync`) | §3 |
| `byte[]` key/value | `ReadOnlyMemory<byte>` | pinned zero-copy — ffi §4 |
| opaque handle | `SafeHandle` (owned) / `IntPtr` (transient) | ffi §2 |
| `String` topic / config | UTF-8, hand-marshalled | ffi §3 |
| `Producer<K,V>` (generic) | non-generic bytes **now**; generic `Producer<TKey,TValue>` when serializers land | §3 |

**Do NOT build:** the ecosystem `confluent-kafka-dotnet` shape (`ProduceAsync`,
delivery-report handlers, `Message<K,V>`, `value.serializer` kwargs). Target the
**Java** client, per `bindings/CLAUDE.md §2`.

---

## 3 · Design decisions

Defaults + rationale; the implementing agent takes the default unless the feature
argues otherwise, and records any deviation (phase PLAN, COMMENTS.DONE, or a code
comment).

| Decision | Default | Why / when |
|---|---|---|
| **Namespace / package id** | `ConfluentKafka` — **distinct from the ecosystem client's `Confluent.Kafka`** (distinct FQN lets both coexist). | before any public type |
| **Disposal** | Both `IAsyncDisposable.DisposeAsync()` (primary; joins pump + `flush`/`close` without blocking) and `IDisposable.Dispose()` (blocking fallback). `close(Duration)` → `CloseAsync(TimeSpan)`. | first client type |
| **Cancellation** | `CancellationToken` on every async method, honored best-effort — cancels the *wait*, never aborts an enqueued send (ffi §7). A host-idiom addition Java lacks (allowed by `bindings/CLAUDE.md §2`). | first async method |
| **Async naming** | `Async` suffix on `Task`-returning methods (`SendAsync`); ffi-marshalling assumes this. Deviation: strict-Java `Send`. | first async method |
| **Interface naming** | `IProducer` — C#'s `I`-prefix is the lexical marker for an interface (Framework Design Guidelines; analyzer CA1715 warns without it), same idiom-layering as `Async`; the root name "Producer" stays recognizable. Deviation: strict-Java bare `Producer` (fights CA1715 / dev expectation). | first interface type |
| **Key/value type** | `ReadOnlyMemory<byte>` (zero-copy-friendly; pins via `MemoryHandle`, ffi §4). `byte[]`-only is an acceptable interim. | porting `ProducerRecord` |
| **Serializers** | ABI is bytes-only; add a .NET-side `ISerializer<T>`/`IDeserializer<T>` producing `byte[]` (makes `Producer<TKey,TValue>` generic later). No per-record callback through the ABI (`CLAUDE.md §11`). | porting (de)serialization |
| **Config** | `IReadOnlyDictionary<string,string>` → per-entry `ProducerProperties_put`; keys are **Java dotted names** (`bootstrap.servers` required); coerce non-string values to `str`; classic-/consumer-only keys accepted silently. | wiring the constructor |
| **Error granularity** | One flat `KafkaException` now; typed subclasses can be added under it later, non-breakingly (ffi §5). | if catch-by-type is needed |
| **Interceptors** | Defer; reserve the Java-shaped name. | a concrete need |
| **Nullable reference types** | `#nullable enable` project-wide; annotate the P/Invoke surface precisely. | project setup |

**Consumer-era note** — a Java sync/async *pair* (e.g. `commitSync`/`commitAsync`)
maps to `CommitSync()` (genuinely synchronous — blocks the caller; fine since
commit is low-frequency, not hot-path) **+** `CommitAsync()` (`Task`). The `Async`
suffix marks the `Task`-returner; the sync twin stays sync — no suffix, calls the
blocking-native ABI directly (not sync-over-async).

---

## 4 · Boundary rules → `ffi-marshalling.md`

The correctness contracts you must not break live in
`.claude/rules/ffi-marshalling.md` (read on demand). Index:

- §1 P/Invoke declarations & type map (`[DllImport]`, `Cdecl`, TFMs)
- §2 Handle ownership (`SafeHandle` vs transient read-and-free)
- §3 String marshalling (UTF-8 by hand)
- §4 Zero-copy & buffer pinning (call-scoped)
- §5 Error model (flat `KafkaException` + precondition exceptions)
- §6 Callback & delegate marshalling (`RecordMetadata_copy`)
- §7 Async / Future completion (the pump + `TaskCompletionSource`)
- §8 Native library loading, packaging & AOT
- *(preamble)* Thread topology — the whole-system thread picture

This file (CLAUDE.md) never restates those; it references them by section.

---

## 5 · Adding & extending

### 5.1 The decision gate

```
Is the feature already exposed at the C ABI (src/ffi)?
        │
   yes ─┤→ MODE A · .NET-only    (Native decl + SafeHandle + managed wrapper)        → 5.2
        │
   no ──┘→ MODE B · Full-stack   (src/ffi → header → Native → managed API)           → 5.3
```

### 5.2 Mode A — .NET-only port

The `kafka_*` function already exists in the header:

1. **P/Invoke** — add the `[DllImport]` declaration to `Native` (ffi §1).
2. **Ownership** — a `SafeHandle` subclass for any new long-lived handle (ffi §2);
   transient handles are read-and-freed, not wrapped.
3. **Managed API** — the Java-shaped method in `Producer.cs`; marshal
   strings/bytes (ffi §3–4), map errors (ffi §5), bridge async to `Task` (ffi §7).
4. **Build & Test** — against `MockProducer`, no broker (§6).

### 5.3 Mode B — full-stack port (the C-ABI-first loop)

The feature lives only in the Rust core. Walk all four layers, ABI first.

**Ownership split:** steps 1–4 (design + write the Rust ABI, regenerate) are a
**Rust-core task** — the shared C ABI is authored by the root `actor-executor`
and reviewed by `kafka-critic` against root `CLAUDE.md` (not the `dotnet-*`
personas; §7.1/§7.2). The `dotnet-actor` **depends on** them and owns **steps
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

### 5.4 ⚠ The consumer receive-path ownership decision (Mode A)

The consumer C ABI has landed, and it **confirms** the receive-path zero-copy
contract (`consumer-threading.md §27`): `ConsumerRecord_key` / `_value` / `_topic`
return a `(const uint8_t* / const char*, int32_t len)` pair that **borrows into
the batch and is valid only until `ConsumerRecords_destroy`**. The producer send
path had no such problem (bytes go *in*, a small handle comes back). Here the crux
decision is how .NET surfaces those **borrowed slices**, which want to become
owned `byte[]` / `ReadOnlyMemory<byte>`:

- **Copy-out** — copy each key/value into a managed array before
  `ConsumerRecords_destroy`. Simple, safe, one copy per record (matches Java's own
  allocation behavior).
- **Keep-alive spans** — hold the `ConsumerRecords_t` handle alive and hand out
  `ReadOnlySpan`/`ReadOnlyMemory` over the borrowed bytes; zero-copy, but ties
  record lifetime to the handle and must forbid use-after-`Dispose`.

Resolve this *before* building the managed `ConsumerRecord` surface — it's the one
genuinely new marshalling decision the consumer adds over the producer.

---

## 6 · Build, test, verify

The build order, commands, running against a broker, and test conventions.

### 6.1 The build is a two-stage pipeline: Rust → .NET (firm)

The .NET binding cannot run until the Rust side has produced the native library:

```
cargo build --features ffi [--release]                    (build-rust)
   └─ target/<profile>/{lib}confluent_kafka.{so,dylib,dll}  ← the binding P/Invokes this
                    │  (must exist first)
                    ▼
dotnet build   (an MSBuild step copies the native into $(OutDir))  (build-dotnet)
```

Never build .NET before Rust — the native won't exist.

### 6.2 Commands (intended)

| Goal | Command |
|---|---|
| Build the native | `cargo build --features ffi [--release]` |
| Build the binding | `dotnet build` (copies the native to output) |
| Unit tests (**no broker**) | `dotnet test` (MockProducer) |
| Rust tests | `cargo test` |
| Format / lint (Rust) | `cargo xtask format` / `cargo xtask lint` |
| Format (C#) | `dotnet format` |

**C# style** — code follows the dotnet/runtime coding style, pinned in a
checked-in `.editorconfig` (`_camelCase`/`s_` fields, PascalCase, Allman braces,
`System.*` usings first, `I`-prefix per CA1715) and enforced by `dotnet format` +
`<EnforceCodeStyleInBuild>` analyzers. This is code hygiene only — it does not
touch the public Java shape (§2/§3).

### 6.3 Running against a broker

- **No broker** — `MockProducer` (unit tests; also how to iterate without infra).
- **Your own local broker** — `bootstrap.servers` is just a config key.
- **Integration** — spin a broker via testcontainers (needs Docker), not a
  checked-in compose file.

### 6.4 Test conventions
- Unit tests hold a `MockProducer` (auto- or manual-complete via
  `complete_next`/`error_next`), and `await` the returned `Task` with a timeout —
  the timeout doubles as the **pump-join / deadlock regression guard** 
- **TFM-matrix smoke test**: the binding loads and a `MockProducer` round-trips on
  **net462** (via netstandard2.0), **net8.0**, **net10.0**
- Parity obligations (`definition-of-done.md §3`): mirror the Java/Rust tests,
  **assert error-message content**, and add a per-record
  **allocation-budget** test on the send path.

### 6.5 Definition of done

A port is not done until it builds on the TFM matrix, unit tests pass against
`MockProducer`, lint/format are clean, and the `ffi-marshalling.md` anti-patterns
are satisfied (`definition-of-done.md`). Integration/multi-language suites are opt-in until
CI-stable.

---

## 7 · Governance & review

Who builds and reviews this binding. The *process* is inherited from root
`agent-roles.md`; the personas add .NET review expertise. The review *criteria*
live in `ffi-marshalling.md` (anti-patterns) and §6 (verify) — this section
points at them.

### 7.1 Personas

- **`dotnet-actor`** and **`dotnet-critic`** (`.claude/agents/dotnet-*.md`)
  inherit the Actor / Critic roles and the `COMMENTS.<N>.md` loop from
  `agent-roles.md`. The Manager is the root `project-manager` (coordination is
  client-agnostic).
- They are needed because the root `actor-executor` / `kafka-critic` are
  Rust-translation-shaped and don't know P/Invoke / .NET interop.
- **Scope — the C# side, header-down.** The `dotnet-actor` builds only C# (from
  the generated header down) and **does not author Rust**; the `dotnet-critic`
  reviews only C#. When a feature needs a new ABI function (Mode B, §5.3
  steps 1–4), that's a Rust-core dependency on the root `actor-executor` /
  `kafka-critic`, not the `dotnet-*` personas.

### 7.2 Review ground truth (firm)

Review a change against the **C ABI header** (`confluent_kafka.h`) and the **Kafka
Java public API shape** — **not** Rust internals, and **not** Java implementation
logic (`bindings/CLAUDE.md §2`).

### 7.3 The Critic's lens (.NET-specific)

`SafeHandle` / `Dispose` correctness · handle leak / double-free / use-after-free ·
byte pinning & buffer lifetime · `MarshalAs(I1)` for `bool` · UTF-8 (no `LPStr`) ·
no managed exception through a callback · `RunContinuationsAsynchronously` on the
pump · flat `KafkaException` vs precondition .NET exceptions · **shape, not
logic**. The concrete checklist is the **Anti-patterns** blocks in
`ffi-marshalling.md` and the decision tables in §2/§3.

### 7.4 Mechanics

- Review comments: `bindings/dotnet/COMMENTS.<N>.md`; resolved →
  `COMMENTS.DONE.<N>.md`.
- Agent memory: `bindings/dotnet/.claude/agent-memory/<persona>/`.
- ⚠ **Nested-agent discovery is unverified** — if the harness does not
  auto-register `bindings/dotnet/.claude/agents/*.md`, place copies under the
  repo-root `.claude/agents/` or invoke with the persona files loaded explicitly.
