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
counterpart: `NativeMethods`, `SafeHandle` subclasses, `IDisposable`, marshalling
helpers, the C-callback→delegate adapter, the `TaskCompletionSource` bridge.
That's expected scaffolding — plumbing for the shape + safe resource management,
never Kafka behavior. If you're writing Kafka behavior in C#, you're in the wrong
layer.

---

## 1 · Orientation

**Four layers.** Java client (the shape) → Rust core (`src/…`, all logic) → C ABI
(`src/ffi/*.rs` → generated `target/include/confluent_kafka.h`) → **.NET binding**
(a `NativeMethods` P/Invoke class + `SafeHandle`s + managed types).

**One call, end to end** (mechanics → ffi-marshalling.md):

```
producer.Send(record)  ──►  Task<RecordMetadata>
   validate args · pin key/value · make TaskCompletionSource
        │  P/Invoke: NativeMethods.Producer_send(handle, …, out err) → future handle
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
(`assignment`/`subscription`/`paused`/`client_id`/`group_metadata`/`wakeup`/
`enforce_rebalance`). `current_lag` and `seek_with_metadata` are also sync-only and
are now **shipped as sync members** (M5/P7): `CurrentLag` is a genuine non-blocking
local read, and both `Seek` overloads ship sync for Python parity — a deliberate §4
divergence, not a gap. Only `close_with_timeout` remains sync-only yet **blocks in
Java** with no timed *async* form — a **gap**, not a design choice (so there is no
timed *async* close, §4 Disposal). ⚠ `Consumer_commit_async`
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
├─ Confluent.Kafka.sln    ← solution + shared config at the root
├─ Directory.Build.props · .editorconfig · .gitignore
├─ src/
│  └─ Confluent.Kafka/    ← the library project (named for the package id, §4)
│     ├─ <public API — flat at the root; topical folders (e.g. Admin/) as families grow>
│     └─ Internal/                       ← internal scaffolding — the ONLY non-public folder
│        └─ Interop/                      ← P/Invoke boundary — unsafe lives only here
└─ tests/
   └─ Confluent.Kafka.UnitTests/   ← Mock* unit tests
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

- **library project root** (`src/Confluent.Kafka/`) — **all public
  API** (namespace `Confluent.Kafka`), whatever the C# kind: the
  client types *and* supporting value types / enums (`ProducerRecord`,
  `RecordMetadata`, `Headers`, `TopicPartition`, later `ConsumerRecord` /
  `OffsetAndMetadata` / enums). If a user can name it, it lives here — flat while
  the surface is small, in a **topical** subfolder with the matching child
  namespace (e.g. `Admin/` → `Confluent.Kafka.Admin`, ckd's
  precedent) once a family grows.
- `Internal/` — **internal** managed scaffolding
  (`Confluent.Kafka.Internal`): the async-completion bridge (the
  consumer's callback→`TaskCompletionSource` adapter; the producer's pull-pump
  *or* push adapter — open, ffi §A7), config → properties marshalling.
- `Internal/Interop/` — the **P/Invoke boundary**
  (`Confluent.Kafka.Internal.Interop`): the `NativeMethods`
  `[DllImport]` class (`NativeMethods.cs` — the name CA1060 requires),
  `SafeHandle`s, `Utf8Marshal` helpers, callback delegates, and the blittable
  `[StructLayout]` mirror structs (e.g. the `ProducerRecord_t` mirror — the
  interop twin of the public `ProducerRecord`). `unsafe` lives only here; 1:1
  with `ffi-marshalling.md`.
- `tests/Confluent.Kafka.UnitTests/` — `Mock*` unit tests
  (`MockProducer`; `MockConsumer` as the consumer lands), a top-level sibling of
  `src/` in its own project directory **outside** the library tree — so default
  SDK compile globbing never pulls test files into the library assembly (no
  compile-scoping hack needed); `InternalsVisibleTo` grants access to internals.

---

## 3 · The target: the .NET API shape

Mirror the **Java** client in idiomatic C#. The producer surface is **generic-only**
(M11/P5 — Java's single `Producer<K,V>`, no bytes sibling; the non-generic bytes types
were **removed**, bytes users write `<byte[], byte[]>` + `Serdes.ByteArray`, exactly as
the consumer M6/P1b):

```csharp
namespace Confluent.Kafka;   // same id/assembly as ckd — revisit before publish, §4

public sealed class ProducerRecord<TKey, TValue> {   // Java `ProducerRecord<K,V>`
    public string Topic { get; }
    public int? Partition { get; }               // null = let the producer choose
    public long? Timestamp { get; }              // null = producer stamps it
    public TKey? Key { get; }                    // null (ref) = no key; three-state via the serializer (§4)
    public TValue? Value { get; }                // null (ref) = tombstone; three-state via the serializer (§4)
    public ProducerRecord(string topic, TValue? value,
        TKey? key = default, int? partition = null, long? timestamp = null);
}

public sealed class RecordMetadata {            // NON-generic (no K/V on the ack); getters → properties
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

public interface IAsyncProducer<TKey, TValue> : IAsyncDisposable, IDisposable {   // Java `Producer<K,V>`
    // method names mirror Java — no `Async` suffix; the interface carries the async distinction
    Task<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default);
    Task Flush(CancellationToken cancellationToken = default);
    Task Close(CancellationToken cancellationToken = default);
    Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default);
}

// The sync `IProducer<TKey,TValue>` (blocking mirror) is **shipped** (M11/P4), generic-only (M11/P5):
// RecordMetadata Send(ProducerRecord<TKey,TValue>) blocks (= Java send(record).get()); void Flush() /
// Close(); IReadOnlyList<PartitionInfo> PartitionsFor(string). Real + mock: KafkaProducer / MockProducer.

// Real clients take the serializers — Java KafkaProducer(Map, Serializer<K>, Serializer<V>).
public sealed class AsyncKafkaProducer<TKey, TValue> : IAsyncProducer<TKey, TValue> {
    public AsyncKafkaProducer(IReadOnlyDictionary<string, string> config,
        ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer);
}

public sealed class AsyncMockProducer<TKey, TValue> : IAsyncProducer<TKey, TValue> {   // Java `MockProducer<K,V>`
    // Mock TAKES the serializers — Java-faithful (MockProducer.java takes Serializer<K>/Serializer<V>
    // and serializes records into its history); UNLIKE the consumer, where mock-takes-deserializers is
    // a deviation. Send serializes K/V → bytes exactly like the real client (§4).
    public AsyncMockProducer(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer,
        bool autoComplete = true);
    public bool CompleteNext();
    public bool ErrorNext(int code, string? message = null);
    public int HistoryCount();                  // METHOD (M11/P4.1) — the ABI gives a count, not history()
    public void Clear();
}
```

**Clipped to today's ABI.** The sketch is the *producer* surface the current ABI
can back — it omits Java members the ABI doesn't expose yet:
`ProducerRecord.headers()`, `RecordMetadata`'s serialized-size / `has*` accessors,
and `MockProducer.history()` (Java returns the full record list; the ABI gives
only a count, hence `HistoryCount()` — a **method**, not a property, per the FDG
precedent that `Assignment()`/`Subscription()`/`Paused()` are methods (each does a
P/Invoke and can throw) plus Python `history_count()` parity). Add each when the
ABI grows to cover it.

**The typed serialize path (M11/P5).** `Send` serializes `TKey`/`TValue` → bytes in
the binding, **above** the bytes-based native producer and **before** the P/Invoke
(no per-record callback through the ABI, `CLAUDE.md §11`); the serialized bytes flow
into the *already-shipped* send/pump stack (an internal `SerializedProducerRecord`
carrier; zero new `[DllImport]`). Three decisions:

  - **Invoke-on-null (Java-faithful).** The serializer is **always invoked**, even for
    a `null` `TKey`/`TValue` (Java `KafkaProducer.doSend` calls `serialize(topic, …,
    record.key())` unconditionally); its `byte[]?` return drives the wire sentinel —
    `null` → absent (no key / tombstone), empty → present-empty, non-empty → present.
    This is the **intentional inverse** of the consumer's not-invoked short-circuit
    (which is *forced* — you cannot hand a deserializer a null span; the producer is
    not forced, so short-circuiting would diverge from Java).
  - **`SerializationException` wrap, thrown synchronously.** Any serializer throw is
    wrapped in a `SerializationException` (inner + topic context) and raised
    **synchronously** for **both** the sync and async `Send` — serialize runs
    pre-native on the caller thread (Java `send` throws `SerializationException`
    synchronously; the async `Send` throws before returning the `Task`, like its
    existing `ArgumentNullException` precondition). Java-contract fidelity, not a
    foreign-thread UB guard (contrast the consumer's fault-the-Task deserialize wrap).
  - **Mock-takes-serializers is Java-faithful.** Java's `MockProducer` takes
    `Serializer<K>/Serializer<V>` and serializes into its history, so the mock ctors
    take serializers too (`(keySer, valueSer, autoComplete=true)`) — **unlike** the
    consumer, where mock-takes-deserializers was a deviation.

**Consumer** follows the identical pattern — the Java surface in C# idiom. Its C
ABI has **landed**, so it's a **Mode A** build (§6.2); the receive-path key/value
ownership decision is §6.4. The surface is **generic-only** (M6/P1b — Java's single
`Consumer<K,V>`, no bytes sibling; bytes users write `<byte[], byte[]>` +
`Serdes.ByteArray`), clipped to today's ABI:

```csharp
public sealed class ConsumerRecord<TKey, TValue> {  // Java `ConsumerRecord<K,V>`, getters → properties
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    public long Timestamp { get; }
    public TimestampType TimestampType { get; }
    public TKey Key { get; }     // deserialized (span → T); default(TKey) if absent (deser NOT called — decision C)
    public TValue Value { get; } // deserialized; default(TValue) if absent (a tombstone; deser NOT called)
    public Headers Headers { get; }  // materialized owned bytes (not routed through a deserializer this phase)
}

public sealed class ConsumerRecords<TKey, TValue>          // Java `ConsumerRecords<K,V>`
    : IReadOnlyCollection<ConsumerRecord<TKey, TValue>> { }

public interface IConsumerCommon {               // shared sync surface (async + deferred sync mirror)
    void Wakeup();                                // interrupt a blocked poll — one-shot (idiom map)
    ConsumerGroupMetadata GroupMetadata();

    // non-blocking / instantaneous in Java → stays sync (idiom map; consumer-threading §1).
    // METHODS, not properties (M5/P1): each does a P/Invoke + marshalling, can throw, and
    // returns a fresh owned snapshot per call (FDG method rule) — matching GroupMetadata()
    // + Java/Python. Java returns a Set; IReadOnlySet post-dates netstandard2.0 → collection.
    IReadOnlyCollection<TopicPartition> Assignment();
    IReadOnlyCollection<string> Subscription();
    IReadOnlyCollection<TopicPartition> Paused();
    void EnforceRebalance(string? reason = null); // KIP-848 logged no-op → returns success
    void CommitAsync();                           // Java commitAsync — non-blocking, fire-and-forget (§4 note; M5/P6)

    // Sync seek + current-lag (M5/P7) — Python parity. Seek BLOCKS in Java yet ships sync
    // here (calls the sync ABI directly, not Task.Run — a deliberate §4 divergence);
    // CurrentLag is a genuine non-blocking local read. Both flavor-independent → this base.
    void Seek(TopicPartition partition, long offset);                          // Java seek(tp, long)
    void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);   // Java seek(tp, OffsetAndMetadata)
    long? CurrentLag(TopicPartition partition);                                // Java currentLag(tp); empty → null

    // Metrics + client id (M9/P2) — Python parity, both SHIPPED sync state reads (§4).
    IReadOnlyDictionary<MetricName, IMetric> Metrics();                        // Java Map<MetricName, ? extends Metric> metrics()
    string ClientId();                                                        // Python client_id() — beyond-Java (deviation); concurrent-null → InvalidOperationException (stricter than Python)
}

// Generic-only (M6/P1b): only Poll retypes; every other member is K/V-free and inherited from the
// non-generic IConsumerCommon or restated unchanged. Bytes users write IAsyncConsumer<byte[], byte[]>.
public interface IAsyncConsumer<TKey, TValue> : IConsumerCommon, IAsyncDisposable, IDisposable {   // Java `Consumer<K,V>`
    // blocking-in-Java / callback-at-ABI → async (§4); method names mirror Java — no `Async` suffix
    Task<ConsumerRecords<TKey, TValue>> Poll(TimeSpan timeout, CancellationToken cancellationToken = default);
    Task Subscribe(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default);
    Task Unsubscribe(CancellationToken cancellationToken = default);
    // Java commitSync / commitSync(Map) — blocks in Java → Task (async-bridged; §4 note; M5/P6)
    Task Commit(CancellationToken cancellationToken = default);
    Task Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        CancellationToken cancellationToken = default);
    Task<long> Position(TopicPartition partition, CancellationToken cancellationToken = default);
    Task Close(TimeSpan timeout, CancellationToken cancellationToken = default);

    // Assignment() / Subscription() / Paused() / EnforceRebalance() / Wakeup() /
    // GroupMetadata() / CommitAsync() (Java commitAsync, fire-and-forget) / Seek(tp,long) /
    // Seek(tp,OffsetAndMetadata) / CurrentLag() (M5/P7) live on IConsumerCommon
    // (non-blocking, or sync for Python parity → stays sync)
}

public sealed class AsyncKafkaConsumer<TKey, TValue> : IAsyncConsumer<TKey, TValue> {   // Java `KafkaConsumer` (KIP-848)
    // 3-param ctor (decision A) — Java KafkaConsumer(Map, Deserializer<K>, Deserializer<V>), KafkaConsumer.java:601
    public AsyncKafkaConsumer(IReadOnlyDictionary<string, string> config,
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer);
}

public sealed class AsyncMockConsumer<TKey, TValue> : IAsyncConsumer<TKey, TValue> {    // Java `MockConsumer`
    // Deviation (§7): the ctor TAKES the deserializers (Java's mock doesn't) — its Poll decodes native
    // bytes like the real consumer. AddRecord stays BYTES-in (Java's is typed-in) — tests the deserialize
    // path in isolation + forced by the bytes-only core ABI. Mock helpers are inherent, not on IAsyncConsumer.
    public AsyncMockConsumer(IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer,
        string? autoOffsetReset = null);
    public void AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value);
}

// The sync `IConsumer` (blocking mirror of `IAsyncConsumer`) is **shipped** — the most
// Java-faithful surface (Java's `Consumer` is synchronous), a sibling of the async trio over the
// SAME native consumer (not a wrapper). Generic-only (M6/P1b), no `CancellationToken` (interruption is
// `Wakeup()` only), with both `Close()` and `Close(TimeSpan)`. Each sync method calls the sync C
// ABI directly (block_on inside the Rust core's runtime — NOT sync-over-async, §4). Grown in two
// sub-phases: the core loop (M5/P8a) + the query family (`Committed` / `OffsetsForTimes` /
// `BeginningOffsets` / `EndOffsets` / `PartitionsFor` / `ListTopics`, added additively in M5/P8b).
// KafkaConsumer<K,V> / MockConsumer<K,V> mirror the async pair's 3-param / deserializer-taking ctors.
public interface IConsumer<TKey, TValue> : IConsumerCommon, IDisposable {   // Java `Consumer<K,V>` (synchronous)
    ConsumerRecords<TKey, TValue> Poll(TimeSpan timeout);      // blocks; Wakeup() interrupts (one-shot)
    void Subscribe(IReadOnlyCollection<string> topics);
    void Unsubscribe();
    void Assign(IReadOnlyCollection<TopicPartition> partitions);
    void Pause(IReadOnlyCollection<TopicPartition> partitions);
    void Resume(IReadOnlyCollection<TopicPartition> partitions);
    void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions);
    void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions);
    long Position(TopicPartition partition);
    void Commit();                                             // Java commitSync (confirming)
    void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets);
    // ---- query family (M5/P8b) — blocks; returns the owned result directly ----
    IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Committed(IReadOnlyCollection<TopicPartition> partitions);
    IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> OffsetsForTimes(IReadOnlyDictionary<TopicPartition, long> timestampsToSearch);
    IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(IReadOnlyCollection<TopicPartition> partitions);
    IReadOnlyDictionary<TopicPartition, long> EndOffsets(IReadOnlyCollection<TopicPartition> partitions);
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic);
    IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics();
    void Close();                                              // Java close()
    void Close(TimeSpan timeout);                              // Java close(Duration); negative → ArgumentOutOfRange, Zero valid
    // Wakeup() / Assignment() / Subscription() / Paused() / GroupMetadata() / EnforceRebalance() /
    // CommitAsync() / Seek(tp,long) / Seek(tp,OffsetAndMetadata) / CurrentLag() come from IConsumerCommon.
}

public sealed class KafkaConsumer<TKey, TValue> : IConsumer<TKey, TValue> {   // Java `KafkaConsumer` (KIP-848), synchronous
    public KafkaConsumer(IReadOnlyDictionary<string, string> config,
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer);  // 3-param (decision A)
}

public sealed class MockConsumer<TKey, TValue> : IConsumer<TKey, TValue> {    // Java `MockConsumer`, synchronous
    public MockConsumer(IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer,
        string? autoOffsetReset = null);   // deviation §7: takes deserializers (Java's mock doesn't)
    public void AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value); // bytes-in (§7); inherent
}
```

**Clipped to today's ABI**, like the producer — the fuller Java surface lands as
each piece is wired (async/sync split per the idiom map). Already wired:
`Assign`/`SeekToBeginning`/`SeekToEnd`/`Pause`/`Resume`/`Position`/`Committed`/
`BeginningOffsets`/`EndOffsets`/`OffsetsForTimes` (M4/M5), plus
`PartitionsFor`/`ListTopics` (M5/P5, with the nested public value types
`PartitionInfo`/`Node`), with the public value types
`OffsetAndMetadata`/`OffsetAndTimestamp` (M5/P4), plus the commit family
`Commit`/`Commit(offsets)`/`CommitAsync` (M5/P6, with the public
`OffsetAndMetadata` constructor), plus the **sync** `Seek(tp, long)` /
`Seek(tp, OffsetAndMetadata)` + `CurrentLag` on `IConsumerCommon` (M5/P7 — Python
parity; `Seek` moved async→sync and down onto the shared base). The **typed generic
`Consumer<K,V>`** is now **shipped** (M6/P1b — generic-only conversion + the zero-copy
typed poll; see §4). Still to come: pattern subscribe, typed *headers* on
`ConsumerRecord<K,V>` (they are materialized owned bytes today), and a
`ConsumerRebalanceListener` argument on `Subscribe`. The typed **producer**
`Producer<K,V>` is now **shipped generic-only** (M11/P5 — the §A7 gate was closed by
Option C's inline pull-pump shipping in M11/P3; see §4).

The **admin client** (`IAdminClient`) is still **Mode B** — sketched once its C
ABI lands (§6.3).

**Serdes** — the (de)serialization foundation (M6/P1a), the Java
`Serializer<T>` / `Deserializer<T>` / `Serde<T>` shape in C# over the bytes-only ABI
(a pure managed, binding-/user-layer concern, §4). `IDeserializer<T>` is **consumed by
P1b's typed consumers**; `ISerializer<T>` is **now consumed by the shipped typed producer (M11/P5)**:

```csharp
public interface ISerializer<T> {
    byte[]? Serialize(string topic, T data);              // Java Serializer<T>.serialize; nullable (see below)
}
public interface IDeserializer<T> {
    T Deserialize(string topic, ReadOnlySpan<byte> data); // Java Deserializer<T>.deserialize — sync, span (see below)
}
public interface ISerde<T> : ISerializer<T>, IDeserializer<T> { }   // Java Serde<T> — what Serdes returns

public static class Serdes {                              // Java `Serdes` factory — Java wire-format parity
    public static ISerde<string> String { get; }         // UTF-8
    public static ISerde<byte[]> ByteArray { get; }       // identity (deserialize copies the span out)
    public static ISerde<int>    Int32 { get; }           // 4 bytes big-endian (IntegerSerializer)
    public static ISerde<long>   Int64 { get; }           // 8 bytes big-endian (LongSerializer)
    public static ISerde<double> Double { get; }          // 8 bytes big-endian doubleToLongBits (DoubleSerializer)
    public static ISerde<Guid>   Guid { get; }            // ⚠ UUID.toString() -> UTF-8, NOT 16 raw bytes (UUIDSerializer)
    public static ISerde<object?> Null { get; }           // VoidSerializer (serialize null; deserialize default)
}

public class SerializationException : KafkaException { }  // flat subclass; serdes throw on malformed input
```

Deliberate deviations, recorded (§4 decision-point latitude; consumer-threading §28 style):

- **`IDeserializer<T>` is sync + `ReadOnlySpan<byte>`**, not Java's `byte[]` — the §6.4 / §27
  zero-copy lock. A `byte[]` param forces a per-record copy; the `ref struct` span borrows the
  native fetch slice in place and provably can't outlive the batch (can't be stored/boxed/
  awaited/sent). Sync because a span can't cross an `await` and serde is CPU-bound — also
  Java-faithful (`Deserializer<T>` is sync).
- **Headers overload deferred** (Java's `default T deserialize(String, Headers, byte[])`) —
  addable later non-breakingly as a C# default-interface-method forwarding to the header-less form.
- **Async serde deferred** — a note only, no async interface; a Schema-Registry path *may* later
  want one (decided then). Java's SR serdes are themselves sync.
- **`Serialize` returns `byte[]?`** (nullable), not the plan's shorthand `byte[]` — Java's
  serializers return `null` for `null` input and `VoidSerializer` always returns `null`, and a
  `null` value is a produce-path tombstone (`ProducerRecord.Value` is nullable). Precise
  nullability per §4 (`#nullable enable`, annotate precisely).
- **`ISerde<T>` added** = Java's `Serde<T>` (what `Serdes.String()` returns); composes the two
  shipped directional interfaces so a `Serdes` member offers both directions from one type. Java's
  `Serde<T>` uses `serializer()`/`deserializer()` accessors + `Closeable`; ours extends both
  directly (stateless serdes, nothing to close).

**The Java → C# idiom map** — the binding's spine. Each row: the Java construct,
its C# realization, and where the enforcing rule lives.

| Java | C# idiom | Rule / detail |
|---|---|---|
| `Future<RecordMetadata>` | `Task<RecordMetadata>` | `TaskCompletionSource` completion — producer pull-pump *or* push (open); consumer push — ffi §A7/§B7 |
| **blocks** in Java, **or** returns `Future<T>`, **or** takes a completion callback — any one is enough (producer `send`/`flush`/`close`/`partitionsFor`; consumer `poll`/`commitSync`/`position`/`subscribe`/`assign`/`pause`/`resume`/`unsubscribe`) | `Task`/`Task<T>` on the **async** interface (`IAsyncProducer`/`IAsyncConsumer`) + `CancellationToken`; method name **mirrors Java** (no `Async` suffix) | the three async triggers — §4 **Sync vs async**; best-effort cancel ffi §A7/§B7. ⚠ `seek` also blocks in Java but ships **sync** (Python parity, M5/P7 §4 divergence — see **Stays sync**) |
| `close()` / `AutoCloseable` | `IAsyncDisposable.DisposeAsync()` (+ `IDisposable`) | graceful close drains the in-flight op / joins the pump — ffi §A2/§A7, §B2/§B7 |
| `KafkaException` hierarchy | one flat `KafkaException` (`Code`/`IsRetriable`/`IsFatal`) | ffi §A5 |
| `IllegalArgumentException` / `IllegalStateException` | `ArgumentException` (family) / `InvalidOperationException` (`ObjectDisposedException` when used after close) | validate **before** the FFI call — ffi §A5 |
| `wakeup()` (interrupt a blocked `poll`/`commit`) | sync `Wakeup()`; the in-flight `Poll`/`Commit` throws flat `KafkaException` (Wakeup code, **one-shot**) | ffi §B5 |
| `ConcurrentModificationException` (consumer is one-op-in-flight) | `InvalidOperationException` (concurrent sync state read) / `KafkaException` (concurrent async op) | ffi §B5 |
| `ConsumerRebalanceListener` | `IConsumerRebalanceListener` (async) | invoked on the **caller's task** during `poll`/`commit`/`close` — consumer-threading §31 |
| `OffsetCommitCallback` | `IOffsetCommitCallback` (async) | same caller's-task model — consumer-threading §31 |
| **non-blocking** in Java — a pure local read, or an action with no completion signal (`assignment()`, `subscription()`, `paused()`, `groupMetadata()`, `wakeup()`, `beginTransaction()`, mock helpers) | **stays sync** — a **property** for a getter, a plain **method** for an action | only 8 consumer members qualify — §4 **Sync vs async**, `consumer-threading.md §1` |
| method `send`, `flush`, `poll` | PascalCase, **mirror Java** — no `Async` suffix (`Send`, `Poll`); the async distinction is carried by the interface (`IAsyncProducer`/`IAsyncConsumer` async; `IProducer`/`IConsumer` the deferred sync mirror), matching `bindings/CLAUDE.md §2.2` + the Python sibling | §4 |
| `byte[]` key/value | `ReadOnlyMemory<byte>` | send: pinned zero-copy — ffi §A4; receive: copy-out (default), keep-alive deferred — ffi §B4 / §6.4 |
| opaque handle | `SafeHandle` (owned) / `IntPtr` (transient) | ffi §A2/§B2 |
| `String` topic / config | UTF-8, hand-marshalled | ffi §A3/§B3 |
| `Duration` (timeouts: `poll`/`close`/`committed`) | `TimeSpan` | ABI takes `int64_t` ms |
| `Map` / `Set` / `List` (returns) | `IReadOnlyDictionary` / `IReadOnlyCollection` / `IReadOnlyList` | `IReadOnlySet` post-dates netstandard2.0 → `IReadOnlyCollection` |
| `Producer<K,V>` / `Consumer<K,V>` (generic) | **both shipped generic-only** — `Consumer<K,V>` (M6/P1b) and now `Producer<K,V>` (M11/P5): the producer trios + `ProducerRecord` are `<TKey,TValue>`, the non-generic ones removed (bytes = `<byte[],byte[]>` + `Serdes.ByteArray`); `RecordMetadata` stays non-generic. Serialize above the bytes core, invoke-on-null, synchronous `SerializationException` wrap (§4) | §4 |

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
| **Namespace / package id** | **`Confluent.Kafka`** — bare name for namespace, assembly and package id (same identity as ckd, which this client is meant to replace). ⚠ **Strong gate — revisit before publishing:** a shared id means a project can hold ckd 2.x **or** this client, never both, so ckd's Schema-Registry / OAuthBearer packages can't be mixed in. Decide then: own SR integration, or diverge the id. | before any public type |
| **Disposal** | Both `IAsyncDisposable.DisposeAsync()` (primary; drains the in-flight op / joins the pump, then `flush`/`close`, without blocking) and `IDisposable.Dispose()` (blocking fallback). `close(Duration)` → `Close(TimeSpan)` (no `Async` suffix — mirrors Java; the timed *consumer* close is deferred, §1). *Note:* the timeout is ABI-backed only for the **consumer** (`Consumer_close_with_timeout`); `Producer_close`/`_flush` take none, so a producer `TimeSpan` is a .NET-side deadline until a timed producer close lands. | first client type |
| **Cancellation** | `CancellationToken` on every async method, honored best-effort. **Producer:** cancels the *wait*, never aborts an enqueued send (ffi §A7). **Consumer:** maps to `wakeup()` → the in-flight op cancels/faults (ffi §B7). A host-idiom addition Java lacks (allowed by `bindings/CLAUDE.md §2`). | first async method |
| **Sync vs async** | Decide **per method from the Java implementation** (`AsyncKafkaConsumer` / `KafkaProducer`) — never from the Javadoc, the interface, or the method name. Three triggers make it async; everything else stays sync. See the **Sync vs async** note below. | every public method |
| **Async naming** | Method names **mirror Java** — **no** `Async` suffix (`Send`, `Poll`, `Commit`). The sync/async distinction is carried by the **interface/class**, not the method name (`IAsyncProducer`/`IAsyncConsumer` async; `IProducer`/`IConsumer` the deferred sync mirror), matching `bindings/CLAUDE.md §2.2` + the Python sibling. `Task`-returning methods still return `Task`; the name just drops the suffix. | first async method |
| **Interface naming** | Async interfaces `IAsyncProducer` / `IAsyncConsumer`; the sync mirror is `IProducer` / `IConsumer` — **`IConsumer` is shipped (M5/P8a)**, `IProducer` still deferred. C#'s `I`-prefix is the lexical marker for an interface (Framework Design Guidelines; analyzer CA1715 warns without it); the sync/async split is carried by the **interface + type** (`IAsyncConsumer`/`AsyncKafkaConsumer` async, `IConsumer`/`KafkaConsumer` sync), **no `Async` suffix on methods** (they mirror Java). Each has a real + mock impl (`KafkaProducer`/`MockProducer`, `AsyncKafkaConsumer`/`AsyncMockConsumer`, `KafkaConsumer`/`MockConsumer`). Deviation: strict-Java bare `Producer`/`Consumer` (fights CA1715 / dev expectation). | first interface type |
| **Key/value type** | `ReadOnlyMemory<byte>` both ways. **Producer (send):** zero-copy — pins the user buffer via `MemoryHandle` (ffi §A4). **Consumer (receive):** wraps an owned copied array (copy-out, §6.4), not a pin. `byte[]`-only is an acceptable interim. | porting `ProducerRecord` / `ConsumerRecord` |
| **Serializers** | **Foundation shipped (M6/P1a)** — the bidirectional serde surface `ISerializer<T>` (`byte[]? Serialize(topic, T)`) + `IDeserializer<T>` (`T Deserialize(topic, ReadOnlySpan<byte>)`, sync + zero-copy over the batch, ffi §B4) + `ISerde<T>` (both, the Java `Serde<T>` shape returned by the `Serdes` factory), with the built-in `Serdes` (String/ByteArray/Int32/Int64/Double/Guid/Null — Java wire-format parity) and `SerializationException`. `IDeserializer<T>` is **now consumed by the shipped typed consumers (M6/P1b)** — the zero-copy typed poll deserializes each record's key/value from a `ReadOnlySpan<byte>` over the native batch (no intermediate per-record `byte[]`), applying the **null→`default(T)` three-state** model (absent → `default(T)`, deserializer NOT called; present-empty → 0-length span; present → the span) and a **mandatory `SerializationException` wrap** of any deserializer throw (inner + topic/partition/offset; on the async path the deserialize runs on the core's foreign dispatcher thread, so the wrap-and-fault — never an unwind into native — is mandatory). `ISerializer<T>` is **now consumed by the shipped typed producer (M11/P5)** — `Send` serializes each record's key/value to bytes **above** the bytes-based native producer (before the P/Invoke), always invoking the serializer even on a `null` `TKey`/`TValue` (Java-faithful — the `byte[]?` return drives the absent/present-empty/present sentinel, the intentional inverse of the consumer's not-invoked short-circuit), and wrapping any serializer throw in a **`SerializationException` thrown synchronously** for both the sync and async `Send` (serialize is pre-native on the caller thread — Java-contract fidelity, not a foreign-thread UB guard). No per-record callback through the ABI (`CLAUDE.md §11`). See §3 for the sketch + the deliberate deviations. | ✅ M6/P1a (foundation) · ✅ M6/P1b (typed consumers) · ✅ M11/P5 (typed producer) |
| **Config** | `IReadOnlyDictionary<string,string>` → per-entry `ProducerProperties_put` (consumer: `ConsumerProperties_put`); keys are **Java dotted names** (`bootstrap.servers` required); coerce non-string values to `str`; classic-/consumer-only keys accepted silently. | wiring the constructor |
| **Error granularity** | One flat `KafkaException` now; typed subclasses can be added under it later, non-breakingly (ffi §A5). The first such subclass is shipped: **`SerializationException : KafkaException`** (M6/P1a) — a flat Java-parity subclass the built-in serdes throw on malformed input; catchable as `KafkaException`. | if catch-by-type is needed |
| **Interceptors** | Defer; reserve the Java-shaped name. | a concrete need |
| **Nullable reference types** | `#nullable enable` project-wide; annotate the P/Invoke surface precisely. | project setup |

### Sync vs async — the governing rule

Decide from the **Java implementation** (`AsyncKafkaConsumer` / `KafkaProducer`),
never from the Javadoc, the interface, or the method name. Under KIP-848 the
consumer is an event loop: the app thread enqueues an event and *waits for the
background thread to apply it*, so `subscribe`, `assign`, `seek`,
`seekToBeginning`/`seekToEnd`, `pause`, `resume` and `unsubscribe`
all **block** despite reading as instantaneous (classic-consumer intuition does
not transfer). **If you cannot check, assume it blocks.** (`currentLag` is the
exception — a genuine non-blocking local read, shipped **sync**. And `seek`, though
it blocks in Java, is deliberately shipped **sync** for Python parity — a §4
divergence, M5/P7; see **Stays sync** below.)

| Java signal | C# |
|---|---|
| **Blocks** — `addAndGet` · `processBackgroundEvents` · `getResult` · `result.await` · `waitOnMetadata` | `Task`/`Task<T>` on the async interface, `CancellationToken`; name mirrors Java (no `Async` suffix) |
| **Returns `Future<T>`** — even if it barely blocks (`send`) | `Task<T>` on the async interface; name mirrors Java (no `Async` suffix) |
| **Takes a completion callback** — even if non-blocking (`send(record, Callback)`, `commitAsync(OffsetCommitCallback)`) | `Task`/`Task<T>` on the async interface — the `Task` **replaces** the callback; do **not** add a callback-taking overload |
| Non-blocking **getter** | sync **property** |
| Non-blocking **action**, no completion signal | sync plain **method** |

Any **one** trigger is enough — blocking is just the most common of the three.

**Stays sync on the consumer — exactly these:** `Assignment()`, `Subscription()`,
`Paused()` (**methods** — shipped M5/P1; they override the generic "getter →
property" idiom-map row on FDG grounds: each does a P/Invoke + marshalling, can
throw, and returns a fresh owned snapshot per call, matching the shipped
`GroupMetadata()` + Java/Python), `GroupMetadata()`, `Wakeup()`, `Metrics()`
(**shipped M9/P2** — `IReadOnlyDictionary<MetricName, IMetric> Metrics()`, Java
`metrics()`; concurrent-access null → `InvalidOperationException`, the shared sync-read
mapping), `ClientId()` (**shipped M9/P2** — Python parity; a **beyond-Java** deviation —
Java's `clientId()` is package-private, not on the `Consumer` interface — and
**stricter than Python** on concurrent access: non-nullable `string`, null →
`InvalidOperationException`), `Register`/`UnregisterMetricForSubscription`, and
`EnforceRebalance(string? reason
= null)` (a no-op that only logs under KIP-848 → returns success, never throws on
that path; one method collapses Java's two overloads), plus (M5/P7) `CurrentLag(tp)`
and **both** `Seek(tp, long)` / `Seek(tp, OffsetAndMetadata)` overloads. **On the
producer:** `Metrics`, `BeginTransaction()`, and the two metric-subscription methods.
Everything else is async.

⚠ **§4 divergence — sync `Seek` / `CurrentLag` (M5/P7).** `CurrentLag` is a genuine
non-blocking local read, so it stays sync straightforwardly. `Seek` **blocks** in
Java (an `addAndGet` event round-trip), so the idiom map above would map it to a
`Task`; it is nonetheless shipped **synchronous** here because (a) Python exposes
`seek` synchronously, (b) `seek_with_metadata` has **no `_async` ABI variant**, and
(c) a sync method calling the sync ABI **directly** (no `Task.Run`) is legitimate —
not the sync-over-async footgun the note just below guards against. The caller parks
inside the core's `block_on` (deadlock-free — multi-thread runtime, ffi §B1),
exactly as the shipped `EnforceRebalance` / `CommitAsync` sync-op paths. `Seek(tp,
long)` keeps a Java-fidelity negative-offset guard (`ArgumentOutOfRangeException`,
`"seek offset must not be a negative number"`) — the one place .NET is deliberately
stricter than Python.

⚠ **The ABI must be able to honor it.** Where Java blocks but the ABI exposes only
a sync entry point, wrapping the sync call in `Task.Run` would be sync-over-async
(forbidden, ffi §B7). `current_lag` / `seek_with_metadata` are **now shipped as sync
members** (M5/P7, per the divergence above — a sync member calling the sync ABI
directly is not sync-over-async). Only `close_with_timeout` remains an un-honorable
gap (no timed *async* close), a **Mode B** (§6.3) item, not a judgment call.

**Exception — Java sync/async pairs (the commit family, M5/P6).** Where Java ships
an explicit pair (`commitSync`/`commitSync(Map)` + `commitAsync`), keep **both**, but
map each **from its Java blocking behavior** (the idiom map), not its Java name:

- **`Task Commit(...)`** (two overloads, on `IAsyncConsumer`) = the **confirming**
  commit — Java `commitSync` / `commitSync(Map)`; Python `commit()`. Java `commitSync`
  **blocks** → idiom map → `Task`. It is **async-bridged** over the void
  `op_callback_t` (`Consumer_commit_sync_async` / `_commit_sync_offsets_async`), *not*
  a blocking-thread `CommitSync` façade — bridging avoids the blocking-thread footgun
  (a `CommitSync` that `block_on`s the caller thread). You can `await` it to know the
  commit landed.
- **`void CommitAsync()`** (on `IConsumerCommon`) = the **fire-and-forget** commit —
  Java `commitAsync()`; Python `commit_async()`. Java `commitAsync` is **non-blocking**
  → idiom map → sync `void`. It calls the sync `Consumer_commit_async` (returns
  `KafkaError*`) directly (the `EnforceRebalance` sync-op shape), takes no
  `CancellationToken`, and lives on the shared non-blocking base because it is
  flavor-independent (§3 / PLAN §5).

Naming: `Commit` + `CommitAsync` is **exact Python parity** (`commit` / `commit_async`)
— note the mapping inverts the Java-name intuition (Java's `commitSync` → our `Commit`;
Java's `commitAsync` → our `CommitAsync`), because the map reads Java's *blocking
behavior* (`commitSync` blocks → `Task`; `commitAsync` non-blocking → sync `void`), not
the Java method name. There is **no** `CommitSync` member — a blocking-thread sync
façade would be the very sync-over-async footgun the async bridge exists to avoid.

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
   yes ─┤→ MODE A · .NET-only    (NativeMethods decl + SafeHandle + wrapper)         → 6.2
        │
   no ──┘→ MODE B · Full-stack   (src/ffi → header → NativeMethods → managed API)    → 6.3
```

### 6.2 Mode A — .NET-only port

The `kafka_*` function already exists in the header:

1. **P/Invoke** — add the `[DllImport]` declaration to `NativeMethods` (ffi §0.1).
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
5. **Wrap in `NativeMethods`** — `[DllImport]` declarations + `SafeHandle`s.
6. **Expose the managed API** — the Java-shaped surface in a new class.
7. **Test & build** — Mock/parity tests → `dotnet build` → `dotnet test`.

**Naming across layers:** `kafka_<pkg-minus-clients>_<Type>_<method>` at the ABI;
C# casing above it (PascalCase, properties for getters, method names mirror Java —
no `Async` suffix; the async distinction is carried by the interface, §3/§4).

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
  `COMMENTS.DONE.<N>.md`. **Both are local working files — neither is committed at
  the binding root.** The single tracked record is the Manager's archived copy at
  `design/history/<Milestone>/<Phase>/COMMENTS.DONE.<N>.md` (below). This matches
  the repo-root Rust convention, where every closed record lives only under
  `design/history/`. Committing the binding-root `COMMENTS.DONE.<N>.md` duplicates
  an immutable archive with a *mutable* file that the next phase reusing the same
  `<N>` will overwrite. Note `COMMENTS.<N>.md` is already covered by the root
  `.gitignore` (`COMMENTS\.[0-9]*\.md`) but `COMMENTS.DONE.<N>.md` is **not**, so
  keeping it out of commits is a discipline, not a mechanism — never `git add` it.
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
