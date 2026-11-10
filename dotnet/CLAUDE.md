# Confluent Kafka .NET Binding — Claude Rules

**What this is.** The rulebook for the .NET binding under `bindings/dotnet/` — a
P/Invoke wrapper that restores Apache Kafka's **Java client API shape** in
idiomatic C# on top of the Rust core's C ABI. It holds the .NET binding's
*shape, decisions, and governance*; the boundary *mechanics* live in
`.claude/rules/ffi-marshalling.md`.

**How it loads.** Working anywhere under `bindings/dotnet/` stacks three
rulebooks: root `CLAUDE.md` → `dotnet/.claude/rules/bindings.md` → this file. Heavy deep-dives
live in `.claude/rules/*.md` and are read **on demand** — this rulebook links
them explicitly (never rely on nested auto-loading).
The shared cross-binding rulebook (moved from `bindings/CLAUDE.md` in M17/P2) is [`dotnet/.claude/rules/bindings.md`](.claude/rules/bindings.md) — read it explicitly.

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
(`src/ffi/*.rs` → generated `target/include/confluent_kafka_bindings.h`) → **.NET binding**
(a `NativeMethods` P/Invoke class + `SafeHandle`s + managed types).

**One call, end to end** (mechanics → ffi-marshalling.md):

```
producer.Send(record)  ──►  ValueTask<AsyncKafkaFuture<RecordMetadata>>   (await → handed over · await future.Get() → delivered)
   validate args · serialize · seal headers · pin key/value · build the record (OptionsBuilder) · Callback_new (self = the record's state)
        │  P/Invoke: kafka_producer_Producer_send_with_callback_cb(view, record, callback, cb, opaque)
        │  the handover cb unpins; the first stage completes in Send when the producer takes or refuses
        │    the record at once, else in the cb on the CompletionThread. Later on_completion runs the user
        │    callback, then the record's KafkaFutureImpl completes, on the CompletionThread (ffi §A1, §A6,
        │    §A7). The consumer and admin use the same queue.
        ▼
   C ABI  kafka_producer_Producer_send_with_callback_cb / _Producer__execute_callbacks   (flat pointers, out_err)
        ▼
   Rust   Producer::send(ProducerRecord, Callback) → KafkaFuture<RecordMetadata>   (the logic)
```

**Status:** The C ABI exposes the **producer**, **consumer** and **admin client**. Every blocking op
has a blocking form and a `_cb` twin, whose completion is queued for the client's
`__execute_callbacks` (M19/P1; the `851c8d95` ABI, ledger 65), except that the
producer's `send_cb` and `send_with_callback_cb` complete inside the call when the producer takes
or refuses the record at once (`ffi-marshalling.md` §A1); the consumer's instantaneous ops have no `_cb` twin
(`assignment`/`subscription`/`paused`/`group_metadata`/`metrics`/`wakeup`). There is no
`client_id`, and the consumer has no `ClientId()` (ledger 102). `enforce_rebalance` has a `_cb`
twin, but `EnforceRebalance` stays sync: Java's KIP-848 `enforceRebalance` only logs.
`current_lag` and `seek_*` wait on the background task and have `_cb` twins, so `CurrentLag`
and `Seek` are blocking members on `IConsumer` and return a `Task` on `IAsyncConsumer`
(decision 22, ledger 103). Java's `close(CloseOptions)` is `Close(CloseOptions)`, over
`close_with_options` and its `_cb` twin (decision 22). ⚠ `Consumer_commit_async` is Java's
`commitAsync`: a call with `out_err` that returns once the commit is started; its `_cb` twin
`commit_async_cb` completes when the core has accepted the commit (decision 13). The `_cb` twin
of `commitSync` is `commit_sync_cb`. The nine timed forms (`*_with_timeout` and
`commit_sync_with_offsets_timeout`), each with a `_cb` twin, carry Java's `Duration` overloads
(decision 22). The admin client is exposed
as `IAdmin` (`KafkaAdminClient` / `MockAdminClient`, M15); since M15/P14 that is
the sync flavour, beside the async `IAsyncAdmin` (`AsyncKafkaAdminClient` /
`AsyncMockAdminClient`). Every admin RPC returns its `*Result_t` at once, its futures complete
through `kafka_common_KafkaFuture_get_cb`, and only `close` and `close_with_timeout` have `_cb`
twins (M19/P1). Producer transactions are exposed on all four producer classes (§4 **Producer
transactions**). Source of truth for the surface = `src/ffi/*.rs` +
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
│     ├─ Producer/ · Consumer/ · Admin/   ← public API: one folder per Java package, `clients` dropped
│     ├─ Common/  + Acl/ · Config/ · Errors/ · Header/ · Quota/ · Record/ · Resource/ ·
│     │            Security/Auth/ · Security/Token/Delegation/ · Serialization/
│     └─ Internal/                       ← internal scaffolding — the ONLY non-public folder
│        └─ Interop/                      ← P/Invoke boundary — unsafe lives only here
└─ tests/
   ├─ Confluent.Kafka.UnitTests/          ← Mock* unit tests
   └─ Confluent.Kafka.IntegrationTests/   ← broker-backed tests (Testcontainers), as python/test/integration
```

(Folders are organizational; C# accessibility is still the `internal` keyword +
the assembly.) **`Internal/` is the visibility marker — the *only* folder that
implies non-public:** every type under it is explicitly `internal` (and `sealed`
where practical), and `Internal/Interop/` is the only place `unsafe` appears.
Wanting a type under `Internal/` to be `public` is the signal it belongs in the public tree.
**Every public type lives in the namespace of its Java package, and its file in the folder that
mirrors that namespace** (M20/P1, ledger 253): drop `org.apache.kafka.`, then `clients.`, PascalCase
each remaining segment, and prefix `Confluent.Kafka.`, as the Rust core names its modules (root
`CLAUDE.md` §2). So `org.apache.kafka.clients.producer.KafkaProducer` is
`Confluent.Kafka.Producer.KafkaProducer` in `Producer/KafkaProducer.cs`,
`org.apache.kafka.common.acl.AclBinding` is `Confluent.Kafka.Common.Acl.AclBinding` in
`Common/Acl/AclBinding.cs`, and `org.apache.kafka.common.security.token.delegation.DelegationToken`
is `Confluent.Kafka.Common.Security.Token.Delegation.DelegationToken`. The only public types declared in
`Confluent.Kafka` itself are the eight JDK exception classes (below). IDE0130 (`.editorconfig`) makes a namespace that does not match its folder
a build error, and `PublicNamespaceLayoutTests` checks every exported type against its Java
package, read from `kafka/`. There is no inner `src/` inside the project: the outer top-level
`src/` *is* the library project's parent.

- **A Java package the binding does not have yet** gets its own namespace and folder when its first
  public type arrives (`org.apache.kafka.common.metrics` → `Common/Metrics/`), never the nearest
  existing one, as Python does.
- **One file per Java top-level class**, named after the C# type, with the class's Java nested types
  nested in it. A C# type that only completes a Java class sits in that class's file: the
  non-generic `KafkaFuture` (Java's `KafkaFuture<Void>`) beside `KafkaFuture<T>`, a static class
  holding a Java enum's methods beside the enum (`<Enum>Methods`, §3 Java names rule 9; today's
  `GroupStateExtensions`, `ScramMechanisms` and `UpgradeTypes` are baselined until renamed), and the
  `<Interface>Extensions` class and `I<Interface>With<Method>` interfaces beside the interface whose Java
  default methods they carry (§3 Java names rule 10). A Java nested type leaves its outer class only where C# cannot keep Java's name
  inside it (CS0102: a member of the outer type has that name), and then sits beside it, in the same
  namespace: `GroupMembershipOperation` (Java `CloseOptions.GroupMembershipOperation`, ledger 109),
  `ConfigResourceType` (Java `ConfigResource.Type`, M15/P3 D16), `AlterConfigOpType` (Java
  `AlterConfigOp.OpType`), `UpgradeType` (Java `FeatureUpdate.UpgradeType`, beside Java's
  `upgradeType()` method, ledger 368).
- **A .NET-only public type** lives in the namespace and folder of the Java-backed type it pairs
  with: an async peer with its sync type (`AsyncKafkaProducer`, `IAsyncProducer` and
  `IAsyncCallback` in `Producer/`; every `Async*Result`, `AsyncKafkaAdminClient`,
  `AsyncMockAdminClient` and `IAsyncAdmin` in `Admin/`; `AsyncKafkaFuture` in `Common/`), the types
  carrying Java's default methods with their interface (§3 Java names rule 10; today the base class
  `ConsumerRebalanceListenerBase`), the
  shared consumer surface with `IConsumer` (`IConsumerCommon`), the Rust-only `ConsumerHandle` with
  the consumer (the core's `consumer::ConsumerHandle`), and a C#-only enum with the type that takes
  it (`ClientQuotaMatchType` with `ClientQuotaFilterComponent` in `Common/Quota/`). *(Python keeps an
  async class in its sync class's module.)*
- **Errors** follow the same rule. Each Java exception class the C enum `kafka_common_Error_e`
  names, and each of Java's abstract bases above them, becomes a class of the same name with
  Java's parent, generated by `rust/xtask/src/error_hierarchy.rs` as Python's are
  (`KafkaException` and `RecordDeserializationException` stay hand-written), in the namespace of
  its Java package: `common.errors` → `Confluent.Kafka.Common.Errors`
  (`SerializationException`, `RecordDeserializationException`), `common` → `Confluent.Kafka.Common`
  (`KafkaException`, `InvalidRecordException`), `clients.consumer` → `Confluent.Kafka.Consumer`
  (`CommitFailedException`), `clients.producer` → `Confluent.Kafka.Producer`
  (`BufferExhaustedException`), `common.config` → `Confluent.Kafka.Common.Config`
  (`ConfigException`), `common.requests` → `Confluent.Kafka.Common.Requests`
  (`CorrelationIdMismatchException`). Two Java classes of one name stay apart in their two
  namespaces, as Java's do: `Confluent.Kafka.Consumer.OffsetOutOfRangeException` and
  `Confluent.Kafka.Common.Errors.OffsetOutOfRangeException`, and likewise `InvalidOffsetException`;
  code that needs both qualifies one. The Java built-in exceptions get Java-named classes too (§3,
  the `KafkaException` hierarchy row), in the root `Confluent.Kafka` namespace, as Python keeps
  them at `confluent_kafka`: each one's package (`java.lang`, `java.util`, `java.util.concurrent`)
  maps to the root itself. Each derives from the .NET built-in that stands in for Java's chain:

  | JDK class | .NET parent |
  |---|---|
  | `IllegalArgumentException` | `ArgumentException` |
  | `IllegalStateException` | `InvalidOperationException` |
  | `IndexOutOfBoundsException` | `ArgumentOutOfRangeException` |
  | `UnsupportedOperationException` | `NotSupportedException` |
  | `ConcurrentModificationException` | `InvalidOperationException` |
  | `NoSuchElementException` | `InvalidOperationException` |
  | `ExecutionException` | `Exception` |
  | `TimeoutException` | `System.TimeoutException` |

  Inside any `Confluent.Kafka.*` namespace a simple name finds a type of the enclosing
  `Confluent.Kafka` before any `using`, so a bare `TimeoutException` there silently means the root
  class, never `System`'s (in `Confluent.Kafka.Common.Errors` it means Kafka's own). Library code
  therefore names every `TimeoutException` in full: `Confluent.Kafka.TimeoutException`,
  `Common.Errors.TimeoutException` or `global::System.TimeoutException`;
  `TimeoutExceptionNameTests` fails on a bare one in `src/Confluent.Kafka`. User code outside
  `Confluent.Kafka` that imports both `System` and `Confluent.Kafka` gets CS0104 for
  `TimeoutException`, and qualifies or aliases it
  (`using FutureTimeoutException = Confluent.Kafka.TimeoutException;`,
  `using KafkaTimeoutException = Confluent.Kafka.Common.Errors.TimeoutException;`).
- **Names that are also namespaces.** A type may share its name with a namespace, as Java's class
  and package do (`Confluent.Kafka.Common.Header.Header`). Inside any `Confluent.Kafka.Common.*`
  namespace the simple name `Header` means the namespace `Confluent.Kafka.Common.Header` (CS0118),
  whatever the `using` lines say; write `Header.Header` there. The same holds for any later type
  named like a namespace segment under `Common` (`Record`, `Config`). CA1724 stays at its default,
  off.
- **`using` lines.** A file imports each namespace it uses with one `using` line outside its
  namespace (IDE0065), in `dotnet format`'s order; there is no `global using`. Code in a
  `Confluent.Kafka.*` namespace (the tests, the gRPC server, soak, perf) imports them like user code
  does; only the eight JDK exception classes are reachable through the enclosing `Confluent.Kafka`
  namespace (above).

- **public folders** (`src/Confluent.Kafka/Producer/`, `Consumer/`, `Admin/`, `Common/` and its
  subfolders) — **all public API**, whatever the C# kind: the client types *and* the supporting
  value types, enums, interfaces and exceptions, each in its Java package's folder and namespace
  (above). If a user can name it, it lives here.
- `Internal/` — **internal** managed scaffolding
  (`Confluent.Kafka.Internal`): the completion bridge (`CompletionThread`, one drain
  thread per client that has one, ffi §0.3; `KafkaFutureImpl` and `Completion`; the
  `_cb` → `TaskCompletionSource` adapters, ffi §A7/§B7), the client configuration's type check and
  text (§4 **Config**).
- `Internal/Interop/` — the **P/Invoke boundary**
  (`Confluent.Kafka.Internal.Interop`): the `NativeMethods`
  `[DllImport]` class (`NativeMethods.cs` — the name CA1060 requires),
  `SafeHandle`s, `Utf8Marshal` helpers, callback delegates, and the blittable
  `[StructLayout]` structs the ABI passes by value (`KafkaBytes`, the ABI's `kafka_Bytes_t`); a
  record (`ProducerRecord_t`) is an opaque handle, not mirrored. `unsafe` lives only here; 1:1
  with `ffi-marshalling.md`.
- `tests/Confluent.Kafka.UnitTests/` — `Mock*` unit tests
  (`MockProducer`; `MockConsumer` as the consumer lands), a top-level sibling of
  `src/` in its own project directory **outside** the library tree — so default
  SDK compile globbing never pulls test files into the library assembly (no
  compile-scoping hack needed); `InternalsVisibleTo` grants access to internals.
- `tests/Confluent.Kafka.IntegrationTests/` — the binding's broker-backed tests, as Python's
  `test/integration`: a Testcontainers broker from the perf suite's `KafkaBrokerFixture.cs`
  (linked, not referenced), net8.0 and net10.0, in the solution. Without Docker the tests skip.
  Testcontainers and `Xunit.SkippableFact` are test-only packages here and in the perf suite,
  never in `src/` or `Confluent.Kafka.UnitTests` (ledger 213).

---

## 3 · The target: the .NET API shape

Mirror the **Java** client in idiomatic C#. The producer surface is **generic-only**
(M11/P5 — Java's single `Producer<K,V>`, no bytes sibling; the non-generic bytes types
were **removed**, bytes users write `<byte[], byte[]>` + `Serdes.ByteArray()`, exactly as
the consumer M6/P1b):

```csharp
namespace Confluent.Kafka.Producer;   // §2: Java's org.apache.kafka.clients.producer; KafkaException
                                      // and AsyncKafkaFuture are in Confluent.Kafka.Common

public sealed class ProducerRecord<TKey, TValue> {   // Java `ProducerRecord<K,V>`; getters are Java's methods
    public string Topic();
    public int? Partition();                     // null = let the producer choose
    public long? Timestamp();                    // null = producer stamps it
    public TKey? Key();                          // null (ref) = no key; three-state via the serializer (§4)
    public TValue? Value();                      // null (ref) = tombstone; three-state via the serializer (§4)
    public Headers Headers();                    // the record's own copy; read-only once a KafkaProducer sends it
    public ProducerRecord(string topic, TValue? value,
        TKey? key = default, int? partition = null, long? timestamp = null,
        IEnumerable<Header>? headers = null);   // Java's checks, topic → timestamp → partition
    // Equals, GetHashCode and ToString are Java's (topic, partition, headers, key, value, timestamp)
}

public sealed class RecordMetadata {            // NON-generic (no K/V on the ack); getters are Java's methods
    public const int UNKNOWN_PARTITION = -1;
    public RecordMetadata(TopicPartition? topicPartition, long baseOffset, int batchIndex, long timestamp,
        int serializedKeySize, int serializedValueSize);   // Java's; offset = baseOffset + batchIndex unless -1
    public bool HasOffset();
    public long Offset();
    public bool HasTimestamp();
    public long Timestamp();
    public int SerializedKeySize();
    public int SerializedValueSize();
    public string Topic();
    public int Partition();
}

public class KafkaException : Exception {        // Java's; the base of the generated Kafka classes (§2, ffi §A5)
    internal int Code { get; }                   // the class's C id, -1 for a bare KafkaException; no IsRetriable:
}                                                // the class is the category (`catch (RetriableException)`)

public interface ICallback {              // Java `Callback` (producer); M14/P1 — see §4
    // SYNC `void`, like Java's onCompletion. `metadata` is NON-nullable (Java's user callback never
    // sees null — the -1 placeholder is substituted on failure); `exception` null == success.
    // `exception` is the class of the error's id (ffi §A5), or the same instance a user callback
    // or a serializer (its `ApiException`, below) threw.
    void OnCompletion(RecordMetadata metadata, Exception? exception);
}

public interface IAsyncCallback {          // Java `Callback`, async form (M19/P1)
    // On IAsyncProducer only. Called on the pump thread; the pump does not wait for the Task.
    // Every later completion of the producer waits until this Task ends; the record's future completes then.
    Task OnCompletion(RecordMetadata metadata, Exception? exception);
}

public interface IAsyncProducer<TKey, TValue> : IAsyncDisposable, IDisposable {   // Java `Producer<K,V>`
    // method names mirror Java — no `Async` suffix; the interface carries the async distinction
    // outer = the handover (Java's send blocking; completes at the core's handover cb, never parks a thread); inner = the Future —
    // AsyncKafkaFuture<RecordMetadata>, a readonly struct whose Get() is the delivery Task (M11/P3.6)
    ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default);
    // Java's SECOND send signature — send(record, Callback) STILL returns the Future, so the callback
    // is an ADDITIONAL parameter, not an alternative (M11/P8 D-6 shape: declared identically on both
    // producer interfaces, no shared base). Fires on the producer's completion thread, before the record's future completes (§4);
    // AsyncKafkaProducer runs it inside Send, on the caller's thread, for a serializer's ApiException (§3).
    ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, ICallback callback,
        CancellationToken cancellationToken = default);
    // Java has no async callback; Python's async producer takes an `async def` one (M19/P1).
    ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<TKey, TValue> record, IAsyncCallback callback,
        CancellationToken cancellationToken = default);
    Task Flush(CancellationToken cancellationToken = default);
    Task Close(CancellationToken cancellationToken = default);
    Task Close(TimeSpan timeout, CancellationToken cancellationToken = default);   // Java close(Duration) (M19/P1)
    Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default);
    IReadOnlyDictionary<MetricName, IMetric> Metrics();   // Java metrics() — NON-blocking, so it stays SYNC (M11/P8)
    // Java's five transaction methods (§4 Producer transactions); each async form waits on its _cb twin
    Task InitTransactions(CancellationToken cancellationToken = default);
    Task BeginTransaction(CancellationToken cancellationToken = default);   // a Task, as Python's async begin_transaction
    Task SendOffsetsToTransaction(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        ConsumerGroupMetadata groupMetadata, CancellationToken cancellationToken = default);
    Task CommitTransaction(CancellationToken cancellationToken = default);
    Task AbortTransaction(CancellationToken cancellationToken = default);
}

public readonly struct AsyncKafkaFuture<T> {   // Java java.util.concurrent.Future<T> (producer send) — M11/P3.6
    public Task<T> Get();   // + Java's other KafkaFuture members (§4 Futures); default(...) → InvalidOperationException
}

// The sync `IProducer<TKey,TValue>` (sync mirror) is **shipped** (M11/P4), generic-only (M11/P5):
// KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey,TValue>) returns once the record is registered (the blocking `send_with_callback`; it blocks only while
// the core waits for metadata or for buffer.memory, ≤ max.block.ms); future.Get() = Java send(record).get() (M11/P4.2);
// KafkaFuture<RecordMetadata> Send(ProducerRecord<TKey,TValue>, ICallback) — Java's second send
// signature, the callback fired on the producer's completion thread (`confluent-kafka-producer-completion`) before Get() returns
// (KafkaProducer: inside Send, on the caller's thread, for a serializer's ApiException, §3); void Flush() /
// Close(); void Close(TimeSpan) (M19/P1); IReadOnlyList<PartitionInfo> PartitionsFor(string);
// IReadOnlyDictionary<MetricName, IMetric> Metrics() (M11/P8); void InitTransactions() / BeginTransaction() /
// SendOffsetsToTransaction(offsets, groupMetadata) / CommitTransaction() / AbortTransaction(), each waiting on its
// _cb twin (§4 Producer transactions). Real + mock: KafkaProducer / MockProducer.
// `Metrics()` and the callback-taking `Send` are declared IDENTICALLY on both producer interfaces —
// there is deliberately **no** `IProducerCommon` for them (M11/P8 decision D-6; the asymmetry with the
// consumer's `IConsumerCommon` is recorded rather than typed away). Both are **public-API additions**
// (breaking for an external implementer of either interface) — acceptable pre-publish.

// Real clients take the serializers — Java KafkaProducer(Map, Serializer<K>, Serializer<V>).
public sealed class AsyncKafkaProducer<TKey, TValue> : IAsyncProducer<TKey, TValue> {
    public AsyncKafkaProducer(IReadOnlyDictionary<string, object?> configs,
        ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer);   // typed values, §4 Config
}

public sealed class AsyncMockProducer<TKey, TValue> : IAsyncProducer<TKey, TValue> {   // Java `MockProducer<K,V>`
    // Mock TAKES the serializers — Java-faithful (MockProducer.java takes Serializer<K>/Serializer<V>
    // and serializes records into its history); UNLIKE the consumer, where mock-takes-deserializers is
    // a deviation. Send serializes K/V → bytes exactly like the real client (§4).
    public AsyncMockProducer(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer,
        bool autoComplete = true);
    public bool CompleteNext();
    public bool ErrorNext(Exception e);   // Java errorNext(RuntimeException); the callback sees this instance, Get an ExecutionException around it
    public int HistoryCount();                  // .NET-only: history() needs typed records, the ABI's hold bytes
    public void Clear();
    // Java's transaction controls (§4 Producer transactions); MockProducer has the same members
    public void SetInitTransactionException(Exception? exception);          // Java's field is a RuntimeException; null clears
    public void SetBeginTransactionException(Exception? exception);
    public void SetSendOffsetsToTransactionException(Exception? exception);
    public void SetCommitTransactionException(Exception? exception);
    public void SetAbortTransactionException(Exception? exception);
    public void SetSendException(Exception? exception);                      // Java's public fields sendException, …: a setter each (allowlisted)
    public void SetFlushException(Exception? exception);                     // also fails CommitTransaction / AbortTransaction
    public void SetPartitionsForException(Exception? exception);
    public void SetCloseException(Exception? exception);                     // Close throws it and leaves the producer open
    public void FenceProducer();
    public bool Closed();                   // Java closed(), read from the binding's state
    public bool TransactionInitialized();   // Java transactionInitialized(), and likewise below
    public bool TransactionInFlight();
    public bool TransactionCommitted();
    public bool TransactionAborted();
    public bool Flushed();
    public bool SentOffsets();
    public long CommitCount();
    public IReadOnlyList<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>>
        ConsumerGroupOffsetsHistory();                                       // METHOD: a fresh copy per call
    public IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> UncommittedOffsets();
}
```

**Clipped to today's ABI.** The sketch is the *producer* surface the current ABI
can back — it omits Java members the ABI doesn't expose yet (`metrics()` is **no
longer** among them: it shipped in M11/P8 as a Mode-A port over the already-present
`kafka_producer_Producer_metrics` + `kafka_producer_MetricMap_*` family; and
`send(record, Callback)` now goes through the ABI's per-record `Callback_t`, ffi §A6, decision 10;
`ProducerRecord.headers()` shipped in M21/P2): `MockProducer.history()` and `MockProducer.uncommittedRecords()`.
Java returns the records as they were sent, typed `ProducerRecord<K, V>`. The ABI returns the records too
(`kafka_producer_MockProducer_history` / `_uncommitted_records`, a `kafka_List_t` of `ProducerRecord_t *`), but
serialized: their key and value are bytes, and the mock takes serializers, not deserializers, so the binding
cannot give back `TKey` / `TValue`. Python's mock is pure Python and keeps the typed records itself. The
.NET-only `HistoryCount()` reads the size of that list. Add both when the binding can give back typed records.

**The typed serialize path (M11/P5).** `Send` serializes `TKey`/`TValue` → bytes in
the binding, **above** the bytes-based native producer and **before** the P/Invoke
(no serializer callback through the ABI, `CLAUDE.md §11`; the per-record `Callback_t` is the
delivery callback, not a serde, ffi §A6); the serialized bytes and the record's sealed headers flow into the record that
`ProducerRecord_with_options` builds through the OptionsBuilder, for every record, as Python's
binding builds it (ledger 300; an internal `SerializedProducerRecord` carrier). Three decisions:

  - **Invoke-on-null (Java-faithful).** The serializer is **always invoked**, even for
    a `null` `TKey`/`TValue` (Java `KafkaProducer.doSend` calls `serialize(topic, …,
    record.key())` unconditionally); its `ReadOnlyMemory<byte>?` return drives the
    wire sentinel — `null` → absent (no key / tombstone), empty → present-empty,
    non-empty → present.
    This is the **intentional inverse** of the consumer's not-invoked short-circuit
    (which is *forced* — you cannot hand a deserializer a null span; the producer is
    not forced, so short-circuiting would diverge from Java).
  - **A serializer's throw, as Java's `doSend`, on `KafkaProducer` / `AsyncKafkaProducer`.**
    Serialize runs pre-native on the caller thread for **both** the sync and async `Send`. Only an `InvalidCastException` (Java's
    `ClassCastException`) is wrapped, in a `SerializationException` "Can't convert key of
    class <key type> to class <serializer type> specified as the key serializer" (the value
    likewise; a null key prints `null`), thrown **synchronously** (the async `Send` throws
    before returning the `Task`). A serializer's `ApiException` is not thrown: as Java's
    `doSend` and the core's `handle_api_error` do for the callback and the future, the
    callback runs once, inside `Send` on the caller's thread, with placeholder metadata (offset
    -1) and the exception, before the failed future is returned (on `IAsyncProducer` the first
    stage completes); nothing crosses the ABI. The core never sees the error, so the binding
    cannot run Java's `maybeTransitionToErrorState`: a serializer that throws one of Java's
    fatal producer classes (`ProducerFencedException`, …) does not make the producer fatal, as
    in Python. On a transactional producer that leaves the transaction open where Java's
    producer would refuse every later call; fixing it needs a core hook that moves the producer
    into an error state *(recorded deviation, as Python's)*. Any other exception
    is rethrown unchanged. *(Python's `send()` raises every serializer error.)* The mocks
    rethrow every serializer exception unchanged, with no callback and no history entry, as
    Java's `MockProducer` does (it calls its serializers with no `try`), and as the core's and
    Python's mocks do. Contrast the consumer's deserialize wrap, which is Java's
    `RecordDeserializationException`.
  - **Mock-takes-serializers is Java-faithful.** Java's `MockProducer` takes
    `Serializer<K>/Serializer<V>` and serializes into its history, so the mock ctors
    take serializers too (`(keySer, valueSer, autoComplete=true)`) — **unlike** the
    consumer, where mock-takes-deserializers was a deviation.

**Consumer** follows the identical pattern — the Java surface in C# idiom. Its C
ABI has **landed**, so it's a **Mode A** build (§6.2); the receive-path key/value
ownership decision is §6.4. The surface is **generic-only** (M6/P1b — Java's single
`Consumer<K,V>`, no bytes sibling; bytes users write `<byte[], byte[]>` +
`Serdes.ByteArray()`), clipped to today's ABI:

```csharp
public sealed class ConsumerRecord<TKey, TValue> {  // Java `ConsumerRecord<K,V>`; getters are Java's methods
    public string Topic();
    public int Partition();
    public long Offset();
    public long Timestamp();
    public TimestampType TimestampType();
    public TKey Key();           // deserialized (span → T); default(TKey) if absent (deser NOT called — decision C)
    public TValue Value();       // deserialized; default(TValue) if absent (a tombstone; deser NOT called)
    public Headers Headers();    // the record's own, writable copies (not routed through a deserializer this phase)
}

public sealed class ConsumerRecords<TKey, TValue>          // Java `ConsumerRecords<K,V>`
    : IReadOnlyCollection<ConsumerRecord<TKey, TValue>> {
    public int Count();                                    // Java's count(); IReadOnlyCollection<T>.Count is explicit
}

public interface IConsumerCommon {               // shared sync surface (IAsyncConsumer + IConsumer)
    void Wakeup();                                // interrupt a blocked poll — one-shot (idiom map)
    ConsumerGroupMetadata GroupMetadata();        // no group.id → InvalidGroupIdException (24), Java's message (ledger 105)

    // non-blocking / instantaneous in Java → stays sync (idiom map; consumer-threading §1).
    // Java's methods, as every Java getter is: each returns a fresh owned snapshot per call.
    // Java returns a Set; IReadOnlySet post-dates netstandard2.0 → collection.
    IReadOnlyCollection<TopicPartition> Assignment();
    IReadOnlyCollection<string> Subscription();
    IReadOnlyCollection<TopicPartition> Paused();
    void EnforceRebalance(string? reason = null); // KIP-848 logged no-op → returns success

    // CommitAsync, Seek and CurrentLag live on each flavor's interface (decisions 13 and 22,
    // ledger 103): the async flavor returns a Task.

    // Metrics (M9/P2) — Python parity, a SHIPPED sync state read (§4). No ClientId(): Java's
    // clientId() is package-private and Python has no client_id() (ledger 102).
    IReadOnlyDictionary<MetricName, IMetric> Metrics();                        // Java Map<MetricName, ? extends Metric> metrics()

    // In-callback reentrancy (M9/P8). Java: the captured `consumer` variable inside a
    // callback — its acquire() is reentrant on the polling thread, the C ABI's guard is not.
    // On a KafkaConsumer the handle's operations work only inside a callback (ledger 104),
    // where the consumer's own calls route to it (ledger 114).
    ConsumerHandle Handle();
}

// Generic-only (M6/P1b): only Poll retypes; every other member is K/V-free and inherited from the
// non-generic IConsumerCommon or restated unchanged. Bytes users write IAsyncConsumer<byte[], byte[]>.
public interface IAsyncConsumer<TKey, TValue> : IConsumerCommon, IAsyncDisposable, IDisposable {   // Java `Consumer<K,V>`
    // blocking-in-Java / callback-at-ABI → async (§4); method names mirror Java — no `Async` suffix
    Task<ConsumerRecords<TKey, TValue>> Poll(TimeSpan timeout, CancellationToken cancellationToken = default);
    Task Subscribe(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default);
    Task Subscribe(IReadOnlyCollection<string> topics, IConsumerRebalanceListener callback,   // Java subscribe(Collection, callback) — M9/P6
        CancellationToken cancellationToken = default);
    Task Subscribe(IReadOnlyCollection<string> topics, IAsyncConsumerRebalanceListener callback, CancellationToken cancellationToken = default);   // Java subscribe(Collection, callback), async listener — M19/P1
    Task Unsubscribe(CancellationToken cancellationToken = default);
    // Java commitSync / commitSync(Map) (+ Duration overloads) — blocks in Java → Task over commit_sync*_cb (§4 note)
    Task Commit(CancellationToken cancellationToken = default);
    Task Commit(TimeSpan timeout, CancellationToken cancellationToken = default);
    Task Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        CancellationToken cancellationToken = default);
    Task Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, TimeSpan timeout,
        CancellationToken cancellationToken = default);
    // Java commitAsync() / (cb) / (Map, cb) → Task over commit_async*_cb: completes when the core has accepted
    // the commit; the commit's outcome still reaches the callback (decision 13)
    Task CommitAsync(CancellationToken cancellationToken = default);
    Task CommitAsync(IOffsetCommitCallback callback, CancellationToken cancellationToken = default);
    Task CommitAsync(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IOffsetCommitCallback? callback = null, CancellationToken cancellationToken = default);
    // Java commitAsync(callback) / commitAsync(Map, callback) with an async callback: Java has none;
    // Python's async consumer takes an `async def` one (M19/P1). Not on IConsumer or ConsumerHandle.
    Task CommitAsync(IAsyncOffsetCommitCallback callback, CancellationToken cancellationToken = default);
    Task CommitAsync(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IAsyncOffsetCommitCallback callback, CancellationToken cancellationToken = default);
    Task<long> Position(TopicPartition partition, CancellationToken cancellationToken = default);
    Task<long> Position(TopicPartition partition, TimeSpan timeout, CancellationToken cancellationToken = default);
    // + a TimeSpan overload of Committed, PartitionsFor, ListTopics, OffsetsForTimes, BeginningOffsets and
    //   EndOffsets, over the *_with_timeout_cb forms (decision 22); every TimeSpan: negative → IllegalArgumentException
    // Java seek — blocks in Java → Task over seek_*_cb (decision 22)
    Task Seek(TopicPartition partition, long offset, CancellationToken cancellationToken = default);
    Task Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata,
        CancellationToken cancellationToken = default);
    // Java currentLag — blocks in Java → Task over current_lag_cb; empty → null (ledger 103). Not wakeable:
    //   a cancel sends no Wakeup(); the Task ends with OperationCanceledException once the lag call has ended
    Task<long?> CurrentLag(TopicPartition topicPartition, CancellationToken cancellationToken = default);
    Task Close(CancellationToken cancellationToken = default);                        // Java close()
    Task Close(CloseOptions options, CancellationToken cancellationToken = default);  // Java close(CloseOptions), close_with_options_cb

    // Assignment() / Subscription() / Paused() / EnforceRebalance() / Wakeup() /
    // GroupMetadata() / Metrics() / Handle() live on IConsumerCommon (non-blocking → stays sync)
}

public sealed class AsyncKafkaConsumer<TKey, TValue> : IAsyncConsumer<TKey, TValue> {   // Java `KafkaConsumer` (KIP-848)
    // 3-param ctor (decision A) — Java KafkaConsumer(Map, Deserializer<K>, Deserializer<V>), KafkaConsumer.java:601
    public AsyncKafkaConsumer(IReadOnlyDictionary<string, object?> configs,
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer);   // typed values, §4 Config
}

public sealed class AsyncMockConsumer<TKey, TValue> : IAsyncConsumer<TKey, TValue> {    // Java `MockConsumer`
    // Deviation (§7): the ctor TAKES the deserializers (Java's mock doesn't) — its Poll decodes native
    // bytes like the real consumer. AddRecord stays BYTES-in (Java's is typed-in) — tests the deserialize
    // path in isolation + forced by the bytes-only core ABI. Mock helpers are inherent, not on IAsyncConsumer.
    public AsyncMockConsumer(IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer,
        string? autoOffsetReset = null);
    public void AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value);
    public Task Rebalance(IReadOnlyCollection<TopicPartition> newAssignment);    // Java MockConsumer.rebalance, rebalance_cb (ledger 48)
    public Task LosePartitions(IReadOnlyCollection<TopicPartition> partitionsLost);  // lose_partitions_cb (ledger 48)
    public void SetPollException(KafkaException? exception);                     // Java setPollException (decision 15); null clears
}

// The sync `IConsumer` (blocking mirror of `IAsyncConsumer`) is **shipped** — the most
// Java-faithful surface (Java's `Consumer` is synchronous), a sibling of the async trio over the
// SAME native consumer (not a wrapper). Generic-only (M6/P1b), no `CancellationToken` (interruption is
// `Wakeup()` only), with `Close()` and `Close(CloseOptions)`. Each sync method calls the blocking C
// ABI form directly (block_on inside the Rust core's runtime — NOT sync-over-async, §4); each first
// drains __execute_callbacks once, and listener and commit callbacks run inline on the caller
// (decision 27; ffi §B1). Grown in two sub-phases: the core loop (M5/P8a) + the query family
// (`Committed` / `OffsetsForTimes` / `BeginningOffsets` / `EndOffsets` / `PartitionsFor` /
// `ListTopics`, added additively in M5/P8b).
// KafkaConsumer<K,V> / MockConsumer<K,V> mirror the async pair's 3-param / deserializer-taking ctors.
public interface IConsumer<TKey, TValue> : IConsumerCommon, IDisposable {   // Java `Consumer<K,V>` (synchronous)
    ConsumerRecords<TKey, TValue> Poll(TimeSpan timeout);      // blocks; Wakeup() interrupts (one-shot)
    void Subscribe(IReadOnlyCollection<string> topics);
    void Subscribe(IReadOnlyCollection<string> topics, IConsumerRebalanceListener callback); // M9/P6
    void Unsubscribe();
    void Assign(IReadOnlyCollection<TopicPartition> partitions);
    void Pause(IReadOnlyCollection<TopicPartition> partitions);
    void Resume(IReadOnlyCollection<TopicPartition> partitions);
    void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions);
    void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions);
    long Position(TopicPartition partition);
    long Position(TopicPartition partition, TimeSpan timeout);              // Java position(tp, Duration)
    void Commit();                                                         // Java commitSync (confirming)
    void Commit(TimeSpan timeout);                                         // Java commitSync(Duration)
    void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets);
    void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, TimeSpan timeout);
    // Java commitAsync — returns once the commit is started; the outcome reaches the callback (decision 13)
    void CommitAsync();
    void CommitAsync(IOffsetCommitCallback callback);
    void CommitAsync(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IOffsetCommitCallback? callback = null);                           // null callback = Java's commitAsync(Map, null)
    void Seek(TopicPartition partition, long offset);                      // Java seek(tp, long) — blocking form
    void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);
    long? CurrentLag(TopicPartition topicPartition);                       // Java currentLag(tp) — blocking form; empty → null (ledger 103)
    // ---- query family (M5/P8b) — blocks; returns the owned result directly; each has a TimeSpan overload (decision 22) ----
    IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Committed(IReadOnlyCollection<TopicPartition> partitions);
    IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Committed(IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout);
    IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> OffsetsForTimes(IReadOnlyDictionary<TopicPartition, long> timestampsToSearch);
    IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> OffsetsForTimes(IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, TimeSpan timeout);
    IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(IReadOnlyCollection<TopicPartition> partitions);
    IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout);
    IReadOnlyDictionary<TopicPartition, long> EndOffsets(IReadOnlyCollection<TopicPartition> partitions);
    IReadOnlyDictionary<TopicPartition, long> EndOffsets(IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout);
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic);
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic, TimeSpan timeout);
    IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics();
    IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics(TimeSpan timeout);
    // every TimeSpan overload: negative → IllegalArgumentException, Zero valid (ffi §B5)
    void Close();                                                          // Java close()
    void Close(CloseOptions options);                                      // Java close(CloseOptions), close_with_options
    // Wakeup() / Assignment() / Subscription() / Paused() / GroupMetadata() / EnforceRebalance() /
    // Metrics() / Handle() come from IConsumerCommon.
}

public sealed class KafkaConsumer<TKey, TValue> : IConsumer<TKey, TValue> {   // Java `KafkaConsumer` (KIP-848), synchronous
    public KafkaConsumer(IReadOnlyDictionary<string, object?> configs,
        IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer);  // 3-param (decision A); typed values, §4 Config
}

public sealed class MockConsumer<TKey, TValue> : IConsumer<TKey, TValue> {    // Java `MockConsumer`, synchronous
    public MockConsumer(IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer,
        string? autoOffsetReset = null);   // deviation §7: takes deserializers (Java's mock doesn't)
    public void AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value); // bytes-in (§7); inherent
    public void Rebalance(IReadOnlyCollection<TopicPartition> newAssignment);  // Java MockConsumer.rebalance — inherent (M9/P6)
    public void LosePartitions(IReadOnlyCollection<TopicPartition> partitionsLost);  // Java MockConsumer.losePartitions — inherent
    public void SetPollException(KafkaException? exception);                     // Java setPollException (decision 15); null clears
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
`OffsetAndMetadata` constructor), plus `Seek(tp, long)` /
`Seek(tp, OffsetAndMetadata)` + `CurrentLag` (M5/P7; since M19/P1 on each flavor's
interface, a `Task` on `IAsyncConsumer`: decision 22, ledger 103). The **typed generic
`Consumer<K,V>`** is now **shipped** (M6/P1b — generic-only conversion + the zero-copy
typed poll; see §4), plus the **rebalance listener** — `IConsumerRebalanceListener`
(+ `ConsumerRebalanceListenerBase`), the `Subscribe(topics, callback)` overload on
both interfaces, and `MockConsumer`'s inherent `Rebalance(newAssignment)` driver and
`AsyncMockConsumer`'s awaitable one (M9/P6; see the §4 listener row), and its async form
`IAsyncConsumerRebalanceListener` (+ `AsyncConsumerRebalanceListenerBase`) on
`IAsyncConsumer` (M19/P1), plus the **offset-commit
callback** — `IOffsetCommitCallback` and the two callback-taking `CommitAsync` overloads,
on each flavor's interface since decision 13 (M9/P7; see the §4 commit-callback row), and its
async form `IAsyncOffsetCommitCallback` on `IAsyncConsumer` (M19/P1), plus the **in-callback
reentrancy handle** — the public `ConsumerHandle` and `ConsumerHandle Handle()` on
`IConsumerCommon` (M9/P8; see the §4 reentrancy row). That closes the **consumer's**
callback-parity work: a listener or commit callback can now call back into its own
consumer, which is what Java gets for free. (The **producer's** half — Java's
`send(record, Callback)` — closed later, in M14/P1; see the §4 delivery-callback
divergence. This sentence used to read "the callback-parity work" unqualified, which
was true of the consumer only.) Still to come:
pattern subscribe and headers for the deserializers (row 153; each `ConsumerRecord`'s `Headers` are its own, writable
copies today, ledger 293). ⚠ The typed **producer** is **no longer** deferred — it shipped in
M11/P5 (generic-only, see the producer sketch above); this line said otherwise until
M14/P1 corrected it, and the producer completion model (§A7) turned out not to gate it.

The **admin client** shipped in M15 as `IAdmin` (`KafkaAdminClient` /
`MockAdminClient`), a Mode A port over the `kafka_admin_*` ABI; its decisions are in
`design/current/STATUS.md`. Since M15/P14 it ships in two flavours over one internal core:
sync `IAdmin` (`KafkaAdminClient` / `MockAdminClient`; Results hold `KafkaFuture` /
`KafkaFuture<T>`) and async `IAsyncAdmin` (`AsyncKafkaAdminClient` / `AsyncMockAdminClient`;
`Async*Result` types hold `AsyncKafkaFuture` / `AsyncKafkaFuture<T>`). Since M19/P1 that core is
on the uniform C FFI: each future is a `KafkaFutureImpl` over an eager `get_cb`
(`ffi-marshalling.md` §C1). `MockAdminClient` and `AsyncMockAdminClient` have Java's two
constructors, `()` and `(brokers, controller)`, and Java's nested `Builder`, over
`kafka_admin_MockAdminClient_Builder_*` (ledger 106). Their `Close` does nothing, as Java's mock
(decision 21).

**Serdes** — the (de)serialization foundation (M6/P1a), the Java
`Serializer<T>` / `Deserializer<T>` / `Serde<T>` shape in C# over the bytes-only ABI
(a pure managed, binding-/user-layer concern, §4). `IDeserializer<T>` is **consumed by
P1b's typed consumers**; `ISerializer<T>` is **now consumed by the shipped typed producer (M11/P5)**:

```csharp
public interface ISerializer<T> {
    ReadOnlyMemory<byte>? Serialize(string topic, T data); // Java Serializer<T>.serialize returns byte[]; see below
}
public interface IDeserializer<T> {
    T Deserialize(string topic, ReadOnlySpan<byte> data); // Java Deserializer<T>.deserialize — sync, span (see below)
}
public interface ISerde<T> : ISerializer<T>, IDeserializer<T> { }   // Java Serde<T> — what Serdes returns

public static class Serdes {     // Java `Serdes` factory, Java's method names — Java wire-format parity
    public static ISerde<string> String();         // UTF-8
    public static ISerde<byte[]> ByteArray();      // identity (deserialize copies the span out)
    public static ISerde<int>    Integer();        // 4 bytes big-endian (IntegerSerializer)
    public static ISerde<long>   Long();           // 8 bytes big-endian (LongSerializer)
    public static ISerde<double> Double();         // 8 bytes big-endian doubleToLongBits (DoubleSerializer)
    public static ISerde<Guid>   UUID();           // ⚠ UUID.toString() -> UTF-8, NOT 16 raw bytes (UUIDSerializer)
    public static ISerde<object?> Void();          // VoidSerializer (serialize null; deserialize default)
}                                // each returns one shared, stateless instance (Java returns a new one per call)

public class SerializationException : KafkaException { }  // generated (§4 Error granularity); serdes throw it on malformed input
```

Deliberate deviations, recorded (§4 decision-point latitude; consumer-threading §28 style):

- **`IDeserializer<T>` is sync + `ReadOnlySpan<byte>`**, not Java's `byte[]` — the §6.4 / §27
  zero-copy lock. A `byte[]` param forces a per-record copy; the `ref struct` span borrows the
  native fetch slice in place and provably can't outlive the batch (can't be stored/boxed/
  awaited/sent). Sync because a span can't cross an `await` and serde is CPU-bound — also
  Java-faithful (`Deserializer<T>` is sync).
- **Headers overload deferred** (Java's `default T deserialize(String, Headers, byte[])`) —
  addable later non-breakingly by §3 Java names rule 10: an extension method forwarding to the
  header-less form, and an interface that replaces it (its name, for an overloaded default, is not
  decided yet).
- **Async serde deferred** — a note only, no async interface; a Schema-Registry path *may* later
  want one (decided then). Java's SR serdes are themselves sync.
- **`Serialize` returns `ReadOnlyMemory<byte>?`**, not Java's `byte[]` (M23/P1, ledger 400).
  Python's serializer may return any buffer; .NET's form of that is `ReadOnlyMemory<byte>`,
  which can be a whole array, a slice of one, or native memory behind a `MemoryManager<byte>`,
  and the producer sends it with no .NET-side copy (ffi §A4). `null` is a null key or value (a
  tombstone for the value); an empty memory is empty bytes. A `byte[]` converts by itself, but a
  null `byte[]`, and `cond ? null : array`, convert to an **empty** memory (ledger 401): the
  binding's serializers return `null` in its own `return` or as `(ReadOnlyMemory<byte>?)null`.
  The producer reads the memory until `Send` returns or throws, or, on the async producers,
  until the `ValueTask` it returns has completed (ledger 73, 402).
- **`ISerde<T>` added** = Java's `Serde<T>` (what `Serdes.String()` returns); composes the two
  shipped directional interfaces so a `Serdes` member offers both directions from one type. Java's
  `Serde<T>` uses `serializer()`/`deserializer()` accessors + `Closeable`; ours extends both
  directly (stateless serdes, nothing to close).

**The Java → C# idiom map** — the binding's spine. Each row: the Java construct,
its C# realization, and where the enforcing rule lives.

| Java | C# idiom | Rule / detail |
|---|---|---|
| `Future<RecordMetadata>` | `Task<RecordMetadata>`; on `IAsyncProducer.Send` wrapped as `AsyncKafkaFuture<RecordMetadata>` (a struct over `KafkaFutureImpl` with Java's `KafkaFuture` members, whose `Get()` returns a `Task`; a deliberate deviation so the two stages are distinguishable — M11/P3.6); on `IProducer.Send` it is `KafkaFuture<RecordMetadata>` (a struct over `KafkaFutureImpl` with Java's `KafkaFuture` members, whose blocking `Get()` is Java's `get()`; the name pairs with `AsyncKafkaFuture` — Java's `org.apache.kafka.common.KafkaFuture` is the Admin type, mapped to `Task<T>` — M11/P4.2). Since M15/P14 Java's Admin `KafkaFuture<T>` maps to `KafkaFuture<T>` in the sync admin Results and to `AsyncKafkaFuture<T>` in the async ones, with non-generic `KafkaFuture` / `AsyncKafkaFuture` for Java's `KafkaFuture<Void>`. This supersedes the 'mapped to `Task<T>`' note (M11/P4.2 D10). Which one a method returns: §4 Sync vs async, the M15/P14 a/b rule. | a `KafkaFutureImpl` (decision 4a) completed on the client's drain thread — producer ffi §A7, consumer ffi §B7; admin: one per native future, completed by its eager `get_cb` delivery on the admin drain thread, ffi §C1 (decisions 5, 8) |
| `send` — **blocks** *and* returns `Future<RecordMetadata>` | `ValueTask<AsyncKafkaFuture<RecordMetadata>>` on `IAsyncProducer`; the outer stage is the block (it completes at the core's handover `cb`; no thread parked), the inner `AsyncKafkaFuture` is the `Future` (`await future.Get()` = Java's `future.get()`); on `IProducer` it is `KafkaFuture<RecordMetadata>` — `Send` is the block (the blocking `send_with_callback`, ≤ `max.block.ms`), `future.Get()` is the `Future` (M11/P4.2) | M11/P3.5, M11/P3.6, M11/P4.2, ffi §A1 |
| **blocks** in Java, **or** returns `Future<T>`, **or** takes a completion callback — any one is enough (producer `send`/`flush`/`close`/`partitionsFor`; consumer `poll`/`commitSync`/`position`/`subscribe`/`assign`/`pause`/`resume`/`unsubscribe`) | `Task`/`Task<T>` on the **async** interface (`IAsyncProducer`/`IAsyncConsumer`) + `CancellationToken`; method name **mirrors Java** (no `Async` suffix) | the three async triggers — §4 **Sync vs async**; best-effort cancel ffi §A7/§B7. `seek` and `currentLag` block in Java too: a `Task` on `IAsyncConsumer` over `seek_*_cb` / `current_lag_cb`, a blocking member on `IConsumer` (decision 22, ledger 103) |
| `close()` / `AutoCloseable` | `IAsyncDisposable.DisposeAsync()` (+ `IDisposable`) | graceful close, then join the client's drain thread, wait for the in-flight uses, and destroy on the closing thread — §4 **Close/Dispose**; ffi §A7, §B2 |
| `KafkaException` hierarchy | Every core error is the class of its id: one generated class per Java exception class, with Java's parent (§2), `KafkaException` the root of the Kafka ones. There is no `IsRetriable` and no `IsFatal`: the class is the category (`catch (RetriableException)`), as in Java and Python. `Code` is internal, the class's C id (§7.6). The Java built-ins map to Java-named classes in the root `Confluent.Kafka` namespace (§2), each derived from the .NET built-in that stands in for Java's chain: `IllegalStateException`, `ConcurrentModificationException`, `NoSuchElementException` : `InvalidOperationException`; `IllegalArgumentException` : `ArgumentException`; `UnsupportedOperationException` : `NotSupportedException`; `TimeoutException` : `System.TimeoutException`; `ExecutionException` : `Exception`; `IndexOutOfBoundsException` : `ArgumentOutOfRangeException`. Java's `NullPointerException` has no class, as in Python (its `TypeError`): the core's -32 (`LocalNullPointerError`, which it returns for a `NULL` mandatory pointer) is an `ArgumentNullException`, with the core's message and no `ParamName`. `CorrelationIdMismatchException` extends `IllegalStateException`, as Java's does. A managed exception that went through the core as a `local_callback` error comes back as the same instance. | ffi §A5 |
| `IllegalArgumentException` / `IllegalStateException` | The Java-named classes, with Java's message, for the binding's own checks too, so a check and the core's error for one condition have one class. A closed client throws `IllegalStateException` with its Java message: "Cannot perform operation after producer has been closed", "This consumer has already been closed.", "Cannot accept new calls when AdminClient is closing." The admin client throws it at the call *(deviation, as Python's: Java's `KafkaAdminClient` returns the Result and fails its futures with it; the native handle is gone after `Close`, so the call has no Result to return; ffi §C1)*. A .NET-only precondition (Java throws `NullPointerException` or nothing) keeps `ArgumentNullException` / `ArgumentOutOfRangeException`, except a negative timeout on a consumer lookup Java does not check (`Committed`, `PartitionsFor`, `ListTopics`, `OffsetsForTimes`, `BeginningOffsets`, `EndOffsets`): it throws `IllegalArgumentException` with the core's "Timeout must not be negative", the error the core's `_with_timeout` forms return (ffi §B5). A disposed `ConsumerHandle` keeps `ObjectDisposedException`. | validate **before** the FFI call — ffi §A5 |
| `wakeup()` (interrupt a blocked `poll`/`commit`) | sync `Wakeup()`; the in-flight `Poll`/`Commit` throws `WakeupException` (**one-shot**), as Java's | ffi §B5 |
| `ConcurrentModificationException` (consumer is one-op-in-flight) | `ConcurrentModificationException`, sync and async, with Java's `acquire()` message, from the binding's owner check and the core's -2 alike (decision 17). | ffi §B5 |
| `ConsumerRebalanceListener` | `IConsumerRebalanceListener` — **sync `void`** methods, on both consumers; on `IAsyncConsumer` also `IAsyncConsumerRebalanceListener` — `Task` methods (M19/P1). `ConsumerRebalanceListenerBase` and `AsyncConsumerRebalanceListenerBase` carry Java's `onPartitionsLost` default. Registered by the `Subscribe(topics, callback)` overloads | Each method reports through `__set_callback_result`; the rebalance waits for the report. See the §4 **rebalance-listener divergence**; ffi §B6, consumer-threading §31 |
| `OffsetCommitCallback` | `IOffsetCommitCallback` — **sync `void`** `OnComplete(offsets, exception)` (`exception` is `Exception?`, the class of the error's id); passed to the `CommitAsync(callback)` / `CommitAsync(offsets, callback)` overloads (M9/P7 — **shipped**); on `IAsyncConsumer` also `IAsyncOffsetCommitCallback` — `Task OnComplete(offsets, exception)`, passed to two more `CommitAsync` overloads (M19/P1) | On `IConsumer` it runs inline on the caller's thread, inside the blocking call that delivers it (`Poll`, `Commit`, `Close`, …); on `IAsyncConsumer`, on the consumer's drain thread (`confluent-kafka-consumer-pump`) (decision 27). `offsets` is `null` on a failed commit (decision 22). An exception it throws, or the async form's faulted or cancelled `Task`, propagates out of the consumer call that delivered it (decision 12, ledger 47). See the §4 **commit-callback divergence**; ffi §B6, consumer-threading §31 |
| `Callback` (producer, `send(record, Callback)`) | `ICallback` — **sync `void`** `OnCompletion(metadata, exception)`; passed to the second `Send(record, callback)` overload on **both** producer interfaces, which still returns the `KafkaFuture<RecordMetadata>` / `ValueTask<AsyncKafkaFuture<RecordMetadata>>` (M14/P1 — **shipped**); on `IAsyncProducer` also `IAsyncCallback` — `Task OnCompletion(metadata, exception)`, passed to a third `Send(record, callback)` overload (M19/P1) | ⚠ **not the caller's task** — one ABI `Callback_t` per record (ffi §A6, decision 10). It fires on the producer's completion thread, on both surfaces, **before** the record's future completes (decision 36), with **non-null** `-1` placeholder metadata on failure; the one exception: for a serializer's `ApiException`, `KafkaProducer` / `AsyncKafkaProducer` run it once inside `Send`, on the caller's thread, with the placeholder, before the failed future is returned, and nothing crosses the ABI (§3); `exception` is `Exception?` (ledger 33). See the §4 **delivery-callback divergence**; ffi §A6/§A7 |
| **non-blocking** in Java — a pure local read, or an action with no completion signal (`assignment()`, `subscription()`, `paused()`, `groupMetadata()`, `wakeup()`, mock helpers) | **stays sync** — a plain **method**, a getter included, with Java's name. Except `beginTransaction()`: the core drains the queued sends first, so it waits, and `IAsyncProducer` makes it a `Task`, as Python's async classes make it `async def` | only 8 consumer members qualify — §4 **Sync vs async**, `consumer-threading.md §1` |
| method `send`, `flush`, `poll` | PascalCase, **mirror Java** — no `Async` suffix (`Send`, `Poll`); the async distinction is carried by the interface (`IAsyncProducer`/`IAsyncConsumer` async; `IProducer`/`IConsumer` the deferred sync mirror), matching `dotnet/.claude/rules/bindings.md §2.2` + the Python sibling | §4 |
| a `public static final` field or an enum constant (`UNKNOWN_PARTITION`, `ResourceType.ANY`) | Java's own name and spelling, as a C# field: a `const` for a primitive or a string (`RecordMetadata.UNKNOWN_PARTITION`), a `static readonly` field otherwise (`Uuid.ZERO_UUID`, `Uuid.RESERVED`, `KafkaPrincipal.ANONYMOUS`, the filters' `ANY`, the mock admin clients' `DEFAULT_LOG_DIRS`), never a property; an enum member keeps the constant's name (`ResourceType.ANY`); numeric values unchanged. A .NET-only type with no Java constant behind it keeps .NET casing (`ClientQuotaMatchType`). A C# enum's `ToString()` prints the Java constant; where Java's enum overrides `toString()` (`GroupState`, `ClassicGroupState`, `GroupType`, `TransactionState`, `TimestampType`, `IsolationLevel`) the text the binding prints or sends is Java's, from an internal map, and the enum's own `ToString()` is a recorded deviation | ledger 285 |
| a Java method, getters included (`topic()`, `isDone()`, `getName()`, `hasOffset()`) | a C# **method** with the PascalCase of Java's exact name, `get`, `is`, `has` and `should` kept (`Topic()`, `IsDone()`, `GetName()`, `RecordMetadata.HasOffset()`, `Uuid.GetMostSignificantBits()`), never a property; a member whose .NET name drifted from Java takes Java's (`TopicPartitionInfo.Isr()`, `IMetric.MetricName()` / `MetricValue()`, `ClientQuotaFilterComponent.Match()`). A Java static method is a C# static method: `Serdes.String()`, `ByteArray()`, `Integer()`, `Long()`, `Double()`, `UUID()`, `Void()`, each returning one shared stateless instance. Where a method is named like a type (`UpgradeType UpgradeType()`), an expression inside the class that reaches the type's members qualifies the type (`Admin.UpgradeType.UPGRADE` in `FeatureUpdate`, `Resource.ResourceType.ANY` in `ResourcePattern` and `ResourcePatternFilter`), since C# binds the simple name to the method group there; a type position needs no qualification (`TopicPartition TopicPartition()`) | ledger 365 |
| a getter and a setter of one name (`timeoutMs()`, `timeoutMs(Integer)`) | two overloads: `TimeoutMs()` reads, `TimeoutMs(int? timeoutMs)` sets and returns what Java's setter returns (the class itself for Java's `this` and for `AbstractOptions`' `T`, since .NET has no `AbstractOptions`; `void` for `RemoveMembersFromConsumerGroupOptions.Reason(string)` and `KafkaPrincipal.TokenAuthenticated(bool)`). An Options class is built by Java's chain, `new CreateTopicsOptions().TimeoutMs(5000).ValidateOnly(true)`; there is no object initializer and no keyword constructor | ledger 366; ffi §C1 (Options) |
| a setter Java names apart from its getter (`ListGroupsOptions.inGroupStates`, `ListTransactionsOptions.filterStates`) | Java's setter name (`InGroupStates(..)`, `WithProtocolTypes(..)`, `WithTypes(..)`, `FilterStates(..)`, `FilterProducerIds(..)`, `FilterOnDuration(..)`, `FilterOnTransactionalIdPattern(..)`) beside Java's getter (`GroupStates()`, `FilteredStates()`); no setter where Java has none (`ListOffsetsOptions` takes its `IsolationLevel` through Java's constructor) | ledger 367 |
| a member .NET forces to be a property | none: a property a .NET interface requires is implemented explicitly beside Java's method (`ConsumerRecords.Count()`, Java's `count()`, and an explicit `IReadOnlyCollection<T>.Count`); the inherited `Exception` ones stay; `PublicJavaMemberShapeTests` and `GeneratedErrorClassesTests` fail on any public property. A name C# forces apart is moved, not renamed: `FeatureUpdate.UpgradeType` (Java's nested enum) sits beside the class as `Confluent.Kafka.Admin.UpgradeType` (CS0102 with Java's `upgradeType()`), as `GroupMembershipOperation` does (ledger 109) | ledger 368 |
| a .NET-only getter or property | none, except where Java's type has no C# form: `ClientQuotaFilterComponent.MatchType()` tells Java's three `match()` states (`ofEntity`, `ofDefaultEntity`, `ofEntityType`) apart, since C# has no `Optional` that can itself be `null` *(deviation)* | ledger 369 |
| `byte[]` key/value | `ReadOnlyMemory<byte>` | send: pinned zero-copy — ffi §A4; receive: copy-out (default), keep-alive deferred — ffi §B4 / §6.4 |
| `Headers` / `Header` (record headers) | `Headers : IEnumerable<Header>` with Java's `Add` / `Remove` / `LastHeader` / `ToArray` and `GetHeaders(key)` for Java's `headers(String)` (CS0542); read-only once a `KafkaProducer` sends the record (`IllegalStateException("RecordHeaders has been closed.")`, Java's class), never on the mocks; a fetched record's are its own and writable | send: the header values are copied by the ABI, which has no borrowed form, a recorded deviation from root `CLAUDE.md` §14 (ledger 301) — ffi §A4 |
| opaque handle | `SafeHandle` (owned) / `IntPtr` (transient) | ffi §A2/§B2 |
| `String` topic / config | UTF-8, hand-marshalled | ffi §A3/§B3 |
| `Map<String, Object>` / `Properties` (a client's configuration) | one `IReadOnlyDictionary<string, object?> configs` per `Kafka*` constructor; a value of the C# type Java's `parseType` accepts for the key, or Java's text; a `null` meets the key's validator or is not set | §4 **Config**; ffi §0.1, §A5 |
| `Duration` (timeouts: `poll`/`close`/`committed`) | `TimeSpan` | ABI takes `int64_t` ms |
| `Map` / `Set` / `List` (returns) | `IReadOnlyDictionary` / `IReadOnlyCollection` / `IReadOnlyList` | `IReadOnlySet` post-dates netstandard2.0 → `IReadOnlyCollection` |
| a Java array `T[]` (`PartitionInfo.replicas()`, `Headers.toArray()`) | a C# array of the mapped element type (`Node[]`, `Header[]`), as Java's | ledger 370 (proposed) |
| a Java primitive parameter (`DescribeFeaturesOptions.nodeId(int)`) | the C# primitive (`int`), never `int?`; an `OptionalInt` getter is `int?` | ledger 181 (amended) |
| a Java getter that maps an unset field to a sentinel (`NewTopic.numPartitions()`, `orElse(-1)`) | the same sentinel (`-1`); only `Equals` and `ToString` tell unset from the sentinel, as Java's | ledger 371 (proposed) |
| `Producer<K,V>` / `Consumer<K,V>` (generic) | **both shipped generic-only** — `Consumer<K,V>` (M6/P1b) and now `Producer<K,V>` (M11/P5): the producer trios + `ProducerRecord` are `<TKey,TValue>`, the non-generic ones removed (bytes = `<byte[],byte[]>` + `Serdes.ByteArray()`); `RecordMetadata` stays non-generic. Serialize above the bytes core, invoke-on-null, synchronous `SerializationException` wrap (§4) | §4 |

**Call-site checks that differ from Java (recorded).** Where the binding checks an argument or
a state itself, it throws Java's class, at Java's point, with Java's message. These do not, and
each goes when the reason it gives goes:

- **Strings.** An unpaired surrogate is encoded as `?` (`0x3F`), as Java's
  `String.getBytes(UTF_8)` does, by `StringSerde` and by every string the binding marshals to the
  C ABI (topics, header keys, group ids, admin strings); a result whose key the core names shows
  that `?`. A NUL character in a string argument throws `ArgumentException` at the call, because a
  C string cannot carry it; Java sends it and the broker rejects the name. Goes when the C ABI
  takes length-delimited strings.
- **`NewTopic`** with a partition count or a replication factor below -1 throws
  `ArgumentOutOfRangeException` at construction; Java's future fails with
  `InvalidPartitionsException` / `InvalidReplicationFactorException`. The C ABI reads any negative
  as unset, so passing the value on would create the topic with the broker's defaults. Goes when
  the C ABI carries the value.
- **Admin null names.** An admin RPC with a null topic name or group id inside an argument throws
  at the call, as Python's does; Java fails that key's future with `InvalidTopicException` /
  `InvalidGroupIdException`. The C ABI cannot carry the null. Two lookups on a Result answer as
  Java: `AlterConsumerGroupOffsetsResult.PartitionResult` of a null-topic partition returns a
  future already failed with Java's `IllegalArgumentException` *(deviation: Java fails it only
  after the call, and with the call's error if the call failed; the Result cannot see that)*, and
  `DeleteConsumerGroupOffsetsResult.PartitionResult` throws it at the call, as Java's.
- **After close.** The producer's `Flush`, `PartitionsFor` and `Metrics`, the consumer's
  `Metrics`, and `MockProducer`'s and `AsyncMockProducer`'s `PartitionsFor`, `Metrics`,
  `CompleteNext`, `ErrorNext`, `Clear`, `HistoryCount`, `SetSendException`, `SetFlushException`,
  `SetPartitionsForException`, the five `Set*TransactionException` setters
  and the transaction state readers (`TransactionInitialized` to `UncommittedOffsets`) throw the
  closed client's `IllegalStateException`, because the native handle is gone; Java answers (Java's
  mock checks for close only in `send`, `flush`, `fenceProducer` and the transaction methods). Goes
  when the native object outlives `Close`.
- **The core admin mock.** `MockAdminClient.Builder` checks that live only in the core mock
  (`controller(int)` out of range, `numBrokers` below the log-dir count, `Build()` with no
  brokers, `AddTopic` with a leader without log dirs) and `SetFetchesRemainingUntilVisible` on an
  unknown topic throw the core's `IllegalArgumentException`; Java's throw `IndexOutOfBoundsException`
  and `RuntimeException`. Goes when the core mock throws Java's. `CreateDelegationToken` with no
  renewer throws Java's `IndexOutOfBoundsException` "Index: 0, Size: 0" at the call, as Java's mock
  reads `renewers().get(0)`.
- **Consumer deserializers.** The config takes no classes, so the consumer has no
  `key.deserializer` / `value.deserializer` route: a null deserializer throws Java's
  `ConfigException` "Missing required configuration "key.deserializer" which has no default
  value." (the value likewise), on all four consumers, the mocks included *(Java's `MockConsumer`
  takes no deserializers)*.
- **Negative partitions.** The consumer leaves a negative partition to the core, which answers as
  Java does: `seek`, `pause`, `resume` and the real consumers' `currentLag` throw
  `IllegalStateException` "No current assignment for partition t--1"; `position` throws "You can
  only check the position for partitions assigned to this consumer.", an `IllegalStateException`
  on the real consumers and an `IllegalArgumentException` on the mocks; the mocks' `currentLag`
  returns 0; on the mocks `beginningOffsets` / `endOffsets` throw `IllegalStateException` "The
  partition t--1 does not have a beginning offset." / "… an end offset." and `offsetsForTimes`
  `UnsupportedOperationException` "Not implemented yet."; `assign` and `commit` accept it. Nothing
  is recorded.
- **A null config value** follows Java per key: a key whose Java validator rejects null throws
  `ConfigException` with that validator's message (`Internal/ClientConfigs.cs`, from the generated
  `Internal/ConfigTypes.g.cs`, §4 **Config**); any other null value is not set, as in
  Python *(deviation: with several null keys .NET reports the first in the dictionary's order,
  Java the first in `ConfigDef` order)*.

**Do NOT build:** the ecosystem `confluent-kafka-dotnet` shape (`ProduceAsync`,
delivery-report handlers, `Message<K,V>`, `value.serializer` kwargs). Target the
**Java** client, per `dotnet/.claude/rules/bindings.md §2`.

⚠ **"delivery-report handlers" above is about ckd's shape, not about Java's
`Callback` (M14/P1).** The shipped `ICallback` +
`Send(record, ICallback)` is the **Java** signature
(`Producer.java:86` — the callback is an additional parameter and the overload
still returns the `Future`), so it is on the target side of this line, not the
forbidden side. What stays out is ckd's *term of art*: a `DeliveryReport<K,V>`
**result object** handed to a handler, plus the `Action<DeliveryReport<K,V>>`
`Produce(...)` fire-and-forget shape that has no `Future`/`Task` at all. Neither
is being added. If a future phase is tempted to rename toward ckd's vocabulary,
that is what this bullet forbids.

### Java names (CKRDA0013)

`analyzers/Confluent.Kafka.Analyzers/JavaNameAnalyzer.cs` checks every public type and member of
`Confluent.Kafka` against Java's, read from `analyzers/java-api.txt`, which
`cargo xtask java-api ../dotnet/analyzers/java-api.txt` writes from `kafka/` (`--check` fails when it is
out of date). The rule:

1. A type has Java's simple name. An interface adds `I`, an async peer `Async` (`IAsyncProducer`,
   `AsyncKafkaProducer`). A Java nested type C# cannot nest under its name (CS0102) sits beside its outer
   type with its bare name.
2. A method is the PascalCase of Java's exact name, `get`, `is`, `has` and `should` kept; never a property.
3. A getter and a setter of one name are overloads.
4. A Java static method keeps its name and stays a static method, a static factory (a static method that
   builds an object of its type) included: never a constructor, `Parse` or a property.
5. Fields and constants keep Java's spelling (ledger 285).
6. Parameters keep Java's names, the interface's declaration first. In an exception's constructor a
   `Throwable` is `innerException`, and a `String` Java names `s` or `msg` is `message`. A Java `long` and
   the `TimeUnit` that qualifies it are one `TimeSpan`.
7. .NET's own forms: `ToString`, `Equals` (with `==` and `!=`), `GetHashCode`; `CompareTo`;
   `GetEnumerator` for `Iterable`; `Dispose` / `DisposeAsync` for `Closeable`; `Message` /
   `InnerException`; `IEquatable<T>.Equals`; a C# enum covers `name`, `ordinal`, `values` and `valueOf`. A
   member that overrides or implements a `System` member keeps its parameter names. A method may end with an
   optional `CancellationToken cancellationToken`.
8. Java's constructor and method overloads, each in Java's parameter order; a `Properties` overload is its
   `Map` overload, and an overload taking a `java.util.regex.Pattern` is not translated. A Java abstract
   class's constructors are `protected`; a struct or sealed class has none.
9. A Java enum's methods sit in a top-level static class named after the enum plus `Methods`, beside the enum
   (`ScramMechanismMethods`), as C# 14 extension members. Java's static methods go in an
   `extension(ScramMechanism)` block, so a C# 14 caller writes `ScramMechanism.FromMechanismName(…)` and an
   older one `ScramMechanismMethods.FromMechanismName(…)`. Its instance methods go in an
   `extension(ScramMechanism mechanism)` block or are `this` extension methods: `mechanism.MechanismName()` on
   every C# version.
10. A Java interface's `default` method is not declared on the .NET interface, which cannot hold a body on
    netstandard2.0. It is an extension method in a top-level static class named after the interface without
    its `I` plus `Extensions`, beside it (`ConsumerRebalanceListenerExtensions`). An interface
    `I<Interface>With<Method>` (the method's name without a leading `On`) extends the interface and declares
    only that method (`IConsumerRebalanceListenerWithPartitionsLost : IConsumerRebalanceListener`). The
    extension method calls the object's own method when it implements that interface, and runs Java's default
    otherwise; a user replaces the default by implementing that interface instead of the main one. Java's
    `Admin` defaults that only convert their arguments stay members of `IAdmin` and `IAsyncAdmin`, whose
    clients are their only implementers (allowlisted). A Java interface's static method goes in an
    `extension(IAdmin)` block of a static class with the Java interface's name: `Admin.Create(…)`, and on C#
    14 `IAdmin.Create(…)`. How the extra interface of an overloaded default is named (the serializers'
    headers overloads) is not decided yet.

A difference is fixed or listed, in `analyzers/`: `java-name-allowlist.txt` (kept on purpose, each group
under a comment saying why), `java-name-pairs.txt` (a renamed type and the Java type it translates, so its
members are compared) or `java-name-baseline.txt` (gaps still to fix). The lists only shrink: a line that
matches nothing is CKRDA0014, and `cargo xtask dotnet-java-name-baseline` rewrites the baseline from a
build, refusing a finding it does not hold unless `--add` is given. The analyzer and the lists are not in the
package.

---

## 4 · Design decisions

Defaults + rationale; the implementing agent takes the default unless the feature
argues otherwise, and records any deviation in the phase PLAN or COMMENTS.DONE, and,
where it is needed, in one concise code comment (§4 Comments).

| Decision | Default | Why / when |
|---|---|---|
| **Namespace / package id** | **`Confluent.Kafka`** — the assembly and package id (same identity as ckd, which this client is meant to replace), and the root of the namespaces: each public type is in its Java package's namespace below it (§2), none in `Confluent.Kafka` itself. ⚠ **Strong gate — revisit before publishing:** a shared id means a project can hold ckd 2.x **or** this client, never both, so ckd's Schema-Registry / OAuthBearer packages can't be mixed in. Decide then: own SR integration, or diverge the id. | before any public type |
| **Disposal** | Both `IAsyncDisposable.DisposeAsync()` (primary; closes, joins the client's drain thread and destroys on the closing thread, §4 **Close/Dispose**, without blocking a thread) and `IDisposable.Dispose()` (blocking fallback). `close(Duration)` → `Close(TimeSpan)` where Java keeps it (no `Async` suffix — mirrors Java). The consumer's `close(Duration)` is deprecated in Java and not translated: the consumer has `Close()` and `Close(CloseOptions)` over `close_with_options[_cb]` (decision 22). The producer has it (M19/P1, ledger 82): `void Close(TimeSpan)` on `IProducer` and `Task Close(TimeSpan, CancellationToken)` on `IAsyncProducer`, over `kafka_producer_Producer_close_with_timeout_cb`. The timeout bounds the drain of the queued sends and the close; the delivery reports are then awaited without a bound, as Java's `close(Duration)` joins its I/O thread. A negative timeout throws `IllegalArgumentException` with Java's message, "The timeout cannot be negative.", on `KafkaProducer` and `AsyncKafkaProducer`, before the closed check, as in Java; the mocks close as for zero, as Java's mock never reads the timeout. `TimeSpan.Zero` is valid. Admin (M15/P14): `IAsyncAdmin : IAsyncDisposable, IDisposable` follows this default; the sync `IAdmin : IDisposable` only (as `IProducer` / `IConsumer`), with a blocking `void Close()` and `void Close(TimeSpan)`. `IAsyncAdmin` has `Task Close(CancellationToken)` and `Task Close(TimeSpan, CancellationToken)` (ledger 175). `Close()` and `Dispose` call `kafka_admin_Admin_close_cb`, and `Close(TimeSpan)` calls `kafka_admin_Admin_close_with_timeout_cb`. The sync flavour waits on a `Completion` (decision 4a), as Python's sync admin does. The async flavour completes its `Task` from the `cb`, and its token ends only the caller's wait. A negative timeout throws `IllegalArgumentException` with Java's message before the closed check (ledgers 82, 174). The mocks' `Close` does nothing and reads no timeout, as Java's mock (decision 21, ledger 174); `Dispose` still frees the native mock. | first client type |
| **Cancellation** | `CancellationToken` on every async method, honored best-effort. **Producer:** the `Send` token never cancels the record. Already cancelled at the call: `Send` throws `OperationCanceledException` and nothing is sent. If it fires while a queued record waits, the first stage (the `ValueTask` that hands back the future) still waits for the hand-over, up to `max.block.ms`, then ends with `OperationCanceledException` carrying the token, whatever the outcome, as Python's `send` does. A record the producer took is still sent, and its delivery callback fires once; a record `Close` fails runs none. After the hand-over, and for a record taken or refused inside the call, the token does nothing. `Get(token)` ends only that wait, and `Cancel(bool)` on a send's future returns `false` (Java's `FutureRecordMetadata`). CKD's token cancels the one Task it returns. **Consumer:** maps to `wakeup()` → the in-flight op cancels/faults (ffi §B7). A host-idiom addition Java lacks (allowed by `dotnet/.claude/rules/bindings.md §2`). | first async method |
| **Sync vs async** | Decide **per method from the Java implementation** (`AsyncKafkaConsumer` / `KafkaProducer`) — never from the Javadoc, the interface, or the method name. Three triggers make it async; everything else stays sync. See the **Sync vs async** note below. | every public method |
| **Async naming** | Method names **mirror Java** — **no** `Async` suffix (`Send`, `Poll`, `Commit`). The sync/async distinction is carried by the **interface/class**, not the method name (`IAsyncProducer`/`IAsyncConsumer` async; `IProducer`/`IConsumer` the deferred sync mirror), matching `dotnet/.claude/rules/bindings.md §2.2` + the Python sibling. `Task`-returning methods still return `Task`; the name just drops the suffix. | first async method |
| **Interface naming** | Async interfaces `IAsyncProducer` / `IAsyncConsumer`; the sync mirror is `IProducer` / `IConsumer` — **`IConsumer` is shipped (M5/P8a)**, `IProducer` still deferred. C#'s `I`-prefix is the lexical marker for an interface (Framework Design Guidelines; analyzer CA1715 warns without it); the sync/async split is carried by the **interface + type** (`IAsyncConsumer`/`AsyncKafkaConsumer` async, `IConsumer`/`KafkaConsumer` sync), **no `Async` suffix on methods** (they mirror Java). Each has a real + mock impl (`KafkaProducer`/`MockProducer`, `AsyncKafkaConsumer`/`AsyncMockConsumer`, `KafkaConsumer`/`MockConsumer`). Deviation: strict-Java bare `Producer`/`Consumer` (fights CA1715 / dev expectation). | first interface type |
| **Key/value type** | `ReadOnlyMemory<byte>` both ways. **Producer (send):** zero-copy — pins the user buffer via `MemoryHandle` (ffi §A4). **Consumer (receive):** wraps an owned copied array (copy-out, §6.4), not a pin. `byte[]`-only is an acceptable interim. | porting `ProducerRecord` / `ConsumerRecord` |
| **Serializers** | **Foundation shipped (M6/P1a)** — the bidirectional serde surface `ISerializer<T>` (`ReadOnlyMemory<byte>? Serialize(topic, T)`, M23/P1) + `IDeserializer<T>` (`T Deserialize(topic, ReadOnlySpan<byte>)`, sync + zero-copy over the batch, ffi §B4) + `ISerde<T>` (both, the Java `Serde<T>` shape returned by the `Serdes` factory), with the built-in `Serdes` (`String()`, `ByteArray()`, `Integer()`, `Long()`, `Double()`, `UUID()`, `Void()`, Java's names — Java wire-format parity) and `SerializationException`. `IDeserializer<T>` is **now consumed by the shipped typed consumers (M6/P1b)** — the zero-copy typed poll deserializes each record's key/value from a `ReadOnlySpan<byte>` over the native batch (no intermediate per-record `byte[]`), applying the **null→`default(T)` three-state** model (absent → `default(T)`, deserializer NOT called; present-empty → 0-length span; present → the span) and a **mandatory `SerializationException` wrap** of any deserializer throw (a `RecordDeserializationException : SerializationException` with Java's fields and accessors, carrying the inner exception: decisions 11 and 22. `Poll` returns the records before the bad one and throws only when there are none, after seeking the failing partition back to the bad record and later partitions to their first record not returned, decision 11. The deserialize runs on the caller's thread, or in the async `Poll`'s continuation, never on the drain thread or inside a native callback). `ISerializer<T>` is **now consumed by the shipped typed producer (M11/P5)** — `Send` serializes each record's key/value to bytes **above** the bytes-based native producer (before the P/Invoke), always invoking the serializer even on a `null` `TKey`/`TValue` (Java-faithful — the `ReadOnlyMemory<byte>?` return drives the absent/present-empty/present sentinel, the intentional inverse of the consumer's not-invoked short-circuit), and handling a serializer's throw as Java's `doSend` on `KafkaProducer` / `AsyncKafkaProducer` (§3; the mocks rethrow every serializer exception, as Java's `MockProducer`): an `InvalidCastException` is wrapped in a **`SerializationException` thrown synchronously** for both the sync and async `Send` (serialize is pre-native on the caller thread), an `ApiException` goes to the callback and a failed future, and any other exception is rethrown unchanged. No serializer callback through the ABI (`CLAUDE.md §11`); the per-record `Callback_t` is the delivery callback, not a serde (ffi §A6). See §3 for the sketch + the deliberate deviations. | ✅ M6/P1a (foundation) · ✅ M6/P1b (typed consumers) · ✅ M11/P5 (typed producer) |
| **Config** | One `IReadOnlyDictionary<string, object?> configs` on each `Kafka*` client constructor: Java's `Map<String, Object>`, with no `Properties` form, as Python merges it. Keys are **Java dotted names** (`bootstrap.servers` required). A value is a C# value of a type Java's `ConfigDef.parseType` accepts for the key's type (`bool`; `int` for INT, `short` for SHORT, `int` or `long` for LONG; any number but `nint` / `nuint` for DOUBLE, `BigInteger` included; an `IList` for LIST; a `System.Type` for CLASS) or the string Java would parse. The binding checks the type against the key's Java `ConfigDef` type (`Internal/ConfigTypes.g.cs`, generated from `kafka/` by `cargo xtask generate-error-codes`) and sends a text the core parses to the value Java's `parseType` gives (integers and `decimal` as digits, a `double` as "G17"; the core rounds); a wrong type, a `null` the key's validator refuses or a `null` key throws `ConfigException` (code −10) with Java's message, before any P/Invoke. Any other `null` leaves the key out, so the core's default applies. The value itself (ranges, valid strings, number text) is the core's to check, and the core's errors are `ConfigException` too; unknown keys reach the core, which logs them as unused. The five own-role serializer keys (`key.serializer`, `value.serializer`, `serializer.encoding`, `key.serializer.encoding`, `value.serializer.encoding`; consumer: the deserializer ones) are neither checked nor sent, as in Python. *(Deviation from the one law: the type check and the text are Kafka logic, here because the C ABI takes every value as a C string and only the binding sees the object's type, as root `CLAUDE.md`'s Python conventions record for Python (Implementation over the FFI, exception 2); and a `null` the validator accepts is "not set" where Java holds `null` (ledger 272). Goes when: the C ABI takes typed config values.)* | wiring the constructor (M22/P1) |
| **Error granularity** | One class per Java exception class, with Java's parent and namespace (§2, §3's `KafkaException` hierarchy row, ffi §A5), generated as Python's are; this replaces decision 14's flat `KafkaException` (`dotnet/design/history/M22/P1-error-hierarchy-parity/PLAN.md`). Two stay hand-written: `KafkaException` and **`RecordDeserializationException : SerializationException`** (M19/P1, decision 22), thrown by `Poll` for a key or value that fails to deserialize when no record precedes it (decision 11). **`SerializationException : KafkaException`**, which the built-in serdes throw on malformed input, is generated. **`ConfigException : KafkaException`** (Java's `common.config` class, −10) is generated too: the client constructors throw it for a configuration value the binding refuses, and it is the type of the core's −10 (§4 **Config**). | a user classifies by class, as in Java and Python |
| **Interceptors** | Defer; reserve the Java-shaped name. | a concrete need |
| **Nullable reference types** | `#nullable enable` project-wide; annotate the P/Invoke surface precisely. | project setup |

### Sync vs async — the governing rule

Decide from the **Java implementation** (`AsyncKafkaConsumer` / `KafkaProducer`),
never from the Javadoc, the interface, or the method name. Under KIP-848 the
consumer is an event loop: the app thread enqueues an event and *waits for the
background thread to apply it*, so `subscribe`, `assign`, `seek`,
`seekToBeginning`/`seekToEnd`, `pause`, `resume`, `unsubscribe` and `currentLag`
all **block** despite reading as instantaneous (classic-consumer intuition does
not transfer). **If you cannot check, assume it blocks.** (Since M19/P1 `seek` and
`currentLag` follow this rule too: decision 22, ledger 103.)

| Java signal | C# |
|---|---|
| **Blocks** — `addAndGet` · `processBackgroundEvents` · `getResult` · `result.await` · `waitOnMetadata` | `Task`/`Task<T>` on the async interface, `CancellationToken`; name mirrors Java (no `Async` suffix) |
| **Returns `Future<T>`** — even if it barely blocks (`send`) | `Task<T>` on the async interface; name mirrors Java (no `Async` suffix). Exception: the producer's `send` yields `AsyncKafkaFuture<RecordMetadata>` (a struct over the `Task<T>`) from its admission `ValueTask` — M11/P3.6, §3 idiom map. The sync `IProducer.Send` returns `KafkaFuture<RecordMetadata>` (a blocking `Get()`) — M11/P4.2. Admin (M15/P14): every RPC returns its Result at once, as in Java; the Result holds one `KafkaFuture<T>` per key on `IAdmin` and one `AsyncKafkaFuture<T>` per key on `IAsyncAdmin`. Only `Close` waits. |
| **Takes a completion callback** — even if non-blocking (`send(record, Callback)`, `commitAsync(OffsetCommitCallback)`) | `Task`/`Task<T>` on the async interface. The `Task` **replaces** the callback **only when the two carry the same information, at the same point, with the same arity**; where they do not, the callback-taking overload is kept **in addition** to the `Task`. **The test — does the callback deliver payload, ordering, or a signature the `Task` cannot?** If yes, keep both and record the reasoning in the phase PLAN (a concise comment at the site only where it is needed, §4 Comments). Two carve-outs are in force: **(1) `commitAsync(OffsetCommitCallback)`** — the callback delivers the **offsets the commit applied to**, which a `Task` returning `void` cannot express, and `commitAsync` is one half of a Java sync/async **pair** whose other half (`commitSync`) already owns the `Task` mapping (`Commit`, M5/P6) → see the §4 **commit-callback divergence** (M9/P7). **(2) `send(record, Callback)`** — Java's **second** `send` overload *still returns the `Future`* (`Producer.java:86`), so the callback is an **additional** parameter, not an alternative; dropping it removes one of Java's two `send` signatures outright. It also fires at a point no `Task` continuation can occupy: **before** the future's waiters are released (`ProducerBatch.java:303-323`), and with a **-1 placeholder metadata** rather than the fault the `Task` carries (`Callback.java:28-33`) → see the §4 **delivery-callback divergence** (M14/P1). Note this row governs **one-shot** completions only; a **multi-shot registration** (a rebalance listener) is never expressible as a `Task` and is out of its scope entirely. |
| Non-blocking **getter** | sync **method**, Java's name (ledger 365) |
| Non-blocking **action**, no completion signal | sync plain **method** |

**M15/P14: `Task` or `AsyncKafkaFuture` on an async interface**

- a. `Task` when the Java method itself blocks. The method call is the wait. Example:
  `Task Flush(CancellationToken)` on `IAsyncProducer`, against `void Flush()` on `IProducer`.
- b. `AsyncKafkaFuture` when Java hands you back a `Future` object to wait on later. Example:
  `AsyncKafkaFuture<T>` inside `AsyncCreateTopicsResult`, against `KafkaFuture<T>` inside
  `CreateTopicsResult`.

**Futures (M19/P1, decision 32).** As in Java and Python, a failed future's `Get()`,
`Get(TimeSpan)`, `Get(CancellationToken)` and `GetNow` throw
`Confluent.Kafka.ExecutionException`, and the async `Get` tasks fault with
it: its `InnerException` is the stored failure and its message the failure's Java `toString()`
(`<class>: <message>`, the class alone when there is no message). This replaces the earlier
rethrow-unchanged deviation (decision 18). An `OperationCanceledException` given as the error of
an ordinary future cancels it, so it is thrown as it is, as Java's `KafkaFutureImpl.get` throws a
stored `CancellationException`. A send's future cannot be cancelled, so there it is a failure like
any other and goes inside the `ExecutionException`, as Java's `FutureRecordMetadata` wraps every
error (an `ErrorNext(new OperationCanceledException())` included). `WhenComplete` actions, `ThenApply`
functions' dependants, the delivery and commit callbacks and `ToCompletionStage()` see the failure
itself, never the wrapper.

*(Decision 32 = yes, Agreed 2026-10-17.)* `KafkaFuture<T>`, `KafkaFuture`, `AsyncKafkaFuture<T>` and
`AsyncKafkaFuture` are `readonly struct`s that carry Java's `KafkaFuture` members, in Java's order:
the statics `CompletedFuture` and `AllOf`, `ThenApply`, `WhenComplete`, `Cancel`, `Get()`,
`Get(TimeSpan)`, `GetNow`, `IsCancelled()`, `IsCompletedExceptionally()`, `IsDone()`, and `ToString` in
Java's `KafkaFutureImpl` format, every one a method (ledger 365). `BaseFunction` and `BiConsumer` are delegates nested in
`KafkaFuture`. Deviations, each recorded:
- `Get(CancellationToken)` is added (no Java twin). It ends only that wait, never the future.
- The non-generic futures are Java's `KafkaFuture<Void>`: their `ThenApply` takes a `Func<R>` and
  their `WhenComplete` an `Action<Exception?>`, because .NET has no `Void`. `KafkaFuture<T>`
  converts implicitly to `KafkaFuture` (the same future), so `AllOf` takes mixed types, and
  `AllOf<T>(params KafkaFuture<T>[])` takes an array of one type. The async structs match.
- `ToCompletionStage()` exists on the async structs only. It returns the future's own `Task`, the
  same on every call, which faults with the failure itself; `Get()` returns a different `Task`,
  which faults with the `ExecutionException`. The sync structs hand out no `Task`. *(Q13 = (a),
  ledger 45.)*
- The async `Get` forms return a `Task`; no struct has a `GetAwaiter`.
- A timeout throws Java's `TimeoutException` (`Confluent.Kafka.TimeoutException`, a
  `System.TimeoutException`): with no message, as `CompletableFuture.get`, and for a send's
  future with `FutureRecordMetadata`'s "Timeout after waiting for <ms> ms.", the milliseconds
  truncated. A negative or zero timeout checks once, as Java;
  `Timeout.InfiniteTimeSpan` waits like `Get()`. A cancelled future throws
  `OperationCanceledException`.
- A send's future is the same type. Its `Cancel(bool)` returns `false` and changes nothing, and
  `IsCancelled()` is `false`, as Java's `FutureRecordMetadata`. Nothing cancels a send's future:
  the `Send` token acts only on the first stage (§4 Cancellation row), and
  `Get(token)` ends only that wait.
- `CreateTopicsResult`'s (and `AsyncCreateTopicsResult`'s) `Config(topic)`, `TopicId(topic)`,
  `NumPartitions(topic)` and `ReplicationFactor(topic)` for a topic not in the request return what the C accessor returns: a
  future failed with `IllegalArgumentException` ("Topic <topic> was not part of the createTopics
  request"), which `Get()` throws inside an `ExecutionException`. Java's accessor dereferences the
  missing key and throws `NullPointerException` at the call. Python does the same as .NET (root
  `CLAUDE.md` Futures). Goes when the C ABI answers as Java's accessor does.

`ThenApply` functions and `WhenComplete` actions run where the future completes: the producer's
completion thread; the admin client's drain thread; the thread whose state read (`GetNow`,
`IsCancelled()`, `IsCompletedExceptionally()`) settles a done admin future before its delivery runs
(`IsDone()` only reads); the closing thread, for the deliveries that destroy fires; or inside the
call when the future is already done. This replaces the narrower surfaces ruled in M11/P3.6 (D4,
D6), M11/P4.2 (D7) and M15/P14 (§1.4), and closes FU-4.

Any **one** trigger is enough — blocking is just the most common of the three.

**Stays sync on the consumer — exactly these:** `Assignment()`, `Subscription()`,
`Paused()` (Java's methods, as every getter is, ledger 365; each returns a fresh owned
snapshot per call), `GroupMetadata()` (without `group.id` it throws
`InvalidGroupIdException` (24) with Java's message, after the closed check and
before the C call, as Java's `throwIfGroupIdNotDefined` and Python's `InvalidGroupIdError`
do; ledger 105), `Wakeup()`, `Metrics()`
(**shipped M9/P2** — `IReadOnlyDictionary<MetricName, IMetric> Metrics()`, Java
`metrics()`; on `KafkaConsumer` and `AsyncKafkaConsumer` it makes no owner check, as Java's
takes no `acquire()`, and holds only the use that keeps the handle alive; the core answers it
while an op holds its flag, BH:30592-30606 at `3f8a5bc6`; the mocks wait, as Java's
`synchronized` mock; after close it throws, the call-site list below §3's idiom map), `Register`/`UnregisterMetricForSubscription`, and
`EnforceRebalance(string? reason
= null)` (a no-op that only logs under KIP-848; one method collapses Java's two overloads). It
makes no owner check and, once the consumer is closed, returns without calling the core, as Java's
only logs. *(Recorded: the core still refuses it with -2, a `ConcurrentModificationException`, while
another op holds its flag. Goes when the core's `enforce_rebalance` takes no flag.)* `Seek` and `CurrentLag` are not on
this list since M19/P1: both block in Java, so each is a `Task` on `IAsyncConsumer` and a
blocking member on `IConsumer` (decision 22, ledger 103). There is no `ClientId()` since
M19/P1: Java's `clientId()` is package-private and Python has no `client_id()` (ledger 102). **On the
producer:** `Metrics` (**shipped M11/P8** — `IReadOnlyDictionary<MetricName, IMetric> Metrics()`,
Java `metrics()`, on **both** `IProducer` and `IAsyncProducer`; it maps **no**
concurrent-access null — the producer ABI documents none, it takes the core `Mutex` and blocks —
so its defensive null guard carries a producer-accurate message, decision D-5) and the two
metric-subscription methods. `BeginTransaction` is not on this list: it waits while the core drains
the queued sends, so it is a `Task` on `IAsyncProducer` (§4 **Producer transactions**).
Everything else is async.

⚠ **`Seek` / `CurrentLag` (M19/P1, decision 22, ledger 103).** Both block in Java (an
`addAndGet` event round-trip). `IAsyncConsumer.Seek` returns a `Task` over `seek_*_cb`, as
Python's async `seek` does, and `IConsumer.Seek` calls the blocking form.
`IAsyncConsumer.CurrentLag` returns a `Task<long?>` over `current_lag_cb`, as Python's async
`current_lag` does, and `IConsumer.CurrentLag` calls the blocking form; `ConsumerHandle.CurrentLag`
calls `ConsumerHandle_current_lag`. The lag wait is not wakeable, as Java's `currentLag` sets no
active task: a cancelled token sends no `Wakeup()`, and the `Task` ends with
`OperationCanceledException` once the lag call has ended, as Python's async `current_lag` does
(`python/confluent_kafka/consumer/_base.py:1425-1429`, `:1159-1212` at `3f8a5bc6`).
`Seek(tp, long)` keeps Java's negative-offset guard (`IllegalArgumentException`,
`"seek offset must not be a negative number"`); the core makes the same check
(`rust/src/consumer/internals/async_kafka_consumer.rs:6067-6070` at `3f8a5bc6`). This replaces
the M5/P7 sync-`Seek` divergence.

⚠ **The ABI must be able to honor it.** Where Java blocks but the ABI exposes only a blocking
entry point, wrapping that call in `Task.Run` would be sync-over-async (forbidden, ffi §B7). At
`3f8a5bc6` every blocking consumer op this binding exposes has a `_cb` twin, so no consumer
method is left that the async flavor cannot honor.

⚠ **§4 divergence — the rebalance listener (M9/P6; the async form M19/P1).**
`IConsumerRebalanceListener`'s three methods return `void`, and both consumers take it. The
async consumer also takes `IAsyncConsumerRebalanceListener`, whose three methods return
`Task`, as Python's async consumer accepts `async def` listener methods. Java has only the sync
form. Either way, the rebalance (and the operation that triggered it) does not advance until
the method's result is reported. Every listener method reports through
`kafka_consumer_Consumer__set_callback_result`: a sync method when it returns, an async one
when its `Task` completes, from any thread. The drain thread never waits on a listener's
`Task`.

Threads: on the sync consumer a listener runs on the calling thread, inside the blocking call
(Java parity). On the async consumer it runs on `confluent-kafka-consumer-pump`; a sync
listener there runs inline, and an async listener's continuation runs wherever its `Task`
completes. The sync consumer takes the sync listener only.

Java's `onPartitionsLost` default (delegate to revoked) cannot live on an interface: C#
default interface methods need .NET Standard 2.1, and the floor is netstandard2.0. It lives on
the public abstract `ConsumerRebalanceListenerBase` and `AsyncConsumerRebalanceListenerBase`
(`definition-of-done.md` §7 types that exist because of a Java behaviour). §3 Java names rule 10
replaces them: the default becomes an extension method in `ConsumerRebalanceListenerExtensions` and
`AsyncConsumerRebalanceListenerExtensions`, which `IConsumerRebalanceListenerWithPartitionsLost` and
`IAsyncConsumerRebalanceListenerWithPartitionsLost` replace; until then CKRDA0013 baselines
`OnPartitionsLost` on the two interfaces. A listener calls
back into its own consumer directly, and the consumer routes the call to `ConsumerHandle`
(ledger 114; the §4 reentrancy row below).

This is a multi-shot registration, so the "takes a completion callback → the `Task` replaces
the callback" row above does not apply: a listener is bound to a subscription and fires N
times. This replaces roadmap Q6 / divergence D1 for the async consumer.

⚠ **§4 divergence — the commit callback is SYNC (with an async form on `IAsyncConsumer`,
M19/P1), and runs on the caller (`IConsumer`) or the drain thread (`IAsyncConsumer`) (M9/P7;
decision 27).** `IOffsetCommitCallback.OnComplete` returns `void`, not `Task`, on both consumer
flavors. Across the ABI it reports through `kafka_consumer_Consumer__set_callback_result`, as a
listener method does (BH:3778-3792 at `3f8a5bc6`). On `IConsumer` it runs inline on the caller,
inside the blocking call that delivers it; on `IAsyncConsumer`, on
`confluent-kafka-consumer-pump` (decision 27). Java's `onComplete` is `void`. On
`IAsyncConsumer`, `CommitAsync` also takes `IAsyncOffsetCommitCallback`, whose `OnComplete`
returns `Task`, as Python's async consumer accepts an `async def` commit callback (M19/P1,
ledger 47); see the async form below; §3's row previously read "(async)", describing the
**Rust core's** trait.

An exception raised by the callback propagates out of the consumer call that ran it (`Poll`
wraps it in `KafkaException`, `Close` in 'Failed to close kafka consumer', `Commit*` rethrows it
as is), as Java and Python do (decision 12); the callbacks queued behind it run at the next such
call. Two error surfaces stay distinct: the exception `CommitAsync` throws on `IConsumer`, or
with which its `Task` faults on `IAsyncConsumer`, is a commit-**initiation** failure; the
`exception` delivered to `OnComplete` is the **commit's** outcome.

This is a **one-shot** completion, so — unlike the listener — the "takes a
completion callback → the `Task` replaces the callback" row *would* textually
apply; the **§4 sync-vs-async table's ⚠ exception** (third row) is what carves it
out, on the grounds that the callback carries offsets a `Task` cannot.

⚠ **Recorded sub-divergence — `CommitAsync(callback)` rejects a `null` callback
that Java accepts.** Java's `commitAsync(OffsetCommitCallback)` accepts `null`
(equivalent to `commitAsync()`); this binding throws `ArgumentNullException`. The
parameter is non-nullable under `#nullable enable`, `CommitAsync()` already
expresses "no callback", and ffi §B5 mandates precondition validation before the
FFI call — so accepting `null` would add a second spelling of an existing member
while weakening the annotation. Deliberately stricter than the reference, recorded
rather than silent. Note the **other** overload is deliberately *not* stricter: on
`IConsumer`, `CommitAsync(offsets, null)` **is** honoured, as Java's
`commitAsync(Map, null)`; on `IAsyncConsumer` it is ambiguous (the async form below).

**The async form (M19/P1, ledger 47).** On `IAsyncConsumer`, `CommitAsync` also takes
`IAsyncOffsetCommitCallback`, whose `OnComplete(offsets, exception)` returns `Task`, as
Python's async consumer accepts an `async def` commit callback. Java has no async form, and
`IConsumer` and `ConsumerHandle` take the sync callback only. It runs on
`confluent-kafka-consumer-pump`, which does not wait for its `Task`. When the `Task` ends,
its continuation reports through `kafka_consumer_Consumer__set_callback_result`, from any
thread. The call that delivers the callback waits for that report, and an exception from
the `Task`, a cancel included, propagates out of that call as a sync callback's does. The
core delivers the next commit callback only after the previous one reported, so they start
one at a time, in commit order. One case differs: a callback that a blocking `ConsumerHandle`
commit runs is reported at once and its `Task` runs on, so the callbacks after it may start
before it ends; the call that delivered the outer callback waits for the `Task`, then throws its
exception, wrapped as that call wraps a callback's, while a `Poll` returns its records and the
next awaited call throws it, as Python does (ledger 52;
`python/confluent_kafka/consumer/async_consumer.py:184-194`, `:515-526` at `3f8a5bc6`). A
`null` async callback throws `ArgumentNullException`. On `IAsyncConsumer`,
`CommitAsync(offsets, null)` is ambiguous and does not compile: write `CommitAsync(offsets)`.

⚠ **§4 divergence — the delivery callback is SYNC (with an async form on `IAsyncProducer`,
M19/P1), and the callback-taking `send` is kept ALONGSIDE the future (M14/P1; mechanism changed
in M19/P1).** `ICallback.OnCompletion` returns `void`, not `Task`, and both `IProducer`
and `IAsyncProducer` carry a **second** `Send` overload taking it, declared identically on each,
with no shared base (the M11/P8 D-6 `Metrics()` precedent). Java's second `send` signature
*still returns the `Future`* (`Producer.java:86`), so the callback is an **additional** parameter,
not an alternative. Since M19/P1 it crosses the ABI as one `Callback_t` per record (ffi §A6,
decision 10). The parts, each decided against the Java source:

- **Thread.** It runs on the producer's completion thread (`confluent-kafka-producer-completion`
  or `confluent-kafka-producer-pump`), .NET's analogue of the "background I/O thread" Java
  documents (`Callback.java:20-21`), on both surfaces: inside `__execute_callbacks`, or, on
  `IAsyncProducer`, as a chain step behind a running async callback (ffi §A6). So one
  producer's callbacks never overlap each other. A callback still pending when the producer is
  destroyed runs on the thread that destroys it (§4 **Close/Dispose**). The sync form is `void`,
  so the pump never waits on it past its return. The async form (below) returns a `Task`; the
  pump thread does not wait for it, but every later completion of the producer waits until it
  ends. One instance shared across two producers can run on both threads at once. The one
  exception: for a serializer's `ApiException`, `KafkaProducer` / `AsyncKafkaProducer` run it
  once inside `Send`, on the caller's thread, with Java's placeholder metadata, before the
  failed future is returned, as Java's `doSend` does; nothing crosses the ABI (§3, the
  serializer's throw). The mocks rethrow that exception from `Send`.
- **Ordering.** It runs **before** the delivery awaiter is released / before `Get()` returns,
  mirroring `ProducerBatch.java:303-323` (value set → callbacks → `done()`), as Python
  does (`_complete` runs the callback, then `complete_record`).
- **Non-null metadata.** The delivered `RecordMetadata` is **never null**: on the failure path it is
  Java's `-1` placeholder (`KafkaProducer.java:1597-1599`, contract at `Callback.java:28-33`), as
  the core delivers it (offset -1, `rust/src/ffi/producer/callback.rs:29-30` at `0035d9ff`).
  Python's `on_delivery` receives `None` on failure, so this is stricter than Python.
- **Which outcomes fire it.** Every record the core accepted fires it exactly once, success or
  failure. A record rejected before the append fires nothing: the sync `Send` throws, and the
  async `Send`'s first stage faults. The one exception is a serializer's `ApiException` on
  `KafkaProducer` / `AsyncKafkaProducer`, which fires it once inside `Send` (**Thread**, above). When the send reports an error, the callback may still
  fire later, once, if the record reached the accumulator (Java's `doSend` appends the record
  with its callback before `maybeAddPartition` fails; BH:38221-38227, BH:38251-38254). So an
  error from `Send` does not prove the callback will not fire. A record that `Close` fails while
  it waits for metadata or buffer memory ("Producer closed while send in progress") fires
  nothing (ledger 70): Java's `doSend` rethrows that `KafkaException` without the callback.
- **Exactly-once, per record.** Invoked unconditionally, also for a record whose `Get` wait was
  abandoned (CLAUDE.md §9.5). The pull-pump's recorded residuals (teardown and pump-window drops)
  are gone with the pump: the core fires every accepted record's callback, at the latest during
  destroy, which runs on the closing thread (§4 **Close/Dispose**).
- **A `Get()` from inside the callback** on the record's own future waits, and never returns, as
  in Java and Python (decision 39). `Get(TimeSpan)` and `Get(token)` end as usual.
- **Throwing.** An exception the callback throws is reported to the core through `local_callback`
  (the core logs it and leaves the record's outcome unchanged) and written once to
  `System.Diagnostics.Trace` (decision 20). It never faults the record's future or another
  record.
- **Null callback rejected** (`ArgumentNullException`) although Java accepts
  `send(record, null)` (`KafkaProducer.java:1058` guards `if (callback != null)`).
  Exactly the `CommitAsync(callback)` sub-divergence above, and here it points only
  one way: the plain `Send(record)` overload already spells "no callback", so — unlike
  `commitAsync(Map, null)` — there is no second parameter that would make a
  null-accepting form non-redundant.

**The async form (M19/P1, ledger 47).** `IAsyncProducer` also takes `IAsyncCallback`,
whose `OnCompletion` returns `Task`, as Python's async producer accepts an `async def`
callback. Java has no async form, and `IProducer` takes the sync callback only. The pump calls
it and does not wait for its `Task`. While its `Task` runs, every later completion of the
producer waits behind it, in completion order, whatever callback its send has (async, sync or
none), as Python's does; the producer keeps one such queue (Python keeps one per event loop).
When the `Task` ends, on the pump: the callback is reported through
`kafka_producer_Producer__set_callback_result`, a fault or a cancel as a `local_callback`
error carrying the exception, traced once; then the record's future completes; then the next
queued completion is handled. The record's outcome does not change. `Close` waits for the
report; `Flush` and a transaction commit or abort wait for it when they succeed. From inside
the callback on `AsyncKafkaProducer`, after an `await` too: `Flush` throws Java's "may lead to
deadlock" message; `Close` closes with a 0 ms timeout and Java's warning, and returns at once; a
transaction commit or abort fails after `max.block.ms` with Kafka's `TimeoutException`
(`Confluent.Kafka.Common.Errors`).
`AsyncMockProducer`'s Java mock has no such rules, but after an `await` the core cannot see the
callback. There `Close` returns at once; `Flush` completes the mock's pending sends and returns
without waiting for their reports, as Java's and Python's mock `flush()` do
(`ffi-marshalling.md` §A7); and a commit or an abort ends with the same `TimeoutException`, a
recorded deviation. A callback that awaits
`Get()` of its own record, or of any later record of the producer, never ends. A `null`
callback throws `ArgumentNullException`, and `Send(record, null)` does not compile.

⚠ **§4 reentrancy row — `ConsumerHandle` is host scaffolding that restores a Java
behavior, not new API surface (M9/P8; DoD #7).** `ConsumerHandle` and
`IConsumerCommon.Handle()` have **no Java counterpart**, and that needs stating
plainly rather than being discovered in review. Java needs none: its callbacks run
on the polling thread, where `KafkaConsumer.acquire()` is *reentrant*, so a listener
simply captures the `consumer` variable and calls `consumer.commitSync()` from
`onPartitionsRevoked` (`consumer-threading.md` §31). The C ABI cannot flatten that —
its single-owner access guard is held for the whole operation that fired the
callback, so the consumer's own C API is rejected with `ConcurrentModification` from
inside one. The handle is the sanctioned route around it, and it exists **because**
the shape was flattened at the ABI (`dotnet/.claude/rules/bindings.md §1.2`), which is exactly the
"scaffolding that supports the shape and adds no Kafka behavior" the mental model
allows. Python uses the same core handle for the same reason, but privately: a consumer
call made inside a callback goes through it, and Python has no public handle type
(`python/confluent_kafka/consumer/_base.py:701-718`, `:1645-1720` at `3f8a5bc6`). .NET keeps
it public (M9/P8). Since ledger 114 .NET also routes a consumer call made inside a callback
through it, as Python does, so a listener calls `consumer.Commit()` as Java's listener does
(`ffi-marshalling.md` §B1).

Four consequences worth stating with it:

- **The method set is deliberately smaller than the consumer's.** It is the core's
  (KIP-1306's `RebalanceConsumer` plus `wakeup`), less the forms ledger 104 leaves out, so it
  has `Wakeup`, `Assignment`, `Paused`, `Pause`, `Resume`, `SeekToBeginning` / `SeekToEnd`, both
  `Seek`s, `Position` and `Position(tp, timeout)`, `Committed`, `BeginningOffsets`, `EndOffsets`,
  `OffsetsForTimes`, both `Commit`s, `CurrentLag` (ledger 103), `CommitAsync()`,
  `CommitAsync(offsets)` (over `ConsumerHandle_commit_async_with_offsets_callback` with a no-op
  callback, as Python's in-callback `commit_nowait(offsets=…)` does; ledger 104), and
  `CommitAsync(callback)` / `CommitAsync(offsets, callback)` over
  `ConsumerHandle_commit_async_with_callback` / `_with_offsets_callback`, as Python's in-callback
  `commit_nowait(callback=…)` (`python/confluent_kafka/consumer/_base.py:1586-1623` at
  `3f8a5bc6`). No `Poll` / `Subscribe` / `Unsubscribe` / `Assign` / `Close` /
  `EnforceRebalance` / `Subscription` / `Metrics`: the core's handle has none
  (`rust/src/consumer/internals/async_kafka_consumer.rs:196-209` at `3f8a5bc6`). No other timed
  form, `PartitionsFor`, `ListTopics` or `GroupMetadata` (ledger 104): Python has no public
  handle, and inside a callback the consumer routes those calls to the core handle's own forms
  (ledger 163), as Python's `_reentrant` does
  (`python/confluent_kafka/consumer/_base.py:1823-1923` at `dd826c17`).
- **On a `KafkaConsumer`, its operations other than `Wakeup`, `Assignment` and `Paused` work
  only inside a callback the consumer is running** (a listener method or an `OnComplete`).
  Elsewhere they throw `ConcurrentModificationException` (the core's -2) with the core's message,
  whose first sentence is Java's `acquire()` text; from another thread, call `Wakeup()`. A
  `MockConsumer` handle is not gated, as Java's `MockConsumer` has no `acquire()`
  (BH:30638-30659 at `3f8a5bc6`; ledger 104).
- **It holds its consumer weakly** (the core's handle does). Once the consumer is destroyed, a
  handle op fails with `IllegalStateException` (the core's -4). The handle does not keep the
  consumer alive and does not defer its destroy. This replaces P8-D1 (M19/P1, ledger 34).
- **`IDisposable` only, not `IAsyncDisposable`** (P8-D4) — every other disposable in
  this binding implements both, so the asymmetry is deliberate, and stated in one concise comment
  at the site: every handle operation is a *synchronous* C call, so a `DisposeAsync` would
  wrap a sync destroy and imply an async surface that does not exist.

**Exception — Java sync/async pairs (the commit family; M5/P6, changed in M19/P1 by
decision 13).** Where Java ships an explicit pair (`commitSync` + `commitAsync`), keep **both**,
mapped from Java's blocking behavior (the idiom map), not from Java's name:

- **`Commit(...)`** = the **confirming** commit: Java `commitSync`, `commitSync(Map)` and their
  `Duration` overloads (decision 22); Python `commit()`. On `IAsyncConsumer` it is `Task
  Commit(...)` over `commit_sync*_cb`, completed on the drain thread; on `IConsumer` it is a
  blocking `void Commit(...)` over `commit_sync*`. When it returns (or its `Task` completes), the
  commit landed.
- **`CommitAsync(...)`** = Java `commitAsync()`, `commitAsync(cb)` and `commitAsync(Map, cb)`;
  Python `commit_nowait()`. Java's `commitAsync` does not wait for the commit. On `IConsumer` it
  is `void CommitAsync(...)` over the plain `commit_async*` forms, which return once the commit is
  started. On `IAsyncConsumer` it is `Task CommitAsync(...)` over `commit_async*_cb`: the `Task`
  completes when the core has accepted the commit, and the commit's outcome still reaches the
  callback (decision 13); two more overloads there take `IAsyncOffsetCommitCallback` (ledger 47).
  It is not on `IConsumerCommon` any more, because the two flavors return different types.
- **Where the commit callback runs:** on `IConsumer`, inline on the caller, inside the blocking
  call that delivers it (`Poll`, `Commit`, `Close`, …); on `IAsyncConsumer`, on the drain thread
  (decision 27).

Naming: `Commit` is Python's `commit()`; `CommitAsync` keeps Java's `commitAsync` name, where
Python's is `commit_nowait()` (root `CLAUDE.md`, Class family). The map reads Java's *blocking
behavior*, not Java's method name, which is why Java's `commitSync` is our `Commit`. There is
**no** `CommitSync` member.

### Producer transactions

Java's `initTransactions`, `beginTransaction`, `sendOffsetsToTransaction`, `commitTransaction` and
`abortTransaction`, on `IProducer` (`void`) and `IAsyncProducer` (`Task`, with a
`CancellationToken`, `BeginTransaction` included, as Python's async classes make `begin_transaction`
`async def`, where Java's `beginTransaction` returns at once).
Each calls its `_cb` twin and waits on a managed `Completion` or `TaskCompletionSource`
(`ffi-marshalling.md` §A7), `BeginTransaction` included, so it waits while the core drains the
sends already handed over (root `producer-transactions.md` §13). The core makes every check, with
Java's class and message; the binding adds only its closed check, first, as for every producer
call, and on `SendOffsetsToTransaction` the consumer commit's check of the offsets' contents
(`ArgumentException` "Topic names must not be null." / "Offset value must not be null."), since
the map is built from the managed dictionary. With an empty `offsets` the real producers return at
once without checking the transaction state, as Java's `KafkaProducer` does; the mocks check it
first, as Java's `MockProducer` does. `SendOffsetsToTransaction` passes the core's own `ConsumerGroupMetadata` handle, which the
object keeps from the consumer that gave it, the mocks' included. Recorded deviations:

- **Overlapping calls.** A transaction call while the core still runs another one's operation on
  the same producer throws the core's `ConcurrentModificationException` "Transactional methods of
  KafkaProducer are not safe for concurrent access."; Java has no such check. The core releases its
  flag before a commit or an abort waits for the delivery reports, so a call made during that wait
  is not refused.
- **Cancellation and retry.** A token already cancelled throws `OperationCanceledException` at
  the call. A token that fires later ends only the wait; the operation runs on in the core, as
  after Java's `max.block.ms` timeout, and a later call gets what the core answers: while the
  core still runs the operation, `ConcurrentModificationException` (above; Java's retry would join the
  pending result, Python's async producer answers as .NET does); once the core has timed it out,
  Java's rules (the same call joins the pending result, another one throws
  `IllegalStateException` "Cannot attempt operation `X` because the previous call to `Y` timed out
  and must be retried"); once it has succeeded, whatever the core answers for a new call (on the
  mocks, `InitTransactions` again throws "MockProducer has already been initialized for
  transactions."). Only `IAsyncProducer` takes a token.
- **Order of the null checks.** `SendOffsetsToTransaction` checks for a closed producer first, then a
  null `groupMetadata` (`IllegalArgumentException` "Consumer group metadata could not be null"), then
  a null `offsets` (`ArgumentNullException`), then the state, on all four classes. Java's
  `KafkaProducer` and `MockProducer` check `groupMetadata` before the closed check (the mock with
  `NullPointerException`), and Java's mock checks a null `offsets` only after its closed and state
  checks.
- **From a delivery callback.** From a sync delivery callback on `KafkaProducer` and
  `AsyncKafkaProducer` each of the five calls ends after `max.block.ms` with Kafka's
  `TimeoutException`, `BeginTransaction` included, where Java's `beginTransaction` returns at
  once (Python's does the same as .NET); from an async callback after an `await`, a commit or an
  abort does (`ffi-marshalling.md` §A7). On the mocks the five calls complete from a delivery
  callback, as Java's mock completes them, with one exception: from a callback the core cannot
  see (on `AsyncMockProducer`, a sync callback or an async callback's synchronous part queued
  behind an async callback, or an async callback after an `await`), `CommitTransaction` and
  `AbortTransaction` wait for that callback's own report, so they end after `max.block.ms`
  (60000 on a mock) with Kafka's `TimeoutException` *(recorded deviation from Java's mock; it
  goes when the core has a commit and abort form that skips the report wait)*. The other three
  take the blocking form there, which waits for no report, and complete at once (§A7).
- **A serializer's fatal error** does not make a transactional producer fatal (§3, the
  serializer's throw).

The mocks carry Java's `MockProducer` transaction controls (§3 sketch), over the core mock's
`kafka_producer_MockProducer_*` functions:
- The five setters take `Exception?`, as `ErrorNext` does, since Java's fields are
  `RuntimeException`; the call throws the same instance, and `null` clears it.
- The flags, `CommitCount()`, `ConsumerGroupOffsetsHistory()` and `UncommittedOffsets()` are
  methods, as every Java getter is (ledger 365); the last two copy a native container out, so
  every call returns a fresh copy.
- Java's public fields `sendException`, `flushException`, `partitionsForException` and
  `closeException` are setters `Set<Field>(Exception?)`, as Python's `set_<field>`; CKRDA0013
  allowlists them, with the five transaction setters.
- `UncommittedRecords()` and `History()` are left out: the ABI's records hold the serialized
  bytes, and the binding cannot turn them back into typed `TKey` / `TValue` records (§3, clipped to
  today's ABI). `HistoryCount()` stands in, so the ports of Java's history tests check
  `HistoryCount()`, not the records.

### Threads and teardown (M19/P1)

**Callback threads.**
- One `CompletionThread` per client drains `__execute_callbacks`, woken by the notify hook.
- Names: sync producer `confluent-kafka-producer-completion`; async producer
  `confluent-kafka-producer-pump`; sync admin `kafka-admin-client-thread`; async admin
  `confluent-kafka-admin-pump`; async consumer `confluent-kafka-consumer-pump`. The sync consumer
  has no thread: it uses the blocking forms, and listeners run on the caller.
- The notify hook only signals.
- Never call a client's `_destroy` inside `__execute_callbacks`.

**Close/Dispose**, in this order:
1. refuse new calls;
2. close;
3. stop and join the drain thread;
4. wait for in-flight uses;
5. destroy on the closing thread.

The admin mocks' `Close` does nothing (decision 21); their `Dispose` runs these steps. A
close from a callback is deferred to the drain thread, after its drain. A SafeHandle reached by
finalization does not destroy: it leaks and writes one `Trace` warning (Java and Python have no
client finalizer).

**Consumer deviation (recorded).** The consumer is single-owner: every call is a use of the
thread that makes it, and a call while another thread holds a use throws
`ConcurrentModificationException` ("KafkaConsumer is not safe for multi-threaded access.") before it
reaches the core, as Java's `acquire()` and Python's use table do (ledger 136;
`ffi-marshalling.md` §B1). So:
- `Dispose` from another thread during an op does not throw. It runs: wakeup, wait for the
  in-flight uses, close, join the drain thread (async consumer), destroy. This is a deviation:
  Java and Python reject the second thread, but .NET code expects `Dispose` not to throw.
- An explicit `Close` from another thread during an op throws `ConcurrentModificationException` at
  once and leaves the consumer open, as Java and Python. Call `Wakeup()` first.
- A `Close` from inside a callback (a listener method or a commit callback, after an `await`
  included) throws `ConcurrentModificationException` and leaves the consumer open, on both consumers
  (ledger 113, Python's way: `python/confluent_kafka/consumer/_base.py:195`, `:611-639` and
  `python/confluent_kafka/consumer/consumer.py:620-632` at `3f8a5bc6`). `Dispose` there
  throws the same way on the sync consumers (ledger 120); on the async consumers it schedules
  the teardown and returns at once (ledger 113, §B2 path 2 of `ffi-marshalling.md`).

### Comments (M19/P1, ledger 38)

1. Doc comments (`///`) go only on the public API, on the interface that declares it. An
   implementing or derived member uses `<inheritdoc/>`. A public member that no interface or base
   declares gets one concise `///` summary, because CS1591 requires it. *(Q17 (a).)*
2. Every class, struct and interface, helper and internal ones included, gets one concise
   summary of what it does.
3. Every other comment goes. The one exception is a concise comment for a recorded or very
   important decision or deviation, where it is needed. It states the decision, not its history.
4. No references to Java or Python code with line numbers: line numbers change and cause needless
   edits. A reference may name the class or method.
5. No phase or review IDs in comments (`M11/P3.5`, `D2`, `Critic 42`). The design history holds
   them. *(Q17 (c).)*
6. Copyright headers stay as they are.
7. The rule covers `src`, the tests, `grpc-server`, `soak` and the perf projects. It does not
   cover rule files or design docs. *(Q17 (b), (d).)*
8. Each phase cleans its own area, in one comment-only commit at the end of the phase.

(The lines marked Q17 follow the plan's recommendation, which Pranav agreed on 2026-10-17,
ledger 40.)

---

## 5 · Boundary rules → `ffi-marshalling.md`

The correctness contracts you must not break live in
`.claude/rules/ffi-marshalling.md` (read on demand), organized as a **Shared**
part + a **Producer** part + a **Consumer** part (each client reads end-to-end) +
an **Admin** part (what the admin client adds).
Index:

**Part 0 · Shared mechanics**
- §0.1 P/Invoke declarations & type map (`[DllImport]`, `Cdecl`, TFMs)
- §0.2 Native library loading, packaging & AOT
- §0.3 Thread topology & thread-safety (shared framing)

**Part A · Producer** (end to end)
- §A1 Thread model · §A2 Handle ownership (`SafeHandle` vs transient) ·
  §A3 String marshalling (UTF-8, the shared mechanism) · §A4 Zero-copy — send
  (call-scoped pin; the async send pins until the handover) · §A5 Error model
  (the class of the error's id + preconditions) ·
  §A6 Callback marshalling (the `_cb` completions and the per-record `Callback_t`) ·
  §A7 Async completion (`KafkaFutureImpl`, `Completion`, the in-callback rules)

**Part B · Consumer** (end to end)
- §B1 Thread model · §B2 Handle ownership (owned container + borrowed view) ·
  §B3 String marshalling — receive path (length-delimited borrow) · §B4 Zero-copy
  — receive (borrow, copy-out) · §B5 Error model additions (wakeup, concurrent) ·
  §B6 Completion callbacks (the `_cb` completions, the listener and commit-callback registrations) · §B7 Async completion (the async consumer's drain thread)

**Part C · Admin**
- §C1 Drain thread, futures (eager `get_cb`, a SafeHandle per owned future) and teardown

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
   `target/include/confluent_kafka_bindings.h`.
5. **Wrap in `NativeMethods`** — `[DllImport]` declarations + `SafeHandle`s.
6. **Expose the managed API** — the Java-shaped surface in a new class.
7. **Test & build** — Mock/parity tests → `dotnet build` → `dotnet test`.

**Naming across layers:** `kafka_<pkg-minus-clients>_<Type>_<method>` at the ABI;
C# casing above it (PascalCase methods with Java's exact names, getters included, ledger 365 —
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
checked-in `.editorconfig` (`_camelCase`/`s_` fields, PascalCase, Java's names for
the constants and enum members of §3's idiom map (ledger 285), Allman braces,
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
  `CompleteNext` / `ErrorNext(Exception)` and `AddRecord` /
  `SetPollException(KafkaException)`, decision 15), and wait on the returned future or `Task`
  with a timeout — the timeout doubles as the **completion / deadlock regression guard**
  (the drain thread's join; a callback that never reports)
- **TFM-matrix smoke test**: the binding loads and a `MockProducer` / `MockConsumer`
  round-trip on **net462** (via netstandard2.0), **net8.0**, **net10.0**
- Parity obligations (`definition-of-done.md §3`): mirror the Java/Rust tests,
  **assert error-message content**, and add a per-record **allocation-budget**
  test — on the **send path** (producer) and, for the consumer, on the
  **receive path** (`consumer-threading.md §27`; the copy-out budget).

### 7.5 Definition of done

A port is not done until it builds on the TFM matrix, unit tests pass against
`MockProducer` / `MockConsumer`, lint/format are clean, and the `ffi-marshalling.md` anti-patterns
are satisfied (`definition-of-done.md`).

The gate is `make verify-dotnet`, run from the repository root. It runs format, the unit tests, the
broker-backed integration tests and the soak tests on net8.0 and net10.0, the `__grpc_dotnet` and `__grpc_dotnet_async`
multilanguage arms over the two .NET gRPC images, and the perf stage. CI runs it on Linux, and
runs the arms again over SSL and SASL_SSL. On macOS, CI runs `make test-dotnet-macos-docker` (the
unit tests, without the integration tests, as Python's macOS job runs `test-unit` only), then starts
Colima and runs `make test-integration-dotnet-native`: the arms with the gRPC server as a native
process, with no perf stage. The
arms skip only the tests named in `rust/Makefile`'s `DOTNET_GRPC_SKIPS`, each listed with its
reason and its REMOVE condition. The gRPC server's own unit tests are a local gate, outside CI:
`make -C dotnet test-grpc-server-dotnet`.

### 7.6 The gRPC test server (`grpc-server/`)

The multilanguage harness (`rust/tests/integration.rs`) reaches the binding through
`grpc-server/`, on the protos in `rust/multilanguage-test-server/proto/`. The server is a
translation of Python's:
- the sync servicers translate `python/grpc_server.py` over the sync clients;
- the async servicers translate `python/grpc_server_async.py` over the async clients;
- `Translate` and `TranslateAdmin` translate `python/grpc_translate.py`.

The server calls only the public API and adds no Kafka behaviour (`bindings.md` §1.2), with one
exception: it reads an error's internal id (`Code`, and the generated type → id lookup for the
classes outside `KafkaException`), as Python's server reads `_ffi_id`. The library grants
`InternalsVisibleTo` to the assemblies `Confluent.Kafka.GrpcServer`, `SoakClient` and
`SoakClient.Tests` for it (the soak client's code tables read the id as Python's soak does),
beside the unit-test assemblies.

**Errors.** A proto `KafkaError`'s `code` is the FFI code (`kafka_common_Error_e`). The code
alone names the error's class (`producer_service.proto`, `KafkaError`). Every exception crosses
through `Translate.ToProto`, as every Python error crosses through `_kafka_error_to_proto`:
- A class with an id is sent with its id and its `Message`, as Python's server sends `_ffi_id`:
  a `KafkaException` its `Code`; a Java-named class (§3, the `KafkaException` hierarchy row) or
  `CorrelationIdMismatchException` the id the lookup gives it. So -2 (the owner check's
  `ConcurrentModificationException` included), -3, -4 (a closed client included), -5, -24 and
  -31 go out as Python sends them.
- An `ArgumentException` that is neither a Java-named class nor an `ArgumentNullException` (a
  .NET-only precondition, ffi §A5) is sent as -3. The text is the message the binding gave it,
  without what .NET appends to it: an `ArgumentException`'s " (Parameter '…')" and an
  `ArgumentOutOfRangeException`'s "Actual value was …" line. So the text is Python's, which sends
  Java's message alone.
- Any other exception is sent as -4 with the message "dotnet server: <Type>: <message>". This is
  Python's fallback for a class with no FFI id, `TypeError` and `RuntimeError` included: an
  `ArgumentNullException`, a plain `InvalidOperationException` (an admin result's invariant error,
  ffi §A5), an `ObjectDisposedException` (a disposed `ConsumerHandle`), a plain
  `NotSupportedException` or `System.TimeoutException`, and the Java-named classes with no id
  (`NoSuchElementException`, `IndexOutOfBoundsException`, `ExecutionException`).
- A request the server cannot translate is sent as -3 (`TranslateAdmin.RequestError`, Python's
  `AdminRequestError`).
- A client constructor's rejection that is not a `KafkaException` is sent as -3
  (`TranslateAdmin.ConstructorError`, Python's `_admin_constructor_error`).
- An unknown client id is sent as -4.

The chaos events use the same helper.

**Per-key results** (`admin_service.proto`, the per-key result envelope). Each per-key future
is resolved on its own, as Python's `_collect` and `_per_key` do:
- Whatever exception the future fails with is that key's entry `error`, translated as above, and
  the other keys keep their outcomes.
- A value-level error is kept per key the same way. `CreateTopicsResult`'s `TopicId`,
  `NumPartitions`, `ReplicationFactor` and `Config` failing give the `TopicMetadataAndConfig`
  error.
- A failure that chooses another path also works with any exception type. The one such path is
  the SCRAM `All()` fallback.

The top-level `error` carries only a failure that comes before any per-key future exists: the RPC
method throwing, an unknown `admin_id`, or a request the server cannot translate. When it is
set, `entries` is empty.

Tests (`GrpcServer.UnitTests`):
- A mock RPC with a per-key future that fails with `UnsupportedOperationException` returns that
  key's -31 entry and the other keys' values: `DescribeConfigs` on a `BROKER_LOGGER` and a `TOPIC`
  resource.
- A mock RPC that Java's mock does not implement returns a -31 entry with "Not implemented yet"
  for each key and no top-level error: the core mock fails each key's future at `Get()` (ledger
  173, -31 since ledger 206), where Python's mock throws at the call and its server sends the
  top-level -31 with no entries (`test_grpc_admin_translate.py`'s
  `test_whole_call_failure_is_the_top_level_error`), a recorded difference (ledger 214).

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

Review a change against the **C ABI header** (`confluent_kafka_bindings.h`) and the **Kafka
Java public API shape** — **not** Rust internals, and **not** Java implementation
logic (`dotnet/.claude/rules/bindings.md §2`).

### 8.3 The Critic's lens (.NET-specific)

`SafeHandle` / `Dispose` correctness · handle leak / double-free / use-after-free ·
byte pinning & buffer lifetime · `MarshalAs(I1)` for `bool` · UTF-8 (no `LPStr`) ·
no managed exception through a callback · `RunContinuationsAsynchronously` on the
completion (the drain thread) · every error the class of its id, and a binding-made one the class the core gives the same condition (ffi §A5), vs the .NET-only precondition exceptions · **shape, not
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
