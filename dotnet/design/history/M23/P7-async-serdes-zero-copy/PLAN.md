# Plan: async serdes and zero-copy deserialization

Status: approved by the owner (2026-11-18). Scope: the full plan.

Milestone 23, Phase 7. Agent number N=129. Mode A (.NET only): no Rust, C ABI, header, Python or proto change.
Branch `esabellico/dotnet_async_serdes_and_zero_copy`, cut from `master` `3b5072532` (#343), in the worktree
`kafka-clients-dotnet-serdes`. No upstream; never pushed by an agent. All paths below are under `dotnet/`.

How it runs: a dotnet-actor 129 implements one step at a time and commits it with `--no-verify` after running
the step's tests (section "Gates per step"); one dotnet-critic 129 loop runs over the whole branch at the end.
The rule edits of S0 are drafts (section "Draft rule text"): no rule file is edited until the owner says so,
and until then this plan wins over the rule text it replaces.

## Context

The .NET async clients take the synchronous `ISerializer<T>` / `IDeserializer<T>` today, so a
serde that does I/O (Schema Registry) has to block. Deserialization also copies: the consumer
hands each deserializer a `ReadOnlySpan<byte>` over the native batch and destroys the batch when
`Poll` returns, so today's `ByteBufferDeserializer` must `ToArray()`.

This change adds `IAsyncSerializer<T>` / `IAsyncDeserializer<T>`, makes the four async clients
take only those, and adds a zero-copy path where a deserializer receives a `ReadOnlyMemory<byte>`
over the native batch that it may keep and return. It is Java's KIP-863
`deserialize(String, Headers, ByteBuffer)` and the core's `deserialize_with_headers_bytes(&Bytes)`.

Decisions already taken with the user:

1. `BytesMemoryManager` references the batch for its whole life. `Pin` / `Unpin` only defer the
   native destroy; the reference is never dropped at refcount 0.
2. The async deserializer's memory is call-scoped by default (valid until the returned
   `ValueTask` completes, then revoked). The marker `IAsyncDeserializerWithRetention<T>`, with no
   methods of its own, makes the same method receive a memory it may keep or return.
3. Release is GC-only: `ConsumerRecords` stays non-disposable, with `GC.AddMemoryPressure`.
4. `using var m = record.Value()` cannot compile (`ReadOnlyMemory` is not `IDisposable`); the
   documented way to hold a span safely is `using MemoryHandle pin = memory.Pin();`.

## Design

### Ownership

- **`SafeConsumerRecordsHandle`** (new, `Internal/Interop/`, on `SafeHandleZeroIsInvalid`):
  owns one native `kafka_consumer_ConsumerRecords_t`. `ReleaseHandle` calls the destroy import
  `ConsumerRecordsMarshal.Destroy` uses today and removes the memory pressure it added. The
  destroy is a plain `Box` drop, independent of the consumer, so it is safe on the finalizer
  thread and after the consumer is disposed.
- **`BytesMemoryManager : MemoryManager<byte>`** (new, internal, sealed): the handle, a pointer
  and a length.
  - `GetSpan()` returns the span over the native bytes.
  - `Pin` is `DangerousAddRef` and `Unpin` is `DangerousRelease`, so a pin taken by the producer
    or by the user keeps the batch alive even when nothing else references it.
  - `Dispose(bool)` does nothing.
  - A revocable variant (a `volatile bool`) makes `GetSpan` and `Pin` throw
    `ObjectDisposedException` once revoked; pins taken before revocation stay valid until `Unpin`.
  - It is handed out only as `ReadOnlyMemory<byte>`. A zero-length value is
    `ReadOnlyMemory<byte>.Empty` with no manager; a null key or value is handled as today.
- **Batch release rule.** A client whose deserializers are all span-based keeps today's path: raw
  `IntPtr`, destroyed in `finally`, no handle and no manager. A client with a memory-path
  deserializer wraps each non-empty polled batch in a `SafeConsumerRecordsHandle`:
  - no retainable memory handed out (only call-scoped ones, or none): `Dispose()` at the end of
    the poll, deterministic, deferred by any open pin;
  - any retainable memory handed out: never disposed explicitly, also on a deserialization
    failure or cancellation (a deserializer may already have kept one). `GC.AddMemoryPressure`
    with the sum of the batch's key and value lengths, computed in the copy-out loop; the GC
    releases it.
- `_recordsHeld` in `NativeConsumer` keeps counting only the copy-out scope, never a retained
  batch, so deserializer disposal is not delayed by retained memory.

### Public surface (`Common/Serialization/`)

```csharp
// Sync, retainable. Java's deserialize(String, Headers, ByteBuffer).
public interface IDeserializerWithReadOnlyMemory<T> : IDeserializer<T>
{
    T? Deserialize(string topic, Headers headers, ReadOnlyMemory<byte> data);
}

public interface IAsyncSerializer<T>
{
    ValueTask<ReadOnlyMemory<byte>?> Serialize(
        string topic, Headers headers, T? data, CancellationToken cancellationToken);
}

public interface IAsyncDeserializer<T>
{
    // `data` is valid until the returned ValueTask completes.
    ValueTask<T?> Deserialize(
        string topic, Headers headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken);
}

// Marker: `data` may be kept and returned.
public interface IAsyncDeserializerWithRetention<T> : IAsyncDeserializer<T> { }

// Synchronous Configure, as on the sync interfaces.
public interface IAsyncSerializerWithConfigure<T> : IAsyncSerializer<T> { /* Configure(configs, isKey) */ }
public interface IAsyncDeserializerWithConfigure<T> : IAsyncDeserializer<T> { /* Configure(configs, isKey) */ }

// One async class per built-in, named Async + the sync class's name:
public sealed class AsyncStringSerializer : IAsyncSerializerWithConfigure<string> { }
public sealed class AsyncStringDeserializer : IAsyncDeserializerWithConfigure<string> { }
public sealed class AsyncReadOnlyMemoryDeserializer : IAsyncDeserializerWithRetention<ReadOnlyMemory<byte>?> { }
// ... Boolean, ByteArray, Double, Float, Integer, Long, ReadOnlyMemory, Short, String, UUID, Void
```

- Java's method names, no `Async` suffix, `ValueTask` per record, one method per async interface
  with `Headers` always passed (so no `WithHeaders` async variant).
- `ReadOnlyMemory<byte>` stands for Java's `ByteBuffer`, as the Java-names analyzer already
  matches them. It has no position or limit: `data` is always exactly the key or the value, and
  the interface docs say so. Its `Equals` is identity, so tests compare bytes with
  `Span.SequenceEqual`.
- Precedence on the sync consumer: `IDeserializerWithReadOnlyMemory<T>` wins over
  `IDeserializerWithHeaders<T>`, which wins over the plain span method, as Java's consumer calls
  only the `ByteBuffer` overload. The capability is tested once at construction.
- **Rename**: Java's `ByteBuffer` family takes the .NET type's name. `ByteBufferSerializer` →
  `ReadOnlyMemorySerializer`, `ByteBufferDeserializer` → `ReadOnlyMemoryDeserializer`,
  `Serdes.ByteBufferSerde` → `Serdes.ReadOnlyMemorySerde`, `Serdes.ByteBuffer()` →
  `Serdes.ReadOnlyMemory()`; the async classes are `AsyncReadOnlyMemorySerializer` /
  `AsyncReadOnlyMemoryDeserializer`. No alias for the old names (the package is `4.4.0-alpha.1`).
  This deviates from the Java names rule (CKRDA0013 rule 1), so it needs entries in
  `analyzers/java-name-allowlist.txt` and a stated deviation (S0). The other built-ins keep
  Java's names (`UUIDSerializer`, `IntegerSerializer`, ...).
- `ReadOnlyMemoryDeserializer` implements `IDeserializerWithReadOnlyMemory<ReadOnlyMemory<byte>?>`
  and returns `data` as it is. Its span method keeps copying. `ByteArrayDeserializer` stays the
  copying one.
- Async built-ins: 11 pairs of `Async<Name>Serializer` / `Async<Name>Deserializer`, each
  delegating to its sync class, so there is one implementation of each byte format. There is no
  public adapter and no `AsAsync()`: a user's own sync serde gets its own async class.
- Each async built-in exposes its sync inner through an internal interface. The async clients
  test it once at construction and unwrap: an inner span deserializer runs on today's span path
  (no manager, no state machine), and `AsyncReadOnlyMemoryDeserializer`'s inner
  `IDeserializerWithReadOnlyMemory<T>` runs on the retainable path. This is private to the
  binding; a user class never takes it.
- Close: an async client disposes an async serde through `IAsyncDisposable`, else `IDisposable`.
  `Configure` stays the user's call, as today (no client calls it).
- `ISerde<T>` is unchanged in this phase; `Serdes` changes only by the rename above.
- `SerdeGuard` / `DefaultMethodGuard` gain the equivalent construction checks: a type declaring
  `Deserialize(string, Headers, ReadOnlyMemory<byte>)` without `IDeserializerWithReadOnlyMemory<T>`
  is refused, and a `Close()` / `DisposeAsync()` the client would not call is refused.

### Clients

- **Sync consumer** (`KafkaConsumer`, `MockConsumer`; `NativeConsumer.PollTyped`,
  `ConsumerRecordsMarshal.CopyOut`): add the memory path next to the span one in
  `RecordReader.Deserialize<T>`; apply the batch release rule.
- **Async consumer** (`AsyncKafkaConsumer`, `AsyncMockConsumer`; `NativeConsumer.Async.cs`
  `CompletePoll`, already `async Task`): constructors take `IAsyncDeserializer<TKey/TValue>` only.
  - Both deserializers async built-ins over span deserializers: today's `CopyOut`, unchanged.
  - Otherwise an awaiting loop, one record at a time, key then value. A call-scoped deserializer
    gets a fresh revocable manager per call (never reused across records, since a stale
    `ReadOnlyMemory` would otherwise read the next record), revoked in `finally`; a
    `WithRetention` one gets the batch-rooted manager.
  - The consumer's use stays released during deserialization, as today, so a deserializer may
    dispose the consumer. A failure or cancellation seeks back to the first record not returned,
    as today (`FirstPositions`, `SeekBackAsync`).
- **Async producer** (`AsyncKafkaProducer`, `AsyncMockProducer`): constructors take
  `IAsyncSerializer<TKey/TValue>` only. `SendValidated` keeps today's non-async path when both
  serializers are async built-ins (a serializer throw stays synchronous, no state machine). Otherwise an
  async path awaits the key then the value serializer, seals the headers, then `SendViaPump`.
  Records then enter the producer in the order their serialization completes; a caller that needs
  order awaits `Send`'s first stage before the next `Send`. A serializer failure faults the
  returned `ValueTask`.
- **Sync producer**: unchanged.
- **Consume then produce**: a value from `ReadOnlyMemoryDeserializer` sent through
  `ReadOnlyMemorySerializer` is pinned by the producer, so the batch outlives the
  `ConsumerRecord` until the producer has taken the record, with no copy in the binding.

## Steps

Each step builds, passes `dotnet test` and is one commit.

- **S0 — plan and rule drafts (Manager)**: this file, with the rule drafts R1–R5 below. The rule files
  themselves need the owner's authorization and are not edited by a step. They cover:
  `CLAUDE.md` §6.4 and the serde section's "Async serde deferred" note;
  `.claude/rules/ffi-marshalling.md` §B2 (Category 3 keep-alive) and §B4 (stored native-backed
  `ReadOnlyMemory` as an anti-pattern, now allowed only through `BytesMemoryManager`); the
  `ByteBuffer` → `ReadOnlyMemory` rename as a deviation in `CLAUDE.md` "Java names (CKRDA0013)"
  and the `Serdes` sketch, with its `analyzers/java-name-allowlist.txt` entries; a decision
  ledger entry; the phase plan at `design/history/M<m>/P<n>-async-serdes-zero-copy/PLAN.md`
  following `design/history/M23/P1-serializer-readonly-memory/PLAN.md`; `STATUS.md`.
- **S1 — sync zero-copy**: `SafeConsumerRecordsHandle`, `BytesMemoryManager`,
  `IDeserializerWithReadOnlyMemory<T>`, the `ByteBuffer` → `ReadOnlyMemory` rename (two classes,
  `Serdes.ReadOnlyMemorySerde`, `Serdes.ReadOnlyMemory()`, their tests and the analyzer
  allowlist), `ReadOnlyMemoryDeserializer`'s memory method, `ConsumerRecordsMarshal`,
  `NativeConsumer.PollTyped`, `SerdeGuard`; docs on `ConsumerRecord` / `ConsumerRecords`
  ("holds owned values only" no longer holds on the memory path).
- **S2 — async interfaces and built-ins**: the five interfaces, the 22 `Async*` built-in
  classes with the internal unwrap interface, guards; Java's serde test vectors run against both
  the sync and the async class.
- **S3 — async consumer**: constructors, `CompletePoll`, `AsyncMockConsumer`.
- **S4 — async producer**: constructors, `SendValidated`, `SerializedProducerRecord`,
  `AsyncMockProducer`.
- **S5 — callers**: every `new Async(Kafka|Mock)(Producer|Consumer)<` passes the `Async*`
  built-in (`new StringSerializer()` becomes `new AsyncStringSerializer()`), and the test serdes
  in `TestSerializers.cs` / `TestDeserializers.cs` get async forms:
  `tests/Confluent.Kafka.UnitTests` (about 84 files), `IntegrationTests`, `GrpcServer.UnitTests`,
  `grpc-server`, `Performance/PerfV3`, `soak/SoakClient`, `SoakClient.Tests`, examples and any
  analyzer list of public names.

## Reused code

- `Internal/Interop/SafeHandleZeroIsInvalid.cs` and the existing `Safe*Handle` classes for the
  handle shape; `ConsumerRecordsMarshal.Destroy` / `FirstPositions`; `NativeConsumer.HoldRecords`
  / `DestroyRecords` / `SeekBack` / `SeekBackAsync`.
- `SerdeGuard.Check`, `DefaultMethodGuard.Declares` / `NeverCalled` / `TypeText`.
- The producer's existing `MemoryManager.Pin` handling (`ISerializer` contract, M23/P1).
- Tests: `tests/Confluent.Kafka.UnitTests/NativeMemoryManager.cs`, `TestDeserializers.cs`,
  `TestSerializers.cs`, and the existing suites listed under Verification.

## Verification

New unit tests (mocks, no broker), asserting message content where an exception is expected:

- A `ReadOnlyMemoryDeserializer` value is readable after `Poll` returns, after the next `Poll`, after
  the consumer is disposed, and when held without its record
  (`records.Select(r => r.Value()!.Value).ToList()`); its bytes are the native ones (no copy).
- A pin defers the destroy: dispose the handle with a pin open, read through the pointer, unpin,
  the handle is released. Pin after release throws `ObjectDisposedException`.
- A call-scoped async memory read after its `ValueTask` completed throws; a `WithRetention` one
  does not.
- Span-only polls create no handle and no manager: `PublicConsumerTypedAllocationBudgetTests` and
  `PublicSyncConsumerAllocationBudgetTests` unchanged; new budgets for the memory path (one
  manager per non-null, non-empty key or value) and for the async built-in path (equal to today's).
- Failure and cancellation mid-batch on the async path seek back (`RecordDeserializationTests`,
  `PollDeserializesAfterItsUseTests`, `PublicConsumerDeserializeThreadTests` extended); a
  deserializer disposing the consumer mid-batch still works.
- Async producer: an awaiting serializer, a failing one (faulted `ValueTask`), cancellation,
  headers sealed after serialization, built-in path throws synchronously.
- Consume then produce with zero copy, the record dropped before the send completes.
- Guards: each refused shape, with its message.
- TFM smoke on net462, net8.0, net10.0.

Commands (from the worktree; `dotnet/CLAUDE.md` §7.2, §7.5):

- `cargo build --features ffi` in `rust/`, then `dotnet build`, `dotnet test`, `dotnet format`
  in `dotnet/`.
- `make -C dotnet test-grpc-server-dotnet`.
- PerfV3 A/B against `master` for the consumer span path and the async built-in path (no
  regression expected), plus the memory path against `master`'s copying `ByteBufferDeserializer`.
- `make verify-dotnet` from the repository root is the gate, run by the user.

## Out of scope

- Per-field retention through a `kafka_Bytes_t` clone entry point (each field is already a
  refcounted `bytes::Bytes` in the core). Retention is batch-rooted here: one kept value keeps its
  whole batch until the GC runs. Follow-up if profiling shows it matters.
- Zero-copy headers (still copied), `IAsyncSerde` / an async `Serdes` factory, and an awaited
  `Configure`.
- A generic public wrapper that turns any `ISerializer<T>` / `IDeserializer<T>` into its async
  interface (the dropped `AsAsync()`). Not shipped in this phase: the 22 `Async*` built-ins cover
  the binding's own serdes, and a user's own sync serde is used with an async client through a
  hand-written async class that calls it and returns a completed `ValueTask`. Such a class takes
  the awaiting path (one revocable manager per key and per value on the consumer), not the
  internal span fast path. Adding the wrapper later is additive.
- Python, C and Rust surfaces: no FFI entry point is added.

## Execution notes (added at approval)

- **A step never leaves a project that does not build.** S3 and S4 change public constructors, so each
  updates, in its own commit, every caller of the constructors it changes, in every project of the repository
  (unit tests, integration tests, `grpc-server`, `GrpcServer.UnitTests`, `soak`, `Performance`, `examples`,
  `tools`). S5 is then what is left: the async forms of the test serdes not yet needed, docs, README, and the
  full gates.
- **Deviations** made during execution are appended to the local file `dotnet/COMMENTS.DONE.129.md`, which is
  never committed at the binding root (`CLAUDE.md` §8.4); the Manager archives it here at the close-out.

## Gates per step

Run from the worktree, before the step's commit:

1. `cargo build --features ffi` in `rust/` (only if the native library is missing or `rust/` changed).
2. `dotnet build Confluent.Kafka.sln -c Release` in `dotnet/`: 0 warnings, 0 errors, no CKRDA0013 / CKRDA0014.
3. Every project outside the solution that the step touched builds (`grpc-server`, `soak`, `Performance`,
   `examples`, `tools`).
4. `dotnet test` of `tests/Confluent.Kafka.UnitTests` on net8.0 and net10.0: 0 failed.
5. The analyzer tests when an analyzer list or the analyzer changed; `make -C dotnet test-grpc-server-dotnet`
   and `SoakClient.Tests` when their projects changed.
6. `dotnet format --verify-no-changes`.

`make verify-dotnet` (repository root) is the close gate, run by the owner.

## Ledger rows (Agreed 2026-11-18)

| # | Source | Topic | Decision | Status |
|---|---|---|---|---|
| 535 | M23/P7 plan D1 | Batch ownership | `BytesMemoryManager` references the batch for its whole life; `Pin` / `Unpin` only defer the native destroy | Agreed |
| 536 | D2 | Async deserializer memory | call-scoped (revoked when the `ValueTask` completes); the marker `IAsyncDeserializerWithRetention<T>`, with no methods, allows keeping or returning it | Agreed |
| 537 | D3 | Release | GC only: `ConsumerRecords` is not `IDisposable`; `GC.AddMemoryPressure` for a retained batch | Agreed |
| 538 | D4 | Async built-ins | one `Async<Name>Serializer` / `Async<Name>Deserializer` class per built-in; no public `AsAsync()` or generic adapter | Agreed |
| 539 | D5 | Async clients | the four async clients take only `IAsyncSerializer<T>` / `IAsyncDeserializer<T>` (breaking) | Agreed |
| 540 | D6 | `ByteBuffer` names | `ReadOnlyMemorySerializer`, `ReadOnlyMemoryDeserializer`, `Serdes.ReadOnlyMemorySerde`, `Serdes.ReadOnlyMemory()` and their `Async` classes; no alias (breaking); a Java-names deviation | Agreed (the owner named the deserializer; the rest of the family follows it) |
| 541 | D7 | Headers | still copied; zero-copy headers out of scope | Agreed |
| 542 | D8 | Rule text R1–R5 | drafts below; applied only on the owner's explicit OK | Open |
| 543 | D9 | Logistics | M23/P7, N=129, S0–S5, this branch; the Actor commits each step with `--no-verify` after the step's gates; one Critic loop at the end | Agreed |

## Ledger rows added at close-out (2026-11-18, Proposed)

Decisions taken during execution and review. None is confirmed by the owner yet; the details are in
`COMMENTS.DONE.129.md` beside this plan.

| # | Source | Topic | Decision | Status |
|---|---|---|---|---|
| 544 | S4 note 1; Critic B Q1, C Q3 | An awaited serializer's `ApiException` | on `AsyncKafkaProducer` it fails the record through the future `Send` hands back, as the built-in path and Java's `KafkaProducer.doSend` do; every other serializer failure fails `Send`'s first stage. Replaces the plan's sentence "A serializer failure faults the returned `ValueTask`" | Proposed |
| 545 | S4 note 2; Critic B Q2 | A send cancelled, or whose producer is closed, during the awaited serialization | not sent; on `AsyncKafkaProducer` its headers are already read-only; documented in the README and on `IAsyncProducer` | Proposed |
| 546 | S2 | `Configure` on the async interfaces | public `AsyncSerializerExtensions.Configure` / `AsyncDeserializerExtensions.Configure`, the shape of the sync ones; a class implementing a sync and an async interface must qualify the call | Proposed |
| 547 | Critic B2 | No `Deserialize(string, Headers, ReadOnlyMemory<byte>)` extension on `IDeserializer<T>` | dropped: beside the span form it made a call with a `byte[]` ambiguous (CS0121) before C# 14 and with an `ArraySegment<byte>` always; the async built-ins call the span form | Proposed |
| 548 | S3 note 1; Critic B1; round 2 Q1 | Disposing an async serde | `IAsyncDisposable` first, else `IDisposable`, once, on every path; a synchronous `Dispose()` and a failed constructor wait on the calling thread; a serde's `DisposeAsync()` is always started with no synchronization context and the default task scheduler current, on the awaited route too | Proposed |
| 549 | S3 note 6 | Mock pin safety | `SafeConsumerRecordsHandle.Lent` and `Dispose(Action)`: a lent mock batch counts as retained until its real release, so a pin on call-scoped memory never points at freed memory | Proposed |
| 550 | Critic A1 | Sync producer and a `MemoryManager`-backed key or value | the blocking send takes the manager's own pin for the native call; an array-backed memory takes none | Proposed |
| 551 | Critic C Q1 | PerfV3's async consumer arm | it runs the awaited path (`ByteCountDeserializer` is a user class), so the A/B of the async built-in path has no consumer arm; A/B against master not run | Open |
| 552 | Critic C Q2 | net462 | the library and the unit tests build for net462; no test of this phase ran on it on the development host | Open |

## Draft rule text (not applied)

Each draft replaces the named text. Until it is applied, this plan wins where the two disagree.

### R1 — `CLAUDE.md` §6.4, the paragraph "Deferred alternative — keep-alive (zero-copy)"

**Keep-alive (zero-copy), through `BytesMemoryManager` only (M23/P7, ledger 535–537).** A deserializer that
takes the memory form (`IDeserializerWithReadOnlyMemory<T>`, `IAsyncDeserializerWithRetention<T>`) receives a
`ReadOnlyMemory<byte>` over the batch that it may keep or return. The memory's `BytesMemoryManager` references
a `SafeConsumerRecordsHandle`, so the batch lives as long as any such memory is reachable, and a `Pin` defers
the native destroy. Release is by the GC; `ConsumerRecords` is not `IDisposable`. A span taken from the memory
does not keep the batch alive: hold `using MemoryHandle pin = memory.Pin();` while reading it. A kept value
keeps its whole batch. Copy-out stays the default: a span deserializer and `ByteArrayDeserializer` are
unchanged, and a client with only span deserializers creates no handle and no manager. No other code may store
a `ReadOnlyMemory<byte>` over native batch memory.

### R2 — `CLAUDE.md` §3, the bullet "Async serde deferred"

- **Async serdes (M23/P7, ledger 536, 538, 539).** `IAsyncSerializer<T>` and `IAsyncDeserializer<T>` have one
  method each, with Java's name, the `Headers` always passed, a `CancellationToken`, and a `ValueTask` result.
  The async clients take only these; the sync clients only the sync interfaces. An async deserializer's `data`
  is valid until its `ValueTask` completes, unless the class also has the marker
  `IAsyncDeserializerWithRetention<T>`. Each built-in has an `Async<Name>Serializer` / `Async<Name>Deserializer`
  class that delegates to the sync one; there is no public sync-to-async adapter.

### R3 — `CLAUDE.md` §3 "Java names (CKRDA0013)", after rule 1, and the `Serdes` sketch

Deviation (M23/P7, ledger 540): Java's `ByteBuffer` serde family is named after the .NET type that stands for
`ByteBuffer`: `ReadOnlyMemorySerializer`, `ReadOnlyMemoryDeserializer`, `Serdes.ReadOnlyMemorySerde`,
`Serdes.ReadOnlyMemory()`. The allowlist carries the `unmatched` / `missing` pairs.

Sketch line: `public static ISerde<ReadOnlyMemory<byte>?> ReadOnlyMemory();   // the deserializer's memory form does not copy (§6.4)`

### R4 — `ffi-marshalling.md` §B2, Category 3, the sentence starting "**Keep-alive**"

**Keep-alive** (hold the root in a `SafeConsumerRecordsHandle`, hand out views through `BytesMemoryManager`,
destroy when the GC releases the handle and no pin is open) is used for the memory-form deserializers only;
see CLAUDE.md §6.4.

### R5 — `ffi-marshalling.md` §B4, the paragraph "Raw bytes are the one copy-out case" and the first anti-pattern

Raw bytes are copy-out through a span deserializer or `ByteArrayDeserializer`, and zero-copy through a
memory-form deserializer, whose `ReadOnlyMemory<byte>` is backed by a `BytesMemoryManager` that keeps the
batch alive (CLAUDE.md §6.4).

Anti-pattern: a stored `ReadOnlyMemory<byte>` over native batch memory that is not backed by a
`BytesMemoryManager` (use-after-free); a span kept past its `MemoryHandle`; eager copies where a
`ReadOnlySpan<byte>` deserialize read would do.
