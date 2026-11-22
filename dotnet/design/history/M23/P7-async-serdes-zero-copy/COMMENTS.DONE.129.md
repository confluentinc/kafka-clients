## S1

Deviations and decisions of step S1 (sync zero-copy), each against the approved plan.

- **One manager class, two variants.** `BytesMemoryManager` serves the retained memory and, through `Revoke()`, the
  revocable one S2 needs. Why: the two differ only in one flag; a second class would duplicate `Pin` / `Unpin`.
- **The handle is created inside `CopyOut`.** `ConsumerRecordsMarshal.CopyOut` takes `ref SafeConsumerRecordsHandle?`
  and creates it only when the batch has records and a deserializer has the memory form. Why: an empty poll returns a
  non-null native batch, and a span-only client must create no handle. A batch from which nothing was handed out
  (every key and value null or empty) is disposed at once, since nothing can reference it.
- **Mock record bytes.** The mock's keys and values live in `_recordBytes` (HGlobal blocks), so `NativeConsumer`
  counts retained batches (`_recordsRetained`) and the handle's `ReleaseHandle` calls back to free the blocks. Only
  the mock passes the callback; a real consumer's handle holds no reference to the consumer.
- **Guard on all four consumers.** The stray-shape refusal sits in the shared `SerdeGuard.Check`, so the async
  consumers refuse it too although they do not call the memory form before S3.
- **No `DeserializerExtensions` overload for the memory form.** The plan lists only the interface. Open point for the
  owner: rule 10 style would add `Deserialize(this IDeserializer<T>, string, Headers, ReadOnlyMemory<byte>)`.
- **Released / revoked messages.** `GetSpan` and `Pin` throw `ObjectDisposedException` with
  `BytesMemoryManager.ReleasedMessage` once the batch is released, `RevokedMessage` once revoked. `SafeHandle` sets
  `IsClosed` only when the last reference goes, so a `Dispose` with a pin open leaves spans and new pins working
  until the last pin is released (tested).
- **Memory pressure** is the sum of key and value lengths of the records visited; after a mid-batch failure the
  unvisited rest is not counted.
- **`#pragma warning disable CKRDA0006`** around `DangerousAddRef` in `Pin` (precedent: `SafeKafkaFutureHandle`): the
  matching release is `Unpin`, not a `finally`.
- **Allowlist, not pairs file.** The rename's `unmatched` / `missing` lines are in `analyzers/java-name-allowlist.txt`
  as the plan's draft text says; `java-name-pairs.txt` would also give member checks. Added
  ``unmatched T:...IDeserializerWithReadOnlyMemory`1`` because the analyzer maps Java's defaults to
  `IDeserializerWithHeaders`.
- **Gates ran with `-p:CargoProfileDir=debug`**: the worktree has only the debug native library.
- **`kafka` submodule initialised in the worktree** (`git submodule update --init --depth 1 kafka`, same commit as the
  index): `PublicNamespaceLayoutTests` reads the Java sources and 429 cases failed without it.
- **Not edited (rule and design files):** `dotnet/CLAUDE.md` and `design/current/STATUS.md` still say `ByteBuffer()`
  for the serde factory; the interface summary of `IDeserializerWithReadOnlyMemory<T>` says an asynchronous consumer
  does not call it, which S3 must revise.

## S2

- **Two interface files, not five.** `IAsyncSerializer.cs` holds `IAsyncSerializer<T>`, `IAsyncSerializerWithConfigure<T>`
  and `AsyncSerializerExtensions`; `IAsyncDeserializer.cs` holds `IAsyncDeserializer<T>`,
  `IAsyncDeserializerWithRetention<T>`, `IAsyncDeserializerWithConfigure<T>` and `AsyncDeserializerExtensions`. This is
  how `ISerializer.cs` / `IDeserializer.cs` are laid out.
- **Two public extension classes the plan does not list.** `AsyncSerializerExtensions.Configure` and
  `AsyncDeserializerExtensions.Configure` (the `WithConfigure` interface when implemented, else nothing). Rule 10
  pairs a `With<Method>` interface with an extension, and the Java-names analyzer reported
  `missing ... #configure(Map,boolean)` on both async interfaces without them. Open point for the owner: the
  alternative is two allowlist lines and no extension.
- **A built-in throws from the call itself.** `new ValueTask<T>(inner(...))` evaluates the sync class before the
  `ValueTask` exists, so its exception (`SerializationException`, `IllegalArgumentException` for Void) is thrown
  synchronously by `Serialize` / `Deserialize`, never carried by a faulted `ValueTask`. The token is not read. The
  interface docs allow both ("thrown by the call or carried by the returned ValueTask").
- **Unwrap interfaces.** `Confluent.Kafka.Internal.IAsyncBuiltInSerializer<T> { ISerializer<T> Inner { get; } }` and
  `IAsyncBuiltInDeserializer<T> { IDeserializer<T> Inner { get; } }`, implemented explicitly by the 22 classes.
- **Guards.** `SerdeGuard.Check<T>(IAsyncSerializer<T>, string client, bool disposes)` and the `IAsyncDeserializer<T>`
  overload. With `disposes`, refused: a public `DisposeAsync()` without `IAsyncDisposable`, a public `Dispose()` without
  `IDisposable`, a public `Close()` with neither interface. A serde with both interfaces is taken although its
  `Dispose()` is then not called. No `Configure` check, as the sync guard has none. No client calls them before S3/S4.
- **`DeserializerExtensions.Deserialize(..., ReadOnlyMemory<byte>)` added** (the S1 open point). Allowlist line
  ``missing T:...IDeserializer`1 ...Deserializer#deserialize(String,Headers,ByteBuffer)`` went stale (CKRDA0014) and is
  deleted; the nine built-in `missing ... #deserialize(String,Headers,ByteBuffer)` lines are still needed and stay.
  **Source break to weigh:** with C# 13 or earlier (the default for net462 / netstandard2.0 callers), a call
  `deserializer.Deserialize(topic, headers, byteArray)` on an `IDeserializer<T>` is now ambiguous (CS0121) between the
  span and the memory extension; C# 14 picks the span one (checked with a two-overload sample on both versions). The
  repository builds with `LangVersion=latest`, so nothing here is affected.
- **Required `CancellationToken`.** The analyzer skips only an optional trailing token, so each async `Serialize` /
  `Deserialize` has an `overload` allowlist line (2 interfaces, 20 built-ins), with the `missing` lines for Java's
  forms that have no async counterpart. The two `AsyncReadOnlyMemory*` classes and the marker are `unmatched` lines.
- **Interface docs state the plan's client contract** (an asynchronous client takes these, disposes through
  `IAsyncDisposable` or else `IDisposable`, retention keeps the batch): no client does so before S3/S4.
- **`PublicNamespaceLayoutTests`**: 29 `=` rows for the new exported types.
- **Gates ran with `-p:CargoProfileDir=debug`.**

## S3

Deviations from the plan's S3 text, each with its reason.

1. **Disposal of an async deserializer on a synchronous path.** One order on every path: `IAsyncDisposable`
   first, else `IDisposable`, each once. The asynchronous paths (`Close`, `DisposeAsync`, the end of the last
   poll's hold) await `DisposeAsync()`. The synchronous paths (`Dispose()`, the constructor when the core
   refuses the configuration) call `DisposeAsync()` and wait for it on the calling thread. Rejected: preferring
   `IDisposable` on the synchronous path when a serde has both, which would give one serde two disposal routes
   depending on how the consumer is closed.
2. **`AsyncMockConsumer` keeps the guard call** (`SerdeGuard.Check(..., disposes: false)`), although with
   `disposes: false` it refuses nothing today; it keeps the mock on the same construction path as the real one.
3. **`analyzers/java-name-baseline.txt` changed too**, not only the allowlist: the replaced constructors'
   baseline lines went with them.
4. **`NativeConsumer.PollWithCallback` takes `AsyncDeserializerCall<T>`s** (the capability test's result),
   so three Interop test files that call it directly were updated.
5. **`CompletePoll` awaits `ReleaseRecordsAsync` in its `finally` on every path**, the built-in one included,
   so the deserializers handed over by a dispose under the poll are disposed through `IAsyncDisposable` there.
6. **Mock pin safety (not in the plan).** A pin taken on a call-scoped (lent) memory may outlive the poll and
   the consumer; the mock's record bytes are HGlobal blocks freed at destruction. Added
   `SafeConsumerRecordsHandle.Lent` and `Dispose(Action released)`, and `NativeConsumer.ReleaseBatch` counts a
   lent mock batch as retained until its real release, so the pin never points at freed memory.
7. **Guard tests split.** The sync `SerdeGuardTests` cover the sync consumers only; the async consumers'
   cases are in `AsyncSerdeGuardTests`. `PublicDeserializerDisposalTests
   .ADeserializerThatIsOnlyIAsyncDisposable_IsNotDisposed` is now the sync flavor only: the async consumer does
   dispose such a deserializer (`AsyncDeserializerDisposalTests`).
8. **Test adapter.** `AsyncDeserializerOver<T>` / `.AsAsync()` (unit tests only) wraps a test's sync
   deserializer; a wrapped one is a user class, so it runs on the awaited, call-scoped path, not the built-in
   one. `RecordDeserializationTests` is sync-only, so its async counterparts are in the new
   `AsyncConsumerDeserializationTests`.
9. **Built-in path budget measured process-wide** (`GC.GetTotalAllocatedBytes`, best of 8, serial collection):
   no user code runs on the copy-out thread to sample a per-thread counter from. Measured 260 B per record
   against the sync poll's 256 B, under the same 448 B bound. The awaited path is sampled per thread inside the
   deserializer: 80 B per record, two 40 B managers.
10. **Deserializers outside the unit tests implement both interfaces** rather than getting a second class:
    `SoakRecordDeserializer` (soak), `ByteCountDeserializer` (PerfV3) and the integration tests'
    `HeaderReadingDeserializer`; `SlowDeserializer` (integration) is async only. As user classes they run on
    the awaited path, one 40 B manager per key or value with bytes, so PerfV3's async consumer numbers include
    that.
11. **Gates run by their recipe lines**, not through `make`, to pass `-p:CargoProfileDir=debug`: the `make`
    targets depend on `build-dotnet`, which builds the release native library. `tools/PackageSmoke` was not
    built: it consumes the packed nupkg and constructs no async consumer.

## S4

The async producers take `IAsyncSerializer<TKey>` / `IAsyncSerializer<TValue>` only. Deviations and notes:

1. **An `ApiException` from an awaited serializer on `AsyncKafkaProducer` still fails the record through its
   future** (`FutureFailure.SendAsync`, the callback running on the thread that ended serialization), as the
   built-in path does, instead of faulting `Send`'s first stage. Every other throw on the awaited path ends
   the first stage. For the Critic to confirm against the plan.
2. **On the awaited path every `Send` exception ends the first stage**, except `ArgumentNullException` and
   the closed check made before the call, which stay synchronous. Cancellation and the closed state are
   checked again in `SendViaPump` after the await; on the Kafka producer the headers are read-only by then.
3. **`InvalidCastException` on the Kafka producer's awaited path is wrapped in `SerializationException`**
   with the built-in path's message; the mock leaves it unchanged, as the sync mock does.
4. **Mock.** Each awaited serializer gets a new, empty `Headers` that is dropped, and the record's headers
   stay writable, as Java's `MockProducer`. Every throw ends the first stage unchanged. `AsyncMockProducer`
   keeps the guard call with `disposes: false` and disposes no serializer.
5. **Disposal.** `NativeProducer.DisposeTheSerializersAfter` uses `DisposeOnceAsync`: `Close` and
   `DisposeAsync` await it, the sync `Dispose` and a failed constructor block on it. One instance given as
   key and value is disposed once per side, as on the sync producer.
6. **`analyzers/java-name-baseline.txt` changed** (not the allowlist): the mock constructor's lines.
7. **The sync producer's `fixed`-span send does not call `MemoryManager.Pin`.** Out of this step's scope;
   flagged for the Critic / S5.
8. **Test shape.** The mock's metadata has serialized sizes 0, so the bytes sent are asserted through the
   internal `SerializedProducerRecord.SerializeAsync` and `SendPins`, not through metadata. The test-only
   `MemorySerializer` at async sites is replaced by `AsyncReadOnlyMemorySerializer`; `AsyncSerializerOver<T>`
   / `.AsAsync()` (unit tests only) wraps a test's sync serializer, which then runs on the awaited path.
   Async-producer guard cases moved from `SerdeGuardTests` to `AsyncSerdeGuardTests`.
9. **Serializers outside the unit tests implement both interfaces**: `SoakRecordSerializer` (soak), the
   integration tests' `HeaderAddingSerializer`, the unit tests' `InvalidCastSerializer`. As user classes
   they run on the awaited path, so the soak's async producer now does.
10. **Awaited-path allocation budget 544 B per send** on the mock with completed serializers: measured 536 B
    on net8.0 and net10.0, the built-in path's 472 B plus the mock's two 32 B `Headers`. The built-in path's
    budgets (480 B, 512 B) are unchanged.
11. **Gates run by their recipe lines** with `-p:CargoProfileDir=debug`. `tools/PackageSmoke` was not built
    (it consumes the packed nupkg and constructs no async producer). net462 tests are not runnable on this
    host.

## S5

1. **PerfV3's async producer is on the built-in path, not the awaited one.** The step text asked the
   notes to say the async client runs the awaited path at 544 B per send on the producer. PerfV3 gives
   its async producer two `AsyncByteArraySerializer`s, so it runs the built-in path; only its async
   consumer (`ByteCountDeserializer`) runs the awaited path (one 40 B manager per key and per value with
   bytes). `tests/Performance/README.md` says that. The soak's producer does run the awaited path
   (`SoakRecordSerializer`); `soak/README.md` quotes the 544 B budget as what it is, the unit-test
   budget on the mock with two such serializers (480 B with two built-ins).
2. **No .NET changelog exists** (`rust/CHANGELOG.md` and `python/CHANGELOG.md` only), so the two breaks
   are a subsection of the package README, "Breaking changes since 4.4.0-alpha.1" (the tag
   `dotnet/v4.4.0-alpha.1` still has the `ByteBuffer*` names).
3. **`soak/README.md` payload paragraph**: besides the interface name, the sentence said a throwing
   deserializer "is wrapped in a `SerializationException` that faults the whole `Poll`, turning one
   corrupt payload into a lost fetch batch". That contradicted row 2 of the same file and the client
   (records before it are returned, then `RecordDeserializationException`, position left on the bad
   record), so it now says what row 2 says.
4. **`ConsumerRecord` remarks** revised (public XML doc): they named only the synchronous memory form;
   they now name `IAsyncDeserializerWithRetention<T>` and `AsyncReadOnlyMemoryDeserializer` too.
5. **Tests added (3)**: `ZeroCopyDeserializationTests.TheValuesOfOnePoll_StayReadable_HeldAsAListWithoutTheirRecords`
   (the plan's `records.Select(r => r.Value()!.Value).ToList()` shape, past the next poll and the
   consumer's disposal); `ConsumerRecordsMarshalNextOffsetsTests.ACopyOutWithNoMemoryForm_CreatesNoBatchHandle_AndLeavesTheNativeBatchToItsCaller`
   and `.ACopyOutWithAMemoryForm_CreatesTheBatchHandle_AndGivesEachValueThroughItsOwnManager` (the
   existing allocation budgets are too loose to show "no handle" directly).
6. **TFM smoke**: no dedicated serde smoke test was added. The new suites have no `#if`, so the
   unit-test project compiles them for net462 and runs them on net8.0 and net10.0; net462 tests cannot
   run on this host (Linux).
7. **`tools/PackageSmoke` not built as a project**: its only reference is the packed `Confluent.Kafka`
   package at an exact version, which needs `pack-dotnet` (a release native build). Its `Program.cs`
   uses only the sync clients with `Serdes.String()`; compiled unchanged against the built net10.0
   assembly in a scratch project outside the repository, 0 warnings. The README samples were compiled
   the same way.
8. **Question 7a (S4 deviation 7) answered, not fixed in S5; fixed in Critic round 1 (A1)**. The question:
   does the sync producer's blocking send keep a consumer's batch alive when the record's key or value is a
   kept `ReadOnlyMemory<byte>` over that batch? At S5 it did not by any rule: `ProducerSendMarshal.Send` read
   `record.Key.Span` / `record.Value.Span` and pinned them with `fixed`, which pins an array but takes no
   `MemoryManager<byte>.Pin()`, and nothing after those lines referenced the memory's manager, so only the
   GC's view of the caller's frame kept the batch from being finalized (and its native bytes freed) during the
   native call. It was not fixed in S5 because no deterministic test could show it failing: the
   `SerializedProducerRecord` is an address-exposed struct in `NativeProducer.Send`'s frame, which the JIT
   reports as live for the whole method, so a forced collection during the native call never released the
   batch. A1 replaces the JIT detail with a contract (the blocking send takes the manager's own pin of a
   `MemoryManager`-backed key or value and releases it when the native call returns; an array costs nothing new) and tests it by counting `Pin` / `Unpin` on a
   test `MemoryManager<byte>`; see A1 under `## Critic round 1`.

## Critic round 1

Findings of `COMMENTS.129.md` (Critics A, B and C over `3b5072532..3befae5b6`), each moved here whole with its
fix. The fixup commits are on top of `3befae5b6`, one per step they fix up.

## Critic A — ownership, lifetime, consumer paths

Reviewed `3b5072532..HEAD` (S1–S5), area A, against the approved plan
(`design/history/M23/P7-async-serdes-zero-copy/PLAN.md`). Two findings, both minor; no memory-safety defect found
on any destroy path.

### A1. The sync producer's send holds no pin and no keep-alive over a batch-backed key or value

**Severity**: minor (hardening; no failure reproduced).

**Where**: `src/Confluent.Kafka/Internal/Interop/ProducerSendMarshal.cs:43-47,65` (sync `Send`); its callers
`src/Confluent.Kafka/Internal/NativeProducer.cs:274-286` and `src/Confluent.Kafka/Producer/KafkaProducer.cs:152`.

**What**: a value fetched through `Serdes.ReadOnlyMemory()` and sent with the sync `KafkaProducer` through
`ReadOnlyMemorySerializer` reaches `ProducerSendMarshal.Send` as a `ReadOnlyMemory<byte>` over a
`BytesMemoryManager`. `Send` takes `.Span` and then `fixed (byte* valueData = value)`. `fixed` over a span of
native memory pins nothing and does not call `MemoryManager.Pin`, so the batch's `SafeHandle` gets no
`DangerousAddRef`, and there is no `GC.KeepAlive`. During `NativeMethods.ProducerSendWithCallback` the only
thing that keeps the manager (and so the batch) reachable is the caller's `SerializedProducerRecord` stack slot:
the user's `ProducerRecord` and `ConsumerRecord` may already be dead. If that slot were not reported, a GC during
the native call would finalize the `SafeConsumerRecordsHandle`, `kafka_consumer_ConsumerRecords_destroy` would
free the bytes, and the core would copy freed memory.

It does not happen today. I modelled the exact call chain (`SendValidated` -> `NativeProducer.Send(struct by
value)` -> `Send(in struct)`, a `MemoryManager<byte>` over native memory referencing a finalizable batch, three
forced GCs inside the "native call") on net8.0 and net10.0, Linux x64, Release, tiered compilation on and off,
cold and after 300 warm calls: the batch was never finalized during the call, while a control that passes the
manager as a class reference was collected. The reason is a RyuJIT implementation detail: `SerializedProducerRecord`
has six fields, so it is never promoted, and a non-promoted, address-exposed struct local holding GC references is
an untracked stack slot, reported live for the whole method. No CLR or ECMA rule promises that, so the path is safe
by how the JIT works, not by contract. It also contradicts the binding's own public rule ("code that keeps only
the span, or a pointer into it, holds a pin for that time") and the plan's sentence that the value "is pinned by
the producer", which is true only of the async producer (`SendPins`). Not checked: net462 (legacy JIT on x86),
Windows x64 and arm64, where a struct this size is passed by hidden reference.

**Reference**: plan lines 161-163 ("Sync producer: unchanged" / "Consume then produce: ... is pinned by the
producer, so the batch outlives the `ConsumerRecord` until the producer has taken the record"); ledger 535;
`Consumer/ConsumerRecord.cs:39-41`, `Common/Serialization/IDeserializer.cs:92`; `COMMENTS.DONE.129.md` S4 note 7
and S5 note 8 ("answered, not fixed").

**Suggested fix**: in the sync `Send`, when a key or value is not array-backed (`!MemoryMarshal.TryGetArray`),
hold `using MemoryHandle pin = memory.Pin()` for the native call instead of `fixed` over its span (the array-backed
path stays as it is, so the common case costs nothing); or, as the smallest change, `GC.KeepAlive` the memory's
owner object after `ProducerSendWithCallback` returns.

**Fixed in `6db6944f5`** (fixup of S1 `8c2d65857`): `ProducerSendMarshal.Send` takes the manager's own pin of a
`MemoryManager`-backed key or value before it reads the spans (`PinIfOwned`: `ReadOnlyMemory<byte>.Pin()` only when
`MemoryMarshal.TryGetMemoryManager` finds a manager) and releases both in a `finally` after the native call
(`SendPins.Unpin`, made internal). A fetched batch is then held by `SafeHandle.DangerousAddRef` for the call: a CLR
rule, not a JIT detail. An array-backed key or value takes no pin and `MemoryHandle` is a struct, so nothing is
allocated and the send budgets are unchanged. Tests (`Interop/ProducerSendPinLifetimeTests`, counting `Pin` / `Unpin`
on a test `MemoryManager<byte>`): `SyncSend_PinsAKeyAndValueOfAMemoryManagerOnce_AndUnpinsThemWhenItReturns` (replaces
`SyncSend_LeavesNoPinOutstandingOnceItReturns`; fails without the fix, 0 pins), `SyncSend_RejectedTopic_IsRefusedBeforeAnythingIsPinned`,
`SyncSend_AnUnpinThatThrows_StillSendsTheRecord_UnpinsTheOtherBuffer_AndTracesTheFailure`,
`SyncSend_AValueWhosePinThrows_UnpinsTheKey_AndSendsNothing`.

### A2. `AwaitedPoll_AllocatesOneManagerPerKeyOrValueWithBytes_AndNoneForAnEmptyOne` fails with the plan's own commands (Debug configuration)

**Severity**: minor.

**Where**: `tests/Confluent.Kafka.UnitTests/PublicAsyncConsumerAllocationBudgetTests.cs:95-109`; the code it
measures is `src/Confluent.Kafka/Internal/Interop/ConsumerRecordsMarshal.cs:580-583` (`DeserializeThenRevoke`).

**What**: run as the plan's Verification section says (`cargo build --features ffi`, then `dotnet test`, which is
the Debug configuration; I ran `dotnet test tests/Confluent.Kafka.UnitTests -p:CargoProfileDir=debug -f net8.0
--filter FullyQualifiedName~AllocationBudgetTests`), the test fails every time with the same numbers: "A record
with a key and a value cost 304 B more than one with both empty (budget 128 B for the two managers; empty=8064 B,
filled=27216 B over 63 records)". With `-c Release` (what `make -C dotnet test-dotnet` uses) it passes. In Debug
the C# compiler emits an async method's state machine as a class, so each `DeserializeThenRevoke` call allocates
one even when the deserializer completes at once, and an empty key or value does not go through that method, so
the subtraction does not cancel it. The 22 allocation-budget tests that existed before the branch pass in Debug;
the two that fail are both new (the other is
`PublicProducerSendAllocationBudgetTests.Send_AwaitedPath_CompletedSerializers_PerRecordAllocation_StaysWithinItsMeasuredBudget`,
same cause, outside my area).

**Reference**: plan, Verification, "Commands" (`dotnet build`, `dotnet test` after a debug `cargo build`);
`dotnet/CLAUDE.md` line 1616 (unit tests: `dotnet test`).

**Suggested fix**: make the measurement independent of the state-machine shape (compare two filled polls of
different record counts, or subtract a filled poll through a deserializer that is given the same lent memory), or
compile the assertion only `#if !DEBUG` with the reason stated, as the test's remarks already explain its other
conditions.

**Fixed in `5bf27c7a7`** (fixup of S3 `abbddec30`), with C2: the test helper `LibraryBuild.IsOptimized` reads the
library assembly's `DebuggableAttribute`. On an optimized library the bound is the exact two 40 B managers per record
(tightened, C3); on an unoptimized one it adds the measured 112 B heap state machine of each awaited call. The Release
bound is not loosened. Passes under bare `dotnet test` (Debug) and `-c Release`, net8.0 and net10.0.

### Critic A: questions (not findings)

1. `ReleaseBatch` (`Internal/NativeConsumer.cs`, the `!batch.HandedOut && !(IsMock && batch.Lent)` branch) is the
   only place where the real consumer ends a poll with a user's pin still open on a lent memory: `batch.Dispose()`,
   deferred by the pin. I read it as correct (same `SafeHandle` deferral that `BytesMemoryManagerTests` proves),
   but no test reaches that branch with a pin open: the mock takes the other branch, and no broker-backed test uses
   a memory form, `IAsyncDeserializerWithRetention` or a pin. The plan asks for mock tests only, so I do not raise
   it; is a broker-backed case wanted?

   **Answered in `5bf27c7a7`**: yes, covered by C1's
   `AsyncKafkaConsumer_APinACallScopedDeserializerLeavesOpen_KeepsTheBatch_UntilItIsReleased`, which reaches that branch
   of the real consumer with pins open.

Checked and found correct: (1) every destroy path in `PollTyped`, `CompletePoll`, `ReleaseBatch` and
`ReleaseRecordsAsync` — the raw pointer is destroyed only when no handle was created, a handle with nothing handed
out is disposed at the end of the poll (deferred by pins), a handed-out one is left to the GC also on failure and
cancellation, and no raw `records` use follows the release; (2) `BytesMemoryManager` — `Pin`/`Unpin` balance, the
`DangerousAddRef` throw mapped to `ObjectDisposedException`, a pin taken before `Revoke` stays valid, the
`elementIndex` range (`== length` allowed, as `Memory<T>.Pin` passes it), empty `Dispose(bool)`, zero-length and
null give no manager, the CKRDA0006 pragma matches ledger 535, and the docs promise nothing about a span taken
before the revoke; (3) one fresh manager per awaited call, revoked in `finally` on the completed, faulted, thrown
and cancelled paths; (4) memory pressure added once in `Retain` and removed once in `ReleaseHandle`, the finalizer
path touching no consumer on the real client, the mock's bytes freed only when `_recordsHeld == 0` and
`_recordsRetained == 0`, deserializers disposed exactly once (`Interlocked.Exchange`); (5) failure and cancellation
on the awaiting loop — seek back to the failed record or to the batch's first positions, key before value, the
exception's buffers are copies, the token reaches the deserializer, no record lost or duplicated; (6) the
capability tests run once at construction, precedence memory form > headers form > span; (7) the awaited loop
touches the native batch only while the state machine roots it; (8) the async producer pins through `SendPins`
until the hand-over and releases on a throw; (9) the area's 64 tests pass on net8.0 in Release, and each asserts
what its name says (native manager identity, addresses, revoked messages, positions, exception content).

## Critic B — surface, built-ins, guards, producer, docs

Range `3b5072532..HEAD` (`3befae5b6`). Experiments were built against `src/Confluent.Kafka/bin/Release/net8.0`
(built from the S4 tree; S5 is docs only) in a scratch directory outside the worktree.

### B1. A synchronous `Dispose()` of an async client deadlocks on a synchronization context when a serde's `DisposeAsync()` awaits

- **Severity:** major
- **Where:** `src/Confluent.Kafka/Internal/SerdeDisposal.cs:110-128` (`DisposeBlocking`, reached from
  `NativeConsumer.cs:249,273,1956,2075` and `NativeProducer.cs:133,154`);
  `src/Confluent.Kafka/Internal/NativeProducer.cs:1146,1157` (`DisposeTheSerializersAfter` starts
  `DisposeOnceAsync()` on the closing thread, `WaitForTheSerializers` then blocks on it, from `Dispose()` at `:501`).
- **What:** the serde's `DisposeAsync()` is started on the thread that called `Dispose()` (or the constructor,
  when the core fails to build the client), with that thread's `SynchronizationContext` current, and the same
  thread then blocks in `GetAwaiter().GetResult()`. A `DisposeAsync` written the ordinary way
  (`await x;` without `ConfigureAwait(false)`) posts its continuation to that context, which the blocked thread
  never pumps. Verified by running: a thread with a single-threaded `SynchronizationContext`, an
  `IAsyncSerializer<string>` / `IAsyncDeserializer<string>` that is `IAsyncDisposable` with
  `await Task.Delay(50)` in `DisposeAsync`, then `new AsyncKafkaProducer<…>(…).Dispose()` and
  `new AsyncKafkaConsumer<…>(…).Dispose()`: neither returned in 20 s. Without the context both return and the
  serde is disposed; with the context and the built-in `AsyncString*` classes both return, so the hang is new
  with this branch (before it, the async clients called `IDisposable.Dispose()` only). The hosts that have such
  a context are the ones `net462` exists for (WinForms, WPF, classic ASP.NET), plus any `net8.0-windows` UI app;
  `using var producer = new AsyncKafkaProducer<…>(…)` compiles and is enough. The existing tests
  (`AsyncDeserializerDisposalTests`, `AsyncProducerAwaitedSerializerTests` `:510`) run without a context, so
  they pass.
- **Reference:** `ffi-marshalling.md` §C1 anti-pattern "a sync Result composed over Tasks … and then blocked on
  (`GetAwaiter().GetResult()`, `.Wait()`, `.Result`)"; `COMMENTS.DONE.129.md` S3 deviation 1; the constructor
  remarks at `AsyncKafkaProducer.cs:86-88` ("a `Dispose()` of the producer and this constructor wait for it on
  the calling thread") describe the wait but not this.
- **Suggested fix:** keep the wait (a `Dispose()` that returns before the serde is disposed would be worse, and
  the plan's one order `IAsyncDisposable`, else `IDisposable` stays), but start the serde's `DisposeAsync()`
  with no context to capture on every blocking path: when `SynchronizationContext.Current` is not null or
  `TaskScheduler.Current` is not the default, `Task.Run(() => disposable.DisposeAsync().AsTask())` and block on
  that; otherwise as now. For the producer that means the sync `Dispose()` / failed-constructor path must not
  start `DisposeOnceAsync()` inline on the caller's thread either. Add a test with a single-threaded context for
  `Dispose()` and for the failed constructor, on both clients, and say in the constructor remarks that
  `Dispose()` blocks the thread and `await using` is the form to prefer. The alternative the Actor rejected
  (call `Dispose()` on the synchronous paths when a serde has both interfaces, the usual .NET pairing of
  `using` / `await using`) removes the wait only for serdes that have both, so it does not fix a serde that is
  `IAsyncDisposable` only; it is the owner's choice whether to add it on top.

**Fixed in `5bf27c7a7`** (fixup of S3 `abbddec30`: the fix and the consumer docs), **`d8fec6fff`** (fixup of S4 `973f29cf3`:
the tests, which need the async producer, and the producer docs) and **`d05b02954`** (fixup of S5 `3befae5b6`: README). The plan's
order is kept and `Dispose()` still waits. `SerdeDisposal.Start` starts every serde's `DisposeAsync()` with no
`SynchronizationContext` and with `TaskScheduler.Default` current (`new Task<ValueTask>(disposable.DisposeAsync)
.RunSynchronously(TaskScheduler.Default)`, inline on the calling thread; the direct call when neither is set), so an
`await` inside the serde cannot post its continuation to the thread `Dispose()` blocks. It is not `Task.Run`: no pool
thread per disposal, and no sync-over-async wrapper around a blocking form. Both callers use it (the awaited
`DisposeAsync` path and the blocking path), so a disposal an awaited `Close` started is safe to wait for too, and the
failed-constructor path goes through the same code. Tests `AsyncSerdeDisposalOnABlockedThreadTests` (9 cases: a
single-threaded synchronization context that is never pumped and a one-thread non-default `TaskScheduler`, each for
the async producer's and the async consumer's `Dispose()` and for their failed construction, plus a disposal that an
awaited close started); 8 of 9 fail without the fix. Documented on `IAsyncConsumer`, `AsyncKafkaConsumer`,
`IAsyncProducer`, `AsyncKafkaProducer` and in the README: `Dispose()` is the blocking form, which blocks its thread
until the client is closed and its serdes are disposed, so `await using` or `DisposeAsync()` is the form to prefer;
a serde's `DisposeAsync()` is always called with no synchronization context and with the default task scheduler
current, so an `await` in it never needs the thread that waits.

### B2. `DeserializerExtensions.Deserialize(…, ReadOnlyMemory<byte>)` is an unlisted source break (CS0121), outside the plan's surface

- **Severity:** major
- **Where:** `src/Confluent.Kafka/Common/Serialization/IDeserializer.cs:184`; `README.md:206-214` (breaking
  changes list); `analyzers/java-name-allowlist.txt:304-310`.
- **What:** verified in a scratch project against the HEAD assembly. With `IDeserializer<string> d`, `Headers h`:
  `d.Deserialize("t", h, bytes)` with `byte[] bytes` is **CS0121** (span extension vs memory extension) at
  `LangVersion` 12 and 13, and compiles only at `latest` on the .NET 10 SDK (C# 14 first-class spans);
  `d.Deserialize("t", h, segment)` with `ArraySegment<byte>` is CS0121 on **every** version, C# 14 included (also
  on a concrete class without its own headers form, e.g. `StringDeserializer`). `Memory<byte>`,
  `ReadOnlyMemory<byte>` and `ReadOnlySpan<byte>` arguments and the two-argument `d.Deserialize("t", bytes)`
  compile everywhere. At `3b5072532` only the span extension existed, so both calls compiled: the ambiguity is new.
  The package's TFMs are `net462;netstandard2.0;net8.0;net10.0`; their default language versions are C# 7.3, 7.3,
  12 and 14, so every consumer of the package except a `net10.0` project on the .NET 10 SDK is affected for
  `byte[]`, and all of them for `ArraySegment<byte>`. A call on a class that declares its own
  `Deserialize(string, Headers, ReadOnlySpan<byte>)` is not affected (the instance method wins). The plan's
  "Public surface" lists `IDeserializerWithReadOnlyMemory<T>` and no extension; the Actor added it for rule 10's
  pairing and flagged it (S2 deviation), but the README's breaking-changes list names only the rename and the
  async constructors.
- **Reference:** PLAN.md "Public surface"; `COMMENTS.DONE.129.md` S2 ("a source break to weigh");
  `dotnet/CLAUDE.md` Java names rule 10.
- **Suggested fix:** drop the extension. Nothing needs it: a consumer tests the interface itself
  (`ConsumerRecordsMarshal.cs:544`), a caller who holds a memory writes `data.Span`, and one who wants the
  keeping form casts to `IDeserializerWithReadOnlyMemory<T>`; the async built-ins that reach it today pass
  `data.Span` instead (the `AsyncReadOnlyMemoryDeserializer` calls its class's instance method and is unchanged).
  Restore the allowlist's `missing … IDeserializer`1 …Deserializer#deserialize(String,Headers,ByteBuffer)` line with
  the comment that `IDeserializerWithReadOnlyMemory<T>` replaces Java's default without an extension, and
  delete `DeserializerMemoryExtensionTests`' extension cases. If the owner wants rule 10's pairing kept instead:
  add a `byte[]` overload that forwards to the span form (it removes the `byte[]` ambiguity on every language
  version; `ArraySegment<byte>` stays ambiguous) and list the break in the README.

**Fixed in `08cf8275a`** (fixup of S2 `746532ada`): `DeserializerExtensions.Deserialize(…, ReadOnlyMemory<byte>)`
is removed; the ten async built-ins that used it call `_inner.Deserialize(topic, headers, data.Span)` directly. The
allowlist line for Java's `Deserializer#deserialize(String,Headers,ByteBuffer)` is back (with why no second extension
exists). `DeserializerMemoryExtensionTests` is removed; `DeserializerHeadersExtensionArgumentTests` (3 tests) calls the
remaining extension with an array, an `ArraySegment<byte>` and a memory's span. A scratch project outside the
worktree, referencing the built net8.0 assembly, compiles `deserializer.Deserialize(topic, headers, byteArray)` and
the `ArraySegment<byte>` call with 0 errors and 0 warnings at `LangVersion` 12 and at `latest`.

### B3. The renamed `ReadOnlyMemory` serde types are listed `unmatched` with no pair, so their members are no longer compared with Java

- **Severity:** minor
- **Where:** `analyzers/java-name-allowlist.txt:297-299,349-350`; `analyzers/java-name-pairs.txt` (unchanged, one
  pair).
- **What:** `ReadOnlyMemorySerializer`, `ReadOnlyMemoryDeserializer`, `Serdes.ReadOnlyMemorySerde`,
  `AsyncReadOnlyMemorySerializer` and `AsyncReadOnlyMemoryDeserializer` have an `unmatched T:` line and no line
  in `java-name-pairs.txt`. Without a pair, `JavaNameAnalyzer.Check` reports "Java has no type of this name, so
  its members are not checked" and returns (`JavaNameAnalyzer.cs:391-403`), so the five classes left the check
  that `ByteBufferSerializer` / `ByteBufferDeserializer` / `Serdes.ByteBufferSerde` were under before the rename
  (the deleted `missing … ByteBufferDeserializer …#deserialize(String,Headers,ByteBuffer)` line shows they were
  paired). A renamed type belongs in the pairs file, as `ConfigResourceType` is (allowlist `:114` plus its pair):
  the `unmatched T:` line stays and the members are compared; `Pair` also resolves an `Async` class through its
  sync class's pair (`JavaNameAnalyzer.cs:247`). Nothing is mismatched in the five classes today (read by hand);
  the gap is that the analyzer no longer says so.
- **Reference:** `dotnet/CLAUDE.md` "A difference is fixed or listed": "`java-name-pairs.txt` (a renamed type and
  the Java type it translates, so its members are compared)"; ledger 540.
- **Suggested fix:** add the three pairs (`…ReadOnlyMemorySerializer org.apache.kafka.common.serialization.ByteBufferSerializer`,
  the deserializer, `…Serdes.ReadOnlyMemorySerde …Serdes$ByteBufferSerde`), rebuild, and fix or list what the
  analyzer then reports (the `missing … Serdes$ByteBufferSerde` line at `:300` probably turns stale; the
  `unmatched M:` / `missing` pair for `Serdes.ReadOnlyMemory()` at `:301-302` is the right kind and stays).

**Fixed in `08cf8275a`** (fixup of S2 `746532ada`): `analyzers/java-name-pairs.txt` pairs `ReadOnlyMemoryDeserializer`,
`ReadOnlyMemorySerializer` and `Serdes.ReadOnlyMemorySerde` with Java's `ByteBufferDeserializer`,
`ByteBufferSerializer` and `Serdes$ByteBufferSerde` (the `unmatched T:` lines stay, as for `ConfigResourceType`), so
the three classes and, through them, the two `Async` classes are compared with Java again; the stale
`missing … Serdes$ByteBufferSerde` line is gone. What the analyzer then reported is listed with its reason: Java's
`ByteBuffer` overload that the memory form stands for, and the members of the two `Async` classes that have no
asynchronous counterpart or another signature. Build: 0 CKRDA0013 / CKRDA0014; analyzer tests pass.

### B4. README and `IAsyncSerializer` say every serializer failure fails `Send`'s first stage; an `ApiException` does not

- **Severity:** minor
- **Where:** `README.md:134-135`; `src/Confluent.Kafka/Common/Serialization/IAsyncSerializer.cs:43-44`.
- **What:** both say a failure "thrown by the call or carried by the `ValueTask`" fails / ends the first stage
  and nothing is sent. On `AsyncKafkaProducer`, `SendAwaited` catches `ApiException`
  (`AsyncKafkaProducer.cs:210`) and completes the first stage **successfully** with a future that fails, after
  running the delivery callback (test `OnTheKafkaProducer_AnApiException_FailsTheRecordThroughItsFuture`). A
  reader of the README who writes `try { await producer.Send(record); } catch …` around a serializer that throws
  an `ApiException` subclass does not see the failure there. `IAsyncProducer.cs:62-67` states it correctly, so
  the three texts disagree.
- **Reference:** `COMMENTS.DONE.129.md` S4 deviation 1; Java `KafkaProducer.doSend` (`catch (ApiException e)`).
- **Suggested fix:** add the exception to both sentences ("except an `ApiException`, which fails the record
  through the future `Send` returns, as on the built-in path"). The plan's sentence "A serializer failure faults
  the returned `ValueTask`" needs the same amendment when the owner accepts deviation 1 (see Questions).

**Fixed in `d8fec6fff`** (fixup of S4 `973f29cf3`: `IAsyncSerializer`, `IAsyncProducer`, `AsyncKafkaProducer`) and
**`d05b02954`** (fixup of S5 `3befae5b6`: README). Behaviour unchanged. The texts now say: a serializer's failure fails `Send`'s
first stage, except an `ApiException`, which fails the record through its future (as Java's `doSend` and the built-in
path do); and a send that is cancelled, or whose producer is closed, during the awaited serialization is not sent
although its headers are already read-only.

### B5. `IDeserializerWithHeaders<T>`'s summary still says a consumer calls the headers form for every class that implements it

- **Severity:** minor
- **Where:** `src/Confluent.Kafka/Common/Serialization/IDeserializer.cs:52-53`.
- **What:** "A consumer calls this form for a deserializer that implements this interface, and
  `Deserialize(string, ReadOnlySpan<byte>)` otherwise." Since S1 a deserializer that also implements
  `IDeserializerWithReadOnlyMemory<T>` gets only the memory form (`ConsumerRecordsMarshal.cs:544-550`,
  `MockConsumer`), and the guard accepts it, so its headers form is never called by a consumer. Only the other
  interface's summary says so ("and no other form of it").
- **Reference:** PLAN.md "Public surface" (the memory form replaces the others for a class that has it).
- **Suggested fix:** end the sentence with "unless it also implements `IDeserializerWithReadOnlyMemory<T>`,
  whose form a consumer then calls instead".

**Fixed in `6db6944f5`** (fixup of S1 `8c2d65857`): the summary of `IDeserializerWithHeaders<T>` ends with "unless it
also implements `IDeserializerWithReadOnlyMemory<T>`, whose form a consumer then calls instead".

### Critic B: questions (not findings)

1. Left open in `COMMENTS.129.md` (an owner's question).

2. **S4 deviation 2.** Yes, a record can be reported as not sent with its headers already read-only: the token
   is cancelled before or during serialization with serializers that ignore it (they are still called), or the
   producer is closed during the await; `SetReadOnly()` then runs and `SendViaPump` throws
   (`AsyncKafkaProducer.cs:216-217`, `NativeProducer.cs:246-272`). It is the plan's order, Java's order
   (`setReadOnly` before `append`) and the built-in path's order at `3b5072532`, so not a defect; but
   `IAsyncProducer`'s remarks promise "still writable" only for a serializer failure and say nothing for these
   two cases. Should they?

   **Answered in `d8fec6fff` and `d05b02954`** (B4): `IAsyncProducer`, `AsyncKafkaProducer` and the README now state
   both cases.

3. **S4 deviation 7** (the sync producer's `fixed`-span send takes no `MemoryManager.Pin`), flagged by the Actor
   and not picked up in S5. `ProducerSendMarshal.Send` (`:43-47`, no diff in this range) reads
   `record.Key.Span` of a consumer's kept memory, and nothing after that line references the memory's manager;
   the batch is released by the GC. In practice the `SerializedProducerRecord` is an address-exposed struct in
   `NativeProducer.Send`'s frame, which the JIT keeps reported for the whole method, so I could not make it
   fail and do not report it; a `GC.KeepAlive` of the two memories' objects after the native call (or `SendPins`
   as the async path uses) would make it a contract instead of a JIT detail. This is the memory Critic's area.

   **Fixed in `6db6944f5`** as A1.

4. **The extra `AsyncSerializerExtensions.Configure` / `AsyncDeserializerExtensions.Configure`.** Right call
   under rule 10 (an `I…WithConfigure` interface is paired with an extension, as on the sync interfaces). One
   side effect, verified: on a class that implements both `ISerializer<T>` and `IAsyncSerializer<T>` and declares
   no `Configure` of its own, `dual.Configure(configs, isKey)` on the class type is CS0121 between the two
   extension classes (a call through either interface compiles). No existing code breaks; no action proposed.

   **No action**, as the Critic proposes.

Checked and found correct: the five interfaces against the plan's "Public surface" (`Headers` non-null, required
`CancellationToken`, `data` documented as valid until the `ValueTask` completes, the empty retention marker, the
two `WithConfigure` interfaces); the 22 `Async*` built-ins (each delegates to its sync class, exposes it only
through the internal `IAsyncBuiltIn(De)Serializer<T>.Inner`, `Configure` only where the sync class has it, only
`AsyncReadOnlyMemoryDeserializer` has the retention marker, none disposable as no sync class is); `SerdeGuard`'s
async checks and messages, the both-interfaces case (accepted, documented) and the mocks' `disposes: false`;
`SerdeDisposal`'s once-only, key-then-value, trace-not-throw behaviour on the async paths; the async producer's
built-in path (identical to `3b5072532`, synchronous throw), key-then-value awaiting, sealing after
serialization, completion-order entry, `SendPins` / `AsyncDeliveryState` unchanged; S4 deviations 3 and 4
against Java's `MockProducer.send`; the baseline changes (three signature renames) and the other allowlist
lines; ledger 540's four names; the four README code samples (compiled at C# 12) and the statements of
`README.md`, `examples/README.md`, `soak/README.md` and `tests/Performance/README.md` that I could check
against code; 260 tests of the six Area B test classes pass on net8.0.

## Critic C — tests and callers

Range `3b5072532..HEAD` (`3befae5b6`). Paths are under `dotnet/`. Unit tests were run from the existing
net8.0 binaries only (`--no-build`, Debug and Release); no file was changed.

### C1. The retention lifetime is tested only where the test or the mock owns the bytes, never on a batch that owns them

- **Severity:** major
- **Where:** `tests/Confluent.Kafka.UnitTests/ZeroCopyDeserializationTests.cs:217,263`,
  `AsyncConsumerDeserializationTests.cs:549`, `ConsumeThenProduceZeroCopyTests.cs:139`,
  `PublicConsumerMemoryAllocationBudgetTests.cs:96-98`, `SerializationTests.cs:608-610`,
  `Interop/ConsumerRecordsMarshalNextOffsetsTests.cs:184-207` (`NativeBatch`, 209+).
  Code under test: `src/Confluent.Kafka/Internal/NativeConsumer.cs:2040-2118` (`ReleaseBatch`).
- **What:** every test that reads a `ReadOnlyMemory<byte>?` after its poll (after the next poll, after the
  consumer is disposed, with the record dropped) runs on `MockConsumer` / `AsyncMockConsumer`. There the
  bytes are `Marshal.AllocHGlobal` blocks in `_recordBytes`, kept by the mock-only branch of `ReleaseBatch`
  (`_recordsRetained++`, `released = ReleaseRetainedRecords`, `FreeRecordBytes`). The Interop `NativeBatch`
  builds a native batch whose records borrow test-owned HGlobal bytes that the test frees itself. So no test
  reads a retained memory from a batch that owns its bytes. The real consumer's branch
  (`batch.Retain(null)`, the native batch outliving the next poll and the consumer handle) could destroy the
  batch at the next poll or at `Close()` and every test would still pass: the mock's bytes stay readable
  whatever happens to the native handle. No file in `tests/Confluent.Kafka.IntegrationTests`, `soak`,
  `grpc-server` or `examples` uses `Serdes.ReadOnlyMemory()`, `ReadOnlyMemoryDeserializer`,
  `AsyncReadOnlyMemoryDeserializer` or `IAsyncDeserializerWithRetention`.
- **Reference:** PLAN Verification bullet 1 (value readable after `Poll` returned, after the next `Poll`,
  after the consumer is disposed; "its bytes are the native ones"); root DoD 12 (the fixture stands in for
  the production primitive). The plan scopes the *new unit tests* to mocks, which is why this is not a
  blocker; the production ownership path is still unproven.
- **Suggested fix:** one broker-backed integration test per consumer (`KafkaConsumer`, `AsyncKafkaConsumer`):
  poll with `Serdes.ReadOnlyMemory()`, keep the values, poll again, close the consumer, force a GC, compare
  the bytes with what was produced. Or an Interop test over a batch built by the core that owns its bytes.

**Fixed in `5bf27c7a7`** (fixup of S3 `abbddec30`): no broker-less batch owns its bytes (the mock's are the mock's
and a unit test's are the test's), so the tests are integration tests against a broker,
`tests/Confluent.Kafka.IntegrationTests/ZeroCopyRetentionBrokerTests` (4 tests, public API only, plus one reflection
read of the batch handle to see its release). They drive the real `NativeConsumer.ReleaseBatch` of `KafkaConsumer`
and `AsyncKafkaConsumer`: (1) `KafkaConsumer` with `Serdes.ReadOnlyMemory()`, (2) `AsyncKafkaConsumer` with the
built-in `AsyncReadOnlyMemoryDeserializer`, (3) `AsyncKafkaConsumer` with an awaited
`IAsyncDeserializerWithRetention` — kept keys and values are native (no array, a memory manager), equal the produced
bytes after the next poll plus a forced GC and after the consumer is closed and disposed plus a forced GC; (4) a
call-scoped deserializer that pins after an `await` and leaves the pins open: the lent memory throws, the pinned bytes
equal the produced ones after the poll, the next poll and the consumer's close and dispose, the batch is not closed
while one pin is open and is closed when the last is released (Critic A's question 1). With `ReleaseBatch` changed to
dispose a handed-out batch on the real consumer, tests 1-3 fail with "The records these bytes belong to were
released.".

### C2. Two new awaited-path budget tests fail in the Debug configuration, the default of `dotnet test`

- **Severity:** minor
- **Where:** `tests/Confluent.Kafka.UnitTests/PublicProducerSendAllocationBudgetTests.cs:147` (assertion
  173-175, budget 544 B at line 61) and `PublicAsyncConsumerAllocationBudgetTests.cs:95` (assertion 104-107).
- **What:** on the Debug net8.0 binaries built after HEAD, `--filter "FullyQualifiedName~Budget|FullyQualifiedName~Allocation"`
  gives 2 failed, 25 passed: the awaited send allocates 1224 B per record against 544 B, and the awaited poll
  304 B per record against 128 B (a Debug build makes each async state machine a class). Both pass in
  Release, which `make test-dotnet` uses (`Makefile:28`, `DOTNET_CONFIG ?= Release`). The 25 older budget
  tests pass in both configurations, so these two are the only ones that depend on the configuration.
- **Reference:** PLAN Verification, commands: "`dotnet test` of `tests/Confluent.Kafka.UnitTests`" (no `-c`).
- **Suggested fix:** assert the bound only on an optimised library (`#if !DEBUG`, or skip when the library's
  `DebuggableAttribute` disables optimisations), or say in the plan and the test that the gate is `-c Release`.

**Fixed in `5bf27c7a7`** (fixup of S3 `abbddec30`: the consumer test, see A2) and **`d8fec6fff`** (fixup of S4 `973f29cf3`: the
producer test): `PublicProducerSendAllocationBudgetTests`' awaited-path test takes the measured 1224 B + 8 B on an
unoptimized library (`LibraryBuild.IsOptimized`), the Release bound unchanged. Both pass in Debug and Release.

### C3. The memory-path manager budgets cannot catch one extra manager per record

- **Severity:** minor
- **Where:** `tests/Confluent.Kafka.UnitTests/PublicConsumerMemoryAllocationBudgetTests.cs:47,76` and
  `PublicAsyncConsumerAllocationBudgetTests.cs:60,105` (`ManagerBudgetBytes = 64`, bound `2 * 64` = 128 B).
- **What:** a record with a key and a value allocates two 40 B `BytesMemoryManager`s, 80 B. The bound is
  128 B, so a third manager per record (120 B), or any other object of 48 B or less, passes. The test's name
  says "one manager per key or value with bytes", and it does not show that.
- **Reference:** PLAN Verification bullet 4: "one manager per non-null, non-empty key or value".
- **Suggested fix:** bound at the measured 80 B on 64-bit (`2 * 40`), or measured + 8 as
  `PublicProducerSendAllocationBudgetTests` does (544 against 536).

**Fixed in `6db6944f5`** (fixup of S1 `8c2d65857`: sync) and **`5bf27c7a7`** (fixup of S3 `abbddec30`: async): the manager is
40 B, and the bound is now exact instead of 64 B each. Sync (`PublicConsumerMemoryAllocationBudgetTests`): the whole
difference of the two polls is at most `records × 2 × 40 B` plus the mock's one 64 B release callback, so one more
object per record, or per poll, fails. Async awaited: `2 × 40 B` per record on an optimized library. 20 of 20 runs
pass on net8.0 and net10.0.

### C4. The async built-in poll budget (448 B against 260 B measured) cannot catch one object per record

- **Severity:** minor
- **Where:** `tests/Confluent.Kafka.UnitTests/PublicAsyncConsumerAllocationBudgetTests.cs:57,65,87`.
- **What:** `PerRecordBudgetBytes = 448` is the sync poll's budget; the async built-in poll measures 260 B
  (S3.9). The 188 B of slack admits a manager per key and value, a boxed state machine or a `Task` per
  record. A built-in path that fell back to the awaited copy-out for every record would pass.
- **Reference:** PLAN Verification bullet 4: the async built-in path's budget is "equal to today's".
- **Suggested fix:** measure the sync poll in the same test and bound the difference, or use measured + 8.

**Fixed in `5bf27c7a7`** (fixup of S3 `abbddec30`): the async built-in poll budget is 272 B per record (measured 263 B
or 264 B on net8.0 / net10.0, plus 8 B: less than the smallest object, 24 B), over 64 and 2112 records so that
what other threads allocate moves the result by about 1 B. 20 of 20 runs pass on both frameworks.

### C5. The memory pressure the copy-out reports for a retained batch has no test

- **Severity:** minor
- **Where:** `src/Confluent.Kafka/Internal/Interop/ConsumerRecordsMarshal.cs:220` and `:725` (the only
  `AddBytes` callers); `tests/Confluent.Kafka.UnitTests/Interop/BytesMemoryManagerTests.cs:176-190`;
  `Interop/ConsumerRecordsMarshalNextOffsetsTests.cs:184-207`.
- **What:** the only assertion on `Pressure` calls `batch.AddBytes(100)` and `AddBytes(23)` itself and then
  checks 123. No test checks the sum after a `CopyOut` or `CopyOutAsync` with a memory form. Deleting either
  `AddBytes` call (so a retained batch reports no pressure and the GC does not see its native bytes) passes
  every test.
- **Reference:** PLAN ledger row 537 (GC-only release with memory pressure equal to the key and value lengths).
- **Suggested fix:** in `ACopyOutWithAMemoryForm_CreatesTheBatchHandle_...` assert `handle.Pressure` equals
  the sum of the values' lengths after the batch is retained; the same for the `CopyOutAsync` retention path.

**Fixed in `5bf27c7a7`** (fixup of S3 `abbddec30`; the sync consumer test is in `d05b02954`, a fixup of S5 `3befae5b6`, because
S5 changed the lines next to it). Seams, internal only: `BytesMemoryManager.Batch` and
`SafeConsumerRecordsHandle.Pressure`, which now reads 0 again once the release removed it. Tests:
`Interop/BytesMemoryManagerTests` (`ARetainedBatch_ReportsItsBytesAsMemoryPressure_UntilTheRelease_WhichRunsItsCallback`:
0 before `Retain`, the bytes after, 0 after the release, callback once;
`ARetainedBatchsMemoryPressure_StaysWhileAPinDefersTheRelease`; `ADisposedBatch_ThatWasNotRetained_ReportsNoMemoryPressure`);
through the consumers, `ARetainedBatch_ReportsTheBytesOfItsKeysAndValues_AsMemoryPressure` in
`ZeroCopyDeserializationTests` (sync) and `AsyncConsumerDeserializationTests` (awaiting and not): the batch's pressure
equals the sum of the key and value lengths handed out, null and empty ones adding none; and
`ACallScopedBatch_ReportsNoMemoryPressure_AndIsReleasedOnceItsLastPinIs`. Removing the two `AddBytes` calls fails four
of them.

### C6. `ABatchNoMemoryReaches_IsCollected_WhileTheConsumerLives` shows only that the manager is collected

- **Severity:** minor
- **Where:** `tests/Confluent.Kafka.UnitTests/ZeroCopyDeserializationTests.cs:123-135`.
- **What:** the test takes a `WeakReference` to the `BytesMemoryManager` and asserts it is dead. The manager
  holds the batch, not the reverse, so a consumer that kept every retained batch handle rooted until
  `Close()` (never released while the consumer lives, the leak the test is named for) passes. No other test
  observes a GC-driven release of a poll's retained batch (the `_released` callback, `IsClosed`, or the
  mock's `_recordsRetained` going back down).
- **Reference:** PLAN ledger row 537; Verification bullet 1 ("held without the record").
- **Suggested fix:** take the `WeakReference` to the `SafeConsumerRecordsHandle` (reached through the
  manager), or assert the release callback ran after the collection.

**Fixed in `d05b02954`** (fixup of S5 `3befae5b6`, which changed the lines next to the test; the `Batch` seam it reads
is in `5bf27c7a7`): `ABatchNoMemoryReaches_IsCollected_WhileTheConsumerLives` also holds a long weak reference
(`trackResurrection: true`) to the batch's handle, which dies only after the finalizer that releases the batch ran,
and asserts it is dead. Rooting retained handles in `ReleaseBatch` fails it.

### Critic C: questions (not findings)

1. Left open in `COMMENTS.129.md` (an owner's question).

2. Left open in `COMMENTS.129.md` (an owner's question).

3. Left open in `COMMENTS.129.md` (an owner's question).

4. S5.8 says "Question 7a answered, not fixed: no deterministic test can show the defect (see the report)".
   The report is not in the worktree, so I could not judge the claim.

   **Answered**: `## S5` note 8 above is rewritten to stand alone (what the question was, why no
   deterministic test existed at S5, and that A1 now fixes it).

### Verification bullet -> test -> verdict

| Verification bullet | Test | Verdict |
|---|---|---|
| 1. Memory value readable after poll / next poll / dispose / without the record; native bytes | `ZeroCopyDeserializationTests`, `SerializationTests` | proves it on mocks; pin address against the native record's `data` never asserted (weak); real consumer missing (C1) |
| 2. A pin defers the destroy; a pin after the release throws | `Interop/BytesMemoryManagerTests` | proves it |
| 3. Call-scoped read after the `ValueTask` completed throws; `WithRetention` does not | `AsyncConsumerDeserializationTests` | proves it (mock) |
| 4. Span-only polls create no handle or manager; new budgets | `ConsumerRecordsMarshalNextOffsetsTests` (no handle), the three budget suites | no-handle proves it; memory budgets weak (C3); async built-in budget weak (C4); Debug failure (C2) |
| 5. Failure or cancellation mid-batch seeks back; deserializer disposing the consumer | `AsyncConsumerDeserializationTests`, `SerializationTests`, `ConsumerAsyncCancelTests`, `AsyncDeserializerDisposalTests` | proves it (the suites the plan names were changed only mechanically; `RecordDeserializationTests` not touched) |
| 6. Async producer: awaited, failing, cancelled, headers sealed after serialization, built-in throws synchronously | `AsyncProducerAwaitedSerializerTests` | proves it |
| 7. Consume then produce with zero copy; record dropped before the send completes | `ConsumeThenProduceZeroCopyTests` | proves it piecewise, on mocks |
| 8. Guards with messages | `SerdeGuardTests`, `AsyncSerdeGuardTests` | proves it |
| 9. TFM smoke (net462, net8.0, net10.0) | existing TFM smoke suites (built-ins only) | weak; not run here |
| Ledger 537: GC-only release with memory pressure | `BytesMemoryManagerTests:176-190`, `ZeroCopyDeserializationTests:123-135` | missing for the copy-out (C5); weak for the release (C6) |

## Critic round 2

Scope: the fixups `6db6944f5`, `08cf8275a`, `5bf27c7a7`, `d8fec6fff`, `d05b02954` (`3befae5b6..HEAD`). Round-1
findings A1, A2, B1-B5 and C1-C6 are fixed; one new finding.

### D1 (minor): the README's two new paragraphs state `AsyncKafka*` behaviour for every asynchronous client

**Where:** `dotnet/README.md:137-139` and `:141-142` (added in `d05b02954`).

**What:** Two sentences are false for the asynchronous mocks.

1. `:141-142`: "An asynchronous client disposes the serializers or deserializers it was given, through
   `IAsyncDisposable` when they have it and otherwise through `IDisposable`." `AsyncMockProducer` and
   `AsyncMockConsumer` dispose nothing.
2. `:137-139`: "A record whose token is cancelled, or whose producer is closed, while a serializer is awaited is not
   sent, although its headers are then already read-only." On `AsyncMockProducer` the headers stay writable.

**Evidence:**

- Only the two real clients hand their serdes to `SerdeDisposal`: `Producer/AsyncKafkaProducer.cs:114` and
  `Consumer/AsyncKafkaConsumer.cs:114` (`SerdeDisposal.OfAsyncSerdes(key, value)`); `grep -n 'SerdeDisposal\|DisposeOnce'`
  finds nothing in `AsyncMockProducer.cs` or `AsyncMockConsumer.cs`.
- The interface docs of the same fixups say the opposite of the README: `Producer/IAsyncProducer.cs:120-121` ("An
  `AsyncKafkaProducer` disposes the serializers it was given when it closes, as its constructor says; an
  `AsyncMockProducer` does not.") and `Consumer/IAsyncConsumer.cs:60-62` (the same for the consumer).
- `Headers.SetReadOnly()` is called only at `Producer/AsyncKafkaProducer.cs:195,221` and `Producer/KafkaProducer.cs:150`;
  `AsyncMockProducer.SendAwaited` (`Producer/AsyncMockProducer.cs:156-167`) awaits `SerializeAsync(..., mock: true, ...)`
  and then calls `SendViaPump`, with no `SetReadOnly`.

**Suggested fix:** name the classes, as the paragraph's `ApiException` sentence already does: "An `AsyncKafkaProducer`
or `AsyncKafkaConsumer` disposes the serializers or deserializers it was given, … (the mocks do not)", and "although
on an `AsyncKafkaProducer` its headers are then already read-only".

Fixed in `89dee01db` (fixup of S5, by the Manager): the README names `AsyncKafkaProducer` and
`AsyncKafkaConsumer` in both sentences and says the asynchronous mocks dispose none.

### Checked and not reported

- B1, `SerdeDisposal.Start`: reproduced in isolation (net8.0). On a thread with a synchronization context, the
  `DisposeAsync` call runs on the calling thread with no context and the default scheduler; the context is restored
  after a completed `ValueTask`, after a synchronous throw (rethrown unwrapped, an `OperationCanceledException`
  included) and after an asynchronous one; nothing is posted to the context. No unit test asserts the restore or the
  synchronous throw on the context path; the code is right, so this is not a finding.
- B1, the awaited route (`SerdeDisposal.DisposeAsync`) also goes through `Start`, so a serde's `DisposeAsync` of a client
  disposed with `await using` on a UI thread no longer resumes on that thread. The README and the constructors' docs
  state it ("always called with no synchronization context"), so it is a documented decision, not a defect.
- C1, `ZeroCopyRetentionBrokerTests`: read, compiled, not run (Docker). `AssertNativeBytes` cannot pass on a copied
  array and the pin test reads the real batch handle's `IsClosed`.
- C4, the 272 B budget (8 B over the measured 264 B, on the process-wide counter): 6 runs on net8.0 and 3 on net10.0
  of the budget, zero-copy and `BytesMemoryManager` tests, 44 passed each time.
- Comment rules: the fixups' new comments state decisions, and carry no phase or review ID and no Java line number.

Gates measured: `dotnet build Confluent.Kafka.sln -c Release -p:CargoProfileDir=debug` (also `--no-incremental`): 0
warnings, 0 errors, no CKRDA0013/0014. Unit tests, Release, net8.0: 9,526 passed, 51 skipped, 0 failed. The new and
changed test classes: 224 passed, 0 failed, on net10.0 Release and on net8.0 Debug.

After round 2, `7e3f7baf4` (fixup of S4, by the Manager) closes the first test gap named above: the
blocked-thread disposal tests assert that the call leaves its thread the synchronization context it had.

## Open for the owner

Every finding (A1, A2, B1-B5, C1-C6) is fixed and moved to `COMMENTS.DONE.129.md`, `## Critic round 1`. What is
left are questions for the owner; none needs an Actor change.

### Critic B, question 1

1. **S4 deviation 1** (an awaited serializer's `ApiException` fails the record through the future, not the
   first stage). It matches Java's `doSend` and the built-in path, and `IAsyncProducer` documents it; it differs
   from the plan's literal sentence. I judge it right; the owner should accept it explicitly and the plan
   sentence be amended (B4 covers the two texts that still follow the plan's wording).

### Critic C, question 1

1. PerfV3's async consumer arm now runs the awaited path (`tests/Performance/PerfV3/V3Backends.cs`,
   `ByteCountDeserializer` is not a built-in; `tests/Performance/README.md` says so). The plan's A/B
   "async built-in path (no regression expected)" then has no PerfV3 consumer arm. Accepted?

### Critic C, question 2

2. Does CI run the unit tests on net462? S5.6 added no serde smoke test and the two consumer budget files
   are `#if NET8_0_OR_GREATER`, so on this host nothing executed `BytesMemoryManager` or the awaited paths
   on net462.

### Critic C, question 3

3. S4.1: an `ApiException` from an awaited serializer on `AsyncKafkaProducer` fails the record through its
   future, not the first stage (pinned by `AsyncProducerAwaitedSerializerTests.cs:283-302`), while the plan
   says "A serializer failure faults the returned `ValueTask`". Is the deviation accepted?

### Critic round 2, question 1

1. The awaited route (`await using` / `DisposeAsync()` of an async client) also starts a serde's `DisposeAsync()`
   with no synchronization context and the default task scheduler, so a serde's own continuations no longer resume
   on a UI thread. The README and the constructors' docs state it. Accepted as part of B1?
