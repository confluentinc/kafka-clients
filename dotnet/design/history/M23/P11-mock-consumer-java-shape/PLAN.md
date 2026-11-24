# Plan: the mock consumers take Java's shape

Status: agreed with the owner in conversation (2026-11-23, "ok let's do the change"). Scope: the full plan. This
copy is the port onto master.

Milestone 23, Phase 11. Agent number N=135, ledger rows 626–632. Mode A (.NET only): no Rust, C ABI, header,
Python or proto change. Branch `esabellico/dotnet_mock_consumer_java_shape_on_master`, from master `a9a422bc9`
(after #356, #353 and #349), in the worktree `kafka-clients-dotnet-wrapped`. Never pushed by an agent. All paths
below are under `dotnet/`.

The phase was first built on `esabellico/dotnet_mock_consumer_java_shape` (head `991014ff2`), stacked on M23/P9
(`esabellico/dotnet_consumer_records_and_headers_wrapped`, `1081aaa19`), where a polled record wraps its native
record. M23/P9 is not on master, so this branch ports the phase without it: a polled record stays master's eager
copy, and nothing of M23/P9 comes along (no wrapped record, no lazy headers, no `ValueMemory`). The design below
is the stacked branch's, restated for master's copy-out; section "Port onto master" lists what differs.

How it runs: Actor 135 ports one step at a time and commits it with `--no-verify` after running the step's gates
(section "Gates per step"). The rule edits were drafts (section "Rule text") until the owner accepted them on
2026-11-23; the port applies them where their text holds on master verbatim.

## Context

Java's `MockConsumer<K, V>` has one non-deprecated constructor, `MockConsumer(String offsetResetStrategy)`, takes
no deserializer, and is fed typed records: `addRecord(ConsumerRecord<K, V> record)`. `poll` hands those records
back.

The .NET `MockConsumer<TKey, TValue>` and `AsyncMockConsumer<TKey, TValue>` deviate twice (ledger 48 and the
"Consumer deserializers" paragraph of `CLAUDE.md`): their constructors take the two deserializers, and
`AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value)` takes bytes, which `Poll`
decodes. The reason given was the bytes-only core ABI.

That reason no longer holds. The C mock is `MockConsumer<GenericValue, GenericValue>`: a record's key and value
cross as `void *` slots the core never reads, and `poll` returns exactly the slot values `add_record` was given
(Rust test `poll_hands_back_the_record_pointers_add_record_was_given`). A polled .NET record is built by
`ConsumerRecordsMarshal.CopyOut`, which takes every field from the native record and the typed key and value from
the deserializers. So the mock can put its own reference in the two slots and let the copy-out resolve it at
`Poll` instead of deserializing, with no bytes and no deserializer.

Outcome: the two mocks have Java's constructor and Java's `AddRecord`; the binding's own decode tests, which are
not translations of Java tests, stop going through the public mock.

## Design

### Public surface

`MockConsumer<TKey, TValue>` and `AsyncMockConsumer<TKey, TValue>`:

- One constructor, `(string offsetResetStrategy)` (Java's parameter name). A null strategy still crosses as NULL
  and the core refuses it with Java's `IllegalArgumentException` ("Auto offset reset strategy is null"). The
  deserializer-taking constructors, the default-strategy constructors, every deserializer field and the
  `SerdeGuard` / `RequireDeserializer` calls go. Java's deprecated `MockConsumer(OffsetResetStrategy)` is not
  translated.
- `void AddRecord(ConsumerRecord<TKey, TValue> record)`. The bytes form goes. A null `record` is refused as the
  binding's null rule says for a Java `NullPointerException` (Java dereferences it); the "partition not
  assigned" check stays the core's, with Java's message.
- `Poll` keeps its signature and returns new records copied out of the C batch, their key and value being the
  objects `AddRecord` was given.
- The type parameter docs and the class remarks drop the two deviations and state the one that replaces them
  (below).

### Key and value slots: ids

`AddRecord` builds a C record (`kafka_consumer_ConsumerRecordOptionsBuilder_*`,
`kafka_consumer_ConsumerRecord_with_options`) and gives it to `kafka_consumer_MockConsumer_add_record`, which
copies it; the binding destroys its own handle after the call, as today.

- The key slot holds an id and the value slot holds another id: an integer cast to a pointer. Nothing is
  allocated or pinned natively, and no `GCHandle` is taken. A null key or value stays `NULL`, so the core's
  "null key" paths and the record's `Key() is null` read the same as for a real record.
- Ids come from one counter per mock, starting at 1, and are never reused. The counter and the dictionary keys
  are `nint`, so the cast to the slot is exact on a 32-bit process too.
- Two typed dictionaries per mock, `Dictionary<nint, TKey>` and `Dictionary<nint, TValue>`: a value-type key or
  value is not boxed.
- An entry is removed when its record is delivered by `Poll` (the core's mock removes a delivered record from
  its queue, so it is never delivered twice). The dictionaries are cleared once the consumer is closed, for the
  records the core dropped without delivering (an offset below the position, a partition that left the
  assignment).
- Java's `MockConsumer` methods are `synchronized`. The dictionaries and the counter are guarded by one private
  lock, re-entrant (`AddRecord` from a poll task runs inside `Poll`), never held across an `await` or a native
  wait.
- Every other field is copied into the C record at `AddRecord`: topic, partition, offset, timestamp, timestamp
  type, serialized key and value sizes, headers (a new native `RecordHeaders` filled from the record's
  `IHeaders` by the producer's header code, `ProducerSendMarshal.AddHeader`, with its null-key check), leader
  epoch, delivery count. A polled record reads them back from the native record, as the copy-out does for any
  polled record.

### Poll: resolving instead of decoding

The copy-out gets a second per-record source of the typed key and value. Today `RecordReader.Copy` reads the two
slots and calls the deserializers on the bytes they point at. For the mock:

- The slot values are ids. **They are never dereferenced**: no `Length`, no `CopyBytes`, no memory over them, no
  `NewError`.
- The key and value are looked up, and removed, in the mock's dictionaries. An id with no entry is an
  `IllegalStateException` naming the record (it means the binding's own bookkeeping is wrong; no test may rely
  on it).
- No deserializer runs, so no `RecordDeserializationException`, no rewind and no headers made for a
  deserializer; the headers are copied out of the native record as for any polled record.
- The mock keeps no native batch: the records are copied out and the batch is destroyed by the poll, so it
  reports no memory pressure.
- The synchronous and the asynchronous mock share the resolving step; the asynchronous `Poll` has nothing left
  to await per record.

The decode path (`PollTyped`, `CopyOut`, `CopyOutAsync` with deserializers) is unchanged and stays the real
consumers'.

How the two sources are expressed (a small internal interface, a generic struct parameter, or two entry points
sharing the batch walk) is the Actor's choice, on these conditions: the real consumers' per-record path gains no
allocation and no interface call (their allocation-budget tests keep their numbers), and the batch walk is not
duplicated.

### The binding's decode tests

Tests of the decode path are the binding's own, not translations of Java's `MockConsumerTest`. They move off the
public mock:

- Preferred: they run the real classes over the C mock. An internal factory on `KafkaConsumer<TKey, TValue>` and
  `AsyncKafkaConsumer<TKey, TValue>` builds the consumer over `NativeConsumer.CreateMock(...)` with the given
  deserializers, running the same checks and the same disposal as `Create`; the test keeps the `NativeConsumer`
  to add byte records (`NativeConsumer.AddRecord`, internal, unchanged) and to drive the mock-only helpers.
  `InternalsVisibleTo` already covers the test projects, `Confluent.Kafka.GrpcServer` and the soak client. The
  factory changes nothing for a consumer built by `Create`.
- Fallback, only when the preferred form cannot be made without changing what `Create` does: one internal test
  helper over `NativeConsumer.CreateMock`, `NativeConsumer.AddRecord` and `NativeConsumer.PollTyped` / its
  asynchronous twin, in the test project. The reason is written in this plan's "Revision" section.

Either way the helper is one shared fixture per test project, not a copy per test class, and it calls the
production primitives (`definition-of-done.md` §12).

Every other test, and every other caller (`grpc-server`, `soak`, `tools`, `examples`, `tests/Performance`,
`README.md`), moves to the Java-shaped constructor and the typed `AddRecord`. A caller that needs the decode
behaviour of the mock (the gRPC server's chaos and serde paths, if they do) uses the internal fixture instead and
says so in a comment.

### Deviations that remain (ledger rows 626–628, 632)

- A polled record is a new `ConsumerRecord` copied out of the native batch, not the instance given to
  `AddRecord`; Java returns the same object. Its key and value are the same objects. `ConsumerRecord` has no
  `equals` in Java, so tests compare fields.
- The record's fields are copied at `AddRecord`: a later change to the caller's `Headers` does not reach the
  polled record, where Java shares the object.
- The mocks are no longer a way to test a deserializer without a broker. Java's never was.
- A cancelled `AsyncMockConsumer.Poll` returns a batch the C mock already delivered rather than ending with
  `OperationCanceledException` and seeking back, as `AsyncKafkaConsumer` does (ledger 632): the C mock cannot
  deliver a dropped batch again.

## Port onto master

What differs from the stacked branch, and why:

- **Polled records.** Master's polled record is an eager copy, so the resolving source builds master's copied
  `ConsumerRecord` (every field read from the native record, headers copied by `RecordReader.CopyHeaders`), where
  the stacked branch built a wrapped record. The batch walk becomes one generic walk over a struct source, the
  deserializing one forwarding to master's unchanged `RecordReader.Copy`.
- **Headers.** #356's types: the record's `IHeaders` crosses through `ProducerSendMarshal.AddHeader`, made
  `internal`, so a user `IHeader` and the null-key check ("Invalid null header key found in headers") behave as
  on a producer send. A user `IHeaders` other than the binding's own is copied first through `RecordHeaders`,
  which refuses a null element.
- **Delivery count.** Master reads no delivery count on a polled record. The mock's resolving source reads it
  (`kafka_consumer_ConsumerRecord_delivery_count_err`, one more P/Invoke), so a polled mock record keeps the
  delivery count given to `AddRecord` (ledger 628); the real consumers' copy-out is unchanged.
- **Memory pressure** (ledger 630). On master only a batch kept for a memory-form deserializer reports pressure;
  the mock keeps no batch, so the row says that instead of "no key and value bytes".
- **`TopicPartition` and `Uuid`** are classes on master (#356); the tests are written for that.
- **Tests not ported**: the ones that exist for M23/P9 only (`PolledConsumerRecordTests`, `PolledHeadersTests`,
  `PublicHeadersTests`); master has none of them.

## Steps

One commit per step, each passing the gates. The stacked branch's fixups are folded into their steps.

- **S0** This plan, `STATUS.md` entry (N=135, rows 626–632, next unused N 136).
- **S1** Interop and core plumbing: the P/Invoke declarations for the record options builder, the
  `TimestampType` singletons and the delivery count; `ConsumerRecordMarshal` (the mock's record builder);
  `MockRecordSlots<TKey, TValue>`; `NativeConsumer.AddRecord` for a built record with the two slot values; the
  resolving copy-out. Internal only, with unit tests over `NativeConsumer` (slot values come back; null slots;
  every field round-trips; a batch with id slots is released and destroyed without a native fault).
- **S2** The decode fixture (section "The binding's decode tests") and the move of the decode-dependent tests to
  it. No test is deleted or weakened.
- **S3** The public change on `MockConsumer` and `AsyncMockConsumer`, and every remaining caller. Java's
  `MockConsumerTest` cases in `PublicMockConsumerJavaHelpersTests` add typed records as Java does. New tests, on
  both mocks: reference-type and value-type keys and values; a null key and a null value; every record field;
  the key and value are the same objects; the entry is gone after delivery and the dictionaries are empty after
  `Close`, also for a record the core dropped; partial delivery under `SetMaxPollRecords`; a null record; the
  constructor's null and invalid strategy; `AddRecord` from another thread while a `Poll` waits.
- **S4** Docs and lists: XML docs of the two mocks; the ledger rows; the `STATUS.md` outcome.
- **S5** The accepted rule text M1–M4, where it applies to master verbatim.
- **S6** `e137a77d0`: M3 with master's wording, on the owner's go-ahead (2026-11-24); see "M3 on master".

## Verification

1. A test in S1 asserts on the native side what the design relies on: the slot values `Poll` returns are the
   values given, for both slots, with one NULL.
2. `PublicMockConsumerJavaHelpersTests` follows Java's `MockConsumerTest` case by case.
3. The real consumers' allocation budgets are unchanged (S2 moves the tests; their numbers do not move).
4. No public member of the two mocks names a deserializer:
   `grep -n "Deserializer" src/Confluent.Kafka/Consumer/MockConsumer.cs src/Confluent.Kafka/Consumer/AsyncMockConsumer.cs`
   prints nothing outside a remark that says the mocks take none.
5. The unit suite count does not drop except for the tests S3 lists as gone.

## Gates per step

1. The native library (`make -C dotnet rust-lib`, `CARGO_INCREMENTAL=0`), built once.
2. `dotnet build Confluent.Kafka.sln -c Release`: 0 warnings, 0 errors.
3. `dotnet test` of `tests/Confluent.Kafka.UnitTests` on net8.0 and net10.0: 0 failed. One test process at a time.
4. `dotnet format --verify-no-changes`.
5. `cargo xtask dotnet-java-name-baseline`: 0 added.
6. Before the last commit: `make -C dotnet test-grpc-server-dotnet`.

`make verify-dotnet` (repository root) is the close gate, run by the owner.

## Out of scope

- Any Rust or C change. If the C mock turns out to read, free or compare a slot value anywhere, the Actor stops
  and reports; it does not change the core.
- `MockProducer` (Java's takes serializers; unchanged).
- Returning the caller's own `ConsumerRecord` instance from `Poll`.
- M23/P9 (wrapped records, lazy headers).

## Ledger rows (agreed by the owner, 2026-11-23)

| # | Source | Topic | Decision | Status |
|---|---|---|---|---|
| 626 | Design, public surface | Mock constructors | `MockConsumer` and `AsyncMockConsumer` have Java's one constructor, `(string offsetResetStrategy)`, and take no deserializer. Supersedes the deserializer-taking constructors of ledger 48 and of the "Consumer deserializers" paragraph. Breaking; the client is in preview | Agreed |
| 627 | Design, public surface; ids | `AddRecord` | `AddRecord(ConsumerRecord<TKey, TValue> record)`, Java's form; the bytes form goes. The key and the value cross the C mock as ids in the record's `void *` slots (`MockRecordSlots`), resolved by `Poll` through two typed dictionaries; a null key or value stays `NULL` and takes no entry; an entry goes at delivery, the rest once a `Close`, `Close(CloseOptions)`, `Dispose` or `DisposeAsync` closes the consumer (one that throws, is canceled or is refused and leaves it open keeps them, so the next `Poll` still returns those records); a record the core refuses leaves no entry. *(Deviation: the polled record is a new instance copied out of the native batch, with the same key and value objects; Java returns the instance added.)* | Agreed |
| 628 | Design, ids | Fields copied at `AddRecord` | timestamp, timestamp type, sizes, headers, leader epoch and delivery count are copied into the C record at `AddRecord`. *(Deviation: a change made to the record's `Headers` after `AddRecord` does not reach the polled record; Java shares the object.)* | Agreed |
| 629 | Design, decode tests | Decode tests | the binding's decode tests run the real consumers over the C mock through the internal factories `KafkaConsumer.CreateOverMock` / `AsyncKafkaConsumer.CreateOverMock` (the preferred form; no fallback, "Revision" stays empty), shared through one fixture per test project; the public mocks no longer decode | Agreed |
| 630 | Design, poll | Mock batch pressure | a mock poll keeps no native batch: its records are copied out and the batch destroyed by the poll, so it reports no memory pressure, and its slots, being ids, are never read as bytes | Agreed |
| 631 | Rule drafts | Rule text M1–M4 | drafts below, checked against what was built (S4); on master (S5, 2026-11-24) M1 and M2 applied verbatim to `dotnet/CLAUDE.md` and M4 to `dotnet/.claude/rules/ffi-marshalling.md` (as a row of the type table); M3 applied with master's wording on the owner's go-ahead (2026-11-24, see below) | Agreed |
| 632 | Design, poll; cancellation | A cancelled `AsyncMockConsumer.Poll` | a batch the C mock has delivered is never dropped: once the native poll has returned records, the mock resolves them and `Poll` completes with them even if the token has fired; `OperationCanceledException` only when the cancellation ended the poll before the C mock delivered a record (a token cancelled at the call, the cancellation's own `wakeup` reaching the C mock before it takes a record, or an empty poll the token fired on), and the records added stay for the next `Poll`. No seek back on this path. The C mock removes what it delivers, as Java's `MockConsumer.poll` does, so a dropped batch could not be delivered again; Java's mock checks its `wakeup` before it takes a record and returns what it took. *(Deviation from `AsyncKafkaConsumer` and the `IAsyncConsumer.Poll` contract, which drop a cancelled poll's records and seek back to them, ending with `OperationCanceledException`; stated on `AsyncMockConsumer.Poll`.)* (Critic 135 issue 4) | Agreed |

Rows 627 and 630 are reworded for master's copied records (section "Port onto master"); their decisions are the
agreed ones.

## Rule text (drafts M1–M4, accepted by the owner 2026-11-23)

### M1 — `CLAUDE.md` §3 consumer sketch, `AsyncMockConsumer` (the deviation comment and the two constructors)

```csharp
public sealed class AsyncMockConsumer<TKey, TValue> : IAsyncConsumer<TKey, TValue> {    // Java `MockConsumer`
    // Java's shape: no deserializers, typed records (ledger 626, 627). Mock helpers are inherent, not on
    // IAsyncConsumer.
    public AsyncMockConsumer(string offsetResetStrategy);   // null: IllegalArgumentException, as Java's MockConsumer(String)
    public void AddRecord(ConsumerRecord<TKey, TValue> record);   // Java MockConsumer.addRecord
```

### M2 — `CLAUDE.md` §3, the line "KafkaConsumer<K,V> / MockConsumer<K,V> mirror the async pair's … ctors" and the `MockConsumer` sketch

`KafkaConsumer<K,V>` mirrors `AsyncKafkaConsumer<K,V>`'s `Create`; `MockConsumer<K,V>` has
`MockConsumer(string offsetResetStrategy)` and `AddRecord(ConsumerRecord<TKey, TValue> record)`, as the async
mock.

### M3 — `CLAUDE.md`, the paragraph "Consumer deserializers" (its last sentence)

Replace "the mocks take the deserializers … *(Java's `MockConsumer` takes no deserializers)*" with: "The mock
consumers take no deserializers, as Java's: `AddRecord` takes a typed `ConsumerRecord`, whose key and value
`Poll` hands back. They cross the C mock as ids in the record's `void *` slots, resolved through dictionaries in
the mock; a polled record is a new instance over the native batch *(deviation: Java returns the instance
added)*. The binding's decode tests run the real consumers over the C mock."

**M3 on master: applied with master's wording (2026-11-24, on the owner's go-ahead).** The sentence above, "a
polled record is a new instance over the native batch", is false on master, where a polled record is copied out
of the batch, and master's "Consumer deserializers" paragraph ended with a different sentence ("The mock
consumers have no configuration, so a null deserializer always throws it *(Java's `MockConsumer` takes no
deserializers)*"), stale since the mocks take no deserializer. That sentence is replaced in `dotnet/CLAUDE.md`
with: "The mock consumers take no deserializers, as Java's: `AddRecord` takes a typed `ConsumerRecord`, whose key
and value `Poll` hands back. They cross the C mock as ids in the record's `void *` slots, resolved through a
table in the mock; a polled record is a new instance copied out of the native batch *(deviation: Java returns
the instance added)*. The binding's decode tests run the real consumers over the C mock."

The applied text differs from the accepted draft in two places: "a new instance over the native batch" became
"a new instance copied out of the native batch" (the draft was false on master), and "resolved through
dictionaries in the mock" became "resolved through a table in the mock". Both are true of the code:
`MockRecordSlots` is one table holding two dictionaries, on both branches, so the second change was not needed
on master; it is a rewording, not a correction.

### M4 — `ffi-marshalling.md`, the generic-slot rows

A generic `void *` slot the core never reads may carry an integer id that the binding resolves; it is never
dereferenced, on either side, and needs no `GCHandle`. First use: the mock consumers' key and value.

### M5 — `CLAUDE.md` §3 producer sketch, the `AsyncMockProducer` comment (proposed, not applied)

Current text (`dotnet/CLAUDE.md:344-346`): "Mock TAKES the serializers — Java-faithful (MockProducer.java takes
Serializer<K>/Serializer<V> and serializes records into its history); UNLIKE the consumer, where
mock-takes-deserializers is a deviation. Send serializes K/V → bytes exactly like the real client (§4)."

Stale because the mock consumers no longer take deserializers (ledger 626): there is no mock-takes-deserializers
deviation left to contrast with. Proposed: "Mock TAKES the serializers — Java-faithful (MockProducer.java takes
Serializer<K>/Serializer<V> and serializes records into its history); UNLIKE the consumer, whose mocks take no
deserializers (ledger 626). Send serializes K/V → bytes exactly like the real client (§4)."

### M6 — `CLAUDE.md`, the disposal steps of the clients (proposed, not applied)

Current text (`dotnet/CLAUDE.md:1631-1632`): "The producer and consumer mocks never dispose their serializers or
deserializers, as Java's and Python's mocks."

Stale because the consumer mocks hold no deserializer to dispose (ledger 626); the sentence still describes them as
holding ones they keep. Proposed: "The producer mocks never dispose their serializers, as Java's and Python's mocks;
the consumer mocks hold no deserializer."

### M7 — `CLAUDE.md`, the producer bullet "Mock-takes-serializers is Java-faithful" (proposed, not applied)

Current text (`dotnet/CLAUDE.md:433-436`): "Java's `MockProducer` takes `Serializer<K>/Serializer<V>` and
serializes into its history, so the mock ctors take serializers too (`(keySer, valueSer, autoComplete=true)`) —
**unlike** the consumer, where mock-takes-deserializers was a deviation."

Found by the F2 sweep. The past tense ("was") keeps it true, but `(keySer, valueSer, autoComplete=true)` is not the
shipped shape: both producer mocks have a two-argument and a three-argument constructor, with no optional
parameter. Proposed: "… so the mock ctors take serializers too (`(keySer, valueSer)` and
`(keySer, valueSer, autoComplete)`) — **unlike** the consumer, whose mocks take no deserializers (ledger 626)."

## Revision

(none: the preferred factory form was built in S2.)

## Outcome (S1–S4)

Commits: S0 `d60e97154`, S1 `d7c5359cf`, S2 `2822920b2`, S3 `47bd5580a`, S4 (the commit with this text). The
stacked branch's fixups for Critic 135's issues 1–4 are folded into the steps that wrote the code they fix.

- **C mock slots.** The C mock stores the key and value slots and never reads, frees or compares them: the
  binding builds it as `MockConsumer<GenericValue, GenericValue>` with `Owns { key: false, value: false }`, and
  the options record owns nothing. `MockRecordSlotsTests` asserts that the ids come back unchanged, with one NULL.
- **The copy-out.** `ConsumerRecordsMarshal` walks a batch once, generic over a struct source: the deserializing
  source forwards to master's unchanged `RecordReader.Copy`, the resolving one to `RecordReader.Resolve`, which
  reads the same fields plus the delivery count and takes the key and value from the slot table. The real
  consumers' allocation budgets did not move.
- **Lists.** The deviations of the old mocks (the missing `(String)` constructor, the deserializer-taking
  constructors, the bytes `addRecord`) were `java-name-allowlist.txt` lines (blocks C1, C2's mock-consumer lines,
  M9), not `java-name-baseline.txt` lines. They went in S3, because an allowlist line that matches nothing fails
  the build (CKRDA0014). The baseline is unchanged; `analyzers/java-api.txt` is generated from Java and is
  unchanged.
- **When the slots go.** The table is emptied when the native consumer is torn down (`NativeConsumer.OnTornDown`,
  set by both mocks), so only by a close or a dispose that closes the consumer, and after every use of it has
  ended. A close or a dispose that throws, is canceled or is refused leaves the consumer open and the slots in
  place, so its next `Poll` returns the records, as Java's `MockConsumer`, whose close never touches its records.
  A dispose inside an async consumer's callback schedules the teardown, which empties the table when it runs.
- **Poll and a concurrent close.** The mock poll resolves its batch's ids while it still holds its use of the
  consumer, on both mocks (`NativeConsumer.PollResolved`, `CompletePoll`), so a close or a dispose from another
  thread cannot empty the table under it. The real consumers' poll path behaves as on master; its allocations
  are master's on the synchronous consumer, and 16 B more per `Poll` on the asynchronous one, at every batch size
  (Critic 135, fourth pass: built-in deserializers 2712 → 2728 B at one record, 272784 → 272800 B at 1024;
  awaited ones 2832 → 2848 B, 223800 → 223816 B). The 16 B are the `slots` parameter and the `resolveFailure`
  local, fields of the state machine of the `CompletePoll` both paths share. They stay: removing them needs a
  mock-only completion method that repeats the shared prologue (`Outcome`, `KeepKeptError`, `HoldRecords`,
  `ExitUse`) and the release of the records. The allocation budgets are upper bounds and hold with master's
  constants.
  `PublicMockConsumerSlotsAtCloseTests` pins both.
- **A cancelled asynchronous poll** (ledger 632). The mock never drops a batch the C mock has delivered. A
  cancellation after the delivery is ignored and `Poll` returns the batch; one that ended the poll before any
  record was delivered throws `OperationCanceledException` and leaves the records for the next `Poll`. The real
  consumers still drop a cancelled poll's batch and seek back to it; `ConsumerAsyncCancelTests` runs that test
  over the C mock through the real path. `PublicAsyncMockConsumerCancelledPollTests` pins both sides.
- **Tests not written.** "`AddRecord` from a poll task": the .NET mocks have no `SchedulePollTask(Runnable)` (a
  known gap, `java-name-baseline.txt`). The nearest re-entrant call, `AddRecord` from a rebalance listener run
  inside `Rebalance`, is refused by the C mock with `ConcurrentModificationException` ("KafkaConsumer is not
  safe for multi-threaded access."), as the bytes `AddRecord` was before.
- **Retention.** A record the mock never delivers (below the position, a partition that left the assignment)
  keeps its entries until the consumer is closed; the XML docs of `AddRecord` say so.
- **Tests removed or narrowed** (S3; their subject was a deserializer given to a mock): the mock rows of
  `PublicSyncConsumerPreconditionTests.Constructor_NullDeserializer_ThrowsJavasConfigException`, of the
  `SerdeGuardTests` consumer theories and `AMockConsumer_TakesADeserializerWithAClose_SinceItDisposesNothing`,
  `AsyncSerdeGuardTests.AnAsyncMockConsumer_TakesEveryShape_SinceItDisposesNothing` and the mock rows of two
  headers-form theories, `PublicDeserializerDisposalTests.TheMockConsumers_NeverDisposeTheirDeserializers` and
  two mock asserts.
- **Master-only callers.** `PublicAsyncConsumerOwnWakeupTests`, `PublicConsumerNullPartitionTests` and
  `PublicProducerNullPartitionTests` (from #356) take the Java-shaped mocks;
  `RecordHeadersTests.Poll_HandsOutWritableHeadersAfterTheDeserializers` runs the real `KafkaConsumer` over the C
  mock, its subject being the decoding consumer.
