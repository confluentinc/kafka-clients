# Plan: producer transactions and idempotence in the .NET binding

**Status:** approved. All decisions are ruled, and the plan review's findings (Critic N=120) are
applied. Phases run in order: 1 (N=121), 2 (N=122), 3 (N=123).

**Goal:** the .NET producers expose Java 4.4's five transaction methods, with Java's behaviour and
messages, on the real and the mock clients, sync and async. The mocks get Java's `MockProducer`
transaction controls. The gRPC test server runs the transaction tests that are skipped for .NET
today. Idempotence, which is config only, gets tests.

**Mode:** A (.NET only). The Rust core and the C header already have every function this plan
needs. Nothing under `rust/src`, `c/` or `python/` changes. The only file
outside `dotnet/` that changes is `rust/Makefile`, where the three skipped gRPC tests are re-enabled
(Phase 3). If a ruling in §1 picks a core change instead, that part becomes Mode B and is planned
separately.

**Background:** the facts this plan relies on are restated where they are used; the C header
(`confluent_kafka_bindings.h`) and Java 4.4 are the ground truth.

**Agent numbers:** N=120 (Critic review of this plan), N=121 (Phase 1), N=122 (Phase 2), N=123 (Phase 3).

---

## 1 · Decisions

### D1 (ruled: the recommendation) · `BeginTransaction` stays a sync `void` on both producer interfaces

Java's `beginTransaction()` only changes local state, and `dotnet/CLAUDE.md` already lists it as
sync. It calls `kafka_producer_Producer_begin_transaction_cb` and waits on a `Completion`, as every
op does (`ffi-marshalling.md` §A7). The wait is short: the C layer only drains sends that were
already handed over.

- *Alternative:* `Task BeginTransaction(CancellationToken)` on `IAsyncProducer`, as Python's async
  producer does.
- *Recommendation:* sync `void`. It is Java's shape and the existing rule; nothing changes in the
  rulebook.

### D2 (ruled: (a)) · How `SendOffsetsToTransaction` passes the group metadata to the core

The C function takes the core's `kafka_consumer_ConsumerGroupMetadata_t *`. Today the .NET
consumer copies the four fields out of that handle and frees it, so no handle is left to pass.

- **(a) Keep the core's handle.** `ConsumerGroupMetadata` keeps the fields it copies today and also
  holds the handle in a `SafeHandle`, freed when the object is collected. Python does the same.
  - Every .NET instance comes from a consumer: the constructor is internal, and Java's public
    constructors are deprecated, so they are not translated. Both `KafkaConsumer` and
    `MockConsumer` are backed by the core, so a mock consumer's metadata works too (Python's mock
    consumer cannot do this).
  - The public class gets no `IDisposable`; the handle is small.
  - The gRPC `GroupMetadataStore` keeps storing the same objects.
- **(b) Build a temporary handle on each call** with `kafka_consumer_ConsumerGroupMetadata_new`,
  which takes four function pointers. The binding must keep the delegates and UTF-8 buffers alive
  until the completion fires. More code at the boundary, and a new kind of lifetime risk.
- **(c) Add a field constructor to the C layer** (`..._new_with_fields`). Simplest for .NET, but it
  is a core change (Mode B).
- *Recommendation:* (a).

### D3 (ruled: the table below, and D3b (a)) · Which mock controls to add

The C layer has all of Java's `MockProducer` transaction members. The proposed .NET surface, on
both `MockProducer` and `AsyncMockProducer`:

| Java member | .NET |
|---|---|
| fields `initTransactionException`, `beginTransactionException`, `sendOffsetsToTransactionException`, `commitTransactionException`, `abortTransactionException` | `SetInitTransactionException(KafkaException?)` and four more, `null` clears. Same pattern as `MockConsumer.SetPollException`. Uses `ManagedExceptionCarrier`, so the caller gets back the same instance. |
| `fenceProducer()` | `FenceProducer()` |
| `transactionInitialized()`, `transactionInFlight()`, `transactionCommitted()`, `transactionAborted()`, `flushed()`, `sentOffsets()` | `bool` properties of the same names (the idiom table: a non-blocking getter is a property) |
| `commitCount()` | `long CommitCount` property |
| `consumerGroupOffsetsHistory()` | `IReadOnlyList<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>>` |
| `uncommittedOffsets()` | `IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>` |
| `uncommittedRecords()` | see D3b |

The parameter type of the setters is open. Java's fields hold any `RuntimeException`. `ErrorNext`
takes `Exception`, while `SetPollException` takes `KafkaException?`. The Actor follows whichever
rule `dotnet/CLAUDE.md` gives for mock exception fields, and records the choice.

**D3b (ruled: (a), the user left the mock's choice to the Manager) · `UncommittedRecords()`.** The C layer returns serialized records (bytes); Java returns the
typed records. .NET already has this problem with `history()`, and there it exposes only
`HistoryCount()`.
- (a) Leave `UncommittedRecords()` out, as `History()` is, and record it. Tests check the counts
  through `HistoryCount()`.
- (b) Add `History()` and `UncommittedRecords()` together as byte-level records. A new public
  shape, better designed on its own.
- *Recommendation:* (a). The four Java tests that read these lists check counts and the records'
  existence, and can be ported with `HistoryCount()`; each port says what it no longer checks.

### D4 (ruled: the recommendation) · A serializer that throws a fatal error on a transactional producer (the open D23)

In Java a serializer's `ProducerFencedException` makes the producer fatal
(`maybeTransitionToErrorState`). In .NET the serializer runs before the C call, so the core never
sees it.
- *Recommendation:* record it as a deviation, as Python does, and change nothing. Fixing it needs a
  C hook that moves the producer into an error state, which is a core change to plan separately.

### D5 (ruled) · Overlapping transaction calls on one producer

The C layer rejects a transaction call while another one is running on the same producer, with
`ConcurrentModificationException` ("Transactional methods of KafkaProducer are not safe for
concurrent access."). Java has no such check.
- *Ruling:* .NET rejects it too, by passing the C layer's error through unchanged; no check of its
  own. `dotnet/CLAUDE.md` records it as a deviation from Java, and a test pins it.

### D6 (ruled: follow Java and the Rust core) · Cancellation and retry (the async forms)

Java has no cancellation. Its nearest case is a transaction call that times out after
`max.block.ms`: the operation keeps running, a retry of the same call returns the same pending
result, and a different call is refused with `IllegalStateException` ("Cannot attempt operation
`X` because the previous call to `Y` timed out and must be retried"). The core does the same
(`TransactionManager.handle_cached_transaction_request_result`). .NET adds nothing on top:
- The core's `TimeoutException`, and the retry rules above, reach the caller unchanged.
- A `CancellationToken` only stops the .NET wait: the `Task` is cancelled with
  `OperationCanceledException` and the operation keeps running in the core, which decides what a
  later call does. A token already cancelled at entry throws before anything reaches the core.
- Dispose during an operation: the `Task` completes exactly once.

A retry after a .NET cancel has three outcomes through the `_cb` path (review 120.3). The C layer
holds its transaction flag until the core's task finishes the operation
(`rust/src/ffi/producer/mod.rs`, `with_txn_control_cb`):
1. A retry while the core is still running the operation gets D5's
   `ConcurrentModificationException`. Java's retry would join the pending result instead (D6b).
2. A retry after the core timed out joins the pending result, as in Java.
3. A retry after the core succeeded starts a new operation, with whatever the core answers for it.

Phase 1 pins all three with tests and states them on the interfaces.

**D6b (ruled: (a), as Python) · outcome 1.** Accepted and documented as a consequence of D5: a
retry while the core is still running the cancelled operation gets `ConcurrentModificationException`.
Python's async producer behaves the same (`_await_payload` raises `CancelledError` at once and the
operation finishes in the core). Only `IAsyncProducer` can hit it, since `IProducer`'s transaction
methods take no `CancellationToken`. The interface docs say so, and a test pins it.

### D7 (ruled: the recommendation) · Argument and state checks

Java's order and messages, through the binding's existing rules:
- **The two null arguments are the C layer's to report, on all four classes**; .NET adds no check
  of its own (review 120.2). The C function refuses a null `groupMetadata` first, with Java's
  `KafkaProducer` message `IllegalArgumentException("Consumer group metadata could not be null")`,
  then reads `offsets`, and only then makes its state checks.
- **Recorded deviation (order):** Java's `MockProducer` reads `offsets` only after its closed,
  fenced and state checks, so in Java a null `offsets` with a wrong state reports the state. Here
  the null is reported first. Java's mock also refuses a null `groupMetadata` with
  `NullPointerException` (`requireNonNull`), where the mocks here give the core's
  `IllegalArgumentException`. The six `MockProducerTest` tests that pass `null` offsets only to
  reach a state check are ported with an empty dictionary, each saying so.
- An empty `offsets` map: passed to the core, with no shortcut in .NET (old D13).
- No `transactional.id`, the wrong state, or a closed producer: the core's `IllegalStateException`
  with Java's message. The binding's existing "closed first" deviation applies unchanged.
- In a delivery callback: commit and abort fail after `max.block.ms` with Kafka's
  `TimeoutException` (`ffi-marshalling.md` §A7, already ruled).

### D8 · Idempotence

No API. Tests only: the config checks Java's `KafkaProducerTest` makes (acks, retries,
`max.in.flight` against `enable.idempotence`), with exact messages. All checks are the core's.

---

## 2 · The public surface

```csharp
public interface IProducer<TKey, TValue> {
    // … existing members …
    void InitTransactions();
    void BeginTransaction();
    void SendOffsetsToTransaction(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
                                  ConsumerGroupMetadata groupMetadata);
    void CommitTransaction();
    void AbortTransaction();
}

public interface IAsyncProducer<TKey, TValue> {
    // … existing members …
    Task InitTransactions(CancellationToken cancellationToken = default);
    void BeginTransaction();                                   // D1
    Task SendOffsetsToTransaction(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
                                  ConsumerGroupMetadata groupMetadata,
                                  CancellationToken cancellationToken = default);
    Task CommitTransaction(CancellationToken cancellationToken = default);
    Task AbortTransaction(CancellationToken cancellationToken = default);
}
```

The two `Kafka*` classes and the two `Mock*` classes implement them. Doc comments are copied from
Java's Javadoc and go on the interfaces. The mock controls are in D3.

---

## 3 · Phases

Each phase: Actor N, then Critic N, then fix rounds until no comments remain. Actors and Critics
use filtered test runs only. The Manager runs the full gate once per phase
(`make test-dotnet-macos-docker` on net8.0 and net10.0, plus `make -C dotnet
test-grpc-server-dotnet`) and commits. The user is the commit author, and the message has no
co-author line.

### Phase 1 · The five operations (N=121)

- **P/Invokes:** the five `_cb` functions. Blocking forms only where the mock's in-drain branch
  needs them, as `FlushBlocking` does. Bump `NativeMethodsPrelinkTests.ExpectedImportCount` and
  extend `NativeAbiConformanceTests`, pinning parameter names too.
- **`NativeProducer`:** a sync and an async form of each op, built like `Flush`:
  - closed check;
  - mock branches;
  - in-callback guard;
  - `Completion` / `AsyncCompletion` over the `_cb` call.

  `waitsForReports` is true for commit and abort only.
- **`ConsumerGroupMetadata`:** holds the core's handle (D2). The consumer's copy-and-destroy
  becomes copy-and-keep. Generalize `NativeCallArguments.OffsetMap` so the consumer's commit and
  `SendOffsetsToTransaction` share it.
- **Public surface:** §2, on all four producer classes. The mocks implement the five ops over the
  same native path. Their controls come in Phase 2.
- **Tests:**
  - D5 (overlapping calls), D6 (the cancellation table and retry-joins), D7 (every check, with
    class and exact message);
  - the required tests of `ffi-marshalling.md` §A7 (review 120.6):
    - commit and abort wait for delivery reports; every op covers sends handed over before it;
    - each op completes through its `_cb` form on sync and async, and runs no delivery callback on
      the caller's thread;
    - from a sync delivery callback, each of the five ops, and from an async callback after an
      `await`, commit and abort: Kafka's `TimeoutException` with the exact message, on the real
      producers; the mocks follow `ffi-marshalling.md` §A7's mock rules (from a sync callback the
      drain runs, the blocking form completes), tested as such;
  - un-skip `PublicProducerCloseWithTimeoutTests.OnlyCanExecuteCloseAfterInitTransactionsTimeout`
    and translate Java's body, against an unreachable bootstrap;
  - the `KafkaProducerTest` ports in §4.
- **Docs:** `dotnet/CLAUDE.md`:
  - remove "Producer transactions are not exposed yet";
  - add the members to the §3 producer sketch;
  - record D4 (closing the open D23 text), D5, D6's three retry outcomes and D7's order deviation;
  - record that from a sync delivery callback all five ops time out after `max.block.ms`,
    `BeginTransaction` included, where Java's `beginTransaction` returns at once (Python records
    the same).

  Also update `ffi-marshalling.md` if the ops add a boundary rule.
- **Done when:** the five ops work on the four classes, every test above passes, and the full gate
  is green.

### Phase 2 · Mock controls and the `MockProducerTest` port (N=122)

- **P/Invokes:** the `MockProducer` functions for D3:
  - `fence_producer`;
  - the four `transaction_*` readers;
  - `flushed`, `sent_offsets`, `commit_count`;
  - `consumer_group_offsets_history`, `uncommitted_offsets`;
  - the five `set_*_transaction_error` setters.

  Bump the import count again.
- **Surface:** D3, on `MockProducer` and `AsyncMockProducer`.
- **Tests:** port Java's `MockProducerTest` (§4), with exact classes and messages. Java's tests
  build metadata with `new ConsumerGroupMetadata(groupId)`, which is not public in .NET. A
  `MockConsumer` always reports Java's mock value `"dummy.group.id"`, so it cannot give the two
  group ids some tests need (review 120.1). The tests take metadata from a `KafkaConsumer` that never
  connects, built with `group.id` set to the id the Java test uses (`"g1"`, `"g2"`): before joining,
  it reports the configured id. The Actor confirms that with a test first.
- **Docs:** add the mock members to the §3 sketch and record D3b.
- **Done when:** every `MockProducerTest` test is ported or skipped with a stated reason, and the
  full gate is green.

### Phase 3 · gRPC server, broker tests, closing docs (N=123)

- **gRPC:** add the five RPCs to `ProducerServiceImpl` and `AsyncProducerServiceImpl`.
  `SendOffsetsToTransaction` reads the metadata from the shared `GroupMetadataStore`, and an
  unknown id is an error status. Add unit tests in `Confluent.Kafka.GrpcServer.UnitTests`.
- **Re-enable** the three tests in `rust/Makefile` `DOTNET_GRPC_SKIPS`:
  - `test_transactional_records_are_visible_only_after_commit`;
  - `test_aborted_transaction_records_are_discarded`;
  - `test_consume_transform_produce_with_offsets`.

  Locally the first needs `RUST_TEST_THREADS=6` (STATUS.md). Rewrite the comment above
  `DOTNET_GRPC_SKIPS` too: delete its transaction paragraph and keep the text about the two pattern
  skips that remain (review 120.7).
- **Integration tests** (`Confluent.Kafka.IntegrationTests`, Docker broker), modelled on Python's
  `test_transactions_broker.py`:
  - committed records are visible to `read_committed` and aborted ones are not;
  - a consume-transform-produce loop with `SendOffsetsToTransaction`;
  - a second producer with the same `transactional.id` fences the first
    (`ProducerFencedException`);
  - an idempotent producer sends without duplicates.
- **Docs:** the transactions entry in `DOTNET.md` and `STATUS.md`.
- **Done when:** the three gRPC tests pass for .NET with the skips deleted, the integration tests
  pass against the Docker broker, and the full gate is green.

---

## 4 · Test map

Ports keep Java's test names, adapted to .NET casing. A skipped test states its reason in the test
file, as the Python binding does.

**`MockProducerTest`** (55 tests, Phase 2):
- **Port (D3):** the transaction tests:
  - init, begin, commit and abort, each with its state errors;
  - fencing, for each op and for `Send`;
  - commit counting;
  - group-offset publishing, dropping and preserving;
  - the sent-offsets flag;
  - empty offsets;
  - each op on a closed producer;
  - the `flushed` tests;
  - `shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled`;
  - the six tests that pass `null` offsets, with an empty dictionary instead (D7).
- **Port with fewer checks (review 120.1):** the group-offset tests that need two group ids use two
  never-connected `KafkaConsumer`s; the preserve-on-abort test with `g` / `g2` says what it no
  longer checks.
- **Skip:** `shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction`. Its
  `NullPointerException` comes from Java's deprecated `ConsumerGroupMetadata(null)` constructor,
  which .NET does not have.
- **Port with D3b changes** (record lists replaced by `HistoryCount()`; each says what it no longer
  checks): `shouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled`,
  `shouldFlushOnCommitForNonAutoCompleteIfTransactionsAreEnabled`,
  `shouldDropMessagesOnAbortIfTransactionsAreEnabled`,
  `shouldPreserveCommittedMessagesOnAbortIfTransactionsAreEnabled`.
- **The three "ByGroupId" tests:** in Java 4.4 they call the group-metadata form, so they are
  ported under their Java names.
- **Already ported, unchanged:** the send, partitioner and manual-completion tests and
  `testMetadataOnException`. The Actor checks each against the existing
  `PublicProducerMockControlTests` / `PublicSyncProducerMockControlTests` and lists them, so the
  ported and skipped tests add up to 55.

**`KafkaProducerTest`** (transaction and idempotence tests, Phases 1 and 3). These verdicts are
tentative; the Actor confirms each against Java and lists it.
- **Port:**
  - `testNullGroupMetadataInSendOffsets`, without Java's `MockClient`: the core refuses the null
    before any state check, so the test needs no `InitTransactions` / `BeginTransaction`;
  - `testOnlyCanExecuteCloseAfterInitTransactionsTimeout` and `testInitTransactionTimeout`
    (unreachable bootstrap).
- **Partial port (review 120.5):** `testAcksAndIdempotenceForIdempotentProducers`,
  `testRetriesAndIdempotenceForIdempotentProducers`,
  `testInflightRequestsAndIdempotenceForIdempotentProducers`. The `ConfigException` cases are
  ported with exact messages, through `new KafkaProducer(..)`. The reads of resolved values are
  skipped, because .NET has no `ProducerConfig` with getters.
- **Skip, no config getters in .NET:** `testOverwriteAcksAndRetriesForIdempotentProducers`.
- **Skip, because the test needs Java's `MockClient`** (a scripted network the binding cannot
  plug in; the core's translation covers each):
  - `testInitTransactionsResponseAfterTimeout`, `testInitTransactionWhileThrottled`;
  - `testAbortTransaction`, the `testCommitTransactionWith…` tests;
  - `testSendTxnOffsetsWith…`, the `testTransactionV2…` tests, `testPartitionAddedToTransaction`;
  - the `testMeasure…Duration` tests, `testTransactionalMethodThrowsWhenSenderClosed`;
  - `testSendOffsetsToTransaction…` (V6 negotiation and bootstrap resolution).
- **Skip, because the test cannot be built in .NET:**
  `testInvalidGenerationIdAndMemberIdCombinedInSendOffsets`. It constructs metadata with chosen
  fields, which .NET cannot do (D2).

---

## 5 · Risks

- **The handle in `ConsumerGroupMetadata` (D2a).** Holding a `SafeHandle` changes a value class
  that the consumer and the gRPC store already use. The Critic checks that the handle is freed
  exactly once and that the consumer's `ConsumerHandle.GroupMetadata()` path keeps working.
- **A retry after a .NET cancel (D6)** depends on whether the core's task finished the operation
  first. Phase 1 pins it with a test; if the result is something Java would never do, the user is
  asked before any workaround.
- **The broker tests are heavy.** The first gRPC transaction test runs out of memory locally
  without `RUST_TEST_THREADS=6`.
- **Lessons from the old branch:**
  - a test filter that selects nothing exits 0, so gate on the reported test count;
  - no shell commands inside Markdown tables;
  - every error assertion checks the class and the exact message;
  - teardown tests poll `IsClosed`;
  - every doc claim is backed by a test.
