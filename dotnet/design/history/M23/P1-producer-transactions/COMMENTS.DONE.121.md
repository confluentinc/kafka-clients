# Critic 121 — resolved

### 121.1 — minor — `SendOffsetsToTransaction` with an empty map: the doc misstates Java and the core; open item (b) is not a deviation

- **Evidence:**
  - Java `KafkaProducer.sendOffsetsToTransaction` runs `throwIfInvalidGroupMetadata`,
    `throwIfNoTransactionManager` and `throwIfProducerClosed`. After those, `if (!offsets.isEmpty())`
    skips everything else, the transaction-state check included.
  - `throwIfNoTransactionManager` tests only `transactionManager == null`. The default producer is
    idempotent, so it has a manager and passes.
  - So in Java, an empty map on a default non-transactional producer, or on a transactional producer
    outside a transaction, returns without error. Java throws ("Cannot use transactional methods without
    enabling transactions by setting the transactional.id configuration property") only with
    `enable.idempotence=false`.
  - The core does exactly the same: `rust/src/producer/kafka_producer.rs`
    `KafkaProducer::send_offsets_to_transaction` returns `Ok(())` on `offsets.is_empty()` after
    `transaction_manager_or_error` and `ensure_not_closed`.
  - So the Actor's open item (b), "the core accepts it where Java throws", is wrong, and nothing should
    be recorded as a deviation.
  - But `IProducer.cs:144-145` says "An empty offsets is passed to the producer, which answers as for
    any other". That is true of the mocks, whose Java `MockProducer` checks the state first, but not of
    `KafkaProducer`/`AsyncKafkaProducer`.
  - The non-transactional theories (`PublicProducerTransactionTests.cs:77-105`) leave out
    `SendOffsetsToTransaction`, so this behaviour is not pinned.
- **Fix:**
  - Reword the remark: on `KafkaProducer`/`AsyncKafkaProducer`, an empty `offsets` returns without
    contacting the coordinator and without checking the transaction state, as Java's; on the mocks it
    gets the state checks, as Java's `MockProducer`.
  - Add tests on both real flavors:
    - an empty map with a non-null metadata on a default (idempotent) producer, and on a transactional
      producer before `InitTransactions`, completes;
    - with `enable.idempotence=false`, it throws `IllegalStateException` with the exact "Cannot use
      transactional methods …" message.
  - Do not add a deviation entry for (b).
- **Resolution (Actor 121):** per the Manager's ruling this is Java behaviour, not a deviation. `IProducer.SendOffsetsToTransaction` remarks (shared by the async interface) now say: with an empty `offsets`, the real producers return without contacting the coordinator and without checking the transaction state, as Java's `KafkaProducer` does; beyond the null checks they throw only on a closed producer or one with no transaction support (`enable.idempotence=false`); the mocks check their state first, as Java's mock does. `dotnet/CLAUDE.md` §4 **Producer transactions** says the same. Pinned in `PublicProducerTransactionTests`: `SendOffsetsToTransaction_EmptyOffsets_OnAnIdempotentProducer_Completes`, `..._BeforeInitTransactions_Completes` and `..._WithoutIdempotence_ThrowsJavasIllegalState` (exact class and message), each on `KafkaProducer` and `AsyncKafkaProducer`.

### 121.2 — minor — The in-callback remarks say all five calls time out on every implementation; the mocks' drain path completes, and is only partly tested

- **Evidence:**
  - Three remarks say every transaction method times out from inside a delivery callback:
    - `IProducer.cs:79-81`: "Called from inside a delivery callback, it waits at most `max.block.ms`,
      then throws … The same holds for the other four transaction methods." It is on the interface, so
      it covers `MockProducer`.
    - `IAsyncProducer.cs:112-114` says the same.
    - `IAsyncProducer.cs:193,218` say the same of commit and abort.
  - But from a mock's sync callback that the drain runs, all five take the blocking form and complete
    (`NativeProducer.Transactions.cs:145-149,166-177`; `dotnet/CLAUDE.md:1180-1181`; §A7).
  - The interfaces already state mock differences elsewhere (`IProducer.cs:43,272,319`).
  - Open item (a) is documented in `dotnet/CLAUDE.md` but tested only for commit and abort, on
    `MockProducer` (`MockProducer_CommitOrAbort_FromASyncCallbackTheDrainRuns_Succeeds`). Not tested:
    - `InitTransactions`, `BeginTransaction` and `SendOffsetsToTransaction` from that callback;
    - `AsyncMockProducer`'s sync callback, which `MockProducer_CommitOrAbort_CompletesThePendingSends_OnTheCompletionThread`
      shows also runs on the drain.
  - The plan's §A7 test bullet says the mocks' rule is "tested as such", for the five ops.
- **Fix:**
  - Add to each of those remarks: "On `MockProducer`/`AsyncMockProducer`, from a sync callback that the
    drain runs, the call completes."
  - Make the test a theory over the five ops × both mock flavors. For the async mock, block on the
    `Task` inside the sync callback. Assert success, the completion thread, and the mock state each op
    leaves.
- **Resolution (Actor 121):** the `InitTransactions` remarks of `IProducer` and `IAsyncProducer` now state that on a mock, from a delivery callback, the calls complete as Java's mock does (`BeginTransaction`, `SendOffsetsToTransaction`, `CommitTransaction` and `AbortTransaction` point there). The test is now the theory `MockProducer_TransactionCall_FromASyncCallbackTheDrainRuns_Completes` over the five ops × both mocks, asserting success, the thread (completion thread for `MockProducer`, the pump thread for `AsyncMockProducer`), a bound well under `max.block.ms`, and the state each op leaves (`AssertStateAfter`).

### 121.3 — minor — `AsyncMockProducer`: from a callback the core cannot see, the five calls wait the mock's 60 s `max.block.ms`, then time out; this is not recorded

- **Evidence:**
  - Some callbacks run outside the drain:
    - a sync `IDeliveryCallback` queued behind an async one;
    - an async callback's synchronous part queued behind another.
  - Both resume through `DeliveryState` → `_reporter.Post` → `CompletionThread.Post`. That runs on the
    completion thread outside `Drain()`, so `IsDraining` is false
    (`CompletionThread.cs:70,197-213,215-222`).
  - `Transaction`/`TransactionWithCallback` then take the `_cb` path with
    `WaitTimeout`/`ReportWaitTimeout` = `MaxBlock`, because `IsCurrentThread` is true. The completion is
    queued behind the running callback, so the call ends after `max.block.ms` with Kafka's
    `TimeoutException`.
  - For a mock, which reads no config, that is `DefaultMaxBlockMs = 60000` (`NativeProducer.cs:44,133`).
    Java's `MockProducer` completes these calls at once.
  - §A7 records this "core cannot see" case for `Flush` (it runs Java's flush body). For transactions it
    records only the async-callback-after-`await` commit/abort deviation, and
    `dotnet/CLAUDE.md` §4 **Producer transactions** says nothing.
  - So a user's mock test that mixes sync and async callbacks stalls for a minute with no documented
    reason.
- **Fix:**
  - Record it as a deviation, in the §A7 mock bullet and in `dotnet/CLAUDE.md` §4 **Producer
    transactions** ("from a mock callback the core cannot see, the five calls end after `max.block.ms`,
    60000 on a mock, with Kafka's `TimeoutException`").
  - Pin it with a test using the existing `SetMaxBlockMs` helper: a sync callback queued behind a held
    async callback calls `CommitTransaction`.
  - Alternatively, if the Manager prefers Java's mock behaviour, route this case as `Flush`'s mock path
    is routed. Either way, the behaviour must be stated.
- **Resolution (Actor 121):** Manager preferred the `Flush` routing. Done where it needs no C-ABI change: `NativeProducer.Transactions.cs` `MockTakesTheBlockingForm` now sends `InitTransactions`, `BeginTransaction` and `SendOffsetsToTransaction` from a mock callback the core cannot see to the blocking form, whose `with_txn_control` uses `ReportWait::No`, runs no queued callback and completes at once, as Java's mock does. Not feasible for `CommitTransaction`/`AbortTransaction`: their blocking form waits (`ReportWait::OnSuccess`) for every unreported callback, the calling one included, so from inside a callback it would never return; only a C form that skips the report wait would fix it. Those two stay on the `_cb` form and end after `max.block.ms` (60000 on a mock) with Kafka's `TimeoutException`, recorded as a deviation in `dotnet/CLAUDE.md` §4 **Producer transactions**, `ffi-marshalling.md` §A7 and the `IAsyncProducer` remarks. Pinned by `AsyncMockProducer_TransactionCall_FromASyncCallbackQueuedBehindAnAsyncOne` (sync callback queued behind a held async one, `SetMaxBlockMs(500)`): the three complete with state asserted; commit/abort throw `KafkaTimeoutException` code 7 with the completion-thread message for `EndTxn(true)`/`EndTxn(false)`. Reverting the routing makes the three Init/Begin/SendOffsets cases fail.

### 121.4 — minor — D6 "Dispose during an operation: the `Task` completes exactly once" has no test

- **Evidence:**
  - Plan D6, third bullet, and Phase 1 "Tests: D6 (the cancellation table …)" require it.
  - No test disposes a producer while a transaction call is waiting. The only teardown-after-call tests
    (`CloseNow` after a cancelled or timed-out call) dispose after the wait has already ended.
  - The guarantee rests on the core keeping the spawned task and running its `deliver` before destroy
    completes. Nothing in the binding pins that the managed `Completion`/`AsyncCompletion` is then
    released, rather than left waiting with an infinite timeout. The sync off-callback path uses
    `Timeout.InfiniteTimeSpan` (`NativeProducer.cs:707,710`).
- **Fix:** add a test per flavor that runs against the unreachable bootstrap with
  `transactional.id` and `max.block.ms=3000`:
  1. Start `InitTransactions` (sync on a `Task.Run`, async directly).
  2. After ~100 ms, call `Dispose`/`DisposeAsync` under `TestTimeout`.
  3. Assert that the call ends within `Deadline` with the exact class and message the core delivers, and
     that `Dispose` returns.
- **Resolution (Actor 121):** added `InitTransactions_DisposeDuringTheWait_EndsTheCallOnce_AndDisposeReturns` on `KafkaProducer` (sync `InitTransactions` on `Task.Run`) and `AsyncKafkaProducer`: unreachable bootstrap, `transactional.id`, `max.block.ms=1000`; dispose after ~100 ms under `TestTimeout`. Observed and pinned: the graceful close does not fail the pending init; the call ends once, within the deadline, with Kafka's `TimeoutException` (code 7) and the core's exact message ("Timeout expired after 1000ms while awaiting InitProducerId. InitTransactions timed out - …"), and dispose returns.

### 121.5 — minor — `dotnet/CLAUDE.md` says the binding adds only its closed check; it also checks the offsets' contents, and that check is untested here

- **Evidence:**
  - `dotnet/CLAUDE.md:1153-1154`: "The core makes every check … the binding adds only its closed check,
    first".
  - `TransactionOffsets` runs `OffsetsSnapshot.Of(offsets)`. It throws `ArgumentException` "Topic names
    must not be null." / "Offset value must not be null." before the core sees the map
    (`NativeProducer.Transactions.cs:224-229`, `OffsetsSnapshot.cs`).
  - `IProducer.cs` documents that `ArgumentException`. No transaction test exercises it; only the
    consumer's commit tests do.
- **Fix:**
  - State the shared offsets-content check in the `dotnet/CLAUDE.md` paragraph (it is the consumer
    commit's, reused).
  - Add a `SendOffsetsToTransaction` test with a null `OffsetAndMetadata` value, asserting the
    `ArgumentException` class and exact message on one real and one mock flavor.
- **Resolution (Actor 121):** `dotnet/CLAUDE.md` §4 **Producer transactions** now states the binding's second check on `SendOffsetsToTransaction`, the consumer commit's offsets-content check (`ArgumentException` "Topic names must not be null." / "Offset value must not be null."), made before the core since the map is built from the managed dictionary. Added `SendOffsetsToTransaction_NullOffsetValue_ThrowsArgumentException` on `KafkaProducer` (real) and `AsyncMockProducer` (mock), asserting the class, `ParamName == "offsets"` and the exact message.

### 121.6 — major — the gRPC server's unit-test producers no longer compile (found by the Manager's gate)

- **Evidence:** `make -C dotnet test-grpc-server-dotnet` failed with CS0535: `SyncTestProducer` and
  `AsyncTestProducer` (`tests/Confluent.Kafka.GrpcServer.UnitTests/Fixtures/`) implement `IProducer` /
  `IAsyncProducer` and lacked the five transaction members. The Actor and the Critic built only the
  library and its unit tests.
- **Resolution (Manager):** both fakes forward the five members to their inner mock, as they forward
  every other call; the async fake records each call's token on its probe, as its other async members do.
