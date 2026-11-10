# Critic 120 — plan review: `design/current/PLAN-producer-transactions.md` (untracked, on `dev_dotnet-idempotency-transactions`)

Scope: the plan only (D1–D8, §2 surface, §3 phases, §4 test map, §5 risks), against the C ABI header
(`rust/target/include/confluent_kafka_bindings.h`, "BH"), Apache Kafka 4.4 in `kafka/`, the current
.NET source, `dotnet/CLAUDE.md` and `dotnet/.claude/rules/ffi-marshalling.md`. The ruled decisions
D1–D7 are not reopened; the findings below are about facts the plan relies on and about gaps.
No `COMMENTS.FP.md` / `COMMENTS.FN.md` exists, at the root or under `dotnet/`.

## Checked, no finding

- **The five `_cb` functions** exist on both `Producer` and `KafkaProducer` views
  (`kafka_producer_Producer_{init,begin,send_offsets_to,commit,abort}_transaction(s)_cb`), each
  `(self_, [offsets, group_metadata,] cb, opaque)`. `send_offsets_to_transaction_cb` takes
  `const kafka_consumer_ConsumerGroupMetadata_t *` and only clones it inside the call
  (`rust/src/ffi/producer/mod.rs` `kafka_producer_Producer_send_offsets_to_transaction_cb`), so a
  handle that is alive for the call is enough.
- **MockProducer exports:** `fence_producer`, `transaction_{initialized,in_flight,committed,aborted}_err`,
  `flushed_err`, `sent_offsets_err`, `commit_count_err`, `consumer_group_offsets_history_err`,
  `uncommitted_offsets_err`, `uncommitted_records_err`, and the five `set_*_transaction_error_err`.
- **`waitsForReports` for commit and abort only** matches `ffi-marshalling.md` §A7 ("`Flush`,
  `CommitTransaction` and `AbortTransaction` when they succeed; `InitTransactions`,
  `BeginTransaction` and `SendOffsetsToTransaction` never") and the core's `ReportWait`; the
  `commit_transaction_cb` doc says its completion waits for the reports and does not run them.
- **D2(a) is feasible.** `ConsumerGroupMetadata` has one internal constructor
  (`src/Confluent.Kafka/Consumer/ConsumerGroupMetadata.cs:28`) and one creator,
  `ConsumerGroupMetadataMarshal.CopyOutAndDestroy`, used by both `NativeConsumer.GroupMetadata()`
  (`NativeConsumer.cs:1834`) and the in-callback `NativeConsumerHandle.GroupMetadata()`
  (`NativeConsumerHandle.cs:243`). Both C sources return an owned handle freed with
  `kafka_consumer_ConsumerGroupMetadata_destroy` (BH, `Consumer_group_metadata` and
  `ConsumerHandle_group_metadata` docs). The gRPC `GroupMetadataStore` stores the objects and
  `Translate` reads only the fields. `Utf8RoundTripTests.cs:48` calls the P/Invoke directly and is
  unaffected. A `MockConsumer`'s `GroupMetadata()` works without a `group.id`: `GroupIdMissing` is
  `!IsMock && !_groupIdDefined` (`NativeConsumer.cs:1008`).
- **`NativeCallArguments.OffsetMap`** takes `NativeConsumer.CommitOffsetsSnapshot` (`:85`), so
  "generalize" is accurate. `ExpectedImportCount = 959` (`NativeMethodsPrelinkTests.cs:32`).
- **gRPC:** the proto has the five RPCs and `SendOffsetsToTransactionRequest.group_metadata_id`
  (`producer_service.proto:61-76`, `:209-214`); the three `DOTNET_GRPC_SKIPS` entries exist
  (`rust/Makefile:254-256`).
- **D5:** `acquire_txn_control` returns `local_concurrent_modification("Transactional methods of
  KafkaProducer are not safe for concurrent access.")` (`rust/src/ffi/producer/mod.rs:482-491`);
  Java 4.4 has no such text.
- **D6's core description** matches `TransactionManager::handle_cached_transaction_request_result`
  (`rust/src/producer/internals/transaction_manager.rs:2637-2665`, keyed on `is_acked`).
- **D7, KafkaProducer null group metadata:** the C function fails with
  `local_illegal_argument("Consumer group metadata could not be null")` before it reads `offsets`
  (`mod.rs` `send_offsets_to_transaction_cb`, via `precondition::fail`, which keeps the error).
- **Java 4.4 surface:** `Producer` has exactly the five transaction methods (no KIP-939 2PC
  members); `MockProducerTest` has 55 `@Test`s, as the plan says.

## Findings

### 120.1 · major · Phase 2: a `MockConsumer`'s group metadata cannot express Java's group ids

**Where:** plan §3 Phase 2 "Tests" ("Group metadata in tests comes from a `MockConsumer`'s
`GroupMetadata()`") and §4 "Port (D3)".

**Evidence:** a mock consumer's metadata is always Java's `MockConsumer.groupMetadata()` value,
`new ConsumerGroupMetadata("dummy.group.id", 1, "1", Optional.empty())`
(`MockConsumer.java:775-777`; `rust/src/consumer/mock_consumer.rs:1263`; pinned by
`PublicConsumerApiTests.cs:168`). Java's `MockProducerTest` builds metadata with chosen ids:
- `shouldPublishConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled` sends offsets for
  `"g1"` and `"g2"` in one transaction and expects a two-entry map. With one fixed id, the second
  `putAll` overwrites the first, and the expected result cannot be built.
- `shouldPreserveOffsetsFromCommitByGroupMetadataOnAbortIfTransactionsAreEnabled` uses `"g"` and
  `"g2"`. It still passes with one id, but no longer checks that the aborted group stays out.
- `shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction` expects
  `NullPointerException` from `new ConsumerGroupMetadata(null)`. That throw is in Java's
  deprecated constructor (`ConsumerGroupMetadata.java:44`, `requireNonNull(groupId, "group.id can't
  be null")`), not in the producer. .NET has no such constructor, so the test cannot be built.

The plan lists "group-offset publishing, dropping and preserving" and "null group metadata" as
straight ports.

**Fix:** in Phase 2, state the source for each test:
- When a test needs distinct ids, take the metadata from a `KafkaConsumer` that is constructed
  but never connected, with `group.id` `"g1"` / `"g2"`. Before a join its `GroupMetadata()` returns
  the configured `group.id` (the `ConsumerGroupMetadata` remarks), and D2(a) gives it a core handle.
  Otherwise use the mock's `"dummy.group.id"` and assert against it.
- Skip `shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction` with that reason. Phase
  1's D7 test of a `null` metadata argument covers the producer side.

### 120.2 · major · D7's up-front `offsets == null` check breaks six ported `MockProducerTest` tests, and the order is unrecorded

**Where:** D7 ("`offsets == null`: `ArgumentNullException` (Java dereferences it)") and §4 "Port
(D3)" (state errors, fencing, closed).

**Evidence:** Java dereferences `offsets` only after every other check:
- `KafkaProducer.sendOffsetsToTransaction` checks the group metadata, then the transaction manager,
  then closed, then calls `offsets.isEmpty()` (`KafkaProducer.java:796-800`).
- `MockProducer.sendOffsetsToTransaction` runs `requireNonNull(groupMetadata)`, `verifyNotClosed`,
  `verifyNotFenced`, `verifyTransactionsInitialized`, `verifyTransactionInFlight` and the injected
  exception, then `offsets.isEmpty()` (`MockProducer.java:193-205`).

Six `MockProducerTest` tests rely on this order. Each passes `null` offsets and expects the state
error: lines 172 and 179 (`IllegalStateException`), 282 and 290 (`ProducerFencedException`), and 641
and 648 (closed, `IllegalStateException`). A .NET check before the FFI throws
`ArgumentNullException` in all six. Passing `NULL` to the core does not help:
`send_offsets_to_transaction_cb` reads `offsets` (`offsets_map` → `require_map`) before
`with_txn_control_cb` runs the state checks.

`dotnet/CLAUDE.md`'s exception row (line 661) asks for Java's class and message, and D7 says "Java's
order", but the plan neither keeps nor records this order.

**Fix:**
1. In D7, record the deviation: a `null` `offsets` is refused before the transaction-state, fenced
   and closed checks (closed is still first, per the existing rule), because the C function reads
   it first.
2. On `KafkaProducer`, keep Java's order between the two arguments: let the core report both
   nulls. It refuses `NULL` group metadata first with `IllegalArgumentException`, then `NULL`
   offsets as -32, which is `ArgumentNullException` under `dotnet/CLAUDE.md:660`. Do not put a .NET
   `offsets` check ahead of it, or `SendOffsetsToTransaction(null, null)` throws
   `ArgumentNullException` where Java throws `IllegalArgumentException`.
3. Port the six tests with an empty dictionary instead of `null`. Java's `null` there is incidental,
   and the state checks run before the empty-map return in both Java bodies. Each port should note
   the change.

### 120.3 · minor · D6 leaves out the predictable `ConcurrentModificationException` window of a retry after a .NET cancel

**Where:** D6 ("Phase 1 tests what a retry after a .NET cancel does … a retry after the operation
finished may start a new operation rather than join").

**Evidence:** `with_txn_control_cb` takes `txn_control_busy` before it spawns the task. It releases
the flag only when the drain and `op` finish on that task (`_guard` lives inside the spawned block,
`rust/src/ffi/producer/mod.rs:1021-1050`). A .NET cancel ends only the wait, so a retry while the
core's `op` is still running, for example `InitTransactions` waiting up to `max.block.ms`, gets D5's
`ConcurrentModificationException`. It does not join the pending result as Java's retry does. The
plan describes only the "after it finished" case.

**Fix:** D6 and the Phase 1 test should name all three outcomes and pin each:
1. Retry while the core task holds the flag → `ConcurrentModificationException`, a consequence of
   D5.
2. Retry after the core timed out → joins the pending result, as Java.
3. Retry after the core succeeded → a new operation.

The interface docs should state outcome 1. Under D6's own rule it is "something Java would never
do", so it goes to the user now rather than after the Actor finds it.

### 120.4 · minor · §4 `MockProducerTest` map: wrong names and uncategorized tests

**Evidence** (the 55 names in `MockProducerTest.java`):
- **"Both 'ByGroupId' tests":** there are three. They are
  `shouldThrowOnSendOffsetsToTransactionByGroupIdIfProducerGotFenced`,
  `shouldPreserveOffsetsFromCommitByGroupIdOnAbortIfTransactionsAreEnabled` and
  `shouldThrowSendOffsetsToTransactionByGroupIdIfProducerIsClosed`. All three use
  `new ConsumerGroupMetadata(...)`.
- **"the … `ClassCastException` tests":** none exists in 4.4.
- **Uncategorized:** `testMetadataOnException`, an `errorNext` test that is probably already ported,
  and `shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled`, which checks that an abort
  completes the pending future.

**Fix:** correct the count to three and drop the `ClassCastException` claim. Put
`testMetadataOnException` under "already ported" (the Actor confirms which test covers it), and put
`shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled` under "Port". That way the Phase 2
"every test is ported or skipped" check adds up to 55.

### 120.5 · minor · §4 `KafkaProducerTest`: the four idempotence "Port" verdicts are mostly unobservable in .NET

**Evidence:**
- `testOverwriteAcksAndRetriesForIdempotentProducers` (`KafkaProducerTest.java:231-244`) only reads
  resolved values through `new ProducerConfig(props).getBoolean/getString/getInt`. That includes
  `client.id` = `producer-transactionalId`.
- The other three mix such reads with `assertThrows(ConfigException.class, …)`. Some of those
  construct a `ProducerConfig`, others a `KafkaProducer` (e.g. `:199`).
- The .NET binding has no `ProducerConfig` with getters (`src/Confluent.Kafka/Producer/` has none),
  so only the `ConfigException` cases are reachable, through `KafkaProducer` construction.

D8 ("the config checks … with exact messages") is fine. The test map calls all four "Port".

**Fix:** mark `testOverwriteAcksAndRetriesForIdempotentProducers` as skipped (no config getter in
.NET). Mark the other three as "partial port": the `ConfigException` cases, with exact messages,
through `new KafkaProducer(...)`; the value reads skipped with that reason. Also note that
`testNullGroupMetadataInSendOffsets` builds its producer through `MockClient`. The port can drop
`InitTransactions` / `BeginTransaction`, because the core's group-metadata check fires before any
state check.

### 120.6 · minor · Phase 1 tests and docs miss parts of `ffi-marshalling.md` §A7 for the transaction ops

**Evidence:**

§A7 "Tests required" asks that the transaction ops "complete through their `_cb` forms on both
flavors, and run no delivery callback on the caller's thread". It also asks for the in-callback
rules "from a sync callback and from an async one after an `await`", with the exact Python-derived
`TimeoutException` messages, and for the mock rules. Phase 1 lists only "commit and abort wait for
delivery reports; the in-callback `TimeoutException`; ops that cover sends handed over before
them".

Separately, from a sync delivery callback, all five ops end in a `TimeoutException` after
`max.block.ms`, because the completion is queued behind the waiting callback (§A7). That includes
`BeginTransaction`, which never waits in Java. Python's rules record this as a deviation ("Behaviour
with no other home": "Java's `beginTransaction` never waits"). `dotnet/CLAUDE.md` (around
`:1044-1052`) states it only for commit and abort from an async callback, and Phase 1's doc list
(D4, D5) does not add it.

**Fix:** add these to the Phase 1 tests:
- "no delivery callback runs on the caller's thread" for each op, on sync and async;
- the in-callback cases for each of the five ops from a sync callback, and for commit and abort from
  an async callback after an `await`, with exact messages, on the real producers and the mocks.

Add to the Phase 1 docs a recorded deviation: the five ops time out from a sync delivery callback,
`BeginTransaction` included, where Java's `beginTransaction` returns at once.

### 120.7 · minor · Phase 3 must rewrite the `DOTNET_GRPC_SKIPS` comment, not only delete three lines

**Evidence:** `rust/Makefile:244-253` explains the three transaction skips and says "With all five
gone, remove DOTNET_GRPC_SKIPS". After Phase 3 the two pattern skips remain, so the transaction
paragraph becomes stale. Phase 3 only says "Re-enable the three tests".

**Fix:** in Phase 3, also delete the transaction paragraph (`:244-251`) and keep the pattern-skip
text, so the comment describes only what is still skipped.
