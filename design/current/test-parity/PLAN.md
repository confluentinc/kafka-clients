# Integration test parity — producer & consumer (Java 4.3.1 → Rust)

Branch: `test-parity/integration`. Manager-driven Actor/Critic loop, one small
phase at a time. **Agents read only the "Common rules" and their own phase
section** — do not load the other phases.

## Common rules (all phases)

- **Test-only.** Production code changes only if a test exposes a genuine
  Java-fidelity bug; keep them minimal, cite the Java line, and call them out in
  the commit message.
- Java contract: `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/...`
  and `kafka/core/src/test/scala/integration/kafka/api/...` (AK 4.3.1). Translate
  the **CONSUMER (KIP-848) arm only**; classic twins are out of scope
  (`consumer-threading.md` §20).
- Harness: `tests/common/{test_context,cluster_config,cluster_pool,kafka_cluster,test_utils}.rs`.
  An Admin client **exists** (`src/admin`, `test_utils::create_topic`) — use it
  instead of auto-create / provisioner workarounds in new code. Brokers are
  pooled per `ClusterConfig`; use a distinct config when broker props differ.
  Broker image is `apache/kafka:4.2.0`; there is **no** broker stop/restart.
- Mirror the Java test: same record counts, same timeouts, **exact error
  messages and typed error variants** (DoD §3). Any deviation gets an inline
  comment with the reason. If a test cannot pass for a real blocker (e.g. needs
  a 4.3 broker), still write it, mark `#[ignore = "<precise reason>"]`, and say
  so in the commit.
- Name: `testAsyncConsumerFoo` → `test_async_consumer_foo`, doc comment cites
  `JavaFile.java:line`. Update the target file's header list of translated tests.
- Run: `cargo test --features integration-tests --test integration <filter>`
  (Docker required), plus `cargo xtask format-check` and `cargo xtask lint`.
  Full `make verify` is not required per phase.
- Actor commits each step; Critic writes findings to `COMMENTS.<N>.md` at repo
  root; Actor moves fixed items to `COMMENTS.DONE.<N>.md` and commits with
  `fixup!` referencing the original commit.
- Critic: report only real issues (wrong/missing assertion vs Java, flaky
  construction, rule violations). No style nits.

## Phase 1 — Consumer tests added in 4.3.1 (Actor/Critic 65)

Targets:
- `PlaintextConsumerCommitTest.testAsyncConsumerNoCommittedOffsets` (Java :187)
  → `tests/integration/plaintext_consumer_commit_test.rs`
- `PlaintextConsumerCommitTest.testAsyncConsumerCommittedDeletedTopic` (Java :225,
  KAFKA-20165) → same file. Uses `Admin::delete_topics`. The client fix exists
  (`src/consumer/internals/commit_request_manager.rs`); verify whether a 4.2.0
  broker produces the behavior the test needs — if not, `#[ignore]` with reason
  "needs 4.3 broker image".
- `PlaintextConsumerAssignTest.testAsyncPollAfterTopicDeleted` (Java :304/:317)
  → `tests/integration/plaintext_consumer_assign_test.rs`. Same 4.3-broker caveat.
- Update both files' headers from "Apache Kafka 4.2" to list these tests.

## Phase 2 — Producer send basics (Actor/Critic 66)

From `core/.../BaseProducerSendTest.scala` / `PlaintextProducerSendTest.scala`
→ `tests/integration/producer_test.rs` (native `rust_only_fallback` at minimum;
multilanguage where the backend supports it):
- `testSendOffset`: full shape — null value / null key / null partition,
  `serialized_key_size` / `serialized_value_size`, 100 non-awaited sends then
  last offset == 104, callback assertions. Extend or replace
  `produce_multiple_records_ordering`.
- `testSendToPartition`: 2-partition topic via admin, send to partition 1,
  consume back and verify key/value/timestamp/offset.
- `testSendBeforeAndAfterPartitionExpansion` via `Admin::create_partitions`.
- `testBatchSizeZero` full shape (`linger.ms=MAX`) and
  `testBatchSizeZeroNoPartitionNoRecordKey`.
- `testCloseWithZeroTimeoutFromSenderThread` (native, `close` from inside a send callback).

## Phase 3 — Producer timestamps (Actor/Critic 67)

→ `producer_test.rs`, topics configured via admin `NewTopic` configs:
- `testSendCompressedMessageWithCreateTime`, `testSendNonCompressedMessageWithCreateTime`.
- `testSendCompressedMessageWithLogAppendTime`, `testSendNonCompressedMessageWithLogAppendTime`.
- `testSendWithInvalidBeforeAndAfterTimestamp`, `testValidBeforeAndAfterTimestampsAtThreshold`,
  `testValidBeforeAndAfterTimestampsWithinThreshold` (each ×2 timestamp configs).

## Phase 4 — Producer failure handling (Actor/Critic 68)

- `ProducerCompressionTest.testCompression`: Java shape (2000×3 records with/without
  key and headers, fixed timestamp, sequential offsets) + consume-back verification,
  all codecs.
- `PlaintextProducerSendTest.testSendRecordBatchWithMaxRequestSizeAndHigher` (exact boundary).
- `ProducerFailureHandlingTest`: `testCannotSendToInternalTopic`,
  `testPartitionTooLargeForReplicationWithAckAll`, `testResponseTooLargeForReplicationWithAckAll`
  (2-broker config), and run `testTooLargeRecordWithAckZero` on the small-max-bytes cluster.
- `testNonBlockingProducer`: add the send-until-queued and `BufferExhausted` halves (native).

## Phase 5 — Transaction fencing & state errors (Actor/Critic 69)

From `core/.../TransactionsTest.scala` (consumer arm) → `tests/integration/producer_transactions_test.rs`
(native-only; the multilanguage harness has one producer per id):
`testFencingOnCommit`, `testFencingOnSendOffsets`, `testFencingOnSend`
(fenced while p1's txn is open), `testConsecutivelyRunInitTransactions`,
`testEmptyAbortAfterCommit`.

## Phase 6 — Transaction happy paths (Actor/Critic 70)

- `ProducerIntegrationTest.testTransactionWithAndWithoutSend`,
  `testTransactionWithInvalidSendAndEndTxnRequestSent` (TV2 arm only; note TV0/TV1).
- `TransactionsTest.testOffsetMetadataInSendOffsetsToTransaction`.
- `TransactionsWithMaxInFlightOneTest.testTransactionalProducerSingleBrokerMaxInFlightOne`.
- Add the `listTransactions()` COMPLETE_COMMIT check to the existing
  `consume_transform_produce_with_offsets` (Java `testTransactionWithSendOffset`).

## Phase 7 — Extended transactions (Actor/Critic 71)

From `TransactionsTest.scala` / `AdminFenceProducersTest.java` → `producer_transactions_test.rs`:
- `testSendOffsetsWithGroupMetadata` full shape (500 records, alternating aborts,
  reset to committed positions, exactly-once assertion).
- `testReadCommittedConsumerShouldNotSeeUndecidedData`: two interleaved producers,
  LSO position check, `offsets_for_times` null for undecided data.
- `testDelayedFetchIncludesAbortedTransaction`, `testMultipleMarkersOneLeader`,
  `testFencingOnTransactionExpiration` (dedicated broker props).
- `AdminFenceProducersTest.testFenceAfterProducerCommit` / `testFenceBeforeProducerCommit`.

## Phase 8 — Send while topic deletion (Actor/Critic 72)

`ProducerSendWhileDeletionTest` (4 tests) → `producer_test.rs` or a new file:
2-broker config, admin replica assignment / reassignment / delete / recreate;
replace broker-internal checks with admin polling.

## Phase 9 — Producer-id / transaction expiration (Actor/Critic 73)

- `ProducerIdExpirationTest.testProducerIdExpirationWithNoTransactions`,
  `testTransactionAfterTransactionIdExpiresButProducerIdRemains`.
- `TransactionsExpirationTest` TV2 arms (2 tests).
Dedicated ClusterConfigs with short expiry props; `describe_producers` /
`list_transactions` / `describe_transactions`. Note TV1 arms as blocked.

## Phase 10 — Group limits & server assignors (Actor/Critic 74)

- `ConsumerBounceTest.testAsyncConsumerReceivesFatalExceptionWhenGroupPassesMaxSize`
  (no broker bounce needed): dedicated config `group.consumer.max.size=5`, 6th
  consumer gets `GroupMaxSizeReached`; others still consume. Only the
  default-assignment-interval arm (the `=0/1000` arms need a 4.3 broker — note it).
- `PlaintextConsumerAssignorsTest.testRemoteAssignorInvalid` and
  `testRemoteAssignorRange` (Scala, `core/.../PlaintextConsumerAssignorsTest.scala`)
  → a new or existing consumer integration file. Exact message prefix
  `"ServerAssignor invalid is not supported. Supported assignors: "`.

## Phase 11 — Consumer close timing & protocol-disabled (Actor/Critic 75)

- `PlaintextConsumerCloseTest` CONSUMER arm (2 tests) → new
  `tests/integration/plaintext_consumer_close_test.rs` (wire into `main.rs`).
- `ConsumerIntegrationTest.testAsyncConsumerWithConsumerProtocolDisabled` →
  `consumer_test.rs`, dedicated cluster config disabling the consumer protocol;
  assert exact message/variant.

## Phase 12 — Vacuous / weakened tests (Actor/Critic 76)

- `producer_test.rs` `flush_sends_pending_records`: `linger.ms` large, assert
  futures not done before `flush` (Java `BaseProducerSendTest.testFlush`), where
  the backend allows (native at least).
- `test_close_with_zero_timeout_aborts_pending`: Java's 50×100 shape, not-done
  check before close, assert error variant.
- `plaintext_consumer_commit_test.rs` `test_commit_async_completed_before_consumer_closes`:
  drop the `committed()` warm-up, pre-create `__consumer_offsets` via admin as Java does.
- `test_produce_partitions_for`: also instantiate in `rust_only_fallback`.

## Phase 13 — Exact error assertions (Actor/Critic 77)

Replace substring/loose-OR checks with exact message + typed variant:
`plaintext_consumer_fetch_test.rs` (fetch invalid offset: `ConsumerNoOffsetForPartition`,
`ConsumerOffsetOutOfRange` + `offset_out_of_range_partitions()`),
`plaintext_consumer_poll_test.rs` (NoOffsetForPartition on poll zero),
`plaintext_consumer_test.rs` (partitions_for invalid topic),
`plaintext_consumer_subscription_test.rs` (`"Invalid topics: [topic abc]"`,
invalid regex variant), `producer_test.rs` (non-existent topic / invalid
partition exact timeout messages; add `testPartitionsForTimeoutErrorWhenTopicDoesNotExist`).

## Phase 14 — SSL / SASL-PLAIN clients (Actor/Critic 78)

- `SaslPlainPlaintextConsumerTest.testAsyncConsumerSimpleConsumption` over
  `sasl_plaintext_bootstrap_servers()`.
- `BaseConsumerTest.testSimpleConsumption` over SSL.
- A small `SslProducerSendTest` subset (send offset, close, flush) over SSL.


## Later (not yet scheduled)

Stale-rationale skips (consumer metrics tests, compressed halves, rebalance
pause via `ConsumerHandle`, static-member new-partition, 4.3.1 revocation
shape); GroupAuthorizer consumer tests; ProducerSendWhileDeletion; expiration
suites; broker image bump to 4.3.x (+ assignment-interval arms); broker
stop/restart harness + fault-injection tests; stale doc/comment cleanup.
Binding gaps found: gRPC servers (python `grpc_translate.py`, C `server.cc`) return
`serialized_key_size`/`serialized_value_size` = -1, and the Python server sends a
null value as `b""` (Phase 2 / Critic 66).

## Run log

(Manager appends one line per loop iteration.)
- Phase 1 / Actor 65: 67fe726b (prod fix: wake bg task on OffsetFetch retry re-enqueue, KAFKA-20165 committed() timed out) + fd77ba85 (3 tests, all green on 4.2.0). Critic 65 reviewing.
- Phase 1 / Critic 65: 1 finding (same missing wake in commit retry drivers) → fixed in f2f5d080; re-review clean. Phase 1 DONE.
- Phase 2 / Actor 66: 8135b2ce + 5e059541 (6 tests, native green; harness waits for all partition leaders). Critic 66: 1 finding (gRPC arms: sizes -1, null value) → fixing.
- Phase 2: fixup bbd0ff4f (gRPC-arm gating); Critic 66 re-review clean. Phase 2 DONE.
- Phase 3 / Actor 67: e3be241d (7 timestamp tests, native green). Critic 67: 2 findings (linger=MAX hangs gRPC arms; LogAppendTime needs 50ms skew slack) → fixed b12144f5; re-review clean. Phase 3 DONE.
- Phase 4 / Actor 68: b151cc5e, eb51f2c0, ee3f602f (compression, failure-handling, non-blocking, max-request-size; 36 native producer tests green). Critic 68: clean. Phase 4 DONE. NOTE: local gRPC images predate a8205c5c — rebuild (make build-grpc-images) and re-run gRPC arms before merge.
