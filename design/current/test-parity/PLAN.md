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

## Phase 2 — Group limits & server assignors (Actor/Critic 66)

- `ConsumerBounceTest.testAsyncConsumerReceivesFatalExceptionWhenGroupPassesMaxSize`
  (no broker bounce needed): dedicated config `group.consumer.max.size=5`, 6th
  consumer gets `GroupMaxSizeReached`; others still consume. Only the
  default-assignment-interval arm (the `=0/1000` arms need a 4.3 broker — note it).
- `PlaintextConsumerAssignorsTest.testRemoteAssignorInvalid` and
  `testRemoteAssignorRange` (Scala, `core/.../PlaintextConsumerAssignorsTest.scala`)
  → a new or existing consumer integration file. Exact message prefix
  `"ServerAssignor invalid is not supported. Supported assignors: "`.

## Phase 3 — Consumer close timing & protocol-disabled (Actor/Critic 67)

- `PlaintextConsumerCloseTest` CONSUMER arm (2 tests) → new
  `tests/integration/plaintext_consumer_close_test.rs` (wire into `main.rs`).
- `ConsumerIntegrationTest.testAsyncConsumerWithConsumerProtocolDisabled` →
  `consumer_test.rs`, dedicated cluster config disabling the consumer protocol;
  assert exact message/variant.

## Phase 4 — Transaction fencing & state errors (Actor/Critic 68)

From `core/.../TransactionsTest.scala` (consumer arm) → `tests/integration/producer_transactions_test.rs`
(native-only; the multilanguage harness has one producer per id):
`testFencingOnCommit`, `testFencingOnSendOffsets`, `testFencingOnSend`
(fenced while p1's txn is open), `testConsecutivelyRunInitTransactions`,
`testEmptyAbortAfterCommit`.

## Phase 5 — Transaction happy paths (Actor/Critic 69)

- `ProducerIntegrationTest.testTransactionWithAndWithoutSend`,
  `testTransactionWithInvalidSendAndEndTxnRequestSent` (TV2 arm only; note TV0/TV1).
- `TransactionsTest.testOffsetMetadataInSendOffsetsToTransaction`.
- `TransactionsWithMaxInFlightOneTest.testTransactionalProducerSingleBrokerMaxInFlightOne`.
- Add the `listTransactions()` COMPLETE_COMMIT check to the existing
  `consume_transform_produce_with_offsets` (Java `testTransactionWithSendOffset`).

## Phase 6 — Vacuous / weakened tests (Actor/Critic 70)

- `producer_test.rs` `flush_sends_pending_records`: `linger.ms` large, assert
  futures not done before `flush` (Java `BaseProducerSendTest.testFlush`), where
  the backend allows (native at least).
- `test_close_with_zero_timeout_aborts_pending`: Java's 50×100 shape, not-done
  check before close, assert error variant.
- `plaintext_consumer_commit_test.rs` `test_commit_async_completed_before_consumer_closes`:
  drop the `committed()` warm-up, pre-create `__consumer_offsets` via admin as Java does.
- `test_produce_partitions_for`: also instantiate in `rust_only_fallback`.

## Phase 7 — Exact error assertions (Actor/Critic 71)

Replace substring/loose-OR checks with exact message + typed variant:
`plaintext_consumer_fetch_test.rs` (fetch invalid offset: `ConsumerNoOffsetForPartition`,
`ConsumerOffsetOutOfRange` + `offset_out_of_range_partitions()`),
`plaintext_consumer_poll_test.rs` (NoOffsetForPartition on poll zero),
`plaintext_consumer_test.rs` (partitions_for invalid topic),
`plaintext_consumer_subscription_test.rs` (`"Invalid topics: [topic abc]"`,
invalid regex variant), `producer_test.rs` (non-existent topic / invalid
partition exact timeout messages; add `testPartitionsForTimeoutErrorWhenTopicDoesNotExist`).

## Phase 8 — SSL / SASL-PLAIN clients (Actor/Critic 72)

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

## Run log

(Manager appends one line per loop iteration.)
