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
  Broker image is `apache/kafka:4.2.0`. Broker stop/restart exists since Phase 15
  (`ClusterConfig::kraft_dedicated(brokers, controllers)` + `ctx.cluster()`
  `shutdown_broker` / `start_broker` / `alive_broker_ids` / `wait_for_ready_brokers`,
  Java `ClusterInstance` equivalents; only on dedicated `Type::Kraft` clusters).
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


## Phase 15 — Harness: broker lifecycle (Actor/Critic 79)

Goal: let integration tests stop/start individual brokers, mirroring Java's
`ClusterInstance` (`kafka/test-common/test-common-runtime/src/main/java/org/apache/kafka/common/test/ClusterInstance.java`:
`shutdownBroker(id)`, `startBroker(id)`, `brokers()` / `aliveBrokers()`,
`brokerIds()`, `controllers()`, `waitForReadyBrokers()`, `type()`), as close to
Java as the Docker harness allows.
- **Isolated controllers (Java `Type.KRAFT`).** Current nodes are combined
  broker+controller, so stopping one can lose the KRaft quorum. Add a cluster
  mode with dedicated controller container(s) and broker-only containers (Java
  default for `@ClusterTest` is KRAFT with 1 controller). Keep the existing
  combined mode (`CO_KRAFT`) for existing tests unchanged.
- **Dedicated (non-pooled) clusters** for any test that stops brokers, so a
  stopped broker never leaks into other tests; torn down when the test ends.
- **Restart fidelity:** same broker id, same host ports (bootstrap addresses
  stay valid), same data dir (no reformat) — testcontainers `stop`/`start` on
  the held `ContainerAsync`, or equivalent.
- `wait_for_ready_brokers()` after start (admin `describe_cluster` shows it).
- Smoke integration tests: stop a broker → admin shows it gone and ISR shrinks;
  start it → rejoins and ISR recovers; controller stays available.
- Must not slow down or change existing pooled tests.

## Phase 16 — Consumer fault injection (Actor/Critic 80)

Using Phase 15: `PlaintextConsumerTest.testAsyncConsumerCloseOnBrokerShutdown`,
`testAsyncConsumeCoordinatorFailover`; `ConsumerIntegrationTest.testLeaderEpoch`;
un-ignore `plaintext_consumer_commit_test.rs` `test_commit_async_fails_when_coordinator_unavailable_during_close`
and give it the real broker shutdown Java performs.

## Phase 17 — ConsumerBounceTest CONSUMER arms (Actor/Critic 81)

`testAsyncConsumerConsumptionWithBrokerFailures`,
`testAsyncConsumerSeekAndCommitWithBrokerFailures` (high watermark via admin
`list_offsets` instead of replicaManager), `testAsyncSubscribeWhenTopicUnavailable`,
`testAsyncClose` → `consumer_bounce_test.rs`.

## Phase 18 — Producer fault injection (Actor/Critic 82)

`ProducerFailureHandlingTest.testNotEnoughReplicasAfterBrokerShutdown`;
`BaseProducerSendTest.testSendToPartitionWithFollowerShutdownShouldNotTimeout`;
`TransactionsTest` `testInitTransactionsTimeout`, `testSendOffsetsToTransactionTimeout`,
`testCommitTransactionTimeout`, `testAbortTransactionTimeout`, `testFailureToFenceEpoch`.

## Phase 19 — Rebootstrap & bounce (Actor/Critic 83)

`ClientRebootstrapTest` producer + consumer (enabled / disabled);
`TransactionsBounceTest.testWithGroupMetadata` (scale down only if needed, documented);
`TransactionsTest.testBumpTransactionalEpochWithTV2Enabled` if Phase 15 supports it.

## Phase 20 — Harness hardening (Actor/Critic 84)

- Cluster start sometimes panics at `kafka_cluster.rs` `start_with_config` on a non-retried
  Docker API error (observed: "failed to list networks: Timeout"; once more with an
  uncaptured error). Classify Docker daemon/API timeouts as transient and retry; include
  the error text in the panic.
- A failed start attempt can leave broker containers in Docker `Created` state — reap
  them (and the attempt's network) on the failure path.
- Verify with repeated dedicated-cluster starts.

## Phase 21 — Transaction-version arms (Actor/Critic 85)

`Admin::update_features(transaction.version, SafeDowngrade)` on a dedicated cluster works
(Phase 18). Translate the skipped TV1 (and TV0 if the downgrade is accepted) arms:
`ProducerIntegrationTest` testTransactionWithAndWithoutSend /
testTransactionWithInvalidSendAndEndTxnRequestSent / testTransactionWithSendOffset (TV0, TV1),
`TransactionsExpirationTest` TV1 arms, `TransactionsTest.testBumpTransactionalEpochWithTV2Disabled`
if it needs no broker internals, `TransactionsTest.testEmptyAbortAfterCommit` TV1 row. Update
the "TV2 only" comments left in Phases 5-9.

## Phase 22 — Auto-create races in pre-existing consumer tests (Actor/Critic 86)

Final checkpoint (after Phase 21): 302 passed / 1 failed —
`plaintext_consumer_poll_test::test_async_consumer_max_poll_records` (record timestamp +1300ms
→ reordered records; passes 5/5 alone). The test produces into an auto-created topic on the
3-broker pooled cluster; Java's `@BeforeEach` creates the topic via `createTopic` (waits for
all brokers). Same root cause as Phase 2 (NOT_LEADER retries reorder with idempotence off).
Fix: in the plaintext_consumer_* suites, where Java pre-creates the topic, create it via
admin + `wait_for_partition_leaders` before producing (shared helper per file).

## Later (not yet scheduled)

Stale-rationale skips (consumer metrics tests, compressed halves, rebalance
pause via `ConsumerHandle`, static-member new-partition, 4.3.1 revocation
shape); GroupAuthorizer consumer tests; ProducerSendWhileDeletion; expiration
suites; broker image bump to 4.3.x (+ assignment-interval arms); broker
stop/restart harness + fault-injection tests; stale doc/comment cleanup.
Binding gaps found: gRPC servers (python `grpc_translate.py`, C `server.cc`) return
`serialized_key_size`/`serialized_value_size` = -1, and the Python server sends a
null value as `b""` (Phase 2 / Critic 66).
Harness: a failed cluster start attempt can leave broker containers in Docker `Created`
state (seen once in Phase 17; removed manually) — reap them on the failure path.

## Production divergences found (not fixed — need a decision)

- **Background auto-commit (Phase 17 / Critic 81):** Rust `CommitRequestManager::poll`
  (`src/consumer/internals/commit_request_manager.rs:1389`) calls `maybe_auto_commit_async`
  on every background-loop iteration. Java's `CommitRequestManager.poll` never auto-commits;
  auto-commit only fires from app-side `poll()` (`CommitRequestManager.java:181-203`, via the
  app-thread poll event). Effect: Rust commits ~auto.commit.interval after construction even if
  the app stops polling. Not changed on this branch (behaviour change outside test scope).

- **Admin client ignores `metadata.recovery.strategy` (Phase 19 / Critic 83):**
  `src/admin/kafka_admin_client.rs:368` hardcodes `MetadataRecoveryStrategy::None`;
  `AdminClientConfig` doesn't parse the recovery keys; Java reads it at
  `KafkaAdminClient.java:635` and rebootstraps at `:731`. Producer/consumer were fixed in
  891571d0. Blocks `ClientRebootstrapTest.testAdminRebootstrap{,Disabled}` (admin scope).

## Run log

(Manager appends one line per loop iteration.)
- Phase 1 / Actor 65: 67fe726b (prod fix: wake bg task on OffsetFetch retry re-enqueue, KAFKA-20165 committed() timed out) + fd77ba85 (3 tests, all green on 4.2.0). Critic 65 reviewing.
- Phase 1 / Critic 65: 1 finding (same missing wake in commit retry drivers) → fixed in f2f5d080; re-review clean. Phase 1 DONE.
- Phase 2 / Actor 66: 8135b2ce + 5e059541 (6 tests, native green; harness waits for all partition leaders). Critic 66: 1 finding (gRPC arms: sizes -1, null value) → fixing.
- Phase 2: fixup bbd0ff4f (gRPC-arm gating); Critic 66 re-review clean. Phase 2 DONE.
- Phase 3 / Actor 67: e3be241d (7 timestamp tests, native green). Critic 67: 2 findings (linger=MAX hangs gRPC arms; LogAppendTime needs 50ms skew slack) → fixed b12144f5; re-review clean. Phase 3 DONE.
- Phase 4 / Actor 68: b151cc5e, eb51f2c0, ee3f602f (compression, failure-handling, non-blocking, max-request-size; 36 native producer tests green). Critic 68: clean. Phase 4 DONE. NOTE: local gRPC images predate a8205c5c — rebuild (make build-grpc-images) and re-run gRPC arms before merge.
- Phase 5 / Actor 69: 69323353 (5 TransactionsTest fencing/state tests, green 3x; wait_for_partition_leaders moved to test_utils). Critic 69: clean. Phase 5 DONE.
- Phase 6 / Actor 70: 3db40d8c (4 txn happy-path tests + listTransactions check; txn_single_broker moved to cluster_config). Critic 70: clean. Phase 6 DONE.
- Phase 7 / Actor 71: 4f91fd6c, 5bb64773, c795b089 (7 extended txn + AdminFence tests; 31 txn+admin-txn green). Critic 71: clean. Phase 7 DONE.
- Phase 8 / Actor 72: e9fa1395 (4 ProducerSendWhileDeletion tests, 5/5 runs each; new file). Critic 72: clean. Phase 8 DONE.
- Phase 9 / Actor 73: 3af420c2, 5a4853bc (4 expiration tests; stale idempotent-PID comment fixed). Critic 73: 1 finding (describe_producers race, 1/20 flake) → fixed af96b13b (10/10). Phase 9 DONE. Producer block (2-9) complete.
- Checkpoint after phase 9: format-check + lint + cargo test --features integration-tests all green (unit 4034, integration 248 passed / 1 pre-existing ignore).
- Phase 10 / Actor 74: 002aa2c0 (group max size + remote assignors; ConsumerAssignmentPoller fixture; 4.3 arms #[ignore]). Critic 74: clean. Phase 10 DONE.
- User (2026-09-24): continue all phases unattended; then solve broker-lifecycle harness as close to Java as possible → added Phases 15-19.
- Phase 11 / Actor 75: 104c35d7 (PROD fix: consumer close passes remaining close timer to bg cleanup, AsyncKafkaConsumer.java:1652), d17de23e (close tests), 4875d2fc (protocol disabled via update_features). Critic 75: 1 finding (cleanup must poll once at 0ms like sendUnsentRequests do-while) → fixed d2d489ea; re-review clean. Phase 11 DONE.
- Phase 12 / Actor 76: 869659ce (flush / close-zero Java shape; partitions_for un-gated), 3e4a2fb9 (no coordinator warm-up; pre-create __consumer_offsets). Critic 76: clean. Phase 12 DONE.
- Phase 13 / Actor 77: 7ba4b551, b5b45912, f7305af9, d9dcda08, cc704bc0 (exact messages + typed variants; added testPartitionsForTimeoutErrorWhenTopicDoesNotExist). Critic 77: 1 low finding (invalid-topic loop must retry like retryOnExceptionWithTimeout) → fixed 9c27aa11. Phase 13 DONE.
- Phase 14 / Actor 78: 56c2d387 (SASL_PLAINTEXT + SSL testSimpleConsumption, shared base_consumer_test.rs), 5adf6d8c (SSL send-offset/close/flush native; Java-shaped testClose). Critic 78: clean. Phase 14 DONE.
- Phase 15 / Actor 79: cd653ef1 (Kraft isolated controllers, dedicated clusters, lifecycle API), 1e630fe0 (smoke tests). Full suite 265 passed / 3 ignored x2. Critic 79 reviewing.
- Phase 15: Critic 79 1 finding (wait_for_ready_brokers not pinned per broker) → fixed 1ba63a5c (raw Metadata v12 per broker); re-review clean. Phase 15 DONE.
- Phase 16 / Actor 80: 5be6ef9c (un-ignored commit-async-during-close w/ real shutdown), 4be18e85 (testLeaderEpoch), 220e220b (harness BrokerProxy: stopped broker refuses/closes conns like in-JVM broker; fixes Docker Desktop forwarder hangs), 4f50cf28 (coordinator failover, close on broker shutdown). Full suite 269 passed / 2 ignored. Critic 80: clean (proxy not masking a client bug: Java close() default 30s timer). Phase 16 DONE.
- Phase 17 / Actor 81: 0289681f, 738b9ba9, 7e16d75a (4 ConsumerBounceTest arms, 5/5 each). Critic 81: 1 finding (coordinator lookup relied on Rust-only background auto-commit) → fixing in test; production divergence logged above.
- Phase 17: fixup f2bdd189 (coordinator via __consumer_offsets p0 leader). Phase 17 DONE.
- Phase 18 / Actor 82: 2f3771a9 (NotEnoughReplicas, follower shutdown), a6a2ecbb (4 txn timeouts, failure-to-fence TV2). Critic 82: 1 finding (TV2 fence row vacuous; TV1 reachable via update_features) → fixed 8370c280 (TV1 row added, downgrade works). Phase 18 DONE. Added Phases 20 (harness hardening) and 21 (TV0/TV1 arms).
- Phase 20 / Actor 84: 69376c92 (retry transient Docker API errors w/ backoff; reap failed-attempt containers+network). Critic 84: 1 finding (reap test leaked on failure) → fixed e003e32e. Phase 20 DONE.
- Phase 19 / Actor 83: 891571d0 (PROD fix: producer/consumer honor metadata.recovery.strategy + trigger, Java defaults), 63cca50d (4 rebootstrap tests), 24e03b13 (TransactionsBounceTest full scale), 97d7686f (epoch bump TV2). Critic 83 (2nd run; 1st stalled): 1 finding (admin ignores recovery strategy) → logged as divergence (admin scope). Phase 19 DONE.
- Phase 21 / Actor 85: fa4da3ed, b0ee9451, e61fe83a (TV0/TV1 arms: ProducerIntegrationTest, TransactionsExpirationTest, BumpTransactionalEpochWithTV2Disabled; TV0 downgrade works). Critic 85: 1 finding (downgrade wait not pinned per broker) → fixed 8602c84b. Open: TV2Disabled 1/30 ProducerFenced flake before the fix (not reproduced since; likely broker-side race Java shares). Phase 21 DONE.
- Phase 22 / Actor 86: 229f2ff5, 87295798, 8c41e7fb, 85cc188c, 5ab5b036, b5c2a0cb (admin-created topics + leader wait where Java pre-creates; exact subscription asserts restored). Critic 86: clean. Phase 22 DONE.
