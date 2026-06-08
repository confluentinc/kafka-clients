# Phase 13: Translate 12 remaining Java consumer integration suites

## Goal

Translate the **12 Java consumer integration test suites** in `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/` (~6,625 LoC, ~229 `@ClusterTest` methods) into Rust integration tests that exercise `AsyncKafkaConsumer` end-to-end against the existing testcontainers Kafka 4.2.0 harness, mirroring the shape of the four tests already in `tests/integration/consumer_test.rs`. KIP-848 only: every test paired across `(classic, consumer)` is translated **only for the `consumer` arm** per `consumer-threading.md` §20, halving the effective method count to ~117. Out-of-scope categories (classic-only, share-consumer, legacy v0/v1 message format, metrics-suite, Java-internals-only) are SKIPped with one-line rationales per DoD §3. Harness gaps (multi-broker, broker restart, SASL_PLAINTEXT through the production ctor) are built in `tests/common/` as they are needed.

## Branch

Lands on `consumer-impl` directly. **No worktree.** Matches Phase 12 and Phase 12.5: this phase is additive to the same long-lived branch carrying Milestone-8 work; no further branching is justified.

## Java sources

Pinned commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b` (same as all of Milestone-8). All files under `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/`:

| Java file | LoC | `@ClusterTest`s | CONSUMER-arm count | Broker count required |
|---|---|---|---|---|
| `ConsumerBounceTest.java` | 814 | 12 | 6 | **3** (`BROKER_COUNT = 3`) |
| `ConsumerIntegrationTest.java` | 402 | 9 | 6 | 1 / 2 / **3** (per-test) |
| `ConsumerTopicCreationTest.java` | 125 | 4 (`@ClusterTemplate`) | 2 | 1 (default KRAFT) |
| `PlaintextConsumerAssignTest.java` | 315 | 16 | 8 | **3** |
| `PlaintextConsumerCallbackTest.java` | 371 | 19 (paired-CT, not @Parameterized) | ~10 | **3** |
| `PlaintextConsumerCommitTest.java` | 636 | 21 | ~11 | **3** |
| `PlaintextConsumerFetchTest.java` | 489 | 18 | 9 | **3** |
| `PlaintextConsumerPollTest.java` | 680 | 24 | 12 | **3** |
| `PlaintextConsumerSubscriptionTest.java` | 628 | 22 | ~11 | **3** |
| `PlaintextConsumerTest.java` | 1785 | 74 | 37 | **3** (some tests need 1; 1 test calls `cluster.shutdownBroker(0)`) |
| `SaslPlainPlaintextConsumerTest.java` | 156 | 6 | 3 | **3** (`ClientsTestUtils.BaseConsumerTestcase.BROKER_COUNT = 3`) |
| `ConsumerWithLegacyMessageFormatIntegrationTest.java` | 224 | 4 | 2 | **3** |
| **Total** | **6,625** | **229** | **~117 CONSUMER-arm** | — |

The Java tests do NOT use `@ParameterizedTest(GroupProtocol)` — they pair two `@ClusterTest`s per logical test (`testClassicConsumerX` + `testAsyncConsumerX`). The KIP-848 rule applies cleanly: translate the `testAsyncConsumer*` half, SKIP the `testClassicConsumer*` half.

## Scope decisions

Bucketed at the **suite level** with method-class call-outs where a single suite is mixed. Categories per the brief:

- **T** = TRANSLATE (KIP-848-compatible, harness supports today).
- **T+H** = TRANSLATE-AFTER-HARNESS-WORK (KIP-848-compatible but needs new test infrastructure — names the gap).
- **S** = SKIP with rationale per DoD §3.

### 1. `ConsumerBounceTest.java` (12 → 6 CONSUMER methods)

- **T+H, all 6 CONSUMER methods.** Every test calls `cluster.shutdownBroker(...)` / `cluster.startBroker(...)` and/or relies on `BROKER_COUNT = 3`. **Harness gaps:** (a) multi-broker cluster (Phase-13 harness work item H1), (b) per-broker `shutdown_broker(node_id)` / `start_broker(node_id)` helpers driving docker stop/start on the broker's container ID (H2), (c) `find_coordinator(group_id)` helper that maps a group to its hosting broker (H3 — used in `testCloseDuringRebalance` and `testConsumerReceivesFatalExceptionWhenGroupPassesMaxSize`).
- Methods in scope after translation: `testConsumerReceivesFatalExceptionWhenGroupPassesMaxSize`, `testCloseDuringRebalance`, `testClose` (CONSUMER), `consumeWithBrokerFailures` (CONSUMER), `seekAndCommitWithBrokerFailures` (CONSUMER), `testSubscribeWhenTopicUnavailable` (CONSUMER).
- **SKIP** the 6 CLASSIC-named twin methods.

### 2. `ConsumerIntegrationTest.java` (9 → 6 CONSUMER methods)

- **T+H mixed**:
  - `testAsyncConsumerWithConsumerProtocolDisabled` — **T+H**: cluster needs `group.coordinator.rebalance.protocols=classic` only, which `ClusterConfig::with_properties` already supports. **T.**
  - `testFetchPartitionsAfterFailedListenerWithGroupProtocolConsumer` + `testFetchPartitionsWithAlwaysFailedListenerWithGroupProtocolConsumer` — **T**: need `ConsumerRebalanceListener` impls that throw. Trait exists; the Rust impl returns a `Result`, but Java's `onPartitionsAssigned` throws unchecked. Translate "throw" as `Err(KafkaError::other(...))` or as a tracked-via-Mutex flag that propagates an error path. Audit during prep (needs reading during Phase 13 prep).
  - `testLeaderEpoch` — **T+H**: requires `brokers = 3`, `cluster.shutdownBroker(...)`, `cluster.getLeaderBrokerId(targetTopicPartition)` (H4 — leader lookup via Metadata RPC), and `ConsumerRecord::leaderEpoch()` accessor. **Audit `ConsumerRecord` API surface during prep** — Phase 7 should have wired `leader_epoch` through; verify.
  - `testRackAwareAssignment` — **S**: uses `RackAwareAssignor` (client-side assignor — out of scope per `consumer-threading.md` §20) and requires `admin.createPartitions(...)`. Rationale: classic-protocol-only assignor surface.
  - The remaining CONSUMER-named methods (2 of the 6) need reading during Phase 13 prep to confirm.

### 3. `ConsumerTopicCreationTest.java` (4 → 2 CONSUMER methods)

- **T+H, 2 CONSUMER methods**: `testAsyncConsumerTopicCreationIfConsumerAllowToCreateTopic`, `testAsyncConsumerTopicCreationIfConsumerDisallowToCreateTopic`.
- **Harness gap H5**: assertion uses `admin.listTopics()` to check whether `TOPIC` exists. **No Admin client in `src/` today.** Two options for the harness:
  - **(a)** Use a separate consumer instance and call `consumer.list_topics()` (already exists in Rust API) — this is what Java's check is verifying anyway, and Java's `admin.listTopics()` is just the most convenient API to call. Behavior-preserving substitution.
  - **(b)** Add a minimal `docker exec kafka-topics.sh --list` invocation in `tests/common/` (gross but unblocking).
  - Recommend (a).
- **SKIP** the 2 CLASSIC-named twins.

### 4. `PlaintextConsumerAssignTest.java` (16 → 8 CONSUMER methods)

- **T**: All 8 CONSUMER methods are assignment-only API tests (`assign`, `pause`, `resume`, `seek`, `position`, `committed`, `paused`, `assignment`). All these Rust APIs exist (audit shows `pub async fn pause/resume/position/committed/seek/seek_to_beginning` present). Needs **3 brokers** (`BROKER_COUNT = 3`) — H1 harness gap, but harness already trivially supports it via `ClusterConfig::with_brokers(3)`.
- Some sub-methods may use `consumer.metrics()` — call those out as **S** (metrics out of scope Milestone-8-wide). Audit during prep.

### 5. `PlaintextConsumerCallbackTest.java` (19 → ~10 CONSUMER methods)

- **T+H**: All CONSUMER methods exercise `ConsumerRebalanceListener` callbacks (Rust trait exists at `src/consumer/consumer_rebalance_listener.rs`). The Java tests construct anonymous-inner-class listeners that record callback invocations. Rust pattern: a `Mutex<Vec<Event>>`-recording listener impl in `tests/common/test_rebalance_listener.rs` (H6).
- The `triggerOnPartitionsAssigned` helper drives a poll-loop with a side-effect closure. Same pattern in Rust via the recording listener.
- **3 brokers** required (H1).

### 6. `PlaintextConsumerCommitTest.java` (21 → ~11 CONSUMER methods)

- **T+H mixed**:
  - Most CONSUMER methods are `commit_sync`/`commit_async`/`committed` API tests. All Rust APIs exist. **T.**
  - One test calls `cluster.brokerIds().forEach(cluster::shutdownBroker)` (line 471) — requires H1+H2.
  - One test calls `consumer.wakeup()` mid-commit. `wakeup()` exists in Rust.
  - Audit during prep: `commit_async` callbacks, `OffsetCommitCallback` invocation order assertions (§31 in `consumer-threading.md`).
- **3 brokers** (H1).

### 7. `PlaintextConsumerFetchTest.java` (18 → 9 CONSUMER methods)

- **T**: 9 CONSUMER methods exercise the fetch path (`poll`, `position`, `seek`, `beginning_offsets`, `end_offsets`, `offsets_for_times`). All Rust APIs exist (audit confirmed).
- **3 brokers** (H1).

### 8. `PlaintextConsumerPollTest.java` (24 → 12 CONSUMER methods)

- **T**: 12 CONSUMER methods exercise `poll` semantics (`max.poll.records`, `max.poll.interval.ms`, multiple-consumer-group, partition-revocation). Rust APIs exist; multi-consumer-in-same-group tests will be **flakier** than the rest — see Risks.
- **3 brokers** (H1).

### 9. `PlaintextConsumerSubscriptionTest.java` (22 → ~11 CONSUMER methods)

- **T+H mixed**:
  - Topic-name-only `subscribe(...)` and `unsubscribe()` paths — **T**.
  - `SubscriptionPattern` regex paths (line 290+): **T** if `subscribe_pattern` accepts a regex and broker-side regex evaluation works. Rust has `subscribe_pattern(Regex)` and `subscribe_pattern_with_listener`. Java tests also exercise `subscribe(Pattern.compile("..."))` (client-side regex evaluation, classic protocol style). The KIP-848 path is the `SubscriptionPattern` (server-side); only the latter is in scope.
  - Test at line 471 uses an invalid regex `"(t.*c"` to assert validation error. **T.**
  - One CONSUMER-named test exercises `subscribe(Pattern, ConsumerRebalanceListener)` (line 261) which is the client-side regex Java overload — **S** with rationale: classic-protocol-only API. Rust's `subscribe_pattern_with_listener` takes `SubscriptionPattern`, not Java `Pattern`.
- **3 brokers** (H1).

### 10. `PlaintextConsumerTest.java` (74 → 37 CONSUMER methods)

This is the biggest suite; it deserves its own sub-bucket pass:

- **T**: ~24 of the 37 CONSUMER methods test core consumer behavior (`simpleConsumption`, `headers`, `autoOffsetReset`, `groupConsumption`, `partitionsFor`, `seek`, `pauseAndResume`, `listTopics`, `endOffsets`, `fetchOffsetsForTime`, `positionRespectsTimeout`, `positionRespectsWakeup`, `closeLeavesGroupOnInterrupt`, `offsetRelatedWhenTimeoutZero`, `stallBetweenPoll`, `headerSerializerDeserializer`, `consumeMessagesWithCreateTime`, `consumeMessagesWithLogAppendTime`, `consumerSeek`, `staticConsumerDetectsNewPartitionCreatedAfterRestart`, `seekThrowsIllegalStateIfPartitionsNotAssigned`).
- **S** (metrics-suite, Milestone-8-wide deferral): all 8 `*MetricsCleanup*` / `*LeadMetrics*` / `*LagMetrics*` / `*QuotaMetrics*` tests. That is 8 method names containing `Metric` in the CONSUMER half. Rationale per Java line numbers `[836, 846, 903, 913, 971, 981, 1026, 1036, 1083, 1094, 1135, 1146]`.
- **S** (interceptor-suite): `testAsyncConsumerInterceptors` and `testAsyncConsumerInterceptorsWithWrongKeyValue` — these rely on `MockConsumerInterceptor` which is a Java reflection-instantiated test helper. The Rust `ConsumerInterceptor` trait exists but is constructed at config-build-time (Phase-2 decision); these tests need either a Rust-side `MockConsumerInterceptor` equivalent (small) or skip. Recommend **T+H**: build a small `tests/common/mock_consumer_interceptor.rs` (H7) that tracks `on_consume` / `on_commit` counters via `AtomicUsize`. Translatable.
- **T+H**: `testAsyncConsumerCloseOnBrokerShutdown` (line 211) calls `cluster.shutdownBroker(0)` — needs H1+H2.
- **T+H**: `testAsyncConsumerCoordinatorFailover` (line 177) — needs H3 (find-coordinator) + H2 (shutdown).
- **T+H**: `testAsyncConsumerClusterResourceListener` (line 158) — needs `ClusterResourceListener` trait wired through. Per Phase-12 PLAN.md line 86–93, this is a Phase-11 deferral and the wire-up exists structurally; tests may translate but need a Rust-side recording listener (H8). Audit during prep.
- **3 brokers** by `@ClusterTestDefaults`; one test (`testAsyncConsumerCloseOnBrokerShutdown`) overrides with `brokers = 1`.

### 11. `SaslPlainPlaintextConsumerTest.java` (6 → 3 CONSUMER methods)

- **T+H**, 3 CONSUMER methods (`testAsyncConsumerSimpleConsumption`, `testAsyncConsumerClusterResourceListener`, `testAsyncConsumeCoordinatorFailover`).
- **Harness gap H9 — production ctor SASL/SSL channel-builder wire-up.** Audit of `src/consumer/async_kafka_consumer.rs:708` confirms the production ctor hardcodes `PlaintextChannelBuilder`. The existing `tests/integration/ssl_sasl_test.rs` operates at the **selector layer** (`SaslChannelBuilder` directly), NOT through `new_consumer(...)`. To run the consumer integration test against SASL_PLAINTEXT, the production ctor must build `SaslChannelBuilder` when `security.protocol=SASL_PLAINTEXT` (and similarly for SSL/SASL_SSL). This is genuine **production work**, not test-harness work — it deserves its own phase or a sub-commit within Phase 13.
- **Recommend** folding the SASL ctor work into Phase 13 as a discrete sub-phase (13c — see Phasing), since the alternative is shipping `SaslPlainPlaintextConsumerTest` 0-of-3 translated.

### 12. `ConsumerWithLegacyMessageFormatIntegrationTest.java` (4 → 2 CONSUMER methods)

- **S, all 4** (both CONSUMER and CLASSIC). Rationale (DoD §3): the suite exercises consumer fetch behavior against **v0 / v1 message format records** written directly to the broker log via the internal `UnifiedLog` server class. Kafka 4.2.0 broker still serves v0/v1 messages, but the test infrastructure requires (a) JVM access to the broker's `LogManager` (impossible from a Rust test process) and (b) crafting `MemoryRecords` with `magicValue=0` or `1`. The Rust translation is a v2-only protocol target (per Milestone-8 scope). **Skip both arms**; carry rationale in Phase-13 close-out and `Milestone-8/PLAN.md`.

### Summary of buckets

| Bucket | Suite count | Approx CONSUMER methods | Rationale tag |
|---|---|---|---|
| **T** (translate today) | ~6 suites partially | ~50 methods | harness already supports |
| **T+H** (needs new harness/ctor work) | 6 suites | ~50 methods | mostly H1/H2/H3 (multi-broker + bounce + coordinator find) |
| **S** (skip with rationale) | 1 suite entirely + ~12 methods scattered | ~14 methods | legacy format, metrics, classic-protocol-only assignors, share-consumer (none here) |

## Phasing

**6,625 LoC of Java is large. Recommend a 3-sub-phase split (13a / 13b / 13c).**

Arguments for splitting:
- ConsumerBounceTest + half of PlaintextConsumerTest + PlaintextConsumerCommitTest all need broker-bounce helpers (H1+H2+H3). That is a phase-sized chunk of harness work on its own.
- SaslPlainPlaintextConsumerTest needs **production ctor work** (H9 — wire `SaslChannelBuilder` into `AsyncKafkaConsumer::new`). That is structural Phase-12-shaped work and should land as a discrete commit reviewable in isolation.
- Critic review on a 7,000-LOC test diff would be painful. The actor/critic loop benefits from sub-phase boundaries.
- 13a's tests being green is a precondition for landing 13b (broker-bounce tests assume the basic consumer loop works); 13c is independent of both.

Arguments against splitting (i.e. one big phase):
- One PLAN.md is easier to navigate than three.
- The harness changes are small enough (H1: trivial `ClusterConfig::with_brokers(3)` already exists; H2: ~50 lines of docker-control shell-out; H3: ~30 lines of metadata RPC; H9: ~80 lines of ctor branching) that each could be a single sub-commit in a unified phase.

**Pick: split into 13a / 13b / 13c.** The Critic-loop cost of a unified 7000-LOC phase outweighs the navigation cost of three PLAN files. Each sub-phase has a natural exit criterion (green tests). Below is the recommended decomposition:

- **Phase 13a — single-broker translatable suites (no harness work beyond N=3 brokers).** Translates: PlaintextConsumerAssignTest, PlaintextConsumerFetchTest, PlaintextConsumerSubscriptionTest, PlaintextConsumerCallbackTest, the non-bounce half of PlaintextConsumerCommitTest, the non-bounce/non-interceptor half of PlaintextConsumerTest, ConsumerTopicCreationTest, the non-broker-restart half of ConsumerIntegrationTest. **Harness additions: H1 (verify `ClusterConfig::with_brokers(3)` works end-to-end with current pool), H6 (recording rebalance listener), H7 (recording interceptor), H8 (recording cluster-resource listener).** Estimated: ~60 methods translated. Estimated LOC: ~3,000 test code.
- **Phase 13b — broker-bounce / multi-broker harness + ConsumerBounceTest.** Translates: ConsumerBounceTest, the `testLeaderEpoch` / `*OnBrokerShutdown` / `*CoordinatorFailover` tests scattered across other suites, the bounce half of PlaintextConsumerCommitTest. **Harness additions: H2 (`KafkaCluster::shutdown_broker(node_id: u16)` / `start_broker(node_id)` via Bollard or `docker` CLI shell-out against `kafka_cluster.container_ids[i]`), H3 (`KafkaCluster::find_coordinator(group_id) -> u16` via FindCoordinator RPC or by polling all brokers).** Estimated: ~12 methods translated. Estimated LOC: ~800 test code + ~150 harness code.
- **Phase 13c — SASL through production ctor + SaslPlainPlaintextConsumerTest.** Translates: SaslPlainPlaintextConsumerTest (3 CONSUMER methods). **Production additions: H9 (`AsyncKafkaConsumer::new` branches on `config.security_protocol()` to construct `SaslChannelBuilder` / `SslChannelBuilder` / `SaslChannelBuilder` with SSL — mirror the producer side if it exists).** Estimated: ~3 methods translated. Estimated LOC: ~300 test code + ~150 prod code. Note: this sub-phase is the only one with **production code changes**; 13a and 13b are tests-only.

If Manager decides on one big phase, the commit ordering would mirror this anyway: 13a-equivalent commits land first (1–12), 13b-equivalent (13–17), 13c-equivalent (18–20). The PLAN-file split is just bookkeeping.

## Harness work needed

Concrete list, with item IDs referenced above:

- **H1 — Multi-broker cluster validation.** `ClusterConfig::with_brokers(3)` already exists (`tests/common/cluster_config.rs:43`). Verify in Phase 13 prep that `KafkaCluster::start_with_config` actually starts 3 brokers and forms a quorum. The existing producer integration tests have not exercised multi-broker; this is a "verify, possibly fix" item, not new code. New helpers: none.
- **H2 — Broker shutdown / start.** New methods on `KafkaCluster`:
  - `pub async fn shutdown_broker(&self, node_id: u16) -> Result<()>` — calls `docker stop {container_ids[node_id - 1]}` (or testcontainers' container stop API if exposed).
  - `pub async fn start_broker(&self, node_id: u16) -> Result<()>` — `docker start {container_ids[node_id - 1]}`.
  - Estimated 50 LOC, located in `tests/common/kafka_cluster.rs`.
- **H3 — Coordinator location.** New helper on `KafkaCluster` or a free `tests/common/coordinator_lookup.rs`:
  - `pub async fn find_coordinator(bootstrap: &str, group_id: &str) -> Result<u16>` — issues a `FindCoordinator` RPC and returns the node ID. Can reuse `crate::common::network::NetworkClient` machinery already used in `connection_test.rs` / `metadata_test.rs`. Estimated 80 LOC.
- **H4 — Leader broker lookup.** For `testLeaderEpoch`: similar RPC-based helper (`Metadata` RPC → `Topic.partition.leader`). Reuse existing metadata-test selector pattern. Estimated 50 LOC.
- **H5 — Topic existence assertion.** Recommend: use `consumer.list_topics()` from a separately constructed consumer in the test. Zero harness code. If we later add an Admin client (out of scope), wire there.
- **H6 — Recording `ConsumerRebalanceListener`.** `tests/common/test_rebalance_listener.rs` — an `Arc<Mutex<Vec<RebalanceEvent>>>`-backed listener that records `OnPartitionsAssigned` / `OnPartitionsRevoked` / `OnPartitionsLost` invocations with the partitions involved. The trait is `Send + Sync + 'static` already (`consumer_rebalance_listener.rs:59`). Estimated 80 LOC including an "always-fails" variant for `ConsumerIntegrationTest`'s `testFetchPartitionsWithAlwaysFailedListener`.
- **H7 — Recording `ConsumerInterceptor`.** `tests/common/mock_consumer_interceptor.rs` mirroring `org.apache.kafka.test.MockConsumerInterceptor` — atomic counters for `on_consume` and `on_commit`, optional record-value mutation. Estimated 60 LOC.
- **H8 — Recording `ClusterResourceListener`.** Status of `ClusterResourceListener` wire-up in Rust needs reading during Phase 13 prep (per Phase-12 PLAN.md it was deferred; verify Phase 12.5 close-out did not pick it up). If trait is wired through, ~40 LOC of test helper; if not, the cluster-resource-listener tests are SKIP-able with "ClusterResourceListener wire-up not yet shipped" rationale.
- **H9 — Production ctor SASL/SSL channel-builder.** `AsyncKafkaConsumer::new` at `async_kafka_consumer.rs:708` currently does `Box::new(PlaintextChannelBuilder::new(None))`. Change to dispatch on `config.security_protocol()`:
  - `PLAINTEXT` → `PlaintextChannelBuilder`.
  - `SSL` → `SslChannelBuilder` (use `SslConfig::from_consumer_config(&config)` — verify Rust crate exposes a parallel construction path; `tests/integration/ssl_sasl_test.rs:60-69` shows the constructor shape).
  - `SASL_PLAINTEXT` → `SaslChannelBuilder` (`tests/integration/ssl_sasl_test.rs:72-82`).
  - `SASL_SSL` → `SaslChannelBuilder` with `Some(ssl_factory)`.
  - Estimated 80 LOC in `async_kafka_consumer.rs`, plus a unit test that builds each variant against a refused-connection peer.

## Rust outputs

### Files added under `tests/integration/`

```
tests/integration/consumer_bounce_test.rs                       # ConsumerBounceTest (13b)
tests/integration/consumer_integration_test.rs                  # ConsumerIntegrationTest (13a/13b mixed)
tests/integration/consumer_topic_creation_test.rs               # ConsumerTopicCreationTest (13a)
tests/integration/plaintext_consumer_assign_test.rs             # PlaintextConsumerAssignTest (13a)
tests/integration/plaintext_consumer_callback_test.rs           # PlaintextConsumerCallbackTest (13a)
tests/integration/plaintext_consumer_commit_test.rs             # PlaintextConsumerCommitTest (13a + 13b)
tests/integration/plaintext_consumer_fetch_test.rs              # PlaintextConsumerFetchTest (13a)
tests/integration/plaintext_consumer_poll_test.rs               # PlaintextConsumerPollTest (13a)
tests/integration/plaintext_consumer_subscription_test.rs       # PlaintextConsumerSubscriptionTest (13a)
tests/integration/plaintext_consumer_test.rs                    # PlaintextConsumerTest (13a + 13b split)
tests/integration/sasl_plain_plaintext_consumer_test.rs         # SaslPlainPlaintextConsumerTest (13c)
# ConsumerWithLegacyMessageFormatIntegrationTest: NO Rust file (SKIPped).
```

### Files added under `tests/common/`

```
tests/common/test_rebalance_listener.rs                         # H6
tests/common/mock_consumer_interceptor.rs                       # H7
tests/common/test_cluster_resource_listener.rs                  # H8 (if scope allows)
tests/common/coordinator_lookup.rs                              # H3 (+ H4 leader lookup, or a sibling test_metadata_helpers.rs)
```

### Files updated

```
tests/integration/main.rs                                       # add 11 new mod declarations
tests/common/mod.rs                                             # add new common helpers
tests/common/kafka_cluster.rs                                   # +shutdown_broker, +start_broker (H2)
src/consumer/async_kafka_consumer.rs                            # H9 — security-protocol dispatch in production ctor (13c only)
design/history/Milestone-8/PLAN.md                              # add rows 13a/13b/13c
design/history/Milestone-8/Phase-13/PLAN.md                     # this file (split into 13a/13b/13c subfiles if Manager chooses)
```

## Behavior parity

Each translated Rust test follows the existing `tests/integration/consumer_test.rs` shape verbatim: `#[tokio::test(flavor = "multi_thread")]`, `TestContext::new(cluster_config_with_kip848()).await`, `produce_deterministic_records(...)`, `new_consumer::<K, V>(make_consumer_config(...))`, `consumer.subscribe/assign`, poll-loop with 30s deadline, assertions, `consumer.close().await`.

**Behavior parity call-outs by suite:**

- **`enforce_rebalance`** (used in some PlaintextConsumerTest CLASSIC paths): no-op + log under KIP-848 per `async_kafka_consumer.rs:3277`. CONSUMER-arm tests should not call it; if they do, the test must accept the no-op outcome. No production work needed.
- **`consumer.metrics()`**: not in `Consumer` trait (metrics deferred). Tests that assert against named `MetricName` keys are SKIP per the metrics-suite-deferred rule. No production work needed; would unblock with the Metrics-translation milestone.
- **`ConsumerRebalanceListener` throwing**: Rust's `ConsumerRebalanceListener::on_partitions_assigned` returns `Result<(), KafkaError>` (read `consumer_rebalance_listener.rs:59-105` in prep). Java's anonymous-inner-class throws unchecked. Translation: `Result::Err(KafkaError::other(...))` from the test listener, or `panic!` if the test infra catches panics. Audit during Phase 13a prep.
- **Static membership (`group.instance.id`)**: `testAsyncConsumerStaticConsumerDetectsNewPartitionCreatedAfterRestart` uses static membership. Confirm `ConsumerConfig` accepts `group.instance.id` (audit during prep). KIP-848 supports static membership; should be translatable.
- **`testFetchPartitionsWithAlwaysFailedListener`**: comment in the Java source (lines 180–186) explicitly addresses async consumer behavior. Translate the async arm; note that the Java test sleeps in a loop and asserts that poll either returns 0 records or throws `User rebalance callback throws an error`. Rust's `KafkaError` message strings need verification to match this expected substring.
- **`testAsyncConsumerCloseLeavesGroupOnInterrupt`** (line 1562): Java uses `Thread.interrupt()` while the consumer is closing. Rust does not have a direct equivalent. The Rust test would either `tokio::time::timeout(close_future)` or call `consumer.wakeup()` mid-close. Audit during 13b prep — this test may need a SKIP with rationale "Java Thread.interrupt has no Rust equivalent; equivalent semantics under tokio are covered by other close tests".

## Risks

1. **Multi-broker docker startup cost.** A 3-broker cluster takes 30–60s to start. Most of these suites need `BROKER_COUNT = 3`. The pooling in `tests/common/cluster_pool.rs` shares clusters across tests with identical `ClusterConfig` — so if every test uses `cluster_config_with_kip848()` (single broker today) AND a separate `cluster_config_with_kip848_3brokers()`, we get exactly two pool entries amortized across the whole suite. **Mitigation:** define one canonical `kip848_3_broker_config()` helper in `tests/common/cluster_config.rs`-adjacent code, use it from every test that needs 3 brokers (the majority).

2. **Real-broker KIP-848 rebalance timing flakiness.** Same as Phase 12 — when a second consumer joins a group, KIP-848 reassignment can take 5–15s to complete on the broker side. The existing `test_commit_sync_then_resume_in_same_group` already sleeps 5s after consumer1 LeaveGroup and uses a 60s deadline. Apply the same pattern to all multi-consumer rebalance tests. **Mitigation:** standardize on a `wait_for_assignment(consumer, expected_partitions, deadline)` helper in `tests/common/test_context.rs` that polls `consumer.assignment()` until the expected set is seen.

3. **Docker control plane (H2) is OS-dependent.** Shelling out to `docker` works on macOS / Linux but is fragile. Bollard (Rust docker client) is more robust but adds a dep. Decide in 13b prep.

4. **`testLeaderEpoch` (`ConsumerIntegrationTest`) requires `ConsumerRecord::leader_epoch()`**. Audit Phase 7 implementation during prep — if the field is plumbed through the fetch decoder, the test translates cleanly. If not, the test is **deferred-with-rationale** (Phase 7 gap), NOT translated.

5. **Test count and the Manager review burden.** Even after SKIPping ~14 methods, ~100 new Rust integration tests is a lot for a single Critic pass. The 13a/13b/13c split helps but each sub-phase still ships ~30+ tests. **Mitigation:** Actor groups tests into per-Java-suite commits (one Rust file per commit), Critic reviews per-file. This is the same granularity Phase 12 used.

6. **Java-internals dependencies (UnifiedLog, MockConsumerInterceptor, MockConsumerRebalanceListener, RackAwareAssignor).** Each Java-internals dep either (a) needs a Rust-side equivalent helper (interceptor, listener — small) or (b) marks the test SKIPPABLE (UnifiedLog, RackAwareAssignor — large/impossible). Bucketed above.

7. **CI runtime explosion.** 100 tests × 30s avg + 60s broker startup × 2 pool entries = ~50 minutes test wall time. **Mitigation:** the cluster pool already shares clusters across tests; ensure tests that can share a cluster use the same `ClusterConfig` instance. Investigate per-test parallelism with `tokio::test` flavor.

8. **`#[ignore]` markers must carry rationale.** DoD requires no `#[ignore]` without justification. For each SKIP bucket, the Rust test file gets a top-of-file doc block enumerating SKIPped Java method names and rationale, mirroring `tests/integration/consumer_test.rs:14-35`.

## Definition of Done

Per `definition-of-done.md` (1–11) plus phase-specific items:

- `cargo build` clean.
- `cargo test` (unit + non-integration tests) clean.
- `cargo xtask format-check` clean.
- `cargo xtask lint` clean (clippy-warnings-as-errors).
- `cargo xtask check-generated` clean.
- `cargo test --features integration-tests --test integration` — **all Phase-13 integration tests green** against the testcontainers Kafka 4.2.0 broker (1- and 3-broker variants). The Actor runs this locally before declaring done; if docker is unavailable, the Actor states so explicitly in the close-out, and the Manager re-runs in a docker-enabled environment.
- **No `#[ignore]` markers** without an inline rationale comment naming the SKIP category (classic-protocol-only, share-consumer-only, legacy-message-format, metrics-suite, ClusterResourceListener-not-wired, java-internals-only).
- Each new `tests/integration/*_test.rs` file carries a top-of-file doc block listing:
  - Java source file + pinned commit.
  - Each translated `@ClusterTest` method as a `// Translated: testAsyncConsumerX -> rust_test_name` line.
  - Each SKIPped method as a `// SKIP: testClassicConsumerX — <category>` line.
- `COMMENTS.1.md` empty (all Critic rounds resolved → `COMMENTS.DONE.1.md`) for each sub-phase. New `COMMENTS.*` files live under `design/history/Milestone-8/Phase-13/` (or `Phase-13a/`, `Phase-13b/`, `Phase-13c/`).
- **Trait surface (DoD §11):** no new `#[async_trait]` on per-record paths. The recording listener / interceptor traits are user-side and follow the existing `ConsumerRebalanceListener` (async via `#[async_trait]`) / `ConsumerInterceptor` (sync) conventions.
- **Hot-path allocation audit (DoD §10):** test code is allowed to allocate per record (`StringDeserializer` clones, recording-vec pushes). Production code touched (H9 ctor branch) sits at construction time, not the per-record path — no allocation regression risk.
- **§16 invariant:** test code does not hold `Mutex` guards across `.await`. Recording listeners do their writes inside `Mutex::lock()` scopes that drop before any await.
- For Phase 13c only: `cargo test --features integration-tests --test integration sasl_plain_plaintext_consumer_test` green; production ctor's SASL/SSL branches each exercised by at least one passing test.
- `Milestone-8/PLAN.md` row 13 (or 13a/13b/13c) added + status flipped to CLOSED on phase close.
- Agent-memory entry under `.claude/agent-memory/` for Phase-13 patterns (rebalance-listener recorder, multi-broker harness, SASL ctor branch).

## Out of scope for Phase 13

Explicit non-coverage list (carry forward to a future milestone or never):

- **`ConsumerWithLegacyMessageFormatIntegrationTest`** entirely (legacy v0/v1 message format requires internal `UnifiedLog` access).
- **All `Share*` files** (`ShareConsumerTest`, `ShareConsumerRackAwareTest`) — KIP-932, out of scope Milestone-8-wide.
- **`RackAwareAssignor`** test — classic-protocol assignor (`consumer-threading.md` §20).
- **`MockConsumerRebalanceListener` Java helper** — replaced by Rust-side `test_rebalance_listener.rs`; we do not translate the Java helper class.
- **Streams-related tests** — none of the 12 suites are Streams; not relevant.
- **`AsyncConsumerMetrics` / `KafkaConsumerMetrics` translation** — metrics-suite tests inside `PlaintextConsumerTest` are SKIPped per the Milestone-8 metrics deferral.
- **C FFI for `AsyncKafkaConsumer`** — out of milestone.
- **Classic-protocol path** — every CLASSIC-named twin method is SKIPped (`consumer-threading.md` §20).
- **`ConsumerAssignmentPoller.java`** (131 LoC) — a Java test helper class, not a test file. If translating any test needs it, port the small bits inline; no separate Rust file.

## Workflow

Per `.claude/rules/agent-roles.md`, three sub-phase rounds:

1. **13a (single-broker translatable):** Actor N=1 translates per-Java-suite, one commit per Rust file. Critic N=1 reviews after each commit batch (3–4 commits), writes `Phase-13a/COMMENTS.1.md`. Actor moves resolved to `Phase-13a/COMMENTS.DONE.1.md` and ships fixup commits. Repeat until empty. Status row 13a → CLOSED.
2. **13b (broker-bounce harness + ConsumerBounceTest):** Actor N=1 ships H2/H3/H4 harness first as a single commit (reviewable in isolation), then ConsumerBounceTest + the broker-shutdown methods across PlaintextConsumerTest/PlaintextConsumerCommitTest/ConsumerIntegrationTest. Critic N=1. Same close-out.
3. **13c (SASL ctor + SaslPlainPlaintextConsumerTest):** Actor N=1 ships H9 (production ctor branch) + per-protocol unit smoke tests in one commit, then SaslPlainPlaintextConsumerTest in a second commit. Critic N=1 reviews the production change carefully (Trait surface, §16, §27 still hold). Status row 13c → CLOSED → row 13 (composite) CLOSED → Milestone-8 status section updated.

Each sub-phase ships independently and can be parallelized between Actors if Manager assigns N=1 to 13a and N=2 to 13b/13c (13b and 13c are mutually independent).

## Status

**Phase 13: OPEN — plan-only.** Sub-phases 13a / 13b / 13c not yet started.
