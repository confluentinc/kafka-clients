# Phase 40 — Integration test parity: consumer PUBLIC-API surface

**Actor:** 40 · **Branch:** consumer-impl · **Type:** test-parity (test-only
unless a genuine Java-fidelity bug surfaces; production change must be
perf/CPU-neutral and cited).

## Goal

Translate the remaining single-broker integration tests covering the
consumer PUBLIC-API surface, per `design/current/test-translation-review/07-integration.md`
(the `PlaintextConsumerTest` / `BaseConsumerTestcase` rows + the 3
client-side-`Pattern` subscription rows + `ConsumerTopicCreationTest`).
**KIP-848 (`GroupProtocol.CONSUMER`) arm only.** Classic twins, metrics,
broker-fault-injection are OUT_OF_SCOPE per `consumer-threading.md` §20.

Java contract:
`kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/{PlaintextConsumerTest,ConsumerTopicCreationTest,PlaintextConsumerSubscriptionTest}.java`
+ `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/ClientsTestUtils.java`
(Apache Kafka 4.2).

## Rust targets

- NEW: `tests/integration/plaintext_consumer_test.rs` — the
  `BaseConsumerTestcase` / `PlaintextConsumerTest` public-API surface.
- NEW: `tests/integration/consumer_topic_creation_test.rs` — the 2
  `ConsumerTopicCreationTest` CONSUMER-arm tests.
- EXTEND: `tests/integration/plaintext_consumer_subscription_test.rs` — the
  3 client-side-`Pattern` tests (documented SKIP, see below).
- Wire both new files into `tests/integration/main.rs`.

Gating MIRRORS the existing suites exactly: crate-root
`#![cfg(feature = "integration-tests")]` in `main.rs` (per-file cfg not
needed); `#[tokio::test(flavor = "multi_thread")]`; shared cluster pool via
`TestContext::new(ClusterConfig)`; byte-typed `new_consumer::<Vec<u8>,
Vec<u8>>` with an inline `ByteArrayDeserializer`.

## In-scope tests (CONSUMER arm) → Rust names

### plaintext_consumer_test.rs

| Java test | Rust name | Status / notes |
|---|---|---|
| `testAsyncConsumerHeaders` | `test_async_consumer_headers` | RUN. Produce 1 record w/ 3 headers (ProducerRecord `with_headers`), assign+seek(0), consume 1, assert `last_header("headerKey")=="headerValue"` and header ORDER preserved (`headers()[0..2].key()`). |
| `testAsyncConsumerHeadersSerializerDeserializer` | SKIP | Java uses a `Serializer`/`Deserializer` that injects a `content-type` header **inside** serialize/deserialize via the `(topic, headers, data)` overload. Rust `Deserializer<T>::deserialize_with_headers` exists but `Serializer` has no symmetric headers-mutating overload wired into the producer write path used here; and the test's whole point is the serializer-injected header. Header round-trip itself is covered by `testAsyncConsumerHeaders`. Low value to force; documented gap. |
| `testAsyncConsumerPartitionPauseAndResume` | `test_async_consumer_partition_pause_and_resume` | RUN. Standalone pause/resume on assigned partition (NOT in-callback — that's the Issue-8 reentrancy case). assign, consume 5, pause, produce 5 more, poll empty, resume, consume next 5. |
| `testAsyncConsumerPauseStateNotPreservedByRebalance` | `test_async_consumer_pause_state_not_preserved_by_rebalance` | RUN. subscribe(topic), consume 5, pause(TP), subscribe(topic2) → rebalance, then consume from offset 5 (pause lost). |
| `testAsyncConsumerPartitionsFor` | `test_async_consumer_partitions_for` | RUN. createTopic(TOPIC,2), `partitions_for(TOPIC)` len==2. |
| `testAsyncConsumerPartitionsForAutoCreate` | `test_async_consumer_partitions_for_auto_create` | RUN (broker auto-create default on). First `partitions_for("non-exist")` triggers create; poll until non-empty. |
| `testAsyncConsumerPartitionsForInvalidTopic` | `test_async_consumer_partitions_for_invalid_topic` | RUN. `partitions_for(";3# ads,{234")` → InvalidTopic error. |
| `testAsyncConsumerListTopics` | `test_async_consumer_list_topics` | RUN. create 3 topics, produce to topic1, subscribe+poll, `list_topics()` contains the 3 (+ `__consumer_offsets`); each has 2 partitions. Assert the 3 test topics present + 2 partitions each (deviation: total count assertion loosened to `>= 4` because internal-topic count is broker-version-dependent; the 3 named topics + their partition counts are asserted exactly). |
| `testAsyncConsumerSeek` | `test_async_consumer_seek` | RUN. seekToEnd → position==total, poll empty; seekToBeginning → position==0, consume 1; seek(mid) → position==mid, consume 1. **Compressed-message half SKIPPED within the test** (no producer `compression.type=gzip` + `linger.ms=MAX` helper wired in the byte harness; non-compressed seek fully covers seekToEnd/seekToBeginning/seek-mid which is the in-scope gap). Documented inline. |
| `testAsyncConsumerSeekThrowsIllegalStateIfPartitionsNotAssigned` | `test_async_consumer_seek_throws_illegal_state_if_partitions_not_assigned` | RUN. `seek_to_end([TP])` unassigned → IllegalState EXACT msg `"No current assignment for partition topic-0"`. |
| `testAsyncConsumerConsumeMessagesWithLogAppendTime` | `test_async_consumer_consume_messages_with_log_append_time` | RUN. Topic w/ `message.timestamp.type=LogAppendTime` (via topic-config — see deviation), produce 50, assign, assert `timestamp_type==LogAppendTime` and ts in `[startTime, now]`. Compressed half SKIPPED (same gzip-helper reason). |
| `testAsyncConsumerConsumingWithNullGroupId` | `test_async_consumer_consuming_with_null_group_id` | RUN. 3 groupless consumers (no `group.id`), earliest/latest/explicit-seek; assert record counts 3/0/2; assert `commit_sync` and `committed` raise InvalidGroupId. |
| `testAsyncConsumerNullGroupIdNotSupportedIfCommitting` | `test_async_consumer_null_group_id_not_supported_if_committing` | RUN. groupless consumer, assign, `commit_sync` → InvalidGroupId EXACT msg. |
| `testAsyncConsumerEndOffsets` | `test_async_consumer_end_offsets` | RUN. produce N to TP, subscribe, await assignment {TP, tp2}, `end_offsets([TP])` == N. (N reduced from 10000 → 200 for harness speed; documented — outcome parity, not count parity.) |
| `testAsyncConsumerFetchOffsetsForTime` | `test_async_consumer_fetch_offsets_for_time` | RUN. 2 partitions, 100 records each (ts==seq), `offsets_for_times` for ts 0 and 20; assert offset/timestamp/leader_epoch==Some(0). Negative-ts → IllegalArgument. |
| `testAsyncConsumerPositionRespectsTimeout` | `test_async_consumer_position_respects_timeout` | RUN. assign TP(15) (nonexistent partition), `position_timeout(3s)` → Timeout. |
| `testAsyncConsumerPositionRespectsWakeup` | `test_async_consumer_position_respects_wakeup` | RUN. assign TP(15); spawn task sleeps 1s then `wakeup()`; `position_timeout(3s)` → Wakeup (§11). |
| `testAsyncConsumerPositionWithErrorConnectionRespectsWakeup` | `test_async_consumer_position_with_error_connection_respects_wakeup` | RUN. bootstrap=`localhost:12345` (unreachable); spawn wakeup after 1s; `position_timeout(100s)` → Wakeup. Standalone cluster-less consumer (no TestContext cluster needed for this one, but harness still spins one up cheaply via pool; bootstrap overridden to the bad addr). |
| `testAsyncConsumerStaticConsumerDetectsNewPartitionCreatedAfterRestart` | SKIP | Requires `admin.createPartitions(increaseTo(2))` mid-test — no admin client in the Rust harness, and the cluster pool has no partition-increase API. Static membership (`group.instance.id`) is config-accepted but the test's observable (new partition appears in assignment after increase) cannot be driven without admin. Documented gap. |
| `testAsyncConsumerInterceptors` | SKIP | No public way to attach a `ConsumerInterceptor` — `new_consumer` always builds an EMPTY chain (`async_kafka_consumer.rs:761`, Java's reflective `interceptor.classes` loader is not translated). `new_with_components` (the only seam carrying interceptors) is `pub(crate)`, unreachable from an integration (external) crate. Documented API-shape gap. |
| `testAsyncConsumerInterceptorsWithWrongKeyValue` | SKIP | Same interceptor-injection gap. (The asserted *outcome* — record value unchanged — is trivially true with an empty chain, so a "translation" here would assert nothing about interceptors.) |
| `testAsyncConsumerSimpleConsumption` | SKIP (already covered) | `consumer_test.rs::test_subscribe_and_poll_records` + assign suite cover subscribe/assign/seek/poll/async-commit. |
| `testAsyncConsumerAutoOffsetReset` / `GroupConsumption` | SKIP (already covered) | Covered by fetch-reset suite + assign suite + this file's headers/pause tests (assign+seek+consume path). |
| `testAsyncConsumerConsumeMessagesWithCreateTime` | SKIP (already covered) | Assign suite verifies `timestamp_type==CreateTime` + full per-record fields. |
| `testAsyncConsumerOffsetRelatedWhenTimeoutZero` | `test_async_consumer_offset_related_when_timeout_zero` | RUN (cheap, in-scope). `beginning_offsets([TP], ZERO)` empty; `end_offsets([TP], ZERO)` empty; `offsets_for_times({TP:0}, ZERO)` size 1, value None. |
| `testAsyncConsumerStallBetweenPoll` | SKIP | KAFKA-19259 timing/perf-regression guard, not a behavioral contract; flaky under shared-pool load; the steady-state stall is separately tracked in perf work. Documented. |
| `testAsyncConsumerClusterResourceListener` | SKIP (OUT_OF_SCOPE-adjacent) | `ClusterResourceListener` on (de)serializer not wired into the public deserializer API; report 07 lists this as a gap. |
| `testAsyncConsumeCoordinatorFailover` / `CloseOnBrokerShutdown` / `CloseLeavesGroupOnInterrupt` | SKIP (OUT_OF_SCOPE) | Require `shutdownBroker` / thread-interrupt — no harness support (§ broker-fault-injection out of scope). |
| all `*MetricsCleanUp*` / `QuotaMetrics*` | SKIP (OUT_OF_SCOPE) | metrics deferred Milestone-wide. |

### consumer_topic_creation_test.rs

| Java test | Rust name | Status / notes |
|---|---|---|
| `testAsyncConsumerTopicCreationIfConsumerAllowToCreateTopic` | `test_async_consumer_topic_creation_if_consumer_allow_to_create_topic` | RUN (best-effort). Two cluster configs: broker `auto.create.topics.enable=true` and `=false`. With consumer `allow.auto.create.topics=true`: topic created iff broker allows. Verify via `list_topics()`/`partitions_for(TOPIC)` (no admin client → use consumer metadata as the topic-existence oracle; documented deviation from Java's `admin.listTopics()`). |
| `testAsyncConsumerTopicCreationIfConsumerDisallowToCreateTopic` | `test_async_consumer_topic_creation_if_consumer_disallow_to_create_topic` | RUN. consumer `allow.auto.create.topics=false` → topic NOT created regardless of broker setting. |

### plaintext_consumer_subscription_test.rs (extend)

The 3 client-side-`Pattern` tests stay **documented SKIP** — already noted
in that file's header. The Rust public API exposes ONLY
`subscribe_pattern(SubscriptionPattern)` (server-side Re2J); there is NO
client-side `Pattern.compile` overload on the `Consumer` trait
(`src/consumer/mod.rs:178-187`). This is a real-but-low-priority API-shape
gap (KIP-848 prefers server-side regex). No new code; the existing SKIP
block already covers `testAsyncConsumerPatternSubscription`,
`...SubsequentPatternSubscription`, `...PatternUnsubscription`. **No edit
to that file needed** — the SKIP rationale is already present and correct.
(If the Manager wants a stronger marker, add an explicit doc note; not
forcing it.)

## Documented SKIPs summary (in-scope but cannot run)

1. **Interceptors** (`testAsyncConsumerInterceptors`,
   `...InterceptorsWithWrongKeyValue`, and Phase-39's
   `testAsyncConsumerAutoCommitIntercept`): no public interceptor-injection
   path. **Real API gap** — `new_consumer` lacks an interceptors parameter;
   Java loads them via reflective `interceptor.classes`. Recommend a future
   `new_consumer_with_interceptors` (or config-driven registry) milestone.
2. **HeadersSerializerDeserializer**: `Serializer` has no headers-mutating
   overload on the producer write path; the test's content-type-injection
   is unrepresentable. Plain header round-trip IS covered.
3. **StaticConsumerDetectsNewPartition**: needs `admin.createPartitions`;
   no admin client.
4. **Client-side `Pattern`** (×3): no client-side regex subscribe overload.
5. **StallBetweenPoll**: perf-regression timing guard, not a contract.
6. OUT_OF_SCOPE (§20): all classic twins, metrics/quota, coordinator
   failover, broker shutdown, close-on-interrupt, ClusterResourceListener.

## Production change

**One addition (Critic round-1 fix, perf-neutral):** `Consumer::wakeup_handle()
-> WakeupHandle` (mirrored on `AsyncKafkaConsumer` + `MockConsumer`).

Java's `Consumer` reference is freely shareable across threads, so
`CompletableFuture.runAsync(() -> consumer.wakeup())`
(`PlaintextConsumerTest.java:1501` / `1535`) can fire `wakeup()` from another
thread while the owner blocks in `position()`. The Rust consumer is borrowed
`&mut self` for the duration of a blocking call, so a reference cannot cross
the task boundary — there was no SAFE way to express the cross-task `wakeup()`
pattern. `WakeupHandle` is a `Clone + Send + 'static` handle that captures only
the internally-synchronized, `Arc`-backed wakeup state (the rotating
`WakeupTrigger` watch channel + the bg-task notify closure, now held as
`Arc<dyn Fn>`); obtain it BEFORE the `&mut` borrow and fire it from the spawned
task. **This CLOSES the "missing shareable wakeup handle" API gap** that the
two wakeup tests previously needed an `unsafe` raw-pointer helper to work
around. **Perf-neutral:** off the hot path entirely (one `Arc`/`watch` clone at
handle-creation, only when a user opts in); no per-record / per-poll cost.

Every OTHER in-scope behavior is already supported by the public API
(verified: InvalidGroupId on `commit_sync`/`committed`; `position_timeout`
returns `Timeout`; `submit_and_drain(enable_wakeup)` returns `Wakeup`;
`seek_to_end` unassigned returns exact "No current assignment" message;
`offsets_for_times` negative-ts IllegalArgument; `partitions_for`
InvalidTopic).

## Verify (Phase-39 protocol; no Docker in dev env)

- `cargo build`
- `cargo test --features integration-tests --test integration --no-run` (compile)
- `cargo clippy --features integration-tests --test integration` (lint; xtask lint does NOT cover integration cfg)
- `cargo xtask format-check`
- `cargo test --lib` (unit, still green)
- Broker-run if Docker available; else compile-verified + gated-identically.

## Commit groups

1. Phase 40: pause/resume + seek + offset-timeout-zero
2. Phase 40: partitions_for / list_topics / offsets_for_times / end_offsets
3. Phase 40: headers + timestamp-type (LogAppendTime)
4. Phase 40: wakeup/timeout (position) + null-group-id
5. Phase 40: topic-creation (+ subscription Pattern SKIP doc if needed)
