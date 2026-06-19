---
name: phase40-integration-public-api-notes
description: Phase 40 test-parity — PlaintextConsumerTest public-API surface + ConsumerTopicCreationTest; interceptor-injection gap, offsets_for_times non-nullable map deviation, cross-task wakeup-during-position via thin-pointer
metadata:
  type: project
---

Phase 40 (Actor 40) added two integration files under `tests/integration/`:
`plaintext_consumer_test.rs` (18 tests: headers, pause/resume, partitions_for,
list_topics, seek full, LogAppendTime, end_offsets, offsets_for_times,
null-group-id, position timeout+wakeup, offset-related-zero-timeout) and
`consumer_topic_creation_test.rs` (2 tests). Wired into
`tests/integration/main.rs`. No Docker in dev env → compile-verified +
gated-identically. ZERO production change (every behavior already supported).

**API-shape gaps surfaced (documented SKIPs, candidates for future
milestones):**
- **Interceptors cannot be injected via the public API.** `new_consumer`
  always builds an EMPTY `ConsumerInterceptors` chain
  (`async_kafka_consumer.rs:761`); Java's reflective `interceptor.classes`
  loader is NOT translated. The `new_with_components` seam carrying
  interceptors is `pub(crate)`, unreachable from an integration crate. So
  `testAsyncConsumerInterceptors` / `...InterceptorsWithWrongKeyValue` /
  Phase-39's `...AutoCommitIntercept` are ALL unrunnable as faithful
  integration tests. Recommend a future `new_consumer_with_interceptors`
  or config-driven registry. (The `ConsumerInterceptor` trait itself exists
  in `src/consumer/interceptor.rs`.)
- **No client-side `Pattern.compile` subscribe overload** — only
  `subscribe_pattern(SubscriptionPattern)` (server-side Re2J). The 3
  client-side-Pattern subscription tests stay SKIP (already documented in
  `plaintext_consumer_subscription_test.rs` header — no edit needed).
- **No admin client / partition-increase API** on the test `KafkaCluster`,
  so `StaticConsumerDetectsNewPartition` (needs `admin.createPartitions`)
  is unrunnable. Topic-existence in the topic-creation test uses the
  consumer's own `list_topics()` as the oracle instead of `admin.listTopics()`.
- **HeadersSerializerDeserializer**: `Serializer` has no headers-mutating
  overload on the producer write path; plain header round-trip covered by
  `test_async_consumer_headers` instead.

**`offsets_for_times` returns `HashMap<TP, OffsetAndTimestamp>` (NOT
`Option`-valued).** A missing key = "no offset"; there is no null-value
representation. Java's zero-timeout arm returns size-1 map with null value;
Rust omits the key (`async_kafka_consumer.rs:3413-3428`). So
`testOffsetRelatedWhenTimeoutZero` asserts `!result3.contains_key(&tp)`,
NOT Java's `size()==1 && get(tp)==null`. Documented contract reduction.
`.get(&tp)` returns `Option<&OffsetAndTimestamp>` directly — no `.as_ref()`.

**Cross-task wakeup-during-blocking-op pattern (§11 integration test).**
`Consumer::wakeup(&self)` needs `&self`; `position_timeout` needs
`&mut self`. The trait is `Send` but NOT `Sync`, and exposes no clonable
wakeup handle, so you can't share `&consumer` across tasks safely. Solution
in `fire_wakeup_during`: cast `consumer as *const Box<BytesConsumer>` (a
THIN pointer — `*const dyn Trait` is a FAT pointer that CANNOT round-trip
through `usize`) to usize, pass to a spawned task that sleeps then derefs
+ calls `wakeup()`. SOUND because `wakeup()` only touches the shared
internally-synchronized wakeup channel/notify, disjoint from `&mut self`
poll state. `await` the waker before returning so it never outlives the
borrow. Needs `#[allow(clippy::borrowed_box)]` on the `&Box<_>` deref.
LogAppendTime: no admin client → set broker-wide
`KAFKA_LOG_MESSAGE_TIMESTAMP_TYPE=LogAppendTime` in a dedicated pool-keyed
ClusterConfig (server_properties become container env vars).

Verified supported (no prod change): InvalidGroupId on commit_sync/committed
(`throw_if_group_id_not_defined`), exact msg "To use the group management
or offset commit APIs, you must provide a valid group.id...";
position_timeout→Timeout; submit_and_drain(enable_wakeup=true)→Wakeup;
seek_to_end unassigned→IllegalState "No current assignment for partition {tp}";
offsets_for_times negative-ts→IllegalArgument; partitions_for→InvalidTopic.
