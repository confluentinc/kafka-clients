---
name: review-m11-phase1-admin
description: Milestone 11 (AdminClient) Phase 1 review patterns — skeleton POJOs + network engine (Call/runnable/KafkaFuture) findings
metadata:
  type: project
---

Milestone 11 = AdminClient translation (`org.apache.kafka.clients.admin` -> `admin`).
Two review passes recorded here: (A) the review-only API skeleton, (B) the
network engine + 4 topic RPCs (`dev/admin-client-implementation`).

Admin methods are sync-returning-futures, NOT async — every RPC returns a
`*Result` holding one `KafkaFuture<T>` per key immediately; only `close()`
(and later `client_instance_id()`) block in Java, so only those are `async fn`.
`#[async_trait]` belongs on the `Admin` trait ONLY, never on POJO/Result/Options
or the internal `Call`/runnable types.

## A. Skeleton-pass recurring findings (POJOs)
- **Derived `PartialEq`/`Eq` vs Java hand-written equals.** Java Admin POJOs
  frequently EXCLUDE a field. `TopicDescription.java:41-53` excludes `topicId`;
  the network pass FIXED this with a manual `PartialEq` excluding `topic_id`
  (now correct — not a defect). NewTopic includes all fields (derive OK).
- **§12 borrowing**: flag by-value non-Copy read-only inputs (was: TopicCollection,
  filters). `create_topics(&[NewTopic])` borrows; `delete/describe_topics(TopicCollection)`
  take by value — TopicCollection is consumed/matched, acceptable.
- **Catalog completeness**: diff Java subclass lists (caught OffsetSpec missing a variant).

## B. Network-engine findings (what to check on every Admin RPC translation)
Engine itself was faithful: `Call.fail` ordering matches `KafkaAdminClient.java:903-948`
verbatim (closing → UV-downgrade-no-tries-bump → backoff+tries++ → timeout →
!retriable → out-of-retries → requeue); `run_once` mirrors `processRequests`
phase order; `close()` truly awaits the bg `JoinHandle` (§9.4 OK); no MutexGuard
across await; `KafkaFuture`/`all_of`/`then_apply`/`join_map` correct.

Real defects found (all in `src/admin/kafka_admin_client.rs`):
1. **describe-by-id "requires DescribeTopicPartitions, deferred" rationale is FALSE.**
   Java `handleDescribeTopicsByIds` uses the **Metadata API** (`convertTopicIdsToMetadataRequestTopic`),
   NOT DescribeTopicPartitions. Rust helper `MetadataRequest::convert_topic_ids_to_metadata_request_topic`
   already exists → by-id is implementable now. Actor failed it with `unsupported_version`
   + wrong justification. ALWAYS verify a "deferred, needs X API" claim by reading the
   Java handler — it may use an already-wired API.
2. **Quota tests skipped wholesale.** Only "…UntilRequestTimeOut" was flagged; but
   `testCreateTopicsRetryThrottlingExceptionWhenEnabled` and `…DontRetryWhenDisabled`
   (x create & delete) are directly translatable with current code and were skipped.
   DoD #3: a skip is only valid per-test-with-rationale, not per-family.
3. **`maybeCompleteQuotaExceededException` simplified**: Java carries per-key
   `ThrottlingQuotaExceededException` (with throttleTimeMs) forward and re-completes
   on timeout; Rust drops the carry-forward → timeout surfaces generic `Timeout`.
   Also KafkaError has no throttle_time_ms field (lost even on don't-retry path).
4. **NOT_CONTROLLER retry path untested** (`testCreateTopicsHandleNotControllerException`
   not translated). Non-trivial path (clear controller + requestUpdate + retry).
5. **CreateTopics RESPONSE config parsing** uses `ConfigEntry::new(name,value)` —
   drops read_only/config_source/is_sensitive that the wire `CreatableTopicConfigs`
   carries and Java's `configEntry()` maps. (Wire REQUEST side is fine: only name+value.)
6. **Missing client-side `topicNameIsUnrepresentable`/`topicIdIsUnrepresentable`**
   guards (empty name / ZERO_UUID) in create/delete/describe — Java fails fast
   client-side with InvalidTopicException; Rust forwards to broker.

Correct-and-not-defects (avoid FP): id-XOR-name closed enum (TopicCollection is
sealed in Java too); `join_map` new combinator (necessary: get-driven model can't
call .get() sync inside then_apply); TopicDescription manual PartialEq.
