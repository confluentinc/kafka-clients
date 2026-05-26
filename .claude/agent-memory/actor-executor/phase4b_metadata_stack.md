---
name: Phase 4b metadata stack
description: Phase 4b decisions — sync model, retain-topic predicate, await_update wall-clock deadline, ProducerMetadata composition
type: project
---

# Phase 4b — what's load-bearing for Phase 5

## Single-mutex sync model on `Metadata`

Java's `Metadata` is `synchronized` on every public method. Translated as
a single `Mutex<MetadataInner>` guarding all fields. Every public method
takes `&self`, locks, computes, drops the lock, optionally
`notify_waiters` on a separate `Arc<Notify>`. **Never** holds the
`MutexGuard` across `.await` (CLAUDE.md rule 9.6) — the `Notify` is
cloned out of the state for waker plumbing in `ProducerMetadata`.

`metadata_snapshot` is stored as `Arc<MetadataSnapshot>` so
`fetch_metadata_snapshot()` is a refcount bump (no lock release while
the snapshot is held). The producer hot path can read partition data
without contending on the mutex.

## Retain-topic predicate via composition

Java's `Metadata.retainTopic(String, boolean, long)` is overridable by
subclasses (`ProducerMetadata`). Rust translation uses composition: a
`RetainTopicFn = Arc<dyn Fn(&str, Option<Uuid>, bool, i64) -> bool +
Send + Sync>` field on `MetadataInner`. `ProducerMetadata::new()`
installs the predicate via `metadata.set_retain_topic_fn(...)` after
constructing the inner `Metadata`. The predicate captures
`Arc<Mutex<ProducerState>>` so it can mutate `topics` (e.g. evict
expired entries) on each call.

The 4-arg signature accommodates both Java overloads: the `Option<Uuid>`
parameter is `None` for the 3-arg variant and `Some(_)` for the 4-arg
variant. Java's default `Metadata.retainTopic` (no override) returns
`true` always — Rust does the same when no predicate is installed.

## `MetadataResponse` inner-class translation

`MetadataResponse.TopicMetadata` and `MetadataResponse.PartitionMetadata`
landed in `src/common/requests/metadata_response.rs` as sibling structs
to `MetadataResponse`. Java's volatile `Holder` cache (broker map +
projected topic metadata) translates to a `OnceLock<Holder>` field
built lazily on first call to `topic_metadata()` / `brokers_by_id()` /
`controller()`.

`MetadataResponse::to_partition_info(...)` is a static method (not a
free function, even though Java exposes it as a static helper) so the
Rust call site reads `MetadataResponse::to_partition_info(...)` and
mirrors the Java `MetadataResponse.toPartitionInfo(...)` namespacing.

## `await_update` wall-clock deadline (Java MockTime divergence)

`ProducerMetadata::await_update(version, timeout_ms)` is async. The
deadline is computed against `tokio::time::Instant::now()` (real wall
clock), **not** the `Time` source the caller injected. This diverges
from Java's `Time.waitObject(...)` which uses the same `Time` source
for both deadline and waker.

Why: Java's `MockTime` overrides `waitObject` to advance the mock clock
during the wait, so test code only has to call `time.sleep(N)` to make
the deadline fire. Rust's `tokio::time::sleep_until` always uses the
runtime's monotonic clock — there's no "advance the mock clock during
sleep" hook. If we anchored the deadline to `MockTime::milliseconds()`,
tests would either hang forever (mock clock never advances) or require
test-only plumbing to inject sleeps.

The behavioural contract for callers is unchanged: a timeout of N ms
returns `Err(Timeout)` after at most N ms of real time. Tests that
need to exercise the predicate path (`update_version`, `is_closed`,
fatal error) trigger the predicate via `notify_waiters` from another
task, not by advancing the mock clock.

The waiter is `tokio::sync::Notify` registered **before** the predicate
check. Without this, a `notify_waiters` call that races between the
predicate check and the future registration is silently lost (`Notify`
only wakes already-registered futures).

## Visibility: `pub(crate)` for `Metadata::new` and `ProducerMetadata`

`Metadata::new` accepts an `Arc<ClusterResourceListeners>` which lives
in the `common.internals` package (CLAUDE.md `pub(crate)` rule). To
avoid the "public type leaks private interface" lint, `Metadata::new`
itself is `pub(crate)`. All `pub(crate)` consumers (Phase 5 producer)
live in the same crate, so this preserves Java parity for callers.

`ProducerMetadata` is entirely `pub(crate)` (Java's
`producer.internals` package).

## `KafkaError::StaleMetadata` variant

Added a new `StaleMetadata(String)` variant to `KafkaError`, classified
as retriable (mirrors Java's `StaleMetadataException extends
InvalidMetadataException` — the `InvalidMetadata` family is retriable).
Code: client-side, librdkafka-style `ERR_CODE_UNKNOWN`. Java class name:
`StaleMetadataException`.

`StaleMetadataError::empty()` and `::with_message(...)` are constructor
shims producing `KafkaError::StaleMetadata`. Used by admin/consumer
code in future phases — producer doesn't currently need to fail with
this variant.

## Inlined `CommonClientConfigs` constants

`Metadata` needs `RETRY_BACKOFF_EXP_BASE = 2` and
`RETRY_BACKOFF_JITTER = 0.2` from `CommonClientConfigs`. Phase 4c will
translate the full module; we inline these two constants in
`metadata.rs` for now to avoid the cross-phase dependency. When Phase
4c lands, these should be replaced with imports from
`crate::common_client_configs` (see CLAUDE.md naming rule for the
`org.apache.kafka.clients` package).

## Test helper for `MetadataResponse` construction

`build_metadata_response` / `metadata_update_with` in `metadata.rs`
test module mirror `RequestTestUtils.metadataResponse` /
`metadataUpdateWith` from the Java test tree. They construct a
`MetadataResponseData` directly and wrap it. Phase 5 may want to
promote these helpers to a shared `request_test_utils` module if
producer / sender tests need similar plumbing.

## `MetadataTest.java` coverage scope

Translated 24 test methods (subset of the 30+ in the Java tree). Skipped:
- `testNodeIfOffline`, `testNodeIfOnlineWhenNotInReplicaSet`,
  `testNodeIfOnlineNonExistentTopicPartition` — exercise
  `Cluster.nodeIfOnline` plumbing covered by `ClusterTest` already.
- `testConcurrentUpdateAndFetchForSnapshotAndCluster` — Java-specific
  `ExecutorService` + `CountDownLatch` concurrency stress test;
  the Rust `Mutex<MetadataInner>` + `Arc<MetadataSnapshot>` design
  has no Java analogue to stress-test in this manner.
- `testEpochUpdateOnChangedTopicIds`, `testMetadataMergeOnIdDowngrade`,
  `testTopicMetadataOnUpdatePartitionLeadership` — depend on
  `MetadataResponse` factory helpers and topic-id transitions whose
  Java semantics are exercised at the `MetadataSnapshot.merge_with`
  level by `MetadataSnapshotTest` (translated in full).

The 24 translated tests cover the contract surface that Phase 5 (the
producer wire-up) depends on.
