---
name: phase13a-fetch-test-notes
description: Phase 13a (2/N) PlaintextConsumerFetchTest patterns — auto-create topic partition count, OOR-error swallowing production gap, busy-cluster timing budget
metadata:
  type: project
---

# Phase 13a (2/N) — PlaintextConsumerFetchTest patterns

**Commit:** `f7cd343` — 7 tests pass, 2 `#[ignore]`-gated on Issue 4
(`design/history/Milestone-8/Phase-13/COMMENTS.1.md`).

## Production gap surfaced: poll_for_fetches error swallow

`src/consumer/async_kafka_consumer.rs:2344-2355` —
`poll_for_fetches` `match`es `collect_fetch` result, logs `Err` at warn
and returns `ConsumerRecords::empty()`. Java's `pollForFetches`
(`AsyncKafkaConsumer.java:1933-1948`) does NOT catch — every
`KafkaException` propagates out of `poll(Duration)`.

This silently swallows the surfaceable error classes Java raises:
`OffsetOutOfRangeException`, `TopicAuthorizationFailed`, `CorruptMessage`,
`IllegalState` fallback. The `FetchCollector::collect_fetch` itself is
correct — only the wrapper is wrong.

**Why this matters more than the Phase-13a-pilot Issue-1 gap:** Pilot's
Issue 1 was an initial-offset-fetch retry omission. This Issue 4 is a
mainline `poll()` contract violation — any user catching
`OffsetOutOfRangeException` won't get notified.

**Fix shape** (one-line, deferred to Manager):
```rust
fn poll_for_fetches(&self) -> Result<ConsumerRecords<K, V>, KafkaError> {
    self.fetch_collector.collect_fetch(&self.fetch_buffer)
}
```
…and propagate the `Result` at the caller (`poll`, around line 2183).

## Test-translation pattern: auto-create topic with N partitions

`TestContext` has no admin/create-topic API; topics are auto-created on
first produce/fetch with the broker's `num.partitions` default. To match
Java's `@BeforeEach cluster.createTopic(topic, 2, BROKER_COUNT)`, set
`KAFKA_NUM_PARTITIONS` on the cluster config:

```rust
props.insert("KAFKA_NUM_PARTITIONS".to_string(), "2".to_string());
```

This is cluster-pool-keyed: tests with different `num.partitions` get
different cluster instances. Bundle tests that share a partition-count
requirement so the pool can amortize startup (30-60s per cluster).

## Test-translation pattern: extended deadline for slow Rust producer flush

Java's `ClientsTestUtils.sendRecords` does `producer.flush()` per call
(same as Rust). For tests that send to N partitions sequentially
(N=90 in `low_max_fetch_size_for_request_and_partition`), the Rust
producer's per-partition flush loop adds significant wall-clock — the
default 60s `consume_records_bytes` deadline isn't enough.

Pattern: parameterize the helper —

```rust
async fn consume_records_bytes_with_deadline(
    consumer: &mut BytesConsumer,
    num_records: usize,
    deadline_duration: Duration,
) -> Vec<ConsumerRecord<Vec<u8>, Vec<u8>>>
```

…and use a 180s deadline for the slow case. Document why in a comment —
Java's JVM producer is faster; the test outcome (all records consumed)
is identical given enough budget.

## Test-translation pattern: settle window for OffsetReset round-trips

Tests asserting `consumer.position(&tp).await == expected_after_reset`
need a settle window covering the ListOffsets round-trip. 5s is too
tight on a busy shared-cluster pool. Use 30s. Java tests use
`waitForCondition` (default 60s); 30s is conservative middle ground.

## Test infra reminders confirmed during Phase 13a (2/N)

- **`std::sync::Mutex` correctness in `Drop`**: `TestContext::drop`
  emits a "WARN: dropped with N uncleaned topics" message — this is
  expected (no admin client yet) and not a test failure.
- **`COMMENTS.<N>.md` is `.gitignore`d** (`.gitignore:5` —
  `COMMENTS\.[0-9]*\.md`). Working-tree only; do not `git add -f`.
- **Critic agent files are out of scope** for Actor commits:
  `.claude/agent-memory/kafka-critic/{MEMORY.md,review_m8_*.md}`,
  `kafka` submodule untracked-content marker.
- **Phase 13 commits are NOT in a worktree** — direct commits on
  `consumer-impl` branch.

## See also

- [[phase13a-assign-test-notes]] — Phase 13a (1/N) pilot, 8 tests
- [[phase13a-issue1-fix-notes]] — Phase 13a (1/N) production-gap fix
- `design/history/Milestone-8/Phase-13/COMMENTS.1.md` Issue 4 entry
