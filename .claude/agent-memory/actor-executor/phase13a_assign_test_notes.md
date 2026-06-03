---
name: phase13a_assign_test_notes
description: Phase 13a pilot — PlaintextConsumerAssignTest translation notes, production-code gaps surfaced (Issues 1-4), patterns for byte-typed test helpers
metadata:
  type: project
---

Phase 13a pilot suite is `tests/integration/plaintext_consumer_assign_test.rs`
(translated from `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerAssignTest.java`).

**Outcome:** 8 CONSUMER-arm methods translated. 1 passes
(`test_async_assign_and_consume_skipping_position`); 7 are
`#[ignore]`-gated on 4 distinct production-code gaps documented in
`design/history/Milestone-8/Phase-13/COMMENTS.1.md`:

- **Issue 1:** `committed()` does not retry NotCoordinator —
  `fetch_offsets_with_retries` in `commit_request_manager.rs:1976-1992`
  is a no-op pass-through (mis-named). Mirror
  `commit_sync_with_retries` directly above it.
- **Issue 2:** `commit_sync_offsets()` immediately after `assign(...)`
  never recovers from NotCoordinator (60s timeout). FindCoordinator
  wiring on the assign-only path needs audit.
- **Issue 3:** `commit_sync()` after a SUCCESSFUL poll still times
  out at 60s for assign-only flows. Suspected coordinator-channel
  liveness on assign-only flows (no heartbeat keeps the connection
  alive).
- **Issue 4:** `poll()` surfaces NotCoordinator as a fatal error
  during the OffsetFetch-on-startup path (resolve-initial-position).
  Workaround: explicit `seek(tp, offset)` skips OffsetFetch.

The single passing test (`test_async_assign_and_consume_skipping_position`)
exercises `assign + seek + poll + position` — `seek` is what makes it
green, since it bypasses the OffsetFetch-on-startup path.

## Patterns established for Phase 13a future suites

### Cluster config for 3-broker KIP-848 tests

Local helper `cluster_config_with_kip848_3brokers()` returns a
`ClusterConfig` with `brokers = 3` and the four broker properties
from the Java `@ClusterConfigProperty` annotations
(`OFFSETS_TOPIC_REPLICATION_FACTOR=3`, `OFFSETS_TOPIC_NUM_PARTITIONS=1`,
`GROUP_MIN_SESSION_TIMEOUT_MS=100`, `GROUP_INITIAL_REBALANCE_DELAY_MS=10`).

Keep this helper LOCAL to each test file for now. If/when a second
suite needs the exact same 3-broker config, promote to
`tests/common/cluster_config.rs`. The cluster pool in
`tests/common/cluster_pool.rs` materializes one cluster per unique
`ClusterConfig` and amortizes 30-60s startup across all tests sharing
that config — so duplicating the helper across files does NOT
duplicate cluster startup.

### Byte-keyed consumer tests

Java's `PlaintextConsumerAssignTest` uses `Consumer<byte[], byte[]>`.
Rust equivalent: `new_consumer::<Vec<u8>, Vec<u8>>(...)`. The crate
exports `ByteArraySerializer` but no symmetric `ByteArrayDeserializer`
— each test file defines a 4-line inline `struct
ByteArrayDeserializer; impl Deserializer<Vec<u8>>` until enough suites
need it to justify promoting to `src/common/serialization/`.

### Helper aliases for `Box<dyn Consumer>` helpers

```rust
type BytesConsumer = dyn Consumer<Vec<u8>, Vec<u8>>;
async fn consume_and_verify_records_bytes(consumer: &mut BytesConsumer, ...) { ... }
```

`new_consumer::<Vec<u8>, Vec<u8>>(...)` returns
`Box<dyn Consumer<Vec<u8>, Vec<u8>>>`. Helpers take `&mut BytesConsumer`;
tests call `consume_and_verify_records_bytes(consumer.as_mut(), ...)`
to coerce `Box<dyn Consumer>` → `&mut dyn Consumer`.

### `ConsumerRecord` is NOT `Clone`

The Phase-2 memory note (claiming "Clone on ConsumerRecord/Records for
panic recovery") was for an internal panic-recovery
`catch_unwind`-style path that did not land in the public API. Public
`ConsumerRecord<K, V>` derives only `PartialEq, Eq` (gated on
`K: PartialEq, V: PartialEq`).

For helpers that need to inspect every record across many polls:
**do NOT buffer into a `Vec<ConsumerRecord<...>>`** — instead consume
each batch via `for record in records { ... }` (this uses the
`IntoIterator for ConsumerRecords<K, V>` impl that hands out owned
records) and verify inline within the helper.

### `CountConsumerCommitCallback` pattern

Java's private inner `CountConsumerCommitCallback` translates to:

```rust
struct CountConsumerCommitCallback {
    success_count: Arc<AtomicUsize>,
    fail_count: Arc<AtomicUsize>,
    last_error: Arc<Mutex<Option<KafkaError>>>,
}

impl CountConsumerCommitCallback {
    fn handles(&self) -> CountConsumerCommitCallbackHandles { ... }
}

#[async_trait]
impl OffsetCommitCallback for CountConsumerCommitCallback {
    async fn on_complete(
        &self,
        _offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        error: Option<&KafkaError>,
    ) { ... }
}
```

The callback itself is moved into an `Arc<dyn OffsetCommitCallback>`
and consumed by `consumer.commit_async_with_callback(Arc::new(cb))`.
The test keeps a separate `Handles` struct (cloned `Arc`s) to read the
counters after polling drives the bg-task callback delivery.

§16 note: the callback body locks `last_error` briefly and never
holds the guard across `.await` — the `match` arm has no further
await statements.

### `committed(...)` map semantics

Java: `Map<TopicPartition, OffsetAndMetadata> = consumer.committed(...)`
and `committedOffset.get(tp) == null` checks "no committed offset".
Rust: `committed(...)` returns `HashMap<TopicPartition, OffsetAndMetadata>`
where the key is absent if no offset has been committed for that
partition. Translate Java's `assertNull(committedOffset.get(tp))` as
`assert!(committed.get(&tp).is_none())`.

### `pollUntilTrue` translation

Java's `ClientsTestUtils.pollUntilTrue` drives `consumer.poll(...)`
in a loop with the predicate. The Rust analog must call
`consumer.poll(...).await` to trigger §31 delivery of any pending
`OffsetCommitCallback` to the caller's task. A polling loop that just
checks the predicate without calling poll() would deadlock — the bg
task enqueues the callback but only `poll()` (and other §31-aware
APIs) drains the queue.

## Open questions for Phase 13a remaining suites

- The harness gap H9 (SASL through production ctor) and H1-H3
  (broker bounce / coordinator lookup) listed in PLAN.md are not
  in Phase 13a scope, but the same 3-broker cluster pool entry will
  be used by 13b/13c too. Phase 13a tests **do** validate that
  multi-broker cluster startup via `ClusterConfig::with_brokers(3)`
  works end-to-end.
- The 4 production-code gaps documented in `COMMENTS.1.md` block 7
  of 8 tests in this file. They also likely block many tests in the
  remaining 13a suites (`PlaintextConsumerCommitTest`,
  `PlaintextConsumerFetchTest` — anything that commits/fetches
  offsets early on an `assign`-only flow). Manager should decide
  whether to fix these gaps before continuing 13a or to translate
  more suites and `#[ignore]` more tests in parallel.
