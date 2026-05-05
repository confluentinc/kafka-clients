# PR #47: KAFKA-19903 — Integration tests for share group throttled delivery

## AK Commit

- **Hash**: `b4aa1087a960cc0e88d1164fd95d0a5c8bada2c9`
- **Branch**: `trunk`
- **Title**: KAFKA-19903: Integration tests for share group throttled delivery (#20953)
- **Author**: Apoorv Mittal
- **Date**: 2025-11-24

## Summary

This AK commit adds integration tests that validate the throttled-delivery
behaviour of the share-group consumer. When a record's delivery count reaches
`Math.ceil(maxDeliveryCount / 2)` (the throttle threshold), the broker
progressively reduces the number of records returned per fetch by halving the
batch size on each additional delivery attempt (implemented via right-shift in
`SharePartition.acquirableRecords`). On the final delivery attempt the broker
caps the batch at 1 record. Records that exceed the maximum delivery count are
archived and never redelivered.

### Java files changed

| File | Change type | Description |
|---|---|---|
| `clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/ShareConsumerTest.java` | Test additions | 5 new integration test methods + 2 new helper methods |
| `core/src/main/java/kafka/server/share/SharePartition.java` | Comment fix | Expands inline comment explaining throttle-batch-size calculation; also fixes a Javadoc typo (`THe` → `The`) |

### New integration test methods

1. `testFetchWithThrottledDelivery` — default delivery limit (5); verifies full
   batches for attempts 1–2, throttled batches for 3–4, and a single-record
   final attempt.
2. `testFetchWithThrottledDeliveryBatchesWithIncreasedDeliveryLimit` — delivery
   limit 10, 512-record batches; exhaustively validates exact batch sizes for
   every delivery attempt across all offset ranges.
3. `testFetchWithThrottledDeliveryValidateDeliveryCount` — delivery limit 10,
   500 records; uses `waitForCondition` to confirm every offset is delivered
   exactly 10 times in total and then no more records are returned.
4. `testFetchWithThrottledDeliveryBatchesWithDecreasedDeliveryLimit` — delivery
   limit 2; verifies that throttling is not applied (threshold is > 2) and both
   fetch cycles return the full 512-record batch.
5. `testFetchWithThrottledDeliveryBatchesMultipleConsumers` — two consumers with
   different `MAX_POLL_RECORDS` values share the same group; validates that
   throttling interacts correctly with per-consumer fetch limits and
   acknowledgement from a different consumer.

### New helper methods (in `ShareConsumerTest`)

- `validateExpectedRecordsInEachPollAndRelease` — polls repeatedly, asserts
  batch size and first-offset, acknowledges with `RELEASE`, commits synchronously.
- `validateExpectedRecordsInEachPollAndAcknowledge` — same as above but accepts
  a configurable `AcknowledgeType`.

## Analysis: what needs to be translated

### `SharePartition.java` change

The change is a pure comment / Javadoc improvement with no behavioural
difference. The corresponding Rust code (when `SharePartition` is
translated) should carry over the updated comment text. No code
translation action is required in this PR by itself.

### `ShareConsumerTest.java` additions

These are **integration tests** for the share-group consumer's throttled-
delivery path. The following Rust components must exist before these tests can
be translated:

1. **`ShareConsumer`** (Java: `org.apache.kafka.clients.consumer.ShareConsumer`)
   — the top-level consumer type including `subscribe`, `poll`, `acknowledge`,
   and `commitSync`.
2. **`SharePartition`** server-side throttling logic — must already implement
   `acquirableRecords` with the delivery-count halving (bit-shift) algorithm.
3. **`AcknowledgeType`** enum (`ACCEPT`, `RELEASE`, `REJECT`).
4. **`TopicIdPartition`** struct.
5. Integration test infrastructure: a running Kafka broker reachable from tests,
   equivalent to the Java `@ClusterTest` / `@ClusterConfigProperty` harness.

## Current state of the Rust codebase

The existing Rust client implements the **producer** side (network layer, batch
accumulation, SSL/SASL, `MockProducer`, C FFI). The **consumer** and
**share-group** subsystems are not yet present.

## Dependencies

| Dependency | Description | Status |
|---|---|---|
| `ShareConsumer` translation | Full translation of the share-group consumer including subscribe/poll/acknowledge/commitSync | Not started |
| `SharePartition` translation | Server-side translation (needed for integration tests against a Rust or embedded broker, or test stubs) | Not started |
| Integration test harness | Equivalent of the Java `@ClusterTest` cluster-test framework | Not started |

Because none of these prerequisites exist, the tests in this commit cannot be
translated in isolation. This PR's translation work is **blocked** on the
above dependencies.

## Translation plan

### Step 1 — Translate `SharePartition` throttle comment (non-blocking, trivial)

When `SharePartition` is translated (in a future PR), carry the updated comment
from this commit verbatim (adapted to Rustdoc). This is a zero-effort follow-on
to that PR.

### Step 2 — Translate `ShareConsumer` and supporting types

Translate the following (may span multiple PRs as the Java consumer translation
progresses):

- `consumer/share_consumer.rs` — `ShareConsumer<K, V>` struct with:
  - `subscribe(topics: &[&str])`
  - `poll(timeout: Duration) -> ConsumerRecords<K, V>`
  - `acknowledge(record: &ConsumerRecord<K, V>, ack_type: AcknowledgeType)`
  - `commit_sync() -> HashMap<TopicIdPartition, Result<(), KafkaError>>`
- `consumer/acknowledge_type.rs` — `AcknowledgeType` enum
- `common/topic_id_partition.rs` — `TopicIdPartition` struct (if not already present)

### Step 3 — Translate throttled-delivery integration tests

Once `ShareConsumer` is available, translate all five test methods into Rust
integration tests under `tests/share_consumer_throttle.rs` (or the appropriate
integration test module):

| Java test | Rust test function |
|---|---|
| `testFetchWithThrottledDelivery` | `test_fetch_with_throttled_delivery` |
| `testFetchWithThrottledDeliveryBatchesWithIncreasedDeliveryLimit` | `test_fetch_with_throttled_delivery_batches_increased_limit` |
| `testFetchWithThrottledDeliveryValidateDeliveryCount` | `test_fetch_with_throttled_delivery_validate_delivery_count` |
| `testFetchWithThrottledDeliveryBatchesWithDecreasedDeliveryLimit` | `test_fetch_with_throttled_delivery_batches_decreased_limit` |
| `testFetchWithThrottledDeliveryBatchesMultipleConsumers` | `test_fetch_with_throttled_delivery_batches_multiple_consumers` |

Also translate helpers:

- `validate_expected_records_in_each_poll_and_release`
- `validate_expected_records_in_each_poll_and_acknowledge`

### Translation notes

- The `@ClusterTest` annotation maps to an integration test that spins up a
  real Kafka broker (either via Docker or an in-process embedded broker). Use
  the same pattern established by the existing integration tests in `tests/`.
- `@ClusterConfigProperty(key = "group.share.delivery.count.limit", value = "10")`
  translates to setting the broker property before starting the test cluster,
  e.g. via the test harness configuration map.
- `waitedPoll(consumer, timeoutMs, expectedCount)` — implement a Rust helper
  `waited_poll` that repeatedly calls `consumer.poll()` until `expectedCount`
  records are accumulated or the timeout is exceeded.
- `ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG` = `"share.acknowledgement.mode"`;
  `ConsumerConfig.MAX_POLL_RECORDS_CONFIG` = `"max.poll.records"`.
- `waitForCondition(predicate, timeoutMs, intervalMs, message)` — implement a
  Rust async helper that polls the predicate in a loop with `tokio::time::sleep`
  intervals up to the total timeout.

## Files to create / modify

| Path | Action |
|---|---|
| `src/consumer/share_consumer.rs` | Create (Step 2) |
| `src/consumer/acknowledge_type.rs` | Create (Step 2) |
| `src/common/topic_id_partition.rs` | Create if absent (Step 2) |
| `tests/share_consumer_throttle.rs` | Create (Step 3) |

## Acceptance criteria

- All five integration tests pass against a real Kafka broker with default and
  custom `group.share.delivery.count.limit` values.
- `cargo test`, `cargo xtask lint`, and `cargo xtask format-check` all pass.
- No TODOs or FIXMEs remain in the translated code.
- Every method present in the Java test class is present in the Rust test
  (including the two helper methods).
