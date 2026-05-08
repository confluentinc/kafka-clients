# Translation Plan: KAFKA-19903 — Integration tests for share group throttled delivery

**AK commit:** `b4aa1087a960cc0e88d1164fd95d0a5c8bada2c9`
**AK branch:** trunk
**PR:** #93
**Rust branch:** `kafka-translate/b4aa1087a960cc0e88d1164fd95d0a5c8bada2c9`

---

## Summary of the Apache Kafka Commit

This commit adds **integration tests** for the share group throttled delivery
feature and includes a minor **comment/documentation improvement** in
production code.

### Production code change (`SharePartition.java`)

1. **Expanded inline comments** in the `acquirableRecordsCount` method to better
   explain the throttling algorithm. The comment now clarifies that when a
   complete batch errors and increases its delivery count, front offsets may have
   higher delivery counts than later offsets. The bit-shift calculation isolates
   records at higher delivery counts first, delivering them individually before
   proceeding with the rest of the batch.

2. **Typo fix** in the `shouldThrottleRecordsDelivery` Javadoc: "THe" -> "The".

No logic changes were made to `SharePartition.java`.

### Test additions (`ShareConsumerTest.java`)

Five new integration test methods were added:

| Test method | Description |
|-------------|-------------|
| `testFetchWithThrottledDelivery` | Verifies throttled delivery with default limit (5), ensuring fetch count halves as delivery count approaches the limit. |
| `testFetchWithThrottledDeliveryBatchesWithIncreasedDeliveryLimit` | Tests throttling with limit=10 and 512 messages (power of 2), verifying the binary-division pattern across all delivery attempts. |
| `testFetchWithThrottledDeliveryValidateDeliveryCount` | Tracks per-offset delivery counts with limit=10 and 500 messages, asserting every offset is delivered exactly 10 times. |
| `testFetchWithThrottledDeliveryBatchesWithDecreasedDeliveryLimit` | Tests with limit=2, verifying throttling does not activate when the limit is too low (requires limit > 2). |
| `testFetchWithThrottledDeliveryBatchesMultipleConsumers` | Tests throttled delivery with two consumers having different `max.poll.records`, verifying correct record isolation and delivery across consumers. |

Two new helper methods were also added:
- `validateExpectedRecordsInEachPollAndRelease`
- `validateExpectedRecordsInEachPollAndAcknowledge`

---

## Rust Translation Analysis

### Does `SharePartition` exist in Rust?

No. The Rust codebase does not currently contain any `SharePartition`
implementation or share group coordinator logic. The `kafka/server/share/`
package has not been translated.

### Do the integration tests exist in Rust?

No. The Rust test suite (`tests/`) does not include share consumer integration
tests. Share consumer functionality (the KIP-932 share group protocol) has not
been implemented in the Rust client.

### Is there production code to translate?

The only production code change is a **comment expansion and typo fix** in
`SharePartition.java`. Since `SharePartition` does not exist in Rust, there is
nothing to translate on the production side.

### What needs to be done?

This commit is **not directly applicable** to the Rust codebase at this time.
Both the production code (`SharePartition`) and the test infrastructure (share
consumer integration tests) are absent from the Rust translation.

---

## Implementation Plan

### Phase 1 — No-op (Current State)

This commit requires no changes to the Rust codebase because:

1. `SharePartition` (the server-side share group partition manager) has not been
   translated to Rust. The comment improvements apply to code that does not
   exist yet.
2. `ShareConsumerTest` integration tests exercise share consumer functionality
   that is not implemented in the Rust client.
3. The helper methods (`validateExpectedRecordsInEachPollAndRelease`,
   `validateExpectedRecordsInEachPollAndAcknowledge`) are test utilities for
   tests that cannot yet run.

### Phase 2 — Future Work (When Share Groups Are Translated)

When `SharePartition` is eventually translated to Rust, the following should be
included:

1. **Port the updated comments** from the `acquirableRecordsCount` method to
   the Rust equivalent, explaining the throttled delivery bit-shift algorithm.

2. **Port the integration tests** to the Rust test suite, including:
   - The five test methods covering default, increased, decreased, and
     multi-consumer throttling scenarios.
   - The `validate_expected_records_in_each_poll_and_release` and
     `validate_expected_records_in_each_poll_and_acknowledge` helper functions.

---

## Files to Create / Modify

| File | Action | Reason |
|------|--------|--------|
| (none) | — | No Rust code changes needed at this time |

---

## Out of Scope

- Translating `SharePartition.java` — requires share group coordinator, KIP-932
  protocol support, and significant server-side infrastructure.
- Translating `ShareConsumerTest.java` — requires a share consumer client
  implementation and a test broker supporting share groups.
- Any changes to the Rust client library or test suite.

---

## Definition of Done

- [x] Design document written and committed.
- [x] Confirmed no Rust code changes are required for this commit.
- [x] No regressions: `cargo build` and `cargo test` remain unaffected (no
  files modified).
