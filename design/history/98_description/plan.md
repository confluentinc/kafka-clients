# Translation Plan: MINOR — Adding check for invalid batch from persister in Share Partition

**AK commit:** `8fe1157ac5f6c2eaf79e21da3d9bfe5e979312a6`
**AK branch:** trunk
**PR:** #98
**Rust branch:** `kafka-translate/8fe1157ac5f6c2eaf79e21da3d9bfe5e979312a6`

---

## Summary of the Apache Kafka Commit

This is a **minor defensive check** added to `SharePartition.maybeInitialize()`.
When the persister returns state batches during initialization, the code now
validates that each batch's `lastOffset` is not less than its `firstOffset`. If
an invalid batch is detected, the partition transitions to a FAILED state with
an `IllegalStateException`, preventing corrupt data from propagating to clients.

**Changed files:**

| File | Change |
|------|--------|
| `core/src/main/java/kafka/server/share/SharePartition.java` | Added validation check for invalid offset range in state batches |
| `core/src/test/java/kafka/server/share/SharePartitionTest.java` | New test `testMaybeInitializeWithInvalidOffsetInBatch` |

**Java change (SharePartition.java):**
```java
if (stateBatch.lastOffset() < stateBatch.firstOffset()) {
    log.error("Invalid state batch found for the share partition: {}-{}. The first offset: {}"
            + " is less than the last offset of the batch: {}.", groupId, topicIdPartition,
        stateBatch.firstOffset(), stateBatch.lastOffset());
    throwable = new IllegalStateException(
        String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
    return;
}
```

This check is inserted inside the `maybeInitialize` method's batch processing
loop, immediately after verifying that the state batch list itself is not empty.

---

## Rust Translation Analysis

### Current Rust state

The `SharePartition` class does **not yet exist** in the Rust codebase. Only
auto-generated protocol data types (e.g., `read_share_group_state_response_data.rs`,
`write_share_group_state_request_data.rs`) exist in the build output. The core
server-side share partition logic has not been translated.

### Translation applicability

Since the entire `SharePartition` module is not yet present in Rust, this commit
**cannot be translated in isolation**. The validation check is meaningful only
within the context of the full `maybeInitialize()` flow, which processes
persisted state batches to reconstruct in-memory share partition state.

---

## Implementation Plan

### Determination: Out of Scope

This commit is **out of scope** for immediate translation because:

1. **No host module exists** — `SharePartition` and its initialization logic
   have not been translated to Rust yet. There is no method or struct to add
   this validation to.
2. **Server-side component** — The share partition is a broker-side component.
   The current Rust codebase focuses on client-side functionality (producer,
   network client, metadata).
3. **Minimal standalone value** — A validation check only makes sense within
   the broader initialization flow. Translating this single check without the
   surrounding logic would be meaningless.

### Future translation guidance

When `SharePartition` is eventually translated to Rust, this validation should
be incorporated as part of the `maybe_initialize()` method implementation:

```rust
if state_batch.last_offset < state_batch.first_offset {
    error!(
        "Invalid state batch found for the share partition: {}-{}. \
         The first offset: {} is less than the last offset of the batch: {}.",
        group_id, topic_id_partition, state_batch.first_offset, state_batch.last_offset
    );
    return Err(IllegalStateError::new(format!(
        "Failed to initialize the share partition {}-{}", group_id, topic_id_partition
    )));
}
```

---

## Files to Modify

None. This commit cannot be translated until `SharePartition` is implemented.

---

## Out of Scope

- Full translation of `SharePartition` and its initialization logic.
- Translation of the unit test `testMaybeInitializeWithInvalidOffsetInBatch` —
  requires the full `SharePartition` struct, persister mock, and test builder
  infrastructure.

---

## Definition of Done

- [x] Design document written acknowledging this commit is out of scope.
- [ ] When `SharePartition` is translated in a future milestone, incorporate
      this validation check into the batch processing loop of `maybe_initialize()`.
