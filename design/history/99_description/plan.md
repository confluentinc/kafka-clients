# Translation Plan: MINOR — Correcting the throttling condition for batch in Share Partition

**AK commit:** `7b017faddd366aea3f8a8196818235f8821e26d4`
**AK branch:** trunk
**PR:** #99
**Rust branch:** `kafka-translate/7b017faddd366aea3f8a8196818235f8821e26d4`

---

## Summary of the Apache Kafka Commit

This is a **bug-fix** commit (tagged MINOR) that corrects the throttling
condition in the `SharePartition.shouldThrottleRecordsDelivery()` method. The
fix ensures that batches and offsets with an ongoing state transition are not
considered for throttling, preventing incorrect throttling decisions during
concurrent state changes (e.g., a release acknowledgement that has not yet been
persisted).

**Changed files:**

| File | Change |
|------|--------|
| `core/src/main/java/kafka/server/share/SharePartition.java` | Fix throttling condition to skip batches/offsets with ongoing state transitions |
| `core/src/test/java/kafka/server/share/SharePartitionTest.java` | Add test `testAcquisitionThrottlingWithOngoingStateTransition`; fix delivery count in existing test data |

**Key logic changes:**

1. **Batch-level (offsetState == null):** Previously, the method only checked
   `batchDeliveryCount() >= limit`. Now it first verifies the batch is in
   `AVAILABLE` state **and** has no ongoing state transition before applying
   the delivery count check. If either condition fails, it returns `false`
   (not throttled).

2. **Offset-level (offsetState != null):** The stream filter previously excluded
   offsets not in `AVAILABLE` state. Now it additionally excludes offsets that
   have an ongoing state transition (`hasOngoingStateTransition()`), ensuring
   pending acknowledgement transitions are ignored.

**Before (Java):**
```java
if (inFlightBatch.offsetState() == null) {
    return inFlightBatch.batchDeliveryCount() >= throttleRecordsDeliveryLimit;
}
// offset-level filter:
if (entry.getValue().state() != RecordState.AVAILABLE) {
    return false;
}
return true;
```

**After (Java):**
```java
if (inFlightBatch.offsetState() == null) {
    if (inFlightBatch.batchState() == RecordState.AVAILABLE && !inFlightBatch.batchHasOngoingStateTransition()) {
        return inFlightBatch.batchDeliveryCount() >= throttleRecordsDeliveryLimit;
    }
    return false;
}
// offset-level filter:
return entry.getValue().state() == RecordState.AVAILABLE && !entry.getValue().hasOngoingStateTransition();
```

---

## Rust Translation Analysis

### Equivalent Rust code

The Rust codebase does **not** contain a `SharePartition` implementation or any
share-group / share-coordinator logic. The `src/` tree has no files matching
`*share*`. The Share Partition feature (KIP-932) is a server-side broker
component that has not been translated to the Rust client library.

### Assessment

This commit modifies **broker-side server code** (`kafka.server.share`). The
Rust project is a **client library** (producer/consumer/admin). Share partition
management is a broker concern and is not part of the client translation scope.

---

## Implementation Plan

**No code changes required.**

This commit is entirely within the broker's share-coordinator module
(`kafka.server.share.SharePartition`), which is outside the scope of the Rust
client library translation. There are no client-facing API changes, protocol
changes, or configuration changes in this commit.

---

## Files to Modify

| File | Action | Reason |
|------|--------|--------|
| *(none)* | — | Commit is broker-only; no Rust client equivalent exists |

---

## Out of Scope

- The entire `SharePartition` class and its test are broker-side server
  internals not relevant to the Rust client library.
- KIP-932 share-group consumer protocol support may be added to the Rust client
  in the future, but this throttling logic would remain server-side regardless.

---

## Definition of Done

- [x] Design document written acknowledging this commit is out of scope for the
      Rust client library (broker-only change).
- [ ] PR merged with no code changes (description-only PR).
