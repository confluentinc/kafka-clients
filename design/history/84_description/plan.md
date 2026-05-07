# Translation Design: MINOR — Improve documentation to clarify preferred replica

**AK commit:** `cf7b4a98b86494076c01d202a616edb3ec817cb1`
**AK branch:** trunk
**PR:** #84
**Rust branch:** `kafka-translate/cf7b4a98b86494076c01d202a616edb3ec817cb1`

---

## Summary of the Java Commit

This is a documentation-only commit that clarifies the contract of
`PartitionInfo.replicas()` in the Java client.

### 1. `PartitionInfo.java` — Javadoc clarification

The Javadoc for the `replicas()` method is updated to state explicitly
that the **preferred replica is the head of the list**:

```java
// Before:
/**
 * The complete set of replicas for this partition regardless of whether they are alive or up-to-date
 */

// After:
/**
 * The complete set of replicas for this partition regardless of whether they are alive or up-to-date. The preferred replica
 * is the head of the list.
 */
```

### 2. `MetadataRequestTest.scala` — new behavioural test

A new integration test `testPartitionInfoPreferredReplica` is added that:
- Creates a topic with a specific replica assignment: partition 0 → `[1, 2, 0]`
  (broker 1 is the preferred/leader replica).
- Sends a `MetadataRequest` and builds the `Cluster` from the response.
- Asserts that `partitionInfo.replicas()[0].id() == 1`, i.e. the first
  element of the replicas array is indeed the preferred replica as
  specified in the assignment.

This test codifies the previously-implicit contract that the broker
preserves the replica assignment order when encoding the
`MetadataResponse`, and that `PartitionInfo.replicas()` reflects that
order faithfully.

---

## Applicability to the Rust Client Library

### Direct Rust counterpart

The Rust library has a direct equivalent of `PartitionInfo` at:

```
src/common/partition_info.rs
```

The `replicas()` method there currently carries the doc comment:

```rust
/// The complete set of replicas for this partition regardless of whether they are alive or up-to-date.
pub fn replicas(&self) -> &[Node] {
    &self.replicas
}
```

This is a word-for-word translation of the *old* Java Javadoc. The Java
commit's clarification — that the preferred replica is first — must be
reflected in the Rust doc comment to keep parity.

### Behavioural correctness of existing code

The Rust `MetadataResponse` deserialization (`src/common/requests/metadata_response.rs`)
builds `PartitionInfo` by iterating over the replicas in the order they
arrive in the wire protocol. The Kafka wire protocol preserves the
broker's replica assignment order, so the preferred replica is already
first. No logic change is required.

### Test gap

The existing Rust unit tests for `PartitionInfo`
(`src/common/partition_info.rs`, `#[cfg(test)]` module) do not have a
test that explicitly verifies the preferred replica is `replicas()[0]`.
This should be added to match the spirit of the Java test.

---

## Rust Implementation Plan

### Files to change

#### `src/common/partition_info.rs`

1. **Update the doc comment for `replicas()`** from:

   ```rust
   /// The complete set of replicas for this partition regardless of whether they are alive or up-to-date.
   ```

   to:

   ```rust
   /// The complete set of replicas for this partition regardless of whether they are alive or up-to-date.
   /// The preferred replica is the head of the slice.
   ```

2. **Add a test `test_replicas_preferred_replica_is_first`** in the
   existing `#[cfg(test)]` module:
   - Construct a `PartitionInfo` where the replicas vec is
     `[node(1), node(2), node(0)]` (node 1 is the preferred replica).
   - Assert `pi.replicas()[0].id() == 1`.
   - This mirrors exactly what `testPartitionInfoPreferredReplica` does
     in the Java test suite.

### Files NOT changed

| Java file | Reason not translated |
|---|---|
| `MetadataRequestTest.scala` | Integration/server-side test; no Rust integration test infrastructure for this scenario is needed beyond the unit test above |

---

## Test Plan

Single new unit test in `src/common/partition_info.rs`:

```rust
#[test]
fn test_replicas_preferred_replica_is_first() {
    // replica assignment: [1, 2, 0] — node 1 is the preferred replica
    let replicas = vec![make_node(1), make_node(2), make_node(0)];
    let pi = PartitionInfo::new(
        "test-topic".to_string(),
        0,
        Some(make_node(1)),
        replicas,
        vec![make_node(1)],
    );
    assert_eq!(pi.replicas()[0].id(), 1, "preferred replica must be first in the replicas slice");
}
```

Run with `cargo test -p confluent-kafka-rust partition_info` to verify.
