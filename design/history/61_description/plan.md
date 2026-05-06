# Translation Design: MINOR: Improve documentation to clarify preferred replica

**AK Commit:** cf7b4a98b86494076c01d202a616edb3ec817cb1
**AK PR:** #20890
**AK Branch:** trunk
**Rust PR:** #61
**Rust Branch:** kafka-translate/cf7b4a98b86494076c01d202a616edb3ec817cb1

---

## Summary of the Java Change

The Apache Kafka commit improves the Javadoc on `PartitionInfo.replicas()` to make
explicit a previously undocumented invariant: the preferred replica is always placed at
the head (`index 0`) of the replicas array.  A new integration-style Scala test
`testPartitionInfoPreferredReplica` is also added to `MetadataRequestTest` to verify
this invariant end-to-end against a live broker.

### Files changed in Apache Kafka

| File | Change |
|------|--------|
| `clients/src/main/java/org/apache/kafka/common/PartitionInfo.java` | Doc-comment update on `replicas()` |
| `core/src/test/scala/unit/kafka/server/MetadataRequestTest.scala` | New test `testPartitionInfoPreferredReplica` |

### Java doc-comment (after patch)

```java
/**
 * The complete set of replicas for this partition regardless of whether they are alive
 * or up-to-date. The preferred replica is the head of the list.
 */
public Node[] replicas() { … }
```

---

## Rust Equivalent

The Rust translation of `PartitionInfo` lives in
`src/common/partition_info.rs`.  The current doc-comment on `replicas()` reads:

```rust
/// The complete set of replicas for this partition regardless of whether they are alive or up-to-date.
pub fn replicas(&self) -> &[Node] {
    &self.replicas
}
```

It is missing the sentence that clarifies the ordering guarantee.

---

## Translation Plan

### Change 1 — Update doc-comment on `PartitionInfo::replicas()`

**File:** `src/common/partition_info.rs`

Update the doc-comment on `replicas()` to match the patched Java Javadoc:

```rust
/// The complete set of replicas for this partition regardless of whether they are alive
/// or up-to-date. The preferred replica is the head of the list.
pub fn replicas(&self) -> &[Node] {
    &self.replicas
}
```

This is a pure documentation change — no behavioural code is modified.

### Change 2 — Add unit test `test_partition_info_preferred_replica`

**File:** `src/common/partition_info.rs` (`#[cfg(test)]` module)

The Java patch adds `testPartitionInfoPreferredReplica` to verify that the preferred
replica (the first broker ID in the assignment) is the first element of
`partitionInfo.replicas()`.  The Rust counterpart should add a matching unit test inside
the existing `#[cfg(test)]` block:

```rust
#[test]
fn test_partition_info_preferred_replica() {
    // replica assignment: preferred = node 1, then 2, then 0
    let replicas = vec\![make_node(1), make_node(2), make_node(0)];
    let pi = PartitionInfo::new(
        "test-topic".to_string(),
        0,
        Some(make_node(1)),
        replicas,
        vec\![make_node(1)],
    );
    // preferred replica must be the head of the replicas slice
    assert_eq\!(pi.replicas()[0].id(), 1);
}
```

The test directly mirrors the Java assertion
`assertEquals(preferredReplicaId, partitionInfo.replicas()[0].id())`.

---

## Scope

| Aspect | Detail |
|--------|--------|
| Type of change | Documentation + test |
| Source behaviour changed | No |
| New public API | No |
| Files to modify | `src/common/partition_info.rs` (1 file) |
| New files | None |
| Dependencies added | None |
| Integration tests needed | No — the invariant is already enforced by how `MetadataResponse` populates `PartitionInfo`; the unit test is sufficient |

---

## No-Op Assessment

This commit is **not** a no-op.  Although the source behaviour is unchanged, the Rust
doc-comment currently omits the ordering guarantee that the Java patch makes explicit, and
the corresponding test is absent.  Both gaps should be closed to keep the Rust codebase in
sync with the Java reference.
