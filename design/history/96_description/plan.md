# Translation Plan: MINOR — Improve documentation to clarify preferred replica

**AK commit:** `cf7b4a98b86494076c01d202a616edb3ec817cb1`
**AK branch:** trunk
**PR:** #96
**Rust branch:** `kafka-translate/cf7b4a98b86494076c01d202a616edb3ec817cb1`

---

## Summary of the Apache Kafka Commit

This is a **documentation-only** commit (tagged MINOR). It improves the Javadoc
on `PartitionInfo.replicas()` to clarify that the preferred replica is always
the head of the returned array. A corresponding Scala integration test
(`testPartitionInfoPreferredReplica`) was added to verify this ordering
guarantee.

**Changed files:**

| File | Change |
|------|--------|
| `clients/src/main/java/org/apache/kafka/common/PartitionInfo.java` | Javadoc update on `replicas()` |
| `core/src/test/scala/unit/kafka/server/MetadataRequestTest.scala` | New test asserting preferred replica ordering |

**Before (Java):**
```java
/** The complete set of replicas for this partition regardless of whether they are alive or up-to-date */
```

**After (Java):**
```java
/** The complete set of replicas for this partition regardless of whether they are alive or up-to-date. The preferred replica
 * is the head of the list. */
```

---

## Rust Translation Analysis

### Equivalent Rust code

The Rust codebase has a direct equivalent:
`src/common/partition_info.rs` — the `PartitionInfo::replicas()` method (line 76).

The current Rust doc comment reads:
```rust
/// The complete set of replicas for this partition regardless of whether they are alive or up-to-date.
```

This matches the **old** Java version and needs the same clarification.

### Test equivalent

The AK commit adds a Scala integration test (`testPartitionInfoPreferredReplica`)
that creates a topic with explicit replica assignment `{0 -> [1, 2, 0]}`, sends
a `MetadataRequest`, and asserts that the head of `partitionInfo.replicas()` has
the expected preferred replica ID.

The Rust project does not currently have a KRaft-capable test broker that
supports `createTopicWithAssignment` or replica placement verification. This
integration test is **out of scope** for this PR.

---

## Implementation Plan

### Phase 1 — Update doc comment on `PartitionInfo::replicas()`

**File:** `src/common/partition_info.rs`

Change the doc comment on `replicas()` from:
```rust
/// The complete set of replicas for this partition regardless of whether they are alive or up-to-date.
```
to:
```rust
/// The complete set of replicas for this partition regardless of whether they are alive or
/// up-to-date. The preferred replica is the head of the list.
```

### Phase 2 — Verify build

Run `cargo build` and `cargo test` to confirm the doc comment change compiles
cleanly and existing tests pass.

---

## Files to Modify

| File | Action | Reason |
|------|--------|--------|
| `src/common/partition_info.rs` | Modify | Update `replicas()` doc comment to note preferred replica ordering |

No new files need to be created. No production logic changes required.

---

## Out of Scope

- Translating `MetadataRequestTest.testPartitionInfoPreferredReplica` — requires
  a test broker with topic creation and explicit replica assignment, which is not
  yet available in the Rust test infrastructure.
- Any changes to how replicas are ordered in `MetadataResponse` handling — the
  ordering guarantee already exists in the Rust code (replicas are stored in the
  order received from the broker); this commit only documents it.

---

## Definition of Done

- [ ] Doc comment on `PartitionInfo::replicas()` updated to mention preferred replica ordering.
- [ ] `cargo build` succeeds with no warnings.
- [ ] `cargo test` passes.
