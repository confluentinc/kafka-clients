# Translation Plan: KAFKA-19922 — Updated docs for start-offset in share groups

**AK commit:** `17a09f5208a1c1d127fe22730f35adf95e5f00a1`
**AK branch:** trunk
**PR:** #112
**Rust branch:** `kafka-translate/17a09f5208a1c1d127fe22730f35adf95e5f00a1`

---

## Summary of the Apache Kafka Commit

This is a **documentation-only** commit. It improves the Javadoc on
`SharePartitionOffsetInfo` to clarify the meaning of the start offset in share
groups, and adds a corresponding explanation to `docs/ops.html`.

**Changed files:**

| File | Change |
|------|--------|
| `clients/src/main/java/org/apache/kafka/clients/admin/SharePartitionOffsetInfo.java` | Added/improved Javadoc on constructor param and getter methods |
| `docs/ops.html` | Added explanation of start offset meaning in share group describe output |

**Key documentation change:**

The start offset is clarified as "the earliest offset for in-flight records
being evaluated for delivery to share consumers. Some records after the start
offset may already have completed delivery."

Additionally, Javadoc was added to the previously undocumented `startOffset()`,
`leaderEpoch()`, and `lag()` getter methods.

---

## Rust Translation Analysis

### Equivalent Rust code

The Rust codebase does **not** currently have a `SharePartitionOffsetInfo`
struct or any share-group-related admin client code. The `src/` directory
contains no files related to share groups or share partitions.

### Documentation equivalent

The `docs/ops.html` change is a server-side documentation file that has no
equivalent in the Rust client library.

---

## Implementation Plan

### Result: No action required

This commit is **not translatable** to the Rust codebase because:

1. **`SharePartitionOffsetInfo`** does not exist in the Rust client — the share
   groups admin API has not been translated yet.
2. **`docs/ops.html`** is server-side operational documentation with no
   equivalent in the Rust client library.

Since this is purely a documentation improvement to code that does not yet exist
in the Rust translation, there is nothing to implement.

---

## Files to Modify

None.

---

## Out of Scope

- Translation of `SharePartitionOffsetInfo` — this class is part of the admin
  client share groups API which has not been translated to Rust yet.
- Translation of `docs/ops.html` — this is server-side documentation, not part
  of the client library.

---

## Definition of Done

- [x] Design document written acknowledging no translation is needed.
- [ ] PR closed with explanation that this commit has no translatable changes.
