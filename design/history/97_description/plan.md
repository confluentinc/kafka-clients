# Translation Plan: MINOR — Cleanup DelayedOperationKey

**AK commit:** `7d54f7b036c15553558ef5638f559622d7b23d4a`
**AK branch:** trunk
**PR:** #97
**Rust branch:** `kafka-translate/7d54f7b036c15553558ef5638f559622d7b23d4a`

---

## Summary of the Apache Kafka Commit

This is a **server-side refactoring** commit. No client-facing behavior was changed.

The commit converts several `DelayedOperationKey` subclasses from traditional
Java classes (with explicit fields, constructors, `equals()`, `hashCode()`, and
`toString()` methods) to Java `record` types, which auto-generate those methods.

**Changed files:**

| File | Change |
|------|--------|
| `server-common/.../purgatory/TopicPartitionOperationKey.java` | Converted to `record`; removed manual `equals`/`hashCode` |
| `server-common/.../purgatory/DelayedOperationTest.java` | Converted `MockKey` inner class to `record` |
| `server/.../share/fetch/DelayedShareFetchGroupKey.java` | Converted to `record`; removed manual `equals`/`hashCode`/`toString` |
| `server/.../share/fetch/DelayedShareFetchPartitionKey.java` | Converted to `record`; removed manual `equals`/`hashCode`/`toString` |
| `storage/.../purgatory/DelayedRemoteListOffsetsTest.java` | Updated field access from `key.topic` to `key.topic()` (record accessor style) |

The net result is removal of ~108 lines of boilerplate with no behavioral change.

---

## Rust Translation Analysis

### Does corresponding code exist in Rust?

No. The Rust codebase is a **client library** (producer, network client,
metadata). The `DelayedOperationKey` hierarchy and the purgatory mechanism are
**broker-side (server) internals** used for managing delayed produce, fetch, and
share-fetch operations. None of these server-side components exist in this Rust
project.

Specifically:
- No `purgatory` module or delayed operation infrastructure exists in `src/`.
- No `DelayedShareFetch` or share-group fetch logic exists.
- The `tests/` directory contains client integration tests only.

### Is there production code to translate?

No. This commit modifies only server-side Java code and server-side tests, none
of which have Rust equivalents in this client library.

### Is there any indirect impact?

No. The refactoring:
- Does not change any wire protocol or message format.
- Does not alter any public API that clients interact with.
- Does not affect the Kafka protocol specification JSON files used by the code
  generator.

---

## Implementation Plan

### No action required

This commit is a **no-op** for the Rust translation. The changes are purely
server-side refactoring (Java class → record conversion) with no impact on
client behavior, protocol, or any code that exists in this repository.

---

## Files to Create / Modify

None.

---

## Out of Scope

- Translating broker-side purgatory/delayed-operation infrastructure — this
  Rust project is a client library and does not implement broker functionality.

---

## Definition of Done

- [x] Confirmed no Rust code corresponds to the changed Java files.
- [x] Confirmed no wire-protocol or client-visible behavior change.
- [x] No code changes needed — this PR is documentation-only.
