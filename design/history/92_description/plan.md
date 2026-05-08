# Translation Plan: KAFKA-12392 — Deprecate '--max-partition-memory-bytes' option in ConsoleProducer

**AK commit:** `4fd1bedaba2ab9dda932457a2efa55186c15c069`
**AK branch:** trunk
**PR:** #92
**Rust branch:** `kafka-translate/4fd1bedaba2ab9dda932457a2efa55186c15c069`

---

## Summary of the Apache Kafka Commit

This commit implements [KIP-1231](https://cwiki.apache.org/confluence/x/xQl3Fw),
which deprecates the `--max-partition-memory-bytes` CLI option in
`ConsoleProducer` in favor of the existing `--batch-size` option.

**Changes made in the Java codebase:**

1. **`tools/src/main/java/org/apache/kafka/tools/ConsoleProducer.java`:**
   - Added `@Deprecated(since = "4.2", forRemoval = true)` annotation to the
     `maxPartitionMemoryBytesOpt` field.
   - Updated the `--batch-size` description to clarify it controls
     `batch.size` in producer configs and describe its buffer semantics.
   - Updated the `--max-partition-memory-bytes` description to indicate it is
     deprecated and will be removed in Apache Kafka 5.0, directing users to
     `--batch-size` instead.
   - Added a runtime deprecation warning printed to stdout when
     `--max-partition-memory-bytes` is used.
   - Removed an obsolete comment referencing KIP-717.

2. **`checkstyle/suppressions.xml`:** Minor suppression adjustment.

3. **`docs/upgrade.html`:** Added upgrade note about the deprecation.

---

## Rust Translation Analysis

### Does this code exist in Rust?

**No.** The Rust project does not have a `ConsoleProducer` CLI tool or any
equivalent command-line producer utility. The `tools/` directory in the Rust
repo contains only supporting infrastructure (dependency graph tooling,
performance tests, and the translation agent) — not Kafka CLI tools.

### Is there production code to translate?

**No.** The commit exclusively modifies:
- A CLI tool (`ConsoleProducer.java`) that has no Rust counterpart
- Checkstyle configuration (Java-specific)
- HTML upgrade documentation (not applicable to the Rust client library)

### What needs to be done?

**Nothing.** This commit is **not applicable** to the Rust translation for the
following reasons:

1. `ConsoleProducer` is a standalone Java CLI tool shipped with the Apache
   Kafka distribution. It is not part of the client library internals.
2. The Rust project is a client library translation, not a full Kafka
   distribution. CLI tools like `ConsoleProducer`, `ConsoleConsumer`,
   `kafka-topics.sh`, etc. are out of scope.
3. The underlying `batch.size` producer configuration that both options map to
   is already handled by the Rust producer configuration infrastructure (if/when
   it exists) without needing a CLI wrapper.

---

## Implementation Plan

This is a **no-op translation**. No Rust code changes are required.

The commit should be recorded in the translation history to track that it was
reviewed and determined to be not applicable.

---

## Files to Create / Modify

| File | Action | Reason |
|------|--------|--------|
| (none) | — | Commit is not applicable to Rust translation |

---

## Out of Scope

- Implementing a `ConsoleProducer` CLI tool in Rust — this is a CLI utility,
  not part of the client library being translated.
- Translating Kafka CLI tools in general — the Rust project focuses on the
  client protocol and library implementation.

---

## Definition of Done

- [x] Commit reviewed and classified as not applicable.
- [x] Design document written and committed to translation history.
