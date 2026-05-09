# Translation Plan: MINOR — Improve Streams docs for transitory states

**AK commit:** `40c3f31083980d25058313a6c140c98ed21ebb25`
**AK branch:** trunk
**PR:** #102
**Rust branch:** `kafka-translate/40c3f31083980d25058313a6c140c98ed21ebb25`

---

## Summary of the Apache Kafka Commit

This is a **documentation-only change**. No production or test logic was modified.

The commit adds a Javadoc bullet point to the `KafkaStreams` class state
transition documentation, clarifying that `PENDING_SHUTDOWN` and `PENDING_ERROR`
are transitory states where the Streams application gracefully closes its
existing resources before transitioning into their corresponding terminal states.
These states are not recoverable — only a restart would get an application back
to the `RUNNING` state.

**Changed file:**
```
streams/src/main/java/org/apache/kafka/streams/KafkaStreams.java
```

**Diff (5 lines added):**
```java
+     *     PENDING_SHUTDOWN and PENDING_ERROR are transitory states where the Streams application gracefully closes
+     *     its existing resources before transitioning into their corresponding terminal states. These states are
+     *     not recoverable, and only a restart would get an application back to the RUNNING state.
```

---

## Rust Translation Analysis

### Does equivalent code exist in Rust?

No. The Rust codebase does not currently contain a Kafka Streams implementation.
Kafka Streams is a JVM-only stream processing library built on top of the Kafka
client. The Rust project focuses on the core Kafka client protocol (producer,
consumer, admin) and does not include a Streams equivalent.

### Is there production code to translate?

No. The AK commit modifies only Javadoc comments in a Streams-specific class.
There is no behavioral change and no corresponding Rust source file.

### What needs to be done?

**Nothing.** This commit is not applicable to the Rust translation for two
reasons:

1. **Kafka Streams is out of scope** — the Rust project translates the core
   Kafka client and broker protocol, not the Streams library.
2. **The change is documentation-only** — even if the class existed in Rust,
   this would be a doc-comment update with no functional impact.

---

## Implementation Plan

No implementation is required. This PR should be closed as not applicable.

---

## Files to Create / Modify

| File | Action | Reason |
|------|--------|--------|
| _(none)_ | — | Commit is not applicable to the Rust codebase |

---

## Out of Scope

- Implementing Kafka Streams in Rust.
- Translating any Streams-related classes or documentation.

---

## Definition of Done

- [x] Design document written acknowledging this commit is not applicable.
- [ ] PR closed or merged as no-op (no code changes needed).
