# Translation Design: KAFKA-12392 — Deprecate '--max-partition-memory-bytes' option in ConsoleProducer

**AK commit:** `4fd1bedaba2ab9dda932457a2efa55186c15c069`
**AK branch:** trunk
**PR:** #80
**Rust branch:** `kafka-translate/4fd1bedaba2ab9dda932457a2efa55186c15c069`

---

## Summary of the Java Commit

KAFKA-12392 implements [KIP-1231](https://cwiki.apache.org/confluence/x/xQl3Fw), deprecating the
`--max-partition-memory-bytes` CLI option in the `kafka-console-producer` tool. The option is
superseded by the pre-existing `--batch-size` option which controls the same `batch.size` producer
config. Changes are confined to three files:

### 1. `tools/src/main/java/org/apache/kafka/tools/ConsoleProducer.java`

- The `maxPartitionMemoryBytesOpt` field is annotated with `@Deprecated(since = "4.2", forRemoval = true)`.
- The description string of `--max-partition-memory-bytes` is updated to include a `(Deprecated)`
  prefix and a note that it will be removed in Apache Kafka 5.0 with a redirect to `--batch-size`.
- The description of `--batch-size` is updated to more clearly describe its function (it now
  matches the description that `--max-partition-memory-bytes` previously had).
- A runtime warning is printed to stdout when `--max-partition-memory-bytes` is used:
  ```
  Warning: --max-partition-memory-bytes is deprecated and will be removed in Apache Kafka 5.0. Use --batch-size instead.
  ```
- The stale comment `// We currently have 2 options to set the batch.size value. We'll deprecate/remove one of them in KIP-717.`
  is removed because the deprecation is now in progress under KIP-1231.

### 2. `docs/upgrade.html`

A new entry is added to the "Notable changes in 4.2.0" section documenting the deprecation.

### 3. `checkstyle/suppressions.xml`

`ConsoleProducer` is added to the `NPathComplexity` suppression rule because the new conditional
branch for the deprecation warning increases the NPath complexity beyond the checkstyle threshold.

---

## Applicability to the Rust Client Library

### Out-of-scope: CLI tooling

The `ConsoleProducer` is a standalone command-line tool in the `tools` package of the Apache Kafka
Java repository. It is **not** part of the Kafka client library. The Rust project in this
repository translates only the Kafka **client library** (`clients/` Java package), not the
administrative or diagnostic CLI tools.

There is no Rust equivalent of `ConsoleProducer`. The `src/bin/` directory contains only
`message_generator.rs`, an internal code-generation utility unrelated to console producing.

### No client-level API impact

The deprecation does not change:
- Any client configuration semantics (the `batch.size` / `BATCH_SIZE_CONFIG` behaviour is
  unchanged).
- Any producer API surface.
- Any wire protocol.

The `batch_size` field already exists in `ProducerConfig` (`src/producer/producer_config.rs`) and
its behaviour is identical before and after this commit.

### Documentation and build tooling

The `docs/upgrade.html` and `checkstyle/suppressions.xml` changes have no Rust counterparts.

---

## Rust Implementation Plan

**No code changes are required.**

This commit is entirely confined to a Java CLI tool (`ConsoleProducer`), upgrade documentation, and
Java build tooling. None of these artefacts exist in the Rust client library project.

### Files NOT changed

| Java file | Reason not translated |
|---|---|
| `tools/src/main/java/org/apache/kafka/tools/ConsoleProducer.java` | CLI tool — no Rust equivalent exists |
| `docs/upgrade.html` | Java project documentation — not applicable to Rust project |
| `checkstyle/suppressions.xml` | Java build / style tooling — no Rust equivalent |

---

## Test Plan

No tests are required because no production code is changed.

The existing test suite (`cargo test`) should pass unchanged and serves as a regression check.
