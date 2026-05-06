# Translation Plan: MINOR: Cleanup DelayedOperationKey (#20947)

## AK Commit

- **SHA:** `7d54f7b036c15553558ef5638f559622d7b23d4a`
- **Branch:** `trunk`
- **Title:** MINOR: Cleanup DelayedOperationKey (#20947)
- **Author:** Jhen-Yung Hsu
- **Date:** 2025-11-25

## Summary of the Java Change

This is a purely mechanical refactor of four server-side broker classes. Each
class that implements `DelayedOperationKey` (or `DelayedShareFetchKey`) was
rewritten as a Java `record`, removing hand-written boilerplate:

| File changed | Change |
|---|---|
| `server-common/src/main/java/org/apache/kafka/server/purgatory/TopicPartitionOperationKey.java` | Convert class → record; remove manual `equals`, `hashCode` |
| `server/src/main/java/org/apache/kafka/server/share/fetch/DelayedShareFetchGroupKey.java` | Convert class → record; remove manual `equals`, `hashCode`, `toString` |
| `server/src/main/java/org/apache/kafka/server/share/fetch/DelayedShareFetchPartitionKey.java` | Convert class → record; remove manual `equals`, `hashCode`, `toString` |
| `server-common/src/test/java/org/apache/kafka/server/purgatory/DelayedOperationTest.java` (inner `MockKey`) | Convert static class → record |
| `storage/src/test/java/org/apache/kafka/server/purgatory/DelayedRemoteListOffsetsTest.java` | Update field access `key.topic` → `key.topic()`, `key.partition` → `key.partition()` |

No logic was changed. Public API semantics (`equals`, `hashCode`, accessor
semantics) are preserved by the Java `record` mechanism.

## Scope Analysis

### Where these classes live in the Java tree

All affected classes are under:
- `server-common/` — broker utility types
- `server/` — broker implementation (share fetch, delayed operations)
- `storage/` — broker storage layer (test)

These modules implement **broker-side** delayed-operation purgatory, not the
Kafka client library. The purgatory subsystem manages delayed produce and fetch
operations inside the broker JVM.

### What the Rust repo translates

The Confluent Kafka Rust project is a translation of the **Apache Kafka client
library** (`kafka/clients/`), not the broker. The current codebase covers:
- Protocol wire layer, request/response types
- Network transport (TCP, TLS, SASL)
- `NetworkClient`, `Metadata`, `KafkaProducer` (in progress)

There are no translated counterparts for any of:
- `DelayedOperationKey` / `DelayedOperation` (purgatory)
- `TopicPartitionOperationKey`
- `DelayedShareFetchGroupKey`
- `DelayedShareFetchPartitionKey`

A search across `src/` confirms none of these names appear anywhere in the Rust
source tree.

## Translation Decision: No-op

This commit touches only broker-internal key types that have no Rust
equivalents. The Java `record` conversion is a language-level refactor with no
behavioral impact. There is nothing to translate.

**No Rust files need to be created or modified.**

## Verification

No build, test, or lint steps are needed since no code changes are made.
