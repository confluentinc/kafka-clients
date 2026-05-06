# PR #67 — Translation Plan

## Apache Kafka Commit

**Commit:** `40c3f31083980d25058313a6c140c98ed21ebb25`
**Branch:** trunk
**Author:** Evan Zhou
**Date:** 2025-11-24
**Title:** MINOR: Improve Streams docs for transitory states (#20980)

## Summary of the Java Change

This commit adds five lines of Javadoc to the `KafkaStreams.State` enum in
`streams/src/main/java/org/apache/kafka/streams/KafkaStreams.java`.

The added documentation clarifies that `PENDING_SHUTDOWN` and `PENDING_ERROR`
are **transitory states** where the Streams application gracefully closes its
existing resources before transitioning into their corresponding terminal
states (`NOT_RUNNING` and `ERROR` respectively).  The key new information is:

> These states are not recoverable, and only a restart would get an
> application back to the RUNNING state.

No code logic, no API surface, and no tests were changed — this is a
pure documentation improvement.

## Files Changed in Java

```
streams/src/main/java/org/apache/kafka/streams/KafkaStreams.java  (+5 lines)
```

## Rust Translation Assessment

### Current State of Kafka Streams in Rust

**Kafka Streams has not been translated to Rust yet.**  The current Rust
codebase implements only the Kafka *client* layer:

- Network / transport stack (`src/common/network/`)
- Protocol serialization (`src/common/protocol/`)
- Request / response framework (`src/common/requests/`)
- Client layer (`src/clients/`)
- Security (SSL + SASL)

The `streams/` module from the Java repository has no counterpart in the Rust
codebase.

### Translation Action

**No implementation work is required for this PR.**

This commit is documentation-only, and the code it documents
(`KafkaStreams.State`) does not exist in the Rust repository.

When `KafkaStreams` is eventually translated, the `State` enum will include the
same states, and its Rustdoc should faithfully reproduce the updated comment,
including the clarification that `PENDING_SHUTDOWN` and `PENDING_ERROR` are
transitory, non-recoverable states requiring a restart to return to `RUNNING`.

### Dependency Analysis

- **Plan dependency:** none
- **Implementation dependency:** none

## Plan

Because this is a documentation-only change to an untranslated module, the
implementation plan is a no-op:

1. No Rust files need to be created or modified.
2. No tests need to be added or modified.
3. The PR can be closed as merged once this design document is on record.

### Future Note (non-blocking)

When `KafkaStreams` translation begins, the `State` enum Rustdoc should include
the equivalent of the added Java comment:

```rust
/// `PENDING_SHUTDOWN` and `PENDING_ERROR` are transitory states where the
/// Streams application gracefully closes its existing resources before
/// transitioning into their corresponding terminal states. These states are
/// not recoverable; only a restart can return an application to the
/// `Running` state.
```

This is tracked here for completeness and requires no action now.
