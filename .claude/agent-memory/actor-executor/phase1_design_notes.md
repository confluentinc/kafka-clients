---
name: phase1-foundation-types-notes
description: Milestone-8 Phase 1 design decisions and gotchas for the consumer foundation types
metadata:
  type: project
---

Milestone-8 Phase 1 (consumer foundation types) decisions and gotchas.

**Why:** the public API surface, the `internals` module-visibility split, and the per-class translation choices established here are load-bearing for Phases 2-12. Re-discovering them is expensive.

**How to apply:** consult before extending the consumer module or translating new "internal" Java types.

## Decisions worth remembering

- **`RecordHeader` / `RecordHeaders` visibility.** Java declares both `public class` inside the `internals` package. CLAUDE.md §2 keeps the Rust `internals` module `pub(crate)`. To make the API actually usable, both types are re-exported at `crate::common::header::{RecordHeader, RecordHeaders}`. The same pattern should be applied to any other Java "internal but public-class" type. The `internals` module declaration on `record_header.rs`/`record_headers.rs` is `pub` (not `pub(crate)`) so the re-export compiles; previously these were `pub(crate) use`.
- **`ConsumerRecord::topic` is `Arc<str>`** per consumer-threading.md §27, NOT `String`. Same `Arc` is cloned across records from the same partition. Headers are owned (`RecordHeaders`).
- **`ConsumerRecord` has no `Clone`/`PartialEq`** — Java doesn't override `equals`, and the receive path doesn't need clones.
- **`ConsumerRecords` uses `IndexMap`, not `HashMap`** — Java's tests assume `LinkedHashMap` insertion-order iteration.
- **`OffsetAndMetadata` equality uses filtered `leader_epoch()`** — not the raw field. Negative epochs normalize to `None`. Hash must match equality.
- **`GroupProtocol::Display` is lowercase** (matches Java `toString().toLowerCase(Locale.ROOT)` used in `ConsumerConfig.DEFAULT_GROUP_PROTOCOL`); `name()` returns the upper-case Java enum name.
- **`AutoOffsetResetStrategy::ByDuration` parses a subset of ISO-8601** (`PnDTnHnMn(.fS)?`) via a hand-rolled parser in `internals/auto_offset_reset_strategy.rs` — no new dep. Negative durations rejected explicitly.
- **`ConsumerConfig` matches Java's default `group.protocol = "classic"`** (per ConsumerConfig.java line 115), not "consumer". Users must opt into KIP-848 explicitly. Classic-protocol-only / share-consumer keys are accepted silently per consumer-threading.md §20; cross-field validation (`postProcessParsedConfig`) is deferred.
- **`ConsumerError` is one enum**, not a separate exception class per Java subclass. Variant message formatters match Java toString byte-for-byte (test asserts on prefixes). Conversion `From<ConsumerError> for KafkaError` preserves retriable classification by mapping `RetriableCommitFailed` to a `KafkaError` whose `Errors` code (`RequestTimedOut`) is itself retriable.

## Pre-existing issues encountered

- **`cargo doc --no-deps` is broken on master.** Many private-item links in `producer/`, `kafka_client.rs`, `metadata.rs`, and generated code. Phase 1 did not introduce any new doc errors (verified by running cargo doc on master). Treat the doc gate as "doesn't make it worse".
- **Header types are public but the `internals` module is `pub(crate)`.** Producer-side already faces this (its `ProducerRecord::with_headers` was unusable externally before Phase 1's re-export). The fix here (`pub use internals::{RecordHeader, RecordHeaders};`) also unbreaks the producer surface for free.

## File-level inventory

| Java                                            | Rust                                                          |
|-------------------------------------------------|---------------------------------------------------------------|
| `ConsumerRecord.java`                           | `src/consumer/consumer_record.rs`                             |
| `ConsumerRecords.java`                          | `src/consumer/consumer_records.rs`                            |
| `ConsumerConfig.java`                           | `src/consumer/consumer_config.rs`                             |
| `ConsumerGroupMetadata.java`                    | `src/consumer/consumer_group_metadata.rs`                     |
| `OffsetAndMetadata.java`                        | `src/consumer/offset_and_metadata.rs`                         |
| `OffsetAndTimestamp.java`                       | `src/consumer/offset_and_timestamp.rs`                        |
| `OffsetResetStrategy.java` (deprecated)         | `src/consumer/offset_reset_strategy.rs`                       |
| `internals/AutoOffsetResetStrategy.java`        | `src/consumer/internals/auto_offset_reset_strategy.rs`        |
| `GroupProtocol.java`                            | `src/consumer/group_protocol.rs`                              |
| `CloseOptions.java`                             | `src/consumer/close_options.rs`                               |
| `SubscriptionPattern.java`                      | `src/consumer/subscription_pattern.rs`                        |
| Consumer exceptions (6 files)                   | `src/consumer/errors.rs` (one `ConsumerError` enum)           |

Tests under `tests/consumer/*_test.rs` (7 files, 25 `#[test]` fns + 2 replacement tests). Each file's preamble documents skipped Java tests with reasons.
