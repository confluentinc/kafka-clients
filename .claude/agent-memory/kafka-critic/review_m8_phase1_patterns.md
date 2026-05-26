---
name: review-m8-phase1-patterns
description: M8 Phase 1 consumer foundation patterns — config validator gaps, error mapping, internals re-export
metadata:
  type: project
---

# Milestone-8 Phase 1 (Consumer Foundation) Review Patterns

Common patterns and issues when reviewing translations of `org.apache.kafka.clients.consumer` foundation types (ConsumerRecord, ConsumerConfig, OffsetAndMetadata, etc.).

## What to check

1. **ConfigDef `atLeast(N)` validators** — Java's `ConfigDef` validators (`atLeast(0)`, `atLeast(1)`) frequently get omitted in Rust's `from_properties` translation because the Rust pattern uses inline `if v < N` checks per key rather than a generic Validator framework. Audit every numeric field in the Java `static CONFIG` block for missing range checks. Common omissions: `metrics.num.samples` (atLeast 1), `max.poll.interval.ms` (atLeast 1), `*_backoff_ms` (atLeast 0).

2. **`From<ConsumerError> for KafkaError` flattens classification** — non-retriable variants often get mapped to `IllegalState`, erasing the actual `Errors::OffsetOutOfRange` / `Errors::UnknownTopicOrPartition` code. Pattern-match downstream code may silently miss these errors. Check that the `From` impl preserves the Errors enum when one exists.

3. **`pub use internals::{X, Y}` from `pub(crate)` modules** — CLAUDE.md §2 says classes in `internal` packages must be `pub(crate)`. But Java often declares `public class X` inside an `internal` package (Headers' RecordHeader/RecordHeaders is the canonical example). The Rust correct pattern: keep `pub(crate) mod internals;` but re-export the public types at the parent module. This matches Java's effective visibility. Don't flag this as a CLAUDE.md §2 violation — it's the correct translation.

4. **Java `@Deprecated` translation** — Java classes with `@Deprecated(since="X", forRemoval=true)` should map to Rust `#[deprecated(since="X", note="...")]`. Common omission: constructors that are deprecated in favor of factory functions. Phase 1 missed `ConsumerGroupMetadata` constructor deprecation.

5. **`CloseOptions::timeout(Duration)` vs `Option<Duration>`** — Java's `withTimeout(null)` is idiomatic but `Option<Duration>` in factory signature is awkward. Watch for "trying too hard" to expose nullability in factory methods that should mirror Java's positive case.

## What NOT to flag as a bug

- `RecordHeader::key` being `String` not `Arc<str>` in M8 Phase-1 — pre-existing from M2P1, and consumer-threading.md §27 explicitly allows owned headers ("Most users do not read headers; allocating eagerly is acceptable").
- `partition.assignment.strategy` and `metric.reporters` defaulting to `Vec::new()` instead of Java's class-list defaults — intentional per consumer-threading.md §20 (classic-only, no JMX in Rust).
- ISO-8601 parser not supporting `PnW` (weeks) — Java's `Duration.parse` also doesn't support `PnW` (that's `Period.parse`); the Rust narrowness matches Java's `AutoOffsetResetStrategy.fromString` behavior exactly.
- `OffsetAndMetadata::with_metadata` accepting `impl Into<String>` and rejecting null — Java normalizes null→empty inside the constructor; Rust callers pass `""` directly, which is equivalent.
- `SubscriptionPattern::new` being infallible — Java's constructor doesn't validate (broker-side regex validation). PLAN.md was wrong to say `Result`; the Rust impl is correct.

## Key Java reference files

- `ConsumerConfig.java:415-710` — the `static { CONFIG = new ConfigDef()... }` block is the spec for all default values and validators.
- `CommonClientConfigs.java` — many `*_CONFIG` constants and `DEFAULT_*` values delegate here. Check both files when looking up a single config's default.
- `AutoOffsetResetStrategy.java:60-99` — the `fromString` method has subtle ordering: the `BY_DURATION` exact-match check happens *before* the enum-options check. Java's `try/catch` wraps the `Negative duration` check inside the outer "Unable to parse..." catch, so the surface-level message for `by_duration:-PT1H` is "Unable to parse duration string..." not "Negative duration is not supported...".
