---
name: phase8c-review-patterns
description: Phase-8c Critic review takeaways — docker-exec console-consumer test harness, end-to-end byte fidelity, doc-grouping false-negative recovery
metadata:
  type: project
---

# Phase 8c — Critic review takeaways (4 commits `82f3254..07a207d`)

## What was being reviewed
- A synchronous `consume_records()` helper invoking `/opt/kafka/bin/kafka-console-consumer.sh` inside the broker container via `docker exec`, returning `Vec<(i32, Vec<u8>, Vec<u8>)>` parsed from per-line stdout.
- An end-to-end `producer_smoke_plaintext_byte_fidelity` integration test (100 records, explicit-partition routing, per-partition byte-by-byte equality between produce and consume).
- A real bug fix (mis-billed initially as a doc nit) moving the `PartitionObserverFn` cfg-gated type alias from above `KafkaProducer` to below it.
- A NOTES.md close stanza.

## Key audit points for `kafka-console-consumer` test-harness helpers

1. **Stdout pollution from deprecation warnings**: `--property` was deprecated in Kafka 3.7+ in favor of `--formatter-property` (for `print.partition`, `print.key`, `key.separator`). Recent broker images print a deprecation warning to **stdout**, which a `lines()`-based parser would treat as a record and panic on. Always use `--formatter-property` going forward.
2. **Field separator format**: `DefaultMessageFormatter` joins ALL printed fields (`Partition`, key, value) with the configured `key.separator` — NOT tab. The per-line shape is `Partition:<n><SEP><key><SEP><value>\n`. Briefs/specs that say `Partition:<n>\t<key><SEP><value>` are wrong; verify against actual broker output.
3. **String-vs-byte deserializer trap**: default deserializer is `StringDeserializer`, which lossy-decodes non-UTF-8 bytes to U+FFFD. ASCII fixtures are exact (lossy decode is identity). Binary fixtures require `--formatter-property key.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer` AND attention to BOTH `\x1F` separator collisions AND `\n` line-split collisions (the parser splits on `lines()` first).
4. **Separator-byte choice for ASCII**: `\x1F` (ASCII unit separator) is the documented "field separator" control character and cannot appear in printable-ASCII payloads. Good default.
5. **Sync helper + async caller**: blocking on `Command::output` is fine; the call must be wrapped in `tokio::task::spawn_blocking` from async contexts, otherwise it stalls the Tokio runtime for up to `timeout_ms`.

## End-to-end byte-fidelity test design verification

- **Explicit-partition send** (record `i` → partition `i % N`) keeps per-partition enqueue order deterministic. Auto-partition is incompatible with per-partition assertions (sticky partitioner batches into one partition per linger window — Phase 8b memory).
- **Grouping by ack partition** (not input partition) is the right invariant: if ever the explicit-vs-broker partition disagreed (currently they always match, pinned by Phase 8b test 1), the grouping still pins the per-partition byte content.
- **`stdout.lines()` + `splitn(3, SEP)`** is correct for ASCII fixtures with no embedded `\n` or `\x1F`. The rustdoc caveat documents this; future binary work needs both axes (Nit 1 in this phase's review).
- **`Vec<u8>` comparison, not `String`**: the test asserts on `Vec<u8>` end-to-end, so even if the lossy UTF-8 decode were lossy for the fixtures, the comparison would catch it. (It isn't for ASCII, but the comparison shape is robust.)

## Doc-grouping false-negative recovery

Phase 8b Round 1 Nit 1 was filed as documentation polish only. In Phase 8c the Actor discovered it was an actual rustdoc miswire: the struct's docs were attached to the cfg-gated type alias, and in default-feature doc builds the docs vanished entirely. See `phase8b_review_patterns.md` for the corrected audit rules. **One-line takeaway**: never assert which item a doc comment attaches to without verifying via actual rustdoc HTML output (or a minimal repro at `/tmp/doctest/`).

## Test-pattern verified safe (record for future review)

The Actor's `Arc::try_unwrap(producer_clone)` pattern in the close path *technically works* because no background task holds `Arc<KafkaProducer>` today (`grep "Arc<Self>\|Arc<KafkaProducer>\|self: Arc<" src/producer/kafka_producer.rs` is empty). But it's a fragility — if a future phase adds an `Arc<Self>`-capturing background task, this test would panic at `try_unwrap` rather than reaching the assertion it's actually pinning. Recommend the simpler `producer.close_with_timeout(…).await` (works on `&self` via `Arc` auto-deref) for any future smoke test that mirrors this pattern.

## Gates run from this review session (2026-05-21)
- `cargo xtask format-check`: green
- `cargo xtask lint`: green, no warnings
- `cargo test --lib`: 1233 passed (no count change)
- `cargo test --features integration-tests --test integration producer_smoke -- --test-threads=1`: **5/5 green**, 21.70 s wall-clock (cold Docker)
- Rustdoc verification on pre-fix `kafka_producer.rs`: `struct.KafkaProducer.html` contained zero of the struct's rustdoc text; `type.PartitionObserverFn.html` contained all of it (with feature enabled) or did not exist (without feature, docs entirely lost)
