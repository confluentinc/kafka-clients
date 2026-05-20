---
name: phase8b-review-patterns
description: Phase-8b review patterns — cfg-gated test seams in production code, observer-callback exactly-once verification, flush-vs-close regression pinning
metadata:
  type: project
---

# Phase 8b — Critic review takeaways (close-out review of 4 commits `7d9f892..275eb3b`)

## What was being reviewed
- Integration tests against a real Testcontainers broker, building on the Phase 8a scaffold
- A `#[cfg(any(test, feature = "integration-tests"))]`-gated `partition_observer` test seam added to the production `KafkaProducer` struct (a Rust-only addition with no Java equivalent at the interceptor layer)
- Three new integration tests (1000-record explicit-partition, 1000-record auto-partition with observer, 50-record flush) and one tightened existing test (close-flushes-pending-inflight shape parity)

## Key Java refs for partition observer placement
- `KafkaProducer.java:950` — `interceptors.onSend(record)` runs BEFORE partitioning at 1024
- `KafkaProducer.java:1014-1024` — explicit-partition early-return in `KafkaProducer.partition()`
- `KafkaProducer.java:1036-1051` — accumulator.append → `assert appendCallbacks.getPartition() != UNKNOWN_PARTITION` → transaction-manager → sender.wakeup
- `KafkaProducer.java:1600` — `interceptors.onAcknowledgement` runs POST-broker-ack (cannot witness pre-network partition)

The Rust observation point should sit between Java line 1038 (post-`append`, post-assert) and line 1044 (transaction-manager block) — this preserves Java lifecycle ordering and ensures the observer sees the final accumulator-resolved partition.

## Audit checklist for cfg-gated test seams added to production structs

1. **Field cfg-gating completeness**: every reference to the field/method must be `#[cfg(any(test, feature = "integration-tests"))]`. Grep all references; expect type alias, field decl, accessor impl, constructor initializer, observation site to all be gated. A leaked reference in non-test code compiles in test builds but breaks production builds (visible) — or worse, compiles in production builds but introduces a hot-path field (silent).
2. **Lock-across-await audit**: if the seam uses `Mutex<Option<Fn>>`, the observation site MUST extract the `Option<Arc<...>>` from the guard, drop the guard via inner scope, THEN invoke the closure. Holding a sync `MutexGuard` across an `.await` is a runtime deadlock per CLAUDE.md 9.6. (For sync closures, this is moot only because the closure has no `.await` — but the pattern should still extract-then-drop for future-proofing.)
3. **Callback exactly-once contract** (CLAUDE.md 9.5): trace every call site of the function containing the observation. For a producer-side observer, the only acceptable shape is one observer-fire per user-facing `send()`. Sender-driven retries must NOT re-enter the observed function (`do_send_inner` in this codebase) — they operate on already-batched records.
4. **Error-path behavior**: the observer should fire ONLY on the success path (after the function's `?` propagations). This makes `observed.len() == N` a meaningful invariant for an N-record happy-path test.

## Flush-vs-close regression-pin pattern (test 3)

Phase 8b's test 3 introduced a strong pattern: after `producer.flush().await`, the test does ONE additional `producer.send(record).await?.get().await` to prove the producer is still usable. This catches a regression that would silently turn `flush()` into a `close()`:
- The captured futures would still resolve (the close-drain path also resolves them)
- Tests 1-3 of the standard flush contract would pass
- Only the post-flush send would fail with `IllegalState`

Apply this pattern to any future "drain" assertion where the underlying call could be misimplemented as a "tear down" call.

## Sticky-partitioner caveat for auto-partition coverage tests

For auto-partition path tests with sequential sends, the default sticky partitioner can batch ALL records into a single partition within one linger window. So an auto-partition test CANNOT meaningfully assert N-partition coverage. The contract that CAN be asserted is partitioner-vs-broker agreement (what the partitioner chose pre-network equals what the broker echoed back). Coverage assertions belong on the explicit-partition path.

## Doc-comment grouping trap (Nit-level)

Rust attaches `///` doc-comments to the next item after the comment block. A blank line resets grouping. If you have:
```rust
/// KafkaProducer struct doc...
///
/// PartitionObserverFn is the test-seam type...
#[cfg(any(test, feature = "integration-tests"))]
type PartitionObserverFn = ...;
```
the `PartitionObserverFn` doc block correctly attaches to the type alias (because of the blank line). But a reader skimming sees four consecutive doc blocks under one heading-like link and assumes they all belong to `KafkaProducer`. Recommend a `// ---- separator ----` non-doc comment to break the visual block.

## Gates run from this review session (2026-05-20)
- `cargo build --features integration-tests`: clean, 12.10 s
- `cargo xtask format-check`: green
- `cargo xtask lint`: green, no warnings
- `cargo test --lib`: 1233 passed
- `cargo test --features integration-tests producer_smoke`: 4/4 green in 9.01 s
