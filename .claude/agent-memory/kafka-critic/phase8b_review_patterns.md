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

## Doc-comment grouping trap — **CORRECTED in Phase 8c Round 1**

**My original Phase 8b note here was wrong.** I claimed a blank line `///` separator "resets grouping" so multiple paragraph blocks could attach to different items. **It does not.** A contiguous `///` block (with or without `///` lines that are visually empty) all attach to the **next syntactic item**. There is no blank-line-separator rule that re-targets doc comments to a later item.

If you have:
```rust
/// KafkaProducer struct doc, paragraph 1.
///
/// Paragraph 2.
///
/// PartitionObserverFn is the test-seam type, paragraph 3.
#[cfg(any(test, feature = "integration-tests"))]
type PartitionObserverFn = ...;

pub struct KafkaProducer { ... }
```
**The entire 3-paragraph doc block attaches to `type PartitionObserverFn`**, NOT to `KafkaProducer`. The struct gets zero rustdoc. Verified empirically in Phase 8c Round 1 by generating rustdoc HTML for both feature configurations:
- With the cfg feature enabled: `type.PartitionObserverFn.html` has all the doc text; `struct.KafkaProducer.html` has none.
- Without the feature: the type alias is excluded from doc build entirely, so the docs **vanish completely**.

### Audit rules for future doc-grouping claims

1. **Never** assert which item a doc-comment attaches to by reading the source alone. Verify by either:
   - Generating rustdoc HTML (`cargo doc --no-deps`) and grepping for the doc text in each candidate page.
   - Building a 10-line minimal repro at `/tmp/doctest/` and running `cargo doc` on it.
2. **Clippy's `empty_line_after_doc_comments` lint** only fires when there is an actual blank line between the `///` block and the next item. If the `///` block is immediately followed by `#[cfg(...)]` or `pub fn` etc., **no lint fires** — but rustdoc still does the re-attachment. The lint is a partial defense, not a full one.
3. **The dangerous pattern is `cfg`-gated test-seam types directly between a public struct's rustdoc and the struct declaration.** Default doc builds will exclude the type alias entirely, causing the public struct's documentation to vanish without warning. Fix: place test-seam type aliases *after* the struct definition, in a `// ---- Test seam type aliases ----` banner block.

### Reason this slipped past Phase 8b Round 1
I trusted the visual layout (blank `///` line as paragraph separator → "looks like the type alias has its own paragraph") instead of testing the actual rustdoc output. Apply rule 1 above unconditionally for any future doc-grouping observation. Source: Phase 8c Round 1 review, commits `82f3254..07a207d`, finding type "Resolution of the 8b Round 1 doc-grouping claim".

## Gates run from this review session (2026-05-20)
- `cargo build --features integration-tests`: clean, 12.10 s
- `cargo xtask format-check`: green
- `cargo xtask lint`: green, no warnings
- `cargo test --lib`: 1233 passed
- `cargo test --features integration-tests producer_smoke`: 4/4 green in 9.01 s
