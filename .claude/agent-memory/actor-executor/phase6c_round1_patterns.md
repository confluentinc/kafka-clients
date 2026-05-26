---
name: Phase 6c Round 1 patterns
description: Translation/test patterns settled during Phase 6c Round 1 review fixes — type-level null elimination, ArithmeticException → panic per CLAUDE.md 10.1, log-original-input parity, fast/slow path coverage, Arc::ptr_eq for staged-state coverage.
type: project
---

Patterns settled during Phase 6c Round 1 fix-up that are likely to recur.

**Why:** Critic flagged real Java parity issues that came from "translate-it-literally" instincts and the Rust type system absorbing semantic checks Java guards at runtime.

**How to apply:** When the same shape comes up in Phase 6d/7+ work, reach for these.

## Type-level null elimination — drop the runtime guard, drop the variant

When a Java constructor's `if (X == null)` becomes a Rust constructor parameter that has no `null` representation (`impl Into<Arc<str>>`, `&str`, `String`), the runtime check is unnecessary AND should not silently absorb the empty-string case. Java's contract is "null is rejected, empty is accepted" — converting `""` to a NullTopic-equivalent variant is a real behavior divergence.

- Drop the runtime `is_empty()`/`if x.is_empty()` guard.
- Drop the corresponding error variant (`NullTopic`, etc.) if it becomes unreachable.
- Document the elimination in the struct-level rustdoc and the test docstring (so future readers know the Java case is intentionally moot).
- Add a positive regression test that pins the new contract (`empty_topic_is_accepted`).

## ArithmeticException divide-by-zero → Rust panic, not -1 sentinel

Java's `random % numPartitions` raises `ArithmeticException` on a zero-partition topic. Three options when translating:
- (a) Result<i32, KafkaError>: invasive, changes every caller, not Java-faithful.
- (b) Keep -1 sentinel: silently routes to "partition -1", real behavior divergence.
- (c) Let the Rust `%` panic / explicit `panic!()`: matches Java exactly.

CLAUDE.md rule 10.1 explicitly permits panic on "OOM or `ArithmeticException` like division by zero". For the partitioner-style paths (and any direct translation of a Java line that would `% 0`), prefer **(c)**. Document the panic in the trait's `# Panics` section and add a regression test using `std::panic::catch_unwind`.

This generalizes: when Java raises an unchecked exception that CLAUDE.md rule 10.1 specifically calls out, pick the Java-faithful panic over a sentinel.

## Original-input vs running-state parity in catch blocks

When a Java try/catch logs the original input parameter (`record.topic()`), and the Rust translation captures from the running state (`intercept_record.topic()`), it's a real parity issue even if no test asserts on log content. Java's `record` reference doesn't change; Rust's `intercept_record` does (it's been mutated by previous interceptors).

**Fix pattern:** capture the original-input snapshot once before the loop, in locals (`original_topic`, `original_partition`), and reuse for every iteration's log message. Bonus: avoids per-iteration `to_string()` allocation on the hot path (one upfront vs N).

This applies to any Java code that says `log(record.topic(), ...)` inside a loop where `record` is the loop's input parameter and a working variable is being mutated separately.

## Fast/slow-path test coverage when Java has no equivalent bifurcation

When the Rust translation introduces a fast/slow path bifurcation that Java doesn't have (e.g. `Arc<AtomicI32>` lock-free hot path + mutex slow path replacing `ConcurrentHashMap.computeIfAbsent`), the Java tests don't exercise the fast path because Java has only one path.

**Fix pattern:** add a Rust-specific regression test that calls the operation N>=2 times and asserts state advances correctly. The first call goes through the slow path (state created), subsequent calls hit the fast path. The assertion only passes if both paths are correct.

## Arc::ptr_eq for staged-state coverage

When a method has an "exists, return early" branch that is hit incidentally by other tests but never pinned by an explicit assertion, add a regression test using `Arc::ptr_eq`:
1. Call the method once (stages state).
2. Call again, capture both Arcs.
3. `assert!(Arc::ptr_eq(&first, &second))` — proves the second call returned the staged Arc, not a new one.

This is more precise than asserting equal partition values (which could pass even if a new Arc was created with the same partition number on each call).

## Race-loser branches — when not deterministically testable

Some race-resolve branches (CAS-lost-to-concurrent-thread) cannot be triggered deterministically without forcing a specific thread interleaving. Acceptable to:
1. Document the branch with an `expect(...)` panic message that would surface in production logs.
2. Mention in the test module docstring that the path is verified by manual reasoning, not by test.
3. Add the easier deterministic branch (early-return) test to lock down the hot path.

Don't add a flaky multi-threaded test that "usually" hits the race.
