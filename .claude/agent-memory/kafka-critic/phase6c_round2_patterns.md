---
name: Phase-6c Round-2 verification patterns
description: Verified-good fix shapes for empty-topic type-level enforcement, original-input log capture, fast-path coverage tests, race-loser staged-state Arc::ptr_eq tests, and ArithmeticException panic translation
type: project
---

# Phase 6c Round 2 — verification patterns

## 1. Java `null` parameter elision via Rust type system

**Pattern**: when Java throws `IllegalArgumentException` for `null`, the
Rust translation should enforce non-null at the type level via `impl
Into<Arc<str>>` (or equivalent owned type) rather than a runtime
`is_empty()` guard. The Java test case that passes literal `null` is
*moot in Rust* and may be elided.

**Verification checklist**:
1. Confirm the Java test passed `null`, not `""` — `find kafka -name
   X.java -exec grep -A30 testName {} \;` and look for `null` or `""`.
   Java accepts `""` at construction; only `null` triggers
   `IllegalArgumentException`.
2. Confirm grep for the removed error variant returns only doc
   references, no live `match` arms or constructions.
3. Confirm a positive regression test pins the new contract (e.g.
   `empty_topic_is_accepted` constructs with `""` and asserts the
   field is preserved).
4. Confirm the type-level enforcement is documented in the rustdoc of
   both the error enum and the struct so future actors do not
   re-introduce the runtime check.

**Why the elision is correct**: Java's `if (topic == null)` has no
behavioral analogue in Rust because `&str` / `String` cannot be null.
A "null topic" is a compile-time error, not a runtime error.

## 2. Capture-original-input-once for log fidelity

**Pattern**: Java code that logs `record.topic()` from the original
parameter (not the loop-local `interceptRecord`) should be translated
by capturing `original_topic` / `original_partition` BEFORE the loop:

```rust
let original_topic = record.topic().to_string();
let original_partition = record.partition();
let mut intercept_record = record;
for interceptor in &self.interceptors {
    // ... catch_unwind, log uses original_topic
}
```

**Verification**:
- Read the Java line cited in the issue and confirm it references the
  outer parameter, not the loop variable. Common confusing variants:
  Java loop body shadows `record` — verify which `record` the catch
  block is using.
- Confirm the per-iteration `to_string()` allocation is collapsed to
  one upfront — bonus optimization scaling O(N) -> O(1).

## 3. Fast-path-coverage test for `Mutex<HashMap<K, Arc<AtomicX>>>` lock-free hot paths

**Pattern**: when a `ConcurrentMap<String, AtomicInteger>` is
translated to `Mutex<HashMap<Arc<str>, Arc<AtomicI32>>>`, the
fast/slow bifurcation introduces a Rust-specific regression risk
(fast path = lock-free atomic increment; slow path = mutex
insertion). Test: call the partition function multiple times for
the same topic and assert monotonic increment.

```rust
let p0 = partitioner.partition(topic, ...); // slow path: insert
let p1 = partitioner.partition(topic, ...); // fast path: increment
let p2 = partitioner.partition(topic, ...); // fast path: increment
assert_ne!(p0, p1);     // increment happened on second call
assert_eq!(p0, p2);     // wrap-around at N partitions
```

**Limitation**: the test proves the counter incremented but does not
strictly prove the fast path was taken vs. a hypothetical "always
slow path" regression. That's acceptable — the fast/slow bifurcation
exists for performance, and a slow-path-only regression would still
produce correct results, just slower.

## 4. Race-loser staged-state test via `Arc::ptr_eq`

**Pattern**: lock-free CAS code with an "early return on existing
value" branch can be tested deterministically by calling twice from a
single thread:
- First call: stages a fresh `Arc::new(...)` via `compare_and_swap`.
- Second call: hits the early-return branch (`if let Some(info) =
  load() { return info; }`).
- `Arc::ptr_eq(&first, &second)` proves the same `Arc` was returned
  (early-return branch took it), distinguishing from a hypothetical
  bug where the slow path was taken twice (which would create a
  fresh `Arc::new` on the second call).

```rust
let first = partitioner.peek_current_partition_info(&cluster);
let second = partitioner.peek_current_partition_info(&cluster);
assert!(Arc::ptr_eq(&first, &second));
```

**The CAS-lost branch (race-loser)** remains untestable
deterministically from a single-threaded test. Acceptable to leave
it documented-only via the `expect(...)` panic message that would
surface in production. Multi-threaded smoke tests are out of scope
for parity with Java's single-threaded test suite.

## 5. ArithmeticException panic translation (CLAUDE.md rule 10.1)

**Pattern**: Java's implicit `% 0` `ArithmeticException` should be
translated as Rust's implicit `% 0` panic — NOT as a `-1` sentinel
return. CLAUDE.md rule 10.1 explicitly permits panic on
`ArithmeticException`-like conditions.

**Verification checklist**:
1. Identify the Java line that raises the exception. Java is implicit:
   no `throw` statement, just `random % partitions.size()`. Read line by
   line.
2. Confirm Rust mirrors the implicit panic — no `if (n == 0) return -1;`
   guard.
3. Confirm `# Panics` rustdoc clauses on:
   - The trait method (so users of the trait know all impls may panic).
   - Each implementing struct's trait impl method.
   - The private helper if applicable.
4. Confirm regression tests with `std::panic::catch_unwind` (or
   `std::panic::catch_unwind(AssertUnwindSafe(...))` for
   non-`RefUnwindSafe` types like those holding `ArcSwap` interior
   mutability).
5. Confirm grep across `src/` returns no callers expecting the
   previous `-1` return.

**Anti-pattern**: silent `-1` sentinel that downstream code may
silently route to "partition -1". Java's stack-trace-on-divide-by-zero
is the explicit failure mode; matching it is a faithful translation.

## 6. Round-2 acceptance section template

When all Round-1 fixes verify clean, append to `COMMENTS.<N>.md`:
- **Header**: `# Round 2 — Phase XYZ accepted` with fixup-SHA list.
- **Per-issue verifications**: cite file:line for the fix, summarize
  what was changed, cross-check against Java line, note any
  bonus-optimization side effects.
- **New defects scan**: scan diffs for orphan references, sentinel
  callers, contract preservation, no-regression on prior phases,
  test soundness, test-count delta.
- **DoD sign-off**: tests pass, lint/format clean, scope-drift
  audit, fixup-chain integrity, license headers, CLAUDE.md rule
  references.
- **Verdict line**: `Verdict: accepted. 0 Blocking, 0 Suggestion.
  Phase XYZ closed.`
