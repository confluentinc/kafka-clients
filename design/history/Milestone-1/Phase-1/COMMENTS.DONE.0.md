# Phase 1 Review — Resolved Comments

Comments that were filed by the Critic (N=0) and have been resolved by the
Actor. Each fix is committed as a fixup against the original commit that
introduced the issue. See `git log --grep=fixup!` for the full list.

---

## Issue 1: `MockTime::new` initial nanoseconds value is wrong (always ~0)

- **Severity:** **MAJOR** (Bug — caller-visible behaviour mismatch with Java)
- **File:** `src/common/utils/mock_time.rs:62-68`
- **Java reference:** `kafka/clients/src/test/java/org/apache/kafka/common/utils/MockTime.java:51-53`
- **What Java does:**
  ```java
  public MockTime(long autoTickMs) {
      this(autoTickMs, System.currentTimeMillis(), System.nanoTime());
  }
  ```
  `System.nanoTime()` returns nanoseconds since some fixed-but-arbitrary epoch — a large, monotonic value. This matters for tests that assert nanosecond/millisecond independence (the comment on line 42-43 of `MockTime.java` is explicit: "Values from `nanoTime` and `currentTimeMillis` are not comparable, so we store them separately to allow tests using this class to detect bugs where this is incorrectly assumed to be true").
- **What Rust did:**
  ```rust
  let now_ns = std::time::Instant::now().elapsed().as_nanos() as i64;
  ```
  `Instant::now().elapsed()` returns the duration *since* that very `Instant::now()` call — i.e. effectively zero (a few hundred nanoseconds at most). The high-res clock starts at ~0, NOT at a `nanoTime`-equivalent reference point.
- **Why it's wrong:** A test that wants to detect "the producer code accidentally subtracted `nanoTime` from `currentTimeMillis` (or vice versa)" cannot do so because both are now numerically tiny and indistinguishable. The independence guarantee Java's `MockTime` is documented to provide is silently broken.
- **Fix applied:** Use `SystemTime::UNIX_EPOCH.elapsed().as_nanos()` (a stable wall-clock reference) so the value is large like Java's. Fixup commit references `2dc5b71`.

---

## Issue 2: `is_fatal()` does not include `OutOfOrderSequence` / `UnknownProducerId`

- **Severity:** **MINOR** (Behaviour parity; only relevant once idempotent/transactional path is added — Phase 6+)
- **File:** `src/common/errors.rs:190-201`
- **Fix applied:** Updated the `is_fatal` docstring to clarify the method covers the **non-idempotent** producer, and added a TODO note pointing at `Sender.completeBatch` so the idempotent / transactional path (Milestone 6+) extends `is_fatal` to include `OutOfOrderSequence` / `UnknownProducerId`. Did not flip them on now since transactions are out of scope for Milestone 1 and unconditionally tagging them fatal would be incorrect for the non-idempotent producer that Phase 1 is building toward. Fixup commit references `43c713b`.

---

## Issue 3: `Uuid::from_string` length check uses byte length, not character count, and slice may panic

- **Severity:** **MINOR** (edge case — only triggers on non-ASCII input that Java would still reject but with a different message)
- **File:** `src/common/uuid.rs:103-107`
- **Fix applied:** Replaced `s.len() > 24` and `&s[..24]` with char-boundary slicing (`s.chars().take(25).count() > 24` and `s.chars().take(24).collect()`). Added a regression test (`from_string_non_ascii_too_long_does_not_panic`) using a 25-emoji input that would have panicked on the old byte-indexed slice. Fixup commit references `43c713b`.

---

## Issue 4: `Uuid` `testHashCode` Java fixture not translated; explicit `hash_code` missing

- **Severity:** **MINOR** (test gap — Java test asserts specific hash values that are part of the wire-equivalence contract)
- **File:** `src/common/uuid.rs`
- **Fix applied:** Added an explicit `pub const fn hash_code(&self) -> i32` matching Java's `(int)(xor >> 32) ^ (int) xor` formula, with a docstring noting that the derived `std::hash::Hash` is intentionally separate (used for HashMap keys; not wire-visible). Translated the Java `testHashCode` fixture as `hash_code_matches_java`. Fixup commit references `43c713b`.

---

## Issue 5: Java `UuidTest::testStringConversion` round-trip with `ZERO_UUID` not translated

- **Severity:** **MINOR** (test coverage gap)
- **File:** `src/common/uuid.rs`
- **Fix applied:** Added `ZERO_UUID` round-trip assertion to `to_string_round_trip` to mirror `UuidTest.java:69-78`. Fixup commit references `43c713b`.

---

## Issue 6: `config_exception::new` formats value with `Debug`, not `Display`

- **Severity:** **MINOR** (error-message text mismatch, no functional impact)
- **File:** `src/common/config/config_exception.rs:26-28`
- **Fix applied:** Implemented `fmt::Display` for `ConfigValue` mirroring Java's `Object.toString()` semantics per variant (`Int(5)` -> `5`, `String("foo")` -> `foo`, `List([a,b])` -> `[a, b]`, `Password` -> `[hidden]`, `Null` -> `null`). Changed `config_exception::new` to take `impl Display` and use `{value}` rather than `{value:?}`. Strengthened `range_at_least_rejects_below` to assert the full Java-equivalent message text. Fixup commit references `5eaecbf`.
