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
