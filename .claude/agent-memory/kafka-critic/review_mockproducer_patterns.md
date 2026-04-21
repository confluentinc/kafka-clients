---
name: MockProducer translation patterns
description: Java public field error injection uses persistent semantics; Rust Option::take() changes to one-shot — systematic behavioral mismatch pattern
type: project
---

Java's MockProducer uses public fields for error injection (`sendException`, `flushException`, etc.) that persist until manually cleared. Initial translation used `Option::take()` which changed from "all calls fail" to "only next call fails" semantics. Fixed in commit 600755f to use `as_ref().clone()`.

**Why:** Java's pattern is `if (this.sendException != null) { throw this.sendException; }` — the field is never cleared by the check. Rust's `Option::take()` consumes the value. This is a systematic pattern that could appear in other Mock* implementations.

**How to apply:** When reviewing any Java class that uses public exception fields for testing injection, verify whether the Rust translation preserves persistence semantics. The `take()` vs `as_ref().clone()` choice determines one-shot vs persistent behavior. This was caught and fixed for MockProducer -- watch for same pattern in MockConsumer or other Mock* classes.

Also notable: Java `synchronized` is reentrant but Rust `Mutex` is not. When Java methods call other `synchronized` methods while holding the lock (e.g., `flush()` calling `completeNext()`), Rust must inline the logic or restructure to avoid deadlock. The MockProducer `flush()` correctly handles this by directly draining completions.

Phase 4 external tests: Watch for tests that intentionally diverge from Java (e.g., sending more records, skipping utility method verification like `clear()`). Even when internal unit tests cover the functionality, external tests should mirror the Java test 1:1 per DoD.

**clear() offset reset bug (found 2026-04-14):** Java `MockProducer.clear()` does NOT reset the `offsets` map (per-TopicPartition offset counter). Rust `clear()` incorrectly calls `inner.offsets.clear()`, which resets offsets to 0. This means post-clear sends restart offset numbering in Rust but continue the sequence in Java. Reported in COMMENTS.0.md.

**How to apply:** When a Java method clears some fields but not others, verify the Rust translation matches exactly which fields are cleared. The `clear()` method is a selective reset, not a full reinit — be suspicious of "clear everything" patterns in Rust that may not match Java's more selective approach.
