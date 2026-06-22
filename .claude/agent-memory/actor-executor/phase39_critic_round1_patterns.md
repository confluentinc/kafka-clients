---
name: phase39-critic-round1-patterns
description: Phase 39 Critic round-1 fixes — Java maybeWrapAsKafkaException conditional wrap on rebalance-callback error path, is_kafka_exception predicate, de-flake via remove-records
metadata:
  type: project
---

Phase 39 Critic round-1 (COMMENTS.39) — three reusable fix patterns.

**1. Java `maybeWrapAsKafkaException(e, message)` is CONDITIONAL — translate the `instanceof KafkaException` test, don't blindly prepend.**
- `ConsumerUtils.java:256`: if `t instanceof KafkaException` return it UNCHANGED; else `new KafkaException(message, t)` whose `getMessage()` is EXACTLY `message` (cause separate). `invokeRebalanceCallbacks` (AsyncKafkaConsumer.java:2334) applies the message-arg variant; the close-path (AsyncKafkaConsumer.java:1641) uses the single-arg `maybeWrapAsKafkaException(t)` which is identity in Rust (everything is already KafkaError) — no change there.
- Added `KafkaError::is_kafka_exception()` (src/common/kafka_error.rs) = `!matches!(IllegalArgument|IllegalState)`. Distinct from `is_api_exception()`: Wakeup IS a KafkaException (passes through) but is NOT an ApiException. Java's IllegalArgumentException/IllegalStateException are the only RuntimeExceptions modelled that are NOT KafkaException.
- The wrap belongs at the consumer's `process_background_events` (mirror of invokeRebalanceCallbacks), NOT the invoker — the invoker still returns the RAW listener error (its Java signature is `Exception invokeXxx(...)`, the caller wraps). So invoker unit tests asserting raw return stay correct; add/rewrite the wrap-contract tests at consumer_utils level.
- Wrapped error: `KafkaError::with_message(Errors::UnknownServerError, msg)` → Display is exactly `msg` (Generic→KafkaGenericError Display = custom_message). That makes `err.to_string() == "User rebalance callback throws an error"` for the exact-message assertion.
- Perf: wrap is `result.map_err(...)` — no-op on Ok, so success path + §31 handshake untouched. Per-rebalance error path only.

**2. Existing helper with non-faithful semantics + zero external callers = safe to rewrite to Java's exact contract.** `maybe_wrap_as_kafka_error_with_msg` previously PREPENDED `"{msg}: {inner}"` for IllegalState/IllegalArgument/Timeout/Wakeup — wrong (Timeout/Wakeup ARE KafkaExceptions, must pass through). grep confirmed only self-references; rewrote + replaced its two unit tests.

**3. De-flake an in-callback-pause() port by removing the records, not adding a guard.** testAutoCommitOnRebalance uses in-callback `pause()` (Issue 8, unsupported in Rust) to stop awaitAssignment's poll loop advancing the seeked position past 300/500 before auto-commit. Fix: DON'T produce the 1000 records (test never consumes them — they only made the seek "meaningful"). No fetchable records ⇒ poll loop can't advance position ⇒ deterministic. seek() sets position without validating against the log, so seek-to-300 on an empty partition is fine.

Issue 3 (callback-reentrancy: &self Arc<dyn Listener> can't call &mut self consumer; inline-invocation holds &mut self; driver-channel deadlocks) is GENUINE structural — documented in PLAN.md "KNOWN API-CAPABILITY GAP" section, no code change. The 8 #[ignore]d tests are correct for the phase.

COMMENTS.39.md is gitignored; COMMENTS.DONE.39.md is NOT — commit the DONE file + PLAN.md only.
