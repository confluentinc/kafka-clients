---
name: phase7f_review_patterns
description: KafkaProducerTest translation review — exhaustive @Test audit, skip-block discipline, production-fix-test-pinning verification, ordering divergence in close paths
type: feedback
---

Phase 7f review of `KafkaProducerTest.java` translation. The actor
translated 24 + cross-referenced 1 + skipped 56 of 82 Java
`@Test`/`@ParameterizedTest` methods, and added a 2-line production
fix (`metadata.close()` in `KafkaProducer::close_inner`).

## Exhaustive `@Test` audit pattern

When the actor reports "every Java @Test is accounted for", do not
trust the count. Build the canonical list and diff:

1. Build authoritative list of Java tests:
   ```sh
   awk '/@Test|@ParameterizedTest|@RepeatedTest/{flag=1; next}
        flag && /(public )?void [a-z]/ {print NR":"$0; flag=0}' \
        kafka/.../FooTest.java
   ```
   (raw `grep -c '@Test'` will under-count if `@RepeatedTest` /
   `@ParameterizedTest` are used, and over-count if `@Test` appears
   inside a comment.)

2. Extract translated Java-test names from rustdoc citations:
   `grep -oE 'FooTest\.[a-zA-Z]+' rust_translation.rs | sort -u`.

3. Extract skipped Java-test names from the skip-rationale block
   (the actor's discipline of "every entry cites Java line number" is
   what makes this audit trivial).

4. `comm -23 <(sort all.txt) <(sort accounted.txt)` to find missing.

In Phase 7f I found `closeShouldBeIdempotent` was already covered by
a Phase 7e test (`close_is_idempotent`) but wasn't cited in the skip
block. The actor's claim of completeness was off by one. **Always
do the diff.** A "covered indirectly elsewhere" cross-reference
sentinel test is a verified-good pattern for closing the audit gap
(the actor used it for `testInterceptorPartitionSetOnTooLargeRecord`
already).

## Production-fix-test-pinning verification

When the actor surfaces a production bug via testing, verify the
test would actually fail without the fix:

1. Read the test logic.
2. Trace: what does the test do that exercises the fixed path?
3. Compute: pre-fix, what would happen on each step?

For `metadata.close()` in `close_inner`: the test
`test_close_when_waiting_for_metadata_update` spawns `send` which
blocks in `wait_on_metadata.await_update` for `max.block.ms = 60_000`.
Then calls `close_with_timeout(Duration::ZERO)` and expects the
spawned `send` to surface an error within 5s. Pre-fix, the
force-close did `force_close=true`, `accumulator.close()`,
`JoinHandle::abort()` — none of which touch `metadata`'s `Notify`,
so the spawned `send` would still wait the full 60s. The 5s test
bound would fail. **Fix is genuinely needed and genuinely pinned.**

Idempotence check: `metadata.close()` sets a flag and calls
`notify.notify_waiters()`. Both are idempotent. Multiple `close`
calls are safe. Verified.

## Close-path ordering divergence (mild Suggestion)

When the actor adds a "wake everything" call to a graceful-close
path that Java places AFTER the run-loop drains, flag the ordering
divergence. In Java, graceful close = `initiateClose` →
`ioThread.join(timeout)` → on natural termination, run-loop calls
`client.close()` which transitively calls `metadata.close()`. So
Java's graceful close gives a blocked `send` a *chance* to receive
metadata before being aborted. In Rust, calling `metadata.close()`
*before* the `select!` over the JoinHandle aborts the in-flight
`await_update` immediately, even if the run loop would have produced
the metadata response within the deadline.

This is a Suggestion (not Blocking) when:
- The pinning test only exercises the force-close arm (Duration::ZERO).
- The deviation is documented in source comments.
- A Phase-N follow-up will fix it once the underlying chain
  (`DefaultMetadataUpdater::close()`) is translated.

This is Blocking when:
- A test in the same phase exercises the graceful arm and fails.
- The actor claims behavioral parity but the deviation breaks parity.

## Test fidelity bypass via internal accumulator

The 50-record flush test calls `producer.accumulator.append()`
directly instead of `producer.send()`. Java calls `send()`. The
flush semantics are still pinned (the futures resolve after
`flush().await`), but the *integration* of `flush` with the public
`send` surface is not tested by this test.

Bypass justification "to keep partition assignment deterministic" is
defensible but the rustdoc should explicitly tag this as a fidelity
divergence so future readers don't think the public surface is being
tested. Suggestion-level. The hot path is exercised by other
Phase 7d tests; the bypass here is for a specific reason (partition
determinism for the staged response set).

## Skip-block organization (verified-good)

The actor grouped skipped entries by reason:
- Transactional (Phase 9)
- Metrics / telemetry (Milestone-1 stubs)
- Rust ownership eliminates null
- Rust `Duration` non-negative
- Java reflective config-class loading
- Other

Each entry cites the Java line number. This is the exemplar for
audit-trail discipline; reuse the pattern in future phase reviews.

## NOTES.md carry-over discipline

When the actor's commit message and source-comment rustdoc both cite
"Phase N will move this back into X", verify NOTES.md has a
corresponding entry under "Phase 7f — landed" (or whatever phase
section). Source-comment Phase 8 markers are discoverable via grep,
but a Phase 8 reviewer reading NOTES.md top-to-bottom will not see
the obligation. In Phase 7f, the `metadata.close()` Phase 8
carry-over was missing from NOTES.md (Suggestion 2). Always
cross-check commit-message claims vs NOTES.md — they should match.

## Quick verification commands

```sh
# Full Java test list (catches @Test / @ParameterizedTest / @RepeatedTest):
awk '/@Test|@ParameterizedTest|@RepeatedTest/{flag=1; next}
     flag && /(public )?void [a-z]/ {print NR":"$0; flag=0}' \
     kafka/.../FooTest.java | wc -l

# Rust-translated tests (cited in rustdoc):
grep -oE 'FooTest\.[a-zA-Z]+' src/.../foo.rs | sort -u

# Production fix test-pinning: run the test that supposedly pins
# the fix, confirm it passes; then mentally revert the fix and walk
# the test logic to confirm the test would have failed.
cargo test --lib --quiet -- module::tests::pinning_test_name
```
