---
name: phase13a-issue6-fix-notes
description: Phase 13a (6/N) Issue 6 fix — root cause was OffsetAndTimestamp's timestamp>=0 validation rejecting broker's -1 sentinel for LATEST/EARLIEST, not the metadata-refresh hypothesis filed; pattern is Java's public/internal type split
metadata:
  type: feedback
---

# Phase 13a (6/N) Issue 6 fix — root cause audit pattern

**Rule:** When an Issue is filed as "production code path X is missing
behaviour Y", audit the upstream `None`-source (where the placeholder
value enters the data flow) BEFORE assuming the downstream filter is
the bug. The downstream filter is often a Java-faithful translation;
the bug is upstream — the value was supposed to be `Some(...)` but
the constructor rejected it.

**Why:** Issue 6 was filed as "`OffsetsRequestManager` doesn't refresh
metadata for KIP-848 server-side assignment push, so `end_offsets(tp)`
silently drops the partition". The hypothesis was wrong:

- `RUST_LOG=debug` tracing showed:
  - metadata was populated (`cluster.leader_for(tp) = Some(Node)`)
  - the ListOffsets request was sent
  - the broker responded with the high watermark
  - `apply_partial_result` finalized with `expected_responses → 0`
  - the oneshot waiter received the result
- The bug was at the very last step:
  `OffsetFetcherUtils::build_offsets_for_times_result` constructed
  `OffsetAndTimestamp::with_leader_epoch(offset, timestamp=-1, epoch)`.
  Public-class validation rejects `timestamp < 0`. `.ok()` → `None`.
- Java sidesteps this with `OffsetAndTimestampInternal` — a
  package-private companion type that allows negative offsets/
  timestamps. The bg-task event payload uses this type; conversion
  to the public class happens only at the `offsetsForTimes` boundary.

**How to apply:**

1. Read the filed Issue's reproducer and hypothesis carefully — but
   don't trust the hypothesis. The filer was working from the
   symptom outward; you have the time to work inward.
2. Add `eprintln!` debug at three points:
   - Where the request goes out (confirm metadata, leader, request shape).
   - Where the response is unmarshalled (confirm the broker data).
   - Where the result is built (confirm the constructor call result).
3. If all three points succeed but the user-visible result is wrong,
   the bug is in the result-building step — usually a Java
   public/internal type split that wasn't replicated.

**Pattern (reusable):** Java's `OffsetAndTimestamp` (public, validated)
vs `OffsetAndTimestampInternal` (package-private, loosely validated)
is the canonical Java idiom for "same fields, different validation
rules depending on caller". In Rust this translates to a
`pub(crate)` companion type with a fallible `build_*` conversion.
Other places this idiom likely appears:

- Anywhere Java has a `*Internal` class in `internals/`.
- Wire protocol decoding paths where the broker may return sentinel
  values (`-1`, `MIN_VALUE`) that the public-API constructor would
  reject.

**Files modified:**

- New `src/consumer/internals/offset_and_timestamp_internal.rs`.
- `src/consumer/internals/{mod.rs, offset_fetcher_utils.rs, offsets_request_manager.rs}`.
- `src/consumer/internals/events/{application_event.rs, application_event_processor.rs}`.
- `src/consumer/async_kafka_consumer.rs` (`beginning_or_end_offsets`,
  `offsets_for_times_timeout`, plus two unit-test drainers).
- `tests/integration/plaintext_consumer_subscription_test.rs`
  (un-ignored both tests; module-level + per-test + helper rustdoc
  rewritten).

**Verification:**

- `cargo test --lib` — 1704/1704.
- `cargo test --features integration-tests --test integration plaintext_consumer_subscription -- --test-threads=1` — 11/11 (was 9/11 ignored).
- `cargo xtask format-check` + `cargo xtask lint` clean.
