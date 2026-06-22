---
name: phase13a-issue7-fix-notes
description: Phase 13a (3/N) Issue 7 fix — fetch_collector + abstract_fetch skip transient internal states ("No current assignment", "Missing position") instead of propagating IllegalState during KIP-848 rebalance window
metadata:
  type: project
---

# Phase 13a (3/N) — Issue 7 fix

**Commit:** `b767eee` (fixup! b243380 Phase 13a (2/N) Issue 4)

## Problem

The Phase 13a (2/N) fix to Issue 4 made `poll_for_fetches` propagate ALL errors from `FetchCollector::collect_fetch`. Correct for `OffsetOutOfRange` (real surfaceable error), but over-propagated two transient signals that Java's `collectFetch` silently skips:

1. `IllegalState("Missing position for fetchable partition X")` — `FetchCollector::fetch_records_from_partition` `MissingPosition` arm. A `CompletedFetch` for a just-revoked partition can land in the buffer between the bg task's fetchable snapshot and the collector's pass.
2. `IllegalState("No current assignment for partition X")` — `AbstractFetch::prepare_fetch_requests`. Between `subscriptions.fetchable_partitions(...)` (snapshot under lock) and per-partition `guard.position(&tp)` (fresh lock), a KIP-848 rebalance can unassign the partition.

## Why the Manager's pointer at fetch_collector was incomplete

The Manager's Issue-7 description said "fix in fetch_collector.rs". But the actual error in the failing test (`test_async_consumer_re2j_pattern_expand_subscription`) surfaces from `abstract_fetch.rs:529-540`, not fetch_collector. The Manager identified the symptom correctly ("transient internal state surfaced as fatal") but the source was broader. **Lesson: when a Manager points at one file, grep for the exact error message across the codebase before assuming the fix locus.**

## Fix shape (option-c not viable, picked direct continue)

Considered options (from task spec):
- (a) `is_transient` method on `KafkaError` — would couple consumer-specific transient classification to the common error type. Rejected.
- (b) String-content match on `KafkaError::IllegalState` — fragile, scattered. Rejected.
- (c) Distinguish error types via custom `FetchError` enum at collect_fetch level — only works at one call site; doesn't reach `abstract_fetch`.

**Chosen:** at both call sites, refuse to construct the error in the first place. Skip the partition with `continue` (abstract_fetch) or return empty `FetchPartitionOutcome` (fetch_collector).

This matches Java's `is_fetchable == false` adjacent path semantics, which is what the issue text actually asked for.

## Rust-vs-Java timing window

Java's `prepareFetchRequests` and `FetchCollector` both have the SAME race in theory (synchronized methods are per-method, not per-scope). But Java's classic flow has tighter timing — the bg thread is a single Java thread driving the entire poll loop, so the window between `fetchablePartitions()` and `position()` is microseconds.

In Rust's KIP-848 bg-task model:
- The bg task interleaves Application events between phases.
- A LeaveGroup or rebalance reconcile can fire between the snapshot and the query.
- The window widens to milliseconds, making the race observable on every `unsubscribe + re-subscribe` cycle.

**Lesson:** Java's "synchronized" + single-thread timing constraints don't translate directly to Rust's `Mutex` + async/await. Tighter Rust timing can make rare-in-Java races common-in-Rust. Document deviations in source comments with explicit rationale.

## Side effect on other Issues

Issue 5 (`by_duration:PT1H`) symptom changed: previously surfaced `IllegalState("Missing position...")` immediately; now hangs waiting for a position that never materializes (until test deadline). Both fail the test — but Issue 5's underlying gap (OffsetsRequestManager doesn't compute by_duration positions) is unchanged. Test remains `#[ignore]`-gated.

## Tests preserved (regression check matrix)

Surfaceable errors that MUST still propagate:
- `OffsetOutOfRange` no-reset-policy → `test_fetch_with_offset_out_of_range_no_default_reset` + integration `test_async_consumer_fetch_invalid_offset` (Issue 4 regression check).
- `TopicAuthorizationFailed` → `test_fetch_with_topic_authorization_failed`.
- `CorruptMessage` → `test_fetch_with_corrupt_message`.
- Unexpected error codes (catch-all `IllegalState` in `handle_initialize_errors`) → `test_fetch_with_other_errors`.

All passed after fix.

## Pattern: "skip-at-source vs filter-at-sink"

When a path can produce a transient internal error that the user shouldn't see:
- **Sink filter** (option a/b) — catch & swallow at one centralized point. Easy to write, hard to extend (need to know every transient message).
- **Source skip** (chosen) — prevent the error from being constructed in the first place. The `continue` in the per-partition loop matches the Java code's `else if (!isFetchable(tp))` log-and-continue pattern semantically.

Source skip is preferred when:
- The race is well-localized to a specific snapshot-then-query pattern.
- Java's behavior already has an adjacent log-and-skip arm we can mirror.
- The error message would otherwise need cross-file/cross-module classification.
