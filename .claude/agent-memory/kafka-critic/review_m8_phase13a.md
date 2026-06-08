---
name: review-m8-phase13a
description: Phase 13a — single-root-cause Issue-1 fix pattern; ctor-time setter wiring of `Arc<CoordinatorRequestManager>` into `CommitRequestManagerInner`; new methods routinely ship without targeted unit tests when integration tests cover them
metadata:
  type: feedback
---

Phase 13a review patterns observed during the `PlaintextConsumerAssignTest` pilot translation + Issue-1 fixup.

**Pattern: "one root cause closes N comments" is sometimes real.**
When the Actor reports that fixing Issue 1 transitively closed Issues 2/3/4, audit the fix against the *Java contract chain*, not against each surface symptom:
- Issue 1 said "OffsetFetch retry loop was a no-op" — root cause.
- Issues 2/3/4 said "commit_sync times out", "commit_sync after poll times out", "poll() returns NotCoordinator fatally" — all are downstream of the SAME missing `mark_coordinator_unknown` wire (response handler had no way to refresh the coordinator before the next retry).
The retry driver was correct in `commit_sync_with_retries`; the missing piece was the trigger to re-discover the coordinator. Lesson: don't reject a "single root cause" claim reflexively — verify by reading the Java contract chain end-to-end.

**Pattern: ctor-time setter wiring for mutually-referencing managers.**
Java constructs `CommitRequestManager(coordinatorRequestManager, ...)` so the dependency is passed in at ctor time. Rust can't replicate that when the two managers reference each other through `Arc`s (chicken-and-egg). The Rust translation builds both managers, then calls `commit_arc.set_coordinator(Arc::clone(coord_arc))` post-hoc.
Audit checklist when reviewing this pattern:
- Is the field `Mutex<Option<Arc<...>>>` (defensive) or just `OnceCell<Arc<...>>` (one-shot)? Mutex<Option> permits multi-set; OnceCell forbids it. Both correct for the single-setter case, but Mutex<Option> signals "intentionally settable >1 time" which may be unintended.
- Is the setter call BEFORE the bg-task spawn? If yes, no race window. If after, a task could observe `None`.
- Does the read path (e.g. `coordinator_node()`) drop the wrapping guard before invoking into the wired target's own locks? Mismatch between `coordinator_node()` (holds 2 guards) and `mark_coordinator_unknown` (clones Arc, drops guard, then calls) is a documentation smell worth flagging as a nit.
- Arc cycle: does the wired-IN target hold a strong reference back to the wiring side? Grep the target's source for the wiring-side type name.

**Pattern: new methods + new fields routinely ship without targeted unit tests.**
The fixup commit added `set_coordinator`, `mark_coordinator_unknown` (commit-side), a real `fetch_offsets_with_retries`, and per-error `mark_coordinator_unknown` calls in two response handlers — all behavior-critical. Zero new unit tests in the `mod tests` block. The Actor relied on the integration suite (8/8) for coverage.
This is acceptable Phase-13a practice but worth flagging as a minor when the Java parallel has dedicated unit tests (Java's `assertExceptionHandling` is parameterized over `NOT_COORDINATOR`/`COORDINATOR_NOT_AVAILABLE`/`REQUEST_TIMED_OUT` with `verify(coordinatorRequestManager).markCoordinatorUnknown(any(), anyLong())`). The Rust side's deferred-tests doc-block (lines 2196-2215) does NOT list coordinator-unknown wiring as deferred, so the absence is unjustified by the file's own rationale.

**Pattern: retry-loop deadline tracking is simulated, not wall-clock.**
Both `commit_sync_with_retries` and the new `fetch_offsets_with_retries` advance `current_time_ms` by `inner.retry_backoff_ms` per attempt instead of re-reading the real clock. Java's `TimedRequestState.isExpired()` uses real `time.milliseconds()`. This means:
- If broker responses are faster than `retry_backoff_ms`, Rust thinks more time has passed than really has → may time out earlier than Java.
- If broker responses are slower, Rust lags reality → may retry longer than Java.
This is a pre-existing pattern not introduced by Issue-1's fix; treat as known divergence. Don't flag when new code mirrors the existing `commit_sync_with_retries` template — the Actor explicitly cites it as the template.

**Pattern: Java `Map.get(k) == null` is ambiguous; Rust `HashMap.get(&k).is_none()` is not.**
Java's `committed(Set.of(tp))` returns a map where missing OR null entries both return `null` from `.get(tp)`. Rust's translation strips `None` values from the inner `HashMap<TopicPartition, Option<OffsetAndMetadata>>` before returning `HashMap<TopicPartition, OffsetAndMetadata>`. Test assertions translated from Java's `assertNull(committedOffset.get(tp))` become `committed_offset.get(&tp).is_none()` — semantically identical for observable behavior, documented at the conversion site (`application_event_processor.rs:868-882`). Not a bug; just be aware when reading test parity.
