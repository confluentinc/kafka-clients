---
name: review-m8-phase7a-fix-cycle
description: Patterns and verification cues from the Phase-7a follow-up review where 16 findings were closed in 8 fixup commits
metadata:
  type: feedback
---

When verifying a §27 zero-copy fix on the receive path, three signals
ranked highest in this review:

1. **Constructor-only `Arc::from`**. `grep "Arc::from" <file>` should
   list only the constructor — any per-record `Arc::from(&str)` is a
   regression. `Arc::clone(&self.topic_arc)` on the hot path is the
   correct shape (atomic refcount bump).

2. **Borrow-then-decode-then-mutate** pattern. The fix to per-record
   `DefaultRecord::clone()` requires the decode block to be scoped so
   `peek_current_record()`'s borrow ends before `cursor.record_index`
   is mutated. Watch for the explicit `{ let (record, batch_meta) =
   ...; ... }` block scope.

3. **`RecordHeaders::from_slice(record.headers())` is OK**. §27
   explicitly allows headers ownership in Milestone-8. Do not flag this
   as a §27 violation — it is the documented tradeoff (per CLAUDE.md
   §13 and consumer-threading.md §27 "Headers handling — owned for
   Milestone-8").

**Why:** Phase 7a's CompletedFetch fix collapsed 4 distinct per-record
allocations (Arc::from + DefaultRecord clone + headers clone +
deserializer alloc) into 1 atomic bump + 1 doc-allowed clone + 1 user
alloc. Future per-record reviews need the same check ordering.

**How to apply:** For any "§27 zero-copy fix" claim, run those three
greps on the changed file, then walk the per-record loop once. If
those three pass, the §27 contract holds.

---

On the `tokio::sync::Notify` race-free pattern: the shape used here is
`tokio::pin!(notified); notified.as_mut().enable(); /* flag check */;
tokio::time::timeout(timeout, notified).await`. Verify the regression
test uses `current_thread` runtime and `yield_now().await` to
deterministically place the awaiter at its `await` point.

**CORRECTION (Milestone-11 Phase-1 review):** the claim that `enable()`
is *mandatory* was wrong for `notify_waiters()`. `Notify::notified()`
captures `notify_waiters_calls` at construction and `poll_notified`
compares it, so a broadcast between construction and first poll is NOT
lost. `enable()` only matters for `notify_one()` permit ordering. Do not
flag a missing `enable()` on a `notify_waiters()`-based wait — see
[[review-notify-waiters-race]].

---

On the `ControlRecordType`-deferred fail-loud strategy: returning
`unsupported_version` for a READ_COMMITTED control batch from a
previously-aborted producer ID is a documented Phase-7a divergence
from Java. Java would either remove the producer ID (ABORT marker) or
skip the batch (COMMIT marker). Rust errors out for BOTH cases. The
practical trigger is broader than the resolution-doc's "rare" framing
suggests — note this when the same code is touched in Phase 7b/c.
