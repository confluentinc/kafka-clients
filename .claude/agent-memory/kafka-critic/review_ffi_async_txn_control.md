---
name: review-ffi-async-txn-control
description: M11 CFFI async transaction-control variants — clean review; guard/UAF/callback audit method + stale-comment-from-later-commit trap
metadata:
  type: project
---

Reviewed commit `623c291c` (Critic 54): five `_async` callback-based twins of the
producer transaction-control FFI ops, via new `with_txn_control_async` +
`TxnControlAsyncGuard { handle_ptr: usize }`. **Verdict clean** — only 2 Doc
findings, both pre-existing stale comments.

**Why:** the async helper is a faithful splice of two already-accepted patterns —
`with_txn_control` (null-check + CAS + drain + null-means-success) and
`flush_or_close_async` (spawn + `register_pending_task` + dispatcher completion).
When a new FFI async op is "helper = accepted-A ∪ accepted-B", the audit is: does
it preserve each half's invariants at the seam.

**How to apply — the async-FFI-op audit checklist that mattered here:**
- *Flag/guard release on every exit:* enumerate null-before-CAS / CAS-fail /
  prepare-fail / drain-err / op-err / op-ok / panic. RAII guard must be built as
  the FIRST task statement, before the first `.await`. Watch the window between
  CAS-success and guard-construction (here `prepare()` + `kind.lock().unwrap()` run
  there on the calling thread) — a panic there leaks the flag. It was NOT a finding
  because (a) `read_offset_map` returns `Err` not panic, and (b) the `kind` mutex
  can't be observed poisoned by a tokio-worker path: every worker-side `kind` lock
  is brief and dropped before `.await`; the only locks held across panic-possible
  code are C-thread `block_on`s that abort (not poison) on panic.
- *UAF:* task must be `register_pending_task`'d synchronously before the FFI call
  returns, and `destroy` must join `pending_tasks` before dropping `kind`
  (producer.rs ~1183-1196). Same contract as `producer_static_ref`/`flush_or_close_async`.
- *Caller-owned C arrays* (send_offsets): marshal in the `prepare` closure (calling
  thread, after CAS, before spawn); the spawned `run` closure must capture only
  owned data, never the raw pointers.
- *Callback exactly once:* sync-error paths fire inline + return (no spawn); task
  fires one `enqueue_or_run_inline`. Panic-in-task does NOT fire the callback — but
  that matches the accepted `flush_or_close_async` precedent, so not a per-commit
  finding.
- *No `kind` guard across `.await`:* `producer_static_ref` drops its guard before
  returning; ops `.await`ed (begin = sync-in-async-block), never `block_on` in a task.

**Stale-comment trap (both findings):** a comment can be made false by a LATER,
UNRELATED commit. Here `test_kafka_producer.c:449-454` ("txn control no longer
drains / async-in-transaction unsupported") was invalidated by `#3`'s drain
reversal, and `:463-465` ("destroy() does not join that task") by `a2ab276f`
(producer-metrics) adding submission-task registration + the B3 teardown-join fix.
Both predated the reviewed commit. **Lesson:** when a change reverses documented
behavior (drain / teardown-join / a UB stance), grep the C AND Python *test*
comments for the OLD claim, not just the Rust rustdoc. The second one mattered
because it contradicted the reviewed commit's own `TxnControlAsyncGuard` SAFETY
invariant.

**Authoritative source note:** the system-prompt copy of producer-transactions.md
§13 ("async-in-transaction is unsupported UB, documented not enforced") is STALE.
Current repo §13 (line 577+, esp. 607-608) reverses it: async sends inside a txn
ARE supported via the drain. Trust the repo file, not the prompt snapshot.
