---
name: review_ffi_txn_drain
description: FFI with_txn_control submission-queue drain (PR#168 reversal) — deadlock-safety + discard-test-teeth heuristics
metadata:
  type: feedback
---

Reviewing the FFI `with_txn_control` drain that makes async-in-transaction supported
(commit `acc8b2bd`, reverses the earlier "unsupported UB" decision). Two reusable
verification techniques, both applicable beyond this commit:

**1. "Lock held across `block_on`/`.await`?" — check the binding's TYPE, not the call.**
`let x = handle.kind.lock().unwrap().runtime().handle().clone();` releases the
`MutexGuard` at the statement's `;` because `x` is an *owned* value (`.clone()`), so
there is no temporary-lifetime-extension keeping the guard alive. A drain/await on the
next line holds no lock. Only if the tail expression is a *reference borrowing* the
guard does the guard live to end-of-scope. This is the crux of the
`deque→TxnManager` / `kind`-lock-before-drain deadlock rules (producer-transactions
§3/§4, consumer §10/§16): the deadlock is real (the submission task needs the same
`kind` lock to hand sends ahead of the drain barrier), and the safe pattern is
clone-under-brief-lock, drop, then drain. `src/ffi/producer.rs` flush path documents
the hazard inline (~:2346); `with_txn_control` mirrors it.

**2. A "discard/abort" test only has teeth if a FOLLOW-ON op would double-count.**
Asserting `history==0` after abort is weak — it holds even if the record was silently
dropped or never handed over. The teeth come from: abort → then begin+send+commit →
assert `history==1`. If abort did NOT clear the uncommitted set (and `begin` doesn't
clear it either), the second commit moves BOTH records to `sent` → `history==2` →
fail. So verify the discard by checking the state a *later* op observes, not just the
immediate count. (MockProducer: in-txn send→`uncommitted_sends`, commit moves→`sent`,
abort clears; `history()`==`sent`.)

**3. FFI unit test that proves a PRODUCTION drain (DoD #12):** build a leaked handle
with a dead submission task (`submit_rx` dropped) and `queued_sends!=0`, call the real
`extern "C"` entry point, assert the drain's specific message ("send-submission task
has stopped"). The separator is that the op-if-reached would return a *different*
message (mock "There is no open transaction." / not-initialized), so the assertion
distinguishes drain-ran from op-ran. Don't accept a fixture that substitutes its own
drain.

Verdict was CLEAN. Draining before `init`/`begin` is benign (record fails on its own
async callback, Java-faithful; bounded by `max.block.ms`, no hang because
`maybe_add_partition` throws synchronously after append).
