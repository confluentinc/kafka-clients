---
name: review-python-ctray-drain
description: Critic 65 review of the Python C-tray drain-before-control-ops fix (_confluentkafka.c on_drained + producer.py _wait_drained); CLEAN. Review heuristics for the drain-waiter machinery.
metadata:
  type: feedback
---

# Python C-tray drain before control ops / flush — Critic 65 (CLEAN, 0 bugs)

Branch `python-drain-tray-before-control-ops`, commits `4d903fc6` (fix) +
`20939a00` (tests). The bug: Python `send()` only appends to a C-side batch
("tray"); a background thread flushes it to Rust every 10 ms / 1000 records, so a
returned-but-un-awaited `send()` could reach Rust AFTER commit/abort/flush. Fix
modelled on the existing `py_Producer_on_space_available` / `space_cbs`
backpressure code: monotonic `accepted`/`handed` counters + `drain_requested`
wake flag + `py_Producer_on_drained(cb)`, and `producer.py` control ops + flush
`_wait_drained()` first. This was a **clean** review — the Actor mirrored the
established twin faithfully.

## The single load-bearing gotcha (checklist item #1)
`handed` MUST advance by the **PRE-compaction** take size. The send loop compacts
`batch_node->count -= errors_found` for immediately-erroring records, but
`accepted++` counted those records too. If `handed` used the post-compaction
count, a waiter with `target == accepted` would have `handed < target` forever →
**hang**. Verify `taken_count` is summed BEFORE the send loop and `handed +=
taken_count` uses it. (In `_confluentkafka.c`: sum ~`:656-659`, compaction
`:703`, advance `:718`.)

## Lost-wakeup / CPU-spin reasoning that makes it correct
- `on_drained` sets `drain_requested` ONLY when `handed < target` (== `accepted`
  snapshot). `handed < accepted` ⟹ un-handed records exist ⟹ tray is non-empty
  (append and `accepted++` are the same critical section). So the
  empty-tray-with-drain_requested case never arises from `on_drained`.
- `drain_requested` cleared at the take (loop top) BEFORE both the paused
  `continue` and the empty-tray `continue`. Clearing at iteration END, or missing
  either continue, is the 100% CPU spin (skip-wait → continue → skip-wait …).
- Lost-wakeup freedom: `on_drained`'s check-and-register and the send task's
  `handed +=` are under the SAME `record_batches_mutex`. Same argument as
  `space_cbs`.
- Fire callbacks only AFTER unlocking the mutex (never call into Python holding
  it). Close-release fires all with `UINT64_MAX`; `thrd_join` orders the
  send-task exit-fire strictly before shutdown's fire (compacting take ⟹ no
  double-fire).
- `on_drained` short-circuits True for closed / paused / `handed>=target` /
  `thrd_equal(thrd_current(), send_thread)` (reentrant control op from an
  immediate-error delivery callback runs on the send thread → self-deadlock).

## How to prove the tests have teeth WITHOUT touching tracked files
Scratch script: `sys.path.insert` the binding dir, then
`P.Producer._wait_drained = lambda self: None` (+ async no-op), re-run the
scenarios, assert the no-fix outcome. Result here: committed → `history_count()
== 0`; abort → record leaks to `1` after a 50 ms sleep; flush → `0`. The tests
assert via `history_count()` (deterministic Rust-side observable), NOT
`fut.done()` — that is what gives them teeth. Also ran a 200-cycle stress
(history == committed exactly, 332/332) + 50-cycle concurrent-sender race (no
hang, perl `alarm` as the macOS `timeout`).

## Calibration / FP-avoidance
- The drain `PyMem_RawRealloc`-into-same-pointer NULL-deref-on-OOM is a faithful
  copy of the pre-existing `space_cbs` code (OOM ⟹ crash OK per CLAUDE.md §10.1).
  Do NOT flag it on the new code in isolation — fix both or neither.
- The dropped "drain waiter released on close" test is **sanctioned by the spec**:
  the paused short-circuit makes a pending-waiter-at-close impossible to create
  deterministically. Correct-but-untested defensive path ≠ a finding.
- `_check_closed()` before `_wait_drained()` (not literally "first") is fine —
  "first" means before the op submission; a closed producer short-circuits
  `on_drained` to True anyway.
- Build/verify recipe (macOS, no cargo — no Rust change): `cc -std=c99 -Wall
  -bundle -undefined dynamic_lookup -I<py3.12 include> -I target/include
  _confluentkafka.c tinycthread.c -Ltarget/release -lconfluent_kafka -o
  _confluentkafka.cpython-312-darwin.so`; `.so` is gitignored. Treat any `-Wall`
  warning as failure (got 0). `venv/bin/python -m pytest test/unit -q`.

## Rules suggestion raised (COMMENTS.65.md S1)
`producer-transactions.md §13` documents only the Rust `send_async` outbox drain;
it should also note the Python binding's independent C-tray drain (control ops +
flush `_wait_drained()` → `Producer_on_drained` before the op), else §13's
anti-pattern list reads as satisfied while a Python control op skipped the tray.
Related: [[review-ffi-txn-drain]], [[review-python-bindings-callbacks]].
