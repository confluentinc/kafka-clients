---
name: review_python_run_async_cancellation
description: Python bindings _run_async cancellation-free divergence (producer vs consumer); deleted helper drops a behavioral guarantee references-grep misses
metadata:
  type: feedback
---

When a Python-binding commit deletes a helper "for consistency" and routes callers
onto an existing shared one, audit the **behavioral delta**, not just dangling
references. A `grep` for the removed name passing is NOT evidence the removal is safe.

**Concrete case (Critic 56, commits e21f445f/4520ae3f, producer async-first txn API):**
- `AsyncProducer._run_async` (`bindings/python/producer.py`) claims in its docstring
  to "mirror the async Consumer's `_run_async`" but does NOT. Its inner `deliver`
  closure does `if not fut.done(): fut.set_result(payload)` with **no else** — on a
  cancelled/done future it drops the payload without freeing.
- The consumer's `_deliver` (`bindings/python/consumer.py`, static method) IS
  cancellation-safe: `if fut.cancelled() or fut.done(): free(payload); return`.
- Result: a `KafkaError` C handle delivered to an already-cancelled async op (e.g.
  `asyncio.wait_for(commit_transaction(), timeout=T)` that later fails) reaches
  neither `_resolve_void` nor `_free_void` → leaked `Box<KafkaError>`. Unbounded
  under a retry-on-timeout loop (the documented "timeout is safe to retry" pattern).
- The deleted `_run_blocking` had an explicit `except CancelledError: ... _free_when_done
  → KafkaError_destroy` guard whose entire docstring was about preventing this exact
  leak. So the "consistency" refactor was a real regression.

**How to apply / repro heuristic:**
- Free-of-C-handle in these bindings happens ONLY via `KafkaError._from_c` (raises+frees)
  or `_lib.KafkaError_destroy`. Monkeypatch BOTH and count calls to prove a leak.
- Leak triggers on: task cancelled + loop still open when late callback fires +
  **non-null** error handle. Success (handle 0) never leaks (nothing to free).
- Whenever two sibling classes carry same-named helpers (`_run_async`/`_deliver`/
  `_run_sync`), diff them line-for-line before trusting a "mirrors X" comment.

**Verified-clean baseline for the C-ext async-op refcount pattern (reusable):**
each `py_Producer_*_async` binding does exactly ONE `Py_INCREF(cb)` immediately
before the FFI call (send_offsets: only AFTER `offsets_to_arrays` succeeds), the
Rust `*_async` fires the callback exactly once on every path
(`with_txn_control_async`: null/CAS-reject/prepare-err/spawned), and
`producer_op_trampoline` does the sole `Py_DECREF(cb)`. That balance is correct —
the risk is never the refcount, it's the free-on-cancel gap above. See
[[review_python_txn_bindings]] for the CPython marshaling FP traps.
