# Critic 56 — resolved comments (Python producer async-first transaction API)

Fix applied as a `fixup! 4520ae3f` (the commit that introduced the regression).
producer.py only — no C/Rust change (the C extension was unchanged, so no
rebuild was needed).

Verification: the sanctioned `make devel-build-python` + pytest path cannot run
on this macOS box (setuptools/pip blocker; CI/Linux is the gate). Verified
locally by executing the exact committed `test_producer.py` against the real
mock FFI via the scratchpad pytest shim runner (python3.12): **110 passed, 0
failed** (109 pre-existing + 1 new regression test). The new test was confirmed
to FAIL without the fix (`freed=[]` — the late error handle is dropped, never
freed) and PASS with it.

---

## Finding 1 — Async txn ops leak the KafkaError handle on task cancellation (regression) — RESOLVED

- **Severity**: Behavior-mismatch / resource leak (cancellation-safety regression)
- **File**: `bindings/python/producer.py` (`AsyncProducer._run_async`, the
  `deliver` closure) — on the path of all 5 async txn ops + the shared async
  `flush`/`partitions_for`/`close`.
- **Contract violated**: CLAUDE.md §9.5 (a translated callback obligation must
  still be met — the callee owns and must free the `KafkaError` handle) and the
  removed `_run_blocking`'s documented invariant ("that handle would leak —
  unbounded under a retry/cancel loop").

- **Original description**: `deliver` was `if not fut.done():
  fut.set_result(payload)` with no else branch. When the awaiting task is
  cancelled (e.g. `asyncio.wait_for(commit_transaction(), timeout=T)` then retry
  — the pattern the docstrings invite) the future is already done/cancelled, so a
  late callback carrying a non-null error handle was dropped — reaching neither
  `_resolve_void` nor `_free_void` — and the `Box<KafkaError>` leaked. A
  regression introduced by `4520ae3f`, which routed all 5 async txn ops onto
  `_run_async` and deleted `_run_blocking` (whose `except CancelledError →
  _free_when_done → KafkaError_destroy` guard prevented exactly this).

- **Resolution**: Mirrored the consumer's cancellation-safe sibling
  `AsyncConsumer._deliver` (`bindings/python/consumer.py`). `_run_async`'s inner
  `deliver` now:

      def deliver(payload):
          # Runs on the event loop thread.
          if fut.cancelled() or fut.done():
              free(payload)
              return
          fut.set_result(payload)

  `_run_async` already threads `free` in (`_free_void` for the void txn/flush/
  close ops, `_free_partitions` for `partitions_for`), so this also closes the
  same latent leak on the shared async `flush`/`partitions_for`/`close` paths.
  The `_run_async` docstring was corrected — it now states `free` consumes the
  handles both when the loop is closed AND when the awaiting task was cancelled,
  and that it genuinely mirrors the consumer (previously aspirational). The sync
  `_run_sync` was left untouched (its KeyboardInterrupt gap is pre-existing/
  shared and the Critic explicitly marked it non-blocking / out of scope).

- **Regression test**: `test_async_txn_cancelled_await_frees_late_error_handle`
  (deterministic, mock/fake-FFI). It fakes `Producer_commit_transaction_async`
  to CAPTURE the callback without firing it (an op that outlives the cancel),
  cancels the await, then fires the late callback with a non-null fake
  `KafkaError*` handle and asserts `KafkaError_destroy` freed it. Confirmed to
  FAIL without the fix (`freed=[]`) and PASS with it.
