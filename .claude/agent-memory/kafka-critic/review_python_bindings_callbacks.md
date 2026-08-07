---
name: review-python-bindings-callbacks
description: Review patterns for the Python C-extension bindings when user callbacks get bridged (FFI callback-bridging Phase 6) — GIL-release audit must cover ALL blocking wrappers, not just the new callback-carrying ones
metadata:
  type: project
---

Reviewing `bindings/python/{_confluentkafka.c,consumer.py,producer.py}` for the
Phase-6 callback bridge (`4c4d7d5` on `ffi-callback-bridging`) produced 3 issues.
The generalizable heuristics:

**1. "New callback → release the GIL" is the wrong audit boundary.** The actor
correctly wrapped every *new* callback-carrying entry point in
`Py_BEGIN_ALLOW_THREADS` (both `commit_async` variants, `MockConsumer_rebalance`,
all 17 blocking `ConsumerHandle_*`) and missed the two **pre-existing** ones that
bridging a listener newly turned into deadlock sites: `py_Consumer_seek` /
`py_Consumer_seek_with_metadata`. The correct boundary is *every* synchronous
wrapper whose Rust call can reach `process_background_events*` — i.e. anything
going through `submit_and_drain` / `process_background_events_until`, since §31
invokes the listener there. Only `submit_and_drain_for_close` passes
`skip_rebalance_callback = true`. Audit recipe: grep the binding for sync FFI
calls with no `ALLOW_THREADS`, then follow each into `src/ffi/` → `sync_void_op`
→ the core method, and check for `submit_and_drain`.

Corollary for async bindings: releasing the GIL is necessary but not sufficient.
A *sync* Python method on an async consumer (`_ConsumerBase.seek`) also occupies
the loop thread, so a coroutine listener driven by
`run_coroutine_threadsafe(...).result()` still deadlocks. The fix is to route the
op through the `_async` FFI + `_run_sync`/`_run_async` spec machinery like every
other blocking op — check whether a `*_async` FFI twin already exists in
`target/include/confluent_kafka.h` before proposing a wrapper-only fix.

**2. Coroutine-awareness asymmetry between sibling adapters.** `_ListenerAdapter`
handles `inspect.isawaitable(result)`; `_CommitCallbackAdapter` does not, so an
`async def` commit callback is created, never awaited, and silently dropped (only
an invisible unawaited-coroutine RuntimeWarning). When one adapter in a family
supports coroutines and the docstrings advertise it, check every sibling — a
silently-dropped user callback violates CLAUDE.md §9.5.

**3. `PyUnicode_AsUTF8` return value unchecked = "returns a result with an
exception set".** `offsets_to_arrays` treats `NULL` as "no metadata" and still
performs the commit, then returns a `PyLong` with a live `TypeError` → a
`SystemError` blamed on an unrelated later call. Grep any refactored marshaling
helper for unchecked `PyUnicode_AsUTF8` / `PyBytes_AsString`. A refactor that
*consolidates* a pre-existing defect into a shared helper is worth reporting:
it's now on 4 code paths instead of 1, and one fix closes all of them.

**Verified-clean patterns worth NOT re-flagging:**

- `PyGILState_Ensure()` from a thread that has an active same-thread
  `Py_BEGIN_ALLOW_THREADS` is the documented-safe re-entrant case
  (`PyEval_RestoreThread` on the saved tstate) — not a bug.
- `kafka_consumer_Consumer_handle` takes **no** access guard
  (`src/ffi/consumer_handle.rs`), so `consumer.handle()` **does** work from
  inside a callback; its `NULL` branch is dead code, not a defect.
- The C `user_data_destroy` hook fires even when the FFI call returns an error
  (adapter is built before anything fallible), so a guard-rejected
  `commit_async(callback=...)` does not leak the Python ref. Probe with
  `weakref` + `gc.collect()` rather than reasoning.
- Producer `on_delivery`: `_completion_to_python` converting both handles into
  self-freeing Python objects *before* branching is correct — the removed
  per-branch `*_destroy` calls are covered by `RecordMetadata.__del__`.

**Environment (this host, macOS arm64):** the suite IS runnable. `python3 -m venv`
+ `pip install --index-url https://pypi.org/simple pytest pytest-asyncio
setuptools`; write a ~90-line pthreads `threads.h` shim (needs `thrd_/mtx_/cnd_`
**and `thrd_sleep`**; do NOT shim `timespec_get`), then
`CFLAGS=-I<shim> CONFLUENT_KAFKA_LIB_DIR=<root>/target/release venv/bin/python
setup.py build_ext --inplace`. Run with
`PYTHONPATH=. DYLD_LIBRARY_PATH=<root>/target/release venv/bin/python -m pytest test/unit -q`
(the `DYLD_LIBRARY_PATH` is needed for plain scripts; the dylib's install name
points at a non-existent `target/release/deps/`). Baseline at this commit:
119 passed / 2 skipped. `cc -std=c99 -Wall -Wextra -fsyntax-only` on the
extension yields exactly 4 pre-existing `missing-field-initializers` warnings —
a fifth is new.

See [[review-m8-phase41-listener]] if written, and
`.claude/rules/consumer-threading.md` §31.
