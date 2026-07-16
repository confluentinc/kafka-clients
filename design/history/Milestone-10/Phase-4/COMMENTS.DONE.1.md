# Milestone-10 Phase 4 — resolved Critic comments (agent N=1)

Fixup round addressing `COMMENTS.1.md` (Critic verdict was NOT BLOCKING). Each
should-fix / minor / hygiene item is resolved below. Branch:
`milestone9-share-consumer-python`.

## Finding 1 — [should-fix] RESOLVED: refcount lifecycle now has test teeth

The ack-commit register/replace/clear path INCREFs the bridge closure in the
extension and DECREFs it only on clear/replace/close. The prior suite asserted
only the wrapper attribute `self._ack_commit_bridge`, which is independent of
the C refcount, so a leaked or doubled INCREF would have passed.

Added five Python-only regression tests in
`bindings/python/test/unit/test_share_consumer.py` that probe the *actual*
refcount via `sys.getrefcount` and `weakref`:

- `test_ack_commit_callback_incref_is_exactly_one` — a registered bridge reads
  `getrefcount == 3` (the wrapper attribute + the single extension INCREF + the
  temporary argument to `getrefcount`). A missing INCREF reads 2; a double
  INCREF reads 4.
- `test_ack_commit_callback_released_on_clear` — a weakref to the bridge dies
  after `set_acknowledgement_commit_callback(None)`.
- `test_ack_commit_callback_released_on_replace` — the old bridge's weakref dies
  on replace; the new bridge is balanced (`getrefcount == 3`).
- `test_ack_commit_callback_no_leak_over_many_cycles` — 200 set/clear cycles
  leave zero live bridges (a per-cycle leak would keep all 200 alive).
- `test_ack_commit_callback_released_on_close` — a weakref dies after `close()`
  with a callback still registered.

Teeth verified empirically: temporarily double-INCREFing `new_cb` in the C
setter and rebuilding made all five tests fail (getrefcount read 4; the weakref
targets survived). Reverted; suite green again. The removed-INCREF direction is
caught by `..._incref_is_exactly_one` reading 2.

The C trampoline itself (owned-handle destroys, deliberate lack of a per-call
DECREF) can only fire against a live broker — the mock's setter is a no-op — so
it stays covered at the FFI / share-consumer layers. A comment in the test file
records this, so a future reader knows why there is no end-to-end mock-firing
test.

## Finding 2 — [minor] ACCEPTED + documented: INCREF leak on drop-without-close

Left as-is to match the established "must close explicitly" contract: the
sibling `consumer.py` has no `__del__` either, and the Rust handle likewise
leaks when a consumer is dropped without `close()`. A `__del__`/finalizer was
not added — it would diverge from the sibling and still could not release the
INCREF without first clearing the callback. Documented on `ShareConsumer`'s
class docstring and on `set_acknowledgement_commit_callback` that a
callback-bearing consumer must be closed (or used as a context manager) to
release the callback.

## Finding 3 — [minor] RESOLVED: close() no longer orphans the bridge

`close()`'s `finally` now drops `self._ack_commit_bridge` unconditionally after
`_destroy()`, so a rejected pre-close clear (only reachable on a real consumer
under concurrent misuse) never leaves a closed consumer dangling a bridge. The
residual extension INCREF on that rejected path is a bounded one-closure leak
(Rust holds the callable as opaque `user_data` and cannot DECREF a PyObject on
destroy) with no use-after-free — the dispatcher is torn down by `_destroy`, so
the stale callback can never fire. A code comment records this.

## Deviation (c) — RESOLVED: Apache-2.0 header added to `_confluentkafka.c`

The file predated this branch and lacked a header; Phase 4 appended a
substantial share block. Added the standard Apache-2.0 Confluent header
(matching the `.py` files and the C test files) to the top of the file, in a
separate hygiene commit. Other pre-existing unheadered files (e.g.
`producer.py`) were left untouched as out of scope.

## Verification

- `pip install -e .` (via the macOS `threads.h` build shim) rebuilds clean.
- `pytest test/unit` = 67 passed (38 share + 29 producer), including the 5 new
  refcount tests.
- Share suite stress-run 20x: clean — no assertion failure / segfault / abort.
- No `src/` / cbindgen / `build.rs` change → `cargo build --features ffi
  --release` is unchanged and green.
