---
name: critic2-python-bindings-round
description: Critic-2 Python-bindings fix round — route a binding's blocking op through the async FFI twin instead of releasing the GIL, cache the asyncio loop for sync methods, and why MockConsumer can never reproduce a block_on deadlock
metadata:
  type: project
---

Critic 2's review of the Phase-6 Python bindings produced 3 issues; all fixed in
`9957c2b` + `c824fc6` (fixup! `4c4d7d5`) on `ffi-callback-bridging`. Four things
generalize.

**1. For a binding wrapper, "release the GIL" is the weaker fix — route through
the async FFI twin.** `Consumer.seek` held the GIL across a sync FFI call that
`block_on`s `AsyncKafkaConsumer::seek`, which drains background events and can
invoke the rebalance listener, whose trampoline needs the GIL on the dispatcher
thread → hard interpreter deadlock. `Py_BEGIN_ALLOW_THREADS` fixes that, but only
half the bug: on the **asyncio** consumer the same sync call also occupies the
event loop that a coroutine listener must be scheduled onto, so a coroutine
listener still deadlocks. Both halves have one cure — use the `*_async` FFI entry
point plus the binding's existing `_run_sync` / `_run_async` machinery, which
parks on a `threading.Event` / awaits on the loop and holds neither. Consequence:
the method becomes per-class (plain on the sync class, a coroutine on the async
one) rather than one shared sync method, even when **Java's** method is
non-blocking — what matters is whether the *Rust* translation awaits. Missing
`_async` twins are cheap to add (mirror the sync sibling; on a marshaling failure
fire the callback inline with the error, like
`kafka_consumer_Consumer_commit_sync_offsets_async`). Prefer **deleting** the sync
Python wrapper over leaving it exported and unused — otherwise the deadlock stays
reachable.
Audit rule that found a second instance: the list is "every wrapper whose FFI call
blocks", not "every wrapper whose own result is a callback". `py_Producer_flush` /
`py_Producer_partitions_for` were the same trap, latent only because `producer.py`
uses their async twins.

**2. An asyncio wrapper class must CACHE its event loop, not call
`get_running_loop()` on demand.** A method that is sync in the binding (Java's
`commitAsync` doesn't block, so it stays a plain method) may legitimately be
called from a worker thread — and with a coroutine callback it *must* be, whenever
the completion is delivered inline (`AsyncMockConsumer` awaits it inside the call,
so blocking the loop there deadlocks). `get_running_loop()` raises off-loop, so the
correct usage would have failed. Fix: record the loop in `_run_async` and in the
loop accessor, fall back to the cached value. Same reason `_rebalance_off_loop`
exists in the test suite — that pattern is a *requirement*, not test convenience.

**3. `MockConsumer` cannot reproduce any `block_on` deadlock**, because its access
guard rejects a concurrent op *before* the op can block. So a mock-based test of
such a deadlock is not a discriminator — the pre-fix code passes it too. The
honest pairing is a **structural** test (the deadlock-capable `_lib` entry points
are absent, the async ones present, the method is a coroutine on the async class
and not on the shared base) plus a *labelled* liveness test. The structural one
mutation-checks cleanly: restoring the shared sync method fails it.

**4. A C helper that returns success with a live `PyErr` set surfaces as a
`SystemError` attributed to an unrelated later call.** `offsets_to_arrays` ignored
`PyUnicode_AsUTF8`'s NULL, so a non-`str` metadata became "no metadata": the
commit went through with the value silently dropped *and* the next unrelated C
call raised `SystemError`. Test shape that pins both halves: assert the TypeError,
assert nothing was committed, **and** assert a following well-formed call still
works (that last assertion is what catches the leftover indicator). Type-check
with `PyUnicode_Check` before converting so the message names the offending type,
and let a real `str` conversion failure keep its own exception.

Also: a callback adapter built at the *call site* can reject an `async def` up
front (`iscoroutinefunction`) so the caller sees it; one built earlier (a
`subscribe`-time listener adapter) can only fail when invoked. Those two being
different is correct, not an inconsistency — but a callable that merely *returns*
an awaitable is invisible to `iscoroutinefunction` either way, so handle it at
invoke time too (log it; Java's `void onComplete` has nowhere to report).

**Environment deltas** (the [[ffi-callback-bridging-phase6]] recipe still holds):
adding an FFI export means the extension must be relinked against a fresh
`cargo build --release --features ffi` (~1m20s) or `dlopen` fails with
`symbol not found in flat namespace`. Unity sources are at
`bindings/c/tests/unity/src/unity.c` (not `unity/unity.c`) — 5 suites, 90 tests
now. The `cc -Wall -Wextra -fsyntax-only` gate emits **116** warnings on the
extension, all pre-existing `unused-parameter` / `missing-field-initializers`
boilerplate; compare counts against `git show HEAD:...` rather than expecting a
small absolute number. Docker multilanguage runs can flake with
`failed to bind host port ...: address already in use` when broker containers
start in parallel — re-run the single test to confirm it is the flake and not a
regression.

See [[ffi-callback-bridging-phase6]], [[ffi-callback-bridging-phase7]],
[[critic1-ffi-contract-round]], [[multilanguage-suite-on-macos]].
