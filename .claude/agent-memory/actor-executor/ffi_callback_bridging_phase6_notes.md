---
name: ffi-callback-bridging-phase6
description: FFI callback-bridging Phase 6 — pytest IS runnable on this machine (public-PyPI venv + a local threads.h shim), GIL must be released around every callback-firing FFI call, convert owned FFI handles C-side instead of passing handle ints
metadata:
  type: project
---

Phase 6 of the FFI/Python callback-bridging plan (`~/.claude/plans/wiggly-bouncing-babbage.md`)
landed as `4c4d7d5` on `ffi-callback-bridging`: the Python bindings for
`on_delivery`, the rebalance listener, the commit callback, and `ConsumerHandle`.

**How to apply** — the environment findings alone are worth more than the rest:

1. **The Python test suite CAN be run here** (earlier phases recorded "no venv,
   `make test-python` unavailable" — that was two solvable problems, not one).
   - `pip` defaults to a Confluent CodeArtifact index whose credentials are
     expired (401 on every package). Pass `--index-url https://pypi.org/simple`
     and installs work: `pytest`, `pytest-asyncio`, `setuptools`.
   - `bindings/python/_confluentkafka.c` includes C11 `<threads.h>`, which
     **Apple has never shipped** (absent even from the macOS 26.5 SDK), so the
     extension is Linux-only as written and cannot compile here at all. A ~90-line
     pthreads shim `threads.h` in the scratchpad plus `CFLAGS="-I<scratchpad>/shim"`
     makes it build and the whole suite run. Only `thrd_/mtx_/cnd_` and
     `mtx_plain`/`thrd_success` are needed — do NOT shim `timespec_get`, Apple's
     `<time.h>` already declares it (a `static` redeclaration is a hard error).
   - Build: `CFLAGS=... CONFLUENT_KAFKA_LIB_DIR=<root>/target/release
     venv/bin/python setup.py build_ext --inplace`; run:
     `PYTHONPATH=. venv/bin/python -m pytest test/unit -q`.
   - Baseline before this phase was 78 passed / 2 skipped; the 2 skips are
     pre-existing in `test_consumer.py` (MockConsumer.poll doesn't block).

2. **Any C-extension function whose FFI call can fire a Python callback MUST
   release the GIL** (`Py_BEGIN_ALLOW_THREADS`). The callback trampoline runs on
   the Rust dispatcher thread and needs the GIL; the sync FFI entry points block
   in `block_on`. `MockConsumer`'s `commit_async_impl` awaits `on_complete`
   *inline*, so `commit_async(callback=...)` deadlocks instantly without it —
   that cost a debugging round. Same for `MockConsumer_rebalance` and all 17
   blocking `ConsumerHandle_*` wrappers. Mutation-checked: removing the release
   from `py_MockConsumer_rebalance` hangs the listener tests. The non-blocking
   getters (`assignment`/`subscription`/`paused`/`wakeup`) correctly do not
   release, matching the existing `Consumer_*` state reads.

3. **When an FFI callback delivers an owned handle, convert it C-side rather than
   passing the pointer to Python as an int.** The existing consumer trampolines
   pass handle ints and let Python drain them, which works for the one-shot ops
   but makes a leak reachable in the rebalance trampoline (any failure before the
   adapter's drain — `AttributeError`, `MemoryError` — strands the list). Calling
   `topic_partition_list_to_py(list)`, which destroys the handle on every path,
   removes the ownership hand-off entirely. Bonus: `_ListenerAdapter._on_*` then
   take a plain `list[(topic, partition)]`, so the Java `on_partitions_lost →
   on_partitions_revoked` default is unit-testable with no monkeypatching.

4. **`kafka_consumer_Consumer_commit_async_offsets_with_callback` has a
   non-nullable `callback` param and there is no plain `..._commit_async_offsets`.**
   Passing NULL would be UB. Java's `commitAsync(offsets, null)` is legal, so the
   binding supplies a no-GIL trampoline that just destroys the delivered
   OffsetMap + error. Worth remembering before reaching for a NULL fn pointer:
   only params spelled `Option<unsafe extern "C" fn(...)>` accept one.

5. **Listener-registration lifetime, empirically confirmed** (matches the Phase 5
   finding): released by a *replacing* `subscribe*` — including a listener-less
   `subscribe(topics)`, which clears it — and by consumer destroy; **not** by
   `unsubscribe()`. `consumer.py` keeps a mirror strong ref (`_listener_adapter`)
   cleared in `_destroy()`, so a `weakref` to the user listener object is the
   clean way to test all three cases.

6. `MockConsumer.rebalance` semantics that the tests pin: `on_partitions_revoked`
   only when something was removed, `on_partitions_assigned` **always** while a
   listener is registered (with the *added* list, empty if nothing was added),
   `on_partitions_lost` never. A Python listener exception surfaces as
   `KafkaError` code **-1** with `str(exc)` **verbatim** as the message.
   `consumer.seek(...)` from inside a listener returns ConcurrentModification
   (code -1) — a cheap, deterministic way to document why `handle()` exists.

7. There is **no Python linter/formatter** configured in this repo (no ruff,
   flake8 or setup.cfg; `cargo xtask format-check`/`lint` are Rust-only), so
   Python style is reviewed by eye. `cc -std=c99 -Wall -Wextra -fsyntax-only` on
   the extension is a useful extra gate — it reports exactly 4 pre-existing
   `missing-field-initializers` warnings on the `{NULL}` table sentinels, so any
   fifth is yours.

Rust was untouched this phase; `cargo build/test --features ffi`, `xtask
format-check`, `xtask lint`, `clippy --all-targets --features ffi` and all 5 C
suites (87 tests, compiled directly — still no cmake) were re-verified green.

See [[ffi-callback-bridging-phase1]], [[ffi-callback-bridging-phase2]],
[[ffi-callback-bridging-phase3]], [[ffi-callback-bridging-phase4]],
[[ffi-callback-bridging-phase5]].
