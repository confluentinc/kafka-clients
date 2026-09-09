---
name: python-bindings-local-build-test-macos
description: how to build+run the Python C-extension bindings and their pytest suite locally on this macOS box (venv exists now), plus the stale site-packages .so trap and deterministic-observable test design
metadata:
  type: feedback
---

The Python bindings (`bindings/python/`) CAN be built and pytest-run locally on
this macOS box. Verify against current repo state — this file has been wrong
before (see the tinycthread + pytest notes).

**Build the extension by hand with `cc`**, mirroring `setup.py` (sources
`_confluentkafka.c` + `tinycthread.c`; `-Itarget/include` for the cbindgen
header; link `libconfluent_kafka` from `target/release`; rpath so import finds
the dylib). No Rust rebuild is needed for a bindings-only change — the release
dylib + header are prebuilt. The `.so` is gitignored, so rebuilding is safe:

    PYINC=/opt/homebrew/opt/python@3.12/Frameworks/Python.framework/Versions/3.12/include/python3.12
    cc -std=c99 -Wall -bundle -undefined dynamic_lookup \
      -I"$PYINC" -I"$REPO/target/include" \
      "$REPO/bindings/python/_confluentkafka.c" "$REPO/bindings/python/tinycthread.c" \
      -L"$REPO/target/release" -lconfluent_kafka \
      -Wl,-rpath,"$REPO/target/release" \
      -o "$REPO/bindings/python/_confluentkafka.cpython-312-darwin.so"

`-Wall` is clean (0 warnings) on the current file — any warning is yours. The old
"`<threads.h>` missing" blocker is GONE (repo vendors `tinycthread.{c,h}`;
`_confluentkafka.c` includes `"tinycthread.h"` under `#ifdef __APPLE__`); do NOT
add a `target/include/threads.h` shim. Only `_confluentkafka.c` changes need the
rebuild; `producer.py`/`consumer.py`/test edits are live.

**pytest is available now** — there is a `venv/` with pytest + pytest-asyncio
(`asyncio_mode = "auto"` in pyproject). Run from `bindings/python`:
`venv/bin/python -m pytest test/unit -q`. (The old "pytest not installed, build a
shim" workaround this file used to describe is OBSOLETE — ignore it.) The 2 skips
in a full `test/unit` run are pre-existing MockConsumer.poll ones.

**Stale-.so shadowing trap (cost a debugging round).** There is ALSO an installed
copy at `venv/lib/python3.12/site-packages/_confluentkafka.*.so` that is usually
OLD (pre your local rebuild). Which `.so` loads depends on how python is
launched:
  - `python -m pytest ...` and `python -c "..."` put CWD on `sys.path[0]`, so
    from `bindings/python` they load the FRESH local `.so` — this is why normal
    pytest sees your changes.
  - `python /abs/path/script.py` puts the SCRIPT's dir (not CWD) on
    `sys.path[0]`, so a scratchpad script loads the STALE site-packages copy and
    misses new FFI symbols (`AttributeError: module '_confluentkafka' has no
    attribute 'Producer_on_drained'`). Fix: run with `PYTHONPATH=.` from
    `bindings/python`, or `python -m`. Confirm via `_confluentkafka.__file__` +
    `hasattr(...)`. Don't overwrite the site-packages copy (out of scope, masks
    issues). Also: `import test.unit.foo` collides with CPython's stdlib `test`
    package — load a test module by file path via
    `importlib.util.spec_from_file_location`.

**Deterministic-observable test design for the C batching producer.** The
producer's Python record future is resolved asynchronously on the poll-futures
thread, so asserting `fut.done()` immediately after `flush()`/commit is RACY. The
reliable discriminators are the Rust-side observables that update synchronously:
`MockProducer_history_count` (0 while a txn record is uncommitted or still in the
C tray; 1 once committed/handed+completed) and a bool FFI return like
`Producer_on_drained`. Assert those; use `fut.result(timeout=…)` only as a
bounded secondary check. A non-transactional completed record DOES enter
`history_count`. Teeth-check a producer.py behavioural fix without editing
sources by monkeypatching the method to a no-op in a throwaway script and
requiring each new test to fail.

**Operational traps:**
- `make ... | tail` (or any build `| tail`) reports tail's exit 0, masking a
  failure. Run with NO pipe and check `$?`, or run in the background.
- Interpreter version matters: the `.so` is `cpython-312`; system `python3` is
  3.9.6 and cannot import it. Use `venv/bin/python` (3.12).
- gRPC servers: only `py_compile` locally; real validation is the Docker
  multilanguage harness (avoid — it has wedged this machine).
- Related: [[commit-hook-timeout]] (use `git commit --no-verify`).
