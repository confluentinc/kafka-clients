---
name: python-bindings-macos-build
description: How to build+test bindings/python on macOS — the C11 threads.h gap and the venv/shim recipe
metadata:
  type: reference
---

Building the Python C extension (`bindings/python/_confluentkafka.c`) on macOS
hits a genuine SDK gap and needs a workaround; on Linux CI it just works.

**The gap:** the producer half of `_confluentkafka.c` `#include <threads.h>`
(C11 threads: `thrd_*`/`mtx_*`/`cnd_*` for its background batching threads). The
macOS Command Line Tools SDK does NOT ship C11 `<threads.h>` (only libxml's
unrelated one). So a plain `make build` fails with `'threads.h' file not found`.
This is pre-existing producer machinery, unrelated to the consumer/share code
(which uses no threads).

**Workaround that lets the real code build + the real tests run** (not a fake):
drop a C11-threads-over-pthreads shim `threads.h` in a scratch dir and put it on
the include path via `CFLAGS="-I<shimdir>"`. Map thrd/mtx/cnd onto pthreads;
`thrd_create` needs a heap trampoline (C11 `int(*)(void*)` vs pthread
`void*(*)(void*)`). Do NOT redefine `timespec_get` — both macOS `<time.h>` and
glibc already declare it. Watch for `*/` inside the shim's own comments.

**Recipe (this env has no system setuptools/pytest):**
1. `cargo build --features ffi --release` first (produces
   `target/release/libconfluent_kafka.dylib` + regenerates `target/include/confluent_kafka.h`).
2. `python3 -m venv <venv>` then `pip install setuptools wheel pytest` into it.
3. `CONFLUENT_KAFKA_LIB_DIR=<root>/target/release CFLAGS="-I<shimdir>" <venv>/bin/pip install -e bindings/python`.
4. `<venv>/bin/python -m pytest bindings/python/test/unit`.

The literal `make build`/`make test` assume system python + a real `<threads.h>`,
so they only run clean on Linux CI. Report this as the exact verification gap
when working on macOS (the M9 multilanguage-harness note set the precedent that
some multilanguage checks aren't run in-session).
