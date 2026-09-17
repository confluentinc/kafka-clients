---
name: ffi-callback-bridging-phase1
description: FFI callback-bridging Phase 1 — merge-break gotchas on ffi-callback-bridging branch, deny(warnings) vs new pub(crate) primitives, no-cmake C test workaround
metadata:
  type: project
---

Phase 1 of the FFI/Python callback-bridging plan (`~/.claude/plans/wiggly-bouncing-babbage.md`)
landed as `cfb795c` on `ffi-callback-bridging`.

**Why:** C/Python bindings had no way to register a rebalance listener or commit
callback, and no way to construct a `KafkaError` handle to return from one.

**How to apply:** three non-obvious things bit during this phase and will bite the
later phases (2-7) too.

1. **The branch base (aa65232, master merged into consumer-metrics-and-perf) did
   not build.** Two independent semantic merge conflicts that git resolved
   cleanly but broke compilation:
   - `src/ffi/consumer.rs` imported `crate::consumer::WakeupHandle`, which commit
     `644f049` ("Phase 41a: ConsumerHandle ... replacing WakeupHandle") deleted.
     `cargo build --features ffi` failed. Fix: store the core `ConsumerHandle`
     (obtained via the `Consumer::handle()` trait method, available on both
     `AsyncKafkaConsumer` and `MockConsumer`) and call `.wakeup()` on it.
   - a test-only `FetchCollector::new` call in `fetch_collector.rs` was missing
     the Phase-M3 `FetchMetricsManager` arg; `cargo test` failed to compile.
   Lesson: **run the full verification set at the start of a phase**, not only
   at the end — otherwise a pre-existing break looks like your own regression.

2. **`#![deny(warnings)]` in `src/lib.rs` makes `dead_code` a hard error.** A new
   `pub(crate)` primitive that only `#[cfg(test)]` code uses fails the non-test
   `cargo build`. Precedent in the repo is an `#[allow(dead_code)]` with a
   comment naming the future consumer (see `FfiConsumerHandle::is_mock`). Both
   `CallbackTarget` and `dispatch_and_wait` carry that allow until Phases 3/5
   construct them — remove the allows then.

3. **`cmake` is not installed on this machine**, so `make test-c` / `make verify`
   cannot run. The C tests can still be built and run directly, which is enough
   to prove an FFI change did not break them:
   `cc -std=c11 -I target/include -I bindings/c/tests/unity/src -o OUT \
      bindings/c/tests/test_X.c bindings/c/tests/unity/src/unity.c \
      target/release/libconfluent_kafka.a -lpthread -ldl -lm \
      -framework Security -framework CoreFoundation`
   (needs `cargo build --release --features ffi` first, i.e. what `make build-c`
   does before invoking cmake). There is also no `venv`, so `make test-python`
   is unavailable without a network install.

Also note `cargo xtask lint` runs clippy **without** `--features ffi`, so it
never sees `src/ffi/`. Run `cargo clippy --all-targets --features ffi -- -D
warnings` separately for any FFI change.
