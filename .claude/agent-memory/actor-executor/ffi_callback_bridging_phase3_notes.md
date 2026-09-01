---
name: ffi-callback-bridging-phase3
description: FFI callback-bridging Phase 3 — cbindgen mis-renders Option<fn-alias>, unconditional user_data transfer via CallbackTarget drop, C test user_data type-punning trap
metadata:
  type: project
---

Phase 3 of the FFI/Python callback-bridging plan (`~/.claude/plans/wiggly-bouncing-babbage.md`)
landed as `0923d9c` on `ffi-callback-bridging`:
`kafka_consumer_Consumer_commit_async{,_offsets}_with_callback`.

**Why:** C had no `OffsetCommitCallback` equivalent.

**How to apply** — findings that generalize to Phases 5-6:

1. **cbindgen renders `Option<MyFnPtrAlias>` literally as `Option<...>`** — an
   un-compilable C header, and cbindgen only *warns*. It handles
   `Option<unsafe extern "C" fn(...)>` (literal bare `fn`) correctly, emitting
   `void (*p)(void*)`. So a nullable function-pointer parameter must spell the
   signature out inline; the `_t` typedef still gets exported via the
   `cbindgen.toml` allowlist and is what C callers should name, but it cannot
   appear in the exported signature. Keep the alias "used" somewhere in Rust
   (e.g. on the private adapter-builder's parameter) or `#![deny(warnings)]`
   fails the build with `dead_code`. Phase 5's rebalance-listener
   `user_data_destroy` needs the same treatment.

2. **"ownership transferred unconditionally, destroy hook fires even on error"
   is free if you build the `CallbackTarget`-owning adapter FIRST**, before any
   fallible step. Every early return then drops the `Arc` and `CallbackTarget::Drop`
   fires the hook — no explicit cleanup path, no way to forget one branch. Note
   `sync_void_op` also drops the closure (hence the adapter) when the access
   guard cannot be acquired, so guard rejection is covered by the same mechanism.

3. **`mpsc::Sender<CompletionJob>` IS `Sync`** on the current toolchain (1.97),
   so an adapter holding one can satisfy `OffsetCommitCallback: Send + Sync`
   directly — no `Mutex<Sender<..>>` wrapper needed. (`FfiConsumerHandle` gets
   there via `unsafe impl Sync`; the adapter does not need to.)

4. **C-test trap that cost a debugging round:** reusing one `on_commit` callback
   across tests whose `user_data` is a *different* struct type silently
   type-puns — writing a big `commit_result_t` through a small `malloc`ed
   counter corrupted the heap and produced a bogus "destroy fired twice"
   failure plus garbled Unity output. One callback per `user_data` struct type.

5. Both `commit_async_*_with_callback` are exercised against `MockConsumer`,
   whose core `commit_async_impl` awaits `on_complete` inline — that makes the
   whole `dispatch_and_wait` round trip synchronous within the FFI call, so the
   "call returns only after the callback returned" assertion needs no `wait_for`
   and is fully deterministic (200 ms sleeping callback, flag set last).

Environment notes from Phase 1 (no cmake — compile the Unity suites directly
against `target/release/libconfluent_kafka.a`; no venv; `cargo xtask lint`
skips the `ffi` feature) all still held. One addition: **do not pipe `cc` into
`head`** — SIGPIPE kills the compile and you get "no such file" instead of a
binary. Redirect to a log file and grep out the harmless
`was built for newer 'macOS' version` linker warnings.

See [[ffi-callback-bridging-phase1]], [[ffi-callback-bridging-phase2]].
