# Milestone 10 Phase 3 — Critic COMMENTS.1 (resolved / adjudicated)

Reviewed by Critic N=1 (commits `029052d`, `b0aa2db`, `c55cfd4`). **Overall verdict:
NOT BLOCKING** — no memory-safety/UB, leak, double-free, use-after-free, behavior
divergence, or rule violation. DoD independently re-verified green: `cargo build`
(default + `--features ffi`), 77 `ffi::` tests, ffi-covering `xtask lint`,
`format-check`, byte-identical header regen (all 5 new types + both callback
typedefs), and the C smoke test built + passing 7/7 via cmake+ctest. Callback
ownership/free correct; `SendUserData: Sync` sound and correctly scoped;
`async_value_op` release timing correct; both deviations accepted (nullable
`Option<fn>` ack typedef = cbindgen nullable-fn idiom; `set_acknowledgement_commit_callback -> *mut KafkaError_t`
= consistent with the other guarded sync ops).

## Finding 1 — minor (register-then-clear callback test) — ACCEPTED, no action
`test_set_acknowledgement_commit_callback_register_then_clear` runs against the
mock whose setter is a no-op (`mock_share_consumer.rs:192`), so it exercises
construction/cast/clone/drop but not end-to-end firing. **Disposition: accepted
as-is.** The marshaling has real teeth in the direct
`test_ffi_ack_commit_callback_marshals_offsets_and_error` (joins the dispatcher;
asserts partitions/offsets/topic-id/error + clean free); end-to-end §31 firing is
covered by the M9 `ShareConsumerImpl` tests. No fix needed.

## Finding 2 — async op panic-safety — FIXED (Phase 3 hardening pass)
If an awaited op **panics** (rather than returning `Err`) inside
`async_value_op` / `async_void_op` / `poll_async`, the completion job is never
enqueued → the callback never fires AND the single-owner guard (`Arc<AtomicU64>`)
is never released → the consumer is **permanently locked**. Pre-existing /
systemic (shared Phase-2+3 async helpers); only triggers on an abnormal panic
(bug/OOM) — normal `Result`-returning errors are handled correctly. The Critic
rated it a "future hardening pass," non-blocking for Phase 3.

**Resolution (2026-07-16):** fixed via a shared, one-shot RAII drop-guard
(`PanicCompletionGuard<P>` in `src/ffi/share_consumer.rs`) armed inside each of
the three spawned async tasks before the `.await`. Under `panic = unwind` (the
crate is not `panic = abort`; std `Mutex` poisoning depends on unwinding) a
panicking op unwinds the tokio task, running the guard's `Drop`. On an armed drop
(only reachable via a panic unwind) it enqueues an **error completion** that
releases the guard through the shared owner `Arc<AtomicU64>` clone — teardown-safe,
never dereferences the handle — and fires the C callback with an
`IllegalState("KafkaShareConsumer operation failed unexpectedly.")` handle, so the
consumer is unlocked and the caller never hangs. The normal path calls
`disarm()`, which defuses the bomb and hands the move-only completion payload
(the result builder + `user_data` for value ops) back to the ordinary completion
job, so the guard is released once and the callback fires once — never twice, and
never both success and error. The panic path mirrors each helper's normal
completion exactly (`poll_async` reuses `PollCompletion`), so normal-path
semantics and the `extern "C"` signatures/header are unchanged (header regenerated
byte-identical, 60375 bytes).

**Teeth:** a new `PanicConsumer` test double whose `poll` / `commit_sync` /
`commit_async` panic drives three regression tests
(`test_{poll_async,commit_sync_async,void_async_op}_panic_releases_guard_and_fires_error`)
asserting (a) the callback fires with a non-null error and (b) a subsequent
`acquire` succeeds (consumer not locked). Teeth confirmed by temporarily
neutering the guard's `Drop`: all three then fail with a callback `Timeout` and
the consumer stays locked — the exact pre-fix behavior.

_Actor (N=1), 2026-07-16._
