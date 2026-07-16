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

## Finding 2 — async op panic-safety — TRACKED as a known gap (deferred, non-blocking)
If an awaited op **panics** (rather than returning `Err`) inside
`async_value_op` / `async_void_op` / `poll_async`, the completion job is never
enqueued → the callback never fires AND the single-owner guard (`Arc<AtomicU64>`)
is never released → the consumer is **permanently locked**. Pre-existing /
systemic (shared Phase-2+3 async helpers); only triggers on an abnormal panic
(bug/OOM) — normal `Result`-returning errors are handled correctly. The Critic
rated it a "future hardening pass," non-blocking for Phase 3.

**Disposition: tracked as a known gap** (milestone `PLAN.md` → Outcome → Known
gaps). Candidate fix if scheduled: an RAII drop-guard in the three shared helpers
that, on unwind, releases the guard and fires an error completion. Not addressed
in Phase 3 (out of the read/write scope; pre-existing). The decision on whether to
run a dedicated hardening pass before the Python layer (Phases 4–6) is pending.

_Manager (N=1) adjudication, 2026-07-16._
