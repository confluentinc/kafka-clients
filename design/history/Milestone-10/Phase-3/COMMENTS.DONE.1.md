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

---

# Critic (agent 1) — hardening-pass verification of commit `be6ac2e`

Scope: commit `be6ac2e` — "panic-safe guard release + error completion in async
FFI ops" (the fix for the M2 / DONE-file "Finding 2" panic gap). Branch
`milestone9-share-consumer-python`, HEAD `455e24f`, worktree clean. This is a
verification of whether the claimed fix is actually correct, with special
attention to the teardown/task-cancellation interaction the commit message did
**not** analyze.

## Overall verdict: FIX IS SOUND — NOT BLOCKING

The drop-guard resolves the panic gap correctly. I found **no** memory-safety
issue, no double-release, no double-fire, no use-after-free, no deadlock, and no
behavioral regression — including the previously-unanalyzed runtime-teardown
cancellation path, which I verified is safe. One **LOW-severity, non-blocking**
documentation-accuracy finding is filed below (F1). The code itself needs no
change to be correct.

### Definitive finding on the teardown / cancellation interaction

**The guard DOES fire during `destroy`** — not only on a panic. An armed
`PanicCompletionGuard` fires on *any* armed `Drop`, and dropping a multi-thread
tokio `Runtime` (which `destroy` does at `share_consumer.rs:587`,
`drop(handle.runtime.take())`) **drops in place the future of any task suspended
at an `.await`**, running that future's `Drop` — hence the armed guard's `Drop`.

I confirmed this empirically with a standalone tokio 1.52 experiment (multi-thread
runtime, a task parked on `tokio::time::sleep`, an armed RAII bomb held across the
await): before the runtime drop the bomb had not fired; **the runtime drop ran the
bomb's `Drop` synchronously** (fired count 0 → 1, message emitted during the
drop). So for a *real* consumer whose `poll`/`commit` is awaiting the
network/timer/channel when the caller tears down, the guard fires during
teardown. (The single-owner guard caps this at **one** in-flight task, so at most
one guard fires.)

**It is memory-safe.** The `on_panic` closure and the completion job it enqueues
capture only owned data — the `owner: Arc<AtomicU64>` clone, a `completion_tx`
clone, the `Copy` callback target / fn-pointer, a freshly-`box_error`'d error, and
(value op) the move-only `complete`/`ud`. None dereferences the handle box, and
none captures `hs` (the `&'static ShareConsumerHandle` is used only in the normal
body before the await, never in `on_panic`). The ordering in `destroy` is the
linchpin and is correct: `drop(runtime.take())` (step 1) is a **blocking**
shutdown that runs every future-drop (hence every guard fire + enqueue) to
completion **before** `drop(handle)` (step 2) frees the box. During step 1
`handle.completion_tx` is still alive, so the dispatcher is up and `tx.send`
succeeds — the error job is queued (not run inline) and runs later on the detached
dispatcher touching only owned data. No UAF.

**No deadlock.** `std::sync::mpsc::Sender::send` (unbounded) is non-blocking; the
dispatcher is detached (destroy never joins it), so enqueuing during teardown
waits on nothing destroy holds.

**Does not break `test_destroy_blocks_until_in_flight_async_completes`.** That
test's `BarrierConsumer::poll` blocks the worker *synchronously* (`barrier.wait()`),
so the runtime cannot cancel it; the runtime drop waits, `poll` then returns
`Ok`, the **normal** path (`disarm`) runs, and no guard fires. I re-ran it 8×
(debug) + 4× (release) with no flake/crash.

**Behavior is acceptable and rule-aligned.** In the teardown-cancel case the guard
now delivers a late error callback where the pre-fix code silently dropped the
callback and left the guard stuck. Firing an error is exactly what CLAUDE.md §5
("silently completing or hanging futures is worse than an explicit error") and
§9.5 (callback obligation) require, and it is consistent with the pre-existing
detached-dispatcher design, under which normal completions already fire after
`destroy` for in-flight ops. Any `user_data` validity concern at that point is the
documented C precondition ("destroying concurrently with an in-flight op is a C
lifetime precondition the caller must uphold", `:568-569`), identical to the
normal-path late completion.

### Drop-guard core soundness — CORRECT
`disarm(self)` and `Drop` are mutually exclusive: the normal path `.await` returns
→ `disarm()` `.take()`s the payload (leaving `armed = None`, so the end-of-scope
`Drop` is a no-op) → the ordinary completion job is built and enqueued; the
abnormal path drops the still-armed guard → `on_panic` enqueues the error job.
There is **no `.await` between `disarm()` and the `enqueue_or_run_inline` call** in
any of the three helpers (`:716-730`, `:794-802`, `:1084-1092`), so a task cannot
be cancelled in that synchronous window — exactly one completion is enqueued, the
guard is released once, the callback fires once, never both success and error.
`disarm`'s `.expect(...)` cannot trip (it consumes `self`, called once, `armed`
always `Some` on the normal path).

### Move-only payload + Rust-2021 capture — CORRECT
`P = (C, SendUserData)` for the value op is `Send` (`C: Send`, `SendUserData:
Send`), so `PanicCompletionGuard<P>: Send` and the spawned future stays `Send`.
The `let target = target;` inside the void/poll `on_panic` closures forces
whole-`target` capture (Send) instead of Rust-2021 disjoint capture of the
`!Send` `*mut c_void` field — same idiom as `SendUserData::into_ptr`. No new
`unsafe impl Send/Sync` was added; the guard's `Send` is auto-derived. Compiles
clean under `--features ffi` (incl. clippy via `xtask lint`, which covers the ffi
surface).

### Teeth — GENUINE
The three panic tests drive a `PanicConsumer` whose awaited ops `panic!`; each
asserts the callback fires with a non-null "failed unexpectedly" error within 5 s
**and** a subsequent `acquire` succeeds. Pre-fix both legs fail (no completion
enqueued → `recv_timeout` trips; guard never released → `acquire` fails), so the
tests bracket the exact defect. They terminate promptly (whole file < 1 s) and the
tokio task harness catches the unwind (no process abort). Verified passing 8×
debug + 4× release.

### No ABI change — CONFIRMED
`PanicCompletionGuard`, `PanicAction`, and `op_panicked_error` are all private (no
`pub` / `no_mangle` / `extern "C"`; absent from the cbindgen allowlist). Forced a
rebuild → `target/include/confluent_kafka.h` is byte-identical (60375 bytes); the
new symbols do not appear (the only "panic" hits are pre-existing producer
`# Panics` docs). C smoke test rebuilt and run via cmake+ctest: **3/3 passed**
(incl. `mock_share_consumer`).

## Finding F1 — `PanicCompletionGuard` doc/message says "only on a panic unwind", but it also fires on teardown cancellation
- **File**: `src/ffi/share_consumer.rs:172-175` (guard doc), `:151-159` +
  `:689-692`, `:1064-1067` (op comments), `op_panicked_error` message `:158`
- **Severity**: Design Flaw (documentation accuracy) — LOW, non-blocking; the
  code is correct as-is
- **Description**: The guard's doc comment states dropping it "only happens on a
  panic unwind, since the normal path `disarm`s it first," and the error is named
  `op_panicked_error` / "operation failed unexpectedly." As verified above, the
  armed `Drop` **also** runs when the tokio runtime is dropped in `destroy` while
  an op is suspended at an `.await` — a cancellation, not a panic. The invariant
  as written is therefore false, and a future maintainer could add
  panic-only assumptions to `on_panic` (e.g. reading a panic payload, or assuming
  the op left broken state) that would be wrong under the cancellation trigger.
- **Expected**: Comment/name should acknowledge both triggers (panic unwind **and**
  runtime-drop cancellation of a suspended task during teardown), so the safety
  argument (owned-data-only capture; runs before the box is freed; late error
  callback honors the callback obligation) is explicit for the cancellation path.
- **Actual**: Presents the fire as panic-exclusive; the teardown-cancellation
  path is undocumented (the commit message explicitly did not analyze it).

_Critic (N=1), 2026-07-16 — verification of `be6ac2e`._

**Resolution (2026-07-16):** doc + naming clarified; **no behavior change**. The
private `PanicCompletionGuard` / `PanicAction` / `op_panicked_error` / `on_panic`
items were renamed to the trigger-neutral `IncompleteOpGuard` / `IncompleteOpAction`
/ `op_incomplete_error` / `on_incomplete`, and their docs/comments now state **both**
triggers of an *armed* drop: (a) a panic unwinding the spawned task, and (b) task
cancellation when `destroy` drops the tokio runtime while an op is suspended at an
`.await`. The invariant is now explicit — *if the op did not complete normally
(`disarm` was not called), release the access guard and fire an error completion* —
and the doc notes the enqueued job captures only owned data, so it stays safe under
teardown cancellation (the blocking runtime shutdown runs the drop before `destroy`
frees the handle box). The error **message**
(`"KafkaShareConsumer operation failed unexpectedly."`) is accurate for both cases
and was left unchanged. All renamed items are private (no `pub` / `no_mangle` /
`extern "C"`), so the C ABI is untouched: the header regenerates **byte-identical**
(60375 bytes, SHA-256 unchanged) and the renamed symbols never appear in it. The
three panic regression tests and `test_destroy_blocks_until_in_flight_async_completes`
still pass; `cargo build`/`test --features ffi`, `xtask lint`, and `format-check` are
all green.

_Actor (N=1), 2026-07-16._
