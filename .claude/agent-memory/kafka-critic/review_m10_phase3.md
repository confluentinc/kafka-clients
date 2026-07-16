---
name: review-m10-phase3
description: M10 Phase 3 share-consumer write-path C FFI — ack-callback marshaling, async_value_op guard/release, SendUserData Sync soundness, header-regen verification
metadata:
  type: project
---

M10 Phase 3 = share-consumer WRITE-path C FFI (commit/close + result containers +
`async_value_op` + registered ack-commit callback + C smoke test). Reviewed clean,
NOT blocking. See `src/ffi/share_consumer.rs`, `src/ffi/common.rs`,
`bindings/c/tests/test_mock_share_consumer.c`. Related: [[review-m10-phase2-share-ffi]],
[[review-m10-phase1-ffi-foundation]].

**Why:** highest-risk phase of the milestone (novel `unsafe` marshaling in the
registered ack-commit callback).

**How to apply (audit heuristics that paid off / would pay off next time):**
- **Borrowed→owned marshal in a callback must have NO `.await` between the borrow
  and the marshal.** Verify by reading the async fn body, not trusting the "no
  await here" comment. `on_complete(&self, &HashMap, Option<&KafkaError>)` marshals
  via synchronous `box_*` + `error.clone()` then enqueues — correct.
- **`unsafe impl Sync` for a `*mut c_void` wrapper (SendUserData) is SOUND** when
  the only shared-ref access is a `Copy`-read of the pointer *value* (never a Rust
  deref) and the C deref is serialized on the single dispatcher thread. It's also
  *necessary*: the wrapper lands in `Arc<dyn AcknowledgementCommitCallback>` (trait
  is Send+Sync), and it only compiles because every field incl.
  `std::sync::mpsc::Sender<CompletionJob>` is Sync in this toolchain — compilation
  success is the proof. Don't flag a scoped single-field `unsafe impl Sync`; DO
  flag a blanket one.
- **async_value_op / async_void_op / poll_async guard/release:** acquire at submit;
  inline acquire-failure fires `complete(Err)` WITHOUT taking the guard (no
  double/missed release); success releases via `Arc<AtomicU64>` clone in the
  completion job BEFORE building the handle + firing; awaited `T` moved into the
  job (owned before release); the job must NOT capture the `&'static handle` (only
  the spawned future does, during the await). Freed-box safety relies on blocking
  `Runtime` drop in `_destroy` step 1.
- **Systemic panic risk — FIXED in `be6ac2e` (Phase-3 hardening).** Was: if the
  awaited `op` PANICS (vs returns Err) in those three helpers, the completion job
  is never enqueued → callback never fires AND guard never released → consumer
  permanently locked. Now hardened with a one-shot RAII drop-guard armed before
  the await. Verified sound — and note it fires on tokio runtime-drop CANCELLATION
  during destroy too, not just panic. See [[review-ffi-async-dropguard-teardown]].
- **Result-container borrowed-error pattern:** `ShareCommitResult_get_error`
  returns `borrow_error_ptr(inner)` (`common.rs`), null = committed-OK; borrowed
  `TopicIdPartition_t` has NO standalone `_destroy`. Confirm nothing frees the
  borrowed sub-handle separately.
- **Mock setter is a no-op (`fn set_acknowledgement_commit_callback(...) {}`),** so a
  register-then-clear test on the mock has NO teeth for firing — the marshaling
  teeth must be a DIRECT `on_complete` unit test (construct FfiAckCommitCallback +
  real dispatcher + stub C cb that reads-all-fields-then-frees, then join the
  dispatcher). This phase did it right.
- **C smoke test verification:** cmake+ctest genuinely (don't trust "7/7"
  reported). `cmake -S bindings/c -B <scratch>`; `cmake --build . --target
  test_mock_share_consumer`; `ctest -R mock_share_consumer`. IDE clang "header not
  found" diagnostics are FALSE positives — the toolchain resolves via
  `RUST_INCLUDE_DIR=target/include`.
- **Header regen is `rerun-if-changed` on cbindgen.toml ONLY (not src).** A plain
  `cargo build --features ffi` won't regenerate it. To truly verify regen: `rm
  target/include/confluent_kafka.h && touch cbindgen.toml && cargo build --features
  ffi` → expect byte-identical output.
- **`cargo xtask lint` DOES cover `--features ffi`** (second clippy pass at
  `xtask/src/main.rs`) — green lint is meaningful for the C surface here.
