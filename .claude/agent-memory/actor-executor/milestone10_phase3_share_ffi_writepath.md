---
name: milestone10-phase3-share-ffi-writepath
description: Milestone 10 Phase 3 share-consumer C FFI write path — commit/close, result containers, registered ack-commit callback, cbindgen Option<fn> gotcha, C test infra
metadata:
  type: project
---

Milestone 10 Phase 3 = the share-consumer C FFI WRITE path (commit/close + the
registered ack-commit callback + C smoke test), built on Phase 1/2. Landed as 3
green commits (3a result-containers+commit/close, 3b ack callback, 3c C smoke
test) on branch `milestone9-share-consumer-python`.

**Why (durable, non-derivable decisions):**

- **cbindgen `Option<typedef>` gotcha (cost me a header regen):** `Option<extern
  "C" fn(...)>` collapses to a nullable C fn pointer ONLY when the fn-ptr type is
  written **inline** inside the `Option` (canonically as the exported typedef's
  own body). `Option<SomeTypedefName>` as a *parameter* type emits literal
  `Option<...>` (invalid C) into the header. Fix pattern used for the ack
  callback: make the **exported** typedef itself `= Option<unsafe extern "C"
  fn(...)>` (inline fn), and add a separate internal bare-fn alias
  (`AckCommitCallbackFn`) for the struct fields / `.map(|cb| ...)` — they are
  structurally the same fn type so assignment just works. The `set_*` parameter
  then takes the (already-`Option`) exported typedef directly, not
  `Option<typedef>`.
- **`std::sync::mpsc::Sender<T>` is `Send + Sync`** in this toolchain (std mpsc
  rewrite; verified by a throwaway `assert_send_sync` compile). So a registered
  `Arc<dyn AcknowledgementCommitCallback>` bridge (`FfiAckCommitCallback`) can
  hold a `completion_tx` clone directly. The only field that blocked `Sync` was
  `SendUserData` → added `unsafe impl Sync for SendUserData` in `ffi/common.rs`
  (sound: only the pointer *value* is read through `&self`, never derefed on the
  Rust side). Function pointers are already `Send + Sync`.
- **Result-container per-entry errors:** added `KafkaErrorInner::new(error)` +
  `borrow_error_ptr(&KafkaErrorInner) -> *const kafka_common_KafkaError_t` to
  `ffi/common.rs`. `ShareCommitResult_t` owns `Option<KafkaErrorInner>` per entry
  and `_get_error` returns a **borrowed** pointer (null = committed OK) usable
  with the existing `kafka_common_KafkaError_*` accessors. C must NOT
  `_destroy` a borrowed error (documented; same contract as reference's borrowed
  `TopicPartition`/`OffsetAndMetadata` from `OffsetMap`).
- **`async_value_op` (value-returning async op)** = the reference's `:2585`
  helper BUT with the Phase-2 teardown-safe divergence: release through an
  `Arc<AtomicU64>` clone (`release_owner(&owner)`) inside the completion job, NOT
  `release(hs)`. Signature: `<T, Fut, F, C>` where `op: F` runs the awaited
  method and `complete: C` builds the typed handle + fires the callback on the
  dispatcher (after release; the `T` is already owned so building post-release is
  safe). Inline-error path fires `complete(Err(e), user_data)` without taking the
  guard (no double-release).
- **`FfiAckCommitCallback::on_complete` (the novel unsafe piece):** `offsets` +
  `error` are BORROWED; marshal into owned handles (`box_share_acknowledge_offsets`
  + `box_error(e.clone())`) **synchronously before returning** — there is no
  `.await` between the borrow and the marshal. Then enqueue a `CompletionJob`
  wrapping an `AckCommitCompletion` (raw ptrs + `unsafe impl Send`) onto the
  shared dispatcher via `enqueue_or_run_inline` — never a tokio worker, never a
  per-call spawn (§31). The C callback OWNS the delivered
  `ShareAcknowledgeOffsets_t` (+ optional error) and frees them; the typedef is
  `*const` but ownership transfers (C casts away const to `_destroy`).
- **Deterministic container ordering:** `HashMap`/`HashSet` iteration order is
  arbitrary; sort container entries by `(topic bytes, partition)` and each
  partition's offsets ascending so C indexed access + test assertions are stable.
- **Production `close(timeout_ms)` bounds the join** with `tokio::time::timeout`
  (`share_consumer_impl.rs:~208`): signal_close → wakeup → bounded await_join. So
  the **Phase-2 finding-2 blocker is now unblocked** — a broker-less production
  acknowledge test IS feasible (construct → acknowledge rejects locally w/o a
  broker → `close_timeout(short)` reaps the bg IO thread → destroy). Deferred it
  this phase (message already asserted at M9; FFI ack path is structurally the
  tested guard/mock path) — flag for the Critic if C-boundary coverage wanted.

**C smoke-test infra (`bindings/c/tests/test_mock_share_consumer.c`):**
- Unity is a git submodule at `bindings/c/tests/unity` (uninitialized by
  default): `git submodule update --init bindings/c/tests/unity`.
- `cmake` is NOT preinstalled here; `brew install cmake` works (brew at
  `/opt/homebrew/bin`). The `bindings/c/Makefile` drives cmake; `bindings/c/build`
  is gitignored.
- For a quick standalone check, direct `cc` works: `cc -I target/include -I
  bindings/c/tests/unity/src <test.c> bindings/c/tests/unity/src/unity.c
  target/debug/libconfluent_kafka.a -liconv -lSystem -lm`. `cargo rustc --features
  ffi --lib -- --print native-static-libs` prints the exact native libs (minimal
  on macOS: `-liconv -lSystem -lc -lm`). The `ld: warning ... built for newer
  macOS` lines are harmless SDK-version mismatches, not errors.
- Wire new C tests into `bindings/c/CMakeLists.txt`: add_executable +
  target_include_directories(RUST_INCLUDE_DIR) + target_link_libraries(unity +
  ${CONFLUENT_KAFKA_RUST} + ${SYSTEM_LIBS}) + add_test.

**Deferred to Phase 4+ (Python):** `_confluentkafka.c` CPython glue,
`share_consumer.py`, gRPC harness. `client_instance_id` omitted whole-milestone.
