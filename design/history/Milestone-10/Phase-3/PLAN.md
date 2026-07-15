# Phase 3: Share consumer FFI — commit + close + ack-commit callback + C smoke test

*(Merged phase: was original C-FFI phases 5+6. This is the WRITE path — the async
commit/close completions plus the one genuinely novel `unsafe` pattern, the
registered acknowledgement-commit callback — and the end-to-end C smoke test.
Isolated from Phase 2 on purpose: the callback/result marshaling is the
highest-risk code in the milestone.)*

## Goal

Complete the share consumer's C ABI in `src/ffi/share_consumer.rs` (+ shared
`records.rs`/`common.rs`) on top of Phase 2:

1. **Commit + close** — `commit_sync`/`commit_sync_timeout` (sync + async),
   `commit_async` (sync + async), `close`/`close_timeout` (sync + async), and the
   sync getter `acquisition_lock_timeout_ms`.
2. **Result containers** — `TopicIdPartition_t`, `ShareCommitResult_t`,
   `ShareAcknowledgeOffsets_t`.
3. **The registered ack-commit callback** — `set_acknowledgement_commit_callback`
   wiring a C callback through a Rust `AcknowledgementCommitCallback` impl that
   marshals completed offsets to owned C handles and fires on the dispatcher.
4. **Value-returning async helper** — implement `async_value_op` (uses the staged
   `SendUserData`) for `commit_sync*`.
5. **C smoke test** — `bindings/c/tests/test_mock_share_consumer.c`, wired into the
   `bindings/c` build behind `--features ffi`.

## Branch

`milestone9-share-consumer-python` (canonical M10 branch). All commits land here.
**Verify `git branch --show-current` == `milestone9-share-consumer-python` before
committing** (a shared-workdir agent switched the branch during Phase 2; do not let
it recur).

## Sources / template

- Template: `src/ffi/consumer.rs` on the sibling branch (scratchpad
  `ref_consumer_ffi.rs`) for `async_value_op` (`:2585`), the sync-commit oneshot
  bridge, and result-container builders. **No** template exists for a *registered*
  ack-commit callback — that is designed fresh (design doc §8).
- Phase-1/2 output: `common.rs` (staged `SendUserData`/`OperationCompletion`/
  `spawn_dispatcher`/`enqueue_or_run_inline`), `share_consumer.rs`
  (`ShareConsumerHandle`, guard, poll bridge), `records.rs`.
- Rust API (M9): `ShareConsumer` trait — `commit_sync -> Result<HashMap<TopicIdPartition, Option<KafkaError>>>`,
  `commit_sync_timeout`, `commit_async -> Result<()>`, `close`/`close_timeout`,
  `acquisition_lock_timeout_ms -> Result<Option<i32>>`,
  `set_acknowledgement_commit_callback(Option<Arc<dyn AcknowledgementCommitCallback>>)`.
  `AcknowledgementCommitCallback::on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&KafkaError>)` (async).
- Design reference: `design/current/share-consumer-c-ffi-plan.md` §6–§9.

## Output — C surface (all `kafka_consumer_`/`kafka_common_` per §3)

**New types** (add to `cbindgen.toml` `[export] include`):
- `kafka_common_TopicIdPartition_t` — `{ topic: CString, topic_id: [u8;16], partition: i32 }`; accessors `_topic` (`*const c_char`), `_topic_id` (`*const u8`, len 16), `_partition` (`i32`). **Borrowed** from a container — no standalone `_destroy`.
- `kafka_consumer_ShareCommitResult_t` — owns `Vec<(TopicIdPartition, Option<KafkaError>)>`; `_count`, `_get_partition(i) -> *const TopicIdPartition_t` (borrowed), `_get_error(i) -> *const kafka_common_KafkaError_t` (borrowed; **null = that partition committed OK**), `_destroy`.
- `kafka_consumer_ShareAcknowledgeOffsets_t` — owns the marshaled `HashMap<TopicIdPartition, HashSet<i64>>`; `_partition_count`, `_get_partition(i) -> *const TopicIdPartition_t`, `_offset_count(i) -> i32`, `_get_offset(i, j) -> i64`, `_destroy`. **Owned by the C ack callback** (it frees it).
- Typedefs: `kafka_consumer_ShareConsumer_commit_callback_t = (*mut ShareCommitResult_t, *mut kafka_common_KafkaError_t, *mut c_void)`; `kafka_consumer_ShareConsumer_AcknowledgementCommitCallback_t = (*const ShareAcknowledgeOffsets_t, *const kafka_common_KafkaError_t, *mut c_void)`.

**Commit / close / getter:**
- `ShareConsumer_commit_sync(consumer, out_error) -> *mut ShareCommitResult_t` + `_commit_sync_async(consumer, commit_callback, user_data)`.
- `ShareConsumer_commit_sync_timeout(consumer, timeout_ms, out_error) -> *mut ShareCommitResult_t` + `_commit_sync_timeout_async(...)`.
- `ShareConsumer_commit_async(consumer) -> *mut kafka_common_KafkaError_t` + `_commit_async_async(consumer, op_callback, user_data)`.
- `ShareConsumer_close(consumer) -> *mut kafka_common_KafkaError_t` + `_close_timeout(consumer, timeout_ms)` + `_close_async` / `_close_timeout_async`.
- `ShareConsumer_acquisition_lock_timeout_ms(consumer, out_ms: *mut i32, out_error) -> bool` (sync; returns presence, writes the value when present).

**Registered ack-commit callback:**
- `ShareConsumer_set_acknowledgement_commit_callback(consumer, callback_or_null, user_data)` (sync, guarded). Non-null → wrap in `FfiAckCommitCallback` and `consumer.set_acknowledgement_commit_callback(Some(Arc::new(...)))`; null → `set_acknowledgement_commit_callback(None)`.

## The registered ack-commit callback (the novel `unsafe` piece — design carefully)

```rust
struct FfiAckCommitCallback {
    cb: AckCommitCb,                       // Send-wrapped C fn ptr
    user_data: SendUserData,
    completion_tx: Sender<CompletionJob>,  // clone of the handle's dispatcher tx
}
#[async_trait]
impl AcknowledgementCommitCallback for FfiAckCommitCallback {
    async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&KafkaError>) {
        // offsets/error are BORROWED — the borrow ends when this returns.
        // Marshal into an OWNED ShareAcknowledgeOffsets_t (+ optional boxed
        // KafkaError_t), then enqueue a CompletionJob that invokes the C fn on
        // the dispatcher thread and hands it ownership. One alloc per commit;
        // off the per-record hot path.
    }
}
```
- **Threading (§31):** `on_complete` runs on the caller/app task during the
  consumer's poll/commit drain. Under the FFI that is a runtime worker (async
  path) or the `block_on` thread (sync path). Route the C invocation onto the
  **dispatcher thread** (enqueue, don't call the C fn on a tokio worker) — keeps
  callback threading consistent with every other FFI callback; never a per-call
  `tokio::spawn`.
- **Ownership:** build owned handles *before* `on_complete` returns; the C
  callback takes ownership of the `ShareAcknowledgeOffsets_t` (and any non-null
  `KafkaError_t`) and must free them.

## Implementation steps (each an independent, green commit)

- **3a — result containers + commit/close + `async_value_op` + acquisition-lock.**
  `TopicIdPartition_t`, `ShareCommitResult_t`; implement the value-returning
  `async_value_op` (drop the `SendUserData` staging); all commit/close entry
  points + `acquisition_lock_timeout_ms`; cbindgen updates. **Test (mock):**
  `commit_sync` → empty `ShareCommitResult` (count 0); `commit_async` ok; `close`
  ok; `acquisition_lock_timeout_ms` presence/value. **Commit.**
- **3b — registered ack-commit callback + marshaling.** `ShareAcknowledgeOffsets_t`,
  `FfiAckCommitCallback`, `set_acknowledgement_commit_callback` (register + clear).
  **Test (direct, NOT via mock — the mock's setter is a no-op):** call
  `FfiAckCommitCallback::on_complete` with a synthetic `HashMap<TopicIdPartition,
  HashSet<i64>>` + an `Option<&KafkaError>` and a Rust-defined stub C callback;
  assert the delivered `ShareAcknowledgeOffsets_t` has the right partitions/offsets
  and the error maps correctly, and that freeing it (the `_destroy` path) is clean
  (no leak/double-free — exercise under the guard/dispatcher). Also test
  register-then-clear does not crash and clearing drops the C pointers. **Commit.**
- **3c — C smoke test + wiring.** `bindings/c/tests/test_mock_share_consumer.c`:
  create → subscribe → `add_record` → poll → read fields → acknowledge →
  `commit_sync` → close → destroy, asserting handle counts / non-null. Wire into
  `bindings/c/Makefile`/CMake behind `--features ffi`. **Commit.**

## Tests (ship with the code, DoD §3)

- Rust `#[cfg(test)]` FFI tests for commit/close/acquisition-lock against
  `MockShareConsumer` (assert result-container contents + error-message content).
- **Direct marshaling unit test** for `FfiAckCommitCallback` (above) — this is the
  teeth for the novel code, since the mock never fires the callback.
- C smoke test builds against the generated header and passes.
- **Callback-firing limitation (document honestly):** `MockShareConsumer`'s
  `set_acknowledgement_commit_callback` is a no-op and never invokes the callback,
  so end-to-end *firing through the ABI* is not exercised by the mock. The §31
  drain that invokes the callback is covered by the M9 `ShareConsumerImpl` unit
  tests; the marshaling is covered by the direct unit test above. Note this in the
  COMMENTS.DONE / PLAN rather than faking a mock round-trip.

## Definition of Done

- `cargo build` (default) and `cargo build --features ffi` clean.
- `cargo test --features ffi` green (incl. the direct marshaling test).
- `cargo xtask lint` (ffi-covering) and `cargo xtask format-check` clean.
- Header regenerates with all new types + the two callback typedefs.
- C smoke test builds + passes.
- No leaks / double-frees in the result containers or the callback marshaling
  (verify the `_destroy` paths and the owned-by-callback handoff).
- Branch is `milestone9-share-consumer-python` at every commit.

## Risks / watch-items

- **Ack-callback marshaling (highest):** borrowed `&HashMap` → owned handles
  before return; hand ownership to the C callback; free rules exact. Capture a
  `completion_tx` clone + Send-wrapped C fn/user_data; enqueue (don't spawn).
- **`async_value_op` guard/release timing:** acquire at submit; build the value
  handle AFTER the await; **release in the completion job before firing the
  callback** (same rule as `_poll_async`); no double-release on the inline error path.
- **`commit_async` semantics:** Java doesn't block on the network but drains the
  ack-callback queue; the sync FFI `block_on(commit_async())` is correct; the
  `_async` variant is a void op.
- **`ShareCommitResult` error mapping:** `Option<KafkaError>` per partition → null
  `KafkaError_t` for the `None` (committed-OK) case; do not box a spurious error.
- **`close` under the mock vs production:** Phase 3 exposes real `close`, so a
  production consumer can now be constructed AND reaped in tests without leaking a
  bg IO thread (the Phase-2 finding-2 blocker) — consider whether a production-path
  test is now feasible for the non-in-flight acknowledge message.
- **Branch hygiene:** re-verify the branch after the Actor and Critic each return.

## Not in this phase (→ Phase 4+, Python)

`_confluentkafka.c` CPython glue, `share_consumer.py`, Python tests, gRPC harness.
`client_instance_id` remains omitted for the whole milestone (KIP-714).
