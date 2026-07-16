# Milestone 10: Share Consumer C + Python bindings (KIP-932)

## Goal

Expose the Milestone-9 client-side KIP-932 share consumer to non-Rust callers:
first a **C FFI layer** over `KafkaShareConsumer` / `MockShareConsumer`, then a
cbindgen-generated `confluent_kafka.h`, a CPython C-extension binding, and a
high-level `share_consumer.py` — mirroring the Milestone-4 producer-binding
stack and the reference consumer bindings.

Detailed C-FFI design (the living reference for phases 1–6):
[`design/current/share-consumer-c-ffi-plan.md`](../../current/share-consumer-c-ffi-plan.md).

## Why now

Milestone 9 landed the full Rust share client on `milestone9-share-consumer`.
The producer already has the complete binding stack (Milestone 4); a consumer
binding stack exists on the sibling branch
`origin/dev/c_and_python_consumer_bindings` but has **no** share code, while this
branch has the share code but **only** the producer FFI. This milestone builds
the share binding stack here, using the sibling branch's consumer FFI as a
**template (studied, not merged)**.

## Scope

**In scope:**

- **C FFI** for the share consumer + supporting in-crate seams + cbindgen/build
  wiring + FFI-level tests (phases 1–6).
- **Python**: CPython C-extension glue, `share_consumer.py` high-level wrapper,
  Python unit tests, gRPC harness wiring (phases 7–9).

**Out of scope:**

- The regular (KIP-848) consumer FFI/Python port to this branch — a separate
  effort; this milestone only de-duplicates cleanly with it later (shared
  `common.rs`).
- Live-broker / `AdminClient` integration tests.
- Metrics / telemetry — `client_instance_id` is omitted pending KIP-714.

## Base / template

- **Rust API (this branch, M9):** `crate::consumer::{KafkaShareConsumer,
  MockShareConsumer, ShareConsumer, new_share_consumer}`.
- **FFI template (sibling branch, studied not merged):** `src/ffi/consumer.rs` +
  `src/ffi/common.rs` on `origin/dev/c_and_python_consumer_bindings`.
- **Producer precedent (this branch):** `src/ffi/producer.rs`, `build.rs`
  cbindgen step, `cbindgen.toml`, `bindings/{c,python}`.

## Phases

| # | Phase | Rust/output | Depends on |
|---|---|---|---|
| 1 | **C-FFI foundation** ✅ DONE — port `common.rs`; add `BytesDeserializer`; `cbindgen.toml` (`item_types += typedefs`, seed `[export]`); wire `src/ffi/mod.rs`; `xtask lint` covers `ffi`. | `src/ffi/common.rs`, `src/common/serialization/bytes_deserializer.rs`, `cbindgen.toml`, `src/ffi/mod.rs`, `xtask/src/main.rs` | M9 |
| 2 | **Handle + read + acknowledge** ✅ DONE *(merged; was 2+3+4)* — `ShareConsumerHandle`/`ShareConsumerKind` + single-owner guard + wakeup seam; `ShareConsumerProperties_*`; `KafkaShareConsumer_new`/`MockShareConsumer_new`/`_destroy`/`_wakeup`; subscribe/unsubscribe/subscription; `_poll`/`_poll_async` + shared `box_records` + `ConsumerRecord(s)_*` (incl. `_delivery_count`); `AcknowledgeType_t` + `_acknowledge`/`_acknowledge_with_type`/`_acknowledge_by_offset`. | `src/ffi/share_consumer.rs`, shared record marshaling in `common.rs`, `new_share_consumer_with_wakeup` in `src/consumer/mod.rs`, `MockShareConsumer::wakeup_handle` | 1 |
| 3 | **Commit + close + ack callback + tests** ✅ DONE *(merged; was 5+6; + async panic-safety hardening)* — `TopicIdPartition_t`, `ShareCommitResult_t`, `ShareAcknowledgeOffsets_t`; commit/close (sync+async); `set_acknowledgement_commit_callback`; `acquisition_lock_timeout_ms`; Rust FFI tests + C smoke test. | `src/ffi/share_consumer.rs` (+ tests), `bindings/c/tests/test_mock_share_consumer.c`, `bindings/c` build wiring | 2 |
| 4 | **Python binding** *(merged; was 4+5+unit tests)* — CPython `_confluentkafka.c` share glue (port the consumer extension machinery + the ack surface: `AcknowledgeType`, `ShareCommitResult`/`ShareAcknowledgeOffsets`/`TopicIdPartition` drains, commit trampoline, persistent `set_acknowledgement_commit_callback`); sync `KafkaShareConsumer` + `MockShareConsumer` wrapper; pytest unit tests. | `bindings/python/_confluentkafka.c`, `bindings/python/share_consumer.py`, `bindings/python/test/unit/test_share_consumer.py`, `pyproject.toml` | 3 |
| 5 | **Multilanguage harness** ⏸ DEFERRED — Docker/gRPC harness wiring + any live-broker integration. Out of scope: integration infra needing Docker + a share-group broker (à la M9's `#[ignore]`-gated integration tests); not required for the binding deliverable. | `bindings/python/grpc_server*.py`, `grpc_translate.py`, `Dockerfile.grpc*` | 4 |

The milestone was originally nine phases and **regrouped** twice at review, to
five. **C FFI (1–3, done):** foundation = 1; handle + read + acknowledge = 2;
commit + close + ack-callback + tests (+ the async panic-safety hardening) = 3 —
split at the read/write seam, with the novel unsafe marshaling isolated in 3.
**Python (4–5):** the CPython extension + the Pythonic wrapper + unit tests are
co-developed and merge into one **Python binding** phase (4, specified in
`Phase-4/PLAN.md`); the Docker/gRPC multilanguage harness is the final phase (5),
detailed when it starts.

## Key constraints (carried into every phase)

- **CLAUDE.md §3 C FFI conventions:** opaque `*_t` types; `kafka_consumer_*`
  namespace (drop `clients`); shared `kafka_common_KafkaError_t` / `Node` /
  `TopicIdPartition`; no NULL-precondition checks on required params.
- **Async Rust over sync C ABI:** embedded Tokio runtime + `block_on` (sync
  path) / dispatcher-thread completion callbacks (`_async` path); single-owner
  `acquire`/`release` guard; `WakeupHandle` for `wakeup()` — per the reference.
- **Receive-path zero-copy (§27):** K/V = `bytes::Bytes`; record key/value cross
  as `(ptr, len)` borrowing the owning `ConsumerRecords` batch.
- **Acknowledgement callback obligation (§31):** the registered ack-commit
  callback fires on the dispatcher thread (app side), never the bg task, never a
  per-callback `tokio::spawn`.
- **Shared `common.rs` from day one** so a later consumer-FFI port de-duplicates.
- K/V = `Bytes`; `client_instance_id` omitted (review decisions §12 of the
  design doc).

## Workflow per phase

Per `.claude/rules/agent-roles.md §3`: Manager-coordinated Actor → Critic → fix
loop, agent number **N=1**. Comment files at
`design/history/Milestone-10/Phase-N/COMMENTS.<critic-id>.md`; resolved comments
move to `COMMENTS.DONE.<critic-id>.md`. All work lands on
`milestone9-share-consumer`.

## Definition of Done (per phase, beyond `definition-of-done.md`)

- `cargo build`, `cargo test`, `cargo xtask lint`, `cargo xtask format-check`
  clean **under `--features ffi`**.
- cbindgen regenerates `target/include/confluent_kafka.h` with the new share
  types (added to `[export] include`; `item_types` includes `typedefs`).
- From phase 6: `bindings/c/tests/test_mock_share_consumer.c` builds against the
  generated header and passes.
- No leaks under the single-owner guard; `_destroy` teardown ordering verified
  (runtime shutdown → drop consumer → drop `completion_tx` + detach dispatcher).
- Error-message content asserted (DoD §3), not just `is_err()`.

## Outcome

**Milestone wrapped at Phase 4 (user decision, 2026-07-16)** — its stated
deliverable (C + Python client bindings for the KIP-932 share consumer) is achieved
and unit-tested end to end across Rust → C → Python:

- **C FFI (Phases 1–3)** on `milestone9-share-consumer-python`: the full
  share-consumer C ABI (construct / subscribe / poll / acknowledge / commit / close
  + the registered ack-commit callback), a cmake-buildable generated header, a
  passing C smoke test, and an async panic-safety hardening pass.
- **Python binding (Phase 4):** CPython `_confluentkafka.c` share glue + a sync
  `KafkaShareConsumer` / `MockShareConsumer` `share_consumer.py` wrapper + 67 pytest
  tests (incl. proven-teeth ack-callback refcount guards).

Every phase ran a Manager-coordinated Actor → Critic → fix loop (N=1); the Critic
caught three real defects that were fixed (reference `destroy` UAF, async
panic-lock, and a refcount test-teeth gap). Nothing pushed; no PR.

### Deferred / not done

- **Phase 5** — the Docker/gRPC multilanguage harness + any live-broker integration
  (integration infra; the same class M9 `#[ignore]`-gated).
- An **`async`** Python `KafkaShareConsumer` (sync-first shipped; the reusable
  `_run_async` makes it an easy additive follow-on).
- `client_instance_id` across the FFI/Python (KIP-714 telemetry).
- macOS-native `make build`/`make test` needs a `<threads.h>` shim (pre-existing
  producer-half dependency; Linux CI unaffected).

### Hardening (resolved)

- **Async op panic-safety** (was Phase 3 Critic finding #2): a panic — or a task
  cancellation when `ShareConsumer_destroy` drops the runtime — in an awaited op
  inside `async_value_op` / `async_void_op` / `poll_async` previously skipped the
  completion job, leaving the single-owner guard held (consumer locked) and the
  callback unfired. **Resolved** by a one-shot RAII drop-guard (`IncompleteOpGuard`)
  that, on an armed drop, releases the guard via the owner `Arc<AtomicU64>` clone
  and fires an error completion (commits `be6ac2e`, `0477c4c`; Critic-verified safe
  under teardown cancellation, header byte-identical). No known gaps remain in the
  C FFI layer.
