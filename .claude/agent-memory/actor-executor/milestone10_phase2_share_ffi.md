---
name: milestone10-phase2-share-ffi
description: Milestone 10 Phase 2 share-consumer C FFI — handle/guard/poll/acknowledge, teardown-safe guard divergence, shared records module, cbindgen enums
metadata:
  type: project
---

Share consumer C FFI (`src/ffi/share_consumer.rs` + shared `src/ffi/records.rs`), built on Phase-1 `common.rs`. Landed as 3 green commits (2a handle/lifecycle/subscribe, 2b poll+records, 2c acknowledge).

**Why (context):** Milestone-10 exposes the KIP-932 share consumer over a C ABI for the CPython/gRPC layers. Template is `src/ffi/consumer.rs` on `origin/dev/c_and_python_consumer_bindings` (scratchpad `ref_consumer_ffi.rs`), adapted to `ShareConsumer<Bytes,Bytes>`.

**How to apply (durable decisions for Phase 3+):**

- **Teardown-safe guard is a deliberate DIVERGENCE from the reference.** The reference's destroy (`shutdown_background()` non-blocking + detached dispatcher + completion jobs holding `&'static Handle` for `release`) has a latent use-after-free that surfaced here as a SIGBUS under the *parallel* test runner (single-threaded hid it). Our fix, reused by Phase 3's commit/close async completions: `owner: Arc<AtomicU64>` (completion jobs release through their own clone, valid after the handle is freed); `runtime: Option<Runtime>` so destroy does `drop(handle.runtime.take())` FIRST (blocking → all spawned tasks stop before the consumer is dropped) while the box is still alive, then `drop(handle)`; dispatcher `JoinHandle` stored plainly and detached (dropped, not joined). Async spawn still captures `&'static Handle` (`hs`) for `consumer_mut(hs)` during the op only — safe because the blocking runtime drop stops the op before free.
- **Shared vs share-private placement:** consumer-generic marshaling lives in shared `src/ffi/records.rs` (`box_records`+`ConsumerRecordsInner`+all `ConsumerRecord(s)_*` accessors, `box_string_list`+`StringList`, `pub(crate) record_ref`) so a future regular-consumer FFI reuses it. Guard/handle/`AcknowledgeType_t`/entry points are share-private in `share_consumer.rs`.
- **Two ffi-gated Rust seams only** (`#[cfg(feature="ffi")]`): `new_share_consumer_with_wakeup` in `consumer/mod.rs` (returns the WakeupHandle + unwrapped error; public `new_share_consumer` keeps its "Failed to construct Kafka share consumer" wrapper) and `MockShareConsumer::wakeup_handle`. `build_share_consumer` now returns the `ShareConsumerWithWakeup<K,V>` type alias (added to dodge clippy `type_complexity`).
- **cbindgen:** `item_types` MUST include `"enums"` or an enum type is referenced but its body never emitted. `prefix_with_name` yields `kafka_consumer_AcknowledgeType_t_ACCEPT` etc. Every new opaque type/typedef/enum must be appended to `[export] include` (it's an allowlist). build.rs reruns on `cbindgen.toml`, but a stale header may need `touch build.rs`.
- **Guard-rejection error:** this branch has NO `KafkaError::concurrent_modification` / `Errors::ConcurrentModification`; use `illegal_state("KafkaShareConsumer is not safe for multi-threaded access.")`.
- **AcknowledgeType_t** needs `#[derive(Clone,Copy)]` (clippy `wrong_self_convention` on `to_*(self)`) and `#[allow(dead_code, clippy::upper_case_acronyms)]` (C ABI variant names; constructed only by C).
- **FFI tests are plain `#[test]` (never `#[tokio::test]`)** — the extern-C fns `block_on`/drop the embedded runtime internally; calling from a tokio context panics. Async paths driven by a Rust `unsafe extern "C"` callback + an mpsc `Sender` in `user_data` (kept alive by a blocking `rx.recv_timeout`). Run the FFI tests under the parallel runner several times to catch teardown races.
- `common::SendUserData` + a value-returning `async_value_op` are still staged (`#[allow(dead_code)]`) for Phase 3's commit_sync/commit_async/close completions.

**Deferred to Phase 3:** commit/close (+async), `set_acknowledgement_commit_callback` (registered callback), result containers (`TopicIdPartition_t`, `ShareCommitResult_t`, `ShareAcknowledgeOffsets_t`), `acquisition_lock_timeout_ms`, the C smoke test. `client_instance_id` getter omitted whole-milestone (only the mock `set_client_instance_id` driver exists).
