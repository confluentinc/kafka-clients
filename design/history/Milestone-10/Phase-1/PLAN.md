# Phase 1: C-FFI foundation (shared `common.rs` + `BytesDeserializer` + cbindgen)

## Goal

Stand up the shared FFI infrastructure that the share-consumer FFI (phases 2–6)
builds on — **with no share-specific code yet**:

1. Factor the cross-cutting FFI machinery out of `producer.rs` into a new shared
   `src/ffi/common.rs` (error handles + logger — immediately used by producer;
   the async dispatcher / callback-marshaling helpers — staged for phase 2).
2. Add a `BytesDeserializer` so phase 2 can construct `ShareConsumer<Bytes, Bytes>`.
3. Update cbindgen + build wiring so the header regenerates with `typedefs` and
   the (unchanged) `kafka_common_KafkaError_t` surface.

No new `extern "C"` functions beyond what **moves** from `producer.rs`. Producer
behavior and generated-header output are unchanged.

## Branch

`milestone9-share-consumer`. All commits land here.

## Why this is a standalone phase (not merged into phase 2)

- `kafka_common_KafkaError_t`, `KafkaErrorInner`, `box_error`, `error_ref`, and
  `init_default_logger` currently live in **`producer.rs`** (verified:
  `producer.rs:68,136,178,290,301`). Defining them again in a new `common.rs`
  would duplicate the type — Rust `E0428` and a cbindgen double-emit. So the
  shared `common.rs` is created by **moving** them out of `producer.rs` and
  repointing producer at `common`. This removes the duplication hazard *and*
  delivers review-decision #5 ("shared `common.rs` from day one").
- The async dispatcher / callback machinery (`spawn_dispatcher`, `CompletionJob`,
  `OperationCompletion`, …) is consumer/share-only — the producer FFI uses a
  block-on `FutureRecordMetadata` model and per-call copy-out callbacks, not the
  dispatcher thread. It therefore lands in `common.rs` **staged behind
  `#[allow(dead_code)]`** until phase 2 consumes it (matching the repo staging
  pattern, e.g. `share_consumer_impl.rs`'s `#![allow(dead_code)]`).

## Sources / template

- **Template:** `src/ffi/common.rs` on `origin/dev/c_and_python_consumer_bindings`
  (staged in scratchpad as `ref_common_ffi.rs`, ~278 lines) — port ≈verbatim.
- **This branch:** `src/ffi/producer.rs` (source of the error/logger machinery to
  move), `src/common/serialization/{deserializer.rs, mod.rs}`, `cbindgen.toml`,
  `build.rs`, `Cargo.toml`.

## Output / changes

**NEW `src/ffi/common.rs`:**
- `init_default_logger()` (idempotent `env_logger::try_init`).
- `KafkaErrorInner { error: KafkaError, message_cstring: CString }`,
  `kafka_common_KafkaError_t` (opaque `#[repr(C)] { _private: [u8; 0] }`),
  `pub(crate) fn box_error`, `pub(crate) unsafe fn error_ref`.
- `extern "C"`: `kafka_common_KafkaError_code` / `_message` / `_is_retriable` /
  `_is_fatal` / `_destroy` (moved from producer, identical bodies).
- **Staged (`#[allow(dead_code)]` until phase 2):** `type CompletionJob`,
  `spawn_dispatcher`, `enqueue_or_run_inline`, `OperationCallbackFn`,
  `OperationCompletion` (+ `fire`, `unsafe impl Send`), `OperationCallbackTarget`,
  `SendUserData` (+ `into_ptr`).

**`src/ffi/producer.rs`:** delete the moved items; add
`use super::common::{kafka_common_KafkaError_t, box_error, error_ref, init_default_logger};`
(only those actually referenced). Type identity is preserved (same name ⇒ cbindgen
emits an identical declaration).

**`src/ffi/mod.rs`:** add `#[cfg(feature = "ffi")] pub(crate) mod common;` above
`producer`.

**NEW `src/common/serialization/bytes_deserializer.rs`:**
```rust
#[cfg(feature = "ffi")]
pub struct BytesDeserializer;
#[cfg(feature = "ffi")]
impl Deserializer<bytes::Bytes> for BytesDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<bytes::Bytes, KafkaError> {
        Ok(bytes::Bytes::copy_from_slice(data))
    }
}
```
Re-export from `serialization/mod.rs` (cfg-gated).

**`Cargo.toml`:** `bytes` is **not** a dependency today (verified). Add
`bytes = { version = "<match Cargo.lock transitive>", optional = true }` and
extend `ffi = ["dep:cbindgen", "dep:env_logger", "dep:bytes"]`. Non-ffi builds are
unaffected.

**`cbindgen.toml`:** `item_types` → `["functions", "structs", "typedefs"]` (this
branch is missing `typedefs`, needed for phase-2+ callback typedefs; harmless now).
`[export] include` keeps `kafka_common_KafkaError_t` (already listed for producer);
**no new types this phase.**

## Implementation steps (commit after each green sub-step)

1. Create `common.rs` with the error + logger machinery moved from `producer.rs`;
   add `pub(crate) mod common` to `mod.rs`; repoint `producer.rs`.
   `cargo build --features ffi` + producer FFI tests green. **Commit.**
2. Add the staged dispatcher / callback machinery to `common.rs`
   (`#[allow(dead_code)]`). Build. **Commit.**
3. Add `bytes` optional dep + `ffi` wiring + `BytesDeserializer` + re-export.
   Build **both** default and `--features ffi`. **Commit.**
4. `cbindgen.toml` `item_types += "typedefs"`; regenerate header; confirm the
   `kafka_common_KafkaError_t` surface is unchanged. **Commit.**

## Tests

- **Regression:** all existing producer FFI tests pass under
  `cargo test --features ffi` — proves the error-machinery move is
  behavior-preserving.
- **New (`common.rs`):** round-trip `box_error(err)` → `_code` / `_message` /
  `_is_retriable` / `_is_fatal` → `_destroy`; **assert message content** (DoD §3),
  not just non-null; assert a null handle yields code 0 / null message.
- **New (`BytesDeserializer`):** `deserialize(topic, &[1,2,3])` equals
  `Bytes::from_static(&[1,2,3])`; empty slice → empty `Bytes`.
- **Header check:** `cargo build --features ffi` regenerates
  `target/include/confluent_kafka.h`; grep-assert it still declares
  `kafka_common_KafkaError_t` + the five accessors, and now contains the
  `typedefs` output (no functional change).

## Definition of Done

- `cargo build` (default **and** `--features ffi`) clean; `cargo test --features ffi`
  green; `cargo xtask lint` + `cargo xtask format-check` clean.
- Exactly one definition of `kafka_common_KafkaError_t` (in `common.rs`);
  `producer.rs` sources it from `common`.
- Non-ffi build unaffected — `bytes` / `BytesDeserializer` are `ffi`-gated.
- Header regenerates; `KafkaError` surface unchanged vs. pre-phase (grep or byte-diff).
- No share-specific code (that begins in phase 2).

## Risks / watch-items

- **Visibility:** `box_error` / `error_ref` are private (`fn` / `unsafe fn`) in
  `producer.rs`; in `common.rs` they must be `pub(crate)` for both producer and the
  future `share_consumer.rs`.
- **cbindgen `typedefs`:** adding it may surface unintended typedefs; the
  `[export] include` allow-list still gates output — confirm the header gains
  nothing beyond the intended surface.
- **`bytes` version skew:** pin to the version already resolved transitively in
  `Cargo.lock` to avoid a second `bytes` crate version in the graph.
- **`init_default_logger`:** keep a single definition in `common.rs`; `env_logger`
  is already an `ffi` dependency, so no new dep beyond `bytes`.

## Not in this phase

`ShareConsumerHandle`, any `extern "C"` share function, the single-owner access
guard, and the async op wrappers (`async_void_op` / `async_value_op`, which are
`dyn ShareConsumer`-specific) — all phase 2+. This phase only makes the shared
foundation exist and keeps producer green on top of it.
