---
name: milestone10-phase1-ffi-common
description: Milestone 10 (share-consumer C FFI) Phase 1 — shared common.rs, BytesDeserializer, cbindgen; ungated-mod + header-reorder + ffi-clippy gotchas
metadata:
  type: project
---

Milestone 10 = share-consumer **C FFI** (KIP-932), building on the Milestone 9
Rust share consumer. Living design ref: `design/current/share-consumer-c-ffi-plan.md`
(§ numbers referenced below); milestone plan: `design/history/Milestone-10/PLAN.md`;
per-phase plans under `design/history/Milestone-10/Phase-N/PLAN.md`. Branch:
`milestone9-share-consumer`.

**Reference template branch:** `origin/dev/c_and_python_consumer_bindings` — has a
complete consumer FFI (`src/ffi/consumer.rs` ~130KB, `src/ffi/common.rs` ~278 lines)
but NO share Rust code. It is a porting *template*, not a merge source.

**Why:** these are non-derivable decisions/gotchas that recur through phases 2-6.

**How to apply — Phase 1 decisions that constrain later phases:**

- `src/ffi/common.rs` is declared **UNGATED** (`pub(crate) mod common;`), NOT
  `#[cfg(feature="ffi")]`, even though the PLAN prose said to gate it. Producer is
  also ungated and imports from common; gating common alone breaks the default
  (non-ffi) build, and gating both would strip the `kafka_*` extern-C symbols from
  the non-ffi cdylib (a behavior change). The sibling's actual `mod.rs` is ungated
  too. `share_consumer` mod (phase 2) should follow the same ungated pattern.
- `SendUserData`(+`into_ptr`) lives in the sibling's **consumer.rs**, not its
  common.rs. It was ported into our common.rs (needed by the §8 ack callback).
  All staged async machinery (`CompletionJob`, `spawn_dispatcher`,
  `enqueue_or_run_inline`, `OperationCallbackFn`, `OperationCompletion`+`fire`,
  `OperationCallbackTarget`, `SendUserData`) is per-item `#[allow(dead_code)]`
  (pub(crate), pub(crate) fields) until phase 2 wires it.
- **`cargo xtask lint`/`lint-fix` now run a second clippy pass with
  `--features ffi`** (added in the Phase-1 Critic-fixup round, COMMENTS.1 #1) on
  top of the default-feature pass, so ffi-gated code (`src/ffi/**`,
  `bytes_deserializer`) IS covered by the authoritative gate. You no longer need
  to run `cargo clippy --all-targets --features ffi -- -D warnings` by hand for
  FFI work — `cargo xtask lint` does it. (The default pass stays because
  without-ffi and with-ffi are distinct compilations.) NOTE: the
  `integration-tests` feature is still NOT linted by xtask — that gap remains.
- **Moving a cbindgen-exported type between modules REORDERS the generated header**
  (`target/include/confluent_kafka.h`): cbindgen emits in module-declaration order,
  so `common` (declared before `producer`) now emits `kafka_common_KafkaError_t` +
  its 5 accessors earlier. Content/signatures/ABI are identical. Verify "surface
  unchanged" with a **sorted** line-diff, not a byte-diff.
- `bytes` is an **optional** dep pinned `"1"` (caret → resolves to the already-locked
  1.11.1, transitive via prost/tokio; no second version). Wired only into the `ffi`
  feature. `BytesDeserializer` (`impl Deserializer<Bytes>`, copy_from_slice) is gated
  at the **mod-decl + re-export** level in `serialization/mod.rs` (not per-item
  `#[cfg]`, which would leave the file's `use` imports unused in non-ffi builds).
- cbindgen `item_types` now includes `"typedefs"` (for phase-2+ callback typedefs).
  The `[export] include` allowlist still gates output — staged pub(crate) typedefs do
  NOT leak. Phases 2-6 append new opaque/result/enum/typedef names to `[export]
  include` (full list in §6 of the design ref).
