---
name: review-m10-phase1-ffi-foundation
description: Milestone-10 Phase-1 C-FFI foundation review — xtask-lint ffi blind spot, ffi module gating, header reorder verification
metadata:
  type: project
---

Milestone-10 = KIP-932 share-consumer C FFI (+ later Python). Phase 1 = extract
shared `src/ffi/common.rs` (KafkaError handle + logger + staged async dispatcher)
out of `producer.rs`, add ffi-gated `BytesDeserializer` + optional `bytes` dep,
add `typedefs` to cbindgen `item_types`. Reviewed clean, no blocking issues.

**Why:** these are recurring review gotchas that apply to every FFI phase (2–6),
not one-offs.

**How to apply (FFI-phase review checklist):**

- **xtask lint has an ffi blind spot.** `cargo xtask lint` (`xtask/src/main.rs`)
  runs `cargo clippy --all-targets -- -D warnings` with **no** `--features ffi`.
  `mod ffi` is `#[cfg(feature="ffi")]` at `src/lib.rs`, so a green `xtask lint`
  proves *nothing* about `src/ffi/**`. Always independently run
  `cargo clippy --all-targets --features ffi -- -D warnings` when reviewing FFI
  code, and don't accept "lint passes" as coverage. Raised as should-fix Issue 1
  (suggest adding an ffi clippy pass to xtask lint). If a later phase closes it,
  drop this bullet.

- **FFI module gating shape.** `mod ffi` is gated once at `lib.rs`; the submodule
  declarations inside `src/ffi/mod.rs` (`producer`, `common`, future `consumer`/
  `share_consumer`) are **ungated** — matching the reference template
  (`origin/dev/c_and_python_consumer_bindings:src/ffi/mod.rs`). A per-submodule
  `#[cfg(feature="ffi")]` is redundant, not required. Don't flag an ungated
  submodule as a bug; the symbol surface is identical either way. (Phase-1
  PLAN.md text said to add the cfg; the Actor correctly dropped it.)

- **Header "pure reorder" verification recipe.** cbindgen output = generated
  `target/include/confluent_kafka.h`. To prove a move/refactor didn't change the
  C ABI: `wc -l` both, then `diff <(sort new) <(sort baseline)` — empty sorted
  diff + equal line count = same declaration set, ordering-only change (cbindgen
  emits in module-declaration order). Baseline was staged in scratchpad as
  `confluent_kafka.baseline.h`.

- **`[export] include` allowlist gates cbindgen output.** Adding `"typedefs"` to
  `item_types` does NOT leak new types: the allowlist still filters, and
  `pub(crate)` type aliases (the staged callback typedefs) aren't emitted. The
  `typedef struct {...} X;` lines in the header are opaque *structs*, always
  emitted regardless of the `typedefs` item-type.

- **Reference templates** live in scratchpad: `ref_common_ffi.rs` (~278 lines,
  the `common.rs` template — has everything except `SendUserData`),
  `ref_consumer_ffi.rs` (`SendUserData` is here at ~line 2555), `ref_mod_ffi.rs`,
  `ref_cbindgen.toml`. Staged async items (`CompletionJob`, `spawn_dispatcher`,
  `OperationCompletion`, etc.) are `#[allow(dead_code)]` until phase 2 wires them;
  grep-confirm they're unused before accepting the allow.

- **Commenting-guardrail watch:** the reference template carries
  "mirrors the librdkafka delivery-report model" — a borderline external-codebase
  reference per the user's global guardrail. Weak finding (design-rationale, not
  port-tracking); note as minor only.
