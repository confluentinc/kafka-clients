---
name: merge-reconciliation-test-impl-hazard
description: Cross-side merge hazard — when adopting one branch's impl, the OTHER branch's auto-merged tests can require impl you're about to drop
metadata:
  type: feedback
---

When reconciling a deep merge conflict by "adopt branch A's impl, re-apply
branch B's features," auto-merged test files from branch B can encode a
behavioral contract that requires a piece of branch B's impl you were told to
drop. Compiling + lint + A's own tests all pass, yet B's test fails at runtime.

**Why:** conflicts are per-file. A conflicted impl file (resolved toward A) and a
*non-conflicted* test file (auto-merged from B, since only B touched it) land
together. B's test exercises B's contract; A's impl doesn't satisfy it.

**Concrete instance (Milestone-11 CFFI←master merge, `src/ffi/producer.rs`):**
The plan said adopt master's submission loop (`let _ = producer.send(...).await`,
drop CFFI's shared-`fired` fire-on-error). But the auto-merged C test
`bindings/c/tests/test_mock_producer.c::test_send_async_on_closed_producer_fires_callback`
(CFFI-authored, "B1") requires the submission task to fire the callback when
`send` returns Err — the callback-obligation contract (CLAUDE.md §9.5). Master's
`let _` drops it → the one C test failed while everything else passed. Fix: port
CFFI's `fired`/`fire_error` (SendRequest carries `RecordCallbackTarget`, task
builds a guarded callback, fires on Err; shared AtomicBool makes it exactly-once).

**How to apply:**
- After a reconstruct-from-one-side resolution, run the FULL suite of the OTHER
  side (C + Python + integration), not just Rust unit + lint. `make verify`
  surfaces this; `cargo build`/`cargo test --lib` alone do NOT (the C/Python
  layers are separate).
- When the plan says "drop branch B's submission/callback rewrite," first grep
  B's tests for what that rewrite was added to satisfy. A test named
  `*_fires_callback` / `*_exactly_once` is a contract, not scaffolding.
- The FFI module is behind `#[cfg(feature = "ffi")]`; `cargo test --lib` runs 0
  ffi tests. Use `cargo test --features ffi --lib -- ffi::producer::tests`.
  cbindgen regenerates `target/include/confluent_kafka.h` only under
  `--all-features`/`--features ffi` builds (e.g. `cargo xtask lint`).
