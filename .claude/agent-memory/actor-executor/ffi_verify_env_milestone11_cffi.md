---
name: ffi-verify-env-milestone11-cffi
description: Verified FFI build/lint/C-test environment on branch milestone11-producer-transactions-CFFI — supersedes stale "no cmake"/"xtask lint skips ffi" claims in earlier phase notes
metadata:
  type: project
---

Verified on branch `milestone11-producer-transactions-CFFI` (2026-09, PR #168 work).
Two environment claims in [[ffi-callback-bridging-phase1]] are **stale on this branch**;
current reality:

1. **`cmake` IS installed** (`/opt/homebrew/bin/cmake`; gcc/clang/make too). The real C
   test flow works — no hand-rolled `cc` link line needed:
   - `make build-c` → `submodules` + `cargo build --all-features --release` + cmake
     configure/build of `bindings/c/build`. Takes ~2 min (release rebuild dominates).
   - `cd bindings/c/build && ctest --output-on-failure -R "mock_producer|kafka_producer"`.
   - **Gotcha:** CMake `find_library(CONFLUENT_KAFKA_RUST … PATHS target/release
     target/debug)` searches **release first**. A stale `target/release/libconfluent_kafka.a`
     is linked over a fresh debug one — so rebuild the *release* all-features lib (i.e.
     run `make build-c`, not just `cargo build --features ffi`) before the C tests, or
     they run against old code. `test_kafka_producer` is broker-free (uses an unreachable
     broker + `max.block.ms` timeouts on purpose).
   - Pre-existing `ld: warning … built for newer 'macOS' version (26.5) than being linked
     (26.0)` noise on link is harmless; targets still build.

2. **`cargo xtask lint` DOES cover `src/ffi/`.** xtask/src/main.rs `lint()` runs clippy
   **twice** — once `--workspace --all-targets` and once `--all-targets --all-features` —
   so the second pass sees the `ffi`-gated code. No separate `cargo clippy --features ffi`
   step is required for the lint gate (the user auto-memory `merge-verify-feature-gated-ffi`
   still applies to a plain `cargo build`, which does *not* compile ffi by default).

The still-true phase-1 facts: `#![deny(warnings)]` makes `dead_code` a hard error (guard
test-only `pub(crate)` with `#[allow(dead_code)]`); run the FULL verify set at phase start
to distinguish a pre-existing break from your own regression.

**Definition-of-Done command set that passed for an FFI-only change:**
`cargo build --features ffi`; `cargo test --features ffi --lib`; `cargo test --features ffi
--test producer` (mock, no broker); `cargo xtask format-check`; `cargo xtask lint`;
`make build-c` + producer `ctest`. Broker-dependent `tests/integration/*` (testcontainers/
Docker) are skippable and orthogonal to FFI-only glue changes — say so explicitly.
