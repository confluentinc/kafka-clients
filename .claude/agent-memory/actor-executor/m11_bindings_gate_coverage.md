---
name: m11-bindings-gate-coverage
description: What each repo gate actually covers after the --features ffi closure — which cargo invocation reaches src/ffi, why `make verify` now includes test-rust, and how to run clippy/rustfmt/cmake on a host with no rustup
metadata:
  type: project
---

## The gate matrix, after closing the `ffi` hole

`src/ffi` is behind `#[cfg(feature = "ffi")]` (`src/lib.rs`), and for every slice
before this one **no gate compiled it** — ~25k lines of producer, consumer and
admin FFI were never linted, and `make verify` did not run the Rust suite at all.
Closed by four lines:

  - `cargo xtask lint` (and `lint_fix`) gained
    `cargo clippy --features ffi --all-targets -- -D warnings` as a second
    root-package arm.
  - `make test-rust` gained `cargo test --features ffi` after its `--workspace`
    line, and `test-rust` joined `verify` and `verify-sandbox`.

**No feature matrix is needed**: `ffi` is one cfg-gated module plus two
build-dependencies, so `default` and `default + ffi` is the whole space. Do
**not** write `cargo test --workspace --features ffi` — `ffi` is a root-package
feature the other members do not declare, so that form needs
`--features confluent-kafka-rust/ffi` or errors. Two root-only invocations are
simpler and sufficient.

Measured cost of the newly covered surface: **4 findings**, all
`clippy::byte_char_slices` in B5b's own test byte arrays. Producer and consumer
FFI were already clean, so `verify` went green rather than newly red. Run the new
arm once before wiring any similar gate in.

## Which invocation reaches what

| command | root pkg | `src/ffi` | other members |
|---|---|---|---|
| `cargo test` | yes | **no** | no |
| `cargo test --workspace` | yes | **no** | yes |
| `cargo test --features ffi` | yes | yes | no |
| `cargo clippy --all-targets` | yes | **no** | no |
| `cargo clippy --features ffi --all-targets` | yes | yes | no |

`cargo xtask check-generated` fails on a clean tree (unformatted generated files
under `target/`), so it is not a usable gate; the real ones are build (both
feature settings), test (both), `format-check`, clippy (all arms), and
`check-bindings`.

## Toolchain on this host: no rustup, so nix store paths directly

`rust-toolchain.toml` pins 1.95.0 but there is **no `rustup`**, and the
nix-profile `cargo`/`rustc` ship without `clippy`, `rustfmt` or `cmake`. Locate
them in the store and prepend to `PATH`:

```
ls -d /nix/store/*-clippy-* /nix/store/*-rustfmt-* /nix/store/*-cmake-* | grep -v drv
```

Caveats found the hard way:

  - **clippy and rustc versions must match.** Mixing nix clippy 1.97.1 with the
    profile's rustc 1.95.0 fails with `E0514 found crate ... compiled by an
    incompatible version of rustc` on the first workspace member. Use the whole
    1.97.1 triple (`clippy`, `cargo`, `rustc`) together, in a **separate**
    `CARGO_TARGET_DIR`, so it does not fight the 1.95 artifacts.
  - State the version deviation when reporting a clippy result: a lint the pinned
    1.95 has but 1.97 dropped would be invisible.
  - `cmake` is needed for `make devel-build-c` / `ctest`; the C suite otherwise
    fails with `cmake: command not found` at the configure step.
  - After a plain `cargo build` (no `ffi`), `target/debug/libconfluent_kafka.a`
    loses the FFI symbols and the C link fails with a wall of undefined
    `kafka_*`. Always re-run `cargo build --features ffi` immediately before
    `cmake --build`.
