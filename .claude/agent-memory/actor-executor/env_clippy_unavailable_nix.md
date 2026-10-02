---
name: env_clippy_unavailable_nix
description: cargo clippy / cargo xtask lint cannot run in this sandbox's nix-profile Rust toolchain (rustc 1.95.0, no rustup, no matching clippy component)
metadata:
  type: project
---

`cargo xtask lint` requires `cargo clippy`, but this sandbox's Rust toolchain
comes from `/Users/pratyush/.nix-profile` (rustc 1.95.0, cargo 1.95.0) with no
`rustup` managing it, so there is no `clippy` component to add via the normal
`rustup component add clippy`.

A clippy derivation IS present in the nix store
(`/nix/store/*-clippy-1.97.1/bin/cargo-clippy`), but it is built against
**rustc 1.97.1**, not the 1.95.0 that `cargo`/`rustc` on PATH actually use.
Running it (even with a fresh `CARGO_TARGET_DIR`) fails with `E0514: found
crate compiled by an incompatible version of rustc` on the very first
workspace dependency (`generator`'s `serde`/`regex` deps) — `cargo clippy`
builds dependencies with the ambient `cargo`/`rustc` (1.95.0) and only swaps
in `clippy-driver` for workspace-member crates, so the two are unbridgeably
version-skewed. `nix profile install nixpkgs#rustup` also fails: it conflicts
with the existing `home-manager-path` profile entry (same `rustdoc` binary
path) — do not force this without asking the user, since it mutates a
profile shared with the user's other work.

**Why:** discovered while trying to run `cargo xtask lint` per the Definition
of Done for a normal Actor task (Milestone: admin per-key async FFI callbacks,
2026-09-15) — not a one-off fluke, this is the standing state of the sandbox
image.

**How to apply:** don't burn time re-diagnosing this each session. Treat
`cargo xtask lint` as unavailable in this environment; rely on
`cargo build`/`cargo test` (the crate has `#![deny(warnings)]` at
`src/lib.rs:16`, so a clean `cargo build` already catches unused
imports/dead-code/etc. at the rustc-warning level) plus careful manual review
for clippy-style idioms (needless clones, redundant closures, etc.), and
explicitly flag in the final report that `cargo xtask lint` could not be run
and should be verified in CI or a properly provisioned toolchain before merge.
Don't attempt `nix profile install` fixes without the user's explicit go-ahead.

**Note on persistence:** a prior write of this same file to the *main*
checkout's `.claude/agent-memory/actor-executor/` (not this worktree's copy)
vanished between sessions — the main tree is shared with other concurrent
Actor sessions (see `admin_per_key_callbacks_phase_a_notes.md`'s note on
worktree isolation), and an untracked file there is vulnerable to another
session's `git clean`/`checkout`. Prefer writing memory into whichever
checkout (worktree or main) you are actually committing work from, and if
working from an isolated worktree, consider that the durable copy for this
session — but be aware it will not automatically propagate back to the main
tree or other worktrees until/unless the memory directory itself is committed
and merged.
