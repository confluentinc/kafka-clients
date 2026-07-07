---
name: build-environment
description: This dev host has no Rust toolchain installed; how builds/tests/lint were run for Milestone 9
metadata:
  type: reference
---

As of 2026-07-07 this machine had **no host Rust toolchain** — `cargo`, `rustc`,
`rustup` were all absent (no `~/.cargo`, no `~/.rustup`, no `target/`), even via a
login shell or with the sandbox disabled. `edition = "2024"` needs Rust ≥ 1.85.

**Decision (user, relayed by coordinator): do NOT install rustup on the host; run all
`cargo`/`xtask` inside Docker.** (A rustup install was briefly triggered by an earlier
"go ahead" before the override arrived — it can be removed with `rustup self uninstall -y`.)

Docker 29.4.2 is available. The stock `rust:1` image is **minimal — it lacks rustfmt and
clippy**, so `cargo fmt`/`cargo clippy`/`cargo xtask format|lint` fail on it. Build a
derived image once:

```
FROM rust:1
RUN rustup component add rustfmt clippy
```
`docker build -t m9-rust ...`

Run as the **host user (`--user 501:20`)** so nothing under the bind-mounted repo (`/work`)
becomes root-owned (`cargo fmt` rewrites source in place). Use chowned named volumes for
`CARGO_HOME` (`/cargo-home`) and `/work/target` (fast on macOS, keeps the repo clean). A
helper script `dcargo.sh` wrapping `docker run --rm --user 501:20 -e CARGO_HOME=/cargo-home
-v m9-cargo-home:/cargo-home -v m9-target:/work/target -v <repo>:/work -w /work m9-rust "$@"`
was kept in the session scratchpad. A clean full build was ~37s; incremental ~7-10s; full
`cargo test --lib` ~13s.

Git stays on the host (commits/branch); only build/test/format/lint go through the container.
Default `cargo test` excludes Docker integration tests (feature `integration-tests`) — no
Docker-in-Docker, so unit tests (`cargo test --lib`) are the bar.
