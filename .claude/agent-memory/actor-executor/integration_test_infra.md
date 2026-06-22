---
name: Integration test infrastructure patterns
description: How the integration test harness works — testcontainers setup, shared cluster pool, test file placement, feature gating
type: project
---

Integration tests live under `tests/integration/` as a single binary crate; the crate root is `tests/integration/main.rs` (`#![cfg(feature = "integration-tests")]`) which declares each test file as `mod <name>_test;`. Add new files there. The test binary is named `integration` — run `cargo test --features integration-tests --test integration`.

`tests/integration/main.rs` does `#[path = "../common/mod.rs"] mod common;` so all files share `tests/common/`. Reference shared infra inside a test file via `use crate::common::...`.

(Historical note: an earlier layout used flat `tests/integration_*.rs` files; that is no longer the structure as of Phase 39.)

All integration test code is gated behind `#![cfg(feature = "integration-tests")]` at the crate root (main.rs) — individual test files do NOT need their own cfg attr.

**Why:** Running `cargo test` without Docker must work for regular development. Feature flag `integration-tests` keeps Docker-dependent tests opt-in.

**How to apply:**
- New integration test files: create `tests/integration/<name>_test.rs` and add `mod <name>_test;` to `tests/integration/main.rs`
- Infra modules in `tests/common/` are also `#[cfg(feature = "integration-tests")]`
- `cargo xtask lint` does NOT pass `--features integration-tests`, so it never lints these files. Self-check with `cargo clippy --features integration-tests --test integration`. `clippy --fix` edits OTHER tracked integration files — `git checkout` them to keep diff scoped.
- `testcontainers::ImageExt::with_env_var` consumes the image and returns `ContainerRequest<I>` -- use `ContainerRequest::from(Image)` first, then chain `with_env_var` calls
- Bootstrap servers are obtained via `container.get_host_port_ipv4(apache::KAFKA_PORT)` then `format!("127.0.0.1:{port}")`
- Shared cluster pool uses `LazyLock<Mutex<HashMap>>` + `tokio::sync::OnceCell` for thread-safe lazy init
