---
name: Integration test infrastructure patterns
description: How the integration test harness works — testcontainers setup, shared cluster pool, test file placement, feature gating
type: project
---

Integration tests live at `tests/integration_*.rs` (not in a subdirectory) because Rust only auto-discovers test crates at the `tests/` root.

All integration test code is gated behind `#[cfg(feature = "integration-tests")]` and `#![cfg(feature = "integration-tests")]`.

**Why:** Running `cargo test` without Docker must work for regular development. Feature flag `integration-tests` keeps Docker-dependent tests opt-in.

**How to apply:**
- New integration test files: create `tests/integration_<name>_test.rs` with `#![cfg(feature = "integration-tests")]` and `mod common;`
- Infra modules in `tests/common/` are also `#[cfg(feature = "integration-tests")]`
- `testcontainers::ImageExt::with_env_var` consumes the image and returns `ContainerRequest<I>` -- use `ContainerRequest::from(Image)` first, then chain `with_env_var` calls
- Bootstrap servers are obtained via `container.get_host_port_ipv4(apache::KAFKA_PORT)` then `format!("127.0.0.1:{port}")`
- Shared cluster pool uses `LazyLock<Mutex<HashMap>>` + `tokio::sync::OnceCell` for thread-safe lazy init
