---
name: macos-verify-docker-multilang-blocked
description: macOS multilanguage Docker arms — the DEFAULT `make verify` arms self-skip, but the `*.macos` Dockerfiles + `-macos` make targets ARE a working path that builds everything inside the container; also pins the `build-python` root cause (host python3=3.9.6 < pyproject requires-python >=3.10)
metadata:
  type: project
---

**CORRECTION (2026-09-01): the multilanguage Docker arms DO run on this macOS
host now.** The earlier version of this note said they were "fundamentally
un-runnable ... needs Manager sign-off" — that is STALE. The repo added
`*.macos` Dockerfiles that build everything inside the Linux container, and I
ran `test_consume_transform_produce_with_offsets` green on all four backends
(`__rust`, `__grpc_python`, `__grpc_python_async`, `__grpc_c`) plus the two
other txn scenarios on this arm64 macOS box.

**Still true: the DEFAULT arms self-skip on macOS.** `make verify`'s
`test-integration-python` / `test-integration-c` guard on `uname -s != Linux`
and print a SKIP, because `Dockerfile.grpc` / `Dockerfile.grpc.async` COPY the
*host-built* `target/release/libconfluent_kafka.{so,a}` — Mach-O / absent on
macOS. That guard is honest and unchanged.

**Root cause of the `build-python` denial (verified 2026-09-08):** a plain
`make verify` fails earlier than the Docker arms — at `build-python`'s
`pip install -e .` — with `ERROR: Package 'confluent-kafka-rust-python' requires
a different Python: 3.9.6 not in '>=3.10'`. The host default `python3` is 3.9.6;
`bindings/python/pyproject.toml` line 9 declares `requires-python = ">=3.10"`;
and the `build-python` target hardcodes `python3` for the venv, so it ignores the
`python3.12` that IS on PATH. `make verify` therefore exits 2 at `build`
(before `format-check`/`lint`/`test`/`check-bindings` run). Everything upstream
passes clean: `build-rust-all-features` (release) compiles, and `build-c` builds
all C targets to 100% (`producer_perf_test skipped: librdkafka not found` is an
optional target, not a failure). To exercise the Rust + C arms without the Python
blocker, run them directly: the Rust quartet (`cargo build`, `cargo test`,
`cargo xtask format-check`, `cargo xtask lint`, `cargo xtask check-bindings`) and
`make test-c` (runs `ctest` over 7 self-contained FFI unit tests — no broker;
`test-integration-c` self-skips on Darwin). If a future task truly needs the local
pytest arm, create the venv with `python3.12` by hand rather than editing the
pinned `requires-python`.

**The working macOS path — `*.macos` Dockerfiles build the lib in-container.**
`bindings/python/Dockerfile.grpc.macos`, `Dockerfile.grpc.async.macos`, and
`bindings/c/Dockerfile.grpc.macos` have a Stage 1 (`debian:trixie-slim`) that
runs `cargo build --release --features ffi` from source, Stage 2 rebuilds the C
extension / C server from source (so binding-source edits ARE picked up — the
gRPC-handler and `_confluentkafka.c` changes flow through), Stage 3 = runtime.
No host artifact copy, so no Mach-O problem. Their `.dockerignore`
(`<Dockerfile>.dockerignore`, BuildKit per-Dockerfile convention) excludes
`target/ .git/ kafka/ …`, so the context is ~23 MB and Stage 1 does a clean cold
build (~5-8 min first time; rustup fetches pinned 1.95.0).

Recipe that worked (Docker Desktop 29.x, buildkit default):
1. Build the images by invoking the per-binding sub-targets DIRECTLY, NOT the
   root `build-grpc-images-python-macos` / `test-integration-*-macos` targets:
   those depend on `build-python` (→ `pip install -e .`, DENIED here). Instead:
     - `make -C bindings/python RUST_PROJECT_ROOT=<repo> grpc-image-macos`
     - `make -C bindings/python RUST_PROJECT_ROOT=<repo> grpc-image-async-macos`
     - `make -C bindings/c     RUST_PROJECT_ROOT=<repo> grpc-image-macos`
   Tags: `confluent-kafka-rust/python-grpc-server:dev`,
   `…/python-async-grpc-server:dev`, `…/c-grpc-server:dev` (what
   `backend_pool.rs` `docker run`s). Stage 1 is BYTE-IDENTICAL across all three
   Dockerfiles AND the `.dockerignore`s match, so build the sync Python image
   first (one cold Rust build) and the async + C images then reuse the cached
   `rust-builder` layer (seconds / a cmake compile). Run detached; don't pipe
   through `tail` (masks exit code).
2. Run the tests directly (broker via testcontainers — `apache/kafka:4.2.0`
   already pulled; `backend_pool` starts the gRPC container on the broker net):
   `cargo test --features integration-tests,multilanguage-tests --test integration -- <name-filter>`
   Multiple positional filters are OR'd. `--skip __grpc_c` excludes the C arm if
   its image isn't built. `--test-threads=N` caps concurrent brokers.

**The `# syntax=docker/dockerfile:1.7` frontend resolves fine now** on this host
(`#2 resolve image config for docker-image://docker.io/docker/dockerfile:1.7 …
CACHED`). The sibling note [[multilanguage-suite-on-macos]]'s "cannot resolve /
strip the syntax line" workaround is STALE — do not strip it.

See [[multilanguage-suite-on-macos]] (older manual cross-build recipe, now
superseded by the `*.macos` Dockerfiles), [[integration-test-infra]],
[[python-bindings-local-build-test-macos]].
