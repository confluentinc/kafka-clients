# Rust client targets. Invoked by the repository root's cross-language
# Makefile (`make -C rust <target>`), or directly from this directory.
#
# Every cargo command runs from this directory: rust-toolchain.toml and
# .cargo/config.toml (the `xtask` alias) are looked up from the current
# directory, so `cargo --manifest-path rust/Cargo.toml` from elsewhere would
# lose both. `make -C rust` changes into it first, so the recipes below can
# call cargo directly.
ARCH := $(shell uname -m)

ifeq ($(ARCH),x86_64)
  # x86-64-v3 (Haswell+, ~2013): faster than baseline, portable across v3 hosts.
  RUSTFLAGS_NATIVE = -C target-cpu=x86-64-v3
else
  # Other arches (e.g. aarch64): default target-cpu is already the portable baseline.
  RUSTFLAGS_NATIVE =
endif

# macOS perf p99 budget (ms), used by test-integration-perf-macos.
MACOS_P99_LIMIT_MS ?= 150

.PHONY: build build-integration-tests build-all-features \
	devel-build devel-build-integration-tests devel-build-all-features \
	test test-all-features test-integration \
	test-integration-grpc-python test-integration-grpc-c \
	test-integration-perf test-integration-perf-macos producer-perf-test \
	format-check check-generated lint doc-check package-check install-rust-analyzer clean

build:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi --release
build-integration-tests:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi,integration-tests --release
build-all-features:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --all-features --release

devel-build:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi
devel-build-integration-tests:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi,integration-tests
devel-build-all-features:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --all-features

test: build
	# --workspace, not the root package alone: `cargo test` from the root only
	# builds and tests `confluent-kafka-rust`, so `generator` (the wire-protocol
	# code generator, 57 tests), `xtask`, `consumer-perf` and
	# `multilanguage-test-server` were never exercised by any gate. Feature
	# unification is not a hazard here: only `consumer-perf` depends on the root
	# package and it takes default features, so neither `integration-tests` nor
	# `ffi` is activated by the extra members.
	cargo test --workspace
	# And once more with `ffi` on: `src/ffi` is behind `#[cfg(feature = "ffi")]`,
	# so the C FFI modules' own unit tests (~500 across producer, consumer and
	# admin) are invisible to the run above. Not `--workspace --features ffi`:
	# `ffi` is a root-package feature the other members do not declare, so a
	# second root-only invocation is both simpler and sufficient.
	cargo test --features ffi

# The whole Rust test suite, compiled with every feature but running only the
# native-Rust tests: unit tests, the functional integration suite, and the
# `__rust` arm of the multilanguage suite. Every container-backed arm carries the
# `__grpc_` infix, so one `--skip __grpc` excludes all of them at once -- and any
# gRPC backend added later is excluded automatically rather than silently
# joining a job that has no image for it.
#
# The feature stays ON rather than being dropped, because it is the *compile*
# that matters: with `multilanguage-tests` off, the whole gRPC harness
# (`backend_pool`, `backend_factory`, `multilanguage_consumer`, the two test
# macros) is not built at all, and silently rots whenever the client API
# changes. Compiling it here catches that immediately, for the cost of one
# compile and no test runtime.
#
# Only the non-Rust backends are filtered, and only at runtime: they are the
# ones needing prebuilt gRPC images and a container per test. Each binding's
# own verify job owns its arm (`test-integration-grpc-python` /
# `test-integration-grpc-c` below, driven by python/'s and c/'s
# `test-integration`), so this job never builds Docker images it never starts.
#
# `__rust` is kept deliberately and costs nothing extra: its macro arm uses
# `RustNativeFactory` directly -- no container, no gRPC hop -- so it needs no
# image. It is also the control case for the other arms: a `__grpc_python` failure
# where `__rust` passes points at the binding or the bridge, not at the client
# or the harness.
#
# Part of the root `verify` (through `test`) and of the CI `verify-rust` job.
test-all-features: build-all-features
	cargo test --all-features -- --skip __grpc

# Functional integration tests only. The performance tests live in their own
# `performance` cargo test target (tests/performance/main.rs), so `--test
# integration` cannot schedule them alongside the functional suite.
test-integration: build-integration-tests
	cargo test --features integration-tests --test integration
	# The integration tests that drive crate internals live in the lib test
	# binary (src/integration_tests), since those types are not public API.
	cargo test --features integration-tests --lib -- integration_tests::

# ── Per-backend multilanguage integration tests ──────────────────────────
#
# The `multilanguage_test!` / `multilanguage_consumer_test!` macros expand each
# case into one test per backend, named `<case>__rust`, `<case>__grpc_python`,
# `<case>__grpc_python_async` and `<case>__grpc_c`. So a backend is selectable
# with a plain libtest name filter -- no cargo feature or env var needed.
#
# The shared `__grpc_` infix is what lets `test-all-features` exclude every
# container-backed arm with a single `--skip __grpc`; `__rust` deliberately
# lacks it.
#
# `__grpc_python` also matches `__grpc_python_async` (the latter contains it),
# which is intended: both exercise the Python binding, one through the sync API
# and one through the asyncio one. To run only the sync binding, add
# `--skip __grpc_python_async`.
#
# The `__rust` arm is not a target here: it needs no gRPC image and already runs
# as part of `test-all-features`.
#
# These only run the tests: the binding's gRPC image must already be built,
# which is what python/'s and c/'s `test-integration` do before calling them.
test-integration-grpc-python:
	cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_python

test-integration-grpc-c:
	cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_c

# Performance integration tests. Separated from the functional suites because
# they assert latency and throughput budgets: run concurrently with ~138
# functional tests they contend for CPU, memory and Docker, and their p99
# budgets fail for reasons unrelated to the code under test. Expects an
# otherwise-idle machine.
test-integration-perf: build-integration-tests
	cargo test --features integration-tests --test performance -- --nocapture

# macOS variant; the perf tail uses MACOS_P99_LIMIT_MS.
test-integration-perf-macos:
	P99_LIMIT_MS=$(MACOS_P99_LIMIT_MS) $(MAKE) test-integration-perf

# Env-driven producer performance benchmark. Configure via environment
# variables (BOOTSTRAP_SERVERS, VALUE_SIZE, LIMIT_RPS, TEST_DURATION_SECONDS,
# COMPRESSION_TYPE, P99_LIMIT_MS, ...); writes metrics.jsonl for plot_metrics.py.
# Keep in sync with the other producer performance tests in the project.
producer-perf-test: build
	cargo xtask producer-perf-test

format-check:
	cargo xtask format-check

# Checks the generated protocol code's formatting and that the checked-in
# error-code tables are current; it reads the build's output, so it follows
# a build in every verify target.
check-generated:
	cargo xtask check-generated

lint:
	cargo xtask lint

# Build the rustdoc of every workspace crate with warnings denied, so a broken
# or private intra-doc link fails CI instead of surfacing only when someone
# runs `cargo doc`. The doctests themselves run as part of `cargo test`.
doc-check:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

# Package the crate as it would be published, fail if the compressed `.crate`
# is above 3 MiB, and build a fresh project that depends on it.
package-check:
	cargo xtask package-check

# Opt-in editor/AI-assistant tooling setup: installs the rust-analyzer
# component for the toolchain pinned in rust-toolchain.toml, and, if the
# `claude` CLI is on PATH, the matching Claude Code LSP plugin. Not a
# prerequisite of `build`/`test` -- not every contributor's editor or
# workflow needs rust-analyzer, so this is run by hand.
install-rust-analyzer:
	rustup component add rust-analyzer
	@if command -v claude >/dev/null 2>&1; then \
		claude plugin install rust-analyzer-lsp@claude-plugins-official; \
	else \
		echo "claude CLI not found on PATH, skipping Claude Code rust-analyzer-lsp plugin install"; \
	fi

clean:
	cargo clean
