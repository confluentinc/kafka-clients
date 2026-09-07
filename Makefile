RUST_PROJECT_ROOT = $(CURDIR)
ARCH := $(shell uname -m)

ifeq ($(ARCH),x86_64)
  # x86-64-v3 (Haswell+, ~2013): faster than baseline, portable across v3 hosts.
  RUSTFLAGS_NATIVE = -C target-cpu=x86-64-v3
  CFLAGS_NATIVE = -march=x86-64-v3 -mtune=generic
else
  # Other arches (e.g. aarch64): default target-cpu is already the portable baseline.
  RUSTFLAGS_NATIVE =
  CFLAGS_NATIVE = -mtune=generic
endif

.PHONY: build build-all \
	build-rust build-rust-integration-tests build-rust-all-features \
	submodules build-c init-venv build-python \
	devel-build devel-build-rust devel-build-rust-integration-tests devel-build-rust-all-features \
	devel-build-c devel-build-python \
	build-grpc-images build-grpc-images-python build-grpc-images-c init init-hooks \
	build-grpc-images-python-macos build-grpc-images-c-macos \
	test test-rust test-integration test-integration-python test-integration-c \
	test-integration-python-macos test-integration-c-macos \
	test-c test-python test-rust-all-features \
	test-c-macos-docker test-python-macos-docker \
	test-integration-perf test-integration-perf-rust test-integration-perf-python \
	producer-perf-test producer-perf-test-c \
	consumer-perf-test-python producer-perf-test-python \
	verify verify-c verify-python verify-rust \
	verify-rust-macos-docker verify-python-macos-docker verify-c-macos-docker \
	verify-sandbox format-check lint clean

build: init-hooks build-all

build-all: build-rust-all-features build-c build-python

build-rust:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi --release
build-rust-integration-tests:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi,integration-tests --release
build-rust-all-features:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --all-features --release

submodules:
	git submodule update --init --recursive

build-c: submodules build-rust-all-features
	cmake -S bindings/c -B bindings/c/build -DRUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) -DCMAKE_C_FLAGS="$(CFLAGS_NATIVE)"
	cmake --build bindings/c/build

init-venv:
	[ -d venv ] || python3 -m venv venv

build-python: init-venv build-rust-all-features
	@(. venv/bin/activate && \
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=release CFLAGS_EXTRA="$(CFLAGS_NATIVE)" build)

devel-build: devel-build-rust devel-build-c devel-build-python

devel-build-rust:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi
devel-build-rust-integration-tests:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi,integration-tests
devel-build-rust-all-features:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --all-features

devel-build-c: submodules devel-build-rust
	cmake -S bindings/c -B bindings/c/build -DRUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) -DCMAKE_C_FLAGS="$(CFLAGS_NATIVE)"
	cmake --build bindings/c/build

devel-build-python: init-venv devel-build-rust
	@(. venv/bin/activate && \
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=debug CFLAGS_EXTRA="$(CFLAGS_NATIVE)" build)

# Build the per-language gRPC server Docker images used by the
# multilanguage integration test harness. See
# design/history/MILESTONE-6/DESIGN-multilanguage-tests.md.
# Depends on the existing `build` target so libconfluent_kafka.{a,so}
# and confluent_kafka.h are present under target/release/.
build-grpc-images: build
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image-async
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image
	$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image
	$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image-async

# Just the two Python gRPC images (sync + asyncio), for the Python-only
# multilanguage run below. Skips the C image, which that run never starts.
build-grpc-images-python: build-rust-all-features build-python
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image-async

# Just the C gRPC image, for the C-only multilanguage run below. Skips the two
# Python images, which that run never starts.
#
# Needs only the Rust release artifacts: Dockerfile.grpc copies
# target/release/libconfluent_kafka.a and target/include/confluent_kafka.h, then
# runs cmake on bindings/c/grpc_server inside the container.
#
# Hence the deliberate asymmetry with `build-grpc-images-python`, which also
# needs `build-python`: the Python image installs the built wheel, whereas the
# host-side `build-c` (cmake for the C unit tests) contributes nothing the C
# image consumes.
build-grpc-images-c: build-rust-all-features
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image

# macOS variants: build libconfluent_kafka inside the image (Dockerfile.grpc.macos
# et al), since the host build is Mach-O and unusable in the Linux gRPC images.
build-grpc-images-python-macos: build-python
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image-macos
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image-async-macos

build-grpc-images-c-macos:
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image-macos

# One-shot setup for a fresh clone or worktree: pulls down the git
# submodules (kafka source reference + Unity for the C unit tests).
# Run this before `make build` on a new checkout.
init:
	@git submodule update --init --recursive
	@[ -d venv ] || python3 -m venv venv
	@(. venv/bin/activate && cd bindings/python && pip install .[dev])

test: test-rust-all-features test-c test-python

test-rust: build-rust
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
# own verify job owns its arm: `test-integration-python` (invoked from
# bindings/python's `test`) and `test-integration-c` (from bindings/c's
# `test`). Dropping `build-grpc-images` from this target is what makes that
# split real: this job no longer builds three Docker images it never starts.
#
# `__rust` is kept deliberately and costs nothing extra: its macro arm uses
# `RustNativeFactory` directly -- no container, no gRPC hop -- so it needs no
# image. It is also the control case for the other arms: a `__grpc_python` failure
# where `__rust` passes points at the binding or the bridge, not at the client
# or the harness.
#
# Part of `verify` (through `test`) and of the CI `verify-rust` job.
test-rust-all-features: build-rust-all-features
	cargo test --all-features -- --skip __grpc

# Functional integration tests only. The performance tests live in their own
# `performance` cargo test target (tests/performance/main.rs), so `--test
# integration` cannot schedule them alongside the functional suite.
test-integration: build-rust-integration-tests
	cargo test --features integration-tests --test integration

# ── Per-backend multilanguage integration tests ──────────────────────────
#
# The `multilanguage_test!` / `multilanguage_consumer_test!` macros expand each
# case into one test per backend, named `<case>__rust`, `<case>__grpc_python`,
# `<case>__grpc_python_async` and `<case>__grpc_c`. So a backend is selectable
# with a plain libtest name filter -- no cargo feature or env var needed.
#
# The shared `__grpc_` infix is what lets `test-rust-all-features` exclude every
# container-backed arm with a single `--skip __grpc`; `__rust` deliberately
# lacks it.
#
# `__grpc_python` also matches `__grpc_python_async` (the latter contains it),
# which is intended: both exercise the Python binding, one through the sync API
# and one through the asyncio one. To run only the sync binding, add
# `--skip __grpc_python_async`.
#
# The `__rust` arm is not a target here: it needs no gRPC image and already runs
# as part of `test-rust-all-features` / `verify-rust`.

# ── Cross-arch guard for the container-backed arms ───────────────────────
#
# Dockerfile.grpc (both bindings) COPY the *host-built* release artifacts
# straight into a Linux container and link them with the container's GNU ld:
# the Python images copy `target/release/libconfluent_kafka.so`, the C image
# copies `target/release/libconfluent_kafka.a` (+ `target/include/confluent_kafka.h`).
# On a non-Linux host those artifacts are the wrong object format — Mach-O on
# macOS, and no ELF `.so` is produced at all — so the image build/link fails,
# and even if it linked the binary could not run. Cross-compiling host->Linux
# is out of scope for a dev-box `make verify`.
#
# So off Linux these two arms SELF-SKIP with a loud notice and exit 0. They are
# NOT skipped in CI: CI runs `make verify` on Linux, where `uname -s` == Linux,
# the artifacts are native ELF, and the images build and run unchanged. The
# skip is honest — it never claims the container arms passed, only that this
# host cannot build the Linux images.
#
# The `build-grpc-images-*` image build is invoked *inside* the recipe (rather
# than as a prerequisite) precisely so the skip also short-circuits the Docker
# build: a prerequisite would run before the recipe and fire the failing image
# build before the guard could stop it.
test-integration-python:
	@if [ "$$(uname -s)" != "Linux" ]; then \
		printf '\n========================================================================\n'; \
		printf 'SKIP test-integration-python: host is %s, not Linux.\n' "$$(uname -s)"; \
		printf '\n'; \
		printf 'The Python gRPC-server images COPY the host-built\n'; \
		printf '  target/release/libconfluent_kafka.so\n'; \
		printf 'into a Linux container and link it with GNU ld. On this host that\n'; \
		printf 'artifact is Mach-O / absent (no ELF .so), so the image cannot build.\n'; \
		printf 'This container arm runs only in CI'"'"'s Linux verify-python job.\n'; \
		printf '========================================================================\n\n'; \
	else \
		$(MAKE) build-grpc-images-python && \
		cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_python; \
	fi

test-integration-c:
	@if [ "$$(uname -s)" != "Linux" ]; then \
		printf '\n========================================================================\n'; \
		printf 'SKIP test-integration-c: host is %s, not Linux.\n' "$$(uname -s)"; \
		printf '\n'; \
		printf 'The C gRPC-server image COPYs the host-built\n'; \
		printf '  target/release/libconfluent_kafka.a\n'; \
		printf 'into a Linux container and links it with GNU ld. On this host that\n'; \
		printf 'artifact is Mach-O, so the image cannot build/link.\n'; \
		printf 'This container arm runs only in CI'"'"'s Linux verify-c job.\n'; \
		printf '========================================================================\n\n'; \
	else \
		$(MAKE) build-grpc-images-c && \
		cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_c; \
	fi

# macOS variants of the two targets above.
test-integration-python-macos: build-grpc-images-python-macos
	cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_python

test-integration-c-macos: build-grpc-images-c-macos
	cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_c

# ── Performance integration tests ────────────────────────────────────────
#
# Separated from the functional suites because they assert latency and
# throughput budgets: run concurrently with ~138 functional tests they contend
# for CPU, memory and Docker, and their p99 budgets fail for reasons unrelated
# to the code under test. Both targets expect an otherwise-idle machine.

test-integration-perf-rust: build-rust-integration-tests
	cargo test --features integration-tests --test performance -- --nocapture

test-integration-perf-python: build-python
	@(. venv/bin/activate && \
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=release test-performance)

# Rust and Python performance suites, one after the other. Recipe lines rather
# than prerequisites so the order holds under `make -j`, and so the two
# suites never overlap — each needs the machine to itself.
test-integration-perf:
	$(MAKE) test-integration-perf-rust
	$(MAKE) test-integration-perf-python

# Env-driven producer performance benchmark. Configure via environment
# variables (BOOTSTRAP_SERVERS, VALUE_SIZE, LIMIT_RPS, TEST_DURATION_SECONDS,
# COMPRESSION_TYPE, P99_LIMIT_MS, ...); writes metrics.jsonl for plot_metrics.py.
# Keep in sync with the other producer performance tests in the project.
producer-perf-test: build-rust
	cargo xtask producer-perf-test

# C producer performance benchmark (opt-in; needs a reachable broker). Same
# env-var contract as `producer-perf-test`; select the backend with
# CLIENT_VERSION (3 = Rust client C bindings, default; 2 = librdkafka baseline).
# Built by `build-c` but intentionally not run under `make test-c`.
producer-perf-test-c: build-c
	./bindings/c/build/producer_perf_test

# Env-driven Python consumer end-to-end latency benchmark (standalone). Mirrors
# consumer-perf/compare/benchmark_e2e_latency.c. Needs a reachable broker via
# BOOTSTRAP_SERVERS; set KAFKA_BIN to a Kafka bin dir to self-spawn load
# (kafka-producer-perf-test.sh), otherwise runs consume-only. Select the backend
# with CLIENT_VERSION (3 = Rust binding, default; 2 = librdkafka baseline). Set
# ASYNC=True to drive the asyncio-native consumer of the selected backend
# (AsyncKafkaConsumer / confluent_kafka.aio.AIOConsumer) instead of the sync one.
consumer-perf-test-python: build-python
	@(. venv/bin/activate && \
	  python $(RUST_PROJECT_ROOT)/bindings/python/test/performance/consumer_performance_test.py)

# Env-driven Python producer performance benchmark (standalone). Same env-var
# contract as `producer-perf-test`.
producer-perf-test-python: build-python
	@(. venv/bin/activate && \
	  python $(RUST_PROJECT_ROOT)/bindings/python/test/performance/producer_performance_test.py)

# Delegates to bindings/c's own `test` (ctest + the C-backend multilanguage arm)
# instead of reimplementing the ctest invocation here, mirroring how
# `test-python` delegates to bindings/python. Passes the same
# RUST_PROJECT_ROOT / CFLAGS_EXTRA as `build-c` so the delegated cmake configure
# matches and does not reconfigure the build dir with different flags.
test-c: build-c
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) CFLAGS_EXTRA="$(CFLAGS_NATIVE)" test

# macOS variant of test-c.
test-c-macos-docker: build-c
	cd bindings/c/build && ctest --output-on-failure
	$(MAKE) test-integration-c-macos

test-python: build-python
	@(. venv/bin/activate && \
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=release test)

# macOS variant of test-python.
test-python-macos-docker: build-python
	@(. venv/bin/activate && \
	cd $(RUST_PROJECT_ROOT)/bindings/python && \
	(pip install .[dev] || pip install --no-dependencies .[dev]) && \
	python -m pytest test/unit -v)
	$(MAKE) test-integration-python-macos

verify: build format-check lint test check-bindings

verify-c: test-c

verify-c-macos-docker: test-c-macos-docker

verify-python: test-python check-bindings
	$(MAKE) test-integration-perf-python

# macOS perf p99 budget (ms)
MACOS_P99_LIMIT_MS ?= 150

# macOS verify-python; perf tail uses MACOS_P99_LIMIT_MS.
verify-python-macos-docker: test-python-macos-docker
	P99_LIMIT_MS=$(MACOS_P99_LIMIT_MS) $(MAKE) test-integration-perf-python

verify-rust: build-rust-all-features format-check lint test-rust-all-features
	$(MAKE) test-integration-perf-rust

# macOS verify-rust; perf tail uses MACOS_P99_LIMIT_MS.
verify-rust-macos-docker: build-rust-all-features format-check lint test-rust-all-features
	P99_LIMIT_MS=$(MACOS_P99_LIMIT_MS) $(MAKE) test-integration-perf-rust

verify-sandbox: build-rust build-c format-check lint test-integration test-c

init-hooks:
	@git config core.hooksPath .githooks
	@chmod +x .githooks/pre-commit

format-check:
	cargo xtask format-check

lint:
	cargo xtask lint

# Static arity check of the hand-written CPython extension's variadic calls.
# A Py_BuildValue / PyArg_Parse* format one unit short of its argument list
# compiles silently and reads a garbage pointer at run time; for every admin
# RPC that Java's MockAdminClient leaves unsupported, the affected drain's
# success path is unreachable from the test suite, so this defect class must
# be caught statically. Needs no build artifacts, so it is cheap to run.
#
# The scanner's own unit tests run first: `cargo test` at the workspace root
# only tests the root package, so nothing else exercises them, and a gate is
# only worth as much as the parser behind it.
check-bindings:
	# Kept even though `test-rust` is now `--workspace`, so that
	# `make check-bindings` on its own still exercises the scanner's own tests
	# before trusting its verdict.
	cargo test -p xtask
	cargo xtask check-bindings

clean:
	cargo clean
	rm -rf bindings/c/build
	@(. venv/bin/activate && \
	$(MAKE) -C bindings/python clean)
