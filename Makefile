RUST_PROJECT_ROOT = $(CURDIR)
ARCH := $(shell uname -m)

ifeq ($(ARCH),x86_64)
  # x86-64-v3: AVX/AVX2/BMI/FMA — Haswell and later (~2013+). Faster than the
  # baseline while staying portable across all v3-capable x86-64 hosts
  RUSTFLAGS_NATIVE = -C target-cpu=x86-64-v3
  CFLAGS_NATIVE = -march=x86-64-v3 -mtune=generic
else
  # No x86-64-vN equivalent on other arches (e.g. aarch64): the toolchain
  # default target-cpu is already the portable generic baseline, so don't
  # override it. -mtune=generic only affects scheduling, not compatibility.
  RUSTFLAGS_NATIVE =
  CFLAGS_NATIVE = -mtune=generic
endif

.PHONY: build build-all \
	build-rust build-rust-integration-tests build-rust-all-features \
	submodules build-c init-venv build-python \
	devel-build devel-build-rust devel-build-rust-integration-tests devel-build-rust-all-features \
	devel-build-c devel-build-python \
	build-grpc-images build-grpc-images-python build-grpc-images-c build-grpc-images-dotnet init init-hooks \
	test test-rust test-integration test-integration-python test-integration-c test-integration-dotnet \
	test-c test-python test-dotnet test-rust-all-features \
	test-integration-perf test-integration-perf-rust test-integration-perf-python \
	producer-perf-test producer-perf-test-c \
	consumer-perf-test-python producer-perf-test-python \
	producer-perf-test-dotnet consumer-perf-test-dotnet \
	verify verify-c verify-python verify-dotnet verify-rust \
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

# The two .NET gRPC images (sync + async), for the .NET-only multilanguage run
# below. Mirrors build-grpc-images-python (also two images). Needs only the Rust
# release artifacts: Dockerfile.grpc / Dockerfile.grpc.async copy
# target/release/libconfluent_kafka.so + target/include/confluent_kafka.h and
# build the .NET gRPC server inside the container (which brings its own SDK), so
# unlike build-grpc-images-python this does NOT depend on a host-side
# binding build. Images are platform-agnostic: CI is amd64-native; local
# Apple-Silicon dev sets DOCKER_DEFAULT_PLATFORM=linux/amd64 in its environment
# (the arm64 Grpc.Tools protoc segfaults), never a Makefile-baked --platform.
build-grpc-images-dotnet: build-rust-all-features
	$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image
	$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image-async

# One-shot setup for a fresh clone or worktree: pulls down the git
# submodules (kafka source reference + Unity for the C unit tests).
# Run this before `make build` on a new checkout.
init:
	@git submodule update --init --recursive
	@[ -d venv ] || python3 -m venv venv
	@(. venv/bin/activate && cd bindings/python && pip install .[dev])

test: test-rust-all-features test-c test-python

test-rust: build-rust
	cargo test

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

test-integration-python: build-grpc-images-python
	cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_python

test-integration-c: build-grpc-images-c
	cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_c

# .NET multilanguage integration arm. The `__grpc_dotnet` filter matches both
# the sync (`__grpc_dotnet`) and async (`__grpc_dotnet_async`) backends — exactly
# as `__grpc_python` matches both python arms — so one target covers both dotnet
# gRPC images. To run only the sync backend, add `--skip __grpc_dotnet_async`.
test-integration-dotnet: build-grpc-images-dotnet
	cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet

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

# Env-driven .NET producer/consumer performance benchmarks (manual; need a
# reachable broker via BOOTSTRAP_SERVERS). Delegate into bindings/dotnet — its
# own producer-perf-test-dotnet / consumer-perf-test-dotnet do the two-stage
# native build (cargo --features ffi) then `dotnet run` the PerfV3 exe, so there
# is no build-rust prerequisite here (mirrors how test-dotnet delegates below).
# Pass RUST_PROJECT_ROOT so the delegated cargo/dotnet resolve the same repo root.
# NOTE (Slice 2): test-integration-perf-dotnet (the Docker in-suite smoke) is
# deliberately NOT wired at the root yet — it lands with Slice 2.
producer-perf-test-dotnet:
	$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) producer-perf-test-dotnet

consumer-perf-test-dotnet:
	$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) consumer-perf-test-dotnet

# Delegates to bindings/c's own `test` (ctest + the C-backend multilanguage arm)
# instead of reimplementing the ctest invocation here, mirroring how
# `test-python` delegates to bindings/python. Passes the same
# RUST_PROJECT_ROOT / CFLAGS_EXTRA as `build-c` so the delegated cmake configure
# matches and does not reconfigure the build dir with different flags.
test-c: build-c
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) CFLAGS_EXTRA="$(CFLAGS_NATIVE)" test

test-python: build-python
	@(. venv/bin/activate && \
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=release test)

# Delegates to bindings/dotnet's own `test-dotnet` (native cargo build -> dotnet
# build matrix -> dotnet format -> unit tests on net8.0 + net10.0), mirroring how
# test-python / test-c delegate to their bindings. The delegated build does the
# Rust native step itself (CLAUDE.md §7.1 two-stage order), so no build-rust
# prerequisite here. Passes RUST_PROJECT_ROOT so the delegated cargo/dotnet
# invocations resolve the same repo root.
test-dotnet:
	$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) test-dotnet

verify: build format-check lint test

verify-c: test-c

verify-python: test-python
	$(MAKE) test-integration-perf-python

# verify-dotnet = build + format + unit(net8+net10) + integration(__grpc_dotnet
# [_async]). Deliberately has NO performance stage — the one shape difference
# from verify-python (which appends test-integration-perf-python above): .NET has
# no broker-based p99 latency suite, its allocation-budget assertions live in the
# unit suite (Decision 4). Recipe line rather than a prerequisite so integration
# runs strictly after the unit gate, matching verify-python's ordering.
verify-dotnet: test-dotnet
	$(MAKE) test-integration-dotnet

verify-rust: build-rust-all-features format-check lint test-rust-all-features
	$(MAKE) test-integration-perf-rust

verify-sandbox: build-rust build-c format-check lint test-integration test-c

init-hooks:
	@git config core.hooksPath .githooks
	@chmod +x .githooks/pre-commit

format-check:
	cargo xtask format-check

lint:
	cargo xtask lint

clean:
	cargo clean
	rm -rf bindings/c/build
	@(. venv/bin/activate && \
	$(MAKE) -C bindings/python clean)
