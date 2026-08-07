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
	build-grpc-images init init-hooks \
	test test-rust test-integration test-c test-python test-rust-all-features \
	test-integration-perf test-integration-perf-rust test-integration-perf-python \
	producer-perf-test producer-perf-test-c \
	consumer-perf-test-python producer-perf-test-python \
	verify verify-c verify-python verify-rust-and-multilanguage-integration-tests \
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

# The whole Rust test suite with every feature enabled. `--all-features`
# turns on `multilanguage-tests`, which runs the producer integration suite
# three times — once per backend (rust / python / c). Those backends are
# docker-run from prebuilt gRPC images via testcontainers, so the images
# must already exist locally: hence the `build-grpc-images` prerequisite.
#
# Part of `verify` (through `test`) and of the CI
# `verify-rust-and-multilanguage-integration-tests` job.
test-rust-all-features: build build-grpc-images
	cargo test --all-features

# Functional integration tests only. The performance tests live in their own
# `performance` cargo test target (tests/performance/main.rs), so `--test
# integration` cannot schedule them alongside the functional suite.
test-integration: build-rust-integration-tests
	cargo test --features integration-tests --test integration

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

test-c: build-c
	cd bindings/c/build && ctest --output-on-failure

test-python: build-python
	@(. venv/bin/activate && \
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=release test)

verify: build format-check lint test

verify-c: test-c

verify-python: test-python
	make test-integration-perf-python

verify-rust-and-multilanguage-integration-tests: build-rust-all-features format-check lint test-rust-all-features
	make test-integration-perf-rust

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
