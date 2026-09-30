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
	build-grpc-images build-grpc-images-python build-grpc-images-c build-grpc-images-dotnet init init-hooks \
	test test-rust test-integration test-integration-python test-integration-c test-integration-dotnet \
	test-c test-python test-dotnet test-rust-all-features \
	test-c-macos-docker test-python-macos-docker test-dotnet-macos-docker \
	test-integration-perf test-integration-perf-rust test-integration-perf-python \
	test-integration-perf-dotnet \
	producer-perf-test producer-perf-test-c \
	consumer-perf-test-python producer-perf-test-python \
	producer-perf-test-dotnet consumer-perf-test-dotnet \
	verify verify-c verify-python verify-dotnet verify-rust \
	verify-rust-macos-docker verify-python-macos-docker verify-c-macos-docker \
	verify-dotnet-macos-docker \
	verify-sandbox format-check lint install-rust-analyzer clean

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

# .NET multilanguage integration arm. The `__grpc_dotnet` filter matches both
# the sync (`__grpc_dotnet`) and async (`__grpc_dotnet_async`) backends — exactly
# as `__grpc_python` matches both python arms — so one target covers both dotnet
# gRPC images. To run only the sync backend, add `--skip __grpc_dotnet_async`.
#
# Non-Linux hosts skip, for the same reason and by the same mechanism as
# `test-integration-python` / `test-integration-c` — see the shared rationale
# above `test-integration-python`, including why `build-grpc-images-dotnet` is
# invoked INSIDE the recipe instead of as a prerequisite (a prerequisite runs
# before the recipe, so it would fire the failing image build before the guard
# could stop it).
#
# Unlike python and c there is deliberately NO `-macos` counterpart to fall back
# to: .NET has no Dockerfile.grpc.macos, because Grpc.Tools ships an arm64 protoc
# that SIGSEGVs during C# codegen, so the images cannot be built on an arm64
# host at all. Skipping is the only option here; the container arm runs in CI's
# amd64 Linux verify-dotnet job (see .semaphore/semaphore.yml).
#
# THREE TESTS ARE SKIPPED FOR .NET ONLY -- the producer transaction arms.
# `multilanguage_test!` emits one arm per backend for every test it wraps, so the
# three transaction tests in tests/integration/producer_transactions_test.rs
# generate __grpc_dotnet / __grpc_dotnet_async arms like every other backend. The
# .NET gRPC server cannot serve them: ProducerServiceImpl /
# AsyncProducerServiceImpl implement 8 RPCs and none of them are the five
# transaction RPCs (InitTransactions, BeginTransaction, CommitTransaction,
# AbortTransaction, SendOffsetsToTransaction) that producer_service.proto added,
# so those arms return gRPC UNIMPLEMENTED. The binding has no transaction surface
# at all yet -- IAsyncProducer's own docs record it as deferred.
#
# The skip is scoped to THIS target, which only ever runs __grpc_dotnet*, so no
# other backend loses coverage: python / c / rust still run all three.
#
# REMOVE THESE THREE LINES when the .NET producer reaches transaction parity with
# Python (the 10 transaction P/Invokes, the public surface on
# IProducer/IAsyncProducer and the four producer types, the three MockProducer
# transaction controls, and the five RPCs in both producer servicers). Deleting
# them is the last step of that phase, not a follow-up to it.
test-integration-dotnet:
	@if [ "$$(uname -s)" != "Linux" ]; then \
		printf '\n========================================================================\n'; \
		printf 'SKIP test-integration-dotnet: host is %s, not Linux.\n' "$$(uname -s)"; \
		printf '\n'; \
		printf 'The .NET gRPC-server images COPY the host-built\n'; \
		printf '  target/release/libconfluent_kafka.so\n'; \
		printf 'into a Linux container. On this host that artifact is\n'; \
		printf 'Mach-O / absent (no ELF .so), so the image cannot build.\n'; \
		printf '\n'; \
		printf 'There is no -macos variant for .NET (Grpc.Tools arm64 protoc\n'; \
		printf 'SIGSEGVs during C# codegen), so this arm runs only in CI'"'"'s\n'; \
		printf 'amd64 Linux verify-dotnet job. Unit tests still run: make test-dotnet.\n'; \
		printf '========================================================================\n\n'; \
	else \
		$(MAKE) build-grpc-images-dotnet && \
		cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet \
			--skip test_transactional_records_are_visible_only_after_commit \
			--skip test_aborted_transaction_records_are_discarded \
			--skip test_consume_transform_produce_with_offsets; \
	fi

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

# Rust, Python and .NET performance suites, one after the other. Recipe lines
# rather than prerequisites so the order holds under `make -j`, and so the
# suites never overlap — each needs the machine to itself. The .NET arm is the
# Docker-gated v3 smoke (skips cleanly without Docker), alongside the Python one.
test-integration-perf:
	$(MAKE) test-integration-perf-rust
	$(MAKE) test-integration-perf-python
	$(MAKE) test-integration-perf-dotnet

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
# native build (cargo --features ffi) then `dotnet run` the exe selected by
# CLIENT_VERSION (3 = PerfV3/our binding, default; 2 = PerfV2/ckd baseline), so
# there is no build-rust prerequisite here (mirrors how test-dotnet delegates
# below). Pass RUST_PROJECT_ROOT so the delegated cargo/dotnet resolve the same
# repo root; CLIENT_VERSION passes through the environment.
producer-perf-test-dotnet:
	$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) producer-perf-test-dotnet

consumer-perf-test-dotnet:
	$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) consumer-perf-test-dotnet

# .NET in-suite performance smoke (xUnit + Testcontainers). Delegate into
# bindings/dotnet, which builds the native + PerfV3 exe then runs the Docker-gated
# net10.0-only smoke (skips cleanly without Docker). Alongside
# test-integration-perf-python above; now part of verify-dotnet (mirroring
# verify-python's perf stage).
test-integration-perf-dotnet:
	$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) test-integration-perf-dotnet

# Delegates to bindings/c's own `test` (ctest + the C-backend multilanguage arm)
# instead of reimplementing the ctest invocation here, mirroring how
# `test-python` delegates to bindings/python. Passes the same
# RUST_PROJECT_ROOT / CFLAGS_EXTRA as `build-c` so the delegated cmake configure
# matches and does not reconfigure the build dir with different flags.
test-c: build-c
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) CFLAGS_EXTRA="$(CFLAGS_NATIVE)" test

# macOS variant of test-c: C unit tests (ctest). The gRPC multilanguage arm
# runs on Linux only (verify-c).
test-c-macos-docker: build-c
	cd bindings/c/build && ctest --output-on-failure

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

# macOS variant of test-dotnet: same target verbatim (native build -> dotnet
# build matrix -> dotnet format -> unit tests on net8.0 + net10.0). Unlike
# test-c-macos-docker / test-python-macos-docker, .NET has no OS-specific
# recipe to swap in -- the delegated bindings/dotnet Makefile step is already
# platform-agnostic -- so this is a thin alias, kept for naming symmetry with
# the other macOS-block targets. The gRPC multilanguage arm runs on Linux only
# (verify-dotnet); this build never touches grpc-server (not in the .sln), so
# .NET's Linux-only Grpc.Tools/protoc constraint does not apply here.
test-dotnet-macos-docker: test-dotnet

# macOS variant of test-python: Python unit tests (pytest test/unit). The gRPC
# multilanguage arm runs on Linux only (verify-python).
test-python-macos-docker: build-python
	@(. venv/bin/activate && \
	cd $(RUST_PROJECT_ROOT)/bindings/python && \
	(pip install .[dev] || pip install --no-dependencies .[dev]) && \
	python -m pytest test/unit -v)

verify: build format-check lint test check-bindings

verify-c: test-c

verify-c-macos-docker: test-c-macos-docker

verify-python: test-python check-bindings
	$(MAKE) test-integration-perf-python

# verify-dotnet = build + format + unit(net8+net10) + integration(__grpc_dotnet
# [_async]) + the Docker-gated net10.0 p99 perf stage (landed M13/P1), mirroring
# verify-python's shape (which appends test-integration-perf-python above). The
# allocation-budget assertions also live in the unit suite. Recipe lines rather
# than prerequisites so each stage runs strictly after the prior gate, matching
# verify-python's ordering.
verify-dotnet: test-dotnet
	$(MAKE) test-integration-dotnet
	$(MAKE) test-integration-perf-dotnet

# macOS verify-dotnet: unit tests only, no integration/perf (mirrors
# verify-c-macos-docker / verify-python-macos-docker, which also drop the
# Linux-only integration stage).
verify-dotnet-macos-docker: test-dotnet-macos-docker

# macOS perf p99 budget (ms), used by verify-rust-macos-docker.
MACOS_P99_LIMIT_MS ?= 150

# macOS verify-python: unit tests.
verify-python-macos-docker: test-python-macos-docker

verify-rust: build-rust-all-features format-check lint test-rust-all-features
	$(MAKE) test-integration-perf-rust

# macOS verify-rust; perf tail uses MACOS_P99_LIMIT_MS.
verify-rust-macos-docker: build-rust-all-features format-check lint test-rust-all-features
	P99_LIMIT_MS=$(MACOS_P99_LIMIT_MS) $(MAKE) test-integration-perf-rust

verify-sandbox: build-rust build-c format-check lint test-integration test-c

init-hooks:
	@git config core.hooksPath .githooks
	@chmod +x .githooks/pre-commit

# Opt-in editor/AI-assistant tooling setup: installs the rust-analyzer
# component for the toolchain pinned in rust-toolchain.toml, and, if the
# `claude` CLI is on PATH, the matching Claude Code LSP plugin. Not a
# prerequisite of `init`/`build`/`verify` -- not every contributor's editor or
# workflow needs rust-analyzer, so this is run by hand.
install-rust-analyzer:
	rustup component add rust-analyzer
	@if command -v claude >/dev/null 2>&1; then \
		claude plugin install rust-analyzer-lsp@claude-plugins-official; \
	else \
		echo "claude CLI not found on PATH, skipping Claude Code rust-analyzer-lsp plugin install"; \
	fi

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
