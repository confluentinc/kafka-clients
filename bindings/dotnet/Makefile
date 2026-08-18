RUST_PROJECT_ROOT ?= $(realpath ../..)
GRPC_IMAGE_TAG ?= confluent-kafka-rust/dotnet-grpc-server:dev
GRPC_ASYNC_IMAGE_TAG ?= confluent-kafka-rust/dotnet-async-grpc-server:dev

# .NET tooling knobs. `dotnet` is resolved from PATH (the CI job provisions it
# via .semaphore/install-dotnet.sh; local dev uses its own install). Operations
# target the SOLUTION explicitly — Confluent.Kafka.sln holds only the library
# (Confluent.Kafka) and its unit tests (Confluent.Kafka.UnitTests). The
# grpc-server project is deliberately NOT in the solution (it needs protoc and is
# only ever built inside the gRPC Docker images), so building/formatting/testing
# the .sln never drags it in.
DOTNET ?= dotnet
DOTNET_CONFIG ?= Release
SOLUTION := $(RUST_PROJECT_ROOT)/bindings/dotnet/Confluent.Kafka.sln

.PHONY: grpc-image grpc-image-async build-dotnet test-dotnet

# Build the .NET gRPC consumer-backend image used by the multilanguage
# integration test harness. The build context is the repo root so the
# Dockerfile can copy from target/release/, bindings/dotnet/, and the
# multilanguage-test-server/ proto crate. Requires
# `cargo build --features ffi --release` to have produced the Linux
# libconfluent_kafka.so in target/release/ first.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/dotnet/Dockerfile.grpc \
		$(RUST_PROJECT_ROOT)

# Build the *async* .NET gRPC consumer-backend image (dotnet_async backend, M8/P2).
# Same build context and prerequisites as grpc-image; the image bakes
# CONSUMER_FLAVOR=async so Program.cs hosts AsyncConsumerServiceImpl
# (AsyncKafkaConsumer). Mirrors the Python two-image shape (grpc-image-async).
grpc-image-async:
	docker build \
		-t $(GRPC_ASYNC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/dotnet/Dockerfile.grpc.async \
		$(RUST_PROJECT_ROOT)

# Two-stage build (CLAUDE.md §7.1, firm order): the Rust core produces the
# native cdylib + generated header FIRST, then `dotnet build` compiles the
# binding and copies that native into $(OutDir). Never build .NET before Rust —
# the native would not exist and the csproj's EnsureNativeLibraryExists target
# fails fast.
#
# `--features ffi` is the minimal build the binding needs (the ffi cdylib +
# header); it is deliberately narrower than the root's `build-rust-all-features`
# used by the integration arm. `--manifest-path` keeps the invocation
# location-independent (works whether invoked here or via the root Makefile).
#
# The `dotnet build` compiles the full TFM matrix: the library
# (netstandard2.0;net8.0;net10.0) and the test project (net462;net8.0;net10.0).
# The net462 leg is BUILD-VERIFIED here (cross-platform via
# Microsoft.NETFramework.ReferenceAssemblies, Decision 2a) but never RUN — net462
# cannot execute on Linux; test execution is net8.0 + net10.0 only (see
# test-dotnet).
build-dotnet:
	cargo build --manifest-path $(RUST_PROJECT_ROOT)/Cargo.toml --features ffi --release
	$(DOTNET) build -c $(DOTNET_CONFIG) $(SOLUTION)

# build + format-check + unit tests, no broker (Mock* tests only). Mirrors the
# python/c per-binding `test` target. Format is dotnet-specific (no Python
# analog): `dotnet format --verify-no-changes` enforces the checked-in
# .editorconfig, and Directory.Build.props already makes analyzer/style
# violations hard build errors (TreatWarningsAsErrors, EnforceCodeStyleInBuild).
#
# Tests RUN on net8.0 AND net10.0 (Decision 2): SDK 10 runs net10.0 natively; the
# net8.0 leg needs the .NET 8 base runtime present (provisioned in CI by
# install-dotnet.sh). net462 is build-verified by build-dotnet, not run here.
test-dotnet: build-dotnet
	$(DOTNET) format $(SOLUTION) --verify-no-changes
	$(DOTNET) test -c $(DOTNET_CONFIG) -f net8.0 $(SOLUTION)
	$(DOTNET) test -c $(DOTNET_CONFIG) -f net10.0 $(SOLUTION)

# ---------------------------------------------------------------------------
# Performance suite (M13/P1). Kept OUT of the solution (like grpc-server), so
# `test-dotnet`/`verify-dotnet` never drag it into the functional gate — the
# p99 budgets must not fail under the unit suite's contention. Invoked by path.
#
# Slice 1 shipped the v3 exe (PerfV3, our binding) + the client-agnostic
# PerformanceCommon + the murmur2 self-check. Slice 2 adds the v2 exe (PerfV2/ckd
# baseline) and the Docker in-suite smoke (test-integration-perf-dotnet).
# CLIENT_VERSION now selects the exe: 3 (default) = PerfV3, 2 = PerfV2. PerfV2 and
# PerfV3 are never in one sln (PLAN §1.1.1 — the same-assembly collision); each is
# built/run by path.
# ---------------------------------------------------------------------------
PERF_DIR := $(RUST_PROJECT_ROOT)/bindings/dotnet/tests/Performance
PERF_TESTS := $(PERF_DIR)/Confluent.Kafka.PerformanceTests/Confluent.Kafka.PerformanceTests.csproj
PERF_V3 := $(PERF_DIR)/PerfV3
PERF_V2 := $(PERF_DIR)/PerfV2
# TFM the manual perf runs execute on (both are built; pick one to run).
PERF_TFM ?= net10.0
# Cross-language client selector: 3 = our binding -> PerfV3 (default); 2 = ckd -> PerfV2.
CLIENT_VERSION ?= 3

.PHONY: perf-unit-test-dotnet producer-perf-test-dotnet consumer-perf-test-dotnet test-integration-perf-dotnet

# The murmur2 self-check automated gate (UtilsTest.testMurmur2 vectors). No broker,
# no Docker, no client — runs on net8.0 AND net10.0. (The Docker in-suite smoke is
# the separate test-integration-perf-dotnet target below.)
perf-unit-test-dotnet:
	$(DOTNET) test -c $(DOTNET_CONFIG) -f net8.0 $(PERF_TESTS)
	$(DOTNET) test -c $(DOTNET_CONFIG) -f net10.0 $(PERF_TESTS)

# Env-driven producer/consumer benchmarks (manual; need a reachable broker via
# BOOTSTRAP_SERVERS). CLIENT_VERSION selects the exe: 3 (default) = PerfV3 (our
# binding), 2 = PerfV2 (ckd baseline). The v3 exe P/Invokes our native, so it does
# the two-stage native build first (CLAUDE.md §7.1); the v2/ckd exe needs no native
# (ckd carries its own librdkafka). Configure the run via the shared env-var
# contract (TOPIC_NAME, VALUE_SIZE, LIMIT_RPS, ASYNC, P99_LIMIT_MS, ...).
producer-perf-test-dotnet:
	@case "$(CLIENT_VERSION)" in \
	  3) cargo build --manifest-path $(RUST_PROJECT_ROOT)/Cargo.toml --features ffi --release && \
	     MODE=producer $(DOTNET) run -c $(DOTNET_CONFIG) -f $(PERF_TFM) --project $(PERF_V3) ;; \
	  2) MODE=producer $(DOTNET) run -c $(DOTNET_CONFIG) -f $(PERF_TFM) --project $(PERF_V2) ;; \
	  *) echo "producer-perf-test-dotnet: CLIENT_VERSION=$(CLIENT_VERSION) unsupported (2=PerfV2/ckd, 3=PerfV3/our binding)" >&2; exit 1 ;; \
	esac

consumer-perf-test-dotnet:
	@case "$(CLIENT_VERSION)" in \
	  3) cargo build --manifest-path $(RUST_PROJECT_ROOT)/Cargo.toml --features ffi --release && \
	     MODE=consumer $(DOTNET) run -c $(DOTNET_CONFIG) -f $(PERF_TFM) --project $(PERF_V3) ;; \
	  2) MODE=consumer $(DOTNET) run -c $(DOTNET_CONFIG) -f $(PERF_TFM) --project $(PERF_V2) ;; \
	  *) echo "consumer-perf-test-dotnet: CLIENT_VERSION=$(CLIENT_VERSION) unsupported (2=PerfV2/ckd, 3=PerfV3/our binding)" >&2; exit 1 ;; \
	esac

# In-suite v3 perf smoke (xUnit + Testcontainers). Docker-gated (skips cleanly
# without Docker); v3-ONLY gate (D10 — PerfV2 is manual-comparison only, never in
# this smoke). Two-stage native build first, then build the PerfV3 exe for BOTH
# TFMs (the smoke subprocess-launches PerfV3 by path, reusing the running test's
# config+TFM), then run the smoke on net8.0 AND net10.0 (mirrors test-dotnet; net8.0
# executes in CI, net10.0 locally). Kept out of verify-dotnet (perf isolated from
# the functional gate).
test-integration-perf-dotnet:
	cargo build --manifest-path $(RUST_PROJECT_ROOT)/Cargo.toml --features ffi --release
	$(DOTNET) build -c $(DOTNET_CONFIG) $(PERF_V3)/PerfV3.csproj
	$(DOTNET) test -c $(DOTNET_CONFIG) -f net8.0 $(PERF_TESTS)
	$(DOTNET) test -c $(DOTNET_CONFIG) -f net10.0 $(PERF_TESTS)
