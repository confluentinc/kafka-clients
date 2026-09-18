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

.PHONY: grpc-image grpc-image-async build-dotnet test-dotnet test-soak-dotnet

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
	$(MAKE) test-soak-dotnet

# ---------------------------------------------------------------------------
# Soak client (M16/P1). Kept OUT of the solution (like grpc-server and the perf
# suite): it carries the OpenTelemetry dependency tree, which must not be
# restored on the functional gate's critical path. Invoked by path — and by
# `test-dotnet` above, mirroring PR #172's `test: … $(MAKE) test-soak` wiring on
# the Python side.
#
# ⚠ The format pass is NOT optional here and is the direct consequence of the
# projects being outside the solution: `dotnet format $(SOLUTION)` above does not
# reach them, so without these two lines the soak would ship unformatted while
# the gate read green.
#
# Depends on build-dotnet for the two-stage build (CLAUDE.md §7.1): the soak
# P/Invokes the same native, and Confluent.Kafka.csproj's EnsureNativeLibraryExists
# fails loudly if cargo has not run. Both steps are incremental, so the re-entry
# from test-dotnet costs almost nothing.
#
# Tests RUN on net8.0 AND net10.0 (the soak exe targets exactly those two; net462
# is deliberately absent — the client is run by `dotnet <dll>` on a Linux box,
# never on .NET Framework).
# ---------------------------------------------------------------------------
SOAK_DIR := $(RUST_PROJECT_ROOT)/bindings/dotnet/soak
SOAK_CLIENT := $(SOAK_DIR)/SoakClient/SoakClient.csproj
SOAK_TESTS := $(SOAK_DIR)/SoakClient.Tests/SoakClient.Tests.csproj

test-soak-dotnet: build-dotnet
	$(DOTNET) build -c $(DOTNET_CONFIG) $(SOAK_TESTS)
	$(DOTNET) format $(SOAK_CLIENT) --verify-no-changes
	$(DOTNET) format $(SOAK_TESTS) --verify-no-changes
	$(DOTNET) test -c $(DOTNET_CONFIG) -f net8.0 $(SOAK_TESTS)
	$(DOTNET) test -c $(DOTNET_CONFIG) -f net10.0 $(SOAK_TESTS)

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

# The perf suite's Docker-free automated gate: the murmur2 self-check
# (UtilsTest.testMurmur2 vectors) plus the engine-level tests (readiness gate,
# async-producer cancellation, 64-bit message count). No broker, no Docker, no
# client — runs on net8.0 AND net10.0. (The Docker in-suite smoke is the separate
# test-integration-perf-dotnet target below.)
#
# The --filter is what makes that true (M13/P3 item F, D-6 Option 1).
# Confluent.Kafka.PerformanceTests holds BOTH the Docker-free tests and the four
# Docker-backed broker smokes, so an unfiltered `dotnet test` here started a Kafka
# container — and, since this target never builds the PerfV3 exe the smokes launch,
# on a clean tree each smoke spun up that container and only THEN failed on a
# missing-file assertion. Slow, surprising, and the opposite of what the comment
# promises. Scope is a local-developer papercut: no root Makefile target and no CI
# job references this one (it appears only in its own .PHONY line and definition).
#
# The predicate EXCLUDES the one Docker-backed class rather than naming the
# Docker-free ones, so a new Docker-free test file is covered here automatically
# instead of silently running nowhere but the Docker target.
perf-unit-test-dotnet:
	$(DOTNET) test -c $(DOTNET_CONFIG) -f net8.0 $(PERF_TESTS) --filter 'FullyQualifiedName!~PerfV3SmokeTests'
	$(DOTNET) test -c $(DOTNET_CONFIG) -f net10.0 $(PERF_TESTS) --filter 'FullyQualifiedName!~PerfV3SmokeTests'

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

# In-suite v3 perf smoke (xUnit + Testcontainers). v3-ONLY gate (D10 — PerfV2 is
# manual-comparison only, never in this smoke). Two-stage native build first, then
# build the PerfV3 exe for BOTH TFMs (the smoke subprocess-launches PerfV3 by path,
# reusing the running test's config+TFM), then run the smoke on net10.0 ONLY. The
# perf gate is a p99-latency check, not TFM coverage: net8.0 stays covered
# functionally by test-dotnet (both TFMs); net10.0 is the designated execution gate
# (PerfV3.csproj still builds for all TFMs). Wired into verify-dotnet (mirroring
# verify-python's perf stage).
#
# PERF_REQUIRE_DOCKER=True (M13/P3 item E) makes a missing/broken Docker or a
# broker that will not start a FAILURE here, not a skip: this target is what
# verify-dotnet reaches, so a green tick from it has to mean the budget was
# actually measured. A bare `dotnet test` (no Makefile) leaves it unset, and a
# Docker-less dev box then gets a real, visible `Skipped` instead. The value must
# be the case-sensitive "True" — the suite-wide Python-compatible convention; =1
# would silently not enable it.
test-integration-perf-dotnet:
	cargo build --manifest-path $(RUST_PROJECT_ROOT)/Cargo.toml --features ffi --release
	$(DOTNET) build -c $(DOTNET_CONFIG) $(PERF_V3)/PerfV3.csproj
	PERF_REQUIRE_DOCKER=True $(DOTNET) test -c $(DOTNET_CONFIG) -f net10.0 $(PERF_TESTS)
