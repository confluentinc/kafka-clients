# .NET binding targets. Invoked by the repository root's cross-language
# Makefile (`make -C dotnet $(ROOTS) <target>`), or directly from this directory.
# REPO_ROOT / RUST_PROJECT_ROOT follow python/Makefile and c/Makefile:
# RUST_PROJECT_ROOT is the Rust workspace (rust/), NOT the repository root.
#
# Every cargo command runs from $(RUST_PROJECT_ROOT), through rust/Makefile
# (`$(MAKE) -C $(RUST_PROJECT_ROOT) <target>`): rust-toolchain.toml and
# .cargo/config.toml (the `xtask` alias) are looked up from the current
# directory, so `cargo --manifest-path rust/Cargo.toml` from here would lose both.
REPO_ROOT ?= $(realpath ..)
RUST_PROJECT_ROOT ?= $(REPO_ROOT)/rust
GRPC_IMAGE_TAG ?= confluent-kafka-rust/dotnet-grpc-server:dev
GRPC_ASYNC_IMAGE_TAG ?= confluent-kafka-rust/dotnet-async-grpc-server:dev
# Native (non-container) gRPC server build for the multilanguage harness,
# written under the Rust target dir, where rust/tests/common/backend_pool.rs
# looks for it in MULTILANG_BACKEND_MODE=native (rust/target/grpc-native/dotnet).
GRPC_NATIVE_DIR = $(RUST_PROJECT_ROOT)/target/grpc-native

# .NET tooling knobs. `dotnet` is resolved from PATH (the CI job provisions it
# via .semaphore/install-dotnet.sh; local dev uses its own install). Operations
# target the SOLUTION explicitly — Confluent.Kafka.sln holds only the library
# (Confluent.Kafka) and its unit tests (Confluent.Kafka.UnitTests). The
# grpc-server project is deliberately NOT in the solution (it needs protoc and is
# built only by the gRPC Docker images and by `grpc-native` below), so
# building/formatting/testing the .sln never drags it in.
DOTNET ?= dotnet
DOTNET_CONFIG ?= Release
SOLUTION := $(REPO_ROOT)/dotnet/Confluent.Kafka.sln

.PHONY: grpc-image grpc-image-async grpc-native test-integration build-dotnet test-dotnet test-soak-dotnet

# Build the .NET gRPC backend image (Producer + Consumer + Admin services) used
# by the multilanguage integration test harness. The build context is the repo
# root (as for python/ and c/) so the Dockerfile can copy from
# rust/target/release/, dotnet/, and the rust/multilanguage-test-server/ proto
# crate. Requires `cargo build --features ffi --release` to have produced the
# Linux libconfluent_kafka.so in rust/target/release/ first.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		-f $(REPO_ROOT)/dotnet/Dockerfile.grpc \
		$(REPO_ROOT)

# Build the *async* .NET gRPC backend image (dotnet_async backend, M8/P2).
# Same build context and prerequisites as grpc-image; the image bakes
# CONSUMER_FLAVOR=async so Program.cs hosts AsyncProducerServiceImpl +
# AsyncConsumerServiceImpl (AsyncKafkaProducer / AsyncKafkaConsumer; no Admin,
# M15/P12 D1). Mirrors the Python two-image shape (grpc-image-async).
grpc-image-async:
	docker build \
		-t $(GRPC_ASYNC_IMAGE_TAG) \
		-f $(REPO_ROOT)/dotnet/Dockerfile.grpc.async \
		$(REPO_ROOT)

# Build the .NET gRPC server for the native (host-process) multilanguage arm
# (M17/P1): the net10.0 leg of grpc-server into rust/target/grpc-native/dotnet,
# where rust/tests/common/backend_pool.rs runs it with `dotnet`. net10.0 because SDK 10
# brings the ASP.NET Core 10 runtime but no ASP.NET Core 8 one; the Docker images
# publish the net8.0 leg instead. Invoked by the root `build-grpc-native-dotnet`,
# which builds the Rust release artifacts first.
#
# `-c Release` is PINNED rather than $(DOTNET_CONFIG): Confluent.Kafka.csproj's
# native-copy <Content> item copies rust/target/$(CargoProfileDir)/libconfluent_kafka.*
# into the output, CargoProfileDir follows the configuration, and only the release
# native is built by build-rust-all-features. A Debug build would copy a stale or
# missing debug native instead.
grpc-native:
	$(DOTNET) build -c Release -f net10.0 $(REPO_ROOT)/dotnet/grpc-server/Confluent.Kafka.GrpcServer.csproj -o $(GRPC_NATIVE_DIR)/dotnet

# The .NET-backend arms of the multilanguage suite: build the sync and async gRPC
# server images, then run the `__grpc_dotnet` tests of the Rust harness against
# them (rust/Makefile's test-integration-grpc-dotnet; the filter also matches
# `__grpc_dotnet_async`, and it carries the .NET-only DOTNET_GRPC_SKIPS). Reached
# from the root's test-integration-dotnet and its -ssl / -sasl-ssl variants, which
# select the broker listener through INTEGRATION_TEST_PROTOCOL in the environment.
#
# Cross-arch guard: both Dockerfiles COPY the *host-built*
# rust/target/release/libconfluent_kafka.so straight into a Linux container. On a
# non-Linux host that artifact is Mach-O / absent (no ELF .so), so the image
# cannot build. Cross-compiling host->Linux is out of scope for a dev-box run, so
# off Linux this SELF-SKIPS with a loud notice and exits 0. It is NOT skipped in
# CI, whose amd64 Linux verify-dotnet jobs run it. The skip is honest -- it never
# claims the container arms passed, only that this host cannot build the Linux
# images. The non-Linux fallback is the native arm, the root
# test-integration-dotnet-native, which the macOS CI verify-dotnet job runs.
#
# The images are built for linux/amd64 because Grpc.Tools' linux_arm64 protoc
# SIGSEGVs during C# codegen inside an arm64 Linux container; local Apple-Silicon
# dev sets DOCKER_DEFAULT_PLATFORM=linux/amd64 in its environment, never a
# Makefile-baked --platform. A macOS arm64 host is unaffected for the native arm:
# Grpc.Tools has no macOS arm64 protoc, and its macosx_x64 one runs under Rosetta 2.
#
# The image builds are invoked *inside* the recipe (rather than as prerequisites)
# so the skip also short-circuits the Docker builds.
test-integration:
	@if [ "$$(uname -s)" != "Linux" ]; then \
		printf '\n========================================================================\n'; \
		printf 'SKIP test-integration-dotnet: host is %s, not Linux.\n' "$$(uname -s)"; \
		printf '\n'; \
		printf 'The .NET gRPC-server images COPY the host-built\n'; \
		printf '  rust/target/release/libconfluent_kafka.so\n'; \
		printf 'into a Linux container. On this host that artifact is\n'; \
		printf 'Mach-O / absent (no ELF .so), so the image cannot build.\n'; \
		printf 'This container arm runs only in CI'"'"'s amd64 Linux verify-dotnet jobs.\n'; \
		printf 'Run it natively on this host with: %smake test-integration-dotnet-native\n' \
			"$${INTEGRATION_TEST_PROTOCOL:+INTEGRATION_TEST_PROTOCOL=$$INTEGRATION_TEST_PROTOCOL }"; \
		printf '========================================================================\n\n'; \
	else \
		$(MAKE) grpc-image grpc-image-async && \
		$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-grpc-dotnet; \
	fi

# Two-stage build (CLAUDE.md §7.1, firm order): the Rust core produces the
# native cdylib + generated header FIRST, then `dotnet build` compiles the
# binding and copies that native into $(OutDir). Never build .NET before Rust —
# the native would not exist and the csproj's EnsureNativeLibraryExists target
# fails fast.
#
# rust/Makefile's `build` (`cargo build --features ffi --release`) is the minimal
# build the binding needs (the ffi cdylib + header, in the release profile
# $(DOTNET_CONFIG)=Release maps to); it is deliberately narrower than the root's
# `build-rust-all-features` used by the integration arm. It runs in
# $(RUST_PROJECT_ROOT) (see the header), so it works whether this Makefile is
# invoked here or via the root Makefile.
#
# The `dotnet build` compiles the full TFM matrix: the library
# (netstandard2.0;net8.0;net10.0) and the test project (net462;net8.0;net10.0).
# The net462 leg is BUILD-VERIFIED here (cross-platform via
# Microsoft.NETFramework.ReferenceAssemblies, Decision 2a) but never RUN — net462
# cannot execute on Linux; test execution is net8.0 + net10.0 only (see
# test-dotnet).
build-dotnet:
	$(MAKE) -C $(RUST_PROJECT_ROOT) build
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
SOAK_DIR := $(REPO_ROOT)/dotnet/soak
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
# PerformanceCommon + the CRC-32 self-check. Slice 2 adds the v2 exe (PerfV2/ckd
# baseline) and the Docker in-suite smoke (test-integration-perf-dotnet).
# CLIENT_VERSION now selects the exe: 3 (default) = PerfV3, 2 = PerfV2. PerfV2 and
# PerfV3 are never in one sln (PLAN §1.1.1 — the same-assembly collision); each is
# built/run by path.
# ---------------------------------------------------------------------------
PERF_DIR := $(REPO_ROOT)/dotnet/tests/Performance
PERF_TESTS := $(PERF_DIR)/Confluent.Kafka.PerformanceTests/Confluent.Kafka.PerformanceTests.csproj
PERF_V3 := $(PERF_DIR)/PerfV3
PERF_V2 := $(PERF_DIR)/PerfV2
# TFM the manual perf runs execute on (both are built; pick one to run).
PERF_TFM ?= net10.0
# Cross-language client selector: 3 = our binding -> PerfV3 (default); 2 = ckd -> PerfV2.
CLIENT_VERSION ?= 3

.PHONY: perf-unit-test-dotnet producer-perf-test-dotnet consumer-perf-test-dotnet test-integration-perf-dotnet

# The perf suite's Docker-free automated gate: the CRC-32 self-check
# (built_in_partitioner.rs golden vectors) plus the engine-level tests (readiness gate,
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
#
# The native build is its own recipe line, ahead of the `case`, rather than a
# `3)` branch of it: GNU make executes every line that contains `$(MAKE)` even
# under `make -n`, so folding it into the `case` would make a dry run actually
# `dotnet run` the benchmark.
producer-perf-test-dotnet:
	@if [ "$(CLIENT_VERSION)" = "3" ]; then $(MAKE) -C $(RUST_PROJECT_ROOT) build; fi
	@case "$(CLIENT_VERSION)" in \
	  3) MODE=producer $(DOTNET) run -c $(DOTNET_CONFIG) -f $(PERF_TFM) --project $(PERF_V3) ;; \
	  2) MODE=producer $(DOTNET) run -c $(DOTNET_CONFIG) -f $(PERF_TFM) --project $(PERF_V2) ;; \
	  *) echo "producer-perf-test-dotnet: CLIENT_VERSION=$(CLIENT_VERSION) unsupported (2=PerfV2/ckd, 3=PerfV3/our binding)" >&2; exit 1 ;; \
	esac

consumer-perf-test-dotnet:
	@if [ "$(CLIENT_VERSION)" = "3" ]; then $(MAKE) -C $(RUST_PROJECT_ROOT) build; fi
	@case "$(CLIENT_VERSION)" in \
	  3) MODE=consumer $(DOTNET) run -c $(DOTNET_CONFIG) -f $(PERF_TFM) --project $(PERF_V3) ;; \
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
	$(MAKE) -C $(RUST_PROJECT_ROOT) build
	$(DOTNET) build -c $(DOTNET_CONFIG) $(PERF_V3)/PerfV3.csproj
	PERF_REQUIRE_DOCKER=True $(DOTNET) test -c $(DOTNET_CONFIG) -f net10.0 $(PERF_TESTS)
