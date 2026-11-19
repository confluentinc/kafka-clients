# .NET binding targets: `make -C dotnet <target>` from the repository root, or
# `make <target>` from this directory. A target that needs the Rust library
# builds it first (rust-lib, or pack's release build).
# REPO_ROOT / RUST_PROJECT_ROOT follow python/Makefile and c/Makefile:
# RUST_PROJECT_ROOT is the Rust workspace (rust/), NOT the repository root.
#
# Every cargo command runs from $(RUST_PROJECT_ROOT), through rust/Makefile
# (`$(MAKE) -C $(RUST_PROJECT_ROOT) <target>`): rust-toolchain.toml and
# .cargo/config.toml (the `xtask` alias) are looked up from the current
# directory, so `cargo --manifest-path rust/Cargo.toml` from here would lose both.
REPO_ROOT ?= $(realpath ..)
RUST_PROJECT_ROOT ?= $(REPO_ROOT)/rust
# The tag of the gRPC images; rust/tests/common/backend_pool.rs reads the same
# variable. Override it per worktree so concurrent sessions do not share images.
MULTILANG_IMAGE_TAG ?= dev
GRPC_IMAGE_TAG ?= confluent-kafka-rust/dotnet-grpc-server:$(MULTILANG_IMAGE_TAG)
GRPC_ASYNC_IMAGE_TAG ?= confluent-kafka-rust/dotnet-async-grpc-server:$(MULTILANG_IMAGE_TAG)
# Native (non-container) gRPC server build for the multilanguage harness,
# written under the Rust target dir, where rust/tests/common/backend_pool.rs
# looks for it in MULTILANG_BACKEND_MODE=native (rust/target/grpc-native/dotnet).
GRPC_NATIVE_DIR = $(RUST_PROJECT_ROOT)/target/grpc-native

# .NET tooling knobs. `dotnet` is resolved from PATH (the CI job provisions it
# via .semaphore/install-dotnet.sh; local dev uses its own install). Operations
# target the SOLUTION explicitly — Confluent.Kafka.sln holds only the library
# (Confluent.Kafka), its unit tests (Confluent.Kafka.UnitTests) and its
# broker-backed integration tests (Confluent.Kafka.IntegrationTests). The
# grpc-server project is deliberately NOT in the solution (it needs protoc and is
# built only by the gRPC Docker images and by `grpc-native` below), so
# building/formatting/testing the .sln never drags it in.
DOTNET ?= dotnet
DOTNET_CONFIG ?= Release
# The cargo profile directory of the Rust library the tests link,
# $(RUST_PROJECT_ROOT)/target/$(RUST_PROFILE): the debug all-features build, the
# library unit the Rust tests build (rust/Makefile `devel-build-all-features`).
# CARGO_PROFILE_PROP points Confluent.Kafka.csproj's native copy at it, whatever
# $(DOTNET_CONFIG) is; every build and test of the test paths passes it. pack-dotnet
# and the perf benchmarks build and link the release library instead.
RUST_PROFILE ?= debug
RUST_BUILD_TARGET = $(if $(filter release,$(RUST_PROFILE)),build,devel-build-all-features)
CARGO_PROFILE_PROP = -p:CargoProfileDir=$(RUST_PROFILE)
SOLUTION := $(REPO_ROOT)/dotnet/Confluent.Kafka.sln

# `$(call selected_run,<options> -- <command>)` runs the command only when the
# test selection (the file TEST_SELECTION names, see the root's
# `select-tests`) selects its section (`dotnet.solution`, `dotnet.soak`,
# `dotnet.performance`, `dotnet.grpc`), and always when none applies. With
# `--dotnet`, an argument `{}` stands for the `--filter` selecting the tests the
# section names, and-ed with the `--dotnet-filter` expression, or for that
# expression alone when the whole suite runs. See `cargo xtask help`
# (`selected-run`). The subshell keeps the `cd`, which cargo needs to find the
# toolchain and the `xtask` alias, from leaking into the rest of the recipe;
# for the same reason an environment variable for the command goes after `--`
# through `env`.
selected_run = (cd $(RUST_PROJECT_ROOT) && cargo -q xtask selected-run -C $(CURDIR) $(1))

.PHONY: grpc-image grpc-image-async grpc-native rust-lib test-integration test-integration-ssl test-integration-sasl-ssl \
	test-integration-native build-dotnet test-dotnet check-dotnet test-soak-dotnet test-grpc-server-dotnet \
	test-examples-dotnet test-verifiable-clients-dotnet test-dotnet-macos verify verify-except-grpc-arm verify-macos \
	pack-dotnet package-check-dotnet coverage-dotnet

# Build the .NET gRPC backend image (Producer + Consumer + Admin services) used
# by the multilanguage integration test harness. The build context is the repo
# root (as for python/ and c/) so the Dockerfile can copy from
# rust/target/$(RUST_PROFILE)/, dotnet/, and the rust/multilanguage-test-server/ proto
# crate. It copies the Linux libconfluent_kafka.so from rust/target/$(RUST_PROFILE)/,
# which rust-lib builds first.
grpc-image: rust-lib
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		--build-arg RUST_PROFILE=$(RUST_PROFILE) \
		-f $(REPO_ROOT)/dotnet/Dockerfile.grpc \
		$(REPO_ROOT)

# Build the *async* .NET gRPC backend image (dotnet_async backend, M8/P2).
# Same build context and prerequisites as grpc-image; the image bakes
# CONSUMER_FLAVOR=async so Program.cs hosts AsyncProducerServiceImpl +
# AsyncConsumerServiceImpl + AsyncAdminServiceImpl (AsyncKafkaProducer /
# AsyncKafkaConsumer, and admin over IAsyncAdmin since M15/P14). Mirrors the
# Python two-image shape (grpc-image-async).
grpc-image-async: rust-lib
	docker build \
		-t $(GRPC_ASYNC_IMAGE_TAG) \
		--build-arg RUST_PROFILE=$(RUST_PROFILE) \
		-f $(REPO_ROOT)/dotnet/Dockerfile.grpc.async \
		$(REPO_ROOT)

# Build the .NET gRPC server for the native (host-process) multilanguage arm
# (M17/P1): the net10.0 leg of grpc-server into rust/target/grpc-native/dotnet,
# where rust/tests/common/backend_pool.rs runs it with `dotnet`. net10.0 because SDK 10
# brings the ASP.NET Core 10 runtime but no ASP.NET Core 8 one; the Docker images
# publish the net8.0 leg instead. rust-lib builds the debug all-features Rust
# library first.
#
# Confluent.Kafka.csproj's native-copy <Content> item copies
# rust/target/$(CargoProfileDir)/libconfluent_kafka.* into the output, and
# CargoProfileDir follows the configuration unless it is given: $(CARGO_PROFILE_PROP)
# points it at the debug library rust-lib builds, with the .NET configuration
# pinned to Release.
grpc-native: rust-lib
	$(DOTNET) build -c Release $(CARGO_PROFILE_PROP) -f net10.0 $(REPO_ROOT)/dotnet/grpc-server/Confluent.Kafka.GrpcServer.csproj -o $(GRPC_NATIVE_DIR)/dotnet

# The .NET-backend arms of the multilanguage suite: build the sync and async gRPC
# server images, then run the `__grpc_dotnet` tests of the Rust harness against
# them (rust/Makefile's test-integration-grpc-dotnet; the filter also matches
# `__grpc_dotnet_async`). The -ssl / -sasl-ssl variants below select the broker
# listener through INTEGRATION_TEST_PROTOCOL in the environment.
#
# Cross-arch guard: both Dockerfiles COPY the *host-built*
# rust/target/$(RUST_PROFILE)/libconfluent_kafka.so straight into a Linux container. On a
# non-Linux host that artifact is Mach-O / absent (no ELF .so), so the image
# cannot build. Cross-compiling host->Linux is out of scope for a dev-box run, so
# off Linux this SELF-SKIPS with a loud notice and exits 0. It is NOT skipped in
# CI, whose amd64 Linux verify-dotnet jobs run it. The skip is honest -- it never
# claims the container arms passed, only that this host cannot build the Linux
# images. The non-Linux fallback is the native arm, test-integration-native,
# which the macOS CI verify-dotnet job runs.
#
# The images are built for linux/amd64 because Grpc.Tools' linux_arm64 protoc
# SIGSEGVs during C# codegen inside an arm64 Linux container; local Apple-Silicon
# dev sets DOCKER_DEFAULT_PLATFORM=linux/amd64 in its environment, never a
# Makefile-baked --platform. A macOS arm64 host is unaffected for the native arm:
# Grpc.Tools has no macOS arm64 protoc, and its macosx_x64 one runs under Rosetta 2.
#
# The image builds are invoked *inside* the recipe (rather than as prerequisites)
# so the skip also short-circuits the Docker builds.
test-integration: rust-lib
	@if [ "$$(uname -s)" != "Linux" ]; then \
		printf '\n========================================================================\n'; \
		printf 'SKIP .NET test-integration: host is %s, not Linux.\n' "$$(uname -s)"; \
		printf '\n'; \
		printf 'The .NET gRPC-server images COPY the host-built\n'; \
		printf '  rust/target/$(RUST_PROFILE)/libconfluent_kafka.so\n'; \
		printf 'into a Linux container. On this host that artifact is\n'; \
		printf 'Mach-O / absent (no ELF .so), so the image cannot build.\n'; \
		printf 'This container arm runs only in CI'"'"'s amd64 Linux verify-dotnet jobs.\n'; \
		printf 'Run it natively on this host with: %smake -C dotnet test-integration-native\n' \
			"$${INTEGRATION_TEST_PROTOCOL:+INTEGRATION_TEST_PROTOCOL=$$INTEGRATION_TEST_PROTOCOL }"; \
		printf '========================================================================\n\n'; \
	else \
		$(call selected_run,--section dotnet.grpc -- $(MAKE) -C $(CURDIR) grpc-image grpc-image-async) && \
		$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-grpc-dotnet; \
	fi

# The same arms over the SSL / SASL_SSL CONTAINER listeners (CTLSONLY :9100 /
# CSASLTLS :9101), which INTEGRATION_TEST_PROTOCOL selects
# (rust/tests/common/test_context.rs); the harness passes the matching TLS and
# SASL settings to the gRPC servers. They inherit the non-Linux skip.
test-integration-ssl:
	INTEGRATION_TEST_PROTOCOL=ssl $(MAKE) test-integration
test-integration-sasl-ssl:
	INTEGRATION_TEST_PROTOCOL=sasl_ssl $(MAKE) test-integration

# The .NET arms with the gRPC server as a host process instead of a container
# (MULTILANG_BACKEND_MODE=native, rust/tests/common/backend_pool.rs): only the
# broker runs in Docker. Used on macOS; also works on Linux. Needs the .NET 10
# SDK on PATH (grpc-native).
test-integration-native: grpc-native
	$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-grpc-dotnet-native

# The Rust library the .NET binding loads, built by rust/Makefile: the debug
# all-features build, or the release one for RUST_PROFILE=release; a cargo
# no-op once it is current.
rust-lib:
	$(MAKE) -C $(RUST_PROJECT_ROOT) $(RUST_BUILD_TARGET)

# Two-stage build (CLAUDE.md §7.1, firm order): the Rust core produces the
# native cdylib + generated header FIRST, then `dotnet build` compiles the
# binding and copies that native into $(OutDir). Never build .NET before Rust —
# the native would not exist and the csproj's EnsureNativeLibraryExists target
# fails fast.
#
# rust/Makefile's `devel-build-all-features` is the debug all-features library,
# the one the Rust tests and the other bindings' tests build, so it compiles
# nothing after them; $(CARGO_PROFILE_PROP) makes the .NET build copy it, with
# the .NET configuration left at $(DOTNET_CONFIG). It runs in
# $(RUST_PROJECT_ROOT) (see the header), so it works from any directory.
#
# The `dotnet build` compiles the full TFM matrix: the library
# (net462;netstandard2.0;net8.0;net10.0), the unit-test project (net462;net8.0;net10.0)
# and the integration-test project (net8.0;net10.0).
# The net462 legs are BUILD-VERIFIED here (cross-platform via
# Microsoft.NETFramework.ReferenceAssemblies, Decision 2a) but never RUN — net462
# cannot execute on Linux; test execution is net8.0 + net10.0 only (see
# test-dotnet).
build-dotnet: rust-lib
	$(DOTNET) build -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) $(SOLUTION)

# build + the solution's tests: the unit tests (Mock* tests, no broker) and the
# integration tests (a Testcontainers broker; they skip without Docker, as
# python/test/integration does), then the soak client's tests. Mirrors the
# python/c per-binding `test` target. The checks that are not tests are
# `check-dotnet` below; Directory.Build.props already makes analyzer and style
# violations build errors (TreatWarningsAsErrors, EnforceCodeStyleInBuild).
#
# Tests RUN on net8.0 AND net10.0 (Decision 2): SDK 10 runs net10.0 natively; the
# net8.0 leg needs the .NET 8 base runtime present (provisioned in CI by
# install-dotnet.sh). net462 is build-verified by build-dotnet, not run here.
#
# DOTNET_TEST_FILTER, empty by default, is a `dotnet test --filter` expression for
# both test lines. test-dotnet-macos below sets it to leave the
# integration tests out, as Python's macOS job runs only its unit tests. The
# test selection's `dotnet.solution` narrows it further. The build always runs.
DOTNET_TEST_FILTER ?=

test-dotnet: build-dotnet
	$(call selected_run,--section dotnet.solution --dotnet --dotnet-filter '$(DOTNET_TEST_FILTER)' -- $(DOTNET) test -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) -f net8.0 $(SOLUTION) {})
	$(call selected_run,--section dotnet.solution --dotnet --dotnet-filter '$(DOTNET_TEST_FILTER)' -- $(DOTNET) test -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) -f net10.0 $(SOLUTION) {})
	$(MAKE) test-soak-dotnet

# test-dotnet without the integration tests (the unit and soak tests still run),
# for macOS, where Python's job also runs only its unit tests, so the
# broker-backed tests run on Linux only.
test-dotnet-macos:
	$(MAKE) DOTNET_TEST_FILTER='FullyQualifiedName!~Confluent.Kafka.IntegrationTests' test-dotnet

# The .NET gate (dotnet/CLAUDE.md §7.5): the tests, the gRPC container arms,
# then the Docker-gated net10.0 p99 perf stage. Recipe lines rather than
# prerequisites so each stage runs after the one before. The checks that are not
# tests are check-dotnet, which CI runs in "check-dotnet (Linux amd64)".
verify: test-dotnet
	$(MAKE) test-integration
	$(MAKE) test-integration-perf-dotnet

# `verify` without the gRPC container arms: the CI job "verify-dotnet (Linux
# amd64) — plaintext". CI runs the arms in "verify-dotnet (Linux amd64) —
# plaintext gRPC arm" (`test-integration`), as it does for C and Python and as
# it runs the ssl / sasl_ssl arms.
verify-except-grpc-arm: test-dotnet
	$(MAKE) test-integration-perf-dotnet

# The macOS gate: the unit and soak tests, then the gRPC multilanguage arm with
# the gRPC server as a native host process. No perf stage.
verify-macos: test-dotnet-macos
	$(MAKE) test-integration-native

# The checks that are not tests: `dotnet format --verify-no-changes`, which
# enforces the checked-in .editorconfig, over the solution and over the soak
# projects, which are outside it (built first, as the soak tests build them), then
# the examples' build and format check. CI runs them in their own job,
# "check-dotnet (Linux amd64)".
check-dotnet: build-dotnet
	$(DOTNET) format $(SOLUTION) --verify-no-changes
	$(DOTNET) build -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) $(SOAK_TESTS)
	$(DOTNET) format $(SOAK_CLIENT) --verify-no-changes
	$(DOTNET) format $(SOAK_TESTS) --verify-no-changes
	$(MAKE) test-examples-dotnet

# ---------------------------------------------------------------------------
# Soak client (M16/P1). Kept OUT of the solution (like grpc-server and the perf
# suite): it carries the OpenTelemetry dependency tree, which must not be
# restored on the functional gate's critical path. Invoked by path — and by
# `test-dotnet` above, mirroring PR #172's `test: … $(MAKE) test-soak` wiring on
# the Python side.
#
# ⚠ The soak projects' format pass, in `check-dotnet`, is the direct consequence
# of the projects being outside the solution: `dotnet format $(SOLUTION)` does not
# reach them, so without it the soak would ship unformatted while the gate read
# green.
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
	$(DOTNET) build -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) $(SOAK_TESTS)
	$(call selected_run,--section dotnet.soak --dotnet -- $(DOTNET) test -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) -f net8.0 $(SOAK_TESTS) {})
	$(call selected_run,--section dotnet.soak --dotnet -- $(DOTNET) test -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) -f net10.0 $(SOAK_TESTS) {})

# ---------------------------------------------------------------------------
# The examples (dotnet/examples/README.md). Kept out of the solution, like the soak
# client: they target net8.0 only, as a user's program would, while test-dotnet runs
# the solution on net10.0 too. Their own solution holds them; this target builds it
# and checks its format, so the examples cannot rot. They are run by hand, against a
# broker, as the README shows.
# ---------------------------------------------------------------------------
EXAMPLES_SOLUTION := $(REPO_ROOT)/dotnet/examples/Confluent.Kafka.Examples.sln

test-examples-dotnet: build-dotnet
	$(DOTNET) build -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) $(EXAMPLES_SOLUTION)
	$(DOTNET) format $(EXAMPLES_SOLUTION) --verify-no-changes

# ---------------------------------------------------------------------------
# Unit-test coverage of the Confluent.Kafka library.
#
# Runs Confluent.Kafka.UnitTests on net10.0 with Microsoft Code Coverage, the
# collector that Microsoft.NET.Test.Sdk already brings, and turns its Cobertura
# output into a report with ReportGenerator, a local tool pinned in
# .config/dotnet-tools.json. Only the Confluent.Kafka assembly is counted, not
# the test assembly. One TFM is enough: net8.0 runs the same code, and net462
# cannot run outside Windows. -sourcedirs lets the HTML report find the sources
# in a CI build, whose PDBs name them /_/... (Directory.Build.props).
#
# Writes artifacts/coverage/report (Summary.txt, printed at the end, and an HTML
# report), then prints a second total without Confluent.Kafka.Common.Errors: the
# generated exception classes, whose unused constructor overloads are most of the
# uncovered lines. CI runs it in the "coverage-dotnet (Linux amd64)" job and keeps
# the HTML report as the workflow artifact coverage/dotnet.tar.gz. It reports only;
# there is no threshold.
# ---------------------------------------------------------------------------
COVERAGE_DIR := $(REPO_ROOT)/dotnet/artifacts/coverage
UNIT_TESTS := $(REPO_ROOT)/dotnet/tests/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj
REPORTGENERATOR = $(DOTNET) tool run reportgenerator

coverage-dotnet: build-dotnet
	rm -rf $(COVERAGE_DIR)
	$(DOTNET) tool restore
	$(DOTNET) test -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) -f net10.0 $(UNIT_TESTS) \
		--collect 'Code Coverage;Format=cobertura' --results-directory $(COVERAGE_DIR)/raw
	$(REPORTGENERATOR) '-reports:$(COVERAGE_DIR)/raw/**/*.cobertura.xml' '-targetdir:$(COVERAGE_DIR)/report' \
		'-sourcedirs:$(REPO_ROOT)' '-assemblyfilters:+Confluent.Kafka' \
		'-reporttypes:TextSummary;Html' -verbosity:Warning
	$(REPORTGENERATOR) '-reports:$(COVERAGE_DIR)/raw/**/*.cobertura.xml' '-targetdir:$(COVERAGE_DIR)/report-without-errors' \
		'-sourcedirs:$(REPO_ROOT)' '-assemblyfilters:+Confluent.Kafka' '-classfilters:-Confluent.Kafka.Common.Errors.*' \
		'-reporttypes:TextSummary' -verbosity:Warning
	cat $(COVERAGE_DIR)/report/Summary.txt
	@echo 'Without Confluent.Kafka.Common.Errors (the generated exception classes):'
	@sed -n '2,/^$$/p' $(COVERAGE_DIR)/report-without-errors/Summary.txt

# ---------------------------------------------------------------------------
# The verifiable clients (dotnet/tools/VerifiableClients), the producer and consumer
# tools Kafka's system tests drive. Their tests alone, on net8.0 and net10.0. The
# projects are in the solution, so test-dotnet runs these tests too; this target
# does not need calling from it.
# ---------------------------------------------------------------------------
VERIFIABLE_CLIENTS_TESTS := $(REPO_ROOT)/dotnet/tools/VerifiableClients/VerifiableClients.Tests/VerifiableClients.Tests.csproj

test-verifiable-clients-dotnet: build-dotnet
	$(DOTNET) test -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) -f net8.0 $(VERIFIABLE_CLIENTS_TESTS)
	$(DOTNET) test -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) -f net10.0 $(VERIFIABLE_CLIENTS_TESTS)

# ---------------------------------------------------------------------------
# The Confluent.Kafka NuGet package.
#
# pack-dotnet packs dotnet/artifacts/package/Confluent.Kafka.<version>.nupkg with
# this machine's own native library only (.semaphore/pack-nupkg.sh --dev). It is
# checked as a release package is (check-nupkg.py), except for the platform
# floors (glibc 2.28, macOS 11.0) a local build does not meet: a package to test
# the packaging, never one to release. The release package, with the natives of
# all five RIDs, is packed by semaphore.yml's "Release (.NET): pack" block.
#
# package-check-dotnet installs that package from a local feed into a project
# outside the repository, with an empty NuGet cache, and runs the smoke program
# on net8.0 and net10.0 (.semaphore/test-nupkg.sh --smoke): what the release's
# package verification runs, less the unit tests against the package, the Linux
# distribution containers and the self-contained publish. CI runs it in the
# macOS verify-dotnet job. The version is <Version> in Directory.Build.props;
# `dotnet pack` without the scripts fails, as the package needs its natives.
# ---------------------------------------------------------------------------
PACKAGE_DIR := $(REPO_ROOT)/dotnet/artifacts/package

pack-dotnet:
	$(MAKE) -C $(RUST_PROJECT_ROOT) build
	rm -rf $(PACKAGE_DIR)
	cd $(REPO_ROOT) && sh .semaphore/pack-nupkg.sh --dev $(PACKAGE_DIR)

package-check-dotnet: pack-dotnet
	cd $(REPO_ROOT) && sh .semaphore/test-nupkg.sh --smoke $(PACKAGE_DIR)

# ---------------------------------------------------------------------------
# gRPC-server unit tests (M18/P1, PLAN §7.1): the chaos-harness service
# (ChaosWorkloadServiceImpl and the shared Chaos/* plumbing) driven over
# MockProducer / MockConsumer through its internal client-factory seam. No broker,
# no Docker.
#
# ⚠ A LOCAL GATE, NOT RUN IN CI (PLAN Q2, matching Python, whose chaos and gRPC
# servers have no tests in CI either). `test-dotnet` deliberately does NOT call
# this target, and neither the root Makefile nor any .semaphore job references
# it: run `make -C dotnet test-grpc-server-dotnet` by hand for server changes.
#
# Like the soak, both projects sit outside the solution, so `dotnet format
# $(SOLUTION)` never reaches them; hence the two explicit format passes. The
# server P/Invokes the same native, hence build-dotnet first (CLAUDE.md §7.1).
#
# net10.0 only: the server is a Microsoft.NET.Sdk.Web app, the ASP.NET Core 8
# runtime is absent locally and in CI (M17/P1 D3), and the test project targets
# net10.0 alone.
# ---------------------------------------------------------------------------
GRPC_SERVER_PROJECT := $(REPO_ROOT)/dotnet/grpc-server/Confluent.Kafka.GrpcServer.csproj
GRPC_SERVER_TESTS := $(REPO_ROOT)/dotnet/tests/Confluent.Kafka.GrpcServer.UnitTests/Confluent.Kafka.GrpcServer.UnitTests.csproj

test-grpc-server-dotnet: build-dotnet
	$(DOTNET) build -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) $(GRPC_SERVER_TESTS)
	$(DOTNET) format $(GRPC_SERVER_PROJECT) --verify-no-changes
	$(DOTNET) format $(GRPC_SERVER_TESTS) --verify-no-changes
	$(DOTNET) test -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) -f net10.0 $(GRPC_SERVER_TESTS)

# ---------------------------------------------------------------------------
# Performance suite. Kept OUT of the solution (like grpc-server), so
# `test-dotnet` never drags it into the functional gate — the
# p99 budgets must not fail under the unit suite's contention. Invoked by path.
#
# It holds the v3 exe (PerfV3, our binding), the v2 exe (PerfV2, the
# confluent-kafka-dotnet baseline), the client-agnostic PerformanceCommon, the
# CRC-32 self-check and the Docker in-suite smoke (test-integration-perf-dotnet).
# CLIENT_VERSION selects the exe: 3 (default) = PerfV3, 2 = PerfV2. PerfV2 and
# PerfV3 are never in one sln (both are the `Confluent.Kafka` assembly); each is
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
# async-producer cancellation, 64-bit message count, PerfEnv's empty-value check). No
# broker, no Docker, no client — runs on net8.0 AND net10.0. The empty-value check runs
# the PerfV3 exe as a child process that stops in its configuration, before it makes a
# client, so this target builds PerfV3 first, as the Docker target does. (The Docker
# in-suite smoke is the separate test-integration-perf-dotnet target below.)
#
# The --filter is what makes that true.
# Confluent.Kafka.PerformanceTests holds BOTH the Docker-free tests and the four
# Docker-backed broker smokes, so an unfiltered `dotnet test` here started a Kafka
# container — and, since this target did not build the PerfV3 exe the smokes launch,
# on a clean tree each smoke spun up that container and only THEN failed on a
# missing-file assertion. Slow, surprising, and the opposite of what the comment
# promises. Scope is a local-developer papercut: no root Makefile target and no CI
# job references this one (it appears only in its own .PHONY line and definition).
#
# The predicate EXCLUDES the one Docker-backed class rather than naming the
# Docker-free ones, so a new Docker-free test file is covered here automatically
# instead of silently running nowhere but the Docker target.
perf-unit-test-dotnet: build-dotnet
	$(DOTNET) build -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) $(PERF_V3)/PerfV3.csproj
	$(call selected_run,--section dotnet.performance --dotnet --dotnet-filter 'FullyQualifiedName!~PerfV3SmokeTests' -- $(DOTNET) test -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) -f net8.0 $(PERF_TESTS) {})
	$(call selected_run,--section dotnet.performance --dotnet --dotnet-filter 'FullyQualifiedName!~PerfV3SmokeTests' -- $(DOTNET) test -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) -f net10.0 $(PERF_TESTS) {})

# Env-driven producer/consumer benchmarks (manual; need a reachable broker via
# BOOTSTRAP_SERVERS). CLIENT_VERSION selects the exe: 3 (default) = PerfV3 (our
# binding), 2 = PerfV2 (ckd baseline). The v3 exe P/Invokes our native, so it does
# the two-stage native build first; the v2/ckd exe needs no native
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

# In-suite v3 perf smoke (xUnit + Testcontainers). v3-ONLY gate (PerfV2 is
# manual-comparison only, never in this smoke). Two-stage native build first, then
# build the PerfV3 exe for BOTH TFMs (the smoke subprocess-launches PerfV3 by path,
# reusing the running test's config+TFM), then run the smoke on net10.0 ONLY. The
# perf gate is a p99-latency check, not TFM coverage: net8.0 stays covered
# functionally by test-dotnet (both TFMs); net10.0 is the designated execution gate
# (PerfV3.csproj still builds for all TFMs). Wired into `verify` (mirroring
# python/'s perf stage).
#
# PERF_REQUIRE_DOCKER=True makes a missing/broken Docker or a
# broker that will not start a FAILURE here, not a skip: this target is what
# `verify` reaches, so a green tick from it has to mean the budget was
# actually measured. A bare `dotnet test` (no Makefile) leaves it unset, and a
# Docker-less dev box then gets a real, visible `Skipped` instead. The value must
# be the case-sensitive "True" — the suite-wide Python-compatible convention; =1
# would silently not enable it.
#
# A latency smoke run at a fixed low rate (100 rps, p99 budget), like the Rust
# perf tests, so it runs on the RUST_PROFILE library (debug by default) as the
# other binding tests do; the PerfV2 / CLIENT_VERSION=2 benchmarks stay on
# release.
test-integration-perf-dotnet:
	$(MAKE) -C $(RUST_PROJECT_ROOT) $(RUST_BUILD_TARGET)
	$(DOTNET) build -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) $(PERF_V3)/PerfV3.csproj
	$(call selected_run,--section dotnet.performance --dotnet -- env PERF_REQUIRE_DOCKER=True $(DOTNET) test -c $(DOTNET_CONFIG) $(CARGO_PROFILE_PROP) -f net10.0 $(PERF_TESTS) {})
