# Cross-language Makefile. Each language owns its recipes in its own Makefile
# -- rust/Makefile, python/Makefile, c/Makefile -- and this one only
# orchestrates them: it orders the builds across languages (the bindings link
# the Rust library, so it is built first) and owns what belongs to the whole
# repository (git submodules and hooks, the shared Python venv).
REPO_ROOT = $(CURDIR)
RUST_PROJECT_ROOT = $(REPO_ROOT)/rust
ROOTS = REPO_ROOT=$(REPO_ROOT) RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT)
# `$(MAKE)` is spelled out in every delegating recipe line rather than hidden
# in a variable: GNU make only treats a line as a recursive make -- passing
# down the -j jobserver, -n, -k -- when `$(MAKE)` appears in it literally.
# Python targets run inside the shared venv.
VENV = . venv/bin/activate

.PHONY: build build-all check-generated \
	build-rust build-rust-integration-tests build-rust-all-features \
	submodules build-c init-venv build-python \
	devel-build devel-build-rust devel-build-rust-integration-tests devel-build-rust-all-features \
	devel-build-c devel-build-python \
	build-grpc-images build-grpc-images-python build-grpc-images-c build-grpc-images-dotnet init init-hooks \
	test test-rust test-integration test-integration-python test-integration-c test-integration-dotnet \
	test-integration-python-ssl test-integration-python-sasl-ssl \
	test-integration-c-ssl test-integration-c-sasl-ssl \
	test-integration-dotnet-ssl test-integration-dotnet-sasl-ssl \
	build-grpc-native-python build-grpc-native-c build-grpc-native-dotnet \
	test-integration-python-native test-integration-c-native test-integration-dotnet-native \
	test-integration-plaintext test-integration-ssl test-integration-sasl-ssl \
	test-c test-python test-dotnet test-rust-all-features \
	test-rust-all-features-ssl test-rust-all-features-sasl-ssl \
	test-c-macos-docker test-python-macos-docker test-dotnet-macos-docker \
	test-integration-perf test-integration-perf-rust test-integration-perf-python \
	test-integration-perf-dotnet \
	producer-perf-test producer-perf-test-c \
	consumer-perf-test-python producer-perf-test-python \
	producer-perf-test-dotnet consumer-perf-test-dotnet \
	verify verify-c verify-python verify-dotnet verify-rust \
	verify-rust-macos-docker verify-python-macos-docker verify-c-macos-docker \
	verify-dotnet-macos-docker \
	verify-sandbox format-check lint doc-check package-check install-rust-analyzer clean

build: init-hooks build-all

build-all: build-rust-all-features build-c build-python

build-rust:
	$(MAKE) -C rust build
build-rust-integration-tests:
	$(MAKE) -C rust build-integration-tests
build-rust-all-features:
	$(MAKE) -C rust build-all-features

submodules:
	git submodule update --init --recursive

build-c: submodules build-rust-all-features
	$(MAKE) -C c $(ROOTS) build

init-venv:
	[ -d venv ] || python3 -m venv venv

build-python: init-venv build-rust-all-features
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=release build)

devel-build: devel-build-rust devel-build-c devel-build-python

devel-build-rust:
	$(MAKE) -C rust devel-build
devel-build-rust-integration-tests:
	$(MAKE) -C rust devel-build-integration-tests
devel-build-rust-all-features:
	$(MAKE) -C rust devel-build-all-features

devel-build-c: submodules devel-build-rust
	$(MAKE) -C c $(ROOTS) devel-build

devel-build-python: init-venv devel-build-rust
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=debug build)

# Build the per-language gRPC server Docker images used by the
# multilanguage integration test harness. See
# design/history/MILESTONE-6/DESIGN-multilanguage-tests.md.
# Depends on `build` so libconfluent_kafka.{a,so} and confluent_kafka.h are
# present under rust/target/release/.
build-grpc-images: build
	@($(VENV) && $(MAKE) -C python $(ROOTS) grpc-image grpc-image-async)
	$(MAKE) -C c $(ROOTS) grpc-image
	$(MAKE) -C dotnet $(ROOTS) grpc-image grpc-image-async

# Just the two Python gRPC images (sync + asyncio). The Python image installs
# the built binding, hence `build-python`.
build-grpc-images-python: build-rust-all-features build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) grpc-image grpc-image-async)

# Just the C gRPC image. Needs only the Rust release artifacts (see
# c/Makefile's `grpc-image`), not the host-side `build-c`.
build-grpc-images-c: build-rust-all-features
	$(MAKE) -C c $(ROOTS) grpc-image

# The two .NET gRPC images (sync + async), for the .NET-only multilanguage run
# below. Mirrors build-grpc-images-python (also two images). Needs only the Rust
# release artifacts: dotnet/Dockerfile.grpc / Dockerfile.grpc.async copy
# rust/target/release/libconfluent_kafka.so + rust/target/include/confluent_kafka.h
# and build the .NET gRPC server inside the container (which brings its own SDK),
# so unlike build-grpc-images-python this does NOT depend on a host-side
# binding build. Images are platform-agnostic: CI is amd64-native; local
# Apple-Silicon dev sets DOCKER_DEFAULT_PLATFORM=linux/amd64 in its environment
# (Grpc.Tools' linux_arm64 protoc segfaults in an arm64 Linux container), never a
# Makefile-baked --platform.
build-grpc-images-dotnet: build-rust-all-features
	$(MAKE) -C dotnet $(ROOTS) grpc-image grpc-image-async

# One-shot setup for a fresh clone or worktree: pulls down the git
# submodules (kafka source reference + Unity for the C unit tests) and the
# Python development dependencies. Run this before `make build` on a new
# checkout.
init: submodules init-venv
	@($(VENV) && $(MAKE) -C python $(ROOTS) init)

test: test-rust-all-features test-c test-python

test-rust:
	$(MAKE) -C rust test

# Every feature compiled, only the native-Rust tests run (see rust/Makefile).
# Part of `verify` (through `test`) and of the CI `verify-rust` job.
test-rust-all-features:
	$(MAKE) -C rust test-all-features

# Functional integration tests (Docker required).
test-integration:
	$(MAKE) -C rust test-integration

# The functional integration tests over one listener of the broker, selected
# by INTEGRATION_TEST_PROTOCOL (see rust/Makefile). For local use.
test-integration-plaintext:
	$(MAKE) -C rust test-integration-plaintext
test-integration-ssl:
	$(MAKE) -C rust test-integration-ssl
test-integration-sasl-ssl:
	$(MAKE) -C rust test-integration-sasl-ssl

# `test-rust-all-features` over the SSL / SASL_SSL listener, without the
# format/lint/perf checks of `verify-rust` (see rust/Makefile). Run by the
# SSL / SASL_SSL CI jobs.
test-rust-all-features-ssl:
	$(MAKE) -C rust test-all-features-ssl
test-rust-all-features-sasl-ssl:
	$(MAKE) -C rust test-all-features-sasl-ssl

# The container-backed arms of the multilanguage suite, one per binding. Each
# binding's `test-integration` builds its gRPC image(s) and runs the matching
# `__grpc_*` tests of the Rust harness; the prerequisites here build the
# artifacts those images copy.
test-integration-python: build-rust-all-features build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) test-integration)

test-integration-c: build-rust-all-features
	$(MAKE) -C c $(ROOTS) test-integration

# .NET's container arm, like python's and c's: dotnet/'s `test-integration` builds
# the sync and async .NET gRPC images and runs the `__grpc_dotnet` tests (through
# rust/Makefile's test-integration-grpc-dotnet, which also carries the .NET-only
# DOTNET_GRPC_SKIPS). It self-skips off Linux, with test-integration-dotnet-native
# below as the fallback the macOS CI verify-dotnet job runs; see dotnet/Makefile.
test-integration-dotnet: build-rust-all-features
	$(MAKE) -C dotnet $(ROOTS) test-integration

# The gRPC Python/C/.NET arms above default to the PLAINTEXT CONTAINER listener.
# These variants drive the same arm over the SSL / SASL_SSL CONTAINER
# listeners (CTLSONLY :9100 / CSASLTLS :9101 — the container-reachable twins
# of the client SSL/SASL_SSL listeners, advertised on the container hostname
# so a sibling gRPC container can reach them). INTEGRATION_TEST_PROTOCOL is
# read by TestProtocol::from_env(); the harness injects the matching
# security.protocol / TLS truststore / SASL settings into the config it
# forwards verbatim to the gRPC server's client constructor. They inherit the
# non-Linux SELF-SKIP from the targets they delegate to.
test-integration-python-ssl:
	INTEGRATION_TEST_PROTOCOL=ssl $(MAKE) test-integration-python
test-integration-python-sasl-ssl:
	INTEGRATION_TEST_PROTOCOL=sasl_ssl $(MAKE) test-integration-python
test-integration-c-ssl:
	INTEGRATION_TEST_PROTOCOL=ssl $(MAKE) test-integration-c
test-integration-c-sasl-ssl:
	INTEGRATION_TEST_PROTOCOL=sasl_ssl $(MAKE) test-integration-c
test-integration-dotnet-ssl:
	INTEGRATION_TEST_PROTOCOL=ssl $(MAKE) test-integration-dotnet
test-integration-dotnet-sasl-ssl:
	INTEGRATION_TEST_PROTOCOL=sasl_ssl $(MAKE) test-integration-dotnet

# ── Native (non-container) gRPC multilanguage arms ───────────────────────
#
# The same `__grpc_python` / `__grpc_c` / `__grpc_dotnet` arms, but with each
# gRPC server run as a host process instead of a Docker container
# (MULTILANG_BACKEND_MODE=native; see rust/tests/common/backend_pool.rs). Only
# the broker runs in Docker. These targets are used on macOS, where containers
# run Linux: the container arms can only test the Linux build of the bindings
# and cannot load the host's Mach-O
# artifacts. Native mode is also supported on Linux.
#
# Build outputs are written under rust/target/grpc-native/ to keep the source
# tree clean:
#   python/  generated gRPC stubs, put on PYTHONPATH by the harness
#   c/       the kafka_grpc_server binary
#   dotnet/  the .NET gRPC server's net10.0 build (Confluent.Kafka.GrpcServer.dll
#            plus the native library), run with the `dotnet` host
build-grpc-native-python: build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) grpc-native)

build-grpc-native-c: build-rust-all-features
	$(MAKE) -C c $(ROOTS) grpc-native

# Builds the .NET gRPC server's net10.0 leg against the host's release
# libconfluent_kafka (dotnet/'s grpc-native target). Requires the .NET 10
# SDK on PATH, which brings the ASP.NET Core 10 runtime the server runs on. On an
# Apple-Silicon host Grpc.Tools runs its macosx_x64 protoc under Rosetta 2.
build-grpc-native-dotnet: build-rust-all-features
	$(MAKE) -C dotnet $(ROOTS) grpc-native

test-integration-python-native: build-grpc-native-python
	$(MAKE) -C rust test-integration-grpc-python-native

test-integration-c-native: build-grpc-native-c
	$(MAKE) -C rust test-integration-grpc-c-native

# Skips the same three transaction tests as test-integration-dotnet
# (DOTNET_GRPC_SKIPS, see rust/Makefile).
test-integration-dotnet-native: build-grpc-native-dotnet
	$(MAKE) -C rust test-integration-grpc-dotnet-native

# ── Performance integration tests ────────────────────────────────────────
#
# Separated from the functional suites because they assert latency and
# throughput budgets; both targets expect an otherwise-idle machine.

test-integration-perf-rust:
	$(MAKE) -C rust test-integration-perf

test-integration-perf-python: build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=release test-performance)

# Rust, Python and .NET performance suites, one after the other. Recipe lines
# rather than prerequisites so the order holds under `make -j`, and so the
# suites never overlap — each needs the machine to itself. The .NET arm is the
# Docker-gated v3 smoke (skips cleanly without Docker), alongside the Python one.
test-integration-perf:
	$(MAKE) test-integration-perf-rust
	$(MAKE) test-integration-perf-python
	$(MAKE) test-integration-perf-dotnet

# Env-driven producer and consumer benchmarks, one per language, sharing one
# env-var contract (see each language's Makefile). Keep them in sync.
producer-perf-test:
	$(MAKE) -C rust producer-perf-test

producer-perf-test-c: build-c
	$(MAKE) -C c $(ROOTS) producer-perf-test

consumer-perf-test-python: build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) consumer-perf-test)

producer-perf-test-python: build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) producer-perf-test)

# Env-driven .NET producer/consumer performance benchmarks (manual; need a
# reachable broker via BOOTSTRAP_SERVERS). Delegate into dotnet/ — its
# own producer-perf-test-dotnet / consumer-perf-test-dotnet do the two-stage
# native build (cargo --features ffi) then `dotnet run` the exe selected by
# CLIENT_VERSION (3 = PerfV3/our binding, default; 2 = PerfV2/ckd baseline), so
# there is no build-rust prerequisite here (mirrors how test-dotnet delegates
# below). Pass $(ROOTS) so the delegated cargo/dotnet resolve the same
# repo and Rust roots; CLIENT_VERSION passes through the environment.
producer-perf-test-dotnet:
	$(MAKE) -C dotnet $(ROOTS) producer-perf-test-dotnet

consumer-perf-test-dotnet:
	$(MAKE) -C dotnet $(ROOTS) consumer-perf-test-dotnet

# .NET in-suite performance smoke (xUnit + Testcontainers). Delegate into
# dotnet/, which builds the native + PerfV3 exe then runs the Docker-gated
# net10.0-only smoke (skips cleanly without Docker). Alongside
# test-integration-perf-python above; now part of verify-dotnet (mirroring
# verify-python's perf stage).
test-integration-perf-dotnet:
	$(MAKE) -C dotnet $(ROOTS) test-integration-perf-dotnet

# c/'s `test` runs ctest and then the C-backend multilanguage arm.
test-c: build-c
	$(MAKE) -C c $(ROOTS) test

# macOS variant of test-c: C unit tests (ctest) only. On macOS the gRPC
# multilanguage arm runs natively instead (see verify-c-macos-docker).
test-c-macos-docker: build-c
	$(MAKE) -C c $(ROOTS) test-unit

test-python: build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=release test)

# Delegates to dotnet/'s own `test-dotnet` (native cargo build -> dotnet
# build matrix -> dotnet format -> unit tests on net8.0 + net10.0), mirroring how
# test-python / test-c delegate to their bindings. The delegated build does the
# Rust native step itself (CLAUDE.md §7.1 two-stage order), so no build-rust
# prerequisite here. Passes $(ROOTS) so the delegated cargo/dotnet
# invocations resolve the same repo and Rust roots.
test-dotnet:
	$(MAKE) -C dotnet $(ROOTS) test-dotnet

# macOS variant of test-dotnet: same target verbatim (native build -> dotnet
# build matrix -> dotnet format -> unit tests on net8.0 + net10.0). Unlike
# test-c-macos-docker / test-python-macos-docker, .NET has no OS-specific
# recipe to swap in -- the delegated dotnet/Makefile step is already
# platform-agnostic -- so this is a thin alias, kept for naming symmetry with
# the other macOS-block targets. On macOS the gRPC multilanguage arm runs
# natively instead (see verify-dotnet-macos-docker); this build never touches
# grpc-server (not in the .sln).
test-dotnet-macos-docker: test-dotnet

# macOS variant of test-python: Python unit tests (pytest test/unit) only. On
# macOS the gRPC multilanguage arm runs natively instead (see
# verify-python-macos-docker).
test-python-macos-docker: build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=release test-unit)

verify: build format-check check-generated lint test check-bindings

verify-c: test-c

# Also runs the gRPC multilanguage arm with the gRPC server as a native host
# process (see test-integration-c-native), since the Linux container arm cannot
# run on macOS.
verify-c-macos-docker: test-c-macos-docker
	$(MAKE) test-integration-c-native

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

# macOS verify-dotnet: unit tests, then the gRPC multilanguage arm with the gRPC
# server as a native host process (see test-integration-dotnet-native), as
# verify-c-macos-docker / verify-python-macos-docker do. No perf stage.
verify-dotnet-macos-docker: test-dotnet-macos-docker
	$(MAKE) test-integration-dotnet-native

# macOS verify-python: Python unit tests, then the gRPC multilanguage arm with
# the gRPC server as a native host process, as verify-c-macos-docker does.
verify-python-macos-docker: test-python-macos-docker
	$(MAKE) test-integration-python-native

verify-rust: build-rust-all-features format-check check-generated lint test-rust-all-features
	$(MAKE) test-integration-perf-rust

# macOS verify-rust; the perf tail uses rust/Makefile's MACOS_P99_LIMIT_MS.
verify-rust-macos-docker: build-rust-all-features format-check check-generated lint test-rust-all-features
	$(MAKE) -C rust test-integration-perf-macos

verify-sandbox: build-rust build-c format-check check-generated lint test-integration test-c

init-hooks:
	@git config core.hooksPath .githooks
	@chmod +x .githooks/pre-commit

# Opt-in editor/AI-assistant tooling (see rust/Makefile); run by hand.
install-rust-analyzer:
	$(MAKE) -C rust install-rust-analyzer

format-check:
	$(MAKE) -C rust format-check

# Checks the generated protocol code's formatting and that the checked-in
# error-code tables are current; it reads the build's output, so it follows
# a build in every verify target.
check-generated:
	$(MAKE) -C rust check-generated

lint:
	$(MAKE) -C rust lint

doc-check:
	$(MAKE) -C rust doc-check

package-check:
	$(MAKE) -C rust package-check

# Static checks of the Python binding (see python/Makefile's `check-static`).
# Needs no build artifacts, so it is cheap to run.
check-bindings:
	@($(VENV) && $(MAKE) -C python $(ROOTS) check-static)

clean:
	$(MAKE) -C rust clean
	$(MAKE) -C c $(ROOTS) clean
	@($(VENV) && $(MAKE) -C python $(ROOTS) clean)
