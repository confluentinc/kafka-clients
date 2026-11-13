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
	build-rust build-rust-integration-tests build-rust-all-features build-rust-tests-all-features \
	submodules build-c init-venv init-venv-librdkafka build-python \
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
	pack-dotnet package-check-dotnet \
	test-integration-perf test-integration-perf-rust test-integration-perf-python \
	test-integration-perf-dotnet \
	producer-perf-test producer-perf-test-c \
	consumer-perf-test-python producer-perf-test-python \
	transactional-producer-perf-test-python \
	producer-perf-test-dotnet consumer-perf-test-dotnet \
	verify verify-c verify-python verify-dotnet verify-rust \
	verify-rust-macos-docker verify-python-macos-docker verify-c-macos-docker \
	verify-dotnet-macos-docker \
	verify-commit verify-commit-checks format-check lint clippy-default-features doc-check package-check check-api-compat audit bump-version install-rust-analyzer \
	select-tests pre-push-test-selection clean-test-selection clean

build: init-hooks build-all

build-all: build-rust-all-features build-c build-python

build-rust:
	$(MAKE) -C rust build
build-rust-integration-tests:
	$(MAKE) -C rust build-integration-tests
build-rust-all-features:
	$(MAKE) -C rust build-all-features
build-rust-tests-all-features:
	$(MAKE) -C rust build-tests-all-features

submodules:
	git submodule update --init --recursive

# The release C build, for packaging and the C benchmark. The tests use
# devel-build-c.
build-c: submodules build-rust-all-features
	$(MAKE) -C c $(ROOTS) RUST_PROFILE=release build

# The second line is a one-time migration: master's `[dev]` extras once
# installed the PyPI confluent-kafka (librdkafka) into venv/, and it shares the
# top-level package `confluent_kafka` with this client. Leaving `[dev]` stops new
# installs but does not remove it from an existing venv, where it shadows the
# package (`cannot import name 'IllegalArgumentError' from 'confluent_kafka'`).
# The librdkafka baseline lives in its own venv (init-venv-librdkafka).
init-venv:
	[ -d venv ] || python3 -m venv venv
	@if venv/bin/python -m pip show -q confluent-kafka >/dev/null 2>&1; then \
	  echo "Removing PyPI confluent-kafka from venv/ (it shadows confluent_kafka)"; \
	  venv/bin/python -m pip uninstall -y -q confluent-kafka; fi

# The librdkafka venv of the Python perf benchmarks: the PyPI confluent-kafka
# client for the CLIENT_VERSION=2 baseline and for the librdkafka-backed helpers
# (topic recreation, end-of-run verification; see
# python/test/performance/librdkafka_helpers.py). It is separate from
# `venv` because both packages install the top-level package `confluent_kafka`.
LIBRDKAFKA_VENV ?= venv-librdkafka
LIBRDKAFKA_PYTHON := $(abspath $(LIBRDKAFKA_VENV))/bin/python

init-venv-librdkafka:
	[ -d $(LIBRDKAFKA_VENV) ] || python3 -m venv $(LIBRDKAFKA_VENV)
	$(LIBRDKAFKA_PYTHON) -m pip install -q -r python/test/performance/requirements-librdkafka.txt

# The release Python build, for packaging and the Python benchmarks. The tests
# use devel-build-python.
build-python: init-venv build-rust-all-features
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=release build)

# The debug builds the binding tests use. They link the debug all-features
# library of devel-build-rust-all-features, the library unit the Rust tests
# (build-rust-tests-all-features) already build, so after the Rust tests it is a
# no-op (see rust/Makefile `devel-build-all-features`).
devel-build: devel-build-c devel-build-python

devel-build-rust:
	$(MAKE) -C rust devel-build
devel-build-rust-integration-tests:
	$(MAKE) -C rust devel-build-integration-tests
devel-build-rust-all-features:
	$(MAKE) -C rust devel-build-all-features

devel-build-c: submodules devel-build-rust-all-features
	$(MAKE) -C c $(ROOTS) RUST_PROFILE=debug devel-build

devel-build-python: init-venv devel-build-rust-all-features
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=debug build)

# Build the per-language gRPC server Docker images used by the
# multilanguage integration test harness. See
# design/history/MILESTONE-6/DESIGN-multilanguage-tests.md.
# Depends on devel-build-rust-all-features so the debug libconfluent_kafka.{a,so}
# are present under rust/target/debug/, and confluent_kafka.h (the C image) and
# confluent_kafka_bindings.h (the Python images) under rust/target/include/.
build-grpc-images: devel-build-rust-all-features
	@($(VENV) && $(MAKE) -C python $(ROOTS) grpc-image grpc-image-async)
	$(MAKE) -C c $(ROOTS) grpc-image
	$(MAKE) -C dotnet $(ROOTS) grpc-image grpc-image-async

# Just the two Python gRPC images (sync + asyncio). The Python image installs
# the built binding, hence `devel-build-python`.
build-grpc-images-python: devel-build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) grpc-image grpc-image-async)

# Just the C gRPC image. Needs only the debug Rust artifacts (see
# c/Makefile's `grpc-image`), not the host-side `devel-build-c`.
build-grpc-images-c: devel-build-rust-all-features
	$(MAKE) -C c $(ROOTS) grpc-image

# The two .NET gRPC images (sync + async), for the .NET-only multilanguage run
# below. Mirrors build-grpc-images-python (also two images). Needs only the debug
# Rust artifacts: dotnet/Dockerfile.grpc / Dockerfile.grpc.async copy
# rust/target/debug/libconfluent_kafka.so + rust/target/include/confluent_kafka.h
# and build the .NET gRPC server inside the container (which brings its own SDK),
# so unlike build-grpc-images-python this does NOT depend on a host-side
# binding build. Images are platform-agnostic: CI is amd64-native; local
# Apple-Silicon dev sets DOCKER_DEFAULT_PLATFORM=linux/amd64 in its environment
# (Grpc.Tools' linux_arm64 protoc segfaults in an arm64 Linux container), never a
# Makefile-baked --platform.
build-grpc-images-dotnet: devel-build-rust-all-features
	$(MAKE) -C dotnet $(ROOTS) grpc-image grpc-image-async

# One-shot setup for a fresh clone or worktree: pulls down the git
# submodules (kafka source reference + Unity for the C unit tests) and the
# Python development dependencies, and registers the git hooks (init-hooks).
# Run this before `make build` on a new checkout.
init: submodules init-venv init-hooks
	@($(VENV) && $(MAKE) -C python $(ROOTS) init)

test: test-rust-all-features test-c test-python

test-rust:
	$(MAKE) -C rust test

# Every feature compiled, only the native-Rust tests run (see rust/Makefile).
# Part of `verify` (through `test`) and `verify-rust`; the CI PLAINTEXT Rust job
# runs it alone, the checks of `verify-rust` running in their own CI job.
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
test-integration-python: devel-build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=debug test-integration)

test-integration-c: devel-build-rust-all-features
	$(MAKE) -C c $(ROOTS) test-integration

# .NET's container arm, like python's and c's: dotnet/'s `test-integration` builds
# the sync and async .NET gRPC images and runs the `__grpc_dotnet` tests (through
# rust/Makefile's test-integration-grpc-dotnet). It self-skips off Linux, with
# test-integration-dotnet-native below as the fallback the macOS CI verify-dotnet
# job runs; see dotnet/Makefile.
test-integration-dotnet: devel-build-rust-all-features
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
build-grpc-native-python: devel-build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) grpc-native)

build-grpc-native-c: devel-build-rust-all-features
	$(MAKE) -C c $(ROOTS) grpc-native

# Builds the .NET gRPC server's net10.0 leg against the host's debug
# all-features libconfluent_kafka (dotnet/'s grpc-native target). Requires the .NET 10
# SDK on PATH, which brings the ASP.NET Core 10 runtime the server runs on. On an
# Apple-Silicon host Grpc.Tools runs its macosx_x64 protoc under Rosetta 2.
build-grpc-native-dotnet: devel-build-rust-all-features
	$(MAKE) -C dotnet $(ROOTS) grpc-native

test-integration-python-native: build-grpc-native-python
	$(MAKE) -C rust test-integration-grpc-python-native

test-integration-c-native: build-grpc-native-c
	$(MAKE) -C rust test-integration-grpc-c-native

test-integration-dotnet-native: build-grpc-native-dotnet
	$(MAKE) -C rust test-integration-grpc-dotnet-native

# ── Performance integration tests ────────────────────────────────────────
#
# Separated from the functional suites because they assert latency and
# throughput budgets; both targets expect an otherwise-idle machine.

test-integration-perf-rust:
	$(MAKE) -C rust test-integration-perf

# A latency smoke run at a fixed low rate (100 rps, p99 budget), like the Rust
# perf tests, so it runs on the same debug all-features library as the other
# binding tests; the throughput benchmarks below stay on release.
test-integration-perf-python: devel-build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=debug test-performance)

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
	$(MAKE) -C c $(ROOTS) RUST_PROFILE=release producer-perf-test

# The Python benchmarks also need the librdkafka venv: CLIENT_VERSION=2 runs the
# librdkafka baseline in it, and the Rust run uses it for its librdkafka-backed
# helpers (see python/Makefile).
consumer-perf-test-python: build-python init-venv-librdkafka
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=release LIBRDKAFKA_PYTHON=$(LIBRDKAFKA_PYTHON) consumer-perf-test)

producer-perf-test-python: build-python init-venv-librdkafka
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=release LIBRDKAFKA_PYTHON=$(LIBRDKAFKA_PYTHON) producer-perf-test)

transactional-producer-perf-test-python: build-python init-venv-librdkafka
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=release LIBRDKAFKA_PYTHON=$(LIBRDKAFKA_PYTHON) transactional-producer-perf-test)

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
test-c: devel-build-c
	$(MAKE) -C c $(ROOTS) RUST_PROFILE=debug test

# macOS variant of test-c: C unit tests (ctest) only. On macOS the gRPC
# multilanguage arm runs natively instead (see verify-c-macos-docker).
test-c-macos-docker: devel-build-c
	$(MAKE) -C c $(ROOTS) RUST_PROFILE=debug test-unit

test-python: devel-build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=debug test)

# Delegates to dotnet/'s own `test-dotnet` (native cargo build -> dotnet
# build matrix -> dotnet format -> the solution's unit and integration tests on
# net8.0 + net10.0 -> the soak tests), mirroring how
# test-python / test-c delegate to their bindings. The delegated build does the
# Rust native step itself (CLAUDE.md §7.1 two-stage order), so no build-rust
# prerequisite here. Passes $(ROOTS) so the delegated cargo/dotnet
# invocations resolve the same repo and Rust roots.
test-dotnet:
	$(MAKE) -C dotnet $(ROOTS) test-dotnet

# macOS variant of test-dotnet: the same delegation with the integration tests
# left out through dotnet/'s DOTNET_TEST_FILTER (the unit and soak tests still
# run), as test-python-macos-docker runs Python's unit tests only, so the
# broker-backed tests run on Linux only. On macOS the gRPC multilanguage arm runs
# natively instead (see verify-dotnet-macos-docker); this build never touches
# grpc-server (not in the .sln).
DOTNET_MACOS_TEST_FILTER = FullyQualifiedName!~Confluent.Kafka.IntegrationTests

test-dotnet-macos-docker:
	$(MAKE) -C dotnet $(ROOTS) DOTNET_TEST_FILTER='$(DOTNET_MACOS_TEST_FILTER)' test-dotnet

# The Confluent.Kafka NuGet package, with this machine's native library only:
# pack-dotnet packs it into dotnet/artifacts/package, and package-check-dotnet
# packs it, then installs it into a fresh project and runs the smoke program
# there. See dotnet/Makefile; the release package is built by semaphore.yml's
# "Release (.NET):" blocks.
pack-dotnet:
	$(MAKE) -C dotnet $(ROOTS) pack-dotnet

package-check-dotnet:
	$(MAKE) -C dotnet $(ROOTS) package-check-dotnet

# macOS variant of test-python: Python unit tests (pytest test/unit) only. On
# macOS the gRPC multilanguage arm runs natively instead (see
# verify-python-macos-docker).
test-python-macos-docker: devel-build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=debug test-unit)

verify: build format-check check-generated lint test check-bindings

verify-c: test-c

# Also runs the gRPC multilanguage arm with the gRPC server as a native host
# process (see test-integration-c-native), since the Linux container arm cannot
# run on macOS.
verify-c-macos-docker: test-c-macos-docker
	$(MAKE) test-integration-c-native

verify-python: test-python check-bindings
	$(MAKE) test-integration-perf-python

# verify-dotnet = build + format + the unit and integration tests (net8+net10) +
# soak + the gRPC arms (__grpc_dotnet[_async]) + the Docker-gated net10.0 p99 perf
# stage (landed M13/P1), mirroring verify-python's shape (which appends
# test-integration-perf-python above). The allocation-budget assertions also live
# in the unit suite. Recipe lines rather than prerequisites so each stage runs
# strictly after the prior gate, matching verify-python's ordering.
verify-dotnet: test-dotnet
	$(MAKE) test-integration-dotnet
	$(MAKE) test-integration-perf-dotnet

# macOS verify-dotnet: the unit and soak tests, then the gRPC multilanguage arm
# with the gRPC server as a native host process (see
# test-integration-dotnet-native), as verify-c-macos-docker /
# verify-python-macos-docker do. No perf stage.
verify-dotnet-macos-docker: test-dotnet-macos-docker
	$(MAKE) test-integration-dotnet-native

# macOS verify-python: Python unit tests, then the gRPC multilanguage arm with
# the gRPC server as a native host process, as verify-c-macos-docker does.
verify-python-macos-docker: test-python-macos-docker
	$(MAKE) test-integration-python-native

# The Rust verify jobs compile once, the debug all-features build the tests run
# (rust/Makefile `build-tests-all-features`), and build nothing in release: no
# Rust test runs a release artifact, and the binding jobs reuse the same debug
# all-features build (devel-build-rust-all-features). The build comes before
# `check-generated`, which reads its generated code.
verify-rust: build-rust-tests-all-features format-check check-generated lint test-rust-all-features
	$(MAKE) test-integration-perf-rust

# macOS verify-rust; the perf tail uses rust/Makefile's MACOS_P99_LIMIT_MS.
verify-rust-macos-docker: build-rust-tests-all-features format-check check-generated lint test-rust-all-features
	$(MAKE) -C rust test-integration-perf-macos

# The pre-commit hook's verification. It first selects the tests the changes
# since the last commit can affect into its own .test-selection-commit.json
# (keeping a selection already made there at HEAD for the same working tree,
# see Test selection), then verifies with that selection. If the selection
# fails there is no file, and every test runs.
verify-commit: export TEST_SELECTION = .test-selection-commit.json
verify-commit:
	$(MAKE) select-tests BASE=HEAD SELECT_TESTS_FLAGS=--reuse || \
		echo "verify-commit: the test selection failed, so every test runs"
	$(MAKE) verify-commit-checks

verify-commit-checks: devel-build-c format-check check-generated lint test-integration test-c

# Registers .githooks: pre-commit runs verify-commit, pre-push
# pre-push-test-selection.
init-hooks:
	@git config core.hooksPath .githooks
	@chmod +x .githooks/pre-commit .githooks/pre-push

# Opt-in editor/AI-assistant tooling (see rust/Makefile); run by hand.
install-rust-analyzer:
	$(MAKE) -C rust install-rust-analyzer

format-check:
	$(MAKE) -C rust format-check

# Checks that the message specs copied into rust/generator/messages match the
# kafka submodule's, the generated protocol code's formatting, and that the
# checked-in error-code tables are current; it reads the build's output, so it
# follows a build in every verify target.
check-generated:
	$(MAKE) -C rust check-generated

lint:
	$(MAKE) -C rust lint

clippy-default-features:
	$(MAKE) -C rust clippy-default-features

doc-check:
	$(MAKE) -C rust doc-check

package-check:
	$(MAKE) -C rust package-check

check-api-compat:
	$(MAKE) -C rust check-api-compat

# Audits the Rust dependencies with cargo-audit: see rust/Makefile.
audit:
	$(MAKE) -C rust audit

# `make bump-version VERSION=0.3.0-rc.1 [PYTHON_VERSION=...] [DOTNET_VERSION=...]`:
# see rust/Makefile. VERSION, PYTHON_VERSION and DOTNET_VERSION reach the
# sub-make through its environment (make passes command-line variables down);
# this recipe never pastes them in.
bump-version:
	$(MAKE) -C rust bump-version

# Static checks of the Python binding (see python/Makefile's `check-static`).
# Needs no build artifacts, so it is cheap to run.
check-bindings:
	@($(VENV) && $(MAKE) -C python $(ROOTS) check-static)

# ── Test selection ───────────────────────────────────────────────────────
#
# `make select-tests` gives an agent the changes from BASE (committed,
# uncommitted and untracked) and writes the tests they can affect, per
# language, to test-selection/test-selection-<HEAD>.json (or to
# $TEST_SELECTION, relative to this directory, when it names a .json file),
# together with the working tree it was made for, and deletes every file in
# test-selection/ but the four most recent. The agent is told to include a
# test whenever in doubt. Commit test-selection/ with the branch: the
# pre-push hook (pre-push-test-selection) does it, as a commit of its own,
# when a pushed HEAD has no selection made for it.
#
# A selection applies only where TEST_SELECTION names it, a file or the
# folder, of which the file made for the working tree applies: CI sets it to
# test-selection on a pull request build, `verify-commit` to its own
# .test-selection-commit.json, and it can be set by hand
# (`TEST_SELECTION=test-selection make test`). Then every test recipe
# (`test`, `verify`, the integration and performance targets, in Rust, C,
# Python and .NET) runs only the selected tests, and a suite the file leaves out does
# not run at all, but only while the working tree is the one the file was made
# for: after any other change, every test runs. Without TEST_SELECTION every
# test runs. A failed `select-tests` removes the previous file rather than
# leave it in force.
#
# AGENT is any command taking Claude Code's `--model`, `--restricted`,
# `--strict-mcp-config`, `--tools`, `--allowedTools`, `--disallowedTools` and
# `-p`: by default a local Claude Code, which reads the working tree, so
# uncommitted and untracked changes count. The agent only reads files and runs
# read-only git commands: restricted mode ignores the user's settings files, so
# only the read-only tools are allowed, and an agent that has not answered after
# SELECT_TESTS_FLAGS' `--timeout` (600 s by default) is stopped and the selection
# fails. rust/xtask/src/test_selection.rs describes the file's format.
AGENT ?= claude
AGENT_MODEL ?= opus
BASE ?= origin/master
# `--reuse` keeps a file made at HEAD that covers every current change.
SELECT_TESTS_FLAGS ?=

select-tests:
	cd rust && cargo xtask select-tests --base "$(BASE)" --agent "$(AGENT)" --model "$(AGENT_MODEL)" $(SELECT_TESTS_FLAGS)

# The pre-push hook's task, reading the pushed refs on standard input: when
# HEAD is pushed and its committed test-selection/ holds no selection made for
# it, it selects for HEAD, commits test-selection/ (with --no-verify), and
# stops the push, which git cannot extend with a commit made by its hook: push
# again. A working tree holding changes HEAD does not, or a failed selection,
# lets the push go on without one, and CI then runs every test. It builds only
# xtask's test-selection binary.
pre-push-test-selection:
	cd rust && cargo run -q --locked -p xtask --bin test-selection -- pre-push --base "$(BASE)" --agent "$(AGENT)" --model "$(AGENT_MODEL)"

# Removes the selections of test-selection/ and the .json file TEST_SELECTION
# names, if any.
clean-test-selection:
	rm -f test-selection/test-selection-*.json
	case "$${TEST_SELECTION:-}" in *.json) rm -f "$$TEST_SELECTION" ;; esac

clean:
	$(MAKE) -C rust clean
	$(MAKE) -C c $(ROOTS) clean
	@($(VENV) && $(MAKE) -C python $(ROOTS) clean)
