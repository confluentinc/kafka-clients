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

.PHONY: build build-all \
	build-rust build-rust-integration-tests build-rust-all-features \
	submodules build-c init-venv build-python \
	devel-build devel-build-rust devel-build-rust-integration-tests devel-build-rust-all-features \
	devel-build-c devel-build-python \
	build-grpc-images build-grpc-images-python build-grpc-images-c init init-hooks \
	test test-rust test-integration test-integration-python test-integration-c \
	test-c test-python test-rust-all-features \
	test-c-macos-docker test-python-macos-docker \
	test-integration-perf test-integration-perf-rust test-integration-perf-python \
	producer-perf-test producer-perf-test-c \
	consumer-perf-test-python producer-perf-test-python \
	verify verify-c verify-python verify-rust \
	verify-rust-macos-docker verify-python-macos-docker verify-c-macos-docker \
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

# Just the two Python gRPC images (sync + asyncio). The Python image installs
# the built binding, hence `build-python`.
build-grpc-images-python: build-rust-all-features build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) grpc-image grpc-image-async)

# Just the C gRPC image. Needs only the Rust release artifacts (see
# c/Makefile's `grpc-image`), not the host-side `build-c`.
build-grpc-images-c: build-rust-all-features
	$(MAKE) -C c $(ROOTS) grpc-image

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

# The container-backed arms of the multilanguage suite, one per binding. Each
# binding's `test-integration` builds its gRPC image(s) and runs the matching
# `__grpc_*` tests of the Rust harness; the prerequisites here build the
# artifacts those images copy.
test-integration-python: build-rust-all-features build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) test-integration)

test-integration-c: build-rust-all-features
	$(MAKE) -C c $(ROOTS) test-integration

# ── Performance integration tests ────────────────────────────────────────
#
# Separated from the functional suites because they assert latency and
# throughput budgets; both targets expect an otherwise-idle machine.

test-integration-perf-rust:
	$(MAKE) -C rust test-integration-perf

test-integration-perf-python: build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=release test-performance)

# Rust and Python performance suites, one after the other. Recipe lines rather
# than prerequisites so the order holds under `make -j`, and so the two
# suites never overlap — each needs the machine to itself.
test-integration-perf:
	$(MAKE) test-integration-perf-rust
	$(MAKE) test-integration-perf-python

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

# c/'s `test` runs ctest and then the C-backend multilanguage arm.
test-c: build-c
	$(MAKE) -C c $(ROOTS) test

# macOS variant of test-c: C unit tests only. The gRPC multilanguage arm
# runs on Linux only (verify-c).
test-c-macos-docker: build-c
	$(MAKE) -C c $(ROOTS) test-unit

test-python: build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=release test)

# macOS variant of test-python: Python unit tests only. The gRPC
# multilanguage arm runs on Linux only (verify-python).
test-python-macos-docker: build-python
	@($(VENV) && $(MAKE) -C python $(ROOTS) PROFILE=release test-unit)

verify: build format-check lint test check-bindings

verify-c: test-c

verify-c-macos-docker: test-c-macos-docker

verify-python: test-python check-bindings
	$(MAKE) test-integration-perf-python

# macOS verify-python: unit tests.
verify-python-macos-docker: test-python-macos-docker

verify-rust: build-rust-all-features format-check lint test-rust-all-features
	$(MAKE) test-integration-perf-rust

# macOS verify-rust; the perf tail uses rust/Makefile's MACOS_P99_LIMIT_MS.
verify-rust-macos-docker: build-rust-all-features format-check lint test-rust-all-features
	$(MAKE) -C rust test-integration-perf-macos

verify-sandbox: build-rust build-c format-check lint test-integration test-c

init-hooks:
	@git config core.hooksPath .githooks
	@chmod +x .githooks/pre-commit

# Opt-in editor/AI-assistant tooling (see rust/Makefile); run by hand.
install-rust-analyzer:
	$(MAKE) -C rust install-rust-analyzer

format-check:
	$(MAKE) -C rust format-check

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
