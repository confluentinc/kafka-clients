RUST_PROJECT_ROOT = $(CURDIR)
RUSTFLAGS_NATIVE = -C target-cpu=native
CFLAGS_NATIVE = -march=native -mtune=native

.PHONY: init build devel-build test test-rust test-integration test-c test-python build-grpc-images test-multilanguage verify format-check lint clean

build:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi --release
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) CFLAGS_EXTRA="$(CFLAGS_NATIVE)" build
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=release CFLAGS_EXTRA="$(CFLAGS_NATIVE)" build

devel-build:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) CFLAGS_EXTRA="$(CFLAGS_NATIVE)" PROFILE=debug devel-build
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=debug CFLAGS_EXTRA="$(CFLAGS_NATIVE)" build

# Build the per-language gRPC server Docker images used by the
# multilanguage integration test harness. See
# design/history/MILESTONE-6/DESIGN-multilanguage-tests.md.
# Depends on the existing `build` target so libconfluent_kafka.{a,so}
# and confluent_kafka.h are present under target/release/.
build-grpc-images: build
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image

# One-shot setup for a fresh clone or worktree: pulls down the git
# submodules (kafka source reference + Unity for the C unit tests).
# Run this before `make build` on a new checkout.
init:
	@git submodule update --init --recursive
	@python3 -m venv venv
	@(. venv/bin/activate && cd bindings/python && pip install .[dev])

test: test-integration test-c test-python

test-rust:
	cargo test

test-integration:
	cargo test --features integration-tests

test-c: build
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) test

test-python: build
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=release test

# Run the producer integration suite three times — once per backend
# (rust / python / c). The Rust test process docker-runs the prebuilt
# images via testcontainers, so the only thing this target ensures
# beyond `cargo test` is that the images exist locally.
#
# Opt-in only — NOT part of `verify` until the harness has been stable
# in CI for one cycle.
test-multilanguage: build-grpc-images
	cargo test --features integration-tests,multilanguage-tests

verify: format-check lint test

format-check:
	cargo xtask format-check

lint:
	cargo xtask lint

clean:
	cargo clean
	$(MAKE) -C bindings/c clean
	$(MAKE) -C bindings/python clean
