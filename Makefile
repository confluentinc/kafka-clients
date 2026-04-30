RUST_PROJECT_ROOT = $(CURDIR)
RUSTFLAGS_NATIVE = -C target-cpu=native
CFLAGS_NATIVE = -march=native -mtune=native

.PHONY: build devel-build test test-rust test-integration test-c test-python verify format-check lint clean

build:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi --release
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) CFLAGS_EXTRA="$(CFLAGS_NATIVE)" build
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=release CFLAGS_EXTRA="$(CFLAGS_NATIVE)" build

devel-build:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) CFLAGS_EXTRA="$(CFLAGS_NATIVE)" PROFILE=debug devel-build
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=debug CFLAGS_EXTRA="$(CFLAGS_NATIVE)" build

# One-shot setup for a fresh clone or worktree: pulls down the git
# submodules (kafka source reference + Unity for the C unit tests).
# `build` dependency is needed for creating the C headers.
init: build
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

verify: format-check lint test

format-check:
	cargo xtask format-check

lint:
	cargo xtask lint

clean:
	cargo clean
	$(MAKE) -C bindings/c clean
	$(MAKE) -C bindings/python clean
