RUST_PROJECT_ROOT = $(CURDIR)

.PHONY: build test test-rust test-integration test-c test-python verify format-check lint clean

build:
	cargo build --features ffi --release

test: test-integration test-c test-python

test-rust:
	cargo test

test-integration:
	cargo test --features integration-tests

test-c: build
	$(MAKE) -C bindings/c RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) test

test-python: build
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) test

verify: format-check lint test

format-check:
	cargo xtask format-check

lint:
	cargo xtask lint

clean:
	cargo clean
	$(MAKE) -C bindings/c clean
	$(MAKE) -C bindings/python clean
