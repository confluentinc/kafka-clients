RUST_PROJECT_ROOT = $(CURDIR)
RUSTFLAGS_NATIVE = -C target-cpu=native
CFLAGS_NATIVE = -march=native -mtune=native

.PHONY: all build build-rust submodules build-c build-all init-venv build-python devel-build devel-build-rust devel-build-c devel-build-python init init-hooks test test-rust test-integration test-c test-python verify format-check lint clean

build: init-hooks build-all

build-all: build-rust build-c build-python

build-rust:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi --release

submodules:
	git submodule update --init --recursive

build-c: submodules build-rust
	cmake -S bindings/c -B bindings/c/build -DRUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) -DCMAKE_C_FLAGS="$(CFLAGS_NATIVE)"
	cmake --build bindings/c/build

build-python: submodules build-rust
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=release CFLAGS_EXTRA="$(CFLAGS_NATIVE)" build

devel-build: devel-build-rust devel-build-c devel-build-python

devel-build-rust:
	RUSTFLAGS="$(RUSTFLAGS_NATIVE)" cargo build --features ffi

devel-build-c: submodules devel-build-rust
	cmake -S bindings/c -B bindings/c/build -DRUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) -DCMAKE_C_FLAGS="$(CFLAGS_NATIVE)"
	cmake --build bindings/c/build

devel-build-python: submodules init-venv devel-build-rust
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=debug CFLAGS_EXTRA="$(CFLAGS_NATIVE)" build

test: test-integration test-c test-python

test-rust: build-rust
	cargo test

test-integration: build-rust
	cargo test --features integration-tests

test-c: build-c
	cd bindings/c/build && ctest --output-on-failure

test-python: build-python
	$(MAKE) -C bindings/python RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) PROFILE=release test

verify: build format-check lint test

verify-sandbox: build-all format-check lint test

init-hooks:
	@git config core.hooksPath .githooks
	@chmod +x .githooks/pre-commit

format-check:
	cargo xtask format-check

lint:
	cargo xtask lint

clean:
	cargo clean
	rm -rf bindings/c/build
	$(MAKE) -C bindings/python clean
