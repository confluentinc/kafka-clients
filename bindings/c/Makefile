RUST_PROJECT_ROOT ?= $(realpath ../..)
BUILD_DIR = build

.PHONY: build test clean

build:
	cd $(RUST_PROJECT_ROOT) && cargo build --features ffi --release
	cmake -S . -B $(BUILD_DIR)
	cmake --build $(BUILD_DIR)

test: build
	cd $(BUILD_DIR) && ctest --output-on-failure

clean:
	rm -rf $(BUILD_DIR)
