RUST_PROJECT_ROOT ?= $(realpath ../..)
BUILD_DIR = build
PROFILE ?= release
CFLAGS_EXTRA ?=

.PHONY: build devel-build test clean

build:
	cmake -S . -B $(BUILD_DIR) \
		-DRUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) \
		-DCMAKE_C_FLAGS="$(CFLAGS_EXTRA)"
	cmake --build $(BUILD_DIR)

devel-build: build

test: build
	cd $(BUILD_DIR) && ctest --output-on-failure

clean:
	rm -rf $(BUILD_DIR)
