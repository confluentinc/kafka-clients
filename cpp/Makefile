REPO_ROOT ?= $(realpath ..)
RUST_PROJECT_ROOT ?= $(REPO_ROOT)/rust
BUILD_DIR = build
CMAKE_GENERATOR ?= Ninja
CLANG_FORMAT ?= clang-format
CLANG_TIDY ?= clang-tidy
# Extra arguments for clang-tidy, e.g. a resource directory when the binary
# ships without one: CLANG_TIDY_EXTRA_ARGS=--extra-arg=-isystem/usr/lib/gcc/x86_64-linux-gnu/15/include
CLANG_TIDY_EXTRA_ARGS ?=

# The linter's own code is formatted too; its vendored grammar and build trees are not.
SOURCES := $(shell find include test tools/lint \( -path tools/lint/build -o -path tools/lint/grammar \) -prune \
	-o \( -name '*.hpp' -o -name '*.cpp' \) -print)
# Translation units clang-tidy processes; headers are covered through
# .clang-tidy's HeaderFilterRegex.
TIDY_SOURCES := $(shell find test -name '*.cpp')

# Every target expects the Rust library already built under
# $(RUST_PROJECT_ROOT)/target: the repository root's Makefile orders that
# (`build-cpp` depends on the Rust build); run standalone, build it first.
.PHONY: build devel-build test test-unit format format-check lint clean

build:
	cmake -S . -B $(BUILD_DIR) -G $(CMAKE_GENERATOR) \
		-DRUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) \
		-DCMAKE_EXPORT_COMPILE_COMMANDS=ON
	cmake --build $(BUILD_DIR)

devel-build: build

test: test-unit

test-unit: build
	cd $(BUILD_DIR) && ctest --output-on-failure

format:
	$(CLANG_FORMAT) -i $(SOURCES)

format-check:
	$(CLANG_FORMAT) --dry-run --Werror $(SOURCES)

lint: build
	$(CLANG_TIDY) -p $(BUILD_DIR) --quiet $(CLANG_TIDY_EXTRA_ARGS) $(TIDY_SOURCES)

clean:
	rm -rf $(BUILD_DIR)
