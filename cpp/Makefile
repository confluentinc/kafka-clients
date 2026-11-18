REPO_ROOT ?= $(realpath ..)
RUST_PROJECT_ROOT ?= $(REPO_ROOT)/rust
BUILD_DIR = build
CMAKE_GENERATOR ?= Ninja
CLANG_FORMAT ?= clang-format
CLANG_TIDY ?= clang-tidy
# Extra arguments for clang-tidy, e.g. a resource directory when the binary
# ships without one: CLANG_TIDY_EXTRA_ARGS=--extra-arg=-isystem/usr/lib/gcc/x86_64-linux-gnu/15/include
CLANG_TIDY_EXTRA_ARGS ?=

# The linter's own code is formatted too; its vendored grammar, its fixtures (line numbers carry
# their expectations) and the build trees are not.
SOURCES := $(shell find include src test tools/lint \( -path tools/lint/build -o -path tools/lint/grammar \
	-o -path tools/lint/test/fixtures \) -prune \
	-o \( -name '*.hpp' -o -name '*.cpp' \) -print)
# Translation units clang-tidy processes; headers are covered through
# .clang-tidy's HeaderFilterRegex.
TIDY_SOURCES := $(shell find src test -name '*.cpp')

# The translation linter (tools/lint, cpp/CLAUDE.md §12-§13): a clang-tidy
# plugin, which loads only into the clang-tidy of the LLVM major it is built
# against (tools/lint/LLVM_VERSION).
LINT_BUILD_DIR = tools/lint/build
LLVM_MAJOR := $(shell cat tools/lint/LLVM_VERSION)
LINT_CLANG_TIDY ?= clang-tidy-$(LLVM_MAJOR)
FFI_INCLUDE = $(RUST_PROJECT_ROOT)/target/include
LINT_PLUGIN = $(LINT_BUILD_DIR)/libConfluentKafkaTidy.so
LINT_PROBE = $(LINT_BUILD_DIR)/probe/probe.cpp
# abi-check: BASELINE=<rev> picks the release compared with (default: the
# highest cpp/vX.Y.Z tag); STRICT=ON fails on a break instead of reporting it.
BASELINE ?=
STRICT ?= OFF

# Every target expects the Rust library already built under
# $(RUST_PROJECT_ROOT)/target: the repository root's Makefile orders that
# (`build-cpp` depends on the Rust build); run standalone, build it first.
.PHONY: build devel-build test test-unit format format-check lint lint-plugin lint-custom lint-tests \
	lint-baseline abi-check verify clean

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

lint: build lint-custom
	$(CLANG_TIDY) -p $(BUILD_DIR) --quiet $(CLANG_TIDY_EXTRA_ARGS) $(TIDY_SOURCES)

# Builds the plugin, ck_java_model and the linter's tests.
lint-plugin:
	cmake -S tools/lint -B $(LINT_BUILD_DIR) -G $(CMAKE_GENERATOR) \
		-DCK_LINT_FFI_INCLUDE_DIR=$(FFI_INCLUDE)
	cmake --build $(LINT_BUILD_DIR)

# The confluent-kafka-* checks on every public header: no finding.
lint-custom: lint-plugin
	cd $(LINT_BUILD_DIR) && ctest --output-on-failure -R '^lint_binding_headers$$'

# The linter's own tests: the Java model, the rules, a fixture per check, the
# mutations of real headers, the Javadoc word-count parity, abi-check.
lint-tests: lint-plugin
	cd $(LINT_BUILD_DIR) && ctest --output-on-failure

# Rewrites tools/lint/data/baseline.txt: one missing-class line per in-scope
# Java class that has no C++ translation yet.
lint-baseline: lint-plugin
	@mkdir -p $(dir $(LINT_PROBE))
	@find include/confluent/kafka -name '*.hpp' | sort | sed 's|^include/\(.*\)|#include <\1>|' > $(LINT_PROBE)
	$(LINT_CLANG_TIDY) -load $(LINT_PLUGIN) --quiet -header-filter='.*' \
		-config="{Checks: '-*,confluent-kafka-surface', CheckOptions: {ConfluentKafkaWriteBaseline: 'true'}}" \
		$(LINT_PROBE) -- -std=c++20 -Iinclude -isystem $(FFI_INCLUDE)

# The interface check against a previous release (cpp/CLAUDE.md §12.4).
abi-check: lint-plugin
	cmake -DREPO_ROOT=$(REPO_ROOT) -DSOURCE_DIR=$(CURDIR) -DWORK_DIR=$(CURDIR)/$(LINT_BUILD_DIR)/abi \
		-DCLANG_TIDY=$(LINT_CLANG_TIDY) -DPLUGIN=$(CURDIR)/$(LINT_PLUGIN) \
		-DCK_JAVA_MODEL=$(CURDIR)/$(LINT_BUILD_DIR)/ck_java_model -DFFI_INCLUDE=$(FFI_INCLUDE) \
		-DBASELINE=$(BASELINE) -DSTRICT=$(STRICT) -P tools/abi_check.cmake

# Everything the C++ binding checks: formatting, both linters, the unit tests,
# the linter's tests and the interface check.
verify: format-check lint test lint-tests abi-check

clean:
	rm -rf $(BUILD_DIR) $(LINT_BUILD_DIR)
