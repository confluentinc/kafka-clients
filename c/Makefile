REPO_ROOT ?= $(realpath ..)
RUST_PROJECT_ROOT ?= $(REPO_ROOT)/rust
# The cargo profile directory of the Rust library every target links:
# $(RUST_PROJECT_ROOT)/target/$(RUST_PROFILE). The tests use the debug
# all-features build; the root's `build` passes release-cdylib, the library as
# it ships (rust/Makefile `build-cdylib`: the platform verifier, no Mozilla
# bundle), and producer-perf-test always links it.
RUST_PROFILE ?= debug
# The rust/Makefile target that builds that library: the shipped build
# (release-cdylib), the release all-features build (release) or the debug
# all-features build.
RUST_BUILD_TARGET = $(if $(filter release-cdylib,$(RUST_PROFILE)),build-cdylib,$(if $(filter release,$(RUST_PROFILE)),build-all-features,devel-build-all-features))
BUILD_DIR = build
# The tag of the gRPC image; rust/tests/common/backend_pool.rs reads the same
# variable. Override it per worktree so concurrent sessions do not share images.
MULTILANG_IMAGE_TAG ?= dev
GRPC_IMAGE_TAG ?= confluent-kafka-rust/c-grpc-server:$(MULTILANG_IMAGE_TAG)
# Native (non-container) gRPC server builds for the multilanguage harness,
# written under the Rust target dir, where rust/tests/common/backend_pool.rs
# looks for them in MULTILANG_BACKEND_MODE=native.
GRPC_NATIVE_DIR = $(RUST_PROJECT_ROOT)/target/grpc-native
GRPC_PROTO_DIR = $(RUST_PROJECT_ROOT)/multilanguage-test-server/proto
ARCH := $(shell uname -m)

ifeq ($(ARCH),x86_64)
  # x86-64-v3 (Haswell+, ~2013): faster than baseline, portable across v3 hosts.
  CFLAGS_NATIVE = -march=x86-64-v3 -mtune=generic
else
  CFLAGS_NATIVE = -mtune=generic
endif
CFLAGS_EXTRA ?= $(CFLAGS_NATIVE)

# `$(call selected_run,<options> -- <command>)` runs the command only when the
# test selection (the file TEST_SELECTION names, see the root's
# `select-tests`) selects its section, and always when none applies;
# an argument `{}` stands for the selected tests. See `cargo xtask help`
# (`selected-run`). The subshell keeps the `cd`, which cargo needs to find the
# toolchain and the `xtask` alias, from leaking into the rest of the recipe.
selected_run = (cd $(RUST_PROJECT_ROOT) && cargo -q xtask selected-run -C $(CURDIR) $(1))

# Every target builds what it needs first: the Rust library of $(RUST_PROFILE)
# (rust-lib) and, for the host build, the Unity submodule the unit tests use.
.PHONY: rust-lib build devel-build test test-unit test-integration test-integration-ssl \
	test-integration-sasl-ssl test-integration-native verify verify-except-grpc-arm verify-macos producer-perf-test \
	grpc-image grpc-native clean

# The Rust library and its headers, built by rust/Makefile; a cargo no-op once
# they are current.
rust-lib:
	$(MAKE) -C $(RUST_PROJECT_ROOT) $(RUST_BUILD_TARGET)

build: rust-lib
	git -C $(REPO_ROOT) submodule update --init c/tests/unity
	cmake -S . -B $(BUILD_DIR) \
		-DRUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) \
		-DRUST_PROFILE=$(RUST_PROFILE) \
		-DCMAKE_C_FLAGS="$(CFLAGS_EXTRA)"
	cmake --build $(BUILD_DIR)

devel-build: build

# CTest unit suite, then the C-backend arm of the Rust multilanguage
# integration suite.
test: test-unit test-integration

# The C gate: the unit tests, then the gRPC multilanguage arm.
verify: test

# `verify` without the gRPC multilanguage arm: the CI job "verify-c (Linux
# amd64) — plaintext". CI runs the arm in "verify-c (Linux amd64) — plaintext
# gRPC arm" (`test-integration`), as it runs the ssl / sasl_ssl arms in jobs of
# their own.
verify-except-grpc-arm: test-unit

# The macOS gate: the unit tests, then the gRPC multilanguage arm with the gRPC
# server as a native host process, since the Linux container arm cannot run on
# macOS.
verify-macos: test-unit
	$(MAKE) test-integration-native

test-unit: build
	$(call selected_run,--section c.unit --ctest -C $(CURDIR)/$(BUILD_DIR) -- ctest --output-on-failure {})

# The C-backend arm of the multilanguage suite: build the gRPC server image,
# then run the `__grpc_c` tests of the Rust harness against it.
#
# Cross-arch guard: Dockerfile.grpc COPYs the *host-built*
# rust/target/$(RUST_PROFILE)/libconfluent_kafka.a (+ confluent_kafka.h) straight into
# a Linux container and links it with the container's GNU ld. On a non-Linux
# host that artifact is the wrong object format (Mach-O on macOS), so the image
# cannot build/link. Cross-compiling host->Linux is out of scope for a dev-box
# run, so off Linux this SELF-SKIPS with a loud notice and exits 0. It is NOT
# skipped in CI, which runs on Linux. The skip is honest -- it never claims the
# container arm passed, only that this host cannot build the Linux image.
#
# The image build is invoked *inside* the recipe (rather than as a
# prerequisite) so the skip also short-circuits the Docker build.
test-integration: rust-lib
	@if [ "$$(uname -s)" != "Linux" ]; then \
		printf '\n========================================================================\n'; \
		printf 'SKIP C test-integration: host is %s, not Linux.\n' "$$(uname -s)"; \
		printf '\n'; \
		printf 'The C gRPC-server image COPYs the host-built\n'; \
		printf '  rust/target/$(RUST_PROFILE)/libconfluent_kafka.a\n'; \
		printf 'into a Linux container and links it with GNU ld. On this host that\n'; \
		printf 'artifact is Mach-O, so the image cannot build/link.\n'; \
		printf 'This container arm runs only in CI'"'"'s Linux verify-c gRPC arm jobs.\n'; \
		printf 'Run it natively on this host with: %smake -C c test-integration-native\n' \
			"$${INTEGRATION_TEST_PROTOCOL:+INTEGRATION_TEST_PROTOCOL=$$INTEGRATION_TEST_PROTOCOL }"; \
		printf '========================================================================\n\n'; \
	else \
		$(call selected_run,--section c.integration -- $(MAKE) -C $(CURDIR) grpc-image) && \
		$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-grpc-c; \
	fi

# The same arm over the SSL / SASL_SSL CONTAINER listeners (CTLSONLY :9100 /
# CSASLTLS :9101), which INTEGRATION_TEST_PROTOCOL selects
# (rust/tests/common/test_context.rs); the harness passes the matching TLS and
# SASL settings to the gRPC server. They inherit the non-Linux skip.
test-integration-ssl:
	INTEGRATION_TEST_PROTOCOL=ssl $(MAKE) test-integration
test-integration-sasl-ssl:
	INTEGRATION_TEST_PROTOCOL=sasl_ssl $(MAKE) test-integration

# The C arm with the gRPC server as a host process instead of a container
# (MULTILANG_BACKEND_MODE=native, rust/tests/common/backend_pool.rs): only the
# broker runs in Docker. Used on macOS; also works on Linux.
test-integration-native: grpc-native
	$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-grpc-c-native

# C producer performance benchmark (opt-in; needs a reachable broker). Same
# env-var contract as the Rust `producer-perf-test`; select the backend with
# CLIENT_VERSION (3 = Rust client C bindings, default; 2 = librdkafka
# baseline). Built by `build` but intentionally not run under `test`. It links
# the library as it ships (release-cdylib), as a benchmark should.
producer-perf-test: RUST_PROFILE = release-cdylib
producer-perf-test: build
	./$(BUILD_DIR)/producer_perf_test

# C++ gRPC server image for the multilang test harness. Context is the repo
# root; it copies libconfluent_kafka.a from rust/target/$(RUST_PROFILE)/.
# Needs only the Rust artifacts: the Dockerfile runs cmake on
# grpc_server/ inside the container, so the host-side `build` contributes
# nothing the image consumes.
grpc-image: rust-lib
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		--build-arg RUST_PROFILE=$(RUST_PROFILE) \
		-f $(REPO_ROOT)/c/Dockerfile.grpc \
		$(REPO_ROOT)

# The C++ gRPC server for the native (host-process) C arm, linked against the
# host's libconfluent_kafka.a. Requires the gRPC C++ and protobuf development
# packages (headers, protoc, grpc_cpp_plugin, pkg-config) on the host.
grpc-native: rust-lib
	cmake -S grpc_server -B $(GRPC_NATIVE_DIR)/c-build -DCMAKE_BUILD_TYPE=Release \
		-DRUST_LIB_DIR=$(RUST_PROJECT_ROOT)/target/$(RUST_PROFILE) \
		-DRUST_INCLUDE_DIR=$(RUST_PROJECT_ROOT)/target/include \
		-DPROTO_DIR=$(GRPC_PROTO_DIR)
	cmake --build $(GRPC_NATIVE_DIR)/c-build --parallel
	mkdir -p $(GRPC_NATIVE_DIR)/c
	cp $(GRPC_NATIVE_DIR)/c-build/kafka_grpc_server $(GRPC_NATIVE_DIR)/c/kafka_grpc_server

clean:
	rm -rf $(BUILD_DIR)
