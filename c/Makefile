REPO_ROOT ?= $(realpath ..)
RUST_PROJECT_ROOT ?= $(REPO_ROOT)/rust
BUILD_DIR = build
GRPC_IMAGE_TAG ?= confluent-kafka-rust/c-grpc-server:dev
ARCH := $(shell uname -m)

ifeq ($(ARCH),x86_64)
  # x86-64-v3 (Haswell+, ~2013): faster than baseline, portable across v3 hosts.
  CFLAGS_NATIVE = -march=x86-64-v3 -mtune=generic
else
  CFLAGS_NATIVE = -mtune=generic
endif
CFLAGS_EXTRA ?= $(CFLAGS_NATIVE)

# Every target expects the Rust library already built under
# $(RUST_PROJECT_ROOT)/target: the repository root's Makefile orders that
# (e.g. `build-c` depends on the Rust build); run standalone, build it first.
.PHONY: build devel-build test test-unit test-integration producer-perf-test grpc-image clean

build:
	cmake -S . -B $(BUILD_DIR) \
		-DRUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) \
		-DCMAKE_C_FLAGS="$(CFLAGS_EXTRA)"
	cmake --build $(BUILD_DIR)

devel-build: build

# CTest unit suite, then the C-backend arm of the Rust multilanguage
# integration suite.
test: test-unit test-integration

test-unit: build
	cd $(BUILD_DIR) && ctest --output-on-failure

# The C-backend arm of the multilanguage suite: build the gRPC server image,
# then run the `__grpc_c` tests of the Rust harness against it.
#
# Cross-arch guard: Dockerfile.grpc COPYs the *host-built*
# rust/target/release/libconfluent_kafka.a (+ confluent_kafka.h) straight into
# a Linux container and links it with the container's GNU ld. On a non-Linux
# host that artifact is the wrong object format (Mach-O on macOS), so the image
# cannot build/link. Cross-compiling host->Linux is out of scope for a dev-box
# run, so off Linux this SELF-SKIPS with a loud notice and exits 0. It is NOT
# skipped in CI, which runs on Linux. The skip is honest -- it never claims the
# container arm passed, only that this host cannot build the Linux image.
#
# The image build is invoked *inside* the recipe (rather than as a
# prerequisite) so the skip also short-circuits the Docker build.
test-integration:
	@if [ "$$(uname -s)" != "Linux" ]; then \
		printf '\n========================================================================\n'; \
		printf 'SKIP C test-integration: host is %s, not Linux.\n' "$$(uname -s)"; \
		printf '\n'; \
		printf 'The C gRPC-server image COPYs the host-built\n'; \
		printf '  rust/target/release/libconfluent_kafka.a\n'; \
		printf 'into a Linux container and links it with GNU ld. On this host that\n'; \
		printf 'artifact is Mach-O, so the image cannot build/link.\n'; \
		printf 'This container arm runs only in CI'"'"'s Linux verify-c job.\n'; \
		printf '========================================================================\n\n'; \
	else \
		$(MAKE) grpc-image && \
		$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-grpc-c; \
	fi

# C producer performance benchmark (opt-in; needs a reachable broker). Same
# env-var contract as the Rust `producer-perf-test`; select the backend with
# CLIENT_VERSION (3 = Rust client C bindings, default; 2 = librdkafka
# baseline). Built by `build` but intentionally not run under `test`.
producer-perf-test: build
	./$(BUILD_DIR)/producer_perf_test

# C++ gRPC server image for the multilang test harness. Context is the repo
# root; requires libconfluent_kafka.a already built in rust/target/release/.
# Needs only the Rust release artifacts: the Dockerfile runs cmake on
# grpc_server/ inside the container, so the host-side `build` contributes
# nothing the image consumes.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		-f $(REPO_ROOT)/c/Dockerfile.grpc \
		$(REPO_ROOT)

clean:
	rm -rf $(BUILD_DIR)
