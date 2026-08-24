RUST_PROJECT_ROOT ?= $(realpath ../..)
BUILD_DIR = build
PROFILE ?= release
CFLAGS_EXTRA ?=
GRPC_IMAGE_TAG ?= confluent-kafka-rust/c-grpc-server:dev
# Repo-root-relative dir the Dockerfile copies libconfluent_kafka.a from.
# Overridden on macOS to a cross-compiled Linux target dir -- see
# test-integration-c-macos in the root Makefile.
RUST_TARGET_DIR ?= target/release

.PHONY: build devel-build test grpc-image grpc-image-macos clean

build:
	cmake -S . -B $(BUILD_DIR) \
		-DRUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) \
		-DCMAKE_C_FLAGS="$(CFLAGS_EXTRA)"
	cmake --build $(BUILD_DIR)

devel-build: build

# `test` runs the CTest unit suite, then the C-backend arm of the Rust
# multilanguage integration suite (Rust test bodies driven against this binding
# over gRPC). The latter lives here rather than in the root `test-c` so the C
# binding owns every test of the C binding -- mirrors bindings/python's `test`.
#
# `-C $(RUST_PROJECT_ROOT)` and not `-C ..`: the parent directory is bindings/,
# which has no Makefile.
#
# Comments must stay outside the recipe: a tab-indented `#` line is handed to
# the shell, so make echoes it as if it were a command.
test: build
	cd $(BUILD_DIR) && ctest --output-on-failure
	$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-c

# Build the C++ gRPC server image used by the multilanguage integration
# test harness. The build context is the repo root so the Dockerfile can
# copy from target/release/, bindings/c/grpc_server/, and the proto
# crate. Requires `cargo build --features ffi --release` to have
# produced libconfluent_kafka.a in target/release/ first.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		--build-arg RUST_TARGET_DIR=$(RUST_TARGET_DIR) \
		-f $(RUST_PROJECT_ROOT)/bindings/c/Dockerfile.grpc \
		$(RUST_PROJECT_ROOT)

# macOS variant: builds libconfluent_kafka.a *inside* the image (see
# Dockerfile.grpc.macos) instead of copying in a host-built Mach-O archive
# this container's Linux ld can't read. No RUST_TARGET_DIR / host build
# needed first.
#
# --platform is required here: Dockerfile.grpc.macos's builder stage uses
# `FROM --platform=$BUILDPLATFORM`, and that build arg is only populated by
# BuildKit when a target --platform is given to `docker build` -- without
# it, $BUILDPLATFORM is empty and the FROM line fails to parse. Hardcoded
# to arm64 since the macOS CI agent (s1-macos-15-arm64-8) is arm64-only.
grpc-image-macos:
	docker build \
		--platform linux/arm64 \
		-t $(GRPC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/c/Dockerfile.grpc.macos \
		$(RUST_PROJECT_ROOT)

clean:
	rm -rf $(BUILD_DIR)
