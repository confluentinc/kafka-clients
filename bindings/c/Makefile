RUST_PROJECT_ROOT ?= $(realpath ../..)
BUILD_DIR = build
PROFILE ?= release
CFLAGS_EXTRA ?=
GRPC_IMAGE_TAG ?= confluent-kafka-rust/c-grpc-server:dev
# Dir the Linux Dockerfile.grpc copies libconfluent_kafka.a from.
RUST_TARGET_DIR ?= target/release

.PHONY: build devel-build test grpc-image grpc-image-macos clean

build:
	cmake -S . -B $(BUILD_DIR) \
		-DRUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) \
		-DCMAKE_C_FLAGS="$(CFLAGS_EXTRA)"
	cmake --build $(BUILD_DIR)

devel-build: build

# CTest unit suite, then the C-backend arm of the Rust multilang integration
# suite. -C $(RUST_PROJECT_ROOT) because the parent dir (bindings/) has no Makefile.
test: build
	cd $(BUILD_DIR) && ctest --output-on-failure
	$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-c

# C++ gRPC server image for the multilang test harness. Context is the repo
# root; requires libconfluent_kafka.a already built in target/release/.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		--build-arg RUST_TARGET_DIR=$(RUST_TARGET_DIR) \
		-f $(RUST_PROJECT_ROOT)/bindings/c/Dockerfile.grpc \
		$(RUST_PROJECT_ROOT)

# macOS variant: builds libconfluent_kafka.a inside the image (see
# Dockerfile.grpc.macos), no host build needed. --platform pins the arm64
# manifest on Colima's plain docker driver.
grpc-image-macos:
	docker build \
		--platform linux/arm64 \
		-t $(GRPC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/c/Dockerfile.grpc.macos \
		$(RUST_PROJECT_ROOT)

clean:
	rm -rf $(BUILD_DIR)
