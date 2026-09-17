RUST_PROJECT_ROOT ?= $(realpath ../..)
BUILD_DIR = build
PROFILE ?= release
CFLAGS_EXTRA ?=
GRPC_IMAGE_TAG ?= confluent-kafka-rust/c-grpc-server:dev

.PHONY: build devel-build test grpc-image clean

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
		-f $(RUST_PROJECT_ROOT)/bindings/c/Dockerfile.grpc \
		$(RUST_PROJECT_ROOT)

clean:
	rm -rf $(BUILD_DIR)
