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

test: build
	cd $(BUILD_DIR) && ctest --output-on-failure

# Build the C++ gRPC server image used by the multilanguage integration
# test harness. The build context is the repo root so the Dockerfile can
# copy from target/release/, bindings/c/grpc_server/, and the proto
# crate. Requires `cargo build --features ffi --release` to have
# produced libconfluent_kafka.a in target/release/ first.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/c/Dockerfile.grpc \
		$(RUST_PROJECT_ROOT)

clean:
	rm -rf $(BUILD_DIR)
