RUST_PROJECT_ROOT ?= $(realpath ../..)
GRPC_IMAGE_TAG ?= confluent-kafka-rust/dotnet-grpc-server:dev

.PHONY: grpc-image

# Build the .NET gRPC consumer-backend image used by the multilanguage
# integration test harness. The build context is the repo root so the
# Dockerfile can copy from target/release/, bindings/dotnet/, and the
# multilanguage-test-server/ proto crate. Requires
# `cargo build --features ffi --release` to have produced the Linux
# libconfluent_kafka.so in target/release/ first.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/dotnet/Dockerfile.grpc \
		$(RUST_PROJECT_ROOT)
