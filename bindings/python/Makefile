RUST_PROJECT_ROOT ?= $(realpath ../..)
PROFILE ?= release
CFLAGS_EXTRA ?=
GRPC_IMAGE_TAG ?= confluent-kafka-rust/python-grpc-server:dev
GRPC_ASYNC_IMAGE_TAG ?= confluent-kafka-rust/python-async-grpc-server:dev

.PHONY: build test grpc-image grpc-image-async clean

build:
	CONFLUENT_KAFKA_LIB_DIR=$(RUST_PROJECT_ROOT)/target/$(PROFILE) \
	CFLAGS="$(CFLAGS_EXTRA)" \
	pip install -e .

test: build
	pip install .[dev] || pip install --no-dependencies .[dev]
	cd $(RUST_PROJECT_ROOT)/bindings/python && python -m pytest test/unit -v

# Build the Python gRPC server image used by the multilanguage
# integration test harness. The build context is the repo root so the
# Dockerfile can copy from target/, bindings/python/, and the
# multilanguage-test-server/ proto crate. Requires `make build` (or
# `make build` in the repo root) to have produced libconfluent_kafka.so
# in target/release/ first.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/python/Dockerfile.grpc \
		$(RUST_PROJECT_ROOT)

# Build the *async* Python gRPC server image (python_async backend). Same build
# context and prerequisites as grpc-image; runs grpc_server_async.py.
grpc-image-async:
	docker build \
		-t $(GRPC_ASYNC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/python/Dockerfile.grpc.async \
		$(RUST_PROJECT_ROOT)

clean:
	rm -rf build *.egg-info __pycache__ *.so producer_service_pb2*.py
