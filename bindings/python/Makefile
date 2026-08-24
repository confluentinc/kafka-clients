RUST_PROJECT_ROOT ?= $(realpath ../..)
PROFILE ?= release
CFLAGS_EXTRA ?=
GRPC_IMAGE_TAG ?= confluent-kafka-rust/python-grpc-server:dev
GRPC_ASYNC_IMAGE_TAG ?= confluent-kafka-rust/python-async-grpc-server:dev
# Repo-root-relative dir the Dockerfiles copy libconfluent_kafka.so from.
# Overridden on macOS to a cross-compiled Linux target dir -- see
# test-integration-python-macos in the root Makefile.
RUST_TARGET_DIR ?= target/release

.PHONY: build test test-performance grpc-image grpc-image-async \
	grpc-image-macos grpc-image-async-macos clean

build:
	CONFLUENT_KAFKA_LIB_DIR=$(RUST_PROJECT_ROOT)/target/$(PROFILE) \
	CFLAGS="$(CFLAGS_EXTRA)" \
	pip install -e .

test: build
	pip install .[dev] || pip install --no-dependencies .[dev]
	cd $(RUST_PROJECT_ROOT)/bindings/python && python -m pytest test/unit -v
	$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-python

# Performance tests (test/performance), kept out of `test` above on purpose:
# they assert latency budgets, so they are only meaningful on an otherwise-idle
# machine. Their `kafka_broker` session fixture (test/performance/conftest.py)
# starts its own container, so nothing else may be competing for the host.
#
# Invoked from the repo root as `make test-integration-perf-python`, which
# sequences them after the functional suites.
test-performance: build
	pip install .[dev] || pip install --no-dependencies .[dev]
	cd $(RUST_PROJECT_ROOT)/bindings/python && python -m pytest test/performance -v

# Build the Python gRPC server image used by the multilanguage
# integration test harness. The build context is the repo root so the
# Dockerfile can copy from target/, bindings/python/, and the
# multilanguage-test-server/ proto crate. Requires `make build` (or
# `make build` in the repo root) to have produced libconfluent_kafka.so
# in target/release/ first.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		--build-arg RUST_TARGET_DIR=$(RUST_TARGET_DIR) \
		-f $(RUST_PROJECT_ROOT)/bindings/python/Dockerfile.grpc \
		$(RUST_PROJECT_ROOT)

# Build the *async* Python gRPC server image (python_async backend). Same build
# context and prerequisites as grpc-image; runs grpc_server_async.py.
grpc-image-async:
	docker build \
		-t $(GRPC_ASYNC_IMAGE_TAG) \
		--build-arg RUST_TARGET_DIR=$(RUST_TARGET_DIR) \
		-f $(RUST_PROJECT_ROOT)/bindings/python/Dockerfile.grpc.async \
		$(RUST_PROJECT_ROOT)

# macOS variants: build libconfluent_kafka.so *inside* the image (see
# Dockerfile.grpc.macos) instead of copying in a host-built Mach-O dylib
# this container's Linux ld.so can't load. No RUST_TARGET_DIR / host build
# needed first.
grpc-image-macos:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/python/Dockerfile.grpc.macos \
		$(RUST_PROJECT_ROOT)

grpc-image-async-macos:
	docker build \
		-t $(GRPC_ASYNC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/python/Dockerfile.grpc.async.macos \
		$(RUST_PROJECT_ROOT)

clean:
	rm -rf build *.egg-info __pycache__ *.so producer_service_pb2*.py
