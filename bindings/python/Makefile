RUST_PROJECT_ROOT ?= $(realpath ../..)
PROFILE ?= release
CFLAGS_EXTRA ?=
GRPC_IMAGE_TAG ?= confluent-kafka-rust/python-grpc-server:dev
GRPC_ASYNC_IMAGE_TAG ?= confluent-kafka-rust/python-async-grpc-server:dev

.PHONY: build test test-soak test-performance grpc-image grpc-image-async \
	grpc-image-macos grpc-image-async-macos clean

build:
	CONFLUENT_KAFKA_LIB_DIR=$(RUST_PROJECT_ROOT)/target/$(PROFILE) \
	CFLAGS="$(CFLAGS_EXTRA)" \
	pip install -e .

test: build
	pip install .[dev] || pip install --no-dependencies .[dev]
	cd $(RUST_PROJECT_ROOT)/bindings/python && python -m pytest test/unit -v
	$(MAKE) test-soak
	$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-python

# Soak client unit tests (soak/test): the payload round-trip, the
# duplicate/gap accounting and the startup config validation. Pure logic, no
# broker and no container, so they belong in `test` above rather than alongside
# the broker-backed suites.
test-soak:
	cd $(RUST_PROJECT_ROOT)/bindings/python && python -m pytest soak/test -v

# Kept out of `test`: they assert latency budgets, so only meaningful on an
# otherwise-idle machine. Run via `make test-integration-perf-python` at the root.
test-performance: build
	pip install .[dev] || pip install --no-dependencies .[dev]
	cd $(RUST_PROJECT_ROOT)/bindings/python && python -m pytest test/performance -v

# Python gRPC server image for the multilang test harness. Context is the repo
# root; requires libconfluent_kafka.so already built in target/release/.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/python/Dockerfile.grpc \
		$(RUST_PROJECT_ROOT)

# Async variant of grpc-image; runs grpc_server_async.py.
grpc-image-async:
	docker build \
		-t $(GRPC_ASYNC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/python/Dockerfile.grpc.async \
		$(RUST_PROJECT_ROOT)

clean:
	rm -rf build *.egg-info __pycache__ *.so producer_service_pb2*.py
