RUST_PROJECT_ROOT ?= $(realpath ../..)
PROFILE ?= release
CFLAGS_EXTRA ?=
GRPC_IMAGE_TAG ?= confluent-kafka-rust/python-grpc-server:dev
GRPC_ASYNC_IMAGE_TAG ?= confluent-kafka-rust/python-async-grpc-server:dev

.PHONY: build test typecheck test-performance grpc-image grpc-image-async \
	grpc-image-macos grpc-image-async-macos clean

build:
	CONFLUENT_KAFKA_LIB_DIR=$(RUST_PROJECT_ROOT)/target/$(PROFILE) \
	CFLAGS="$(CFLAGS_EXTRA)" \
	pip install -e .

# Static type check of the new `confluent_kafka` package (spec §11 / rule 11):
# `mypy --strict` catches stub/implementation drift so it fails `make verify`.
# `test/unit/test_typing.py` carries `typing.assert_type` assertions that
# machine-verify the `@overload` stubs and the generics inference, so it is type
# checked here too — a broken stub fails `make verify`.
typecheck:
	cd $(RUST_PROJECT_ROOT)/bindings/python && python -m mypy --strict confluent_kafka test/unit/test_typing.py

test: build typecheck
	pip install .[dev] || pip install --no-dependencies .[dev]
	cd $(RUST_PROJECT_ROOT)/bindings/python && python -m pytest test/unit -v
	$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-python

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

# macOS variants: build libconfluent_kafka.so inside the image (see
# Dockerfile.grpc.macos), no host build needed. --platform pins the arm64
# manifest on Colima's plain docker driver.
grpc-image-macos:
	docker build \
		--platform linux/arm64 \
		-t $(GRPC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/python/Dockerfile.grpc.macos \
		$(RUST_PROJECT_ROOT)

grpc-image-async-macos:
	docker build \
		--platform linux/arm64 \
		-t $(GRPC_ASYNC_IMAGE_TAG) \
		-f $(RUST_PROJECT_ROOT)/bindings/python/Dockerfile.grpc.async.macos \
		$(RUST_PROJECT_ROOT)

clean:
	rm -rf build *.egg-info __pycache__ *.so producer_service_pb2*.py
