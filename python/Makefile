REPO_ROOT ?= $(realpath ..)
RUST_PROJECT_ROOT ?= $(REPO_ROOT)/rust
PROFILE ?= release
GRPC_IMAGE_TAG ?= confluent-kafka-rust/python-grpc-server:dev
GRPC_ASYNC_IMAGE_TAG ?= confluent-kafka-rust/python-async-grpc-server:dev
ARCH := $(shell uname -m)

ifeq ($(ARCH),x86_64)
  # x86-64-v3 (Haswell+, ~2013): faster than baseline, portable across v3 hosts.
  CFLAGS_NATIVE = -march=x86-64-v3 -mtune=generic
else
  CFLAGS_NATIVE = -mtune=generic
endif
CFLAGS_EXTRA ?= $(CFLAGS_NATIVE)

# Every target runs in the Python environment the caller activated (the
# repository root's Makefile activates its venv first) and, except
# `check-static` and `test-soak`, expects the Rust library already built under
# $(RUST_PROJECT_ROOT)/target/$(PROFILE): the root Makefile orders that.
.PHONY: init build test test-unit check-static test-soak test-integration test-performance \
	consumer-perf-test producer-perf-test grpc-image grpc-image-async clean

# Development dependencies, for a fresh clone (the root `init`).
init:
	pip install .[dev]

build:
	CONFLUENT_KAFKA_LIB_DIR=$(RUST_PROJECT_ROOT)/target/$(PROFILE) \
	CFLAGS="$(CFLAGS_EXTRA)" \
	pip install -e .

test: build
	pip install .[dev] || pip install --no-dependencies .[dev]
	python -m pytest test/unit -v
	$(MAKE) check-static
	$(MAKE) test-soak
	$(MAKE) test-integration

# Unit tests only (pytest test/unit). The gRPC multilanguage arm runs on Linux
# only; this is what the macOS verify job runs.
test-unit: build
	pip install .[dev] || pip install --no-dependencies .[dev]
	python -m pytest test/unit -v

# Static checks (test/static): the Py_BuildValue / PyArg_Parse* /
# PyObject_CallFunction format-arity scan of _confluentkafka.c, and the
# staleness check of the generated _error_code.py. A format one unit short of
# its argument list compiles silently and reads a garbage pointer at run time,
# and for every admin RPC that Java's MockAdminClient leaves unsupported the
# affected drain's success path is unreachable from the test suite, so this
# defect class must be caught statically. They read source files only, so they
# need neither the Rust library nor the C extension built.
check-static:
	python -m pytest test/static -v

# Soak client unit tests (soak/test): the payload round-trip, the
# duplicate/gap accounting and the startup config validation. Pure logic, no
# broker and no container, so they belong in `test` above rather than alongside
# the broker-backed suites.
test-soak:
	python -m pytest soak/test -v

# The Python-backend arms of the multilanguage suite: build the sync and
# asyncio gRPC server images, then run the `__grpc_python` tests of the Rust
# harness against them (the filter also matches `__grpc_python_async`).
#
# Cross-arch guard: both Dockerfiles COPY the *host-built*
# rust/target/release/libconfluent_kafka.so straight into a Linux container.
# On a non-Linux host that artifact is Mach-O / absent (no ELF .so), so the
# image cannot build. Cross-compiling host->Linux is out of scope for a dev-box
# run, so off Linux this SELF-SKIPS with a loud notice and exits 0. It is NOT
# skipped in CI, which runs on Linux. The skip is honest -- it never claims the
# container arms passed, only that this host cannot build the Linux images.
#
# The image builds are invoked *inside* the recipe (rather than as
# prerequisites) so the skip also short-circuits the Docker builds.
test-integration:
	@if [ "$$(uname -s)" != "Linux" ]; then \
		printf '\n========================================================================\n'; \
		printf 'SKIP Python test-integration: host is %s, not Linux.\n' "$$(uname -s)"; \
		printf '\n'; \
		printf 'The Python gRPC-server images COPY the host-built\n'; \
		printf '  rust/target/release/libconfluent_kafka.so\n'; \
		printf 'into a Linux container and link it with GNU ld. On this host that\n'; \
		printf 'artifact is Mach-O / absent (no ELF .so), so the image cannot build.\n'; \
		printf 'This container arm runs only in CI'"'"'s Linux verify-python job.\n'; \
		printf '========================================================================\n\n'; \
	else \
		$(MAKE) grpc-image grpc-image-async && \
		$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-grpc-python; \
	fi

# Kept out of `test`: they assert latency budgets, so only meaningful on an
# otherwise-idle machine. Run via `make test-integration-perf-python` at the root.
test-performance: build
	pip install .[dev] || pip install --no-dependencies .[dev]
	python -m pytest test/performance -v

# Env-driven consumer end-to-end latency benchmark (standalone). Mirrors
# rust/consumer-perf/compare/benchmark_e2e_latency.c. Needs a reachable broker
# via BOOTSTRAP_SERVERS; set KAFKA_BIN to a Kafka bin dir to self-spawn load
# (kafka-producer-perf-test.sh), otherwise runs consume-only. Select the
# backend with CLIENT_VERSION (3 = Rust binding, default; 2 = librdkafka
# baseline). Set ASYNC=True to drive the asyncio-native consumer of the
# selected backend (AsyncKafkaConsumer / confluent_kafka.aio.AIOConsumer)
# instead of the sync one.
consumer-perf-test: build
	python test/performance/consumer_performance_test.py

# Env-driven producer performance benchmark (standalone). Same env-var
# contract as the Rust `producer-perf-test`.
producer-perf-test: build
	python test/performance/producer_performance_test.py

# Python gRPC server image for the multilang test harness. Context is the repo
# root; requires libconfluent_kafka.so already built in rust/target/release/.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		-f $(REPO_ROOT)/python/Dockerfile.grpc \
		$(REPO_ROOT)

# Async variant of grpc-image; runs grpc_server_async.py.
grpc-image-async:
	docker build \
		-t $(GRPC_ASYNC_IMAGE_TAG) \
		-f $(REPO_ROOT)/python/Dockerfile.grpc.async \
		$(REPO_ROOT)

clean:
	rm -rf build *.egg-info __pycache__ *.so producer_service_pb2*.py
