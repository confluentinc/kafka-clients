REPO_ROOT ?= $(realpath ..)
RUST_PROJECT_ROOT ?= $(REPO_ROOT)/rust
PROFILE ?= release
GRPC_IMAGE_TAG ?= confluent-kafka-rust/python-grpc-server:dev
GRPC_ASYNC_IMAGE_TAG ?= confluent-kafka-rust/python-async-grpc-server:dev
# Native (non-container) gRPC server builds for the multilanguage harness,
# written under the Rust target dir, where rust/tests/common/backend_pool.rs
# looks for them in MULTILANG_BACKEND_MODE=native.
GRPC_NATIVE_DIR = $(RUST_PROJECT_ROOT)/target/grpc-native
GRPC_PROTO_DIR = $(RUST_PROJECT_ROOT)/multilanguage-test-server/proto
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
.PHONY: init build test test-unit typecheck check-static test-soak test-integration test-performance \
	consumer-perf-test producer-perf-test grpc-image grpc-image-async grpc-native clean

# Development dependencies, for a fresh clone (the root `init`).
init:
	pip install .[dev]

build:
	CONFLUENT_KAFKA_LIB_DIR=$(RUST_PROJECT_ROOT)/target/$(PROFILE) \
	CFLAGS="$(CFLAGS_EXTRA)" \
	pip install -e .

# `typecheck` runs after the dev extras are installed: mypy is one of them, and
# a fresh venv (CI) has it only from that step. `test/integration` holds the
# broker-backed binding tests (a testcontainers KRaft broker; they skip without
# Docker): what the Java tests drive through a MockClient the binding cannot
# inject. They are not the gRPC multilanguage arm of `test-integration` below.
test: build
	pip install .[dev] || pip install --no-dependencies .[dev]
	$(MAKE) typecheck
	python -m pytest test/unit -v
	python -m pytest test/integration -v
	$(MAKE) check-static
	$(MAKE) test-soak
	$(MAKE) test-integration

# Unit tests only (pytest test/unit). The gRPC multilanguage arm runs on Linux
# only; this is what the macOS verify job runs.
test-unit: build
	pip install .[dev] || pip install --no-dependencies .[dev]
	python -m pytest test/unit -v

# Static type check of the `confluent_kafka` package (CLAUDE.md, Python Binding
# Conventions, Tests and typing: `mypy --strict` over the package runs in
# `make verify`). It catches stub/implementation drift.
# `test/unit/test_typing.py` carries `typing.assert_type` assertions that
# machine-verify the `@overload` stubs and the generics inference, so it is type
# checked here too — a broken stub fails `make verify`. Like every target here it
# runs in the venv the caller activated, where mypy and the built
# `_confluentkafka` extension live.
typecheck:
	python -m mypy --strict confluent_kafka test/unit/test_typing.py

# Static checks (test/static): the Py_BuildValue / PyArg_Parse* /
# PyObject_CallFunction format-arity scan of _confluentkafka.c. A format one
# unit short of its argument list compiles silently and reads a garbage pointer
# at run time, and for every admin RPC that Java's MockAdminClient leaves
# unsupported the affected drain's success path is unreachable from the test
# suite, so this defect class must be caught statically. They read source files
# only, so they need neither the Rust library nor the C extension built.
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
		printf 'Run it natively on this host with: %smake test-integration-python-native\n' \
			"$${INTEGRATION_TEST_PROTOCOL:+INTEGRATION_TEST_PROTOCOL=$$INTEGRATION_TEST_PROTOCOL }"; \
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

# The librdkafka venv's interpreter (the root's `init-venv-librdkafka`): the PyPI
# confluent-kafka client shares the top-level package `confluent_kafka` with this
# client, so it cannot live in the active venv.
LIBRDKAFKA_PYTHON ?= $(REPO_ROOT)/venv-librdkafka/bin/python

# Env-driven consumer end-to-end latency benchmark (standalone). Mirrors
# rust/consumer-perf/compare/benchmark_e2e_latency.c. Needs a reachable broker
# via BOOTSTRAP_SERVERS; set KAFKA_BIN to a Kafka bin dir to self-spawn load
# (kafka-producer-perf-test.sh), otherwise runs consume-only. Select the
# backend with CLIENT_VERSION (3 = Rust binding, default; 2 = librdkafka
# baseline). Set ASYNC=True to drive the asyncio-native consumer of the
# selected backend (AsyncKafkaConsumer / confluent_kafka.aio.AIOConsumer)
# instead of the sync one. CLIENT_VERSION=2 runs in the librdkafka venv; the
# Rust run uses it only for CREATE_TOPIC.
consumer-perf-test: build
	@(if [ "$${CLIENT_VERSION:-3}" = "2" ]; then py=$(LIBRDKAFKA_PYTHON); else py=python; fi; \
	  LIBRDKAFKA_PYTHON=$(LIBRDKAFKA_PYTHON) $$py test/performance/consumer_performance_test.py)

# Env-driven producer performance benchmark (standalone). Same env-var
# contract as the Rust `producer-perf-test`; CLIENT_VERSION selects the backend
# as for consumer-perf-test. CLIENT_VERSION=2 runs in the librdkafka venv; the
# Rust run uses it only for CREATE_TOPIC / VERIFY_CONSUMED.
producer-perf-test: build
	@(if [ "$${CLIENT_VERSION:-3}" = "2" ]; then py=$(LIBRDKAFKA_PYTHON); else py=python; fi; \
	  LIBRDKAFKA_PYTHON=$(LIBRDKAFKA_PYTHON) $$py test/performance/producer_performance_test.py)

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

# Generated gRPC stubs for the native (host-process) Python arm: installs the
# gRPC packages into the active environment and runs protoc. The versions are
# pinned in requirements-grpc{,-tools}.txt, shared with the Docker images.
grpc-native:
	pip install -r requirements-grpc-tools.txt
	mkdir -p $(GRPC_NATIVE_DIR)/python
	python -m grpc_tools.protoc -I$(GRPC_PROTO_DIR) \
		--python_out=$(GRPC_NATIVE_DIR)/python --grpc_python_out=$(GRPC_NATIVE_DIR)/python \
		$(GRPC_PROTO_DIR)/producer_service.proto $(GRPC_PROTO_DIR)/consumer_service.proto \
		$(GRPC_PROTO_DIR)/admin_service.proto

clean:
	rm -rf build *.egg-info __pycache__ *.so producer_service_pb2*.py
