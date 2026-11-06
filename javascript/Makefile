# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

REPO_ROOT ?= $(realpath ..)
RUST_PROJECT_ROOT ?= $(REPO_ROOT)/rust
PROFILE ?= release
CONFLUENT_KAFKA_LIB_DIR ?= $(RUST_PROJECT_ROOT)/target/$(PROFILE)
CONFLUENT_KAFKA_INCLUDE_DIR ?= $(RUST_PROJECT_ROOT)/target/include
export CONFLUENT_KAFKA_LIB_DIR CONFLUENT_KAFKA_INCLUDE_DIR

NODE ?= node
NPM ?= npm
# node-gyp: the one on PATH, else the copy bundled with npm (npm puts the same
# one on PATH for `npm run build`), so a build needs no registry access. npm's
# own directory depends on how Node was installed: the official tarball (and
# mise, nvm, Homebrew) keeps it at <node prefix>/lib/node_modules/npm, while Nix
# links bin/npm into a separate package's lib/node_modules/npm/bin.
NPM_NODE_GYP_CANDIDATES = $(NODE_PREFIX)/lib/node_modules/npm/node_modules/node-gyp/bin/node-gyp.js \
	$(dir $(realpath $(shell command -v $(NPM))))../node_modules/node-gyp/bin/node-gyp.js
NPM_NODE_GYP = $(firstword $(wildcard $(NPM_NODE_GYP_CANDIDATES)))
NODE_GYP ?= $(or $(shell command -v node-gyp 2>/dev/null),$(if $(NPM_NODE_GYP),$(NODE) $(NPM_NODE_GYP)),$(error node-gyp not found: put it on PATH or set NODE_GYP; npm's bundled copy is not at $(NPM_NODE_GYP_CANDIDATES)))
# The headers of the running Node: an install that ships them (official
# tarballs, Nix, Homebrew) has include/node/node_api.h under its prefix, and
# node-gyp then needs no header download.
NODE_PREFIX ?= $(realpath $(dir $(realpath $(shell command -v $(NODE))))..)
NODEDIR_FLAG = $(if $(wildcard $(NODE_PREFIX)/include/node/node_api.h),--nodedir=$(NODE_PREFIX),)

# TypeScript (a devDependency): `make types` generates the committed .d.ts
# files from lib/'s JSDoc, and test-typing checks them. TSC is the command that
# runs tsc, TS_TYPE_ROOTS the directory holding @types/node.
TSC ?= node_modules/.bin/tsc
TS_TYPE_ROOTS ?= node_modules/@types
export TSC TS_TYPE_ROOTS

# node --test takes files: the unit test directories up to four levels deep
# (test/unit/common/security/auth/), as lib/'s folders are.
UNIT_TESTS = $(wildcard test/unit/*.test.js test/unit/*/*.test.js test/unit/*/*/*.test.js test/unit/*/*/*/*.test.js)
STATIC_TESTS = test/static/*.test.js
TYPING_TESTS = test/typing/*.test.js
INTEGRATION_TESTS = test/integration/*.test.js

# The integration test's scratch broker (test-integration starts one unless
# KAFKA_BOOTSTRAP_SERVERS is already set).
KAFKA_IMAGE ?= apache/kafka:4.3.1
KAFKA_CONTAINER ?= kafka-js-test
KAFKA_PORT ?= 39092

# Every target expects the Rust library already built under
# $(CONFLUENT_KAFKA_LIB_DIR) and its header under $(CONFLUENT_KAFKA_INCLUDE_DIR):
# the repository root's Makefile orders that (`build-javascript` depends on the
# Rust build); run standalone, build it first.
# The gRPC server of the Rust multilanguage harness's `__grpc_js` producer arm
# (grpc_server.js; see rust/tests/common/backend_pool.rs). Its two packages are
# pinned once, in package.json's devDependencies; GRPC_NPM_PACKAGES lists them
# as name@version for the native install (Dockerfile.grpc reads the same pins).
GRPC_IMAGE_TAG ?= confluent-kafka-rust/javascript-grpc-server:dev
GRPC_NATIVE_DIR ?= $(RUST_PROJECT_ROOT)/target/grpc-native/javascript
GRPC_NPM_PACKAGES = $(shell $(NODE) -p "Object.entries(require('./package.json').devDependencies) \
	.filter(([n]) => n.startsWith('@grpc/')).map(([n, v]) => n + '@' + v).join(' ')")

.PHONY: build types test test-static test-unit test-typing test-integration \
	grpc-image grpc-native test-integration-grpc clean

build:
	$(NODE_GYP) rebuild $(NODEDIR_FLAG)

types:
	$(NODE) scripts/gen_types.js --write

# Then the container gRPC arm of the Rust multilanguage suite, as python/'s and
# c/'s `test` end with theirs: a recipe line, so it runs after the broker-backed
# test and the two never hold a broker at once. It self-skips off Linux (see
# test-integration-grpc); the root `verify-javascript-macos-docker` runs the
# native arm there instead.
test: test-static test-unit test-typing test-integration
	$(MAKE) test-integration-grpc

# Header conformance and the package layout: no build needed.
test-static:
	$(NODE) --test $(STATIC_TESTS)

test-unit: build
	$(NODE) --test $(UNIT_TESTS)

# The committed .d.ts are up to date and type-check under --strict; skipped,
# with a visible message, when $(TSC) cannot be run.
test-typing:
	$(NODE) --test $(TYPING_TESTS)

# Against KAFKA_BOOTSTRAP_SERVERS when set; otherwise against a scratch broker
# this target starts with `docker run` and removes afterwards.
test-integration: build
	@if [ -n "$$KAFKA_BOOTSTRAP_SERVERS" ]; then \
	  $(NODE) --test $(INTEGRATION_TESTS); \
	else \
	  docker rm -f $(KAFKA_CONTAINER) >/dev/null 2>&1 || true; \
	  docker run -d --rm --name $(KAFKA_CONTAINER) -p $(KAFKA_PORT):9092 \
	    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
	    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
	    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:$(KAFKA_PORT) \
	    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
	    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
	    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
	    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
	    $(KAFKA_IMAGE) >/dev/null || exit 1; \
	  status=0; \
	  KAFKA_BOOTSTRAP_SERVERS=localhost:$(KAFKA_PORT) \
	    $(NODE) --test $(INTEGRATION_TESTS) || status=$$?; \
	  docker rm -f $(KAFKA_CONTAINER) >/dev/null 2>&1 || true; \
	  exit $$status; \
	fi

# The javascript gRPC server image. Context is the repo root; requires the
# Linux libconfluent_kafka.so and confluent_kafka_bindings.h already built under
# rust/target/ (the image builds the addon against them), so it builds only on a
# Linux host.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		-f $(REPO_ROOT)/javascript/Dockerfile.grpc \
		$(REPO_ROOT)

# The native (host-process) arm's gRPC packages, installed into
# $(GRPC_NATIVE_DIR)/node_modules, which the harness puts on NODE_PATH (the
# addon itself is `build`, which the root `build-grpc-native-javascript` runs
# first). With MULTILANG_JS_NODE_PATH set to a node_modules that already holds
# them, nothing is installed and the harness uses that one instead.
grpc-native:
	@if [ -n "$$MULTILANG_JS_NODE_PATH" ]; then \
	  for pkg in @grpc/grpc-js @grpc/proto-loader; do \
	    [ -d "$$MULTILANG_JS_NODE_PATH/$$pkg" ] || { \
	      echo "MULTILANG_JS_NODE_PATH=$$MULTILANG_JS_NODE_PATH has no $$pkg" >&2; exit 1; }; \
	  done; \
	  echo "grpc-native: using MULTILANG_JS_NODE_PATH=$$MULTILANG_JS_NODE_PATH"; \
	else \
	  mkdir -p $(GRPC_NATIVE_DIR) && \
	  echo '{"private": true}' > $(GRPC_NATIVE_DIR)/package.json && \
	  $(NPM) install --prefix $(GRPC_NATIVE_DIR) --no-save --no-package-lock --ignore-scripts \
	    --no-audit --no-fund $(GRPC_NPM_PACKAGES); \
	fi

# The container arm of the multilanguage suite for this binding: builds the
# image, then runs the `__grpc_js` tests of the Rust harness. Linux only, like
# python/'s and c/'s `test-integration`.
test-integration-grpc:
	@if [ "$$(uname -s)" != "Linux" ]; then \
		printf '\n========================================================================\n'; \
		printf 'SKIP JavaScript test-integration-grpc: host is %s, not Linux.\n' "$$(uname -s)"; \
		printf '\n'; \
		printf 'The JavaScript gRPC-server image COPYs the host-built\n'; \
		printf '  rust/target/release/libconfluent_kafka.so\n'; \
		printf 'into a Linux container and links the addon against it. On this host\n'; \
		printf 'that artifact is Mach-O / absent (no ELF .so), so the image cannot build.\n'; \
		printf 'Run it natively on this host with: %smake test-integration-javascript-native\n' \
			"$${INTEGRATION_TEST_PROTOCOL:+INTEGRATION_TEST_PROTOCOL=$$INTEGRATION_TEST_PROTOCOL }"; \
		printf '========================================================================\n\n'; \
	else \
		$(MAKE) grpc-image && \
		$(MAKE) -C $(RUST_PROJECT_ROOT) test-integration-grpc-js; \
	fi

clean:
	rm -rf build
