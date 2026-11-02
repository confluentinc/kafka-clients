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
# one on PATH for `npm run build`), so a build needs no registry access.
NODE_GYP ?= $(shell command -v node-gyp 2>/dev/null || \
	echo "$(NODE) $(dir $(realpath $(shell command -v $(NPM))))../node_modules/node-gyp/bin/node-gyp.js")
# The headers of the running Node: an install that ships them (official
# tarballs, Nix, Homebrew) has include/node/node_api.h under its prefix, and
# node-gyp then needs no header download.
NODE_PREFIX ?= $(realpath $(dir $(realpath $(shell command -v $(NODE))))..)
NODEDIR_FLAG = $(if $(wildcard $(NODE_PREFIX)/include/node/node_api.h),--nodedir=$(NODE_PREFIX),)

# The integration test's scratch broker (test-integration starts one unless
# KAFKA_BOOTSTRAP_SERVERS is already set).
KAFKA_IMAGE ?= apache/kafka:4.3.1
KAFKA_CONTAINER ?= kafka-js-test
KAFKA_PORT ?= 39092

# Every target expects the Rust library already built under
# $(CONFLUENT_KAFKA_LIB_DIR) and its header under $(CONFLUENT_KAFKA_INCLUDE_DIR):
# the repository root's Makefile orders that (`build-javascript` depends on the
# Rust build); run standalone, build it first.
.PHONY: build test test-unit test-integration clean

build:
	$(NODE_GYP) rebuild $(NODEDIR_FLAG)

test: test-unit test-integration

test-unit: build
	$(NODE) --test test/unit/*.test.js

# Against KAFKA_BOOTSTRAP_SERVERS when set; otherwise against a scratch broker
# this target starts with `docker run` and removes afterwards.
test-integration: build
	@if [ -n "$$KAFKA_BOOTSTRAP_SERVERS" ]; then \
	  $(NODE) --test test/integration/*.test.js; \
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
	    $(NODE) --test test/integration/*.test.js || status=$$?; \
	  docker rm -f $(KAFKA_CONTAINER) >/dev/null 2>&1 || true; \
	  exit $$status; \
	fi

clean:
	rm -rf build
