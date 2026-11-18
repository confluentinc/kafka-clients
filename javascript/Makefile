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
# How the addon links libconfluent_kafka (native/binding.gyp): dynamic, the default,
# for development, the tests and the gRPC image; static for a release, whose
# addon carries the library (.semaphore/build-javascript-native.sh), e.g.
# `make -C javascript build CONFLUENT_KAFKA_LINK=static`.
CONFLUENT_KAFKA_LINK ?= dynamic
export CONFLUENT_KAFKA_LIB_DIR CONFLUENT_KAFKA_INCLUDE_DIR CONFLUENT_KAFKA_LINK

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

# TypeScript (a devDependency, with @types/node: `npm ci` installs them). The
# library is written in TypeScript, src/**/*.ts, and `build` compiles it into
# dist/ (tsconfig.json), the package's entry points; test-typing type-checks
# test/typing/usage.ts against dist/'s declarations. TSC is the command that
# runs tsc, TS_TYPE_ROOTS the directory holding @types/node for the typing
# test's consumer projects.
TSC ?= node_modules/.bin/tsc
TS_TYPE_ROOTS ?= node_modules/@types
export TSC TS_TYPE_ROOTS

# The tests run on Jest (jest.config.cjs, one project per target below, run
# by jest-light-runner in plain Node.js child processes). JEST_ARGS goes to
# Jest before the project, so a test file (`JEST_ARGS=test/unit/x.test.js`)
# runs only that file and `-t <name>` only the tests it names. The integration tests run in band, one file
# at a time, so two files never share the broker at once.
JEST ?= node_modules/.bin/jest
JEST_ARGS ?=
JEST_RUN = $(NODE) $(JEST) --ci $(JEST_ARGS) --selectProjects

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
# (grpc-server/grpc_server.js; see rust/tests/common/backend_pool.rs). Its two packages are
# pinned once, in package.json's devDependencies: `npm ci` installs them in
# node_modules/, where the server finds them natively, and Dockerfile.grpc
# reads the same pins for the image.
GRPC_IMAGE_TAG ?= confluent-kafka-rust/javascript-grpc-server:dev

# The npm packages of this machine's platform (`pack`), and their install test
# (`pack-check`): the addon linked statically, the main package and this
# platform's package packed into $(PACK_DIR), checked by check-npm.py, then
# installed into a fresh project, where the smoke program runs
# (.semaphore/pack-npm.sh --dev, .semaphore/test-npm.sh). Packages to test the
# packaging, never ones to release: the release packages are built by
# semaphore.yml's "Release (JavaScript):" blocks.
PACK_DIR ?= $(CURDIR)/build/pack
HOST_PLATFORM = $(shell $(NODE) -p "process.platform + '-' + process.arch + (process.platform === 'linux' ? '-gnu' : '')")

.PHONY: build build-addon build-ts test test-static test-unit test-typing test-integration \
	test-integration-ssl test-integration-sasl security-certs test-memory test-memory-mutation test-asan \
	grpc-image grpc-native test-integration-grpc pack pack-check clean lint-rules test-lint \
	test-verifiable-clients test-grpc-server test-stress soak check-api update-api docs format format-check \
	typecheck-tests

# The addon and the compiled library: the package cannot run without either.
build: build-addon build-ts

# The addon: node-gyp runs in native/, beside binding.gyp, and builds
# native/build/Release/confluent-kafka-javascript.node. The package root has no
# binding.gyp, so npm never compiles on install.
build-addon:
	cd native && $(NODE_GYP) rebuild $(NODEDIR_FLAG)

# src/**/*.ts -> dist/ (.js, .d.ts and their maps), from a clean dist/ so a
# removed source leaves no stale output. The private modules (src/internal/)
# are never declared: their .d.ts are removed, so nothing can type against them
# (test/typing checks no public declaration refers to one). Then each .js and
# .d.ts gets its source's licence header first (scripts/dist_headers.js), as
# the package check asks of every tarball.
build-ts:
	@$(TSC) --version >/dev/null 2>&1 || { \
	  echo "javascript: cannot run tsc ($(TSC)): run \`npm ci\` in javascript/ to install the devDependencies (typescript, @types/node), or set TSC" >&2; \
	  exit 1; }
	rm -rf dist
	$(TSC) -p tsconfig.json
	find dist/internal \( -name '*.d.ts' -o -name '*.d.ts.map' \) -delete
	$(NODE) scripts/dist_headers.js src dist

# Then the short stress (test-stress) and the container gRPC arm of the Rust
# multilanguage suite, as python/'s and c/'s `test` end with theirs: recipe
# lines, so they run after the broker-backed test and no two of them hold a
# broker at once. The gRPC arm self-skips off Linux (see
# test-integration-grpc); the root `verify-javascript-macos-docker` runs the
# native arm there instead.
test: test-static test-unit test-memory test-typing check-api typecheck-tests test-verifiable-clients test-grpc-server \
	test-integration
	$(MAKE) test-stress
	$(MAKE) test-integration-grpc

# Header conformance and the package layout: no build needed.
test-static:
	$(JEST_RUN) static

# The rule linter (design/current/javascript-rule-linter/PLAN.md): the rules of
# CLAUDE.md checked from the sources, as `cargo xtask lint-custom` checks the
# Rust crate's, in two halves: ESLint (eslint.config.cjs) runs the rules that
# read TypeScript (`kafka/...`, with eslint-baseline.txt) and
# typescript-eslint's recommended type-checked rules on src/; the runner
# (scripts/lint_rules.js, with lint-baseline.txt) runs the addon's C rules and
# the other files. Both run; the target fails if either fails. What the rules
# read from Java is the committed java-index.json, which `cargo xtask java-api`
# writes from the kafka submodule (and `cargo xtask check-generated` checks):
# neither target runs cargo or reads the submodule. ESLINT_ARGS goes to ESLint;
# LINT_RULES_ARGS (a rule name, check-..., and --no-baseline) to the runner.
# ESLint's --cache is not supported: the kafka/ findings depend on other files
# than the one cached (the baselines, the Java index, the C sources). The
# formatter (scripts/lint/eslint/formatter.js) prints a finding about the
# project, which the kafka/ rules report on every file, once, with its file
# count.
ESLINT ?= node_modules/.bin/eslint
ESLINT_ARGS ?=
LINT_RULES_ARGS ?=

lint-rules:
	@status=0; \
	$(NODE) $(ESLINT) . --format ./scripts/lint/eslint/formatter.js $(ESLINT_ARGS) || status=1; \
	$(NODE) scripts/lint_rules.js $(LINT_RULES_ARGS) || status=1; \
	exit $$status

# The formatter (scripts/format.js, prettier.config.cjs; root conventions, Build,
# packaging and release, Q74): Prettier over the client's TypeScript and
# JavaScript, generated files left out. `format-check` fails on a file it
# would change, and the root `lint-javascript` runs it; `format` rewrites them.
format:
	$(NODE) scripts/format.js write

format-check:
	$(NODE) scripts/format.js check

# The rule linter's own tests: no build of the client.
test-lint:
	$(JEST_RUN) lint

test-unit: build
	$(JEST_RUN) unit

# The native layer's memory probes (test/memory/, Jest project memory): each
# runs in a child process with --expose-gc and the allocator's poisoning
# (test/memory/_probe.js), and checks the addon's test-only counts of every
# native handle (producerStats(), errorStats(), headerStats()), the records a
# send built (recordSpy()), the lifetimes of a process's producers, and the
# send, future and headers workloads under the GC stress flags. No broker.
test-memory: build
	$(JEST_RUN) memory

test-verifiable-clients: build
	$(JEST_RUN) verifiable-clients

# The gRPC conformance server's handlers (grpc-server/), called in-process
# (test/grpc_server/), as the .NET server's unit tests call its own: against
# the FFI's MockProducer and an address where no broker listens.
test-grpc-server: build
	$(JEST_RUN) grpc-server

# The compiled declarations (dist/) type-check under --strict in a consumer's
# configurations; needs only the compiled library, not the addon.
test-typing: build-ts
	$(JEST_RUN) typing

# The type check of the tests (root conventions, Tests, Q67, and Build,
# packaging and release, Q74; scripts/typecheck_tests.js): `tsc --strict` over
# every test file, under test/ and verifiable_clients/test/
# (test/tsconfig.json), the JavaScript tests through checkJs, against the
# compiled declarations in dist/ as a user's code types against them. A
# deliberate misuse is marked `// @ts-expect-error <why>`.
# `test` and the root `test-javascript-unit` run it, beside the other targets
# that read dist/ after the one build-ts.
typecheck-tests: build-ts
	$(NODE) scripts/typecheck_tests.js

# The interface check (root conventions, Stability; scripts/api_report.js):
# api-extractor's report of each import path's compiled declarations and the
# exports map of package.json, against the committed baseline in api/. A change
# warns in a pre-release or a major release and fails in any other release;
# `update-api` records a meant change (and, at a release, the new baseline).
check-api: build-ts
	$(NODE) scripts/api_report.js check

update-api: build-ts
	$(NODE) scripts/api_report.js update

# The HTML reference (typedoc.config.cjs; root conventions, Documentation,
# Q64): TypeDoc over src/, one module per import path, into build/docs/. Not
# part of `verify`; test/static/docs.test.js checks that it builds.
TYPEDOC ?= node_modules/.bin/typedoc
docs:
	$(NODE) $(TYPEDOC) --options typedoc.config.cjs

# Against KAFKA_BOOTSTRAP_SERVERS when set; otherwise against a scratch broker
# this target starts with `docker run` and removes afterwards, whose name it
# passes as KAFKA_CONTAINER: the tests that pause or restart the broker
# (test/integration/_fetch_records.js, brokerContainer()) run only then.
test-integration: build
	@if [ -n "$$KAFKA_BOOTSTRAP_SERVERS" ]; then \
	  $(JEST_RUN) integration --runInBand; \
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
	  KAFKA_BOOTSTRAP_SERVERS=localhost:$(KAFKA_PORT) KAFKA_CONTAINER=$(KAFKA_CONTAINER) \
	    $(JEST_RUN) integration --runInBand || status=$$?; \
	  docker rm -f $(KAFKA_CONTAINER) >/dev/null 2>&1 || true; \
	  exit $$status; \
	fi

# The memory probes have teeth (test/memory_mutation/, Jest project
# memory-mutation): each mutant of the addon, built with node-gyp in a scratch
# copy under MEMORY_MUTATION_DIR (default: a fresh directory under TMPDIR),
# must make the probes that guard what it breaks fail, and the unmutated copy
# pass them. Manual or nightly (.semaphore/memory-javascript.yml). The
# hand-over probe needs a broker: KAFKA_BOOTSTRAP_SERVERS when set, otherwise
# a scratch broker as test-integration starts one.
MEMORY_MUTATION_RUN = NODE_GYP='$(NODE_GYP)' NODEDIR_FLAG='$(NODEDIR_FLAG)' $(JEST_RUN) memory-mutation --runInBand
test-memory-mutation: build
	@if [ -n "$$KAFKA_BOOTSTRAP_SERVERS" ]; then \
	  $(MEMORY_MUTATION_RUN); \
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
	  KAFKA_BOOTSTRAP_SERVERS=localhost:$(KAFKA_PORT) KAFKA_CONTAINER=$(KAFKA_CONTAINER) \
	    $(MEMORY_MUTATION_RUN) || status=$$?; \
	  docker rm -f $(KAFKA_CONTAINER) >/dev/null 2>&1 || true; \
	  exit $$status; \
	fi

# AddressSanitizer: a copy of the package under ASAN_DIR (everything but
# node_modules, which it links, build/ and native/build/), its addon built with
# ASAN_CFLAGS through node-gyp's CFLAGS, CXXFLAGS and LDFLAGS (native/
# binding.gyp unchanged), and the unit and memory projects run in the copy,
# so the tests load its addon. Node itself is not instrumented: gcc's libasan
# is preloaded into every process, leak detection is off (Node and V8 leave
# memory to the process at exit), and the first error aborts the process that
# hit it, which fails its test. Linux with gcc; nightly
# (.semaphore/memory-javascript.yml).
ASAN_DIR ?= $(CURDIR)/build/asan
ASAN_CFLAGS = -fsanitize=address -fno-omit-frame-pointer -g -O1
test-asan: build
	@[ "$$(uname -s)" = Linux ] || { echo "test-asan: Linux only (gcc's libasan, preloaded with LD_PRELOAD)" >&2; exit 1; }
	rm -rf $(ASAN_DIR)
	mkdir -p $(ASAN_DIR)
	rsync -a --exclude /node_modules --exclude /build --exclude /native/build --exclude /bench --exclude /.claude ./ $(ASAN_DIR)/
	ln -s $(CURDIR)/node_modules $(ASAN_DIR)/node_modules
	cd $(ASAN_DIR)/native && CC=gcc CXX=g++ CFLAGS='$(ASAN_CFLAGS)' CXXFLAGS='$(ASAN_CFLAGS)' LDFLAGS='-fsanitize=address' \
	  $(NODE_GYP) rebuild $(NODEDIR_FLAG)
	cd $(ASAN_DIR) && LD_PRELOAD="$$(gcc -print-file-name=libasan.so)" ASAN_OPTIONS=detect_leaks=0:abort_on_error=1 \
	  $(NODE) $(JEST) --ci $(JEST_ARGS) --selectProjects unit memory

# A short stress of the producer (test/stress/; root conventions, Tests, Q68):
# a million records sent in batches whose sends are awaited together, under
# buffer pressure, and producers made, used and closed one after another, at
# most a few minutes. Against KAFKA_BOOTSTRAP_SERVERS when set, otherwise
# against a scratch broker started and removed as for test-integration;
# `test` runs it after the broker-backed tests, so the two never hold a
# broker at once. STRESS_RECORDS and STRESS_PRODUCERS scale it down.
test-stress: build
	@if [ -n "$$KAFKA_BOOTSTRAP_SERVERS" ]; then \
	  $(JEST_RUN) stress --runInBand; \
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
	  KAFKA_BOOTSTRAP_SERVERS=localhost:$(KAFKA_PORT) KAFKA_CONTAINER=$(KAFKA_CONTAINER) \
	    $(JEST_RUN) stress --runInBand || status=$$?; \
	  docker rm -f $(KAFKA_CONTAINER) >/dev/null 2>&1 || true; \
	  exit $$status; \
	fi

# The producer soak (verifiable_clients/soak/soak_check.js; root conventions,
# Tests, Q68): soakclient.js for SOAK_HOURS hours, judged on its steady state
# after the warm-up: the RSS, the heap and the external memory may not keep
# growing, the open file descriptors and threads stay flat, and every delivery
# callback runs once. Against KAFKA_BOOTSTRAP_SERVERS when set, otherwise
# against a scratch broker started and removed as for test-integration. Its
# report and the client's metrics go to SOAK_OUT. By hand, or weekly
# (.semaphore/soak-javascript.yml); never part of `test`. SOAK_ARGS goes to
# soak_check.js (--warmup-minutes, --max-slope-mib-per-hour, ...),
# SOAK_CLIENT_ARGS to soakclient.js.
SOAK_HOURS ?= 6
SOAK_OUT ?= $(CURDIR)/build/soak
SOAK_TOPIC ?= js-soak
SOAK_ARGS ?=
SOAK_CLIENT_ARGS ?= -r 1000
SOAK_RUN = $(NODE) verifiable_clients/soak/soak_check.js --hours $(SOAK_HOURS) --out $(SOAK_OUT) $(SOAK_ARGS) -- \
	-i soak -t $(SOAK_TOPIC) $(SOAK_CLIENT_ARGS) -b
soak: build
	@if [ -n "$$KAFKA_BOOTSTRAP_SERVERS" ]; then \
	  $(SOAK_RUN) "$$KAFKA_BOOTSTRAP_SERVERS"; \
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
	  $(SOAK_RUN) localhost:$(KAFKA_PORT) || status=$$?; \
	  docker rm -f $(KAFKA_CONTAINER) >/dev/null 2>&1 || true; \
	  exit $$status; \
	fi

# The integration tests over SSL, mTLS and SASL (test/integration/ssl.test.js,
# sasl.test.js): each target starts its own broker with `docker run`, runs the
# integration project against it and removes it. Without their variables those
# files skip, so `test-integration` runs none of them. security-certs writes,
# with openssl, a CA, a second CA nothing trusts, the broker's certificate
# (valid for localhost only, not for 127.0.0.1), a client certificate for mTLS
# and the broker's JAAS file (SASL/PLAIN users admin, a super user, and alice)
# into SECURITY_DIR, which the broker mounts as /etc/kafka/secrets. Each broker
# also has a PLAINTEXT listener, which the tests' protocol helper
# (_fetch_records.js) and kafka-acls.sh use. The listener names avoid "SSL://",
# which would turn on the image's own SSL setup (/etc/kafka/docker/configure).
SECURITY_DIR ?= $(CURDIR)/build/security
SECURITY_CLUSTER_ID ?= 5L6g3nShT-eMCtK--X86sw
KAFKA_SSL_CONTAINER ?= js-tests-ssl
KAFKA_SSL_PORT ?= 39192
KAFKA_SASL_CONTAINER ?= js-tests-sasl
KAFKA_SASL_PORT ?= 39292
# The broker settings both security brokers share: one KRaft node, its
# certificates in PEM, at most 2.5 GB.
SECURITY_BROKER = --memory 2500m --memory-swap 2500m -v $(SECURITY_DIR):/etc/kafka/secrets:ro \
	-e CLUSTER_ID=$(SECURITY_CLUSTER_ID) -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
	-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9099 \
	-e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
	-e KAFKA_HEAP_OPTS='-Xmx512m -Xms256m' \
	-e KAFKA_SSL_KEYSTORE_TYPE=PEM -e KAFKA_SSL_KEYSTORE_LOCATION=/etc/kafka/secrets/broker-keystore.pem \
	-e KAFKA_SSL_TRUSTSTORE_TYPE=PEM -e KAFKA_SSL_TRUSTSTORE_LOCATION=/etc/kafka/secrets/ca.pem

security-certs:
	@set -e; d='$(SECURITY_DIR)'; rm -rf "$$d"; mkdir -p "$$d"; cd "$$d"; \
	printf 'basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n' > ca.ext; \
	for ca in ca untrusted-ca; do \
	  openssl req -new -newkey rsa:2048 -nodes -subj "/CN=js-tests-$$ca" -keyout $$ca.key -out $$ca.csr 2>/dev/null; \
	  openssl x509 -req -in $$ca.csr -signkey $$ca.key -days 30 -extfile ca.ext -out $$ca.pem 2>/dev/null; \
	done; \
	printf 'basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost\n' > broker.ext; \
	printf 'basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth\n' > client.ext; \
	serial=2; \
	for leaf in broker client; do \
	  openssl req -new -newkey rsa:2048 -nodes -subj "/CN=js-tests-$$leaf" -keyout $$leaf.key -out $$leaf.csr 2>/dev/null; \
	  openssl x509 -req -in $$leaf.csr -CA ca.pem -CAkey ca.key -set_serial $$serial -days 30 -extfile $$leaf.ext -out $$leaf.pem 2>/dev/null; \
	  cat $$leaf.key $$leaf.pem > $$leaf-keystore.pem; \
	  serial=$$((serial + 1)); \
	done; \
	printf '%s\n' 'KafkaServer {' '  org.apache.kafka.common.security.plain.PlainLoginModule required' \
	  '  username="admin" password="admin-secret"' '  user_admin="admin-secret"' '  user_alice="alice-secret";' '};' \
	  > kafka_server_jaas.conf; \
	rm -f ./*.csr ./*.ext; chmod a+r ./*

# SSL on KAFKA_SSL_PORT + 1 (server authentication only) and mTLS on
# KAFKA_SSL_PORT + 2 (ssl.client.auth required), PLAINTEXT on KAFKA_SSL_PORT.
test-integration-ssl: build security-certs
	@p=$(KAFKA_SSL_PORT); tls=$$((p + 1)); mtls=$$((p + 2)); \
	docker rm -f $(KAFKA_SSL_CONTAINER) >/dev/null 2>&1 || true; \
	docker run -d --rm --name $(KAFKA_SSL_CONTAINER) -p $$p:9092 -p $$tls:9093 -p $$mtls:9094 $(SECURITY_BROKER) \
	  -e KAFKA_LISTENERS=PLAINTEXT://:9092,TLSONLY://:9093,MTLS://:9094,CONTROLLER://:9099 \
	  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:$$p,TLSONLY://localhost:$$tls,MTLS://localhost:$$mtls \
	  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,TLSONLY:SSL,MTLS:SSL \
	  -e KAFKA_LISTENER_NAME_MTLS_SSL_CLIENT_AUTH=required \
	  $(KAFKA_IMAGE) >/dev/null || exit 1; \
	status=0; \
	KAFKA_SECURITY_DIR='$(SECURITY_DIR)' KAFKA_SSL_PLAINTEXT_BOOTSTRAP_SERVERS=localhost:$$p \
	  KAFKA_SSL_BOOTSTRAP_SERVERS=localhost:$$tls KAFKA_MTLS_BOOTSTRAP_SERVERS=localhost:$$mtls \
	  $(NODE) $(JEST) --ci $(or $(JEST_ARGS),test/integration/ssl.test.js) --selectProjects integration --runInBand || status=$$?; \
	docker rm -f $(KAFKA_SSL_CONTAINER) >/dev/null 2>&1 || true; \
	exit $$status

# SASL_PLAINTEXT on KAFKA_SASL_PORT + 1 and SASL_SSL on KAFKA_SASL_PORT + 2,
# both SASL/PLAIN, PLAINTEXT on KAFKA_SASL_PORT, under the StandardAuthorizer:
# admin, and ANONYMOUS (the PLAINTEXT listener), are super users; alice has
# only the ACLs a test gives her. The PLAINTEXT listener listens on the same
# port inside the container as outside, so kafka-acls.sh, run in the
# container, reaches the address the broker advertises.
test-integration-sasl: build security-certs
	@p=$(KAFKA_SASL_PORT); plain=$$((p + 1)); tls=$$((p + 2)); \
	docker rm -f $(KAFKA_SASL_CONTAINER) >/dev/null 2>&1 || true; \
	docker run -d --rm --name $(KAFKA_SASL_CONTAINER) -p $$p:$$p -p $$plain:9093 -p $$tls:9094 $(SECURITY_BROKER) \
	  -e KAFKA_LISTENERS=PLAINTEXT://:$$p,SASLPLAIN://:9093,SASLTLS://:9094,CONTROLLER://:9099 \
	  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:$$p,SASLPLAIN://localhost:$$plain,SASLTLS://localhost:$$tls \
	  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASLPLAIN:SASL_PLAINTEXT,SASLTLS:SASL_SSL \
	  -e KAFKA_SASL_ENABLED_MECHANISMS=PLAIN \
	  -e KAFKA_OPTS=-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_server_jaas.conf \
	  -e KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer \
	  -e 'KAFKA_SUPER_USERS=User:admin;User:ANONYMOUS' \
	  $(KAFKA_IMAGE) >/dev/null || exit 1; \
	status=0; \
	KAFKA_SECURITY_DIR='$(SECURITY_DIR)' KAFKA_SASL_ADMIN_BOOTSTRAP_SERVERS=localhost:$$p \
	  KAFKA_SASL_PLAINTEXT_BOOTSTRAP_SERVERS=localhost:$$plain KAFKA_SASL_SSL_BOOTSTRAP_SERVERS=localhost:$$tls \
	  KAFKA_SASL_CONTAINER=$(KAFKA_SASL_CONTAINER) \
	  $(NODE) $(JEST) --ci $(or $(JEST_ARGS),test/integration/sasl.test.js) --selectProjects integration --runInBand || status=$$?; \
	docker rm -f $(KAFKA_SASL_CONTAINER) >/dev/null 2>&1 || true; \
	exit $$status

# The javascript gRPC server image. Context is the repo root; requires the
# Linux libconfluent_kafka.so and confluent_kafka_bindings.h already built under
# rust/target/ (the image builds the addon against them), so it builds only on a
# Linux host.
grpc-image:
	docker build \
		-t $(GRPC_IMAGE_TAG) \
		-f $(REPO_ROOT)/javascript/Dockerfile.grpc \
		$(REPO_ROOT)

# The native (host-process) arm loads its gRPC packages from node_modules/,
# which grpc-server/grpc_server.js resolves, where `npm ci` installs them with the other
# devDependencies; this only checks they are there (the addon itself is
# `build`, which the root `build-grpc-native-javascript` runs first).
grpc-native:
	@for pkg in @grpc/grpc-js @grpc/proto-loader; do \
	  [ -d node_modules/$$pkg ] || { \
	    echo "grpc-native: javascript/node_modules/$$pkg is missing: run \`npm ci\` in javascript/" >&2; exit 1; }; \
	done

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

# build-addon first: the pack carries its static addon, native/build/Release/.
pack:
	$(MAKE) build-addon CONFLUENT_KAFKA_LINK=static
	rm -rf $(PACK_DIR)
	cd $(REPO_ROOT) && sh .semaphore/pack-npm.sh --dev $(PACK_DIR)

# publint (root conventions, Build, packaging and release, Q74) lints each
# tarball first: an error, a warning or a suggestion fails it.
PUBLINT ?= node_modules/.bin/publint
pack-check: pack
	@for tarball in $(PACK_DIR)/*.tgz; do \
	  out=$$($(NODE) $(PUBLINT) run --strict "$$tarball" 2>&1); status=$$?; \
	  printf '%s\n' "$$out"; \
	  [ $$status -eq 0 ] || exit 1; \
	  case "$$out" in *Suggestions:*|*Warnings:*|*Errors:*) echo "publint: $$tarball has findings" >&2; exit 1;; esac; \
	done
	cd $(REPO_ROOT) && sh .semaphore/test-npm.sh $(PACK_DIR) $(HOST_PLATFORM)

clean:
	rm -rf build dist native/build
