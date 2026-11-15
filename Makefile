# Cross-language Makefile. Each language owns its targets in its own Makefile
# -- rust/, c/, python/, dotnet/ -- run as `make -C <language> <target>`, and
# each builds what it needs first (the Rust library, its submodule, its venv).
# This one holds only what spans several languages or the whole repository:
# the combined builds and gates, the git submodules and hooks, the version
# bump and the test selection.
#
# `$(MAKE)` is spelled out in every delegating recipe line rather than hidden
# in a variable: GNU make only treats a line as a recursive make -- passing
# down the -j jobserver, -n, -k -- when `$(MAKE)` appears in it literally. The
# languages run as recipe lines, one after the other, so their builds of the
# shared Rust library never overlap.

.PHONY: build build-all devel-build build-grpc-images submodules init init-hooks \
	test test-integration-perf verify verify-commit verify-commit-checks bump-version \
	select-tests pre-push-test-selection clean-test-selection clean \
	build-javascript test-javascript test-javascript-unit lint-javascript verify-javascript \
	test-javascript-macos-docker verify-javascript-macos-docker \
	build-grpc-images-javascript test-integration-javascript \
	test-integration-javascript-ssl test-integration-javascript-sasl-ssl \
	build-grpc-native-javascript test-integration-javascript-native \
	pack-javascript package-check-javascript

build: init-hooks build-all

# The release builds, for packaging and the benchmarks: the Rust library with
# every feature, the C library and the Python binding. The tests use
# devel-build. Like the other cross-language targets it first initialises every
# submodule (kafka/ included, which the code generator and the lints read); the
# language folders fetch only what they need.
build-all: submodules
	$(MAKE) -C rust build-all-features
	$(MAKE) -C c RUST_PROFILE=release build
	$(MAKE) -C python PROFILE=release build

# The debug builds the binding tests use. They link the debug all-features
# library (rust/Makefile `devel-build-all-features`), the library unit the Rust
# tests (`build-tests-all-features`) already build, so after the Rust tests it
# is a no-op.
devel-build: submodules
	$(MAKE) -C c devel-build
	$(MAKE) -C python build

# The per-language gRPC server Docker images of the multilanguage integration
# test harness (see design/history/MILESTONE-6/DESIGN-multilanguage-tests.md);
# each binding's `test-integration` builds its own.
build-grpc-images:
	$(MAKE) -C python grpc-image grpc-image-async
	$(MAKE) -C c grpc-image
	$(MAKE) -C dotnet grpc-image grpc-image-async

submodules:
	git submodule update --init --recursive

# One-shot setup for a fresh clone or worktree: pulls down the git
# submodules (kafka source reference + Unity for the C unit tests), creates the
# Python venv with the development dependencies (python/'s `init`), and
# registers the git hooks (init-hooks). Run this before `make build` on a new
# checkout.
init: submodules init-hooks
	$(MAKE) -C python init

# Registers .githooks: pre-commit runs verify-commit, pre-push
# pre-push-test-selection.
init-hooks:
	@git config core.hooksPath .githooks
	@chmod +x .githooks/pre-commit .githooks/pre-push

# The Rust tests with every feature compiled (the native-Rust ones; the gRPC
# arms run with their binding), then the C and Python tests, and the Node.js
# client's tests that need no broker (test-javascript-unit).
test: submodules
	$(MAKE) -C rust test-all-features
	$(MAKE) -C c test
	$(MAKE) -C python test
	$(MAKE) test-javascript-unit

# The Rust, Python and .NET performance suites, one after the other: they
# assert latency budgets, so each needs the machine to itself. The .NET arm is
# the Docker-gated v3 smoke (it skips cleanly without Docker).
test-integration-perf:
	$(MAKE) -C rust test-integration-perf
	$(MAKE) -C python test-performance
	$(MAKE) -C dotnet test-integration-perf-dotnet

# The whole gate: the release builds, the Rust checks, the tests, the Python
# binding's checks (mypy --strict and the static checks) and the Node.js
# client's rule linter (lint-javascript). It runs no .NET target: `make -C
# dotnet verify` is the .NET gate.
verify: build
	$(MAKE) -C rust format-check check-generated lint
	$(MAKE) test
	$(MAKE) -C python check
	$(MAKE) lint-javascript

# The pre-commit hook's verification. It first selects the tests the changes
# since the last commit can affect into its own .test-selection-commit.json
# (keeping a selection already made there at HEAD for the same working tree,
# see Test selection), then verifies with that selection. If the selection
# fails there is no file, and every test runs.
verify-commit: export TEST_SELECTION = .test-selection-commit.json
verify-commit:
	$(MAKE) select-tests BASE=HEAD SELECT_TESTS_FLAGS=--reuse || \
		echo "verify-commit: the test selection failed, so every test runs"
	$(MAKE) verify-commit-checks

# The C build first, which builds the debug Rust library that check-generated
# reads; then the Rust checks and integration tests, and the C tests.
verify-commit-checks: submodules
	$(MAKE) -C c devel-build
	$(MAKE) -C rust format-check check-generated lint test-integration
	$(MAKE) -C c test

# ── The Node.js client ───────────────────────────────────────────────────
#
# The Node.js client (javascript/). `verify` runs its build, its rule linter,
# its format check, its type check and its unit tests (test-javascript-unit, through `test`, and
# lint-javascript), so it needs Node.js 22.14 or newer and `npm ci` in
# javascript/ first (typescript and eslint are devDependencies); its
# broker-backed and gRPC arms run in CI's own jobs (verify-javascript and
# verify-javascript-macos-docker, in .semaphore/semaphore.yml). Its build is the
# addon and the TypeScript library. javascript/Makefile expects the Rust library
# already built, so these targets build it first.
build-javascript:
	$(MAKE) -C rust build-all-features
	$(MAKE) -C javascript build

# Just the JavaScript gRPC image (producer only). Like the C image it needs only
# the Rust release artifacts: javascript/Dockerfile.grpc builds the addon itself
# against the copied libconfluent_kafka.so. Not part of `build-grpc-images`,
# as the JavaScript client is not yet part of `build` either.
build-grpc-images-javascript:
	$(MAKE) -C rust build-all-features
	$(MAKE) -C javascript grpc-image

# The Node.js client's static, unit and typing tests, its broker-backed test
# against a scratch broker it starts with docker (or KAFKA_BOOTSTRAP_SERVERS
# when set), then the container gRPC multilanguage arm (Linux only; see
# test-integration-javascript), as python/'s and c/'s `test` end with theirs. The
# build needs the devDependencies (`npm ci` in javascript/): typescript compiles
# the library (javascript/src/**/*.ts -> javascript/dist/).
test-javascript: build-javascript
	$(MAKE) -C javascript test

# The Node.js client's tests that need no broker, the part of `test` (and so
# of `verify`): the static tests (header conformance, the package layout, the
# release scripts), the unit tests, and the type check, `tsc --strict` over
# the sources (the build) and over the type tests (test-typing, against the
# compiled declarations), with the interface check of the declarations and the
# exports map against the committed API report (check-api).
test-javascript-unit: build-javascript
	$(MAKE) -C javascript test-static test-unit test-typing check-api

# macOS variant of test-javascript: the unit tests only, as python/'s
# `verify-macos` runs pytest test/unit only. On macOS the gRPC multilanguage arm
# runs natively instead (see verify-javascript-macos-docker).
test-javascript-macos-docker: build-javascript
	$(MAKE) -C javascript test-unit

# The Node.js client's rule linter (ESLint, javascript/eslint.config.cjs, and
# the runner, javascript/scripts/lint_rules.js) and its own tests; ESLint is a
# devDependency, so `npm ci` in javascript/ first, as for the build. It reads
# the bindings header the Rust build writes and the committed Java index
# (javascript/java-index.json, `cargo xtask java-api`), not the kafka submodule.
lint-javascript:
	$(MAKE) -C rust build-all-features
	$(MAKE) -C javascript lint-rules test-lint format-check

verify-javascript: test-javascript lint-javascript

# macOS verify-javascript: the unit tests, then the gRPC multilanguage arm with
# the gRPC server as a native host process, as python/'s `verify-macos` does.
verify-javascript-macos-docker: test-javascript-macos-docker
	$(MAKE) test-integration-javascript-native

# The producer-only `__grpc_js` arm. Self-skips off Linux, like the Python and C
# arms (the image copies the host-built Linux libconfluent_kafka.so); use
# `test-integration-javascript-native` there. Part of `verify-javascript`
# (through javascript/'s `test`), not of `verify`.
test-integration-javascript:
	$(MAKE) -C rust build-all-features
	$(MAKE) -C javascript test-integration-grpc

# The same arm over the SSL / SASL_SSL CONTAINER listeners (CTLSONLY :9100 /
# CSASLTLS :9101), which INTEGRATION_TEST_PROTOCOL selects
# (rust/tests/common/test_context.rs), as python/'s and c/'s
# `test-integration-ssl` and `test-integration-sasl-ssl` do. They inherit the
# non-Linux skip.
test-integration-javascript-ssl:
	INTEGRATION_TEST_PROTOCOL=ssl $(MAKE) test-integration-javascript
test-integration-javascript-sasl-ssl:
	INTEGRATION_TEST_PROTOCOL=sasl_ssl $(MAKE) test-integration-javascript

# The `__grpc_js` arm with the gRPC server as a host process instead of a
# container (MULTILANG_BACKEND_MODE=native; see rust/tests/common/backend_pool.rs):
# only the broker runs in Docker. Used on macOS, where a container runs Linux
# and could only test the Linux build of the binding. The server needs nothing
# built for it: it runs from javascript/, with the gRPC packages `npm ci`
# installs in javascript/node_modules.
build-grpc-native-javascript: build-javascript
	$(MAKE) -C javascript grpc-native

test-integration-javascript-native: build-grpc-native-javascript
	$(MAKE) -C rust test-integration-grpc-js-native

# The npm packages of the Node.js client, with this machine's addon only:
# pack-javascript packs them into javascript/build/pack, and
# package-check-javascript packs them, then installs them into a fresh project
# and runs the smoke program there. See javascript/Makefile; the release
# packages are built by release.yml's "Release (JavaScript):" blocks.
pack-javascript:
	$(MAKE) -C rust build-all-features
	$(MAKE) -C javascript pack

package-check-javascript:
	$(MAKE) -C rust build-all-features
	$(MAKE) -C javascript pack-check

# `make bump-version VERSION=0.3.0-rc.1 [PYTHON_VERSION=...] [DOTNET_VERSION=...]
# [JAVASCRIPT_VERSION=...]` bumps the versions of the Rust crate, the Python
# wheels, the .NET package and the Node.js package: see rust/Makefile. VERSION,
# PYTHON_VERSION, DOTNET_VERSION and JAVASCRIPT_VERSION reach the sub-make
# through its environment (make passes command-line variables down); this
# recipe never pastes them in.
bump-version:
	$(MAKE) -C rust bump-version

# ── Test selection ───────────────────────────────────────────────────────
#
# `make select-tests` gives an agent the changes from BASE (committed,
# uncommitted and untracked) and writes the tests they can affect, per
# language, to test-selection/test-selection-<HEAD>.json (or to
# $TEST_SELECTION, relative to this directory, when it names a .json file),
# together with the working tree it was made for, and deletes every file in
# test-selection/ but the four most recent. The agent is told to include a
# test whenever in doubt. Commit test-selection/ with the branch: the
# pre-push hook (pre-push-test-selection) does it, as a commit of its own,
# when a pushed HEAD has no selection made for it.
#
# A selection applies only where TEST_SELECTION names it, a file or the
# folder, of which the file made for the working tree applies: CI sets it to
# test-selection on a pull request build, `verify-commit` to its own
# .test-selection-commit.json, and it can be set by hand
# (`TEST_SELECTION=test-selection make test`). Then every test recipe
# (`test`, `verify`, the integration and performance targets, in Rust, C,
# Python and .NET) runs only the selected tests, and a suite the file leaves out does
# not run at all, but only while the working tree is the one the file was made
# for: after any other change, every test runs. Without TEST_SELECTION every
# test runs. A failed `select-tests` removes the previous file rather than
# leave it in force.
#
# AGENT is any command taking Claude Code's `--model`, `--restricted`,
# `--strict-mcp-config`, `--tools`, `--allowedTools`, `--disallowedTools` and
# `-p`: by default a local Claude Code, which reads the working tree, so
# uncommitted and untracked changes count. The agent only reads files and runs
# read-only git commands: restricted mode ignores the user's settings files, so
# only the read-only tools are allowed, and an agent that has not answered after
# SELECT_TESTS_FLAGS' `--timeout` (600 s by default) is stopped and the selection
# fails. rust/xtask/src/test_selection.rs describes the file's format.
AGENT ?= claude
AGENT_MODEL ?= opus
BASE ?= origin/master
# `--reuse` keeps a file made at HEAD that covers every current change.
SELECT_TESTS_FLAGS ?=

select-tests:
	cd rust && cargo xtask select-tests --base "$(BASE)" --agent "$(AGENT)" --model "$(AGENT_MODEL)" $(SELECT_TESTS_FLAGS)

# The pre-push hook's task, reading the pushed refs on standard input: when
# HEAD is pushed and its committed test-selection/ holds no selection made for
# it, it selects for HEAD, commits test-selection/ (with --no-verify), pushes
# that commit to PUSH_REMOTE (the remote git gives the hook) at the refs the
# push sends HEAD to, and fails the push, which git cannot extend with a commit
# made by its hook. The commit is pushed with GIT_PUSH_COMMAND, run by sh and
# followed by the remote and the refspecs (`GIT_PUSH_COMMAND=git_push_external`),
# or else `git push`; a ref the push forces gets a --force-with-lease on the
# commit git expected there. A working tree holding changes HEAD does not, or a
# failed selection, lets the push go on without one, and CI then runs every
# test. It builds only xtask's test-selection binary.
PUSH_REMOTE ?=

pre-push-test-selection:
	cd rust && cargo run -q --locked -p xtask --bin test-selection -- pre-push --remote "$(PUSH_REMOTE)" --base "$(BASE)" --agent "$(AGENT)" --model "$(AGENT_MODEL)"

# Removes the selections of test-selection/ and the .json file TEST_SELECTION
# names, if any.
clean-test-selection:
	rm -f test-selection/test-selection-*.json
	case "$${TEST_SELECTION:-}" in *.json) rm -f "$$TEST_SELECTION" ;; esac

clean:
	$(MAKE) -C rust clean
	$(MAKE) -C c clean
	$(MAKE) -C python clean
