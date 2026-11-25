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

# The tag of the five gRPC images of the multilanguage harness
# (confluent-kafka-rust/{python,python-async,c,dotnet,dotnet-async}-grpc-server).
# python/Makefile, c/Makefile and dotnet/Makefile build them under it and
# rust/tests/common/backend_pool.rs runs them under it (the variable reaches
# cargo through the environment). Set it per worktree, e.g.
# `MULTILANG_IMAGE_TAG=mlconv-1234 make build-grpc-images`, so concurrent
# sessions on one host do not overwrite each other's images.
MULTILANG_IMAGE_TAG ?= dev
export MULTILANG_IMAGE_TAG

.PHONY: build build-all devel-build build-grpc-images clean-grpc-images submodules init init-hooks \
	test test-integration-perf verify verify-commit verify-commit-checks bump-version \
	select-tests pre-push-test-selection clean-test-selection clean

build: init-hooks build-all

# The release builds, for packaging and the benchmarks: the C library and the
# Python binding over the library as it ships (rust/Makefile `build-cdylib`, in
# target/release-cdylib/: the platform verifier, no Mozilla bundle), one
# release build with fat LTO. The tests use devel-build. Like the other
# cross-language targets it first initialises every submodule (kafka/ included,
# which the code generator and the lints read); the language folders fetch only
# what they need.
build-all: submodules
	$(MAKE) -C c RUST_PROFILE=release-cdylib build
	$(MAKE) -C python PROFILE=release-cdylib build

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

# Removes the five gRPC images that carry the current MULTILANG_IMAGE_TAG,
# e.g. a worktree's own tag once its work is done. The shared default `dev`
# images are left alone unless CLEAN_DEV_IMAGES=1, since other sessions on the
# host may be using them. The tag is checked and used with surrounding
# whitespace stripped, so "dev " is still dev, and anything that is not a
# Docker tag is refused.
CLEAN_IMAGE_TAG = $(strip $(MULTILANG_IMAGE_TAG))
clean-grpc-images:
	@case "$(CLEAN_IMAGE_TAG)" in \
		""|-*|.*|*[!A-Za-z0-9_.-]*) \
			echo "Refusing: '$(MULTILANG_IMAGE_TAG)' is not a Docker tag."; exit 1;; \
	esac
	@if [ "$(CLEAN_IMAGE_TAG)" = "dev" ] && [ "$(CLEAN_DEV_IMAGES)" != "1" ]; then \
		echo "Refusing to remove the shared :dev gRPC images; set MULTILANG_IMAGE_TAG, or CLEAN_DEV_IMAGES=1."; \
		exit 1; \
	fi
	-docker rmi confluent-kafka-rust/python-grpc-server:$(CLEAN_IMAGE_TAG) \
		confluent-kafka-rust/python-async-grpc-server:$(CLEAN_IMAGE_TAG) \
		confluent-kafka-rust/c-grpc-server:$(CLEAN_IMAGE_TAG) \
		confluent-kafka-rust/dotnet-grpc-server:$(CLEAN_IMAGE_TAG) \
		confluent-kafka-rust/dotnet-async-grpc-server:$(CLEAN_IMAGE_TAG)

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
# arms run with their binding), then the C and Python tests.
test: submodules
	$(MAKE) -C rust test-all-features
	$(MAKE) -C c test
	$(MAKE) -C python test

# The Rust, Python and .NET performance suites, one after the other: they
# assert latency budgets, so each needs the machine to itself. The .NET arm is
# the Docker-gated v3 smoke (it skips cleanly without Docker).
test-integration-perf:
	$(MAKE) -C rust test-integration-perf
	$(MAKE) -C python test-performance
	$(MAKE) -C dotnet test-integration-perf-dotnet

# The whole gate: the release builds, the Rust checks, the tests, and the
# Python binding's checks (mypy --strict and the static checks). It runs no
# .NET target: `make -C dotnet verify` is the .NET gate.
verify: build
	$(MAKE) -C rust format-check check-generated lint
	$(MAKE) test
	$(MAKE) -C python check

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

# `make bump-version VERSION=0.3.0-rc.1 [PYTHON_VERSION=...] [DOTNET_VERSION=...]`
# bumps the versions of the Rust crate, the Python wheels and the .NET package:
# see rust/Makefile. VERSION, PYTHON_VERSION and DOTNET_VERSION reach the
# sub-make through its environment (make passes command-line variables down);
# this recipe never pastes them in.
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
