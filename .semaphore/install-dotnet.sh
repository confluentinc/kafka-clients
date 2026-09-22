#!/usr/bin/env bash
set -euo pipefail
#
# Job-scoped .NET SDK provisioning, shared by the verify-dotnet job in the
# "Verify language bindings (Linux amd64)" CI block (M10/P1, Decision 1) and
# the "verify-dotnet (macOS arm64)" job in "Build + unit test bindings
# (macOS)". This lives OUTSIDE the shared dependency-install scripts --
# `.semaphore/dependencies.sh` on Linux, `.semaphore/dependencies-macos.sh` on
# macOS -- and outside either block's shared task prologue, on purpose: no
# other job in either block needs a .NET SDK, so the install stays local to
# the one job that does. The official `dotnet-install.sh` this script fetches
# is cross-platform (Linux + macOS, both have bash + curl), so the same
# script and the same job-level export sequence work unmodified on both.
#
# Two installs into a single DOTNET_ROOT ($HOME/.dotnet), coexisting:
#   1. SDK 10.0            — builds every TFM (netstandard2.0 / net462 / net8.0 /
#                            net10.0) AND runs the net10.0 unit tests.
#   2. .NET 8 base runtime — needed to RUN the net8.0 unit tests. A net8.0 test
#      (--runtime dotnet)    assembly does NOT roll forward to the net10 runtime
#                            by default, so SDK 10 alone cannot execute
#                            `dotnet test -f net8.0`. The lean base runtime
#                            (Microsoft.NETCore.App) is all a net8.0 *unit* test
#                            needs; the ASP.NET Core runtime is only used inside
#                            the gRPC Docker images, which bundle their own
#                            aspnet:8.0. (CKD installs both as full SDKs; the lean
#                            runtime is a smaller, faster CI install and
#                            sufficient here — deliberate, not an omission.)
#
# DOTNET_MULTILEVEL_LOOKUP=0 + one DOTNET_ROOT keep runtime resolution
# deterministic (both installs are found under $HOME/.dotnet only), mirroring
# CKD's macOS block.

DOTNET_ROOT="${HOME}/.dotnet"

# Fetch the official installer to a temp path (never curl | bash — keep the
# downloaded script inspectable / re-runnable within the job).
INSTALL_SCRIPT="$(mktemp)"
curl -fsSL https://dot.net/v1/dotnet-install.sh -o "${INSTALL_SCRIPT}"
chmod +x "${INSTALL_SCRIPT}"

# 1) SDK 10 (builds all TFMs, runs net10.0).
"${INSTALL_SCRIPT}" --channel 10.0 --install-dir "${DOTNET_ROOT}"
# 2) .NET 8 base runtime (runs net8.0 tests).
"${INSTALL_SCRIPT}" --channel 8.0 --runtime dotnet --install-dir "${DOTNET_ROOT}"

# This script INSTALLS only -- it deliberately does NOT export DOTNET_ROOT/PATH.
# Semaphore runs a job's prologue + job `commands:` in ONE persistent shell
# session, but this script executes in a child subshell, so any `export` here
# would die on return and never reach the later `make verify-dotnet` /
# `make verify-dotnet-macos-docker` command. The SDK env is therefore set by
# top-level `export` commands in each verify-dotnet job's own `commands:`
# (`.semaphore/semaphore.yml`, right after this script runs), which mutate
# that shared session directly. This matches the
# confluent-kafka-dotnet precedent, whose .semaphore/semaphore.yml sets
# DOTNET_ROOT/PATH/DOTNET_MULTILEVEL_LOOKUP as top-level job commands and writes
# no profile file. Keep the install dir here (${DOTNET_ROOT}) identical to the
# DOTNET_ROOT/PATH set in those job commands.

# Validate the install using an absolute path -- `dotnet` is NOT on PATH inside
# this script (the PATH export happens after this script returns, in the job's
# own commands). Both net8 and net10 runtimes must be present, or the net8.0
# test leg fails.
"${DOTNET_ROOT}/dotnet" --list-sdks
"${DOTNET_ROOT}/dotnet" --list-runtimes
