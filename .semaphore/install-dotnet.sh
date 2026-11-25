#!/usr/bin/env bash
set -euo pipefail
#
# Job-scoped .NET SDK provisioning, shared by two jobs in the "Verify Rust and
# language bindings (Linux amd64)" CI block, "verify-dotnet (Linux amd64) —
# plaintext" (M10/P1, Decision 1) and "verify-dotnet (Linux amd64) — admin
# examples", which runs the .NET admin examples on the host, and by the
# "verify-dotnet (macOS arm64)", "verify-dotnet (macOS arm64) — plaintext gRPC
# arm" and "verify-dotnet (macOS arm64) — plaintext Native AOT gRPC arm" jobs in
# "Verify Rust and language bindings (macOS arm64)". The
# Linux "verify-dotnet (Linux amd64) —
# plaintext gRPC arm", "— plaintext Native AOT gRPC arm", "— ssl" and "— sasl_ssl
# (plain)" jobs do NOT run this script: they run only the gRPC multilanguage arm,
# whose .NET server runs
# inside the gRPC Docker images, so they need no host .NET. This lives OUTSIDE the shared
# dependency-install scripts -- `.semaphore/dependencies.sh` on Linux,
# `.semaphore/dependencies-macos.sh` on macOS -- and outside either block's
# shared task prologue, on purpose: no other job in either block needs a host
# .NET SDK, so the install stays local to the jobs that do. The
# official `dotnet-install.sh` this script fetches
# is cross-platform (Linux + macOS, both have bash + curl), so the same
# script and the same job-level export sequence work unmodified on both.
#
# One install into DOTNET_ROOT ($HOME/.dotnet): SDK 10.0, which builds every TFM
# (netstandard2.0 / net462 / net10.0) AND runs the tests, all of which target
# net10.0; Confluent.Kafka.UnitTests.NetStandard runs the unit tests against the
# library's netstandard2.0 build on that runtime too. No .NET 8 runtime: .NET 8 is
# out of Microsoft support, and the package has no net8.0 build.
# SDK 10 also brings the ASP.NET Core 10 runtime, which the native, host-process gRPC
# server on the macOS job runs on (M17/P1 D3); the gRPC Docker images bundle their
# own aspnet:10.0.
#
# DOTNET_MULTILEVEL_LOOKUP=0 + one DOTNET_ROOT keep runtime resolution
# deterministic (the install is found under $HOME/.dotnet only), mirroring
# CKD's macOS block.

DOTNET_ROOT="${HOME}/.dotnet"

# Runs a command up to 5 times, 5 seconds apart: for a download, which can fail for a moment.
# The installer downloads the SDK, so its run is retried too.
retry() {
    _n=1
    while [ "$_n" -le 5 ]; do
        if "$@"; then return 0; fi
        [ "$_n" -eq 5 ] && break
        echo "retry: attempt $_n/5 of '$*' failed; sleeping 5s" >&2
        _n=$((_n + 1))
        sleep 5
    done
    echo "retry: '$*' failed after 5 attempts" >&2
    return 1
}

# Fetch the official installer to a temp path (never curl | bash — keep the
# downloaded script inspectable / re-runnable within the job).
INSTALL_SCRIPT="$(mktemp)"
retry curl -fsSL https://dot.net/v1/dotnet-install.sh -o "${INSTALL_SCRIPT}"
chmod +x "${INSTALL_SCRIPT}"

# SDK 10 (builds all TFMs, runs net10.0).
retry "${INSTALL_SCRIPT}" --channel 10.0 --install-dir "${DOTNET_ROOT}"

# This script INSTALLS only -- it deliberately does NOT export DOTNET_ROOT/PATH.
# Semaphore runs a job's prologue + job `commands:` in ONE persistent shell
# session, but this script executes in a child subshell, so any `export` here
# would die on return and never reach the later
# `make -C dotnet verify-except-grpc-arm` (Linux) /
# `make -C dotnet verify-macos` (macOS) command. The SDK env is therefore set by
# top-level `export` commands in each verify-dotnet job's own `commands:`
# (`.semaphore/semaphore.yml`, right after this script runs), which mutate
# that shared session directly. This matches the
# confluent-kafka-dotnet precedent, whose .semaphore/semaphore.yml sets
# DOTNET_ROOT/PATH/DOTNET_MULTILEVEL_LOOKUP as top-level job commands and writes
# no profile file. Keep the install dir here (${DOTNET_ROOT}) identical to the
# DOTNET_ROOT/PATH set in those job commands.

# Validate the install using an absolute path -- `dotnet` is NOT on PATH inside
# this script (the PATH export happens after this script returns, in the job's
# own commands). The net10 runtime must be present, or the tests cannot run.
"${DOTNET_ROOT}/dotnet" --list-sdks
"${DOTNET_ROOT}/dotnet" --list-runtimes
