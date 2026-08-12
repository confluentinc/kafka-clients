#!/usr/bin/env bash
set -euo pipefail
#
# Job-scoped .NET SDK provisioning for the "Verify .NET binding" CI block
# (M10/P1, Decision 1). This lives OUTSIDE the shared `.semaphore/dependencies.sh`
# on purpose: no other job (verify-rust / verify-c / verify-python) needs a .NET
# SDK, so the install stays local to the one block that does.
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

# Export for this shell (harmless if the script is sourced) AND persist to the
# login profile so the subsequent job command (`make verify-dotnet`) resolves
# `dotnet`. Semaphore runs each command of a job in a login shell that sources
# ~/.bash_profile; a plain `export` here would not survive into the next command
# because this script executes in its own subshell, so the profile append is the
# mechanism that carries DOTNET_ROOT/PATH forward. Both ~/.bash_profile and
# ~/.bashrc are written for robustness across shell-init variants.
export DOTNET_ROOT
export PATH="${DOTNET_ROOT}:${PATH}"
export DOTNET_MULTILEVEL_LOOKUP=0

for profile in "${HOME}/.bash_profile" "${HOME}/.bashrc"; do
  {
    echo "export DOTNET_ROOT=\"${DOTNET_ROOT}\""
    echo "export PATH=\"${DOTNET_ROOT}:\${PATH}\""
    echo "export DOTNET_MULTILEVEL_LOOKUP=0"
  } >> "${profile}"
done

# Validate the install within this subshell (its own exports are live here).
# Both net8 and net10 runtimes must be present, or the net8.0 test leg fails.
dotnet --info
dotnet --list-sdks
dotnet --list-runtimes
