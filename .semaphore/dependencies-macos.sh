#!/bin/bash
set -ex
# macOS counterpart to dependencies.sh, for the arm64 CI job. Confluent's
# self-hosted macOS Semaphore pool ships neither cmake nor rustup preinstalled
# (unlike the Ubuntu pool, which apt-installs them) -- see
# https://confluentinc.atlassian.net/wiki/spaces/TOOLS/pages/2820542346#macOS
# for the pool's tool inventory. Homebrew isn't listed as preinstalled either,
# so install it first if missing.
#
# `set -x` (in addition to `set -e`) is deliberate for this first real run of
# the macOS blocks: everything below is unverified against the actual agent
# (only against the DevProd tool-inventory doc), so trace every command while
# we find out what's really there.

echo "=== macOS agent diagnostics (pre-install) ==="
uname -a
sw_vers || true
echo "PATH=$PATH"

echo "--- Docker check ---"
if command -v docker >/dev/null 2>&1; then
  echo "docker binary found at $(command -v docker)"
  docker --version || true
  docker info || echo "docker info failed -- binary present but daemon not reachable"
else
  echo "docker binary NOT found on PATH"
fi

echo "--- Homebrew / cmake / rustup check (before install) ---"
command -v brew >/dev/null 2>&1 && brew --version || echo "brew not found"
command -v cmake >/dev/null 2>&1 && cmake --version || echo "cmake not found"
command -v rustc >/dev/null 2>&1 && rustc --version || echo "rustc not found"

echo "=== Installing dependencies ==="
if ! command -v brew >/dev/null 2>&1; then
  NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi
eval "$(/opt/homebrew/bin/brew shellenv)"
brew install cmake rustup-init
# `rust-toolchain.toml` pins the exact channel; running any rustc/cargo
# command inside the repo makes rustup auto-install that pinned version
# instead of a floating `stable` (see dependencies.sh history).
rustup-init -y --default-toolchain none
source "$HOME/.cargo/env"
git submodule update --init --depth=1 kafka

echo "=== macOS agent diagnostics (post-install) ==="
brew --version
cmake --version
rustc --version
cargo --version
