#!/bin/bash
set -ex
# macOS counterpart to dependencies.sh, for the arm64 CI job. Confluent's
# self-hosted macOS Semaphore pool ships neither cmake, rustup, nor Docker
# preinstalled (unlike the Ubuntu pool) -- see
# https://confluentinc.atlassian.net/wiki/spaces/TOOLS/pages/2820542346#macOS.
# Homebrew isn't preinstalled either, so install it first if missing.

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
brew install cmake
# Homebrew's `rustup` formula no longer ships a `rustup-init` binary and is
# keg-only, so use the official installer script instead.
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --default-toolchain none
source "$HOME/.cargo/env"
git submodule update --init --depth=1 kafka

# Docker via Colima, mirroring ce-kafka's bazel_macos_runner.yml +
# tools-bazel/ci-setup-docker.sh, already proven on this same agent type
# (s1-macos-15-arm64-8).
echo "=== Setting up Docker via Colima ==="
if colima status &>/dev/null; then
  echo "Colima is already running"
else
  command -v colima >/dev/null 2>&1 || brew install colima
  colima start --cpu 2 --memory 12 --disk 50
fi

echo "Waiting for Docker daemon..."
docker_wait_timeout=90
while ! docker info >/dev/null 2>&1; do
  if [ "$docker_wait_timeout" -le 0 ]; then
    echo "ERROR: Docker failed to start within 90 seconds"
    colima status || true
    exit 1
  fi
  sleep 3
  docker_wait_timeout=$((docker_wait_timeout - 3))
done
echo "Docker is ready:"
docker info

# testcontainers needs the socket path as seen inside the Colima VM for its
# Ryuk reaper container, not the path the macOS host sees.
export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
export TESTCONTAINERS_RYUK_DISABLED=false


export CONFLUENT_KAFKA_TEST_FUTURE_TIMEOUT=8

echo "=== macOS agent diagnostics (post-install) ==="
brew --version
cmake --version
rustc --version
cargo --version
docker --version
colima status

# Only drop xtrace -- this script is `source`d (not run in a subshell), so
# `set +e` here would also disable errexit for the job commands that follow
# in the same shell session, letting a failing test silently report success.
set +x
