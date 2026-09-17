#!/bin/bash
set -ex

echo "=== macOS agent diagnostics (pre-install) ==="
uname -a
sw_vers || true
echo "PATH=$PATH"

# macOS's default per-process open-file soft limit (typically 256) is far
# too low for the multilanguage integration suite: each live Kafka cluster
# and gRPC backend container holds several sockets (broker connections,
# Colima's docker.sock proxy, gRPC channels), and testcontainers' own
# cluster-eviction bound (TARGET_LIVE_CLUSTERS) caps concurrent clusters,
# not total fds in flight across RUST_TEST_THREADS parallel test threads.
# Without this, bollard's docker-socket calls fail with "Too many open
# files" partway through the suite (a cascading failure, not a single
# test's fault) -- the Linux CI image's default is already high enough
# that this was never needed there. 10240 matches macOS's typical
# kern.maxfilesperproc ceiling; fall back to the process's hard limit if
# that is lower on this host.
echo "--- Raising open-file limit for the Docker-heavy integration suite ---"
echo "ulimit -n before: $(ulimit -n)"
ulimit -n 10240 2>/dev/null || ulimit -n "$(ulimit -Hn)"
echo "ulimit -n after: $(ulimit -n)"

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
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --default-toolchain none
source "$HOME/.cargo/env"
git submodule update --init --depth=1 kafka

# MACOS_SKIP_COLIMA=true skips the Colima/Docker bring-up below (the toolchain
# setup above always runs). Set by blocks whose tests need no container.
if [ "${MACOS_SKIP_COLIMA:-}" = "true" ]; then
  echo "=== MACOS_SKIP_COLIMA=true -- skipping Colima/Docker setup (unit-tests-only block) ==="
else
  echo "=== Setting up Docker via Colima ==="
  # The agent (s1-macos-15-arm64-8) has 8 physical cores, but Colima's VM was
  # only getting 2 -- too tight once RUST_TEST_THREADS=4 parallel test threads
  # each try to bring up a KRaft broker container at once. Under that
  # contention a broker can fail its controller-quorum registration handshake
  # before the (non-retried, by design -- see kafka_cluster.rs's
  # is_port_allocation_error comment) startup timeout, aborting the whole
  # suite. Bump to 6, leaving 2 cores of headroom for the host OS and the
  # Semaphore agent process itself.
  if colima status &>/dev/null; then
    echo "Colima is already running"
  else
    command -v colima >/dev/null 2>&1 || brew install colima
    colima start --cpu 6 --memory 12 --disk 50
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

  export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
  export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
  export TESTCONTAINERS_RYUK_DISABLED=false
fi

export RUST_TEST_THREADS=4

export CONFLUENT_KAFKA_TEST_FUTURE_TIMEOUT=8

echo "=== macOS agent diagnostics (post-install) ==="
brew --version
cmake --version
rustc --version
cargo --version
if [ "${MACOS_SKIP_COLIMA:-}" != "true" ]; then
  docker --version
  colima status
fi

set +x
