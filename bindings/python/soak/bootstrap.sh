#!/usr/bin/env bash
#
# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# One-time EC2 (Ubuntu) bootstrap for the Rust client soak.
#
# Installs the build toolchain, the OpenTelemetry Collector and the soak's
# metrics pipeline, then builds the client into the venv the soak runs from.
# Modelled on the reference librdkafka Python soak's bootstrap.sh
# (confluent-kafka-python/tests/soak/bootstrap.sh), adapted for the Rust client:
# a Rust toolchain + jemalloc instead of a librdkafka build, and no
# setup_all_versions.py (that is librdkafka-version-specific).
#
# The Rust repository is not public yet and CANNOT be cloned on the box (the
# Confluent GitHub org IP allow list blocks it — auth succeeds, the IP check
# fails). Ship the source with `git archive | scp`, unpack it, and run this from
# the unpacked bindings/python/soak directory. Because an scp'd tree has no .git,
# the commit SHA must be passed explicitly (build.sh --sha requires it).
#
# Usage:
#   ./bootstrap.sh <sha> [label]        # first-time setup; build.sh for rebuilds
#
# Before running, fill the three FILL_IN_* values in otel-config.yaml.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <sha> [label]" >&2
    exit 1
fi
SHA="$1"
LABEL="${2:-bootstrap}"

SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$(cd "$SOAK_DIR/../../.." && pwd)"   # bindings/python/soak -> repo root

otel_collector_version=0.130.0
otel_collector_package_url="https://github.com/open-telemetry/"\
"opentelemetry-collector-releases/releases/download/"\
"v${otel_collector_version}/otelcol-contrib_${otel_collector_version}_linux_amd64.deb"

# --- validate the collector config before touching the box ------------------
validate_config() {
    local errors=0
    local token
    for token in FILL_IN_REGION_HERE FILL_IN_ROLE_ARN_HERE FILL_IN_REMOTE_WRITE_ENDPOINT_HERE; do
        if grep -q "$token" "$SOAK_DIR/otel-config.yaml"; then
            echo "ERROR: $token is not filled in otel-config.yaml" >&2
            errors=$((errors + 1))
        fi
    done
    if [[ $errors -gt 0 ]]; then
        echo "Configuration validation failed. Fill in otel-config.yaml and re-run." >&2
        exit 1
    fi
    echo "Configuration validation passed."
}
validate_config

# --- system packages --------------------------------------------------------
# libjemalloc2: SOAK_JEMALLOC=true LD_PRELOADs it to fix the glibc RSS strand a
# producer stall leaves behind; run.sh fails loudly at startup if it is
# requested but missing.
sudo apt-get update
sudo apt-get install -y \
    git curl wget build-essential pkg-config libssl-dev \
    python3-dev python3-pip python3-venv \
    libjemalloc2

# --- Rust toolchain (build.sh assumes cargo/rustc are on PATH) --------------
if ! command -v cargo >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
export PATH="$HOME/.cargo/bin:$PATH"

# --- OpenTelemetry Collector ------------------------------------------------
wget -O otel_collector_package.deb "$otel_collector_package_url"
sudo dpkg -i otel_collector_package.deb
rm otel_collector_package.deb
sudo cp "$SOAK_DIR/otel-config.yaml" /etc/otelcol-contrib/config.yaml
sudo systemctl restart otelcol-contrib

# --- build the client into the soak venv ------------------------------------
"$SOAK_DIR/build.sh" --src "$SRC_DIR" --sha "$SHA" --label "$LABEL"

venv="$SRC_DIR/venv-soak"
echo
echo "All done. Activate the virtualenv before running the soak:"
echo "  source $venv/bin/activate"
