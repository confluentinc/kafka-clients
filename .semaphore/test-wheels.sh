#!/bin/sh
#
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
#
#
# Verify the built wheels across every supported Python version. On Linux the
# matrix runs on bare distro containers; on macOS it runs natively on the
# runner. For each environment it installs the wheel and runs: import, the
# binding unit tests, and a compression/TLS feature smoke. uv provides each
# Python as a portable standalone build, so a host's own Python version does
# not limit coverage. Must be POSIX sh.
#
# Usage: .semaphore/test-wheels.sh <wheelhouse-dir>
# Override the distro list (Linux) with DISTRO_IMAGES="img1 img2 ...".

set -eu

wheelhouse="${1:?Usage: $0 <wheelhouse-dir>}"
PY_VERSIONS="3.10 3.11 3.12 3.13 3.14"

# $1 is the source root the wheelhouse and bindings are found under.
run_matrix() {
    base="$1"
    export PATH="$HOME/.local/bin:$PATH"
    uv python install $PY_VERSIONS

    # Neutral test dir so 'import producer' resolves to the installed wheel.
    testtmp=$(mktemp -d)
    cp -r "$base/bindings/python/test" "$testtmp/"

    for py in $PY_VERSIONS; do
        echo "== Python $py =="
        uv venv --python "$py" "/tmp/v$py"
        vpy="/tmp/v$py/bin/python"
        uv pip install --python "$vpy" --no-index --find-links "$base/$wheelhouse" confluent-kafka4
        uv pip install --python "$vpy" pytest pytest-asyncio
        "$vpy" -c "import _confluentkafka; print('import OK: Python $py')"
        # asyncio_mode=auto is set here rather than read from pyproject.toml,
        # since tests run from a neutral dir without it (see above).
        ( cd "$testtmp" && "$vpy" -m pytest test/unit -q -o asyncio_mode=auto )
        "$vpy" -c "from producer import KafkaProducer as P; [P({'bootstrap.servers':'localhost:9092','compression.type':c}).close() or print('OK: compression '+c) for c in ('gzip','snappy','lz4','zstd')]; P({'bootstrap.servers':'localhost:9092','security.protocol':'SSL'}).close(); print('OK: security.protocol=SSL')"
    done
}

if [ "${IN_DOCKER:-0}" = "1" ]; then
    set -x

    # curl is needed to fetch uv (AlmaLinux ships curl-minimal already).
    if ! command -v curl >/dev/null 2>&1; then
        if command -v apt-get >/dev/null 2>&1; then
            export DEBIAN_FRONTEND=noninteractive
            apt-get update -qq && apt-get install -y -qq curl ca-certificates
        elif command -v dnf >/dev/null 2>&1; then
            dnf install -y -q curl
        fi
    fi

    export HOME=/root
    curl -LsSf https://astral.sh/uv/install.sh | sh
    run_matrix /io
    exit 0
fi

if [ "$(uname -s)" = "Darwin" ]; then
    set -x
    command -v uv >/dev/null 2>&1 || curl -LsSf https://astral.sh/uv/install.sh | sh
    run_matrix "$PWD"
    exit 0
fi

: "${DISTRO_IMAGES:=almalinux:8 almalinux:9 ubuntu:22.04 debian:12 ubuntu:24.04}"

# Shared uv cache on the host so Python builds download once, not per distro.
uvcache=/tmp/uvcache
mkdir -p "$uvcache"

for img in $DISTRO_IMAGES; do
    echo "== testing wheels on $img =="
    docker run --rm \
        -e IN_DOCKER=1 \
        -e UV_CACHE_DIR=/uvcache/cache \
        -e UV_PYTHON_INSTALL_DIR=/uvcache/pythons \
        -v "$uvcache":/uvcache \
        -v "$PWD":/io -w /io "$img" \
        sh /io/.semaphore/test-wheels.sh "$wheelhouse"
done
