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
# Verify the built wheels on bare Linux distro containers, across every
# supported Python version. For each distro x Python it installs the wheel and
# runs: import, the binding unit tests, and a compression/TLS feature smoke.
# uv provides each Python as a portable standalone build, so a distro's own
# Python version does not limit coverage. Must be POSIX sh.
#
# Usage: .semaphore/test-wheels.sh <wheelhouse-dir>
# Override the distro list with DISTRO_IMAGES="img1 img2 ...".

set -eu

wheelhouse="${1:?Usage: $0 <wheelhouse-dir>}"
PY_VERSIONS="3.10 3.11 3.12 3.13 3.14"

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
    export PATH="/root/.local/bin:$PATH"
    uv python install $PY_VERSIONS

    # Neutral test dir so 'import producer' resolves to the installed wheel.
    testtmp=$(mktemp -d)
    cp -r /io/bindings/python/test "$testtmp/"

    for py in $PY_VERSIONS; do
        echo "== Python $py =="
        uv venv --python "$py" "/tmp/v$py"
        vpy="/tmp/v$py/bin/python"
        uv pip install --python "$vpy" --no-index --find-links "/io/$wheelhouse" confluent-kafka-rust-python
        uv pip install --python "$vpy" pytest
        "$vpy" -c "import _confluentkafka; print('import OK: Python $py')"
        ( cd "$testtmp" && "$vpy" -m pytest test/unit -q )
        "$vpy" -c "from producer import KafkaProducer as P; [P({'bootstrap.servers':'localhost:9092','compression.type':c}).close() or print('OK: compression '+c) for c in ('gzip','snappy','lz4','zstd')]; P({'bootstrap.servers':'localhost:9092','security.protocol':'SSL'}).close(); print('OK: security.protocol=SSL')"
    done
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
