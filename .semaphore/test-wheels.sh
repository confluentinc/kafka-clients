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
# Install the built wheels on bare Linux distro containers and import the
# extension, verifying the manylinux wheels are portable across distros and
# glibc versions (almalinux:8 is at the glibc 2.28 floor). The script loops
# over the distro images on the host and re-execs itself inside each via
# IN_DOCKER. Must be POSIX sh.
#
# Usage: .semaphore/test-wheels.sh <wheelhouse-dir>
# Override the distro list with DISTRO_IMAGES="img1 img2 ...".

set -eu

wheelhouse="${1:?Usage: $0 <wheelhouse-dir>}"

if [ "${IN_DOCKER:-0}" = "1" ]; then
    set -x
    # Install a Python >= 3.10 + pip for this distro's package manager.
    if command -v apt-get >/dev/null 2>&1; then
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -qq
        apt-get install -y -qq python3 python3-venv python3-pip
        py=python3
    elif command -v dnf >/dev/null 2>&1; then
        dnf install -y -q python3.11 python3.11-pip
        py=python3.11
    else
        echo "$0: no supported package manager in image"; exit 1
    fi
    "$py" -m venv /tmp/venv
    . /tmp/venv/bin/activate
    python -m pip install --no-index --find-links "/io/$wheelhouse" confluent-kafka-rust-python
    python -c "import _confluentkafka; print('import OK')"
    exit 0
fi

: "${DISTRO_IMAGES:=ubuntu:22.04 almalinux:8}"
for img in $DISTRO_IMAGES; do
    echo "== testing wheel on $img =="
    docker run --rm -e IN_DOCKER=1 -v "$PWD":/io -w /io "$img" \
        sh /io/.semaphore/test-wheels.sh "$wheelhouse"
done
