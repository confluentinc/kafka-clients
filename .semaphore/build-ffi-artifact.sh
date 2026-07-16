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
# Build the FFI shared library (libconfluent_kafka.so) and the cbindgen header
# (confluent_kafka.h) inside a manylinux container, producing a glibc-portable
# library. The script spins up the container on the host and re-execs itself
# inside it via --in-docker. Must be POSIX sh.
#
# Usage (host):      .semaphore/build-ffi-artifact.sh <docker-image>
# Usage (internal):  .semaphore/build-ffi-artifact.sh --in-docker
#
# Run from the repo root. Outputs:
#   dist/libconfluent_kafka.so
#   dist/confluent_kafka.h

set -eu

if [ "${1:-}" = "--in-docker" ]; then
    set -x

    # Build in a pristine clone so the host tree stays clean.
    git config --global --add safe.directory /io
    git config --global --add safe.directory /io/.git
    git clone /io /build
    cd /build

    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain stable --profile minimal
    . "$HOME/.cargo/env"

    # aws-lc-rs (rustls crypto backend) build deps: cmake + perl always, nasm
    # only on x86_64.
    if command -v dnf >/dev/null 2>&1; then PKG=dnf; else PKG=yum; fi
    command -v cmake >/dev/null 2>&1 || "$PKG" install -y cmake
    command -v perl  >/dev/null 2>&1 || "$PKG" install -y perl
    if [ "$(uname -m)" = "x86_64" ]; then
        command -v nasm >/dev/null 2>&1 || "$PKG" install -y nasm
    fi

    cargo build --features ffi --release

    lib=target/release/libconfluent_kafka.so
    echo "== LINKAGE =="; ldd "$lib" || true
    echo "== GLIBC symbol floor =="; objdump -T "$lib" | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1 || true
    echo "== SHA256 =="; sha256sum "$lib" target/include/confluent_kafka.h || true

    mkdir -p /io/dist
    cp "$lib" target/include/confluent_kafka.h /io/dist/
    exit 0
fi

docker_image="${1:?Usage: $0 <docker-image>}"

exec docker run --rm -v "$PWD":/io -w /io "$docker_image" \
    sh /io/.semaphore/build-ffi-artifact.sh --in-docker
