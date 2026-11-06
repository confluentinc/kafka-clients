#!/bin/sh
#
# Copyright 2026 Confluent Inc.
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
# Builds the Node.js addon of this machine's platform for its npm platform package, for the
# "Release (JavaScript): native addon" blocks of semaphore.yml. The Rust core is the library the
# Python wheels and the NuGet package carry, built the same way (.semaphore/build-ffi-library.sh),
# whose static archive the addon links (javascript/binding.gyp, CONFLUENT_KAFKA_LINK=static), so
# the addon is the one file of the platform package. On Linux both are built in the
# manylinux_2_28 image that build-wheels.sh uses, so that the addon needs glibc 2.28 at most: the
# addon with a pinned Node.js whose tarball's SHA-256 is checked, and node-gyp as its npm bundles
# it. On macOS both are built on the runner for macOS 11.0, with the Node.js on PATH. Must be
# POSIX sh.
#
# Usage, from the repository root (on macOS with MACOSX_DEPLOYMENT_TARGET=11.0):
#   .semaphore/build-javascript-native.sh <out-dir>
# Writes <out-dir>/<platform>/confluent-kafka-javascript.node, after check-npm.py's native checks.
# <out-dir> must not exist or be empty. On Linux the addon is built from the committed HEAD.
# Usage (internal): .semaphore/build-javascript-native.sh --in-docker <platform>
#   Builds the addon inside the image, from /io (the working tree) into /out/<platform>.

set -eu

# The Node.js whose node-gyp and headers build the Linux addon: the Node 22 of the CI jobs
# (`sem-version node 22`). Node-API is ABI-stable, so the addon also loads in Node 18, 20 and 24.
NODE_VERSION=22.23.4
NODE_SHA256_X64=b8492284c76a54baf58d61668dd0e41ee0fc98adabf9503d30f05d08f55b4406
NODE_SHA256_ARM64=f7a1e486dbc8b286847815b477d4fb5e9fb963ed4489c3e1588853d685f589f9

die() {
    echo "build-javascript-native: $*" >&2
    exit 1
}

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

if [ "${1:-}" = "--in-docker" ]; then
    platform="$2"
    set -x
    case "$(uname -m)" in
        x86_64) node_arch=x64; node_sha256=$NODE_SHA256_X64 ;;
        aarch64) node_arch=arm64; node_sha256=$NODE_SHA256_ARM64 ;;
        *) die "no Node.js tarball for $(uname -m)" ;;
    esac
    node_tarball="node-v$NODE_VERSION-linux-$node_arch.tar.gz"
    retry curl --proto '=https' --tlsv1.2 -sSfL -o "/tmp/$node_tarball" "https://nodejs.org/dist/v$NODE_VERSION/$node_tarball"
    echo "$node_sha256  /tmp/$node_tarball" | sha256sum -c - || die "$node_tarball does not have the pinned SHA-256"
    mkdir /opt/node
    tar -xzf "/tmp/$node_tarball" -C /opt/node --strip-components=1
    export PATH="/opt/node/bin:$PATH"
    node --version

    # gyp, which node-gyp runs, needs a recent Python: the image's own, not the system's.
    for python in /opt/python/cp312-cp312/bin/python3 /opt/python/cp311-cp311/bin/python3; do
        if [ -x "$python" ]; then
            export PYTHON="$python"
            break
        fi
    done

    # Builds from a copy of the committed tree, as build-ffi-library.sh does, so that the build
    # leaves nothing in the host's working tree but the addon it writes.
    git config --global --add safe.directory /io
    mkdir /build
    git -C /io archive --format=tar HEAD:javascript | tar -xf - -C /build
    # The addon only: no npm install, since node-gyp is npm's own copy and the TypeScript build
    # (dist/) is the pack's.
    make -C /build build-addon CONFLUENT_KAFKA_LINK=static \
        CONFLUENT_KAFKA_LIB_DIR=/io/python/native/lib CONFLUENT_KAFKA_INCLUDE_DIR=/io/python/native/include
    mkdir -p "/out/$platform"
    cp /build/build/Release/confluent-kafka-javascript.node "/out/$platform/"
    chown -R "$(stat -c %u:%g /io)" /out
    addon="/out/$platform/confluent-kafka-javascript.node"
    echo "== LINKAGE =="; ldd "$addon" || true
    echo "== GLIBC symbol floor =="; objdump -T "$addon" | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1 || true
    echo "== EXPORTS =="; objdump -T "$addon" | grep -v '\*UND\*' | grep -E ' (g|w) ' || true
    exit 0
fi

[ $# -eq 1 ] || { echo "Usage: $0 <out-dir>" >&2; exit 2; }
out="$1"
[ -f .semaphore/build-ffi-library.sh ] || die "run from the repository root"
if [ -e "$out" ] && [ -n "$(ls -A "$out")" ]; then
    die "$out is not empty"
fi

case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) platform=linux-x64-gnu ;;
    Linux-aarch64) platform=linux-arm64-gnu ;;
    Darwin-arm64) platform=darwin-arm64 ;;
    *) die "the package has no platform package for $(uname -s) $(uname -m)" ;;
esac

case "$platform" in
    linux-*)
        tag=$(sed -n 's/^MANYLINUX_TAG=//p' .semaphore/build-wheels.sh)
        [ -n "$tag" ] || die "could not read MANYLINUX_TAG from .semaphore/build-wheels.sh"
        image="quay.io/pypa/manylinux_2_28_$(uname -m):$tag"
        STAGE_STATIC_LIB=1 sh .semaphore/build-ffi-library.sh "$image"
        [ -f python/native/lib/libconfluent_kafka.a ] || die "build-ffi-library.sh staged no libconfluent_kafka.a"
        mkdir -p "$out"
        out_abs=$(cd "$out" && pwd -P)
        docker run --rm -v "$PWD":/io:ro -v "$out_abs":/out -w /io "$image" \
            sh /io/.semaphore/build-javascript-native.sh --in-docker "$platform"
        ;;
    darwin-*)
        [ "${MACOSX_DEPLOYMENT_TARGET:-}" = "11.0" ] \
            || die "set MACOSX_DEPLOYMENT_TARGET=11.0, the oldest macOS the package supports"
        command -v node >/dev/null 2>&1 || die "node is not on PATH; run \`sem-version node 22\`"
        STAGE_STATIC_LIB=1 sh .semaphore/build-ffi-library.sh
        [ -f python/native/lib/libconfluent_kafka.a ] || die "build-ffi-library.sh staged no libconfluent_kafka.a"
        make -C javascript build-addon CONFLUENT_KAFKA_LINK=static \
            CONFLUENT_KAFKA_LIB_DIR="$PWD/python/native/lib" CONFLUENT_KAFKA_INCLUDE_DIR="$PWD/python/native/include"
        mkdir -p "$out/$platform"
        cp javascript/build/Release/confluent-kafka-javascript.node "$out/$platform/"
        echo "== LINKAGE =="; otool -L "$out/$platform/confluent-kafka-javascript.node" || true
        echo "== MINIMUM OS VERSION =="; otool -l "$out/$platform/confluent-kafka-javascript.node" | grep -A3 LC_BUILD_VERSION || true
        ;;
esac

python3 .semaphore/check-npm.py native "$platform" "$out/$platform/confluent-kafka-javascript.node"
ls -l "$out/$platform"
