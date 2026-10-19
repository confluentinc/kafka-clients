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
# Builds the Rust FFI library that the Python extension links against
# (libconfluent_kafka.{so,dylib}) and the C headers that cbindgen generates for
# it, and stages them under python/native/, where .semaphore/build-wheels.sh
# finds them. On Linux the library is built inside a manylinux container, so
# that it runs on every distribution the wheel's manylinux tag covers: the
# script starts the container and runs itself inside it with --in-docker. On
# macOS it builds on the runner, for the macOS version in
# MACOSX_DEPLOYMENT_TARGET, which must be the one the wheels are built for.
# Must be POSIX sh.
#
# Usage (Linux):     .semaphore/build-ffi-library.sh <docker-image>
# Usage (macOS):     MACOSX_DEPLOYMENT_TARGET=<version> .semaphore/build-ffi-library.sh
# Usage (internal):  .semaphore/build-ffi-library.sh --in-docker
#
# Run from the repository root. On Linux the library is built from the
# committed HEAD, so uncommitted changes are not included. Outputs:
#   python/native/lib/libconfluent_kafka.{so,dylib}
#   python/native/include/confluent_kafka.h
#   python/native/include/confluent_kafka_bindings.h

set -eu

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

# The toolchain pinned in rust/rust-toolchain.toml.
toolchain() {
    sed -n 's/^channel *= *"\(.*\)"/\1/p' rust/rust-toolchain.toml
}

# Installs rustup with the pinned toolchain, unless rustup is already present,
# in which case only the pinned toolchain is installed.
install_rust() {
    channel=$(toolchain)
    [ -n "$channel" ] || { echo "could not read the toolchain from rust/rust-toolchain.toml" >&2; return 1; }
    [ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
    if command -v rustup >/dev/null 2>&1; then
        retry rustup toolchain install "$channel" --profile minimal
    else
        retry curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup-init.sh
        retry sh /tmp/rustup-init.sh -y --default-toolchain "$channel" --profile minimal
        . "$HOME/.cargo/env"
    fi
}

# Builds the library in rust/ and copies it, with both headers, to
# $1/python/native.
build_and_stage() {
    dest="$1"
    lib="$2"
    rm -f rust/target/include/confluent_kafka.h rust/target/include/confluent_kafka_bindings.h
    (cd rust && cargo build --locked --features ffi --release)
    test -f rust/target/include/confluent_kafka.h
    test -f rust/target/include/confluent_kafka_bindings.h
    rm -rf "$dest/python/native"
    mkdir -p "$dest/python/native/lib" "$dest/python/native/include"
    cp "rust/target/release/$lib" "$dest/python/native/lib/"
    cp rust/target/include/confluent_kafka.h rust/target/include/confluent_kafka_bindings.h \
        "$dest/python/native/include/"
}

if [ "${1:-}" = "--in-docker" ]; then
    set -x

    # Builds from a copy of the committed tree, so that the build leaves
    # nothing but python/native in the host's working tree. `git archive`
    # works whether HEAD is a branch or detached, and in a shallow clone.
    git config --global --add safe.directory /io
    mkdir /build
    git -C /io archive --format=tar HEAD | tar -xf - -C /build
    cd /build

    install_rust

    # Build dependencies of aws-lc-rs, the rustls crypto backend: cmake and
    # perl, and nasm on x86_64.
    if command -v dnf >/dev/null 2>&1; then PKG=dnf; else PKG=yum; fi
    command -v cmake >/dev/null 2>&1 || retry "$PKG" install -y cmake
    command -v perl  >/dev/null 2>&1 || retry "$PKG" install -y perl
    if [ "$(uname -m)" = "x86_64" ]; then
        command -v nasm >/dev/null 2>&1 || retry "$PKG" install -y nasm
    fi

    build_and_stage /io libconfluent_kafka.so
    # The container runs as root; give the staged files to the owner of the
    # working tree, so that they can be removed without root.
    chown -R "$(stat -c %u:%g /io)" /io/python/native

    lib=/io/python/native/lib/libconfluent_kafka.so
    echo "== LINKAGE =="; ldd "$lib" || true
    echo "== GLIBC symbol floor =="; objdump -T "$lib" | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1 || true
    echo "== SHA256 =="; sha256sum "$lib" /io/python/native/include/*.h || true
    exit 0
fi

if [ "$(uname -s)" = "Darwin" ]; then
    # Read by rustc and by the C compiler that builds aws-lc, so that no part of
    # the library requires a newer macOS than the wheel's platform tag states.
    : "${MACOSX_DEPLOYMENT_TARGET:?set MACOSX_DEPLOYMENT_TARGET to the macOS version the wheels are built for}"
    export MACOSX_DEPLOYMENT_TARGET
    set -x

    install_rust

    # Build dependencies of aws-lc-rs, the rustls crypto backend: cmake, and
    # nasm on x86_64.
    command -v cmake >/dev/null 2>&1 || retry brew install cmake
    if [ "$(uname -m)" = "x86_64" ]; then
        command -v nasm >/dev/null 2>&1 || retry brew install nasm
    fi

    build_and_stage "$PWD" libconfluent_kafka.dylib

    lib=python/native/lib/libconfluent_kafka.dylib
    # An @rpath install name, so that the extension links against
    # @rpath/libconfluent_kafka.dylib rather than this runner's absolute path,
    # and delocate can copy the library into the wheel.
    install_name_tool -id @rpath/libconfluent_kafka.dylib "$lib"
    echo "== LINKAGE =="; otool -L "$lib" || true
    echo "== INSTALL NAME =="; otool -D "$lib" || true
    echo "== MINIMUM OS VERSION =="; otool -l "$lib" | grep -A3 LC_BUILD_VERSION || true
    echo "== SHA256 =="; shasum -a 256 "$lib" python/native/include/*.h || true
    exit 0
fi

docker_image="${1:?Usage: $0 <docker-image>}"

retry docker pull "$docker_image"
exec docker run --rm -v "$PWD":/io -w /io "$docker_image" \
    sh /io/.semaphore/build-ffi-library.sh --in-docker
