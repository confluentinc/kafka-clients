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
# Builds the wheels of the Python binding into wheelhouse/, one for each
# supported CPython version, for the platform of the machine it runs on (Linux
# x86_64 or aarch64, macOS x86_64 or arm64). It first builds the Rust FFI
# library (.semaphore/build-ffi-library.sh), then compiles the C extension
# (python/_confluentkafka.c) against it with cibuildwheel, which repairs each
# wheel so that it contains the library: auditwheel copies the .so and sets the
# manylinux tag, delocate copies the .dylib. Must be POSIX sh.
#
# Usage: .semaphore/build-wheels.sh
# On macOS, set MACOSX_DEPLOYMENT_TARGET to the oldest macOS version the wheels
# support. Run from the repository root, with Python 3.11 or later as `python`
# (cibuildwheel's own requirement).

set -eu

# The manylinux_2_28 image (glibc 2.28) that both the FFI library and the
# extension are built in, so that the two agree on the glibc floor.
MANYLINUX_TAG=2026.10.26-1
CIBUILDWHEEL_VERSION=4.3.3

# CPython 3.10 to 3.15. The free-threaded builds (cp314t, cp315t), musllinux
# and 32-bit x86 are not built.
export CIBW_BUILD="cp310-* cp311-* cp312-* cp313-* cp314-* cp315-*"
export CIBW_SKIP="*musllinux* *i686"

case "$(uname -s)" in
    Linux)
        image="quay.io/pypa/manylinux_2_28_$(uname -m):${MANYLINUX_TAG}"
        sh .semaphore/build-ffi-library.sh "$image"
        export CIBW_MANYLINUX_X86_64_IMAGE="quay.io/pypa/manylinux_2_28_x86_64:${MANYLINUX_TAG}"
        export CIBW_MANYLINUX_AARCH64_IMAGE="quay.io/pypa/manylinux_2_28_aarch64:${MANYLINUX_TAG}"
        # Single quotes: cibuildwheel expands the variables in its container,
        # where the repository is /project. auditwheel finds the library
        # through the extension's rpath or LD_LIBRARY_PATH, which the image
        # already sets for its compiler toolset, so the directory is prepended.
        export CIBW_ENVIRONMENT='CFLAGS=-I$PWD/python/native/include CONFLUENT_KAFKA_LIB_DIR=$PWD/python/native/lib LDFLAGS=-L$PWD/python/native/lib LD_LIBRARY_PATH=$PWD/python/native/lib:$LD_LIBRARY_PATH'
        platform=linux
        ;;
    Darwin)
        : "${MACOSX_DEPLOYMENT_TARGET:?set MACOSX_DEPLOYMENT_TARGET to the oldest macOS version the wheels support}"
        sh .semaphore/build-ffi-library.sh
        # Single quotes: cibuildwheel expands the variables itself. The header
        # padding leaves delocate room to rewrite the library's install name.
        export CIBW_ENVIRONMENT='CFLAGS=-I$PWD/python/native/include CONFLUENT_KAFKA_LIB_DIR=$PWD/python/native/lib LDFLAGS="-L$PWD/python/native/lib -Wl,-headerpad_max_install_names" MACOSX_DEPLOYMENT_TARGET=$MACOSX_DEPLOYMENT_TARGET'
        platform=macos
        ;;
    *)
        echo "unsupported platform: $(uname -s)" >&2
        exit 1
        ;;
esac

n=1
until python -m pip install "cibuildwheel==${CIBUILDWHEEL_VERSION}"; do
    [ "$n" -lt 5 ] || { echo "could not install cibuildwheel ${CIBUILDWHEEL_VERSION}" >&2; exit 1; }
    n=$((n + 1))
    sleep 5
done

rm -rf wheelhouse
python -m cibuildwheel --platform "$platform" --output-dir wheelhouse python
ls -l wheelhouse
