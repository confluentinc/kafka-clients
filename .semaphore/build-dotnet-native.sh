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
# Builds the native library of this machine's RID for the Confluent.Kafka NuGet package, for
# the "Release (.NET): native library" blocks of semaphore.yml. It is the library the Python
# wheels carry, built the same way (.semaphore/build-ffi-library.sh): on Linux in the
# manylinux_2_28 image that build-wheels.sh uses, so that it needs glibc 2.28 at most; on macOS
# for macOS 11.0. Must be POSIX sh.
#
# Usage, from the repository root (on macOS with MACOSX_DEPLOYMENT_TARGET=11.0):
#   .semaphore/build-dotnet-native.sh <out-dir>
# Writes <out-dir>/<rid>/<library> and <out-dir>/<rid>/confluent_kafka_bindings.h, the header of
# that library's C ABI, which the package's unit tests compare the assembly with. <out-dir> must
# not exist or be empty. On Linux the library is built from the committed HEAD.

set -eu

die() {
    echo "build-dotnet-native: $*" >&2
    exit 1
}

[ $# -eq 1 ] || { echo "Usage: $0 <out-dir>" >&2; exit 2; }
out="$1"
[ -f .semaphore/build-ffi-library.sh ] || die "run from the repository root"
if [ -e "$out" ] && [ -n "$(ls -A "$out")" ]; then
    die "$out is not empty"
fi

case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) rid=linux-x64 ;;
    Linux-aarch64) rid=linux-arm64 ;;
    Darwin-x86_64) rid=osx-x64 ;;
    Darwin-arm64) rid=osx-arm64 ;;
    *) die "the package has no RID for $(uname -s) $(uname -m)" ;;
esac

case "$rid" in
    linux-*)
        lib=libconfluent_kafka.so
        tag=$(sed -n 's/^MANYLINUX_TAG=//p' .semaphore/build-wheels.sh)
        [ -n "$tag" ] || die "could not read MANYLINUX_TAG from .semaphore/build-wheels.sh"
        sh .semaphore/build-ffi-library.sh "quay.io/pypa/manylinux_2_28_$(uname -m):$tag"
        ;;
    osx-*)
        lib=libconfluent_kafka.dylib
        [ "${MACOSX_DEPLOYMENT_TARGET:-}" = "11.0" ] \
            || die "set MACOSX_DEPLOYMENT_TARGET=11.0, the oldest macOS the package supports"
        sh .semaphore/build-ffi-library.sh
        ;;
esac

mkdir -p "$out/$rid"
cp "python/native/lib/$lib" python/native/include/confluent_kafka_bindings.h "$out/$rid/"

if [ "${rid%%-*}" = osx ]; then
    # build-ffi-library.sh rewrites the install name after the linker signed the library, which
    # can invalidate the linker's ad hoc signature, and macOS on Apple silicon loads only signed
    # code (the x86_64 linker does not sign by default). Sign it ad hoc again, as the linker does,
    # when the signature is missing or invalid.
    if ! codesign --verify --strict "$out/$rid/$lib" 2>/dev/null; then
        codesign --force --sign - "$out/$rid/$lib"
        codesign --verify --strict "$out/$rid/$lib" || die "the ad hoc signature of $lib is not valid"
    fi
fi
python3 .semaphore/check-nupkg.py native "$rid" "$out/$rid/$lib"
ls -l "$out/$rid"
