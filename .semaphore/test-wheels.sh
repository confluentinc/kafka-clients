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
# Tests the wheels of the Python binding with every supported Python version:
# on Linux in containers of several distributions, on macOS on the runner. For
# each version it installs the wheel into a new virtual environment, then runs
# an import check and a compression and TLS smoke test from a directory without
# the sources, so that only the installed wheel can be imported. The binding's
# unit tests also run, with every Python version, on macOS and in the first
# Linux distribution, whose glibc is the oldest that the manylinux_2_28 wheels
# support; the other distributions run the import and smoke tests only. uv
# provides each Python version as a standalone build, so the versions tested do
# not depend on what the host has installed. Must be POSIX sh.
#
# The unit tests run from the repository, because some of them read repository
# files (the kafka submodule included), but against the installed wheel:
# pytest's `--import-mode=append` puts python/ after site-packages on sys.path,
# so `confluent_kafka` and `_confluentkafka` are imported from the wheel.
#
# Usage, from the repository root:
#   .semaphore/test-wheels.sh <wheelhouse-dir> [<checksum-file>]
#       Tests the wheels in <wheelhouse-dir>, a path relative to the repository
#       root that must hold exactly one wheel for each supported Python version
#       and nothing else. With <checksum-file>, which must be outside
#       <wheelhouse-dir>, writes `<sha256>  <wheel>` for each wheel to it once
#       every test has passed.
#   .semaphore/test-wheels.sh --index <simple-index-url> <version>
#       Installs <version> of the package from the index instead, and runs the
#       import and smoke tests only.
# Override the distribution list (Linux) with DISTRO_IMAGES="img1 img2 ...".

set -eu

UV_VERSION=0.12.17
PY_VERSIONS="3.10 3.11 3.12 3.13 3.14"

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

usage() {
    echo "Usage: $0 <wheelhouse-dir> [<checksum-file>]" >&2
    echo "       $0 --index <simple-index-url> <version>" >&2
    exit 2
}

# Installs the pinned uv into $1.
install_uv() {
    retry curl --proto '=https' --tlsv1.2 -LsSf "https://astral.sh/uv/${UV_VERSION}/install.sh" -o /tmp/uv-install.sh
    retry env UV_INSTALL_DIR="$1" UV_NO_MODIFY_PATH=1 sh /tmp/uv-install.sh
}

# Fails unless the wheelhouse holds exactly one wheel for each version in
# PY_VERSIONS, and nothing else.
check_wheelhouse() {
    total=0
    for f in "$WHEELHOUSE"/*; do
        [ -e "$f" ] && total=$((total + 1))
    done
    for py in $PY_VERSIONS; do
        tag="cp$(printf '%s' "$py" | tr -d .)"
        count=0
        for f in "$WHEELHOUSE"/*-"$tag"-"$tag"-*.whl; do
            [ -e "$f" ] && count=$((count + 1))
        done
        if [ "$count" -ne 1 ]; then
            echo "expected one $tag wheel in $WHEELHOUSE, found $count" >&2
            return 1
        fi
    done
    expected=$(set -- $PY_VERSIONS; echo $#)
    if [ "$total" -ne "$expected" ]; then
        echo "$WHEELHOUSE must hold only the $expected wheels, but it holds $total files:" >&2
        ls -l "$WHEELHOUSE" >&2
        return 1
    fi
}

# Prints `<sha256>  <wheel>` for each wheel in the wheelhouse.
wheel_checksums() {
    if command -v sha256sum >/dev/null 2>&1; then
        (cd "$WHEELHOUSE" && sha256sum -- *.whl)
    else
        (cd "$WHEELHOUSE" && shasum -a 256 -- *.whl)
    fi
}

# Runs the tests with every version in PY_VERSIONS. $1 is the repository root;
# $2 is 1 to run the unit tests too.
run_matrix() {
    base="$1"
    full="$2"
    # The unit tests run from the repository; no .pyc or pytest cache is
    # written there (on Linux it is mounted from the host and this runs as
    # root).
    export PYTHONDONTWRITEBYTECODE=1
    # Only uv's standalone builds, never a Python the host happens to have.
    export UV_PYTHON_PREFERENCE=only-managed
    package=$(sed -n 's/^name *= *"\(.*\)"/\1/p' "$base/python/pyproject.toml" | head -1)
    [ -n "$package" ] || { echo "could not read the package name from python/pyproject.toml" >&2; return 1; }
    retry uv python install $PY_VERSIONS

    for py in $PY_VERSIONS; do
        echo "== Python $py =="
        venv="/tmp/test-wheels-venv-$py"
        rm -rf "$venv"
        uv venv --python "$py" "$venv"
        vpy="$venv/bin/python"
        if [ -n "${INDEX_URL:-}" ]; then
            retry uv pip install --python "$vpy" --no-cache --only-binary :all: \
                --index-url "$INDEX_URL" "$package==$PACKAGE_VERSION"
            "$vpy" -c "import importlib.metadata as m, sys; v = m.version(sys.argv[1]); sys.exit(None if v == sys.argv[2] else f'installed {v}, not {sys.argv[2]}')" \
                "$package" "$PACKAGE_VERSION"
        else
            uv pip install --python "$vpy" --no-index --find-links "$base/$WHEELHOUSE" "$package"
        fi
        ( cd /tmp && "$vpy" -c "import _confluentkafka, confluent_kafka; print('import OK: Python $py:', _confluentkafka.__file__, confluent_kafka.__file__)" )
        if [ "$full" = 1 ]; then
            retry uv pip install --python "$vpy" pytest pytest-asyncio
            ( cd "$base" && "$vpy" -m pytest python/test/unit -q -p no:cacheprovider --import-mode=append )
        fi
        ( cd /tmp && "$vpy" -c "from confluent_kafka.producer import KafkaProducer as P; [P(configs={'bootstrap.servers':'localhost:9092','compression.type':c}).close() or print('OK: compression '+c) for c in ('gzip','snappy','lz4','zstd')]; P(configs={'bootstrap.servers':'localhost:9092','security.protocol':'SSL'}).close(); print('OK: security.protocol=SSL')" )
    done
}

if [ "${IN_DOCKER:-0}" = "1" ]; then
    set -x
    export PATH="$UV_HOME/bin:$PATH"
    run_matrix /io "${FULL_SUITE:-0}"
    exit 0
fi

case "${1:-}" in
    --index)
        [ $# -eq 3 ] || usage
        INDEX_URL="$2"
        PACKAGE_VERSION="$3"
        WHEELHOUSE=""
        CHECKSUM_FILE=""
        full=0
        ;;
    ""|-*)
        usage
        ;;
    *)
        [ $# -le 2 ] || usage
        WHEELHOUSE="$1"
        CHECKSUM_FILE="${2:-}"
        INDEX_URL=""
        PACKAGE_VERSION=""
        full=1
        case "$WHEELHOUSE" in
            /*) echo "<wheelhouse-dir> must be relative to the repository root" >&2; exit 2 ;;
        esac
        check_wheelhouse
        # Recorded before the tests, and written only if they all pass.
        checksums=$(wheel_checksums)
        ;;
esac
export WHEELHOUSE INDEX_URL PACKAGE_VERSION

if [ "$(uname -s)" = "Darwin" ]; then
    set -x
    install_uv /tmp/uv/bin
    export PATH="/tmp/uv/bin:$PATH"
    run_matrix "$PWD" "$full"
else
    : "${DISTRO_IMAGES:=almalinux:8 almalinux:9 ubuntu:22.04 debian:12 ubuntu:24.04}"

    # uv and the Python builds are installed once, on the host, into a
    # directory that every container mounts at the same path, along with uv's
    # cache.
    export UV_HOME=/tmp/uv
    export UV_CACHE_DIR="$UV_HOME/cache"
    export UV_PYTHON_INSTALL_DIR="$UV_HOME/pythons"
    install_uv "$UV_HOME/bin"
    export PATH="$UV_HOME/bin:$PATH"
    retry uv python install $PY_VERSIONS

    for img in $DISTRO_IMAGES; do
        echo "== testing on $img =="
        retry docker pull "$img"
        docker run --rm \
            -e IN_DOCKER=1 -e FULL_SUITE="$full" \
            -e WHEELHOUSE -e INDEX_URL -e PACKAGE_VERSION \
            -e UV_HOME -e UV_CACHE_DIR -e UV_PYTHON_INSTALL_DIR \
            -v "$UV_HOME":"$UV_HOME" \
            -v "$PWD":/io -w /io "$img" \
            sh /io/.semaphore/test-wheels.sh
        # The unit tests run in the first distribution only.
        full=0
    done
fi

if [ -n "$CHECKSUM_FILE" ]; then
    printf '%s\n' "$checksums" > "$CHECKSUM_FILE"
    echo "Tested:"
    cat "$CHECKSUM_FILE"
fi
