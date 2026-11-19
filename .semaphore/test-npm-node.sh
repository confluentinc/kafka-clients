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
# The package tests of test-npm.sh on another Node.js version, and on Linux in another
# distribution, before the packages are recorded: the tag build's package verification runs it
# on the `engines` floor (22.14.0) and on Node.js 24 and 26, and in almalinux:8, whose glibc 2.28
# is the packages' floor, as javascript-post-install.yml does only after publishing. It runs
# test-npm.sh with TEST_NPM_DEV_CHECKS=0: the install check, the CommonJS and ES module smoke
# programs and the missing-platform check, none of which needs the devDependencies. Must be
# POSIX sh.
#
# Usage, from the repository root:
#   .semaphore/test-npm-node.sh <tarball-dir> <platform> <node-version> <docker-image>   (Linux)
#   .semaphore/test-npm-node.sh <tarball-dir> <platform> <node-version> --here           (macOS)
#       <node-version> is an exact version (22.14.0) or a major one (24: its latest release).
#       On Linux the job runs in a container of <docker-image>, with the official Node.js
#       tarball of that version, checked against nodejs.org's SHASUMS256.txt; with --here, with
#       the Node.js on PATH (`sem-version node <node-version>`), which must be that version.
# Usage (internal): .semaphore/test-npm-node.sh --in-docker <platform> <node-version>

set -eu

die() {
    echo "test-npm-node: $*" >&2
    exit 1
}

retry() {
    _n=1
    while [ "$_n" -le 5 ]; do
        if "$@"; then return 0; fi
        [ "$_n" -eq 5 ] && break
        echo "retry: attempt $_n/5 of '$*' failed; sleeping 10s" >&2
        _n=$((_n + 1))
        sleep 10
    done
    echo "retry: '$*' failed after 5 attempts" >&2
    return 1
}

check_node_version() {
    printf '%s' "$1" | grep -Eqx '[1-9][0-9]*(\.[0-9]+\.[0-9]+)?' || die "'$1' is not a Node.js version (22.14.0) or major (24)"
}

# The Node.js on PATH is <node-version>: exactly, or of that major.
node_is() {
    have=$(node --version)
    case "$1" in
        *.*) [ "$have" = "v$1" ] ;;
        *) case "$have" in "v$1".*) true ;; *) false ;; esac ;;
    esac || die "node is $have, not $1"
}

if [ "${1:-}" = "--in-docker" ]; then
    platform="$2"
    version="$3"
    # test-npm.sh needs python3 (3.9 or later: check-npm.py) and the tools to unpack Node.js, and
    # Node.js 26 needs libatomic, which the smallest images lack.
    if command -v apt-get >/dev/null 2>&1; then
        export DEBIAN_FRONTEND=noninteractive
        retry apt-get update -qq
        retry apt-get install -y -qq --no-install-recommends ca-certificates curl tar gzip libatomic1 python3 >/dev/null
    elif command -v dnf >/dev/null 2>&1; then
        retry dnf install -y -q tar gzip libatomic >/dev/null
        command -v curl >/dev/null 2>&1 || retry dnf install -y -q curl >/dev/null
        # EL 8's python3 is 3.6.
        if dnf list -q python39 >/dev/null 2>&1; then retry dnf install -y -q python39 >/dev/null; else retry dnf install -y -q python3 >/dev/null; fi
    else
        die "the image has neither apt-get nor dnf"
    fi
    case "$(uname -m)" in
        x86_64) arch=x64 ;;
        aarch64) arch=arm64 ;;
        *) die "no Node.js tarball for $(uname -m)" ;;
    esac
    case "$version" in
        *.*) base="https://nodejs.org/dist/v$version" ; pattern="node-v$version-linux-$arch\.tar\.gz" ;;
        *) base="https://nodejs.org/dist/latest-v$version.x" ; pattern="node-v$version\.[0-9]+\.[0-9]+-linux-$arch\.tar\.gz" ;;
    esac
    retry curl --proto '=https' --tlsv1.2 -sSfL -o /tmp/SHASUMS256.txt "$base/SHASUMS256.txt"
    line=$(grep -E "  $pattern\$" /tmp/SHASUMS256.txt) || die "$base/SHASUMS256.txt names no linux-$arch tarball"
    tarball=${line##* }
    retry curl --proto '=https' --tlsv1.2 -sSfL -o "/tmp/$tarball" "$base/$tarball"
    (cd /tmp && printf '%s\n' "$line" | sha256sum -c -) || die "$tarball does not match $base/SHASUMS256.txt"
    mkdir -p /opt/node
    tar -xzf "/tmp/$tarball" -C /opt/node --strip-components=1
    export PATH="/opt/node/bin:$PATH"
    node_is "$version"
    echo "Node.js $(node --version) in $(. /etc/os-release && echo "$PRETTY_NAME"), glibc $(ldd --version | head -n 1 | awk '{ print $NF }')"
    cd /repo
    TEST_NPM_DEV_CHECKS=0 sh .semaphore/test-npm.sh /packages "$platform"
    exit 0
fi

[ -f .semaphore/test-npm.sh ] || die "run from the repository root"
[ $# -eq 4 ] || { echo "Usage: $0 <tarball-dir> <platform> <node-version> <docker-image> | --here" >&2; exit 2; }
dir="$1"
platform="$2"
version="$3"
check_node_version "$version"
[ -d "$dir" ] || die "$dir is not a directory"
case "$4" in
    --here)
        command -v node >/dev/null 2>&1 || die "node is not on PATH; run \`sem-version node $version\`"
        node_is "$version"
        TEST_NPM_DEV_CHECKS=0 sh .semaphore/test-npm.sh "$dir" "$platform"
        ;;
    *)
        [ "$(uname -s)" = Linux ] || die "a Docker image is for Linux; on macOS pass --here"
        dir=$(cd "$dir" && pwd -P)
        retry docker pull -q "$4"
        docker run --rm -v "$PWD":/repo:ro -v "$dir":/packages:ro "$4" sh /repo/.semaphore/test-npm-node.sh --in-docker "$platform" "$version"
        ;;
esac
