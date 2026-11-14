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
# Installs a published version of @confluentinc/kafka-javascript from npmjs.com, as a user does,
# into an empty project, and runs the smoke program (javascript/tools/package-smoke) in it: the
# install jobs of javascript-post-install.yml. On Linux it runs in a container of the given
# image, with the official Node.js tarball of the given major version, whose SHA-256 is checked
# against nodejs.org's SHASUMS256.txt; on macOS with the Node.js on PATH (`sem-version node`).
# Must be POSIX sh. Nothing here holds a token.
#
# Usage, from the repository root:
#   .semaphore/test-published-npm.sh <version> <docker-image> <node-major>    (Linux)
#   .semaphore/test-published-npm.sh <version> --here                         (macOS)
# Usage (internal): .semaphore/test-published-npm.sh --in-docker <version> <node-major>

set -eu

die() {
    echo "test-published-npm: $*" >&2
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

host_platform() {
    case "$(uname -s)-$(uname -m)" in
        Linux-x86_64) echo linux-x64-gnu ;;
        Darwin-arm64) echo darwin-arm64 ;;
        *) die "no platform package for $(uname -s) $(uname -m)" ;;
    esac
}

check_version() {
    printf '%s' "$1" | grep -Eqx '(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(alpha|beta|dev|rc)\.(0|[1-9][0-9]*))?' \
        || die "'$1' is not a release version (X.Y.Z, optionally followed by -alpha.N, -beta.N, -dev.N or -rc.N)"
}

# Installs the package into an empty project and runs the smoke program there. $1: the version;
# $2: the smoke program.
install_and_smoke() {
    platform=$(host_platform)
    work=$(mktemp -d /tmp/test-published-npm.XXXXXX)
    work=$(cd "$work" && pwd -P)
    mkdir "$work/project"
    # An empty cache and no user or global configuration; the public registry, whatever the
    # machine sets. npm reads npm_config_* case-insensitively, the last variant winning, so drop
    # every inherited one and pass the settings on the command line too (see test-npm.sh).
    for name in $(env | sed -n 's/^\([Nn][Pp][Mm]_[Cc][Oo][Nn][Ff][Ii][Gg]_[A-Za-z0-9_]*\)=.*/\1/p'); do
        unset "$name"
    done
    export npm_config_cache="$work/cache" npm_config_userconfig="$work/npmrc" npm_config_globalconfig="$work/npmrc-global"
    export npm_config_registry=https://registry.npmjs.org/ npm_config_update_notifier=false
    : > "$work/npmrc"
    : > "$work/npmrc-global"
    echo "Node.js $(node --version), npm $(npm --version), on $platform"
    (cd "$work/project" && npm init -y >/dev/null \
        && retry npm install --ignore-scripts --no-audit --no-fund \
            --registry=https://registry.npmjs.org/ --@confluentinc:registry=https://registry.npmjs.org/ \
            --cache="$work/cache" --userconfig="$work/npmrc" --globalconfig="$work/npmrc-global" \
            "@confluentinc/kafka-javascript@$1") \
        || die "npm install @confluentinc/kafka-javascript@$1 failed"
    node "$2" --platform "$platform" --version "$1" --project "$work/project"
    rm -rf "$work"
}

if [ "${1:-}" = "--in-docker" ]; then
    version="$2"
    major="$3"
    # The tools to fetch and unpack Node.js, which the smallest images lack.
    if command -v apt-get >/dev/null 2>&1; then
        export DEBIAN_FRONTEND=noninteractive
        retry apt-get update -qq
        retry apt-get install -y -qq --no-install-recommends ca-certificates curl tar gzip >/dev/null
    elif command -v dnf >/dev/null 2>&1; then
        retry dnf install -y -q tar gzip >/dev/null
        command -v curl >/dev/null 2>&1 || retry dnf install -y -q curl >/dev/null
    else
        die "the image has neither apt-get nor dnf"
    fi
    case "$(uname -m)" in
        x86_64) arch=x64 ;;
        aarch64) arch=arm64 ;;
        *) die "no Node.js tarball for $(uname -m)" ;;
    esac
    # The latest release of the major version, checked against nodejs.org's checksums, read over
    # https.
    base="https://nodejs.org/dist/latest-v$major.x"
    retry curl --proto '=https' --tlsv1.2 -sSfL -o /tmp/SHASUMS256.txt "$base/SHASUMS256.txt"
    line=$(grep -E "  node-v$major\.[0-9]+\.[0-9]+-linux-$arch\.tar\.gz\$" /tmp/SHASUMS256.txt) \
        || die "$base/SHASUMS256.txt names no linux-$arch tarball"
    tarball=${line##* }
    retry curl --proto '=https' --tlsv1.2 -sSfL -o "/tmp/$tarball" "$base/$tarball"
    (cd /tmp && printf '%s\n' "$line" | sha256sum -c -) || die "$tarball does not match $base/SHASUMS256.txt"
    mkdir -p /opt/node
    tar -xzf "/tmp/$tarball" -C /opt/node --strip-components=1
    export PATH="/opt/node/bin:$PATH"
    install_and_smoke "$version" /repo/javascript/tools/package-smoke/package-smoke.js
    exit 0
fi

[ -f javascript/tools/package-smoke/package-smoke.js ] || die "run from the repository root"
case "$#-${2:-}" in
    2---here)
        check_version "$1"
        command -v node >/dev/null 2>&1 || die "node is not on PATH; run \`sem-version node <major>\`"
        install_and_smoke "$1" "$PWD/javascript/tools/package-smoke/package-smoke.js"
        ;;
    3-*)
        check_version "$1"
        printf '%s' "$3" | grep -Eqx '[1-9][0-9]*' || die "'$3' is not a Node.js major version"
        [ "$(uname -s)" = Linux ] || die "a Docker image is for Linux; on macOS pass --here"
        retry docker pull -q "$2"
        docker run --rm -v "$PWD":/repo:ro "$2" sh /repo/.semaphore/test-published-npm.sh --in-docker "$1" "$3"
        ;;
    *)
        echo "Usage: $0 <version> <docker-image> <node-major>" >&2
        echo "       $0 <version> --here" >&2
        exit 2
        ;;
esac
