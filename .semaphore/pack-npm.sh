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
# Packs the npm packages of the Node.js client: one platform package per platform,
# @confluentinc/kafka-javascript-<platform>, carrying the addon of that platform, and the main
# package, @confluentinc/kafka-javascript, carrying the compiled library (javascript/dist/),
# whose optionalDependencies name the platform packages at its exact version, so that npm
# installs the one of the user's platform. Then checks them with check-npm.py. Used by the
# "Release (JavaScript): pack" block of semaphore.yml and by `make -C javascript pack`. Must be
# POSIX sh, with node and npm on PATH, the devDependencies installed in javascript/ (`npm ci`),
# and python3.
#
# Usage, from the repository root:
#   .semaphore/pack-npm.sh <natives-dir> <out-dir> [<platform> ...]
#       <natives-dir> holds <platform>/confluent-kafka-javascript.node for each platform and
#       nothing else; the platforms default to linux-x64-gnu linux-arm64-gnu darwin-arm64. Each
#       addon must pass check-npm.py's native checks, platform floors included. The package
#       sources (javascript/) must be those of HEAD. Writes the tarballs `npm pack` names,
#       confluentinc-kafka-javascript-<version>.tgz and
#       confluentinc-kafka-javascript-<platform>-<version>.tgz, into <out-dir>, which must not
#       exist or be empty, and prints their SHA-256.
#   .semaphore/pack-npm.sh --dev <out-dir>
#       Packs this machine's own static build (javascript/build/Release/, from `make -C
#       javascript build-addon CONFLUENT_KAFKA_LINK=static`) as the one platform package of this
#       machine, without the platform floor checks: packages to test the packaging, never ones to
#       release. The main package still names all three platform packages.
#
# The version is "version" in javascript/package.json. The repository's package.json stays
# private; the published one is written by check-npm.py manifest.

set -eu

ALL_PLATFORMS="linux-x64-gnu linux-arm64-gnu darwin-arm64"
ADDON=confluent-kafka-javascript.node

die() {
    echo "pack-npm: $*" >&2
    exit 1
}

usage() {
    echo "Usage: $0 <natives-dir> <out-dir> [<platform> ...]" >&2
    echo "       $0 --dev <out-dir>" >&2
    exit 2
}

# The platform of this machine.
host_platform() {
    case "$(uname -s)-$(uname -m)" in
        Linux-x86_64) echo linux-x64-gnu ;;
        Linux-aarch64|Linux-arm64) echo linux-arm64-gnu ;;
        Darwin-arm64) echo darwin-arm64 ;;
        *) die "no platform package for $(uname -s) $(uname -m)" ;;
    esac
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

[ -f javascript/package.json ] && [ -f .semaphore/check-npm.py ] || die "run from the repository root"
command -v npm >/dev/null 2>&1 || die "npm is not on PATH"

stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT INT TERM

if [ "${1:-}" = "--dev" ]; then
    [ $# -eq 2 ] || usage
    out="$2"
    dev="--dev"
    platforms=$(host_platform)
    [ -f "javascript/build/Release/$ADDON" ] \
        || die "javascript/build/Release/$ADDON does not exist; run \`make -C javascript build-addon CONFLUENT_KAFKA_LINK=static\` first"
    mkdir -p "$stage/natives/$platforms"
    cp "javascript/build/Release/$ADDON" "$stage/natives/$platforms/"
else
    [ $# -ge 2 ] || usage
    natives="$1"
    out="$2"
    shift 2
    dev=""
    platforms="${*:-$ALL_PLATFORMS}"
    [ -d "$natives" ] || die "$natives is not a directory"
    # Each platform's directory holds its addon and nothing else, and there is no other directory,
    # so nothing the native blocks did not build can reach a package.
    for entry in "$natives"/* "$natives"/.[!.]*; do
        [ -e "$entry" ] || continue
        name=$(basename "$entry")
        case " $platforms " in
            *" $name "*) ;;
            *) die "$natives holds $name, which is not one of the platforms to pack: $platforms" ;;
        esac
    done
    for platform in $platforms; do
        [ -f "$natives/$platform/$ADDON" ] || die "$natives/$platform/$ADDON does not exist"
        listing=$(ls -A "$natives/$platform")
        [ "$listing" = "$ADDON" ] || die "$natives/$platform must hold only $ADDON, but holds: $(printf "%s" "$listing" | tr "\n" " ")"
        mkdir -p "$stage/natives/$platform"
        cp "$natives/$platform/$ADDON" "$stage/natives/$platform/"
    done
fi

for platform in $platforms; do
    python3 .semaphore/check-npm.py native "$platform" "$stage/natives/$platform/$ADDON" $dev
done

if [ -e "$out" ] && [ -n "$(ls -A "$out")" ]; then
    die "$out is not empty"
fi
mkdir -p "$out"
out=$(cd "$out" && pwd -P)

version=$(python3 .semaphore/check-npm.py version)
commit=$(git rev-parse HEAD)
# A release is packed only from the files of the commit it is tagged at: no uncommitted change
# and no untracked file among the sources the pack reads. The build outputs (build/, dist/,
# node_modules/) are ignored, so they do not count.
if [ -z "$dev" ]; then
    changes=$(git status --porcelain -- javascript LICENSE)
    [ -z "$changes" ] || die "the package sources differ from $commit: $(printf "%s" "$changes" | tr "\n" " ")"
fi
echo "Packing @confluentinc/kafka-javascript $version at $commit with the addons of: $platforms"

# The compiled library, from a clean dist/ (javascript/Makefile's build-ts).
make -C javascript build-ts

# The main package: what its package.json's "files" lists, the LICENSE, and the package.json to
# publish.
mkdir "$stage/main"
cp -R javascript/dist "$stage/main/dist"
cp javascript/README.md javascript/CHANGELOG.md LICENSE "$stage/main/"
python3 .semaphore/check-npm.py manifest main --version "$version" > "$stage/main/package.json"

# A platform package per platform: its addon, a README pointing at the main package, the LICENSE
# and its package.json.
for platform in $platforms; do
    mkdir "$stage/$platform"
    cp "$stage/natives/$platform/$ADDON" LICENSE "$stage/$platform/"
    cat > "$stage/$platform/README.md" <<EOF
# @confluentinc/kafka-javascript-$platform

The $platform native binary of @confluentinc/kafka-javascript. Install that package instead: npm
installs this one with it, on $platform, as one of its optional dependencies.
EOF
    python3 .semaphore/check-npm.py manifest "$platform" --version "$version" > "$stage/$platform/package.json"
done

# --ignore-scripts: the published package.json has no scripts, and none may run while packing.
for package in $platforms main; do
    (cd "$stage/$package" && npm pack --ignore-scripts --pack-destination "$out" >/dev/null) \
        || die "npm pack of the $package package failed"
done

expected=$(for platform in $platforms; do echo "confluentinc-kafka-javascript-$platform-$version.tgz"; done
    echo "confluentinc-kafka-javascript-$version.tgz")
expected=$(printf '%s\n' "$expected" | LC_ALL=C sort)
listing=$(ls -A "$out" | LC_ALL=C sort)
[ "$listing" = "$expected" ] || die "the pack wrote $(printf "%s" "$listing" | tr "\n" " "), not $(printf "%s" "$expected" | tr "\n" " ")"

python3 .semaphore/check-npm.py package "$out" --version "$version" --platforms "$platforms" $dev
for file in $listing; do
    echo "$(sha256_of "$out/$file")  $file"
done
