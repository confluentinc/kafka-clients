#!/bin/bash
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
# Publishes a release of the Node.js client to npmjs.com from its release bundle, the zip that
# the tag build's "Release (JavaScript): record" job recorded (.semaphore/release-npm.sh). Run it
# by hand, from the unzipped bundle, logged in to npm (`npm login`) with an account that may
# publish to @confluentinc; see README.txt. It publishes the tarballs themselves, so npm uploads
# exactly the bytes the tag build tested: the three platform packages, then the main package,
# each with --access public --tag preview, asking for a one-time password before each.
#
# It refuses unless the tarballs match SHA256SUMS, npm is logged in, and the version is a
# pre-release: a stable version would need the `latest` dist-tag, which the earlier
# librdkafka-based client still holds (README.txt). A package already published with the same
# tarball is skipped, so an interrupted run can be repeated; one published with another tarball
# stops it.
#
# Usage: ./publish.sh
# Needs bash, node and npm (Node.js 20 or newer), and sha256sum or shasum.

set -euo pipefail

REGISTRY=https://registry.npmjs.org/
DIST_TAG=preview
MAIN=@confluentinc/kafka-javascript
PLATFORMS=(linux-x64-gnu linux-arm64-gnu darwin-arm64)

die() {
    echo "publish.sh: $*" >&2
    exit 1
}

cd "$(dirname "$0")"
[ -f SHA256SUMS ] || die "run it from the unzipped release bundle, which holds SHA256SUMS"
command -v node >/dev/null 2>&1 || die "node is not on PATH"
command -v npm >/dev/null 2>&1 || die "npm is not on PATH"

echo "== Checking the tarballs against SHA256SUMS =="
if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c SHA256SUMS || die "a tarball does not match SHA256SUMS; download the bundle again"
else
    shasum -a 256 -c SHA256SUMS || die "a tarball does not match SHA256SUMS; download the bundle again"
fi

# The version, from the main package's tarball: confluentinc-kafka-javascript-<version>.tgz.
version=""
listed=()
while read -r _sum file; do
    listed+=("$file")
    case "$file" in
        confluentinc-kafka-javascript-[0-9]*.tgz)
            version="${file#confluentinc-kafka-javascript-}"
            version="${version%.tgz}"
            ;;
    esac
done < SHA256SUMS
[ -n "$version" ] || die "SHA256SUMS names no main package tarball"
tarballs=()
names=()
for platform in "${PLATFORMS[@]}"; do
    tarballs+=("confluentinc-kafka-javascript-$platform-$version.tgz")
    names+=("$MAIN-$platform")
done
tarballs+=("confluentinc-kafka-javascript-$version.tgz")
names+=("$MAIN")
[ "$(printf '%s\n' "${listed[@]}" | LC_ALL=C sort)" = "$(printf '%s\n' "${tarballs[@]}" | LC_ALL=C sort)" ] \
    || die "SHA256SUMS lists ${listed[*]}, not ${tarballs[*]}"

case "$version" in
    *-*) ;;
    *) die "$version is not a pre-release. Publishing a stable version moves npm's latest dist-tag away from the earlier client (1.x); README.txt says what has to be agreed first" ;;
esac

echo "== Checking the npm login =="
user=$(npm whoami --registry "$REGISTRY") || die "npm is not logged in to $REGISTRY; run \`npm login\`"
echo "Logged in to $REGISTRY as $user"

# The sha512 integrity npm computes for a tarball.
integrity_of() {
    node -e 'const c = require("node:crypto"); const f = require("node:fs");
        process.stdout.write("sha512-" + c.createHash("sha512").update(f.readFileSync(process.argv[1])).digest("base64"));' "$1"
}

# The integrity the registry has for $1@$version, or nothing if it has no such version. Fails on
# any other answer than a missing package or version.
published_integrity() {
    local out err status
    err=$(mktemp)
    status=0
    out=$(npm view "$1@$version" dist.integrity --registry "$REGISTRY" 2>"$err") || status=$?
    if [ "$status" -ne 0 ]; then
        if grep -q E404 "$err"; then
            rm -f "$err"
            return 0
        fi
        cat "$err" >&2
        rm -f "$err"
        return 1
    fi
    rm -f "$err"
    printf '%s' "$out"
}

echo "== Comparing with $REGISTRY =="
pending=()
for i in "${!tarballs[@]}"; do
    local_integrity=$(integrity_of "${tarballs[$i]}")
    remote=$(published_integrity "${names[$i]}") || die "could not read ${names[$i]} from $REGISTRY"
    if [ -z "$remote" ]; then
        echo "${names[$i]}@$version: not published yet"
        pending+=("$i")
    elif [ "$remote" = "$local_integrity" ]; then
        echo "${names[$i]}@$version: already published with this tarball; skipping it"
    else
        die "${names[$i]}@$version is already published with another tarball ($remote, not $local_integrity); stop, and release a new version"
    fi
done
if [ "${#pending[@]}" -eq 0 ]; then
    echo "Every package of $version is already published."
else
    echo "== Dry run =="
    for i in "${pending[@]}"; do
        npm publish "${tarballs[$i]}" --dry-run --access public --tag "$DIST_TAG" --registry "$REGISTRY" \
            || die "the dry run of ${tarballs[$i]} failed"
    done

    echo "== Publishing =="
    for i in "${pending[@]}"; do
        otp=""
        while [ -z "$otp" ]; do
            read -r -p "One-time password for ${names[$i]}@$version: " otp < /dev/tty
        done
        npm publish "${tarballs[$i]}" --access public --tag "$DIST_TAG" --otp "$otp" --registry "$REGISTRY" \
            || die "npm publish of ${tarballs[$i]} failed; rerun ./publish.sh, which skips what is already published"
        echo "Published ${names[$i]}@$version"
    done
fi

echo "== Summary =="
for i in "${!tarballs[@]}"; do
    sum=$(grep -F "  ${tarballs[$i]}" SHA256SUMS | cut -d' ' -f1)
    echo "$sum  ${tarballs[$i]}  ${names[$i]}@$version"
done
echo "Published $MAIN $version under the $DIST_TAG dist-tag. Check it with:"
echo "  npm view $MAIN dist-tags"
echo "then run the javascript-post-install Semaphore task with JAVASCRIPT_VERSION=$version (README.txt)."
