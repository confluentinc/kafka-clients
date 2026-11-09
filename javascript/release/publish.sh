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
# stops it. Once every package is there it reads the dist-tags back, whether it published or
# skipped: each package's `preview` must be the version, and the main package's `latest` (and
# `rc` / `alpha`, when present) a version of the earlier client, below 4.0.0, so a publish that
# moved one of them by hand is caught.
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

# For a scoped package npm uses an `@confluentinc:registry` setting before `registry`, so a
# ~/.npmrc that maps the scope elsewhere would take every lookup and upload there: pin both.
REGISTRY_FLAGS=(--registry "$REGISTRY" "--@confluentinc:registry=$REGISTRY")

echo "== Checking the npm login =="
user=$(npm whoami "${REGISTRY_FLAGS[@]}") || die "npm is not logged in to $REGISTRY; run \`npm login\`"
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
    out=$(npm view "$1@$version" dist.integrity "${REGISTRY_FLAGS[@]}" 2>"$err") || status=$?
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
        npm publish "${tarballs[$i]}" --dry-run --access public --tag "$DIST_TAG" "${REGISTRY_FLAGS[@]}" \
            || die "the dry run of ${tarballs[$i]} failed"
    done

    echo "== Publishing =="
    for i in "${pending[@]}"; do
        otp=""
        while [ -z "$otp" ]; do
            read -r -p "One-time password for ${names[$i]}@$version: " otp < /dev/tty \
                || die "cannot read a one-time password: run ./publish.sh in a terminal"
        done
        npm publish "${tarballs[$i]}" --access public --tag "$DIST_TAG" --otp "$otp" "${REGISTRY_FLAGS[@]}" \
            || die "npm publish of ${tarballs[$i]} failed; rerun ./publish.sh, which skips what is already published"
        echo "Published ${names[$i]}@$version"
    done
fi

echo "== Reading the registry back =="
# The registry can take a little while to serve a new version, so each package gets a few tries.
for i in "${!tarballs[@]}"; do
    local_integrity=$(integrity_of "${tarballs[$i]}")
    remote=""
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        remote=$(published_integrity "${names[$i]}") || remote=""
        [ "$remote" = "$local_integrity" ] && break
        sleep 6
    done
    [ "$remote" = "$local_integrity" ] \
        || die "$REGISTRY serves ${names[$i]}@$version with the integrity '${remote:-none}', not $local_integrity, the tarball's"
    echo "${names[$i]}@$version: $REGISTRY serves this tarball ($local_integrity)"
done

# What is wrong with the dist-tags of $1 once $version is published, one problem per line, or
# nothing: `preview` must be $version, and for the main package `latest`, `rc` and `alpha`, when
# present, a version below 4 (the earlier client's; the registry sets a new platform package's
# `latest` on its first publish, which is harmless). Fails if the registry cannot be read.
dist_tag_problems() {
    local tags
    tags=$(npm view "$1" dist-tags --json "${REGISTRY_FLAGS[@]}") || return 1
    # shellcheck disable=SC2016 # JavaScript, not shell: nothing in it is for the shell to expand.
    node -e '
        const [json, name, version, main, distTag] = process.argv.slice(1);
        let tags;
        try {
            tags = JSON.parse(json);
        } catch {
            tags = null;
        }
        if (tags === null || typeof tags !== "object") {
            console.log(`npm printed no dist-tags for ${name}: ${json}`);
            process.exit(0);
        }
        const problems = [];
        if (tags[distTag] !== version) {
            problems.push(`the ${distTag} dist-tag of ${name} is ${tags[distTag] ?? "not set"}, not ${version}`);
        }
        if (name === main) {
            for (const tag of ["latest", "rc", "alpha"]) {
                if (!(tag in tags)) continue;
                const major = /^(0|[1-9][0-9]*)\./.exec(String(tags[tag]));
                if (major === null || Number(major[1]) >= 4) {
                    problems.push(`the ${tag} dist-tag of ${name} is ${tags[tag]}, but it must stay on the earlier ` +
                        `client, below 4.0.0: point it back with \`npm dist-tag add ${name}@<its version> ${tag}\``);
                }
            }
        }
        if (problems.length > 0) console.log(problems.join("\n"));
    ' "$tags" "$1" "$version" "$MAIN" "$DIST_TAG"
}

echo "== Checking the dist-tags =="
# A tag set by a publish can take a little while to be served too.
for i in "${!tarballs[@]}"; do
    problems=""
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        problems=$(dist_tag_problems "${names[$i]}") || problems="could not read the dist-tags of ${names[$i]} from $REGISTRY"
        [ -z "$problems" ] && break
        sleep 6
    done
    [ -z "$problems" ] || die "$problems"
    echo "${names[$i]}: $DIST_TAG is $version"
done

echo "== Summary =="
for i in "${!tarballs[@]}"; do
    sum=$(grep -F "  ${tarballs[$i]}" SHA256SUMS | cut -d' ' -f1)
    echo "$sum  ${tarballs[$i]}  ${names[$i]}@$version"
done
echo "$MAIN $version is published under the $DIST_TAG dist-tag, and latest is on the earlier client. Check it with:"
echo "  npm view $MAIN dist-tags"
echo "then run the javascript-post-install Semaphore task with JAVASCRIPT_VERSION=$version (README.txt)."
