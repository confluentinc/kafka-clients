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
# Publishes a release of the Node.js client to npmjs.com, for the "Release (JavaScript): publish to
# npm" block of publish-npm.yml: the tarballs the tag build's "Release (JavaScript): record" job
# recorded (.semaphore/release-npm.sh), so npm uploads exactly the bytes the tag build tested. The
# platform packages go first, then the main package, so that its optional dependencies exist when
# it appears.
#
# Usage, from the repository root:
#   .semaphore/publish-npm.sh [--check] <dir>
#       <dir> holds the recorded release, npm/release/ of the tag build's workflow: SHA256SUMS and
#       the tarballs it names, the main package's and one per platform package, and nothing else.
#       --check makes the first four checks below, reading the registry anonymously, and stops
#       there: publish-npm.yml runs it before it loads the token from Vault, so every refusal that
#       needs no token comes before the token exists, as in the crates.io block.
#
# It refuses unless all of the following hold, and checks the first four, anonymously, before it
# reads the token:
#   - SEMAPHORE_GIT_TAG_NAME is javascript/v<version>, the version of the main tarball, 4.0.0 or
#     higher (the npm name's versions below 4 are the earlier, librdkafka-based client's);
#   - the tarballs match SHA256SUMS, which names exactly the expected ones;
#   - the dist-tag is the one `check-npm.py dist-tag` gives the version: `beta` for a
#     pre-release, which it never puts on `latest`, `latest` for a release (`legacy` is the 1.x
#     line's, published from the earlier client's repository, never from here);
#   - a package already on npmjs.com at the version has this tarball (it is skipped, so a rerun
#     completes an interrupted publish); npm never takes a version twice;
#   - NPM_TOKEN is set: a granular access token of the npm account that publishes to
#     @confluentinc, with read and write access to the client's packages only and two-factor
#     authentication bypassed for publishing, which publish-npm.yml loads from Vault
#     (npm/kafka-javascript), as the crate's and the wheels' tokens are loaded.
# There is no --provenance: the packages are published from Semaphore, which npm cannot attest.
# Once every package is published it reads them back: the registry must serve each tarball, the
# dist-tag must point at the version, and for a pre-release the main package's `latest` (and `rc`
# / `alpha`, when present) must stay on the earlier client, a version below 4.0.0.
#
# Needs bash, node, npm and python3, and sha256sum or shasum.

set -euo pipefail

REGISTRY=https://registry.npmjs.org/
MAIN=@confluentinc/kafka-javascript
PLATFORMS=(linux-x64-gnu darwin-arm64)
MIN_MAJOR=4

die() {
    echo "publish-npm: $*" >&2
    exit 1
}

check_only=false
if [ "${1:-}" = --check ]; then
    check_only=true
    shift
fi
[ $# -eq 1 ] || { echo "Usage: $0 [--check] <dir>" >&2; exit 2; }
[ -f .semaphore/check-npm.py ] || die "run from the repository root"
check_npm="$(pwd)/.semaphore/check-npm.py"
cd "$1" || die "$1 is not a directory"
command -v node >/dev/null 2>&1 || die "node is not on PATH"
command -v npm >/dev/null 2>&1 || die "npm is not on PATH"

release_tag_pattern='^javascript/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(alpha|beta|dev|rc)\.(0|[1-9][0-9]*))?$'
[[ "${SEMAPHORE_GIT_TAG_NAME:-}" =~ ${release_tag_pattern} ]] \
    || die "'${SEMAPHORE_GIT_TAG_NAME:-}' is not a JavaScript release tag (javascript/v<major>.<minor>.<patch>, optionally followed by -alpha.N, -beta.N, -dev.N or -rc.N)"
version="${SEMAPHORE_GIT_TAG_NAME#javascript/v}"
[ "${version%%.*}" -ge "$MIN_MAJOR" ] \
    || die "$version is below $MIN_MAJOR.0.0; the versions of $MAIN below $MIN_MAJOR are the earlier client's, published from its own repository under the legacy dist-tag"

[ -f SHA256SUMS ] || die "$1 has no SHA256SUMS"
echo "== Checking the tarballs against SHA256SUMS =="
if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c SHA256SUMS || die "a tarball does not match SHA256SUMS"
else
    shasum -a 256 -c SHA256SUMS || die "a tarball does not match SHA256SUMS"
fi
tarballs=()
names=()
for platform in "${PLATFORMS[@]}"; do
    tarballs+=("confluentinc-kafka-javascript-$platform-$version.tgz")
    names+=("$MAIN-$platform")
done
tarballs+=("confluentinc-kafka-javascript-$version.tgz")
names+=("$MAIN")
listed=$(cut -d' ' -f3 SHA256SUMS | LC_ALL=C sort)
[ "$listed" = "$(printf '%s\n' "${tarballs[@]}" | LC_ALL=C sort)" ] \
    || die "SHA256SUMS lists $(printf '%s' "$listed" | tr '\n' ' '), not ${tarballs[*]}"
[ "$(ls -A | LC_ALL=C sort)" = "$(printf '%s\n' SHA256SUMS "${tarballs[@]}" | LC_ALL=C sort)" ] \
    || die "$1 holds $(ls -A | tr '\n' ' '), not SHA256SUMS and ${tarballs[*]}"

dist_tag=$(python3 "$check_npm" dist-tag "$version") || die "no dist-tag for $version"
[ "$dist_tag" != legacy ] || die "$version is a 1.x fix, published from the earlier client's repository, not from here"
echo "Publishing $MAIN $version under the $dist_tag dist-tag"

# npm reads a user config of its own, empty until the token is needed, so the registry comparison
# below is anonymous and any other npm config of the machine is not read.
userconfig=$(mktemp)
trap 'rm -f "$userconfig"' EXIT
export NPM_CONFIG_USERCONFIG="$userconfig"
# For a scoped package npm uses an `@confluentinc:registry` setting before `registry`: pin both.
REGISTRY_FLAGS=(--registry "$REGISTRY" "--@confluentinc:registry=$REGISTRY")

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
        die "${names[$i]}@$version is already published with another tarball ($remote, not $local_integrity); release a new version"
    fi
done

if [ "$check_only" = true ]; then
    echo "Every check that needs no token passed."
    exit 0
fi

[ -n "${NPM_TOKEN:-}" ] || die "NPM_TOKEN is not set: publish-npm.yml loads the granular access token from Vault (npm/kafka-javascript)"
# The token reaches npm through the user config, which npm reads ${NPM_TOKEN} into, so it is never
# on a command line or in the log.
# shellcheck disable=SC2016 # npm, not the shell, expands ${NPM_TOKEN}.
printf '//registry.npmjs.org/:_authToken=${NPM_TOKEN}\n' > "$userconfig"

echo "== Checking the token =="
user=$(npm whoami "${REGISTRY_FLAGS[@]}") || die "npm does not accept the token in NPM_TOKEN"
echo "Publishing to $REGISTRY as $user"

if [ "${#pending[@]}" -eq 0 ]; then
    echo "Every package of $version is already published."
else
    echo "== Publishing =="
    for i in "${pending[@]}"; do
        npm publish "${tarballs[$i]}" --access public --tag "$dist_tag" --provenance=false "${REGISTRY_FLAGS[@]}" \
            || die "npm publish of ${tarballs[$i]} failed; rerun the block, which skips what is already published"
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
# nothing: the dist-tag must be $version, and for a pre-release the main package's `latest`, `rc`
# and `alpha`, when present, a version below 4 (the earlier client's; the registry sets a new
# platform package's `latest` on its first publish, which is harmless). Fails if the registry
# cannot be read.
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
        if (name === main && version.includes("-")) {
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
    ' "$tags" "$1" "$version" "$MAIN" "$dist_tag"
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
    echo "${names[$i]}: $dist_tag is $version"
done

echo "== Summary =="
for i in "${!tarballs[@]}"; do
    sum=$(grep -F "  ${tarballs[$i]}" SHA256SUMS | cut -d' ' -f1)
    echo "$sum  ${tarballs[$i]}  ${names[$i]}@$version"
done
echo "$MAIN $version is published under the $dist_tag dist-tag."
