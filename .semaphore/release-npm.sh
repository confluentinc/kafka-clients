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
# Records the tested npm packages of a JavaScript release tag build as the release, for the
# "Release (JavaScript): record" block of semaphore.yml. Nothing is published: the release is
# published to npmjs.com by hand, with the publish.sh of the bundle this script writes.
#
# Usage, from the repository root of a javascript/v<version> tag build:
#   .semaphore/release-npm.sh
#       Refuses unless all of the following hold, then records the release in the workflow's
#       artifacts: npm/release/confluent-kafka-javascript-<version>-release.zip, holding the four
#       tarballs, their SHA256SUMS, publish.sh and README.txt (javascript/release/), with the
#       tarballs' checksums in npm/release/SHA256SUMS. It loads no token, and reads npmjs.com only
#       to list the versions of the packages.
#         - the build is of a JavaScript release tag that still points at the commit the build
#           checked out, and that commit is part of master;
#         - the tag matches "version" in javascript/package.json at that commit, which is 4.0.0
#           or higher, and the package is ${NPM_PACKAGE};
#         - npmjs.com has that version of none of the four packages yet, as it never takes a
#           version twice;
#         - the tarballs the pack block recorded are the tarballs every verification block of
#           ${NPM_PLATFORMS} tested;
#         - check-npm.py accepts them;
#         - this workflow has not recorded other tarballs as the release.
#
# Environment: NPM_PACKAGE, the main package's name; NPM_PLATFORMS, the platforms of the
# verification blocks, whose checksums are recorded under npm/tested/<platform>; and Semaphore's
# SEMAPHORE_* variables. /usr/bin/python3 is the image's system Python.

set -uo pipefail

work=/tmp/npm-release
ALL_PLATFORMS="linux-x64-gnu linux-arm64-gnu darwin-arm64"

# Fails with a message stating why nothing was recorded.
refuse() {
    echo "Refusing to record the release: $*" >&2
    exit 1
}

sha256_of() {
    sha256sum "$1" | cut -d' ' -f1
}

# Sets PACKAGE_VERSION, the tag name without its leading `javascript/v`, and BUNDLE. The block's
# `run` condition already limits it to JavaScript release tags; the check is repeated here, so
# that the release does not depend on that condition alone. A build whose jobs were redirected to
# another branch (SEMAPHORE_GIT_BRANCH_CHECKOUT, see .semaphore/job-start.sh) did not test the
# tagged commit.
release_version() {
    local release_tag_pattern='^javascript/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(alpha|beta|dev|rc)\.(0|[1-9][0-9]*))?$'
    if [ "${SEMAPHORE_GIT_REF_TYPE:-}" != "tag" ]; then
        refuse "only a release tag build records a release (ref type '${SEMAPHORE_GIT_REF_TYPE:-}'); push a tag javascript/v<version> for the master commit to release"
    fi
    if ! [[ "${SEMAPHORE_GIT_TAG_NAME:-}" =~ ${release_tag_pattern} ]]; then
        refuse "'${SEMAPHORE_GIT_TAG_NAME:-}' is not a JavaScript release tag (javascript/v<major>.<minor>.<patch>, optionally followed by -alpha.N, -beta.N, -dev.N or -rc.N)"
    fi
    if [ -n "${SEMAPHORE_GIT_BRANCH_CHECKOUT:-}" ]; then
        refuse "this build's jobs were switched to branch '${SEMAPHORE_GIT_BRANCH_CHECKOUT}' and did not test ${SEMAPHORE_GIT_TAG_NAME}"
    fi
    [ "${NPM_PACKAGE:-}" = @confluentinc/kafka-javascript ] || refuse "NPM_PACKAGE is '${NPM_PACKAGE:-}', not @confluentinc/kafka-javascript"
    PACKAGE_VERSION="${SEMAPHORE_GIT_TAG_NAME#javascript/v}"
    # The name is shared with confluent-kafka-javascript, whose versions are 1.x: this client's
    # start at 4, the Apache Kafka line, so that no version of it can be taken for one of those.
    if [ "${PACKAGE_VERSION%%.*}" -lt 4 ]; then
        refuse "${PACKAGE_VERSION} is below 4.0.0; the versions of ${NPM_PACKAGE} below 4 are confluent-kafka-javascript's"
    fi
    BUNDLE="confluent-kafka-javascript-${PACKAGE_VERSION}-release.zip"
}

# Because a version on npmjs.com can never be replaced, the tag must still point at the commit
# this build tested, and that commit must be part of master. `git ls-remote` reports a
# lightweight tag as the commit itself, and an annotated tag as the tag object followed by
# `<tag>^{}`, the commit it references. Its patterns also match the end of longer ref names
# (refs/tags/x/refs/tags/<tag>), so only the lines of exactly this tag count.
check_tag() {
    local head_sha remote_tag tag_sha
    head_sha=$(git rev-parse HEAD) || refuse "could not resolve HEAD"
    remote_tag=$(git ls-remote origin "refs/tags/${SEMAPHORE_GIT_TAG_NAME}" "refs/tags/${SEMAPHORE_GIT_TAG_NAME}^{}") \
        || refuse "could not read tag ${SEMAPHORE_GIT_TAG_NAME} from origin"
    tag_sha=$(printf '%s\n' "${remote_tag}" | awk -v ref="refs/tags/${SEMAPHORE_GIT_TAG_NAME}" \
        '$2 == ref "^{}" { peeled = $1 } $2 == ref { direct = $1 } END { print (peeled != "" ? peeled : direct) }')
    if [ -z "${tag_sha}" ]; then
        refuse "tag ${SEMAPHORE_GIT_TAG_NAME} no longer exists on origin"
    fi
    if [ "${tag_sha}" != "${head_sha}" ]; then
        refuse "tag ${SEMAPHORE_GIT_TAG_NAME} now points at ${tag_sha}, not at ${head_sha}, which this build tested; the build of the moved tag records the release instead"
    fi
    git fetch --quiet --no-tags origin master \
        || refuse "could not fetch origin/master to confirm that ${SEMAPHORE_GIT_TAG_NAME} is on master"
    git merge-base --is-ancestor "${head_sha}" FETCH_HEAD \
        || refuse "${SEMAPHORE_GIT_TAG_NAME} (${head_sha}) is not on master; tag a master commit"
}

release_version
echo "Recording ${NPM_PACKAGE} ${PACKAGE_VERSION} as the release"
if ! { rm -rf "${work}" && mkdir -p "${work}/bundle"; }; then
    refuse "could not create ${work}"
fi
check_tag

# The tag must match "version" in javascript/package.json at the tagged commit, whose package must
# also have the name this script records.
package_version=$(/usr/bin/python3 .semaphore/check-npm.py version) \
    || refuse "could not read \"version\" from javascript/package.json"
if [ "${PACKAGE_VERSION}" != "${package_version}" ]; then
    refuse "tag ${SEMAPHORE_GIT_TAG_NAME} names version '${PACKAGE_VERSION}', but javascript/package.json at this commit has '${package_version}'; tag the commit that sets the version"
fi

# create-release-tag.yml makes the same check, but a tag can also be pushed by other means.
packages=("${NPM_PACKAGE}")
for platform in ${ALL_PLATFORMS}; do
    packages+=("${NPM_PACKAGE}-${platform}")
done
for package in "${packages[@]}"; do
    /usr/bin/python3 .semaphore/check-published-npm.py versions "${package}" > "${work}/published" \
        || refuse "could not read npmjs.com to confirm that ${package} ${PACKAGE_VERSION} is new"
    if grep -qxF -- "${PACKAGE_VERSION}" "${work}/published"; then
        refuse "${package} ${PACKAGE_VERSION} is already on npmjs.com, which never takes a version twice; release a new version"
    fi
done

# Proceeds only if the tarballs the pack block recorded are exactly the tarballs each verification
# block tested. The pack block records nothing else under npm/built.
artifact pull workflow npm/built --destination "${work}/built" --force >/dev/null \
    || refuse "this workflow has no packed tarballs; \"Release (JavaScript): pack\" must pass first"
expected=$(for platform in ${ALL_PLATFORMS}; do echo "confluentinc-kafka-javascript-${platform}-${PACKAGE_VERSION}.tgz"; done
    echo "confluentinc-kafka-javascript-${PACKAGE_VERSION}.tgz")
expected=$(printf '%s\n' "${expected}" | LC_ALL=C sort)
listing=$(ls -A "${work}/built" | LC_ALL=C sort)
if [ "${listing}" != "${expected}" ]; then
    refuse "npm/built holds $(printf '%s' "${listing}" | tr '\n' ' '), not $(printf '%s' "${expected}" | tr '\n' ' ')"
fi
# The format test-npm.sh writes: `<sha256>  <tarball>`, sorted by tarball.
checksums=""
for file in ${listing}; do
    sum=$(sha256_of "${work}/built/${file}") || refuse "could not hash ${file}"
    checksums+="${sum}  ${file}"$'\n'
done
checksums=$(printf '%s' "${checksums}" | LC_ALL=C sort -k2)
[ -n "${NPM_PLATFORMS:-}" ] || refuse "NPM_PLATFORMS is empty"
for platform in ${NPM_PLATFORMS}; do
    artifact pull workflow "npm/tested/${platform}" --destination "${work}/tested-${platform}" --force >/dev/null \
        || refuse "this workflow has no tested checksums for ${platform}; \"Release (JavaScript): package verification (${platform})\" must pass first"
    tested=$(cat "${work}/tested-${platform}")
    if [ "${tested}" != "${checksums}" ]; then
        refuse "${platform} tested '${tested}', not '${checksums}', the packed tarballs"
    fi
done
/usr/bin/python3 .semaphore/check-npm.py package "${work}/built" --version "${PACKAGE_VERSION}" \
    || refuse "the packed tarballs do not pass check-npm.py"
printf '%s\n' "${checksums}" > "${work}/SHA256SUMS"

# The bundle: the tarballs, their SHA256SUMS, publish.sh and its README.txt. The zip is written
# with fixed entry times, so that a rerun over the same tarballs writes the same bytes.
cp "${work}/built/"*.tgz "${work}/SHA256SUMS" javascript/release/publish.sh javascript/release/README.txt "${work}/bundle/" \
    || refuse "could not assemble the bundle"
/usr/bin/python3 - "${work}/bundle" "${work}/${BUNDLE}" <<'PYTHON' || refuse "could not write ${BUNDLE}"
import os, sys, zipfile

source, target = sys.argv[1:]
with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
    for name in sorted(os.listdir(source)):
        entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
        entry.compress_type = zipfile.ZIP_DEFLATED
        entry.external_attr = (0o100755 if name == "publish.sh" else 0o100644) << 16
        with open(os.path.join(source, name), "rb") as f:
            archive.writestr(entry, f.read())
PYTHON
bundle_sum=$(sha256_of "${work}/${BUNDLE}") || refuse "could not hash ${BUNDLE}"

# Once recorded, the release is final: it may already have been downloaded and published. A
# rerun of the pack block packs other bytes (a pack need not be reproducible), so its tarballs are
# not recorded over them. SHA256SUMS claims the release: it is pushed first, without --force,
# which fails when the file exists, so a pull that fails for another reason cannot lead to a
# recorded release being replaced. The bundle is then written under it, so that the bundle
# recorded always holds the tarballs SHA256SUMS names, and a rerun completes a job interrupted
# between the two pushes.
if artifact pull workflow npm/release/SHA256SUMS --destination "${work}/recorded.sha256" --force >/dev/null 2>&1; then
    if ! cmp -s "${work}/SHA256SUMS" "${work}/recorded.sha256"; then
        refuse "this workflow already recorded $(tr '\n' ' ' < "${work}/recorded.sha256")as the release, which may already be on npmjs.com; it is not replaced by the repacked $(tr '\n' ' ' < "${work}/SHA256SUMS"). Publish the recorded release, or release a new version"
    fi
    echo "This workflow already recorded these tarballs; recording the bundle again"
else
    artifact push workflow "${work}/SHA256SUMS" --destination npm/release/SHA256SUMS \
        || refuse "could not record the checksums of the tarballs, either because this workflow already recorded a release that could not be read, or because the artifact store failed; rerun this job"
fi
artifact push workflow "${work}/${BUNDLE}" --destination "npm/release/${BUNDLE}" --force \
    || refuse "could not record ${BUNDLE}; rerun this job"
cat "${work}/SHA256SUMS"
echo "${bundle_sum}  ${BUNDLE}"
echo "Recorded ${BUNDLE}, holding the tarballs that were tested on: ${NPM_PLATFORMS}."
echo "Download npm/release/${BUNDLE} from this workflow's artifacts, check that its SHA-256 and"
echo "those of the tarballs in it are the ones printed above, then run its publish.sh (README.txt)."
