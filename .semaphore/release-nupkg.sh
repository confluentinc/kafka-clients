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
# Records the tested Confluent.Kafka NuGet package of a .NET release tag build as the release, for
# the "Release (.NET): packaging" block of semaphore.yml. Nothing is published: the release is
# uploaded to nuget.org by hand, from the tag build's artifacts.
#
# Usage, from the repository root of a dotnet/v<version> tag build:
#   .semaphore/release-nupkg.sh
#       Refuses unless all of the following hold, then records the package as the release in the
#       workflow's artifacts: nuget/release/<package>, with `<sha256>  <package>` in
#       nuget/release/SHA256SUMS. It loads no key, and reads nuget.org only to list the versions
#       of the package.
#         - the build is of a .NET release tag that still points at the commit the build
#           checked out, and that commit is part of master;
#         - the tag matches <Version> in dotnet/Directory.Build.props at that commit, which is
#           4.0.0 or higher, and the library's <PackageId> is ${NUGET_PACKAGE};
#         - nuget.org does not have that version yet, which it would refuse to take again;
#         - the package the pack block recorded is the package every verification block of
#           ${NUGET_PLATFORMS} tested;
#         - check-nupkg.py accepts it, with the natives of every RID, for this commit;
#         - this workflow has not recorded another package as the release.
#
# Environment: NUGET_PACKAGE, the package id; NUGET_PLATFORMS, the platforms of the verification
# blocks, whose checksums are recorded under nuget/tested/<platform>; and Semaphore's SEMAPHORE_*
# variables. /usr/bin/python3 is the image's system Python.

set -uo pipefail

work=/tmp/nuget-release
ALL_RIDS="linux-x64 linux-arm64 osx-arm64 win-x64 win-x86"

# Fails with a message stating why nothing was recorded.
refuse() {
    echo "Refusing to record the release: $*" >&2
    exit 1
}

sha256_of() {
    sha256sum "$1" | cut -d' ' -f1
}

# Sets PACKAGE_VERSION, the tag name without its leading `dotnet/v`, and PACKAGE_FILE. The
# block's `run` condition already limits it to .NET release tags; the check is repeated here, so
# that the release does not depend on that condition alone. A build whose jobs were redirected to
# another branch (SEMAPHORE_GIT_BRANCH_CHECKOUT, see .semaphore/job-start.sh) did not test the
# tagged commit.
release_version() {
    local release_tag_pattern='^dotnet/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((alpha|beta|rc)\.(0|[1-9][0-9]*)(\.dev\.(0|[1-9][0-9]*))?|dev\.(0|[1-9][0-9]*)))?$'
    if [ "${SEMAPHORE_GIT_REF_TYPE:-}" != "tag" ]; then
        refuse "only a release tag build records a release (ref type '${SEMAPHORE_GIT_REF_TYPE:-}'); push a tag dotnet/v<version> for the master commit to release"
    fi
    if ! [[ "${SEMAPHORE_GIT_TAG_NAME:-}" =~ ${release_tag_pattern} ]]; then
        refuse "'${SEMAPHORE_GIT_TAG_NAME:-}' is not a .NET release tag (dotnet/v<major>.<minor>.<patch>, optionally followed by -alpha.N, -beta.N, -dev.N or -rc.N, an -alpha.N, -beta.N or -rc.N then optionally by .dev.M)"
    fi
    if [ -n "${SEMAPHORE_GIT_BRANCH_CHECKOUT:-}" ]; then
        refuse "this build's jobs were switched to branch '${SEMAPHORE_GIT_BRANCH_CHECKOUT}' and did not test ${SEMAPHORE_GIT_TAG_NAME}"
    fi
    [ "${NUGET_PACKAGE:-}" = Confluent.Kafka ] || refuse "NUGET_PACKAGE is '${NUGET_PACKAGE:-}', not Confluent.Kafka"
    PACKAGE_VERSION="${SEMAPHORE_GIT_TAG_NAME#dotnet/v}"
    # The id is shared with confluent-kafka-dotnet, whose versions are 2.x: this client's start
    # at 4, the Apache Kafka line, so that no version of it can be taken for one of those.
    if [ "${PACKAGE_VERSION%%.*}" -lt 4 ]; then
        refuse "${PACKAGE_VERSION} is below 4.0.0; the versions of ${NUGET_PACKAGE} below 4 are confluent-kafka-dotnet's"
    fi
    PACKAGE_FILE="${NUGET_PACKAGE}.${PACKAGE_VERSION}.nupkg"
}

# Because a version on nuget.org can never be replaced, the tag must still point at the commit
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
echo "Recording ${NUGET_PACKAGE} ${PACKAGE_VERSION} as the release"
if ! { rm -rf "${work}" && mkdir -p "${work}/release"; }; then
    refuse "could not create ${work}"
fi
check_tag

# The tag must match <Version> in dotnet/Directory.Build.props at the tagged commit, whose library
# must also have the package id this script records.
props_version=$(/usr/bin/python3 .semaphore/check-nupkg.py version) \
    || refuse "could not read <Version> from dotnet/Directory.Build.props"
if [ "${PACKAGE_VERSION}" != "${props_version}" ]; then
    refuse "tag ${SEMAPHORE_GIT_TAG_NAME} names version '${PACKAGE_VERSION}', but dotnet/Directory.Build.props at this commit has '${props_version}'; tag the commit that sets the version"
fi
package_id=$(/usr/bin/python3 -c 'import sys, xml.etree.ElementTree as E; ids = [e.text for e in E.parse(sys.argv[1]).getroot().iter("PackageId")]; print(ids[0]) if len(ids) == 1 else sys.exit("expected one <PackageId>")' dotnet/src/Confluent.Kafka/Confluent.Kafka.csproj) \
    || refuse "could not read <PackageId> from dotnet/src/Confluent.Kafka/Confluent.Kafka.csproj"
if [ "${package_id}" != "${NUGET_PACKAGE}" ]; then
    refuse "dotnet/src/Confluent.Kafka/Confluent.Kafka.csproj at this commit has the package id '${package_id}', not ${NUGET_PACKAGE}"
fi

# create-release-tag.yml makes the same check, but a tag can also be pushed by other means.
/usr/bin/python3 .semaphore/check-published-nupkg.py versions https://api.nuget.org/v3/index.json "${NUGET_PACKAGE}" > "${work}/published" \
    || refuse "could not read nuget.org to confirm that ${NUGET_PACKAGE} ${PACKAGE_VERSION} is new"
if grep -qxF -- "${PACKAGE_VERSION}" "${work}/published"; then
    refuse "${NUGET_PACKAGE} ${PACKAGE_VERSION} is already on nuget.org, which never takes a version twice; release a new version"
fi

# Proceeds only if the package the pack block recorded is exactly the package each verification
# block tested.
artifact pull workflow "nuget/built/${PACKAGE_FILE}" --destination "${work}/release/${PACKAGE_FILE}" --force >/dev/null \
    || refuse "this workflow has no packed ${PACKAGE_FILE}; \"Release (.NET): pack\" must pass first"
checksum=$(sha256_of "${work}/release/${PACKAGE_FILE}") || refuse "could not hash ${PACKAGE_FILE}"
[ -n "${NUGET_PLATFORMS:-}" ] || refuse "NUGET_PLATFORMS is empty"
for platform in ${NUGET_PLATFORMS}; do
    artifact pull workflow "nuget/tested/${platform}" --destination "${work}/tested-${platform}" --force >/dev/null \
        || refuse "this workflow has no tested checksum for ${platform}; \"Release (.NET): package verification (${platform})\" must pass first"
    tested=$(cat "${work}/tested-${platform}")
    if [ "${tested}" != "${checksum}  ${PACKAGE_FILE}" ]; then
        refuse "${platform} tested '${tested}', not '${checksum}  ${PACKAGE_FILE}', the packed package"
    fi
done
/usr/bin/python3 .semaphore/check-nupkg.py package "${work}/release/${PACKAGE_FILE}" \
    --version "${PACKAGE_VERSION}" --rids "${ALL_RIDS}" --commit "$(git rev-parse HEAD)" \
    || refuse "${PACKAGE_FILE} does not pass check-nupkg.py"
echo "${checksum}  ${PACKAGE_FILE}" > "${work}/SHA256SUMS"

# Once recorded, the release is final: it may already have been downloaded and uploaded to
# nuget.org. A rerun of the pack block packs other bytes (a pack is not reproducible), so its
# package is not recorded over it. SHA256SUMS claims the release: it is pushed first, without
# --force, which fails when the file exists, so a pull that fails for another reason cannot lead to
# a recorded release being replaced. The package is then written under it, so that the package
# recorded is always the one SHA256SUMS names, and a rerun completes a job interrupted between
# the two pushes.
if artifact pull workflow nuget/release/SHA256SUMS --destination "${work}/recorded.sha256" --force >/dev/null 2>&1; then
    if ! cmp -s "${work}/SHA256SUMS" "${work}/recorded.sha256"; then
        refuse "this workflow already recorded $(cat "${work}/recorded.sha256") as the release, which may already be on nuget.org; it is not replaced by the repacked $(cat "${work}/SHA256SUMS"). Upload the recorded release, or release a new version"
    fi
    echo "This workflow already recorded ${PACKAGE_FILE} with this checksum; recording the package again"
else
    artifact push workflow "${work}/SHA256SUMS" --destination nuget/release/SHA256SUMS \
        || refuse "could not record the checksum of ${PACKAGE_FILE}, either because this workflow already recorded a release that could not be read, or because the artifact store failed; rerun this job"
fi
artifact push workflow "${work}/release/${PACKAGE_FILE}" --destination "nuget/release/${PACKAGE_FILE}" --force \
    || refuse "could not record ${PACKAGE_FILE}; rerun this job"
cat "${work}/SHA256SUMS"
echo "Recorded ${PACKAGE_FILE}, identical to the package that was tested on: ${NUGET_PLATFORMS}."
echo "Upload nuget/release/${PACKAGE_FILE} from this workflow's artifacts to nuget.org by hand,"
echo "after checking that its SHA-256 is the one printed above."
