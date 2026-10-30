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
# Packages the tested Confluent.Kafka NuGet package of a .NET release tag build and publishes it
# to one gallery, the counterpart of publish-wheels.sh, for the "Release (.NET): packaging" block
# of semaphore.yml and the "Release (.NET): publish to ..." blocks of publish-nuget-test.yml and
# publish-nuget.yml, the tag build's manual promotions. Every version goes to the NuGet test
# gallery first, then to nuget.org, after it has passed the post-publish checks on the test
# gallery.
#
# Usage, from the repository root of a dotnet/v<version> tag build:
#   .semaphore/publish-nupkg.sh package
#       Refuses unless all of the following hold, then records the package as the release:
#       nuget/release/<package>, with `<sha256>  <package>` in nuget/release/SHA256SUMS. It
#       loads no key and reads no gallery.
#         - the build is of a .NET release tag that still points at the commit the build
#           checked out, and that commit is part of master;
#         - the tag matches <Version> in dotnet/Directory.Build.props at that commit, and the
#           library's <PackageId> is ${NUGET_PACKAGE};
#         - the package the pack block recorded is the package every verification block of
#           ${NUGET_PLATFORMS} tested;
#         - check-nupkg.py accepts it, with the natives of every RID, for this commit.
#   .semaphore/publish-nupkg.sh prepare
#       Refuses unless all of the following hold, then copies the package to upload into
#       /tmp/nuget-publish/upload and writes `<sha256>  <package>` to
#       /tmp/nuget-publish/upload.sha256. It loads no key.
#         - the tag still points at the commit the build checked out, and that commit is part
#           of master (checked again, right before the upload);
#         - the version is not yet on the gallery. The only exception is completing an
#           interrupted publish of the same version from this workflow (see `upload`);
#         - the package is exactly the one `package` recorded, and check-nupkg.py accepts it
#           for this commit;
#         - for nuget.org, it is exactly the package that passed the post-publish checks on the
#           test gallery in this workflow, and the package the test gallery serves.
#       A version below 4.0.0 is always refused: those of the id are confluent-kafka-dotnet's.
#   .semaphore/publish-nupkg.sh upload
#       Loads the API key and pushes the package that `prepare` copied, then records it for
#       the post-publish blocks.
#
# Environment: NUGET_PACKAGE, the package id; NUGET_GALLERY (prepare and upload),
# https://int.nugettest.org or https://www.nuget.org; NUGET_PLATFORMS (package), the platforms
# of the verification blocks, whose checksums are recorded under nuget/tested/<platform>; and
# Semaphore's SEMAPHORE_* variables. What this script records for a gallery is under
# nuget/<gallery host>/ in the workflow's artifacts. `upload` needs `dotnet` (SDK 10), in
# $HOME/.dotnet or on PATH. /usr/bin/python3 is the image's system Python.

set -uo pipefail

work=/tmp/nuget-publish
ALL_RIDS="linux-x64 linux-arm64 osx-x64 osx-arm64"

# Fails with a message stating why nothing was published.
refuse() {
    echo "Refusing to publish: $*" >&2
    exit 1
}

sha256_of() {
    sha256sum "$1" | cut -d' ' -f1
}

# Sets PACKAGE_VERSION, the tag name without its leading `dotnet/v`, and PACKAGE_FILE. The
# blocks' `run` conditions already limit them to .NET release tags; the check is repeated here,
# before the key is loaded, so that the publish does not depend on those conditions alone. A
# build whose jobs were redirected to another branch (SEMAPHORE_GIT_BRANCH_CHECKOUT, see the
# global prologue of semaphore.yml) did not test the tagged commit.
release_version() {
    local release_tag_pattern='^dotnet/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(alpha|beta|dev|rc)\.(0|[1-9][0-9]*))?$'
    if [ "${SEMAPHORE_GIT_REF_TYPE:-}" != "tag" ]; then
        refuse "only a release tag build can publish (ref type '${SEMAPHORE_GIT_REF_TYPE:-}'); push a tag dotnet/v<version> for the master commit to release"
    fi
    if ! [[ "${SEMAPHORE_GIT_TAG_NAME:-}" =~ ${release_tag_pattern} ]]; then
        refuse "'${SEMAPHORE_GIT_TAG_NAME:-}' is not a .NET release tag (dotnet/v<major>.<minor>.<patch>, optionally followed by -alpha.N, -beta.N, -dev.N or -rc.N)"
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

# Sets the service index, key name and artifact directory of ${NUGET_GALLERY}.
gallery_settings() {
    case "${NUGET_GALLERY:-}" in
        https://int.nugettest.org)
            service_index=https://apiint.nugettest.org/v3/index.json
            key_name=test_nuget_api_key
            ;;
        https://www.nuget.org)
            service_index=https://api.nuget.org/v3/index.json
            key_name=nuget_api_key
            # Every version goes to nuget.org, a pre-release included: NuGet installs a
            # pre-release only on request (`--prerelease`, or the version itself), and the
            # publish is a manual promotion.
            ;;
        *)
            refuse "NUGET_GALLERY is '${NUGET_GALLERY:-}', not https://int.nugettest.org or https://www.nuget.org"
            ;;
    esac
    artifacts="nuget/${NUGET_GALLERY#https://}"
}

# Creates an empty ${work}, with the directory of the package to upload.
new_work() {
    if ! { rm -rf "${work}" && mkdir -p "${work}/upload"; }; then
        refuse "could not create ${work}"
    fi
}

# Because a version on a NuGet gallery can never be replaced, the tag must still point at the
# commit this build tested, and that commit must be part of master. Checking immediately before
# the upload also catches a tag that was moved or deleted while CI ran. `git ls-remote` reports
# a lightweight tag as the commit itself, and an annotated tag as the tag object followed by
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
        refuse "tag ${SEMAPHORE_GIT_TAG_NAME} now points at ${tag_sha}, not at ${head_sha}, which this build tested; the build of the moved tag publishes instead"
    fi
    git fetch --quiet --no-tags origin master \
        || refuse "could not fetch origin/master to confirm that ${SEMAPHORE_GIT_TAG_NAME} is on master"
    git merge-base --is-ancestor "${head_sha}" FETCH_HEAD \
        || refuse "${SEMAPHORE_GIT_TAG_NAME} (${head_sha}) is not on master; tag a master commit"
}

# Puts the SDK that install-dotnet.sh installed on PATH.
use_dotnet() {
    if [ -x "${HOME}/.dotnet/dotnet" ]; then
        export DOTNET_ROOT="${HOME}/.dotnet" PATH="${HOME}/.dotnet:${PATH}" DOTNET_MULTILEVEL_LOOKUP=0
    fi
    export DOTNET_NOLOGO=1 DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
    command -v dotnet >/dev/null 2>&1 || refuse "dotnet is not on PATH; run .semaphore/install-dotnet.sh first"
}

package() {
    release_version
    echo "Packaging ${NUGET_PACKAGE} ${PACKAGE_VERSION}"
    new_work
    check_tag

    # The tag must match <Version> in dotnet/Directory.Build.props at the tagged commit, whose
    # library must also have the package id this script publishes.
    local props_version package_id
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

    # Proceeds only if the package the pack block recorded is exactly the package each
    # verification block tested.
    artifact pull workflow "nuget/built/${PACKAGE_FILE}" --destination "${work}/upload/${PACKAGE_FILE}" --force >/dev/null \
        || refuse "this workflow has no packed ${PACKAGE_FILE}; \"Release (.NET): pack\" must pass first"
    local checksum platform tested
    checksum=$(sha256_of "${work}/upload/${PACKAGE_FILE}") || refuse "could not hash ${PACKAGE_FILE}"
    [ -n "${NUGET_PLATFORMS:-}" ] || refuse "NUGET_PLATFORMS is empty"
    for platform in ${NUGET_PLATFORMS}; do
        artifact pull workflow "nuget/tested/${platform}" --destination "${work}/tested-${platform}" --force >/dev/null \
            || refuse "this workflow has no tested checksum for ${platform}; \"Release (.NET): package verification (${platform})\" must pass first"
        tested=$(cat "${work}/tested-${platform}")
        if [ "${tested}" != "${checksum}  ${PACKAGE_FILE}" ]; then
            refuse "${platform} tested '${tested}', not '${checksum}  ${PACKAGE_FILE}', the packed package"
        fi
    done
    /usr/bin/python3 .semaphore/check-nupkg.py package "${work}/upload/${PACKAGE_FILE}" \
        --version "${PACKAGE_VERSION}" --rids "${ALL_RIDS}" --commit "$(git rev-parse HEAD)" \
        || refuse "${PACKAGE_FILE} does not pass check-nupkg.py"
    echo "${checksum}  ${PACKAGE_FILE}" > "${work}/upload.sha256"

    # Once this workflow has started uploading the release to a gallery, the release is final.
    # A rerun of the pack block packs other bytes (a pack is not reproducible), and recording
    # them would leave no package that both galleries can accept.
    if artifact pull workflow nuget/release/SHA256SUMS --destination "${work}/recorded.sha256" --force >/dev/null 2>&1 \
        && ! cmp -s "${work}/upload.sha256" "${work}/recorded.sha256"; then
        local host
        for host in int.nugettest.org www.nuget.org; do
            if artifact pull workflow "nuget/${host}/upload-started" --destination "${work}/started-${host}" --force >/dev/null 2>&1; then
                refuse "this workflow has started uploading the recorded release, $(cat "${work}/recorded.sha256"), to ${host}, so it cannot be replaced by the repacked $(cat "${work}/upload.sha256"); publish the recorded release, or release a new version"
            fi
        done
    fi

    # SHA256SUMS is recorded last: `prepare` refuses without it, so a package job interrupted
    # while recording the package publishes nothing.
    artifact push workflow "${work}/upload/${PACKAGE_FILE}" --destination "nuget/release/${PACKAGE_FILE}" --force \
        || refuse "could not record the packaged ${PACKAGE_FILE}; rerun this job"
    artifact push workflow "${work}/upload.sha256" --destination nuget/release/SHA256SUMS --force \
        || refuse "could not record the checksum of the packaged ${PACKAGE_FILE}; rerun this job"
    cat "${work}/upload.sha256"
    echo "Packaged ${PACKAGE_FILE}, identical to the package that was tested on: ${NUGET_PLATFORMS}"
}

prepare() {
    release_version
    gallery_settings
    echo "${NUGET_PACKAGE} ${PACKAGE_VERSION} is published to ${NUGET_GALLERY}"
    new_work
    check_tag

    # A version already on the gallery, listed or not, cannot be published again: NuGet never
    # accepts a version twice, even after it was deleted. The exception is a version whose
    # upload this workflow has already started: `upload` records upload-started before
    # uploading, so a rerun after an interrupted upload can complete it. A version need not be
    # higher than the ones already published: confluent-kafka-dotnet keeps publishing 2.x
    # versions of the same id, and a patch of an older minor version can follow a newer one. The
    # gallery lists versions in their lowercase normalized form, which a tag version has already.
    local listing
    listing=$(/usr/bin/python3 .semaphore/check-published-nupkg.py versions "${service_index}" "${NUGET_PACKAGE}") \
        || refuse "could not read ${NUGET_GALLERY} to confirm that ${NUGET_PACKAGE} ${PACKAGE_VERSION} is new"
    if printf '%s\n' "${listing}" | grep -qxF -- "${PACKAGE_VERSION}"; then
        if artifact pull workflow "${artifacts}/upload-started" --destination "${work}/upload-started" --force >/dev/null 2>&1; then
            echo "${NUGET_PACKAGE} ${PACKAGE_VERSION} is already on ${NUGET_GALLERY} and this workflow started its upload; continuing to finish that publish"
        else
            refuse "${NUGET_PACKAGE} ${PACKAGE_VERSION} is already on ${NUGET_GALLERY}, and a published version cannot be replaced; bump <Version> in dotnet/Directory.Build.props and tag that commit"
        fi
    else
        echo "${NUGET_PACKAGE} ${PACKAGE_VERSION} is not yet on ${NUGET_GALLERY}"
    fi

    # Proceeds only if the recorded release is exactly the package that `package` listed in
    # SHA256SUMS. Its checksum is written to upload.sha256, which `upload` requires, only once
    # every check here has passed.
    local candidate="${work}/candidate.sha256"
    artifact pull workflow nuget/release/SHA256SUMS --destination "${work}/SHA256SUMS" --force >/dev/null \
        || refuse "this workflow has not packaged ${NUGET_PACKAGE} ${PACKAGE_VERSION}; \"Release (.NET): packaging\" must pass first"
    artifact pull workflow "nuget/release/${PACKAGE_FILE}" --destination "${work}/upload/${PACKAGE_FILE}" --force >/dev/null \
        || refuse "this workflow has no packaged ${PACKAGE_FILE}"
    local checksum
    checksum=$(sha256_of "${work}/upload/${PACKAGE_FILE}") || refuse "could not hash ${PACKAGE_FILE}"
    echo "${checksum}  ${PACKAGE_FILE}" > "${candidate}"
    cmp -s "${candidate}" "${work}/SHA256SUMS" \
        || refuse "the package to upload, $(cat "${candidate}"), is not the packaged package, $(cat "${work}/SHA256SUMS")"

    # Any job of the workflow can write its artifacts, so the package is checked again here,
    # against this commit, rather than trusted because `package` checked it.
    /usr/bin/python3 .semaphore/check-nupkg.py package "${work}/upload/${PACKAGE_FILE}" \
        --version "${PACKAGE_VERSION}" --rids "${ALL_RIDS}" --commit "$(git rev-parse HEAD)" \
        || refuse "${PACKAGE_FILE} does not pass check-nupkg.py"

    # nuget.org receives exactly the package that passed the post-publish checks on the test
    # gallery, which the last test gallery block recorded once they passed. The "Publish to
    # NuGet" promotion can be started even from a failed pipeline, so this check, not the order
    # of the blocks, is what guarantees it. The package must also be the one the test gallery
    # serves, which only the test gallery's key can have published, rather than only the one
    # the workflow's artifacts name.
    if [ "${NUGET_GALLERY}" = https://www.nuget.org ]; then
        artifact pull workflow nuget/int.nugettest.org/verified --destination "${work}/test-gallery-verified" --force >/dev/null \
            || refuse "${NUGET_PACKAGE} ${PACKAGE_VERSION} has not passed the post-publish checks on the NuGet test gallery in this workflow; the \"Publish to NuGet test gallery\" promotion of this tag build must pass first"
        cmp -s "${candidate}" "${work}/test-gallery-verified" \
            || refuse "the package to upload to nuget.org is not the package that passed the checks on the NuGet test gallery"
        /usr/bin/python3 .semaphore/check-published-nupkg.py check https://apiint.nugettest.org/v3/index.json \
            "${NUGET_PACKAGE}" "${PACKAGE_VERSION}" "${work}/upload/${PACKAGE_FILE}" --attempts 1 \
            || refuse "the package to upload to nuget.org is not the package the NuGet test gallery serves"
    fi
    use_dotnet
    dotnet --version || refuse "dotnet does not run"
    mv "${candidate}" "${work}/upload.sha256" || refuse "could not record the package to upload"
    cat "${work}/upload.sha256"
    echo "${PACKAGE_FILE} to upload to ${NUGET_GALLERY}, identical to the package that was tested"
}

upload() {
    release_version
    gallery_settings
    [ -s "${work}/upload.sha256" ] || refuse "nothing to upload; run '$0 prepare' first"
    local checksum
    checksum=$(sha256_of "${work}/upload/${PACKAGE_FILE}") || refuse "could not hash ${PACKAGE_FILE}"
    [ "$(cat "${work}/upload.sha256")" = "${checksum}  ${PACKAGE_FILE}" ] \
        || refuse "${work}/upload/${PACKAGE_FILE} is not the package that '$0 prepare' checked"
    use_dotnet

    # The Vault secret is nuget/confluent-kafka, as Python's is pypi/confluent-kafka, holding one
    # key per gallery (key_name).
    # shellcheck disable=SC1091 # provided by the Semaphore agent
    . vault-get-secret --secret-key="${key_name}" --export-as=NUGET_API_KEY nuget/confluent-kafka
    [ -n "${NUGET_API_KEY:-}" ] || refuse "Vault returned no ${key_name} for nuget/confluent-kafka"

    # Recorded before the upload, so that a rerun after an interrupted upload passes the
    # "already on the gallery" check of `prepare`.
    if ! { echo "${SEMAPHORE_JOB_ID:-}" > "${work}/upload-started" \
        && artifact push workflow "${work}/upload-started" --destination "${artifacts}/upload-started" --force; }; then
        refuse "could not record that the upload started"
    fi

    # Pushed from an empty directory with a nuget.config of its own, so that no NuGet
    # configuration of the repository or the machine takes part. The gallery answers a push of a
    # version it already has with a conflict, and the push may also report an error for a
    # package the gallery accepted. In either case, the publish is treated as successful only if
    # the gallery serves exactly this package; the post-publish block then checks it again.
    mkdir -p "${work}/push" && printf '<configuration><packageSources><clear /></packageSources></configuration>\n' > "${work}/push/nuget.config"
    if ! (cd "${work}/push" && dotnet nuget push "${work}/upload/${PACKAGE_FILE}" --source "${service_index}" \
        --api-key "${NUGET_API_KEY}" --timeout 900 --disable-buffering); then
        unset NUGET_API_KEY
        echo "dotnet nuget push failed; checking whether ${NUGET_GALLERY} serves the package anyway"
        /usr/bin/python3 .semaphore/check-published-nupkg.py check "${service_index}" "${NUGET_PACKAGE}" "${PACKAGE_VERSION}" \
            "${work}/upload/${PACKAGE_FILE}" --attempts 40 \
            || refuse "dotnet nuget push failed, and ${NUGET_GALLERY} does not serve the package that was tested (see above). If the gallery is still validating an upload it accepted, rerun this job later: the rerun finishes the publish"
        echo "${NUGET_GALLERY} serves exactly this package; continuing"
    fi
    unset NUGET_API_KEY

    # Records the uploaded package for the post-publish blocks.
    artifact push workflow "${work}/upload.sha256" --destination "${artifacts}/uploaded.sha256" --force \
        || refuse "could not record the uploaded package; rerun this job to record it"
}

case "${1:-}" in
    package) package ;;
    prepare) prepare ;;
    upload) upload ;;
    *)
        echo "usage: $0 package|prepare|upload" >&2
        exit 2
        ;;
esac
