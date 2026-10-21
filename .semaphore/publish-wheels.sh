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
# Packages the tested wheels of a Python release tag build and publishes them
# to one index, for the "Release (Python): packaging" and "Release (Python):
# publish to ..." blocks of semaphore.yml and publish-pypi.yml. Every version
# goes to Test PyPI first; a final version then goes to PyPI, after its wheels
# have passed the post-publish checks on Test PyPI.
#
# Usage, from the repository root of a python/v<version> tag build:
#   .semaphore/publish-wheels.sh package
#       Refuses unless all of the following hold, then records the wheels as
#       the release: pypi/release/wheels, with `<sha256>  <wheel>` for each in
#       pypi/release/SHA256SUMS. It loads no token and reads no index.
#         - the build is of a Python release tag that still points at the
#           commit the build checked out, and that commit is part of master;
#         - the tag matches [project] version in python/pyproject.toml at that
#           commit, and [project] name is ${PYPI_PACKAGE};
#         - the wheels the wheel blocks recorded are exactly the wheels the
#           wheel verification blocks tested, all of this version, one set per
#           platform of ${WHEEL_PLATFORMS};
#         - `twine check` accepts their metadata.
#   .semaphore/publish-wheels.sh prepare
#       Refuses unless all of the following hold, then copies the wheels to
#       upload into /tmp/pypi-publish/upload and writes `<sha256>  <wheel>` for
#       each to /tmp/pypi-publish/upload.sha256. It loads no token.
#         - the tag still points at the commit the build checked out, and that
#           commit is part of master (checked again, right before the upload);
#         - the index takes the version: Test PyPI takes every version, PyPI
#           only a final one (no aN, bN, rcN or .devN suffix);
#         - the version is not yet on the index. The only exception is
#           completing an interrupted publish of the same version from this
#           workflow (see `upload`);
#         - the wheels are exactly the ones `package` recorded;
#         - for PyPI, they are exactly the wheels that passed the post-publish
#           checks on Test PyPI in this workflow.
#   .semaphore/publish-wheels.sh upload
#       Loads the token and uploads the wheels that `prepare` copied, then
#       records them for the post-publish blocks.
#
# Environment: PYPI_PACKAGE, the project; PYPI_INDEX (prepare and upload),
# https://test.pypi.org or https://pypi.org; WHEEL_PLATFORMS (package), the
# platforms of the wheel blocks, whose wheels are recorded under
# pypi/wheels/<platform> and, once tested, their checksums under
# pypi/tested/<platform>; and Semaphore's SEMAPHORE_* variables. What this
# script records for an index is under pypi/<index host>/ in the workflow's
# artifacts. /usr/bin/python3 is the image's system Python (3.12 on Ubuntu
# 24.04), which provides tomllib.

set -uo pipefail

work=/tmp/pypi-publish

# Fails with a message stating why nothing was published.
refuse() {
    echo "Refusing to publish: $*" >&2
    exit 1
}

# simple_index: prints `version <version>` for each version of
# ${PYPI_PACKAGE} on ${PYPI_INDEX}, yanked ones included, then `file <sha256>
# <filename>` for each of its files, and nothing if the project does not exist
# there. Returns non-zero when the index cannot be read, including an empty
# response. It reads the JSON form of the simple repository API (PEP 691, with
# the version list of PEP 700).
simple_index() {
    local body status
    body=$(mktemp) || return 1
    status=$(curl -sS --retry 3 --retry-delay 5 -o "${body}" -w '%{http_code}' \
        -H 'Accept: application/vnd.pypi.simple.v1+json' "${PYPI_INDEX}/simple/${PYPI_PACKAGE}/") || { rm -f "${body}"; return 1; }
    case "${status}" in
        200)
            [ -s "${body}" ] || { echo "${PYPI_INDEX} returned an empty listing for ${PYPI_PACKAGE}" >&2; rm -f "${body}"; return 1; }
            /usr/bin/python3 -c '
import json, sys
page = json.load(open(sys.argv[1]))
for version in page["versions"]:
    print("version", version)
for file in page["files"]:
    print("file", file["hashes"]["sha256"], file["filename"])
' "${body}" || { rm -f "${body}"; return 1; }
            ;;
        404) ;;
        *)
            echo "${PYPI_INDEX} returned HTTP ${status} for ${PYPI_PACKAGE}" >&2
            rm -f "${body}"
            return 1
            ;;
    esac
    rm -f "${body}"
}

# Sets PACKAGE_VERSION, the tag name without its leading `python/`. The
# blocks' `run` conditions already limit them to Python release tags; the
# check is repeated here, before the token is loaded, so that the publish does
# not depend on those conditions alone. A build whose jobs were redirected to
# another branch (SEMAPHORE_GIT_BRANCH_CHECKOUT, see the global prologue of
# semaphore.yml) did not test the tagged commit.
release_version() {
    local release_tag_pattern='^python/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)((a|b|rc)(0|[1-9][0-9]*))?(\.dev(0|[1-9][0-9]*))?$'
    if [ "${SEMAPHORE_GIT_REF_TYPE:-}" != "tag" ]; then
        refuse "only a release tag build can publish (ref type '${SEMAPHORE_GIT_REF_TYPE:-}'); push a tag python/v<version> for the master commit to release"
    fi
    if ! [[ "${SEMAPHORE_GIT_TAG_NAME:-}" =~ ${release_tag_pattern} ]]; then
        refuse "'${SEMAPHORE_GIT_TAG_NAME:-}' is not a Python release tag (python/v<major>.<minor>.<patch>, optionally followed by aN, bN or rcN, then optionally by .devN)"
    fi
    if [ -n "${SEMAPHORE_GIT_BRANCH_CHECKOUT:-}" ]; then
        refuse "this build's jobs were switched to branch '${SEMAPHORE_GIT_BRANCH_CHECKOUT}' and did not test ${SEMAPHORE_GIT_TAG_NAME}"
    fi
    PACKAGE_VERSION="${SEMAPHORE_GIT_TAG_NAME#python/v}"
}

# Sets the upload URL, token key and artifact directory of ${PYPI_INDEX}.
index_settings() {
    case "${PYPI_INDEX:-}" in
        https://test.pypi.org)
            upload_url=https://test.pypi.org/legacy/
            token_key=test_pypi_token
            ;;
        https://pypi.org)
            upload_url=https://upload.pypi.org/legacy/
            token_key=pypi_token
            # A pre-release or a dev version stops at Test PyPI, whose
            # versions never reach users of PyPI. The promotion's `run`
            # condition already skips their tags; the check is repeated here,
            # before the token is loaded.
            if [[ "${PACKAGE_VERSION}" =~ \.dev[0-9]+$ ]]; then
                refuse "${PACKAGE_VERSION} is a dev version, which is published to Test PyPI only"
            fi
            if [[ "${PACKAGE_VERSION}" =~ (a|b|rc)[0-9]+$ ]]; then
                refuse "${PACKAGE_VERSION} is a pre-release, which is published to Test PyPI only"
            fi
            ;;
        *)
            refuse "PYPI_INDEX is '${PYPI_INDEX:-}', not https://test.pypi.org or https://pypi.org"
            ;;
    esac
    artifacts="pypi/${PYPI_INDEX#https://}"
}

# Creates an empty ${work}, with the directory of the wheels to upload.
new_work() {
    if ! { rm -rf "${work}" && mkdir -p "${work}/upload"; }; then
        refuse "could not create ${work}"
    fi
}

# Because a version on PyPI can never be replaced, the tag must still point at
# the commit this build tested, and that commit must be part of master.
# Checking immediately before the upload also catches a tag that was moved or
# deleted while CI ran. `git ls-remote` reports a lightweight tag as the commit
# itself, and an annotated tag as the tag object followed by `<tag>^{}`, the
# commit it references.
check_tag() {
    local head_sha remote_tag tag_sha
    head_sha=$(git rev-parse HEAD) || refuse "could not resolve HEAD"
    remote_tag=$(git ls-remote origin "refs/tags/${SEMAPHORE_GIT_TAG_NAME}" "refs/tags/${SEMAPHORE_GIT_TAG_NAME}^{}") \
        || refuse "could not read tag ${SEMAPHORE_GIT_TAG_NAME} from origin"
    tag_sha=$(printf '%s\n' "${remote_tag}" | awk '$2 ~ /\^\{\}$/ { peeled = $1 } $1 != "" { direct = $1 } END { print (peeled != "" ? peeled : direct) }')
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

package() {
    release_version
    echo "Packaging ${PYPI_PACKAGE} ${PACKAGE_VERSION}"
    new_work
    check_tag

    # The tag must match [project] version in python/pyproject.toml at the
    # tagged commit, which must also name the project this script publishes.
    local manifest manifest_name manifest_version
    manifest=$(/usr/bin/python3 -c 'import sys, tomllib; p = tomllib.load(open("python/pyproject.toml", "rb"))["project"]; n, v = p.get("name"), p.get("version"); print(n, v) if isinstance(n, str) and isinstance(v, str) else sys.exit("[project] name and version must be literal strings")') \
        || refuse "could not read [project] name and version from python/pyproject.toml"
    read -r manifest_name manifest_version <<< "${manifest}"
    if [ "${manifest_name}" != "${PYPI_PACKAGE}" ]; then
        refuse "python/pyproject.toml at this commit names the project '${manifest_name}', not ${PYPI_PACKAGE}"
    fi
    if [ "${PACKAGE_VERSION}" != "${manifest_version}" ]; then
        refuse "tag ${SEMAPHORE_GIT_TAG_NAME} names version '${PACKAGE_VERSION}', but python/pyproject.toml at this commit has '${manifest_version}'; tag the commit that sets the version"
    fi

    # Proceeds only if the wheels recorded by the wheel blocks are exactly the
    # wheels the wheel verification blocks tested, all of this version, one
    # set per platform. Prints `<sha256>  <wheel>` for each, and copies them
    # into a directory of their own.
    artifact pull workflow pypi/wheels --destination "${work}/wheels" --force >/dev/null \
        || refuse "this workflow has no wheels for ${PYPI_PACKAGE} ${PACKAGE_VERSION}"
    artifact pull workflow pypi/tested --destination "${work}/tested" --force >/dev/null \
        || refuse "this workflow has no tested checksums for ${PYPI_PACKAGE} ${PACKAGE_VERSION}"
    /usr/bin/python3 - "${work}/wheels" "${work}/tested" "${work}/upload" "${PACKAGE_VERSION}" "${WHEEL_PLATFORMS}" > "${work}/upload.sha256" <<'PYTHON' \
        || refuse "the recorded wheels are not the wheels that were tested"
import hashlib, os, re, shutil, sys

wheels_root, tested_root, upload_dir, version, platforms = sys.argv[1:6]
# The distribution name in wheel file names (PEP 427).
dist = re.sub(r"[-_.]+", "_", os.environ["PYPI_PACKAGE"])

def find(root, name, directory):
    hits = [os.path.join(parent, name) for parent, dirs, files in os.walk(root) if name in (dirs if directory else files)]
    if len(hits) != 1:
        sys.exit(f"expected one {'directory' if directory else 'file'} named {name} in the recorded artifacts, found {len(hits)}")
    return hits[0]

def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()

problems = []
for platform in platforms.split():
    wheel_dir = find(wheels_root, platform, True)
    tested = {}
    for line in open(find(tested_root, platform, False)):
        if line.strip():
            checksum, name = line.split()
            tested[name] = checksum
    present = sorted(os.listdir(wheel_dir))
    if present != sorted(tested):
        problems.append(f"{platform}: the recorded wheels {present} are not the tested wheels {sorted(tested)}")
        continue
    for name in present:
        path = os.path.join(wheel_dir, name)
        if not (name.startswith(f"{dist}-{version}-") and name.endswith(".whl")):
            problems.append(f"{platform}: {name} is not a {dist} {version} wheel")
        elif sha256(path) != tested[name]:
            problems.append(f"{platform}: {name} is not the wheel that was tested")
        elif os.path.exists(os.path.join(upload_dir, name)):
            problems.append(f"{platform}: {name} was also recorded for another platform")
        else:
            shutil.copy(path, upload_dir)
            print(f"{tested[name]}  {name}")
if problems:
    sys.exit("\n".join(problems))
PYTHON

    # `twine check` checks the metadata of each wheel as the index will,
    # including that the long description (python/README.md) renders.
    python -m pip install twine==6.1.0 || refuse "could not install twine"
    python -m twine check "${work}/upload"/*.whl || refuse "twine check failed"

    # SHA256SUMS is recorded last: `prepare` refuses without it, so a package
    # job interrupted while recording the wheels publishes nothing.
    artifact push workflow "${work}/upload" --destination pypi/release/wheels --force \
        || refuse "could not record the packaged wheels; rerun this job"
    artifact push workflow "${work}/upload.sha256" --destination pypi/release/SHA256SUMS --force \
        || refuse "could not record the checksums of the packaged wheels; rerun this job"
    cat "${work}/upload.sha256"
    echo "Packaged $(grep -c . "${work}/upload.sha256") wheels of ${PYPI_PACKAGE} ${PACKAGE_VERSION}, identical to the wheels that were tested"
}

prepare() {
    release_version
    index_settings
    echo "${PYPI_PACKAGE} ${PACKAGE_VERSION} is published to ${PYPI_INDEX}"
    new_work
    check_tag

    # A version already on the index, whether yanked or not, cannot be
    # published again, and PyPI refuses a file name that was ever used, even
    # after its file was deleted. The exception is a version whose upload this
    # workflow has already started: `upload` records upload-started before
    # uploading, so a rerun after an interrupted upload can complete it. A
    # version need not be higher than the ones already published: as with
    # Apache Kafka, a patch of an older minor version can follow a newer minor
    # version (4.4.101 after 4.5.0).
    local listing
    listing=$(simple_index) \
        || refuse "could not read ${PYPI_INDEX} to confirm that ${PYPI_PACKAGE} ${PACKAGE_VERSION} is new"
    if printf '%s\n' "${listing}" | awk -v v="${PACKAGE_VERSION}" '$1 == "version" && $2 == v { found = 1 } END { exit !found }'; then
        if artifact pull workflow "${artifacts}/upload-started" --destination "${work}/upload-started" --force >/dev/null 2>&1; then
            echo "${PYPI_PACKAGE} ${PACKAGE_VERSION} is already on ${PYPI_INDEX} and this workflow started its upload; continuing to finish that publish"
        else
            refuse "${PYPI_PACKAGE} ${PACKAGE_VERSION} is already on ${PYPI_INDEX}, and a published version cannot be replaced; bump [project] version in python/pyproject.toml and tag that commit"
        fi
    else
        echo "${PYPI_PACKAGE} ${PACKAGE_VERSION} is not yet on ${PYPI_INDEX}"
    fi

    # Proceeds only if the recorded release holds exactly the wheels that
    # `package` listed in SHA256SUMS, with those checksums. Prints
    # `<sha256>  <wheel>` for each, and copies them for the upload.
    artifact pull workflow pypi/release/SHA256SUMS --destination "${work}/SHA256SUMS" --force >/dev/null \
        || refuse "this workflow has not packaged ${PYPI_PACKAGE} ${PACKAGE_VERSION}; \"Release (Python): packaging\" must pass first"
    artifact pull workflow pypi/release/wheels --destination "${work}/release" --force >/dev/null \
        || refuse "this workflow has no packaged wheels for ${PYPI_PACKAGE} ${PACKAGE_VERSION}"
    /usr/bin/python3 - "${work}/release" "${work}/SHA256SUMS" "${work}/upload" "${PACKAGE_VERSION}" > "${work}/upload.sha256" <<'PYTHON' \
        || refuse "the wheels to upload are not the packaged wheels"
import hashlib, os, re, shutil, sys

release_dir, sums_file, upload_dir, version = sys.argv[1:5]
dist = re.sub(r"[-_.]+", "_", os.environ["PYPI_PACKAGE"])

def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()

sums = {}
for line in open(sums_file):
    if line.strip():
        checksum, name = line.split()
        sums[name] = checksum
paths = sorted(os.path.join(parent, f) for parent, dirs, files in os.walk(release_dir) for f in files)
names = sorted(os.path.basename(p) for p in paths)
if not sums:
    sys.exit("SHA256SUMS lists no wheel")
if names != sorted(sums):
    sys.exit(f"the packaged wheels {names} are not the wheels SHA256SUMS lists {sorted(sums)}")
problems = []
for path in paths:
    name = os.path.basename(path)
    if not (name.startswith(f"{dist}-{version}-") and name.endswith(".whl")):
        problems.append(f"{name} is not a {dist} {version} wheel")
    elif sha256(path) != sums[name]:
        problems.append(f"{name} is not the wheel that was packaged")
    else:
        shutil.copy(path, upload_dir)
        print(f"{sums[name]}  {name}")
if problems:
    sys.exit("\n".join(problems))
PYTHON

    # PyPI receives exactly the wheels that passed the post-publish checks on
    # Test PyPI, which the last Test PyPI block recorded once they passed. The
    # "Publish to PyPI" promotion can be started even from a failed pipeline,
    # so this check, not the order of the blocks, is what guarantees it.
    if [ "${PYPI_INDEX}" = https://pypi.org ]; then
        artifact pull workflow pypi/test.pypi.org/verified --destination "${work}/test-pypi-verified" --force >/dev/null \
            || refuse "${PYPI_PACKAGE} ${PACKAGE_VERSION} has not passed the post-publish checks on Test PyPI in this workflow; the Test PyPI blocks of this tag build must pass first"
        cmp -s <(sort "${work}/upload.sha256") <(sort "${work}/test-pypi-verified") \
            || refuse "the wheels to upload to PyPI are not the wheels that passed the checks on Test PyPI"
    fi
    cat "${work}/upload.sha256"
    echo "$(grep -c . "${work}/upload.sha256") wheels to upload to ${PYPI_INDEX}, identical to the wheels that were tested"

    # Installed before the token is loaded; twine is the only code that runs
    # while it is.
    python -m pip install twine==6.1.0 || refuse "could not install twine"
}

upload() {
    release_version
    index_settings
    [ -s "${work}/upload.sha256" ] || refuse "nothing to upload; run '$0 prepare' first"

    export TWINE_USERNAME=__token__
    # shellcheck disable=SC1091 # provided by the Semaphore agent
    . vault-get-secret --secret-key="${token_key}" --export-as=TWINE_PASSWORD "pypi/${PYPI_PACKAGE}"
    [ -n "${TWINE_PASSWORD:-}" ] || refuse "Vault returned no ${token_key} for pypi/${PYPI_PACKAGE}"

    # Recorded before the upload, so that a rerun after an interrupted upload
    # passes the "already on the index" check of `prepare`.
    if ! { echo "${SEMAPHORE_JOB_ID:-}" > "${work}/upload-started" \
        && artifact push workflow "${work}/upload-started" --destination "${artifacts}/upload-started" --force; }; then
        refuse "could not record that the upload started"
    fi

    # The index answers a re-upload of a file it already has with an error
    # unless the bytes are identical. twine may also report an error for an
    # upload that the index accepted. In either case, the publish is treated as
    # successful only if the index lists every wheel with the checksum that
    # was tested; the post-publish checksum block then checks the files the
    # index serves.
    if ! python -m twine upload --non-interactive --repository-url "${upload_url}" "${work}/upload"/*.whl; then
        echo "twine upload failed; checking whether ${PYPI_INDEX} has every wheel anyway"
        local listed="" attempt
        for attempt in $(seq 1 10); do
            if simple_index \
                | awk 'NR == FNR { want[$2] = $1; next } $1 == "file" && ($3 in want) && want[$3] == $2 { seen[$3] = 1 } END { for (w in want) if (!(w in seen)) exit 1 }' "${work}/upload.sha256" -; then
                listed=yes
                break
            fi
            echo "attempt ${attempt}/10: ${PYPI_INDEX} does not list every wheel yet; retrying in 30s"
            sleep 30
        done
        if [ -z "${listed}" ]; then
            refuse "twine upload failed and ${PYPI_INDEX} does not list every wheel with the checksum that was tested"
        fi
        echo "${PYPI_INDEX} lists exactly these wheels; continuing"
    fi
    unset TWINE_PASSWORD

    # Records the uploaded wheels for the post-publish blocks.
    artifact push workflow "${work}/upload.sha256" --destination "${artifacts}/uploaded.sha256" --force \
        || refuse "could not record the uploaded wheels; rerun this job to record them"
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
