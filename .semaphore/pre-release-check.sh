#!/bin/sh
#
# Copyright 2025 Confluent Inc.
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
# Pre-release validation for the confluent-kafka4 Python binding.
#
# A pre-publish tripwire that fails the pipeline if the things that must be
# consistent for a release are not. For us that is a small set: our version
# lives only in bindings/python/pyproject.toml and we have no external native
# dependency to pin, so (unlike confluent-kafka-python's richer check, which also
# cross-checks a C version constant / soak lists / a librdkafka RC pin) we only
# assert:
#
#   1. the package version is a well-formed X.Y.Z[suffix] release version, and
#   2. when building on a release tag, the tag matches that version.
#
# On a non-tag build (the manual-promotion flow) the tag check is skipped, so the
# script is a no-op gate there and a real gate on a tagged publish run.
#
# See design/current/python-wheel-ci-design.md ("Pre-release check").

set -e

pyproject=bindings/python/pyproject.toml
version=$(python3 -c "import tomllib; print(tomllib.load(open('$pyproject','rb'))['project']['version'])")
echo "pyproject.toml version = $version"

# 1. Version must be X.Y.Z, optionally with a PEP 440 pre/dev/post suffix.
if ! printf '%s' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([._-]?(a|b|rc|dev|post)[0-9]+)?$'; then
    echo "FAIL: version '$version' is not a valid X.Y.Z[suffix] release version" >&2
    exit 1
fi
echo "OK:   version '$version' is well-formed"

# 2. On a release tag, the tag must match the version (allow an optional 'v').
tag="${SEMAPHORE_GIT_TAG_NAME:-}"
if [ -z "$tag" ]; then
    echo "OK:   no release tag (SEMAPHORE_GIT_TAG_NAME unset) -- skipping tag/version match"
    exit 0
fi
tag_version=${tag#v}
if [ "$tag_version" != "$version" ]; then
    echo "FAIL: release tag '$tag' does not match pyproject.toml version '$version'" >&2
    exit 1
fi
echo "OK:   release tag '$tag' matches pyproject.toml version '$version'"
