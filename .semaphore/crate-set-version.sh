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
# Set the crate version in Cargo.toml from the release tag, so the git tag is
# the single source of truth for the published version. Strips a leading 'v';
# fails unless the tag is X.Y.Z.

set -eu

# On-demand (non-tag) runs keep the placeholder version already in Cargo.toml.
tag="${SEMAPHORE_GIT_TAG_NAME:-}"
if [ -z "$tag" ]; then
    echo "no tag set -- keeping the version already in Cargo.toml"
    grep -m1 '^version' Cargo.toml
    exit 0
fi
version="${tag#v}"

if ! echo "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
    echo "tag '$tag' is not a version tag (expected vX.Y.Z or X.Y.Z)" >&2
    exit 1
fi

# Replace the first top-level `version = "..."` line, which is the [package]
# version (the [workspace] table above it has none, and dependency versions
# are inline `{ version = ... }`, not at the start of a line).
sed -i "0,/^version = .*/s//version = \"$version\"/" Cargo.toml

echo "set crate version to $version (from tag $tag)"
grep -m1 '^version' Cargo.toml
