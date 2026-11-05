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
# Tests the npm packages of the Node.js client as a user installs them, the counterpart of
# test-nupkg.sh, on Linux and macOS. A project outside the repository, with an empty npm cache,
# installs the main package and this machine's platform package from their tarballs, then runs
# the smoke program (javascript/tools/package-smoke) in it with this machine's Node.js. Must be
# POSIX sh, with node, npm and python3 on PATH.
#
# Nothing is read from a registry: npm's registry is an address where nothing listens. The
# main package has no dependencies besides the optional platform packages, of which the one of
# this machine is installed from its tarball; npm fails to fetch the two others, which it would
# skip on this machine anyway, and skips them as optional dependencies. The installed tree is
# then checked to hold exactly the two packages, from the tarballs, byte for byte.
#
# Usage, from the repository root:
#   .semaphore/test-npm.sh <tarball-dir> <platform> [<checksum-file>]
#       Tests the tarballs in <tarball-dir>, which must hold the main package and the platform
#       package of <platform>, this machine's, and may hold other platform packages; their
#       version is check-npm.py version's. With <checksum-file>, writes `<sha256>  <tarball>` of
#       every tarball in <tarball-dir>, sorted, once every test has passed: the checksums are
#       taken before the tests, so the file names the bytes that were tested.

set -eu

die() {
    echo "test-npm: $*" >&2
    exit 1
}

usage() {
    echo "Usage: $0 <tarball-dir> <platform> [<checksum-file>]" >&2
    exit 2
}

# Without a pipeline, so that a failing hash fails the caller's assignment.
sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        _sum=$(sha256sum "$1") || return 1
    else
        _sum=$(shasum -a 256 "$1") || return 1
    fi
    _sum=${_sum%% *}
    [ -n "$_sum" ] || return 1
    echo "$_sum"
}

host_platform() {
    case "$(uname -s)-$(uname -m)" in
        Linux-x86_64) echo linux-x64-gnu ;;
        Linux-aarch64|Linux-arm64) echo linux-arm64-gnu ;;
        Darwin-arm64) echo darwin-arm64 ;;
        *) die "no platform package for $(uname -s) $(uname -m)" ;;
    esac
}

[ $# -ge 2 ] && [ $# -le 3 ] || usage
dir="$1"
platform="$2"
CHECKSUM_FILE="${3:-}"
[ -f javascript/tools/package-smoke/package-smoke.js ] || die "run from the repository root"
[ -d "$dir" ] || die "$dir is not a directory"
case "$CHECKSUM_FILE" in
    "$dir"/*) die "<checksum-file> must be outside <tarball-dir>" ;;
esac
[ "$platform" = "$(host_platform)" ] || die "$platform is not this machine's platform, $(host_platform)"
command -v node >/dev/null 2>&1 || die "node is not on PATH"
command -v npm >/dev/null 2>&1 || die "npm is not on PATH"

version=$(python3 .semaphore/check-npm.py version)
main="confluentinc-kafka-javascript-$version.tgz"
mine="confluentinc-kafka-javascript-$platform-$version.tgz"
[ -f "$dir/$main" ] || die "$dir has no $main"
[ -f "$dir/$mine" ] || die "$dir has no $mine"
for file in "$dir"/* "$dir"/.[!.]*; do
    [ -e "$file" ] || continue
    case "$(basename "$file")" in
        confluentinc-kafka-javascript-*"-$version.tgz"|"$main") ;;
        *) die "$dir holds $(basename "$file"), which is not a tarball of version $version" ;;
    esac
done

# Recorded before the tests, and written only if they all pass.
checksums=""
for file in "$dir"/*.tgz; do
    sum=$(sha256_of "$file") || die "could not hash $file"
    checksums="$checksums$sum  $(basename "$file")
"
done
checksums=$(printf '%s' "$checksums" | LC_ALL=C sort -k2)

# A physical path: the smoke test compares the path of the loaded addon with it.
work=$(mktemp -d /tmp/test-npm.XXXXXX)
work=$(cd "$work" && pwd -P)
# A signal ends the script with a failure, after the EXIT trap has removed $work.
trap 'rm -rf "$work"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir "$work/tarballs" "$work/project"
cp "$dir/$main" "$dir/$mine" "$work/tarballs/"

# An empty cache and no user or global configuration, so nothing cached or configured earlier
# can be used; a registry where nothing listens (port 9, discard), with no retries.
export npm_config_cache="$work/cache" npm_config_userconfig="$work/npmrc" npm_config_globalconfig="$work/npmrc-global"
export npm_config_registry=http://127.0.0.1:9/ npm_config_fetch_retries=0 npm_config_update_notifier=false
export npm_config_audit=false npm_config_fund=false
: > "$work/npmrc"
: > "$work/npmrc-global"
echo '{"name": "package-smoke", "version": "1.0.0", "private": true}' > "$work/project/package.json"
echo "Installing @confluentinc/kafka-javascript $version and its $platform package into $work/project"
(cd "$work/project" && npm install --ignore-scripts --no-audit --no-fund "$work/tarballs/$main" "$work/tarballs/$mine") \
    || die "npm install of the tarballs failed"

# The installed tree is exactly the two packages, from the tarballs under test.
python3 - "$work/project" "$work/tarballs" "$platform" <<'PYTHON' || die "the installed tree is not the two tarballs"
import base64, hashlib, json, os, sys

project, tarballs, platform = sys.argv[1:]
lock = json.load(open(os.path.join(project, "package-lock.json")))
names = {"node_modules/@confluentinc/kafka-javascript": "confluentinc-kafka-javascript-",
         f"node_modules/@confluentinc/kafka-javascript-{platform}": f"confluentinc-kafka-javascript-{platform}-"}
packages = {key: value for key, value in lock["packages"].items() if key}
if set(packages) != set(names):
    sys.exit(f"the project installed {sorted(packages)}, not {sorted(names)}")
for key, entry in packages.items():
    if entry.get("version") is None or not entry.get("resolved", "").startswith("file:"):
        sys.exit(f"{key} is not installed from a tarball: {entry}")
    tarball = os.path.normpath(os.path.join(project, entry["resolved"][len("file:"):]))
    if os.path.dirname(tarball) != tarballs or not os.path.basename(tarball).startswith(names[key]):
        sys.exit(f"{key} is installed from {tarball}")
    with open(tarball, "rb") as f:
        integrity = "sha512-" + base64.b64encode(hashlib.sha512(f.read()).digest()).decode()
    if entry.get("integrity") != integrity:
        sys.exit(f"{key} has the integrity {entry.get('integrity')}, not {integrity}, that of {tarball}")
installed = sorted(os.listdir(os.path.join(project, "node_modules")))
if installed != [".package-lock.json", "@confluentinc"]:
    sys.exit(f"node_modules holds {installed}")
scoped = sorted(os.listdir(os.path.join(project, "node_modules", "@confluentinc")))
if scoped != sorted(key.split("/")[-1] for key in names):
    sys.exit(f"node_modules/@confluentinc holds {scoped}")
print(f"installed {', '.join(sorted(names))} from {tarballs}")
PYTHON

echo "== smoke test, Node.js $(node --version), on this machine =="
node javascript/tools/package-smoke/package-smoke.js --platform "$platform" --version "$version" --project "$work/project"

if [ -n "$CHECKSUM_FILE" ]; then
    printf '%s\n' "$checksums" > "$CHECKSUM_FILE"
    echo "Tested:"
    cat "$CHECKSUM_FILE"
fi
echo "@confluentinc/kafka-javascript $version passed every test on $platform"
