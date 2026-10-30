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
# Tests the Confluent.Kafka NuGet package as a user receives it, the counterpart of
# test-wheels.sh. The smoke project (dotnet/tools/PackageSmoke), a copy outside the repository,
# references the package by its exact version from that one feed (packageSourceMapping), with
# an empty NuGet cache, so nothing but the package can be used. It is built on this machine and
# run for net8.0 and net10.0: on macOS on the runner, on Linux on the runner and then in a
# container of each distribution of DISTRO_IMAGES, which mounts the runner's .NET install and
# the build. A self-contained `dotnet publish` for this machine's RID must carry the native
# library and run. With a package directory, the binding's unit tests then run against the
# package too, on the runner: the unit-test project's -p:ConfluentKafkaPackageVersion swaps its
# project reference for the package, so the assembly and the native library both come from it.
# Must be POSIX sh. Needs .NET SDK 10 and the .NET 8 runtime (.semaphore/install-dotnet.sh), in
# $HOME/.dotnet or on PATH, and Docker on Linux.
#
# Usage, from the repository root:
#   .semaphore/test-nupkg.sh <package-dir> <abi-header> [<checksum-file>]
#       Tests the package in <package-dir>, which must hold Confluent.Kafka.<version>.nupkg and
#       nothing else. <abi-header> is the confluent_kafka_bindings.h of the native library this
#       machine loads, for the unit tests' ABI conformance test. With <checksum-file>, writes
#       `<sha256>  <package>` to it once every test has passed.
#   .semaphore/test-nupkg.sh --source <nuget-v3-index-url> <version>
#       Restores <version> from that gallery instead, and runs the smoke tests only.
#   .semaphore/test-nupkg.sh --smoke <package-dir>
#       Runs the smoke tests on this machine only (make package-check-dotnet).
# Override the distribution list (Linux) with DISTRO_IMAGES="img1 img2 ...".

set -eu

TFMS="net8.0 net10.0"

die() {
    echo "test-nupkg: $*" >&2
    exit 1
}

usage() {
    echo "Usage: $0 <package-dir> <abi-header> [<checksum-file>]" >&2
    echo "       $0 --source <nuget-v3-index-url> <version>" >&2
    echo "       $0 --smoke <package-dir>" >&2
    exit 2
}

retry() {
    _n=1
    while [ "$_n" -le 5 ]; do
        if "$@"; then return 0; fi
        [ "$_n" -eq 5 ] && break
        echo "retry: attempt $_n/5 of '$*' failed; sleeping 30s" >&2
        _n=$((_n + 1))
        sleep 30
    done
    echo "retry: '$*' failed after 5 attempts" >&2
    return 1
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

host_rid() {
    case "$(uname -s)-$(uname -m)" in
        Linux-x86_64) echo linux-x64 ;;
        Linux-aarch64|Linux-arm64) echo linux-arm64 ;;
        Darwin-x86_64) echo osx-x64 ;;
        Darwin-arm64) echo osx-arm64 ;;
        *) die "no RID for $(uname -s) $(uname -m)" ;;
    esac
}

# The single Confluent.Kafka.<version>.nupkg in $1; sets PACKAGE and VERSION.
find_package() {
    [ -d "$1" ] || die "$1 is not a directory"
    listing=$(ls -A "$1")
    case "$listing" in
        Confluent.Kafka.*.nupkg) ;;
        *) die "$1 must hold Confluent.Kafka.<version>.nupkg and nothing else, but holds: $(printf "%s" "$listing" | tr "\n" " ")" ;;
    esac
    PACKAGE="$1/$listing"
    VERSION="${listing#Confluent.Kafka.}"
    VERSION="${VERSION%.nupkg}"
}

# Writes the nuget.config of a project in $1 that takes Confluent.Kafka from $2 only and every
# other package from nuget.org.
write_nuget_config() {
    cat > "$1/nuget.config" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="confluent-kafka" value="$2" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="confluent-kafka">
      <package pattern="Confluent.Kafka" />
    </packageSource>
    <packageSource key="nuget.org">
      <package pattern="*" />
    </packageSource>
  </packageSourceMapping>
  <fallbackPackageFolders>
    <clear />
  </fallbackPackageFolders>
</configuration>
EOF
}

mode=full
case "${1:-}" in
    --source)
        [ $# -eq 3 ] || usage
        mode=source
        FEED="$2"
        VERSION="$3"
        PACKAGE=""
        HEADER=""
        CHECKSUM_FILE=""
        ;;
    --smoke)
        [ $# -eq 2 ] || usage
        mode=smoke
        find_package "$2"
        FEED=""
        HEADER=""
        CHECKSUM_FILE=""
        ;;
    ""|-*)
        usage
        ;;
    *)
        [ $# -ge 2 ] && [ $# -le 3 ] || usage
        find_package "$1"
        FEED=""
        HEADER="$2"
        CHECKSUM_FILE="${3:-}"
        [ -f "$HEADER" ] || die "$HEADER does not exist"
        case "$CHECKSUM_FILE" in
            "$1"/*) die "<checksum-file> must be outside <package-dir>" ;;
        esac
        ;;
esac
[ -f dotnet/tools/PackageSmoke/PackageSmoke.csproj ] || die "run from the repository root"
python3 - "$VERSION" <<'PYTHON' || die "'$VERSION' is not a release version"
import re, sys
sys.exit(re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(alpha|beta|dev|rc)\.(0|[1-9][0-9]*))?", sys.argv[1]) is None)
PYTHON

if [ -x "$HOME/.dotnet/dotnet" ]; then
    # Where .semaphore/install-dotnet.sh installs .NET.
    export DOTNET_ROOT="$HOME/.dotnet" PATH="$HOME/.dotnet:$PATH"
fi
command -v dotnet >/dev/null 2>&1 || die "dotnet is not on PATH; run .semaphore/install-dotnet.sh"
# The directory of the dotnet executable itself, which a distribution's /usr/bin/dotnet links to.
DOTNET_ROOT="${DOTNET_ROOT:-$(python3 -c 'import os, sys; print(os.path.dirname(os.path.realpath(sys.argv[1])))' "$(command -v dotnet)")}"
export DOTNET_ROOT DOTNET_MULTILEVEL_LOOKUP=0 DOTNET_NOLOGO=1 DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
for runtime in 8.0. 10.0.; do
    dotnet --list-runtimes | grep -q "^Microsoft.NETCore.App $runtime" \
        || die "the .NET $runtime runtime is not installed in $DOTNET_ROOT; run .semaphore/install-dotnet.sh"
done

# Recorded before the tests, and written only if they all pass.
checksum=""
if [ -n "$PACKAGE" ]; then
    sum=$(sha256_of "$PACKAGE") || die "could not hash $PACKAGE"
    checksum="$sum  $(basename "$PACKAGE")"
fi

# A physical path: the smoke test compares the path of the loaded native library with it.
work=$(mktemp -d /tmp/test-nupkg.XXXXXX)
work=$(cd "$work" && pwd -P)
# A signal ends the script with a failure, after the EXIT trap has removed $work.
trap 'rm -rf "$work"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# An empty NuGet cache, so that a package of the same version cached earlier cannot be used.
export NUGET_PACKAGES="$work/packages" NUGET_HTTP_CACHE_PATH="$work/http-cache"
# A gallery is read without NuGet's HTTP cache, so that a retry sees a version it has just started
# to serve.
no_http_cache=""
[ "$mode" = source ] && no_http_cache="--no-http-cache"
if [ -n "$PACKAGE" ]; then
    mkdir "$work/feed"
    cp "$PACKAGE" "$work/feed/"
    FEED="$work/feed"
fi
echo "Testing Confluent.Kafka $VERSION from $FEED"

# The smoke project, outside the repository.
mkdir "$work/smoke"
cp dotnet/tools/PackageSmoke/PackageSmoke.csproj dotnet/tools/PackageSmoke/Program.cs \
    dotnet/tools/PackageSmoke/Directory.Build.props "$work/smoke/"
write_nuget_config "$work/smoke" "$FEED"
(
    cd "$work/smoke"
    retry dotnet restore --configfile nuget.config $no_http_cache -p:ConfluentKafkaVersion="$VERSION"
    dotnet build --no-restore -c Release -p:ConfluentKafkaVersion="$VERSION"
)

# The package restored is the one under test, byte for byte.
restored="$NUGET_PACKAGES/confluent.kafka/$VERSION/confluent.kafka.$VERSION.nupkg"
[ -f "$restored" ] || die "Confluent.Kafka $VERSION was not restored into $NUGET_PACKAGES"
if [ -n "$PACKAGE" ]; then
    cmp -s "$restored" "$PACKAGE" || die "the restored Confluent.Kafka $VERSION is not the package under test"
fi
sum=$(sha256_of "$restored") || die "could not hash $restored"
echo "Restored Confluent.Kafka $VERSION: $sum"

for tfm in $TFMS; do
    echo "== smoke test, $tfm, on this machine =="
    dotnet "$work/smoke/bin/Release/$tfm/PackageSmoke.dll" "$VERSION"
done

if [ "$(uname -s)" = "Linux" ] && [ "$mode" != smoke ]; then
    : "${DISTRO_IMAGES:=almalinux:8 almalinux:9 ubuntu:22.04 debian:12 ubuntu:24.04}"
    for img in $DISTRO_IMAGES; do
        retry docker pull -q "$img"
        for tfm in $TFMS; do
            echo "== smoke test, $tfm, on $img =="
            # The images have no ICU, which the package does not need; a user's image that has
            # none sets the same.
            docker run --rm --user "$(id -u):$(id -g)" \
                -e HOME=/tmp -e DOTNET_ROOT=/opt/dotnet -e DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 \
                -e DOTNET_NOLOGO=1 -e DOTNET_CLI_TELEMETRY_OPTOUT=1 \
                -v "$DOTNET_ROOT":/opt/dotnet:ro -v "$work/smoke":/smoke:ro -w /smoke \
                "$img" /opt/dotnet/dotnet "bin/Release/$tfm/PackageSmoke.dll" "$VERSION"
        done
    done
fi

if [ "$mode" != smoke ]; then
    rid=$(host_rid)
    echo "== self-contained publish for $rid =="
    case "$rid" in
        linux-*) lib=libconfluent_kafka.so ;;
        *) lib=libconfluent_kafka.dylib ;;
    esac
    (
        cd "$work/smoke"
        retry dotnet restore --configfile nuget.config $no_http_cache -r "$rid" -p:ConfluentKafkaVersion="$VERSION"
        dotnet publish --no-restore -c Release -f net10.0 -r "$rid" --self-contained \
            -p:ConfluentKafkaVersion="$VERSION" -o "$work/publish"
    )
    [ -f "$work/publish/$lib" ] || die "the self-contained publish for $rid has no $lib"
    "$work/publish/PackageSmoke" "$VERSION"
fi

if [ "$mode" = full ]; then
    # The unit-test project and what it builds with, without the library's sources or any
    # build output, so the package is the only Confluent.Kafka it can use.
    # Through a file rather than a pipe, so that a failing tar fails the script.
    mkdir "$work/unit"
    tar -C dotnet --exclude bin --exclude obj -cf "$work/unit.tar" Directory.Build.props Confluent.Kafka.snk \
        .editorconfig tests/Confluent.Kafka.UnitTests
    tar -C "$work/unit" -xf "$work/unit.tar"
    write_nuget_config "$work/unit" "$FEED"
    project="$work/unit/tests/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj"
    retry dotnet restore --configfile "$work/unit/nuget.config" "$project" -p:ConfluentKafkaPackageVersion="$VERSION"
    for tfm in $TFMS; do
        echo "== unit tests, $tfm, against the package =="
        dotnet test --no-restore -c Release -f "$tfm" "$project" \
            -p:ConfluentKafkaPackageVersion="$VERSION" -p:AbiHeaderPath="$(cd "$(dirname "$HEADER")" && pwd -P)/$(basename "$HEADER")"
    done
fi

if [ -n "$CHECKSUM_FILE" ]; then
    printf '%s\n' "$checksum" > "$CHECKSUM_FILE"
    echo "Tested:"
    cat "$CHECKSUM_FILE"
fi
echo "Confluent.Kafka $VERSION passed every test"
