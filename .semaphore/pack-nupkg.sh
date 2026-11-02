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
# Packs the Confluent.Kafka NuGet package with the native library of each RID, then checks it
# with check-nupkg.py. Used by the "Release (.NET): pack" block of semaphore.yml and by
# `make package-check-dotnet`. Must be POSIX sh, with `dotnet` (SDK 10) on PATH or in
# $HOME/.dotnet, and python3.
#
# Usage, from the repository root:
#   .semaphore/pack-nupkg.sh <natives-dir> <out-dir> [<rid> ...]
#       <natives-dir> holds <rid>/<library> for each RID and nothing else; the RIDs default to
#       linux-x64 linux-arm64 osx-x64 osx-arm64. Each library must pass check-nupkg.py's native
#       checks, platform floors included. The package sources (dotnet/src and what it builds with)
#       must be those of HEAD. Writes Confluent.Kafka.<version>.nupkg into <out-dir>, which must
#       not exist or be empty, and prints its SHA-256.
#   .semaphore/pack-nupkg.sh --dev <out-dir>
#       Packs this machine's own release build (rust/target/release, from
#       `cargo build --features ffi --release`) as the one RID of this machine, without the
#       platform floor checks: a package to test the packaging, never one to release. On macOS
#       its copy gets the @rpath install name a release build has.
#
# The version is <Version> in dotnet/Directory.Build.props, and the package names the commit
# HEAD, which must be the commit the libraries were built from.

set -eu

ALL_RIDS="linux-x64 linux-arm64 osx-x64 osx-arm64"
PROJECT=dotnet/src/Confluent.Kafka/Confluent.Kafka.csproj

die() {
    echo "pack-nupkg: $*" >&2
    exit 1
}

usage() {
    echo "Usage: $0 <natives-dir> <out-dir> [<rid> ...]" >&2
    echo "       $0 --dev <out-dir>" >&2
    exit 2
}

# The RID of this machine.
host_rid() {
    case "$(uname -s)-$(uname -m)" in
        Linux-x86_64) echo linux-x64 ;;
        Linux-aarch64|Linux-arm64) echo linux-arm64 ;;
        Darwin-x86_64) echo osx-x64 ;;
        Darwin-arm64) echo osx-arm64 ;;
        *) die "no RID for $(uname -s) $(uname -m)" ;;
    esac
}

library_of() {
    case "$1" in
        linux-*) echo libconfluent_kafka.so ;;
        osx-*) echo libconfluent_kafka.dylib ;;
        *) die "$1 is not one of: $ALL_RIDS" ;;
    esac
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

[ -f "$PROJECT" ] || die "run from the repository root"
if [ -x "$HOME/.dotnet/dotnet" ]; then
    # Where .semaphore/install-dotnet.sh installs the SDK.
    export DOTNET_ROOT="$HOME/.dotnet" PATH="$HOME/.dotnet:$PATH" DOTNET_MULTILEVEL_LOOKUP=0
fi
command -v dotnet >/dev/null 2>&1 || die "dotnet is not on PATH; run .semaphore/install-dotnet.sh"
export DOTNET_NOLOGO=1 DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1

stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT INT TERM

if [ "${1:-}" = "--dev" ]; then
    [ $# -eq 2 ] || usage
    out="$2"
    dev="--dev"
    rids=$(host_rid)
    lib=$(library_of "$rids")
    [ -f "rust/target/release/$lib" ] || die "rust/target/release/$lib does not exist; run \`make -C rust build\` first"
    mkdir -p "$stage/$rids"
    cp "rust/target/release/$lib" "$stage/$rids/"
    if [ "${rids%%-*}" = osx ]; then
        # The install name .semaphore/build-ffi-library.sh gives a release build, instead of the
        # absolute path of this machine's target directory. Changing it can invalidate the
        # signature the linker made, and Apple silicon loads only signed code: sign again, ad hoc.
        install_name_tool -id "@rpath/$lib" "$stage/$rids/$lib" 2>/dev/null
        codesign --verify --strict "$stage/$rids/$lib" 2>/dev/null \
            || codesign --force --sign - "$stage/$rids/$lib"
    fi
else
    [ $# -ge 2 ] || usage
    natives="$1"
    out="$2"
    shift 2
    dev=""
    rids="${*:-$ALL_RIDS}"
    [ -d "$natives" ] || die "$natives is not a directory"
    # Each RID's directory holds its library and nothing else, and there is no other directory,
    # so nothing the native blocks did not build can reach the package.
    for entry in "$natives"/* "$natives"/.[!.]*; do
        [ -e "$entry" ] || continue
        name=$(basename "$entry")
        case " $rids " in
            *" $name "*) ;;
            *) die "$natives holds $name, which is not one of the RIDs to pack: $rids" ;;
        esac
    done
    for rid in $rids; do
        lib=$(library_of "$rid")
        [ -f "$natives/$rid/$lib" ] || die "$natives/$rid/$lib does not exist"
        listing=$(ls -A "$natives/$rid")
        [ "$listing" = "$lib" ] || die "$natives/$rid must hold only $lib, but holds: $(printf "%s" "$listing" | tr "\n" " ")"
        mkdir -p "$stage/$rid"
        cp "$natives/$rid/$lib" "$stage/$rid/"
    done
fi

for rid in $rids; do
    python3 .semaphore/check-nupkg.py native "$rid" "$stage/$rid/$(library_of "$rid")" $dev
done

if [ -e "$out" ] && [ -n "$(ls -A "$out")" ]; then
    die "$out is not empty"
fi
mkdir -p "$out"

version=$(python3 .semaphore/check-nupkg.py version)
commit=$(git rev-parse HEAD)
# The package names the commit, so a release package is packed only from the files of that commit:
# no uncommitted change and no untracked file among the sources the pack reads.
if [ -z "$dev" ]; then
    changes=$(git status --porcelain -- dotnet/src dotnet/Directory.Build.props dotnet/.editorconfig \
        dotnet/Confluent.Kafka.snk dotnet/README.md dotnet/icon.png)
    [ -z "$changes" ] || die "the package sources differ from $commit: $(printf "%s" "$changes" | tr "\n" " ")"
fi
echo "Packing Confluent.Kafka $version at $commit with the natives of: $rids"

# ContinuousIntegrationBuild maps the PDB source paths to the repository's, so no path of this
# machine is embedded in the assemblies.
dotnet pack "$PROJECT" -c Release --nologo -o "$out" \
    -p:NativeAssetsDir="$stage" -p:ContinuousIntegrationBuild=true

package="$out/Confluent.Kafka.$version.nupkg"
listing=$(ls -A "$out")
[ "$listing" = "Confluent.Kafka.$version.nupkg" ] || die "the pack wrote $(printf "%s" "$listing" | tr "\n" " "), not Confluent.Kafka.$version.nupkg alone"

python3 .semaphore/check-nupkg.py package "$package" --version "$version" --rids "$rids" --commit "$commit" $dev
echo "$(sha256_of "$package")  Confluent.Kafka.$version.nupkg"
