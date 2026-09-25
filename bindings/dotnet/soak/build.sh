#!/usr/bin/env bash
#
# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Build a pinned version of the Rust client and the .NET soak client the soak
# runs from.
#
# Two source modes, both required:
#   --ref <git-ref>   clone/fetch the repository and check out <git-ref>
#   --src <dir>       build an existing source tree in place
#
# The local-directory mode is not a convenience: the Rust repository is not
# public yet, so on a host that cannot clone it, the sources arrive by scp.
#
# Ordering is mandatory (bindings/dotnet/CLAUDE.md §7.1, firm). `dotnet build`
# consumes target/<profile>/libconfluent_kafka.* -- Confluent.Kafka.csproj copies
# it into the output, and its EnsureNativeLibraryExists target fails the build
# loudly if cargo has not run. So cargo runs first, always.
#
# Ported from bindings/python/soak/build.sh: the source resolution, the cargo
# step, the post-build test gate and the build-manifest writer are the same; the
# venv/pip section becomes a `dotnet build` by path, and there is no
# CONFLUENT_KAFKA_LIB_DIR (the csproj computes the native path itself).

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  build.sh --src <dir>  [--profile release|debug] [--tfm <tfm>]
  build.sh --ref <ref>  [--repo <url|path>] [--workdir <dir>]
                        [--profile release|debug] [--tfm <tfm>]

Options:
  --src <dir>       Build this source tree in place (e.g. an scp'd copy).
  --ref <ref>       Git ref (tag, branch or SHA) to build.
  --repo <url|path> Repository to clone for --ref. Defaults to the origin of
                    the tree this script lives in.
  --workdir <dir>   Where --ref clones to. Default: ./soak-build.
  --profile <p>     cargo profile: release (default) or debug. The .NET build
                    configuration follows it (Release / Debug) -- they must
                    match, because Confluent.Kafka.csproj derives the cargo
                    profile directory from $(Configuration).
  --tfm <tfm>       Target framework to run the unit tests on, and the one
                    run.sh will execute. Default: net10.0.
  --sha <sha>       Commit the sources correspond to. REQUIRED in practice for
                    --src builds of a tree with no .git (an scp'd copy), which
                    is otherwise untraceable.
  --label <text>    Free-form build label recorded alongside the sha.
  -h, --help        This message.

Writes <source>/bindings/dotnet/soak/build-manifest.json (git SHA, toolchain
versions, build time), which the soak client logs at startup so a two-week run
is traceable to an exact commit.
EOF
}

SRC_DIR=""
GIT_REF=""
REPO=""
WORKDIR="$(pwd)/soak-build"
PROFILE="release"
TFM="net10.0"
SHA_OVERRIDE=""
BUILD_LABEL=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --src)     SRC_DIR="$2"; shift 2 ;;
        --ref)     GIT_REF="$2"; shift 2 ;;
        --repo)    REPO="$2"; shift 2 ;;
        --workdir) WORKDIR="$2"; shift 2 ;;
        --profile) PROFILE="$2"; shift 2 ;;
        --tfm)     TFM="$2"; shift 2 ;;
        --sha)     SHA_OVERRIDE="$2"; shift 2 ;;
        --label)   BUILD_LABEL="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ -n "$SRC_DIR" && -n "$GIT_REF" ]]; then
    echo "ERROR: --src and --ref are mutually exclusive" >&2
    exit 2
fi
if [[ -z "$SRC_DIR" && -z "$GIT_REF" ]]; then
    echo "ERROR: one of --src or --ref is required" >&2
    usage >&2
    exit 2
fi
if [[ "$PROFILE" != "release" && "$PROFILE" != "debug" ]]; then
    echo "ERROR: --profile must be 'release' or 'debug'" >&2
    exit 2
fi

# The .NET build configuration MUST match the cargo profile: the csproj maps
# Configuration -> target/<debug|release>/ to find the native.
if [[ "$PROFILE" == "release" ]]; then
    DOTNET_CONFIG="Release"
else
    DOTNET_CONFIG="Debug"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOTNET="${SOAK_DOTNET:-dotnet}"

# ---------------------------------------------------------------------------
# 1. Resolve the source tree
# ---------------------------------------------------------------------------
if [[ -n "$GIT_REF" ]]; then
    if [[ -z "$REPO" ]]; then
        REPO="$(git -C "$SCRIPT_DIR" remote get-url origin 2>/dev/null || true)"
        if [[ -z "$REPO" ]]; then
            echo "ERROR: --ref needs --repo (this tree has no 'origin' remote)" >&2
            exit 2
        fi
    fi
    mkdir -p "$WORKDIR"
    ROOT="$WORKDIR/confluent-kafka-rust"
    if [[ -d "$ROOT/.git" ]]; then
        echo ">>> Fetching $REPO in $ROOT"
        git -C "$ROOT" fetch --all --tags --prune
    else
        echo ">>> Cloning $REPO into $ROOT"
        git clone "$REPO" "$ROOT"
    fi
    echo ">>> Checking out $GIT_REF"
    git -C "$ROOT" checkout --detach "$GIT_REF"
    # Submodules are deliberately NOT initialised: `kafka` is a multi-GB Java
    # source reference and `unity` only backs the C unit tests. Neither is a
    # prerequisite of the cargo ffi build (build.rs generates from the in-repo
    # generator/messages/ specs).
else
    ROOT="$(cd "$SRC_DIR" && pwd)"
    if [[ ! -f "$ROOT/Cargo.toml" ]]; then
        echo "ERROR: $ROOT does not look like the client source tree "\
             "(no Cargo.toml)" >&2
        exit 2
    fi
    echo ">>> Building the source tree at $ROOT in place"
fi

SOAK_DIR="$ROOT/bindings/dotnet/soak"
if [[ ! -d "$SOAK_DIR" ]]; then
    echo "ERROR: $SOAK_DIR not found -- is this source tree too old?" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 2. Build the Rust client with the FFI feature
#
# Produces target/<profile>/libconfluent_kafka.{so,dylib,a} and
# target/include/confluent_kafka.h.
# ---------------------------------------------------------------------------
CARGO_ARGS=(build --features ffi)
[[ "$PROFILE" == "release" ]] && CARGO_ARGS+=(--release)

echo ">>> cargo ${CARGO_ARGS[*]}"
( cd "$ROOT" && cargo "${CARGO_ARGS[@]}" )

LIB_DIR="$ROOT/target/$PROFILE"
if ! ls "$LIB_DIR"/libconfluent_kafka.* >/dev/null 2>&1; then
    echo "ERROR: no libconfluent_kafka.* in $LIB_DIR after the cargo build" >&2
    exit 1
fi
if [[ ! -f "$ROOT/target/include/confluent_kafka.h" ]]; then
    echo "ERROR: target/include/confluent_kafka.h missing after the cargo build" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 3. Build the soak client
#
# By PATH, never through Confluent.Kafka.sln: the soak is deliberately outside
# the solution (it carries the OpenTelemetry dependency tree). No venv and no
# CONFLUENT_KAFKA_LIB_DIR -- Confluent.Kafka.csproj computes the native path
# from target/<profile>/ itself and copies it into the output.
# ---------------------------------------------------------------------------
echo ">>> $DOTNET build -c $DOTNET_CONFIG (soak client)"
"$DOTNET" build -c "$DOTNET_CONFIG" "$SOAK_DIR/SoakClient/SoakClient.csproj"

SOAKCLIENT_DLL="$SOAK_DIR/SoakClient/bin/$DOTNET_CONFIG/$TFM/SoakClient.dll"
if [[ ! -f "$SOAKCLIENT_DLL" ]]; then
    echo "ERROR: expected $SOAKCLIENT_DLL after the build. Is --tfm $TFM one of" \
         "the project's TargetFrameworks?" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 4. Post-build gate: the unit suite needs no broker and runs in well under a
# second, so there is no reason for a build to hand over an artifact whose
# payload parsing or duplicate/gap accounting is broken.
# ---------------------------------------------------------------------------
echo ">>> $DOTNET test -f $TFM (soak unit tests)"
"$DOTNET" test -c "$DOTNET_CONFIG" -f "$TFM" "$SOAK_DIR/SoakClient.Tests/SoakClient.Tests.csproj"

# Fail loudly here rather than three days into a soak: prove the built client
# starts, loads the native and accepts its own preflight.
echo ">>> $DOTNET $SOAKCLIENT_DLL --check"
SOAK_TESTID="build-check" SOAK_TOPIC="build-check" \
    "$DOTNET" "$SOAKCLIENT_DLL" --check

# ---------------------------------------------------------------------------
# 5. Manifest -- logged by the soak client at startup
# ---------------------------------------------------------------------------
MANIFEST="$SOAK_DIR/build-manifest.json"
GIT_SHA="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
GIT_DESCRIBE="$(git -C "$ROOT" describe --tags --always --dirty 2>/dev/null || echo unknown)"
GIT_BRANCH="$(git -C "$ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"

# An scp'd tree has no .git, so `rev-parse` yields "unknown" and the manifest
# cannot identify the commit -- defeating its entire purpose in exactly the mode
# the README recommends (the repo is private and cannot be cloned on the box).
# --sha supplies it; without it, say so loudly here AND in the manifest, so the
# soak's startup log repeats the warning for the next two weeks.
SHA_SOURCE="git"
if [[ -n "$SHA_OVERRIDE" ]]; then
    if [[ "$GIT_SHA" != "unknown" && "$GIT_SHA" != "$SHA_OVERRIDE" ]]; then
        echo ">>> WARNING: --sha $SHA_OVERRIDE disagrees with the tree's own git" \
             "HEAD $GIT_SHA; recording the override and keeping both." >&2
        SHA_SOURCE="--sha (overrides git HEAD $GIT_SHA)"
    else
        SHA_SOURCE="--sha"
    fi
    GIT_SHA="$SHA_OVERRIDE"
fi

TRACEABLE=true
if [[ "$GIT_SHA" == "unknown" ]]; then
    TRACEABLE=false
    SHA_SOURCE="none"
    cat >&2 <<'WARNEOF'

>>> ############################################################
>>> WARNING: this build is NOT traceable to a commit.
>>>
>>> The source tree has no git metadata and no --sha was given, so
>>> the manifest cannot say which commit is being soaked. If this
>>> run finds a bug in two weeks, nobody will know what to fix.
>>>
>>> Re-run with:  build.sh --src <dir> --sha <commit> [--label <text>]
>>> ############################################################

WARNEOF
fi

RUSTC_VERSION="$(rustc --version 2>/dev/null || echo unknown)"
CARGO_VERSION="$(cargo --version 2>/dev/null || echo unknown)"
DOTNET_VERSION="$("$DOTNET" --version 2>/dev/null || echo unknown)"
BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
BUILD_HOST="$(hostname)"

# Every value is JSON-escaped before it reaches the file rather than being
# interpolated raw. The Python original builds the manifest with json.dump for
# the same reason and records it as a fix for a real injection bug: --label and
# --sha are operator-supplied, and a `"` in either (or, less obviously, in a git
# branch name) would otherwise produce a manifest that is not valid JSON -- which
# the client then cannot read, silently losing the traceability the manifest
# exists to provide.
json_escape() {
    local s=${1//\\/\\\\}
    s=${s//\"/\\\"}
    s=${s//$'\n'/\\n}
    s=${s//$'\r'/\\r}
    s=${s//$'\t'/\\t}
    printf '%s' "$s"
}

cat > "$MANIFEST" <<EOF
{
  "git_sha": "$(json_escape "$GIT_SHA")",
  "git_sha_source": "$(json_escape "$SHA_SOURCE")",
  "traceable": $TRACEABLE,
  "build_label": "$(json_escape "$BUILD_LABEL")",
  "git_describe": "$(json_escape "$GIT_DESCRIBE")",
  "git_branch": "$(json_escape "$GIT_BRANCH")",
  "git_ref_requested": "$(json_escape "${GIT_REF:-<local source>}")",
  "source_root": "$(json_escape "$ROOT")",
  "cargo_profile": "$(json_escape "$PROFILE")",
  "dotnet_configuration": "$(json_escape "$DOTNET_CONFIG")",
  "target_framework": "$(json_escape "$TFM")",
  "rustc_version": "$(json_escape "$RUSTC_VERSION")",
  "cargo_version": "$(json_escape "$CARGO_VERSION")",
  "dotnet_version": "$(json_escape "$DOTNET_VERSION")",
  "build_output": "$(json_escape "$(dirname "$SOAKCLIENT_DLL")")",
  "lib_dir": "$(json_escape "$LIB_DIR")",
  "build_time_utc": "$(json_escape "$BUILD_TIME")",
  "build_host": "$(json_escape "$BUILD_HOST")"
}
EOF

echo ">>> Wrote $MANIFEST"
cat "$MANIFEST"

cat <<EOF

>>> Build complete.

    cd $SOAK_DIR
    TESTID=<id> ./run.sh <client.config>                 # 80 msg/s, 50 B payloads
    HI=true TESTID=<id> ./run.sh <client.config>         # 1000 msg/s, 10 KB payloads

    Rolling is cluster-side: point bootstrap.servers at the rolled cluster.
    Override anything with SOAK_RATE=, SOAK_PAYLOAD_SIZE=, SOAK_VARIANT=.

    Recommended for real batch runs: SOAK_JEMALLOC=true. Under a producer
    stall, glibc can permanently strand freed memory; jemalloc returns it on
    its own. The strand is a property of the NATIVE allocator, which the Rust
    core still uses here. Needs libjemalloc on this host (Debian/Ubuntu:
    apt-get install libjemalloc2) -- run.sh fails loudly at startup if it is
    missing rather than silently running on glibc. See "Memory allocator" in
    ./run.sh --help for the full explanation and why the default stays off.

    SOAK_JEMALLOC=true TESTID=<id> ./run.sh <client.config>
EOF
