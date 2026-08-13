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
# Build a pinned version of the Rust client and its Python bindings into a
# virtualenv the soak can run from.
#
# Two source modes, both required:
#   --ref <git-ref>   clone/fetch the repository and check out <git-ref>
#   --src <dir>       build an existing source tree in place
#
# The local-directory mode is not a convenience: the Rust repository is not
# public yet, so on a host that cannot clone it, the sources arrive by scp.
#
# Ordering is mandatory. `pip install -e bindings/python` compiles the C
# extension against target/include/confluent_kafka.h and links
# libconfluent_kafka from CONFLUENT_KAFKA_LIB_DIR, so cargo must run first.
# This mirrors the repo's `make build-python` / bindings/python/Makefile.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  build.sh --src <dir>  [--venv <dir>] [--profile release|debug] [--no-venv]
  build.sh --ref <ref>  [--repo <url|path>] [--workdir <dir>] [--venv <dir>]
                        [--profile release|debug]

Options:
  --src <dir>       Build this source tree in place (e.g. an scp'd copy).
  --ref <ref>       Git ref (tag, branch or SHA) to build.
  --repo <url|path> Repository to clone for --ref. Defaults to the origin of
                    the tree this script lives in.
  --workdir <dir>   Where --ref clones to. Default: ./soak-build.
  --venv <dir>      Virtualenv to create/use. Default: <source>/venv-soak.
  --profile <p>     cargo profile: release (default) or debug.
  --no-venv         Install into the active Python environment.
  --sha <sha>       Commit the sources correspond to. REQUIRED in practice for
                    --src builds of a tree with no .git (an scp'd copy), which
                    is otherwise untraceable.
  --label <text>    Free-form build label recorded alongside the sha.
  -h, --help        This message.

Writes <source>/bindings/python/soak/build-manifest.json (git SHA, rustc
version, build time), which soakclient.py logs at startup so a two-week run is
traceable to an exact commit.
EOF
}

SRC_DIR=""
GIT_REF=""
REPO=""
WORKDIR="$(pwd)/soak-build"
VENV_DIR=""
PROFILE="release"
USE_VENV=1
SHA_OVERRIDE=""
BUILD_LABEL=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --src)     SRC_DIR="$2"; shift 2 ;;
        --ref)     GIT_REF="$2"; shift 2 ;;
        --repo)    REPO="$2"; shift 2 ;;
        --workdir) WORKDIR="$2"; shift 2 ;;
        --venv)    VENV_DIR="$2"; shift 2 ;;
        --profile) PROFILE="$2"; shift 2 ;;
        --sha)     SHA_OVERRIDE="$2"; shift 2 ;;
        --label)   BUILD_LABEL="$2"; shift 2 ;;
        --no-venv) USE_VENV=0; shift ;;
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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
    # prerequisite of `cargo build --features ffi` (build.rs generates from the
    # in-repo generator/messages/ specs), which is why the repo's own
    # `build-rust` target does not depend on them either.
else
    ROOT="$(cd "$SRC_DIR" && pwd)"
    if [[ ! -f "$ROOT/Cargo.toml" ]]; then
        echo "ERROR: $ROOT does not look like the client source tree "\
             "(no Cargo.toml)" >&2
        exit 2
    fi
    echo ">>> Building the source tree at $ROOT in place"
fi

SOAK_DIR="$ROOT/bindings/python/soak"
if [[ ! -d "$SOAK_DIR" ]]; then
    echo "ERROR: $SOAK_DIR not found — is this source tree too old?" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 2. Build the Rust client with the FFI feature
#
# Produces target/<profile>/libconfluent_kafka.{so,dylib,a} and
# target/include/confluent_kafka.h (the cbindgen header the C extension
# includes).
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
# 3. Virtualenv
# ---------------------------------------------------------------------------
PYTHON="${PYTHON:-python3}"
if [[ "$USE_VENV" -eq 1 ]]; then
    [[ -n "$VENV_DIR" ]] || VENV_DIR="$ROOT/venv-soak"
    if [[ ! -d "$VENV_DIR" ]]; then
        echo ">>> Creating virtualenv $VENV_DIR"
        "$PYTHON" -m venv "$VENV_DIR"
    fi
    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
    PYTHON="$VENV_DIR/bin/python"
    echo ">>> Using virtualenv $VENV_DIR"
else
    VENV_DIR=""
    echo ">>> Installing into the active Python environment"
fi

"$PYTHON" -m pip install --upgrade pip setuptools wheel

# ---------------------------------------------------------------------------
# 4. Soak dependencies, then the bindings themselves
# ---------------------------------------------------------------------------
echo ">>> pip install -r requirements.txt"
"$PYTHON" -m pip install -r "$SOAK_DIR/requirements.txt"

echo ">>> pip install -e bindings/python (CONFLUENT_KAFKA_LIB_DIR=$LIB_DIR)"
CONFLUENT_KAFKA_LIB_DIR="$LIB_DIR" \
    "$PYTHON" -m pip install -e "$ROOT/bindings/python"

# Fail loudly here rather than three days into a soak. Run from $ROOT (which
# contains no producer.py / consumer.py) and assert the modules resolved inside
# the tree we just built: `python -` puts the cwd first on sys.path, so running
# this from another checkout's bindings/python would import that one instead and
# the check would pass while the install was broken.
( cd "$ROOT" && CONFLUENT_KAFKA_LIB_DIR="$LIB_DIR" ROOT="$ROOT" \
    SOAK_DIR="$SOAK_DIR" "$PYTHON" - <<'PYEOF'
import os
import sys

import consumer
import producer

root = os.path.realpath(os.environ["ROOT"])
for module in (producer, consumer):
    path = os.path.realpath(module.__file__)
    if not path.startswith(root + os.sep):
        sys.exit(">>> ERROR: {} resolved to {}, outside the tree just built "
                 "({})".format(module.__name__, path, root))
print(">>> bindings import OK: producer=%s consumer=%s"
      % (producer.__file__, consumer.__file__))

# Import the soak client too: it additionally needs psutil and the
# soak_metrics import, and a missing dependency or a syntax
# error here would otherwise surface as a supervised restart loop on the first
# run.sh invocation instead of as a build failure.
sys.path.insert(0, os.environ["SOAK_DIR"])
import soakclient  # noqa: E402

print(">>> soakclient import OK: %s" % soakclient.__file__)
PYEOF
)

# The unit suite needs no broker and runs in ~0.05 s, so there is no reason for a
# build to hand over an artifact whose payload parsing or duplicate/gap
# accounting is broken.
echo ">>> pytest $SOAK_DIR/test"
"$PYTHON" -m pytest "$SOAK_DIR/test" -q

# ---------------------------------------------------------------------------
# 5. Manifest — logged by soakclient.py at startup
# ---------------------------------------------------------------------------
MANIFEST="$SOAK_DIR/build-manifest.json"
GIT_SHA="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
GIT_DESCRIBE="$(git -C "$ROOT" describe --tags --always --dirty 2>/dev/null || echo unknown)"
GIT_BRANCH="$(git -C "$ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"

# An scp'd tree has no .git, so `rev-parse` yields "unknown" and the manifest
# cannot identify the commit — defeating its entire purpose in exactly the mode
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
PYTHON_VERSION="$("$PYTHON" -c 'import sys; print(sys.version.split()[0])')"
BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cat > "$MANIFEST" <<EOF
{
  "git_sha": "$GIT_SHA",
  "git_sha_source": "$SHA_SOURCE",
  "traceable": $TRACEABLE,
  "build_label": "$BUILD_LABEL",
  "git_describe": "$GIT_DESCRIBE",
  "git_branch": "$GIT_BRANCH",
  "git_ref_requested": "${GIT_REF:-<local source>}",
  "source_root": "$ROOT",
  "cargo_profile": "$PROFILE",
  "rustc_version": "$RUSTC_VERSION",
  "cargo_version": "$CARGO_VERSION",
  "python_version": "$PYTHON_VERSION",
  "venv": "${VENV_DIR:-<none>}",
  "lib_dir": "$LIB_DIR",
  "build_time_utc": "$BUILD_TIME",
  "build_host": "$(hostname)"
}
EOF

echo ">>> Wrote $MANIFEST"
cat "$MANIFEST"

cat <<EOF

>>> Build complete.

    source ${VENV_DIR:-<active env>}/bin/activate
    cd $SOAK_DIR
    TESTID=<id> ./run.sh <client.config>                 # 80 msg/s, 50 B payloads
    HI=true TESTID=<id> ./run.sh <client.config>         # 80 msg/s, 10 KB payloads

    Rolling is cluster-side: point bootstrap.servers at the rolled cluster.
    Override anything with SOAK_RATE=, SOAK_PAYLOAD_SIZE=, SOAK_VARIANT=.
EOF
