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
# Builds THIRD_PARTY_LICENSES.txt, the third-party licence file every platform package of the
# Node.js client ships (javascript/npm/README.md): the licences of the Rust crates linked into the
# addon, the confluent-kafka crate with its `ffi` feature, from rust/Cargo.lock, with cargo-about
# and the configuration and template in javascript/npm/ (about.toml, about.hbs). A crate whose
# licence about.toml does not accept fails the build, and so does a crate whose licence cannot be
# read. Then checks the file with check-npm.py licenses. Used by the "Release (JavaScript):
# third-party licences" block of semaphore.yml and by `make -C javascript pack`.
#
# Usage, from the repository root:
#   .semaphore/third-party-licenses.sh <out-file>
#
# cargo-about is the one on PATH when it is version CARGO_ABOUT_VERSION; otherwise this script
# installs that version, with cargo, into rust/target/cargo-about-<version>/ (git-ignored) and
# uses it from there. It needs cargo (rust/rust-toolchain.toml's) and network access, both to
# install cargo-about and to read the crates' licence files.

set -eu

CARGO_ABOUT_VERSION=0.9.3

die() {
    echo "third-party-licenses: $*" >&2
    exit 1
}

[ $# -eq 1 ] || { echo "Usage: $0 <out-file>" >&2; exit 2; }
out="$1"
[ -f javascript/npm/about.toml ] && [ -f rust/Cargo.lock ] || die "run from the repository root"
command -v cargo >/dev/null 2>&1 || die "cargo is not on PATH"

# The toolchain pinned in rust/rust-toolchain.toml, for the whole script, as
# build-ffi-library.sh's toolchain() reads it: rustup finds that file only from a
# cwd inside rust/, and this script runs from the root, where CI configures no
# default toolchain. It covers the install below and cargo-about's own
# `cargo metadata`.
RUSTUP_TOOLCHAIN=$(sed -n 's/^channel *= *"\(.*\)"/\1/p' rust/rust-toolchain.toml)
[ -n "$RUSTUP_TOOLCHAIN" ] || die "could not read the toolchain from rust/rust-toolchain.toml"
export RUSTUP_TOOLCHAIN

if command -v cargo-about >/dev/null 2>&1 && [ "$(cargo-about --version)" = "cargo-about $CARGO_ABOUT_VERSION" ]; then
    about=cargo-about
else
    root="rust/target/cargo-about-$CARGO_ABOUT_VERSION"
    if [ ! -x "$root/bin/cargo-about" ]; then
        # --locked: cargo-about's own lockfile, so the tool is the same on every run.
        cargo install --locked --features cli --root "$root" "cargo-about@$CARGO_ABOUT_VERSION" \
            || die "could not install cargo-about $CARGO_ABOUT_VERSION"
    fi
    about="$root/bin/cargo-about"
fi

# --locked: the crates of rust/Cargo.lock, as the addon is built from (build-ffi-library.sh).
# --fail: a crate with no readable licence fails rather than being left out.
"$about" generate --locked --fail --manifest-path rust/Cargo.toml --features ffi \
    -c javascript/npm/about.toml -o "$out" javascript/npm/about.hbs \
    || die "cargo-about could not build $out"
python3 .semaphore/check-npm.py licenses "$out"
