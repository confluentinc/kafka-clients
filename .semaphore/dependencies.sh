#!/bin/bash
set -e
# `rust/rust-toolchain.toml` pins the exact channel; running any rustc/cargo
# command inside `rust/` makes rustup auto-install that pinned
# version instead of a floating `stable` (which drifted ahead of the
# locally installed toolchain and broke lint parity -- see
# rust-toolchain.toml history).
sudo apt install -y cmake rustup && (cd rust && rustc --version)
git submodule update --init --depth=1 kafka
