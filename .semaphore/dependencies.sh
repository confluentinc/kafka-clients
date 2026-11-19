#!/bin/bash
set -e

# Runs a command up to 5 times, 5 seconds apart: for a download, which can fail for a moment.
retry() {
    _n=1
    while [ "$_n" -le 5 ]; do
        if "$@"; then return 0; fi
        [ "$_n" -eq 5 ] && break
        echo "retry: attempt $_n/5 of '$*' failed; sleeping 5s" >&2
        _n=$((_n + 1))
        sleep 5
    done
    echo "retry: '$*' failed after 5 attempts" >&2
    return 1
}

# `rust/rust-toolchain.toml` pins the exact channel; running any rustc/cargo
# command inside `rust/` makes rustup auto-install that pinned
# version instead of a floating `stable` (which drifted ahead of the
# locally installed toolchain and broke lint parity -- see
# rust-toolchain.toml history).
sudo apt install -y cmake rustup && (cd rust && rustc --version)
retry git submodule update --init --depth=1 kafka
