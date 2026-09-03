---
name: commit-hook-timeout
description: git pre-commit hook runs full FFI/C/Python build (make verify-sandbox) that exceeds the agent bash/watchdog timeout; use --no-verify after cargo checks
metadata:
  type: feedback
---

`git commit` (plain) triggers `.githooks/pre-commit` → `make verify-sandbox`,
which does `cargo build --features ffi --release` (~1m47s) + cmake C build +
Python build. That reliably **exceeds the 2-minute foreground Bash timeout** and
kills the commit mid-hook (the commit does NOT land; changes stay staged).

**Why:** the hook re-runs a full cross-language build superset on every commit.
The hook file itself documents "Bypass with `git commit --no-verify`".

**How to apply:** for pure-Rust changes, run the required checks directly first
(`cargo build`, `cargo test --lib`, `cargo xtask lint`, `cargo xtask
format-check` — all must be clean per the DoD), then commit with
`git commit --no-verify`. Note the bypass + "cargo checks run directly" in the
commit message so it's auditable. Do NOT run a plain `git commit` and wait — it
will time out and the prior Actor's "stall" pattern recurs. If the change touches
FFI/C/Python, run `make verify` yourself (in the background / long timeout)
before committing rather than relying on the hook.
