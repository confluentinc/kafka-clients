---
name: workflow-docker-wedged-no-verify
description: The pre-commit hook runs make verify-sandbox, which needs Docker; when Docker is wedged, never bypass the hook on your own — stop, ask the user (through the Manager as a subagent) and list the checks still owed
metadata:
  type: feedback
---

`core.hooksPath = .githooks`, and `.githooks/pre-commit` is `exec make verify-sandbox`:
`build-rust build-c format-check check-generated lint test-integration test-c`. So **every
`git commit` builds the C bindings in release and then needs Docker** for the integration
tests. The whole hook takes about 10 minutes; [[commit-hook-timeout]] says how to run it in
the background.

**Why it matters:** when Docker is wedged (orphaned broker containers holding ports, a stuck
daemon), the hook's integration step hangs or fails for a reason outside the change. That
does not license a bypass: the user decided that every commit runs the hook, and only the
user can make an exception.

**How to apply:**

  - Never commit with `--no-verify` on your own. Stop, and ask the user, through the Manager
    when running as a subagent. Report what is wrong with Docker.
  - In the same message, say which checks you ran yourself (for example `cargo build`,
    `cargo test`, `cargo xtask format-check`, `cargo xtask lint`,
    `cargo xtask check-generated`) and which of the hook's checks are still owed (the
    integration and C tests). If the user allows the bypass, state in the commit message and
    the report that those checks are **owed**, not passed.
  - Capture exit codes without pipes (`cmd > log 2>&1; echo "EXIT=$?"`): `| tail` masks the
    status.
  - `docker ps -q | wc -l` is read-only and safe; use it before and after to prove you
    orphaned nothing. While Docker is wedged, start no more integration runs, and do not
    clean up containers you did not start.

**Check first (2026-09-29):** the hook exists only if `core.hooksPath` is set, which `make init-hooks` (a prerequisite of `make build`) does. In a clone where that never ran, `git config core.hooksPath` is empty and a plain `git commit` runs no hook.
