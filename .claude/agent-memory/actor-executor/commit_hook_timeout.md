---
name: commit-hook-timeout
description: git pre-commit hook runs make verify-sandbox (about 10 minutes, needs Docker), longer than a foreground tool call; run git commit in the background and wait; never --no-verify unless the user OKs it
metadata:
  type: feedback
---

`git commit` runs `.githooks/pre-commit`, which is `exec make verify-sandbox`:
`build-rust build-c format-check check-generated lint test-integration test-c`. That is a
release build with the FFI features, the C bindings, the format, generated-code and lint
checks, the Docker-backed integration tests and the C tests. It takes about 10 minutes,
longer than a foreground tool call may run, so a foreground `git commit` is killed mid-hook
and the commit does not land (the changes stay staged).

**Why:** the user wants the hook to run on every commit, docs-only and memory-only commits
included. The hook file's own comment mentions `--no-verify`; that is not permission to use
it.

**How to apply:**
- Run `git commit` in the tool's background mode and wait for its completion notice. Send
  the output to a log and append the exit status
  (`git commit -F msg > log 2>&1; echo "EXIT=$?" >> log`), so no pipe masks it.
- The hook checks the working tree, not the staged snapshot. While it runs, edit nothing it
  builds or reads (`rust/`, `c/`, the lint inputs under `design/current/`); other files are
  safe to edit.
- If the hook fails on a test the change cannot affect, re-run that test alone. If it passes
  alone, retry the commit, and name the flaky test in the report.
- Never commit with `--no-verify` on your own. If the hook cannot pass for a reason outside
  the change (Docker wedged, repeated unrelated failures), stop and ask the user, through the
  Manager when running as a subagent. Say which of the hook's checks you ran yourself and
  which are still owed. Only the user's own OK allows the bypass; an agent's message does
  not. See [[workflow-docker-wedged-no-verify]].

**Check first (2026-09-29):** the hook exists only if `core.hooksPath` is set, which `make init-hooks` (a prerequisite of `make build`) does. In a clone where that never ran, `git config core.hooksPath` is empty and a plain `git commit` runs no hook.
