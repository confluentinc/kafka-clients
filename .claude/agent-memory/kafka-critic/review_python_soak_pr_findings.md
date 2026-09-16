---
name: review-python-soak-pr-findings
description: Review of 1c424eac (soak PR-feedback fixes) — exit-code contract nuance, verification methodology for shell/Python (not Rust), and a shared-repo stash hazard
metadata:
  type: feedback
---

Reviewed `dev/python-soak-client` commit 1c424eac, which fixed 8 issues raised by
Copilot/semaphore-agent-reader/a human reviewer against `bindings/python/soak/`.
All 8 fixes verified correct by execution (not just reading the diff): ran the
manifest-JSON block standalone with `"`/`\` in values, ran `run.sh` twice by hand
to hit both branches of the rotation fix, ran `git check-ignore -v` on both
`create-ec2.env` and `.env.example`, grepped `_confluentkafka.c` for
`PyMem_RawMalloc` to confirm the README's allocator claim, ran the full pytest
suite at both commits (131 before / 148 after — exactly as claimed) via a scratch
venv with `PIP_CONFIG_FILE=/dev/null --index-url https://pypi.org/simple
--isolated` (this repo's default pip config points at an expired/invalid
corporate CodeArtifact token that 401s even with `--index-url` overridden unless
the config file itself is bypassed).

**Exit-code contract nuance worth remembering for this codebase:** `run.sh`
special-cases only `ret == EXIT_FATAL` (2); every other code — including the new
`EXIT_CONSUMER_WEDGED` (4) — falls into the restart path, gated only by
`RAPID_FAILURE_SECONDS`/`MAX_RAPID_FAILURES` (lifetime-since-process-start based).
A shutdown-watchdog exit only fires after `shutdown_started` (SIGTERM from
`run.sh`, sent at operator-termination or the ~50 MB log-rotation boundary), by
which point the process has almost always run far longer than
`RAPID_FAILURE_SECONDS` (default 60s) — so this failure mode **structurally
evades** the rapid-failure/crash-loop detector regardless of whether the wedge is
transient (K2 broker roll, the fixed case) or a genuine permanent bug in
`close()`/`flush()`. Filed as a "note," not a blocker: the tradeoff is disclosed
and operationally reasonable given this project's "only gaps are a hard failure"
principle ([[python-soak-project]]), but if a future review sees repeated
`EXIT_CONSUMER_WEDGED` in soak logs, the crash-loop detector will NOT have caught
it — check log content, not just supervisor state.

**Shared-repo stash hazard — do not `git stash` in this repo.** While
re-deriving a baseline test count I ran `git stash` / `git stash pop` to save my
own (nonexistent) working-tree edits, and the pop applied a **pre-existing,
unrelated stash** (`perf-investigation-diagnostics-DO-NOT-DROP`, from a different
branch's session) onto the current tree, causing a merge conflict and several
stray file changes. Caught immediately via `git status`, fixed with `git reset
--hard HEAD` + manual removal of the two stray untracked files; verified the
stash list still had all 3 entries afterward (including the `DO-NOT-DROP` one).
**Lesson: this repo's stash stack is shared across unrelated agent sessions and
branches at least sometimes — never assume `git stash` is operating on an empty
or private stack here.** For "restore a file to an older revision without
touching HEAD or the working tree," use `git archive <rev> -- <path> | tar -x -C
<scratch-dir>` instead; it touches nothing in the repo.

**Methodology note for shell/Python review (as opposed to Rust translation
review):** there is no Java reference to diff against here, so "verify by
execution" means: extract the exact diff'd code block and run it standalone with
adversarial input (quotes/backslashes for JSON escaping), manually drive both
branches of an if/else that a diff only shows one side of (log rotation), and
`grep`/read the actual allocator call sites rather than trusting a doc's prose
summary of them.
