# Translation agent

Python orchestrator that watches Apache Kafka commits and produces
translated Rust PRs in this repository by spawning `r2 sandbox claude`
instances. State is persisted in a sqlite DB that lives as a Semaphore CI
project artifact between runs.

The orchestrator does NOT translate Java to Rust itself — it dispatches
Claude Code (Manager-role) instances inside `r2` sandboxes, each of which
runs its own Actor/Critic loop per `.claude/rules/agent-roles.md`.

See `design/history/Milestone-7/DESIGN-translation-agent.md` for the full
contract.

## Requirements

- Python 3.10+.
- `r2` (the sandbox runner) on PATH for live use. Tests do not require it.
- `gh` CLI authenticated against this repo (`gh auth status`) for PR
  creation.
- `git` with push credentials for the Rust repo's `origin`.
- `artifact` (Semaphore CLI) on PATH for the artifact push step. Use
  `--no-artifact-push` for local runs.

## Install

From this directory:

```bash
pip install -e '.[dev]'
```

This installs the package, the `translation-agent` console script, and
`pytest` for tests.

If your Python interpreter was built without sqlite3 support (a common
pyenv pitfall when libsqlite3-dev was missing at compile time), also run:

```bash
pip install pysqlite3-binary
```

The package automatically falls back to it.

## CLI modes

`translation-agent` has three mutually exclusive top-level modes plus
shared options.

### Sweep mode (default)

```bash
translation-agent \
  --ak-repo-path /path/to/kafka \
  --rust-branch master \
  [--db-path ./translation_agent.db] \
  [--no-artifact-push] [--dry-run] [--verbose]
```

Sweep is **narrowly scoped to global cursor administration**. It does
three things and nothing else:

1. **Closure check + cursor advance.** Walks the next 10 AK commits on
   the cursor's branch; for each one whose draft PR is CLOSED or MERGED
   on GitHub, archives it and advances `branch_commit` accordingly.
2. **Re-fetch.** If the cursor advanced, re-reads the next 10 AK commits
   from the new position before the next step.
3. **Create draft PRs.** For each remaining AK commit (up to 10),
   creates a branch + draft PR + `pr_commit` row at status 0.

Sweep does **not** evaluate dependencies, generate plans, or run
implementations -- those are dispatched per-PR by Semaphore on each PR
build (see "Per-PR mode" below).

### Per-PR mode

A `--pr <N>` invocation reads the row's current status and **cascades**
through the next applicable steps until it hits a human gate or
completes:

```bash
translation-agent --ak-repo-path /path/to/kafka --pr <N>
```

Cascade transitions:

| From | To | Action |
|---|---|---|
| 0 (`no_plan`) | 1 (`dependencies_evaluated`) | dep-eval against the bounded candidate range `cursor..pr.ak_commit` |
| 1 (`dependencies_evaluated`) | 2 (`plan_created`) | plan generation; gated on `plan_dependency` PR being at status ≥ 3 |
| 2 (`plan_created`) | --- | **human gate**, waits for `--plan-approve` |
| 3 (`plan_approved`) | 4 (`implementation_done`) | implementation; gated on `implementation_dependency` PR being at status ≥ 4 |
| 4 (`implementation_done`) | --- | noop |

The cascade re-reads the row at the top of each iteration (race-safe:
if another runner advanced the row between steps, the cascade sees the
new status and dispatches correctly or noops). It's bounded at 5
iterations defensively, though the status enum naturally caps it at
0→1→2 in one invocation.

Plan approval (used by Semaphore manual-promotion jobs — flips status
2 → 3 and immediately runs implementation):

```bash
translation-agent --pr <N> --plan-approve
```

`--plan-approve` is **idempotent on the impl side**: re-running it on a
row already at status 3 (impl was started but didn't finish) skips the
2→3 transition and goes straight to impl. This is the recovery path for
"orphaned" status-3 rows: manually re-run the PR build in Semaphore and
the cascade picks it up.

### Seed mode

Bootstraps the `branch_commit` correspondence on first run:

```bash
translation-agent --seed \
  --ak-branch trunk --ak-commit <full-ak-sha> \
  --rust-branch master
```

Idempotent — a row already present for `rust_branch` with the same
`(ak_branch, ak_commit)` is left unchanged.

## State machine

`pr_commit.status` is an enum:

| Value | Name | Set by |
|---|---|---|
| 0 | `no_plan` | sweep (draft PR creation) |
| 1 | `dependencies_evaluated` | per-PR cascade (after r2 dep-eval) |
| 2 | `plan_created` | per-PR cascade (after r2 plan generation) |
| 3 | `plan_approved` | `--pr N --plan-approve` |
| 4 | `implementation_done` | per-PR cascade or `--plan-approve` |

A `last_error TEXT` column captures the most recent failure (subprocess
stderr + exit code). On failure the status is left unchanged so a
future `--pr N` build retries.

When a row reaches status 4, the orchestrator atomically updates
`branch_commit` with the new Rust commit (via `mark_implementation_done`).

## Concurrency

Per-PR work parallelizes naturally across Semaphore PR builds: each
build of a PR runs `--pr <N>` for that PR's row, in its own job. Sweep
runs separately on the main branch and only does cursor admin (it
doesn't dispatch LLM work).

### Distributed lock + per-op artifact pull/push

Every public `db.py` call goes through a `locked_db.session()`
context manager that:

1. **Acquires a Semaphore-artifact-based mutex** by pushing
   `translation_agent.db.lock` *without* `--force`. The push fails
   when the artifact already exists; that failure means another runner
   holds the lock. Retry every 60 s, up to 10 attempts (10 minutes).
2. **Pulls the latest DB** (`artifact pull project translation_agent.db`).
   First-run absence is tolerated.
3. **Opens a fresh sqlite connection**, yields it to the caller, closes.
4. **Pushes the modified DB** if the session was opened as `write=True`
   AND the with-block exited cleanly. On exception the push is *skipped*
   so partial/corrupted state is not published.
5. **Releases the lock** by `artifact yank project translation_agent.db.lock`.

LLM calls (dep-eval, plan, impl) happen entirely **outside** the lock:
the cascade reads the row under one lock, releases, runs r2 (minutes),
re-acquires for the write. The lock holds for sub-second sqlite ops only.

The lock artifact's payload is JSON: `{runner_id, acquired_at, pid}`.
An operator inspecting a stale lock can match `runner_id` against
active Semaphore jobs to confirm staleness before yanking.

### Stale-lock recovery (manual)

If a runner crashes while holding the lock, all subsequent `--pr N`
builds that need DB I/O will fail with `LockTimeoutError` after the
10-minute retry budget. To recover:

1. Inspect the lock artifact contents to confirm the holder is
   genuinely dead (Semaphore job ID no longer running).
2. Run `artifact yank project translation_agent.db.lock` from any
   Semaphore job (or via the Semaphore UI's artifact browser).
3. The next `--pr N` build will acquire on its first attempt.

### Worktree isolation for plan / impl

Each plan and implementation task runs in **its own temporary git
worktree** on the Rust repo (created from `origin/<branch_name>`,
which Phase B already pushed). The inner Claude commits inside the
worktree; the orchestrator then pushes from the worktree before
removal. The R2 sandbox denies `git push` from inside the inner
Claude, so the orchestrator is the sole pusher to the remote.

Dep-eval does NOT use a worktree -- it's read-only and emits JSON to
stdout.

Each agent's stdout (merged with stderr) is line-buffered and flushed
every 100 lines, prefixed with `>>>>> From agent #<pr_number>` so
operators can demultiplex the parallel streams.

## Tests

```bash
pytest
```

(or `python -m pytest` if pytest is not on PATH after install).

## Dry-run mode

`--dry-run` skips destructive remote operations (`git push`,
`gh pr create`, the Semaphore artifact push) and remote-mutating DB
state changes (no `mark_plan_created` / `mark_implementation_done`,
no `branch_commit` insert), but DOES:

- Insert `pr_commit` rows with **synthetic negative `pr_number`s** derived
  from a sha1 of the AK commit (deterministic across re-runs, so the
  insert is idempotent). Real GitHub PR numbers are positive, so the
  sign distinguishes them unambiguously.
- **If `r2` is on PATH**, run all three claude phases for real:
  - **Dep-eval** (read-only — emits JSON): persists deps and transitions
    rows 0 → 1.
  - **Plan generation**: runs in a per-PR worktree (preserved on disk
    for inspection), with the prompt augmented to tell claude NOT to
    `git push`. DB status stays at 1.
  - **Implementation**: same — runs in a preserved per-PR worktree, no
    push, DB status stays at 3 (or stays at 2 → 3 → stuck-at-3 in the
    `--plan-approve` cascade).

If `r2` is **not** on PATH, all three phases just log "would ...".

The preserved worktrees live at `/tmp/translation-agent-<branch>-XXXXXX`
so the operator can `cd` into them and inspect what claude produced.
Clean up with:

```bash
rm -rf /tmp/translation-agent-*
git -C <rust-repo> worktree prune
```

To clean up synthetic `pr_commit` rows after dry-run testing:

```bash
sqlite3 ./translation_agent.db "DELETE FROM pr_commit WHERE pr_number < 0"
```

## Local development without Semaphore

For end-to-end testing on a developer workstation (no `r2` runner, no
`artifact` CLI), use the `dev-bin/r2` wrapper:

```bash
export PATH="$(git rev-parse --show-toplevel)/tools/translation_agent/dev-bin:$PATH"
translation-agent --no-artifact-push --db-path /tmp/ta_local.db ...
```

The wrapper translates `r2 sandbox claude -p "<prompt>"` into a direct
`claude -p "<prompt>"` call against the locally-installed Claude Code
CLI. Unlike the real Semaphore r2 runner (which runs Claude fully
autonomously inside an isolated container), the local claude prompts
the operator for permission on every tool call -- slower and noisier
but safer because you stay in the loop on every filesystem write,
commit, and push.

`--no-artifact-push` is **required** for local runs since the Semaphore
`artifact` CLI isn't installed: with the per-op locking design, every
DB read or write attempts to acquire the lock via `artifact push`, and
without the binary the very first operation crashes with FileNotFoundError.
Passing `--no-artifact-push` short-circuits the entire artifact layer
(no lock, no pull, no push).

## Semaphore CI integration

The orchestrator is invoked from a single CI pipeline plus a
manually-triggered Task, defined under `.semaphore/` at the repo root:

| File | Trigger | What it runs |
|---|---|---|
| `.semaphore/semaphore.yml` | every push / PR | Sweep on the configured `${MAIN_BRANCH}` (default `master`); `--pr <N>` cascade on PR builds; no-op otherwise. |
| `.semaphore/plan-approve.yml` | manual promotion from a PR build | `translation-agent --pr <N> --plan-approve` (flips status 2→3 and cascades into implementation for that PR). |
| `.semaphore/seed.yml` | manual Task in the Semaphore project's Tasks tab | `translation-agent --seed ...` to bootstrap `branch_commit` on first use. Required Task parameter: `AK_COMMIT`; optional: `AK_BRANCH`, `RUST_BRANCH`. |

Each pipeline runs `artifact pull project translation_agent.db || true`
in its prologue (the `|| true` lets first-ever runs proceed before the
artifact exists). The orchestrator itself handles per-op pull + push
inside `locked_db.session()` (see "Concurrency" above), so there is no
end-of-run artifact push -- every committed write is published as soon
as its session exits cleanly.

The mainline branch is configurable via the `MAIN_BRANCH` env var on
the main pipeline (set in the project's Environment Variables tab to
follow a non-`master` branch).

## Layout

```
translation_agent/
  __init__.py
  __main__.py        # python -m translation_agent
  cli.py             # argparse entry point + sweep/PR mode wiring
  db.py              # sqlite schema + helpers
  git_ops.py         # git subprocess wrappers
  github.py          # gh pr create wrapper, PR/branch naming
  prompts.py         # dep-eval / plan / impl prompt templates + JSON parser
  locked_db.py       # distributed lock + per-op artifact pull/push session()
  r2.py              # non-streaming r2 sandbox claude wrapper
  semaphore.py       # Semaphore artifact push/pull/yank wrappers
  streaming.py       # line-buffered Popen wrapper with per-PR prefix
  worktree.py        # per-PR git worktree context manager
dev-bin/
  r2                 # local-development emulation wrapper for r2 sandbox claude
tests/
  test_*.py          # ~250 unit + integration tests
pyproject.toml       # pytest + setuptools config + dev deps
README.md
```
