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
  --ak-branch trunk \
  --rust-branch master \
  [--max-parallel 4] \
  [--db-path ./translation_agent.db] \
  [--no-artifact-push] [--dry-run] [--verbose]
```

Walks the `branch_commit` cursor for `--rust-branch`, takes the next 10
AK commits on `--ak-branch`, opens a draft PR for each, and dispatches
parallel `r2 sandbox claude` runs for dependency evaluation, plan
generation, and (where unblocked) implementation. Pushes the sqlite DB
to the Semaphore project artifact at the end.

### Per-PR mode

Status check (read-only):

```bash
translation-agent --pr <N>
```

Plan approval (used by Semaphore manual-promotion jobs — flips status 2
→ 3 and immediately runs implementation for that PR):

```bash
translation-agent --pr <N> --plan-approve
```

### Seed mode

Bootstraps the `branch_commit` correspondence on first run:

```bash
translation-agent --seed \
  --ak-branch trunk --ak-commit <full-ak-sha> \
  --rust-branch master --rust-commit <full-rust-sha>
```

Idempotent — a row already present for `(ak_branch, ak_commit, rust_branch)`
is left unchanged.

## State machine

Per design step 3, `pr_commit.status` is an enum:

| Value | Name | Set by |
|---|---|---|
| 0 | `no_plan` | sweep step 3 (PR creation) |
| 1 | `dependencies_evaluated` | sweep step 5 (after r2 dep-eval) |
| 2 | `plan_created` | sweep step 6 (after r2 plan generation) |
| 3 | `plan_approved` | per-PR step 7 (`--pr N --plan-approve`) |
| 4 | `implementation_done` | sweep step 8 OR per-PR cascade |

A `last_error TEXT` column captures the most recent failure (subprocess
stderr + exit code). On failure the status is left unchanged so the next
sweep retries.

When a row reaches status 4, the orchestrator inserts a new row into
`branch_commit` with the new Rust commit, advancing the cursor for the
next sweep.

## Concurrency

`--max-parallel` (default 4) caps concurrent `r2 sandbox claude`
invocations across both step-6 (plan) and step-8 (implementation) tasks
within a single sweep — they share one `ThreadPoolExecutor`. The
unblocked predicate keeps work dependency-safe, so no two-pass scan is
needed.

Each plan and implementation task runs in **its own temporary git
worktree** on the Rust repo (created from `origin/<branch_name>`,
which Phase B already pushed). The inner Claude commits and pushes
inside the worktree, then the worktree is removed on context exit
(success or failure). This makes parallel runs safe regardless of the
runner: the real Semaphore `r2 sandbox` provides container-level
isolation, but even local runs via `dev-bin/r2` cannot clobber each
other's edits because the worktrees physically separate them.

Dep-eval (step 4) does NOT use a worktree -- it's read-only and emits
JSON to stdout, so isolation buys nothing.

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

`--no-artifact-push` is recommended for local runs since the Semaphore
`artifact` CLI isn't installed; without it the orchestrator will log a
"binary not found on PATH" warning at the end (the inner work still
succeeds).

## Semaphore CI integration

The orchestrator is invoked from a single CI pipeline plus a
manually-triggered Task, defined under `.semaphore/` at the repo root:

| File | Trigger | What it runs |
|---|---|---|
| `.semaphore/semaphore.yml` | every push / PR | Sweep on the configured `${MAIN_BRANCH}` (default `master`); `--pr <N>` status check on PR builds; no-op otherwise. |
| `.semaphore/plan-approve.yml` | manual promotion from a PR build | `translation-agent --pr <N> --plan-approve` (flips status 2→3 and cascades into implementation for that PR). |
| `.semaphore/seed.yml` | manual Task in the Semaphore project's Tasks tab | `translation-agent --seed ...` to bootstrap `branch_commit` on first use. Required Task parameters: `AK_COMMIT`, `RUST_COMMIT`; optional: `AK_BRANCH`, `RUST_BRANCH`. |

Each pipeline:
- runs `artifact pull project translation_agent.db || true` in its
  prologue (the `|| true` lets first-ever runs proceed before the
  artifact exists);
- delegates `artifact push` to the orchestrator itself (in a
  `try/finally` so partial state is persisted on inner failures).

The artifact name is configurable via `--artifact-name`
(default: `translation_agent.db`). The mainline branch is configurable
via the `MAIN_BRANCH` env var on the main pipeline (set in the project's
Environment Variables tab to follow a non-`master` branch).

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
  r2.py              # non-streaming r2 sandbox claude wrapper
  semaphore.py       # Semaphore artifact push
  streaming.py       # line-buffered Popen wrapper with per-PR prefix
  worktree.py        # per-PR git worktree context manager
dev-bin/
  r2                 # local-development emulation wrapper for r2 sandbox claude
tests/
  test_*.py          # 100 unit + integration tests
pyproject.toml       # pytest + setuptools config + dev deps
README.md
```
