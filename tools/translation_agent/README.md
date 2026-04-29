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

Each agent's stdout (merged with stderr) is line-buffered and flushed
every 100 lines, prefixed with `>>>>> From agent #<pr_number>` so
operators can demultiplex the parallel streams.

## Tests

```bash
pytest
```

(or `python -m pytest` if pytest is not on PATH after install).

## Semaphore CI integration

The orchestrator is designed to be invoked from two Semaphore pipelines:

1. **Main sweep pipeline** (periodic):
   - `artifact pull project translation_agent_db || true`
   - `translation-agent --ak-repo-path ... --ak-branch ... --rust-branch ...`
   - The orchestrator pushes the artifact at the end.

2. **Per-PR plan-approval pipeline** (manual promotion):
   - `artifact pull project translation_agent_db`
   - `translation-agent --pr <N> --plan-approve`
   - The orchestrator pushes the artifact at the end.

The pipeline is responsible for `artifact pull` before invocation; the
orchestrator handles `artifact push project <name> <db-path>` itself in
a `try/finally` so the DB is persisted on both success and failure of
the inner work.

The artifact name is configurable via `--artifact-name`
(default: `translation_agent_db`).

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
tests/
  test_*.py          # 95 unit + integration tests
pyproject.toml       # pytest + setuptools config + dev deps
README.md
```
