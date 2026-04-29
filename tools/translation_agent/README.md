# Translation agent

Python orchestrator that watches Apache Kafka commits and produces
translated Rust PRs in this repository by spawning `r2 sandbox claude`
instances. State is persisted in a sqlite DB that lives as a Semaphore CI
project artifact between runs.

The orchestrator does NOT translate Java to Rust itself — it dispatches
Claude Code (Manager-role) instances inside r2 sandboxes, each of which
runs its own Actor/Critic loop per the project's
`.claude/rules/agent-roles.md`.

See `design/history/Milestone-7/DESIGN-translation-agent.md` for the full
contract.

## Status

Phase A (foundation) — sqlite schema, CLI skeleton, Semaphore push
wrapper, seed and per-PR modes.

Phases B–E (PR creation, dependency evaluation, plan + implementation
dispatch, end-to-end glue) — pending.

## Requirements

- Python 3.10+
- `r2` (the sandbox runner) on PATH for live use. Tests do not require it.
- `gh` CLI authenticated against this repo for PR creation (Phase B+).
- `artifact` CLI (Semaphore) on PATH for the artifact push (Phase E). Use
  `--no-artifact-push` for local runs.

## Install

From this directory:

```bash
pip install -e '.[dev]'
```

This installs the package and the `translation-agent` console script, plus
`pytest` for tests.

## Run

Console script (after install):

```bash
translation-agent --help
```

Without install, from this directory:

```bash
python -m translation_agent --help
```

### Modes

| Mode | Invocation | Effect |
|---|---|---|
| Seed | `translation-agent --seed --ak-branch trunk --ak-commit <sha> --rust-branch master --rust-commit <sha>` | Insert (or no-op) a row into `branch_commit`. |
| Status check | `translation-agent --pr <N>` | Print the row from `pr_commit`. |
| Plan approve | `translation-agent --pr <N> --plan-approve` | Transition PR `N` from status 2 to 3. (Phase D will cascade into the implementation step.) |
| Sweep | `translation-agent --ak-repo-path <path> --ak-branch <name> --rust-branch <name>` | Main loop. Phases B–E will fill in. |

### State machine

Per design step 3, the `pr_commit.status` column is an enum:

| Value | Name | Meaning |
|---|---|---|
| 0 | `no_plan` | Just created. Step 4 will populate dependencies. |
| 1 | `dependencies_evaluated` | `plan_dependency` and `implementation_dependency` are set. |
| 2 | `plan_created` | A plan has been written and pushed to the PR's branch. |
| 3 | `plan_approved` | A human approved the plan via Semaphore manual promotion (`--plan-approve`). |
| 4 | `implementation_done` | Code lands; `branch_commit` updated. |

A `last_error TEXT` column captures the most recent failure (subprocess
stderr + exit code). On failure the status is left unchanged, so the next
sweep retries.

## Tests

```bash
pytest
```

(or `python -m pytest` if pytest is not on PATH).

## Semaphore CI integration

The orchestrator is designed to be invoked from two distinct Semaphore
pipelines:

1. **Main sweep pipeline** (periodic): pulls the artifact, invokes
   `translation-agent --ak-repo-path ... --ak-branch ... --rust-branch ...`,
   and lets the orchestrator push the artifact at the end.
2. **Per-PR plan-approval pipeline** (manual promotion): pulls the
   artifact, invokes `translation-agent --pr <N> --plan-approve`, and lets
   the orchestrator push the artifact.

The pipeline is responsible for `artifact pull project <name>` before the
orchestrator runs; the orchestrator handles `artifact push project <name>`
itself in a `try/finally` so it runs on both success and failure.

## Layout

```
translation_agent/
  __init__.py
  __main__.py        # python -m translation_agent
  cli.py             # argparse entry point
  db.py              # sqlite schema + helpers
  r2.py              # r2 sandbox claude wrapper
  semaphore.py       # Semaphore artifact push wrapper
tests/
  test_cli.py
  test_db.py
  test_r2.py
  test_semaphore.py
pyproject.toml       # pytest + setuptools config + dev deps
README.md
```
