# Milestone 7: Create a translation agent for Apache Kafka commits

In this milestone an agent must be implemented that looks at new commits in
Apache Kafka and creates PR in this repository with corresponding translations
to Rust code.

The workflow should be a Python application that does these things, using a sqlite db for storing the tables.

0. the Semaphore CI pipeline loads the sqlite db from project artifacts.
1. the agent runs with a given AK branch and Rust branch it has a table `branch_commit` with
the AK branch and commit hash, Rust client branch and commit hash
2. it runs with a Rust branch. It takes the latest commit and gets the corresponding commit
   in the corresponding AK branch looking in `branch_commit` table.
3. it get the next 10 commits on AK branch and for each commit it creates a branch and a PR, starting from the initial AK branch.
   It inserts into a table `pr_commit`. This table has the PR number, the Rust branch, the corresponding AK commit
   and two optional columns `plan_dependency` and `implementation_dependency` that contain commits (hashes) that
   are a precondition before planning this commit translation or before starting the implementation.
   The table also contain a status enum:
   - 0: no plan
   - 1: dependencies evaluated
   - 2: plan created
   - 3: plan approved
   - 4: implementation done
4. for each PR that has status (0: no plan) it starts Claude Code with r2 command, like:
   `r2 sandbox claude -p "Claude Code prompt"`, to identify the dependencies
   of that commit for planning or for implementing. It outputs the dependencies in a JSON file.
   There should be only a single `plan_dependency` and a single `implementation_dependency`:
   the latest commit that is a dependency.
5. the application reads the dependencies and updates the `pr_commit` table with those
   and sets the status to (1: dependencies evaluated)
6. for each PR that has status (1: dependencies evaluated) and has no `plan_dependency`
   or the plan dependency is not among those in the table (open ones) or present but with
   status >= (3: plan approved), it runs claude with `r2` and asks the manager
   agent to create a plan and to save it to `./design/history/<pr_number>_description/plan.md`.
   The Claude Code runs with `r2` should be in parallel, and the output should
   be flushed every 100 lines and written to stdout preceded with
   ">>>>> From agent #<pr_number>".
   Each agent commits the plan and pushes it to the branch corresponding to the AK commit.
   The commit message should be "Design document". It updates the status for that PRs to
   (2: plan created). 
7. when run with `--pr <number>` and `--plan-approve` it changes the status of the corresponding
   PR from (2: plan created) to (3: plan approved) and continues with (8).
   When run with `--pr <number>` only it just checks the status of that PR.
   `--plan-approve` happens when the Semaphore CI PR pipeline is running and a manual promotion is triggered.
8. for each PR that has status (3: plan approved) and has no `implementation_dependency`
   or the implementation dependency is not among those in the table (open ones),
   it runs claude with `r2` and asks the manager
   agent to start the implementation of the plan at `./design/history/<pr_number>_description/plan.md`.
   Running the actor and critic loop and the final handoff.
   The Claude Code agents with `r2` should be in parallel, and the output should
   be flushed every 100 lines and written to stdout preceded with
   ">>>>> From agent #<pr_number>".
   It pushes the generated commits to the branch corresponding to the AK commit.
   It updates the status for that PR to (4: implementation done).
9. last two steps can be done in parallel.
10. finally after all agents complete successfully with a semaphore command it saves the sqlite database as a project artifact.

---

## Implementation details (added during build)

The following details emerged during implementation under
`tools/translation_agent/`. They refine — not contradict — the 10-step
contract above.

### Per-PR worktrees for plan/impl (steps 6 and 8)

Each `r2 sandbox claude` invocation for plan generation (step 6) and
implementation (step 8) runs inside a temporary git worktree at
`/tmp/translation-agent-<branch>-XXXXXX`. The orchestrator owns this
isolation rather than relying on the runner — same code path works
under the real Semaphore `r2 sandbox` and the local `dev-bin/r2`
emulation wrapper. Worktrees are removed on context exit in real runs;
preserved on disk in `--dry-run` for operator inspection.

### Worktree bootstrap: `make` + submodule bump commit

Before spawning claude in each worktree, the orchestrator:

1. Runs `make` (the Makefile's default target, which transitively runs
   `git submodule update --init --recursive`, builds the Rust crate,
   builds C bindings, and creates the Python venv). This gives claude
   a fully-built workspace with the C headers it might reference.
2. Checks out `<ak_commit>` in the `kafka/` submodule and commits the
   submodule pointer bump as a standalone commit:
   `Bump kafka submodule to <ak_commit>`.
3. THEN invokes claude. Claude's `Design document` commit (step 6) or
   implementation commits (step 8) sit on top of the bump commit. Each
   PR's branch ends with a clean two-commit (or N+1-commit) shape.

### Dependency evaluation does NOT use a worktree

Step 4 (dep-eval via `r2 sandbox claude`) is read-only and emits JSON
to stdout, so it runs in the orchestrator's own working directory with
no per-PR isolation. Cheaper, and there's nothing to push or commit
that could conflict.

### `branch_commit` cursor advance after step 8

When step 8 succeeds for a PR, the orchestrator inserts a new row into
`branch_commit` recording `(rust_branch, new_rust_commit) ↔
(ak_branch, ak_commit)`. This is what makes the orchestrator
**resumable across sweeps** — without it, the next sweep would re-walk
the same 10 AK commits from the original seed. The status update and
the `branch_commit` insert happen in a single sqlite transaction.

### Initial seed of `branch_commit`

Bootstrapped via a CLI subcommand:
`translation-agent --seed --ak-branch <> --ak-commit <> --rust-branch
<> --rust-commit <>` (idempotent — re-runs are no-ops). The Semaphore
`seed.yml` task wraps this for first-time setup of a new project.

### CLI shape (single binary, four invocation modes)

| Mode | Command shape | Triggers |
|---|---|---|
| Sweep | `translation-agent --ak-repo-path <> --ak-branch <> --rust-branch <>` | Steps 1–6, 8, 10 |
| Per-PR status | `translation-agent --pr <N>` | Read-only check (step 7) |
| Per-PR approve | `translation-agent --pr <N> --plan-approve` | Step 7 + cascade into step 8 for that PR |
| Seed | `translation-agent --seed --ak-branch <> --ak-commit <> --rust-branch <> --rust-commit <>` | Bootstrap `branch_commit` |

Cross-cutting flags: `--db-path`, `--max-parallel` (default 4),
`--no-artifact-push`, `--dry-run`, `--verbose`, `--artifact-name`,
`--rust-repo-path`.

### Failure handling: `last_error` column

Added a `last_error TEXT` column to `pr_commit`. On any subprocess
failure (`r2`, `gh`, `git`), the failure message is written to
`last_error` and the PR's status is left unchanged. The next sweep
naturally retries those rows. Avoids inventing a separate "failed"
status — the state machine stays a clean 0→1→2→3→4 ladder.

### Dry-run mode (`--dry-run`)

Local-development knob that skips destructive remote operations:
`git push`, `gh pr create`, the Semaphore `artifact push`. But it DOES:

- Insert `pr_commit` rows with **synthetic negative `pr_number`s**
  derived from `sha1(ak_commit)` (deterministic across re-runs).
- Run dep-eval / plan / impl `r2 sandbox claude` calls **iff `r2` is
  on PATH**. Plan/impl runs use preserved worktrees for inspection.
- Advance DB status locally (sqlite-only effect; never pushed to the
  Semaphore artifact since artifact push is also skipped).

Cleanup recipe: `DELETE FROM pr_commit WHERE pr_number < 0` to drop
synthetic rows when switching the same DB path to a real run.

### Concurrency

Steps 6 and 8 share a single `ThreadPoolExecutor(max_workers=N)` per
design step 9. The unblocked-predicate query keeps the two task types
dependency-safe — no two-pass scan needed.

### Local emulation: `dev-bin/r2`

A bash wrapper at `tools/translation_agent/dev-bin/r2` translates the
orchestrator's `r2 sandbox claude -p "<prompt>"` invocations into
`claude --permission-mode dontAsk --verbose --allowedTools <list> -p
"<prompt>"` against the locally-installed Claude Code CLI. Lets the
full pipeline be tested end-to-end on a developer workstation without
the real Semaphore `r2 sandbox` runner. The allow-list grants
`Read`, `Edit`, `Write`, `ExitPlanMode`, `Bash(git *)`, `Bash(cargo
*)`, plus a few common shell utilities.

### Semaphore CI configuration

Three pipeline files under `.semaphore/`:

| File | Trigger | What it runs |
|---|---|---|
| `semaphore.yml` | every push / PR | Conditional: sweep on `${MAIN_BRANCH}`; `--pr <N>` on PR builds; no-op otherwise |
| `plan-approve.yml` | manual promotion from a PR build | `translation-agent --pr <N> --plan-approve` |
| `seed.yml` | manual Task | `translation-agent --seed ...` |

Each pipeline pulls the artifact at start (`artifact pull project ...`)
and lets the orchestrator handle `artifact push` itself.

### Implementation phase breakdown

Built in five Actor/Critic cycles per `.claude/rules/agent-roles.md`:

- **Phase A** — sqlite schema + CLI skeleton + Semaphore push.
- **Phase B** — sweep steps 1–3 (cursor lookup, branch + draft PR).
- **Phase C** — sweep steps 4–5 (parallel dep-eval via r2).
- **Phase D** — steps 6, 7, 8 (plan, plan-approve cascade, impl).
- **Phase E** — end-to-end glue, artifact push, README, integration tests.

108 unit + integration tests at `tools/translation_agent/tests/`.
