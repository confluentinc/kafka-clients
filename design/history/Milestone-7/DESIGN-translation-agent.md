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
3. after checking the next 10 commits of the base AK commit it must verify, scanning the commits in order, if they're present in the          
  `pr_commit` table and if the corresponding PR was closed or merged. In this case it continues checking the next commit until it finds one where the PR is missing or still open. For the commits with PRs closed it removes the row from the table and finally updates the corresponding AK commit in the `branch_commit` table to the last closed one.
  or merged. After that it selects the next N commits again and continues the sweep phase.
4. it get the next 10 commits on AK branch and for each commit it creates a branch and a PR, starting from the initial AK branch.
   It inserts into a table `pr_commit`. This table has the PR number, the Rust branch, the corresponding AK commit
   and two optional columns `plan_dependency` and `implementation_dependency` that contain commits (hashes) that
   are a precondition before planning this commit translation or before starting the implementation.
   The table also contain a status enum:
   - 0: no plan
   - 1: dependencies evaluated
   - 2: plan created
   - 3: plan approved
   - 4: implementation done
5. for each PR that has status (0: no plan) it starts Claude Code with r2 command, like:
   `r2 sandbox claude -p "Claude Code prompt"`, to identify the dependencies
   of that commit for planning or for implementing. It outputs the dependencies in a JSON file.
   There should be only a single `plan_dependency` and a single `implementation_dependency`:
   the latest commit that is a dependency.
6. the application reads the dependencies and updates the `pr_commit` table with those
   and sets the status to (1: dependencies evaluated)
7. for each PR that has status (1: dependencies evaluated) and has no `plan_dependency`
   or the plan dependency is not among those in the table (open ones) or present but with
   status >= (3: plan approved), it runs claude with `r2` and asks the manager
   agent to create a plan and to save it to `./design/history/<pr_number>_description/plan.md`.
   The Claude Code runs with `r2` should be in parallel, and the output should
   be flushed every 100 lines and written to stdout preceded with
   ">>>>> From agent #<pr_number>".
   Each agent commits the plan and pushes it to the branch corresponding to the AK commit.
   The commit message should be "Design document". It updates the status for that PRs to
   (2: plan created). 
8. when run with `--pr <number>` and `--plan-approve` it changes the status of the corresponding
   PR from (2: plan created) to (3: plan approved) and continues with (8).
   When run with `--pr <number>` only it just checks the status of that PR.
   `--plan-approve` happens when the Semaphore CI PR pipeline is running and a manual promotion is triggered.
9. for each PR that has status (3: plan approved) and has no `implementation_dependency`
   or the implementation dependency is not among those in the table (open ones),
   it runs claude with `r2` and asks the manager
   agent to start the implementation of the plan at `./design/history/<pr_number>_description/plan.md`.
   Running the actor and critic loop and the final handoff.
   The Claude Code agents with `r2` should be in parallel, and the output should
   be flushed every 100 lines and written to stdout preceded with
   ">>>>> From agent #<pr_number>".
   It pushes the generated commits to the branch corresponding to the AK commit.
   It updates the status for that PR to (4: implementation done).
10. last two steps can be done in parallel.
11. finally after all agents complete successfully with a semaphore command it saves the sqlite database as a project artifact.

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

Before spawning claude in each plan/impl worktree, the orchestrator:

1. Runs `make` (the Makefile's default target, which transitively runs
   `git submodule update --init --recursive`, builds the Rust crate,
   builds C bindings, and creates the Python venv). This gives claude
   a fully-built workspace with the C headers it might reference.
2. Checks out `<ak_commit>` in the `kafka/` submodule and commits the
   submodule pointer bump as a standalone commit:
   `Bump kafka submodule to <ak_commit>`. Idempotent: the commit is
   skipped when the submodule pointer is already at `<ak_commit>`
   (`git diff --cached --quiet` guard) so re-runs don't double-commit.
3. THEN invokes claude. Claude's `Design document` commit (step 6) or
   implementation commits (step 8) sit on top of the bump commit. Each
   PR's branch ends with a clean two-commit (or N+1-commit) shape.

A lighter variant of the same flow is used in step 3 (PR creation,
see below) -- it skips `make` and only runs
`git submodule update --init kafka` so it's seconds rather than
minutes.

### Worktree base-ref resolution

The worktree manager picks the source ref for `git worktree add` in
this priority order:

1. **Remote branch exists** (probed via `git ls-remote --heads
   origin <branch>`): fetch it and base on `FETCH_HEAD`. Handles the
   re-run case where a previous sweep created the PR branch but
   didn't complete -- the new worktree picks up the existing bump
   commit and any plan/impl commits already on origin, so the bump
   becomes a no-op and the eventual push is a clean fast-forward.
2. **Local branch exists, no remote**: reuse the local branch. This
   is the dry-run cascade case where prior plan/impl commits live
   only locally because dry-run never pushed.
3. **Brand new**: fetch `origin/<base_remote_branch>` (the rust
   branch) and base on `FETCH_HEAD`.

Both fetch paths use `FETCH_HEAD` rather than `origin/<branch>`
because Semaphore's depth-50 single-branch clone has a refspec
restricted to the original branch -- fetching any other branch
updates `FETCH_HEAD` but does NOT create
`refs/remotes/origin/<other-branch>`, so `origin/<branch>` doesn't
resolve. `-B` is used to force-reset the local branch in case a
stale ref exists from a previous run.

Stale worktrees on the same branch (typically left over from a
previous dry-run with `cleanup=False`) are evicted via
`git worktree remove --force` before adding a new one, otherwise
`git worktree add` errors with "branch is already used by worktree."

### Step 3 PR creation includes a kafka-submodule bump commit

GitHub's `createPullRequest` GraphQL refuses to open a PR when the
head branch is at the same commit as the base. So before
`gh pr create`, the orchestrator opens a lightweight worktree
(no `make`, just `git submodule update --init kafka`), runs the same
`_bump_kafka_submodule` helper to record the pointer commit, and
pushes. The new branch lands on origin with one real commit (the
bump), giving GitHub a non-empty diff to PR.

### Sandbox contract: agent commits, orchestrator publishes

`git push` is denied at the R2 sandbox boundary; the orchestrator
runs every push itself after the R2 invocation returns. Same for
`gh pr edit`. Three layers enforce this:

1. The plan/impl prompts explicitly tell claude not to push and
   explain that the orchestrator does it.
2. `dev-bin/r2`'s deny-list has explicit `Bash(git push)` /
   `Bash(git push:*)` / `Bash(git push *)` and the same triple for
   `gh pr edit`. (`gh` is also absent from the allow-list, so it's
   already denied by exclusion — the explicit deny is defense in
   depth and survives an inadvertent allow-list expansion.)
3. The orchestrator owns the publish: `_run_plan_one` and
   `_run_impl_one` call `git_ops.push_branch` after R2 returns rc=0;
   PR-body updates flow through `github.update_pr_body` /
   `prepend_pr_body`.

The implementation reads the new commit SHA from the LOCAL ref
(`git rev-parse <branch_name>`) rather than fetching `origin/<branch>`
— since the orchestrator just performed the push, no round-trip
through the remote is needed.

### PR description refresh on each transition

The PR description is regenerated to reflect the latest progress at
each state transition:

| Transition | Body update mechanism |
|---|---|
| Plan created (1 → 2) | `r2 sandbox claude` reads `plan.md`, writes `./pr_body.md`; orchestrator publishes via `gh pr edit <N> --body-file -` |
| Plan approved (2 → 3) | Programmatic prepend `✓ Plan approved on YYYY-MM-DD` (no LLM call — no new content vs status 2) |
| Implementation done (3 → 4) | `r2 sandbox claude` reads commits + plan.md, writes `./pr_body.md`; orchestrator publishes |

Description-update failures are logged as WARNING and do NOT reverse
the state transition — the body is cosmetic, not load-bearing. The
next phase transition naturally overwrites stale bodies, so transient
gh failures self-heal without a retry loop coupling the queue to
GitHub API availability.

Synthetic dry-run rows (`pr_number < 0`) and dry-run mode skip the
update entirely (no real PR to edit).

### Sweep PR-closure check (prefix-prune + cursor advance)

After fetching the next N AK commits, the sweep walks them in
chronological order and prunes the contiguous prefix of commits whose
`pr_commit` row exists AND whose GitHub PR is CLOSED or MERGED. For
each pruned commit, `db.archive_pr_commit(pr_number, rust_commit=...)`
runs three steps in a single transaction:

1. **Archive (MERGED only)**: insert
   `(rust_branch, ak_branch, ak_commit, merge_sha)` into the
   `pr_commit_history` audit table via `INSERT OR REPLACE` keyed on
   `(rust_branch, ak_branch, ak_commit)`. The merge SHA comes from
   `gh pr view --json state,mergeCommit` and works uniformly across
   GitHub's three merge styles (merge commit / squash / rebase) —
   `mergeCommit.oid` is the right base-branch commit in every case.
   CLOSED-without-merge PRs have no merge SHA and are NOT archived.
2. **Discharge dependents**: NULL out `plan_dependency` and
   `implementation_dependency` in any other `pr_commit` row on the
   **same `rust_branch`** that referenced this row's `ak_commit`. The
   dependency is logically discharged once the dep PR is gone — the
   downstream PR shouldn't wait forever. Scoped to the same
   `rust_branch` because branches are independent translation queues.
3. **Delete** the `pr_commit` row.

The AK commit is then remembered as the new cursor candidate, and
the `branch_commit` cursor for `rust_branch` advances after the
walk.

The walk stops at the first commit whose row is missing, synthetic
(`pr_number < 0`), still OPEN, or hits a `gh pr view` failure. After
the walk, if any rows were pruned, the sweep re-fetches the next N
AK commits from the advanced cursor before creating new PRs.

Prefix-only (not middle-of-batch) is deliberate. Walking the entire
batch would either need N `gh pr view` calls per sweep or would
discharge dependents in an unexpected order; the prefix-only rule
keeps the closure check predictable, cheap, and easy to reason
about. The next sweep naturally compacts further closures as the
prefix advances.

`gh pr view` transient failures log a WARNING and stop the walk —
the closure check is best-effort and shouldn't abort the sweep.
Dry-run skips the entire check. `--seed --cleanup-prs` (a manual
queue reset) also bypasses `archive_pr_commit` entirely: it does a
bulk DELETE without writing history rows or nulling dependents,
because those rows are typically stale/failed/dry-run garbage rather
than real PR resolutions.

### `pr_commit_history`: AK→Rust merge audit log

A separate, append-style table records every MERGED PR's AK→Rust
correspondence after the live `pr_commit` row is removed:

```
CREATE TABLE pr_commit_history (
    rust_branch  TEXT NOT NULL,
    ak_branch    TEXT NOT NULL,
    ak_commit    TEXT NOT NULL,
    rust_commit  TEXT NOT NULL,
    PRIMARY KEY (rust_branch, ak_branch, ak_commit)
)
```

The PK on `(rust_branch, ak_branch, ak_commit)` enforces one entry
per AK commit per branch. `INSERT OR REPLACE` semantics mean a
re-archive (e.g. operator did `--cleanup-prs`, recreated the PR,
re-merged) overwrites with the latest merge SHA — the row reflects
the *current* truth, not the first archive. The `branch_commit`
cursor itself stays AK-only; this table is what holds the Rust SHA
so it survives the live row's deletion.

### `next_commits`: shallow-clone-aware range over the AK repo

Semaphore's prologue does `git submodule update --init --depth=1
kafka`, leaving the AK submodule with a single-commit history. The
orchestrator's cursor is almost always older than that. `next_commits`
handles the shallow case end-to-end:

1. Always run `git fetch <remote> <branch>` first so `FETCH_HEAD`
   reflects the remote's actual current tip — without this, the local
   `origin/<branch>` ref is stuck at the submodule pointer SHA and
   the range's upper bound stays months stale.
2. Ensure the cursor is **reachable** from `FETCH_HEAD`, not merely
   present in the local DB. The probe is `_commit_present` AND
   `merge-base --is-ancestor`. The Semaphore submodule init can land
   the cursor object as a separate shallow root, disconnected from
   the branch tip; only deepening materializes the path between
   them. The deepen loop uses `git fetch --deepen=50 <remote>
   <branch>` and retries the transient
   `fatal: shallow file has changed since we read it` race up to
   three times with a 100ms backoff.
3. Use `FETCH_HEAD` (not bare `<branch>`) as the range upper bound
   so the log walks against the just-fetched tip.
4. Drop `--max-count` from the `git log --reverse` call and slice in
   Python to return the N OLDEST in range. Git's `--max-count` is
   applied during the default newest-first walk, so
   `git log --reverse <range> --max-count=N` returns the N NEWEST in
   range (displayed oldest-first) — wrong for sweep ordering, where
   we need the next N chronologically after the cursor so the cursor
   advances commit-by-commit rather than skipping the middle.

If the cursor genuinely isn't on the branch (full clone, or shallow
clone fully unshallowed without finding ancestry), `next_commits`
raises GitError with a message that pinpoints the failure mode and
suggests `--seed --force`.

### Per-PR sweep log shows status, deps, next action

For each AK commit in the batch, the sweep log surfaces:

- Status code + symbolic name (`db.STATUS_NAMES`).
- Plan/impl dependency SHAs (`plan_dep=<sha12 or ->,
  impl_dep=<sha12 or ->`) for status ≥ 1.
- A one-liner describing what the sweep will do next for that
  status — e.g. `waiting for manual --plan-approve (no automatic
  action)` — so operators can see the orchestrator's plan without
  querying sqlite.

When `_run_plan_and_impl` finds nothing unblocked, it logs the row
counts at every status (`no_plan=N, plan_created=M, ...`) so the
operator can see WHY there's nothing to do.

### Dependency evaluation does NOT use a worktree

Step 4 (dep-eval via `r2 sandbox claude`) is read-only and emits JSON
to stdout, so it runs in the orchestrator's own working directory with
no per-PR isolation. Cheaper, and there's nothing to push or commit
that could conflict.

### `branch_commit` schema and cursor advance

The `branch_commit` table's primary key is `rust_branch` alone (one
cursor row per Rust branch — there is no scenario where a single
Rust branch tracks multiple AK branches simultaneously). The schema
also auto-migrates from an earlier multi-column-PK shape, so DBs
seeded before this change are upgraded in place.

When step 8 succeeds for a PR, the orchestrator updates the
`branch_commit` row for `rust_branch` via `INSERT OR REPLACE` to
record the new `(ak_branch, ak_commit)`. This is what makes the
orchestrator **resumable across sweeps** — without it, the next
sweep would re-walk the same 10 AK commits from the original seed.
The status update and the `branch_commit` write happen in a single
sqlite transaction.

The cursor also advances during the sweep PR-closure check (see
below) when one or more PRs at the front of the batch are found
already CLOSED or MERGED on GitHub.

### Initial seed of `branch_commit`

Bootstrapped via a CLI subcommand:
`translation-agent --seed --ak-branch <> --ak-commit <> --rust-branch
<> [--force] [--cleanup-prs]` — idempotent on the same values. Two
optional flags compose:

- `--force`: overwrite the cursor when a row already exists with
  different values. Without `--force`, a mismatched re-seed errors
  out so the operator notices.
- `--cleanup-prs`: delete every `pr_commit` row matching
  `--rust-branch` before seeding. Used to reset a branch's PR queue
  when stale/failed/dry-run rows would otherwise be picked up by
  the next sweep.

The Semaphore `seed.yml` task wraps this for first-time setup of a
new project. The Task exposes `AK_BRANCH`, `AK_COMMIT`, `RUST_BRANCH`,
`FORCE`, `CLEANUP_PRS` as parameters; defaults are applied shell-side
via `${VAR:-default}` rather than via task-level `env_vars` (the
latter would shadow Task-parameter values supplied at trigger time).

### CLI shape (single binary, four invocation modes)

| Mode | Command shape | Triggers |
|---|---|---|
| Sweep | `translation-agent --ak-repo-path <> --rust-branch <>` | Steps 1–6, 8, 10 (the AK branch is read from the `branch_commit` cursor row, not from a CLI flag) |
| Per-PR status | `translation-agent --pr <N>` | Read-only check (step 7). Returns 0 (not 1) when the PR row is missing — most PRs in this repo aren't translation PRs and Semaphore auto-runs this on every PR build. |
| Per-PR approve | `translation-agent --pr <N> --plan-approve` | Step 7 + cascade into step 8 for that PR. Returns 1 on missing PR row (deliberate manual promotion is an operator error if the PR isn't tracked). |
| Seed | `translation-agent --seed --ak-branch <> --ak-commit <> --rust-branch <> [--force] [--cleanup-prs]` | Bootstrap `branch_commit` |

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
`claude --permission-mode dontAsk --verbose --allowedTools <list>
--disallowedTools <denylist> -p "<prompt>"` against the locally-
installed Claude Code CLI. Lets the full pipeline be tested
end-to-end on a developer workstation without the real Semaphore
`r2 sandbox` runner.

- **Allow-list**: `Read`, `Edit`, `Write`, `ExitPlanMode`,
  `Bash(git *)`, `Bash(cargo *)`, plus common shell utilities
  (`ls`, `cat`, `grep`, `find`, `head`, `tail`, `wc`, `sed`, `awk`,
  `make`, `cmake`, `rustup`, `rustc`, `mkdir`, `pytest`, `python3`).
  `ExitPlanMode` is required because plan-generation prompts contain
  the word "plan" and would otherwise auto-trigger Claude Code's
  plan mode.
- **Deny-list**: `Bash(git push)`, `Bash(git push:*)`,
  `Bash(git push *)` and the same triple for `Bash(gh pr edit)`.
  Carves remote-write commands back out of the broad `Bash(git *)`
  / out of the absent `gh` allow-list so the orchestrator stays the
  sole publisher (see "Sandbox contract" above).

### Semaphore CI configuration

Three pipeline files under `.semaphore/`:

| File | Trigger | What it runs |
|---|---|---|
| `semaphore.yml` | every push / PR | Four-cell dispatch (truth table below) |
| `plan-approve.yml` | manual promotion from a PR build | `translation-agent --pr <N> --plan-approve` |
| `seed.yml` | manual Task | `translation-agent --seed ...` |

Each pipeline pulls the artifact at start (`artifact pull project ...`)
and lets the orchestrator handle `artifact push` itself.

**`semaphore.yml` dispatch truth table** (PR-on-main is the
most-specific case and must be tested first to avoid the sweep
clause swallowing it):

| `SEMAPHORE_GIT_BRANCH` | `SEMAPHORE_GIT_PR_NUMBER` | Action |
|---|---|---|
| = MAIN_BRANCH | set | **skip both** (PR review against main; we don't want sweep to publish new translation PRs mid-review, and the PR isn't a translation PR managed by the orchestrator) |
| = MAIN_BRANCH | empty | run sweep |
| ≠ MAIN_BRANCH | set | run `translation-agent --pr <N>` (status check) |
| ≠ MAIN_BRANCH | empty | skip |

Note Semaphore reports `SEMAPHORE_GIT_BRANCH` as the **target**
branch on PR builds (not the head), so a PR opened against main
shows `branch == MAIN_BRANCH` AND a PR number — exactly the case the
PR-on-main skip rule catches.

The prologue also handles `SEMAPHORE_GIT_BRANCH_CHECKOUT` (set when
a Task is triggered manually with a "Run on branch" override): if
present, the prologue fetches and `git checkout -B <branch>
FETCH_HEAD` (FETCH_HEAD because shallow single-branch clones don't
create `refs/remotes/origin/<other-branch>`), then the job command
shadows `SEMAPHORE_GIT_BRANCH` with the override before the dispatch
if-block fires.

### Implementation phase breakdown

Built in five Actor/Critic cycles per `.claude/rules/agent-roles.md`:

- **Phase A** — sqlite schema + CLI skeleton + Semaphore push.
- **Phase B** — sweep steps 1–3 (cursor lookup, branch + draft PR).
- **Phase C** — sweep steps 4–5 (parallel dep-eval via r2).
- **Phase D** — steps 6, 7, 8 (plan, plan-approve cascade, impl).
- **Phase E** — end-to-end glue, artifact push, README, integration tests.

Subsequent **production-hardening iterations** (driven by real
Semaphore CI runs) added the items above this section: shallow-clone
correctness in `next_commits`, no-push-from-R2 contract,
`--cleanup-prs` flag, PR-on-main skip rule, sweep PR-closure check,
worktree base resolution for re-runs over existing PR branches,
SEMAPHORE_GIT_BRANCH_CHECKOUT support, PR description refresh on each
transition, and per-PR sweep log enrichment.

170 unit + integration tests at `tools/translation_agent/tests/`.
