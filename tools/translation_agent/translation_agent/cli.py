# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Argparse entry point for the translation agent.

Three invocation modes per the design:

- Sweep mode (default): `translation-agent --ak-repo-path ... --rust-branch
  ...` — runs design steps 1..6+8. The AK branch is read from the
  `branch_commit` cursor row whose primary key is `--rust-branch`.
- Per-PR mode: `translation-agent --pr <N> [--plan-approve]` — design step 7.
- Seed mode: `translation-agent --seed --ak-branch ... --ak-commit ...
  --rust-branch ... [--force] [--cleanup-prs]` -- bootstraps the
  `branch_commit` table on first use. `--force` overwrites an existing
  cursor; `--cleanup-prs` deletes every `pr_commit` row for
  `--rust-branch` before seeding.
- Delete-PRs mode: `translation-agent --delete-prs --rust-branch ...
  --pr-numbers N1,N2,...` -- targeted destructive cleanup of an
  explicit list of stale/failed PRs. Deletes the GitHub head branch
  (auto-closing the PR) and the `pr_commit` row for each. `--rust-branch`
  is a safety scope: any PR whose stored branch differs aborts the run.
"""

import argparse
import datetime as _dt
import hashlib
import logging
import shutil
import sys
from pathlib import Path
from typing import Optional, Sequence

from . import db, git_ops, github, locked_db, prompts, r2, streaming, worktree


def _db_session(
    args: "argparse.Namespace", *,
    write: bool,
    allow_missing: bool = False,
):
    """Open a locked_db.session driven by `args`.

    Both `--dry-run` and `--no-artifact-push` skip the lock + pull +
    push subprocess calls. In dry-run mode we don't talk to Semaphore
    at all; with --no-artifact-push the operator wants real LLM/git
    work but no artifact RPCs -- typically used for local runs without
    the Semaphore `artifact` CLI installed.

    `allow_missing=True` should ONLY be set by seed mode. In every
    other mode, a missing or unreachable artifact must hard-fail
    rather than let the orchestrator operate against stale local
    state and then push-overwrite the canonical artifact with our
    outdated view.
    """
    return locked_db.session(
        args.db_path,
        write=write,
        dry_run=args.dry_run or args.no_artifact_push,
        allow_missing=allow_missing,
    )


def _r2_available() -> bool:
    """True iff the `r2` binary is on PATH. Cheap, can be called per-sweep."""
    return shutil.which("r2") is not None


def _synthetic_pr_number(ak_commit: str) -> int:
    """Deterministic negative integer for dry-run pr_commit rows.

    Real GitHub PR numbers are positive sequential integers, so negative
    values are unambiguously synthetic. Derived from a sha1 of the AK
    commit (rather than parsing the AK SHA's hex directly) so the
    function works on any input string -- helpful for tests that pass
    synthetic AK identifiers.

    Idempotency: re-running dry-run on the same AK commit hits the
    same synthetic pr_number, so INSERT OR IGNORE preserves prior dep-
    eval results. Cleanup is one SQL: `DELETE FROM pr_commit WHERE
    pr_number < 0`.
    """
    h = hashlib.sha1(ak_commit.encode()).hexdigest()
    n = int(h[:7], 16)
    return -(n or 1)


log = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="translation-agent",
        description=(
            "Orchestrator producing Rust translations of Apache Kafka "
            "commits via r2 sandbox claude. See "
            "design/history/Milestone-7/DESIGN-translation-agent.md."
        ),
    )
    parser.add_argument(
        "--db-path", default="./translation_agent.db",
        help="Path to the sqlite state DB (default: %(default)s).",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable DEBUG logging.",
    )
    parser.add_argument(
        "--no-artifact-push", action="store_true",
        help="Skip the Semaphore artifact push at the end of the run.",
    )
    parser.add_argument(
        "--artifact-name", default="translation_agent.db",
        help="Semaphore project-artifact name for the sqlite DB "
             "(default: %(default)s).",
    )

    # Mode flags. Mutually exclusive so we can keep the spec wording literal:
    #   default       sweep mode
    #   --pr N        per-PR mode (status check or --plan-approve)
    #   --seed        seed mode
    #   --delete-prs  targeted destructive cleanup
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--pr", type=int, metavar="N", help="Operate on a single PR.")
    mode.add_argument("--seed", action="store_true",
                      help="Insert a row into branch_commit (idempotent).")
    mode.add_argument(
        "--delete-prs", action="store_true",
        help="Delete pr_commit rows AND GitHub head branches for an "
             "explicit list of PR numbers on --rust-branch. Requires "
             "--rust-branch and --pr-numbers.",
    )

    parser.add_argument(
        "--plan-approve", action="store_true",
        help=(
            "With --pr <N>: transition that PR from status 2 (plan_created) "
            "to 3 (plan_approved) and continue with implementation. Used by "
            "Semaphore CI manual-promotion jobs."
        ),
    )

    # Sweep-mode args.
    parser.add_argument("--ak-repo-path", help="Path to a local clone of the AK repo.")
    parser.add_argument(
        "--ak-branch",
        help="AK branch (only required for --seed; sweep reads it from the "
             "branch_commit cursor).",
    )
    parser.add_argument("--rust-branch", help="Rust branch to write PRs against.")
    parser.add_argument(
        "--rust-repo-path", default=".",
        help="Path to a local checkout of the Rust repo (default: %(default)s).",
    )
    parser.add_argument(
        "--max-parallel", type=int, default=4,
        help="Max parallel r2 sandbox invocations (default: %(default)s).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print intent without invoking r2/gh/git/artifact.",
    )

    # Seed-mode args (--rust-branch is shared with sweep mode; --ak-branch
    # is required for seed only).
    parser.add_argument("--ak-commit", help="AK commit hash (for --seed).")
    parser.add_argument(
        "--force", action="store_true",
        help="With --seed: overwrite the branch_commit cursor for the "
             "given --rust-branch even if a row already exists with "
             "different ak/rust commits. Without --force, an existing "
             "row with different values causes an error.",
    )
    parser.add_argument(
        "--cleanup-prs", action="store_true",
        help="With --seed: delete every pr_commit row for --rust-branch "
             "before seeding. Used to reset a branch's PR queue when "
             "stale/failed rows would otherwise be picked up by the "
             "next sweep's unblocked-predicate checks. Independent of "
             "--force; the two compose.",
    )
    parser.add_argument(
        "--allow-missing-artifact", action="store_true",
        help="With --seed only: tolerate a DB-artifact pull failure "
             "and proceed to create a fresh local DB. Use this ONLY "
             "for the very first bootstrap of a new project where "
             "the artifact does not exist yet. For re-seeds (where "
             "the artifact should exist), do NOT pass this flag -- "
             "without it, a transient pull failure becomes a hard "
             "error rather than silently overwriting the canonical "
             "artifact with an empty local DB.",
    )

    # --delete-prs args.
    parser.add_argument(
        "--pr-numbers", metavar="N1,N2,...",
        help="Comma-separated list of PR numbers (no spaces) for "
             "--delete-prs.",
    )

    return parser


def _parse_pr_numbers(raw: str) -> list[int] | None:
    """Parse a comma-separated PR-number string into a list of ints.

    Returns None on any malformed input (empty, whitespace-only,
    non-integer token, duplicate). The caller logs the user-facing
    error and returns rc=2; this helper just signals "bad input."
    """
    parts = [p for p in raw.split(",") if p != ""]
    if not parts:
        return None
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return None
    if len(set(nums)) != len(nums):
        return None
    return nums


def _run_delete_prs(args: argparse.Namespace) -> int:
    if not args.rust_branch or not args.pr_numbers:
        missing = []
        if not args.rust_branch:
            missing.append("--rust-branch")
        if not args.pr_numbers:
            missing.append("--pr-numbers")
        log.error("--delete-prs requires: %s", ", ".join(missing))
        return 2

    pr_numbers = _parse_pr_numbers(args.pr_numbers)
    if pr_numbers is None:
        log.error(
            "--delete-prs: --pr-numbers must be a non-empty comma-"
            "separated list of unique integers (e.g. '123,456'); got %r",
            args.pr_numbers,
        )
        return 2

    # Pre-flight pass: validate everything BEFORE any mutation. A
    # failure here means rc=1 with the DB and GitHub completely
    # untouched -- the operator's --rust-branch acts as a safety scope
    # against typos that would otherwise delete the wrong PRs.
    targets: list[tuple[int, str]] = []
    for n in pr_numbers:
        with _db_session(args, write=False) as conn:
            row = db.get_pr(conn, n)
        if row is None:
            log.error(
                "--delete-prs: PR %d not found in pr_commit -- aborting",
                n,
            )
            return 1
        if row["rust_branch"] != args.rust_branch:
            log.error(
                "--delete-prs: PR %d belongs to rust_branch=%s, not %s "
                "-- aborting (no changes made)",
                n, row["rust_branch"], args.rust_branch,
            )
            return 1
        targets.append((n, github.branch_name_for_ak(row["ak_commit"])))

    if args.dry_run:
        log.info(
            "--delete-prs --dry-run: %d PR(s) would be deleted on rust_branch=%s",
            len(targets), args.rust_branch,
        )
        for pr_number, branch in targets:
            log.info("  would delete: PR %d branch=%s", pr_number, branch)
        return 0

    # Execute. Per design: GitHub branch FIRST (closes the PR
    # implicitly), then DB row. Fail-fast on real gh errors (auth,
    # network, 5xx). A "branch already gone" response is a soft
    # success: log it, then proceed to delete the DB row so the run
    # continues with remaining PRs.
    for pr_number, branch in targets:
        try:
            github.delete_remote_branch(args.rust_repo_path, branch)
            branch_outcome = "removed"
        except github.GhBranchAlreadyGone as e:
            log.info(
                "--delete-prs: branch %s for PR %d already gone on remote, "
                "proceeding to remove DB row (%s)",
                branch, pr_number, e,
            )
            branch_outcome = "already gone"
        except github.GhError as e:
            log.error(
                "--delete-prs: failed to delete branch %s for PR %d: %s "
                "-- aborting (DB row not touched for this PR)",
                branch, pr_number, e,
            )
            return 1
        with _db_session(args, write=True) as conn:
            db.delete_pr_commit(conn, pr_number)
        log.info(
            "Deleted PR %d (branch=%s %s, pr_commit row removed)",
            pr_number, branch, branch_outcome,
        )
    return 0


def _run_seed(args: argparse.Namespace) -> int:
    required = ("ak_branch", "ak_commit", "rust_branch")
    missing = [f"--{r.replace('_', '-')}" for r in required if not getattr(args, r)]
    if missing:
        log.error("--seed requires: %s", ", ".join(missing))
        return 2

    if args.cleanup_prs:
        # Run before the seed so the seed log line is the last thing
        # the operator sees and matches the post-state of the DB.
        # Always log the count (even 0) so the operator gets
        # confirmation the flag took effect.
        # allow_missing comes from the operator-supplied
        # --allow-missing-artifact flag; True only for first-ever
        # bootstrap. For re-seeds the operator omits the flag so a
        # transient pull failure surfaces loudly rather than silently
        # creating an empty DB and overwriting the canonical artifact.
        with _db_session(
            args, write=True, allow_missing=args.allow_missing_artifact,
        ) as conn:
            deleted = db.cleanup_pr_commits_for_rust_branch(
                conn, args.rust_branch,
            )
        log.info(
            "Cleaned up %d pr_commit row(s) for rust_branch=%s",
            deleted, args.rust_branch,
        )

    try:
        with _db_session(
            args, write=True, allow_missing=args.allow_missing_artifact,
        ) as conn:
            result = db.seed_correspondence(
                conn,
                args.ak_branch, args.ak_commit, args.rust_branch,
                force=args.force,
            )
    except ValueError as e:
        log.error("%s", e)
        return 1
    if result == "inserted":
        log.info(
            "Inserted branch_commit (rust=%s -> ak=%s/%s)",
            args.rust_branch, args.ak_branch, args.ak_commit,
        )
    elif result == "updated":
        log.info(
            "Updated branch_commit cursor for rust_branch=%s -> ak=%s/%s "
            "[--force]",
            args.rust_branch, args.ak_branch, args.ak_commit,
        )
    else:  # "unchanged"
        log.info(
            "branch_commit cursor for rust_branch=%s already at ak=%s/%s "
            "-- no change",
            args.rust_branch, args.ak_branch, args.ak_commit,
        )
    return 0


def _run_pr_mode(args: argparse.Namespace) -> int:
    """Entry point for `--pr N` mode.

    Reads the row's current status and runs the next applicable step,
    cascading until it hits the human gate (status 2 = plan_created)
    or completes the impl transition 3 -> 4. Each step's DB I/O is in
    its own `_db_session` block so the lock is held only for the read
    or write op, never across the LLM call.
    """
    with _db_session(args, write=False) as conn:
        pr = db.get_pr(conn, args.pr)

    if pr is None:
        # - `--pr <N>` (status check, auto-triggered by Semaphore on
        #   every PR build): missing rows are the NORMAL case for any
        #   PR that isn't a translation PR managed by the orchestrator.
        #   Return 0 so we don't fail CI for unrelated PRs.
        # - `--pr <N> --plan-approve` (manual promotion): approving a
        #   plan for a non-existent PR is a real operator error;
        #   surface it loudly with rc=1.
        if args.plan_approve:
            log.error(
                "No pr_commit row for PR %d -- cannot --plan-approve a PR "
                "the orchestrator doesn't know about", args.pr,
            )
            return 1
        log.info(
            "No pr_commit row for PR %d -- not a translation PR managed "
            "by the orchestrator, skipping", args.pr,
        )
        return 0

    if args.plan_approve:
        return _run_pr_plan_approve(args, pr)

    # Cascade loop. Re-read the row at the top of every iteration so
    # that if another runner advanced it between my steps, I see the
    # new status and dispatch correctly (or noop). Bound at 5 as a
    # defensive cap -- the status enum naturally bounds it at 2
    # transitions per invocation (0 -> 1 -> 2 stops at the human gate).
    for _ in range(5):
        with _db_session(args, write=False) as conn:
            pr = db.get_pr(conn, args.pr)
        if pr is None:
            log.info(
                "PR %d row vanished mid-cascade; stopping.", args.pr,
            )
            return 0
        st = pr["status"]

        if st == db.STATUS_NO_PLAN:                  # 0
            err = _do_dep_eval_step(args, pr)
            if err is not None:
                return 1
            continue  # cascade to status 1

        if st == db.STATUS_DEPENDENCIES_EVALUATED:   # 1
            err = _do_plan_step(args, pr)
            if err is not None:
                return 1
            return 0  # human gate at status 2

        if st == db.STATUS_PLAN_CREATED:             # 2
            log.info(
                "PR #%d at status 2 (plan_created); "
                "waiting for manual --plan-approve.",
                args.pr,
            )
            return 0

        if st == db.STATUS_PLAN_APPROVED:            # 3
            err = _do_impl_step(args, pr)
            if err is not None:
                return 1
            return 0

        if st == db.STATUS_IMPLEMENTATION_DONE:      # 4
            log.info(
                "PR #%d already at status 4 (implementation_done); "
                "no work to do.",
                args.pr,
            )
            return 0

        log.warning(
            "PR #%d at unexpected status %d; stopping cascade.",
            args.pr, st,
        )
        return 1

    log.warning(
        "Cascade for PR #%d did not terminate after 5 iterations.",
        args.pr,
    )
    return 1


def _run_pr_plan_approve(args, pr) -> int:
    """Handle `--pr N --plan-approve`. Idempotent on the impl side: if
    the row is already at status 3 (impl was started but didn't finish,
    or the operator re-promoted), skip mark_plan_approved and run impl
    directly. Status 4 noop. Status 0/1 raise ValueError out of
    mark_plan_approved -> return 1 so operator misuse is loud.
    """
    if pr["status"] >= db.STATUS_IMPLEMENTATION_DONE:
        log.info(
            "PR %d already at status %d (implementation_done); "
            "nothing to do for --plan-approve.",
            args.pr, pr["status"],
        )
        return 0

    if pr["status"] < db.STATUS_PLAN_APPROVED:
        try:
            with _db_session(args, write=True) as conn:
                db.mark_plan_approved(conn, args.pr)
        except ValueError as e:
            log.error("%s", e)
            return 1
        log.info(
            "PR %d marked plan_approved (status %d)",
            args.pr, db.STATUS_PLAN_APPROVED,
        )

        # Programmatic body marker for the 2 -> 3 transition. No LLM
        # call: nothing has happened content-wise vs status 2, so a
        # full regeneration would just rewrite near-identical text.
        # Skipped for synthetic dry-run rows (no real PR) and dry-run
        # mode (no remote writes). Cosmetic: failure is a warning,
        # not a blocker.
        if not args.dry_run and args.pr >= 0:
            approval_line = (
                f"✓ Plan approved on {_dt.date.today().isoformat()}"
            )
            try:
                github.prepend_pr_body(
                    args.rust_repo_path, args.pr, approval_line,
                )
                log.info(
                    "PR #%d: prepended approval marker to body", args.pr,
                )
            except github.GhError as e:
                log.warning(
                    "PR #%d approval body prepend failed: %s",
                    args.pr, e,
                )

        # Re-fetch the row after the status transition.
        with _db_session(args, write=False) as conn:
            pr = db.get_pr(conn, args.pr)

    if pr["ak_branch"] is None:
        log.error(
            "PR %d has no ak_branch recorded -- cannot record "
            "correspondence after implementation. This row predates "
            "the schema with ak_branch; re-create it via a sweep.",
            args.pr,
        )
        return 1

    err = _do_impl_step(args, pr)
    if err is not None:
        return 1
    return 0


def _dep_blocks_step(
    args, pr, dep_column: str, threshold: int, phase: str,
) -> bool:
    """True iff this PR's `dep_column` (an AK SHA) points to a PR on the
    same rust_branch whose status is below `threshold`. False if the
    dep is satisfied (status >= threshold) or unresolvable (the dep PR
    row was deleted or archived and the dep column wasn't nulled).

    Replaces the SQL-JOIN-based gating that lived inside the deleted
    `get_unblocked_for_status` query: per-PR cascade looks up one dep
    at a time rather than scanning the whole branch.

    Phase ("plan"/"impl") just controls the log message wording.
    """
    pr_number = pr["pr_number"]
    dep_sha = pr[dep_column]
    with _db_session(args, write=False) as conn:
        dep_row = db.get_pr_commit_by_branch_and_ak(
            conn, pr["rust_branch"], dep_sha,
        )
    if dep_row is None:
        # The dep column points at an AK SHA we no longer have a row
        # for -- either the dep was deleted via --delete-prs without
        # nulling out dependents, or some other inconsistency. Stalling
        # is safer than running impl on broken state.
        log.info(
            "PR #%d %s blocked: dep AK %s has no pr_commit row "
            "on rust_branch=%s (likely deleted out-of-band) -- "
            "row stays at status %d",
            pr_number, phase, dep_sha[:12],
            pr["rust_branch"], pr["status"],
        )
        return True
    if dep_row["status"] < threshold:
        log.info(
            "PR #%d %s blocked: dep PR #%s (AK %s) is at status %d, "
            "need >= %d -- row stays at status %d",
            pr_number, phase, dep_row["pr_number"], dep_sha[:12],
            dep_row["status"], threshold, pr["status"],
        )
        return True
    return False


def _do_dep_eval_step(args, pr):
    """Run dep-eval for a status-0 row. Computes the bounded candidate
    range from the cursor to this PR's ak_commit, invokes _dep_eval_one
    (which calls r2 outside the lock), then writes the resulting deps
    under a write session. Returns None on success or a non-empty
    error string on failure (already persisted via set_last_error).
    """
    pr_number = pr["pr_number"]

    # Guard at the precondition boundary: the cascade enters dep-eval
    # for status-0 rows and IMMEDIATELY needs args.ak_repo_path to call
    # git_ops.commits_between. Without it, we'd crash 4 frames deep in
    # subprocess.run with "expected str, bytes or os.PathLike object,
    # not NoneType". Better to fail loud here with an actionable message
    # the operator can act on, persisted to last_error so it's visible
    # in the pr_commit row after the run.
    if not args.ak_repo_path:
        err = (
            "--ak-repo-path is required for dep-eval (cascading "
            "status 0 -> 1 needs the AK git repo to compute the "
            "candidate dep range). Re-run with --ak-repo-path set, "
            "or set AK_REPO_PATH in the Semaphore env."
        )
        log.error("PR #%d: %s", pr_number, err)
        with _db_session(args, write=True) as conn:
            db.set_last_error(conn, pr_number, err)
        return err

    with _db_session(args, write=False) as conn:
        cursor = db.get_latest_correspondence(conn, pr["rust_branch"])
    if cursor is None:
        err = (
            f"no branch_commit cursor for rust_branch={pr['rust_branch']}; "
            f"cannot determine dep-eval candidate range"
        )
        log.error("PR #%d: %s", pr_number, err)
        with _db_session(args, write=True) as conn:
            db.set_last_error(conn, pr_number, err)
        return err

    try:
        candidate_aks = git_ops.commits_between(
            args.ak_repo_path,
            since=cursor["ak_commit"],
            until=pr["ak_commit"],
            branch=pr["ak_branch"] or cursor["ak_branch"],
        )
    except git_ops.GitError as e:
        err = f"failed to compute dep candidates: {e}"
        log.error("PR #%d: %s", pr_number, err)
        with _db_session(args, write=True) as conn:
            db.set_last_error(conn, pr_number, err)
        return err

    if args.dry_run and not _r2_available():
        log.info(
            "[dry-run] would dep-eval PR #%d (AK %s) -- "
            "r2 not on PATH, skipping",
            pr_number, pr["ak_commit"][:12],
        )
        return None
    if args.dry_run:
        log.info(
            "[dry-run] r2 is on PATH -- running dep-eval for PR #%d",
            pr_number,
        )

    plan_dep, impl_dep, err_msg = _dep_eval_one(args, pr, candidate_aks)
    if err_msg:
        log.error("PR #%d: %s", pr_number, err_msg)
        if not args.dry_run:
            with _db_session(args, write=True) as conn:
                db.set_last_error(conn, pr_number, err_msg)
        return err_msg

    # Out-of-candidate-range deps treated as None (per spec: a dep must
    # be a still-in-flight commit between the cursor and this PR).
    valid_aks = set(candidate_aks)
    if plan_dep and plan_dep not in valid_aks:
        log.warning(
            "PR #%d plan_dep %s not in candidates -- treating as None",
            pr_number, plan_dep[:12],
        )
        plan_dep = None
    if impl_dep and impl_dep not in valid_aks:
        log.warning(
            "PR #%d impl_dep %s not in candidates -- treating as None",
            pr_number, impl_dep[:12],
        )
        impl_dep = None

    with _db_session(args, write=True) as conn:
        db.update_dependencies(conn, pr_number, plan_dep, impl_dep)
    log.info(
        "PR #%d -> status %d (plan_dep=%s, impl_dep=%s)",
        pr_number, db.STATUS_DEPENDENCIES_EVALUATED,
        (plan_dep[:12] if plan_dep else None),
        (impl_dep[:12] if impl_dep else None),
    )

    plan_dep_pr = _lookup_dep_pr_number(args, pr["rust_branch"], plan_dep)
    impl_dep_pr = _lookup_dep_pr_number(args, pr["rust_branch"], impl_dep)
    _update_pr_dep_section(args, pr_number, plan_dep_pr, impl_dep_pr)
    _apply_label_transition(
        args, pr_number, add=(prompts.LABEL_DEPENDENCIES_EVALUATED,),
    )
    return None


def _do_plan_step(args, pr):
    """Run plan generation for a status-1 row. Returns None on success
    or a non-empty error string on failure.

    Gated on `pr["plan_dependency"]`: if there's a plan_dep AK SHA,
    the dep PR's status must be >= STATUS_PLAN_APPROVED. Otherwise the
    step noops (returns None) and the row stays at status 1 -- a future
    `--pr N` build (after the dep advances) will pick it up.
    """
    pr_number = pr["pr_number"]

    if pr["plan_dependency"]:
        if _dep_blocks_step(
            args, pr, "plan_dependency", db.STATUS_PLAN_APPROVED, "plan",
        ):
            return None

    if args.dry_run and not _r2_available():
        log.info(
            "[dry-run] would generate plan for PR #%d -- "
            "r2 not on PATH, skipping",
            pr_number,
        )
        return None
    if args.dry_run:
        log.info(
            "[dry-run] r2 is on PATH -- generating plan for PR #%d "
            "(no push, worktree preserved)",
            pr_number,
        )

    err, _ = _run_plan_one(args, pr)
    if err:
        log.error("PR #%d (plan): %s", pr_number, err)
        if not args.dry_run:
            with _db_session(args, write=True) as conn:
                db.set_last_error(conn, pr_number, err)
        return err

    label = " [dry-run]" if args.dry_run else ""
    with _db_session(args, write=True) as conn:
        db.mark_plan_created(conn, pr_number)
    log.info(
        "PR #%d -> status %d (plan_created)%s",
        pr_number, db.STATUS_PLAN_CREATED, label,
    )
    _apply_label_transition(
        args, pr_number,
        remove=(prompts.LABEL_DEPENDENCIES_EVALUATED,),
        add=(prompts.LABEL_PLAN_CREATED,),
    )
    return None


def _do_impl_step(args, pr):
    """Run implementation for a status-3 row. Returns None on success
    or a non-empty error string on failure.

    Gated on `pr["implementation_dependency"]`: if there's an impl_dep
    AK SHA, the dep PR's status must be >= STATUS_IMPLEMENTATION_DONE.
    Otherwise the step noops (returns None) and the row stays at
    status 3 -- a future `--pr N` (after the dep advances) picks it up.
    """
    pr_number = pr["pr_number"]

    if pr["implementation_dependency"]:
        if _dep_blocks_step(
            args, pr, "implementation_dependency",
            db.STATUS_IMPLEMENTATION_DONE, "impl",
        ):
            return None

    if args.dry_run and not _r2_available():
        log.info(
            "[dry-run] would implement PR #%d -- r2 not on PATH, skipping",
            pr_number,
        )
        return None
    if args.dry_run:
        log.info(
            "[dry-run] r2 is on PATH -- running impl for PR #%d "
            "(no push, worktree preserved)",
            pr_number,
        )

    err, sha = _run_impl_one(args, pr)
    if err:
        log.error("PR #%d (impl): %s", pr_number, err)
        if not args.dry_run:
            with _db_session(args, write=True) as conn:
                db.set_last_error(conn, pr_number, err)
        return err

    ak_branch = pr["ak_branch"] or args.ak_branch
    with _db_session(args, write=True) as conn:
        db.mark_implementation_done(
            conn, pr_number,
            ak_branch=ak_branch,
            ak_commit=pr["ak_commit"],
            rust_branch=pr["rust_branch"],
        )
    label = " [dry-run]" if args.dry_run else ""
    log.info(
        "PR #%d -> status %d (implementation_done) sha=%s%s",
        pr_number, db.STATUS_IMPLEMENTATION_DONE,
        sha[:12] if sha else "??", label,
    )
    _apply_label_transition(
        args, pr_number,
        remove=(
            prompts.LABEL_DEPENDENCIES_EVALUATED,
            prompts.LABEL_PLAN_CREATED,
            prompts.LABEL_IMPLEMENTATION_NEEDED,
        ),
        add=(prompts.LABEL_IMPLEMENTATION_DONE,),
    )
    return None


def _check_pr_closures_and_advance_cursor(
    args: argparse.Namespace, ak_commits, cursor,
) -> "Optional[str]":
    """Walk `ak_commits` in chronological order; for each one whose
    pr_commit row is present AND whose GitHub PR is CLOSED or MERGED,
    delete the row and remember the AK commit as the new cursor
    candidate. Stop at the first row that's missing, synthetic
    (pr_number < 0), still OPEN, or hits a gh-view failure.

    On any cleanup, advance `branch_commit` for `args.rust_branch`
    via `db.seed_correspondence(..., force=True)`.

    Returns the new cursor `ak_commit` value if advanced, or None
    otherwise. No-ops in dry-run mode and when ak_commits is empty.

    Each DB op runs inside its own `_db_session` so the lock is not
    held across the GitHub `gh pr view` round-trip in the loop body --
    those network calls happen entirely outside the lock.
    """
    if args.dry_run or not ak_commits:
        return None

    new_ak = None

    for ak in ak_commits:
        with _db_session(args, write=False) as conn:
            row = db.get_pr_commit_by_branch_and_ak(
                conn, args.rust_branch, ak,
            )
        if row is None:
            break  # unprocessed commit -> stop
        if row["pr_number"] is None or row["pr_number"] < 0:
            break  # synthetic dry-run row -> stop
        try:
            state, merge_sha = github.get_pr_state(
                args.rust_repo_path, row["pr_number"],
            )
        except github.GhError as e:
            log.warning(
                "PR closure check: gh failed for #%d: %s -- "
                "stopping walk, proceeding with the rest of the sweep",
                row["pr_number"], e,
            )
            break
        if state == "OPEN":
            break  # still in flight -> stop
        # CLOSED or MERGED -> archive (MERGED only) + null-out deps + delete.
        with _db_session(args, write=True) as conn:
            db.archive_pr_commit(
                conn, row["pr_number"],
                rust_commit=merge_sha if state == "MERGED" else None,
            )
        new_ak = ak
        if state == "MERGED":
            log.info(
                "PR #%d (MERGED) for AK %s: archived rust=%s to "
                "pr_commit_history; nulled dependents; removed pr_commit row",
                row["pr_number"], ak[:12], (merge_sha or "")[:12],
            )
        else:
            log.info(
                "PR #%d (CLOSED) for AK %s: nulled dependents; removed "
                "pr_commit row (no merge SHA to archive)",
                row["pr_number"], ak[:12],
            )

    if new_ak is not None:
        with _db_session(args, write=True) as conn:
            db.seed_correspondence(
                conn, cursor["ak_branch"], new_ak,
                args.rust_branch, force=True,
            )
        log.info("Cursor advanced: ak=%s", new_ak[:12])
    return new_ak


def _run_sweep(args: argparse.Namespace) -> int:
    required = ("ak_repo_path", "rust_branch")
    # Sweep narrowed to global cursor administration: dep-eval + plan +
    # impl now run per-PR (driven by Semaphore on each PR build)
    num_commits = 10
    missing = [f"--{r.replace('_', '-')}" for r in required if not getattr(args, r)]
    if missing:
        log.error("sweep mode requires: %s", ", ".join(missing))
        return 2

    # Step 2: find the AK cursor for this Rust branch. The cursor row (PK is
    # rust_branch alone) tells us which AK branch + commit we're tracking, so
    # sweep mode does not take --ak-branch on the CLI.
    with _db_session(args, write=False) as conn:
        cursor = db.get_latest_correspondence(conn, args.rust_branch)
    if cursor is None:
        log.error(
            "No branch_commit row for rust_branch=%s. Use --seed to bootstrap.",
            args.rust_branch,
        )
        return 1
    ak_branch = cursor["ak_branch"]
    log.info(
        "Cursor: rust=%s -> AK=%s/%s",
        cursor["rust_branch"], ak_branch, cursor["ak_commit"],
    )

    # Step 3: get the next 10 AK commits.
    try:
        ak_commits = git_ops.next_commits(
            args.ak_repo_path, since=cursor["ak_commit"], branch=ak_branch, n=num_commits,
        )
    except git_ops.GitError as e:
        log.error("Failed to read AK commits: %s", e)
        return 1
    if not ak_commits:
        log.info("No new AK commits to translate; will still process existing rows.")
    else:
        log.info("Found %d new AK commit(s) on %s", len(ak_commits), ak_branch)

    # Prune the contiguous prefix of ak_commits whose PRs are already
    # CLOSED or MERGED on GitHub: delete each pr_commit row and advance
    # the branch_commit cursor accordingly. If the cursor moved, re-fetch
    # the next batch from the new position before creating new PRs.
    new_cursor_ak = _check_pr_closures_and_advance_cursor(
        args, ak_commits, cursor,
    )
    if new_cursor_ak is not None:
        with _db_session(args, write=False) as conn:
            cursor = db.get_latest_correspondence(conn, args.rust_branch)
        try:
            ak_commits = git_ops.next_commits(
                args.ak_repo_path, since=cursor["ak_commit"],
                branch=ak_branch, n=num_commits,
            )
        except git_ops.GitError as e:
            log.error(
                "Failed to re-read AK commits after cursor advance: %s", e,
            )
            return 1
        log.info(
            "Re-fetched %d AK commit(s) after cursor advance to %s",
            len(ak_commits), new_cursor_ak[:12],
        )

    # Step 3 (cont): create branches + draft PRs, insert into pr_commit.
    new_pr_count = 0
    for ak_commit in ak_commits:
        rc = _create_pr_for_ak_commit(args, ak_branch, ak_commit)
        if rc:
            new_pr_count += 1

    log.info("Sweep step 3 done. Created %d new PR(s).", new_pr_count)

    # Sweep is intentionally limited to: closure check + cursor advance
    # + create new draft PRs. Dep-evaluation, plan generation, and
    # implementation are driven by Semaphore on each PR build via
    # `--pr <N>` (see _run_pr_mode), which cascades through the row's
    # statuses until it hits the human gate at status 2 (or completes
    # 3 -> 4 after manual --plan-approve).
    return 0


def _dep_eval_one(args, row, candidate_aks):
    """Run r2 sandbox claude for dep-eval on one commit.

    `candidate_aks` is the set of AK SHAs the inner claude is told to
    consider as possible dependencies. With per-PR cascade dispatch
    (one row at a time), this is the bounded range
    `git log <cursor>..<this PR's ak_commit>` on the AK branch -- i.e.
    every still-in-flight commit on the same Rust branch that's an
    ancestor of this PR's commit.

    Returns `(plan_dep, impl_dep, err)`. On success err is None and the
    deps may each be a SHA string or None. On failure plan_dep and
    impl_dep are both None and err is a non-empty error message.
    """
    pr_number = row["pr_number"]
    ak_commit = row["ak_commit"]
    other = [sha for sha in candidate_aks if sha != ak_commit]
    batch_listing = (
        "\n".join(f"- {sha}" for sha in other)
        if other else "(no other candidate commits)"
    )
    prompt = prompts.DEPENDENCY_EVAL_PROMPT_TEMPLATE.format(
        ak_commit=ak_commit,
        ak_repo_path=args.ak_repo_path,
        batch_listing=batch_listing,
    )
    try:
        rc, captured = streaming.run_with_prefix(
            [*r2.R2_CLAUDE_CMD_PREFIX, "-p", prompt],
            pr_number=pr_number,
        )
    except FileNotFoundError as e:
        return None, None, f"r2 not on PATH: {e}"
    except Exception as e:
        return None, None, f"r2 invocation crashed: {e}"
    if rc != 0:
        return None, None, f"r2 dep-eval failed (rc={rc})"
    parsed = prompts.parse_dep_eval_json(captured)
    if parsed is None:
        return None, None, "could not parse dep-eval JSON from r2 output"
    return parsed[0], parsed[1], None


def _lookup_dep_pr_number(
    args: argparse.Namespace, rust_branch: str, ak_commit: Optional[str],
) -> Optional[int]:
    """Resolve a dep AK SHA to its pr_commit.pr_number on `rust_branch`,
    or None if the SHA is None or no row matches. Synthetic dry-run
    rows (negative pr_number) are returned as-is and treated as None
    by callers (the dep section won't render a #-link for them).

    Opens its own `_db_session` so the lock is held only for the
    single read and released before any caller-side I/O.
    """
    if not ak_commit:
        return None
    with _db_session(args, write=False) as conn:
        row = db.get_pr_commit_by_branch_and_ak(conn, rust_branch, ak_commit)
    if row is None:
        return None
    pr_number = row["pr_number"]
    if pr_number is None or pr_number < 0:
        return None
    return pr_number


def _apply_label_transition(
    args: argparse.Namespace,
    pr_number: int,
    *,
    add: "tuple[str, ...]" = (),
    remove: "tuple[str, ...]" = (),
) -> None:
    """Apply +add / -remove labels to PR `pr_number`. Per-label failures
    log a warning and continue (the orchestrator never fails the sweep
    over a cosmetic labeling problem). No-ops in dry-run and for
    synthetic pr_numbers (< 0).
    """
    if pr_number is None or pr_number < 0:
        return
    if args.dry_run:
        if add or remove:
            log.info(
                "[dry-run] PR #%d: would +%s -%s",
                pr_number, list(add), list(remove),
            )
        return
    for label in remove:
        try:
            github.remove_pr_label(args.rust_repo_path, pr_number, label)
            log.info("PR #%d: removed label %r", pr_number, label)
        except github.GhError as e:
            log.warning(
                "PR #%d: failed to remove %r label: %s",
                pr_number, label, e,
            )
    for label in add:
        try:
            github.add_pr_label(args.rust_repo_path, pr_number, label)
            log.info("PR #%d: added label %r", pr_number, label)
        except github.GhError as e:
            log.warning(
                "PR #%d: failed to add %r label: %s",
                pr_number, label, e,
            )


def _update_pr_dep_section(
    args: argparse.Namespace,
    pr_number: int,
    plan_dep_pr_number: Optional[int],
    impl_dep_pr_number: Optional[int],
) -> None:
    """Read PR `pr_number`'s body, replace the orchestrator-managed
    dependency section in place, and write the result back.

    Idempotent on retry (replace_dep_section strips any existing block
    before prepending). No-ops in dry-run and for synthetic pr_numbers.
    Failures log a warning and continue (cosmetic).
    """
    if pr_number is None or pr_number < 0:
        return
    if args.dry_run:
        log.info(
            "[dry-run] PR #%d: would set dep section (plan=#%s, impl=#%s)",
            pr_number, plan_dep_pr_number, impl_dep_pr_number,
        )
        return
    section = github.format_dep_section(
        plan_dep_pr_number, impl_dep_pr_number,
    )
    try:
        body = github.get_pr_body(args.rust_repo_path, pr_number)
    except github.GhError as e:
        log.warning(
            "PR #%d: failed to read body for dep-section update: %s",
            pr_number, e,
        )
        return
    new_body = github.replace_dep_section(body, section)
    if new_body == body:
        return
    try:
        github.update_pr_body(args.rust_repo_path, pr_number, new_body)
        log.info(
            "PR #%d: dep section updated (plan=#%s, impl=#%s)",
            pr_number, plan_dep_pr_number, impl_dep_pr_number,
        )
    except github.GhError as e:
        log.warning(
            "PR #%d: failed to update body with dep section: %s",
            pr_number, e,
        )


def _update_pr_description_via_r2(
    args: argparse.Namespace, row, phase: str, wt: Path,
) -> Optional[str]:
    """Inside an open worktree, invoke r2 to write ./pr_body.md, then
    publish via `gh pr edit <N> --body-file -`. Returns None on success
    or an error string. Intended to be called from `_run_plan_one`
    (phase="plan") and `_run_impl_one` (phase="impl") right after the
    orchestrator's `git_ops.push_branch` succeeds.

    No-ops for synthetic dry-run rows (pr_number < 0) and dry-run mode
    in general (logs intent only). Description-update failures are
    cosmetic; callers log them as warnings and continue rather than
    rolling back the state transition.
    """
    pr_number = row["pr_number"]
    if pr_number is None or pr_number < 0:
        return None  # synthetic dry-run row, no real PR to edit
    if args.dry_run:
        log.info(
            "[dry-run] PR #%d: would regenerate description (%s phase)",
            pr_number, phase,
        )
        return None
    branch_name = github.branch_name_for_ak(row["ak_commit"])
    prompt = prompts.PR_DESCRIPTION_PROMPT_TEMPLATE.format(
        phase=phase,
        pr_number=pr_number,
        branch_name=branch_name,
        base_branch=args.rust_branch,
        ak_commit=row["ak_commit"],
    )
    try:
        rc, _ = streaming.run_with_prefix(
            [*r2.R2_CLAUDE_CMD_PREFIX, "-p", prompt],
            pr_number=pr_number,
            cwd=str(wt),
        )
    except FileNotFoundError as e:
        return f"r2 not on PATH: {e}"
    except Exception as e:
        return f"r2 description invocation crashed: {e}"
    if rc != 0:
        return f"r2 description failed (rc={rc})"
    body_path = wt / "pr_body.md"
    if not body_path.exists():
        return "r2 description did not produce ./pr_body.md"
    try:
        body = body_path.read_text()
    except OSError as e:
        return f"failed to read ./pr_body.md: {e}"
    if not body.strip():
        return "./pr_body.md is empty"
    try:
        github.update_pr_body(args.rust_repo_path, pr_number, body)
    except github.GhError as e:
        return f"gh pr edit failed: {e}"
    log.info("PR #%d: updated description (%s phase)", pr_number, phase)
    # Plan-phase post-publish: choose the label action based on which
    # of the two canonical markers claude emitted in the body.
    #   - NO_IMPLEMENTATION_NEEDED_MARKER (no-op plan): skip the
    #     implementation-needed label and proactively remove it (in case
    #     a previous body had set it on a re-plan). PR stays at
    #     STATUS_PLAN_CREATED for human review.
    #   - IMPLEMENTATION_NEEDED_MARKER: apply the label as before.
    #   - Neither: leave the label state untouched (matches the existing
    #     "claude omitted the marker" branch).
    # If both markers somehow appear, the no-op branch wins -- safer
    # default than triggering an unwanted implementation run. Label
    # failures are cosmetic; warn and continue rather than rolling back
    # the description update.
    if phase == "plan":
        if prompts.NO_IMPLEMENTATION_NEEDED_MARKER in body:
            log.info(
                "PR #%d: plan declared no-op; skipping %r label",
                pr_number, prompts.LABEL_IMPLEMENTATION_NEEDED,
            )
            try:
                github.remove_pr_label(
                    args.rust_repo_path, pr_number,
                    prompts.LABEL_IMPLEMENTATION_NEEDED,
                )
            except github.GhError as e:
                log.warning(
                    "PR #%d: failed to remove %r label: %s",
                    pr_number, prompts.LABEL_IMPLEMENTATION_NEEDED, e,
                )
        elif prompts.IMPLEMENTATION_NEEDED_MARKER in body:
            try:
                github.add_pr_label(
                    args.rust_repo_path, pr_number,
                    prompts.LABEL_IMPLEMENTATION_NEEDED,
                )
                log.info(
                    "PR #%d: labeled %r",
                    pr_number, prompts.LABEL_IMPLEMENTATION_NEEDED,
                )
            except github.GhError as e:
                log.warning(
                    "PR #%d: failed to add %r label: %s",
                    pr_number, prompts.LABEL_IMPLEMENTATION_NEEDED, e,
                )
    return None


def _run_plan_one(args, row):
    """Returns (err, None). err is None on success.

    In dry-run mode the worktree is preserved on disk for inspection and
    the prompt is augmented with a "do not push" suffix.
    """
    pr_number = row["pr_number"]
    ak_commit = row["ak_commit"]
    branch_name = github.branch_name_for_ak(ak_commit)
    prompt = prompts.PLAN_GENERATION_PROMPT_TEMPLATE.format(
        ak_commit=ak_commit,
        ak_branch=row["ak_branch"] or "(unknown)",
        pr_number=pr_number,
        branch_name=branch_name,
    )
    if args.dry_run:
        prompt = prompt + "\n" + prompts.DRY_RUN_NOTE
    try:
        with worktree.worktree_for_branch(
            args.rust_repo_path, branch_name,
            cleanup=not args.dry_run,
            base_remote_branch=(
                args.rust_branch if args.dry_run else None
            ),
            ak_commit=ak_commit,
            ak_branch=row["ak_branch"] or "trunk",
        ) as wt:
            try:
                rc, _ = streaming.run_with_prefix(
                    [*r2.R2_CLAUDE_CMD_PREFIX, "-p", prompt],
                    pr_number=pr_number,
                    cwd=str(wt),
                )
            except FileNotFoundError as e:
                return f"r2 not on PATH: {e}", None
            except Exception as e:
                return f"r2 plan invocation crashed: {e}", None
            if rc != 0:
                return f"r2 plan failed (rc={rc})", None
            if args.dry_run:
                log.info(
                    "[dry-run] PR #%d plan worktree preserved at %s",
                    pr_number, wt,
                )
            else:
                # The R2 sandbox denies `git push`, so claude only
                # committed locally. Publish the plan commit ourselves.
                # force=True: kafka-translate/<sha> is exclusively
                # orchestrator-owned, and the plan worktree's local tip
                # IS the authoritative state -- if origin diverged
                # (e.g. master advanced and an earlier sweep step
                # rebased the branch, or a prior failed run left a
                # stale tip), unconditionally overwrite.
                try:
                    git_ops.push_branch(
                        args.rust_repo_path, branch_name, force=True,
                    )
                except git_ops.GitError as e:
                    return (
                        f"failed to push plan branch {branch_name}: {e}",
                        None,
                    )
                # Refresh the PR body now that the plan is published.
                # Cosmetic; failure is a warning, not a blocker.
                desc_err = _update_pr_description_via_r2(
                    args, row, "plan", wt,
                )
                if desc_err:
                    log.warning(
                        "PR #%d description update failed: %s",
                        pr_number, desc_err,
                    )
    except worktree.WorktreeError as e:
        return f"worktree setup failed: {e}", None
    return None, None


def _run_impl_one(args, row):
    """Returns (err, new_rust_commit_sha).

    In dry-run mode: prompt augmented with "do not push", worktree
    preserved on disk; SHA is read from the **local** branch ref
    (claude's commits there) since nothing was pushed to origin.
    """
    pr_number = row["pr_number"]
    ak_commit = row["ak_commit"]
    branch_name = github.branch_name_for_ak(ak_commit)
    prompt = prompts.IMPLEMENTATION_PROMPT_TEMPLATE.format(
        ak_commit=ak_commit,
        ak_branch=row["ak_branch"] or "(unknown)",
        pr_number=pr_number,
        branch_name=branch_name,
    )
    if args.dry_run:
        prompt = prompt + "\n" + prompts.DRY_RUN_NOTE
    try:
        with worktree.worktree_for_branch(
            args.rust_repo_path, branch_name,
            cleanup=not args.dry_run,
            base_remote_branch=(
                args.rust_branch if args.dry_run else None
            ),
            ak_commit=ak_commit,
            ak_branch=row["ak_branch"] or "trunk",
        ) as wt:
            try:
                rc, _ = streaming.run_with_prefix(
                    [*r2.R2_CLAUDE_CMD_PREFIX, "-p", prompt],
                    pr_number=pr_number,
                    cwd=str(wt),
                )
            except FileNotFoundError as e:
                return f"r2 not on PATH: {e}", None
            except Exception as e:
                return f"r2 impl invocation crashed: {e}", None
            if rc != 0:
                return f"r2 impl failed (rc={rc})", None
            # Read the local branch tip -- claude's commits in the
            # worktree advance the local ref directly. We're the source
            # of truth for what gets pushed, so no fetch needed.
            try:
                sha = git_ops.rev_parse(args.rust_repo_path, branch_name)
            except git_ops.GitError as e:
                return (
                    f"failed to read local commit on {branch_name}: {e}",
                    None,
                )
            if args.dry_run:
                log.info(
                    "[dry-run] PR #%d impl worktree preserved at %s",
                    pr_number, wt,
                )
                return None, sha
            # Real run: the R2 sandbox denies `git push`, so publish
            # the impl commits ourselves before returning the SHA that
            # branch_commit will record. force=True for the same reason
            # as the plan push (orchestrator-owned branch, local tip is
            # authoritative).
            try:
                git_ops.push_branch(
                    args.rust_repo_path, branch_name, force=True,
                )
            except git_ops.GitError as e:
                return (
                    f"failed to push impl branch {branch_name}: {e}",
                    None,
                )
            # Refresh the PR body now that the implementation is
            # published. Overwrites whatever the plan-phase claude
            # wrote (and whatever approval prepended). Cosmetic;
            # failure is a warning, not a blocker.
            desc_err = _update_pr_description_via_r2(
                args, row, "impl", wt,
            )
            if desc_err:
                log.warning(
                    "PR #%d description update failed: %s",
                    pr_number, desc_err,
                )
    except worktree.WorktreeError as e:
        return f"worktree setup failed: {e}", None
    return None, sha


def _create_pr_for_ak_commit(
    args: argparse.Namespace, ak_branch: str, ak_commit: str,
) -> bool:
    """Create the branch + draft PR + pr_commit row for a single AK commit.

    Returns True if a new pr_commit row was inserted, False otherwise (PR
    already existed, or an error occurred and was logged). Errors here
    don't abort the sweep -- we log and move on so other commits aren't
    blocked by a single failure.

    In dry-run mode the git push and gh-pr-create are skipped, but the
    pr_commit row IS inserted with a synthetic negative pr_number derived
    from the AK SHA -- so subsequent dry-run dep-eval can read it.
    """
    branch_name = github.branch_name_for_ak(ak_commit)
    try:
        subject = git_ops.commit_subject(args.ak_repo_path, ak_commit)
    except git_ops.GitError as e:
        log.error("Failed to read AK commit %s subject: %s", ak_commit[:12], e)
        return False
    title = github.pr_title_for_ak(ak_commit, subject)
    body = github.pr_body_for_ak(ak_commit, subject)

    if args.dry_run:
        pr_number = _synthetic_pr_number(ak_commit)
        log.info(
            "[dry-run] would bump kafka -> %s on branch %s, push, and "
            "create draft PR %r (synthetic pr_number=%d)",
            ak_commit[:12], branch_name, title, pr_number,
        )
    else:
        try:
            # Branch is created with one commit (the kafka submodule
            # bump) on top of origin/<rust-branch> so `gh pr create`
            # has a real diff to PR -- otherwise GitHub's GraphQL
            # rejects with "No commits between <base> and <head>".
            worktree.push_branch_with_kafka_bump(
                args.rust_repo_path,
                branch_name,
                base_remote_branch=args.rust_branch,
                ak_commit=ak_commit,
                ak_branch=ak_branch,
            )
        except worktree.WorktreeError as e:
            log.error("Failed to bump+push branch %s: %s", branch_name, e)
            return False
        try:
            pr_number = github.create_draft_pr(
                args.rust_repo_path,
                base_branch=args.rust_branch,
                head_branch=branch_name,
                title=title, body=body,
            )
        except github.GhPrAlreadyExists:
            # Idempotent re-run: recover the existing PR number so we can
            # ensure pr_commit has a row for it.
            try:
                existing = github.find_pr_number_for_branch(
                    args.rust_repo_path, branch_name,
                )
            except github.GhError as e:
                log.error("Failed to look up existing PR for %s: %s", branch_name, e)
                return False
            if existing is None:
                log.error("PR reportedly exists for %s but lookup found none", branch_name)
                return False
            pr_number = existing
            log.info("PR #%d already exists for %s -- not duplicating", pr_number, branch_name)
        except github.GhError as e:
            log.error("Failed to create PR for %s: %s", branch_name, e)
            return False

    with _db_session(args, write=True) as conn:
        inserted = db.insert_pr_commit(
            conn, pr_number, args.rust_branch, ak_branch, ak_commit,
        )
    label = "[dry-run] " if args.dry_run else ""
    if inserted:
        log.info(
            "%sCreated PR #%d for AK commit %s -- status=%d (%s), %s",
            label, pr_number, ak_commit[:12],
            db.STATUS_NO_PLAN,
            db.STATUS_NAMES.get(db.STATUS_NO_PLAN, "?"),
            _next_sweep_action_for_status(db.STATUS_NO_PLAN),
        )
    else:
        with _db_session(args, write=False) as conn:
            existing = db.get_pr(conn, pr_number)
        status = existing["status"] if existing is not None else None
        if status is None:
            log.info(
                "%sPR #%d already in pr_commit -- left unchanged",
                label, pr_number,
            )
        else:
            # Show dep SHAs only for status >= 1 (status 0 hasn't been
            # dep-evaluated yet, so both are always NULL there).
            dep_str = (
                _dep_summary(existing) + ", "
                if status >= db.STATUS_DEPENDENCIES_EVALUATED else ""
            )
            log.info(
                "%sPR #%d already in pr_commit -- status=%d (%s), %s%s",
                label, pr_number,
                status, db.STATUS_NAMES.get(status, "?"),
                dep_str,
                _next_sweep_action_for_status(status),
            )
    return inserted


def _dep_summary(row) -> str:
    """Compact one-liner of a row's two dependencies, formatted as
    `plan_dep=<sha12 or ->, impl_dep=<sha12 or ->`. Used in the
    per-PR sweep log so the operator can see WHICH commits a row is
    waiting on without querying sqlite. 12-char SHA matches the
    convention used elsewhere in orchestrator logs."""
    def _fmt(sha):
        return sha[:12] if sha else "-"
    return (
        f"plan_dep={_fmt(row['plan_dependency'])}, "
        f"impl_dep={_fmt(row['implementation_dependency'])}"
    )


def _next_sweep_action_for_status(status: int) -> str:
    """Human-readable description of what (if anything) this sweep will
    do for a row at `status`. Used by the per-PR log line in
    `_create_pr_for_ak_commit` so operators can see at a glance what
    each existing row is destined for without having to read the
    state machine in their head."""
    if status == db.STATUS_NO_PLAN:
        return "will dep-eval next"
    if status == db.STATUS_DEPENDENCIES_EVALUATED:
        return "will generate plan when its plan_dependency is approved"
    if status == db.STATUS_PLAN_CREATED:
        return "waiting for manual --plan-approve (no automatic action)"
    if status == db.STATUS_PLAN_APPROVED:
        return "will run implementation when its impl_dependency is done"
    if status == db.STATUS_IMPLEMENTATION_DONE:
        return "complete (no automatic action)"
    return f"unknown status -- no automatic action"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    )
    # No long-lived sqlite connection here: every db.* call goes through
    # `_db_session` -> `locked_db.session` -> per-op lock + pull + (push
    # if write) + release. The end-of-run artifact push that used to
    # live here is no longer needed: each write is published as soon as
    # its session commits.

    # --allow-missing-artifact is meaningful only with --seed. Reject
    # the combination loudly elsewhere so an operator who sets it
    # expecting it to apply (e.g. for sweep on a fresh project) finds
    # out immediately rather than getting silent fail-loud behavior.
    if args.allow_missing_artifact and not args.seed:
        log.error(
            "--allow-missing-artifact is only valid with --seed. "
            "It exists to tolerate a missing DB artifact during the "
            "very first bootstrap; for sweep / --pr / --delete-prs "
            "modes the artifact must always exist (operating against "
            "stale local state and pushing back would lose data)."
        )
        return 2

    if args.seed:
        return _run_seed(args)
    if args.delete_prs:
        return _run_delete_prs(args)
    if args.pr is not None:
        return _run_pr_mode(args)
    return _run_sweep(args)


if __name__ == "__main__":
    sys.exit(main())
