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

from concurrent.futures import ThreadPoolExecutor, as_completed

from . import db, git_ops, github, prompts, semaphore, streaming, worktree


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


def _run_delete_prs(args: argparse.Namespace, conn) -> int:
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
        db.delete_pr_commit(conn, pr_number)
        log.info(
            "Deleted PR %d (branch=%s %s, pr_commit row removed)",
            pr_number, branch, branch_outcome,
        )
    return 0


def _run_seed(args: argparse.Namespace, conn) -> int:
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
        deleted = db.cleanup_pr_commits_for_rust_branch(
            conn, args.rust_branch,
        )
        log.info(
            "Cleaned up %d pr_commit row(s) for rust_branch=%s",
            deleted, args.rust_branch,
        )

    try:
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


def _run_pr_mode(args: argparse.Namespace, conn) -> int:
    pr = db.get_pr(conn, args.pr)
    if pr is None:
        # Behavior diverges by intent:
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
    if not args.plan_approve:
        for k, v in pr.items():
            print(f"{k}: {v}")
        return 0

    # Step 7: flip 2 -> 3, then cascade into step 8 for this PR.
    try:
        db.mark_plan_approved(conn, args.pr)
    except ValueError as e:
        log.error("%s", e)
        return 1
    log.info("PR %d marked plan_approved (status %d)", args.pr, db.STATUS_PLAN_APPROVED)

    # Programmatic body marker for the 2 -> 3 transition. No LLM call:
    # nothing new has happened content-wise vs status 2, so a full
    # regeneration would just rewrite near-identical text. Skipped for
    # synthetic dry-run rows (no real PR) and dry-run mode (no remote
    # writes). Cosmetic: failure is a warning, not a blocker.
    if not args.dry_run and args.pr >= 0:
        approval_line = f"✓ Plan approved on {_dt.date.today().isoformat()}"
        try:
            github.prepend_pr_body(args.rust_repo_path, args.pr, approval_line)
            log.info("PR #%d: prepended approval marker to body", args.pr)
        except github.GhError as e:
            log.warning(
                "PR #%d approval body prepend failed: %s", args.pr, e,
            )

    pr = db.get_pr(conn, args.pr)
    if pr["ak_branch"] is None:
        log.error(
            "PR %d has no ak_branch recorded -- cannot record correspondence "
            "after implementation. This row predates the schema with ak_branch; "
            "re-create it via a sweep.",
            args.pr,
        )
        return 1

    if args.dry_run and not _r2_available():
        log.info(
            "[dry-run] would implement PR #%d -- r2 not on PATH, skipping",
            args.pr,
        )
        return 0
    if args.dry_run:
        log.info(
            "[dry-run] r2 is on PATH -- running impl (no push, worktree "
            "preserved)"
        )

    err, sha = _run_impl_one(args, pr)
    if err:
        log.error("PR #%d implementation failed: %s", args.pr, err)
        if not args.dry_run:
            db.set_last_error(conn, args.pr, err)
        return 1
    db.mark_implementation_done(
        conn, args.pr,
        ak_branch=pr["ak_branch"],
        ak_commit=pr["ak_commit"],
        rust_branch=pr["rust_branch"],
    )
    label = " [dry-run]" if args.dry_run else ""
    log.info(
        "PR #%d -> status %d (implementation_done) sha=%s%s",
        args.pr, db.STATUS_IMPLEMENTATION_DONE, sha[:12], label,
    )
    _apply_label_transition(
        args, args.pr,
        remove=(
            prompts.LABEL_DEPENDENCIES_EVALUATED,
            prompts.LABEL_PLAN_CREATED,
            prompts.LABEL_IMPLEMENTATION_NEEDED,
        ),
        add=(prompts.LABEL_IMPLEMENTATION_DONE,),
    )
    return 0


def _check_pr_closures_and_advance_cursor(
    args: argparse.Namespace, conn, ak_commits, cursor,
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
    """
    if args.dry_run or not ak_commits:
        return None

    new_ak = None

    for ak in ak_commits:
        row = db.get_pr_commit_by_branch_and_ak(conn, args.rust_branch, ak)
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
        db.seed_correspondence(
            conn, cursor["ak_branch"], new_ak,
            args.rust_branch, force=True,
        )
        log.info("Cursor advanced: ak=%s", new_ak[:12])
    return new_ak


def _run_sweep(args: argparse.Namespace, conn) -> int:
    required = ("ak_repo_path", "rust_branch")
    num_commits=2
    missing = [f"--{r.replace('_', '-')}" for r in required if not getattr(args, r)]
    if missing:
        log.error("sweep mode requires: %s", ", ".join(missing))
        return 2

    # Step 2: find the AK cursor for this Rust branch. The cursor row (PK is
    # rust_branch alone) tells us which AK branch + commit we're tracking, so
    # sweep mode does not take --ak-branch on the CLI.
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
        args, conn, ak_commits, cursor,
    )
    if new_cursor_ak is not None:
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
        rc = _create_pr_for_ak_commit(args, conn, ak_branch, ak_commit)
        if rc:
            new_pr_count += 1

    log.info("Sweep step 3 done. Created %d new PR(s).", new_pr_count)

    # Steps 4-5: dependency evaluation for all status-0 rows.
    _run_dep_eval(args, conn)

    # Steps 6 + 8: plan generation and implementation, dispatched concurrently
    # to a single shared executor (per design step 9). The unblocked predicate
    # keeps the two task types dependency-safe.
    _run_plan_and_impl(args, conn)

    return 0


def _run_dep_eval(args: argparse.Namespace, conn) -> None:
    """Step 4-5: for each status-0 row, run r2 dep-eval in parallel, transition 0 -> 1."""
    rows = db.get_pr_commits_by_status(
        conn, db.STATUS_NO_PLAN, rust_branch=args.rust_branch
    )
    if not rows:
        return
    log.info("Evaluating dependencies for %d status-0 PR(s)", len(rows))

    batch_aks = [r["ak_commit"] for r in rows]

    # Extended dry-run: if r2 is on PATH we DO run dep-eval (it's read-only,
    # produces JSON only) and DO persist the resulting deps. If r2 is absent
    # we just log what would happen.
    if args.dry_run and not _r2_available():
        for r in rows:
            log.info(
                "[dry-run] would dep-eval PR #%d (AK %s) -- r2 not on PATH, skipping",
                r["pr_number"], r["ak_commit"][:12],
            )
        return
    if args.dry_run:
        log.info("[dry-run] r2 is on PATH -- running dep-eval on %d row(s)", len(rows))

    with ThreadPoolExecutor(max_workers=args.max_parallel) as pool:
        futures = {
            pool.submit(_dep_eval_one, args, r, batch_aks): r for r in rows
        }
        valid_aks = set(batch_aks)
        for fut in as_completed(futures):
            row = futures[fut]
            pr_number = row["pr_number"]
            try:
                plan_dep, impl_dep, err = fut.result()
            except Exception as e:
                err = f"dep-eval worker crashed: {e}"
                plan_dep = impl_dep = None
            if err:
                log.error("PR #%d: %s", pr_number, err)
                db.set_last_error(conn, pr_number, err)
                continue
            # Out-of-batch deps are treated as None (per spec: dep must be
            # "among those in the table").
            if plan_dep and plan_dep not in valid_aks:
                log.warning(
                    "PR #%d plan_dep %s not in batch -- treating as None",
                    pr_number, plan_dep[:12],
                )
                plan_dep = None
            if impl_dep and impl_dep not in valid_aks:
                log.warning(
                    "PR #%d impl_dep %s not in batch -- treating as None",
                    pr_number, impl_dep[:12],
                )
                impl_dep = None
            db.update_dependencies(conn, pr_number, plan_dep, impl_dep)
            log.info(
                "PR #%d -> status %d (plan_dep=%s, impl_dep=%s)",
                pr_number,
                db.STATUS_DEPENDENCIES_EVALUATED,
                (plan_dep[:12] if plan_dep else None),
                (impl_dep[:12] if impl_dep else None),
            )
            # Resolve dep AK SHAs to PR numbers within the same rust
            # branch (out-of-batch deps were already coerced to None
            # above, so any non-None dep here has a pr_commit row).
            plan_dep_pr = _lookup_dep_pr_number(
                conn, args.rust_branch, plan_dep,
            )
            impl_dep_pr = _lookup_dep_pr_number(
                conn, args.rust_branch, impl_dep,
            )
            _update_pr_dep_section(
                args, pr_number, plan_dep_pr, impl_dep_pr,
            )
            _apply_label_transition(
                args, pr_number, add=(prompts.LABEL_DEPENDENCIES_EVALUATED,),
            )


def _dep_eval_one(args, row, batch_aks):
    """Run r2 sandbox claude for dep-eval on one commit.

    Returns `(plan_dep, impl_dep, err)`. On success err is None and the deps
    may each be a SHA string or None. On failure plan_dep and impl_dep are
    both None and err is a non-empty error message.
    """
    pr_number = row["pr_number"]
    ak_commit = row["ak_commit"]
    other = [sha for sha in batch_aks if sha != ak_commit]
    batch_listing = (
        "\n".join(f"- {sha}" for sha in other)
        if other else "(no other commits in this batch)"
    )
    prompt = prompts.DEPENDENCY_EVAL_PROMPT_TEMPLATE.format(
        ak_commit=ak_commit,
        ak_repo_path=args.ak_repo_path,
        batch_listing=batch_listing,
    )
    try:
        rc, captured = streaming.run_with_prefix(
            ["r2", "sandbox", "claude", "-p", prompt],
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


def _run_plan_and_impl(args: argparse.Namespace, conn) -> None:
    """Sweep steps 6 + 8: plan generation and implementation in parallel."""
    plan_rows = db.get_unblocked_for_status(
        conn,
        status=db.STATUS_DEPENDENCIES_EVALUATED,
        blocking_status_min=db.STATUS_PLAN_APPROVED,
        dep_column="plan_dependency",
        rust_branch=args.rust_branch,
    )
    impl_rows = db.get_unblocked_for_status(
        conn,
        status=db.STATUS_PLAN_APPROVED,
        blocking_status_min=db.STATUS_IMPLEMENTATION_DONE,
        dep_column="implementation_dependency",
        rust_branch=args.rust_branch,
    )
    if not plan_rows and not impl_rows:
        # Surface row counts at every status so the operator can see
        # WHY there's nothing to do this sweep. E.g. "plan_created=10"
        # means everything is waiting for manual --plan-approve, not
        # that the sweep is broken.
        breakdown = {
            s: len(db.get_pr_commits_by_status(
                conn, status=s, rust_branch=args.rust_branch,
            ))
            for s in (
                db.STATUS_NO_PLAN,
                db.STATUS_DEPENDENCIES_EVALUATED,
                db.STATUS_PLAN_CREATED,
                db.STATUS_PLAN_APPROVED,
                db.STATUS_IMPLEMENTATION_DONE,
            )
        }
        breakdown_str = ", ".join(
            f"{db.STATUS_NAMES[s]}={breakdown[s]}" for s in sorted(breakdown)
        )
        log.info(
            "No unblocked plan or implementation work this sweep "
            "(rust_branch=%s row counts: %s)",
            args.rust_branch, breakdown_str,
        )
        return
    log.info(
        "Dispatching %d plan-generation task(s) and %d implementation task(s) "
        "(max_parallel=%d)",
        len(plan_rows), len(impl_rows), args.max_parallel,
    )

    # Extended dry-run: with r2 on PATH we DO run plan/impl r2 calls
    # inside per-PR worktrees (which are preserved for inspection), but
    # the prompt tells claude not to push and we don't advance DB status
    # or update branch_commit. Without r2 we just log "would ..." like
    # before.
    if args.dry_run and not _r2_available():
        for r in plan_rows:
            log.info(
                "[dry-run] would generate plan for PR #%d -- r2 not on PATH, skipping",
                r["pr_number"],
            )
        for r in impl_rows:
            log.info(
                "[dry-run] would implement PR #%d -- r2 not on PATH, skipping",
                r["pr_number"],
            )
        return
    if args.dry_run:
        log.info(
            "[dry-run] r2 is on PATH -- running plan/impl tasks (no push, "
            "worktree preserved)"
        )

    with ThreadPoolExecutor(max_workers=args.max_parallel) as pool:
        futures = {}
        for r in plan_rows:
            futures[pool.submit(_run_plan_one, args, r)] = ("plan", r)
        for r in impl_rows:
            futures[pool.submit(_run_impl_one, args, r)] = ("impl", r)

        for fut in as_completed(futures):
            kind, row = futures[fut]
            pr_number = row["pr_number"]
            try:
                err, sha = fut.result()
            except Exception as e:
                err = f"{kind} worker crashed: {e}"
                sha = None
            if err:
                log.error("PR #%d (%s): %s", pr_number, kind, err)
                if not args.dry_run:
                    db.set_last_error(conn, pr_number, err)
                continue
            # Persist the status transition AND (for impl) the
            # branch_commit row -- in both real and dry-run mode. Dry-run
            # doesn't push to origin or to the Semaphore artifact, so the
            # write stays purely local; the operator can clean up via
            # `DELETE FROM pr_commit WHERE pr_number < 0` if they later
            # want to switch this DB path to a real run.
            label = " [dry-run]" if args.dry_run else ""
            if kind == "plan":
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
            else:
                # impl: row["ak_branch"] should be populated by the sweep.
                ak_branch = row["ak_branch"] or args.ak_branch
                db.mark_implementation_done(
                    conn, pr_number,
                    ak_branch=ak_branch,
                    ak_commit=row["ak_commit"],
                    rust_branch=row["rust_branch"],
                )
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


def _lookup_dep_pr_number(
    conn, rust_branch: str, ak_commit: Optional[str],
) -> Optional[int]:
    """Resolve a dep AK SHA to its pr_commit.pr_number on `rust_branch`,
    or None if the SHA is None or no row matches. Synthetic dry-run
    rows (negative pr_number) are returned as-is and treated as None
    by callers (the dep section won't render a #-link for them)."""
    if not ak_commit:
        return None
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
            ["r2", "sandbox", "claude", "-p", prompt],
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
                    ["r2", "sandbox", "claude", "-p", prompt],
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
                    ["r2", "sandbox", "claude", "-p", prompt],
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
    args: argparse.Namespace, conn, ak_branch: str, ak_commit: str,
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
    conn = db.connect(args.db_path)
    db.migrate(conn)
    # State-mutating modes push the DB back to Semaphore at the end (in a
    # try/finally so partial work is still persisted). --seed and
    # --delete-prs both mutate state; --pr (status check, no
    # --plan-approve) does not. --delete-prs in --dry-run mode mutates
    # nothing, hence the existing `not args.dry_run` guard already
    # excludes it.
    push_artifact = (
        not args.no_artifact_push and not args.dry_run
        and (args.seed or args.delete_prs or args.plan_approve or
             (args.pr is None and not args.delete_prs))  # sweep mode
    )
    rc = 1
    try:
        if args.seed:
            rc = _run_seed(args, conn)
        elif args.delete_prs:
            rc = _run_delete_prs(args, conn)
        elif args.pr is not None:
            rc = _run_pr_mode(args, conn)
        else:
            rc = _run_sweep(args, conn)
    finally:
        # Close the connection BEFORE pushing to flush WAL etc.
        conn.close()
        if push_artifact:
            try:
                semaphore.push_project_artifact(args.artifact_name, args.db_path)
            except FileNotFoundError as e:
                log.error("Artifact push skipped: %s", e)
            except Exception as e:
                log.error("Artifact push failed: %s", e)
    return rc


if __name__ == "__main__":
    sys.exit(main())
