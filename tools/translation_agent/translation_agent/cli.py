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

- Sweep mode (default): `translation-agent --ak-repo-path ... --ak-branch ...
  --rust-branch ...` — runs design steps 1..6+8.
- Per-PR mode: `translation-agent --pr <N> [--plan-approve]` — design step 7.
- Seed mode: `translation-agent --seed --ak-branch ... --ak-commit ...
  --rust-branch ... --rust-commit ...` — bootstraps the `branch_commit`
  table on first use.
"""

import argparse
import hashlib
import logging
import shutil
import sys
from typing import Sequence

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
        "--artifact-name", default="translation_agent_db",
        help="Semaphore project-artifact name for the sqlite DB "
             "(default: %(default)s).",
    )

    # Mode flags. Mutually exclusive so we can keep the spec wording literal:
    #   default       sweep mode
    #   --pr N        per-PR mode (status check or --plan-approve)
    #   --seed        seed mode
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--pr", type=int, metavar="N", help="Operate on a single PR.")
    mode.add_argument("--seed", action="store_true",
                      help="Insert a row into branch_commit (idempotent).")

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
    parser.add_argument("--ak-branch", help="AK branch to follow.")
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

    # Seed-mode args (--ak-branch and --rust-branch are shared with sweep mode).
    parser.add_argument("--ak-commit", help="AK commit hash (for --seed).")
    parser.add_argument("--rust-commit", help="Rust commit hash (for --seed).")

    return parser


def _run_seed(args: argparse.Namespace, conn) -> int:
    required = ("ak_branch", "ak_commit", "rust_branch", "rust_commit")
    missing = [f"--{r.replace('_', '-')}" for r in required if not getattr(args, r)]
    if missing:
        log.error("--seed requires: %s", ", ".join(missing))
        return 2
    inserted = db.seed_correspondence(
        conn, args.ak_branch, args.ak_commit, args.rust_branch, args.rust_commit,
    )
    if inserted:
        log.info(
            "Inserted branch_commit (ak=%s/%s, rust=%s/%s)",
            args.ak_branch, args.ak_commit, args.rust_branch, args.rust_commit,
        )
    else:
        log.info(
            "branch_commit already exists for (%s, %s, %s) -- left unchanged",
            args.ak_branch, args.ak_commit, args.rust_branch,
        )
    return 0


def _run_pr_mode(args: argparse.Namespace, conn) -> int:
    pr = db.get_pr(conn, args.pr)
    if pr is None:
        log.error("No pr_commit row for PR %d", args.pr)
        return 1
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
            "preserved, DB status unchanged)"
        )

    err, sha = _run_impl_one(args, pr)
    if err:
        log.error("PR #%d implementation failed: %s", args.pr, err)
        if not args.dry_run:
            db.set_last_error(conn, args.pr, err)
        return 1
    if args.dry_run:
        log.info("[dry-run] PR #%d impl completed -- DB status unchanged", args.pr)
        return 0
    db.mark_implementation_done(
        conn, args.pr,
        ak_branch=pr["ak_branch"],
        ak_commit=pr["ak_commit"],
        rust_branch=pr["rust_branch"],
        rust_commit=sha,
    )
    log.info(
        "PR #%d -> status %d (implementation_done) rust_commit=%s",
        args.pr, db.STATUS_IMPLEMENTATION_DONE, sha[:12],
    )
    return 0


def _run_sweep(args: argparse.Namespace, conn) -> int:
    required = ("ak_repo_path", "ak_branch", "rust_branch")
    missing = [f"--{r.replace('_', '-')}" for r in required if not getattr(args, r)]
    if missing:
        log.error("sweep mode requires: %s", ", ".join(missing))
        return 2

    # Step 2: find the AK cursor for this Rust branch.
    cursor = db.get_latest_correspondence(conn, args.rust_branch)
    if cursor is None:
        log.error(
            "No branch_commit row for rust_branch=%s. Use --seed to bootstrap.",
            args.rust_branch,
        )
        return 1
    log.info(
        "Cursor: rust=%s/%s -> AK=%s/%s",
        cursor["rust_branch"], cursor["rust_commit"],
        cursor["ak_branch"], cursor["ak_commit"],
    )
    if cursor["ak_branch"] != args.ak_branch:
        log.warning(
            "Cursor's ak_branch=%s differs from --ak-branch=%s; using --ak-branch.",
            cursor["ak_branch"], args.ak_branch,
        )

    # Step 3: get the next 10 AK commits.
    try:
        ak_commits = git_ops.next_commits(
            args.ak_repo_path, since=cursor["ak_commit"], branch=args.ak_branch, n=10,
        )
    except git_ops.GitError as e:
        log.error("Failed to read AK commits: %s", e)
        return 1
    if not ak_commits:
        log.info("No new AK commits to translate; will still process existing rows.")
    else:
        log.info("Found %d new AK commit(s) on %s", len(ak_commits), args.ak_branch)

    # Step 3 (cont): create branches + draft PRs, insert into pr_commit.
    new_pr_count = 0
    for ak_commit in ak_commits:
        rc = _create_pr_for_ak_commit(args, conn, ak_commit)
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
        log.info("No unblocked plan or implementation work this sweep.")
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
            "worktree preserved, DB status unchanged)"
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
            if args.dry_run:
                log.info(
                    "[dry-run] PR #%d (%s) completed -- DB status unchanged",
                    pr_number, kind,
                )
                continue
            if kind == "plan":
                db.mark_plan_created(conn, pr_number)
                log.info(
                    "PR #%d -> status %d (plan_created)",
                    pr_number, db.STATUS_PLAN_CREATED,
                )
            else:
                # impl: row["ak_branch"] should be populated by the sweep.
                ak_branch = row["ak_branch"] or args.ak_branch
                db.mark_implementation_done(
                    conn, pr_number,
                    ak_branch=ak_branch,
                    ak_commit=row["ak_commit"],
                    rust_branch=row["rust_branch"],
                    rust_commit=sha,
                )
                log.info(
                    "PR #%d -> status %d (implementation_done) rust_commit=%s",
                    pr_number, db.STATUS_IMPLEMENTATION_DONE,
                    sha[:12] if sha else "??",
                )


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
        ak_branch=row["ak_branch"] or args.ak_branch or "(unknown)",
        pr_number=pr_number,
        branch_name=branch_name,
    )
    if args.dry_run:
        prompt = prompt + "\n" + prompts.DRY_RUN_NOTE
    try:
        with worktree.worktree_for_branch(
            args.rust_repo_path, branch_name, cleanup=not args.dry_run,
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
    except worktree.WorktreeError as e:
        return f"worktree setup failed: {e}", None
    return None, None


def _run_impl_one(args, row):
    """Returns (err, new_rust_commit_sha).

    In dry-run mode: prompt augmented with "do not push", worktree
    preserved on disk, no post-r2 fetch/rev-parse (nothing was pushed),
    sha returned as None.
    """
    pr_number = row["pr_number"]
    ak_commit = row["ak_commit"]
    branch_name = github.branch_name_for_ak(ak_commit)
    prompt = prompts.IMPLEMENTATION_PROMPT_TEMPLATE.format(
        ak_commit=ak_commit,
        ak_branch=row["ak_branch"] or args.ak_branch or "(unknown)",
        pr_number=pr_number,
        branch_name=branch_name,
    )
    if args.dry_run:
        prompt = prompt + "\n" + prompts.DRY_RUN_NOTE
    try:
        with worktree.worktree_for_branch(
            args.rust_repo_path, branch_name, cleanup=not args.dry_run,
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
            if args.dry_run:
                log.info(
                    "[dry-run] PR #%d impl worktree preserved at %s",
                    pr_number, wt,
                )
                return None, None
            # Capture the new rust commit so we can update branch_commit.
            # Done inside the `with` so a failed rev-parse still triggers
            # worktree cleanup.
            try:
                git_ops.fetch(args.rust_repo_path, branch_name)
                sha = git_ops.rev_parse(args.rust_repo_path, f"origin/{branch_name}")
            except git_ops.GitError as e:
                return f"failed to read new rust commit on {branch_name}: {e}", None
    except worktree.WorktreeError as e:
        return f"worktree setup failed: {e}", None
    return None, sha


def _create_pr_for_ak_commit(
    args: argparse.Namespace, conn, ak_commit: str,
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
            "[dry-run] would push %s to branch %s and create draft PR %r "
            "(synthetic pr_number=%d)",
            f"origin/{args.rust_branch}", branch_name, title, pr_number,
        )
    else:
        try:
            git_ops.push_new_branch(
                args.rust_repo_path,
                source_ref=f"origin/{args.rust_branch}",
                target_branch=branch_name,
            )
        except git_ops.GitError as e:
            log.error("Failed to push branch %s: %s", branch_name, e)
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
        conn, pr_number, args.rust_branch, args.ak_branch, ak_commit,
    )
    label = "[dry-run] " if args.dry_run else ""
    if inserted:
        log.info("%sCreated PR #%d for AK commit %s", label, pr_number, ak_commit[:12])
    else:
        log.info("%sPR #%d already in pr_commit -- left unchanged", label, pr_number)
    return inserted


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
    # try/finally so partial work is still persisted). --seed mutates state
    # too; --pr (status check, no --plan-approve) does not.
    push_artifact = (
        not args.no_artifact_push and not args.dry_run
        and (args.seed or args.plan_approve or
             (args.pr is None))  # sweep mode
    )
    rc = 1
    try:
        if args.seed:
            rc = _run_seed(args, conn)
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
