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
import logging
import sys
from typing import Sequence

from . import db


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
    try:
        db.mark_plan_approved(conn, args.pr)
    except ValueError as e:
        log.error("%s", e)
        return 1
    log.info("PR %d marked plan_approved (status %d)", args.pr, db.STATUS_PLAN_APPROVED)
    log.info("Step-8 (implementation) cascade not yet wired -- Phase D pending")
    return 0


def _run_sweep(args: argparse.Namespace, conn) -> int:
    log.info("Sweep mode not yet implemented -- Phases B/C/D/E pending")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    )
    conn = db.connect(args.db_path)
    db.migrate(conn)
    try:
        if args.seed:
            return _run_seed(args, conn)
        if args.pr is not None:
            return _run_pr_mode(args, conn)
        return _run_sweep(args, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
