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

"""`gh` CLI wrappers for PR creation.

Naming is deterministic so re-running the sweep is naturally idempotent:
* branch:  `kafka-translate/<full-ak-sha>` -- pushing the same SHA twice
  is a no-op at the git level.
* PR:      `gh pr create` will refuse to create a duplicate PR for the
  same head branch, returning a non-zero exit. Callers treat that as
  "already done, skip" rather than a real failure.
"""

import re
import subprocess
from typing import Optional


class GhError(RuntimeError):
    pass


class GhPrAlreadyExists(GhError):
    """Raised when `gh pr create` fails because a PR already exists for the head branch."""


BRANCH_PREFIX = "kafka-translate/"


def branch_name_for_ak(ak_commit: str) -> str:
    """Deterministic Rust-branch name for an AK commit. Stable across re-runs."""
    return f"{BRANCH_PREFIX}{ak_commit}"


def pr_title_for_ak(ak_commit: str, subject: str) -> str:
    return f"Translate kafka commit {ak_commit[:12]}: {subject}"


def pr_body_for_ak(ak_commit: str, subject: str, body: str = "") -> str:
    parts = [
        f"Translates [Apache Kafka commit `{ak_commit}`]"
        f"(https://github.com/apache/kafka/commit/{ak_commit}).",
        "",
        f"**{subject}**",
    ]
    if body.strip():
        parts.append("")
        parts.append(body.strip())
    return "\n".join(parts)


_PR_URL_RE = re.compile(r"/pull/(\d+)\b")
_ALREADY_EXISTS_RE = re.compile(r"already exists", re.IGNORECASE)


def create_draft_pr(
    repo_path: str,
    base_branch: str,
    head_branch: str,
    title: str,
    body: str,
) -> int:
    """Create a draft PR via `gh pr create --draft`. Returns the PR number.

    Raises GhPrAlreadyExists if `gh` reports a PR already exists for the
    head branch (treated as "already done, skip" by the sweep).
    Raises GhError on other non-zero exits or unparseable output.
    """
    proc = subprocess.run(
        [
            "gh", "pr", "create",
            "--draft",
            "--base", base_branch,
            "--head", head_branch,
            "--title", title,
            "--body", body,
        ],
        capture_output=True,
        text=True,
        cwd=repo_path,
    )
    if proc.returncode != 0:
        if _ALREADY_EXISTS_RE.search(proc.stderr):
            raise GhPrAlreadyExists(proc.stderr.strip())
        raise GhError(
            f"gh pr create failed (rc={proc.returncode}): {proc.stderr.strip()}"
        )
    pr_number = _parse_pr_number(proc.stdout)
    if pr_number is None:
        raise GhError(
            f"could not parse PR number from gh output: {proc.stdout!r}"
        )
    return pr_number


def get_pr_state(
    repo_path: str, pr_number: int,
) -> "tuple[str, Optional[str]]":
    """Return (state, merge_commit_sha) for `pr_number`.

    `state` is one of "OPEN", "CLOSED", "MERGED" (gh's exact strings).
    `merge_commit_sha` is the SHA of the resulting commit on the base
    branch when state is MERGED, otherwise None.

    Wraps `gh pr view <N> --json state,mergeCommit`. Raises GhError on
    non-zero exit; the caller decides whether to abort or skip.
    """
    proc = subprocess.run(
        [
            "gh", "pr", "view", str(pr_number),
            "--json", "state,mergeCommit",
        ],
        capture_output=True,
        text=True,
        cwd=repo_path,
    )
    if proc.returncode != 0:
        raise GhError(
            f"gh pr view failed (rc={proc.returncode}): {proc.stderr.strip()}"
        )
    import json
    data = json.loads(proc.stdout)
    state = data.get("state", "")
    merge_commit = data.get("mergeCommit") or {}
    merge_sha = merge_commit.get("oid") if isinstance(merge_commit, dict) else None
    return state, merge_sha


def find_pr_number_for_branch(repo_path: str, head_branch: str) -> Optional[int]:
    """Return the PR number for `head_branch`, or None if none exists.

    Used by the sweep to recover the PR number when `gh pr create` failed
    with `GhPrAlreadyExists` (re-run idempotency).
    """
    proc = subprocess.run(
        [
            "gh", "pr", "list",
            "--head", head_branch,
            "--state", "all",
            "--json", "number",
            "--limit", "1",
        ],
        capture_output=True,
        text=True,
        cwd=repo_path,
    )
    if proc.returncode != 0:
        raise GhError(
            f"gh pr list failed (rc={proc.returncode}): {proc.stderr.strip()}"
        )
    import json
    rows = json.loads(proc.stdout)
    if not rows:
        return None
    return int(rows[0]["number"])


def _parse_pr_number(gh_output: str) -> Optional[int]:
    m = _PR_URL_RE.search(gh_output)
    return int(m.group(1)) if m else None


def update_pr_body(repo_path: str, pr_number: int, body: str) -> None:
    """Replace PR `pr_number`'s body via `gh pr edit <N> --body-file -`.

    The body is fed through stdin (not argv) so long markdown bodies
    can't hit OS argv length limits. Raises GhError on non-zero exit.

    The orchestrator calls this after each state transition (post plan,
    post impl) to keep the PR description in sync with what's been
    done so far. Sandbox is denied `gh pr edit` -- only the
    orchestrator publishes to GitHub.
    """
    proc = subprocess.run(
        ["gh", "pr", "edit", str(pr_number), "--body-file", "-"],
        input=body,
        capture_output=True,
        text=True,
        cwd=repo_path,
    )
    if proc.returncode != 0:
        raise GhError(
            f"gh pr edit failed (rc={proc.returncode}): {proc.stderr.strip()}"
        )


def get_pr_body(repo_path: str, pr_number: int) -> str:
    """Return PR `pr_number`'s current body via `gh pr view --json body`.

    Used by `prepend_pr_body` to read-modify-write. Raises GhError on
    non-zero exit or unparseable JSON.
    """
    proc = subprocess.run(
        [
            "gh", "pr", "view", str(pr_number),
            "--json", "body", "--jq", ".body",
        ],
        capture_output=True,
        text=True,
        cwd=repo_path,
    )
    if proc.returncode != 0:
        raise GhError(
            f"gh pr view failed (rc={proc.returncode}): {proc.stderr.strip()}"
        )
    # `--jq '.body'` produces the raw string with a trailing newline;
    # strip just that to avoid every prepend doubling blank lines.
    return proc.stdout.rstrip("\n")


def prepend_pr_body(repo_path: str, pr_number: int, line: str) -> None:
    """Read the current body, prepend `line` + a blank separator, write
    it back. Convenience for state-transition markers (e.g. the plan-
    approved tick) where we don't want an LLM in the loop just to add
    one line.

    Idempotency: not enforced -- repeated calls will prepend repeated
    lines. Callers should only invoke this on the actual state flip,
    not on every retry of an already-flipped state.
    """
    current = get_pr_body(repo_path, pr_number)
    new_body = f"{line}\n\n{current}" if current else line
    update_pr_body(repo_path, pr_number, new_body)
