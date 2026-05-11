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


class GhBranchAlreadyGone(GhError):
    """Raised when `gh api -X DELETE` of a ref returns a 'Reference does
    not exist' error (HTTP 422 -- GitHub's quirky status for missing
    refs, not 404). Distinct from GhError so callers that only care
    about *removing* the branch can treat it as a soft success."""


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


def delete_remote_branch(repo_path: str, branch_name: str) -> None:
    """Delete `branch_name` from the GitHub remote via
    `gh api -X DELETE /repos/{owner}/{repo}/git/refs/heads/<branch>`.

    `gh api` resolves `{owner}/{repo}` from the git remote of the cwd,
    matching how every other helper in this module locates the repo.

    Side effect on GitHub: any open PR whose head was this branch
    auto-closes (PRs cannot be deleted on GitHub, only closed).

    Raises GhBranchAlreadyGone if gh stderr indicates the ref does not
    exist (HTTP 422 "Reference does not exist" -- GitHub's idiosyncratic
    status for missing refs). Callers can catch this specifically to
    treat it as a soft success.

    Raises GhError for any other non-zero exit (auth, network, real 5xx).
    """
    proc = subprocess.run(
        [
            "gh", "api", "-X", "DELETE",
            f"repos/{{owner}}/{{repo}}/git/refs/heads/{branch_name}",
        ],
        capture_output=True,
        text=True,
        cwd=repo_path,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        if "Reference does not exist" in stderr:
            raise GhBranchAlreadyGone(
                f"branch {branch_name} already gone on remote: {stderr}"
            )
        raise GhError(
            f"gh api delete branch {branch_name} failed "
            f"(rc={proc.returncode}): {stderr}"
        )


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


def add_pr_comment(repo_path: str, pr_number: int, body_file: str) -> None:
    """Post a new comment on PR `pr_number` whose body is the contents
    of `body_file`. Wraps `gh pr comment <N> --body-file <path>`.

    The body is fed via `--body-file` (not `--body`) so long markdown
    bodies can't hit OS argv length limits. Each call posts a NEW
    comment -- there is no read-modify-write semantics here, unlike
    `update_pr_body` / `prepend_pr_body`.

    Used by `--ask` to publish the agent's answer (already prefixed
    with the user's question quoted as a markdown blockquote inside
    the file). Sandbox is denied this `gh` subcommand by absence
    from the allow-list -- only the orchestrator publishes to GitHub.
    """
    proc = subprocess.run(
        ["gh", "pr", "comment", str(pr_number), "--body-file", body_file],
        capture_output=True,
        text=True,
        cwd=repo_path,
    )
    if proc.returncode != 0:
        raise GhError(
            f"gh pr comment failed (rc={proc.returncode}): "
            f"{proc.stderr.strip()}"
        )


def add_pr_label(repo_path: str, pr_number: int, label: str) -> None:
    """Apply `label` to PR `pr_number` via `gh pr edit <N> --add-label`.

    Idempotent on GitHub's side -- adding a label that's already on
    the PR is a no-op (gh returns success). Raises GhError on
    non-zero exit (e.g. label doesn't exist on the repo, network
    failure); the caller decides whether to log+continue or fail.

    The orchestrator uses this for state markers like
    `implementation-needed` after a plan-phase PR description update.
    """
    proc = subprocess.run(
        ["gh", "pr", "edit", str(pr_number), "--add-label", label],
        capture_output=True,
        text=True,
        cwd=repo_path,
    )
    if proc.returncode != 0:
        raise GhError(
            f"gh pr edit --add-label {label} failed "
            f"(rc={proc.returncode}): {proc.stderr.strip()}"
        )


def remove_pr_label(repo_path: str, pr_number: int, label: str) -> None:
    """Remove `label` from PR `pr_number` via
    `gh pr edit <N> --remove-label`.

    Idempotent: removing a label the PR doesn't have returns success.
    Raises GhError on real failures (auth, network, etc.).
    """
    proc = subprocess.run(
        ["gh", "pr", "edit", str(pr_number), "--remove-label", label],
        capture_output=True,
        text=True,
        cwd=repo_path,
    )
    if proc.returncode != 0:
        raise GhError(
            f"gh pr edit --remove-label {label} failed "
            f"(rc={proc.returncode}): {proc.stderr.strip()}"
        )


# Markers delimiting the orchestrator-managed dependency section in a
# PR body. HTML-comment form so they render invisibly on github.com but
# are still grep-able for replace_dep_section's read-modify-write.
DEP_SECTION_START = "<!-- deps:start -->"
DEP_SECTION_END = "<!-- deps:end -->"

_DEP_SECTION_RE = re.compile(
    re.escape(DEP_SECTION_START) + r".*?" + re.escape(DEP_SECTION_END) + r"\n*",
    re.DOTALL,
)


def format_dep_section(
    plan_dep_pr_number: Optional[int] = None,
    impl_dep_pr_number: Optional[int] = None,
) -> str:
    """Build the markdown dep-section body (without the start/end
    markers). Returns "" when both deps are None so callers can short-
    circuit and just strip any existing block.

    PR numbers are rendered as bare `#N` references; GitHub auto-links
    these to the corresponding PR within the same repo.
    """
    if plan_dep_pr_number is None and impl_dep_pr_number is None:
        return ""
    lines = ["**Dependencies:**"]
    if plan_dep_pr_number is not None:
        lines.append(f"- Plan: #{plan_dep_pr_number}")
    if impl_dep_pr_number is not None:
        lines.append(f"- Implementation: #{impl_dep_pr_number}")
    return "\n".join(lines)


def replace_dep_section(body: str, dep_section: str) -> str:
    """Idempotently set the dep section in `body`.

    Strips any existing `DEP_SECTION_START..DEP_SECTION_END` block, then
    prepends the new block (markers + section). If `dep_section` is the
    empty string, just strips. Safe to call repeatedly: the block is
    replaced in place rather than accumulated.
    """
    stripped = _DEP_SECTION_RE.sub("", body)
    if not dep_section:
        return stripped
    block = f"{DEP_SECTION_START}\n{dep_section}\n{DEP_SECTION_END}"
    if stripped:
        return f"{block}\n\n{stripped}"
    return block


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
