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
