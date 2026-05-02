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

"""Git wrappers used by the orchestrator.

Thin layer over `git -C <repo_path> ...` subprocess calls. Used to read
commits from the AK repo (sweep step 2/3) and to push new branches to the
Rust repo (sweep step 3). Each wrapper raises GitError on a non-zero git
exit so the caller can surface a single exception type.
"""

import subprocess
from typing import List


class GitError(RuntimeError):
    pass


def _run_git(repo_path: str, args: list) -> str:
    proc = subprocess.run(
        ["git", "-C", repo_path, *args],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise GitError(
            f"git {' '.join(args)} (in {repo_path}) failed (rc={proc.returncode}): "
            f"{proc.stderr.strip()}"
        )
    return proc.stdout


def fetch(repo_path: str, *refspecs: str, remote: str = "origin") -> None:
    """`git fetch <remote> [<refspec>...]`."""
    _run_git(repo_path, ["fetch", remote, *refspecs])


def rev_parse(repo_path: str, rev: str) -> str:
    """`git rev-parse <rev>` -- returns the resolved commit SHA."""
    return _run_git(repo_path, ["rev-parse", rev]).strip()


def next_commits(
    repo_path: str, since: str, branch: str, n: int = 10,
) -> List[str]:
    """Return up to n commit SHAs on `branch`, oldest-to-newest, after `since`.

    Equivalent to `git log --reverse <since>..<branch> --max-count=N --format=%H`.
    """
    out = _run_git(
        repo_path,
        [
            "log", "--reverse",
            f"{since}..{branch}",
            "--max-count", str(n),
            "--format=%H",
        ],
    )
    return [line.strip() for line in out.splitlines() if line.strip()]


def commit_subject(repo_path: str, commit: str) -> str:
    """First line of the commit message."""
    return _run_git(repo_path, ["log", "-1", "--format=%s", commit]).strip()


def push_new_branch(
    repo_path: str,
    source_ref: str,
    target_branch: str,
    remote: str = "origin",
) -> None:
    """Push source_ref to a new branch on remote without mutating local state.

    Equivalent to `git push <remote> <source_ref>:refs/heads/<target_branch>`.
    Idempotent: pushing the same SHA again is a no-op.
    """
    _run_git(
        repo_path,
        ["push", remote, f"{source_ref}:refs/heads/{target_branch}"],
    )
