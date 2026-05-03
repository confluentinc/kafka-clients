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


def _is_shallow(repo_path: str) -> bool:
    """True iff `repo_path` is a shallow clone (has a `.git/shallow` file)."""
    try:
        out = _run_git(repo_path, ["rev-parse", "--is-shallow-repository"])
    except GitError:
        return False
    return out.strip() == "true"


def _commit_present(repo_path: str, commit: str) -> bool:
    """True iff `commit` exists in the local object DB.

    Uses `git cat-file -e <commit>^{{commit}}` -- the canonical
    object-existence probe. Side-effect-free; safe to call repeatedly.
    """
    proc = subprocess.run(
        ["git", "-C", repo_path, "cat-file", "-e", f"{commit}^{{commit}}"],
        capture_output=True,
    )
    return proc.returncode == 0


def _ensure_commit_reachable(
    repo_path: str,
    commit: str,
    branch: str,
    *,
    remote: str = "origin",
    deepen_step: int = 50,
    max_deepens: int = 200,
) -> None:
    """Deepen the shallow clone of `branch` until `commit` is in the
    local object DB.

    No-op if `commit` is already present, or if the repo isn't shallow
    (full clones already have everything).

    On a shallow clone, runs `git fetch --deepen=<deepen_step> <remote>
    <branch>` repeatedly until: (a) `commit` appears, (b) the repo is
    no longer shallow (we've fetched the full history), or (c) we've
    done `max_deepens` rounds. Cases (b) and (c) without finding the
    commit raise GitError -- the cursor is not on this branch.

    Required because Semaphore's `git submodule update --init --depth=1`
    leaves the AK submodule with only 1 commit, while the orchestrator's
    branch_commit cursor can be hundreds/thousands of commits older.
    """
    if _commit_present(repo_path, commit):
        return
    if not _is_shallow(repo_path):
        raise GitError(
            f"commit {commit} not found in {repo_path}; the repo is not "
            f"shallow, so this commit is not reachable from {remote}/{branch}"
        )
    for round_idx in range(max_deepens):
        try:
            _run_git(
                repo_path,
                ["fetch", f"--deepen={deepen_step}", remote, branch],
            )
        except GitError as e:
            raise GitError(
                f"failed to deepen shallow clone (round {round_idx + 1}): {e}"
            ) from e
        if _commit_present(repo_path, commit):
            return
        if not _is_shallow(repo_path):
            # The deepen exhausted the remote's history; if we still
            # don't have the commit, it's not on this branch.
            raise GitError(
                f"commit {commit} not found after fully unshallowing "
                f"{remote}/{branch}; commit is not on that branch"
            )
    raise GitError(
        f"commit {commit} not found after {max_deepens} deepen rounds "
        f"(~{max_deepens * deepen_step} commits) of {remote}/{branch}"
    )


def next_commits(
    repo_path: str, since: str, branch: str, n: int = 10,
    *,
    remote: str = "origin",
    deepen_step: int = 50,
    max_deepens: int = 200,
) -> List[str]:
    """Return up to n commit SHAs on `branch`, oldest-to-newest, after `since`.

    Equivalent to `git log --reverse <since>..<branch> --max-count=N --format=%H`.

    If `repo_path` is a shallow clone and `since` isn't in the local
    object DB (the common case under Semaphore's `--depth=1` submodule
    init), deepens the clone progressively until `since` becomes
    reachable. See `_ensure_commit_reachable`.
    """
    _ensure_commit_reachable(
        repo_path, since, branch,
        remote=remote, deepen_step=deepen_step, max_deepens=max_deepens,
    )
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


def push_branch(
    repo_path: str,
    branch_name: str,
    remote: str = "origin",
    set_upstream: bool = True,
) -> None:
    """Push the local `branch_name` to `remote`.

    Used by the orchestrator after a plan/impl R2 invocation returns,
    because the sandbox denies `git push` to keep the agent from
    surprising the operator with a remote write. The local commits
    (made by claude inside the worktree) are still on the local ref --
    this just publishes them.

    Idempotent fast-forward: if origin already has the branch at the
    same SHA, this is a no-op. If origin's tip diverges, raises GitError
    so the caller can decide.
    """
    args = ["push"]
    if set_upstream:
        args.append("-u")
    args.extend([remote, branch_name])
    _run_git(repo_path, args)
