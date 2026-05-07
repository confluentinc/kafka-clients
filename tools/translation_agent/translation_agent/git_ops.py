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
import time
from typing import List


class GitError(RuntimeError):
    pass


# Git's "shallow file has changed since we read it" race: another process
# (concurrent submodule update, parallel deepen on the same .git/modules/<sub>,
# or Semaphore housekeeping) mutated .git/shallow between our fetch's read
# and write phases. Transient -- a retry typically lands cleanly because
# the conflicting writer is finished by then.
_SHALLOW_RACE_FRAGMENT = "shallow file has changed"
_DEEPEN_RETRY_ATTEMPTS = 3
_DEEPEN_RETRY_BACKOFF_SECONDS = 0.1


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


def _commit_reachable_from(
    repo_path: str, commit: str, ref: str = "FETCH_HEAD",
) -> bool:
    """True iff `commit` is in the local object DB AND is an ancestor
    of `ref` in the local clone.

    Both conditions matter for shallow clones: Semaphore's
    `git submodule update --init --depth=1` pins the submodule at the
    submodule-pointer SHA (often the cursor itself), which can leave
    the cursor object present but DISCONNECTED from the branch tip's
    history -- the two end up as separate shallow roots until a
    deepen materializes the path between them. Just checking object
    presence would falsely declare success while the connecting
    commits are still missing.

    Side-effect-free.
    """
    if not _commit_present(repo_path, commit):
        return False
    proc = subprocess.run(
        ["git", "-C", repo_path, "merge-base", "--is-ancestor", commit, ref],
        capture_output=True,
    )
    return proc.returncode == 0


def _deepen_with_shallow_race_retry(
    repo_path: str,
    deepen_step: int,
    remote: str,
    branch: str,
) -> None:
    """Run `git fetch --deepen=<step> <remote> <branch>`, retrying the
    specific "shallow file has changed since we read it" race up to
    `_DEEPEN_RETRY_ATTEMPTS` times with a small backoff.

    Other GitError messages propagate immediately -- we don't want to
    mask network/auth failures or revision-not-found errors with a
    retry loop.
    """
    for attempt in range(_DEEPEN_RETRY_ATTEMPTS):
        try:
            _run_git(
                repo_path,
                ["fetch", f"--deepen={deepen_step}", remote, branch],
            )
            return
        except GitError as e:
            is_last = attempt == _DEEPEN_RETRY_ATTEMPTS - 1
            if _SHALLOW_RACE_FRAGMENT in str(e) and not is_last:
                time.sleep(_DEEPEN_RETRY_BACKOFF_SECONDS)
                continue
            raise


def _ensure_commit_reachable(
    repo_path: str,
    commit: str,
    branch: str,
    *,
    remote: str = "origin",
    deepen_step: int = 50,
    max_deepens: int = 200,
    ref: str = "FETCH_HEAD",
) -> None:
    """Deepen the shallow clone of `branch` until `commit` is reachable
    from `ref` (default FETCH_HEAD = the just-fetched tip of `branch`).

    The probe uses `_commit_reachable_from` -- object present AND
    ancestor of `ref` -- not just object presence. This matters
    for the Semaphore submodule init case: the parent repo's submodule
    pointer can land the cursor commit as a separate shallow root,
    disconnected from the branch tip. A presence-only probe would
    falsely succeed and the subsequent log range would be empty (or
    raise downstream).

    With the default `ref="FETCH_HEAD"`, caller must have run
    `git fetch <remote> <branch>` first so FETCH_HEAD is set to the
    branch's tip. The deepen fetches inside this function will refresh
    FETCH_HEAD on each round, but with a non-default `ref` (e.g. a
    fixed SHA used by `commits_between`) that doesn't matter -- the
    ancestry probe targets the caller-supplied ref, not FETCH_HEAD.

    On a shallow clone, runs `git fetch --deepen=<deepen_step>
    <remote> <branch>` repeatedly until: (a) cursor becomes reachable,
    (b) the repo is no longer shallow (full history fetched), or
    (c) `max_deepens` rounds exhausted. Cases (b) and (c) without
    success raise GitError -- the cursor is not on this branch.
    """
    if _commit_reachable_from(repo_path, commit, ref=ref):
        return
    if not _is_shallow(repo_path):
        raise GitError(
            f"commit {commit} is not reachable from {remote}/{branch} "
            f"in {repo_path}; the repo is not shallow, so deepening "
            f"cannot help. The cursor may be on a different branch or "
            f"unrelated history. Re-seed branch_commit (--force) "
            f"pointing at a commit on {remote}/{branch}."
        )
    for round_idx in range(max_deepens):
        try:
            _deepen_with_shallow_race_retry(
                repo_path, deepen_step, remote, branch,
            )
        except GitError as e:
            raise GitError(
                f"failed to deepen shallow clone (round {round_idx + 1}): {e}"
            ) from e
        if _commit_reachable_from(repo_path, commit, ref=ref):
            return
        if not _is_shallow(repo_path):
            # The deepen exhausted the remote's history; if cursor
            # still isn't reachable, it's genuinely not on this branch.
            raise GitError(
                f"commit {commit} not reachable from {remote}/{branch} "
                f"after fully unshallowing; commit is not on that branch"
            )
    raise GitError(
        f"commit {commit} not reachable after {max_deepens} deepen rounds "
        f"(~{max_deepens * deepen_step} commits) of {remote}/{branch}"
    )


def next_commits(
    repo_path: str, since: str, branch: str, n: int = 10,
    *,
    remote: str = "origin",
    deepen_step: int = 50,
    max_deepens: int = 200,
) -> List[str]:
    """Return up to `n` commit SHAs on `branch`, oldest-to-newest, after
    `since`.

    Three subtleties this implementation gets right:

    1. **Fetch the latest tip first.** Semaphore's
       `git submodule update --init --depth=1` pins the local
       `origin/<branch>` at the submodule pointer SHA, NOT at
       `<branch>`'s actual current head on the remote. Without an
       explicit fetch, the range's upper bound stays months stale.
       We always run `git fetch <remote> <branch>` first so
       `FETCH_HEAD` reflects the remote's real tip, then use
       `FETCH_HEAD` (not bare `<branch>`) as the upper bound.

    2. **Verify the cursor is an ancestor of the tip.** If `since`
       isn't reachable from `FETCH_HEAD`, `git log <since>..<tip>`
       silently degrades to `git log <tip>` -- returning unrelated
       commits. We use `git merge-base --is-ancestor` to fail loudly
       instead.

    3. **Take the N oldest in range, not the N newest.** Git's
       `--max-count` is applied during the default newest-first walk,
       so `git log --reverse <range> --max-count=N` returns the N
       NEWEST commits in the range, displayed oldest-first. For
       sweep ordering we want the N OLDEST so the cursor advances
       commit-by-commit. We walk the full range and limit in Python.

    Also handles the shallow-clone case via `_ensure_commit_reachable`
    -- if `since` isn't in the local object DB after the initial
    fetch, deepens progressively until it appears.
    """
    # Step 1: refresh the local view of <branch>'s tip on the remote.
    # FETCH_HEAD is updated as a side effect.
    try:
        _run_git(repo_path, ["fetch", remote, branch])
    except GitError as e:
        raise GitError(
            f"failed to fetch {remote}/{branch} latest tip: {e}"
        ) from e

    # Step 2: ensure the cursor commit is reachable from FETCH_HEAD --
    # both present in the local DB AND in FETCH_HEAD's ancestry. The
    # initial fetch above may already satisfy this; otherwise deepen
    # until it does. The probe (see _commit_reachable_from) handles
    # the Semaphore submodule-init case where the cursor object lands
    # as a separate shallow root, disconnected from the branch tip
    # until enough deepen rounds materialize the path between them.
    _ensure_commit_reachable(
        repo_path, since, branch,
        remote=remote, deepen_step=deepen_step, max_deepens=max_deepens,
    )

    # Step 3: walk the full range oldest-first and limit in Python.
    out = _run_git(
        repo_path,
        [
            "log", "--reverse",
            f"{since}..FETCH_HEAD",
            "--format=%H",
        ],
    )
    commits = [line.strip() for line in out.splitlines() if line.strip()]
    return commits[:n]


def commits_between(
    repo_path: str, since: str, until: str, branch: str,
    *,
    remote: str = "origin",
    deepen_step: int = 50,
    max_deepens: int = 200,
) -> List[str]:
    """Return commit SHAs in `(since, until]` on `branch`, oldest-to-newest.

    Differs from `next_commits` in two ways:

    1. **Bounded by an explicit `until` SHA** rather than FETCH_HEAD /
       branch tip. Used by per-PR dep-eval to enumerate the candidate
       set for one PR: every AK commit between the cursor (`since`)
       and the PR's own `ak_commit` (`until`).
    2. **Returns the full range**, no `n=` cap -- the bounded-range
       query is naturally bounded by the AK commit graph.

    Steps:
    - Fetch `<remote>/<branch>` so we have the latest tip locally and
      can deepen its history if needed.
    - If `until` isn't already in the local object DB, fetch it by
      bare SHA. This works on GitHub by default (the server has
      `uploadpack.allowAnySHA1InWant=true`) and is the only reliable
      way to materialize a specific historical commit that may have
      fallen out of the shallow window.
    - Reuse `_ensure_commit_reachable` (with `ref=until`) to deepen
      until `since` is an ancestor of `until` -- same shallow-clone
      / Semaphore-submodule-init handling as `next_commits`.
    - Walk `git log <since>..<until> --reverse --format=%H` and
      return the SHAs.

    Returns `[]` when `since == until` (empty range, exclusive lower
    bound). Raises `GitError` if the bare-SHA fetch fails (private
    repo / wrong remote) or if `since` is not an ancestor of `until`
    in a fully-fetched clone.
    """
    try:
        _run_git(repo_path, ["fetch", remote, branch])
    except GitError as e:
        raise GitError(
            f"failed to fetch {remote}/{branch} "
            f"for commits_between: {e}"
        ) from e

    if not _commit_present(repo_path, until):
        try:
            _run_git(repo_path, ["fetch", remote, until])
        except GitError as e:
            raise GitError(
                f"failed to fetch until commit {until} from {remote} "
                f"(server may not allow bare-SHA fetch): {e}"
            ) from e

    _ensure_commit_reachable(
        repo_path, since, branch,
        remote=remote, deepen_step=deepen_step, max_deepens=max_deepens,
        ref=until,
    )

    out = _run_git(
        repo_path,
        [
            "log", "--reverse",
            f"{since}..{until}",
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
    force: bool = False,
) -> None:
    """Push the local `branch_name` to `remote`.

    Used by the orchestrator after a plan/impl R2 invocation returns,
    because the sandbox denies `git push` to keep the agent from
    surprising the operator with a remote write. The local commits
    (made by claude inside the worktree) are still on the local ref --
    this just publishes them.

    With `force=False` (default), behaves as a fast-forward push:
    idempotent if origin is at the same SHA, raises GitError if
    origin's tip diverges. With `force=True`, passes `--force` to
    unconditionally overwrite origin's tip with the local one --
    appropriate only for orchestrator-owned branches (e.g.
    `kafka-translate/<sha>`) where the orchestrator is the sole
    writer; do NOT use for shared branches.
    """
    args = ["push"]
    if set_upstream:
        args.append("-u")
    if force:
        args.append("--force")
    args.extend([remote, branch_name])
    _run_git(repo_path, args)
