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

"""Per-PR git worktree manager.

Plan generation (sweep step 6) and implementation (sweep step 8) need an
isolated working tree per parallel PR so concurrent Claude invocations
can commit and push without clobbering each other. Each task wraps its
`r2 sandbox claude` call in `worktree_for_branch(...)`, which spins up a
detached worktree on a fresh temp dir, yields the path as the cwd for
the subprocess, and tears it down on exit -- regardless of success or
failure of the inner work.

Dep-eval (sweep step 4) does NOT use this module: it's read-only and
emits JSON to stdout, so isolation buys nothing.
"""

import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional


class WorktreeError(RuntimeError):
    pass


def _git(repo_path: str, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", repo_path, *args],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise WorktreeError(
            f"git {' '.join(args)} (in {repo_path}) failed "
            f"(rc={proc.returncode}): {proc.stderr.strip()}"
        )
    return proc.stdout


@contextmanager
def worktree_for_branch(
    repo_path: str,
    branch_name: str,
    *,
    cleanup: bool = True,
    base_remote_branch: Optional[str] = None,
    ak_commit: Optional[str] = None,
    ak_branch: str = "trunk",
    build: bool = True,
) -> Iterator[Path]:
    """Create a temporary git worktree checked out at `branch_name`.

    Yields the worktree path.

    By default the local `branch_name` ref is reset to `origin/<branch_name>`
    (which must already exist -- the real-run flow where Phase B has
    already pushed the branch).

    Pass `base_remote_branch="<other-branch>"` to base the worktree on
    `origin/<other-branch>` instead. The dry-run flow uses this because
    Phase B skips the push in dry-run, so `origin/<branch_name>` doesn't
    exist yet -- we fall back to the rust-branch tip (the same starting
    point Phase B's push *would have* used).

    Pass `ak_commit=<sha>` to bootstrap the worktree for plan/impl agent
    work: the orchestrator runs `make` (= `make build`, the Makefile's
    default target -- which walks `submodules -> build-rust -> build-c
    -> build-python` in dependency order, so the kafka submodule is
    initialized as a prerequisite of the C build), checks out
    `ak_commit` in the `kafka/` submodule, and commits the submodule
    pointer bump as its own commit BEFORE the agent runs. The agent's
    subsequent commits (e.g. "Design document") sit on top of the bump
    commit.

    Pass `build=False` together with `ak_commit` to skip the slow `make`
    build and only initialize the `kafka/` submodule (cheap) before the
    bump. This is the PR-creation path (sweep step 3): we just need a
    branch with the bump commit so `gh pr create` has something to PR --
    no Cargo/cmake/Python build required at that point.

    Commits made inside the worktree advance the local `branch_name` ref;
    a `git push` from inside also advances `origin/<branch_name>`. With
    `cleanup=True` (default), the worktree is removed on context exit
    regardless of success/failure; the local branch ref is left in place
    so the next sweep can read it. With `cleanup=False`, both the
    worktree directory and the local ref are preserved -- useful for
    dry-run inspection.
    """
    safe_name = branch_name.replace("/", "_")
    worktree_dir = Path(tempfile.mkdtemp(prefix=f"translation-agent-{safe_name}-"))
    created = False
    try:
        # If a previous (preserved-from-dry-run) worktree is still using
        # this branch, evict it -- otherwise `git worktree add` errors
        # with "branch is already used by worktree at <path>".
        prior = _find_worktree_for_branch(repo_path, branch_name)
        if prior is not None:
            try:
                _git(repo_path, "worktree", "remove", "--force", prior)
            except WorktreeError:
                pass  # best-effort; if remove fails the next add will too
        if _remote_branch_exists(repo_path, branch_name):
            # The PR branch already exists on origin (e.g. a previous
            # sweep created it but didn't complete, or this is a re-run
            # after the operator restarted CI). Base the worktree on
            # origin/<branch_name> so:
            #   - we have the existing bump commit and any subsequent
            #     plan/impl commits the remote already carries,
            #   - `_bump_kafka_submodule`'s diff-quiet guard makes a
            #     no-op when the kafka pointer is already correct, and
            #   - the eventual `git push` is a clean fast-forward (or
            #     a no-op) instead of being rejected as non-FF.
            # `-B` force-resets the local branch in case a stale ref
            # exists from a previous run.
            _git(repo_path, "fetch", "origin", branch_name)
            _git(
                repo_path, "worktree", "add",
                "-B", branch_name,
                str(worktree_dir),
                f"origin/{branch_name}",
            )
        elif _local_branch_exists(repo_path, branch_name):
            # No remote branch but local exists -- the dry-run cascade
            # case (per-PR --plan-approve, sweep impl after a prior plan
            # run) where the local branch carries plan/impl commits
            # that origin doesn't have because dry-run never pushed.
            _git(
                repo_path, "worktree", "add",
                str(worktree_dir),
                branch_name,
            )
        else:
            # First-time setup: fetch the base from origin and create
            # the local branch reset to it. `-B` makes the create-or-
            # reset idempotent if a stale ref happens to exist.
            base = (
                base_remote_branch
                if base_remote_branch is not None
                else branch_name
            )
            _git(repo_path, "fetch", "origin", base)
            _git(
                repo_path, "worktree", "add",
                "-B", branch_name,
                str(worktree_dir),
                f"origin/{base}",
            )
        created = True
        if ak_commit is not None:
            if build:
                _make_build(worktree_dir)
            else:
                # Skip the full make build but still populate kafka/ so
                # _bump_kafka_submodule can fetch + checkout inside it.
                _git(
                    str(worktree_dir),
                    "submodule", "update", "--init", "kafka",
                )
            _bump_kafka_submodule(worktree_dir, ak_commit, ak_branch)
        yield worktree_dir
    finally:
        if cleanup:
            if created:
                try:
                    _git(
                        repo_path, "worktree", "remove",
                        "--force", str(worktree_dir),
                    )
                except WorktreeError:
                    # Best-effort: a stale .git/worktrees entry is recoverable
                    # via `git worktree prune` later; don't mask the original
                    # exception (if any) by raising here.
                    pass
            if worktree_dir.exists():
                shutil.rmtree(worktree_dir, ignore_errors=True)


def _find_worktree_for_branch(
    repo_path: str, branch_name: str,
) -> Optional[str]:
    """Return the path of the worktree that currently has `branch_name`
    checked out, or None. Parses `git worktree list --porcelain`.
    """
    proc = subprocess.run(
        ["git", "-C", repo_path, "worktree", "list", "--porcelain"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return None
    current_path: Optional[str] = None
    for line in proc.stdout.splitlines():
        if line.startswith("worktree "):
            current_path = line[len("worktree "):]
        elif line == f"branch refs/heads/{branch_name}" and current_path:
            return current_path
    return None


def _remote_branch_exists(
    repo_path: str, branch_name: str, remote: str = "origin",
) -> bool:
    """True iff `branch_name` exists on `remote`.

    Probes via `git ls-remote --heads <remote> <branch_name>`. Output
    is non-empty if and only if the branch exists on the remote.

    On a non-zero exit (network failure, auth issue) we return False
    rather than raise: the caller treats "remote unknown" the same as
    "remote doesn't exist" -- the subsequent `git push` will surface
    any real connectivity issue with a clear error.
    """
    proc = subprocess.run(
        ["git", "-C", repo_path, "ls-remote", "--heads", remote, branch_name],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return False
    return bool(proc.stdout.strip())


def _local_branch_exists(repo_path: str, branch_name: str) -> bool:
    """True iff `branch_name` exists as a local ref under refs/heads/."""
    proc = subprocess.run(
        [
            "git", "-C", repo_path,
            "show-ref", "--verify", "--quiet",
            f"refs/heads/{branch_name}",
        ],
        capture_output=True,
    )
    return proc.returncode == 0


def _make_build(worktree_dir: Path) -> None:
    """Run `make` (= `make build`, the default target) inside the worktree.

    Per the root Makefile, `build` depends on `build-rust`, `build-c`
    (which itself depends on `submodules` -> `git submodule update --init
    --recursive`), and `build-python`. So a single `make` call walks the
    full prerequisite chain in the right order: submodules first, then
    cargo + cmake + python build. Required so the spawned agent can read
    AK source under `kafka/` and the C bindings have generated headers.

    Slow (typically a few minutes for a fresh worktree because of the
    cargo release build).
    """
    proc = subprocess.run(
        ["make"],
        cwd=str(worktree_dir),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise WorktreeError(
            f"make in {worktree_dir} failed (rc={proc.returncode}): "
            f"{proc.stderr.strip()}"
        )


def _bump_kafka_submodule(
    worktree_dir: Path, ak_commit: str, ak_branch: str = "trunk",
) -> None:
    """Check out `ak_commit` in `kafka/` and commit the submodule bump.

    Fetches `ak_branch` from origin first to make sure `ak_commit` is
    locally available (it may not be in the cached objects from the main
    repo if the main repo's submodule pointer is at a different commit).
    Then `git add kafka` + `git commit` records the pointer change as a
    standalone commit, BEFORE the spawned agent does its own work. The
    agent's subsequent commit (e.g. "Design document") sits on top.

    If the submodule was already at `ak_commit` (e.g. the local branch
    was already bumped in a previous run that we're now reusing), there's
    nothing to commit -- skip the commit silently rather than failing.
    """
    kafka_dir = str(worktree_dir / "kafka")
    _git(kafka_dir, "fetch", "origin", ak_branch)
    _git(kafka_dir, "checkout", ak_commit)
    _git(str(worktree_dir), "add", "kafka")
    # `git diff --cached --quiet` exits 0 if there are NO staged changes,
    # 1 if there are. We only want to commit when there's something to
    # commit; otherwise the bump is already recorded and we move on.
    diff = subprocess.run(
        ["git", "-C", str(worktree_dir), "diff", "--cached", "--quiet"],
        capture_output=True,
    )
    if diff.returncode == 0:
        return
    _git(
        str(worktree_dir), "commit",
        "-m", f"Bump kafka submodule to {ak_commit}",
    )


def push_branch_with_kafka_bump(
    rust_repo_path: str,
    branch_name: str,
    *,
    base_remote_branch: str,
    ak_commit: str,
    ak_branch: str,
) -> None:
    """Create `branch_name` on origin from `base_remote_branch` with one
    commit that bumps the kafka submodule pointer to `ak_commit`.

    Used by sweep step 3 before `gh pr create`. GitHub refuses to open a
    PR when head and base point at the same commit, so the PR-creation
    branch needs at least one differentiating commit. Bumping the kafka
    submodule is the natural choice -- the same bump the plan/impl
    worktree would do anyway, just done earlier.

    Implementation: opens a temporary worktree (no `make` build, just a
    `git submodule update --init kafka` to populate the directory),
    commits the bump via `_bump_kafka_submodule`, then `git push`es the
    local branch to origin. Worktree is torn down on exit.

    Idempotent: if origin already has the branch with this exact bump
    commit, the inner `_bump_kafka_submodule` is a no-op (no diff to
    commit) and the push is a no-op (same SHA).
    """
    with worktree_for_branch(
        rust_repo_path, branch_name,
        cleanup=True,
        base_remote_branch=base_remote_branch,
        ak_commit=ak_commit,
        ak_branch=ak_branch,
        build=False,
    ):
        _git(rust_repo_path, "push", "-u", "origin", branch_name)
