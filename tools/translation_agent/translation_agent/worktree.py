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
    base = base_remote_branch if base_remote_branch is not None else branch_name
    try:
        # Refresh origin/<base>; safe even with that branch checked out in
        # another worktree because we don't use the colon refspec.
        _git(repo_path, "fetch", "origin", base)
        # `-B` creates or resets the local branch to origin's tip of `base`,
        # which makes setup idempotent if a previous run left the ref behind.
        _git(
            repo_path, "worktree", "add",
            "-B", branch_name,
            str(worktree_dir),
            f"origin/{base}",
        )
        created = True
        if ak_commit is not None:
            _make_build(worktree_dir)
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
    """
    kafka_dir = str(worktree_dir / "kafka")
    _git(kafka_dir, "fetch", "origin", ak_branch)
    _git(kafka_dir, "checkout", ak_commit)
    _git(str(worktree_dir), "add", "kafka")
    _git(
        str(worktree_dir), "commit",
        "-m", f"Bump kafka submodule to {ak_commit}",
    )
