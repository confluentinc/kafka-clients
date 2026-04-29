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
from typing import Iterator


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
    repo_path: str, branch_name: str, *, cleanup: bool = True,
) -> Iterator[Path]:
    """Create a temporary git worktree checked out at `branch_name`.

    Yields the worktree path. The local `branch_name` ref is reset to
    `origin/<branch_name>` (which must already exist -- the orchestrator's
    Phase B sweep pushes these branches before plan/impl work begins).

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
        # Refresh origin/<branch_name>; safe even with the branch checked
        # out in another worktree because we don't use the colon refspec.
        _git(repo_path, "fetch", "origin", branch_name)
        # `-B` creates or resets the local branch to origin's tip, which
        # makes setup idempotent if a previous run left the ref behind.
        _git(
            repo_path, "worktree", "add",
            "-B", branch_name,
            str(worktree_dir),
            f"origin/{branch_name}",
        )
        created = True
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
