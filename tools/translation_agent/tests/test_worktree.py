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

from unittest.mock import patch

import pytest

from translation_agent import worktree


def _completed(rc, stdout="", stderr=""):
    return type("CP", (), {"returncode": rc, "stdout": stdout, "stderr": stderr})()


def _ok(*_args, **_kwargs):
    return _completed(0)


def test_worktree_for_branch_yields_path_runs_setup_and_teardown():
    calls = []

    def record(args, **kwargs):
        calls.append(args[3:])  # strip ["git", "-C", "/repo"]
        return _completed(0)

    with patch.object(worktree.subprocess, "run", side_effect=record):
        with worktree.worktree_for_branch("/repo", "kafka-translate/abc") as wt:
            yielded = wt
    # Order: fetch -> worktree add -> worktree remove (on exit).
    assert calls[0] == ["fetch", "origin", "kafka-translate/abc"]
    assert calls[1][:2] == ["worktree", "add"]
    assert "kafka-translate/abc" in calls[1]
    assert "origin/kafka-translate/abc" in calls[1]
    assert calls[-1][:3] == ["worktree", "remove", "--force"]
    # Yielded path is gone after cleanup.
    assert not yielded.exists()


def test_worktree_for_branch_cleans_up_on_inner_exception():
    yielded = None
    with patch.object(worktree.subprocess, "run", side_effect=_ok):
        with pytest.raises(RuntimeError, match="inner"):
            with worktree.worktree_for_branch("/repo", "br") as wt:
                yielded = wt
                raise RuntimeError("inner failure")
    assert yielded is not None
    assert not yielded.exists()


def test_worktree_for_branch_propagates_setup_failure_no_cleanup_call():
    """If the initial fetch fails, we never created a worktree -- don't try
    to remove one."""
    fetch_failed = False

    def maybe_fail(args, **kwargs):
        nonlocal fetch_failed
        if "fetch" in args:
            fetch_failed = True
            return _completed(1, stderr="fatal: no such ref")
        # If we ever get here, the test caught a regression: setup didn't
        # really fail, and we attempted further git ops anyway.
        return _completed(0)

    with patch.object(worktree.subprocess, "run", side_effect=maybe_fail):
        with pytest.raises(worktree.WorktreeError, match="no such ref"):
            with worktree.worktree_for_branch("/repo", "br"):
                pytest.fail("body should not have been entered")
    assert fetch_failed


def test_worktree_for_branch_swallows_cleanup_failure():
    """A failing `worktree remove` must not mask the inner exception."""
    def fail_only_on_remove(args, **kwargs):
        if "remove" in args:
            return _completed(1, stderr="fatal: locked")
        return _completed(0)

    with patch.object(worktree.subprocess, "run", side_effect=fail_only_on_remove):
        with pytest.raises(ValueError, match="boom"):
            with worktree.worktree_for_branch("/repo", "br"):
                raise ValueError("boom")


def test_worktree_for_branch_cleanup_false_preserves_dir():
    """With cleanup=False the worktree is left on disk for inspection."""
    calls = []

    def record(args, **kwargs):
        calls.append(args[3:])
        return _completed(0)

    with patch.object(worktree.subprocess, "run", side_effect=record):
        with worktree.worktree_for_branch("/repo", "br", cleanup=False) as wt:
            yielded = wt
    # No worktree-remove call should have been made.
    assert not any(a[:3] == ["worktree", "remove", "--force"] for a in calls)
    # The yielded path is still on disk.
    assert yielded.exists()
    # Manual cleanup so the test doesn't pollute /tmp.
    import shutil
    shutil.rmtree(yielded, ignore_errors=True)


def test_worktree_with_ak_commit_runs_make_build_and_bumps_submodule():
    """When `ak_commit` is provided, the worktree runs `make` (the
    Makefile's default target = `build`, which depends on `submodules`),
    fetches AK in kafka/, checks out the AK commit, and commits the
    submodule pointer bump -- all BEFORE yielding to the caller."""
    calls = []

    def record(args, **kwargs):
        # Record the full argv for git calls; for `make` record the
        # tool name + any args so we can spot it in the sequence.
        if args[0] == "make":
            calls.append(("make", tuple(args[1:])))
        elif args[0] == "git":
            # args[2] is the value for `-C` (the git path arg)
            calls.append(("git", args[2], tuple(args[3:])))
        return _completed(0)

    with patch.object(worktree.subprocess, "run", side_effect=record):
        with worktree.worktree_for_branch(
            "/repo", "kafka-translate/abc",
            ak_commit="ak123", ak_branch="trunk",
        ):
            pass

    # Verify ordered sequence:
    #  1. git -C /repo fetch origin kafka-translate/abc  (base fetch)
    #  2. git -C /repo worktree add ...                  (create worktree)
    #  3. make                                           (in worktree, default target)
    #  4. git -C <wt>/kafka fetch origin trunk           (refresh AK ref)
    #  5. git -C <wt>/kafka checkout ak123               (point at target)
    #  6. git -C <wt> add kafka                          (stage submodule bump)
    #  7. git -C <wt> commit -m "Bump kafka submodule to ak123"
    #  8. git -C /repo worktree remove --force ...       (cleanup)
    kinds = [c[0] for c in calls]
    assert kinds[2] == "make", f"make should be 3rd call, got {kinds}"
    # No target arg -- `make` defaults to the first target (build).
    assert calls[2][1] == ()

    # Find the kafka submodule operations.
    kafka_calls = [c for c in calls if c[0] == "git" and c[1].endswith("/kafka")]
    assert any(c[2][:2] == ("fetch", "origin") and c[2][2] == "trunk"
               for c in kafka_calls), kafka_calls
    assert any(c[2][:2] == ("checkout", "ak123") for c in kafka_calls), kafka_calls

    # Find the worktree-level git add kafka + commit.
    wt_calls = [c for c in calls if c[0] == "git" and not c[1].endswith("/kafka")
                and c[1] != "/repo"]
    assert any(c[2][:2] == ("add", "kafka") for c in wt_calls), wt_calls
    commit_call = [c for c in wt_calls if c[2][:1] == ("commit",)]
    assert commit_call, wt_calls
    assert "Bump kafka submodule to ak123" in commit_call[0][2]


def test_worktree_branch_name_with_slash_sanitized_in_temp_prefix():
    """Branch names with slashes must produce valid temp-dir paths."""
    seen_prefix = []

    def record(args, **kwargs):
        # The third positional arg to `worktree add` is the path.
        if args[3:5] == ["worktree", "add"]:
            seen_prefix.append(args[7])
        return _completed(0)

    with patch.object(worktree.subprocess, "run", side_effect=record):
        with worktree.worktree_for_branch("/repo", "kafka-translate/abc"):
            pass
    # The temp path uses an underscore in place of the slash.
    assert "translation-agent-kafka-translate_abc-" in seen_prefix[0]
