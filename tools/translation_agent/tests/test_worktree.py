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
