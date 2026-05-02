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

from translation_agent import git_ops


def _completed(rc, stdout="", stderr=""):
    return type("CP", (), {"returncode": rc, "stdout": stdout, "stderr": stderr})()


def test_run_git_raises_on_nonzero_exit():
    with patch.object(git_ops.subprocess, "run", return_value=_completed(128, "", "fatal: bad")):
        with pytest.raises(git_ops.GitError, match="fatal: bad"):
            git_ops._run_git("/tmp", ["status"])


def test_fetch_invokes_git_fetch():
    with patch.object(git_ops.subprocess, "run", return_value=_completed(0)) as mrun:
        git_ops.fetch("/repo", "trunk")
    mrun.assert_called_once_with(
        ["git", "-C", "/repo", "fetch", "origin", "trunk"],
        capture_output=True, text=True,
    )


def test_rev_parse_returns_stripped_sha():
    with patch.object(git_ops.subprocess, "run", return_value=_completed(0, "abc123\n")):
        assert git_ops.rev_parse("/repo", "HEAD") == "abc123"


def test_next_commits_parses_oldest_to_newest():
    out = "sha1\nsha2\nsha3\n"
    with patch.object(git_ops.subprocess, "run", return_value=_completed(0, out)) as mrun:
        commits = git_ops.next_commits("/repo", since="base", branch="trunk", n=10)
    mrun.assert_called_once_with(
        [
            "git", "-C", "/repo",
            "log", "--reverse", "base..trunk",
            "--max-count", "10", "--format=%H",
        ],
        capture_output=True, text=True,
    )
    assert commits == ["sha1", "sha2", "sha3"]


def test_next_commits_handles_empty_output():
    with patch.object(git_ops.subprocess, "run", return_value=_completed(0, "")):
        assert git_ops.next_commits("/repo", "base", "trunk") == []


def test_commit_subject_strips_whitespace():
    with patch.object(git_ops.subprocess, "run",
                      return_value=_completed(0, "Add foo bar\n")):
        assert git_ops.commit_subject("/repo", "abc") == "Add foo bar"


def test_push_new_branch_uses_refspec():
    with patch.object(git_ops.subprocess, "run", return_value=_completed(0)) as mrun:
        git_ops.push_new_branch("/repo", source_ref="origin/master",
                                target_branch="kafka-translate/abc")
    mrun.assert_called_once_with(
        [
            "git", "-C", "/repo",
            "push", "origin", "origin/master:refs/heads/kafka-translate/abc",
        ],
        capture_output=True, text=True,
    )
