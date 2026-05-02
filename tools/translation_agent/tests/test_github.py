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

from translation_agent import github


def _completed(rc, stdout="", stderr=""):
    return type("CP", (), {"returncode": rc, "stdout": stdout, "stderr": stderr})()


def test_branch_name_for_ak_is_deterministic():
    assert github.branch_name_for_ak("abc123") == "kafka-translate/abc123"
    assert github.branch_name_for_ak("abc123") == github.branch_name_for_ak("abc123")


def test_pr_title_uses_short_sha():
    title = github.pr_title_for_ak("a" * 40, "Fix things")
    assert title == "Translate kafka commit aaaaaaaaaaaa: Fix things"


def test_pr_body_links_apache_kafka():
    body = github.pr_body_for_ak("a" * 40, "Fix things")
    assert "github.com/apache/kafka/commit/" + ("a" * 40) in body
    assert "**Fix things**" in body


def test_pr_body_appends_optional_body():
    body = github.pr_body_for_ak("abc", "Subj", "Long\nbody")
    assert "Long\nbody" in body


def test_create_draft_pr_returns_number_from_url():
    out = "https://github.com/owner/repo/pull/42\n"
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0, stdout=out)) as mrun:
        n = github.create_draft_pr(
            "/repo", base_branch="master", head_branch="kafka-translate/abc",
            title="t", body="b",
        )
    mrun.assert_called_once_with(
        [
            "gh", "pr", "create",
            "--draft",
            "--base", "master",
            "--head", "kafka-translate/abc",
            "--title", "t",
            "--body", "b",
        ],
        capture_output=True, text=True, cwd="/repo",
    )
    assert n == 42


def test_create_draft_pr_already_exists():
    err = "a pull request for branch \"kafka-translate/abc\" already exists"
    with patch.object(github.subprocess, "run",
                      return_value=_completed(1, stderr=err)):
        with pytest.raises(github.GhPrAlreadyExists):
            github.create_draft_pr(
                "/repo", "master", "kafka-translate/abc", "t", "b",
            )


def test_create_draft_pr_other_error():
    with patch.object(github.subprocess, "run",
                      return_value=_completed(1, stderr="boom")):
        with pytest.raises(github.GhError, match="boom"):
            github.create_draft_pr("/repo", "master", "head", "t", "b")


def test_create_draft_pr_unparseable_output():
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0, stdout="something weird")):
        with pytest.raises(github.GhError, match="could not parse"):
            github.create_draft_pr("/repo", "master", "head", "t", "b")


def test_find_pr_number_for_branch_returns_int():
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0, stdout='[{"number": 99}]')):
        assert github.find_pr_number_for_branch("/repo", "kafka-translate/abc") == 99


def test_find_pr_number_for_branch_none_when_empty():
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0, stdout="[]")):
        assert github.find_pr_number_for_branch("/repo", "kafka-translate/abc") is None
