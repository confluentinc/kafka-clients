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


# --- update_pr_body / get_pr_body / prepend_pr_body -------------------------

def test_update_pr_body_passes_body_via_stdin():
    """Long markdown bodies must come via stdin to avoid argv limits.
    The argv shape is `gh pr edit <N> --body-file -` and the body
    arrives through `input=`."""
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0)) as mrun:
        github.update_pr_body("/repo", 42, "## Summary\n\nThe new body.")
    mrun.assert_called_once_with(
        ["gh", "pr", "edit", "42", "--body-file", "-"],
        input="## Summary\n\nThe new body.",
        capture_output=True, text=True,
        cwd="/repo",
    )


def test_update_pr_body_raises_on_nonzero_exit():
    with patch.object(github.subprocess, "run",
                      return_value=_completed(1, stderr="not authorized")):
        with pytest.raises(github.GhError, match="not authorized"):
            github.update_pr_body("/repo", 42, "x")


def test_add_pr_label_invokes_gh_pr_edit_add_label():
    """add_pr_label wraps `gh pr edit <N> --add-label <label>` and runs
    it in `repo_path`. argv is positional and stdin-free."""
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0)) as mrun:
        github.add_pr_label("/repo", 42, "implementation-needed")
    mrun.assert_called_once_with(
        ["gh", "pr", "edit", "42", "--add-label", "implementation-needed"],
        capture_output=True, text=True,
        cwd="/repo",
    )


def test_add_pr_label_raises_on_nonzero_exit():
    """A missing label or auth failure surfaces as GhError so the caller
    can log+continue (it's cosmetic) without crashing the sweep."""
    with patch.object(
        github.subprocess, "run",
        return_value=_completed(1, stderr="label 'implementation-needed' not found"),
    ):
        with pytest.raises(github.GhError, match="not found"):
            github.add_pr_label("/repo", 42, "implementation-needed")


def test_remove_pr_label_invokes_gh_pr_edit_remove_label():
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0)) as mrun:
        github.remove_pr_label("/repo", 42, "dependencies-evaluated")
    mrun.assert_called_once_with(
        ["gh", "pr", "edit", "42", "--remove-label", "dependencies-evaluated"],
        capture_output=True, text=True,
        cwd="/repo",
    )


def test_remove_pr_label_raises_on_nonzero_exit():
    with patch.object(
        github.subprocess, "run",
        return_value=_completed(1, stderr="not authorized"),
    ):
        with pytest.raises(github.GhError, match="not authorized"):
            github.remove_pr_label("/repo", 42, "dependencies-evaluated")


# --- delete_remote_branch ---------------------------------------------------

def test_delete_remote_branch_invokes_gh_api_delete():
    """delete_remote_branch wraps `gh api -X DELETE
    repos/{owner}/{repo}/git/refs/heads/<branch>` and runs it from
    `repo_path`. The {owner}/{repo} placeholders are emitted literally
    so `gh api` resolves them from the cwd's git remote."""
    branch = "kafka-translate/abc"
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0)) as mrun:
        github.delete_remote_branch("/repo", branch)
    mrun.assert_called_once_with(
        [
            "gh", "api", "-X", "DELETE",
            "repos/{owner}/{repo}/git/refs/heads/" + branch,
        ],
        capture_output=True, text=True,
        cwd="/repo",
    )


def test_delete_remote_branch_raises_branch_already_gone_on_422():
    """GitHub returns HTTP 422 'Reference does not exist' (NOT 404) when
    the ref to delete is missing. The helper detects that string and
    raises the distinct GhBranchAlreadyGone subclass so callers can
    treat it as a soft success."""
    with patch.object(
        github.subprocess, "run",
        return_value=_completed(1, stderr="gh: Reference does not exist (HTTP 422)"),
    ):
        with pytest.raises(github.GhBranchAlreadyGone, match="already gone"):
            github.delete_remote_branch("/repo", "kafka-translate/abc")


def test_delete_remote_branch_raises_plain_gh_error_for_other_failures():
    """Non-422 errors (auth, network, 5xx) surface as GhError, NOT
    GhBranchAlreadyGone -- callers must fail-fast on these."""
    with patch.object(
        github.subprocess, "run",
        return_value=_completed(1, stderr="HTTP 401: Bad credentials"),
    ):
        with pytest.raises(github.GhError, match="Bad credentials") as ei:
            github.delete_remote_branch("/repo", "kafka-translate/abc")
        # Specifically NOT the soft-success subclass.
        assert not isinstance(ei.value, github.GhBranchAlreadyGone)


# --- format_dep_section / replace_dep_section -------------------------------

def test_format_dep_section_both_deps_renders_both_lines():
    """Both PR numbers present produces the full two-line dep block."""
    s = github.format_dep_section(plan_dep_pr_number=123, impl_dep_pr_number=456)
    assert s == "**Dependencies:**\n- Plan: #123\n- Implementation: #456"


def test_format_dep_section_only_plan_dep_omits_impl_line():
    """Skip the impl line when impl dep is None (and vice versa)."""
    assert github.format_dep_section(plan_dep_pr_number=123) == (
        "**Dependencies:**\n- Plan: #123"
    )
    assert github.format_dep_section(impl_dep_pr_number=456) == (
        "**Dependencies:**\n- Implementation: #456"
    )


def test_format_dep_section_no_deps_returns_empty_string():
    """No deps -> empty string so callers can short-circuit (and
    replace_dep_section will then just strip any existing block)."""
    assert github.format_dep_section() == ""
    assert github.format_dep_section(None, None) == ""


def test_replace_dep_section_prepends_when_no_existing_block():
    """Body has no orchestrator markers yet -> the new block lands at
    the top with a blank-line separator before the existing body."""
    body = "## Summary\n\nOriginal body."
    section = "**Dependencies:**\n- Plan: #1"
    out = github.replace_dep_section(body, section)
    assert out == (
        f"{github.DEP_SECTION_START}\n"
        f"**Dependencies:**\n- Plan: #1\n"
        f"{github.DEP_SECTION_END}\n\n"
        f"## Summary\n\nOriginal body."
    )


def test_replace_dep_section_replaces_existing_block_in_place():
    """Calling twice replaces, never accumulates -- the markers delimit
    a single block. This is what makes the helper safe to retry on
    network failures during dep-eval."""
    body = "## Summary\n\nBody."
    first = github.replace_dep_section(
        body, "**Dependencies:**\n- Plan: #1",
    )
    second = github.replace_dep_section(
        first, "**Dependencies:**\n- Plan: #2",
    )
    assert "#1" not in second
    assert "#2" in second
    # Markers appear exactly once.
    assert second.count(github.DEP_SECTION_START) == 1
    assert second.count(github.DEP_SECTION_END) == 1


def test_replace_dep_section_empty_section_strips_existing_block():
    """Passing '' as the section is the way to clear the block (for
    cases where dep-eval discovers both deps are None)."""
    body_with = github.replace_dep_section(
        "## Summary", "**Dependencies:**\n- Plan: #1",
    )
    out = github.replace_dep_section(body_with, "")
    assert github.DEP_SECTION_START not in out
    assert github.DEP_SECTION_END not in out
    assert out.endswith("## Summary")


def test_replace_dep_section_only_block_no_other_body():
    """Edge: an otherwise empty body should just hold the block,
    without trailing blank-line ceremony."""
    out = github.replace_dep_section("", "**Dependencies:**\n- Plan: #1")
    assert out == (
        f"{github.DEP_SECTION_START}\n"
        f"**Dependencies:**\n- Plan: #1\n"
        f"{github.DEP_SECTION_END}"
    )


def test_get_pr_body_returns_stripped_string():
    """`gh pr view --json body --jq '.body'` emits the body with one
    trailing newline; we strip just that."""
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0, stdout="existing body\n")) as mrun:
        body = github.get_pr_body("/repo", 42)
    assert body == "existing body"
    mrun.assert_called_once_with(
        ["gh", "pr", "view", "42", "--json", "body", "--jq", ".body"],
        capture_output=True, text=True,
        cwd="/repo",
    )


def test_get_pr_body_raises_on_nonzero_exit():
    with patch.object(github.subprocess, "run",
                      return_value=_completed(1, stderr="not found")):
        with pytest.raises(github.GhError, match="not found"):
            github.get_pr_body("/repo", 42)


def test_prepend_pr_body_round_trips_get_then_update():
    """prepend_pr_body must read current body, prepend the line + a
    blank separator, and write the combined body back -- no truncation,
    no extra blanks."""
    calls = []

    def router(args, **kwargs):
        if "view" in args:
            calls.append(("view", args, kwargs))
            return _completed(0, stdout="Original body line.\n")
        if "edit" in args:
            calls.append(("edit", args, kwargs))
            return _completed(0)
        raise AssertionError(f"unexpected gh call: {args}")

    with patch.object(github.subprocess, "run", side_effect=router):
        github.prepend_pr_body("/repo", 42, "Approved on 2026-05-03")

    assert calls[0][0] == "view"
    assert calls[1][0] == "edit"
    # The body fed to `gh pr edit` is the prepend line + blank + original.
    edit_kwargs = calls[1][2]
    assert edit_kwargs["input"] == "Approved on 2026-05-03\n\nOriginal body line."


# --- get_pr_state ----------------------------------------------------------
#
# `gh pr view --json state,mergeCommit` is the contract probed here.
# Pin all three real-world response shapes so the closure-check sweep
# can rely on the return tuple without merge-style branching.

def test_get_pr_state_open_returns_state_and_none_merge_sha():
    out = '{"state":"OPEN","mergeCommit":null}'
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0, stdout=out)) as mrun:
        state, sha = github.get_pr_state("/repo", 42)
    assert state == "OPEN"
    assert sha is None
    mrun.assert_called_once_with(
        ["gh", "pr", "view", "42", "--json", "state,mergeCommit"],
        capture_output=True, text=True, cwd="/repo",
    )


def test_get_pr_state_closed_without_merge_returns_state_and_none():
    """Closed-without-merge: state=CLOSED, mergeCommit=null. Operator
    abandoned the PR; no commit on base for us to point cursor at."""
    out = '{"state":"CLOSED","mergeCommit":null}'
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0, stdout=out)):
        state, sha = github.get_pr_state("/repo", 42)
    assert state == "CLOSED"
    assert sha is None


def test_get_pr_state_merged_squash_returns_squash_commit_sha():
    """The most common merge style on this repo. Squash produces a
    single commit on base; gh exposes its SHA via mergeCommit.oid."""
    out = '{"state":"MERGED","mergeCommit":{"oid":"abc123squashsha"}}'
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0, stdout=out)):
        state, sha = github.get_pr_state("/repo", 42)
    assert state == "MERGED"
    assert sha == "abc123squashsha"


def test_get_pr_state_merged_via_merge_commit_returns_merge_commit_sha():
    """Same return shape as squash: gh's mergeCommit field is the
    base-branch commit containing the merge -- whether it's an actual
    merge commit, a squash, or the tip of a rebase. The closure-check
    code path doesn't need to differentiate."""
    out = '{"state":"MERGED","mergeCommit":{"oid":"def456mergecommit"}}'
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0, stdout=out)):
        state, sha = github.get_pr_state("/repo", 42)
    assert state == "MERGED"
    assert sha == "def456mergecommit"


def test_get_pr_state_merged_via_rebase_returns_rebased_tip_sha():
    """Rebase merges put new commits on base; mergeCommit is the tip
    (newest) of those. Same JSON shape as the other merge styles."""
    out = '{"state":"MERGED","mergeCommit":{"oid":"789rebasetipsha"}}'
    with patch.object(github.subprocess, "run",
                      return_value=_completed(0, stdout=out)):
        state, sha = github.get_pr_state("/repo", 42)
    assert state == "MERGED"
    assert sha == "789rebasetipsha"


def test_get_pr_state_raises_on_nonzero_exit():
    with patch.object(github.subprocess, "run",
                      return_value=_completed(1, stderr="not found")):
        with pytest.raises(github.GhError, match="not found"):
            github.get_pr_state("/repo", 42)


def test_prepend_pr_body_handles_empty_existing_body():
    """When the current body is empty, prepend_pr_body just writes
    the line with no leading separator."""
    def router(args, **kwargs):
        if "view" in args:
            return _completed(0, stdout="")
        if "edit" in args:
            assert kwargs["input"] == "Approved", kwargs["input"]
            return _completed(0)
        raise AssertionError(f"unexpected gh call: {args}")

    with patch.object(github.subprocess, "run", side_effect=router):
        github.prepend_pr_body("/repo", 42, "Approved")
