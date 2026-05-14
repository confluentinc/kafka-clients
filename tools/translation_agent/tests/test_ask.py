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

"""Tests for the `--ask` side-channel manual promotion.

The promotion is dispatched by `.semaphore/ask.yml` -> `translation-agent
--pr <N> --ask "<text>"`. It runs a sandboxed claude with the reviewer's
free-text command and, depending on the PR's current status, posts an
answer comment and optionally pushes fixup commits the agent made in the
worktree. The promotion never advances the state machine.
"""

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import pytest

from translation_agent import cli, db, prompts


# --- Fixtures ----------------------------------------------------------------

@pytest.fixture
def real_worktree(tmp_path, monkeypatch):
    """Replace `worktree.worktree_for_branch` with a context manager that
    yields a REAL temp directory.

    Unlike `test_cli.py`'s fake `Path("/fake/...")` mock, --ask code
    needs to actually write/stat ./ask_answer.md inside the worktree, so
    a non-existent path won't do. Each test gets a fresh tmp dir per
    pytest's `tmp_path` fixture.
    """
    wt_dir = tmp_path / "worktree"
    wt_dir.mkdir()

    @contextmanager
    def fake_wt(repo_path, branch_name, **kwargs):
        # Accept any kwargs (cleanup, base_remote_branch, ak_commit,
        # ak_branch, build) so signature changes upstream don't break
        # the fixture.
        yield wt_dir

    monkeypatch.setattr(
        "translation_agent.cli.worktree.worktree_for_branch", fake_wt,
    )
    return wt_dir


@pytest.fixture
def fake_streaming(monkeypatch):
    """Default streaming.run_with_prefix to (rc=0, "ok"). Tests that need
    a different return / side-effect re-patch with their own value.

    Returns the MagicMock so tests can assert call counts / inspect
    arguments.
    """
    mock = MagicMock(return_value=(0, "ok"))
    monkeypatch.setattr(
        "translation_agent.cli.streaming.run_with_prefix", mock,
    )
    return mock


@pytest.fixture(autouse=True)
def _default_pr_body_mock(monkeypatch):
    """Patch `github.get_pr_body` to a fixed string by default so tests
    don't accidentally invoke real `gh pr view` against the developer's
    repo. `_run_pr_ask` now calls this on every supported status, so
    every integration test would otherwise be non-hermetic.

    Tests that want to exercise the real path (or assert on the body)
    re-patch the same target with `with patch(...)` inside the test;
    the inner patch wins for its `with` scope.
    """
    monkeypatch.setattr(
        "translation_agent.cli.github.get_pr_body",
        lambda repo_path, pr_number: "default test PR body",
    )


@pytest.fixture
def no_artifact_io(monkeypatch):
    """Stub out the four locked_db artifact RPCs so the suite runs in
    environments without the Semaphore `artifact` CLI installed. Mirrors
    `test_cli.py`'s `_no_artifact_io_in_cli_tests` autouse but is
    explicitly opt-in here (test_ask.py controls its own fixture set).
    """
    monkeypatch.setattr(
        "translation_agent.locked_db.semaphore.push_project_artifact_no_force",
        lambda name, file_path, destination=None: None,
    )
    monkeypatch.setattr(
        "translation_agent.locked_db.semaphore.push_project_artifact",
        lambda name, file_path: None,
    )
    monkeypatch.setattr(
        "translation_agent.locked_db.semaphore.yank_project_artifact",
        lambda name: None,
    )
    monkeypatch.setattr(
        "translation_agent.locked_db.semaphore.pull_project_artifact",
        lambda name, dest_dir: None,
    )


def _run(*argv, db_path=None):
    if db_path:
        argv = ["--db-path", db_path] + list(argv)
    return cli.main(list(argv))


def _insert_pr_at_status(
    db_path, pr_number, ak_commit, status,
    rust_branch="master", ak_branch="trunk",
):
    conn = db.connect(db_path)
    db.migrate(conn)
    db.insert_pr_commit(conn, pr_number, rust_branch, ak_branch, ak_commit)
    conn.execute(
        "UPDATE pr_commit SET status = ? WHERE pr_number = ?",
        (status, pr_number),
    )
    conn.commit()
    conn.close()


def _write_answer(wt_dir: Path, content: str = "answer body") -> None:
    """Helper: write ./ask_answer.md inside the worktree from the test's
    streaming side-effect, simulating what claude would do."""
    (wt_dir / "ask_answer.md").write_text(content)


# --- Prompt rendering --------------------------------------------------------

def test_ask_question_only_template_substitutes_and_quotes_command():
    """The Q-only prompt must (a) interpolate every placeholder, (b)
    embed the user command between the BEGIN/END REVIEWER markers, and
    (c) instruct claude to write ./ask_answer.md with a `> `-quoted
    blockquote of the reviewer's command at the top."""
    p = prompts.ASK_QUESTION_ONLY_PROMPT_TEMPLATE.format(
        user_command="What does foo() do?",
        ak_commit="abc123",
        ak_branch="trunk",
        pr_number=42,
        branch_name="kafka-translate/abc123",
        plan_path="./design/history/42_description/plan.md",
        pr_body="## Summary\n\nThe scope of this PR.",
    )
    assert "What does foo() do?" in p
    assert "--- BEGIN REVIEWER COMMAND ---" in p
    assert "--- END REVIEWER COMMAND ---" in p
    assert "./ask_answer.md" in p
    assert "> " in p  # the quote-instruction explains the prefix
    assert "must not create any commits" in p
    assert "abc123" in p
    assert "kafka-translate/abc123" in p


def test_ask_plan_fixup_template_substitutes_and_allows_fixup_commits():
    p = prompts.ASK_PLAN_FIXUP_PROMPT_TEMPLATE.format(
        user_command="Please clarify the dependency rationale.",
        ak_commit="abc",
        ak_branch="trunk",
        pr_number=7,
        branch_name="kafka-translate/abc",
        plan_path="./design/history/7_description/plan.md",
        pr_body="## Summary\n\nPlan-fixup PR body.",
    )
    assert "Please clarify the dependency rationale." in p
    assert "./design/history/7_description/plan.md" in p
    assert "git commit --fixup=" in p
    assert "Do NOT run `git push`" in p
    assert "Do NOT run `git push` or `gh pr edit`" in p


def test_ask_impl_or_plan_fixup_template_allows_make_verify():
    p = prompts.ASK_IMPL_OR_PLAN_FIXUP_PROMPT_TEMPLATE.format(
        user_command="Add a doc comment to bar().",
        ak_commit="abc",
        ak_branch="trunk",
        pr_number=11,
        branch_name="kafka-translate/abc",
        plan_path="./design/history/11_description/plan.md",
        pr_body="## Summary\n\nImpl-fixup PR body.",
    )
    assert "Add a doc comment to bar()." in p
    assert "make verify" in p
    assert "git commit --fixup=" in p
    assert "Do NOT run `git push`" in p


def test_ask_template_handles_multiline_command_with_backticks():
    """Prompt rendering must not fail on user commands with newlines or
    code-fence backticks (common when reviewers paste short snippets)."""
    multiline_cmd = (
        "Why is this loop O(n^2)?\n"
        "Look at `consumer.poll()`:\n"
        "```rust\n"
        "for r in records { for t in topics { ... } }\n"
        "```"
    )
    p = prompts.ASK_PLAN_FIXUP_PROMPT_TEMPLATE.format(
        user_command=multiline_cmd,
        ak_commit="abc", ak_branch="trunk", pr_number=1,
        branch_name="b", plan_path="p.md",
        pr_body="(body)",
    )
    assert multiline_cmd in p


@pytest.mark.parametrize(
    "template",
    [
        prompts.ASK_QUESTION_ONLY_PROMPT_TEMPLATE,
        prompts.ASK_PLAN_FIXUP_PROMPT_TEMPLATE,
        prompts.ASK_IMPL_OR_PLAN_FIXUP_PROMPT_TEMPLATE,
    ],
)
def test_ask_template_includes_pr_body_block(template):
    """All three ask templates must inline the PR description between
    BEGIN/END markers so the agent sees what the reviewer is reasoning
    against. Verbatim body text appears between the markers."""
    body = "## Summary\n\nThe quick brown fox jumps over the lazy dog."
    p = template.format(
        user_command="cmd", ak_commit="abc", ak_branch="trunk",
        pr_number=1, branch_name="b", plan_path="p.md", pr_body=body,
    )
    assert "--- BEGIN PR DESCRIPTION ---" in p
    assert "--- END PR DESCRIPTION ---" in p
    assert body in p
    # The body block precedes the reviewer-command block (so the agent
    # reads the PR's stated scope before it sees the reviewer's question).
    assert p.index("--- END PR DESCRIPTION ---") < p.index(
        "--- BEGIN REVIEWER COMMAND ---"
    )


# --- Argparse / dispatch validation -----------------------------------------

def test_ask_without_pr_returns_2(tmp_path):
    """`--ask` without `--pr` is an operator error (the side-channel
    targets one PR row) -- reject at argparse-validation time with
    rc=2 (usage error), not rc=1 (operational error)."""
    db_path = str(tmp_path / "t.db")
    rc = _run("--ask", "anything", db_path=db_path)
    assert rc == 2


def test_ask_with_plan_approve_returns_2(tmp_path):
    """`--ask` and `--plan-approve` are mutually exclusive: one advances
    the state machine, the other is read-only w.r.t. status. Mixing
    them is an operator error."""
    db_path = str(tmp_path / "t.db")
    rc = _run(
        "--pr", "42", "--ask", "anything", "--plan-approve",
        "--no-artifact-push",
        db_path=db_path,
    )
    assert rc == 2


def test_ask_unknown_pr_returns_1(tmp_path, no_artifact_io):
    """Acting on a PR the orchestrator doesn't know about is a real
    operator error (vs. the `--pr <N>` status-check path which returns
    0 for unknown PRs because Semaphore auto-triggers it on every
    PR build)."""
    db_path = str(tmp_path / "t.db")
    rc = _run("--pr", "999", "--ask", "anything", db_path=db_path)
    assert rc == 1


def test_ask_empty_command_returns_1(tmp_path, no_artifact_io):
    """An empty / whitespace-only command would produce no useful
    answer; reject upfront rather than spend an r2 invocation on it."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)
    rc = _run("--pr", "42", "--ask", "   ", db_path=db_path)
    assert rc == 1


# --- _ask_prompt_for_status dispatch ----------------------------------------

@pytest.mark.parametrize(
    "status,expected",
    [
        (db.STATUS_NO_PLAN,                  None),
        (db.STATUS_DEPENDENCIES_EVALUATED,
         prompts.ASK_QUESTION_ONLY_PROMPT_TEMPLATE),
        (db.STATUS_PLAN_CREATED,
         prompts.ASK_PLAN_FIXUP_PROMPT_TEMPLATE),
        (db.STATUS_PLAN_APPROVED,
         prompts.ASK_PLAN_FIXUP_PROMPT_TEMPLATE),
        (db.STATUS_IMPLEMENTATION_DONE,
         prompts.ASK_IMPL_OR_PLAN_FIXUP_PROMPT_TEMPLATE),
        (99,                                 None),
    ],
)
def test_ask_prompt_dispatch(status, expected):
    """Status -> template mapping. Status 0 (no plan to discuss) and
    out-of-enum values map to None (unsupported). Status 3 deliberately
    maps to the same template as status 2 per design decision -- a
    reviewer asking at plan_approved should still get plan-fixup
    behavior, not full impl-fixup (the impl hasn't run yet)."""
    assert cli._ask_prompt_for_status(status) is expected


def test_ask_status_0_refuses_with_rc_1(tmp_path, real_worktree, no_artifact_io):
    """Status 0 (no_plan): no plan to discuss yet; refuse with rc=1.
    No worktree call, no streaming call, no PR write."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_NO_PLAN)
    with patch("translation_agent.cli.streaming.run_with_prefix") as mstream, \
         patch("translation_agent.cli.github.add_pr_comment") as mcomment, \
         patch("translation_agent.cli.git_ops.push_branch") as mpush:
        rc = _run("--pr", "42", "--ask", "anything", db_path=db_path)
    assert rc == 1
    mstream.assert_not_called()
    mcomment.assert_not_called()
    mpush.assert_not_called()


# --- Status 1: question-only mode -------------------------------------------

def test_ask_status_1_posts_comment_and_does_not_push(
    tmp_path, real_worktree, fake_streaming, no_artifact_io,
):
    """Status 1: agent writes ask_answer.md, orchestrator posts it as a
    PR comment, no push happens (Q-only mode)."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(
        db_path, 42, "abc", db.STATUS_DEPENDENCIES_EVALUATED,
    )

    def streaming_side_effect(*args, **kwargs):
        _write_answer(real_worktree, "> What does X do?\n\nIt does Y.")
        return (0, "ok")
    fake_streaming.side_effect = streaming_side_effect

    with patch("translation_agent.cli.git_ops.rev_parse",
               return_value="same_sha") as mrev, \
         patch("translation_agent.cli.git_ops.push_branch") as mpush, \
         patch("translation_agent.cli.git_ops.reset_hard") as mreset, \
         patch("translation_agent.cli.github.add_pr_comment") as mcomment:
        rc = _run("--pr", "42", "--ask", "What does X do?", db_path=db_path)
    assert rc == 0
    fake_streaming.assert_called_once()
    mcomment.assert_called_once_with(
        ANY, 42, str(real_worktree / "ask_answer.md"),
    )
    mpush.assert_not_called()
    mreset.assert_not_called()  # no commits made -> nothing to reset

    # Status unchanged (--ask never advances).
    conn = db.connect(db_path)
    pr = dict(conn.execute(
        "SELECT * FROM pr_commit WHERE pr_number = 42",
    ).fetchone())
    assert pr["status"] == db.STATUS_DEPENDENCIES_EVALUATED


def test_ask_status_1_discards_unexpected_commits(
    tmp_path, real_worktree, fake_streaming, no_artifact_io,
):
    """Status 1: if the agent ignored the prompt and made commits
    anyway, the orchestrator detects the HEAD change, posts the answer
    anyway, and `git reset --hard`s the local branch back to the
    pre-run HEAD. Push is NEVER called."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(
        db_path, 42, "abc", db.STATUS_DEPENDENCIES_EVALUATED,
    )

    def streaming_side_effect(*args, **kwargs):
        _write_answer(real_worktree)
        return (0, "ok")
    fake_streaming.side_effect = streaming_side_effect

    with patch(
        "translation_agent.cli.git_ops.rev_parse",
        side_effect=["pre_sha", "post_sha"],  # different HEADs
    ), \
         patch("translation_agent.cli.git_ops.push_branch") as mpush, \
         patch("translation_agent.cli.git_ops.reset_hard") as mreset, \
         patch("translation_agent.cli.github.add_pr_comment") as mcomment:
        rc = _run("--pr", "42", "--ask", "Q?", db_path=db_path)
    assert rc == 0
    mcomment.assert_called_once()
    mreset.assert_called_once_with(str(real_worktree), "pre_sha")
    mpush.assert_not_called()


# --- Status 2 / 3: plan-fixup mode ------------------------------------------

@pytest.mark.parametrize(
    "status",
    [db.STATUS_PLAN_CREATED, db.STATUS_PLAN_APPROVED],
)
def test_ask_status_2_or_3_pushes_when_agent_committed(
    tmp_path, real_worktree, fake_streaming, no_artifact_io, status,
):
    """Status 2 or 3 with a fixup commit: posts comment, pushes branch
    force-true (orchestrator-owned branch), status unchanged."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", status)

    def streaming_side_effect(*args, **kwargs):
        _write_answer(real_worktree)
        return (0, "ok")
    fake_streaming.side_effect = streaming_side_effect

    with patch(
        "translation_agent.cli.git_ops.rev_parse",
        side_effect=["pre_sha", "post_sha"],
    ), \
         patch("translation_agent.cli.git_ops.push_branch") as mpush, \
         patch("translation_agent.cli.git_ops.reset_hard") as mreset, \
         patch("translation_agent.cli.github.add_pr_comment") as mcomment:
        rc = _run("--pr", "42", "--ask", "Tweak the plan", db_path=db_path)
    assert rc == 0
    mcomment.assert_called_once()
    mpush.assert_called_once_with(
        ANY, "kafka-translate/abc", force=True,
    )
    mreset.assert_not_called()  # status >= 2: never reset

    conn = db.connect(db_path)
    assert conn.execute(
        "SELECT status FROM pr_commit WHERE pr_number = 42",
    ).fetchone()[0] == status


def test_ask_status_2_no_commit_no_push(
    tmp_path, real_worktree, fake_streaming, no_artifact_io,
):
    """Status 2 without a commit (agent only answered): comment posted,
    push NOT called -- nothing to publish."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)

    def streaming_side_effect(*args, **kwargs):
        _write_answer(real_worktree)
        return (0, "ok")
    fake_streaming.side_effect = streaming_side_effect

    with patch(
        "translation_agent.cli.git_ops.rev_parse",
        return_value="same_sha",  # same HEAD pre and post
    ), \
         patch("translation_agent.cli.git_ops.push_branch") as mpush, \
         patch("translation_agent.cli.github.add_pr_comment") as mcomment:
        rc = _run("--pr", "42", "--ask", "Question?", db_path=db_path)
    assert rc == 0
    mcomment.assert_called_once()
    mpush.assert_not_called()


# --- Status 4: full fixup mode ----------------------------------------------

def test_ask_status_4_pushes_impl_fixup(
    tmp_path, real_worktree, fake_streaming, no_artifact_io,
):
    """Status 4: agent may amend impl as well as plan. Worktree must be
    requested with build=True (so `make verify` can run); push happens
    when commits were made; status stays at 4."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_IMPLEMENTATION_DONE)

    def streaming_side_effect(*args, **kwargs):
        _write_answer(real_worktree)
        return (0, "ok")
    fake_streaming.side_effect = streaming_side_effect

    with patch(
        "translation_agent.cli.worktree.worktree_for_branch",
    ) as mwt:
        # Re-mock worktree to capture the build= kwarg.
        @contextmanager
        def fake_wt(repo_path, branch_name, **kwargs):
            mwt.captured_kwargs = kwargs
            yield real_worktree
        mwt.side_effect = fake_wt
        with patch(
            "translation_agent.cli.git_ops.rev_parse",
            side_effect=["pre", "post"],
        ), \
             patch("translation_agent.cli.git_ops.push_branch") as mpush, \
             patch("translation_agent.cli.github.add_pr_comment") as mcomment:
            rc = _run(
                "--pr", "42", "--ask", "Add a docstring",
                db_path=db_path,
            )
    assert rc == 0
    mcomment.assert_called_once()
    mpush.assert_called_once()
    assert mwt.captured_kwargs["build"] is True


def test_ask_status_2_uses_build_false(
    tmp_path, real_worktree, fake_streaming, no_artifact_io,
):
    """Status 2 (plan-only fixup): no Rust compilation needed, so the
    worktree is requested with build=False. This saves the multi-minute
    `make build` chain on every plan-phase ask."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)

    def streaming_side_effect(*args, **kwargs):
        _write_answer(real_worktree)
        return (0, "ok")
    fake_streaming.side_effect = streaming_side_effect

    with patch(
        "translation_agent.cli.worktree.worktree_for_branch",
    ) as mwt:
        @contextmanager
        def fake_wt(repo_path, branch_name, **kwargs):
            mwt.captured_kwargs = kwargs
            yield real_worktree
        mwt.side_effect = fake_wt
        with patch(
            "translation_agent.cli.git_ops.rev_parse",
            return_value="same",
        ), \
             patch("translation_agent.cli.git_ops.push_branch"), \
             patch("translation_agent.cli.github.add_pr_comment"):
            rc = _run(
                "--pr", "42", "--ask", "Tiny plan change",
                db_path=db_path,
            )
    assert rc == 0
    assert mwt.captured_kwargs["build"] is False


# --- Failure modes ----------------------------------------------------------

def test_ask_missing_answer_file_returns_1(
    tmp_path, real_worktree, fake_streaming, no_artifact_io,
):
    """If the agent exits 0 but never wrote ./ask_answer.md, the
    promotion fails: there's nothing to publish, and pretending the
    side-channel succeeded would silently hide the regression."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)
    # streaming returns 0 but doesn't create ask_answer.md
    fake_streaming.return_value = (0, "ok")

    with patch("translation_agent.cli.git_ops.rev_parse",
               return_value="same"), \
         patch("translation_agent.cli.github.add_pr_comment") as mcomment, \
         patch("translation_agent.cli.git_ops.push_branch") as mpush:
        rc = _run("--pr", "42", "--ask", "Q?", db_path=db_path)
    assert rc == 1
    mcomment.assert_not_called()
    mpush.assert_not_called()


def test_ask_empty_answer_file_returns_1(
    tmp_path, real_worktree, fake_streaming, no_artifact_io,
):
    """An empty ask_answer.md is treated as a failure: posting an
    empty PR comment is worse than failing loudly."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)

    def streaming_side_effect(*args, **kwargs):
        _write_answer(real_worktree, "")
        return (0, "ok")
    fake_streaming.side_effect = streaming_side_effect

    with patch("translation_agent.cli.git_ops.rev_parse",
               return_value="same"), \
         patch("translation_agent.cli.github.add_pr_comment") as mcomment:
        rc = _run("--pr", "42", "--ask", "Q?", db_path=db_path)
    assert rc == 1
    mcomment.assert_not_called()


def test_ask_r2_failure_returns_1_without_setting_last_error(
    tmp_path, real_worktree, fake_streaming, no_artifact_io,
):
    """When r2 itself fails, --ask returns rc=1 BUT does NOT call
    set_last_error -- ask is read-only w.r.t. the state machine; the
    next automatic `--pr <N>` build must not see a phantom cascade
    failure on its row."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)
    fake_streaming.return_value = (3, "boom")

    with patch("translation_agent.cli.git_ops.rev_parse",
               return_value="same"), \
         patch("translation_agent.cli.db.set_last_error") as mset:
        rc = _run("--pr", "42", "--ask", "Q?", db_path=db_path)
    assert rc == 1
    mset.assert_not_called()
    # Status untouched, last_error unset.
    conn = db.connect(db_path)
    pr = dict(conn.execute(
        "SELECT * FROM pr_commit WHERE pr_number = 42",
    ).fetchone())
    assert pr["status"] == db.STATUS_PLAN_CREATED
    assert pr["last_error"] is None


def test_ask_does_not_advance_status_on_success(
    tmp_path, real_worktree, fake_streaming, no_artifact_io,
):
    """Belt-and-suspenders: even on full success across all paths, the
    PR's status is never updated. Verified for each status the
    promotion supports."""
    db_path = str(tmp_path / "t.db")
    for pr_num, status in [
        (10, db.STATUS_DEPENDENCIES_EVALUATED),
        (20, db.STATUS_PLAN_CREATED),
        (30, db.STATUS_PLAN_APPROVED),
        (40, db.STATUS_IMPLEMENTATION_DONE),
    ]:
        _insert_pr_at_status(db_path, pr_num, f"ak{pr_num}", status)

        def streaming_side_effect(*args, **kwargs):
            _write_answer(real_worktree)
            return (0, "ok")
        fake_streaming.side_effect = streaming_side_effect

        with patch("translation_agent.cli.git_ops.rev_parse",
                   return_value="same"), \
             patch("translation_agent.cli.git_ops.push_branch"), \
             patch("translation_agent.cli.git_ops.reset_hard"), \
             patch("translation_agent.cli.github.add_pr_comment"):
            rc = _run(
                "--pr", str(pr_num), "--ask", "Q?", db_path=db_path,
            )
        assert rc == 0, f"status {status} returned rc={rc}"
        conn = db.connect(db_path)
        actual = conn.execute(
            "SELECT status FROM pr_commit WHERE pr_number = ?",
            (pr_num,),
        ).fetchone()[0]
        assert actual == status, (
            f"PR #{pr_num}: status changed from {status} to {actual}"
        )
        conn.close()


# --- PR-body context (inlined into the prompt) -----------------------------

def test_ask_includes_pr_body_in_prompt(
    tmp_path, real_worktree, fake_streaming, no_artifact_io, monkeypatch,
):
    """The PR body fetched via gh must appear inside the prompt between
    the BEGIN/END PR-DESCRIPTION markers, so the agent reads the same
    context the reviewer is reasoning against."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(
        db_path, 42, "abc", db.STATUS_DEPENDENCIES_EVALUATED,
    )
    body_text = (
        "## Summary\n\n"
        "This PR translates `FetchRequest` from Java to Rust.\n\n"
        "**Dependencies:**\n- Plan: #41"
    )
    monkeypatch.setattr(
        "translation_agent.cli.github.get_pr_body",
        lambda repo_path, pr_number: body_text,
    )

    def streaming_side_effect(*args, **kwargs):
        _write_answer(real_worktree)
        return (0, "ok")
    fake_streaming.side_effect = streaming_side_effect

    with patch("translation_agent.cli.git_ops.rev_parse",
               return_value="same"), \
         patch("translation_agent.cli.github.add_pr_comment"):
        rc = _run("--pr", "42", "--ask", "Q?", db_path=db_path)
    assert rc == 0
    # Prompt is the second positional arg to streaming.run_with_prefix:
    # `[*r2.R2_CLAUDE_CMD_PREFIX, "-p", prompt]` -- index -1 is the
    # prompt text itself.
    cmd_args = fake_streaming.call_args.args[0]
    prompt = cmd_args[-1]
    assert "--- BEGIN PR DESCRIPTION ---" in prompt
    assert "--- END PR DESCRIPTION ---" in prompt
    assert body_text in prompt


def test_ask_hard_fails_when_pr_body_fetch_fails(
    tmp_path, fake_streaming, no_artifact_io, monkeypatch,
):
    """If `gh pr view` fails for the PR body, --ask must hard-fail with
    rc=1 BEFORE creating the worktree or invoking r2. The PR description
    is required context; better to surface the error than to send a
    half-informed agent."""
    from translation_agent import github as gh
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(
        db_path, 42, "abc", db.STATUS_DEPENDENCIES_EVALUATED,
    )

    def boom(repo_path, pr_number):
        raise gh.GhError("simulated gh failure")
    monkeypatch.setattr(
        "translation_agent.cli.github.get_pr_body", boom,
    )

    with patch(
        "translation_agent.cli.worktree.worktree_for_branch",
    ) as mwt, \
         patch("translation_agent.cli.github.add_pr_comment") as mcomment:
        rc = _run("--pr", "42", "--ask", "Q?", db_path=db_path)
    assert rc == 1
    # Fail-fast: worktree never opened, r2 never invoked, no comment.
    mwt.assert_not_called()
    fake_streaming.assert_not_called()
    mcomment.assert_not_called()


def test_ask_synthetic_pr_number_uses_placeholder_body(
    tmp_path, real_worktree, fake_streaming, no_artifact_io, monkeypatch,
):
    """Synthetic (negative) pr_numbers skip the gh fetch entirely and
    use a literal placeholder string. Mirrors the synthetic-pr_number
    guard at the comment-posting site."""
    sentinel = MagicMock(side_effect=AssertionError(
        "github.get_pr_body must NOT be called for synthetic pr_numbers",
    ))
    monkeypatch.setattr(
        "translation_agent.cli.github.get_pr_body", sentinel,
    )

    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(
        db_path, -1, "abc", db.STATUS_DEPENDENCIES_EVALUATED,
    )

    def streaming_side_effect(*args, **kwargs):
        _write_answer(real_worktree)
        return (0, "ok")
    fake_streaming.side_effect = streaming_side_effect

    with patch("translation_agent.cli.git_ops.rev_parse",
               return_value="same"), \
         patch("translation_agent.cli.github.add_pr_comment") as mcomment:
        rc = _run("--pr", "-1", "--ask", "Q?", db_path=db_path)
    assert rc == 0
    sentinel.assert_not_called()
    # No comment posted for synthetic pr_numbers (existing guard).
    mcomment.assert_not_called()
    # Prompt contains the placeholder, not a fetched body.
    prompt = fake_streaming.call_args.args[0][-1]
    assert "(synthetic PR; no description fetched)" in prompt


def test_ask_empty_pr_body_renders_empty_markers(
    tmp_path, real_worktree, fake_streaming, no_artifact_io, monkeypatch,
):
    """An empty PR body still renders the BEGIN/END markers (with
    nothing between them), and the agent invocation proceeds normally.
    The markers carry the meaning -- the absence of text between them
    explicitly signals 'no description'."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(
        db_path, 42, "abc", db.STATUS_DEPENDENCIES_EVALUATED,
    )
    monkeypatch.setattr(
        "translation_agent.cli.github.get_pr_body",
        lambda repo_path, pr_number: "",
    )

    def streaming_side_effect(*args, **kwargs):
        _write_answer(real_worktree)
        return (0, "ok")
    fake_streaming.side_effect = streaming_side_effect

    with patch("translation_agent.cli.git_ops.rev_parse",
               return_value="same"), \
         patch("translation_agent.cli.github.add_pr_comment"):
        rc = _run("--pr", "42", "--ask", "Q?", db_path=db_path)
    assert rc == 0
    fake_streaming.assert_called_once()
    prompt = fake_streaming.call_args.args[0][-1]
    assert (
        "--- BEGIN PR DESCRIPTION ---\n\n--- END PR DESCRIPTION ---"
        in prompt
    )
