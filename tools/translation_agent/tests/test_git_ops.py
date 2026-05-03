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


def _next_commits_router(log_stdout: str = ""):
    """Side-effect builder for next_commits tests.

    next_commits now first runs `git cat-file -e <since>^{commit}` to
    decide whether the cursor commit is in the local object DB. We treat
    that as "yes" so the deepen path is skipped, then return `log_stdout`
    for the `git log` call. Any other call raises so we catch unexpected
    git invocations.
    """
    def router(args, **_kwargs):
        if "cat-file" in args:
            return _completed(0)  # commit present
        if "log" in args and "--reverse" in args:
            return _completed(0, log_stdout)
        raise AssertionError(f"unexpected git call: {args}")
    return router


def test_next_commits_parses_oldest_to_newest():
    out = "sha1\nsha2\nsha3\n"
    with patch.object(git_ops.subprocess, "run",
                      side_effect=_next_commits_router(out)) as mrun:
        commits = git_ops.next_commits("/repo", since="base", branch="trunk", n=10)
    # The log call (the second one, after the cat-file probe) carries
    # the actual range argv we want to pin.
    log_call = next(c for c in mrun.call_args_list
                    if "log" in c.args[0] and "--reverse" in c.args[0])
    assert log_call.args[0] == [
        "git", "-C", "/repo",
        "log", "--reverse", "base..trunk",
        "--max-count", "10", "--format=%H",
    ]
    assert commits == ["sha1", "sha2", "sha3"]


def test_next_commits_handles_empty_output():
    with patch.object(git_ops.subprocess, "run",
                      side_effect=_next_commits_router("")):
        assert git_ops.next_commits("/repo", "base", "trunk") == []


def test_next_commits_deepens_shallow_clone_until_since_appears():
    """Semaphore's `--depth=1` submodule init means the cursor commit is
    almost always missing locally. next_commits must `fetch --deepen`
    until the cursor appears, then run the log."""
    state = {"deepen_rounds": 0, "commit_present_after": 2}

    def router(args, **_kwargs):
        if "cat-file" in args:
            # Commit becomes present after N deepen rounds.
            rc = 0 if state["deepen_rounds"] >= state["commit_present_after"] else 1
            return _completed(rc)
        if "rev-parse" in args and "--is-shallow-repository" in args:
            # Repo is still shallow during the deepen loop.
            return _completed(0, "true\n")
        if "fetch" in args and any(a.startswith("--deepen=") for a in args):
            state["deepen_rounds"] += 1
            return _completed(0)
        if "log" in args and "--reverse" in args:
            return _completed(0, "newsha\n")
        raise AssertionError(f"unexpected git call: {args}")

    with patch.object(git_ops.subprocess, "run", side_effect=router) as mrun:
        commits = git_ops.next_commits("/repo", "base", "trunk", n=5)

    assert commits == ["newsha"]
    assert state["deepen_rounds"] == 2
    # Verify the deepen invocation shape: fetch --deepen=50 origin trunk
    deepens = [c.args[0] for c in mrun.call_args_list
               if "fetch" in c.args[0] and any(a.startswith("--deepen=") for a in c.args[0])]
    assert deepens, "expected at least one deepen fetch"
    assert deepens[0] == [
        "git", "-C", "/repo", "fetch", "--deepen=50", "origin", "trunk",
    ]


def test_next_commits_raises_when_full_clone_lacks_since():
    """If the repo is NOT shallow and the cursor isn't there, no amount
    of deepening will help -- raise immediately rather than loop."""
    def router(args, **_kwargs):
        if "cat-file" in args:
            return _completed(1)  # commit absent
        if "rev-parse" in args and "--is-shallow-repository" in args:
            return _completed(0, "false\n")  # full clone
        raise AssertionError(f"unexpected git call: {args}")

    with patch.object(git_ops.subprocess, "run", side_effect=router):
        with pytest.raises(git_ops.GitError, match="not shallow"):
            git_ops.next_commits("/repo", "base", "trunk")


def test_next_commits_raises_when_branch_fully_unshallowed_without_since():
    """If a deepen turns the repo into a full clone but the cursor still
    isn't there, the cursor isn't on this branch -- raise."""
    def router(args, **_kwargs):
        if "cat-file" in args:
            return _completed(1)  # always absent
        if "rev-parse" in args and "--is-shallow-repository" in args:
            # First check (before deepen): shallow. After the first
            # deepen, the repo is fully unshallowed.
            if router.deepen_rounds == 0:
                return _completed(0, "true\n")
            return _completed(0, "false\n")
        if "fetch" in args:
            router.deepen_rounds += 1
            return _completed(0)
        raise AssertionError(f"unexpected git call: {args}")
    router.deepen_rounds = 0

    with patch.object(git_ops.subprocess, "run", side_effect=router):
        with pytest.raises(git_ops.GitError, match="not on that branch"):
            git_ops.next_commits("/repo", "base", "trunk")


def test_next_commits_raises_after_max_deepens():
    """If we exhaust max_deepens rounds while still shallow without
    finding the cursor, give up with a clear error."""
    def router(args, **_kwargs):
        if "cat-file" in args:
            return _completed(1)  # never present
        if "rev-parse" in args and "--is-shallow-repository" in args:
            return _completed(0, "true\n")  # always shallow
        if "fetch" in args:
            return _completed(0)
        raise AssertionError(f"unexpected git call: {args}")

    with patch.object(git_ops.subprocess, "run", side_effect=router):
        with pytest.raises(git_ops.GitError, match="after 3 deepen rounds"):
            git_ops.next_commits(
                "/repo", "base", "trunk",
                deepen_step=10, max_deepens=3,
            )


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


def test_push_branch_uses_set_upstream_by_default():
    """The orchestrator-side push (called after R2 returns because the
    sandbox denies `git push`) defaults to `-u origin <branch>`."""
    with patch.object(git_ops.subprocess, "run", return_value=_completed(0)) as mrun:
        git_ops.push_branch("/repo", "kafka-translate/abc")
    mrun.assert_called_once_with(
        ["git", "-C", "/repo", "push", "-u", "origin", "kafka-translate/abc"],
        capture_output=True, text=True,
    )


def test_push_branch_set_upstream_false_omits_dash_u():
    with patch.object(git_ops.subprocess, "run", return_value=_completed(0)) as mrun:
        git_ops.push_branch("/repo", "kafka-translate/abc", set_upstream=False)
    mrun.assert_called_once_with(
        ["git", "-C", "/repo", "push", "origin", "kafka-translate/abc"],
        capture_output=True, text=True,
    )
