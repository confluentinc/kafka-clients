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

    next_commits's control flow:
      1. `git fetch origin <branch>` (refresh tip; no --deepen)
      2. `git cat-file -e <since>^{commit}` (cursor present?)
      3. `git merge-base --is-ancestor <since> FETCH_HEAD`
      4. `git log --reverse <since>..FETCH_HEAD --format=%H`

    The router default-cases all of (1)-(3) to success and returns
    `log_stdout` for (4). Any other call raises so unexpected git
    invocations are caught.
    """
    def router(args, **_kwargs):
        if "fetch" in args and not any(a.startswith("--deepen=") for a in args):
            return _completed(0)  # initial fetch
        if "cat-file" in args:
            return _completed(0)  # commit present
        if "merge-base" in args and "--is-ancestor" in args:
            return _completed(0)  # is an ancestor
        if "log" in args and "--reverse" in args:
            return _completed(0, log_stdout)
        raise AssertionError(f"unexpected git call: {args}")
    return router


def test_next_commits_parses_oldest_to_newest():
    out = "sha1\nsha2\nsha3\n"
    with patch.object(git_ops.subprocess, "run",
                      side_effect=_next_commits_router(out)) as mrun:
        commits = git_ops.next_commits("/repo", since="base", branch="trunk", n=10)
    # The log call carries the range argv we want to pin: FETCH_HEAD
    # (not bare "trunk") as the upper bound, and NO --max-count
    # (which would interact with --reverse to return the N newest
    # rather than the N oldest in range).
    log_call = next(c for c in mrun.call_args_list
                    if "log" in c.args[0] and "--reverse" in c.args[0])
    assert log_call.args[0] == [
        "git", "-C", "/repo",
        "log", "--reverse", "base..FETCH_HEAD", "--format=%H",
    ]
    assert commits == ["sha1", "sha2", "sha3"]


def test_next_commits_handles_empty_output():
    with patch.object(git_ops.subprocess, "run",
                      side_effect=_next_commits_router("")):
        assert git_ops.next_commits("/repo", "base", "trunk") == []


def test_next_commits_fetches_branch_tip_before_log():
    """First action must be `git fetch origin <branch>` so FETCH_HEAD
    reflects the remote's actual current tip, not a stale shallow ref
    pinned by `git submodule update --init --depth=1`."""
    with patch.object(git_ops.subprocess, "run",
                      side_effect=_next_commits_router("")) as mrun:
        git_ops.next_commits("/repo", "base", "trunk")
    # Find the first fetch call (no --deepen) and verify shape.
    fetches = [
        c.args[0] for c in mrun.call_args_list
        if "fetch" in c.args[0]
        and not any(a.startswith("--deepen=") for a in c.args[0])
    ]
    assert fetches, "expected an initial (non-deepen) fetch"
    assert fetches[0] == ["git", "-C", "/repo", "fetch", "origin", "trunk"]


def test_next_commits_limits_in_python_returning_oldest_n():
    """Walks the full range and slices in Python: must return the N
    OLDEST commits in the range, not the N newest. (git's
    `--max-count --reverse` would return the N newest displayed
    oldest-first; we deliberately don't use that combo.)"""
    # Simulate a range with 7 commits: oldest -> newest.
    full_range_oldest_first = "\n".join(f"c{i}" for i in range(7)) + "\n"
    with patch.object(git_ops.subprocess, "run",
                      side_effect=_next_commits_router(full_range_oldest_first)):
        commits = git_ops.next_commits("/repo", "base", "trunk", n=3)
    # The first 3 of the oldest-first walk = the 3 OLDEST in range.
    assert commits == ["c0", "c1", "c2"]


def test_next_commits_raises_when_cursor_not_ancestor():
    """If the cursor isn't reachable from FETCH_HEAD (e.g. on an
    unmerged branch or pre-rewrite history), `<cursor>..FETCH_HEAD`
    silently degrades to all of FETCH_HEAD's history. The merge-base
    --is-ancestor probe must catch this and raise loudly so the
    operator re-seeds, instead of returning random commits."""
    def router(args, **_kwargs):
        if "fetch" in args and not any(a.startswith("--deepen=") for a in args):
            return _completed(0)
        if "cat-file" in args:
            return _completed(0)  # cursor IS present in DB
        if "merge-base" in args and "--is-ancestor" in args:
            return _completed(1)  # NOT an ancestor
        raise AssertionError(f"unexpected git call: {args}")

    with patch.object(git_ops.subprocess, "run", side_effect=router):
        with pytest.raises(git_ops.GitError, match="not an ancestor"):
            git_ops.next_commits("/repo", "wrongsha", "trunk")


def test_next_commits_initial_fetch_failure_propagates():
    """A failed initial fetch (network/auth) must propagate so the
    operator sees the real cause, not a downstream cursor-missing
    error."""
    def router(args, **_kwargs):
        if "fetch" in args and not any(a.startswith("--deepen=") for a in args):
            return _completed(128, "", "fatal: unable to access 'origin'")
        raise AssertionError(f"unexpected git call: {args}")

    with patch.object(git_ops.subprocess, "run", side_effect=router):
        with pytest.raises(git_ops.GitError, match="unable to access"):
            git_ops.next_commits("/repo", "base", "trunk")


def test_next_commits_deepens_shallow_clone_until_since_appears():
    """Semaphore's `--depth=1` submodule init means the cursor commit is
    almost always missing locally. next_commits must `fetch --deepen`
    until the cursor appears, then run the log."""
    state = {"deepen_rounds": 0, "commit_present_after": 2}

    def router(args, **_kwargs):
        if "fetch" in args and not any(a.startswith("--deepen=") for a in args):
            return _completed(0)  # initial (non-deepen) fetch
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
        if "merge-base" in args and "--is-ancestor" in args:
            return _completed(0)  # cursor is an ancestor
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
        if "fetch" in args and not any(a.startswith("--deepen=") for a in args):
            return _completed(0)  # initial fetch
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
        if "fetch" in args and not any(a.startswith("--deepen=") for a in args):
            return _completed(0)  # initial fetch
        if "cat-file" in args:
            return _completed(1)  # always absent
        if "rev-parse" in args and "--is-shallow-repository" in args:
            # First check (before deepen): shallow. After the first
            # deepen, the repo is fully unshallowed.
            if router.deepen_rounds == 0:
                return _completed(0, "true\n")
            return _completed(0, "false\n")
        if "fetch" in args and any(a.startswith("--deepen=") for a in args):
            router.deepen_rounds += 1
            return _completed(0)
        raise AssertionError(f"unexpected git call: {args}")
    router.deepen_rounds = 0

    with patch.object(git_ops.subprocess, "run", side_effect=router):
        with pytest.raises(git_ops.GitError, match="not on that branch"):
            git_ops.next_commits("/repo", "base", "trunk")


def test_next_commits_retries_deepen_on_shallow_file_race():
    """Git's `fatal: shallow file has changed since we read it` is a
    transient race. The deepen helper must retry it inside the same
    round, not bubble it up as a hard failure."""
    state = {"successful_deepen_fetches": 0, "deepen_attempts": 0}

    def router(args, **_kwargs):
        if "fetch" in args and not any(a.startswith("--deepen=") for a in args):
            return _completed(0)  # initial fetch always succeeds
        if "cat-file" in args:
            # Commit becomes present only after a deepen fetch has
            # actually succeeded (failed attempts don't count).
            return _completed(0) if state["successful_deepen_fetches"] >= 1 else _completed(1)
        if "rev-parse" in args and "--is-shallow-repository" in args:
            return _completed(0, "true\n")
        if "fetch" in args and any(a.startswith("--deepen=") for a in args):
            state["deepen_attempts"] += 1
            # First deepen attempt hits the shallow-race; second succeeds.
            if state["deepen_attempts"] == 1:
                return _completed(
                    128, "",
                    "fatal: shallow file has changed since we read it",
                )
            state["successful_deepen_fetches"] += 1
            return _completed(0)
        if "merge-base" in args and "--is-ancestor" in args:
            return _completed(0)
        if "log" in args and "--reverse" in args:
            return _completed(0, "newsha\n")
        raise AssertionError(f"unexpected git call: {args}")

    # Patch the backoff to 0 so the test stays fast.
    with patch.object(git_ops, "_DEEPEN_RETRY_BACKOFF_SECONDS", 0), \
         patch.object(git_ops.subprocess, "run", side_effect=router) as mrun:
        commits = git_ops.next_commits("/repo", "base", "trunk", n=5)

    assert commits == ["newsha"]
    # Total 3 fetches: 1 initial + 1 failed deepen + 1 deepen retry.
    fetch_calls = [c for c in mrun.call_args_list if "fetch" in c.args[0]]
    assert len(fetch_calls) == 3, fetch_calls
    deepen_calls = [
        c for c in fetch_calls
        if any(a.startswith("--deepen=") for a in c.args[0])
    ]
    assert len(deepen_calls) == 2, "expected 1 failed deepen + 1 retry"


def test_next_commits_does_not_retry_non_shallow_race_fetch_errors():
    """Network/auth/other fetch failures must propagate immediately --
    we only retry the specific shallow-file-changed race. Failure on
    the INITIAL fetch (network/auth) propagates directly without ever
    reaching the deepen logic."""
    def router(args, **_kwargs):
        if "fetch" in args:
            # Both initial and any deepen fetch fail with a non-race
            # error (e.g. unreachable remote).
            return _completed(128, "", "fatal: unable to access 'origin'")
        raise AssertionError(f"unexpected git call: {args}")

    with patch.object(git_ops, "_DEEPEN_RETRY_BACKOFF_SECONDS", 0), \
         patch.object(git_ops.subprocess, "run", side_effect=router) as mrun:
        with pytest.raises(git_ops.GitError, match="unable to access"):
            git_ops.next_commits("/repo", "base", "trunk")
    # The initial fetch fails first; we never get to the deepen loop.
    fetch_calls = [c for c in mrun.call_args_list if "fetch" in c.args[0]]
    assert len(fetch_calls) == 1, fetch_calls


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
