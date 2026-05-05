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

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import pytest

from translation_agent import cli, db


@pytest.fixture(autouse=True)
def _patch_worktree(monkeypatch):
    """Replace the real worktree context manager with a no-op for cli tests.

    The plan/impl flow wraps `r2 sandbox claude` in `worktree.worktree_for_branch`
    which would try real `git worktree add` against `args.rust_repo_path` and
    fail (no such repo, no such branch). Since we already mock streaming for
    every cli test, the worktree itself is incidental -- short-circuit it.
    """
    @contextmanager
    def fake_wt(repo_path, branch_name, *, cleanup=True,
                base_remote_branch=None, ak_commit=None, ak_branch="trunk"):
        yield Path("/fake/worktree") / branch_name.replace("/", "_")

    monkeypatch.setattr(
        "translation_agent.cli.worktree.worktree_for_branch", fake_wt
    )


@pytest.fixture(autouse=True)
def _patch_r2_default_absent(monkeypatch):
    """Default `_r2_available()` to False for all CLI tests.

    Without this, tests that don't explicitly mock streaming would
    invoke real `r2 sandbox claude` if the developer happened to have
    `dev-bin/` on PATH (which the README recommends). Tests that need
    r2 available re-patch this to True.
    """
    monkeypatch.setattr("translation_agent.cli._r2_available", lambda: False)


@pytest.fixture(autouse=True)
def _patch_streaming_default_fnf(monkeypatch):
    """Default streaming.run_with_prefix to raise FileNotFoundError so
    no test accidentally invokes real r2 even in non-dry-run paths
    (which don't gate on _r2_available). Tests that exercise the
    streaming path patch this explicitly with their own behavior.
    """
    def fake(*args, **kwargs):
        raise FileNotFoundError("r2 not on PATH (test default)")
    monkeypatch.setattr(
        "translation_agent.cli.streaming.run_with_prefix", fake
    )


def _run(*argv, db_path=None):
    if db_path:
        argv = ["--db-path", db_path] + list(argv)
    return cli.main(list(argv))


def test_seed_creates_branch_commit_row(tmp_path):
    db_path = str(tmp_path / "t.db")
    rc = _run(
        "--seed",
        "--ak-branch", "trunk", "--ak-commit", "abc",
        "--rust-branch", "master",
        db_path=db_path,
    )
    assert rc == 0
    conn = db.connect(db_path)
    assert conn.execute("SELECT count(*) FROM branch_commit").fetchone()[0] == 1


def test_seed_idempotent(tmp_path):
    db_path = str(tmp_path / "t.db")
    args = [
        "--seed", "--ak-branch", "trunk", "--ak-commit", "abc",
        "--rust-branch", "master",
    ]
    assert _run(*args, db_path=db_path) == 0
    assert _run(*args, db_path=db_path) == 0
    conn = db.connect(db_path)
    assert conn.execute("SELECT count(*) FROM branch_commit").fetchone()[0] == 1


def test_seed_missing_args_returns_2(tmp_path):
    db_path = str(tmp_path / "t.db")
    rc = _run("--seed", "--ak-branch", "trunk", db_path=db_path)
    assert rc == 2


def test_seed_with_cleanup_prs_deletes_branch_rows_and_seeds_cursor(tmp_path):
    """`--seed --cleanup-prs` clears the supplied rust_branch's pr_commit
    rows BEFORE seeding the cursor, leaves other branches untouched, and
    still returns rc=0 after a successful seed."""
    db_path = str(tmp_path / "t.db")
    # Pre-populate stale rows on two branches.
    conn = db.connect(db_path)
    db.migrate(conn)
    db.insert_pr_commit(conn, 1, "master",      "trunk", "ak1")
    db.insert_pr_commit(conn, 2, "master",      "trunk", "ak2")
    db.insert_pr_commit(conn, 3, "dev/feature", "trunk", "ak3")
    conn.commit()
    conn.close()

    rc = _run(
        "--seed", "--cleanup-prs",
        "--ak-branch", "trunk", "--ak-commit", "akseed",
        "--rust-branch", "master",
        db_path=db_path,
    )
    assert rc == 0

    conn = db.connect(db_path)
    # master's PRs were wiped; dev/feature's row survived.
    rows = [dict(r) for r in
            conn.execute("SELECT * FROM pr_commit ORDER BY pr_number").fetchall()]
    assert len(rows) == 1
    assert rows[0]["pr_number"] == 3
    assert rows[0]["rust_branch"] == "dev/feature"
    # The seed itself still ran -- branch_commit has the master cursor.
    bc = db.get_latest_correspondence(conn, "master")
    assert bc is not None
    assert bc["ak_commit"] == "akseed"


def test_seed_cleanup_prs_no_rows_is_noop(tmp_path):
    """When the branch has no pr_commit rows, --cleanup-prs is a clean
    no-op (still returns 0, still seeds)."""
    db_path = str(tmp_path / "t.db")
    rc = _run(
        "--seed", "--cleanup-prs",
        "--ak-branch", "trunk", "--ak-commit", "abc",
        "--rust-branch", "master",
        db_path=db_path,
    )
    assert rc == 0
    conn = db.connect(db_path)
    assert conn.execute("SELECT count(*) FROM pr_commit").fetchone()[0] == 0
    assert db.get_latest_correspondence(conn, "master") is not None


def test_pr_mode_missing_returns_0_for_status_check(tmp_path):
    """`--pr <N>` (no --plan-approve) is the auto-triggered Semaphore
    status check that runs on every PR build. Most PRs in this repo
    aren't translation PRs managed by the orchestrator, so a missing
    row is the NORMAL case -- return 0 (green CI), not 1 (red CI)."""
    db_path = str(tmp_path / "t.db")
    rc = _run("--pr", "42", db_path=db_path)
    assert rc == 0


def test_pr_mode_missing_with_plan_approve_returns_1(tmp_path):
    """`--pr <N> --plan-approve` is a deliberate manual promotion;
    approving a plan for a PR the orchestrator doesn't know about is
    an operator error and must fail loudly."""
    db_path = str(tmp_path / "t.db")
    rc = _run("--pr", "42", "--plan-approve", db_path=db_path)
    assert rc == 1


def _insert_pr_at_status(db_path, pr_number, ak_commit, status,
                         rust_branch="master", ak_branch="trunk"):
    conn = db.connect(db_path)
    db.migrate(conn)
    db.insert_pr_commit(conn, pr_number, rust_branch, ak_branch, ak_commit)
    conn.execute("UPDATE pr_commit SET status = ? WHERE pr_number = ?",
                 (status, pr_number))
    conn.commit()
    conn.close()


def test_pr_status_check_prints_row(tmp_path, capsys):
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc123", db.STATUS_PLAN_CREATED)
    rc = _run("--pr", "42", db_path=db_path)
    assert rc == 0
    captured = capsys.readouterr()
    assert "pr_number: 42" in captured.out
    assert f"status: {db.STATUS_PLAN_CREATED}" in captured.out


def test_pr_plan_approve_transitions_2_to_3_and_runs_impl(tmp_path):
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)
    with patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "")), \
         patch("translation_agent.cli.git_ops.push_branch"), \
         patch("translation_agent.cli.git_ops.rev_parse",
               return_value="rust_new_sha"):
        rc = _run("--pr", "42", "--plan-approve", db_path=db_path)
    assert rc == 0
    conn = db.connect(db_path)
    pr = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 42").fetchone())
    assert pr["status"] == db.STATUS_IMPLEMENTATION_DONE
    bc = dict(conn.execute(
        "SELECT * FROM branch_commit WHERE rust_branch = 'master'"
    ).fetchone())
    assert bc["ak_commit"] == "abc"
    assert bc["ak_branch"] == "trunk"


def test_pr_plan_approve_dry_run_no_r2_only_flips_status(tmp_path):
    """Without r2: --plan-approve --dry-run flips 2 -> 3 in DB but does
    not invoke claude for impl."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)
    with patch("translation_agent.cli.streaming.run_with_prefix") as mstream, \
         patch("translation_agent.cli._r2_available", return_value=False):
        rc = _run("--pr", "42", "--plan-approve", "--dry-run", db_path=db_path)
    assert rc == 0
    mstream.assert_not_called()
    conn = db.connect(db_path)
    assert conn.execute(
        "SELECT status FROM pr_commit WHERE pr_number = 42"
    ).fetchone()[0] == db.STATUS_PLAN_APPROVED


def test_pr_plan_approve_dry_run_r2_present_runs_impl_advances_status_locally(tmp_path):
    """With r2: --plan-approve --dry-run flips 2 -> 3, runs impl claude
    in a preserved worktree, then advances to 4 + inserts branch_commit
    -- all locally. Real `git push` and Semaphore artifact push are still
    skipped, so the side effect doesn't escape the local DB."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)
    with patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "ok")) as mstream, \
         patch("translation_agent.cli._r2_available", return_value=True), \
         patch("translation_agent.cli.git_ops.rev_parse",
               return_value="local_dry_run_sha"):
        rc = _run("--pr", "42", "--plan-approve", "--dry-run", db_path=db_path)
    assert rc == 0
    mstream.assert_called_once()  # impl r2 ran
    conn = db.connect(db_path)
    pr = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 42").fetchone())
    assert pr["status"] == db.STATUS_IMPLEMENTATION_DONE
    # branch_commit cursor advanced to the impl's ak_commit.
    bc = dict(conn.execute(
        "SELECT * FROM branch_commit WHERE rust_branch = 'master'"
    ).fetchone())
    assert bc["ak_commit"] == "abc"


def test_pr_plan_approve_impl_failure_persists_last_error(tmp_path):
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)
    with patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(2, "boom")):
        rc = _run("--pr", "42", "--plan-approve", db_path=db_path)
    assert rc == 1
    conn = db.connect(db_path)
    pr = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 42").fetchone())
    # Status remains 3 (plan_approved) so a re-run can retry.
    assert pr["status"] == db.STATUS_PLAN_APPROVED
    assert "rc=2" in pr["last_error"]


def test_pr_plan_approve_wrong_status_returns_1(tmp_path):
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_NO_PLAN)
    rc = _run("--pr", "42", "--plan-approve", db_path=db_path)
    assert rc == 1


def test_mutually_exclusive_seed_and_pr():
    with pytest.raises(SystemExit):
        cli.main(["--seed", "--pr", "1"])


# --- artifact push wiring (Phase E) -----------------------------------------

def test_seed_pushes_artifact(tmp_path):
    db_path = str(tmp_path / "t.db")
    with patch("translation_agent.cli.semaphore.push_project_artifact") as mpush:
        rc = _run("--seed", "--ak-branch", "trunk", "--ak-commit", "a",
                  "--rust-branch", "master",
                  db_path=db_path)
    assert rc == 0
    mpush.assert_called_once_with("translation_agent.db", db_path)


def test_no_artifact_push_skips(tmp_path):
    db_path = str(tmp_path / "t.db")
    with patch("translation_agent.cli.semaphore.push_project_artifact") as mpush:
        rc = _run("--no-artifact-push",
                  "--seed", "--ak-branch", "trunk", "--ak-commit", "a",
                  "--rust-branch", "master",
                  db_path=db_path)
    assert rc == 0
    mpush.assert_not_called()


def test_pr_status_check_does_not_push(tmp_path):
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)
    with patch("translation_agent.cli.semaphore.push_project_artifact") as mpush:
        rc = _run("--pr", "42", db_path=db_path)
    assert rc == 0
    # --pr <N> alone is read-only; no need to push.
    mpush.assert_not_called()


def test_dry_run_does_not_push(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    with patch("translation_agent.cli.git_ops.next_commits", return_value=[]), \
         patch("translation_agent.cli.semaphore.push_project_artifact") as mpush:
        rc = _run("--dry-run",
                  "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
                  "--rust-branch", "master",
                  db_path=db_path)
    assert rc == 0
    mpush.assert_not_called()


def test_artifact_push_failure_does_not_crash(tmp_path):
    db_path = str(tmp_path / "t.db")
    with patch("translation_agent.cli.semaphore.push_project_artifact",
               side_effect=Exception("artifact server down")):
        rc = _run("--seed", "--ak-branch", "trunk", "--ak-commit", "a",
                  "--rust-branch", "master",
                  db_path=db_path)
    # Seed succeeded; artifact push failed but logged. RC reflects the seed.
    assert rc == 0


def test_artifact_push_runs_even_when_sweep_fails(tmp_path):
    """If the sweep itself returns non-zero, we still push so the partial
    state is captured."""
    db_path = str(tmp_path / "t.db")
    # No seed -> sweep returns 1.
    with patch("translation_agent.cli.semaphore.push_project_artifact") as mpush:
        rc = _run("--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
                  "--rust-branch", "master",
                  db_path=db_path)
    assert rc == 1
    mpush.assert_called_once()


# --- end-to-end integration -------------------------------------------------

def test_end_to_end_full_lifecycle(tmp_path):
    """One sweep + one --plan-approve drives a row through 0 -> 1 -> 2 -> 3 -> 4."""
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path, ak_commit="ak_seed")

    # Sweep run: creates PR for ak_a, dep-evals, plans it. Stops at status 2
    # (no auto plan-approve).
    dep_eval_json = '{"plan_dependency": null, "implementation_dependency": null}'
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.git_ops.push_branch"), \
         patch("translation_agent.cli.github.create_draft_pr",
               side_effect=[100]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=[(0, dep_eval_json), (0, "")]), \
         patch("translation_agent.cli.semaphore.push_project_artifact") as mpush_sweep:
        rc = _run("--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
                  "--rust-branch", "master",
                  db_path=db_path)
    assert rc == 0
    mpush_sweep.assert_called_once()
    conn = db.connect(db_path)
    pr = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 100").fetchone())
    assert pr["status"] == db.STATUS_PLAN_CREATED
    assert pr["ak_branch"] == "trunk"
    conn.close()

    # Plan-approve run: 2 -> 3 -> 4, branch_commit updated.
    with patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "")), \
         patch("translation_agent.cli.git_ops.push_branch"), \
         patch("translation_agent.cli.git_ops.rev_parse",
               return_value="rust_a_sha"), \
         patch("translation_agent.cli.semaphore.push_project_artifact") as mpush_appr:
        rc = _run("--pr", "100", "--plan-approve", db_path=db_path)
    assert rc == 0
    mpush_appr.assert_called_once()

    conn = db.connect(db_path)
    pr = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 100").fetchone())
    assert pr["status"] == db.STATUS_IMPLEMENTATION_DONE
    bc = db.get_latest_correspondence(conn, "master")
    assert bc["ak_commit"] == "ak_a"


def test_help_runs():
    with pytest.raises(SystemExit) as exc:
        cli.main(["--help"])
    assert exc.value.code == 0


# --- sweep mode -------------------------------------------------------------

def _seed_db(db_path, ak_commit="ak0"):
    conn = db.connect(db_path)
    db.migrate(conn)
    db.seed_correspondence(conn, "trunk", ak_commit, "master")
    conn.close()


def test_sweep_missing_args_returns_2(tmp_path):
    db_path = str(tmp_path / "t.db")
    rc = _run("--ak-branch", "trunk", db_path=db_path)
    assert rc == 2


def test_sweep_no_seed_returns_1(tmp_path):
    db_path = str(tmp_path / "t.db")
    rc = _run(
        "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
        "--rust-branch", "master",
        db_path=db_path,
    )
    assert rc == 1


def test_sweep_no_new_commits_returns_0(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    with patch("translation_agent.cli.git_ops.next_commits", return_value=[]):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0


def test_sweep_creates_prs_for_new_commits(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a", "ak_b"]), \
         patch("translation_agent.cli.git_ops.commit_subject",
               side_effect=lambda repo, c: f"subj for {c}"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump") as mpush, \
         patch("translation_agent.cli.github.create_draft_pr",
               side_effect=[101, 102]) as mcreate:
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    assert mpush.call_count == 2
    assert mcreate.call_count == 2

    conn = db.connect(db_path)
    rows = {r["pr_number"]: dict(r) for r in
            conn.execute("SELECT * FROM pr_commit ORDER BY pr_number").fetchall()}
    assert set(rows) == {101, 102}
    assert rows[101]["ak_commit"] == "ak_a"
    assert rows[102]["ak_commit"] == "ak_b"
    assert rows[101]["status"] == db.STATUS_NO_PLAN


def test_sweep_dry_run_skips_remote_but_inserts_synthetic_row(tmp_path):
    """Dry-run skips git push and gh-pr-create but DOES insert a pr_commit
    row with a negative synthetic pr_number, so subsequent dry-run dep-eval
    has something to read."""
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="subj"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump") as mpush, \
         patch("translation_agent.cli.github.create_draft_pr") as mcreate, \
         patch("translation_agent.cli._r2_available", return_value=False):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master", "--dry-run",
            db_path=db_path,
        )
    assert rc == 0
    mpush.assert_not_called()
    mcreate.assert_not_called()
    conn = db.connect(db_path)
    rows = conn.execute("SELECT * FROM pr_commit").fetchall()
    assert len(rows) == 1
    assert rows[0]["pr_number"] < 0  # synthetic
    assert rows[0]["ak_commit"] == "ak_a"
    assert rows[0]["status"] == db.STATUS_NO_PLAN


def test_sweep_dry_run_synthetic_pr_number_is_idempotent(tmp_path):
    """Re-running dry-run on the same AK commit hits the same synthetic
    pr_number (no duplicate rows)."""
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    common = dict(side_effects={})
    args = dict(db_path=db_path)
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="subj"), \
         patch("translation_agent.cli._r2_available", return_value=False):
        _run("--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
             "--rust-branch", "master", "--dry-run", **args)
        _run("--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
             "--rust-branch", "master", "--dry-run", **args)
    conn = db.connect(db_path)
    assert conn.execute("SELECT count(*) FROM pr_commit").fetchone()[0] == 1


def test_synthetic_pr_number_is_deterministic_and_negative():
    n1 = cli._synthetic_pr_number("abc")
    n2 = cli._synthetic_pr_number("abc")
    assert n1 == n2 < 0
    assert cli._synthetic_pr_number("abc") != cli._synthetic_pr_number("def")


# --- _update_pr_description_via_r2 -----------------------------------------
#
# These tests opt out of the autouse no-op patch by depending on the
# `real_pr_description` fixture (defined in conftest.py).

def _desc_args(dry_run=False):
    """Minimal namespace for _update_pr_description_via_r2 tests."""
    from types import SimpleNamespace
    return SimpleNamespace(
        rust_repo_path=".",
        rust_branch="dev/milestone-7",
        dry_run=dry_run,
    )


def _desc_row(pr_number=42, ak_commit="abc123"):
    return {"pr_number": pr_number, "ak_commit": ak_commit, "ak_branch": "trunk"}


def test_update_pr_description_writes_pr_body_and_publishes(tmp_path, real_pr_description):
    """Happy path: r2 produces ./pr_body.md, helper reads it, calls
    github.update_pr_body with the captured body. Returns None."""
    wt = tmp_path / "wt"
    wt.mkdir()

    def fake_streaming(cmd, pr_number, cwd):
        # Simulate claude writing the body to ./pr_body.md inside the worktree.
        Path(cwd, "pr_body.md").write_text("## Plan summary\n\nThe plan covers X.")
        return 0, ""

    with patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=fake_streaming), \
         patch("translation_agent.cli.github.update_pr_body") as mupd:
        err = cli._update_pr_description_via_r2(
            _desc_args(), _desc_row(), "plan", wt,
        )
    assert err is None
    mupd.assert_called_once_with(".", 42, "## Plan summary\n\nThe plan covers X.")


def test_update_pr_description_noop_for_synthetic_pr_number(tmp_path, real_pr_description):
    """Negative pr_numbers come from dry-run synthetic ids; no real PR
    exists to edit, so the helper returns None without invoking r2 or gh."""
    wt = tmp_path / "wt"
    wt.mkdir()
    with patch("translation_agent.cli.streaming.run_with_prefix") as mstream, \
         patch("translation_agent.cli.github.update_pr_body") as mupd:
        err = cli._update_pr_description_via_r2(
            _desc_args(), _desc_row(pr_number=-12345), "plan", wt,
        )
    assert err is None
    mstream.assert_not_called()
    mupd.assert_not_called()


def test_update_pr_description_noop_in_dry_run(tmp_path, real_pr_description):
    """Dry-run never makes remote writes. Helper logs intent and returns."""
    wt = tmp_path / "wt"
    wt.mkdir()
    with patch("translation_agent.cli.streaming.run_with_prefix") as mstream, \
         patch("translation_agent.cli.github.update_pr_body") as mupd:
        err = cli._update_pr_description_via_r2(
            _desc_args(dry_run=True), _desc_row(), "impl", wt,
        )
    assert err is None
    mstream.assert_not_called()
    mupd.assert_not_called()


def test_update_pr_description_returns_error_when_pr_body_md_missing(tmp_path, real_pr_description):
    """If r2 returns 0 but didn't produce ./pr_body.md, the helper
    surfaces a clear error instead of silently calling gh with empty body."""
    wt = tmp_path / "wt"
    wt.mkdir()
    with patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "")), \
         patch("translation_agent.cli.github.update_pr_body") as mupd:
        err = cli._update_pr_description_via_r2(
            _desc_args(), _desc_row(), "plan", wt,
        )
    assert err is not None
    assert "did not produce" in err
    mupd.assert_not_called()


def test_update_pr_description_returns_error_on_r2_nonzero_exit(tmp_path, real_pr_description):
    wt = tmp_path / "wt"
    wt.mkdir()
    with patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(1, "")), \
         patch("translation_agent.cli.github.update_pr_body") as mupd:
        err = cli._update_pr_description_via_r2(
            _desc_args(), _desc_row(), "plan", wt,
        )
    assert err is not None
    assert "rc=1" in err
    mupd.assert_not_called()


def test_update_pr_description_propagates_gh_error(tmp_path, real_pr_description):
    """A failure from `gh pr edit` is surfaced as the returned error
    string -- the orchestrator logs it as a warning, not a hard failure."""
    from translation_agent import github as gh
    wt = tmp_path / "wt"
    wt.mkdir()

    def fake_streaming(cmd, pr_number, cwd):
        Path(cwd, "pr_body.md").write_text("body")
        return 0, ""

    with patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=fake_streaming), \
         patch("translation_agent.cli.github.update_pr_body",
               side_effect=gh.GhError("not authorized")):
        err = cli._update_pr_description_via_r2(
            _desc_args(), _desc_row(), "plan", wt,
        )
    assert err is not None
    assert "not authorized" in err


def test_update_pr_description_plan_phase_with_marker_applies_label(
    tmp_path, real_pr_description,
):
    """When phase='plan' and the body claude wrote ends with the
    IMPLEMENTATION_NEEDED_MARKER, the helper applies the
    LABEL_IMPLEMENTATION_NEEDED via github.add_pr_label after a
    successful update_pr_body."""
    from translation_agent import prompts
    wt = tmp_path / "wt"
    wt.mkdir()

    body = (
        "## Plan summary\n\nThe plan covers X.\n\n"
        + prompts.IMPLEMENTATION_NEEDED_MARKER + "\n"
    )

    def fake_streaming(cmd, pr_number, cwd):
        Path(cwd, "pr_body.md").write_text(body)
        return 0, ""

    with patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=fake_streaming), \
         patch("translation_agent.cli.github.update_pr_body") as mupd, \
         patch("translation_agent.cli.github.add_pr_label") as mlabel:
        err = cli._update_pr_description_via_r2(
            _desc_args(), _desc_row(), "plan", wt,
        )
    assert err is None
    mupd.assert_called_once_with(".", 42, body)
    mlabel.assert_called_once_with(
        ".", 42, prompts.LABEL_IMPLEMENTATION_NEEDED,
    )


def test_update_pr_description_plan_phase_without_marker_skips_label(
    tmp_path, real_pr_description,
):
    """If claude omitted the marker (e.g. the prompt failed to produce
    the trailer), no label is applied."""
    wt = tmp_path / "wt"
    wt.mkdir()

    def fake_streaming(cmd, pr_number, cwd):
        Path(cwd, "pr_body.md").write_text("## Summary\n\nNo marker here.")
        return 0, ""

    with patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=fake_streaming), \
         patch("translation_agent.cli.github.update_pr_body"), \
         patch("translation_agent.cli.github.add_pr_label") as mlabel:
        err = cli._update_pr_description_via_r2(
            _desc_args(), _desc_row(), "plan", wt,
        )
    assert err is None
    mlabel.assert_not_called()


def test_update_pr_description_impl_phase_does_not_apply_label_even_if_marker_present(
    tmp_path, real_pr_description,
):
    """The label-add is scoped to phase='plan'. If an impl-phase body
    happens to contain the marker (stale or copy-paste), don't apply
    the label -- impl is no longer 'needed'."""
    from translation_agent import prompts
    wt = tmp_path / "wt"
    wt.mkdir()

    def fake_streaming(cmd, pr_number, cwd):
        Path(cwd, "pr_body.md").write_text(
            "## Impl summary\n\n" + prompts.IMPLEMENTATION_NEEDED_MARKER,
        )
        return 0, ""

    with patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=fake_streaming), \
         patch("translation_agent.cli.github.update_pr_body"), \
         patch("translation_agent.cli.github.add_pr_label") as mlabel:
        err = cli._update_pr_description_via_r2(
            _desc_args(), _desc_row(), "impl", wt,
        )
    assert err is None
    mlabel.assert_not_called()


def test_update_pr_description_label_failure_logged_not_returned(
    tmp_path, real_pr_description, caplog,
):
    """A label-add failure is cosmetic: warn and continue, do NOT
    surface it as an error to the caller (the description was
    successfully published)."""
    import logging
    from translation_agent import github as gh, prompts
    wt = tmp_path / "wt"
    wt.mkdir()

    def fake_streaming(cmd, pr_number, cwd):
        Path(cwd, "pr_body.md").write_text(
            "body\n" + prompts.IMPLEMENTATION_NEEDED_MARKER,
        )
        return 0, ""

    with patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=fake_streaming), \
         patch("translation_agent.cli.github.update_pr_body"), \
         patch("translation_agent.cli.github.add_pr_label",
               side_effect=gh.GhError("label not found")):
        with caplog.at_level(logging.WARNING, logger="translation_agent.cli"):
            err = cli._update_pr_description_via_r2(
                _desc_args(), _desc_row(), "plan", wt,
            )
    assert err is None
    assert any(
        "implementation-needed" in r.message and r.levelno == logging.WARNING
        for r in caplog.records
    )


def test_update_pr_description_plan_phase_with_no_op_marker_skips_label_and_removes_existing(
    tmp_path, real_pr_description,
):
    """When the plan-phase body claude wrote contains the
    NO_IMPLEMENTATION_NEEDED_MARKER, the helper must NOT call
    add_pr_label, and must call remove_pr_label so any
    implementation-needed label set by a previous body is cleared
    on re-plan."""
    from translation_agent import prompts
    wt = tmp_path / "wt"
    wt.mkdir()

    body = (
        "## Plan summary\n\nNo Rust changes needed.\n\n"
        + prompts.NO_IMPLEMENTATION_NEEDED_MARKER + "\n"
    )

    def fake_streaming(cmd, pr_number, cwd):
        Path(cwd, "pr_body.md").write_text(body)
        return 0, ""

    with patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=fake_streaming), \
         patch("translation_agent.cli.github.update_pr_body") as mupd, \
         patch("translation_agent.cli.github.add_pr_label") as madd, \
         patch("translation_agent.cli.github.remove_pr_label") as mrm:
        err = cli._update_pr_description_via_r2(
            _desc_args(), _desc_row(), "plan", wt,
        )
    assert err is None
    mupd.assert_called_once_with(".", 42, body)
    madd.assert_not_called()
    mrm.assert_called_once_with(
        ".", 42, prompts.LABEL_IMPLEMENTATION_NEEDED,
    )


def test_update_pr_description_plan_phase_with_both_markers_treats_as_no_op(
    tmp_path, real_pr_description,
):
    """Defensive: if claude somehow emits both markers, the no-op
    branch wins. Triggering an implementation run on a body whose
    author hedged is the worse failure mode."""
    from translation_agent import prompts
    wt = tmp_path / "wt"
    wt.mkdir()

    body = (
        "## Plan summary\n\nMixed signals.\n\n"
        + prompts.IMPLEMENTATION_NEEDED_MARKER + "\n\n"
        + prompts.NO_IMPLEMENTATION_NEEDED_MARKER + "\n"
    )

    def fake_streaming(cmd, pr_number, cwd):
        Path(cwd, "pr_body.md").write_text(body)
        return 0, ""

    with patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=fake_streaming), \
         patch("translation_agent.cli.github.update_pr_body"), \
         patch("translation_agent.cli.github.add_pr_label") as madd, \
         patch("translation_agent.cli.github.remove_pr_label") as mrm:
        err = cli._update_pr_description_via_r2(
            _desc_args(), _desc_row(), "plan", wt,
        )
    assert err is None
    madd.assert_not_called()
    mrm.assert_called_once_with(
        ".", 42, prompts.LABEL_IMPLEMENTATION_NEEDED,
    )


def test_update_pr_description_plan_phase_no_op_remove_label_failure_logged_not_returned(
    tmp_path, real_pr_description, caplog,
):
    """Mirror of the add-label cosmetic-failure test for the no-op
    branch: a remove_pr_label failure must be logged at WARNING and
    must NOT be returned to the caller."""
    import logging
    from translation_agent import github as gh, prompts
    wt = tmp_path / "wt"
    wt.mkdir()

    def fake_streaming(cmd, pr_number, cwd):
        Path(cwd, "pr_body.md").write_text(
            "body\n" + prompts.NO_IMPLEMENTATION_NEEDED_MARKER,
        )
        return 0, ""

    with patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=fake_streaming), \
         patch("translation_agent.cli.github.update_pr_body"), \
         patch("translation_agent.cli.github.remove_pr_label",
               side_effect=gh.GhError("label not found")):
        with caplog.at_level(logging.WARNING, logger="translation_agent.cli"):
            err = cli._update_pr_description_via_r2(
                _desc_args(), _desc_row(), "plan", wt,
            )
    assert err is None
    assert any(
        "implementation-needed" in r.message and r.levelno == logging.WARNING
        for r in caplog.records
    )


# --- approval prepend in _run_pr_mode --------------------------------------

def test_plan_approve_prepends_approval_marker_before_impl(tmp_path, real_pr_description):
    """`--plan-approve` flips 2->3 then runs impl. Between those steps
    the orchestrator prepends an approval marker line via
    github.prepend_pr_body. The impl phase later overwrites the body
    via update_pr_body -- both calls should fire in order."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)

    # Override the autouse fake worktree with a real tmp dir so the
    # description helper can actually write + read pr_body.md.
    real_wt = tmp_path / "wt"
    real_wt.mkdir()

    @contextmanager
    def real_dir_wt(repo_path, branch_name, **_kwargs):
        yield real_wt

    def fake_streaming(cmd, pr_number, cwd):
        # Simulate claude writing the body inside the worktree (impl phase).
        Path(cwd, "pr_body.md").write_text("## Implementation done")
        return 0, ""

    with patch("translation_agent.cli.worktree.worktree_for_branch",
               new=real_dir_wt), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=fake_streaming), \
         patch("translation_agent.cli.git_ops.push_branch"), \
         patch("translation_agent.cli.git_ops.rev_parse",
               return_value="rust_new_sha"), \
         patch("translation_agent.cli.github.prepend_pr_body") as mprep, \
         patch("translation_agent.cli.github.update_pr_body") as mupd:
        rc = _run("--pr", "42", "--plan-approve", db_path=db_path)

    assert rc == 0
    # Approval prepend fires exactly once with a date-bearing marker.
    mprep.assert_called_once()
    args_call = mprep.call_args.args
    assert args_call[0] == "."           # rust_repo_path
    assert args_call[1] == 42            # pr_number
    assert "Plan approved" in args_call[2]
    # Impl description update also fires (after the impl r2 call).
    mupd.assert_called_once()
    assert mupd.call_args.args[1] == 42  # pr_number
    assert mupd.call_args.args[2] == "## Implementation done"


def test_plan_approve_skips_prepend_for_synthetic_pr(tmp_path, real_pr_description):
    """Synthetic dry-run PR numbers are negative; no real PR to edit."""
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, -1, "abc", db.STATUS_PLAN_CREATED)

    with patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "")), \
         patch("translation_agent.cli.git_ops.push_branch"), \
         patch("translation_agent.cli.git_ops.rev_parse", return_value="sha"), \
         patch("translation_agent.cli.github.prepend_pr_body") as mprep, \
         patch("translation_agent.cli.github.update_pr_body") as mupd:
        rc = _run("--pr", "-1", "--plan-approve", db_path=db_path)
    assert rc == 0
    mprep.assert_not_called()
    mupd.assert_not_called()


def test_next_sweep_action_for_status_covers_all_statuses():
    """Every status code defined in db must have a human-readable
    next-action string. Catches regressions when a new status is added
    without updating the per-PR log helper."""
    for status, name in db.STATUS_NAMES.items():
        action = cli._next_sweep_action_for_status(status)
        assert isinstance(action, str) and action, (
            f"status={status} ({name}) returned empty action"
        )
    # Spot-check a couple of specific strings: this is what reviewers
    # actually see in production logs and what motivated the change.
    assert "dep-eval" in cli._next_sweep_action_for_status(db.STATUS_NO_PLAN)
    assert "plan-approve" in cli._next_sweep_action_for_status(db.STATUS_PLAN_CREATED)
    assert "complete" in cli._next_sweep_action_for_status(db.STATUS_IMPLEMENTATION_DONE)


def test_sweep_logs_existing_pr_status_and_next_action(tmp_path, caplog):
    """When a sweep encounters a pr_commit row that already exists,
    the log line must surface the row's STATUS (numeric + symbolic
    name), the resolved dependency SHAs, AND a description of what
    the sweep will do next. Without this, operators see "PR #N
    already in pr_commit" and have to query sqlite to understand
    the orchestrator's plan."""
    import logging
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    # Pre-insert a row at status PLAN_CREATED with concrete deps --
    # the most common "stuck waiting" state in production.
    conn = db.connect(db_path)
    db.insert_pr_commit(conn, 99, "master", "trunk", "ak_a")
    conn.execute(
        "UPDATE pr_commit SET status = ?, plan_dependency = ?, "
        "implementation_dependency = ? WHERE pr_number = ?",
        (db.STATUS_PLAN_CREATED, "plandepsha1234", "impldepsha5678", 99),
    )
    conn.commit()
    conn.close()

    from translation_agent import github as gh
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               side_effect=gh.GhPrAlreadyExists("already exists")), \
         patch("translation_agent.cli.github.find_pr_number_for_branch",
               return_value=99), \
         patch("translation_agent.cli.github.get_pr_state",
               return_value=("OPEN", None)), \
         caplog.at_level(logging.INFO, logger="translation_agent.cli"):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    # The log must mention status=2, plan_created, the next-action
    # hint, AND the truncated dependency SHAs.
    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert "PR #99 already in pr_commit" in log_text
    assert "status=2" in log_text
    assert "plan_created" in log_text
    assert "plan-approve" in log_text
    # Deps shown with 12-char short SHAs.
    assert "plan_dep=plandepsha12" in log_text
    assert "impl_dep=impldepsha56" in log_text


def test_dep_summary_formats_short_sha_and_dash_for_none():
    """Helper that the per-PR sweep log uses to summarize a row's two
    dependencies. 12-char SHA matches the orchestrator's convention
    everywhere else; `-` for None reads cleaner than `None` in a
    key=value context."""
    row_both = {
        "plan_dependency": "abcdef0123456789",
        "implementation_dependency": "fedcba9876543210",
    }
    assert cli._dep_summary(row_both) == (
        "plan_dep=abcdef012345, impl_dep=fedcba987654"
    )
    row_neither = {
        "plan_dependency": None,
        "implementation_dependency": None,
    }
    assert cli._dep_summary(row_neither) == "plan_dep=-, impl_dep=-"
    row_only_plan = {
        "plan_dependency": "abcdef0123456789",
        "implementation_dependency": None,
    }
    assert cli._dep_summary(row_only_plan) == (
        "plan_dep=abcdef012345, impl_dep=-"
    )


def test_sweep_recovers_pr_number_on_already_exists(tmp_path):
    from translation_agent import github as gh

    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               side_effect=gh.GhPrAlreadyExists("already exists")), \
         patch("translation_agent.cli.github.find_pr_number_for_branch",
               return_value=77):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    row = conn.execute(
        "SELECT * FROM pr_commit WHERE pr_number = 77"
    ).fetchone()
    assert row is not None
    assert row["ak_commit"] == "ak_a"


# --- PR closure check (sweep prefix-prune + cursor advance) ---------------

def _seed_with_existing_prs(db_path, ak_to_pr):
    """Helper: seed branch_commit at ak_seed and pre-insert pr_commit
    rows for each (ak_commit -> pr_number) entry in `ak_to_pr`."""
    conn = db.connect(db_path)
    db.migrate(conn)
    db.seed_correspondence(conn, "trunk", "ak_seed", "master")
    for i, (ak, pr) in enumerate(ak_to_pr.items()):
        db.insert_pr_commit(conn, pr, "master", "trunk", ak)
    conn.commit()
    conn.close()


def test_sweep_closure_check_prunes_merged_prefix_and_advances_cursor(tmp_path):
    """The core happy path: ak_a/ak_b/ak_c all have PR rows, gh
    reports them MERGED. Walk should delete all three rows, advance
    cursor to ak_c with the merge SHA of PR #103, and re-fetch the
    next batch from the new cursor."""
    db_path = str(tmp_path / "t.db")
    _seed_with_existing_prs(db_path, {"ak_a": 101, "ak_b": 102, "ak_c": 103})

    # next_commits is called TWICE: first with the original cursor
    # (returns the closed batch), then again after the cursor advance
    # (returns ak_d, which is new and unprocessed).
    next_commits_mock = MagicMock(side_effect=[
        ["ak_a", "ak_b", "ak_c"],   # initial fetch
        ["ak_d"],                    # re-fetch after cursor advance
    ])
    with patch("translation_agent.cli.git_ops.next_commits", next_commits_mock), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               side_effect=[201]), \
         patch("translation_agent.cli.github.get_pr_state",
               side_effect=[
                   ("MERGED", "merge_sha_a"),
                   ("MERGED", "merge_sha_b"),
                   ("MERGED", "merge_sha_c"),
               ]):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0

    conn = db.connect(db_path)
    # All three pruned rows are gone.
    pruned = conn.execute(
        "SELECT * FROM pr_commit WHERE pr_number IN (101, 102, 103)"
    ).fetchall()
    assert len(pruned) == 0
    # Cursor advanced through the entire MERGED prefix to ak_c.
    bc = db.get_latest_correspondence(conn, "master")
    assert bc["ak_commit"] == "ak_c"
    # Each MERGED PR was archived to pr_commit_history with its merge SHA.
    history = sorted(
        (dict(r) for r in conn.execute(
            "SELECT * FROM pr_commit_history ORDER BY ak_commit"
        ).fetchall()),
        key=lambda r: r["ak_commit"],
    )
    assert history == [
        {"rust_branch": "master", "ak_branch": "trunk",
         "ak_commit": "ak_a", "rust_commit": "merge_sha_a"},
        {"rust_branch": "master", "ak_branch": "trunk",
         "ak_commit": "ak_b", "rust_commit": "merge_sha_b"},
        {"rust_branch": "master", "ak_branch": "trunk",
         "ak_commit": "ak_c", "rust_commit": "merge_sha_c"},
    ]
    # Re-fetched batch was used: ak_d's row exists with the new pr_number.
    new_row = conn.execute(
        "SELECT * FROM pr_commit WHERE pr_number = 201"
    ).fetchone()
    assert new_row is not None
    assert new_row["ak_commit"] == "ak_d"
    # next_commits was called twice (initial + re-fetch).
    assert next_commits_mock.call_count == 2


def test_sweep_closure_check_stops_at_first_open_pr(tmp_path):
    """ak_a is MERGED, ak_b is OPEN, ak_c is also MERGED but should
    NOT be pruned (we only prune the contiguous closed prefix).
    Cursor advances to ak_a only."""
    db_path = str(tmp_path / "t.db")
    _seed_with_existing_prs(db_path, {"ak_a": 101, "ak_b": 102, "ak_c": 103})

    next_commits_mock = MagicMock(side_effect=[
        ["ak_a", "ak_b", "ak_c"],
        ["ak_b", "ak_c", "ak_d"],  # re-fetch after advancing past ak_a
    ])
    with patch("translation_agent.cli.git_ops.next_commits", next_commits_mock), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               return_value=999), \
         patch("translation_agent.cli.github.get_pr_state",
               side_effect=[
                   ("MERGED", "merge_sha_a"),
                   ("OPEN", None),  # walk stops here
               ]):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0

    conn = db.connect(db_path)
    # Only ak_a's row was pruned; ak_b (OPEN) and ak_c (not yet
    # checked) remain.
    surviving_pr_numbers = sorted(
        r["pr_number"] for r in conn.execute(
            "SELECT pr_number FROM pr_commit ORDER BY pr_number"
        ).fetchall()
    )
    assert 101 not in surviving_pr_numbers
    assert 102 in surviving_pr_numbers
    assert 103 in surviving_pr_numbers
    # Cursor advanced to ak_a, NOT to ak_c.
    bc = db.get_latest_correspondence(conn, "master")
    assert bc["ak_commit"] == "ak_a"


def test_sweep_closure_check_stops_at_first_missing_row(tmp_path):
    """ak_a has a MERGED PR row, ak_b has NO row at all (it's a new
    commit). Walk advances cursor to ak_a then stops."""
    db_path = str(tmp_path / "t.db")
    _seed_with_existing_prs(db_path, {"ak_a": 101})

    next_commits_mock = MagicMock(side_effect=[
        ["ak_a", "ak_b", "ak_c"],
        ["ak_b", "ak_c", "ak_d"],
    ])
    with patch("translation_agent.cli.git_ops.next_commits", next_commits_mock), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               return_value=999), \
         patch("translation_agent.cli.github.get_pr_state",
               side_effect=[("MERGED", "merge_sha_a")]):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    bc = db.get_latest_correspondence(conn, "master")
    assert bc["ak_commit"] == "ak_a"


def test_sweep_closure_check_stops_on_gh_failure(tmp_path):
    """Transient gh failure on PR #102: walk stops at ak_b, cursor
    advance reflects only what we processed before (ak_a). Sweep
    proceeds (rc=0) -- closure check is best-effort."""
    from translation_agent import github as gh
    db_path = str(tmp_path / "t.db")
    _seed_with_existing_prs(db_path, {"ak_a": 101, "ak_b": 102, "ak_c": 103})

    next_commits_mock = MagicMock(side_effect=[
        ["ak_a", "ak_b", "ak_c"],
        ["ak_b", "ak_c", "ak_d"],  # re-fetch
    ])
    with patch("translation_agent.cli.git_ops.next_commits", next_commits_mock), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               return_value=999), \
         patch("translation_agent.cli.github.get_pr_state",
               side_effect=[
                   ("MERGED", "merge_sha_a"),
                   gh.GhError("rate limit hit"),
               ]):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    bc = db.get_latest_correspondence(conn, "master")
    # Only ak_a was processed before the failure.
    assert bc["ak_commit"] == "ak_a"


def test_sweep_closure_check_skipped_in_dry_run(tmp_path):
    """Dry-run never makes remote queries: get_pr_state must not be
    called at all."""
    db_path = str(tmp_path / "t.db")
    _seed_with_existing_prs(db_path, {"ak_a": 101})

    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr"), \
         patch("translation_agent.cli.github.get_pr_state") as mstate, \
         patch("translation_agent.cli._r2_available", return_value=False):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master", "--dry-run",
            db_path=db_path,
        )
    assert rc == 0
    mstate.assert_not_called()
    # Cursor unchanged.
    conn = db.connect(db_path)
    bc = db.get_latest_correspondence(conn, "master")
    assert bc["ak_commit"] == "ak_seed"


def test_sweep_closure_check_skips_synthetic_pr_rows(tmp_path):
    """Synthetic dry-run rows (pr_number < 0) have no real PR; walk
    must stop at them without calling gh."""
    db_path = str(tmp_path / "t.db")
    conn = db.connect(db_path)
    db.migrate(conn)
    db.seed_correspondence(conn, "trunk", "ak_seed", "master")
    db.insert_pr_commit(conn, -12345, "master", "trunk", "ak_a")  # synthetic
    conn.commit()
    conn.close()

    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               return_value=999), \
         patch("translation_agent.cli.github.get_pr_state") as mstate:
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    mstate.assert_not_called()
    # Synthetic row is preserved; cursor unchanged.
    conn = db.connect(db_path)
    assert conn.execute(
        "SELECT count(*) FROM pr_commit WHERE pr_number = -12345"
    ).fetchone()[0] == 1
    bc = db.get_latest_correspondence(conn, "master")
    assert bc["ak_commit"] == "ak_seed"


def test_sweep_closure_check_advances_cursor_for_closed_without_merge(tmp_path):
    """CLOSED-without-merge PRs still advance the cursor by ak_commit
    just like MERGED ones -- the row is removed and the next sweep
    moves on. No pr_commit_history row is written (only MERGED PRs
    are archived)."""
    db_path = str(tmp_path / "t.db")
    _seed_with_existing_prs(db_path, {"ak_a": 101})

    next_commits_mock = MagicMock(side_effect=[["ak_a"], []])
    with patch("translation_agent.cli.git_ops.next_commits", next_commits_mock), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               return_value=999), \
         patch("translation_agent.cli.github.get_pr_state",
               side_effect=[("CLOSED", None)]):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    bc = db.get_latest_correspondence(conn, "master")
    assert bc["ak_commit"] == "ak_a"
    # CLOSED-without-merge: no archive row.
    assert conn.execute(
        "SELECT count(*) FROM pr_commit_history"
    ).fetchone()[0] == 0


def test_sweep_closure_check_nulls_out_dependents_when_archiving(tmp_path):
    """When the closure walker archives PR #101 (ak_a), any other
    pr_commit row on the same rust_branch that references ak_a as a
    plan_dependency or implementation_dependency must be nulled out
    in place. The downstream PR remains; only its dep columns clear."""
    db_path = str(tmp_path / "t.db")
    _seed_with_existing_prs(db_path, {"ak_a": 101})
    # Add a downstream PR with deps on ak_a -- and a same-branch PR
    # with an unrelated dep that must be left alone.
    conn = db.connect(db_path)
    db.insert_pr_commit(conn, 200, "master", "trunk", "ak_x")
    db.update_dependencies(conn, 200, "ak_a", "ak_a")  # both deps on ak_a
    db.insert_pr_commit(conn, 201, "master", "trunk", "ak_y")
    db.update_dependencies(conn, 201, "ak_other", None)  # unrelated dep
    conn.commit()
    conn.close()

    next_commits_mock = MagicMock(side_effect=[["ak_a"], []])
    with patch("translation_agent.cli.git_ops.next_commits", next_commits_mock), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               return_value=999), \
         patch("translation_agent.cli.github.get_pr_state",
               side_effect=[("MERGED", "merge_sha_a")]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, '{"plan_dependency": null, '
                                '"implementation_dependency": null}')), \
         patch("translation_agent.cli.git_ops.push_branch"), \
         patch("translation_agent.cli.git_ops.rev_parse",
               return_value="rust_x_sha"):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    pr200 = db.get_pr(conn, 200)
    pr201 = db.get_pr(conn, 201)
    # Dependent PR's both columns nulled in place.
    assert pr200["plan_dependency"] is None
    assert pr200["implementation_dependency"] is None
    # Unrelated dep on a different ak_commit was NOT touched.
    assert pr201["plan_dependency"] == "ak_other"


# --- dep-eval flow ----------------------------------------------------------

def test_sweep_dep_eval_transitions_status_0_to_1(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    json_a = '{"plan_dependency": null, "implementation_dependency": null}'
    json_b = '{"plan_dependency": "ak_a", "implementation_dependency": null}'
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a", "ak_b"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               side_effect=[101, 102]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=[(0, json_a), (0, json_b)]):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    rows = {r["pr_number"]: dict(r) for r in
            conn.execute("SELECT * FROM pr_commit").fetchall()}
    for pr_number, row in rows.items():
        assert row["status"] == db.STATUS_DEPENDENCIES_EVALUATED, row
    # PR for ak_b should have plan_dep=ak_a (which is in batch).
    pr_for_ak_b = next(r for r in rows.values() if r["ak_commit"] == "ak_b")
    assert pr_for_ak_b["plan_dependency"] == "ak_a"


def test_sweep_dep_eval_out_of_batch_dep_treated_as_none(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    bogus_json = '{"plan_dependency": "not_in_batch_sha", "implementation_dependency": null}'
    # streaming.run_with_prefix is called both for dep-eval AND for the plan
    # generation that immediately follows in the same sweep (status 1 -> 2,
    # since plan_dep is None after the out-of-batch coercion). Both succeed
    # with empty stdout.
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.git_ops.push_branch"), \
         patch("translation_agent.cli.github.create_draft_pr",
               side_effect=[101]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=[(0, bogus_json), (0, "")]):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    row = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 101").fetchone())
    # Out-of-batch dep coerced to None; plan then generated (status 1 -> 2).
    assert row["plan_dependency"] is None
    assert row["status"] == db.STATUS_PLAN_CREATED


def test_sweep_dep_eval_failure_persists_last_error(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               side_effect=[101]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(2, "boom")):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    row = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 101").fetchone())
    assert row["status"] == db.STATUS_NO_PLAN  # unchanged
    assert "rc=2" in row["last_error"]


def test_sweep_dep_eval_unparseable_json_persists_last_error(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               side_effect=[101]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "no json here")):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    row = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 101").fetchone())
    assert row["status"] == db.STATUS_NO_PLAN
    assert "could not parse" in row["last_error"]


def test_sweep_dep_eval_dry_run_skips_r2_when_r2_absent(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.streaming.run_with_prefix") as mstream, \
         patch("translation_agent.cli._r2_available", return_value=False):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master", "--dry-run",
            db_path=db_path,
        )
    assert rc == 0
    mstream.assert_not_called()


def test_sweep_dep_eval_dry_run_runs_r2_when_present(tmp_path):
    """When --dry-run AND r2 is on PATH, dep-eval is invoked for real
    (read-only). The dep result is persisted, transitioning the synthetic
    row 0 -> 1. Then the plan phase ALSO runs (since r2 is available and
    the row is now at status 1, unblocked) AND advances status 1 -> 2
    locally."""
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    json_resp = '{"plan_dependency": null, "implementation_dependency": null}'
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=[(0, json_resp), (0, "")]) as mstream, \
         patch("translation_agent.cli._r2_available", return_value=True):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master", "--dry-run",
            db_path=db_path,
        )
    assert rc == 0
    # 1 dep-eval call + 1 plan call (cascade after dep-eval transitioned to 1).
    assert mstream.call_count == 2
    conn = db.connect(db_path)
    row = dict(conn.execute("SELECT * FROM pr_commit").fetchone())
    assert row["pr_number"] < 0  # synthetic
    assert row["status"] == db.STATUS_PLAN_CREATED  # advanced 0 -> 1 -> 2
    assert row["plan_dependency"] is None


# --- plan + implementation flow (sweep step 6 + 8) --------------------------

def test_sweep_dry_run_plan_runs_when_r2_present_advances_status_locally(tmp_path):
    """With --dry-run + r2 + a status-1 row, the sweep invokes claude
    for plan generation in a preserved worktree AND advances DB status
    1 -> 2 (locally only -- no push, no artifact upload)."""
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    conn = db.connect(db_path)
    db.insert_pr_commit(conn, 90, "master", "trunk", "ak_z")
    db.update_dependencies(conn, 90, None, None)
    conn.close()
    with patch("translation_agent.cli.git_ops.next_commits", return_value=[]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "")) as mstream, \
         patch("translation_agent.cli._r2_available", return_value=True):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master", "--dry-run",
            db_path=db_path,
        )
    assert rc == 0
    mstream.assert_called_once()
    conn = db.connect(db_path)
    pr = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 90").fetchone())
    assert pr["status"] == db.STATUS_PLAN_CREATED  # advanced 1 -> 2 locally


def test_sweep_dry_run_plan_skipped_when_r2_absent(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    conn = db.connect(db_path)
    db.insert_pr_commit(conn, 91, "master", "trunk", "ak_w")
    db.update_dependencies(conn, 91, None, None)
    conn.close()
    with patch("translation_agent.cli.git_ops.next_commits", return_value=[]), \
         patch("translation_agent.cli.streaming.run_with_prefix") as mstream, \
         patch("translation_agent.cli._r2_available", return_value=False):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master", "--dry-run",
            db_path=db_path,
        )
    assert rc == 0
    mstream.assert_not_called()


def test_sweep_plan_step_transitions_status_1_to_2(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    # Pre-populate a status-1 row that needs a plan generated.
    conn = db.connect(db_path)
    db.insert_pr_commit(conn, 50, "master", "trunk", "ak_x")
    db.update_dependencies(conn, 50, None, None)
    conn.close()
    with patch("translation_agent.cli.git_ops.next_commits", return_value=[]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "")), \
         patch("translation_agent.cli.git_ops.push_branch"):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    pr = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 50").fetchone())
    assert pr["status"] == db.STATUS_PLAN_CREATED


def test_sweep_impl_step_transitions_status_3_to_4_and_updates_branch_commit(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    conn = db.connect(db_path)
    db.insert_pr_commit(conn, 60, "master", "trunk", "ak_y")
    conn.execute("UPDATE pr_commit SET status = ? WHERE pr_number = 60",
                 (db.STATUS_PLAN_APPROVED,))
    conn.commit()
    conn.close()
    with patch("translation_agent.cli.git_ops.next_commits", return_value=[]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "")), \
         patch("translation_agent.cli.git_ops.push_branch"), \
         patch("translation_agent.cli.git_ops.rev_parse",
               return_value="rust_y_sha"):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    pr = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 60").fetchone())
    assert pr["status"] == db.STATUS_IMPLEMENTATION_DONE
    bc = db.get_latest_correspondence(conn, "master")
    assert bc["ak_commit"] == "ak_y"


# --- per-state label transitions -------------------------------------------

def test_sweep_dep_eval_adds_dependencies_evaluated_label_and_writes_dep_section(
    tmp_path, real_pr_description,
):
    """After dep-eval moves a row to status 1, the orchestrator (a)
    adds the 'dependencies-evaluated' label, and (b) writes the dep
    section to the PR body resolving dep AK SHAs to their pr_commit
    pr_numbers within the same rust_branch."""
    from translation_agent import prompts
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    # Two PRs in the batch: ak_a (depended on) and ak_b (depends on ak_a).
    conn = db.connect(db_path)
    db.insert_pr_commit(conn, 100, "master", "trunk", "ak_a")
    db.insert_pr_commit(conn, 200, "master", "trunk", "ak_b")
    conn.close()
    # ak_a -> no deps; ak_b -> plan_dep=ak_a, impl_dep=ak_a.
    json_a = '{"plan_dependency": null, "implementation_dependency": null}'
    json_b = '{"plan_dependency": "ak_a", "implementation_dependency": "ak_a"}'
    with patch("translation_agent.cli.git_ops.next_commits", return_value=[]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               side_effect=[(0, json_a), (0, json_b)]), \
         patch("translation_agent.cli.github.get_pr_body",
               return_value="## Summary"), \
         patch("translation_agent.cli.github.update_pr_body") as mupd, \
         patch("translation_agent.cli.github.add_pr_label") as madd, \
         patch("translation_agent.cli.github.remove_pr_label") as mrem:
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    # Both rows transitioned to status 1.
    conn = db.connect(db_path)
    assert dict(conn.execute(
        "SELECT * FROM pr_commit WHERE pr_number = 100"
    ).fetchone())["status"] == db.STATUS_DEPENDENCIES_EVALUATED
    assert dict(conn.execute(
        "SELECT * FROM pr_commit WHERE pr_number = 200"
    ).fetchone())["status"] == db.STATUS_DEPENDENCIES_EVALUATED
    # Both got the dependencies-evaluated label.
    assert madd.call_count == 2
    for call in madd.call_args_list:
        assert call.args[2] == prompts.LABEL_DEPENDENCIES_EVALUATED
    # No removals at the dep-eval stage.
    mrem.assert_not_called()
    # PR 200 got a dep section update referencing PR #100; PR 100 has
    # no deps so update_pr_body MAY or may not be called depending on
    # whether the body changed (replace_dep_section is a no-op then).
    body_writes_for_200 = [
        c for c in mupd.call_args_list if c.args[1] == 200
    ]
    assert len(body_writes_for_200) == 1
    new_body = body_writes_for_200[0].args[2]
    assert "**Dependencies:**" in new_body
    assert "- Plan: #100" in new_body
    assert "- Implementation: #100" in new_body


def test_sweep_plan_step_swaps_dependencies_evaluated_for_plan_created_label(
    tmp_path, real_pr_description,
):
    """After plan generation transitions status 1 -> 2, the orchestrator
    removes 'dependencies-evaluated' (if present) and adds
    'plan-created'."""
    from translation_agent import prompts
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    conn = db.connect(db_path)
    db.insert_pr_commit(conn, 50, "master", "trunk", "ak_x")
    db.update_dependencies(conn, 50, None, None)  # status -> 1
    conn.close()
    # Make _update_pr_description_via_r2 a no-op so the label assertions
    # aren't muddied by the description-update path.
    with patch("translation_agent.cli.git_ops.next_commits", return_value=[]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "")), \
         patch("translation_agent.cli.git_ops.push_branch"), \
         patch("translation_agent.cli._update_pr_description_via_r2",
               return_value=None), \
         patch("translation_agent.cli.github.add_pr_label") as madd, \
         patch("translation_agent.cli.github.remove_pr_label") as mrem:
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    mrem.assert_called_once_with(
        ANY, 50, prompts.LABEL_DEPENDENCIES_EVALUATED,
    )
    madd.assert_called_once_with(ANY, 50, prompts.LABEL_PLAN_CREATED)


def test_sweep_impl_step_clears_intermediate_labels_and_marks_implementation_done(
    tmp_path, real_pr_description,
):
    """After impl transitions 3 -> 4, the orchestrator strips the three
    intermediate labels (dependencies-evaluated, plan-created,
    implementation-needed) and sets implementation-done."""
    from translation_agent import prompts
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    conn = db.connect(db_path)
    db.insert_pr_commit(conn, 60, "master", "trunk", "ak_y")
    conn.execute("UPDATE pr_commit SET status = ? WHERE pr_number = 60",
                 (db.STATUS_PLAN_APPROVED,))
    conn.commit()
    conn.close()
    with patch("translation_agent.cli.git_ops.next_commits", return_value=[]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "")), \
         patch("translation_agent.cli.git_ops.push_branch"), \
         patch("translation_agent.cli.git_ops.rev_parse",
               return_value="rust_y_sha"), \
         patch("translation_agent.cli._update_pr_description_via_r2",
               return_value=None), \
         patch("translation_agent.cli.github.add_pr_label") as madd, \
         patch("translation_agent.cli.github.remove_pr_label") as mrem:
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    removed_labels = [c.args[2] for c in mrem.call_args_list]
    assert removed_labels == [
        prompts.LABEL_DEPENDENCIES_EVALUATED,
        prompts.LABEL_PLAN_CREATED,
        prompts.LABEL_IMPLEMENTATION_NEEDED,
    ]
    madd.assert_called_once_with(ANY, 60, prompts.LABEL_IMPLEMENTATION_DONE)


def test_pr_plan_approve_impl_clears_intermediate_labels_and_marks_done(
    tmp_path, real_pr_description,
):
    """The --plan-approve cascade (per-PR mode) takes the same
    label-cleanup path as the sweep impl step: -3 intermediates,
    +implementation-done."""
    from translation_agent import prompts
    db_path = str(tmp_path / "t.db")
    _insert_pr_at_status(db_path, 42, "abc", db.STATUS_PLAN_CREATED)
    with patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "")), \
         patch("translation_agent.cli.git_ops.push_branch"), \
         patch("translation_agent.cli.git_ops.rev_parse",
               return_value="rust_new_sha"), \
         patch("translation_agent.cli._update_pr_description_via_r2",
               return_value=None), \
         patch("translation_agent.cli.github.prepend_pr_body"), \
         patch("translation_agent.cli.github.add_pr_label") as madd, \
         patch("translation_agent.cli.github.remove_pr_label") as mrem:
        rc = _run("--pr", "42", "--plan-approve", db_path=db_path)
    assert rc == 0
    assert [c.args[2] for c in mrem.call_args_list] == [
        prompts.LABEL_DEPENDENCIES_EVALUATED,
        prompts.LABEL_PLAN_CREATED,
        prompts.LABEL_IMPLEMENTATION_NEEDED,
    ]
    madd.assert_called_once_with(ANY, 42, prompts.LABEL_IMPLEMENTATION_DONE)


def test_apply_label_transition_skips_synthetic_pr_number(real_pr_description):
    """Synthetic dry-run pr_numbers (< 0) have no real PR; the helper
    must not invoke any gh subprocess for them."""
    from types import SimpleNamespace
    args = SimpleNamespace(rust_repo_path=".", dry_run=False)
    with patch("translation_agent.cli.github.add_pr_label") as madd, \
         patch("translation_agent.cli.github.remove_pr_label") as mrem:
        cli._apply_label_transition(
            args, -12345, add=("a",), remove=("b",),
        )
    madd.assert_not_called()
    mrem.assert_not_called()


def test_apply_label_transition_skips_in_dry_run(real_pr_description):
    """Dry-run never touches GitHub. Helper logs intent and returns."""
    from types import SimpleNamespace
    args = SimpleNamespace(rust_repo_path=".", dry_run=True)
    with patch("translation_agent.cli.github.add_pr_label") as madd, \
         patch("translation_agent.cli.github.remove_pr_label") as mrem:
        cli._apply_label_transition(
            args, 42, add=("a",), remove=("b",),
        )
    madd.assert_not_called()
    mrem.assert_not_called()


def test_apply_label_transition_swallows_per_label_errors(
    real_pr_description, caplog,
):
    """One label failing to add/remove must not block the others, and
    must not raise -- labeling is cosmetic."""
    import logging
    from types import SimpleNamespace
    from translation_agent import github as gh
    args = SimpleNamespace(rust_repo_path=".", dry_run=False)
    with patch(
        "translation_agent.cli.github.remove_pr_label",
        side_effect=[gh.GhError("not authorized"), None],
    ), patch(
        "translation_agent.cli.github.add_pr_label",
        side_effect=gh.GhError("label not found"),
    ):
        with caplog.at_level(logging.WARNING, logger="translation_agent.cli"):
            cli._apply_label_transition(
                args, 42, add=("c",), remove=("a", "b"),
            )
    msgs = [r.message for r in caplog.records]
    assert any("'a'" in m for m in msgs)
    assert any("'c'" in m for m in msgs)


def test_update_pr_dep_section_idempotent_on_repeated_calls(real_pr_description):
    """A retry of dep-eval (same plan/impl deps) must NOT accumulate
    dep blocks in the body. Two consecutive calls produce identical
    content."""
    from types import SimpleNamespace
    args = SimpleNamespace(rust_repo_path=".", dry_run=False)
    bodies = []  # capture each new_body that update_pr_body would write

    def fake_get():
        return bodies[-1] if bodies else "## Summary"

    def fake_update(repo, pr, new):
        bodies.append(new)

    with patch("translation_agent.cli.github.get_pr_body",
               side_effect=lambda r, n: fake_get()), \
         patch("translation_agent.cli.github.update_pr_body",
               side_effect=fake_update):
        cli._update_pr_dep_section(args, 42, 100, 200)
        cli._update_pr_dep_section(args, 42, 100, 200)
    # Second call: replace_dep_section sees an identical dep block, the
    # produced body equals the current one, and the helper short-
    # circuits without calling update_pr_body again.
    assert len(bodies) == 1
    assert bodies[0].count("<!-- deps:start -->") == 1


def test_sweep_plan_blocked_by_unapproved_dep_skipped(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    conn = db.connect(db_path)
    # PR 70 has ak_a (status 1, not approved). PR 71 depends on ak_a.
    db.insert_pr_commit(conn, 70, "master", "trunk", "ak_a")
    db.update_dependencies(conn, 70, None, None)
    db.insert_pr_commit(conn, 71, "master", "trunk", "ak_b")
    db.update_dependencies(conn, 71, "ak_a", None)
    conn.close()
    with patch("translation_agent.cli.git_ops.next_commits", return_value=[]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, "")) as mstream, \
         patch("translation_agent.cli.git_ops.push_branch"):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    # Only PR 70 should have been planned (status 1->2). PR 71's plan_dep ak_a
    # is still at status 1 (not >= 3), so it stays at status 1 this sweep.
    assert mstream.call_count == 1
    conn = db.connect(db_path)
    pr70 = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 70").fetchone())
    pr71 = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 71").fetchone())
    assert pr70["status"] == db.STATUS_PLAN_CREATED
    assert pr71["status"] == db.STATUS_DEPENDENCIES_EVALUATED


def test_sweep_plan_failure_persists_last_error_keeps_status_1(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    conn = db.connect(db_path)
    db.insert_pr_commit(conn, 80, "master", "trunk", "ak_z")
    db.update_dependencies(conn, 80, None, None)
    conn.close()
    with patch("translation_agent.cli.git_ops.next_commits", return_value=[]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(7, "boom")):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    pr = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 80").fetchone())
    assert pr["status"] == db.STATUS_DEPENDENCIES_EVALUATED  # unchanged
    assert "rc=7" in pr["last_error"]


def test_sweep_continues_after_gh_error_on_one_commit(tmp_path):
    """One commit fails with GhError, the next still succeeds."""
    from translation_agent import github as gh

    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a", "ak_b"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.worktree.push_branch_with_kafka_bump"), \
         patch("translation_agent.cli.github.create_draft_pr",
               side_effect=[gh.GhError("boom"), 200]):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    rows = conn.execute("SELECT pr_number, ak_commit FROM pr_commit").fetchall()
    assert len(rows) == 1
    assert dict(rows[0]) == {"pr_number": 200, "ak_commit": "ak_b"}

