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
from unittest.mock import patch

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
    def fake_wt(repo_path, branch_name, *, cleanup=True, base_remote_branch=None):
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
        "--rust-branch", "master", "--rust-commit", "def",
        db_path=db_path,
    )
    assert rc == 0
    conn = db.connect(db_path)
    assert conn.execute("SELECT count(*) FROM branch_commit").fetchone()[0] == 1


def test_seed_idempotent(tmp_path):
    db_path = str(tmp_path / "t.db")
    args = [
        "--seed", "--ak-branch", "trunk", "--ak-commit", "abc",
        "--rust-branch", "master", "--rust-commit", "def",
    ]
    assert _run(*args, db_path=db_path) == 0
    assert _run(*args, db_path=db_path) == 0
    conn = db.connect(db_path)
    assert conn.execute("SELECT count(*) FROM branch_commit").fetchone()[0] == 1


def test_seed_missing_args_returns_2(tmp_path):
    db_path = str(tmp_path / "t.db")
    rc = _run("--seed", "--ak-branch", "trunk", db_path=db_path)
    assert rc == 2


def test_pr_mode_missing_returns_1(tmp_path):
    db_path = str(tmp_path / "t.db")
    rc = _run("--pr", "42", db_path=db_path)
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
         patch("translation_agent.cli.git_ops.fetch"), \
         patch("translation_agent.cli.git_ops.rev_parse",
               return_value="rust_new_sha"):
        rc = _run("--pr", "42", "--plan-approve", db_path=db_path)
    assert rc == 0
    conn = db.connect(db_path)
    pr = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 42").fetchone())
    assert pr["status"] == db.STATUS_IMPLEMENTATION_DONE
    bc = dict(conn.execute(
        "SELECT * FROM branch_commit WHERE rust_commit = 'rust_new_sha'"
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
    # branch_commit gets a row pointing at the local dry-run SHA.
    bc = dict(conn.execute(
        "SELECT * FROM branch_commit WHERE rust_commit = 'local_dry_run_sha'"
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
                  "--rust-branch", "master", "--rust-commit", "r",
                  db_path=db_path)
    assert rc == 0
    mpush.assert_called_once_with("translation_agent_db", db_path)


def test_no_artifact_push_skips(tmp_path):
    db_path = str(tmp_path / "t.db")
    with patch("translation_agent.cli.semaphore.push_project_artifact") as mpush:
        rc = _run("--no-artifact-push",
                  "--seed", "--ak-branch", "trunk", "--ak-commit", "a",
                  "--rust-branch", "master", "--rust-commit", "r",
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
                  "--rust-branch", "master", "--rust-commit", "r",
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
    _seed_db(db_path, ak_commit="ak_seed", rust_commit="rust_seed")

    # Sweep run: creates PR for ak_a, dep-evals, plans it. Stops at status 2
    # (no auto plan-approve).
    dep_eval_json = '{"plan_dependency": null, "implementation_dependency": null}'
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.git_ops.push_new_branch"), \
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
         patch("translation_agent.cli.git_ops.fetch"), \
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
    assert bc["rust_commit"] == "rust_a_sha"


def test_help_runs():
    with pytest.raises(SystemExit) as exc:
        cli.main(["--help"])
    assert exc.value.code == 0


# --- sweep mode -------------------------------------------------------------

def _seed_db(db_path, ak_commit="ak0", rust_commit="r0"):
    conn = db.connect(db_path)
    db.migrate(conn)
    db.seed_correspondence(conn, "trunk", ak_commit, "master", rust_commit)
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
         patch("translation_agent.cli.git_ops.push_new_branch") as mpush, \
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
         patch("translation_agent.cli.git_ops.push_new_branch") as mpush, \
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


def test_sweep_recovers_pr_number_on_already_exists(tmp_path):
    from translation_agent import github as gh

    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.git_ops.push_new_branch"), \
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


# --- dep-eval flow ----------------------------------------------------------

def test_sweep_dep_eval_transitions_status_0_to_1(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    json_a = '{"plan_dependency": null, "implementation_dependency": null}'
    json_b = '{"plan_dependency": "ak_a", "implementation_dependency": null}'
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a", "ak_b"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.git_ops.push_new_branch"), \
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
         patch("translation_agent.cli.git_ops.push_new_branch"), \
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
         patch("translation_agent.cli.git_ops.push_new_branch"), \
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
         patch("translation_agent.cli.git_ops.push_new_branch"), \
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
               return_value=(0, "")):
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
         patch("translation_agent.cli.git_ops.fetch"), \
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
    assert bc["rust_commit"] == "rust_y_sha"


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
               return_value=(0, "")) as mstream:
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
         patch("translation_agent.cli.git_ops.push_new_branch"), \
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

