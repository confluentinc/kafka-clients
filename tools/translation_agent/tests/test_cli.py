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

from translation_agent import cli, db


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


def test_pr_status_check_prints_row(tmp_path, capsys):
    db_path = str(tmp_path / "t.db")
    conn = db.connect(db_path)
    db.migrate(conn)
    conn.execute(
        "INSERT INTO pr_commit (pr_number, rust_branch, ak_commit, status) "
        "VALUES (?, ?, ?, ?)",
        (42, "master", "abc123", db.STATUS_PLAN_CREATED),
    )
    conn.commit()
    conn.close()
    rc = _run("--pr", "42", db_path=db_path)
    assert rc == 0
    captured = capsys.readouterr()
    assert "pr_number: 42" in captured.out
    assert f"status: {db.STATUS_PLAN_CREATED}" in captured.out


def test_pr_plan_approve_transitions_2_to_3(tmp_path):
    db_path = str(tmp_path / "t.db")
    conn = db.connect(db_path)
    db.migrate(conn)
    conn.execute(
        "INSERT INTO pr_commit (pr_number, rust_branch, ak_commit, status) "
        "VALUES (?, ?, ?, ?)",
        (42, "master", "abc", db.STATUS_PLAN_CREATED),
    )
    conn.commit()
    conn.close()
    rc = _run("--pr", "42", "--plan-approve", db_path=db_path)
    assert rc == 0
    conn = db.connect(db_path)
    assert conn.execute(
        "SELECT status FROM pr_commit WHERE pr_number = 42"
    ).fetchone()[0] == db.STATUS_PLAN_APPROVED


def test_pr_plan_approve_wrong_status_returns_1(tmp_path):
    db_path = str(tmp_path / "t.db")
    conn = db.connect(db_path)
    db.migrate(conn)
    conn.execute(
        "INSERT INTO pr_commit (pr_number, rust_branch, ak_commit, status) "
        "VALUES (?, ?, ?, ?)",
        (42, "master", "abc", db.STATUS_NO_PLAN),
    )
    conn.commit()
    conn.close()
    rc = _run("--pr", "42", "--plan-approve", db_path=db_path)
    assert rc == 1


def test_mutually_exclusive_seed_and_pr():
    with pytest.raises(SystemExit):
        cli.main(["--seed", "--pr", "1"])


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


def test_sweep_dry_run_creates_no_prs(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="subj"), \
         patch("translation_agent.cli.git_ops.push_new_branch") as mpush, \
         patch("translation_agent.cli.github.create_draft_pr") as mcreate:
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master", "--dry-run",
            db_path=db_path,
        )
    assert rc == 0
    mpush.assert_not_called()
    mcreate.assert_not_called()
    conn = db.connect(db_path)
    assert conn.execute("SELECT count(*) FROM pr_commit").fetchone()[0] == 0


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
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.git_ops.push_new_branch"), \
         patch("translation_agent.cli.github.create_draft_pr",
               side_effect=[101]), \
         patch("translation_agent.cli.streaming.run_with_prefix",
               return_value=(0, bogus_json)):
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master",
            db_path=db_path,
        )
    assert rc == 0
    conn = db.connect(db_path)
    row = dict(conn.execute("SELECT * FROM pr_commit WHERE pr_number = 101").fetchone())
    assert row["plan_dependency"] is None
    assert row["status"] == db.STATUS_DEPENDENCIES_EVALUATED


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


def test_sweep_dep_eval_dry_run_skips_r2(tmp_path):
    db_path = str(tmp_path / "t.db")
    _seed_db(db_path)
    with patch("translation_agent.cli.git_ops.next_commits",
               return_value=["ak_a"]), \
         patch("translation_agent.cli.git_ops.commit_subject", return_value="s"), \
         patch("translation_agent.cli.streaming.run_with_prefix") as mstream:
        rc = _run(
            "--ak-repo-path", "/tmp/ak", "--ak-branch", "trunk",
            "--rust-branch", "master", "--dry-run",
            db_path=db_path,
        )
    assert rc == 0
    mstream.assert_not_called()


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

