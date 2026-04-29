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
