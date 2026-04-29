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

"""SQLite schema and persistence helpers for the translation agent.

Tables track:

- `branch_commit`: which AK commit on which AK branch corresponds to which
  Rust commit on which Rust branch. Seeded once with `--seed` and updated
  by the orchestrator each time a Rust commit lands.
- `pr_commit`: per-PR state machine, status enum 0..4 per design step 3.
"""

try:
    import sqlite3
except ImportError as _e:
    # pyenv-built Pythons sometimes lack the _sqlite3 C extension because
    # libsqlite3-dev was missing at compile time. Fall back to the
    # pip-installable `pysqlite3` binary so dev environments work without
    # rebuilding the interpreter. No effect on environments with a working
    # stdlib sqlite3 (the common case).
    try:
        import pysqlite3 as sqlite3
    except ImportError:
        raise ImportError(
            "Neither stdlib sqlite3 nor pysqlite3 is available. Either "
            "rebuild your Python with libsqlite3-dev installed, or run "
            "`pip install pysqlite3-binary`."
        ) from _e

from typing import Optional


# Status enum values match design step 3.
STATUS_NO_PLAN = 0
STATUS_DEPENDENCIES_EVALUATED = 1
STATUS_PLAN_CREATED = 2
STATUS_PLAN_APPROVED = 3
STATUS_IMPLEMENTATION_DONE = 4

STATUS_NAMES = {
    STATUS_NO_PLAN: "no_plan",
    STATUS_DEPENDENCIES_EVALUATED: "dependencies_evaluated",
    STATUS_PLAN_CREATED: "plan_created",
    STATUS_PLAN_APPROVED: "plan_approved",
    STATUS_IMPLEMENTATION_DONE: "implementation_done",
}


_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS branch_commit (
        ak_branch    TEXT NOT NULL,
        ak_commit    TEXT NOT NULL,
        rust_branch  TEXT NOT NULL,
        rust_commit  TEXT NOT NULL,
        PRIMARY KEY (ak_branch, ak_commit, rust_branch)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pr_commit (
        pr_number                  INTEGER PRIMARY KEY,
        rust_branch                TEXT NOT NULL,
        ak_commit                  TEXT NOT NULL,
        plan_dependency            TEXT,
        implementation_dependency  TEXT,
        status                     INTEGER NOT NULL DEFAULT 0,
        last_error                 TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_pr_commit_status ON pr_commit(status)",
    "CREATE INDEX IF NOT EXISTS idx_pr_commit_rust_branch ON pr_commit(rust_branch)",
    "CREATE INDEX IF NOT EXISTS idx_branch_commit_rust ON branch_commit(rust_branch, rust_commit)",
]


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    """Idempotently create all tables and indexes."""
    with conn:
        for stmt in _SCHEMA:
            conn.execute(stmt)


def seed_correspondence(
    conn: sqlite3.Connection,
    ak_branch: str,
    ak_commit: str,
    rust_branch: str,
    rust_commit: str,
) -> bool:
    """Insert a (AK, Rust) correspondence row.

    Idempotent: if a row already exists for the (ak_branch, ak_commit,
    rust_branch) primary key, the existing row is left unchanged. Returns
    True if a new row was inserted, False otherwise.
    """
    with conn:
        cursor = conn.execute(
            "INSERT OR IGNORE INTO branch_commit "
            "(ak_branch, ak_commit, rust_branch, rust_commit) "
            "VALUES (?, ?, ?, ?)",
            (ak_branch, ak_commit, rust_branch, rust_commit),
        )
        return cursor.rowcount > 0


def get_pr(conn: sqlite3.Connection, pr_number: int) -> Optional[dict]:
    row = conn.execute(
        "SELECT * FROM pr_commit WHERE pr_number = ?", (pr_number,)
    ).fetchone()
    return dict(row) if row else None


def mark_plan_approved(conn: sqlite3.Connection, pr_number: int) -> None:
    """Transition a pr_commit row from status 2 (plan_created) to 3 (plan_approved).

    Raises ValueError if the row does not exist or is not in status 2.
    Clears `last_error` on success.
    """
    with conn:
        row = conn.execute(
            "SELECT status FROM pr_commit WHERE pr_number = ?", (pr_number,)
        ).fetchone()
        if row is None:
            raise ValueError(f"No pr_commit row for PR {pr_number}")
        if row["status"] != STATUS_PLAN_CREATED:
            raise ValueError(
                f"PR {pr_number} is in status {row['status']} "
                f"({STATUS_NAMES.get(row['status'], 'unknown')}), "
                f"expected {STATUS_PLAN_CREATED} (plan_created)"
            )
        conn.execute(
            "UPDATE pr_commit SET status = ?, last_error = NULL WHERE pr_number = ?",
            (STATUS_PLAN_APPROVED, pr_number),
        )
