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

from translation_agent import db


@pytest.fixture
def conn():
    c = db.connect(":memory:")
    db.migrate(c)
    yield c
    c.close()


def test_migrate_is_idempotent(conn):
    db.migrate(conn)
    db.migrate(conn)


def test_seed_correspondence_inserts_then_idempotent(conn):
    assert db.seed_correspondence(conn, "trunk", "abc", "master") == "inserted"
    assert db.seed_correspondence(conn, "trunk", "abc", "master") == "unchanged"
    rows = conn.execute("SELECT * FROM branch_commit").fetchall()
    assert len(rows) == 1
    assert dict(rows[0]) == {
        "rust_branch": "master", "ak_branch": "trunk", "ak_commit": "abc",
    }


def test_seed_correspondence_distinct_rust_branches_coexist(conn):
    assert db.seed_correspondence(conn, "trunk", "abc", "master") == "inserted"
    assert db.seed_correspondence(conn, "trunk", "abc", "feature") == "inserted"
    assert conn.execute("SELECT count(*) FROM branch_commit").fetchone()[0] == 2


def test_seed_correspondence_same_rust_branch_different_values_errors(conn):
    """PK is rust_branch alone -- a second seed for the same rust_branch
    with different values must error unless `force=True`."""
    db.seed_correspondence(conn, "trunk", "abc", "master")
    with pytest.raises(ValueError, match="already exists"):
        db.seed_correspondence(conn, "trunk", "newak", "master")


def test_seed_correspondence_force_updates_in_place(conn):
    """With force=True, existing rust_branch row is updated to new
    ak values."""
    db.seed_correspondence(conn, "trunk", "abc", "master")
    assert db.seed_correspondence(
        conn, "trunk", "newak", "master", force=True,
    ) == "updated"
    row = dict(conn.execute(
        "SELECT * FROM branch_commit WHERE rust_branch = 'master'"
    ).fetchone())
    assert row == {
        "rust_branch": "master", "ak_branch": "trunk", "ak_commit": "newak",
    }


def test_seed_correspondence_force_no_change_returns_unchanged(conn):
    """force=True is a no-op if values match the existing row."""
    db.seed_correspondence(conn, "trunk", "abc", "master")
    assert db.seed_correspondence(
        conn, "trunk", "abc", "master", force=True,
    ) == "unchanged"


def test_migrate_old_branch_commit_pk_is_collapsed(conn):
    """A pre-existing branch_commit table with the old multi-column PK
    AND the now-removed rust_commit column gets rebuilt with the
    current schema, keeping only the most recent row per rust_branch
    and dropping the column."""
    # Drop and recreate with the OLD schema (multi-PK + rust_commit
    # column), plus two rows for the same rust_branch (representing
    # what an older orchestrator would have accumulated as the cursor
    # advanced).
    conn.execute("DROP TABLE branch_commit")
    conn.execute("""
        CREATE TABLE branch_commit (
            ak_branch    TEXT NOT NULL,
            ak_commit    TEXT NOT NULL,
            rust_branch  TEXT NOT NULL,
            rust_commit  TEXT NOT NULL,
            PRIMARY KEY (ak_branch, ak_commit, rust_branch)
        )
    """)
    conn.execute(
        "INSERT INTO branch_commit VALUES ('trunk', 'old_ak', 'master', 'old_rust')")
    conn.execute(
        "INSERT INTO branch_commit VALUES ('trunk', 'new_ak', 'master', 'new_rust')")
    conn.commit()
    db.migrate(conn)
    rows = conn.execute("SELECT * FROM branch_commit").fetchall()
    assert len(rows) == 1
    assert dict(rows[0]) == {
        "rust_branch": "master", "ak_branch": "trunk", "ak_commit": "new_ak",
    }
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(branch_commit)")}
    assert cols == {"rust_branch", "ak_branch", "ak_commit"}


def test_migrate_drops_rust_commit_column_from_single_pk_schema(conn):
    """A DB on the intermediate schema (single-column PK on rust_branch
    BUT still with the vestigial rust_commit column) gets the column
    dropped in place. Rows are preserved verbatim across the rebuild."""
    conn.execute("DROP TABLE branch_commit")
    conn.execute("""
        CREATE TABLE branch_commit (
            rust_branch  TEXT NOT NULL PRIMARY KEY,
            ak_branch    TEXT NOT NULL,
            ak_commit    TEXT NOT NULL,
            rust_commit  TEXT NOT NULL
        )
    """)
    conn.execute(
        "INSERT INTO branch_commit VALUES ('master', 'trunk', 'ak1', 'rust1')")
    conn.execute(
        "INSERT INTO branch_commit VALUES ('feature', 'trunk', 'ak2', 'rust2')")
    conn.commit()
    db.migrate(conn)
    rows = sorted(
        (dict(r) for r in conn.execute("SELECT * FROM branch_commit")),
        key=lambda r: r["rust_branch"],
    )
    assert rows == [
        {"rust_branch": "feature", "ak_branch": "trunk", "ak_commit": "ak2"},
        {"rust_branch": "master",  "ak_branch": "trunk", "ak_commit": "ak1"},
    ]
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(branch_commit)")}
    assert cols == {"rust_branch", "ak_branch", "ak_commit"}


def test_get_pr_missing_returns_none(conn):
    assert db.get_pr(conn, 42) is None


def test_get_pr_returns_row(conn):
    conn.execute(
        "INSERT INTO pr_commit (pr_number, rust_branch, ak_commit) VALUES (?, ?, ?)",
        (42, "master", "abc"),
    )
    row = db.get_pr(conn, 42)
    assert row is not None
    assert row["pr_number"] == 42
    assert row["status"] == db.STATUS_NO_PLAN
    assert row["plan_dependency"] is None


def test_mark_plan_approved_transitions_2_to_3(conn):
    conn.execute(
        "INSERT INTO pr_commit (pr_number, rust_branch, ak_commit, status) "
        "VALUES (?, ?, ?, ?)",
        (42, "master", "abc", db.STATUS_PLAN_CREATED),
    )
    db.mark_plan_approved(conn, 42)
    assert db.get_pr(conn, 42)["status"] == db.STATUS_PLAN_APPROVED


def test_mark_plan_approved_clears_last_error(conn):
    conn.execute(
        "INSERT INTO pr_commit (pr_number, rust_branch, ak_commit, status, last_error) "
        "VALUES (?, ?, ?, ?, ?)",
        (42, "master", "abc", db.STATUS_PLAN_CREATED, "old failure"),
    )
    db.mark_plan_approved(conn, 42)
    assert db.get_pr(conn, 42)["last_error"] is None


def test_mark_plan_approved_missing_pr_raises(conn):
    with pytest.raises(ValueError, match="No pr_commit row"):
        db.mark_plan_approved(conn, 999)


def test_get_pr_commits_by_status_filters(conn):
    db.insert_pr_commit(conn, 1, "master", "trunk", "ak1")
    db.insert_pr_commit(conn, 2, "master", "trunk", "ak2")
    db.insert_pr_commit(conn, 3, "feature", "trunk", "ak3")
    conn.execute("UPDATE pr_commit SET status = 1 WHERE pr_number = 2")
    conn.commit()
    s0 = db.get_pr_commits_by_status(conn, db.STATUS_NO_PLAN)
    assert {r["pr_number"] for r in s0} == {1, 3}
    s0_master = db.get_pr_commits_by_status(conn, db.STATUS_NO_PLAN, "master")
    assert {r["pr_number"] for r in s0_master} == {1}


def test_update_dependencies_transitions_to_status_1(conn):
    db.insert_pr_commit(conn, 42, "master", "trunk", "ak42")
    db.update_dependencies(conn, 42, "depA", "depB")
    row = db.get_pr(conn, 42)
    assert row["status"] == db.STATUS_DEPENDENCIES_EVALUATED
    assert row["plan_dependency"] == "depA"
    assert row["implementation_dependency"] == "depB"


def test_update_dependencies_clears_last_error(conn):
    db.insert_pr_commit(conn, 42, "master", "trunk", "ak42")
    db.set_last_error(conn, 42, "old")
    db.update_dependencies(conn, 42, None, None)
    assert db.get_pr(conn, 42)["last_error"] is None


def test_set_last_error_does_not_change_status(conn):
    db.insert_pr_commit(conn, 42, "master", "trunk", "ak42")
    db.set_last_error(conn, 42, "boom")
    row = db.get_pr(conn, 42)
    assert row["last_error"] == "boom"
    assert row["status"] == db.STATUS_NO_PLAN


def test_get_latest_correspondence_none_when_empty(conn):
    assert db.get_latest_correspondence(conn, "master") is None


def test_get_latest_correspondence_returns_row_per_branch(conn):
    """Each rust_branch has at most one row (PK on rust_branch)."""
    db.seed_correspondence(conn, "trunk", "ak1", "master")
    db.seed_correspondence(conn, "trunk", "ak3", "feature")
    row = db.get_latest_correspondence(conn, "master")
    assert row["ak_commit"] == "ak1"
    row2 = db.get_latest_correspondence(conn, "feature")
    assert row2["ak_commit"] == "ak3"


def test_get_latest_correspondence_after_force_update(conn):
    """After a force-update, get_latest reflects the new values."""
    db.seed_correspondence(conn, "trunk", "ak1", "master")
    db.seed_correspondence(conn, "trunk", "ak2", "master", force=True)
    row = db.get_latest_correspondence(conn, "master")
    assert row["ak_commit"] == "ak2"


def test_insert_pr_commit_sets_status_zero(conn):
    assert db.insert_pr_commit(conn, 42, "master", "trunk", "akabc") is True
    row = db.get_pr(conn, 42)
    assert row["status"] == db.STATUS_NO_PLAN
    assert row["rust_branch"] == "master"
    assert row["ak_branch"] == "trunk"
    assert row["ak_commit"] == "akabc"


def test_insert_pr_commit_idempotent(conn):
    assert db.insert_pr_commit(conn, 42, "master", "trunk", "akabc") is True
    assert db.insert_pr_commit(conn, 42, "master", "trunk", "akabc") is False
    assert conn.execute("SELECT count(*) FROM pr_commit").fetchone()[0] == 1


def test_get_pr_commit_by_branch_and_ak_returns_matching_row(conn):
    """The sweep PR-closure check looks up rows by (rust_branch,
    ak_commit) -- not by pr_number -- so the helper must scope to
    both columns."""
    db.insert_pr_commit(conn, 1, "master",      "trunk", "akA")
    db.insert_pr_commit(conn, 2, "master",      "trunk", "akB")
    db.insert_pr_commit(conn, 3, "dev/feature", "trunk", "akA")  # same ak, diff branch

    row = db.get_pr_commit_by_branch_and_ak(conn, "master", "akB")
    assert row is not None
    assert row["pr_number"] == 2

    # Same ak on a different rust branch: distinct row.
    row = db.get_pr_commit_by_branch_and_ak(conn, "dev/feature", "akA")
    assert row is not None
    assert row["pr_number"] == 3


def test_get_pr_commit_by_branch_and_ak_returns_none_when_missing(conn):
    db.insert_pr_commit(conn, 1, "master", "trunk", "akA")
    assert db.get_pr_commit_by_branch_and_ak(conn, "master", "akZ") is None
    assert db.get_pr_commit_by_branch_and_ak(conn, "other-branch", "akA") is None


def test_archive_pr_commit_merged_writes_history_and_deletes(conn):
    """MERGED PRs (rust_commit not None) record the AK->Rust pair in
    pr_commit_history and remove the live pr_commit row."""
    db.insert_pr_commit(conn, 1, "master", "trunk", "akA")
    db.insert_pr_commit(conn, 2, "master", "trunk", "akB")

    assert db.archive_pr_commit(conn, 1, rust_commit="rust_a_sha") is True
    remaining = [
        dict(r) for r in conn.execute(
            "SELECT * FROM pr_commit ORDER BY pr_number"
        ).fetchall()
    ]
    assert len(remaining) == 1
    assert remaining[0]["pr_number"] == 2

    history = [
        dict(r) for r in conn.execute(
            "SELECT * FROM pr_commit_history"
        ).fetchall()
    ]
    assert history == [{
        "rust_branch": "master", "ak_branch": "trunk",
        "ak_commit": "akA", "rust_commit": "rust_a_sha",
    }]


def test_archive_pr_commit_closed_without_merge_skips_history(conn):
    """CLOSED-without-merge (rust_commit=None) deletes the row but
    leaves pr_commit_history empty."""
    db.insert_pr_commit(conn, 1, "master", "trunk", "akA")
    assert db.archive_pr_commit(conn, 1, rust_commit=None) is True
    assert conn.execute("SELECT count(*) FROM pr_commit").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM pr_commit_history").fetchone()[0] == 0


def test_archive_pr_commit_returns_false_when_no_match(conn):
    """Idempotent: archiving a non-existent pr_number is a no-op + False
    and writes no history."""
    db.insert_pr_commit(conn, 1, "master", "trunk", "akA")
    assert db.archive_pr_commit(conn, 999, rust_commit="x") is False
    assert conn.execute("SELECT count(*) FROM pr_commit").fetchone()[0] == 1
    assert conn.execute("SELECT count(*) FROM pr_commit_history").fetchone()[0] == 0
    # Re-archiving an already-removed row also returns False.
    assert db.archive_pr_commit(conn, 1, rust_commit="rust1") is True
    assert db.archive_pr_commit(conn, 1, rust_commit="rust2") is False


def test_archive_pr_commit_re_archive_replaces_rust_commit(conn):
    """If the same (rust_branch, ak_branch, ak_commit) is archived
    twice (e.g. after --cleanup-prs and a fresh re-merge), the second
    insert overwrites the first via INSERT OR REPLACE."""
    db.insert_pr_commit(conn, 1, "master", "trunk", "akA")
    db.archive_pr_commit(conn, 1, rust_commit="first_merge_sha")
    # Re-create and re-archive the same logical PR with a new merge SHA.
    db.insert_pr_commit(conn, 2, "master", "trunk", "akA")
    db.archive_pr_commit(conn, 2, rust_commit="second_merge_sha")

    rows = [dict(r) for r in conn.execute("SELECT * FROM pr_commit_history")]
    assert rows == [{
        "rust_branch": "master", "ak_branch": "trunk",
        "ak_commit": "akA", "rust_commit": "second_merge_sha",
    }]


def test_archive_pr_commit_nulls_dependents_on_same_branch(conn):
    """Other rows on the same rust_branch with plan_dependency or
    implementation_dependency = the archived ak_commit get nulled out."""
    db.insert_pr_commit(conn, 10, "master", "trunk", "akDep")
    db.insert_pr_commit(conn, 11, "master", "trunk", "akX")
    db.insert_pr_commit(conn, 12, "master", "trunk", "akY")
    db.insert_pr_commit(conn, 13, "master", "trunk", "akZ")
    db.update_dependencies(conn, 11, "akDep", None)        # plan dep on akDep
    db.update_dependencies(conn, 12, None,    "akDep")     # impl dep on akDep
    db.update_dependencies(conn, 13, "akDep", "akDep")     # both

    assert db.archive_pr_commit(conn, 10, rust_commit="rust_dep") is True

    pr11 = db.get_pr(conn, 11)
    pr12 = db.get_pr(conn, 12)
    pr13 = db.get_pr(conn, 13)
    assert pr11["plan_dependency"] is None
    assert pr11["implementation_dependency"] is None  # was already None
    assert pr12["plan_dependency"] is None
    assert pr12["implementation_dependency"] is None
    assert pr13["plan_dependency"] is None
    assert pr13["implementation_dependency"] is None


def test_archive_pr_commit_does_not_null_deps_on_other_branches(conn):
    """Dep null-out is scoped to the same rust_branch; other branches'
    dependents that happen to reference the same ak_commit are
    untouched (branches are independent translation queues)."""
    db.insert_pr_commit(conn, 10, "master",      "trunk", "akDep")
    db.insert_pr_commit(conn, 20, "master",      "trunk", "akSame")
    db.insert_pr_commit(conn, 21, "dev/feature", "trunk", "akSame")
    db.update_dependencies(conn, 20, "akDep", None)  # same-branch dependent
    db.update_dependencies(conn, 21, "akDep", None)  # other-branch dependent

    db.archive_pr_commit(conn, 10, rust_commit="rust_dep")

    assert db.get_pr(conn, 20)["plan_dependency"] is None
    assert db.get_pr(conn, 21)["plan_dependency"] == "akDep"


def test_archive_pr_commit_nulls_deps_even_when_skipping_history(conn):
    """CLOSED-without-merge still discharges dependents -- the dep PR
    is gone, downstream PRs shouldn't wait forever."""
    db.insert_pr_commit(conn, 10, "master", "trunk", "akDep")
    db.insert_pr_commit(conn, 11, "master", "trunk", "akX")
    db.update_dependencies(conn, 11, "akDep", None)

    db.archive_pr_commit(conn, 10, rust_commit=None)

    assert db.get_pr(conn, 11)["plan_dependency"] is None
    assert conn.execute("SELECT count(*) FROM pr_commit_history").fetchone()[0] == 0


def test_cleanup_pr_commits_for_rust_branch_deletes_only_matching_branch(conn):
    """Used by --seed --cleanup-prs. Must delete exactly the supplied
    branch's PR rows and leave other branches untouched."""
    db.insert_pr_commit(conn, 1, "master",       "trunk", "ak1")
    db.insert_pr_commit(conn, 2, "master",       "trunk", "ak2")
    db.insert_pr_commit(conn, 3, "dev/feature",  "trunk", "ak3")

    deleted = db.cleanup_pr_commits_for_rust_branch(conn, "master")
    assert deleted == 2

    remaining = [
        dict(r) for r in conn.execute(
            "SELECT * FROM pr_commit ORDER BY pr_number"
        ).fetchall()
    ]
    assert len(remaining) == 1
    assert remaining[0]["pr_number"] == 3
    assert remaining[0]["rust_branch"] == "dev/feature"


def test_cleanup_pr_commits_for_rust_branch_idempotent_returns_zero(conn):
    """Re-running on a branch with no rows is a clean no-op (rowcount 0)."""
    db.insert_pr_commit(conn, 1, "master", "trunk", "ak1")
    assert db.cleanup_pr_commits_for_rust_branch(conn, "master") == 1
    assert db.cleanup_pr_commits_for_rust_branch(conn, "master") == 0
    # Other branches untouched throughout.
    assert db.cleanup_pr_commits_for_rust_branch(conn, "nonexistent") == 0


def test_delete_pr_commit_removes_only_target_row(conn):
    """delete_pr_commit drops the row matching pr_number and leaves
    siblings untouched. Idempotent: re-deleting the same number is a
    no-op (no rowcount-checking inside the helper)."""
    db.insert_pr_commit(conn, 1, "master", "trunk", "ak1")
    db.insert_pr_commit(conn, 2, "master", "trunk", "ak2")
    db.delete_pr_commit(conn, 1)
    assert db.get_pr(conn, 1) is None
    assert db.get_pr(conn, 2) is not None
    # Idempotent re-delete of an already-gone row is a no-op.
    db.delete_pr_commit(conn, 1)
    assert db.get_pr(conn, 2) is not None


def test_delete_pr_commit_does_not_write_history(conn):
    """Unlike archive_pr_commit (which writes pr_commit_history for
    merged PRs), delete_pr_commit must NOT touch the history table.
    Rationale: PRs deleted by --delete-prs were explicitly NOT merged,
    so writing them to history would corrupt that table's semantics."""
    db.insert_pr_commit(conn, 1, "master", "trunk", "ak1")
    db.delete_pr_commit(conn, 1)
    rows = conn.execute("SELECT * FROM pr_commit_history").fetchall()
    assert rows == []


def test_mark_plan_created_transitions_1_to_2(conn):
    db.insert_pr_commit(conn, 42, "master", "trunk", "ak")
    db.update_dependencies(conn, 42, None, None)
    db.mark_plan_created(conn, 42)
    assert db.get_pr(conn, 42)["status"] == db.STATUS_PLAN_CREATED


def test_mark_implementation_done_inserts_branch_commit_atomically(conn):
    db.insert_pr_commit(conn, 42, "master", "trunk", "ak42")
    conn.execute("UPDATE pr_commit SET status = ? WHERE pr_number = 42",
                 (db.STATUS_PLAN_APPROVED,))
    conn.commit()
    db.mark_implementation_done(
        conn, 42, ak_branch="trunk", ak_commit="ak42",
        rust_branch="master",
    )
    pr = db.get_pr(conn, 42)
    assert pr["status"] == db.STATUS_IMPLEMENTATION_DONE
    bc = db.get_latest_correspondence(conn, "master")
    assert bc["ak_commit"] == "ak42"


# --- unblocked predicate ----------------------------------------------------

def _make_pr(conn, n, ak_commit, status, plan_dep=None, impl_dep=None):
    db.insert_pr_commit(conn, n, "master", "trunk", ak_commit)
    conn.execute(
        "UPDATE pr_commit SET status = ?, plan_dependency = ?, "
        "implementation_dependency = ? WHERE pr_number = ?",
        (status, plan_dep, impl_dep, n),
    )
    conn.commit()


def test_unblocked_invalid_dep_column_raises(conn):
    with pytest.raises(ValueError, match="invalid dep_column"):
        db.get_unblocked_for_status(conn, 1, 3, "bogus")


def test_unblocked_no_dep_is_unblocked(conn):
    _make_pr(conn, 1, "ak1", db.STATUS_DEPENDENCIES_EVALUATED)
    rows = db.get_unblocked_for_status(
        conn, db.STATUS_DEPENDENCIES_EVALUATED, db.STATUS_PLAN_APPROVED,
        "plan_dependency",
    )
    assert {r["pr_number"] for r in rows} == {1}


def test_unblocked_dep_out_of_batch_is_unblocked(conn):
    # PR 1 depends on a SHA that isn't in pr_commit at all.
    _make_pr(conn, 1, "ak1", db.STATUS_DEPENDENCIES_EVALUATED, plan_dep="bogus")
    rows = db.get_unblocked_for_status(
        conn, db.STATUS_DEPENDENCIES_EVALUATED, db.STATUS_PLAN_APPROVED,
        "plan_dependency",
    )
    assert {r["pr_number"] for r in rows} == {1}


def test_unblocked_dep_below_threshold_is_blocked(conn):
    _make_pr(conn, 1, "ak1", db.STATUS_PLAN_APPROVED)  # status 3, but not 4
    _make_pr(conn, 2, "ak2", db.STATUS_DEPENDENCIES_EVALUATED, plan_dep="ak1")
    rows = db.get_unblocked_for_status(
        conn, db.STATUS_DEPENDENCIES_EVALUATED, db.STATUS_PLAN_APPROVED,
        "plan_dependency",
    )
    assert {r["pr_number"] for r in rows} == {2}  # ak1 is at >= 3 -> 2 unblocked


def test_unblocked_dep_at_threshold_unblocks(conn):
    _make_pr(conn, 1, "ak1", db.STATUS_IMPLEMENTATION_DONE)
    _make_pr(conn, 2, "ak2", db.STATUS_PLAN_APPROVED, impl_dep="ak1")
    rows = db.get_unblocked_for_status(
        conn, db.STATUS_PLAN_APPROVED, db.STATUS_IMPLEMENTATION_DONE,
        "implementation_dependency",
    )
    assert {r["pr_number"] for r in rows} == {2}


def test_unblocked_dep_below_impl_threshold_blocks(conn):
    _make_pr(conn, 1, "ak1", db.STATUS_PLAN_APPROVED)  # 3, no impl_dep
    _make_pr(conn, 2, "ak2", db.STATUS_PLAN_APPROVED, impl_dep="ak1")
    rows = db.get_unblocked_for_status(
        conn, db.STATUS_PLAN_APPROVED, db.STATUS_IMPLEMENTATION_DONE,
        "implementation_dependency",
    )
    # 1 has no dep -> unblocked. 2's dep ak1 is at status 3 (not >= 4) -> blocked.
    assert {r["pr_number"] for r in rows} == {1}


def test_unblocked_filters_by_rust_branch(conn):
    db.insert_pr_commit(conn, 1, "master", "trunk", "ak1")
    db.insert_pr_commit(conn, 2, "feature", "trunk", "ak2")
    conn.execute("UPDATE pr_commit SET status = 1")
    conn.commit()
    rows = db.get_unblocked_for_status(
        conn, 1, 3, "plan_dependency", rust_branch="master",
    )
    assert {r["pr_number"] for r in rows} == {1}


def test_mark_plan_approved_wrong_status_raises(conn):
    conn.execute(
        "INSERT INTO pr_commit (pr_number, rust_branch, ak_commit, status) "
        "VALUES (?, ?, ?, ?)",
        (42, "master", "abc", db.STATUS_NO_PLAN),
    )
    with pytest.raises(ValueError, match="expected"):
        db.mark_plan_approved(conn, 42)
