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

import json
import os
import subprocess
from unittest.mock import patch

import pytest

from translation_agent import db, locked_db


# ---------- acquire_lock ----------


def test_acquire_lock_succeeds_on_first_attempt():
    """Single push call, no retries, no sleeps."""
    sleep_calls = []
    with patch.object(
        locked_db.semaphore, "push_project_artifact_no_force",
    ) as mpush:
        locked_db.acquire_lock(
            retries=3, retry_delay_s=0.0,
            sleep_fn=sleep_calls.append,
        )
    assert mpush.call_count == 1
    assert sleep_calls == []  # no retries means no sleeps


def test_acquire_lock_retries_then_succeeds():
    """Push fails twice (lock held), succeeds on third attempt."""
    sleep_calls = []
    err = subprocess.CalledProcessError(1, ["artifact"])
    with patch.object(
        locked_db.semaphore, "push_project_artifact_no_force",
        side_effect=[err, err, None],
    ) as mpush:
        locked_db.acquire_lock(
            retries=3, retry_delay_s=0.5,
            sleep_fn=sleep_calls.append,
        )
    assert mpush.call_count == 3
    # Two retries means two sleeps (no sleep after the successful attempt).
    assert sleep_calls == [0.5, 0.5]


def test_acquire_lock_raises_timeout_after_all_retries_fail():
    """All push attempts fail -> LockTimeoutError after `retries`."""
    sleep_calls = []
    err = subprocess.CalledProcessError(1, ["artifact"])
    with patch.object(
        locked_db.semaphore, "push_project_artifact_no_force",
        side_effect=err,
    ) as mpush:
        with pytest.raises(locked_db.LockTimeoutError, match="3 attempts"):
            locked_db.acquire_lock(
                retries=3, retry_delay_s=0.1,
                sleep_fn=sleep_calls.append,
            )
    assert mpush.call_count == 3
    # N retries means N-1 sleeps (we don't sleep after the final failure).
    assert sleep_calls == [0.1, 0.1]


def test_acquire_lock_writes_holder_payload_with_required_fields():
    """The lock artifact body is JSON with runner_id, acquired_at, pid."""
    captured_paths = []

    def fake_push(name, file_path):
        # Capture the on-disk file content before acquire_lock unlinks it.
        with open(file_path) as f:
            captured_paths.append(f.read())

    with patch.object(
        locked_db.semaphore, "push_project_artifact_no_force",
        side_effect=fake_push,
    ):
        locked_db.acquire_lock(
            retries=1, retry_delay_s=0,
            sleep_fn=lambda _: None,
        )
    assert len(captured_paths) == 1
    body = json.loads(captured_paths[0])
    assert set(body.keys()) == {"runner_id", "acquired_at", "pid"}
    assert isinstance(body["pid"], int)
    assert body["pid"] == os.getpid()


def test_acquire_lock_cleans_up_tmpfile_on_timeout():
    """The local tmp file is unlinked even when acquire fails."""
    tmp_paths = []
    err = subprocess.CalledProcessError(1, ["artifact"])

    def capture_path(name, file_path):
        tmp_paths.append(file_path)
        raise err

    with patch.object(
        locked_db.semaphore, "push_project_artifact_no_force",
        side_effect=capture_path,
    ):
        with pytest.raises(locked_db.LockTimeoutError):
            locked_db.acquire_lock(
                retries=2, retry_delay_s=0,
                sleep_fn=lambda _: None,
            )
    # Same tmp path each retry; should be unlinked after the final attempt.
    assert tmp_paths
    assert not os.path.exists(tmp_paths[-1])


# ---------- release_lock ----------


def test_release_lock_calls_yank():
    with patch.object(
        locked_db.semaphore, "yank_project_artifact",
    ) as myank:
        locked_db.release_lock()
    myank.assert_called_once_with(locked_db.LOCK_ARTIFACT_NAME)


def test_release_lock_tolerates_failure_without_raising():
    """If yank fails (e.g. lock already gone), log+swallow, don't propagate."""
    err = subprocess.CalledProcessError(1, ["artifact"])
    with patch.object(
        locked_db.semaphore, "yank_project_artifact", side_effect=err,
    ):
        # Must not raise.
        locked_db.release_lock()


# ---------- pull_db / push_db ----------


def test_pull_db_invokes_pull_with_dest_dir():
    with patch.object(
        locked_db.semaphore, "pull_project_artifact",
    ) as mpull:
        locked_db.pull_db("/tmp/state/translation_agent.db")
    mpull.assert_called_once_with("translation_agent.db", "/tmp/state")


def test_pull_db_tolerates_first_run_absence():
    """First-ever sweep: artifact doesn't exist; pull returns non-zero;
    pull_db swallows so the orchestrator can create the DB from scratch."""
    err = subprocess.CalledProcessError(1, ["artifact"])
    with patch.object(
        locked_db.semaphore, "pull_project_artifact", side_effect=err,
    ):
        # Must not raise.
        locked_db.pull_db("/tmp/state/translation_agent.db")


def test_push_db_invokes_force_push():
    with patch.object(
        locked_db.semaphore, "push_project_artifact",
    ) as mpush:
        locked_db.push_db("/tmp/state/translation_agent.db")
    mpush.assert_called_once_with(
        "translation_agent.db", "/tmp/state/translation_agent.db",
    )


# ---------- session ----------


def test_session_read_only_does_not_push(tmp_path):
    """Read session: acquire, pull, yield conn, close, release. No push."""
    db_path = str(tmp_path / "td.db")
    with patch.object(locked_db, "acquire_lock") as macq, \
         patch.object(locked_db, "release_lock") as mrel, \
         patch.object(locked_db, "pull_db") as mpull, \
         patch.object(locked_db, "push_db") as mpush:
        with locked_db.session(db_path, write=False) as conn:
            assert conn is not None
            # Quick smoke: the conn is a usable sqlite handle.
            conn.execute("SELECT 1")
    macq.assert_called_once()
    mpull.assert_called_once_with(db_path)
    mpush.assert_not_called()
    mrel.assert_called_once()


def test_session_write_pushes_after_clean_exit(tmp_path):
    """Write session: acquire, pull, yield, commit, close, push, release."""
    db_path = str(tmp_path / "td.db")
    with patch.object(locked_db, "acquire_lock") as macq, \
         patch.object(locked_db, "release_lock") as mrel, \
         patch.object(locked_db, "pull_db") as mpull, \
         patch.object(locked_db, "push_db") as mpush:
        with locked_db.session(db_path, write=True) as conn:
            db.insert_pr_commit(conn, 42, "rust-br", "ak-br", "ak-sha")
    macq.assert_called_once()
    mpull.assert_called_once_with(db_path)
    mpush.assert_called_once_with(db_path)
    mrel.assert_called_once()


def test_session_exception_skips_push_but_releases_lock(tmp_path):
    """If the with-body raises, push is skipped and lock is released."""
    db_path = str(tmp_path / "td.db")
    with patch.object(locked_db, "acquire_lock") as macq, \
         patch.object(locked_db, "release_lock") as mrel, \
         patch.object(locked_db, "pull_db") as mpull, \
         patch.object(locked_db, "push_db") as mpush:
        with pytest.raises(RuntimeError, match="boom"):
            with locked_db.session(db_path, write=True) as conn:
                raise RuntimeError("boom")
    macq.assert_called_once()
    mpull.assert_called_once_with(db_path)
    mpush.assert_not_called()  # CRITICAL: no corrupted state published
    mrel.assert_called_once()  # lock always released


def test_session_dry_run_skips_all_subprocess_calls(tmp_path):
    """dry_run=True: no acquire, no pull, no push, no release. Just open/close."""
    db_path = str(tmp_path / "td.db")
    with patch.object(locked_db, "acquire_lock") as macq, \
         patch.object(locked_db, "release_lock") as mrel, \
         patch.object(locked_db, "pull_db") as mpull, \
         patch.object(locked_db, "push_db") as mpush:
        with locked_db.session(db_path, write=True, dry_run=True) as conn:
            db.insert_pr_commit(conn, 99, "rust-br", "ak-br", "ak-sha")
    macq.assert_not_called()
    mrel.assert_not_called()
    mpull.assert_not_called()
    mpush.assert_not_called()
    # The local DB should still have been migrated and written:
    conn = db.connect(db_path)
    try:
        assert db.get_pr(conn, 99) is not None
    finally:
        conn.close()


def test_session_yields_migrated_connection_usable_for_db_helpers(tmp_path):
    """The yielded conn has the schema applied -- db.* helpers work."""
    db_path = str(tmp_path / "td.db")
    with patch.object(locked_db, "acquire_lock"), \
         patch.object(locked_db, "release_lock"), \
         patch.object(locked_db, "pull_db"), \
         patch.object(locked_db, "push_db"):
        with locked_db.session(db_path, write=True) as conn:
            db.insert_pr_commit(conn, 7, "rust-br", "ak-br", "ak-sha")
        with locked_db.session(db_path, write=False) as conn:
            row = db.get_pr(conn, 7)
            assert row is not None
            assert row["ak_commit"] == "ak-sha"
