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
    captured_bodies = []

    def fake_push(name, file_path, destination=None):
        # Capture the on-disk file content before acquire_lock unlinks it.
        with open(file_path) as f:
            captured_bodies.append(f.read())

    with patch.object(
        locked_db.semaphore, "push_project_artifact_no_force",
        side_effect=fake_push,
    ):
        locked_db.acquire_lock(
            retries=1, retry_delay_s=0,
            sleep_fn=lambda _: None,
        )
    assert len(captured_bodies) == 1
    body = json.loads(captured_bodies[0])
    assert set(body.keys()) == {"runner_id", "acquired_at", "pid"}
    assert isinstance(body["pid"], int)
    assert body["pid"] == os.getpid()


def test_acquire_lock_cleans_up_tmpfile_on_timeout():
    """The local lock file is unlinked even when acquire fails. Path
    is fixed (`LOCK_LOCAL_PATH`), so the same path is captured each
    retry; the file should be gone after the final attempt."""
    err = subprocess.CalledProcessError(1, ["artifact"])
    with patch.object(
        locked_db.semaphore, "push_project_artifact_no_force",
        side_effect=err,
    ):
        with pytest.raises(locked_db.LockTimeoutError):
            locked_db.acquire_lock(
                retries=2, retry_delay_s=0,
                sleep_fn=lambda _: None,
            )
    assert not os.path.exists(locked_db.LOCK_LOCAL_PATH)


def test_acquire_lock_pushes_with_canonical_destination():
    """REGRESSION: the `destination` arg passed to
    `push_project_artifact_no_force` MUST equal LOCK_ARTIFACT_NAME so
    the resulting remote artifact has that name and release_lock's yank
    targets it correctly.

    The earlier implementation relied on the artifact CLI deriving the
    artifact name from the file's basename, and the local file had a
    randomized basename like `translation_agent_lock_0hi1bkbn.lock`.
    Result: each runner pushed a uniquely-named artifact -> no
    contention check fired -> the lock silently allowed concurrent
    runners into the critical section, AND the artifact store
    accumulated orphan lock files. The fix routes through the
    `destination` parameter so the remote name is explicit and pinned.
    """
    captured_destinations = []

    def capture(name, file_path, destination=None):
        captured_destinations.append(destination)

    with patch.object(
        locked_db.semaphore, "push_project_artifact_no_force",
        side_effect=capture,
    ):
        locked_db.acquire_lock(
            retries=1, retry_delay_s=0,
            sleep_fn=lambda _: None,
        )
    assert len(captured_destinations) == 1
    assert captured_destinations[0] == locked_db.LOCK_ARTIFACT_NAME


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


def test_pull_db_raises_on_failure_by_default():
    """Default behavior is STRICT: any pull failure (artifact missing,
    network error, server down) MUST raise so the orchestrator never
    operates against stale local state and then push-overwrites the
    canonical artifact with our outdated view."""
    err = subprocess.CalledProcessError(1, ["artifact"])
    with patch.object(
        locked_db.semaphore, "pull_project_artifact", side_effect=err,
    ):
        with pytest.raises(subprocess.CalledProcessError):
            locked_db.pull_db("/tmp/state/translation_agent.db")


def test_pull_db_tolerates_failure_when_allow_missing_true_and_no_local_file(
    tmp_path,
):
    """`allow_missing=True` is the seed-mode escape hatch: tolerate a
    pull failure ONLY when there's no local DB file -- that's the
    unambiguous first-run signal. The local file's absence here is
    real (tmp_path is fresh)."""
    db_path = str(tmp_path / "translation_agent.db")
    assert not os.path.exists(db_path)
    err = subprocess.CalledProcessError(1, ["artifact"])
    with patch.object(
        locked_db.semaphore, "pull_project_artifact", side_effect=err,
    ):
        # Must not raise.
        locked_db.pull_db(db_path, allow_missing=True)


def test_pull_db_raises_when_allow_missing_true_but_local_file_exists(
    tmp_path,
):
    """REGRESSION: even with `allow_missing=True`, if a local DB file
    exists AND the pull failed, we MUST raise. The existing local file
    might be stale (from a prior run), and the pull failure might be
    transient infra (artifact server down, network blip) rather than
    "artifact not present". Silently proceeding would risk
    push-overwriting the canonical artifact with our potentially-stale
    local view -- the exact data-loss scenario this guard prevents.
    Operator must `rm` the local file to signal explicit fresh-start
    intent."""
    db_path = str(tmp_path / "translation_agent.db")
    # Simulate a leftover local DB from a prior run.
    with open(db_path, "w") as f:
        f.write("existing local state")
    err = subprocess.CalledProcessError(1, ["artifact"])
    with patch.object(
        locked_db.semaphore, "pull_project_artifact", side_effect=err,
    ):
        with pytest.raises(subprocess.CalledProcessError):
            locked_db.pull_db(db_path, allow_missing=True)


def test_push_db_invokes_force_push():
    with patch.object(
        locked_db.semaphore, "push_project_artifact",
    ) as mpush:
        locked_db.push_db("/tmp/state/translation_agent.db")
    mpush.assert_called_once_with(
        "translation_agent.db", "/tmp/state/translation_agent.db",
    )


def test_pull_db_derives_artifact_name_from_db_path_basename():
    """With a non-default --db-path, the artifact name follows the
    local file's basename and the pull lands at db_path (<dir>/<basename>)."""
    with patch.object(
        locked_db.semaphore, "pull_project_artifact",
    ) as mpull:
        locked_db.pull_db("/tmp/state/ta_local.db")
    mpull.assert_called_once_with("ta_local.db", "/tmp/state")


def test_push_db_derives_artifact_name_from_db_path_basename():
    with patch.object(
        locked_db.semaphore, "push_project_artifact",
    ) as mpush:
        locked_db.push_db("/tmp/state/ta_local.db")
    mpush.assert_called_once_with("ta_local.db", "/tmp/state/ta_local.db")


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
    mpull.assert_called_once_with(db_path, allow_missing=False)
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
    mpull.assert_called_once_with(db_path, allow_missing=False)
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
    mpull.assert_called_once_with(db_path, allow_missing=False)
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


def test_session_propagates_pull_failure_when_allow_missing_false(tmp_path):
    """A pull failure inside session() with the default `allow_missing=False`
    propagates the CalledProcessError out of the with-block. The lock
    is still released (always-finally). Push is NOT attempted."""
    db_path = str(tmp_path / "td.db")
    err = subprocess.CalledProcessError(1, ["artifact"])
    with patch.object(locked_db, "acquire_lock"), \
         patch.object(locked_db, "release_lock") as mrel, \
         patch.object(
             locked_db.semaphore, "pull_project_artifact",
             side_effect=err,
         ), \
         patch.object(locked_db, "push_db") as mpush:
        with pytest.raises(subprocess.CalledProcessError):
            with locked_db.session(db_path, write=True) as conn:
                pass  # pragma: no cover -- pull raises before yield
    mpush.assert_not_called()
    mrel.assert_called_once()


def test_session_tolerates_pull_failure_when_allow_missing_true(tmp_path):
    """With `allow_missing=True` (seed mode), a pull failure is
    swallowed inside session() and the with-block runs against the
    fresh local DB. The push at the end then publishes the new state."""
    db_path = str(tmp_path / "td.db")
    err = subprocess.CalledProcessError(1, ["artifact"])
    with patch.object(locked_db, "acquire_lock"), \
         patch.object(locked_db, "release_lock"), \
         patch.object(
             locked_db.semaphore, "pull_project_artifact",
             side_effect=err,
         ), \
         patch.object(locked_db, "push_db") as mpush:
        with locked_db.session(
            db_path, write=True, allow_missing=True,
        ) as conn:
            db.insert_pr_commit(conn, 42, "rust-br", "ak-br", "ak-sha")
    mpush.assert_called_once_with(db_path)


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
