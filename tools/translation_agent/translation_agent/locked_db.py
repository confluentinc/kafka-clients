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

"""Distributed lock + per-op artifact pull/push around the sqlite state DB.

Concurrency model: every public db.py call from cli.py is wrapped in a
`session(db_path, write=...)` block which acquires a Semaphore-artifact-
based mutex, pulls the latest DB, opens a fresh sqlite connection,
yields it to the caller, closes it, pushes the modified DB back if
`write=True`, and releases the mutex.

Lock primitive: `artifact push project translation_agent.db.lock`
WITHOUT `--force`. Success = lock acquired; CalledProcessError = the
artifact already exists (someone else holds the lock); sleep and retry.
Release: `artifact yank project translation_agent.db.lock`.

Stale-lock recovery is manual: an operator yanks the lock via the
Semaphore UI. The 10-minute production timeout (10 retries x 60 s) is
deliberate -- long enough to absorb a normal slow run, short enough that
a crashed runner doesn't wedge the system overnight.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterator, Optional

from . import db, semaphore

if TYPE_CHECKING:
    # Type-only import to avoid pulling in the stdlib sqlite3 module here
    # -- `db.py` already runs the pysqlite3 fallback dance for runtime use,
    # and at runtime we just pass through whatever `db.connect()` returns.
    import sqlite3


LOCK_ARTIFACT_NAME = "translation_agent.db.lock"
LOCK_LOCAL_PATH = os.path.join(tempfile.gettempdir(), LOCK_ARTIFACT_NAME)
DB_ARTIFACT_NAME_DEFAULT = "translation_agent.db"


log = logging.getLogger(__name__)


class LockTimeoutError(RuntimeError):
    """Raised when `acquire_lock` exhausts its retry budget."""


def _holder_payload() -> dict:
    """JSON payload written into the lock artifact so an operator can
    decide whether a stale lock is safe to yank.

    Includes the Semaphore job ID (cross-checkable against the job list
    in the Semaphore UI), an ISO-8601 UTC acquire timestamp, and the
    holder pid (sometimes useful for log correlation).
    """
    return {
        "runner_id": os.environ.get("SEMAPHORE_JOB_ID", "unknown"),
        "acquired_at": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
    }


def acquire_lock(
    *,
    retries: int = 10,
    retry_delay_s: float = 60.0,
    sleep_fn: Callable[[float], None] = time.sleep,
    payload: Optional[dict] = None,
) -> None:
    """Acquire the global DB lock by pushing the lock artifact without --force.

    Retries `retries` times with `retry_delay_s` between attempts. Raises
    `LockTimeoutError` if the budget is exhausted. Production defaults
    (10 x 60 s = 10 min) are tunable per call site so unit tests can
    inject `retries=3, retry_delay_s=0, sleep_fn=lambda _: None` to
    cover both the happy path and the timeout path in milliseconds.
    """
    if payload is None:
        payload = _holder_payload()
    body = json.dumps(payload, sort_keys=True)

    # Fixed local path: the Semaphore `artifact push project <file>` CLI
    # derives the artifact name from the file's basename (no `--name`
    # flag exists), so the local file MUST be named LOCK_ARTIFACT_NAME
    # for the resulting artifact to match what release_lock yanks. The
    # actual mutex is the artifact server's "exists" check on the
    # without-`--force` push -- the local path just carries the
    # holder payload and doesn't need to be unique per call.
    with open(LOCK_LOCAL_PATH, "w") as tmp:
        tmp.write(body)

    try:
        for attempt in range(1, retries + 1):
            try:
                semaphore.push_project_artifact_no_force(
                    LOCK_ARTIFACT_NAME, LOCK_LOCAL_PATH, LOCK_ARTIFACT_NAME
                )
                log.debug("Acquired DB lock on attempt %d/%d", attempt, retries)
                return
            except subprocess.CalledProcessError:
                if attempt == retries:
                    break
                log.info(
                    "DB lock held by another runner; "
                    "retrying in %.1fs (attempt %d/%d)",
                    retry_delay_s, attempt, retries,
                )
                sleep_fn(retry_delay_s)
        raise LockTimeoutError(
            f"Failed to acquire DB lock after {retries} attempts. "
            f"If the lock is stale, yank it via: "
            f"`artifact yank project {LOCK_ARTIFACT_NAME}`"
        )
    finally:
        # Best-effort cleanup of the local file. The lock artifact
        # itself lives in the Semaphore artifact store and is released
        # via release_lock -> yank; the local file is just payload
        # and a leftover doesn't affect correctness.
        try:
            os.unlink(LOCK_LOCAL_PATH)
        except OSError:
            pass


def release_lock() -> None:
    """Release the global DB lock by yanking the lock artifact.

    Best-effort: logs a warning on failure rather than raising. The lock
    may already be gone if an operator yanked it for stale-lock cleanup;
    in that case the next `acquire_lock` simply succeeds on its first
    attempt rather than blocking.
    """
    try:
        semaphore.yank_project_artifact(LOCK_ARTIFACT_NAME)
        log.debug("Released DB lock")
    except subprocess.CalledProcessError as e:
        log.warning(
            "Lock release (yank %r) failed: %s. The lock may already "
            "have been cleaned up; subsequent acquires will proceed.",
            LOCK_ARTIFACT_NAME, e,
        )


def pull_db(
    db_path: str, *,
    name: str = DB_ARTIFACT_NAME_DEFAULT,
    allow_missing: bool = False,
) -> None:
    """Pull the DB artifact into the directory containing `db_path`.

    Default behavior is **strict**: any pull failure (artifact missing,
    network error, server down) raises so the orchestrator never
    operates against stale local state and then push-overwrites the
    canonical artifact with our outdated view. The CLI exit code does
    not distinguish "artifact not found" from "transient infra error",
    so we treat both the same to be safe.

    `allow_missing=True` (set only from seed mode) tolerates a pull
    failure ONLY when there's no local file at `db_path`. The
    rationale: a missing local file is the unambiguous "truly first
    run, nothing to operate against" signal. If a local file exists
    AND the pull failed, the artifact COULD exist on the server (the
    failure could be transient infra) and our local file COULD be
    stale -- proceeding would risk push-overwriting the canonical
    state with our outdated view. Operator must `rm` the local file
    to signal explicit intent to bootstrap fresh.
    """
    dest_dir = str(Path(db_path).parent or ".")
    try:
        semaphore.pull_project_artifact(name, dest_dir)
    except subprocess.CalledProcessError as e:
        if allow_missing and not os.path.exists(db_path):
            log.info(
                "DB artifact pull failed AND no local DB at %s "
                "(allow_missing=True, treating as first-run seed): %s. "
                "Proceeding to create a fresh DB locally.",
                db_path, e,
            )
            return
        # Fail loud: do NOT operate against stale local state.
        raise


def push_db(db_path: str, *, name: str = DB_ARTIFACT_NAME_DEFAULT) -> None:
    """Push the DB at `db_path` to the artifact store with --force.

    Reuses `semaphore.push_project_artifact` (which already passes
    `--force`). Always overwrites: the orchestrator is the sole writer
    while holding the lock, so there's no version to preserve.
    """
    semaphore.push_project_artifact(name, db_path)


@contextlib.contextmanager
def session(
    db_path: str, *,
    write: bool,
    dry_run: bool = False,
    allow_missing: bool = False,
) -> Iterator[sqlite3.Connection]:
    """Acquire lock + pull DB + open conn + yield + close + (push if write) + release.

    The yielded connection is fresh per call (open/close per session) so
    no caller can hold a connection across lock boundaries.

    Failure semantics:
    - Exception inside the `with` block -> conn closed without commit,
      DB push SKIPPED (so corrupted/partial state is not published to
      other runners), lock released. The exception propagates.
    - Exception in commit (rare) -> same as above: no push.
    - Clean exit + write=True -> commit + push (in that order).
    - Clean exit + write=False -> no commit, no push.
    - DB pull failure (any reason) -> raises by default. Pass
      `allow_missing=True` from seed mode only -- that's the one
      legitimate "first-run, artifact may not exist yet" scenario.

    In `dry_run=True` mode all subprocess calls are skipped: just open
    the local DB, yield, close. This matches the existing dry-run
    semantics used elsewhere in cli.py.
    """
    if dry_run:
        conn = db.connect(db_path)
        try:
            db.migrate(conn)
            yield conn
            if write:
                conn.commit()
        finally:
            conn.close()
        return

    acquire_lock()
    try:
        pull_db(db_path, allow_missing=allow_missing)
        conn = db.connect(db_path)
        push_after = False
        try:
            db.migrate(conn)
            yield conn
            if write:
                conn.commit()
                push_after = True
        finally:
            conn.close()
        if push_after:
            push_db(db_path)
    finally:
        release_lock()
