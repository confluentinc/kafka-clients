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

import subprocess
from unittest.mock import patch

import pytest

from translation_agent import semaphore


def test_push_raises_when_artifact_binary_missing():
    with patch.object(semaphore.shutil, "which", return_value=None):
        with pytest.raises(FileNotFoundError, match="`artifact` binary"):
            semaphore.push_project_artifact("td", "/tmp/db.sqlite")


def test_push_invokes_subprocess():
    with patch.object(
        semaphore.shutil, "which", return_value="/usr/local/bin/artifact"
    ), patch.object(semaphore.subprocess, "run") as mrun:
        semaphore.push_project_artifact("td", "/tmp/db.sqlite")
    mrun.assert_called_once_with(
        ["artifact", "push", "project", "/tmp/db.sqlite", "--force"],
        check=True,
        capture_output=True,
    )


def test_push_no_force_raises_when_artifact_binary_missing():
    with patch.object(semaphore.shutil, "which", return_value=None):
        with pytest.raises(FileNotFoundError, match="`artifact` binary"):
            semaphore.push_project_artifact_no_force("lk", "/tmp/lock")


def test_push_no_force_invokes_subprocess_without_force_flag():
    """Without an explicit `destination`, the subprocess argv uses the
    local file_path as the destination (the CLI then derives the
    artifact name from its basename). No --force flag is passed."""
    with patch.object(
        semaphore.shutil, "which", return_value="/usr/local/bin/artifact"
    ), patch.object(semaphore.subprocess, "run") as mrun:
        semaphore.push_project_artifact_no_force("lk", "/tmp/lock")
    mrun.assert_called_once_with(
        [
            "artifact", "push", "project", "/tmp/lock",
            "--destination", "/tmp/lock",
        ],
        check=True,
        capture_output=True,
    )
    sent = mrun.call_args[0][0]
    assert "--force" not in sent


def test_push_no_force_with_explicit_destination_overrides_remote_name():
    """With an explicit `destination`, the remote artifact is named
    after `destination`, NOT after the local file's basename. This is
    the mechanism the lock primitive uses to ensure the artifact
    always lands as `translation_agent.db.lock` regardless of where
    the local lock file lives on disk."""
    with patch.object(
        semaphore.shutil, "which", return_value="/usr/local/bin/artifact"
    ), patch.object(semaphore.subprocess, "run") as mrun:
        semaphore.push_project_artifact_no_force(
            "lk", "/tmp/translation_agent.db.lock",
            destination="translation_agent.db.lock",
        )
    mrun.assert_called_once_with(
        [
            "artifact", "push", "project",
            "/tmp/translation_agent.db.lock",
            "--destination", "translation_agent.db.lock",
        ],
        check=True,
        capture_output=True,
    )


def test_pull_raises_when_artifact_binary_missing():
    with patch.object(semaphore.shutil, "which", return_value=None):
        with pytest.raises(FileNotFoundError, match="`artifact` binary"):
            semaphore.pull_project_artifact("td", "/tmp")


def test_pull_invokes_subprocess():
    with patch.object(
        semaphore.shutil, "which", return_value="/usr/local/bin/artifact"
    ), patch.object(semaphore.subprocess, "run") as mrun:
        semaphore.pull_project_artifact("td", "/tmp")
    mrun.assert_called_once_with(
        ["artifact", "pull", "project", "td",
         "--destination", "/tmp", "--force"],
        check=True,
        capture_output=True,
    )


def test_yank_raises_when_artifact_binary_missing():
    with patch.object(semaphore.shutil, "which", return_value=None):
        with pytest.raises(FileNotFoundError, match="`artifact` binary"):
            semaphore.yank_project_artifact("lk")


def test_yank_invokes_subprocess():
    with patch.object(
        semaphore.shutil, "which", return_value="/usr/local/bin/artifact"
    ), patch.object(semaphore.subprocess, "run") as mrun:
        semaphore.yank_project_artifact("lk")
    mrun.assert_called_once_with(
        ["artifact", "yank", "project", "lk"],
        check=True,
        capture_output=True,
    )


# --- diagnostic surfacing ---------------------------------------------------
#
# Every artifact CLI failure goes through `_run_artifact()` which logs
# the captured stderr/stdout at ERROR level before re-raising the
# CalledProcessError. Without this, the operator only sees a bare
# "Command [...] returned non-zero exit status N" with no clue what
# the CLI actually complained about. The four tests below pin that
# surfacing for each wrapper.

def _failing_run(returncode: int, stderr: bytes = b"", stdout: bytes = b""):
    """Build a subprocess.run mock side_effect that always raises
    CalledProcessError with the supplied stderr/stdout payloads."""
    err = subprocess.CalledProcessError(
        returncode, ["artifact"], output=stdout, stderr=stderr,
    )

    def _raise(*_a, **_kw):
        raise err

    return _raise


def test_push_logs_stderr_and_reraises_on_cli_failure(caplog):
    import logging
    with patch.object(
        semaphore.shutil, "which", return_value="/usr/local/bin/artifact"
    ), patch.object(
        semaphore.subprocess, "run",
        side_effect=_failing_run(1, stderr=b"unknown flag: --xyz"),
    ):
        with caplog.at_level(logging.ERROR, logger="translation_agent.semaphore"):
            with pytest.raises(subprocess.CalledProcessError):
                semaphore.push_project_artifact("td", "/tmp/db")
    assert any(
        "unknown flag: --xyz" in rec.message for rec in caplog.records
    )


def test_push_no_force_logs_stderr_and_reraises_on_cli_failure(caplog):
    import logging
    with patch.object(
        semaphore.shutil, "which", return_value="/usr/local/bin/artifact"
    ), patch.object(
        semaphore.subprocess, "run",
        side_effect=_failing_run(1, stderr=b"namespace not found: project"),
    ):
        with caplog.at_level(logging.ERROR, logger="translation_agent.semaphore"):
            with pytest.raises(subprocess.CalledProcessError):
                semaphore.push_project_artifact_no_force("lk", "/tmp/lock")
    assert any(
        "namespace not found: project" in rec.message
        for rec in caplog.records
    )


def test_pull_logs_stderr_and_reraises_on_cli_failure(caplog):
    import logging
    with patch.object(
        semaphore.shutil, "which", return_value="/usr/local/bin/artifact"
    ), patch.object(
        semaphore.subprocess, "run",
        side_effect=_failing_run(
            1, stderr=b"--destination: invalid path '.'",
        ),
    ):
        with caplog.at_level(logging.ERROR, logger="translation_agent.semaphore"):
            with pytest.raises(subprocess.CalledProcessError):
                semaphore.pull_project_artifact("td", ".")
    assert any(
        "--destination: invalid path '.'" in rec.message
        for rec in caplog.records
    )


def test_yank_logs_stderr_and_reraises_on_cli_failure(caplog):
    import logging
    with patch.object(
        semaphore.shutil, "which", return_value="/usr/local/bin/artifact"
    ), patch.object(
        semaphore.subprocess, "run",
        side_effect=_failing_run(1, stderr=b"artifact does not exist: lk"),
    ):
        with caplog.at_level(logging.ERROR, logger="translation_agent.semaphore"):
            with pytest.raises(subprocess.CalledProcessError):
                semaphore.yank_project_artifact("lk")
    assert any(
        "artifact does not exist: lk" in rec.message
        for rec in caplog.records
    )


def test_run_artifact_also_logs_stdout_when_present(caplog):
    """If the CLI prints to stdout AND fails, both streams are logged."""
    import logging
    with patch.object(
        semaphore.subprocess, "run",
        side_effect=_failing_run(
            2, stderr=b"err msg", stdout=b"out msg",
        ),
    ):
        with caplog.at_level(logging.ERROR, logger="translation_agent.semaphore"):
            with pytest.raises(subprocess.CalledProcessError):
                semaphore._run_artifact(["artifact", "test"])
    msgs = [rec.message for rec in caplog.records]
    assert any("err msg" in m for m in msgs)
    assert any("out msg" in m for m in msgs)
