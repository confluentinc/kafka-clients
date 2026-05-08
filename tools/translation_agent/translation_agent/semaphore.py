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

"""Semaphore CI artifact CLI wrappers.

Per design step 0 the Semaphore pipeline pulls the sqlite DB before
invoking the orchestrator; per design step 10 the orchestrator pushes the
DB itself with a Semaphore command. This module provides only the push
side. The pull is the pipeline's responsibility (`artifact pull project
<name>` step in the YAML).
"""

import logging
import shutil
import subprocess


ARTIFACT_BINARY = "artifact"
ARTIFACT_NOT_INSTALLED_MSG = (
    "`artifact` binary not found on PATH. The translation agent expects to "
    "run inside a Semaphore CI job where the `artifact` CLI is preinstalled. "
    "Pass --no-artifact-push for local runs."
)

log = logging.getLogger(__name__)


def _run_artifact(argv: list) -> None:
    """Run the artifact CLI with `argv`. On non-zero exit, log the
    captured stderr (and stdout, if non-empty) at ERROR level before
    re-raising the CalledProcessError.

    Without this, `subprocess.run(check=True, capture_output=True)`
    swallows the CLI's diagnostic output into the exception object
    and the operator only sees `Command [...] returned non-zero exit
    status N` -- useless for figuring out whether the failure was
    "unknown flag", "permission denied", "namespace not found", or
    something else. The exception still propagates so all existing
    catch sites (locked_db.acquire_lock retry, locked_db.pull_db
    allow_missing tolerance) work unchanged.
    """
    try:
        subprocess.run(argv, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        if e.stderr:
            log.error(
                "artifact CLI failed (rc=%d, argv=%s): %s",
                e.returncode, argv,
                e.stderr.decode("utf-8", errors="replace").strip(),
            )
        if e.stdout:
            log.error(
                "artifact CLI stdout: %s",
                e.stdout.decode("utf-8", errors="replace").strip(),
            )
        raise


def push_project_artifact(name: str, file_path: str) -> None:
    """Push file_path to the Semaphore project-level artifact `name`.

    Raises FileNotFoundError if the `artifact` binary is not on PATH; raises
    subprocess.CalledProcessError on non-zero exit (with the CLI's
    stderr surfaced via _run_artifact's ERROR log).
    """
    if shutil.which(ARTIFACT_BINARY) is None:
        raise FileNotFoundError(ARTIFACT_NOT_INSTALLED_MSG)
    log.info("Pushing %s to Semaphore project artifact %r", file_path, name)
    _run_artifact(
        [ARTIFACT_BINARY, "push", "project", file_path, "--force"],
    )


def push_project_artifact_no_force(
    name: str, file_path: str, destination: str = None,
) -> None:
    """Push file_path to project artifact `name` WITHOUT --force.

    `destination` (optional) overrides the remote artifact name. When
    omitted, defaults to `file_path` (the CLI then uses the local
    file's basename). Use `destination` to upload a local file under
    a controlled remote name -- e.g., the lock primitive uploads
    `/tmp/translation_agent.db.lock` under destination
    `translation_agent.db.lock` so release_lock's yank targets the
    right artifact.

    Used as the lock-acquire primitive: when the artifact already exists,
    the CLI returns non-zero and `subprocess.run(check=True)` raises
    CalledProcessError. Callers (locked_db.acquire_lock) catch that and
    treat it as "lock currently held by another runner."

    Raises FileNotFoundError if the `artifact` binary is not on PATH.
    """
    if not destination:
        destination = file_path
    if shutil.which(ARTIFACT_BINARY) is None:
        raise FileNotFoundError(ARTIFACT_NOT_INSTALLED_MSG)
    log.info(
        "Pushing %s to Semaphore project artifact %s", file_path, destination,
    )
    _run_artifact(
        [
            ARTIFACT_BINARY, "push", "project", file_path,
            "--destination", destination,
        ],
    )


def pull_project_artifact(name: str, dest_dir: str) -> None:
    """Pull project artifact `name` into `dest_dir` with --force overwrite.

    Raises FileNotFoundError if the `artifact` binary is not on PATH;
    raises subprocess.CalledProcessError on non-zero exit (e.g. the
    artifact does not exist -- callers decide whether that's fatal).
    """
    if shutil.which(ARTIFACT_BINARY) is None:
        raise FileNotFoundError(ARTIFACT_NOT_INSTALLED_MSG)
    log.info("Pulling Semaphore project artifact %r into %s", name, dest_dir)
    _run_artifact(
        [ARTIFACT_BINARY, "pull", "project", name,
         "--destination", dest_dir, "--force"],
    )


def yank_project_artifact(name: str) -> None:
    """Delete project artifact `name` from the artifact store.

    Used as the lock-release primitive. Raises FileNotFoundError if the
    `artifact` binary is not on PATH; raises subprocess.CalledProcessError
    on non-zero exit (e.g. artifact already gone -- callers may choose
    to log-and-continue rather than abort).
    """
    if shutil.which(ARTIFACT_BINARY) is None:
        raise FileNotFoundError(ARTIFACT_NOT_INSTALLED_MSG)
    log.info("Yanking Semaphore project artifact %r", name)
    _run_artifact(
        [ARTIFACT_BINARY, "yank", "project", name],
    )
