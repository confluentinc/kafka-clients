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


def push_project_artifact(name: str, file_path: str) -> None:
    """Push file_path to the Semaphore project-level artifact `name`.

    Raises FileNotFoundError if the `artifact` binary is not on PATH; raises
    subprocess.CalledProcessError on non-zero exit.
    """
    if shutil.which(ARTIFACT_BINARY) is None:
        raise FileNotFoundError(ARTIFACT_NOT_INSTALLED_MSG)
    log.info("Pushing %s to Semaphore project artifact %r", file_path, name)
    subprocess.run(
        [ARTIFACT_BINARY, "push", "project", file_path, "--force"],
        check=True,
    )
