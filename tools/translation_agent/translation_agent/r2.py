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

"""Wrapper around `r2 sandbox claude` invocations.

Phase A only provides the non-streaming variant (`run_r2_claude`). Phase C
will add a Popen-based streaming wrapper that buffers stdout and flushes
every 100 lines with the `>>>>> From agent #<pr_number>` prefix.
"""

import shutil
import subprocess
from dataclasses import dataclass


R2_BINARY = "r2"
R2_NOT_INSTALLED_MSG = (
    "`r2` binary not found on PATH. The translation agent requires r2 to "
    "spawn sandboxed Claude Code instances. See README.md for installation."
)


@dataclass
class R2Result:
    returncode: int
    stdout: str
    stderr: str


def check_r2_available() -> None:
    """Raise FileNotFoundError if the `r2` binary is not on PATH."""
    if shutil.which(R2_BINARY) is None:
        raise FileNotFoundError(R2_NOT_INSTALLED_MSG)


def run_r2_claude(prompt: str, timeout: float | None = None) -> R2Result:
    """Run `r2 sandbox claude -p <prompt>` and return its result.

    Blocking call; not for use with the line-streaming Phase C/D path. This
    is the simple non-streaming variant intended for design step 4
    (dependency evaluation), where the inner Claude is expected to emit a
    single JSON object.
    """
    check_r2_available()
    proc = subprocess.run(
        [R2_BINARY, "sandbox", "claude", "-p", prompt],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return R2Result(returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)
