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

from unittest.mock import patch

import pytest

from translation_agent import r2


def test_check_r2_available_raises_when_missing():
    with patch.object(r2.shutil, "which", return_value=None):
        with pytest.raises(FileNotFoundError, match="not found on PATH"):
            r2.check_r2_available()


def test_check_r2_available_passes_when_present():
    with patch.object(r2.shutil, "which", return_value="/usr/local/bin/r2"):
        r2.check_r2_available()


def test_run_r2_claude_invokes_subprocess():
    fake_completed = type(
        "CP", (), {"returncode": 0, "stdout": "ok", "stderr": ""}
    )()
    with patch.object(r2.shutil, "which", return_value="/usr/local/bin/r2"), \
         patch.object(r2.subprocess, "run", return_value=fake_completed) as mrun:
        result = r2.run_r2_claude("hello")
    mrun.assert_called_once_with(
        ["r2", "sandbox", "claude", "-p", "hello"],
        capture_output=True,
        text=True,
        timeout=None,
    )
    assert result.returncode == 0
    assert result.stdout == "ok"
    assert result.stderr == ""
