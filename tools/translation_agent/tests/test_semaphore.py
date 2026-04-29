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
        ["artifact", "push", "project", "--force", "td", "/tmp/db.sqlite"],
        check=True,
    )
