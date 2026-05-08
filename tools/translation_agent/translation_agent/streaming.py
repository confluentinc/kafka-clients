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

"""Subprocess wrapper that streams output with a per-PR prefix.

Per design steps 6 and 7, output should be flushed every 100 lines and
written to stdout preceded by ">>>>> From agent #<pr_number>". We merge
stderr into stdout so a verbose inner Claude can't deadlock on a
stderr-pipe full buffer (we'd never drain it). The merged stream is both
streamed live and captured for downstream parsing.
"""

import subprocess
import sys
from typing import List, Optional, TextIO, Tuple


FLUSH_EVERY = 100
PREFIX_TEMPLATE = ">>>>> From agent #{pr_number}"


def run_with_prefix(
    cmd: List[str],
    pr_number: int,
    cwd: Optional[str] = None,
    timeout: Optional[float] = None,
    out_stream: Optional[TextIO] = None,
) -> Tuple[int, str]:
    """Run cmd as a subprocess and stream its merged stdout+stderr.

    Output is flushed to `out_stream` (default sys.stdout) in batches of
    FLUSH_EVERY lines, each batch preceded by the prefix line. The full
    captured output is also returned so callers can parse it.

    Returns (returncode, captured_output).
    """
    if out_stream is None:
        out_stream = sys.stdout
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    captured: List[str] = []
    buffer: List[str] = []
    prefix_line = PREFIX_TEMPLATE.format(pr_number=pr_number) + "\n"

    def flush_buffer() -> None:
        if not buffer:
            return
        out_stream.write(prefix_line)
        out_stream.writelines(buffer)
        out_stream.flush()
        buffer.clear()

    assert proc.stdout is not None
    try:
        for line in proc.stdout:
            captured.append(line)
            buffer.append(line)
            if len(buffer) >= FLUSH_EVERY:
                flush_buffer()
        flush_buffer()
        rc = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        flush_buffer()
        raise
    return rc, "".join(captured)
