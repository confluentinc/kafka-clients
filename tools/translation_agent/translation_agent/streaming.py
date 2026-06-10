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

import queue
import subprocess
import sys
import threading
import time
from typing import List, Optional, TextIO, Tuple


FLUSH_EVERY = 100
PREFIX_TEMPLATE = ">>>>> From agent #{pr_number}"

# Sentinel pushed onto the line queue by the reader task when stdout reaches
# EOF (the child closed its output). Distinguishes "stream ended" from "no
# line arrived within the poll window" without a separate flag.
_EOF = object()


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

    `timeout` (seconds) is a wall-clock deadline on the whole run. It is
    enforced even when the child produces no output: reading happens on a
    dedicated task and the main loop waits on a queue with the remaining
    budget, so a hung `r2` that stops emitting newlines (but never exits)
    still raises `subprocess.TimeoutExpired` instead of blocking forever.
    Iterating `proc.stdout` directly cannot do this -- it blocks until the
    child closes the stream.

    Returns (returncode, captured_output). Raises subprocess.TimeoutExpired
    (after killing the child) when the deadline elapses.
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

    # Drain stdout on a dedicated task so the main loop can honour the
    # deadline regardless of whether the child is producing output.
    line_q: "queue.Queue" = queue.Queue()

    def reader() -> None:
        try:
            for line in proc.stdout:
                line_q.put(line)
        finally:
            line_q.put(_EOF)

    reader_task = threading.Thread(target=reader, daemon=True)
    reader_task.start()

    deadline = None if timeout is None else time.monotonic() + timeout

    def kill_and_drain() -> None:
        proc.kill()
        proc.wait()
        # Best-effort join only: a grandchild that inherited the stdout pipe
        # (e.g. `sh -c "sleep 30"` keeps the write end open after sh dies)
        # would otherwise block the reader on EOF indefinitely. The reader is
        # a daemon task, so it's fine to abandon it -- it dies with the
        # interpreter and we've already captured everything that arrived.
        reader_task.join(timeout=1.0)
        flush_buffer()

    try:
        while True:
            if deadline is None:
                remaining = None
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    kill_and_drain()
                    raise subprocess.TimeoutExpired(
                        cmd, timeout, output="".join(captured),
                    )
            try:
                item = line_q.get(timeout=remaining)
            except queue.Empty:
                kill_and_drain()
                raise subprocess.TimeoutExpired(
                    cmd, timeout, output="".join(captured),
                )
            if item is _EOF:
                break
            captured.append(item)
            buffer.append(item)
            if len(buffer) >= FLUSH_EVERY:
                flush_buffer()
        flush_buffer()
        rc = proc.wait()
    except BaseException:
        # Never leak the child on any unexpected error path.
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        raise
    reader_task.join()
    return rc, "".join(captured)
