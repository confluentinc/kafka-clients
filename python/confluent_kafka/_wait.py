# Copyright 2026 Confluent Inc.
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

"""Waits in short slices, shared by the sync clients and ``KafkaFuture`` (private).

A sync wait sleeps in :data:`WAIT_SLICE_S` slices instead of one long block, so
a Ctrl+C is seen promptly on every platform (CLAUDE.md, Python Binding
Conventions, Class family and Threads and callbacks). Two policies:

- :func:`wait_sliced` / :func:`result_sliced` wait for an operation of a client
  that goes on after a Ctrl+C (a producer's handover, flush, close): the first
  interrupt is raised once the wait is over.
- :func:`done_within` waits for a ``KafkaFuture``: a Ctrl+C ends the wait at
  once, as Java's ``Future.get()`` throws ``InterruptedException`` when its
  thread is interrupted; nothing is left to finish, since the future only
  waits.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, wait
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any, TypeVar

__all__ = ["WAIT_SLICE_S", "done_within", "result_sliced", "wait_sliced"]

#: The slice a sync wait sleeps in, so Ctrl+C is seen promptly.
WAIT_SLICE_S = 0.1

_T = TypeVar("_T")


def wait_sliced(event: threading.Event, deadline: float | None = None,
                on_interrupt: Callable[[], None] | None = None) -> bool:
    """Wait for ``event`` in :data:`WAIT_SLICE_S` slices, up to ``deadline``
    (``time.monotonic()``) when given; whether it was set.

    A Ctrl+C (``KeyboardInterrupt``) does not end the wait: the operation goes
    on, as the producer lets an operation finish, and the first interrupt is
    raised once the wait is over (a second one meanwhile is dropped, as Java
    keeps only the first interrupt). ``on_interrupt`` runs at the first one."""
    interrupted: KeyboardInterrupt | None = None
    try:
        while True:
            try:
                while not event.wait(WAIT_SLICE_S):
                    if deadline is not None and time.monotonic() >= deadline:
                        return False
                return True
            except KeyboardInterrupt as exc:
                if interrupted is None:
                    interrupted = exc
                    if on_interrupt is not None:
                        on_interrupt()
    finally:
        if interrupted is not None:
            raise interrupted


def result_sliced(future: Future[_T]) -> _T:
    """``future.result()``, waited for as :func:`wait_sliced` waits: in
    :data:`WAIT_SLICE_S` slices, a Ctrl+C letting the wait end and then raised
    instead of the result."""
    interrupted: KeyboardInterrupt | None = None
    try:
        while True:
            try:
                return future.result(WAIT_SLICE_S)
            except FutureTimeoutError:
                continue
            except KeyboardInterrupt as exc:
                if interrupted is None:
                    interrupted = exc
    finally:
        if interrupted is not None:
            raise interrupted


def done_within(future: Future[Any], timeout: float | None) -> bool:
    """Wait until ``future`` is done, in :data:`WAIT_SLICE_S` slices, for at
    most ``timeout`` seconds (``None``: no bound); whether it is done.

    Never raises the future's own outcome (the caller reads it), and a
    ``KeyboardInterrupt`` ends the wait at once, as Java's ``Future.get()``
    throws ``InterruptedException``."""
    if future.done():
        return True
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        slice_s = WAIT_SLICE_S
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return future.done()
            slice_s = min(slice_s, remaining)
        # ``wait`` returns at completion without raising the outcome; a future
        # cancelled without ``set_running_or_notify_cancel`` is only ``done()``.
        wait((future,), slice_s)
        if future.done():
            return True
