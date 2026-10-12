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

"""Fixtures shared by the unit test modules."""

import asyncio
import sys
import threading
from collections.abc import Callable, Coroutine, Iterator
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

# How long a held completion waits for its release.
_HOLD_TIMEOUT = 10.0


@pytest.fixture
def two_gib_bytes():
    """A zero-filled ``bytes`` of exactly 2 GiB (``2**31`` bytes): one byte more
    than the ``int32_t`` lengths of the C API can carry.

    ``bytes(n)`` is allocated with ``calloc``, so its pages are mapped lazily and
    the buffer costs almost no resident memory. It needs a 64-bit interpreter,
    and a test using it is skipped rather than failed where it cannot be
    allocated.
    """
    if sys.maxsize <= 2**32:
        pytest.skip("a 2 GiB buffer needs a 64-bit Python")
    try:
        return bytes(2**31)
    except MemoryError:
        pytest.skip("could not allocate a 2 GiB buffer")


class HeldCompletion:
    """The completion of one entry point of the C extension, held on the native
    thread that delivers it until :attr:`release` is set: the test decides when
    the result reaches the client's callback (and so its event loop).

    ``payloads`` holds the results as the C trampoline passes them, ``raised``
    what the client's callback raised back to the trampoline (which prints it
    and drops it), and ``delivered`` is set once the callback has returned."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, name: str) -> None:
        self.submitted = threading.Event()
        self.release = threading.Event()
        self.delivered = threading.Event()
        self.payloads: list[tuple[Any, ...]] = []
        self.raised: list[BaseException] = []
        real = getattr(_lib, name)

        def entry(*args: Any) -> Any:
            *rest, cb = args

            def held(*payload: Any) -> None:
                self.payloads.append(payload)
                self.release.wait(_HOLD_TIMEOUT)
                try:
                    cb(*payload)
                except BaseException as exc:
                    self.raised.append(exc)
                    raise
                finally:
                    self.delivered.set()

            result = real(*rest, held)
            self.submitted.set()
            return result

        monkeypatch.setattr(_lib, name, entry)


@pytest.fixture
def hold_completion(monkeypatch: pytest.MonkeyPatch) -> Callable[[str], HeldCompletion]:
    """``hold_completion(name)`` holds the completions of ``_lib.<name>``, whose
    last argument is the completion callback (see :class:`HeldCompletion`)."""
    return lambda name: HeldCompletion(monkeypatch, name)


@pytest.fixture
def freed_errors(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """The error handles freed with ``KafkaError_destroy`` during the test, in
    order (each is still freed)."""
    freed: list[int] = []
    real = _lib.KafkaError_destroy

    def destroy(handle: int) -> None:
        freed.append(handle)
        real(handle)

    monkeypatch.setattr(_lib, "KafkaError_destroy", destroy)
    return freed


class StoppedLoop:
    """A fresh event loop that runs a call until it waits, then stops, as
    ``loop.run_until_complete(...)`` returning while another task of the loop
    still waits: the call's task is left pending on the stopped loop."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._task: asyncio.Task[Any] | None = None

    def start(self, call: Coroutine[Any, Any, Any]) -> None:
        self._task = self.loop.create_task(call)
        self.loop.run_until_complete(asyncio.sleep(0))

    def abandon(self) -> None:
        """Close the loop, then do to the task what the garbage collector does
        to a task left on a closed loop: close its coroutine, which runs its
        ``finally`` blocks (an awaiting consumer call gives its use of the
        consumer back), without the "destroyed but pending" report."""
        if not self.loop.is_closed():
            self.loop.close()
        task, self._task = self._task, None
        if task is not None:
            task._log_destroy_pending = False  # noqa: SLF001
            task.get_coro().close()


@pytest.fixture
def stopped_loop() -> Iterator[StoppedLoop]:
    """A :class:`StoppedLoop`, abandoned at the end of the test."""
    loop = StoppedLoop()
    yield loop
    loop.abandon()
