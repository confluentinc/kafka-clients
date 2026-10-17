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

"""Private: awaiting work that must end even when the awaiting task is
cancelled, handing a native result to the event loop awaiting it, and keeping
an async client's callback pump on one thread."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from typing import Any, TypeVar

_T = TypeVar("_T")

# A native result as its C trampoline passes it, and the function freeing its
# handles.
_Payload = tuple[Any, ...]
_Free = Callable[[_Payload], None]


async def await_to_end(future: asyncio.Future[_T]) -> _T:
    """Await ``future`` to its end, even if the awaiting task is cancelled
    meanwhile, then raise the first cancellation: cancelling the task awaiting
    a call lets the call end, then raises ``CancelledError``.

    For work that goes on once started, such as a close's teardown in an
    executor. ``asyncio.shield`` alone keeps that work going but ends the wait,
    so the caller would carry on while it still runs. A cancellation wins over
    the work's own error, which asyncio then logs as never retrieved."""
    cancelled: asyncio.CancelledError | None = None
    while not future.done():
        try:
            # asyncio.wait neither cancels nor raises from the future.
            await asyncio.wait((future,))
        except asyncio.CancelledError as exc:
            if cancelled is None:
                cancelled = exc
    if cancelled is not None:
        raise cancelled
    return future.result()


def call_soon_on(loop: asyncio.AbstractEventLoop, callback: Callable[..., object],
                 *args: Any) -> None:
    """Schedule ``callback(*args)`` on ``loop`` from any thread, raising
    ``RuntimeError`` when the loop is closed, as ``call_soon_threadsafe`` does.

    On the loop's own thread -- a completion run by the client's pump on its
    loop, the common case -- it is ``loop.call_soon``: the running loop needs
    no wake-up, and ``call_soon_threadsafe``'s wake-up writes to the loop's
    self-pipe, a socket send that releases the GIL in the middle of the
    client's drain (see :class:`LoopBinding`)."""
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if running is loop:
        loop.call_soon(callback, *args)
    else:
        loop.call_soon_threadsafe(callback, *args)


class Undelivered:
    """The native results of a client's async operations on their way to the
    event loop awaiting them: handed to it with :func:`call_soon_on` on the
    thread they arrived on, and not run on the loop yet.

    A result that does not reach its loop would keep its native handles for
    good: :func:`call_soon_on` refuses it with ``RuntimeError`` when the
    loop closes between the ``is_closed()`` check and the call, and
    ``loop.close()`` discards the calls of a loop that stopped before it ran
    them. So each result is kept here, per loop, until its delivery runs: a
    refused one is freed at once, and :meth:`free_closed`, which the client
    calls at the start of each async operation and in ``close()``, frees those
    left for a loop that has closed. A result is freed with its operation's
    own function, the one that frees the result of a cancelled call, so who
    owns which handle does not change."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_loop: dict[asyncio.AbstractEventLoop, list[tuple[_Payload, _Free]]] = {}

    def hand_over(self, loop: asyncio.AbstractEventLoop, deliver: Callable[[_Payload], None],
                  payload: _Payload, free: _Free) -> None:
        """Run ``deliver(payload)`` on ``loop``, or ``free(payload)`` if
        ``loop`` has closed. Called on the thread the result arrived on."""
        if loop.is_closed():
            free(payload)
            return
        with self._lock:
            self._by_loop.setdefault(loop, []).append((payload, free))
        try:
            call_soon_on(loop, self._deliver, loop, deliver, payload)
        except RuntimeError:  # the loop closed since the check
            if self._take(loop, payload):
                free(payload)

    def free_closed(self) -> None:
        """Free the results left for event loops that have closed: no delivery
        of theirs will run."""
        with self._lock:
            closed = [loop for loop in self._by_loop if loop.is_closed()]
            stranded = [entry for loop in closed for entry in self._by_loop.pop(loop)]
        for payload, free in stranded:
            free(payload)

    def _deliver(self, loop: asyncio.AbstractEventLoop, deliver: Callable[[_Payload], None],
                 payload: _Payload) -> None:
        if self._take(loop, payload):
            deliver(payload)

    def _take(self, loop: asyncio.AbstractEventLoop, payload: _Payload) -> bool:
        """Remove ``payload`` (this very tuple) from the results kept for
        ``loop``; whether it was still there, so exactly one of its delivery,
        its refusal and :meth:`free_closed` takes it."""
        with self._lock:
            entries = self._by_loop.get(loop, [])
            for i, (kept, _free) in enumerate(entries):
                if kept is payload:
                    del entries[i]
                    if not entries:
                        del self._by_loop[loop]
                    return True
        return False


_LOG = logging.getLogger("confluent_kafka")


class LoopBinding:
    """The one event loop an async client runs on.

    The client's queued native callbacks (``<Client>__execute_callbacks``) run
    with the GIL held from the first to the last, so they must have a single
    drainer: a second thread entering the drain would block inside the native
    call with the GIL held, while the first waits for the GIL to finish its
    callback. An async client drains on its loop, so it is bound to one: the
    loop running when it is constructed, or else the loop of its first async
    call. A call from another loop raises ``RuntimeError``, as asyncio's own
    primitives do once bound to a loop."""

    __slots__ = ("_owner", "_loop", "_lock")

    def __init__(self, owner: str) -> None:
        self._owner = owner
        self._lock = threading.Lock()
        try:
            self._loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None

    @property
    def loop(self) -> asyncio.AbstractEventLoop | None:
        """The bound loop, or ``None`` before the first async call."""
        return self._loop

    def bind(self) -> asyncio.AbstractEventLoop:
        """The running loop, bound on the first call; raises ``RuntimeError``
        when it is not the bound one. Called on the loop's thread at the start
        of every async call."""
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._loop is None:
                self._loop = loop
            elif self._loop is not loop:
                raise RuntimeError(f"{self._owner} is bound to a different event loop")
        return loop


class OffLoopPump:
    """Runs a client's callback pump on a helper thread once its event loop
    has closed, so the completions still queued for it end (each dropped and
    reported) instead of waiting for a loop that will not run again.

    One helper at a time: a request while one runs makes it pump once more
    when it would otherwise end, so the pump keeps a single drainer (see
    :class:`LoopBinding`). The loop, once closed, never pumps again, so the
    helper never overlaps it either."""

    __slots__ = ("_pump", "_name", "_lock", "_running", "_again")

    def __init__(self, pump: Callable[[], object], name: str) -> None:
        self._pump = pump
        self._name = name
        self._lock = threading.Lock()
        self._running = False
        self._again = False

    def start(self) -> None:
        """Pump on the helper thread, starting it unless it already runs."""
        with self._lock:
            if self._running:
                self._again = True
                return
            self._running = True
        threading.Thread(target=self._run, name=self._name, daemon=True).start()

    def _run(self) -> None:
        while True:
            try:
                self._pump()
            except Exception:  # noqa: BLE001 - a helper thread has no caller
                _LOG.exception("Error running the callback pump")
            with self._lock:
                if not self._again:
                    self._running = False
                    return
                self._again = False
