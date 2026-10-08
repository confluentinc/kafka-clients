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

"""The synchronous clients' waiter over the C API's queued (``_cb``) entry points.

The synchronous ``Producer`` / ``Consumer`` / ``Admin`` classes do not call the
blocking C entry points. A blocking entry point parks the calling thread inside
a native ``block_on`` where CPython cannot run signal handlers, so ``Ctrl-C``
would only take effect once the call returned (after a whole ``poll`` timeout,
say). Instead every blocking-in-Java operation is submitted through its ``_cb``
twin and the caller waits here, in Python, in short slices: the interpreter
stays reachable between slices and a pending ``KeyboardInterrupt`` is raised
promptly.

How a wait proceeds:

1. ``submit(cb)`` issues the ``_cb`` entry point with ``cb`` as its completion
   callback. Rust queues the completion on the client's callbacks vector; the
   client's ``set_callbacks_notify`` hook -- :meth:`SyncWaiter.notify`, which
   only sets a ``threading.Event`` -- fires once each time that vector goes
   from empty to non-empty.
2. The waiter sleeps on the event in 100 ms slices. When it is set, the waiter
   clears it and drains the vector with the client's ``execute_callbacks`` on
   the *calling* thread, so completions, delivery callbacks, rebalance
   listeners and commit callbacks all run on the thread that made the call --
   Java's threading model for these callbacks.
3. The wait ends when ``cb`` has run. A ``KeyboardInterrupt`` raised while
   waiting calls ``on_interrupt()`` once (the consumer passes ``wakeup``, which
   aborts the in-flight operation; the producer and admin have no equivalent
   and let the operation finish), keeps waiting for the completion so the
   client is left in a consistent state (the single-owner guard released, the
   payload freed), and then re-raises the interrupt.

The slice length bounds the latency of a completion by at most one slice only
when the notify hook has fired *between* a drain and the next wait; the common
case wakes the event immediately. The 100 ms slices (rather than an unbounded
``Event.wait()``) keep the wait interruptible on every platform, including
those where a blocking lock acquire is not interrupted by signals.

A client whose C layer has its own wake-up primitive subclasses
:class:`SyncWaiter` and overrides :meth:`SyncWaiter.wait_slice` with one
bounded call into it, leaving :meth:`SyncWaiter.notify` unused (the producer:
``Producer_poll`` waits on the condition variable the extension's notify hook
broadcasts, see ``producer._ProducerWaiter``). :meth:`SyncWaiter.run` only
relies on ``wait_slice`` returning within about one slice.
"""

import threading

__all__ = ["SyncWaiter"]

_SLICE_SECONDS = 0.1


class SyncWaiter:
    """Drives one client's callback pump from a synchronous caller.

    Args:
        execute_callbacks: ``callable() -> int`` running the client's queued
            callbacks on the calling thread and returning how many ran
            (``<Client>_execute_callbacks``). It must tolerate being called
            after the client was destroyed (return ``0``).
    """

    __slots__ = ("_execute_callbacks", "_event")

    def __init__(self, execute_callbacks):
        self._execute_callbacks = execute_callbacks
        self._event = threading.Event()

    def notify(self):
        """The client's ``set_callbacks_notify`` hook. It only signals: it runs
        on whichever thread queued the first callback (a Rust task, or the
        calling thread when a completion is queued synchronously) and must
        never run callbacks itself."""
        self._event.set()

    def drain(self):
        """Run every queued callback on the calling thread; returns how many
        ran in total."""
        total = 0
        while True:
            n = self._execute_callbacks()
            if n <= 0:
                return total
            total += n

    def wait_slice(self, timeout=_SLICE_SECONDS):
        """Wait up to ``timeout`` seconds for the notify hook, then drain.
        Returns how many callbacks ran. Interruptible between slices."""
        if self._event.wait(timeout):
            # Clear BEFORE draining so a notification that races the drain is
            # not lost; one already consumed by the drain costs at most a
            # spurious early wake-up.
            self._event.clear()
        return self.drain()

    def run(self, submit, on_interrupt=None):
        """Submit a ``_cb`` operation and block until its completion ran.

        ``submit(cb)`` must call the ``_cb`` entry point with ``cb`` as its
        completion callback; ``cb`` may be invoked inline (a completion that is
        already available, a mock) or later from :meth:`drain`. Returns the
        tuple of arguments the completion was called with.

        On ``KeyboardInterrupt`` calls ``on_interrupt()`` once (if given), keeps
        waiting for the completion, then re-raises the interrupt; the payload
        of an interrupted operation is dropped.
        """
        box = []

        def cb(*payload):
            box.append(payload)

        submit(cb)
        interrupted = None
        while not box:
            try:
                self.wait_slice()
            except KeyboardInterrupt as exc:
                if interrupted is None and on_interrupt is not None:
                    on_interrupt()
                interrupted = exc
        if interrupted is not None:
            raise interrupted
        return box[0]
