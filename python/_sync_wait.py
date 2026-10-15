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

"""The synchronous admin clients' waiter over the C API's queued (``_cb``) entry points.

The synchronous ``AdminClient`` / ``MockAdminClient`` of ``admin.py`` wait here;
the ``confluent_kafka`` package's synchronous producer and consumer follow the
same rule with waits of their own (the producer's completion thread and
``wait_sliced``, the consumer's ``_run_sync``). None of them calls the
blocking C entry points. A blocking entry point parks the calling thread inside
a native ``block_on`` where CPython cannot run signal handlers, so ``Ctrl-C``
would only take effect once the call returned. Instead every blocking-in-Java
operation is submitted through its ``_cb`` twin and the caller waits here, in
Python, in short slices: the interpreter stays reachable between slices and a
pending ``KeyboardInterrupt`` is raised promptly.

How a wait proceeds:

1. ``submit(cb)`` issues the ``_cb`` entry point with ``cb`` as its completion
   callback. Rust queues the completion on the client's callbacks vector; the
   client's ``set_callbacks_notify`` hook -- :meth:`SyncWaiter.notify`, which
   only sets a ``threading.Event`` -- fires once each time that vector goes
   from empty to non-empty.
2. The client's one pump thread wakes on that event, clears it and drains the
   vector with the client's ``execute_callbacks``. The drain keeps the GIL from
   the first callback to the last, so it must have a single drainer: a second
   thread entering it would block inside the native call with the GIL held
   while the first waits for the GIL. The pump thread is that drainer, however
   many threads wait at once.
3. The completion ``cb`` stores its payload and sets the waiting call's own
   event; the caller sleeps on that event in 100 ms slices. A
   ``KeyboardInterrupt`` raised while waiting calls ``on_interrupt()`` once, if
   given (a client with a ``wakeup`` would pass it to abort the in-flight
   operation; admin has none and lets the operation finish), keeps waiting for
   the completion so the client is left in a consistent state (the payload
   freed), and then re-raises the interrupt.

The 100 ms slices (rather than an unbounded ``Event.wait()``) keep the wait
interruptible on every platform, including those where a blocking lock acquire
is not interrupted by signals. The pump thread itself waits unbounded: it
never needs to see a signal.
"""

import logging
import threading

__all__ = ["SyncWaiter"]

_SLICE_SECONDS = 0.1

_LOG = logging.getLogger("confluent_kafka.admin")


class SyncWaiter:
    """Runs one client's callback pump on its own thread and waits for the
    completions of synchronous callers.

    Args:
        execute_callbacks: ``callable() -> int`` running the client's queued
            callbacks on the calling thread and returning how many ran
            (``<Client>__execute_callbacks``). It must tolerate being called
            after the client was destroyed (return ``0``).
        name: the pump thread's name.
    """

    __slots__ = ("_execute_callbacks", "_event", "_name", "_stopped", "_thread")

    def __init__(self, execute_callbacks, name):
        self._execute_callbacks = execute_callbacks
        self._event = threading.Event()
        self._name = name
        self._stopped = False
        self._thread = None

    def start(self):
        """Start the pump thread, once the client's notify hook is set."""
        self._thread = threading.Thread(target=self._pump, name=self._name, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the pump thread and wait for it to end, its running drain
        included; the callbacks still queued are left for the client's
        ``_destroy``, which runs them. Called from the pump thread itself it
        does not wait."""
        thread = self._thread
        if thread is None:
            return
        self._stopped = True
        self._event.set()
        if thread is not threading.current_thread():
            thread.join()

    def notify(self):
        """The client's ``set_callbacks_notify`` hook. It only signals: it runs
        on whichever thread queued the first callback (a Rust task, or the
        calling thread when a completion is queued synchronously) and must
        never run callbacks itself."""
        self._event.set()

    def _pump(self):
        while True:
            self._event.wait()
            # Clear BEFORE draining so a notification that races the drain is
            # not lost; one already consumed by the drain costs at most a
            # spurious wake-up.
            self._event.clear()
            if self._stopped:
                return
            try:
                while self._execute_callbacks() > 0:
                    pass
            except Exception:  # noqa: BLE001 - the pump thread has no caller
                _LOG.exception("Error running the admin client's callbacks")

    def run(self, submit, on_interrupt=None):
        """Submit a ``_cb`` operation and block until its completion ran.

        ``submit(cb)`` must call the ``_cb`` entry point with ``cb`` as its
        completion callback; ``cb`` may be invoked inline (a completion that is
        already available, a mock) or later on the pump thread. Returns the
        tuple of arguments the completion was called with.

        On ``KeyboardInterrupt`` calls ``on_interrupt()`` once (if given), keeps
        waiting for the completion, then re-raises the interrupt; the payload
        of an interrupted operation is dropped.
        """
        box = []
        done = threading.Event()

        def cb(*payload):
            box.append(payload)
            done.set()

        submit(cb)
        interrupted = None
        while not done.is_set():
            try:
                done.wait(_SLICE_SECONDS)
            except KeyboardInterrupt as exc:
                if interrupted is None and on_interrupt is not None:
                    on_interrupt()
                interrupted = exc
        if interrupted is not None:
            raise interrupted
        return box[0]
