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

"""``KafkaFuture`` and ``AsyncKafkaFuture``: Java's ``org.apache.kafka.common.KafkaFuture``.

Every future the package hands out is one of these: :class:`KafkaFuture` from a
sync class, :class:`AsyncKafkaFuture` from an asyncio one (CLAUDE.md, Python
Binding Conventions, Futures). Both have Java's members in Java's order;
``get`` is ``async def`` on :class:`AsyncKafkaFuture`.

Two value sources sit behind one surface:

- a C ``kafka_common_KafkaFuture_t`` handle, through the extension's future
  object (a client's future): it answers ``is_done`` itself and delivers its
  value through ``get_cb``, on the thread that runs the owning client's
  callbacks (the producer's completion thread, the sync admin client's pump
  thread, the event loop of an asyncio client), or inline for a future of no
  client;
- :class:`_KafkaFutureImpl`, Java's ``KafkaFutureImpl``: Python state, the
  value source of ``completed_future``, of the dependants (``then_apply``,
  ``when_complete``, ``all_of``) and of the mocks' futures. Its behaviour is
  the mixin :class:`_FutureState`, so a future can be its own state: the
  asyncio producer's record is one (``async_producer._AsyncRecord``).

A future over a C handle is lazy: it is three slots, and the Python state that
mirrors the handle (a :class:`_KafkaFutureImpl` the ``get_cb`` delivery
completes) and the inner ``concurrent.futures.Future`` / ``asyncio.Future`` are
made by the first operation that needs the completion delivered: a wait,
``get_now`` on a done handle, ``then_apply``, ``when_complete``,
``to_completion_stage()``, ``cancel``, or becoming a source of ``all_of``.
Until then ``is_done()`` reads the handle, and the other state reads settle
the Python state from a done handle at once (the C future cannot be
cancelled, so a handle not done answers ``False`` to both).

The extension wraps only C futures whose C ``get`` does not drain a client's
callback queue (every admin future): a done handle is then read at once
through ``get_if_done``, on any thread, without waiting and without running
the client's callbacks, which is how ``get``, ``get_now`` and the state reads
answer a done handle before its ``get_cb`` delivery has run. The producers'
``send()`` futures are backed by Python state instead: a
:class:`_KafkaFutureImpl` on the sync producers, the record object itself on
the asyncio ones.

The cancel family is Python-side, as Java's ``KafkaFutureImpl``: ``cancel()``
cancels the future and, through it, every dependant built from it, never its
source, the request or the core.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import math
import threading
import time
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, SupportsIndex, TypeVar, overload

from confluent_kafka import _throwable
from confluent_kafka._args import MISSING
from confluent_kafka._native_copy import not_copyable
from confluent_kafka._wait import WAIT_SLICE_S, done_within

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from confluent_kafka import Duration

__all__ = ["AsyncKafkaFuture", "KafkaFuture"]

T = TypeVar("T")
R = TypeVar("R")
U = TypeVar("U")
_A = TypeVar("_A")
_B = TypeVar("_B")

_LONG_MIN = -(1 << 63)
_LONG_MAX = (1 << 63) - 1

# The core's FFI id of a foreign callback error (`kafka_common_Error_LOCAL_CALLBACK`),
# which carries the Python exception a completer gave.
_LOCAL_CALLBACK = -30

# A _FutureState's states.
_PENDING = 0
_VALUE = 1
_ERROR = 2
_CANCELLED = 3

# Java's CancellationException, either kind.
_CANCELLED_ERRORS = (concurrent.futures.CancelledError, asyncio.CancelledError)

# Guards the lazy creation of a future's Python state and inner future.
_LAZY_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Python state: Java's KafkaFutureImpl
# ---------------------------------------------------------------------------


class _FutureState(Generic[T]):
    """Java's ``KafkaFutureImpl`` as a mixin: the behaviour of a future
    completed from Python, from any thread, over four attributes the class
    mixing it in stores: ``_lock`` (a ``threading.Lock``, its own or a shared
    one), ``_state`` (``_PENDING`` to ``_CANCELLED``), ``_outcome`` (the value,
    or the error) and ``_listeners``. :class:`_KafkaFutureImpl` is the state
    standing alone; the asyncio producer's record is a future that is its own
    state (``async_producer._AsyncRecord``), one object per send.

    Its listeners (dependants, the inner future, ``when_complete``) run where
    the completion runs: inside :meth:`complete` / :meth:`complete_with_error`
    / :meth:`cancel`, or inside the registering call when it is already done,
    as Java's ``CompletableFuture`` runs them. An error that is a
    ``CancelledError`` (Java's ``CancellationException``) cancels the future,
    as ``CompletableFuture.completeExceptionally`` does.

    Java's ``KafkaFutureImpl`` wraps and unwraps ``CompletionException`` to
    compensate for ``CompletableFuture``'s own unwrapping; nothing here wraps an
    error, so the stored error is always the one given, which is what Java's
    compensation achieves.
    """

    # No slots of its own: a class mixing it in beside another slotted base
    # would otherwise conflict in layout. The four attributes are declared
    # here and stored by the class mixing it in, so mypy's slot check flags
    # their assignment below (hence the ``misc`` ignores).
    __slots__ = ()

    _lock: threading.Lock
    _state: int
    _outcome: Any
    _listeners: list[Callable[[_FutureState[T]], None]] | None

    # -- completion ------------------------------------------------------------

    def _settle(self, state: int, outcome: Any) -> bool:
        with self._lock:
            if self._state != _PENDING:
                return False
            self._state = state  # type: ignore[misc]
            self._outcome = outcome
            listeners = self._listeners
            self._listeners = None  # type: ignore[misc]
        if listeners:
            # Every listener runs, so sibling dependants, the inner future and
            # all_of's count always complete; the first error a listener let
            # through (a KeyboardInterrupt) is raised after the last one.
            first: BaseException | None = None
            for listener in listeners:
                try:
                    listener(self)
                except BaseException as error:  # noqa: BLE001 - re-raised below
                    if first is None:
                        first = error
            if first is not None:
                raise first
        return True

    def complete(self, value: T) -> bool:
        """If not already completed, sets the value returned by ``get()`` and
        related methods to the given value."""
        return self._settle(_VALUE, value)

    def complete_with_error(self, error: BaseException) -> bool:
        """Java's ``completeExceptionally``: if not already completed, causes
        invocations of ``get()`` and related methods to raise the given error
        (as the cause of an ``ExecutionError``); a ``CancelledError`` cancels."""
        if isinstance(error, _CANCELLED_ERRORS):
            return self._settle(_CANCELLED, error)
        return self._settle(_ERROR, error)

    def cancel(self, error: BaseException) -> bool:
        """If not already completed, completes this future with ``error``, a
        ``CancelledError``; dependants not yet completed are cancelled too."""
        return self._settle(_CANCELLED, error)

    def _deliver_from_c(self, value: Any, error: Any) -> None:
        """The ``get_cb`` completion of the extension's future object: the
        converted value, or the error as an owned FFI handle (an ``int``) or
        the exception the value's converter raised."""
        if error is None:
            self.complete(value)
        elif isinstance(error, BaseException):
            self.complete_with_error(error)
        else:
            self.complete_with_error(_error_from_c(error))

    # -- state -----------------------------------------------------------------

    def is_done(self) -> bool:
        return self._state != _PENDING

    def is_cancelled(self) -> bool:
        return self._state == _CANCELLED

    def is_completed_exceptionally(self) -> bool:
        return self._state >= _ERROR

    def _add_listener(self, listener: Callable[[_FutureState[T]], None]) -> None:
        """Run ``listener(self)`` on completion, or now when already done."""
        with self._lock:
            if self._state == _PENDING:
                if self._listeners is None:
                    self._listeners = [listener]  # type: ignore[misc]
                else:
                    self._listeners.append(listener)
                return
        listener(self)

    # -- dependants --------------------------------------------------------------

    def then_apply(self, function: Callable[[T], R]) -> _KafkaFutureImpl[R]:
        """Java's ``thenApply``: a dependant completed with ``function``'s result
        on this future's value, failed with what ``function`` raises, and
        carrying this future's failure or cancellation unchanged."""
        dependant: _KafkaFutureImpl[R] = _KafkaFutureImpl()

        def on_done(source: _FutureState[T]) -> None:
            if source._state == _VALUE:
                try:
                    result = function(source._outcome)
                except BaseException as error:  # Java catches Throwable
                    dependant.complete_with_error(error)
                    if isinstance(error, (KeyboardInterrupt, SystemExit)):
                        raise
                else:
                    dependant.complete(result)
            else:
                dependant._settle(source._state, source._outcome)

        self._add_listener(on_done)
        return dependant

    def when_complete(self, action: Callable[[T | None, BaseException | None], None]
                      ) -> _KafkaFutureImpl[T]:
        """Java's ``whenComplete``: a dependant with this future's outcome,
        completed once ``action(value, None)`` or ``action(None, error)``
        returns; when the action raises, the dependant fails with the action's
        error if this future succeeded, else with this future's."""
        dependant: _KafkaFutureImpl[T] = _KafkaFutureImpl()

        def on_done(source: _FutureState[T]) -> None:
            state, outcome = source._state, source._outcome
            try:
                if state == _VALUE:
                    action(outcome, None)
                else:
                    action(None, outcome)
            except BaseException as error:  # Java catches Throwable
                if state == _VALUE:
                    dependant.complete_with_error(error)
                else:
                    dependant._settle(state, outcome)
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
            else:
                dependant._settle(state, outcome)

        self._add_listener(on_done)
        return dependant

    def __str__(self) -> str:
        """Java's ``KafkaFutureImpl.toString()``."""
        return _to_string(self._state, self._outcome)


class _KafkaFutureImpl(_FutureState[T]):
    """Java's ``KafkaFutureImpl`` standing alone: Python state with its own
    lock, the value source of ``completed_future``, of the dependants
    (``then_apply``, ``when_complete``, ``all_of``), of the mocks' futures and
    of the sync producers' ``send()`` futures."""

    __slots__ = ("_listeners", "_lock", "_outcome", "_state")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state = _PENDING
        self._outcome = None
        self._listeners = None

    @staticmethod
    def all_of(sources: list[_FutureState[Any]]) -> _KafkaFutureImpl[None]:
        """Java's ``KafkaFuture.allOf``: completed once every source is, with
        ``None``, or failed (or cancelled) with the first failure among them."""
        result: _KafkaFutureImpl[None] = _KafkaFutureImpl()
        if not sources:
            result.complete(None)
            return result
        lock = threading.Lock()
        state: list[Any] = [len(sources), None]  # remaining, first failure

        def on_done(source: _FutureState[Any]) -> None:
            with lock:
                if source._state != _VALUE and state[1] is None:
                    state[1] = source._outcome
                state[0] -= 1
                last = state[0] == 0
            if last:
                if state[1] is None:
                    result.complete(None)
                else:
                    result.complete_with_error(state[1])

        for source in sources:
            source._add_listener(on_done)
        return result


def _to_string(state: int, outcome: Any) -> str:
    value = outcome if state == _VALUE else None
    error = outcome if state >= _ERROR else None
    shown_value = "None" if value is None else str(value)
    shown_error = "None" if error is None else _throwable.to_string(error)
    done = "True" if (error is not None or value is not None) else "False"
    return f"KafkaFuture{{value={shown_value},exception={shown_error},done={done}}}"


def _error_from_c(handle: int) -> BaseException:
    """The Python error of an owned FFI error handle (freed here): a foreign
    callback error gives back the exception the completer gave, any other the
    error class of its FFI id (``from_ffi_error``)."""
    import _confluentkafka as lib  # type: ignore[import-not-found]

    from confluent_kafka._errors import from_ffi_error

    original: BaseException | None = lib.KafkaError_callback_exception(handle)
    if original is not None and lib.KafkaError_code(handle) == _LOCAL_CALLBACK:
        lib.KafkaError_destroy(handle)
        return original
    return from_ffi_error(handle)


def _timeout_s(timeout: Duration) -> float:
    """A ``get`` timeout in seconds, read as the Timeouts rule reads a
    ``Duration`` (whole milliseconds, rounded down; ``OverflowError`` past a
    signed 64-bit count, infinity included; ``ValueError`` for ``NaN``). A
    negative or zero one is no error: the wait is zero, as Java's
    ``CompletableFuture.get(timeout, unit)``."""
    if isinstance(timeout, timedelta):
        millis = ((timeout.days * 86_400 + timeout.seconds) * 1_000
                  + timeout.microseconds // 1_000)
    else:
        millis = math.floor(float(timeout) * 1000.0)
    if not _LONG_MIN <= millis <= _LONG_MAX:
        raise OverflowError(f"timeout of {millis} ms does not fit a signed 64-bit integer")
    return max(millis, 0) / 1000.0


def _timed_out() -> BaseException:
    """Java's ``java.util.concurrent.TimeoutException`` of a timed-out ``get``
    (no message, as ``CompletableFuture`` throws it)."""
    from confluent_kafka.timeout_error import TimeoutError as JavaTimeoutError

    return JavaTimeoutError()


def _execution_error(cause: BaseException) -> BaseException:
    from confluent_kafka.execution_error import ExecutionError

    return ExecutionError(cause=cause)


# ---------------------------------------------------------------------------
# The inner futures: Java's KafkaCompletableFuture
# ---------------------------------------------------------------------------

# Java's KafkaCompletableFuture.erroneousCompletionException() message.
_USER_COMPLETION_MESSAGE = "User code should not complete futures returned from Kafka clients"


def _user_completion() -> BaseException:
    """Java's ``UnsupportedOperationException`` (``UnsupportedOperationError``)
    for user code completing an inner future."""
    from confluent_kafka.unsupported_operation_error import UnsupportedOperationError

    return UnsupportedOperationError(message=_USER_COMPLETION_MESSAGE)


class _InnerFuture(concurrent.futures.Future[T]):
    """The ``concurrent.futures.Future`` a :class:`KafkaFuture` waits on and
    :meth:`KafkaFuture.to_completion_stage` returns: Java's
    ``KafkaCompletableFuture``, which user code cannot complete. Cancelling it
    cancels the ``KafkaFuture``, as cancelling Java's completion stage does."""

    def set_result(self, result: T) -> None:
        raise _user_completion()

    def set_exception(self, exception: BaseException | None) -> None:
        raise _user_completion()


class _InnerAsyncFuture(asyncio.Future[T]):
    """The ``asyncio.Future`` of an :class:`AsyncKafkaFuture`, as
    :class:`_InnerFuture`."""

    def set_result(self, result: T) -> None:
        raise _user_completion()

    def set_exception(self, exception: type | BaseException) -> None:
        raise _user_completion()


def _cancel_from_inner(impl: _FutureState[Any], cancelled_error: type[BaseException]
                       ) -> Callable[[Any], None]:
    def on_inner_done(inner: Any) -> None:
        if inner.cancelled():
            # The state's cancel, not the future's: a future that is its own
            # state answers cancel(may_interrupt_if_running=...) instead.
            impl._settle(_CANCELLED, cancelled_error())
    return on_inner_done


# ---------------------------------------------------------------------------
# The public classes
# ---------------------------------------------------------------------------


class _KafkaFutureBase(Generic[T]):
    """What both kinds share (private). A future holds its C source (the
    extension's future object) or its Python state, and its inner future once
    made."""

    __slots__ = ("_impl", "_inner", "_native")

    _native: Any
    _impl: _FutureState[T] | None
    _inner: Any

    #: The ``CancelledError`` class of the kind.
    _CANCELLED_ERROR: type[BaseException] = concurrent.futures.CancelledError

    # -- the lazy state ------------------------------------------------------------

    def _python_state(self) -> _FutureState[T]:
        """The Python state, made on first use for a future over a C handle:
        a :class:`_KafkaFutureImpl` the handle's ``get_cb`` delivery completes.
        Not ``_state``: that is the state's own progress field, and a future
        that is its own state (``async_producer._AsyncRecord``) has both."""
        impl = self._impl
        if impl is not None:
            return impl
        with _LAZY_LOCK:
            impl = self._impl
            if impl is not None:
                return impl
            impl = _KafkaFutureImpl()
            self._impl = impl
        # Outside the lock: a future of no client delivers inline.
        self._native.get_cb(impl._deliver_from_c)
        return impl

    def _new_inner(self) -> Any:
        raise NotImplementedError

    def _waiter(self) -> Any:
        """The inner future, made and linked to the Python state on first use."""
        inner = self._inner
        if inner is not None:
            return inner
        impl = self._python_state()
        with _LAZY_LOCK:
            inner = self._inner
            if inner is not None:
                return inner
            inner = self._new_inner()
            self._inner = inner
        impl._add_listener(self._link(inner))
        inner.add_done_callback(_cancel_from_inner(impl, self._CANCELLED_ERROR))
        return inner

    def _link(self, inner: Any) -> Callable[[_FutureState[T]], None]:
        raise NotImplementedError

    def _answer(self, impl: _FutureState[T]) -> T:
        """The value of a done state, or its failure raised as ``get`` raises it."""
        state = impl._state
        if state == _VALUE:
            return impl._outcome  # type: ignore[no-any-return]
        error: BaseException = impl._outcome
        if state == _CANCELLED:
            if isinstance(error, self._CANCELLED_ERROR):
                raise error
            raise self._CANCELLED_ERROR(*error.args)
        raise _execution_error(error) from error

    def _read_done_handle(self) -> _FutureState[T] | None:
        """The Python state, settled at once from a done C handle when it is
        missing or still pending (its ``get_cb`` delivery not yet run), as
        Java's ``CompletableFuture`` reads a completed state directly: the
        handle's ``get_if_done`` does not wait, and the value is the one the
        later delivery hands over, which then finds the state settled. A
        ``when_complete`` action registered before may still be pending when
        this answers, as in Java."""
        impl = self._impl
        native = self._native
        if native is None or (impl is not None and impl._state != _PENDING):
            return impl
        if not native.is_done():
            return impl
        if impl is None:
            with _LAZY_LOCK:
                impl = self._impl
                if impl is None:
                    # Done already: settled below, so no get_cb is needed.
                    impl = _KafkaFutureImpl()
                    self._impl = impl
        if impl._state == _PENDING:
            outcome = native.get_if_done()
            if outcome is not None:
                impl._deliver_from_c(*outcome)
        return impl

    def _get_now(self, value_if_absent: T) -> T:
        impl = self._read_done_handle()
        if impl is None or impl._state == _PENDING:
            return value_if_absent
        return self._answer(impl)

    # -- the shared members --------------------------------------------------------

    def cancel(self, *, may_interrupt_if_running: bool) -> bool:
        """If not already completed, completes this future with a
        ``CancelledError``. Dependent futures that have not already completed
        will also be cancelled. Never cancels the source, the request or the
        core; ``False`` for a future already done."""
        if self.is_done():
            return False
        return self._python_state().cancel(self._CANCELLED_ERROR())

    def is_cancelled(self) -> bool:
        """Returns true if this future was cancelled before it completed normally."""
        impl = self._read_done_handle()
        return impl is not None and impl._state == _CANCELLED

    def is_completed_exceptionally(self) -> bool:
        """Returns true if this future completed exceptionally, in any way."""
        impl = self._read_done_handle()
        return impl is not None and impl._state >= _ERROR

    def is_done(self) -> bool:
        """Returns true if completed in any fashion: normally, exceptionally, or
        via cancellation."""
        impl = self._impl
        if impl is not None and impl._state != _PENDING:
            return True
        native = self._native
        return native is not None and bool(native.is_done())

    def __str__(self) -> str:
        """Java's ``KafkaFutureImpl.toString()``."""
        impl = self._read_done_handle()
        if impl is None:
            return _to_string(_PENDING, None)
        return _to_string(impl._state, impl._outcome)

    def __reduce_ex__(self, protocol: SupportsIndex) -> str | tuple[Any, ...]:
        """``copy``, ``deepcopy`` and ``pickle``: refused for a future over a C
        handle, whose copy would share the native future, as Java's
        ``KafkaFuture`` is neither ``Cloneable`` nor ``Serializable`` (CLAUDE.md,
        Python Binding Conventions, Types, the ``java.io.Serializable`` row); a
        future over Python state keeps Python's default."""
        if self._native is not None:
            raise not_copyable(type(self).__name__, "it holds the client's native future")
        return super().__reduce_ex__(protocol)




def _checked_futures(futures: Any) -> list[Any]:
    """``allOf(KafkaFuture<?>... futures)``'s argument: Java's
    ``Arrays.stream(futures)`` dereferences a null array, and each null element
    is dereferenced in turn (``KafkaFuture.java:76, 80``)."""
    if futures is None:
        raise TypeError("futures must not be None")
    sources = list(futures)
    if any(f is None for f in sources):
        raise TypeError("futures must not contain None")
    return sources

class KafkaFuture(_KafkaFutureBase[T]):
    """A flexible future which supports call chaining and other asynchronous
    programming patterns.

    It is possible to obtain the inner ``concurrent.futures.Future`` from a
    ``KafkaFuture`` by calling :meth:`to_completion_stage`. A failed
    ``KafkaFuture`` raises ``ExecutionError`` from :meth:`get`, with the failure
    as its ``__cause__``, whereas the inner future raises the failure itself.

    Java: ``org.apache.kafka.common.KafkaFuture``.
    """

    __slots__ = ()

    #: A function which takes objects of type A and returns objects of type B.
    BaseFunction: TypeAlias = Callable[[_A], _B]
    #: A consumer of two different types of object.
    BiConsumer: TypeAlias = Callable[[_A, _B], None]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "KafkaFuture cannot be constructed directly; use "
            "KafkaFuture.completed_future(...) or KafkaFuture.all_of(...)")

    @classmethod
    def _of_native(cls, native: Any) -> KafkaFuture[Any]:
        """A future over the extension's future object (a client's future)."""
        future = cls.__new__(cls)
        future._native = native
        future._impl = None
        future._inner = None
        return future

    @classmethod
    def _of_impl(cls, impl: _FutureState[U]) -> KafkaFuture[U]:
        """A future over Python state (``completed_future``, a dependant, a mock's)."""
        future: Any = cls.__new__(cls)
        future._native = None
        future._impl = impl
        future._inner = None
        return future  # type: ignore[no-any-return]

    @staticmethod
    def completed_future(*, value: U) -> KafkaFuture[U]:
        """Returns a new KafkaFuture that is already completed with the given value."""
        impl: _KafkaFutureImpl[U] = _KafkaFutureImpl()
        impl.complete(value)
        return KafkaFuture._of_impl(impl)

    @staticmethod
    def all_of(*, futures: tuple[KafkaFuture[Any], ...]) -> KafkaFuture[None]:
        """Returns a new KafkaFuture that is completed when all the given futures
        have completed. If any future fails, the returned future fails with it.
        If multiple futures fail, the first failure is returned. A ``None``
        ``futures``, or a ``None`` among them, raises ``TypeError``, as Java's
        ``Arrays.stream(null)`` and ``kafkaFuture.toCompletionStage()`` do."""
        return KafkaFuture._of_impl(_KafkaFutureImpl.all_of(
            [f._python_state() for f in _checked_futures(futures)]))

    def to_completion_stage(self) -> concurrent.futures.Future[T]:
        """Gets the inner ``concurrent.futures.Future`` this future waits on,
        which completes when this future completes and in the same way (with
        the same result or error, or cancelled).

        To wait for a KafkaFuture use :meth:`get` or :meth:`get_now`, rather
        than the inner future's ``result()``."""
        return self._waiter()  # type: ignore[no-any-return]

    def then_apply(self, *, function: Callable[[T], R]) -> KafkaFuture[R]:
        """Returns a new KafkaFuture that, when this future completes normally,
        is executed with this future's result as the argument to the supplied
        function.

        The function runs where this future completes (the producer's
        completion thread, the admin client's pump thread), or inside this call
        when the future is already done."""
        return KafkaFuture._of_impl(self._python_state().then_apply(function))

    def when_complete(self, *, action: Callable[[T | None, BaseException | None], None]
                      ) -> KafkaFuture[T]:
        """Returns a new KafkaFuture with the same result or error as this
        future, that executes the given action when this future completes.

        When this future is done, the given action is invoked with the result
        (or ``None`` if none) and the error (or ``None`` if none) of this future
        as arguments. The returned future is completed when the action
        returns. The supplied action should not raise. However, if it does, the
        following rules apply: if this future completed normally but the
        supplied action raises, then the returned future fails with the
        action's error. Or, if this future failed and the supplied action
        raises, then the returned future fails with this future's error.

        The action runs where this future completes, or inside this call when
        the future is already done."""
        return KafkaFuture._of_impl(self._python_state().when_complete(action))

    def cancel(self, *, may_interrupt_if_running: bool) -> bool:
        """If not already completed, completes this future with a
        ``concurrent.futures.CancelledError``. Dependent futures that have not
        already completed will also be cancelled. Never cancels the source, the
        request or the core."""
        return super().cancel(may_interrupt_if_running=may_interrupt_if_running)

    @overload
    def get(self) -> T: ...
    @overload
    def get(self, *, timeout: Duration) -> T: ...
    def get(self, *, timeout: Duration = MISSING) -> T:
        """Waits if necessary for this future to complete, and then returns its
        result; with ``timeout``, for at most that long, raising
        ``confluent_kafka.TimeoutError`` when it elapses first (a negative or
        zero timeout raises at once unless the future is done). A ``None``
        timeout raises ``TypeError``, as Java's null ``TimeUnit`` does.

        A failed future raises ``confluent_kafka.ExecutionError`` from the
        failure; a cancelled one ``concurrent.futures.CancelledError``. The wait
        is in short slices, so Ctrl+C ends it."""
        if timeout is None:  # Java's unit.toNanos(timeout) on a null TimeUnit
            raise TypeError("timeout must not be None")
        seconds = None if timeout is MISSING else _timeout_s(timeout)
        impl = self._read_done_handle()
        if impl is None or impl._state == _PENDING:
            inner = self._waiter()
            deadline = None if seconds is None else time.monotonic() + seconds
            # Each slice also reads the handle, so a handle that completes
            # while its delivery is queued behind this thread (a get() from the
            # client's own callback thread) answers instead of waiting for it.
            while True:
                impl = self._read_done_handle()
                if impl is not None and impl._state != _PENDING:
                    break
                slice_s = WAIT_SLICE_S
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        if done_within(inner, 0):
                            continue
                        raise _timed_out()
                    slice_s = min(slice_s, remaining)
                done_within(inner, slice_s)
        return self._answer(impl)

    def get_now(self, *, value_if_absent: T) -> T:
        """Returns the result value (or raises any encountered error) if
        completed, else returns the given ``value_if_absent``."""
        return self._get_now(value_if_absent)

    def is_cancelled(self) -> bool:
        """Returns true if this future was cancelled before it completed normally."""
        return super().is_cancelled()

    def is_completed_exceptionally(self) -> bool:
        """Returns true if this future completed exceptionally, in any way."""
        return super().is_completed_exceptionally()

    def is_done(self) -> bool:
        """Returns true if completed in any fashion: normally, exceptionally, or
        via cancellation."""
        return super().is_done()

    # -- private ---------------------------------------------------------------------

    def _new_inner(self) -> concurrent.futures.Future[T]:
        return _InnerFuture()

    def _link(self, inner: Any) -> Callable[[_FutureState[T]], None]:
        def settle(impl: _FutureState[T]) -> None:
            _settle_sync(inner, impl)
        return settle


def _settle_sync(inner: concurrent.futures.Future[Any], impl: _FutureState[Any]) -> None:
    if inner.done():
        return
    state = impl._state
    if state == _VALUE:
        concurrent.futures.Future.set_result(inner, impl._outcome)
    elif state == _ERROR:
        concurrent.futures.Future.set_exception(inner, impl._outcome)
    elif inner.cancel():
        # Wakes the waiters of concurrent.futures.wait as well.
        inner.set_running_or_notify_cancel()
    else:
        # An inner future that refuses cancellation still releases its
        # waiters: the outcome (a CancelledError) is delivered as its failure,
        # and the future answers from its own state.
        concurrent.futures.Future.set_exception(inner, impl._outcome)


class AsyncKafkaFuture(_KafkaFutureBase[T]):
    """The asyncio peer of :class:`KafkaFuture`: the future an asyncio class
    hands out. :meth:`get` is awaited; the inner future is an ``asyncio.Future``
    on the client's event loop, and a cancelled future raises
    ``asyncio.CancelledError``.

    Java: ``org.apache.kafka.common.KafkaFuture``.
    """

    __slots__ = ("_loop",)

    _loop: asyncio.AbstractEventLoop | None

    _CANCELLED_ERROR = asyncio.CancelledError

    #: A function which takes objects of type A and returns objects of type B.
    BaseFunction: TypeAlias = Callable[[_A], _B]
    #: A consumer of two different types of object.
    BiConsumer: TypeAlias = Callable[[_A, _B], None]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "AsyncKafkaFuture cannot be constructed directly; use "
            "AsyncKafkaFuture.completed_future(...) or AsyncKafkaFuture.all_of(...)")

    @classmethod
    def _of_native(cls, native: Any, loop: asyncio.AbstractEventLoop | None = None
                   ) -> AsyncKafkaFuture[Any]:
        """A future over the extension's future object, its inner future on
        ``loop`` (the client's), else on the loop of its first wait."""
        future = cls.__new__(cls)
        future._native = native
        future._impl = None
        future._inner = None
        future._loop = loop
        return future

    @classmethod
    def _of_impl(cls, impl: _FutureState[U], loop: asyncio.AbstractEventLoop | None = None
                 ) -> AsyncKafkaFuture[U]:
        """A future over Python state."""
        future: Any = cls.__new__(cls)
        future._native = None
        future._impl = impl
        future._inner = None
        future._loop = loop
        return future  # type: ignore[no-any-return]

    @staticmethod
    def completed_future(*, value: U) -> AsyncKafkaFuture[U]:
        """Returns a new AsyncKafkaFuture that is already completed with the given value."""
        impl: _KafkaFutureImpl[U] = _KafkaFutureImpl()
        impl.complete(value)
        return AsyncKafkaFuture._of_impl(impl)

    @staticmethod
    def all_of(*, futures: tuple[AsyncKafkaFuture[Any], ...]) -> AsyncKafkaFuture[None]:
        """Returns a new AsyncKafkaFuture that is completed when all the given
        futures have completed. If any future fails, the returned future fails
        with it. If multiple futures fail, the first failure is returned. A
        ``None`` ``futures``, or a ``None`` among them, raises ``TypeError``, as
        Java's ``Arrays.stream(null)`` and ``kafkaFuture.toCompletionStage()``
        do."""
        sources = _checked_futures(futures)
        loop = next((f._loop for f in sources if f._loop is not None), None)
        return AsyncKafkaFuture._of_impl(
            _KafkaFutureImpl.all_of([f._python_state() for f in sources]), loop)

    def to_completion_stage(self) -> asyncio.Future[T]:
        """Gets the inner ``asyncio.Future`` this future waits on, on the
        client's event loop (or the running loop), which completes when this
        future completes and in the same way."""
        return self._waiter()  # type: ignore[no-any-return]

    def then_apply(self, *, function: Callable[[T], R]) -> AsyncKafkaFuture[R]:
        """Returns a new AsyncKafkaFuture that, when this future completes
        normally, is executed with this future's result as the argument to the
        supplied function.

        The function runs where this future completes (the event loop of an
        asyncio client), or inside this call when the future is already done."""
        return AsyncKafkaFuture._of_impl(self._python_state().then_apply(function), self._loop)

    def when_complete(self, *, action: Callable[[T | None, BaseException | None], None]
                      ) -> AsyncKafkaFuture[T]:
        """Returns a new AsyncKafkaFuture with the same result or error as this
        future, that executes the given action when this future completes; see
        :meth:`KafkaFuture.when_complete`."""
        return AsyncKafkaFuture._of_impl(self._python_state().when_complete(action), self._loop)

    def cancel(self, *, may_interrupt_if_running: bool) -> bool:
        """If not already completed, completes this future with an
        ``asyncio.CancelledError``. Dependent futures that have not already
        completed will also be cancelled. Never cancels the source, the request
        or the core."""
        return super().cancel(may_interrupt_if_running=may_interrupt_if_running)

    @overload
    async def get(self) -> T: ...
    @overload
    async def get(self, *, timeout: Duration) -> T: ...
    async def get(self, *, timeout: Duration = MISSING) -> T:
        """Waits if necessary for this future to complete, and then returns its
        result; with ``timeout``, for at most that long, raising
        ``confluent_kafka.TimeoutError`` when it elapses first (a negative or
        zero timeout raises at once unless the future is done). A ``None``
        timeout raises ``TypeError``, as Java's null ``TimeUnit`` does.

        A failed future raises ``confluent_kafka.ExecutionError`` from the
        failure; a cancelled one ``asyncio.CancelledError``. Cancelling the
        awaiting task does not cancel the future."""
        if timeout is None:  # Java's unit.toNanos(timeout) on a null TimeUnit
            raise TypeError("timeout must not be None")
        seconds = None if timeout is MISSING else _timeout_s(timeout)
        impl = self._read_done_handle()
        if impl is None or impl._state == _PENDING:
            inner = self._waiter()
            if not inner.done():
                if seconds is not None and seconds <= 0:
                    raise _timed_out()
                done, _ = await asyncio.wait((inner,), timeout=seconds)
                if not done:
                    raise _timed_out()
            impl = self._impl
            assert impl is not None
        return self._answer(impl)

    def get_now(self, *, value_if_absent: T) -> T:
        """Returns the result value (or raises any encountered error) if
        completed, else returns the given ``value_if_absent``."""
        return self._get_now(value_if_absent)

    def is_cancelled(self) -> bool:
        """Returns true if this future was cancelled before it completed normally."""
        return super().is_cancelled()

    def is_completed_exceptionally(self) -> bool:
        """Returns true if this future completed exceptionally, in any way."""
        return super().is_completed_exceptionally()

    def is_done(self) -> bool:
        """Returns true if completed in any fashion: normally, exceptionally, or
        via cancellation."""
        return super().is_done()

    # -- private ---------------------------------------------------------------------

    def _new_inner(self) -> asyncio.Future[T]:
        loop = self._loop
        if loop is None:
            loop = asyncio.get_running_loop()
            self._loop = loop
        return _InnerAsyncFuture(loop=loop)

    def _link(self, inner: Any) -> Callable[[_FutureState[T]], None]:
        loop: asyncio.AbstractEventLoop = inner.get_loop()

        def settle(impl: _FutureState[T]) -> None:
            if _on_loop_thread(loop):
                _settle_async(inner, impl)
            else:
                try:
                    loop.call_soon_threadsafe(_settle_async, inner, impl)
                except RuntimeError:
                    pass  # the loop is closed: nothing can wait on it any more
        return settle


def _on_loop_thread(loop: asyncio.AbstractEventLoop) -> bool:
    try:
        return asyncio.get_running_loop() is loop
    except RuntimeError:
        return False


def _settle_async(inner: asyncio.Future[Any], impl: _FutureState[Any]) -> None:
    if inner.done():
        return
    state = impl._state
    if state == _VALUE:
        asyncio.Future.set_result(inner, impl._outcome)
    elif state == _ERROR:
        asyncio.Future.set_exception(inner, impl._outcome)
        # The future answers from its own state; mark the inner failure seen,
        # so asyncio does not log it as never retrieved.
        inner.exception()
    else:
        message = _throwable.java_message(impl._outcome)
        if not inner.cancel(message):
            # An inner future that refuses cancellation still releases its
            # waiters, as _settle_sync does.
            asyncio.Future.set_exception(inner, impl._outcome)
            inner.exception()
