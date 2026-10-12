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

"""The admin ``*Result`` classes' private future helpers and client-kind marker.

Java's ``KafkaFuture<T>`` is Python's ``concurrent.futures.Future[T]`` on the sync
client and ``asyncio.Future[T]`` on the async one; ``KafkaFuture``'s own methods are
not generated, and the result classes keep the two they use as private helpers,
:func:`_then_apply` (``thenApply``) and :func:`_all_of` (``allOf``) (CLAUDE.md,
Python Binding Conventions, Idiom translations).

A result class is shared by both clients, so it is generic over a private marker of
the client kind, :class:`Sync` or :class:`Async`, set by the client that returns it
(Class family): ``CreateTopicsResult[Sync]`` holds ``concurrent.futures.Future``\\ s,
``CreateTopicsResult[Async]`` ``asyncio.Future``\\ s. No user code writes the marker.

Java's ``KafkaFutureImpl`` completes a future from any thread. An
``asyncio.Future`` is completed only on its loop's thread, so :func:`_complete` and
:func:`_complete_with_error` go through ``loop.call_soon_threadsafe`` when called
from another thread. A derived future (``_then_apply``, ``_all_of``) of futures that
are already done is done on return, as Java's is: every future of an admin result is
complete when the call returns (the FFI calls back once).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Callable, Iterable
from typing import Any, TypeVar, Union, overload

__all__ = [
    "Async",
    "AnyFuture",
    "KindT",
    "Sync",
    "_all_of",
    "_complete",
    "_complete_with_error",
    "_completed_future",
    "_kind_of",
    "_mark_retrieved",
    "_new_future",
    "_new_future_like",
    "_then_apply",
    "_when_complete",
]

_T = TypeVar("_T")
_U = TypeVar("_U")


class Sync:
    """Marker of the sync client kind: a result's futures are
    ``concurrent.futures.Future``\\ s."""

    __slots__ = ()

    def __init__(self) -> None:
        raise TypeError("Sync is a type marker; it has no instances")


class Async:
    """Marker of the async client kind: a result's futures are
    ``asyncio.Future``\\ s."""

    __slots__ = ()

    def __init__(self) -> None:
        raise TypeError("Async is a type marker; it has no instances")


#: The client kind a result class is generic over.
KindT = TypeVar("KindT", Sync, Async)

#: A future of either kind, for the private ``_new`` constructors, which both
#: clients call.
AnyFuture = Union[concurrent.futures.Future[_T], asyncio.Future[_T]]


def _kind_of(futures: Iterable[AnyFuture[Any]]) -> type[Sync] | type[Async]:
    """The kind of the given futures: :class:`Async` when one is an
    ``asyncio.Future``, else :class:`Sync` (also for none)."""
    for future in futures:
        return Async if isinstance(future, asyncio.Future) else Sync
    return Sync


def _loop_for_new_future() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        from confluent_kafka.illegal_state_error import IllegalStateError

        raise IllegalStateError(
            message="An async admin result's future can only be created on a running event loop"
        ) from None


@overload
def _new_future(kind: type[Sync]) -> concurrent.futures.Future[Any]: ...
@overload
def _new_future(kind: type[Async], loop: asyncio.AbstractEventLoop | None = None
                ) -> asyncio.Future[Any]: ...
@overload
def _new_future(kind: type[Sync] | type[Async], loop: asyncio.AbstractEventLoop | None = None
                ) -> AnyFuture[Any]: ...


def _new_future(kind: type[Sync] | type[Async],
                loop: asyncio.AbstractEventLoop | None = None) -> AnyFuture[Any]:
    """Java's ``new KafkaFutureImpl<>()``: a pending future of ``kind``; an async one
    on ``loop``, else on the running loop."""
    if kind is Async:
        return (loop if loop is not None else _loop_for_new_future()).create_future()
    return concurrent.futures.Future()


def _on_loop_thread(future: asyncio.Future[Any]) -> bool:
    try:
        return asyncio.get_running_loop() is future.get_loop()
    except RuntimeError:
        return False


def _complete(future: AnyFuture[_T], value: _T) -> bool:
    """Java's ``KafkaFutureImpl.complete(value)``: completes a pending future;
    ``False`` when it was already done."""
    if isinstance(future, asyncio.Future):
        if future.done():
            return False
        if _on_loop_thread(future) or not future.get_loop().is_running():
            future.set_result(value)
        else:
            future.get_loop().call_soon_threadsafe(_set_result_if_pending, future, value)
        return True
    if future.done():
        return False
    try:
        future.set_result(value)
    except concurrent.futures.InvalidStateError:
        return False
    return True


def _complete_with_error(future: AnyFuture[Any], error: BaseException) -> bool:
    """Java's ``KafkaFutureImpl.completeExceptionally(error)``: fails a pending
    future; ``False`` when it was already done. A cancellation error (as
    :func:`_when_complete` hands over for a cancelled source) cancels it, as Java's
    ``CancellationException`` does."""
    if isinstance(error, (asyncio.CancelledError, concurrent.futures.CancelledError)):
        if future.done():
            return False
        return future.cancel()
    if isinstance(future, asyncio.Future):
        if future.done():
            return False
        if _on_loop_thread(future) or not future.get_loop().is_running():
            future.set_exception(error)
        else:
            future.get_loop().call_soon_threadsafe(_set_exception_if_pending, future, error)
        return True
    if future.done():
        return False
    try:
        future.set_exception(error)
    except concurrent.futures.InvalidStateError:
        return False
    return True


def _set_result_if_pending(future: asyncio.Future[_T], value: _T) -> None:
    if not future.done():
        future.set_result(value)


def _set_exception_if_pending(future: asyncio.Future[Any], error: BaseException) -> None:
    if not future.done():
        future.set_exception(error)


@overload
def _new_future_like(source: concurrent.futures.Future[Any]) -> concurrent.futures.Future[Any]: ...
@overload
def _new_future_like(source: asyncio.Future[Any]) -> asyncio.Future[Any]: ...


def _new_future_like(source: AnyFuture[Any]) -> AnyFuture[Any]:
    """``new KafkaFutureImpl<>()`` for a future completed from ``source``'s
    ``whenComplete``: pending, of ``source``'s kind (and loop)."""
    if isinstance(source, asyncio.Future):
        return source.get_loop().create_future()
    return concurrent.futures.Future()


@overload
def _completed_future(kind: type[Sync], value: _T) -> concurrent.futures.Future[_T]: ...
@overload
def _completed_future(kind: type[Async], value: _T) -> asyncio.Future[_T]: ...


def _completed_future(kind: type[Sync] | type[Async], value: _T) -> AnyFuture[_T]:
    """Java's ``KafkaFuture.completedFuture(value)``, of ``kind``."""
    future: AnyFuture[_T] = _new_future(kind)
    future.set_result(value)
    return future


def _copy_outcome(source: AnyFuture[Any], target: AnyFuture[Any],
                  fn: Callable[[Any], Any] | None) -> None:
    """Complete ``target`` from the done ``source``: cancelled, failed, or with
    ``fn(result)`` (``fn``'s own error fails it)."""
    if source.cancelled():
        target.cancel()
        return
    error = source.exception()
    if error is not None:
        _complete_with_error(target, error)
        return
    result = source.result()
    if fn is None:
        _complete(target, result)
        return
    try:
        value = fn(result)
    except Exception as e:  # noqa: BLE001 - Java's thenApply fails the future with fn's error
        _complete_with_error(target, e)
        return
    _complete(target, value)


@overload
def _then_apply(future: concurrent.futures.Future[_T],
                fn: Callable[[_T], _U]) -> concurrent.futures.Future[_U]: ...
@overload
def _then_apply(future: asyncio.Future[_T], fn: Callable[[_T], _U]) -> asyncio.Future[_U]: ...


def _then_apply(future: AnyFuture[_T], fn: Callable[[_T], _U]) -> AnyFuture[_U]:
    """Java's ``KafkaFuture.thenApply(function)``: a new future of the same kind that
    completes with ``fn(result)`` when ``future`` succeeds, and fails as ``future``
    fails (or with ``fn``'s error)."""
    if isinstance(future, asyncio.Future):
        derived: AnyFuture[_U] = future.get_loop().create_future()
    else:
        derived = concurrent.futures.Future()
    if future.done():
        _copy_outcome(future, derived, fn)
    else:
        future.add_done_callback(lambda done: _copy_outcome(done, derived, fn))
    return derived


@overload
def _all_of(futures: Iterable[concurrent.futures.Future[Any]],
            kind: type[Sync]) -> concurrent.futures.Future[None]: ...
@overload
def _all_of(futures: Iterable[asyncio.Future[Any]],
            kind: type[Async]) -> asyncio.Future[None]: ...
@overload
def _all_of(futures: Iterable[AnyFuture[Any]],
            kind: type[Sync] | type[Async]) -> AnyFuture[None]: ...


def _all_of(futures: Iterable[AnyFuture[Any]], kind: type[Sync] | type[Async]) -> AnyFuture[None]:
    """Java's ``KafkaFuture.allOf(futures...)``: a future of ``kind`` that completes
    with ``None`` when every given future has completed, failing with the error of the
    first one (in the given order) that failed, or cancelled if that one was cancelled,
    as ``CompletableFuture.allOf`` does. With no futures it is already complete."""
    sources = list(futures)
    loop = sources[0].get_loop() if sources and isinstance(sources[0], asyncio.Future) else None
    result: AnyFuture[None] = _new_future(kind, loop)

    def finish() -> None:
        for source in sources:
            if source.cancelled():
                result.cancel()
                return
            if source.exception() is not None:
                _complete_with_error(result, source.exception())  # type: ignore[arg-type]
                return
        _complete(result, None)

    pending = [s for s in sources if not s.done()]
    if not pending:
        finish()
        return result
    lock = threading.Lock()
    remaining = [len(pending)]

    def on_done(_: AnyFuture[Any]) -> None:
        with lock:
            remaining[0] -= 1
            last = remaining[0] == 0
        if last:
            finish()

    for source in pending:
        source.add_done_callback(on_done)
    return result


def _outcome(future: AnyFuture[_T]) -> tuple[_T | None, BaseException | None]:
    """``(value, None)`` or ``(None, error)`` of a done future; a cancelled one gives
    its kind's ``CancelledError``."""
    if future.cancelled():
        if isinstance(future, asyncio.Future):
            return None, asyncio.CancelledError()
        return None, concurrent.futures.CancelledError()
    error = future.exception()
    if error is not None:
        return None, error
    return future.result(), None


def _when_complete(future: AnyFuture[_T],
                   action: Callable[[_T | None, BaseException | None], None]) -> None:
    """Java's ``KafkaFuture.whenComplete(action)`` where the returned future is not
    used: ``action(value, error)`` runs once ``future`` is done (at once when it already
    is, as Java runs it on the calling thread), with ``error`` ``None`` on success."""
    if future.done():
        action(*_outcome(future))
    else:
        future.add_done_callback(lambda done: action(*_outcome(done)))


def _mark_retrieved(future: AnyFuture[Any]) -> None:
    """Keep asyncio from logging "Future exception was never retrieved" for a future a
    result class creates on its own (no caller asked for it, or a caller may leave a key
    of a map unread): Java's futures and ``concurrent.futures`` log nothing. The error is
    read once the future is done (at once when it already is), which only clears
    asyncio's flag: a caller reading the future still gets the error."""
    if not isinstance(future, asyncio.Future):
        return

    def read(done: asyncio.Future[Any]) -> None:
        if not done.cancelled():
            done.exception()

    if future.done():
        read(future)
    else:
        future.add_done_callback(read)
