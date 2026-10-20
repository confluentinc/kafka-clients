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

"""Send-completion helpers shared by the producers (private).

The C extension hands each record to the C API's send
(``kafka_producer_Producer_send_with_callback_cb``, which tries the producer
on the calling thread and queues the record on the core's submission task only
when the producer cannot take it at once), or, for a sync producer's send()
from any producer's delivery callback, to its
blocking send, with a delivery callback, which the producer's callback pump
(``kafka_producer_Producer__execute_callbacks``)
runs as ``on_completion(metadata, error, callback_id)``: the metadata's fields
as a tuple (``None`` when there is none), the BORROWED error handle as an int
(0 for none) and the id the callback reports under. :func:`completion_to_python`
turns them into the public objects once per completion; :func:`invoke_callback`
runs the user's ``Callback`` for the FFI-backed producers and
:func:`call_callback` for the mocks (CLAUDE.md, Python Binding Conventions,
Threads and callbacks).

The C API completes the record's future, and lets ``flush()``, the
transaction-end calls and ``close()`` return, only once the callback reported
(``kafka_producer_Producer__set_callback_result``), as Java fires the callbacks
before ``produceFuture.done()``. ``on_completion`` reports by returning: ``None``
for a callback that returned, or raising the exception it raised, which the C
extension reports as the callback's error. Python does not log that exception:
the core logs it, once, with Java's message (``ProducerBatch.completeFutureAndFireCallbacks``,
``ProducerBatch.java:318-320``) and the exception's text, and fails nothing. It
returns ``True`` when the report comes later, from
``_lib.Producer__set_callback_result``: the asyncio producer's callback that
runs on another event loop, or is an ``async def``.
"""

from __future__ import annotations

import inspect
import threading
from types import FunctionType, MethodType
from typing import TYPE_CHECKING, cast

from confluent_kafka._errors import from_ffi_error
from confluent_kafka._kwargs import require_kwargs
from confluent_kafka.common.topic_partition import TopicPartition

from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["MetadataTuple", "call_callback", "check_callback", "completion_to_python",
           "failure_metadata",
           "in_any_callback", "in_callback", "invoke_callback", "mark_completion_thread",
           "reject_coroutine_callback", "run_callback"]

# A coroutine callback given to the synchronous producer.
_COROUTINE_CALLBACK_MESSAGE = "a coroutine send() callback requires an AsyncProducer"
_CO_COROUTINE = inspect.CO_COROUTINE

# The producer whose completion thread the current thread is, or whose delivery
# callback runs on it, if any: Java tests ``Thread.currentThread() ==
# this.ioThread`` in ``flush()`` and ``close()``, so any user code the
# completion thread runs (a callback, a record future's dependants) counts.
_callback_scope = threading.local()


def failure_metadata(topic: str, partition: int | None) -> RecordMetadata:
    """The metadata Java hands a callback on failure: ``RecordMetadata(tp, -1,
    -1, RecordBatch.NO_TIMESTAMP, -1, -1)``, the partition -1 when none was
    chosen (``KafkaProducer.AppendCallbacks.onCompletion``)."""
    tp = TopicPartition(topic=topic, partition=-1 if partition is None else partition)
    return RecordMetadata(topic_partition=tp, base_offset=-1, batch_index=-1, timestamp=-1,
                          serialized_key_size=-1, serialized_value_size=-1)


# The delivery callback's metadata as the C extension passes it: (topic,
# partition, offset, timestamp, serialized key size, serialized value size),
# offset and timestamp -1 when absent.
MetadataTuple = tuple[str, int, int, int, int, int]


def completion_to_python(metadata_tuple: MetadataTuple | None, error: int, topic: str,
                         partition: int | None) -> tuple[RecordMetadata, Exception | None]:
    """The ``(metadata, exception)`` of a completion, from what the delivery
    callback received: the metadata tuple and the BORROWED error handle, which
    is read here and never freed (its owner, the C API, frees it once the
    callback returns).

    Both may be given: a failure the core raises before the record reaches a
    batch carries Java's -1 metadata beside the error, its partition the one
    Java's ``AppendCallbacks.topicPartition()`` gives (the partition chosen for
    the record, else the record's own, else -1), as the core reports it. A
    failure with no metadata (a broker-side or delivery failure) gets
    :func:`failure_metadata`, as Java's ``AppendCallbacks`` replaces the null
    metadata of ``ProducerBatch.completeFutureAndFireCallbacks``; the FFI does
    not report the partition a batch had, so it is the record's own, or -1.
    """
    exception = cast("Exception", from_ffi_error(error, owned=False)) if error else None
    if metadata_tuple is None:
        return failure_metadata(topic, partition), exception
    (topic_, partition_, offset, timestamp, key_size, value_size) = metadata_tuple
    metadata = RecordMetadata._from_ffi(
        topic=topic_, partition=partition_, offset=offset, timestamp=timestamp,
        serialized_key_size=key_size, serialized_value_size=value_size)
    return metadata, exception


def reject_coroutine_callback(callback: Callable[..., object]) -> None:
    """Raise ``TypeError`` for a coroutine function (an ``async def``, or an
    object whose ``__call__`` is one) given to a synchronous producer's
    ``send()``: its completion thread, or the mock's completing call, cannot
    await it; the asyncio producers do (CLAUDE.md, Python Binding Conventions,
    Threads and callbacks).

    It runs on every ``send()`` with a callback, so a function or a bound
    method, the usual callbacks, is answered from its code object's
    ``CO_COROUTINE`` flag; anything else (a ``functools.partial``, a callable
    object) goes through ``inspect.iscoroutinefunction``. A plain function
    that only returns an awaitable, ``inspect.markcoroutinefunction``'s
    included, passes here and is closed and reported at its completion
    (:func:`call_callback`)."""
    function = callback.__func__ if type(callback) is MethodType else callback
    if type(function) is FunctionType:
        if function.__code__.co_flags & _CO_COROUTINE:
            raise TypeError(_COROUTINE_CALLBACK_MESSAGE)
        return
    if inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(
            getattr(type(callback), "__call__", None)):
        raise TypeError(_COROUTINE_CALLBACK_MESSAGE)


def check_callback(callback: Callable[..., object]) -> None:
    """The ``send()`` callback's keyword contract: it is called as
    ``callback(metadata=..., exception=...)`` and must accept ``**kwargs``, or
    ``send()`` raises ``IllegalArgumentError("callback must accept **kwargs")``
    (CLAUDE.md, Python Binding Conventions, Signatures). One look at the code
    object for a function or a bound method (``_kwargs.require_kwargs``)."""
    require_kwargs(callback, "callback")


def run_callback(producer: object, callback: Callable[..., object],
                 metadata: RecordMetadata, exception: Exception | None) -> object:
    """Call the user's callback for one completion and return what it returned
    (an awaitable for an ``async def`` callback), raising what it raised. While
    it runs, :func:`in_callback` is true for ``producer`` on this thread."""
    previous = getattr(_callback_scope, "producer", None)
    _callback_scope.producer = producer
    try:
        return callback(metadata=metadata, exception=exception)
    finally:
        _callback_scope.producer = previous


def call_callback(producer: object, callback: Callable[..., object] | None,
                  metadata: RecordMetadata, exception: Exception | None) -> None:
    """Run the user's ``Callback`` for one completion, synchronously, raising
    what it raised, as Java's ``MockProducer.Completion.complete`` calls
    ``onCompletion`` with no try/catch. A callback returning an awaitable
    cannot be awaited here: the awaitable is closed and a ``TypeError`` raised,
    as the synchronous consumer does for a coroutine listener (a coroutine
    function itself is refused at ``send()``, by
    :func:`reject_coroutine_callback`)."""
    if callback is None:
        return
    result = run_callback(producer, callback, metadata, exception)
    if inspect.isawaitable(result):
        close = getattr(result, "close", None)
        if callable(close):
            close()
        raise TypeError(_COROUTINE_CALLBACK_MESSAGE)


def invoke_callback(producer: object, callback: Callable[..., object] | None,
                    metadata: RecordMetadata, exception: Exception | None) -> Exception | None:
    """:func:`call_callback`, returning the exception the callback raised, if
    any, instead of raising it.

    The caller is the thread running the producer's callbacks; it reports the
    exception to the core (see the module docstring), and the next callback
    must still run. It is not logged here: the core logs it once, as Java's
    ``ProducerBatch.completeFutureAndFireCallbacks`` does.
    """
    try:
        call_callback(producer, callback, metadata, exception)
    except Exception as exc:  # noqa: BLE001 - a user callback must not escape
        return exc
    return None


def mark_completion_thread(producer: object) -> None:
    """Mark the current thread, the sync producer's completion thread, as
    ``producer``'s for its whole life: every piece of user code it runs, a
    ``send()`` callback or a ``then_apply`` function or ``when_complete``
    action of a record's future, then sees :func:`in_callback` true, as Java
    tests the I/O thread itself rather than the callback."""
    _callback_scope.producer = producer


def in_callback(producer: object) -> bool:
    """Whether the current thread is ``producer``'s completion thread (see
    :func:`mark_completion_thread`) or is running a delivery callback of
    ``producer`` elsewhere (Java's ``Thread.currentThread() ==
    this.ioThread``)."""
    return getattr(_callback_scope, "producer", None) is producer


def in_any_callback() -> bool:
    """Whether the current thread is a producer's completion thread (see
    :func:`mark_completion_thread`) or is running a delivery callback of any
    producer, and so blocks a thread some producer's completions run on."""
    return getattr(_callback_scope, "producer", None) is not None
