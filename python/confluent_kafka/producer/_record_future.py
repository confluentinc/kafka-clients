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

"""The future ``send()`` returns (private): Java's ``FutureRecordMetadata``.

Every producer's ``send()`` returns a :class:`~confluent_kafka.common.KafkaFuture`
(an :class:`~confluent_kafka.common.AsyncKafkaFuture` on the asyncio
producers) over Python state, a ``_KafkaFutureImpl`` the record's completion
completes once its callback has returned (CLAUDE.md, Python Binding
Conventions, Futures and Threads and callbacks). The classes here keep Java's
``FutureRecordMetadata.cancel()`` answer: ``False``, cancelling nothing, since
the record is already on its way. The inner future :meth:`to_completion_stage`
returns refuses cancellation in the same way, so nothing but the record's
completion ends it.
"""

from __future__ import annotations

import asyncio
from typing import Any

from confluent_kafka.common.kafka_future import (
    _ERROR,
    AsyncKafkaFuture,
    KafkaFuture,
    _InnerAsyncFuture,
    _InnerFuture,
    _KafkaFutureImpl,
)

from .record_metadata import RecordMetadata

__all__ = ["AsyncRecordFuture", "RecordFuture", "complete_record"]


def complete_record(future: _KafkaFutureImpl[RecordMetadata], metadata: RecordMetadata,
                    exception: BaseException | None) -> None:
    """Complete a record's future: with ``metadata``, or with ``exception`` as
    an ordinary failure, a ``CancelledError`` included. Java's
    ``FutureRecordMetadata`` is never cancelled (``isCancelled()`` is false)
    and boxes every error in ``ExecutionException`` (``valueOrError``), so
    ``get()`` raises ``ExecutionError`` from it."""
    if exception is None:
        future.complete(metadata)
    else:
        future._settle(_ERROR, exception)


class _RecordInnerFuture(_InnerFuture[RecordMetadata]):
    """The ``concurrent.futures.Future`` of a :class:`RecordFuture`: it cannot
    be cancelled, as Java's ``FutureRecordMetadata``."""

    def cancel(self) -> bool:
        return False


class _AsyncRecordInnerFuture(_InnerAsyncFuture[RecordMetadata]):
    """The ``asyncio.Future`` of an :class:`AsyncRecordFuture`: it cannot be
    cancelled, as Java's ``FutureRecordMetadata``. A task awaiting it directly
    is therefore cancelled only once the record completes; bound such a wait
    with ``get(timeout=…)`` instead."""

    def cancel(self, msg: Any | None = None) -> bool:
        return False


class RecordFuture(KafkaFuture[RecordMetadata]):
    """The ``KafkaFuture`` the sync producers' ``send()`` returns."""

    __slots__ = ()

    def cancel(self, *, may_interrupt_if_running: bool) -> bool:
        """Java's ``FutureRecordMetadata.cancel``: always ``False``; the record
        is not cancelled, nor is any future built from this one."""
        return False

    def _new_inner(self) -> _RecordInnerFuture:
        return _RecordInnerFuture()


class AsyncRecordFuture(AsyncKafkaFuture[RecordMetadata]):
    """The ``AsyncKafkaFuture`` the asyncio producers' ``send()`` returns."""

    __slots__ = ()

    def cancel(self, *, may_interrupt_if_running: bool) -> bool:
        """Java's ``FutureRecordMetadata.cancel``: always ``False``; the record
        is not cancelled, nor is any future built from this one."""
        return False

    def _new_inner(self) -> asyncio.Future[RecordMetadata]:
        loop = self._loop
        if loop is None:
            loop = asyncio.get_running_loop()
            self._loop = loop
        return _AsyncRecordInnerFuture(loop=loop)
