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

"""Reading a ``send()`` future in the producer tests (not a test module).

A record's future is a ``KafkaFuture`` (``AsyncKafkaFuture`` on the asyncio
producers): a failed record makes ``get()`` raise ``ExecutionError`` whose
``__cause__`` is the error, as Java's ``Future.get()`` throws
``ExecutionException``. :func:`assert_send_throws` is Java's
``TestUtils.assertFutureThrows`` (``assert_future_throws`` of the
``KafkaFutureTest`` port); :func:`failure` is the cause, or ``None`` for a
record that succeeded, where a Java test reads ``getCause()`` of what ``get()``
threw.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka import ExecutionError, _throwable

from .test_kafka_future import SyncKind, assert_future_throws

__all__ = ["SYNC", "assert_send_throws", "async_assert_send_throws", "async_failure",
           "failure"]

#: The sync future kind of the ``KafkaFutureTest`` port.
SYNC = SyncKind()


def _cause(error: ExecutionError) -> BaseException:
    cause = error.__cause__
    assert cause is not None
    assert str(error) == _throwable.to_string(cause)
    return cause


def failure(future: Any, timeout: float | None = None) -> BaseException | None:
    """The error ``future.get()`` raises an ``ExecutionError`` from, or
    ``None`` when the record succeeded."""
    try:
        if timeout is None:
            future.get()
        else:
            future.get(timeout=timeout)
    except ExecutionError as error:
        return _cause(error)
    return None


async def async_failure(future: Any, timeout: float | None = None) -> BaseException | None:
    """:func:`failure` for an ``AsyncKafkaFuture``."""
    try:
        if timeout is None:
            await future.get()
        else:
            await future.get(timeout=timeout)
    except ExecutionError as error:
        return _cause(error)
    return None


def assert_send_throws(future: Any, cls: type[BaseException],
                       timeout: float | None = None) -> BaseException:
    """Java's ``TestUtils.assertFutureThrows`` on a sync ``send()`` future: its
    cause is exactly of ``cls``, and returned."""
    return assert_future_throws(SYNC, future, cls, timeout)


async def async_assert_send_throws(future: Any, cls: type[BaseException],
                                   timeout: float | None = None) -> BaseException:
    """:func:`assert_send_throws` for an ``AsyncKafkaFuture``."""
    cause = await async_failure(future, timeout)
    assert type(cause) is cls, cause
    return cause
