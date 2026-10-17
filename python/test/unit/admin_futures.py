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

"""Reading an admin result's futures in the admin tests (not a test module).

An admin RPC returns its Result at once and its futures complete later (Futures):
a ``KafkaFuture`` on the sync client, an ``AsyncKafkaFuture`` on the asyncio one.
A failed future makes ``get()`` raise ``ExecutionError`` whose ``__cause__`` is
the error, as Java's ``KafkaFuture.get()`` throws ``ExecutionException``.
:func:`assert_future_throws` is Java's ``TestUtils.assertFutureThrows`` (the cause
exactly of the class, a subclass rejected); :func:`failure` is the cause, or
``None`` for a future that succeeded. Every wait is bounded (:data:`WAIT`), so a
future that never completes fails the test instead of hanging it.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka import ExecutionError, _throwable

__all__ = ["WAIT", "assert_future_throws", "async_assert_future_throws", "async_failure",
           "async_value", "failure", "value"]

#: The longest a test waits for a future, in seconds.
WAIT = 15.0


def _cause(error: ExecutionError) -> BaseException:
    cause = error.__cause__
    assert cause is not None
    assert str(error) == _throwable.to_string(cause)
    return cause


def value(future: Any, timeout: float = WAIT) -> Any:
    """``future.get(timeout=…)`` of a sync future."""
    return future.get(timeout=timeout)


async def async_value(future: Any, timeout: float = WAIT) -> Any:
    """``await future.get(timeout=…)`` of an ``AsyncKafkaFuture``."""
    return await future.get(timeout=timeout)


def failure(future: Any, timeout: float = WAIT) -> BaseException | None:
    """The error ``future.get()`` raises an ``ExecutionError`` from, or ``None``
    when the future succeeded."""
    try:
        future.get(timeout=timeout)
    except ExecutionError as error:
        return _cause(error)
    return None


async def async_failure(future: Any, timeout: float = WAIT) -> BaseException | None:
    """:func:`failure` for an ``AsyncKafkaFuture``."""
    try:
        await future.get(timeout=timeout)
    except ExecutionError as error:
        return _cause(error)
    return None


def assert_future_throws(future: Any, cls: type[BaseException],
                         timeout: float = WAIT) -> BaseException:
    """Java's ``TestUtils.assertFutureThrows``: ``get()`` raises ``ExecutionError``
    whose cause is exactly of ``cls``; the cause is returned."""
    cause = failure(future, timeout)
    assert cause is not None, "the future succeeded"
    assert type(cause) is cls, f"{type(cause).__name__} is not {cls.__name__}: {cause}"
    return cause


async def async_assert_future_throws(future: Any, cls: type[BaseException],
                                     timeout: float = WAIT) -> BaseException:
    """:func:`assert_future_throws` for an ``AsyncKafkaFuture``."""
    cause = await async_failure(future, timeout)
    assert cause is not None, "the future succeeded"
    assert type(cause) is cls, f"{type(cause).__name__} is not {cls.__name__}: {cause}"
    return cause
