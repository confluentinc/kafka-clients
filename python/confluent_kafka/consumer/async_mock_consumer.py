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

"""``AsyncMockConsumer``: the asyncio peer of :class:`MockConsumer`, with the
same constructor and the same mock methods (CLAUDE.md, Python Binding
Conventions, Class family). ``rebalance`` is ``async def``: Java's waits on the
listener it runs, which may be ``async def`` here and is awaited. A
``commit_nowait()`` callback may be ``async def`` too: Java's mock runs the
callback inside ``commitAsync``, which a plain ``def`` cannot await, so it is
awaited in a task on the loop, and the next awaited call waits for it, as on
``AsyncKafkaConsumer``."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Generic, TypeVar

from ._mock_core import MockConsumerCore
from .async_consumer import AsyncConsumer
from .mock_consumer import reset_strategy_of

if TYPE_CHECKING:
    from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["AsyncMockConsumer"]

K = TypeVar("K")
V = TypeVar("V")

_LOG = logging.getLogger("confluent_kafka.consumer")


class AsyncMockConsumer(MockConsumerCore[K, V], AsyncConsumer[K, V], Generic[K, V]):
    """The asyncio peer of ``MockConsumer``, a mock of the ``Consumer``
    interface you can use for testing code that uses Kafka.

    Java: ``org.apache.kafka.clients.consumer.MockConsumer<K, V>``.
    """

    def __init__(self, *, offset_reset_strategy: str) -> None:
        """See :meth:`MockConsumer.__init__`."""
        strategy = reset_strategy_of(offset_reset_strategy)
        AsyncConsumer.__init__(self)
        self._init_mock(strategy)

    async def rebalance(self, *, new_assignment: Iterable[TopicPartition]) -> None:
        """See :meth:`MockConsumer.rebalance`; a coroutine listener method is
        awaited. Until the rebalance returns, a call from another task or
        thread raises ``ConcurrentModificationError`` *(deviation: Java's caller
        waits for the synchronized rebalance)*; the listener's own calls back
        into the consumer pass, and so does ``wakeup()``."""
        await self._a_rebalance(new_assignment)

    # ---- coroutine commit callbacks -------------------------------------------
    _COROUTINE_REQUIRES = "a running event loop"

    def _defer_commit_callback(self, start: Callable[[], object]) -> bool:
        """Run a commit callback in a task on the running loop: ``start()``
        calls it, and what it returns is awaited when awaitable. The task waits
        for the previously deferred one first, so the callbacks start in commit
        order, each after the previous one ended. An exception the callback
        raises, ``CancelledError`` included, is logged: the ``commit_nowait()``
        Java's mock would raise it from has returned. The awaited calls wait for
        the task (``_await_commit_callbacks``)."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        previous = self._commit_callback_tail

        async def run() -> None:
            if previous is not None and not previous.done():
                await asyncio.wait({previous})
            try:
                result = start()
                if inspect.isawaitable(result):
                    await result
            except BaseException as exc:  # noqa: BLE001 - nothing it can propagate to
                _LOG.error("Error in OffsetCommitCallback", exc_info=exc)

        task = loop.create_task(run())
        self._commit_callback_tail = task
        self._commit_callback_tasks.add(task)
        task.add_done_callback(self._commit_callback_tasks.discard)
        return True
