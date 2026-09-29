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
listener it runs, which may be ``async def`` here and is awaited. As
:class:`AsyncConsumer`, it has no ``current_lag()``: a mock does not generate
what its base class does not (Implementation over the FFI)."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Generic, TypeVar, overload

from ._mock_core import MockConsumerCore
from .async_consumer import AsyncConsumer
from .mock_consumer import reset_strategy_of
from .offset_reset_strategy import OffsetResetStrategy

if TYPE_CHECKING:
    from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["AsyncMockConsumer"]

K = TypeVar("K")
V = TypeVar("V")


class AsyncMockConsumer(MockConsumerCore[K, V], AsyncConsumer[K, V], Generic[K, V]):
    """The asyncio peer of ``MockConsumer``, a mock of the ``Consumer``
    interface you can use for testing code that uses Kafka.

    Java: ``org.apache.kafka.clients.consumer.MockConsumer<K, V>``.
    """

    @overload
    def __init__(self, *, offset_reset_strategy: str) -> None: ...
    @overload
    def __init__(self, *, offset_reset_strategy: OffsetResetStrategy) -> None: ...

    def __init__(self, *, offset_reset_strategy: str | OffsetResetStrategy) -> None:
        """See :meth:`MockConsumer.__init__`.

        Deprecated: the ``OffsetResetStrategy`` form. Since 4.0. Use the ``str``
        form instead.
        """
        strategy = reset_strategy_of(offset_reset_strategy)
        AsyncConsumer.__init__(self)
        self._init_mock(strategy)

    async def rebalance(self, *, new_assignment: Iterable[TopicPartition]) -> None:
        """See :meth:`MockConsumer.rebalance`; a coroutine listener method is
        awaited."""
        await self._a_rebalance(new_assignment)
