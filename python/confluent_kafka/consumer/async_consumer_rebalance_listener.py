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

"""``AsyncConsumerRebalanceListener``: the asyncio form of
:class:`ConsumerRebalanceListener` (CLAUDE.md, Python Binding Conventions, Class
family).

By rule 3 a callback interface an async class takes has a public asyncio form
beside it, named ``Async`` + the interface's name and placed in its module,
whose methods are ``async def``: ``AsyncConsumer.subscribe(callback=…)`` takes a
:class:`ConsumerRebalanceListener` or an ``AsyncConsumerRebalanceListener``. A
class to subclass (``abc.ABC``), as the Java form: ``on_partitions_revoked`` and
``on_partitions_assigned`` are abstract, so a subclass missing one fails at
construction with ``TypeError``, and ``on_partitions_lost`` awaits
``on_partitions_revoked``, as Java's default calls it. It is not a subclass of
:class:`ConsumerRebalanceListener`, whose methods are plain.

The asyncio consumers await each method on the event loop, inside the call that
delivers it (``poll()``, ``unsubscribe()``, ``close()`` on
``AsyncKafkaConsumer``; ``rebalance()`` and ``lose_partitions()`` on
``AsyncMockConsumer``), and the rebalance does not advance until it returns. A
plain :class:`ConsumerRebalanceListener` given to an asyncio consumer runs on the
event loop and must not block it, as Java's listener must be fast.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["AsyncConsumerRebalanceListener"]


class AsyncConsumerRebalanceListener(ABC):
    """The asyncio form of the callback interface that the user can implement
    to trigger custom actions when the set of partitions assigned to the
    consumer changes.

    Java: ``org.apache.kafka.clients.consumer.ConsumerRebalanceListener``, whose
    methods are ``async def`` here.
    """

    @abstractmethod
    async def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
        """See :meth:`ConsumerRebalanceListener.on_partitions_revoked`."""

    @abstractmethod
    async def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
        """See :meth:`ConsumerRebalanceListener.on_partitions_assigned`."""

    async def on_partitions_lost(self, partitions: set[TopicPartition]) -> None:
        """See :meth:`ConsumerRebalanceListener.on_partitions_lost`. By default it
        awaits ``on_partitions_revoked``, as Java's default calls it; override it
        to distinguish the handling of revoked from lost partitions."""
        await self.on_partitions_revoked(partitions)
