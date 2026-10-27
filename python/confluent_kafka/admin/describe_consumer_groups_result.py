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

"""``DescribeConsumerGroupsResult``: Java's
``org.apache.kafka.clients.admin.DescribeConsumerGroupsResult``.

Java's constructor is public, so it stays the keyword-only ``__init__``; the
client-kind marker is read from the futures given (``AsyncKafkaFuture``\\ s: the async
kind, else the sync one), and the clients build it through ``_new``, which sets the kind
explicitly, as for every result class (CLAUDE.md, Python Binding Conventions, Class
family). ``describedGroups()`` returns a copy of the stored map, so each call gives a
new ``dict``. ``all()`` is Java's ``allOf(...).thenApply(...)``, through the futures'
``all_of`` and ``then_apply``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync, _kind_of

if TYPE_CHECKING:
    from confluent_kafka.admin.consumer_group_description import (
        ConsumerGroupDescription,
    )
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeConsumerGroupsResult"]


class DescribeConsumerGroupsResult(Generic[KindT]):
    """The result of the ``Admin.describe_consumer_groups()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeConsumerGroupsResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(
            self, *,
            futures: Mapping[str, KafkaFuture[ConsumerGroupDescription] | AsyncKafkaFuture[ConsumerGroupDescription]]) -> None:
        if futures is None:
            raise TypeError("futures must not be None")
        self._futures = dict(futures)
        self._kind: type[Sync] | type[Async] = _kind_of(self._futures.values())

    @classmethod
    def _new(
            cls, *,
            futures: Mapping[str, KafkaFuture[ConsumerGroupDescription] | AsyncKafkaFuture[ConsumerGroupDescription]],
            kind: type[Sync] | type[Async]) -> DescribeConsumerGroupsResult[Any]:
        """The clients' builder: Java's public ``DescribeConsumerGroupsResult(futures)``
        with the client kind set by the caller."""
        result = cls.__new__(cls)
        result._futures = None if futures is None else dict(futures)
        result._kind = kind
        return result

    def described_groups(
            self) -> dict[str, KafkaFuture[ConsumerGroupDescription] | AsyncKafkaFuture[ConsumerGroupDescription]]:
        """Return a map from group id to futures which yield group descriptions."""
        return dict(self._futures)

    def all(
            self) -> KafkaFuture[dict[str, ConsumerGroupDescription]] | AsyncKafkaFuture[dict[str, ConsumerGroupDescription]]:
        """Return a future which yields all ConsumerGroupDescription objects, if all the
        describes succeed."""
        futures = self._futures

        def collect(_: None) -> dict[str, ConsumerGroupDescription]:
            # allOf ensured that all the futures completed successfully.
            return {key: future.get_now(value_if_absent=None) for key, future in futures.items()}

        return self._kind._FUTURE.all_of(futures=tuple(futures.values())).then_apply(function=collect)
