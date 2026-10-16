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

"""``DescribeClassicGroupsResult``: Java's
``org.apache.kafka.clients.admin.DescribeClassicGroupsResult``.

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
    from confluent_kafka.admin.classic_group_description import ClassicGroupDescription
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeClassicGroupsResult"]


class DescribeClassicGroupsResult(Generic[KindT]):
    """The result of the ``Admin.describe_classic_groups()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeClassicGroupsResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(
            self, *,
            futures: Mapping[str, KafkaFuture[ClassicGroupDescription] | AsyncKafkaFuture[ClassicGroupDescription]]) -> None:
        self._futures = dict(futures)
        self._kind: type[Sync] | type[Async] = _kind_of(self._futures.values())

    @classmethod
    def _new(
            cls, *,
            futures: Mapping[str, KafkaFuture[ClassicGroupDescription] | AsyncKafkaFuture[ClassicGroupDescription]],
            kind: type[Sync] | type[Async]) -> DescribeClassicGroupsResult[Any]:
        """The clients' builder: Java's public ``DescribeClassicGroupsResult(futures)``
        with the client kind set by the caller."""
        result = cls.__new__(cls)
        result._futures = dict(futures)
        result._kind = kind
        return result

    def described_groups(
            self) -> dict[str, KafkaFuture[ClassicGroupDescription] | AsyncKafkaFuture[ClassicGroupDescription]]:
        """Return a map from group id to futures which yield group descriptions."""
        return dict(self._futures)

    def all(
            self) -> KafkaFuture[dict[str, ClassicGroupDescription]] | AsyncKafkaFuture[dict[str, ClassicGroupDescription]]:
        """Return a future which yields all ClassicGroupDescription objects, if all the
        describes succeed."""
        futures = self._futures

        def collect(_: None) -> dict[str, ClassicGroupDescription]:
            # allOf ensured that all the futures completed successfully.
            return {key: future.get_now(value_if_absent=None) for key, future in futures.items()}

        return self._kind._FUTURE.all_of(futures=tuple(futures.values())).then_apply(function=collect)
