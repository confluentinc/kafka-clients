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

"""``DescribeClientQuotasResult``: Java's
``org.apache.kafka.clients.admin.DescribeClientQuotasResult``.

Java's constructor is public, so it stays the keyword-only ``__init__``; the
client-kind marker is read from the futures given (``AsyncKafkaFuture``\\ s: the async
kind, else the sync one), and the clients build it through ``_new``, which sets the kind
explicitly, as for every result class (CLAUDE.md, Python Binding Conventions, Class
family).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync, _kind_of

if TYPE_CHECKING:
    from confluent_kafka.common import ClientQuotaEntity
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeClientQuotasResult"]


class DescribeClientQuotasResult(Generic[KindT]):
    """The result of the ``Admin.describe_client_quotas()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeClientQuotasResult``.
    """

    __slots__ = ("_entities", "_kind")

    def __init__(
            self, *,
            entities: KafkaFuture[dict[ClientQuotaEntity, dict[str, float]]] | AsyncKafkaFuture[dict[ClientQuotaEntity, dict[str, float]]]) -> None:
        """Maps an entity to its configured quota value(s). Note if no value is defined
        for a quota type for that entity's config, then it is not included in the
        resulting value map.

        :param entities: future for the collection of entities that matched the filter
        """
        self._entities = entities
        self._kind: type[Sync] | type[Async] = _kind_of((self._entities,))

    @classmethod
    def _new(
            cls, *,
            entities: KafkaFuture[dict[ClientQuotaEntity, dict[str, float]]] | AsyncKafkaFuture[dict[ClientQuotaEntity, dict[str, float]]],
            kind: type[Sync] | type[Async]) -> DescribeClientQuotasResult[Any]:
        """The clients' builder: Java's public ``DescribeClientQuotasResult(entities)``
        with the client kind set by the caller.

        :param entities: future for the collection of entities that matched the filter
        """
        result = cls.__new__(cls)
        result._entities = entities
        result._kind = kind
        return result

    def entities(
            self) -> KafkaFuture[dict[ClientQuotaEntity, dict[str, float]]] | AsyncKafkaFuture[dict[ClientQuotaEntity, dict[str, float]]]:
        """Returns a map from quota entity to a future which can be used to check the
        status of the operation."""
        return self._entities
