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

"""``DescribeClusterResult``: Java's ``org.apache.kafka.clients.admin.DescribeClusterResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). A ``Collection`` yields
a ``list``: Java's ``KafkaAdminClient`` builds no ``Set`` for it (Types).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync

if TYPE_CHECKING:
    from confluent_kafka.common import Node
    from confluent_kafka.common.acl import AclOperation
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeClusterResult"]


class DescribeClusterResult(Generic[KindT]):
    """The result of the ``Admin.describe_cluster()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeClusterResult``.
    """

    __slots__ = ("_nodes", "_controller", "_cluster_id", "_authorized_operations", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DescribeClusterResult cannot be constructed directly; use the admin client's "
            "describe_cluster()")

    @classmethod
    def _new(
            cls, *, nodes: KafkaFuture[list[Node]] | AsyncKafkaFuture[list[Node]],
            controller: KafkaFuture[Node] | AsyncKafkaFuture[Node],
            cluster_id: KafkaFuture[str] | AsyncKafkaFuture[str],
            authorized_operations: KafkaFuture[set[AclOperation] | None] | AsyncKafkaFuture[set[AclOperation] | None],
            kind: type[Sync] | type[Async]) -> DescribeClusterResult[Any]:
        """Java's package-private ``DescribeClusterResult(nodes, controller, clusterId,
        authorizedOperations)``."""
        result = cls.__new__(cls)
        result._nodes = nodes
        result._controller = controller
        result._cluster_id = cluster_id
        result._authorized_operations = authorized_operations
        result._kind = kind
        return result

    def nodes(self) -> KafkaFuture[list[Node]] | AsyncKafkaFuture[list[Node]]:
        """Returns a future which yields a collection of nodes."""
        return self._nodes

    def controller(self) -> KafkaFuture[Node] | AsyncKafkaFuture[Node]:
        """Returns a future which yields the current controller node.

        When using ``bootstrap.servers``, the controller refer to a random broker. When
        using ``bootstrap.controllers``, it refers to the current voter leader.
        """
        return self._controller

    def cluster_id(self) -> KafkaFuture[str] | AsyncKafkaFuture[str]:
        """Returns a future which yields the current cluster id."""
        return self._cluster_id

    def authorized_operations(
            self) -> KafkaFuture[set[AclOperation] | None] | AsyncKafkaFuture[set[AclOperation] | None]:
        """Returns a future which yields authorized operations. The future value will be
        non-null if the broker supplied this information, and null otherwise."""
        return self._authorized_operations
