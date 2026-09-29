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

"""``PartitionInfo``: Java's ``org.apache.kafka.common.PartitionInfo``.

Java's two constructors are one keyword-only constructor: the shorter passes
``new Node[0]`` as ``offlineReplicas``, so ``offline_replicas=()``. Java's
``Node[]`` is ``tuple[Node, ...]`` (CLAUDE.md, Python Binding Conventions,
Types).
"""

from __future__ import annotations

from confluent_kafka._java import java_str

from .node import Node

__all__ = ["PartitionInfo"]


class PartitionInfo:
    """This is used to describe per-partition state in the MetadataResponse.

    Java: ``org.apache.kafka.common.PartitionInfo``.
    """

    __slots__ = ("_topic", "_partition", "_leader", "_replicas",
                 "_in_sync_replicas", "_offline_replicas")

    def __init__(self, *, topic: str, partition: int, leader: Node | None,
                 replicas: tuple[Node, ...], in_sync_replicas: tuple[Node, ...],
                 offline_replicas: tuple[Node, ...] = ()) -> None:
        self._topic = topic
        self._partition = partition
        self._leader = leader
        self._replicas = tuple(replicas)
        self._in_sync_replicas = tuple(in_sync_replicas)
        self._offline_replicas = tuple(offline_replicas)

    def topic(self) -> str:
        """The topic name."""
        return self._topic

    def partition(self) -> int:
        """The partition id."""
        return self._partition

    def leader(self) -> Node | None:
        """The node id of the node currently acting as a leader for this
        partition or ``None`` if there is no leader."""
        return self._leader

    def replicas(self) -> tuple[Node, ...]:
        """The complete set of replicas for this partition regardless of
        whether they are alive or up-to-date. The preferred replica is the head
        of the list."""
        return self._replicas

    def in_sync_replicas(self) -> tuple[Node, ...]:
        """The subset of the replicas that are in sync, that is caught-up to
        the leader and ready to take over as leader if the leader should
        fail."""
        return self._in_sync_replicas

    def offline_replicas(self) -> tuple[Node, ...]:
        """The subset of the replicas that are offline."""
        return self._offline_replicas

    def __hash__(self) -> int:
        return hash((self._topic, self._partition, self._leader, self._replicas,
                     self._in_sync_replicas, self._offline_replicas))

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, PartitionInfo)
        return (self._topic == other._topic
                and self._partition == other._partition
                and self._leader == other._leader
                and self._replicas == other._replicas
                and self._in_sync_replicas == other._in_sync_replicas
                and self._offline_replicas == other._offline_replicas)

    def __str__(self) -> str:
        leader = "none" if self._leader is None else self._leader.id_string()
        return (f"Partition(topic = {java_str(self._topic)}, partition = {self._partition}, "
                f"leader = {leader}, replicas = {_format_node_ids(self._replicas)}, "
                f"isr = {_format_node_ids(self._in_sync_replicas)}, "
                f"offlineReplicas = {_format_node_ids(self._offline_replicas)})")


def _format_node_ids(nodes: tuple[Node, ...] | None) -> str:
    """Extract the node ids from each item in the array and format for
    display."""
    return "[" + ",".join(n.id_string() for n in (nodes or ())) + "]"
