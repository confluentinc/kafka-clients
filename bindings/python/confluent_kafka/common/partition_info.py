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

"""``PartitionInfo`` — per-partition metadata state.

Translated from ``org.apache.kafka.common.PartitionInfo`` (Apache Kafka
4.3.1). Java's two constructors — with and without ``offlineReplicas`` —
collapse to one keyword-only constructor with ``offline_replicas=()`` (the
shorter overload's ``new Node[0]`` delegation value). Java's ``Node[]`` arrays
map to ``tuple[Node, ...]`` (rule 3.8).
"""

from __future__ import annotations

from .node import Node


class PartitionInfo:
    """Per-partition metadata state.

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
        self._replicas = replicas
        self._in_sync_replicas = in_sync_replicas
        self._offline_replicas = offline_replicas

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def leader(self) -> Node | None:
        return self._leader

    def replicas(self) -> tuple[Node, ...]:
        return self._replicas

    def in_sync_replicas(self) -> tuple[Node, ...]:
        return self._in_sync_replicas

    def offline_replicas(self) -> tuple[Node, ...]:
        return self._offline_replicas

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, PartitionInfo):
            return NotImplemented
        return (self._topic == other._topic
                and self._partition == other._partition
                and self._leader == other._leader
                and self._replicas == other._replicas
                and self._in_sync_replicas == other._in_sync_replicas
                and self._offline_replicas == other._offline_replicas)

    def __hash__(self) -> int:
        return hash((self._topic, self._partition, self._leader,
                     self._replicas, self._in_sync_replicas,
                     self._offline_replicas))

    @staticmethod
    def _format_node_ids(nodes: tuple[Node, ...]) -> str:
        return "[" + ",".join(n.id_string() for n in nodes) + "]"

    def __repr__(self) -> str:
        # Java toString:
        # Partition(topic = .., partition = .., leader = .., replicas = ..,
        #           isr = .., offlineReplicas = ..)
        leader = "none" if self._leader is None else self._leader.id_string()
        return (f"Partition(topic = {self._topic}, "
                f"partition = {self._partition}, leader = {leader}, "
                f"replicas = {self._format_node_ids(self._replicas)}, "
                f"isr = {self._format_node_ids(self._in_sync_replicas)}, "
                f"offlineReplicas = "
                f"{self._format_node_ids(self._offline_replicas)})")
