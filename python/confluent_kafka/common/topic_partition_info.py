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

"""``TopicPartitionInfo``: Java's ``org.apache.kafka.common.TopicPartitionInfo``.

Java's two constructors ``(partition, leader, replicas, isr)`` and
``(partition, leader, replicas, isr, elr, lastKnownElr)`` are one keyword-only
constructor matched by ``java_forms`` (CLAUDE.md, Python Binding Conventions,
Signatures): ``elr`` and ``last_known_elr`` are given together or not at all,
and the four-argument form leaves both ``None`` (Java assigns ``null``), so two
stubs, one per constructor.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from confluent_kafka._args import Form, java_forms
from confluent_kafka._java import java_str
from confluent_kafka.common.node import Node

__all__ = ["TopicPartitionInfo"]


class TopicPartitionInfo:
    """A class containing leadership, replicas and ISR information for a topic
    partition.

    Java: ``org.apache.kafka.common.TopicPartitionInfo``.
    """

    __slots__ = ("_partition", "_leader", "_replicas", "_isr", "_elr", "_last_known_elr")

    @overload
    def __init__(self, *, partition: int, leader: Node | None, replicas: Sequence[Node],
                 isr: Sequence[Node], elr: Sequence[Node],
                 last_known_elr: Sequence[Node]) -> None: ...
    @overload
    def __init__(self, *, partition: int, leader: Node | None, replicas: Sequence[Node],
                 isr: Sequence[Node]) -> None: ...

    @java_forms(Form("partition", "leader", "replicas", "isr", "elr", "last_known_elr",
                     defaults={"elr": None, "last_known_elr": None}),
                Form("partition", "leader", "replicas", "isr"))
    def __init__(self, *, partition: int, leader: Node | None, replicas: Sequence[Node],
                 isr: Sequence[Node], elr: Sequence[Node] | None = None,
                 last_known_elr: Sequence[Node] | None = None) -> None:
        """Create an instance of this class with the provided parameters.

        ``partition`` is the partition id; ``leader`` the leader of the
        partition or ``None`` if there is none; ``replicas`` the replicas of
        the partition in the same order as the replica assignment (the
        preferred replica is the head of the list); ``isr`` the in-sync
        replicas; ``elr`` the eligible leader replicas; ``last_known_elr`` the
        last known eligible leader replicas.
        """
        self._partition = partition
        self._leader = leader
        self._replicas = tuple(replicas)
        self._isr = tuple(isr)
        self._elr = None if elr is None else tuple(elr)
        self._last_known_elr = None if last_known_elr is None else tuple(last_known_elr)

    def partition(self) -> int:
        """Return the partition id."""
        return self._partition

    def leader(self) -> Node | None:
        """Return the leader of the partition or ``None`` if there is none."""
        return self._leader

    def replicas(self) -> list[Node]:
        """Return the replicas of the partition in the same order as the
        replica assignment. The preferred replica is the head of the list.

        Brokers with version lower than 0.11.0.0 return the replicas in
        unspecified order due to a bug."""
        return list(self._replicas)

    def isr(self) -> list[Node]:
        """Return the in-sync replicas of the partition. Note that the ordering
        of the result is unspecified."""
        return list(self._isr)

    def elr(self) -> list[Node] | None:
        """Return the eligible leader replicas of the partition. Note that the
        ordering of the result is unspecified."""
        return None if self._elr is None else list(self._elr)

    def last_known_elr(self) -> list[Node] | None:
        """Return the last known eligible leader replicas of the partition. Note
        that the ordering of the result is unspecified."""
        return None if self._last_known_elr is None else list(self._last_known_elr)

    def __str__(self) -> str:
        elr_string = "N/A" if self._elr is None else ", ".join(str(n) for n in self._elr)
        last_known_elr_string = ("N/A" if self._last_known_elr is None
                                 else ", ".join(str(n) for n in self._last_known_elr))
        return ("(partition=" + str(self._partition) + ", leader=" + java_str(self._leader)
                + ", replicas=" + ", ".join(str(n) for n in self._replicas)
                + ", isr=" + ", ".join(str(n) for n in self._isr)
                + ", elr=" + elr_string + ", lastKnownElr=" + last_known_elr_string + ")")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, TopicPartitionInfo)
        return (self._partition == other._partition
                and self._leader == other._leader
                and self._replicas == other._replicas
                and self._isr == other._isr
                and self._elr == other._elr
                and self._last_known_elr == other._last_known_elr)

    def __hash__(self) -> int:
        return hash((self._partition, self._leader, self._replicas, self._isr, self._elr,
                     self._last_known_elr))
