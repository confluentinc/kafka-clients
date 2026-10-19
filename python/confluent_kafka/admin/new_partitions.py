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

"""``NewPartitions``: Java's ``org.apache.kafka.clients.admin.NewPartitions``.

Java's constructor is private, so ``NewPartitions()`` raises ``TypeError``
naming the static factory; Java's private constructor is ``_new`` (CLAUDE.md,
Python Binding Conventions, Class family). Java's two
``increaseTo`` overloads are one ``increase_to``: ``(totalCount)`` passes
``null`` as the assignments. Java declares no ``equals`` / ``hashCode``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, overload

from confluent_kafka._args import MISSING
from confluent_kafka._java import java_str

__all__ = ["NewPartitions"]


class NewPartitions:
    """Describes new partitions for a particular topic in a call to the admin
    client's ``create_partitions()``.

    Java: ``org.apache.kafka.clients.admin.NewPartitions``.
    """

    __slots__ = ("_total_count", "_new_assignments")

    _total_count: int
    _new_assignments: tuple[tuple[int, ...], ...] | None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "NewPartitions cannot be constructed directly; use NewPartitions.increase_to(...)")

    @classmethod
    def _new(cls, *, total_count: int,
             new_assignments: Sequence[Sequence[int]] | None) -> NewPartitions:
        """Java's private ``NewPartitions(int totalCount, List<List<Integer>>
        newAssignments)``."""
        partitions = cls.__new__(cls)
        partitions._total_count = total_count
        partitions._new_assignments = (
            None if new_assignments is None else tuple(tuple(a) for a in new_assignments))
        return partitions

    @overload
    @staticmethod
    def increase_to(*, total_count: int) -> NewPartitions: ...
    @overload
    @staticmethod
    def increase_to(*, total_count: int,
                    new_assignments: Sequence[Sequence[int]] | None) -> NewPartitions: ...

    @staticmethod
    def increase_to(*, total_count: int,
                    new_assignments: Sequence[Sequence[int]] | None = MISSING) -> NewPartitions:
        """Increase the partition count for a topic to the given
        ``total_count``, assigning the new partitions according to the given
        ``new_assignments``, or, when it is left out (Java's ``increaseTo(totalCount)``
        passes ``null``) or ``None``, leaving the assignment of new replicas to
        brokers to the broker.

        The length of the given ``new_assignments`` should equal
        ``total_count - old_count``, since the assignment of existing partitions
        are not changed. Each inner list of ``new_assignments`` should have a
        length equal to the topic's replication factor. The first broker id in
        each inner list is the "preferred replica".

        For example, suppose a topic currently has a replication factor of 2,
        and has 3 partitions. The number of partitions can be increased to 6
        using ``NewPartitions.increase_to(total_count=6, new_assignments=[[1, 2],
        [2, 3], [3, 1]])``: partition 3's preferred leader will be broker 1,
        partition 4's preferred leader will be broker 2 and partition 5's
        preferred leader will be broker 3.
        """
        if new_assignments is MISSING:
            new_assignments = None
        return NewPartitions._new(total_count=total_count, new_assignments=new_assignments)

    def total_count(self) -> int:
        """The total number of partitions after the operation succeeds."""
        return self._total_count

    def assignments(self) -> list[list[int]] | None:
        """The replica assignments for the new partitions, or ``None`` if the
        assignment will be done by the controller."""
        if self._new_assignments is None:
            return None
        return [list(a) for a in self._new_assignments]

    def __str__(self) -> str:
        return ("(totalCount=" + str(self.total_count()) + ", newAssignments="
                + java_str(self.assignments()) + ")")
