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

"""``MemberAssignment``: Java's ``org.apache.kafka.clients.admin.MemberAssignment``."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from confluent_kafka._java import java_str

if TYPE_CHECKING:
    from confluent_kafka.common import TopicPartition

__all__ = ["MemberAssignment"]


class MemberAssignment:
    """A description of the assignments of a specific group member.

    Java: ``org.apache.kafka.clients.admin.MemberAssignment``.
    """

    __slots__ = ("_topic_partitions",)

    def __init__(self, *, topic_partitions: Iterable[TopicPartition] | None) -> None:
        """Creates an instance with the specified parameters: the topic
        partitions (``None`` is an empty set)."""
        self._topic_partitions: frozenset[TopicPartition] = (
            frozenset() if topic_partitions is None else frozenset(topic_partitions))

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, MemberAssignment)
        return self._topic_partitions == other._topic_partitions

    def __hash__(self) -> int:
        return hash(self._topic_partitions)

    def topic_partitions(self) -> set[TopicPartition]:
        """The topic partitions assigned to a group member."""
        return set(self._topic_partitions)

    def __str__(self) -> str:
        return "(topicPartitions=" + ",".join(
            java_str(tp) for tp in self._topic_partitions) + ")"
