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

"""``ListConsumerGroupOffsetsSpec``: Java's
``org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec``.

Java builds it with its setter, so the constructor also takes the setter's field
as a keyword, which calls the setter when given. The
setter and the getter share the name ``topic_partitions``: no argument reads,
one argument sets and returns the object, an explicit ``None`` setting Java's
``null`` ("all topic partitions").
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, overload

from confluent_kafka._args import UNSET
from confluent_kafka._java import java_str

from ._values import frozen

if TYPE_CHECKING:
    from confluent_kafka.common import TopicPartition

__all__ = ["ListConsumerGroupOffsetsSpec"]


class ListConsumerGroupOffsetsSpec:
    """Specification of consumer group offsets to list using the admin
    client's ``list_consumer_group_offsets()``.

    Java: ``org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec``.
    """

    __slots__ = ("_topic_partitions",)

    def __init__(self, *, topic_partitions: Iterable[TopicPartition] | None = UNSET) -> None:
        self._topic_partitions: list[TopicPartition] | None = None
        if topic_partitions is not UNSET:
            self.topic_partitions(topic_partitions=topic_partitions)

    @overload
    def topic_partitions(self) -> list[TopicPartition] | None: ...
    @overload
    def topic_partitions(self, *, topic_partitions: Iterable[TopicPartition] | None
                         ) -> ListConsumerGroupOffsetsSpec: ...

    def topic_partitions(self, *, topic_partitions: Iterable[TopicPartition] | None = UNSET
                         ) -> list[TopicPartition] | None | ListConsumerGroupOffsetsSpec:
        """With no argument: the topic partitions whose offsets are to be
        listed for a consumer group; ``None`` indicates that offsets of all
        partitions of the group are to be listed.

        With ``topic_partitions``: set the topic partitions whose offsets are to
        be listed for a consumer group (``None`` includes all topic partitions)
        and return this ``ListConsumerGroupOffsetsSpec``.
        """
        if topic_partitions is UNSET:
            return self._topic_partitions
        self._topic_partitions = None if topic_partitions is None else list(topic_partitions)
        return self

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, ListConsumerGroupOffsetsSpec):
            return NotImplemented
        return self._topic_partitions == other._topic_partitions

    def __hash__(self) -> int:
        return hash((frozen(self._topic_partitions),))

    def __str__(self) -> str:
        return "ListConsumerGroupOffsetsSpec(topicPartitions=" + java_str(
            self._topic_partitions) + ")"
