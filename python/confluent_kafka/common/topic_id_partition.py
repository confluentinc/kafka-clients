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

"""``TopicIdPartition``: Java's ``org.apache.kafka.common.TopicIdPartition``.

A ``common`` type the share consumer reaches (CLAUDE.md, Python Binding
Conventions, Scope). Java's two constructors, ``(topicId, topicPartition)`` and
``(topicId, partition, topic)``, are one keyword-only constructor matched by
``java_forms``.
"""

from __future__ import annotations

from typing import overload

from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka._java import java_str
from confluent_kafka.null_pointer_error import NullPointerError

from .topic_partition import TopicPartition
from .uuid import Uuid

__all__ = ["TopicIdPartition"]


class TopicIdPartition:
    """This represents universally unique identifier with topic id for a topic
    partition. This makes sure that topics recreated with the same name will
    always have unique topic identifiers.

    Java: ``org.apache.kafka.common.TopicIdPartition``.
    """

    __slots__ = ("_topic_id", "_topic_partition")

    @overload
    def __init__(self, *, topic_id: Uuid, topic_partition: TopicPartition) -> None: ...
    @overload
    def __init__(self, *, topic_id: Uuid, partition: int, topic: str | None) -> None: ...

    @java_forms(Form("topic_id", "topic_partition"), Form("topic_id", "partition", "topic"))
    def __init__(self, *, topic_id: Uuid, topic_partition: TopicPartition | None = None,
                 partition: int | None = None, topic: str | None = UNSET) -> None:
        """Create an instance with the provided parameters: the topic id and
        either the topic partition, or the partition id and the topic name (or
        null: ``topic=None`` is given)."""
        if topic_id is None:
            raise NullPointerError(message="topicId can not be null")
        self._topic_id = topic_id
        if topic_partition is not None:
            self._topic_partition = topic_partition
        else:
            assert partition is not None
            self._topic_partition = TopicPartition(topic=topic, partition=partition)  # type: ignore[arg-type]

    def topic_id(self) -> Uuid:
        """Universally unique id representing this topic partition."""
        return self._topic_id

    def topic(self) -> str | None:
        """The topic name or ``None`` if it is unknown."""
        return self._topic_partition.topic()

    def partition(self) -> int:
        """The partition id."""
        return self._topic_partition.partition()

    def topic_partition(self) -> TopicPartition:
        """Topic partition representing this instance."""
        return self._topic_partition

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, TopicIdPartition)
        return (self._topic_id == other._topic_id
                and self._topic_partition == other._topic_partition)

    def __hash__(self) -> int:
        return hash((self._topic_id, self._topic_partition))

    def __str__(self) -> str:
        return (java_str(self._topic_id) + ":" + java_str(self.topic()) + "-"
                + str(self.partition()))
