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

"""``TopicIdPartition`` — a topic id together with a topic partition.

Translated from ``org.apache.kafka.common.TopicIdPartition`` (Apache Kafka
4.3.1). Java has two constructors — ``(Uuid topicId, int partition, String
topic)`` and ``(Uuid topicId, TopicPartition topicPartition)`` — collapsed into
one keyword-only constructor with an ``@overload`` stub per form (D27). Exactly
one form must be given.

NOTE: this type appears on Java's share-consumer surface only
(``ShareConsumer.commitSync`` / ``AcknowledgementCommitCallback``). With the
share consumer out of scope (C1) it is defined here for parity (rule 10a) but is
referenced by no producer/consumer method.
"""

from __future__ import annotations

from typing import overload

from confluent_kafka._args import exactly_one

from .topic_partition import TopicPartition
from .uuid import Uuid


class TopicIdPartition:
    """A topic id and topic partition.

    Java: ``org.apache.kafka.common.TopicIdPartition``.
    """

    __slots__ = ("_topic_id", "_topic_partition")

    @overload
    def __init__(self, *, topic_id: Uuid, partition: int,
                 topic: str | None) -> None: ...
    @overload
    def __init__(self, *, topic_id: Uuid,
                 topic_partition: TopicPartition) -> None: ...

    def __init__(self, *, topic_id: Uuid, partition: int | None = None,
                 topic: str | None = None,
                 topic_partition: TopicPartition | None = None) -> None:
        # Distinguish the two Java constructors by which key was supplied. A
        # null topic is legal in the (topic_id, partition, topic) form, so the
        # discriminator is partition vs topic_partition (not topic).
        chosen = exactly_one(
            "TopicIdPartition",
            partition=partition,
            topic_partition=topic_partition,
        )
        # Java: Objects.requireNonNull(topicId, "topicId can not be null").
        if topic_id is None:
            raise TypeError("topicId can not be null")
        self._topic_id = topic_id
        if chosen == "topic_partition":
            assert topic_partition is not None
            self._topic_partition = topic_partition
        else:
            assert partition is not None
            self._topic_partition = TopicPartition(
                topic=topic, partition=partition  # type: ignore[arg-type]
            )

    def topic_id(self) -> Uuid:
        return self._topic_id

    def partition(self) -> int:
        return self._topic_partition.partition()

    def topic(self) -> str | None:
        """The topic name, or ``None`` if it is unknown (Java returns null)."""
        return self._topic_partition.topic()

    def topic_partition(self) -> TopicPartition:
        return self._topic_partition

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, TopicIdPartition):
            return NotImplemented
        return (self._topic_id == other._topic_id
                and self._topic_partition == other._topic_partition)

    def __hash__(self) -> int:
        # Java: prime=31; result = prime + topicId.hashCode();
        #       result = prime*result + topicPartition.hashCode().
        return hash((self._topic_id, self._topic_partition))

    def __repr__(self) -> str:
        # Java toString: topicId + ":" + topic() + "-" + partition()
        return f"{self._topic_id}:{self.topic()}-{self.partition()}"
