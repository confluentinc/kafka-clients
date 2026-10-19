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

"""``TopicPartitionReplica``: Java's ``org.apache.kafka.common.TopicPartitionReplica``.

Java's class is ``Serializable``: ``__reduce__`` rebuilds it through its public
constructor by keyword, so ``pickle``, ``copy`` and ``deepcopy`` work; Java's
cached ``hash`` field is not saved (CLAUDE.md, Python Binding Conventions,
Types, JDK interfaces).
"""

from __future__ import annotations

import functools
from typing import Any


__all__ = ["TopicPartitionReplica"]


class TopicPartitionReplica:
    """The topic name, partition number and the brokerId of the replica.

    Java: ``org.apache.kafka.common.TopicPartitionReplica``.
    """

    __slots__ = ("_broker_id", "_partition", "_topic")

    def __init__(self, *, topic: str, partition: int, broker_id: int) -> None:
        if topic is None:
            raise TypeError("topic must not be None")
        self._topic = topic
        self._partition = partition
        self._broker_id = broker_id

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def broker_id(self) -> int:
        return self._broker_id

    def __hash__(self) -> int:
        return hash((self._topic, self._partition, self._broker_id))

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, TopicPartitionReplica)
        return (self._partition == other._partition and self._broker_id == other._broker_id
                and self._topic == other._topic)

    def __str__(self) -> str:
        return f"{self._topic}-{self._partition:d}-{self._broker_id:d}"

    def __reduce__(self) -> tuple[Any, ...]:
        """Rebuild through the public constructor by keyword (``pickle``,
        ``copy``)."""
        return (functools.partial(type(self), topic=self._topic, partition=self._partition,
                                  broker_id=self._broker_id), ())
