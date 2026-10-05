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

"""``TopicPartition``: Java's ``org.apache.kafka.common.TopicPartition``."""

from __future__ import annotations

from confluent_kafka._java import java_str

__all__ = ["TopicPartition"]


class TopicPartition:
    """A topic name and partition number.

    Java: ``org.apache.kafka.common.TopicPartition``. Immutable, with value
    equality, so it can key a ``dict``.
    """

    __slots__ = ("_partition", "_topic")

    def __init__(self, *, topic: str, partition: int) -> None:
        self._partition = partition
        self._topic = topic

    def partition(self) -> int:
        return self._partition

    def topic(self) -> str:
        return self._topic

    def __hash__(self) -> int:
        return hash((self._partition, self._topic))

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, TopicPartition)
        return self._partition == other._partition and self._topic == other._topic

    def __str__(self) -> str:
        return java_str(self._topic) + "-" + str(self._partition)
