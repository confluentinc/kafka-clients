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

"""``TopicPartition`` — a topic name and partition number.

Translated from ``org.apache.kafka.common.TopicPartition`` (Apache Kafka
4.3.1). Immutable value type read through accessor methods (D16); value
equality and hashable so it can key a ``dict`` (Java ``equals``/``hashCode``).
"""

from __future__ import annotations


class TopicPartition:
    """A topic name and partition number.

    Java: ``org.apache.kafka.common.TopicPartition``
    (``TopicPartition(String topic, int partition)``).
    """

    __slots__ = ("_topic", "_partition")

    def __init__(self, *, topic: str, partition: int) -> None:
        self._topic = topic
        self._partition = partition

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, TopicPartition):
            return NotImplemented
        return self._partition == other._partition and self._topic == other._topic

    def __hash__(self) -> int:
        # Java: prime=31; result = prime + partition; result = prime*result + hash(topic).
        # We mirror the field set, not the exact int; Python hashing is its own.
        return hash((self._topic, self._partition))

    def __repr__(self) -> str:
        # Java toString: topic + "-" + partition
        return f"{self._topic}-{self._partition}"
