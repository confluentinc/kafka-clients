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

"""``TopicListing``: Java's ``org.apache.kafka.clients.admin.TopicListing``.

Java declares no ``equals`` / ``hashCode``, so equality is identity.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from confluent_kafka._java import java_str

if TYPE_CHECKING:
    from confluent_kafka.common import Uuid

__all__ = ["TopicListing"]


class TopicListing:
    """A listing of a topic in the cluster.

    Java: ``org.apache.kafka.clients.admin.TopicListing``.
    """

    __slots__ = ("_name", "_topic_id", "_internal")

    def __init__(self, *, name: str, topic_id: Uuid, internal: bool) -> None:
        """Create an instance with the specified parameters: the topic name,
        the topic id, and whether the topic is internal to Kafka."""
        self._topic_id = topic_id
        self._name = name
        self._internal = internal

    def topic_id(self) -> Uuid:
        """The id of the topic."""
        return self._topic_id

    def name(self) -> str:
        """The name of the topic."""
        return self._name

    def is_internal(self) -> bool:
        """Whether the topic is internal to Kafka. An example of an internal
        topic is the offsets and group management topic: __consumer_offsets."""
        return self._internal

    def __str__(self) -> str:
        return ("(name=" + java_str(self._name) + ", topicId=" + java_str(self._topic_id)
                + ", internal=" + java_str(self._internal) + ")")
