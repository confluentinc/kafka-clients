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

"""``ConsumerGroupMetadata``: Java's ``org.apache.kafka.clients.consumer.ConsumerGroupMetadata``.

Both Java constructors are ``@Deprecated(since = "4.2", forRemoval = true)``: a
call warns (CLAUDE.md, Python Binding Conventions, Class family). They are one
keyword-only constructor: ``(groupId)`` passes
``JoinGroupRequest.UNKNOWN_GENERATION_ID`` (-1),
``JoinGroupRequest.UNKNOWN_MEMBER_ID`` (``""``) and ``Optional.empty()``. The
client builds its instances without the constructor, so
``group_metadata()`` does not warn.
"""

from __future__ import annotations

import warnings

from confluent_kafka.null_pointer_error import NullPointerError

__all__ = ["ConsumerGroupMetadata"]

_DEPRECATED = ("Since 4.2, please use ``KafkaConsumer.group_metadata()`` instead. "
               "This class will be an interface in Kafka 5.0.")


class ConsumerGroupMetadata:
    """A metadata struct containing the consumer group information. Note: Any
    change to this class is considered public and requires a KIP.

    Java: ``org.apache.kafka.clients.consumer.ConsumerGroupMetadata``.
    """

    __slots__ = ("_group_id", "_generation_id", "_member_id", "_group_instance_id")

    def __init__(self, *, group_id: str, generation_id: int = -1, member_id: str = "",
                 group_instance_id: str | None = None) -> None:
        """Deprecated: since 4.2, please use ``KafkaConsumer.group_metadata()``
        instead. This class will be an interface in Kafka 5.0."""
        given = "group_id" if (generation_id == -1 and member_id == ""
                               and group_instance_id is None) else (
            "group_id, generation_id, member_id, group_instance_id")
        warnings.warn(f"ConsumerGroupMetadata({given}) is deprecated. {_DEPRECATED}",
                      DeprecationWarning, stacklevel=2)
        self._init(group_id, generation_id, member_id, group_instance_id)

    def _init(self, group_id: str, generation_id: int, member_id: str,
              group_instance_id: str | None) -> None:
        if group_id is None:
            raise NullPointerError(message="group.id can't be null")
        if member_id is None:
            raise NullPointerError(message="member.id can't be null")
        self._group_id = group_id
        self._generation_id = generation_id
        self._member_id = member_id
        self._group_instance_id = group_instance_id

    @classmethod
    def _of(cls, *, group_id: str, generation_id: int, member_id: str,
            group_instance_id: str | None) -> ConsumerGroupMetadata:
        """The client's instance (``groupMetadata()``), built without the
        deprecated constructor."""
        metadata = cls.__new__(cls)
        metadata._init(group_id, generation_id, member_id, group_instance_id)
        return metadata

    def group_id(self) -> str:
        return self._group_id

    def generation_id(self) -> int:
        return self._generation_id

    def member_id(self) -> str:
        return self._member_id

    def group_instance_id(self) -> str | None:
        return self._group_instance_id

    def __str__(self) -> str:
        instance = "" if self._group_instance_id is None else self._group_instance_id
        return (f"GroupMetadata(groupId = {self._group_id}, generationId = "
                f"{self._generation_id}, memberId = {self._member_id}, "
                f"groupInstanceId = {instance})")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ConsumerGroupMetadata)
        return (self._generation_id == other._generation_id
                and self._group_id == other._group_id
                and self._member_id == other._member_id
                and self._group_instance_id == other._group_instance_id)

    def __hash__(self) -> int:
        return hash((self._group_id, self._generation_id, self._member_id,
                     self._group_instance_id))
