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

Both Java constructors are ``@Deprecated(since = "4.2", forRemoval = true)``, so
none is offered: ``ConsumerGroupMetadata()`` raises ``TypeError``, and instances
come from a consumer's ``group_metadata()``, as Java intends. A
``KafkaConsumer``'s instance also holds the core's metadata handle, which
``KafkaProducer.send_offsets_to_transaction`` hands to the core.
"""

from __future__ import annotations

from typing import Any

__all__ = ["ConsumerGroupMetadata"]


class ConsumerGroupMetadata:
    """A metadata struct containing the consumer group information. Note: Any
    change to this class is considered public and requires a KIP.

    Java: ``org.apache.kafka.clients.consumer.ConsumerGroupMetadata``.
    """

    __slots__ = ("_group_id", "_generation_id", "_member_id", "_group_instance_id",
                 "_native")
    _group_id: str
    _generation_id: int
    _member_id: str
    _group_instance_id: str | None
    _native: object

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "ConsumerGroupMetadata cannot be constructed directly; use the "
            "consumer's group_metadata()")

    @classmethod
    def _of(cls, *, group_id: str, generation_id: int, member_id: str,
            group_instance_id: str | None, native: object = None) -> ConsumerGroupMetadata:
        """A consumer's ``groupMetadata()``; ``native`` is the core's handle when
        the consumer runs in the core."""
        metadata = cls.__new__(cls)
        metadata._group_id = group_id
        metadata._generation_id = generation_id
        metadata._member_id = member_id
        metadata._group_instance_id = group_instance_id
        metadata._native = native
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

    def __deepcopy__(self, memo: dict[int, Any]) -> ConsumerGroupMetadata:
        # Immutable, and the core's handle cannot be copied: a deep copy is the
        # object itself.
        return self
