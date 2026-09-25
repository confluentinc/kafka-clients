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

"""``ConsumerGroupMetadata`` — the consumer group information.

Translated from ``org.apache.kafka.clients.consumer.ConsumerGroupMetadata``
(Apache Kafka 4.3.1). Java's two constructors are both
``@Deprecated(since="4.2", forRemoval=true)`` — obtain instances from
``group_metadata()`` — but are kept per rule 3.13. They collapse into one
keyword-only constructor whose defaults (``generation_id=-1``, ``member_id=""``,
``group_instance_id=None``) are the one-arg overload's delegation values
(``JoinGroupRequest.UNKNOWN_GENERATION_ID`` / ``UNKNOWN_MEMBER_ID`` /
``Optional.empty()``).
"""

from __future__ import annotations


class ConsumerGroupMetadata:
    """The consumer group information.

    Java: ``org.apache.kafka.clients.consumer.ConsumerGroupMetadata``.

    .. deprecated::
        Both Java constructors are ``@Deprecated(since="4.2",
        forRemoval=true)``; obtain instances from ``group_metadata()`` instead.
        Kept per rule 3.13.
    """

    __slots__ = ("_group_id", "_generation_id", "_member_id",
                 "_group_instance_id")

    def __init__(self, *, group_id: str, generation_id: int = -1,
                 member_id: str = "",
                 group_instance_id: str | None = None) -> None:
        # Java: Objects.requireNonNull(groupId, "group.id can't be null") and
        # requireNonNull(memberId, "member.id can't be null"). group_instance_id
        # models Java's Optional<String>, whose empty value is None (valid).
        if group_id is None:
            raise TypeError("group.id can't be null")
        if member_id is None:
            raise TypeError("member.id can't be null")
        self._group_id = group_id
        self._generation_id = generation_id
        self._member_id = member_id
        self._group_instance_id = group_instance_id

    def group_id(self) -> str:
        return self._group_id

    def generation_id(self) -> int:
        return self._generation_id

    def member_id(self) -> str:
        return self._member_id

    def group_instance_id(self) -> str | None:
        return self._group_instance_id

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, ConsumerGroupMetadata):
            return NotImplemented
        return (self._generation_id == other._generation_id
                and self._group_id == other._group_id
                and self._member_id == other._member_id
                and self._group_instance_id == other._group_instance_id)

    def __hash__(self) -> int:
        return hash((self._group_id, self._generation_id, self._member_id,
                     self._group_instance_id))

    def __repr__(self) -> str:
        # Java toString: groupInstanceId.orElse("") — None renders as "".
        gi = self._group_instance_id if self._group_instance_id is not None else ""
        return (f"GroupMetadata(groupId = {self._group_id}, "
                f"generationId = {self._generation_id}, "
                f"memberId = {self._member_id}, groupInstanceId = {gi})")
