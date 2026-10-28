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

"""``TopicDescription``: Java's ``org.apache.kafka.clients.admin.TopicDescription``.

Java's three constructors ``(name, internal, partitions)``, ``(…,
authorizedOperations)`` and ``(…, authorizedOperations, topicId)`` are one
keyword-only constructor matched by ``java_forms`` (CLAUDE.md, Python Binding
Conventions, Signatures): the shorter ones pass ``Collections.emptySet()`` and
``Uuid.ZERO_UUID``. ``authorized_operations`` is nullable and dropping it from
the longest leaves no Java constructor, so an explicit ``None`` is given (Java's
``null``, "not known"). Java's ``equals`` / ``hashCode`` leave out the topic id;
so do these.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, overload

from confluent_kafka._args import MISSING, Form, java_forms
from confluent_kafka._java import java_str
from confluent_kafka.common import Uuid

if TYPE_CHECKING:
    from confluent_kafka.common.acl import AclOperation
    from confluent_kafka.common import TopicPartitionInfo

__all__ = ["TopicDescription"]


class TopicDescription:
    """A detailed description of a single topic in the cluster.

    Java: ``org.apache.kafka.clients.admin.TopicDescription``.
    """

    __slots__ = ("_name", "_internal", "_partitions", "_authorized_operations", "_topic_id")

    @overload
    def __init__(self, *, name: str, internal: bool,
                 partitions: Sequence[TopicPartitionInfo]) -> None: ...
    @overload
    def __init__(self, *, name: str, internal: bool, partitions: Sequence[TopicPartitionInfo],
                 authorized_operations: Iterable[AclOperation] | None) -> None: ...
    @overload
    def __init__(self, *, name: str, internal: bool, partitions: Sequence[TopicPartitionInfo],
                 authorized_operations: Iterable[AclOperation] | None,
                 topic_id: Uuid) -> None: ...

    @java_forms(Form("name", "internal", "partitions"),
                Form("name", "internal", "partitions", "authorized_operations"),
                Form("name", "internal", "partitions", "authorized_operations", "topic_id",
                     defaults={"authorized_operations": (), "topic_id": Uuid.ZERO_UUID}))
    def __init__(self, *, name: str, internal: bool, partitions: Sequence[TopicPartitionInfo],
                 authorized_operations: Iterable[AclOperation] | None = MISSING,
                 topic_id: Uuid = MISSING) -> None:
        """Create an instance with the specified parameters.

        ``name``: the topic name; ``internal``: whether the topic is internal to
        Kafka; ``partitions``: a list of partitions where the index represents
        the partition id and the element contains leadership and replica
        information for that partition; ``authorized_operations``: authorized
        operations for this topic, or empty set if this is not known;
        ``topic_id``: the topic id.
        """
        self._name = name
        self._internal = internal
        # Java stores the reference, null included: partitions() returns it,
        # equals / hashCode take it, toString dereferences it.
        self._partitions: tuple[TopicPartitionInfo, ...] | None = (
            None if partitions is None else tuple(partitions))
        self._authorized_operations: frozenset[AclOperation] | None = (
            None if authorized_operations is None else frozenset(authorized_operations))
        self._topic_id = topic_id

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, TopicDescription)
        return (self._internal == other._internal
                and self._name == other._name
                and self._partitions == other._partitions
                and self._authorized_operations == other._authorized_operations)

    def __hash__(self) -> int:
        return hash((self._name, self._internal, self._partitions, self._authorized_operations))

    def name(self) -> str:
        """The name of the topic."""
        return self._name

    def is_internal(self) -> bool:
        """Whether the topic is internal to Kafka. An example of an internal
        topic is the offsets and group management topic: __consumer_offsets."""
        return self._internal

    def topic_id(self) -> Uuid:
        return self._topic_id

    def partitions(self) -> list[TopicPartitionInfo]:
        """A list of partitions where the index represents the partition id and
        the element contains leadership and replica information for that
        partition (``None`` when the constructor was given ``None``, as Java
        returns its ``null``)."""
        if self._partitions is None:
            return None  # type: ignore[return-value]  # Java's stored null
        return list(self._partitions)

    def authorized_operations(self) -> set[AclOperation] | None:
        """authorized operations for this topic, or ``None`` if this is not
        known."""
        ops = self._authorized_operations
        return None if ops is None else set(ops)

    def _partitions_or_raise(self) -> tuple[TopicPartitionInfo, ...]:
        """Java's ``partitions.stream()`` in ``toString``: a ``None`` raises."""
        if self._partitions is None:
            raise TypeError("partitions must not be None")
        return self._partitions

    def __str__(self) -> str:
        return ("(name=" + java_str(self._name) + ", internal=" + java_str(self._internal)
                + ", partitions=" + ",".join(java_str(p) for p in self._partitions_or_raise())
                + ", authorizedOperations=" + java_str(self._authorized_operations) + ")")
