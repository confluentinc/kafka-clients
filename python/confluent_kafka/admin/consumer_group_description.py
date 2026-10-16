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

"""``ConsumerGroupDescription``: Java's
``org.apache.kafka.clients.admin.ConsumerGroupDescription``.

Only Java's non-deprecated ten-argument constructor is generated: the three
constructors taking the ``@Deprecated`` ``ConsumerGroupState``, and the
``@Deprecated`` ``state()`` getter, are not (CLAUDE.md, Python Binding
Conventions, Class family). Java's ``Optional<Integer>`` epochs are
``int | None`` (Types).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from confluent_kafka._java import java_str

from ._values import frozen

if TYPE_CHECKING:
    from confluent_kafka.common import AclOperation, GroupState, GroupType, Node

    from .member_description import MemberDescription

__all__ = ["ConsumerGroupDescription"]


class ConsumerGroupDescription:
    """A detailed description of a single consumer group in the cluster.

    Java: ``org.apache.kafka.clients.admin.ConsumerGroupDescription``.
    """

    __slots__ = ("_group_id", "_is_simple_consumer_group", "_members", "_partition_assignor",
                 "_type", "_group_state", "_coordinator", "_authorized_operations",
                 "_group_epoch", "_target_assignment_epoch")

    def __init__(self, *, group_id: str | None, is_simple_consumer_group: bool,
                 members: Iterable[MemberDescription] | None, partition_assignor: str | None,
                 type: GroupType,  # noqa: A002 - Java's name
                 group_state: GroupState, coordinator: Node | None,
                 authorized_operations: Iterable[AclOperation] | None,
                 group_epoch: int | None, target_assignment_epoch: int | None) -> None:
        self._group_id = "" if group_id is None else group_id
        self._is_simple_consumer_group = is_simple_consumer_group
        self._members: tuple[MemberDescription, ...] = (
            () if members is None else tuple(members))
        self._partition_assignor = "" if partition_assignor is None else partition_assignor
        self._type = type
        self._group_state = group_state
        self._coordinator = coordinator
        self._authorized_operations: frozenset[AclOperation] | None = (
            None if authorized_operations is None else frozenset(authorized_operations))
        self._group_epoch = group_epoch
        self._target_assignment_epoch = target_assignment_epoch

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ConsumerGroupDescription)
        return (self._is_simple_consumer_group == other._is_simple_consumer_group
                and self._group_id == other._group_id
                and self._members == other._members
                and self._partition_assignor == other._partition_assignor
                and self._type is other._type
                and self._group_state is other._group_state
                and self._coordinator == other._coordinator
                and self._authorized_operations == other._authorized_operations
                and self._group_epoch == other._group_epoch
                and self._target_assignment_epoch == other._target_assignment_epoch)

    def __hash__(self) -> int:
        return hash((self._group_id, self._is_simple_consumer_group, frozen(self._members),
                     self._partition_assignor, self._type, self._group_state, self._coordinator,
                     self._authorized_operations, self._group_epoch,
                     self._target_assignment_epoch))

    def group_id(self) -> str:
        """The id of the consumer group."""
        return self._group_id

    def is_simple_consumer_group(self) -> bool:
        """If consumer group is simple or not."""
        return self._is_simple_consumer_group

    def members(self) -> list[MemberDescription]:
        """A list of the members of the consumer group."""
        return list(self._members)

    def partition_assignor(self) -> str:
        """The consumer group partition assignor."""
        return self._partition_assignor

    def type(self) -> GroupType:  # noqa: A003 - Java's name
        """The group type (or the protocol) of this consumer group. It defaults
        to Classic if not provided by the server."""
        return self._type

    def group_state(self) -> GroupState:
        """The group state, or UNKNOWN if the state is too new for us to
        parse."""
        return self._group_state

    def coordinator(self) -> Node | None:
        """The consumer group coordinator, or ``None`` if the coordinator is
        not known."""
        return self._coordinator

    def authorized_operations(self) -> set[AclOperation] | None:
        """authorizedOperations for this group, or ``None`` if that information
        is not known."""
        ops = self._authorized_operations
        return None if ops is None else set(ops)

    def group_epoch(self) -> int | None:
        """The epoch of the consumer group. Set to an integer if it is a
        ``GroupType.CONSUMER`` group, and to ``None`` if it is a
        ``GroupType.CLASSIC`` group."""
        return self._group_epoch

    def target_assignment_epoch(self) -> int | None:
        """The epoch of the target assignment. Set to an integer if it is a
        ``GroupType.CONSUMER`` group, and to ``None`` if it is a
        ``GroupType.CLASSIC`` group."""
        return self._target_assignment_epoch

    def __str__(self) -> str:
        return ("(groupId=" + self._group_id
                + ", isSimpleConsumerGroup=" + java_str(self._is_simple_consumer_group)
                + ", members=" + ",".join(java_str(m) for m in self._members)
                + ", partitionAssignor=" + self._partition_assignor
                + ", type=" + java_str(self._type)
                + ", groupState=" + java_str(self._group_state)
                + ", coordinator=" + java_str(self._coordinator)
                + ", authorizedOperations=" + java_str(self._authorized_operations)
                + ", groupEpoch=" + java_str(self._group_epoch)
                + ", targetAssignmentEpoch=" + java_str(self._target_assignment_epoch) + ")")
