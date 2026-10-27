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

"""``ClassicGroupDescription``: Java's
``org.apache.kafka.clients.admin.ClassicGroupDescription``.

Java's six-argument constructor passes ``Set.of()`` as the authorized
operations to the seven-argument one, so the two are one constructor, one stub
each, whose ``authorized_operations`` left out is ``()`` (CLAUDE.md, Python
Binding Conventions, Signatures); every combination of the keywords is a Java
constructor, so there is no ``java_forms``. An explicit ``None`` is Java's
``null`` ("not known"), stored as given. The client builds a description from
core data through the private ``_new``, the seven-argument constructor.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, overload

from confluent_kafka._args import MISSING

from confluent_kafka._java import java_str

from ._values import frozen

if TYPE_CHECKING:
    from confluent_kafka.common.acl import AclOperation
    from confluent_kafka.common import ClassicGroupState, Node

    from .member_description import MemberDescription

__all__ = ["ClassicGroupDescription"]


class ClassicGroupDescription:
    """A detailed description of a single classic group in the cluster.

    Java: ``org.apache.kafka.clients.admin.ClassicGroupDescription``.
    """

    __slots__ = ("_group_id", "_protocol", "_protocol_data", "_members", "_state",
                 "_coordinator", "_authorized_operations")

    @overload
    def __init__(self, *, group_id: str | None, protocol: str, protocol_data: str | None,
                 members: Iterable[MemberDescription] | None, state: ClassicGroupState,
                 coordinator: Node | None) -> None: ...
    @overload
    def __init__(self, *, group_id: str | None, protocol: str, protocol_data: str | None,
                 members: Iterable[MemberDescription] | None, state: ClassicGroupState,
                 coordinator: Node | None,
                 authorized_operations: Iterable[AclOperation] | None) -> None: ...

    def __init__(self, *, group_id: str | None, protocol: str, protocol_data: str | None,
                 members: Iterable[MemberDescription] | None, state: ClassicGroupState,
                 coordinator: Node | None,
                 authorized_operations: Iterable[AclOperation] | None = MISSING) -> None:
        if authorized_operations is MISSING:
            authorized_operations = ()  # the six-argument form's Set.of()
        self._init(group_id, protocol, protocol_data, members, state, coordinator,
                   authorized_operations)

    @classmethod
    def _new(cls, *, group_id: str | None, protocol: str, protocol_data: str | None,
             members: Iterable[MemberDescription] | None, state: ClassicGroupState,
             coordinator: Node | None,
             authorized_operations: Iterable[AclOperation] | None) -> ClassicGroupDescription:
        """Java's seven-argument constructor with ``authorized_operations``
        stored as given, ``None`` being Java's ``null`` ("not known"), for the
        client's conversion of core data."""
        description: Any = cls.__new__(cls)
        description._init(group_id, protocol, protocol_data, members, state, coordinator,
                          authorized_operations)
        assert isinstance(description, ClassicGroupDescription)
        return description

    def _init(self, group_id: str | None, protocol: str, protocol_data: str | None,
              members: Iterable[MemberDescription] | None, state: ClassicGroupState,
              coordinator: Node | None,
              authorized_operations: Iterable[AclOperation] | None) -> None:
        """The body of Java's seven-argument constructor."""
        self._group_id = "" if group_id is None else group_id
        self._protocol = protocol
        self._protocol_data = "" if protocol_data is None else protocol_data
        self._members: tuple[MemberDescription, ...] = (
            () if members is None else tuple(members))
        self._state = state
        self._coordinator = coordinator
        self._authorized_operations: frozenset[AclOperation] | None = (
            None if authorized_operations is None else frozenset(authorized_operations))

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ClassicGroupDescription)
        return (self._group_id == other._group_id
                and self._protocol == other._protocol
                and self._protocol_data == other._protocol_data
                and self._members == other._members
                and self._state is other._state
                and self._coordinator == other._coordinator
                and self._authorized_operations == other._authorized_operations)

    def __hash__(self) -> int:
        return hash((self._group_id, self._protocol, self._protocol_data, frozen(self._members),
                     self._state, self._coordinator, self._authorized_operations))

    def group_id(self) -> str:
        """The id of the classic group."""
        return self._group_id

    def protocol(self) -> str:
        """The group protocol type."""
        return self._protocol

    def protocol_data(self) -> str:
        """The group protocol data. The meaning depends on the group protocol
        type. For a classic consumer group, this is the partition assignor
        name. For a classic connect group, this indicates which Connect
        protocols are enabled."""
        return self._protocol_data

    def is_simple_consumer_group(self) -> bool:
        """If the group is a simple consumer group or not."""
        return self._protocol == ""

    def members(self) -> list[MemberDescription]:
        """A list of the members of the classic group."""
        return list(self._members)

    def state(self) -> ClassicGroupState:
        """The classic group state, or UNKNOWN if the state is too new for us
        to parse."""
        return self._state

    def coordinator(self) -> Node | None:
        """The classic group coordinator, or ``None`` if the coordinator is not
        known."""
        return self._coordinator

    def authorized_operations(self) -> set[AclOperation] | None:
        """authorizedOperations for this group, or ``None`` if that information
        is not known."""
        ops = self._authorized_operations
        return None if ops is None else set(ops)

    def __str__(self) -> str:
        return ("(groupId=" + self._group_id
                + ", protocol='" + java_str(self._protocol) + "'"
                + ", protocolData=" + self._protocol_data
                + ", members=" + ",".join(java_str(m) for m in self._members)
                + ", state=" + java_str(self._state)
                + ", coordinator=" + java_str(self._coordinator)
                + ", authorizedOperations=" + java_str(self._authorized_operations) + ")")
