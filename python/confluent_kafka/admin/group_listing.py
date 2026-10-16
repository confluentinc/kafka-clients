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

"""``GroupListing``: Java's ``org.apache.kafka.clients.admin.GroupListing``.

Java's ``Optional<GroupType>`` / ``Optional<GroupState>`` are ``… | None``
(Types).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from confluent_kafka._java import java_str
from confluent_kafka.common import GroupType

if TYPE_CHECKING:
    from confluent_kafka.common import GroupState

__all__ = ["GroupListing"]


class GroupListing:
    """A listing of a group in the cluster.

    Java: ``org.apache.kafka.clients.admin.GroupListing``.
    """

    __slots__ = ("_group_id", "_type", "_protocol", "_group_state")

    def __init__(self, *, group_id: str, type: GroupType | None,  # noqa: A002 - Java's name
                 protocol: str, group_state: GroupState | None) -> None:
        """Create an instance with the specified parameters: the group id,
        group type, protocol and group state."""
        self._group_id = group_id
        self._type = type
        self._protocol = protocol
        self._group_state = group_state

    def group_id(self) -> str:
        """The group id."""
        return self._group_id

    def type(self) -> GroupType | None:  # noqa: A003 - Java's name
        """The type of the group.

        If the broker returns a group type which is not recognised, as might
        happen when talking to a broker with a later version, the type will be
        ``GroupType.UNKNOWN``. If the broker is earlier than version 2.6.0, the
        group type will not be available, and the type will be ``None``.
        """
        return self._type

    def protocol(self) -> str:
        """The protocol of the group."""
        return self._protocol

    def group_state(self) -> GroupState | None:
        """The group state.

        If the broker returns a group state which is not recognised, as might
        happen when talking to a broker with a later version, the state will be
        ``GroupState.UNKNOWN``.
        """
        return self._group_state

    def is_simple_consumer_group(self) -> bool:
        """If the group is a simple consumer group or not."""
        return self._type is GroupType.CLASSIC and self._protocol == ""

    def __str__(self) -> str:
        return ("(groupId='" + java_str(self._group_id) + "'"
                + ", type=" + ("none" if self._type is None else str(self._type))
                + ", protocol='" + java_str(self._protocol) + "'"
                + ", groupState=" + ("none" if self._group_state is None
                                     else str(self._group_state)) + ")")

    def __hash__(self) -> int:
        return hash((self._group_id, self._type, self._protocol, self._group_state))

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, GroupListing):
            return NotImplemented
        return (self._group_id == other._group_id and self._type == other._type
                and self._protocol == other._protocol
                and self._group_state == other._group_state)
