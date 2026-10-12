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

"""``MemberDescription``: Java's ``org.apache.kafka.clients.admin.MemberDescription``.

Only Java's non-deprecated nine-argument constructor is generated; the four
``@Deprecated`` ones are not (CLAUDE.md, Python Binding Conventions, Class
family). Java's ``Optional`` fields are ``… | None`` (Types); ``__str__`` keeps
Java's ``Optional[…]`` text for the target assignment.
"""

from __future__ import annotations

from confluent_kafka._java import java_str

from ._values import optional_str
from .member_assignment import MemberAssignment


__all__ = ["MemberDescription"]


class MemberDescription:
    """A detailed description of a single group member in the cluster.

    Java: ``org.apache.kafka.clients.admin.MemberDescription``.
    """

    __slots__ = ("_member_id", "_group_instance_id", "_rack_id", "_client_id", "_host",
                 "_assignment", "_target_assignment", "_member_epoch", "_upgraded")

    def __init__(self, *, member_id: str | None, group_instance_id: str | None,
                 rack_id: str | None, client_id: str | None, host: str | None,
                 assignment: MemberAssignment | None,
                 target_assignment: MemberAssignment | None, member_epoch: int | None,
                 upgraded: bool | None) -> None:
        self._member_id = "" if member_id is None else member_id
        self._group_instance_id = group_instance_id
        self._rack_id = rack_id
        self._client_id = "" if client_id is None else client_id
        self._host = "" if host is None else host
        self._assignment = (MemberAssignment(topic_partitions=set())
                            if assignment is None else assignment)
        self._target_assignment = target_assignment
        self._member_epoch = member_epoch
        self._upgraded = upgraded

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, MemberDescription)
        return (self._member_id == other._member_id
                and self._group_instance_id == other._group_instance_id
                and self._rack_id == other._rack_id
                and self._client_id == other._client_id
                and self._host == other._host
                and self._assignment == other._assignment
                and self._target_assignment == other._target_assignment
                and self._member_epoch == other._member_epoch
                and self._upgraded == other._upgraded)

    def __hash__(self) -> int:
        return hash((self._member_id, self._group_instance_id, self._rack_id, self._client_id,
                     self._host, self._assignment, self._target_assignment, self._member_epoch,
                     self._upgraded))

    def consumer_id(self) -> str:
        """The consumer id of the group member."""
        return self._member_id

    def group_instance_id(self) -> str | None:
        """The instance id of the group member."""
        return self._group_instance_id

    def rack_id(self) -> str | None:
        """The rack id of the group member.

        It is only available for consumer groups using the new consumer group
        protocol (``group.protocol=consumer``).
        """
        return self._rack_id

    def client_id(self) -> str:
        """The client id of the group member."""
        return self._client_id

    def host(self) -> str:
        """The host where the group member is running."""
        return self._host

    def assignment(self) -> MemberAssignment:
        """The assignment of the group member. Provided for both classic group
        and consumer group."""
        return self._assignment

    def target_assignment(self) -> MemberAssignment | None:
        """The target assignment of the member. Provided only for consumer
        group."""
        return self._target_assignment

    def member_epoch(self) -> int | None:
        """The epoch of the group member. Set to an integer if the member is in
        a ``GroupType.CONSUMER`` group, and to ``None`` if it is in a
        ``GroupType.CLASSIC`` group."""
        return self._member_epoch

    def upgraded(self) -> bool | None:
        """The flag indicating whether a member within a ``GroupType.CONSUMER``
        group uses the ``GroupType.CONSUMER`` protocol. Set to ``True`` if it
        does, to ``False`` if it does not, and to ``None`` if it is unknown or
        if the group is a ``GroupType.CLASSIC`` group."""
        return self._upgraded

    def __str__(self) -> str:
        return ("(memberId=" + self._member_id
                + ", groupInstanceId=" + java_str(self._group_instance_id)
                + ", rackId=" + java_str(self._rack_id)
                + ", clientId=" + self._client_id
                + ", host=" + self._host
                + ", assignment=" + java_str(self._assignment)
                + ", targetAssignment=" + optional_str(self._target_assignment)
                + ", memberEpoch=" + java_str(self._member_epoch)
                + ", upgraded=" + java_str(self._upgraded) + ")")
