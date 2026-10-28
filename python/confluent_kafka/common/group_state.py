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

"""``GroupState``: Java's ``org.apache.kafka.common.GroupState``.

Java's constructor sets no integer field, so an ``enum.Enum`` valued by the
constant's name; Java's private ``name`` field is reached through Java's own
methods, ``__str__`` (``toString``) and ``parse`` (CLAUDE.md, Python Binding
Conventions, Class family).
"""

from __future__ import annotations

from enum import Enum

from confluent_kafka.common.group_type import GroupType
from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["GroupState"]


class GroupState(Enum):
    """The group state.

    The correspondence between the group states and types:

    ====================  =======  ========  =====  =======
    State                 Classic  Consumer  Share  Streams
    ====================  =======  ========  =====  =======
    UNKNOWN               Yes      Yes       Yes    Yes
    PREPARING_REBALANCE   Yes      Yes
    COMPLETING_REBALANCE  Yes      Yes
    STABLE                Yes      Yes       Yes    Yes
    DEAD                  Yes      Yes       Yes    Yes
    EMPTY                 Yes      Yes       Yes    Yes
    ASSIGNING                      Yes              Yes
    RECONCILING                    Yes              Yes
    NOT_READY                                       Yes
    ====================  =======  ========  =====  =======

    Java: ``org.apache.kafka.common.GroupState``.
    """

    UNKNOWN = "UNKNOWN"
    PREPARING_REBALANCE = "PREPARING_REBALANCE"
    COMPLETING_REBALANCE = "COMPLETING_REBALANCE"
    STABLE = "STABLE"
    DEAD = "DEAD"
    EMPTY = "EMPTY"
    ASSIGNING = "ASSIGNING"
    RECONCILING = "RECONCILING"
    NOT_READY = "NOT_READY"

    @staticmethod
    def parse(*, name: str) -> GroupState:
        """Case-insensitive group state lookup by string name."""
        if name is None:
            # Java's ``name.toUpperCase(Locale.ROOT)`` dereferences it (Types).
            raise TypeError("name must not be None")
        state = _NAME_TO_ENUM.get(name.upper())
        return GroupState.UNKNOWN if state is None else state

    @staticmethod
    def group_states_for_type(*, type: GroupType) -> set[GroupState]:  # noqa: A002 - Java's name
        """The states a group of ``type`` can be in; ``IllegalArgumentError``
        for ``GroupType.UNKNOWN``."""
        s = GroupState
        if type is GroupType.CLASSIC:
            return {s.PREPARING_REBALANCE, s.COMPLETING_REBALANCE, s.STABLE, s.DEAD, s.EMPTY}
        if type is GroupType.CONSUMER:
            return {s.PREPARING_REBALANCE, s.COMPLETING_REBALANCE, s.STABLE, s.DEAD, s.EMPTY,
                    s.ASSIGNING, s.RECONCILING}
        if type is GroupType.STREAMS:
            return {s.STABLE, s.DEAD, s.EMPTY, s.ASSIGNING, s.RECONCILING, s.NOT_READY}
        if type is GroupType.SHARE:
            return {s.STABLE, s.DEAD, s.EMPTY}
        raise IllegalArgumentError(message="Group type not known")

    def __str__(self) -> str:
        return _NAMES[self]


_NAMES: dict[GroupState, str] = {
    GroupState.UNKNOWN: "Unknown",
    GroupState.PREPARING_REBALANCE: "PreparingRebalance",
    GroupState.COMPLETING_REBALANCE: "CompletingRebalance",
    GroupState.STABLE: "Stable",
    GroupState.DEAD: "Dead",
    GroupState.EMPTY: "Empty",
    GroupState.ASSIGNING: "Assigning",
    GroupState.RECONCILING: "Reconciling",
    GroupState.NOT_READY: "NotReady",
}

_NAME_TO_ENUM: dict[str, GroupState] = {name.upper(): s for s, name in _NAMES.items()}
