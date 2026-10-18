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

"""``ClassicGroupState``: Java's ``org.apache.kafka.common.ClassicGroupState``.

Java's constructor sets no integer field, so an ``enum.Enum`` valued by the
constant's name; Java's private ``name`` field is reached through Java's own
methods, ``__str__`` (``toString``) and ``parse`` (CLAUDE.md, Python Binding
Conventions, Class family).
"""

from __future__ import annotations

from enum import Enum

__all__ = ["ClassicGroupState"]


class ClassicGroupState(Enum):
    """The classic group state.

    Java: ``org.apache.kafka.common.ClassicGroupState``.
    """

    UNKNOWN = "UNKNOWN"
    PREPARING_REBALANCE = "PREPARING_REBALANCE"
    COMPLETING_REBALANCE = "COMPLETING_REBALANCE"
    STABLE = "STABLE"
    DEAD = "DEAD"
    EMPTY = "EMPTY"

    @staticmethod
    def parse(*, name: str) -> ClassicGroupState:
        """Case-insensitive classic group state lookup by string name."""
        state = _NAME_TO_ENUM.get(name.upper())
        return ClassicGroupState.UNKNOWN if state is None else state

    def __str__(self) -> str:
        return _NAMES[self]


_NAMES: dict[ClassicGroupState, str] = {
    ClassicGroupState.UNKNOWN: "Unknown",
    ClassicGroupState.PREPARING_REBALANCE: "PreparingRebalance",
    ClassicGroupState.COMPLETING_REBALANCE: "CompletingRebalance",
    ClassicGroupState.STABLE: "Stable",
    ClassicGroupState.DEAD: "Dead",
    ClassicGroupState.EMPTY: "Empty",
}

_NAME_TO_ENUM: dict[str, ClassicGroupState] = {
    name.upper(): state for state, name in _NAMES.items()}
