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

"""``GroupType``: Java's ``org.apache.kafka.common.GroupType``.

Java's constructor sets no integer field, so an ``enum.Enum`` valued by the
constant's name; Java's private ``name`` field is reached through Java's own
methods, ``__str__`` (``toString``) and ``parse`` (CLAUDE.md, Python Binding
Conventions, Class family).
"""

from __future__ import annotations

from enum import Enum

__all__ = ["GroupType"]


class GroupType(Enum):
    """The group type.

    Java: ``org.apache.kafka.common.GroupType``.
    """

    UNKNOWN = "UNKNOWN"
    CONSUMER = "CONSUMER"
    CLASSIC = "CLASSIC"
    SHARE = "SHARE"
    STREAMS = "STREAMS"

    @staticmethod
    def parse(*, name: str | None) -> GroupType:
        """Parse a string into a consumer group type, in a case-insensitive
        manner; ``UNKNOWN`` for ``None`` or an unknown name."""
        if name is None:
            return GroupType.UNKNOWN
        group_type = _NAME_TO_ENUM.get(name.lower())
        return GroupType.UNKNOWN if group_type is None else group_type

    def __str__(self) -> str:
        return _NAMES[self]


_NAMES: dict[GroupType, str] = {
    GroupType.UNKNOWN: "Unknown",
    GroupType.CONSUMER: "Consumer",
    GroupType.CLASSIC: "Classic",
    GroupType.SHARE: "Share",
    GroupType.STREAMS: "Streams",
}

_NAME_TO_ENUM: dict[str, GroupType] = {name.lower(): t for t, name in _NAMES.items()}
