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

"""``PatternType``: Java's ``org.apache.kafka.common.resource.PatternType``.

In ``confluent_kafka.common``, the nearest module above ``common.resource``. An
``enum.IntEnum`` valued by Java's ``code`` field (CLAUDE.md, Python Binding
Conventions, Class family); ``__str__`` is Java's ``Enum.toString()``, the
constant's name.
"""

from __future__ import annotations

from enum import IntEnum

__all__ = ["PatternType"]


class PatternType(IntEnum):
    """Resource pattern type.

    Java: ``org.apache.kafka.common.resource.PatternType``.
    """

    #: Represents any PatternType which this client cannot understand, perhaps
    #: because this client is too old.
    UNKNOWN = 0
    #: In a filter, matches any resource pattern type.
    ANY = 1
    #: In a filter, will perform pattern matching.
    #:
    #: e.g. Given a filter of ``ResourcePatternFilter(TOPIC, "payments.received",
    #: MATCH)``, the filter match any ``ResourcePattern`` that matches topic
    #: 'payments.received'. This might include: a Literal pattern with the same
    #: type and name, e.g. ``ResourcePattern(TOPIC, "payments.received",
    #: LITERAL)``; a Wildcard pattern with the same type, e.g.
    #: ``ResourcePattern(TOPIC, "*", LITERAL)``; a Prefixed pattern with the
    #: same type and where the name is a matching prefix, e.g.
    #: ``ResourcePattern(TOPIC, "payments.", PREFIXED)``.
    MATCH = 2
    #: A literal resource name.
    #:
    #: A literal name defines the full name of a resource, e.g. topic with name
    #: 'foo', or group with name 'bob'. The special wildcard character ``*`` can
    #: be used to represent a resource with any name.
    LITERAL = 3
    #: A prefixed resource name.
    #:
    #: A prefixed name defines a prefix for a resource, e.g. topics with names
    #: that start with 'foo'.
    PREFIXED = 4

    def code(self) -> int:
        """The code of this resource."""
        return int(self)

    def is_unknown(self) -> bool:
        """Whether this resource pattern type is UNKNOWN."""
        return self is PatternType.UNKNOWN

    def is_specific(self) -> bool:
        """Whether this resource pattern type is a concrete type, rather than
        UNKNOWN or one of the filter types."""
        return self not in (PatternType.UNKNOWN, PatternType.ANY, PatternType.MATCH)

    @staticmethod
    def from_code(*, code: int) -> PatternType:
        """Return the ``PatternType`` with the provided code or ``UNKNOWN`` if
        one cannot be found."""
        try:
            return PatternType(code)
        except ValueError:
            return PatternType.UNKNOWN

    @staticmethod
    def from_string(*, name: str) -> PatternType:
        """Return the ``PatternType`` with the provided name or ``UNKNOWN`` if
        one cannot be found. The name is matched exactly (case-sensitive)."""
        member = PatternType.__members__.get(name)
        return PatternType.UNKNOWN if member is None else member

    def __str__(self) -> str:
        return self.name
