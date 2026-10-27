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

"""``ResourceType``: Java's ``org.apache.kafka.common.resource.ResourceType``.

In ``confluent_kafka.common.resource``, the module of its Java package. An
``enum.IntEnum`` valued by Java's ``code`` field (CLAUDE.md, Python Binding
Conventions, Class family); ``__str__`` is Java's ``Enum.toString()``, the
constant's name.
"""

from __future__ import annotations

from enum import IntEnum

__all__ = ["ResourceType"]


class ResourceType(IntEnum):
    """Represents a type of resource which an ACL can be applied to.

    Java: ``org.apache.kafka.common.resource.ResourceType``.
    """

    #: Represents any ResourceType which this client cannot understand,
    #: perhaps because this client is too old.
    UNKNOWN = 0
    #: In a filter, matches any ResourceType.
    ANY = 1
    #: A Kafka topic.
    TOPIC = 2
    #: A consumer group.
    GROUP = 3
    #: The cluster as a whole.
    CLUSTER = 4
    #: A transactional ID.
    TRANSACTIONAL_ID = 5
    #: A token ID.
    DELEGATION_TOKEN = 6
    #: A user principal
    USER = 7

    @staticmethod
    def from_string(*, str: str) -> ResourceType:  # noqa: A002 - Java's name
        """Parse the given string as an ACL resource type: the
        ``ResourceType``, or ``UNKNOWN`` if the string could not be matched."""
        if str is None:
            # Java's ``str.toUpperCase(Locale.ROOT)`` dereferences it (Types).
            raise TypeError("str must not be None")
        member = ResourceType.__members__.get(str.upper())
        return ResourceType.UNKNOWN if member is None else member

    @staticmethod
    def from_code(*, code: int) -> ResourceType:
        """Return the ``ResourceType`` with the provided code or
        ``ResourceType.UNKNOWN`` if one cannot be found."""
        try:
            return ResourceType(code)
        except ValueError:
            return ResourceType.UNKNOWN

    def code(self) -> int:
        """Return the code of this resource."""
        return int(self)

    def is_unknown(self) -> bool:
        """Return whether this resource type is UNKNOWN."""
        return self is ResourceType.UNKNOWN

    def __str__(self) -> str:
        return self.name
