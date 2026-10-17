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

"""``AclPermissionType``: Java's ``org.apache.kafka.common.acl.AclPermissionType``.

In ``confluent_kafka.common``, the nearest module above ``common.acl``. An
``enum.IntEnum`` valued by Java's ``code`` field (CLAUDE.md, Python Binding
Conventions, Class family); ``__str__`` is Java's ``Enum.toString()``, the
constant's name.
"""

from __future__ import annotations

from enum import IntEnum

__all__ = ["AclPermissionType"]


class AclPermissionType(IntEnum):
    """Represents whether an ACL grants or denies permissions.

    Java: ``org.apache.kafka.common.acl.AclPermissionType``.
    """

    #: Represents any AclPermissionType which this client cannot understand,
    #: perhaps because this client is too old.
    UNKNOWN = 0
    #: In a filter, matches any AclPermissionType.
    ANY = 1
    #: Disallows access.
    DENY = 2
    #: Grants access.
    ALLOW = 3

    @staticmethod
    def from_string(*, str: str) -> AclPermissionType:  # noqa: A002 - Java's name
        """Parse the given string as an ACL permission: the
        ``AclPermissionType``, or ``UNKNOWN`` if the string could not be
        matched."""
        member = AclPermissionType.__members__.get(str.upper())
        return AclPermissionType.UNKNOWN if member is None else member

    @staticmethod
    def from_code(*, code: int) -> AclPermissionType:
        """Return the ``AclPermissionType`` with the provided code or
        ``AclPermissionType.UNKNOWN`` if one cannot be found."""
        try:
            return AclPermissionType(code)
        except ValueError:
            return AclPermissionType.UNKNOWN

    def code(self) -> int:
        """Return the code of this permission type."""
        return int(self)

    def is_unknown(self) -> bool:
        """Return true if this permission type is UNKNOWN."""
        return self is AclPermissionType.UNKNOWN

    def __str__(self) -> str:
        return self.name
