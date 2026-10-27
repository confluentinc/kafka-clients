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

"""``AclOperation``: Java's ``org.apache.kafka.common.acl.AclOperation``.

In ``confluent_kafka.common.acl``, the module of its Java package. An
``enum.IntEnum`` valued by Java's ``code`` field, the one integer field its
constructor sets (CLAUDE.md, Python Binding Conventions, Class family).
``__str__`` is Java's inherited ``Enum.toString()``, the constant's name.
"""

from __future__ import annotations

from enum import IntEnum

__all__ = ["AclOperation"]


class AclOperation(IntEnum):
    """Represents an operation which an ACL grants or denies permission to
    perform.

    Some operations imply other operations:

    - ``ALLOW ALL`` implies ``ALLOW`` everything
    - ``DENY ALL`` implies ``DENY`` everything
    - ``ALLOW READ`` implies ``ALLOW DESCRIBE``
    - ``ALLOW WRITE`` implies ``ALLOW DESCRIBE``
    - ``ALLOW DELETE`` implies ``ALLOW DESCRIBE``
    - ``ALLOW ALTER`` implies ``ALLOW DESCRIBE``
    - ``ALLOW ALTER_CONFIGS`` implies ``ALLOW DESCRIBE_CONFIGS``

    Java: ``org.apache.kafka.common.acl.AclOperation``.
    """

    #: Represents any AclOperation which this client cannot understand,
    #: perhaps because this client is too old.
    UNKNOWN = 0
    #: In a filter, matches any AclOperation.
    ANY = 1
    #: ALL operation.
    ALL = 2
    #: READ operation.
    READ = 3
    #: WRITE operation.
    WRITE = 4
    #: CREATE operation.
    CREATE = 5
    #: DELETE operation.
    DELETE = 6
    #: ALTER operation.
    ALTER = 7
    #: DESCRIBE operation.
    DESCRIBE = 8
    #: CLUSTER_ACTION operation.
    CLUSTER_ACTION = 9
    #: DESCRIBE_CONFIGS operation.
    DESCRIBE_CONFIGS = 10
    #: ALTER_CONFIGS operation.
    ALTER_CONFIGS = 11
    #: IDEMPOTENT_WRITE operation.
    IDEMPOTENT_WRITE = 12
    #: CREATE_TOKENS operation.
    CREATE_TOKENS = 13
    #: DESCRIBE_TOKENS operation.
    DESCRIBE_TOKENS = 14
    #: TWO_PHASE_COMMIT operation.
    TWO_PHASE_COMMIT = 15

    # Note: we cannot have more than 30 ACL operations without modifying the
    # format used to describe ACL operations in MetadataResponse.

    @staticmethod
    def from_string(*, str: str) -> AclOperation:  # noqa: A002 - Java's name
        """Parse the given string as an ACL operation: the ``AclOperation``, or
        ``UNKNOWN`` if the string could not be matched."""
        member = AclOperation.__members__.get(str.upper())
        return AclOperation.UNKNOWN if member is None else member

    @staticmethod
    def from_code(*, code: int) -> AclOperation:
        """Return the ``AclOperation`` with the provided code or
        ``AclOperation.UNKNOWN`` if one cannot be found."""
        try:
            return AclOperation(code)
        except ValueError:
            return AclOperation.UNKNOWN

    def code(self) -> int:
        """Return the code of this operation."""
        return int(self)

    def is_unknown(self) -> bool:
        """Return true if this operation is UNKNOWN."""
        return self is AclOperation.UNKNOWN

    def __str__(self) -> str:
        return self.name
