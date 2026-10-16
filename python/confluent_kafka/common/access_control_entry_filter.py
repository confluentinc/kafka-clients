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

"""``AccessControlEntryFilter``: Java's
``org.apache.kafka.common.acl.AccessControlEntryFilter``.

In ``confluent_kafka.common``, the nearest module above ``common.acl``. Java's
package-private ``AccessControlEntryFilter(AccessControlEntryData)``, used by
``AccessControlEntry.toFilter()``, is the private class method ``_new``
(CLAUDE.md, Python Binding Conventions, Class family).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from confluent_kafka.common._access_control_entry_data import _AccessControlEntryData
from confluent_kafka.common.acl_operation import AclOperation
from confluent_kafka.common.acl_permission_type import AclPermissionType
from confluent_kafka.null_pointer_error import NullPointerError

if TYPE_CHECKING:
    from confluent_kafka.common.access_control_entry import AccessControlEntry

__all__ = ["AccessControlEntryFilter"]


class AccessControlEntryFilter:
    """Represents a filter which matches access control entries.

    Java: ``org.apache.kafka.common.acl.AccessControlEntryFilter``.
    """

    __slots__ = ("_data",)

    #: Matches any access control entry.
    ANY: ClassVar[AccessControlEntryFilter]

    def __init__(self, *, principal: str | None, host: str | None, operation: AclOperation,
                 permission_type: AclPermissionType) -> None:
        """Create an instance of an access control entry filter with the
        provided parameters: ``principal`` the principal or ``None``, ``host``
        the host or ``None``, ``operation`` a non-null operation,
        ``permission_type`` a non-null permission type."""
        if operation is None:
            raise NullPointerError()
        if permission_type is None:
            raise NullPointerError()
        self._data = _AccessControlEntryData(principal=principal, host=host, operation=operation,
                                             permission_type=permission_type)

    @classmethod
    def _new(cls, *, data: _AccessControlEntryData) -> AccessControlEntryFilter:
        """Java's package-private ``AccessControlEntryFilter(data)``, used in
        ``AccessControlEntry.to_filter()``."""
        entry_filter = cls.__new__(cls)
        entry_filter._data = data
        return entry_filter

    def principal(self) -> str | None:
        """Return the principal or ``None``."""
        return self._data.principal()

    def host(self) -> str | None:
        """Return the host or ``None``. The value ``*`` means any host."""
        return self._data.host()

    def operation(self) -> AclOperation:
        """Return the AclOperation."""
        return self._data.operation()

    def permission_type(self) -> AclPermissionType:
        """Return the AclPermissionType."""
        return self._data.permission_type()

    def __str__(self) -> str:
        return str(self._data)

    def is_unknown(self) -> bool:
        """Return true if there are any UNKNOWN components."""
        return self._data.is_unknown()

    def matches(self, *, other: AccessControlEntry) -> bool:
        """Returns true if this filter matches the given AccessControlEntry."""
        principal = self.principal()
        if principal is not None and principal != other.principal():
            return False
        host = self.host()
        if host is not None and host != other.host():
            return False
        if self.operation() is not AclOperation.ANY and self.operation() != other.operation():
            return False
        return (self.permission_type() is AclPermissionType.ANY
                or self.permission_type() == other.permission_type())

    def matches_at_most_one(self) -> bool:
        """Returns true if this filter could only match one ACE -- in other
        words, if there are no ANY or UNKNOWN fields."""
        return self.find_indefinite_field() is None

    def find_indefinite_field(self) -> str | None:
        """Returns a string describing an ANY or UNKNOWN field, or ``None`` if
        there is no such field."""
        return self._data.find_indefinite_field()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AccessControlEntryFilter):
            return False
        return self._data == other._data

    def __hash__(self) -> int:
        return hash(self._data)


AccessControlEntryFilter.ANY = AccessControlEntryFilter(
    principal=None, host=None, operation=AclOperation.ANY, permission_type=AclPermissionType.ANY)
