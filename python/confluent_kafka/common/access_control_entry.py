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

"""``AccessControlEntry``: Java's ``org.apache.kafka.common.acl.AccessControlEntry``.

In ``confluent_kafka.common``, the nearest module above ``common.acl``. Java's
``equals`` uses ``instanceof``, so a subclass instance with the same data is
equal, as in Java.
"""

from __future__ import annotations

from confluent_kafka.common._access_control_entry_data import _AccessControlEntryData
from confluent_kafka.common.access_control_entry_filter import AccessControlEntryFilter
from confluent_kafka.common.acl_operation import AclOperation
from confluent_kafka.common.acl_permission_type import AclPermissionType
from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["AccessControlEntry"]


class AccessControlEntry:
    """Represents an access control entry. ACEs are a tuple of principal, host,
    operation, and permissionType.

    Java: ``org.apache.kafka.common.acl.AccessControlEntry``.
    """

    __slots__ = ("_data",)

    def __init__(self, *, principal: str, host: str, operation: AclOperation,
                 permission_type: AclPermissionType) -> None:
        """Create an instance of an access control entry with the provided
        parameters: ``principal`` a non-null principal, ``host`` a non-null
        host, ``operation`` a non-null operation (ANY is not an allowed
        operation), ``permission_type`` a non-null permission type (ANY is not
        an allowed type)."""
        if principal is None:
            raise TypeError("principal must not be None")
        if host is None:
            raise TypeError("host must not be None")
        if operation is None:
            raise TypeError("operation must not be None")
        if operation is AclOperation.ANY:
            raise IllegalArgumentError(message="operation must not be ANY")
        if permission_type is None:
            raise TypeError("permission_type must not be None")
        if permission_type is AclPermissionType.ANY:
            raise IllegalArgumentError(message="permissionType must not be ANY")
        self._data = _AccessControlEntryData(principal=principal, host=host, operation=operation,
                                             permission_type=permission_type)

    def principal(self) -> str:
        """Return the principal for this entry."""
        principal = self._data.principal()
        assert principal is not None
        return principal

    def host(self) -> str:
        """Return the host or ``*`` for all hosts."""
        host = self._data.host()
        assert host is not None
        return host

    def operation(self) -> AclOperation:
        """Return the AclOperation. This method will never return
        ``AclOperation.ANY``."""
        return self._data.operation()

    def permission_type(self) -> AclPermissionType:
        """Return the AclPermissionType. This method will never return
        ``AclPermissionType.ANY``."""
        return self._data.permission_type()

    def to_filter(self) -> AccessControlEntryFilter:
        """Create a filter which matches only this AccessControlEntry."""
        return AccessControlEntryFilter._new(data=self._data)

    def __str__(self) -> str:
        return str(self._data)

    def is_unknown(self) -> bool:
        """Return true if this AclResource has any UNKNOWN components."""
        return self._data.is_unknown()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AccessControlEntry):
            return False
        return self._data == other._data

    def __hash__(self) -> int:
        return hash(self._data)
