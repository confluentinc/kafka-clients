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

"""``_AccessControlEntryData``: Java's package-private
``org.apache.kafka.common.acl.AccessControlEntryData``, translated as a private
helper of ``AccessControlEntry`` and ``AccessControlEntryFilter``."""

from __future__ import annotations

from confluent_kafka._java import java_str
from confluent_kafka.common.acl_operation import AclOperation
from confluent_kafka.common.acl_permission_type import AclPermissionType

__all__: list[str] = []


class _AccessControlEntryData:
    """An internal, private class which contains the data stored in
    ``AccessControlEntry`` and ``AccessControlEntryFilter`` objects."""

    __slots__ = ("_principal", "_host", "_operation", "_permission_type")

    def __init__(self, *, principal: str | None, host: str | None, operation: AclOperation,
                 permission_type: AclPermissionType) -> None:
        self._principal = principal
        self._host = host
        self._operation = operation
        self._permission_type = permission_type

    def principal(self) -> str | None:
        return self._principal

    def host(self) -> str | None:
        return self._host

    def operation(self) -> AclOperation:
        return self._operation

    def permission_type(self) -> AclPermissionType:
        return self._permission_type

    def find_indefinite_field(self) -> str | None:
        """Returns a string describing an ANY or UNKNOWN field, or ``None`` if
        there is no such field."""
        if self._principal is None:
            return "Principal is NULL"
        if self._host is None:
            return "Host is NULL"
        if self._operation is AclOperation.ANY:
            return "Operation is ANY"
        if self._operation is AclOperation.UNKNOWN:
            return "Operation is UNKNOWN"
        if self._permission_type is AclPermissionType.ANY:
            return "Permission type is ANY"
        if self._permission_type is AclPermissionType.UNKNOWN:
            return "Permission type is UNKNOWN"
        return None

    def __str__(self) -> str:
        return ("(principal=" + ("<any>" if self._principal is None else self._principal)
                + ", host=" + ("<any>" if self._host is None else self._host)
                + ", operation=" + java_str(self._operation)
                + ", permissionType=" + java_str(self._permission_type) + ")")

    def is_unknown(self) -> bool:
        """Return true if there are any UNKNOWN components."""
        return self._operation.is_unknown() or self._permission_type.is_unknown()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _AccessControlEntryData):
            return False
        return (self._principal == other._principal and self._host == other._host
                and self._operation == other._operation
                and self._permission_type == other._permission_type)

    def __hash__(self) -> int:
        return hash((self._principal, self._host, self._operation, self._permission_type))
