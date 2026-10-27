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

"""``MemberToRemove``: Java's ``org.apache.kafka.clients.admin.MemberToRemove``.

Java's package-private ``toMemberIdentity()`` is not generated; Java declares
no ``toString``.
"""

from __future__ import annotations

__all__ = ["MemberToRemove"]


class MemberToRemove:
    """A struct containing information about the member to be removed.

    Java: ``org.apache.kafka.clients.admin.MemberToRemove``.
    """

    __slots__ = ("_group_instance_id",)

    def __init__(self, *, group_instance_id: str) -> None:
        self._group_instance_id = group_instance_id

    def group_instance_id(self) -> str:
        return self._group_instance_id

    def __eq__(self, other: object) -> bool:
        if isinstance(other, MemberToRemove):
            if self._group_instance_id is None:
                # Java's this.groupInstanceId.equals(...) dereferences it (Types);
                # its hashCode is null-safe.
                raise TypeError("group_instance_id must not be None")
            return self._group_instance_id == other._group_instance_id
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self._group_instance_id,))
