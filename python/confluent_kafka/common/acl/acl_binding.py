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

"""``AclBinding``: Java's ``org.apache.kafka.common.acl.AclBinding``.

In ``confluent_kafka.common.acl``, the module of its Java package.
"""

from __future__ import annotations

from confluent_kafka._java import java_str
from confluent_kafka.common.acl.access_control_entry import AccessControlEntry
from confluent_kafka.common.acl.acl_binding_filter import AclBindingFilter
from confluent_kafka.common.resource.resource_pattern import ResourcePattern

__all__ = ["AclBinding"]


class AclBinding:
    """Represents a binding between a resource pattern and an access control
    entry.

    Java: ``org.apache.kafka.common.acl.AclBinding``.
    """

    __slots__ = ("_pattern", "_entry")

    def __init__(self, *, pattern: ResourcePattern, entry: AccessControlEntry) -> None:
        """Create an instance of this class with the provided parameters:
        ``pattern`` a non-null resource pattern, ``entry`` a non-null entry."""
        if pattern is None:
            raise TypeError("pattern must not be None")
        self._pattern = pattern
        if entry is None:
            raise TypeError("entry must not be None")
        self._entry = entry

    def is_unknown(self) -> bool:
        """``True`` if this binding has any UNKNOWN components."""
        return self._pattern.is_unknown() or self._entry.is_unknown()

    def pattern(self) -> ResourcePattern:
        """The resource pattern for this binding."""
        return self._pattern

    def entry(self) -> AccessControlEntry:
        """The access control entry for this binding."""
        return self._entry

    def to_filter(self) -> AclBindingFilter:
        """Create a filter which matches only this AclBinding."""
        return AclBindingFilter(pattern_filter=self._pattern.to_filter(),
                                entry_filter=self._entry.to_filter())

    def __str__(self) -> str:
        return "(pattern=" + java_str(self._pattern) + ", entry=" + java_str(self._entry) + ")"

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, AclBinding)
        return self._pattern == other._pattern and self._entry == other._entry

    def __hash__(self) -> int:
        return hash((self._pattern, self._entry))
