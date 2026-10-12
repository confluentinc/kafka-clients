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

"""``AclBindingFilter``: Java's ``org.apache.kafka.common.acl.AclBindingFilter``.

In ``confluent_kafka.common``, the nearest module above ``common.acl``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from confluent_kafka._java import java_str
from confluent_kafka.common.access_control_entry_filter import AccessControlEntryFilter
from confluent_kafka.common.resource_pattern_filter import ResourcePatternFilter
from confluent_kafka.null_pointer_error import NullPointerError

if TYPE_CHECKING:
    from confluent_kafka.common.acl_binding import AclBinding

__all__ = ["AclBindingFilter"]


class AclBindingFilter:
    """A filter which can match ``AclBinding`` objects.

    Java: ``org.apache.kafka.common.acl.AclBindingFilter``.
    """

    __slots__ = ("_pattern_filter", "_entry_filter")

    #: A filter which matches any ACL binding.
    ANY: ClassVar[AclBindingFilter]

    def __init__(self, *, pattern_filter: ResourcePatternFilter,
                 entry_filter: AccessControlEntryFilter) -> None:
        """Create an instance of this filter with the provided parameters:
        ``pattern_filter`` a non-null pattern filter, ``entry_filter`` a
        non-null access control entry filter."""
        if pattern_filter is None:
            raise NullPointerError(message="patternFilter")
        self._pattern_filter = pattern_filter
        if entry_filter is None:
            raise NullPointerError(message="entryFilter")
        self._entry_filter = entry_filter

    def is_unknown(self) -> bool:
        """``True`` if this filter has any UNKNOWN components."""
        return self._pattern_filter.is_unknown() or self._entry_filter.is_unknown()

    def pattern_filter(self) -> ResourcePatternFilter:
        """The resource pattern filter."""
        return self._pattern_filter

    def entry_filter(self) -> AccessControlEntryFilter:
        """The access control entry filter."""
        return self._entry_filter

    def __str__(self) -> str:
        return ("(patternFilter=" + java_str(self._pattern_filter) + ", entryFilter="
                + java_str(self._entry_filter) + ")")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, AclBindingFilter)
        return (self._pattern_filter == other._pattern_filter
                and self._entry_filter == other._entry_filter)

    def matches_at_most_one(self) -> bool:
        """Return true if the resource and entry filters can only match one
        ACE. In other words, if there are no ANY or UNKNOWN fields."""
        return self._pattern_filter.matches_at_most_one() and self._entry_filter.matches_at_most_one()

    def find_indefinite_field(self) -> str | None:
        """Return a string describing an ANY or UNKNOWN field, or ``None`` if
        there is no such field."""
        indefinite = self._pattern_filter.find_indefinite_field()
        if indefinite is not None:
            return indefinite
        return self._entry_filter.find_indefinite_field()

    def matches(self, *, binding: AclBinding) -> bool:
        """Return true if the resource filter matches the binding's resource
        and the entry filter matches binding's entry."""
        return (self._pattern_filter.matches(pattern=binding.pattern())
                and self._entry_filter.matches(other=binding.entry()))

    def __hash__(self) -> int:
        return hash((self._pattern_filter, self._entry_filter))


AclBindingFilter.ANY = AclBindingFilter(pattern_filter=ResourcePatternFilter.ANY,
                                        entry_filter=AccessControlEntryFilter.ANY)
