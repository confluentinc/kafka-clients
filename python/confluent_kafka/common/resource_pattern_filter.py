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

"""``ResourcePatternFilter``: Java's
``org.apache.kafka.common.resource.ResourcePatternFilter``.

In ``confluent_kafka.common``, the nearest module above ``common.resource``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from confluent_kafka._java import java_str
from confluent_kafka.common.pattern_type import PatternType
from confluent_kafka.common.resource_type import ResourceType
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.null_pointer_error import NullPointerError

if TYPE_CHECKING:
    from confluent_kafka.common.resource_pattern import ResourcePattern

__all__ = ["ResourcePatternFilter"]

# ResourcePattern.WILDCARD_RESOURCE; repeated here because resource_pattern
# imports this module (ResourcePattern.to_filter).
_WILDCARD_RESOURCE = "*"


class ResourcePatternFilter:
    """Represents a filter that can match ``ResourcePattern``.

    Java: ``org.apache.kafka.common.resource.ResourcePatternFilter``.
    """

    __slots__ = ("_resource_type", "_name", "_pattern_type")

    #: Matches any resource pattern.
    ANY: ClassVar[ResourcePatternFilter]

    def __init__(self, *, resource_type: ResourceType, name: str | None,
                 pattern_type: PatternType) -> None:
        """Create a filter using the supplied parameters.

        ``resource_type``: non-null resource type. If ``ResourceType.ANY``, the
        filter will ignore the resource type of the pattern. If any other
        resource type, the filter will match only patterns with the same type.

        ``name``: resource name or ``None``. If ``None``, the filter will
        ignore the name of resources. If ``ResourcePattern.WILDCARD_RESOURCE``,
        will match only wildcard patterns.

        ``pattern_type``: non-null resource pattern type. If
        ``PatternType.ANY``, the filter will match patterns regardless of
        pattern type. If ``PatternType.MATCH``, the filter will match patterns
        that would match the supplied ``name``, including a matching prefixed
        and wildcards patterns. If any other resource pattern type, the filter
        will match only patterns with the same type.
        """
        if resource_type is None:
            raise NullPointerError(message="resourceType")
        self._resource_type = resource_type
        self._name = name
        if pattern_type is None:
            raise NullPointerError(message="patternType")
        self._pattern_type = pattern_type

    def is_unknown(self) -> bool:
        """``True`` if this filter has any UNKNOWN components."""
        return self._resource_type.is_unknown() or self._pattern_type.is_unknown()

    def resource_type(self) -> ResourceType:
        """The specific resource type this pattern matches."""
        return self._resource_type

    def name(self) -> str | None:
        """The resource name."""
        return self._name

    def pattern_type(self) -> PatternType:
        """The resource pattern type."""
        return self._pattern_type

    def matches(self, *, pattern: ResourcePattern) -> bool:
        """``True`` if this filter matches the given pattern."""
        if (self._resource_type is not ResourceType.ANY
                and self._resource_type != pattern.resource_type()):
            return False
        if (self._pattern_type is not PatternType.ANY and self._pattern_type is not PatternType.MATCH
                and self._pattern_type != pattern.pattern_type()):
            return False
        if self._name is None:
            return True
        if self._pattern_type is PatternType.ANY or self._pattern_type == pattern.pattern_type():
            return self._name == pattern.name()
        if pattern.pattern_type() is PatternType.LITERAL:
            return self._name == pattern.name() or pattern.name() == _WILDCARD_RESOURCE
        if pattern.pattern_type() is PatternType.PREFIXED:
            return self._name.startswith(pattern.name())
        raise IllegalArgumentError(
            message="Unsupported PatternType: " + java_str(pattern.pattern_type()))

    def matches_at_most_one(self) -> bool:
        """``True`` if this filter could only match one pattern. In other
        words, if there are no ANY or UNKNOWN fields."""
        return self.find_indefinite_field() is None

    def find_indefinite_field(self) -> str | None:
        """A string describing any ANY or UNKNOWN field, or ``None`` if there
        is no such field."""
        if self._resource_type is ResourceType.ANY:
            return "Resource type is ANY."
        if self._resource_type is ResourceType.UNKNOWN:
            return "Resource type is UNKNOWN."
        if self._name is None:
            return "Resource name is NULL."
        if self._pattern_type is PatternType.MATCH:
            return "Resource pattern type is MATCH."
        if self._pattern_type is PatternType.UNKNOWN:
            return "Resource pattern type is UNKNOWN."
        return None

    def __str__(self) -> str:
        return ("ResourcePattern(resourceType=" + java_str(self._resource_type) + ", name="
                + ("<any>" if self._name is None else self._name) + ", patternType="
                + java_str(self._pattern_type) + ")")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ResourcePatternFilter)
        return (self._resource_type is other._resource_type and self._name == other._name
                and self._pattern_type is other._pattern_type)

    def __hash__(self) -> int:
        return hash((self._resource_type, self._name, self._pattern_type))


ResourcePatternFilter.ANY = ResourcePatternFilter(
    resource_type=ResourceType.ANY, name=None, pattern_type=PatternType.ANY)
