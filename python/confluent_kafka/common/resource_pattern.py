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

"""``ResourcePattern``: Java's ``org.apache.kafka.common.resource.ResourcePattern``.

In ``confluent_kafka.common``, the nearest module above ``common.resource``.
"""

from __future__ import annotations

from confluent_kafka._java import java_str
from confluent_kafka.common.pattern_type import PatternType
from confluent_kafka.common.resource_pattern_filter import ResourcePatternFilter
from confluent_kafka.common.resource_type import ResourceType
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.null_pointer_error import NullPointerError

__all__ = ["ResourcePattern"]


class ResourcePattern:
    """Represents a pattern that is used by ACLs to match zero or more
    resources.

    Java: ``org.apache.kafka.common.resource.ResourcePattern``.
    """

    #: A special literal resource name that corresponds to 'all resources of
    #: a certain type'.
    WILDCARD_RESOURCE = "*"

    __slots__ = ("_resource_type", "_name", "_pattern_type")

    def __init__(self, *, resource_type: ResourceType, name: str,
                 pattern_type: PatternType) -> None:
        """Create a pattern using the supplied parameters: ``resource_type`` a
        non-null, specific, resource type; ``name`` a non-null resource name,
        which can be the ``WILDCARD_RESOURCE``; ``pattern_type`` a non-null,
        specific, resource pattern type, which controls how the pattern will
        match resource names."""
        if resource_type is None:
            raise NullPointerError(message="resourceType")
        self._resource_type = resource_type
        if name is None:
            raise NullPointerError(message="name")
        self._name = name
        if pattern_type is None:
            raise NullPointerError(message="patternType")
        self._pattern_type = pattern_type
        if resource_type is ResourceType.ANY:
            raise IllegalArgumentError(message="resourceType must not be ANY")
        if pattern_type is PatternType.MATCH or pattern_type is PatternType.ANY:
            raise IllegalArgumentError(message="patternType must not be " + java_str(pattern_type))

    def resource_type(self) -> ResourceType:
        """The specific resource type this pattern matches."""
        return self._resource_type

    def name(self) -> str:
        """The resource name."""
        return self._name

    def pattern_type(self) -> PatternType:
        """The resource pattern type."""
        return self._pattern_type

    def to_filter(self) -> ResourcePatternFilter:
        """A filter which matches only this pattern."""
        return ResourcePatternFilter(resource_type=self._resource_type, name=self._name,
                                     pattern_type=self._pattern_type)

    def __str__(self) -> str:
        return ("ResourcePattern(resourceType=" + java_str(self._resource_type) + ", name="
                + self._name + ", patternType=" + java_str(self._pattern_type) + ")")

    def is_unknown(self) -> bool:
        """``True`` if this Resource has any UNKNOWN components."""
        return self._resource_type.is_unknown() or self._pattern_type.is_unknown()

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ResourcePattern)
        return (self._resource_type is other._resource_type and self._name == other._name
                and self._pattern_type is other._pattern_type)

    def __hash__(self) -> int:
        return hash((self._resource_type, self._name, self._pattern_type))
