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

"""``ConfigResource``: Java's ``org.apache.kafka.common.config.ConfigResource``.

The nested enum ``ConfigResource.Type`` is an ``enum.IntEnum`` valued by Java's
``id`` field, its members in Java's (descending id) order; ``__str__`` is
Java's ``Enum.toString()``, the constant's name (CLAUDE.md, Python Binding
Conventions, Class family).
"""

from __future__ import annotations

from enum import IntEnum

from confluent_kafka._java import java_str
from confluent_kafka.null_pointer_error import NullPointerError

__all__ = ["ConfigResource"]


class ConfigResource:
    """A class representing resources that have configs.

    Java: ``org.apache.kafka.common.config.ConfigResource``.
    """

    class Type(IntEnum):
        """Type of resource.

        Java: ``org.apache.kafka.common.config.ConfigResource.Type``.
        """

        GROUP = 32
        CLIENT_METRICS = 16
        BROKER_LOGGER = 8
        BROKER = 4
        TOPIC = 2
        UNKNOWN = 0

        def id(self) -> int:
            return int(self)

        @staticmethod
        def for_id(*, id: int) -> ConfigResource.Type:  # noqa: A002 - Java's name
            """The type whose id is ``id``, or ``UNKNOWN``."""
            try:
                return ConfigResource.Type(id)
            except ValueError:
                return ConfigResource.Type.UNKNOWN

        def __str__(self) -> str:
            return self.name

    __slots__ = ("_type", "_name")

    def __init__(self, *, type: ConfigResource.Type, name: str) -> None:  # noqa: A002 - Java's name
        """Create an instance of this class with the provided parameters:
        ``type`` a non-null resource type, ``name`` a non-null resource
        name."""
        if type is None:
            raise NullPointerError(message="type should not be null")
        if name is None:
            raise NullPointerError(message="name should not be null")
        self._type = type
        self._name = name

    def type(self) -> ConfigResource.Type:
        """Return the resource type."""
        return self._type

    def name(self) -> str:
        """Return the resource name."""
        return self._name

    def is_default(self) -> bool:
        """Returns true if this is the default resource of a resource type.
        Resource name is empty for the default resource."""
        return self._name == ""

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):  # noqa: E721
            return NotImplemented
        assert isinstance(other, ConfigResource)
        return self._type is other._type and self._name == other._name

    def __hash__(self) -> int:
        return hash((self._type, self._name))

    def __str__(self) -> str:
        return "ConfigResource(type=" + java_str(self._type) + ", name='" + self._name + "')"
