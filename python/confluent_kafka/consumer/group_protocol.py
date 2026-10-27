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

"""``GroupProtocol``: Java's ``org.apache.kafka.clients.consumer.GroupProtocol``.

Java's constructor sets no integer field, so an ``enum.Enum`` valued by the
constant's name; Java's public ``name`` field equals it, which ``.name``
and ``__str__`` (Java's ``Enum.toString()``) give (CLAUDE.md, Python
Binding Conventions, Class family).
"""

from __future__ import annotations

from enum import Enum

from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["GroupProtocol"]

_JAVA_CLASS = "org.apache.kafka.clients.consumer.GroupProtocol"


class GroupProtocol(Enum):
    """Java: ``org.apache.kafka.clients.consumer.GroupProtocol``."""

    CLASSIC = "CLASSIC"
    """Classic group protocol."""
    CONSUMER = "CONSUMER"
    """Consumer group protocol."""

    @staticmethod
    def of(*, name: str) -> GroupProtocol:
        """The protocol named ``name`` in any case, as Java's
        ``valueOf(name.toUpperCase(Locale.ROOT))``."""
        if name is None:
            raise TypeError("name must not be None")
        upper = name.upper()
        try:
            return GroupProtocol[upper]
        except KeyError:
            raise IllegalArgumentError(message=f"No enum constant {_JAVA_CLASS}.{upper}") from None

    def __str__(self) -> str:
        return self.name
