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

"""``SecurityProtocol``: Java's ``org.apache.kafka.common.security.auth.SecurityProtocol``.

Java's constructor sets one integer field, ``id``, so an ``enum.IntEnum``
valued by it: Python's ``.value`` reads what Java's ``id`` reads. Java's
other public field, ``name``, equals the constant's name, which ``.name``
and ``__str__`` (Java's ``Enum.toString()``) give (CLAUDE.md, Python
Binding Conventions, Class family).
"""

from __future__ import annotations

from enum import IntEnum

from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["SecurityProtocol"]

_JAVA_CLASS = "org.apache.kafka.common.security.auth.SecurityProtocol"


class SecurityProtocol(IntEnum):
    """Java: ``org.apache.kafka.common.security.auth.SecurityProtocol``."""

    PLAINTEXT = 0
    """Un-authenticated, non-encrypted channel."""
    SSL = 1
    """SSL channel."""
    SASL_PLAINTEXT = 2
    """SASL authenticated, non-encrypted channel."""
    SASL_SSL = 3
    """SASL authenticated, SSL channel."""

    @staticmethod
    def names() -> list[str]:
        """The names of the protocols, in declaration order."""
        return [p.name for p in SecurityProtocol]

    @staticmethod
    def for_id(*, id: int) -> SecurityProtocol | None:
        """The protocol whose ``id`` is ``id``, or ``None``, as Java's
        map lookup returns ``null``."""
        for p in SecurityProtocol:
            if p.value == id:
                return p
        return None

    @staticmethod
    def for_name(*, name: str) -> SecurityProtocol:
        """The protocol named ``name`` in any case, as Java's
        ``valueOf(name.toUpperCase(Locale.ROOT))``."""
        if name is None:
            raise TypeError("name must not be None")
        upper = name.upper()
        try:
            return SecurityProtocol[upper]
        except KeyError:
            raise IllegalArgumentError(message=f"No enum constant {_JAVA_CLASS}.{upper}") from None

    def __str__(self) -> str:
        return self.name
