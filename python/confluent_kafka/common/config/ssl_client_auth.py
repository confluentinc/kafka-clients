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

"""``SslClientAuth``: Java's ``org.apache.kafka.common.config.SslClientAuth``.

Java's enum has no constructor, so an ``enum.Enum`` valued by the constant's
name; ``__str__`` is Java's ``toString`` override, the name in lower case
(CLAUDE.md, Python Binding Conventions, Class family). Java's constant
``VALUES`` is a class attribute, a ``tuple`` of the constants in Java's order
(``List.of(values())``).
"""

from __future__ import annotations

from enum import Enum
from typing import ClassVar

__all__ = ["SslClientAuth"]


class SslClientAuth(Enum):
    """Describes whether the server should require or request client
    authentication.

    Java: ``org.apache.kafka.common.config.SslClientAuth``.
    """

    REQUIRED = "REQUIRED"
    REQUESTED = "REQUESTED"
    NONE = "NONE"

    VALUES: ClassVar[tuple[SslClientAuth, ...]]

    @staticmethod
    def for_config(*, key: str | None) -> SslClientAuth | None:
        """The constant whose name is ``key`` in any case; ``NONE`` for
        ``None``, and ``None`` when no constant has that name, as Java's
        ``forConfig`` returns ``null``."""
        if key is None:
            return SslClientAuth.NONE
        upper_case_key = key.upper()
        for auth in SslClientAuth:
            if auth.name == upper_case_key:
                return auth
        return None

    def __str__(self) -> str:
        return self.name.lower()


# Set after the class: an attribute assigned in an Enum's body is a member.
type.__setattr__(SslClientAuth, "VALUES", tuple(SslClientAuth))
