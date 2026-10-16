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

"""``ElectionType``: Java's ``org.apache.kafka.common.ElectionType``.

An ``enum.IntEnum`` valued by Java's ``public final byte value`` field, so
Python's ``.value`` reads what Java's ``value`` field reads (CLAUDE.md, Python
Binding Conventions, Class family). ``__str__`` is Java's ``Enum.toString()``,
the constant's name.
"""

from __future__ import annotations

from enum import IntEnum

from confluent_kafka._java import java_str
from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["ElectionType"]


class ElectionType(IntEnum):
    """Options for ``Admin.elect_leaders``.

    Java: ``org.apache.kafka.common.ElectionType``.
    """

    PREFERRED = 0
    UNCLEAN = 1

    @staticmethod
    def value_of(*, value: int) -> ElectionType:
        """The election type whose ``value`` is ``value``;
        ``IllegalArgumentError`` when there is none."""
        if value == ElectionType.PREFERRED.value:
            return ElectionType.PREFERRED
        if value == ElectionType.UNCLEAN.value:
            return ElectionType.UNCLEAN
        raise IllegalArgumentError(
            message="Value " + java_str(value) + " must be one of "
            + java_str(list(ElectionType)))

    def __str__(self) -> str:
        return self.name
