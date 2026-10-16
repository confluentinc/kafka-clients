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

"""``IsolationLevel``: Java's ``org.apache.kafka.common.IsolationLevel``.

An ``enum.IntEnum`` valued by Java's ``id`` field (CLAUDE.md, Python Binding
Conventions, Class family). ``__str__`` is Java's ``toString()``: the
constant's name in lower case.
"""

from __future__ import annotations

from enum import IntEnum

from confluent_kafka._java import java_str
from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["IsolationLevel"]


class IsolationLevel(IntEnum):
    """The isolation level of a read.

    Java: ``org.apache.kafka.common.IsolationLevel``.
    """

    READ_UNCOMMITTED = 0
    READ_COMMITTED = 1

    def id(self) -> int:
        return int(self)

    @staticmethod
    def for_id(*, id: int) -> IsolationLevel:  # noqa: A002 - Java's name
        """The isolation level whose id is ``id``; ``IllegalArgumentError``
        when there is none."""
        if id == 0:
            return IsolationLevel.READ_UNCOMMITTED
        if id == 1:
            return IsolationLevel.READ_COMMITTED
        raise IllegalArgumentError(message="Unknown isolation level " + java_str(id))

    def __str__(self) -> str:
        return self.name.lower()
