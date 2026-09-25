# Copyright 2025 Confluent Inc.
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

"""``TimestampType``: Java's ``org.apache.kafka.common.record.TimestampType``.

In ``confluent_kafka.common``, the nearest module above ``common.record``. An
``enum.IntEnum`` valued by Java's ``id`` field; Java's other public field,
``name``, is reached through Java's own methods: ``__str__`` (``toString``) and
``for_name`` (``forName``) (CLAUDE.md, Python Binding Conventions, Class
family).
"""

from __future__ import annotations

from enum import IntEnum

from confluent_kafka._java import java_str
from confluent_kafka.no_such_element_error import NoSuchElementError

__all__ = ["TimestampType"]


class TimestampType(IntEnum):
    """The timestamp type of the records.

    Java: ``org.apache.kafka.common.record.TimestampType``.
    """

    NO_TIMESTAMP_TYPE = -1
    CREATE_TIME = 0
    LOG_APPEND_TIME = 1

    @staticmethod
    def for_name(*, name: str) -> TimestampType:
        """The type whose Java ``name`` is ``name``; ``NoSuchElementError``
        when there is none."""
        for t in TimestampType:
            if _NAMES[t] == name:
                return t
        raise NoSuchElementError(message="Invalid timestamp type " + java_str(name))

    def __str__(self) -> str:
        return _NAMES[self]


_NAMES: dict[TimestampType, str] = {
    TimestampType.NO_TIMESTAMP_TYPE: "NoTimestampType",
    TimestampType.CREATE_TIME: "CreateTime",
    TimestampType.LOG_APPEND_TIME: "LogAppendTime",
}
