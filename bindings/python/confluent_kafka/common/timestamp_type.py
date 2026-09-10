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

"""``TimestampType`` — the timestamp type of a record.

Translated from ``org.apache.kafka.common.record.TimestampType`` (Apache Kafka
4.3.1). The Java enum carries two fields: ``id`` (the wire value, ``-1``/``0``/
``1``) and ``name`` (a label string, ``"NoTimestampType"`` / ``"CreateTime"`` /
``"LogAppendTime"``). The spec (§5.3) maps the enum to a Python ``IntEnum``
whose member value is Java's ``id``. Java's label ``name`` is exposed through
``label()`` and ``__str__`` (Java ``toString``), and ``for_name`` mirrors Java's
static lookup — the Python member ``.name`` (``"NO_TIMESTAMP_TYPE"``) is a
different string and is left to Python.
"""

from __future__ import annotations

from enum import IntEnum


class TimestampType(IntEnum):
    """The timestamp type of the records.

    Java: ``org.apache.kafka.common.record.TimestampType``. The member value is
    Java's ``id`` field.
    """

    NO_TIMESTAMP_TYPE = -1
    CREATE_TIME = 0
    LOG_APPEND_TIME = 1

    def id(self) -> int:
        """Java's ``id`` field — the wire value (``-1``/``0``/``1``)."""
        return self.value

    def label(self) -> str:
        """Java's ``name`` field — the label string used on the wire/logs."""
        return _LABELS[self]

    @staticmethod
    def for_name(*, name: str) -> TimestampType:
        """Look up a ``TimestampType`` by its Java label (``name`` field).

        Java ``forName`` raises ``NoSuchElementException`` for an unknown label;
        the Python analog raises ``KeyError`` for the same condition.
        """
        for member, label in _LABELS.items():
            if label == name:
                return member
        raise KeyError(f"Invalid timestamp type {name}")

    def __str__(self) -> str:
        # Java toString returns the label (name field), not the member name.
        return _LABELS[self]


_LABELS: dict[TimestampType, str] = {
    TimestampType.NO_TIMESTAMP_TYPE: "NoTimestampType",
    TimestampType.CREATE_TIME: "CreateTime",
    TimestampType.LOG_APPEND_TIME: "LogAppendTime",
}
