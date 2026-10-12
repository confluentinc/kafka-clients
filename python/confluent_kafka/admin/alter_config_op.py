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

"""``AlterConfigOp``: Java's ``org.apache.kafka.clients.admin.AlterConfigOp``.

``OpType`` is an ``enum.IntEnum`` valued by Java's ``id`` field (CLAUDE.md,
Python Binding Conventions, Class family). The constructor's second keyword is
Java's parameter name, ``operation_type``; the getter keeps Java's
``op_type()``.
"""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING

from confluent_kafka._java import java_str

if TYPE_CHECKING:
    from .config_entry import ConfigEntry

__all__ = ["AlterConfigOp"]


class AlterConfigOp:
    """A class representing an alter configuration entry containing name,
    value and operation type.

    **Note for Broker Logger Configuration:** when altering broker logger
    levels (using ``ConfigResource.Type.BROKER_LOGGER``), it is strongly
    recommended to use log level constants from ``LogLevelConfig`` instead of
    string literals. This ensures compatibility with Kafka's log level
    validation and avoids potential configuration errors.

    Java: ``org.apache.kafka.clients.admin.AlterConfigOp``.
    """

    class OpType(IntEnum):
        """The operation of an ``AlterConfigOp``.

        Java: ``org.apache.kafka.clients.admin.AlterConfigOp.OpType``.
        """

        #: Set the value of the configuration entry.
        SET = 0
        #: Revert the configuration entry to the default value (possibly null).
        DELETE = 1
        #: (For list-type configuration entries only.) Add the specified values
        #: to the current value of the configuration entry. If the configuration
        #: value has not been set, adds to the default value.
        APPEND = 2
        #: (For list-type configuration entries only.) Removes the specified
        #: values from the current value of the configuration entry. It is legal
        #: to remove values that are not currently in the configuration entry.
        #: Removing all entries from the current configuration value leaves an
        #: empty list and does NOT revert to the default value of the entry.
        SUBTRACT = 3

        def id(self) -> int:  # noqa: A003 - Java's name
            return self.value

        @staticmethod
        def for_id(*, id: int) -> AlterConfigOp.OpType | None:  # noqa: A002 - Java's name
            """The operation whose id is ``id``, or ``None`` when there is
            none."""
            for op in AlterConfigOp.OpType:
                if op.value == id:
                    return op
            return None

        def __str__(self) -> str:
            return self.name

    __slots__ = ("_config_entry", "_op_type")

    def __init__(self, *, config_entry: ConfigEntry,
                 operation_type: AlterConfigOp.OpType) -> None:
        self._config_entry = config_entry
        self._op_type = operation_type

    def config_entry(self) -> ConfigEntry:
        return self._config_entry

    def op_type(self) -> AlterConfigOp.OpType:
        return self._op_type

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, AlterConfigOp)
        return self._op_type is other._op_type and self._config_entry == other._config_entry

    def __hash__(self) -> int:
        return hash((self._op_type, self._config_entry))

    def __str__(self) -> str:
        return ("AlterConfigOp{opType=" + java_str(self._op_type) + ", configEntry="
                + java_str(self._config_entry) + "}")
