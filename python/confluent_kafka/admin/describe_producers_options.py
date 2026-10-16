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

"""``DescribeProducersOptions``: Java's ``org.apache.kafka.clients.admin.DescribeProducersOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter
field (CLAUDE.md, Python Binding Conventions, Signatures); ``brokerId`` starts
``OptionalInt.empty()``, so the stub writes ``broker_id: int = ...``. Java's
``toString`` prints the ``OptionalInt`` as ``OptionalInt[5]`` / ``OptionalInt.empty``;
``equals`` / ``hashCode`` read ``brokerId`` and ``timeoutMs``.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka._args import UNSET
from confluent_kafka._java import java_str
from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["DescribeProducersOptions"]


class DescribeProducersOptions(AbstractOptions):
    """Options for ``Admin.describe_producers()``.

    Java: ``org.apache.kafka.clients.admin.DescribeProducersOptions``.
    """

    __slots__ = ("_broker_id",)

    def __init__(self, *, timeout_ms: int | None = UNSET, broker_id: int = UNSET) -> None:
        super().__init__()
        self._broker_id: int | None = None  # OptionalInt.empty()
        if timeout_ms is not UNSET:
            self.timeout_ms(timeout_ms=timeout_ms)
        if broker_id is not UNSET:
            self.broker_id(broker_id=broker_id)

    def broker_id(self, *, broker_id: int | None = None) -> Any:
        """With ``broker_id``: set the broker id and return this object. Without: the
        broker id, or ``None`` when not set."""
        if broker_id is None:
            return self._broker_id
        self._broker_id = broker_id  # OptionalInt.of(brokerId)
        self._record("broker_id", self._broker_id)
        return self

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, DescribeProducersOptions)
        return self._broker_id == other._broker_id and self._timeout_ms == other._timeout_ms

    def __hash__(self) -> int:
        return hash((self._broker_id, self._timeout_ms))

    def __str__(self) -> str:
        broker_id = ("OptionalInt.empty" if self._broker_id is None
                     else "OptionalInt[" + java_str(self._broker_id) + "]")
        return ("DescribeProducersOptions(" + "brokerId=" + broker_id
                + ", timeoutMs=" + java_str(self._timeout_ms) + ")")
