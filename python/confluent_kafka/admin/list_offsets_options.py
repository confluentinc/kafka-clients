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

"""``ListOffsetsOptions``: Java's ``org.apache.kafka.clients.admin.ListOffsetsOptions``.

Java has a constructor that takes an argument, so the class keeps only Java's
constructors, ``()`` and ``(isolationLevel)``, as one keyword-only constructor
(CLAUDE.md, Python Binding Conventions, Signatures), with one stub each. ``()`` calls
``this(IsolationLevel.READ_UNCOMMITTED)``, a Java-given default the constructor fills
when ``isolation_level`` is left out; every combination is a Java form, so there is no
``java_forms`` check. ``isolationLevel`` is ``final``: there is no setter.
"""

from __future__ import annotations

from confluent_kafka._args import MISSING
from confluent_kafka.admin.abstract_options import AbstractOptions
from confluent_kafka.common.isolation_level import IsolationLevel

__all__ = ["ListOffsetsOptions"]


class ListOffsetsOptions(AbstractOptions):
    """Options for ``Admin.list_offsets()``.

    Java: ``org.apache.kafka.clients.admin.ListOffsetsOptions``.
    """

    __slots__ = ("_isolation_level",)

    def __init__(self, *, isolation_level: IsolationLevel = MISSING) -> None:
        super().__init__()
        if isolation_level is MISSING:
            isolation_level = IsolationLevel.READ_UNCOMMITTED
        self._isolation_level = isolation_level

    def isolation_level(self) -> IsolationLevel:
        return self._isolation_level
