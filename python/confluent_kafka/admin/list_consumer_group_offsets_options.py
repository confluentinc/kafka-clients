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

"""``ListConsumerGroupOffsetsOptions``: Java's
``org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter field,
the parent's ``timeout_ms`` first, then the class's own in Java's field order
(CLAUDE.md, Python Binding Conventions, Signatures). Each keyword given, ``None``
included, calls its setter; one left out leaves the field at Java's starting value. A
setter sharing its name with its getter is one method with two stubs: no argument reads,
one argument sets (Signatures).
"""

from __future__ import annotations

from typing import Any

from confluent_kafka._args import UNSET
from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["ListConsumerGroupOffsetsOptions"]


class ListConsumerGroupOffsetsOptions(AbstractOptions):
    """Options for ``Admin.list_consumer_group_offsets()``.

    Java: ``org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions``.
    """

    __slots__ = ("_require_stable",)

    def __init__(self, *, timeout_ms: int | None = UNSET, require_stable: bool = UNSET) -> None:
        super().__init__()
        self._require_stable = False
        if timeout_ms is not UNSET:
            self.timeout_ms(timeout_ms=timeout_ms)
        if require_stable is not UNSET:
            self.require_stable(require_stable=require_stable)

    def require_stable(self, *, require_stable: bool | None = None) -> Any:
        """With ``require_stable``: sets an optional requireStable flag, and return this
        object.

        Without: the current value.
        """
        if require_stable is None:
            return self._require_stable
        self._require_stable = require_stable
        self._record("require_stable", self._require_stable)
        return self
