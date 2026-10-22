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

"""``ListTopicsOptions``: Java's ``org.apache.kafka.clients.admin.ListTopicsOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter
field (CLAUDE.md, Python Binding Conventions, Signatures). Java's ``equals`` /
``hashCode`` / ``toString`` are ``__eq__`` / ``__hash__`` / ``__str__`` (Idiom
translations); like Java's they read ``listInternal`` only, not ``timeoutMs``.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka._args import MISSING
from confluent_kafka._java import java_str
from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["ListTopicsOptions"]


class ListTopicsOptions(AbstractOptions):
    """Options for ``Admin.list_topics()``.

    Java: ``org.apache.kafka.clients.admin.ListTopicsOptions``.
    """

    __slots__ = ("_list_internal",)

    def __init__(self, *, timeout_ms: int | None = MISSING, list_internal: bool = MISSING) -> None:
        super().__init__()
        self._list_internal = False
        if timeout_ms is not MISSING:
            self.timeout_ms(timeout_ms=timeout_ms)
        if list_internal is not MISSING:
            self.list_internal(list_internal=list_internal)

    def timeout_ms(self, *, timeout_ms: int | None = MISSING) -> Any:
        """With ``timeout_ms``: set the timeout in milliseconds for this operation or
        ``None`` if the default api timeout for the AdminClient should be used, and
        return this object. Without: the timeout in milliseconds, or ``None``."""
        # This method is retained to keep binary compatibility with 0.11
        if timeout_ms is MISSING:
            return self._timeout_ms
        self._timeout_ms = timeout_ms
        self._record("timeout_ms", self._timeout_ms)
        return self

    def list_internal(self, *, list_internal: bool) -> ListTopicsOptions:
        """Set whether we should list internal topics.

        :param list_internal: Whether we should list internal topics.
        :return: This ListTopicsOptions object.
        """
        if list_internal is None:  # a boolean: Java cannot pass null
            raise TypeError("list_internal must not be None")
        self._list_internal = list_internal
        self._record("list_internal", self._list_internal)
        return self

    def should_list_internal(self) -> bool:
        """Return true if we should list internal topics."""
        return self._list_internal

    def __str__(self) -> str:
        return "ListTopicsOptions(" + "listInternal=" + java_str(self._list_internal) + ")"

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ListTopicsOptions)
        return self._list_internal == other._list_internal

    def __hash__(self) -> int:
        return hash((self._list_internal,))
