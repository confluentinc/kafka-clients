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

"""``DescribeClusterOptions``: Java's ``org.apache.kafka.clients.admin.DescribeClusterOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter field,
the parent's ``timeout_ms`` first, then the class's own in Java's field order
(CLAUDE.md, Python Binding Conventions, Signatures). Each keyword given, ``None``
included, calls its setter; one left out leaves the field at Java's starting value. A
setter sharing its name with its getter is one method with two stubs: no argument reads,
one argument sets (Signatures). Java re-declares ``timeoutMs`` (binary compatibility
with 0.11) with its own return type; so does this class.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka._args import UNSET
from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["DescribeClusterOptions"]


class DescribeClusterOptions(AbstractOptions):
    """Options for ``Admin.describe_cluster()``.

    Java: ``org.apache.kafka.clients.admin.DescribeClusterOptions``.
    """

    __slots__ = ("_include_authorized_operations", "_include_fenced_brokers")

    def __init__(self, *, timeout_ms: int | None = UNSET,
                 include_authorized_operations: bool = UNSET,
                 include_fenced_brokers: bool = UNSET) -> None:
        super().__init__()
        self._include_authorized_operations = False
        self._include_fenced_brokers = False
        if timeout_ms is not UNSET:
            self.timeout_ms(timeout_ms=timeout_ms)
        if include_authorized_operations is not UNSET:
            self.include_authorized_operations(
                include_authorized_operations=include_authorized_operations)
        if include_fenced_brokers is not UNSET:
            self.include_fenced_brokers(include_fenced_brokers=include_fenced_brokers)

    def timeout_ms(self, *, timeout_ms: int | None = UNSET) -> Any:
        """With ``timeout_ms``: set the timeout in milliseconds for this operation or
        ``None`` if the default api timeout for the AdminClient should be used, and
        return this object. Without: the timeout in milliseconds, or ``None``."""
        # This method is retained to keep binary compatibility with 0.11
        if timeout_ms is UNSET:
            return self._timeout_ms
        self._timeout_ms = timeout_ms
        self._record("timeout_ms", self._timeout_ms)
        return self

    def include_authorized_operations(self, *,
                                      include_authorized_operations: bool | None = None) -> Any:
        """With ``include_authorized_operations``: set it and return this object.

        Without: specify if authorized operations should be included in the response.
        Note that some older brokers cannot supply this information even if it is
        requested.
        """
        if include_authorized_operations is None:
            return self._include_authorized_operations
        self._include_authorized_operations = include_authorized_operations
        self._record("include_authorized_operations", self._include_authorized_operations)
        return self

    def include_fenced_brokers(self, *, include_fenced_brokers: bool | None = None) -> Any:
        """With ``include_fenced_brokers``: set it and return this object.

        Without: specify if fenced brokers should be included in the response. Note that
        some older brokers cannot supply this information even if it is requested.
        """
        if include_fenced_brokers is None:
            return self._include_fenced_brokers
        self._include_fenced_brokers = include_fenced_brokers
        self._record("include_fenced_brokers", self._include_fenced_brokers)
        return self
