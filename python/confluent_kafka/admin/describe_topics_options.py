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

"""``DescribeTopicsOptions``: Java's ``org.apache.kafka.clients.admin.DescribeTopicsOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter
field (CLAUDE.md, Python Binding Conventions, Signatures). Each setter shares its name
with its getter: one method, two stubs, no argument reading and one argument setting.
Left out, the argument reads; it is not nullable, so a ``None`` raises ``TypeError``.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka._args import MISSING
from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["DescribeTopicsOptions"]


class DescribeTopicsOptions(AbstractOptions):
    """Options for ``Admin.describe_topics()``.

    Java: ``org.apache.kafka.clients.admin.DescribeTopicsOptions``.
    """

    __slots__ = ("_include_authorized_operations", "_partition_size_limit_per_response")

    def __init__(self, *, timeout_ms: int | None = MISSING,
                 include_authorized_operations: bool = MISSING,
                 partition_size_limit_per_response: int = MISSING) -> None:
        super().__init__()
        self._include_authorized_operations = False
        self._partition_size_limit_per_response = 2000
        if timeout_ms is not MISSING:
            self.timeout_ms(timeout_ms=timeout_ms)
        if include_authorized_operations is not MISSING:
            self.include_authorized_operations(
                include_authorized_operations=include_authorized_operations)
        if partition_size_limit_per_response is not MISSING:
            self.partition_size_limit_per_response(
                partition_size_limit_per_response=partition_size_limit_per_response)

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

    def include_authorized_operations(self, *,
                                      include_authorized_operations: bool = MISSING) -> Any:
        """With ``include_authorized_operations``: set it and return this object.
        Without: whether to include the authorized operations."""
        if include_authorized_operations is MISSING:
            return self._include_authorized_operations
        if include_authorized_operations is None:  # a boolean: Java cannot pass null
            raise TypeError("include_authorized_operations must not be None")
        self._include_authorized_operations = include_authorized_operations
        self._record("include_authorized_operations", self._include_authorized_operations)
        return self

    def partition_size_limit_per_response(self, *,
                                          partition_size_limit_per_response: int = MISSING) -> Any:
        """With ``partition_size_limit_per_response``: set the maximum number of
        partitions to be returned in a single response, and return this object.

        This option is only effective when using topic names (not topic IDs), and will
        not be effective if it is larger than the server-side configuration
        ``max.request.partition.size.limit``.

        Without: the maximum number of partitions per response.
        """
        if partition_size_limit_per_response is MISSING:
            return self._partition_size_limit_per_response
        if partition_size_limit_per_response is None:  # a int: Java cannot pass null
            raise TypeError("partition_size_limit_per_response must not be None")
        self._partition_size_limit_per_response = partition_size_limit_per_response
        self._record("partition_size_limit_per_response", self._partition_size_limit_per_response)
        return self
