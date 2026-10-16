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
The arguments are not nullable, so their default is ``None`` ("not given": read).
"""

from __future__ import annotations

from typing import Any

from confluent_kafka._args import UNSET
from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["DescribeTopicsOptions"]


class DescribeTopicsOptions(AbstractOptions):
    """Options for ``Admin.describe_topics()``.

    Java: ``org.apache.kafka.clients.admin.DescribeTopicsOptions``.
    """

    __slots__ = ("_include_authorized_operations", "_partition_size_limit_per_response")

    def __init__(self, *, timeout_ms: int | None = UNSET,
                 include_authorized_operations: bool = UNSET,
                 partition_size_limit_per_response: int = UNSET) -> None:
        super().__init__()
        self._include_authorized_operations = False
        self._partition_size_limit_per_response = 2000
        if timeout_ms is not UNSET:
            self.timeout_ms(timeout_ms=timeout_ms)
        if include_authorized_operations is not UNSET:
            self.include_authorized_operations(
                include_authorized_operations=include_authorized_operations)
        if partition_size_limit_per_response is not UNSET:
            self.partition_size_limit_per_response(
                partition_size_limit_per_response=partition_size_limit_per_response)

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
        Without: whether to include the authorized operations."""
        if include_authorized_operations is None:
            return self._include_authorized_operations
        self._include_authorized_operations = include_authorized_operations
        self._record("include_authorized_operations", self._include_authorized_operations)
        return self

    def partition_size_limit_per_response(
            self, *, partition_size_limit_per_response: int | None = None) -> Any:
        """With ``partition_size_limit_per_response``: set the maximum number of
        partitions to be returned in a single response, and return this object.

        This option is only effective when using topic names (not topic IDs), and will
        not be effective if it is larger than the server-side configuration
        ``max.request.partition.size.limit``.

        Without: the maximum number of partitions per response.
        """
        if partition_size_limit_per_response is None:
            return self._partition_size_limit_per_response
        self._partition_size_limit_per_response = partition_size_limit_per_response
        self._record("partition_size_limit_per_response", self._partition_size_limit_per_response)
        return self
