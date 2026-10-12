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

"""``DescribeConsumerGroupsOptions``: Java's
``org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions``.

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

__all__ = ["DescribeConsumerGroupsOptions"]


class DescribeConsumerGroupsOptions(AbstractOptions):
    """Options for ``Admin.describe_consumer_groups()``.

    Java: ``org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions``.
    """

    __slots__ = ("_include_authorized_operations",)

    def __init__(self, *, timeout_ms: int | None = UNSET,
                 include_authorized_operations: bool = UNSET) -> None:
        super().__init__()
        self._include_authorized_operations = False
        if timeout_ms is not UNSET:
            self.timeout_ms(timeout_ms=timeout_ms)
        if include_authorized_operations is not UNSET:
            self.include_authorized_operations(
                include_authorized_operations=include_authorized_operations)

    def include_authorized_operations(self, *,
                                      include_authorized_operations: bool | None = None) -> Any:
        """With ``include_authorized_operations``: set it and return this object.

        Without: the current value.
        """
        if include_authorized_operations is None:
            return self._include_authorized_operations
        self._include_authorized_operations = include_authorized_operations
        return self
