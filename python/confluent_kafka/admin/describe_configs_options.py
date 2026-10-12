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

"""``DescribeConfigsOptions``: Java's ``org.apache.kafka.clients.admin.DescribeConfigsOptions``.

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

__all__ = ["DescribeConfigsOptions"]


class DescribeConfigsOptions(AbstractOptions):
    """Options for ``Admin.describe_configs()``.

    Java: ``org.apache.kafka.clients.admin.DescribeConfigsOptions``.
    """

    __slots__ = ("_include_synonyms", "_include_documentation")

    def __init__(self, *, timeout_ms: int | None = UNSET, include_synonyms: bool = UNSET,
                 include_documentation: bool = UNSET) -> None:
        super().__init__()
        self._include_synonyms = False
        self._include_documentation = False
        if timeout_ms is not UNSET:
            self.timeout_ms(timeout_ms=timeout_ms)
        if include_synonyms is not UNSET:
            self.include_synonyms(include_synonyms=include_synonyms)
        if include_documentation is not UNSET:
            self.include_documentation(include_documentation=include_documentation)

    def timeout_ms(self, *, timeout_ms: int | None = UNSET) -> Any:
        """With ``timeout_ms``: set the timeout in milliseconds for this operation or
        ``None`` if the default api timeout for the AdminClient should be used, and
        return this object. Without: the timeout in milliseconds, or ``None``."""
        # This method is retained to keep binary compatibility with 0.11
        if timeout_ms is UNSET:
            return self._timeout_ms
        self._timeout_ms = timeout_ms
        return self

    def include_synonyms(self, *, include_synonyms: bool | None = None) -> Any:
        """With ``include_synonyms``: set to true if synonym configs should be returned
        in the response, and return this object.

        Without: return true if synonym configs should be returned in the response.
        """
        if include_synonyms is None:
            return self._include_synonyms
        self._include_synonyms = include_synonyms
        return self

    def include_documentation(self, *, include_documentation: bool | None = None) -> Any:
        """With ``include_documentation``: set to true if config documentation should be
        returned in the response, and return this object.

        Without: return true if config documentation should be returned in the response.
        """
        if include_documentation is None:
            return self._include_documentation
        self._include_documentation = include_documentation
        return self
