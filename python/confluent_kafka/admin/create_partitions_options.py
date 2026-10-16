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

"""``CreatePartitionsOptions``: Java's ``org.apache.kafka.clients.admin.CreatePartitionsOptions``.

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

__all__ = ["CreatePartitionsOptions"]


class CreatePartitionsOptions(AbstractOptions):
    """Options for ``Admin.create_partitions()``.

    Java: ``org.apache.kafka.clients.admin.CreatePartitionsOptions``.
    """

    __slots__ = ("_validate_only", "_retry_on_quota_violation")

    def __init__(self, *, timeout_ms: int | None = UNSET, validate_only: bool = UNSET,
                 retry_on_quota_violation: bool = UNSET) -> None:
        super().__init__()
        self._validate_only = False
        self._retry_on_quota_violation = True
        if timeout_ms is not UNSET:
            self.timeout_ms(timeout_ms=timeout_ms)
        if validate_only is not UNSET:
            self.validate_only(validate_only=validate_only)
        if retry_on_quota_violation is not UNSET:
            self.retry_on_quota_violation(retry_on_quota_violation=retry_on_quota_violation)

    def validate_only(self, *, validate_only: bool | None = None) -> Any:
        """With ``validate_only``: set to true if the request should be validated
        without creating new partitions, and return this object.

        Without: return true if the request should be validated without creating new
        partitions.
        """
        if validate_only is None:
            return self._validate_only
        self._validate_only = validate_only
        self._record("validate_only", self._validate_only)
        return self

    def retry_on_quota_violation(self, *,
                                 retry_on_quota_violation: bool) -> CreatePartitionsOptions:
        """Set to true if quota violation should be automatically retried."""
        self._retry_on_quota_violation = retry_on_quota_violation
        self._record("retry_on_quota_violation", self._retry_on_quota_violation)
        return self

    def should_retry_on_quota_violation(self) -> bool:
        """Returns true if quota violation should be automatically retried."""
        return self._retry_on_quota_violation
