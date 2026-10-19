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

"""``CreateTopicsOptions``: Java's ``org.apache.kafka.clients.admin.CreateTopicsOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter
field, the parent's ``timeout_ms`` first, then ``validate_only`` and
``retry_on_quota_violation`` in Java's field order (CLAUDE.md, Python Binding
Conventions, Signatures). Each keyword given, ``None`` included, calls its setter; one
left out leaves the field at Java's starting value. Java re-declares ``timeoutMs``
(binary compatibility with 0.11) with its own return type; so does this class.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka._args import MISSING
from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["CreateTopicsOptions"]


class CreateTopicsOptions(AbstractOptions):
    """Options for ``Admin.create_topics()``.

    Java: ``org.apache.kafka.clients.admin.CreateTopicsOptions``.
    """

    __slots__ = ("_validate_only", "_retry_on_quota_violation")

    def __init__(self, *, timeout_ms: int | None = MISSING, validate_only: bool = MISSING,
                 retry_on_quota_violation: bool = MISSING) -> None:
        super().__init__()
        self._validate_only = False
        self._retry_on_quota_violation = True
        if timeout_ms is not MISSING:
            self.timeout_ms(timeout_ms=timeout_ms)
        if validate_only is not MISSING:
            self.validate_only(validate_only=validate_only)
        if retry_on_quota_violation is not MISSING:
            self.retry_on_quota_violation(retry_on_quota_violation=retry_on_quota_violation)

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

    def validate_only(self, *, validate_only: bool) -> CreateTopicsOptions:
        """Set to true if the request should be validated without creating the topic."""
        if validate_only is None:  # a boolean: Java cannot pass null
            raise TypeError("validate_only must not be None")
        self._validate_only = validate_only
        self._record("validate_only", self._validate_only)
        return self

    def should_validate_only(self) -> bool:
        """Return true if the request should be validated without creating the topic."""
        return self._validate_only

    def retry_on_quota_violation(self, *, retry_on_quota_violation: bool) -> CreateTopicsOptions:
        """Set to true if quota violation should be automatically retried."""
        if retry_on_quota_violation is None:  # a boolean: Java cannot pass null
            raise TypeError("retry_on_quota_violation must not be None")
        self._retry_on_quota_violation = retry_on_quota_violation
        self._record("retry_on_quota_violation", self._retry_on_quota_violation)
        return self

    def should_retry_on_quota_violation(self) -> bool:
        """Returns true if quota violation should be automatically retried."""
        return self._retry_on_quota_violation
