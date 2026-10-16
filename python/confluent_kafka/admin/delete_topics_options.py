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

"""``DeleteTopicsOptions``: Java's ``org.apache.kafka.clients.admin.DeleteTopicsOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter field,
the parent's ``timeout_ms`` first, then the class's own in Java's field order
(CLAUDE.md, Python Binding Conventions, Signatures). Each keyword given, ``None``
included, calls its setter; one left out leaves the field at Java's starting value. Java
re-declares ``timeoutMs`` (binary compatibility with 0.11) with its own return type; so
does this class.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka._args import UNSET
from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["DeleteTopicsOptions"]


class DeleteTopicsOptions(AbstractOptions):
    """Options for ``Admin.delete_topics()``.

    Java: ``org.apache.kafka.clients.admin.DeleteTopicsOptions``.
    """

    __slots__ = ("_retry_on_quota_violation",)

    def __init__(self, *, timeout_ms: int | None = UNSET,
                 retry_on_quota_violation: bool = UNSET) -> None:
        super().__init__()
        self._retry_on_quota_violation = True
        if timeout_ms is not UNSET:
            self.timeout_ms(timeout_ms=timeout_ms)
        if retry_on_quota_violation is not UNSET:
            self.retry_on_quota_violation(retry_on_quota_violation=retry_on_quota_violation)

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

    def retry_on_quota_violation(self, *, retry_on_quota_violation: bool) -> DeleteTopicsOptions:
        """Set to true if quota violation should be automatically retried."""
        self._retry_on_quota_violation = retry_on_quota_violation
        self._record("retry_on_quota_violation", self._retry_on_quota_violation)
        return self

    def should_retry_on_quota_violation(self) -> bool:
        """Returns true if quota violation should be automatically retried."""
        return self._retry_on_quota_violation
