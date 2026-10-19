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

"""``AlterClientQuotasOptions``: Java's ``org.apache.kafka.clients.admin.AlterClientQuotasOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter field,
the parent's ``timeout_ms`` first, then the class's own in Java's field order
(CLAUDE.md, Python Binding Conventions, Signatures). Each keyword given, ``None``
included, calls its setter; one left out leaves the field at Java's starting value. A
setter sharing its name with its getter is one method with two stubs: no argument reads,
one argument sets (Signatures).
"""

from __future__ import annotations

from typing import Any

from confluent_kafka._args import MISSING
from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["AlterClientQuotasOptions"]


class AlterClientQuotasOptions(AbstractOptions):
    """Options for ``Admin.alter_client_quotas()``.

    Java: ``org.apache.kafka.clients.admin.AlterClientQuotasOptions``.
    """

    __slots__ = ("_validate_only",)

    def __init__(self, *, timeout_ms: int | None = MISSING, validate_only: bool = MISSING) -> None:
        super().__init__()
        self._validate_only = False
        if timeout_ms is not MISSING:
            self.timeout_ms(timeout_ms=timeout_ms)
        if validate_only is not MISSING:
            self.validate_only(validate_only=validate_only)

    def validate_only(self, *, validate_only: bool = MISSING) -> Any:
        """With ``validate_only``: sets whether the request should be validated without
        altering the configs, and return this object.

        Without: returns whether the request should be validated without altering the
        configs.
        """
        if validate_only is MISSING:
            return self._validate_only
        if validate_only is None:  # a boolean: Java cannot pass null
            raise TypeError("validate_only must not be None")
        self._validate_only = validate_only
        self._record("validate_only", self._validate_only)
        return self
