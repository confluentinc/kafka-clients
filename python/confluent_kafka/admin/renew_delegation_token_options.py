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

"""``RenewDelegationTokenOptions``: Java's
``org.apache.kafka.clients.admin.RenewDelegationTokenOptions``.

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

__all__ = ["RenewDelegationTokenOptions"]


class RenewDelegationTokenOptions(AbstractOptions):
    """Options for ``Admin.renew_delegation_token()``.

    Java: ``org.apache.kafka.clients.admin.RenewDelegationTokenOptions``.
    """

    __slots__ = ("_renew_time_period_ms",)

    def __init__(self, *, timeout_ms: int | None = MISSING,
                 renew_time_period_ms: int = MISSING) -> None:
        super().__init__()
        self._renew_time_period_ms = -1
        if timeout_ms is not MISSING:
            self.timeout_ms(timeout_ms=timeout_ms)
        if renew_time_period_ms is not MISSING:
            self.renew_time_period_ms(renew_time_period_ms=renew_time_period_ms)

    def renew_time_period_ms(self, *, renew_time_period_ms: int = MISSING) -> Any:
        """With ``renew_time_period_ms``: set it and return this object.

        Without: the current value.
        """
        if renew_time_period_ms is MISSING:
            return self._renew_time_period_ms
        if renew_time_period_ms is None:  # a long: Java cannot pass null
            raise TypeError("renew_time_period_ms must not be None")
        self._renew_time_period_ms = renew_time_period_ms
        self._record("renew_time_period_ms", self._renew_time_period_ms)
        return self
