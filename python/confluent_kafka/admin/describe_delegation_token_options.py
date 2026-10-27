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

"""``DescribeDelegationTokenOptions``: Java's
``org.apache.kafka.clients.admin.DescribeDelegationTokenOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter field,
the parent's ``timeout_ms`` first, then the class's own in Java's field order
(CLAUDE.md, Python Binding Conventions, Signatures). Each keyword given, ``None``
included, calls its setter; one left out leaves the field at Java's starting value. A
setter sharing its name with its getter is one method with two stubs: no argument reads,
one argument sets (Signatures).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from confluent_kafka._args import MISSING
from confluent_kafka.admin.abstract_options import AbstractOptions

if TYPE_CHECKING:
    from confluent_kafka.common.security.auth import KafkaPrincipal

__all__ = ["DescribeDelegationTokenOptions"]


class DescribeDelegationTokenOptions(AbstractOptions):
    """Options for ``Admin.describe_delegation_token()``.

    Java: ``org.apache.kafka.clients.admin.DescribeDelegationTokenOptions``.
    """

    __slots__ = ("_owners",)

    def __init__(self, *, timeout_ms: int | None = MISSING,
                 owners: Sequence[KafkaPrincipal] | None = MISSING) -> None:
        super().__init__()
        self._owners: list[KafkaPrincipal] | None = None
        if timeout_ms is not MISSING:
            self.timeout_ms(timeout_ms=timeout_ms)
        if owners is not MISSING:
            self.owners(owners=owners)

    def owners(self, *, owners: Sequence[KafkaPrincipal] | None = MISSING) -> Any:
        """With ``owners``: if owners is null, all the user owned tokens and tokens
        where user have Describe permission will be returned, and return this object.

        Without: the current value.

        :param owners: The owners that we want to describe delegation tokens for
        :return: this instance
        """
        if owners is MISSING:
            return self._owners
        self._owners = None if owners is None else list(owners)
        self._record("owners", self._owners)
        return self
