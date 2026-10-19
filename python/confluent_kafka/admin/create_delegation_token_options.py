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

"""``CreateDelegationTokenOptions``: Java's
``org.apache.kafka.clients.admin.CreateDelegationTokenOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter
field (CLAUDE.md, Python Binding Conventions, Signatures). Two Java setters write
``maxLifetimeMs``: ``maxLifetimeMs(long)`` and the ``@Deprecated``
``maxlifeTimeMs(long)``; the keyword ``max_lifetime_ms`` calls the one named like the
field, and the deprecated pair (setter and getter) is not generated (Class family).
``owner()`` returns ``Optional.ofNullable(owner)``, so the field can hold ``null``:
the setter's argument is nullable, so ``owner(owner=None)`` sets ``null``.
``renewers`` is not nullable (it starts as ``new LinkedList<>()``, Java has no null branch
or Javadoc for it), so ``renewers()`` with no argument reads. Java's setter stores its
argument as given, so ``renewers(renewers=None)`` stores ``None``, as Java stores ``null``;
``create_delegation_token`` raises ``TypeError("CreateDelegationTokenOptions.renewers must
not be None")`` where Java iterates it (``KafkaAdminClient.createDelegationToken``,
``MockAdminClient.createDelegationToken``) (Signatures, Nullability).

Not generated (deprecated): ``maxlifeTimeMs(long)``, ``maxlifeTimeMs()``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from confluent_kafka._args import MISSING
from confluent_kafka.admin.abstract_options import AbstractOptions

if TYPE_CHECKING:
    from confluent_kafka.common import KafkaPrincipal

__all__ = ["CreateDelegationTokenOptions"]


class CreateDelegationTokenOptions(AbstractOptions):
    """Options for ``Admin.create_delegation_token()``.

    Java: ``org.apache.kafka.clients.admin.CreateDelegationTokenOptions``.
    """

    __slots__ = ("_max_lifetime_ms", "_renewers", "_owner")

    def __init__(self, *, timeout_ms: int | None = MISSING, max_lifetime_ms: int = MISSING,
                 renewers: Sequence[KafkaPrincipal] = MISSING,
                 owner: KafkaPrincipal | None = MISSING) -> None:
        super().__init__()
        self._max_lifetime_ms = -1
        self._renewers: list[KafkaPrincipal] | None = []
        self._owner: KafkaPrincipal | None = None
        if timeout_ms is not MISSING:
            self.timeout_ms(timeout_ms=timeout_ms)
        if max_lifetime_ms is not MISSING:
            self.max_lifetime_ms(max_lifetime_ms=max_lifetime_ms)
        if renewers is not MISSING:
            self.renewers(renewers=renewers)
        if owner is not MISSING:
            self.owner(owner=owner)

    def renewers(self, *, renewers: Sequence[KafkaPrincipal] = MISSING) -> Any:
        """With ``renewers``: set the renewers and return this object. Without: the
        renewers."""
        if renewers is MISSING:
            return self._renewers
        self._renewers = None if renewers is None else list(renewers)
        self._record("renewers", self._renewers)
        return self

    def owner(self, *, owner: KafkaPrincipal | None = MISSING) -> Any:
        """With ``owner``: set the owner (``None`` for none) and return this object.
        Without: the owner, or ``None``."""
        if owner is MISSING:
            return self._owner  # Optional.ofNullable(owner)
        self._owner = owner
        self._record("owner", self._owner)
        return self

    def max_lifetime_ms(self, *, max_lifetime_ms: int = MISSING) -> Any:
        """With ``max_lifetime_ms``: set the maximum lifetime in milliseconds and return
        this object. Without: the maximum lifetime in milliseconds."""
        if max_lifetime_ms is MISSING:
            return self._max_lifetime_ms
        if max_lifetime_ms is None:  # a long: Java cannot pass null
            raise TypeError("max_lifetime_ms must not be None")
        self._max_lifetime_ms = max_lifetime_ms
        self._record("max_lifetime_ms", self._max_lifetime_ms)
        return self
