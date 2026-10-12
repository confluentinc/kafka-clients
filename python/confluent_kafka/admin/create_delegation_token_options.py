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
the setter's argument is nullable, and, as it shares its name with the getter,
defaults to ``_args.UNSET`` so that ``owner(owner=None)`` sets ``null``.
``renewers`` is not nullable (it starts as ``new LinkedList<>()``, Java has no null branch
or Javadoc for it, and the admin client iterates it), so ``renewers()`` with no argument
reads and ``None`` is "not given" (Signatures, Nullability).

Not generated (deprecated): ``maxlifeTimeMs(long)``, ``maxlifeTimeMs()``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from confluent_kafka._args import UNSET
from confluent_kafka.admin.abstract_options import AbstractOptions

if TYPE_CHECKING:
    from confluent_kafka.common import KafkaPrincipal

__all__ = ["CreateDelegationTokenOptions"]


class CreateDelegationTokenOptions(AbstractOptions):
    """Options for ``Admin.create_delegation_token()``.

    Java: ``org.apache.kafka.clients.admin.CreateDelegationTokenOptions``.
    """

    __slots__ = ("_max_lifetime_ms", "_renewers", "_owner")

    def __init__(self, *, timeout_ms: int | None = UNSET, max_lifetime_ms: int = UNSET,
                 renewers: Sequence[KafkaPrincipal] = UNSET,
                 owner: KafkaPrincipal | None = UNSET) -> None:
        super().__init__()
        self._max_lifetime_ms = -1
        self._renewers: list[KafkaPrincipal] = []
        self._owner: KafkaPrincipal | None = None
        if timeout_ms is not UNSET:
            self.timeout_ms(timeout_ms=timeout_ms)
        if max_lifetime_ms is not UNSET:
            self.max_lifetime_ms(max_lifetime_ms=max_lifetime_ms)
        if renewers is not UNSET:
            self.renewers(renewers=renewers)
        if owner is not UNSET:
            self.owner(owner=owner)

    def renewers(self, *, renewers: Sequence[KafkaPrincipal] | None = None) -> Any:
        """With ``renewers``: set the renewers and return this object. Without: the
        renewers."""
        if renewers is None:
            return self._renewers
        self._renewers = list(renewers)
        return self

    def owner(self, *, owner: KafkaPrincipal | None = UNSET) -> Any:
        """With ``owner``: set the owner (``None`` for none) and return this object.
        Without: the owner, or ``None``."""
        if owner is UNSET:
            return self._owner  # Optional.ofNullable(owner)
        self._owner = owner
        return self

    def max_lifetime_ms(self, *, max_lifetime_ms: int | None = None) -> Any:
        """With ``max_lifetime_ms``: set the maximum lifetime in milliseconds and return
        this object. Without: the maximum lifetime in milliseconds."""
        if max_lifetime_ms is None:
            return self._max_lifetime_ms
        self._max_lifetime_ms = max_lifetime_ms
        return self
