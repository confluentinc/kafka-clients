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

"""``RemoveMembersFromConsumerGroupOptions``: Java's
``org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions``.

Java has a constructor that takes arguments, so the class keeps only Java's
constructors, ``(members)`` and ``()``, as one keyword-only constructor; every
combination is one of them, so there is no ``java_forms`` check (CLAUDE.md, Python
Binding Conventions, Signatures). Leaving ``members`` out is the ``()`` form, which
removes all members; ``members=None`` raises ``TypeError``, as Java's
``members.isEmpty()`` on a null. Java's setter ``reason(String)`` returns ``void``, so
``reason(reason=…)`` returns ``None``; it shares its name with the getter, and its
argument is nullable ("an optional reason"), so ``reason(reason=None)`` sets ``null``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from confluent_kafka._args import MISSING
from confluent_kafka.admin.abstract_options import AbstractOptions
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    from confluent_kafka.admin.member_to_remove import MemberToRemove

__all__ = ["RemoveMembersFromConsumerGroupOptions"]


class RemoveMembersFromConsumerGroupOptions(AbstractOptions):
    """Options for ``Admin.remove_members_from_consumer_group()``. It carries the
    members to be removed from the consumer group.

    Java: ``org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions``.
    """

    __slots__ = ("_members", "_reason")

    def __init__(self, *, members: Iterable[MemberToRemove] = MISSING) -> None:
        super().__init__()
        self._reason: str | None = None
        if members is MISSING:
            self._members: set[MemberToRemove] = set()  # Collections.emptySet()
            return
        if members is None:  # Java's members.isEmpty() on a null
            raise TypeError("members must not be None")
        copied = set(members)
        if not copied:
            raise IllegalArgumentError(message="Invalid empty members has been provided")
        self._members = copied

    def reason(self, *, reason: str | None = MISSING) -> Any:
        """With ``reason``: sets an optional reason (returns ``None``, as Java's
        ``void``). Without: the reason, or ``None``."""
        if reason is MISSING:
            return self._reason
        self._reason = reason
        self._record("reason", self._reason)
        return None

    def members(self) -> set[MemberToRemove]:
        return self._members

    def remove_all(self) -> bool:
        return not self._members
