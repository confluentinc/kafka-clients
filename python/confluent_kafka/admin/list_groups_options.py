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

"""``ListGroupsOptions``: Java's ``org.apache.kafka.clients.admin.ListGroupsOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter
field, named after the field, not the setter: ``group_states`` calls
``in_group_states``, ``types`` calls ``with_types``, ``protocol_types`` calls
``with_protocol_types`` (CLAUDE.md, Python Binding Conventions, Signatures). The
setters store a changed value: ``null`` or empty becomes ``Set.of()``, anything else a
``Set.copyOf`` copy, whose ``NullPointerException`` on a ``null`` element is
``TypeError("<argument> must not contain None")``.
Java's sets are immutable, so the getters return a new ``set`` each call. The static
factories are ``@staticmethod``\\ s.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, TypeVar

from confluent_kafka._args import MISSING, reject_str
from confluent_kafka.admin.abstract_options import AbstractOptions

if TYPE_CHECKING:
    from confluent_kafka.common import GroupState, GroupType

__all__ = ["ListGroupsOptions"]

_E = TypeVar("_E")

# ``ConsumerProtocol.PROTOCOL_TYPE`` (consumer/internals/ConsumerProtocol.java:45).
_CONSUMER_PROTOCOL_TYPE = "consumer"


def _copy_of(values: Iterable[_E] | None, name: str) -> frozenset[_E]:
    """``(values == null || values.isEmpty()) ? Set.of() : Set.copyOf(values)``;
    ``Set.copyOf`` throws on a null element (``TypeError`` naming ``name``)."""
    if values is None:
        return frozenset()
    copied = frozenset(values)
    if None in copied:
        raise TypeError(f"{name} must not contain None")
    return copied


class ListGroupsOptions(AbstractOptions):
    """Options for ``Admin.list_groups()``.

    Java: ``org.apache.kafka.clients.admin.ListGroupsOptions``.
    """

    __slots__ = ("_group_states", "_types", "_protocol_types")

    def __init__(self, *, timeout_ms: int | None = MISSING,
                 group_states: Iterable[GroupState] | None = MISSING,
                 types: Iterable[GroupType] | None = MISSING,
                 protocol_types: Iterable[str] | None = MISSING) -> None:
        super().__init__()
        self._group_states: frozenset[GroupState] = frozenset()
        self._types: frozenset[GroupType] = frozenset()
        self._protocol_types: frozenset[str] = frozenset()
        if timeout_ms is not MISSING:
            self.timeout_ms(timeout_ms=timeout_ms)
        if group_states is not MISSING:
            self.in_group_states(group_states=group_states)
        if types is not MISSING:
            self.with_types(types=types)
        if protocol_types is not MISSING:
            self.with_protocol_types(protocol_types=protocol_types)

    @staticmethod
    def for_consumer_groups() -> ListGroupsOptions:
        """Only consumer groups will be returned by ``list_groups()``.

        This operation sets filters on group type and protocol type which select
        consumer groups."""
        from confluent_kafka.common import GroupType

        return (ListGroupsOptions()
                .with_types(types={GroupType.CLASSIC, GroupType.CONSUMER})
                .with_protocol_types(protocol_types={"", _CONSUMER_PROTOCOL_TYPE}))

    @staticmethod
    def for_share_groups() -> ListGroupsOptions:
        """Only share groups will be returned by ``list_groups()``.

        This operation sets a filter on group type which select share groups."""
        from confluent_kafka.common import GroupType

        return ListGroupsOptions().with_types(types={GroupType.SHARE})

    @staticmethod
    def for_streams_groups() -> ListGroupsOptions:
        """Only streams groups will be returned by ``list_groups()``.

        This operation sets a filter on group type which select streams groups."""
        from confluent_kafka.common import GroupType

        return ListGroupsOptions().with_types(types={GroupType.STREAMS})

    def in_group_states(self, *, group_states: Iterable[GroupState] | None) -> ListGroupsOptions:
        """If group_states is set, only groups in these states will be returned by
        ``list_groups()``. Otherwise, all groups are returned.

        This operation is supported by brokers with version 2.6.0 or later."""
        self._group_states = _copy_of(group_states, "group_states")
        self._record("in_group_states", self._group_states)
        return self

    def with_protocol_types(self, *, protocol_types: Iterable[str] | None) -> ListGroupsOptions:
        """If protocol types is set, only groups of these protocol types will be
        returned by ``list_groups()``. Otherwise, all groups are returned."""
        if protocol_types is not None:
            reject_str("protocol_types", protocol_types)
        self._protocol_types = _copy_of(protocol_types, "protocol_types")
        self._record("with_protocol_types", self._protocol_types)
        return self

    def with_types(self, *, types: Iterable[GroupType] | None) -> ListGroupsOptions:
        """If types is set, only groups of these types will be returned by
        ``list_groups()``. Otherwise, all groups are returned."""
        self._types = _copy_of(types, "types")
        self._record("with_types", self._types)
        return self

    def group_states(self) -> set[GroupState]:
        """Returns the list of group states that are requested or empty if no states
        have been specified."""
        return set(self._group_states)

    def protocol_types(self) -> set[str]:
        """Returns the list of protocol types that are requested or empty if no protocol
        types have been specified."""
        return set(self._protocol_types)

    def types(self) -> set[GroupType]:
        """Returns the list of group types that are requested or empty if no types have
        been specified."""
        return set(self._types)
