# Copyright 2026 Confluent Inc.
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

"""``ClientQuotaFilter``: Java's ``org.apache.kafka.common.quota.ClientQuotaFilter``.

In ``confluent_kafka.common.quota``, the module of its Java package. Java's
only constructor is private and the class has no setters, so ``__init__``
raises ``TypeError`` naming the static factories, and the private constructor
is the class method ``_new`` (CLAUDE.md, Python Binding Conventions, Class
family).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from confluent_kafka._java import java_str
from confluent_kafka.common.quota.client_quota_filter_component import ClientQuotaFilterComponent

__all__ = ["ClientQuotaFilter"]


class ClientQuotaFilter:
    """Describes a client quota entity filter.

    Java: ``org.apache.kafka.common.quota.ClientQuotaFilter``.
    """

    __slots__ = ("_components", "_strict")

    _components: tuple[ClientQuotaFilterComponent, ...]
    _strict: bool

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "ClientQuotaFilter cannot be constructed directly; use "
            "ClientQuotaFilter.contains(...), ClientQuotaFilter.contains_only(...) or "
            "ClientQuotaFilter.all()")

    @classmethod
    def _new(cls, *, components: Iterable[ClientQuotaFilterComponent],
             strict: bool) -> ClientQuotaFilter:
        """Java's private ``ClientQuotaFilter(components, strict)``: a filter to
        be applied to matching client quotas; ``strict`` is whether the filter
        only includes specified components."""
        quota_filter = cls.__new__(cls)
        quota_filter._components = tuple(components)
        quota_filter._strict = strict
        return quota_filter

    @staticmethod
    def contains(*, components: Iterable[ClientQuotaFilterComponent]) -> ClientQuotaFilter:
        """Constructs and returns a quota filter that matches all provided
        components. Matching entities with entity types that are not specified
        by a component will also be included in the result."""
        return ClientQuotaFilter._new(components=components, strict=False)

    @staticmethod
    def contains_only(*, components: Iterable[ClientQuotaFilterComponent]) -> ClientQuotaFilter:
        """Constructs and returns a quota filter that matches all provided
        components. Matching entities with entity types that are not specified
        by a component will *not* be included in the result."""
        return ClientQuotaFilter._new(components=components, strict=True)

    @staticmethod
    def all() -> ClientQuotaFilter:
        """Constructs and returns a quota filter that matches all configured
        entities."""
        return ClientQuotaFilter._new(components=(), strict=False)

    def components(self) -> list[ClientQuotaFilterComponent]:
        """The filter's components."""
        return list(self._components)

    def strict(self) -> bool:
        """Whether the filter is strict, i.e. only includes specified
        components."""
        return self._strict

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ClientQuotaFilter)
        return self._components == other._components and self._strict == other._strict

    def __hash__(self) -> int:
        return hash((self._components, self._strict))

    def __str__(self) -> str:
        return ("ClientQuotaFilter(components=" + java_str(list(self._components)) + ", strict="
                + java_str(self._strict) + ")")
