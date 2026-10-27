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

"""``ClientQuotaFilterComponent``: Java's
``org.apache.kafka.common.quota.ClientQuotaFilterComponent``.

In ``confluent_kafka.common.quota``, the module of its Java package. Java's
only constructor is private and the class has no setters, so ``__init__``
raises ``TypeError`` naming the static factories, and the private constructor
is the class method ``_new`` (CLAUDE.md, Python Binding Conventions, Class
family).

Java's ``match`` field is an ``Optional<String>`` that can itself be ``null``:
present (``of_entity``: match that name), empty (``of_default_entity``: match
the default entity) or ``null`` (``of_entity_type``: match any name). The Types
rule maps both ``Optional.empty()`` and ``null`` to ``None``, so ``match()``
returns ``None`` for both; the object keeps the three states apart for its
``__eq__``, ``__hash__`` and ``__str__`` (Java's ``match=Optional.empty`` /
``match=null``), and the client reads the private ``_match_is_null`` to send
the right one.
"""

from __future__ import annotations

from typing import Any


__all__ = ["ClientQuotaFilterComponent"]


class ClientQuotaFilterComponent:
    """Describes a component for applying a client quota filter.

    Java: ``org.apache.kafka.common.quota.ClientQuotaFilterComponent``.
    """

    __slots__ = ("_entity_type", "_match", "_match_is_null")

    _entity_type: str
    _match: str | None
    _match_is_null: bool

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "ClientQuotaFilterComponent cannot be constructed directly; use "
            "ClientQuotaFilterComponent.of_entity(...), "
            "ClientQuotaFilterComponent.of_default_entity(...) or "
            "ClientQuotaFilterComponent.of_entity_type(...)")

    @classmethod
    def _new(cls, *, entity_type: str, match: str | None,
             match_is_null: bool) -> ClientQuotaFilterComponent:
        """Java's private ``ClientQuotaFilterComponent(entityType, match)``:
        ``match_is_null`` is Java's ``null`` ``match``; otherwise ``match`` is
        the ``Optional``'s value, ``None`` for ``Optional.empty()``."""
        if entity_type is None:
            raise TypeError("entity_type must not be None")
        component = cls.__new__(cls)
        component._entity_type = entity_type
        component._match = match
        component._match_is_null = match_is_null
        return component

    @staticmethod
    def of_entity(*, entity_type: str, entity_name: str) -> ClientQuotaFilterComponent:
        """Constructs and returns a filter component that exactly matches the
        provided entity name for the entity type."""
        if entity_name is None:
            raise TypeError("entity_name must not be None")
        return ClientQuotaFilterComponent._new(entity_type=entity_type, match=entity_name,
                                               match_is_null=False)

    @staticmethod
    def of_default_entity(*, entity_type: str) -> ClientQuotaFilterComponent:
        """Constructs and returns a filter component that matches the built-in
        default entity name for the entity type."""
        return ClientQuotaFilterComponent._new(entity_type=entity_type, match=None,
                                               match_is_null=False)

    @staticmethod
    def of_entity_type(*, entity_type: str) -> ClientQuotaFilterComponent:
        """Constructs and returns a filter component that matches any specified
        name for the entity type."""
        return ClientQuotaFilterComponent._new(entity_type=entity_type, match=None,
                                               match_is_null=True)

    def entity_type(self) -> str:
        """The component's entity type."""
        return self._entity_type

    def match(self) -> str | None:
        """The match string: if present, the name that's matched exactly;
        ``None`` when the component matches the default name
        (``of_default_entity``) or any specified name (``of_entity_type``)."""
        return self._match

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ClientQuotaFilterComponent)
        return (other._entity_type == self._entity_type and other._match == self._match
                and other._match_is_null == self._match_is_null)

    def __hash__(self) -> int:
        return hash((self._entity_type, self._match, self._match_is_null))

    def __str__(self) -> str:
        if self._match_is_null:
            match = "None"
        elif self._match is None:
            match = "Optional.empty"
        else:
            match = "Optional[" + self._match + "]"
        return ("ClientQuotaFilterComponent(entityType=" + self._entity_type + ", match="
                + match + ")")
