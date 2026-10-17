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

"""Private helpers for the admin value classes: Java's ``toString`` of the
``Optional`` family, a hashable view of a collection, and the one name that is a
static factory on the class and a getter on an instance."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Generic, TypeVar, overload

from confluent_kafka._java import java_str

__all__ = ["FactoryOrGetter", "frozen", "optional_str"]

_F = TypeVar("_F", bound=Callable[..., Any])
_G = TypeVar("_G", bound=Callable[..., Any])


def optional_str(value: object, kind: str = "Optional") -> str:
    """Java's ``Optional.toString()`` (``OptionalInt`` / ``OptionalLong`` with
    ``kind``): ``Optional[v]`` when present, ``Optional.empty`` for ``None``."""
    if value is None:
        return kind + ".empty"
    return kind + "[" + java_str(value) + "]"


def frozen(value: object) -> object:
    """A hashable view of ``value`` for ``__hash__``: lists become tuples,
    sets frozensets and mappings frozensets of their items, recursively."""
    if isinstance(value, Mapping):
        return frozenset((frozen(k), frozen(v)) for k, v in value.items())
    if isinstance(value, (set, frozenset)):
        return frozenset(frozen(v) for v in value)
    if isinstance(value, (list, tuple)):
        return tuple(frozen(v) for v in value)
    return value


class FactoryOrGetter(Generic[_F, _G]):
    """One name that is a static factory on the class and a getter on an
    instance (CLAUDE.md, Python Binding Conventions, Signatures)."""

    def __init__(self, factory: _F, getter: Callable[[Any], Any]) -> None:
        self._factory = factory
        self._getter = getter

    @overload
    def __get__(self, obj: None, owner: type) -> _F: ...
    @overload
    def __get__(self, obj: object, owner: type | None = None) -> _G: ...

    def __get__(self, obj: object | None, owner: type | None = None) -> Any:
        if obj is None:
            return self._factory
        getter = self._getter
        return lambda: getter(obj)
