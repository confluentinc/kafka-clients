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

"""``TopicCollection``: Java's ``org.apache.kafka.common.TopicCollection``.

Java's class is ``abstract``, so constructing it raises ``TypeError`` naming its
static factories (CLAUDE.md, Python Binding Conventions, Class family). Its two
nested subclasses, ``TopicCollection.TopicIdCollection`` and
``TopicCollection.TopicNameCollection``, have only private constructors and no
setters, so their ``__init__`` raises ``TypeError`` naming the factory that
returns them, and Java's private constructor is the private class method
``_new``. Neither overrides ``equals`` / ``hashCode`` / ``toString``, so both
keep identity equality, as in Java.

A Python class body cannot name a subclass of the class it defines, so the two
nested classes are defined after ``TopicCollection`` and attached to it under
their Java names (``__qualname__`` included, so they pickle and print as
``TopicCollection.TopicIdCollection``); a type checker sees them as nested
types through class-level aliases.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from confluent_kafka.common.uuid import Uuid

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

__all__ = ["TopicCollection"]


class TopicCollection:
    """A class used to represent a collection of topics. This collection may
    define topics by name or ID.

    Java: ``org.apache.kafka.common.TopicCollection``.
    """

    __slots__ = ()

    if TYPE_CHECKING:
        TopicIdCollection: TypeAlias = "_TopicIdCollection"
        TopicNameCollection: TypeAlias = "_TopicNameCollection"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "TopicCollection cannot be constructed directly; use "
            "TopicCollection.of_topic_ids(...) or TopicCollection.of_topic_names(...)")

    @staticmethod
    def of_topic_ids(*, topics: Iterable[Uuid]) -> TopicCollection.TopicIdCollection:
        """A collection of topics defined by topic ID."""
        return _TopicIdCollection._new(topic_ids=topics)

    @staticmethod
    def of_topic_names(*, topics: Iterable[str]) -> TopicCollection.TopicNameCollection:
        """A collection of topics defined by topic name."""
        return _TopicNameCollection._new(topic_names=topics)


class _TopicIdCollection(TopicCollection):
    """A class used to represent a collection of topics defined by their topic
    ID. Subclassing this class beyond the classes provided here is not
    supported.

    Java: ``org.apache.kafka.common.TopicCollection.TopicIdCollection``.
    """

    __slots__ = ("_topic_ids",)

    _topic_ids: tuple[Uuid, ...]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "TopicCollection.TopicIdCollection cannot be constructed directly; use "
            "TopicCollection.of_topic_ids(...)")

    @classmethod
    def _new(cls, *, topic_ids: Iterable[Uuid]) -> _TopicIdCollection:
        """Java's private ``TopicIdCollection(Collection<Uuid>)``: a copy of
        the ids."""
        collection = cls.__new__(cls)
        collection._topic_ids = tuple(topic_ids)
        return collection

    def topic_ids(self) -> list[Uuid]:
        """A collection of topic IDs."""
        return list(self._topic_ids)


class _TopicNameCollection(TopicCollection):
    """A class used to represent a collection of topics defined by their topic
    name. Subclassing this class beyond the classes provided here is not
    supported.

    Java: ``org.apache.kafka.common.TopicCollection.TopicNameCollection``.
    """

    __slots__ = ("_topic_names",)

    _topic_names: tuple[str, ...]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "TopicCollection.TopicNameCollection cannot be constructed directly; use "
            "TopicCollection.of_topic_names(...)")

    @classmethod
    def _new(cls, *, topic_names: Iterable[str]) -> _TopicNameCollection:
        """Java's private ``TopicNameCollection(Collection<String>)``: a copy of
        the names."""
        collection = cls.__new__(cls)
        collection._topic_names = tuple(topic_names)
        return collection

    def topic_names(self) -> list[str]:
        """A collection of topic names."""
        return list(self._topic_names)


for _nested, _name in ((_TopicIdCollection, "TopicIdCollection"),
                       (_TopicNameCollection, "TopicNameCollection")):
    _nested.__name__ = _name
    _nested.__qualname__ = "TopicCollection." + _name
    setattr(TopicCollection, _name, _nested)
del _nested, _name
