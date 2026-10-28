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

"""``ListTopicsResult``: Java's ``org.apache.kafka.clients.admin.ListTopicsResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). One future; Java's
``listings()`` / ``names()`` are ``then_apply`` views of it (Futures). ``Map::values``
is no ``Set``, so ``listings()`` yields a ``list``; ``Map::keySet`` yields a ``set``
(Types).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync, _Result

if TYPE_CHECKING:
    from confluent_kafka.admin.topic_listing import TopicListing
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["ListTopicsResult"]


class ListTopicsResult(_Result, Generic[KindT]):
    """The result of the ``Admin.list_topics()`` call.

    Java: ``org.apache.kafka.clients.admin.ListTopicsResult``.
    """

    __slots__ = ("_future", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "ListTopicsResult cannot be constructed directly; use the admin client's "
            "list_topics()")

    @classmethod
    def _new(
            cls, *,
            future: KafkaFuture[dict[str, TopicListing]] | AsyncKafkaFuture[dict[str, TopicListing]],
            kind: type[Sync] | type[Async]) -> ListTopicsResult[Any]:
        """Java's package-private ``ListTopicsResult(future)``."""
        result = cls.__new__(cls)
        result._future = future
        result._kind = kind
        return result

    def names_to_listings(
            self) -> KafkaFuture[dict[str, TopicListing]] | AsyncKafkaFuture[dict[str, TopicListing]]:
        """Return a future which yields a map of topic names to TopicListing objects."""
        return self._future

    def listings(self) -> KafkaFuture[list[TopicListing]] | AsyncKafkaFuture[list[TopicListing]]:
        """Return a future which yields a collection of TopicListing objects."""
        return self._future.then_apply(function=lambda listings: list(listings.values()))

    def names(self) -> KafkaFuture[set[str]] | AsyncKafkaFuture[set[str]]:
        """Return a future which yields a collection of topic names."""
        return self._future.then_apply(function=lambda listings: set(listings.keys()))
