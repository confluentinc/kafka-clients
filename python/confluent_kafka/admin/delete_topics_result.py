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

"""``DeleteTopicsResult``: Java's ``org.apache.kafka.clients.admin.DeleteTopicsResult``.

Java's constructor is protected: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync, _all_of
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    from confluent_kafka.common import Uuid

__all__ = ["DeleteTopicsResult"]


class DeleteTopicsResult(Generic[KindT]):
    """The result of the ``Admin.delete_topics()`` call.

    Java: ``org.apache.kafka.clients.admin.DeleteTopicsResult``.
    """

    __slots__ = ("_topic_id_futures", "_name_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DeleteTopicsResult cannot be constructed directly; use the admin client's "
            "delete_topics()")

    @classmethod
    def _new(cls, *, topic_id_futures: Mapping[Uuid, AnyFuture[None]] | None,
             name_futures: Mapping[str, AnyFuture[None]] | None,
             kind: type[Sync] | type[Async]) -> DeleteTopicsResult[Any]:
        """Java's protected ``DeleteTopicsResult(topicIdFutures, nameFutures)``."""
        if topic_id_futures is not None and name_futures is not None:
            raise IllegalArgumentError(
                message="topicIdFutures and nameFutures cannot both be specified.")
        if topic_id_futures is None and name_futures is None:
            raise IllegalArgumentError(
                message="topicIdFutures and nameFutures cannot both be null.")
        result = cls.__new__(cls)
        result._topic_id_futures = None if topic_id_futures is None else dict(topic_id_futures)
        result._name_futures = None if name_futures is None else dict(name_futures)
        result._kind = kind
        return result

    @staticmethod
    def _of_topic_ids(*, topic_id_futures: Mapping[Uuid, AnyFuture[None]],
                      kind: type[Sync] | type[Async]) -> DeleteTopicsResult[Any]:
        """Java's package-private ``ofTopicIds(topicIdFutures)``."""
        return DeleteTopicsResult._new(topic_id_futures=topic_id_futures, name_futures=None,
                                       kind=kind)

    @staticmethod
    def _of_topic_names(*, name_futures: Mapping[str, AnyFuture[None]],
                        kind: type[Sync] | type[Async]) -> DeleteTopicsResult[Any]:
        """Java's package-private ``ofTopicNames(nameFutures)``."""
        return DeleteTopicsResult._new(topic_id_futures=None, name_futures=name_futures,
                                       kind=kind)

    def topic_id_values(self) -> dict[Uuid, AnyFuture[None]] | None:
        """Use when ``Admin.delete_topics()`` used a ``TopicIdCollection``: a map from
        topic IDs to futures which can be used to check the status of individual
        deletions if the request used topic IDs. Otherwise return ``None``."""
        return self._topic_id_futures

    def topic_name_values(self) -> dict[str, AnyFuture[None]] | None:
        """Use when ``Admin.delete_topics()`` used a ``TopicNameCollection``: a map from
        topic names to futures which can be used to check the status of individual
        deletions if the request used topic names. Otherwise return ``None``."""
        return self._name_futures

    def all(self) -> AnyFuture[None]:
        """A future which succeeds only if all the topic deletions succeed."""
        if self._topic_id_futures is None:
            assert self._name_futures is not None
            return _all_of(self._name_futures.values(), self._kind)
        return _all_of(self._topic_id_futures.values(), self._kind)
