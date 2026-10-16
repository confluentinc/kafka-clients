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

"""``DescribeTopicsResult``: Java's ``org.apache.kafka.clients.admin.DescribeTopicsResult``.

Hand-written: outside every generator pattern (``_reference_set.md``, Coverage), as it
has a private generic static ``all(Map<T, …>)`` and ``allTopicNames()`` /
``allTopicIds()`` return ``null`` instead of a future. Java's constructor is protected
and its static factories ``ofTopicIds`` / ``ofTopicNames`` package-private, so
``__init__`` raises ``TypeError`` naming the method that returns it, the constructor is
``_new`` and the factories the private static methods ``_of_topic_ids`` /
``_of_topic_names`` (CLAUDE.md, Python Binding Conventions, Class family), as
``DeleteTopicsResult``. The private static ``all`` is ``_all``, given the kind for an
empty map.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    from confluent_kafka.admin.topic_description import TopicDescription
    from confluent_kafka.common import Uuid
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeTopicsResult"]

_T = TypeVar("_T")


class DescribeTopicsResult(Generic[KindT]):
    """The result of the ``Admin.describe_topics()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeTopicsResult``.
    """

    __slots__ = ("_topic_id_futures", "_name_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DescribeTopicsResult cannot be constructed directly; use the admin client's "
            "describe_topics()")

    @classmethod
    def _new(cls, *, topic_id_futures: Mapping[Uuid, KafkaFuture[TopicDescription] | AsyncKafkaFuture[TopicDescription]] | None,
             name_futures: Mapping[str, KafkaFuture[TopicDescription] | AsyncKafkaFuture[TopicDescription]] | None,
             kind: type[Sync] | type[Async]) -> DescribeTopicsResult[Any]:
        """Java's protected ``DescribeTopicsResult(topicIdFutures, nameFutures)``."""
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
    def _of_topic_ids(*, topic_id_futures: Mapping[Uuid, KafkaFuture[TopicDescription] | AsyncKafkaFuture[TopicDescription]],
                      kind: type[Sync] | type[Async]) -> DescribeTopicsResult[Any]:
        """Java's package-private ``ofTopicIds(topicIdFutures)``."""
        return DescribeTopicsResult._new(topic_id_futures=topic_id_futures, name_futures=None,
                                         kind=kind)

    @staticmethod
    def _of_topic_names(*, name_futures: Mapping[str, KafkaFuture[TopicDescription] | AsyncKafkaFuture[TopicDescription]],
                        kind: type[Sync] | type[Async]) -> DescribeTopicsResult[Any]:
        """Java's package-private ``ofTopicNames(nameFutures)``."""
        return DescribeTopicsResult._new(topic_id_futures=None, name_futures=name_futures,
                                         kind=kind)

    def topic_id_values(self) -> dict[Uuid, KafkaFuture[TopicDescription] | AsyncKafkaFuture[TopicDescription]] | None:
        """Use when ``Admin.describe_topics()`` used a ``TopicIdCollection``: a map from
        topic IDs to futures which can be used to check the status of individual topics
        if the request used topic IDs, otherwise return ``None``."""
        return self._topic_id_futures

    def topic_name_values(self) -> dict[str, KafkaFuture[TopicDescription] | AsyncKafkaFuture[TopicDescription]] | None:
        """Use when ``Admin.describe_topics()`` used a ``TopicNameCollection``: a map from
        topic names to futures which can be used to check the status of individual topics
        if the request used topic names, otherwise return ``None``."""
        return self._name_futures

    def all_topic_names(self) -> KafkaFuture[dict[str, TopicDescription]] | AsyncKafkaFuture[dict[str, TopicDescription]] | None:
        """A future map from topic names to descriptions which can be used to check the
        status of individual description if the describe topic request used topic names,
        otherwise return ``None``; this request succeeds only if all the topic
        descriptions succeed."""
        return DescribeTopicsResult._all(self._name_futures, self._kind)

    def all_topic_ids(self) -> KafkaFuture[dict[Uuid, TopicDescription]] | AsyncKafkaFuture[dict[Uuid, TopicDescription]] | None:
        """A future map from topic ids to descriptions which can be used to check the
        status of individual description if the describe topic request used topic ids,
        otherwise return ``None``; this request succeeds only if all the topic
        descriptions succeed."""
        return DescribeTopicsResult._all(self._topic_id_futures, self._kind)

    @staticmethod
    def _all(futures: Mapping[_T, KafkaFuture[TopicDescription] | AsyncKafkaFuture[TopicDescription]] | None,
             kind: type[Sync] | type[Async]) -> KafkaFuture[dict[_T, TopicDescription]] | AsyncKafkaFuture[dict[_T, TopicDescription]] | None:
        """Java's private static ``all(futures)``: a future which succeeds only if all
        the topic descriptions succeed."""
        if futures is None:
            return None
        future = kind._FUTURE.all_of(futures=tuple(futures.values()))

        def collect(_: None) -> dict[_T, TopicDescription]:
            # allOf ensured that all the futures completed successfully.
            return {key: value.get_now(value_if_absent=None) for key, value in futures.items()}

        return future.then_apply(function=collect)
