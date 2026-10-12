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

"""``CreateTopicsResult``: Java's ``org.apache.kafka.clients.admin.CreateTopicsResult``.

Java's constructor is protected: ``__init__`` raises ``TypeError`` and ``_new`` builds
it (CLAUDE.md, Python Binding Conventions, Class family). Its futures carry Java's
public nested ``TopicMetadataAndConfig``, which no public signature reaches, so it is
the private helper ``_TopicMetadataAndConfig`` (decision log, entry 6); the public
methods are ``thenApply`` views of those futures (``_then_apply``). The
package-private constant ``UNKNOWN`` is ``_UNKNOWN``. Java's ``config`` /
``topicId`` / ``numPartitions`` / ``replicationFactor`` call ``thenApply`` on
``futures.get(topic)``, which throws ``NullPointerException`` for a topic that was
not part of the call; they raise ``NullPointerError`` the same way.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka._args import Form, java_forms
from confluent_kafka.admin._futures import (
    AnyFuture,
    Async,
    KindT,
    Sync,
    _all_of,
    _mark_retrieved,
    _then_apply,
)
from confluent_kafka.null_pointer_error import NullPointerError

if TYPE_CHECKING:
    from confluent_kafka.admin.config import Config
    from confluent_kafka.common import Uuid
    from confluent_kafka.common.errors import ApiError

__all__ = ["CreateTopicsResult"]

# Java's package-private ``static final int UNKNOWN = -1``.
_UNKNOWN = -1


class _TopicMetadataAndConfig:
    """Java's ``CreateTopicsResult.TopicMetadataAndConfig``: a topic's metadata and
    configs, or the error that replaced them (private: no public signature reaches
    it)."""

    __slots__ = ("_exception", "_topic_id", "_num_partitions", "_replication_factor",
                 "_config")

    @java_forms(
        Form("topic_id", "num_partitions", "replication_factor", "config"),
        Form("exception"),
        name="TopicMetadataAndConfig",
    )
    def __init__(self, *, topic_id: Uuid | None = None, num_partitions: int | None = None,
                 replication_factor: int | None = None, config: Config | None = None,
                 exception: ApiError | None = None) -> None:
        if exception is None:
            self._exception: ApiError | None = None
            self._topic_id = topic_id
            self._num_partitions = num_partitions
            self._replication_factor = replication_factor
            self._config = config
            return
        from confluent_kafka.common import Uuid

        self._exception = exception
        self._topic_id = Uuid.ZERO_UUID
        self._num_partitions = _UNKNOWN
        self._replication_factor = _UNKNOWN
        self._config = None

    def topic_id(self) -> Uuid | None:
        self._ensure_success()
        return self._topic_id

    def num_partitions(self) -> int | None:
        self._ensure_success()
        return self._num_partitions

    def replication_factor(self) -> int | None:
        self._ensure_success()
        return self._replication_factor

    def config(self) -> Config | None:
        self._ensure_success()
        return self._config

    def _ensure_success(self) -> None:
        if self._exception is not None:
            raise self._exception


class CreateTopicsResult(Generic[KindT]):
    """The result of ``Admin.create_topics()``.

    Java: ``org.apache.kafka.clients.admin.CreateTopicsResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "CreateTopicsResult cannot be constructed directly; use the admin client's "
            "create_topics()")

    @classmethod
    def _new(cls, *, futures: Mapping[str, AnyFuture[_TopicMetadataAndConfig]],
             kind: type[Sync] | type[Async]) -> CreateTopicsResult[Any]:
        """Java's protected ``CreateTopicsResult(futures)``."""
        result = cls.__new__(cls)
        result._futures = dict(futures)
        result._kind = kind
        return result

    def values(self) -> dict[str, AnyFuture[None]]:
        """Return a map from topic names to futures, which can be used to check the
        status of individual topic creations."""
        values: dict[str, AnyFuture[None]] = {}
        for topic, future in self._futures.items():
            derived: AnyFuture[None] = _then_apply(future, lambda v: None)
            # A topic the caller leaves unread does not log its error, as with the
            # client's own per-key futures.
            _mark_retrieved(derived)
            values[topic] = derived
        return values

    def all(self) -> AnyFuture[None]:
        """Return a future which succeeds if all the topic creations succeed."""
        return _all_of(self._futures.values(), self._kind)

    def config(self, *, topic: str) -> AnyFuture[Config]:
        """Returns a future that provides topic configs for the topic when the request
        completes.

        If broker version doesn't support replication factor in the response, it fails
        with ``UnsupportedVersionError``. If broker returned an error for topic configs,
        it fails with the appropriate error; for example ``TopicAuthorizationError`` if
        user does not have permission to describe topic configs. Note that the values
        for the type and documentation fields will be ``None``.
        """
        return _then_apply(self._future_of(topic), _TopicMetadataAndConfig.config)

    def topic_id(self, *, topic: str) -> AnyFuture[Uuid]:
        """Returns a future that provides topic ID for the topic when the request
        completes; it fails as ``config(topic=…)`` does."""
        return _then_apply(self._future_of(topic), _TopicMetadataAndConfig.topic_id)

    def num_partitions(self, *, topic: str) -> AnyFuture[int]:
        """Returns a future that provides number of partitions in the topic when the
        request completes; it fails as ``config(topic=…)`` does."""
        return _then_apply(self._future_of(topic), _TopicMetadataAndConfig.num_partitions)

    def replication_factor(self, *, topic: str) -> AnyFuture[int]:
        """Returns a future that provides replication factor for the topic when the
        request completes; it fails as ``config(topic=…)`` does."""
        return _then_apply(self._future_of(topic), _TopicMetadataAndConfig.replication_factor)

    def _future_of(self, topic: str) -> AnyFuture[_TopicMetadataAndConfig]:
        future = self._futures.get(topic)
        if future is None:
            # ``futures.get(topic).thenApply(...)`` on a topic not in the call.
            raise NullPointerError()
        return future
