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

"""``UpdateFeaturesResult``: Java's ``org.apache.kafka.clients.admin.UpdateFeaturesResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). ``all()`` is Java's
``KafkaFuture.allOf``, the ``all_of`` of the kind's future class (Futures).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync, _Result

if TYPE_CHECKING:
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["UpdateFeaturesResult"]


class UpdateFeaturesResult(_Result, Generic[KindT]):
    """The result of the ``Admin.update_features()`` call.

    Java: ``org.apache.kafka.clients.admin.UpdateFeaturesResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "UpdateFeaturesResult cannot be constructed directly; use the admin client's "
            "update_features()")

    @classmethod
    def _new(cls, *, futures: Mapping[str, KafkaFuture[None] | AsyncKafkaFuture[None]],
             kind: type[Sync] | type[Async]) -> UpdateFeaturesResult[Any]:
        """Java's package-private ``UpdateFeaturesResult(futures)``.

        :param futures: a map from feature name to future, which can be used to check
        the status of individual feature updates.
        """
        result = cls.__new__(cls)
        result._futures = None if futures is None else dict(futures)
        result._kind = kind
        return result

    def values(self) -> Mapping[str, KafkaFuture[None] | AsyncKafkaFuture[None]]:
        return self._futures

    def all(self) -> KafkaFuture[None] | AsyncKafkaFuture[None]:
        """Return a future which succeeds if all the feature updates succeed."""
        return self._kind._FUTURE.all_of(futures=tuple(self._futures.values()))
