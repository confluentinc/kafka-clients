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

"""``DescribeFeaturesResult``: Java's ``org.apache.kafka.clients.admin.DescribeFeaturesResult``.

Java's constructor is protected: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync, _Result

if TYPE_CHECKING:
    from confluent_kafka.admin.feature_metadata import FeatureMetadata
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeFeaturesResult"]


class DescribeFeaturesResult(_Result, Generic[KindT]):
    """The result of the ``Admin.describe_features()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeFeaturesResult``.
    """

    __slots__ = ("_future", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DescribeFeaturesResult cannot be constructed directly; use the admin client's "
            "describe_features()")

    @classmethod
    def _new(cls, *, future: KafkaFuture[FeatureMetadata] | AsyncKafkaFuture[FeatureMetadata],
             kind: type[Sync] | type[Async]) -> DescribeFeaturesResult[Any]:
        """Java's protected ``DescribeFeaturesResult(future)``."""
        result = cls.__new__(cls)
        result._future = future
        result._kind = kind
        return result

    def feature_metadata(self) -> KafkaFuture[FeatureMetadata] | AsyncKafkaFuture[FeatureMetadata]:
        return self._future
