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

"""``DescribeConfigsResult``: Java's ``org.apache.kafka.clients.admin.DescribeConfigsResult``.

Java's constructor is protected: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). ``all()`` is Java's
``allOf(...).thenApply(...)``, through the futures' ``all_of`` and ``then_apply``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync

if TYPE_CHECKING:
    from confluent_kafka.admin.config import Config
    from confluent_kafka.common.config import ConfigResource
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeConfigsResult"]


class DescribeConfigsResult(Generic[KindT]):
    """The result of the ``Admin.describe_configs()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeConfigsResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DescribeConfigsResult cannot be constructed directly; use the admin client's "
            "describe_configs()")

    @classmethod
    def _new(cls, *,
             futures: Mapping[ConfigResource, KafkaFuture[Config] | AsyncKafkaFuture[Config]],
             kind: type[Sync] | type[Async]) -> DescribeConfigsResult[Any]:
        """Java's protected ``DescribeConfigsResult(futures)``."""
        result = cls.__new__(cls)
        result._futures = dict(futures)
        result._kind = kind
        return result

    def values(self) -> Mapping[ConfigResource, KafkaFuture[Config] | AsyncKafkaFuture[Config]]:
        """Return a map from resources to futures which can be used to check the status
        of the configuration for each resource."""
        return self._futures

    def all(
            self) -> KafkaFuture[dict[ConfigResource, Config]] | AsyncKafkaFuture[dict[ConfigResource, Config]]:
        """Return a future which succeeds only if all the config descriptions
        succeed."""
        futures = self._futures

        def collect(_: None) -> dict[ConfigResource, Config]:
            # allOf ensured that all the futures completed successfully.
            return {resource: future.get_now(value_if_absent=None) for resource, future in futures.items()}

        return self._kind._FUTURE.all_of(futures=tuple(futures.values())).then_apply(function=collect)
