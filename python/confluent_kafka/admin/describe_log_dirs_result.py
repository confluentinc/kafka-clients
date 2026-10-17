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

"""``DescribeLogDirsResult``: Java's ``org.apache.kafka.clients.admin.DescribeLogDirsResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). ``allDescriptions()``
is Java's ``allOf(...).thenApply(...)``, through the futures' ``all_of`` and
``then_apply``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync

if TYPE_CHECKING:
    from confluent_kafka.admin.log_dir_description import LogDirDescription
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeLogDirsResult"]


class DescribeLogDirsResult(Generic[KindT]):
    """The result of the ``Admin.describe_log_dirs()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeLogDirsResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DescribeLogDirsResult cannot be constructed directly; use the admin client's "
            "describe_log_dirs()")

    @classmethod
    def _new(
            cls, *,
            futures: Mapping[int, KafkaFuture[dict[str, LogDirDescription]] | AsyncKafkaFuture[dict[str, LogDirDescription]]],
            kind: type[Sync] | type[Async]) -> DescribeLogDirsResult[Any]:
        """Java's package-private ``DescribeLogDirsResult(futures)``."""
        result = cls.__new__(cls)
        result._futures = dict(futures)
        result._kind = kind
        return result

    def descriptions(
            self) -> Mapping[int, KafkaFuture[dict[str, LogDirDescription]] | AsyncKafkaFuture[dict[str, LogDirDescription]]]:
        """Return a map from brokerId to future which can be used to check the
        information of partitions on each individual broker. The result of the future is
        a map from broker log directory path to a description of that log directory."""
        return self._futures

    def all_descriptions(
            self) -> KafkaFuture[dict[int, dict[str, LogDirDescription]]] | AsyncKafkaFuture[dict[int, dict[str, LogDirDescription]]]:
        """Return a future which succeeds only if all the brokers have responded without
        error. The result of the future is a map from brokerId to a map from broker log
        directory path to a description of that log directory."""
        futures = self._futures

        def collect(_: None) -> dict[int, dict[str, LogDirDescription]]:
            # allOf ensured that all the futures completed successfully.
            return {broker_id: future.get_now(value_if_absent=None) for broker_id, future in futures.items()}

        return self._kind._FUTURE.all_of(futures=tuple(futures.values())).then_apply(function=collect)
