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

"""``DescribeTransactionsResult``: Java's
``org.apache.kafka.clients.admin.DescribeTransactionsResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). Its map is keyed by
Java's internal ``CoordinatorKey``, the private helper ``_CoordinatorKey``
(``_internals``); ``all()`` is ``allOf(...).thenApply(...)`` keyed by ``idValue``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.admin._internals import _CoordinatorKey
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    from confluent_kafka.admin.transaction_description import TransactionDescription
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeTransactionsResult"]


class DescribeTransactionsResult(Generic[KindT]):
    """The result of the ``Admin.describe_transactions()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeTransactionsResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DescribeTransactionsResult cannot be constructed directly; use the admin "
            "client's describe_transactions()")

    @classmethod
    def _new(
            cls, *,
            futures: Mapping[_CoordinatorKey, KafkaFuture[TransactionDescription] | AsyncKafkaFuture[TransactionDescription]],
            kind: type[Sync] | type[Async]) -> DescribeTransactionsResult[Any]:
        """Java's package-private ``DescribeTransactionsResult(futures)``."""
        result = cls.__new__(cls)
        result._futures = dict(futures)
        result._kind = kind
        return result

    def description(
            self, *,
            transactional_id: str) -> KafkaFuture[TransactionDescription] | AsyncKafkaFuture[TransactionDescription]:
        """Get the description of a specific transactional ID: a future which completes
        when the transaction description of a particular transactional ID is available.

        Raises ``IllegalArgumentError`` if the ``transactional_id`` was not included in
        the respective call to ``Admin.describe_transactions()``.
        """
        key = _CoordinatorKey.by_transactional_id(transactional_id=transactional_id)
        future = self._futures.get(key)
        if future is None:
            raise IllegalArgumentError(
                message="TransactionalId " + "`" + transactional_id
                + "` was not included in the request")
        return future

    def all(
            self) -> KafkaFuture[dict[str, TransactionDescription]] | AsyncKafkaFuture[dict[str, TransactionDescription]]:
        """Get a future which returns a map of the transaction descriptions requested in
        the respective call to ``Admin.describe_transactions()``.

        If the description fails on any of the transactional IDs in the request, then
        this future will also fail.
        """
        futures = self._futures

        def collect(_: None) -> dict[str, TransactionDescription]:
            # allOf ensured that all the futures completed successfully.
            return {key.id_value: future.get_now(value_if_absent=None) for key, future in futures.items()}

        return self._kind._FUTURE.all_of(futures=tuple(futures.values())).then_apply(function=collect)
