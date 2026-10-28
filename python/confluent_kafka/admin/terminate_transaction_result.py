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

"""``TerminateTransactionResult``: Java's
``org.apache.kafka.clients.admin.TerminateTransactionResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync, _Result

if TYPE_CHECKING:
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["TerminateTransactionResult"]


class TerminateTransactionResult(_Result, Generic[KindT]):
    """The result of the ``Admin.force_terminate_transaction()`` call.

    Java: ``org.apache.kafka.clients.admin.TerminateTransactionResult``.
    """

    __slots__ = ("_future", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "TerminateTransactionResult cannot be constructed directly; use the admin "
            "client's force_terminate_transaction()")

    @classmethod
    def _new(cls, *, future: KafkaFuture[None] | AsyncKafkaFuture[None],
             kind: type[Sync] | type[Async]) -> TerminateTransactionResult[Any]:
        """Java's package-private ``TerminateTransactionResult(future)``."""
        result = cls.__new__(cls)
        result._future = future
        result._kind = kind
        return result

    def result(self) -> KafkaFuture[None] | AsyncKafkaFuture[None]:
        """Return a future which indicates whether the transaction was successfully
        terminated."""
        return self._future
