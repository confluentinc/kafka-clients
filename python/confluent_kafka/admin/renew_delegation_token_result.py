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

"""``RenewDelegationTokenResult``: Java's
``org.apache.kafka.clients.admin.RenewDelegationTokenResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync

if TYPE_CHECKING:
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["RenewDelegationTokenResult"]


class RenewDelegationTokenResult(Generic[KindT]):
    """The result of the ``Admin.renew_delegation_token()`` call.

    Java: ``org.apache.kafka.clients.admin.RenewDelegationTokenResult`` (whose Javadoc
    links ``expireDelegationToken`` by mistake).
    """

    __slots__ = ("_expiry_timestamp", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "RenewDelegationTokenResult cannot be constructed directly; use the admin "
            "client's renew_delegation_token()")

    @classmethod
    def _new(cls, *, expiry_timestamp: KafkaFuture[int] | AsyncKafkaFuture[int],
             kind: type[Sync] | type[Async]) -> RenewDelegationTokenResult[Any]:
        """Java's package-private ``RenewDelegationTokenResult(expiryTimestamp)``."""
        result = cls.__new__(cls)
        result._expiry_timestamp = expiry_timestamp
        result._kind = kind
        return result

    def expiry_timestamp(self) -> KafkaFuture[int] | AsyncKafkaFuture[int]:
        """Returns a future which yields expiry timestamp"""
        return self._expiry_timestamp
