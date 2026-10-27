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

"""``DescribeDelegationTokenResult``: Java's
``org.apache.kafka.clients.admin.DescribeDelegationTokenResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync

if TYPE_CHECKING:
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture
    from confluent_kafka.common.security.token.delegation import DelegationToken

__all__ = ["DescribeDelegationTokenResult"]


class DescribeDelegationTokenResult(Generic[KindT]):
    """The result of the ``Admin.describe_delegation_token()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeDelegationTokenResult``.
    """

    __slots__ = ("_delegation_tokens", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DescribeDelegationTokenResult cannot be constructed directly; use the admin "
            "client's describe_delegation_token()")

    @classmethod
    def _new(
            cls, *,
            delegation_tokens: KafkaFuture[list[DelegationToken]] | AsyncKafkaFuture[list[DelegationToken]],
            kind: type[Sync] | type[Async]) -> DescribeDelegationTokenResult[Any]:
        """Java's package-private ``DescribeDelegationTokenResult(delegationTokens)``."""
        result = cls.__new__(cls)
        result._delegation_tokens = delegation_tokens
        result._kind = kind
        return result

    def delegation_tokens(
            self) -> KafkaFuture[list[DelegationToken]] | AsyncKafkaFuture[list[DelegationToken]]:
        """Returns a future which yields list of delegation tokens"""
        return self._delegation_tokens
