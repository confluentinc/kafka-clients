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

"""``AlterUserScramCredentialsResult``: Java's
``org.apache.kafka.clients.admin.AlterUserScramCredentialsResult``.

Hand-written: outside every generator pattern (``_reference_set.md``, Coverage), as the
constructor body is ``Collections.unmodifiableMap(Objects.requireNonNull(futures))``.
Java's constructor is public: it is ``__init__(*, futures)``, the kind read from the
futures, plus the clients' ``_new`` (rule interpretation 2). ``requireNonNull`` raises
``TypeError``; the unmodifiable map is a copy, and ``values()`` returns a new
``dict`` each call, so no caller changes the stored map.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync, _kind_of

if TYPE_CHECKING:
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["AlterUserScramCredentialsResult"]


class AlterUserScramCredentialsResult(Generic[KindT]):
    """The result of the ``Admin.alter_user_scram_credentials()`` call.

    Java: ``org.apache.kafka.clients.admin.AlterUserScramCredentialsResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(self, *, futures: Mapping[str, KafkaFuture[None] | AsyncKafkaFuture[None]]) -> None:
        """``futures``: the required map from user names to futures representing the
        results of the alteration(s) for each user."""
        if futures is None:
            raise TypeError("futures must not be None")
        self._futures = dict(futures)
        self._kind: type[Sync] | type[Async] = _kind_of(self._futures.values())

    @classmethod
    def _new(cls, *, futures: Mapping[str, KafkaFuture[None] | AsyncKafkaFuture[None]],
             kind: type[Sync] | type[Async]) -> AlterUserScramCredentialsResult[Any]:
        """The clients' builder: Java's public ``AlterUserScramCredentialsResult(futures)``
        with the client kind set by the caller."""
        if futures is None:
            raise TypeError("futures must not be None")
        result = cls.__new__(cls)
        result._futures = dict(futures)
        result._kind = kind
        return result

    def values(self) -> dict[str, KafkaFuture[None] | AsyncKafkaFuture[None]]:
        """Return a map from user names to futures, which can be used to check the status
        of the alteration(s) for each user."""
        return dict(self._futures)

    def all(self) -> KafkaFuture[None] | AsyncKafkaFuture[None]:
        """Return a future which succeeds only if all the user SCRAM credential
        alterations succeed."""
        return self._kind._FUTURE.all_of(futures=tuple(self._futures.values()))
