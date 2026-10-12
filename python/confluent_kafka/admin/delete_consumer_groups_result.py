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

"""``DeleteConsumerGroupsResult``: Java's
``org.apache.kafka.clients.admin.DeleteConsumerGroupsResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). ``deletedGroups()``
returns a copy of the stored map, so each call gives a new ``dict``. ``all()`` is Java's
``KafkaFuture.allOf``, kept through the private helper ``_all_of`` (Idiom
translations).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Generic

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync, _all_of

__all__ = ["DeleteConsumerGroupsResult"]


class DeleteConsumerGroupsResult(Generic[KindT]):
    """The result of the ``Admin.delete_consumer_groups()`` call.

    Java: ``org.apache.kafka.clients.admin.DeleteConsumerGroupsResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DeleteConsumerGroupsResult cannot be constructed directly; use the admin "
            "client's delete_consumer_groups()")

    @classmethod
    def _new(cls, *, futures: Mapping[str, AnyFuture[None]],
             kind: type[Sync] | type[Async]) -> DeleteConsumerGroupsResult[Any]:
        """Java's package-private ``DeleteConsumerGroupsResult(futures)``."""
        result = cls.__new__(cls)
        result._futures = dict(futures)
        result._kind = kind
        return result

    def deleted_groups(self) -> dict[str, AnyFuture[None]]:
        """Return a map from group id to futures which can be used to check the status
        of individual deletions."""
        return dict(self._futures)

    def all(self) -> AnyFuture[None]:
        """Return a future which succeeds only if all the consumer group deletions
        succeed."""
        return _all_of(self._futures.values(), self._kind)
