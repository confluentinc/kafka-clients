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

"""``ListConfigResourcesResult``: Java's
``org.apache.kafka.clients.admin.ListConfigResourcesResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). ``all()`` makes a new
future completed from the stored future's ``whenComplete``, kept through the private
helpers ``_new_future_like`` / ``_when_complete`` / ``_complete*``. A ``Collection``
yields a ``list``: Java's ``KafkaAdminClient`` builds no ``Set`` for it (Types).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import (
    AnyFuture,
    Async,
    KindT,
    Sync,
    _complete,
    _complete_with_error,
    _new_future_like,
    _when_complete,
)

if TYPE_CHECKING:
    from confluent_kafka.common.config import ConfigResource

__all__ = ["ListConfigResourcesResult"]


class ListConfigResourcesResult(Generic[KindT]):
    """The result of the ``Admin.list_config_resources()`` call.

    Java: ``org.apache.kafka.clients.admin.ListConfigResourcesResult``.
    """

    __slots__ = ("_future", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "ListConfigResourcesResult cannot be constructed directly; use the admin "
            "client's list_config_resources()")

    @classmethod
    def _new(cls, *, future: AnyFuture[list[ConfigResource]],
             kind: type[Sync] | type[Async]) -> ListConfigResourcesResult[Any]:
        """Java's package-private ``ListConfigResourcesResult(future)``."""
        result = cls.__new__(cls)
        result._future = future
        result._kind = kind
        return result

    def all(self) -> AnyFuture[list[ConfigResource]]:
        """Returns a future that yields either an exception, or the full set of config
        resources.

        In the event of a failure, the future yields nothing but the first exception
        which occurred.
        """
        result: AnyFuture[list[ConfigResource]] = _new_future_like(self._future)

        def action(resources: list[ConfigResource] | None, throwable: BaseException | None) -> None:
            if throwable is not None:
                _complete_with_error(result, throwable)
            else:
                assert resources is not None
                _complete(result, resources)

        _when_complete(self._future, action)
        return result
