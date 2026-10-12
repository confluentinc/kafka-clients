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

"""``ElectLeadersResult``: Java's ``org.apache.kafka.clients.admin.ElectLeadersResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). ``all()`` makes a new
future completed from the stored future's ``whenComplete``, kept through the private
helpers ``_new_future_like`` / ``_when_complete`` / ``_complete*``. Java's
``Optional<Throwable>`` is ``BaseException | None`` (Types).
"""

from __future__ import annotations

from collections.abc import Mapping
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
    from confluent_kafka.common import TopicPartition

__all__ = ["ElectLeadersResult"]


class ElectLeadersResult(Generic[KindT]):
    """The result of ``Admin.elect_leaders()``.

    Java: ``org.apache.kafka.clients.admin.ElectLeadersResult``.
    """

    __slots__ = ("_election_future", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "ElectLeadersResult cannot be constructed directly; use the admin client's "
            "elect_leaders()")

    @classmethod
    def _new(cls, *, election_future: AnyFuture[dict[TopicPartition, BaseException | None]],
             kind: type[Sync] | type[Async]) -> ElectLeadersResult[Any]:
        """Java's package-private ``ElectLeadersResult(electionFuture)``."""
        result = cls.__new__(cls)
        result._election_future = election_future
        result._kind = kind
        return result

    def partitions(self) -> AnyFuture[dict[TopicPartition, BaseException | None]]:
        """Get a future for the topic partitions for which a leader election was attempted. If
        the election succeeded then the value for a topic partition is ``None``; otherwise the
        election failed and the value is the error."""
        return self._election_future

    def all(self) -> AnyFuture[None]:
        """Return a future which succeeds if all the topic elections succeed."""
        result: AnyFuture[None] = _new_future_like(self._election_future)

        def action(topic_partitions: Mapping[TopicPartition, BaseException | None] | None,
                   throwable: BaseException | None) -> None:
            if throwable is not None:
                _complete_with_error(result, throwable)
            else:
                assert topic_partitions is not None
                for exception in topic_partitions.values():
                    if exception is not None:
                        _complete_with_error(result, exception)
                        return
                _complete(result, None)

        _when_complete(self.partitions(), action)
        return result
