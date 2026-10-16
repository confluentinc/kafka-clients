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
future completed from the stored future's ``when_complete``: Java's ``new
KafkaFutureImpl<>()`` is the private ``_KafkaFutureImpl``. Java's
``Optional<Throwable>`` is ``BaseException | None`` (Types).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.common.kafka_future import _KafkaFutureImpl

if TYPE_CHECKING:
    from confluent_kafka.common import TopicPartition
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

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
    def _new(
            cls, *,
            election_future: KafkaFuture[dict[TopicPartition, BaseException | None]] | AsyncKafkaFuture[dict[TopicPartition, BaseException | None]],
            kind: type[Sync] | type[Async]) -> ElectLeadersResult[Any]:
        """Java's package-private ``ElectLeadersResult(electionFuture)``."""
        result = cls.__new__(cls)
        result._election_future = election_future
        result._kind = kind
        return result

    def partitions(
            self) -> KafkaFuture[dict[TopicPartition, BaseException | None]] | AsyncKafkaFuture[dict[TopicPartition, BaseException | None]]:
        """Get a future for the topic partitions for which a leader election was attempted. If
        the election succeeded then the value for a topic partition is ``None``; otherwise the
        election failed and the value is the error."""
        return self._election_future

    def all(self) -> KafkaFuture[None] | AsyncKafkaFuture[None]:
        """Return a future which succeeds if all the topic elections succeed."""
        result: _KafkaFutureImpl[None] = _KafkaFutureImpl()

        def action(topic_partitions: Mapping[TopicPartition, BaseException | None] | None,
                   throwable: BaseException | None) -> None:
            if throwable is not None:
                result.complete_with_error(throwable)
            else:
                assert topic_partitions is not None
                for exception in topic_partitions.values():
                    if exception is not None:
                        result.complete_with_error(exception)
                        return
                result.complete(None)

        self.partitions().when_complete(action=action)
        return self._kind._FUTURE._of_impl(result)
