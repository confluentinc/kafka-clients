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

"""``AlterConsumerGroupOffsetsResult``: Java's
``org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult``.

Hand-written: outside every generator pattern (``_reference_set.md``, Coverage), as
``all()`` throws ``error.exception(message)`` from inside ``thenApply``. Java's
constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new`` builds
it (CLAUDE.md, Python Binding Conventions, Class family). The stored map's values are
Java's internal ``Errors``, carried as what ``Errors.exception()`` gives (``None`` for
``NONE``); ``Errors.exception(message)`` is ``_errors_exception`` (``_internals``).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka._java import java_str
from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.common.kafka_future import _KafkaFutureImpl
from confluent_kafka.admin._internals import _errors_exception
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    from confluent_kafka.common import TopicPartition
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["AlterConsumerGroupOffsetsResult"]


class AlterConsumerGroupOffsetsResult(Generic[KindT]):
    """The result of the ``Admin.alter_consumer_group_offsets()`` call.

    Java: ``org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult``.
    """

    __slots__ = ("_future", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "AlterConsumerGroupOffsetsResult cannot be constructed directly; use the admin "
            "client's alter_consumer_group_offsets()")

    @classmethod
    def _new(cls, *, future: KafkaFuture[dict[TopicPartition, BaseException | None]] | AsyncKafkaFuture[dict[TopicPartition, BaseException | None]],
             kind: type[Sync] | type[Async]) -> AlterConsumerGroupOffsetsResult[Any]:
        """Java's package-private ``AlterConsumerGroupOffsetsResult(future)``."""
        result = cls.__new__(cls)
        result._future = future
        result._kind = kind
        return result

    def partition_result(self, *, partition: TopicPartition) -> KafkaFuture[None] | AsyncKafkaFuture[None]:
        """Return a future which can be used to check the result for a given partition."""
        result: _KafkaFutureImpl[None] = _KafkaFutureImpl()

        def action(topic_partitions: Mapping[TopicPartition, BaseException | None] | None,
                   throwable: BaseException | None) -> None:
            if throwable is not None:
                result.complete_with_error(throwable)
                return
            assert topic_partitions is not None
            if partition not in topic_partitions:
                result.complete_with_error(IllegalArgumentError(
                    message="Alter offset for partition \"" + java_str(partition)
                    + "\" was not attempted"))
            else:
                error = topic_partitions[partition]
                if error is None:
                    result.complete(None)
                else:
                    result.complete_with_error(error)

        self._future.when_complete(action=action)
        return self._kind._FUTURE._of_impl(result)

    def all(self) -> KafkaFuture[None] | AsyncKafkaFuture[None]:
        """Return a future which succeeds if all the alter offsets succeed."""
        def check(topic_partition_errors_map: Mapping[TopicPartition, BaseException | None]
                  ) -> None:
            partitions_failed = [partition for partition, error
                                 in topic_partition_errors_map.items() if error is not None]
            for error in topic_partition_errors_map.values():
                if error is not None:
                    exception = _errors_exception(
                        error, "Failed altering group offsets for the following partitions: "
                        + java_str(partitions_failed))
                    assert exception is not None
                    raise exception
            return None

        return self._future.then_apply(function=check)
