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

"""``DeleteConsumerGroupOffsetsResult``: Java's
``org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). Its methods make a new
future completed from the stored future's ``whenComplete``, kept through the private
helpers ``_new_future_like`` / ``_when_complete`` / ``_complete*`` (Idiom
translations). The stored map's values are Java's internal ``Errors``, carried as what
``Errors.exception()`` gives (``None`` for ``NONE``; ``_internals``).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka._java import java_str
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
from confluent_kafka.admin._internals import _sub_level_error
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    from confluent_kafka.common import TopicPartition

__all__ = ["DeleteConsumerGroupOffsetsResult"]


class DeleteConsumerGroupOffsetsResult(Generic[KindT]):
    """The result of the ``Admin.delete_consumer_group_offsets()`` call.

    Java: ``org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult``.
    """

    __slots__ = ("_future", "_partitions", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DeleteConsumerGroupOffsetsResult cannot be constructed directly; use the admin "
            "client's delete_consumer_group_offsets()")

    @classmethod
    def _new(cls, *, future: AnyFuture[dict[TopicPartition, BaseException | None]],
             partitions: Iterable[TopicPartition],
             kind: type[Sync] | type[Async]) -> DeleteConsumerGroupOffsetsResult[Any]:
        """Java's package-private ``DeleteConsumerGroupOffsetsResult(future, partitions)``."""
        result = cls.__new__(cls)
        result._future = future
        result._partitions = set(partitions)
        result._kind = kind
        return result

    def partition_result(self, *, partition: TopicPartition) -> AnyFuture[None]:
        """Return a future which can be used to check the result for a given partition."""
        if partition not in self._partitions:
            raise IllegalArgumentError(
                message="Partition " + java_str(partition)
                + " was not included in the original request")
        result: AnyFuture[None] = _new_future_like(self._future)

        def action(topic_partitions: Mapping[TopicPartition, BaseException | None] | None,
                   throwable: BaseException | None) -> None:
            if throwable is not None:
                _complete_with_error(result, throwable)
            elif not self._maybe_complete_exceptionally(topic_partitions, partition, result):
                _complete(result, None)

        _when_complete(self._future, action)
        return result

    def all(self) -> AnyFuture[None]:
        """Return a future which succeeds only if all the deletions succeed. If not, the
        first partition error shall be returned."""
        result: AnyFuture[None] = _new_future_like(self._future)

        def action(topic_partitions: Mapping[TopicPartition, BaseException | None] | None,
                   throwable: BaseException | None) -> None:
            if throwable is not None:
                _complete_with_error(result, throwable)
            else:
                for partition in self._partitions:
                    if self._maybe_complete_exceptionally(topic_partitions, partition, result):
                        return
                _complete(result, None)

        _when_complete(self._future, action)
        return result

    def _maybe_complete_exceptionally(
            self, partition_level_errors: Mapping[TopicPartition, BaseException | None] | None,
            partition: TopicPartition, result: AnyFuture[None]) -> bool:
        assert partition_level_errors is not None
        exception = _sub_level_error(
            partition_level_errors, partition,
            "Offset deletion result for partition \"" + java_str(partition)
            + "\" was not included in the response")
        if exception is not None:
            _complete_with_error(result, exception)
            return True
        return False
