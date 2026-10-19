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

"""``ListConsumerGroupOffsetsResult``: Java's
``org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult``.

Hand-written: outside every generator pattern (``_reference_set.md``, Coverage), as the
constructor re-keys a ``CoordinatorKey`` map (``_CoordinatorKey``, ``_internals``) by
``idValue``, and ``partitionsToOffsetAndMetadata()`` is size-checked
(``IllegalStateException``) before ``iterator().next()``. Java's constructor is
package-private: ``__init__`` raises ``TypeError`` and ``_new`` builds it (CLAUDE.md,
Python Binding Conventions, Class family). Java's overloads
``partitionsToOffsetAndMetadata()`` / ``(String groupId)`` are one method, ``group_id``
``None`` selecting the first (Signatures); Java's messages, which name the Java
overloads, are kept. A partition without a committed offset maps to ``None`` (Javadoc).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka._args import MISSING
from confluent_kafka._java import java_str
from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.illegal_state_error import IllegalStateError

if TYPE_CHECKING:
    from confluent_kafka.admin._internals import _CoordinatorKey
    from confluent_kafka.common import TopicPartition
    from confluent_kafka.consumer import OffsetAndMetadata
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["ListConsumerGroupOffsetsResult"]

_Offsets = dict["TopicPartition", "OffsetAndMetadata | None"]


class ListConsumerGroupOffsetsResult(Generic[KindT]):
    """The result of the ``Admin.list_consumer_group_offsets()`` call.

    Java: ``org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "ListConsumerGroupOffsetsResult cannot be constructed directly; use the admin "
            "client's list_consumer_group_offsets()")

    @classmethod
    def _new(cls, *, futures: Mapping[_CoordinatorKey, KafkaFuture[_Offsets] | AsyncKafkaFuture[_Offsets]],
             kind: type[Sync] | type[Async]) -> ListConsumerGroupOffsetsResult[Any]:
        """Java's package-private ``ListConsumerGroupOffsetsResult(futures)``."""
        result = cls.__new__(cls)
        result._futures = {key.id_value: future for key, future in futures.items()}
        result._kind = kind
        return result

    def partitions_to_offset_and_metadata(self, *, group_id: str | None = MISSING
                                          ) -> KafkaFuture[_Offsets] | AsyncKafkaFuture[_Offsets]:
        """Return a future which yields a map of topic partitions to
        ``OffsetAndMetadata`` objects, for the specified group (``group_id``) or, with it
        left out, for the one group requested. If the group does not have a committed
        offset for a partition, the corresponding value in the returned map will be
        ``None``.

        Without ``group_id``, raises ``IllegalStateError`` when offsets of several groups
        were requested; with it, ``IllegalArgumentError`` when that group's were not (a
        ``None`` is Java's ``null`` group id, ``CoordinatorKey.byGroupId(null)``).
        """
        if group_id is MISSING:
            if len(self._futures) != 1:
                raise IllegalStateError(
                    message="Offsets from multiple consumer groups were requested. "
                    "Use partitionsToOffsetAndMetadata(groupId) instead to get future for a "
                    "specific group.")
            return next(iter(self._futures.values()))
        if group_id not in self._futures:
            raise IllegalArgumentError(
                message="Offsets for consumer group '" + java_str(group_id)
                + "' were not requested.")
        return self._futures[group_id]

    def all(self) -> KafkaFuture[dict[str, _Offsets]] | AsyncKafkaFuture[dict[str, _Offsets]]:
        """Return a future which yields all ``dict[str, dict[TopicPartition,
        OffsetAndMetadata]]`` objects, if requests for all the groups succeed."""
        futures = self._futures

        def collect(_: None) -> dict[str, _Offsets]:
            # KafkaFuture#allOf already ensured that all of the futures completed
            # successfully.
            return {key: future.get_now(value_if_absent=None) for key, future in futures.items()}

        return self._kind._FUTURE.all_of(futures=tuple(futures.values())).then_apply(function=collect)
