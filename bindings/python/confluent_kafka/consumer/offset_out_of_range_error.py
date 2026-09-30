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

"""``OffsetOutOfRangeError``: Java's ``org.apache.kafka.clients.consumer.OffsetOutOfRangeException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar, TYPE_CHECKING

from confluent_kafka import _throwable
from confluent_kafka._java import java_str
from confluent_kafka.consumer.invalid_offset_error import InvalidOffsetError

if TYPE_CHECKING:
    from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["OffsetOutOfRangeError"]


class OffsetOutOfRangeError(InvalidOffsetError):
    """No reset policy has been defined, and the offsets for these partitions are
    either larger or smaller than the range of offsets the server has for the
    given partition.

    Java: ``org.apache.kafka.clients.consumer.OffsetOutOfRangeException``.
    """

    __module__ = "confluent_kafka.consumer"

    _ffi_id: ClassVar[int] = -22  # kafka_common_ErrorCode_CONSUMER_OFFSET_OUT_OF_RANGE

    def __init__(
        self,
        *,
        message: str | None = None,
        offset_out_of_range_partitions: Mapping[TopicPartition, int],
    ) -> None:
        if message is not None:
            _throwable.init(self, message, None)
            self._offset_out_of_range_partitions = _throwable.copy_dict(offset_out_of_range_partitions)
        else:
            _throwable.init(self, "Offsets out of range with no configured reset policy for partitions: " + java_str(offset_out_of_range_partitions), None)
            self._offset_out_of_range_partitions = _throwable.copy_dict(offset_out_of_range_partitions)
        self._java_kwargs = _throwable.kwargs(message=message, offset_out_of_range_partitions=offset_out_of_range_partitions)

    def offset_out_of_range_partitions(self) -> dict[TopicPartition, int]:
        """Java's ``offsetOutOfRangePartitions()``."""
        return self._offset_out_of_range_partitions

    def partitions(self) -> set[TopicPartition]:
        """Java's ``partitions()``."""
        return set(self._offset_out_of_range_partitions.keys())
