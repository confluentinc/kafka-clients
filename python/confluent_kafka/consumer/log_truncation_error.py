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

"""``LogTruncationError``: Java's ``org.apache.kafka.clients.consumer.LogTruncationException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar, TYPE_CHECKING

from confluent_kafka import _throwable
from confluent_kafka._java import java_str
from confluent_kafka.consumer.offset_out_of_range_error import OffsetOutOfRangeError

if TYPE_CHECKING:
    from confluent_kafka.consumer.offset_and_metadata import OffsetAndMetadata
    from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["LogTruncationError"]


class LogTruncationError(OffsetOutOfRangeError):
    """In the event of an unclean leader election, the log will be truncated,
    previously committed data will be lost, and new data will be written over
    these offsets. When this happens, the consumer will detect the truncation
    and raise this exception (if no automatic reset policy has been defined)
    with the first offset known to diverge from what the consumer previously
    read.

    Java: ``org.apache.kafka.clients.consumer.LogTruncationException``.
    """

    __module__ = "confluent_kafka.consumer"

    _ffi_id: ClassVar[int] = -20  # kafka_common_ErrorCode_CONSUMER_LOG_TRUNCATION

    def __init__(
        self,
        *,
        message: str | None = None,
        fetch_offsets: Mapping[TopicPartition, int],
        divergent_offsets: Mapping[TopicPartition, OffsetAndMetadata],
    ) -> None:
        if message is not None:
            _throwable.init(self, message, None)
            self._offset_out_of_range_partitions = _throwable.copy_dict(fetch_offsets)
            self._divergent_offsets = _throwable.copy_dict(divergent_offsets)
        else:
            _throwable.init(self, "Truncated partitions detected with divergent offsets " + java_str(divergent_offsets), None)
            self._offset_out_of_range_partitions = _throwable.copy_dict(fetch_offsets)
            self._divergent_offsets = _throwable.copy_dict(divergent_offsets)
        self._java_kwargs = _throwable.kwargs(message=message, fetch_offsets=fetch_offsets, divergent_offsets=divergent_offsets)

    def divergent_offsets(self) -> dict[TopicPartition, OffsetAndMetadata]:
        """Java's ``divergentOffsets()``."""
        return self._divergent_offsets
