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

"""Generated consumer-package errors for ``confluent_kafka.consumer``.

GENERATED, DO NOT EDIT. Produced from the Java exception sources by
`cargo xtask generate-error-codes`, cross-checked against the FFI
`kafka_common_ErrorCode_t` enum, and validated for staleness by
`cargo xtask check-generated`.
"""

from __future__ import annotations

from typing import ClassVar, TYPE_CHECKING

from confluent_kafka.common.errors._base import KafkaError
from confluent_kafka.common.errors._generated import RetriableError

if TYPE_CHECKING:
    from confluent_kafka.common.topic_partition import TopicPartition
    from confluent_kafka.consumer.offset_and_metadata import OffsetAndMetadata

__all__ = [
    "CommitFailedError",
    "InvalidOffsetError",
    "LogTruncationError",
    "NoOffsetForPartitionError",
    "OffsetOutOfRangeError",
    "RetriableCommitFailedError",
]


class CommitFailedError(KafkaError):
    """Mirrors Java's ``org.apache.kafka.clients.consumer.CommitFailedException``."""

    _ffi_id: ClassVar[int] = -19  # kafka_common_ErrorCode_CONSUMER_COMMIT_FAILED


class InvalidOffsetError(KafkaError):
    """Mirrors Java's ``org.apache.kafka.clients.consumer.InvalidOffsetException``."""

    def __init__(self, *args: object) -> None:
        if type(self) is InvalidOffsetError:
            raise TypeError(
                "InvalidOffsetError is an abstract catch-only base; it is never raised directly"
            )
        super().__init__(*args)


class NoOffsetForPartitionError(InvalidOffsetError):
    """Mirrors Java's ``org.apache.kafka.clients.consumer.NoOffsetForPartitionException``."""

    _ffi_id: ClassVar[int] = -21  # kafka_common_ErrorCode_CONSUMER_NO_OFFSET_FOR_PARTITION

    def partitions(self) -> set[TopicPartition]:
        """The partitions with no defined offset."""
        return self._error_payload["partitions"]  # type: ignore[attr-defined,no-any-return]


class OffsetOutOfRangeError(InvalidOffsetError):
    """Mirrors Java's ``org.apache.kafka.clients.consumer.OffsetOutOfRangeException``."""

    _ffi_id: ClassVar[int] = -22  # kafka_common_ErrorCode_CONSUMER_OFFSET_OUT_OF_RANGE

    def offset_out_of_range_partitions(self) -> dict[TopicPartition, int]:
        """A map of the out-of-range partitions to their requested offsets."""
        return self._error_payload["offset_out_of_range_partitions"]  # type: ignore[attr-defined,no-any-return]

    def partitions(self) -> set[TopicPartition]:
        """The out-of-range partitions."""
        return set(self._error_payload["offset_out_of_range_partitions"].keys())  # type: ignore[attr-defined]


class RetriableCommitFailedError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.clients.consumer.RetriableCommitFailedException``."""

    _ffi_id: ClassVar[int] = -23  # kafka_common_ErrorCode_CONSUMER_RETRIABLE_COMMIT_FAILED


class LogTruncationError(OffsetOutOfRangeError):
    """Mirrors Java's ``org.apache.kafka.clients.consumer.LogTruncationException``."""

    _ffi_id: ClassVar[int] = -20  # kafka_common_ErrorCode_CONSUMER_LOG_TRUNCATION

    def divergent_offsets(self) -> dict[TopicPartition, OffsetAndMetadata]:
        """A map of the truncated partitions to the divergent offsets at which the logs diverged."""
        return self._error_payload["divergent_offsets"]  # type: ignore[attr-defined,no-any-return]
