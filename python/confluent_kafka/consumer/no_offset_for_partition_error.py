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

"""``NoOffsetForPartitionError``: Java's ``org.apache.kafka.clients.consumer.NoOffsetForPartitionException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import ClassVar, TYPE_CHECKING

from confluent_kafka import _throwable
from confluent_kafka._args import Form, java_forms
from confluent_kafka._java import java_str
from confluent_kafka.consumer.invalid_offset_error import InvalidOffsetError

if TYPE_CHECKING:
    from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["NoOffsetForPartitionError"]


class NoOffsetForPartitionError(InvalidOffsetError):
    """Indicates that there is no stored offset for a partition and no defined
    offset reset policy.

    Java: ``org.apache.kafka.clients.consumer.NoOffsetForPartitionException``.
    """

    __module__ = "confluent_kafka.consumer"

    _ffi_id: ClassVar[int] = -21  # kafka_common_ErrorCode_CONSUMER_NO_OFFSET_FOR_PARTITION

    @java_forms(
        Form("partition"),
        Form("partitions"),
    )
    def __init__(
        self,
        *,
        partition: TopicPartition | None = None,
        partitions: Iterable[TopicPartition] | None = None,
        _java_form: int = -1,
    ) -> None:
        partitions = _throwable.materialize(partitions)
        if _java_form == 0:
            _throwable.init(self, "Undefined offset with no reset policy for partition: " + java_str(partition), None)
            self._partitions = {partition}
        else:
            _throwable.init(self, "Undefined offset with no reset policy for partitions: " + java_str(partitions), None)
            self._partitions = _throwable.copy_set(partitions)
        self._java_kwargs = _throwable.kwargs(partition=partition, partitions=partitions)

    def partitions(self) -> set[TopicPartition]:
        """Java's ``partitions()``."""
        return self._partitions
