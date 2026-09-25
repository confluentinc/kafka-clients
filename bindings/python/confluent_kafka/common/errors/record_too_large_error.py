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

"""``RecordTooLargeError``: Java's ``org.apache.kafka.common.errors.RecordTooLargeException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar, TYPE_CHECKING

from confluent_kafka import _throwable
from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka.common.errors.api_error import ApiError

if TYPE_CHECKING:
    from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["RecordTooLargeError"]


class RecordTooLargeError(ApiError):
    """This record is larger than the maximum allowable size

    Java: ``org.apache.kafka.common.errors.RecordTooLargeException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 10  # kafka_common_ErrorCode_MESSAGE_TOO_LARGE

    @java_forms(
        Form(),
        Form("message", "cause"),
        Form("message"),
        Form("cause"),
        Form("message", "record_too_large_partitions", defaults={"record_too_large_partitions": None}),
    )
    def __init__(
        self,
        *,
        message: str = UNSET,
        cause: BaseException | None = None,
        record_too_large_partitions: Mapping[TopicPartition, int] | None = None,
        _java_form: int = -1,
    ) -> None:
        if _java_form == 0:
            _throwable.init(self, None, None)
            self._record_too_large_partitions = None
        elif _java_form == 1:
            _throwable.init(self, message, cause)
            self._record_too_large_partitions = None
        elif _java_form == 3:
            _throwable.init(self, _throwable.cause_message(cause), cause)
            self._record_too_large_partitions = None
        else:
            _throwable.init(self, message, None)
            self._record_too_large_partitions = _throwable.copy_dict(record_too_large_partitions)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause, record_too_large_partitions=record_too_large_partitions)

    def record_too_large_partitions(self) -> dict[TopicPartition, int] | None:
        """Java's ``recordTooLargePartitions()``."""
        return self._record_too_large_partitions
