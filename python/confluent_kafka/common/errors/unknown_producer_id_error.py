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

"""``UnknownProducerIdError``: Java's ``org.apache.kafka.common.errors.UnknownProducerIdException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.out_of_order_sequence_error import OutOfOrderSequenceError

__all__ = ["UnknownProducerIdError"]


class UnknownProducerIdError(OutOfOrderSequenceError):
    """This exception is raised by the broker if it could not locate the producer
    metadata associated with the producerId in question. This could happen if,
    for instance, the producer's records were deleted because their retention
    time had elapsed. Once the last records of the producerId are removed, the
    producer's metadata is removed from the broker, and future appends by the
    producer will return this exception.

    Java: ``org.apache.kafka.common.errors.UnknownProducerIdException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 59  # kafka_common_ErrorCode_UNKNOWN_PRODUCER_ID

    def __init__(
        self,
        *,
        message: str,
    ) -> None:
        _throwable.init(self, message, None)
        self._java_kwargs = _throwable.kwargs(message=message)
