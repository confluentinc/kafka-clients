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

"""``InvalidProducerEpochError``: Java's ``org.apache.kafka.common.errors.InvalidProducerEpochException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.application_recoverable_error import ApplicationRecoverableError

__all__ = ["InvalidProducerEpochError"]


class InvalidProducerEpochError(ApplicationRecoverableError):
    """This exception indicates that the produce request sent to the partition
    leader contains a non-matching producer epoch. When encountering this
    exception, user should abort the ongoing transaction by calling
    KafkaProducer#abortTransaction which would try to send initPidRequest and
    reinitialize the producer under the hood.

    Java: ``org.apache.kafka.common.errors.InvalidProducerEpochException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 47  # kafka_common_ErrorCode_INVALID_PRODUCER_EPOCH

    def __init__(
        self,
        *,
        message: str,
    ) -> None:
        _throwable.init(self, message, None)
        self._java_kwargs = _throwable.kwargs(message=message)
