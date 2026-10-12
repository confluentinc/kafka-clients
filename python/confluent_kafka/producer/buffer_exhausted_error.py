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

"""``BufferExhaustedError``: Java's ``org.apache.kafka.clients.producer.BufferExhaustedException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.timeout_error import TimeoutError

__all__ = ["BufferExhaustedError"]


class BufferExhaustedError(TimeoutError):
    """This exception is thrown if the producer cannot allocate memory for a
    record within max.block.ms due to the buffer being too full.

    In earlier versions a TimeoutException was thrown instead of this. To keep
    existing catch-clauses working this class extends TimeoutException.

    Java: ``org.apache.kafka.clients.producer.BufferExhaustedException``.
    """

    __module__ = "confluent_kafka.producer"

    _ffi_id: ClassVar[int] = -28  # kafka_common_ErrorCode_PRODUCER_BUFFER_EXHAUSTED

    def __init__(
        self,
        *,
        message: str,
    ) -> None:
        _throwable.init(self, message, None)
        self._java_kwargs = _throwable.kwargs(message=message)
