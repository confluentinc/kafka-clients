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

"""``CorrelationIdMismatchError``: Java's ``org.apache.kafka.common.requests.CorrelationIdMismatchException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.illegal_state_error import IllegalStateError

__all__ = ["CorrelationIdMismatchError"]


class CorrelationIdMismatchError(IllegalStateError):
    """Raised if the correlationId in a response header does not match the
    expected value from the request header.

    Java: ``org.apache.kafka.common.requests.CorrelationIdMismatchException``.
    """

    __module__ = "confluent_kafka.common.requests"

    _ffi_id: ClassVar[int] = -24  # kafka_common_ErrorCode_CORRELATION_ID_MISMATCH

    def __init__(
        self,
        *,
        message: str,
        request_correlation_id: int,
        response_correlation_id: int,
    ) -> None:
        _throwable.init(self, message, None)
        self._request_correlation_id = request_correlation_id
        self._response_correlation_id = response_correlation_id
        self._java_kwargs = _throwable.kwargs(message=message, request_correlation_id=request_correlation_id, response_correlation_id=response_correlation_id)

    def request_correlation_id(self) -> int:
        """Java's ``requestCorrelationId()``."""
        return self._request_correlation_id

    def response_correlation_id(self) -> int:
        """Java's ``responseCorrelationId()``."""
        return self._response_correlation_id
