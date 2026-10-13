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

"""``InvalidTxnTimeoutError``: Java's ``org.apache.kafka.common.errors.InvalidTxnTimeoutException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.api_error import ApiError

__all__ = ["InvalidTxnTimeoutError"]


class InvalidTxnTimeoutError(ApiError):
    """The transaction coordinator returns this error code if the timeout received
    via the InitProducerIdRequest is larger than the
    `transaction.max.timeout.ms` config value.

    Java: ``org.apache.kafka.common.errors.InvalidTxnTimeoutException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 50  # kafka_common_ErrorCode_INVALID_TRANSACTION_TIMEOUT

    def __init__(
        self,
        *,
        message: str,
        cause: BaseException | None = None,
    ) -> None:
        _throwable.init(self, message, cause)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
