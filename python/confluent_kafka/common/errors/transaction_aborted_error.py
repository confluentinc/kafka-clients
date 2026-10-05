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

"""``TransactionAbortedError``: Java's ``org.apache.kafka.common.errors.TransactionAbortedException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka._args import Form, java_forms
from confluent_kafka.common.errors.api_error import ApiError

__all__ = ["TransactionAbortedError"]


class TransactionAbortedError(ApiError):
    """This is the Exception thrown when we are aborting any undrained batches
    during a transaction which is aborted without any underlying cause - which
    likely means that the user chose to abort.

    Java: ``org.apache.kafka.common.errors.TransactionAbortedException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = -17  # kafka_common_ErrorCode_TRANSACTION_ABORTED

    @java_forms(
        Form("message", "cause"),
        Form("message"),
        Form(),
    )
    def __init__(
        self,
        *,
        message: str | None = None,
        cause: BaseException | None = None,
        _java_form: int = -1,
    ) -> None:
        if _java_form == 0:
            _throwable.init(self, message, cause)
        elif _java_form == 1:
            _throwable.init(self, message, None)
        else:
            _throwable.init(self, "Failing batch since transaction was aborted", None)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
