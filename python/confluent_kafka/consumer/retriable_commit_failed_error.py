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

"""``RetriableCommitFailedError``: Java's ``org.apache.kafka.clients.consumer.RetriableCommitFailedException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka._args import Form, java_forms
from confluent_kafka.common.errors.retriable_error import RetriableError

__all__ = ["RetriableCommitFailedError"]


class RetriableCommitFailedError(RetriableError):
    """Java: ``org.apache.kafka.clients.consumer.RetriableCommitFailedException``."""

    __module__ = "confluent_kafka.consumer"

    _ffi_id: ClassVar[int] = -23  # kafka_common_ErrorCode_CONSUMER_RETRIABLE_COMMIT_FAILED

    @java_forms(
        Form("cause"),
        Form("message"),
        Form("message", "cause"),
    )
    def __init__(
        self,
        *,
        message: str | None = None,
        cause: BaseException | None = None,
        _java_form: int = -1,
    ) -> None:
        if _java_form == 0:
            _throwable.init(self, "Offset commit failed with a retriable exception. You should retry committing " + "the latest consumed offsets.", cause)
        elif _java_form == 1:
            _throwable.init(self, message, None)
        else:
            _throwable.init(self, message, cause)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
