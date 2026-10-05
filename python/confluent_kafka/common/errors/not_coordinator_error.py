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

"""``NotCoordinatorError``: Java's ``org.apache.kafka.common.errors.NotCoordinatorException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.refresh_retriable_error import RefreshRetriableError

__all__ = ["NotCoordinatorError"]


class NotCoordinatorError(RefreshRetriableError):
    """In the context of the group coordinator, the broker returns this error code
    if it receives an offset fetch or commit request for a group it's not the
    coordinator of.

    In the context of the transactional coordinator, it returns this error when
    it receives a transactional request with a transactionalId the coordinator
    doesn't own.

    Java: ``org.apache.kafka.common.errors.NotCoordinatorException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 16  # kafka_common_ErrorCode_NOT_COORDINATOR

    def __init__(
        self,
        *,
        message: str,
        cause: BaseException | None = None,
    ) -> None:
        _throwable.init(self, message, cause)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
