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

"""``CoordinatorNotAvailableError``: Java's ``org.apache.kafka.common.errors.CoordinatorNotAvailableException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.refresh_retriable_error import RefreshRetriableError

__all__ = ["CoordinatorNotAvailableError"]


class CoordinatorNotAvailableError(RefreshRetriableError):
    """In the context of the group coordinator, the broker returns this error code
    for metadata or offset commit requests if the group metadata topic has not
    been created yet.

    In the context of the transactional coordinator, this error will be
    returned if the underlying transactional log is under replicated or if an
    append to the log times out.

    Java: ``org.apache.kafka.common.errors.CoordinatorNotAvailableException``.
    """

    __module__ = "confluent_kafka.common.errors"

    INSTANCE: ClassVar[CoordinatorNotAvailableError]

    _ffi_id: ClassVar[int] = 15  # kafka_common_ErrorCode_COORDINATOR_NOT_AVAILABLE

    def __init__(
        self,
        *,
        message: str,
        cause: BaseException | None = None,
    ) -> None:
        _throwable.init(self, message, cause)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)


CoordinatorNotAvailableError.INSTANCE = _throwable.singleton(CoordinatorNotAvailableError, "CoordinatorNotAvailableError.INSTANCE", None, None)
