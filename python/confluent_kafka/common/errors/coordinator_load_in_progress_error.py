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

"""``CoordinatorLoadInProgressError``: Java's ``org.apache.kafka.common.errors.CoordinatorLoadInProgressException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.retriable_error import RetriableError

__all__ = ["CoordinatorLoadInProgressError"]


class CoordinatorLoadInProgressError(RetriableError):
    """In the context of the group coordinator, the broker returns this error code
    for any coordinator request if it is still loading the group metadata (e.g.
    after a leader change for that group metadata topic partition).

    In the context of the transactional coordinator, this error will be
    returned if there is a pending transactional request with the same
    transactional id, or if the transaction cache is currently being populated
    from the transaction log.

    Java:
    ``org.apache.kafka.common.errors.CoordinatorLoadInProgressException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 14  # kafka_common_ErrorCode_COORDINATOR_LOAD_IN_PROGRESS

    def __init__(
        self,
        *,
        message: str,
        cause: BaseException | None = None,
    ) -> None:
        _throwable.init(self, message, cause)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
